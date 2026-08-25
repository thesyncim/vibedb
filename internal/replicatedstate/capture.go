package replicatedstate

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

var ErrTransitionCapture = errors.New("replicatedstate: invalid transition capture")

const TransitionCaptureCollectionName = "__vibedb_split_capture"

// TransitionCaptureTarget is one private synchronous collection in the same
// transaction domain as the replicated system and user collections.
type TransitionCaptureTarget struct {
	Name       string
	Collection *durable.Collection
}

// TransitionCaptureBounds describe one encoder input without retaining row
// bytes. Capture implementations use the same method for construction-time
// worst-case qualification and exact admission accounting.
type TransitionCaptureBounds struct {
	Transitions uint64
	KeyBytes    uint64
	BeforeBytes uint64
	AfterBytes  uint64
}

// TransitionMutation is one exact final source-row change. Key, Before, and
// After are read-only borrowed apply-plan bytes. Nil means that the row is
// absent on that side.
type TransitionMutation struct {
	Key    []byte
	Before []byte
	After  []byte
}

// CapturedTransition is one consecutive replicated publication. Mutations are
// strictly key ordered. Empty transitions remain required for no-op,
// configuration, and ownership entries.
type CapturedTransition struct {
	Applied               uint64
	Term                  uint64
	BeforeOwnershipEpoch  uint64
	AfterOwnershipEpoch   uint64
	BeforeRoutingVersion  uint64
	AfterRoutingVersion   uint64
	BeforeRouteGeneration uint64
	AfterRouteGeneration  uint64
	PreviousEntryDigest   [32]byte
	EntryDigest           [32]byte
	BeforeDataChainDigest [32]byte
	AfterDataChainDigest  [32]byte
	mutations             []finalMutation
}

// MutationCount returns the exact changed-row count.
func (t CapturedTransition) MutationCount() int { return len(t.mutations) }

// Mutation returns one borrowed exact before-and-after transition.
func (t CapturedTransition) Mutation(index int) TransitionMutation {
	mutation := &t.mutations[index]
	result := TransitionMutation{Key: mutation.key}
	if mutation.beforeFound {
		result.Before = mutation.before
	}
	if !mutation.delete {
		result.After = mutation.value
	}
	return result
}

// Bounds returns aggregate input bytes without walking or encoding JSON in a
// capture implementation more than once.
func (t CapturedTransition) Bounds() TransitionCaptureBounds {
	result := TransitionCaptureBounds{Transitions: uint64(len(t.mutations))}
	for i := range t.mutations {
		mutation := &t.mutations[i]
		result.KeyBytes += uint64(len(mutation.key))
		if mutation.beforeFound {
			result.BeforeBytes += uint64(len(mutation.before))
		}
		if !mutation.delete {
			result.AfterBytes += uint64(len(mutation.value))
		}
	}
	return result
}

// TransitionCapture encodes one exact publication into a private collection
// row that the Machine commits atomically with its state and user mutations.
// Implementations must be serial, deterministic, and must not retain the
// borrowed transition. The target collection's value grammar belongs to the
// implementation; AppendTransition appends one complete value.
// Published runs after the source and capture record are durable; an error
// poisons apply and requires reopen, but cannot roll back that publication.
type TransitionCapture interface {
	Target() TransitionCaptureTarget
	// Begin creates or recovers the capture header. An empty target must publish
	// exactly one header through publish; a non-empty target must recover without
	// calling it. Machine supplies the only mutation authority, so a participant
	// reserved by a CheckpointGroup never escapes through Collection.Update.
	Begin(State, func(key, value []byte) error) error
	MaxEncodedBytes(TransitionCaptureBounds) (int, error)
	AppendTransition([]byte, CapturedTransition) ([]byte, error)
	Published(CapturedTransition) error
}

func (m *Machine) beginTransitionCapture(capture TransitionCapture) error {
	if capture == nil || !m.initialized || m.capture != nil {
		return ErrTransitionCapture
	}
	target := capture.Target()
	reserved := m.reservedCaptureTarget.Collection != nil || m.reservedCaptureTarget.Name != ""
	if (m.checkpointGroup != nil && !reserved) ||
		(reserved && (target != m.reservedCaptureTarget || target.Collection == nil)) {
		return ErrTransitionCapture
	}
	if err := m.validateTransitionCaptureTarget(target, capture); err != nil {
		return err
	}
	empty := target.Collection.Len() == 0
	published := false
	publish := func(key, value []byte) error {
		if !empty || published || len(key) == 0 || len(value) == 0 {
			return ErrTransitionCapture
		}
		published = true
		write := func(batch *durable.DatabaseBatch) error {
			captureBatch, err := batch.CollectionHandle(target.Collection)
			if err != nil {
				return err
			}
			return captureBatch.Put(key, value)
		}
		members := []durable.NamedCollection{{
			Name: target.Name, Collection: target.Collection, BatchDocumentsHint: 1,
		}}
		if m.checkpointGroup != nil {
			return m.checkpointGroup.Update(
				m.state.Applied, members, m.options.TxnLimits, write,
			)
		}
		return durable.UpdateCollections(m.txnLog, members, m.options.TxnLimits, write)
	}
	if err := capture.Begin(cloneState(m.state), publish); err != nil {
		return fmt.Errorf("%w: begin: %v", ErrTransitionCapture, err)
	}
	if empty != published || (empty && target.Collection.Len() != 1) {
		return fmt.Errorf("%w: noncanonical header publication", ErrTransitionCapture)
	}
	m.capture = capture
	m.captureTarget = target
	return nil
}

func validReservedTransitionCaptureTarget(
	target TransitionCaptureTarget,
	system CollectionTarget,
	relations []relationCollection,
) bool {
	if target.Name == "" || len(target.Name) > replication.MaxCollectionBytes ||
		!utf8.ValidString(target.Name) || strings.IndexByte(target.Name, 0) >= 0 ||
		target.Collection == nil || target.Name == systemCollectionName ||
		target.Collection == system.Collection || target.Collection.HasSchema() ||
		target.Collection.HasIndexes() || !target.Collection.HasOpaqueValues() ||
		!target.Collection.HasSynchronousDurability() || !target.Collection.SupportsUpdate() ||
		target.Collection.MaxKeyBytes() < 8 || target.Collection.MaxBatchDocuments() < 1 {
		return false
	}
	for i := range relations {
		if target.Name == relations[i].name || target.Collection == relations[i].target.Collection {
			return false
		}
	}
	return true
}

// BeginTransitionCapture installs a source capture at one exact publication.
// The machine mutex excludes apply while Begin creates or validates its base.
func (m *Machine) BeginTransitionCapture(capture TransitionCapture) error {
	if m == nil {
		return ErrTransitionCapture
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return err
	}
	return m.beginTransitionCapture(capture)
}

func (m *Machine) validateTransitionCaptureTarget(
	target TransitionCaptureTarget,
	capture TransitionCapture,
) error {
	if capture == nil || target.Collection == nil || target.Name == "" ||
		target.Name == systemCollectionName || target.Name == m.userName ||
		len(target.Name) > replication.MaxCollectionBytes ||
		!utf8.ValidString(target.Name) || strings.IndexByte(target.Name, 0) >= 0 ||
		target.Collection == m.system.Collection || target.Collection == m.user.Collection ||
		target.Collection.HasSchema() || target.Collection.HasIndexes() ||
		!target.Collection.HasSynchronousDurability() ||
		!target.Collection.SupportsUpdate() || target.Collection.MaxKeyBytes() < 8 ||
		target.Collection.MaxBatchDocuments() < 1 {
		return ErrTransitionCapture
	}
	requiredDocuments, err := RequiredBundleTransactionDocuments(
		m.user.Limits.MaxDistinctMutations, m.options.RetryWindow, true,
	)
	if err != nil || m.options.TxnLimits.MaxCollections < 3 ||
		m.options.TxnLimits.MaxDocuments < requiredDocuments {
		return fmt.Errorf("%w: transaction dimensions", ErrTransitionCapture)
	}
	maxBefore := uint64(m.user.Limits.MaxDistinctMutations) *
		uint64(m.user.Limits.MaxDocumentBytes)
	// Keys and after images share both the durable batch envelope and the
	// narrower replicated command envelope; their independent maxima cannot
	// occur in one admitted transition.
	maxPayload := uint64(min(m.user.Limits.MaxBatchBytes, replication.MaxCommandBytes))
	possiblePayload := uint64(m.user.Limits.MaxDistinctMutations) *
		uint64(m.user.Limits.MaxDocumentBytes+m.user.Limits.MaxKeyBytes)
	maxPayload = min(maxPayload, possiblePayload)
	maxAfter := min(maxPayload, uint64(m.user.Limits.MaxDistinctMutations)*
		uint64(m.user.Limits.MaxDocumentBytes))
	maxKeys := maxPayload - maxAfter
	if maxKeys > uint64(m.user.Limits.MaxDistinctMutations)*uint64(m.user.Limits.MaxKeyBytes) {
		return fmt.Errorf("%w: record payload dimensions", ErrTransitionCapture)
	}
	maxRecord, err := capture.MaxEncodedBytes(TransitionCaptureBounds{
		Transitions: uint64(m.user.Limits.MaxDistinctMutations),
		KeyBytes:    maxKeys, BeforeBytes: maxBefore, AfterBytes: maxAfter,
	})
	if err != nil || maxRecord <= 0 || maxRecord > target.Collection.MaxDocumentBytes() ||
		maxRecord > target.Collection.MaxBatchBytes()-8 {
		return fmt.Errorf("%w: record capacity", ErrTransitionCapture)
	}
	baseBytes, ok := checkedTxnBytes(
		min(m.user.Limits.MaxBatchBytes, replication.MaxCommandBytes),
		maxSystemTransitionBytes(m.system.Limits))
	if !ok || int64(maxRecord) > math.MaxInt64-baseBytes-8 ||
		m.options.TxnLimits.MaxBytes < baseBytes+int64(maxRecord)+8 {
		return fmt.Errorf("%w: transaction byte capacity", ErrTransitionCapture)
	}
	if err := m.txnLog.ValidateCollections([]durable.NamedCollection{
		{Name: systemCollectionName, Collection: m.system.Collection},
		{Name: m.userName, Collection: m.user.Collection},
		{Name: target.Name, Collection: target.Collection},
	}); err != nil {
		return fmt.Errorf("%w: transaction binding: %v", ErrTransitionCapture, err)
	}
	return nil
}

func maxSystemTransitionBytes(limits CollectionLimits) int {
	return limits.MaxBatchBytes
}

func (m *Machine) capturedTransition(next State, changes []finalMutation) CapturedTransition {
	if cap(m.captureChanges) < len(changes) {
		m.captureChanges = make([]finalMutation, len(changes))
	} else {
		m.captureChanges = m.captureChanges[:len(changes)]
	}
	copy(m.captureChanges, changes)
	slices.SortFunc(m.captureChanges, func(left, right finalMutation) int {
		return bytes.Compare(left.key, right.key)
	})
	return CapturedTransition{
		Applied: next.Applied, Term: next.LastTerm,
		BeforeOwnershipEpoch:  m.state.Binding.OwnershipEpoch,
		AfterOwnershipEpoch:   next.Binding.OwnershipEpoch,
		BeforeRoutingVersion:  m.state.Binding.RoutingVersion,
		AfterRoutingVersion:   next.Binding.RoutingVersion,
		BeforeRouteGeneration: m.state.Binding.RouteGeneration,
		AfterRouteGeneration:  next.Binding.RouteGeneration,
		PreviousEntryDigest:   m.state.LastEntryDigest,
		EntryDigest:           next.LastEntryDigest,
		BeforeDataChainDigest: m.state.DataChainDigest,
		AfterDataChainDigest:  next.DataChainDigest,
		mutations:             m.captureChanges,
	}
}

func (m *Machine) releaseCaptureChanges() {
	clear(m.captureChanges)
	m.captureChanges = m.captureChanges[:0]
}

func (m *Machine) shouldCaptureTransition(next State) bool {
	return m.capture != nil && m.initialized && m.state.Applied != math.MaxUint64 &&
		next.Applied == m.state.Applied+1
}

func validCapturedTransition(t CapturedTransition) bool {
	if t.Applied == 0 || t.Applied == math.MaxUint64 || t.Term == 0 ||
		t.Term == math.MaxUint64 || t.BeforeOwnershipEpoch == 0 ||
		t.AfterOwnershipEpoch == 0 || t.BeforeRoutingVersion == 0 ||
		t.AfterRoutingVersion == 0 || t.BeforeRouteGeneration == 0 ||
		t.AfterRouteGeneration == 0 || t.PreviousEntryDigest == ([32]byte{}) ||
		t.EntryDigest == ([32]byte{}) || t.BeforeDataChainDigest == ([32]byte{}) ||
		t.AfterDataChainDigest == ([32]byte{}) || len(t.mutations) > MaxDistinctMutations {
		return false
	}
	var previous []byte
	for i := range t.mutations {
		mutation := &t.mutations[i]
		if len(mutation.key) == 0 ||
			previous != nil && bytes.Compare(previous, mutation.key) >= 0 ||
			!mutation.beforeFound && mutation.delete {
			return false
		}
		previous = mutation.key
	}
	return true
}
