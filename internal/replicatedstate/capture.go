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
	Relation replication.RelationID
	Key      []byte
	Before   []byte
	After    []byte
}

// CapturedTransition is one consecutive replicated publication. Mutations are
// strictly ordered by relation then key. Empty transitions remain required for
// no-op, configuration, and ownership entries.
type CapturedTransition struct {
	Applied                uint64
	Term                   uint64
	BeforeOwnershipEpoch   uint64
	AfterOwnershipEpoch    uint64
	BeforeRoutingVersion   uint64
	AfterRoutingVersion    uint64
	BeforeRouteGeneration  uint64
	AfterRouteGeneration   uint64
	BeforeSchemaGeneration uint64
	AfterSchemaGeneration  uint64
	PreviousEntryDigest    [32]byte
	EntryDigest            [32]byte
	BeforeDataChainDigest  [32]byte
	AfterDataChainDigest   [32]byte
	mutations              []capturedMutation
}

// Only active capture workspace carries a relation tag. The ordinary apply
// planner's finalMutation and its allocation-free hot path remain unchanged.
type capturedMutation struct {
	finalMutation
	relation replication.RelationID
}

// MutationCount returns the exact changed-row count.
func (t CapturedTransition) MutationCount() int { return len(t.mutations) }

// Mutation returns one borrowed exact before-and-after transition.
func (t CapturedTransition) Mutation(index int) TransitionMutation {
	mutation := &t.mutations[index]
	result := TransitionMutation{Relation: mutation.relation, Key: mutation.key}
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

// RelationTransitionCapture explicitly opts into all user relations. Existing
// split encoders keep their base-relation-only byte grammar. Online schema
// builds need the complete before/after stream, including independently
// maintained index relations; they must persist each mutation's Relation.
// The result must be immutable for the lifetime of the capture.
type RelationTransitionCapture interface {
	TransitionCapture
	CaptureAllRelations() bool
}

// StoppableTransitionCapture may stop recording only after publishing a durable
// terminal record. A recovered terminal capture reports the same state from
// Begin. Stopping capture does not certify a schema target: consumers must
// distinguish successful closure from an aborted build in their own grammar.
type StoppableTransitionCapture interface {
	TransitionCapture
	CaptureStopped() bool
}

// FinishingTransitionCapture seals a private build stream at an exact source
// cut. This is replica-local artifact metadata, not a schema activation or a
// distributed write fence. The coordinator must fence new writes separately
// and certify/activate the target at the same expected applied index.
type FinishingTransitionCapture interface {
	StoppableTransitionCapture
	Finish(State, func(key, value []byte) error) error
}

func (m *Machine) FinishTransitionCapture(capture FinishingTransitionCapture, expectedApplied uint64) error {
	if m == nil || capture == nil || expectedApplied == 0 {
		return ErrTransitionCapture
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkUsable(); err != nil {
		return err
	}
	if m.capture != capture || m.state.Applied != expectedApplied || capture.Target() != m.captureTarget {
		return ErrTransitionCapture
	}
	published := false
	finishErr := capture.Finish(cloneState(m.state), func(key, value []byte) error {
		if published || len(key) == 0 || len(value) == 0 {
			return ErrTransitionCapture
		}
		published = true
		write := func(batch *durable.DatabaseBatch) error {
			collection, err := batch.CollectionHandle(m.captureTarget.Collection)
			if err != nil {
				return err
			}
			return collection.Put(key, value)
		}
		members := []durable.NamedCollection{{Name: m.captureTarget.Name, Collection: m.captureTarget.Collection, BatchDocumentsHint: 1}}
		var err error
		if m.checkpointGroup != nil {
			err = m.checkpointGroup.Update(m.state.Applied, members, m.options.TxnLimits, write)
		} else {
			err = durable.UpdateCollections(m.txnLog, members, m.options.TxnLimits, write)
		}
		if err != nil {
			m.poison = err
		}
		return err
	})
	if finishErr != nil {
		if published {
			m.poison = finishErr
		}
		return errors.Join(ErrTransitionCapture, finishErr)
	}
	if !published || !capture.CaptureStopped() {
		m.poison = ErrTransitionCapture
		return ErrTransitionCapture
	}
	m.capture, m.captureBuffer, m.captureChanges = nil, nil, nil
	return nil
}

func captureStopped(capture TransitionCapture) bool {
	stoppable, ok := capture.(StoppableTransitionCapture)
	return ok && stoppable.CaptureStopped()
}

func capturesAllRelations(capture TransitionCapture) bool {
	all, ok := capture.(RelationTransitionCapture)
	return ok && all.CaptureAllRelations()
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
	if captureStopped(capture) {
		m.capture = nil
		m.captureBuffer = nil
		m.captureChanges = nil
	}
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
	for i := range m.relations {
		if target.Name == m.relations[i].name || target.Collection == m.relations[i].target.Collection {
			return ErrTransitionCapture
		}
	}
	limits := m.captureCollectionLimits(capture)
	requiredDocuments, err := RequiredBundleTransactionDocuments(
		limits.MaxDistinctMutations, m.options.RetryWindow, true,
	)
	if err != nil || m.options.TxnLimits.MaxCollections < len(m.relations)+2 ||
		m.options.TxnLimits.MaxDocuments < requiredDocuments {
		return fmt.Errorf("%w: transaction dimensions", ErrTransitionCapture)
	}
	maxBefore := uint64(limits.MaxDistinctMutations) *
		uint64(limits.MaxDocumentBytes)
	// Keys and after images share both the durable batch envelope and the
	// narrower replicated command envelope; their independent maxima cannot
	// occur in one admitted transition.
	maxPayload := uint64(min(limits.MaxBatchBytes, replication.MaxCommandBytes))
	possiblePayload := uint64(limits.MaxDistinctMutations) *
		uint64(limits.MaxDocumentBytes+limits.MaxKeyBytes)
	maxPayload = min(maxPayload, possiblePayload)
	maxAfter := min(maxPayload, uint64(limits.MaxDistinctMutations)*
		uint64(limits.MaxDocumentBytes))
	maxKeys := maxPayload - maxAfter
	if maxKeys > uint64(limits.MaxDistinctMutations)*uint64(limits.MaxKeyBytes) {
		return fmt.Errorf("%w: record payload dimensions", ErrTransitionCapture)
	}
	maxRecord, err := capture.MaxEncodedBytes(TransitionCaptureBounds{
		Transitions: uint64(limits.MaxDistinctMutations),
		KeyBytes:    maxKeys, BeforeBytes: maxBefore, AfterBytes: maxAfter,
	})
	if err != nil || maxRecord <= 0 || maxRecord > target.Collection.MaxDocumentBytes() ||
		maxRecord > target.Collection.MaxBatchBytes()-8 {
		return fmt.Errorf("%w: record capacity", ErrTransitionCapture)
	}
	baseBytes, ok := checkedTxnBytes(
		min(limits.MaxBatchBytes, replication.MaxCommandBytes),
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

func (m *Machine) captureCollectionLimits(capture TransitionCapture) CollectionLimits {
	if !capturesAllRelations(capture) {
		return m.user.Limits
	}
	var limits CollectionLimits
	for i := range m.relations {
		l := m.relations[i].target.Limits
		limits.MaxDistinctMutations = min(replication.MaxMutations, limits.MaxDistinctMutations+l.MaxDistinctMutations)
		limits.MaxDocumentBytes = max(limits.MaxDocumentBytes, l.MaxDocumentBytes)
		limits.MaxKeyBytes = max(limits.MaxKeyBytes, l.MaxKeyBytes)
		limits.MaxBatchBytes += min(replication.MaxCommandBytes-limits.MaxBatchBytes, l.MaxBatchBytes)
	}
	return limits
}

func maxSystemTransitionBytes(limits CollectionLimits) int {
	return limits.MaxBatchBytes
}

func (m *Machine) capturedTransition(next State, changes []finalMutation, spans []plannedRelationChanges) CapturedTransition {
	if !capturesAllRelations(m.capture) {
		changes = baseRelationChanges(changes, spans)
		spans = nil
	} else if len(changes) != 0 && len(spans) == 0 {
		return CapturedTransition{}
	}
	if cap(m.captureChanges) < len(changes) {
		m.captureChanges = make([]capturedMutation, len(changes))
	} else {
		m.captureChanges = m.captureChanges[:len(changes)]
	}
	for i := range changes {
		m.captureChanges[i] = capturedMutation{finalMutation: changes[i], relation: 1}
	}
	compareKeys := func(left, right capturedMutation) int {
		return bytes.Compare(left.key, right.key)
	}
	if len(spans) == 0 {
		slices.SortFunc(m.captureChanges, compareKeys)
	}
	var end uint32
	for i, span := range spans {
		if span.start != end || span.end < span.start || uint64(span.end) > uint64(len(changes)) ||
			int(span.ordinal) >= len(m.relations) || i != 0 && span.ordinal <= spans[i-1].ordinal {
			return CapturedTransition{}
		}
		for i := span.start; i < span.end; i++ {
			m.captureChanges[i].relation = m.relations[span.ordinal].id
		}
		// The planner already orders relation spans. Sort only within each
		// bounded relation, not the entire multi-relation command.
		slices.SortFunc(m.captureChanges[span.start:span.end], compareKeys)
		end = span.end
	}
	if len(spans) != 0 && int(end) != len(changes) {
		return CapturedTransition{}
	}
	return CapturedTransition{
		Applied: next.Applied, Term: next.LastTerm,
		BeforeOwnershipEpoch:   m.state.Binding.OwnershipEpoch,
		AfterOwnershipEpoch:    next.Binding.OwnershipEpoch,
		BeforeRoutingVersion:   m.state.Binding.RoutingVersion,
		AfterRoutingVersion:    next.Binding.RoutingVersion,
		BeforeRouteGeneration:  m.state.Binding.RouteGeneration,
		AfterRouteGeneration:   next.Binding.RouteGeneration,
		BeforeSchemaGeneration: m.state.Binding.SchemaGeneration,
		AfterSchemaGeneration:  next.Binding.SchemaGeneration,
		PreviousEntryDigest:    m.state.LastEntryDigest,
		EntryDigest:            next.LastEntryDigest,
		BeforeDataChainDigest:  m.state.DataChainDigest,
		AfterDataChainDigest:   next.DataChainDigest,
		mutations:              m.captureChanges,
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
		t.AfterDataChainDigest == ([32]byte{}) || len(t.mutations) > replication.MaxMutations {
		return false
	}
	var previous []byte
	var relation replication.RelationID
	var count int
	for i := range t.mutations {
		mutation := &t.mutations[i]
		if mutation.relation != relation {
			if mutation.relation <= relation || mutation.relation > replication.MaxRelationID {
				return false
			}
			relation, previous, count = mutation.relation, nil, 0
		}
		count++
		if len(mutation.key) == 0 ||
			count > MaxDistinctMutations || relation == 0 ||
			previous != nil && bytes.Compare(previous, mutation.key) >= 0 ||
			!mutation.beforeFound && mutation.delete {
			return false
		}
		previous = mutation.key
	}
	return true
}
