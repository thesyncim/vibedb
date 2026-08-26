package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/shardservice"
)

var errDurableFaultInjected = errors.New("durable request fault injected")

type durableFaultOperation uint8

const (
	durableFaultLookup durableFaultOperation = iota + 1
	durableFaultCreate
	durableFaultAppendPage
	durableFaultSeal
	durableFaultLoadPage
	durableFaultPutPending
	durableFaultAdvance
	durableFaultComplete
	durableFaultAcknowledge
)

type durableFaultTiming uint8

const (
	durableFaultBefore durableFaultTiming = iota + 1
	durableFaultAfter
)

type durableFaultRule struct {
	mu        sync.Mutex
	operation durableFaultOperation
	timing    durableFaultTiming
	skip      int
	remaining int
	crashed   bool
}

func (rule *durableFaultRule) arm(
	operation durableFaultOperation,
	timing durableFaultTiming,
) {
	rule.armNth(operation, timing, 1)
}

func (rule *durableFaultRule) armNth(
	operation durableFaultOperation,
	timing durableFaultTiming,
	nth int,
) {
	rule.mu.Lock()
	rule.operation = operation
	rule.timing = timing
	rule.skip = max(nth-1, 0)
	rule.remaining = 1
	rule.crashed = false
	rule.mu.Unlock()
}

func (rule *durableFaultRule) clear() {
	rule.mu.Lock()
	rule.operation = 0
	rule.timing = 0
	rule.skip = 0
	rule.remaining = 0
	rule.crashed = false
	rule.mu.Unlock()
}

func (rule *durableFaultRule) fires(
	operation durableFaultOperation,
	timing durableFaultTiming,
) bool {
	rule.mu.Lock()
	defer rule.mu.Unlock()
	if rule.operation != operation || rule.timing != timing || rule.remaining == 0 {
		return false
	}
	if rule.skip != 0 {
		rule.skip--
		return false
	}
	rule.remaining--
	rule.crashed = true
	return true
}

func (rule *durableFaultRule) blocked() bool {
	rule.mu.Lock()
	defer rule.mu.Unlock()
	return rule.crashed
}

type durableFaultStoredKey struct {
	home         replication.Digest
	scope        requestledger.ScopeKind
	principal    requestledger.PrincipalID
	tenantDigest requestledger.Digest
	requestID    requestledger.RequestID
}

type durableFaultStoredEntry struct {
	entry DurableRequestLedgerEntry
	pages map[uint32][]byte
}

// durableFaultMemoryLedger models revision-CAS and exact retry behavior. The
// faulting wrapper below can lose either side of any mutator response without
// weakening the durable state observed by a replacement executor.
type durableFaultMemoryLedger struct {
	mu      sync.Mutex
	entries map[durableFaultStoredKey]*durableFaultStoredEntry
}

func newDurableFaultMemoryLedger() *durableFaultMemoryLedger {
	return &durableFaultMemoryLedger{
		entries: make(map[durableFaultStoredKey]*durableFaultStoredEntry),
	}
}

func durableFaultKey(
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
) durableFaultStoredKey {
	return durableFaultStoredKey{
		home: home.Identity, scope: key.RequestKey.Scope, principal: key.RequestKey.Principal,
		tenantDigest: key.RequestKey.TenantDigest, requestID: key.RequestKey.Request,
	}
}

func cloneDurableFaultDescriptor(value DurableRequestPlanDescriptor) DurableRequestPlanDescriptor {
	value.Inline = bytes.Clone(value.Inline)
	return value
}

func cloneDurableFaultTerminal(value DurableRequestTerminal) DurableRequestTerminal {
	value.Result = bytes.Clone(value.Result)
	return value
}

func cloneDurableFaultEntry(value DurableRequestLedgerEntry) DurableRequestLedgerEntry {
	value.Plan = cloneDurableFaultDescriptor(value.Plan)
	value.Pending = cloneDurableRequestPending(value.Pending)
	value.Progress = bytes.Clone(value.Progress)
	value.Terminal = cloneDurableFaultTerminal(value.Terminal)
	return value
}

func (ledger *durableFaultMemoryLedger) lookupLocked(
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
) (*durableFaultStoredEntry, error) {
	stored := ledger.entries[durableFaultKey(home, key)]
	if stored != nil && stored.entry.Digest != key.Digest {
		return nil, ErrDurableRequestConflict
	}
	return stored, nil
}

func (ledger *durableFaultMemoryLedger) Lookup(
	_ context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
) (DurableRequestLedgerEntry, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stored, err := ledger.lookupLocked(home, key)
	if err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	if stored == nil {
		return DurableRequestLedgerEntry{State: DurableRequestLedgerAbsent}, nil
	}
	return cloneDurableFaultEntry(stored.entry), nil
}

func (ledger *durableFaultMemoryLedger) create(
	_ context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	generation uint64,
	descriptor DurableRequestPlanDescriptor,
	sealed bool,
) (DurableRequestLedgerEntry, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stored, err := ledger.lookupLocked(home, key)
	if err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	if stored != nil {
		if stored.entry.Generation != generation ||
			!equalDurableRequestDescriptor(stored.entry.Plan, descriptor) {
			return DurableRequestLedgerEntry{}, ErrDurableRequestConflict
		}
		return cloneDurableFaultEntry(stored.entry), nil
	}
	state := DurableRequestLedgerCreating
	if sealed {
		state = DurableRequestLedgerSealed
	}
	stored = &durableFaultStoredEntry{
		entry: DurableRequestLedgerEntry{
			State: state, Revision: 1,
			Digest: key.Digest, Plan: cloneDurableFaultDescriptor(descriptor),
			Generation: generation,
		},
		pages: make(map[uint32][]byte),
	}
	ledger.entries[durableFaultKey(home, key)] = stored
	return cloneDurableFaultEntry(stored.entry), nil
}

func (ledger *durableFaultMemoryLedger) CreatePlanning(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	generation uint64,
	descriptor DurableRequestPlanDescriptor,
) (DurableRequestLedgerEntry, error) {
	return ledger.create(ctx, home, key, generation, descriptor, false)
}

func (ledger *durableFaultMemoryLedger) CreateSealed(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	generation uint64,
	descriptor DurableRequestPlanDescriptor,
) (DurableRequestLedgerEntry, error) {
	return ledger.create(ctx, home, key, generation, descriptor, true)
}

func (ledger *durableFaultMemoryLedger) AppendPlanPage(
	_ context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	ordinal uint32,
	page []byte,
	seal bool,
) (DurableRequestLedgerEntry, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stored, err := ledger.lookupLocked(home, key)
	if err != nil || stored == nil {
		return DurableRequestLedgerEntry{}, errors.Join(err, ErrDurableRequestUnresolved)
	}
	if prior, ok := stored.pages[ordinal]; ok {
		if !bytes.Equal(prior, page) {
			return DurableRequestLedgerEntry{}, ErrDurableRequestConflict
		}
		return cloneDurableFaultEntry(stored.entry), nil
	}
	if stored.entry.State != DurableRequestLedgerCreating ||
		stored.entry.Revision != expected || ordinal != uint32(len(stored.pages)) ||
		ordinal >= stored.entry.Plan.PageCount || len(page) == 0 ||
		len(page) > DurableRequestPlanPageBytes {
		return DurableRequestLedgerEntry{}, ErrDurableRequestUnresolved
	}
	stored.pages[ordinal] = bytes.Clone(page)
	stored.entry.Revision++
	stored.entry.AppendedPageCount++
	if seal {
		if ordinal+1 != stored.entry.Plan.PageCount {
			return DurableRequestLedgerEntry{}, ErrDurableRequestUnresolved
		}
		stored.entry.State = DurableRequestLedgerSealed
	}
	return cloneDurableFaultEntry(stored.entry), nil
}

func (ledger *durableFaultMemoryLedger) SealPlan(
	_ context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
) (DurableRequestLedgerEntry, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stored, err := ledger.lookupLocked(home, key)
	if err != nil || stored == nil {
		return DurableRequestLedgerEntry{}, errors.Join(err, ErrDurableRequestUnresolved)
	}
	if stored.entry.State >= DurableRequestLedgerSealed {
		return cloneDurableFaultEntry(stored.entry), nil
	}
	if stored.entry.State != DurableRequestLedgerCreating ||
		stored.entry.Revision != expected ||
		uint32(len(stored.pages)) != stored.entry.Plan.PageCount {
		return DurableRequestLedgerEntry{}, ErrDurableRequestUnresolved
	}
	stored.entry.State = DurableRequestLedgerSealed
	stored.entry.Revision++
	return cloneDurableFaultEntry(stored.entry), nil
}

func (ledger *durableFaultMemoryLedger) LoadPlanPage(
	_ context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	ordinal uint32,
) ([]byte, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stored, err := ledger.lookupLocked(home, key)
	if err != nil || stored == nil || stored.entry.State < DurableRequestLedgerCreating {
		return nil, errors.Join(err, ErrDurableRequestUnresolved)
	}
	page := stored.pages[ordinal]
	if len(page) == 0 {
		return nil, ErrDurableRequestUnresolved
	}
	return bytes.Clone(page), nil
}

func (ledger *durableFaultMemoryLedger) PutPending(
	_ context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	pending DurableRequestPending,
) (DurableRequestLedgerEntry, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stored, err := ledger.lookupLocked(home, key)
	if err != nil || stored == nil {
		return DurableRequestLedgerEntry{}, errors.Join(err, ErrDurableRequestUnresolved)
	}
	if stored.entry.State == DurableRequestLedgerPending {
		if !equalDurableRequestPending(stored.entry.Pending, pending) {
			return DurableRequestLedgerEntry{}, ErrDurableRequestConflict
		}
		return cloneDurableFaultEntry(stored.entry), nil
	}
	if stored.entry.State != DurableRequestLedgerSealed ||
		stored.entry.Revision != expected || pending.StepRevision != expected+1 ||
		len(pending.Target) == 0 || len(pending.Command) == 0 {
		return DurableRequestLedgerEntry{}, ErrDurableRequestUnresolved
	}
	stored.entry.State = DurableRequestLedgerPending
	stored.entry.Revision++
	stored.entry.Pending = cloneDurableRequestPending(pending)
	return cloneDurableFaultEntry(stored.entry), nil
}

func (ledger *durableFaultMemoryLedger) Advance(
	_ context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	stepRevision uint64,
	progress []byte,
) (DurableRequestLedgerEntry, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stored, err := ledger.lookupLocked(home, key)
	if err != nil || stored == nil {
		return DurableRequestLedgerEntry{}, errors.Join(err, ErrDurableRequestUnresolved)
	}
	if stored.entry.State == DurableRequestLedgerSealed &&
		stored.entry.Revision == expected+1 && stored.entry.SettledStepRevision == stepRevision &&
		stored.entry.ProgressDigest == replication.Digest(requestledger.ObservationDigest(progress)) &&
		bytes.Equal(stored.entry.Progress, progress) {
		return cloneDurableFaultEntry(stored.entry), nil
	}
	if stored.entry.State != DurableRequestLedgerPending ||
		stored.entry.Revision != expected ||
		stored.entry.Pending.StepRevision != stepRevision || len(progress) == 0 {
		return DurableRequestLedgerEntry{}, ErrDurableRequestUnresolved
	}
	stored.entry.State = DurableRequestLedgerSealed
	stored.entry.Revision++
	stored.entry.Pending = DurableRequestPending{}
	stored.entry.Progress = bytes.Clone(progress)
	stored.entry.SettledStepRevision = stepRevision
	stored.entry.ProgressDigest = replication.Digest(requestledger.ObservationDigest(progress))
	return cloneDurableFaultEntry(stored.entry), nil
}

func (ledger *durableFaultMemoryLedger) Complete(
	_ context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	terminal DurableRequestTerminal,
) (DurableRequestLedgerEntry, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stored, err := ledger.lookupLocked(home, key)
	if err != nil || stored == nil {
		return DurableRequestLedgerEntry{}, errors.Join(err, ErrDurableRequestUnresolved)
	}
	if stored.entry.State >= DurableRequestLedgerTerminal {
		if !equalDurableRequestTerminal(stored.entry.Terminal, terminal) {
			return DurableRequestLedgerEntry{}, ErrDurableRequestConflict
		}
		return cloneDurableFaultEntry(stored.entry), nil
	}
	if stored.entry.State != DurableRequestLedgerSealed ||
		stored.entry.Revision != expected {
		return DurableRequestLedgerEntry{}, ErrDurableRequestUnresolved
	}
	stored.entry.State = DurableRequestLedgerTerminal
	stored.entry.Revision++
	stored.entry.Terminal = cloneDurableFaultTerminal(terminal)
	tokenHash := sha256.New()
	_, _ = tokenHash.Write([]byte("vibedb/test/durable-request/ack-token\x00"))
	_, _ = tokenHash.Write(key.Digest[:])
	_, _ = tokenHash.Write(stored.entry.Plan.Root[:])
	var revision [8]byte
	binary.LittleEndian.PutUint64(revision[:], stored.entry.Revision)
	_, _ = tokenHash.Write(revision[:])
	resultDigest := requestledger.ResultDigest(terminal.Result)
	_, _ = tokenHash.Write(resultDigest[:])
	_ = tokenHash.Sum(stored.entry.AckToken[:0])
	return cloneDurableFaultEntry(stored.entry), nil
}

func (ledger *durableFaultMemoryLedger) Acknowledge(
	_ context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	terminalRevision uint64,
	resultDigest replication.Digest,
	token DurableRequestAckToken,
) (DurableRequestLedgerEntry, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stored, err := ledger.lookupLocked(home, key)
	if err != nil || stored == nil {
		return DurableRequestLedgerEntry{}, errors.Join(err, ErrDurableRequestUnresolved)
	}
	if stored.entry.State == DurableRequestLedgerAcked {
		if stored.entry.AckTerminalRevision != terminalRevision ||
			stored.entry.AckResultDigest != resultDigest ||
			stored.entry.AckTokenDigest != durableRequestAckTokenDigest(token) {
			return DurableRequestLedgerEntry{}, ErrDurableRequestConflict
		}
		return cloneDurableFaultEntry(stored.entry), nil
	}
	if stored.entry.State != DurableRequestLedgerTerminal ||
		stored.entry.Revision != expected || terminalRevision != expected ||
		resultDigest != replication.Digest(requestledger.ResultDigest(stored.entry.Terminal.Result)) {
		return DurableRequestLedgerEntry{}, ErrDurableRequestUnresolved
	}
	if stored.entry.AckToken == (DurableRequestAckToken{}) {
		return DurableRequestLedgerEntry{}, ErrDurableRequestUnresolved
	}
	if token != stored.entry.AckToken {
		return DurableRequestLedgerEntry{}, ErrDurableRequestConflict
	}
	var ackInput [64]byte
	copy(ackInput[:32], stored.entry.Digest[:])
	copy(ackInput[32:], resultDigest[:])
	ack := replication.Digest(sha256.Sum256(ackInput[:]))
	stored.entry.State = DurableRequestLedgerAcked
	stored.entry.Revision++
	stored.entry.AckDigest = ack
	stored.entry.AckTerminalRevision = terminalRevision
	stored.entry.AckResultDigest = resultDigest
	stored.entry.AckTokenDigest = durableRequestAckTokenDigest(token)
	stored.entry.AckPlanRoot = stored.entry.Plan.Root
	stored.entry.AckTerminalContractDigest = stored.entry.Plan.Contract.TerminalContractDigest
	stored.entry.AckToken = DurableRequestAckToken{}
	stored.entry.Plan = DurableRequestPlanDescriptor{}
	stored.entry.Pending = DurableRequestPending{}
	stored.entry.Progress = nil
	stored.entry.Terminal = DurableRequestTerminal{}
	stored.pages = make(map[uint32][]byte)
	return cloneDurableFaultEntry(stored.entry), nil
}

type durableFaultLedger struct {
	base  *durableFaultMemoryLedger
	rule  *durableFaultRule
	stale *durableStaleHomeRule
	mu    sync.Mutex
	calls [durableFaultAcknowledge + 1]int
}

type durableCapacityLedger struct {
	*durableFaultLedger
	mu       sync.Mutex
	full     bool
	rejected int
}

type durableAheadLedger struct {
	*durableFaultLedger
	mu        sync.Mutex
	operation durableFaultOperation
	lost      bool
	applied   chan struct{}
	release   chan struct{}
}

func (ledger *durableAheadLedger) loseResponseAfter(
	operation durableFaultOperation,
) bool {
	ledger.mu.Lock()
	if ledger.operation != operation || ledger.lost {
		ledger.mu.Unlock()
		return false
	}
	ledger.lost = true
	close(ledger.applied)
	ledger.mu.Unlock()
	<-ledger.release
	return true
}

func (ledger *durableAheadLedger) AppendPlanPage(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	ordinal uint32,
	page []byte,
	seal bool,
) (DurableRequestLedgerEntry, error) {
	entry, err := ledger.durableFaultLedger.AppendPlanPage(
		ctx, home, key, expected, ordinal, page, seal,
	)
	if err == nil && ledger.loseResponseAfter(durableFaultAppendPage) {
		return DurableRequestLedgerEntry{}, errDurableFaultInjected
	}
	return entry, err
}

func (ledger *durableAheadLedger) PutPending(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	pending DurableRequestPending,
) (DurableRequestLedgerEntry, error) {
	entry, err := ledger.durableFaultLedger.PutPending(ctx, home, key, expected, pending)
	if err == nil && ledger.loseResponseAfter(durableFaultPutPending) {
		return DurableRequestLedgerEntry{}, errDurableFaultInjected
	}
	return entry, err
}

func (ledger *durableAheadLedger) Advance(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	stepRevision uint64,
	progress []byte,
) (DurableRequestLedgerEntry, error) {
	entry, err := ledger.durableFaultLedger.Advance(
		ctx, home, key, expected, stepRevision, progress,
	)
	if err == nil && ledger.loseResponseAfter(durableFaultAdvance) {
		return DurableRequestLedgerEntry{}, errDurableFaultInjected
	}
	return entry, err
}

func (ledger *durableCapacityLedger) reject() bool {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.full {
		ledger.rejected++
		return true
	}
	return false
}

func (ledger *durableCapacityLedger) setFull(full bool) {
	ledger.mu.Lock()
	ledger.full = full
	ledger.mu.Unlock()
}

func (ledger *durableCapacityLedger) rejectionCount() int {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.rejected
}

func (ledger *durableCapacityLedger) CreatePlanning(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	generation uint64,
	descriptor DurableRequestPlanDescriptor,
) (DurableRequestLedgerEntry, error) {
	if ledger.reject() {
		return DurableRequestLedgerEntry{State: DurableRequestLedgerAbsent},
			ErrDurableRequestCapacity
	}
	return ledger.durableFaultLedger.CreatePlanning(
		ctx, home, key, generation, descriptor,
	)
}

func (ledger *durableCapacityLedger) CreateSealed(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	generation uint64,
	descriptor DurableRequestPlanDescriptor,
) (DurableRequestLedgerEntry, error) {
	if ledger.reject() {
		return DurableRequestLedgerEntry{State: DurableRequestLedgerAbsent},
			ErrDurableRequestCapacity
	}
	return ledger.durableFaultLedger.CreateSealed(
		ctx, home, key, generation, descriptor,
	)
}

type durableStaleHomeRule struct {
	mu        sync.Mutex
	operation durableFaultOperation
	holder    *DurableRequestLedgerTopologyHolder
	next      DurableRequestLedgerTopology
	published bool
}

func (rule *durableStaleHomeRule) reject(
	operation durableFaultOperation,
	home DurableRequestLedgerHome,
) error {
	if rule == nil || operation != rule.operation {
		return nil
	}
	rule.mu.Lock()
	defer rule.mu.Unlock()
	if !rule.published {
		if err := rule.holder.Publish(rule.next); err != nil {
			return err
		}
		rule.published = true
	}
	if home.TopologyGeneration < rule.next.Generation {
		return ErrDurableRequestStaleHome
	}
	return nil
}

func (ledger *durableFaultLedger) record(operation durableFaultOperation) {
	ledger.mu.Lock()
	ledger.calls[operation]++
	ledger.mu.Unlock()
}

func (ledger *durableFaultLedger) callCount(operation durableFaultOperation) int {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.calls[operation]
}

func durableFaultReturn[T any](
	rule *durableFaultRule,
	operation durableFaultOperation,
	apply func() (T, error),
) (T, error) {
	var zero T
	if rule.blocked() {
		return zero, errDurableFaultInjected
	}
	if rule.fires(operation, durableFaultBefore) {
		return zero, errDurableFaultInjected
	}
	value, err := apply()
	if err == nil && rule.fires(operation, durableFaultAfter) {
		return zero, errDurableFaultInjected
	}
	return value, err
}

func (ledger *durableFaultLedger) Lookup(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
) (DurableRequestLedgerEntry, error) {
	ledger.record(durableFaultLookup)
	if err := ledger.stale.reject(durableFaultLookup, home); err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	return durableFaultReturn(ledger.rule, durableFaultLookup, func() (DurableRequestLedgerEntry, error) {
		return ledger.base.Lookup(ctx, home, key)
	})
}

func (ledger *durableFaultLedger) CreatePlanning(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	generation uint64,
	descriptor DurableRequestPlanDescriptor,
) (DurableRequestLedgerEntry, error) {
	ledger.record(durableFaultCreate)
	if err := ledger.stale.reject(durableFaultCreate, home); err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	return durableFaultReturn(ledger.rule, durableFaultCreate, func() (DurableRequestLedgerEntry, error) {
		return ledger.base.CreatePlanning(ctx, home, key, generation, descriptor)
	})
}

func (ledger *durableFaultLedger) CreateSealed(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	generation uint64,
	descriptor DurableRequestPlanDescriptor,
) (DurableRequestLedgerEntry, error) {
	ledger.record(durableFaultSeal)
	if err := ledger.stale.reject(durableFaultSeal, home); err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	return durableFaultReturn(ledger.rule, durableFaultSeal, func() (DurableRequestLedgerEntry, error) {
		return ledger.base.CreateSealed(ctx, home, key, generation, descriptor)
	})
}

func (ledger *durableFaultLedger) AppendPlanPage(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	ordinal uint32,
	page []byte,
	seal bool,
) (DurableRequestLedgerEntry, error) {
	operation := durableFaultAppendPage
	if seal {
		operation = durableFaultSeal
	}
	ledger.record(operation)
	if err := ledger.stale.reject(operation, home); err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	return durableFaultReturn(ledger.rule, operation, func() (DurableRequestLedgerEntry, error) {
		return ledger.base.AppendPlanPage(ctx, home, key, expected, ordinal, page, seal)
	})
}

func (ledger *durableFaultLedger) SealPlan(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
) (DurableRequestLedgerEntry, error) {
	return durableFaultReturn(ledger.rule, durableFaultSeal, func() (DurableRequestLedgerEntry, error) {
		return ledger.base.SealPlan(ctx, home, key, expected)
	})
}

func (ledger *durableFaultLedger) LoadPlanPage(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	ordinal uint32,
) ([]byte, error) {
	ledger.record(durableFaultLoadPage)
	if err := ledger.stale.reject(durableFaultLoadPage, home); err != nil {
		return nil, err
	}
	return durableFaultReturn(ledger.rule, durableFaultLoadPage, func() ([]byte, error) {
		return ledger.base.LoadPlanPage(ctx, home, key, ordinal)
	})
}

func (ledger *durableFaultLedger) PutPending(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	pending DurableRequestPending,
) (DurableRequestLedgerEntry, error) {
	ledger.record(durableFaultPutPending)
	if err := ledger.stale.reject(durableFaultPutPending, home); err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	return durableFaultReturn(ledger.rule, durableFaultPutPending, func() (DurableRequestLedgerEntry, error) {
		return ledger.base.PutPending(ctx, home, key, expected, pending)
	})
}

func (ledger *durableFaultLedger) Advance(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	stepRevision uint64,
	progress []byte,
) (DurableRequestLedgerEntry, error) {
	ledger.record(durableFaultAdvance)
	if err := ledger.stale.reject(durableFaultAdvance, home); err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	return durableFaultReturn(ledger.rule, durableFaultAdvance, func() (DurableRequestLedgerEntry, error) {
		return ledger.base.Advance(ctx, home, key, expected, stepRevision, progress)
	})
}

func (ledger *durableFaultLedger) Complete(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	terminal DurableRequestTerminal,
) (DurableRequestLedgerEntry, error) {
	ledger.record(durableFaultComplete)
	if err := ledger.stale.reject(durableFaultComplete, home); err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	return durableFaultReturn(ledger.rule, durableFaultComplete, func() (DurableRequestLedgerEntry, error) {
		return ledger.base.Complete(ctx, home, key, expected, terminal)
	})
}

func (ledger *durableFaultLedger) Acknowledge(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	expected uint64,
	terminalRevision uint64,
	resultDigest replication.Digest,
	token DurableRequestAckToken,
) (DurableRequestLedgerEntry, error) {
	ledger.record(durableFaultAcknowledge)
	if err := ledger.stale.reject(durableFaultAcknowledge, home); err != nil {
		return DurableRequestLedgerEntry{}, err
	}
	return durableFaultReturn(ledger.rule, durableFaultAcknowledge, func() (DurableRequestLedgerEntry, error) {
		return ledger.base.Acknowledge(
			ctx, home, key, expected, terminalRevision, resultDigest, token,
		)
	})
}

type durableRunnerFaultTiming uint8

const (
	durableRunnerFaultBeforeSend durableRunnerFaultTiming = iota + 1
	durableRunnerFaultAfterApply
)

type durableRunnerFaultRule struct {
	mu      sync.Mutex
	step    int
	timing  durableRunnerFaultTiming
	armed   bool
	crashed bool
}

func (rule *durableRunnerFaultRule) arm(step int, timing durableRunnerFaultTiming) {
	rule.mu.Lock()
	rule.step = step
	rule.timing = timing
	rule.armed = true
	rule.crashed = false
	rule.mu.Unlock()
}

func (rule *durableRunnerFaultRule) clear() {
	rule.mu.Lock()
	rule.armed = false
	rule.crashed = false
	rule.mu.Unlock()
}

func (rule *durableRunnerFaultRule) fire(step int, timing durableRunnerFaultTiming) bool {
	rule.mu.Lock()
	defer rule.mu.Unlock()
	if rule.crashed {
		return true
	}
	if !rule.armed || rule.step != step || rule.timing != timing {
		return false
	}
	rule.armed = false
	rule.crashed = true
	return true
}

type durableRunnerStep struct {
	target  []byte
	command []byte
}

type durableRunnerData struct {
	mu           sync.Mutex
	attempts     [][]byte
	applications map[string]int
}

func newDurableRunnerData() *durableRunnerData {
	return &durableRunnerData{applications: make(map[string]int)}
}

func (data *durableRunnerData) dispatch(target, command []byte) {
	data.mu.Lock()
	defer data.mu.Unlock()
	exact := make([]byte, 0, len(target)+1+len(command))
	exact = append(exact, target...)
	exact = append(exact, 0)
	exact = append(exact, command...)
	data.attempts = append(data.attempts, bytes.Clone(exact))
	data.applications[string(exact)] = 1
}

func (data *durableRunnerData) snapshot() (attempts [][]byte, applications map[string]int) {
	data.mu.Lock()
	defer data.mu.Unlock()
	attempts = make([][]byte, len(data.attempts))
	for index := range data.attempts {
		attempts[index] = bytes.Clone(data.attempts[index])
	}
	applications = make(map[string]int, len(data.applications))
	for key, value := range data.applications {
		applications[key] = value
	}
	return attempts, applications
}

type durableFaultRunner struct {
	steps []durableRunnerStep
	rule  *durableRunnerFaultRule
	data  *durableRunnerData
}

type durableParticipantProofRunner struct {
	data *durableRunnerData
}

type durableStaticTerminalRunner struct {
	terminal DurableRequestTerminal
}

func durableRunnerProgress(next int) []byte {
	var progress [8]byte
	binary.LittleEndian.PutUint64(progress[:], uint64(next))
	return progress[:]
}

func openDurableRunnerProgress(progress []byte, maximum int) (int, error) {
	if len(progress) == 0 {
		return 0, nil
	}
	if len(progress) != 8 {
		return 0, ErrDurableRequestUnresolved
	}
	next := binary.LittleEndian.Uint64(progress)
	if next > uint64(maximum) {
		return 0, ErrDurableRequestUnresolved
	}
	return int(next), nil
}

func (runner *durableFaultRunner) Run(
	ctx context.Context,
	recipe DurableRequestRecipe,
	journal DurableRequestStepJournal,
) (DurableRequestTerminal, error) {
	if runner == nil || journal == nil || recipe.ParticipantStream == nil ||
		recipe.ParticipantCount == 0 {
		return DurableRequestTerminal{}, ErrDurableRequest
	}
	var participants uint64
	for recipe.ParticipantStream.Next() {
		_ = recipe.ParticipantStream.Current()
		participants++
	}
	if err := recipe.ParticipantStream.Err(); err != nil {
		return DurableRequestTerminal{}, err
	}
	if !recipe.ParticipantStream.Complete() ||
		participants != recipe.ParticipantCount || participants > math.MaxInt64 {
		return DurableRequestTerminal{}, ErrDurableRequestConflict
	}
	next, err := openDurableRunnerProgress(recipe.Progress, len(runner.steps))
	if err != nil {
		return DurableRequestTerminal{}, err
	}
	if len(recipe.Pending.Command) != 0 {
		if next >= len(runner.steps) ||
			!bytes.Equal(recipe.Pending.Target, runner.steps[next].target) ||
			!bytes.Equal(recipe.Pending.Command, runner.steps[next].command) {
			return DurableRequestTerminal{}, ErrDurableRequestConflict
		}
		if runner.rule.fire(next, durableRunnerFaultBeforeSend) {
			return DurableRequestTerminal{}, errDurableFaultInjected
		}
		runner.data.dispatch(recipe.Pending.Target, recipe.Pending.Command)
		if runner.rule.fire(next, durableRunnerFaultAfterApply) {
			return DurableRequestTerminal{}, errDurableFaultInjected
		}
		if err := journal.Settle(
			ctx, recipe.Pending.StepRevision, durableRunnerProgress(next+1),
		); err != nil {
			return DurableRequestTerminal{}, err
		}
		next++
	}
	for ; next < len(runner.steps); next++ {
		step := runner.steps[next]
		revision, err := journal.Stage(ctx, step.target, step.command)
		if err != nil {
			return DurableRequestTerminal{}, err
		}
		if runner.rule.fire(next, durableRunnerFaultBeforeSend) {
			return DurableRequestTerminal{}, errDurableFaultInjected
		}
		runner.data.dispatch(step.target, step.command)
		if runner.rule.fire(next, durableRunnerFaultAfterApply) {
			return DurableRequestTerminal{}, errDurableFaultInjected
		}
		if err := journal.Settle(ctx, revision, durableRunnerProgress(next+1)); err != nil {
			return DurableRequestTerminal{}, err
		}
	}
	result, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed: true, AffectedRows: int64(participants),
		Transaction:       [16]byte(recipe.Identity.ID),
		CatalogGeneration: recipe.Identity.CatalogGeneration,
		ShardsFanned:      participants, TransitionTag: recipe.Contract.CommitTransitionTag,
		TerminalStateDigest:     recipe.Contract.CommitTerminalStateDigest,
		TerminalContractDigest:  recipe.Contract.TerminalContractDigest,
		RetirementWitnessDigest: recipe.Contract.RetirementWitnessDigest,
		Payload:                 []byte("committed"),
	})
	if err != nil {
		return DurableRequestTerminal{}, err
	}
	return DurableRequestTerminal{Result: result}, nil
}

func durableParticipantProofStep(
	participant DurableRequestLogicalParticipant,
	ordinal uint64,
) (target, command []byte) {
	target = append(target, participant.Group.GroupID[:]...)
	command = append(command, participant.Group.ShardIncarnation[:]...)
	var encodedOrdinal [8]byte
	binary.LittleEndian.PutUint64(encodedOrdinal[:], ordinal)
	command = append(command, encodedOrdinal[:]...)
	return target, command
}

func (runner *durableParticipantProofRunner) Run(
	ctx context.Context,
	recipe DurableRequestRecipe,
	journal DurableRequestStepJournal,
) (DurableRequestTerminal, error) {
	if runner == nil || runner.data == nil || journal == nil ||
		recipe.ParticipantStream == nil || recipe.ParticipantCount == 0 ||
		recipe.ParticipantCount > math.MaxInt64 {
		return DurableRequestTerminal{}, ErrDurableRequest
	}
	next, err := openDurableRunnerProgress(recipe.Progress, int(recipe.ParticipantCount))
	if err != nil {
		return DurableRequestTerminal{}, err
	}
	pending := len(recipe.Pending.Command) != 0
	var ordinal uint64
	for recipe.ParticipantStream.Next() {
		participant := recipe.ParticipantStream.Current()
		if ordinal < uint64(next) {
			ordinal++
			continue
		}
		target, command := durableParticipantProofStep(participant, ordinal)
		if pending {
			if ordinal != uint64(next) ||
				!bytes.Equal(recipe.Pending.Target, target) ||
				!bytes.Equal(recipe.Pending.Command, command) {
				return DurableRequestTerminal{}, ErrDurableRequestConflict
			}
			runner.data.dispatch(target, command)
			if err := journal.Settle(
				ctx, recipe.Pending.StepRevision, durableRunnerProgress(next+1),
			); err != nil {
				return DurableRequestTerminal{}, err
			}
			pending = false
			next++
			ordinal++
			continue
		}
		revision, err := journal.Stage(ctx, target, command)
		if err != nil {
			return DurableRequestTerminal{}, err
		}
		runner.data.dispatch(target, command)
		if err := journal.Settle(ctx, revision, durableRunnerProgress(next+1)); err != nil {
			return DurableRequestTerminal{}, err
		}
		next++
		ordinal++
	}
	if err := recipe.ParticipantStream.Err(); err != nil {
		return DurableRequestTerminal{}, err
	}
	if !recipe.ParticipantStream.Complete() || pending ||
		ordinal != recipe.ParticipantCount || uint64(next) != ordinal {
		return DurableRequestTerminal{}, ErrDurableRequestConflict
	}
	result, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed: true, AffectedRows: int64(ordinal),
		Transaction:       [16]byte(recipe.Identity.ID),
		CatalogGeneration: recipe.Identity.CatalogGeneration, ShardsFanned: ordinal,
		TransitionTag:           recipe.Contract.CommitTransitionTag,
		TerminalStateDigest:     recipe.Contract.CommitTerminalStateDigest,
		TerminalContractDigest:  recipe.Contract.TerminalContractDigest,
		RetirementWitnessDigest: recipe.Contract.RetirementWitnessDigest,
		Payload:                 []byte("wide-committed"),
	})
	if err != nil {
		return DurableRequestTerminal{}, err
	}
	return DurableRequestTerminal{Result: result}, nil
}

func (runner durableStaticTerminalRunner) Run(
	_ context.Context,
	recipe DurableRequestRecipe,
	journal DurableRequestStepJournal,
) (DurableRequestTerminal, error) {
	if journal == nil || recipe.ParticipantStream == nil {
		return DurableRequestTerminal{}, ErrDurableRequest
	}
	var count uint64
	for recipe.ParticipantStream.Next() {
		_ = recipe.ParticipantStream.Current()
		count++
	}
	if err := recipe.ParticipantStream.Err(); err != nil {
		return DurableRequestTerminal{}, err
	}
	if !recipe.ParticipantStream.Complete() || count != recipe.ParticipantCount {
		return DurableRequestTerminal{}, ErrDurableRequestConflict
	}
	return cloneDurableFaultTerminal(runner.terminal), nil
}

func durableFaultParticipants(t *testing.T) []ReplicatedTransactionParticipant {
	t.Helper()
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	queries := []Query{
		{SQL: `DELETE FROM messages WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("message-fault"),
		}},
		{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("log-fault"),
		}},
	}
	participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, queries, executor.profileFor(ClassInteractive),
	)
	if err != nil || !handled || len(participants) != 2 {
		t.Fatalf("fault participants handled=%v count=%d err=%v", handled, len(participants), err)
	}
	return participants
}

func durableFaultParticipantsN(
	t *testing.T,
	count int,
) []ReplicatedTransactionParticipant {
	t.Helper()
	base := durableFaultParticipants(t)[0]
	participants := make([]ReplicatedTransactionParticipant, count)
	for index := range participants {
		participant := base
		participant.Route = cloneDurableRequestRoute(base.Route)
		participant.Route.Distribution = distribution.DistributionName(
			"fault-distribution-" + strconv.Itoa(index),
		)
		participant.Route.Shard = distribution.ShardID("fault-shard-" + strconv.Itoa(index))
		participant.Route.Group.GroupID = [16]byte{}
		participant.Route.Group.ShardIncarnation = [16]byte{}
		binary.LittleEndian.PutUint64(participant.Route.Group.GroupID[:8], uint64(index)+1)
		binary.LittleEndian.PutUint64(
			participant.Route.Group.ShardIncarnation[:8], uint64(index)+1,
		)
		participants[index] = participant
	}
	return participants
}

func durableFaultSteps() []durableRunnerStep {
	return []durableRunnerStep{
		{target: []byte("coordinator"), command: []byte("begin")},
		{target: []byte("participant-1"), command: []byte("prepare")},
		{target: []byte("coordinator"), command: []byte("commit")},
		{target: []byte("participant-1"), command: []byte("apply")},
		{target: []byte("coordinator"), command: []byte("retire")},
	}
}

func durableFaultTopology(
	t *testing.T,
	participants []ReplicatedTransactionParticipant,
) *DurableRequestLedgerTopologyHolder {
	t.Helper()
	holder, err := NewDurableRequestLedgerTopologyHolder(DurableRequestLedgerTopology{
		Generation: 1,
		Ranges: []DurableRequestLedgerRange{
			{
				End:      requestledger.LedgerHome{0x80},
				Identity: replication.Digest{1}, Route: participants[0].Route,
			},
			{
				Start:    requestledger.LedgerHome{0x80},
				Identity: replication.Digest{2}, Route: participants[1].Route,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return holder
}

func newDurableFaultExecutor(
	t *testing.T,
	topology *DurableRequestLedgerTopologyHolder,
	ledger durableRequestCoarseLedger,
	runner DurableRequestRunner,
) *DurableRequestExecutor {
	t.Helper()
	executor, err := NewDurableRequestExecutor(DurableRequestExecutorOptions{
		Topology: topology, Ledger: ledger, Runner: runner,
	})
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func durableFaultRequest(t *testing.T, participants []ReplicatedTransactionParticipant) DurableRequest {
	t.Helper()
	return durableFaultRequestWith(
		t, participants, replication.ID128{0x51}, replication.Digest{0x61}, 7,
	)
}

func durableFaultRequestWith(
	t *testing.T,
	participants []ReplicatedTransactionParticipant,
	requestID replication.ID128,
	requestDigest replication.Digest,
	catalogGeneration uint64,
) DurableRequest {
	t.Helper()
	tenant := []byte("tenant-fault")
	laneDigest := sha256.Sum256(requestID[:])
	var issuerLane requestledger.IssuerLane
	copy(issuerLane[:], laneDigest[:len(issuerLane)])
	key := DurableRequestLedgerKey{RequestKey: requestledger.RequestKey{
		Scope: requestledger.ScopeLocalInstall, Principal: requestledger.PrincipalID{0x44},
		Request:      requestledger.RequestID(requestID),
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)), IssuerEpoch: 1,
		IssuerSequence: 1, IssuerLane: issuerLane,
	}, Digest: requestDigest}
	keyDigest, err := requestledger.KeyDigest(key.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	logical := make([]DurableRequestLogicalParticipant, len(participants))
	for index := range participants {
		participant := &participants[index]
		rangeDigest := sha256.Sum256([]byte(
			"range/" + string(participant.Route.Distribution) + "/" + string(participant.Route.Shard),
		))
		lineageDigest := sha256.Sum256([]byte(
			"lineage/" + string(participant.Route.Distribution) + "/" + string(participant.Route.Shard),
		))
		forwardingDigest := sha256.Sum256([]byte(
			"forward/" + string(participant.Route.Distribution) + "/" + string(participant.Route.Shard),
		))
		logical[index] = DurableRequestLogicalParticipant{
			Distribution:           participant.Route.Distribution,
			Shard:                  participant.Route.Shard,
			RangeIdentity:          replication.Digest(rangeDigest),
			Group:                  participant.Route.Group,
			SchemaGeneration:       participant.Route.Command.SchemaGeneration,
			RelationManifestDigest: participant.Route.Command.RelationManifestDigest,
			LineageDigest:          replication.Digest(lineageDigest),
			ForwardingRuleDigest:   replication.Digest(forwardingDigest),
			BucketBits:             participant.BucketBits,
			IntentScopes:           participant.IntentScopes,
			Batches:                participant.Batches,
		}
	}
	slices.SortFunc(logical, compareDurableRequestLogicalParticipant)
	program, err := SealDurableRequestLogicalProgram(DurableRequestLogicalProgram{
		Identity: ReplicatedTransactionIdentity{
			CatalogGeneration: catalogGeneration, RecoveryDeadline: 1 << 60,
		},
		Contract: DurableRequestExecutionContract{
			ApplyContractDigest:          replication.Digest{0xa1},
			InitialStateDigest:           replication.Digest{0xa2},
			CommitTerminalStateDigest:    replication.Digest{0xa3},
			AbortTerminalStateDigest:     replication.Digest{0xa4},
			TerminalSummaryDigest:        replication.Digest{0xa8},
			PinEpoch:                     1,
			PinDigest:                    replication.Digest{0xa5},
			RouteSchemaCertificateDigest: replication.Digest{0xa6},
			RetirementWitnessDigest:      replication.Digest{0xa7},
			CommitTransitionTag:          1,
			AbortTransitionTag:           2,
			CommitFinalWaveCount:         uint64(len(durableFaultSteps())),
			AbortFinalWaveCount:          1,
			MaxPendingWaveBytes:          requestledger.MaxPendingWaveRecordBytes,
			MaxContinuationBytes:         requestledger.MaxContinuationRecordBytes,
			MaxTerminalBytes:             requestledger.MaxLifecyclePayloadBytes,
			PlanningLeaseExpiryIndex:     1 << 60,
			PlanningLeaseGeneration:      1,
		},
		Tenant: tenant, KeyDigest: replication.Digest(keyDigest), RequestID: requestID,
		RequestDigest: requestDigest, Participants: logical,
	})
	if err != nil {
		t.Fatal(err)
	}
	return DurableRequest{Key: key, Program: program}
}

func TestDurableRequestExecutorRequiresExactStructuredIssuerKey(t *testing.T) {
	participants := durableFaultParticipants(t)
	topology := durableFaultTopology(t, participants)
	executor := newDurableFaultExecutor(t, topology, newDurableFaultMemoryLedger(),
		&durableFaultRunner{steps: durableFaultSteps(), rule: new(durableRunnerFaultRule),
			data: newDurableRunnerData()})
	request := durableFaultRequest(t, participants)

	for name, mutate := range map[string]func(*DurableRequestLedgerKey){
		"epoch":    func(key *DurableRequestLedgerKey) { key.IssuerEpoch = 0 },
		"lane":     func(key *DurableRequestLedgerKey) { key.IssuerLane = requestledger.IssuerLane{} },
		"sequence": func(key *DurableRequestLedgerKey) { key.IssuerSequence = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := request
			mutate(&candidate.Key)
			if _, err := executor.Execute(t.Context(), candidate); !errors.Is(err, ErrDurableRequest) {
				t.Fatalf("execute error=%v", err)
			}
			if _, _, err := executor.Replay(t.Context(), candidate.Key); !errors.Is(err, ErrDurableRequest) {
				t.Fatalf("replay error=%v", err)
			}
			if err := executor.Acknowledge(t.Context(), candidate.Key, DurableRequestAckToken{1}); !errors.Is(err, ErrDurableRequest) {
				t.Fatalf("acknowledge error=%v", err)
			}
		})
	}

	for name, mutate := range map[string]func(*DurableRequestLedgerKey){
		"principal": func(key *DurableRequestLedgerKey) { key.Principal[0]++ },
		"request":   func(key *DurableRequestLedgerKey) { key.Request[0]++ },
		"tenant":    func(key *DurableRequestLedgerKey) { key.TenantDigest[0]++ },
		"digest":    func(key *DurableRequestLedgerKey) { key.Digest[0]++ },
	} {
		t.Run("mismatch_"+name, func(t *testing.T) {
			candidate := request
			mutate(&candidate.Key)
			if _, err := executor.Execute(t.Context(), candidate); !errors.Is(err, ErrDurableRequestConflict) {
				t.Fatalf("execute error=%v", err)
			}
		})
	}
}

func TestDurableRequestReplacementRecoversEveryReplicatedBoundary(t *testing.T) {
	testCases := []struct {
		name           string
		ledgerOp       durableFaultOperation
		ledgerTiming   durableFaultTiming
		runnerStep     int
		runnerTiming   durableRunnerFaultTiming
		attemptsBefore int
	}{
		{name: "seal_applied_before_response", ledgerOp: durableFaultSeal, ledgerTiming: durableFaultAfter},
		{name: "confirmed_seal_before_coordinator_send", runnerStep: 0, runnerTiming: durableRunnerFaultBeforeSend},
		{name: "pending_applied_before_response_and_lookup_unavailable", ledgerOp: durableFaultPutPending, ledgerTiming: durableFaultAfter},
		{name: "pending_persisted_send_response_lost", runnerStep: 0, runnerTiming: durableRunnerFaultAfterApply, attemptsBefore: 1},
		{name: "response_before_ledger_advance", ledgerOp: durableFaultAdvance, ledgerTiming: durableFaultBefore, attemptsBefore: 1},
		{name: "ledger_advance_applied_before_response", ledgerOp: durableFaultAdvance, ledgerTiming: durableFaultAfter, attemptsBefore: 1},
		{name: "coordinator_commit_response_lost", runnerStep: 2, runnerTiming: durableRunnerFaultAfterApply, attemptsBefore: 3},
		{name: "participant_apply_response_lost", runnerStep: 3, runnerTiming: durableRunnerFaultAfterApply, attemptsBefore: 4},
		{name: "retire_applied_before_terminal", runnerStep: 4, runnerTiming: durableRunnerFaultAfterApply, attemptsBefore: 5},
		{name: "terminal_before_apply", ledgerOp: durableFaultComplete, ledgerTiming: durableFaultBefore, attemptsBefore: 5},
		{name: "terminal_applied_before_response", ledgerOp: durableFaultComplete, ledgerTiming: durableFaultAfter, attemptsBefore: 5},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			participants := durableFaultParticipants(t)
			topology := durableFaultTopology(t, participants)
			base := newDurableFaultMemoryLedger()
			ledgerRule := new(durableFaultRule)
			ledger := &durableFaultLedger{base: base, rule: ledgerRule}
			runnerRule := new(durableRunnerFaultRule)
			data := newDurableRunnerData()
			steps := durableFaultSteps()
			if testCase.ledgerOp != 0 {
				ledgerRule.arm(testCase.ledgerOp, testCase.ledgerTiming)
			} else {
				runnerRule.arm(testCase.runnerStep, testCase.runnerTiming)
			}
			first := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
				steps: steps, rule: runnerRule, data: data,
			})
			request := durableFaultRequest(t, participants)
			ctx, cancel := context.WithTimeout(
				WithLocalReplicatedTransactionRequestScope(t.Context()), 10*time.Second,
			)
			defer cancel()
			if outcome, err := first.Execute(ctx, request); err == nil {
				t.Fatalf("first gateway unexpectedly completed: %+v", outcome)
			}
			attempts, _ := data.snapshot()
			if len(attempts) != testCase.attemptsBefore {
				t.Fatalf("attempts before replacement=%d, want %d", len(attempts), testCase.attemptsBefore)
			}
			pendingStep := -1
			if testCase.runnerTiming == durableRunnerFaultBeforeSend ||
				testCase.runnerTiming == durableRunnerFaultAfterApply {
				pendingStep = testCase.runnerStep
			} else if testCase.ledgerOp == durableFaultAdvance &&
				testCase.ledgerTiming == durableFaultBefore {
				pendingStep = 0
			} else if testCase.ledgerOp == durableFaultPutPending &&
				testCase.ledgerTiming == durableFaultAfter {
				pendingStep = 0
			}
			if pendingStep >= 0 {
				key := request.Key
				point, keyErr := durableRequestLedgerHome(key)
				if keyErr != nil {
					t.Fatal(keyErr)
				}
				home, ok := topology.Current().Home(requestledger.LedgerHome(point))
				if !ok {
					t.Fatal("pending home missing")
				}
				entry, lookupErr := base.Lookup(ctx, home, key)
				want := steps[pendingStep]
				if lookupErr != nil || entry.State != DurableRequestLedgerPending ||
					!bytes.Equal(entry.Pending.Target, want.target) ||
					!bytes.Equal(entry.Pending.Command, want.command) {
					t.Fatalf("pending step=%+v err=%v, want target=%q command=%q", entry.Pending, lookupErr, want.target, want.command)
				}
			}

			// A replacement receives no process-local recovery handle and no
			// participant recipe. The exact structured request key, topology, and
			// replicated ledger are its complete recovery input.
			ledgerRule.clear()
			runnerRule.clear()
			replacement := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
				steps: steps, rule: runnerRule, data: data,
			})
			outcome, found, err := replacement.Replay(ctx, request.Key)
			if err != nil || !found || !outcome.Committed || outcome.AffectedRows != 2 ||
				outcome.CatalogGeneration != request.Program.Identity.CatalogGeneration ||
				outcome.ShardsFanned != 2 {
				t.Fatalf("replacement outcome=%+v found=%v err=%v", outcome, found, err)
			}
			attempts, applications := data.snapshot()
			for _, step := range steps {
				exact := append(bytes.Clone(step.target), 0)
				exact = append(exact, step.command...)
				if applications[string(exact)] == 0 {
					t.Fatalf("step %q was not applied", step.command)
				}
			}
			if testCase.runnerTiming == durableRunnerFaultAfterApply ||
				(testCase.ledgerOp == durableFaultAdvance && testCase.ledgerTiming == durableFaultBefore) {
				if len(attempts) != len(steps)+1 {
					t.Fatalf("response-loss attempts=%d, want one exact replay", len(attempts))
				}
				frequencies := make(map[string]int, len(attempts))
				for _, attempt := range attempts {
					frequencies[string(attempt)]++
				}
				duplicates := 0
				for _, frequency := range frequencies {
					if frequency == 2 {
						duplicates++
					} else if frequency != 1 {
						t.Fatalf("non-exact replay frequency=%d", frequency)
					}
				}
				if duplicates != 1 {
					t.Fatalf("exact duplicate commands=%d, want one", duplicates)
				}
			} else if len(attempts) != len(steps) {
				t.Fatalf("attempts=%d, want %d", len(attempts), len(steps))
			}
		})
	}
}

func TestDurableRequestAckResponseLossLeavesCompactPermanentTombstone(t *testing.T) {
	participants := durableFaultParticipants(t)
	topology := durableFaultTopology(t, participants)
	base := newDurableFaultMemoryLedger()
	ledgerRule := new(durableFaultRule)
	ledger := &durableFaultLedger{base: base, rule: ledgerRule}
	runnerRule := new(durableRunnerFaultRule)
	data := newDurableRunnerData()
	runner := &durableFaultRunner{steps: durableFaultSteps(), rule: runnerRule, data: data}
	executor := newDurableFaultExecutor(t, topology, ledger, runner)
	request := durableFaultRequest(t, participants)
	ctx := WithLocalReplicatedTransactionRequestScope(t.Context())
	outcome, err := executor.Execute(ctx, request)
	if err != nil || !outcome.Committed || outcome.Acknowledged {
		t.Fatalf("terminal outcome=%+v err=%v", outcome, err)
	}
	if outcome.AckToken == (DurableRequestAckToken{}) {
		t.Fatalf("terminal outcome missing ACK possession token: %+v", outcome.AckToken)
	}
	attemptsBefore, _ := data.snapshot()
	changedDigest := request.Program.RequestDigest
	changedDigest[0] ^= 0xff
	forgedDigest := durableFaultRequestWith(
		t, participants, request.Program.RequestID, changedDigest,
		request.Program.Identity.CatalogGeneration,
	)
	forgedPlan := durableFaultRequestWith(
		t, participants, request.Program.RequestID, request.Program.RequestDigest,
		request.Program.Identity.CatalogGeneration+1,
	)
	for name, candidate := range map[string]DurableRequest{
		"request_digest": forgedDigest,
		"sealed_plan":    forgedPlan,
	} {
		if _, err := executor.Execute(ctx, candidate); !errors.Is(err, ErrDurableRequestConflict) {
			t.Fatalf("terminal %s identity reuse error=%v", name, err)
		}
	}
	if attempts, _ := data.snapshot(); len(attempts) != len(attemptsBefore) {
		t.Fatalf("terminal identity conflict invoked runner: before=%d after=%d", len(attemptsBefore), len(attempts))
	}
	wrongToken := outcome.AckToken
	wrongToken[0] ^= 0xff
	for _, token := range []DurableRequestAckToken{{}, wrongToken} {
		if err := executor.Acknowledge(ctx, request.Key, token); !errors.Is(err, ErrDurableRequestConflict) {
			t.Fatalf("invalid ACK possession token %+v error=%v", token, err)
		}
	}
	if retained, found, err := executor.Replay(ctx, request.Key); err != nil || !found || !retained.Committed ||
		retained.AckToken != outcome.AckToken || !bytes.Equal(retained.Result, outcome.Result) {
		t.Fatalf("invalid ACK token discarded terminal: outcome=%+v found=%v err=%v", retained, found, err)
	}

	ledgerRule.arm(durableFaultAcknowledge, durableFaultAfter)
	if err := executor.Acknowledge(ctx, request.Key, outcome.AckToken); err == nil {
		t.Fatal("lost ACK response unexpectedly succeeded")
	}
	ledgerRule.clear()
	replacement := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
		steps: durableFaultSteps(), rule: runnerRule, data: data,
	})
	replayed, err := replacement.Execute(ctx, request)
	if !errors.Is(err, ErrDurableRequestAcknowledged) || !replayed.Acknowledged ||
		replayed.Committed || replayed.AffectedRows != 0 ||
		replayed.ID != ([16]byte{}) {
		t.Fatalf("acked execute replay=%+v err=%v", replayed, err)
	}
	if replayed, found, err := replacement.Replay(ctx, request.Key); !found || !errors.Is(err, ErrDurableRequestAcknowledged) ||
		!replayed.Acknowledged || replayed.Committed {
		t.Fatalf("acked replay=%+v found=%v err=%v", replayed, found, err)
	}
	for name, candidate := range map[string]DurableRequest{
		"request_digest": forgedDigest,
		"sealed_plan":    forgedPlan,
	} {
		if _, err := replacement.Execute(ctx, candidate); !errors.Is(err, ErrDurableRequestConflict) {
			t.Fatalf("acked %s identity reuse error=%v", name, err)
		}
	}
	if err := replacement.Acknowledge(ctx, request.Key, outcome.AckToken); err != nil {
		t.Fatalf("duplicate ACK: %v", err)
	}
	attemptsAfter, _ := data.snapshot()
	if len(attemptsAfter) != len(attemptsBefore) {
		t.Fatalf("ACK replay invoked runner: before=%d after=%d", len(attemptsBefore), len(attemptsAfter))
	}

	key := request.Key
	point, err := durableRequestLedgerHome(key)
	if err != nil {
		t.Fatal(err)
	}
	home, ok := topology.Current().Home(requestledger.LedgerHome(point))
	if !ok {
		t.Fatal("acked home missing")
	}
	entry, err := base.Lookup(ctx, home, key)
	if err != nil {
		t.Fatal(err)
	}
	if entry.State != DurableRequestLedgerAcked || entry.Digest != request.Program.RequestDigest ||
		entry.AckDigest == (replication.Digest{}) || entry.AckTerminalRevision == 0 ||
		entry.AckResultDigest == (replication.Digest{}) ||
		entry.AckTokenDigest != durableRequestAckTokenDigest(outcome.AckToken) ||
		entry.AckToken != (DurableRequestAckToken{}) {
		t.Fatalf("invalid ACK tombstone: %+v", entry)
	}
	if entry.Plan.TotalBytes != 0 || entry.Plan.PageCount != 0 ||
		entry.Plan.Root != (replication.Digest{}) || len(entry.Plan.Inline) != 0 ||
		entry.Pending.StepRevision != 0 || len(entry.Pending.Target) != 0 ||
		len(entry.Pending.Command) != 0 || len(entry.Progress) != 0 ||
		len(entry.Terminal.Result) != 0 {
		t.Fatalf("ACK retained recipe/result body: %+v", entry)
	}
}

func TestDurableRequestTerminalServesAbortAndRejectsForgedSemantics(t *testing.T) {
	participants := durableFaultParticipants(t)
	topology := durableFaultTopology(t, participants)
	ctx := WithLocalReplicatedTransactionRequestScope(t.Context())
	abortRequest := durableFaultRequestWith(
		t, participants, replication.ID128{0x76}, replication.Digest{0x77}, 7,
	)
	abortRaw, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed:               false,
		Transaction:             [16]byte(abortRequest.Program.Identity.ID),
		CatalogGeneration:       abortRequest.Program.Identity.CatalogGeneration,
		ShardsFanned:            uint64(len(abortRequest.Program.Participants)),
		TransitionTag:           abortRequest.Program.Contract.AbortTransitionTag,
		TerminalStateDigest:     abortRequest.Program.Contract.AbortTerminalStateDigest,
		TerminalContractDigest:  abortRequest.Program.Contract.TerminalContractDigest,
		RetirementWitnessDigest: abortRequest.Program.Contract.RetirementWitnessDigest,
		Payload:                 []byte("aborted"),
	})
	if err != nil {
		t.Fatal(err)
	}
	abortBase := newDurableFaultMemoryLedger()
	abortLedger := &durableFaultLedger{base: abortBase, rule: new(durableFaultRule)}
	abortExecutor := newDurableFaultExecutor(t, topology, abortLedger,
		durableStaticTerminalRunner{terminal: DurableRequestTerminal{Result: abortRaw}})
	abortOutcome, err := abortExecutor.Execute(ctx, abortRequest)
	if err != nil || abortOutcome.Committed || abortOutcome.AffectedRows != 0 ||
		abortOutcome.ID != [16]byte(abortRequest.Program.Identity.ID) ||
		abortOutcome.AckToken == (DurableRequestAckToken{}) ||
		!bytes.Equal(abortOutcome.Result, []byte("aborted")) {
		t.Fatalf("abort outcome=%+v err=%v", abortOutcome, err)
	}
	if replayed, found, err := abortExecutor.Replay(ctx, abortRequest.Key); err != nil || !found || replayed.Committed || replayed.ID != abortOutcome.ID ||
		replayed.AckToken != abortOutcome.AckToken {
		t.Fatalf("abort replay=%+v found=%v err=%v", replayed, found, err)
	}
	if err := abortExecutor.Acknowledge(ctx, abortRequest.Key, abortOutcome.AckToken); err != nil {
		t.Fatalf("abort ACK: %v", err)
	}

	forgedRequest := durableFaultRequestWith(
		t, participants, replication.ID128{0x78}, replication.Digest{0x79}, 7,
	)
	validCommit, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed:               true,
		AffectedRows:            2,
		Transaction:             [16]byte(forgedRequest.Program.Identity.ID),
		CatalogGeneration:       forgedRequest.Program.Identity.CatalogGeneration,
		ShardsFanned:            uint64(len(forgedRequest.Program.Participants)),
		TransitionTag:           forgedRequest.Program.Contract.CommitTransitionTag,
		TerminalStateDigest:     forgedRequest.Program.Contract.CommitTerminalStateDigest,
		TerminalContractDigest:  forgedRequest.Program.Contract.TerminalContractDigest,
		RetirementWitnessDigest: forgedRequest.Program.Contract.RetirementWitnessDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	invalidDecision := bytes.Clone(validCommit)
	invalidDecision[9] = 3
	zeroTransaction := bytes.Clone(validCommit)
	clear(zeroTransaction[24:40])
	negativeAffected := bytes.Clone(validCommit)
	for index := 16; index < 24; index++ {
		negativeAffected[index] = 0xff
	}
	validAbort, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed:               false,
		Transaction:             [16]byte(forgedRequest.Program.Identity.ID),
		CatalogGeneration:       forgedRequest.Program.Identity.CatalogGeneration,
		ShardsFanned:            uint64(len(forgedRequest.Program.Participants)),
		TransitionTag:           forgedRequest.Program.Contract.AbortTransitionTag,
		TerminalStateDigest:     forgedRequest.Program.Contract.AbortTerminalStateDigest,
		TerminalContractDigest:  forgedRequest.Program.Contract.TerminalContractDigest,
		RetirementWitnessDigest: forgedRequest.Program.Contract.RetirementWitnessDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	abortAffected := bytes.Clone(validAbort)
	binary.LittleEndian.PutUint64(abortAffected[16:24], 1)
	mismatchedCatalog, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed:               true,
		AffectedRows:            2,
		Transaction:             [16]byte(forgedRequest.Program.Identity.ID),
		CatalogGeneration:       forgedRequest.Program.Identity.CatalogGeneration + 1,
		ShardsFanned:            uint64(len(forgedRequest.Program.Participants)),
		TransitionTag:           forgedRequest.Program.Contract.CommitTransitionTag,
		TerminalStateDigest:     forgedRequest.Program.Contract.CommitTerminalStateDigest,
		TerminalContractDigest:  forgedRequest.Program.Contract.TerminalContractDigest,
		RetirementWitnessDigest: forgedRequest.Program.Contract.RetirementWitnessDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	mismatchedShards, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed:               true,
		AffectedRows:            2,
		Transaction:             [16]byte(forgedRequest.Program.Identity.ID),
		CatalogGeneration:       forgedRequest.Program.Identity.CatalogGeneration,
		ShardsFanned:            uint64(len(forgedRequest.Program.Participants)) + 1,
		TransitionTag:           forgedRequest.Program.Contract.CommitTransitionTag,
		TerminalStateDigest:     forgedRequest.Program.Contract.CommitTerminalStateDigest,
		TerminalContractDigest:  forgedRequest.Program.Contract.TerminalContractDigest,
		RetirementWitnessDigest: forgedRequest.Program.Contract.RetirementWitnessDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name string
		raw  []byte
	}{
		{name: "decision", raw: invalidDecision},
		{name: "transaction", raw: zeroTransaction},
		{name: "negative_affected", raw: negativeAffected},
		{name: "abort_affected", raw: abortAffected},
		{name: "catalog", raw: mismatchedCatalog},
		{name: "shards", raw: mismatchedShards},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			base := newDurableFaultMemoryLedger()
			ledger := &durableFaultLedger{base: base, rule: new(durableFaultRule)}
			executor := newDurableFaultExecutor(t, topology, ledger,
				durableStaticTerminalRunner{terminal: DurableRequestTerminal{Result: testCase.raw}})
			request := forgedRequest
			if _, err := executor.Execute(ctx, request); !errors.Is(err, ErrDurableRequestConflict) {
				t.Fatalf("forged terminal error=%v", err)
			}
			key := request.Key
			point, err := durableRequestLedgerHome(key)
			if err != nil {
				t.Fatal(err)
			}
			home, ok := topology.Current().Home(requestledger.LedgerHome(point))
			if !ok {
				t.Fatal("forged terminal home missing")
			}
			entry, err := base.Lookup(ctx, home, key)
			if err != nil || entry.State != DurableRequestLedgerSealed || len(entry.Terminal.Result) != 0 {
				t.Fatalf("forged terminal persisted entry=%+v err=%v", entry, err)
			}
		})
	}
}

func TestDurableRequestStatelessCapacityFloodDoesNotConsumeRecoveryAuthority(t *testing.T) {
	participants := durableFaultParticipants(t)
	topology := durableFaultTopology(t, participants)
	base := newDurableFaultMemoryLedger()
	ledger := &durableCapacityLedger{
		durableFaultLedger: &durableFaultLedger{base: base, rule: new(durableFaultRule)},
		full:               true,
	}
	runnerRule := new(durableRunnerFaultRule)
	data := newDurableRunnerData()
	runner := &durableFaultRunner{steps: durableFaultSteps(), rule: runnerRule, data: data}
	executor := newDurableFaultExecutor(t, topology, ledger, runner)
	ctx := WithLocalReplicatedTransactionRequestScope(t.Context())

	const freshRequests = 257
	var admitted DurableRequest
	for index := 0; index < freshRequests; index++ {
		requestID := replication.ID128{}
		binary.LittleEndian.PutUint64(requestID[:8], uint64(index+1))
		request := durableFaultRequestWith(
			t, participants, requestID,
			replication.Digest{0x91, byte(index), byte(index >> 8)}, 7,
		)
		outcome, err := executor.Execute(ctx, request)
		if !errors.Is(err, ErrDurableRequestCapacity) ||
			outcome.Committed || outcome.Acknowledged || outcome.AffectedRows != 0 ||
			outcome.ID != ([16]byte{}) || outcome.CatalogGeneration != 0 ||
			outcome.ShardsFanned != 0 || len(outcome.Result) != 0 ||
			outcome.AckToken != (DurableRequestAckToken{}) {
			t.Fatalf("fresh request %d capacity outcome=%+v err=%v", index, outcome, err)
		}
		if index == freshRequests/2 {
			admitted = request
		}
	}
	if got := ledger.rejectionCount(); got != freshRequests {
		t.Fatalf("capacity refusals=%d, want %d", got, freshRequests)
	}
	if attempts, _ := data.snapshot(); len(attempts) != 0 {
		t.Fatalf("capacity refusal dispatched %d data commands", len(attempts))
	}
	base.mu.Lock()
	retainedAfterFlood := len(base.entries)
	base.mu.Unlock()
	if retainedAfterFlood != 0 {
		t.Fatalf("stateless capacity flood retained %d ledger identities", retainedAfterFlood)
	}

	// Capacity may later become available for the same identity. Exercise an
	// outcome-unknown data cut to prove that normal recovery and cleanup still
	// own their durable state after a flood of unauthenticated-cost-free rejects.
	ledger.setFull(false)
	runnerRule.arm(2, durableRunnerFaultAfterApply)
	if _, err := executor.Execute(ctx, admitted); err == nil {
		t.Fatal("outcome-unknown admitted execution unexpectedly succeeded")
	}
	runnerRule.clear()
	replacement := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
		steps: durableFaultSteps(), rule: runnerRule, data: data,
	})
	outcome, err := replacement.Execute(ctx, admitted)
	if err != nil || !outcome.Committed || outcome.AckToken == (DurableRequestAckToken{}) {
		t.Fatalf("admitted recovery outcome=%+v err=%v", outcome, err)
	}
	if err := replacement.Acknowledge(ctx, admitted.Key, outcome.AckToken); err != nil {
		t.Fatalf("admitted cleanup ACK: %v", err)
	}
	if replayed, found, err := replacement.Replay(ctx, admitted.Key); !found || !replayed.Acknowledged ||
		!errors.Is(err, ErrDurableRequestAcknowledged) {
		t.Fatalf("admitted cleanup replay=%+v found=%v err=%v", replayed, found, err)
	}
	_, applications := data.snapshot()
	if len(applications) != len(durableFaultSteps()) {
		t.Fatalf("recovered unique applications=%d, want %d", len(applications), len(durableFaultSteps()))
	}
	for command, effects := range applications {
		if effects != 1 {
			t.Fatalf("recovered command %q effects=%d, want 1", command, effects)
		}
	}
}

func TestDurableRequestStageUsesAppliedCompletionFastPath(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		responseLost bool
		wantLookups  int
		wantStageErr bool
	}{
		{name: "success", wantLookups: 0},
		{name: "response_lost", responseLost: true, wantLookups: 1, wantStageErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			participants := durableFaultParticipants(t)
			topology := durableFaultTopology(t, participants)
			base := newDurableFaultMemoryLedger()
			rule := new(durableFaultRule)
			ledger := &durableFaultLedger{base: base, rule: rule}
			executor := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
				steps: durableFaultSteps(), rule: new(durableRunnerFaultRule),
				data: newDurableRunnerData(),
			})
			request := durableFaultRequestWith(
				t, participants, replication.ID128{0x72}, replication.Digest{0x73}, 7,
			)
			ctx := WithLocalReplicatedTransactionRequestScope(t.Context())
			key := request.Key
			point, err := durableRequestLedgerHome(key)
			if err != nil {
				t.Fatal(err)
			}
			measurement, err := measureDurableRequestPlan(key, request.Program)
			descriptor := measurement.descriptor()
			if err != nil || len(descriptor.Inline) == 0 {
				t.Fatalf("inline descriptor=%+v err=%v", descriptor, err)
			}
			home, ok := topology.Current().Home(requestledger.LedgerHome(point))
			if !ok {
				t.Fatal("stage home missing")
			}
			entry, err := base.CreateSealed(ctx, home, key, 1, descriptor)
			if err != nil {
				t.Fatal(err)
			}
			journal := &durableRequestJournal{
				executor: executor, home: home, key: key, entry: entry,
			}
			if testCase.responseLost {
				rule.arm(durableFaultPutPending, durableFaultAfter)
			}
			revision, stageErr := journal.Stage(ctx, []byte("target"), []byte("command"))
			if (stageErr != nil) != testCase.wantStageErr {
				t.Fatalf("stage revision=%d err=%v", revision, stageErr)
			}
			if got := ledger.callCount(durableFaultPutPending); got != 1 {
				t.Fatalf("PutPending calls=%d, want 1", got)
			}
			if got := ledger.callCount(durableFaultLookup); got != testCase.wantLookups {
				t.Fatalf("Lookup calls=%d, want %d", got, testCase.wantLookups)
			}
			stored, err := base.Lookup(ctx, home, key)
			if err != nil || stored.State != DurableRequestLedgerPending ||
				!bytes.Equal(stored.Pending.Target, []byte("target")) ||
				!bytes.Equal(stored.Pending.Command, []byte("command")) {
				t.Fatalf("stored pending=%+v err=%v", stored.Pending, err)
			}
		})
	}
}

func TestDurableRequestRefreshesSameHomeAtEveryLifecycleCut(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		operation durableFaultOperation
		wantCalls int
		ack       bool
	}{
		{name: "inline_seal", operation: durableFaultSeal, wantCalls: 2},
		{name: "put_pending", operation: durableFaultPutPending, wantCalls: len(durableFaultSteps()) + 1},
		{name: "advance", operation: durableFaultAdvance, wantCalls: len(durableFaultSteps()) + 1},
		{name: "complete", operation: durableFaultComplete, wantCalls: 2},
		{name: "ack", operation: durableFaultAcknowledge, wantCalls: 2, ack: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			participants := durableFaultParticipants(t)
			topology := durableFaultTopology(t, participants)
			current := topology.Current()
			nextRanges := make([]DurableRequestLedgerRange, len(current.Ranges))
			for index := range current.Ranges {
				nextRanges[index] = current.Ranges[index]
				nextRanges[index].Route = cloneDurableRequestRoute(current.Ranges[index].Route)
				for replica := range nextRanges[index].Route.Replicas {
					nextRanges[index].Route.Replicas[replica].Address += "-refresh"
				}
			}
			base := newDurableFaultMemoryLedger()
			ledger := &durableFaultLedger{
				base: base, rule: new(durableFaultRule),
				stale: &durableStaleHomeRule{
					operation: testCase.operation, holder: topology,
					next: DurableRequestLedgerTopology{
						Generation: current.Generation + 1, Ranges: nextRanges,
					},
				},
			}
			data := newDurableRunnerData()
			executor := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
				steps: durableFaultSteps(), rule: new(durableRunnerFaultRule), data: data,
			})
			request := durableFaultRequestWith(
				t, participants, replication.ID128{0x74, byte(testCase.operation)},
				replication.Digest{0x75, byte(testCase.operation)}, 7,
			)
			ctx := WithLocalReplicatedTransactionRequestScope(t.Context())
			outcome, err := executor.Execute(ctx, request)
			if err != nil || !outcome.Committed {
				t.Fatalf("refreshed execute outcome=%+v err=%v", outcome, err)
			}
			if testCase.ack {
				if err := executor.Acknowledge(ctx, request.Key, outcome.AckToken); err != nil {
					t.Fatalf("refreshed ACK: %v", err)
				}
			}
			if got := ledger.callCount(testCase.operation); got != testCase.wantCalls {
				t.Fatalf("operation %d calls=%d, want one stale retry over %d", testCase.operation, got, testCase.wantCalls)
			}
			if attempts, _ := data.snapshot(); len(attempts) != len(durableFaultSteps()) {
				t.Fatalf("stale pre-admission duplicated data attempts=%d", len(attempts))
			}
			if generation := topology.Current().Generation; generation != current.Generation+1 {
				t.Fatalf("topology generation=%d", generation)
			}
		})
	}
}

func TestDurableRequestPagedRecipeHasNoParticipantCliff(t *testing.T) {
	for _, count := range []int{64, 65, 4097} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			participants := durableFaultParticipantsN(t, count)
			topology := durableFaultTopology(t, participants)
			base := newDurableFaultMemoryLedger()
			ledger := &durableFaultLedger{base: base, rule: new(durableFaultRule)}
			data := newDurableRunnerData()
			executor := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
				steps: durableFaultSteps(), rule: new(durableRunnerFaultRule), data: data,
			})
			request := durableFaultRequestWith(
				t, participants, replication.ID128{byte(count), byte(count >> 8)},
				replication.Digest{byte(count), byte(count >> 8), 0x7d}, 7,
			)
			ctx := WithLocalReplicatedTransactionRequestScope(t.Context())
			outcome, err := executor.Execute(ctx, request)
			if err != nil || !outcome.Committed || outcome.AffectedRows != int64(count) ||
				outcome.ShardsFanned != count {
				t.Fatalf("count=%d outcome=%+v err=%v", count, outcome, err)
			}
			key := request.Key
			point, err := durableRequestLedgerHome(key)
			if err != nil {
				t.Fatal(err)
			}
			home, ok := topology.Current().Home(requestledger.LedgerHome(point))
			if !ok {
				t.Fatal("paged home missing")
			}
			entry, err := base.Lookup(ctx, home, key)
			if err != nil {
				t.Fatal(err)
			}
			wantPaged := entry.Plan.TotalBytes > DurableRequestInlineBytes
			if (entry.Plan.PageCount != 0) != wantPaged ||
				(len(entry.Plan.Inline) == 0) != wantPaged {
				t.Fatalf("count=%d size-based paging mismatch: %+v", count, entry.Plan)
			}
			if count <= 65 && wantPaged {
				t.Fatalf("count=%d hit a participant-count paging cliff: %+v", count, entry.Plan)
			}
			base.mu.Lock()
			storedPages := len(base.entries[durableFaultKey(home, key)].pages)
			base.mu.Unlock()
			if storedPages != int(entry.Plan.PageCount) {
				t.Fatalf("count=%d stored pages=%d descriptor=%d", count, storedPages, entry.Plan.PageCount)
			}
		})
	}
}

func TestDurableRequestWideStreamDispatchesEveryParticipant(t *testing.T) {
	const count = 8193
	participants := durableFaultParticipantsN(t, count)
	topology := durableFaultTopology(t, participants)
	base := newDurableFaultMemoryLedger()
	ledger := &durableFaultLedger{base: base, rule: new(durableFaultRule)}
	data := newDurableRunnerData()
	executor := newDurableFaultExecutor(
		t, topology, ledger, &durableParticipantProofRunner{data: data},
	)
	request := durableFaultRequestWith(
		t, participants, replication.ID128{0x79, 0x20}, replication.Digest{0x7a, 0x20}, 7,
	)
	ctx := WithLocalReplicatedTransactionRequestScope(t.Context())
	outcome, err := executor.Execute(ctx, request)
	if err != nil || !outcome.Committed || outcome.AffectedRows != count ||
		outcome.ShardsFanned != count {
		t.Fatalf("wide outcome=%+v err=%v", outcome, err)
	}
	attempts, applications := data.snapshot()
	if len(attempts) != count || len(applications) != count {
		t.Fatalf("wide dispatch attempts=%d unique=%d, want %d", len(attempts), len(applications), count)
	}
	for command, effects := range applications {
		if effects != 1 {
			t.Fatalf("wide command %q effects=%d, want one", command, effects)
		}
	}
}

func TestDurableRequestPagedBuildResumesAtEveryDurableCut(t *testing.T) {
	participants := durableFaultParticipantsN(t, 4097)
	request := durableFaultRequestWith(
		t, participants, replication.ID128{0x7a}, replication.Digest{0x7b}, 7,
	)
	topology := durableFaultTopology(t, participants)
	ctx := WithLocalReplicatedTransactionRequestScope(t.Context())
	key := request.Key
	point, err := durableRequestLedgerHome(key)
	if err != nil {
		t.Fatal(err)
	}
	measurement, err := measureDurableRequestPlan(key, request.Program)
	descriptor := measurement.descriptor()
	if err != nil || descriptor.PageCount < 2 {
		t.Fatalf("descriptor=%+v err=%v, want multiple pages", descriptor, err)
	}
	home, ok := topology.Current().Home(requestledger.LedgerHome(point))
	if !ok {
		t.Fatal("paged build home missing")
	}

	type faultCase struct {
		name          string
		operation     durableFaultOperation
		timing        durableFaultTiming
		nth           int
		appendedPages uint32
		state         DurableRequestLedgerState
	}
	cases := []faultCase{
		{name: "before_create_planning", operation: durableFaultCreate, timing: durableFaultBefore, nth: 1, state: DurableRequestLedgerAbsent},
		{name: "after_create_planning", operation: durableFaultCreate, timing: durableFaultAfter, nth: 1, state: DurableRequestLedgerCreating},
	}
	for page := uint32(0); page < descriptor.PageCount; page++ {
		operation := durableFaultAppendPage
		nth := int(page) + 1
		if page+1 == descriptor.PageCount {
			operation = durableFaultSeal
			nth = 1
		}
		cases = append(cases,
			faultCase{
				name:      "before_page_" + strconv.FormatUint(uint64(page), 10),
				operation: operation, timing: durableFaultBefore, nth: nth,
				appendedPages: page, state: DurableRequestLedgerCreating,
			},
			faultCase{
				name:      "after_page_" + strconv.FormatUint(uint64(page), 10),
				operation: operation, timing: durableFaultAfter, nth: nth,
				appendedPages: page + 1,
				state: func() DurableRequestLedgerState {
					if page+1 == descriptor.PageCount {
						return DurableRequestLedgerSealed
					}
					return DurableRequestLedgerCreating
				}(),
			},
		)
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			base := newDurableFaultMemoryLedger()
			rule := new(durableFaultRule)
			rule.armNth(testCase.operation, testCase.timing, testCase.nth)
			ledger := &durableFaultLedger{base: base, rule: rule}
			data := newDurableRunnerData()
			first := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
				steps: durableFaultSteps(), rule: new(durableRunnerFaultRule), data: data,
			})
			if outcome, err := first.Execute(ctx, request); err == nil {
				t.Fatalf("first gateway unexpectedly completed: %+v", outcome)
			}
			if attempts, _ := data.snapshot(); len(attempts) != 0 {
				t.Fatalf("data dispatched before sealed completion: %d attempts", len(attempts))
			}
			entry, lookupErr := base.Lookup(ctx, home, key)
			if lookupErr != nil || entry.State != testCase.state ||
				entry.AppendedPageCount != testCase.appendedPages {
				t.Fatalf("durable cut entry=%+v err=%v", entry, lookupErr)
			}
			rule.clear()
			replacement := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
				steps: durableFaultSteps(), rule: new(durableRunnerFaultRule), data: data,
			})
			outcome, err := replacement.Execute(ctx, request)
			if err != nil || !outcome.Committed || outcome.ShardsFanned != len(participants) {
				t.Fatalf("replacement outcome=%+v err=%v", outcome, err)
			}
			entry, err = base.Lookup(ctx, home, key)
			if err != nil || entry.State != DurableRequestLedgerTerminal ||
				entry.AppendedPageCount != descriptor.PageCount {
				t.Fatalf("terminal entry=%+v err=%v", entry, err)
			}
		})
	}
}

func TestDurableRequestConcurrentGatewaysConvergeOnOneOutcome(t *testing.T) {
	participants := durableFaultParticipants(t)
	topology := durableFaultTopology(t, participants)
	base := newDurableFaultMemoryLedger()
	ledger := &durableFaultLedger{base: base, rule: new(durableFaultRule)}
	data := newDurableRunnerData()
	steps := durableFaultSteps()
	request := durableFaultRequest(t, participants)
	ctx := WithLocalReplicatedTransactionRequestScope(t.Context())
	type result struct {
		outcome DurableRequestOutcome
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		executor := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
			steps: steps, rule: new(durableRunnerFaultRule), data: data,
		})
		go func() {
			<-start
			outcome, err := executor.Execute(ctx, request)
			results <- result{outcome: outcome, err: err}
		}()
	}
	close(start)
	for range 2 {
		value := <-results
		if value.err != nil || !value.outcome.Committed || value.outcome.AffectedRows != 2 {
			t.Fatalf("concurrent outcome=%+v err=%v", value.outcome, value.err)
		}
	}
	_, applications := data.snapshot()
	if len(applications) != len(steps) {
		t.Fatalf("unique applications=%d, want %d", len(applications), len(steps))
	}
	for command, effects := range applications {
		if effects != 1 {
			t.Fatalf("command %q effects=%d, want one", command, effects)
		}
	}
}

func TestDurableRequestLostResponseConvergesWhenAnotherGatewayAdvancesAhead(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		operation  durableFaultOperation
		wideRecipe bool
	}{
		{name: "append_page", operation: durableFaultAppendPage, wideRecipe: true},
		{name: "put_pending", operation: durableFaultPutPending},
		{name: "advance", operation: durableFaultAdvance},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			participants := durableFaultParticipants(t)
			if testCase.wideRecipe {
				participants = durableFaultParticipantsN(t, 4097)
			}
			topology := durableFaultTopology(t, participants)
			base := newDurableFaultMemoryLedger()
			normal := &durableFaultLedger{base: base, rule: new(durableFaultRule)}
			ahead := &durableAheadLedger{
				durableFaultLedger: &durableFaultLedger{base: base, rule: new(durableFaultRule)},
				operation:          testCase.operation,
				applied:            make(chan struct{}),
				release:            make(chan struct{}),
			}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(ahead.release) }) }
			defer release()
			data := newDurableRunnerData()
			steps := durableFaultSteps()
			first := newDurableFaultExecutor(t, topology, ahead, &durableFaultRunner{
				steps: steps, rule: new(durableRunnerFaultRule), data: data,
			})
			second := newDurableFaultExecutor(t, topology, normal, &durableFaultRunner{
				steps: steps, rule: new(durableRunnerFaultRule), data: data,
			})
			request := durableFaultRequestWith(
				t, participants, replication.ID128{0x7c, byte(testCase.operation)},
				replication.Digest{0x7d, byte(testCase.operation)}, 7,
			)
			ctx, cancel := context.WithTimeout(
				WithLocalReplicatedTransactionRequestScope(t.Context()), 10*time.Second,
			)
			defer cancel()
			type executionResult struct {
				outcome DurableRequestOutcome
				err     error
			}
			firstResult := make(chan executionResult, 1)
			go func() {
				outcome, err := first.Execute(ctx, request)
				firstResult <- executionResult{outcome: outcome, err: err}
			}()
			select {
			case <-ahead.applied:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			secondOutcome, secondErr := second.Execute(ctx, request)
			release()
			var firstValue executionResult
			select {
			case firstValue = <-firstResult:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if secondErr != nil || !secondOutcome.Committed ||
				secondOutcome.ShardsFanned != len(participants) {
				t.Fatalf("ahead gateway outcome=%+v err=%v", secondOutcome, secondErr)
			}
			if firstValue.err != nil || !firstValue.outcome.Committed ||
				firstValue.outcome.ID != secondOutcome.ID ||
				firstValue.outcome.AffectedRows != secondOutcome.AffectedRows ||
				firstValue.outcome.CatalogGeneration != secondOutcome.CatalogGeneration ||
				firstValue.outcome.ShardsFanned != secondOutcome.ShardsFanned ||
				firstValue.outcome.AckToken != secondOutcome.AckToken ||
				!bytes.Equal(firstValue.outcome.Result, secondOutcome.Result) {
				t.Fatalf("lost-response gateway outcome=%+v err=%v, ahead=%+v", firstValue.outcome, firstValue.err, secondOutcome)
			}
			attempts, applications := data.snapshot()
			if len(attempts) != len(steps) || len(applications) != len(steps) {
				t.Fatalf("converged attempts=%d unique=%d, want %d", len(attempts), len(applications), len(steps))
			}
			for command, effects := range applications {
				if effects != 1 {
					t.Fatalf("converged command %q effects=%d", command, effects)
				}
			}
		})
	}
}

func TestDurableRequestRoutesIndependentIdentitiesToTwoLedgerHomes(t *testing.T) {
	participants := durableFaultParticipants(t)
	topology := durableFaultTopology(t, participants)
	base := newDurableFaultMemoryLedger()
	ledger := &durableFaultLedger{base: base, rule: new(durableFaultRule)}
	data := newDurableRunnerData()
	executor := newDurableFaultExecutor(t, topology, ledger, &durableFaultRunner{
		steps: durableFaultSteps(), rule: new(durableRunnerFaultRule), data: data,
	})
	ctx := WithLocalReplicatedTransactionRequestScope(t.Context())
	requests := make(map[replication.Digest]DurableRequest)
	for value := uint64(1); value < 1<<16 && len(requests) != 2; value++ {
		requestID := replication.ID128{}
		binary.LittleEndian.PutUint64(requestID[:8], value)
		request := durableFaultRequestWith(
			t, participants, requestID,
			replication.Digest{byte(value), byte(value >> 8), 0x39}, 7,
		)
		point, err := durableRequestLedgerHome(request.Key)
		if err != nil {
			t.Fatal(err)
		}
		home, ok := topology.Current().Home(requestledger.LedgerHome(point))
		if !ok {
			t.Fatal("home missing")
		}
		requests[home.Identity] = request
	}
	if len(requests) != 2 {
		t.Fatal("could not derive identities in both home ranges")
	}
	for identity, request := range requests {
		outcome, err := executor.Execute(ctx, request)
		if err != nil || !outcome.Committed {
			t.Fatalf("home %x outcome=%+v err=%v", identity, outcome, err)
		}
	}
	base.mu.Lock()
	seen := make(map[replication.Digest]struct{})
	for key := range base.entries {
		seen[key.home] = struct{}{}
	}
	base.mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("durable entries used %d homes, want 2", len(seen))
	}
}

func TestDurableRequestLedgerHomeUsesCanonicalHalfOpenDigestRanges(t *testing.T) {
	participants := durableFaultParticipants(t)
	holder := durableFaultTopology(t, participants)
	topology := holder.Current()
	boundaryMinusOne := requestledger.LedgerHome{0x7f}
	for index := 1; index < len(boundaryMinusOne); index++ {
		boundaryMinusOne[index] = 0xff
	}
	boundaryPlusOne := requestledger.LedgerHome{0x80}
	boundaryPlusOne[len(boundaryPlusOne)-1] = 1
	maximum := requestledger.LedgerHome{}
	for index := range maximum {
		maximum[index] = 0xff
	}
	testCases := []struct {
		name  string
		point requestledger.LedgerHome
		index int
	}{
		{name: "first", point: requestledger.LedgerHome{}, index: 0},
		{name: "boundary_minus_one", point: boundaryMinusOne, index: 0},
		{name: "boundary", point: requestledger.LedgerHome{0x80}, index: 1},
		{name: "boundary_plus_one", point: boundaryPlusOne, index: 1},
		{name: "maximum", point: maximum, index: 1},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			home, ok := topology.Home(testCase.point)
			if !ok || home.Identity != topology.Ranges[testCase.index].Identity {
				t.Fatalf("point=%x home=%x ok=%v, want index %d", testCase.point, home.Identity, ok, testCase.index)
			}
		})
	}
}

func TestDurableRequestLedgerTopologyDoesNotExposeMutableRoutes(t *testing.T) {
	participants := durableFaultParticipants(t)
	holder := durableFaultTopology(t, participants)
	current := holder.Current()
	wantIdentity := current.Ranges[0].Identity
	wantAddress := current.Ranges[0].Route.Replicas[0].Address
	current.Ranges[0].Identity[0] ^= 0xff
	current.Ranges[0].Route.Replicas[0].Address = "corrupted-by-reader"

	fresh := holder.Current()
	if fresh.Ranges[0].Identity != wantIdentity ||
		fresh.Ranges[0].Route.Replicas[0].Address != wantAddress {
		t.Fatalf("Current exposed mutable topology: identity=%x address=%q", fresh.Ranges[0].Identity, fresh.Ranges[0].Route.Replicas[0].Address)
	}
	point := requestledger.LedgerHome{1}
	home, ok := fresh.Home(point)
	if !ok {
		t.Fatal("home lookup failed")
	}
	selectedIdentity := home.Identity
	selectedRoute := home.ReplicatedRoute()
	selectedAddress := selectedRoute.Replicas[0].Address
	home.Identity[0] ^= 0xff
	selectedRoute.Replicas[0].Address = "corrupted-selected-home"
	again, ok := holder.Current().Home(point)
	againRoute := again.ReplicatedRoute()
	if !ok || again.Identity != selectedIdentity ||
		againRoute.Replicas[0].Address != selectedAddress {
		t.Fatalf("Home exposed mutable route: identity=%x address=%q", again.Identity, againRoute.Replicas[0].Address)
	}
}

func TestDurableRequestLedgerTopologyRejectsUnprovedRehome(t *testing.T) {
	participants := durableFaultParticipants(t)
	holder := durableFaultTopology(t, participants)
	current := holder.Current()
	ranges := make([]DurableRequestLedgerRange, len(current.Ranges))
	for index := range current.Ranges {
		ranges[index] = DurableRequestLedgerRange{
			Start: current.Ranges[index].Start, End: current.Ranges[index].End,
			Identity: current.Ranges[index].Identity,
			Route:    cloneDurableRequestRoute(current.Ranges[index].Route),
		}
	}
	ranges[0].Route.Replicas[0].Address += "-refreshed"
	if err := holder.Publish(DurableRequestLedgerTopology{
		Generation: current.Generation + 1, Ranges: ranges,
	}); err != nil {
		t.Fatalf("same-home route refresh: %v", err)
	}
	for name, mutate := range map[string]func(*ReplicatedRoute){
		"range identity":    func(route *ReplicatedRoute) { route.RangeIdentity[0]++ },
		"lineage digest":    func(route *ReplicatedRoute) { route.LineageDigest[0]++ },
		"forwarding digest": func(route *ReplicatedRoute) { route.ForwardingRuleDigest[0]++ },
	} {
		t.Run(name, func(t *testing.T) {
			drifted := append([]DurableRequestLedgerRange(nil), ranges...)
			drifted[0].Route = cloneDurableRequestRoute(ranges[0].Route)
			mutate(&drifted[0].Route)
			if err := holder.Publish(DurableRequestLedgerTopology{
				Generation: current.Generation + 2, Ranges: drifted,
			}); !errors.Is(err, ErrDurableRequest) {
				t.Fatalf("unproved logical route refresh error=%v", err)
			}
		})
	}

	added := []DurableRequestLedgerRange{
		{End: requestledger.LedgerHome{0x40}, Identity: ranges[0].Identity, Route: ranges[0].Route},
		{Start: requestledger.LedgerHome{0x40}, End: ranges[0].End, Identity: replication.Digest{3}, Route: participants[0].Route},
		ranges[1],
	}
	if err := holder.Publish(DurableRequestLedgerTopology{
		Generation: current.Generation + 2, Ranges: added,
	}); !errors.Is(err, ErrDurableRequest) {
		t.Fatalf("unproved added-home publication error=%v", err)
	}
	replaced := append([]DurableRequestLedgerRange(nil), ranges...)
	replaced[0].Identity = replication.Digest{9}
	if err := holder.Publish(DurableRequestLedgerTopology{
		Generation: current.Generation + 3, Ranges: replaced,
	}); !errors.Is(err, ErrDurableRequest) {
		t.Fatalf("unproved replaced-home publication error=%v", err)
	}
	if err := holder.Publish(DurableRequestLedgerTopology{
		Generation: current.Generation + 4, Ranges: ranges[:1],
	}); !errors.Is(err, ErrDurableRequest) {
		t.Fatalf("unproved removed-home publication error=%v", err)
	}
}

func TestDurableRequestLedgerTopologyConcurrentPublishNeverRegresses(t *testing.T) {
	participants := durableFaultParticipants(t)
	const publishers = 64
	for attempt := 0; attempt < 32; attempt++ {
		holder := durableFaultTopology(t, participants)
		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(publishers)
		for generation := uint64(2); generation <= publishers+1; generation++ {
			go func(generation uint64) {
				defer wait.Done()
				<-start
				current := holder.Current()
				_ = holder.Publish(DurableRequestLedgerTopology{
					Generation: generation, Ranges: current.Ranges,
				})
			}(generation)
		}
		close(start)
		wait.Wait()
		if got := holder.Current().Generation; got != publishers+1 {
			t.Fatalf("attempt %d final generation=%d, want %d", attempt, got, publishers+1)
		}
	}
}
