// Package raftserve provides a bounded proposal waiter and result settlement
// safe point. It does not provide a listener, authentication, leader routing,
// gateway integration, or a complete serving protocol.
package raftserve

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"sync"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	AbsoluteMaxOutstandingIdentities         = 65536
	AbsoluteMaxOutstandingAttempts           = 65536
	AbsoluteMaxWaiters                       = 65536
	AbsoluteMaxAttemptsPerIdentity           = 64
	AbsoluteMaxRetainedCompletionBytes int64 = 128 << 20

	tenantSlotBytes     = replication.MaxIdentityBytes
	completionSlotBytes = replication.MaxEmptyResultCompletionEnvelopeBytes
)

const noIndex = ^uint32(0)

var (
	ErrInvalidLimits              = errors.New("raftserve: invalid registry limits")
	ErrRegistryClosed             = errors.New("raftserve: registry is closed")
	ErrGroupCapacity              = errors.New("raftserve: outstanding group capacity reached")
	ErrIdentityCapacity           = errors.New("raftserve: outstanding identity capacity reached")
	ErrAttemptCapacity            = errors.New("raftserve: outstanding attempt capacity reached")
	ErrWaiterCapacity             = errors.New("raftserve: waiter capacity reached")
	ErrAttemptsPerIdentity        = errors.New("raftserve: attempts per identity capacity reached")
	ErrCommandGroupMismatch       = errors.New("raftserve: command belongs to another group")
	ErrWaiterClosed               = errors.New("raftserve: waiter is closed")
	ErrWaiterPending              = errors.New("raftserve: waiter result is pending")
	ErrWaiterBusy                 = errors.New("raftserve: waiter already has a blocking claimant")
	ErrWaitContext                = errors.New("raftserve: wait context is required")
	ErrCompletionDestinationSmall = errors.New("raftserve: completion destination is too small")
	ErrSettlementRange            = errors.New("raftserve: invalid applied settlement range")
	ErrSettlementResult           = errors.New("raftserve: invalid applied settlement result")
	ErrRegistryCorrupt            = errors.New("raftserve: registry invariant failure")
	ErrGenerationExhausted        = errors.New("raftserve: registry generation exhausted")
	ErrProposalRefused            = errors.New("raftserve: proposal refused before local core admission")
)

// Limits fixes every retained registry dimension. All fields are required.
type Limits struct {
	MaxGroups                  int
	MaxOutstandingIdentities   int
	MaxOutstandingAttempts     int
	MaxWaiters                 int
	MaxAttemptsPerIdentity     int
	MaxRetainedCompletionBytes int64
}

// Stats is a detached view of live and fixed-capacity registry storage.
type Stats struct {
	OutstandingGroups       int
	OutstandingIdentities   int
	OutstandingAttempts     int
	Waiters                 int
	RetainedCompletionBytes int
	PeakIdentities          int
	PeakGroups              int
	PeakAttempts            int
	PeakWaiters             int
	PeakCompletionBytes     int
	IdentityCapacity        int
	AttemptCapacity         int
	WaiterCapacity          int
	TableCapacity           int
	GroupCapacity           int
	GroupTableCapacity      int
	PendingGroups           int
	PendingAdmittedAttempts int
	TenantArenaBytes        int
	CompletionArenaBytes    int
	LastLookupProbes        int
	PeakLookupProbes        int
}

type tableState uint8

const (
	tableEmpty tableState = iota
	tableOccupied
)

type positionSlot struct {
	hash       uint64
	entry      uint32
	generation uint64
	state      tableState
}

type pendingGroupSlot struct {
	group           raftmember.GroupKey
	hash            uint64
	identityCount   uint32
	pendingAttempts uint32
	entryHead       uint32
	state           tableState
}

type entryRecord struct {
	position          requestPosition
	fingerprint       replication.Digest
	logical           [32]byte
	generation        uint64
	completionApplied uint64
	freeOrGroupNext   uint32
	groupPrevious     uint32
	attemptHead       uint32
	tableSlot         uint32
	attemptCount      uint16
	tenantLen         uint16
	completionLen     uint16
	active            bool
}

type attemptState uint8

const (
	attemptPending attemptState = iota + 1
	attemptSettling
	attemptComplete
)

type attemptRecord struct {
	digest           [32]byte
	generation       uint64
	appliedIndex     uint64
	freeNext         uint32
	next             uint32
	waiterHead       uint32
	entry            uint32
	entryGeneration  uint64
	waiterCount      uint32
	outcome          OutcomeCode
	state            attemptState
	hasCompletion    bool
	lifecyclePending bool
	admitted         bool
	active           bool
}

type waiterRecord struct {
	wake              chan struct{}
	generation        uint64
	freeNext          uint32
	previous          uint32
	next              uint32
	attempt           uint32
	attemptGeneration uint64
	blocking          bool
	active            bool
}

// ProposalEnqueuer is the serialized tracked Host ingress used after registry
// reservation. multiraft.Host implements it.
type ProposalEnqueuer interface {
	EnqueueTrackedProposal(
		raftmember.GroupKey,
		[]byte,
		multiraft.ProposalToken,
	) error
}

// Registry owns bounded waiter identities for one or more serialized Host
// lanes. Enqueue calls for a particular Host must obey that Host's single-owner
// rule. Cancellation, waiting, result copying, and settlement are synchronized.
type Registry struct {
	mu       sync.Mutex
	settleMu sync.Mutex
	limits   Limits
	seed     maphash.Seed
	hashMask uint64

	table             []positionSlot
	groupTable        []pendingGroupSlot
	entries           []entryRecord
	attempts          []attemptRecord
	waiters           []waiterRecord
	tenantArena       []byte
	completionArena   []byte
	settlementScratch []byte

	freeEntry   uint32
	freeAttempt uint32
	freeWaiter  uint32
	stats       Stats
	failure     error
	closed      bool
}

// NewRegistry allocates every retained table, identity byte, completion byte,
// and waiter wake channel before any proposal can be registered.
func NewRegistry(limits Limits) (*Registry, error) {
	completionBytes, tenantBytes, tableSize, groupTableSize, err := validateLimits(limits)
	if err != nil {
		return nil, err
	}
	registry := &Registry{
		limits:            limits,
		seed:              maphash.MakeSeed(),
		hashMask:          math.MaxUint64,
		table:             make([]positionSlot, tableSize),
		groupTable:        make([]pendingGroupSlot, groupTableSize),
		entries:           make([]entryRecord, limits.MaxOutstandingIdentities),
		attempts:          make([]attemptRecord, limits.MaxOutstandingAttempts),
		waiters:           make([]waiterRecord, limits.MaxWaiters),
		tenantArena:       make([]byte, tenantBytes),
		completionArena:   make([]byte, completionBytes),
		settlementScratch: make([]byte, completionSlotBytes),
		freeEntry:         0, freeAttempt: 0, freeWaiter: 0,
	}
	for index := range registry.entries {
		registry.entries[index].freeOrGroupNext = nextFree(index, len(registry.entries))
		registry.entries[index].groupPrevious = noIndex
		registry.entries[index].attemptHead = noIndex
		registry.entries[index].tableSlot = noIndex
	}
	for index := range registry.attempts {
		registry.attempts[index].freeNext = nextFree(index, len(registry.attempts))
		registry.attempts[index].next = noIndex
		registry.attempts[index].waiterHead = noIndex
	}
	for index := range registry.waiters {
		registry.waiters[index].freeNext = nextFree(index, len(registry.waiters))
		registry.waiters[index].previous = noIndex
		registry.waiters[index].next = noIndex
		registry.waiters[index].attempt = noIndex
		registry.waiters[index].wake = make(chan struct{}, 1)
	}
	registry.stats.IdentityCapacity = len(registry.entries)
	registry.stats.AttemptCapacity = len(registry.attempts)
	registry.stats.WaiterCapacity = len(registry.waiters)
	registry.stats.TableCapacity = len(registry.table)
	registry.stats.GroupCapacity = limits.MaxGroups
	registry.stats.GroupTableCapacity = len(registry.groupTable)
	registry.stats.TenantArenaBytes = len(registry.tenantArena)
	registry.stats.CompletionArenaBytes = len(registry.completionArena)
	return registry, nil
}

func validateLimits(limits Limits) (
	completionBytes int,
	tenantBytes int,
	tableSize int,
	groupTableSize int,
	err error,
) {
	if limits.MaxGroups <= 0 || limits.MaxGroups > multiraft.AbsoluteMaxGroups ||
		limits.MaxOutstandingIdentities <= 0 ||
		limits.MaxOutstandingIdentities > AbsoluteMaxOutstandingIdentities ||
		limits.MaxOutstandingAttempts < limits.MaxOutstandingIdentities ||
		limits.MaxOutstandingAttempts > AbsoluteMaxOutstandingAttempts ||
		limits.MaxWaiters < limits.MaxOutstandingIdentities ||
		limits.MaxWaiters > AbsoluteMaxWaiters ||
		limits.MaxAttemptsPerIdentity <= 0 ||
		limits.MaxAttemptsPerIdentity > AbsoluteMaxAttemptsPerIdentity ||
		limits.MaxOutstandingAttempts < limits.MaxAttemptsPerIdentity ||
		limits.MaxRetainedCompletionBytes <= 0 ||
		limits.MaxRetainedCompletionBytes > AbsoluteMaxRetainedCompletionBytes {
		return 0, 0, 0, 0, ErrInvalidLimits
	}
	completion64 := int64(limits.MaxOutstandingIdentities) * int64(completionSlotBytes)
	tenant64 := int64(limits.MaxOutstandingIdentities) * int64(tenantSlotBytes)
	maxInt := int64(^uint(0) >> 1)
	if completion64 > limits.MaxRetainedCompletionBytes ||
		completion64 > maxInt || tenant64 > maxInt {
		return 0, 0, 0, 0, ErrInvalidLimits
	}
	tableSize = 1
	for tableSize < limits.MaxOutstandingIdentities*2 {
		tableSize <<= 1
	}
	groupTableSize = 1
	for groupTableSize < limits.MaxGroups*2 {
		groupTableSize <<= 1
	}
	return int(completion64), int(tenant64), tableSize, groupTableSize, nil
}

func nextFree(index, count int) uint32 {
	if index+1 >= count {
		return noIndex
	}
	return uint32(index + 1)
}

// Enqueue validates and reserves one exact command attempt before Host queue
// admission. Exact repeats share an attempt and do not amplify the Raft log.
// A changed acknowledgement or mutable fence creates a distinct attempt and is
// enqueued once while sharing the logical result identity.
func (registry *Registry) Enqueue(
	host ProposalEnqueuer,
	group raftmember.GroupKey,
	data []byte,
) (Waiter, error) {
	if registry == nil {
		return Waiter{}, ErrRegistryClosed
	}
	if host == nil {
		return Waiter{}, multiraft.ErrHostClosed
	}
	identity, err := openCommandIdentity(group, data)
	if err != nil {
		return Waiter{}, err
	}
	registry.mu.Lock()
	waiter, token, enqueue, err := registry.registerLocked(identity)
	registry.mu.Unlock()
	if err != nil || !enqueue {
		return waiter, err
	}
	if err := host.EnqueueTrackedProposal(group, data, token.proposalToken()); err != nil {
		registry.mu.Lock()
		registry.rollbackRegistrationLocked(token)
		registry.mu.Unlock()
		return Waiter{}, err
	}
	return waiter, nil
}

type registrationToken struct {
	entry             uint32
	entryGeneration   uint64
	attempt           uint32
	attemptGeneration uint64
	waiter            uint32
	waiterGeneration  uint64
}

func (token registrationToken) proposalToken() multiraft.ProposalToken {
	return multiraft.ProposalToken{
		uint64(token.entry), token.entryGeneration,
		uint64(token.attempt), token.attemptGeneration,
	}
}

func (registry *Registry) registerLocked(
	identity commandIdentity,
) (Waiter, registrationToken, bool, error) {
	if registry == nil || registry.closed {
		return Waiter{}, registrationToken{}, false, ErrRegistryClosed
	}
	if registry.failure != nil {
		return Waiter{}, registrationToken{}, false, registry.failure
	}
	hash := hashPosition(registry.seed, identity.position, identity.tenant) & registry.hashMask
	entryIndex, tableIndex, found, err := registry.findEntryLocked(identity, hash)
	if err != nil {
		return Waiter{}, registrationToken{}, false, err
	}
	if found {
		entry := &registry.entries[entryIndex]
		if entry.fingerprint != identity.fingerprint || entry.logical != identity.logical {
			return Waiter{}, registrationToken{}, false, &replicatedstate.RequestConflictError{
				Key: identity.position.sessionDigest,
			}
		}
		if attemptIndex, ok := registry.findAttemptLocked(entry, identity.attempt); ok {
			waiterIndex, waiterGeneration, allocErr := registry.allocateWaiterLocked(attemptIndex)
			if allocErr != nil {
				return Waiter{}, registrationToken{}, false, allocErr
			}
			return Waiter{
				registry: registry, index: waiterIndex, generation: waiterGeneration,
			}, registrationToken{}, false, nil
		}
		if int(entry.attemptCount) >= registry.limits.MaxAttemptsPerIdentity {
			return Waiter{}, registrationToken{}, false, ErrAttemptsPerIdentity
		}
		attemptIndex, attemptGeneration, allocErr := registry.allocateAttemptLocked(entryIndex)
		if allocErr != nil {
			return Waiter{}, registrationToken{}, false, allocErr
		}
		attempt := &registry.attempts[attemptIndex]
		attempt.digest = identity.attempt
		waiterIndex, waiterGeneration, allocErr := registry.allocateWaiterLocked(attemptIndex)
		if allocErr != nil {
			registry.removeAttemptLocked(attemptIndex)
			return Waiter{}, registrationToken{}, false, allocErr
		}
		token := registrationToken{
			entry: entryIndex, entryGeneration: entry.generation,
			attempt: attemptIndex, attemptGeneration: attemptGeneration,
			waiter: waiterIndex, waiterGeneration: waiterGeneration,
		}
		return Waiter{
			registry: registry, index: waiterIndex, generation: waiterGeneration,
		}, token, true, nil
	}
	if registry.freeEntry == noIndex {
		return Waiter{}, registrationToken{}, false, ErrIdentityCapacity
	}
	if registry.freeAttempt == noIndex {
		return Waiter{}, registrationToken{}, false, ErrAttemptCapacity
	}
	if registry.freeWaiter == noIndex {
		return Waiter{}, registrationToken{}, false, ErrWaiterCapacity
	}
	groupIndex, retainErr := registry.retainGroupIdentityLocked(identity.position.group)
	if retainErr != nil {
		return Waiter{}, registrationToken{}, false, retainErr
	}
	entryIndex, entryGeneration, allocErr := registry.allocateEntryLocked(
		identity, hash, tableIndex, groupIndex,
	)
	if allocErr != nil {
		if !registry.releaseGroupIdentityLocked(identity.position.group) {
			registry.poisonLocked(errors.New("new identity lost its group capacity record"))
		}
		return Waiter{}, registrationToken{}, false, allocErr
	}
	attemptIndex, attemptGeneration, allocErr := registry.allocateAttemptLocked(entryIndex)
	if allocErr != nil {
		registry.removeEntryLocked(entryIndex)
		return Waiter{}, registrationToken{}, false, allocErr
	}
	registry.attempts[attemptIndex].digest = identity.attempt
	waiterIndex, waiterGeneration, allocErr := registry.allocateWaiterLocked(attemptIndex)
	if allocErr != nil {
		registry.removeAttemptLocked(attemptIndex)
		return Waiter{}, registrationToken{}, false, allocErr
	}
	token := registrationToken{
		entry: entryIndex, entryGeneration: entryGeneration,
		attempt: attemptIndex, attemptGeneration: attemptGeneration,
		waiter: waiterIndex, waiterGeneration: waiterGeneration,
	}
	return Waiter{
		registry: registry, index: waiterIndex, generation: waiterGeneration,
	}, token, true, nil
}

func (registry *Registry) rollbackRegistrationLocked(token registrationToken) {
	if int(token.attempt) >= len(registry.attempts) ||
		int(token.entry) >= len(registry.entries) {
		return
	}
	attempt := &registry.attempts[token.attempt]
	entry := &registry.entries[token.entry]
	if !attempt.active || attempt.generation != token.attemptGeneration ||
		!entry.active || entry.generation != token.entryGeneration ||
		attempt.state != attemptPending {
		return
	}
	attempt.lifecyclePending = false
	if int(token.waiter) < len(registry.waiters) {
		waiter := &registry.waiters[token.waiter]
		if waiter.active && waiter.generation == token.waiterGeneration {
			registry.releaseWaiterLocked(token.waiter)
		}
	}
	if attempt.active && attempt.generation == token.attemptGeneration {
		if attempt.waiterCount == 0 {
			registry.removeAttemptLocked(token.attempt)
		} else {
			attempt.outcome = OutcomeProposalRefused
			attempt.state = attemptComplete
			registry.signalAttemptLocked(attempt)
		}
	}
}

func (registry *Registry) settleProposalAdmission(
	admission multiraft.ProposalAdmission,
) {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed || registry.failure != nil {
		return
	}
	entryIndex := uint32(admission.Token[0])
	entryGeneration := admission.Token[1]
	attemptIndex := uint32(admission.Token[2])
	attemptGeneration := admission.Token[3]
	if admission.Token == (multiraft.ProposalToken{}) ||
		uint64(entryIndex) != admission.Token[0] ||
		uint64(attemptIndex) != admission.Token[2] ||
		admission.Admitted == (admission.Cause != nil) {
		registry.poisonLocked(errors.New("malformed proposal lifecycle admission"))
		return
	}
	if int(attemptIndex) >= len(registry.attempts) ||
		int(entryIndex) >= len(registry.entries) {
		registry.poisonLocked(errors.New("unknown proposal lifecycle token"))
		return
	}
	attempt := &registry.attempts[attemptIndex]
	entry := &registry.entries[entryIndex]
	if !attempt.active || attempt.generation != attemptGeneration ||
		attempt.entry != entryIndex || !entry.active || entry.generation != entryGeneration ||
		entry.position.group != admission.Group || !attempt.lifecyclePending {
		registry.poisonLocked(errors.New("stale or duplicate proposal lifecycle token"))
		return
	}
	attempt.lifecyclePending = false
	if admission.Admitted {
		if attempt.state == attemptPending {
			if !registry.addGroupPendingLocked(entry.position.group) {
				registry.poisonLocked(errors.New("admitted proposal has no group capacity record"))
				return
			}
			attempt.admitted = true
		}
		if attempt.waiterCount == 0 && registry.attemptRemovableLocked(attempt) {
			registry.removeAttemptLocked(attemptIndex)
		}
		return
	}
	if attempt.state == attemptComplete {
		if attempt.waiterCount == 0 {
			registry.removeAttemptLocked(attemptIndex)
		}
		return
	}
	if attempt.state != attemptPending || attempt.admitted {
		registry.poisonLocked(errors.New("proposal refusal after local admission"))
		return
	}
	attempt.outcome = OutcomeProposalRefused
	if errors.Is(admission.Cause, raftmodel.ErrNotLeader) {
		attempt.outcome = OutcomeNotLeader
	} else if deterministic, known := outcomeCode(admission.Cause); known {
		attempt.outcome = deterministic
	}
	attempt.state = attemptComplete
	registry.signalAttemptLocked(attempt)
	if attempt.waiterCount == 0 {
		registry.removeAttemptLocked(attemptIndex)
	}
}

// TerminateGroup resolves every admitted pending attempt whose local apply path
// ended at one closed infrastructure boundary. The caller must linearize the
// boundary after fencing old Host ingress and before new-epoch admission.
func (registry *Registry) TerminateGroup(
	group raftmember.GroupKey,
	reason multiraft.ProposalGroupTerminationReason,
) error {
	if registry == nil {
		return ErrRegistryClosed
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	if registry.failure != nil {
		return registry.failure
	}
	return registry.terminateGroupLocked(group, reason)
}

func (registry *Registry) settleProposalGroupTermination(
	termination multiraft.ProposalGroupTermination,
) {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed || registry.failure != nil {
		return
	}
	if err := registry.terminateGroupLocked(termination.Group, termination.Reason); err != nil {
		registry.poisonLocked(err)
	}
}

func (registry *Registry) terminateGroupLocked(
	group raftmember.GroupKey,
	reason multiraft.ProposalGroupTerminationReason,
) error {
	if group == (raftmember.GroupKey{}) {
		return errors.New("invalid proposal group termination identity")
	}
	outcome := OutcomeProposalAbandoned
	switch reason {
	case multiraft.ProposalGroupLeadershipLost:
		outcome = OutcomeNotLeader
	case multiraft.ProposalGroupRemoved,
		multiraft.ProposalGroupFaulted,
		multiraft.ProposalHostClosed:
	case 0:
		return errors.New("invalid proposal group termination reason")
	default:
		return errors.New("unknown proposal group termination reason")
	}
	groupIndex, found, groupErr := registry.findGroupLocked(group)
	if groupErr != nil {
		return groupErr
	}
	if !found {
		return nil
	}
	groupRecord := &registry.groupTable[groupIndex]
	if groupRecord.pendingAttempts == 0 {
		return nil
	}
	if groupRecord.identityCount == 0 || groupRecord.entryHead == noIndex {
		return ErrRegistryCorrupt
	}
	pendingToRelease := 0
	visitedEntries := uint32(0)
	previousEntry := noIndex
	for entryIndex := groupRecord.entryHead; entryIndex != noIndex; {
		if int(entryIndex) >= len(registry.entries) {
			return ErrRegistryCorrupt
		}
		entry := &registry.entries[entryIndex]
		if !entry.active || entry.position.group != group ||
			entry.groupPrevious != previousEntry || entry.attemptCount == 0 {
			return ErrRegistryCorrupt
		}
		visitedAttempts := uint16(0)
		for attemptIndex := entry.attemptHead; attemptIndex != noIndex; {
			if int(attemptIndex) >= len(registry.attempts) {
				return ErrRegistryCorrupt
			}
			attempt := &registry.attempts[attemptIndex]
			if !attempt.active || attempt.entry != entryIndex ||
				attempt.entryGeneration != entry.generation {
				return ErrRegistryCorrupt
			}
			visitedAttempts++
			if visitedAttempts > entry.attemptCount {
				return ErrRegistryCorrupt
			}
			if attempt.state == attemptPending && attempt.admitted &&
				!attempt.lifecyclePending {
				pendingToRelease++
			}
			attemptIndex = attempt.next
		}
		if visitedAttempts != entry.attemptCount {
			return ErrRegistryCorrupt
		}
		visitedEntries++
		if visitedEntries > groupRecord.identityCount {
			return ErrRegistryCorrupt
		}
		previousEntry = entryIndex
		entryIndex = entry.freeOrGroupNext
	}
	if visitedEntries != groupRecord.identityCount ||
		groupRecord.pendingAttempts != uint32(pendingToRelease) ||
		registry.stats.PendingGroups == 0 ||
		registry.stats.PendingAdmittedAttempts < pendingToRelease {
		return ErrRegistryCorrupt
	}
	for entryIndex := groupRecord.entryHead; entryIndex != noIndex; {
		entry := &registry.entries[entryIndex]
		nextEntry := entry.freeOrGroupNext
		for attemptIndex := entry.attemptHead; attemptIndex != noIndex; {
			attempt := &registry.attempts[attemptIndex]
			next := attempt.next
			if attempt.active && attempt.entryGeneration == entry.generation &&
				attempt.state == attemptPending && attempt.admitted &&
				!attempt.lifecyclePending {
				if !registry.removeGroupPendingLocked(group) {
					return ErrRegistryCorrupt
				}
				attempt.admitted = false
				attempt.outcome = outcome
				attempt.state = attemptComplete
				registry.signalAttemptLocked(attempt)
				if attempt.waiterCount == 0 {
					registry.removeAttemptLocked(attemptIndex)
				}
			}
			attemptIndex = next
		}
		entryIndex = nextEntry
	}
	return nil
}

func (registry *Registry) poisonLocked(cause error) {
	if registry == nil || registry.failure != nil || registry.closed {
		return
	}
	registry.failure = errors.Join(ErrRegistryCorrupt, cause)
	for index := range registry.waiters {
		if registry.waiters[index].active {
			notify(registry.waiters[index].wake)
		}
	}
}

func (registry *Registry) findEntryLocked(
	identity commandIdentity,
	hash uint64,
) (entry uint32, table uint32, found bool, err error) {
	mask := uint64(len(registry.table) - 1)
	probes := 0
	defer func() {
		registry.stats.LastLookupProbes = probes
		registry.stats.PeakLookupProbes = max(registry.stats.PeakLookupProbes, probes)
	}()
	for offset := uint64(0); offset < uint64(len(registry.table)); offset++ {
		probes++
		index := uint32((hash + offset) & mask)
		slot := &registry.table[index]
		switch slot.state {
		case tableEmpty:
			return noIndex, index, false, nil
		case tableOccupied:
			if int(slot.entry) >= len(registry.entries) {
				return noIndex, noIndex, false, ErrRegistryCorrupt
			}
			candidate := &registry.entries[slot.entry]
			if !candidate.active || candidate.generation != slot.generation ||
				candidate.tableSlot != index {
				return noIndex, noIndex, false, ErrRegistryCorrupt
			}
			if slot.hash == hash && registry.entryPositionEqual(slot.entry, candidate, identity) {
				return slot.entry, index, true, nil
			}
		}
	}
	return noIndex, noIndex, false, ErrIdentityCapacity
}

func (registry *Registry) findGroupLocked(
	group raftmember.GroupKey,
) (uint32, bool, error) {
	hash := hashGroup(registry.seed, group) & registry.hashMask
	mask := uint64(len(registry.groupTable) - 1)
	for offset := uint64(0); offset < uint64(len(registry.groupTable)); offset++ {
		index := uint32((hash + offset) & mask)
		slot := &registry.groupTable[index]
		switch slot.state {
		case tableEmpty:
			return index, false, nil
		case tableOccupied:
			if slot.hash == hash && slot.group == group {
				return index, true, nil
			}
		default:
			return noIndex, false, ErrRegistryCorrupt
		}
	}
	return noIndex, false, ErrGroupCapacity
}

func (registry *Registry) retainGroupIdentityLocked(
	group raftmember.GroupKey,
) (uint32, error) {
	if group == (raftmember.GroupKey{}) {
		return noIndex, ErrRegistryCorrupt
	}
	index, found, err := registry.findGroupLocked(group)
	if err != nil {
		return noIndex, err
	}
	if found {
		slot := &registry.groupTable[index]
		if slot.identityCount == math.MaxUint32 {
			return noIndex, ErrRegistryCorrupt
		}
		slot.identityCount++
		return index, nil
	}
	if registry.stats.OutstandingGroups >= registry.limits.MaxGroups {
		return noIndex, ErrGroupCapacity
	}
	hash := hashGroup(registry.seed, group) & registry.hashMask
	registry.groupTable[index] = pendingGroupSlot{
		group: group, hash: hash, identityCount: 1,
		entryHead: noIndex, state: tableOccupied,
	}
	registry.stats.OutstandingGroups++
	registry.stats.PeakGroups = max(
		registry.stats.PeakGroups,
		registry.stats.OutstandingGroups,
	)
	return index, nil
}

func (registry *Registry) releaseGroupIdentityLocked(
	group raftmember.GroupKey,
) bool {
	index, found, err := registry.findGroupLocked(group)
	if err != nil || !found {
		return false
	}
	slot := &registry.groupTable[index]
	if slot.identityCount == 0 {
		return false
	}
	if slot.identityCount == 1 &&
		(slot.pendingAttempts != 0 || slot.entryHead != noIndex) {
		return false
	}
	if slot.identityCount > 1 && slot.entryHead == noIndex {
		return false
	}
	slot.identityCount--
	if slot.identityCount != 0 {
		return true
	}
	registry.deleteGroupSlotLocked(index)
	registry.stats.OutstandingGroups--
	return true
}

func (registry *Registry) addGroupPendingLocked(
	group raftmember.GroupKey,
) bool {
	index, found, err := registry.findGroupLocked(group)
	if err != nil || !found {
		return false
	}
	slot := &registry.groupTable[index]
	if slot.identityCount == 0 || slot.pendingAttempts == math.MaxUint32 {
		return false
	}
	if slot.pendingAttempts == 0 {
		registry.stats.PendingGroups++
	}
	slot.pendingAttempts++
	registry.stats.PendingAdmittedAttempts++
	return true
}

func (registry *Registry) removeGroupPendingLocked(
	group raftmember.GroupKey,
) bool {
	index, found, err := registry.findGroupLocked(group)
	if err != nil || !found {
		return false
	}
	slot := &registry.groupTable[index]
	if slot.identityCount == 0 || slot.pendingAttempts == 0 ||
		registry.stats.PendingAdmittedAttempts == 0 ||
		(slot.pendingAttempts == 1 && registry.stats.PendingGroups == 0) {
		return false
	}
	slot.pendingAttempts--
	registry.stats.PendingAdmittedAttempts--
	if slot.pendingAttempts == 0 {
		registry.stats.PendingGroups--
	}
	return true
}

func (registry *Registry) hasPendingGroup(
	group raftmember.GroupKey,
) bool {
	if registry == nil {
		return true
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed || registry.failure != nil {
		return true
	}
	index, found, err := registry.findGroupLocked(group)
	if err != nil {
		registry.poisonLocked(err)
		return true
	}
	return found && registry.groupTable[index].pendingAttempts != 0
}

func (registry *Registry) unlinkGroupEntryLocked(
	entryIndex uint32,
	entry *entryRecord,
) bool {
	if entry == nil || !entry.active || int(entryIndex) >= len(registry.entries) {
		return false
	}
	groupIndex, found, err := registry.findGroupLocked(entry.position.group)
	if err != nil || !found {
		return false
	}
	group := &registry.groupTable[groupIndex]
	if group.identityCount == 0 ||
		(group.identityCount == 1 && group.pendingAttempts != 0) {
		return false
	}
	previous := entry.groupPrevious
	next := entry.freeOrGroupNext
	if previous == entryIndex || next == entryIndex ||
		(previous != noIndex && previous == next) {
		return false
	}
	if previous == noIndex {
		if group.entryHead != entryIndex {
			return false
		}
	} else {
		if int(previous) >= len(registry.entries) {
			return false
		}
		previousEntry := &registry.entries[previous]
		if !previousEntry.active ||
			previousEntry.position.group != entry.position.group ||
			previousEntry.freeOrGroupNext != entryIndex {
			return false
		}
	}
	if next != noIndex {
		if int(next) >= len(registry.entries) {
			return false
		}
		nextEntry := &registry.entries[next]
		if !nextEntry.active || nextEntry.position.group != entry.position.group ||
			nextEntry.groupPrevious != entryIndex {
			return false
		}
	}
	if group.identityCount == 1 && (previous != noIndex || next != noIndex) {
		return false
	}
	if previous == noIndex {
		group.entryHead = next
	} else {
		registry.entries[previous].freeOrGroupNext = next
	}
	if next != noIndex {
		registry.entries[next].groupPrevious = previous
	}
	entry.groupPrevious = noIndex
	entry.freeOrGroupNext = noIndex
	return true
}

func (registry *Registry) entryPositionEqual(
	entryIndex uint32,
	entry *entryRecord,
	identity commandIdentity,
) bool {
	if entry == nil || entry.position != identity.position ||
		int(entry.tenantLen) != len(identity.tenant) {
		return false
	}
	stored := registry.tenantSlot(entryIndex)
	return bytes.Equal(stored[:entry.tenantLen], identity.tenant)
}

func (registry *Registry) findAttemptLocked(
	entry *entryRecord,
	digest [32]byte,
) (uint32, bool) {
	for index := entry.attemptHead; index != noIndex; index = registry.attempts[index].next {
		attempt := &registry.attempts[index]
		if attempt.active && attempt.entryGeneration == entry.generation && attempt.digest == digest {
			return index, true
		}
	}
	return noIndex, false
}

func (registry *Registry) allocateEntryLocked(
	identity commandIdentity,
	hash uint64,
	tableIndex uint32,
	groupIndex uint32,
) (uint32, uint64, error) {
	index := registry.freeEntry
	if index == noIndex || int(index) >= len(registry.entries) {
		return noIndex, 0, ErrIdentityCapacity
	}
	if int(groupIndex) >= len(registry.groupTable) {
		return noIndex, 0, ErrRegistryCorrupt
	}
	groupSlot := &registry.groupTable[groupIndex]
	if groupSlot.state != tableOccupied ||
		groupSlot.group != identity.position.group || groupSlot.identityCount == 0 {
		return noIndex, 0, ErrRegistryCorrupt
	}
	if groupSlot.entryHead != noIndex {
		if int(groupSlot.entryHead) >= len(registry.entries) {
			return noIndex, 0, ErrRegistryCorrupt
		}
		head := &registry.entries[groupSlot.entryHead]
		if !head.active || head.position.group != identity.position.group ||
			head.groupPrevious != noIndex {
			return noIndex, 0, ErrRegistryCorrupt
		}
	}
	record := &registry.entries[index]
	generation, err := nextGeneration(record.generation)
	if err != nil {
		registry.poisonLocked(err)
		return noIndex, 0, registry.failure
	}
	registry.freeEntry = record.freeOrGroupNext
	*record = entryRecord{
		position:        identity.position,
		fingerprint:     identity.fingerprint,
		logical:         identity.logical,
		generation:      generation,
		freeOrGroupNext: groupSlot.entryHead, groupPrevious: noIndex,
		attemptHead: noIndex,
		tableSlot:   tableIndex, tenantLen: uint16(len(identity.tenant)), active: true,
	}
	if groupSlot.entryHead != noIndex {
		registry.entries[groupSlot.entryHead].groupPrevious = index
	}
	groupSlot.entryHead = index
	tenant := registry.tenantSlot(index)
	clear(tenant)
	copy(tenant, identity.tenant)
	clear(registry.completionSlot(index))
	registry.table[tableIndex] = positionSlot{
		hash: hash, entry: index, generation: generation, state: tableOccupied,
	}
	registry.stats.OutstandingIdentities++
	registry.stats.PeakIdentities = max(registry.stats.PeakIdentities, registry.stats.OutstandingIdentities)
	return index, generation, nil
}

func (registry *Registry) allocateAttemptLocked(
	entryIndex uint32,
) (uint32, uint64, error) {
	if registry.freeAttempt == noIndex || int(entryIndex) >= len(registry.entries) {
		return noIndex, 0, ErrAttemptCapacity
	}
	entry := &registry.entries[entryIndex]
	if !entry.active {
		return noIndex, 0, ErrRegistryCorrupt
	}
	index := registry.freeAttempt
	record := &registry.attempts[index]
	generation, err := nextGeneration(record.generation)
	if err != nil {
		registry.poisonLocked(err)
		return noIndex, 0, registry.failure
	}
	registry.freeAttempt = record.freeNext
	*record = attemptRecord{
		generation: generation, freeNext: noIndex,
		next: entry.attemptHead, waiterHead: noIndex,
		entry: entryIndex, entryGeneration: entry.generation,
		state: attemptPending, lifecyclePending: true, active: true,
	}
	entry.attemptHead = index
	entry.attemptCount++
	registry.stats.OutstandingAttempts++
	registry.stats.PeakAttempts = max(registry.stats.PeakAttempts, registry.stats.OutstandingAttempts)
	return index, generation, nil
}

func (registry *Registry) allocateWaiterLocked(
	attemptIndex uint32,
) (uint32, uint64, error) {
	if registry.freeWaiter == noIndex || int(attemptIndex) >= len(registry.attempts) {
		return noIndex, 0, ErrWaiterCapacity
	}
	attempt := &registry.attempts[attemptIndex]
	if !attempt.active {
		return noIndex, 0, ErrRegistryCorrupt
	}
	index := registry.freeWaiter
	record := &registry.waiters[index]
	generation, err := nextGeneration(record.generation)
	if err != nil {
		registry.poisonLocked(err)
		return noIndex, 0, registry.failure
	}
	registry.freeWaiter = record.freeNext
	drainSignal(record.wake)
	wake := record.wake
	*record = waiterRecord{
		wake: wake, generation: generation, freeNext: noIndex,
		previous: noIndex, next: attempt.waiterHead,
		attempt: attemptIndex, attemptGeneration: attempt.generation,
		active: true,
	}
	if attempt.waiterHead != noIndex {
		registry.waiters[attempt.waiterHead].previous = index
	}
	attempt.waiterHead = index
	attempt.waiterCount++
	registry.stats.Waiters++
	registry.stats.PeakWaiters = max(registry.stats.PeakWaiters, registry.stats.Waiters)
	return index, generation, nil
}

func nextGeneration(previous uint64) (uint64, error) {
	if previous == math.MaxUint64 {
		return 0, ErrGenerationExhausted
	}
	return previous + 1, nil
}

func drainSignal(signal chan struct{}) {
	select {
	case <-signal:
	default:
	}
}

func notify(signal chan struct{}) {
	select {
	case signal <- struct{}{}:
	default:
	}
}

func (registry *Registry) releaseWaiterLocked(index uint32) {
	if int(index) >= len(registry.waiters) {
		return
	}
	waiter := &registry.waiters[index]
	if !waiter.active || int(waiter.attempt) >= len(registry.attempts) {
		return
	}
	attemptIndex := waiter.attempt
	attempt := &registry.attempts[attemptIndex]
	if !attempt.active || attempt.generation != waiter.attemptGeneration {
		return
	}
	if waiter.previous == noIndex {
		attempt.waiterHead = waiter.next
	} else {
		registry.waiters[waiter.previous].next = waiter.next
	}
	if waiter.next != noIndex {
		registry.waiters[waiter.next].previous = waiter.previous
	}
	attempt.waiterCount--
	wake := waiter.wake
	generation := waiter.generation
	*waiter = waiterRecord{
		wake: wake, generation: generation, freeNext: registry.freeWaiter,
		previous: noIndex, next: noIndex, attempt: noIndex,
	}
	registry.freeWaiter = index
	registry.stats.Waiters--
	if attempt.waiterCount == 0 && registry.attemptRemovableLocked(attempt) {
		registry.removeAttemptLocked(attemptIndex)
	}
}

func (registry *Registry) attemptRemovableLocked(attempt *attemptRecord) bool {
	if attempt == nil || !attempt.active || attempt.state == attemptSettling {
		return false
	}
	if attempt.state == attemptComplete {
		return !attempt.lifecyclePending
	}
	return !attempt.lifecyclePending && !attempt.admitted
}

func (registry *Registry) removeAttemptLocked(index uint32) {
	if int(index) >= len(registry.attempts) {
		return
	}
	attempt := &registry.attempts[index]
	if !attempt.active || attempt.waiterCount != 0 || int(attempt.entry) >= len(registry.entries) {
		return
	}
	entryIndex := attempt.entry
	entry := &registry.entries[entryIndex]
	if !entry.active || entry.generation != attempt.entryGeneration {
		return
	}
	link := &entry.attemptHead
	for *link != noIndex && *link != index {
		link = &registry.attempts[*link].next
	}
	if *link != index {
		return
	}
	*link = attempt.next
	entry.attemptCount--
	generation := attempt.generation
	*attempt = attemptRecord{
		generation: generation, freeNext: registry.freeAttempt,
		next: noIndex, waiterHead: noIndex, entry: noIndex,
	}
	registry.freeAttempt = index
	registry.stats.OutstandingAttempts--
	if entry.attemptCount == 0 {
		registry.removeEntryLocked(entryIndex)
	}
}

func (registry *Registry) removeEntryLocked(index uint32) {
	if int(index) >= len(registry.entries) {
		return
	}
	entry := &registry.entries[index]
	if !entry.active || entry.attemptCount != 0 || int(entry.tableSlot) >= len(registry.table) {
		return
	}
	slot := &registry.table[entry.tableSlot]
	if slot.state != tableOccupied || slot.entry != index || slot.generation != entry.generation {
		return
	}
	group := entry.position.group
	if !registry.unlinkGroupEntryLocked(index, entry) {
		registry.poisonLocked(errors.New("identity lost its group entry link"))
		return
	}
	if !registry.releaseGroupIdentityLocked(group) {
		registry.poisonLocked(errors.New("identity lost its group capacity record"))
		return
	}
	registry.deleteTableSlotLocked(entry.tableSlot)
	clear(registry.tenantSlot(index))
	clear(registry.completionSlot(index))
	registry.stats.RetainedCompletionBytes -= int(entry.completionLen)
	generation := entry.generation
	*entry = entryRecord{
		generation: generation, freeOrGroupNext: registry.freeEntry,
		groupPrevious: noIndex,
		attemptHead:   noIndex, tableSlot: noIndex,
	}
	registry.freeEntry = index
	registry.stats.OutstandingIdentities--
}

func (registry *Registry) deleteGroupSlotLocked(hole uint32) {
	mask := uint32(len(registry.groupTable) - 1)
	for scan := (hole + 1) & mask; ; scan = (scan + 1) & mask {
		slot := registry.groupTable[scan]
		if slot.state == tableEmpty {
			registry.groupTable[hole] = pendingGroupSlot{}
			return
		}
		home := uint32(slot.hash) & mask
		toScan := (scan - home) & mask
		toHole := (hole - home) & mask
		if toHole >= toScan {
			continue
		}
		registry.groupTable[hole] = slot
		hole = scan
	}
}

func (registry *Registry) deleteTableSlotLocked(hole uint32) {
	mask := uint32(len(registry.table) - 1)
	for scan := (hole + 1) & mask; ; scan = (scan + 1) & mask {
		slot := registry.table[scan]
		if slot.state == tableEmpty {
			registry.table[hole] = positionSlot{}
			return
		}
		home := uint32(slot.hash) & mask
		toScan := (scan - home) & mask
		toHole := (hole - home) & mask
		if toHole >= toScan {
			continue
		}
		registry.table[hole] = slot
		if int(slot.entry) >= len(registry.entries) {
			registry.poisonLocked(errors.New("table shift references unknown entry"))
			return
		}
		registry.entries[slot.entry].tableSlot = hole
		hole = scan
	}
}

func (registry *Registry) tenantSlot(index uint32) []byte {
	if int(index) >= len(registry.entries) {
		return nil
	}
	start := int(index) * tenantSlotBytes
	return registry.tenantArena[start : start+tenantSlotBytes : start+tenantSlotBytes]
}

func (registry *Registry) completionSlot(index uint32) []byte {
	if int(index) >= len(registry.entries) {
		return nil
	}
	start := int(index) * completionSlotBytes
	return registry.completionArena[start : start+completionSlotBytes : start+completionSlotBytes]
}

// Stats returns current live use and immutable capacity bounds.
func (registry *Registry) Stats() Stats {
	if registry == nil {
		return Stats{}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.stats
}

// Close releases every waiter and retained byte. Callers must first close the
// associated Host successfully. A Host close blocked on result settlement must
// remain paired with an open Registry so RunOne can retry that settlement.
func (registry *Registry) Close() error {
	if registry == nil {
		return nil
	}
	registry.settleMu.Lock()
	defer registry.settleMu.Unlock()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil
	}
	registry.closed = true
	for index := range registry.waiters {
		if registry.waiters[index].active {
			notify(registry.waiters[index].wake)
		}
	}
	clear(registry.table)
	clear(registry.groupTable)
	clear(registry.tenantArena)
	clear(registry.completionArena)
	clear(registry.settlementScratch)
	for index := range registry.entries {
		generation := registry.entries[index].generation
		registry.entries[index] = entryRecord{
			generation:      generation,
			freeOrGroupNext: nextFree(index, len(registry.entries)),
			groupPrevious:   noIndex,
			attemptHead:     noIndex, tableSlot: noIndex,
		}
	}
	for index := range registry.attempts {
		generation := registry.attempts[index].generation
		registry.attempts[index] = attemptRecord{
			generation: generation, freeNext: nextFree(index, len(registry.attempts)),
			next: noIndex, waiterHead: noIndex, entry: noIndex,
		}
	}
	for index := range registry.waiters {
		wake := registry.waiters[index].wake
		generation := registry.waiters[index].generation
		registry.waiters[index] = waiterRecord{
			wake: wake, generation: generation,
			freeNext: nextFree(index, len(registry.waiters)),
			previous: noIndex, next: noIndex, attempt: noIndex,
		}
	}
	registry.freeEntry = 0
	registry.freeAttempt = 0
	registry.freeWaiter = 0
	registry.stats.OutstandingIdentities = 0
	registry.stats.OutstandingGroups = 0
	registry.stats.OutstandingAttempts = 0
	registry.stats.Waiters = 0
	registry.stats.RetainedCompletionBytes = 0
	registry.stats.PendingGroups = 0
	registry.stats.PendingAdmittedAttempts = 0
	return nil
}

// Waiter owns one bounded notification slot for one exact attempt.
type Waiter struct {
	registry   *Registry
	index      uint32
	generation uint64
}

// Poll returns ready=false while the exact attempt has not been observed in a
// published applied range.
func (waiter Waiter) Poll() (outcome Outcome, ready bool, err error) {
	registry := waiter.registry
	if registry == nil {
		return Outcome{}, false, ErrWaiterClosed
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.pollWaiterLocked(waiter)
}

// Wait waits without a helper goroutine. If cancellation linearizes before
// completion, it releases this waiter. If completion linearizes first, the
// ready outcome wins and remains owned until TakeCompletionInto or Cancel. At
// most one goroutine may block on a waiter and ctx must be non-nil.
func (waiter Waiter) Wait(ctx context.Context) (Outcome, error) {
	registry := waiter.registry
	if registry == nil {
		return Outcome{}, ErrWaiterClosed
	}
	if ctx == nil {
		return Outcome{}, ErrWaitContext
	}
	registry.mu.Lock()
	outcome, ready, err := registry.pollWaiterLocked(waiter)
	if err != nil || ready {
		registry.mu.Unlock()
		return outcome, err
	}
	record := &registry.waiters[waiter.index]
	if record.blocking {
		registry.mu.Unlock()
		return Outcome{}, ErrWaiterBusy
	}
	record.blocking = true
	signal := record.wake
	registry.mu.Unlock()
	for {
		select {
		case <-signal:
		case <-ctx.Done():
		}
		registry.mu.Lock()
		outcome, ready, err = registry.pollWaiterLocked(waiter)
		if err != nil {
			if int(waiter.index) < len(registry.waiters) {
				record = &registry.waiters[waiter.index]
				if record.active && record.generation == waiter.generation {
					record.blocking = false
				}
			}
			registry.mu.Unlock()
			return Outcome{}, err
		}
		record = &registry.waiters[waiter.index]
		if ready {
			record.blocking = false
			registry.mu.Unlock()
			return outcome, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			record.blocking = false
			registry.releaseWaiterLocked(waiter.index)
			registry.mu.Unlock()
			return Outcome{}, ctxErr
		}
		registry.mu.Unlock()
	}
}

func (registry *Registry) pollWaiterLocked(
	waiter Waiter,
) (Outcome, bool, error) {
	if registry.failure != nil {
		return Outcome{}, false, registry.failure
	}
	if int(waiter.index) >= len(registry.waiters) {
		return Outcome{}, false, ErrWaiterClosed
	}
	record := &registry.waiters[waiter.index]
	if !record.active || record.generation != waiter.generation ||
		int(record.attempt) >= len(registry.attempts) {
		return Outcome{}, false, ErrWaiterClosed
	}
	attempt := &registry.attempts[record.attempt]
	if !attempt.active || attempt.generation != record.attemptGeneration {
		return Outcome{}, false, ErrRegistryCorrupt
	}
	if attempt.state != attemptComplete {
		return Outcome{}, false, nil
	}
	entry := &registry.entries[attempt.entry]
	if !entry.active || entry.generation != attempt.entryGeneration {
		return Outcome{}, false, ErrRegistryCorrupt
	}
	completionBytes := 0
	completionApplied := uint64(0)
	if attempt.hasCompletion {
		completionBytes = int(entry.completionLen)
		completionApplied = entry.completionApplied
	}
	return Outcome{
		Code: attempt.outcome, AppliedIndex: attempt.appliedIndex,
		CompletionAppliedSequence: completionApplied,
		CompletionBytes:           completionBytes,
	}, true, nil
}

// TakeCompletionInto copies a ready canonical completion into dst and releases
// this waiter. It returns ErrCompletionDestinationSmall without changing dst
// or releasing ownership when caller capacity is insufficient.
func (waiter Waiter) TakeCompletionInto(
	dst []byte,
) ([]byte, Outcome, error) {
	registry := waiter.registry
	if registry == nil {
		return dst, Outcome{}, ErrWaiterClosed
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	outcome, ready, err := registry.pollWaiterLocked(waiter)
	if err != nil {
		return dst, Outcome{}, err
	}
	if !ready {
		return dst, Outcome{}, ErrWaiterPending
	}
	if outcome.CompletionBytes > cap(dst)-len(dst) {
		return dst, outcome, ErrCompletionDestinationSmall
	}
	record := &registry.waiters[waiter.index]
	attempt := &registry.attempts[record.attempt]
	if attempt.hasCompletion {
		entry := &registry.entries[attempt.entry]
		slot := registry.completionSlot(attempt.entry)
		dst = append(dst, slot[:entry.completionLen]...)
	}
	registry.releaseWaiterLocked(waiter.index)
	return dst, outcome, nil
}

// Cancel releases only this waiter's ownership. It never removes or blocks a
// Raft proposal that has already entered Host.
func (waiter Waiter) Cancel() bool {
	registry := waiter.registry
	if registry == nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if int(waiter.index) >= len(registry.waiters) {
		return false
	}
	record := &registry.waiters[waiter.index]
	if !record.active || record.generation != waiter.generation {
		return false
	}
	notify(record.wake)
	registry.releaseWaiterLocked(waiter.index)
	return true
}

// NewHost constructs a Host wired to this registry's settlement sink. The
// ordinary multiraft.NewHost constructor keeps its explicit no-waiter sink.
func (registry *Registry) NewHost(limits multiraft.Limits) (*multiraft.Host, error) {
	if registry == nil {
		return nil, ErrRegistryClosed
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil, ErrRegistryClosed
	}
	if registry.failure != nil {
		return nil, registry.failure
	}
	return multiraft.NewHostWithServingSinks(
		limits,
		registry.SettleAppliedBatch,
		registry.settleProposalAdmission,
		registry.settleProposalGroupTermination,
		registry.hasPendingGroup,
	)
}

func (registry *Registry) signalAttemptLocked(attempt *attemptRecord) {
	if attempt == nil {
		return
	}
	for index := attempt.waiterHead; index != noIndex; index = registry.waiters[index].next {
		notify(registry.waiters[index].wake)
	}
}

func (registry *Registry) validAttemptLocked(
	index uint32,
	generation uint64,
	state attemptState,
) (*attemptRecord, *entryRecord, bool) {
	if int(index) >= len(registry.attempts) {
		return nil, nil, false
	}
	attempt := &registry.attempts[index]
	if !attempt.active || attempt.generation != generation || attempt.state != state ||
		int(attempt.entry) >= len(registry.entries) {
		return nil, nil, false
	}
	entry := &registry.entries[attempt.entry]
	if !entry.active || entry.generation != attempt.entryGeneration {
		return nil, nil, false
	}
	return attempt, entry, true
}

func unexpectedSettlementError(err error) error {
	return fmt.Errorf("%w: %v", ErrSettlementResult, err)
}
