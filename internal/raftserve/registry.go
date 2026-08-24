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
	"sync/atomic"

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
	ErrSourceOwnerClaimed         = errors.New("raftserve: applied source already has a live owner")
	ErrSourceOwnerMismatch        = errors.New("raftserve: applied source owner mismatch")
	ErrSourceOwnersLive           = errors.New("raftserve: applied source owners are still live")
	ErrProposalRefused            = errors.New("raftserve: proposal refused before local core admission")
)

var nextRegistryID atomic.Uint64

func allocateRegistryID() (uint64, error) {
	for {
		current := nextRegistryID.Load()
		if current == math.MaxUint64 {
			return 0, ErrGenerationExhausted
		}
		if nextRegistryID.CompareAndSwap(current, current+1) {
			return current + 1, nil
		}
	}
}

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
	LiveSourceOwners        int
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
	group                raftmember.GroupKey
	hash                 uint64
	allocationGeneration uint64
	memberID             uint64
	storeID              [16]byte
	nodeIncarnation      uint64
	ownerEpoch           uint64
	identityCount        uint32
	pendingAttempts      uint32
	entryHead            uint32
	state                tableState
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
	ownerEpoch       uint64
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
	settlementPinned bool
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
	releasePending    bool
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
// lanes. Live Hosts sharing a Registry must own disjoint GroupKeys. Replacing a
// GroupKey owner requires fencing old ingress, terminating and closing the old
// Host, and releasing its exact registry/epoch capability before replacement
// admission. Cancellation, waiting, result copying, ownership, and settlement
// are synchronized.
type Registry struct {
	mu             sync.Mutex
	settleMu       sync.Mutex
	limits         Limits
	seed           maphash.Seed
	hashMask       uint64
	registryID     uint64
	nextOwnerEpoch uint64

	table             []positionSlot
	groupTable        []pendingGroupSlot
	entries           []entryRecord
	attempts          []attemptRecord
	waiters           []waiterRecord
	tenantArena       []byte
	completionArena   []byte
	settlementScratch []byte
	settlementLookup  raftmember.AppliedBatchCompletionWorkspace

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
	registryID, err := allocateRegistryID()
	if err != nil {
		return nil, err
	}
	registry := &Registry{
		limits:            limits,
		seed:              maphash.MakeSeed(),
		hashMask:          math.MaxUint64,
		registryID:        registryID,
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
// enqueued once while sharing the logical result identity. The selected Host
// must be the sole live owner of the group across every Host sharing this
// Registry; an old owner must be fenced, terminated, and closed before
// replacement admission.
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
		if errors.Is(err, ErrRegistryCorrupt) {
			return Waiter{}, registrationToken{}, false, registry.corruptLocked(err)
		}
		return Waiter{}, registrationToken{}, false, err
	}
	if found {
		entry := &registry.entries[entryIndex]
		if entry.fingerprint != identity.fingerprint || entry.logical != identity.logical {
			return Waiter{}, registrationToken{}, false, &replicatedstate.RequestConflictError{
				Key: identity.position.sessionDigest,
			}
		}
		ownerEpoch, sourceErr := registry.groupOwnerEpochLocked(identity.position.group)
		if sourceErr != nil {
			return Waiter{}, registrationToken{}, false, registry.corruptLocked(sourceErr)
		}
		attemptIndex, ok, findErr := registry.findAttemptLocked(
			entry, identity.attempt, ownerEpoch,
		)
		if findErr != nil {
			return Waiter{}, registrationToken{}, false, registry.corruptLocked(findErr)
		}
		if ok {
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
		attemptIndex, attemptGeneration, allocErr := registry.allocateAttemptLocked(
			entryIndex, ownerEpoch,
		)
		if allocErr != nil {
			return Waiter{}, registrationToken{}, false, allocErr
		}
		attempt := &registry.attempts[attemptIndex]
		attempt.digest = identity.attempt
		waiterIndex, waiterGeneration, allocErr := registry.allocateWaiterLocked(attemptIndex)
		if allocErr != nil {
			if !registry.removeAttemptLocked(attemptIndex) {
				return Waiter{}, registrationToken{}, false, registry.failure
			}
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
		if registry.stats.OutstandingIdentities != len(registry.entries) {
			return Waiter{}, registrationToken{}, false, registry.corruptLocked(
				errors.New("identity free list ended before capacity"),
			)
		}
		return Waiter{}, registrationToken{}, false, ErrIdentityCapacity
	}
	if registry.freeAttempt == noIndex {
		if registry.stats.OutstandingAttempts != len(registry.attempts) {
			return Waiter{}, registrationToken{}, false, registry.corruptLocked(
				errors.New("attempt free list ended before capacity"),
			)
		}
		return Waiter{}, registrationToken{}, false, ErrAttemptCapacity
	}
	if registry.freeWaiter == noIndex {
		if registry.stats.Waiters != len(registry.waiters) {
			return Waiter{}, registrationToken{}, false, registry.corruptLocked(
				errors.New("waiter free list ended before capacity"),
			)
		}
		return Waiter{}, registrationToken{}, false, ErrWaiterCapacity
	}
	groupIndex, retainErr := registry.retainGroupIdentityLocked(identity.position.group)
	if retainErr != nil {
		if errors.Is(retainErr, ErrRegistryCorrupt) {
			return Waiter{}, registrationToken{}, false, registry.corruptLocked(retainErr)
		}
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
	ownerEpoch := registry.groupTable[groupIndex].ownerEpoch
	attemptIndex, attemptGeneration, allocErr := registry.allocateAttemptLocked(
		entryIndex, ownerEpoch,
	)
	if allocErr != nil {
		if !registry.removeEntryLocked(entryIndex) {
			return Waiter{}, registrationToken{}, false, registry.failure
		}
		return Waiter{}, registrationToken{}, false, allocErr
	}
	registry.attempts[attemptIndex].digest = identity.attempt
	waiterIndex, waiterGeneration, allocErr := registry.allocateWaiterLocked(attemptIndex)
	if allocErr != nil {
		if !registry.removeAttemptLocked(attemptIndex) {
			return Waiter{}, registrationToken{}, false, registry.failure
		}
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
		attempt.entry != token.entry || !entry.active ||
		entry.generation != token.entryGeneration ||
		!attempt.lifecyclePending || attempt.admitted {
		return
	}
	if attempt.state != attemptPending && attempt.state != attemptSettling &&
		attempt.state != attemptComplete {
		registry.poisonLocked(errors.New("rollback found invalid proposal lifecycle state"))
		return
	}
	attempt.lifecyclePending = false
	if int(token.waiter) < len(registry.waiters) {
		waiter := &registry.waiters[token.waiter]
		if waiter.active && waiter.generation == token.waiterGeneration {
			if !registry.releaseWaiterLocked(token.waiter) {
				return
			}
		}
	}
	if attempt.active && attempt.generation == token.attemptGeneration {
		switch attempt.state {
		case attemptPending:
			if attempt.waiterCount == 0 {
				registry.removeAttemptLocked(token.attempt)
				return
			}
			attempt.outcome = OutcomeProposalRefused
			attempt.state = attemptComplete
			if !registry.signalAttemptLocked(token.attempt, attempt) {
				return
			}
		case attemptSettling:
			attempt.outcome = OutcomeProposalRefused
		case attemptComplete:
			if attempt.waiterCount == 0 && registry.attemptRemovableLocked(attempt) {
				registry.removeAttemptLocked(token.attempt)
			}
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
	ownerEpoch, sourceErr := registry.validateCallbackSourceLocked(
		admission.Group, admission.SourceOwner, admission.SourceToken,
	)
	if sourceErr != nil {
		registry.poisonLocked(errors.Join(errors.New("proposal admission source"), sourceErr))
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
		switch attempt.state {
		case attemptPending, attemptSettling:
			if attempt.ownerEpoch != ownerEpoch {
				registry.poisonLocked(errors.New("admitted proposal changed source epoch"))
				return
			}
			if !registry.addGroupPendingLocked(entry.position.group) {
				registry.poisonLocked(errors.New("admitted proposal has no group capacity record"))
				return
			}
			attempt.admitted = true
		case attemptComplete:
		default:
			registry.poisonLocked(errors.New("proposal admission found an invalid attempt state"))
			return
		}
		if attempt.waiterCount == 0 && registry.attemptRemovableLocked(attempt) {
			if !registry.removeAttemptLocked(attemptIndex) {
				return
			}
		}
		return
	}
	if attempt.state == attemptComplete {
		if attempt.waiterCount == 0 && registry.attemptRemovableLocked(attempt) {
			if !registry.removeAttemptLocked(attemptIndex) {
				return
			}
		}
		return
	}
	if attempt.admitted || (attempt.state != attemptPending && attempt.state != attemptSettling) {
		registry.poisonLocked(errors.New("proposal refusal after local admission"))
		return
	}
	attempt.outcome = OutcomeProposalRefused
	if errors.Is(admission.Cause, raftmodel.ErrNotLeader) {
		attempt.outcome = OutcomeNotLeader
	} else if deterministic, known := outcomeCode(admission.Cause); known {
		attempt.outcome = deterministic
	}
	if attempt.state == attemptSettling {
		return
	}
	attempt.state = attemptComplete
	if !registry.signalAttemptLocked(attemptIndex, attempt) {
		return
	}
	if attempt.waiterCount == 0 {
		registry.removeAttemptLocked(attemptIndex)
	}
}

// TerminateGroup resolves every admitted pending attempt whose local apply path
// ended at one closed infrastructure boundary. The caller must linearize the
// boundary after fencing old Host ingress, then close the old Host before any
// replacement owner admits the GroupKey.
func (registry *Registry) TerminateGroup(
	group raftmember.GroupKey,
	reason multiraft.ProposalGroupTerminationReason,
) error {
	if registry == nil {
		return ErrRegistryClosed
	}
	registry.settleMu.Lock()
	defer registry.settleMu.Unlock()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	if registry.failure != nil {
		return registry.failure
	}
	if groupIndex, found, findErr := registry.findGroupLocked(group); findErr != nil {
		return findErr
	} else if found && registry.groupTable[groupIndex].ownerEpoch != 0 {
		return ErrSourceOwnerMismatch
	}
	err := registry.terminateGroupLocked(group, reason)
	if err != nil && errors.Is(err, ErrRegistryCorrupt) {
		return registry.corruptLocked(err)
	}
	return err
}

func (registry *Registry) settleProposalGroupTermination(
	termination multiraft.ProposalGroupTermination,
) {
	if registry == nil {
		return
	}
	registry.settleMu.Lock()
	defer registry.settleMu.Unlock()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed || registry.failure != nil {
		return
	}
	if _, err := registry.validateCallbackSourceLocked(
		termination.Group, termination.SourceOwner, termination.SourceToken,
	); err != nil {
		registry.poisonLocked(errors.Join(errors.New("proposal group termination source"), err))
		return
	}
	if err := registry.terminateGroupLocked(
		termination.Group, termination.Reason,
	); err != nil {
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
	if groupRecord.identityCount == 0 ||
		int(groupRecord.identityCount) > registry.stats.OutstandingIdentities ||
		int(groupRecord.identityCount) > registry.limits.MaxOutstandingIdentities {
		return ErrRegistryCorrupt
	}
	if groupRecord.pendingAttempts == 0 {
		return nil
	}
	if groupRecord.entryHead == noIndex || registry.stats.PendingGroups <= 0 ||
		registry.stats.PendingGroups > registry.limits.MaxGroups {
		return ErrRegistryCorrupt
	}
	pendingToRelease := 0
	visitedEntries := uint32(0)
	previousEntry := noIndex
	for entryIndex := groupRecord.entryHead; entryIndex != noIndex; {
		if visitedEntries >= groupRecord.identityCount ||
			visitedEntries >= uint32(len(registry.entries)) ||
			int(entryIndex) >= len(registry.entries) {
			return ErrRegistryCorrupt
		}
		entry := &registry.entries[entryIndex]
		if !entry.active || entry.position.group != group ||
			entry.groupPrevious != previousEntry || entry.attemptCount == 0 ||
			entry.attemptCount > uint16(registry.limits.MaxAttemptsPerIdentity) {
			return ErrRegistryCorrupt
		}
		visitedAttempts := uint16(0)
		for attemptIndex := entry.attemptHead; attemptIndex != noIndex; {
			if visitedAttempts >= entry.attemptCount ||
				int(attemptIndex) >= len(registry.attempts) {
				return ErrRegistryCorrupt
			}
			attempt := &registry.attempts[attemptIndex]
			if !attempt.active || attempt.entry != entryIndex ||
				attempt.entryGeneration != entry.generation {
				return ErrRegistryCorrupt
			}
			visitedAttempts++
			if attempt.state == attemptPending && attempt.admitted &&
				!attempt.lifecyclePending {
				if attempt.ownerEpoch != groupRecord.ownerEpoch {
					return ErrRegistryCorrupt
				}
				pendingToRelease++
			}
			attemptIndex = attempt.next
		}
		if visitedAttempts != entry.attemptCount {
			return ErrRegistryCorrupt
		}
		visitedEntries++
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
				if attempt.ownerEpoch != groupRecord.ownerEpoch {
					return ErrRegistryCorrupt
				}
				if !registry.removeGroupPendingLocked(group) {
					return ErrRegistryCorrupt
				}
				attempt.admitted = false
				attempt.outcome = outcome
				attempt.state = attemptComplete
				if !registry.signalAttemptLocked(attemptIndex, attempt) {
					return registry.failure
				}
				if attempt.waiterCount == 0 {
					if !registry.removeAttemptLocked(attemptIndex) {
						return registry.failure
					}
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

func (registry *Registry) corruptLocked(cause error) error {
	registry.poisonLocked(cause)
	if registry.failure != nil {
		return registry.failure
	}
	return errors.Join(ErrRegistryCorrupt, cause)
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
				candidate.tableSlot != index ||
				int(candidate.tenantLen) > tenantSlotBytes ||
				int(candidate.completionLen) > completionSlotBytes ||
				candidate.position.group == (raftmember.GroupKey{}) ||
				(candidate.attemptCount == 0) != (candidate.attemptHead == noIndex) {
				return noIndex, noIndex, false, ErrRegistryCorrupt
			}
			if slot.hash == hash && registry.entryPositionEqual(slot.entry, candidate, identity) {
				return slot.entry, index, true, nil
			}
		default:
			return noIndex, noIndex, false, ErrRegistryCorrupt
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

func (registry *Registry) groupOwnerEpochLocked(group raftmember.GroupKey) (uint64, error) {
	index, found, err := registry.findGroupLocked(group)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrRegistryCorrupt
	}
	slot := &registry.groupTable[index]
	if slot.identityCount == 0 || slot.entryHead == noIndex {
		return 0, ErrRegistryCorrupt
	}
	return slot.ownerEpoch, nil
}

func (registry *Registry) retainGroupIdentityLocked(
	group raftmember.GroupKey,
) (uint32, error) {
	if group == (raftmember.GroupKey{}) {
		return noIndex, ErrRegistryCorrupt
	}
	if registry.stats.OutstandingGroups < 0 ||
		registry.stats.OutstandingGroups > registry.limits.MaxGroups ||
		registry.stats.OutstandingIdentities < 0 ||
		registry.stats.OutstandingIdentities > len(registry.entries) {
		return noIndex, ErrRegistryCorrupt
	}
	index, found, err := registry.findGroupLocked(group)
	if err != nil {
		return noIndex, err
	}
	if found {
		slot := &registry.groupTable[index]
		if registry.stats.OutstandingGroups == 0 ||
			int(slot.identityCount) > registry.stats.OutstandingIdentities ||
			slot.identityCount >= uint32(registry.limits.MaxOutstandingIdentities) ||
			(slot.identityCount == 0 && slot.ownerEpoch == 0) ||
			(slot.identityCount == 0) != (slot.entryHead == noIndex) {
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
	if registry.stats.OutstandingGroups <= 0 ||
		registry.stats.OutstandingGroups > registry.limits.MaxGroups {
		return false
	}
	index, found, err := registry.findGroupLocked(group)
	if err != nil || !found {
		return false
	}
	slot := &registry.groupTable[index]
	if slot.identityCount == 0 ||
		slot.identityCount > uint32(registry.limits.MaxOutstandingIdentities) {
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
	if slot.ownerEpoch != 0 {
		return true
	}
	if !registry.deleteGroupSlotLocked(index) {
		return false
	}
	registry.stats.OutstandingGroups--
	return true
}

func validAppliedSourceOwner(owner raftmember.AppliedSourceOwner) bool {
	return owner.Group != (raftmember.GroupKey{}) && owner.AllocationGeneration != 0 &&
		owner.MemberID != 0 && owner.StoreID != ([16]byte{}) && owner.NodeIncarnation != 0
}

func groupSlotOwner(slot *pendingGroupSlot) raftmember.AppliedSourceOwner {
	if slot == nil || slot.ownerEpoch == 0 {
		return raftmember.AppliedSourceOwner{}
	}
	return raftmember.AppliedSourceOwner{
		Group: slot.group, AllocationGeneration: slot.allocationGeneration,
		MemberID: slot.memberID, StoreID: slot.storeID,
		NodeIncarnation: slot.nodeIncarnation,
	}
}

func (registry *Registry) validateSourceOwnerLocked(
	owner raftmember.AppliedSourceOwner,
	token raftmember.AppliedSourceToken,
) (uint32, error) {
	if !validAppliedSourceOwner(owner) || token.RegistryID == 0 || token.OwnerEpoch == 0 ||
		token.RegistryID != registry.registryID {
		return noIndex, ErrSourceOwnerMismatch
	}
	index, found, err := registry.findGroupLocked(owner.Group)
	if err != nil {
		return noIndex, err
	}
	if !found {
		return noIndex, ErrSourceOwnerMismatch
	}
	slot := &registry.groupTable[index]
	if slot.ownerEpoch != token.OwnerEpoch || groupSlotOwner(slot) != owner {
		return noIndex, ErrSourceOwnerMismatch
	}
	return index, nil
}

func (registry *Registry) validateCallbackSourceLocked(
	group raftmember.GroupKey,
	owner raftmember.AppliedSourceOwner,
	token raftmember.AppliedSourceToken,
) (uint64, error) {
	if owner == (raftmember.AppliedSourceOwner{}) &&
		token == (raftmember.AppliedSourceToken{}) {
		if group == (raftmember.GroupKey{}) {
			return 0, ErrSourceOwnerMismatch
		}
		index, found, err := registry.findGroupLocked(group)
		if err != nil {
			return 0, err
		}
		if found && registry.groupTable[index].ownerEpoch != 0 {
			return 0, ErrSourceOwnerMismatch
		}
		return 0, nil
	}
	if owner.Group != group {
		return 0, ErrSourceOwnerMismatch
	}
	if _, err := registry.validateSourceOwnerLocked(owner, token); err != nil {
		return 0, err
	}
	return token.OwnerEpoch, nil
}

func (registry *Registry) claimAppliedSource(
	owner raftmember.AppliedSourceOwner,
) (raftmember.AppliedSourceToken, error) {
	if registry == nil {
		return raftmember.AppliedSourceToken{}, ErrRegistryClosed
	}
	if !validAppliedSourceOwner(owner) {
		return raftmember.AppliedSourceToken{}, ErrSourceOwnerMismatch
	}
	registry.settleMu.Lock()
	defer registry.settleMu.Unlock()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return raftmember.AppliedSourceToken{}, ErrRegistryClosed
	}
	if registry.failure != nil {
		return raftmember.AppliedSourceToken{}, registry.failure
	}
	index, found, err := registry.findGroupLocked(owner.Group)
	if err != nil {
		return raftmember.AppliedSourceToken{}, err
	}
	if found {
		slot := &registry.groupTable[index]
		if slot.ownerEpoch != 0 {
			return raftmember.AppliedSourceToken{}, ErrSourceOwnerClaimed
		}
		if slot.identityCount == 0 || slot.entryHead == noIndex ||
			registry.stats.OutstandingGroups <= 0 {
			return raftmember.AppliedSourceToken{}, registry.corruptLocked(
				errors.New("source claim found an ownerless empty group slot"),
			)
		}
	} else {
		if registry.stats.OutstandingGroups >= registry.limits.MaxGroups {
			return raftmember.AppliedSourceToken{}, ErrGroupCapacity
		}
		registry.groupTable[index] = pendingGroupSlot{
			group: owner.Group, hash: hashGroup(registry.seed, owner.Group) & registry.hashMask,
			entryHead: noIndex, state: tableOccupied,
		}
		registry.stats.OutstandingGroups++
		registry.stats.PeakGroups = max(registry.stats.PeakGroups, registry.stats.OutstandingGroups)
	}
	if registry.nextOwnerEpoch == math.MaxUint64 {
		if !found && !registry.deleteGroupSlotLocked(index) {
			return raftmember.AppliedSourceToken{}, registry.failure
		}
		if !found {
			registry.stats.OutstandingGroups--
		}
		return raftmember.AppliedSourceToken{}, ErrGenerationExhausted
	}
	registry.nextOwnerEpoch++
	slot := &registry.groupTable[index]
	slot.allocationGeneration = owner.AllocationGeneration
	slot.memberID = owner.MemberID
	slot.storeID = owner.StoreID
	slot.nodeIncarnation = owner.NodeIncarnation
	slot.ownerEpoch = registry.nextOwnerEpoch
	registry.stats.LiveSourceOwners++
	return raftmember.AppliedSourceToken{
		RegistryID: registry.registryID, OwnerEpoch: slot.ownerEpoch,
	}, nil
}

func (registry *Registry) releaseAppliedSource(
	owner raftmember.AppliedSourceOwner,
	token raftmember.AppliedSourceToken,
) error {
	if registry == nil {
		return ErrRegistryClosed
	}
	registry.settleMu.Lock()
	defer registry.settleMu.Unlock()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	if registry.failure != nil {
		return registry.failure
	}
	index, err := registry.validateSourceOwnerLocked(owner, token)
	if err != nil {
		return err
	}
	slot := &registry.groupTable[index]
	if slot.pendingAttempts != 0 || registry.stats.LiveSourceOwners <= 0 {
		return ErrSourceOwnersLive
	}
	slot.allocationGeneration = 0
	slot.memberID = 0
	slot.storeID = [16]byte{}
	slot.nodeIncarnation = 0
	slot.ownerEpoch = 0
	registry.stats.LiveSourceOwners--
	if slot.identityCount != 0 {
		return nil
	}
	if slot.entryHead != noIndex || !registry.deleteGroupSlotLocked(index) {
		return registry.corruptLocked(errors.New("released source retained an invalid empty group"))
	}
	registry.stats.OutstandingGroups--
	return nil
}

func (registry *Registry) addGroupPendingLocked(
	group raftmember.GroupKey,
) bool {
	index, found, err := registry.findGroupLocked(group)
	if err != nil || !found {
		return false
	}
	slot := &registry.groupTable[index]
	if slot.identityCount == 0 || slot.group != group ||
		slot.pendingAttempts >= uint32(registry.limits.MaxOutstandingAttempts) ||
		uint(registry.stats.PendingAdmittedAttempts) >=
			uint(registry.limits.MaxOutstandingAttempts) {
		return false
	}
	if slot.pendingAttempts == 0 {
		if uint(registry.stats.PendingGroups) >= uint(registry.limits.MaxGroups) {
			return false
		}
		registry.stats.PendingGroups++
	} else if registry.stats.PendingGroups <= 0 {
		return false
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
		registry.stats.PendingAdmittedAttempts <= 0 ||
		registry.stats.PendingAdmittedAttempts > registry.limits.MaxOutstandingAttempts ||
		registry.stats.PendingGroups <= 0 ||
		registry.stats.PendingGroups > registry.limits.MaxGroups ||
		int(slot.pendingAttempts) > registry.stats.PendingAdmittedAttempts {
		return false
	}
	slot.pendingAttempts--
	registry.stats.PendingAdmittedAttempts--
	if slot.pendingAttempts == 0 {
		registry.stats.PendingGroups--
	}
	return true
}

func (registry *Registry) hasPendingOwnedGroup(
	owner raftmember.AppliedSourceOwner,
	token raftmember.AppliedSourceToken,
) bool {
	if registry == nil {
		return true
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed || registry.failure != nil {
		return true
	}
	index, err := registry.validateSourceOwnerLocked(owner, token)
	if err != nil {
		registry.poisonLocked(err)
		return true
	}
	return registry.groupTable[index].pendingAttempts != 0
}

func (registry *Registry) hasPendingGroup(group raftmember.GroupKey) bool {
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
	if found && registry.groupTable[index].ownerEpoch != 0 {
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
	ownerEpoch uint64,
) (uint32, bool, error) {
	if entry == nil || !entry.active ||
		entry.attemptCount > uint16(registry.limits.MaxAttemptsPerIdentity) ||
		int(entry.attemptCount) > registry.stats.OutstandingAttempts ||
		(entry.attemptCount == 0) != (entry.attemptHead == noIndex) {
		return noIndex, false, ErrRegistryCorrupt
	}
	visited := uint16(0)
	for index := entry.attemptHead; index != noIndex; {
		if visited >= entry.attemptCount || int(index) >= len(registry.attempts) {
			return noIndex, false, ErrRegistryCorrupt
		}
		attempt := &registry.attempts[index]
		if !attempt.active || attempt.entryGeneration != entry.generation ||
			attempt.entry >= uint32(len(registry.entries)) ||
			&registry.entries[attempt.entry] != entry {
			return noIndex, false, ErrRegistryCorrupt
		}
		visited++
		if attempt.digest == digest && attempt.ownerEpoch == ownerEpoch {
			return index, true, nil
		}
		index = attempt.next
	}
	if visited != entry.attemptCount {
		return noIndex, false, ErrRegistryCorrupt
	}
	return noIndex, false, nil
}

func (registry *Registry) allocateEntryLocked(
	identity commandIdentity,
	hash uint64,
	tableIndex uint32,
	groupIndex uint32,
) (uint32, uint64, error) {
	index := registry.freeEntry
	if index == noIndex {
		if registry.stats.OutstandingIdentities != len(registry.entries) {
			return noIndex, 0, registry.corruptLocked(
				errors.New("identity free list ended before capacity"),
			)
		}
		return noIndex, 0, ErrIdentityCapacity
	}
	if int(index) >= len(registry.entries) || int(tableIndex) >= len(registry.table) ||
		int(groupIndex) >= len(registry.groupTable) {
		return noIndex, 0, registry.corruptLocked(errors.New("invalid identity allocation index"))
	}
	if registry.stats.OutstandingIdentities < 0 ||
		registry.stats.OutstandingIdentities >= len(registry.entries) ||
		registry.table[tableIndex].state != tableEmpty {
		return noIndex, 0, registry.corruptLocked(errors.New("invalid identity allocation count or table slot"))
	}
	groupSlot := &registry.groupTable[groupIndex]
	if groupSlot.state != tableOccupied ||
		groupSlot.group != identity.position.group || groupSlot.identityCount == 0 ||
		int(groupSlot.identityCount) > registry.stats.OutstandingIdentities+1 {
		return noIndex, 0, registry.corruptLocked(errors.New("invalid identity allocation group"))
	}
	if groupSlot.entryHead != noIndex {
		if int(groupSlot.entryHead) >= len(registry.entries) {
			return noIndex, 0, registry.corruptLocked(errors.New("identity allocation group head is out of range"))
		}
		head := &registry.entries[groupSlot.entryHead]
		if !head.active || head.position.group != identity.position.group ||
			head.groupPrevious != noIndex {
			return noIndex, 0, registry.corruptLocked(errors.New("identity allocation group head is corrupt"))
		}
	}
	record := &registry.entries[index]
	if record.active || record.freeOrGroupNext == index ||
		(record.freeOrGroupNext != noIndex && int(record.freeOrGroupNext) >= len(registry.entries)) {
		return noIndex, 0, registry.corruptLocked(errors.New("corrupt identity free-list record"))
	}
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
	ownerEpoch uint64,
) (uint32, uint64, error) {
	if registry.freeAttempt == noIndex {
		if registry.stats.OutstandingAttempts != len(registry.attempts) {
			return noIndex, 0, registry.corruptLocked(
				errors.New("attempt free list ended before capacity"),
			)
		}
		return noIndex, 0, ErrAttemptCapacity
	}
	if int(registry.freeAttempt) >= len(registry.attempts) ||
		int(entryIndex) >= len(registry.entries) {
		return noIndex, 0, registry.corruptLocked(errors.New("invalid attempt free-list head"))
	}
	entry := &registry.entries[entryIndex]
	if !entry.active ||
		(entry.attemptCount == 0) != (entry.attemptHead == noIndex) ||
		entry.attemptCount >= uint16(registry.limits.MaxAttemptsPerIdentity) ||
		int(entry.attemptCount) > registry.stats.OutstandingAttempts ||
		registry.stats.OutstandingAttempts < 0 ||
		registry.stats.OutstandingAttempts >= registry.limits.MaxOutstandingAttempts {
		return noIndex, 0, registry.corruptLocked(errors.New("invalid attempt allocation owner or count"))
	}
	if entry.attemptHead != noIndex {
		if int(entry.attemptHead) >= len(registry.attempts) {
			return noIndex, 0, registry.corruptLocked(errors.New("attempt allocation head is out of range"))
		}
		head := &registry.attempts[entry.attemptHead]
		if !head.active || head.entry != entryIndex ||
			head.entryGeneration != entry.generation {
			return noIndex, 0, registry.corruptLocked(errors.New("attempt allocation head is corrupt"))
		}
	}
	index := registry.freeAttempt
	record := &registry.attempts[index]
	if record.active || record.freeNext == index ||
		(record.freeNext != noIndex && int(record.freeNext) >= len(registry.attempts)) {
		return noIndex, 0, registry.corruptLocked(errors.New("corrupt attempt free-list record"))
	}
	generation, err := nextGeneration(record.generation)
	if err != nil {
		registry.poisonLocked(err)
		return noIndex, 0, registry.failure
	}
	registry.freeAttempt = record.freeNext
	*record = attemptRecord{
		generation: generation, ownerEpoch: ownerEpoch, freeNext: noIndex,
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
	if registry.freeWaiter == noIndex {
		if registry.stats.Waiters != len(registry.waiters) {
			return noIndex, 0, registry.corruptLocked(
				errors.New("waiter free list ended before capacity"),
			)
		}
		return noIndex, 0, ErrWaiterCapacity
	}
	if int(registry.freeWaiter) >= len(registry.waiters) ||
		int(attemptIndex) >= len(registry.attempts) {
		return noIndex, 0, registry.corruptLocked(errors.New("invalid waiter free-list head"))
	}
	attempt := &registry.attempts[attemptIndex]
	if !attempt.active || registry.stats.Waiters < 0 ||
		registry.stats.Waiters >= registry.limits.MaxWaiters ||
		attempt.waiterCount >= uint32(registry.limits.MaxWaiters) ||
		int(attempt.waiterCount) > registry.stats.Waiters ||
		(attempt.waiterCount == 0) != (attempt.waiterHead == noIndex) {
		return noIndex, 0, registry.corruptLocked(errors.New("invalid waiter allocation owner or count"))
	}
	if attempt.waiterHead != noIndex {
		if int(attempt.waiterHead) >= len(registry.waiters) {
			return noIndex, 0, registry.corruptLocked(errors.New("waiter allocation head is out of range"))
		}
		head := &registry.waiters[attempt.waiterHead]
		if !head.active || head.attempt != attemptIndex ||
			head.attemptGeneration != attempt.generation || head.previous != noIndex {
			return noIndex, 0, registry.corruptLocked(errors.New("waiter allocation head is corrupt"))
		}
	}
	index := registry.freeWaiter
	record := &registry.waiters[index]
	if record.active || record.blocking || record.releasePending || record.wake == nil ||
		cap(record.wake) != 1 || record.freeNext == index ||
		(record.freeNext != noIndex && int(record.freeNext) >= len(registry.waiters)) {
		return noIndex, 0, registry.corruptLocked(errors.New("corrupt waiter free-list record"))
	}
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

func (registry *Registry) releaseWaiterLocked(index uint32) bool {
	if int(index) >= len(registry.waiters) {
		registry.corruptLocked(errors.New("waiter release index is out of range"))
		return false
	}
	waiter := &registry.waiters[index]
	if !waiter.active || waiter.blocking || int(waiter.attempt) >= len(registry.attempts) {
		registry.corruptLocked(errors.New("waiter release found an invalid active record"))
		return false
	}
	attemptIndex := waiter.attempt
	attempt := &registry.attempts[attemptIndex]
	if !attempt.active || attempt.generation != waiter.attemptGeneration ||
		attempt.waiterCount == 0 ||
		attempt.waiterCount > uint32(registry.limits.MaxWaiters) ||
		registry.stats.Waiters <= 0 || registry.stats.Waiters > len(registry.waiters) ||
		int(attempt.waiterCount) > registry.stats.Waiters ||
		waiter.wake == nil || cap(waiter.wake) != 1 ||
		(registry.freeWaiter != noIndex && int(registry.freeWaiter) >= len(registry.waiters)) ||
		registry.freeWaiter == index ||
		waiter.previous == index || waiter.next == index ||
		(waiter.previous != noIndex && waiter.previous == waiter.next) {
		registry.corruptLocked(errors.New("waiter release found invalid ownership or counts"))
		return false
	}
	if registry.freeWaiter != noIndex && registry.waiters[registry.freeWaiter].active {
		registry.corruptLocked(errors.New("waiter release found an active free-list head"))
		return false
	}
	if waiter.previous == noIndex {
		if attempt.waiterHead != index {
			registry.corruptLocked(errors.New("waiter release lost the attempt head"))
			return false
		}
	} else {
		if int(waiter.previous) >= len(registry.waiters) {
			registry.corruptLocked(errors.New("waiter release previous link is out of range"))
			return false
		}
		previous := &registry.waiters[waiter.previous]
		if !previous.active || previous.attempt != attemptIndex ||
			previous.attemptGeneration != attempt.generation || previous.next != index {
			registry.corruptLocked(errors.New("waiter release previous link is corrupt"))
			return false
		}
	}
	if waiter.next != noIndex {
		if int(waiter.next) >= len(registry.waiters) {
			registry.corruptLocked(errors.New("waiter release next link is out of range"))
			return false
		}
		next := &registry.waiters[waiter.next]
		if !next.active || next.attempt != attemptIndex ||
			next.attemptGeneration != attempt.generation || next.previous != index {
			registry.corruptLocked(errors.New("waiter release next link is corrupt"))
			return false
		}
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
		return registry.removeAttemptLocked(attemptIndex)
	}
	return true
}

func (registry *Registry) requestWaiterReleaseLocked(index uint32) bool {
	if int(index) >= len(registry.waiters) {
		return false
	}
	waiter := &registry.waiters[index]
	if !waiter.active || waiter.releasePending {
		return false
	}
	if waiter.wake == nil || cap(waiter.wake) != 1 {
		registry.corruptLocked(errors.New("waiter release request found an invalid notification channel"))
		return false
	}
	if waiter.blocking {
		waiter.releasePending = true
		notify(waiter.wake)
		return true
	}
	return registry.releaseWaiterLocked(index)
}

func (registry *Registry) attemptRemovableLocked(attempt *attemptRecord) bool {
	if attempt == nil || !attempt.active {
		return false
	}
	if attempt.settlementPinned {
		return false
	}
	switch attempt.state {
	case attemptSettling:
		return false
	case attemptComplete:
		return !attempt.lifecyclePending
	case attemptPending:
		return !attempt.lifecyclePending && !attempt.admitted
	default:
		registry.corruptLocked(errors.New("attempt removal predicate found an invalid state"))
		return false
	}
}

func (registry *Registry) removeAttemptLocked(index uint32) bool {
	if int(index) >= len(registry.attempts) {
		registry.corruptLocked(errors.New("attempt removal index is out of range"))
		return false
	}
	attempt := &registry.attempts[index]
	if !attempt.active || attempt.admitted || attempt.settlementPinned || attempt.waiterCount != 0 ||
		attempt.waiterHead != noIndex || attempt.next == index ||
		int(attempt.entry) >= len(registry.entries) ||
		(registry.freeAttempt != noIndex && int(registry.freeAttempt) >= len(registry.attempts)) ||
		registry.freeAttempt == index {
		registry.corruptLocked(errors.New("attempt removal found an invalid active record"))
		return false
	}
	if registry.freeAttempt != noIndex && registry.attempts[registry.freeAttempt].active {
		registry.corruptLocked(errors.New("attempt removal found an active free-list head"))
		return false
	}
	entryIndex := attempt.entry
	entry := &registry.entries[entryIndex]
	if !entry.active || entry.generation != attempt.entryGeneration ||
		entry.attemptCount == 0 ||
		entry.attemptCount > uint16(registry.limits.MaxAttemptsPerIdentity) ||
		int(entry.attemptCount) > registry.stats.OutstandingAttempts ||
		registry.stats.OutstandingAttempts <= 0 ||
		registry.stats.OutstandingAttempts > len(registry.attempts) {
		registry.corruptLocked(errors.New("attempt removal found invalid ownership or counts"))
		return false
	}
	link := &entry.attemptHead
	visited := uint16(0)
	for *link != noIndex && *link != index {
		if visited >= entry.attemptCount || int(*link) >= len(registry.attempts) {
			registry.corruptLocked(errors.New("attempt removal found a corrupt or cyclic chain"))
			return false
		}
		candidate := &registry.attempts[*link]
		if !candidate.active || candidate.entry != entryIndex ||
			candidate.entryGeneration != entry.generation {
			registry.corruptLocked(errors.New("attempt removal found a foreign chain record"))
			return false
		}
		visited++
		link = &candidate.next
	}
	if *link != index {
		registry.corruptLocked(errors.New("attempt removal lost its entry link"))
		return false
	}
	if attempt.next != noIndex {
		if int(attempt.next) >= len(registry.attempts) {
			registry.corruptLocked(errors.New("attempt removal next link is out of range"))
			return false
		}
		next := &registry.attempts[attempt.next]
		if !next.active || next.entry != entryIndex ||
			next.entryGeneration != entry.generation {
			registry.corruptLocked(errors.New("attempt removal next link is foreign"))
			return false
		}
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
		return registry.removeEntryLocked(entryIndex)
	}
	return true
}

func (registry *Registry) removeEntryLocked(index uint32) bool {
	if int(index) >= len(registry.entries) {
		registry.corruptLocked(errors.New("identity removal index is out of range"))
		return false
	}
	entry := &registry.entries[index]
	if !entry.active || entry.attemptCount != 0 || entry.attemptHead != noIndex ||
		int(entry.tableSlot) >= len(registry.table) ||
		registry.stats.OutstandingIdentities <= 0 ||
		registry.stats.OutstandingIdentities > len(registry.entries) ||
		(registry.freeEntry != noIndex && int(registry.freeEntry) >= len(registry.entries)) ||
		registry.freeEntry == index || int(entry.tenantLen) > tenantSlotBytes ||
		int(entry.completionLen) > completionSlotBytes ||
		registry.stats.RetainedCompletionBytes < 0 ||
		registry.stats.RetainedCompletionBytes > len(registry.completionArena) ||
		int(entry.completionLen) > registry.stats.RetainedCompletionBytes {
		registry.corruptLocked(errors.New("identity removal found invalid ownership or counts"))
		return false
	}
	if registry.freeEntry != noIndex && registry.entries[registry.freeEntry].active {
		registry.corruptLocked(errors.New("identity removal found an active free-list head"))
		return false
	}
	slot := &registry.table[entry.tableSlot]
	if slot.state != tableOccupied || slot.entry != index || slot.generation != entry.generation {
		registry.corruptLocked(errors.New("identity removal found a corrupt table slot"))
		return false
	}
	group := entry.position.group
	if !registry.unlinkGroupEntryLocked(index, entry) {
		registry.poisonLocked(errors.New("identity lost its group entry link"))
		return false
	}
	if !registry.releaseGroupIdentityLocked(group) {
		registry.poisonLocked(errors.New("identity lost its group capacity record"))
		return false
	}
	if !registry.deleteTableSlotLocked(entry.tableSlot) {
		return false
	}
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
	return true
}

func (registry *Registry) deleteGroupSlotLocked(hole uint32) bool {
	if int(hole) >= len(registry.groupTable) ||
		registry.groupTable[hole].state != tableOccupied {
		registry.corruptLocked(errors.New("group table deletion hole is invalid"))
		return false
	}
	mask := uint32(len(registry.groupTable) - 1)
	for scanned, scan := 0, (hole+1)&mask; scanned < len(registry.groupTable); scanned, scan = scanned+1, (scan+1)&mask {
		slot := registry.groupTable[scan]
		if slot.state == tableEmpty {
			registry.groupTable[hole] = pendingGroupSlot{}
			return true
		}
		if slot.state != tableOccupied || slot.group == (raftmember.GroupKey{}) ||
			(slot.identityCount == 0) != (slot.entryHead == noIndex) ||
			(slot.identityCount == 0 && slot.ownerEpoch == 0) ||
			(slot.identityCount != 0 && int(slot.entryHead) >= len(registry.entries)) ||
			int(slot.pendingAttempts) > registry.stats.PendingAdmittedAttempts ||
			(slot.pendingAttempts != 0 && slot.identityCount == 0) ||
			(slot.ownerEpoch == 0 && (slot.allocationGeneration != 0 || slot.memberID != 0 ||
				slot.storeID != ([16]byte{}) || slot.nodeIncarnation != 0)) ||
			(slot.ownerEpoch != 0 && !validAppliedSourceOwner(groupSlotOwner(&slot))) {
			registry.corruptLocked(errors.New("group table deletion found a corrupt slot"))
			return false
		}
		if slot.identityCount != 0 {
			head := &registry.entries[slot.entryHead]
			if !head.active || head.position.group != slot.group ||
				head.groupPrevious != noIndex {
				registry.corruptLocked(errors.New("group table deletion found a corrupt entry head"))
				return false
			}
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
	registry.corruptLocked(errors.New("group table deletion found no empty slot"))
	return false
}

func (registry *Registry) deleteTableSlotLocked(hole uint32) bool {
	if int(hole) >= len(registry.table) || registry.table[hole].state != tableOccupied {
		registry.corruptLocked(errors.New("identity table deletion hole is invalid"))
		return false
	}
	mask := uint32(len(registry.table) - 1)
	for scanned, scan := 0, (hole+1)&mask; scanned < len(registry.table); scanned, scan = scanned+1, (scan+1)&mask {
		slot := registry.table[scan]
		if slot.state == tableEmpty {
			registry.table[hole] = positionSlot{}
			return true
		}
		if slot.state != tableOccupied {
			registry.corruptLocked(errors.New("identity table deletion found an invalid state"))
			return false
		}
		home := uint32(slot.hash) & mask
		toScan := (scan - home) & mask
		toHole := (hole - home) & mask
		if toHole >= toScan {
			continue
		}
		if int(slot.entry) >= len(registry.entries) {
			registry.poisonLocked(errors.New("table shift references unknown entry"))
			return false
		}
		entry := &registry.entries[slot.entry]
		if !entry.active || entry.generation != slot.generation || entry.tableSlot != scan {
			registry.corruptLocked(errors.New("table shift references a corrupt entry"))
			return false
		}
		registry.table[hole] = slot
		registry.entries[slot.entry].tableSlot = hole
		hole = scan
	}
	registry.corruptLocked(errors.New("identity table deletion found no empty slot"))
	return false
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
	if registry.closed {
		registry.mu.Unlock()
		return nil
	}
	if registry.stats.LiveSourceOwners != 0 {
		registry.mu.Unlock()
		return ErrSourceOwnersLive
	}
	registry.mu.Unlock()
	if err := registry.settlementLookup.Release(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
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
	registry.stats.LiveSourceOwners = 0
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
				if record.active && record.generation == waiter.generation && record.blocking {
					record.blocking = false
					if record.releasePending && !registry.releaseWaiterLocked(waiter.index) {
						err = registry.failure
					}
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
			if !registry.releaseWaiterLocked(waiter.index) {
				registry.mu.Unlock()
				return Outcome{}, registry.failure
			}
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
	if !record.active || record.generation != waiter.generation {
		return Outcome{}, false, ErrWaiterClosed
	}
	if record.releasePending {
		return Outcome{}, false, ErrWaiterClosed
	}
	if int(record.attempt) >= len(registry.attempts) {
		return Outcome{}, false, registry.corruptLocked(
			errors.New("waiter attempt index is out of range"),
		)
	}
	attempt := &registry.attempts[record.attempt]
	if !attempt.active || attempt.generation != record.attemptGeneration {
		return Outcome{}, false, registry.corruptLocked(
			errors.New("waiter references an invalid attempt"),
		)
	}
	switch attempt.state {
	case attemptPending, attemptSettling:
		return Outcome{}, false, nil
	case attemptComplete:
	default:
		return Outcome{}, false, registry.corruptLocked(
			errors.New("waiter observed an invalid attempt state"),
		)
	}
	if int(attempt.entry) >= len(registry.entries) {
		return Outcome{}, false, registry.corruptLocked(
			errors.New("waiter attempt entry is out of range"),
		)
	}
	entry := &registry.entries[attempt.entry]
	if !entry.active || entry.generation != attempt.entryGeneration {
		return Outcome{}, false, registry.corruptLocked(
			errors.New("waiter attempt references an invalid identity"),
		)
	}
	completionBytes := 0
	completionApplied := uint64(0)
	if attempt.hasCompletion {
		if entry.completionLen == 0 || int(entry.completionLen) > completionSlotBytes {
			return Outcome{}, false, registry.corruptLocked(
				errors.New("waiter completion metadata is invalid"),
			)
		}
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
// or releasing ownership when caller capacity is insufficient. A concurrent
// blocking Wait owns reclamation and makes TakeCompletionInto return
// ErrWaiterBusy.
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
	record := &registry.waiters[waiter.index]
	if record.blocking {
		return dst, Outcome{}, ErrWaiterBusy
	}
	if !ready {
		return dst, Outcome{}, ErrWaiterPending
	}
	if outcome.CompletionBytes > cap(dst)-len(dst) {
		return dst, outcome, ErrCompletionDestinationSmall
	}
	attempt := &registry.attempts[record.attempt]
	if attempt.hasCompletion {
		entry := &registry.entries[attempt.entry]
		slot := registry.completionSlot(attempt.entry)
		dst = append(dst, slot[:entry.completionLen]...)
	}
	if !registry.releaseWaiterLocked(waiter.index) {
		return dst, Outcome{}, registry.failure
	}
	return dst, outcome, nil
}

// Cancel releases only this waiter's ownership. It never removes or blocks a
// Raft proposal that has already entered Host. If a Wait is blocking, physical
// slot reuse is deferred until that claimant observes cancellation and
// acknowledges the release.
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
	return registry.requestWaiterReleaseLocked(waiter.index)
}

// NewHost constructs a Host wired to this registry's settlement sink. Every
// live Host constructed from one Registry must own disjoint GroupKeys. Before a
// replacement Host admits the same GroupKey, callers must fence ingress,
// terminate pending attempts, and close the old Host. The ordinary
// multiraft.NewHost constructor keeps its explicit no-waiter sink.
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
	return multiraft.NewHostWithServingSinks(limits, multiraft.ServingSinks{
		Settle:          registry.settleOwnedAppliedBatch,
		Proposals:       registry.settleProposalAdmission,
		ProposalGroups:  registry.settleProposalGroupTermination,
		ProposalPending: registry.hasPendingOwnedGroup,
		ClaimSource:     registry.claimAppliedSource,
		ReleaseSource:   registry.releaseAppliedSource,
	})
}

func (registry *Registry) signalAttemptLocked(
	attemptIndex uint32,
	attempt *attemptRecord,
) bool {
	if attempt == nil || !attempt.active || int(attemptIndex) >= len(registry.attempts) ||
		&registry.attempts[attemptIndex] != attempt ||
		attempt.waiterCount > uint32(len(registry.waiters)) ||
		(attempt.waiterCount == 0) != (attempt.waiterHead == noIndex) {
		registry.corruptLocked(errors.New("attempt signal found an invalid owner or count"))
		return false
	}
	visited := uint32(0)
	previous := noIndex
	for index := attempt.waiterHead; index != noIndex; {
		if visited >= attempt.waiterCount || int(index) >= len(registry.waiters) {
			registry.corruptLocked(errors.New("attempt signal found a corrupt or cyclic waiter chain"))
			return false
		}
		waiter := &registry.waiters[index]
		if !waiter.active || waiter.attempt != attemptIndex ||
			waiter.attemptGeneration != attempt.generation || waiter.previous != previous ||
			waiter.wake == nil {
			registry.corruptLocked(errors.New("attempt signal found a foreign waiter record"))
			return false
		}
		notify(waiter.wake)
		visited++
		previous = index
		index = waiter.next
	}
	if visited != attempt.waiterCount {
		registry.corruptLocked(errors.New("attempt signal waiter count mismatch"))
		return false
	}
	return true
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
