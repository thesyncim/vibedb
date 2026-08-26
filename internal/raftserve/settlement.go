package raftserve

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
)

const settlementIndexSlots = 2 * raftmodel.MaxNormalApplyBatchEntries

type settlementAttemptItem struct {
	attempt            uint32
	attemptGeneration  uint64
	entry              uint32
	entryGeneration    uint64
	appliedIndex       uint64
	completionApplied  uint64
	completionBytes    uint16
	outcome            OutcomeCode
	originalOutcome    OutcomeCode
	originalState      attemptState
	originalCompletion bool
	observed           bool
}

type settlementEntryItem struct {
	entry                     uint32
	entryGeneration           uint64
	originalCompletionApplied uint64
	completionApplied         uint64
	originalCompletionBytes   uint16
	completionBytes           uint16
	touched                   bool
}

type settlementLookupItem struct {
	offset  uint16
	attempt uint16
	entry   uint16
}

type settlementIndexSlot struct {
	key  uint32
	item uint16
	used bool
}

type appliedBatchView interface {
	Len() int
	Group() raftmember.GroupKey
	Source() raftmember.AppliedBatchSource
	ReadyID() uint64
	FirstIndex() uint64
	LastIndex() uint64
	Entry(int) (raftmodel.NormalApply, bool)
	FinalPublication() raftmodel.Publication
	LookupCompletionInto(int, []byte) (replicatedstate.CompletionLookup, bool, error)
	BeginCompletionLookup(*raftmember.AppliedBatchCompletionWorkspace) error
	LookupCompletionIntoWorkspace(
		*raftmember.AppliedBatchCompletionWorkspace,
		int,
		[]byte,
	) (replicatedstate.CompletionLookup, bool, error)
	EndCompletionLookup(*raftmember.AppliedBatchCompletionWorkspace) error
}

// SettleAppliedBatch resolves locally registered attempts in one exact
// published range. Replay entries without a local waiter are accepted. All
// matched attempts become visible together after the complete range validates.
func (registry *Registry) SettleAppliedBatch(batch raftmember.AppliedBatch) error {
	return settleAppliedBatch(registry, batch)
}

func (registry *Registry) settleOwnedAppliedBatch(
	owner raftmember.AppliedSourceOwner,
	token raftmember.AppliedSourceToken,
	batch raftmember.AppliedBatch,
) error {
	if batch.Source().Owner() != owner {
		return ErrSourceOwnerMismatch
	}
	return settleAppliedBatchOwned(registry, batch, token)
}

func settleAppliedBatch[Batch appliedBatchView](
	registry *Registry,
	batch Batch,
) error {
	return settleAppliedBatchWithOwner(registry, batch, raftmember.AppliedSourceToken{}, false)
}

func settleAppliedBatchOwned[Batch appliedBatchView](
	registry *Registry,
	batch Batch,
	token raftmember.AppliedSourceToken,
) error {
	return settleAppliedBatchWithOwner(registry, batch, token, true)
}

func settleAppliedBatchWithOwner[Batch appliedBatchView](
	registry *Registry,
	batch Batch,
	token raftmember.AppliedSourceToken,
	owned bool,
) error {
	if registry == nil {
		return ErrRegistryClosed
	}
	registry.settleMu.Lock()
	defer registry.settleMu.Unlock()
	if err := validateBatchSource(batch); err != nil {
		return err
	}

	var attempts [raftmodel.MaxNormalApplyBatchEntries]settlementAttemptItem
	var entries [raftmodel.MaxNormalApplyBatchEntries]settlementEntryItem
	var lookups [raftmodel.MaxNormalApplyBatchEntries]settlementLookupItem
	var attemptTable [settlementIndexSlots]settlementIndexSlot
	var entryTable [settlementIndexSlots]settlementIndexSlot
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return ErrRegistryClosed
	}
	if registry.failure != nil {
		failure := registry.failure
		registry.mu.Unlock()
		return failure
	}
	ownerEpoch := uint64(0)
	if owned {
		if _, sourceErr := registry.validateSourceOwnerLocked(batch.Source().Owner(), token); sourceErr != nil {
			registry.mu.Unlock()
			return sourceErr
		}
		ownerEpoch = token.OwnerEpoch
	} else if groupIndex, found, sourceErr := registry.findGroupLocked(batch.Group()); sourceErr != nil {
		registry.mu.Unlock()
		return sourceErr
	} else if found && registry.groupTable[groupIndex].ownerEpoch != 0 {
		registry.mu.Unlock()
		return ErrSourceOwnerMismatch
	}
	attemptCount, entryCount, lookupCount, stageErr := stageSettlementLocked(
		registry, batch, ownerEpoch, &attempts, &entries, &lookups, &attemptTable, &entryTable,
	)
	if stageErr != nil {
		rollbackErr := rollbackSettlementLocked(
			registry, &attempts, attemptCount, &entries, entryCount,
		)
		if rollbackErr == nil && errors.Is(stageErr, ErrRegistryCorrupt) {
			stageErr = registry.corruptLocked(stageErr)
		}
		registry.mu.Unlock()
		if rollbackErr != nil {
			return rollbackErr
		}
		return stageErr
	}
	if lookupCount == 0 {
		registry.mu.Unlock()
		return nil
	}
	registry.mu.Unlock()

	lookupErr := lookupSettlementBatch(
		registry, batch, &attempts, &entries, &lookups, lookupCount,
	)

	registry.mu.Lock()
	if lookupErr != nil || registry.closed || registry.failure != nil {
		rollbackErr := rollbackSettlementLocked(
			registry, &attempts, attemptCount, &entries, entryCount,
		)
		if lookupErr == nil {
			switch {
			case registry.failure != nil:
				lookupErr = registry.failure
			case registry.closed:
				lookupErr = ErrRegistryClosed
			}
		}
		registry.mu.Unlock()
		if rollbackErr != nil {
			return rollbackErr
		}
		return lookupErr
	}
	commitErr := commitSettlementLocked(
		registry, batch.Group(), &attempts, attemptCount, &entries, entryCount,
	)
	if commitErr != nil && registry.failure == nil {
		rollbackErr := rollbackSettlementLocked(
			registry, &attempts, attemptCount, &entries, entryCount,
		)
		if rollbackErr != nil {
			commitErr = rollbackErr
		} else if errors.Is(commitErr, ErrRegistryCorrupt) {
			commitErr = registry.corruptLocked(commitErr)
		}
	}
	registry.mu.Unlock()
	return commitErr
}

func stageSettlementLocked[Batch appliedBatchView](
	registry *Registry,
	batch Batch,
	ownerEpoch uint64,
	attempts *[raftmodel.MaxNormalApplyBatchEntries]settlementAttemptItem,
	entries *[raftmodel.MaxNormalApplyBatchEntries]settlementEntryItem,
	lookups *[raftmodel.MaxNormalApplyBatchEntries]settlementLookupItem,
	attemptTable *[settlementIndexSlots]settlementIndexSlot,
	entryTable *[settlementIndexSlots]settlementIndexSlot,
) (attemptCount, entryCount, lookupCount int, err error) {
	for offset := 0; offset < batch.Len(); offset++ {
		entry, ok := batch.Entry(offset)
		if !ok || entry.Meta.Type != pb.EntryNormal ||
			entry.Meta.Index != batch.FirstIndex()+uint64(offset) {
			return attemptCount, entryCount, lookupCount, ErrSettlementRange
		}
		if len(entry.Data) == 0 {
			continue
		}
		if replicatedstate.IsOwnershipTransition(entry.Data) {
			transition, transitionErr := replicatedstate.OpenOwnershipTransition(entry.Data)
			if transitionErr != nil || !transitionMatchesGroup(transition, batch.Group()) {
				return attemptCount, entryCount, lookupCount,
					fmt.Errorf("%w: ownership transition", ErrSettlementResult)
			}
			continue
		}
		identity, identityErr := openCommandIdentity(batch.Group(), entry.Data)
		if identityErr != nil {
			return attemptCount, entryCount, lookupCount,
				fmt.Errorf("%w: command: %v", ErrSettlementResult, identityErr)
		}
		hash := hashPosition(registry.seed, identity.position, identity.tenant) & registry.hashMask
		entryIndex, _, found, findErr := registry.findEntryLocked(identity, hash)
		if errors.Is(findErr, ErrIdentityCapacity) {
			found = false
			findErr = nil
		}
		if findErr != nil {
			return attemptCount, entryCount, lookupCount, findErr
		}
		if !found {
			continue
		}
		logical := &registry.entries[entryIndex]
		if logical.fingerprint != identity.fingerprint || logical.logical != identity.logical {
			continue
		}
		attemptIndex, found, attemptErr := registry.findAttemptLocked(
			logical, identity.attempt, ownerEpoch,
		)
		if attemptErr != nil {
			return attemptCount, entryCount, lookupCount, attemptErr
		}
		if !found {
			continue
		}
		attempt := &registry.attempts[attemptIndex]
		if attempt.ownerEpoch != ownerEpoch {
			if attempt.hasFlag(attemptAdmitted) || attempt.state == attemptPending ||
				attempt.state == attemptSettling {
				return attemptCount, entryCount, lookupCount,
					errors.Join(ErrRegistryCorrupt, errors.New("settlement found a foreign source epoch"))
			}
			continue
		}
		if attempt.state == attemptComplete && infrastructureOutcome(attempt.outcome) {
			continue
		}

		stagedAttempt, repeatedAttempt := settlementIndexLookup(attemptTable, attemptIndex)
		if !repeatedAttempt {
			if attempt.hasFlag(attemptSettlementPinned) || attempt.entry != entryIndex ||
				attempt.entryGeneration != logical.generation {
				return attemptCount, entryCount, lookupCount,
					errors.Join(ErrRegistryCorrupt, errors.New("settlement found an invalid attempt owner"))
			}
			switch attempt.state {
			case attemptPending:
				if attempt.hasFlag(attemptLifecyclePending) ==
					attempt.hasFlag(attemptAdmitted) {
					return attemptCount, entryCount, lookupCount,
						errors.Join(ErrRegistryCorrupt, errors.New("pending settlement has an invalid lifecycle"))
				}
			case attemptComplete:
				if attempt.hasFlag(attemptAdmitted) ||
					attempt.hasFlag(attemptHasCompletion) && logical.completionLen == 0 {
					return attemptCount, entryCount, lookupCount,
						errors.Join(ErrRegistryCorrupt, errors.New("complete settlement has invalid retained state"))
				}
			default:
				return attemptCount, entryCount, lookupCount,
					errors.Join(ErrRegistryCorrupt, errors.New("settlement found an invalid attempt state"))
			}
			stagedAttempt = uint16(attemptCount)
			attempts[attemptCount] = settlementAttemptItem{
				attempt: attemptIndex, attemptGeneration: attempt.generation,
				entry: entryIndex, entryGeneration: logical.generation,
				appliedIndex:    entry.Meta.Index,
				originalOutcome: attempt.outcome, originalState: attempt.state,
				originalCompletion: attempt.hasFlag(attemptHasCompletion),
			}
			attempt.setFlag(attemptSettlementPinned)
			if attempt.state == attemptPending {
				attempt.state = attemptSettling
			}
			settlementIndexInsert(attemptTable, attemptIndex, stagedAttempt)
			attemptCount++
		} else {
			item := &attempts[stagedAttempt]
			wantState := item.originalState
			if wantState == attemptPending {
				wantState = attemptSettling
			}
			if !attempt.hasFlag(attemptSettlementPinned) || attempt.state != wantState ||
				item.entry != entryIndex || item.entryGeneration != logical.generation {
				return attemptCount, entryCount, lookupCount,
					errors.Join(ErrRegistryCorrupt, errors.New("settlement duplicate lost its generation fence"))
			}
		}

		stagedEntry, repeatedEntry := settlementIndexLookup(entryTable, entryIndex)
		if !repeatedEntry {
			if (logical.completionLen == 0) != (logical.completionApplied == 0) {
				return attemptCount, entryCount, lookupCount,
					errors.Join(ErrRegistryCorrupt, errors.New("settlement found invalid retained completion metadata"))
			}
			stagedEntry = uint16(entryCount)
			entries[entryCount] = settlementEntryItem{
				entry: entryIndex, entryGeneration: logical.generation,
				originalCompletionApplied: logical.completionApplied,
				originalCompletionBytes:   logical.completionLen,
			}
			settlementIndexInsert(entryTable, entryIndex, stagedEntry)
			entryCount++
		} else if entries[stagedEntry].entryGeneration != logical.generation {
			return attemptCount, entryCount, lookupCount,
				errors.Join(ErrRegistryCorrupt, errors.New("settlement entry generation changed while staging"))
		}
		lookups[lookupCount] = settlementLookupItem{
			offset: uint16(offset), attempt: stagedAttempt, entry: stagedEntry,
		}
		lookupCount++
	}
	return attemptCount, entryCount, lookupCount, nil
}

func lookupSettlementBatch[Batch appliedBatchView](
	registry *Registry,
	batch Batch,
	attempts *[raftmodel.MaxNormalApplyBatchEntries]settlementAttemptItem,
	entries *[raftmodel.MaxNormalApplyBatchEntries]settlementEntryItem,
	lookups *[raftmodel.MaxNormalApplyBatchEntries]settlementLookupItem,
	lookupCount int,
) error {
	if err := batch.BeginCompletionLookup(&registry.settlementLookup); err != nil {
		return unexpectedSettlementError(err)
	}
	var lookupErr error
	for index := 0; index < lookupCount; index++ {
		item := lookups[index]
		entry, ok := batch.Entry(int(item.offset))
		if !ok || len(entry.Data) == 0 {
			lookupErr = ErrSettlementRange
			break
		}
		identity, identityErr := openCommandIdentity(batch.Group(), entry.Data)
		if identityErr != nil {
			lookupErr = fmt.Errorf("%w: command: %v", ErrSettlementResult, identityErr)
			break
		}
		stagedEntry := &entries[item.entry]
		dst := registry.settlementScratch[:0]
		direct := stagedEntry.originalCompletionBytes == 0 && stagedEntry.completionBytes == 0
		if direct {
			dst = registry.completionSlot(stagedEntry.entry)[:0]
			stagedEntry.touched = true
		}
		lookup, outcome, resultErr := settlementLookup(
			batch, &registry.settlementLookup, int(item.offset), dst, identity,
		)
		if resultErr == nil && len(lookup.Bytes) != 0 &&
			(len(dst) != 0 || cap(dst) == 0 || &lookup.Bytes[0] != &dst[:cap(dst)][0]) {
			resultErr = ErrSettlementResult
		}
		if resultErr != nil {
			clear(registry.settlementScratch)
			lookupErr = resultErr
			break
		}
		completionBytes := uint16(len(lookup.Bytes))
		stagedAttempt := &attempts[item.attempt]
		if stagedAttempt.observed {
			if stagedAttempt.outcome != outcome ||
				stagedAttempt.completionBytes != completionBytes ||
				stagedAttempt.completionApplied != lookup.AppliedSequence {
				lookupErr = ErrSettlementResult
			}
		} else {
			stagedAttempt.outcome = outcome
			stagedAttempt.completionBytes = completionBytes
			stagedAttempt.completionApplied = lookup.AppliedSequence
			stagedAttempt.observed = true
		}
		if lookupErr == nil && stagedAttempt.originalState == attemptComplete &&
			(stagedAttempt.originalOutcome != outcome ||
				stagedAttempt.originalCompletion != (completionBytes != 0)) {
			lookupErr = ErrSettlementResult
		}
		if lookupErr == nil && completionBytes != 0 {
			slot := registry.completionSlot(stagedEntry.entry)
			switch {
			case stagedEntry.originalCompletionBytes != 0:
				if stagedEntry.originalCompletionApplied != lookup.AppliedSequence ||
					stagedEntry.originalCompletionBytes != completionBytes ||
					!bytes.Equal(slot[:stagedEntry.originalCompletionBytes], lookup.Bytes) {
					lookupErr = ErrSettlementResult
				}
			case stagedEntry.completionBytes != 0:
				if stagedEntry.completionApplied != lookup.AppliedSequence ||
					stagedEntry.completionBytes != completionBytes ||
					!bytes.Equal(slot[:stagedEntry.completionBytes], lookup.Bytes) {
					lookupErr = ErrSettlementResult
				}
			default:
				if !direct || lookup.AppliedSequence == 0 {
					lookupErr = ErrSettlementResult
				} else {
					stagedEntry.completionApplied = lookup.AppliedSequence
					stagedEntry.completionBytes = completionBytes
				}
			}
		}
		clear(registry.settlementScratch)
		if lookupErr != nil {
			break
		}
	}
	endErr := batch.EndCompletionLookup(&registry.settlementLookup)
	if endErr != nil {
		endErr = unexpectedSettlementError(endErr)
	}
	return errors.Join(lookupErr, endErr)
}

func rollbackSettlementLocked(
	registry *Registry,
	attempts *[raftmodel.MaxNormalApplyBatchEntries]settlementAttemptItem,
	attemptCount int,
	entries *[raftmodel.MaxNormalApplyBatchEntries]settlementEntryItem,
	entryCount int,
) error {
	for index := 0; index < entryCount; index++ {
		item := entries[index]
		if item.touched {
			if item.originalCompletionBytes != 0 {
				return registry.corruptLocked(errors.New("settlement touched a published completion"))
			}
			clear(registry.completionSlot(item.entry))
		}
	}
	for index := 0; index < attemptCount; index++ {
		item := attempts[index]
		if int(item.attempt) >= len(registry.attempts) ||
			int(item.entry) >= len(registry.entries) {
			return registry.corruptLocked(errors.New("settlement rollback index is out of range"))
		}
		attempt := &registry.attempts[item.attempt]
		entry := &registry.entries[item.entry]
		wantState := item.originalState
		if wantState == attemptPending {
			wantState = attemptSettling
		}
		if !attempt.hasFlag(attemptActive) || attempt.generation != item.attemptGeneration ||
			attempt.entry != item.entry || attempt.entryGeneration != item.entryGeneration ||
			!entry.active || entry.generation != item.entryGeneration ||
			!attempt.hasFlag(attemptSettlementPinned) || attempt.state != wantState {
			return registry.corruptLocked(errors.New("settlement rollback lost its generation fence"))
		}
		attempt.clearFlag(attemptSettlementPinned)
		if item.originalState == attemptPending {
			if !attempt.hasFlag(attemptLifecyclePending) &&
				!attempt.hasFlag(attemptAdmitted) {
				if attempt.outcome == OutcomePending {
					return registry.corruptLocked(errors.New("settlement rollback lost proposal refusal outcome"))
				}
				attempt.state = attemptComplete
				if !registry.signalAttemptLocked(item.attempt, attempt) {
					return registry.failure
				}
			} else {
				attempt.state = attemptPending
			}
		}
	}
	for index := 0; index < attemptCount; index++ {
		item := attempts[index]
		attempt := &registry.attempts[item.attempt]
		if attempt.hasFlag(attemptActive) && attempt.generation == item.attemptGeneration &&
			attempt.waiterCount == 0 && registry.attemptRemovableLocked(attempt) {
			if !registry.removeAttemptLocked(item.attempt) {
				return registry.failure
			}
		}
	}
	return nil
}

func commitSettlementLocked(
	registry *Registry,
	group raftmember.GroupKey,
	attempts *[raftmodel.MaxNormalApplyBatchEntries]settlementAttemptItem,
	attemptCount int,
	entries *[raftmodel.MaxNormalApplyBatchEntries]settlementEntryItem,
	entryCount int,
) error {
	pendingToRelease := 0
	completionToRetain := 0
	for index := 0; index < attemptCount; index++ {
		item := attempts[index]
		if !item.observed || int(item.attempt) >= len(registry.attempts) ||
			int(item.entry) >= len(registry.entries) {
			return errors.Join(ErrRegistryCorrupt, errors.New("settlement staged an invalid attempt"))
		}
		attempt := &registry.attempts[item.attempt]
		entry := &registry.entries[item.entry]
		wantState := item.originalState
		if wantState == attemptPending {
			wantState = attemptSettling
		}
		if !attempt.hasFlag(attemptActive) || attempt.generation != item.attemptGeneration ||
			attempt.entry != item.entry || attempt.entryGeneration != item.entryGeneration ||
			!entry.active || entry.generation != item.entryGeneration ||
			!attempt.hasFlag(attemptSettlementPinned) || attempt.state != wantState {
			return errors.Join(ErrRegistryCorrupt, errors.New("settlement staged attempt changed ownership"))
		}
		if item.originalState == attemptComplete &&
			(attempt.outcome != item.originalOutcome ||
				attempt.hasFlag(attemptHasCompletion) != item.originalCompletion ||
				attempt.hasFlag(attemptAdmitted)) {
			return errors.Join(ErrRegistryCorrupt, errors.New("complete settlement changed while staged"))
		}
		if item.originalState == attemptPending && attempt.hasFlag(attemptAdmitted) {
			if attempt.hasFlag(attemptLifecyclePending) {
				return errors.Join(ErrRegistryCorrupt, errors.New("admitted settlement retained a lifecycle callback"))
			}
			pendingToRelease++
		}
	}
	if pendingToRelease != 0 {
		groupIndex, found, groupErr := registry.findGroupLocked(group)
		if groupErr != nil || !found ||
			registry.groupTable[groupIndex].pendingAttempts < uint32(pendingToRelease) ||
			registry.stats.PendingGroups == 0 ||
			registry.stats.PendingAdmittedAttempts < pendingToRelease {
			return errors.Join(ErrRegistryCorrupt, errors.New("settlement pending-group counters are inconsistent"))
		}
	}
	for index := 0; index < entryCount; index++ {
		item := entries[index]
		if int(item.entry) >= len(registry.entries) {
			return errors.Join(ErrRegistryCorrupt, errors.New("settlement entry index is out of range"))
		}
		entry := &registry.entries[item.entry]
		if !entry.active || entry.generation != item.entryGeneration ||
			entry.completionLen != item.originalCompletionBytes ||
			entry.completionApplied != item.originalCompletionApplied {
			return errors.Join(ErrRegistryCorrupt, errors.New("settlement completion identity changed before commit"))
		}
		if item.completionBytes == 0 {
			if item.touched {
				clear(registry.completionSlot(item.entry))
			}
			continue
		}
		if item.originalCompletionBytes != 0 || !item.touched ||
			int(item.completionBytes) > completionSlotBytes || item.completionApplied == 0 {
			return errors.Join(ErrRegistryCorrupt, errors.New("settlement staged invalid completion metadata"))
		}
		completionToRetain += int(item.completionBytes)
	}
	if registry.stats.RetainedCompletionBytes < 0 ||
		registry.stats.RetainedCompletionBytes > len(registry.completionArena) ||
		completionToRetain > len(registry.completionArena)-registry.stats.RetainedCompletionBytes ||
		int64(completionToRetain) > registry.limits.MaxRetainedCompletionBytes-
			int64(registry.stats.RetainedCompletionBytes) {
		return errors.Join(ErrRegistryCorrupt, errors.New("settlement completion byte counter is inconsistent"))
	}
	for index := 0; index < entryCount; index++ {
		item := entries[index]
		if item.completionBytes == 0 {
			continue
		}
		entry := &registry.entries[item.entry]
		entry.completionLen = item.completionBytes
		entry.completionApplied = item.completionApplied
		registry.stats.RetainedCompletionBytes += int(item.completionBytes)
		registry.stats.PeakCompletionBytes = max(
			registry.stats.PeakCompletionBytes,
			registry.stats.RetainedCompletionBytes,
		)
	}
	for index := 0; index < attemptCount; index++ {
		item := attempts[index]
		attempt := &registry.attempts[item.attempt]
		if item.originalState == attemptPending {
			if attempt.hasFlag(attemptAdmitted) {
				if !registry.unlinkGroupPendingAttemptLocked(item.attempt) {
					registry.poisonLocked(errors.New("settled attempt lost its pending group record"))
					return registry.failure
				}
			}
			attempt.appliedIndex = item.appliedIndex
			attempt.outcome = item.outcome
			if item.completionBytes != 0 {
				attempt.setFlag(attemptHasCompletion)
			} else {
				attempt.clearFlag(attemptHasCompletion)
			}
			attempt.state = attemptComplete
		}
		attempt.clearFlag(attemptSettlementPinned)
	}
	for index := 0; index < attemptCount; index++ {
		item := attempts[index]
		attempt := &registry.attempts[item.attempt]
		if item.originalState == attemptPending &&
			!registry.signalAttemptLocked(item.attempt, attempt) {
			return registry.failure
		}
		if attempt.waiterCount == 0 && registry.attemptRemovableLocked(attempt) {
			if !registry.removeAttemptLocked(item.attempt) {
				return registry.failure
			}
		}
	}
	return nil
}

func validateBatchSource[Batch appliedBatchView](batch Batch) error {
	source := batch.Source()
	publication := batch.FinalPublication()
	if batch.Len() <= 0 || batch.Len() > raftmodel.MaxNormalApplyBatchEntries ||
		source.Group == (raftmember.GroupKey{}) || source.Group != batch.Group() ||
		source.AllocationGeneration == 0 ||
		source.MemberID == 0 ||
		source.StoreID == ([16]byte{}) || source.NodeIncarnation == 0 ||
		source.ReadyID == 0 || source.FirstIndex == 0 || source.LastIndex < source.FirstIndex ||
		source.FirstIndex != batch.FirstIndex() || source.LastIndex != batch.LastIndex() ||
		source.ReadyID != batch.ReadyID() ||
		source.LastIndex-source.FirstIndex+1 != uint64(batch.Len()) ||
		publication.Applied != source.LastIndex ||
		publication.DataChainDigest == ([32]byte{}) ||
		source.FinalDataChainDigest != publication.DataChainDigest {
		return ErrSettlementRange
	}
	return nil
}

func settlementIndexLookup(
	table *[settlementIndexSlots]settlementIndexSlot,
	key uint32,
) (uint16, bool) {
	start := uint32(uint64(key) * 11400714819323198485 >> 56)
	for offset := uint32(0); offset < settlementIndexSlots; offset++ {
		slot := table[(start+offset)&(settlementIndexSlots-1)]
		if !slot.used {
			return 0, false
		}
		if slot.key == key {
			return slot.item, true
		}
	}
	return 0, false
}

func settlementIndexInsert(
	table *[settlementIndexSlots]settlementIndexSlot,
	key uint32,
	item uint16,
) {
	start := uint32(uint64(key) * 11400714819323198485 >> 56)
	for offset := uint32(0); offset < settlementIndexSlots; offset++ {
		index := (start + offset) & (settlementIndexSlots - 1)
		if !table[index].used {
			table[index] = settlementIndexSlot{key: key, item: item, used: true}
			return
		}
	}
	panic("raftserve: bounded settlement index overflow")
}

func settlementLookup[Batch appliedBatchView](
	batch Batch,
	workspace *raftmember.AppliedBatchCompletionWorkspace,
	offset int,
	dst []byte,
	identity commandIdentity,
) (replicatedstate.CompletionLookup, OutcomeCode, error) {
	lookup, hasCommand, lookupErr := batch.LookupCompletionIntoWorkspace(
		workspace, offset, dst,
	)
	if !hasCommand {
		return replicatedstate.CompletionLookup{}, OutcomePending, ErrSettlementResult
	}
	outcome, known := outcomeCode(lookupErr)
	if !known {
		return replicatedstate.CompletionLookup{}, OutcomePending,
			unexpectedSettlementError(lookupErr)
	}
	if lookupErr != nil {
		return replicatedstate.CompletionLookup{}, outcome, nil
	}
	if err := validateCompletionLookup(identity, lookup); err != nil {
		return replicatedstate.CompletionLookup{}, OutcomePending, err
	}
	if len(lookup.Bytes) > completionSlotBytes {
		return replicatedstate.CompletionLookup{}, OutcomePending, ErrSettlementResult
	}
	return lookup, outcome, nil
}

func transitionMatchesGroup(
	transition replicatedstate.OwnershipTransitionView,
	group raftmember.GroupKey,
) bool {
	return transition.ClusterID == group.ClusterID &&
		transition.ClusterIncarnation == group.ClusterIncarnation &&
		transition.TopologyRecoveryEpoch == group.TopologyRecoveryEpoch &&
		transition.ShardIncarnation == group.ShardIncarnation &&
		transition.GroupID == group.GroupID
}

func validateCompletionLookup(
	identity commandIdentity,
	lookup replicatedstate.CompletionLookup,
) error {
	if lookup.Key != identity.position.sessionDigest || len(lookup.Bytes) == 0 ||
		lookup.AppliedSequence == 0 {
		return ErrSettlementResult
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil {
		return fmt.Errorf("%w: completion: %v", ErrSettlementResult, err)
	}
	position := identity.position
	if completion.ClusterID != position.group.ClusterID ||
		completion.ClusterIncarnation != position.group.ClusterIncarnation ||
		completion.TopologyRecoveryEpoch != position.group.TopologyRecoveryEpoch ||
		completion.ShardIncarnation != position.group.ShardIncarnation ||
		completion.GroupID != position.group.GroupID ||
		!bytes.Equal(completion.Tenant, identity.tenant) ||
		completion.ClientID != position.clientID ||
		completion.ClientSequence != position.sequence ||
		completion.Fingerprint != identity.fingerprint ||
		completion.AppliedSequence != lookup.AppliedSequence ||
		completion.Storage != replication.CompletionInline {
		return ErrSettlementResult
	}
	if identity.transactionRole != 0 {
		result, resultErr := replicatedstate.OpenTransactionCompletionResult(
			completion.ResultCode, completion.InlineResult,
		)
		if resultErr != nil ||
			completion.ResultFormat != replicatedstate.ResultFormatTransaction ||
			completion.ResultLength != uint64(len(completion.InlineResult)) ||
			result.Role != identity.transactionRole ||
			result.Operation != identity.transactionOperation {
			return ErrSettlementResult
		}
	} else {
		if completion.ResultFormat != replicatedstate.ResultFormatMutation ||
			completion.ResultLength != uint64(len(completion.InlineResult)) {
			return ErrSettlementResult
		}
		if _, resultErr := replicatedstate.OpenMutationCompletionResult(
			completion.ResultCode, completion.InlineResult,
		); resultErr != nil {
			return ErrSettlementResult
		}
	}
	if position.namespace == requestNamespaceOpen {
		if position.epoch != 0 || completion.ClientEpoch == 0 {
			return ErrSettlementResult
		}
	} else if completion.ClientEpoch != position.epoch {
		return ErrSettlementResult
	}
	return nil
}
