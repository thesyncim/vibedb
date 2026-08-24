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
	attempt           uint32
	attemptGeneration uint64
	entry             uint32
	appliedIndex      uint64
	completionApplied uint64
	completionBytes   uint16
	outcome           OutcomeCode
	observed          bool
}

type settlementEntryItem struct {
	entry             uint32
	completionApplied uint64
	completionBytes   uint16
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
}

// SettleAppliedBatch resolves locally registered attempts in one exact
// published range. Replay entries without a local waiter are accepted. All
// matched attempts become visible together after the complete range validates.
func (registry *Registry) SettleAppliedBatch(batch raftmember.AppliedBatch) error {
	return settleAppliedBatch(registry, batch)
}

func settleAppliedBatch[Batch appliedBatchView](
	registry *Registry,
	batch Batch,
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
	var attemptTable [settlementIndexSlots]settlementIndexSlot
	var entryTable [settlementIndexSlots]settlementIndexSlot
	attemptCount := 0
	entryCount := 0
	pendingToRelease := 0
	completionToRetain := 0
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	if registry.failure != nil {
		return registry.failure
	}
	rollback := func() {
		for index := 0; index < attemptCount; index++ {
			item := attempts[index]
			attempt, _, ok := registry.validAttemptLocked(
				item.attempt, item.attemptGeneration, attemptSettling,
			)
			if ok {
				attempt.state = attemptPending
			}
		}
		for index := 0; index < entryCount; index++ {
			clear(registry.completionSlot(entries[index].entry))
		}
	}

	for offset := 0; offset < batch.Len(); offset++ {
		entry, ok := batch.Entry(offset)
		if !ok || entry.Meta.Type != pb.EntryNormal ||
			entry.Meta.Index != batch.FirstIndex()+uint64(offset) {
			rollback()
			return ErrSettlementRange
		}
		if len(entry.Data) == 0 {
			continue
		}
		if replicatedstate.IsOwnershipTransition(entry.Data) {
			transition, err := replicatedstate.OpenOwnershipTransition(entry.Data)
			if err != nil || !transitionMatchesGroup(transition, batch.Group()) {
				rollback()
				return fmt.Errorf("%w: ownership transition", ErrSettlementResult)
			}
			continue
		}
		identity, err := openCommandIdentity(batch.Group(), entry.Data)
		if err != nil {
			rollback()
			return fmt.Errorf("%w: command: %v", ErrSettlementResult, err)
		}
		hash := hashPosition(registry.seed, identity.position, identity.tenant) & registry.hashMask
		entryIndex, _, found, findErr := registry.findEntryLocked(identity, hash)
		if errors.Is(findErr, ErrIdentityCapacity) {
			found = false
			findErr = nil
		}
		if findErr != nil {
			rollback()
			if errors.Is(findErr, ErrRegistryCorrupt) {
				return registry.corruptLocked(findErr)
			}
			return findErr
		}
		if !found {
			continue
		}
		logical := &registry.entries[entryIndex]
		if logical.fingerprint != identity.fingerprint || logical.logical != identity.logical {
			continue
		}
		attemptIndex, found, attemptErr := registry.findAttemptLocked(logical, identity.attempt)
		if attemptErr != nil {
			rollback()
			return registry.corruptLocked(attemptErr)
		}
		if !found {
			continue
		}
		attempt := &registry.attempts[attemptIndex]
		if attempt.state == attemptComplete && infrastructureOutcome(attempt.outcome) {
			continue
		}
		stagedAttempt, repeated := settlementIndexLookup(&attemptTable, attemptIndex)
		if !repeated {
			switch attempt.state {
			case attemptComplete:
			case attemptPending:
				attempt.state = attemptSettling
				stagedAttempt = uint16(attemptCount)
				attempts[attemptCount] = settlementAttemptItem{
					attempt: attemptIndex, attemptGeneration: attempt.generation,
					entry: entryIndex, appliedIndex: entry.Meta.Index,
				}
				attemptCount++
				settlementIndexInsert(&attemptTable, attemptIndex, stagedAttempt)
			default:
				rollback()
				return registry.corruptLocked(errors.New("settlement found an invalid attempt state"))
			}
		} else if attempt.state != attemptSettling {
			rollback()
			return registry.corruptLocked(errors.New("settlement duplicate lost its staged attempt state"))
		}

		lookup, outcome, lookupErr := settlementLookup(
			batch, offset, registry.settlementScratch[:0], identity,
		)
		if lookupErr != nil {
			clear(registry.settlementScratch)
			rollback()
			return lookupErr
		}
		completionBytes := uint16(len(lookup.Bytes))
		if attempt.state == attemptComplete {
			if attempt.outcome != outcome || attempt.hasCompletion != (completionBytes != 0) ||
				!registry.completionMatchesLocked(logical, lookup) {
				clear(registry.settlementScratch)
				rollback()
				return ErrSettlementResult
			}
			clear(registry.settlementScratch)
			continue
		}

		item := &attempts[stagedAttempt]
		if item.observed {
			if item.outcome != outcome || item.completionBytes != completionBytes ||
				item.completionApplied != lookup.AppliedSequence {
				clear(registry.settlementScratch)
				rollback()
				return ErrSettlementResult
			}
		} else {
			item.outcome = outcome
			item.completionBytes = completionBytes
			item.completionApplied = lookup.AppliedSequence
			item.observed = true
		}
		if completionBytes != 0 {
			if logical.completionLen != 0 {
				if !registry.completionMatchesLocked(logical, lookup) {
					clear(registry.settlementScratch)
					rollback()
					return ErrSettlementResult
				}
			} else if stagedEntry, exists := settlementIndexLookup(&entryTable, entryIndex); exists {
				stored := entries[stagedEntry]
				slot := registry.completionSlot(entryIndex)
				if stored.completionApplied != lookup.AppliedSequence ||
					stored.completionBytes != completionBytes ||
					!bytes.Equal(slot[:stored.completionBytes], lookup.Bytes) {
					clear(registry.settlementScratch)
					rollback()
					return ErrSettlementResult
				}
			} else {
				entries[entryCount] = settlementEntryItem{
					entry: entryIndex, completionApplied: lookup.AppliedSequence,
					completionBytes: completionBytes,
				}
				copy(registry.completionSlot(entryIndex), lookup.Bytes)
				settlementIndexInsert(&entryTable, entryIndex, uint16(entryCount))
				entryCount++
			}
		}
		clear(registry.settlementScratch)
	}

	for index := 0; index < attemptCount; index++ {
		if !attempts[index].observed {
			rollback()
			return registry.corruptLocked(errors.New("settlement staged an unobserved attempt"))
		}
		item := attempts[index]
		attempt, _, ok := registry.validAttemptLocked(
			item.attempt, item.attemptGeneration, attemptSettling,
		)
		if !ok || attempt.entry != item.entry {
			rollback()
			return registry.corruptLocked(errors.New("settlement staged attempt changed ownership"))
		}
		if attempt.admitted {
			if attempt.lifecyclePending {
				rollback()
				return registry.corruptLocked(errors.New("admitted settlement retained a lifecycle callback"))
			}
			pendingToRelease++
		}
	}
	if pendingToRelease != 0 {
		groupIndex, found, groupErr := registry.findGroupLocked(batch.Group())
		if groupErr != nil || !found ||
			registry.groupTable[groupIndex].pendingAttempts < uint32(pendingToRelease) ||
			registry.stats.PendingGroups == 0 ||
			registry.stats.PendingAdmittedAttempts < pendingToRelease {
			rollback()
			return registry.corruptLocked(errors.New("settlement pending-group counters are inconsistent"))
		}
	}
	for index := 0; index < entryCount; index++ {
		item := entries[index]
		if int(item.entry) >= len(registry.entries) || item.completionBytes == 0 ||
			int(item.completionBytes) > completionSlotBytes ||
			item.completionApplied == 0 {
			rollback()
			return registry.corruptLocked(errors.New("settlement staged invalid completion metadata"))
		}
		entry := &registry.entries[item.entry]
		if !entry.active || entry.completionLen != 0 {
			rollback()
			return registry.corruptLocked(errors.New("settlement completion identity changed before commit"))
		}
		completionToRetain += int(item.completionBytes)
	}
	if registry.stats.RetainedCompletionBytes < 0 ||
		registry.stats.RetainedCompletionBytes > len(registry.completionArena) ||
		completionToRetain > len(registry.completionArena)-registry.stats.RetainedCompletionBytes ||
		int64(completionToRetain) > registry.limits.MaxRetainedCompletionBytes-
			int64(registry.stats.RetainedCompletionBytes) {
		rollback()
		return registry.corruptLocked(errors.New("settlement completion byte counter is inconsistent"))
	}
	for index := 0; index < entryCount; index++ {
		item := entries[index]
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
		attempt, _, _ := registry.validAttemptLocked(
			item.attempt, item.attemptGeneration, attemptSettling,
		)
		if attempt.admitted {
			if !registry.removeGroupPendingLocked(batch.Group()) {
				registry.poisonLocked(errors.New("settled attempt lost its pending group record"))
				return registry.failure
			}
			attempt.admitted = false
		}
		attempt.appliedIndex = item.appliedIndex
		attempt.outcome = item.outcome
		attempt.hasCompletion = item.completionBytes != 0
		attempt.state = attemptComplete
	}
	for index := 0; index < attemptCount; index++ {
		item := attempts[index]
		attempt := &registry.attempts[item.attempt]
		if !registry.signalAttemptLocked(item.attempt, attempt) {
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
	offset int,
	dst []byte,
	identity commandIdentity,
) (replicatedstate.CompletionLookup, OutcomeCode, error) {
	lookup, hasCommand, lookupErr := batch.LookupCompletionInto(offset, dst)
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

func (registry *Registry) completionMatchesLocked(
	entry *entryRecord,
	lookup replicatedstate.CompletionLookup,
) bool {
	if len(lookup.Bytes) == 0 {
		return true
	}
	if entry == nil || entry.completionLen == 0 ||
		entry.completionApplied != lookup.AppliedSequence ||
		int(entry.completionLen) != len(lookup.Bytes) {
		return false
	}
	// tableSlot is not the arena index. Resolve the table's generation-fenced
	// entry index before reading its completion slot.
	if int(entry.tableSlot) >= len(registry.table) {
		return false
	}
	table := registry.table[entry.tableSlot]
	if table.state != tableOccupied || table.generation != entry.generation {
		return false
	}
	slot := registry.completionSlot(table.entry)
	return bytes.Equal(slot[:entry.completionLen], lookup.Bytes)
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
		completion.Storage != replication.CompletionInline ||
		completion.ResultLength != 0 || len(completion.InlineResult) != 0 {
		return ErrSettlementResult
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
