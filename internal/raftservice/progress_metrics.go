package raftservice

import (
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

var ErrProgressMetricsGroups = errors.New("raftservice: invalid progress metrics group directory")

const AbsoluteMaxProgressMetricsGroups = 1 << 14

// ProgressMetrics is the bounded process-incarnation counter seam for shipped
// RF3 owner loops. It observes already-produced fixed-width progress after a
// Host turn; no formatting, labels, clocks, maps, or callbacks enter Raft.
type ProgressMetrics struct {
	proposalCommands  atomic.Uint64
	proposalBytes     atomic.Uint64
	appliedEntries    atomic.Uint64
	readyPersisted    atomic.Uint64
	snapshotsFinished atomic.Uint64
	readCompletions   atomic.Uint64
	faults            atomic.Uint64
	groups            atomic.Pointer[progressMetricsGroupTable]
}

type progressMetricsGroup struct {
	identity raftmember.RuntimeIdentity
	counters progressMetricsCounters
}

type progressMetricsCounters struct {
	proposalCommands, proposalBytes, appliedEntries atomic.Uint64
	readyPersisted, snapshotsFinished               atomic.Uint64
	readCompletions, faults                         atomic.Uint64
}

type progressMetricsGroupTable struct {
	mask  uint64
	slots []progressMetricsGroup
}

// ProgressMetricsSnapshot is a detached, consistent-enough counter cut. Every
// field is cumulative for one process incarnation.
type ProgressMetricsSnapshot struct {
	ProposalCommands  uint64
	ProposalBytes     uint64
	AppliedEntries    uint64
	ReadyPersisted    uint64
	SnapshotsFinished uint64
	ReadCompletions   uint64
	Faults            uint64
}

func (metrics *ProgressMetrics) observeProgress(progress multiraft.Progress, done bool, err error) {
	if metrics == nil {
		return
	}
	if progress.ProposalCount > 0 {
		metrics.proposalCommands.Add(uint64(progress.ProposalCount))
		metrics.proposalBytes.Add(uint64(progress.ProposalBytes))
	}
	if progress.AppliedCount > 0 {
		metrics.appliedEntries.Add(uint64(progress.AppliedCount))
	}
	switch progress.ReadyKind {
	case raftmember.DrivePersisted:
		metrics.readyPersisted.Add(1)
	case raftmember.DriveSnapshotFinished:
		metrics.snapshotsFinished.Add(1)
	case raftmember.DriveReadStatesFinished:
		if count := len(progress.ReadOutcomes); count != 0 {
			metrics.readCompletions.Add(uint64(count))
		}
	}
	if err != nil && done {
		metrics.faults.Add(1)
	}
	if group := metrics.group(progress.Group); group != nil {
		group.counters.observe(progress, done, err)
	}
}

func (counters *progressMetricsCounters) observe(progress multiraft.Progress, done bool, err error) {
	if progress.ProposalCount > 0 {
		counters.proposalCommands.Add(uint64(progress.ProposalCount))
		counters.proposalBytes.Add(uint64(progress.ProposalBytes))
	}
	if progress.AppliedCount > 0 {
		counters.appliedEntries.Add(uint64(progress.AppliedCount))
	}
	switch progress.ReadyKind {
	case raftmember.DrivePersisted:
		counters.readyPersisted.Add(1)
	case raftmember.DriveSnapshotFinished:
		counters.snapshotsFinished.Add(1)
	case raftmember.DriveReadStatesFinished:
		counters.readCompletions.Add(uint64(len(progress.ReadOutcomes)))
	}
	if err != nil && done {
		counters.faults.Add(1)
	}
}

// ConfigureGroups publishes a bounded immutable open-addressed directory for
// zero-allocation per-group counter updates. It must run before owner lanes;
// repeated configuration is rejected so no hot-path counter can be orphaned.
func (metrics *ProgressMetrics) ConfigureGroups(identities []raftmember.RuntimeIdentity) error {
	if metrics == nil || len(identities) == 0 || len(identities) > AbsoluteMaxProgressMetricsGroups ||
		metrics.groups.Load() != nil {
		return ErrProgressMetricsGroups
	}
	size := 2
	for size < len(identities)*2 {
		size <<= 1
	}
	table := &progressMetricsGroupTable{mask: uint64(size - 1), slots: make([]progressMetricsGroup, size)}
	for _, identity := range identities {
		if identity.Group == (raftmember.GroupKey{}) || identity.MemberID == 0 {
			return ErrProgressMetricsGroups
		}
		index := progressMetricsGroupHash(identity.Group) & table.mask
		for probes := 0; probes < size; probes++ {
			slot := &table.slots[index]
			if slot.identity.Group == (raftmember.GroupKey{}) {
				slot.identity = identity
				break
			}
			if slot.identity.Group == identity.Group {
				return ErrProgressMetricsGroups
			}
			index = (index + 1) & table.mask
		}
	}
	if !metrics.groups.CompareAndSwap(nil, table) {
		return ErrProgressMetricsGroups
	}
	return nil
}

func progressMetricsGroupHash(group raftmember.GroupKey) uint64 {
	// SplitMix64 finalization over every fixed identity limb. This is a lookup
	// table hash, not an authority digest; exact GroupKey equality resolves a
	// collision without allocation or attacker-controlled string hashing.
	value := group.TopologyRecoveryEpoch
	blocks := [...][16]byte{group.ClusterID, group.ClusterIncarnation, group.ShardIncarnation, group.GroupID}
	for _, block := range blocks {
		value ^= binary.LittleEndian.Uint64(block[:8])
		value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
		value ^= binary.LittleEndian.Uint64(block[8:])
		value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	}
	return value ^ (value >> 31)
}

func (metrics *ProgressMetrics) group(key raftmember.GroupKey) *progressMetricsGroup {
	if metrics == nil || key == (raftmember.GroupKey{}) {
		return nil
	}
	table := metrics.groups.Load()
	if table == nil {
		return nil
	}
	index := progressMetricsGroupHash(key) & table.mask
	for probes := 0; probes < len(table.slots); probes++ {
		slot := &table.slots[index]
		if slot.identity.Group == key {
			return slot
		}
		if slot.identity.Group == (raftmember.GroupKey{}) {
			return nil
		}
		index = (index + 1) & table.mask
	}
	return nil
}

// GroupProgressMetrics returns one local member's exact process-incarnation
// counter cut. The immutable directory makes the read lock-free and bounded.
func (metrics *ProgressMetrics) GroupProgressMetrics(group raftmember.GroupKey) (raftmember.RuntimeIdentity, ProgressMetricsSnapshot, bool) {
	slot := metrics.group(group)
	if slot == nil {
		return raftmember.RuntimeIdentity{}, ProgressMetricsSnapshot{}, false
	}
	c := &slot.counters
	return slot.identity, ProgressMetricsSnapshot{
		ProposalCommands: c.proposalCommands.Load(), ProposalBytes: c.proposalBytes.Load(),
		AppliedEntries: c.appliedEntries.Load(), ReadyPersisted: c.readyPersisted.Load(),
		SnapshotsFinished: c.snapshotsFinished.Load(), ReadCompletions: c.readCompletions.Load(),
		Faults: c.faults.Load(),
	}, true
}

func (metrics *ProgressMetrics) Snapshot() ProgressMetricsSnapshot {
	if metrics == nil {
		return ProgressMetricsSnapshot{}
	}
	return ProgressMetricsSnapshot{
		ProposalCommands:  metrics.proposalCommands.Load(),
		ProposalBytes:     metrics.proposalBytes.Load(),
		AppliedEntries:    metrics.appliedEntries.Load(),
		ReadyPersisted:    metrics.readyPersisted.Load(),
		SnapshotsFinished: metrics.snapshotsFinished.Load(),
		ReadCompletions:   metrics.readCompletions.Load(),
		Faults:            metrics.faults.Load(),
	}
}
