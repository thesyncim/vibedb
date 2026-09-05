package raftservice

import (
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
)

var ErrProgressMetricsGroups = errors.New("raftservice: invalid progress metrics group directory")

const AbsoluteMaxProgressMetricsGroups = 1 << 14

const (
	// ProposalEntryHistogramBuckets has one slot for every normal proposal
	// batch size and a final overflow slot. Keeping the shape fixed makes the
	// diagnostics cut safe to expose without labels or an allocation on the
	// owner lane.
	ProposalEntryHistogramBuckets = raftmodel.MaxProposalBatchEntries + 1
	// ProposalBytesHistogramBuckets has a zero bucket, sixteen equal-width
	// buckets through the normal one-megabyte batching target, and one overflow
	// bucket for a valid oversized single proposal.
	ProposalBytesHistogramBuckets = 18
)

// ProgressMetrics is the bounded process-incarnation counter seam for shipped
// RF3 owner loops. It observes already-produced fixed-width progress after a
// Host turn. Successful progress uses no formatting, labels, clocks, maps, or
// callbacks. An optional first-seen failure diagnostic runs after the Host turn.
type ProgressMetrics struct {
	// ProposalFailure is optional and must be set before the owner starts.
	// It receives at most one callback per fixed reason across all lanes.
	// Calls for different reasons may be concurrent. No command or raw error
	// is exposed. The callback runs synchronously on an owner lane and should
	// perform only bounded local diagnostic work, not remote I/O.
	ProposalFailure     func(raftmember.GroupKey, ProposalFailureReason)
	proposalFailureSeen atomic.Uint64
	proposalBatches     atomic.Uint64
	proposalCommands    atomic.Uint64
	proposalBytes       atomic.Uint64
	applyBatches        atomic.Uint64
	appliedEntries      atomic.Uint64
	commitAdvancements  atomic.Uint64
	committedEntries    atomic.Uint64
	readyPersisted      atomic.Uint64
	snapshotsFinished   atomic.Uint64
	readCompletions     atomic.Uint64
	faults              atomic.Uint64
	scheduler           progressMetricsSchedulerCounters
	groups              atomic.Pointer[progressMetricsGroupTable]
}

type progressMetricsGroup struct {
	identity raftmember.RuntimeIdentity
	counters progressMetricsCounters
}

type progressMetricsCounters struct {
	proposalBatches, proposalCommands, proposalBytes atomic.Uint64
	applyBatches, appliedEntries                     atomic.Uint64
	commitAdvancements, committedEntries             atomic.Uint64
	readyPersisted, snapshotsFinished                atomic.Uint64
	readCompletions, faults                          atomic.Uint64
	scheduler                                        progressMetricsSchedulerCounters
}

type progressMetricsSchedulerCounters struct {
	proposalWindowQueued atomic.Uint64
	lateJoinUsed         atomic.Uint64
	lateJoinMissed       atomic.Uint64
	lateJoinEntries      atomic.Uint64
	proposalQueueDepth   [ProposalEntryHistogramBuckets]atomic.Uint64
	proposalEntriesReady [ProposalEntryHistogramBuckets]atomic.Uint64
	proposalBytesReady   [ProposalBytesHistogramBuckets]atomic.Uint64
}

type progressMetricsGroupTable struct {
	mask  uint64
	slots []progressMetricsGroup
}

// ProgressMetricsSnapshot is a detached, consistent-enough counter cut. Every
// field is cumulative for one process incarnation.
type ProgressMetricsSnapshot struct {
	ProposalBatches    uint64
	ProposalCommands   uint64
	ProposalBytes      uint64
	ApplyBatches       uint64
	AppliedEntries     uint64
	CommitAdvancements uint64
	CommittedEntries   uint64
	ReadyPersisted     uint64
	SnapshotsFinished  uint64
	ReadCompletions    uint64
	Faults             uint64

	// ProposalWindowQueued is the cumulative number of queued proposals
	// observed while an open normal-proposal window had room. The depth
	// histogram below preserves the per-observation queue shape. LateJoinEntries
	// is the cumulative number admitted by the one-turn opportunity.
	ProposalWindowQueued uint64
	LateJoinUsed         uint64
	LateJoinMissed       uint64
	LateJoinEntries      uint64
	// These fixed histograms are indexed by capped proposal entry count. The
	// final entry bucket is overflow.
	ProposalQueueDepthHistogram [ProposalEntryHistogramBuckets]uint64
	ProposalEntriesPerReady     [ProposalEntryHistogramBuckets]uint64
	ProposalBytesPerReady       [ProposalBytesHistogramBuckets]uint64
}

func proposalEntryHistogramBucket(value int) int {
	if value <= 0 {
		return 0
	}
	if value >= ProposalEntryHistogramBuckets {
		return ProposalEntryHistogramBuckets - 1
	}
	return value
}

func proposalBytesHistogramBucket(value int64) int {
	if value <= 0 {
		return 0
	}
	const normalBucketWidth = raftmodel.MaxProposalBatchBytes / (ProposalBytesHistogramBuckets - 2)
	bucket := int((value + normalBucketWidth - 1) / normalBucketWidth)
	if bucket >= ProposalBytesHistogramBuckets-1 {
		return ProposalBytesHistogramBuckets - 1
	}
	return bucket
}

func (counters *progressMetricsSchedulerCounters) observe(progress multiraft.Progress) {
	if progress.LateJoinQueued > 0 {
		counters.proposalWindowQueued.Add(uint64(progress.LateJoinQueued))
		counters.proposalQueueDepth[proposalEntryHistogramBucket(progress.LateJoinQueued)].Add(1)
	}
	if progress.LateJoinUsed {
		counters.lateJoinUsed.Add(1)
		if progress.LateJoinEntries > 0 {
			counters.lateJoinEntries.Add(uint64(progress.LateJoinEntries))
		}
	}
	if progress.LateJoinMissed {
		counters.lateJoinMissed.Add(1)
	}
	if progress.CapturedProposalCount > 0 {
		counters.proposalEntriesReady[proposalEntryHistogramBucket(progress.CapturedProposalCount)].Add(1)
		counters.proposalBytesReady[proposalBytesHistogramBucket(progress.CapturedProposalBytes)].Add(1)
	}
}

func (counters *progressMetricsSchedulerCounters) snapshot(snapshot *ProgressMetricsSnapshot) {
	if counters == nil || snapshot == nil {
		return
	}
	snapshot.ProposalWindowQueued = counters.proposalWindowQueued.Load()
	snapshot.LateJoinUsed = counters.lateJoinUsed.Load()
	snapshot.LateJoinMissed = counters.lateJoinMissed.Load()
	snapshot.LateJoinEntries = counters.lateJoinEntries.Load()
	for index := range counters.proposalQueueDepth {
		snapshot.ProposalQueueDepthHistogram[index] = counters.proposalQueueDepth[index].Load()
		snapshot.ProposalEntriesPerReady[index] = counters.proposalEntriesReady[index].Load()
	}
	for index := range counters.proposalBytesReady {
		snapshot.ProposalBytesPerReady[index] = counters.proposalBytesReady[index].Load()
	}
}

func (metrics *ProgressMetrics) observeProgress(progress multiraft.Progress, done bool, err error) {
	if metrics == nil {
		return
	}
	if err != nil && metrics.ProposalFailure != nil &&
		(progress.Kind == multiraft.ProgressProposal ||
			progress.Kind == multiraft.ProgressFault && progress.ProposalCount != 0) {
		metrics.observeProposalFailure(progress.Group, err)
	}
	if progress.ProposalCount > 0 {
		metrics.proposalBatches.Add(1)
		metrics.proposalCommands.Add(uint64(progress.ProposalCount))
		metrics.proposalBytes.Add(uint64(progress.ProposalBytes))
	}
	metrics.scheduler.observe(progress)
	if progress.AppliedCount > 0 {
		metrics.applyBatches.Add(1)
		metrics.appliedEntries.Add(uint64(progress.AppliedCount))
	}
	if progress.CommitAdvancements != 0 {
		metrics.commitAdvancements.Add(progress.CommitAdvancements)
	}
	if progress.CommittedEntries != 0 {
		metrics.committedEntries.Add(progress.CommittedEntries)
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
		counters.proposalBatches.Add(1)
		counters.proposalCommands.Add(uint64(progress.ProposalCount))
		counters.proposalBytes.Add(uint64(progress.ProposalBytes))
	}
	counters.scheduler.observe(progress)
	if progress.AppliedCount > 0 {
		counters.applyBatches.Add(1)
		counters.appliedEntries.Add(uint64(progress.AppliedCount))
	}
	if progress.CommitAdvancements != 0 {
		counters.commitAdvancements.Add(progress.CommitAdvancements)
	}
	if progress.CommittedEntries != 0 {
		counters.committedEntries.Add(progress.CommittedEntries)
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
	snapshot := ProgressMetricsSnapshot{
		ProposalBatches: c.proposalBatches.Load(), ProposalCommands: c.proposalCommands.Load(),
		ProposalBytes: c.proposalBytes.Load(), ApplyBatches: c.applyBatches.Load(),
		AppliedEntries: c.appliedEntries.Load(), ReadyPersisted: c.readyPersisted.Load(),
		CommitAdvancements: c.commitAdvancements.Load(), CommittedEntries: c.committedEntries.Load(),
		SnapshotsFinished: c.snapshotsFinished.Load(), ReadCompletions: c.readCompletions.Load(),
		Faults: c.faults.Load(),
	}
	c.scheduler.snapshot(&snapshot)
	return slot.identity, snapshot, true
}

func (metrics *ProgressMetrics) Snapshot() ProgressMetricsSnapshot {
	if metrics == nil {
		return ProgressMetricsSnapshot{}
	}
	snapshot := ProgressMetricsSnapshot{
		ProposalBatches:    metrics.proposalBatches.Load(),
		ProposalCommands:   metrics.proposalCommands.Load(),
		ProposalBytes:      metrics.proposalBytes.Load(),
		ApplyBatches:       metrics.applyBatches.Load(),
		AppliedEntries:     metrics.appliedEntries.Load(),
		CommitAdvancements: metrics.commitAdvancements.Load(),
		CommittedEntries:   metrics.committedEntries.Load(),
		ReadyPersisted:     metrics.readyPersisted.Load(),
		SnapshotsFinished:  metrics.snapshotsFinished.Load(),
		ReadCompletions:    metrics.readCompletions.Load(),
		Faults:             metrics.faults.Load(),
	}
	metrics.scheduler.snapshot(&snapshot)
	return snapshot
}
