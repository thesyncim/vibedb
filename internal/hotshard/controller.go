// Package hotshard connects bounded shard pressure evidence to the existing
// deterministic topology schedulers. It is an advisory controller: replicated
// catalog generations and authority revisions fence evidence, while the sink
// remains the only durable operation-admission authority.
package hotshard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/topologyscheduler"
)

const (
	// MaxReports bounds one controller partition. Large clusters partition the
	// replicated pressure directory by group identity; tenants are never the
	// unit of scheduling or controller ownership.
	MaxReports = topologyscheduler.MaxReplicaMoveCandidates
	indexSlots = MaxReports * 2
)

var ErrInvalidPressureCut = errors.New("hotshard: invalid replicated pressure cut")

// Report is one exact group/window observation produced from the shard's
// fixed-space SABLE recorder. Recommendation carries bucket distribution and
// pressure; Demand and MigrationBytes feed replica movement and admission.
type Report struct {
	Group          raftmember.GroupKey
	Recommendation autosplit.Recommendation
	Demand         autosplit.CapacityVector
	MigrationBytes uint64
}

// View is one replicated health-authority cut. Revision advances by consensus,
// not elapsed time. Reports must be strictly ordered by Group and contain at
// most one observation for an exact source incarnation.
type View struct {
	CatalogGeneration uint64
	AuthorityRevision uint64
	Reports           []Report
	Nodes             []topologyscheduler.NodeCapacity
}

// Policy composes sustained-hotness, split admission, and replica-move bounds.
// MoveTriggerPressurePPM avoids moving cold allocations merely because a node
// is busy for unrelated work.
type Policy struct {
	Tracker                autosplit.TrackerPolicy
	Split                  topologyscheduler.Policy
	Move                   topologyscheduler.ReplicaMovePolicy
	MoveTriggerPressurePPM uint64
}

func DefaultPolicy() Policy {
	policy := Policy{
		Tracker: autosplit.DefaultTrackerPolicy(), Split: topologyscheduler.DefaultPolicy(),
		Move: topologyscheduler.DefaultReplicaMovePolicy(), MoveTriggerPressurePPM: 900_000,
	}
	// The current catalog authority publishes one global generation CAS at a
	// time. Admit one topology operation per pressure cut; this is not a shard,
	// transaction, or cluster-size ceiling. A later batch catalog transition can
	// raise both without changing evidence collection.
	policy.Split.MaxBatch = 1
	policy.Split.MaxPerDistribution = 1
	policy.Move.MaxMoves = 1
	return policy
}

// SplitWork and MoveWork are detached scheduler results retained in one
// canonical admission. They grant no catalog, placement, or Raft authority.
type SplitWork struct {
	Group     raftmember.GroupKey
	Candidate topologyscheduler.SplitCandidate
}

type MoveWork struct {
	Group     raftmember.GroupKey
	Selection topologyscheduler.ReplicaMoveSelection
}

// Admission is the fixed-capacity idempotent handoff to the replicated
// operation journal. ID is byte-identical for retries of the same authority
// cut and selected work, including after an outcome-unknown response.
type Admission struct {
	ID                [sha256.Size]byte
	CatalogGeneration uint64
	AuthorityRevision uint64
	SplitCount        uint8
	MoveCount         uint8
	Splits            [topologyscheduler.MaxBatch]SplitWork
	Moves             [topologyscheduler.MaxBatch]MoveWork
}

func (a Admission) Empty() bool { return a.SplitCount == 0 && a.MoveCount == 0 }

// Sink durably submits one idempotent admission into the replicated catalog
// operation authority. Implementations must settle duplicate ID as success and
// reject different bytes at the same ID.
type Sink interface {
	SubmitHotShardAdmission(context.Context, Admission) error
}

type trackerEntry struct {
	source autosplit.SourceIdentity
	group  raftmember.GroupKey
	track  autosplit.Tracker
	stamp  uint64
	used   bool
}

// Checkpoint is a fixed-space restart image. It intentionally contains no wall
// clock or duration. Persist it beside the replicated directory cursor.
type Checkpoint struct {
	CatalogGeneration uint64
	AuthorityRevision uint64
	Stamp             uint64
	Count             uint16
	Entries           [MaxReports]CheckpointEntry
}

type CheckpointEntry struct {
	Group   raftmember.GroupKey
	Tracker autosplit.TrackerCheckpoint
	Stamp   uint64
	Used    bool
}

// Controller owns bounded tracker and scheduler scratch. Process is single-
// owner; routed request hot paths only touch shard-local Recorder lanes.
type Controller struct {
	policy Policy

	catalogGeneration uint64
	authorityRevision uint64
	stamp             uint64
	count             uint16
	index             [indexSlots]uint16
	entries           [MaxReports]trackerEntry

	// Undo makes a failed/ambiguous sink submission retryable byte-for-byte
	// without allocating or partially consuming evidence.
	undoSlots [MaxReports]uint16
	undo      [MaxReports]trackerEntry
	undoCount uint16

	splitCandidates [MaxReports]topologyscheduler.SplitCandidate
	splitGroups     [MaxReports]raftmember.GroupKey
	moveCandidates  [MaxReports]topologyscheduler.ReplicaMoveCandidate
	moveGroups      [MaxReports]raftmember.GroupKey
	splitWorkspace  topologyscheduler.Workspace
	moveWorkspace   topologyscheduler.ReplicaMoveWorkspace
}

func New(policy Policy) (*Controller, error) {
	if !validPolicy(policy) {
		return nil, ErrInvalidPressureCut
	}
	return &Controller{policy: policy}, nil
}

func Restore(policy Policy, checkpoint Checkpoint) (*Controller, error) {
	controller, err := New(policy)
	if err != nil || checkpoint.Count > MaxReports ||
		(checkpoint.AuthorityRevision != 0 && checkpoint.CatalogGeneration == 0) {
		return nil, ErrInvalidPressureCut
	}
	controller.catalogGeneration = checkpoint.CatalogGeneration
	controller.authorityRevision = checkpoint.AuthorityRevision
	controller.stamp = checkpoint.Stamp
	for ordinal := range checkpoint.Entries {
		entry := checkpoint.Entries[ordinal]
		if !entry.Used {
			if entry != (CheckpointEntry{}) {
				return nil, ErrInvalidPressureCut
			}
			continue
		}
		tracker, ok := autosplit.RestoreTracker(entry.Tracker)
		if !ok || entry.Group == (raftmember.GroupKey{}) || entry.Stamp == 0 {
			return nil, ErrInvalidPressureCut
		}
		source := entry.Tracker.Source
		if _, found := controller.find(source); found ||
			!controller.insertIndex(source, uint16(ordinal)) {
			return nil, ErrInvalidPressureCut
		}
		controller.entries[ordinal] = trackerEntry{
			source: source, group: entry.Group, track: tracker, stamp: entry.Stamp, used: true,
		}
		controller.count++
	}
	if controller.count != checkpoint.Count {
		return nil, ErrInvalidPressureCut
	}
	return controller, nil
}

func (c *Controller) Checkpoint() Checkpoint {
	if c == nil {
		return Checkpoint{}
	}
	out := Checkpoint{CatalogGeneration: c.catalogGeneration,
		AuthorityRevision: c.authorityRevision, Stamp: c.stamp, Count: c.count}
	for ordinal := range c.entries {
		entry := &c.entries[ordinal]
		if entry.used {
			out.Entries[ordinal] = CheckpointEntry{Group: entry.group,
				Tracker: entry.track.Checkpoint(), Stamp: entry.stamp, Used: true}
		}
	}
	return out
}

// Process validates one complete replicated view, advances sustained evidence,
// feeds both existing topology schedulers, and submits their canonical union.
// State advances only after the sink settles success. Empty cuts advance the
// replicated cursor without calling the sink.
func (c *Controller) Process(
	ctx context.Context, catalog *gateway.Snapshot, view View, sink Sink,
) (Admission, error) {
	if c == nil || ctx == nil || catalog == nil || sink == nil ||
		view.CatalogGeneration != catalog.Generation() || view.AuthorityRevision == 0 ||
		view.AuthorityRevision <= c.authorityRevision || len(view.Reports) > MaxReports ||
		len(view.Nodes) > topologyscheduler.MaxPlacementNodes || !orderedReports(view.Reports) {
		return Admission{}, ErrInvalidPressureCut
	}
	if c.catalogGeneration != 0 && view.CatalogGeneration < c.catalogGeneration {
		return Admission{}, ErrInvalidPressureCut
	}
	c.undoCount = 0
	oldStamp, oldCount := c.stamp, c.count
	splitCount, moveCount, err := c.consumeReports(catalog, view)
	if err != nil {
		c.rollback(oldStamp, oldCount)
		return Admission{}, err
	}
	decision, err := topologyscheduler.SelectSplits(catalog,
		c.splitCandidates[:splitCount], c.policy.Split, &c.splitWorkspace)
	if err != nil {
		c.rollback(oldStamp, oldCount)
		return Admission{}, errors.Join(err, ErrInvalidPressureCut)
	}
	if decision.Count != 0 {
		// A catalog generation is one publication serial point. Recompute node
		// movement after the selected split publishes instead of admitting a
		// deliberately stale sibling operation from this generation.
		moveCount = 0
	} else {
		moveCount = c.removeSplitMoveCollisions(moveCount, decision)
	}
	var moveCut topologyscheduler.ReplicaMoveCut
	if moveCount != 0 {
		moveCut, err = topologyscheduler.SelectReplicaMoves(catalog,
			c.moveCandidates[:moveCount], view.Nodes, c.policy.Move, &c.moveWorkspace)
		if err != nil {
			c.rollback(oldStamp, oldCount)
			return Admission{}, errors.Join(err, ErrInvalidPressureCut)
		}
	}
	admission, err := c.buildAdmission(catalog, view, moveCount, decision, moveCut)
	if err != nil {
		c.rollback(oldStamp, oldCount)
		return Admission{}, err
	}
	if !admission.Empty() {
		if err = sink.SubmitHotShardAdmission(ctx, admission); err != nil {
			c.rollback(oldStamp, oldCount)
			return admission, err
		}
	}
	c.catalogGeneration, c.authorityRevision = view.CatalogGeneration, view.AuthorityRevision
	c.undoCount = 0
	return admission, nil
}

// removeSplitMoveCollisions gives range splitting priority over moving the
// same exact allocation in one authority cut. Both are useful independently,
// but composing them would deliberately create mutually stale operations.
func (c *Controller) removeSplitMoveCollisions(
	moveCount int, decision topologyscheduler.Decision,
) int {
	out := 0
	for move := 0; move < moveCount; move++ {
		collides := false
		for split := 0; split < int(decision.Count); split++ {
			ordinal := decision.Ordinals[split]
			if c.moveCandidates[move].Source == c.splitCandidates[ordinal].Recommendation.Source {
				collides = true
				break
			}
		}
		if collides {
			continue
		}
		if out != move {
			c.moveCandidates[out], c.moveGroups[out] = c.moveCandidates[move], c.moveGroups[move]
		}
		out++
	}
	return out
}

func (c *Controller) consumeReports(catalog *gateway.Snapshot, view View) (int, int, error) {
	splitCount, moveCount := 0, 0
	for index := range view.Reports {
		report := &view.Reports[index]
		recommendation := report.Recommendation
		if recommendation.WindowSequence == 0 || report.Group == (raftmember.GroupKey{}) ||
			!exactSource(catalog, recommendation.Source) {
			return 0, 0, ErrInvalidPressureCut
		}
		slot, exists := c.find(recommendation.Source)
		if !exists {
			var ok bool
			slot, ok = c.allocate()
			if !ok {
				return 0, 0, ErrInvalidPressureCut
			}
		}
		entry := &c.entries[slot]
		if exists && entry.group != report.Group {
			return 0, 0, ErrInvalidPressureCut
		}
		c.saveUndo(slot)
		if !exists {
			if entry.used {
				c.deleteIndex(entry.source)
			} else {
				c.count++
			}
			entry.source, entry.group, entry.used = recommendation.Source, report.Group, true
			if !c.insertIndex(recommendation.Source, uint16(slot)) {
				return 0, 0, ErrInvalidPressureCut
			}
		}
		if c.stamp != ^uint64(0) {
			c.stamp++
		}
		entry.stamp = c.stamp
		if entry.track.Observe(recommendation, c.policy.Tracker) {
			c.splitCandidates[splitCount] = topologyscheduler.SplitCandidate{
				CatalogGeneration: view.CatalogGeneration, Recommendation: recommendation,
				MigrationBytes: report.MigrationBytes,
			}
			c.splitGroups[splitCount] = report.Group
			splitCount++
		}
		if recommendation.CurrentPressurePPM >= c.policy.MoveTriggerPressurePPM &&
			report.Demand != (autosplit.CapacityVector{}) {
			c.moveCandidates[moveCount] = topologyscheduler.ReplicaMoveCandidate{
				CatalogGeneration: view.CatalogGeneration, Source: recommendation.Source,
				Demand: report.Demand, MigrationBytes: report.MigrationBytes,
			}
			c.moveGroups[moveCount] = report.Group
			moveCount++
		}
	}
	return splitCount, moveCount, nil
}

func (c *Controller) buildAdmission(catalog *gateway.Snapshot, view View, moveCount int,
	decision topologyscheduler.Decision, moveCut topologyscheduler.ReplicaMoveCut,
) (Admission, error) {
	out := Admission{CatalogGeneration: view.CatalogGeneration,
		AuthorityRevision: view.AuthorityRevision, SplitCount: decision.Count}
	for index := 0; index < int(decision.Count); index++ {
		ordinal := decision.Ordinals[index]
		out.Splits[index] = SplitWork{Group: c.splitGroups[ordinal],
			Candidate: c.splitCandidates[ordinal]}
	}
	if moveCut.Count() > topologyscheduler.MaxBatch {
		return Admission{}, ErrInvalidPressureCut
	}
	out.MoveCount = uint8(moveCut.Count())
	for index := 0; index < moveCut.Count(); index++ {
		candidateOrdinal, _, _, ok := moveCut.MoveAt(index)
		if !ok {
			return Admission{}, ErrInvalidPressureCut
		}
		selection, err := topologyscheduler.ResolveReplicaMove(catalog,
			c.moveCandidates[:moveCount], view.Nodes, c.policy.Move, moveCut, index)
		if err != nil {
			return Admission{}, errors.Join(err, ErrInvalidPressureCut)
		}
		out.Moves[index] = MoveWork{Group: c.moveGroups[candidateOrdinal], Selection: selection}
	}
	if !out.Empty() {
		out.ID = admissionDigest(out)
	}
	return out, nil
}

func validPolicy(policy Policy) bool {
	return policy.Tracker.WindowCount != 0 && policy.Tracker.WindowCount <= 64 &&
		policy.Tracker.RequiredWindows != 0 &&
		policy.Tracker.RequiredWindows <= policy.Tracker.WindowCount &&
		policy.Split.MaxBatch != 0 && policy.Split.MaxBatch <= topologyscheduler.MaxBatch &&
		policy.Move.MaxMoves != 0 && policy.Move.MaxMoves <= topologyscheduler.MaxBatch &&
		policy.MoveTriggerPressurePPM != 0
}

func orderedReports(reports []Report) bool {
	for index := range reports {
		if index != 0 && compareGroup(reports[index-1].Group, reports[index].Group) >= 0 {
			return false
		}
	}
	return true
}

func compareGroup(a, b raftmember.GroupKey) int {
	if order := bytes.Compare(a.ClusterID[:], b.ClusterID[:]); order != 0 {
		return order
	}
	if order := bytes.Compare(a.ClusterIncarnation[:], b.ClusterIncarnation[:]); order != 0 {
		return order
	}
	if a.TopologyRecoveryEpoch < b.TopologyRecoveryEpoch {
		return -1
	}
	if a.TopologyRecoveryEpoch > b.TopologyRecoveryEpoch {
		return 1
	}
	if order := bytes.Compare(a.ShardIncarnation[:], b.ShardIncarnation[:]); order != 0 {
		return order
	}
	if order := bytes.Compare(a.GroupID[:], b.GroupID[:]); order != 0 {
		return order
	}
	return 0
}

func exactSource(catalog *gateway.Snapshot, source autosplit.SourceIdentity) bool {
	manifest, ok := catalog.Manifest(source.Distribution)
	if !ok || manifest.Version() != source.RoutingVersion {
		return false
	}
	shard, ok := manifest.ShardMetadataForRange(source.Range)
	return ok && shard.ID == source.Shard && shard.AllocationGeneration == source.AllocationGeneration &&
		shard.Epoch == source.OwnershipEpoch
}

func (c *Controller) saveUndo(slot int) {
	for index := 0; index < int(c.undoCount); index++ {
		if int(c.undoSlots[index]) == slot {
			return
		}
	}
	c.undoSlots[c.undoCount], c.undo[c.undoCount] = uint16(slot), c.entries[slot]
	c.undoCount++
}

func (c *Controller) rollback(stamp uint64, count uint16) {
	for index := int(c.undoCount) - 1; index >= 0; index-- {
		slot := int(c.undoSlots[index])
		current, old := c.entries[slot], c.undo[index]
		if current.used {
			c.deleteIndex(current.source)
		}
		c.entries[slot] = old
		if old.used {
			_ = c.insertIndex(old.source, uint16(slot))
		}
	}
	c.stamp, c.count = stamp, count
	c.undoCount = 0
}

func (c *Controller) find(source autosplit.SourceIdentity) (int, bool) {
	start := int(sourceHash(source)) & (indexSlots - 1)
	for probe := 0; probe < indexSlots; probe++ {
		reference := c.index[(start+probe)&(indexSlots-1)]
		if reference == 0 {
			return 0, false
		}
		if reference != ^uint16(0) {
			slot := int(reference - 1)
			if c.entries[slot].used && c.entries[slot].source == source {
				return slot, true
			}
		}
	}
	return 0, false
}

func (c *Controller) allocate() (int, bool) {
	if c.count < MaxReports {
		for index := range c.entries {
			if !c.entries[index].used {
				return index, true
			}
		}
	}
	oldest, stamp := -1, ^uint64(0)
	for index := range c.entries {
		if c.entries[index].stamp < stamp {
			oldest, stamp = index, c.entries[index].stamp
		}
	}
	return oldest, oldest >= 0
}

func (c *Controller) insertIndex(source autosplit.SourceIdentity, slot uint16) bool {
	start, tombstone := int(sourceHash(source))&(indexSlots-1), -1
	for probe := 0; probe < indexSlots; probe++ {
		position := (start + probe) & (indexSlots - 1)
		switch c.index[position] {
		case ^uint16(0):
			if tombstone < 0 {
				tombstone = position
			}
		case 0:
			if tombstone >= 0 {
				position = tombstone
			}
			c.index[position] = slot + 1
			return true
		}
	}
	if tombstone >= 0 {
		c.index[tombstone] = slot + 1
		return true
	}
	return false
}

func (c *Controller) deleteIndex(source autosplit.SourceIdentity) {
	start := int(sourceHash(source)) & (indexSlots - 1)
	for probe := 0; probe < indexSlots; probe++ {
		position := (start + probe) & (indexSlots - 1)
		reference := c.index[position]
		if reference == 0 {
			return
		}
		if reference != ^uint16(0) && c.entries[reference-1].source == source {
			c.index[position] = ^uint16(0)
			return
		}
	}
}

func sourceHash(source autosplit.SourceIdentity) uint64 {
	value := uint64(1469598103934665603)
	mix := func(bytes []byte) {
		for _, b := range bytes {
			value ^= uint64(b)
			value *= 1099511628211
		}
	}
	mix([]byte(source.Distribution))
	mix([]byte{0})
	mix([]byte(source.Shard))
	mix([]byte{0})
	mix(source.Range.Start[:])
	mix(source.Range.End.Point[:])
	var scalar [8]byte
	for _, v := range []uint64{uint64(source.AllocationGeneration), uint64(source.RoutingVersion), uint64(source.OwnershipEpoch)} {
		binary.LittleEndian.PutUint64(scalar[:], v)
		mix(scalar[:])
	}
	if source.Range.End.Max {
		mix([]byte{1})
	} else {
		mix([]byte{0})
	}
	mix([]byte{source.BucketBits})
	return value
}

func admissionDigest(admission Admission) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("vibedb/hot-shard-admission\x00"))
	writeU64(h, admission.CatalogGeneration)
	writeU64(h, admission.AuthorityRevision)
	for index := 0; index < int(admission.SplitCount); index++ {
		hashSplit(h, admission.Splits[index])
	}
	for index := 0; index < int(admission.MoveCount); index++ {
		hashMove(h, admission.Moves[index])
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func hashSplit(h hash.Hash, work SplitWork) {
	hashGroup(h, work.Group)
	rec := work.Candidate.Recommendation
	hashSource(h, rec.Source)
	writeU64(h, rec.WindowSequence)
	writeU64(h, uint64(rec.Kind))
	writeU64(h, uint64(rec.Reason))
	writeU64(h, uint64(rec.BoundaryCount))
	for index := 0; index < int(rec.BoundaryCount); index++ {
		_, _ = h.Write(rec.Boundaries[index][:])
	}
	writeU64(h, uint64(rec.CandidateBin))
	_, _ = h.Write(rec.HotBucketStart[:])
	writeU64(h, rec.CurrentPressurePPM)
	writeU64(h, rec.PredictedPressurePPM)
	writeU64(h, rec.BenefitPPM)
	writeU64(h, rec.FanoutTaxPPM)
	writeU64(h, rec.MigrationTaxPPM)
	writeU64(h, work.Candidate.MigrationBytes)
}
func hashMove(h hash.Hash, work MoveWork) {
	hashGroup(h, work.Group)
	hashSource(h, work.Selection.Source)
	writeBytes(h, []byte(work.Selection.SourceEndpoint))
	writeBytes(h, []byte(work.Selection.TargetEndpoint))
	for _, value := range work.Selection.Demand {
		writeU64(h, value)
	}
	writeU64(h, work.Selection.MigrationBytes)
}
func hashGroup(h hash.Hash, group raftmember.GroupKey) {
	_, _ = h.Write(group.ClusterID[:])
	_, _ = h.Write(group.ClusterIncarnation[:])
	writeU64(h, group.TopologyRecoveryEpoch)
	_, _ = h.Write(group.ShardIncarnation[:])
	_, _ = h.Write(group.GroupID[:])
}
func hashSource(h hash.Hash, source autosplit.SourceIdentity) {
	writeBytes(h, []byte(source.Distribution))
	writeBytes(h, []byte(source.Shard))
	writeU64(h, uint64(source.AllocationGeneration))
	_, _ = h.Write(source.Range.Start[:])
	_, _ = h.Write(source.Range.End.Point[:])
	if source.Range.End.Max {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}
	writeU64(h, uint64(source.BucketBits))
	writeU64(h, uint64(source.RoutingVersion))
	writeU64(h, uint64(source.OwnershipEpoch))
}
func writeU64(h hash.Hash, value uint64) {
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], value)
	_, _ = h.Write(raw[:])
}
func writeBytes(h hash.Hash, value []byte) {
	writeU64(h, uint64(len(value)))
	_, _ = h.Write(value)
}
