package gateway

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
)

const AbsoluteMaxDistributedMetricSamples = 1 << 15

var ErrDistributedMetrics = errors.New("gateway: invalid distributed metrics collection")

type DistributedMetricsOpen interface {
	OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type DistributedMetricsSample struct {
	Group         raftmember.GroupKey
	Member        uint64
	Node          rafttransport.NodeID
	Cut           raftservice.ProgressMetricsSnapshot
	Stages        servicemetrics.StageMetricsSnapshot
	NodeAggregate bool
	Reads         uint64
	Faults        uint64
}

type DistributedMetricsAggregate struct {
	Cut      raftservice.ProgressMetricsSnapshot
	Stages   servicemetrics.StageMetricsSnapshot
	Samples  uint64
	Reads    uint64
	Faults   uint64
	Overflow bool
}

type distributedMetricsSlot struct {
	group         raftmember.GroupKey
	member        uint64
	node          rafttransport.NodeID
	seq           atomic.Uint64
	refreshing    atomic.Bool
	values        [7]atomic.Uint64
	stages        [17]atomic.Uint64
	nodeAggregate bool
	reads         atomic.Uint64
	faults        atomic.Uint64
}

type DistributedMetrics struct {
	opener DistributedMetricsOpen
	slots  []distributedMetricsSlot
}

// NewDistributedMetrics fixes the complete group/member directory once. The
// serving read path subsequently loads only atomics and caller-owned storage.
func NewDistributedMetrics(opener DistributedMetricsOpen, routes []ReplicatedRoute) (*DistributedMetrics, error) {
	if opener == nil || len(routes) == 0 || len(routes) > AbsoluteMaxDistributedMetricSamples {
		return nil, ErrDistributedMetrics
	}
	count := 0
	for _, route := range routes {
		if route.Group == (raftmember.GroupKey{}) || len(route.Replicas) == 0 ||
			count > AbsoluteMaxDistributedMetricSamples-len(route.Replicas) {
			return nil, ErrDistributedMetrics
		}
		count += len(route.Replicas)
	}
	nodes := make([]rafttransport.NodeID, 0, count)
	for _, route := range routes {
		for _, endpoint := range route.Replicas {
			found := false
			for _, node := range nodes {
				if node == endpoint.Node {
					found = true
					break
				}
			}
			if !found {
				nodes = append(nodes, endpoint.Node)
			}
		}
	}
	if count > AbsoluteMaxDistributedMetricSamples-len(nodes) {
		return nil, ErrDistributedMetrics
	}
	metrics := &DistributedMetrics{opener: opener, slots: make([]distributedMetricsSlot, count+len(nodes))}
	index := 0
	for _, route := range routes {
		for _, endpoint := range route.Replicas {
			if endpoint.Member == 0 || endpoint.Node == (rafttransport.NodeID{}) {
				return nil, ErrDistributedMetrics
			}
			for prior := 0; prior < index; prior++ {
				if metrics.slots[prior].group == route.Group && metrics.slots[prior].member == endpoint.Member {
					return nil, ErrDistributedMetrics
				}
			}
			metrics.slots[index].group, metrics.slots[index].member, metrics.slots[index].node =
				route.Group, endpoint.Member, endpoint.Node
			index++
		}
	}
	for _, node := range nodes {
		metrics.slots[index].node, metrics.slots[index].nodeAggregate = node, true
		index++
	}
	return metrics, nil
}

func (metrics *DistributedMetrics) Len() int {
	if metrics == nil {
		return 0
	}
	return len(metrics.slots)
}

// RefreshOne performs one authenticated fixed-width exchange. Publication is
// a seqlock write; readers never observe fields from two remote cuts.
func (metrics *DistributedMetrics) RefreshOne(ctx context.Context, index int) error {
	if metrics == nil || ctx == nil || index < 0 || index >= len(metrics.slots) {
		return ErrDistributedMetrics
	}
	slot := &metrics.slots[index]
	if !slot.refreshing.CompareAndSwap(false, true) {
		return ErrDistributedMetrics
	}
	defer slot.refreshing.Store(false)
	client := servicemetrics.Client{Open: func(openCtx context.Context) (rafttransport.PeerConnection, error) {
		return metrics.opener.OpenShardControl(openCtx, slot.node)
	}}
	var snapshot servicemetrics.Snapshot
	var err error
	if slot.nodeAggregate {
		snapshot, err = client.ReadNode(ctx)
	} else {
		snapshot, err = client.ReadGroup(ctx, slot.group)
	}
	slot.reads.Add(1)
	if err != nil || snapshot.Group != slot.group || snapshot.Member != slot.member {
		slot.faults.Add(1)
		return errors.Join(ErrDistributedMetrics, err)
	}
	values := [...]uint64{snapshot.Metrics.ProposalCommands, snapshot.Metrics.ProposalBytes,
		snapshot.Metrics.AppliedEntries, snapshot.Metrics.ReadyPersisted,
		snapshot.Metrics.SnapshotsFinished, snapshot.Metrics.ReadCompletions, snapshot.Metrics.Faults}
	slot.seq.Add(1)
	for i, value := range values {
		slot.values[i].Store(value)
	}
	stages := snapshot.Stages
	stageValues := [...]uint64{stages.CheckpointApplied, stages.Checkpoints, stages.PhysicalCheckpoints,
		stages.CheckpointBarrierSyncs, stages.WALLiveBytes, stages.WALEntries, stages.WALSyncs,
		stages.BackupRequests, stages.BackupFaults, stages.BackupLogicalBytes, stages.BackupScanBytes,
		stages.SnapshotTransferChunks, stages.SnapshotTransferBytes, stages.SnapshotResidentBytes,
		stages.ReplicaActionRequests, stages.ReplicaActionCompletions, stages.ReplicaActionFaults}
	for i, value := range stageValues {
		slot.stages[i].Store(value)
	}
	slot.seq.Add(1)
	return nil
}

// RunRefresh refreshes the immutable directory with a fixed worker set. Slow
// nodes consume only their worker and the configured shard-control deadline;
// no goroutine is created per sample or interval.
func (metrics *DistributedMetrics) RunRefresh(ctx context.Context, interval time.Duration, concurrency int) error {
	if metrics == nil || ctx == nil || interval <= 0 || concurrency <= 0 || concurrency > len(metrics.slots) {
		return ErrDistributedMetrics
	}
	type job struct {
		index int
		done  *sync.WaitGroup
	}
	jobs := make(chan job, concurrency)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for range concurrency {
		go func() {
			defer workers.Done()
			for work := range jobs {
				_ = metrics.RefreshOne(ctx, work.index)
				work.done.Done()
			}
		}()
	}
	defer func() { close(jobs); workers.Wait() }()
	refresh := func() bool {
		var done sync.WaitGroup
		done.Add(len(metrics.slots))
		for index := range metrics.slots {
			select {
			case jobs <- job{index: index, done: &done}:
			case <-ctx.Done():
				// Balance work that was never submitted before joining submitted
				// jobs; workers observe the same cancellation through RefreshOne.
				for unsent := index; unsent < len(metrics.slots); unsent++ {
					done.Done()
				}
				done.Wait()
				return false
			}
		}
		done.Wait()
		return true
	}
	if !refresh() {
		return context.Cause(ctx)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !refresh() {
				return context.Cause(ctx)
			}
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// SnapshotInto copies the current cache into caller-owned memory and computes
// a saturating aggregate. It allocates nothing and performs bounded work.
func (metrics *DistributedMetrics) SnapshotInto(dst []DistributedMetricsSample) ([]DistributedMetricsSample, DistributedMetricsAggregate, error) {
	if metrics == nil || cap(dst) < len(metrics.slots) {
		return dst[:0], DistributedMetricsAggregate{}, ErrDistributedMetrics
	}
	dst = dst[:len(metrics.slots)]
	aggregate := DistributedMetricsAggregate{Samples: uint64(len(dst))}
	for index := range metrics.slots {
		sample, values, stageValues, err := metrics.snapshotAt(index)
		if err != nil {
			return dst[:index], aggregate, ErrDistributedMetrics
		}
		dst[index] = sample
		aggregate.Reads, aggregate.Overflow = saturatingAdd(aggregate.Reads, sample.Reads, aggregate.Overflow)
		aggregate.Faults, aggregate.Overflow = saturatingAdd(aggregate.Faults, sample.Faults, aggregate.Overflow)
		if sample.NodeAggregate {
			stageFields := []*uint64{&aggregate.Stages.CheckpointApplied, &aggregate.Stages.Checkpoints, &aggregate.Stages.PhysicalCheckpoints,
				&aggregate.Stages.CheckpointBarrierSyncs, &aggregate.Stages.WALLiveBytes, &aggregate.Stages.WALEntries, &aggregate.Stages.WALSyncs,
				&aggregate.Stages.BackupRequests, &aggregate.Stages.BackupFaults, &aggregate.Stages.BackupLogicalBytes, &aggregate.Stages.BackupScanBytes,
				&aggregate.Stages.SnapshotTransferChunks, &aggregate.Stages.SnapshotTransferBytes, &aggregate.Stages.SnapshotResidentBytes,
				&aggregate.Stages.ReplicaActionRequests, &aggregate.Stages.ReplicaActionCompletions, &aggregate.Stages.ReplicaActionFaults}
			for field, value := range stageValues {
				*stageFields[field], aggregate.Overflow = saturatingAdd(*stageFields[field], value, aggregate.Overflow)
			}
			continue
		}
		fields := []*uint64{&aggregate.Cut.ProposalCommands, &aggregate.Cut.ProposalBytes,
			&aggregate.Cut.AppliedEntries, &aggregate.Cut.ReadyPersisted, &aggregate.Cut.SnapshotsFinished,
			&aggregate.Cut.ReadCompletions, &aggregate.Cut.Faults}
		for field, value := range values {
			*fields[field], aggregate.Overflow = saturatingAdd(*fields[field], value, aggregate.Overflow)
		}
	}
	return dst, aggregate, nil
}

// SnapshotAt exposes one stable cached sample without allocation.
func (metrics *DistributedMetrics) SnapshotAt(index int) (DistributedMetricsSample, error) {
	sample, _, _, err := metrics.snapshotAt(index)
	return sample, err
}

// Aggregate returns the saturating cluster cut without allocating sample
// storage. It intentionally reuses SnapshotInto's exact aggregation contract.
func (metrics *DistributedMetrics) Aggregate() (DistributedMetricsAggregate, error) {
	if metrics == nil {
		return DistributedMetricsAggregate{}, ErrDistributedMetrics
	}
	aggregate := DistributedMetricsAggregate{Samples: uint64(len(metrics.slots))}
	for index := range metrics.slots {
		sample, values, stageValues, err := metrics.snapshotAt(index)
		if err != nil {
			return DistributedMetricsAggregate{}, err
		}
		aggregate.Reads, aggregate.Overflow = saturatingAdd(aggregate.Reads, sample.Reads, aggregate.Overflow)
		aggregate.Faults, aggregate.Overflow = saturatingAdd(aggregate.Faults, sample.Faults, aggregate.Overflow)
		if sample.NodeAggregate {
			fields := []*uint64{&aggregate.Stages.CheckpointApplied, &aggregate.Stages.Checkpoints, &aggregate.Stages.PhysicalCheckpoints,
				&aggregate.Stages.CheckpointBarrierSyncs, &aggregate.Stages.WALLiveBytes, &aggregate.Stages.WALEntries, &aggregate.Stages.WALSyncs,
				&aggregate.Stages.BackupRequests, &aggregate.Stages.BackupFaults, &aggregate.Stages.BackupLogicalBytes, &aggregate.Stages.BackupScanBytes,
				&aggregate.Stages.SnapshotTransferChunks, &aggregate.Stages.SnapshotTransferBytes, &aggregate.Stages.SnapshotResidentBytes,
				&aggregate.Stages.ReplicaActionRequests, &aggregate.Stages.ReplicaActionCompletions, &aggregate.Stages.ReplicaActionFaults}
			for field, value := range stageValues {
				*fields[field], aggregate.Overflow = saturatingAdd(*fields[field], value, aggregate.Overflow)
			}
			continue
		}
		fields := []*uint64{&aggregate.Cut.ProposalCommands, &aggregate.Cut.ProposalBytes, &aggregate.Cut.AppliedEntries,
			&aggregate.Cut.ReadyPersisted, &aggregate.Cut.SnapshotsFinished, &aggregate.Cut.ReadCompletions, &aggregate.Cut.Faults}
		for field, value := range values {
			*fields[field], aggregate.Overflow = saturatingAdd(*fields[field], value, aggregate.Overflow)
		}
	}
	return aggregate, nil
}

func (metrics *DistributedMetrics) snapshotAt(index int) (DistributedMetricsSample, [7]uint64, [17]uint64, error) {
	if metrics == nil || index < 0 || index >= len(metrics.slots) {
		return DistributedMetricsSample{}, [7]uint64{}, [17]uint64{}, ErrDistributedMetrics
	}
	slot := &metrics.slots[index]
	var values [7]uint64
	var stages [17]uint64
	for range 4 {
		before := slot.seq.Load()
		if before&1 != 0 {
			continue
		}
		for field := range values {
			values[field] = slot.values[field].Load()
		}
		for field := range stages {
			stages[field] = slot.stages[field].Load()
		}
		if slot.seq.Load() != before {
			continue
		}
		return DistributedMetricsSample{Group: slot.group, Member: slot.member, Node: slot.node,
			NodeAggregate: slot.nodeAggregate, Reads: slot.reads.Load(), Faults: slot.faults.Load(),
			Cut:    raftservice.ProgressMetricsSnapshot{ProposalCommands: values[0], ProposalBytes: values[1], AppliedEntries: values[2], ReadyPersisted: values[3], SnapshotsFinished: values[4], ReadCompletions: values[5], Faults: values[6]},
			Stages: servicemetrics.StageMetricsSnapshot{CheckpointApplied: stages[0], Checkpoints: stages[1], PhysicalCheckpoints: stages[2], CheckpointBarrierSyncs: stages[3], WALLiveBytes: stages[4], WALEntries: stages[5], WALSyncs: stages[6], BackupRequests: stages[7], BackupFaults: stages[8], BackupLogicalBytes: stages[9], BackupScanBytes: stages[10], SnapshotTransferChunks: stages[11], SnapshotTransferBytes: stages[12], SnapshotResidentBytes: stages[13], ReplicaActionRequests: stages[14], ReplicaActionCompletions: stages[15], ReplicaActionFaults: stages[16]},
		}, values, stages, nil
	}
	return DistributedMetricsSample{}, values, stages, ErrDistributedMetrics
}

func saturatingAdd(left, right uint64, overflow bool) (uint64, bool) {
	if right > math.MaxUint64-left {
		return math.MaxUint64, true
	}
	return left + right, overflow
}
