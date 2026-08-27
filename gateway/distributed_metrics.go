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
	Group  raftmember.GroupKey
	Member uint64
	Node   rafttransport.NodeID
	Cut    raftservice.ProgressMetricsSnapshot
	Reads  uint64
	Faults uint64
}

type DistributedMetricsAggregate struct {
	Cut      raftservice.ProgressMetricsSnapshot
	Samples  uint64
	Reads    uint64
	Faults   uint64
	Overflow bool
}

type distributedMetricsSlot struct {
	group  raftmember.GroupKey
	member uint64
	node   rafttransport.NodeID
	seq    atomic.Uint64
	values [7]atomic.Uint64
	reads  atomic.Uint64
	faults atomic.Uint64
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
	metrics := &DistributedMetrics{opener: opener, slots: make([]distributedMetricsSlot, count)}
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
	snapshot, err := (servicemetrics.Client{Open: func(openCtx context.Context) (rafttransport.PeerConnection, error) {
		return metrics.opener.OpenShardControl(openCtx, slot.node)
	}}).ReadGroup(ctx, slot.group)
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
		slot := &metrics.slots[index]
		values := [7]uint64{}
		stable := false
		for range 4 {
			before := slot.seq.Load()
			if before&1 != 0 {
				continue
			}
			for field := range values {
				values[field] = slot.values[field].Load()
			}
			stable = slot.seq.Load() == before
			if stable {
				break
			}
		}
		if !stable {
			return dst[:index], aggregate, ErrDistributedMetrics
		}
		sample := &dst[index]
		sample.Group, sample.Member, sample.Node = slot.group, slot.member, slot.node
		sample.Cut = raftservice.ProgressMetricsSnapshot{ProposalCommands: values[0], ProposalBytes: values[1],
			AppliedEntries: values[2], ReadyPersisted: values[3], SnapshotsFinished: values[4],
			ReadCompletions: values[5], Faults: values[6]}
		sample.Reads, sample.Faults = slot.reads.Load(), slot.faults.Load()
		aggregate.Reads, aggregate.Overflow = saturatingAdd(aggregate.Reads, sample.Reads, aggregate.Overflow)
		aggregate.Faults, aggregate.Overflow = saturatingAdd(aggregate.Faults, sample.Faults, aggregate.Overflow)
		fields := []*uint64{&aggregate.Cut.ProposalCommands, &aggregate.Cut.ProposalBytes,
			&aggregate.Cut.AppliedEntries, &aggregate.Cut.ReadyPersisted, &aggregate.Cut.SnapshotsFinished,
			&aggregate.Cut.ReadCompletions, &aggregate.Cut.Faults}
		for field, value := range values {
			*fields[field], aggregate.Overflow = saturatingAdd(*fields[field], value, aggregate.Overflow)
		}
	}
	return dst, aggregate, nil
}

func saturatingAdd(left, right uint64, overflow bool) (uint64, bool) {
	if right > math.MaxUint64-left {
		return math.MaxUint64, true
	}
	return left + right, overflow
}
