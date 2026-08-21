package autosplit

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
)

const (
	DefaultRecorderLanes = 8
	MaxRecorderLanes     = 32
)

var ErrInvalidRecorder = errors.New("autosplit: invalid recorder")

type recorderLane struct {
	mu     sync.Mutex
	sketch Sketch
}

// Recorder is a bounded striped concurrent telemetry window. The routed hot
// path pays one relaxed lane ticket plus one normally uncontended small lock;
// all sketch storage is allocated at construction and never grows with bucket
// count, tenant count, or request count.
type Recorder struct {
	source SourceIdentity
	lanes  []recorderLane
	next   atomic.Uint32
	rotate sync.Mutex
}

const (
	recorderHeaderBytes = unsafe.Sizeof(Recorder{})
	recorderLaneBytes   = unsafe.Sizeof(recorderLane{})
)

// NewRecorder constructs one exact observation window. lanes==0 selects the
// finite default; callers may tune contention against the fixed per-shard
// memory footprint.
func NewRecorder(source SourceIdentity, sequence uint64, lanes int) (*Recorder, error) {
	if !source.valid() || !bucketAlignedSource(source) ||
		sequence == 0 || lanes < 0 || lanes > MaxRecorderLanes {
		return nil, ErrInvalidRecorder
	}
	if lanes == 0 {
		lanes = DefaultRecorderLanes
	}
	recorder := &Recorder{source: source, lanes: make([]recorderLane, lanes)}
	for i := range recorder.lanes {
		recorder.lanes[i].sketch = Sketch{source: source, sequence: sequence}
	}
	return recorder, nil
}

func bucketAlignedSource(source SourceIdentity) bool {
	return distribution.VirtualBucketBoundary(source.Range.Start, source.BucketBits) &&
		(source.Range.End.Max ||
			distribution.VirtualBucketBoundary(source.Range.End.Point, source.BucketBits))
}

// Source returns the immutable ownership identity this recorder observes.
func (r *Recorder) Source() SourceIdentity {
	if r == nil {
		return SourceIdentity{}
	}
	return r.source
}

// RetainedBytes reports the fixed Recorder header and striped lane backing.
// It excludes only allocator metadata.
func (r *Recorder) RetainedBytes() uint64 {
	if r == nil {
		return 0
	}
	return uint64(recorderHeaderBytes) +
		uint64(len(r.lanes))*uint64(recorderLaneBytes)
}

// ObservePoint records one routed point. Inputs are borrowed for the call.
func (r *Recorder) ObservePoint(
	point distribution.KeyspacePoint,
	load LoadVector,
	hotWeight uint32,
) bool {
	lane := r.acquireLane()
	if lane == nil {
		return false
	}
	ok := lane.sketch.ObservePoint(point, load, hotWeight)
	lane.mu.Unlock()
	return ok
}

// ObserveBucket records resource, hotness, and fan-out evidence for one exact
// canonical virtual-bucket start under a single stripe lock.
func (r *Recorder) ObserveBucket(
	start distribution.KeyspacePoint,
	load LoadVector,
	hotWeight uint32,
	fanoutWeight uint32,
) bool {
	if r == nil || fanoutWeight == 0 {
		return false
	}
	lane := r.acquireLane()
	if lane == nil {
		return false
	}
	ok := lane.sketch.observeBucket(start, load, hotWeight, fanoutWeight)
	lane.mu.Unlock()
	return ok
}

// ObserveSpan records fan-out evidence for one bounded range.
func (r *Recorder) ObserveSpan(
	start distribution.KeyspacePoint,
	end distribution.KeyspaceEnd,
	weight uint32,
) bool {
	lane := r.acquireLane()
	if lane == nil {
		return false
	}
	ok := lane.sketch.ObserveSpan(start, end, weight)
	lane.mu.Unlock()
	return ok
}

// ObserveUnbounded records scatter fan-out evidence.
func (r *Recorder) ObserveUnbounded(weight uint32) {
	lane := r.acquireLane()
	if lane == nil {
		return
	}
	lane.sketch.ObserveUnbounded(weight)
	lane.mu.Unlock()
}

// ObserveUniform records resource pressure whose exact key contribution is not
// known. The planner projects its exact aggregate proportionally across range
// candidates without manufacturing a false hot bucket.
func (r *Recorder) ObserveUniform(load LoadVector) bool {
	lane := r.acquireLane()
	if lane == nil {
		return false
	}
	lane.sketch.observeUniform(load)
	lane.mu.Unlock()
	return true
}

// ObserveUnknown records exact resource totals plus unbounded fan-out when no
// trustworthy bucket geometry is available, under one stripe lock.
func (r *Recorder) ObserveUnknown(load LoadVector, fanoutWeight uint32) bool {
	lane := r.acquireLane()
	if lane == nil {
		return false
	}
	lane.sketch.observeUniform(load)
	lane.sketch.ObserveUnbounded(fanoutWeight)
	lane.mu.Unlock()
	return true
}

func (r *Recorder) acquireLane() *recorderLane {
	if r == nil || len(r.lanes) == 0 {
		return nil
	}
	ordinal := r.next.Add(1) - 1
	lane := &r.lanes[int(ordinal%uint32(len(r.lanes)))]
	lane.mu.Lock()
	return lane
}

// Rotate closes the current exact window and starts nextSequence. All stripes
// are locked in stable order for one bounded merge, so no observation can land
// partly in two windows. The returned sketch is detached and immutable once
// handed to the controller.
func (r *Recorder) Rotate(nextSequence uint64) (*Sketch, error) {
	if r == nil || len(r.lanes) == 0 || nextSequence == 0 {
		return nil, ErrInvalidRecorder
	}
	r.rotate.Lock()
	defer r.rotate.Unlock()
	for i := range r.lanes {
		r.lanes[i].mu.Lock()
	}
	defer func() {
		for i := len(r.lanes) - 1; i >= 0; i-- {
			r.lanes[i].mu.Unlock()
		}
	}()
	sequence := r.lanes[0].sketch.sequence
	if nextSequence <= sequence {
		return nil, ErrInvalidSequence
	}
	out := &Sketch{source: r.source, sequence: sequence}
	for i := range r.lanes {
		mergeSketch(out, &r.lanes[i].sketch)
		r.lanes[i].sketch = Sketch{source: r.source, sequence: nextSequence}
	}
	return out, nil
}

func mergeSketch(dst, src *Sketch) {
	if dst == nil || src == nil || dst.source != src.source || dst.sequence != src.sequence {
		if dst != nil {
			dst.overflow = true
		}
		return
	}
	for resource := range ResourceCount {
		dst.total[resource] = saturatingAdd64(dst.total[resource], src.total[resource], &dst.overflow)
		dst.uniform[resource] = saturatingAdd64(
			dst.uniform[resource], src.uniform[resource], &dst.overflow,
		)
		for bin := range BinCount {
			dst.bins[bin][resource] = saturatingAdd32(
				dst.bins[bin][resource], src.bins[bin][resource], &dst.overflow,
			)
		}
	}
	for i := range dst.cross {
		value, ok := addSigned(dst.cross[i], src.cross[i])
		if !ok {
			dst.overflow = true
		} else {
			dst.cross[i] = value
		}
	}
	dst.hotTotal = saturatingAdd64(dst.hotTotal, src.hotTotal, &dst.overflow)
	dst.bounded = saturatingAdd64(dst.bounded, src.bounded, &dst.overflow)
	dst.unbounded = saturatingAdd64(dst.unbounded, src.unbounded, &dst.overflow)
	dst.samples = saturatingAdd64(dst.samples, src.samples, &dst.overflow)
	dst.overflow = dst.overflow || src.overflow
	for i := range src.heavy {
		if src.heavy[i].used {
			mergeHeavy(&dst.heavy, src.heavy[i], &dst.overflow)
		}
	}
}

func mergeHeavy(dst *[HeavyHitterCount]heavyHitter, candidate heavyHitter, overflow *bool) {
	for i := range dst {
		if dst[i].used && dst[i].point == candidate.point {
			dst[i].estimate = saturatingAdd64(dst[i].estimate, candidate.estimate, overflow)
			dst[i].error = saturatingAdd64(dst[i].error, candidate.error, overflow)
			for resource := range ResourceCount {
				dst[i].load[resource] = saturatingAdd64(
					dst[i].load[resource], candidate.load[resource], overflow,
				)
			}
			return
		}
	}
	for i := range dst {
		if !dst[i].used {
			dst[i] = candidate
			return
		}
	}
	victim := 0
	for i := 1; i < len(dst); i++ {
		if dst[i].estimate < dst[victim].estimate ||
			(dst[i].estimate == dst[victim].estimate &&
				distribution.ComparePoints(dst[i].point, dst[victim].point) > 0) {
			victim = i
		}
	}
	base := dst[victim].estimate
	candidate.estimate = saturatingAdd64(base, candidate.estimate, overflow)
	candidate.error = saturatingAdd64(base, candidate.error, overflow)
	dst[victim] = candidate
}
