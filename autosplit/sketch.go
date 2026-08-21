// Package autosplit contains SABLE, a bounded sustained adaptive bottleneck
// and locality evidence recorder, recommender, and generation-fenced desired
// split planner. It never publishes manifests, moves data, or changes shard
// ownership.
package autosplit

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"

	"github.com/thesyncim/vibedb/distribution"
)

const (
	// BinCount fixes both recommendation cost and retained sketch space.
	BinCount = 64
	// HeavyHitterCount is the fixed SpaceSaving candidate capacity.
	HeavyHitterCount = 8
)

// Resource is one independently capacity-normalized load dimension.
type Resource uint8

const (
	ResourceWriteCPU Resource = iota
	ResourceReadCPU
	ResourceScanCPU
	ResourceIO
	ResourceRequests
	ResourceLatencyDebt
	ResourceLiveBytes
	ResourceCount
)

// LoadVector holds quantized load for one observation. Units are selected by
// the collector and must match the CapacityVector supplied to Recommend.
type LoadVector [ResourceCount]uint32

// CapacityVector is one window's available capacity in the same units as a
// LoadVector. A zero capacity rejects non-zero load on that resource.
type CapacityVector [ResourceCount]uint64

// Pulse is the compact first-stage signal sent for every shard. Controllers
// request or retain the detailed Sketch only for candidate shards. Sequence is
// supplied by the collector and must be monotonic within its incarnation.
type Pulse struct {
	Source    SourceIdentity
	Sequence  uint64
	Total     [ResourceCount]uint64
	Samples   uint64
	Bounded   uint64
	Unbounded uint64
	Overflow  bool
}

// ErrInvalidSource reports an incomplete identity or invalid range that cannot
// fence a sketch to one exact ownership incarnation.
var ErrInvalidSource = errors.New("autosplit: invalid source identity")

// ErrInvalidSequence reports a zero observation-window sequence. Sequences are
// positive and contiguous within one exact source incarnation.
var ErrInvalidSequence = errors.New("autosplit: invalid window sequence")

type loadBin [ResourceCount]uint32

type heavyHitter struct {
	point    distribution.KeyspacePoint
	load     [ResourceCount]uint64
	estimate uint64
	error    uint64
	used     bool
}

// Sketch is a fixed-space, single-window range sketch. It is intentionally
// not concurrency-safe: use one per collector lane and combine observations
// before handing an immutable window to the controller.
//
// Point load is kept in 64 equal subranges of the source range. Span crossings
// use a difference vector, so observing a bounded range costs O(log BinCount)
// and querying all candidate boundaries costs O(BinCount). Eight deterministic
// SpaceSaving entries retain exact hot-bucket candidates.
type Sketch struct {
	source   SourceIdentity
	sequence uint64
	bins     [BinCount]loadBin
	total    [ResourceCount]uint64
	uniform  [ResourceCount]uint64
	cross    [BinCount + 1]int64
	heavy    [HeavyHitterCount]heavyHitter

	hotTotal  uint64
	bounded   uint64
	unbounded uint64
	samples   uint64
	overflow  bool
}

// NewSketch returns an empty fixed-space sketch fenced to source's exact
// distribution, shard allocation, range, routing generation, ownership epoch,
// and positive observation-window sequence.
func NewSketch(source SourceIdentity, sequence uint64) (*Sketch, error) {
	if !source.valid() {
		return nil, ErrInvalidSource
	}
	if sequence == 0 {
		return nil, ErrInvalidSequence
	}
	return &Sketch{source: source, sequence: sequence}, nil
}

// Range reports the immutable source range represented by s.
func (s *Sketch) Range() distribution.KeyRange {
	if s == nil {
		return distribution.KeyRange{}
	}
	return s.source.Range
}

// Samples reports accepted point observations.
func (s *Sketch) Samples() uint64 {
	if s == nil {
		return 0
	}
	return s.samples
}

// Overflow reports whether any compact bin or span counter saturated. A
// planner refuses an overflowed window rather than treating clipped evidence
// as exact.
func (s *Sketch) Overflow() bool { return s != nil && s.overflow }

// Pulse returns a detached compact summary of this detailed window.
func (s *Sketch) Pulse() Pulse {
	if s == nil {
		return Pulse{}
	}
	return Pulse{
		Source: s.source, Sequence: s.sequence, Total: s.total,
		Samples: s.samples, Bounded: s.bounded,
		Unbounded: s.unbounded, Overflow: s.overflow,
	}
}

// ObservePoint adds one point's load and hotWeight. hotWeight is a caller-
// normalized dominant-load sample used only to discover celebrity points; it
// does not participate in resource accounting. The method returns false for a
// nil sketch or a point outside its source range.
func (s *Sketch) ObservePoint(point distribution.KeyspacePoint, load LoadVector, hotWeight uint32) bool {
	if s == nil || !s.source.Range.Contains(point) {
		return false
	}
	bin := s.binFor(point)
	for resource := range ResourceCount {
		value := load[resource]
		s.total[resource] = saturatingAdd64(s.total[resource], uint64(value), &s.overflow)
		s.bins[bin][resource] = saturatingAdd32(s.bins[bin][resource], value, &s.overflow)
	}
	s.samples = saturatingAdd64(s.samples, 1, &s.overflow)
	if hotWeight != 0 {
		bucket, ok := distribution.VirtualBucketForPoint(point, s.source.BucketBits)
		if !ok {
			return false
		}
		bucketRange, ok := distribution.VirtualBucketRange(bucket, s.source.BucketBits)
		if !ok {
			return false
		}
		// A virtual bucket is the smallest movable unit. Collapse every hot
		// point in it to the canonical bucket start so the controller never
		// recommends an unservable row-sized range.
		s.observeHeavy(bucketRange.Start, load, uint64(hotWeight))
	}
	return true
}

func (s *Sketch) observeBucket(
	start distribution.KeyspacePoint,
	load LoadVector,
	hotWeight uint32,
	fanoutWeight uint32,
) bool {
	if s == nil || fanoutWeight == 0 || !s.source.Range.Contains(start) ||
		!distribution.VirtualBucketBoundary(start, s.source.BucketBits) {
		return false
	}
	bin := s.binFor(start)
	for resource := range ResourceCount {
		value := load[resource]
		s.total[resource] = saturatingAdd64(s.total[resource], uint64(value), &s.overflow)
		s.bins[bin][resource] = saturatingAdd32(s.bins[bin][resource], value, &s.overflow)
	}
	s.samples = saturatingAdd64(s.samples, 1, &s.overflow)
	// A canonical bucket cannot cross a valid split boundary: any compact
	// boundary inside it is rejected as unaligned, while its endpoints route the
	// request wholly to one child. Retain the bounded-query denominator without
	// touching the crossing difference vector.
	s.bounded = saturatingAdd64(s.bounded, uint64(fanoutWeight), &s.overflow)
	if hotWeight != 0 {
		s.observeHeavy(start, load, uint64(hotWeight))
	}
	return true
}

// ObserveSpan records one bounded query span [start,end) for split fan-out
// costing. It clips the span to the source range and returns false when the
// span is malformed or does not overlap it. It does not add point load.
func (s *Sketch) ObserveSpan(start distribution.KeyspacePoint, end distribution.KeyspaceEnd, weight uint32) bool {
	if s == nil || weight == 0 || !validSpan(start, end) {
		return false
	}
	start = maxPoint(start, s.source.Range.Start)
	end = minEnd(end, s.source.Range.End)
	if !pointBelowEnd(start, end) {
		return false
	}
	s.bounded = saturatingAdd64(s.bounded, uint64(weight), &s.overflow)

	first := s.firstBoundaryAfter(start)
	stop := s.firstBoundaryAtOrAfterEnd(end)
	if first >= stop {
		return true
	}
	amount := int64(weight)
	if s.cross[first] > math.MaxInt64-amount || s.cross[stop] < math.MinInt64+amount {
		s.overflow = true
		return true
	}
	s.cross[first] += amount
	s.cross[stop] -= amount
	return true
}

// ObserveUnbounded records queries that will acquire one additional target for
// every split boundary. It is separate from point load because such queries
// cannot be attributed safely to a keyspace subrange.
func (s *Sketch) ObserveUnbounded(weight uint32) {
	if s == nil {
		return
	}
	s.unbounded = saturatingAdd64(s.unbounded, uint64(weight), &s.overflow)
}

func (s *Sketch) observeUniform(load LoadVector) {
	if s == nil {
		return
	}
	for resource := range ResourceCount {
		value := load[resource]
		s.total[resource] = saturatingAdd64(s.total[resource], uint64(value), &s.overflow)
		s.uniform[resource] = saturatingAdd64(s.uniform[resource], uint64(value), &s.overflow)
	}
	s.samples = saturatingAdd64(s.samples, 1, &s.overflow)
}

func (s *Sketch) observeHeavy(point distribution.KeyspacePoint, load LoadVector, weight uint64) {
	s.hotTotal = saturatingAdd64(s.hotTotal, weight, &s.overflow)
	for i := range s.heavy {
		h := &s.heavy[i]
		if h.used && h.point == point {
			h.estimate = saturatingAdd64(h.estimate, weight, &s.overflow)
			addHeavyLoad(h, load, &s.overflow)
			return
		}
	}
	for i := range s.heavy {
		if !s.heavy[i].used {
			s.heavy[i] = heavyHitter{point: point, estimate: weight, used: true}
			addHeavyLoad(&s.heavy[i], load, &s.overflow)
			return
		}
	}

	// Stable tie-breaking makes an identical observation stream byte-for-byte
	// reproducible: among equal minima, replace the lexically greatest point.
	victim := 0
	for i := 1; i < len(s.heavy); i++ {
		if s.heavy[i].estimate < s.heavy[victim].estimate ||
			(s.heavy[i].estimate == s.heavy[victim].estimate &&
				distribution.ComparePoints(s.heavy[i].point, s.heavy[victim].point) > 0) {
			victim = i
		}
	}
	base := s.heavy[victim].estimate
	s.heavy[victim] = heavyHitter{
		point: point, estimate: saturatingAdd64(base, weight, &s.overflow),
		error: base, used: true,
	}
	addHeavyLoad(&s.heavy[victim], load, &s.overflow)
}

func addHeavyLoad(h *heavyHitter, load LoadVector, overflow *bool) {
	for resource := range ResourceCount {
		h.load[resource] = saturatingAdd64(h.load[resource], uint64(load[resource]), overflow)
	}
}

func saturatingAdd32(a, b uint32, overflow *bool) uint32 {
	if math.MaxUint32-a < b {
		*overflow = true
		return math.MaxUint32
	}
	return a + b
}

func saturatingAdd64(a, b uint64, overflow *bool) uint64 {
	if math.MaxUint64-a < b {
		*overflow = true
		return math.MaxUint64
	}
	return a + b
}

func (s *Sketch) binFor(point distribution.KeyspacePoint) int {
	return binForRange(s.source.Range, point)
}

func binForRange(r distribution.KeyRange, point distribution.KeyspacePoint) int {
	start := pointUint64(r.Start)
	value := pointUint64(point)
	delta := value - start
	if r.End.Max && start == 0 {
		return int(value >> 58)
	}
	width := rangeWidth(r)
	hi, lo := bits.Mul64(delta, BinCount)
	q, _ := bits.Div64(hi, lo, width)
	if q >= BinCount {
		return BinCount - 1
	}
	return int(q)
}

// boundary returns the kth interior equal-width boundary, 1 <= k < BinCount.
func (s *Sketch) boundary(k int) distribution.KeyspacePoint {
	return boundaryForRange(s.source.Range, k)
}

func boundaryForRange(r distribution.KeyRange, k int) distribution.KeyspacePoint {
	start := pointUint64(r.Start)
	if r.End.Max && start == 0 {
		return uint64Point(uint64(k) << 58)
	}
	width := rangeWidth(r)
	hi, lo := bits.Mul64(width, uint64(k))
	offset := hi<<58 | lo>>6 // exact floor(product / 64)
	return uint64Point(start + offset)
}

func (s *Sketch) firstBoundaryAfter(point distribution.KeyspacePoint) int {
	lo, hi := 1, BinCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		if mid == BinCount || distribution.ComparePoints(s.boundary(mid), point) > 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (s *Sketch) firstBoundaryAtOrAfterEnd(end distribution.KeyspaceEnd) int {
	if end.Max {
		return BinCount
	}
	lo, hi := 1, BinCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		if mid == BinCount || distribution.ComparePoints(s.boundary(mid), end.Point) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func rangeWidth(r distribution.KeyRange) uint64 {
	start := pointUint64(r.Start)
	if r.End.Max {
		return -start
	}
	return pointUint64(r.End.Point) - start
}

func pointUint64(p distribution.KeyspacePoint) uint64 { return binary.BigEndian.Uint64(p[:]) }

func uint64Point(v uint64) distribution.KeyspacePoint {
	var p distribution.KeyspacePoint
	binary.BigEndian.PutUint64(p[:], v)
	return p
}

func validSpan(start distribution.KeyspacePoint, end distribution.KeyspaceEnd) bool {
	return end.Max || distribution.ComparePoints(start, end.Point) < 0
}

func pointBelowEnd(point distribution.KeyspacePoint, end distribution.KeyspaceEnd) bool {
	return end.Max || distribution.ComparePoints(point, end.Point) < 0
}

func maxPoint(a, b distribution.KeyspacePoint) distribution.KeyspacePoint {
	if distribution.ComparePoints(a, b) >= 0 {
		return a
	}
	return b
}

func minEnd(a, b distribution.KeyspaceEnd) distribution.KeyspaceEnd {
	switch {
	case a.Max:
		return b
	case b.Max:
		return a
	case distribution.ComparePoints(a.Point, b.Point) <= 0:
		return a
	default:
		return b
	}
}
