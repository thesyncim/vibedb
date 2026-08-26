package autosplit

import (
	"math/bits"

	"github.com/thesyncim/vibedb/distribution"
)

// TrackerPolicy controls sustained-evidence admission. WindowCount is bounded
// to 64; RequiredWindows qualifies that many of the last WindowCount windows.
type TrackerPolicy struct {
	WindowCount        uint8
	RequiredWindows    uint8
	CooldownWindows    uint16
	MaxBoundaryDrift   uint8
	TriggerPressurePPM uint64
}

// DefaultTrackerPolicy returns an asymmetric anti-flap policy: six qualifying
// windows out of eight, followed by an eight-window cooldown.
func DefaultTrackerPolicy() TrackerPolicy {
	return TrackerPolicy{
		WindowCount: 8, RequiredWindows: 6, CooldownWindows: 8,
		MaxBoundaryDrift: 1, TriggerPressurePPM: 900_000,
	}
}

// Tracker is the fixed-space sustained-hotness state for one source shard.
// Observe advances exactly one evidence window and reports whether a
// recommendation has qualified now.
type Tracker struct {
	source       SourceIdentity
	lastSequence uint64

	history uint64
	fast    uint64
	slow    uint64

	cooldown     uint16
	seen         uint8
	anchorBin    uint8
	lastKind     RecommendationKind
	stable       bool
	anchorBucket distribution.KeyspacePoint
}

// TrackerCheckpoint is the complete fixed-size restart image of one sustained
// hotness tracker. It contains only logical evidence-window state: wall-clock
// time never enters split qualification or cooldown decisions.
//
// The checkpoint is intentionally a value rather than an encoded document.
// The replicated controller that owns it remains responsible for binding the
// value to its catalog generation and durable authority revision.
type TrackerCheckpoint struct {
	Source       SourceIdentity
	LastSequence uint64
	History      uint64
	Fast         uint64
	Slow         uint64
	Cooldown     uint16
	Seen         uint8
	AnchorBin    uint8
	LastKind     RecommendationKind
	Stable       bool
	AnchorBucket distribution.KeyspacePoint
}

// Checkpoint returns a detached, allocation-free restart image.
func (t *Tracker) Checkpoint() TrackerCheckpoint {
	if t == nil {
		return TrackerCheckpoint{}
	}
	return TrackerCheckpoint{
		Source: t.source, LastSequence: t.lastSequence,
		History: t.history, Fast: t.fast, Slow: t.slow,
		Cooldown: t.cooldown, Seen: t.seen, AnchorBin: t.anchorBin,
		LastKind: t.lastKind, Stable: t.stable, AnchorBucket: t.anchorBucket,
	}
}

// RestoreTracker validates and restores one checkpoint. Empty checkpoints are
// legal and restore the zero tracker. Malformed history, anchors, or source
// identities fail closed instead of manufacturing hotness evidence.
func RestoreTracker(checkpoint TrackerCheckpoint) (Tracker, bool) {
	if checkpoint == (TrackerCheckpoint{}) {
		return Tracker{}, true
	}
	if !checkpoint.Source.valid() || checkpoint.LastSequence == 0 ||
		checkpoint.Seen > 64 || checkpoint.LastKind > RecommendationUnsplittableBucket ||
		(checkpoint.Stable && checkpoint.LastKind != RecommendationBinarySplit &&
			checkpoint.LastKind != RecommendationIsolateBucket) ||
		(!checkpoint.Stable && (checkpoint.AnchorBin != 0 ||
			checkpoint.AnchorBucket != (distribution.KeyspacePoint{}) ||
			checkpoint.LastKind != RecommendationNone)) {
		return Tracker{}, false
	}
	if checkpoint.Seen < 64 && checkpoint.History>>checkpoint.Seen != 0 {
		return Tracker{}, false
	}
	return Tracker{
		source: checkpoint.Source, lastSequence: checkpoint.LastSequence,
		history: checkpoint.History, fast: checkpoint.Fast, slow: checkpoint.Slow,
		cooldown: checkpoint.Cooldown, seen: checkpoint.Seen,
		anchorBin: checkpoint.AnchorBin, lastKind: checkpoint.LastKind,
		stable: checkpoint.Stable, anchorBucket: checkpoint.AnchorBucket,
	}, true
}

// Observe consumes one recommendation window. It applies fast (1/2) and slow
// (1/8) fixed-point EWMAs, boundary stability, N-of-M evidence, and cooldown.
func (t *Tracker) Observe(rec Recommendation, policy TrackerPolicy) bool {
	if t == nil {
		return false
	}
	policy = normalizeTrackerPolicy(policy)
	if rec.WindowSequence == 0 || !rec.Source.valid() {
		return false
	}
	if t.source != rec.Source {
		// Never combine evidence across a routing generation, ownership epoch,
		// allocation, range, shard, or distribution. Adopt the new source as a
		// fresh window; its sequence belongs to a distinct incarnation.
		*t = Tracker{source: rec.Source}
	} else if t.lastSequence != 0 {
		if rec.WindowSequence <= t.lastSequence {
			// Replayed and regressed evidence cannot advance time, cool down, or
			// alter the current qualification window.
			return false
		}
		if rec.WindowSequence != t.lastSequence+1 {
			// Sustained evidence means contiguous windows. A gap starts a new run,
			// while retaining any active anti-flap cooldown conservatively.
			t.resetEvidence()
		}
	}
	t.lastSequence = rec.WindowSequence

	qualifies := actionableRecommendation(rec)
	if qualifies && t.stable && !t.sameTarget(rec, policy) {
		// Drift is bounded against the first candidate in the sustained run, not
		// merely the preceding window, so a boundary cannot walk cumulatively.
		t.resetEvidence()
	}
	pressure := rec.CurrentPressurePPM
	if t.seen == 0 {
		t.fast, t.slow = pressure, pressure
	} else {
		t.fast = average(t.fast, pressure)
		t.slow = saturatingAdd(saturatingMul(t.slow, 7), pressure) / 8
	}

	if qualifies {
		if !t.stable {
			t.anchorBin = rec.CandidateBin
			t.anchorBucket = rec.HotBucketStart
			t.lastKind, t.stable = rec.Kind, true
		}
	}

	mask := uint64(1)<<policy.WindowCount - 1
	t.history = (t.history << 1) & mask
	if qualifies {
		t.history |= 1
	} else if t.history == 0 {
		t.clearAnchor()
	}
	if t.seen < policy.WindowCount {
		t.seen++
	}
	if t.cooldown != 0 {
		t.cooldown--
		return false
	}
	if t.seen < policy.WindowCount ||
		uint8(bits.OnesCount64(t.history)) < policy.RequiredWindows ||
		t.fast < policy.TriggerPressurePPM || t.slow < policy.TriggerPressurePPM {
		return false
	}
	t.cooldown = policy.CooldownWindows
	t.history, t.seen = 0, 0
	return true
}

func (t *Tracker) resetEvidence() {
	t.history = 0
	t.fast = 0
	t.slow = 0
	t.seen = 0
	t.clearAnchor()
}

func (t *Tracker) clearAnchor() {
	t.anchorBin = 0
	t.anchorBucket = distribution.KeyspacePoint{}
	t.lastKind = RecommendationNone
	t.stable = false
}

func (t *Tracker) sameTarget(rec Recommendation, policy TrackerPolicy) bool {
	if rec.Kind != t.lastKind {
		return false
	}
	switch rec.Kind {
	case RecommendationBinarySplit:
		return distance(rec.CandidateBin, t.anchorBin) <= policy.MaxBoundaryDrift
	case RecommendationIsolateBucket:
		return rec.HotBucketStart == t.anchorBucket
	default:
		return false
	}
}

func actionableRecommendation(rec Recommendation) bool {
	if rec.Reason != ReasonNone || rec.BenefitPPM == 0 {
		return false
	}
	switch rec.Kind {
	case RecommendationBinarySplit:
		return rec.BoundaryCount == 1 && rec.CandidateBin > 0 &&
			rec.CandidateBin < BinCount &&
			rec.Boundaries[0] == boundaryForRange(rec.Source.Range, int(rec.CandidateBin))
	case RecommendationIsolateBucket:
		if !rec.Source.Range.Contains(rec.HotBucketStart) ||
			!distribution.VirtualBucketBoundary(rec.HotBucketStart, rec.Source.BucketBits) ||
			rec.CandidateBin >= BinCount ||
			int(rec.CandidateBin) != binForRange(rec.Source.Range, rec.HotBucketStart) {
			return false
		}
		var boundaries [2]distribution.KeyspacePoint
		count := isolationBoundaries(
			rec.Source.Range, rec.Source.BucketBits, rec.HotBucketStart, &boundaries,
		)
		return count != 0 && rec.BoundaryCount == count && rec.Boundaries == boundaries
	default:
		return false
	}
}

// Pressures reports the current fast and slow fixed-point EWMAs.
func (t *Tracker) Pressures() (fast, slow uint64) {
	if t == nil {
		return 0, 0
	}
	return t.fast, t.slow
}

func normalizeTrackerPolicy(policy TrackerPolicy) TrackerPolicy {
	if policy.WindowCount == 0 || policy.WindowCount > 64 {
		policy.WindowCount = 8
	}
	if policy.RequiredWindows == 0 || policy.RequiredWindows > policy.WindowCount {
		policy.RequiredWindows = policy.WindowCount
	}
	return policy
}

func distance(a, b uint8) uint8 {
	if a >= b {
		return a - b
	}
	return b - a
}

func average(a, b uint64) uint64 { return (a & b) + ((a ^ b) >> 1) }

func saturatingMul(value, multiplier uint64) uint64 {
	if multiplier != 0 && value > ^uint64(0)/multiplier {
		return ^uint64(0)
	}
	return value * multiplier
}

func saturatingAdd(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}
