package autosplit

import "math/bits"

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
// Observe advances exactly one evidence window and reports whether a shadow
// recommendation has qualified now.
type Tracker struct {
	source SourceIdentity

	history uint64
	fast    uint64
	slow    uint64

	cooldown uint16
	seen     uint8
	lastBin  uint8
	lastKind RecommendationKind
	stable   bool
}

// Observe consumes one recommendation window. It applies fast (1/2) and slow
// (1/8) fixed-point EWMAs, boundary stability, N-of-M evidence, and cooldown.
func (t *Tracker) Observe(rec Recommendation, policy TrackerPolicy) bool {
	if t == nil {
		return false
	}
	policy = normalizeTrackerPolicy(policy)
	if t.source != rec.Source {
		// Never combine evidence across a routing generation, ownership epoch,
		// range, shard, or distribution. Adopt the new source as a fresh window.
		*t = Tracker{source: rec.Source}
	}
	pressure := rec.CurrentPressurePPM
	if t.seen == 0 {
		t.fast, t.slow = pressure, pressure
	} else {
		t.fast = average(t.fast, pressure)
		t.slow = saturatingAdd(saturatingMul(t.slow, 7), pressure) / 8
	}

	qualifies := rec.Kind != RecommendationNone &&
		rec.Kind != RecommendationUnsplittableHotKey &&
		rec.BenefitPPM != 0
	if qualifies {
		if t.stable && (rec.Kind != t.lastKind || distance(rec.CandidateBin, t.lastBin) > policy.MaxBoundaryDrift) {
			t.history, t.seen = 0, 0
		}
		t.lastBin, t.lastKind, t.stable = rec.CandidateBin, rec.Kind, true
	}

	mask := uint64(1)<<policy.WindowCount - 1
	t.history = (t.history << 1) & mask
	if qualifies {
		t.history |= 1
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
