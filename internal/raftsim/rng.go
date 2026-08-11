package raftsim

// RNG is the simulator's versioned SplitMix64 choice source. Its algorithm is
// part of SimulatorVersion: changing it requires a simulator-version change so
// an old seed never silently denotes a different fault schedule.
type RNG struct {
	state uint64
}

// NewRNG returns a deterministic choice source. Every uint64 seed, including
// zero, identifies one exact stream.
func NewRNG(seed uint64) RNG {
	return RNG{state: seed}
}

// Uint64 returns the next word in the fixed SplitMix64 stream.
func (r *RNG) Uint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Choose returns an unbiased value in [0, n). It reports false when n is zero.
func (r *RNG) Choose(n uint64) (uint64, bool) {
	if n == 0 {
		return 0, false
	}
	// Values below threshold are the incomplete tail of the uint64 domain.
	// Rejecting them makes the modulo reduction exact without division-based
	// floating point or platform-dependent behavior.
	threshold := -n % n
	for {
		v := r.Uint64()
		if v >= threshold {
			return v % n, true
		}
	}
}
