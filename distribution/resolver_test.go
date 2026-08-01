package distribution

// The fast resolvers are a binary search plus a forward walk; the only way to
// trust them at scale is to check them against an obviously-correct reference.
// This file defines that reference — a linear scan built solely from the
// exported geometry (KeyRange.Contains / KeyRange.Overlaps) — and a seeded
// differential test that generates hundreds of random valid manifests and
// hammers ResolvePoint / ResolveRange / ResolveDestinationSet against it at
// every range boundary (each Start, each End-1), at the zero and maximum
// points, and at random positions. The maximum point gets its own test because
// it is the one position that must resolve without any successor arithmetic.
//
// The random source is seeded from a recorded constant, never the wall clock,
// so a failure reproduces exactly.

import (
	"errors"
	"math"
	"math/rand"
	"slices"
	"testing"
)

// resolverPropertySeed seeds the differential generator. It is a fixed
// constant, logged by the test, so any counterexample is reproducible.
const resolverPropertySeed int64 = 0x0B1B5EED2026

// slowResolvePoint is the O(n) reference: a linear scan returning the id of the
// one shard whose range contains p. It uses only exported geometry, so a
// disagreement with ResolvePoint indicts the fast path, not shared code.
func slowResolvePoint(m *Manifest, p KeyspacePoint) (ShardID, bool) {
	for i := 0; i < m.ShardCount(); i++ {
		s, _ := m.ShardInfo(i)
		if s.Range.Contains(p) {
			return s.ID, true
		}
	}
	return "", false
}

// slowResolveRange is the O(n) reference for range resolution: every shard
// overlapping r, in keyspace order.
func slowResolveRange(m *Manifest, r KeyRange) ([]ShardID, bool) {
	if !r.Valid() {
		return nil, false
	}
	var out []ShardID
	for i := 0; i < m.ShardCount(); i++ {
		s, _ := m.ShardInfo(i)
		if s.Range.Overlaps(r) {
			out = append(out, s.ID)
		}
	}
	return out, true
}

// slowResolveDestinationSet is the O(n*d) reference: a shard is included once
// if it contains any point or overlaps any range, walked in keyspace order so
// the result is naturally sorted and deduplicated.
func slowResolveDestinationSet(m *Manifest, d DestinationSet) ([]ShardID, error) {
	for _, r := range d.Ranges {
		if !r.Valid() {
			return nil, &DestinationError{Reason: "range start is not below its end"}
		}
	}
	var out []ShardID
	for i := 0; i < m.ShardCount(); i++ {
		s, _ := m.ShardInfo(i)
		hit := false
		for _, p := range d.Points {
			if s.Range.Contains(p) {
				hit = true
				break
			}
		}
		if !hit {
			for _, r := range d.Ranges {
				if s.Range.Overlaps(r) {
					hit = true
					break
				}
			}
		}
		if hit {
			out = append(out, s.ID)
		}
	}
	return out, nil
}

// randomBounds returns n distinct non-zero uint64 boundaries in ascending
// order, the interior split points of a random manifest.
func randomBounds(rng *rand.Rand, n int) []uint64 {
	set := make(map[uint64]struct{}, n)
	for len(set) < n {
		if v := rng.Uint64(); v != 0 {
			set[v] = struct{}{}
		}
	}
	out := make([]uint64, 0, n)
	for v := range set {
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}

// randomRange returns a valid range: a concrete [Start, End) or, one time in
// four (and always when Start is the maximum point), a Max-ended range.
func randomRange(rng *rand.Rand) KeyRange {
	a, b := rng.Uint64(), rng.Uint64()
	if a > b {
		a, b = b, a
	}
	if a == math.MaxUint64 || rng.Intn(4) == 0 {
		return KeyRange{Start: pt(a), End: maxKeyEnd}
	}
	if a == b {
		b = a + 1 // a < MaxUint64 here, so no overflow
	}
	return KeyRange{Start: pt(a), End: kend(b)}
}

// boundaryProbes returns the points a resolver is most likely to get wrong:
// zero, the maximum, and for every shard its inclusive Start and its last
// contained point (End-1).
func boundaryProbes(m *Manifest) []KeyspacePoint {
	probes := []KeyspacePoint{pt(0), pt(math.MaxUint64)}
	for i := 0; i < m.ShardCount(); i++ {
		s, _ := m.ShardInfo(i)
		probes = append(probes, s.Range.Start, endMinusOne(s.Range.End))
	}
	return probes
}

// TestResolversMatchReference is the differential property test: across many
// randomized valid manifests, the fast binary-search resolvers must agree with
// the slow linear-scan reference on boundary points, random points, random
// ranges, and random destination sets.
func TestResolversMatchReference(t *testing.T) {
	rng := rand.New(rand.NewSource(resolverPropertySeed))
	t.Logf("resolver differential seed = %#x", resolverPropertySeed)

	const generations = 500
	for gen := 0; gen < generations; gen++ {
		shardCount := 1 + rng.Intn(32)
		m := manifestFromBounds(t, randomBounds(rng, shardCount-1)...)

		// Point resolution: boundary probes plus random positions.
		probes := boundaryProbes(m)
		for i := 0; i < 8; i++ {
			probes = append(probes, pt(rng.Uint64()))
		}
		for _, p := range probes {
			gotID, gotOK := m.ResolvePoint(p)
			wantID, wantOK := slowResolvePoint(m, p)
			if gotID != wantID || gotOK != wantOK {
				t.Fatalf("gen %d (%d shards): ResolvePoint(%#016x) = (%q,%v), reference (%q,%v)",
					gen, shardCount, ptVal(p), gotID, gotOK, wantID, wantOK)
			}
			if !gotOK {
				t.Fatalf("gen %d: ResolvePoint(%#016x) missed on a complete manifest", gen, ptVal(p))
			}
		}

		// Range resolution: random valid ranges.
		for i := 0; i < 16; i++ {
			r := randomRange(rng)
			gotIDs, gotOK := m.ResolveRange(r)
			wantIDs, wantOK := slowResolveRange(m, r)
			if gotOK != wantOK || !slices.Equal(gotIDs, wantIDs) {
				t.Fatalf("gen %d: ResolveRange(%+v) = (%v,%v), reference (%v,%v)",
					gen, r, gotIDs, gotOK, wantIDs, wantOK)
			}
		}

		// A malformed range is rejected identically by both paths.
		bad := KeyRange{Start: pt(2), End: kend(1)}
		if ids, ok := m.ResolveRange(bad); ok || ids != nil {
			t.Fatalf("gen %d: ResolveRange(malformed) = (%v,%v), want (nil,false)", gen, ids, ok)
		}

		// Destination-set resolution: random points and ranges unioned.
		d := DestinationSet{}
		for i := rng.Intn(5); i > 0; i-- {
			d.Points = append(d.Points, pt(rng.Uint64()))
		}
		for i := rng.Intn(4); i > 0; i-- {
			d.Ranges = append(d.Ranges, randomRange(rng))
		}
		gotIDs, gotErr := m.ResolveDestinationSet(d)
		wantIDs, wantErr := slowResolveDestinationSet(m, d)
		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("gen %d: ResolveDestinationSet error mismatch: %v vs %v", gen, gotErr, wantErr)
		}
		if gotErr == nil && !slices.Equal(gotIDs, wantIDs) {
			t.Fatalf("gen %d: ResolveDestinationSet = %v, reference %v", gen, gotIDs, wantIDs)
		}
	}
}

// TestMaximumPointHandling covers the one position with no successor: the
// maximum point 0xFFFFFFFFFFFFFFFF must resolve into the final Max-ended shard
// and into no other, a Max-ended top range must resolve to exactly that shard,
// and the reference must agree — proving End.Max semantics rather than
// overflow arithmetic carry the top of the keyspace.
func TestMaximumPointHandling(t *testing.T) {
	m := manifestFromBounds(t, hb(0x80), hb(0xc0), hb(0xe0), hb(0xf0))
	last, _ := m.ShardInfo(m.ShardCount() - 1)
	maxPoint := pt(math.MaxUint64)

	id, ok := m.ResolvePoint(maxPoint)
	if !ok {
		t.Fatal("ResolvePoint(max) missed; the maximum point must resolve")
	}
	if id != last.ID {
		t.Fatalf("ResolvePoint(max) = %q, want final shard %q", id, last.ID)
	}
	if sid, sok := slowResolvePoint(m, maxPoint); sid != id || sok != ok {
		t.Fatalf("reference disagrees on max: (%q,%v) vs (%q,%v)", sid, sok, id, ok)
	}

	// Only the final, Max-ended shard contains the maximum point.
	for i := 0; i < m.ShardCount(); i++ {
		s, _ := m.ShardInfo(i)
		wantContains := i == m.ShardCount()-1
		if got := s.Range.Contains(maxPoint); got != wantContains {
			t.Fatalf("shard %d Contains(max) = %v, want %v", i, got, wantContains)
		}
		if got := s.Range.End.Max; got != wantContains {
			t.Fatalf("shard %d End.Max = %v, want %v", i, got, wantContains)
		}
	}

	// A Max-ended range over the top shard resolves to exactly that shard.
	top := KeyRange{Start: pt(hb(0xf0)), End: maxKeyEnd}
	ids, ok := m.ResolveRange(top)
	if !ok || !slices.Equal(ids, []ShardID{last.ID}) {
		t.Fatalf("ResolveRange(top) = (%v,%v), want ([%q], true)", ids, ok, last.ID)
	}

	// The zero point lands in the first shard.
	first, _ := m.ShardInfo(0)
	if zid, _ := m.ResolvePoint(pt(0)); zid != first.ID {
		t.Fatalf("ResolvePoint(0) = %q, want first shard %q", zid, first.ID)
	}
}

// TestResolveRangeSpanning pins the walk with explicit multi-shard cases: a
// range crossing several shards returns them in keyspace order, an exclusive
// end exactly on a shard's Start excludes that shard, and a whole-keyspace
// range returns every shard.
func TestResolveRangeSpanning(t *testing.T) {
	m := manifestFromBounds(t, hb(0x80), hb(0xc0), hb(0xe0), hb(0xf0)) // s0..s4
	ids := func(names ...string) []ShardID {
		out := make([]ShardID, len(names))
		for i, n := range names {
			out[i] = ShardID(n)
		}
		return out
	}

	cases := []struct {
		name string
		r    KeyRange
		want []ShardID
	}{
		{"within one shard", KeyRange{Start: pt(hb(0x90)), End: kend(hb(0xa0))}, ids("s1")},
		{"end on a boundary excludes the next shard", KeyRange{Start: pt(hb(0x80)), End: kend(hb(0xe0))}, ids("s1", "s2")},
		{"max-ended tail", KeyRange{Start: pt(hb(0x80)), End: maxKeyEnd}, ids("s1", "s2", "s3", "s4")},
		{"whole keyspace", KeyRange{Start: pt(0), End: maxKeyEnd}, ids("s0", "s1", "s2", "s3", "s4")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := m.ResolveRange(c.r)
			if !ok {
				t.Fatalf("ResolveRange(%+v) reported not ok", c.r)
			}
			if !slices.Equal(got, c.want) {
				t.Fatalf("ResolveRange = %v, want %v", got, c.want)
			}
			if want, _ := slowResolveRange(m, c.r); !slices.Equal(got, want) {
				t.Fatalf("ResolveRange = %v, reference = %v", got, want)
			}
		})
	}
}

// TestResolveDestinationSetRejectsMalformedRange asserts the destination-set
// resolver fails closed on a malformed range with the typed error.
func TestResolveDestinationSetRejectsMalformedRange(t *testing.T) {
	m := manifestFromBounds(t, hb(0x80))
	d := DestinationSet{
		Points: []KeyspacePoint{pt(hb(0x10))},
		Ranges: []KeyRange{{Start: pt(0x80), End: kend(0x40)}},
	}
	ids, err := m.ResolveDestinationSet(d)
	if err == nil {
		t.Fatal("ResolveDestinationSet accepted a malformed range")
	}
	if !errors.Is(err, ErrInvalidDestination) {
		t.Fatalf("error %v does not match ErrInvalidDestination", err)
	}
	var de *DestinationError
	if !errors.As(err, &de) {
		t.Fatalf("error is %T, want *DestinationError", err)
	}
	if ids != nil {
		t.Fatalf("ResolveDestinationSet returned ids %v alongside an error", ids)
	}
}
