package distribution

// The bound-constraint layer is where predicate order, exact-number spelling,
// and dynamic unknowns must all fold into one canonical ordinal domain before a
// single byte of routing work happens, so these tests pin the design's
// "Constraint properties": predicate order does not change domains, equality and
// membership intersect correctly, exact numeric spellings deduplicate, a
// contradiction collapses to an empty domain, an unknown dynamic value never
// narrows, and the overflow-safe candidate estimator is exact at its boundary
// and never forms an overflowing product. Equality of values is asserted through
// the current codec bytes, never Go equality or source spelling, so a
// regression that lets "5" and "5.0" diverge is caught here.

import (
	"math"
	"testing"
)

// must fails the test on a non-nil error.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// canonOf returns a scalar's canonical bytes as a string key.
func canonOf(t *testing.T, s Scalar) string {
	t.Helper()
	b, err := CurrentTupleCodec.AppendScalar(nil, s)
	if err != nil {
		t.Fatalf("AppendScalar(%v): %v", s, err)
	}
	return string(b)
}

// assertFiniteDomain asserts dom is finite and holds exactly the want values as
// a set keyed by canonical bytes, catching both missing and duplicate values.
func assertFiniteDomain(t *testing.T, dom ValueDomain, want ...Scalar) {
	t.Helper()
	if dom.Kind != DomainFinite {
		t.Fatalf("domain kind = %v, want DomainFinite", dom.Kind)
	}
	wantSet := make(map[string]bool, len(want))
	for _, s := range want {
		wantSet[canonOf(t, s)] = true
	}
	if len(dom.Values) != len(wantSet) {
		t.Fatalf("domain has %d values, want %d distinct", len(dom.Values), len(wantSet))
	}
	for _, v := range dom.Values {
		c := canonOf(t, v)
		if !wantSet[c] {
			t.Fatalf("domain contains unexpected value %v", v)
		}
		delete(wantSet, c) // a repeat here means dom.Values held a duplicate
	}
	if len(wantSet) != 0 {
		t.Fatalf("domain is missing %d expected values", len(wantSet))
	}
}

// TestConstraintEqualityMembershipIntersection covers the mandatory equality/IN
// intersection cases and their order-independence: x=5 AND x IN (4,5,5.0) keeps
// {5}, x=5 AND x IN (6,7) contradicts to empty, and x IN (5,5.0,5e0) deduplicates
// three spellings to one value by canonical bytes.
func TestConstraintEqualityMembershipIntersection(t *testing.T) {
	n4, n5, n50, n5e0, n6, n7 := mustNumber("4"), mustNumber("5"), mustNumber("5.0"), mustNumber("5e0"), mustNumber("6"), mustNumber("7")

	b := NewConstraintBuilder()
	must(t, b.AddEquality(n5))
	must(t, b.AddMembership([]Scalar{n4, n5, n50}))
	assertFiniteDomain(t, b.Domain(), n5)

	// Same predicates, reversed order: the domain must be identical.
	b.Reset()
	must(t, b.AddMembership([]Scalar{n4, n5, n50}))
	must(t, b.AddEquality(n5))
	assertFiniteDomain(t, b.Domain(), n5)

	// Equality and membership with no common value contradict to empty.
	b.Reset()
	must(t, b.AddEquality(n5))
	must(t, b.AddMembership([]Scalar{n6, n7}))
	if got := b.Domain(); got.Kind != DomainEmpty {
		t.Fatalf("x=5 AND x IN (6,7): kind = %v, want DomainEmpty", got.Kind)
	}

	// Three equal-valued spellings of five deduplicate to one canonical value.
	dom, err := FiniteDomain(n5, n50, n5e0)
	must(t, err)
	assertFiniteDomain(t, dom, n5)
	if len(dom.Values) != 1 {
		t.Fatalf("IN (5,5.0,5e0): %d values, want 1 after canonical dedup", len(dom.Values))
	}
}

// TestConstraintEmptyMembershipIsEmpty asserts an unsatisfiable membership set
// (no values) is a proven contradiction, not an unknown, in both entry points.
func TestConstraintEmptyMembershipIsEmpty(t *testing.T) {
	b := NewConstraintBuilder()
	must(t, b.AddMembership(nil))
	if got := b.Domain(); got.Kind != DomainEmpty {
		t.Fatalf("empty IN: kind = %v, want DomainEmpty", got.Kind)
	}
	dom, err := FiniteDomain()
	must(t, err)
	if dom.Kind != DomainEmpty {
		t.Fatalf("FiniteDomain(): kind = %v, want DomainEmpty", dom.Kind)
	}
}

// TestConstraintUnbindableNeverNarrows pins "unknown dynamic values never narrow
// routing unsafely": an unbindable predicate leaves an otherwise-finite ordinal
// Unknown regardless of order, yet a proven bindable contradiction still wins
// over an unbindable value so a provably empty ordinal short-circuits to empty.
func TestConstraintUnbindableNeverNarrows(t *testing.T) {
	n5, n6, n7 := mustNumber("5"), mustNumber("6"), mustNumber("7")

	// A satisfiable finite predicate plus an unbindable one stays Unknown.
	b := NewConstraintBuilder()
	must(t, b.AddEquality(n5))
	b.AddUnbindable()
	if got := b.Domain(); got.Kind != DomainUnknown {
		t.Fatalf("finite + unbindable: kind = %v, want DomainUnknown (must not narrow)", got.Kind)
	}

	// Order does not change that outcome.
	b.Reset()
	b.AddUnbindable()
	must(t, b.AddEquality(n5))
	if got := b.Domain(); got.Kind != DomainUnknown {
		t.Fatalf("unbindable + finite: kind = %v, want DomainUnknown", got.Kind)
	}

	// A proven contradiction wins even when an unbindable value is also present.
	b.Reset()
	must(t, b.AddEquality(n5))
	must(t, b.AddMembership([]Scalar{n6, n7}))
	b.AddUnbindable()
	if got := b.Domain(); got.Kind != DomainEmpty {
		t.Fatalf("contradiction + unbindable: kind = %v, want DomainEmpty (contradiction must win)", got.Kind)
	}

	// A lone unbindable value is Unknown.
	b.Reset()
	b.AddUnbindable()
	if got := b.Domain(); got.Kind != DomainUnknown {
		t.Fatalf("lone unbindable: kind = %v, want DomainUnknown", got.Kind)
	}
}

// TestConstraintRejectsUnencodableValue asserts a zero-value Scalar (outside the
// closed placement set) is rejected with the typed *ShardValueError through both
// builder entry points.
func TestConstraintRejectsUnencodableValue(t *testing.T) {
	b := NewConstraintBuilder()
	if err := b.AddEquality(Scalar{}); !isShardValueError(err) {
		t.Fatalf("AddEquality(zero): err = %v, want *ShardValueError", err)
	}
	b.Reset()
	if err := b.AddMembership([]Scalar{mustNumber("1"), {}}); !isShardValueError(err) {
		t.Fatalf("AddMembership(zero): err = %v, want *ShardValueError", err)
	}
	if _, err := FiniteDomain(Scalar{}); !isShardValueError(err) {
		t.Fatalf("FiniteDomain(zero): err = %v, want *ShardValueError", err)
	}
}

// domainOfSize fabricates a canonSet reporting size n, so the pure product
// estimator can be exercised without materializing real values.
func domainOfSize(n int) canonSet {
	return canonSet{items: make([]canonItem, n)}
}

// TestProductExceedsBoundary pins the estimator's exactness at the limit: it
// reports true exactly when the product of domain sizes exceeds the limit, using
// the identity p > floor(limit/n) iff p*n > limit.
func TestProductExceedsBoundary(t *testing.T) {
	cases := []struct {
		sizes []int
		limit int
		want  bool
	}{
		{[]int{4, 4}, 16, false},   // product == limit is not "exceeds"
		{[]int{4, 4}, 15, true},    // product 16 > 15
		{[]int{5, 3}, 15, false},   // product == limit across uneven factors
		{[]int{5, 4}, 15, true},    // one factor alone trips: 5 > 15/4
		{[]int{1, 1, 1}, 1, false}, // all singletons stay at the limit
		{[]int{2}, 1, true},        // a single oversized domain trips immediately
		{[]int{256}, 256, false},   // exactly at the conservative default
		{[]int{257}, 256, true},    // one past it
	}
	for _, c := range cases {
		dom := make([]canonSet, len(c.sizes))
		for i, s := range c.sizes {
			dom[i] = domainOfSize(s)
		}
		if got := productExceeds(dom, c.limit); got != c.want {
			t.Fatalf("productExceeds(sizes=%v, limit=%d) = %v, want %v", c.sizes, c.limit, got, c.want)
		}
	}
}

// TestProductExceedsOverflowSafe drives the estimator with domains whose true
// product (512^8 = 2^72) far exceeds any int and whose naive running multiply
// would overflow int64 (512 * 2^54 = 2^63): it must report true without forming
// an overflowing intermediate, and must report false on the non-overflow side.
func TestProductExceedsOverflowSafe(t *testing.T) {
	huge := make([]canonSet, 8)
	for i := range huge {
		huge[i] = domainOfSize(512)
	}
	if !productExceeds(huge, math.MaxInt) {
		t.Fatal("productExceeds under-reported a product that overflows int")
	}

	// 1024^3 = 2^30 stays well within int, so the same arithmetic reports false.
	small := make([]canonSet, 3)
	for i := range small {
		small[i] = domainOfSize(1024)
	}
	if productExceeds(small, math.MaxInt) {
		t.Fatal("productExceeds over-reported a product well within int")
	}
}

// TestPrefixSet covers membership and longest-supported-prefix selection,
// including that length 0 and lengths outside 1..63 are never members.
func TestPrefixSet(t *testing.T) {
	s := NewPrefixSet(1, 3, 5)
	for _, l := range []int{1, 3, 5} {
		if !s.Contains(l) {
			t.Fatalf("Contains(%d) = false, want true", l)
		}
	}
	for _, l := range []int{-1, 0, 2, 4, 6, 64, 100} {
		if s.Contains(l) {
			t.Fatalf("Contains(%d) = true, want false", l)
		}
	}
	if got := s.LongestAtMost(4); got != 3 {
		t.Fatalf("LongestAtMost(4) = %d, want 3", got)
	}
	if got := s.LongestAtMost(5); got != 5 {
		t.Fatalf("LongestAtMost(5) = %d, want 5", got)
	}
	if got := s.LongestAtMost(2); got != 1 {
		t.Fatalf("LongestAtMost(2) = %d, want 1", got)
	}
	if got := s.LongestAtMost(0); got != 0 {
		t.Fatalf("LongestAtMost(0) = %d, want 0", got)
	}
	if got := NewPrefixSet(0, 64, 100).LongestAtMost(63); got != 0 {
		t.Fatalf("out-of-range prefix lengths must be ignored, got LongestAtMost = %d", got)
	}
}
