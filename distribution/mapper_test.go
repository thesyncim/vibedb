package distribution

// The native reference mapper is the dependency-free stand-in that lets routing
// run end-to-end, so its two structural guarantees are pinned here: equal-valued
// scalar spellings map to identical keyspace positions (the same canonical-byte
// identity the golden tuple vectors freeze), and a full key always lands inside
// every one of its own shorter leading-prefix ranges — the invariant the router
// leans on so a partially bound key can only ever widen, never point somewhere a
// full key would not. Arity, prefix, and value-type contracts round out the set.

import (
	"errors"
	"testing"
)

// TestNativeMapperArity covers the arity contract: 1..8 construct and report
// their arity, anything else panics because a component needs a keyspace byte.
func TestNativeMapperArity(t *testing.T) {
	for _, a := range []int{-1, 0, 9, 100} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("NewNativeMapper(%d) did not panic", a)
				}
			}()
			_ = NewNativeMapper(a)
		}()
	}
	for _, a := range []int{1, 4, 8} {
		m := NewNativeMapper(a)
		if m.Arity() != a {
			t.Fatalf("Arity() = %d, want %d", m.Arity(), a)
		}
		if !m.SupportedPrefixes().Contains(1) || !m.SupportedPrefixes().Contains(a) {
			t.Fatalf("arity %d must support leading prefixes 1..%d", a, a)
		}
		if m.Version() != NativeMapperVersion {
			t.Fatalf("Version() = %d, want %d", m.Version(), NativeMapperVersion)
		}
	}
}

// TestNativeMapperEqualSpellingsMapIdentically asserts placement identity by
// value, not spelling: every equal-valued number spelling, and the zero/negative
// zero pair, fold to one keyspace point.
func TestNativeMapperEqualSpellingsMapIdentically(t *testing.T) {
	m := NewNativeMapper(1)
	var want KeyspacePoint
	for i, s := range []string{"5", "5.0", "5e0", "50e-1"} {
		p, err := m.PointFor([]Scalar{mustNumber(s)})
		must(t, err)
		if i == 0 {
			want = p
			continue
		}
		if p != want {
			t.Fatalf("spelling %q mapped to %x, want %x (canonical placement identity)", s, p, want)
		}
	}
	z, err := m.PointFor([]Scalar{mustNumber("0")})
	must(t, err)
	nz, err := m.PointFor([]Scalar{mustNumber("-0")})
	must(t, err)
	if z != nz {
		t.Fatalf("0 mapped to %x but -0 to %x; they must unify", z, nz)
	}
}

// TestNativeMapperPrefixRangeContainsFullKey pins the leading-prefix invariant:
// for several arities and value shapes, the full-key point lies inside every one
// of its supported shorter leading-prefix ranges, and each such range is valid.
func TestNativeMapperPrefixRangeContainsFullKey(t *testing.T) {
	sample := []Scalar{mustNumber("1"), mustNumber("42"), NewString("tenant-x"), mustNumber("-7.5"), NewString("")}
	for arity := 2; arity <= 4; arity++ {
		m := NewNativeMapper(arity)
		full := make([]Scalar, arity)
		for i := range full {
			full[i] = sample[i%len(sample)]
		}
		p, err := m.PointFor(full)
		must(t, err)
		for l := 1; l < arity; l++ {
			rng, err := m.PrefixRangeFor(full[:l])
			must(t, err)
			if !rng.Valid() {
				t.Fatalf("arity %d prefix %d produced an invalid range %+v", arity, l, rng)
			}
			if !rng.Contains(p) {
				t.Fatalf("arity %d prefix %d range %+v does not contain full-key point %x", arity, l, rng, p)
			}
		}
	}
}

// TestNativeMapperFullKeyMapsToPoint asserts a full key maps to exactly one
// point equal to PointFor, and a supported shorter prefix maps to exactly the
// PrefixRangeFor range.
func TestNativeMapperFullKeyMapsToPoint(t *testing.T) {
	m := NewNativeMapper(2)
	full := []Scalar{mustNumber("7"), NewString("abc")}
	p, err := m.PointFor(full)
	must(t, err)
	ds, err := m.MapPrefix(full)
	must(t, err)
	if len(ds.Ranges) != 0 || len(ds.Points) != 1 || ds.Points[0] != p {
		t.Fatalf("MapPrefix(full) = %+v, want a single point %x", ds, p)
	}

	prefix := full[:1]
	rng, err := m.PrefixRangeFor(prefix)
	must(t, err)
	ds, err = m.MapPrefix(prefix)
	must(t, err)
	if len(ds.Points) != 0 || len(ds.Ranges) != 1 || ds.Ranges[0] != rng {
		t.Fatalf("MapPrefix(prefix) = %+v, want a single range %+v", ds, rng)
	}
}

// TestNativeMapperAdmits covers the value-type and length contracts: a length
// mismatch is ErrIncompleteShardKey, an unsupported length is ErrUnsupportedMapper,
// a non-encodable value is ErrInvalidShardValue, and String/Number are accepted.
func TestNativeMapperAdmits(t *testing.T) {
	m := NewNativeMapper(2)

	if err := m.Admits(2, []Scalar{mustNumber("1")}); !errors.Is(err, ErrIncompleteShardKey) {
		t.Fatalf("length mismatch: err = %v, want ErrIncompleteShardKey", err)
	}
	if err := m.Admits(3, []Scalar{mustNumber("1"), mustNumber("2"), mustNumber("3")}); !errors.Is(err, ErrUnsupportedMapper) {
		t.Fatalf("unsupported length: err = %v, want ErrUnsupportedMapper", err)
	}
	if err := m.Admits(1, []Scalar{{}}); !errors.Is(err, ErrInvalidShardValue) {
		t.Fatalf("non-encodable value: err = %v, want ErrInvalidShardValue", err)
	}
	if err := m.Admits(2, []Scalar{NewString("a"), mustNumber("3")}); err != nil {
		t.Fatalf("String and Number must be admitted, got %v", err)
	}
}

// TestNativeMapperMapPrefixRejectsBadLength asserts MapPrefix self-validates: a
// zero-length or over-arity prefix is ErrIncompleteShardKey.
func TestNativeMapperMapPrefixRejectsBadLength(t *testing.T) {
	m := NewNativeMapper(2)
	if _, err := m.MapPrefix(nil); !errors.Is(err, ErrIncompleteShardKey) {
		t.Fatalf("MapPrefix(nil): err = %v, want ErrIncompleteShardKey", err)
	}
	if _, err := m.MapPrefix([]Scalar{mustNumber("1"), mustNumber("2"), mustNumber("3")}); !errors.Is(err, ErrIncompleteShardKey) {
		t.Fatalf("MapPrefix(over-arity): err = %v, want ErrIncompleteShardKey", err)
	}
}

// TestNativeMapperPredictionHelpersReject asserts the test-prediction helpers
// reject inputs outside their contract: PointFor needs a full key, PrefixRangeFor
// needs a supported shorter prefix.
func TestNativeMapperPredictionHelpersReject(t *testing.T) {
	m := NewNativeMapper(2)
	if _, err := m.PointFor([]Scalar{mustNumber("1")}); !errors.Is(err, ErrIncompleteShardKey) {
		t.Fatalf("PointFor(short): err = %v, want ErrIncompleteShardKey", err)
	}
	if _, err := m.PrefixRangeFor([]Scalar{mustNumber("1"), mustNumber("2")}); !errors.Is(err, ErrUnsupportedMapper) {
		t.Fatalf("PrefixRangeFor(full length): err = %v, want ErrUnsupportedMapper", err)
	}
}
