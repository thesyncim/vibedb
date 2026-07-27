package bitset

import (
	"math/rand"
	"slices"
	"testing"
)

func TestBooleanDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	sizes := make([]int, 81)
	for i := range sizes {
		sizes[i] = i
	}
	sizes = append(sizes, 127, 128, 129, 511, 512, 513, 1023, 1024, 1025, 4095, 4096, 4097)
	for _, n := range sizes {
		a := make([]uint64, n)
		b := make([]uint64, n+n%5)
		for i := range a {
			a[i] = rng.Uint64()
		}
		for i := range b {
			b[i] = rng.Uint64()
		}
		prefix := []uint64{0xfeed, 0xbeef}
		for _, test := range []struct {
			name string
			got  []uint64
			ref  func([]uint64, []uint64, []uint64) []uint64
		}{
			{"and", And(append([]uint64{}, prefix...), a, b), refAnd},
			{"or", Or(append([]uint64{}, prefix...), a, b), refOr},
			{"and-not", AndNot(append([]uint64{}, prefix...), a, b), refAndNot},
		} {
			want := test.ref(append([]uint64{}, prefix...), a, b)
			if !slices.Equal(test.got, want) {
				t.Fatalf("%s n=%d mismatch", test.name, n)
			}
		}
		if got, want := And3(append([]uint64{}, prefix...), a, b, a), refAnd3(append([]uint64{}, prefix...), a, b, a); !slices.Equal(got, want) {
			t.Fatalf("and3 n=%d mismatch", n)
		}

		// Exact in-place aliasing is a load-before-store requirement of every
		// vector block and catches accidental output/input overlap assumptions.
		checkAlias := func(name string, got, want []uint64) {
			t.Helper()
			if !slices.Equal(got, want) {
				t.Fatalf("in-place %s n=%d mismatch", name, n)
			}
		}
		aliasA := cloneWords(a, max(len(a), len(b)))
		checkAlias("and-a", And(aliasA[:0], aliasA, b), refAnd(nil, a, b))
		aliasA = cloneWords(a, max(len(a), len(b)))
		checkAlias("and3-a", And3(aliasA[:0], aliasA, b, a), refAnd3(nil, a, b, a))
		aliasB := cloneWords(b, max(len(a), len(b)))
		checkAlias("and3-b", And3(aliasB[:0], a, aliasB, a), refAnd3(nil, a, b, a))
		aliasC := cloneWords(a, max(len(a), len(b)))
		checkAlias("and3-c", And3(aliasC[:0], a, b, aliasC), refAnd3(nil, a, b, a))
		aliasB = cloneWords(b, max(len(a), len(b)))
		checkAlias("and-b", And(aliasB[:0], a, aliasB), refAnd(nil, a, b))
		aliasA = cloneWords(a, max(len(a), len(b)))
		checkAlias("or-a", Or(aliasA[:0], aliasA, b), refOr(nil, a, b))
		aliasB = cloneWords(b, max(len(a), len(b)))
		checkAlias("or-b", Or(aliasB[:0], a, aliasB), refOr(nil, a, b))
		aliasA = cloneWords(a, len(a))
		checkAlias("and-not-a", AndNot(aliasA[:0], aliasA, b), refAndNot(nil, a, b))
		aliasB = cloneWords(b, max(len(a), len(b)))
		checkAlias("and-not-b", AndNot(aliasB[:0], a, aliasB), refAndNot(nil, a, b))
	}
}

func cloneWords(src []uint64, capacity int) []uint64 {
	dst := make([]uint64, len(src), capacity)
	copy(dst, src)
	return dst
}

func refAnd(dst, a, b []uint64) []uint64 {
	for i := 0; i < min(len(a), len(b)); i++ {
		dst = append(dst, a[i]&b[i])
	}
	return dst
}

func refAnd3(dst, a, b, c []uint64) []uint64 {
	for i := 0; i < min(len(a), len(b), len(c)); i++ {
		dst = append(dst, a[i]&b[i]&c[i])
	}
	return dst
}

func refOr(dst, a, b []uint64) []uint64 {
	for i := 0; i < max(len(a), len(b)); i++ {
		var x, y uint64
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		dst = append(dst, x|y)
	}
	return dst
}

func refAndNot(dst, a, b []uint64) []uint64 {
	for i, x := range a {
		var y uint64
		if i < len(b) {
			y = b[i]
		}
		dst = append(dst, x&^y)
	}
	return dst
}

func TestBooleanSteadyAllocs(t *testing.T) {
	a := make([]uint64, 4096)
	b := make([]uint64, 4096)
	dst := make([]uint64, 0, 4096)
	for _, test := range []struct {
		name string
		run  func([]uint64, []uint64, []uint64) []uint64
	}{
		{"And", And},
		{"Or", Or},
		{"AndNot", AndNot},
	} {
		allocs := testing.AllocsPerRun(100, func() {
			dst = test.run(dst[:0], a, b)
		})
		if allocs != 0 {
			t.Fatalf("%s allocated %.2f times, want 0", test.name, allocs)
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		dst = And3(dst[:0], a, b, a)
	})
	if allocs != 0 {
		t.Fatalf("And3 allocated %.2f times, want 0", allocs)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n != 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
