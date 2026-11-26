package storeio

import (
	"fmt"
	"testing"
)

func TestCompactCommonPrefixEqualHeadParity(t *testing.T) {
	baseA := make([]byte, 384)
	baseB := make([]byte, 384)
	for i := range baseA {
		v := byte((i*31 + 17) & 0xff)
		baseA[i] = v
		baseB[i] = v
	}

	lengths := []int{0, 1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 47, 48, 63, 64, 65, 95, 96, 127, 128, 129, 192, 255, 256, 257, 300}
	for _, length := range lengths {
		for alignA := 0; alignA <= 17; alignA += 5 {
			for alignB := 0; alignB <= 19; alignB += 6 {
				a := make([]byte, length+alignA)[alignA:]
				b := make([]byte, length+alignB)[alignB:]
				copy(a, baseA[:length])
				copy(b, baseB[:length])

				want := compactCommonPrefixEqualHeadScalar(a, b)
				got := compactCommonPrefixEqualHead(a, b)
				if got != want || got != length {
					t.Fatalf("equal len=%d align=(%d,%d): got=%d want=%d", length, alignA, alignB, got, want)
				}

				for diffPos := 0; diffPos < length; diffPos++ {
					b[diffPos] ^= 0x01
					want := compactCommonPrefixEqualHeadScalar(a, b)
					got := compactCommonPrefixEqualHead(a, b)
					if got != want || got != diffPos {
						t.Fatalf("mismatch len=%d pos=%d align=(%d,%d): got=%d want=%d", length, diffPos, alignA, alignB, got, want)
					}
					b[diffPos] ^= 0x80
					want = compactCommonPrefixEqualHeadScalar(a, b)
					got = compactCommonPrefixEqualHead(a, b)
					if got != want || got != diffPos {
						t.Fatalf("mismatch high-bit len=%d pos=%d align=(%d,%d): got=%d want=%d", length, diffPos, alignA, alignB, got, want)
					}
					b[diffPos] = a[diffPos]
				}
			}
		}
	}

	// Asymmetric lengths with identical prefix and sentinel differing byte beyond min(len).
	for _, pair := range [][2]int{{0, 64}, {1, 64}, {31, 64}, {32, 64}, {63, 128}, {64, 128}, {95, 200}, {128, 64}, {200, 33}} {
		a := append([]byte(nil), baseA[:pair[0]]...)
		b := append([]byte(nil), baseB[:pair[1]]...)
		want := compactCommonPrefixEqualHeadScalar(a, b)
		got := compactCommonPrefixEqualHead(a, b)
		if got != want || got != min(pair[0], pair[1]) {
			t.Fatalf("asymmetric (%d,%d): got=%d want=%d", pair[0], pair[1], got, want)
		}
	}

	// Verify zero allocations on hot path.
	a := baseA[:256]
	b := append([]byte(nil), baseB[:256]...)
	b[200] ^= 1
	allocs := testing.AllocsPerRun(100, func() {
		if compactCommonPrefixEqualHead(a, b) != 200 {
			t.Fatal("unexpected mismatch offset")
		}
	})
	if allocs != 0 {
		t.Fatalf("compactCommonPrefixEqualHead allocated %.2f times/op, want 0", allocs)
	}
}

func BenchmarkCompactCommonPrefixEqualHead(b *testing.B) {
	for _, length := range []int{32, 64, 128, 256} {
		a := make([]byte, length)
		c := make([]byte, length)
		for i := range a {
			a[i] = byte(i*13 + 7)
			c[i] = a[i]
		}
		c[length-1] ^= 1
		b.Run(fmt.Sprintf("scalar/%d", length), func(b *testing.B) {
			var sink int
			for i := 0; i < b.N; i++ {
				sink += compactCommonPrefixEqualHeadScalar(a, c)
			}
			if sink == 0 {
				b.Fatal("zero sink")
			}
		})
		b.Run(fmt.Sprintf("simd/%d", length), func(b *testing.B) {
			var sink int
			for i := 0; i < b.N; i++ {
				sink += compactCommonPrefixEqualHead(a, c)
			}
			if sink == 0 {
				b.Fatal("zero sink")
			}
		})
	}
}
