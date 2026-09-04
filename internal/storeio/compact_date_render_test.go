package storeio

import (
	"bytes"
	"testing"
)

func TestCompactDateRenderMatchesArithmetic(t *testing.T) {
	var got, want [12]byte
	for ordinal := int32(0); ordinal < int32(compactDaysBeforeYear(10_000)); ordinal++ {
		if !bytes.Equal(appendCompactDate(got[:0], ordinal), appendCompactDateArithmetic(want[:0], ordinal)) {
			t.Fatalf("ordinal=%d got=%s want=%s", ordinal, got, want)
		}
	}
	for _, ordinal := range []int32{-1 << 31, -1, 3652425, 1<<31 - 1} {
		if !bytes.Equal(appendCompactDate(got[:0], ordinal), appendCompactDateArithmetic(want[:0], ordinal)) {
			t.Fatalf("out-of-range ordinal=%d differs", ordinal)
		}
	}
}

func BenchmarkCompactDateRender(b *testing.B) {
	for _, test := range []struct {
		name string
		fn   func([]byte, int32) []byte
	}{{"arithmetic", appendCompactDateArithmetic}, {"cycle", appendCompactDate}} {
		b.Run(test.name, func(b *testing.B) {
			var dst [12]byte
			ordinal := uint32(737790)
			var sink byte
			b.ReportAllocs()
			for b.Loop() {
				ordinal = (ordinal + 31) % 3652425
				out := test.fn(dst[:0], int32(ordinal))
				sink ^= out[10]
			}
			if sink == 255 {
				b.Fatal("invalid date digit")
			}
		})
	}
}
