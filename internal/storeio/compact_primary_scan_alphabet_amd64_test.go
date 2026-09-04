//go:build amd64

package storeio

import (
	"encoding/binary"
	"testing"

	xsyscpu "golang.org/x/sys/cpu"
)

func TestCompactScanAlphabet5BMI2MatchesPackedCodes(t *testing.T) {
	if !xsyscpu.X86.HasBMI2 {
		t.Skip("BMI2 unavailable")
	}
	for offset := 0; offset < 8; offset++ {
		for symbols := 32; symbols <= 96; symbols++ {
			data := make([]byte, (offset+symbols*5+7)/8+8)
			want := make([]byte, symbols)
			for i := range symbols {
				code := byte((i*17 + offset*3) % 26)
				want[i] = 'a' + code
				at := offset + i*5
				word := binary.LittleEndian.Uint64(data[at/8:])
				word |= uint64(code) << uint(at&7)
				binary.LittleEndian.PutUint64(data[at/8:], word)
			}
			got := make([]byte, symbols)
			n := compactScanAlphabet5BMI2(&got[0], len(got), &data[0], len(data), offset, 'a')
			if n != symbols&^7 {
				t.Fatalf("offset=%d symbols=%d decoded=%d", offset, symbols, n)
			}
			for i := 0; i < n; i++ {
				if got[i] != want[i] {
					t.Fatalf("offset=%d symbols=%d index=%d got=%q want=%q", offset, symbols, i, got[i], want[i])
				}
			}
		}
	}
}
