package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"
)

func selectorBounds(count int) []BlockSelectorBoundary {
	out := make([]BlockSelectorBoundary, count)
	for i := range out {
		out[i] = BlockSelectorBoundary{First: []byte(fmt.Sprintf("k/%08d/0", i)), Last: []byte(fmt.Sprintf("k/%08d/9", i)), BlockID: uint32(i + 1)}
	}
	return out
}
func TestBlockSelectorSeekRangeGapAndSpace(t *testing.T) {
	bounds := selectorBounds(2048)
	s, err := BuildBlockSelector(bounds)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.Space(64); got.PageCount < 2 || got.BytesPerKey() > .40 {
		t.Fatalf("space=%+v", got)
	}
	var c BlockSelectorCursor
	gap := append(slices.Clone(bounds[23].Last), 0)
	hit, ok := s.Seek(gap, &c, func(id uint32, target []byte) (uint8, bool) {
		n := int(id - 1)
		return 0, bytes.Compare(bounds[n].Last, target) >= 0
	})
	if !ok || !hit.Fallback || hit.BlockID != 25 {
		t.Fatalf("hit=%+v ok=%v", hit, ok)
	}
	r := s.Range(bounds[100].First, &c)
	for want := uint32(101); want < 106; want++ {
		got, ok := r.BlockID()
		if !ok || got != want {
			t.Fatalf("range=%d/%v", got, ok)
		}
		if want < 105 && !r.NextBlock() {
			t.Fatal("early")
		}
	}
	if a := testing.AllocsPerRun(1000, func() {
		if _, _, ok := s.Select(bounds[100].First, &c); !ok {
			panic("select")
		}
	}); a != 0 {
		t.Fatalf("allocs=%v", a)
	}
}
func TestBlockSelectorCorruptionAndTailZero(t *testing.T) {
	p, err := BuildBlockSelectorPage(selectorBounds(96))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, mut := range []func([]byte){func(b []byte) { b[0] ^= 1 }, func(b []byte) { b[len(b)-1] = 1 }, func(b []byte) { b[len(b)-1] = 1; binary.LittleEndian.PutUint32(b[24:28], blockSelectorChecksum(b)) }} {
		b := slices.Clone(p.data)
		mut(b)
		if _, err := OpenBlockSelectorPage(b); !errors.Is(err, ErrBlockSelectorCorrupt) {
			t.Fatalf("err=%v", err)
		}
	}
}
func BenchmarkBlockSelectorSelect(b *testing.B) {
	bounds := selectorBounds(4096)
	s, err := BuildBlockSelector(bounds)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	var c BlockSelectorCursor
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := s.Select(bounds[i%len(bounds)].First, &c); !ok {
			b.Fatal("select")
		}
	}
}
