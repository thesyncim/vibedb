package tin

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

// wireRoundTrip seals every term of ix, remarshals each sealed list, and
// demands structural identity plus byte stability.
func wireRoundTrip(t *testing.T, ix *Index) map[uint64][]byte {
	t.Helper()
	wires := map[uint64][]byte{}
	for term, p := range ix.post {
		if p.sealed == nil {
			continue
		}
		w, err := MarshalSealed(p.sealed)
		if err != nil {
			t.Fatalf("term %d: marshal: %v", term, err)
		}
		if len(w) != sealWireLen(p.sealed) {
			t.Fatalf("term %d: wire %d != predicted %d", term, len(w), sealWireLen(p.sealed))
		}
		rt, err := UnmarshalSealed(w)
		if err != nil {
			t.Fatalf("term %d: unmarshal: %v", term, err)
		}
		if !reflect.DeepEqual(rt, p.sealed) {
			t.Fatalf("term %d: round trip changed structure", term)
		}
		w2, err := MarshalSealed(rt)
		if err != nil || !bytes.Equal(w, w2) {
			t.Fatalf("term %d: remarshal unstable", term)
		}
		wires[term] = w
	}
	if len(wires) == 0 {
		t.Fatal("no sealed lists to round-trip")
	}
	return wires
}

func TestSealWireRoundTrip(t *testing.T) {
	// Single-block, two-block, and multi-block shapes; tiny lists stay
	// open under adopt-if-smaller, so sizes start where sealing bites.
	for _, n := range []int{128, 129, 300, 2000} {
		ix := topKExactIndex(t, false, n)
		ix.Seal()
		wires := wireRoundTrip(t, ix)
		// A receiving node swaps in the shipped lists: Match and Score
		// must agree bit-exactly with the pristine twin.
		twin := topKExactIndex(t, false, n)
		twin.Seal()
		for term, w := range wires {
			rt, err := UnmarshalSealed(w)
			if err != nil {
				t.Fatal(err)
			}
			ix.post[term].sealed = rt
		}
		for _, term := range []string{"zipf", "common", "rare"} {
			q := Query{Op: OpTerm, Term: mustHash(t, term)}
			if !reflect.DeepEqual(ix.Match(q, nil), twin.Match(q, nil)) {
				t.Fatalf("n=%d term=%q: shipped match differs", n, term)
			}
			for _, topK := range []int{0, 1, 10} {
				if !reflect.DeepEqual(ix.Score(q, topK, nil), twin.Score(q, topK, nil)) {
					t.Fatalf("n=%d term=%q topK=%d: shipped score differs", n, term, topK)
				}
			}
		}
		and := Query{Op: OpAnd, Kids: []Query{
			{Op: OpTerm, Term: mustHash(t, "zipf")},
			{Op: OpTerm, Term: mustHash(t, "common")},
		}}
		if !reflect.DeepEqual(ix.Match(and, nil), twin.Match(and, nil)) {
			t.Fatalf("n=%d: shipped AND match differs", n)
		}
	}
}

func TestSealWireRejects(t *testing.T) {
	ix := topKExactIndex(t, false, 300)
	ix.Seal()
	var good []byte
	nb := 0
	for _, p := range ix.post {
		if p.sealed != nil {
			good, _ = MarshalSealed(p.sealed)
			nb = len(p.sealed.blk)
			break
		}
	}
	// Field offsets from the layout: header 21, first 8/block,
	// directory 51/block (rows 4, widths 3, offsets 12, ckpts 32).
	blk0 := 21 + 8*nb
	corrupt := func(mut func([]byte)) []byte {
		b := bytes.Clone(good)
		mut(b)
		return b
	}
	cases := map[string][]byte{
		"empty":      {},
		"short":      good[:3],
		"magic":      corrupt(func(b []byte) { b[0] ^= 0xff }),
		"version":    corrupt(func(b []byte) { b[4] = 2 }),
		"zeroBlocks": corrupt(func(b []byte) { b[17], b[18], b[19], b[20] = 0, 0, 0, 0 }),
		"hugeBlocks": corrupt(func(b []byte) { b[17], b[18], b[19], b[20] = 0xff, 0xff, 0xff, 0xff }),
		"zeroWidth":  corrupt(func(b []byte) { b[blk0+4] = 0 }),
		"wideWidth":  corrupt(func(b []byte) { b[blk0+5] = 33 }),
		"badRows":    corrupt(func(b []byte) { b[blk0] ^= 0xff }),
		"badOff": corrupt(func(b []byte) {
			b[blk0+7], b[blk0+8], b[blk0+9], b[blk0+10] = 0xff, 0xff, 0xff, 0xff
		}),
		"truncTail": good[:len(good)-1],
		"truncMid":  good[:len(good)/2],
	}
	// A stream length claiming gigabytes with no bytes behind it.
	idsLen := blk0 + 51*nb
	huge := corrupt(func(b []byte) {
		b[idsLen], b[idsLen+1], b[idsLen+2], b[idsLen+3] = 0xff, 0xff, 0xff, 0xff
	})
	cases["hugeStream"] = huge
	for name, b := range cases {
		if _, err := UnmarshalSealed(b); err == nil {
			t.Fatalf("%s: corrupt wire accepted", name)
		}
	}
	// The huge claim must fail at the stream length, before the make:
	// the exact failure names the pre-make bounds check. (Error values
	// themselves allocate, so alloc counting cannot isolate the make.)
	_, err := UnmarshalSealed(huge)
	if err == nil || !strings.Contains(err.Error(), "bad ids length") {
		t.Fatalf("huge stream length failed as %v", err)
	}
	if _, err := MarshalSealed(nil); err == nil {
		t.Fatal("nil marshal accepted")
	}
}

func BenchmarkSealWire(b *testing.B) {
	ix := skewTopKIndex(b, true)
	var lists []*sealedPostings
	var total uint64
	for _, p := range ix.post {
		if p.sealed != nil {
			lists = append(lists, p.sealed)
			total += p.sealed.sealedBytes()
		}
	}
	wires := make([][]byte, len(lists))
	for i, s := range lists {
		w, err := MarshalSealed(s)
		if err != nil {
			b.Fatal(err)
		}
		wires[i] = w
	}
	var wireTotal int
	for _, w := range wires {
		wireTotal += len(w)
	}
	b.ReportMetric(float64(wireTotal)/float64(total)*100, "%-of-sealed")
	b.Run("Marshal", func(b *testing.B) {
		b.SetBytes(int64(total))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, s := range lists {
				_, _ = MarshalSealed(s)
			}
		}
	})
	b.Run("Unmarshal", func(b *testing.B) {
		b.SetBytes(int64(wireTotal))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, w := range wires {
				_, _ = UnmarshalSealed(w)
			}
		}
	})
}
