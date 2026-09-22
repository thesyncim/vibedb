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

// encodeSealedV1 emits the pre-packing wire layout for s with raw bases
// from open: the exact historical bytes a v1 node would ship. It pins the
// rolling-upgrade reader against the old layout, never today's marshal.
func encodeSealedV1(t *testing.T, s *sealedPostings, open *postings) []byte {
	t.Helper()
	var out []byte
	putU32 := func(v uint32) {
		out = append(out, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	}
	putU64 := func(v uint64) {
		putU32(uint32(v))
		putU32(uint32(v >> 32))
	}
	out = append(out, sealWireMagic...)
	out = append(out, sealWireVersion1)
	putU32(s.n)
	putU64(s.occ)
	putU32(uint32(len(s.blk)))
	for _, f := range s.first {
		putU64(f)
	}
	for i := range s.blk {
		bl := &s.blk[i]
		putU32(bl.rows)
		out = append(out, bl.idW, bl.cntW, bl.posW)
		putU32(bl.idOff)
		putU32(bl.cntOff)
		putU32(bl.posOff)
		for _, c := range bl.posCkpt {
			putU32(c)
		}
	}
	putStream := func(w []uint32) {
		putU32(uint32(len(w)))
		for _, v := range w {
			putU32(v)
		}
	}
	putStream(s.ids)
	putStream(s.cnts)
	raw := make([]uint32, 0, s.n)
	for g := 0; g < int(s.n); g++ {
		raw = append(raw, open.pos[open.off[g]])
	}
	putStream(raw)
	putStream(s.pos)
	return out
}

// TestSealWireV1Compat proves a v1 shipment decodes to a queryable list:
// every sealed list re-encoded in the pre-packing layout must decode and
// agree with the v2 twin over the whole operator corpus (phrases and
// proximity exercise packed-base seeks through the width-32 synthesis),
// and the v1 wire must be larger on position-small corpora, proving the
// packing saves space.
func TestSealWireV1Compat(t *testing.T) {
	open, sealed := sealTwin(t, sealedCorpusDocs())
	// Raw bases come from the open twin, whose rows must sort into the
	// sealed row order first (map-order Adds append unsorted).
	open.mu.Lock()
	open.ensureSorted()
	open.mu.Unlock()
	v1count := 0
	var v1total, v2total int
	for term, p := range sealed.post {
		if p.sealed == nil {
			continue
		}
		op := open.post[term]
		if op == nil {
			t.Fatalf("term %x missing from open twin", term)
		}
		v1 := encodeSealedV1(t, p.sealed, op)
		v2, err := MarshalSealed(p.sealed)
		if err != nil {
			t.Fatalf("term %x: marshal: %v", term, err)
		}
		// Per-list v2 can cost up to 5 directory bytes more than v1
		// when a tiny list's bases round up to whole words; the
		// packing win is aggregate, and the adopt-if-smaller policy
		// still pins every list against open.
		v1total += len(v1)
		v2total += len(v2)
		s1, err := UnmarshalSealed(v1)
		if err != nil {
			t.Fatalf("term %x: v1 unmarshal: %v", term, err)
		}
		p.sealed = s1
		v1count++
	}
	if v1count == 0 {
		t.Fatal("no sealed lists for v1 compat")
	}
	if v1total <= v2total {
		t.Fatalf("v1 wire %d bytes <= v2 %d, want larger", v1total, v2total)
	}
	t.Logf("v1 wire %d bytes vs v2 %d (%.2fx)", v1total, v2total, float64(v1total)/float64(v2total))
	for _, input := range scoreCorpusInputs {
		q, err := open.ParseTINQL(input)
		if err != nil {
			t.Fatalf("ParseTINQL(%q): %v", input, err)
		}
		if got, want := matchString(sealed, q), matchString(open, q); got != want {
			t.Fatalf("%q match:\n v1 %s\n open %s", input, got, want)
		}
		if got, want := scoreString(sealed, q), scoreString(open, q); got != want {
			t.Fatalf("%q score:\n v1 %s\n open %s", input, got, want)
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
	// directory 56/block (rows 4, widths 4, offsets 16, ckpts 32).
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
		"version":    corrupt(func(b []byte) { b[4] = 3 }),
		"versionV1":  corrupt(func(b []byte) { b[4] = 1 }),
		"zeroBlocks": corrupt(func(b []byte) { b[17], b[18], b[19], b[20] = 0, 0, 0, 0 }),
		"hugeBlocks": corrupt(func(b []byte) { b[17], b[18], b[19], b[20] = 0xff, 0xff, 0xff, 0xff }),
		"zeroWidth":  corrupt(func(b []byte) { b[blk0+4] = 0 }),
		"wideWidth":  corrupt(func(b []byte) { b[blk0+5] = 33 }),
		// baseW sits fourth in the widths; both directions fail.
		"zeroBaseWidth": corrupt(func(b []byte) { b[blk0+7] = 0 }),
		"wideBaseWidth": corrupt(func(b []byte) { b[blk0+7] = 33 }),
		"badRows":       corrupt(func(b []byte) { b[blk0] ^= 0xff }),
		"badOff": corrupt(func(b []byte) {
			b[blk0+8], b[blk0+9], b[blk0+10], b[blk0+11] = 0xff, 0xff, 0xff, 0xff
		}),
		"badBaseOff": corrupt(func(b []byte) {
			b[blk0+20], b[blk0+21], b[blk0+22], b[blk0+23] = 0xff, 0xff, 0xff, 0xff
		}),
		"truncTail": good[:len(good)-1],
		"truncMid":  good[:len(good)/2],
	}
	// A stream length claiming gigabytes with no bytes behind it.
	idsLen := blk0 + 56*nb
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
