package tin

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"slices"
	"testing"
)

// segmentCorpus mixes phrases, skewed terms, and varied lengths over
// enough documents that every list seals.
func segmentCorpus(t testing.TB, n int) *Index {
	t.Helper()
	ix := NewIndex()
	for i := 0; i < n; i++ {
		body := "alpha beta gamma"
		if i%3 == 0 {
			body += " alpha alpha"
		}
		if i%7 == 0 {
			body += " zebrastripe"
		}
		for k := 0; k < (i*97)%160; k++ {
			body += " filler"
		}
		ix.Add(DocID(1000+i), body)
	}
	ix.Seal()
	return ix
}

var segmentBattery = []string{
	`alpha`, `zebrastripe`, `nonexistentterm`,
	`alpha AND beta`, `alpha OR zebrastripe`, `alpha AND NOT beta`,
	`"alpha beta"`, `"alpha beta"~1`, `"beta gamma alpha"~2`,
	`alpha THEN/0 beta`, `alpha NEAR/2 gamma`,
	`alpha IN FIRST 3 WORDS`, `gamma IN LAST 2 WORDS`,
	`AT LEAST 2 OF [alpha beta gamma]`, `ALL OF [alpha beta]`,
	`*`, `alpha^2`, `"alpha beta"^1.5`,
	`(alpha OR zebrastripe) AND beta`,
}

// shipRoundTrip exports, wires, unwires, and opens: the receiver must
// answer every query exactly like the sender, and rewiring must be
// byte-stable (sorted map order).
func shipRoundTrip(t *testing.T, sender *Index) (*Index, []byte) {
	t.Helper()
	seg := sender.ExportSegment()
	if len(seg.Lists) == 0 {
		t.Fatal("nothing sealed to ship")
	}
	wire, err := MarshalSegment(seg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := UnmarshalSegment(wire)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	receiver, err := OpenSegment(got)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	wire2, err := MarshalSegment(got)
	if err != nil || !bytes.Equal(wire, wire2) {
		t.Fatal("remarshal unstable")
	}
	return receiver, wire
}

func TestSegmentShipAndQuery(t *testing.T) {
	sender := segmentCorpus(t, 500)
	receiver, _ := shipRoundTrip(t, sender)
	for _, input := range segmentBattery {
		qs, err := sender.ParseTINQL(input)
		if err != nil {
			t.Fatalf("sender parse %q: %v", input, err)
		}
		qr, err := receiver.ParseTINQL(input)
		if err != nil {
			t.Fatalf("receiver parse %q (shipped vocab): %v", input, err)
		}
		if !reflect.DeepEqual(sender.Match(qs, nil), receiver.Match(qr, nil)) {
			t.Fatalf("%q: shipped match differs", input)
		}
		for _, topK := range []int{0, 1, 10} {
			if !reflect.DeepEqual(sender.Score(qs, topK, nil), receiver.Score(qr, topK, nil)) {
				t.Fatalf("%q topK=%d: shipped score differs", input, topK)
			}
		}
	}
	// Pinned statistics agree, so transient row scoring matches too.
	qs, _ := sender.ParseTINQL(`alpha AND beta`)
	qr, _ := receiver.ParseTINQL(`alpha AND beta`)
	var ss, rs ScoreStats
	sender.RefreshScoreStats(qs, &ss)
	receiver.RefreshScoreStats(qr, &rs)
	if !reflect.DeepEqual(ss.idfs, rs.idfs) || ss.avg != rs.avg || ss.nDocs != rs.nDocs {
		t.Fatal("shipped statistics differ")
	}
}

// TestSegmentSparseShipAndQuery covers gapped id spaces end to end:
// far-apart documents with position-heavy texts (so the lists still
// seal), shipped through the delta-gap lens encoding. The battery
// derives from shipped terms only: open lists never ship, so queries
// touching them diverge by design (pinned below).
func TestSegmentSparseShipAndQuery(t *testing.T) {
	sender := NewIndex()
	// Gaps stay under 2^32: wider gaps cannot seal (delta-coded ids),
	// so they never ship — see the segment contract.
	ids := []DocID{7, 1000000, 4000000000}
	for i, id := range ids {
		body := "alpha beta gamma"
		for k := 0; k < 100; k++ {
			body += " alpha"
		}
		for k := 0; k < 500+i; k++ {
			body += " filler"
		}
		sender.Add(id, body)
	}
	sender.Seal()
	seg := sender.ExportSegment()
	if len(seg.Lists) < 2 {
		t.Fatalf("%d sealed lists, want at least 2", len(seg.Lists))
	}
	var terms []string
	for term := range seg.Lists {
		terms = append(terms, seg.Dict[term])
	}
	slices.Sort(terms)
	battery := []string{terms[0], terms[0] + " AND " + terms[1]}
	receiver, _ := shipRoundTrip(t, sender)
	for _, input := range battery {
		qs, err := sender.ParseTINQL(input)
		if err != nil {
			t.Fatal(err)
		}
		qr, err := receiver.ParseTINQL(input)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(sender.Match(qs, nil), receiver.Match(qr, nil)) {
			t.Fatalf("%q: shipped match differs", input)
		}
		if !reflect.DeepEqual(sender.Score(qs, 2, nil), receiver.Score(qr, 2, nil)) {
			t.Fatalf("%q: shipped score differs", input)
		}
	}
	// Open lists never ship: a term left open matches on the sender
	// and empty on the receiver, by contract.
	for term, p := range sender.post {
		if p.sealed != nil {
			continue
		}
		spell := seg.Dict[term]
		qs, err := sender.ParseTINQL(spell)
		if err != nil {
			t.Fatal(err)
		}
		qr, err := receiver.ParseTINQL(spell)
		if err != nil {
			t.Fatal(err)
		}
		if len(sender.Match(qs, nil)) == 0 {
			continue
		}
		if got := receiver.Match(qr, nil); len(got) != 0 {
			t.Fatalf("open term %q matched %v on receiver", spell, got)
		}
		break
	}
}

func TestSegmentRejects(t *testing.T) {
	sender := segmentCorpus(t, 300)
	seg := sender.ExportSegment()
	wire, err := MarshalSegment(seg)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty":   {},
		"short":   wire[:3],
		"magic":   bytes.Clone(wire),
		"version": bytes.Clone(wire),
		"trunc":   wire[:len(wire)/2],
		"tail":    wire[:len(wire)-1],
	}
	cases["magic"][0] ^= 0xff
	cases["version"][4] = 9
	// nDocs field is the first u64 after the 5-byte header.
	tampered := bytes.Clone(wire)
	tampered[5] ^= 0xff
	cases["count"] = tampered
	for name, b := range cases {
		if _, err := UnmarshalSegment(b); err == nil {
			t.Fatalf("%s: corrupt segment accepted", name)
		}
	}
	// Valid wire, impossible accounting: a list larger than the corpus.
	bad := seg
	for term, s := range seg.Lists {
		cp := *s
		cp.n = uint32(seg.NDocs + 1)
		bad.Lists = map[uint64]*sealedPostings{term: &cp}
		break
	}
	if _, err := OpenSegment(bad); err == nil {
		t.Fatal("oversize list accepted")
	}
	if _, err := OpenSegment(nil); err == nil {
		t.Fatal("nil segment accepted")
	}
	if _, err := MarshalSegment(nil); err == nil {
		t.Fatal("nil marshal accepted")
	}
}

// TestLensSection pins the compact length encoding: contiguous and
// sparse round-trips, byte stability, and fail-closed decoding (every
// strict prefix errors, bad modes/gaps/lengths rejected, wraparound
// duplicates caught).
func TestLensSection(t *testing.T) {
	toMap := func(ids []DocID, lens []uint32) map[DocID]uint32 {
		m := make(map[DocID]uint32, len(ids))
		for i, id := range ids {
			m[id] = lens[i]
		}
		return m
	}
	cases := []struct {
		name string
		ids  []DocID
		lens []uint32
	}{
		{"empty", nil, nil},
		{"single", []DocID{42}, []uint32{7}},
		{"contiguous", []DocID{1000, 1001, 1002, 1003}, []uint32{20, 300, 1, 4294967295}},
		{"sparse", []DocID{7, 1000000, 1 << 40}, []uint32{5, 5, 5}},
		{"clustered", []DocID{9, 10, 11, 5000, 5001}, []uint32{100, 2, 30000, 4, 4}},
	}
	for _, c := range cases {
		wire := appendLensSection(nil, c.ids, toMap(c.ids, c.lens))
		r := &sealReader{b: wire}
		n := int(r.u32())
		if n != len(c.ids) {
			t.Fatalf("%s: count %d, want %d", c.name, n, len(c.ids))
		}
		got, err := readLensSection(r, n)
		if err != nil {
			t.Fatalf("%s: decode: %v", c.name, err)
		}
		if !reflect.DeepEqual(got, toMap(c.ids, c.lens)) {
			t.Fatalf("%s: round trip changed lengths", c.name)
		}
		if len(r.b) != 0 {
			t.Fatalf("%s: %d trailing bytes", c.name, len(r.b))
		}
		for i := range wire {
			r := &sealReader{b: wire[:i]}
			n := int(r.u32())
			if r.err != nil {
				continue
			}
			if _, err := readLensSection(r, n); err == nil {
				t.Fatalf("%s: prefix %d accepted", c.name, i)
			}
		}
	}
	// Bad mode byte (offset 4, after the count).
	wire := appendLensSection(nil, []DocID{1, 2}, map[DocID]uint32{1: 1, 2: 2})
	bad := bytes.Clone(wire)
	bad[4] = 2
	r := &sealReader{b: bad}
	_ = r.u32()
	if _, err := readLensSection(r, 2); err == nil {
		t.Fatal("bad mode accepted")
	}
	// Zero gap (gapped ids 10, 20: mode at 4, base at 5..13, gap at 13).
	wire = appendLensSection(nil, []DocID{10, 20, 30}, map[DocID]uint32{10: 1, 20: 1, 30: 1})
	bad = bytes.Clone(wire)
	bad[13] = 0
	r = &sealReader{b: bad}
	_ = r.u32()
	if _, err := readLensSection(r, 3); err == nil {
		t.Fatal("zero gap accepted")
	}
	// Wraparound duplicate: base 0, gaps 2^63, 2^63 wrap back to 0.
	var wrap []byte
	wrap = append(wrap, 3, 0, 0, 0) // count
	wrap = append(wrap, 1)          // gapped
	wrap = append(wrap, 0, 0, 0, 0, 0, 0, 0, 0)
	wrap = binary.AppendUvarint(wrap, 1<<63)
	wrap = binary.AppendUvarint(wrap, 1<<63)
	wrap = binary.AppendUvarint(wrap, 1)
	wrap = binary.AppendUvarint(wrap, 1)
	wrap = binary.AppendUvarint(wrap, 1)
	r = &sealReader{b: wrap}
	_ = r.u32()
	if _, err := readLensSection(r, 3); err == nil {
		t.Fatal("wraparound duplicate accepted")
	}
}

func BenchmarkSegmentShip(b *testing.B) {
	sender := segmentCorpus(b, 8192)
	var sealedTotal uint64
	for _, p := range sender.post {
		if p.sealed != nil {
			sealedTotal += p.sealed.sealedBytes()
		}
	}
	seg := sender.ExportSegment()
	wire, err := MarshalSegment(seg)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(wire))/float64(sealedTotal)*100, "%-of-sealed")
	b.Run("Export", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = sender.ExportSegment()
		}
	})
	b.Run("Marshal", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = MarshalSegment(seg)
		}
	})
	b.Run("Unmarshal", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = UnmarshalSegment(wire)
		}
	})
	b.Run("Open", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, _ := UnmarshalSegment(wire)
			_, _ = OpenSegment(got)
		}
	})
}
