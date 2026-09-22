package tin

import (
	"bytes"
	"reflect"
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
