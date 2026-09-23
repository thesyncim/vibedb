package tin

import (
	"strings"
	"testing"
)

// bytesCorpus exercises both lanes' fold rules: empty, separators only,
// case folding, digits, hyphens, Latin-1, CJK, emoji, malformed bytes, and
// a long document crossing the SIMD fold threshold.
var bytesCorpus = []string{
	"",
	"   ",
	"...,,,",
	"hello world",
	"Hello WORLD MiXeD",
	"well-known co-op re-entry",
	"price $19.99 (sale!) 100%",
	"caf\u00e9 na\u00efve r\u00e9sum\u00e9 \u00c5ngstr\u00f6m",
	"\u00df \u00e6 \u0153 \u00f1",
	"\u4e2d\u6587\u6d4b\u8bd5 \u65e5\u672c\u8a9e \ud55c\uad6d\uc5b4",
	"emoji \U0001F600 test \U0001F50D query",
	"mixed caf\u00e9 \u4e2d\u6587 hello123",
	"\xff\xfe invalid \x80 bytes",
	"tab\tseparated\nnewline\rcarriage",
	strings.Repeat("Alpha Beta GAMMA delta caf\u00e9 ", 20),
}

// collectTokens runs the string lane's emit machine into a slice.
func collectTokens(text string) (pairs []tokPos, spells [][]byte) {
	scanStringRec(text, nil, func(h uint64, p uint32, _ []byte) {
		pairs = append(pairs, tokPos{hash: h, pos: p})
	})
	var buf []byte
	scanStringRec(text, &buf, func(h uint64, p uint32, spelling []byte) {
		cp := make([]byte, len(spelling))
		copy(cp, spelling)
		spells = append(spells, cp)
	})
	return pairs, spells
}

// TestScanBytesAgreesWithScanString locks the byte lane to the string
// lane: identical (hash, pos) streams and identical captured spellings
// over the whole fold corpus. Any drift is a correctness bug, not a
// dialect — Add and AddBytes must analyze identically.
func TestScanBytesAgreesWithScanString(t *testing.T) {
	for _, text := range bytesCorpus {
		buf := []byte(text)
		var want []tokPos
		scanBytes(buf, func(h uint64, p uint32) {
			want = append(want, tokPos{hash: h, pos: p})
		})
		have, haveSpells := collectTokens(text)
		if len(want) != len(have) {
			t.Fatalf("text %q: byte tokens %d, string %d", text, len(want), len(have))
		}
		for i := range have {
			if want[i] != have[i] {
				t.Fatalf("text %q token %d: byte %+v, string %+v", text, i, want[i], have[i])
			}
		}
		var gotSpells [][]byte
		var staging []byte
		scanBytesRec(buf, &staging, func(h uint64, p uint32, spelling []byte) {
			cp := make([]byte, len(spelling))
			copy(cp, spelling)
			gotSpells = append(gotSpells, cp)
		})
		if len(gotSpells) != len(haveSpells) {
			t.Fatalf("text %q: byte spellings %d, string %d", text, len(gotSpells), len(haveSpells))
		}
		for i := range haveSpells {
			if string(gotSpells[i]) != string(haveSpells[i]) {
				t.Fatalf("text %q spelling %d: byte %q, string %q", text, i, gotSpells[i], haveSpells[i])
			}
		}
	}
}

// TestScanPairsBytesAgrees locks scanPairsBytes to scanPairs, including
// warmed scratch reuse without drift.
func TestScanPairsBytesAgrees(t *testing.T) {
	for _, text := range bytesCorpus {
		buf := []byte(text)
		want := scanPairs(text, nil)
		got := scanPairsBytes(buf, nil)
		if len(got) != len(want) {
			t.Fatalf("text %q: byte pairs %d, string %d", text, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("text %q pair %d: byte %+v, string %+v", text, i, got[i], want[i])
			}
		}
		reused := scanPairsBytes(buf, got[:0])
		for i := range want {
			if reused[i] != want[i] {
				t.Fatalf("text %q reused pair %d: byte %+v, string %+v", text, i, reused[i], want[i])
			}
		}
	}
}

// untilCase is one early-exit scenario: text plus a query whose needed set
// is derived from the query itself.
type untilCase struct {
	text  string
	query Query
}

// TestScanPairsUntilAgrees locks the byte early-exit machine to the string
// one: identical pair streams, found sets, stop flags, and verdicts,
// covering exits, fall-throughs, and missing terms.
func TestScanPairsUntilAgrees(t *testing.T) {
	h := func(term string) uint64 {
		hh, n := FoldTerm(term)
		if n != 1 {
			t.Fatalf("FoldTerm(%q) scanned %d tokens", term, n)
		}
		return hh
	}
	cases := []untilCase{
		{"alpha beta gamma", Query{Op: OpTerm, Term: h("beta")}},
		{"alpha beta gamma", Query{Op: OpTerm, Term: h("missing")}},
		{"alpha beta gamma", Query{Op: OpOr, Kids: []Query{{Op: OpTerm, Term: h("missing")}, {Op: OpTerm, Term: h("gamma")}}}},
		{"alpha beta gamma", Query{Op: OpAnd, Kids: []Query{{Op: OpTerm, Term: h("alpha")}, {Op: OpTerm, Term: h("gamma")}}}},
		{"alpha beta gamma", Query{Op: OpAnd, Kids: []Query{{Op: OpTerm, Term: h("alpha")}, {Op: OpTerm, Term: h("missing")}}}},
		{"alpha alpha beta", Query{Op: OpTerm, Term: h("alpha")}},
		{"", Query{Op: OpTerm, Term: h("alpha")}},
		{"caf\u00e9 \u4e2d\u6587 beta", Query{Op: OpOr, Kids: []Query{{Op: OpTerm, Term: h("caf\u00e9")}, {Op: OpTerm, Term: h("beta")}}}},
	}
	for _, c := range cases {
		var neededBuf [stopCap]uint64
		needed, ok := collectTerms(&c.query, neededBuf[:0])
		if !ok || len(needed) == 0 {
			t.Fatalf("query %+v: no stop set", c.query)
		}
		var f1, f2 [stopCap]tokPos
		wOut, wFound, wStop, wVerdict := scanPairsUntil(c.text, nil, c.query, needed, f1[:0])
		gOut, gFound, gStop, gVerdict := scanPairsUntilBytes([]byte(c.text), nil, c.query, needed, f2[:0])
		if wStop != gStop || wVerdict != gVerdict {
			t.Fatalf("text %q: string (stop=%v verdict=%v), byte (stop=%v verdict=%v)",
				c.text, wStop, wVerdict, gStop, gVerdict)
		}
		if len(wOut) != len(gOut) || len(wFound) != len(gFound) {
			t.Fatalf("text %q: string (%d pairs, %d found), byte (%d pairs, %d found)",
				c.text, len(wOut), len(wFound), len(gOut), len(gFound))
		}
		for i := range wOut {
			if wOut[i] != gOut[i] {
				t.Fatalf("text %q pair %d: string %+v, byte %+v", c.text, i, wOut[i], gOut[i])
			}
		}
		for i := range wFound {
			if wFound[i] != gFound[i] {
				t.Fatalf("text %q found %d: string %+v, byte %+v", c.text, i, wFound[i], gFound[i])
			}
		}
	}
}

// TestFoldBytesAgrees locks the byte fold to the string fold over every
// byte value (including >= 0x80 passthrough), prose, and empty input — on
// the dispatched impl (wide under simd, scalar elsewhere) and on the
// scalar pair explicitly, so the fallback can never drift either.
func TestFoldBytesAgrees(t *testing.T) {
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	inputs := [][]byte{
		{},
		[]byte(""),
		[]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#"),
		[]byte("MiXeD Case With Spaces And\ttabs"),
		all,
		[]byte(strings.Repeat("The Quick Brown Fox Jumps Over ", 40)),
		[]byte("caf\u00e9 \u4e2d\u6587 \U0001F600 mixed"),
	}
	for _, in := range inputs {
		want := make([]byte, len(in))
		foldASCII(want, string(in))
		got := make([]byte, len(in))
		foldASCIIBytes(got, in)
		if string(got) != string(want) {
			t.Fatalf("len %d: byte fold %q, string fold %q", len(in), got, want)
		}
		scalar := make([]byte, len(in))
		foldASCIIScalar(scalar, string(in))
		scalarBytes := make([]byte, len(in))
		foldASCIIScalarBytes(scalarBytes, in)
		if string(scalarBytes) != string(scalar) {
			t.Fatalf("len %d: scalar lanes disagree", len(in))
		}
	}
}

// addBytesDocs covers both ingest lanes: empty, short, fold-lane long
// (>256B, multibyte inside), fold-boundary sizes, and replacement.
func addBytesDocs() []string {
	return []string{
		"fuji apple juicy red pie",
		"",
		"Hello WORLD café \u4e2d\u6587",
		strings.Repeat("Alpha Beta GAMMA delta café ", 20),
		strings.Repeat("ab ", 85),
		strings.Repeat("ab ", 85) + "c",
		"punctuation ...,,, and $19.99 (sale!)",
	}
}

// TestAddBytesAgreesWithAdd proves mixed-lane indexes indistinguishable:
// string-built and bytes-built indexes (open and sealed) answer every
// query shape with identical verdicts and scores, including the 2-term
// phrase fast path and id replacement across lanes.
func TestAddBytesAgreesWithAdd(t *testing.T) {
	docs := addBytesDocs()
	strIx, byteIx := NewIndex(), NewIndex()
	for id, text := range docs {
		strIx.Add(DocID(id+1), text)
		byteIx.AddBytes(DocID(id+1), []byte(text))
	}
	// Cross-lane replacement: string doc replaced by bytes and back.
	strIx.AddBytes(1, []byte("replaced through the byte lane fuji"))
	byteIx.Add(1, "replaced through the byte lane fuji")
	strIx.Add(2, "replaced back through strings")
	byteIx.AddBytes(2, []byte("replaced back through strings"))

	inputs := []string{
		`fuji`, `apple`, `replaced`, `missing`,
		`fuji AND apple`, `apple OR missing`, `apple AND NOT pie`,
		`"fuji apple"`, `"byte lane"`, `"Alpha Beta"`,
		`[apple fuji replaced]`, `AT LEAST 2 OF [apple fuji replaced]`,
		`apple THEN/0 fuji`, `appl*`, `CONTAINS apple`,
	}
	check := func(a, b *Index, name string) {
		for _, input := range inputs {
			qa, err := a.ParseTINQL(input)
			if err != nil {
				t.Fatalf("%s: ParseTINQL(%q): %v", name, input, err)
			}
			qb, err := b.ParseTINQL(input)
			if err != nil {
				t.Fatalf("%s: ParseTINQL(%q): %v", name, input, err)
			}
			ma, mb := a.Match(qa, nil), b.Match(qb, nil)
			if len(ma) != len(mb) {
				t.Fatalf("%s %q: string-lane %v, byte-lane %v", name, input, ma, mb)
			}
			for i := range ma {
				if ma[i] != mb[i] {
					t.Fatalf("%s %q: string-lane %v, byte-lane %v", name, input, ma, mb)
				}
			}
			sa, sb := a.Score(qa, 0, nil), b.Score(qb, 0, nil)
			if len(sa) != len(sb) {
				t.Fatalf("%s %q scores: %d vs %d entries", name, input, len(sa), len(sb))
			}
			for i := range sa {
				if sa[i] != sb[i] {
					t.Fatalf("%s %q score %d: %+v vs %+v", name, input, i, sa[i], sb[i])
				}
			}
		}
		if a.Docs() != b.Docs() {
			t.Fatalf("%s: doc counts %d vs %d", name, a.Docs(), b.Docs())
		}
	}
	check(strIx, byteIx, "open")
	strIx.Seal()
	byteIx.Seal()
	check(strIx, byteIx, "sealed")
}

// TestAddBytesReusesBuffer proves the no-retention contract: one buffer
// serves consecutive documents, mutation between calls leaves indexed
// documents (postings and vocabulary spellings) intact.
func TestAddBytesReusesBuffer(t *testing.T) {
	ix := NewIndex()
	buf := []byte("alpha uniqueone beta")
	ix.AddBytes(1, buf)
	for i := range buf {
		buf[i] = 'x'
	}
	copy(buf, "gamma uniquetwo delta")
	ix.AddBytes(2, buf[:20])
	if got := ix.Match(Query{Op: OpTerm, Term: mustHash(t, "uniqueone")}, nil); len(got) != 1 || got[0] != 1 {
		t.Fatalf("uniqueone matched %v, want [1]", got)
	}
	if got := ix.Match(Query{Op: OpTerm, Term: mustHash(t, "uniquetwo")}, nil); len(got) != 1 || got[0] != 2 {
		t.Fatalf("uniquetwo matched %v, want [2]", got)
	}
	// The vocabulary owns its spellings: the buffer now holds doc 2's
	// bytes, but doc 1's spelling survives verbatim.
	if spell := ix.dict[mustHash(t, "uniqueone")]; spell != "uniqueone" {
		t.Fatalf("dict spelling %q, want %q", spell, "uniqueone")
	}
	// Positions survive too: the exact bigram still resolves to doc 1.
	q := Query{Op: OpPhrase, Phrase: []PhrasePos{
		{Alts: []uint64{mustHash(t, "alpha")}},
		{Alts: []uint64{mustHash(t, "uniqueone")}},
	}}
	if got := ix.Match(q, nil); len(got) != 1 || got[0] != 1 {
		t.Fatalf("phrase matched %v, want [1]", got)
	}
}

// TestTransientBytesAgree locks the transient byte lanes to the string
// lanes over the whole operator corpus: identical verdicts, bit-identical
// scores.
func TestTransientBytesAgree(t *testing.T) {
	ix := NewIndex()
	for id, text := range scoreCorpusDocs {
		ix.Add(DocID(id+1), text)
	}
	var scratchS, scratchB TextScratch
	var st ScoreStats
	for _, input := range scoreCorpusInputs {
		q, err := ix.ParseTINQL(input)
		if err != nil {
			t.Fatalf("ParseTINQL(%q): %v", input, err)
		}
		ix.RefreshScoreStats(q, &st)
		matched := ix.Match(q, nil)
		inSet := make(map[DocID]bool, len(matched))
		for _, id := range matched {
			inSet[id] = true
		}
		for id, text := range scoreCorpusDocs {
			buf := []byte(text)
			if got, want := MatchSingleBytes(buf, q, &scratchB), MatchSingle(text, q, &scratchS); got != want {
				t.Fatalf("%q doc %d: byte=%v string=%v", input, id+1, got, want)
			}
			if inSet[DocID(id+1)] != MatchSingleBytes(buf, q, &scratchB) {
				t.Fatalf("%q doc %d: transient disagrees with index", input, id+1)
			}
			gs, ws := ScoreSingleBytes(buf, q, &st, &scratchB), ScoreSingle(text, q, &st, &scratchS)
			if gs != ws && !scoresClose(gs, ws) {
				t.Fatalf("%q doc %d: byte score %v, string score %v", input, id+1, gs, ws)
			}
		}
	}
}

// TestBytesLanesZeroAlloc pins the warm budget of every new byte lane:
// scanning, folding, transient matching, and transient scoring over a
// reused buffer touch nothing on the heap.
func TestBytesLanesZeroAlloc(t *testing.T) {
	buf := []byte("luxury vintage watches fuji apple")
	scratchBuf := []byte("luxury vintage watches fuji apple")
	scratchPairs = scanPairsBytes(scratchBuf, scratchPairs)
	if n := testing.AllocsPerRun(50, func() {
		scratchPairs = scanPairsBytes(scratchBuf, scratchPairs[:0])
	}); n != 0 {
		t.Fatalf("scanPairsBytes allocated %v per run, want 0", n)
	}
	dst := make([]byte, len(buf))
	if n := testing.AllocsPerRun(50, func() {
		foldASCIIBytes(dst, buf)
	}); n != 0 {
		t.Fatalf("foldASCIIBytes allocated %v per run, want 0", n)
	}
	ix := NewIndex()
	ix.Add(1, "luxury vintage watches")
	q, err := ix.ParseTINQL("luxury AND vintage")
	if err != nil {
		t.Fatal(err)
	}
	var scratch TextScratch
	if n := testing.AllocsPerRun(50, func() {
		MatchSingleBytes(buf[:22], q, &scratch)
	}); n != 0 {
		t.Fatalf("MatchSingleBytes allocated %v per run, want 0", n)
	}
	stats := ix.RefreshScoreStats(q, nil)
	if n := testing.AllocsPerRun(50, func() {
		ScoreSingleBytes(buf[:22], q, stats, &scratch)
	}); n != 0 {
		t.Fatalf("ScoreSingleBytes allocated %v per run, want 0", n)
	}
}

var scratchPairs []tokPos

// FuzzByteLanesAgree fuzzes the lane lockstep: arbitrary bytes (the
// fuzzer lives for multibyte boundaries and malformed sequences) must
// tokenize, spell, pair, fold, and early-exit identically on both lanes.
// The seed corpus runs under plain `go test`; extended exploration is a
// `go test -fuzz` away. Inputs are capped so one worker stays quick.
func FuzzByteLanesAgree(f *testing.F) {
	for _, s := range bytesCorpus {
		f.Add([]byte(s))
	}
	f.Add([]byte{0xff, 0xfe, 0x80, 0xc3})
	f.Add([]byte("a"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 2048 {
			t.Skip()
		}
		text := string(data)
		var wantPairs []tokPos
		scanBytes(data, func(h uint64, p uint32) {
			wantPairs = append(wantPairs, tokPos{hash: h, pos: p})
		})
		var havePairs []tokPos
		scanString(text, func(h uint64, p uint32) {
			havePairs = append(havePairs, tokPos{hash: h, pos: p})
		})
		if len(wantPairs) != len(havePairs) {
			t.Fatalf("%d bytes: byte tokens %d, string %d", len(data), len(wantPairs), len(havePairs))
		}
		for i := range havePairs {
			if wantPairs[i] != havePairs[i] {
				t.Fatalf("%d bytes token %d: byte %+v, string %+v", len(data), i, wantPairs[i], havePairs[i])
			}
		}
		if got, want := scanPairsBytes(data, nil), scanPairs(text, nil); len(got) != len(want) {
			t.Fatalf("%d bytes: byte pairs %d, string %d", len(data), len(got), len(want))
		} else {
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("%d bytes pair %d: byte %+v, string %+v", len(data), i, got[i], want[i])
				}
			}
		}
		foldWant := make([]byte, len(data))
		foldASCII(foldWant, text)
		foldGot := make([]byte, len(data))
		foldASCIIBytes(foldGot, data)
		if string(foldGot) != string(foldWant) {
			t.Fatalf("%d bytes: fold lanes disagree", len(data))
		}
		// Early-exit lockstep on queries derived from the input's own
		// tokens, so exits, fall-throughs, and misses all occur.
		var queries []Query
		if len(wantPairs) > 0 {
			queries = append(queries, Query{Op: OpTerm, Term: wantPairs[0].hash})
			if len(wantPairs) > 1 {
				queries = append(queries, Query{Op: OpOr, Kids: []Query{
					{Op: OpTerm, Term: wantPairs[0].hash},
					{Op: OpTerm, Term: wantPairs[1].hash},
				}})
				queries = append(queries, Query{Op: OpAnd, Kids: []Query{
					{Op: OpTerm, Term: wantPairs[0].hash},
					{Op: OpTerm, Term: wantPairs[1].hash},
				}})
			}
		}
		queries = append(queries, Query{Op: OpTerm, Term: 0x9E3779B97F4A7C15})
		for _, q := range queries {
			var neededBuf [stopCap]uint64
			needed, ok := collectTerms(&q, neededBuf[:0])
			if !ok || len(needed) == 0 {
				continue
			}
			var f1, f2 [stopCap]tokPos
			wOut, wFound, wStop, wVerdict := scanPairsUntil(text, nil, q, needed, f1[:0])
			gOut, gFound, gStop, gVerdict := scanPairsUntilBytes(data, nil, q, needed, f2[:0])
			if wStop != gStop || wVerdict != gVerdict || len(wOut) != len(gOut) || len(wFound) != len(gFound) {
				t.Fatalf("%d bytes op %d: string (%d,%d,%v,%v) byte (%d,%d,%v,%v)",
					len(data), q.Op, len(wOut), len(wFound), wStop, wVerdict,
					len(gOut), len(gFound), gStop, gVerdict)
			}
			for i := range wOut {
				if wOut[i] != gOut[i] {
					t.Fatalf("%d bytes op %d pair %d: string %+v, byte %+v", len(data), q.Op, i, wOut[i], gOut[i])
				}
			}
		}
	})
}

// buildBenchTexts mixes short, fold-lane long, multibyte, and empty
// documents for the ingest A/B.
var buildBenchTexts = []string{
	"fuji apple juicy red pie",
	"Hello WORLD café \u4e2d\u6587",
	"",
	strings.Repeat("Alpha Beta GAMMA delta café ", 20),
	strings.Repeat("the quick brown fox jumps over the lazy dog ", 12),
	"punctuation ...,,, and $19.99 (sale!)",
}

// BenchmarkAddBuildLane is the ingest A/B: full index builds over the same
// documents through Add, through AddBytes from per-document buffers, and
// through AddBytes from one reused buffer (copy included, so the reuse
// story pays its honest price).
func BenchmarkAddBuildLane(b *testing.B) {
	bufs := make([][]byte, len(buildBenchTexts))
	for i, s := range buildBenchTexts {
		bufs[i] = []byte(s)
	}
	b.Run("string", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ix := NewIndex()
			for id, s := range buildBenchTexts {
				ix.Add(DocID(id+1), s)
			}
		}
	})
	b.Run("bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ix := NewIndex()
			for id, buf := range bufs {
				ix.AddBytes(DocID(id+1), buf)
			}
		}
	})
	b.Run("bytes-reuse", func(b *testing.B) {
		var scratch []byte
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ix := NewIndex()
			for id, buf := range bufs {
				if cap(scratch) < len(buf) {
					scratch = make([]byte, len(buf))
				}
				scratch = scratch[:len(buf)]
				copy(scratch, buf)
				ix.AddBytes(DocID(id+1), scratch)
			}
		}
	})
}

// BenchmarkScanPairsBytes mirrors BenchmarkScanPairs for the A/B: same
// documents, same metric, byte lane.
func BenchmarkScanPairsBytes(b *testing.B) {
	for name, doc := range scanBenchDocs {
		b.Run(name, func(b *testing.B) {
			buf := []byte(doc)
			var out []tokPos
			out = scanPairsBytes(buf, out)
			b.SetBytes(int64(len(buf)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out = scanPairsBytes(buf, out[:0])
			}
			_ = out
		})
	}
}
