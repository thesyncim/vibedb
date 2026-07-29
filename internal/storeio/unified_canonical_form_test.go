package storeio

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// buildTestIndex builds a tape over src, growing storage on ErrIndexFull the
// way production builders do.
func buildTestIndex(t testing.TB, src []byte) vibejson.Index {
	t.Helper()
	storage := make([]vibejson.IndexEntry, 0, 64)
	for {
		index, err := vibejson.BuildIndex(src, storage)
		if err == nil {
			return index
		}
		if !errors.Is(err, document.ErrIndexFull) {
			t.Fatalf("BuildIndex(%q): %v", src, err)
		}
		storage = make([]vibejson.IndexEntry, 0, 2*cap(storage)+64)
	}
}

// checkCanonicalAgainstLibrary pins the three §8 differential properties for
// one document: the tape render equals AppendCanonicalize, the render is
// idempotent, and IndexIsCanonical is exactly the already-canonical
// predicate (true iff the source equals its own canonicalization).
func checkCanonicalAgainstLibrary(t *testing.T, ws *CanonicalWorkspace, src []byte) {
	t.Helper()
	want, err := vibejson.AppendCanonicalize(nil, src)
	if err != nil {
		t.Fatalf("AppendCanonicalize(%q): %v", src, err)
	}
	index := buildTestIndex(t, src)
	got, err := AppendCanonicalIndexed(nil, index, ws)
	if err != nil {
		t.Fatalf("AppendCanonicalIndexed(%q): %v", src, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("canonical render diverges from library:\n src  %q\n got  %q\n want %q", src, got, want)
	}
	if gotCheck, wantCheck := IndexIsCanonical(index, ws), bytes.Equal(src, want); gotCheck != wantCheck {
		t.Fatalf("IndexIsCanonical(%q) = %v, want %v (canonical form %q)", src, gotCheck, wantCheck, want)
	}
	// Idempotence: rendering the canonical bytes reproduces them and the
	// fast check accepts them, so journal replay of canonical bytes is a
	// fixed point (§3.2, §7.5).
	canonIndex := buildTestIndex(t, want)
	again, err := AppendCanonicalIndexed(nil, canonIndex, ws)
	if err != nil {
		t.Fatalf("AppendCanonicalIndexed(canonical %q): %v", want, err)
	}
	if !bytes.Equal(again, want) {
		t.Fatalf("canonical render not idempotent:\n once  %q\n twice %q", want, again)
	}
	if !IndexIsCanonical(canonIndex, ws) {
		t.Fatalf("IndexIsCanonical rejects its own render: %q", want)
	}
}

// competitiveShapeJSON is one document of the ~250 B competitive-corpus
// shape (bench/competitive/corpus.go CorpusOf), reproduced literally because
// that module is separate; the source shape is not key-sorted, so this
// exercises the sorting render.
const competitiveShapeJSON = "{\"id\":7,\"name\":\"user-7\",\"country\":\"DE\",\"score\":481,\"active\":true," +
	"\"profile\":{\"tier\":\"pro\",\"region\":\"eu-west-1\",\"joined\":\"2020-04-17\"}," +
	"\"tags\":[\"alpha\",\"beta\"],\"note\":\"steady state, no anomalies observed in the last reporting window\"}"

func TestCanonicalRenderHandcrafted(t *testing.T) {
	ws := &CanonicalWorkspace{}
	cases := []string{
		// Scalar roots: every kind, verbatim number spellings.
		"null", "true", "false", "0", "-0", "1e9", "1E+9", "-0.5",
		"\"x\"", "\"\"", "123456789012345678901234567890",
		// Empty containers, including whitespace-carrying spellings that
		// must collapse to the pinned scalar-leaf forms (design §3.2).
		"{}", "[]", "{ }", "[ ]", "{\"a\":{ },\"b\":[  ]}",
		// Member sorting by decoded byte order, nesting, arrays in order.
		"{\"b\":1,\"a\":2}", "{\"a\":2,\"b\":1}",
		"{\"b\":{\"d\":1,\"c\":2},\"a\":[3,2,1]}",
		"[{\"z\":1,\"y\":2},{\"y\":1,\"z\":2}]",
		// Duplicate keys retained in original relative order (§3.2): the
		// first two are different documents at rest.
		"{\"a\":1,\"a\":2}", "{\"a\":2,\"a\":1}",
		"{\"b\":0,\"a\":1,\"a\":2,\"a\":3}",
		// Whitespace stripping.
		"  {\n\t\"b\" : 1 ,\r\n \"a\" : [ 1 , 2 ] }  ",
		// Escape normalization: over-escaped spellings collapse, mandatory
		// escapes keep their short forms, hex is lowercase, solidus stays
		// raw.
		"\"\\u0041\"", "\"\\/\"", "\"/\"", "\"\\n\"", "\"\\u000a\"",
		"\"\\u000A\"", "\"\\t\\r\\b\\f\"", "\"\\\"\"", "\"\\\\\"",
		"\"\\u0022\"", "\"\\u005C\"", "\"\\u0000\"", "\"\\u001f\"",
		"\"\\u001F\"",
		// Surrogate pairs decode to raw characters; unpaired surrogates are
		// rejected at admission (§3.2) and cannot occur.
		"\"\\uD83D\\uDE00\"", "\"\U0001F600\"", "{\"\U0001F600\":1}",
		// Raw U+2028/U+2029 stay raw in the pinned library revision, and
		// their escaped spellings normalize to the raw characters (see
		// appendCanonicalQuoted's provenance note).
		"\"a b\"", "\"a b\"", "\"\\u2028\"", "\"\\u2029\"",
		// Escaped keys sort by their *decoded* spelling: "c" decodes
		// to "c" and must sort after "b" even though its raw first byte
		// '\\' precedes 'b'.
		"{\"\\u0063\":1,\"b\":2}", "{\"b\":1,\"\\u0061\":2}",
		// Multi-byte UTF-8 keys and values pass through raw.
		"{\"é\":1,\"→\":2,\"z\":3}", "\"héllo → wörld\"",
		competitiveShapeJSON,
	}
	for _, src := range cases {
		checkCanonicalAgainstLibrary(t, ws, []byte(src))
	}
}

// appendRandomCanonicalTestValue writes one random valid JSON value,
// deliberately mixing canonical and non-canonical spellings: unsorted and
// duplicate keys, whitespace, over-escaping, varied number spellings.
func appendRandomCanonicalTestValue(dst []byte, rng *rand.Rand, depth int) []byte {
	keys := []string{
		"a", "b", "aa", "ab", "z", "id", "name", "é", "\\u0061",
		"\\n", "k\\u002fx", "→", "", "score", "active", "A", "Z",
		"\\uD83D\\uDE00",
	}
	strs := []string{
		"", "x", "hello world", "\\u0041\\u005A", "line\\nbreak", "\\/",
		"emoji \\ud83d\\ude00", "é→", "tab\\there", "\\u001f",
		"q\\\"q", "a b",
	}
	nums := []string{
		"0", "-0", "1", "-1", "42", "1234567890123", "0.5", "-3.25",
		"1e9", "1E+9", "2.5e-3", "999999999999999999",
		"1000000000000000000", "123456789012345678901234567890",
	}
	pad := func(dst []byte) []byte {
		for rng.IntN(3) == 0 {
			dst = append(dst, " \t\n\r"[rng.IntN(4)])
		}
		return dst
	}
	dst = pad(dst)
	kind := rng.IntN(7)
	if depth <= 0 && kind >= 5 {
		kind = rng.IntN(5)
	}
	switch kind {
	case 0:
		dst = append(dst, "null"...)
	case 1:
		if rng.IntN(2) == 0 {
			dst = append(dst, "true"...)
		} else {
			dst = append(dst, "false"...)
		}
	case 2:
		dst = append(dst, nums[rng.IntN(len(nums))]...)
	case 3, 4:
		dst = append(dst, '"')
		dst = append(dst, strs[rng.IntN(len(strs))]...)
		dst = append(dst, '"')
	case 5:
		n := rng.IntN(5)
		dst = append(dst, '[')
		for i := 0; i < n; i++ {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendRandomCanonicalTestValue(dst, rng, depth-1)
		}
		dst = pad(dst)
		dst = append(dst, ']')
	default:
		n := rng.IntN(6)
		dst = append(dst, '{')
		for i := 0; i < n; i++ {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = pad(dst)
			dst = append(dst, '"')
			dst = append(dst, keys[rng.IntN(len(keys))]...)
			dst = append(dst, '"')
			dst = pad(dst)
			dst = append(dst, ':')
			dst = appendRandomCanonicalTestValue(dst, rng, depth-1)
		}
		dst = pad(dst)
		dst = append(dst, '}')
	}
	return pad(dst)
}

// TestCanonicalRenderDifferentialGenerative sweeps randomized documents —
// duplicate keys arise naturally from the small key pool — through the
// library differential. Seeds are fixed so a failure replays.
func TestCanonicalRenderDifferentialGenerative(t *testing.T) {
	ws := &CanonicalWorkspace{}
	rng := rand.New(rand.NewPCG(0x5EED, 0xD1FF))
	for i := 0; i < 4000; i++ {
		src := appendRandomCanonicalTestValue(nil, rng, 4)
		checkCanonicalAgainstLibrary(t, ws, src)
	}
}

// TestCanonicalRenderWideObject pushes past the insertion-sort threshold so
// the merge path is exercised, with duplicates to prove its stability.
func TestCanonicalRenderWideObject(t *testing.T) {
	ws := &CanonicalWorkspace{}
	var src []byte
	src = append(src, '{')
	rng := rand.New(rand.NewPCG(7, 11))
	for i := 0; i < 200; i++ {
		if i > 0 {
			src = append(src, ',')
		}
		// Roughly four duplicates per key on average; distinct values keep
		// the stable order observable in the output.
		src = append(src, '"', 'k')
		src = strconv.AppendInt(src, int64(rng.IntN(50)), 10)
		src = append(src, '"', ':')
		src = strconv.AppendInt(src, int64(i), 10)
	}
	src = append(src, '}')
	checkCanonicalAgainstLibrary(t, ws, src)
}

func TestCanonicalCheckRejectsNonCanonical(t *testing.T) {
	ws := &CanonicalWorkspace{}
	cases := []string{
		" {}", "{} ", "{\"a\" :1}", "{\"a\": 1}", "{\"b\":1,\"a\":2}",
		"[1, 2]", "{ }", "[ ]",
		"\"\\u0041\"", "\"\\/\"", "\"\\u000A\"", "\"\\u000a\"",
		"\"\\u2028\"", "\"\\u2029\"", "\"\\uD83D\\uDE00\"", "\"\\u001F\"",
		// Decoded-key order violation behind canonical-looking spellings.
		"{\"\\u0063\":1,\"b\":2}",
	}
	for _, src := range cases {
		index := buildTestIndex(t, []byte(src))
		if IndexIsCanonical(index, ws) {
			t.Errorf("IndexIsCanonical(%q) = true, want false", src)
		}
	}
	if IndexIsCanonical(vibejson.Index{}, ws) {
		t.Error("IndexIsCanonical(zero index) = true, want false")
	}
}

func TestCanonicalRenderEmptyTapeFailsClosed(t *testing.T) {
	ws := &CanonicalWorkspace{}
	if _, err := AppendCanonicalIndexed(nil, vibejson.Index{}, ws); err == nil {
		t.Fatal("AppendCanonicalIndexed(zero index) succeeded, want error")
	}
}

// TestCanonicalRenderZeroAllocs pins the U0 gate's allocation half: once the
// workspace has warmed, neither the render nor the check allocates
// (zero-GC directive; design §11 U0 row).
func TestCanonicalRenderZeroAllocs(t *testing.T) {
	ws := &CanonicalWorkspace{}
	src := []byte(competitiveShapeJSON)
	index := buildTestIndex(t, src)
	dst := make([]byte, 0, 4*len(src))
	var err error
	dst, err = AppendCanonicalIndexed(dst[:0], index, ws)
	if err != nil {
		t.Fatal(err)
	}
	canonIndex := buildTestIndex(t, append([]byte(nil), dst...))
	if allocs := testing.AllocsPerRun(200, func() {
		dst, _ = AppendCanonicalIndexed(dst[:0], index, ws)
	}); allocs != 0 {
		t.Errorf("AppendCanonicalIndexed allocates %.1f/op, want 0", allocs)
	}
	ok := true
	if allocs := testing.AllocsPerRun(200, func() {
		ok = ok && IndexIsCanonical(canonIndex, ws)
	}); allocs != 0 {
		t.Errorf("IndexIsCanonical allocates %.1f/op, want 0", allocs)
	}
	if !ok {
		t.Error("IndexIsCanonical rejected the canonical render")
	}
}
