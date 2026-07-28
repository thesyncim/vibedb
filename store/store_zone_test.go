package store

import (
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// Unit evidence for the chunk-summary tier. The query-level differential
// (query/zone_differential_test.go) proves that pruning never changes a
// result; these tests prove the two properties that differential rests on, at
// the level where a violation is a one-line diagnosis instead of a missing row
// three layers up:
//
//  1. the zone code is monotone with respect to the query's exact-decimal
//     total order, so min/max bounds cannot be crossed;
//  2. a folded summary covers every live document in its chunk, so a bound
//     that says "no match here" is telling the truth.

// zoneCodeOfJSON is the fold-side classification of one raw JSON value,
// returning the interval the value's code is known to lie in.
func zoneCodeOfJSON(t testing.TB, raw string) (lo, hi uint32, flag uint8) {
	t.Helper()
	return zoneValueCodes([]byte(raw))
}

// Given every ordered pair of JSON numbers in a corpus that straddles
// float64's exact-integer limit, when both are folded, then the code order
// never contradicts exact decimal order. A single inversion here is a licence
// to prune a chunk that matches.
func TestZoneNumberCodeIsMonotoneOverExactDecimals(t *testing.T) {
	numbers := []string{
		"-1e308", "-9007199254740994", "-9007199254740993", "-9007199254740992",
		"-1000000", "-1.5", "-1", "-0.5", "-1e-320", "-0", "0", "0.0", "1e-320",
		"0.5", "1", "1.0", "1e0", "1.5", "2", "1000000",
		"9007199254740992", "9007199254740993", "9007199254740994",
		"9007199254740995", "18014398509481984", "1e308", "1e309",
		"123456789012345678901234567890",
	}
	exact := make([]*big.Float, len(numbers))
	codes := make([]uint32, len(numbers))
	for i, n := range numbers {
		f, _, err := big.ParseFloat(n, 10, 300, big.ToNearestEven)
		if err != nil {
			t.Fatalf("ParseFloat(%s): %v", n, err)
		}
		exact[i] = f
		code, ok := ZoneCodeNumber([]byte(n))
		if !ok {
			t.Fatalf("ZoneCodeNumber(%s) declined", n)
		}
		codes[i] = code
	}
	for i := range numbers {
		for j := range numbers {
			// The reference order is math/big's, not float64's: comparing
			// through float64 would silently agree with a bug that rounds.
			if exact[i].Cmp(exact[j]) <= 0 && codes[i] > codes[j] {
				t.Fatalf("code order contradicts decimal order: %s (code %#x) > %s (code %#x)",
					numbers[i], codes[i], numbers[j], codes[j])
			}
		}
	}
}

// Given the mixed-kind values the query's total order ranks (bool < number <
// string < container), when each is folded, then the code intervals respect
// that ranking. A schemaless path holds different kinds in different
// documents, and a comparison against a number must not prune a chunk whose
// only value there is a string.
func TestZoneCodeRespectsCrossKindOrder(t *testing.T) {
	ordered := []string{
		"false", "true",
		"-1e308", "0", "1", "1e308",
		`""`, `"a"`, `"ab"`, `"abcd"`, `"abcde"`, `"b"`, `"z"`,
		// Containers order by their raw bytes, so "[1]" precedes "[]" ('1' is
		// below ']') and every object follows every array ('{' is above '[').
		`[1]`, `[]`, `{"a":1}`,
	}
	previousHi := uint32(0)
	for i, raw := range ordered {
		lo, hi, flag := zoneCodeOfJSON(t, raw)
		if flag != zoneFlagValue {
			t.Fatalf("%s: flag %d, want a comparable value", raw, flag)
		}
		if i > 0 && lo < previousHi {
			t.Fatalf("%s: lo %#x below the previous value's hi %#x", raw, lo, previousHi)
		}
		previousHi = hi
	}
}

// Given a string whose escape falls inside the four bytes the code keeps, when
// it is folded, then the interval brackets the code of its decoded form.
// Truncating at the backslash without widening would put an escaped string
// outside its own chunk's bounds.
func TestZoneEscapedStringFoldsToABracketingInterval(t *testing.T) {
	cases := []struct{ raw, decoded string }{
		{`"ab"`, "ab"},
		{`"\u0000"`, "\x00"},
		{`"ab\ncd"`, "ab\ncd"},
		{`"abc\td"`, "abc\td"},
		{`"abcd\te"`, "abcd\te"},
		{`"\\"`, `\`},
	}
	for _, c := range cases {
		lo, hi, _ := zoneCodeOfJSON(t, c.raw)
		want := ZoneCodeString(c.decoded)
		if want < lo || want > hi {
			t.Fatalf("%s: decoded code %#x outside folded interval [%#x,%#x]", c.raw, want, lo, hi)
		}
	}
}

// Given a corpus of documents folded into one summary, when every document is
// probed against every predicate shape the summary answers, then the summary
// never claims a chunk cannot match a document it holds. This is the fold
// soundness property stated directly: a brute-force check that "keep" is
// implied by an actual match.
func TestZoneFoldNeverPrunesAMatchingDocument(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	values := []string{
		"null", "true", "false", "0", "-0", "1", "1.0", "2", "-3",
		"9007199254740992", "9007199254740993", "1e308", "1e-320",
		`""`, `"a"`, `"ab"`, `"abcd"`, `"abcde"`, `"ab\ncd"`, `"z"`,
		`[1,2]`, `{"x":1}`,
	}
	probeValues := append([]string{}, values...)

	for range 3000 {
		count := 1 + rng.IntN(8)
		docs := make([]string, count)
		for i := range docs {
			switch rng.IntN(8) {
			case 0:
				docs[i] = `{"other":1}` // v absent
			case 1:
				docs[i] = `{}`
			case 2:
				docs[i] = `[1,2,3]`
			default:
				docs[i] = fmt.Sprintf(`{"v":%s}`, values[rng.IntN(len(values))])
			}
		}
		var z chunkZone
		for i, doc := range docs {
			z.fold([]byte(doc), i)
		}
		if z.state != zoneStateOK {
			t.Fatalf("fold poisoned by %v", docs)
		}
		hash := ZonePathHash("v")

		for _, probeRaw := range probeValues {
			if probeRaw == "null" {
				continue // no Cmp literal is null
			}
			// The probe side must use the *exact* code of the literal, the
			// way query does when it compiles one, not the widened interval
			// the fold side is allowed to use. Using the fold interval here
			// would hide precisely the bug where a widened fold bound is
			// dropped, because both sides would then be narrow together.
			code, ok := zoneProbeCode(probeRaw)
			if !ok {
				continue
			}
			for _, op := range []ZoneOp{ZoneOpEq, ZoneOpNe, ZoneOpLt, ZoneOpLe, ZoneOpGt, ZoneOpGe, ZoneOpIsNull, ZoneOpExists} {
				probe := ZoneProbe{Path: hash, Lo: code, Hi: code, Op: op}
				if z.keep(probe) {
					continue
				}
				// The summary says no document matches. Verify against the
				// documents themselves.
				for _, doc := range docs {
					if zoneReferenceMatch(t, doc, probeRaw, op) {
						t.Fatalf("pruned a matching chunk: op %d literal %s doc %s (corpus %v)",
							op, probeRaw, doc, docs)
					}
				}
			}
		}
	}
}

// zoneProbeCode is the query-side code for one raw JSON literal: exact, with
// strings decoded first, mirroring query/zone.go's zoneCodeOf.
func zoneProbeCode(raw string) (uint32, bool) {
	switch zoneReferenceRank(raw) {
	case 1:
		return ZoneCodeBool(raw == "true"), true
	case 2:
		return ZoneCodeNumber([]byte(raw))
	case 3:
		decoded, ok := zoneReferenceUnquote(raw)
		if !ok {
			return 0, false
		}
		return ZoneCodeString(decoded), true
	default:
		return 0, false
	}
}

// zoneReferenceMatch decides whether one document satisfies one predicate,
// independently of the summary: it re-derives the value at "v" by string
// inspection and applies the query's documented comparison rules. Numbers are
// compared with math/big so agreement with the summary is not agreement about
// float64.
func zoneReferenceMatch(t testing.TB, doc, literal string, op ZoneOp) bool {
	t.Helper()
	value, present := zoneReferenceValue(doc)
	switch op {
	case ZoneOpIsNull:
		return !present || value == "null"
	case ZoneOpExists:
		return present
	}
	if !present || value == "null" {
		// evalCmp rejects a null or absent cell before comparing.
		return false
	}
	cmp, ok := zoneReferenceCompare(t, value, literal)
	if !ok {
		return true // unknown ordering: assume a match rather than a false alarm
	}
	switch op {
	case ZoneOpEq:
		return cmp == 0
	case ZoneOpNe:
		return cmp != 0
	case ZoneOpLt:
		return cmp < 0
	case ZoneOpLe:
		return cmp <= 0
	case ZoneOpGt:
		return cmp > 0
	case ZoneOpGe:
		return cmp >= 0
	}
	return true
}

// zoneReferenceValue extracts the raw text of member "v" from a flat one- or
// two-member document, and reports whether it was present. The corpus above is
// deliberately simple enough that this needs no parser.
func zoneReferenceValue(doc string) (string, bool) {
	const prefix = `{"v":`
	if len(doc) < len(prefix) || doc[:len(prefix)] != prefix {
		return "", false
	}
	return doc[len(prefix) : len(doc)-1], true
}

// zoneReferenceCompare ranks two raw JSON scalars by the query's total order.
func zoneReferenceCompare(t testing.TB, a, b string) (int, bool) {
	ra, rb := zoneReferenceRank(a), zoneReferenceRank(b)
	if ra != rb {
		if ra < rb {
			return -1, true
		}
		return 1, true
	}
	switch ra {
	case 1: // bool
		switch {
		case a == b:
			return 0, true
		case a == "false":
			return -1, true
		default:
			return 1, true
		}
	case 2: // number
		fa, _, err := big.ParseFloat(a, 10, 400, big.ToNearestEven)
		if err != nil {
			return 0, false
		}
		fb, _, err := big.ParseFloat(b, 10, 400, big.ToNearestEven)
		if err != nil {
			return 0, false
		}
		return fa.Cmp(fb), true
	case 3: // string
		da, okA := zoneReferenceUnquote(a)
		db, okB := zoneReferenceUnquote(b)
		if !okA || !okB {
			return 0, false
		}
		switch {
		case da < db:
			return -1, true
		case da > db:
			return 1, true
		default:
			return 0, true
		}
	default: // container: exact source bytes, matching compareScalar
		switch {
		case a < b:
			return -1, true
		case a > b:
			return 1, true
		default:
			return 0, true
		}
	}
}

func zoneReferenceRank(raw string) int {
	switch {
	case raw == "true" || raw == "false":
		return 1
	case len(raw) > 0 && raw[0] == '"':
		return 3
	case len(raw) > 0 && (raw[0] == '[' || raw[0] == '{'):
		return 4
	default:
		return 2
	}
}

func zoneReferenceUnquote(raw string) (string, bool) {
	s, err := strconv.Unquote(raw)
	return s, err == nil
}

// Given a collection whose chunks are populated in a known pattern, when a
// snapshot is probed, then only chunks that cannot match are skipped and the
// surviving masks carry each chunk's exact live slots. This is the store-level
// non-vacuity check: the query differential proves pruning is harmless, this
// proves it happens.
func TestZoneSnapshotMasksSkipOnlyImpossibleChunks(t *testing.T) {
	collection, err := New(Options{ChunkDocuments: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := range 64 {
		if _, err := collection.Put(fmt.Sprintf("k%03d", i), fmt.Appendf(nil, `{"v":%d}`, i)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	snapshot, _ := collection.Snapshot()

	code, ok := ZoneCodeNumber([]byte("40"))
	if !ok {
		t.Fatal("ZoneCodeNumber declined a plain integer")
	}
	masks, pruned, bounded := snapshot.AppendZoneMasks(nil, ZoneProbe{
		Path: ZonePathHash("v"), Lo: code, Hi: code, Op: ZoneOpGe,
	})
	if !bounded {
		t.Fatal("a probe covering five of eight chunks reported itself unbounded")
	}
	if pruned != 5 {
		t.Fatalf("pruned: got %d want 5 (chunks holding 0-39)", pruned)
	}
	if len(masks) != 3 {
		t.Fatalf("surviving masks: got %d want 3", len(masks))
	}
	previous := int64(-1)
	for _, mask := range masks {
		if int64(mask.Chunk) <= previous {
			t.Fatalf("masks are not strictly ascending: %v", masks)
		}
		previous = int64(mask.Chunk)
		if mask.Bits != 0xFF {
			t.Fatalf("chunk %d mask: got %#x want the full eight live slots", mask.Chunk, mask.Bits)
		}
	}

	// With pruning disabled the identical probe must decline, which is the
	// mechanism the differential's control arm relies on.
	restore := SetZonePruning(false)
	_, _, bounded = snapshot.AppendZoneMasks(nil, ZoneProbe{
		Path: ZonePathHash("v"), Lo: code, Hi: code, Op: ZoneOpGe,
	})
	SetZonePruning(restore)
	if bounded {
		t.Fatal("SetZonePruning(false) did not disable probing")
	}
}

// Given a chunk whose path budget is exhausted, when a path with no entry is
// probed, then the chunk survives. The "no entry means the path is absent"
// deduction is only sound while nothing has been turned away, and getting that
// backwards would prune every chunk for every path the budget dropped.
func TestZoneOverflowDisablesTheAbsentPathDeduction(t *testing.T) {
	var wide chunkZone
	var fields []byte
	fields = append(fields, '{')
	for i := range ZonePaths + 2 {
		if i > 0 {
			fields = append(fields, ',')
		}
		fields = fmt.Appendf(fields, `"f%d":%d`, i, i)
	}
	fields = append(fields, '}')
	wide.fold(fields, 0)
	if !wide.overflow {
		t.Fatalf("folding %d fields into a %d-entry budget did not record overflow", ZonePaths+2, ZonePaths)
	}
	code, _ := ZoneCodeNumber([]byte("0"))
	dropped := ZoneProbe{Path: ZonePathHash(fmt.Sprintf("f%d", ZonePaths)), Lo: code, Hi: code, Op: ZoneOpEq}
	if !wide.keep(dropped) {
		t.Fatal("pruned a chunk for a path the budget dropped")
	}

	var narrow chunkZone
	narrow.fold([]byte(`{"a":1}`), 0)
	if narrow.overflow {
		t.Fatal("a one-field document overflowed the budget")
	}
	absent := ZoneProbe{Path: ZonePathHash("b"), Lo: code, Hi: code, Op: ZoneOpEq}
	if narrow.keep(absent) {
		t.Fatal("a path no document carries should be prunable while the budget is intact")
	}
	if !narrow.keep(ZoneProbe{Path: ZonePathHash("b"), Op: ZoneOpIsNull}) {
		t.Fatal("IS NULL must match a path every document lacks")
	}
	if narrow.keep(ZoneProbe{Path: ZonePathHash("b"), Op: ZoneOpExists}) {
		t.Fatal("EXISTS cannot match a path no document carries")
	}
}

// Given a document with an escaped member name, when it is folded, then the
// chunk's summary is poisoned and prunes nothing. The fold path cannot key an
// escaped name without decoding it, and silently skipping the member would let
// a later document create an entry that does not cover this one.
func TestZoneEscapedMemberNamePoisonsTheChunk(t *testing.T) {
	var z chunkZone
	z.fold([]byte(`{"a":1}`), 0)
	// The same member name written with a \u escape. Folding cannot key it
	// without decoding, and a skipped member would let the next document
	// create an "a" entry that does not cover this one.
	z.fold([]byte(`{"\u0061":2}`), 1)
	if z.state != zoneStatePoisoned {
		t.Fatalf("state: got %d want poisoned", z.state)
	}
	code, _ := ZoneCodeNumber([]byte("99"))
	if !z.keep(ZoneProbe{Path: ZonePathHash("a"), Lo: code, Hi: code, Op: ZoneOpEq}) {
		t.Fatal("a poisoned summary pruned a chunk")
	}
}

// Given a collection reopened from a persisted image, when it is queried, then
// its restored chunks prune nothing, and when one is rewritten, then its
// summary is recomputed from every document rather than only the new one.
// A restored chunk that looked like an empty-but-valid summary would let the
// next write create an entry covering one document and claiming all 64.
func TestZoneRestoredChunkIsStaleUntilRewritten(t *testing.T) {
	collection, err := New(Options{ChunkDocuments: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := range 8 {
		if _, err := collection.Put(fmt.Sprintf("k%d", i), fmt.Appendf(nil, `{"v":%d}`, i)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	path := filepath.Join(t.TempDir(), "collection.vibe")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := collection.WriteTo(file); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	image, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	reopened, err := Open(image)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	snapshot, _ := reopened.Snapshot()
	chunks, sound, _ := snapshot.ZoneStats()
	if chunks == 0 || sound != 0 {
		t.Fatalf("restored chunks: %d total, %d sound; want every chunk stale", chunks, sound)
	}

	// Rewriting one document rebuilds the chunk, which must recompute the
	// summary over all eight documents, not just the rewritten one.
	if _, err := reopened.Put("k0", []byte(`{"v":0}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	snapshot, _ = reopened.Snapshot()
	if _, sound, _ = snapshot.ZoneStats(); sound != 1 {
		t.Fatalf("sound chunks after rewrite: got %d want 1", sound)
	}
	code, _ := ZoneCodeNumber([]byte("7"))
	if _, _, bounded := snapshot.AppendZoneMasks(nil, ZoneProbe{
		Path: ZonePathHash("v"), Lo: code, Hi: code, Op: ZoneOpEq,
	}); bounded {
		t.Fatal("the rebuilt summary pruned itself for a value it holds")
	}
	code, _ = ZoneCodeNumber([]byte("100"))
	masks, pruned, bounded := snapshot.AppendZoneMasks(nil, ZoneProbe{
		Path: ZonePathHash("v"), Lo: code, Hi: code, Op: ZoneOpEq,
	})
	if !bounded || pruned != 1 || len(masks) != 0 {
		t.Fatalf("rebuilt summary did not prune a value outside its range: pruned=%d masks=%v bounded=%v",
			pruned, masks, bounded)
	}
}

// Given the summary's declared per-chunk cost, when it is measured, then it
// matches the space bound documented on chunkZone. The bound is the whole
// justification for the path-selection policy, so it is checked rather than
// asserted in prose.
func TestZoneSpaceBound(t *testing.T) {
	const want = 144
	if got := ZoneChunkBytes(); got != want {
		t.Fatalf("per-chunk summary bytes: got %d want %d", got, want)
	}
	// 2.25 bytes per document at the default chunk size; the benchmark corpus
	// averages 257 bytes per document, so the summary is 0.88% of the data.
	perDocument := float64(want) / MaxChunkDocuments
	if perDocument > 2.5 {
		t.Fatalf("summary costs %.2f B/document, above the 2.5 B budget", perDocument)
	}
}

// Given -0 and 0, when both are folded, then they share one code. The query
// treats them as one value, and a summary that separated them would exclude
// `x = 0` from a chunk holding -0.
func TestZoneNegativeZeroFoldsWithPositiveZero(t *testing.T) {
	negative, ok := ZoneCodeNumber([]byte("-0"))
	if !ok {
		t.Fatal("ZoneCodeNumber(-0) declined")
	}
	positive, _ := ZoneCodeNumber([]byte("0"))
	if negative != positive {
		t.Fatalf("-0 code %#x differs from 0 code %#x", negative, positive)
	}
	if got := zoneMonotoneFloat(math.Copysign(0, -1)); got != zoneMonotoneFloat(0) {
		t.Fatalf("monotone float order separates -0 from 0: %#x", got)
	}
}
