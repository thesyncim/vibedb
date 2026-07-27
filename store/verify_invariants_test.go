package store

// Encoding-invariant checks for the bit-packing cores.
//
// The index layer folds several fields into single machine words: an entry's
// count, kind, and flags share one 32-bit info word (index.go), and a narrow
// shape-tape value packs a start/end span into one 32-bit word (segment_shape.go).
// Every accessor and every widening rests on these packings being lossless and
// their fields being non-overlapping. This file checks those properties two
// ways, against the real packing functions rather than a model:
//
//   - Exhaustively in Go over a bounded domain, where a field is small enough
//     to sweep completely (the 3-bit kind and flags fields are enumerated in
//     full; each 16-bit span half is swept in full) or dense enough that a full
//     sweep of one axis plus boundary and randomized saturation of the other
//     covers the reachable joint space.
//   - Machine-checked by z3 over the full machine-word domain: testdata/smt/*.smt2
//     assert the negation of each invariant across all 2^32 inputs and are
//     discharged by the solver returning `unsat`, so for these specific bit
//     arithmetic invariants the z3 result is a proof over the whole word domain
//     (not a statement about the wider system). TestEncodingInvariantsSMT ties
//     each committed script to the code's live constants and runs z3 when it is
//     on PATH.
//
// scripts/verify-smt.sh discharges the committed proof obligations with z3.

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// Field masks derived from the live info-word constants (index.go). The flags
// field is the three bits above the kind field; deriving its mask here rather
// than hard-coding it keeps this check honest if the layout constants move.
const (
	infoFlagsBits          = 32 - vibejson.InfoFlagsShift
	infoFlagsMask   uint32 = (1<<infoFlagsBits - 1) << vibejson.InfoFlagsShift
	infoKindFieldW  uint32 = 1 << vibejson.InfoKindBits // 2^3: one past the max kind
	infoFlagsFieldW uint32 = 1 << infoFlagsBits         // 2^3: one past the max flags
)

// TestInfoWordFieldsDisjoint checks that the info word partitions into three
// non-overlapping fields that together cover all 32 bits. Non-overlap is what
// lets bumpCount and setCount touch the count field without disturbing kind or
// flags, and what lets packInfo compose the fields with a bare bitwise or.
func TestInfoWordFieldsDisjoint(t *testing.T) {
	countMask := vibejson.InfoCountMask
	kindMask := vibejson.InfoKindMask
	flagsMask := infoFlagsMask

	if countMask&kindMask != 0 {
		t.Fatalf("count and kind fields overlap: %#08x & %#08x", countMask, kindMask)
	}
	if countMask&flagsMask != 0 {
		t.Fatalf("count and flags fields overlap: %#08x & %#08x", countMask, flagsMask)
	}
	if kindMask&flagsMask != 0 {
		t.Fatalf("kind and flags fields overlap: %#08x & %#08x", kindMask, flagsMask)
	}
	if got := countMask | kindMask | flagsMask; got != ^uint32(0) {
		t.Fatalf("fields do not cover the word: union %#08x, want %#08x", got, ^uint32(0))
	}
	// The count field is exactly the max-count value, and the kind field sits
	// immediately above it: the layout the SMT script models.
	if vibejson.InfoMaxCount != countMask {
		t.Fatalf("infoMaxCount %#08x != count mask %#08x", vibejson.InfoMaxCount, countMask)
	}
	if vibejson.InfoKindShift != vibejson.InfoCountBits {
		t.Fatalf("kind shift %d != count width %d", vibejson.InfoKindShift, vibejson.InfoCountBits)
	}
	if vibejson.InfoFlagsShift != vibejson.InfoCountBits+vibejson.InfoKindBits {
		t.Fatalf("flags shift %d != count+kind width %d", vibejson.InfoFlagsShift, vibejson.InfoCountBits+vibejson.InfoKindBits)
	}
}

// TestInfoWordRoundTrip checks packInfo and the Count/Kind/flags accessors are
// mutually inverse over the domain packInfo's callers may reach: every count in
// [0, infoMaxCount], every 3-bit kind, every 3-bit flags. The kind and flags
// axes are enumerated exhaustively; the 26-bit count axis is swept in full for
// the worst-case kind and flags and sampled at every boundary and across a
// randomized saturation elsewhere. With the fields checked disjoint
// (TestInfoWordFieldsDisjoint) and packInfo a pure or of independently masked
// shifts, per-axis inversion is joint inversion; TestEncodingInvariantsSMT
// machine-checks the full joint domain with z3.
func TestInfoWordRoundTrip(t *testing.T) {
	// Every kind and flags value, against a boundary-and-random count set.
	counts := infoWordCountProbes()
	for kind := uint32(0); kind < infoKindFieldW; kind++ {
		for flags := uint32(0); flags < infoFlagsFieldW; flags++ {
			for _, count := range counts {
				assertInfoRoundTrip(t, count, kind, flags)
				if t.Failed() {
					return
				}
			}
		}
	}

	// The full 26-bit count sweep, with kind and flags saturated so any bit
	// bleed between fields would surface. This is the exhaustive half of the
	// count axis.
	const maxKind = infoKindFieldW - 1
	const maxFlags = infoFlagsFieldW - 1
	sweepTo := uint32(testIterations(int(vibejson.InfoMaxCount), 1<<16))
	for count := uint32(0); ; count++ {
		assertInfoRoundTrip(t, count, maxKind, maxFlags)
		if count == sweepTo {
			break
		}
	}
	if t.Failed() {
		return
	}

	// setCount and bumpCount must move only the count field.
	rng := rand.New(rand.NewSource(0x5e7c0117))
	setBumpIters := testIterations(1<<20, 1<<12)
	for i := 0; i < setBumpIters; i++ {
		kind := document.Kind(rng.Uint32() % infoKindFieldW)
		flags := uint8(rng.Uint32() % infoFlagsFieldW)
		count := rng.Uint32() & vibejson.InfoCountMask
		e := vibejson.IndexEntry{Info: vibejson.PackInfo(0, kind, flags)}
		e.SetCount(count)
		if e.Count() != count || e.Kind() != kind || e.Flags() != flags {
			t.Fatalf("setCount(%d) on (kind=%d,flags=%d) -> (%d,%d,%d)", count, kind, flags, e.Count(), e.Kind(), e.Flags())
		}
		if count < vibejson.InfoMaxCount {
			e.BumpCount()
			if e.Count() != count+1 || e.Kind() != kind || e.Flags() != flags {
				t.Fatalf("bumpCount from %d disturbed kind/flags", count)
			}
		}
	}

	t.Logf("info word: kind x flags exhausted (%d combos), count swept over [0,%d] plus %d probes/combo; setCount/bumpCount checked over %d randomized states",
		infoKindFieldW*infoFlagsFieldW, sweepTo, len(counts), setBumpIters)
}

// assertInfoRoundTrip packs one (count, kind, flags) triple through the real
// PackInfo and asserts every accessor recovers its field.
func assertInfoRoundTrip(t *testing.T, count, kind, flags uint32) {
	t.Helper()
	info := vibejson.PackInfo(count, document.Kind(kind), uint8(flags))
	e := vibejson.IndexEntry{Info: info}
	if e.Count() != count {
		t.Fatalf("packInfo(%d,%d,%d): Count = %d", count, kind, flags, e.Count())
	}
	if uint32(e.Kind()) != kind {
		t.Fatalf("packInfo(%d,%d,%d): Kind = %d", count, kind, flags, e.Kind())
	}
	if uint32(e.Flags()) != flags {
		t.Fatalf("packInfo(%d,%d,%d): flags = %d", count, kind, flags, e.Flags())
	}
}

// infoWordCountProbes returns every boundary count plus a deterministic
// randomized saturation of the 26-bit count field.
func infoWordCountProbes() []uint32 {
	probes := []uint32{0, 1, 2, 3, 7, 8, 63, 64, 1 << 12, 1 << 20, 1 << 25, vibejson.InfoMaxCount - 2, vibejson.InfoMaxCount - 1, vibejson.InfoMaxCount}
	rng := rand.New(rand.NewSource(0x1f0c0de))
	for i := 0; i < 2048; i++ {
		probes = append(probes, rng.Uint32()&vibejson.InfoCountMask)
	}
	return probes
}

// TestNarrowSpanRoundTrip checks the narrow shape-tape span packing is
// lossless and its halves are non-overlapping over the documented precondition
// (start, end <= shapeNarrowMaxEnd). Each 16-bit half is swept in full against
// boundary values of the other half, and their joint space is saturated with a
// deterministic random sample; widen must reconstruct the exact wide entry the
// narrow value was packed from, which is the invariant every narrow read and
// every Doc widening depends on. TestEncodingInvariantsSMT machine-checks the
// full 2^32 joint domain with z3.
func TestNarrowSpanRoundTrip(t *testing.T) {
	const max = ShapeNarrowMaxEnd
	opposites := []uint32{0, 1, 2, max - 1, max, max / 2}

	// Full sweep of the start half against boundary ends, and vice versa.
	for _, end := range opposites {
		for start := uint32(0); start <= max; start++ {
			assertNarrowRoundTrip(t, start, end)
		}
		if t.Failed() {
			return
		}
	}
	for _, start := range opposites {
		for end := uint32(0); end <= max; end++ {
			assertNarrowRoundTrip(t, start, end)
		}
		if t.Failed() {
			return
		}
	}

	// Randomized saturation of the joint 32-bit span space.
	rng := rand.New(rand.NewSource(0x0ff5e701))
	randPairs := testIterations(1<<21, 1<<14)
	for i := 0; i < randPairs; i++ {
		start := rng.Uint32() & max
		end := rng.Uint32() & max
		assertNarrowRoundTrip(t, start, end)
		if t.Failed() {
			return
		}
	}

	t.Logf("narrow span: both 16-bit halves swept fully over [0,%d] against %d boundary opposites, joint space saturated over %d random pairs",
		max, len(opposites), randPairs)
}

// assertNarrowRoundTrip packs a wide entry's span into a narrow value through
// the real packing and asserts start/end unpack exactly and widen reconstructs
// the original wide entry bit for bit. The info word is varied with the span
// so the widening carries it through unchanged.
func assertNarrowRoundTrip(t *testing.T, start, end uint32) {
	t.Helper()
	wide := vibejson.IndexEntry{Start: start, End: end, Next: 1, Info: vibejson.PackInfo(0, document.String, vibejson.TapeFlagKey) ^ (start * 2654435761)}
	sv := ShapeNarrowValue{Span: wide.Start | wide.End<<16, Info: wide.Info}
	if sv.Start() != start {
		t.Fatalf("narrow span (%d,%d): start() = %d", start, end, sv.Start())
	}
	if sv.End() != end {
		t.Fatalf("narrow span (%d,%d): end() = %d", start, end, sv.End())
	}
	if got := sv.widen(); got != wide {
		t.Fatalf("narrow span (%d,%d): widen() = %+v, want %+v", start, end, got, wide)
	}
	// The two packed halves must not share a bit: the low half recovers start
	// exactly when and only when the shift left the high half's low 16 bits
	// clear.
	if sv.Span&0xFFFF != start || sv.Span>>16 != end {
		t.Fatalf("narrow span (%d,%d): halves overlap in %#08x", start, end, sv.Span)
	}
}

// smtScript pairs a committed SMT script with the live constants it must
// encode, so a layout change that outdates a script fails this test rather than
// passing silently.
type smtScript struct {
	file  string
	needs []string
}

// TestEncodingInvariantsSMT checks the committed SMT scripts still model the
// code's live constants and, when z3 is on PATH, discharges each by requiring
// `unsat` — a machine-checked proof that the specific bit-packing invariant
// holds across the whole 32-bit word domain. When z3 is absent the scripts
// stand as the committed proof obligation; scripts/verify-smt.sh discharges
// them and records the artifact.
func TestEncodingInvariantsSMT(t *testing.T) {
	scripts := []smtScript{
		{
			file: "info_word.smt2",
			needs: []string{
				fmt.Sprintf("#x%08x", vibejson.InfoCountMask),          // count mask / infoMaxCount
				fmt.Sprintf("#x%08x", vibejson.InfoKindMask),           // kind mask
				fmt.Sprintf("#x%08x", uint32(vibejson.InfoKindShift)),  // << to kind field
				fmt.Sprintf("#x%08x", uint32(vibejson.InfoFlagsShift)), // << to flags field
			},
		},
		{
			file: "info_word_disjoint.smt2",
			needs: []string{
				fmt.Sprintf("#x%08x", vibejson.InfoCountMask),
				fmt.Sprintf("#x%08x", vibejson.InfoKindMask),
				fmt.Sprintf("#x%08x", infoFlagsMask),
			},
		},
		{
			file: "narrow_span.smt2",
			needs: []string{
				fmt.Sprintf("#x%08x", uint32(ShapeNarrowMaxEnd)), // span half mask
				"#x00000010", // << 16
			},
		},
	}

	dir := filepath.Join("testdata", "smt")
	for _, s := range scripts {
		path := filepath.Join(dir, s.file)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(body)
		for _, need := range s.needs {
			if !strings.Contains(text, need) {
				t.Fatalf("%s does not model live constant %s; update the script and rerun scripts/verify-smt.sh", path, need)
			}
		}
	}

	z3, err := exec.LookPath("z3")
	if err != nil {
		t.Logf("z3 not on PATH: %d committed SMT scripts stand as the full-word-domain proof obligation for the bit arithmetic; run scripts/verify-smt.sh with z3 to discharge them", len(scripts))
		return
	}
	for _, s := range scripts {
		path := filepath.Join(dir, s.file)
		out, err := exec.Command(z3, "-smt2", path).CombinedOutput()
		if err != nil {
			t.Fatalf("z3 %s: %v\n%s", path, err, out)
		}
		if got := strings.TrimSpace(string(out)); got != "unsat" {
			t.Fatalf("z3 %s reported %q, want unsat (a satisfying model is a counterexample to the invariant)", path, got)
		}
		t.Logf("z3 discharged %s: unsat", s.file)
	}
}
