package query

import (
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// The containment haystack is indexed with build-and-retry rather than with a
// RequiredIndexEntries sizing pass, so its correctness now rests on the growth
// loop instead of on a count that was right by construction. These tests hold
// the three properties that replaced it: a starved tape still produces exactly
// the index a correctly sized one would, a warm tape is reused without
// allocating, and malformed input is still rejected as malformed rather than
// as a storage shortfall.

// tapeGrowthShapes are haystacks chosen to span the entry-count-per-byte range
// the loop has to reach: a bare scalar, empty containers, a dense numeric
// array (the densest real shape, near one entry per two source bytes), deep
// nesting, escaped and duplicate keys, and one document wide enough that a
// seed of one has to double many times to cover it.
func tapeGrowthShapes() map[string]string {
	wide := make([]string, 512)
	for i := range wide {
		wide[i] = fmt.Sprintf(`"k%d":%d`, i, i)
	}
	dense := make([]string, 1024)
	for i := range dense {
		dense[i] = "1"
	}
	return map[string]string{
		"scalar-number": `1`,
		"scalar-string": `"abc"`,
		"scalar-null":   `null`,
		"empty-object":  `{}`,
		"empty-array":   `[]`,
		"nested-empty":  `{"a":{},"b":[],"c":[{}]}`,
		"dense-array":   "[" + strings.Join(dense, ",") + "]",
		"deep":          strings.Repeat("[", 60) + "1" + strings.Repeat("]", 60),
		"escaped-key":   `{"abc":1,"d\\e":[2,3]}`,
		"duplicate-key": `{"a":1,"a":2,"a":3}`,
		"wide-object":   "{" + strings.Join(wide, ",") + "}",
		"mixed":         `{"id":7,"tags":["x","y"],"obj":{"x":1,"live":true},"f":1.5e3}`,
	}
}

// TestContainsTapeGrowthMatchesExactSizing drives the growth loop from a seed
// of one entry — smaller than any document needs — and requires the resulting
// tape to be identical, entry for entry, to one built into storage sized by
// RequiredIndexEntries. Seeding at one is what makes this non-vacuous: the
// production seed of len/8+8 covers most of these shapes on the first attempt,
// so only an explicitly starved seed proves the retry reconstructs the tape
// rather than truncating it.
func TestContainsTapeGrowthMatchesExactSizing(t *testing.T) {
	for name, src := range tapeGrowthShapes() {
		t.Run(name, func(t *testing.T) {
			raw := []byte(src)
			need, err := vibejson.RequiredIndexEntries(raw)
			if err != nil {
				t.Fatalf("sizing %q: %v", src, err)
			}
			want, err := vibejson.BuildIndex(raw, make([]vibejson.IndexEntry, need))
			if err != nil {
				t.Fatalf("reference build: %v", err)
			}

			var s evalScratch
			got, err := s.containsTapeFrom(raw, 1)
			if err != nil {
				t.Fatalf("grown build: %v", err)
			}
			if len(got.Entries) != len(want.Entries) {
				t.Fatalf("grown tape has %d entries, want %d", len(got.Entries), len(want.Entries))
			}
			for i := range want.Entries {
				if got.Entries[i] != want.Entries[i] {
					t.Fatalf("entry %d: got %+v, want %+v", i, got.Entries[i], want.Entries[i])
				}
			}
			if cap(s.entries) < need {
				t.Fatalf("tape stopped growing at %d entries, needed %d", cap(s.entries), need)
			}
		})
	}
}

// TestContainsTapeWarmBuildIsAllocationFree holds the property that pays for
// removing the sizing pass: growth is warmup, not per row. Once the tape has
// reached the width a haystack needs, every later build of a haystack no wider
// must reuse it and allocate nothing. A loop that reset, shrank, or resized
// the buffer per call would still return the right tape — the correctness test
// above would not notice — while turning a one-time cost into a per-row one,
// which is the whole reason for the change.
func TestContainsTapeWarmBuildIsAllocationFree(t *testing.T) {
	docs := [][]byte{
		[]byte(`{"a":1,"b":[2,3],"c":{"d":4}}`),
		[]byte(`{"a":9,"b":[8,7],"c":{"d":6}}`),
		[]byte(`{"a":0,"b":[1,1],"c":{"d":1}}`),
	}
	var s evalScratch
	for _, doc := range docs {
		if _, err := s.containsTapeFrom(doc, 1); err != nil {
			t.Fatal(err)
		}
	}
	warm := cap(s.entries)
	if warm == 0 {
		t.Fatal("tape did not grow")
	}

	row := 0
	allocs := testing.AllocsPerRun(200, func() {
		if _, err := s.containsTapeFrom(docs[row%len(docs)], 1); err != nil {
			t.Fatal(err)
		}
		row++
	})
	if allocs != 0 {
		t.Fatalf("warm build allocated %.1f times per row, want 0", allocs)
	}
	if cap(s.entries) != warm {
		t.Fatalf("tape changed from %d to %d entries while warm", warm, cap(s.entries))
	}
}

// TestContainsTapeRejectsInvalidHaystack pins that build-and-retry did not
// weaken validation. The sizing pass used to reject malformed input before any
// tape was written; now the builder does, and it must still refuse rather than
// grow until it reaches the limit and returns ErrIndexFull.
func TestContainsTapeRejectsInvalidHaystack(t *testing.T) {
	for _, src := range []string{`{`, `[1,2`, `{"a":}`, `tru`, `1 2`, `"unterminated`, `[1,]`} {
		t.Run(src, func(t *testing.T) {
			var s evalScratch
			_, err := s.containsTapeFrom([]byte(src), 1)
			if err == nil {
				t.Fatalf("indexed malformed %q", src)
			}
			if err == document.ErrIndexFull {
				t.Fatalf("malformed %q reported as a storage shortfall", src)
			}
		})
	}
}
