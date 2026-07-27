package durable

import (
	"fmt"
	"strings"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// The multi-column exact recheck and the pre-update index build both index a
// document with build-and-retry rather than with a RequiredIndexEntries
// sizing pass, so their correctness rests on the growth loop instead of on a
// count that was right by construction. These tests hold what replaced it.

func indexGrowthShapes() map[string]string {
	wide := make([]string, 512)
	for i := range wide {
		wide[i] = fmt.Sprintf(`"k%d":%d`, i, i)
	}
	dense := make([]string, 1024)
	for i := range dense {
		dense[i] = "1"
	}
	return map[string]string{
		"scalar":        `1`,
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

// TestBuildIndexGrowingMatchesExactSizing drives the growth loop from a seed
// of one entry — smaller than any document needs — and requires the resulting
// tape to be identical, entry for entry, to one built into storage sized by
// RequiredIndexEntries. Seeding at one is what makes this non-vacuous: the
// production seed of len/8+8 covers most of these shapes on the first attempt.
func TestBuildIndexGrowingMatchesExactSizing(t *testing.T) {
	options := document.IndexOptions{HashKeys: true}
	for name, src := range indexGrowthShapes() {
		t.Run(name, func(t *testing.T) {
			raw := []byte(src)
			need, err := vibejson.RequiredIndexEntries(raw)
			if err != nil {
				t.Fatalf("sizing: %v", err)
			}
			want, err := vibejson.BuildIndexOptions(raw, make([]vibejson.IndexEntry, need), options)
			if err != nil {
				t.Fatalf("reference build: %v", err)
			}

			var tape []vibejson.IndexEntry
			got, err := buildIndexGrowingFrom(&tape, raw, options, 1)
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
			if cap(tape) < need {
				t.Fatalf("tape stopped growing at %d entries, needed %d", cap(tape), need)
			}
		})
	}
}

// TestBuildIndexGrowingWarmTapeIsAllocationFree holds the property that pays
// for removing the sizing pass: growth is warmup, not per document. Once the
// tape has reached the width a document needs, every later build of a document
// no wider must reuse it and allocate nothing. A loop that reset, shrank, or
// resized the tape per call would still return the right entries — the
// correctness test above would not notice — while turning a one-time cost into
// a per-document one, which is the whole reason for the change.
func TestBuildIndexGrowingWarmTapeIsAllocationFree(t *testing.T) {
	docs := [][]byte{
		[]byte(`{"a":1,"b":[2,3],"c":{"d":4}}`),
		[]byte(`{"a":9,"b":[8,7],"c":{"d":6}}`),
		[]byte(`{"a":0,"b":[1,1],"c":{"d":1}}`),
	}
	var tape []vibejson.IndexEntry
	for _, doc := range docs {
		if _, err := buildIndexGrowingFrom(&tape, doc, document.IndexOptions{}, 1); err != nil {
			t.Fatal(err)
		}
	}
	warm := cap(tape)
	if warm == 0 {
		t.Fatal("tape did not grow")
	}

	row := 0
	allocs := testing.AllocsPerRun(200, func() {
		if _, err := buildIndexGrowingFrom(&tape, docs[row%len(docs)], document.IndexOptions{}, 1); err != nil {
			t.Fatal(err)
		}
		row++
	})
	if allocs != 0 {
		t.Fatalf("warm build allocated %.1f times per document, want 0", allocs)
	}
	if cap(tape) != warm {
		t.Fatalf("tape changed from %d to %d entries while warm", warm, cap(tape))
	}
}

// TestBuildIndexGrowingRejectsInvalid pins that build-and-retry did not weaken
// validation: malformed input must report its syntax error rather than be
// grown at until the limit turns the failure into ErrIndexFull.
func TestBuildIndexGrowingRejectsInvalid(t *testing.T) {
	for _, src := range []string{`{`, `[1,2`, `{"a":}`, `tru`, `1 2`, `"unterminated`} {
		t.Run(src, func(t *testing.T) {
			var tape []vibejson.IndexEntry
			_, err := buildIndexGrowingFrom(&tape, []byte(src), document.IndexOptions{}, 1)
			if err == nil {
				t.Fatalf("indexed malformed %q", src)
			}
			if err == document.ErrIndexFull {
				t.Fatalf("malformed %q reported as a storage shortfall", src)
			}
		})
	}
}
