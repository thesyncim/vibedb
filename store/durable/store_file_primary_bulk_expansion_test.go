package durable

import (
	"bytes"
	"errors"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

func TestCanonicalizePrimaryBulkRecordsExpandsUnicodeSeparators(t *testing.T) {
	canonical := []byte(`{"a":1}`)
	inputs := [][]byte{
		[]byte(`{"value":"` + strings.Repeat("\u2028", 128) + `"}`),
		[]byte(`{"value":"` + strings.Repeat("\u2029", 128) + `"}`),
		[]byte(`{"` + strings.Repeat("\u2028\u2029", 64) + `":1}`),
	}
	records := []storeio.PrimaryGraphRecord{{Value: byteview.String(canonical)}}
	want := make([][]byte, len(inputs))
	for at, input := range inputs {
		var err error
		want[at], err = vibejson.AppendCanonicalize(nil, input)
		if err != nil {
			t.Fatal(err)
		}
		if len(want[at]) <= len(input) {
			t.Fatal("fixture does not require canonical expansion")
		}
		records = append(records, storeio.PrimaryGraphRecord{Value: byteview.String(input)})
	}
	if err := canonicalizePrimaryBulkRecords(records, document.IndexOptions{}, math.MaxInt, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if &byteview.Bytes(records[0].Value)[0] != &canonical[0] {
		t.Fatal("canonical record stopped borrowing its source")
	}
	for at, input := range inputs {
		got := byteview.Bytes(records[at+1].Value)
		if !bytes.Equal(got, want[at]) {
			t.Fatalf("record %d differs from vibejson canonical output: got %q want %q", at, got, want[at])
		}
		if &got[0] == &input[0] {
			t.Fatalf("expanded record %d aliases its input", at)
		}
		clear(input)
	}
	for at := range inputs {
		if !bytes.Equal(byteview.Bytes(records[at+1].Value), want[at]) {
			t.Fatalf("expanded record %d lost its owned spelling", at)
		}
	}
}

func TestCreateFromRecordsCanonicalExpansionHonorsLimitsBeforeWriting(t *testing.T) {
	raw := []byte(`{"value":"` + strings.Repeat("\u2028\u2029", 8) + `"}`)
	canonical, err := vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name          string
		documentLimit int
		inlineLimit   int
		want          error
	}{
		{"document", len(canonical) - 1, len(canonical) - 1, ErrDocumentTooLarge},
		{"inline", len(canonical), len(canonical) - 1, ErrPrimaryCutoverUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "bulk-expansion-*.vibe")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			_, err = CreateFromRecords([]PrimaryBulkRecord{{Key: "a", Value: raw}}, file, Options{
				Backend: BackendPortable, Durability: DurabilityAsyncVisible,
				MaxDocumentBytes: test.documentLimit, InlineValueBytes: test.inlineLimit,
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("canonical bound refusal=%v, want %v", err, test.want)
			}
			info, err := file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != 0 {
				t.Fatalf("canonical bound refusal wrote %d bytes", info.Size())
			}
		})
	}
}

func TestPrimaryBulkCanonicalArenaUsesOnlyInputExpansion(t *testing.T) {
	inputs := []string{`{"a":1}`, ` { "a" : 1 } `, `"\u2028"`, "\"\u2028\u2029\""}
	records := make([]storeio.PrimaryGraphRecord, len(inputs))
	want := 6 // Two raw three-byte separators become six-byte escapes.
	for at, input := range inputs {
		records[at].Value = input
		want += len(input)
	}
	got, err := primaryBulkCanonicalArenaBytes(records)
	if err != nil || got != want {
		t.Fatalf("arena bound=(%d,%v), want %d", got, err, want)
	}
}

func TestCreateFromRecordsUnicodeExpansionSurvivesReopen(t *testing.T) {
	for _, byteKeys := range []bool{false, true} {
		name := "string-keys"
		if byteKeys {
			name = "byte-keys"
		}
		t.Run(name, func(t *testing.T) {
			inputs := [][]byte{
				[]byte(`{"value":"` + strings.Repeat("\u2028", 128) + `"}`),
				[]byte(`{"` + strings.Repeat("\u2029", 128) + `":1}`),
			}
			want := make([][]byte, len(inputs))
			for at, input := range inputs {
				var err error
				want[at], err = vibejson.AppendCanonicalize(nil, input)
				if err != nil {
					t.Fatal(err)
				}
			}
			file, err := os.CreateTemp(t.TempDir(), "bulk-unicode-*.vibe")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			maximum := max(len(want[0]), len(want[1]))
			options := Options{Backend: BackendPortable, Durability: DurabilityAsyncVisible,
				MaxDocumentBytes: maximum, InlineValueBytes: maximum}
			if byteKeys {
				_, err = CreateFromByteRecords([]PrimaryBulkBytesRecord{
					{Key: []byte("a"), Value: inputs[0]}, {Key: []byte("b"), Value: inputs[1]},
				}, file, options)
			} else {
				_, err = CreateFromRecords([]PrimaryBulkRecord{
					{Key: "a", Value: inputs[0]}, {Key: "b", Value: inputs[1]},
				}, file, options)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, input := range inputs {
				clear(input)
			}
			collection, err := Open(file, options)
			if err != nil {
				t.Fatal(err)
			}
			defer collection.Close()
			for at, key := range [][]byte{[]byte("a"), []byte("b")} {
				got, found, err := collection.AppendRaw(nil, key)
				if err != nil || !found || !bytes.Equal(got, want[at]) {
					t.Fatalf("reopened record %q: found=%t err=%v got=%q want=%q", key, found, err, got, want[at])
				}
			}
		})
	}
}
