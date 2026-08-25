package main

import (
	"bytes"
	"strings"
	"testing"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
	vibejson "github.com/thesyncim/vibejson"
)

type rawOutput struct {
	Key string `json:"key"`
	Raw string `json:"raw"`
}

func TestEmitFixtureTypedAndRaw(t *testing.T) {
	for _, shape := range []string{"typed", "raw"} {
		var out bytes.Buffer
		if err := emitFixture(&out, 3, competitive.LowCardinality, shape); err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(bytes.TrimSuffix(out.Bytes(), []byte{'\n'}), []byte{'\n'})
		if len(lines) != 3 {
			t.Fatalf("%s lines = %d, want 3", shape, len(lines))
		}
		for _, line := range lines {
			if !vibejson.Valid(line) {
				t.Fatalf("%s emitted invalid JSON: %s", shape, line)
			}
		}
		if shape == "raw" {
			var got rawOutput
			if err := vibejson.Unmarshal(lines[0], &got); err != nil {
				t.Fatal(err)
			}
			want := competitive.CorpusOf(1, competitive.LowCardinality)[0]
			if got.Key != want.Key || got.Raw != string(want.JSON) {
				t.Fatalf("raw row = %+v, want key %q raw %q", got, want.Key, want.JSON)
			}
		}
	}
}

func TestEmitDocumentsRejectsUnknownExactOverflowAndBounds(t *testing.T) {
	tests := []competitive.Doc{
		{Key: "unknown", JSON: []byte(`{"id":1,"unknown":true}`)},
		{Key: "overflow", JSON: []byte(`{"id":18446744073709551616}`)},
		{Key: "depth", JSON: []byte(`{"id":1,"profile":{"tier":{"x":{"y":0}}}}`)},
	}
	for _, doc := range tests {
		if err := emitDocuments(&bytes.Buffer{}, []competitive.Doc{doc}, "typed"); err == nil {
			t.Fatalf("invalid document %q was accepted", doc.Key)
		}
	}
	huge := competitive.Doc{Key: "huge", JSON: bytes.Repeat([]byte{' '}, maxFixtureDocumentBytes+1)}
	if err := emitDocuments(&bytes.Buffer{}, []competitive.Doc{huge}, "raw"); err == nil {
		t.Fatal("oversized document was accepted")
	}

	exact := competitive.Doc{Key: "max", JSON: []byte(`{"id":18446744073709551615,"name":"n","country":"PT","score":65535,"active":true,"profile":{"tier":"t","region":"r","joined":"j"},"tags":[],"note":""}`)}
	var out bytes.Buffer
	if err := emitDocuments(&out, []competitive.Doc{exact}, "typed"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"id":`+"18446744073709551615") {
		t.Fatalf("exact uint64 spelling lost: %s", out.Bytes())
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, bytes.ErrTooLarge }

func TestEmitDocumentsPropagatesSinkError(t *testing.T) {
	if err := emitFixture(failWriter{}, 1, competitive.LowCardinality, "typed"); err == nil {
		t.Fatal("sink error was ignored")
	}
	if err := emitFixture(&bytes.Buffer{}, 1, competitive.LowCardinality, "other"); err == nil {
		t.Fatal("unknown shape was accepted")
	}
}

func TestTypedTransformSteadyStateAllocs(t *testing.T) {
	source := competitive.CorpusOf(1, competitive.LowCardinality)[0]
	var doc document
	if err := documentDecoder.Decode(source.JSON, &doc); err != nil {
		t.Fatal(err)
	}
	scratch := make([]byte, 0, 1024)
	decodeAllocs := testing.AllocsPerRun(1000, func() {
		if err := documentDecoder.Decode(source.JSON, &doc); err != nil {
			panic(err)
		}
	})
	encoded := row{
		Key: source.Key, ID: doc.ID, Name: doc.Name, Country: doc.Country,
		Score: doc.Score, Active: doc.Active, Tier: doc.Profile.Tier,
		Region: doc.Profile.Region, Joined: doc.Profile.Joined,
		Tags: doc.Tags, Note: doc.Note,
	}
	encodeAllocs := testing.AllocsPerRun(1000, func() {
		var err error
		scratch, err = rowEncoder.AppendJSON(scratch[:0], &encoded)
		if err != nil {
			panic(err)
		}
	})
	if decodeAllocs != 0 || encodeAllocs != 0 {
		t.Fatalf("steady-state allocations: decode=%v encode=%v, want 0/0", decodeAllocs, encodeAllocs)
	}
}
