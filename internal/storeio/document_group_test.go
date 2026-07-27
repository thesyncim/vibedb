package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"
)

func testDocumentGroupChunks() []DocumentGroupChunk {
	values0 := make([]float64, 64)
	values1 := make([]float64, 64)
	values0[0], values0[2] = 1.5, 2.5
	values1[1] = 3.5
	return []DocumentGroupChunk{
		{
			ChunkID: 7,
			Live:    0b101,
			Rows: []DocumentGroupRecord{
				{
					Key: []byte("alpha"), JSON: []byte(`{"kind":"same","n":1}`), Slot: 0,
					Spans: []DocumentGroupSpan{{Start: 8, End: 14}, {Start: 19, End: 20}},
				},
				{
					Key: []byte("gamma"), JSON: []byte(`{"kind":"same","n":3}`), Slot: 2,
					Spans: []DocumentGroupSpan{{Start: 8, End: 14}, {Start: 19, End: 20}},
				},
			},
			Columns: DocumentFloat64Columns{Masks: []uint64{0b101}, Values: values0},
		},
		{
			ChunkID: 8,
			Live:    0b10,
			Rows: []DocumentGroupRecord{
				{
					Key: []byte("beta"), JSON: []byte(`{"kind":"same","n":2}`), Slot: 1,
					Spans: []DocumentGroupSpan{{Start: 8, End: 14}, {Start: 19, End: 20}},
				},
			},
			Columns: DocumentFloat64Columns{Masks: []uint64{0b10}, Values: values1},
		},
	}
}

func encodeTestDocumentGroup(t *testing.T) []byte {
	t.Helper()
	chunks := testDocumentGroupChunks()
	var workspace DocumentGroupWorkspace
	size, ok := DocumentGroupSize(chunks, testSuperblockPageSize, &workspace)
	if !ok {
		t.Fatal("DocumentGroupSize rejected valid chunks")
	}
	page := make([]byte, size)
	encoded, err := EncodeDocumentGroup(page, DocumentGroupHeader{
		StoreID: testStoreID, Generation: 3, LogicalID: 11, PageSize: size,
		FirstChunk: 7, ChunkCount: 2, RowCount: 3, ColumnCount: 1,
	}, chunks, 20, &workspace)
	if err != nil {
		t.Fatalf("EncodeDocumentGroup: %v", err)
	}
	return encoded
}

func TestDocumentGroupExactRoundTripAndColumns(t *testing.T) {
	page := encodeTestDocumentGroup(t)
	view, err := OpenDocumentGroup(page, 9, 20)
	if err != nil {
		t.Fatalf("OpenDocumentGroup: %v", err)
	}
	if got := view.Header(); got.FirstChunk != 7 || got.ChunkCount != 2 || got.RowCount != 3 {
		t.Fatalf("header = %+v", got)
	}
	chunk, ok := view.Chunk(7)
	if !ok || chunk.Live() != 0b101 || chunk.Len() != 2 {
		t.Fatalf("chunk 7 = (%+v,%v)", chunk, ok)
	}
	record, ok := chunk.Lookup(2)
	if !ok || string(record.Key) != "gamma" || record.JSONLength != 21 {
		t.Fatalf("record = (%+v,%v)", record, ok)
	}
	dst, ok := chunk.AppendJSON(make([]byte, 0, 64), 2)
	if !ok || string(dst) != `{"kind":"same","n":3}` {
		t.Fatalf("AppendJSON = (%q,%v)", dst, ok)
	}
	if _, ok := chunk.LookupString(2, "not-gamma"); ok {
		t.Fatal("LookupString accepted the wrong complete key")
	}
	column, ok := chunk.Float64Column(0)
	if !ok {
		t.Fatal("Float64Column(0) missing")
	}
	if got, ok := column.Lookup(2); !ok || got != 2.5 {
		t.Fatalf("column slot 2 = (%v,%v)", got, ok)
	}
	reuse := make([]byte, 0, 64)
	if allocs := testing.AllocsPerRun(100, func() {
		var found bool
		reuse, found = chunk.AppendJSON(reuse[:0], 2)
		if !found {
			panic("AppendJSON failed")
		}
	}); allocs != 0 {
		t.Fatalf("AppendJSON allocations = %.2f", allocs)
	}
	second, ok := view.Chunk(8)
	if !ok {
		t.Fatal("chunk 8 missing")
	}
	got, ok := second.AppendJSON(nil, 1)
	if !ok || string(got) != `{"kind":"same","n":2}` {
		t.Fatalf("second AppendJSON = (%q,%v)", got, ok)
	}
}

func TestDocumentGroupEncodingIsDeterministic(t *testing.T) {
	first := encodeTestDocumentGroup(t)
	second := encodeTestDocumentGroup(t)
	if !bytes.Equal(first, second) {
		t.Fatal("identical group inputs produced different durable bytes")
	}
}

func TestDocumentGroupLiteralTokenBoundaries(t *testing.T) {
	short := []byte(`"` + strings.Repeat("x", documentGroupMaxShortBytes-2) + `"`)
	long := []byte(`"` + strings.Repeat("y", documentGroupMaxShortBytes-1) + `"`)
	if len(short) != documentGroupMaxShortBytes || len(long) != documentGroupMaxShortBytes+1 {
		t.Fatal("literal boundary fixture drift")
	}
	chunks := []DocumentGroupChunk{
		{ChunkID: 4, Live: 1, Rows: []DocumentGroupRecord{{
			Key: []byte("short"), JSON: short, Spans: []DocumentGroupSpan{{End: uint32(len(short))}},
		}}},
		{ChunkID: 5, Live: 1, Rows: []DocumentGroupRecord{{
			Key: []byte("long"), JSON: long, Spans: []DocumentGroupSpan{{End: uint32(len(long))}},
		}}},
	}
	var workspace DocumentGroupWorkspace
	size, ok := DocumentGroupSize(chunks, testSuperblockPageSize, &workspace)
	if !ok {
		t.Fatal("literal boundary group rejected")
	}
	page := make([]byte, size)
	page, err := EncodeDocumentGroup(page, DocumentGroupHeader{
		StoreID: testStoreID, Generation: 2, LogicalID: 9, PageSize: size,
		FirstChunk: 4, ChunkCount: 2, RowCount: 2,
	}, chunks, 12, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenDocumentGroup(page, 6, 12)
	if err != nil {
		t.Fatal(err)
	}
	for chunkID, want := range map[uint32][]byte{4: short, 5: long} {
		chunk, found := view.Chunk(chunkID)
		if !found {
			t.Fatalf("chunk %d missing", chunkID)
		}
		got, found := chunk.AppendJSON(nil, 0)
		if !found || !bytes.Equal(got, want) {
			t.Fatalf("chunk %d literal = (%q,%v), want %q", chunkID, got, found, want)
		}
	}
}

func TestDocumentGroupPlanningAndEncodingAllocs(t *testing.T) {
	chunks := testDocumentGroupChunks()
	var workspace DocumentGroupWorkspace
	size, ok := DocumentGroupSize(chunks, testSuperblockPageSize, &workspace)
	if !ok {
		t.Fatal("DocumentGroupSize rejected valid chunks")
	}
	page := make([]byte, size)
	header := DocumentGroupHeader{
		StoreID: testStoreID, Generation: 3, LogicalID: 11, PageSize: size,
		FirstChunk: 7, ChunkCount: 2, RowCount: 3, ColumnCount: 1,
	}
	if _, err := EncodeDocumentGroup(page, header, chunks, 20, &workspace); err != nil {
		t.Fatal(err)
	}

	if allocs := testing.AllocsPerRun(100, func() {
		if _, ok := DocumentGroupSize(chunks, testSuperblockPageSize, &workspace); !ok {
			panic("DocumentGroupSize failed")
		}
	}); allocs != 0 {
		t.Fatalf("warm DocumentGroupSize allocations = %.2f, want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, err := EncodeDocumentGroup(page, header, chunks, 20, &workspace); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm EncodeDocumentGroup allocations = %.2f, want 0", allocs)
	}
}

func TestDocumentGroupRejectsResealedCorruption(t *testing.T) {
	page := encodeTestDocumentGroup(t)
	pageHeader, payload, err := OpenPage(page)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func([]byte)
	}{
		{"row slot", func(p []byte) {
			rowStart := DocumentGroupPayloadHeaderSize + 2*DocumentGroupChunkSize
			p[rowStart+14] = 63
		}},
		{"literal token", func(p []byte) {
			chunks := int(binary.LittleEndian.Uint32(p[16:20]))
			rows := int(binary.LittleEndian.Uint32(p[20:24]))
			keys := int(binary.LittleEndian.Uint32(p[24:28]))
			body := DocumentGroupPayloadHeaderSize + chunks + rows + keys
			p[body] = 0xfe
		}},
		{"template end", func(p []byte) {
			chunks := int(binary.LittleEndian.Uint32(p[16:20]))
			rows := int(binary.LittleEndian.Uint32(p[20:24]))
			keys := int(binary.LittleEndian.Uint32(p[24:28]))
			bodies := int(binary.LittleEndian.Uint32(p[28:32]))
			templates := DocumentGroupPayloadHeaderSize + chunks + rows + keys + bodies
			binary.LittleEndian.PutUint32(p[templates:templates+4], math.MaxUint32)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := bytes.Clone(page)
			payloadCopy := corrupt[PageHeaderSize : PageHeaderSize+len(payload)]
			tc.mutate(payloadCopy)
			if _, err := sealInitializedPage(corrupt[:pageHeader.PageSize]); err != nil {
				t.Fatalf("reseal: %v", err)
			}
			if _, err := OpenDocumentGroup(corrupt, 9, 20); !errors.Is(err, ErrDocumentGroupCorrupt) {
				t.Fatalf("OpenDocumentGroup = %v, want %v", err, ErrDocumentGroupCorrupt)
			}
		})
	}
}
