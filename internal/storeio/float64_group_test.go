package storeio

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func encodeTestFloat64Group(t *testing.T) ([]byte, []DocumentGroupChunk) {
	t.Helper()
	chunks := testDocumentGroupChunks()
	size, ok := Float64GroupSize(chunks, testSuperblockPageSize)
	if !ok {
		t.Fatal("Float64GroupSize rejected valid chunks")
	}
	page := make([]byte, size)
	page, err := EncodeFloat64Group(page, Float64GroupHeader{
		StoreID: testStoreID, Generation: 3, LogicalID: 12, PageSize: size,
		FirstChunk: 7, ChunkCount: 2, RowCount: 3, ColumnCount: 1,
	}, chunks, 20)
	if err != nil {
		t.Fatal(err)
	}
	return page, chunks
}

func TestFloat64GroupExactRoundTripDerivationAndAllocs(t *testing.T) {
	page, chunks := encodeTestFloat64Group(t)
	view, err := OpenFloat64Group(page, 9, 20)
	if err != nil {
		t.Fatal(err)
	}
	if got := view.Header(); got.FirstChunk != 7 || got.ChunkCount != 2 ||
		got.RowCount != 3 || got.ColumnCount != 1 {
		t.Fatalf("header = %+v", got)
	}
	first, ok := view.Chunk(7)
	if !ok || first.Float64ColumnCount() != 1 {
		t.Fatalf("first chunk = (%+v,%v)", first, ok)
	}
	column, ok := first.Float64Column(0)
	if !ok {
		t.Fatal("first column missing")
	}
	if value, found := column.Lookup(2); !found || value != 2.5 {
		t.Fatalf("slot two = (%v,%v)", value, found)
	}
	second, ok := view.Chunk(8)
	if !ok {
		t.Fatal("second chunk missing")
	}
	column, ok = second.Float64Column(0)
	if !ok {
		t.Fatal("second column missing")
	}
	if value, found := column.Lookup(1); !found || value != 3.5 {
		t.Fatalf("second slot one = (%v,%v)", value, found)
	}
	firstValues, encoding, ok := view.Float64ColumnRangeValues(0, 7, 1)
	if !ok || encoding != Float64GroupFloat64LE || len(firstValues) != 16 ||
		math.Float64frombits(binary.LittleEndian.Uint64(firstValues[0:8])) != 1.5 ||
		math.Float64frombits(binary.LittleEndian.Uint64(firstValues[8:16])) != 2.5 {
		t.Fatalf("first packed range = (%x,%v)", firstValues, ok)
	}
	secondValues, encoding, ok := view.Float64ColumnRangeValues(0, 8, 1)
	if !ok || encoding != Float64GroupFloat64LE || len(secondValues) != 8 ||
		math.Float64frombits(binary.LittleEndian.Uint64(secondValues)) != 3.5 {
		t.Fatalf("second packed range = (%x,%v)", secondValues, ok)
	}

	group := PageRef{
		Offset: uint64(4 * testSuperblockPageSize), LogicalID: 8, Generation: 3,
		Length: 2 * testSuperblockPageSize, Kind: PageDocumentGroup,
	}
	sidecar := PageRef{
		Offset: uint64(12 * testSuperblockPageSize), LogicalID: 12, Generation: 3,
		Length: uint32(len(page)), Kind: PageFloat64Group,
	}
	group, err = AttachDocumentGroupFloat64Sidecar(group, sidecar, testSuperblockPageSize)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := DocumentGroupFloat64Sidecar(group, testSuperblockPageSize)
	if err != nil || !found || got != sidecar {
		t.Fatalf("derived sidecar = (%+v,%v,%v), want %+v", got, found, err, sidecar)
	}
	other := PageRef{
		Offset: uint64(8 * testSuperblockPageSize), LogicalID: 10, Generation: 3,
		Length: 2 * testSuperblockPageSize, Kind: PageDocumentGroup,
	}
	other, err = AttachDocumentGroupFloat64Sidecar(other, sidecar, testSuperblockPageSize)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err = DocumentGroupFloat64Sidecar(other, testSuperblockPageSize)
	if err != nil || !found || got != sidecar {
		t.Fatalf("shared derived sidecar = (%+v,%v,%v), want %+v", got, found, err, sidecar)
	}
	maxForward := DocumentGroupFloat64MaxForwardBytes(testSuperblockPageSize)
	if want := uint64(documentGroupFloat64OffsetMask) * uint64(testSuperblockPageSize); maxForward != want {
		t.Fatalf("max sidecar forward bytes = %d, want %d", maxForward, want)
	}
	tooFar := sidecar
	tooFar.Offset = group.Offset + maxForward + uint64(testSuperblockPageSize)
	if _, err := AttachDocumentGroupFloat64Sidecar(
		group, tooFar, testSuperblockPageSize,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("out-of-range sidecar = %v, want %v", err, ErrInvalidWrite)
	}

	reuse := make([]byte, len(page))
	header := Float64GroupHeader{
		StoreID: testStoreID, Generation: 3, LogicalID: 12, PageSize: uint32(len(page)),
		FirstChunk: 7, ChunkCount: 2, RowCount: 3, ColumnCount: 1,
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if _, ok := Float64GroupSize(chunks, testSuperblockPageSize); !ok {
			panic("Float64GroupSize failed")
		}
		if _, encodeErr := EncodeFloat64Group(reuse, header, chunks, 20); encodeErr != nil {
			panic(encodeErr)
		}
	}); allocs != 0 {
		t.Fatalf("float64 group warm allocations = %.2f, want 0", allocs)
	}
	if _, err := OpenFloat64Group(reuse, 9, 20); err != nil {
		t.Fatal(err)
	}
}

func TestFloat64GroupRejectsResealedCorruption(t *testing.T) {
	page, _ := encodeTestFloat64Group(t)
	_, payload, err := OpenPage(page)
	if err != nil {
		t.Fatal(err)
	}
	chunkBytes := int(binary.LittleEndian.Uint32(payload[16:20]))
	directoryBytes := int(binary.LittleEndian.Uint32(payload[20:24]))
	data := PageHeaderSize + Float64GroupPayloadHeaderSize + chunkBytes + directoryBytes
	cases := []struct {
		name   string
		offset int
		value  byte
	}{
		{"row count", PageHeaderSize + 12, 4},
		{"reserved byte", PageHeaderSize + 28, 1},
		{"mask outside live", data, 0xff},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := append([]byte(nil), page...)
			corrupt[tc.offset] = tc.value
			if _, err := SealPage(corrupt); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenFloat64Group(corrupt, 9, 20); !errors.Is(err, ErrFloat64GroupCorrupt) {
				t.Fatalf("corruption = %v, want %v", err, ErrFloat64GroupCorrupt)
			}
		})
	}
}

func TestFloat64GroupAdaptiveIntegerWidthsAndSignedZero(t *testing.T) {
	tests := []struct {
		name     string
		values   [3]float64
		encoding Float64GroupEncoding
	}{
		{"uint8", [3]float64{1, 255, 7}, Float64GroupUint8},
		{"uint16", [3]float64{1, 300, 65535}, Float64GroupUint16},
		{"uint32", [3]float64{1, 70000, math.MaxUint32}, Float64GroupUint32},
		{
			"signed zero",
			[3]float64{math.Copysign(0, -1), 1, 2},
			Float64GroupFloat64LE,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunks := testDocumentGroupChunks()
			chunks[0].Columns.Values[0] = test.values[0]
			chunks[0].Columns.Values[2] = test.values[1]
			chunks[1].Columns.Values[1] = test.values[2]
			size, ok := Float64GroupSize(chunks, testSuperblockPageSize)
			if !ok {
				t.Fatal("Float64GroupSize rejected adaptive fixture")
			}
			page := make([]byte, size)
			page, err := EncodeFloat64Group(page, Float64GroupHeader{
				StoreID: testStoreID, Generation: 3, LogicalID: 12, PageSize: size,
				FirstChunk: 7, ChunkCount: 2, RowCount: 3, ColumnCount: 1,
			}, chunks, 20)
			if err != nil {
				t.Fatal(err)
			}
			view, err := OpenFloat64Group(page, 9, 20)
			if err != nil {
				t.Fatal(err)
			}
			values, encoding, ok := view.Float64ColumnValues(0)
			if !ok || encoding != test.encoding ||
				len(values) != len(test.values)*test.encoding.ByteWidth() {
				t.Fatalf(
					"packed values = (%x,%v,%v), want width %d",
					values, encoding, ok, test.encoding.ByteWidth(),
				)
			}
			first, ok := view.Chunk(7)
			if !ok {
				t.Fatal("first adaptive chunk missing")
			}
			column, ok := first.Float64Column(0)
			if !ok {
				t.Fatal("first adaptive column missing")
			}
			got, ok := column.Lookup(0)
			if !ok || math.Float64bits(got) != math.Float64bits(test.values[0]) {
				t.Fatalf(
					"first adaptive value = (%016x,%v), want %016x",
					math.Float64bits(got), ok, math.Float64bits(test.values[0]),
				)
			}
		})
	}
}
