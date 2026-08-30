package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestPageCatalogCanonicalOrderDedupAndOwnership(t *testing.T) {
	input := PageCatalogDefinition{
		Indexes: []PageCatalogIndex{
			{Name: "z_by_score", Paths: []string{"/score"}},
			{Name: "tenant_status_alias", Paths: []string{"/tenant", "/status"}},
			{Name: "tenant_status", Paths: []string{"/tenant", "/status"}, Unique: true},
		},
		SkipPaths: []string{"/score", "/profile/region"},
		Schema: &PageCatalogSchema{
			Root: PageCatalogSchemaObject,
			Fields: []PageCatalogSchemaField{
				{Path: "/tenant", Types: PageCatalogSchemaString, Required: true},
				{
					Path:  "/score",
					Types: PageCatalogSchemaNumber | PageCatalogSchemaInteger,
				},
			},
		},
	}
	catalog, err := BuildCanonicalPageCatalog(input)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.PhysicalIndexCount() != 2 {
		t.Fatalf("physical indexes = %d, want 2", catalog.PhysicalIndexCount())
	}
	got := catalog.Definition()
	if names := []string{
		got.Indexes[0].Name, got.Indexes[1].Name, got.Indexes[2].Name,
	}; !slices.Equal(names, []string{
		"tenant_status", "tenant_status_alias", "z_by_score",
	}) {
		t.Fatalf("alias order = %q", names)
	}
	if !got.Indexes[0].Unique || got.Indexes[1].Unique || got.Indexes[2].Unique {
		t.Fatalf("canonical unique flags = %+v", got.Indexes)
	}
	if got.Schema.Fields[0].Path != "/score" ||
		got.Schema.Fields[0].Types != PageCatalogSchemaNumber ||
		got.Schema.Fields[1].Path != "/tenant" {
		t.Fatalf("canonical schema = %+v", got.Schema)
	}
	if !slices.Equal(got.SkipPaths, []string{"/profile/region", "/score"}) {
		t.Fatalf("canonical skip paths = %q", got.SkipPaths)
	}

	reordered := PageCatalogDefinition{
		Indexes: []PageCatalogIndex{
			{Name: "tenant_status", Paths: []string{"/tenant", "/status"}, Unique: true},
			{Name: "z_by_score", Paths: []string{"/score"}},
			{Name: "tenant_status_alias", Paths: []string{"/tenant", "/status"}},
		},
		SkipPaths: []string{"/profile/region", "/score"},
		Schema: &PageCatalogSchema{
			Root: PageCatalogSchemaObject,
			Fields: []PageCatalogSchemaField{
				{
					Path:  "/score",
					Types: PageCatalogSchemaInteger | PageCatalogSchemaNumber,
				},
				{Path: "/tenant", Types: PageCatalogSchemaString, Required: true},
			},
		},
	}
	other, err := BuildCanonicalPageCatalog(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if !catalog.Equal(other) {
		t.Fatal("caller order changed canonical bytes")
	}
	opened, err := OpenCanonicalPageCatalog(catalog.AppendCanonical(nil))
	if err != nil || !opened.Equal(catalog) {
		t.Fatalf("OpenCanonicalPageCatalog = (%v,%v)", opened, err)
	}

	// Mutating caller and returned declarative storage cannot mutate the image.
	input.Indexes[0].Name = "mutated"
	input.Indexes[1].Paths[0] = "/mutated"
	input.SkipPaths[0] = "/mutated"
	got.Indexes[0].Name = "also_mutated"
	got.Indexes[0].Unique = false
	got.SkipPaths[0] = "/also_mutated"
	got.Schema.Fields[0].Path = "/also_mutated"
	if !catalog.Equal(other) {
		t.Fatal("catalog aliases caller or returned definition storage")
	}
}

func TestPageCatalogUniqueAliasFlagIsCanonicalIdentity(t *testing.T) {
	definition := PageCatalogDefinition{Indexes: []PageCatalogIndex{
		{Name: "ordinary", Paths: []string{"/value"}},
		{Name: "unique", Paths: []string{"/value"}, Unique: true},
	}}
	catalog, err := BuildCanonicalPageCatalog(definition)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenCanonicalPageCatalog(catalog.AppendCanonical(nil))
	if err != nil {
		t.Fatal(err)
	}
	got := opened.Definition()
	if len(got.Indexes) != 2 || got.Indexes[0].Unique ||
		!got.Indexes[1].Unique || opened.PhysicalIndexCount() != 1 {
		t.Fatalf("opened unique aliases = %+v, physical=%d",
			got.Indexes, opened.PhysicalIndexCount())
	}
	definition.Indexes[1].Unique = false
	nonUnique, err := BuildCanonicalPageCatalog(definition)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Equal(nonUnique) || catalog.Digest() == nonUnique.Digest() {
		t.Fatal("unique alias flag did not affect canonical catalog identity")
	}
}

func TestPageCatalogNilEmptyAndExplicitSchemaCanonicality(t *testing.T) {
	nilCatalog, err := BuildCanonicalPageCatalog(PageCatalogDefinition{})
	if err != nil {
		t.Fatal(err)
	}
	emptyCatalog, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Indexes: []PageCatalogIndex{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !nilCatalog.Equal(emptyCatalog) ||
		nilCatalog.CanonicalSize() != 0 ||
		nilCatalog.Digest() != ([PageCatalogDigestSize]byte{}) ||
		nilCatalog.SegmentCount() != 0 {
		t.Fatalf(
			"nil/empty = equal %v size %d digest %x segments %d",
			nilCatalog.Equal(emptyCatalog), nilCatalog.CanonicalSize(),
			nilCatalog.Digest(),
			nilCatalog.SegmentCount(),
		)
	}
	openedEmpty, err := OpenCanonicalPageCatalog(nil)
	if err != nil || !openedEmpty.Equal(nilCatalog) {
		t.Fatalf("OpenCanonicalPageCatalog(nil) = (%+v,%v)", openedEmpty, err)
	}
	chainEmpty, err := OpenPageCatalogChain(nil, PageCatalogBounds{})
	if err != nil || !chainEmpty.Equal(nilCatalog) {
		t.Fatalf("OpenPageCatalogChain(nil) = (%+v,%v)", chainEmpty, err)
	}
	if _, err := OpenPageCatalogChain(nil, PageCatalogBounds{
		ExpectedDigest: [PageCatalogDigestSize]byte{1},
	}); !errors.Is(err, ErrPageCatalogCorrupt) {
		t.Fatalf("empty chain with digest = %v", err)
	}
	if _, err := OpenPageCatalogChain(nil, PageCatalogBounds{
		Generation: 1,
	}); !errors.Is(err, ErrPageCatalogCorrupt) {
		t.Fatalf("empty chain with identity bounds = %v", err)
	}
	explicitAny, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Schema: &PageCatalogSchema{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if nilCatalog.Equal(explicitAny) ||
		explicitAny.Definition().Schema.Root != PageCatalogSchemaAny {
		t.Fatal("absent schema collapsed into explicit SchemaAny")
	}
}

func TestPageCatalogFrontCodingAndExactCanonicalGolden(t *testing.T) {
	catalog, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Indexes: []PageCatalogIndex{
			{Name: "by_region", Paths: []string{"/profile/region"}},
			{Name: "by_role", Paths: []string{"/profile/role"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	image := catalog.AppendCanonical(nil)
	stringBytes := int(binary.LittleEndian.Uint32(image[36:40]))
	stringCount := int(binary.LittleEndian.Uint32(image[20:24]))
	cursor := PageCatalogCanonicalHeaderSize
	previous := ""
	saved := 0
	for range stringCount {
		prefix := int(binary.LittleEndian.Uint16(image[cursor : cursor+2]))
		suffix := int(binary.LittleEndian.Uint16(image[cursor+2 : cursor+4]))
		value := previous[:prefix] + string(image[cursor+4:cursor+4+suffix])
		saved += prefix
		previous = value
		cursor += 4 + suffix
	}
	if cursor != PageCatalogCanonicalHeaderSize+stringBytes || saved < 14 {
		t.Fatalf("front coding cursor/saved = %d/%d", cursor, saved)
	}
	const wantHex = "534a43415443303000000000400000007e000000040000000200000002000000000000002e00000008000000000000000000000000000000000000000000000000000f002f70726f66696c652f726567696f6e0a0003006f6c650000090062795f726567696f6e040003006f6c6501000000010001000200000003000100"
	if fmt.Sprintf("%x", image) != wantHex {
		t.Fatalf("canonical bytes changed:\n%x", image)
	}
}

func TestPageCatalogRejectsInvalidDefinitions(t *testing.T) {
	tooWide := make([]string, PageCatalogMaxIndexColumns+1)
	for i := range tooWide {
		tooWide[i] = fmt.Sprintf("/p%d", i)
	}
	tooManyPhysical := make([]PageCatalogIndex, PageCatalogMaxPhysicalIndexes+1)
	for i := range tooManyPhysical {
		tooManyPhysical[i] = PageCatalogIndex{
			Name: fmt.Sprintf("i%03d", i), Paths: []string{fmt.Sprintf("/p%03d", i)},
		}
	}
	tooManyLogical := make(
		[]PageCatalogIndex, PageCatalogMaxLogicalIndexes+1,
	)
	tooManySchema := make(
		[]PageCatalogSchemaField, PageCatalogMaxSchemaFields+1,
	)
	tooManySkipPaths := make([]string, PageCatalogMaxSkipIndexes+1)
	for i := range tooManySkipPaths {
		tooManySkipPaths[i] = fmt.Sprintf("/skip/%d", i)
	}
	tests := []PageCatalogDefinition{
		{Indexes: tooManyLogical},
		{SkipPaths: tooManySkipPaths},
		{SkipPaths: []string{"not/a/pointer"}},
		{SkipPaths: []string{"/bad~2escape"}},
		{SkipPaths: []string{"/same", "/same"}},
		{Schema: &PageCatalogSchema{Fields: tooManySchema}},
		{Indexes: []PageCatalogIndex{{Name: "", Paths: []string{"/a"}}}},
		{Indexes: []PageCatalogIndex{{Name: "a"}}},
		{Indexes: []PageCatalogIndex{{Name: "a", Paths: tooWide}}},
		{Indexes: []PageCatalogIndex{{Name: "a", Paths: []string{"not/a/pointer"}}}},
		{Indexes: []PageCatalogIndex{{Name: "a", Paths: []string{"/bad~2escape"}}}},
		{Indexes: []PageCatalogIndex{
			{Name: "a", Paths: []string{"/a"}},
			{Name: "a", Paths: []string{"/b"}},
		}},
		{Indexes: tooManyPhysical},
		{Schema: &PageCatalogSchema{Root: 1 << 15}},
		{Schema: &PageCatalogSchema{Fields: []PageCatalogSchemaField{{
			Path: "", Types: PageCatalogSchemaString,
		}}}},
		{Schema: &PageCatalogSchema{Fields: []PageCatalogSchemaField{
			{Path: "/a", Types: PageCatalogSchemaString},
			{Path: "/a", Types: PageCatalogSchemaNumber},
		}}},
		{Schema: &PageCatalogSchema{Fields: []PageCatalogSchemaField{{
			Path: "/a", Types: 0,
		}}}},
		{Indexes: []PageCatalogIndex{{
			Name: string([]byte{0xff}), Paths: []string{"/a"},
		}}},
	}
	for i, definition := range tests {
		if _, err := BuildCanonicalPageCatalog(definition); !errors.Is(
			err, ErrPageCatalogDefinition,
		) {
			t.Fatalf("case %d = %v, want %v", i, err, ErrPageCatalogDefinition)
		}
	}
}

func TestPageCatalogCompactAccountingAtOrdinaryAndMaximumCounts(t *testing.T) {
	ordinary, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Indexes: []PageCatalogIndex{
			{Name: "a", Paths: []string{"/tenant", "/status"}},
			{Name: "a_alias", Paths: []string{"/tenant", "/status"}},
			{Name: "score", Paths: []string{"/score"}},
		},
		Schema: &PageCatalogSchema{
			Root: PageCatalogSchemaObject,
			Fields: []PageCatalogSchemaField{{
				Path: "/tenant", Types: PageCatalogSchemaString, Required: true,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertPageCatalogCompactAccounting(t, ordinary, 6)

	maximum := PageCatalogDefinition{
		Indexes: make(
			[]PageCatalogIndex, PageCatalogMaxLogicalIndexes,
		),
		Schema: &PageCatalogSchema{
			Root: PageCatalogSchemaObject,
			Fields: make(
				[]PageCatalogSchemaField, PageCatalogMaxSchemaFields,
			),
		},
	}
	maximum.SkipPaths = make([]string, PageCatalogMaxSkipIndexes)
	for i := range maximum.SkipPaths {
		maximum.SkipPaths[i] = fmt.Sprintf("/skip/%02d", i)
	}
	physicalPaths := make(
		[][]string, PageCatalogMaxPhysicalIndexes,
	)
	for physicalID := range physicalPaths {
		physicalPaths[physicalID] = make(
			[]string, PageCatalogMaxIndexColumns,
		)
		for column := range physicalPaths[physicalID] {
			physicalPaths[physicalID][column] = fmt.Sprintf(
				"/physical/%02d/%d", physicalID, column,
			)
		}
	}
	for i := range maximum.Indexes {
		maximum.Indexes[i] = PageCatalogIndex{
			Name: fmt.Sprintf("alias_%04d", i),
			Paths: slices.Clone(
				physicalPaths[i%PageCatalogMaxPhysicalIndexes],
			),
		}
	}
	for i := range maximum.Schema.Fields {
		maximum.Schema.Fields[i] = PageCatalogSchemaField{
			Path:     fmt.Sprintf("/schema/%04d", i),
			Types:    PageCatalogSchemaString,
			Required: i&1 != 0,
		}
	}
	maxCatalog, err := BuildCanonicalPageCatalog(maximum)
	if err != nil {
		t.Fatal(err)
	}
	assertPageCatalogCompactAccounting(
		t, maxCatalog, PageCatalogMaxUniqueStrings,
	)
	image := maxCatalog.canonical
	if got := binary.LittleEndian.Uint32(image[20:24]); got !=
		PageCatalogMaxUniqueStrings {
		t.Fatalf("maximum unique strings = %d", got)
	}
	if got := binary.LittleEndian.Uint32(image[24:28]); got !=
		PageCatalogMaxPhysicalIndexes {
		t.Fatalf("maximum physical indexes = %d", got)
	}
	if got := binary.LittleEndian.Uint32(image[28:32]); got !=
		PageCatalogMaxLogicalIndexes {
		t.Fatalf("maximum aliases = %d", got)
	}
	if got := binary.LittleEndian.Uint32(image[32:36]); got !=
		PageCatalogMaxSchemaFields {
		t.Fatalf("maximum schema fields = %d", got)
	}

	maxSegments := (PageCatalogMaxCanonicalBytes +
		PageCatalogSegmentDataCapacity - 1) /
		PageCatalogSegmentDataCapacity
	encodedSegments := pageCatalogSegmentCountFor(
		PageCatalogMaxCanonicalBytes, PageCatalogSegmentPageSize,
	)
	if maxSegments > int(^uint16(0)) ||
		encodedSegments != uint16(maxSegments) {
		t.Fatalf(
			"maximum segments = %d encoded %d",
			maxSegments, encodedSegments,
		)
	}
	if PageCatalogMaxUniqueStrings > int(^uint16(0))+1 ||
		PageCatalogMaxPhysicalIndexes > int(^uint8(0))+1 {
		t.Fatal("declared maxima do not fit compact identifiers")
	}
}

func assertPageCatalogCompactAccounting(
	t *testing.T, catalog *CanonicalPageCatalog, wantStrings int,
) {
	t.Helper()
	definition := catalog.Definition()
	values, _, err := pageCatalogStrings(definition, catalog.physical)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != wantStrings {
		t.Fatalf("unique strings = %d, want %d", len(values), wantStrings)
	}
	stringBytes := 0
	previous := ""
	for _, value := range values {
		prefix := pageCatalogCommonPrefix(previous, value)
		stringBytes += 4 + len(value) - prefix
		previous = value
	}
	physicalBytes := 0
	for _, physical := range catalog.physical {
		physicalBytes += 2 + 2*len(physical.paths)
	}
	fieldCount := 0
	if definition.Schema != nil {
		fieldCount = len(definition.Schema.Fields)
	}
	wantTotal := PageCatalogCanonicalHeaderSize +
		stringBytes + physicalBytes +
		len(definition.Indexes)*4 +
		len(definition.SkipPaths)*2 +
		fieldCount*6
	if catalog.CanonicalSize() != wantTotal ||
		int(binary.LittleEndian.Uint32(catalog.canonical[16:20])) != wantTotal ||
		int(binary.LittleEndian.Uint32(catalog.canonical[36:40])) != stringBytes ||
		int(binary.LittleEndian.Uint32(catalog.canonical[40:44])) != physicalBytes {
		t.Fatalf(
			"accounting size/string/physical = %d/%d/%d, want %d/%d/%d",
			catalog.CanonicalSize(),
			binary.LittleEndian.Uint32(catalog.canonical[36:40]),
			binary.LittleEndian.Uint32(catalog.canonical[40:44]),
			wantTotal, stringBytes, physicalBytes,
		)
	}
}

func TestPageCatalogRejectsNonCanonicalImages(t *testing.T) {
	catalog, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Indexes: []PageCatalogIndex{
			{Name: "a", Paths: []string{"/aa"}},
			{Name: "b", Paths: []string{"/ab"}},
		},
		SkipPaths: []string{"/skip/a", "/skip/b"},
		Schema: &PageCatalogSchema{
			Fields: []PageCatalogSchemaField{{
				Path: "/s", Types: PageCatalogSchemaString,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	image := catalog.AppendCanonical(nil)
	stringBytes := int(binary.LittleEndian.Uint32(image[36:40]))
	physicalBytes := int(binary.LittleEndian.Uint32(image[40:44]))
	aliasStart := PageCatalogCanonicalHeaderSize + stringBytes + physicalBytes
	skipStart := aliasStart + 2*4
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"magic", func(src []byte) { src[0] ^= 1 }},
		{"reserved", func(src []byte) { src[46] = 1 }},
		{"total", func(src []byte) { binary.LittleEndian.PutUint32(src[16:20], 1) }},
		{"front prefix", func(src []byte) {
			binary.LittleEndian.PutUint16(
				src[PageCatalogCanonicalHeaderSize:PageCatalogCanonicalHeaderSize+2], 1,
			)
		}},
		{"physical reserved", func(src []byte) {
			src[PageCatalogCanonicalHeaderSize+stringBytes+1] = 1
		}},
		{"alias order", func(src []byte) {
			first := slices.Clone(src[aliasStart : aliasStart+4])
			copy(src[aliasStart:aliasStart+4], src[aliasStart+4:aliasStart+8])
			copy(src[aliasStart+4:aliasStart+8], first)
		}},
		{"alias flags", func(src []byte) { src[aliasStart+3] = 2 }},
		{"skip order", func(src []byte) {
			first := slices.Clone(src[skipStart : skipStart+2])
			copy(src[skipStart:skipStart+2], src[skipStart+2:skipStart+4])
			copy(src[skipStart+2:skipStart+4], first)
		}},
		{"schema reserved", func(src []byte) { src[len(src)-1] = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := slices.Clone(image)
			test.mutate(corrupt)
			if _, err := OpenCanonicalPageCatalog(corrupt); !errors.Is(
				err, ErrPageCatalogCorrupt,
			) {
				t.Fatalf("OpenCanonicalPageCatalog = %v", err)
			}
		})
	}

	// Attacker-controlled max+1 counts fail before any count-sized allocation.
	for _, count := range []struct {
		offset int
		value  uint32
	}{
		{20, PageCatalogMaxUniqueStrings + 1},
		{24, PageCatalogMaxPhysicalIndexes + 1},
		{28, PageCatalogMaxLogicalIndexes + 1},
		{32, PageCatalogMaxSchemaFields + 1},
	} {
		corrupt := slices.Clone(image)
		binary.LittleEndian.PutUint32(
			corrupt[count.offset:count.offset+4], count.value,
		)
		if _, err := OpenCanonicalPageCatalog(corrupt); !errors.Is(
			err, ErrPageCatalogCorrupt,
		) {
			t.Fatalf("oversize count at %d = %v", count.offset, err)
		}
	}
}

func TestPageCatalogSegmentChainRoundTripAndCorruption(t *testing.T) {
	definition := PageCatalogDefinition{
		Indexes: []PageCatalogIndex{
			{Name: "tenant", Paths: []string{"/tenant"}},
			{Name: "tenant_alias", Paths: []string{"/tenant"}},
		},
		Schema: &PageCatalogSchema{Root: PageCatalogSchemaObject},
	}
	for i := range 700 {
		definition.Schema.Fields = append(
			definition.Schema.Fields,
			PageCatalogSchemaField{
				Path:     fmt.Sprintf("/records/%04d/value", i),
				Types:    PageCatalogSchemaNumber,
				Required: i%3 == 0,
			},
		)
	}
	catalog, err := BuildCanonicalPageCatalog(definition)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.SegmentCount() < 2 {
		t.Fatalf("catalog unexpectedly fits %d segment", catalog.SegmentCount())
	}
	pages, bounds := encodePageCatalogTestChain(t, catalog, testStoreID, 7)
	digest := catalog.Digest()
	bounds.ExpectedDigest = digest
	if PageCatalogSegmentPayloadHeaderSize != 64 ||
		PageCatalogSegmentDataCapacity != 3960 {
		t.Fatalf(
			"segment header/capacity = %d/%d",
			PageCatalogSegmentPayloadHeaderSize,
			PageCatalogSegmentDataCapacity,
		)
	}
	firstPayload := pages[0].Page[PageHeaderSize:]
	if binary.LittleEndian.Uint32(firstPayload[0:4]) !=
		pageCatalogSegmentVersion ||
		binary.LittleEndian.Uint16(firstPayload[4:6]) != 0 ||
		binary.LittleEndian.Uint16(firstPayload[6:8]) !=
			uint16(len(pages)) ||
		binary.LittleEndian.Uint32(firstPayload[8:12]) !=
			uint32(catalog.CanonicalSize()) ||
		binary.LittleEndian.Uint16(firstPayload[12:14]) !=
			uint16(min(
				PageCatalogSegmentDataCapacity,
				catalog.CanonicalSize(),
			)) ||
		!allZero(firstPayload[14:16]) ||
		!bytes.Equal(firstPayload[16:32], digest[:]) {
		t.Fatal("compact segment header offsets changed")
	}
	opened, err := OpenPageCatalogChain(pages, bounds)
	if err != nil || !opened.Equal(catalog) ||
		opened.PhysicalIndexCount() != 1 {
		t.Fatalf(
			"OpenPageCatalogChain = (equal=%v physical=%d err=%v)",
			opened != nil && opened.Equal(catalog),
			func() int {
				if opened == nil {
					return -1
				}
				return opened.PhysicalIndexCount()
			}(),
			err,
		)
	}
	for i, page := range pages {
		view, err := OpenPageCatalogSegment(page.Page, bounds)
		if err != nil || view.Header().Ordinal != uint16(i) ||
			view.DataOffset() != i*PageCatalogSegmentDataCapacity {
			t.Fatalf("segment %d = (%+v,%v)", i, view, err)
		}
	}

	for byteIndex := range pages[0].Page {
		pages[0].Page[byteIndex] ^= 1
		if _, err := OpenPageCatalogSegment(pages[0].Page, bounds); !errors.Is(
			err, ErrPageCatalogCorrupt,
		) {
			t.Fatalf("byte %d corruption = %v", byteIndex, err)
		}
		pages[0].Page[byteIndex] ^= 1
	}

	t.Run("link mismatch", func(t *testing.T) {
		broken := clonePageCatalogTestChain(pages)
		payloadOffset := PageHeaderSize + 32
		binary.LittleEndian.PutUint64(
			broken[0].Page[payloadOffset:payloadOffset+8],
			broken[0].Ref.Offset,
		)
		if _, err := SealPage(broken[0].Page); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenPageCatalogChain(broken, bounds); !errors.Is(
			err, ErrPageCatalogCorrupt,
		) {
			t.Fatalf("link graft = %v", err)
		}
	})
	t.Run("reference identity", func(t *testing.T) {
		broken := clonePageCatalogTestChain(pages)
		broken[0].Ref.LogicalID++
		if _, err := OpenPageCatalogChain(broken, bounds); !errors.Is(
			err, ErrPageCatalogCorrupt,
		) {
			t.Fatalf("reference graft = %v", err)
		}
	})
	t.Run("cross store", func(t *testing.T) {
		otherID := testStoreID
		otherID[0] ^= 0xff
		foreign, _ := encodePageCatalogTestChain(t, catalog, otherID, 7)
		if _, err := OpenPageCatalogChain(foreign, bounds); !errors.Is(
			err, ErrPageCatalogCorrupt,
		) {
			t.Fatalf("cross-Store graft = %v", err)
		}
	})
	t.Run("segment reorder", func(t *testing.T) {
		broken := clonePageCatalogTestChain(pages)
		broken[0], broken[1] = broken[1], broken[0]
		if _, err := OpenPageCatalogChain(broken, bounds); !errors.Is(
			err, ErrPageCatalogCorrupt,
		) {
			t.Fatalf("reordered chain = %v", err)
		}
	})
	t.Run("reserved resealed", func(t *testing.T) {
		broken := clonePageCatalogTestChain(pages)
		broken[0].Page[PageHeaderSize+14] = 1
		if _, err := SealPage(broken[0].Page); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenPageCatalogChain(broken, bounds); !errors.Is(
			err, ErrPageCatalogCorrupt,
		) {
			t.Fatalf("reserved bytes = %v", err)
		}
	})
	t.Run("wrong fast digest", func(t *testing.T) {
		wrong := bounds
		wrong.ExpectedDigest[0] ^= 1
		if _, err := OpenPageCatalogChain(pages, wrong); !errors.Is(
			err, ErrPageCatalogCorrupt,
		) {
			t.Fatalf("wrong digest = %v", err)
		}
	})
}

func TestPageCatalogSegmentWriteAndReferenceBounds(t *testing.T) {
	catalog, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Schema: &PageCatalogSchema{},
	})
	if err != nil {
		t.Fatal(err)
	}
	layout, _ := MutableStoreLayout(PageCatalogSegmentPageSize)
	bounds := PageCatalogBounds{
		StoreID: testStoreID, Generation: 7,
		PageSize:       PageCatalogSegmentPageSize,
		DataStart:      layout.DataStart,
		FileEnd:        layout.DataStart + uint64(PageCatalogSegmentPageSize),
		NextLogicalID:  10,
		TotalBytes:     uint32(catalog.CanonicalSize()),
		ExpectedDigest: catalog.Digest(),
	}
	valid := PageCatalogSegmentHeader{
		StoreID: testStoreID, Generation: 7, LogicalID: 2,
	}
	for _, mutate := range []func(*PageCatalogSegmentHeader){
		func(header *PageCatalogSegmentHeader) { header.StoreID = [16]byte{} },
		func(header *PageCatalogSegmentHeader) { header.Generation = 0 },
		func(header *PageCatalogSegmentHeader) { header.Generation = 8 },
		func(header *PageCatalogSegmentHeader) { header.LogicalID = 0 },
		func(header *PageCatalogSegmentHeader) { header.LogicalID = 10 },
		func(header *PageCatalogSegmentHeader) { header.Ordinal = 1 },
		func(header *PageCatalogSegmentHeader) {
			header.Next = PageRef{
				Offset: layout.DataStart, LogicalID: 3, Generation: 7,
				Length: PageCatalogSegmentPageSize, Kind: PageCatalogSegment,
			}
		},
	} {
		header := valid
		mutate(&header)
		if _, err := EncodePageCatalogSegment(
			make([]byte, PageCatalogSegmentPageSize),
			header, catalog, bounds,
		); !errors.Is(err, ErrInvalidWrite) {
			t.Fatalf("invalid header %+v = %v", header, err)
		}
	}
}

func TestPageCatalogSegmentFollowsStoreAllocationQuantum(t *testing.T) {
	definition := PageCatalogDefinition{
		Schema: &PageCatalogSchema{Root: PageCatalogSchemaObject},
	}
	for i := range 2_000 {
		definition.Schema.Fields = append(
			definition.Schema.Fields,
			PageCatalogSchemaField{
				Path:  fmt.Sprintf("/wide/%04d", i),
				Types: PageCatalogSchemaString,
			},
		)
	}
	catalog, err := BuildCanonicalPageCatalog(definition)
	if err != nil {
		t.Fatal(err)
	}
	const pageSize = uint32(16 << 10)
	count, ok := catalog.SegmentCountFor(pageSize)
	if !ok || count < 2 || count >= catalog.SegmentCount() {
		t.Fatalf(
			"16 KiB segments = %d,%v; 4 KiB segments=%d",
			count, ok, catalog.SegmentCount(),
		)
	}
	pages, bounds := encodePageCatalogTestChainAtPageSize(
		t, catalog, testStoreID, 11, pageSize,
	)
	bounds.ExpectedDigest = catalog.Digest()
	opened, err := OpenPageCatalogChain(pages, bounds)
	if err != nil || !opened.Equal(catalog) {
		t.Fatalf("16 KiB chain = equal %v, error %v", opened != nil && opened.Equal(catalog), err)
	}
	capacity, ok := pageCatalogSegmentDataCapacity(pageSize)
	if !ok {
		t.Fatal("16 KiB catalog capacity rejected")
	}
	for i, page := range pages {
		if page.Ref.Length != pageSize || len(page.Page) != int(pageSize) {
			t.Fatalf("segment %d extent = %d/%d", i, page.Ref.Length, len(page.Page))
		}
		view, openErr := OpenPageCatalogSegment(page.Page, bounds)
		if openErr != nil || view.DataOffset() != i*capacity {
			t.Fatalf("segment %d = offset %d, error %v", i, view.DataOffset(), openErr)
		}
	}
	wrong := bounds
	wrong.PageSize = PageCatalogSegmentPageSize
	if _, err := OpenPageCatalogChain(pages, wrong); !errors.Is(err, ErrPageCatalogCorrupt) {
		t.Fatalf("wrong allocation quantum = %v", err)
	}
}

func encodePageCatalogTestChain(
	t testing.TB,
	catalog *CanonicalPageCatalog,
	storeID [16]byte,
	generation uint64,
) ([]PageCatalogChainPage, PageCatalogBounds) {
	return encodePageCatalogTestChainAtPageSize(
		t, catalog, storeID, generation, PageCatalogSegmentPageSize,
	)
}

func encodePageCatalogTestChainAtPageSize(
	t testing.TB,
	catalog *CanonicalPageCatalog,
	storeID [16]byte,
	generation uint64,
	pageSize uint32,
) ([]PageCatalogChainPage, PageCatalogBounds) {
	t.Helper()
	layout, err := MutableStoreLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	count, ok := catalog.SegmentCountFor(pageSize)
	if !ok {
		t.Fatalf("invalid catalog page size %d", pageSize)
	}
	refs := make([]PageRef, count)
	for i := range refs {
		refs[i] = PageRef{
			Offset:    layout.DataStart + uint64(i)*uint64(pageSize),
			LogicalID: uint64(10 + i), Generation: generation,
			Length: pageSize, Kind: PageCatalogSegment,
		}
	}
	bounds := PageCatalogBounds{
		StoreID: storeID, Generation: generation, PageSize: pageSize,
		DataStart:      layout.DataStart,
		FileEnd:        layout.DataStart + uint64(count)*uint64(pageSize),
		NextLogicalID:  uint64(10 + count),
		TotalBytes:     uint32(catalog.CanonicalSize()),
		ExpectedDigest: catalog.Digest(),
	}
	pages := make([]PageCatalogChainPage, count)
	for i := range pages {
		var next PageRef
		if i+1 < len(refs) {
			next = refs[i+1]
		}
		page, err := EncodePageCatalogSegment(
			make([]byte, pageSize),
			PageCatalogSegmentHeader{
				StoreID: storeID, Generation: generation,
				LogicalID: refs[i].LogicalID, Ordinal: uint16(i), Next: next,
			},
			catalog, bounds,
		)
		if err != nil {
			t.Fatal(err)
		}
		pages[i] = PageCatalogChainPage{Ref: refs[i], Page: page}
	}
	return pages, bounds
}

func clonePageCatalogTestChain(
	pages []PageCatalogChainPage,
) []PageCatalogChainPage {
	out := make([]PageCatalogChainPage, len(pages))
	for i, page := range pages {
		out[i] = PageCatalogChainPage{
			Ref: page.Ref, Page: slices.Clone(page.Page),
		}
	}
	return out
}

func TestPageCatalogKindIsDenseAndDistinct(t *testing.T) {
	if PageCatalogSegment != PageFreeIndex+1 ||
		PagePrimaryCatalog != PageCatalogSegment+1 ||
		PageCatalogSegment == PageFreeIndex ||
		PageCatalogSegment == PagePrimaryCatalog {
		t.Fatalf(
			"catalog kind = %d, want dense between free-index %d and primary %d",
			PageCatalogSegment, PageFreeIndex, PagePrimaryCatalog,
		)
	}
}

func TestPageCatalogCanonicalImageDoesNotExposeMutableBytes(t *testing.T) {
	catalog, err := BuildCanonicalPageCatalog(PageCatalogDefinition{
		Indexes: []PageCatalogIndex{{Name: "x", Paths: []string{"/x"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	a := catalog.AppendCanonical(nil)
	b := catalog.AppendCanonical(nil)
	a[0] ^= 1
	if bytes.Equal(a, b) || string(b[0:8]) != string(pageCatalogCanonicalMagic[:]) {
		t.Fatal("AppendCanonical aliases catalog storage")
	}
}
