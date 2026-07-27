package vnext

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"strings"
	"testing"
)

var testIdentity = Identity{
	StoreID:    [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	Generation: 7,
	LogicalID:  19,
}

func TestKeyedFingerprintSipHashVectors(t *testing.T) {
	seed := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	vectors := []uint64{
		0x726fdb47dd0e0e31,
		0x74f839c593dc67fd,
		0x0d6c8009d9a94f5a,
		0x85676696d7fb7e2d,
	}
	var key string
	for length, want := range vectors {
		if got := KeyedFingerprint(seed, key); got != want {
			t.Fatalf("length %d = %016x, want %016x", length, got, want)
		}
		key += string(rune(length))
	}
}

func TestFingerprintPageExactCollisionVerification(t *testing.T) {
	const collision = uint64(0x9a11223344556677)
	entries := []FingerprintEntry{
		{Hash: 3, Location: Location{BlockID: 1, Slot: 2}},
		{Hash: collision, Location: Location{BlockID: 7, Slot: 1}},
		{Hash: collision, Location: Location{BlockID: 9, Slot: 63}},
		{Hash: ^uint64(0) - 1, Location: Location{BlockID: 11, Slot: 0}},
	}
	page, err := EncodeFingerprintPage(make([]byte, FingerprintPageSize), testIdentity, entries)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenFingerprintPage(page)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	location, found := view.Lookup(collision, func(candidate Location) bool {
		checked++
		return candidate.BlockID == 9
	})
	if !found || location != (Location{BlockID: 9, Slot: 63}) || checked != 2 {
		t.Fatalf("collision lookup = (%+v,%v), checked %d", location, found, checked)
	}
	if _, found := view.Lookup(collision, nil); found {
		t.Fatal("fingerprint-only lookup returned an unverified hit")
	}
	checked = 0
	if _, found := view.Lookup(4, func(Location) bool {
		checked++
		return true
	}); found || checked != 0 {
		t.Fatalf("miss = %v after %d verifications", found, checked)
	}
}

func TestFingerprintPageCapacitySpaceAndCorruption(t *testing.T) {
	entries := make([]FingerprintEntry, FingerprintPageCapacity)
	for i := range entries {
		entries[i] = FingerprintEntry{
			Hash: uint64(i) << 32,
			Location: Location{
				BlockID: uint32(i + 1),
				Slot:    uint8(i % RawBlockSlotCount),
			},
		}
	}
	page, err := EncodeFingerprintPage(make([]byte, FingerprintPageSize), testIdentity, entries)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFingerprintPage(page); err != nil {
		t.Fatal(err)
	}
	seventyPercent := FingerprintPageCapacity * 7 / 10
	if bytesPerKey := float64(FingerprintPageSize) / float64(seventyPercent); bytesPerKey > 24 {
		t.Fatalf("70%% occupancy = %.2f bytes/key, want <= 24", bytesPerKey)
	}
	tooMany := append(entries, FingerprintEntry{
		Hash: ^uint64(0), Location: Location{BlockID: 1 << 20},
	})
	if _, err := EncodeFingerprintPage(
		make([]byte, FingerprintPageSize),
		testIdentity,
		tooMany,
	); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("over-capacity encode = %v", err)
	}
	for i := range page {
		corruptPage := slices.Clone(page)
		corruptPage[i] ^= 1
		if _, err := OpenFingerprintPage(corruptPage); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("byte %d corruption = %v", i, err)
		}
	}
}

func TestFingerprintPageFullCollisionRunIsExplicitAndExact(t *testing.T) {
	const hash = uint64(0xfeedfacecafebeef)
	entries := make([]FingerprintEntry, FingerprintPageCapacity)
	for i := range entries {
		entries[i] = FingerprintEntry{
			Hash: hash,
			Location: Location{
				BlockID: uint32(i + 1),
				Slot:    uint8(i % RawBlockSlotCount),
			},
		}
	}
	page, err := EncodeFingerprintPage(make([]byte, FingerprintPageSize), testIdentity, entries)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenFingerprintPage(page)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	location, ok := view.Lookup(hash, func(candidate Location) bool {
		checked++
		return candidate == entries[len(entries)-1].Location
	})
	if !ok || location != entries[len(entries)-1].Location ||
		checked != FingerprintPageCapacity {
		t.Fatalf("full collision lookup = (%+v,%v), checked %d", location, ok, checked)
	}
}

func TestFingerprintPageRejectsResealedStructureCorruption(t *testing.T) {
	entries := []FingerprintEntry{
		{Hash: 1, Location: Location{BlockID: 1}},
		{Hash: 2, Location: Location{BlockID: 2}},
	}
	page, err := EncodeFingerprintPage(make([]byte, FingerprintPageSize), testIdentity, entries)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"bucket order", func(p []byte) {
			at := FrameHeaderSize + FingerprintPayloadHeaderSize + 2
			binary.LittleEndian.PutUint16(p[at:at+2], 0)
		}},
		{"location", func(p []byte) {
			at := FrameHeaderSize + FingerprintPayloadHeaderSize +
				FingerprintBucketBytes + 7
			binary.LittleEndian.PutUint32(p[at:at+4], 0)
		}},
		{"range", func(p []byte) {
			binary.LittleEndian.PutUint64(p[FrameHeaderSize+12:], 99)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			corruptPage := slices.Clone(page)
			test.mutate(corruptPage)
			if err := sealFrame(corruptPage); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenFingerprintPage(corruptPage); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("OpenFingerprintPage = %v", err)
			}
		})
	}
}

func TestRawBlockSparseRoundTripAndExactLookup(t *testing.T) {
	rows := []RawRow{
		{Slot: 0, Key: []byte("alpha"), JSON: []byte(`{"v":1}`)},
		{Slot: 7, Key: []byte("beta"), JSON: []byte(`{"v":2}`)},
		{Slot: 63, Key: []byte("omega"), JSON: []byte(`{"v":3}`)},
	}
	used, err := RawBlockEncodedBytes(rows)
	if err != nil {
		t.Fatal(err)
	}
	span, err := (GeometryPolicy{TargetFillPermille: 1000}).SelectSpan(used)
	if err != nil || span != Quantum {
		t.Fatalf("SelectSpan = (%d,%v), want (%d,nil)", span, err, Quantum)
	}
	page, err := EncodeRawBlock(make([]byte, span), testIdentity, 91, rows)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenRawBlock(page)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		key, json, ok := view.Lookup(row.Slot)
		if !ok || !slices.Equal(key, row.Key) || !slices.Equal(json, row.JSON) {
			t.Fatalf("Lookup(%d) = (%q,%q,%v)", row.Slot, key, json, ok)
		}
		if exact, ok := view.LookupKey(row.Slot, string(row.Key)); !ok ||
			!slices.Equal(exact, row.JSON) {
			t.Fatalf("LookupKey(%d) = (%q,%v)", row.Slot, exact, ok)
		}
	}
	if _, _, ok := view.Lookup(8); ok {
		t.Fatal("vacant slot returned a row")
	}
	if _, ok := view.LookupKey(7, "foreign"); ok {
		t.Fatal("foreign key passed exact verification")
	}
}

func TestRawBlockLexicalLowerBoundAndIterationPreserveStableSlots(t *testing.T) {
	rows := []RawRow{
		{Slot: 1, Key: []byte("delta"), JSON: []byte(`{"v":4}`)},
		{Slot: 7, Key: []byte{0x00, 0xff}, JSON: []byte(`{"v":1}`)},
		{Slot: 18, Key: []byte("beta"), JSON: []byte(`{"v":3}`)},
		{Slot: 63, Key: []byte{0x00}, JSON: []byte(`{"v":0}`)},
	}
	used, err := RawBlockEncodedBytes(rows)
	if err != nil {
		t.Fatal(err)
	}
	span, err := (GeometryPolicy{TargetFillPermille: 1000}).SelectSpan(used)
	if err != nil {
		t.Fatal(err)
	}
	page, err := EncodeRawBlock(make([]byte, span), testIdentity, 92, rows)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenRawBlock(page)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		slot uint8
		key  []byte
	}{
		{63, []byte{0x00}},
		{7, []byte{0x00, 0xff}},
		{18, []byte("beta")},
		{1, []byte("delta")},
	}
	iter := view.Iterator()
	for rank, expected := range want {
		slot, key, _, ok := iter.Next()
		if !ok || slot != expected.slot || !bytes.Equal(key, expected.key) {
			t.Fatalf("rank %d = (%d,%x,%v), want (%d,%x,true)",
				rank, slot, key, ok, expected.slot, expected.key)
		}
	}
	if _, _, _, ok := iter.Next(); ok {
		t.Fatal("iterator advanced past end")
	}
	for _, test := range []struct {
		target []byte
		slot   uint8
		ok     bool
	}{
		{nil, 63, true},
		{[]byte{0x00}, 63, true},
		{[]byte{0x00, 0x01}, 7, true},
		{[]byte("beta"), 18, true},
		{[]byte("charlie"), 1, true},
		{[]byte("delta"), 1, true},
		{[]byte("z"), 0, false},
	} {
		slot, ok := view.LowerBound(test.target)
		if slot != test.slot || ok != test.ok {
			t.Fatalf("LowerBound(%x) = (%d,%v), want (%d,%v)",
				test.target, slot, ok, test.slot, test.ok)
		}
	}
	if RawBlockOrderSize != 48 ||
		float64(RawBlockOrderSize)/RawBlockSlotCount != 0.75 {
		t.Fatalf("lexical order bytes = %d, want 48 and 0.75/current",
			RawBlockOrderSize)
	}
}

func TestRawBlockRejectsDuplicateAndResealedLexicalCorruption(t *testing.T) {
	duplicate := []RawRow{
		{Slot: 1, Key: []byte("same"), JSON: []byte("1")},
		{Slot: 9, Key: []byte("same"), JSON: []byte("2")},
	}
	if _, err := EncodeRawBlock(make([]byte, Quantum), testIdentity, 9, duplicate); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("duplicate key encode = %v", err)
	}

	rows := []RawRow{
		{Slot: 1, Key: []byte("a"), JSON: []byte("1")},
		{Slot: 9, Key: []byte("b"), JSON: []byte("2")},
	}
	page, err := EncodeRawBlock(make([]byte, Quantum), testIdentity, 9, rows)
	if err != nil {
		t.Fatal(err)
	}
	orderAt := FrameHeaderSize + RawBlockPayloadHeaderSize +
		RawBlockSlotDirectorySize
	corrupt := slices.Clone(page)
	// Repeating the first stable slot breaks exact live-set coverage even after
	// an attacker recomputes the outer checksum.
	putRawBlockOrder(corrupt[orderAt:orderAt+RawBlockOrderSize], 1, rows[0].Slot)
	resealTestFrame(t, corrupt)
	if _, err := OpenRawBlock(corrupt); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("resealed duplicate order = %v", err)
	}
}

func TestRawBlockSpaceAndArbitraryQuantumGeometry(t *testing.T) {
	rows := make([]RawRow, RawBlockSlotCount)
	for i := range rows {
		rows[i] = RawRow{
			Slot: uint8(i),
			Key:  []byte(fmt.Sprintf("key-%09d", i)),
			JSON: []byte(fmt.Sprintf(
				`{"id":%d,"payload":"%0256d"}`,
				i,
				i,
			)),
		}
	}
	used, err := RawBlockEncodedBytes(rows)
	if err != nil {
		t.Fatal(err)
	}
	span, err := (GeometryPolicy{TargetFillPermille: 1000}).SelectSpan(used)
	if err != nil {
		t.Fatal(err)
	}
	payloadBytes := 0
	for _, row := range rows {
		payloadBytes += len(row.Key) + len(row.JSON)
	}
	if ratio := float64(span) / float64(payloadBytes); ratio > 1.20 {
		t.Fatalf("raw block physical ratio = %.3fx, want <= 1.20x", ratio)
	}
	if span%Quantum != 0 || span&(span-1) == 0 {
		t.Fatalf("span = %d, want a non-power-of-two 4 KiB multiple", span)
	}

	for _, encoded := range []int{8<<10 + 1, 12<<10 - 1, 12 << 10, 12<<10 + 1} {
		got, err := (GeometryPolicy{TargetFillPermille: 1000}).SelectSpan(encoded)
		if err != nil {
			t.Fatal(err)
		}
		want := ((encoded + Quantum - 1) / Quantum) * Quantum
		if got != want {
			t.Fatalf("SelectSpan(%d) = %d, want %d", encoded, got, want)
		}
	}
}

func TestRawBlockEveryQuantumSpan(t *testing.T) {
	for span := Quantum; span <= MaxSpan; span += Quantum {
		jsonBytes := span - RawBlockFixedBytes - 1
		rows := []RawRow{{
			Slot: 63, Key: []byte("k"), JSON: bytes.Repeat([]byte{'x'}, jsonBytes),
		}}
		page, err := EncodeRawBlock(make([]byte, span), testIdentity, uint32(span), rows)
		if err != nil {
			t.Fatalf("EncodeRawBlock(%d) = %v", span, err)
		}
		view, err := OpenRawBlock(page)
		if err != nil {
			t.Fatalf("OpenRawBlock(%d) = %v", span, err)
		}
		if got, ok := view.LookupKey(63, "k"); !ok || len(got) != jsonBytes {
			t.Fatalf("LookupKey(%d) = %d,%v, want %d,true", span, len(got), ok, jsonBytes)
		}
	}
}

func TestRawBlockCorruptionAndSteadyAllocations(t *testing.T) {
	rows := []RawRow{{Slot: 5, Key: []byte("key"), JSON: []byte(`{"ok":true}`)}}
	page := make([]byte, Quantum)
	if _, err := EncodeRawBlock(page, testIdentity, 7, rows); err != nil {
		t.Fatal(err)
	}
	view, err := OpenRawBlock(page)
	if err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, err := EncodeRawBlock(page, testIdentity, 7, rows); err != nil {
			panic(err)
		}
		opened, err := OpenRawBlock(page)
		if err != nil {
			panic(err)
		}
		if _, ok := opened.LookupKey(5, "key"); !ok {
			panic("missing")
		}
	}); allocations != 0 {
		t.Fatalf("encode/open/lookup allocations = %g, want 0", allocations)
	}
	if _, ok := view.LookupKey(5, "key"); !ok {
		t.Fatal("baseline lookup failed")
	}
	for i := range page {
		corruptPage := slices.Clone(page)
		corruptPage[i] ^= 1
		if _, err := OpenRawBlock(corruptPage); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("byte %d corruption = %v", i, err)
		}
	}
}

func TestPackedBlockSpaceExactnessAndAllocations(t *testing.T) {
	rows := repetitivePackedRows()
	used, err := PackedBlockEncodedBytes(rows)
	if err != nil {
		t.Fatal(err)
	}
	span, err := (GeometryPolicy{TargetFillPermille: 1000}).SelectSpan(used)
	if err != nil {
		t.Fatal(err)
	}
	logical := 0
	for _, row := range rows {
		logical += len(row.Key) + len(row.JSON)
	}
	if ratio := float64(span) / float64(logical); ratio > 0.75 {
		t.Fatalf("packed physical ratio = %.3fx, want <= 0.75x", ratio)
	}
	t.Logf("packed block physical ratio: %d/%d = %.3fx",
		span, logical, float64(span)/float64(logical))
	page, err := EncodePackedBlock(make([]byte, span), testIdentity, 77, rows)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenPackedBlock(page)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 0, len(rows[37].JSON))
	got, ok := view.AppendJSON(dst, 37, string(rows[37].Key))
	if !ok || !slices.Equal(got, rows[37].JSON) {
		t.Fatalf("AppendJSON = (%q,%v)", got, ok)
	}
	if got, ok := view.AppendJSON(dst[:0], 37, "foreign"); ok || len(got) != 0 {
		t.Fatalf("foreign AppendJSON = (%q,%v)", got, ok)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		got, ok := view.AppendJSON(dst[:0], 37, "key-000000037")
		if !ok || len(got) != len(rows[37].JSON) {
			panic("lookup")
		}
	}); allocations != 0 {
		t.Fatalf("packed AppendJSON allocations = %g, want 0", allocations)
	}
}

func TestPackedBlockLexicalLowerBoundAndIteration(t *testing.T) {
	rows := []RawRow{
		{Slot: 2, Key: []byte("omega"), JSON: []byte(`{"v":"shared-omega"}`)},
		{Slot: 11, Key: []byte{0x00, 0xff}, JSON: []byte(`{"v":"shared-one"}`)},
		{Slot: 29, Key: []byte("beta"), JSON: []byte(`{"v":"shared-beta"}`)},
		{Slot: 61, Key: []byte{0x00}, JSON: []byte(`{"v":"shared-zero"}`)},
	}
	page, err := EncodePackedBlock(
		make([]byte, Quantum), testIdentity, 81, rows,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenPackedBlock(page)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		slot uint8
		key  []byte
	}{
		{61, []byte{0x00}},
		{11, []byte{0x00, 0xff}},
		{29, []byte("beta")},
		{2, []byte("omega")},
	}
	iter := view.Iterator()
	for rank, expected := range want {
		slot, key, ok := iter.Next()
		if !ok || slot != expected.slot || !bytes.Equal(key, expected.key) {
			t.Fatalf("rank %d = (%d,%x,%v), want (%d,%x,true)",
				rank, slot, key, ok, expected.slot, expected.key)
		}
	}
	if _, _, ok := iter.Next(); ok {
		t.Fatal("iterator advanced past end")
	}
	for _, test := range []struct {
		target []byte
		slot   uint8
		ok     bool
	}{
		{nil, 61, true},
		{[]byte{0x00, 1}, 11, true},
		{[]byte("beta"), 29, true},
		{[]byte("delta"), 2, true},
		{[]byte("z"), 0, false},
	} {
		slot, ok := view.LowerBound(test.target)
		if slot != test.slot || ok != test.ok {
			t.Fatalf("LowerBound(%x) = (%d,%v), want (%d,%v)",
				test.target, slot, ok, test.slot, test.ok)
		}
	}
	if _, err := EncodePackedBlock(
		make([]byte, Quantum), testIdentity, 81,
		[]RawRow{
			{Slot: 1, Key: []byte("same"), JSON: []byte("1")},
			{Slot: 2, Key: []byte("same"), JSON: []byte("2")},
		},
	); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("duplicate key encode = %v", err)
	}
}

func TestCanonicalBlockPlanRejectsWeakPacking(t *testing.T) {
	repetitive := repetitivePackedRows()
	plan, err := PlanCanonicalBlock(
		repetitive,
		GeometryPolicy{TargetFillPermille: 1000},
		BlockPlanPolicy{PackedReadGatePassed: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Encoding != BlockEncodingPacked || plan.PackedSpan != Quantum ||
		plan.RawSpan <= plan.PackedSpan {
		t.Fatalf("repetitive plan = %+v", plan)
	}
	disabled, err := PlanCanonicalBlock(
		repetitive,
		GeometryPolicy{TargetFillPermille: 1000},
		BlockPlanPolicy{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if disabled.Encoding != BlockEncodingRaw {
		t.Fatalf("unqualified packed reader plan = %+v", disabled)
	}

	incompressible := make([]RawRow, RawBlockSlotCount)
	for i := range incompressible {
		payload := strings.Repeat(string(rune('a'+i%26)), 257)
		json := []byte(fmt.Sprintf(
			`{"head":%d,"payload":%q,"tail":%d}`,
			i, payload, RawBlockSlotCount-i,
		))
		incompressible[i] = RawRow{
			Slot: uint8(i),
			Key:  []byte(fmt.Sprintf("key-%09d", i)),
			JSON: json,
		}
	}
	plan, err = PlanCanonicalBlock(
		incompressible,
		GeometryPolicy{TargetFillPermille: 1000},
		BlockPlanPolicy{PackedReadGatePassed: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Encoding != BlockEncodingRaw || plan.Span != plan.RawSpan ||
		plan.PackedSpan != plan.RawSpan {
		t.Fatalf("incompressible plan = %+v", plan)
	}
}

func TestPackedFourByteDirectoryAvoidsExtentGrowth(t *testing.T) {
	rows := make([]RawRow, RawBlockSlotCount)
	json := []byte(`{"payload":"` + strings.Repeat("x", 3286) + `"}`)
	for i := range rows {
		rows[i] = RawRow{
			Slot: uint8(i),
			Key:  []byte(fmt.Sprintf("k%04d", i)),
			JSON: json,
		}
	}
	used, err := PackedBlockEncodedBytes(rows)
	if err != nil {
		t.Fatal(err)
	}
	span, err := (GeometryPolicy{TargetFillPermille: 1000}).SelectSpan(used)
	if err != nil {
		t.Fatal(err)
	}
	if PackedBlockSlotRecordSize != 4 || span != Quantum || used+4*RawBlockSlotCount <= Quantum {
		t.Fatalf("packed directory: record=%d used=%d span=%d",
			PackedBlockSlotRecordSize, used, span)
	}
	page, err := EncodePackedBlock(make([]byte, span), testIdentity, 78, rows)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenPackedBlock(page)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := view.AppendJSON(nil, 63, "k0063"); !ok || !slices.Equal(got, json) {
		t.Fatalf("last row = (%d,%v)", len(got), ok)
	}
}

func TestPackedBlockValidJSONCorpusMatrix(t *testing.T) {
	tests := []struct {
		name string
		rows []RawRow
	}{
		{name: "vary-first", rows: packedCorpusRows(func(i int) string {
			return fmt.Sprintf(`{"id":%d,"payload":%q}`, i, strings.Repeat("x", 240))
		})},
		{name: "vary-middle", rows: packedCorpusRows(func(i int) string {
			return fmt.Sprintf(`{"prefix":%q,"id":%d,"suffix":%q}`,
				strings.Repeat("p", 120), i, strings.Repeat("s", 120))
		})},
		{name: "vary-last", rows: packedCorpusRows(func(i int) string {
			return fmt.Sprintf(`{"payload":%q,"id":%d}`, strings.Repeat("x", 240), i)
		})},
		{name: "escaped", rows: packedCorpusRows(func(i int) string {
			return fmt.Sprintf(`{"payload":%q,"id":%d}`,
				"quote:\" slash:\\ newline:\\n "+strings.Repeat("x", 210), i)
		})},
		{name: "heterogeneous", rows: packedCorpusRows(func(i int) string {
			if i%2 == 0 {
				return fmt.Sprintf(`{"alpha":%d,"payload":%q}`, i, strings.Repeat("a", 233))
			}
			return fmt.Sprintf(`[%d,%q,{"omega":true}]`, i, strings.Repeat("z", 233))
		})},
		{name: "high-cardinality", rows: packedCorpusRows(func(i int) string {
			return fmt.Sprintf(`{"head":%d,"payload":%q,"tail":%d}`,
				i, strings.Repeat(string(rune('a'+i%26)), 257), 64-i)
		})},
	}
	selected := 0
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := PlanCanonicalBlock(
				test.rows,
				GeometryPolicy{TargetFillPermille: 1000},
				BlockPlanPolicy{PackedReadGatePassed: true},
			)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Encoding == BlockEncodingPacked {
				selected++
				if plan.PackedSpan >= plan.RawSpan ||
					(plan.RawSpan-plan.PackedSpan)*1000 < plan.RawSpan*125 {
					t.Fatalf("weak packed plan = %+v", plan)
				}
			}
			t.Logf("encoding=%d raw=%d packed=%d", plan.Encoding, plan.RawSpan, plan.PackedSpan)
		})
	}
	if selected < 2 || selected == len(tests) {
		t.Fatalf("selected %d/%d corpora, want both raw and packed outcomes",
			selected, len(tests))
	}
}

func TestPackedBlockCorruption(t *testing.T) {
	rows := repetitivePackedRows()
	used, _ := PackedBlockEncodedBytes(rows)
	span, _ := (GeometryPolicy{TargetFillPermille: 1000}).SelectSpan(used)
	page, err := EncodePackedBlock(make([]byte, span), testIdentity, 77, rows)
	if err != nil {
		t.Fatal(err)
	}
	for i := range page {
		corruptPage := slices.Clone(page)
		corruptPage[i] ^= 1
		if _, err := OpenPackedBlock(corruptPage); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("byte %d corruption = %v", i, err)
		}
	}
	corruptPage := slices.Clone(page)
	first := FrameHeaderSize + PackedBlockPayloadHeaderSize
	record := binary.LittleEndian.Uint32(corruptPage[first:])
	record &^= uint32(0xffff) << 16
	record |= uint32(0xffff) << 16
	binary.LittleEndian.PutUint32(corruptPage[first:], record)
	if err := sealFrame(corruptPage); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPackedBlock(corruptPage); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("resealed packed corruption = %v", err)
	}

	corruptPage = slices.Clone(page)
	header, ok := decodeFrameHeader(corruptPage)
	if !ok {
		t.Fatal("decode frame")
	}
	padding := FrameHeaderSize + int(header.payloadLength)
	corruptPage[padding] = 1
	trailer := len(corruptPage) - FrameTrailerSize
	checksum := crc32.Checksum(corruptPage[:trailer], frameCRC)
	binary.LittleEndian.PutUint32(corruptPage[trailer:], checksum)
	binary.LittleEndian.PutUint32(corruptPage[trailer+4:], ^checksum)
	if _, err := OpenPackedBlock(corruptPage); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("resealed non-canonical padding = %v", err)
	}
}

func TestPackedBlockRejectsResealedMalformedStructures(t *testing.T) {
	rows := []RawRow{
		{Slot: 0, Key: []byte("a"), JSON: []byte(`{"v":"alpha"}`)},
		{Slot: 2, Key: []byte("b"), JSON: []byte(`{"v":"beta"}`)},
	}
	used, err := PackedBlockEncodedBytes(rows)
	if err != nil {
		t.Fatal(err)
	}
	page, err := EncodePackedBlock(make([]byte, Quantum), testIdentity, 79, rows)
	if err != nil {
		t.Fatal(err)
	}
	payload := FrameHeaderSize
	directory := payload + PackedBlockPayloadHeaderSize
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "zero block", mutate: func(p []byte) {
			binary.LittleEndian.PutUint32(p[payload+4:], 0)
		}},
		{name: "live count", mutate: func(p []byte) {
			p[payload+22]++
		}},
		{name: "zero live", mutate: func(p []byte) {
			binary.LittleEndian.PutUint64(p[payload+8:], 0)
		}},
		{name: "reserved header", mutate: func(p []byte) {
			p[payload+23] = 1
		}},
		{name: "edge overflow", mutate: func(p []byte) {
			binary.LittleEndian.PutUint16(p[payload+16:], uint16(used))
			binary.LittleEndian.PutUint16(p[payload+18:], uint16(used))
		}},
		{name: "absent slot record", mutate: func(p []byte) {
			binary.LittleEndian.PutUint32(p[directory+PackedBlockSlotRecordSize:], 0)
		}},
		{name: "first gap", mutate: func(p []byte) {
			record := binary.LittleEndian.Uint32(p[directory:])
			binary.LittleEndian.PutUint32(p[directory:], record+1)
		}},
		{name: "overlap", mutate: func(p []byte) {
			first := binary.LittleEndian.Uint32(p[directory:])
			at := directory + 2*PackedBlockSlotRecordSize
			second := binary.LittleEndian.Uint32(p[at:])
			second = second&0xffff0000 | uint32(uint16(first>>16)-1)
			binary.LittleEndian.PutUint32(p[at:], second)
		}},
		{name: "wrong kind", mutate: func(p []byte) {
			p[12] = byte(frameRawBlock)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			corruptPage := slices.Clone(page)
			test.mutate(corruptPage)
			resealTestFrame(t, corruptPage)
			if _, err := OpenPackedBlock(corruptPage); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("OpenPackedBlock = %v", err)
			}
		})
	}
}

func FuzzPackedBlockRoundTrip(f *testing.F) {
	f.Add("alpha")
	f.Add("quote:\" slash:\\ newline:\n")
	f.Fuzz(func(t *testing.T, seed string) {
		if len(seed) > 512 {
			t.Skip()
		}
		rows := make([]RawRow, 8)
		for i := range rows {
			value, err := json.Marshal(seed)
			if err != nil {
				t.Fatal(err)
			}
			rows[i] = RawRow{
				Slot: uint8(i * 7),
				Key:  []byte(fmt.Sprintf("key-%d", i)),
				JSON: []byte(fmt.Sprintf(`{"slot":%d,"value":%s}`, i, value)),
			}
		}
		used, err := PackedBlockEncodedBytes(rows)
		if err != nil {
			t.Fatal(err)
		}
		span, err := (GeometryPolicy{TargetFillPermille: 1000}).SelectSpan(used)
		if err != nil {
			t.Fatal(err)
		}
		page, err := EncodePackedBlock(make([]byte, span), testIdentity, 80, rows)
		if err != nil {
			t.Fatal(err)
		}
		view, err := OpenPackedBlock(page)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			got, ok := view.AppendJSON(nil, row.Slot, string(row.Key))
			if !ok || !slices.Equal(got, row.JSON) {
				t.Fatalf("slot %d = (%q,%v)", row.Slot, got, ok)
			}
		}
	})
}

func TestGeometryTraceExactSpansAndDeleteMaintenance(t *testing.T) {
	operations := make([]TraceOperation, 0, 20_000)
	for key := range uint64(10_000) {
		operations = append(operations, TraceOperation{
			Kind: TracePut, Key: key, RecordBytes: 299 + int(key%5)*173,
		})
	}
	for key := range uint64(9_000) {
		operations = append(operations, TraceOperation{Kind: TraceDelete, Key: key})
	}
	metrics, err := SimulateTrace(operations, TraceConfig{
		HotMaxSpan:                  12 << 10,
		ColdMaxSpan:                 24 << 10,
		PlacementCandidateLimit:     4,
		MaintenanceSteps:            256,
		MaintenanceRelocationBudget: 512,
	})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Documents != 1_000 ||
		metrics.ExactSpanBytes > metrics.FreshRebuildDataExtentBytes*110/100 {
		t.Fatalf("delete maintenance metrics = %+v", metrics)
	}
	if metrics.ExactSpanBytes >= metrics.PowerOfTwoSpanBytes ||
		metrics.ExactDataExtentWriteBytes >= metrics.PowerOfTwoDataExtentWriteBytes {
		t.Fatalf("exact spans did not save space and writes: %+v", metrics)
	}
	if metrics.MaintenancePairProbes > 256 ||
		metrics.MaintenanceRelocations > 512 {
		t.Fatalf("maintenance exceeded configured bounds: %+v", metrics)
	}
	t.Logf("90%% delete trace: exact=%d power2=%d fresh=%d writes=%d/%d relocations=%d",
		metrics.ExactSpanBytes, metrics.PowerOfTwoSpanBytes,
		metrics.FreshRebuildDataExtentBytes,
		metrics.ExactDataExtentWriteBytes, metrics.PowerOfTwoDataExtentWriteBytes,
		metrics.MaintenanceRelocations)
}

func TestGeometryTraceReplacementUsesSubPowerOfTwoSpans(t *testing.T) {
	operations := make([]TraceOperation, 0, 4_000)
	for key := range uint64(2_000) {
		operations = append(operations, TraceOperation{
			Kind: TracePut, Key: key, RecordBytes: 700 + int(key%7)*113,
		})
	}
	for key := range uint64(2_000) {
		operations = append(operations, TraceOperation{
			Kind: TracePut, Key: key, RecordBytes: 900 + int(key%11)*97,
		})
	}
	metrics, err := SimulateTrace(operations, TraceConfig{
		HotMaxSpan: 12 << 10, ColdMaxSpan: 24 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.ExactDataExtentWriteBytes*100 >
		metrics.PowerOfTwoDataExtentWriteBytes*88 {
		t.Fatalf("exact-span write bytes = %d, power-of-two = %d, want >=12%% saving",
			metrics.ExactDataExtentWriteBytes, metrics.PowerOfTwoDataExtentWriteBytes)
	}
	t.Logf("replacement trace write bytes: exact=%d power2=%d",
		metrics.ExactDataExtentWriteBytes, metrics.PowerOfTwoDataExtentWriteBytes)
}

func TestGeometryTraceBoundsPlacementAndMaintenance(t *testing.T) {
	operations := make([]TraceOperation, 12)
	for i := range operations {
		operations[i] = TraceOperation{
			Kind: TracePut, Key: uint64(i), RecordBytes: 3_000,
		}
	}
	metrics, err := SimulateTrace(operations, TraceConfig{
		HotMaxSpan:                  4 << 10,
		ColdMaxSpan:                 8 << 10,
		PlacementCandidateLimit:     2,
		MaintenanceSteps:            3,
		MaintenanceRelocationBudget: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.PlacementProbes > uint64(len(operations)*2) {
		t.Fatalf("placement probes = %d, want <= %d",
			metrics.PlacementProbes, len(operations)*2)
	}
	if metrics.MaintenancePairProbes > 3 ||
		metrics.MaintenanceRelocations > 2 ||
		metrics.MaintenanceMerges != 2 ||
		metrics.Blocks != 10 {
		t.Fatalf("bounded maintenance metrics = %+v", metrics)
	}
}

func TestGeometryTraceRejectsRecordAboveHotMaximum(t *testing.T) {
	const hot = 4 << 10
	maxRecord := hot - RawBlockFixedBytes
	if _, err := SimulateTrace(
		[]TraceOperation{{Kind: TracePut, Key: 1, RecordBytes: maxRecord}},
		TraceConfig{HotMaxSpan: hot, ColdMaxSpan: 8 << 10},
	); err != nil {
		t.Fatalf("maximum hot record = %v", err)
	}
	for _, operations := range [][]TraceOperation{
		{{Kind: TracePut, Key: 1, RecordBytes: maxRecord + 1}},
		{
			{Kind: TracePut, Key: 1, RecordBytes: 1},
			{Kind: TracePut, Key: 1, RecordBytes: maxRecord + 1},
		},
	} {
		if _, err := SimulateTrace(
			operations,
			TraceConfig{HotMaxSpan: hot, ColdMaxSpan: 8 << 10},
		); !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("oversized hot record = %v, want %v", err, ErrInvalidFrame)
		}
	}
}

func TestGeometryTraceReplacementSplitRespectsHotMaximum(t *testing.T) {
	metrics, err := SimulateTrace([]TraceOperation{
		{Kind: TracePut, Key: 1, RecordBytes: 1_500},
		{Kind: TracePut, Key: 2, RecordBytes: 500},
		{Kind: TracePut, Key: 3, RecordBytes: 1_500},
		{Kind: TracePut, Key: 2, RecordBytes: 4<<10 - RawBlockFixedBytes},
	}, TraceConfig{HotMaxSpan: 4 << 10, ColdMaxSpan: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Blocks != 2 || metrics.SplitRelocations != 1 {
		t.Fatalf("replacement split metrics = %+v", metrics)
	}
}

func BenchmarkFingerprintLookupResident(b *testing.B) {
	entries := make([]FingerprintEntry, FingerprintPageCapacity)
	for i := range entries {
		entries[i] = FingerprintEntry{
			Hash:     uint64(i) << 32,
			Location: Location{BlockID: uint32(i + 1), Slot: uint8(i % 64)},
		}
	}
	page, _ := EncodeFingerprintPage(make([]byte, FingerprintPageSize), testIdentity, entries)
	view, _ := OpenFingerprintPage(page)
	hash := entries[len(entries)/2].Hash
	want := entries[len(entries)/2].Location
	b.ReportAllocs()
	b.SetBytes(FingerprintEntrySize)
	for b.Loop() {
		if got, ok := view.Lookup(hash, func(candidate Location) bool {
			return candidate == want
		}); !ok || got != want {
			b.Fatal("lookup")
		}
	}
}

func BenchmarkRawBlockLookupExact(b *testing.B) {
	rows := make([]RawRow, 64)
	for i := range rows {
		rows[i] = RawRow{
			Slot: uint8(i),
			Key:  []byte(fmt.Sprintf("key-%09d", i)),
			JSON: []byte(`{"payload":"abcdefghijklmnopqrstuvwxyz"}`),
		}
	}
	page, _ := EncodeRawBlock(make([]byte, Quantum), testIdentity, 7, rows)
	view, _ := OpenRawBlock(page)
	b.ReportAllocs()
	b.SetBytes(int64(len(rows[37].JSON)))
	for b.Loop() {
		if _, ok := view.LookupKey(37, "key-000000037"); !ok {
			b.Fatal("lookup")
		}
	}
}

func BenchmarkRawBlockLexical(b *testing.B) {
	rows := make([]RawRow, RawBlockSlotCount)
	for i := range rows {
		rows[i] = RawRow{
			Slot: uint8(i),
			Key:  []byte(fmt.Sprintf("tenant/blue/device/%08d", (i*37+11)&63)),
			JSON: []byte(`{"payload":"abcdefghijklmnopqrstuvwxyz"}`),
		}
	}
	page, _ := EncodeRawBlock(make([]byte, 8<<10), testIdentity, 8, rows)
	view, _ := OpenRawBlock(page)
	target := []byte("tenant/blue/device/00000037")
	var targetSlot uint8
	for _, row := range rows {
		if bytes.Equal(row.Key, target) {
			targetSlot = row.Slot
			break
		}
	}
	b.Run("lower-bound", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if slot, ok := view.LowerBound(target); !ok || slot != targetSlot {
				b.Fatal("lower bound")
			}
		}
	})
	b.Run("scan", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(RawBlockSlotCount)
		for b.Loop() {
			iter := view.Iterator()
			count := 0
			for {
				_, _, _, ok := iter.Next()
				if !ok {
					break
				}
				count++
			}
			if count != RawBlockSlotCount {
				b.Fatal("scan")
			}
		}
	})
}

func BenchmarkPackedBlockAppendExact(b *testing.B) {
	rows := repetitivePackedRows()
	used, _ := PackedBlockEncodedBytes(rows)
	span, _ := (GeometryPolicy{TargetFillPermille: 1000}).SelectSpan(used)
	page, _ := EncodePackedBlock(make([]byte, span), testIdentity, 77, rows)
	view, _ := OpenPackedBlock(page)
	dst := make([]byte, 0, len(rows[37].JSON))
	b.ReportAllocs()
	b.SetBytes(int64(len(rows[37].JSON)))
	for b.Loop() {
		got, ok := view.AppendJSON(dst[:0], 37, "key-000000037")
		if !ok || len(got) != len(rows[37].JSON) {
			b.Fatal("lookup")
		}
	}
}

func repetitivePackedRows() []RawRow {
	rows := make([]RawRow, RawBlockSlotCount)
	padding := string(bytes.Repeat([]byte{'x'}, 256))
	for i := range rows {
		rows[i] = RawRow{
			Slot: uint8(i),
			Key:  []byte(fmt.Sprintf("key-%09d", i)),
			JSON: []byte(fmt.Sprintf(
				`{"id":"%09d","payload":"%s","active":true}`,
				i,
				padding,
			)),
		}
	}
	return rows
}

func packedCorpusRows(jsonFor func(int) string) []RawRow {
	rows := make([]RawRow, RawBlockSlotCount)
	for i := range rows {
		rows[i] = RawRow{
			Slot: uint8(i),
			Key:  []byte(fmt.Sprintf("key-%09d", i)),
			JSON: []byte(jsonFor(i)),
		}
	}
	return rows
}

func resealTestFrame(t *testing.T, page []byte) {
	t.Helper()
	header, ok := decodeFrameHeader(page)
	if !ok {
		t.Fatal("decode frame")
	}
	trailer := int(header.span) - FrameTrailerSize
	checksum := crc32.Checksum(page[:trailer], frameCRC)
	binary.LittleEndian.PutUint32(page[trailer:], checksum)
	binary.LittleEndian.PutUint32(page[trailer+4:], ^checksum)
}
