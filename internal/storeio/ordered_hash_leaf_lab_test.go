package storeio

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"testing"
)

var orderedHashLeafLabTestSeed = [16]byte{
	0x91, 0x2a, 0x73, 0x44, 0x55, 0xc6, 0x17, 0x88,
	0x39, 0xaa, 0x6b, 0xcc, 0x1d, 0xee, 0x4f, 0x70,
}

func orderedHashLeafLabTestHeader(generation uint64) OrderedHashLeafLabHeader {
	return OrderedHashLeafLabHeader{
		BucketID: 0x1020304050607080, Generation: generation,
	}
}

func orderedHashLeafLabRecords(count, valueLength int) []OrderedHashLeafLabRecord {
	records := make([]OrderedHashLeafLabRecord, count)
	for index := range records {
		records[index] = OrderedHashLeafLabRecord{
			Key:   []byte(fmt.Sprintf("key-%04d", index)),
			Value: bytes.Repeat([]byte{byte(index), byte(index >> 8)}, (valueLength+1)/2)[:valueLength],
		}
	}
	if err := PlaceOrderedHashLeafLabRecords(orderedHashLeafLabTestSeed, records); err != nil {
		panic(err)
	}
	return records
}

func encodeOrderedHashLeafLabTest(
	t testing.TB, records []OrderedHashLeafLabRecord,
) []byte {
	t.Helper()
	page, err := EncodeOrderedHashLeafLab(
		make([]byte, OrderedHashLeafLabPageSize),
		orderedHashLeafLabTestHeader(7),
		orderedHashLeafLabTestSeed,
		records,
	)
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func openOrderedHashLeafLabTest(
	t testing.TB, records []OrderedHashLeafLabRecord,
) OrderedHashLeafLabView {
	t.Helper()
	view, err := OpenOrderedHashLeafLab(
		encodeOrderedHashLeafLabTest(t, records),
		orderedHashLeafLabTestSeed,
	)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func TestOrderedHashLeafLabMetadataAtSteadyOccupancy(t *testing.T) {
	for _, live := range []int{218, 225, 230} {
		got := OrderedHashLeafLabMetadataBytesPerLiveKey(live)
		if got > 5 {
			t.Fatalf("%d live metadata = %.4f B/key, want <= 5", live, got)
		}
		if whole := float64(OrderedHashLeafLabPageSize) / float64(live); whole < got {
			t.Fatalf("%d live whole-page %.4f < metadata %.4f", live, whole, got)
		}
	}
}

func TestOrderedHashLeafLabPersistsMinimalFourKiBExtent(t *testing.T) {
	for _, test := range []struct {
		name        string
		count       int
		valueLength int
		wantExtent  int
	}{
		{name: "small", count: 1, valueLength: 1, wantExtent: 4 << 10},
		{name: "steady", count: 230, valueLength: 8, wantExtent: 8 << 10},
		{name: "larger-values", count: 230, valueLength: 32, wantExtent: 12 << 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			page := encodeOrderedHashLeafLabTest(
				t, orderedHashLeafLabRecords(test.count, test.valueLength),
			)
			view, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) != test.wantExtent || view.ExtentBytes() != test.wantExtent ||
				len(view.PersistentBytes()) != test.wantExtent {
				t.Fatalf(
					"extent = (%d,%d,%d), want %d",
					len(page), view.ExtentBytes(), len(view.PersistentBytes()),
					test.wantExtent,
				)
			}
			if want := orderedHashLeafLabExtentForHeapEnd(int(view.heapEnd)); len(page) != want {
				t.Fatalf("extent = %d, minimal extent = %d", len(page), want)
			}
		})
	}

	page := encodeOrderedHashLeafLabTest(t, orderedHashLeafLabRecords(1, 1))
	nonCanonical := make([]byte, OrderedHashLeafLabPageSize)
	copy(nonCanonical, page)
	if _, err := OpenOrderedHashLeafLab(
		nonCanonical, orderedHashLeafLabTestSeed,
	); !errors.Is(err, ErrOrderedHashLeafLabCorrupt) {
		t.Fatalf("non-minimal 64 KiB admission = %v", err)
	}
}

func TestOrderedHashLeafLabOwnedMutationRequiresCOWGrowthAndRetainsExtent(t *testing.T) {
	records := orderedHashLeafLabRecords(1, 1)
	encoded := encodeOrderedHashLeafLabTest(t, records)
	before := append([]byte(nil), encoded...)
	view, err := OpenOrderedHashLeafLab(encoded, orderedHashLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte{0xa5}, 4<<10)
	if err := view.MaterializeOwnedUpdate(
		records[0].Slot, records[0].Key, large, false,
	); !errors.Is(err, ErrOrderedHashLeafLabResize) {
		t.Fatalf("owned extent growth = %v", err)
	}
	if !bytes.Equal(view.PersistentBytes(), before) {
		t.Fatal("resize signal modified owned image")
	}
	grown, err := view.UpdateTo(
		make([]byte, OrderedHashLeafLabPageSize), 8,
		records[0].Slot, records[0].Key, large, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err = OpenOrderedHashLeafLab(grown, orderedHashLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	grownHeader := view.Header()
	if err := view.MaterializeOwnedUpdate(
		records[0].Slot, records[0].Key, []byte{0x5a}, false,
	); err != nil {
		t.Fatal(err)
	}
	if view.ExtentBytes() != 8<<10 || view.MinimalExtentBytes() != 4<<10 {
		t.Fatalf(
			"owned shrink extents = physical %d minimal %d, want 8192/4096",
			view.ExtentBytes(), view.MinimalExtentBytes(),
		)
	}
	if view.Header() != grownHeader {
		t.Fatalf("owned shrink changed page identity: %+v -> %+v", grownHeader, view.Header())
	}
	wantRecords := orderedHashLeafLabRecords(1, 1)
	wantRecords[0].Value = []byte{0x5a}
	want, err := EncodeOrderedHashLeafLab(
		make([]byte, OrderedHashLeafLabPageSize),
		grownHeader,
		orderedHashLeafLabTestSeed,
		wantRecords,
	)
	if err != nil {
		t.Fatal(err)
	}
	value, _, ok := view.LookupSlot(records[0].Slot, records[0].Key)
	if !ok || !bytes.Equal(value, wantRecords[0].Value) {
		t.Fatal("owned shrink lost the replacement")
	}
	if len(want) != 4<<10 {
		t.Fatalf("COW shrink extent = %d, want 4096", len(want))
	}
}

func TestOrderedHashLeafLabOwnedUpdatePreservesPageRefIdentityAndUntouchedBytes(t *testing.T) {
	records := orderedHashLeafLabRecords(230, 32)
	page := encodeOrderedHashLeafLabTest(t, records)
	before := append([]byte(nil), page...)
	view, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	beforeHeader := view.Header()
	target := records[115]
	rank := int(view.page[orderedHashLeafLabRankStart+int(target.Slot)])
	start, end, ok := view.recordBounds(rank)
	if !ok {
		t.Fatal("target bounds")
	}
	valueStart := start + len(target.Key)
	replacement := bytes.Repeat([]byte{0x3c}, len(target.Value))
	if err := view.MaterializeOwnedUpdate(
		target.Slot, target.Key, replacement, false,
	); err != nil {
		t.Fatal(err)
	}
	if view.Header() != beforeHeader || view.ExtentBytes() != len(before) {
		t.Fatalf(
			"owned update changed PageRef identity/extent: (%+v,%d) -> (%+v,%d)",
			beforeHeader, len(before), view.Header(), view.ExtentBytes(),
		)
	}
	trailerStart := orderedHashLeafLabTrailerStart(page)
	for index := range page {
		inValue := index >= valueStart && index < end
		inChecksumTrailer := index >= trailerStart
		if !inValue && !inChecksumTrailer && page[index] != before[index] {
			t.Fatalf("owned same-length update changed byte %d outside payload/trailer", index)
		}
	}
}

func TestOrderedHashLeafLabExactLookupAndLexicalIteration(t *testing.T) {
	records := orderedHashLeafLabRecords(230, 32)
	view := openOrderedHashLeafLabTest(t, records)
	if view.Header() != orderedHashLeafLabTestHeader(7) ||
		view.Len() != len(records) {
		t.Fatalf("header/len = (%+v,%d)", view.Header(), view.Len())
	}
	for _, record := range records {
		slot, value, overflow, ok := view.Lookup(record.Key)
		if !ok || slot != record.Slot || overflow ||
			!bytes.Equal(value, record.Value) {
			t.Fatalf("Lookup(%q) = (%d,%d,%v,%v)", record.Key, slot, len(value), overflow, ok)
		}
		hinted, _, ok := view.LookupSlot(record.Slot, record.Key)
		if !ok || !bytes.Equal(hinted, record.Value) {
			t.Fatalf("LookupSlot(%d,%q) failed", record.Slot, record.Key)
		}
	}
	if _, _, _, ok := view.Lookup([]byte("absent")); ok {
		t.Fatal("absent key returned a hit")
	}
	iterator := view.AllRows()
	for index, record := range records {
		key, value, overflow, ok := iterator.NextBorrowed()
		if !ok || overflow || !bytes.Equal(key, record.Key) ||
			!bytes.Equal(value, record.Value) {
			t.Fatalf("row %d = (%q,%d,%v,%v)", index, key, len(value), overflow, ok)
		}
	}
	if _, _, _, ok := iterator.NextBorrowed(); ok {
		t.Fatal("iterator returned an extra row")
	}
}

func TestOrderedHashLeafLabInputMustBeCanonicalLexical(t *testing.T) {
	records := orderedHashLeafLabRecords(3, 8)
	records[0], records[1] = records[1], records[0]
	if _, err := EncodeOrderedHashLeafLab(
		make([]byte, OrderedHashLeafLabPageSize),
		orderedHashLeafLabTestHeader(1),
		orderedHashLeafLabTestSeed,
		records,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("unordered encode = %v", err)
	}
}

func TestOrderedHashLeafLabRangePrefixAndLowerBound(t *testing.T) {
	records := orderedHashLeafLabRecords(230, 8)
	view := openOrderedHashLeafLabTest(t, records)
	if got := view.LowerBound([]byte("key-0115")); got != 115 {
		t.Fatalf("LowerBound exact = %d", got)
	}
	if got := view.LowerBound([]byte("key-0115x")); got != 116 {
		t.Fatalf("LowerBound gap = %d", got)
	}
	rangeIt := view.Range([]byte("key-0100"), []byte("key-0105"))
	for want := 100; want < 105; want++ {
		key, _, _, ok := rangeIt.NextBorrowed()
		if !ok || string(key) != fmt.Sprintf("key-%04d", want) {
			t.Fatalf("range row %d = %q,%v", want, key, ok)
		}
	}
	if _, _, _, ok := rangeIt.NextBorrowed(); ok {
		t.Fatal("range crossed exclusive upper bound")
	}
	prefixIt := view.Prefix([]byte("key-01"))
	count := 0
	for {
		key, _, _, ok := prefixIt.NextBorrowed()
		if !ok {
			break
		}
		if !bytes.HasPrefix(key, []byte("key-01")) {
			t.Fatalf("prefix returned %q", key)
		}
		count++
	}
	if count != 100 {
		t.Fatalf("prefix count = %d, want 100", count)
	}
}

func TestOrderedHashLeafLabFalseTagsRequireExactSameLeafKey(t *testing.T) {
	records := orderedHashLeafLabRecords(230, 8)
	view := openOrderedHashLeafLabTest(t, records)
	target := []byte("not-present")
	hash := KeyHashBytes(orderedHashLeafLabTestSeed, target)
	want := orderedHashLeafLabControlLive | byte(hash>>57)
	matches := 0
	for ordinal := 0; ordinal < orderedHashLeafLabGroupSize*2; ordinal++ {
		slot := orderedHashLeafLabCandidate(hash, ordinal)
		if view.page[orderedHashLeafLabControlStart+int(slot)] == want {
			matches++
		}
	}
	if matches == 0 {
		for candidate := 0; candidate < 100000; candidate++ {
			target = []byte(fmt.Sprintf("missing-%06d", candidate))
			hash = KeyHashBytes(orderedHashLeafLabTestSeed, target)
			want = orderedHashLeafLabControlLive | byte(hash>>57)
			for ordinal := 0; ordinal < orderedHashLeafLabGroupSize*2; ordinal++ {
				slot := orderedHashLeafLabCandidate(hash, ordinal)
				if view.page[orderedHashLeafLabControlStart+int(slot)] == want {
					matches++
				}
			}
			if matches != 0 {
				break
			}
		}
	}
	if matches == 0 {
		t.Fatal("failed to construct a false tag candidate")
	}
	if _, _, _, ok := view.LookupHashed(hash, target); ok {
		t.Fatal("false tag match returned a key")
	}
}

func TestOrderedHashLeafLabEightByteHashMatchesStorePrimitive(t *testing.T) {
	for index := 0; index < 10_000; index++ {
		key := []byte(fmt.Sprintf("h%07d", index))
		got := orderedHashLeafLabKeyHash(orderedHashLeafLabTestSeed, key)
		want := KeyHashBytes(orderedHashLeafLabTestSeed, key)
		if got != want {
			t.Fatalf("hash(%q) = %x, want %x", key, got, want)
		}
	}
}

func TestOrderedHashLeafLabDeleteIsCanonicalAndTombstoneFree(t *testing.T) {
	records := orderedHashLeafLabRecords(230, 16)
	view := openOrderedHashLeafLabTest(t, records)
	target := records[117]
	page, err := view.DeleteTo(
		make([]byte, OrderedHashLeafLabPageSize), 8, target.Slot, target.Key,
	)
	if err != nil {
		t.Fatal(err)
	}
	after, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	if after.Len() != 229 ||
		after.page[orderedHashLeafLabControlStart+int(target.Slot)] != 0 ||
		after.page[orderedHashLeafLabRankStart+int(target.Slot)] != orderedHashLeafLabEmptyRank {
		t.Fatal("delete retained a control, rank, or row")
	}
	if _, _, _, ok := after.Lookup(target.Key); ok {
		t.Fatal("deleted key remains visible")
	}
	for _, record := range records {
		if record.Slot == target.Slot {
			continue
		}
		slot, value, _, ok := after.Lookup(record.Key)
		if !ok || slot != record.Slot || !bytes.Equal(value, record.Value) {
			t.Fatalf("survivor %q moved or changed", record.Key)
		}
	}
	wantRecords := append([]OrderedHashLeafLabRecord(nil), records[:117]...)
	wantRecords = append(wantRecords, records[118:]...)
	want, err := EncodeOrderedHashLeafLab(
		make([]byte, OrderedHashLeafLabPageSize),
		orderedHashLeafLabTestHeader(8),
		orderedHashLeafLabTestSeed,
		wantRecords,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(page, want) {
		t.Fatal("delete after-image is not canonical")
	}
}

func TestOrderedHashLeafLabInsertPreservesSlotsAndSignalsSplit(t *testing.T) {
	records := orderedHashLeafLabRecords(230, 8)
	view := openOrderedHashLeafLabTest(t, records)
	page, slot, err := view.InsertTo(
		make([]byte, OrderedHashLeafLabPageSize), 8,
		[]byte("key-0115a"), []byte("inserted"), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	after, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	value, _, ok := after.LookupSlot(slot, []byte("key-0115a"))
	if !ok || string(value) != "inserted" {
		t.Fatalf("inserted slot = (%d,%q,%v)", slot, value, ok)
	}
	for _, record := range records {
		got, _, _, ok := after.Lookup(record.Key)
		if !ok || got != record.Slot {
			t.Fatalf("insert relocated %q from %d to %d", record.Key, record.Slot, got)
		}
	}

	full := orderedHashLeafLabRecords(256, 1)
	fullView := openOrderedHashLeafLabTest(t, full)
	if _, _, err := fullView.InsertTo(
		make([]byte, OrderedHashLeafLabPageSize), 8,
		[]byte("zzzz"), []byte("x"), false,
	); !errors.Is(err, ErrOrderedHashLeafLabSplit) {
		t.Fatalf("full insert = %v", err)
	}
}

func TestOrderedHashLeafLabUpdatePreservesSlotAndOrder(t *testing.T) {
	records := orderedHashLeafLabRecords(230, 8)
	view := openOrderedHashLeafLabTest(t, records)
	target := records[80]
	replacement := []byte("a much longer replacement")
	page, err := view.UpdateTo(
		make([]byte, OrderedHashLeafLabPageSize), 9,
		target.Slot, target.Key, replacement, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	after, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	slot, value, _, ok := after.Lookup(target.Key)
	if !ok || slot != target.Slot || !bytes.Equal(value, replacement) {
		t.Fatalf("updated value = (%d,%q,%v)", slot, value, ok)
	}
	records[80].Value = replacement
	want, err := EncodeOrderedHashLeafLab(
		make([]byte, OrderedHashLeafLabPageSize),
		orderedHashLeafLabTestHeader(9),
		orderedHashLeafLabTestSeed,
		records,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(page, want) {
		t.Fatal("update after-image is not canonical")
	}
	shrunk := []byte("x")
	shrinkPage, err := view.UpdateTo(
		make([]byte, OrderedHashLeafLabPageSize), 10,
		target.Slot, target.Key, shrunk, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	records[80].Value = shrunk
	shrinkWant, err := EncodeOrderedHashLeafLab(
		make([]byte, OrderedHashLeafLabPageSize),
		orderedHashLeafLabTestHeader(10),
		orderedHashLeafLabTestSeed,
		records,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(shrinkPage, shrinkWant) {
		t.Fatal("shrinking update after-image is not canonical")
	}
	iterator := after.AllRows()
	var previous []byte
	for {
		key, _, _, ok := iterator.NextBorrowed()
		if !ok {
			break
		}
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			t.Fatalf("non-lexical update: %q >= %q", previous, key)
		}
		previous = key
	}
}

func TestOrderedHashLeafLabCOWRejectsPartiallyOverlappingDestination(t *testing.T) {
	records := orderedHashLeafLabRecords(8, 8)
	backing := make([]byte, OrderedHashLeafLabPageSize+1)
	page, err := EncodeOrderedHashLeafLab(
		backing[:OrderedHashLeafLabPageSize],
		orderedHashLeafLabTestHeader(7),
		orderedHashLeafLabTestSeed,
		records,
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	target := records[3]
	if _, err := view.UpdateTo(
		backing[1:], 8, target.Slot, target.Key, []byte("replacement"), false,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("partially overlapping COW update = %v", err)
	}
}

func TestOrderedHashLeafLabOwnedMaterializationMatchesImmutableCOW(t *testing.T) {
	records := orderedHashLeafLabRecords(230, 32)
	basePage := encodeOrderedHashLeafLabTest(t, records)
	original := append([]byte(nil), basePage...)
	base, err := OpenOrderedHashLeafLab(basePage, orderedHashLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	target := records[117]

	t.Run("update", func(t *testing.T) {
		replacement := bytes.Repeat([]byte{0x3c}, 48)
		want, err := base.UpdateTo(
			make([]byte, OrderedHashLeafLabPageSize), base.Header().Generation,
			target.Slot, target.Key, replacement, false,
		)
		if err != nil {
			t.Fatal(err)
		}
		got := append([]byte(nil), basePage...)
		owned, err := OpenOrderedHashLeafLab(got, orderedHashLeafLabTestSeed)
		if err != nil {
			t.Fatal(err)
		}
		if err := owned.MaterializeOwnedUpdate(
			target.Slot, target.Key, replacement, false,
		); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("owned update differs from immutable COW")
		}
		if _, err := OpenOrderedHashLeafLab(got, orderedHashLeafLabTestSeed); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		want, err := base.DeleteTo(
			make([]byte, OrderedHashLeafLabPageSize), base.Header().Generation,
			target.Slot, target.Key,
		)
		if err != nil {
			t.Fatal(err)
		}
		got := append([]byte(nil), basePage...)
		owned, err := OpenOrderedHashLeafLab(got, orderedHashLeafLabTestSeed)
		if err != nil {
			t.Fatal(err)
		}
		if err := owned.MaterializeOwnedDelete(target.Slot, target.Key); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("owned delete differs from immutable COW")
		}
		if _, err := OpenOrderedHashLeafLab(got, orderedHashLeafLabTestSeed); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("insert", func(t *testing.T) {
		key := []byte("key-0117a")
		value := []byte("inserted")
		want, wantSlot, err := base.InsertTo(
			make([]byte, OrderedHashLeafLabPageSize), base.Header().Generation,
			key, value, false,
		)
		if err != nil {
			t.Fatal(err)
		}
		got := append([]byte(nil), basePage...)
		owned, err := OpenOrderedHashLeafLab(got, orderedHashLeafLabTestSeed)
		if err != nil {
			t.Fatal(err)
		}
		gotSlot, err := owned.MaterializeOwnedInsert(key, value, false)
		if err != nil {
			t.Fatal(err)
		}
		if gotSlot != wantSlot || !bytes.Equal(got, want) {
			t.Fatalf("owned insert = slot %d, want %d or bytes differ", gotSlot, wantSlot)
		}
		if _, err := OpenOrderedHashLeafLab(got, orderedHashLeafLabTestSeed); err != nil {
			t.Fatal(err)
		}
	})

	if !bytes.Equal(basePage, original) || !bytes.Equal(base.page, original) {
		t.Fatal("immutable COW base was modified")
	}
}

func TestOrderedHashLeafLabOwnedStashDeleteInsertKeepsExactCount(t *testing.T) {
	records := orderedHashLeafLabRecords(256, 1)
	page := encodeOrderedHashLeafLabTest(t, records)
	view, err := OpenOrderedHashLeafLab(page, orderedHashLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	beforeHeader := view.Header()
	beforeStash := view.StashLen()
	var target OrderedHashLeafLabRecord
	found := false
	for _, record := range records {
		if record.Slot >= OrderedHashLeafLabNormalSlotCount {
			target = record
			found = true
			break
		}
	}
	if !found || beforeStash == 0 {
		t.Fatal("full leaf has no stash record")
	}
	if err := view.MaterializeOwnedDelete(target.Slot, target.Key); err != nil {
		t.Fatal(err)
	}
	if view.StashLen() != beforeStash-1 {
		t.Fatalf("stash after delete = %d, want %d", view.StashLen(), beforeStash-1)
	}
	slot, err := view.MaterializeOwnedInsert(target.Key, target.Value, false)
	if err != nil {
		t.Fatal(err)
	}
	if slot < OrderedHashLeafLabNormalSlotCount || view.StashLen() != beforeStash {
		t.Fatalf("stash restore = slot %d count %d, want stash/%d", slot, view.StashLen(), beforeStash)
	}
	if view.Header() != beforeHeader {
		t.Fatalf("stash mutation changed identity: %+v -> %+v", beforeHeader, view.Header())
	}
	if _, err := OpenOrderedHashLeafLab(
		view.PersistentBytes(), orderedHashLeafLabTestSeed,
	); err != nil {
		t.Fatal(err)
	}
}

func TestOrderedHashLeafLabOverflow(t *testing.T) {
	records := orderedHashLeafLabRecords(4, 8)
	records[2].Value = bytes.Repeat([]byte{0xa5}, OrderedHashLeafLabOverflowRefSize)
	records[2].Overflow = true
	view := openOrderedHashLeafLabTest(t, records)
	slot, value, overflow, ok := view.Lookup(records[2].Key)
	if !ok || slot != records[2].Slot || !overflow ||
		!bytes.Equal(value, records[2].Value) {
		t.Fatalf("overflow lookup = (%d,%d,%v,%v)", slot, len(value), overflow, ok)
	}
}

func TestOrderedHashLeafLabOpenRejectsCanonicalCorruption(t *testing.T) {
	records := orderedHashLeafLabRecords(64, 8)
	original := encodeOrderedHashLeafLabTest(t, records)
	tests := []struct {
		name string
		edit func([]byte)
		seal bool
	}{
		{name: "checksum", edit: func(page []byte) { page[100] ^= 1 }},
		{name: "second-checksum-region", edit: func(page []byte) {
			page[orderedHashLeafLabChecksumSplit(page)+7] ^= 1
		}},
		{name: "checksum-table", edit: func(page []byte) {
			page[orderedHashLeafLabTrailerStart(page)] ^= 1
		}},
		{name: "rank", seal: true, edit: func(page []byte) {
			page[orderedHashLeafLabRankStart+int(records[0].Slot)] =
				page[orderedHashLeafLabRankStart+int(records[1].Slot)]
		}},
		{name: "tag", seal: true, edit: func(page []byte) {
			page[orderedHashLeafLabControlStart+int(records[0].Slot)] ^= 1
		}},
		{name: "boundary", seal: true, edit: func(page []byte) {
			layout := orderedHashLeafLabMakeLayout(len(records))
			page[layout.lowStart+1] = page[layout.lowStart]
		}},
		{name: "padding", seal: true, edit: func(page []byte) {
			heapEnd := int(binaryLittleEndianUint16OrderedHashLeafLab(page[38:40]))
			page[heapEnd+1] = 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			page := append([]byte(nil), original...)
			test.edit(page)
			if test.seal {
				sealOrderedHashLeafLab(page)
			}
			if _, err := OpenOrderedHashLeafLab(
				page, orderedHashLeafLabTestSeed,
			); !errors.Is(err, ErrOrderedHashLeafLabCorrupt) {
				t.Fatalf("Open corruption = %v", err)
			}
		})
	}
}

func binaryLittleEndianUint16OrderedHashLeafLab(src []byte) uint16 {
	return uint16(src[0]) | uint16(src[1])<<8
}

func TestOrderedHashLeafLabReadPathsAllocateNothing(t *testing.T) {
	records := orderedHashLeafLabRecords(230, 32)
	view := openOrderedHashLeafLabTest(t, records)
	target := records[115].Key
	if got := testing.AllocsPerRun(1000, func() {
		_, _, _, _ = view.Lookup(target)
	}); got != 0 {
		t.Fatalf("Lookup allocations = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_ = view.LowerBound(target)
	}); got != 0 {
		t.Fatalf("LowerBound allocations = %v", got)
	}
	if got := testing.AllocsPerRun(1000, func() {
		iterator := view.AllRows()
		for {
			_, _, _, ok := iterator.NextBorrowed()
			if !ok {
				break
			}
		}
	}); got != 0 {
		t.Fatalf("iteration allocations = %v", got)
	}
}

func TestOrderedHashLeafLabPlacementIsValidAtHighOccupancy(t *testing.T) {
	records := orderedHashLeafLabRecords(230, 1)
	slots := make([]int, len(records))
	for index, record := range records {
		hash := KeyHashBytes(orderedHashLeafLabTestSeed, record.Key)
		if !orderedHashLeafLabSlotIsCandidate(hash, record.Slot) {
			t.Fatalf("record %d slot %d is not a candidate", index, record.Slot)
		}
		slots[index] = int(record.Slot)
	}
	sort.Ints(slots)
	for index := 1; index < len(slots); index++ {
		if slots[index] == slots[index-1] {
			t.Fatalf("duplicate slot %d", slots[index])
		}
	}
}

func FuzzOpenOrderedHashLeafLab(f *testing.F) {
	records := orderedHashLeafLabRecords(32, 8)
	page, err := EncodeOrderedHashLeafLab(
		make([]byte, OrderedHashLeafLabPageSize),
		orderedHashLeafLabTestHeader(1),
		orderedHashLeafLabTestSeed,
		records,
	)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(page)
	f.Fuzz(func(t *testing.T, candidate []byte) {
		_, _ = OpenOrderedHashLeafLab(candidate, orderedHashLeafLabTestSeed)
	})
}
