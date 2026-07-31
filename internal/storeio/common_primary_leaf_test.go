package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

var (
	commonPrimaryLeafTestSeed = [16]byte{
		0x92, 0x17, 0x44, 0x5a, 0x81, 0xe3, 0x29, 0x6c,
		0x0f, 0xb8, 0x37, 0xd1, 0xaa, 0x63, 0x54, 0x2e,
	}
	commonPrimaryLeafTestStoreID = [16]byte{0x51, 0x39, 0x7a, 0x12}
)

func commonPrimaryLeafTestBounds(t testing.TB) CommonPrimaryLeafBounds {
	t.Helper()
	layout, err := MutableStoreLayout(physicalPageQuantum)
	if err != nil {
		t.Fatal(err)
	}
	return CommonPrimaryLeafBounds{
		FileEnd:           layout.DataStart + 1024*uint64(physicalPageQuantum),
		NextLogicalID:     CommonPrimaryLeafFirstDynamicLogicalID + 4096,
		AllocationQuantum: physicalPageQuantum,
	}
}

func commonPrimaryLeafTestRecords(
	t testing.TB, class CommonPrimaryLeafClass,
	count, valueBytes int,
) []CommonPrimaryLeafRecord {
	t.Helper()
	records := make([]CommonPrimaryLeafRecord, count)
	for index := range records {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(index+1))
		value := bytes.Repeat([]byte{byte(index + 1)}, valueBytes)
		records[index] = CommonPrimaryLeafRecord{
			Key: key, Value: CommonPrimaryLeafValue{Inline: value},
		}
	}
	if err := PlaceCommonPrimaryLeafRecords(
		class, commonPrimaryLeafTestSeed, records,
	); err != nil {
		t.Fatal(err)
	}
	return records
}

func commonPrimaryLeafTestRef(
	t testing.TB,
	bounds CommonPrimaryLeafBounds,
	bucket BucketID,
	generation uint64,
	pageSize uint32,
) PageRef {
	t.Helper()
	logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
	if !ok {
		t.Fatal("invalid test bucket")
	}
	layout, err := MutableStoreLayout(bounds.AllocationQuantum)
	if err != nil {
		t.Fatal(err)
	}
	return PageRef{
		Offset: layout.DataStart, LogicalID: logicalID,
		Generation: generation, Length: pageSize,
		Kind: commonPrimaryLeafPageKind,
	}
}

func commonPrimaryLeafOpenTest(
	t testing.TB,
	class CommonPrimaryLeafClass,
	pageSize uint32,
	records []CommonPrimaryLeafRecord,
) ([]byte, CommonPrimaryLeafView, PageRef, CommonPrimaryLeafBounds) {
	t.Helper()
	const generation = uint64(17)
	const bucket = BucketID(991)
	bounds := commonPrimaryLeafTestBounds(t)
	page, err := EncodeCommonPrimaryLeaf(
		make([]byte, pageSize), class,
		CommonPrimaryLeafHeader{
			StoreID:    commonPrimaryLeafTestStoreID,
			Generation: generation, Bucket: bucket, PageSize: pageSize,
		},
		commonPrimaryLeafTestSeed, records, bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref := commonPrimaryLeafTestRef(
		t, bounds, bucket, generation, pageSize,
	)
	view, err := OpenCommonPrimaryLeaf(
		page, commonPrimaryLeafTestSeed, bucket,
		ref, generation, bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	return page, view, ref, bounds
}

func TestCommonPrimaryLeafGeometry(t *testing.T) {
	if got := CommonPrimaryLeafStructuralBytes(
		CommonPrimaryLeafNarrow,
		CommonPrimaryLeafNarrowLive,
		CommonPrimaryLeafNarrowBytes,
	); got != 973 {
		t.Fatalf("Narrow structural bytes=%d want=973", got)
	}
	if got := CommonPrimaryLeafStructuralBytesPerKey(
		CommonPrimaryLeafNarrow,
		CommonPrimaryLeafNarrowLive,
		CommonPrimaryLeafNarrowBytes,
	); got < 4.989 || got > 4.990 {
		t.Fatalf("Narrow B/key=%f", got)
	}
	if got := CommonPrimaryLeafStructuralBytes(
		CommonPrimaryLeafWide,
		CommonPrimaryLeafWideSlots,
		CommonPrimaryLeafWideBytes,
	); got != 1275 {
		t.Fatalf("Wide structural bytes=%d want=1275", got)
	}
	if got := CommonPrimaryLeafStructuralBytesPerKey(
		CommonPrimaryLeafWide,
		CommonPrimaryLeafWideSlots,
		CommonPrimaryLeafWideBytes,
	); got < 4.980 || got > 4.981 {
		t.Fatalf("Wide B/key=%f", got)
	}

	// Extent and slot class are orthogonal. A representative 249-byte inline
	// JSON value retains 195 stable rows in one 64 KiB Narrow leaf.
	records := commonPrimaryLeafTestRecords(
		t, CommonPrimaryLeafNarrow,
		CommonPrimaryLeafNarrowLive, 249,
	)
	page, view, _, _ := commonPrimaryLeafOpenTest(
		t, CommonPrimaryLeafNarrow, 64<<10, records,
	)
	if len(page) != 64<<10 || view.Len() != len(records) {
		t.Fatalf("representative extent=%d rows=%d", len(page), view.Len())
	}
	t.Logf(
		"representative 8B key + 249B value: structural=%.3f B/key physical=%.3f B/key",
		CommonPrimaryLeafStructuralBytesPerKey(
			CommonPrimaryLeafNarrow, len(records), len(page),
		),
		float64(len(page))/float64(len(records)),
	)
}

func TestCommonPrimaryLeafRoundTripAndScan(t *testing.T) {
	for _, test := range []struct {
		name     string
		class    CommonPrimaryLeafClass
		pageSize uint32
		count    int
	}{
		{"Narrow", CommonPrimaryLeafNarrow, 4 << 10, 195},
		{"Wide", CommonPrimaryLeafWide, 8 << 10, 256},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := commonPrimaryLeafTestRecords(
				t, test.class, test.count, 8,
			)
			_, view, _, _ := commonPrimaryLeafOpenTest(
				t, test.class, test.pageSize, records,
			)
			for index, record := range records {
				slot, value, ok := view.Lookup(record.Key)
				if !ok || slot != record.Slot ||
					!bytes.Equal(value.Inline, record.Value.Inline) {
					t.Fatalf("lookup rank=%d slot=%d ok=%v", index, slot, ok)
				}
			}
			if _, _, ok := view.Lookup([]byte("absent!!")); ok {
				t.Fatal("unexpected miss hit")
			}
			it := view.AllRows()
			for index := range records {
				row, ok := it.Next()
				if !ok || !bytes.Equal(row.Key, records[index].Key) ||
					!bytes.Equal(row.Value.Inline, records[index].Value.Inline) {
					t.Fatalf("scan row=%d ok=%v", index, ok)
				}
			}
			if _, ok := it.Next(); ok {
				t.Fatal("iterator ran past end")
			}
			lower := records[33].Key
			upper := records[47].Key
			rangeIt := view.Range(lower, upper)
			for index := 33; index < 47; index++ {
				row, ok := rangeIt.Next()
				if !ok || !bytes.Equal(row.Key, records[index].Key) {
					t.Fatalf("range rank=%d ok=%v", index, ok)
				}
			}
			if _, ok := rangeIt.Next(); ok {
				t.Fatal("range ignored upper bound")
			}
		})
	}
}

func TestCommonPrimaryLeafEmpty(t *testing.T) {
	page, view, ref, bounds := commonPrimaryLeafOpenTest(
		t, CommonPrimaryLeafNarrow, 4<<10, nil,
	)
	if view.Len() != 0 {
		t.Fatalf("empty Len=%d", view.Len())
	}
	if _, _, ok := view.Lookup([]byte("missing")); ok {
		t.Fatal("empty lookup hit")
	}
	it := view.AllRows()
	if _, ok := it.Next(); ok {
		t.Fatal("empty scan produced a row")
	}
	if _, err := OpenCommonPrimaryLeaf(
		page, commonPrimaryLeafTestSeed, 991,
		ref, uint64(1)<<48, bounds,
	); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
		t.Fatalf("oversized selecting generation err=%v", err)
	}
}

func TestCommonPrimaryLeafLongKeyEscape(t *testing.T) {
	lengths := []int{1, 126, 127, 128, 255, 256}
	records := make([]CommonPrimaryLeafRecord, len(lengths))
	for index, length := range lengths {
		key := bytes.Repeat([]byte{byte('a' + index)}, length)
		records[index] = CommonPrimaryLeafRecord{
			Key: key,
			Value: CommonPrimaryLeafValue{
				Inline: []byte{byte(index + 1)},
			},
		}
	}
	if err := PlaceCommonPrimaryLeafRecords(
		CommonPrimaryLeafNarrow,
		commonPrimaryLeafTestSeed, records,
	); err != nil {
		t.Fatal(err)
	}
	_, view, _, _ := commonPrimaryLeafOpenTest(
		t, CommonPrimaryLeafNarrow, 4<<10, records,
	)
	for _, record := range records {
		slot, value, ok := view.Lookup(record.Key)
		if !ok || slot != record.Slot || len(value.Inline) != 1 {
			t.Fatalf("long-key lookup length=%d", len(record.Key))
		}
	}
	tooLong := bytes.Repeat([]byte("z"), 257)
	if _, _, ok := view.Lookup(tooLong); ok {
		t.Fatal("257-byte key was accepted")
	}
}

func TestCommonPrimaryLeafOverflowRef(t *testing.T) {
	bounds := commonPrimaryLeafTestBounds(t)
	layout, err := MutableStoreLayout(bounds.AllocationQuantum)
	if err != nil {
		t.Fatal(err)
	}
	overflow := PageRef{
		Offset:     layout.DataStart + 8*uint64(bounds.AllocationQuantum),
		LogicalID:  CommonPrimaryLeafFirstDynamicLogicalID + 7,
		Generation: 9, Length: bounds.AllocationQuantum,
		Kind: PageOverflow,
	}
	records := []CommonPrimaryLeafRecord{{
		Key: []byte("overflow"),
		Value: CommonPrimaryLeafValue{
			Overflow: overflow,
		},
	}}
	if err := PlaceCommonPrimaryLeafRecords(
		CommonPrimaryLeafNarrow,
		commonPrimaryLeafTestSeed, records,
	); err != nil {
		t.Fatal(err)
	}
	_, view, _, _ := commonPrimaryLeafOpenTest(
		t, CommonPrimaryLeafNarrow, 4<<10, records,
	)
	_, got, ok := view.Lookup(records[0].Key)
	if !ok || got.Overflow != overflow || len(got.Inline) != 0 {
		t.Fatalf("overflow=%+v ok=%v", got, ok)
	}

	bad := records
	bad[0].Value.Overflow.Generation = 18
	if _, err := EncodeCommonPrimaryLeaf(
		make([]byte, 4<<10), CommonPrimaryLeafNarrow,
		CommonPrimaryLeafHeader{
			StoreID:    commonPrimaryLeafTestStoreID,
			Generation: 17, Bucket: 991, PageSize: 4 << 10,
		},
		commonPrimaryLeafTestSeed, bad, bounds,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("future overflow generation err=%v", err)
	}
}

func TestCommonPrimaryLeafMutations(t *testing.T) {
	records := commonPrimaryLeafTestRecords(
		t, CommonPrimaryLeafNarrow, 100, 8,
	)
	_, view, ref, bounds := commonPrimaryLeafOpenTest(
		t, CommonPrimaryLeafNarrow, 4<<10, records,
	)
	target := records[40]
	updatedValue := CommonPrimaryLeafValue{
		Inline: []byte("updated!"),
	}
	updated, err := view.UpdateTo(
		make([]byte, 4<<10), ref.Generation+1,
		target.Slot, target.Key, updatedValue,
	)
	if err != nil {
		t.Fatal(err)
	}
	updatedRef := ref
	updatedRef.Generation++
	updatedView, err := OpenCommonPrimaryLeaf(
		updated, commonPrimaryLeafTestSeed, view.header.Bucket,
		updatedRef, updatedRef.Generation, bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := updatedView.LookupSlot(target.Slot, target.Key); !ok || !bytes.Equal(value.Inline, updatedValue.Inline) {
		t.Fatal("equal-length update not visible")
	}
	for _, record := range records {
		if bytes.Equal(record.Key, target.Key) {
			continue
		}
		if _, ok := updatedView.LookupSlot(record.Slot, record.Key); !ok {
			t.Fatalf("update moved stable slot %d", record.Slot)
		}
	}

	deleted, err := updatedView.DeleteTo(
		make([]byte, 4<<10), updatedRef.Generation+1,
		target.Slot, target.Key,
	)
	if err != nil {
		t.Fatal(err)
	}
	deletedRef := updatedRef
	deletedRef.Generation++
	deletedView, err := OpenCommonPrimaryLeaf(
		deleted, commonPrimaryLeafTestSeed, view.header.Bucket,
		deletedRef, deletedRef.Generation, bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := deletedView.Lookup(target.Key); ok {
		t.Fatal("delete retained target")
	}
	inserted, slot, err := deletedView.InsertTo(
		make([]byte, 4<<10), deletedRef.Generation+1,
		target.Key, target.Value,
	)
	if err != nil {
		t.Fatal(err)
	}
	insertedRef := deletedRef
	insertedRef.Generation++
	insertedView, err := OpenCommonPrimaryLeaf(
		inserted, commonPrimaryLeafTestSeed, view.header.Bucket,
		insertedRef, insertedRef.Generation, bounds,
	)
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := insertedView.LookupSlot(slot, target.Key); !ok || !bytes.Equal(value.Inline, target.Value.Inline) {
		t.Fatal("insert not visible")
	}
}

func TestCommonPrimaryLeafMutationCanonical(t *testing.T) {
	for _, test := range []struct {
		name     string
		class    CommonPrimaryLeafClass
		pageSize uint32
		count    int
	}{
		{"Narrow", CommonPrimaryLeafNarrow, 4 << 10, 195},
		{"Wide", CommonPrimaryLeafWide, 8 << 10, 224},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := commonPrimaryLeafTestRecords(
				t, test.class, test.count, 8,
			)
			_, view, ref, bounds := commonPrimaryLeafOpenTest(
				t, test.class, test.pageSize, records,
			)
			targets := []int{0, len(records) - 1}
			for index := range records {
				if int(records[index].Slot) >= CommonPrimaryLeafNormalSlots {
					targets = append(targets, index)
					break
				}
			}
			for _, targetIndex := range targets {
				target := records[targetIndex]
				t.Run(fmt.Sprintf("rank-%d-slot-%d", targetIndex, target.Slot), func(t *testing.T) {
					deleted, err := view.DeleteTo(
						make([]byte, test.pageSize), ref.Generation+1,
						target.Slot, target.Key,
					)
					if err != nil {
						t.Fatal(err)
					}
					expectedRecords := make(
						[]CommonPrimaryLeafRecord, 0, len(records)-1,
					)
					expectedRecords = append(expectedRecords, records[:targetIndex]...)
					expectedRecords = append(expectedRecords, records[targetIndex+1:]...)
					expectedDeleted, err := EncodeCommonPrimaryLeaf(
						make([]byte, test.pageSize), test.class,
						CommonPrimaryLeafHeader{
							StoreID:    commonPrimaryLeafTestStoreID,
							Generation: ref.Generation + 1,
							Bucket:     refBucket(ref), PageSize: test.pageSize,
						},
						commonPrimaryLeafTestSeed, expectedRecords, bounds,
					)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(deleted, expectedDeleted) {
						t.Fatal("localized delete is not canonical")
					}
					deletedRef := ref
					deletedRef.Generation++
					deletedView, err := OpenCommonPrimaryLeaf(
						deleted, commonPrimaryLeafTestSeed,
						refBucket(ref), deletedRef, deletedRef.Generation, bounds,
					)
					if err != nil {
						t.Fatal(err)
					}
					restored, err := deletedView.InsertSlotTo(
						make([]byte, test.pageSize), deletedRef.Generation+1,
						target.Slot, target.Key, target.Value,
					)
					if err != nil {
						t.Fatal(err)
					}
					expectedRestored, err := EncodeCommonPrimaryLeaf(
						make([]byte, test.pageSize), test.class,
						CommonPrimaryLeafHeader{
							StoreID:    commonPrimaryLeafTestStoreID,
							Generation: deletedRef.Generation + 1,
							Bucket:     refBucket(ref), PageSize: test.pageSize,
						},
						commonPrimaryLeafTestSeed, records, bounds,
					)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(restored, expectedRestored) {
						t.Fatal("localized stable-slot restore is not canonical")
					}
				})
			}
		})
	}
}

func refBucket(ref PageRef) BucketID {
	return BucketID(ref.LogicalID - CommonPrimaryLeafLeafLogicalIDBase)
}

func TestCommonPrimaryLeafFailsClosed(t *testing.T) {
	records := commonPrimaryLeafTestRecords(
		t, CommonPrimaryLeafNarrow, 64, 8,
	)
	page, _, ref, bounds := commonPrimaryLeafOpenTest(
		t, CommonPrimaryLeafNarrow, 4<<10, records,
	)
	for _, test := range []struct {
		name   string
		mutate func([]byte, *PageRef)
		reseal bool
	}{
		{"checksum", func(page []byte, _ *PageRef) { page[100] ^= 1 }, false},
		{"wrong logical", func(_ []byte, ref *PageRef) { ref.LogicalID++ }, false},
		{"future generation", func(_ []byte, ref *PageRef) { ref.Generation++ }, false},
		{"bad class", func(page []byte, _ *PageRef) {
			page[PageHeaderSize+2] = 7
		}, true},
		{"bad long escape", func(page []byte, _ *PageRef) {
			// Turn the first short key into an escape whose prefix decodes to
			// length one; structural validation must reject it.
			payload := page[PageHeaderSize:]
			layout := commonPrimaryLeafLayoutFor(
				CommonPrimaryLeafNarrow, len(records), len(page),
			)
			commonPrimaryLeafPutKeyLength(
				payload, &layout, 0, commonPrimaryLeafEscapeLength,
			)
			payload[layout.heapStart] = 0
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			corrupt := append([]byte(nil), page...)
			badRef := ref
			test.mutate(corrupt, &badRef)
			if test.reseal {
				if _, err := sealPage(corrupt, false); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := OpenCommonPrimaryLeaf(
				corrupt, commonPrimaryLeafTestSeed, 991,
				badRef, ref.Generation, bounds,
			); !errors.Is(err, ErrCommonPrimaryLeafCorrupt) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestCommonPrimaryLeafNoReadAllocations(t *testing.T) {
	records := commonPrimaryLeafTestRecords(
		t, CommonPrimaryLeafNarrow, 195, 8,
	)
	page, view, ref, bounds := commonPrimaryLeafOpenTest(
		t, CommonPrimaryLeafNarrow, 4<<10, records,
	)
	if allocs := testing.AllocsPerRun(100, func() {
		opened, err := OpenCommonPrimaryLeaf(
			page, commonPrimaryLeafTestSeed, 991,
			ref, ref.Generation, bounds,
		)
		if err != nil || opened.Len() != len(records) {
			panic(fmt.Sprintf("open: %v", err))
		}
	}); allocs != 0 {
		t.Fatalf("Open allocations=%f", allocs)
	}
	target := records[len(records)/2]
	hash := commonPrimaryLeafHash(
		commonPrimaryLeafTestSeed, target.Key,
	)
	if allocs := testing.AllocsPerRun(1000, func() {
		_, value, ok := view.LookupHashed(hash, target.Key)
		if !ok || len(value.Inline) != 8 {
			panic("lookup")
		}
	}); allocs != 0 {
		t.Fatalf("Lookup allocations=%f", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		it := view.AllRows()
		count := 0
		for {
			_, ok := it.Next()
			if !ok {
				break
			}
			count++
		}
		if count != len(records) {
			panic("scan")
		}
	}); allocs != 0 {
		t.Fatalf("scan allocations=%f", allocs)
	}
}
