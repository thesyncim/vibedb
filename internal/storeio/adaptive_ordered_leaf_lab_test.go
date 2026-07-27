package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

var adaptiveOrderedLeafLabTestSeed = [16]byte{
	0x73, 0x11, 0xe2, 0x94, 0x55, 0x36, 0xc7, 0x18,
	0xb9, 0x6a, 0x4b, 0xdc, 0x2d, 0x8e, 0xff, 0x40,
}

func adaptiveOrderedLeafLabTestHeader(generation uint64) AdaptiveOrderedLeafLabHeader {
	return AdaptiveOrderedLeafLabHeader{
		BucketID: 0x0102030405060708, Generation: generation,
	}
}

func adaptiveOrderedLeafLabTestRecords(
	t testing.TB,
	class AdaptiveOrderedLeafLabClass,
	count, valueLength int,
) []AdaptiveOrderedLeafLabRecord {
	t.Helper()
	records := make([]AdaptiveOrderedLeafLabRecord, count)
	for index := range records {
		records[index] = AdaptiveOrderedLeafLabRecord{
			Key:   []byte(fmt.Sprintf("k%07d", index)),
			Value: bytes.Repeat([]byte{byte(index), byte(index >> 8)}, (valueLength+1)/2)[:valueLength],
		}
	}
	if err := PlaceAdaptiveOrderedLeafLabRecords(
		class, adaptiveOrderedLeafLabTestSeed, records,
	); err != nil {
		t.Fatal(err)
	}
	return records
}

func encodeAdaptiveOrderedLeafLabTest(
	t testing.TB,
	class AdaptiveOrderedLeafLabClass,
	records []AdaptiveOrderedLeafLabRecord,
) []byte {
	t.Helper()
	page, err := EncodeAdaptiveOrderedLeafLab(
		make([]byte, class.extentBytes()),
		class,
		adaptiveOrderedLeafLabTestHeader(7),
		adaptiveOrderedLeafLabTestSeed,
		records,
	)
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func openAdaptiveOrderedLeafLabTest(
	t testing.TB,
	class AdaptiveOrderedLeafLabClass,
	records []AdaptiveOrderedLeafLabRecord,
) AdaptiveOrderedLeafLabView {
	t.Helper()
	view, err := OpenAdaptiveOrderedLeafLab(
		encodeAdaptiveOrderedLeafLabTest(t, class, records),
		adaptiveOrderedLeafLabTestSeed,
	)
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func TestAdaptiveOrderedLeafLabNarrowSpaceTargets(t *testing.T) {
	records := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	view := openAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabNarrow, records,
	)
	if got := AdaptiveOrderedLeafLabMetadataBytesPerLive(
		AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget,
	); got > 5 {
		t.Fatalf("narrow structural = %.4f B/live, want <= 5", got)
	}
	if got := float64(view.PhysicalSlackBytes()) / float64(view.Len()); got > 1 {
		t.Fatalf("narrow physical slack = %.4f B/live, want <= 1", got)
	}
	if len(view.PersistentBytes()) != AdaptiveOrderedLeafLabNarrowBytes {
		t.Fatalf("narrow image = %d, want 4096", len(view.PersistentBytes()))
	}
	if view.StashLen() < 3 {
		t.Fatalf("narrow stash = %d, want at least the 3 rows beyond normal capacity", view.StashLen())
	}
	t.Logf(
		"narrow target: structural=%.4f B/live slack=%.4f B/live stash=%d",
		AdaptiveOrderedLeafLabMetadataBytesPerLive(
			AdaptiveOrderedLeafLabNarrow, view.Len(),
		),
		float64(view.PhysicalSlackBytes())/float64(view.Len()),
		view.StashLen(),
	)
}

func TestAdaptiveOrderedLeafLabWideSpaceTargets(t *testing.T) {
	const live = AdaptiveOrderedLeafLabWideSlots
	if got := AdaptiveOrderedLeafLabMetadataBytesPerLive(
		AdaptiveOrderedLeafLabWide, live,
	); got > 5 {
		t.Fatalf("wide structural = %.4f B/live, want <= 5", got)
	}
	if got := AdaptiveOrderedLeafLabMetadataBytes(
		AdaptiveOrderedLeafLabWide, live,
	); got != 1280 {
		t.Fatalf("full Wide structural bytes = %d, want 1280", got)
	}
	const churnLive = AdaptiveOrderedLeafLabNarrowLiveTarget
	if got := AdaptiveOrderedLeafLabMetadataBytesPerLive(
		AdaptiveOrderedLeafLabWide, churnLive,
	); got > 5 {
		t.Fatalf("sparse Wide structural = %.4f B/live, want <= 5", got)
	}
	if got := AdaptiveOrderedLeafLabMetadataBytes(
		AdaptiveOrderedLeafLabWide, churnLive,
	); got != 910 {
		t.Fatalf("sparse Wide structural bytes = %d, want 910", got)
	}
	records := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabWide, churnLive, 8,
	)
	view := openAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabWide, records,
	)
	if got := len(view.PersistentBytes()); got != AdaptiveOrderedLeafLabNarrowBytes {
		t.Fatalf("sparse Wide extent = %d, want 4096; stash=%d layout=%+v",
			got, view.StashLen(), view.layout)
	}
}

func TestAdaptiveOrderedLeafLabPlanExactSlabAndExtentCliff(t *testing.T) {
	for _, test := range []struct {
		name        string
		class       AdaptiveOrderedLeafLabClass
		live        int
		valueLength int
		wantExtent  int
	}{
		{"Narrow195", AdaptiveOrderedLeafLabNarrow, 195, 8, 4 << 10},
		{"Wide195", AdaptiveOrderedLeafLabWide, 195, 8, 4 << 10},
		{"Wide196", AdaptiveOrderedLeafLabWide, 196, 8, 4 << 10},
		{"Wide256Small", AdaptiveOrderedLeafLabWide, 256, 1, 4 << 10},
		{"Wide256Larger", AdaptiveOrderedLeafLabWide, 256, 15, 8 << 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := adaptiveOrderedLeafLabTestRecords(
				t, test.class, test.live, test.valueLength,
			)
			plan, err := PlanAdaptiveOrderedLeafLab(
				test.class, adaptiveOrderedLeafLabTestSeed, records,
			)
			if err != nil {
				t.Fatal(err)
			}
			if plan.ExtentBytes() != test.wantExtent ||
				plan.SlabClass() != test.wantExtent {
				t.Fatalf("extent/slab = %d/%d, want %d; plan=%+v",
					plan.ExtentBytes(), plan.SlabClass(), test.wantExtent, plan)
			}
			dst := make([]byte, AdaptiveOrderedLeafLabWideBytes)
			page, err := plan.Encode(
				dst, adaptiveOrderedLeafLabTestHeader(9),
				adaptiveOrderedLeafLabTestSeed, records,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) != test.wantExtent || cap(page) != test.wantExtent {
				t.Fatalf("encoded len/cap = %d/%d, want %d",
					len(page), cap(page), test.wantExtent)
			}
			view, err := OpenAdaptiveOrderedLeafLab(
				page, adaptiveOrderedLeafLabTestSeed,
			)
			if err != nil {
				t.Fatal(err)
			}
			if plan.MetadataBytes() !=
				view.layout.heapStart+AdaptiveOrderedLeafLabTrailerBytes {
				t.Fatalf("metadata = %d, layout = %d",
					plan.MetadataBytes(),
					view.layout.heapStart+AdaptiveOrderedLeafLabTrailerBytes)
			}
			if plan.MetadataBytes()+plan.PayloadBytes()+
				view.PhysicalSlackBytes() != plan.ExtentBytes() {
				t.Fatalf("extent accounting: metadata=%d payload=%d slack=%d extent=%d",
					plan.MetadataBytes(), plan.PayloadBytes(),
					view.PhysicalSlackBytes(), plan.ExtentBytes())
			}
		})
	}
}

func TestAdaptiveOrderedLeafLabRejectsDestinationAliasesBeforeClear(t *testing.T) {
	backing := bytes.Repeat([]byte{0xa5}, AdaptiveOrderedLeafLabNarrowBytes)
	key := backing[128:136]
	copy(key, "alias-key")
	records := []AdaptiveOrderedLeafLabRecord{{
		Slot:  0,
		Key:   key,
		Value: []byte("12345678"),
	}}
	hash := adaptiveOrderedLeafLabKeyHash(adaptiveOrderedLeafLabTestSeed, key)
	records[0].Slot = adaptiveOrderedLeafLabCandidate(hash, 0)
	plan, err := PlanAdaptiveOrderedLeafLab(
		AdaptiveOrderedLeafLabNarrow, adaptiveOrderedLeafLabTestSeed, records,
	)
	if err != nil {
		t.Fatal(err)
	}
	before := append([]byte(nil), backing...)
	if _, err := plan.Encode(
		backing, adaptiveOrderedLeafLabTestHeader(1),
		adaptiveOrderedLeafLabTestSeed, records,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("aliased encode = %v, want ErrInvalidWrite", err)
	}
	if !bytes.Equal(backing, before) {
		t.Fatal("aliased encode cleared or changed source before rejection")
	}

	live := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	view := openAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabNarrow, live,
	)
	target := live[0]
	pageBefore := append([]byte(nil), view.page...)
	if _, err := view.UpdateTo(
		view.page, 8, target.Slot, target.Key, target.Value, false,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("aliased UpdateTo = %v", err)
	}
	if _, _, err := view.InsertTo(
		view.page, 8, []byte("z-alias"), []byte("12345678"), false,
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("aliased InsertTo = %v", err)
	}
	if !bytes.Equal(view.page, pageBefore) {
		t.Fatal("aliased mutation changed source image")
	}
}

func TestAdaptiveOrderedLeafLabStashExactCheckBound(t *testing.T) {
	records := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabWide, AdaptiveOrderedLeafLabWideSlots, 1,
	)
	view := openAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabWide, records,
	)
	counts := make(map[uint16]int)
	salt := view.page[21]
	for _, record := range records {
		if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
			continue
		}
		hash := adaptiveOrderedLeafLabKeyHash(
			adaptiveOrderedLeafLabTestSeed, record.Key,
		)
		tag := adaptiveOrderedLeafLabSaltedStashTag12(hash, salt)
		counts[tag]++
	}
	for tag, count := range counts {
		if count > AdaptiveOrderedLeafLabMaxStashExactChecks {
			t.Fatalf("tag %d selects %d exact candidates, bound %d",
				tag, count, AdaptiveOrderedLeafLabMaxStashExactChecks)
		}
	}
}

func TestAdaptiveOrderedLeafLabExactLookupAndOrderedOperations(t *testing.T) {
	for _, class := range []AdaptiveOrderedLeafLabClass{
		AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabWide,
	} {
		count := AdaptiveOrderedLeafLabNarrowLiveTarget
		if class == AdaptiveOrderedLeafLabWide {
			count = 256
		}
		t.Run(fmt.Sprintf("class-%d", class), func(t *testing.T) {
			records := adaptiveOrderedLeafLabTestRecords(t, class, count, 8)
			view := openAdaptiveOrderedLeafLabTest(t, class, records)
			if view.Class() != class || view.Len() != count ||
				view.Header() != adaptiveOrderedLeafLabTestHeader(7) {
				t.Fatalf("header/class/count = %+v/%d/%d", view.Header(), view.Class(), view.Len())
			}
			for _, record := range records {
				slot, value, overflow, ok := view.Lookup(record.Key)
				if !ok || slot != record.Slot || overflow ||
					!bytes.Equal(value, record.Value) {
					t.Fatalf("Lookup(%q) = (%d,%x,%v,%v), slot %d", record.Key, slot, value, overflow, ok, record.Slot)
				}
				value, overflow, ok = view.LookupSlot(record.Slot, record.Key)
				if !ok || overflow || !bytes.Equal(value, record.Value) {
					t.Fatalf("LookupSlot(%d,%q) failed", record.Slot, record.Key)
				}
			}
			if _, _, _, ok := view.Lookup([]byte("not-here")); ok {
				t.Fatal("missing key found")
			}
			if got := view.LowerBound([]byte("k0000100")); got != 100 {
				t.Fatalf("LowerBound = %d, want 100", got)
			}

			iterator := view.Range([]byte("k0000100"), []byte("k0000110"))
			for index := 100; index < 110; index++ {
				key, value, overflow, ok := iterator.NextBorrowed()
				if !ok || overflow || !bytes.Equal(key, records[index].Key) ||
					!bytes.Equal(value, records[index].Value) {
					t.Fatalf("range row %d = %q/%x/%v/%v", index, key, value, overflow, ok)
				}
			}
			if _, _, _, ok := iterator.NextBorrowed(); ok {
				t.Fatal("range exceeded upper bound")
			}

			prefix := view.Prefix([]byte("k00001"))
			prefixCount := 0
			for {
				key, _, _, ok := prefix.NextBorrowed()
				if !ok {
					break
				}
				if !bytes.HasPrefix(key, []byte("k00001")) {
					t.Fatalf("prefix returned %q", key)
				}
				prefixCount++
			}
			wantPrefix := min(100, count-100)
			if prefixCount != wantPrefix {
				t.Fatalf("prefix count = %d, want %d", prefixCount, wantPrefix)
			}
		})
	}
}

func TestAdaptiveOrderedLeafLabReadPathsAllocateZero(t *testing.T) {
	records := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	view := openAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabNarrow, records,
	)
	normal := records[0]
	stash := records[len(records)-1]
	for _, record := range records {
		if record.Slot >= AdaptiveOrderedLeafLabNormalSlots {
			stash = record
			break
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		_, _, _, _ = view.Lookup(normal.Key)
		_, _, _, _ = view.Lookup(stash.Key)
		_, _, _, _ = view.Lookup([]byte("not-here"))
		iterator := view.AllRows()
		for {
			_, _, _, ok := iterator.NextBorrowed()
			if !ok {
				break
			}
		}
	}); allocations != 0 {
		t.Fatalf("read allocations = %.2f, want 0", allocations)
	}
}

func TestAdaptiveOrderedLeafLabPromotionPreservesEverySlot(t *testing.T) {
	records := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	narrow := openAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabNarrow, records,
	)
	page, err := PromoteAdaptiveOrderedLeafLab(
		make([]byte, AdaptiveOrderedLeafLabWideBytes), 8, &narrow,
	)
	if err != nil {
		t.Fatal(err)
	}
	wide, err := OpenAdaptiveOrderedLeafLab(page, adaptiveOrderedLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	if wide.Class() != AdaptiveOrderedLeafLabWide ||
		wide.Header().Generation != 8 ||
		wide.Header().BucketID != narrow.Header().BucketID {
		t.Fatalf("promoted identity = %+v class %d", wide.Header(), wide.Class())
	}
	for _, record := range records {
		slot, value, _, ok := wide.Lookup(record.Key)
		if !ok || slot != record.Slot || !bytes.Equal(value, record.Value) {
			t.Fatalf("promotion changed slot/value for %q: %d -> %d", record.Key, record.Slot, slot)
		}
	}

	extraKey := []byte("z0000000")
	page, slot, err := wide.InsertTo(
		make([]byte, AdaptiveOrderedLeafLabWideBytes),
		9, extraKey, []byte("12345678"), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if int(slot) < AdaptiveOrderedLeafLabNormalSlots {
		t.Fatalf("full normal region inserted at normal slot %d", slot)
	}
	wide, err = OpenAdaptiveOrderedLeafLab(page, adaptiveOrderedLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	if got, _, _, ok := wide.Lookup(extraKey); !ok || got != slot {
		t.Fatalf("post-promotion insert = %d/%v, want %d", got, ok, slot)
	}
}

func TestAdaptiveOrderedLeafLabDeleteRestoreAndUpdate(t *testing.T) {
	records := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	original := openAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabNarrow, records,
	)
	target := records[len(records)/2]
	deletedPage, err := original.DeleteTo(
		make([]byte, AdaptiveOrderedLeafLabNarrowBytes),
		8, target.Slot, target.Key,
	)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := OpenAdaptiveOrderedLeafLab(
		deletedPage, adaptiveOrderedLeafLabTestSeed,
	)
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Len() != original.Len()-1 {
		t.Fatalf("delete len = %d, want %d", deleted.Len(), original.Len()-1)
	}
	if _, _, _, ok := deleted.Lookup(target.Key); ok {
		t.Fatal("deleted key remains visible")
	}
	for _, record := range records {
		if bytes.Equal(record.Key, target.Key) {
			continue
		}
		slot, _, _, ok := deleted.Lookup(record.Key)
		if !ok || slot != record.Slot {
			t.Fatalf("delete moved survivor %q slot %d -> %d", record.Key, record.Slot, slot)
		}
	}

	restoredPage, err := deleted.RestoreTo(
		make([]byte, AdaptiveOrderedLeafLabNarrowBytes),
		9, target.Slot, target.Key, target.Value, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := OpenAdaptiveOrderedLeafLab(
		restoredPage, adaptiveOrderedLeafLabTestSeed,
	)
	if err != nil {
		t.Fatal(err)
	}
	if slot, value, _, ok := restored.Lookup(target.Key); !ok ||
		slot != target.Slot || !bytes.Equal(value, target.Value) {
		t.Fatalf("restore = %d/%x/%v, want slot %d", slot, value, ok, target.Slot)
	}

	replacement := []byte("REPLACED")
	headerBefore := restored.Header()
	if err := restored.MaterializeOwnedUpdate(
		target.Slot, target.Key, replacement, false,
	); err != nil {
		t.Fatal(err)
	}
	if restored.Header() != headerBefore {
		t.Fatalf("owned update changed identity: %+v -> %+v", headerBefore, restored.Header())
	}
	if value, _, ok := restored.LookupSlot(target.Slot, target.Key); !ok ||
		!bytes.Equal(value, replacement) {
		t.Fatalf("owned replacement = %q/%v", value, ok)
	}
	if err := restored.MaterializeOwnedUpdate(
		target.Slot, target.Key, []byte("short"), false,
	); !errors.Is(err, ErrAdaptiveOrderedLeafLabNeedsRewrite) {
		t.Fatalf("length-changing owned update = %v", err)
	}
}

func TestAdaptiveOrderedLeafLabWideRank255AndFullStash(t *testing.T) {
	records := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabWide, AdaptiveOrderedLeafLabWideSlots, 1,
	)
	view := openAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabWide, records,
	)
	if view.Len() != 256 || view.StashLen() < 64 {
		t.Fatalf("wide full len/stash = %d/%d", view.Len(), view.StashLen())
	}
	last := records[255]
	slot, value, _, ok := view.Lookup(last.Key)
	if !ok || slot != last.Slot || !bytes.Equal(value, last.Value) {
		t.Fatalf("rank 255 lookup = %d/%x/%v, slot %d", slot, value, ok, last.Slot)
	}
}

func TestAdaptiveOrderedLeafLabSparseWideFullStableStash(t *testing.T) {
	full := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabWide, AdaptiveOrderedLeafLabWideSlots, 1,
	)
	const live = AdaptiveOrderedLeafLabNarrowLiveTarget
	records := make([]AdaptiveOrderedLeafLabRecord, 0, live)
	normal, stash := 0, 0
	for _, record := range full {
		switch {
		case int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots &&
			normal < live-AdaptiveOrderedLeafLabWideStash:
			records = append(records, record)
			normal++
		case int(record.Slot) >= AdaptiveOrderedLeafLabNormalSlots:
			records = append(records, record)
			stash++
		}
	}
	if len(records) != live || stash != AdaptiveOrderedLeafLabWideStash {
		t.Fatalf("scan fixture rows/stash = %d/%d", len(records), stash)
	}
	view := openAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabWide, records,
	)
	if view.layout.stashHashLen != 0 || view.layout.filterLen != 32 ||
		view.layout.stashTagLen != 144 {
		t.Fatalf("sparse scan layout = %+v", view.layout)
	}
	if len(view.page) != AdaptiveOrderedLeafLabNarrowBytes {
		t.Fatalf("sparse scan extent = %d, want 4096", len(view.page))
	}
	if structural := float64(
		view.layout.heapStart+AdaptiveOrderedLeafLabTrailerBytes,
	) / float64(view.Len()); structural > 5.7 {
		t.Fatalf("exceptional 64-stash structure = %.4f B/live, want <= 5.7", structural)
	}
	for _, record := range records {
		slot, value, overflow, ok := view.Lookup(record.Key)
		if !ok || slot != record.Slot || overflow != record.Overflow ||
			!bytes.Equal(value, record.Value) {
			t.Fatalf("scan lookup key=%x = %d/%x/%v/%v, want %d/%x/%v",
				record.Key, slot, value, overflow, ok,
				record.Slot, record.Value, record.Overflow)
		}
	}
}

func TestAdaptiveOrderedLeafLabSparseWideCanonicalModeBoundary(t *testing.T) {
	full := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabWide, AdaptiveOrderedLeafLabWideSlots, 1,
	)
	for _, test := range []struct {
		name          string
		stashIndexes  map[int]bool
		denseRanks    bool
		tagBytes      int
		normalRecords int
	}{
		{
			name: "Indexed25",
			stashIndexes: func() map[int]bool {
				m := make(map[int]bool)
				for i := 0; i < 25; i++ {
					m[i] = true
				}
				return m
			}(),
			denseRanks:    true,
			tagBytes:      64,
			normalRecords: 170,
		},
		{
			name: "Scan26",
			stashIndexes: func() map[int]bool {
				m := make(map[int]bool)
				for i := 0; i < 26; i++ {
					m[i] = true
				}
				return m
			}(),
			normalRecords: 169,
			denseRanks:    true,
			tagBytes:      64,
		},
		{
			name:          "ScanHighStableSlot",
			stashIndexes:  map[int]bool{0: true, 1: true, 31: true},
			normalRecords: 192,
			denseRanks:    true,
			tagBytes:      32,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := make([]AdaptiveOrderedLeafLabRecord, 0,
				AdaptiveOrderedLeafLabNarrowLiveTarget)
			normal := 0
			for _, record := range full {
				if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
					if normal < test.normalRecords {
						records = append(records, record)
						normal++
					}
					continue
				}
				index := int(record.Slot) - AdaptiveOrderedLeafLabNormalSlots
				if test.stashIndexes[index] {
					records = append(records, record)
				}
			}
			if len(records) != AdaptiveOrderedLeafLabNarrowLiveTarget {
				t.Fatalf("fixture rows = %d", len(records))
			}
			page := encodeAdaptiveOrderedLeafLabTest(
				t, AdaptiveOrderedLeafLabWide, records,
			)
			view, err := OpenAdaptiveOrderedLeafLab(
				page, adaptiveOrderedLeafLabTestSeed,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := view.layout.denseStashRanks; got != test.denseRanks {
				t.Fatalf("dense stash ranks = %v, want %v; layout=%+v",
					got, test.denseRanks, view.layout)
			}
			if view.layout.stashHashLen != 0 ||
				view.layout.stashTagLen != test.tagBytes ||
				view.layout.stashRankLen != len(test.stashIndexes) {
				t.Fatalf("canonical sparse metadata = %+v", view.layout)
			}
		})
	}
}

func TestAdaptiveOrderedLeafLabFailClosedCorruption(t *testing.T) {
	records := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabNarrow,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 8,
	)
	canonical := encodeAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabNarrow, records,
	)
	for _, at := range []int{
		0, 12, 16, 22,
		AdaptiveOrderedLeafLabHeaderBytes,
		len(canonical) / 2,
		len(canonical) - AdaptiveOrderedLeafLabTrailerBytes,
	} {
		corrupt := append([]byte(nil), canonical...)
		corrupt[at] ^= 0x80
		if _, err := OpenAdaptiveOrderedLeafLab(
			corrupt, adaptiveOrderedLeafLabTestSeed,
		); !errors.Is(err, ErrAdaptiveOrderedLeafLabCorrupt) {
			t.Fatalf("corruption at %d admitted: %v", at, err)
		}
	}

	view, err := OpenAdaptiveOrderedLeafLab(canonical, adaptiveOrderedLeafLabTestSeed)
	if err != nil {
		t.Fatal(err)
	}
	semantic := append([]byte(nil), canonical...)
	firstLive, secondLive := -1, -1
	for slot := 0; slot < AdaptiveOrderedLeafLabNormalSlots; slot++ {
		if semantic[view.layout.controlStart+slot] == 0 {
			continue
		}
		if firstLive < 0 {
			firstLive = slot
		} else {
			secondLive = slot
			break
		}
	}
	if firstLive < 0 || secondLive < 0 {
		t.Fatal("normal corruption fixture")
	}
	semantic[view.layout.normalRankStart+secondLive] =
		semantic[view.layout.normalRankStart+firstLive]
	adaptiveOrderedLeafLabSeal(semantic)
	if _, err := OpenAdaptiveOrderedLeafLab(
		semantic, adaptiveOrderedLeafLabTestSeed,
	); !errors.Is(err, ErrAdaptiveOrderedLeafLabCorrupt) {
		t.Fatalf("duplicate rank admitted: %v", err)
	}

	filter := append([]byte(nil), canonical...)
	filter[21] ^= 0xff
	adaptiveOrderedLeafLabSeal(filter)
	if _, err := OpenAdaptiveOrderedLeafLab(
		filter, adaptiveOrderedLeafLabTestSeed,
	); !errors.Is(err, ErrAdaptiveOrderedLeafLabCorrupt) {
		t.Fatalf("wrong rejection filter admitted: %v", err)
	}

	wideRecords := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabWide, AdaptiveOrderedLeafLabWideSlots, 1,
	)
	widePage := encodeAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabWide, wideRecords,
	)
	wide, err := OpenAdaptiveOrderedLeafLab(
		widePage, adaptiveOrderedLeafLabTestSeed,
	)
	if err != nil {
		t.Fatal(err)
	}
	hashDirectory := append([]byte(nil), widePage...)
	hashDirectory[wide.layout.stashBitmap] ^= 1
	adaptiveOrderedLeafLabSeal(hashDirectory)
	if _, err := OpenAdaptiveOrderedLeafLab(
		hashDirectory, adaptiveOrderedLeafLabTestSeed,
	); !errors.Is(err, ErrAdaptiveOrderedLeafLabCorrupt) {
		t.Fatalf("wrong Wide stash occupancy admitted: %v", err)
	}

	stashTag := append([]byte(nil), widePage...)
	stashTag[wide.layout.stashTagStart] ^= 1
	adaptiveOrderedLeafLabSeal(stashTag)
	if _, err := OpenAdaptiveOrderedLeafLab(
		stashTag, adaptiveOrderedLeafLabTestSeed,
	); !errors.Is(err, ErrAdaptiveOrderedLeafLabCorrupt) {
		t.Fatalf("wrong packed Wide stash tag admitted: %v", err)
	}

	sparseRecords := adaptiveOrderedLeafLabTestRecords(
		t, AdaptiveOrderedLeafLabWide,
		AdaptiveOrderedLeafLabNarrowLiveTarget, 1,
	)
	sparsePage := encodeAdaptiveOrderedLeafLabTest(
		t, AdaptiveOrderedLeafLabWide, sparseRecords,
	)
	sparse, err := OpenAdaptiveOrderedLeafLab(
		sparsePage, adaptiveOrderedLeafLabTestSeed,
	)
	if err != nil {
		t.Fatal(err)
	}
	sparseDirectory := append([]byte(nil), sparsePage...)
	sparseDirectory[sparse.layout.stashBitmap] ^= 0x80
	adaptiveOrderedLeafLabSeal(sparseDirectory)
	if _, err := OpenAdaptiveOrderedLeafLab(
		sparseDirectory, adaptiveOrderedLeafLabTestSeed,
	); !errors.Is(err, ErrAdaptiveOrderedLeafLabCorrupt) {
		t.Fatalf("wrong sparse Wide dense-rank bitmap admitted: %v", err)
	}

	sparseMode := append([]byte(nil), sparsePage...)
	sparseMode[21] ^= 0xff
	adaptiveOrderedLeafLabSeal(sparseMode)
	if _, err := OpenAdaptiveOrderedLeafLab(
		sparseMode, adaptiveOrderedLeafLabTestSeed,
	); !errors.Is(err, ErrAdaptiveOrderedLeafLabCorrupt) {
		t.Fatalf("non-canonical sparse Wide salt admitted: %v", err)
	}
}

func TestAdaptiveOrderedLeafLabPlacementAcrossSeedsAndSkew(t *testing.T) {
	base := make([]AdaptiveOrderedLeafLabRecord, AdaptiveOrderedLeafLabWideSlots)
	for index := range base {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(index))
		base[index] = AdaptiveOrderedLeafLabRecord{
			Key: append([]byte(nil), key[:]...), Value: []byte{byte(index)},
		}
	}
	random := rand.New(rand.NewSource(0x51707))
	for iteration := 0; iteration < 32; iteration++ {
		var seed [16]byte
		binary.LittleEndian.PutUint64(seed[:8], random.Uint64())
		binary.LittleEndian.PutUint64(seed[8:], random.Uint64())
		records := append([]AdaptiveOrderedLeafLabRecord(nil), base...)
		if err := PlaceAdaptiveOrderedLeafLabRecords(
			AdaptiveOrderedLeafLabWide, seed, records,
		); err != nil {
			t.Fatalf("seed %d placement: %v", iteration, err)
		}
		normal := 0
		for _, record := range records {
			if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
				normal++
			}
		}
		if normal != AdaptiveOrderedLeafLabNormalSlots {
			t.Fatalf("seed %d normal rows = %d, want %d",
				iteration, normal, AdaptiveOrderedLeafLabNormalSlots)
		}
		if iteration < 4 {
			page, err := EncodeAdaptiveOrderedLeafLab(
				make([]byte, AdaptiveOrderedLeafLabWideBytes),
				AdaptiveOrderedLeafLabWide,
				adaptiveOrderedLeafLabTestHeader(uint64(iteration+1)),
				seed, records,
			)
			if err != nil {
				t.Fatalf("seed %d encode: %v", iteration, err)
			}
			if _, err := OpenAdaptiveOrderedLeafLab(page, seed); err != nil {
				t.Fatalf("seed %d open: %v", iteration, err)
			}
		}
	}

	skewed := make([]AdaptiveOrderedLeafLabRecord, 0, AdaptiveOrderedLeafLabWideSlots)
	var secondCounts [adaptiveOrderedLeafLabGroupCount]int
	for candidate := uint64(0); len(skewed) < cap(skewed); candidate++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], candidate)
		hash := adaptiveOrderedLeafLabKeyHash(adaptiveOrderedLeafLabTestSeed, key[:])
		first, second := adaptiveOrderedLeafLabGroups(hash)
		if first != 0 || second == 0 || secondCounts[second] == 24 {
			continue
		}
		secondCounts[second]++
		skewed = append(skewed, AdaptiveOrderedLeafLabRecord{
			Key: append([]byte(nil), key[:]...), Value: []byte{1},
		})
	}
	if err := PlaceAdaptiveOrderedLeafLabRecords(
		AdaptiveOrderedLeafLabWide, adaptiveOrderedLeafLabTestSeed, skewed,
	); err != nil {
		t.Fatalf("skewed placement: %v", err)
	}
	normal := 0
	for _, record := range skewed {
		if int(record.Slot) < AdaptiveOrderedLeafLabNormalSlots {
			normal++
		}
	}
	if normal != AdaptiveOrderedLeafLabNormalSlots {
		t.Fatalf("skewed normal rows = %d, want %d",
			normal, AdaptiveOrderedLeafLabNormalSlots)
	}
}

func TestAdaptiveOrderedLeafLabSteadyReplacementSpaceEnvelope(t *testing.T) {
	const (
		leaves = 8
		steps  = 4096
		live   = AdaptiveOrderedLeafLabNarrowLiveTarget
	)
	promoted := 0
	promotionStepTotal := 0
	minPromotionStep := steps + 1
	maxPromotionStep := 0
	stashTotal := 0
	maxStash := 0
	maxStructural := 0.0
	maxPhysical := 0.0
	for leaf := 0; leaf < leaves; leaf++ {
		var seed [16]byte
		binary.LittleEndian.PutUint64(seed[:8], uint64(leaf+1)*0x9e3779b97f4a7c15)
		binary.LittleEndian.PutUint64(seed[8:], uint64(leaf+1)*0xd1b54a32d192ed03)
		records := make([]AdaptiveOrderedLeafLabRecord, live)
		var keys [live][8]byte
		var slots [live]uint8
		for index := range records {
			binary.BigEndian.PutUint64(
				keys[index][:], uint64(leaf)<<48|uint64(index),
			)
			records[index] = AdaptiveOrderedLeafLabRecord{
				Key: keys[index][:], Value: []byte("12345678"),
			}
		}
		if err := PlaceAdaptiveOrderedLeafLabRecords(
			AdaptiveOrderedLeafLabNarrow, seed, records,
		); err != nil {
			t.Fatalf("leaf %d initial placement: %v", leaf, err)
		}
		for index := range records {
			slots[index] = records[index].Slot
		}
		buffers := [2][]byte{
			make([]byte, AdaptiveOrderedLeafLabWideBytes),
			make([]byte, AdaptiveOrderedLeafLabWideBytes),
		}
		page, err := EncodeAdaptiveOrderedLeafLab(
			buffers[0], AdaptiveOrderedLeafLabNarrow,
			adaptiveOrderedLeafLabTestHeader(1), seed, records,
		)
		if err != nil {
			t.Fatalf("leaf %d initial encode: %v", leaf, err)
		}
		view, err := OpenAdaptiveOrderedLeafLab(page, seed)
		if err != nil {
			t.Fatalf("leaf %d initial open: %v", leaf, err)
		}
		current := 0
		generation := uint64(2)
		promotionAt := 0
		random := rand.New(rand.NewSource(int64(0x7000 + leaf)))
		for step := 1; step <= steps; step++ {
			target := random.Intn(live)
			other := 1 - current
			deletedPage, deleteErr := view.DeleteTo(
				buffers[other], generation, slots[target], keys[target][:],
			)
			if deleteErr != nil {
				t.Fatalf("leaf %d step %d delete: %v", leaf, step, deleteErr)
			}
			generation++
			deleted, openErr := OpenAdaptiveOrderedLeafLab(deletedPage, seed)
			if openErr != nil {
				t.Fatalf("leaf %d step %d deleted open: %v", leaf, step, openErr)
			}
			structural := float64(
				deleted.layout.heapStart+AdaptiveOrderedLeafLabTrailerBytes,
			) / float64(deleted.Len())
			maxStructural = max(maxStructural, structural)
			maxPhysical = max(
				maxPhysical,
				float64(len(deleted.page))/float64(deleted.Len()),
			)
			if structural > 5 {
				t.Fatalf("leaf %d step %d delete structural %.4f B/live > 5",
					leaf, step, structural)
			}
			if len(deleted.page) != AdaptiveOrderedLeafLabNarrowBytes {
				t.Fatalf("leaf %d step %d delete extent = %d, want 4096",
					leaf, step, len(deleted.page))
			}

			binary.BigEndian.PutUint64(
				keys[target][:],
				uint64(leaf)<<48|uint64(step+live)<<8|uint64(target),
			)
			replacedPage, slot, insertErr := deleted.InsertTo(
				buffers[current], generation,
				keys[target][:], []byte("12345678"), false,
			)
			if errors.Is(insertErr, ErrAdaptiveOrderedLeafLabNeedsWide) {
				promotedPage, promoteErr := PromoteAdaptiveOrderedLeafLab(
					buffers[current], generation, &deleted,
				)
				if promoteErr != nil {
					t.Fatalf("leaf %d step %d promote: %v", leaf, step, promoteErr)
				}
				generation++
				wide, wideErr := OpenAdaptiveOrderedLeafLab(promotedPage, seed)
				if wideErr != nil {
					t.Fatalf("leaf %d step %d promoted open: %v", leaf, step, wideErr)
				}
				replacedPage, slot, insertErr = wide.InsertTo(
					buffers[other], generation,
					keys[target][:], []byte("12345678"), false,
				)
				current = other
				if promotionAt == 0 {
					promotionAt = step
				}
			}
			if insertErr != nil {
				t.Fatalf("leaf %d step %d insert: %v", leaf, step, insertErr)
			}
			generation++
			slots[target] = slot
			view, err = OpenAdaptiveOrderedLeafLab(replacedPage, seed)
			if err != nil {
				t.Fatalf("leaf %d step %d replacement open: %v", leaf, step, err)
			}
			structural = float64(
				view.layout.heapStart+AdaptiveOrderedLeafLabTrailerBytes,
			) / float64(view.Len())
			maxStructural = max(maxStructural, structural)
			maxPhysical = max(
				maxPhysical,
				float64(len(view.page))/float64(view.Len()),
			)
			if structural > 5 {
				t.Fatalf("leaf %d step %d structural %.4f B/live > 5",
					leaf, step, structural)
			}
			if len(view.page) != AdaptiveOrderedLeafLabNarrowBytes {
				t.Fatalf("leaf %d step %d extent = %d, want 4096",
					leaf, step, len(view.page))
			}
		}
		if promotionAt != 0 {
			promoted++
			promotionStepTotal += promotionAt
			minPromotionStep = min(minPromotionStep, promotionAt)
			maxPromotionStep = max(maxPromotionStep, promotionAt)
		}
		stashTotal += view.StashLen()
		maxStash = max(maxStash, view.StashLen())
		for index := 0; index < live; index += 31 {
			slot, _, _, ok := view.Lookup(keys[index][:])
			if !ok || slot != slots[index] {
				t.Fatalf("leaf %d final key %d = slot %d/%v, want %d",
					leaf, index, slot, ok, slots[index])
			}
		}
	}
	meanPromotion := 0.0
	if promoted != 0 {
		meanPromotion = float64(promotionStepTotal) / float64(promoted)
	}
	t.Logf(
		"steady churn: promoted=%d/%d promotion_step min/mean/max=%d/%.1f/%d "+
			"final_stash mean/max=%.1f/%d structural_max=%.4f physical_max=%.4f",
		promoted, leaves, minPromotionStep, meanPromotion, maxPromotionStep,
		float64(stashTotal)/leaves, maxStash, maxStructural, maxPhysical,
	)
}

func TestAdaptiveOrderedLeafLabRandomizedDifferential(t *testing.T) {
	random := rand.New(rand.NewSource(0x5eed))
	for iteration := 0; iteration < 100; iteration++ {
		class := AdaptiveOrderedLeafLabNarrow
		limit := AdaptiveOrderedLeafLabNarrowLiveTarget
		if iteration&1 != 0 {
			class = AdaptiveOrderedLeafLabWide
			limit = AdaptiveOrderedLeafLabWideSlots
		}
		count := 1 + random.Intn(limit)
		keys := make([]uint32, 0, count)
		seen := make(map[uint32]struct{}, count)
		for len(keys) < count {
			key := random.Uint32()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		records := make([]AdaptiveOrderedLeafLabRecord, count)
		for index, key := range keys {
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], uint64(key))
			records[index] = AdaptiveOrderedLeafLabRecord{
				Key:   append([]byte(nil), encoded[:]...),
				Value: []byte{byte(index), byte(index >> 8)},
			}
		}
		if err := PlaceAdaptiveOrderedLeafLabRecords(
			class, adaptiveOrderedLeafLabTestSeed, records,
		); err != nil {
			t.Fatalf("iteration %d placement: %v", iteration, err)
		}
		view := openAdaptiveOrderedLeafLabTest(t, class, records)
		iterator := view.AllRows()
		for index, record := range records {
			key, value, overflow, ok := iterator.NextBorrowed()
			if !ok || overflow || !bytes.Equal(key, record.Key) ||
				!bytes.Equal(value, record.Value) {
				t.Fatalf("iteration %d row %d differs", iteration, index)
			}
			slot, got, _, found := view.Lookup(record.Key)
			if !found || slot != record.Slot || !bytes.Equal(got, record.Value) {
				t.Fatalf("iteration %d lookup %d differs", iteration, index)
			}
		}
		if _, _, _, ok := iterator.NextBorrowed(); ok {
			t.Fatalf("iteration %d iterator overrun", iteration)
		}
	}
}

func FuzzAdaptiveOrderedLeafLabRoundTrip(f *testing.F) {
	f.Add([]byte("adaptive ordered primary leaf"))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) == 0 {
			return
		}
		class := AdaptiveOrderedLeafLabNarrow
		count := 1 + len(input)%64
		if input[0]&1 != 0 {
			class = AdaptiveOrderedLeafLabWide
			count = 1 + len(input)%AdaptiveOrderedLeafLabWideSlots
		}
		records := make([]AdaptiveOrderedLeafLabRecord, count)
		for index := range records {
			records[index] = AdaptiveOrderedLeafLabRecord{
				Key:   []byte(fmt.Sprintf("%03d-%02x", index, input[index%len(input)])),
				Value: []byte{input[(index*7)%len(input)]},
			}
		}
		if err := PlaceAdaptiveOrderedLeafLabRecords(
			class,
			adaptiveOrderedLeafLabTestSeed,
			records,
		); err != nil {
			t.Fatal(err)
		}
		page := encodeAdaptiveOrderedLeafLabTest(
			t, class, records,
		)
		view, err := OpenAdaptiveOrderedLeafLab(
			page, adaptiveOrderedLeafLabTestSeed,
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			slot, value, _, ok := view.Lookup(record.Key)
			if !ok || slot != record.Slot || !bytes.Equal(value, record.Value) {
				t.Fatalf("round trip key %q", record.Key)
			}
		}
		corrupt := append([]byte(nil), page...)
		at := int(input[0]) % (len(corrupt) - AdaptiveOrderedLeafLabTrailerBytes)
		corrupt[at] ^= 1
		if _, err := OpenAdaptiveOrderedLeafLab(
			corrupt, adaptiveOrderedLeafLabTestSeed,
		); !errors.Is(err, ErrAdaptiveOrderedLeafLabCorrupt) {
			t.Fatalf("corruption admitted: %v", err)
		}
	})
}
