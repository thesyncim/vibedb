package storeio

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"
)

var hashBucketLabTestSeed = [16]byte{
	0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
	0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
}

func hashBucketLabTestHeader() HashBucketLabHeader {
	return HashBucketLabHeader{
		BucketID:   0x1020304050607080,
		Generation: 7,
		Seed:       hashBucketLabTestSeed,
	}
}

func hashBucketLabTestRecords() []HashBucketLabRecord {
	records := []HashBucketLabRecord{
		{Key: []byte("last"), Value: []byte(`{"v":3}`)},
		{Key: []byte("first"), Value: []byte(`{"v":1}`)},
		{Key: []byte("middle"), Value: []byte(`{"v":2}`)},
	}
	if err := PlaceHashBucketLabRecords(hashBucketLabTestSeed, records); err != nil {
		panic(err)
	}
	return records
}

func encodeHashBucketLabTestPage(t testing.TB, records []HashBucketLabRecord) []byte {
	t.Helper()
	page, err := EncodeHashBucketLab(
		make([]byte, HashBucketLabPageSize),
		hashBucketLabTestHeader(),
		records,
	)
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func TestHashBucketLabDescriptorFitsExactlyFourBytes(t *testing.T) {
	if HashBucketLabDescriptorSize != 4 ||
		14+8+6+4 != 32 ||
		hashBucketLabOffsetMask|hashBucketLabKeyMask|hashBucketLabTagMask|
			hashBucketLabLiveFlag|hashBucketLabOverflowFlag|hashBucketLabReservedMask != math.MaxUint32 {
		t.Fatal("hash-bucket lab descriptor does not cover exactly 32 disjoint bits")
	}
	if HashBucketLabHeapStart != 1120 || HashBucketLabHeapStart > HashBucketLabMaxOffset {
		t.Fatalf("heap start = %d, max offset = %d", HashBucketLabHeapStart, HashBucketLabMaxOffset)
	}
	if got := HashBucketLabMetadataBytesPerLiveKey(244); got > 5 {
		t.Fatalf("95%% occupancy metadata = %.3f B/live key, want <= 5", got)
	}
}

func TestHashBucketLabDeterministicEncodeExactLookupAndCanonicalIteration(t *testing.T) {
	records := hashBucketLabTestRecords()
	first := encodeHashBucketLabTestPage(t, records)
	reversed := []HashBucketLabRecord{records[2], records[1], records[0]}
	second := encodeHashBucketLabTestPage(t, reversed)
	if !bytes.Equal(first, second) {
		t.Fatal("input order changed canonical bucket image")
	}

	view, err := OpenHashBucketLab(first)
	if err != nil {
		t.Fatal(err)
	}
	if view.Header() != hashBucketLabTestHeader() || view.Len() != len(records) {
		t.Fatalf("opened header/len = (%+v,%d)", view.Header(), view.Len())
	}
	for _, record := range records {
		slot, value, overflow, ok := view.Lookup(record.Key)
		if !ok || slot != record.Slot || overflow != record.Overflow ||
			!bytes.Equal(value, record.Value) {
			t.Fatalf("Lookup(%q) = (%d,%q,%v,%v)", record.Key, slot, value, overflow, ok)
		}
	}
	if _, _, _, ok := view.Lookup([]byte("absent")); ok {
		t.Fatal("absent key returned a hit")
	}

	iterator := view.AllRows()
	wantSlots := []uint8{records[0].Slot, records[1].Slot, records[2].Slot}
	sort.Slice(wantSlots, func(i, j int) bool { return wantSlots[i] < wantSlots[j] })
	for index, wantSlot := range wantSlots {
		row, ok := iterator.Next()
		if !ok || row.BucketID != hashBucketLabTestHeader().BucketID ||
			row.Slot != wantSlot {
			t.Fatalf("row %d = (%+v,%v), want slot %d", index, row, ok, wantSlot)
		}
	}
	if _, ok := iterator.Next(); ok {
		t.Fatal("iterator returned a fourth row")
	}
}

func TestHashBucketLabFalseTagMatchRequiresExactKey(t *testing.T) {
	target := []byte("target")
	targetHash := KeyHashBytes(hashBucketLabTestSeed, target)
	var collision []byte
	targetFirst, _ := hashBucketLabCandidateGroups(targetHash)
	for candidate := 0; candidate < 100000; candidate++ {
		key := []byte(fmt.Sprintf("x%05d", candidate))
		hash := KeyHashBytes(hashBucketLabTestSeed, key)
		first, second := hashBucketLabCandidateGroups(hash)
		if hash>>58 == targetHash>>58 &&
			(first == targetFirst || second == targetFirst) {
			collision = key
			break
		}
	}
	if collision == nil {
		t.Fatal("failed to construct six-bit tag collision")
	}
	collisionSlot := hashBucketLabCandidate(targetHash, 0)
	targetSlot := hashBucketLabCandidate(targetHash, 1)
	records := []HashBucketLabRecord{
		{Slot: collisionSlot, Key: collision, Value: []byte("wrong")},
		{Slot: targetSlot, Key: target, Value: []byte("right")},
	}
	page := encodeHashBucketLabTestPage(t, records)
	view, err := OpenHashBucketLab(page)
	if err != nil {
		t.Fatal(err)
	}
	slot, value, _, ok := view.LookupHashed(targetHash, target)
	if !ok || slot != targetSlot || string(value) != "right" {
		t.Fatalf("exact lookup = (%d,%q,%v)", slot, value, ok)
	}
}

func TestHashBucketLabTwoCandidateGroupPlacementAt95Percent(t *testing.T) {
	records := make([]HashBucketLabRecord, 244)
	for index := range records {
		records[index] = HashBucketLabRecord{
			Key:   []byte(fmt.Sprintf("placed-%03d", index)),
			Value: []byte("v"),
		}
	}
	if err := PlaceHashBucketLabRecords(hashBucketLabTestSeed, records); err != nil {
		t.Fatal(err)
	}
	var occupied [HashBucketLabSlotCount]bool
	for index := range records {
		hash := KeyHashBytes(hashBucketLabTestSeed, records[index].Key)
		if occupied[records[index].Slot] ||
			!hashBucketLabSlotIsCandidate(hash, records[index].Slot) {
			t.Fatalf("record %d has duplicate/out-of-group slot %d", index, records[index].Slot)
		}
		occupied[records[index].Slot] = true
		first, second := hashBucketLabCandidateGroups(hash)
		if first == second {
			t.Fatalf("record %d has duplicate groups %#x", index, first)
		}
		var candidates [HashBucketLabSlotCount]bool
		for ordinal := 0; ordinal < 32; ordinal++ {
			candidates[hashBucketLabCandidate(hash, ordinal)] = true
		}
		distinct := 0
		for _, present := range candidates {
			if present {
				distinct++
			}
		}
		if distinct != 32 {
			t.Fatalf("record %d candidates = %d, want 32", index, distinct)
		}
	}
	page := encodeHashBucketLabTestPage(t, records)
	view, err := OpenHashBucketLab(page)
	if err != nil || view.Len() != len(records) {
		t.Fatalf("95%% bucket open = (%d,%v)", view.Len(), err)
	}
}

func TestHashBucketLabEncodeRejectsSlotOutsideCandidateGroups(t *testing.T) {
	record := HashBucketLabRecord{
		Key: []byte("misplaced"), Value: []byte("value"),
	}
	hash := KeyHashBytes(hashBucketLabTestSeed, record.Key)
	first, second := hashBucketLabCandidateGroups(hash)
	for group := uint8(0); ; group += 0x10 {
		if group != first && group != second {
			record.Slot = group
			break
		}
	}
	if _, err := EncodeHashBucketLab(
		make([]byte, HashBucketLabPageSize),
		hashBucketLabTestHeader(),
		[]HashBucketLabRecord{record},
	); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("misplaced record encode = %v", err)
	}
}

func TestHashBucketLabOpenRejectsResealedSlotOutsideCandidateGroups(t *testing.T) {
	records := []HashBucketLabRecord{{Key: []byte("misplaced"), Value: []byte("value")}}
	if err := PlaceHashBucketLabRecords(hashBucketLabTestSeed, records); err != nil {
		t.Fatal(err)
	}
	page := encodeHashBucketLabTestPage(t, records)
	oldSlot := records[0].Slot
	hash := KeyHashBytes(hashBucketLabTestSeed, records[0].Key)
	first, second := hashBucketLabCandidateGroups(hash)
	badSlot := uint8(0)
	for group := uint8(0); ; group += 0x10 {
		if group != first && group != second {
			badSlot = group
			break
		}
	}
	descriptor := hashBucketLabDescriptor(page, oldSlot)
	putHashBucketLabDescriptor(page, oldSlot, 0)
	page[hashBucketLabBitmapStart+int(oldSlot)/8] &^=
		byte(1) << uint(oldSlot&7)
	putHashBucketLabDescriptor(page, badSlot, descriptor)
	page[hashBucketLabBitmapStart+int(badSlot)/8] |=
		byte(1) << uint(badSlot&7)
	sealHashBucketLab(page)
	if _, err := OpenHashBucketLab(page); !errors.Is(err, ErrHashBucketLabCorrupt) {
		t.Fatalf("resealed misplaced record open = %v", err)
	}
}

func TestHashBucketLabPageLocalUpdateIsCanonicalAndKeepsSlots(t *testing.T) {
	records := hashBucketLabTestRecords()
	page := encodeHashBucketLabTestPage(t, records)
	view, err := OpenHashBucketLab(page)
	if err != nil {
		t.Fatal(err)
	}
	target := &records[2]
	replacement := []byte(`{"v":222222222222222222222222}`)
	if err := view.Update(target.Slot, target.Key, replacement, false); err != nil {
		t.Fatal(err)
	}
	target.Value = replacement
	want := encodeHashBucketLabTestPage(t, records)
	if !bytes.Equal(page, want) {
		t.Fatal("page-local growing update differs from canonical re-encode")
	}
	if _, err := OpenHashBucketLab(page); err != nil {
		t.Fatal(err)
	}

	replacement = []byte("x")
	if err := view.Update(target.Slot, target.Key, replacement, false); err != nil {
		t.Fatal(err)
	}
	target.Value = replacement
	want = encodeHashBucketLabTestPage(t, records)
	if !bytes.Equal(page, want) {
		t.Fatal("page-local shrinking update differs from canonical re-encode")
	}
	if err := view.Update(target.Slot, []byte("not-middle"), replacement, false); !errors.Is(err, ErrHashBucketLabKeyNotFound) {
		t.Fatalf("wrong exact key update = %v", err)
	}
}

func TestHashBucketLabDeleteIsCanonicalTombstoneFreeAndKeepsOtherSlots(t *testing.T) {
	records := hashBucketLabTestRecords()
	page := encodeHashBucketLabTestPage(t, records)
	view, err := OpenHashBucketLab(page)
	if err != nil {
		t.Fatal(err)
	}
	target := &records[2]
	if err := view.Delete(target.Slot, target.Key); err != nil {
		t.Fatal(err)
	}
	wantRecords := []HashBucketLabRecord{records[0], records[1]}
	want := encodeHashBucketLabTestPage(t, wantRecords)
	if !bytes.Equal(page, want) {
		t.Fatal("page-local delete differs from tombstone-free canonical re-encode")
	}
	if hashBucketLabDescriptor(page, target.Slot) != 0 ||
		page[hashBucketLabBitmapStart+int(target.Slot)/8]&(byte(1)<<uint(target.Slot&7)) != 0 {
		t.Fatal("delete retained descriptor or live bit")
	}
	if _, _, _, ok := view.Lookup([]byte("middle")); ok {
		t.Fatal("deleted key remains visible")
	}
	for _, record := range wantRecords {
		slot, value, _, ok := view.Lookup(record.Key)
		if !ok || slot != record.Slot || !bytes.Equal(value, record.Value) {
			t.Fatalf("surviving key %q moved or changed", record.Key)
		}
	}
	if _, err := OpenHashBucketLab(page); err != nil {
		t.Fatal(err)
	}
}

func TestHashBucketLabOverflowAndFallbackSignals(t *testing.T) {
	overflow := bytes.Repeat([]byte{0xa5}, HashBucketLabOverflowReferenceSize)
	records := []HashBucketLabRecord{
		{Key: []byte("large"), Value: overflow, Overflow: true},
	}
	if err := PlaceHashBucketLabRecords(hashBucketLabTestSeed, records); err != nil {
		t.Fatal(err)
	}
	page := encodeHashBucketLabTestPage(t, records)
	view, err := OpenHashBucketLab(page)
	if err != nil {
		t.Fatal(err)
	}
	_, value, isOverflow, ok := view.Lookup([]byte("large"))
	if !ok || !isOverflow || !bytes.Equal(value, overflow) {
		t.Fatalf("overflow lookup = (%x,%v,%v)", value, isOverflow, ok)
	}

	wideKey := bytes.Repeat([]byte("k"), HashBucketLabMaxKeyLength+1)
	if _, err := EncodeHashBucketLab(
		make([]byte, HashBucketLabPageSize),
		hashBucketLabTestHeader(),
		[]HashBucketLabRecord{{Key: wideKey, Value: []byte("v")}},
	); !errors.Is(err, ErrHashBucketLabNeedsWide) {
		t.Fatalf("wide key encode = %v", err)
	}
	largeValue := bytes.Repeat([]byte("v"), HashBucketLabPageSize)
	if _, err := EncodeHashBucketLab(
		make([]byte, HashBucketLabPageSize),
		hashBucketLabTestHeader(),
		[]HashBucketLabRecord{{Key: []byte("k"), Value: largeValue}},
	); !errors.Is(err, ErrHashBucketLabNeedsOverflow) {
		t.Fatalf("large inline value encode = %v", err)
	}
}

func TestHashBucketLabRejectsChecksumAndResealedStructureCorruption(t *testing.T) {
	records := hashBucketLabTestRecords()
	original := encodeHashBucketLabTestPage(t, records)
	targetSlot := records[1].Slot
	tests := []struct {
		name   string
		mutate func([]byte)
		reseal bool
	}{
		{
			name: "checksum",
			mutate: func(page []byte) {
				page[HashBucketLabHeapStart] ^= 0x80
			},
		},
		{
			name: "bitmap descriptor mismatch",
			mutate: func(page []byte) {
				page[hashBucketLabBitmapStart+int(targetSlot)/8] &^=
					byte(1) << uint(targetSlot&7)
			},
			reseal: true,
		},
		{
			name: "keyed tag",
			mutate: func(page []byte) {
				descriptor := hashBucketLabDescriptor(page, targetSlot)
				descriptor ^= uint32(1) << 22
				putHashBucketLabDescriptor(page, targetSlot, descriptor)
			},
			reseal: true,
		},
		{
			name: "non-canonical first offset",
			mutate: func(page []byte) {
				firstSlot := records[0].Slot
				for _, record := range records[1:] {
					if record.Slot < firstSlot {
						firstSlot = record.Slot
					}
				}
				descriptor := hashBucketLabDescriptor(page, firstSlot)
				descriptor = descriptor&^hashBucketLabOffsetMask |
					uint32(HashBucketLabHeapStart+1)
				putHashBucketLabDescriptor(page, firstSlot, descriptor)
			},
			reseal: true,
		},
		{
			name: "padding",
			mutate: func(page []byte) {
				heapEnd := binaryLittleEndianUint16(page[22:24])
				page[heapEnd] = 1
			},
			reseal: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			page := append([]byte(nil), original...)
			test.mutate(page)
			if test.reseal {
				sealHashBucketLab(page)
			}
			if _, err := OpenHashBucketLab(page); !errors.Is(err, ErrHashBucketLabCorrupt) {
				t.Fatalf("OpenHashBucketLab = %v", err)
			}
		})
	}
}

func TestHashBucketLabWarmOperationsAllocateNothing(t *testing.T) {
	records := hashBucketLabTestRecords()
	page := encodeHashBucketLabTestPage(t, records)
	view, err := OpenHashBucketLab(page)
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		_, hashBucketLabBenchmarkBytes, _, _ = view.Lookup([]byte("middle"))
	}); allocs != 0 {
		t.Fatalf("Lookup allocations = %g, want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		iterator := view.AllRows()
		for {
			row, ok := iterator.Next()
			if !ok {
				break
			}
			hashBucketLabBenchmarkSlot = row.Slot
			hashBucketLabBenchmarkBytes = row.Value
		}
	}); allocs != 0 {
		t.Fatalf("iteration allocations = %g, want 0", allocs)
	}

	short := []byte("1")
	long := []byte("123456789")
	toggle := false
	target := &records[2]
	if allocs := testing.AllocsPerRun(1000, func() {
		value := short
		if toggle {
			value = long
		}
		toggle = !toggle
		if err := view.Update(target.Slot, target.Key, value, false); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("Update allocations = %g, want 0", allocs)
	}

	before := encodeHashBucketLabTestPage(t, records)
	work := make([]byte, HashBucketLabPageSize)
	if allocs := testing.AllocsPerRun(1000, func() {
		copy(work, before)
		candidate, err := OpenHashBucketLab(work)
		if err != nil {
			panic(err)
		}
		if err := candidate.Delete(target.Slot, target.Key); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("Delete allocations = %g, want 0", allocs)
	}
}

func FuzzHashBucketLabOpen(f *testing.F) {
	records := hashBucketLabTestRecords()
	original, err := EncodeHashBucketLab(
		make([]byte, HashBucketLabPageSize),
		hashBucketLabTestHeader(),
		records,
	)
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte(nil))
	f.Add([]byte{0, 1, 63, 0xff, 96, 7, 255, 3})
	f.Fuzz(func(t *testing.T, damage []byte) {
		page := append([]byte(nil), original...)
		for index := 0; index+1 < len(damage); index += 2 {
			offset := int(damage[index])<<8 | int(damage[index+1])
			offset %= hashBucketLabTrailerStart
			page[offset] ^= byte(offset) | 1
		}
		sealHashBucketLab(page)
		view, openErr := OpenHashBucketLab(page)
		if openErr != nil {
			return
		}
		seen := 0
		iterator := view.AllRows()
		for {
			row, ok := iterator.Next()
			if !ok {
				break
			}
			seen++
			slot, value, overflow, found := view.Lookup(row.Key)
			if !found || slot != row.Slot || overflow != row.Overflow ||
				!bytes.Equal(value, row.Value) {
				t.Fatalf("admitted row is not exactly lookup-addressable: %+v", row)
			}
		}
		if seen != view.Len() {
			t.Fatalf("iterator count = %d, view len = %d", seen, view.Len())
		}
	})
}

// Avoid importing encoding/binary solely for one deliberately corrupted
// padding offset while keeping the corruption site readable.
func binaryLittleEndianUint16(src []byte) uint16 {
	return uint16(src[0]) | uint16(src[1])<<8
}

var (
	hashBucketLabBenchmarkBytes []byte
	hashBucketLabBenchmarkSlot  uint8
)
