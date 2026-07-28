package storeio

import (
	"math/rand"
	"testing"
)

func TestFreeExtentIndexCapacityBoundaries(t *testing.T) {
	tests := []struct {
		extents int
		maxima  int
	}{
		{extents: -1, maxima: 0},
		{extents: 0, maxima: 0},
		{extents: 1, maxima: 1},
		{extents: 63, maxima: 1},
		{extents: 64, maxima: 1},
		{extents: 65, maxima: 3},
		{extents: 4_096, maxima: 65},
		{extents: 46_200, maxima: 735},
	}
	for _, test := range tests {
		if got := FreeExtentIndexCapacity(test.extents); got != test.maxima {
			t.Errorf("FreeExtentIndexCapacity(%d) = %d, want %d", test.extents, got, test.maxima)
		}
	}
}

func TestFreeExtentIndexFirstFitBoundaries(t *testing.T) {
	for _, count := range []int{1, 63, 64, 65, 46_200} {
		t.Run(testCountName(count), func(t *testing.T) {
			extents := freeExtentIndexTestExtents(count)
			for i := range extents {
				extents[i].Length = 4 << 10
			}
			extents[count-1].Length = 64 << 10

			var index FreeExtentIndex
			storage := make([]uint64, FreeExtentIndexCapacity(count))
			if !index.Rebuild(extents, storage) {
				t.Fatal("Rebuild failed")
			}
			if got, ok := index.FirstFit(extents, 4<<10); !ok || got != 0 {
				t.Fatalf("4 KiB FirstFit = %d, %v, want 0, true", got, ok)
			}
			if got, ok := index.FirstFit(extents, 64<<10); !ok || got != count-1 {
				t.Fatalf("64 KiB FirstFit = %d, %v, want %d, true", got, ok, count-1)
			}
			if got, ok := index.FirstFit(extents, 128<<10); ok {
				t.Fatalf("128 KiB FirstFit = %d, true, want no match", got)
			}
			if index.Len() != count {
				t.Fatalf("Len = %d, want %d", index.Len(), count)
			}
			if index.StorageBytes() != len(storage)*8 {
				t.Fatalf("StorageBytes = %d, want %d", index.StorageBytes(), len(storage)*8)
			}
		})
	}
}

func TestFreeExtentIndexUpdatePreservesLowestOffsetFirstFit(t *testing.T) {
	extents := freeExtentIndexTestExtents(130)
	for i := range extents {
		extents[i].Length = 4 << 10
	}
	extents[64].Length = 64 << 10
	extents[129].Length = 64 << 10

	var index FreeExtentIndex
	storage := make([]uint64, FreeExtentIndexCapacity(len(extents)))
	if !index.Rebuild(extents, storage) {
		t.Fatal("Rebuild failed")
	}
	assertFreeExtentFirstFit(t, &index, extents, 64<<10, 64, true)

	extents[64].Length = 0
	if !index.Update(extents, 64) {
		t.Fatal("Update(64) failed")
	}
	assertFreeExtentFirstFit(t, &index, extents, 64<<10, 129, true)

	extents[17].Length = 64 << 10
	if !index.Update(extents, 17) {
		t.Fatal("Update(17) failed")
	}
	assertFreeExtentFirstFit(t, &index, extents, 64<<10, 17, true)

	extents[17].Length = 0
	extents[129].Length = 0
	if !index.Update(extents, 17) || !index.Update(extents, 129) {
		t.Fatal("clearing Update failed")
	}
	assertFreeExtentFirstFit(t, &index, extents, 64<<10, 0, false)
}

func TestFreeExtentIndexRandomizedDifferential(t *testing.T) {
	random := rand.New(rand.NewSource(0x7a11_0ca7))
	for _, count := range []int{1, 2, 63, 64, 65, 255, 256, 4_096, 46_200} {
		extents := freeExtentIndexTestExtents(count)
		for i := range extents {
			extents[i].Length = uint64(random.Intn(33)) * (4 << 10)
		}
		storage := make([]uint64, FreeExtentIndexCapacity(count))
		var index FreeExtentIndex
		if !index.Rebuild(extents, storage) {
			t.Fatalf("count %d: Rebuild failed", count)
		}

		iterations := 2_000
		if count == 46_200 {
			iterations = 10_000
		}
		for iteration := 0; iteration < iterations; iteration++ {
			want := uint64(random.Intn(40)+1) * (4 << 10)
			got, gotOK := index.FirstFit(extents, want)
			wantIndex, wantOK := linearFreeExtentFirstFit(extents, want)
			if got != wantIndex || gotOK != wantOK {
				t.Fatalf(
					"count %d iteration %d want %d: indexed = %d, %v; linear = %d, %v",
					count, iteration, want, got, gotOK, wantIndex, wantOK,
				)
			}

			changed := random.Intn(count)
			extents[changed].Length = uint64(random.Intn(33)) * (4 << 10)
			if !index.Update(extents, changed) {
				t.Fatalf("count %d iteration %d: Update(%d) failed", count, iteration, changed)
			}
		}
	}
}

func TestFreeExtentIndexNearestFitBoundaries(t *testing.T) {
	// Offsets are (i+4)*4KiB, so extent i sits at offset (i+4)*4096.
	extents := freeExtentIndexTestExtents(130)
	for i := range extents {
		extents[i].Length = 4 << 10
	}
	// Only two extents can hold a 64 KiB request: one low, one high.
	extents[20].Length = 64 << 10
	extents[100].Length = 64 << 10

	var index FreeExtentIndex
	storage := make([]uint64, FreeExtentIndexCapacity(len(extents)))
	if !index.Rebuild(extents, storage) {
		t.Fatal("Rebuild failed")
	}

	offsetOf := func(i int) uint64 { return uint64(i+4) * (4 << 10) }

	// A 4 KiB request finds the extent whose offset is nearest the hint, on
	// either side, exactly at a boundary, and clamped past both ends.
	cases := []struct {
		hint uint64
		want uint64
		idx  int
		ok   bool
	}{
		{hint: 0, want: 4 << 10, idx: 0, ok: true},                           // below all
		{hint: offsetOf(129) + (1 << 20), want: 4 << 10, idx: 129, ok: true}, // above all
		{hint: offsetOf(50), want: 4 << 10, idx: 50, ok: true},               // exact hit
		{hint: offsetOf(50) + (1 << 10), want: 4 << 10, idx: 50, ok: true},   // just above 50
		{hint: offsetOf(50) - (1 << 10), want: 4 << 10, idx: 50, ok: true},   // just below 50
		// Exactly midway between offsets 50 and 51: tie favours the lower offset.
		{hint: offsetOf(50) + (2 << 10), want: 4 << 10, idx: 50, ok: true},
		// A 64 KiB request only fits at 20 or 100; the nearer wins, ties low.
		{hint: offsetOf(0), want: 64 << 10, idx: 20, ok: true},
		{hint: offsetOf(129), want: 64 << 10, idx: 100, ok: true},
		{hint: offsetOf(60), want: 64 << 10, idx: 20, ok: true},  // 40 vs 40 -> lower
		{hint: offsetOf(61), want: 64 << 10, idx: 100, ok: true}, // 41 vs 39 -> high
		{hint: offsetOf(60), want: 128 << 10, idx: 0, ok: false}, // nothing fits
	}
	for _, c := range cases {
		got, ok := index.NearestFit(extents, c.want, c.hint)
		if got != c.idx || ok != c.ok {
			t.Fatalf(
				"NearestFit(want=%d hint=%d) = %d, %v; want %d, %v",
				c.want, c.hint, got, ok, c.idx, c.ok,
			)
		}
	}

	// A zero-length request, a resized slice, and an unbuilt index all miss.
	if _, ok := index.NearestFit(extents, 0, offsetOf(50)); ok {
		t.Fatal("zero-length NearestFit succeeded")
	}
	if _, ok := index.NearestFit(extents[:64], 4<<10, offsetOf(10)); ok {
		t.Fatal("resized NearestFit succeeded")
	}
	var empty FreeExtentIndex
	if _, ok := empty.NearestFit(extents, 4<<10, offsetOf(10)); ok {
		t.Fatal("unbuilt NearestFit succeeded")
	}
}

func TestFreeExtentIndexNearestFitSingleGroup(t *testing.T) {
	// A single leaf group (<= fan-out extents) exercises the direct-scan paths
	// with no hierarchy ascent on either side.
	for _, count := range []int{1, 2, 63, 64} {
		t.Run(testCountName(count), func(t *testing.T) {
			extents := freeExtentIndexTestExtents(count)
			for i := range extents {
				extents[i].Length = 4 << 10
			}
			var index FreeExtentIndex
			storage := make([]uint64, FreeExtentIndexCapacity(count))
			if !index.Rebuild(extents, storage) {
				t.Fatal("Rebuild failed")
			}
			for hintScale := 0; hintScale < count+8; hintScale++ {
				hint := uint64(hintScale) * (4 << 10)
				got, ok := index.NearestFit(extents, 4<<10, hint)
				wantIdx, wantOK := linearFreeExtentNearestFit(extents, 4<<10, hint)
				if got != wantIdx || ok != wantOK {
					t.Fatalf(
						"count %d hint %d: indexed = %d, %v; linear = %d, %v",
						count, hint, got, ok, wantIdx, wantOK,
					)
				}
			}
		})
	}
}

func TestFreeExtentIndexNearestFitRandomizedDifferential(t *testing.T) {
	random := rand.New(rand.NewSource(0x4ea5_e57f))
	for _, count := range []int{1, 2, 63, 64, 65, 255, 256, 4_096, 46_200} {
		extents := freeExtentIndexTestExtents(count)
		for i := range extents {
			extents[i].Length = uint64(random.Intn(33)) * (4 << 10)
		}
		storage := make([]uint64, FreeExtentIndexCapacity(count))
		var index FreeExtentIndex
		if !index.Rebuild(extents, storage) {
			t.Fatalf("count %d: Rebuild failed", count)
		}
		// Hints span below the first offset through above the last so both
		// directional descents, boundary clamping, and exact hits are covered.
		span := uint64(count+8) * (4 << 10)
		iterations := 4_000
		if count == 46_200 {
			iterations = 12_000
		}
		for iteration := 0; iteration < iterations; iteration++ {
			want := uint64(random.Intn(40)+1) * (4 << 10)
			hint := uint64(random.Int63n(int64(span)))
			got, gotOK := index.NearestFit(extents, want, hint)
			wantIdx, wantOK := linearFreeExtentNearestFit(extents, want, hint)
			if got != wantIdx || gotOK != wantOK {
				t.Fatalf(
					"count %d iteration %d want %d hint %d: indexed = %d, %v; linear = %d, %v",
					count, iteration, want, hint, got, gotOK, wantIdx, wantOK,
				)
			}
			changed := random.Intn(count)
			extents[changed].Length = uint64(random.Intn(33)) * (4 << 10)
			if !index.Update(extents, changed) {
				t.Fatalf("count %d iteration %d: Update(%d) failed", count, iteration, changed)
			}
		}
	}
}

func TestFreeExtentIndexNearestFitZeroAllocation(t *testing.T) {
	extents := freeExtentIndexTestExtents(46_200)
	for i := range extents {
		extents[i].Length = uint64(i%32+1) * (4 << 10)
	}
	storage := make([]uint64, FreeExtentIndexCapacity(len(extents)))
	var index FreeExtentIndex
	if !index.Rebuild(extents, storage) {
		t.Fatal("Rebuild failed")
	}
	hint := uint64(20_000) * (4 << 10)
	if allocations := testing.AllocsPerRun(1_000, func() {
		if _, ok := index.NearestFit(extents, 64<<10, hint); !ok {
			panic("NearestFit failed")
		}
	}); allocations != 0 {
		t.Fatalf("NearestFit allocations = %g, want 0", allocations)
	}
}

// linearFreeExtentNearestFit is the O(n) reference: the offset-nearest extent
// that satisfies want, ties resolved toward the lower offset (the first seen in
// ascending order).
func linearFreeExtentNearestFit(extents []FreeExtent, want, hint uint64) (int, bool) {
	best := -1
	var bestDistance uint64
	for i := range extents {
		if extents[i].Length < want {
			continue
		}
		distance := extents[i].Offset - hint
		if extents[i].Offset < hint {
			distance = hint - extents[i].Offset
		}
		if best < 0 || distance < bestDistance {
			best = i
			bestDistance = distance
		}
	}
	if best < 0 {
		return 0, false
	}
	return best, true
}

func TestFreeExtentIndexRejectsInvalidUse(t *testing.T) {
	extents := freeExtentIndexTestExtents(65)
	required := FreeExtentIndexCapacity(len(extents))
	storage := make([]uint64, required)
	var index FreeExtentIndex

	if index.Rebuild(extents, storage[:required-1]) {
		t.Fatal("short-storage Rebuild succeeded")
	}
	if index.Len() != 0 || index.StorageBytes() != 0 {
		t.Fatalf("failed Rebuild retained index: len=%d bytes=%d", index.Len(), index.StorageBytes())
	}
	if _, ok := index.FirstFit(extents, 4<<10); ok {
		t.Fatal("unbuilt FirstFit succeeded")
	}
	if !index.Rebuild(extents, storage) {
		t.Fatal("Rebuild failed")
	}
	if _, ok := index.FirstFit(extents[:64], 4<<10); ok {
		t.Fatal("resized FirstFit succeeded")
	}
	if _, ok := index.FirstFit(extents, 0); ok {
		t.Fatal("zero-length FirstFit succeeded")
	}
	if index.Update(extents[:64], 0) || index.Update(extents, -1) ||
		index.Update(extents, len(extents)) {
		t.Fatal("invalid Update succeeded")
	}
}

func TestFreeExtentIndexZeroAllocationOperations(t *testing.T) {
	extents := freeExtentIndexTestExtents(46_200)
	for i := range extents {
		extents[i].Length = uint64(i%32+1) * (4 << 10)
	}
	storage := make([]uint64, FreeExtentIndexCapacity(len(extents)))
	var index FreeExtentIndex

	if allocations := testing.AllocsPerRun(100, func() {
		if !index.Rebuild(extents, storage) {
			panic("Rebuild failed")
		}
	}); allocations != 0 {
		t.Fatalf("Rebuild allocations = %g, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1_000, func() {
		if _, ok := index.FirstFit(extents, 64<<10); !ok {
			panic("FirstFit failed")
		}
	}); allocations != 0 {
		t.Fatalf("FirstFit allocations = %g, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1_000, func() {
		extents[17].Length ^= 4 << 10
		if !index.Update(extents, 17) {
			panic("Update failed")
		}
	}); allocations != 0 {
		t.Fatalf("Update allocations = %g, want 0", allocations)
	}
}

func assertFreeExtentFirstFit(
	t *testing.T, index *FreeExtentIndex, extents []FreeExtent, want uint64,
	wantIndex int, wantOK bool,
) {
	t.Helper()
	got, gotOK := index.FirstFit(extents, want)
	if got != wantIndex || gotOK != wantOK {
		t.Fatalf("FirstFit(%d) = %d, %v, want %d, %v", want, got, gotOK, wantIndex, wantOK)
	}
}

func linearFreeExtentFirstFit(extents []FreeExtent, want uint64) (int, bool) {
	for i := range extents {
		if extents[i].Length >= want {
			return i, true
		}
	}
	return 0, false
}

func freeExtentIndexTestExtents(count int) []FreeExtent {
	extents := make([]FreeExtent, count)
	for i := range extents {
		extents[i] = FreeExtent{
			Offset:            uint64(i+4) * (4 << 10),
			Length:            4 << 10,
			RetiredGeneration: 1,
		}
	}
	return extents
}

func testCountName(count int) string {
	const digits = "0123456789"
	var buffer [20]byte
	next := len(buffer)
	for {
		next--
		buffer[next] = digits[count%10]
		count /= 10
		if count == 0 {
			return string(buffer[next:])
		}
	}
}
