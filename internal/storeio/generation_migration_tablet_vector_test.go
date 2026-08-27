package storeio

import (
	"os"
	"testing"
)

func TestGenerationMigrationTabletVectorCrosses4096WithBoundedMemory(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "tablet-vector-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	v, err := OpenGenerationMigrationTabletVector(file, 0, 9000, testStoreID, [16]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	makeRef := func(tablet uint32, generation uint64) PageRef {
		logical, ok := GlobalTabletCatalogTabletRootLogicalID(tablet)
		if !ok {
			t.Fatal("tablet id")
		}
		return PageRef{Offset: uint64(tablet+1) * 4096, LogicalID: logical, Generation: generation, Length: SegmentedTabletRouterRootBytes, Kind: PageTabletRoute}
	}
	for _, tablet := range []uint32{0, 4095, 4096, 8999} {
		if err := v.Put(tablet, GenerationMigrationTabletRef{Source: makeRef(tablet, 7), Target: makeRef(tablet, 8)}); err != nil {
			t.Fatal(err)
		}
	}
	visited := 0
	if err := v.Visit(func(_ uint32, _ GenerationMigrationTabletRef) error {
		visited++
		return nil
	}); err != nil || visited != 4 {
		t.Fatalf("visit count=%d err=%v", visited, err)
	}
	if err := v.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, tablet := range []uint32{0, 4095, 4096, 8999} {
		entry, ok, err := v.Get(tablet)
		if err != nil || !ok || entry.Source.Generation != 7 || entry.Target.Generation != 8 {
			t.Fatalf("tablet %d = %+v ok=%v err=%v", tablet, entry, ok, err)
		}
	}
	if got, max := v.PhysicalBytes(), uint64(2*((9000+GenerationMigrationTabletVectorFanout-1)/GenerationMigrationTabletVectorFanout))*4096; got != max {
		t.Fatalf("physical bytes=%d want=%d", got, max)
	}
}

func TestGenerationMigrationTabletVectorTornLatestFallsBack(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "tablet-vector-torn-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	v, err := OpenGenerationMigrationTabletVector(file, 0, 1, testStoreID, [16]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	source := PageRef{Offset: 4096, LogicalID: PrimaryTabletRootLogicalIDBase, Generation: 7, Length: SegmentedTabletRouterRootBytes, Kind: PageTabletRoute}
	if err := v.Put(0, GenerationMigrationTabletRef{Source: source}); err != nil {
		t.Fatal(err)
	}
	target := source
	target.Offset, target.Generation = 8192, 8
	if err := v.Put(0, GenerationMigrationTabletRef{Source: source, Target: target}); err != nil {
		t.Fatal(err)
	}
	// Sequence two is slot zero; corrupting it exposes sequence one.
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		t.Fatal(err)
	}
	entry, ok, err := v.Get(0)
	if err != nil || !ok || entry.Source != source || entry.Target != (PageRef{}) {
		t.Fatalf("fallback = %+v ok=%v err=%v", entry, ok, err)
	}
}

func TestGenerationMigrationTabletVectorWarmGetAllocations(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "tablet-vector-alloc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	v, err := OpenGenerationMigrationTabletVector(file, 0, 1, testStoreID, [16]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	source := PageRef{Offset: 4096, LogicalID: PrimaryTabletRootLogicalIDBase, Generation: 7, Length: SegmentedTabletRouterRootBytes, Kind: PageTabletRoute}
	if err := v.Put(0, GenerationMigrationTabletRef{Source: source}); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if _, ok, err := v.Get(0); err != nil || !ok {
			t.Fatalf("get ok=%v err=%v", ok, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warm Get allocations = %.2f, want zero", allocs)
	}
}
