package storeio

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestUnrootedGenerationWriterResumeAndBounds(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "unrooted-writer-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := UnrootedGenerationReservation{Offset: 64 << 10, Length: 3 * 4096, FirstLogicalID: 100, LogicalIDCount: 3}
	makePage := func(offset uint64, logical uint64) (PageRef, []byte) {
		ref := PageRef{Offset: offset, LogicalID: logical, Generation: 9, Length: 4096, Kind: PageIndexPosting}
		image := make([]byte, 4096)
		payload, err := InitPage(image, PageHeader{StoreID: testStoreID, Generation: 9, LogicalID: logical, PageSize: 4096, PayloadLength: 1, Kind: PageIndexPosting})
		if err != nil {
			t.Fatal(err)
		}
		payload[0] = byte(logical)
		if _, err = SealPage(image); err != nil {
			t.Fatal(err)
		}
		return ref, image
	}
	w, err := NewUnrootedGenerationWriter(f, r, testStoreID, 9, 0)
	if err != nil {
		t.Fatal(err)
	}
	ref0, p0 := makePage(r.Offset, 100)
	if err = w.Append(ref0, p0); err != nil {
		t.Fatal(err)
	}
	if err = w.Sync(); err != nil {
		t.Fatal(err)
	}
	w, err = NewUnrootedGenerationWriter(f, r, testStoreID, 9, 4096)
	if err != nil {
		t.Fatal(err)
	}
	ref1, p1 := makePage(r.Offset+4096, 101)
	if err = w.Append(ref1, p1); err != nil {
		t.Fatal(err)
	}
	if err = w.Append(ref1, p1); err == nil {
		t.Fatal("nonsequential append accepted")
	}
	got := make([]byte, 4096)
	if _, err = f.ReadAt(got, int64(ref0.Offset)); err != nil || !bytes.Equal(got, p0) {
		t.Fatalf("first page = %v equal=%v", err, bytes.Equal(got, p0))
	}
}

func TestUnrootedPrimaryGraphSinkUsesOneBoundedPageBuffer(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "unrooted-sink-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := UnrootedGenerationReservation{Offset: 64 << 10, Length: 128 << 10, FirstLogicalID: 100, LogicalIDCount: 32}
	w, err := NewUnrootedGenerationWriter(f, r, testStoreID, 9, 0)
	if err != nil {
		t.Fatal(err)
	}
	scratch := make([]byte, CommonPrimaryLeafMaxExtentBytes)
	s, err := NewUnrootedPrimaryGraphSink(w, testStoreID, 9, 100, r.Offset+r.Length, scratch)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.AllocatePage(PageIndexPosting, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	if p.Ref().LogicalID != 100 || s.BuildNextLogicalID() != 101 {
		t.Fatalf("dynamic identity = %d/%d", p.Ref().LogicalID, s.BuildNextLogicalID())
	}
	second, err := s.AllocatePage(PageIndexPosting, 4096, 0)
	if err != nil || second.Ref().Offset != p.Ref().Offset+4096 {
		t.Fatalf("bounded second page = %+v,%v", second, err)
	}
	payload, err := InitPage(p.Bytes(), PageHeader{StoreID: testStoreID, Generation: 9, LogicalID: p.Ref().LogicalID, PageSize: 4096, PayloadLength: 1, Kind: PageIndexPosting})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 7
	if _, err = SealPage(p.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err = p.Stage(); err != nil {
		t.Fatal(err)
	}
	payload, err = InitPage(second.Bytes(), PageHeader{StoreID: testStoreID, Generation: 9, LogicalID: second.Ref().LogicalID, PageSize: 4096, PayloadLength: 1, Kind: PageIndexPosting})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 8
	if _, err = SealPage(second.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err = second.Stage(); err != nil {
		t.Fatal(err)
	}
	if &scratch[0] != &p.Bytes()[0] || w.WrittenBytes() != 8192 {
		t.Fatalf("scratch/write = %p/%p %d", &scratch[0], &p.Bytes()[0], w.WrittenBytes())
	}
}

func TestPlannedPrimaryGraphEmitsIntoReservedSink(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "reserved-graph-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	records := make([]PrimaryGraphRecord, 512)
	for i := range records {
		records[i] = BorrowPrimaryGraphRecord([]byte(fmt.Sprintf("k-%04d", i)), []byte(`{"v":1}`))
	}
	plan, err := PlanPrimaryGraph(testStoreID, records, false)
	if err != nil {
		t.Fatal(err)
	}
	r := UnrootedGenerationReservation{Offset: 64 << 10, Length: 16 << 20, FirstLogicalID: PrimaryFirstDynamicLogicalID, LogicalIDCount: 1 << 20}
	w, err := NewUnrootedGenerationWriter(f, r, testStoreID, 9, 0)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewUnrootedPrimaryGraphSink(w, testStoreID, 9, PrimaryFirstDynamicLogicalID, r.Offset+r.Length, make([]byte, 512<<10))
	if err != nil {
		t.Fatal(err)
	}
	root, err := BuildPlannedPrimaryGraphToSink(s, &plan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Sync(); err != nil {
		t.Fatal(err)
	}
	cache, err := NewPageCache(f, PageCacheOptions{PageSize: 4096, MaxPageSize: 64 << 10, ResidentBytes: 2 << 20, StoreID: testStoreID, ReadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	router, err := BuildResidentPrimaryRouter(cache, root, GlobalTabletCatalogBounds{StoreID: testStoreID, SelectedRootGeneration: 9, FileEnd: r.Offset + r.Length, NextLogicalID: s.BuildNextLogicalID()})
	if err != nil {
		t.Fatal(err)
	}
	if router.Len() == 0 {
		t.Fatal("reserved graph has no routes")
	}
}

func TestAbandonedUnrootedGenerationWaitsForSnapshotAndRecoveryFloors(t *testing.T) {
	leases, err := NewGenerationLeases(GenerationLeaseOptions{MaxLeases: 2})
	if err != nil {
		t.Fatal(err)
	}
	reclaimer, err := NewExtentReclaimer(
		leases, ExtentReclaimerOptions{MaxRetiredExtents: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := leases.Acquire(5)
	if err != nil {
		t.Fatal(err)
	}
	reservation := UnrootedGenerationReservation{
		Offset: 64 << 10, Length: 8 << 20,
		FirstLogicalID: 100, LogicalIDCount: 1000,
	}
	if err := RetireAbandonedUnrootedGeneration(reclaimer, reservation, 6); err != nil {
		t.Fatal(err)
	}
	reusable, err := reclaimer.AppendReusable(nil, 7, 7, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(reusable) != 0 {
		t.Fatalf("active snapshot reclaimed abandoned range: %+v", reusable)
	}
	lease.Release()
	reusable = make([]FreeExtent, 0, 1)
	reusable, err = reclaimer.AppendReusable(reusable, 7, 6, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(reusable) != 0 {
		t.Fatalf("fallback root reclaimed abandoned range: %+v", reusable)
	}
	reusable, err = reclaimer.AppendReusable(reusable, 7, 7, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(reusable) != 1 || reusable[0].Offset != reservation.Offset ||
		reusable[0].Length != reservation.Length {
		t.Fatalf("reclaimed abandoned range = %+v", reusable)
	}

	manifest := GenerationMigrationManifest{
		StoreID: testStoreID, MigrationID: [16]byte{9},
		Phase:            GenerationMigrationCatchingUp,
		SourceGeneration: 5, TargetGeneration: 7,
		ReservedOffset: reservation.Offset, ReservedBytes: reservation.Length,
		FirstLogicalID: reservation.FirstLogicalID,
		LogicalIDCount: reservation.LogicalIDCount,
		SourcePrimaryRoot: PageRef{
			Offset: 32 << 10, LogicalID: GlobalTabletCatalogRootLogicalID,
			Generation: 5, Length: GlobalTabletCatalogRootBytes,
			Kind: PagePrimaryCatalog,
		},
	}
	got, ok := AbandonedUnrootedGenerationReservation(manifest)
	if !ok || got != reservation {
		t.Fatalf("manifest reservation = %+v,%v", got, ok)
	}
	manifest.Phase = GenerationMigrationPublished
	if _, ok := AbandonedUnrootedGenerationReservation(manifest); ok {
		t.Fatal("published migration was treated as abandoned")
	}
}
