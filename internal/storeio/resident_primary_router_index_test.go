package storeio

import (
	"fmt"
	"testing"
	"unsafe"
)

func residentBucketTestEntry(t testing.TB, tablet, local uint32) (BucketID, residentRouteEntry) {
	t.Helper()
	bucket, ok := MakeTabletLocalIdentityBucket(tablet, local)
	if !ok {
		t.Fatalf("bucket %d/%d", tablet, local)
	}
	cell := &residentRouteCell{}
	cell.meta.Store(uint64(4096) | uint64(bucket)<<32)
	return BucketID(bucket), residentRouteEntry{fence: []byte(fmt.Sprintf("%06x", bucket)), cell: cell}
}

func TestResidentBucketIndexBulkLookupTabletAndDuplicate(t *testing.T) {
	entries := make([]residentRouteEntry, 0, 5000)
	var ids []BucketID
	for i := range 5000 {
		id, entry := residentBucketTestEntry(t, uint32(i/1000+7), uint32((i*37)%1000))
		ids, entries = append(ids, id), append(entries, entry)
	}
	index, err := residentBucketBuild(entries)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		got, ok := index.lookup(id)
		if !ok || got.cell != entries[i].cell {
			t.Fatalf("lookup %d", id)
		}
	}
	set, ok := index.tabletLocals(9)
	if !ok {
		t.Fatal("tablet 9 absent")
	}
	for i, id := range ids {
		tablet, local, _ := SplitTabletLocalIdentityBucket(uint32(id))
		if tablet == 9 && !set.contains(uint32(local)) {
			t.Fatalf("local %d absent at %d", local, i)
		}
	}
	if _, err := residentBucketBuild(append(entries, entries[0])); err == nil {
		t.Fatal("duplicate bulk bucket accepted")
	}
	invalid := &residentRouteCell{}
	invalid.meta.Store(uint64(4096) | uint64(^uint32(0))<<32)
	if _, err := residentBucketBuild([]residentRouteEntry{{cell: invalid}}); err == nil {
		t.Fatal("out-of-namespace bucket accepted")
	}
	if index.retainedBytes() <= len(entries)*int(unsafeSizeResidentRouteEntry()) {
		t.Fatalf("retained bytes=%d excludes nodes", index.retainedBytes())
	}
}

func unsafeSizeResidentRouteEntry() uintptr {
	var entry residentRouteEntry
	return unsafe.Sizeof(entry)
}

func TestResidentBucketIndexPathCopyDeleteMaxAndOldRoot(t *testing.T) {
	ids := make([]BucketID, 0, 128)
	entries := make([]residentRouteEntry, 0, 128)
	for i := range 128 {
		id, entry := residentBucketTestEntry(t, uint32(i%11), uint32((i*101)%4092))
		ids, entries = append(ids, id), append(entries, entry)
	}
	base, err := residentBucketBuild(entries)
	if err != nil {
		t.Fatal(err)
	}
	addedID, added := residentBucketTestEntry(t, 99, 4095)
	next, err := base.set(addedID, added)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := base.lookup(addedID); ok {
		t.Fatal("old root observed add")
	}
	if got, ok := next.lookup(addedID); !ok || got.cell != added.cell {
		t.Fatal("new root lost add")
	}
	if max, ok := next.max(); !ok || max != addedID {
		t.Fatalf("max=%d,%v", max, ok)
	}
	deleted, ok := next.delete(addedID)
	if !ok {
		t.Fatal("delete absent")
	}
	if _, ok := deleted.lookup(addedID); ok {
		t.Fatal("deleted root retained key")
	}
	if _, ok := next.lookup(addedID); !ok {
		t.Fatal("delete mutated old root")
	}
	wantMax := ids[0]
	for _, id := range ids[1:] {
		if id > wantMax {
			wantMax = id
		}
	}
	if max, ok := deleted.max(); !ok || max != wantMax {
		t.Fatalf("max after delete=%d,%v want %d", max, ok, wantMax)
	}
	if same, ok := deleted.delete(addedID); ok || same != deleted {
		t.Fatal("absent delete changed root")
	}
}

func TestResidentBucketIndexSetAllocationIsCardinalityIndependent(t *testing.T) {
	build := func(n int) *residentBucketIndex {
		entries := make([]residentRouteEntry, n)
		for i := range n {
			_, entries[i] = residentBucketTestEntry(t, uint32(i/4092), uint32(i%4092))
		}
		index, err := residentBucketBuild(entries)
		if err != nil {
			t.Fatal(err)
		}
		return index
	}
	small, large := build(32), build(100000)
	id, entry := residentBucketTestEntry(t, 100, 7)
	smallAllocs := testing.AllocsPerRun(20, func() {
		if _, err := small.set(id, entry); err != nil {
			panic(err)
		}
	})
	largeAllocs := testing.AllocsPerRun(20, func() {
		if _, err := large.set(id, entry); err != nil {
			panic(err)
		}
	})
	if largeAllocs > smallAllocs+1 {
		t.Fatalf("set allocs scale with cardinality: small %.0f large %.0f", smallAllocs, largeAllocs)
	}
}

var residentBucketIndexBenchmarkSink *residentBucketIndex

// BenchmarkResidentBucketIndexPersistentEditScaling guards the fixed-depth
// edit property. In particular, retained-byte accounting must not walk the
// untouched subtree as index cardinality grows.
func BenchmarkResidentBucketIndexPersistentEditScaling(b *testing.B) {
	for _, count := range []int{32, 100000} {
		entries := make([]residentRouteEntry, count)
		for i := range count {
			_, entries[i] = residentBucketTestEntry(b, uint32(i/4092), uint32(i%4092))
		}
		index, err := residentBucketBuild(entries)
		if err != nil {
			b.Fatal(err)
		}
		setID, setEntry := residentBucketTestEntry(b, 100, 7)
		deleteID, _ := residentBucketEntryID(entries[count/2])
		b.Run(fmt.Sprintf("entries=%d/set", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				residentBucketIndexBenchmarkSink, err = index.set(setID, setEntry)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("entries=%d/delete", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var ok bool
				residentBucketIndexBenchmarkSink, ok = index.delete(deleteID)
				if !ok {
					b.Fatal("existing bucket not deleted")
				}
			}
		})
	}
}
