package storeio

import (
	"bytes"
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
	if _, err = s.AllocatePage(PageIndexPosting, 4096, 0); err == nil {
		t.Fatal("second outstanding page accepted")
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
	if &scratch[0] != &p.Bytes()[0] || w.WrittenBytes() != 4096 {
		t.Fatalf("scratch/write = %p/%p %d", &scratch[0], &p.Bytes()[0], w.WrittenBytes())
	}
}
