package storeio

import (
	"encoding/binary"
	"errors"
	"testing"
)

func testPageHeader(kind PageKind, logicalID, generation uint64) PageHeader {
	return PageHeader{
		StoreID:       testStoreID,
		Generation:    generation,
		LogicalID:     logicalID,
		PageSize:      testSuperblockPageSize,
		PayloadLength: 137,
		Kind:          kind,
	}
}

func resealTestPage(page []byte) {
	trailer := len(page) - PageTrailerSize
	checksum := PageChecksum(page[:trailer])
	binary.LittleEndian.PutUint32(page[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(page[trailer+4:], ^checksum)
}

func TestPageCodecRoundTripAndEveryByteCorruption(t *testing.T) {
	page := make([]byte, testSuperblockPageSize)
	wantHeader := testPageHeader(PageDocument, 19, 7)
	payload, err := InitPage(page, wantHeader)
	if err != nil {
		t.Fatal(err)
	}
	for i := range payload {
		payload[i] = byte(i*29 + 11)
	}
	wantPayload := append([]byte(nil), payload...)
	if _, err := SealPage(page); err != nil {
		t.Fatal(err)
	}
	gotHeader, gotPayload, err := OpenPage(page)
	if err != nil || gotHeader != wantHeader || string(gotPayload) != string(wantPayload) {
		t.Fatalf("OpenPage = (%+v,%x,%v), want (%+v,%x,nil)", gotHeader, gotPayload, err, wantHeader, wantPayload)
	}

	for i := range page {
		page[i] ^= 1
		if _, _, err := OpenPage(page); !errors.Is(err, ErrPageCorrupt) {
			t.Fatalf("byte %d corruption = %v, want %v", i, err, ErrPageCorrupt)
		}
		page[i] ^= 1
	}
	for cut := range page {
		if _, _, err := OpenPage(page[:cut]); !errors.Is(err, ErrPageCorrupt) {
			t.Fatalf("cut %d = %v, want %v", cut, err, ErrPageCorrupt)
		}
	}
}

func TestPageFingerprintDirectoryKindIsDistinctAndRoundTrips(t *testing.T) {
	if PageFingerprintDirectory == PageKeyDirectory {
		t.Fatal("fingerprint and full-key directories share a durable kind")
	}
	page := make([]byte, testSuperblockPageSize)
	want := testPageHeader(PageFingerprintDirectory, 23, 9)
	payload, err := InitPage(page, want)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 0xa5
	if _, err := SealPage(page); err != nil {
		t.Fatal(err)
	}
	got, opened, err := OpenPage(page)
	if err != nil || got != want || len(opened) != int(want.PayloadLength) || opened[0] != 0xa5 {
		t.Fatalf("OpenPage fingerprint = (%+v,%x,%v), want (%+v,a5...,nil)", got, opened, err, want)
	}
}

func TestHybridPrimaryPageKindsAreDistinctAndRoundTrip(t *testing.T) {
	kinds := [...]PageKind{
		PagePrimaryCatalog,
		PageTabletDirectory,
		PagePrimaryLocator,
		PageTabletRoute,
		PagePrimaryAnchor,
		PagePrimaryLeaf,
	}
	seen := make(map[PageKind]struct{}, len(kinds))
	for rank, kind := range kinds {
		if kind <= PageCatalogSegment {
			t.Fatalf("hybrid primary kind %d aliases an earlier schema", kind)
		}
		if _, exists := seen[kind]; exists {
			t.Fatalf("hybrid primary kind %d is duplicated", kind)
		}
		seen[kind] = struct{}{}
		page := make([]byte, testSuperblockPageSize)
		want := testPageHeader(kind, uint64(31+rank), uint64(17+rank))
		payload, err := InitPage(page, want)
		if err != nil {
			t.Fatal(err)
		}
		payload[0] = byte(rank + 1)
		if _, err := SealPage(page); err != nil {
			t.Fatal(err)
		}
		got, opened, err := OpenPage(page)
		if err != nil || got != want || opened[0] != byte(rank+1) {
			t.Fatalf("OpenPage kind %d = (%+v,%x,%v)", kind, got, opened, err)
		}
	}
}

func TestPageCodecRejectsResealedNonCanonicalBytes(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"header reserved", func(page []byte) { page[14] = 1 }},
		{"header tail", func(page []byte) { page[56] = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			page := make([]byte, testSuperblockPageSize)
			if _, err := InitPage(page, testPageHeader(PageDocument, 2, 1)); err != nil {
				t.Fatal(err)
			}
			if _, err := SealPage(page); err != nil {
				t.Fatal(err)
			}
			test.mutate(page)
			resealTestPage(page)
			if _, _, err := OpenPage(page); !errors.Is(err, ErrPageCorrupt) {
				t.Fatalf("OpenPage = %v, want %v", err, ErrPageCorrupt)
			}
		})
	}
}

func TestPageCodecValidationAndUnsealedPadding(t *testing.T) {
	valid := testPageHeader(PageDocument, 2, 1)
	for _, test := range []struct {
		name   string
		mutate func(*PageHeader)
	}{
		{"store id", func(h *PageHeader) { h.StoreID = [16]byte{} }},
		{"generation", func(h *PageHeader) { h.Generation = 0 }},
		{"logical id", func(h *PageHeader) { h.LogicalID = 0 }},
		{"kind", func(h *PageHeader) { h.Kind = 0 }},
		{"flags", func(h *PageHeader) { h.Flags = 1 }},
		{"page size", func(h *PageHeader) { h.PageSize = 5000 }},
		{"payload", func(h *PageHeader) { h.PayloadLength = h.PageSize }},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := valid
			test.mutate(&header)
			page := make([]byte, testSuperblockPageSize)
			if _, err := InitPage(page, header); !errors.Is(err, ErrInvalidWrite) {
				t.Fatalf("InitPage = %v, want %v", err, ErrInvalidWrite)
			}
		})
	}

	page := make([]byte, testSuperblockPageSize)
	if _, err := InitPage(page, valid); err != nil {
		t.Fatal(err)
	}
	page[PageHeaderSize+int(valid.PayloadLength)] = 1
	if _, err := SealPage(page); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("SealPage padding = %v, want %v", err, ErrInvalidWrite)
	}
	if _, err := InitPage(page[:len(page)-1], valid); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("InitPage short = %v, want %v", err, ErrInvalidWrite)
	}
	if _, err := InitPage(page, valid); err != nil {
		t.Fatal(err)
	}
	page[56] = 1
	if _, err := SealPage(page); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("SealPage reserved = %v, want %v", err, ErrInvalidWrite)
	}
}
