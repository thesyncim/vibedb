package vnext

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func BenchmarkCurrentFingerprintLookupResident(b *testing.B) {
	const count = (FingerprintPageSize - storeio.PageHeaderSize -
		storeio.PageTrailerSize - storeio.PageKeyDirectoryPayloadHeaderSize) /
		storeio.PageKeyLeafEntrySize
	entries := make([]storeio.PageKeyLocation, count)
	for i := range entries {
		entries[i] = storeio.PageKeyLocation{
			Hash: uint64(i) << 32, Chunk: uint32(i + 1), Slot: uint8(i % 64),
		}
	}
	page, err := storeio.EncodePageKeyLeaf(
		make([]byte, FingerprintPageSize),
		storeio.PageKeyDirectoryHeader{
			StoreID: testIdentity.StoreID, Generation: testIdentity.Generation,
			LogicalID: testIdentity.LogicalID, PageSize: FingerprintPageSize,
			MinHash: entries[0].Hash, MaxHash: entries[len(entries)-1].Hash,
		},
		entries,
		64*FingerprintPageSize,
		64,
		uint32(count+1),
		64,
	)
	if err != nil {
		b.Fatal(err)
	}
	view := storeio.AdmittedPageKeyDirectory(page)
	hash := entries[len(entries)/2].Hash
	want := entries[len(entries)/2]
	b.ReportAllocs()
	b.SetBytes(storeio.PageKeyLeafEntrySize)
	for b.Loop() {
		first, end, ok := view.CandidateRange(hash)
		if !ok || end != first+1 {
			b.Fatal("range")
		}
		if got, ok := view.LocationAt(first); !ok || got != want {
			b.Fatal("location")
		}
	}
}

func BenchmarkCurrentDocumentPageLookupExact(b *testing.B) {
	rows := make([]storeio.DocumentRecord, 64)
	live := ^uint64(0)
	for i := range rows {
		rows[i] = storeio.DocumentRecord{
			Slot: uint8(i),
			Key:  []byte(fmt.Sprintf("key-%09d", i)),
			JSON: []byte(`{"payload":"abcdefghijklmnopqrstuvwxyz"}`),
		}
	}
	page, err := storeio.EncodeDocumentPage(
		make([]byte, 8<<10),
		storeio.DocumentPageHeader{
			StoreID: testIdentity.StoreID, Generation: testIdentity.Generation,
			LogicalID: testIdentity.LogicalID, PageSize: 8 << 10,
			ChunkID: 7, Live: live,
		},
		rows,
		64,
	)
	if err != nil {
		b.Fatal(err)
	}
	view := storeio.AdmittedDocumentPage(page)
	b.ReportAllocs()
	b.SetBytes(int64(len(rows[37].JSON)))
	for b.Loop() {
		if _, ok := view.LookupString(37, "key-000000037"); !ok {
			b.Fatal("lookup")
		}
	}
}

func BenchmarkCurrentDocumentPageAppendExact(b *testing.B) {
	packedRows := repetitivePackedRows()
	rows := make([]storeio.DocumentRecord, len(packedRows))
	live := ^uint64(0)
	for i, row := range packedRows {
		rows[i] = storeio.DocumentRecord{
			Slot: row.Slot, Key: row.Key, JSON: row.JSON,
		}
	}
	page, err := storeio.EncodeDocumentPage(
		make([]byte, 32<<10),
		storeio.DocumentPageHeader{
			StoreID: testIdentity.StoreID, Generation: testIdentity.Generation,
			LogicalID: testIdentity.LogicalID, PageSize: 32 << 10,
			ChunkID: 7, Live: live,
		},
		rows,
		64,
	)
	if err != nil {
		b.Fatal(err)
	}
	view := storeio.AdmittedDocumentPage(page)
	dst := make([]byte, 0, len(rows[37].JSON))
	b.ReportAllocs()
	b.SetBytes(int64(len(rows[37].JSON)))
	for b.Loop() {
		raw, ok := view.LookupString(37, "key-000000037")
		if !ok {
			b.Fatal("lookup")
		}
		got := append(dst[:0], raw...)
		if len(got) != len(rows[37].JSON) {
			b.Fatal("append")
		}
	}
}
