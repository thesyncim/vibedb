package durable

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

func TestCanonicalizePrimaryBulkRecordsBorrowsCanonicalValues(t *testing.T) {
	value := make([]byte, 1, (1<<20)+2)
	value[0] = '"'
	value = append(value, bytes.Repeat([]byte("a"), 1<<20)...)
	value = append(value, '"')
	const copies = 16
	records := make([]storeio.PrimaryGraphRecord, copies)
	for at := range records {
		records[at].Value = byteview.String(value)
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	if err := canonicalizePrimaryBulkRecords(records, document.IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	if allocated > 2<<20 {
		t.Fatalf(
			"canonical bulk validation allocated %d bytes for borrowed values, want at most %d",
			allocated, 2<<20,
		)
	}
	for at := range records {
		borrowed := byteview.Bytes(records[at].Value)
		if len(records[at].Value) != len(value) ||
			&borrowed[0] != &value[0] {
			t.Fatalf("canonical record %d stopped borrowing its source", at)
		}
	}
}

func TestCanonicalizePrimaryBulkRecordsLazilyRewrites(t *testing.T) {
	canonical := []byte(`{"a":1}`)
	noncanonical := []byte(`{ "a" : 1 }`)
	records := []storeio.PrimaryGraphRecord{
		{Value: byteview.String(canonical)},
		{Value: byteview.String(noncanonical)},
		{Value: byteview.String(canonical)},
	}
	if err := canonicalizePrimaryBulkRecords(records, document.IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	first := byteview.Bytes(records[0].Value)
	third := byteview.Bytes(records[2].Value)
	if &first[0] != &canonical[0] || &third[0] != &canonical[0] {
		t.Fatal("canonical values stopped borrowing their source")
	}
	rewritten := byteview.Bytes(records[1].Value)
	if !bytes.Equal(rewritten, canonical) {
		t.Fatalf("rewritten value = %s, want %s", records[1].Value, canonical)
	}
	if &rewritten[0] == &noncanonical[0] {
		t.Fatal("noncanonical value was not rewritten into the lazy arena")
	}
}
