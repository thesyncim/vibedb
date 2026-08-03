package durable

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson/document"
)

func TestCanonicalizePrimaryBulkRecordsBorrowsCanonicalValues(t *testing.T) {
	value := make([]byte, 1, (1<<20)+2)
	value[0] = '"'
	value = append(value, bytes.Repeat([]byte("a"), 1<<20)...)
	value = append(value, '"')
	const copies = 16
	records := make([]storeio.PrimaryGraphRecord, copies)
	for at := range records {
		records[at].Value = value
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
		if len(records[at].Value) != len(value) ||
			&records[at].Value[0] != &value[0] {
			t.Fatalf("canonical record %d stopped borrowing its source", at)
		}
	}
}

func TestCanonicalizePrimaryBulkRecordsLazilyRewrites(t *testing.T) {
	canonical := []byte(`{"a":1}`)
	noncanonical := []byte(`{ "a" : 1 }`)
	records := []storeio.PrimaryGraphRecord{
		{Value: canonical},
		{Value: noncanonical},
		{Value: canonical},
	}
	if err := canonicalizePrimaryBulkRecords(records, document.IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	if &records[0].Value[0] != &canonical[0] ||
		&records[2].Value[0] != &canonical[0] {
		t.Fatal("canonical values stopped borrowing their source")
	}
	if !bytes.Equal(records[1].Value, canonical) {
		t.Fatalf("rewritten value = %s, want %s", records[1].Value, canonical)
	}
	if &records[1].Value[0] == &noncanonical[0] {
		t.Fatal("noncanonical value was not rewritten into the lazy arena")
	}
}
