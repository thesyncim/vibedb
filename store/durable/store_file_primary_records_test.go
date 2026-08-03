package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestCreateFromRecordsMatchesPrimaryCollectionBytes(t *testing.T) {
	const count = 257
	records := make([]PrimaryBulkRecord, 0, count)
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Reverse input order and retain whitespace/object-key disorder so both
	// paths must perform the same lexical sort and canonicalization work.
	for i := count - 1; i >= 0; i-- {
		key := fmt.Sprintf("record-%06d", i)
		value := fmt.Appendf(nil,
			`{ "z" : %d, "a" : { "enabled" : %t, "name" : "row %d" } }`,
			i, i&1 == 0, i,
		)
		records = append(records, PrimaryBulkRecord{Key: key, Value: value})
		if err := builder.Append(key, value); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, Durability: DurabilityAsyncVisible,
	}
	collectionPath := filepath.Join(t.TempDir(), "collection.vibe")
	collectionFile, err := os.OpenFile(
		collectionPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer collectionFile.Close()
	if _, err := CreateFromPrimary(source, collectionFile, options); err != nil {
		t.Fatal(err)
	}
	directPath := filepath.Join(t.TempDir(), "records.vibe")
	directFile, err := os.OpenFile(
		directPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer directFile.Close()
	if _, err := CreateFromRecords(records, directFile, options); err != nil {
		t.Fatal(err)
	}
	collectionBytes, err := os.ReadFile(collectionPath)
	if err != nil {
		t.Fatal(err)
	}
	directBytes, err := os.ReadFile(directPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(directBytes, collectionBytes) {
		t.Fatalf(
			"native record build differs from collection conversion: direct=%d bytes collection=%d bytes",
			len(directBytes), len(collectionBytes),
		)
	}
}

func TestCreateFromRecordsRejectsDuplicateBeforeWriting(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "records-duplicate-*.vibe")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	_, err = CreateFromRecords([]PrimaryBulkRecord{
		{Key: "duplicate", Value: []byte(`{"a":1}`)},
		{Key: "duplicate", Value: []byte(`{"a":2}`)},
	}, file, Options{Backend: BackendPortable})
	if err == nil {
		t.Fatal("duplicate native records were accepted")
	}
	info, statErr := file.Stat()
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Size() != 0 {
		t.Fatalf("duplicate rejection wrote %d bytes", info.Size())
	}
}
