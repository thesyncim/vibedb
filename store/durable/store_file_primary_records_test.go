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

func TestCreateFromRecordsDefersOrdinaryBufferedJournalUntilMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lazy-journal.vibe")
	file, err := os.OpenFile(
		path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := Options{
		Backend: BackendPortable, Durability: DurabilityBufferedVisible,
		ResidentBytes: 32 << 20,
	}
	if _, err := CreateFromRecords([]PrimaryBulkRecord{
		{Key: "log-000001", Value: []byte(`{"level":"info","seq":1}`)},
		{Key: "log-000002", Value: []byte(`{"level":"warn","seq":2}`)},
	}, file, options); err != nil {
		t.Fatal(err)
	}
	journalPath := RecoveryJournalPath(path)
	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatalf("immutable bulk build journal stat = %v, want absent", err)
	}

	coll, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if coll.journalEnabled() ||
		coll.durableState.Load().root.JournalID != ([16]byte{}) {
		t.Fatal("read-only open enabled an unneeded recovery journal")
	}
	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatalf("read-only open journal stat = %v, want absent", err)
	}

	if _, err := coll.Put(
		[]byte("log-000001"), []byte(`{"level":"error","seq":1}`),
	); err != nil {
		t.Fatal(err)
	}
	if !coll.journalEnabled() || coll.journalID == ([16]byte{}) {
		t.Fatal("first valid mutation did not mint foreground journal")
	}
	if _, err := os.Stat(journalPath); err != nil {
		t.Fatalf("stat lazy journal after mutation: %v", err)
	}
	if coll.bufferedJournalDeltaLane() ||
		coll.durableState.Load().root.JournalID != ([16]byte{}) {
		t.Fatal("unrooted lazy journal was allowed to acknowledge a delta")
	}

	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	if !coll.bufferedJournalDeltaLane() ||
		coll.durableState.Load().root.JournalID != coll.journalID {
		t.Fatal("first foreground flush did not root lazy journal")
	}
	physical := coll.committer.DurableGeneration()
	before := coll.Stats()
	if _, err := coll.Put(
		[]byte("log-000002"), []byte(`{"level":"info","seq":2}`),
	); err != nil {
		t.Fatal(err)
	}
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	after := coll.Stats()
	if after.JournalDeltaCheckpoints != before.JournalDeltaCheckpoints+1 {
		t.Fatalf("second flush delta checkpoints = %d, want %d",
			after.JournalDeltaCheckpoints, before.JournalDeltaCheckpoints+1)
	}
	if got := coll.committer.DurableGeneration(); got != physical {
		t.Fatalf("second flush physical generation = %d, want unchanged %d",
			got, physical)
	}
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, ok, err := reopened.AppendRaw(nil, []byte("log-000002"))
	if err != nil || !ok || string(got) != `{"level":"info","seq":2}` {
		t.Fatalf("reopened lazy-journal value = %s,%t,%v", got, ok, err)
	}
}
