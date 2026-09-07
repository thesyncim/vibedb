package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

func bufferedRetentionValue(t testing.TB, id, revision int) []byte {
	t.Helper()
	raw := fmt.Appendf(nil, `{"id":"%09d","rev":"%040d","tag":"%09d","pad":"%s"}`, id, revision, id, strings.Repeat("p", 1000))
	value, err := vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func bufferedRetentionSeedValue(t testing.TB, id int) []byte {
	t.Helper()
	value, err := vibejson.AppendCanonicalize(nil, primaryBufferedFixedValue(id, 0))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type bufferedRetentionReader interface {
	AppendRaw(dst []byte, key []byte) ([]byte, bool, error)
}

func requireBufferedRetentionRows(
	t testing.TB,
	reader bufferedRetentionReader,
	keys []string,
	want func(int) []byte,
) {
	t.Helper()
	var scratch []byte
	for id := range keys {
		got, found, readErr := reader.AppendRaw(scratch[:0], []byte(keys[id]))
		scratch = got
		if readErr != nil || !found || !bytes.Equal(got, want(id)) {
			t.Fatalf("id=%d found=%v err=%v, want exact value", id, found, readErr)
		}
	}
}

// TestBufferedPrimarySnapshotReservationRepro holds one immutable view while
// same-sized out-of-line replacements repeatedly materialize primary leaves.
// The initial retirement slice is the fold window (normally 1024 entries),
// while MaxRetiredExtents is the actual policy bound. The test must cross that
// window without losing either the held snapshot or the current/reopened view.
func TestBufferedPrimarySnapshotReservationRepro(t *testing.T) {
	const count = 2000
	built, keys := buildPrimaryBufferedFixedCorpus(t, count)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 128 << 20,
		Durability: DurabilitySync, MaxRetiredExtents: 65536,
		MaxBatchDocuments: 1024,
		Indexes:           []store.IndexDefinition{{Name: "tag", Paths: []string{"/tag"}}},
	}
	file := createPrimaryPointFile(t, built, options, "retention-repro.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	initialCapacity := cap(collection.primaryVolatileRetired)
	if initialCapacity >= options.MaxRetiredExtents {
		t.Fatalf("fixture did not retain a smaller fold window: initial cap=%d max=%d", initialCapacity, options.MaxRetiredExtents)
	}
	seed := make([][]byte, count)
	for id := range count {
		seed[id] = bufferedRetentionSeedValue(t, id)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	latest := make(map[int][]byte)
	for revision := 1; revision <= 1200; revision++ {
		id := (revision * 47) % count
		value := bufferedRetentionValue(t, id, revision)
		if created, err := collection.Put([]byte(keys[id]), value); err != nil || created {
			t.Fatalf("revision %d id %d: created=%v err=%v", revision, id, created, err)
		}
		latest[id] = value
	}
	verify := func(label string, reader bufferedRetentionReader, want func(int) []byte) {
		t.Helper()
		var scratch []byte
		for id := range count {
			got, found, readErr := reader.AppendRaw(scratch[:0], []byte(keys[id]))
			scratch = got
			if readErr != nil || !found || !bytes.Equal(got, want(id)) {
				t.Fatalf("%s id=%d found=%v err=%v, want exact value", label, id, found, readErr)
			}
		}
	}
	verify("live", collection, func(id int) []byte {
		if value, ok := latest[id]; ok {
			return value
		}
		return seed[id]
	})
	verify("snapshot", snapshot, func(id int) []byte { return seed[id] })
	if len(collection.primaryVolatileRetired) <= initialCapacity {
		t.Fatalf("retirement table did not cross fold window: initial cap=%d retained=%d", initialCapacity, len(collection.primaryVolatileRetired))
	}
	if cap(collection.primaryVolatileRetired) > options.MaxRetiredExtents {
		t.Fatalf("retirement table exceeds configured bound: cap=%d max=%d", cap(collection.primaryVolatileRetired), options.MaxRetiredExtents)
	}
	stats := collection.Stats()
	t.Logf("held snapshot survived 1200 replacements: initial cap=%d generation=%d retired=%d/%d checkpoints=%d", initialCapacity, collection.Generation(), len(collection.primaryVolatileRetired), cap(collection.primaryVolatileRetired), stats.RetirementPressureCheckpoints)
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	verify("reopen", reopened, func(id int) []byte {
		if value, ok := latest[id]; ok {
			return value
		}
		return seed[id]
	})
}

// TestBufferedPrimaryBatchSnapshotReservationRepro keeps one snapshot pinned
// while repeated 64-document Updates accumulate pending primary parents. This
// exercises the batch planner's reservation and publish path across the same
// fold-window boundary as the scalar regression.
func TestBufferedPrimaryBatchSnapshotReservationRepro(t *testing.T) {
	const (
		count     = 2000
		batches   = 60
		batchSize = 64
	)
	built, keys := buildPrimaryBufferedFixedCorpus(t, count)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 128 << 20,
		Durability: DurabilitySync, MaxRetiredExtents: 65536,
		MaxBatchDocuments: 1024,
	}
	file := createPrimaryPointFile(t, built, options, "retention-batch-repro.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	initialCapacity := cap(collection.primaryVolatileRetired)
	if initialCapacity >= options.MaxRetiredExtents {
		t.Fatalf("fixture did not retain a smaller fold window: initial cap=%d max=%d", initialCapacity, options.MaxRetiredExtents)
	}
	seed := make([][]byte, count)
	for id := range count {
		seed[id] = bufferedRetentionSeedValue(t, id)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	latest := make(map[int][]byte, batches*batchSize)
	for batchIndex := range batches {
		if err := collection.Update(func(batch *WriteBatch) error {
			for item := range batchSize {
				sequence := batchIndex*batchSize + item + 1
				id := (sequence * 47) % count
				value := bufferedRetentionValue(t, id, sequence)
				if err := batch.Put([]byte(keys[id]), value); err != nil {
					return err
				}
				latest[id] = value
			}
			return nil
		}); err != nil {
			t.Fatalf("batch %d: %v", batchIndex, err)
		}
	}
	wantCurrent := func(id int) []byte {
		if value, ok := latest[id]; ok {
			return value
		}
		return seed[id]
	}
	requireBufferedRetentionRows(t, collection, keys, wantCurrent)
	requireBufferedRetentionRows(t, snapshot, keys, func(id int) []byte { return seed[id] })
	if len(collection.primaryVolatileRetired) <= initialCapacity {
		t.Fatalf("batch path did not cross fold window: initial cap=%d retained=%d", initialCapacity, len(collection.primaryVolatileRetired))
	}
	if cap(collection.primaryVolatileRetired) > options.MaxRetiredExtents {
		t.Fatalf("retirement table exceeds configured bound: cap=%d max=%d", cap(collection.primaryVolatileRetired), options.MaxRetiredExtents)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireBufferedRetentionRows(t, reopened, keys, wantCurrent)
}

// TestBufferedPrimaryConfiguredRetirementLimitRepro proves that the shared
// reservation still fails closed at the configured limit. A failed replacement
// must leave the current row, held snapshot, publication, journal, and dirty
// frame state unchanged; releasing the snapshot makes the same replacement
// retryable and durable across reopen.
func TestBufferedPrimaryConfiguredRetirementLimitRepro(t *testing.T) {
	const count = 500
	built, keys := buildPrimaryBufferedFixedCorpus(t, count)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 128 << 20,
		Durability: DurabilitySync, MaxRetiredExtents: 2048,
		MaxBatchDocuments: 64,
		Indexes:           []store.IndexDefinition{{Name: "tag", Paths: []string{"/tag"}}},
	}
	file := createPrimaryPointFile(t, built, options, "retention-limit.vibe")
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		collection.Close()
		t.Fatal(err)
	}
	defer snapshot.Close()
	failedID, failedRevision := -1, -1
	previous := make(map[int][]byte)
	var failedBefore Stats
	for revision := 1; revision <= 1500; revision++ {
		id := (revision * 47) % count
		candidate := bufferedRetentionValue(t, id, revision)
		before := collection.Stats()
		created, putErr := collection.Put([]byte(keys[id]), candidate)
		if errors.Is(putErr, storeio.ErrRetiredExtentCapacity) {
			if created {
				t.Fatalf("capacity failure reported created row at revision=%d id=%d", revision, id)
			}
			failedID, failedRevision = id, revision
			failedBefore = before
			after := collection.Stats()
			if after.PublishedGeneration != before.PublishedGeneration || after.JournalAcks != before.JournalAcks {
				t.Fatalf("failed write changed publication: generation %d->%d journal acknowledgements %d->%d", before.PublishedGeneration, after.PublishedGeneration, before.JournalAcks, after.JournalAcks)
			}
			t.Logf("limit refusal at revision=%d id=%d generation=%d retired=%d/%d err=%v", revision, id, collection.Generation(), len(collection.primaryVolatileRetired), cap(collection.primaryVolatileRetired), putErr)
			break
		}
		if putErr != nil || created {
			t.Fatalf("revision %d id %d: created=%v err=%v", revision, id, created, putErr)
		}
		previous[id] = candidate
	}
	if failedRevision < 0 {
		t.Fatal("snapshot-pinned writes did not hit configured retirement limit")
	}
	if failedBefore.PublishedGeneration == 0 {
		t.Fatal("missing pre-failure publication snapshot")
	}
	wantBefore := bufferedRetentionSeedValue(t, failedID)
	if value, ok := previous[failedID]; ok {
		wantBefore = value
	}
	got, found, readErr := collection.AppendRaw(nil, []byte(keys[failedID]))
	if readErr != nil || !found || !bytes.Equal(got, wantBefore) {
		t.Fatalf("failed write changed live row: got=%q found=%v err=%v, want %q", got, found, readErr, wantBefore)
	}
	if got, found, readErr := snapshot.AppendRaw(nil, []byte(keys[failedID])); readErr != nil || !found || !bytes.Equal(got, bufferedRetentionSeedValue(t, failedID)) {
		t.Fatalf("snapshot row = %q found=%v err=%v, want seed", got, found, readErr)
	}
	if len(collection.primaryVolatileRetired) > options.MaxRetiredExtents || cap(collection.primaryVolatileRetired) > options.MaxRetiredExtents {
		t.Fatalf("retirement table exceeds configured bound: len=%d cap=%d max=%d", len(collection.primaryVolatileRetired), cap(collection.primaryVolatileRetired), options.MaxRetiredExtents)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	candidate := bufferedRetentionValue(t, failedID, failedRevision)
	created, err := collection.Put([]byte(keys[failedID]), candidate)
	if err != nil || created {
		t.Fatalf("retry after snapshot release: created=%v err=%v", created, err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, found, readErr := collection.AppendRaw(nil, []byte(keys[failedID])); readErr != nil || !found || !bytes.Equal(got, candidate) {
		t.Fatalf("post-retry live row = %q found=%v err=%v", got, found, readErr)
	}
	path := file.Name()
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopenFile, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(reopenFile, options)
	if err != nil {
		reopenFile.Close()
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, found, readErr := reopened.AppendRaw(nil, []byte(keys[failedID])); readErr != nil || !found || !bytes.Equal(got, candidate) {
		t.Fatalf("reopen live row = %q found=%v err=%v", got, found, readErr)
	}
}
