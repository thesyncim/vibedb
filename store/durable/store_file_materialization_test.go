package durable

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileMaterializationUpdateSnapshotAndBusyFallback(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "materialization-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	options.MaterializationDamageGranule = 512
	options.DisableMutationCombining = true
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	const key = "materialized"
	values := [][]byte{
		[]byte(`{"a":1,"b":2,"c":3,"payload":"aaaaaaaa"}`),
		[]byte(`{"a":1,"b":2,"c":3,"payload":"bbbbbbbb"}`),
		[]byte(`{"a":1,"b":2,"c":3,"payload":"cccccccc"}`),
		[]byte(`{"a":1,"b":2,"c":3,"payload":"dddddddd"}`),
		[]byte(`{"a":1,"b":2,"c":3,"payload":"eeeeeeee"}`),
	}
	if _, err := collection.Put(key, values[0]); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}

	firstState := collection.visibleState.Load()
	firstMatch, found, err := collection.resolveFileFingerprint(
		firstState, []byte(key),
	)
	if err != nil || !found {
		t.Fatalf("resolve initial document = %v, %v", found, err)
	}
	stableRef := firstMatch.documentRef
	firstMatch.Release()

	if _, err := collection.Put(key, values[1]); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	stats := collection.Stats()
	if stats.MaterializationUpdates != 1 ||
		stats.MaterializedBatches != 1 ||
		stats.MaterializationJournalBytes != storeio.MaterializationJournalSize ||
		stats.MaterializationTargetBytes == 0 ||
		stats.MaterializationTargetBytes >= uint64(stableRef.Length) {
		t.Fatalf("first materialization stats = %+v, ref=%+v", stats, stableRef)
	}
	secondState := collection.visibleState.Load()
	secondMatch, found, err := collection.resolveFileFingerprint(
		secondState, []byte(key),
	)
	if err != nil || !found {
		t.Fatalf("resolve materialized document = %v, %v", found, err)
	}
	if secondMatch.documentRef != stableRef {
		t.Fatalf("materialization changed PageRef: got %+v, want %+v",
			secondMatch.documentRef, stableRef)
	}
	secondMatch.Release()
	requireFileMaterializationValue(t, collection, key, values[1])

	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put(key, values[2]); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshotValue, ok, err := snapshot.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(snapshotValue, values[1]) {
		t.Fatalf("snapshot after fallback = %q, %v, %v", snapshotValue, ok, err)
	}
	requireFileMaterializationValue(t, collection, key, values[2])
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := collection.Put(key, values[3]); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	requireFileMaterializationValue(t, collection, key, values[3])

	current := collection.visibleState.Load()
	pinned, found, err := collection.resolveFileFingerprint(current, []byte(key))
	if err != nil || !found {
		t.Fatalf("pin current document = %v, %v", found, err)
	}
	if _, err := collection.Put(key, values[4]); err != nil {
		pinned.Release()
		t.Fatal(err)
	}
	pinned.Release()
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	requireFileMaterializationValue(t, collection, key, values[4])

	stats = collection.Stats()
	if stats.MaterializationUpdates != 2 ||
		stats.MaterializationSnapshotSkips == 0 ||
		stats.MaterializationBusySkips == 0 ||
		stats.MaterializationFallbacks < 2 {
		t.Fatalf("materialization/fallback stats = %+v", stats)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	requireFileMaterializationValue(t, reopened, key, values[4])
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileMaterializationDurableCallbackReleasesCanonicalFrame(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "materialization-callback-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	options.MaterializationDamageGranule = 512
	options.DisableMutationCombining = true
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	const key = "callback"
	values := [...][]byte{
		[]byte(`{"payload":"aaaaaaaa"}`),
		[]byte(`{"payload":"bbbbbbbb"}`),
		[]byte(`{"payload":"cccccccc"}`),
	}
	if _, err := collection.Put(key, values[0]); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put(key, values[1]); err != nil {
		t.Fatal(err)
	}
	target := collection.Generation()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stats := collection.Stats()
		if stats.DurableGeneration >= target && stats.DirtyBytes == 0 {
			break
		}
		time.Sleep(100 * time.Microsecond)
	}
	if stats := collection.Stats(); stats.DurableGeneration < target ||
		stats.DirtyBytes != 0 {
		t.Fatalf(
			"background callback did not settle generation %d: %+v",
			target, stats,
		)
	}
	if _, err := collection.Put(key, values[2]); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if stats := collection.Stats(); stats.MaterializationUpdates != 2 ||
		stats.MaterializedBatches != 2 {
		t.Fatalf(
			"consecutive callback-released materializations = %+v",
			stats,
		)
	}
	requireFileMaterializationValue(t, collection, key, values[2])
}

func requireFileMaterializationValue(
	t testing.TB, collection *Collection, key string, want []byte,
) {
	t.Helper()
	got, found, err := collection.AppendRaw(nil, key)
	if err != nil || !found || !bytes.Equal(got, want) {
		t.Fatalf("AppendRaw(%q) = %q, %v, %v; want %q",
			key, got, found, err, want)
	}
}
