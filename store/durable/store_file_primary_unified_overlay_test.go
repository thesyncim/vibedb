package durable

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestPrimaryUnifiedOverlayBucketDirectory(t *testing.T) {
	// Find two identities with the same initial directory slot so the test
	// exercises probing rather than relying only on the no-collision case.
	var first, second storeio.BucketID
	var seen [primaryUnifiedOverlayBucketTable]storeio.BucketID
	for candidate := storeio.BucketID(1); second == 0; candidate++ {
		slot := primaryUnifiedOverlayBucketHash(candidate) &
			(primaryUnifiedOverlayBucketTable - 1)
		if seen[slot] != 0 {
			first, second = seen[slot], candidate
			break
		}
		seen[slot] = candidate
	}

	overlay := newPrimaryUnifiedOverlay(16 << 10)
	publish := func(
		bucket storeio.BucketID,
		hash, generation uint64,
		key, value string,
		rawDelta, countDelta int,
		slot uint8,
	) {
		t.Helper()
		prepared, err := overlay.prepare(
			bucket, hash, generation, []byte(key), []byte(value),
			rawDelta, countDelta, primaryUnifiedOverlayPut, slot,
		)
		if err != nil {
			t.Fatalf("prepare generation %d: %v", generation, err)
		}
		overlay.publish(prepared)
	}

	publish(first, 11, 1, "first", "v1", 10, 1, 5)
	publish(first, 11, 2, "first", "v2", 3, 0, 5)
	publish(second, 22, 3, "second", "other", 12, 1, 67)

	if gotRaw, gotRows := overlay.pendingBucketDeltas(first); gotRaw != 13 ||
		gotRows != 1 {
		t.Fatalf("first deltas = %d,%d, want 13,1", gotRaw, gotRows)
	}
	if gotRaw, gotRows := overlay.pendingBucketDeltas(second); gotRaw != 12 ||
		gotRows != 1 {
		t.Fatalf("second deltas = %d,%d, want 12,1", gotRaw, gotRows)
	}
	firstSlots := overlay.pendingInsertSlots(first)
	secondSlots := overlay.pendingInsertSlots(second)
	if firstSlots[0] != uint64(1)<<5 ||
		secondSlots[1] != uint64(1)<<3 {
		t.Fatalf(
			"insert slots = %#x/%#x, want bit 5 / bit 67",
			firstSlots, secondSlots,
		)
	}
	if got := overlay.bucketVersion(first, 3); got != 2 {
		t.Fatalf("first bucket version = %d, want 2", got)
	}

	var buckets [primaryUnifiedOverlayBuckets]storeio.BucketID
	var keys [primaryUnifiedOverlayBuckets][]byte
	n, err := overlay.pendingBuckets(&buckets, &keys)
	if err != nil || n != 2 {
		t.Fatalf("pending buckets = %d,%v, want 2,nil", n, err)
	}
	representatives := make(map[storeio.BucketID]string, n)
	for i := range n {
		representatives[buckets[i]] = string(keys[i])
	}
	if representatives[first] != "first" ||
		representatives[second] != "second" {
		t.Fatalf("representatives = %#v", representatives)
	}

	var recordStorage [2]storeio.CommonPrimaryLeafRecord
	records, err := overlay.applyBucket(
		recordStorage[:0], first, 3,
	)
	if err != nil || len(records) != 1 ||
		!bytes.Equal(records[0].Key, []byte("first")) ||
		!bytes.Equal(records[0].Value.Inline, []byte("v2")) {
		t.Fatalf("applied records = %#v,%v", records, err)
	}

	// A non-recycling fold clears only the pending-bucket directory. The global
	// hash chains remain available to the old generation, while a new window
	// starts with exact zeroed deltas and no bucket-chain link into stale rows.
	overlay.markFolded(3, false)
	if overlay.pendingBucket(first) || overlay.bucketCount.Load() != 0 {
		t.Fatal("fold retained pending directory state")
	}
	if raw, rows := overlay.pendingBucketDeltas(first); raw != 0 || rows != 0 {
		t.Fatalf("post-fold deltas = %d,%d, want 0,0", raw, rows)
	}
	old, disposition, _ := overlay.lookup(first, 11, []byte("first"), 3)
	if disposition != primaryUnifiedOverlayValue ||
		!bytes.Equal(old, []byte("v2")) {
		t.Fatalf("old-generation lookup = %q,%d", old, disposition)
	}
	publish(first, 11, 4, "first", "v4", 0, 0, 5)
	current, disposition, _ :=
		overlay.lookup(first, 11, []byte("first"), 4)
	if disposition != primaryUnifiedOverlayValue ||
		!bytes.Equal(current, []byte("v4")) {
		t.Fatalf("new-window lookup = %q,%d", current, disposition)
	}
	old, disposition, _ = overlay.lookup(first, 11, []byte("first"), 3)
	if disposition != primaryUnifiedOverlayValue ||
		!bytes.Equal(old, []byte("v2")) {
		t.Fatalf("pinned old-generation lookup = %q,%d", old, disposition)
	}
}

func TestPrimaryUnifiedOverlayBucketDirectoryPressure(t *testing.T) {
	overlay := newPrimaryUnifiedOverlay(64 << 10)
	for index := range primaryUnifiedOverlayBuckets {
		key := fmt.Appendf(nil, "k%d", index)
		prepared, err := overlay.prepare(
			storeio.BucketID(index+1), uint64(index+1), uint64(index+1),
			key, []byte("v"), 1, 1, primaryUnifiedOverlayPut, uint8(index),
		)
		if err != nil {
			t.Fatalf("prepare bucket %d: %v", index, err)
		}
		overlay.publish(prepared)
	}
	if got := overlay.bucketCount.Load(); got != primaryUnifiedOverlayBuckets {
		t.Fatalf("bucket count = %d, want %d", got, primaryUnifiedOverlayBuckets)
	}
	_, err := overlay.prepare(
		storeio.BucketID(primaryUnifiedOverlayBuckets+1),
		primaryUnifiedOverlayBuckets+1,
		primaryUnifiedOverlayBuckets+1,
		[]byte("overflow"), []byte("v"), 1, 1,
		primaryUnifiedOverlayPut, 1,
	)
	if !errors.Is(err, storeio.ErrPageCachePinned) {
		t.Fatalf("65th distinct bucket error = %v", err)
	}
	// Reusing one of the admitted buckets remains legal at the exact limit.
	if _, err := overlay.prepare(
		1, 1, primaryUnifiedOverlayBuckets+1,
		[]byte("k0"), []byte("new"), 2, 0,
		primaryUnifiedOverlayPut, 0,
	); err != nil {
		t.Fatalf("existing bucket at directory limit: %v", err)
	}
}

func TestPrimaryUnifiedOverlayBucketDirectoryPinnedSnapshotGuard(t *testing.T) {
	built, keys, _ := buildFilePrimaryCorpus(t, 1_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "primary-overlay-directory-pinned.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	key := []byte(keys[500])
	first := []byte(`{"window":"first","id":500}`)
	second := []byte(`{"window":"other","id":500}`)
	third := []byte(`{"window":"third","id":500}`)
	canonical := canonicalDocs(t, [][]byte{first, second, third})
	first, second, third = canonical[0], canonical[1], canonical[2]
	if _, err := collection.Put(key, first); err != nil {
		t.Fatal(err)
	}
	// The public snapshot contract first materializes the overlay and then
	// leases that rooted generation, so its point reads and ordered scans name
	// the same base.
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put(key, second); err != nil {
		t.Fatal(err)
	}
	// An active generation lease deliberately vetoes the canonical overlay
	// lane. The mutation uses snapshot-safe COW, so production cannot clear and
	// reuse the bucket directory underneath an old snapshot.
	overlay := collection.primaryUnifiedOverlay
	if overlay.hasPending() || overlay.bucketCount.Load() != 0 {
		t.Fatal("snapshot-contended mutation entered overlay lane")
	}
	got, ok, err := snapshot.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, first) {
		t.Fatalf("pinned point read = %q,%v,%v, want %q", got, ok, err, first)
	}
	var (
		scanned int
		scanGot []byte
	)
	if err := snapshot.RangeRaw(func(gotKey, value []byte) error {
		scanned++
		if bytes.Equal(gotKey, key) {
			scanGot = append(scanGot[:0], value...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if scanned != 1_000 || !bytes.Equal(scanGot, first) {
		t.Fatalf(
			"pinned ordered scan = %d rows, target %q, want 1000/%q",
			scanned, scanGot, first,
		)
	}
	got, ok, err = collection.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, second) {
		t.Fatalf("live point read = %q,%v,%v, want %q", got, ok, err, second)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	// Once the lease exits, the ordinary allocation-free overlay lane resumes
	// and starts from an exact one-bucket directory.
	if _, err := collection.Put(key, third); err != nil {
		t.Fatal(err)
	}
	if !overlay.hasPending() || overlay.bucketCount.Load() != 1 {
		t.Fatal("overlay lane did not resume after snapshot release")
	}
	got, ok, err = collection.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, third) {
		t.Fatalf("resumed point read = %q,%v,%v, want %q", got, ok, err, third)
	}
}
