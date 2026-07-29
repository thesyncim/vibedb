package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileStoreBufferedVisibleCrashBoundary(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "buffered-visible-*")
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = collection.Close()
		_ = file.Close()
	}()

	before, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	value := []byte(`{"value":"buffered"}`)
	if created, err := collection.Put("key", value); err != nil || !created {
		t.Fatalf("Put = (%v, %v), want created", created, err)
	}
	if got, want := collection.Generation(), uint64(2); got != want {
		t.Fatalf("visible generation = %d, want %d", got, want)
	}
	if got, want := collection.DurableGeneration(), uint64(1); got != want {
		t.Fatalf("durable generation = %d, want %d", got, want)
	}
	got, found, err := collection.AppendRaw(nil, "key")
	if err != nil || !found || !bytes.Equal(got, value) {
		t.Fatalf("visible read = (%q, %v, %v), want %q", got, found, err, value)
	}
	during, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(during, before) {
		t.Fatal("acknowledged buffered Put changed the file before a checkpoint")
	}

	beforeCrash := openBufferedImage(t, before, options)
	if got, found, err := beforeCrash.AppendRaw(nil, "key"); err != nil || found {
		t.Fatalf("recovery before checkpoint = (%q, %v, %v), want absent", got, found, err)
	}

	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, want := collection.DurableGeneration(), collection.Generation(); got != want {
		t.Fatalf("checkpoint generations = durable %d, visible %d", got, want)
	}
	after, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	afterCrash := openBufferedImage(t, after, options)
	got, found, err = afterCrash.AppendRaw(nil, "key")
	if err != nil || !found || !bytes.Equal(got, value) {
		t.Fatalf("recovery after checkpoint = (%q, %v, %v), want %q", got, found, err, value)
	}
}

// TestFileStoreBufferedPrewriteDoesNotPublishRoot was retired with the chunk
// store. It drove the committer write-behind (half-full staging pre-writes
// dirty pages to the device ahead of the checkpoint), a path overflow-on-Put
// makes unreachable in the buffered lane: a large value is stored out of line as
// volatile, memory-only frames until a checkpoint, and the leaf that names its
// chain stays small, so buffered staging never grows a half-full page vector to
// pre-write. PrewrittenPageWrites is therefore always zero here and the
// assertion can no longer fire. If volatile-frame spilling reintroduces
// device-ahead staging in the buffered lane, this coverage returns with it.

func TestFileStoreBufferedVisibleCloseCheckpoints(t *testing.T) {
	path := t.TempDir() + "/buffered-close.db"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte(`{"value":"close"}`)
	if _, err := collection.Put("key", value); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedFile, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedFile.Close()
	reopened, err := Open(reopenedFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, found, err := reopened.AppendRaw(nil, "key")
	if err != nil || !found || !bytes.Equal(got, value) {
		t.Fatalf("read after Close checkpoint = (%q, %v, %v), want %q", got, found, err, value)
	}
}

func TestFileStoreBufferedVisibleCheckpointPreservesOlderSnapshotFaults(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "buffered-snapshot-fault-*")
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	options.MaxDocumentBytes = 4 << 10
	options.MaxRetiredExtents = 1 << 15
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	// Keep the cache at its transaction-safety floor so unrelated writes must
	// evict old clean pages after the checkpoint.
	options.ResidentBytes = int64(normalized.maxTransactionBytes)
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = collection.Close()
		_ = file.Close()
	}()

	oldValue := []byte(`{"version":"old"}`)
	newValue := []byte(`{"version":"new"}`)
	if created, putErr := collection.Put("target", oldValue); putErr != nil || !created {
		t.Fatalf("initial Put = (%v, %v), want created", created, putErr)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if created, putErr := collection.Put("target", newValue); putErr != nil || created {
		t.Fatalf("replacement Put = (%v, %v), want replaced", created, putErr)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}

	evictions := collection.Stats().Evictions
	for item := 0; item < 2048 && collection.Stats().Evictions == evictions; item++ {
		key := fmt.Sprintf("pressure-%04d", item)
		if created, putErr := collection.Put(
			key, []byte(`{"payload":"cache-pressure"}`),
		); putErr != nil || !created {
			t.Fatalf("pressure Put %d = (%v, %v)", item, created, putErr)
		}
	}
	if got := collection.Stats().Evictions; got <= evictions {
		t.Fatalf("cache pressure caused no eviction: before=%d after=%d", evictions, got)
	}
	got, found, err := snapshot.AppendRaw(nil, "target")
	if err != nil || !found || !bytes.Equal(got, oldValue) {
		t.Fatalf(
			"older snapshot fault after checkpoint/eviction = (%q, %v, %v), want %q",
			got, found, err, oldValue,
		)
	}
	got, found, err = collection.AppendRaw(nil, "target")
	if err != nil || !found || !bytes.Equal(got, newValue) {
		t.Fatalf("latest read = (%q, %v, %v), want %q", got, found, err, newValue)
	}
}

func TestFileStoreBufferedVisibleFlushTakesWriterCut(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "buffered-flush-lock-*")
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = collection.Close()
		_ = file.Close()
	}()

	collection.writer.Lock()
	flushed := make(chan error, 1)
	go func() { flushed <- collection.Flush() }()
	select {
	case err := <-flushed:
		collection.writer.Unlock()
		t.Fatalf("Flush bypassed writer cut: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	collection.writer.Unlock()
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Flush did not complete after writer cut was released")
	}
}

func TestFileStoreBufferedVisibleCheckpointFailureIsSticky(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "buffered-failure-*")
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte(`{"value":"visible"}`)
	if _, err := collection.Put("key", value); err != nil {
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	checkpointErr := collection.Flush()
	if checkpointErr == nil {
		t.Fatal("checkpoint on a closed device succeeded")
	}
	if persistErr := collection.PersistenceError(); !errors.Is(checkpointErr, persistErr) {
		t.Fatalf("PersistenceError = %v, checkpoint error = %v", persistErr, checkpointErr)
	}
	got, found, err := collection.AppendRaw(nil, "key")
	if err != nil || !found || !bytes.Equal(got, value) {
		t.Fatalf("read after failed checkpoint = (%q, %v, %v), want %q", got, found, err, value)
	}
	if _, err := collection.Put("later", []byte(`{"value":2}`)); !errors.Is(err, checkpointErr) {
		t.Fatalf("Put after failed checkpoint = %v, want sticky %v", err, checkpointErr)
	}
	if err := collection.Flush(); !errors.Is(err, checkpointErr) {
		t.Fatalf("second Flush = %v, want sticky %v", err, checkpointErr)
	}
	if err := collection.Close(); !errors.Is(err, checkpointErr) {
		t.Fatalf("Close = %v, want sticky %v", err, checkpointErr)
	}
}

func TestFileStoreBufferedVisibleRejectsCanonicalMaterialization(t *testing.T) {
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	options.MaterializationDamageGranule = 512
	if _, err := options.normalized(); err == nil {
		t.Fatal("buffered-visible canonical materialization was accepted")
	}
}

func TestFileStoreCheckpointStrengthIsExplicitAndConstrained(t *testing.T) {
	zero, err := (Options{}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	if zero.CheckpointStrength != CheckpointPowerSafe {
		t.Fatalf("zero checkpoint strength = %d, want power-safe", zero.CheckpointStrength)
	}
	ordinary := testFileStoreOptions()
	ordinary.Durability = DurabilityBufferedVisible
	ordinary.Backend = BackendPortable
	ordinary.CheckpointStrength = CheckpointFilesystem
	if _, err := ordinary.normalized(); err != nil {
		t.Fatalf("explicit ordinary-filesystem checkpoint rejected: %v", err)
	}
	file, err := os.CreateTemp(t.TempDir(), "ordinary-checkpoint-*")
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Create(file, ordinary)
	if err != nil {
		t.Fatal(err)
	}
	if got := collection.Stats().CheckpointStrength; got != CheckpointFilesystem {
		t.Fatalf("reported checkpoint strength = %d, want ordinary filesystem", got)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*Options)
	}{
		{"sync durability", func(o *Options) { o.Durability = DurabilitySync }},
		{"async durability", func(o *Options) { o.Durability = DurabilityAsyncVisible }},
		{"auto backend", func(o *Options) { o.Backend = BackendAuto }},
		{"ring backend", func(o *Options) { o.Backend = BackendIOUring }},
		{"direct try", func(o *Options) { o.WriteMode = WriteDirectTry }},
		{"direct require", func(o *Options) { o.WriteMode = WriteDirectRequire }},
		{"unknown strength", func(o *Options) { o.CheckpointStrength = CheckpointFilesystem + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := ordinary
			test.mutate(&options)
			if _, err := options.normalized(); err == nil {
				t.Fatalf("options accepted: %+v", options)
			}
		})
	}
}

func TestFileStoreBufferedVisibleAutomaticallyCheckpointsStagingPressure(t *testing.T) {
	// A buffered-visible checkpoint window records one pending parent per distinct
	// routed leaf its mutations touch, capped at filePrimaryPendingParentLimit
	// (64). Touching a 65th distinct leaf with no intervening Flush is the genuine
	// staging-pressure trigger: it forces one checkpoint and accounts it as an
	// automatic checkpoint. The earlier version of this test wrote 1,024 fresh
	// keys into a virgin tree, which never spread across enough distinct leaves in
	// one window to reach the cap, so AutomaticCheckpoints stayed at zero and the
	// assertion was vacuous.
	//
	// This seeds a corpus wide enough to span well past 64 routed leaves through
	// the bulk cutover -- which does not run the Put-path pending-parent logic, so
	// the seed leaves the automatic counter at zero -- then updates one key from
	// each of more than 64 distinct leaves inside a single window. No snapshot is
	// held, so the superseded leaf frames drain and volatile-retirement pressure
	// (a separate counter) never preempts the pending-parent trigger under test.
	const corpusSize = 12_000
	built, keys, _ := buildFilePrimaryCorpus(t, corpusSize)
	options := Options{
		Backend:       BackendPortable,
		ResidentBytes: 32 << 20,
		Durability:    DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(t, built, options, "buffered-pressure.db")
	path := file.Name()
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	baseline := collection.Stats()
	if baseline.AutomaticCheckpoints != 0 {
		t.Fatalf(
			"bulk seed forced %d automatic checkpoints; the trigger must come from the update window",
			baseline.AutomaticCheckpoints,
		)
	}

	// One key per distinct routed leaf, enough to cross the 64-leaf cap. Selecting
	// by route bucket guarantees the update window touches strictly more than
	// filePrimaryPendingParentLimit distinct leaves.
	const wantLeaves = filePrimaryPendingParentLimit + 16
	state := collection.state.Load()
	seen := make(map[storeio.BucketID]struct{}, wantLeaves)
	targets := make([]string, 0, wantLeaves)
	for _, key := range keys {
		route, routeErr := collection.currentPrimaryResidentRoute(state, []byte(key))
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if _, ok := seen[route.Bucket]; ok {
			continue
		}
		seen[route.Bucket] = struct{}{}
		targets = append(targets, key)
		if len(targets) == wantLeaves {
			break
		}
	}
	if len(targets) <= filePrimaryPendingParentLimit {
		t.Fatalf(
			"seed corpus spanned only %d distinct leaves, need more than %d",
			len(targets), filePrimaryPendingParentLimit,
		)
	}

	updated := make(map[string][]byte, len(targets))
	for at, key := range targets {
		value := fmt.Appendf(nil, `{"pressure":%d,"leaf":%d}`, at, at)
		updated[key] = value
		if _, putErr := collection.Put(key, value); putErr != nil {
			t.Fatalf("update %q: %v", key, putErr)
		}
	}

	stats := collection.Stats()
	if stats.AutomaticCheckpoints <= baseline.AutomaticCheckpoints {
		t.Fatalf(
			"automatic checkpoints = %d, baseline %d; pending-parent pressure across %d leaves did not checkpoint",
			stats.AutomaticCheckpoints, baseline.AutomaticCheckpoints, len(targets),
		)
	}
	if stats.DeviceCommits <= baseline.DeviceCommits {
		t.Fatalf(
			"device commits = %d, baseline %d; the forced checkpoint never reached the device",
			stats.DeviceCommits, baseline.DeviceCommits,
		)
	}
	if stats.ResidentBytes > stats.CapacityBytes ||
		stats.DirtyBytes > stats.CapacityBytes {
		t.Fatalf(
			"cache escaped bound: resident=%d dirty=%d capacity=%d",
			stats.ResidentBytes, stats.DirtyBytes, stats.CapacityBytes,
		)
	}
	if stats.CapacityBytes != baseline.CapacityBytes ||
		stats.CommitCapacityBytes != baseline.CommitCapacityBytes {
		t.Fatalf(
			"configured memory bounds changed: cache=%d/%d commit=%d/%d",
			stats.CapacityBytes, baseline.CapacityBytes,
			stats.CommitCapacityBytes, baseline.CommitCapacityBytes,
		)
	}
	if got, want := collection.Len(), uint64(corpusSize); got != want {
		t.Fatalf("Len = %d, want %d", got, want)
	}

	// An explicit Flush is a requested checkpoint, not a pressure one: it must not
	// move the automatic counter.
	automaticCheckpoints := stats.AutomaticCheckpoints
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := collection.Stats().AutomaticCheckpoints; got != automaticCheckpoints {
		t.Fatalf(
			"explicit Flush changed automatic checkpoints from %d to %d",
			automaticCheckpoints, got,
		)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	// The pressure-forced checkpoint plus the final Flush make every update
	// durable: a reopen must see the whole corpus and every updated value byte for
	// byte.
	reopenedFile, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedFile.Close()
	reopened, err := Open(reopenedFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, want := reopened.Len(), uint64(corpusSize); got != want {
		t.Fatalf("reopened Len = %d, want %d", got, want)
	}
	var scratch []byte
	for key, value := range updated {
		got, found, rawErr := reopened.AppendRaw(scratch[:0], key)
		if rawErr != nil || !found || !bytes.Equal(got, value) {
			t.Fatalf("reopened %q = (%q, %v, %v), want %q", key, got, found, rawErr, value)
		}
		scratch = got
	}
}

func TestFileStoreBufferedVisibleSupersedesExactRetiredPagesAndReopens(t *testing.T) {
	path := t.TempDir() + "/buffered-supersession.db"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	first := []byte(`{"value":"first","pad":"aaaaaaaaaaaaaaaa"}`)
	second := []byte(`{"value":"second","pad":"bbbbbbbbbbbbbbbb"}`)
	if _, err := collection.Put("key", first); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("key", second); err != nil {
		t.Fatal(err)
	}
	if got, found, err := collection.AppendRaw(nil, "key"); err != nil ||
		!found || !bytes.Equal(got, second) {
		t.Fatalf("latest buffered value = (%q, %v, %v)", got, found, err)
	}
	// The primary graph does not cancel copy-on-write pages the way the committer's
	// SupersededPageWrites counter once measured; a hot replacement drops the
	// superseded version's volatile frames and the checkpoint retires the durable
	// extents it made unreachable. So the observable that proves the supersession
	// is the retirement table growing across the checkpoint, not a page-write
	// counter that never moves on this layout.
	beforeCheckpoint := collection.Stats()
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	afterCheckpoint := collection.Stats()
	if afterCheckpoint.PendingRetiredExtents <= beforeCheckpoint.PendingRetiredExtents ||
		afterCheckpoint.PendingRetiredBytes <= beforeCheckpoint.PendingRetiredBytes {
		t.Fatalf("checkpoint retired no superseded extents: before=%d extents/%d bytes, after=%d extents/%d bytes",
			beforeCheckpoint.PendingRetiredExtents, beforeCheckpoint.PendingRetiredBytes,
			afterCheckpoint.PendingRetiredExtents, afterCheckpoint.PendingRetiredBytes)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedFile, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedFile.Close()
	reopened, err := Open(reopenedFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, found, err := reopened.AppendRaw(nil, "key"); err != nil ||
		!found || !bytes.Equal(got, second) {
		t.Fatalf("reopened latest value = (%q, %v, %v)", got, found, err)
	}
}

func TestFileStoreBufferedVisibleSnapshotPinsRetiredQueuedPages(t *testing.T) {
	path := t.TempDir() + "/buffered-snapshot-pin.db"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = collection.Close()
		_ = file.Close()
	}()

	first := []byte(`{"value":"snapshot"}`)
	second := []byte(`{"value":"current"}`)
	if _, err := collection.Put("key", first); err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	// The snapshot pins the ordered-primary graph the first Put published; the
	// second Put copies it forward and retires that root. The pin is what this
	// test proves: the historical read below must still resolve the first value
	// from the retired root after it has been evicted, so the second Put cannot
	// have reclaimed the pages the snapshot still needs.
	oldRefs := []storeio.PageRef{snapshot.state.root.PrimaryRoot}
	if _, err := collection.Put("key", second); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	// Force the historical read to fault the generation-two pages from disk.
	// If any queued write it needs was omitted, identity/checksum validation
	// fails here instead of being hidden by dirty cache residency.
	for _, ref := range oldRefs {
		if ref != (storeio.PageRef{}) {
			collection.cache.Invalidate(ref)
		}
	}
	if got, found, err := snapshot.AppendRaw(nil, "key"); err != nil ||
		!found || !bytes.Equal(got, first) {
		t.Fatalf("evicted historical snapshot = (%q, %v, %v)", got, found, err)
	}
	if got, found, err := collection.AppendRaw(nil, "key"); err != nil ||
		!found || !bytes.Equal(got, second) {
		t.Fatalf("current value = (%q, %v, %v)", got, found, err)
	}
}

func TestFileStoreBufferedVisibleSupersessionCoversDeleteAndBatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Collection) error
	}{
		{
			name: "delete",
			mutate: func(collection *Collection) error {
				deleted, err := collection.Delete("key")
				if err == nil && !deleted {
					return errors.New("delete reported missing key")
				}
				return err
			},
		},
		{
			name: "batch",
			mutate: func(collection *Collection) error {
				return collection.Update(func(batch *WriteBatch) error {
					return batch.Put("key", []byte(`{"value":"batch"}`))
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "buffered-mutation-path-*")
			if err != nil {
				t.Fatal(err)
			}
			options := testFileStoreOptions()
			options.Durability = DurabilityBufferedVisible
			collection, err := Create(file, options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = collection.Close()
				_ = file.Close()
			}()
			if _, err := collection.Put("key", []byte(`{"value":"old"}`)); err != nil {
				t.Fatal(err)
			}
			snapshot, err := collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if err := test.mutate(collection); err != nil {
				snapshot.Close()
				t.Fatal(err)
			}
			if err := collection.Flush(); err != nil {
				snapshot.Close()
				t.Fatal(err)
			}
			if got, found, err := snapshot.AppendRaw(nil, "key"); err != nil ||
				!found || !bytes.Contains(got, []byte(`"old"`)) {
				snapshot.Close()
				t.Fatalf("%s historical read = (%q, %v, %v)", test.name, got, found, err)
			}
			snapshot.Close()
		})
	}
}

func openBufferedImage(t *testing.T, image []byte, options Options) *Collection {
	t.Helper()
	path := t.TempDir() + "/crash-image.db"
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = collection.Close()
		_ = file.Close()
	})
	return collection
}
