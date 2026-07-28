package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
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
	options.DisableMutationCombining = true
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

func TestFileStoreBufferedPrewriteDoesNotPublishRoot(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "buffered-prewrite-*")
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	options.DisableMutationCombining = true
	options.QueueSlots = 64
	options.GroupLimit = 64
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = collection.Close()
		_ = file.Close()
	}()

	value := append([]byte(`{"payload":"`), bytes.Repeat([]byte("x"), 60<<10)...)
	value = append(value, '"', '}')
	for i := 0; i < 48 && collection.Stats().PrewrittenPageWrites == 0; i++ {
		if _, err := collection.Put(fmt.Sprintf("prewrite-%02d", i), value); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for collection.Stats().PrewrittenPageWrites == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stats := collection.Stats()
	if stats.PrewrittenPageWrites == 0 || stats.PrewrittenPageBytes == 0 {
		t.Fatalf("half-full buffered staging produced no pre-write: %+v", stats)
	}
	if stats.DurableGeneration != 1 {
		t.Fatalf("pre-write advanced durable generation to %d", stats.DurableGeneration)
	}
	image, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	recovered := openBufferedImage(t, image, options)
	defer recovered.Close()
	if got, found, err := recovered.AppendRaw(nil, "prewrite-00"); err != nil || found {
		t.Fatalf("recovery selected unpublished pre-write = (%q,%v,%v)", got, found, err)
	}
}

func TestFileStoreBufferedVisibleCloseCheckpoints(t *testing.T) {
	path := t.TempDir() + "/buffered-close.db"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	options.DisableMutationCombining = true
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
	options.DisableMutationCombining = true
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
	options.DisableMutationCombining = true
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
	options.DisableMutationCombining = true
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
	path := t.TempDir() + "/buffered-pressure.db"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend:    BackendPortable,
		Durability: DurabilityBufferedVisible,
	}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	baseline := collection.Stats()

	const (
		writers   = 16
		perWriter = 64
	)
	start := make(chan struct{})
	errs := make(chan error, writers)
	var wait sync.WaitGroup
	wait.Add(writers)
	for writer := range writers {
		writer := writer
		go func() {
			defer wait.Done()
			<-start
			for item := range perWriter {
				key := fmt.Sprintf("key-%02d-%03d", writer, item)
				value := []byte(fmt.Sprintf(`{"writer":%d,"item":%d}`, writer, item))
				if created, putErr := collection.Put(key, value); putErr != nil {
					errs <- putErr
					return
				} else if !created {
					errs <- fmt.Errorf("key %q unexpectedly replaced", key)
					return
				}
			}
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	stats := collection.Stats()
	if stats.DeviceCommits <= baseline.DeviceCommits {
		t.Fatalf(
			"device commits = %d, baseline %d; staging pressure never checkpointed",
			stats.DeviceCommits, baseline.DeviceCommits,
		)
	}
	if stats.AutomaticMutationRequests == 0 {
		t.Fatal("concurrent run did not exercise default mutation combining")
	}
	if stats.AutomaticCheckpoints <= baseline.AutomaticCheckpoints {
		t.Fatalf(
			"automatic checkpoints = %d, baseline %d; pressure checkpoint was not accounted",
			stats.AutomaticCheckpoints, baseline.AutomaticCheckpoints,
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
	if got, want := collection.Len(), uint64(writers*perWriter); got != want {
		t.Fatalf("Len = %d, want %d", got, want)
	}
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
	if got, want := reopened.Len(), uint64(writers*perWriter); got != want {
		t.Fatalf("reopened Len = %d, want %d", got, want)
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
	options.DisableMutationCombining = true
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	first := []byte(`{"value":"first","pad":"aaaaaaaaaaaaaaaa"}`)
	second := []byte(`{"value":"second","pad":"bbbbbbbbbbbbbbbb"}`)
	if _, err := collection.Put("key", first); err != nil {
		t.Fatal(err)
	}
	before := collection.Stats()
	if _, err := collection.Put("key", second); err != nil {
		t.Fatal(err)
	}
	after := collection.Stats()
	if after.SupersededPageWrites <= before.SupersededPageWrites ||
		after.SupersededPageBytes <= before.SupersededPageBytes {
		t.Fatalf("hot replacement superseded no queued pages: before=%+v after=%+v", before, after)
	}
	if got, found, err := collection.AppendRaw(nil, "key"); err != nil ||
		!found || !bytes.Equal(got, second) {
		t.Fatalf("latest buffered value = (%q, %v, %v)", got, found, err)
	}
	if err := collection.Flush(); err != nil {
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
	options.DisableMutationCombining = true
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
	match, found, err := collection.resolveFileFingerprint(
		snapshot.state, []byte("key"),
	)
	if err != nil || !found {
		t.Fatalf("resolve snapshot document = (%v, %v)", found, err)
	}
	oldDocument := match.documentRef
	match.Release()
	oldRefs := []storeio.PageRef{
		snapshot.state.keyRoot,
		snapshot.state.chunkRoot,
		snapshot.state.indexRoot,
		oldDocument,
	}
	before := collection.Stats()
	if _, err := collection.Put("key", second); err != nil {
		t.Fatal(err)
	}
	after := collection.Stats()
	if after.SupersededPageWrites != before.SupersededPageWrites {
		t.Fatalf("active snapshot allowed page supersession: before=%+v after=%+v", before, after)
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

func TestFileStoreBufferedVisibleSnapshotGateLinearizesSupersession(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "buffered-gate-race-*")
	if err != nil {
		t.Fatal(err)
	}
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	options.DisableMutationCombining = true
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = collection.Close()
		_ = file.Close()
	}()
	if _, err := collection.Put("key", []byte(`{"version":0}`)); err != nil {
		t.Fatal(err)
	}

	for version := 1; version <= 12; version++ {
		oldGeneration := collection.Generation()
		before := collection.Stats().SupersededPageWrites
		start := make(chan struct{})
		snapshotResult := make(chan *Snapshot, 1)
		snapshotErrors := make(chan error, 1)
		putErrors := make(chan error, 1)
		go func() {
			<-start
			snapshot, snapshotErr := collection.Snapshot()
			if snapshotErr != nil {
				snapshotErrors <- snapshotErr
				return
			}
			snapshotResult <- snapshot
		}()
		value := []byte(fmt.Sprintf(`{"version":%d}`, version))
		go func() {
			<-start
			_, putErr := collection.Put("key", value)
			putErrors <- putErr
		}()
		close(start)
		var snapshot *Snapshot
		select {
		case err := <-snapshotErrors:
			t.Fatal(err)
		case snapshot = <-snapshotResult:
		}
		if err := <-putErrors; err != nil {
			snapshot.Close()
			t.Fatal(err)
		}
		after := collection.Stats().SupersededPageWrites
		if snapshot.Generation() == oldGeneration && after != before {
			snapshot.Close()
			t.Fatalf(
				"iteration %d: old generation %d lease raced with %d page cancellations",
				version, oldGeneration, after-before,
			)
		}
		got, found, err := snapshot.AppendRaw(nil, "key")
		if err != nil || !found {
			snapshot.Close()
			t.Fatalf("iteration %d snapshot read = (%q, %v, %v)", version, got, found, err)
		}
		snapshot.Close()
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
			options.DisableMutationCombining = true
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
			before := collection.Stats().SupersededPageWrites
			if err := test.mutate(collection); err != nil {
				snapshot.Close()
				t.Fatal(err)
			}
			if after := collection.Stats().SupersededPageWrites; after != before {
				snapshot.Close()
				t.Fatalf("active snapshot allowed %s supersession: %d -> %d", test.name, before, after)
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
