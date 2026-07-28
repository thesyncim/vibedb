package durable

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func bufferedInplaceOptions() Options {
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	options.DisableMutationCombining = true
	options.QueueSlots = 64
	options.GroupLimit = 64
	return options
}

func bufferedInplaceValue(version int) []byte {
	return []byte(fmt.Sprintf(
		`{"version":"%08d","mirror":"%08d"}`,
		version, version,
	))
}

func TestFileStoreBufferedInplaceEngagesOnSecondTouchFrameNative(t *testing.T) {
	previousZones := store.SetZonePruning(false)
	defer store.SetZonePruning(previousZones)

	file, err := os.CreateTemp(t.TempDir(), "buffered-inplace-two-pass-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, bufferedInplaceOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if _, err := collection.Put("key", bufferedInplaceValue(0)); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}

	baseline := collection.Stats().BufferedInplaceUpdates
	if _, err := collection.Put("key", bufferedInplaceValue(1)); err != nil {
		t.Fatal(err)
	}
	if got := collection.Stats().BufferedInplaceUpdates; got != baseline {
		t.Fatalf("first touch used in-place path: %d -> %d", baseline, got)
	}
	state := collection.state.Load()
	first, found, err := collection.resolveFileFingerprint(state, []byte("key"))
	if err != nil || !found {
		t.Fatalf("resolve first COW frame = (%v, %v)", found, err)
	}
	firstRef := first.documentRef
	first.Release()

	if _, err := collection.Put("key", bufferedInplaceValue(2)); err != nil {
		t.Fatal(err)
	}
	if got := collection.Stats().BufferedInplaceUpdates; got != baseline+1 {
		t.Fatalf("second touch in-place updates = %d, want %d", got, baseline+1)
	}
	state = collection.state.Load()
	second, found, err := collection.resolveFileFingerprint(state, []byte("key"))
	if err != nil || !found {
		t.Fatalf("resolve second-touch frame = (%v, %v)", found, err)
	}
	secondRef := second.documentRef
	second.Release()
	if secondRef != firstRef {
		t.Fatalf("second touch changed frame ref: first=%+v second=%+v", firstRef, secondRef)
	}

	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	lease, err := collection.cache.Acquire(secondRef)
	if err != nil {
		t.Fatal(err)
	}
	_, _, openErr := storeio.OpenPage(lease.Page())
	lease.Release()
	if openErr != nil {
		t.Fatalf("checkpoint left frame unsealed: %v", openErr)
	}
}

func TestFileStoreBufferedInplaceAcknowledgesAndCheckpointsCanonicalFrame(
	t *testing.T,
) {
	previousZones := store.SetZonePruning(false)
	defer store.SetZonePruning(previousZones)

	path := t.TempDir() + "/buffered-inplace.db"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := bufferedInplaceOptions()
	options.QueueSlots = 0
	options.GroupLimit = 0
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("key", bufferedInplaceValue(0)); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}

	baseline := collection.Stats().BufferedInplaceUpdates
	const versions = 24
	for version := 1; version <= versions; version++ {
		value := bufferedInplaceValue(version)
		if created, err := collection.Put("key", value); err != nil || created {
			t.Fatalf("Put(%d) = (%v, %v)", version, created, err)
		}
		got, found, err := collection.AppendRaw(nil, "key")
		if err != nil || !found || !bytes.Equal(got, value) {
			t.Fatalf(
				"read after acknowledgement %d = (%q, %v, %v), want %q",
				version, got, found, err, value,
			)
		}
	}
	if got := collection.Stats().BufferedInplaceUpdates - baseline; got != versions-1 {
		t.Fatalf("in-place updates = %d, want %d", got, versions-1)
	}

	// A snapshot opened after acknowledgement names the first COW physical ref.
	// Flush must reseal and write that exact canonical frame before making it
	// evictable, so a later snapshot fault still returns the acknowledged value.
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	match, found, err := collection.resolveFileFingerprint(
		snapshot.state, []byte("key"),
	)
	if err != nil || !found {
		snapshot.Close()
		t.Fatalf("resolve snapshot frame = (%v, %v)", found, err)
	}
	checkpointedRef := match.documentRef
	match.Release()
	if err := collection.Flush(); err != nil {
		snapshot.Close()
		t.Fatal(err)
	}
	if !collection.cache.Invalidate(checkpointedRef) {
		snapshot.Close()
		t.Fatal("checkpointed canonical frame was not clean and evictable")
	}
	want := bufferedInplaceValue(versions)
	got, found, err := snapshot.AppendRaw(nil, "key")
	if err != nil || !found || !bytes.Equal(got, want) {
		snapshot.Close()
		t.Fatalf("snapshot fault = (%q, %v, %v), want %q", got, found, err, want)
	}
	snapshot.Close()

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
	got, found, err = reopened.AppendRaw(nil, "key")
	if err != nil || !found || !bytes.Equal(got, want) {
		t.Fatalf("reopened value = (%q, %v, %v), want %q", got, found, err, want)
	}
}

func TestFileStoreBufferedInplaceConcurrentReadsNeverSeeTornVersion(
	t *testing.T,
) {
	previousZones := store.SetZonePruning(false)
	defer store.SetZonePruning(previousZones)

	file, err := os.CreateTemp(t.TempDir(), "buffered-inplace-race-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, bufferedInplaceOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if _, err := collection.Put("key", bufferedInplaceValue(0)); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var pause atomic.Bool
	var reading atomic.Int32
	var acknowledged atomic.Int64
	readerErr := make(chan error, 1)
	startReaders := make(chan struct{})
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			<-startReaders
			for !stop.Load() {
				if pause.Load() {
					runtime.Gosched()
					continue
				}
				reading.Add(1)
				if pause.Load() {
					reading.Add(-1)
					continue
				}
				raw, found, err := collection.AppendRaw(nil, "key")
				reading.Add(-1)
				if err != nil || !found {
					select {
					case readerErr <- fmt.Errorf("read = (%q, %v, %v)", raw, found, err):
					default:
					}
					return
				}
				version, valid := parseBufferedInplaceValue(raw)
				if !valid || int64(version) > acknowledged.Load()+1 {
					select {
					case readerErr <- fmt.Errorf(
						"torn or future value %q at acknowledged %d",
						raw, acknowledged.Load(),
					):
					default:
					}
					return
				}
				// Leave small lease-free gaps. Updates racing an active read must
				// fall back; updates entering a gap exercise the exclusive frame
				// transition while the reader goroutine remains concurrent.
				time.Sleep(time.Microsecond)
			}
		})
	}

	baseline := collection.Stats().BufferedInplaceUpdates
	if _, err := collection.Put("key", bufferedInplaceValue(1)); err != nil {
		t.Fatal(err)
	}
	acknowledged.Store(1)
	close(startReaders)
	const versions = 300
	for version := 2; version <= versions; version++ {
		// Eligibility deliberately rejects a reader already holding a pin.
		// Periodically create a bounded lease-free admission window while the
		// reader goroutines remain live, guaranteeing that this race test covers
		// both the exclusive in-place transition and the contended fallback.
		if version%10 == 0 {
			pause.Store(true)
			for reading.Load() != 0 {
				runtime.Gosched()
			}
		}
		if _, err := collection.Put("key", bufferedInplaceValue(version)); err != nil {
			stop.Store(true)
			pause.Store(false)
			readers.Wait()
			t.Fatal(err)
		}
		acknowledged.Store(int64(version))
		pause.Store(false)
		runtime.Gosched()
	}
	stop.Store(true)
	readers.Wait()
	select {
	case err := <-readerErr:
		t.Fatal(err)
	default:
	}
	if got := collection.Stats().BufferedInplaceUpdates - baseline; got == 0 {
		t.Fatal("concurrent run admitted no in-place update")
	}
}

func parseBufferedInplaceValue(raw []byte) (int, bool) {
	const prefix = `{"version":"`
	const middle = `","mirror":"`
	const suffix = `"}`
	if len(raw) != len(prefix)+8+len(middle)+8+len(suffix) ||
		string(raw[:len(prefix)]) != prefix {
		return 0, false
	}
	firstStart := len(prefix)
	middleStart := firstStart + 8
	if string(raw[middleStart:middleStart+len(middle)]) != middle {
		return 0, false
	}
	secondStart := middleStart + len(middle)
	if string(raw[secondStart+8:]) != suffix ||
		!bytes.Equal(raw[firstStart:firstStart+8], raw[secondStart:secondStart+8]) {
		return 0, false
	}
	version, err := strconv.Atoi(string(raw[firstStart : firstStart+8]))
	return version, err == nil
}

// TestFileStoreBufferedInplaceSnapshotForcesCOWFallback pins the per-frame
// observability rule. A snapshot at generation S can only observe a frame whose
// current identity was published at or before S, so it forces a copy-on-write
// fallback only for frames it can actually read. A frame first published inside
// the buffered window -- born after the newest active snapshot -- is invisible
// to every reader and stays eligible for the in-place small-update path even
// while that snapshot is held. The two subtests write both halves explicitly.
func TestFileStoreBufferedInplaceSnapshotForcesCOWFallback(t *testing.T) {
	previousZones := store.SetZonePruning(false)
	defer store.SetZonePruning(previousZones)

	t.Run("observable frame forces fallback", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "buffered-inplace-observed-*")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		collection, err := Create(file, bufferedInplaceOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer collection.Close()
		createValue := bufferedInplaceValue(1)
		firstTouchValue := bufferedInplaceValue(2)
		updateValue := bufferedInplaceValue(3)
		if _, err := collection.Put("key", createValue); err != nil {
			t.Fatal(err)
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
		// The first touch copies the value into a brand-new frame. Snapshot AFTER
		// that COW so the lease pins the exact frame a second touch would patch;
		// its birth generation equals the snapshot generation, so it is observable
		// and in-place must be refused.
		if _, err := collection.Put("key", firstTouchValue); err != nil {
			t.Fatal(err)
		}
		snapshot, err := collection.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		defer snapshot.Close()
		baseline := collection.Stats().BufferedInplaceUpdates
		if _, err := collection.Put("key", updateValue); err != nil {
			t.Fatal(err)
		}
		if got := collection.Stats().BufferedInplaceUpdates; got != baseline {
			t.Fatalf("observable frame allowed in-place update: %d -> %d", baseline, got)
		}
		got, found, err := snapshot.AppendRaw(nil, "key")
		if err != nil || !found || !bytes.Equal(got, firstTouchValue) {
			t.Fatalf("snapshot value = (%q, %v, %v), want %q", got, found, err, firstTouchValue)
		}
		got, found, err = collection.AppendRaw(nil, "key")
		if err != nil || !found || !bytes.Equal(got, updateValue) {
			t.Fatalf("current value = (%q, %v, %v), want %q", got, found, err, updateValue)
		}
	})

	t.Run("frame born after snapshot goes in-place", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "buffered-inplace-born-*")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		collection, err := Create(file, bufferedInplaceOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer collection.Close()
		observedValue := bufferedInplaceValue(1)
		firstTouchValue := bufferedInplaceValue(2)
		updateValue := bufferedInplaceValue(3)
		if _, err := collection.Put("key", observedValue); err != nil {
			t.Fatal(err)
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}
		// Snapshot BEFORE the frame's first COW in this window. It observes the
		// pre-COW frame; the first touch then rehomes the value into a frame born
		// after the lease, which the second touch may patch in place without the
		// snapshot ever seeing the mutation.
		snapshot, err := collection.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		defer snapshot.Close()
		if _, err := collection.Put("key", firstTouchValue); err != nil {
			t.Fatal(err)
		}
		baseline := collection.Stats().BufferedInplaceUpdates
		if _, err := collection.Put("key", updateValue); err != nil {
			t.Fatal(err)
		}
		if got := collection.Stats().BufferedInplaceUpdates; got != baseline+1 {
			t.Fatalf("post-snapshot frame refused in-place update: %d -> %d", baseline, got)
		}
		got, found, err := snapshot.AppendRaw(nil, "key")
		if err != nil || !found || !bytes.Equal(got, observedValue) {
			t.Fatalf("snapshot value = (%q, %v, %v), want %q", got, found, err, observedValue)
		}
		got, found, err = collection.AppendRaw(nil, "key")
		if err != nil || !found || !bytes.Equal(got, updateValue) {
			t.Fatalf("current value = (%q, %v, %v), want %q", got, found, err, updateValue)
		}
	})
}

// TestFileStoreBufferedInplaceLongLivedSnapshotKeepsEngagement holds a snapshot
// over the pre-mutation image for the entire run and drives hot same-size
// updates on keys whose frames are all born after the lease. The old
// collection-wide veto would force every one of those updates to copy-on-write;
// the per-frame rule keeps in-place engagement identical to the no-snapshot
// baseline while the snapshot keeps returning its bytes unchanged.
func TestFileStoreBufferedInplaceLongLivedSnapshotKeepsEngagement(t *testing.T) {
	previousZones := store.SetZonePruning(false)
	defer store.SetZonePruning(previousZones)

	const keys = 16
	const rounds = 64
	keyName := func(index int) string { return fmt.Sprintf("key-%03d", index) }

	// run creates keys, flushes, optionally opens a long-lived snapshot, then
	// COWs each key once (a frame born after that lease) before driving rounds of
	// same-size hot updates. It returns the in-place updates admitted during the
	// hot loop, the fallbacks it took, and any automatic checkpoints it forced.
	// Automatic capacity checkpoints periodically reset the first-touch window,
	// so a handful of hot updates re-pay a first-touch COW; that noise is
	// identical with and without the snapshot, which is the whole point.
	run := func(t *testing.T, holdSnapshot bool) (inplace, fallbacks, forced uint64) {
		t.Helper()
		file, err := os.CreateTemp(t.TempDir(), "buffered-inplace-longlived-*")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		collection, err := Create(file, bufferedInplaceOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer collection.Close()

		baseValues := make([][]byte, keys)
		for index := range keys {
			baseValues[index] = bufferedInplaceValue(0)
			if _, err := collection.Put(keyName(index), baseValues[index]); err != nil {
				t.Fatal(err)
			}
		}
		if err := collection.Flush(); err != nil {
			t.Fatal(err)
		}

		var snapshot *Snapshot
		if holdSnapshot {
			snapshot, err = collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
		}

		// First touch each key: an ordinary COW that rehomes the value into a
		// frame whose birth generation is newer than the snapshot above.
		for index := range keys {
			if _, err := collection.Put(keyName(index), bufferedInplaceValue(1)); err != nil {
				t.Fatal(err)
			}
		}

		before := collection.Stats()
		for round := 2; round < 2+rounds; round++ {
			value := bufferedInplaceValue(round)
			for index := range keys {
				if _, err := collection.Put(keyName(index), value); err != nil {
					t.Fatal(err)
				}
			}
			if !holdSnapshot {
				continue
			}
			// The long-lived lease must return the pre-mutation image byte-for-byte
			// throughout, proving the in-place patches never disturbed it.
			for index := range keys {
				got, found, err := snapshot.AppendRaw(nil, keyName(index))
				if err != nil || !found || !bytes.Equal(got, baseValues[index]) {
					t.Fatalf("snapshot key %d round %d = (%q, %v, %v), want %q",
						index, round, got, found, err, baseValues[index])
				}
			}
		}
		after := collection.Stats()
		return after.BufferedInplaceUpdates - before.BufferedInplaceUpdates,
			after.BufferedInplaceFallbacks - before.BufferedInplaceFallbacks,
			after.AutomaticCheckpoints - before.AutomaticCheckpoints
	}

	withoutInplace, withoutFallbacks, withoutForced := run(t, false)
	withInplace, withFallbacks, withForced := run(t, true)
	total := uint64(keys * rounds)
	t.Logf("hot in-place engagement over %d updates: without snapshot=%d "+
		"(fallbacks=%d, forced-cp=%d), with old snapshot=%d (fallbacks=%d, forced-cp=%d)",
		total, withoutInplace, withoutFallbacks, withoutForced,
		withInplace, withFallbacks, withForced)

	// The per-frame rule must make the held snapshot free: identical in-place
	// engagement, identical fallbacks, identical forced checkpoints. Under the
	// former collection-wide veto the with-snapshot run would have fallen back on
	// every hot update instead.
	if withInplace != withoutInplace {
		t.Fatalf("old snapshot forfeited in-place engagement: with=%d, without=%d",
			withInplace, withoutInplace)
	}
	if withFallbacks != withoutFallbacks {
		t.Fatalf("old snapshot forced extra fallbacks: with=%d, without=%d",
			withFallbacks, withoutFallbacks)
	}
	// Engagement must stay high: only the occasional post-checkpoint first-touch
	// COW is allowed to miss the in-place path.
	if withInplace*10 < total*9 {
		t.Fatalf("in-place engagement collapsed: %d of %d (<90%%)", withInplace, total)
	}
}

func TestFileStoreBufferedInplaceExtraFramePinForcesCOWFallback(t *testing.T) {
	previousZones := store.SetZonePruning(false)
	defer store.SetZonePruning(previousZones)

	file, err := os.CreateTemp(t.TempDir(), "buffered-inplace-pin-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, bufferedInplaceOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	oldValue := bufferedInplaceValue(1)
	firstTouchValue := bufferedInplaceValue(2)
	newValue := bufferedInplaceValue(3)
	if _, err := collection.Put("key", oldValue); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("key", firstTouchValue); err != nil {
		t.Fatal(err)
	}
	state := collection.state.Load()
	match, found, err := collection.resolveFileFingerprint(state, []byte("key"))
	if err != nil || !found {
		t.Fatalf("resolve = (%v, %v)", found, err)
	}
	ref := match.documentRef
	match.Release()
	extra, err := collection.cache.Acquire(ref)
	if err != nil {
		t.Fatal(err)
	}
	baseline := collection.Stats().BufferedInplaceUpdates
	if _, err := collection.Put("key", newValue); err != nil {
		extra.Release()
		t.Fatal(err)
	}
	if got := collection.Stats().BufferedInplaceUpdates; got != baseline {
		extra.Release()
		t.Fatalf("extra frame pin allowed in-place update: %d -> %d", baseline, got)
	}
	extra.Release()
	got, found, err := collection.AppendRaw(nil, "key")
	if err != nil || !found || !bytes.Equal(got, newValue) {
		t.Fatalf("current value = (%q, %v, %v), want %q", got, found, err, newValue)
	}
}

func TestFileStoreBufferedInplaceIneligibilityMatrix(t *testing.T) {
	tests := []struct {
		name       string
		zones      bool
		options    func(*Options)
		oldValue   []byte
		newValue   []byte
		createOnly bool
	}{
		{
			name:     "size change",
			oldValue: []byte(`{"v":"aaaa"}`),
			newValue: []byte(`{"v":"bbbbbbbb"}`),
		},
		{
			name:       "live mask create",
			oldValue:   []byte(`{"v":"aaaa"}`),
			newValue:   []byte(`{"v":"bbbb"}`),
			createOnly: true,
		},
		{
			name: "exact index",
			options: func(options *Options) {
				options.Indexes = []store.IndexDefinition{{
					Name: "v", Paths: []string{"/v"},
				}}
			},
			oldValue: []byte(`{"v":"aaaa"}`),
			newValue: []byte(`{"v":"bbbb"}`),
		},
		{
			name: "schema",
			options: func(options *Options) {
				options.Collection.Schema = testDurableStoreSchema(t)
			},
			oldValue: []byte(`{"id":1,"profile":{"name":"aaaa"}}`),
			newValue: []byte(`{"id":2,"profile":{"name":"bbbb"}}`),
		},
		{
			name: "float64 scan column",
			options: func(options *Options) {
				options.Float64Columns = []string{"/v"}
			},
			oldValue: []byte(`{"v":1111}`),
			newValue: []byte(`{"v":2222}`),
		},
		{
			name: "sync mode",
			options: func(options *Options) {
				options.Durability = DurabilitySync
			},
			oldValue: []byte(`{"v":"aaaa"}`),
			newValue: []byte(`{"v":"bbbb"}`),
		},
		{
			name: "async mode",
			options: func(options *Options) {
				options.Durability = DurabilityAsyncVisible
			},
			oldValue: []byte(`{"v":"aaaa"}`),
			newValue: []byte(`{"v":"bbbb"}`),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			previousZones := store.SetZonePruning(test.zones)
			defer store.SetZonePruning(previousZones)
			options := bufferedInplaceOptions()
			if test.options != nil {
				test.options(&options)
			}
			file, err := os.CreateTemp(t.TempDir(), "buffered-ineligible-*")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			collection, err := Create(file, options)
			if err != nil {
				t.Fatal(err)
			}
			defer collection.Close()
			if _, err := collection.Put("key", test.oldValue); err != nil {
				t.Fatal(err)
			}
			if err := collection.Flush(); err != nil {
				t.Fatal(err)
			}
			baseline := collection.Stats().BufferedInplaceUpdates
			key := "key"
			if test.createOnly {
				key = "new-key"
			}
			if _, err := collection.Put(key, test.newValue); err != nil {
				t.Fatal(err)
			}
			if got := collection.Stats().BufferedInplaceUpdates; got != baseline {
				t.Fatalf("ineligible mutation used in-place path: %d -> %d", baseline, got)
			}
		})
	}
}

func TestFileStoreBufferedInplaceCrashBoundary(t *testing.T) {
	previousZones := store.SetZonePruning(false)
	defer store.SetZonePruning(previousZones)

	file, err := os.CreateTemp(t.TempDir(), "buffered-inplace-crash-*")
	if err != nil {
		t.Fatal(err)
	}
	options := bufferedInplaceOptions()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = collection.Close()
		_ = file.Close()
	}()
	oldValue := bufferedInplaceValue(1)
	firstTouchValue := bufferedInplaceValue(2)
	newValue := bufferedInplaceValue(3)
	if _, err := collection.Put("key", oldValue); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("key", firstTouchValue); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("key", newValue); err != nil {
		t.Fatal(err)
	}
	if collection.Stats().BufferedInplaceUpdates == 0 {
		t.Fatal("crash test replacement did not use in-place path")
	}
	acknowledged, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(acknowledged, before) {
		t.Fatal("in-place acknowledgement changed file before checkpoint")
	}
	recoveredOld := openBufferedImage(t, acknowledged, options)
	got, found, err := recoveredOld.AppendRaw(nil, "key")
	if err != nil || !found || !bytes.Equal(got, oldValue) {
		t.Fatalf("pre-checkpoint recovery = (%q, %v, %v), want %q", got, found, err, oldValue)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	checkpointed, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	recoveredNew := openBufferedImage(t, checkpointed, options)
	got, found, err = recoveredNew.AppendRaw(nil, "key")
	if err != nil || !found || !bytes.Equal(got, newValue) {
		t.Fatalf("post-checkpoint recovery = (%q, %v, %v), want %q", got, found, err, newValue)
	}
}

// BenchmarkFileStoreBufferedInplaceAck measures the explicitly eligible second
// touch. Each first COW touch and periodic checkpoint is outside the timer.
func BenchmarkFileStoreBufferedInplaceAck(b *testing.B) {
	previousZones := store.SetZonePruning(false)
	defer store.SetZonePruning(previousZones)

	const documents = 4096
	keys := make([]string, documents)
	first := make([][]byte, documents)
	second := make([][]byte, documents)
	for index := range documents {
		keys[index] = fmt.Sprintf("key-%08d", index)
		first[index] = []byte(fmt.Sprintf(
			`{"id":"%08d","revision":"aaaaaaaa","padding":"the quick brown fox jumps over the lazy dog"}`,
			index,
		))
		second[index] = []byte(fmt.Sprintf(
			`{"id":"%08d","revision":"bbbbbbbb","padding":"the quick brown fox jumps over the lazy dog"}`,
			index,
		))
	}
	file, err := os.CreateTemp(b.TempDir(), "buffered-inplace-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	options := bufferedInplaceOptions()
	options.Collection.ChunkDocuments = 64
	options.ResidentBytes = 64 << 20
	options.QueueSlots = 1024
	options.GroupLimit = 1024
	options.CheckpointStrength = CheckpointFilesystem
	collection, err := Create(file, options)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = collection.Close()
		_ = file.Close()
	})
	for index := range documents {
		if index%64 == 63 {
			if err := collection.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := collection.Put(keys[index], first[index]); err != nil {
			b.Fatal(err)
		}
	}
	if err := collection.Flush(); err != nil {
		b.Fatal(err)
	}
	baseline := collection.Stats()
	var timedUpdates uint64
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		if iteration%64 == 63 {
			if err := collection.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		index := iteration * 4051 & (documents - 1)
		if _, err := collection.Put(keys[index], first[index]); err != nil {
			b.Fatal(err)
		}
		beforeTimed := collection.Stats().BufferedInplaceUpdates
		b.StartTimer()
		if _, err := collection.Put(keys[index], second[index]); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		timedUpdates += collection.Stats().BufferedInplaceUpdates - beforeTimed
	}
	if err := collection.Flush(); err != nil {
		b.Fatal(err)
	}
	stats := collection.Stats()
	forced := stats.AutomaticCheckpoints - baseline.AutomaticCheckpoints
	b.ReportMetric(float64(timedUpdates)/float64(b.N), "inplace/op")
	b.ReportMetric(float64(forced), "forced-cp")
	if timedUpdates != uint64(b.N) {
		b.Fatalf(
			"timed in-place updates = %d, want %d; total=%d attempts=%d fallbacks=%d automatic=%d",
			timedUpdates, b.N,
			stats.BufferedInplaceUpdates-baseline.BufferedInplaceUpdates,
			stats.BufferedInplaceAttempts-baseline.BufferedInplaceAttempts,
			stats.BufferedInplaceFallbacks-baseline.BufferedInplaceFallbacks,
			forced,
		)
	}
	if forced != 0 {
		b.Fatalf("automatic checkpoints = %d, want zero", forced)
	}
}
