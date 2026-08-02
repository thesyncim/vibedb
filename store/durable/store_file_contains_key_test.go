package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

type containsKeyReader interface {
	ContainsKey([]byte) (bool, error)
	AppendRaw([]byte, []byte) ([]byte, bool, error)
}

type containsKeyFixture struct {
	collection  *Collection
	path        string
	inlineKey   []byte
	overflowKey []byte
	missingKey  []byte
	inline      []byte
	overflow    []byte
}

func containsKeyInlineDocument(version int) []byte {
	return canonicalContainsKeyDocument(
		fmt.Appendf(nil, `{"kind":"inline","version":%d}`, version),
	)
}

func containsKeyOverflowDocument(version int) []byte {
	return canonicalContainsKeyDocument(
		fmt.Appendf(nil,
			`{"kind":"overflow","version":%d,"payload":%q}`,
			version, strings.Repeat(string(rune('a'+version%26)), 8<<10),
		),
	)
}

func canonicalContainsKeyDocument(src []byte) []byte {
	out, err := vibejson.AppendCanonicalize(nil, src)
	if err != nil {
		panic(err)
	}
	return out
}

func newContainsKeyFixture(tb testing.TB, options Options) *containsKeyFixture {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "contains-key-*")
	if err != nil {
		tb.Fatal(err)
	}
	collection, err := Create(file, options)
	if err != nil {
		_ = file.Close()
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := collection.Close(); err != nil && !collection.CloseCompleted() {
			tb.Errorf("close ContainsKey fixture: %v", err)
		}
		if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			tb.Errorf("close ContainsKey fixture file: %v", err)
		}
	})

	fixture := &containsKeyFixture{
		collection:  collection,
		path:        file.Name(),
		inlineKey:   []byte("contains-inline"),
		overflowKey: []byte("contains-overflow"),
		missingKey:  []byte("contains-missing"),
		inline:      containsKeyInlineDocument(0),
		overflow:    containsKeyOverflowDocument(0),
	}
	if len(fixture.inline) > options.InlineValueBytes {
		tb.Fatalf("inline fixture length = %d, limit %d", len(fixture.inline), options.InlineValueBytes)
	}
	if len(fixture.overflow) <= options.InlineValueBytes {
		tb.Fatalf("overflow fixture length = %d, limit %d", len(fixture.overflow), options.InlineValueBytes)
	}
	if created, err := collection.Put(fixture.inlineKey, fixture.inline); err != nil || !created {
		tb.Fatalf("put inline fixture: created=%v err=%v", created, err)
	}
	if created, err := collection.Put(fixture.overflowKey, fixture.overflow); err != nil || !created {
		tb.Fatalf("put overflow fixture: created=%v err=%v", created, err)
	}
	return fixture
}

func assertContainsKeyDifferential(
	tb testing.TB,
	reader containsKeyReader,
	key []byte,
	wantFound bool,
	wantDocument []byte,
) {
	tb.Helper()
	gotFound, gotErr := reader.ContainsKey(key)
	prefix := []byte("caller-owned-prefix:")
	raw, appendFound, appendErr := reader.AppendRaw(prefix, key)
	if gotErr != nil || appendErr != nil {
		tb.Fatalf("key %q: ContainsKey err=%v AppendRaw err=%v", key, gotErr, appendErr)
	}
	if gotFound != appendFound || gotFound != wantFound {
		tb.Fatalf(
			"key %q: ContainsKey=%v AppendRaw=%v want=%v",
			key, gotFound, appendFound, wantFound,
		)
	}
	if !bytes.HasPrefix(raw, prefix) {
		tb.Fatalf("key %q: AppendRaw replaced caller prefix: %q", key, raw)
	}
	if !wantFound {
		if !bytes.Equal(raw, prefix) {
			tb.Fatalf("key %q: miss changed destination: %q", key, raw)
		}
		return
	}
	if !bytes.Equal(raw[len(prefix):], wantDocument) {
		tb.Fatalf("key %q: document mismatch", key)
	}
}

func TestFileStoreContainsKeyDifferentialMatrix(t *testing.T) {
	fixture := newContainsKeyFixture(t, testFileStoreOptions())
	snapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	readers := []struct {
		name   string
		reader containsKeyReader
	}{
		{name: "collection", reader: fixture.collection},
		{name: "snapshot", reader: snapshot},
	}
	cases := []struct {
		name     string
		key      []byte
		found    bool
		document []byte
	}{
		{name: "inline-hit", key: fixture.inlineKey, found: true, document: fixture.inline},
		{name: "overflow-hit", key: fixture.overflowKey, found: true, document: fixture.overflow},
		{name: "miss", key: fixture.missingKey},
		{name: "empty-key", key: []byte{}},
		{
			name: "oversized-key",
			key:  bytes.Repeat([]byte{'k'}, storeio.CommonPrimaryLeafMaxKeyBytes+1),
		},
	}
	for _, reader := range readers {
		t.Run(reader.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					assertContainsKeyDifferential(
						t, reader.reader, tc.key, tc.found, tc.document,
					)
				})
			}
		})
	}
}

func TestFileStoreContainsKeyPinnedSnapshotUpdateDelete(t *testing.T) {
	fixture := newContainsKeyFixture(t, testFileStoreOptions())
	old, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()

	updatedInline := containsKeyInlineDocument(1)
	updatedOverflow := containsKeyOverflowDocument(1)
	insertedKey := []byte("contains-inserted-after-snapshot")
	inserted := containsKeyInlineDocument(2)
	for _, mutation := range []struct {
		key   []byte
		value []byte
	}{
		{key: fixture.inlineKey, value: updatedInline},
		{key: fixture.overflowKey, value: updatedOverflow},
		{key: insertedKey, value: inserted},
	} {
		if _, err := fixture.collection.Put(mutation.key, mutation.value); err != nil {
			t.Fatal(err)
		}
	}

	assertContainsKeyDifferential(t, old, fixture.inlineKey, true, fixture.inline)
	assertContainsKeyDifferential(t, old, fixture.overflowKey, true, fixture.overflow)
	assertContainsKeyDifferential(t, old, insertedKey, false, nil)
	assertContainsKeyDifferential(t, fixture.collection, fixture.inlineKey, true, updatedInline)
	assertContainsKeyDifferential(t, fixture.collection, fixture.overflowKey, true, updatedOverflow)
	assertContainsKeyDifferential(t, fixture.collection, insertedKey, true, inserted)

	updated, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer updated.Close()
	for _, key := range [][]byte{fixture.inlineKey, fixture.overflowKey, insertedKey} {
		deleted, deleteErr := fixture.collection.Delete(key)
		if deleteErr != nil || !deleted {
			t.Fatalf("delete %q: deleted=%v err=%v", key, deleted, deleteErr)
		}
	}

	assertContainsKeyDifferential(t, old, fixture.inlineKey, true, fixture.inline)
	assertContainsKeyDifferential(t, old, fixture.overflowKey, true, fixture.overflow)
	assertContainsKeyDifferential(t, updated, fixture.inlineKey, true, updatedInline)
	assertContainsKeyDifferential(t, updated, fixture.overflowKey, true, updatedOverflow)
	assertContainsKeyDifferential(t, updated, insertedKey, true, inserted)
	assertContainsKeyDifferential(t, fixture.collection, fixture.inlineKey, false, nil)
	assertContainsKeyDifferential(t, fixture.collection, fixture.overflowKey, false, nil)
	assertContainsKeyDifferential(t, fixture.collection, insertedKey, false, nil)
}

func TestFileStoreContainsKeyCloseAndErrorBehavior(t *testing.T) {
	var nilCollection *Collection
	if found, err := nilCollection.ContainsKey([]byte("key")); found || !errors.Is(err, ErrClosed) {
		t.Fatalf("nil collection = %v,%v, want false,%v", found, err, ErrClosed)
	}
	var nilSnapshot *Snapshot
	if found, err := nilSnapshot.ContainsKey([]byte("key")); found || !errors.Is(err, ErrClosed) {
		t.Fatalf("nil snapshot = %v,%v, want false,%v", found, err, ErrClosed)
	}
	var zeroSnapshot Snapshot
	if found, err := zeroSnapshot.ContainsKey([]byte("key")); found || !errors.Is(err, ErrClosed) {
		t.Fatalf("zero snapshot = %v,%v, want false,%v", found, err, ErrClosed)
	}

	fixture := newContainsKeyFixture(t, testFileStoreOptions())
	snapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if found, err := snapshot.ContainsKey(fixture.inlineKey); found || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed snapshot = %v,%v, want false,%v", found, err, ErrClosed)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("repeated snapshot close: %v", err)
	}
	if err := fixture.collection.Close(); err != nil {
		t.Fatal(err)
	}
	if found, err := fixture.collection.ContainsKey(fixture.inlineKey); found || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed collection = %v,%v, want false,%v", found, err, ErrClosed)
	}
}

func TestFileStoreContainsKeyOverflowDoesNotReadPayload(t *testing.T) {
	for _, receiver := range []string{"collection", "snapshot"} {
		t.Run(receiver, func(t *testing.T) {
			options := testFileStoreOptions()
			fixture := newContainsKeyFixture(t, options)
			if err := fixture.collection.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := fixture.collection.Close(); err != nil {
				t.Fatal(err)
			}

			file, err := os.OpenFile(fixture.path, os.O_RDWR, 0o600)
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

			var reader containsKeyReader = collection
			var snapshot *Snapshot
			if receiver == "snapshot" {
				snapshot, err = collection.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				defer snapshot.Close()
				reader = snapshot
			}

			before := collection.Stats()
			found, err := reader.ContainsKey(fixture.overflowKey)
			if err != nil || !found {
				t.Fatalf("ContainsKey overflow: found=%v err=%v", found, err)
			}
			afterContains := collection.Stats()
			raw, appendFound, err := reader.AppendRaw(nil, fixture.overflowKey)
			if err != nil || !appendFound || !bytes.Equal(raw, fixture.overflow) {
				t.Fatalf("AppendRaw overflow: found=%v bytes=%d err=%v", appendFound, len(raw), err)
			}
			afterAppend := collection.Stats()
			if payloadReads := afterAppend.PageReads - afterContains.PageReads; payloadReads == 0 {
				t.Fatalf(
					"AppendRaw added no page reads after ContainsKey; before=%d contains=%d append=%d",
					before.PageReads, afterContains.PageReads, afterAppend.PageReads,
				)
			}
			if payloadBytes := afterAppend.ReadBytes - afterContains.ReadBytes; payloadBytes == 0 {
				t.Fatalf("AppendRaw added no payload read bytes after ContainsKey")
			}
		})
	}
}

func TestFileStoreContainsKeyWarmedZeroAllocations(t *testing.T) {
	fixture := newContainsKeyFixture(t, testFileStoreOptions())
	snapshot, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	readers := []struct {
		name   string
		reader containsKeyReader
	}{
		{name: "collection", reader: fixture.collection},
		{name: "snapshot", reader: snapshot},
	}
	probes := []struct {
		name  string
		key   []byte
		found bool
	}{
		{name: "inline-hit", key: fixture.inlineKey, found: true},
		{name: "overflow-hit", key: fixture.overflowKey, found: true},
		{name: "miss", key: fixture.missingKey},
	}
	for _, reader := range readers {
		for _, probe := range probes {
			t.Run(reader.name+"/"+probe.name, func(t *testing.T) {
				if found, err := reader.reader.ContainsKey(probe.key); err != nil || found != probe.found {
					t.Fatalf("warmup: found=%v err=%v", found, err)
				}
				var (
					found  bool
					runErr error
				)
				allocations := testing.AllocsPerRun(1_000, func() {
					found, runErr = reader.reader.ContainsKey(probe.key)
				})
				if runErr != nil || found != probe.found {
					t.Fatalf("measured probe: found=%v err=%v", found, runErr)
				}
				if allocations != 0 {
					t.Fatalf("warmed ContainsKey allocated %.2f times, want 0", allocations)
				}
			})
		}
	}
}

func TestFileStoreContainsKeyConcurrentPublication(t *testing.T) {
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	options.QueueSlots = 128
	options.GroupLimit = 32
	fixture := newContainsKeyFixture(t, options)
	pinned, err := fixture.collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()

	inlineValues := [][]byte{fixture.inline, containsKeyInlineDocument(1)}
	overflowValues := [][]byte{fixture.overflow, containsKeyOverflowDocument(1)}
	const (
		readerCount = 4
		readerLoops = 128
		// Overflow replacement retires a chain while pinned preserves the old
		// cut. Keep this deliberately below the fixture's retirement bound: the
		// test is a race/linearization probe, not a pressure-exhaustion test.
		writerLoops = 24
	)
	start := make(chan struct{})
	failures := make(chan error, 1)
	report := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}
	var group sync.WaitGroup
	group.Add(readerCount + 1)
	for readerID := range readerCount {
		go func() {
			defer group.Done()
			<-start
			inlineBuffer := make([]byte, 0, len(inlineValues[1]))
			overflowBuffer := make([]byte, 0, len(overflowValues[1]))
			pinnedBuffer := make([]byte, 0, len(fixture.overflow))
			for iteration := range readerLoops {
				for _, key := range [][]byte{fixture.inlineKey, fixture.overflowKey} {
					found, probeErr := fixture.collection.ContainsKey(key)
					if probeErr != nil || !found {
						report(fmt.Errorf("reader %d iteration %d live key %q: found=%v err=%v", readerID, iteration, key, found, probeErr))
						return
					}
				}
				if found, probeErr := fixture.collection.ContainsKey(fixture.missingKey); probeErr != nil || found {
					report(fmt.Errorf("reader %d iteration %d miss: found=%v err=%v", readerID, iteration, found, probeErr))
					return
				}
				inlineRaw, found, readErr := fixture.collection.AppendRaw(inlineBuffer[:0], fixture.inlineKey)
				if readErr != nil || !found || (!bytes.Equal(inlineRaw, inlineValues[0]) && !bytes.Equal(inlineRaw, inlineValues[1])) {
					report(fmt.Errorf("reader %d iteration %d inline read: found=%v err=%v", readerID, iteration, found, readErr))
					return
				}
				inlineBuffer = inlineRaw
				overflowRaw, found, readErr := fixture.collection.AppendRaw(overflowBuffer[:0], fixture.overflowKey)
				if readErr != nil || !found || (!bytes.Equal(overflowRaw, overflowValues[0]) && !bytes.Equal(overflowRaw, overflowValues[1])) {
					report(fmt.Errorf("reader %d iteration %d overflow read: found=%v bytes=%d err=%v", readerID, iteration, found, len(overflowRaw), readErr))
					return
				}
				overflowBuffer = overflowRaw
				if iteration&15 == 0 {
					found, probeErr := pinned.ContainsKey(fixture.overflowKey)
					pinnedRaw, pinnedFound, readErr := pinned.AppendRaw(pinnedBuffer[:0], fixture.overflowKey)
					if probeErr != nil || !found || readErr != nil || !pinnedFound || !bytes.Equal(pinnedRaw, fixture.overflow) {
						report(fmt.Errorf("reader %d pinned cut changed: contains=%v/%v append=%v/%v", readerID, found, probeErr, pinnedFound, readErr))
						return
					}
					pinnedBuffer = pinnedRaw
				}
			}
		}()
	}
	go func() {
		defer group.Done()
		<-start
		for iteration := range writerLoops {
			version := iteration & 1
			if created, putErr := fixture.collection.Put(fixture.inlineKey, inlineValues[version]); putErr != nil || created {
				report(fmt.Errorf("writer inline iteration %d: created=%v err=%v", iteration, created, putErr))
				return
			}
			if created, putErr := fixture.collection.Put(fixture.overflowKey, overflowValues[version]); putErr != nil || created {
				report(fmt.Errorf("writer overflow iteration %d: created=%v err=%v", iteration, created, putErr))
				return
			}
		}
	}()
	close(start)
	group.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	assertContainsKeyDifferential(t, pinned, fixture.inlineKey, true, fixture.inline)
	assertContainsKeyDifferential(t, pinned, fixture.overflowKey, true, fixture.overflow)
}

var (
	containsKeyBenchmarkFound bool
	containsKeyBenchmarkErr   error
)

func BenchmarkFileStoreContainsKey(b *testing.B) {
	fixture := newContainsKeyFixture(b, testFileStoreOptions())
	snapshot, err := fixture.collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()

	readers := []struct {
		name   string
		reader containsKeyReader
	}{
		{name: "collection", reader: fixture.collection},
		{name: "snapshot", reader: snapshot},
	}
	probes := []struct {
		name  string
		key   []byte
		found bool
	}{
		{name: "inline-hit", key: fixture.inlineKey, found: true},
		{name: "overflow-hit", key: fixture.overflowKey, found: true},
		{name: "miss", key: fixture.missingKey},
	}
	for _, reader := range readers {
		b.Run(reader.name, func(b *testing.B) {
			for _, probe := range probes {
				b.Run(probe.name, func(b *testing.B) {
					if found, err := reader.reader.ContainsKey(probe.key); err != nil || found != probe.found {
						b.Fatalf("warmup: found=%v err=%v", found, err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						containsKeyBenchmarkFound, containsKeyBenchmarkErr = reader.reader.ContainsKey(probe.key)
					}
					b.StopTimer()
					if containsKeyBenchmarkErr != nil || containsKeyBenchmarkFound != probe.found {
						b.Fatalf("measured probe: found=%v err=%v", containsKeyBenchmarkFound, containsKeyBenchmarkErr)
					}
				})
			}
		})
	}
}
