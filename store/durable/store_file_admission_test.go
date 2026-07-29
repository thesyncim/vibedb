package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

const testAdmissionPageSize = 4096

// TestCollectionRetirementBackpressureRecovers pins the operational contract
// stated on Options.MaxRetiredExtents: a snapshot held across a write loop
// eventually fails writes, the failure names the snapshot that caused it, and
// closing that snapshot restores the writer without reopening the file.
//
// A wedge here was a real bug once. The bound is small so the loop is short.
func TestCollectionRetirementBackpressureRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pressure.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	// MaxBatchDocuments: 1 keeps the single-document transaction geometry. The
	// default batch size reserves for a whole Update, which pushes one
	// worst-case transaction past the deliberately small retirement table this
	// test needs to reach backpressure in a short loop.
	collection, err := Create(file, Options{
		Collection: store.Options{ChunkDocuments: 16}, ResidentBytes: 64 << 20,
		Backend: BackendPortable, MaxRetiredExtents: 1024, MaxBatchDocuments: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	const keys = 32
	for i := range keys {
		if _, err := collection.Put(fmt.Sprintf("key-%09d", i), benchDocument(i)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshotGeneration := snapshot.Generation()
	var failure error
	accepted := 0
	for i := range 5000 {
		if _, err := collection.Put(fmt.Sprintf("key-%09d", i%keys), benchDocument(i)); err != nil {
			failure = err
			break
		}
		accepted++
	}
	if failure == nil {
		t.Fatalf("no backpressure after %d writes under a held snapshot", accepted)
	}
	if !errors.Is(failure, storeio.ErrRetiredExtentCapacity) {
		t.Fatalf("unexpected failure: %v", failure)
	}
	// The bare sentinel names a resource, not a cause. An operator needs the
	// pinned generation to find the reader responsible.
	message := failure.Error()
	for _, want := range []string{"snapshot", "generation", "MaxRetiredExtents"} {
		if !strings.Contains(message, want) {
			t.Fatalf("failure message %q does not mention %q", message, want)
		}
	}
	if !strings.Contains(message, fmt.Sprint(snapshotGeneration)) {
		t.Fatalf("failure message %q does not name pinned generation %d", message, snapshotGeneration)
	}
	stats := collection.Stats()
	if stats.RetirementPressureCheckpoints == 0 {
		t.Fatal("retirement pressure did not force or count a checkpoint attempt")
	}
	if want := collection.Generation() - snapshotGeneration; stats.OldestSnapshotAgeGenerations != want {
		t.Fatalf("oldest snapshot age = %d, want %d",
			stats.OldestSnapshotAgeGenerations, want)
	}

	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	// Recovery must need nothing but the release: no reopen, no compaction.
	for i := range keys {
		if _, err := collection.Put(fmt.Sprintf("key-%09d", i), benchDocument(i)); err != nil {
			t.Fatalf("write %d still fails after releasing the snapshot: %v; stats=%+v",
				i, err, collection.Stats())
		}
	}
	for i := range keys {
		got, ok, err := collection.AppendRaw(nil, fmt.Sprintf("key-%09d", i))
		if err != nil || !ok {
			t.Fatalf("read %d after recovery: ok=%v err=%v", i, ok, err)
		}
		if string(got) != string(benchDocument(i)) {
			t.Fatalf("read %d returned %s", i, got)
		}
	}
}

// TestDefaultCommitBufferDepth pins the shipped commit-buffer pool to the depth
// its documented throughput table was measured at. A pool that merely exceeds
// one worst-case transaction gives a serialized writer depth one, which costs
// one durability fence per Put and was measured at 3.5 ms against 197 µs.
func TestDefaultCommitBufferDepth(t *testing.T) {
	for _, c := range []struct {
		name             string
		transactionPages int
		maxPageSize      int
		wantBuffers      int
		wantAtLeastDepth int
	}{
		{"shipped geometry", 113, 64 << 10, 512, defaultCommitDepth},
		{"small extents", 113, 4 << 10, 512, defaultCommitDepth},
		// A transaction whose own worst case already exceeds the staging budget
		// keeps the frugal depth-one pool: the budget is a cap, not a target.
		{"oversized documents", 1074, 64 << 10, 2048, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := defaultBufferCount(c.transactionPages, c.maxPageSize)
			if got != c.wantBuffers {
				t.Fatalf("defaultBufferCount(%d,%d) = %d, want %d",
					c.transactionPages, c.maxPageSize, got, c.wantBuffers)
			}
			if got <= c.transactionPages {
				t.Fatalf("pool of %d cannot hold one %d-page transaction", got, c.transactionPages)
			}
			if depth := got / (c.transactionPages + 1); depth < c.wantAtLeastDepth {
				t.Fatalf("pool of %d gives depth %d, want at least %d",
					got, depth, c.wantAtLeastDepth)
			}
		})
	}
}
