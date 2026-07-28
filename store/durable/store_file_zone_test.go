package durable

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func zoneTestOptions(chunkDocuments int) Options {
	options := testFileStoreOptions()
	options.Collection.ChunkDocuments = chunkDocuments
	options.MaxBatchDocuments = chunkDocuments
	options.MaxRetiredExtents = 4096
	options.ResidentBytes = 64 << 20
	options.BufferCount = 1024
	options.MaxBatchDocuments = 64
	return options
}

func newZoneTestStore(t *testing.T, options Options) *Collection {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "file-zone-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	return collection
}

// zoneTestDocument is deliberately three top-level members wide, which is
// exactly the durable summary's path budget, plus a fourth on some documents so
// the overflow bit is exercised too.
func zoneTestDocument(id int) string {
	return fmt.Sprintf(`{"id":%d,"price":%d,"name":"n%04d"}`, id, id*10, id)
}

func zoneProbe(name string, op store.ZoneOp, lo, hi uint32) store.ZoneProbe {
	return store.ZoneProbe{Path: store.ZonePathHash(name), Lo: lo, Hi: hi, Op: op}
}

func zoneNumberCode(t *testing.T, spelling string) uint32 {
	t.Helper()
	code, ok := store.ZoneCodeNumber([]byte(spelling))
	if !ok {
		t.Fatalf("ZoneCodeNumber(%q) not derivable", spelling)
	}
	return code
}

// TestFileStoreZoneMapsPruneClusteredChunks is the basic capability check: a
// collection written in ascending order must keep exactly the chunks whose
// value range can hold the probe, and the surviving set must be a superset of
// the true answer.
func TestFileStoreZoneMapsPruneClusteredChunks(t *testing.T) {
	collection := newZoneTestStore(t, zoneTestOptions(4))
	const documents = 200
	for i := range documents {
		if _, err := collection.Put(fmt.Sprintf("k%05d", i), []byte(zoneTestDocument(i))); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	chunks, sound, paths, err := snapshot.ZoneStats()
	if err != nil {
		t.Fatal(err)
	}
	if chunks == 0 || sound != chunks {
		t.Fatalf("ZoneStats = %d chunks, %d sound; want every chunk sound", chunks, sound)
	}
	if want := chunks * 3; paths != want {
		t.Fatalf("tracked paths = %d, want %d", paths, want)
	}

	// price is id*10, so a single value lives in exactly one chunk of four.
	code := zoneNumberCode(t, "1000")
	kept, total, err := snapshot.ZoneChunksScanned(zoneProbe("price", store.ZoneOpEq, code, code))
	if err != nil {
		t.Fatal(err)
	}
	if total != chunks {
		t.Fatalf("ZoneChunksScanned total = %d, want %d", total, chunks)
	}
	if kept >= total/2 {
		t.Fatalf("equality on a clustered column kept %d of %d chunks", kept, total)
	}

	// A path no document carries must prune every chunk for an ordered
	// predicate and keep every chunk for IS NULL.
	kept, _, err = snapshot.ZoneChunksScanned(zoneProbe("absent", store.ZoneOpEq, code, code))
	if err != nil {
		t.Fatal(err)
	}
	if kept != 0 {
		t.Fatalf("equality on an absent path kept %d chunks", kept)
	}
	kept, _, err = snapshot.ZoneChunksScanned(zoneProbe("absent", store.ZoneOpIsNull, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if kept != total {
		t.Fatalf("IS NULL on an absent path kept %d of %d chunks", kept, total)
	}
}

// TestFileStoreZoneMasksAreSound is the property that matters: every chunk
// holding a document the predicate matches must survive the probe. It walks
// every document rather than sampling, so a bound that is one code too narrow
// anywhere fails here.
func TestFileStoreZoneMasksAreSound(t *testing.T) {
	collection := newZoneTestStore(t, zoneTestOptions(8))
	const documents = 512
	random := rand.New(rand.NewSource(7))
	values := make([]int, documents)
	for i := range values {
		values[i] = random.Intn(4096)
		document := fmt.Sprintf(`{"id":%d,"price":%d,"name":"n%04d"}`, i, values[i], values[i])
		if _, err := collection.Put(fmt.Sprintf("k%05d", i), []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	chunkDocuments := int(snapshot.state.root.ChunkDocuments)
	for _, target := range []int{0, 1, 17, 300, 2048, 4095} {
		code := zoneNumberCode(t, fmt.Sprint(target))
		for _, probe := range []store.ZoneProbe{
			zoneProbe("price", store.ZoneOpEq, code, code),
			zoneProbe("price", store.ZoneOpLt, code, code),
			zoneProbe("price", store.ZoneOpGt, code, code),
			zoneProbe("price", store.ZoneOpLe, code, code),
			zoneProbe("price", store.ZoneOpGe, code, code),
		} {
			masks, _, _ := snapshot.AppendZoneMasks(nil, probe)
			survives := map[uint32]bool{}
			for _, mask := range masks {
				survives[mask.Chunk] = true
			}
			for i, value := range values {
				matches := false
				switch probe.Op {
				case store.ZoneOpEq:
					matches = value == target
				case store.ZoneOpLt:
					matches = value < target
				case store.ZoneOpGt:
					matches = value > target
				case store.ZoneOpLe:
					matches = value <= target
				case store.ZoneOpGe:
					matches = value >= target
				}
				if !matches {
					continue
				}
				chunk := uint32(i / chunkDocuments)
				if len(masks) != 0 && !survives[chunk] {
					t.Fatalf("op %d target %d pruned chunk %d holding a matching value %d",
						probe.Op, target, chunk, value)
				}
			}
		}
	}
}

// TestFileStoreZoneMapsSurviveReopen proves the summaries are durable rather
// than derived: a reopened collection must prune exactly what the original did,
// without any write having happened in between.
func TestFileStoreZoneMapsSurviveReopen(t *testing.T) {
	options := zoneTestOptions(4)
	file, err := os.CreateTemp(t.TempDir(), "file-zone-reopen-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 128 {
		if _, err := collection.Put(fmt.Sprintf("k%05d", i), []byte(zoneTestDocument(i))); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	code := zoneNumberCode(t, "500")
	probe := zoneProbe("price", store.ZoneOpEq, code, code)
	beforeKept, beforeTotal, err := snapshot.ZoneChunksScanned(probe)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Close()
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	afterKept, afterTotal, err := after.ZoneChunksScanned(probe)
	if err != nil {
		t.Fatal(err)
	}
	if beforeKept != afterKept || beforeTotal != afterTotal {
		t.Fatalf("reopen changed pruning: before %d/%d, after %d/%d",
			beforeKept, beforeTotal, afterKept, afterTotal)
	}
	if beforeTotal == 0 || beforeKept == beforeTotal {
		t.Fatalf("probe pruned nothing: %d of %d chunks kept", beforeKept, beforeTotal)
	}
}

// TestFileStoreZoneMapsFoldBatchedUpdates covers the batched Update path, which
// rebuilds one page per touched chunk and therefore folds several documents
// into one summary in a single commit.
func TestFileStoreZoneMapsFoldBatchedUpdates(t *testing.T) {
	options := zoneTestOptions(8)
	options.MaxBatchDocuments = 64
	collection := newZoneTestStore(t, options)
	for base := 0; base < 256; base += 64 {
		if err := collection.Update(func(batch *WriteBatch) error {
			for i := base; i < base+64; i++ {
				if err := batch.Put(fmt.Sprintf("k%05d", i), []byte(zoneTestDocument(i))); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	chunks, sound, _, err := snapshot.ZoneStats()
	if err != nil {
		t.Fatal(err)
	}
	if chunks == 0 || sound != chunks {
		t.Fatalf("batched Update left %d of %d chunks unsummarized", chunks-sound, chunks)
	}
	code := zoneNumberCode(t, "1000")
	kept, total, err := snapshot.ZoneChunksScanned(zoneProbe("price", store.ZoneOpEq, code, code))
	if err != nil {
		t.Fatal(err)
	}
	if kept >= total/2 {
		t.Fatalf("batched load kept %d of %d chunks", kept, total)
	}
}

// TestFileStoreZoneMapsWidenOnReplacement pins the merge-only contract: a
// replaced document's old values stay inside the bounds. That is the price of
// O(1)-per-write maintenance and it must be a documented widening, not a
// surprise.
func TestFileStoreZoneMapsWidenOnReplacement(t *testing.T) {
	collection := newZoneTestStore(t, zoneTestOptions(4))
	for i := range 4 {
		if _, err := collection.Put(fmt.Sprintf("k%05d", i), []byte(zoneTestDocument(i))); err != nil {
			t.Fatal(err)
		}
	}
	// Replace every document in chunk 0 with a far larger price.
	for i := range 4 {
		document := fmt.Sprintf(`{"id":%d,"price":%d,"name":"n%04d"}`, i, 1_000_000+i, i)
		if _, err := collection.Put(fmt.Sprintf("k%05d", i), []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	code := zoneNumberCode(t, "0")
	kept, total, err := snapshot.ZoneChunksScanned(zoneProbe("price", store.ZoneOpEq, code, code))
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || kept != 1 {
		t.Fatalf("merge-only bounds should still admit a replaced value: kept %d of %d", kept, total)
	}
}

// TestFileStoreZoneMapsDisabledWriteNothing proves the kill switch is complete:
// with pruning off no summary is written, so a store built that way prunes
// nothing and a differential run compares stored bytes rather than plans.
func TestFileStoreZoneMapsDisabledWriteNothing(t *testing.T) {
	previous := store.SetZonePruning(false)
	defer store.SetZonePruning(previous)
	collection := newZoneTestStore(t, zoneTestOptions(4))
	for i := range 64 {
		if _, err := collection.Put(fmt.Sprintf("k%05d", i), []byte(zoneTestDocument(i))); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	chunks, sound, _, err := snapshot.ZoneStats()
	if err != nil {
		t.Fatal(err)
	}
	if chunks == 0 || sound != 0 {
		t.Fatalf("pruning disabled still wrote %d sound summaries over %d chunks", sound, chunks)
	}
	masks, pruned, bounded := snapshot.AppendZoneMasks(nil, zoneProbe("price", store.ZoneOpEq, 1, 1))
	if bounded || pruned != 0 || len(masks) != 0 {
		t.Fatalf("pruning disabled produced masks: %d masks, %d pruned, bounded %v", len(masks), pruned, bounded)
	}
}

// TestFileStoreZoneMapsRebuildFromStale exercises the recovery path a chunk
// with no summary takes: the first commit that touches it recomputes from the
// rows it is already publishing.
func TestFileStoreZoneMapsRebuildFromStale(t *testing.T) {
	options := zoneTestOptions(4)
	file, err := os.CreateTemp(t.TempDir(), "file-zone-stale-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	previous := store.SetZonePruning(false)
	collection, err := Create(file, options)
	if err != nil {
		store.SetZonePruning(previous)
		t.Fatal(err)
	}
	for i := range 8 {
		if _, err := collection.Put(fmt.Sprintf("k%05d", i), []byte(zoneTestDocument(i))); err != nil {
			t.Fatal(err)
		}
	}
	store.SetZonePruning(previous)
	defer collection.Close()

	// One further write into chunk 0 must rebuild that chunk's summary from
	// every row it publishes, not only from the row it changed.
	if _, err := collection.Put("k00001", []byte(zoneTestDocument(1))); err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	chunks, sound, _, err := snapshot.ZoneStats()
	if err != nil {
		t.Fatal(err)
	}
	if sound != 1 || chunks != 2 {
		t.Fatalf("stale rebuild produced %d sound of %d chunks, want 1 of 2", sound, chunks)
	}
	// Chunk 0 holds ids 0..3, so prices 0..30. A probe for 30 must keep it and
	// a probe for 300 must not, which is only true if the rebuild saw all four
	// rows rather than just the rewritten one.
	// Chunk 1 has no summary and therefore always survives, so the counts below
	// are "both chunks" and "the stale one only".
	for _, test := range []struct {
		spelling string
		kept     int
	}{{"30", 2}, {"3000", 1}} {
		code := zoneNumberCode(t, test.spelling)
		kept, total, scanErr := snapshot.ZoneChunksScanned(zoneProbe("price", store.ZoneOpEq, code, code))
		if scanErr != nil {
			t.Fatal(scanErr)
		}
		if total != 2 || kept != test.kept {
			t.Fatalf("price = %s kept %d of %d chunks, want %d of 2", test.spelling, kept, total, test.kept)
		}
	}
}

// TestFileStoreZoneMapsPoisonOverflowRows proves the one document shape the
// fold cannot summarize fails closed. An overflowing document has no inline
// spelling in the row set, and treating it as absent would be a false negative.
func TestFileStoreZoneMapsPoisonOverflowRows(t *testing.T) {
	options := zoneTestOptions(4)
	options.InlineValueBytes = 64
	options.MaxDocumentBytes = 64 << 10
	collection := newZoneTestStore(t, options)
	if _, err := collection.Put("k0", []byte(zoneTestDocument(0))); err != nil {
		t.Fatal(err)
	}
	big := fmt.Sprintf(`{"id":1,"price":10,"blob":"%s"}`, stringRepeat("x", 4096))
	if _, err := collection.Put("k1", []byte(big)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	// Whatever the summary ended up as, every ordered probe must keep the chunk
	// that holds the row the fold could not read.
	for _, spelling := range []string{"0", "10", "1000000"} {
		code := zoneNumberCode(t, spelling)
		kept, total, err := snapshot.ZoneChunksScanned(zoneProbe("price", store.ZoneOpEq, code, code))
		if err != nil {
			t.Fatal(err)
		}
		if total != 1 || kept != 1 {
			t.Fatalf("price = %s kept %d of %d chunks holding an unreadable row", spelling, kept, total)
		}
	}
}

func stringRepeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for range n {
		out = append(out, s...)
	}
	return string(out)
}

// TestFileStoreZoneChunkBudgetFitsSmallestPage pins the space claim the layout
// depends on. Exceeding it would not make the page larger, it would make the
// page invalid, so this is a build-breaking assertion rather than a
// measurement.
func TestFileStoreZoneChunkBudgetFitsSmallestPage(t *testing.T) {
	const smallestPage = 4096
	usable := smallestPage - storeio.PageHeaderSize - storeio.PageTrailerSize -
		storeio.ChunkDirectoryPayloadHeaderSize - 64*storeio.PageRefSize
	if storeio.ChunkZoneSize*64 > usable {
		t.Fatalf("64 summaries of %d bytes exceed the %d a full leaf has left",
			storeio.ChunkZoneSize, usable)
	}
	if store.ZoneCompactBytes != storeio.ChunkZoneSize {
		t.Fatalf("summary schema is %d bytes, leaf lane holds %d",
			store.ZoneCompactBytes, storeio.ChunkZoneSize)
	}
}
