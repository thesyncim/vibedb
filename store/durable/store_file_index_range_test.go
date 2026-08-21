package durable

import (
	"fmt"
	"math/bits"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

func durableRangeNeedle(t testing.TB, raw string) vibejson.Index {
	t.Helper()
	src := []byte(raw)
	needed, err := vibejson.RequiredIndexEntries(src)
	if err != nil {
		t.Fatal(err)
	}
	index, err := vibejson.BuildIndex(
		src, make([]vibejson.IndexEntry, needed),
	)
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func durableRangeMaskRows(masks []store.Mask) int {
	rows := 0
	for _, mask := range masks {
		rows += bits.OnesCount64(mask.Bits)
	}
	return rows
}

func TestFileSnapshotOrderedIndexRangeTracksOverlayAndReopen(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-index-range-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.BufferCount = 1024
	options.Indexes = []store.IndexDefinition{{
		Name: "score", Paths: []string{"/score"},
	}}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for score := range 10 {
		document := fmt.Appendf(nil, `{"score":%d,"row":%d}`, score, score)
		if _, err := collection.Put(
			fmt.Appendf(nil, "k%02d", score), document,
		); err != nil {
			t.Fatal(err)
		}
	}

	three := durableRangeNeedle(t, "3")
	seven := durableRangeNeedle(t, "7")
	span := store.IndexRange{
		Lower: three, HasLower: true, LowerInclusive: false,
		Upper: seven, HasUpper: true, UpperInclusive: true,
	}
	old, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var workspace IndexWorkspace
	masks := make([]store.Mask, 0, 8)
	masks, bounded, err := old.AppendIndexRangeCandidateMasksInto(
		masks, &workspace, "score", span,
	)
	if err != nil || !bounded || durableRangeMaskRows(masks) != 4 {
		t.Fatalf("initial range = (%+v,%v,%v), rows %d", masks, bounded, err, durableRangeMaskRows(masks))
	}
	if stats := workspace.LastProbeStats(); stats.CandidateRows != 4 ||
		stats.CandidateChunks == 0 || stats.MatchedRows != 0 {
		t.Fatalf("initial range stats = %+v", stats)
	}

	// Move one base row to a term absent from the fold base. The new snapshot
	// must discover that overlay-only term, while the pinned old snapshot keeps
	// its original generation.
	if _, err := collection.Put(
		[]byte("k00"), []byte(`{"score":42,"row":0}`),
	); err != nil {
		t.Fatal(err)
	}
	forty := durableRangeNeedle(t, "40")
	overlaySpan := store.IndexRange{
		Lower: forty, HasLower: true, LowerInclusive: true,
	}
	current, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	bound, err := current.IndexProbeMemoryBound()
	if err != nil || bound.RangeWorkspaceBytes <= 0 || bound.MaskCount == 0 {
		t.Fatalf("ordered range memory bound = (%+v,%v)", bound, err)
	}
	masks, bounded, err = current.AppendIndexRangeCandidateMasksInto(
		masks[:0], &workspace, "score", overlaySpan,
	)
	if err != nil || !bounded || durableRangeMaskRows(masks) != 1 {
		t.Fatalf("overlay-only range = (%+v,%v,%v), rows %d", masks, bounded, err, durableRangeMaskRows(masks))
	}
	masks, bounded, err = old.AppendIndexRangeCandidateMasksInto(
		masks[:0], &workspace, "score", overlaySpan,
	)
	if err != nil || !bounded || durableRangeMaskRows(masks) != 0 {
		t.Fatalf("old range = (%+v,%v,%v), rows %d", masks, bounded, err, durableRangeMaskRows(masks))
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	// All endpoint, term, posting, and union buffers are workspace-owned.
	// Once warmed, the full multi-term range probe must allocate nothing.
	masks, bounded, err = current.AppendIndexRangeCandidateMasksInto(
		masks[:0], &workspace, "score", span,
	)
	if err != nil || !bounded || durableRangeMaskRows(masks) != 4 {
		t.Fatalf("warm range = (%+v,%v,%v)", masks, bounded, err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		var runErr error
		masks, bounded, runErr = current.AppendIndexRangeCandidateMasksInto(
			masks[:0], &workspace, "score", span,
		)
		if runErr != nil || !bounded || durableRangeMaskRows(masks) != 4 {
			panic("ordered range probe failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed ordered range allocated %.2f times, want 0", allocs)
	}

	if err := current.Close(); err != nil {
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
	reopenedSnapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedSnapshot.Close()
	masks, bounded, err = reopenedSnapshot.AppendIndexRangeCandidateMasksInto(
		masks[:0], &workspace, "score", overlaySpan,
	)
	if err != nil || !bounded || durableRangeMaskRows(masks) != 1 {
		t.Fatalf("reopened range = (%+v,%v,%v), rows %d", masks, bounded, err, durableRangeMaskRows(masks))
	}
}

func TestFileSnapshotOrderedIndexRangeConcurrentOverlayPublication(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-index-range-race-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = DurabilityAsyncVisible
	options.BufferCount = 1024
	options.Indexes = []store.IndexDefinition{{
		Name: "score", Paths: []string{"/score"},
	}}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	for score := range 64 {
		if _, err := collection.Put(
			fmt.Appendf(nil, "k%02d", score),
			fmt.Appendf(nil, `{"score":%d}`, score),
		); err != nil {
			t.Fatal(err)
		}
	}

	hundred := durableRangeNeedle(t, "100")
	span := store.IndexRange{
		Lower: hundred, HasLower: true, LowerInclusive: true,
	}
	writerErr := make(chan error, 1)
	go func() {
		for score := 100; score < 164; score++ {
			if _, err := collection.Put(
				[]byte("k00"), fmt.Appendf(nil, `{"score":%d}`, score),
			); err != nil {
				writerErr <- err
				return
			}
		}
		writerErr <- nil
	}()

	var workspace IndexWorkspace
	var masks []store.Mask
	for range 128 {
		snapshot, err := collection.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		masks, bounded, probeErr := snapshot.AppendIndexRangeCandidateMasksInto(
			masks[:0], &workspace, "score", span,
		)
		closeErr := snapshot.Close()
		if probeErr != nil || closeErr != nil || !bounded ||
			durableRangeMaskRows(masks) > 1 {
			t.Fatalf(
				"concurrent range = (%+v,%v,%v), close %v rows %d",
				masks, bounded, probeErr, closeErr, durableRangeMaskRows(masks),
			)
		}
	}
	if err := <-writerErr; err != nil {
		t.Fatal(err)
	}
}
