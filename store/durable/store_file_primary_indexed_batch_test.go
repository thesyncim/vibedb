package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

func primaryIndexedBatchLanes() []primaryBatchLane {
	lanes := primaryBatchLanes()
	for i := range lanes {
		lanes[i].options.Indexes = []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
		}
	}
	return lanes
}

func primaryExactSnapshotKeys(
	t *testing.T, snapshot *Snapshot, name string, value vibejson.Index,
) []string {
	t.Helper()
	masks, err := snapshot.AppendIndexMasks(nil, name, value)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, primaryExactMaskRows(masks))
	if err := snapshot.RangeMasksRaw(masks, func(key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

// TestFilePrimaryIndexedBatchAtomicPublication covers the batch shape the SQL
// transaction layer needs: inserts, replacements, and deletes spanning several
// keys publish with their exact-index postings in one snapshot cut on every
// supported durability lane. A snapshot pinned before the batch retains both
// the old rows and old postings.
func TestFilePrimaryIndexedBatchAtomicPublication(t *testing.T) {
	for _, lane := range primaryIndexedBatchLanes() {
		t.Run(lane.name, func(t *testing.T) {
			coll, file, _ := openPrimaryBatchStore(t, lane.options)
			defer coll.Close()
			defer file.Close()
			if !coll.SupportsUpdate() {
				t.Fatal("indexed deferred-canonical collection does not report Update support")
			}
			for i, country := range []string{"old", "gone", "stay"} {
				key := fmt.Sprintf("indexed-%d", i)
				doc := []byte(fmt.Sprintf(`{"country":%q,"i":%d}`, country, i))
				if _, err := coll.Put([]byte(key), doc); err != nil {
					t.Fatalf("seed %s: %v", key, err)
				}
			}
			before, err := coll.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer before.Close()
			goneBefore := primaryExactTestNeedle(t, `"gone"`)
			if got := primaryExactSnapshotKeys(t, before, "country", goneBefore); !slices.Equal(got, []string{"indexed-1"}) {
				t.Fatalf("pre-batch gone postings = %v", got)
			}

			if err := coll.Update(func(batch *WriteBatch) error {
				if err := batch.Put(
					[]byte("indexed-0"), []byte(`{"country":"new","i":10}`),
				); err != nil {
					return err
				}
				if err := batch.Delete([]byte("indexed-1")); err != nil {
					return err
				}
				if err := batch.Put(
					[]byte("indexed-3"), []byte(`{"country":"new","i":30}`),
				); err != nil {
					return err
				}
				return batch.Delete([]byte("missing"))
			}); err != nil {
				t.Fatalf("indexed Update: %v", err)
			}
			// Advance the live router and exact overlay again. The pinned snapshot
			// must continue to pair its old masks with its old rooted leaf, never
			// with either newer resident handle.
			if _, err := coll.Put(
				[]byte("indexed-4"), []byte(`{"country":"later","i":40}`),
			); err != nil {
				t.Fatalf("later publication: %v", err)
			}

			old := primaryExactTestNeedle(t, `"old"`)
			gone := goneBefore
			newCountry := primaryExactTestNeedle(t, `"new"`)
			if value, found, readErr := before.AppendRaw(nil, []byte("indexed-1")); readErr != nil || !found {
				t.Fatalf("old snapshot row indexed-1 = %q,%v,%v", value, found, readErr)
			}
			if got := primaryExactSnapshotKeys(t, before, "country", old); !slices.Equal(got, []string{"indexed-0"}) {
				t.Fatalf("old snapshot old postings = %v", got)
			}
			if got := primaryExactSnapshotKeys(t, before, "country", gone); !slices.Equal(got, []string{"indexed-1"}) {
				t.Fatalf("old snapshot gone postings = %v", got)
			}
			got := primaryExactTestKeys(t, coll, "country", newCountry)
			slices.Sort(got)
			if !slices.Equal(got, []string{"indexed-0", "indexed-3"}) {
				t.Fatalf("live new postings = %v", got)
			}
			if got := primaryExactTestKeys(t, coll, "country", old); len(got) != 0 {
				t.Fatalf("live retained old postings = %v", got)
			}
			if got := primaryExactTestKeys(t, coll, "country", gone); len(got) != 0 {
				t.Fatalf("live retained deleted postings = %v", got)
			}

			if err := coll.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			got = primaryExactTestKeys(t, coll, "country", newCountry)
			slices.Sort(got)
			if !slices.Equal(got, []string{"indexed-0", "indexed-3"}) {
				t.Fatalf("post-checkpoint new postings = %v", got)
			}
		})
	}
}

// TestFilePrimaryIndexedBatchPrepareFailureRollsBackPostings proves that index
// derivation remains on the prepare side of the publication point. A malformed
// sibling rejects both primary and posting changes and does not poison the file.
func TestFilePrimaryIndexedBatchPrepareFailureRollsBackPostings(t *testing.T) {
	for _, lane := range primaryIndexedBatchLanes() {
		t.Run(lane.name, func(t *testing.T) {
			coll, file, _ := openPrimaryBatchStore(t, lane.options)
			defer coll.Close()
			defer file.Close()
			if _, err := coll.Put(
				[]byte("indexed-base"), []byte(`{"country":"base","i":0}`),
			); err != nil {
				t.Fatal(err)
			}
			generation := coll.Generation()
			err := coll.Update(func(batch *WriteBatch) error {
				if err := batch.Put(
					[]byte("indexed-base"), []byte(`{"country":"changed","i":1}`),
				); err != nil {
					return err
				}
				return batch.Put([]byte("invalid"), []byte(`{"country":`))
			})
			if err == nil {
				t.Fatal("malformed indexed batch succeeded")
			}
			if coll.Generation() != generation {
				t.Fatalf("generation advanced from %d to %d", generation, coll.Generation())
			}
			base := primaryExactTestNeedle(t, `"base"`)
			changed := primaryExactTestNeedle(t, `"changed"`)
			if got := primaryExactTestKeys(t, coll, "country", base); !slices.Equal(got, []string{"indexed-base"}) {
				t.Fatalf("base postings after rollback = %v", got)
			}
			if got := primaryExactTestKeys(t, coll, "country", changed); len(got) != 0 {
				t.Fatalf("changed postings survived rollback = %v", got)
			}
			if err := coll.PersistenceError(); err != nil {
				t.Fatalf("prepare rejection poisoned collection: %v", err)
			}
		})
	}
}

// TestFilePrimaryIndexedBatchPressureFoldsAllTouchedLeaves forces the bounded
// posting overlay to its foreground-fold escape while one batch replaces rows
// in several primary leaves. The fresh epoch must describe the complete final
// graph, remain byte-identical to a graph rebuild, and survive Flush/reopen.
func TestFilePrimaryIndexedBatchPressureFoldsAllTouchedLeaves(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Durability:        DurabilityBufferedVisible,
		MaxBatchDocuments: 16,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
		},
	}
	docs := make(map[string][]byte, 800)
	for row := range 800 {
		key := fmt.Sprintf("k%04d", row)
		docs[key] = []byte(fmt.Sprintf(
			`{"country":"c%02d","row":%d,"pad":"abcdefghijklmnop"}`,
			row%31, row,
		))
	}
	coll := buildIndexedPrimaryFile(
		t, t.TempDir(), "indexed-batch-pressure-*", docs, options,
	)
	keys := []string{"k0000", "k0400", "k0799"}
	buckets := make(map[uint32]bool)
	for _, key := range keys {
		route, ok := coll.primaryRouter.Load().Route([]byte(key))
		if !ok {
			t.Fatalf("route %s", key)
		}
		buckets[uint32(route.Bucket)] = true
	}
	if len(buckets) < 2 {
		t.Fatalf("pressure batch routed to %d leaf; want multiple", len(buckets))
	}

	oldEpoch := coll.primaryEpoch
	// A full bump cursor is a deterministic pressure seam. No published count
	// is changed, so the installed read view remains valid while prepare is
	// forced to discard its partial rebase and build a fresh foreground epoch.
	oldEpoch.termRecordN = len(oldEpoch.termRecords)
	if err := coll.Update(func(batch *WriteBatch) error {
		for i, key := range keys {
			if err := batch.Put([]byte(key), []byte(fmt.Sprintf(
				`{"country":"pressure-%d","row":%d}`, i, i,
			))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("pressure Update: %v", err)
	}
	if coll.primaryEpoch == oldEpoch {
		t.Fatal("posting pressure did not install a fresh epoch")
	}
	if coll.primaryEpoch.termRecordCount.Load() != 0 ||
		coll.primaryEpoch.tileRecordCount.Load() != 0 {
		t.Fatalf("fresh epoch retained overlay records: term=%d tile=%d",
			coll.primaryEpoch.termRecordCount.Load(),
			coll.primaryEpoch.tileRecordCount.Load())
	}
	for i, key := range keys {
		needle := primaryExactTestNeedle(t, fmt.Sprintf(`"pressure-%d"`, i))
		if got := primaryExactTestKeys(t, coll, "country", needle); !slices.Equal(got, []string{key}) {
			t.Fatalf("pressure-%d postings = %v", i, got)
		}
	}
	assertFoldMatchesRebuild(t, coll, "indexed batch pressure")

	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	wantLeaves := make([][]byte, len(coll.primaryEpoch.exact[0].leaves))
	for i := range wantLeaves {
		wantLeaves[i] = slices.Clone(coll.primaryEpoch.exact[0].leaves[i].encoded)
	}
	file := coll.file
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if len(reopened.primaryEpoch.exact[0].leaves) != len(wantLeaves) {
		t.Fatalf("reopened exact leaves = %d, want %d",
			len(reopened.primaryEpoch.exact[0].leaves), len(wantLeaves))
	}
	for i := range wantLeaves {
		if !slices.Equal(reopened.primaryEpoch.exact[0].leaves[i].encoded, wantLeaves[i]) {
			t.Fatalf("reopened exact leaf %d changed", i)
		}
	}
	assertFoldMatchesRebuild(t, reopened, "indexed batch pressure reopen")
}

// TestFilePrimarySnapshotRouteFlipBeforeLeafAcquire pins the hybrid mask-scan
// race rule directly: a route sampled from the retained snapshot router remains
// acquirable after a newer publication flips that router row, because the
// snapshot generation lease protects the old immutable PageRef. RangeMasks then
// takes its rooted fallback and returns the same old row.
func TestFilePrimarySnapshotRouteFlipBeforeLeafAcquire(t *testing.T) {
	options := primaryIndexedBatchLanes()[0].options
	coll, file, _ := openPrimaryBatchStore(t, options)
	defer coll.Close()
	defer file.Close()
	if _, err := coll.Put(
		[]byte("route-flip"), []byte(`{"country":"before","i":1}`),
	); err != nil {
		t.Fatal(err)
	}
	snapshot, err := coll.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	needle := primaryExactTestNeedle(t, `"before"`)
	masks, err := snapshot.AppendIndexMasks(nil, "country", needle)
	if err != nil || len(masks) == 0 {
		t.Fatalf("old masks = %v, %v", masks, err)
	}
	bucket := storeio.BucketID(masks[0].Chunk >> 2)
	oldRoute, _, ok := snapshot.primaryRouter.ResolveBucketFloor(bucket)
	if !ok {
		t.Fatalf("resolve old bucket %d", bucket)
	}
	if _, err := coll.Put(
		[]byte("route-flip"), []byte(`{"country":"after","i":2}`),
	); err != nil {
		t.Fatal(err)
	}
	current, ok := coll.primaryRouter.Load().ResolveBucketID(bucket)
	if !ok || current.Ref == oldRoute.Ref {
		t.Fatalf("route did not flip: old=%+v current=%+v", oldRoute.Ref, current.Ref)
	}
	lease, err := snapshot.primaryRouter.AcquireLeaf(coll.cache, oldRoute)
	if err != nil {
		t.Fatalf("acquire snapshot route after flip: %v", err)
	}
	if _, admitted := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), coll.storeID, bucket,
	); !admitted {
		lease.Release()
		t.Fatal("old snapshot leaf no longer admitted")
	}
	lease.Release()
	if got := primaryExactSnapshotKeys(t, snapshot, "country", needle); !slices.Equal(got, []string{"route-flip"}) {
		t.Fatalf("old snapshot after route flip = %v", got)
	}
}

// TestFilePrimaryIndexedBatchSyncedUnpublishedRecovers closes the exact-index
// half of the journal publication window. The crash image is captured after one
// mixed batch record is durable but before either its primary images or posting
// rebases are published. Recovery must install the complete pair, and the next
// Open must not replay any residue.
func TestFilePrimaryIndexedBatchSyncedUnpublishedRecovers(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "state", Paths: []string{"/state"}},
	}
	coll, file, path := openPrimaryBatchStore(t, options)
	for key, document := range map[string][]byte{
		"replace": []byte(`{"state":"old","v":1}`),
		"delete":  []byte(`{"state":"gone","v":2}`),
		"keep":    []byte(`{"state":"keep","v":3}`),
	} {
		if _, err := coll.Put([]byte(key), document); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	var captured *journalCrashImage
	previous := recoveryJournalPostSyncHook
	recoveryJournalPostSyncHook = func() {
		if captured == nil {
			image := captureJournalImage(t, path)
			captured = &image
		}
	}
	err := coll.Update(func(batch *WriteBatch) error {
		if err := batch.Put(
			[]byte("replace"), []byte(`{"state":"new","v":10}`),
		); err != nil {
			return err
		}
		if err := batch.Delete([]byte("delete")); err != nil {
			return err
		}
		return batch.Put(
			[]byte("insert"), []byte(`{"state":"new","v":20}`),
		)
	})
	recoveryJournalPostSyncHook = previous
	if err != nil {
		t.Fatalf("mixed indexed Update: %v", err)
	}
	_ = coll.Close()
	_ = file.Close()
	if captured == nil {
		t.Fatal("post-sync hook did not capture indexed batch")
	}

	crashPath := filepath.Join(t.TempDir(), "indexed-synced-unpublished.vibe")
	if err := os.WriteFile(crashPath, captured.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashPath+".rjournal", captured.journal, 0o600); err != nil {
		t.Fatal(err)
	}
	assertRecovered := func(pass string) {
		reopenedFile, openErr := os.OpenFile(crashPath, os.O_RDWR, 0o600)
		if openErr != nil {
			t.Fatal(openErr)
		}
		recovered, openErr := Open(reopenedFile, options)
		if openErr != nil {
			_ = reopenedFile.Close()
			t.Fatalf("%s Open: %v", pass, openErr)
		}
		defer reopenedFile.Close()
		defer recovered.Close()
		want := map[string]string{
			"seed":    `{"v":0}`,
			"replace": `{"state":"new","v":10}`,
			"insert":  `{"state":"new","v":20}`,
			"keep":    `{"state":"keep","v":3}`,
		}
		if got := primaryStoreContent(t, recovered); !mapsEqual(got, want) {
			t.Fatalf("%s primary = %v, want %v", pass, got, want)
		}
		for term, wantKeys := range map[string][]string{
			"old": nil, "gone": nil,
			"new": {"insert", "replace"}, "keep": {"keep"},
		} {
			got := primaryExactTestKeys(
				t, recovered, "state",
				primaryExactTestNeedle(t, fmt.Sprintf("%q", term)),
			)
			slices.Sort(got)
			if !slices.Equal(got, wantKeys) {
				t.Fatalf("%s state=%s postings = %v, want %v",
					pass, term, got, wantKeys)
			}
		}
	}
	assertRecovered("first recovery")
	assertRecovered("second recovery")

	journalFile, err := os.Open(crashPath + ".rjournal")
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storeio.OpenRecoveryJournal(journalFile)
	if err != nil {
		_ = journalFile.Close()
		t.Fatal(err)
	}
	if got := journal.Cursor(); got != 0 {
		t.Fatalf("second recovery retained %d journal bytes", got)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
}

func indexedBatchCrashSeed(t *testing.T, operations int) *store.Collection {
	t.Helper()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append(
		"stable", []byte(`{"state":"base","kind":"stable"}`),
	); err != nil {
		t.Fatal(err)
	}
	for i := range operations {
		key := fmt.Sprintf("drop-%02d", i)
		document := []byte(fmt.Sprintf(
			`{"state":"base","kind":"drop","n":%d}`, i,
		))
		if err := builder.Append(key, document); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return built
}

func runPrimaryIndexedBatchCheckpointFaultPass(
	t *testing.T, options Options, fc *faultController,
	plan storeio.FaultPlan, operations int,
) (boundaries []int, contents []map[string]string,
	records []storeio.FaultCommitRecord, image []byte, faulted bool,
) {
	t.Helper()
	previous := storeCommitterFactory
	storeCommitterFactory = fc.factory()
	defer func() { storeCommitterFactory = previous }()
	fc.plan = plan
	fc.device = nil

	path := filepath.Join(t.TempDir(), "indexed-batch-checkpoint-crash.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(
		indexedBatchCrashSeed(t, operations), file, options,
	); err != nil {
		_ = file.Close()
		t.Fatalf("CreateFromPrimary: %v", err)
	}
	coll, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatalf("Open: %v", err)
	}
	device := fc.device
	if device == nil {
		_ = coll.Close()
		_ = file.Close()
		t.Fatal("fault device was not installed")
	}

	boundaries = []int{device.Commits()}
	contents = []map[string]string{snapshotCollectionContent(t, coll)}
	for i := range operations {
		state := fmt.Sprintf("step-%d", i)
		opErr := coll.Update(func(batch *WriteBatch) error {
			if err := batch.Put([]byte("stable"), []byte(fmt.Sprintf(
				`{"state":%q,"kind":"stable","n":%d}`, state, i,
			))); err != nil {
				return err
			}
			if err := batch.Delete([]byte(fmt.Sprintf("drop-%02d", i))); err != nil {
				return err
			}
			return batch.Put([]byte(fmt.Sprintf("add-%02d", i)), []byte(fmt.Sprintf(
				`{"state":%q,"kind":"insert","n":%d}`, state, i,
			)))
		})
		if opErr == nil {
			opErr = coll.Flush()
		}
		if device.Faulted() {
			break
		}
		if opErr != nil {
			_ = coll.Close()
			_ = file.Close()
			t.Fatalf("operation %d: %v", i, opErr)
		}
		boundaries = append(boundaries, device.Commits())
		contents = append(contents, snapshotCollectionContent(t, coll))
	}
	faulted = device.Faulted()
	records = device.Records()
	image, _ = os.ReadFile(path)
	_ = coll.Close()
	_ = file.Close()
	return boundaries, contents, records, image, faulted
}

func verifyIndexedBatchCrashImage(
	t *testing.T, options Options, image []byte,
	legal []map[string]string, terms []string, label string,
) string {
	t.Helper()
	outcome := verifyCrashImage(t, options, image, legal, label)
	if outcome != "recovered" {
		return outcome
	}
	path := filepath.Join(t.TempDir(), "indexed-recovered.vibe")
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	coll, err := Open(file, options)
	if err != nil {
		t.Fatalf("%s: indexed oracle reopen: %v", label, err)
	}
	defer coll.Close()
	content := snapshotCollectionContent(t, coll)
	matched := false
	for _, candidate := range legal {
		if mapsEqual(content, candidate) {
			matched = true
			break
		}
	}
	if !matched {
		t.Fatalf("%s: indexed oracle content %v is not legal %v",
			label, content, legal)
	}
	for _, term := range terms {
		want := make([]string, 0)
		fragment := fmt.Sprintf(`"state":%q`, term)
		for key, document := range content {
			if strings.Contains(document, fragment) {
				want = append(want, key)
			}
		}
		slices.Sort(want)
		got := primaryExactTestKeys(
			t, coll, "state",
			primaryExactTestNeedle(t, fmt.Sprintf("%q", term)),
		)
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("%s: state=%s postings = %v, want %v for primary %v",
				label, term, got, want, content)
		}
	}
	return outcome
}

// TestFilePrimaryIndexedBatchCheckpointCrashBoundary tears every write phase
// of an exact-index checkpoint whose final logical operation is one mixed
// insert/update/delete batch. Any successful reopen must select one legal
// primary generation and the exact posting set belonging to that same
// generation; a cross-generation primary/index pair is a test failure.
func TestFilePrimaryIndexedBatchCheckpointCrashBoundary(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		GroupLimit: 1, CommitCoalesce: 0,
		Indexes: []store.IndexDefinition{
			{Name: "state", Paths: []string{"/state"}},
		},
	}
	const operations = 3
	controller := &faultController{}
	boundaries, contents, records, _, faulted :=
		runPrimaryIndexedBatchCheckpointFaultPass(
			t, options, controller,
			storeio.FaultPlan{Phase: storeio.FaultNone}, operations,
		)
	if faulted {
		t.Fatal("clean indexed checkpoint probe faulted")
	}
	if len(boundaries) != operations+1 {
		t.Fatalf("probe boundaries = %d, want %d", len(boundaries), operations+1)
	}

	op := operations - 1
	windowLo, windowHi := boundaries[op], boundaries[op+1]
	terms := []string{"base", "step-0", "step-1", "step-2"}
	exercised := 0
	for commit := windowLo; commit < windowHi; commit++ {
		record := records[commit]
		plans := make([]storeio.FaultPlan, 0, len(record.DataPages)+4)
		for dataIndex := range record.DataPages {
			plans = append(plans, storeio.FaultPlan{
				Commit: commit, Phase: storeio.FaultAfterDataWrite,
				DataIndex: dataIndex,
			})
		}
		plans = append(plans,
			storeio.FaultPlan{Commit: commit, Phase: storeio.FaultAfterBarrier},
			storeio.FaultPlan{Commit: commit, Phase: storeio.FaultAfterRootWrite},
			storeio.FaultPlan{Commit: commit, Phase: storeio.FaultAfterFinalSync},
			storeio.FaultPlan{Commit: commit, Phase: storeio.FaultTornRoot},
		)
		for _, plan := range plans {
			_, _, _, image, didFault :=
				runPrimaryIndexedBatchCheckpointFaultPass(
					t, options, controller, plan, op+1,
				)
			if !didFault {
				continue
			}
			label := fmt.Sprintf("commit=%d phase=%d data=%d",
				plan.Commit, plan.Phase, plan.DataIndex)
			legal := expectedStates(
				commit, plan.Phase, boundaries, contents,
			)
			outcome := verifyIndexedBatchCrashImage(
				t, options, image, legal, terms, label,
			)
			if outcome == "hole" || outcome == "panic" {
				t.Fatalf("%s: indexed crash outcome %s", label, outcome)
			}
			exercised++
		}
	}
	if exercised == 0 {
		t.Fatal("no indexed checkpoint crash points exercised")
	}
}
