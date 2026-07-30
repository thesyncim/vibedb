package durable

import (
	"bytes"
	"errors"
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

func journalDeltaTestOptions() Options {
	o := primaryExactOverlayTestOptions("/group")
	o.CheckpointStrength = CheckpointPowerSafe
	o.RecoveryJournal = false
	o.GroupLimit = 1
	o.CommitCoalesce = 0
	return o
}

func journalDeltaGroupDoc(at, group int) []byte {
	return fmt.Appendf(nil,
		`{"id":%d,"kind":"document","group":%d,"active":%t,`+
			`"tier":"standard","region":"eu-west-1","name":"row %d"}`,
		at, group, at%3 == 0, at,
	)
}

func journalDeltaCanonical(t testing.TB, raw []byte) string {
	t.Helper()
	got, err := vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	return string(got)
}

func TestBufferedJournalDeltaUsesOverlaySizedJournal(t *testing.T) {
	ordinaryOptions := journalDeltaTestOptions()
	ordinary := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), 128, ordinaryOptions,
	)
	const wantDeltaCapacity = uint64(512) << 10
	if got := ordinary.journal.Header().Capacity; got != wantDeltaCapacity {
		t.Fatalf("ordinary delta journal capacity = %d, want %d",
			got, wantDeltaCapacity)
	}

	ackOptions := journalDeltaTestOptions()
	ackOptions.RecoveryJournal = true
	ack := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), 128, ackOptions,
	)
	if got := ack.journal.Header().Capacity; got <= wantDeltaCapacity {
		t.Fatalf("per-mutation ack journal capacity = %d, want > %d",
			got, wantDeltaCapacity)
	}
}

func cloneStringMap(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneIntMap(src map[string]int) map[string]int {
	dst := make(map[string]int, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func journalDeltaImageBeforeSync(
	t testing.TB, storePath string, priorJournal []byte,
) journalCrashImage {
	t.Helper()
	storeBytes, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatal(err)
	}
	return journalCrashImage{
		store:   storeBytes,
		journal: append([]byte(nil), priorJournal...),
	}
}

func assertJournalDeltaImage(
	t *testing.T,
	options Options,
	image journalCrashImage,
	want map[string]string,
	groups map[string]int,
	generation uint64,
	label string,
) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "recovered.vibe")
	if err := os.WriteFile(path, image.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".rjournal", image.journal, 0o600); err != nil {
		t.Fatal(err)
	}

	probed := []int{10, 11, 12, 13, 20, 21, 22, 23, 40, 41, 42, 43}
	for attempt := range 2 {
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		coll, err := Open(file, options)
		if err != nil {
			_ = file.Close()
			t.Fatalf("%s reopen %d: %v", label, attempt, err)
		}
		if got := coll.Generation(); got != generation {
			_ = coll.Close()
			_ = file.Close()
			t.Fatalf("%s reopen %d generation = %d, want %d",
				label, attempt, got, generation)
		}
		if got := primaryStoreContent(t, coll); !mapsEqual(got, want) {
			_ = coll.Close()
			_ = file.Close()
			t.Fatalf("%s reopen %d content differs: got %d rows want %d",
				label, attempt, len(got), len(want))
		}
		for _, group := range probed {
			got := primaryExactTestKeys(
				t, coll, "group",
				primaryExactTestNeedle(t, fmt.Sprintf("%d", group)),
			)
			slices.Sort(got)
			expected := oracleGroupKeys(groups, group)
			if !slices.Equal(got, expected) {
				_ = coll.Close()
				_ = file.Close()
				t.Fatalf("%s reopen %d group=%d: got %v want %v",
					label, attempt, group, got, expected)
			}
		}
		if err := coll.Close(); err != nil {
			_ = file.Close()
			t.Fatalf("%s reopen %d close: %v", label, attempt, err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestBufferedJournalDeltaCrashReplayAndClose pins the complete cheap Flush
// lifecycle. Each round captures the lost pre-sync append and the synced batch,
// reopens both twice, and checks both rows and exact postings. Flush keeps the
// physical root unchanged; Close finally folds the overlay and recycles the
// journal.
func TestBufferedJournalDeltaCrashReplayAndClose(t *testing.T) {
	const documents = 320
	options := journalDeltaTestOptions()
	getFault, restoreFault := installJournalFaultSeam(t)
	defer restoreFault()
	coll := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), documents, options,
	)
	fault := getFault()
	if fault == nil {
		t.Fatal("journal fault seam was not installed")
	}
	path := coll.file.Name()

	want := primaryStoreContent(t, coll)
	groups := make(map[string]int, documents)
	for at := range documents {
		groups[templateHeavyOverlayKey(at)] = at % 37
	}
	baseGeneration := coll.Generation()
	baseRoute, err := coll.currentPrimaryResidentRoute(
		coll.state.Load(), []byte(templateHeavyOverlayKey(20)),
	)
	if err != nil {
		t.Fatal(err)
	}

	previousPre := recoveryJournalDeltaPreSyncHook
	previousPost := recoveryJournalDeltaPostSyncHook
	defer func() {
		recoveryJournalDeltaPreSyncHook = previousPre
		recoveryJournalDeltaPostSyncHook = previousPost
	}()

	type mutation func()
	putGroup := func(at, group int) mutation {
		return func() {
			key := templateHeavyOverlayKey(at)
			raw := journalDeltaGroupDoc(at, group)
			_, putErr := coll.Put([]byte(key), raw)
			if putErr != nil {
				t.Fatalf("Put(%s): %v", key, putErr)
			}
			want[key] = journalDeltaCanonical(t, raw)
			groups[key] = group
		}
	}
	deleteKey := func(at int) mutation {
		return func() {
			key := templateHeavyOverlayKey(at)
			deleted, deleteErr := coll.Delete([]byte(key))
			if deleteErr != nil || !deleted {
				t.Fatalf("Delete(%s) = %v,%v", key, deleted, deleteErr)
			}
			delete(want, key)
			delete(groups, key)
		}
	}

	rounds := [][]mutation{
		{
			putGroup(20, 40),
			deleteKey(21),
			putGroup(21, 41), // delete then restore in one delta cut
			deleteKey(22),
		},
		{
			deleteKey(20),
			putGroup(20, 42), // restore across the second delta cut
			putGroup(21, 43),
			deleteKey(23),
		},
	}
	for round, mutations := range rounds {
		priorWant := cloneStringMap(want)
		priorGroups := cloneIntMap(groups)
		priorGeneration := coll.DurableGeneration()
		priorJournal, readErr := os.ReadFile(path + ".rjournal")
		if readErr != nil {
			t.Fatal(readErr)
		}
		for _, mutate := range mutations {
			mutate()
		}
		target := coll.Generation()
		if target != priorGeneration+uint64(len(mutations)) {
			t.Fatalf("round %d generation = %d, want %d",
				round, target, priorGeneration+uint64(len(mutations)))
		}
		if !coll.primaryUnifiedOverlay.hasPending() {
			t.Fatalf("round %d produced no unified overlay", round)
		}

		var preSync, postSync *journalCrashImage
		var preTarget, postTarget uint64
		recoveryJournalDeltaPreSyncHook = func(target uint64) {
			image := journalDeltaImageBeforeSync(t, path, priorJournal)
			preSync = &image
			preTarget = target
		}
		recoveryJournalDeltaPostSyncHook = func(target uint64) {
			image := captureJournalImage(t, path)
			postSync = &image
			postTarget = target
		}
		beforeStats := coll.Stats()
		beforeAppends, beforeSyncs := fault.Appends(), fault.Syncs()
		if err := coll.Flush(); err != nil {
			t.Fatalf("round %d delta Flush: %v", round, err)
		}
		afterStats := coll.Stats()
		if preSync == nil || postSync == nil ||
			preTarget != target || postTarget != target {
			t.Fatalf("round %d hooks pre=%v/%d post=%v/%d target=%d",
				round, preSync != nil, preTarget, postSync != nil, postTarget, target)
		}
		if afterStats.JournalDeltaCheckpoints !=
			beforeStats.JournalDeltaCheckpoints+1 ||
			afterStats.JournalDeltaRecords !=
				beforeStats.JournalDeltaRecords+uint64(len(mutations)) ||
			afterStats.JournalDeltaBytes <= beforeStats.JournalDeltaBytes {
			t.Fatalf("round %d delta stats before=%+v after=%+v",
				round, beforeStats, afterStats)
		}
		if fault.Appends() != beforeAppends+1 ||
			fault.Syncs() != beforeSyncs+1 {
			t.Fatalf("round %d journal work append %d->%d sync %d->%d",
				round, beforeAppends, fault.Appends(), beforeSyncs, fault.Syncs())
		}
		if coll.DurableGeneration() != target ||
			coll.committer.DurableGeneration() >= target {
			t.Fatalf("round %d logical/physical durable = %d/%d target=%d",
				round, coll.DurableGeneration(),
				coll.committer.DurableGeneration(), target)
		}
		if !coll.primaryUnifiedOverlay.hasPending() {
			t.Fatalf("round %d delta Flush physically folded the overlay", round)
		}
		route, routeErr := coll.currentPrimaryResidentRoute(
			coll.state.Load(), []byte(templateHeavyOverlayKey(20)),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		if route.Ref != baseRoute.Ref {
			t.Fatalf("round %d delta Flush changed physical leaf %v -> %v",
				round, baseRoute.Ref, route.Ref)
		}

		assertJournalDeltaImage(
			t, options, *preSync, priorWant, priorGroups,
			priorGeneration, fmt.Sprintf("round-%d/pre-sync", round),
		)
		assertJournalDeltaImage(
			t, options, *postSync, cloneStringMap(want), cloneIntMap(groups),
			target, fmt.Sprintf("round-%d/post-sync", round),
		)

		// The live read rule and exact overlay must match the same final state
		// without forcing Snapshot's physical fold.
		for at := range documents {
			key := templateHeavyOverlayKey(at)
			expected, expectedFound := want[key]
			got, found, readErr := coll.AppendRaw(nil, []byte(key))
			if readErr != nil || found != expectedFound ||
				expectedFound && string(got) != expected {
				t.Fatalf("round %d live %s = %q,%v,%v want %q,%v",
					round, key, got, found, readErr, expected, expectedFound)
			}
		}
		for _, group := range []int{20, 21, 22, 23, 40, 41, 42, 43} {
			got := probeKeysMidWindow(t, coll, "group", fmt.Sprintf("%d", group))
			expected := oracleGroupKeys(groups, group)
			if !slices.Equal(got, expected) {
				t.Fatalf("round %d live group=%d: got %v want %v",
					round, group, got, expected)
			}
		}

		// An idle Flush is a true no-op: no record, no barrier, no counter.
		recoveryJournalDeltaPreSyncHook = func(uint64) {
			t.Fatal("idle Flush called pre-sync hook")
		}
		recoveryJournalDeltaPostSyncHook = func(uint64) {
			t.Fatal("idle Flush called post-sync hook")
		}
		idleStats := coll.Stats()
		idleAppends, idleSyncs := fault.Appends(), fault.Syncs()
		if err := coll.Flush(); err != nil {
			t.Fatalf("round %d idle Flush: %v", round, err)
		}
		if got := coll.Stats(); got.JournalDeltaCheckpoints !=
			idleStats.JournalDeltaCheckpoints ||
			got.JournalDeltaRecords != idleStats.JournalDeltaRecords ||
			got.JournalDeltaBytes != idleStats.JournalDeltaBytes ||
			fault.Appends() != idleAppends || fault.Syncs() != idleSyncs {
			t.Fatalf("round %d idle Flush performed work", round)
		}
	}

	finalGeneration := coll.Generation()
	finalWant := cloneStringMap(want)
	finalGroups := cloneIntMap(groups)
	if err := coll.Close(); err != nil {
		t.Fatalf("Close full fold: %v", err)
	}
	if err := coll.file.Close(); err != nil {
		t.Fatal(err)
	}
	journalFile, err := os.Open(path + ".rjournal")
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storeio.OpenRecoveryJournal(journalFile)
	if err != nil {
		_ = journalFile.Close()
		t.Fatal(err)
	}
	if journal.Cursor() != 0 || journal.BaseGeneration() != finalGeneration {
		t.Fatalf("Close journal cursor/base = %d/%d want 0/%d",
			journal.Cursor(), journal.BaseGeneration(), finalGeneration)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if reopened.Generation() != finalGeneration ||
		reopened.DurableGeneration() != finalGeneration {
		t.Fatalf("post-Close generation = %d/%d want %d",
			reopened.Generation(), reopened.DurableGeneration(), finalGeneration)
	}
	if got := primaryStoreContent(t, reopened); !mapsEqual(got, finalWant) {
		t.Fatalf("post-Close content differs: got %d want %d", len(got), len(finalWant))
	}
	for _, group := range []int{20, 21, 22, 23, 40, 41, 42, 43} {
		got := primaryExactTestKeys(
			t, reopened, "group",
			primaryExactTestNeedle(t, fmt.Sprintf("%d", group)),
		)
		slices.Sort(got)
		if expected := oracleGroupKeys(finalGroups, group); !slices.Equal(got, expected) {
			t.Fatalf("post-Close group=%d: got %v want %v", group, got, expected)
		}
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if baseGeneration >= finalGeneration {
		t.Fatalf("workload did not advance generation: %d -> %d",
			baseGeneration, finalGeneration)
	}
}

// TestBufferedJournalDeltaContinuesAcrossDeviceSilentFolds mirrors the mixed
// workload's 64-mutation checkpoint cadence for more than two complete row
// overlay windows. A full overlay is materialized into a staged physical root
// without a device fence; later scheduled checkpoints must keep appending
// journal deltas because the preceding journal watermark already covers that
// staged root. Neither fold is a forced persistence boundary.
func TestBufferedJournalDeltaContinuesAcrossDeviceSilentFolds(t *testing.T) {
	const checkpointEvery = 64
	const mutations = primaryUnifiedOverlayRecords*2 + checkpointEvery*2

	options := journalDeltaTestOptions()
	coll := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), 320, options,
	)
	path := coll.file.Name()
	want := primaryStoreContent(t, coll)
	groups := make(map[string]int, 320)
	for at := range 320 {
		groups[templateHeavyOverlayKey(at)] = at % 37
	}

	keyString := templateHeavyOverlayKey(20)
	key := []byte(keyString)
	baseline := coll.Stats()
	physicalBase := coll.committer.DurableGeneration()
	stagedFolds := make(map[uint64]struct{}, 2)

	for mutation := 1; mutation <= mutations; mutation++ {
		group := 40 + mutation&1
		raw := journalDeltaGroupDoc(20, group)
		if created, err := coll.Put(key, raw); err != nil || created {
			t.Fatalf("Put %d = %v,%v", mutation, created, err)
		}
		want[keyString] = journalDeltaCanonical(t, raw)
		groups[keyString] = group
		if mutation%checkpointEvery != 0 {
			continue
		}

		before := coll.Stats()
		target := coll.Generation()
		if err := coll.Flush(); err != nil {
			t.Fatalf("scheduled Flush %d: %v", mutation/checkpointEvery, err)
		}
		after := coll.Stats()
		if after.JournalDeltaCheckpoints !=
			before.JournalDeltaCheckpoints+1 {
			t.Fatalf("Flush %d delta checkpoints = %d, want %d",
				mutation/checkpointEvery,
				after.JournalDeltaCheckpoints,
				before.JournalDeltaCheckpoints+1)
		}
		if after.JournalDeltaRecords !=
			before.JournalDeltaRecords+checkpointEvery {
			t.Fatalf("Flush %d delta records = %d, want %d",
				mutation/checkpointEvery,
				after.JournalDeltaRecords,
				before.JournalDeltaRecords+checkpointEvery)
		}
		if after.JournalDeltaFullFallbacks !=
			baseline.JournalDeltaFullFallbacks {
			t.Fatalf("Flush %d performed a full fallback: %d -> %d",
				mutation/checkpointEvery,
				baseline.JournalDeltaFullFallbacks,
				after.JournalDeltaFullFallbacks)
		}
		if after.AutomaticCheckpoints != baseline.AutomaticCheckpoints {
			t.Fatalf("Flush %d forced persistence: %d -> %d",
				mutation/checkpointEvery,
				baseline.AutomaticCheckpoints,
				after.AutomaticCheckpoints)
		}
		if got := coll.committer.DurableGeneration(); got != physicalBase {
			t.Fatalf("Flush %d physical durable = %d, want %d",
				mutation/checkpointEvery, got, physicalBase)
		}
		if coll.DurableGeneration() != target ||
			coll.journalDeltaGeneration.Load() != target {
			t.Fatalf("Flush %d logical/watermark = %d/%d, want %d",
				mutation/checkpointEvery,
				coll.DurableGeneration(),
				coll.journalDeltaGeneration.Load(), target)
		}

		published := coll.committer.PublishedGeneration()
		if published > physicalBase {
			stagedFolds[published] = struct{}{}
			base := coll.primaryCheckpointBase
			if base == nil || base.root.Generation != published ||
				published > coll.journalDeltaGeneration.Load() {
				t.Fatalf(
					"Flush %d invalid staged fold: published=%d base=%v watermark=%d",
					mutation/checkpointEvery, published, base != nil,
					coll.journalDeltaGeneration.Load(),
				)
			}
		}
	}

	finalStats := coll.Stats()
	if got := finalStats.PrimaryOverlayFolds -
		baseline.PrimaryOverlayFolds; got != 2 {
		t.Fatalf("primary overlay folds = %d, want 2", got)
	}
	if len(stagedFolds) != 2 {
		t.Fatalf("distinct staged fold generations = %d, want 2",
			len(stagedFolds))
	}
	if got, wantCheckpoints := finalStats.JournalDeltaCheckpoints-
		baseline.JournalDeltaCheckpoints,
		uint64(mutations/checkpointEvery); got != wantCheckpoints {
		t.Fatalf("delta checkpoints = %d, want %d", got, wantCheckpoints)
	}

	finalGeneration := coll.Generation()
	crashImage := captureJournalImage(t, path)
	assertJournalDeltaImage(
		t, options, crashImage, cloneStringMap(want), cloneIntMap(groups),
		finalGeneration, "device-silent-fold/crash",
	)

	if err := coll.Close(); err != nil {
		t.Fatalf("final Close: %v", err)
	}
	closedImage := captureJournalImage(t, path)
	if err := coll.file.Close(); err != nil {
		t.Fatal(err)
	}
	assertJournalDeltaImage(
		t, options, closedImage, cloneStringMap(want), cloneIntMap(groups),
		finalGeneration, "device-silent-fold/close",
	)
}

// TestBufferedJournalDeltaSnapshotKeepsPhysicalBase protects the distinction
// between the journal's logical durable watermark and the last rooted physical
// checkpoint. Snapshot materializes the first delta at the same generation;
// the following physical fold must build on that materialized root, not on the
// older root that preceded the journal-only Flush.
func TestBufferedJournalDeltaSnapshotKeepsPhysicalBase(t *testing.T) {
	options := journalDeltaTestOptions()
	coll := buildTemplateHeavyOverlayCollection(t, t.TempDir(), 128, options)
	want := primaryStoreContent(t, coll)

	keyA := templateHeavyOverlayKey(20)
	rawA := journalDeltaGroupDoc(20, 40)
	if created, err := coll.Put([]byte(keyA), rawA); err != nil || created {
		t.Fatalf("first Put = %v,%v", created, err)
	}
	want[keyA] = journalDeltaCanonical(t, rawA)
	if err := coll.Flush(); err != nil {
		t.Fatalf("delta Flush: %v", err)
	}
	firstGeneration := coll.Generation()
	if coll.DurableGeneration() != firstGeneration ||
		coll.committer.DurableGeneration() >= firstGeneration {
		t.Fatalf("logical/physical durable = %d/%d, want %d/<%d",
			coll.DurableGeneration(), coll.committer.DurableGeneration(),
			firstGeneration, firstGeneration)
	}

	snapshot, err := coll.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("snapshot Close: %v", err)
	}
	if coll.committer.PublishedGeneration() != firstGeneration {
		t.Fatalf("materialized generation = %d, want %d",
			coll.committer.PublishedGeneration(), firstGeneration)
	}

	keyB := templateHeavyOverlayKey(21)
	rawB := journalDeltaGroupDoc(21, 41)
	if created, err := coll.Put([]byte(keyB), rawB); err != nil || created {
		t.Fatalf("second Put = %v,%v", created, err)
	}
	want[keyB] = journalDeltaCanonical(t, rawB)
	if err := flushPhysicalForTest(coll); err != nil {
		t.Fatalf("physical Flush: %v", err)
	}
	if got := primaryStoreContent(t, coll); !mapsEqual(got, want) {
		t.Fatalf("same-generation materialization lost data: got %d rows want %d",
			len(got), len(want))
	}
	for group, expected := range map[int][]string{
		40: {keyA},
		41: {keyB},
	} {
		got := primaryExactTestKeys(
			t, coll, "group",
			primaryExactTestNeedle(t, fmt.Sprintf("%d", group)),
		)
		slices.Sort(got)
		if !slices.Equal(got, expected) {
			t.Fatalf("group=%d: got %v want %v", group, got, expected)
		}
	}
}

// TestBufferedJournalDeltaSyncFailureIsSticky proves the logical durable
// watermark advances only after the journal barrier. A failed barrier poisons
// the collection and every later write/checkpoint/Close returns that same error.
func TestBufferedJournalDeltaSyncFailureIsSticky(t *testing.T) {
	options := journalDeltaTestOptions()
	getFault, restoreFault := installJournalFaultSeam(t)
	defer restoreFault()
	coll := buildTemplateHeavyOverlayCollection(t, t.TempDir(), 128, options)
	fault := getFault()
	key := []byte(templateHeavyOverlayKey(20))
	if _, err := coll.Put(key, journalDeltaGroupDoc(20, 40)); err != nil {
		t.Fatal(err)
	}
	before := coll.journalDeltaGeneration.Load()
	fault.Program(storeio.JournalFaultPlan{
		Phase:     storeio.JournalFaultSyncError,
		SyncIndex: fault.Syncs(),
	})
	postCalled := false
	recoveryJournalDeltaPostSyncHook = func(uint64) { postCalled = true }
	defer func() { recoveryJournalDeltaPostSyncHook = nil }()
	flushErr := coll.Flush()
	if flushErr == nil || !fault.Faulted() {
		t.Fatalf("faulted delta Flush = %v, fired=%v", flushErr, fault.Faulted())
	}
	if postCalled {
		t.Fatal("post-sync hook ran after a failed journal barrier")
	}
	if got := coll.journalDeltaGeneration.Load(); got != before {
		t.Fatalf("failed barrier advanced delta generation %d -> %d", before, got)
	}
	if persistence := coll.PersistenceError(); !errors.Is(flushErr, persistence) {
		t.Fatalf("PersistenceError=%v, Flush=%v", persistence, flushErr)
	}
	if _, err := coll.Put(key, journalDeltaGroupDoc(20, 41)); !errors.Is(err, flushErr) {
		t.Fatalf("Put after poison = %v, want %v", err, flushErr)
	}
	if err := coll.Flush(); !errors.Is(err, flushErr) {
		t.Fatalf("second Flush = %v, want %v", err, flushErr)
	}
	if err := coll.Close(); !errors.Is(err, flushErr) {
		t.Fatalf("Close after poison = %v, want %v", err, flushErr)
	}
}

func buildJournalDeltaFallbackCollection(
	t *testing.T, options Options,
) *Collection {
	t.Helper()
	docs := make(map[string][]byte, 320)
	for at := range 320 {
		docs[templateHeavyOverlayKey(at)] = templateHeavyOverlayDoc(at)
	}
	return buildIndexedPrimaryFile(
		t, t.TempDir(), "journal-delta-fallback-*", docs, options,
	)
}

// TestBufferedJournalDeltaIneligibleMutationsPhysicallyFold ensures Flush never
// treats a partial overlay as the whole durable cut. Inserts, overflow values,
// and WriteBatch all leave physical staging or a generation gap and therefore
// take the root-fold fallback.
func TestBufferedJournalDeltaIneligibleMutationsPhysicallyFold(t *testing.T) {
	tests := []struct {
		name    string
		options func() Options
		mutate  func(*testing.T, *Collection)
	}{
		{
			name:    "growing-insert",
			options: journalDeltaTestOptions,
			mutate: func(t *testing.T, coll *Collection) {
				raw := []byte(fmt.Sprintf(
					`{"group":55,"pad":%q}`, strings.Repeat("i", 900),
				))
				created, err := coll.Put([]byte("fresh-growing-key"), raw)
				if err != nil || !created {
					t.Fatalf("insert = %v,%v", created, err)
				}
			},
		},
		{
			name: "overflow",
			options: func() Options {
				o := journalDeltaTestOptions()
				o.InlineValueBytes = 256
				o.MaxDocumentBytes = 8 << 10
				return o
			},
			mutate: func(t *testing.T, coll *Collection) {
				raw := []byte(fmt.Sprintf(
					`{"group":56,"pad":%q}`, strings.Repeat("o", 2048),
				))
				created, err := coll.Put(
					[]byte(templateHeavyOverlayKey(20)), raw,
				)
				if err != nil || created {
					t.Fatalf("overflow update = %v,%v", created, err)
				}
			},
		},
		{
			name: "batch",
			options: func() Options {
				o := journalDeltaTestOptions()
				o.Indexes = nil
				return o
			},
			mutate: func(t *testing.T, coll *Collection) {
				if err := coll.Update(func(batch *WriteBatch) error {
					if err := batch.Put(
						[]byte(templateHeavyOverlayKey(20)),
						journalDeltaGroupDoc(20, 57),
					); err != nil {
						return err
					}
					return batch.Delete([]byte(templateHeavyOverlayKey(21)))
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coll := buildJournalDeltaFallbackCollection(t, test.options())
			beforePhysical := coll.committer.DurableGeneration()
			before := coll.Stats()
			test.mutate(t, coll)
			target := coll.Generation()
			if err := coll.Flush(); err != nil {
				t.Fatal(err)
			}
			after := coll.Stats()
			if after.JournalDeltaCheckpoints != before.JournalDeltaCheckpoints {
				t.Fatalf("ineligible mutation used delta checkpoint: before=%d after=%d",
					before.JournalDeltaCheckpoints, after.JournalDeltaCheckpoints)
			}
			if coll.committer.DurableGeneration() != target ||
				coll.DurableGeneration() != target {
				t.Fatalf("physical/logical durable = %d/%d want %d",
					coll.committer.DurableGeneration(), coll.DurableGeneration(), target)
			}
			if coll.primaryUnifiedOverlay.hasPending() {
				t.Fatal("physical fallback left row overlay pending")
			}
			if after.JournalDeltaFullFallbacks == before.JournalDeltaFullFallbacks &&
				coll.committer.DurableGeneration() == beforePhysical {
				t.Fatal("ineligible mutation neither counted nor performed a physical fallback")
			}
		})
	}
}

// TestBufferedJournalDeltaOverlayPressureFullFolds fills the bounded class-5
// record window. The next mutation must physically fold/recycle before it can
// publish, rather than growing the overlay or dropping a generation.
func TestBufferedJournalDeltaOverlayPressureFullFolds(t *testing.T) {
	options := journalDeltaTestOptions()
	coll := buildJournalDeltaFallbackCollection(t, options)
	key := []byte(templateHeavyOverlayKey(20))
	first := journalDeltaGroupDoc(20, 40)
	second := journalDeltaGroupDoc(20, 41)
	before := coll.committer.DurableGeneration()
	for i := range primaryUnifiedOverlayRecords + 1 {
		value := first
		if i&1 != 0 {
			value = second
		}
		if _, err := coll.Put(key, value); err != nil {
			t.Fatalf("pressure Put %d: %v", i, err)
		}
	}
	stats := coll.Stats()
	if stats.PrimaryOverlayFolds == 0 {
		t.Fatal("overlay pressure physical fold was not counted")
	}
	if stats.AutomaticCheckpoints != 0 {
		t.Fatalf("device-silent fold counted %d automatic checkpoints",
			stats.AutomaticCheckpoints)
	}
	firstCanonical := []byte(journalDeltaCanonical(t, first))
	if got, found, err := coll.AppendRaw(nil, key); err != nil || !found ||
		!bytes.Equal(got, firstCanonical) {
		t.Fatalf("post-pressure read = %q,%v,%v want %q",
			got, found, err, firstCanonical)
	}
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	if coll.committer.DurableGeneration() <= before {
		t.Fatal("overlay pressure did not force a physical checkpoint")
	}
	if coll.DurableGeneration() != coll.Generation() {
		t.Fatalf("post-pressure Flush durable=%d generation=%d",
			coll.DurableGeneration(), coll.Generation())
	}
}

// TestBufferedJournalDeltaStructuralSplitPhysicallyFolds grows one seed tablet
// until a leaf split fires. Structural publication must reach the physical
// committer and cannot be represented as an overlay-only journal checkpoint.
func TestBufferedJournalDeltaStructuralSplitPhysicallyFolds(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("seed", []byte(`{"v":0}`)); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := journalDeltaTestOptions()
	options.InlineValueBytes = 2048
	options.MaxDocumentBytes = 2048
	path := filepath.Join(t.TempDir(), "journal-delta-split.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(source, file, options); err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = coll.Close()
		_ = file.Close()
	}()

	beforeDelta := coll.Stats().JournalDeltaCheckpoints
	for at := 0; at < 48 && coll.Stats().PrimaryLeafSplits == 0; at++ {
		if _, err := coll.Put(
			[]byte(primarySplitCrashKey(at)), primarySplitCrashValue(at),
		); err != nil {
			t.Fatalf("split Put %d: %v", at, err)
		}
	}
	if coll.Stats().PrimaryLeafSplits == 0 {
		t.Fatal("fixture did not trigger a structural split")
	}
	target := coll.Generation()
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	if coll.Stats().JournalDeltaCheckpoints != beforeDelta {
		t.Fatal("structural split was acknowledged as an overlay delta")
	}
	if coll.committer.DurableGeneration() != target {
		t.Fatalf("split physical durable=%d want %d",
			coll.committer.DurableGeneration(), target)
	}
}
