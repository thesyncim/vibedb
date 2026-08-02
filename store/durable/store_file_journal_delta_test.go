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

// The journal-delta corpus toggles one exact-indexed scalar. Every replacement
// can consume two exact term records (remove old, add new), so its device-silent
// fold cadence is bounded by both the primary record overlay and the exact term
// record overlay. Keep pressure tests tied to those production constants rather
// than assuming the primary overlay is always the first arena exhausted.
const journalDeltaTestPressureMutations = min(
	primaryUnifiedOverlayRecords,
	primaryExactOverlayTermRecordCap/2,
)

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

func TestBufferedJournalDeltaOverlayFoldTransactionGeometry(t *testing.T) {
	options := journalDeltaTestOptions()
	options.ResidentBytes = 64 << 20
	options.MaxBatchDocuments = 1
	options.MaxDocumentBytes = 1 << 10
	options.BufferCount = 1024
	options.QueueSlots = 1024
	normalized, err := options.normalized()
	if err != nil {
		t.Fatalf("competitive geometry: %v", err)
	}

	const parentLevels = 4
	overlayBuckets := normalized.primaryUnifiedOverlayBuckets
	if overlayBuckets <= 64 || overlayBuckets >= primaryUnifiedOverlayBuckets {
		t.Fatalf("explicit 1024-buffer overlay buckets = %d, want adaptive (64, %d)",
			overlayBuckets, primaryUnifiedOverlayBuckets)
	}
	metadataPages := parentLevels*overlayBuckets + 2 +
		normalized.freeFoldLimit + storeio.FreeLogMaxIndexPages +
		storeio.FreeLogMaxDeltaPages
	corePages := overlayBuckets + metadataPages
	// One exact-index catalog/root plus at least the admitted dirty-leaf
	// allowance must coexist with the complete primary/free-log fold.
	minExactPages := fileStoreExactStagePagesMin +
		len(normalized.indexes) + 1
	if got, want := normalized.maxTransactionPages,
		corePages+minExactPages; got < want {
		t.Fatalf("max transaction pages = %d, want at least overlay %d + exact %d = %d",
			got, corePages, minExactPages, want)
	}
	coreBytes :=
		uint64(overlayBuckets)*
			uint64(normalized.MaxPageSize) +
			uint64(metadataPages)*uint64(normalized.PageSize)
	minBytes := coreBytes +
		uint64(minExactPages)*uint64(normalized.MaxPageSize)
	if got := normalized.maxTransactionBytes; got < minBytes {
		t.Fatalf("max transaction bytes = %d, want at least %d", got, minBytes)
	}
	if normalized.BufferCount != 1024 ||
		normalized.BufferCount <= normalized.maxTransactionPages {
		t.Fatalf("competitive BufferCount=%d cannot hold %d-page overlay fold",
			normalized.BufferCount, normalized.maxTransactionPages)
	}
}

func unindexedBufferedOverlayGeometryOptions() Options {
	return Options{
		ResidentBytes:      64 << 20,
		MaxBatchDocuments:  1,
		MaxDocumentBytes:   1 << 10,
		BufferCount:        8192,
		QueueSlots:         128,
		Durability:         DurabilityBufferedVisible,
		Backend:            BackendPortable,
		CheckpointStrength: CheckpointFilesystem,
	}
}

func TestBufferedJournalDeltaEntryScratchEligibility(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Options)
		want   int
	}{
		{name: "ordinary buffered delta", want: primaryUnifiedOverlayRecords},
		{
			name: "async visible",
			change: func(options *Options) {
				options.Durability = DurabilityAsyncVisible
				options.CheckpointStrength = CheckpointPowerSafe
			},
		},
		{
			name: "per mutation recovery journal",
			change: func(options *Options) {
				options.RecoveryJournal = true
			},
		},
		{
			name: "overlay disabled",
			change: func(options *Options) {
				options.QueueSlots = 1 << 16
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := unindexedBufferedOverlayGeometryOptions()
			if test.change != nil {
				test.change(&options)
			}
			normalized, err := options.normalized()
			if err != nil {
				t.Fatal(err)
			}
			scratch := newBufferedJournalDeltaEntryScratch(normalized)
			if len(scratch) != test.want || cap(scratch) != test.want {
				t.Fatalf("journal delta scratch len/cap = %d/%d, want %d/%d",
					len(scratch), cap(scratch), test.want, test.want)
			}
			if test.want == 0 && scratch != nil {
				t.Fatal("ineligible journal delta scratch is non-nil")
			}
		})
	}
}

func TestPrimaryUnifiedOverlayAdaptiveBucketGeometry(t *testing.T) {
	t.Run("competitive maximum", func(t *testing.T) {
		options := unindexedBufferedOverlayGeometryOptions()
		normalized, err := options.normalized()
		if err != nil {
			t.Fatal(err)
		}
		if got := normalized.primaryUnifiedOverlayBuckets; got != primaryUnifiedOverlayBuckets {
			t.Fatalf("overlay buckets = %d, want compile maximum %d",
				got, primaryUnifiedOverlayBuckets)
		}
		if normalized.maxTransactionPages >= normalized.BufferCount {
			t.Fatalf("max transaction pages = %d, BufferCount = %d",
				normalized.maxTransactionPages, normalized.BufferCount)
		}
		if got := fileVisibilitySlots(normalized.QueueSlots) *
			normalized.maxTransactionPages; got > storeio.MaxCommitDescriptors {
			t.Fatalf("committer descriptors = %d, maximum %d",
				got, storeio.MaxCommitDescriptors)
		}
	})

	t.Run("default buffers reach maximum", func(t *testing.T) {
		options := unindexedBufferedOverlayGeometryOptions()
		options.BufferCount = 0
		normalized, err := options.normalized()
		if err != nil {
			t.Fatal(err)
		}
		if got := normalized.primaryUnifiedOverlayBuckets; got != primaryUnifiedOverlayBuckets {
			t.Fatalf("overlay buckets = %d, want compile maximum %d",
				got, primaryUnifiedOverlayBuckets)
		}
		if normalized.BufferCount != 8192 {
			t.Fatalf("default BufferCount = %d, want 8192",
				normalized.BufferCount)
		}
	})

	t.Run("explicit smaller buffers stay admissible", func(t *testing.T) {
		options := unindexedBufferedOverlayGeometryOptions()
		options.BufferCount = 1024
		options.QueueSlots = 1024
		normalized, err := options.normalized()
		if err != nil {
			t.Fatal(err)
		}
		if got := normalized.primaryUnifiedOverlayBuckets; got <= 64 || got >= primaryUnifiedOverlayBuckets {
			t.Fatalf("overlay buckets = %d, want adaptive (64, %d)",
				got, primaryUnifiedOverlayBuckets)
		}
		if normalized.BufferCount != 1024 ||
			normalized.maxTransactionPages >= normalized.BufferCount {
			t.Fatalf("BufferCount=%d cannot retain %d-page fold",
				normalized.BufferCount, normalized.maxTransactionPages)
		}
	})

	t.Run("resident bytes narrow the byte window", func(t *testing.T) {
		options := unindexedBufferedOverlayGeometryOptions()
		wide, err := options.normalized()
		if err != nil {
			t.Fatal(err)
		}
		options.ResidentBytes = 16 << 20
		normalized, err := options.normalized()
		if err != nil {
			t.Fatal(err)
		}
		if got := normalized.primaryUnifiedOverlayBuckets; got != primaryUnifiedOverlayBuckets {
			t.Fatalf("overlay buckets = %d, want descriptor maximum %d",
				got, primaryUnifiedOverlayBuckets)
		}
		if got := normalized.primaryUnifiedOverlayDirtyBytes; got == 0 || got >= wide.primaryUnifiedOverlayDirtyBytes {
			t.Fatalf("16 MiB dirty-byte window = %d, want in (0,%d)",
				got, wide.primaryUnifiedOverlayDirtyBytes)
		}
		residentNeed := normalized.maxTransactionBytes +
			uint64(normalized.primaryUnifiedOverlayBytes) +
			uint64(normalized.MaxPageSize)
		if residentNeed > uint64(normalized.ResidentBytes) {
			t.Fatalf("resident need = %d, budget = %d",
				residentNeed, normalized.ResidentBytes)
		}
	})

	t.Run("small resident budget retains an adaptive arena", func(t *testing.T) {
		options := unindexedBufferedOverlayGeometryOptions()
		options.ResidentBytes = 8 << 20
		normalized, err := options.normalized()
		if err != nil {
			t.Fatal(err)
		}
		preferred := primaryUnifiedOverlayTargetBytes(
			normalized.PageSize,
			normalized.MaxKeyBytes,
			normalized.InlineValueBytes,
		)
		if got := normalized.primaryUnifiedOverlayBytes; got < 64<<10 || got >= preferred {
			t.Fatalf("8 MiB overlay arena = %d, want adaptive [64 KiB,%d)",
				got, preferred)
		}
		if normalized.primaryUnifiedOverlayBuckets == 0 ||
			normalized.primaryUnifiedOverlayDirtyBytes == 0 ||
			normalized.primaryUnifiedOverlayParentBytes == 0 {
			t.Fatalf("adaptive overlay geometry disabled: bytes=%d buckets=%d dirty=%d parent=%d",
				normalized.primaryUnifiedOverlayBytes,
				normalized.primaryUnifiedOverlayBuckets,
				normalized.primaryUnifiedOverlayDirtyBytes,
				normalized.primaryUnifiedOverlayParentBytes)
		}
		residentNeed := normalized.maxTransactionBytes +
			uint64(normalized.primaryUnifiedOverlayBytes) +
			uint64(normalized.MaxPageSize)
		if residentNeed > uint64(normalized.ResidentBytes) {
			t.Fatalf("resident need = %d, budget = %d",
				residentNeed, normalized.ResidentBytes)
		}
	})

	t.Run("indexed tight residency adapts around ordinary transaction", func(t *testing.T) {
		options := journalDeltaTestOptions()
		options.MaxBatchDocuments = 1
		options.BufferCount = 8192
		options.QueueSlots = 128
		adapted := false
		for resident := int64(8 << 20); resident <= 40<<20; resident += 1 << 20 {
			options.ResidentBytes = resident
			normalized, err := options.normalized()
			if err != nil || normalized.primaryUnifiedOverlayBytes == 0 {
				continue
			}
			withoutOverlay := options
			withoutOverlay.QueueSlots = 1 << 16
			ordinary, err := withoutOverlay.normalized()
			if err != nil {
				t.Fatalf("ordinary indexed geometry at %d bytes: %v", resident, err)
			}
			if ordinary.primaryUnifiedOverlayBytes != 0 {
				t.Fatalf("ordinary reference retained %d overlay bytes",
					ordinary.primaryUnifiedOverlayBytes)
			}
			need := ordinary.maxTransactionBytes +
				uint64(normalized.primaryUnifiedOverlayBytes) +
				uint64(normalized.MaxPageSize)
			if need > uint64(resident) {
				t.Fatalf("indexed resident need = %d, budget = %d (ordinary=%d arena=%d slack=%d)",
					need, resident, ordinary.maxTransactionBytes,
					normalized.primaryUnifiedOverlayBytes, normalized.MaxPageSize)
			}
			preferred := primaryUnifiedOverlayTargetBytes(
				normalized.PageSize,
				normalized.MaxKeyBytes,
				normalized.InlineValueBytes,
			)
			if normalized.primaryUnifiedOverlayBytes < preferred {
				adapted = true
			}
		}
		if !adapted {
			t.Fatal("indexed tight-residency sweep never selected an adaptive arena")
		}
	})

	t.Run("disabled overlay has coherent zero geometry", func(t *testing.T) {
		options := unindexedBufferedOverlayGeometryOptions()
		options.QueueSlots = 1 << 16
		normalized, err := options.normalized()
		if err != nil {
			t.Fatal(err)
		}
		if normalized.primaryUnifiedOverlayBytes != 0 ||
			normalized.primaryUnifiedOverlayBuckets != 0 ||
			normalized.primaryUnifiedOverlayDirtyBytes != 0 ||
			normalized.primaryUnifiedOverlayParentBytes != 0 {
			t.Fatalf("disabled overlay geometry = bytes=%d buckets=%d dirty=%d parent=%d, want all zero",
				normalized.primaryUnifiedOverlayBytes,
				normalized.primaryUnifiedOverlayBuckets,
				normalized.primaryUnifiedOverlayDirtyBytes,
				normalized.primaryUnifiedOverlayParentBytes)
		}
	})
}

func TestBufferedJournalDeltaOneFoldDirtyHeadroom(t *testing.T) {
	options := unindexedBufferedOverlayGeometryOptions()
	options.ResidentBytes = 16 << 20
	coll := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), 128, options,
	)
	available := coll.cache.DirtyCapacityAvailable()
	reserve := coll.options.maxTransactionBytes
	if available < reserve || available >= 2*reserve {
		t.Fatalf("dirty capacity = %d, want in [%d, %d)",
			available, reserve, 2*reserve)
	}
	if coll.bufferedJournalDeltaPhysicalDrainNeeded(0) {
		t.Fatal("one-fold dirty reserve forced an unnecessary physical drain")
	}
}

func TestBufferedJournalDeltaUsesOverlaySizedJournal(t *testing.T) {
	ordinaryOptions := journalDeltaTestOptions()
	ordinary := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), 128, ordinaryOptions,
	)
	ordinaryHeader := ordinary.journal.Header()
	completeOverlayBatch := uint64(storeio.RecoveryBatchRecordPaddedSizeForPayload(
		ordinaryHeader.SectorSize, primaryUnifiedOverlayRecords,
		len(ordinary.primaryUnifiedOverlay.arena),
	))
	wantDeltaCapacity := min(
		max(2*completeOverlayBatch, recoveryJournalDeltaMinCapacityBytes),
		recoveryJournalMaxCapacityBytes,
	)
	wantDeltaCapacity = min(
		wantDeltaCapacity, recoveryJournalCompactDeltaCapacityBytes,
	)
	if got := ordinary.journal.Header().Capacity; got != wantDeltaCapacity {
		t.Fatalf("ordinary delta journal capacity = %d, want %d",
			got, wantDeltaCapacity)
	}
	if got := ordinaryHeader.Capacity -
		recoveryJournalCompactFutureReserveBytes; got != uint64(2)<<20 {
		t.Fatalf("ordinary delta append window = %d, want %d",
			got, uint64(2)<<20)
	}
	if ordinaryHeader.FormatVersion !=
		storeio.RecoveryJournalFormatScalarPatch {
		t.Fatalf("ordinary delta journal format = %d, want scalar-patch",
			ordinaryHeader.FormatVersion)
	}

	ackOptions := journalDeltaTestOptions()
	ackOptions.RecoveryJournal = true
	ack := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), 128, ackOptions,
	)
	ackHeader := ack.journal.Header()
	wantAckCapacity := recoveryJournalCapacityBytesFor(
		ackHeader.SectorSize, ack.options.MaxKeyBytes,
		ack.options.InlineValueBytes, ack.options.MaxDocumentBytes, 0,
	)
	if got := ackHeader.Capacity; got != wantAckCapacity ||
		got >= completeOverlayBatch {
		t.Fatalf(
			"per-mutation ack journal capacity = %d, want cadence-sized %d below delta batch %d",
			got, wantAckCapacity, completeOverlayBatch,
		)
	}
	if ackHeader.FormatVersion != storeio.RecoveryJournalFormatLegacy {
		t.Fatalf("per-mutation journal format = %d, want legacy",
			ackHeader.FormatVersion)
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
// workload's 64-mutation checkpoint cadence through one complete indexed row
// overlay window. The full overlay is materialized into a staged physical root
// without a device fence; the following scheduled checkpoint then consumes the
// one-fold dirty reserve and drains the current cut instead of making a later
// mutation pay for an automatic checkpoint.
func TestBufferedJournalDeltaContinuesAcrossDeviceSilentFolds(t *testing.T) {
	const checkpointEvery = 64
	const mutations = journalDeltaTestPressureMutations + checkpointEvery

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
		publishedBeforeFlush := coll.committer.PublishedGeneration()
		if publishedBeforeFlush > physicalBase {
			stagedFolds[publishedBeforeFlush] = struct{}{}
			base := coll.primaryCheckpointBase
			if base == nil || base.root.Generation != publishedBeforeFlush ||
				publishedBeforeFlush > coll.journalDeltaGeneration.Load() {
				t.Fatalf(
					"Flush %d invalid staged fold: published=%d base=%v watermark=%d",
					mutation/checkpointEvery, publishedBeforeFlush, base != nil,
					coll.journalDeltaGeneration.Load(),
				)
			}
		}
		if err := coll.Flush(); err != nil {
			t.Fatalf("scheduled Flush %d: %v", mutation/checkpointEvery, err)
		}
		after := coll.Stats()
		scheduledDrain := mutation == mutations
		wantCheckpointDelta := uint64(1)
		wantRecordDelta := uint64(checkpointEvery)
		wantPhysical := physicalBase
		if scheduledDrain {
			wantCheckpointDelta = 0
			wantRecordDelta = 0
			wantPhysical = target
		}
		if after.JournalDeltaCheckpoints !=
			before.JournalDeltaCheckpoints+wantCheckpointDelta {
			t.Fatalf("Flush %d delta checkpoints = %d, want %d",
				mutation/checkpointEvery,
				after.JournalDeltaCheckpoints,
				before.JournalDeltaCheckpoints+wantCheckpointDelta)
		}
		if after.JournalDeltaRecords !=
			before.JournalDeltaRecords+wantRecordDelta {
			t.Fatalf("Flush %d delta records = %d, want %d",
				mutation/checkpointEvery,
				after.JournalDeltaRecords,
				before.JournalDeltaRecords+wantRecordDelta)
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
		if got := coll.committer.DurableGeneration(); got != wantPhysical {
			t.Fatalf("Flush %d physical durable = %d, want %d",
				mutation/checkpointEvery, got, wantPhysical)
		}
		if coll.DurableGeneration() != target ||
			coll.journalDeltaGeneration.Load() != target {
			t.Fatalf("Flush %d logical/watermark = %d/%d, want %d",
				mutation/checkpointEvery,
				coll.DurableGeneration(),
				coll.journalDeltaGeneration.Load(), target)
		}

	}

	finalStats := coll.Stats()
	if got := finalStats.PrimaryOverlayFolds -
		baseline.PrimaryOverlayFolds; got != 1 {
		t.Fatalf("primary overlay folds = %d, want 1", got)
	}
	if len(stagedFolds) != 1 {
		t.Fatalf("distinct staged fold generations = %d, want 1",
			len(stagedFolds))
	}
	if got, wantCheckpoints := finalStats.JournalDeltaCheckpoints-
		baseline.JournalDeltaCheckpoints,
		uint64(mutations/checkpointEvery-1); got != wantCheckpoints {
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

// TestBufferedJournalDeltaScheduledPhysicalDrain bounds the resource lag behind
// journal durability. One device-silent fold consumes the deliberately
// retained physical-fold reserve; the next explicit Flush must drain the current cut
// physically instead of leaving a later mutation to force an automatic
// checkpoint. Descriptor pressure can request the same drain independently.
func TestBufferedJournalDeltaScheduledPhysicalDrain(t *testing.T) {
	const (
		documents       = 320
		checkpointEvery = 64
		mutations       = journalDeltaTestPressureMutations + checkpointEvery
	)
	options := journalDeltaTestOptions()
	options.QueueSlots = 4
	coll := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), documents, options,
	)
	path := coll.file.Name()
	physicalBase := coll.committer.DurableGeneration()
	baseline := coll.Stats()

	want := primaryStoreContent(t, coll)
	groups := make(map[string]int, documents)
	for at := range documents {
		groups[templateHeavyOverlayKey(at)] = at % 37
	}
	keyString := templateHeavyOverlayKey(20)
	key := []byte(keyString)

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
		if err := coll.Flush(); err != nil {
			t.Fatalf("Flush %d: %v", mutation/checkpointEvery, err)
		}
	}

	target := coll.Generation()
	stats := coll.Stats()
	if stats.AutomaticCheckpoints != baseline.AutomaticCheckpoints {
		t.Fatalf("scheduled drain counted as automatic: %d -> %d",
			baseline.AutomaticCheckpoints, stats.AutomaticCheckpoints)
	}
	if stats.JournalDeltaFullFallbacks !=
		baseline.JournalDeltaFullFallbacks {
		t.Fatalf("scheduled drain counted as delta fallback: %d -> %d",
			baseline.JournalDeltaFullFallbacks,
			stats.JournalDeltaFullFallbacks)
	}
	if got := coll.committer.DurableGeneration(); got != target ||
		got == physicalBase {
		t.Fatalf("physical durable = %d, want current %d above base %d",
			got, target, physicalBase)
	}
	if got := coll.committer.Stats().QueuedGenerations; got != 0 {
		t.Fatalf("queued generations after scheduled drain = %d, want 0", got)
	}
	if coll.DurableGeneration() != target ||
		coll.journalDeltaGeneration.Load() != target ||
		coll.journalDeltaAppendedGeneration.Load() != target {
		t.Fatalf(
			"generation visible/durable/appended = %d/%d/%d, want %d",
			coll.DurableGeneration(), coll.journalDeltaGeneration.Load(),
			coll.journalDeltaAppendedGeneration.Load(), target,
		)
	}
	if got, wantCheckpoints := stats.JournalDeltaCheckpoints-
		baseline.JournalDeltaCheckpoints,
		uint64(mutations/checkpointEvery-1); got != wantCheckpoints {
		t.Fatalf("journal delta checkpoints = %d, want %d",
			got, wantCheckpoints)
	}

	crashImage := captureJournalImage(t, path)
	assertJournalDeltaImage(
		t, options, crashImage, cloneStringMap(want), cloneIntMap(groups),
		target, "scheduled-physical-drain",
	)
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}
	if err := coll.file.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBufferedJournalDeltaSameKeyHeadroomRotatesAtExplicitFlush proves journal
// bytes are bounded independently of committer and dirty-frame pressure. One
// hot key fills records much faster than it fills the physical overlay queue;
// the explicit Flush preflight must rotate before the remaining journal region
// can no longer hold both its suffix and one future full overlay carry.
func TestBufferedJournalDeltaSameKeyHeadroomRotatesAtExplicitFlush(t *testing.T) {
	const (
		documents       = 320
		checkpointEvery = 64
		checkpoints     = 640
		mutations       = checkpointEvery * checkpoints
	)
	options := journalDeltaTestOptions()
	options.QueueSlots = 1024
	coll := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), documents, options,
	)
	path := coll.file.Name()
	baseline := coll.Stats()
	baseRecycle := coll.journal.Header().RecycleCount

	want := primaryStoreContent(t, coll)
	groups := make(map[string]int, documents)
	for at := range documents {
		groups[templateHeavyOverlayKey(at)] = at % 37
	}
	keyString := templateHeavyOverlayKey(20)
	key := []byte(keyString)
	padding := strings.Repeat("x", 400)
	for mutation := 1; mutation <= mutations; mutation++ {
		group := 40 + mutation&1
		raw := fmt.Appendf(nil,
			`{"group":%d,"padding":"%s"}`, group, padding,
		)
		if created, err := coll.Put(key, raw); err != nil || created {
			t.Fatalf("Put %d = %v,%v", mutation, created, err)
		}
		if mutation == mutations {
			want[keyString] = journalDeltaCanonical(t, raw)
			groups[keyString] = group
		}
		if mutation%checkpointEvery == 0 {
			if err := coll.Flush(); err != nil {
				t.Fatalf("Flush %d: %v", mutation/checkpointEvery, err)
			}
		}
	}

	stats := coll.Stats()
	recycles := coll.journal.Header().RecycleCount - baseRecycle
	if recycles < 2 {
		t.Fatalf("journal headroom rotations = %d, want multiple", recycles)
	}
	if stats.AutomaticCheckpoints != baseline.AutomaticCheckpoints {
		t.Fatalf("journal headroom forced automatic checkpoints: %d -> %d",
			baseline.AutomaticCheckpoints, stats.AutomaticCheckpoints)
	}
	if stats.JournalDeltaFullFallbacks !=
		baseline.JournalDeltaFullFallbacks {
		t.Fatalf("journal headroom used full fallback: %d -> %d",
			baseline.JournalDeltaFullFallbacks,
			stats.JournalDeltaFullFallbacks)
	}
	if got, wantCheckpoints := stats.JournalDeltaCheckpoints-
		baseline.JournalDeltaCheckpoints+recycles,
		uint64(checkpoints); got != wantCheckpoints {
		t.Fatalf("delta checkpoints + rotations = %d, want %d",
			got, wantCheckpoints)
	}
	target := coll.Generation()
	if coll.DurableGeneration() != target {
		t.Fatalf("durable generation = %d, want %d",
			coll.DurableGeneration(), target)
	}
	assertJournalDeltaImage(
		t, options, captureJournalImage(t, path),
		cloneStringMap(want), cloneIntMap(groups), target,
		"same-key-headroom-rotation",
	)
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}
	if err := coll.file.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBufferedJournalDeltaNonAlignedPressureCarryCrashReplay uses a checkpoint
// cadence that does not divide the effective indexed-overlay mutation window.
// The first pressure fold therefore carries the suffix after the preceding
// scheduled Flush without syncing; the next scheduled Flush appends the rest
// and fences both batches once. A crash may lose that unsynced carry and recover
// the preceding scheduled cut, or retain its complete bytes and recover the
// pressure cut. The next scheduled Flush consumes the one-fold physical
// reserve; later journal checkpoints prove the watermarks remain consecutive
// across that physical drain and journal recycle.
func TestBufferedJournalDeltaNonAlignedPressureCarryCrashReplay(t *testing.T) {
	const (
		documents             = 320
		checkpointEvery       = 100
		pressure              = journalDeltaTestPressureMutations
		durableBeforeMutation = pressure / checkpointEvery * checkpointEvery
		durableAfterMutation  = (pressure + checkpointEvery - 1) /
			checkpointEvery * checkpointEvery
		mutations = (2*pressure + checkpointEvery - 1) /
			checkpointEvery * checkpointEvery
	)
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
	baseGeneration := coll.Generation()
	physicalBase := coll.committer.DurableGeneration()
	baseline := coll.Stats()

	want := primaryStoreContent(t, coll)
	groups := make(map[string]int, documents)
	for at := range documents {
		groups[templateHeavyOverlayKey(at)] = at % 37
	}
	keyString := templateHeavyOverlayKey(20)
	key := []byte(keyString)

	type crashCut struct {
		image      journalCrashImage
		want       map[string]string
		groups     map[string]int
		generation uint64
	}
	var (
		durableBefore crashCut
		carriedFirst  crashCut
		durableAfter  crashCut
		carryCuts     []uint64
		lastSyncs     = fault.Syncs()
	)
	previousCarryHook := recoveryJournalDeltaCarryHook
	defer func() { recoveryJournalDeltaCarryHook = previousCarryHook }()
	recoveryJournalDeltaCarryHook = func(target uint64) {
		if got := fault.Syncs(); got != lastSyncs {
			t.Fatalf("carry target=%d issued a sync: %d -> %d",
				target, lastSyncs, got)
		}
		carryCuts = append(carryCuts, target)
		if target == baseGeneration+pressure {
			carriedFirst = crashCut{
				image:      captureJournalImage(t, path),
				want:       cloneStringMap(want),
				groups:     cloneIntMap(groups),
				generation: target,
			}
		}
	}

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

		beforeSyncs := fault.Syncs()
		target := coll.Generation()
		if err := coll.Flush(); err != nil {
			t.Fatalf("Flush %d: %v", mutation/checkpointEvery, err)
		}
		wantSyncs := 1
		if mutation == durableAfterMutation {
			wantSyncs = 2
		}
		if got := fault.Syncs(); got != beforeSyncs+wantSyncs {
			t.Fatalf("Flush %d syncs = %d, want %d",
				mutation/checkpointEvery, got-beforeSyncs, wantSyncs)
		}
		lastSyncs = fault.Syncs()
		if coll.DurableGeneration() != target ||
			coll.journalDeltaGeneration.Load() != target ||
			coll.journalDeltaAppendedGeneration.Load() != target {
			t.Fatalf(
				"Flush %d generation visible/durable/appended = %d/%d/%d, want %d",
				mutation/checkpointEvery, coll.Generation(),
				coll.journalDeltaGeneration.Load(),
				coll.journalDeltaAppendedGeneration.Load(), target,
			)
		}
		cut := crashCut{
			image:      captureJournalImage(t, path),
			want:       cloneStringMap(want),
			groups:     cloneIntMap(groups),
			generation: target,
		}
		switch mutation {
		case durableBeforeMutation:
			durableBefore = cut
		case durableAfterMutation:
			durableAfter = cut
		}
	}

	if got, expected := carryCuts, []uint64{
		baseGeneration + pressure,
	}; !slices.Equal(got, expected) {
		t.Fatalf("carry generations = %v, want %v", got, expected)
	}
	stats := coll.Stats()
	if got := stats.PrimaryOverlayFolds -
		baseline.PrimaryOverlayFolds; got != 1 {
		t.Fatalf("overlay folds = %d, want 1", got)
	}
	if got := stats.JournalDeltaRecords -
		baseline.JournalDeltaRecords; got != mutations-(durableAfterMutation-pressure) {
		t.Fatalf("journal delta records = %d, want %d", got,
			mutations-(durableAfterMutation-pressure))
	}
	if got := stats.JournalDeltaCheckpoints -
		baseline.JournalDeltaCheckpoints; got != mutations/checkpointEvery-1 {
		t.Fatalf("journal delta checkpoints = %d, want %d",
			got, mutations/checkpointEvery-1)
	}
	if stats.AutomaticCheckpoints != baseline.AutomaticCheckpoints ||
		stats.JournalDeltaFullFallbacks != baseline.JournalDeltaFullFallbacks {
		t.Fatalf("non-aligned carry forced persistence: before=%+v after=%+v",
			baseline, stats)
	}
	wantPhysical := physicalBase + durableAfterMutation
	if got := coll.committer.DurableGeneration(); got != wantPhysical {
		t.Fatalf("physical durable = %d, want %d", got, wantPhysical)
	}

	// Losing the unsynced append is legal buffered-visible behavior.
	assertJournalDeltaImage(
		t, options, durableBefore.image, durableBefore.want, durableBefore.groups,
		durableBefore.generation, "non-aligned/lost-carry",
	)
	// If the complete unsynced append reached storage, its framed batch is a
	// valid recoverable prefix even though no explicit durability was promised.
	assertJournalDeltaImage(
		t, options, carriedFirst.image, carriedFirst.want, carriedFirst.groups,
		carriedFirst.generation, "non-aligned/retained-carry",
	)
	// The following explicit Flush must make both the carried batch and its
	// remaining suffix durable through the exact target generation and index.
	assertJournalDeltaImage(
		t, options, durableAfter.image, durableAfter.want, durableAfter.groups,
		durableAfter.generation, "non-aligned/post-flush",
	)

	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}
	if err := coll.file.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestBufferedJournalDeltaTornUnsyncedCarryTruncates programs a simulated crash
// at the non-aligned pressure append. FaultJournal writes only the framing
// prefix and returns as if the process stopped at that instruction, so the live
// return value is intentionally irrelevant. Recovery must stop at the preceding
// explicitly flushed batch rather than exposing any fragment of the carry.
func TestBufferedJournalDeltaTornUnsyncedCarryTruncates(t *testing.T) {
	const (
		documents       = 320
		checkpointEvery = 100
	)
	options := journalDeltaTestOptions()
	getFault, restoreFault := installJournalFaultSeam(t)
	coll := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), documents, options,
	)
	fault := getFault()
	if fault == nil {
		restoreFault()
		t.Fatal("journal fault seam was not installed")
	}
	path := coll.file.Name()
	want := primaryStoreContent(t, coll)
	groups := make(map[string]int, documents)
	for at := range documents {
		groups[templateHeavyOverlayKey(at)] = at % 37
	}
	keyString := templateHeavyOverlayKey(20)
	key := []byte(keyString)

	for mutation := 1; mutation <= journalDeltaTestPressureMutations; mutation++ {
		group := 40 + mutation&1
		raw := journalDeltaGroupDoc(20, group)
		if created, err := coll.Put(key, raw); err != nil || created {
			restoreFault()
			t.Fatalf("Put %d = %v,%v", mutation, created, err)
		}
		want[keyString] = journalDeltaCanonical(t, raw)
		groups[keyString] = group
		if mutation == checkpointEvery {
			if err := coll.Flush(); err != nil {
				restoreFault()
				t.Fatalf("baseline Flush: %v", err)
			}
		}
	}
	baselineGeneration := coll.journalDeltaGeneration.Load()
	baselineAppended := coll.journalDeltaAppendedGeneration.Load()
	if baselineGeneration != baselineAppended {
		restoreFault()
		t.Fatalf("baseline durable/appended = %d/%d",
			baselineGeneration, baselineAppended)
	}
	baselineWant := cloneStringMap(want)
	baselineGroups := cloneIntMap(groups)
	// The oracle above includes mutations after checkpointEvery through the
	// pressure boundary, which were not explicitly flushed. Reconstruct the
	// exact last-scheduled-Flush value for the recovery cut.
	baselineRaw := journalDeltaGroupDoc(20, 40+checkpointEvery&1)
	baselineWant[keyString] = journalDeltaCanonical(t, baselineRaw)
	baselineGroups[keyString] = 40 + checkpointEvery&1

	beforeSyncs := fault.Syncs()
	fault.Program(storeio.JournalFaultPlan{
		Phase:       storeio.JournalFaultTornAppend,
		AppendIndex: fault.Appends(),
	})
	_, _ = coll.Put(key, journalDeltaGroupDoc(20, 42))
	if !fault.Faulted() {
		restoreFault()
		t.Fatal("torn carry seam did not fire")
	}
	if got := fault.Syncs(); got != beforeSyncs {
		restoreFault()
		t.Fatalf("torn carry issued %d sync(s)", got-beforeSyncs)
	}
	image := captureJournalImage(t, path)
	_ = coll.Close()
	_ = coll.file.Close()
	restoreFault()

	assertJournalDeltaImage(
		t, options, image, baselineWant, baselineGroups,
		baselineGeneration, "non-aligned/torn-carry",
	)
}

// TestBufferedJournalDeltaRetainsCarryAcrossOlderStagedFlush exercises the
// recovery branch materializePrimaryParentsLocked uses when committer staging
// is exhausted. One effective pressure window is journal-durable and then
// staged device-silently; 17 more generations are carried unsynced before that
// older staged root is flushed. That physical
// flush must not recycle the newer journal or regress either watermark.
// Recovery without the carry still reaches the explicitly durable cut; after
// the current overlay is materialized, Flush fences the carry without appending
// it twice and recovery reaches the exact indexed target.
func TestBufferedJournalDeltaRetainsCarryAcrossOlderStagedFlush(t *testing.T) {
	const (
		documents = 320
		mutations = journalDeltaTestPressureMutations + 17
	)
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
	journalBase := coll.journal.BaseGeneration()
	want := primaryStoreContent(t, coll)
	groups := make(map[string]int, documents)
	for at := range documents {
		groups[templateHeavyOverlayKey(at)] = at % 37
	}
	keyString := templateHeavyOverlayKey(20)
	key := []byte(keyString)
	var (
		durableWantCut    map[string]string
		durableGroupsCut  map[string]int
		durableJournalCut []byte
	)

	for mutation := 1; mutation <= mutations; mutation++ {
		group := 40 + mutation&1
		raw := journalDeltaGroupDoc(20, group)
		if created, err := coll.Put(key, raw); err != nil || created {
			t.Fatalf("Put %d = %v,%v", mutation, created, err)
		}
		want[keyString] = journalDeltaCanonical(t, raw)
		groups[keyString] = group
		if mutation <= journalDeltaTestPressureMutations &&
			mutation%64 == 0 {
			if err := coll.Flush(); err != nil {
				t.Fatalf("aligned Flush %d: %v", mutation/64, err)
			}
			if mutation == journalDeltaTestPressureMutations {
				durableWantCut = cloneStringMap(want)
				durableGroupsCut = cloneIntMap(groups)
				var readErr error
				durableJournalCut, readErr =
					os.ReadFile(path + ".rjournal")
				if readErr != nil {
					t.Fatal(readErr)
				}
			}
		}
	}
	stagedGeneration := coll.committer.PublishedGeneration()
	journalDurable := coll.journalDeltaGeneration.Load()
	target := coll.Generation()
	if stagedGeneration != journalDurable || journalDurable+17 != target {
		t.Fatalf("staged/journal/current generation = %d/%d/%d, want deltas 0/17",
			stagedGeneration, journalDurable, target)
	}
	if durableWantCut == nil || durableGroupsCut == nil ||
		durableJournalCut == nil {
		t.Fatal("explicitly durable crash cut was not captured")
	}

	beforeAppends, beforeSyncs := fault.Appends(), fault.Syncs()
	coll.writer.Lock()
	handled, err := coll.carryBufferedJournalDeltaBeforeFoldLocked()
	if err != nil || !handled {
		coll.writer.Unlock()
		t.Fatalf("carry = %v,%v", handled, err)
	}
	if got := coll.journalDeltaAppendedGeneration.Load(); got != target {
		coll.writer.Unlock()
		t.Fatalf("carried generation = %d, want %d", got, target)
	}
	if err := coll.flushBufferedPublishedLocked(); err != nil {
		coll.writer.Unlock()
		t.Fatalf("older staged Flush: %v", err)
	}
	if got := coll.committer.DurableGeneration(); got != stagedGeneration {
		coll.writer.Unlock()
		t.Fatalf("physical durable = %d, want %d", got, stagedGeneration)
	}
	if got := coll.journalDeltaGeneration.Load(); got != journalDurable {
		coll.writer.Unlock()
		t.Fatalf("older physical Flush regressed durable watermark to %d, want %d",
			got, journalDurable)
	}
	if got := coll.journalDeltaAppendedGeneration.Load(); got != target {
		coll.writer.Unlock()
		t.Fatalf("older physical Flush discarded appended watermark %d, want %d",
			got, target)
	}
	if got := coll.journal.BaseGeneration(); got != journalBase {
		coll.writer.Unlock()
		t.Fatalf("older physical Flush recycled journal base %d, want %d",
			got, journalBase)
	}
	lostCarryImage := journalDeltaImageBeforeSync(
		t, path, durableJournalCut,
	)
	if err := coll.materializePrimaryParentsLocked(); err != nil {
		coll.writer.Unlock()
		t.Fatalf("materialize carried cut: %v", err)
	}
	coll.writer.Unlock()

	if got := fault.Appends(); got != beforeAppends+1 {
		t.Fatalf("carry appends = %d, want 1", got-beforeAppends)
	}
	if got := fault.Syncs(); got != beforeSyncs {
		t.Fatalf("carry/staged Flush journal syncs = %d, want 0",
			got-beforeSyncs)
	}
	if got := coll.DurableGeneration(); got < journalDurable {
		t.Fatalf("public durable regressed to %d from %d",
			got, journalDurable)
	}
	assertJournalDeltaImage(
		t, options, lostCarryImage, durableWantCut, durableGroupsCut,
		journalDurable, "staged-exhaustion/lost-carry",
	)

	if err := coll.Flush(); err != nil {
		t.Fatalf("explicit Flush: %v", err)
	}
	if got := fault.Appends(); got != beforeAppends+1 {
		t.Fatalf("explicit Flush duplicated carry: appends=%d, want 1",
			got-beforeAppends)
	}
	if got := fault.Syncs(); got != beforeSyncs+1 {
		t.Fatalf("explicit Flush syncs = %d, want 1", got-beforeSyncs)
	}
	if coll.DurableGeneration() != target ||
		coll.journalDeltaGeneration.Load() != target ||
		coll.journalDeltaAppendedGeneration.Load() != target {
		t.Fatalf("post-Flush visible/durable/appended = %d/%d/%d, want %d",
			coll.Generation(), coll.journalDeltaGeneration.Load(),
			coll.journalDeltaAppendedGeneration.Load(), target)
	}

	image := captureJournalImage(t, path)
	assertJournalDeltaImage(
		t, options, image, cloneStringMap(want), cloneIntMap(groups),
		target, "staged-exhaustion/post-flush",
	)
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}
	if err := coll.file.Close(); err != nil {
		t.Fatal(err)
	}
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

// TestBufferedJournalDeltaCarryAppendFailureIsSticky fails the unsynced carry
// attempted at overlay pressure. The full old window remains reader-visible,
// neither journal watermark advances, and the device error poisons every later
// mutation/checkpoint/Close. The crash image still exposes only the original
// physical generation because ENOSPC wrote no batch bytes.
func TestBufferedJournalDeltaCarryAppendFailureIsSticky(t *testing.T) {
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
	baseGeneration := coll.Generation()
	baseWant := primaryStoreContent(t, coll)
	baseGroups := make(map[string]int, documents)
	for at := range documents {
		baseGroups[templateHeavyOverlayKey(at)] = at % 37
	}
	key := []byte(templateHeavyOverlayKey(20))
	for mutation := 1; mutation <= journalDeltaTestPressureMutations; mutation++ {
		if _, err := coll.Put(
			key, journalDeltaGroupDoc(20, 40+mutation&1),
		); err != nil {
			t.Fatalf("Put %d: %v", mutation, err)
		}
	}

	beforeAppends, beforeSyncs := fault.Appends(), fault.Syncs()
	beforeStats := coll.Stats()
	fault.Program(storeio.JournalFaultPlan{
		Phase:       storeio.JournalFaultENOSPCAppend,
		AppendIndex: beforeAppends,
	})
	_, carryErr := coll.Put(key, journalDeltaGroupDoc(20, 42))
	if carryErr == nil || !fault.Faulted() {
		t.Fatalf("faulted carry Put = %v, fired=%v",
			carryErr, fault.Faulted())
	}
	if errors.Is(carryErr, ErrCommitOutcomeUnknown) {
		t.Fatalf("pre-sync delta append failure misclassified as unknown: %v", carryErr)
	}
	if got := fault.Appends(); got != beforeAppends+1 {
		t.Fatalf("carry append attempts = %d, want 1", got-beforeAppends)
	}
	if got := fault.Syncs(); got != beforeSyncs {
		t.Fatalf("failed carry issued %d sync(s)", got-beforeSyncs)
	}
	if got := coll.journalDeltaGeneration.Load(); got != baseGeneration {
		t.Fatalf("failed carry advanced durable watermark to %d, want %d",
			got, baseGeneration)
	}
	if got := coll.journalDeltaAppendedGeneration.Load(); got != baseGeneration {
		t.Fatalf("failed carry advanced appended watermark to %d, want %d",
			got, baseGeneration)
	}
	if got := coll.Stats(); got.PrimaryOverlayFolds !=
		beforeStats.PrimaryOverlayFolds ||
		got.AutomaticCheckpoints != beforeStats.AutomaticCheckpoints {
		t.Fatalf("failed carry folded/checkpointed overlay: before=%+v after=%+v",
			beforeStats, got)
	}
	if persistence := coll.PersistenceError(); persistence == nil || !errors.Is(carryErr, persistence) {
		t.Fatalf("PersistenceError=%v, carry=%v", persistence, carryErr)
	}
	image := captureJournalImage(t, path)
	if _, err := coll.Put(key, journalDeltaGroupDoc(20, 43)); !errors.Is(err, carryErr) {
		t.Fatalf("Put after poison = %v, want %v", err, carryErr)
	}
	if err := coll.Flush(); !errors.Is(err, carryErr) {
		t.Fatalf("Flush after poison = %v, want %v", err, carryErr)
	}
	if err := coll.Close(); !errors.Is(err, carryErr) {
		t.Fatalf("Close after poison = %v, want %v", err, carryErr)
	}
	assertJournalDeltaImage(
		t, options, image, baseWant, baseGroups, baseGeneration,
		"carry-append-failure",
	)
}

// TestBufferedJournalDeltaBarrierFailureOutcomeUnknownMayReplay covers the
// ordinary buffered-visible Flush lane's true append-then-sync boundary. A
// complete delta record is present when its fence returns EIO, so the failed
// Flush cannot say whether that logical generation will replay after reopen.
func TestBufferedJournalDeltaBarrierFailureOutcomeUnknownMayReplay(t *testing.T) {
	options := journalDeltaTestOptions()
	getFault, restoreFault := installJournalFaultSeam(t)
	defer restoreFault()
	coll := buildTemplateHeavyOverlayCollection(t, t.TempDir(), 320, options)
	fault := getFault()
	if fault == nil {
		t.Fatal("journal fault seam was not installed")
	}
	path := coll.file.Name()
	keyString := templateHeavyOverlayKey(20)
	key := []byte(keyString)
	raw := journalDeltaGroupDoc(20, 71)
	if created, err := coll.Put(key, raw); err != nil || created {
		t.Fatalf("Put = %v,%v, want replacement", created, err)
	}
	target := coll.Generation()
	beforeAppends, beforeSyncs := fault.Appends(), fault.Syncs()
	fault.Program(storeio.JournalFaultPlan{
		Phase:     storeio.JournalFaultSyncError,
		SyncIndex: beforeSyncs,
	})

	flushErr := coll.Flush()
	requireUnknownJournalOutcome(t, flushErr)
	if !fault.Faulted() {
		t.Fatal("programmed delta journal sync fault never fired")
	}
	if got := fault.Appends(); got != beforeAppends+1 {
		t.Fatalf("delta appends = %d, want %d", got, beforeAppends+1)
	}
	if got := fault.Syncs(); got != beforeSyncs+1 {
		t.Fatalf("delta syncs = %d, want %d", got, beforeSyncs+1)
	}
	if got := coll.journalDeltaGeneration.Load(); got >= target {
		t.Fatalf("failed delta barrier advanced durable watermark to %d, target %d",
			got, target)
	}
	requireUnknownJournalOutcome(t, coll.PersistenceError())

	image := captureJournalImage(t, path)
	_ = coll.Close()
	recovered := reopenJournalImage(t, options, image)
	requireJournalKey(t, recovered, keyString, journalDeltaCanonical(t, raw))
	got := primaryExactTestKeys(
		t, recovered, "group", primaryExactTestNeedle(t, "71"),
	)
	if !slices.Equal(got, []string{keyString}) {
		t.Fatalf("replayed exact index group=71: got %v want [%s]", got, keyString)
	}
}

// TestBufferedJournalDeltaSyncFailureIsSticky proves the logical durable
// watermark advances only after the journal barrier. Pressure first carries a
// complete effective indexed-overlay window without syncing. The following
// Flush takes the scheduled physical-drain path, whose first journal barrier
// fails before the one-record suffix is admitted. Neither watermark reaches
// the target, and every later
// write/checkpoint/Close returns the same poison.
func TestBufferedJournalDeltaSyncFailureIsSticky(t *testing.T) {
	options := journalDeltaTestOptions()
	getFault, restoreFault := installJournalFaultSeam(t)
	defer restoreFault()
	coll := buildTemplateHeavyOverlayCollection(t, t.TempDir(), 320, options)
	fault := getFault()
	if fault == nil {
		t.Fatal("journal fault seam was not installed")
	}
	key := []byte(templateHeavyOverlayKey(20))
	for mutation := 1; mutation <= journalDeltaTestPressureMutations+1; mutation++ {
		if _, err := coll.Put(
			key, journalDeltaGroupDoc(20, 40+mutation&1),
		); err != nil {
			t.Fatalf("Put %d: %v", mutation, err)
		}
	}
	before := coll.journalDeltaGeneration.Load()
	target := coll.Generation()
	if got := coll.journalDeltaAppendedGeneration.Load(); got != target-1 {
		t.Fatalf("pressure appended generation = %d, want %d", got, target-1)
	}
	beforeAppends, beforeSyncs := fault.Appends(), fault.Syncs()
	fault.Program(storeio.JournalFaultPlan{
		Phase:     storeio.JournalFaultSyncError,
		SyncIndex: beforeSyncs,
	})
	postCalled := false
	previousPostSyncHook := recoveryJournalDeltaPostSyncHook
	recoveryJournalDeltaPostSyncHook = func(uint64) { postCalled = true }
	defer func() {
		recoveryJournalDeltaPostSyncHook = previousPostSyncHook
	}()
	flushErr := coll.Flush()
	if flushErr == nil || !fault.Faulted() {
		t.Fatalf("faulted delta Flush = %v, fired=%v", flushErr, fault.Faulted())
	}
	// This scheduled physical-drain path has already made target durable in the
	// primary root; the fault is the following journal recycle barrier, not an
	// append-then-sync acknowledgement. Its data outcome is therefore definite.
	if errors.Is(flushErr, ErrCommitOutcomeUnknown) {
		t.Fatalf("post-root recycle failure misclassified as unknown: %v", flushErr)
	}
	if got := fault.Appends(); got != beforeAppends {
		t.Fatalf("suffix appends = %d, want 0", got-beforeAppends)
	}
	if got := fault.Syncs(); got != beforeSyncs+1 {
		t.Fatalf("failed barriers = %d, want 1", got-beforeSyncs)
	}
	if postCalled {
		t.Fatal("post-sync hook ran after a failed journal barrier")
	}
	if got := coll.journalDeltaGeneration.Load(); got != before {
		t.Fatalf("failed barrier advanced delta generation %d -> %d", before, got)
	}
	if got := coll.journalDeltaAppendedGeneration.Load(); got != target-1 {
		t.Fatalf("failed barrier appended generation = %d, want %d", got, target-1)
	}
	if persistence := coll.PersistenceError(); !errors.Is(flushErr, persistence) {
		t.Fatalf("PersistenceError=%v, Flush=%v", persistence, flushErr)
	} else if errors.Is(persistence, ErrCommitOutcomeUnknown) {
		t.Fatalf("sticky recycle poison misclassified as unknown: %v", persistence)
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

// TestBufferedJournalDeltaCloseFoldsUnsyncedCarry closes immediately after
// pressure carried one complete overlay and left the newest mutation in the
// replacement overlay. Close must physically publish the entire visible cut,
// recycle the unsynced journal, and reopen with exact rows and postings.
func TestBufferedJournalDeltaCloseFoldsUnsyncedCarry(t *testing.T) {
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
	baseGeneration := coll.Generation()
	want := primaryStoreContent(t, coll)
	groups := make(map[string]int, documents)
	for at := range documents {
		groups[templateHeavyOverlayKey(at)] = at % 37
	}
	keyString := templateHeavyOverlayKey(20)
	key := []byte(keyString)
	beforeSyncs := fault.Syncs()
	for mutation := 1; mutation <= journalDeltaTestPressureMutations+1; mutation++ {
		group := 40 + mutation&1
		raw := journalDeltaGroupDoc(20, group)
		if _, err := coll.Put(key, raw); err != nil {
			t.Fatalf("Put %d: %v", mutation, err)
		}
		want[keyString] = journalDeltaCanonical(t, raw)
		groups[keyString] = group
	}
	target := coll.Generation()
	if target != baseGeneration+journalDeltaTestPressureMutations+1 {
		t.Fatalf("target generation = %d, want %d",
			target, baseGeneration+journalDeltaTestPressureMutations+1)
	}
	if got := coll.journalDeltaAppendedGeneration.Load(); got != target-1 {
		t.Fatalf("pressure appended generation = %d, want %d", got, target-1)
	}
	if got := coll.journalDeltaGeneration.Load(); got != baseGeneration {
		t.Fatalf("pressure durable generation = %d, want %d",
			got, baseGeneration)
	}
	if got := fault.Syncs(); got != beforeSyncs {
		t.Fatalf("pressure issued %d journal sync(s)", got-beforeSyncs)
	}

	if err := coll.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	image := captureJournalImage(t, path)
	journalFile, err := os.Open(path + ".rjournal")
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storeio.OpenRecoveryJournal(journalFile)
	if err != nil {
		_ = journalFile.Close()
		t.Fatal(err)
	}
	if journal.Cursor() != 0 || journal.BaseGeneration() != target {
		t.Fatalf("Close journal cursor/base = %d/%d, want 0/%d",
			journal.Cursor(), journal.BaseGeneration(), target)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	assertJournalDeltaImage(
		t, options, image, want, groups, target, "close-unsynced-carry",
	)
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

// TestBufferedJournalDeltaResumesAfterSuccessfulBatch protects the batch
// point-of-no-return bookkeeping. A successful batch owns its admitted frames
// after router publication and must clear only the unwind tracker. Otherwise a
// stale tracker permanently makes every later ordinary overlay Flush look like
// a mixed physical cut and silently disables the cheap journal-delta lane.
func TestBufferedJournalDeltaResumesAfterSuccessfulBatch(t *testing.T) {
	const documents = 320
	options := journalDeltaTestOptions()
	coll := buildJournalDeltaFallbackCollection(t, options)
	path := coll.file.Name()
	want := primaryStoreContent(t, coll)
	groups := make(map[string]int, documents)
	for at := range documents {
		groups[templateHeavyOverlayKey(at)] = at % 37
	}

	batchGroups := map[int]int{20: 57, 21: 58}
	if err := coll.Update(func(batch *WriteBatch) error {
		for at, group := range batchGroups {
			if err := batch.Put(
				[]byte(templateHeavyOverlayKey(at)),
				journalDeltaGroupDoc(at, group),
			); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("indexed batch: %v", err)
	}
	if len(coll.batchPrimaryAdmitted) != 0 {
		t.Fatalf("successful batch retained %d admitted-frame trackers",
			len(coll.batchPrimaryAdmitted))
	}
	for at, group := range batchGroups {
		key := templateHeavyOverlayKey(at)
		want[key] = journalDeltaCanonical(t, journalDeltaGroupDoc(at, group))
		groups[key] = group
	}
	beforeBatchFlush := coll.Stats()
	batchGeneration := coll.Generation()
	if err := coll.Flush(); err != nil {
		t.Fatalf("batch physical Flush: %v", err)
	}
	afterBatchFlush := coll.Stats()
	if afterBatchFlush.JournalDeltaCheckpoints !=
		beforeBatchFlush.JournalDeltaCheckpoints {
		t.Fatalf("batch Flush incorrectly used delta lane: %d -> %d",
			beforeBatchFlush.JournalDeltaCheckpoints,
			afterBatchFlush.JournalDeltaCheckpoints)
	}
	if coll.committer.DurableGeneration() != batchGeneration {
		t.Fatalf("batch physical generation = %d, want %d",
			coll.committer.DurableGeneration(), batchGeneration)
	}

	const pointAt, pointGroup = 22, 59
	pointKey := templateHeavyOverlayKey(pointAt)
	pointRaw := journalDeltaGroupDoc(pointAt, pointGroup)
	created, err := coll.Put([]byte(pointKey), pointRaw)
	if err != nil || created {
		t.Fatalf("eligible point replacement = %v,%v", created, err)
	}
	want[pointKey] = journalDeltaCanonical(t, pointRaw)
	groups[pointKey] = pointGroup
	if !coll.primaryUnifiedOverlay.hasPending() {
		t.Fatal("eligible point replacement produced no row overlay")
	}
	if !coll.bufferedJournalDeltaStateEligible(batchGeneration) {
		t.Fatal("post-batch point replacement is not delta-state eligible")
	}

	previousPostSync := recoveryJournalDeltaPostSyncHook
	defer func() { recoveryJournalDeltaPostSyncHook = previousPostSync }()
	var crashImage *journalCrashImage
	recoveryJournalDeltaPostSyncHook = func(uint64) {
		image := captureJournalImage(t, path)
		crashImage = &image
	}
	beforeDelta := coll.Stats()
	physicalBeforeDelta := coll.committer.DurableGeneration()
	target := coll.Generation()
	if err := coll.Flush(); err != nil {
		t.Fatalf("post-batch delta Flush: %v", err)
	}
	afterDelta := coll.Stats()
	if afterDelta.JournalDeltaCheckpoints !=
		beforeDelta.JournalDeltaCheckpoints+1 ||
		afterDelta.JournalDeltaRecords != beforeDelta.JournalDeltaRecords+1 ||
		afterDelta.JournalDeltaBytes <= beforeDelta.JournalDeltaBytes {
		t.Fatalf("post-batch delta metrics before=%+v after=%+v",
			beforeDelta, afterDelta)
	}
	if afterDelta.JournalDeltaFullFallbacks !=
		beforeDelta.JournalDeltaFullFallbacks {
		t.Fatalf("post-batch point Flush fell back physically: %d -> %d",
			beforeDelta.JournalDeltaFullFallbacks,
			afterDelta.JournalDeltaFullFallbacks)
	}
	if coll.DurableGeneration() != target ||
		coll.committer.DurableGeneration() != physicalBeforeDelta ||
		physicalBeforeDelta >= target {
		t.Fatalf("logical/physical generation = %d/%d, want %d/%d (< target)",
			coll.DurableGeneration(), coll.committer.DurableGeneration(),
			target, physicalBeforeDelta)
	}
	if !coll.primaryUnifiedOverlay.hasPending() {
		t.Fatal("delta Flush physically folded the eligible point overlay")
	}
	if crashImage == nil {
		t.Fatal("delta Flush did not reach post-sync crash seam")
	}
	assertJournalDeltaImage(
		t, options, *crashImage, want, groups, target,
		"successful-batch-then-delta",
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

// TestBufferedJournalDeltaOverlayPressureCarriesUnsynced fills one complete
// effective indexed-overlay window and publishes one more mutation. Pressure
// must append the complete old window without syncing, device-silently fold it,
// and retain the final mutation in the new overlay. Flush appends that one-record
// suffix and fences both batches with exactly one Sync. If one worst-case batch
// consumes the complete bounded journal, Flush must instead take the stronger
// physical drain: after a carried prefix there is mathematically no room to
// retain the required complete future-window reserve.
func TestBufferedJournalDeltaOverlayPressureCarriesUnsynced(t *testing.T) {
	options := journalDeltaTestOptions()
	getFault, restoreFault := installJournalFaultSeam(t)
	defer restoreFault()
	coll := buildJournalDeltaFallbackCollection(t, options)
	defer coll.Close()
	fault := getFault()
	if fault == nil {
		t.Fatal("journal fault seam was not installed")
	}
	key := []byte(templateHeavyOverlayKey(20))
	first := journalDeltaGroupDoc(20, 40)
	second := journalDeltaGroupDoc(20, 41)
	before := coll.committer.DurableGeneration()
	beforeSyncs := fault.Syncs()
	for i := range journalDeltaTestPressureMutations + 1 {
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
	target := coll.Generation()
	if got := coll.journalDeltaAppendedGeneration.Load(); got != target-1 {
		t.Fatalf("pressure appended generation = %d, want %d",
			got, target-1)
	}
	if got := coll.journalDeltaGeneration.Load(); got != before {
		t.Fatalf("pressure durable watermark = %d, want %d", got, before)
	}
	if got := fault.Syncs(); got != beforeSyncs {
		t.Fatalf("pressure issued %d journal sync(s), want 0",
			got-beforeSyncs)
	}
	physicalDrainRequired :=
		coll.bufferedJournalDeltaPhysicalDrainNeeded(0)
	firstCanonical := []byte(journalDeltaCanonical(t, first))
	if got, found, err := coll.AppendRaw(nil, key); err != nil || !found ||
		!bytes.Equal(got, firstCanonical) {
		t.Fatalf("post-pressure read = %q,%v,%v want %q",
			got, found, err, firstCanonical)
	}
	if err := coll.Flush(); err != nil {
		t.Fatal(err)
	}
	wantPhysical := before
	if physicalDrainRequired {
		wantPhysical = target
	}
	if got := coll.committer.DurableGeneration(); got != wantPhysical {
		t.Fatalf("pressure Flush physical durable = %d, want %d", got, wantPhysical)
	}
	if coll.DurableGeneration() != target ||
		coll.journalDeltaAppendedGeneration.Load() != target {
		t.Fatalf("post-pressure Flush durable=%d generation=%d",
			coll.DurableGeneration(), target)
	}
	wantSyncs := 1
	if physicalDrainRequired {
		wantSyncs = 2
	}
	if got := fault.Syncs(); got != beforeSyncs+wantSyncs {
		t.Fatalf("explicit Flush syncs = %d, want %d",
			got-beforeSyncs, wantSyncs)
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
