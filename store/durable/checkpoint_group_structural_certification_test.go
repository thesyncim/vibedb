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
	"github.com/thesyncim/vibejson"
)

const structuralCertificationBatchRows = 64

func TestCheckpointGroupCertificationSentinelRequiresExactError(t *testing.T) {
	if !isExactCheckpointGroupCertificationRequired(errCheckpointGroupCertificationRequired) {
		t.Fatal("exact certification sentinel was not recognized")
	}
	wrapped := fmt.Errorf("wrapped certification failure: %w", errCheckpointGroupCertificationRequired)
	joined := errors.Join(errCheckpointGroupCertificationRequired, errors.New("storage abort"))
	for name, err := range map[string]error{
		"wrapped":  wrapped,
		"joined":   joined,
		"pressure": ErrCheckpointGroupPressure,
	} {
		if isExactCheckpointGroupCertificationRequired(err) {
			t.Fatalf("%s error was accepted as an exact certification retry: %v", name, err)
		}
	}
}

func structuralCertificationOptions() Options {
	options := txnTestOptions()
	options.MaxBatchDocuments = structuralCertificationBatchRows
	options.MaxBatchBytes = 1 << 20
	options.ResidentBytes = 64 << 20
	return options
}

func structuralCertificationExactOptions(options Options) Options {
	options.Indexes = []store.IndexDefinition{{
		Name: "by_n", Paths: []string{"/n"},
	}}
	return options
}

func newStructuralCertificationGroup(
	t *testing.T,
) (string, []NamedCollection, *TxnLog, *CheckpointGroup, Options) {
	return newStructuralCertificationGroupWithOptions(t, structuralCertificationOptions())
}

func newStructuralCertificationGroupWithOptions(
	t *testing.T, options Options,
) (string, []NamedCollection, *TxnLog, *CheckpointGroup, Options) {
	t.Helper()
	dir := t.TempDir()
	members := []NamedCollection{
		openTxnNamedCollection(t, dir, "system", options),
		openTxnNamedCollection(t, dir, "user", options),
	}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatalf("NewTxnLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{
		CheckpointEvery: 128,
	})
	if err != nil {
		_ = log.Close()
		t.Fatalf("NewCheckpointGroup: %v", err)
	}
	t.Cleanup(func() { _ = group.Close() })
	return dir, members, log, group, options
}

func structuralCertificationBatch(
	t testing.TB,
	group *CheckpointGroup,
	members []NamedCollection,
	firstApplied uint64,
	writeNames map[string]bool,
	callbackCount *int,
) error {
	return structuralCertificationBatchWithValue(
		t, group, members, firstApplied, writeNames, callbackCount,
		func(row int) []byte {
			return []byte(fmt.Sprintf(`{"n":%d}`, row))
		},
	)
}

func structuralCertificationBatchWithValue(
	t testing.TB,
	group *CheckpointGroup,
	members []NamedCollection,
	firstApplied uint64,
	writeNames map[string]bool,
	callbackCount *int,
	valueFor func(int) []byte,
) error {
	t.Helper()
	lastApplied := firstApplied + structuralCertificationBatchRows - 1
	return group.UpdateConsecutive(
		firstApplied, lastApplied, members, defaultTxnLimits(),
		func(database *DatabaseBatch) error {
			(*callbackCount)++
			for _, member := range members {
				if !writeNames[member.Name] {
					continue
				}
				write, err := database.Collection(member.Name)
				if err != nil {
					return err
				}
				for row := int(firstApplied - 1); row < int(lastApplied); row++ {
					key := []byte(fmt.Sprintf("row-%08d", row))
					if err := write.Put(key, valueFor(row)); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}

func structuralCertificationWideValue(row int) []byte {
	return fmt.Appendf(
		nil, `{"n":%d,"payload":%q}`, row,
		structuralCertificationPayload(row, 128),
	)
}

func structuralCertificationPayload(row, size int) []byte {
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	payload := make([]byte, size)
	state := uint64(row+1) * 0x9e3779b97f4a7c15
	for index := range payload {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		state *= 0x2545f4914f6cdd1d
		payload[index] = alphabet[state%uint64(len(alphabet))]
	}
	return payload
}

// structuralCertificationUnindexedBoundaryRows uses the same canonical rows
// as the group test to find the 64 KiB compact-stripe boundary. Rounding down
// to a whole publication leaves the next 64-row batch as the first overflowing
// publication, independently of the maintained 256-slot geometry.
func structuralCertificationUnindexedBoundaryRows(t testing.TB) int {
	t.Helper()
	records := make([]storeio.CommonPrimaryLeafRecord, storeio.CompactPrimaryStripeMaxRows)
	for row := range records {
		records[row] = storeio.CommonPrimaryLeafRecord{
			Key: []byte(fmt.Sprintf("row-%08d", row)),
			Value: storeio.CommonPrimaryLeafValue{
				Inline: structuralCertificationWideValue(row),
			},
		}
	}
	planner, err := storeio.NewPrimaryValueLeafWindowPlanner(nil)
	if err != nil {
		t.Fatalf("create compact boundary planner: %v", err)
	}
	count, extent, _, err := planner.PlanUnplaced(
		records, storeio.CommonPrimaryLeafMaxExtentBytes,
	)
	if err != nil {
		t.Fatalf("plan compact boundary: %v", err)
	}
	if count <= storeio.CommonPrimaryLeafWideSlots ||
		count >= storeio.CompactPrimaryStripeMaxRows ||
		extent > storeio.CommonPrimaryLeafMaxExtentBytes {
		t.Fatalf("compact boundary = %d rows/%d bytes, want byte-limited range in (%d,%d)",
			count, extent, storeio.CommonPrimaryLeafWideSlots,
			storeio.CompactPrimaryStripeMaxRows)
	}
	seedRows := count / structuralCertificationBatchRows * structuralCertificationBatchRows
	if seedRows <= storeio.CommonPrimaryLeafWideSlots || seedRows+structuralCertificationBatchRows <= count {
		t.Fatalf("rounded compact boundary = %d of %d rows, next batch would not overflow", seedRows, count)
	}
	return seedRows
}

func structuralNamedCollectionsFromHandles(
	collections []*Collection,
) []NamedCollection {
	result := make([]NamedCollection, len(collections))
	for index, collection := range collections {
		name := "system"
		if index == 1 {
			name = "user"
		}
		result[index] = NamedCollection{Name: name, Collection: collection}
	}
	return result
}

func requireStructuralCertificationRows(
	t testing.TB, collection *Collection, first, last int, present bool,
) {
	requireStructuralCertificationRowsWithValue(
		t, collection, first, last, present,
		func(row int) []byte { return []byte(fmt.Sprintf(`{"n":%d}`, row)) },
	)
}

func requireStructuralCertificationRowsWithValue(
	t testing.TB, collection *Collection, first, last int, present bool,
	valueFor func(int) []byte,
) {
	t.Helper()
	for row := first; row < last; row++ {
		key := []byte(fmt.Sprintf("row-%08d", row))
		want := valueFor(row)
		got, found, err := collection.AppendRaw(nil, key)
		if err != nil {
			t.Fatalf("row %d read: %v", row, err)
		}
		if found != present {
			t.Fatalf("row %d found=%v, want %v", row, found, present)
		}
		if present && string(got) != string(want) {
			t.Fatalf("row %d = %q, want %q", row, got, want)
		}
	}
}

func requireStructuralCertificationSnapshotRows(
	t testing.TB, snapshot *Snapshot, first, last int, present bool,
) {
	requireStructuralCertificationSnapshotRowsWithValue(
		t, snapshot, first, last, present,
		func(row int) []byte { return []byte(fmt.Sprintf(`{"n":%d}`, row)) },
	)
}

func requireStructuralCertificationSnapshotRowsWithValue(
	t testing.TB, snapshot *Snapshot, first, last int, present bool,
	valueFor func(int) []byte,
) {
	t.Helper()
	for row := first; row < last; row++ {
		key := []byte(fmt.Sprintf("row-%08d", row))
		want := valueFor(row)
		got, found, err := snapshot.AppendRaw(nil, key)
		if err != nil {
			t.Fatalf("snapshot row %d read: %v", row, err)
		}
		if found != present {
			t.Fatalf("snapshot row %d found=%v, want %v", row, found, present)
		}
		if present && string(got) != string(want) {
			t.Fatalf("snapshot row %d = %q, want %q", row, got, want)
		}
	}
}

func openStructuralCertificationCopy(
	t *testing.T, dir string, options Options,
) ([]*Collection, *TxnLog, *CheckpointGroup) {
	t.Helper()
	names := []string{"system", "user"}
	requests := make([]TransactionCollectionOpen, len(names))
	files := make([]*os.File, len(names))
	for index, name := range names {
		file, err := os.OpenFile(filepath.Join(dir, name+".vjc"), os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("open copied member %q: %v", name, err)
		}
		files[index] = file
		requests[index] = TransactionCollectionOpen{File: file, Options: options}
	}
	collections, log, group, err := OpenCollectionsWithCheckpointGroup(
		dir, TxnLogOptions{}, requests, names,
		CheckpointGroupOptions{CheckpointEvery: 128},
	)
	if err != nil {
		for _, file := range files {
			_ = file.Close()
		}
		t.Fatalf("open copied checkpoint group: %v", err)
	}
	t.Cleanup(func() {
		_ = group.Close()
		for _, collection := range collections {
			_ = collection.Close()
		}
		_ = log.Close()
		for _, file := range files {
			_ = file.Close()
		}
	})
	return collections, log, group
}

// TestCheckpointGroupStructuralSplitCertifiesOnlyPrecedingCut exercises a
// byte-limited unindexed compact stripe wider than 256 rows. The next 64-row
// publication requires a structural split. Certification advances only the
// preceding group cut; the split collection performs its own durable
// structural flush while the idle member remains at its prior physical root.
// The failed-prepare image contains only the newly certified preceding cut and
// no decision; it must reopen at that exact prefix and accept the next-index
// retry. A later successful copy contains mixed member roots plus an
// uncertified decision and exercises the same recovery rule.
func TestCheckpointGroupStructuralSplitCertifiesOnlyPrecedingCut(t *testing.T) {
	dir, members, _, group, options := newStructuralCertificationGroup(t)
	seedRows := structuralCertificationUnindexedBoundaryRows(t)
	seedBatches := seedRows / structuralCertificationBatchRows
	finalRows := seedRows + structuralCertificationBatchRows
	allMembers := map[string]bool{"system": true, "user": true}
	targetOnly := map[string]bool{"system": true}
	callbackCount := 0
	var heldSnapshot *Snapshot
	for batch := 0; batch < seedBatches; batch++ {
		first := uint64(batch*structuralCertificationBatchRows + 1)
		if err := structuralCertificationBatchWithValue(
			t, group, members, first, allMembers, &callbackCount,
			structuralCertificationWideValue,
		); err != nil {
			t.Fatalf("seed batch %d: %v", batch, err)
		}
		if batch == 0 {
			if err := group.Checkpoint(); err != nil {
				t.Fatalf("checkpoint first seed batch: %v", err)
			}
			beforeSnapshot := group.Stats()
			var err error
			heldSnapshot, err = members[0].Collection.Snapshot()
			if err != nil {
				t.Fatalf("post-first-batch snapshot: %v", err)
			}
			t.Cleanup(func() { _ = heldSnapshot.Close() })
			if heldSnapshot.Len() != structuralCertificationBatchRows {
				t.Fatalf("held snapshot rows = %d, want %d",
					heldSnapshot.Len(), structuralCertificationBatchRows)
			}
			if afterSnapshot := group.Stats(); afterSnapshot != beforeSnapshot {
				t.Fatalf("snapshot changed group state: before=%+v after=%+v",
					beforeSnapshot, afterSnapshot)
			}
			requireStructuralCertificationSnapshotRowsWithValue(
				t, heldSnapshot, 0, structuralCertificationBatchRows, true,
				structuralCertificationWideValue,
			)
			requireStructuralCertificationSnapshotRowsWithValue(
				t, heldSnapshot, structuralCertificationBatchRows,
				finalRows, false, structuralCertificationWideValue,
			)
		}
	}
	if got := members[0].Collection.primaryRouter.Load().Len(); got != 1 {
		t.Fatalf("pre-split unindexed router leaves = %d, want one", got)
	}
	preSplitRoute, ok := members[0].Collection.primaryRouter.Load().RouteAtRank(0)
	if !ok {
		t.Fatal("pre-split unindexed route missing")
	}
	preSplitLease, err := members[0].Collection.cache.Acquire(preSplitRoute.Ref)
	if err != nil {
		t.Fatalf("acquire pre-split unindexed stripe: %v", err)
	}
	preSplitStripe, valid := storeio.AdmittedCompactPrimaryStripe(
		preSplitLease.Page(), members[0].Collection.storeID, preSplitRoute.Bucket,
	)
	preSplitLease.Release()
	if !valid || preSplitStripe.Len() != seedRows ||
		preSplitStripe.Len() <= storeio.CommonPrimaryLeafWideSlots {
		t.Fatalf("pre-split unindexed stripe = %d rows/valid=%v, want %d rows wider than 256",
			preSplitStripe.Len(), valid, seedRows)
	}
	before := group.Stats()
	if before.AppliedIndex != uint64(seedRows) ||
		before.CheckpointAppliedIndex != structuralCertificationBatchRows ||
		before.TransactionHighWater != uint64(seedBatches) ||
		before.CheckpointTransactions != 1 || before.PhysicalCheckpoints != 2 {
		t.Fatalf("pre-split stats = %+v", before)
	}
	targetGenerationBefore := members[0].Collection.Generation()
	targetSplitsBefore := members[0].Collection.Stats().PrimaryLeafSplits
	idleDurableBefore := members[1].Collection.DurableGeneration()

	fault := errors.New("stop after structural prepare")
	preparedAfterCertification := false
	previousHook := checkpointGroupFaultHook
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if point == checkpointGroupAfterPrepareAppend {
			preparedAfterCertification = group.certTxn.Load() == group.txn
			return fault
		}
		return nil
	}
	err = structuralCertificationBatchWithValue(
		t, group, members, uint64(seedRows+1), targetOnly, &callbackCount,
		structuralCertificationWideValue,
	)
	checkpointGroupFaultHook = previousHook
	if !errors.Is(err, fault) {
		t.Fatalf("faulted split admission = %v, want %v", err, fault)
	}
	if !preparedAfterCertification {
		t.Fatal("fault hook did not observe local shape after preceding certification")
	}
	if callbackCount != seedBatches+1 {
		t.Fatalf("faulted split callbacks = %d, want %d", callbackCount, seedBatches+1)
	}
	faulted := group.Stats()
	if faulted.AppliedIndex != before.AppliedIndex ||
		faulted.TransactionHighWater != before.TransactionHighWater ||
		faulted.CheckpointAppliedIndex != before.AppliedIndex ||
		faulted.CheckpointTransactions != before.TransactionHighWater ||
		faulted.Checkpoints != before.Checkpoints+1 ||
		faulted.PhysicalCheckpoints != before.PhysicalCheckpoints {
		t.Fatalf("faulted split state before=%+v after=%+v", before, faulted)
	}
	if got := members[0].Collection.Generation(); got <= targetGenerationBefore {
		t.Fatalf("faulted split did not publish its content-equivalent shape: generation=%d before=%d",
			got, targetGenerationBefore)
	}
	if got := members[0].Collection.Stats().PrimaryLeafSplits; got != targetSplitsBefore+1 {
		t.Fatalf("faulted split count=%d, want %d", got, targetSplitsBefore+1)
	}
	if got := members[1].Collection.DurableGeneration(); got != idleDurableBefore {
		t.Fatalf("faulted split advanced idle generation to %d from %d", got, idleDurableBefore)
	}
	// The post-prepare hook simulates an interruption after an uncommitted
	// conditional prepare. Its API contract requires reopening before any
	// retry, so the original owner must still expose only the certified prefix
	// while retaining the old snapshot view.
	for _, member := range members {
		requireStructuralCertificationRowsWithValue(
			t, member.Collection, 0, seedRows, true, structuralCertificationWideValue,
		)
		requireStructuralCertificationRowsWithValue(
			t, member.Collection, seedRows, finalRows, false, structuralCertificationWideValue,
		)
	}
	requireStructuralCertificationSnapshotRowsWithValue(
		t, heldSnapshot, 0, structuralCertificationBatchRows, true,
		structuralCertificationWideValue,
	)
	requireStructuralCertificationSnapshotRowsWithValue(
		t, heldSnapshot, structuralCertificationBatchRows,
		finalRows, false, structuralCertificationWideValue,
	)
	faultImage := copyCheckpointGroupDirectory(t, dir)

	// The failed prepare has no decision. The preceding certificate is the only
	// durable change, so a copied image must reopen at the exact seed cut and
	// accept the same next-index batch without evaluating it twice.
	faultRecovered, _, faultReopened := openStructuralCertificationCopy(t, faultImage, options)
	faultRecoveredStats := faultReopened.Stats()
	if faultRecoveredStats.AppliedIndex != uint64(seedRows) ||
		faultRecoveredStats.CheckpointAppliedIndex != uint64(seedRows) ||
		faultRecoveredStats.TransactionHighWater != uint64(seedBatches) {
		t.Fatalf("faulted image recovered stats = %+v", faultRecoveredStats)
	}
	for _, collection := range faultRecovered {
		requireStructuralCertificationRowsWithValue(
			t, collection, 0, seedRows, true, structuralCertificationWideValue,
		)
		requireStructuralCertificationRowsWithValue(
			t, collection, seedRows, finalRows, false, structuralCertificationWideValue,
		)
	}
	faultRecoveryCallbacks := 0
	if err := structuralCertificationBatchWithValue(
		t, faultReopened, structuralNamedCollectionsFromHandles(faultRecovered),
		uint64(seedRows+1), targetOnly, &faultRecoveryCallbacks,
		structuralCertificationWideValue,
	); err != nil {
		t.Fatalf("faulted image exact retry: %v", err)
	}
	if faultRecoveryCallbacks != 1 {
		t.Fatalf("faulted image retry callbacks = %d, want 1", faultRecoveryCallbacks)
	}
	// Preserve a copy immediately after the successful exact replay, before a
	// full checkpoint. It contains the mixed physical roots and the
	// uncertified decision that recovery must discard and replay.
	mixedImage := copyCheckpointGroupDirectory(t, faultImage)
	if err := faultReopened.Checkpoint(); err != nil {
		t.Fatalf("faulted image final checkpoint: %v", err)
	}
	if got := faultReopened.CheckpointAppliedIndex(); got != uint64(finalRows) {
		t.Fatalf("faulted image final checkpoint = %d, want %d", got, finalRows)
	}
	requireStructuralCertificationRowsWithValue(
		t, faultRecovered[0], 0, finalRows, true, structuralCertificationWideValue,
	)
	requireStructuralCertificationRowsWithValue(
		t, faultRecovered[1], 0, seedRows, true, structuralCertificationWideValue,
	)

	mixedRecovered, _, mixedReopened := openStructuralCertificationCopy(t, mixedImage, options)
	mixedStats := mixedReopened.Stats()
	if mixedStats.AppliedIndex != uint64(seedRows) ||
		mixedStats.CheckpointAppliedIndex != uint64(seedRows) ||
		mixedStats.TransactionHighWater != uint64(seedBatches) {
		t.Fatalf("mixed-root recovered stats = %+v", mixedStats)
	}
	for _, collection := range mixedRecovered {
		requireStructuralCertificationRowsWithValue(
			t, collection, 0, seedRows, true, structuralCertificationWideValue,
		)
		requireStructuralCertificationRowsWithValue(
			t, collection, seedRows, finalRows, false, structuralCertificationWideValue,
		)
	}
	callbackCount = 0
	if err := structuralCertificationBatchWithValue(
		t, mixedReopened, structuralNamedCollectionsFromHandles(mixedRecovered),
		uint64(seedRows+1), targetOnly, &callbackCount, structuralCertificationWideValue,
	); err != nil {
		t.Fatalf("mixed-root exact next-index retry: %v", err)
	}
	if callbackCount != 1 {
		t.Fatalf("mixed-root recovery callbacks = %d, want 1", callbackCount)
	}
	if err := mixedReopened.Checkpoint(); err != nil {
		t.Fatalf("mixed-root recovery full checkpoint: %v", err)
	}
	if got := mixedReopened.CheckpointAppliedIndex(); got != uint64(finalRows) {
		t.Fatalf("mixed-root recovered final checkpoint = %d, want %d", got, finalRows)
	}
	requireStructuralCertificationRowsWithValue(
		t, mixedRecovered[0], 0, finalRows, true, structuralCertificationWideValue,
	)
	requireStructuralCertificationRowsWithValue(
		t, mixedRecovered[1], 0, seedRows, true, structuralCertificationWideValue,
	)
}

// TestCheckpointGroupStructuralSplitCertificatePreflightRejectsTerminalMember
// verifies that the certification retry remains an all-member preflight. A
// terminal recycle counter on the idle member rejects the split before any
// certificate, root, marker, or logical publication changes; repairing that
// member permits the exact same batch to retry once.
func TestCheckpointGroupStructuralSplitCertificatePreflightRejectsTerminalMember(t *testing.T) {
	indexedOptions := structuralCertificationExactOptions(structuralCertificationOptions())
	dir, members, _, group, _ := newStructuralCertificationGroupWithOptions(t, indexedOptions)
	allMembers := map[string]bool{"system": true, "user": true}
	targetOnly := map[string]bool{"system": true}
	callbackCount := 0
	for batch := 0; batch < 4; batch++ {
		if err := structuralCertificationBatch(
			t, group, members,
			uint64(batch*structuralCertificationBatchRows+1),
			allMembers, &callbackCount,
		); err != nil {
			t.Fatalf("seed batch %d: %v", batch, err)
		}
	}
	idle := members[1].Collection
	originalRecycleCount := idle.journal.Header().RecycleCount
	checkpointGroupTestRewriteLiveJournalHeader(
		t, group, idle,
		func(header *storeio.RecoveryJournalHeader) {
			header.RecycleCount = ^uint64(0)
		},
	)
	beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
	beforeStats := group.Stats()
	beforeTargetGeneration := members[0].Collection.Generation()
	if err := structuralCertificationBatch(
		t, group, members[:1], 257, targetOnly, &callbackCount,
	); !errors.Is(err, ErrCheckpointGroupSequence) {
		t.Fatalf("terminal split admission = %v, want sequence exhaustion", err)
	}
	if callbackCount != 5 {
		t.Fatalf("terminal rejection callbacks = %d, want one callback before certifier preflight", callbackCount)
	}
	if got := members[0].Collection.Generation(); got != beforeTargetGeneration {
		t.Fatalf("terminal rejection advanced target generation to %d from %d",
			got, beforeTargetGeneration)
	}
	if after := group.Stats(); after != beforeStats {
		t.Fatalf("terminal rejection changed stats: before=%+v after=%+v", beforeStats, after)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, beforeDirectory)
	checkpointGroupTestRewriteLiveJournalHeader(
		t, group, idle,
		func(header *storeio.RecoveryJournalHeader) {
			header.RecycleCount = originalRecycleCount
		},
	)
	if err := structuralCertificationBatch(
		t, group, members[:1], 257, targetOnly, &callbackCount,
	); err != nil {
		t.Fatalf("repaired exact split retry: %v", err)
	}
	if callbackCount != 6 {
		t.Fatalf("repaired retry callbacks = %d, want one callback after certifier preflight", callbackCount)
	}
}

// TestCheckpointGroupStructuralSplitMixedMemberRoots exercises the mixed-root
// state after structural certification. The narrow member performs the local
// physical fold for the split while the wider member has only a certified
// journal suffix and stays at its previous physical root. Recovery discards the
// uncertified split and accepts the exact next-index replay under the same
// asymmetric public options.
func TestCheckpointGroupStructuralSplitMixedMemberRoots(t *testing.T) {
	smallOptions := structuralCertificationExactOptions(syncPrimaryJournalTestOptions())
	smallOptions.MaxBatchDocuments = 1
	smallOptions.MaxKeyBytes = 64
	smallOptions.InlineValueBytes = 512
	smallOptions.MaxDocumentBytes = 512
	// Leave ample sealed room for the one-row structural transaction itself;
	// the 1 MiB versus 2 MiB asymmetry still makes the later member-local
	// journal fold observable without forcing a group-wide pressure checkpoint
	// during the split admission.
	smallOptions.SealedRecoveryJournalBytes = 1 << 20
	smallOptions.PortableSealedCapacity = true
	wideOptions := smallOptions
	wideOptions.SealedRecoveryJournalBytes = 2 << 20
	for name, options := range map[string]Options{
		"small": smallOptions,
		"wide":  wideOptions,
	} {
		if _, err := options.normalized(); err != nil {
			t.Fatalf("%s member options: %v", name, err)
		}
	}

	dir := t.TempDir()
	members := []NamedCollection{
		openTxnNamedCollection(t, dir, "system", smallOptions),
		openTxnNamedCollection(t, dir, "user", wideOptions),
	}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatalf("NewTxnLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{
		CheckpointEvery: 128,
	})
	if err != nil {
		_ = log.Close()
		t.Fatalf("NewCheckpointGroup: %v", err)
	}
	t.Cleanup(func() { _ = group.Close() })

	callbackCount := 0
	putRow := func(applied uint64, memberName string, row int) error {
		return group.Update(applied, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
			callbackCount++
			write, err := batch.Collection(memberName)
			if err != nil {
				return err
			}
			key := []byte(fmt.Sprintf("row-%08d", row))
			value := []byte(fmt.Sprintf(`{"n":%d}`, row))
			return write.Put(key, value)
		})
	}
	for row := 0; row < 256; row++ {
		if err := putRow(uint64(row+1), "system", row); err != nil {
			t.Fatalf("seed row %d: %v", row, err)
		}
	}
	// Keep the small sealed journal from becoming the reason for a group
	// pressure retry during setup. The split below is the only update whose
	// local fold is observed in the mixed physical state.
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	if err := putRow(257, "user", 0); err != nil {
		t.Fatalf("uncertified idle-member suffix: %v", err)
	}
	beforeSplit := group.Stats()
	if beforeSplit.TransactionHighWater != 257 ||
		beforeSplit.CheckpointTransactions != 256 ||
		beforeSplit.CheckpointAppliedIndex != 256 {
		t.Fatalf("pre-split certified suffix = %+v", beforeSplit)
	}
	small := members[0].Collection
	wide := members[1].Collection
	smallBefore := small.DurableGeneration()
	wideBefore := wide.DurableGeneration()
	physicalBefore := beforeSplit.PhysicalCheckpoints
	if err := putRow(258, "system", 256); err != nil {
		t.Fatalf("structural split with asymmetric members: %v", err)
	}
	afterSplit := group.Stats()
	if afterSplit.AppliedIndex != 258 || afterSplit.CheckpointAppliedIndex != 257 ||
		afterSplit.TransactionHighWater != 258 || afterSplit.CheckpointTransactions != 257 ||
		afterSplit.Checkpoints != beforeSplit.Checkpoints+1 ||
		afterSplit.PhysicalCheckpoints != physicalBefore ||
		afterSplit.PressureCheckpoints != beforeSplit.PressureCheckpoints {
		t.Fatalf("mixed structural state before=%+v after=%+v", beforeSplit, afterSplit)
	}
	if small.DurableGeneration() <= smallBefore || wide.DurableGeneration() != wideBefore {
		t.Fatalf("mixed structural roots small=%d/%d wide=%d/%d",
			small.DurableGeneration(), smallBefore, wide.DurableGeneration(), wideBefore)
	}
	if callbackCount != 258 {
		t.Fatalf("structural callbacks = %d, want one callback per update", callbackCount)
	}
	crashImage := copyCheckpointGroupDirectory(t, dir)

	requests := make([]TransactionCollectionOpen, 2)
	files := make([]*os.File, 2)
	names := []string{"system", "user"}
	options := []Options{smallOptions, wideOptions}
	for index, name := range names {
		file, openErr := os.OpenFile(filepath.Join(crashImage, name+".vjc"), os.O_RDWR, 0)
		if openErr != nil {
			t.Fatal(openErr)
		}
		files[index] = file
		requests[index] = TransactionCollectionOpen{File: file, Options: options[index]}
	}
	recoveredCollections, recoveredLog, recoveredGroup, openErr :=
		OpenCollectionsWithCheckpointGroup(
			crashImage, TxnLogOptions{}, requests, names,
			CheckpointGroupOptions{CheckpointEvery: 128},
		)
	if openErr != nil {
		for _, file := range files {
			_ = file.Close()
		}
		t.Fatalf("reopen mixed structural suffix: %v", openErr)
	}
	t.Cleanup(func() {
		_ = recoveredGroup.Close()
		for _, collection := range recoveredCollections {
			_ = collection.Close()
		}
		_ = recoveredLog.Close()
		for _, file := range files {
			_ = file.Close()
		}
	})
	recoveredStats := recoveredGroup.Stats()
	if recoveredStats.AppliedIndex != 257 ||
		recoveredStats.CheckpointAppliedIndex != 257 ||
		recoveredStats.TransactionHighWater != 257 ||
		recoveredStats.CheckpointTransactions != 257 {
		t.Fatalf("recovered mixed structural cut = %+v", recoveredStats)
	}
	requireStructuralCertificationRows(t, recoveredCollections[0], 0, 256, true)
	requireStructuralCertificationRows(t, recoveredCollections[0], 256, 320, false)
	requireStructuralCertificationRows(t, recoveredCollections[1], 0, 1, true)
	requireStructuralCertificationRows(t, recoveredCollections[1], 1, 320, false)
	if err := recoveredGroup.Update(258, structuralNamedCollectionsFromHandles(recoveredCollections),
		defaultTxnLimits(), func(batch *DatabaseBatch) error {
			write, err := batch.Collection("system")
			if err != nil {
				return err
			}
			return write.Put([]byte("row-00000256"), []byte(`{"n":256}`))
		}); err != nil {
		t.Fatalf("exact next-index structural replay: %v", err)
	}
	if err := recoveredGroup.Checkpoint(); err != nil {
		t.Fatalf("mixed structural replay checkpoint: %v", err)
	}
	finalStats := recoveredGroup.Stats()
	if finalStats.AppliedIndex != 258 || finalStats.CheckpointAppliedIndex != 258 ||
		finalStats.TransactionHighWater != 258 || finalStats.CheckpointTransactions != 258 {
		t.Fatalf("mixed structural final stats = %+v", finalStats)
	}
	requireStructuralCertificationRows(t, recoveredCollections[0], 0, 257, true)
	requireStructuralCertificationRows(t, recoveredCollections[0], 257, 320, false)
	requireStructuralCertificationRows(t, recoveredCollections[1], 0, 1, true)
	requireStructuralCertificationRows(t, recoveredCollections[1], 1, 320, false)
}

// TestCheckpointGroupStructuralSplitCertifiesBeforeOverlayPressure proves the
// two bounded retries in one real transition. The first dirty member needs a
// leaf split with no pending overlay, so it emits the private certification
// sentinel. The second dirty member then crosses its supported public overlay
// bucket window, forcing the ordinary full-group pressure checkpoint. The
// callback and logical transaction remain single-shot throughout both retries.
func TestCheckpointGroupStructuralSplitCertifiesBeforeOverlayPressure(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	options.MaxBatchDocuments = structuralCertificationBatchRows
	options.InlineValueBytes = 1024
	options.MaxDocumentBytes = 1 << 10
	options.BufferCount = 768
	options.ResidentBytes = 32 << 20
	normalizedOptions, err := options.normalized()
	if err != nil {
		t.Fatalf("overlay pressure options: %v", err)
	}
	systemOptions := structuralCertificationExactOptions(options)
	// Exact posting maintenance adds bounded descriptors to the system
	// transaction geometry; leave the user collection at the smaller arena
	// because it is the member whose overlay bucket limit this test crosses.
	systemOptions.BufferCount = 1024
	if _, err := systemOptions.normalized(); err != nil {
		t.Fatalf("indexed system options: %v", err)
	}

	dir := t.TempDir()
	members := []NamedCollection{
		openTxnNamedCollection(t, dir, "system", systemOptions),
		openTxnNamedCollection(t, dir, "user", options),
	}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatalf("NewTxnLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{
		CheckpointEvery: 128,
	})
	if err != nil {
		t.Fatalf("NewCheckpointGroup: %v", err)
	}
	t.Cleanup(func() { _ = group.Close() })

	const corpusSize = 12_000
	keys := make([]string, corpusSize)
	for index := range keys {
		keys[index] = fmt.Sprintf("primary-key-%09d", index)
	}
	canonicalValue := func(value int) []byte {
		raw := fmt.Appendf(
			nil, `{"v":%d,"w":10,"payload":%q}`,
			value, strings.Repeat("x", 256),
		)
		canonical, err := vibejson.AppendCanonicalize(nil, raw)
		if err != nil {
			t.Fatalf("canonical pressure value %d: %v", value, err)
		}
		return canonical
	}
	seedValue := func(row int) []byte {
		raw := fmt.Appendf(
			nil, `{"v":0,"w":10,"payload":%q}`,
			structuralCertificationPayload(row, 768),
		)
		canonical, err := vibejson.AppendCanonicalize(nil, raw)
		if err != nil {
			t.Fatalf("canonical pressure seed row %d: %v", row, err)
		}
		return canonical
	}
	valueFor := func(value int) []byte {
		return canonicalValue(value)
	}
	writeRange := func(
		member NamedCollection, firstApplied uint64, start, end int,
		value func(int) []byte,
	) error {
		return group.UpdateConsecutive(
			firstApplied, firstApplied+uint64(end-start)-1,
			[]NamedCollection{member}, defaultTxnLimits(),
			func(batch *DatabaseBatch) error {
				write, err := batch.Collection(member.Name)
				if err != nil {
					return err
				}
				for index := start; index < end; index++ {
					if err := write.Put([]byte(keys[index]), value(index)); err != nil {
						return err
					}
				}
				return nil
			},
		)
	}
	// The system's exact /n index retains the 256-slot geometry, so four
	// 64-row publications fill one leaf. User rows are seeded separately so the
	// later user overlay can be the second dirty member after the system split
	// has crossed the certification boundary.
	for batch := 0; batch < 4; batch++ {
		start := batch * structuralCertificationBatchRows
		if err := writeRange(
			members[0], uint64(start+1), start, start+structuralCertificationBatchRows,
			func(index int) []byte {
				return []byte(fmt.Sprintf(`{"n":%d}`, index))
			},
		); err != nil {
			t.Fatalf("system seed batch %d: %v", batch, err)
		}
	}
	userSeedStart := 256
	userSeedRows := corpusSize
	for start := 0; start < userSeedRows; start += structuralCertificationBatchRows {
		end := min(start+structuralCertificationBatchRows, userSeedRows)
		firstApplied := uint64(userSeedStart + start + 1)
		if err := writeRange(
			members[1], firstApplied, start, end,
			seedValue,
		); err != nil {
			t.Fatalf("user seed range [%d,%d): %v", start, end, err)
		}
	}
	// Saturated, high-entropy 64 KiB leaves decline changed-row overlay batches.
	// Rewrite the corpus with same-shape short values and checkpoint once; this
	// preserves its route buckets while leaving scalar overlay replacements room
	// below the leaf extent ceiling for the actual pressure exercise.
	countBuckets := func() int {
		state := members[1].Collection.state.Load()
		seen := make(map[storeio.BucketID]struct{})
		for _, key := range keys {
			route, routeErr := members[1].Collection.currentPrimaryResidentRoute(
				state, []byte(key),
			)
			if routeErr != nil {
				t.Fatalf("route compacted user key %q: %v", key, routeErr)
			}
			seen[route.Bucket] = struct{}{}
		}
		return len(seen)
	}
	userBucketsBeforeShrink := countBuckets()
	userShrinkStart := userSeedStart + userSeedRows
	for start := 0; start < userSeedRows; start += structuralCertificationBatchRows {
		end := min(start+structuralCertificationBatchRows, userSeedRows)
		firstApplied := uint64(userShrinkStart + start + 1)
		if err := writeRange(
			members[1], firstApplied, start, end,
			func(int) []byte { return canonicalValue(0) },
		); err != nil {
			t.Fatalf("user compacting range [%d,%d): %v", start, end, err)
		}
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	if userBucketsAfterShrink := countBuckets(); userBucketsAfterShrink < userBucketsBeforeShrink ||
		userBucketsAfterShrink <= normalizedOptions.primaryUnifiedOverlayBuckets {
		t.Fatalf("user route buckets after compacting = %d, before = %d, overlay limit = %d",
			userBucketsAfterShrink, userBucketsBeforeShrink,
			normalizedOptions.primaryUnifiedOverlayBuckets)
	}
	seedApplied := uint64(userShrinkStart + userSeedRows)
	baseline := group.Stats()
	if baseline.AppliedIndex != seedApplied ||
		baseline.CheckpointAppliedIndex != seedApplied ||
		baseline.TransactionHighWater != baseline.CheckpointTransactions {
		t.Fatalf("seed group state = %+v", baseline)
	}

	user := members[1].Collection
	pressureLimit := user.options.primaryUnifiedOverlayBuckets
	if pressureLimit <= 1 || pressureLimit >= primaryUnifiedOverlayBuckets {
		t.Fatalf("overlay pressure limit = %d, want (1,%d)",
			pressureLimit, primaryUnifiedOverlayBuckets)
	}
	state := user.state.Load()
	seen := make(map[storeio.BucketID]struct{}, pressureLimit+1)
	targets := make([]int, 0, pressureLimit+1)
	for index, key := range keys {
		route, routeErr := user.currentPrimaryResidentRoute(state, []byte(key))
		if routeErr != nil {
			t.Fatalf("route seed key %q: %v", key, routeErr)
		}
		if route.Ref.Length >= storeio.CommonPrimaryLeafMaxExtentBytes {
			t.Fatalf("compacted user bucket %d extent=%d, want headroom below %d",
				route.Bucket, route.Ref.Length, storeio.CommonPrimaryLeafMaxExtentBytes)
		}
		if _, exists := seen[route.Bucket]; exists {
			continue
		}
		seen[route.Bucket] = struct{}{}
		targets = append(targets, index)
		if len(targets) == pressureLimit+1 {
			break
		}
	}
	if len(targets) != pressureLimit+1 {
		t.Fatalf("routed pressure targets = %d, want %d", len(targets), pressureLimit+1)
	}
	preCount := min(pressureLimit-1, structuralCertificationBatchRows)
	if preCount <= 0 || pressureLimit+1-preCount > structuralCertificationBatchRows {
		t.Fatalf("overlay pressure split sizes pre=%d limit=%d", preCount, pressureLimit)
	}
	writeTargets := func(
		applied uint64, targetMembers []NamedCollection, start, end, value int,
	) error {
		return group.Update(applied, targetMembers, defaultTxnLimits(), func(batch *DatabaseBatch) error {
			for _, member := range targetMembers {
				write, err := batch.Collection(member.Name)
				if err != nil {
					return err
				}
				for _, target := range targets[start:end] {
					if err := write.Put([]byte(keys[target]), valueFor(value)); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	if err := writeTargets(seedApplied+1, members[1:], 0, preCount, 1); err != nil {
		t.Fatalf("uncertified overlay prefix: %v", err)
	}
	before := group.Stats()
	beforeOverlayBuckets := user.primaryUnifiedOverlay.bucketCount.Load()
	if before.CheckpointAppliedIndex != baseline.CheckpointAppliedIndex ||
		before.TransactionHighWater != baseline.TransactionHighWater+1 ||
		int(beforeOverlayBuckets) != preCount || int(beforeOverlayBuckets) >= pressureLimit {
		t.Fatalf("overlay prefix state = %+v buckets=%d limit=%d",
			before, beforeOverlayBuckets, pressureLimit)
	}
	if int(beforeOverlayBuckets)+pressureLimit+1-preCount <= pressureLimit {
		t.Fatalf("overlay batch would not cross limit: buckets=%d pre=%d limit=%d",
			beforeOverlayBuckets, preCount, pressureLimit)
	}

	system := members[0].Collection
	systemGenerationBefore := system.Generation()
	systemSplitsBefore := system.Stats().PrimaryLeafSplits
	physicalHooks := 0
	splitObservedDuringPhysicalFold := false
	previousHook := checkpointGroupFaultHook
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if point == checkpointGroupAfterPhysicalCheckpoint {
			physicalHooks++
			if group.certTxn.Load() == group.txn &&
				system.Generation() > systemGenerationBefore &&
				system.Stats().PrimaryLeafSplits > systemSplitsBefore {
				splitObservedDuringPhysicalFold = true
			}
		}
		return nil
	}
	callbackCount := 0
	err = group.Update(seedApplied+2, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		callbackCount++
		system, err := batch.Collection("system")
		if err != nil {
			return err
		}
		if err := system.Put([]byte("primary-key-000000256"), []byte(`{"n":256}`)); err != nil {
			return err
		}
		userBatch, err := batch.Collection("user")
		if err != nil {
			return err
		}
		for _, target := range targets[preCount:] {
			if err := userBatch.Put([]byte(keys[target]), valueFor(2)); err != nil {
				return err
			}
		}
		return nil
	})
	checkpointGroupFaultHook = previousHook
	if err != nil {
		t.Fatalf("certification then overlay pressure update: %v", err)
	}
	if callbackCount != 1 {
		t.Fatalf("overlay pressure callbacks = %d, want one", callbackCount)
	}
	if physicalHooks < 2 || !splitObservedDuringPhysicalFold {
		t.Fatalf("retry order physicalHooks=%d splitObservedDuringPhysicalFold=%v before=%+v after=%+v",
			physicalHooks, splitObservedDuringPhysicalFold, before, group.Stats())
	}
	after := group.Stats()
	if after.AppliedIndex != before.AppliedIndex+1 ||
		after.TransactionHighWater != before.TransactionHighWater+1 ||
		after.CheckpointAppliedIndex != before.AppliedIndex ||
		after.CheckpointTransactions != before.TransactionHighWater ||
		after.Checkpoints != before.Checkpoints+1 ||
		after.PhysicalCheckpoints != before.PhysicalCheckpoints+2 ||
		after.PressureCheckpoints != before.PressureCheckpoints+1 {
		t.Fatalf("certification/pressure stats before=%+v after=%+v", before, after)
	}
	if got := user.primaryUnifiedOverlay.bucketCount.Load(); got >= uint32(pressureLimit) {
		t.Fatalf("overlay retained %d buckets after pressure admission, limit=%d",
			got, pressureLimit)
	}
	for index, target := range targets {
		want := valueFor(2)
		if index < preCount {
			want = valueFor(1)
		}
		got, found, readErr := user.AppendRaw(nil, []byte(keys[target]))
		if readErr != nil || !found || string(got) != string(want) {
			t.Fatalf("user pressure row %q = %q/%v/%v, want %q",
				keys[target], got, found, readErr, want)
		}
	}
	if got, found, readErr := members[0].Collection.AppendRaw(
		nil, []byte("primary-key-000000256"),
	); readErr != nil || !found || string(got) != `{"n":256}` {
		t.Fatalf("system split row = %q/%v/%v", got, found, readErr)
	}
}
