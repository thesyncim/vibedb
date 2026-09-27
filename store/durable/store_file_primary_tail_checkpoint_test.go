package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestCheckpointGroupRightmostTailSplitsFoldAtOneCheckpoint(t *testing.T) {
	options := txnTestOptions()
	options.MaxBatchDocuments = structuralCertificationBatchRows
	options.MaxBatchBytes = 1 << 20
	options.InlineValueBytes = 2048
	options.MaxDocumentBytes = 2048
	options.ResidentBytes = 64 << 20
	if _, err := options.normalized(); err != nil {
		t.Fatalf("tail split options: %v", err)
	}

	dir := t.TempDir()
	members := []NamedCollection{
		openTxnNamedCollection(t, dir, "system", options),
		openTxnNamedCollection(t, dir, "user", options),
	}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatalf("NewTxnLog: %v", err)
	}
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{
		CheckpointEvery: 4096,
	})
	if err != nil {
		_ = log.Close()
		t.Fatalf("NewCheckpointGroup: %v", err)
	}
	t.Cleanup(func() {
		_ = group.Close()
		_ = log.Close()
	})

	writeRows := func(start int) error {
		first := uint64(start + 1)
		last := first + structuralCertificationBatchRows - 1
		return group.UpdateConsecutive(
			first, last, members[1:], defaultTxnLimits(),
			func(batch *DatabaseBatch) error {
				write, err := batch.Collection("user")
				if err != nil {
					return err
				}
				for row := start; row < start+structuralCertificationBatchRows; row++ {
					key := []byte(fmt.Sprintf("tail-row-%08d", row))
					if err := write.Put(key, tailSplitQualificationValue(row)); err != nil {
						return err
					}
				}
				return nil
			},
		)
	}
	if err := writeRows(0); err != nil {
		t.Fatalf("seed tail rows: %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seed rows: %v", err)
	}
	user := members[1].Collection
	baselineGroup := group.Stats()
	baselineCollection := user.Stats()
	baselineRoutes := user.primaryRouter.Load().Len()
	for _, start := range []int{
		structuralCertificationBatchRows,
		2 * structuralCertificationBatchRows,
		3 * structuralCertificationBatchRows,
	} {
		beforeGeneration := user.Generation()
		if err := writeRows(start); err != nil {
			t.Fatalf("append tail rows [%d,%d): %v", start,
				start+structuralCertificationBatchRows, err)
		}
		if got := user.Generation(); got != beforeGeneration+1 {
			t.Fatalf("tail batch generation %d -> %d, want exactly one logical generation",
				beforeGeneration, got)
		}
		currentGroup := group.Stats()
		currentCollection := user.Stats()
		if currentGroup.CheckpointAppliedIndex != baselineGroup.CheckpointAppliedIndex ||
			currentGroup.CheckpointTransactions != baselineGroup.CheckpointTransactions ||
			currentGroup.CertificateSyncs != baselineGroup.CertificateSyncs ||
			currentGroup.PhysicalCheckpoints != baselineGroup.PhysicalCheckpoints ||
			currentCollection.DeviceCommits != baselineCollection.DeviceCommits {
			t.Fatalf("tail append forced a checkpoint: group before=%+v after=%+v collection before=%+v after=%+v",
				baselineGroup, currentGroup, baselineCollection, currentCollection)
		}
	}
	if user.primaryTailSplit == nil || len(user.primaryTailSplit.leaves) < 3 {
		got := 0
		if user.primaryTailSplit != nil {
			got = len(user.primaryTailSplit.leaves)
		}
		t.Fatalf("tail split lineage leaves = %d, want at least 3 before checkpoint", got)
	}
	if got := user.primaryRouter.Load().Len(); got < baselineRoutes+2 {
		t.Fatalf("resident routes after repeated tail splits = %d, baseline=%d", got, baselineRoutes)
	}
	for row := 0; row < 4*structuralCertificationBatchRows; row++ {
		requireTailSplitRow(t, user, row)
	}

	group.mu.Lock()
	err = group.certifyLocked()
	group.mu.Unlock()
	if err != nil {
		t.Fatalf("certificate-only cut before snapshot materialization: %v", err)
	}
	materialized, err := user.Snapshot()
	if err != nil {
		t.Fatalf("materialize certified tail snapshot: %v", err)
	}
	if err := materialized.Close(); err != nil {
		t.Fatalf("close materialized tail snapshot: %v", err)
	}
	base, durable := user.primaryCheckpointBase, user.durableState.Load()
	if base == nil || durable == nil || base.root.Generation <= durable.root.Generation {
		t.Fatalf("flushless snapshot did not advance checkpoint base: base=%v durable=%v",
			base, durable)
	}
	if user.primaryTailSplit != nil || len(user.primaryPendingParents) != 0 {
		t.Fatalf("snapshot left first tail lineage pending: lineage=%v parents=%d",
			user.primaryTailSplit != nil, len(user.primaryPendingParents))
	}
	secondWindowGroup := group.Stats()
	secondWindowCollection := user.Stats()
	for _, start := range []int{
		4 * structuralCertificationBatchRows,
		5 * structuralCertificationBatchRows,
		6 * structuralCertificationBatchRows,
	} {
		beforeGeneration := user.Generation()
		if err := writeRows(start); err != nil {
			t.Fatalf("append second tail window [%d,%d): %v", start,
				start+structuralCertificationBatchRows, err)
		}
		if got := user.Generation(); got != beforeGeneration+1 {
			t.Fatalf("second-window generation %d -> %d, want exactly one logical generation",
				beforeGeneration, got)
		}
		currentGroup := group.Stats()
		currentCollection := user.Stats()
		if currentGroup.CheckpointAppliedIndex != secondWindowGroup.CheckpointAppliedIndex ||
			currentGroup.CheckpointTransactions != secondWindowGroup.CheckpointTransactions ||
			currentGroup.CertificateSyncs != secondWindowGroup.CertificateSyncs ||
			currentGroup.PhysicalCheckpoints != secondWindowGroup.PhysicalCheckpoints ||
			currentCollection.DeviceCommits != secondWindowCollection.DeviceCommits {
			t.Fatalf("second tail window forced a checkpoint: group before=%+v after=%+v collection before=%+v after=%+v",
				secondWindowGroup, currentGroup, secondWindowCollection, currentCollection)
		}
	}
	requireNonzeroTailSplitSource(t, user)
	for row := 0; row < 7*structuralCertificationBatchRows; row++ {
		requireTailSplitRow(t, user, row)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("fold second tail lineage: %v", err)
	}
	if user.primaryTailSplit != nil || len(user.primaryPendingParents) != 0 {
		t.Fatalf("tail lineage survived checkpoint: lineage=%v pending=%d",
			user.primaryTailSplit != nil, len(user.primaryPendingParents))
	}
	for row := 0; row < 7*structuralCertificationBatchRows; row++ {
		requireTailSplitRow(t, user, row)
	}
	assertTailSplitPersistedVerify(t, user, 7*structuralCertificationBatchRows)

	crashImage := copyCheckpointGroupDirectory(t, dir)
	reopened, reopenedLog, reopenedGroup, files := openTailCheckpointGroupCopy(
		t, crashImage, options,
	)
	t.Cleanup(func() {
		_ = reopenedGroup.Close()
		for _, collection := range reopened {
			_ = collection.Close()
		}
		_ = reopenedLog.Close()
		for _, file := range files {
			_ = file.Close()
		}
	})
	assertFreeSetMirror(t, reopened[1], "rightmost tail split after checkpoint reopen")
	assertTailSplitPersistedVerify(t, reopened[1], 7*structuralCertificationBatchRows)
	for row := 0; row < 7*structuralCertificationBatchRows; row++ {
		requireTailSplitRow(t, reopened[1], row)
	}

	reopenedMembers := []NamedCollection{
		{Name: "system", Collection: reopened[0]},
		{Name: "user", Collection: reopened[1]},
	}
	reopenedUser := reopened[1]
	for _, start := range []int{
		7 * structuralCertificationBatchRows,
		8 * structuralCertificationBatchRows,
		9 * structuralCertificationBatchRows,
	} {
		beforeGeneration := reopenedUser.Generation()
		if err := writeTailSplitRows(reopenedGroup, reopenedMembers[1:], "user", start); err != nil {
			t.Fatalf("append post-reopen tail rows at %d: %v", start, err)
		}
		if got := reopenedUser.Generation(); got != beforeGeneration+1 {
			t.Fatalf("post-reopen generation %d -> %d, want exactly one logical generation",
				beforeGeneration, got)
		}
	}
	requireNonzeroTailSplitSource(t, reopenedUser)
	for row := 0; row < 10*structuralCertificationBatchRows; row++ {
		requireTailSplitRow(t, reopenedUser, row)
	}
	if err := reopenedGroup.Checkpoint(); err != nil {
		t.Fatalf("fold post-reopen tail lineage: %v", err)
	}
	assertFreeSetMirror(t, reopenedUser, "repeated tail split checkpoint after reopen")
	assertTailSplitPersistedVerify(t, reopenedUser, 10*structuralCertificationBatchRows)
	for row := 0; row < 10*structuralCertificationBatchRows; row++ {
		requireTailSplitRow(t, reopenedUser, row)
	}

	finalImage := copyCheckpointGroupDirectory(t, crashImage)
	finalCollections, finalLog, finalGroup, finalFiles := openTailCheckpointGroupCopy(
		t, finalImage, options,
	)
	t.Cleanup(func() {
		_ = finalGroup.Close()
		for _, collection := range finalCollections {
			_ = collection.Close()
		}
		_ = finalLog.Close()
		for _, file := range finalFiles {
			_ = file.Close()
		}
	})
	assertFreeSetMirror(t, finalCollections[1], "repeated tail split final reopen")
	assertTailSplitPersistedVerify(t, finalCollections[1], 10*structuralCertificationBatchRows)
}

func TestCheckpointGroupTailSplitAbortedMemberBatchPublishesNothing(t *testing.T) {
	_, members, group := newTailSplitFixture(t, 4096)
	user := members[1].Collection
	if err := writeTailSplitRows(group, members[1:], "user", 0); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seed: %v", err)
	}
	if err := writeTailSplitRows(group, members[1:], "user", 64); err != nil {
		t.Fatalf("start tail lineage: %v", err)
	}
	lineage := user.primaryTailSplit
	if lineage == nil || len(lineage.leaves) < 2 {
		t.Fatalf("tail lineage not established: %+v", lineage)
	}
	beforeLeaves := append([]primaryTailSplitLeaf(nil), lineage.leaves...)
	beforeGroup := group.Stats()
	beforeGeneration := user.Generation()
	beforeRouteCount := user.primaryRouter.Load().Len()
	system := members[0].Collection
	beforeUserDirty := user.cache.Stats().DirtyBytes
	beforeSystemDirty := system.cache.Stats().DirtyBytes
	beforeUserAdmitted := len(user.batchPrimaryAdmitted)
	beforeSystemAdmitted := len(system.batchPrimaryAdmitted)
	beforeTailCharged := lineage.chargedBytes
	beforeTailRouterCharge := lineage.routerWorstCaseBytes
	beforeTailRouterRetained := lineage.routerRetainedBytes
	beforeTailCurrentCharge := user.primaryTailCurrentChargeBytes.Load()
	fault := errors.New("abort after conditional prepare")
	previousHook := checkpointGroupFaultHook
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if point == checkpointGroupAfterPrepareAppend {
			return fault
		}
		return nil
	}
	err := group.UpdateConsecutive(
		129, 192, members, defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			userBatch, err := batch.Collection("user")
			if err != nil {
				return err
			}
			for row := 128; row < 192; row++ {
				if err := userBatch.Put(
					[]byte(fmt.Sprintf("tail-row-%08d", row)),
					tailSplitQualificationValue(row),
				); err != nil {
					return err
				}
			}
			systemBatch, err := batch.Collection("system")
			if err != nil {
				return err
			}
			return systemBatch.Put([]byte("aborted-member-row"), []byte(`{"v":1}`))
		},
	)
	checkpointGroupFaultHook = previousHook
	if !errors.Is(err, fault) {
		t.Fatalf("faulted tail batch = %v, want %v", err, fault)
	}
	if after := group.Stats(); after != beforeGroup {
		t.Fatalf("aborted member batch changed group state: before=%+v after=%+v", beforeGroup, after)
	}
	if got := user.cache.Stats().DirtyBytes; got != beforeUserDirty {
		t.Fatalf("aborted tail batch leaked user dirty frames: %d -> %d", beforeUserDirty, got)
	}
	if got := system.cache.Stats().DirtyBytes; got != beforeSystemDirty {
		t.Fatalf("aborted tail batch leaked system dirty frames: %d -> %d", beforeSystemDirty, got)
	}
	if len(user.batchPrimaryAdmitted) != beforeUserAdmitted ||
		len(system.batchPrimaryAdmitted) != beforeSystemAdmitted {
		t.Fatalf("aborted batch left admitted refs: user=%d/%d system=%d/%d",
			len(user.batchPrimaryAdmitted), beforeUserAdmitted,
			len(system.batchPrimaryAdmitted), beforeSystemAdmitted)
	}
	if lineage.chargedBytes != beforeTailCharged ||
		lineage.routerWorstCaseBytes != beforeTailRouterCharge ||
		lineage.routerRetainedBytes != beforeTailRouterRetained ||
		user.primaryTailCurrentChargeBytes.Load() != beforeTailCurrentCharge {
		t.Fatalf("aborted tail batch changed current reservations: charged=%d/%d router-bound=%d/%d router-retained=%d/%d current=%d/%d",
			lineage.chargedBytes, beforeTailCharged,
			lineage.routerWorstCaseBytes, beforeTailRouterCharge,
			lineage.routerRetainedBytes, beforeTailRouterRetained,
			user.primaryTailCurrentChargeBytes.Load(), beforeTailCurrentCharge)
	}
	if user.Generation() != beforeGeneration || user.primaryRouter.Load().Len() != beforeRouteCount ||
		user.primaryTailSplit != lineage || len(lineage.leaves) != len(beforeLeaves) {
		t.Fatalf("aborted tail batch published state: generation=%d/%d routes=%d/%d lineage=%p/%p leaves=%d/%d",
			user.Generation(), beforeGeneration, user.primaryRouter.Load().Len(), beforeRouteCount,
			user.primaryTailSplit, lineage, len(lineage.leaves), len(beforeLeaves))
	}
	for index := range beforeLeaves {
		if lineage.leaves[index].volatile != beforeLeaves[index].volatile ||
			lineage.leaves[index].localID != beforeLeaves[index].localID {
			t.Fatalf("aborted tail lineage changed leaf %d: before=%+v after=%+v",
				index, beforeLeaves[index], lineage.leaves[index])
		}
	}
	for row := 128; row < 192; row++ {
		key := []byte(fmt.Sprintf("tail-row-%08d", row))
		if _, found, readErr := user.AppendRaw(nil, key); readErr != nil || found {
			t.Fatalf("aborted user row %q visible: found=%v err=%v", key, found, readErr)
		}
	}
	if _, found, readErr := members[0].Collection.AppendRaw(nil, []byte("aborted-member-row")); readErr != nil || found {
		t.Fatalf("aborted system row visible: found=%v err=%v", found, readErr)
	}
}

func TestCheckpointGroupTailSplitPartialMemberFoldReplaysCertifiedRows(t *testing.T) {
	dir, members, group := newTailSplitFixture(t, 4096)
	user := members[1].Collection
	if err := writeTailSplitRows(group, members[1:], "user", 0); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seed: %v", err)
	}
	userGenerationBeforeReplay := user.Generation()
	for _, start := range []int{64, 128, 192} {
		if err := writeTailSplitRows(group, members[1:], "user", start); err != nil {
			t.Fatalf("append rows from %d: %v", start, err)
		}
	}
	lineage := user.primaryTailSplit
	if lineage == nil || len(lineage.leaves) < 2 {
		t.Fatalf("tail lineage missing before partial fold: %+v", lineage)
	}
	for row := 0; row < 256; row++ {
		requireTailSplitRow(t, user, row)
	}
	before := group.Stats()
	fault := errors.New("capture image after first member fold")
	var crashImage string
	previousHook := checkpointGroupFaultHook
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if point == checkpointGroupAfterPhysicalCheckpoint && crashImage == "" {
			crashImage = copyCheckpointGroupDirectory(t, dir)
			return fault
		}
		return nil
	}
	err := group.Checkpoint()
	checkpointGroupFaultHook = previousHook
	if !errors.Is(err, fault) {
		t.Fatalf("partial member fold = %v, want injected error %v", err, fault)
	}
	if crashImage == "" {
		t.Fatal("partial fold did not capture a pre-cleanup crash image")
	}
	if after := group.Stats(); after.PhysicalCheckpoints != before.PhysicalCheckpoints+1 {
		t.Fatalf("physical checkpoint count after partial fold = %d, want %d",
			after.PhysicalCheckpoints, before.PhysicalCheckpoints+1)
	}
	uncutUserFile, err := os.Open(filepath.Join(crashImage, "user.vjc"))
	if err != nil {
		t.Fatalf("open pre-replay user image: %v", err)
	}
	uncutReport, verifyErr := Verify(uncutUserFile)
	_ = uncutUserFile.Close()
	if verifyErr != nil || !uncutReport.OK() || uncutReport.Documents != 64 {
		t.Fatalf("pre-replay physical user image = %+v err=%v, want clean 64-row seed",
			uncutReport, verifyErr)
	}

	options := txnTestOptions()
	options.MaxBatchDocuments = structuralCertificationBatchRows
	options.MaxBatchBytes = 1 << 20
	options.InlineValueBytes = 2048
	options.MaxDocumentBytes = 2048
	options.ResidentBytes = 64 << 20
	replayDirectory := copyCheckpointGroupDirectory(t, crashImage)
	replayFault := errors.New("interrupt certified conditional replay")
	previousReplayHook := recoveryJournalReplayBatchEntryHook
	defer func() { recoveryJournalReplayBatchEntryHook = previousReplayHook }()
	var interruptedImage string
	var replayPrefixGeneration uint64
	var firstReplayRecordGeneration uint64
	hookCalls := 0
	recoveryJournalReplayBatchEntryHook = func(
		collection *Collection, record storeio.RecoveryRecord, entryIndex int,
	) error {
		if collection.file.Name() != filepath.Join(replayDirectory, "user.vjc") ||
			record.Kind != storeio.RecoveryRecordKindConditionalBatch || entryIndex != 0 ||
			interruptedImage != "" {
			return nil
		}
		hookCalls++
		firstReplayRecordGeneration = record.Generation
		collection.writer.Lock()
		checkpointErr := collection.checkpointBufferedLocked()
		replayPrefixGeneration = collection.committer.DurableGeneration()
		collection.writer.Unlock()
		if checkpointErr != nil {
			return checkpointErr
		}
		interruptedImage = copyCheckpointGroupDirectory(t, replayDirectory)
		return replayFault
	}
	openErr := openTailCheckpointGroupCopyExpectError(t, replayDirectory, options)
	recoveryJournalReplayBatchEntryHook = previousReplayHook
	if !errors.Is(openErr, replayFault) || hookCalls != 1 ||
		firstReplayRecordGeneration <= userGenerationBeforeReplay ||
		replayPrefixGeneration < firstReplayRecordGeneration {
		t.Fatalf("interrupted conditional replay = err %v hooks %d first record %d prefix %d, want %v/1/generation > %d with matching prefix",
			openErr, hookCalls, firstReplayRecordGeneration, replayPrefixGeneration, replayFault,
			userGenerationBeforeReplay)
	}
	if interruptedImage == "" {
		t.Fatal("interrupted replay did not preserve its post-prefix crash image")
	}
	interruptedUserFile, err := os.Open(filepath.Join(interruptedImage, "user.vjc"))
	if err != nil {
		t.Fatalf("open interrupted replay user image: %v", err)
	}
	interruptedReport, verifyErr := Verify(interruptedUserFile)
	_ = interruptedUserFile.Close()
	if verifyErr != nil || !interruptedReport.OK() ||
		interruptedReport.Documents != 128 ||
		interruptedReport.Generation != replayPrefixGeneration {
		t.Fatalf("interrupted physical replay prefix = %+v err=%v, want 128 rows at generation %d",
			interruptedReport, verifyErr, replayPrefixGeneration)
	}
	interruptedJournalFile, err := os.OpenFile(
		RecoveryJournalPath(filepath.Join(interruptedImage, "user.vjc")),
		os.O_RDWR, 0,
	)
	if err != nil {
		t.Fatalf("open interrupted replay journal: %v", err)
	}
	interruptedJournal, err := storeio.OpenRecoveryJournal(interruptedJournalFile)
	if err != nil {
		_ = interruptedJournalFile.Close()
		t.Fatalf("open interrupted recovery journal: %v", err)
	}
	hasCoveredPrepare, hasUncoveredPrepare := false, false
	journalErr := interruptedJournal.Replay(
		interruptedJournal.BaseGeneration(),
		func(record storeio.RecoveryRecord) error {
			if record.Kind != storeio.RecoveryRecordKindConditionalBatch {
				return fmt.Errorf("interrupted journal kind %d is not conditional", record.Kind)
			}
			hasCoveredPrepare = hasCoveredPrepare || record.Generation <= replayPrefixGeneration
			hasUncoveredPrepare = hasUncoveredPrepare || record.Generation > replayPrefixGeneration
			return nil
		},
	)
	remainingCursor := interruptedJournal.Cursor()
	closeJournalErr := interruptedJournal.Close()
	if journalErr != nil || closeJournalErr != nil || remainingCursor == 0 ||
		!hasCoveredPrepare || !hasUncoveredPrepare {
		t.Fatalf("interrupted journal suffix cursor=%d covered=%v uncovered=%v replayErr=%v closeErr=%v",
			remainingCursor, hasCoveredPrepare, hasUncoveredPrepare,
			journalErr, closeJournalErr)
	}

	retryCollections, retryLog, retryGroup, retryFiles := openTailCheckpointGroupCopy(
		t, interruptedImage, options,
	)
	t.Cleanup(func() {
		_ = retryGroup.Close()
		for _, collection := range retryCollections {
			_ = collection.Close()
		}
		_ = retryLog.Close()
		for _, file := range retryFiles {
			_ = file.Close()
		}
	})
	for row := 0; row < 256; row++ {
		requireTailSplitRow(t, retryCollections[1], row)
	}
	if err := retryGroup.Checkpoint(); err != nil {
		t.Fatalf("complete checkpoint after interrupted certified replay: %v", err)
	}
	assertTailSplitPersistedVerify(t, retryCollections[1], 256)
	assertFreeSetMirror(t, retryCollections[1], "tail split after repeated certified replay")
}

func TestCheckpointGroupTailSplitKeepsHeldSnapshotAndCurrentRange(t *testing.T) {
	dir, members, group := newTailSplitFixture(t, 4096)
	user := members[1].Collection
	if err := writeTailSplitRows(group, members[1:], "user", 0); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seed: %v", err)
	}
	held, err := user.Snapshot()
	if err != nil {
		t.Fatalf("hold seed snapshot: %v", err)
	}
	t.Cleanup(func() {
		if held != nil {
			_ = held.Close()
		}
	})
	for _, start := range []int{64, 128, 192} {
		if err := writeTailSplitRows(group, members[1:], "user", start); err != nil {
			t.Fatalf("append rows from %d: %v", start, err)
		}
	}
	if user.primaryTailSplit == nil {
		t.Fatal("tail lineage missing before checkpoint")
	}
	lineage := user.primaryTailSplit
	beforeGroup := group.Stats()
	beforeGeneration := user.Generation()
	beforeRoutes := user.primaryRouter.Load().Len()
	beforeBytes := checkpointGroupDirectoryBytes(t, dir)
	if snapshot, snapshotErr := user.Snapshot(); snapshotErr == nil {
		_ = snapshot.Close()
		t.Fatal("uncertified tail snapshot unexpectedly materialized")
	} else if !errors.Is(snapshotErr, ErrCheckpointGroupPressure) {
		t.Fatalf("uncertified tail snapshot = %v, want checkpoint pressure", snapshotErr)
	}
	if group.Stats() != beforeGroup || user.Generation() != beforeGeneration ||
		user.primaryTailSplit != lineage || user.primaryRouter.Load().Len() != beforeRoutes {
		t.Fatalf("uncertified snapshot changed tail state: group=%+v/%+v generation=%d/%d lineage=%p/%p routes=%d/%d",
			beforeGroup, group.Stats(), user.Generation(), beforeGeneration,
			user.primaryTailSplit, lineage, user.primaryRouter.Load().Len(), beforeRoutes)
	}
	afterBytes := checkpointGroupDirectoryBytes(t, dir)
	if len(beforeBytes) != len(afterBytes) {
		t.Fatalf("uncertified snapshot changed group files: count %d -> %d",
			len(beforeBytes), len(afterBytes))
	}
	for name, before := range beforeBytes {
		if after, ok := afterBytes[name]; !ok || !bytes.Equal(before, after) {
			t.Fatalf("uncertified snapshot changed file %q", name)
		}
	}
	group.mu.Lock()
	err = group.certifyLocked()
	group.mu.Unlock()
	if err != nil {
		t.Fatalf("certificate-only cut: %v", err)
	}
	oldRows := 0
	if err := held.RangeRaw(func(key, value []byte) error {
		row, parseErr := parseTailSplitRowKey(key)
		if parseErr != nil || row >= 64 || string(value) != string(tailSplitQualificationValue(row)) {
			return fmt.Errorf("held snapshot row %q=%q parse=%v", key, value, parseErr)
		}
		oldRows++
		return nil
	}); err != nil {
		t.Fatalf("held snapshot range: %v", err)
	}
	if oldRows != 64 {
		t.Fatalf("held snapshot rows = %d, want 64", oldRows)
	}
	current, err := user.Snapshot()
	if err != nil {
		t.Fatalf("snapshot certified pending topology: %v", err)
	}
	if user.primaryTailSplit != nil || len(user.primaryPendingParents) != 0 {
		_ = current.Close()
		t.Fatalf("current snapshot left tail topology pending: lineage=%v parents=%d",
			user.primaryTailSplit != nil, len(user.primaryPendingParents))
	}
	currentRows := 0
	if err := current.RangeRaw(func(key, value []byte) error {
		row, parseErr := parseTailSplitRowKey(key)
		if parseErr != nil || row >= 256 || string(value) != string(tailSplitQualificationValue(row)) {
			return fmt.Errorf("current snapshot row %q=%q parse=%v", key, value, parseErr)
		}
		currentRows++
		return nil
	}); err != nil {
		_ = current.Close()
		t.Fatalf("current snapshot range: %v", err)
	}
	_ = current.Close()
	if currentRows != 256 {
		t.Fatalf("current range rows = %d, want 256", currentRows)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("finish certified group checkpoint: %v", err)
	}
	if err := held.Close(); err != nil {
		t.Fatalf("release held snapshot: %v", err)
	}
	held = nil
	assertFreeSetMirror(t, user, "tail split after held snapshot release")
	assertTailSplitPersistedVerify(t, user, 256)
}

func TestCheckpointGroupTailEarlierDescendantUpdateUsesPressureFallback(t *testing.T) {
	_, members, group := newTailSplitFixture(t, 4096)
	user := members[1].Collection
	if err := writeTailSplitRows(group, members[1:], "user", 0); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint seed: %v", err)
	}
	for _, start := range []int{64, 128} {
		if err := writeTailSplitRows(group, members[1:], "user", start); err != nil {
			t.Fatalf("append rows from %d: %v", start, err)
		}
	}
	lineage := user.primaryTailSplit
	if lineage == nil || len(lineage.leaves) < 2 {
		t.Fatalf("tail lineage missing: %+v", lineage)
	}
	lastBucketValue, ok := storeio.MakeTabletLocalIdentityBucket(
		lineage.tabletID, uint32(lineage.leaves[len(lineage.leaves)-1].localID),
	)
	if !ok {
		t.Fatal("invalid trailing descendant identity")
	}
	state := user.state.Load()
	var earlierKey []byte
	for row := 0; row < 192; row++ {
		key := []byte(fmt.Sprintf("tail-row-%08d", row))
		route, routeErr := user.currentPrimaryResidentRoute(state, key)
		if routeErr != nil {
			t.Fatalf("resolve earlier candidate %q: %v", key, routeErr)
		}
		if uint32(route.Bucket) != lastBucketValue {
			earlierKey = key
			break
		}
	}
	if len(earlierKey) == 0 {
		t.Fatal("no row routed to an earlier tail descendant")
	}
	baseline := group.Stats()
	newValue := []byte(`{"n":999999,"payload":"updated earlier descendant"}`)
	err := group.UpdateConsecutive(
		193, 193, members[1:], defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			write, err := batch.Collection("user")
			if err != nil {
				return err
			}
			return write.Put(earlierKey, newValue)
		},
	)
	if err != nil {
		t.Fatalf("earlier descendant update through pressure fallback: %v", err)
	}
	after := group.Stats()
	if after.PressureCheckpoints != baseline.PressureCheckpoints+1 ||
		after.PhysicalCheckpoints != baseline.PhysicalCheckpoints+uint64(len(members)) ||
		after.AppliedIndex != 193 || user.primaryTailSplit != nil {
		t.Fatalf("earlier update fallback before=%+v after=%+v lineage=%v",
			baseline, after, user.primaryTailSplit != nil)
	}
	got, found, readErr := user.AppendRaw(nil, earlierKey)
	if readErr != nil || !found || string(got) != string(newValue) {
		t.Fatalf("earlier descendant value = %q/%v/%v want %q", got, found, readErr, newValue)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after earlier-descendant fallback: %v", err)
	}
	assertTailSplitPersistedVerify(t, user, 192)
}

func TestCheckpointGroupTailAnchorPressureUsesPhysicalFallback(t *testing.T) {
	const rows = storeio.SegmentedTabletRouterRowsPerPage
	options := txnTestOptions()
	options.ResidentBytes = 512 << 20
	options.MaxBatchDocuments = structuralCertificationBatchRows
	options.MaxBatchBytes = 1 << 20
	options.InlineValueBytes = 64 << 10
	options.MaxDocumentBytes = 64 << 10
	dir := t.TempDir()
	system := openTxnNamedCollection(t, dir, "system", options)
	userFile, err := os.OpenFile(
		filepath.Join(dir, "user.vjc"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600,
	)
	if err != nil {
		t.Fatalf("create user file: %v", err)
	}
	t.Cleanup(func() { _ = userFile.Close() })
	seed := make([]PrimaryBulkBytesRecord, rows)
	for row := range seed {
		value := fmt.Appendf(nil, `{"n":%d,"payload":"`, row)
		value = appendWideJSONSafePattern(value, 60<<10, row*37+11)
		value = append(value, `"}`...)
		seed[row] = PrimaryBulkBytesRecord{
			Key: fmt.Appendf(nil, "row-%06d", row), Value: value,
		}
	}
	if _, err := CreateFromByteRecords(seed, userFile, options); err != nil {
		t.Fatalf("bulk seed full anchor: %v", err)
	}
	user, err := Open(userFile, options)
	if err != nil {
		t.Fatalf("open bulk-seeded user: %v", err)
	}
	t.Cleanup(func() { _ = user.Close() })
	members := []NamedCollection{system, {Name: "user", Collection: user}}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatalf("NewTxnLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	seedCut := CheckpointGroupSeed{
		Applied: 9, Member: "system", Envelope: []byte(`{"seed":"anchor-pressure"}`),
	}
	seedCut.Images = checkpointGroupSeedImagesForTest(members, seedCut.Member)
	group, err := NewSeededCheckpointGroup(log, members, seedCut, CheckpointGroupOptions{
		CheckpointEvery: 4096,
	})
	if err != nil {
		t.Fatalf("NewSeededCheckpointGroup: %v", err)
	}
	t.Cleanup(func() { _ = group.Close() })
	if err := group.Seed(seedCut, system, defaultTxnLimits(), []byte("state")); err != nil {
		t.Fatalf("seed group around imported anchor image: %v", err)
	}
	if err := group.Update(seedCut.Applied, members[:1], defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			write, err := batch.Collection("system")
			if err != nil {
				return err
			}
			return write.Put([]byte("state"), []byte(`{"seed":"anchor-pressure-bound"}`))
		}); err != nil {
		t.Fatalf("bind seeded snapshot base: %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("certify seeded snapshot base: %v", err)
	}
	router := user.primaryRouter.Load()
	if router == nil || router.Len() != rows {
		t.Fatalf("bulk seed resident leaves = %v, want exactly %d", router, rows)
	}
	lastKey := fmt.Appendf(nil, "row-%06d", rows-1)
	state := user.state.Load()
	resident, err := user.currentPrimaryResidentRoute(state, lastKey)
	if err != nil {
		t.Fatalf("resolve full-anchor tail route: %v", err)
	}
	for row := 0; row < 64; row++ {
		key := fmt.Appendf(nil, "row-%06d-tail-%02d", rows-1, row)
		route, routeOK := router.Route(key)
		if !routeOK || route.Bucket != resident.Bucket {
			t.Fatalf("anchor-pressure key %q routes to %v, want rightmost source bucket %d",
				key, route, resident.Bucket)
		}
	}
	var path filePrimaryMutationPath
	if err := user.acquirePrimaryRoutingPath(&path, state, lastKey, resident); err != nil {
		t.Fatalf("acquire full-anchor tail path: %v", err)
	}
	if got := path.tablet.AnchorCount(); got != 1 {
		path.Release()
		t.Fatalf("full-anchor tablet anchor count = %d, want 1", got)
	}
	if got := path.anchor.Count(); got != rows {
		path.Release()
		t.Fatalf("rightmost anchor rows = %d, want %d", got, rows)
	}
	rightFence := append(append([]byte(nil), lastKey...), '-')
	plan, err := path.tablet.PlanLeafPartition(
		&path.anchor, path.leafRoute, [][]byte{rightFence},
	)
	path.Release()
	if err != nil {
		t.Fatalf("plan split in full anchor: %v", err)
	}
	if !plan.RequiresTabletRebuild() {
		t.Fatalf("one-leaf split plan = %+v, want full-tablet rebuild", plan)
	}
	if err := group.UpdateConsecutive(
		seedCut.Applied+1, seedCut.Applied+1, members[:1], defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			write, err := batch.Collection("system")
			if err != nil {
				return err
			}
			return write.Put([]byte("state"), []byte(`{"seed":"pending-anchor-pressure"}`))
		},
	); err != nil {
		t.Fatalf("leave unrelated group cut pending before anchor fallback: %v", err)
	}
	baseline := group.Stats()
	if baseline.AppliedIndex != seedCut.Applied+1 ||
		baseline.CheckpointAppliedIndex != seedCut.Applied {
		t.Fatalf("pending pressure fixture cut = %+v", baseline)
	}
	err = group.UpdateConsecutive(
		seedCut.Applied+2, seedCut.Applied+65, members[1:], defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			write, err := batch.Collection("user")
			if err != nil {
				return err
			}
			for row := 0; row < 64; row++ {
				key := fmt.Appendf(nil, "row-%06d-tail-%02d", rows-1, row)
				if err := write.Put(
					key,
					anchorPressureQualificationValue(row),
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("append beyond localized anchor budget: %v", err)
	}
	after := group.Stats()
	if after.PressureCheckpoints != baseline.PressureCheckpoints+1 ||
		after.PhysicalCheckpoints != baseline.PhysicalCheckpoints+uint64(len(members)) ||
		after.AppliedIndex != seedCut.Applied+65 || user.primaryTailSplit != nil {
		t.Fatalf("anchor pressure fallback before=%+v after=%+v lineage=%v user=%+v",
			baseline, after, user.primaryTailSplit != nil, user.Stats())
	}
	for row := 0; row < rows; row++ {
		key := fmt.Appendf(nil, "row-%06d", row)
		got, found, readErr := user.AppendRaw(nil, key)
		if readErr != nil || !found || len(got) == 0 {
			t.Fatalf("anchor seed row %q = %q/%v/%v", key, got, found, readErr)
		}
	}
	for row := 0; row < 64; row++ {
		key := fmt.Appendf(nil, "row-%06d-tail-%02d", rows-1, row)
		got, found, readErr := user.AppendRaw(nil, key)
		want := anchorPressureQualificationValue(row)
		if readErr != nil || !found || string(got) != string(want) {
			t.Fatalf("anchor-pressure row %q = %q/%v/%v want %q", key, got,
				found, readErr, want)
		}
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after anchor fallback: %v", err)
	}
	assertTailSplitPersistedVerify(t, user, rows+64)
	assertFreeSetMirror(t, user, "full-anchor tail fallback")
}

func tailSplitQualificationValue(row int) []byte {
	return fmt.Appendf(
		nil, `{"n":%d,"payload":%q}`,
		row, structuralCertificationPayload(row, 700),
	)
}

func anchorPressureQualificationValue(row int) []byte {
	return fmt.Appendf(
		nil, `{"n":%d,"payload":%q}`,
		row, structuralCertificationPayload(row, 12<<10),
	)
}

func requireTailSplitRow(t testing.TB, collection *Collection, row int) {
	t.Helper()
	key := []byte(fmt.Sprintf("tail-row-%08d", row))
	got, found, err := collection.AppendRaw(nil, key)
	if err != nil || !found || string(got) != string(tailSplitQualificationValue(row)) {
		t.Fatalf("tail row %q = %q/%v/%v", key, got, found, err)
	}
}

func requireNonzeroTailSplitSource(t testing.TB, collection *Collection) {
	t.Helper()
	lineage := collection.primaryTailSplit
	if lineage == nil || len(lineage.leaves) < 2 {
		t.Fatalf("tail lineage missing while checking source identity: %+v", lineage)
	}
	_, localID, ok := storeio.SplitTabletLocalIdentityBucket(
		uint32(lineage.source.leafRoute.Bucket),
	)
	if !ok || localID == 0 || len(lineage.leaves[0].localFloor) == 0 {
		t.Fatalf("second-window source identity/floor = localID %d valid=%v floor=%q",
			localID, ok, lineage.leaves[0].localFloor)
	}
}

func assertTailSplitPersistedVerify(t testing.TB, collection *Collection, documents int) {
	t.Helper()
	report, err := Verify(collection.file)
	if err != nil {
		t.Fatalf("verify persisted tail split: %v", err)
	}
	if !report.OK() || report.Documents != documents {
		t.Fatalf("persisted tail split verify = %+v, want clean %d-document report",
			report, documents)
	}
}

func parseTailSplitRowKey(key []byte) (int, error) {
	var row int
	if _, err := fmt.Sscanf(string(key), "tail-row-%08d", &row); err != nil {
		return 0, err
	}
	return row, nil
}

func newTailSplitFixture(
	t *testing.T, checkpointEvery uint64,
) (string, []NamedCollection, *CheckpointGroup) {
	t.Helper()
	options := txnTestOptions()
	options.MaxBatchDocuments = structuralCertificationBatchRows
	options.MaxBatchBytes = 1 << 20
	options.InlineValueBytes = 2048
	options.MaxDocumentBytes = 2048
	options.ResidentBytes = 64 << 20
	if _, err := options.normalized(); err != nil {
		t.Fatalf("tail split options: %v", err)
	}
	dir := t.TempDir()
	members := []NamedCollection{
		openTxnNamedCollection(t, dir, "system", options),
		openTxnNamedCollection(t, dir, "user", options),
	}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatalf("NewTxnLog: %v", err)
	}
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{
		CheckpointEvery: checkpointEvery,
	})
	if err != nil {
		_ = log.Close()
		t.Fatalf("NewCheckpointGroup: %v", err)
	}
	t.Cleanup(func() {
		_ = group.Close()
		_ = log.Close()
	})
	return dir, members, group
}

func writeTailSplitRows(
	group *CheckpointGroup, members []NamedCollection, memberName string, start int,
) error {
	first := uint64(start + 1)
	last := first + structuralCertificationBatchRows - 1
	return group.UpdateConsecutive(
		first, last, members, defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			write, err := batch.Collection(memberName)
			if err != nil {
				return err
			}
			for row := start; row < start+structuralCertificationBatchRows; row++ {
				if err := write.Put(
					[]byte(fmt.Sprintf("tail-row-%08d", row)),
					tailSplitQualificationValue(row),
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func openTailCheckpointGroupCopy(
	t *testing.T, dir string, options Options,
) ([]*Collection, *TxnLog, *CheckpointGroup, []*os.File) {
	t.Helper()
	names := []string{"system", "user"}
	requests := make([]TransactionCollectionOpen, len(names))
	files := make([]*os.File, len(names))
	for index, name := range names {
		file, err := os.OpenFile(filepath.Join(dir, name+".vjc"), os.O_RDWR, 0)
		if err != nil {
			for _, opened := range files {
				if opened != nil {
					_ = opened.Close()
				}
			}
			t.Fatalf("open copied member %q: %v", name, err)
		}
		files[index] = file
		requests[index] = TransactionCollectionOpen{File: file, Options: options}
	}
	collections, log, group, err := OpenCollectionsWithCheckpointGroup(
		dir, TxnLogOptions{}, requests, names,
		CheckpointGroupOptions{CheckpointEvery: 4096},
	)
	if err != nil {
		for _, file := range files {
			_ = file.Close()
		}
		t.Fatalf("reopen copied group: %v", err)
	}
	return collections, log, group, files
}

func openTailCheckpointGroupCopyExpectError(
	t *testing.T, dir string, options Options,
) error {
	t.Helper()
	names := []string{"system", "user"}
	requests := make([]TransactionCollectionOpen, len(names))
	files := make([]*os.File, len(names))
	for index, name := range names {
		file, err := os.OpenFile(filepath.Join(dir, name+".vjc"), os.O_RDWR, 0)
		if err != nil {
			for _, opened := range files {
				if opened != nil {
					_ = opened.Close()
				}
			}
			return err
		}
		files[index] = file
		requests[index] = TransactionCollectionOpen{File: file, Options: options}
	}
	collections, log, group, err := OpenCollectionsWithCheckpointGroup(
		dir, TxnLogOptions{}, requests, names,
		CheckpointGroupOptions{CheckpointEvery: 4096},
	)
	if err == nil {
		_ = group.Close()
		for _, collection := range collections {
			_ = collection.Close()
		}
		_ = log.Close()
	}
	for _, file := range files {
		_ = file.Close()
	}
	return err
}
