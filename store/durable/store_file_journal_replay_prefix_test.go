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

func TestRecoveryJournalLegacyBatchIgnoresSmallerReopenByteBound(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	options.MaxKeyBytes = 8
	options.InlineValueBytes = 256
	options.MaxDocumentBytes = 256
	options.MaxBatchDocuments = 2
	options.MaxBatchBytes = 512
	coll, file, path := openPrimaryBatchStore(t, options)
	valueA := []byte(fmt.Sprintf(`{"pad":%q,"v":1}`, strings.Repeat("a", 176)))
	valueB := []byte(fmt.Sprintf(`{"pad":%q,"v":2}`, strings.Repeat("b", 176)))
	if len(valueA)+len(valueB)+2 <= 256+2*8 {
		t.Fatal("test batch does not exceed smaller reopen byte bound")
	}
	var captured *journalCrashImage
	previous := recoveryJournalPostSyncHook
	recoveryJournalPostSyncHook = func() {
		if captured == nil {
			image := captureJournalImage(t, path)
			captured = &image
		}
	}
	if err := coll.Update(func(batch *WriteBatch) error {
		if err := batch.Put([]byte("a"), valueA); err != nil {
			return err
		}
		return batch.Put([]byte("b"), valueB)
	}); err != nil {
		recoveryJournalPostSyncHook = previous
		t.Fatal(err)
	}
	recoveryJournalPostSyncHook = previous
	_ = coll.Close()
	_ = file.Close()
	if captured == nil {
		t.Fatal("did not capture legacy batch")
	}
	crashPath := filepath.Join(t.TempDir(), "legacy-smaller-byte-bound.vibe")
	if err := os.WriteFile(crashPath, captured.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashPath+".rjournal", captured.journal, 0o600); err != nil {
		t.Fatal(err)
	}
	reopenOptions := options
	reopenOptions.MaxBatchBytes = reopenOptions.MaxDocumentBytes +
		reopenOptions.MaxBatchDocuments*reopenOptions.MaxKeyBytes
	reopenedFile, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(reopenedFile, reopenOptions)
	if err != nil {
		_ = reopenedFile.Close()
		t.Fatalf("recover with smaller MaxBatchBytes: %v", err)
	}
	defer reopenedFile.Close()
	defer recovered.Close()
	for key, want := range map[string][]byte{"a": valueA, "b": valueB} {
		got, found, readErr := recovered.AppendRaw(nil, []byte(key))
		if readErr != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("recovered %s = %q,%v,%v", key, got, found, readErr)
		}
	}
}

// TestRecoveryJournalLegacyIndexedDeleteBatchSecondCrash stays on the legacy
// one-generation journal grammar used by synchronous Update. Recovery publishes
// a pure-delete batch atomically, checkpoints it, and is interrupted before
// recycle; the next Open must re-consume the now-no-op batch, preserve the
// negative exact postings, and finally empty the journal.
func TestRecoveryJournalLegacyIndexedDeleteBatchSecondCrash(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	options.Indexes = []store.IndexDefinition{
		{Name: "number", Paths: []string{"/i"}},
	}
	docs := map[string][]byte{
		"delete-a": []byte(`{"i":1,"v":"a"}`),
		"delete-b": []byte(`{"i":2,"v":"b"}`),
		"keep":     []byte(`{"i":3,"v":"keep"}`),
	}
	coll := buildIndexedPrimaryFile(
		t, t.TempDir(), "legacy-delete-batch-*", docs, options,
	)
	path := coll.file.Name()
	var captured *journalCrashImage
	previousPostSync := recoveryJournalPostSyncHook
	recoveryJournalPostSyncHook = func() {
		if captured == nil {
			image := captureJournalImage(t, path)
			captured = &image
		}
	}
	if err := coll.Update(func(batch *WriteBatch) error {
		if err := batch.Delete([]byte("delete-a")); err != nil {
			return err
		}
		return batch.Delete([]byte("delete-b"))
	}); err != nil {
		recoveryJournalPostSyncHook = previousPostSync
		t.Fatal(err)
	}
	recoveryJournalPostSyncHook = previousPostSync
	if captured == nil {
		t.Fatal("did not capture synced unpublished delete batch")
	}
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}

	crashPath := filepath.Join(t.TempDir(), "legacy-delete-second-crash.vibe")
	if err := os.WriteFile(crashPath, captured.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashPath+".rjournal", captured.journal, 0o600); err != nil {
		t.Fatal(err)
	}

	secondCrash := errors.New("test: crash after atomic legacy delete replay")
	previousReplay := recoveryJournalReplayBatchEntryHook
	defer func() {
		recoveryJournalPostSyncHook = previousPostSync
		recoveryJournalReplayBatchEntryHook = previousReplay
	}()
	hookCalls := 0
	recoveryJournalReplayBatchEntryHook = func(
		replayed *Collection, _ storeio.RecoveryRecord, entryIndex int,
	) error {
		if entryIndex != 0 {
			return nil
		}
		hookCalls++
		for _, key := range []string{"delete-a", "delete-b"} {
			if _, found, err := replayed.AppendRaw(nil, []byte(key)); err != nil || found {
				return fmt.Errorf("atomic replay retained %s: found=%v err=%v", key, found, err)
			}
		}
		replayed.writer.Lock()
		err := replayed.checkpointBufferedLocked()
		replayed.writer.Unlock()
		if err != nil {
			return err
		}
		return secondCrash
	}
	firstFile, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, firstErr := Open(firstFile, options)
	_ = firstFile.Close()
	if !errors.Is(firstErr, secondCrash) || hookCalls != 1 {
		t.Fatalf("first recovery = %v, hooks=%d", firstErr, hookCalls)
	}
	recoveryJournalReplayBatchEntryHook = nil

	secondFile, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	reopenOptions := options
	// Batch admission is a caller policy, not durable journal semantics. This
	// acknowledged two-key record must recover even when the next process admits
	// only one-key Updates of its own.
	reopenOptions.MaxBatchDocuments = 1
	reopenOptions.MaxBatchBytes = 0
	recovered, err := Open(secondFile, reopenOptions)
	if err != nil {
		_ = secondFile.Close()
		t.Fatalf("second recovery: %v", err)
	}
	defer secondFile.Close()
	defer recovered.Close()
	for _, key := range []string{"delete-a", "delete-b"} {
		if _, found, readErr := recovered.AppendRaw(nil, []byte(key)); readErr != nil || found {
			t.Fatalf("recovered %s: found=%v err=%v", key, found, readErr)
		}
	}
	if value, found, readErr := recovered.AppendRaw(nil, []byte("keep")); readErr != nil || !found || string(value) != `{"i":3,"v":"keep"}` {
		t.Fatalf("recovered keep = %q,%v,%v", value, found, readErr)
	}
	for _, raw := range []string{"1", "2"} {
		if got := primaryExactTestKeys(
			t, recovered, "number", primaryExactTestNeedle(t, raw),
		); len(got) != 0 {
			t.Fatalf("recovered number=%s postings = %v", raw, got)
		}
	}
	if recovered.journal.Cursor() != 0 {
		t.Fatalf("second recovery retained %d journal bytes", recovered.journal.Cursor())
	}
}

func legacyRecoveryDispersedValue(row int, state string) []byte {
	padding := make([]byte, 1700)
	for at := range padding {
		padding[at] = byte('a' + (row*31+at*17+(row^(at>>3)))%26)
	}
	return []byte(fmt.Sprintf(
		`{"state":%q,"row":%d,"pad":"%s"}`, state, row, padding,
	))
}

// TestRecoveryJournalLegacyDispersedBatchWithTinyReopenGeometry proves that a
// durable legacy batch is not constrained by the next process's batch-admission
// arenas. The acknowledged batch spans more leaves than that process can retain
// as deferred parents, so private recovery must make bounded forward progress
// through pressure checkpoints while retaining the original journal until every
// replacement and exact posting is durable.
func TestRecoveryJournalLegacyDispersedBatchWithTinyReopenGeometry(t *testing.T) {
	const sourceRows = 8192
	options := syncPrimaryJournalTestOptions()
	options.MaxBatchDocuments = 256
	options.MaxBatchBytes = 1 << 20
	options.Indexes = []store.IndexDefinition{
		{Name: "state", Paths: []string{"/state"}},
	}

	reopenOptions := options
	reopenOptions.MaxBatchDocuments = 1
	reopenOptions.MaxBatchBytes = 0
	// Remove the class-5 overlay window as well as shrinking the caller batch
	// arenas. Recovery must not inherit the process geometry that acknowledged
	// the record.
	var reopenGeometry normalizedFileStoreOptions
	foundBoundedGeometry := false
	for resident := int64(1 << 20); resident <= options.ResidentBytes; resident += 4096 {
		candidate := reopenOptions
		candidate.ResidentBytes = resident
		normalized, normalizeErr := candidate.normalized()
		if normalizeErr != nil || normalized.primaryUnifiedOverlayBuckets != 0 {
			continue
		}
		reopenOptions = candidate
		reopenGeometry = normalized
		foundBoundedGeometry = true
		break
	}
	if !foundBoundedGeometry {
		t.Fatal("could not select valid reopen geometry without a row overlay")
	}
	recoveryWindow := filePrimaryPendingCapacity(reopenGeometry)
	if recoveryWindow >= options.MaxBatchDocuments {
		t.Fatalf("tiny reopen pending window = %d, want below source batch bound %d",
			recoveryWindow, options.MaxBatchDocuments)
	}

	documents := make(map[string][]byte, sourceRows)
	for row := range sourceRows {
		documents[fmt.Sprintf("row-%05d", row)] =
			legacyRecoveryDispersedValue(row, "old")
	}
	coll := buildIndexedPrimaryFile(
		t, t.TempDir(), "legacy-dispersed-*", documents, options,
	)
	path := coll.file.Name()

	// Select one key per routed leaf until the batch exceeds the reopen
	// process's complete deferred-parent window. This makes the pressure
	// checkpoint requirement executable rather than inferred from corpus size.
	selected := make([]int, 0, min(options.MaxBatchDocuments, recoveryWindow+32))
	buckets := make(map[storeio.BucketID]struct{}, cap(selected))
	for row := 0; row < sourceRows && len(selected) < cap(selected); row++ {
		key := []byte(fmt.Sprintf("row-%05d", row))
		route, ok := coll.primaryRouter.Load().Route(key)
		if !ok {
			t.Fatalf("route %q", key)
		}
		if _, exists := buckets[route.Bucket]; exists {
			continue
		}
		buckets[route.Bucket] = struct{}{}
		selected = append(selected, row)
	}
	if len(selected) <= recoveryWindow {
		t.Fatalf("source exposed %d dispersed leaves, need more than reopen window %d",
			len(selected), recoveryWindow)
	}

	var captured *journalCrashImage
	previousPostSync := recoveryJournalPostSyncHook
	recoveryJournalPostSyncHook = func() {
		if captured == nil {
			image := captureJournalImage(t, path)
			captured = &image
		}
	}
	updateErr := coll.Update(func(batch *WriteBatch) error {
		for _, row := range selected {
			if err := batch.Put(
				[]byte(fmt.Sprintf("row-%05d", row)),
				legacyRecoveryDispersedValue(row, "new"),
			); err != nil {
				return err
			}
		}
		return nil
	})
	recoveryJournalPostSyncHook = previousPostSync
	if updateErr != nil {
		t.Fatalf("source dispersed Update: %v", updateErr)
	}
	if captured == nil {
		t.Fatal("did not capture synced unpublished dispersed batch")
	}
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}

	crashPath := filepath.Join(t.TempDir(), "legacy-dispersed-recovery.vibe")
	if err := os.WriteFile(crashPath, captured.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashPath+".rjournal", captured.journal, 0o600); err != nil {
		t.Fatal(err)
	}

	previousReplay := recoveryJournalReplayBatchEntryHook
	defer func() {
		recoveryJournalPostSyncHook = previousPostSync
		recoveryJournalReplayBatchEntryHook = previousReplay
	}()
	hookCalls := 0
	pressureCheckpointed := false
	recoveryJournalReplayBatchEntryHook = func(
		replayed *Collection, _ storeio.RecoveryRecord, _ int,
	) error {
		hookCalls++
		if replayed.automaticCheckpoints.Load() != 0 {
			pressureCheckpointed = true
		}
		return nil
	}
	recoveredFile, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(recoveredFile, reopenOptions)
	recoveryJournalReplayBatchEntryHook = previousReplay
	if err != nil {
		_ = recoveredFile.Close()
		t.Fatalf("recover dispersed batch with one-document geometry: %v", err)
	}
	if hookCalls != len(selected) {
		_ = recovered.Close()
		_ = recoveredFile.Close()
		t.Fatalf("replayed entries = %d, want %d", hookCalls, len(selected))
	}
	if !pressureCheckpointed {
		_ = recovered.Close()
		_ = recoveredFile.Close()
		t.Fatal("dispersed recovery did not cross a bounded pressure checkpoint")
	}
	if got := cap(recovered.primaryPendingParents); got != recoveryWindow {
		_ = recovered.Close()
		_ = recoveredFile.Close()
		t.Fatalf("recovered pending window = %d, want %d", got, recoveryWindow)
	}
	if recovered.journal.Cursor() != 0 {
		_ = recovered.Close()
		_ = recoveredFile.Close()
		t.Fatalf("recovery retained %d journal bytes", recovered.journal.Cursor())
	}

	newKeys := primaryExactTestKeys(
		t, recovered, "state", primaryExactTestNeedle(t, `"new"`),
	)
	slices.Sort(newKeys)
	wantNew := make([]string, len(selected))
	for i, row := range selected {
		wantNew[i] = fmt.Sprintf("row-%05d", row)
		wantValue, canonicalErr := vibejson.AppendCanonicalize(
			nil, legacyRecoveryDispersedValue(row, "new"),
		)
		if canonicalErr != nil {
			t.Fatal(canonicalErr)
		}
		value, found, readErr := recovered.AppendRaw(nil, []byte(wantNew[i]))
		if readErr != nil || !found || !bytes.Equal(value, wantValue) {
			_ = recovered.Close()
			_ = recoveredFile.Close()
			t.Fatalf("recovered %s = %q,%v,%v", wantNew[i], value, found, readErr)
		}
	}
	slices.Sort(wantNew)
	if !slices.Equal(newKeys, wantNew) {
		_ = recovered.Close()
		_ = recoveredFile.Close()
		t.Fatalf("recovered new postings = %v, want %v", newKeys, wantNew)
	}
	if got := primaryExactTestKeys(
		t, recovered, "state", primaryExactTestNeedle(t, `"old"`),
	); len(got) != sourceRows-len(selected) {
		_ = recovered.Close()
		_ = recoveredFile.Close()
		t.Fatalf("recovered old postings = %d, want %d",
			len(got), sourceRows-len(selected))
	}
	if err := recovered.Close(); err != nil {
		_ = recoveredFile.Close()
		t.Fatal(err)
	}
	if err := recoveredFile.Close(); err != nil {
		t.Fatal(err)
	}

	secondFile, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(secondFile, reopenOptions)
	if err != nil {
		_ = secondFile.Close()
		t.Fatalf("second recovery: %v", err)
	}
	if second.journal.Cursor() != 0 {
		_ = second.Close()
		_ = secondFile.Close()
		t.Fatalf("second recovery retained %d journal bytes", second.journal.Cursor())
	}
	if err := second.Close(); err != nil {
		_ = secondFile.Close()
		t.Fatal(err)
	}
	if err := secondFile.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryJournalScalarReplaySecondCrashSkipsDurablePrefix(t *testing.T) {
	options := concurrentPrimaryTestOptions()
	fixture := openConcurrentPrimaryTestFixture(t, 512, options)
	same, _ := concurrentPrimaryTestTargets(t, fixture)
	keys := [2][]byte{
		[]byte(fixture.keys[same[0]]),
		[]byte(fixture.keys[same[1]]),
	}
	replaceGroup := func(base []byte, scalar string) []byte {
		t.Helper()
		start := bytes.Index(base, []byte(`"group":`))
		if start < 0 {
			t.Fatalf("group scalar missing from %q", base)
		}
		start += len(`"group":`)
		end := start
		for end < len(base) && base[end] >= '0' && base[end] <= '9' {
			end++
		}
		value := make([]byte, 0, len(base)-(end-start)+len(scalar))
		value = append(value, base[:start]...)
		value = append(value, scalar...)
		return append(value, base[end:]...)
	}
	want := [2][]byte{
		replaceGroup(
			canonicalConcurrentPrimaryValue(t, fixture.values[same[0]]),
			"999999999999999999",
		),
		replaceGroup(
			canonicalConcurrentPrimaryValue(t, fixture.values[same[1]]),
			"888888888888888888",
		),
	}
	baseGeneration := fixture.collection.Generation()
	physicalBase := fixture.collection.committer.DurableGeneration()
	for i := range keys {
		created, err := fixture.collection.Put(keys[i], want[i])
		if err != nil || created {
			t.Fatalf("Put %d = %v,%v", i, created, err)
		}
	}
	targetGeneration := fixture.collection.Generation()
	if targetGeneration != baseGeneration+2 {
		t.Fatalf("target generation = %d, want %d",
			targetGeneration, baseGeneration+2)
	}
	if err := fixture.collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := fixture.collection.committer.DurableGeneration(); got != physicalBase {
		t.Fatalf("cheap Flush advanced physical generation = %d, want %d",
			got, physicalBase)
	}
	image := captureJournalImage(t, fixture.path)
	if err := fixture.collection.Close(); err != nil {
		t.Fatal(err)
	}

	crashPath := filepath.Join(t.TempDir(), "scalar-second-crash.vibe")
	if err := os.WriteFile(crashPath, image.store, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crashPath+".rjournal", image.journal, 0o600); err != nil {
		t.Fatal(err)
	}

	// The compact development format journals complete canonical values. Pin the
	// consecutive batch so replay after a prefix checkpoint remains idempotent
	// without retaining the removed class-5 scalar-patch compatibility lane.
	journalFile, err := os.Open(crashPath + ".rjournal")
	if err != nil {
		t.Fatal(err)
	}
	journal, err := storeio.OpenRecoveryJournal(journalFile)
	if err != nil {
		_ = journalFile.Close()
		t.Fatal(err)
	}
	if journal.Header().FormatVersion != storeio.RecoveryJournalFormatScalarPatch {
		t.Fatalf("journal format = %d, want v1", journal.Header().FormatVersion)
	}
	checkedBatch := false
	if err := journal.Replay(baseGeneration, func(rec storeio.RecoveryRecord) error {
		if rec.Kind != storeio.RecoveryRecordKindBatch || len(rec.Entries) != 2 ||
			rec.Entries[0].Kind != storeio.RecoveryRecordKindPut ||
			rec.Entries[1].Kind != storeio.RecoveryRecordKindPut ||
			!bytes.Equal(rec.Entries[0].Value, want[0]) ||
			!bytes.Equal(rec.Entries[1].Value, want[1]) {
			return fmt.Errorf("unexpected scalar replay batch: %#v", rec)
		}
		checkedBatch = true
		return nil
	}); err != nil {
		_ = journal.Close()
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if !checkedBatch {
		t.Fatal("v1 scalar batch was not present")
	}

	secondCrash := errors.New("test: crash after durable replay prefix")
	previousHook := recoveryJournalReplayBatchEntryHook
	defer func() { recoveryJournalReplayBatchEntryHook = previousHook }()
	hookCalls := 0
	var prefixGeneration uint64
	recoveryJournalReplayBatchEntryHook = func(
		coll *Collection, rec storeio.RecoveryRecord, entryIndex int,
	) error {
		if rec.Generation != targetGeneration || entryIndex != 0 {
			return nil
		}
		hookCalls++
		// Force the same physical checkpoint bounded staging pressure may take
		// between replayed entries. journalReplaying keeps the v1 batch intact.
		coll.writer.Lock()
		checkpointErr := coll.checkpointBufferedLocked()
		prefixGeneration = coll.committer.DurableGeneration()
		coll.writer.Unlock()
		if checkpointErr != nil {
			return checkpointErr
		}
		return secondCrash
	}
	firstRecoveryFile, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, firstRecoveryErr := Open(firstRecoveryFile, options)
	_ = firstRecoveryFile.Close()
	if !errors.Is(firstRecoveryErr, secondCrash) || hookCalls != 1 ||
		prefixGeneration != baseGeneration+1 {
		t.Fatalf("interrupted recovery = err %v calls %d durable %d, want %v/1/%d",
			firstRecoveryErr, hookCalls, prefixGeneration, secondCrash,
			baseGeneration+1)
	}
	recoveryJournalReplayBatchEntryHook = nil

	secondRecoveryFile, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(secondRecoveryFile, options)
	if err != nil {
		_ = secondRecoveryFile.Close()
		t.Fatalf("second recovery: %v", err)
	}
	if recovered.Generation() != targetGeneration {
		t.Fatalf("second recovery generation = %d, want %d",
			recovered.Generation(), targetGeneration)
	}
	for i := range keys {
		got, found, readErr := recovered.AppendRaw(nil, keys[i])
		if readErr != nil || !found || !bytes.Equal(got, want[i]) {
			t.Fatalf("second recovery key %d = %q,%v,%v, want %q",
				i, got, found, readErr, want[i])
		}
	}
	if recovered.journal.Cursor() != 0 {
		t.Fatalf("second recovery retained %d journal bytes",
			recovered.journal.Cursor())
	}
	if err := recovered.Close(); err != nil {
		_ = secondRecoveryFile.Close()
		t.Fatal(err)
	}
	if err := secondRecoveryFile.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryJournalScalarFormatRejectsRuntimeLaneChange(t *testing.T) {
	options := journalDeltaTestOptions()
	coll := buildTemplateHeavyOverlayCollection(
		t, t.TempDir(), 128, options,
	)
	rootLazyBufferedJournal(t, coll)
	path := coll.file.Name()
	if got := coll.journal.Header().FormatVersion; got !=
		storeio.RecoveryJournalFormatScalarPatch {
		t.Fatalf("journal format = %d, want scalar-patch", got)
	}
	if err := coll.Close(); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name   string
		change func(*Options)
	}{
		{
			name: "per-mutation journal",
			change: func(o *Options) {
				o.RecoveryJournal = true
			},
		},
		{
			name: "synchronous",
			change: func(o *Options) {
				o.Durability = DurabilitySync
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			reopenOptions := options
			test.change(&reopenOptions)
			file, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			_, openErr := Open(file, reopenOptions)
			_ = file.Close()
			if !errors.Is(openErr, storeio.ErrRecoveryJournalGeometry) {
				t.Fatalf("Open = %v, want recovery-journal geometry error", openErr)
			}
		})
	}
}

func TestRecoveryJournalScalarFormatReplaySkipsDurableBatchPrefix(t *testing.T) {
	record := storeio.RecoveryRecord{
		Generation: 104,
		Kind:       storeio.RecoveryRecordKindBatch,
		Entries: []storeio.RecoveryBatchEntry{
			{
				Kind:  storeio.RecoveryRecordKindScalarPatch,
				Key:   []byte("scalar"),
				Value: []byte("10"),
			},
			{Kind: storeio.RecoveryRecordKindPut, Key: []byte("full-a")},
			{Kind: storeio.RecoveryRecordKindDelete, Key: []byte("gone")},
			{Kind: storeio.RecoveryRecordKindPut, Key: []byte("full-b")},
		},
	}

	start, err := recoveryJournalBatchReplayStart(
		storeio.RecoveryJournalFormatScalarPatch, record, 102,
	)
	if err != nil {
		t.Fatal(err)
	}
	if start != 2 || record.Entries[start].Kind != storeio.RecoveryRecordKindDelete {
		t.Fatalf("replay start = %d, want mixed-entry suffix at index 2", start)
	}

	// Legacy batches retain their one-generation atomic grammar even if the same
	// target/count numbers would look like a partially covered delta interval.
	start, err = recoveryJournalBatchReplayStart(
		storeio.RecoveryJournalFormatLegacy, record, 102,
	)
	if err != nil || start != 0 {
		t.Fatalf("legacy replay start = %d, %v; want 0, nil", start, err)
	}
}

func TestRecoveryJournalScalarFormatReplayRejectsGenerationUnderflow(t *testing.T) {
	record := storeio.RecoveryRecord{
		Generation: 1,
		Kind:       storeio.RecoveryRecordKindBatch,
		Entries: []storeio.RecoveryBatchEntry{
			{Kind: storeio.RecoveryRecordKindPut, Key: []byte("a")},
			{Kind: storeio.RecoveryRecordKindPut, Key: []byte("b")},
		},
	}
	if _, err := recoveryJournalBatchReplayStart(
		storeio.RecoveryJournalFormatScalarPatch, record, 0,
	); !errors.Is(err, storeio.ErrRecoveryJournalRecord) {
		t.Fatalf("generation underflow = %v, want journal-record error", err)
	}
}
