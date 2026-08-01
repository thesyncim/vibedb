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

	// Pin that the crash image really contains a consecutive v1 batch whose first
	// entry widens a scalar. Reapplying that entry after the prefix checkpoint
	// replaces only the shorter old spelling and therefore fails its result CRC.
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
			rec.Entries[0].Kind != storeio.RecoveryRecordKindScalarPatch ||
			int(rec.Entries[0].ScalarPatch.OldScalarLength) ==
				len(rec.Entries[0].Value) {
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
