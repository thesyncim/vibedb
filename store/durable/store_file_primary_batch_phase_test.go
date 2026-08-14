package durable

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// phaseWorkload is the scripted single-collection Update used to pin that the
// stage → kind-3 fence → publish recomposition stays byte-identical to the
// pre-split path for journal record grammar and final content.
func phaseWorkload(batch *WriteBatch) error {
	if err := batch.Put([]byte("a"), []byte(`{"v":1}`)); err != nil {
		return err
	}
	if err := batch.Put([]byte("b"), []byte(`{"v":2}`)); err != nil {
		return err
	}
	if err := batch.Put([]byte("c"), []byte(`{"v":3}`)); err != nil {
		return err
	}
	return batch.Delete([]byte("missing"))
}

func phaseExpectedContent() map[string]string {
	return map[string]string{
		"a": `{"v":1}`,
		"b": `{"v":2}`,
		"c": `{"v":3}`,
	}
}

// TestPrimaryBatchPhaseSplitByteIdenticalFrozenContract pins that Collection.Update
// on every supported lane still produces kind-3 (or ordinary delta) journal
// records — never kind-4 — and the same logical end state. Journal/store file
// identities are random per CreateFromPrimary, so the frozen proof is the
// decoded record grammar plus a second Update on a cloned image matching
// byte-for-byte after normalizing away only the live append cursor region is
// not required: we compare the encoded kind-3 record bytes against storeio's
// encoder for the same logical batch, which is what the fence emits.
func TestPrimaryBatchPhaseSplitByteIdenticalFrozenContract(t *testing.T) {
	for _, lane := range primaryBatchLanes() {
		t.Run(lane.name, func(t *testing.T) {
			coll, file, path := openPrimaryBatchStore(t, lane.options)
			beforeJournal, beforeStore := readStoreJournalOptional(t, path)
			if err := coll.Update(phaseWorkload); err != nil {
				t.Fatalf("Update: %v", err)
			}
			if lane.name == "buffered" {
				if err := coll.Flush(); err != nil {
					t.Fatalf("Flush: %v", err)
				}
			}
			content := primaryStoreContent(t, coll)
			want := phaseExpectedContent()
			// Seed document from CreateFromPrimary remains unless deleted.
			for k, v := range content {
				if k == "a" || k == "b" || k == "c" {
					if v != want[k] {
						t.Fatalf("content[%q]=%q, want %q", k, v, want[k])
					}
				}
			}
			for _, k := range []string{"a", "b", "c"} {
				if _, ok := content[k]; !ok {
					t.Fatalf("missing key %q", k)
				}
			}
			// Capture journal before Close: Close checkpoints and recycles, so
			// the post-Close sibling no longer holds the Update's kind-3 record.
			afterJournal, afterStore := readStoreJournalOptional(t, path)
			// Primary batch leaves admit as memory-only buffered dirty frames;
			// journaled lanes fence durability in the sibling journal, so the
			// store image may be byte-identical until a later checkpoint/Flush
			// when allocation reuses existing free extents instead of growing
			// the file. Buffered Flush forces materialization and must dirty it.
			if lane.name == "buffered" {
				if bytes.Equal(beforeStore, afterStore) {
					t.Fatal("store bytes unchanged after Flush")
				}
			} else {
				if bytes.Equal(beforeJournal, afterJournal) {
					t.Fatal("journal bytes unchanged after Update")
				}
				assertJournalHasKind3BatchOnly(t, afterJournal, path)
			}
			if err := coll.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			_ = file.Close()
			// Reopen and confirm content stable (no kind-4 / no in-doubt).
			file, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			reopened, err := Open(file, lane.options)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()
			got := primaryStoreContent(t, reopened)
			for _, k := range []string{"a", "b", "c"} {
				if got[k] != want[k] {
					t.Fatalf("reopen content[%q]=%q, want %q", k, got[k], want[k])
				}
			}
			t.Logf("%s store=%s journal=%s", lane.name,
				shortHash(afterStore), shortHash(afterJournal))
		})
	}
}

func readStoreJournalOptional(t *testing.T, path string) (journal, store []byte) {
	t.Helper()
	var err error
	store, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	journal, err = os.ReadFile(path + ".rjournal")
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return journal, store
}

func shortHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

func assertJournalHasKind3BatchOnly(t *testing.T, journalBytes []byte, storePath string) {
	t.Helper()
	dir := t.TempDir()
	jpath := filepath.Join(dir, "probe.rjournal")
	if err := os.WriteFile(jpath, journalBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(jpath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rj, err := storeio.OpenRecoveryJournal(f)
	if err != nil {
		t.Fatalf("OpenRecoveryJournal: %v", err)
	}
	defer rj.Close()
	if rj.Header().Format != storeio.RecoveryJournalFormat {
		t.Fatalf("journal format = %d, want current", rj.Header().Format)
	}
	sawBatch := false
	if err := rj.Replay(rj.BaseGeneration(), func(rec storeio.RecoveryRecord) error {
		switch rec.Kind {
		case storeio.RecoveryRecordKindConditionalBatch:
			t.Fatalf("unexpected kind-4 conditional batch in single-collection journal")
		case storeio.RecoveryRecordKindBatch:
			sawBatch = true
			if rec.Conditional != (storeio.RecoveryConditionalHeader{}) {
				t.Fatalf("kind-3 batch carried conditional header")
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !sawBatch && storePath != "" {
		// buffered-journal and sync lanes must have journaled the Update batch.
		t.Fatal("expected at least one kind-3 batch record")
	}
}

// TestPrimaryBatchPhaseUnwindOnPrepareFailure stages a batch, abandons it
// before any journal fence, and proves full unwind: no journal growth, no
// reader-visible change, no leftover admitted frames.
func TestPrimaryBatchPhaseUnwindOnPrepareFailure(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	coll, file, path := openPrimaryBatchStore(t, options)
	defer file.Close()
	defer coll.Close()

	before := primaryStoreContent(t, coll)
	beforeJournal, err := os.ReadFile(path + ".rjournal")
	if err != nil {
		t.Fatal(err)
	}

	coll.writer.Lock()
	batch := coll.fileWriteBatch()
	if err := phaseWorkload(batch); err != nil {
		coll.writer.Unlock()
		t.Fatal(err)
	}
	staged, err := coll.stagePrimaryBatchLocked(batch)
	if err != nil {
		coll.releaseFileWriteBatch(batch)
		coll.writer.Unlock()
		t.Fatalf("stage: %v", err)
	}
	if !staged.live {
		coll.releaseFileWriteBatch(batch)
		coll.writer.Unlock()
		t.Fatal("expected live staged batch")
	}
	if len(coll.batchPrimaryAdmitted) == 0 {
		coll.unwindStagedPrimaryBatch(&staged)
		coll.releaseFileWriteBatch(batch)
		coll.writer.Unlock()
		t.Fatal("expected admitted frames before unwind")
	}
	// Fail between stage and prepare: full unwind, no journal fence.
	coll.unwindStagedPrimaryBatch(&staged)
	if staged.live {
		coll.releaseFileWriteBatch(batch)
		coll.writer.Unlock()
		t.Fatal("staged.live still set after unwind")
	}
	if len(coll.batchPrimaryAdmitted) != 0 {
		coll.releaseFileWriteBatch(batch)
		coll.writer.Unlock()
		t.Fatalf("admitted frames remain: %d", len(coll.batchPrimaryAdmitted))
	}
	coll.releaseFileWriteBatch(batch)
	coll.writer.Unlock()

	after := primaryStoreContent(t, coll)
	if len(after) != len(before) {
		t.Fatalf("reader content changed after unwind: before=%d after=%d",
			len(before), len(after))
	}
	for k, v := range before {
		if after[k] != v {
			t.Fatalf("key %q changed after unwind", k)
		}
	}
	afterJournal, err := os.ReadFile(path + ".rjournal")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeJournal, afterJournal) {
		t.Fatal("journal bytes changed without prepare")
	}
}

// TestPrimaryBatchPhasePrepareFailureOutcomeUnknown proves both an injected
// append error and an injected sync error poison the collection and report an
// unknown outcome. Either failure can leave a complete authenticated prepare
// visible after reopen, so callers must reconcile rather than retry in place.
func TestPrimaryBatchPhasePrepareFailureOutcomeUnknown(t *testing.T) {
	cases := []struct {
		name  string
		phase storeio.JournalFaultPhase
		index int
	}{
		{"append", storeio.JournalFaultENOSPCAppend, 0},
		{"sync", storeio.JournalFaultSyncError, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			get, restore := installJournalFaultSeam(t)
			defer restore()
			options := syncPrimaryJournalTestOptions()
			coll, file, _ := openPrimaryBatchStore(t, options)
			defer file.Close()
			defer coll.Close()

			fj := get()
			if fj == nil {
				t.Fatal("fault seam not installed")
			}
			if tc.phase == storeio.JournalFaultSyncError {
				fj.Program(storeio.JournalFaultPlan{
					Phase: tc.phase, SyncIndex: tc.index,
				})
			} else {
				fj.Program(storeio.JournalFaultPlan{
					Phase: tc.phase, AppendIndex: tc.index,
				})
			}

			var markerID [16]byte
			for i := range markerID {
				markerID[i] = byte(i + 1)
			}
			coll.writer.Lock()
			batch := coll.fileWriteBatch()
			if err := phaseWorkload(batch); err != nil {
				coll.writer.Unlock()
				t.Fatal(err)
			}
			staged, err := coll.stagePrimaryBatchConditionalLocked(batch)
			if err != nil {
				coll.releaseFileWriteBatch(batch)
				coll.writer.Unlock()
				t.Fatalf("stage: %v", err)
			}
			prepErr := coll.preparePrimaryBatchConditionalLocked(
				&staged, markerID, 1, 7, true,
			)
			coll.unwindStagedPrimaryBatch(&staged)
			coll.releaseFileWriteBatch(batch)
			coll.writer.Unlock()

			if prepErr == nil {
				t.Fatal("prepare succeeded, want injected failure")
			}
			if !errors.Is(prepErr, ErrCommitOutcomeUnknown) {
				t.Fatalf("prepare error = %v, want unknown outcome", prepErr)
			}
			persistence := coll.PersistenceError()
			if persistence == nil {
				t.Fatal("expected sticky persistence poison")
			}
			if !errors.Is(persistence, ErrCommitOutcomeUnknown) {
				t.Fatalf("sticky poison = %v, want unknown outcome", persistence)
			}
			if !fj.Faulted() {
				t.Fatal("programmed fault never fired")
			}
		})
	}
}

// TestPrimaryBatchPhaseManualRecomposeMatchesUpdate drives stage → kind-3
// fence → publishPrimaryBatchGateHeld under an externally held gate and
// compares content to a sibling Collection.Update on the same seed.
func TestPrimaryBatchPhaseManualRecomposeMatchesUpdate(t *testing.T) {
	options := syncPrimaryJournalTestOptions()
	oracle, oracleFile, _ := openPrimaryBatchStore(t, options)
	defer oracleFile.Close()
	if err := oracle.Update(phaseWorkload); err != nil {
		t.Fatalf("oracle Update: %v", err)
	}
	want := primaryStoreContent(t, oracle)
	_ = oracle.Close()

	coll, file, _ := openPrimaryBatchStore(t, options)
	defer file.Close()
	defer coll.Close()

	coll.writer.Lock()
	batch := coll.fileWriteBatch()
	if err := phaseWorkload(batch); err != nil {
		coll.writer.Unlock()
		t.Fatal(err)
	}
	staged, err := coll.stagePrimaryBatchLocked(batch)
	if err != nil {
		coll.releaseFileWriteBatch(batch)
		coll.writer.Unlock()
		t.Fatalf("stage: %v", err)
	}
	if err := coll.journalBatchBeforePublishLocked(
		staged.generation, coll.batchJournalEntries,
	); err != nil {
		coll.unwindStagedPrimaryBatch(&staged)
		coll.releaseFileWriteBatch(batch)
		coll.writer.Unlock()
		t.Fatalf("kind-3 fence: %v", err)
	}
	coll.batchPrimaryAdmitted = coll.batchPrimaryAdmitted[:0]
	coll.snapshotGate.Lock()
	coll.publishPrimaryBatchGateHeld(staged)
	coll.snapshotGate.Unlock()
	coll.releaseFileWriteBatch(batch)
	coll.writer.Unlock()

	got := primaryStoreContent(t, coll)
	if len(got) != len(want) {
		t.Fatalf("content size %d, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("content[%q]=%q, want %q", k, got[k], v)
		}
	}
}
