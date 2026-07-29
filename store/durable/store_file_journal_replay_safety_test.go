package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// replaySafetyCrashImage builds a crash image whose journal holds n
// acknowledged mutations the recovered root does not cover, spread across a
// seed-doc-wide graph so replay touches many distinct leaves, and returns the
// image path plus the expected content.
func replaySafetyCrashImage(
	t *testing.T, options Options, seedDocs, n int,
) (string, map[string]string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "store.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]string, seedDocs+n)
	for i := 0; i < seedDocs; i++ {
		key := fmt.Sprintf("seed-%06d", i)
		val := fmt.Sprintf(`{"v":%d}`, i)
		if err := builder.Append(key, []byte(val)); err != nil {
			t.Fatal(err)
		}
		want[key] = val
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		t.Fatalf("CreateFromPrimary: %v", err)
	}
	_ = file.Close()
	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	stride := seedDocs / n
	if stride == 0 {
		stride = 1
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("seed-%06d", (i*stride)%seedDocs)
		val := journalValue(i)
		if _, err := coll.Put([]byte(key), val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		want[key] = string(val)
	}
	if coll.Stats().JournalAcks == 0 {
		t.Fatal("no journal acknowledgements before crash copy")
	}
	crash := filepath.Join(dir, "crash.vibe")
	copyFileForCrash(t, path, crash)
	copyFileForCrash(t, path+".rjournal", crash+".rjournal")
	if err := coll.Close(); err != nil {
		t.Fatalf("close live: %v", err)
	}
	_ = file.Close()
	return crash, want
}

// TestRecoveryJournalReplayFoldFailureKeepsRecords is the second-crash
// window: the post-replay fold's recycle fails (ENOSPC), Open surfaces the
// failure, and the journal image must still hold every record so the next
// open recovers everything. Before replay-time recycle suppression, a
// checkpoint mid-replay emptied the journal while the fold was not yet
// durable, and this window lost acknowledged synchronous writes.
func TestRecoveryJournalReplayFoldFailureKeepsRecords(t *testing.T) {
	options := journalTestOptions(CheckpointFilesystem)

	crash, want := replaySafetyCrashImage(t, options, 200, 40)

	// The failing recycle happens inside Open's post-replay fold, so the
	// fault plan must be programmed the moment the journal pairs.
	prevHook := recoveryJournalFaultHook
	recoveryJournalFaultHook = func(rj *storeio.RecoveryJournal) {
		storeio.NewFaultJournal(rj).Program(storeio.JournalFaultPlan{
			Phase: storeio.JournalFaultENOSPCRecycle,
		})
	}
	cf, err := os.OpenFile(crash, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if rc, openErr := Open(cf, options); openErr == nil {
		// Some fold shapes may legitimately absorb a recycle fault into a
		// poison surfaced later; either way nothing below may be lost.
		_ = rc.Close()
	}
	_ = cf.Close()
	recoveryJournalFaultHook = prevHook

	cf, err = os.OpenFile(crash, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	rc, err := Open(cf, options)
	if err != nil {
		t.Fatalf("reopen after failed fold: %v", err)
	}
	defer rc.Close()
	if got := snapshotCollectionContent(t, rc); !mapsEqual(got, want) {
		t.Fatalf(
			"acknowledged writes lost across failed-fold reopen: %d keys, want %d",
			len(got), len(want),
		)
	}
}
