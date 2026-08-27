package durable

import (
	"bytes"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func TestOnlineCompactionManifestAndChainedExtentSurviveReopen(t *testing.T) {
	file, _ := buildPrimaryOpenTestFile(t)
	collection, err := Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	collection.writer.Lock()
	manifestStore, manifest, err := collection.beginOnlineGenerationMigrationLocked()
	collection.writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	state := collection.state.Load()
	if state.root.MigrationManifestOffset == 0 || manifest.SourceGeneration != state.root.Generation ||
		manifest.SourcePrimaryRoot != state.root.PrimaryRoot {
		t.Fatalf("state=%+v manifest=%+v", state.root, manifest)
	}
	data, linked, err := collection.growOnlineMigrationStaging(manifestStore, 64<<10, 16)
	if err != nil {
		t.Fatal(err)
	}
	if data.Length != uint64(collection.options.MaxPageSize) || linked.StagingExtentCount != 1 ||
		linked.StagingChainTail == (storeio.PageRef{}) || linked.PendingExtentBytes != 0 {
		t.Fatalf("data=%+v linked=%+v", data, linked)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedState := reopened.state.Load()
	if reopenedState.root.MigrationManifestOffset != state.root.MigrationManifestOffset {
		t.Fatalf("manifest locator after reopen=%d want=%d", reopenedState.root.MigrationManifestOffset, state.root.MigrationManifestOffset)
	}
	reopenedStore, err := storeio.OpenGenerationMigrationManifestStore(file, int64(reopenedState.root.MigrationManifestOffset))
	if err != nil {
		t.Fatal(err)
	}
	reopenedManifest, err := reopenedStore.Load()
	if err != nil || reopenedManifest.StagingChainTail != linked.StagingChainTail ||
		reopenedManifest.TargetFileEnd != linked.TargetFileEnd {
		t.Fatalf("reopened manifest=%+v err=%v", reopenedManifest, err)
	}
}

func TestCompactOnlineRebuildsExactIndexesAndResidentEpoch(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "online-compact-indexed-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testBatchOptions(16)
	options.Indexes = []store.IndexDefinition{{Name: "country", Paths: []string{"/country"}}}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	for key, country := range map[string]string{"a": "pt", "b": "us", "c": "pt"} {
		if _, err := collection.Put([]byte(key), []byte(`{"country":"`+country+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	needle := primaryExactTestNeedle(t, `"pt"`)
	if got := primaryExactTestKeys(t, collection, "country", needle); len(got) != 2 {
		t.Fatalf("before keys=%v", got)
	}
	oldEpoch := collection.primaryEpoch
	if _, err := collection.CompactOnline(); err != nil {
		t.Fatal(err)
	}
	if collection.primaryEpoch == oldEpoch {
		t.Fatal("resident exact epoch was not atomically replaced")
	}
	if got := primaryExactTestKeys(t, collection, "country", needle); len(got) != 2 {
		t.Fatalf("after keys=%v", got)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := primaryExactTestKeys(t, reopened, "country", needle); len(got) != 2 {
		t.Fatalf("reopened keys=%v", got)
	}
}

func TestCompactOnlinePreservesOpaqueInlineAndOverflowValues(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "online-compact-opaque-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testBatchOptions(8)
	options.OpaqueValues = true
	options.InlineValueBytes = 64
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	inline := []byte{0xff, 0, 1, 2}
	overflow := bytes.Repeat([]byte{0, 0xfe, 0x80}, 100)
	if _, err := collection.Put([]byte("inline"), inline); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put([]byte("overflow"), overflow); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.CompactOnline(); err != nil {
		t.Fatal(err)
	}
	assertOpaqueValue(t, collection, []byte("inline"), inline)
	assertOpaqueValue(t, collection, []byte("overflow"), overflow)
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertOpaqueValue(t, reopened, []byte("inline"), inline)
	assertOpaqueValue(t, reopened, []byte("overflow"), overflow)
}

func TestCompactOnlineInstallsReopenableServingRoot(t *testing.T) {
	file, _ := buildPrimaryOpenTestFile(t)
	collection, err := Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	before := collection.state.Load()
	report, err := collection.CompactOnline()
	if err != nil {
		t.Fatal(err)
	}
	after := collection.state.Load()
	if report.Documents != 1000 || report.SourceFileEnd < before.fileEnd ||
		after.root.PrimaryRoot == before.root.PrimaryRoot || after.root.Generation <= before.root.Generation ||
		report.StagingAllocatedBytes == 0 || report.InstalledFileEnd != after.fileEnd {
		t.Fatalf("report=%+v before=%+v after=%+v", report, before, after)
	}
	rows := 0
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.RangeRaw(func(_, _ []byte) error { rows++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if rows != 1000 {
		t.Fatalf("rows=%d", rows)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if verified, verifyErr := Verify(file); verifyErr != nil || !verified.OK() {
		layout, _ := storeio.MutableStoreLayout(uint32(after.root.PageSize))
		for slot, offset := range layout.RootOffsets {
			buf := make([]byte, storeio.InlineSuperblockSize)
			_, _ = file.ReadAt(buf, int64(offset))
			inline, decodeErr := storeio.DecodeInlineSuperblock(buf)
			t.Logf("slot=%d inline=%+v decode=%v", slot, inline, decodeErr)
		}
		t.Fatalf("verify before reopen: report=%+v err=%v", verified, verifyErr)
	}
	reopened, err := Open(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	rows = 0
	snapshot, err = reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.RangeRaw(func(_, _ []byte) error { rows++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if rows != 1000 {
		t.Fatalf("reopened rows=%d", rows)
	}
}
