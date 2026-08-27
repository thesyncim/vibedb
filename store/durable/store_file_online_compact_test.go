package durable

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
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
	if data.Length != onlineCompactionStagingChunkBytes || linked.StagingExtentCount != 1 ||
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
