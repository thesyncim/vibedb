package raftstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
)

func descriptorCatalogTestOptions(groups int) NodeStoreOptions {
	return NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 512, RecentWaves: 128, MaxEntriesPerGroup: 64, ReaderSlots: 2, MaxGroups: groups}
}

func createDescriptorCatalogTestStore(t *testing.T, groups int) (string, *NodeStore, NodeStoreOptions) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "node")
	options := descriptorCatalogTestOptions(groups)
	store, err := CreateNodeStore(dir, testNodeIdentity(), testKey(), []NodeBootstrap{{Descriptor: testGroupDescriptor(100), Snapshot: nodeSnapshot(100, 1, 1)}}, options)
	if err != nil {
		t.Fatal(err)
	}
	return dir, store, options
}

func requireDescriptorKey(t *testing.T, store *NodeStore, descriptor GroupDescriptor, key uint64) {
	t.Helper()
	view, ok := store.GroupByID(descriptor.GroupID)
	if !ok || view == nil || view.group != key {
		t.Fatalf("descriptor %x: view=%+v found=%v want key=%d", descriptor.GroupID, view, ok, key)
	}
	got, err := view.Descriptor()
	if err != nil || got.LogKey != key || got.GroupID != descriptor.GroupID || got.StoreID != descriptor.StoreID {
		t.Fatalf("descriptor %x: got=%+v err=%v", descriptor.GroupID, got, err)
	}
}

func TestDescriptorCatalogCheckpointSeedsOpenAndReplaysConcurrentTail(t *testing.T) {
	dir, store, options := createDescriptorCatalogTestStore(t, 8)
	second := testGroupDescriptor(200)
	if got, err := store.RegisterGroup(second); err != nil || got.GroupID != 2 {
		t.Fatalf("register second=%+v err=%v", got, err)
	}
	third := testGroupDescriptor(50)
	inserted := false
	store.descriptorCheckpointHookTest = func(phase DescriptorCheckpointPhase) error {
		if phase == DescriptorCheckpointBeforeLogReference && !inserted {
			inserted = true
			got, err := store.RegisterGroup(third)
			if err != nil || got.GroupID != 3 {
				return errors.Join(ErrInvalid, err)
			}
		}
		return nil
	}
	if err := store.CheckpointDescriptorCatalog(); err != nil {
		t.Fatal(err)
	}
	metadata, ok := store.engine.Metadata(nodeDescriptorGroup)
	if !ok || metadata.Checkpoint.Index != 2 || metadata.TruncateIndex != 2 || metadata.FirstIndex != 3 || metadata.LastIndex != 3 || metadata.Hard.Commit != 3 {
		t.Fatalf("descriptor metadata=%+v ok=%v", metadata, ok)
	}
	catalogBytes, err := os.ReadFile(descriptorCatalogPath(dir, metadata.Checkpoint.ID))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(catalogBytes, []byte(second.Distribution)) || bytes.Contains(catalogBytes, []byte(second.Shard)) {
		t.Fatal("catalog leaked plaintext placement coordinates")
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	requireDescriptorKey(t, store, testGroupDescriptor(100), 1)
	requireDescriptorKey(t, store, second, 2)
	requireDescriptorKey(t, store, third, 3)
}

func TestDescriptorCatalogCheckpointCrashCutsBeforeReferenceKeepLogAuthority(t *testing.T) {
	injected := errors.New("descriptor checkpoint crash")
	phases := []DescriptorCheckpointPhase{
		DescriptorCheckpointTempWritten,
		DescriptorCheckpointFileSynced,
		DescriptorCheckpointRenamed,
		DescriptorCheckpointDirectorySynced,
		DescriptorCheckpointBeforeLogReference,
	}
	for _, phase := range phases {
		t.Run(phaseName(phase), func(t *testing.T) {
			dir, store, options := createDescriptorCatalogTestStore(t, 8)
			second := testGroupDescriptor(200)
			if _, err := store.RegisterGroup(second); err != nil {
				t.Fatal(err)
			}
			store.descriptorCheckpointHookTest = func(got DescriptorCheckpointPhase) error {
				if got == phase {
					return injected
				}
				return nil
			}
			if err := store.CheckpointDescriptorCatalog(); !errors.Is(err, injected) {
				t.Fatalf("checkpoint error=%v", err)
			}
			metadata, _ := store.engine.Metadata(nodeDescriptorGroup)
			if metadata.Checkpoint != (seglog.Checkpoint{}) || metadata.FirstIndex != 1 {
				t.Fatalf("unreferenced file gained authority: %+v", metadata)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
			if err != nil {
				t.Fatal(err)
			}
			requireDescriptorKey(t, store, testGroupDescriptor(100), 1)
			requireDescriptorKey(t, store, second, 2)
			store.descriptorCheckpointHookTest = nil
			if err = store.CheckpointDescriptorCatalog(); err != nil {
				t.Fatal(err)
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDescriptorCatalogCheckpointCrashAfterReferenceRecoversCheckpoint(t *testing.T) {
	dir, store, options := createDescriptorCatalogTestStore(t, 8)
	second := testGroupDescriptor(200)
	if _, err := store.RegisterGroup(second); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("crash after descriptor reference")
	store.descriptorCheckpointHookTest = func(phase DescriptorCheckpointPhase) error {
		if phase == DescriptorCheckpointLogReferenceDurable {
			return injected
		}
		return nil
	}
	if err := store.CheckpointDescriptorCatalog(); !errors.Is(err, injected) {
		t.Fatalf("checkpoint error=%v", err)
	}
	metadata, _ := store.engine.Metadata(nodeDescriptorGroup)
	if metadata.Checkpoint.Index != 2 || metadata.TruncateIndex != 2 {
		t.Fatalf("durable cut missing: %+v", metadata)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	requireDescriptorKey(t, store, second, 2)
}

func TestDescriptorCatalogCheckpointAuthenticationAndExactIdentity(t *testing.T) {
	dir, store, options := createDescriptorCatalogTestStore(t, 8)
	if _, err := store.RegisterGroup(testGroupDescriptor(200)); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckpointDescriptorCatalog(); err != nil {
		t.Fatal(err)
	}
	metadata, _ := store.engine.Metadata(nodeDescriptorGroup)
	path := descriptorCatalogPath(dir, metadata.Checkpoint.ID)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-descriptorCatalogTrailerBytes-1] ^= 0x80
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if reopened, openErr := OpenNodeStore(dir, testNodeIdentity(), testKey(), options); !errors.Is(openErr, ErrCorrupt) {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatalf("tampered referenced catalog accepted: %v", openErr)
	}
}

func TestDescriptorCatalogCheckpointSupersedesAndRemovesOldCatalog(t *testing.T) {
	dir, store, options := createDescriptorCatalogTestStore(t, 8)
	if err := store.CheckpointDescriptorCatalog(); err != nil {
		t.Fatal(err)
	}
	first, _ := store.engine.Metadata(nodeDescriptorGroup)
	secondDescriptor := testGroupDescriptor(200)
	if _, err := store.RegisterGroup(secondDescriptor); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckpointDescriptorCatalog(); err != nil {
		t.Fatal(err)
	}
	second, _ := store.engine.Metadata(nodeDescriptorGroup)
	if first.Checkpoint.ID == second.Checkpoint.ID || second.Checkpoint.Index != 2 {
		t.Fatalf("checkpoint did not advance: first=%+v second=%+v", first.Checkpoint, second.Checkpoint)
	}
	if _, err := os.Stat(descriptorCatalogPath(dir, first.Checkpoint.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("superseded catalog remains: %v", err)
	}
	if _, err := os.Stat(descriptorCatalogPath(dir, second.Checkpoint.ID)); err != nil {
		t.Fatalf("authoritative catalog missing: %v", err)
	}
	syncs := 0
	store.SetDataSyncForTesting(func(*os.File) error { syncs++; return nil })
	if err := store.CheckpointDescriptorCatalog(); err != nil {
		t.Fatal(err)
	}
	if syncs != 0 {
		t.Fatalf("unchanged catalog emitted %d durability waves", syncs)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	requireDescriptorKey(t, store, secondDescriptor, 2)
}

func TestDescriptorCatalogCheckpointUsesSequencerControlLane(t *testing.T) {
	dir, store, options := createDescriptorCatalogTestStore(t, 8)
	sequencer, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	var registration Submission
	if err = registration.Initialize(); err != nil {
		t.Fatal(err)
	}
	descriptor := testGroupDescriptor(200)
	if err = registration.PrepareRegisterGroup(descriptor); err != nil {
		t.Fatal(err)
	}
	if _, err = sequencer.TrySubmit(&registration); err != nil {
		t.Fatal(err)
	}
	if _, err = registration.Wait(); err != nil {
		t.Fatal(err)
	}
	if err = store.CheckpointDescriptorCatalog(); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	requireDescriptorKey(t, store, descriptor, 2)
}

func TestDescriptorCatalogTruncationMakesWholeDeadSegmentsReclaimable(t *testing.T) {
	dir, store, options := createDescriptorCatalogTestStore(t, 16)
	for i := uint64(1); i <= 8; i++ {
		if _, err := store.RegisterGroup(testGroupDescriptor(100 + i)); err != nil {
			t.Fatal(err)
		}
		if err := store.engine.Rotate(nil); err != nil {
			t.Fatal(err)
		}
		if err := store.engine.WaitSeal(); err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.ReadDir(filepath.Join(dir, nodeLogDir))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.CheckpointDescriptorCatalog(); err != nil {
		t.Fatal(err)
	}
	if err = store.engine.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = store.engine.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if err = store.ReclaimDeadNodeLogPrefix(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadDir(filepath.Join(dir, nodeLogDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(after) >= len(before) {
		t.Fatalf("reclaim did not reduce node-log files: before=%d after=%d", len(before), len(after))
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenNodeStore(dir, testNodeIdentity(), testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := uint64(0); i <= 8; i++ {
		requireDescriptorKey(t, store, testGroupDescriptor(100+i), i+1)
	}
}

func phaseName(phase DescriptorCheckpointPhase) string {
	switch phase {
	case DescriptorCheckpointTempWritten:
		return "temp-written"
	case DescriptorCheckpointFileSynced:
		return "file-synced"
	case DescriptorCheckpointRenamed:
		return "renamed"
	case DescriptorCheckpointDirectorySynced:
		return "directory-synced"
	case DescriptorCheckpointBeforeLogReference:
		return "before-log-reference"
	default:
		return "unknown"
	}
}
