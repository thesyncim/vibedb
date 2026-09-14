package raftstore

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
)

func TestNodeMaintenanceAcceptsOnlyAuthenticatedEmptyGenesis(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty genesis", true: "missing descriptor inventory"}[malformed], func(t *testing.T) {
			options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1, MaxGroups: 4}
			store, err := CreateNodeStore(filepath.Join(t.TempDir(), "node"), testNodeIdentity(), testKey(), nil, options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if malformed {
				if _, err = store.RegisterGroupWithSnapshot(testGroupDescriptor(1), nodeSnapshot(1, 1, 1)); err != nil {
					t.Fatal(err)
				}
				// An empty in-memory inventory with a nonempty authenticated
				// descriptor log is corruption, not a fresh capacity node.
				store.descriptors = store.descriptors[:0]
			}
			q, err := NewNodeSubmissionSequencer(store, 8)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = q.Close() })
			before, _ := store.engine.AppendWitness()
			err = q.MaintainNodeLog()
			if malformed {
				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("malformed empty metadata accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("fresh empty-node maintenance: %v", err)
			}
			if after, _ := store.engine.AppendWitness(); after != before || q.NodeMaintenanceRetryNeeded() {
				t.Fatal("empty maintenance wrote or scheduled an unnecessary retry")
			}
			for key := uint64(1); key <= 2; key++ {
				var registration Submission
				if err = registration.Initialize(); err == nil {
					err = registration.PrepareRegisterGroupWithSnapshotAt(testGroupDescriptor(key), nodeSnapshot(key, 1, 1), 7)
				}
				if err == nil {
					_, err = q.TrySubmit(&registration)
				}
				if err == nil {
					_, err = registration.Wait()
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err = q.MaintainNodeLog(); err != nil && !errors.Is(err, seglog.ErrBounds) {
				t.Fatalf("enrolled-node maintenance: %v", err)
			}
			if err = errors.Join(q.Close(), store.Close()); err != nil {
				t.Fatalf("enrolled-node shutdown: %v", err)
			}
		})
	}
}

func TestNodeMaintenanceSignalsRegistrationAndCheckpointsDescriptorCatalog(t *testing.T) {
	_, store, _ := createDescriptorCatalogTestStore(t, 8)
	defer store.Close()
	q, err := NewNodeSubmissionSequencer(store, 8)
	if err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case <-q.NodeMaintenanceWake():
		default:
			goto drained
		}
	}
drained:
	var registration Submission
	if err = registration.Initialize(); err != nil {
		t.Fatal(err)
	}
	if err = registration.PrepareRegisterGroup(testGroupDescriptor(200)); err != nil {
		t.Fatal(err)
	}
	if _, err = q.TrySubmit(&registration); err != nil {
		t.Fatal(err)
	}
	if _, err = registration.Wait(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.NodeMaintenanceWake():
	default:
		t.Fatal("successful registration did not signal maintenance")
	}
	if err = q.MaintainNodeLog(); err != nil && !errors.Is(err, seglog.ErrBounds) {
		t.Fatal(err)
	}
	metadata, ok := store.engine.Metadata(nodeDescriptorGroup)
	if !ok || metadata.Checkpoint.Index != uint64(len(store.descriptors)) {
		t.Fatalf("descriptor checkpoint=%+v descriptors=%d", metadata, len(store.descriptors))
	}
}
