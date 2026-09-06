package main

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func testRF3AdoptedInventory(t *testing.T) *rf3AdoptedGroupInventory {
	t.Helper()
	// An explicit node log selects the grouped grammar.  In-memory fixtures
	// without it intentionally retain the legacy single-group fallback.
	manifest := rf3Manifest{NodeLog: &rf3NodeLogManifest{}, Digest: [32]byte{1}, ReplicaControl: rf3ManifestReplicaControl{SourceDataRoot: t.TempDir()}, Groups: []rf3ManifestGroup{{}, {}}}
	inventory, err := openRF3AdoptedGroupInventory(manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inventory.Close() })
	return inventory
}

func testRF3AdoptedEntry(index byte) rf3AdoptedGroupEntry {
	return rf3AdoptedGroupEntry{operation: [32]byte{index}, receipt: [32]byte{index, 1}, plan: [32]byte{index, 2},
		certificate: [32]byte{index, 3}, cutover: [32]byte{index, 4}, group: 1, child: 1}
}

func TestRF3AdoptedInventoryCrashCutBeforeAndAfterPublish(t *testing.T) {
	inventory := testRF3AdoptedInventory(t)
	manifest := inventory.manifest
	first := testRF3AdoptedEntry(1)
	if err := inventory.record(first); err != nil {
		t.Fatal(err)
	}
	if _, err := openRF3AdoptedGroupInventory(manifest); err == nil {
		t.Fatal("second writer admitted")
	}
	if err := inventory.Close(); err != nil {
		t.Fatal(err)
	}
	// Pre-rename crash: only the committed file is authority, never the temp.
	if err := os.WriteFile(filepath.Join(manifest.ReplicaControl.SourceDataRoot, "adopted-groups.tmp"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := openRF3AdoptedGroupInventory(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.entries[0] != first || reopened.liveCount() != 1 {
		t.Fatal("lost certified group before host publication")
	}
	second := testRF3AdoptedEntry(2)
	if err := reopened.record(second); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	// Post-rename crash (including before host registration): exact inventory
	// must reconstruct both groups regardless of split-operation state.
	reopened, err = openRF3AdoptedGroupInventory(manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.liveCount() != 2 || reopened.entries[1] != second {
		t.Fatal("lost post-publish live inventory")
	}
	if err := reopened.record(first); err != nil {
		t.Fatal(err)
	}
	changed := first
	changed.plan[0] ^= 1
	if err := reopened.record(changed); err == nil {
		t.Fatal("changed plan reused live allocation")
	}
	if info, err := os.Stat(filepath.Join(manifest.ReplicaControl.SourceDataRoot, "adopted-groups.state")); err != nil || info.Size() != rf3AdoptedInventoryBytes {
		t.Fatalf("size %v %v", info, err)
	}
}

func TestRF3AdoptedInventoryCapacityIncludesDurablePendingSlots(t *testing.T) {
	inventory := testRF3AdoptedInventory(t)
	for index := 1; index <= maxRF3ManifestGroups-len(inventory.manifest.groupBundles()); index++ {
		if err := inventory.record(testRF3AdoptedEntry(byte(index))); err != nil {
			t.Fatal(err)
		}
	}
	var slots [maxRF3SplitChildOperations]rf3GroupChildPrepareSlot
	slots[0].certificates[0] = testRF3AdoptedEntry(1).certificate
	if err := inventory.checkCapacity(slots); err != nil {
		t.Fatal("activation counted same allocation twice", err)
	}
	slots[0].certificates[1] = [32]byte{255}
	if err := inventory.checkCapacity(slots); !errors.Is(err, errRF3SplitChildRegistryBound) {
		t.Fatal("pending allocation escaped live cap", err)
	}
	if err := inventory.record(testRF3AdoptedEntry(254)); !errors.Is(err, errRF3SplitChildRegistryBound) {
		t.Fatal("inventory exceeded process cap", err)
	}
}

func TestRF3AdoptedInventoryRejectsCorruptionAndRelabel(t *testing.T) {
	for _, kind := range []string{"truncated", "digest", "reserved", "group-overflow", "child-overflow", "duplicate", "manifest", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			inventory := testRF3AdoptedInventory(t)
			manifest := inventory.manifest
			if err := inventory.record(testRF3AdoptedEntry(1)); err != nil {
				t.Fatal(err)
			}
			if err := inventory.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(manifest.ReplicaControl.SourceDataRoot, "adopted-groups.state")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "truncated":
				raw = raw[:len(raw)-1]
			case "digest":
				raw[90] ^= 1
			case "reserved":
				raw[64+176] = 1
			case "group-overflow":
				binary.LittleEndian.PutUint64(raw[64+160:64+168], 1<<32)
			case "child-overflow":
				binary.LittleEndian.PutUint64(raw[64+168:64+176], 1<<32)
			case "duplicate":
				copy(raw[64+rf3AdoptedInventoryEntryBytes:], raw[64:64+rf3AdoptedInventoryEntryBytes])
			case "manifest":
				manifest.Digest[0] ^= 1
			case "symlink":
				outside := filepath.Join(t.TempDir(), "foreign")
				if err := os.WriteFile(outside, raw, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, path); err != nil {
					t.Fatal(err)
				}
			}
			if kind != "truncated" && kind != "digest" {
				digest := sha256.Sum256(raw[:len(raw)-32])
				copy(raw[len(raw)-32:], digest[:])
			}
			if kind != "symlink" {
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if reopened, err := openRF3AdoptedGroupInventory(manifest); err == nil {
				_ = reopened.Close()
				t.Fatal("accepted corrupt inventory")
			}
		})
	}
}

func TestRF3AdoptedInventoryUncertainOrClosedHandleRejectsExactRetry(t *testing.T) {
	inventory := testRF3AdoptedInventory(t)
	first := testRF3AdoptedEntry(1)
	if err := inventory.record(first); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(inventory.manifest.ReplicaControl.SourceDataRoot, "adopted-groups.state")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := inventory.record(testRF3AdoptedEntry(2)); err == nil || !inventory.failed {
		t.Fatal("uncertain publication not poisoned")
	}
	if err := inventory.record(first); err == nil {
		t.Fatal("exact retry bypassed poison")
	}
	if err := inventory.Close(); err != nil {
		t.Fatal(err)
	}
	if err := inventory.record(first); err == nil {
		t.Fatal("exact retry used closed inventory")
	}
}
