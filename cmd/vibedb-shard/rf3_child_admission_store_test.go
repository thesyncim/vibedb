package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRF3ChildAdmissionCheckpointExactRestartAndBound(t *testing.T) {
	root, digest := t.TempDir(), [32]byte{1}
	store, slots, err := openRF3ChildAdmissionStore(root, digest, 2)
	if err != nil {
		t.Fatal(err)
	}
	slots[0] = rf3GroupChildPrepareSlot{operation: [32]byte{2}, group: 1,
		certificates: [3][32]byte{{}, {3}}, requests: [3][32]byte{{}, {4}}}
	if err := store.save(slots); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openRF3ChildAdmissionStore(root, digest, 2); err == nil {
		t.Fatal("second writer acquired active journal")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// A crash before rename can leave only the fixed temporary record. The
	// committed state remains the sole recovery authority.
	if err := os.WriteFile(filepath.Join(root, "child-preparations.tmp"), []byte("truncated"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, recovered, err := openRF3ChildAdmissionStore(root, digest, 2)
	if err != nil || recovered != slots {
		t.Fatalf("recovery mismatch err=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(root, "child-preparations.state")); err != nil || info.Size() != rf3ChildAdmissionBytes {
		t.Fatalf("unbounded checkpoint %v %v", info, err)
	}
	if _, _, err := openRF3ChildAdmissionStore(root, [32]byte{9}, 2); err == nil {
		t.Fatal("manifest substitution accepted")
	}
	if _, _, err := openRF3ChildAdmissionStore(root, digest, 1); err == nil {
		t.Fatal("capacity substitution accepted")
	}
}

func TestRF3ChildAdmissionCheckpointRejectsCorruption(t *testing.T) {
	for _, kind := range []string{"truncate", "digest", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			root, digest := t.TempDir(), [32]byte{1}
			store, _, err := openRF3ChildAdmissionStore(root, digest, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "child-preparations.state")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "truncate":
				err = os.WriteFile(path, raw[:len(raw)-1], 0o600)
			case "digest":
				raw[100] ^= 1
				err = os.WriteFile(path, raw, 0o600)
			case "symlink":
				outside := filepath.Join(t.TempDir(), "state")
				if err := os.WriteFile(outside, raw, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink(outside, path)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := openRF3ChildAdmissionStore(root, digest, 1); err == nil {
				t.Fatal("corrupt checkpoint accepted")
			}
		})
	}
}

func TestRF3ChildAdmissionUncertainPublicationPoisonsHandle(t *testing.T) {
	root, digest := t.TempDir(), [32]byte{1}
	store, slots, err := openRF3ChildAdmissionStore(root, digest, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Force rename failure after the replacement file has been written/synced.
	if err := os.Rename(filepath.Join(root, "child-preparations.state"), filepath.Join(root, "retained")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "child-preparations.state"), 0o700); err != nil {
		t.Fatal(err)
	}
	slots[0].operation = [32]byte{2}
	if err := store.save(slots); err == nil || !store.failed {
		t.Fatalf("publication err=%v poisoned=%t", err, store.failed)
	}
	if err := store.save(slots); err == nil {
		t.Fatal("poisoned journal accepted a new admission")
	}
}
