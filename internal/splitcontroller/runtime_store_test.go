package splitcontroller

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

const runtimeStoreCrashRoot = "VIBEDB_SPLIT_RUNTIME_CRASH_ROOT"

func TestDurableRuntimeStoreRecoversEveryBoundedOperationSlot(t *testing.T) {
	root := t.TempDir()
	operation, manifest := runtimeStoreIdentity()
	store, err := OpenDurableRuntimeStore(root, operation, manifest)
	if err != nil {
		t.Fatal(err)
	}
	writes := []struct {
		kind    RuntimeStateKind
		child   uint8
		payload []byte
	}{
		{RuntimeStateCapture, 0, []byte("capture-collection-fence")},
		{RuntimeStateArtifacts, 0, []byte("artifact-manifest")},
		{RuntimeStateStage, 0, []byte("retained-stage")},
		{RuntimeStateStage, 1, []byte("child-stage")},
		{RuntimeStateTail, 0, []byte("tail-cursor")},
		{RuntimeStatePrune, 0, []byte("prune-cursor")},
	}
	for _, write := range writes {
		if err = store.Persist(write.kind, write.child, 1, write.payload); err != nil {
			t.Fatalf("persist kind=%d child=%d: %v", write.kind, write.child, err)
		}
		if err = store.Persist(write.kind, write.child, 1, write.payload); err != nil {
			t.Fatalf("idempotent retry kind=%d child=%d: %v", write.kind, write.child, err)
		}
	}
	if err = store.Persist(RuntimeStateTail, 0, 2, []byte("tail-cursor-next")); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenDurableRuntimeStore(root, operation, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, write := range writes {
		wantRevision, wantPayload := uint64(1), write.payload
		if write.kind == RuntimeStateTail {
			wantRevision, wantPayload = 2, []byte("tail-cursor-next")
		}
		state, ok, loadErr := reopened.Load(write.kind, write.child)
		if loadErr != nil || !ok || state.Revision != wantRevision ||
			!bytes.Equal(state.Payload, wantPayload) {
			t.Fatalf("load kind=%d child=%d state=%+v ok=%v err=%v", write.kind, write.child, state, ok, loadErr)
		}
		if len(state.Payload) != 0 {
			state.Payload[0] ^= 0xff
			again, _, _ := reopened.Load(write.kind, write.child)
			if bytes.Equal(state.Payload, again.Payload) {
				t.Fatal("load payload aliases retained store state")
			}
		}
	}
}

func TestDurableRuntimeStoreRejectsConcurrentWriterAndRevisionRegression(t *testing.T) {
	root := t.TempDir()
	operation, manifest := runtimeStoreIdentity()
	store, err := OpenDurableRuntimeStore(root, operation, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = OpenDurableRuntimeStore(root, operation, manifest); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("second writer err=%v", err)
	}
	if err = store.Persist(RuntimeStateCapture, 0, 2, []byte("skipped")); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("initial skipped revision err=%v", err)
	}
	if err = store.Persist(RuntimeStateCapture, 0, 1, []byte("one")); err != nil {
		t.Fatal(err)
	}
	for _, revision := range []uint64{0, 1, 3} {
		if err = store.Persist(RuntimeStateCapture, 0, revision, []byte("different")); !errors.Is(err, ErrRuntimeStore) {
			t.Fatalf("revision %d err=%v", revision, err)
		}
	}
	state, ok, err := store.Load(RuntimeStateCapture, 0)
	if err != nil || !ok || state.Revision != 1 || string(state.Payload) != "one" {
		t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
	}
}

func TestDurableRuntimeStoreRecoversAfterProcessExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory replacement deliberately fails closed on non-unix platforms")
	}
	root := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestDurableRuntimeStoreCrashHelper$")
	command.Env = append(os.Environ(), runtimeStoreCrashRoot+"="+root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}
	operation, manifest := runtimeStoreIdentity()
	store, err := OpenDurableRuntimeStore(root, operation, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, check := range []struct {
		kind RuntimeStateKind
		want string
	}{
		{RuntimeStateCapture, "capture-before-exit"},
		{RuntimeStateTail, "tail-before-exit"},
	} {
		state, ok, loadErr := store.Load(check.kind, 0)
		if loadErr != nil || !ok || state.Revision != 1 || string(state.Payload) != check.want {
			t.Fatalf("kind=%d state=%+v ok=%v err=%v", check.kind, state, ok, loadErr)
		}
	}
}

func TestDurableRuntimeStoreCrashHelper(t *testing.T) {
	root := os.Getenv(runtimeStoreCrashRoot)
	if root == "" {
		return
	}
	operation, manifest := runtimeStoreIdentity()
	store, err := OpenDurableRuntimeStore(root, operation, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Persist(RuntimeStateCapture, 0, 1, []byte("capture-before-exit")); err != nil {
		t.Fatal(err)
	}
	if err = store.Persist(RuntimeStateTail, 0, 1, []byte("tail-before-exit")); err != nil {
		t.Fatal(err)
	}
	// Deliberately bypass Close and all testing cleanup. The parent verifies
	// that the kernel-released lease and fsynced replacements reopen exactly.
	os.Exit(0)
}

func TestDurableRuntimeStoreFailsClosedOnCorruptionAndWrongManifest(t *testing.T) {
	root := t.TempDir()
	operation, manifest := runtimeStoreIdentity()
	store, err := OpenDurableRuntimeStore(root, operation, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Persist(RuntimeStateTail, 0, 1, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := manifest
	wrong[0] ^= 0xff
	if _, err = OpenDurableRuntimeStore(root, operation, wrong); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("wrong manifest err=%v", err)
	}
	statePath := runtimeStoreStatePath(root, operation, "tail.state")
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err = os.WriteFile(statePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenDurableRuntimeStore(root, operation, manifest); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("corrupt state err=%v", err)
	}
}

func TestDurableRuntimeStoreBoundsBeforeDiskWrite(t *testing.T) {
	root := t.TempDir()
	operation, manifest := runtimeStoreIdentity()
	store, err := OpenDurableRuntimeStore(root, operation, manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	oversized := make([]byte, MaxTailControlBytes+1)
	if err = store.Persist(RuntimeStateTail, 0, 1, oversized); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("oversized err=%v", err)
	}
	if _, err = os.Stat(runtimeStoreStatePath(root, operation, "tail.state")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversized write created state err=%v", err)
	}
	if err = store.Persist(RuntimeStateStage, 3, 1, []byte("bad child")); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("bad child err=%v", err)
	}
}

func TestDurableRuntimeStoreRejectsSymlinkNamespace(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "split-runtime")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	operation, manifest := runtimeStoreIdentity()
	if _, err := OpenDurableRuntimeStore(root, operation, manifest); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("symlink runtime err=%v", err)
	}
}

func runtimeStoreIdentity() (OperationID, [sha256.Size]byte) {
	return OperationID(sha256.Sum256([]byte("split operation"))),
		sha256.Sum256([]byte("retained member manifest"))
}

func runtimeStoreStatePath(root string, operation OperationID, name string) string {
	return filepath.Join(root, "split-runtime", fmtOperationID(operation), name)
}

func fmtOperationID(operation OperationID) string {
	var encoded [64]byte
	hex.Encode(encoded[:], operation[:])
	return string(encoded[:])
}
