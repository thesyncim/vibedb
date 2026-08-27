package splitcontroller

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

const runtimeRegistryCrashRoot = "VIBEDB_SPLIT_REGISTRY_CRASH_ROOT"

type runtimeTerminalAuthorityStub struct {
	operation OperationID
	manifest  [sha256.Size]byte
	proof     [sha256.Size]byte
	terminal  bool
	calls     int
}

func (s *runtimeTerminalAuthorityStub) CertifyRuntimeTerminal(
	_ context.Context, operation OperationID, manifest [sha256.Size]byte,
) ([sha256.Size]byte, bool, error) {
	s.calls++
	if operation != s.operation || manifest != s.manifest {
		return [sha256.Size]byte{}, false, ErrRuntimeStore
	}
	return s.proof, s.terminal, nil
}

func TestRuntimeStoreRegistryBoundsAndReferenceCountsLeases(t *testing.T) {
	root := preparedRuntimeRoot(t)
	_, manifest := runtimeStoreIdentity()
	registry, err := OpenRuntimeStoreRegistry(root, manifest, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	first := runtimeOperation(1)
	leaseA, err := registry.Acquire(first)
	if err != nil {
		t.Fatal(err)
	}
	leaseB, err := registry.Acquire(first)
	if err != nil {
		t.Fatal(err)
	}
	secondLease, err := registry.Acquire(runtimeOperation(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.Acquire(runtimeOperation(3)); !errors.Is(err, ErrRuntimeRegistryCapacity) {
		t.Fatalf("capacity err=%v", err)
	}
	if err = leaseA.Persist(RuntimeStateCapture, 0, 1, []byte("shared")); err != nil {
		t.Fatal(err)
	}
	state, ok, err := leaseB.Load(RuntimeStateCapture, 0)
	if err != nil || !ok || string(state.Payload) != "shared" {
		t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
	}
	if err = leaseA.Release(); err != nil {
		t.Fatal(err)
	}
	if err = leaseA.Persist(RuntimeStateCapture, 0, 2, []byte("released")); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("released lease err=%v", err)
	}
	if err = registry.Close(); !errors.Is(err, ErrRuntimeRegistryInUse) {
		t.Fatalf("close with leases err=%v", err)
	}
	if err = leaseB.Release(); err != nil {
		t.Fatal(err)
	}
	if err = secondLease.Release(); err != nil {
		t.Fatal(err)
	}
	third, err := registry.Acquire(runtimeOperation(3))
	if err != nil {
		t.Fatal(err)
	}
	if err = third.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreRegistryTerminalGCIsWitnessedAndCrashDurable(t *testing.T) {
	root := preparedRuntimeRoot(t)
	operation, manifest := runtimeStoreIdentity()
	authority := &runtimeTerminalAuthorityStub{
		operation: operation, manifest: manifest,
		proof: sha256.Sum256([]byte("replicated terminal catalog record")), terminal: true,
	}
	registry, err := OpenRuntimeStoreRegistry(root, manifest, 1, authority)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(operation)
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Persist(RuntimeStatePrune, 0, 1, []byte("complete prune")); err != nil {
		t.Fatal(err)
	}
	if collected, collectErr := registry.CollectTerminal(context.Background(), operation); collected || !errors.Is(collectErr, ErrRuntimeRegistryInUse) || authority.calls != 0 {
		t.Fatalf("leased collect=%v err=%v calls=%d", collected, collectErr, authority.calls)
	}
	if err = lease.Release(); err != nil {
		t.Fatal(err)
	}
	collected, err := registry.CollectTerminal(context.Background(), operation)
	if err != nil || !collected || authority.calls != 1 {
		t.Fatalf("collect=%v err=%v calls=%d", collected, err, authority.calls)
	}
	if _, err = os.Stat(filepath.Join(root, runtimeOperationName(operation))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("operation directory retained err=%v", err)
	}
	if _, err = registry.Acquire(operation); !errors.Is(err, ErrRuntimeTerminal) {
		t.Fatalf("terminal acquire err=%v", err)
	}
	if err = registry.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen proves the fixed witness, rather than process memory, prevents
	// resurrection. It also authorizes idempotent cleanup without another RPC.
	reopened, err := OpenRuntimeStoreRegistry(root, manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err = reopened.Acquire(operation); !errors.Is(err, ErrRuntimeTerminal) {
		t.Fatalf("reopened terminal acquire err=%v", err)
	}
	if collected, err = reopened.CollectTerminal(context.Background(), operation); err != nil || !collected {
		t.Fatalf("reopened collect=%v err=%v", collected, err)
	}
}

func TestRuntimeStoreRegistryResumesCrashAfterTerminalWitness(t *testing.T) {
	root := preparedRuntimeRoot(t)
	operation, manifest := runtimeStoreIdentity()
	registry, err := OpenRuntimeStoreRegistry(root, manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(operation)
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Persist(RuntimeStatePrune, 0, 1, []byte("terminal state")); err != nil {
		t.Fatal(err)
	}
	if err = lease.Release(); err != nil {
		t.Fatal(err)
	}
	proof := sha256.Sum256([]byte("terminal authority witness"))
	// This is the exact crash boundary: the terminal marker reached disk but
	// the operation directory was not removed.
	if err = persistRuntimeTerminal(registry.root, operation, manifest, proof); err != nil {
		t.Fatal(err)
	}
	if err = registry.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenRuntimeStoreRegistry(root, manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err = reopened.Acquire(operation); !errors.Is(err, ErrRuntimeTerminal) {
		t.Fatalf("terminal operation resurrected err=%v", err)
	}
	if collected, collectErr := reopened.CollectTerminal(context.Background(), operation); collectErr != nil || !collected {
		t.Fatalf("resume collect=%v err=%v", collected, collectErr)
	}
	if _, err = os.Stat(filepath.Join(root, runtimeOperationName(operation))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("operation directory retained err=%v", err)
	}
}

func TestRuntimeStoreRegistryDoesNotCollectWithoutExactTerminalAuthority(t *testing.T) {
	root := preparedRuntimeRoot(t)
	operation, manifest := runtimeStoreIdentity()
	authority := &runtimeTerminalAuthorityStub{
		operation: operation, manifest: manifest,
		proof: sha256.Sum256([]byte("not terminal")), terminal: false,
	}
	registry, err := OpenRuntimeStoreRegistry(root, manifest, 1, authority)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	lease, err := registry.Acquire(operation)
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Persist(RuntimeStateTail, 0, 1, []byte("live")); err != nil {
		t.Fatal(err)
	}
	if err = lease.Release(); err != nil {
		t.Fatal(err)
	}
	if collected, collectErr := registry.CollectTerminal(context.Background(), operation); collectErr != nil || collected {
		t.Fatalf("uncertified collect=%v err=%v", collected, collectErr)
	}
	reopened, err := registry.Acquire(operation)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Release()
	state, ok, err := reopened.Load(RuntimeStateTail, 0)
	if err != nil || !ok || string(state.Payload) != "live" {
		t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
	}
}

func TestRuntimeStoreRegistryRecoversLeasesAfterProcessExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory replacement deliberately fails closed on non-unix platforms")
	}
	root := preparedRuntimeRoot(t)
	command := exec.Command(os.Args[0], "-test.run=^TestRuntimeStoreRegistryCrashHelper$")
	command.Env = append(os.Environ(), runtimeRegistryCrashRoot+"="+root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}
	operation, manifest := runtimeStoreIdentity()
	registry, err := OpenRuntimeStoreRegistry(root, manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	lease, err := registry.Acquire(operation)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	state, ok, err := lease.Load(RuntimeStateCapture, 0)
	if err != nil || !ok || state.Revision != 1 || string(state.Payload) != "registry crash" {
		t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
	}
}

func TestRuntimeStoreRegistryCrashHelper(t *testing.T) {
	root := os.Getenv(runtimeRegistryCrashRoot)
	if root == "" {
		return
	}
	operation, manifest := runtimeStoreIdentity()
	registry, err := OpenRuntimeStoreRegistry(root, manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(operation)
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Persist(RuntimeStateCapture, 0, 1, []byte("registry crash")); err != nil {
		t.Fatal(err)
	}
	// Bypass both lease release and registry close. The parent must recover
	// solely from fsynced state and kernel-released writer locks.
	os.Exit(0)
}

func TestRuntimeStoreRegistryRejectsUnpreparedAndSymlinkRoots(t *testing.T) {
	_, manifest := runtimeStoreIdentity()
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := OpenRuntimeStoreRegistry(missing, manifest, 1, nil); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("missing root err=%v", err)
	}
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "runtime")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := OpenRuntimeStoreRegistry(link, manifest, 1, nil); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("symlink root err=%v", err)
	}
}

func TestRuntimeStoreLeaseOwnsExactTopologySessionJournal(t *testing.T) {
	root := preparedRuntimeRoot(t)
	operation, manifest := runtimeStoreIdentity()
	registry, err := OpenRuntimeStoreRegistry(root, manifest, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	lease, err := registry.Acquire(operation)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, runtimeOperationName(operation), "topology-session")
	if got, pathErr := lease.TopologySessionJournalPath(); pathErr != nil || got != want {
		t.Fatalf("path=%q want=%q err=%v", got, want, pathErr)
	}
	if err = lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err = lease.TopologySessionJournalPath(); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("released lease path err=%v", err)
	}
}

func TestRuntimeStoreRegistryTerminalWitnessRejectsWrongManifest(t *testing.T) {
	root := preparedRuntimeRoot(t)
	operation, manifest := runtimeStoreIdentity()
	proof := sha256.Sum256([]byte("terminal"))
	registry, err := OpenRuntimeStoreRegistry(root, manifest, 1, &runtimeTerminalAuthorityStub{
		operation: operation, manifest: manifest, proof: proof, terminal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.CollectTerminal(context.Background(), operation); err != nil {
		t.Fatal(err)
	}
	if err = registry.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := manifest
	wrong[0] ^= 0xff
	if _, err = OpenRuntimeStoreRegistry(root, wrong, 1, nil); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("wrong root manifest err=%v", err)
	}
}

func preparedRuntimeRoot(t testing.TB) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "split-runtime")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func runtimeOperation(marker byte) OperationID {
	return OperationID(sha256.Sum256([]byte{marker}))
}
