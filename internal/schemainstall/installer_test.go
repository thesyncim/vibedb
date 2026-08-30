package schemainstall

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

type testActivator struct {
	mu                  sync.Mutex
	staged              map[[32]byte][32]byte
	active, drained     map[[32]byte]bool
	stageCalls          int
	activateCalls       int
	drainCalls          int
	failAfterActivation bool
	failAfterDrain      bool
	failAfterStage      bool
}

func newTestActivator() *testActivator {
	return &testActivator{staged: make(map[[32]byte][32]byte),
		active: make(map[[32]byte]bool), drained: make(map[[32]byte]bool)}
}

func (a *testActivator) ObserveStaged(_ context.Context, r Request, artifact [32]byte, _ string) ([32]byte, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	witness, found := a.staged[r.Operation]
	if found && artifact != r.BundleDigest {
		return [32]byte{}, false, ErrConflict
	}
	return witness, found, nil
}
func (a *testActivator) Stage(_ context.Context, r Request, artifact [32]byte, _ string) ([32]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if artifact != r.BundleDigest {
		return [32]byte{}, ErrConflict
	}
	a.stageCalls++
	witness := sha256.Sum256(append([]byte("materialized:"), r.Operation[:]...))
	a.staged[r.Operation] = witness
	if a.failAfterStage {
		return [32]byte{}, errors.New("stage response lost")
	}
	return witness, nil
}

func (a *testActivator) ObserveActive(_ context.Context, r Request, _ Authorization, _ [32]byte, _ string) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.active[r.Operation], nil
}

func (a *testActivator) Commit(context.Context, Request, Authorization, [32]byte, string) error {
	return nil
}
func (a *testActivator) Activate(_ context.Context, r Request, _ Authorization, _ [32]byte, _ string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.activateCalls++
	a.active[r.Operation] = true
	if a.failAfterActivation {
		return errors.New("activation response lost")
	}
	return nil
}
func (a *testActivator) ObserveDrained(_ context.Context, r Request, _ Authorization, _ DrainProof, _ [32]byte) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.drained[r.Operation], nil
}
func (a *testActivator) DrainOld(_ context.Context, r Request, _ Authorization, _ DrainProof, _ [32]byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.drainCalls++
	a.drained[r.Operation] = true
	if a.failAfterDrain {
		return errors.New("drain response lost")
	}
	return nil
}

func schemaFixture(seed byte) (Request, Authorization, DrainProof, []byte) {
	bundle := []byte{0x91, seed, 0x83, 0x01, 0x02, 0x03}
	request := Request{
		Operation: [32]byte{seed}, Group: raftGroup(seed), AllocationGeneration: 7,
		FromSchemaGeneration: 11, FromRelationManifestDigest: [32]byte{seed, 1},
		ToSchemaGeneration: 12, ToRelationManifestDigest: [32]byte{seed, 2},
		ApplyContractDigest: [32]byte{seed, 3}, BundleDigest: sha256.Sum256(bundle),
		BundleBytes: uint64(len(bundle)),
	}
	authorization := Authorization{Operation: request.Operation, TargetCatalogGeneration: 19,
		TargetCatalogDigest: [32]byte{seed, 4}, PreparedGroupCount: 3,
		PreparedGroupRoot: [32]byte{seed, 5}, ContractDigest: ContractDigest()}
	proof := DrainProof{Operation: request.Operation,
		TargetCatalogGeneration:       authorization.TargetCatalogGeneration,
		TargetCatalogDigest:           authorization.TargetCatalogDigest,
		ActivationAuthorizationDigest: AuthorizationDigest(authorization),
		CompletedOperationDigest:      [32]byte{seed, 6}, ReleasedExecutionPinRoot: [32]byte{seed, 7}}
	return request, authorization, proof, bundle
}

func raftGroup(seed byte) (group raftmember.GroupKey) {
	group.ClusterID[0] = seed
	group.ClusterIncarnation[0] = seed + 1
	group.TopologyRecoveryEpoch = 3
	group.ShardIncarnation[0] = seed + 2
	group.GroupID[0] = seed + 3
	return group
}

func openTestInstaller(t *testing.T, root string, activator *testActivator, max int) (*Installer, *FileJournal, *DirectoryBackend) {
	t.Helper()
	journal, err := OpenFileJournal(filepath.Join(root, "journal"), max)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := OpenDirectoryBackend(DirectoryOptions{Path: filepath.Join(root, "bundles"),
		MaxArtifacts: max, MaxDiskBytes: 1 << 20, Activator: activator})
	if err != nil {
		_ = journal.Close()
		t.Fatal(err)
	}
	installer, err := New(Options{Journal: journal, Backend: backend, MaxConcurrent: 8})
	if err != nil {
		_ = journal.Close()
		_ = backend.Close()
		t.Fatal(err)
	}
	return installer, journal, backend
}

func TestInstallerCrashReopenAuthorizationActivationAndDrain(t *testing.T) {
	root := t.TempDir()
	activator := newTestActivator()
	request, authorization, proof, bundle := schemaFixture(7)
	installer, journal, backend := openTestInstaller(t, root, activator, 4)
	receipt, err := installer.Prepare(context.Background(), request, bundle)
	if err != nil || receipt.ContractDigest != ContractDigest() || receipt.InstallationDigest == ([32]byte{}) {
		t.Fatalf("prepare = %#v, %v", receipt, err)
	}
	activator.mu.Lock()
	stageWitness := activator.staged[request.Operation]
	activator.mu.Unlock()
	wantInstallation := InstallationDigest(
		request, MaterializedArtifactDigest(request.BundleDigest, stageWitness),
	)
	if receipt.InstallationDigest != wantInstallation ||
		receipt.InstallationDigest == InstallationDigest(request, request.BundleDigest) {
		t.Fatal("prepared receipt does not bind materialized target witness")
	}
	if err = errors.Join(journal.Close(), backend.Close()); err != nil {
		t.Fatal(err)
	}

	installer, journal, backend = openTestInstaller(t, root, activator, 4)
	again, err := installer.Prepare(context.Background(), request, bundle)
	if err != nil || again != receipt {
		t.Fatalf("reopen prepare = %#v, %v", again, err)
	}
	record, err := installer.Authorize(context.Background(), authorization)
	if err != nil || record.State != StateAuthorized {
		t.Fatalf("authorize = %#v, %v", record, err)
	}
	activator.failAfterActivation = true
	record, err = installer.Activate(context.Background(), authorization)
	if err != nil || record.State != StateActive {
		t.Fatalf("activate settlement = %#v, %v", record, err)
	}
	activator.failAfterDrain = true
	record, err = installer.Drain(context.Background(), authorization, proof)
	if err != nil || record.State != StateDrained {
		t.Fatalf("drain settlement = %#v, %v", record, err)
	}
	if activator.activateCalls != 1 || activator.drainCalls != 1 {
		t.Fatalf("side effects = activate %d drain %d", activator.activateCalls, activator.drainCalls)
	}
	if activator.stageCalls != 1 {
		t.Fatalf("materialization calls = %d, want 1", activator.stageCalls)
	}
	if _, err = installer.Activate(context.Background(), authorization); err != nil {
		t.Fatal(err)
	}
	if _, err = installer.Drain(context.Background(), authorization, proof); err != nil {
		t.Fatal(err)
	}
	if activator.activateCalls != 1 || activator.drainCalls != 1 {
		t.Fatal("idempotent retry repeated side effect")
	}
	if err = errors.Join(journal.Close(), backend.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestInstallerSettlesOutcomeUnknownMaterialization(t *testing.T) {
	activator := newTestActivator()
	activator.failAfterStage = true
	installer, journal, backend := openTestInstaller(t, t.TempDir(), activator, 2)
	defer journal.Close()
	defer backend.Close()
	request, _, _, bundle := schemaFixture(8)
	receipt, err := installer.Prepare(context.Background(), request, bundle)
	if err != nil || receipt.InstallationDigest == ([32]byte{}) {
		t.Fatalf("outcome-unknown stage did not settle: %#v, %v", receipt, err)
	}
	if activator.stageCalls != 1 {
		t.Fatalf("stage calls = %d", activator.stageCalls)
	}
}

func TestInstallerRejectsSubstitutionAndPrematureActivation(t *testing.T) {
	installer, journal, backend := openTestInstaller(t, t.TempDir(), newTestActivator(), 2)
	defer journal.Close()
	defer backend.Close()
	request, authorization, proof, bundle := schemaFixture(9)
	if _, err := installer.Prepare(context.Background(), request, append(bundle, 0)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := installer.Prepare(context.Background(), request, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := installer.Activate(context.Background(), authorization); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	wrong := authorization
	wrong.PreparedGroupRoot[0]++
	if _, err := installer.Authorize(context.Background(), wrong); err != nil {
		t.Fatal(err)
	}
	if _, err := installer.Activate(context.Background(), authorization); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, err := installer.Activate(context.Background(), wrong); err != nil {
		t.Fatal(err)
	}
	if _, err := installer.Drain(context.Background(), wrong, DrainProof{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := installer.Drain(context.Background(), wrong, proof); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestInstallerConcurrentPrepareExecutesOnce(t *testing.T) {
	root := t.TempDir()
	activator := newTestActivator()
	installer, journal, backend := openTestInstaller(t, root, activator, 2)
	defer journal.Close()
	defer backend.Close()
	request, _, _, bundle := schemaFixture(12)
	const workers = 32
	results := make(chan error, workers)
	for range workers {
		go func() { _, err := installer.Prepare(context.Background(), request, bundle); results <- err }()
	}
	for range workers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "bundles"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("bundle directory entries = %d, want lock + one artifact", len(entries))
	}
}

func TestFileJournalRejectsCorruptionAndBound(t *testing.T) {
	root := t.TempDir()
	activator := newTestActivator()
	installer, journal, backend := openTestInstaller(t, root, activator, 1)
	first, _, _, bundle := schemaFixture(21)
	if _, err := installer.Prepare(context.Background(), first, bundle); err != nil {
		t.Fatal(err)
	}
	second, _, _, secondBundle := schemaFixture(22)
	if _, err := installer.Prepare(context.Background(), second, secondBundle); !errors.Is(err, ErrBound) {
		t.Fatal(err)
	}
	if err := errors.Join(journal.Close(), backend.Close()); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(root, "journal", recordName(first.Operation))
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 1
	if err = os.WriteFile(name, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenFileJournal(filepath.Join(root, "journal"), 1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
