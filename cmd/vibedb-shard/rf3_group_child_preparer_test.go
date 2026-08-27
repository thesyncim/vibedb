package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestRF3GroupChildRegistrySelectionBindsExactGroupProfileAndPaths(t *testing.T) {
	manifest := rf3Manifest{SplitControl: rf3ManifestSplitControl{MaxOperations: 2}}
	for _, table := range []string{"orders", "accounts"} {
		manifest.Groups = append(manifest.Groups, rf3ManifestGroup{
			Route: rf3ManifestGroupRoute{Distribution: table},
			ChildRegistry: rf3ManifestSplitChildRegistry{
				Root: filepath.Join(t.TempDir(), "split-children"), MaxOperations: 2, Table: table,
				Apply: rf3ManifestSplitChildApply{ShardKey: "/" + table},
			},
		})
	}
	operation := [32]byte{1}
	for index, group := range manifest.Groups {
		projected := manifest.withGroup(group)
		if projected.SplitControl.ChildRegistry.Root != group.ChildRegistry.Root ||
			projected.SplitControl.ChildRegistry.Table != group.ChildRegistry.Table ||
			projected.SplitControl.operationLimit() != 2 {
			t.Fatal("group projection substituted shared child authority")
		}
		paths, err := group.ChildRegistry.childPaths(operation, 1)
		if err != nil {
			t.Fatal(err)
		}
		target := splitcontroller.ChildReplicaTarget{
			RuntimeRoot: paths.Root, SQLPath: paths.Database, WALPath: paths.WAL,
			SQL: sqldriver.ReplicatedShardStoreIdentity{
				UserTable: group.ChildRegistry.Table,
				Binding:   sqldriver.ReplicatedShardStoreBinding{Distribution: group.Route.Distribution},
			},
			Apply: sqldriver.ReplicatedApplyIdentity{
				Placement: sqldriver.ReplicatedPlacementProfile{ShardKey: group.ChildRegistry.Apply.ShardKey},
			},
		}
		selected, registry, ok := rf3SplitChildRegistryForTarget(manifest, operation, 1, target)
		if !ok || selected != index || registry.Root != group.ChildRegistry.Root {
			t.Fatalf("selected=%d want=%d registry=%+v ok=%t", selected, index, registry, ok)
		}
		for name, change := range map[string]func(*splitcontroller.ChildReplicaTarget){
			"table":        func(target *splitcontroller.ChildReplicaTarget) { target.SQL.UserTable = "other" },
			"placement":    func(target *splitcontroller.ChildReplicaTarget) { target.Apply.Placement.ShardKey = "/other" },
			"distribution": func(target *splitcontroller.ChildReplicaTarget) { target.SQL.Binding.Distribution = "other" },
			"runtime path": func(target *splitcontroller.ChildReplicaTarget) { target.RuntimeRoot += "-other" },
			"wal path":     func(target *splitcontroller.ChildReplicaTarget) { target.WALPath += "-other" },
		} {
			changed := target
			change(&changed)
			if _, _, ok := rf3SplitChildRegistryForTarget(manifest, operation, 1, changed); ok {
				t.Fatalf("group %d accepted changed %s", index, name)
			}
		}
		if _, _, ok := rf3SplitChildRegistryForTarget(manifest, [32]byte{2}, 1, target); ok {
			t.Fatal("operation identity could reuse another operation's paths")
		}
		aliased := manifest
		aliased.Groups = []rf3ManifestGroup{group, group}
		if _, _, ok := rf3SplitChildRegistryForTarget(aliased, operation, 1, target); ok {
			t.Fatal("ambiguous child authority accepted")
		}
	}
}

func TestRF3GroupChildPreparationHasOneGlobalOperationBound(t *testing.T) {
	preparer := testRF3ChildAdmissionPreparer(t, 1)
	operation := [32]byte{1}
	if err := preparer.admit(operation, 0); err != nil {
		t.Fatal(err)
	}
	if err := preparer.admit(operation, 0); err != nil {
		t.Fatalf("exact retry failed: %v", err)
	}
	if err := preparer.admit(operation, 1); !errors.Is(err, splitcontroller.ErrChildPreparation) {
		t.Fatalf("operation changed group: %v", err)
	}
	if err := preparer.admit([32]byte{2}, 1); !errors.Is(err, errRF3SplitChildRegistryBound) {
		t.Fatalf("second group bypassed process-wide bound: %v", err)
	}
}

func (preparer *rf3GroupChildPreparer) admit(operation [32]byte, group int) error {
	_, err := preparer.reserve(operation, group, 0, operation, operation)
	return err
}

func testRF3ChildAdmissionPreparer(t *testing.T, limit int) *rf3GroupChildPreparer {
	t.Helper()
	manifest := rf3Manifest{Digest: [32]byte{0x42}, SplitControl: rf3ManifestSplitControl{MaxOperations: limit},
		ReplicaControl: rf3ManifestReplicaControl{SourceDataRoot: t.TempDir()}}
	preparer := &rf3GroupChildPreparer{manifest: manifest, preparers: make([]*rf3ChildPreparer, 2)}
	for group := range preparer.preparers {
		template := rf3ManifestSplitChildRegistry{Root: t.TempDir(), MaxOperations: limit}
		registry, err := newRF3SplitChildPathRegistry(template)
		if err != nil {
			t.Fatal(err)
		}
		preparer.manifest.Groups = append(preparer.manifest.Groups, rf3ManifestGroup{ChildRegistry: template})
		preparer.preparers[group] = &rf3ChildPreparer{registry: registry}
	}
	var err error
	preparer.store, preparer.slots, err = openRF3ChildAdmissionStore(manifest.ReplicaControl.SourceDataRoot, manifest.Digest, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = preparer.Close() })
	return preparer
}

func TestRF3ChildAdmissionCanceledDoesNotConsumeCapacity(t *testing.T) {
	preparer := testRF3ChildAdmissionPreparer(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 4 {
		if _, err := preparer.PrepareChild(ctx, splitcontroller.ChildPreparation{}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	if err := preparer.admit([32]byte{1}, 0); err != nil {
		t.Fatal(err)
	}
}

func TestRF3ChildAdmissionRecoversCapacityAndRejectsRelabel(t *testing.T) {
	preparer := testRF3ChildAdmissionPreparer(t, 1)
	op := [32]byte{1}
	if _, err := preparer.reserve(op, 0, 1, [32]byte{2}, [32]byte{3}); err != nil {
		t.Fatal(err)
	}
	if err := preparer.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	preparer.store, preparer.slots, err = openRF3ChildAdmissionStore(preparer.manifest.ReplicaControl.SourceDataRoot, preparer.manifest.Digest, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := preparer.admit(op, 1); !errors.Is(err, splitcontroller.ErrChildPreparation) {
		t.Fatalf("relabel=%v", err)
	}
	if err := preparer.admit([32]byte{4}, 1); !errors.Is(err, errRF3SplitChildRegistryBound) {
		t.Fatalf("restart bypassed bound=%v", err)
	}
	if _, err := preparer.reserve(op, 0, 1, [32]byte{2}, [32]byte{3}); err != nil {
		t.Fatal(err)
	}
	if _, err := preparer.reserve(op, 0, 1, [32]byte{5}, [32]byte{3}); !errors.Is(err, splitcontroller.ErrChildPreparation) {
		t.Fatalf("certificate substitution=%v", err)
	}
}

func TestRF3ChildAdmissionConstructorRestoresGroupLocalCapacity(t *testing.T) {
	preparer := testRF3ChildAdmissionPreparer(t, 1)
	op := [32]byte{1}
	if err := preparer.admit(op, 0); err != nil {
		t.Fatal(err)
	}
	if err := preparer.Close(); err != nil {
		t.Fatal(err)
	}
	address := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
	reopened, err := newRF3GroupChildPreparer(preparer.manifest, rafttransport.NodeID{1}, address, address, address, address)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.preparers[0].registry.acquire([32]byte{2}, 0); !errors.Is(err, errRF3SplitChildRegistryBound) {
		t.Fatalf("local capacity not reconstructed: %v", err)
	}
	if err := reopened.admit(op, 1); !errors.Is(err, splitcontroller.ErrChildPreparation) {
		t.Fatalf("reopened relabel=%v", err)
	}
}

func TestRF3ChildAdmissionReclaimsOnlyDurableTerminalCapacity(t *testing.T) {
	preparer := testRF3ChildAdmissionPreparer(t, 1)
	op, certificate := [32]byte{1}, [32]byte{2}
	if _, err := preparer.reserve(op, 0, 1, certificate, [32]byte{3}); err != nil {
		t.Fatal(err)
	}
	if err := preparer.recoverTerminal(); err != nil {
		t.Fatal(err)
	}
	if err := preparer.admit([32]byte{4}, 1); !errors.Is(err, errRF3SplitChildRegistryBound) {
		t.Fatalf("unwitnessed release=%v", err)
	}
	paths, err := preparer.manifest.Groups[0].ChildRegistry.childPaths(op, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	registry, err := splitcontroller.OpenRuntimeStoreRegistry(paths.Root, certificate, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.CollectCertifiedTerminal(splitcontroller.OperationID(op), [32]byte{9}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	// Crash after terminal witness but before admission checkpoint release.
	if err := preparer.Close(); err != nil {
		t.Fatal(err)
	}
	preparer.store, preparer.slots, err = openRF3ChildAdmissionStore(preparer.manifest.ReplicaControl.SourceDataRoot, preparer.manifest.Digest, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := preparer.recoverTerminal(); err != nil {
		t.Fatal(err)
	}
	if err := preparer.admit([32]byte{4}, 1); err != nil {
		t.Fatalf("terminal slot not reclaimed=%v", err)
	}
}
