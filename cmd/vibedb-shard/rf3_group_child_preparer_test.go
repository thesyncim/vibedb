package main

import (
	"errors"
	"path/filepath"
	"testing"

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
	preparer := &rf3GroupChildPreparer{
		manifest:  rf3Manifest{SplitControl: rf3ManifestSplitControl{MaxOperations: 1}},
		preparers: make([]*rf3ChildPreparer, 2),
	}
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
