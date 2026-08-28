package driver

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// These host tests exercise catalog publication and pre-recovery rejection
// without pretending a portable collection qualifies Linux sealed storage.
func TestReplicatedSnapshotTargetHostPreparationGuards(t *testing.T) {
	for _, kind := range []string{"empty", "dirty", "initialized"} {
		t.Run(kind, func(t *testing.T) {
			catalog, reserved := childReservationCatalogFixture(t)
			base := *catalog.ReplicatedShardStore
			catalog.ReplicatedChildApply = nil
			root := t.TempDir()
			file, err := os.OpenFile(filepath.Join(root, "portable-user.vdb"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			collection, err := durable.Create(file, durable.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer collection.Close()
			core := &database{path: filepath.Join(root, "catalog.vdb"), dataDir: root, catalog: catalog,
				tables: map[string]*table{"docs": {meta: catalog.Tables["docs"], collection: collection}}, syncDir: func(path string) error {
					directory, openErr := os.Open(path)
					if openErr != nil {
						return openErr
					}
					return errors.Join(directory.Sync(), directory.Close())
				}}
			database := &Database{connector: &dbConnector{db: core}}
			if kind == "dirty" {
				if _, err = collection.Put([]byte("row"), []byte(`{"id":"row"}`)); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "initialized" {
				meta := replicatedApplyMetaFromIdentity(reserved)
				core.catalog.ReplicatedApply = &meta
			}
			err = database.PrepareReplicatedSnapshotTarget(base, reserved)
			if kind != "empty" {
				if !errors.Is(err, ErrReplicatedSnapshotStageProof) {
					t.Fatalf("accepted %s target: %v", kind, err)
				}
				if _, statErr := os.Stat(core.path); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("rejected target wrote catalog: %v", statErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = database.PrepareReplicatedSnapshotTarget(base, reserved); err != nil {
				t.Fatal(err)
			}
			if _, err = database.NewSession(context.Background()); !errors.Is(err, ErrReplicatedChildStageBusy) {
				t.Fatalf("unfenced target: %v", err)
			}
			raw, err := os.ReadFile(core.path)
			if err != nil {
				t.Fatal(err)
			}
			var decoded catalogFileVibe
			if err = decodeCatalogJSON(raw, &decoded); err != nil || decoded.ReplicatedApply != nil || decoded.ReplicatedChildApply == nil || decoded.ReplicatedChildApply.identity() != reserved {
				t.Fatalf("exact empty reservation not durable: %v", err)
			}
			wrong := reserved
			wrong.MaxSessions++
			if err = database.PrepareReplicatedSnapshotTarget(base, wrong); !errors.Is(err, ErrReplicatedApplyMismatch) {
				t.Fatalf("mismatched retry: %v", err)
			}
			if core.checkpointGroup != nil || core.replicatedApplyCollection != nil {
				t.Fatal("reservation initialized apply")
			}
		})
	}
}

func TestReplicatedSnapshotTargetHostIdentityBeforeRecovery(t *testing.T) {
	for _, active := range []bool{false, true} {
		catalog, reserved := childReservationCatalogFixture(t)
		base := *catalog.ReplicatedShardStore
		if active {
			catalog.ReplicatedApply, catalog.ReplicatedChildApply = catalog.ReplicatedChildApply, nil
		}
		root := t.TempDir()
		path := filepath.Join(root, "catalog.vdb")
		raw, err := appendCatalogJSON(nil, catalog)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func(*ReplicatedShardStoreIdentity, *ReplicatedApplyIdentity){
			func(_ *ReplicatedShardStoreIdentity, apply *ReplicatedApplyIdentity) { apply.MaxSessions++ },
			func(_ *ReplicatedShardStoreIdentity, apply *ReplicatedApplyIdentity) {
				apply.Storage = strings.Repeat("c", 64)
			},
			func(base *ReplicatedShardStoreIdentity, _ *ReplicatedApplyIdentity) { base.Binding.StoreID[0]++ },
		} {
			wrongBase, wrongApply := base, reserved
			mutate(&wrongBase, &wrongApply)
			opened, openErr := OpenReplicatedSnapshotTarget(path, wrongBase, wrongApply)
			if opened != nil || (!errors.Is(openErr, ErrReplicatedApplyMismatch) && !errors.Is(openErr, ErrReplicatedShardStoreIdentityMismatch)) {
				t.Fatalf("active=%t wrong identity reached recovery: %v", active, openErr)
			}
			if _, statErr := os.Stat(path + ".tables"); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("mismatch entered private namespace: %v", statErr)
			}
		}
	}
}

func TestReplicatedSnapshotTargetPrepareColdOpenInstallReopen(t *testing.T) {
	// Reuse a real certified artifact containing a user row, session and capture
	// records. The new target has no apply storage or checkpoint at preparation.
	original, sourceTarget, bootstrap, artifact, _ := newReplicatedSnapshotStageFixture(t)
	manifest := original.expected
	if err := errors.Join(original.Close(), sourceTarget.Close()); err != nil {
		t.Fatal(err)
	}
	path, target, binding, _ := prepareReplicatedTestRoot(t, "cold-target", false)
	binding.MemberID = 10
	binding.StoreID[0]++
	base := requireReplicatedShardStoreBind(t, target, binding, "docs")
	options := testReplicatedApplyOptions()
	reserved, err := NewReplicatedChildApplyIdentity(base, strings.Repeat("a", 64), strings.Repeat("b", 64), options)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err = target.PrepareReplicatedSnapshotTarget(base, reserved); err != nil {
			t.Fatal(err)
		}
	}
	core := target.connector.db
	if core.checkpointGroup != nil || core.catalog.ReplicatedApply != nil || core.replicatedApplyCollection != nil {
		t.Fatal("cold preparation initialized a replicated state machine")
	}
	if _, err = target.NewSession(context.Background()); !errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatalf("cold preparation allowed SQL: %v", err)
	}
	if err = target.Close(); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []func(*ReplicatedApplyIdentity){
		func(identity *ReplicatedApplyIdentity) { identity.MaxSessions++ },
		func(identity *ReplicatedApplyIdentity) { identity.Storage = strings.Repeat("c", 64) },
	} {
		wrong := reserved
		mutation(&wrong)
		if opened, openErr := OpenReplicatedSnapshotTarget(path, base, wrong); opened != nil || !errors.Is(openErr, ErrReplicatedApplyMismatch) {
			t.Fatalf("substituted cold identity accepted: %v", openErr)
		}
	}
	if opened, openErr := OpenReplicatedShardStoreWithApply(path, base, reserved); opened != nil || !errors.Is(openErr, ErrReplicatedApplyUninitialized) {
		t.Fatalf("uninitialized target opened as serving apply: %v", openErr)
	}
	target, err = OpenReplicatedSnapshotTarget(path, base, reserved)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	if _, err = target.NewSession(context.Background()); !errors.Is(err, ErrReplicatedChildStageBusy) {
		t.Fatal(err)
	}
	if apply, _, applyErr := target.OpenReplicatedApply(base, bootstrap, options); apply != nil || !errors.Is(applyErr, ErrReplicatedChildStageBusy) {
		t.Fatalf("cold opener authorized apply: %v", applyErr)
	}
	stage, actualIdentity, err := target.OpenReplicatedSnapshotStage(base, manifest, nil, options, replicatedstate.SnapshotArtifactStageOptions{})
	if err != nil || actualIdentity != reserved {
		t.Fatalf("stage identity=%+v err=%v", actualIdentity, err)
	}
	var cursor []byte
	if _, err = stage.Receive(bytes.NewReader(artifact), func(raw []byte) error { cursor = bytes.Clone(raw); return nil }); err != nil {
		t.Fatal(err)
	}
	if err = errors.Join(stage.Close(), target.Close()); err != nil {
		t.Fatal(err)
	}
	// Resume after complete image receipt but before any checkpoint creation.
	target, err = OpenReplicatedSnapshotTarget(path, base, reserved)
	if err != nil {
		t.Fatal(err)
	}
	stage, _, err = target.OpenReplicatedSnapshotStage(base, manifest, cursor, options, replicatedstate.SnapshotArtifactStageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stage.Receive(bytes.NewReader(artifact[stage.Offset():]), func(raw []byte) error { cursor = bytes.Clone(raw); return nil }); err != nil {
		t.Fatal(err)
	}
	wrongBootstrap := proto.Clone(bootstrap).(*pb.Snapshot)
	wrongBootstrap.Metadata.ConfState.Learners = nil
	if _, err = stage.Activate(wrongBootstrap); err == nil {
		t.Fatal("accepted different original bootstrap learner set")
	}
	activation, err := stage.Activate(bootstrap)
	if err != nil || activation.Apply == nil {
		t.Fatalf("activation: %v", err)
	}
	if got, identityErr := activation.Apply.Identity(); identityErr != nil || got != reserved {
		t.Fatalf("installed identity: %+v %v", got, identityErr)
	}
	if err = errors.Join(activation.Apply.Close(), stage.Close(), target.Close()); err != nil {
		t.Fatal(err)
	}
	target, err = OpenReplicatedSnapshotTarget(path, base, reserved)
	if err != nil {
		t.Fatal(err)
	}
	activation, resumed, err := target.ResumeReplicatedSnapshotActivation(base, manifest, bootstrap, options)
	if err != nil || !resumed || activation.Apply == nil {
		t.Fatalf("certified restart resumed=%v err=%v", resumed, err)
	}
	if err = errors.Join(activation.Apply.Close(), target.Close()); err != nil {
		t.Fatal(err)
	}
	target, err = OpenReplicatedShardStoreWithApply(path, base, reserved)
	if err != nil {
		t.Fatalf("certified target exact serving reopen: %v", err)
	}
}

func TestReplicatedSnapshotTargetRejectsInitializedStore(t *testing.T) {
	path, database, binding, _ := prepareReplicatedTestRoot(t, "initialized-not-cold", false)
	base := requireReplicatedShardStoreBind(t, database, binding, "docs")
	bootstrap := testReplicatedApplyBootstrap()
	apply, identity, err := database.OpenReplicatedApply(base, bootstrap, testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	if err = apply.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.PrepareReplicatedSnapshotTarget(base, identity); !errors.Is(err, ErrReplicatedSnapshotStageProof) {
		t.Fatalf("repurposed initialized target: %v", err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	checkpointPath := filepath.Join(path+".tables", "checkpoint.vgc")
	checkpoint, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatal(err)
	}
	if opened, openErr := OpenReplicatedSnapshotTarget(path, base, identity); opened != nil || !errors.Is(openErr, ErrReplicatedSnapshotStageProof) {
		t.Fatalf("initialized target cold open: %v", openErr)
	}
	after, err := os.ReadFile(checkpointPath)
	if err != nil || !bytes.Equal(checkpoint, after) {
		t.Fatalf("rejected cold open changed initialized checkpoint: %v", err)
	}
	database, err = OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedSnapshotTargetResumesCertifiedActivationCuts(t *testing.T) {
	for _, point := range []replicatedSnapshotStageFaultPoint{
		replicatedSnapshotStageAfterGroupCreate, replicatedSnapshotStageAfterSeed,
		replicatedSnapshotStageAfterMachineOpen, replicatedSnapshotStageAfterSnapshotInstall,
	} {
		t.Run(string(rune('0'+point)), func(t *testing.T) {
			stage, target, bootstrap, artifact, cursor := newReplicatedSnapshotStageFixture(t)
			path, base, identity, manifest := target.connector.db.path, stage.base, stage.identity, stage.expected
			fault := errors.New("certified activation cut")
			previous := replicatedSnapshotStageFaultHook
			fired := false
			replicatedSnapshotStageFaultHook = func(got replicatedSnapshotStageFaultPoint) error {
				if got == point && !fired {
					fired = true
					return fault
				}
				return nil
			}
			_, err := stage.Activate(bootstrap)
			replicatedSnapshotStageFaultHook = previous
			if !fired || !errors.Is(err, fault) {
				t.Fatalf("cut fired=%v err=%v", fired, err)
			}
			if err = errors.Join(stage.Close(), target.Close()); err != nil {
				t.Fatal(err)
			}
			target, err = OpenReplicatedSnapshotTarget(path, base, identity)
			if err != nil {
				t.Fatalf("open seeded recovery: %v", err)
			}
			defer target.Close()
			activation, resumed, err := target.ResumeReplicatedSnapshotActivation(base, manifest, bootstrap, testReplicatedApplyOptions())
			if err != nil {
				t.Fatal(err)
			}
			if !resumed {
				stage, _, err = target.OpenReplicatedSnapshotStage(base, manifest, cursor, testReplicatedApplyOptions(), replicatedstate.SnapshotArtifactStageOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = stage.Receive(bytes.NewReader(artifact[stage.Offset():]), func([]byte) error { return nil }); err != nil {
					t.Fatal(err)
				}
				activation, err = stage.Activate(bootstrap)
				if err != nil {
					t.Fatal(err)
				}
			}
			if activation.Apply == nil || activation.ApplyIdentity != identity {
				t.Fatal("recovery substituted apply identity")
			}
			if err = activation.Apply.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
