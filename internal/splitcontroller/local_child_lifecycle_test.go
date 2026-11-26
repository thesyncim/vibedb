package splitcontroller

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

type rejectingSplitChildAdopter struct{ err error }

func (a rejectingSplitChildAdopter) AdoptSplitChild(
	context.Context, OperationID, uint8, PreparedChildRuntime,
) error {
	return a.err
}

func TestLocalChildLifecycleObservationSurvivesRuntimeHandoff(t *testing.T) {
	lifecycle := &LocalChildLifecycle{options: LocalChildLifecycleOptions{Child: 1},
		applyID:    sqldriver.ReplicatedApplyIdentity{Storage: "apply"},
		profile:    sqldriver.ReplicatedApplyCapacityProfile{Initialized: true},
		walBinding: sqldriver.ReplicatedShardStoreBinding{MemberID: 2},
		adopted:    raftmember.RuntimeIdentity{MemberID: 2},
	}
	observed, err := lifecycle.ObserveChild(1)
	if err != nil || observed == nil || observed.Phase != ChildPhaseRuntimeAdopted ||
		observed.ApplyIdentity.Storage != "apply" || observed.WALBinding.MemberID != 2 ||
		observed.RuntimeIdentity.MemberID != 2 {
		t.Fatalf("observation=%+v err=%v", observed, err)
	}
	if _, err = lifecycle.ObserveChild(2); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("wrong child err=%v", err)
	}
}

func TestLocalChildLifecycleRejectsActionBeforeExactCertifiedPredecessor(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	target, ok := plan.Target(1)
	if !ok {
		t.Fatal("missing child target")
	}
	lifecycle, err := NewLocalChildLifecycle(LocalChildLifecycleOptions{
		Child: 1, Stage: new(sqldriver.ReplicatedChildStage), Database: new(sqldriver.Database),
		StaticBootstrap: new(pb.Snapshot), WALPath: filepath.Join(t.TempDir(), "child.wal"),
		WALIdentity: target.WAL, TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
		Authority: target.Authority, SQL: target.SQL,
		Adopter: rejectingSplitChildAdopter{err: errors.New("must not be called")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = lifecycle.ExecuteCreateChildWAL(
		plan, rangesplit.CutoverCertificate{},
	); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("WAL before activation = %v", err)
	}
	if err = lifecycle.ExecuteAdoptChildRuntime(
		context.Background(), plan,
	); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("runtime before WAL = %v", err)
	}
}

func TestLocalChildLifecycleRequiresExactPlanTargetAndAbsoluteWAL(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	target, _ := plan.Target(1)
	options := LocalChildLifecycleOptions{
		Child: 1, Stage: new(sqldriver.ReplicatedChildStage), Database: new(sqldriver.Database),
		StaticBootstrap: new(pb.Snapshot), WALPath: "relative.wal",
		WALIdentity: target.WAL, TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
		Authority: target.Authority, SQL: target.SQL,
		Adopter: rejectingSplitChildAdopter{},
	}
	if _, err := NewLocalChildLifecycle(options); !errors.Is(err, ErrRuntimeStore) {
		t.Fatalf("relative WAL = %v", err)
	}
	options.WALPath = filepath.Join(t.TempDir(), "child.wal")
	lifecycle, err := NewLocalChildLifecycle(options)
	if err != nil {
		t.Fatal(err)
	}
	wrong := *plan
	wrong.targets[1].WAL.MemberID++
	if err = lifecycle.ExecuteActivateChild(
		&wrong, rangesplit.CutoverCertificate{},
	); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("wrong target activation = %v", err)
	}
}

func testLocalChildLifecycleReplayWithCertificate(
	t *testing.T,
	plan *Plan,
	certificate rangesplit.CutoverCertificate,
) {
	t.Helper()
	target, ok := plan.Target(1)
	if !ok || len(target.Replicas) == 0 {
		t.Fatalf("expected target replicas, got ok=%t len=%d", ok, len(target.Replicas))
	}
	replica := target.Replicas[len(target.Replicas)-1]
	replicaCopy := replica
	replicaCopy.SQL = replica.SQL.Clone()
	local := cloneChildTarget(target)
	local.WAL = replica.WAL
	local.SQL = replica.SQL.Clone()
	lifecycle := &LocalChildLifecycle{
		options: LocalChildLifecycleOptions{
			Child:                 1,
			Replica:               &replicaCopy,
			WALIdentity:           replica.WAL,
			TopologyRecoveryEpoch: target.TopologyRecoveryEpoch,
			Authority:             target.Authority,
			SQL:                   replica.SQL.Clone(),
		},
		certificateDigest: certificate.Digest(),
		applyID:           replica.Apply,
		profile: sqldriver.ReplicatedApplyCapacityProfile{
			Binding:                replica.SQL.Binding,
			Initialized:            true,
			Applied:                certificate.SourceCut().Applied,
			SessionEpochHighWater:  certificate.SourceCut().Applied,
			RelationManifestDigest: target.RelationManifestDigest,
			MaxSessions:            replica.Apply.MaxSessions,
			RetryWindow:            replica.Apply.RetryWindow,
		},
		walBinding: replica.SQL.Binding,
		adopted:    testRuntimeIdentity(local),
	}
	if err := lifecycle.ExecuteActivateChild(plan, certificate); err != nil {
		t.Fatalf("replayed ExecuteActivateChild after adoption = %v", err)
	}
	if err := lifecycle.ExecuteCreateChildWAL(plan, certificate); err != nil {
		t.Fatalf("replayed ExecuteCreateChildWAL after adoption = %v", err)
	}
	lifecycle.certificateDigest[0] ^= 0xff
	if err := lifecycle.ExecuteActivateChild(plan, certificate); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("mismatched digest ExecuteActivateChild = %v, want ErrTopologyConflict", err)
	}
	if err := lifecycle.ExecuteCreateChildWAL(plan, certificate); !errors.Is(err, ErrTopologyConflict) {
		t.Fatalf("mismatched digest ExecuteCreateChildWAL = %v, want ErrTopologyConflict", err)
	}
}
