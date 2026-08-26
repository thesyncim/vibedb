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
	context.Context, OperationID, uint8, *raftmember.Runtime,
) error {
	return a.err
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
