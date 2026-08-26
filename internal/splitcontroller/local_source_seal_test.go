package splitcontroller

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestLocalSourceSealAndCutoverCertificateSurviveRestart(t *testing.T) {
	plan, _, _, _ := testPlan(t)
	source := newFlowSource(t, plan)
	if _, err := source.machine.InstallSnapshot(source.bootstrap); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	manifest := testManifestDigest("source-seal-certificate")
	store, err := OpenDurableRuntimeStore(root, plan.OperationID(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	actions, err := NewLocalSourceActions(store, source.machine, source.capture)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := actions.ExecuteStartCapture(plan)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := actions.ExecuteBuildArtifacts(
		plan, capture, rangesplit.MinChildArtifactChunkBytes,
	)
	if err != nil {
		t.Fatal(err)
	}

	childDB, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer childDB.Close()
	childCollection, err := childDB.CreateCollection(
		"child", durable.Options{MaxBatchDocuments: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	childStore, err := OpenDurableRuntimeStore(
		t.TempDir(), plan.OperationID(), testManifestDigest("sealed-child"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer childStore.Close()
	child, err := NewLocalChildActions(childStore, childCollection, 0)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := actions.OpenChildArtifact(plan, artifacts, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = child.ExecuteStageChild(plan, artifacts, 1, artifact); err != nil {
		_ = artifact.Close()
		t.Fatal(err)
	}
	if err = artifact.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = source.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	sinks := []rangesplit.TailSink{
		func(rangesplit.TailBatch) error { return nil },
		func(batch rangesplit.TailBatch) error {
			return child.ApplyTailBatch(plan, artifacts, 1, batch)
		},
	}
	tail, advanced, err := actions.ExecuteCatchUpTail(plan, capture, artifacts, sinks)
	if err != nil || !advanced || tail.Sealed() || tail.SourceCut().Applied != 2 {
		t.Fatalf("configuration tail=%+v advanced=%t err=%v", tail, advanced, err)
	}
	state := flowSourceState(t, source.machine)
	serving := sourceServingState(t, source.machine, state)
	proposer := new(capturingSealProposer)
	if err = actions.ExecuteSealSource(
		context.Background(), plan, state, tail, serving, proposer,
	); err != nil {
		t.Fatal(err)
	}
	if proposer.calls != 1 || proposer.fence != serving.Fence() {
		t.Fatalf("proposal calls=%d fence=%+v", proposer.calls, proposer.fence)
	}
	transition, err := replicatedstate.OpenOwnershipTransition(proposer.command)
	if err != nil || transition.SourceMember != 1 || transition.TargetMember != 2 ||
		transition.ToOwnedRange != plan.children[plan.retained].Range {
		t.Fatalf("terminal transition=%+v err=%v", transition, err)
	}

	stale := serving
	stale.Command.RouteGeneration--
	if err = actions.ExecuteSealSource(
		context.Background(), plan, state, tail, stale, proposer,
	); !errors.Is(err, ErrTopologyConflict) || proposer.calls != 1 {
		t.Fatalf("stale leader admitted calls=%d err=%v", proposer.calls, err)
	}
	if _, err = source.machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: 3, Term: 2, Type: pb.EntryNormal,
	}, proposer.command); err != nil {
		t.Fatal(err)
	}
	tail, advanced, err = actions.ExecuteCatchUpTail(plan, capture, artifacts, sinks)
	if err != nil || !advanced || !tail.Sealed() || tail.SourceCut().Applied != 3 {
		t.Fatalf("sealed tail=%+v advanced=%t err=%v", tail, advanced, err)
	}
	stage, ok, err := child.Observe(plan, artifacts, 1)
	if err != nil || !ok || stage.Phase() != rangesplit.ChildStageSealed {
		t.Fatalf("sealed child=%+v ok=%t err=%v", stage, ok, err)
	}
	certificate, err := actions.ExecuteCertifyCutover(
		plan, capture, tail, []rangesplit.ChildStageCursor{stage},
	)
	if err != nil || certificate.SourceCut() != tail.SourceCut() {
		t.Fatalf("certificate=%+v err=%v", certificate, err)
	}
	if retried, retryErr := actions.ExecuteCertifyCutover(
		plan, capture, tail, []rangesplit.ChildStageCursor{stage},
	); retryErr != nil || retried != certificate {
		t.Fatalf("certificate retry=%+v err=%v", retried, retryErr)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDurableRuntimeStore(root, plan.OperationID(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, revision, ok, err := reopened.LoadCutoverCertificate(plan.partitioner)
	if err != nil || !ok || revision != 1 || recovered != certificate {
		t.Fatalf("recovered certificate revision=%d ok=%t err=%v", revision, ok, err)
	}
}

type capturingSealProposer struct {
	calls   int
	fence   raftservice.ServingFence
	command []byte
}

func (p *capturingSealProposer) ProposeOwnershipTransition(
	_ context.Context,
	fence raftservice.ServingFence,
	command []byte,
) error {
	p.calls++
	p.fence = fence
	p.command = append(p.command[:0], command...)
	return nil
}

func sourceServingState(
	t testing.TB,
	machine *replicatedstate.Machine,
	state replicatedstate.State,
) raftservice.ServingState {
	t.Helper()
	digest, err := machine.RelationManifestDigest()
	if err != nil {
		t.Fatal(err)
	}
	binding := state.Binding
	return raftservice.ServingState{
		Identity: raftmember.RuntimeIdentity{
			Group: raftmember.GroupKey{
				ClusterID:             [16]byte(binding.ClusterID),
				ClusterIncarnation:    [16]byte(binding.ClusterIncarnation),
				TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
				ShardIncarnation:      [16]byte(binding.ShardIncarnation),
				GroupID:               [16]byte(binding.GroupID),
			},
			Distribution: binding.Distribution, Shard: binding.Shard,
			AllocationGeneration: binding.AllocationGeneration,
			MemberID:             1, StoreID: testID(31), NodeIncarnation: 1,
		},
		Command: raftservice.CommandFence{
			ReplicaSetVersion:      state.ReplicaSetVersion,
			ActivePolicyGeneration: binding.ActivePolicyGeneration,
			ProtectionEpoch:        binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
			SchemaGeneration: binding.SchemaGeneration, RelationManifestDigest: digest,
			RoutingVersion: binding.RoutingVersion, RouteGeneration: binding.RouteGeneration,
		},
		Status: raftmember.RuntimeStatus{
			MemberID: 1, LeaderID: 1, Term: state.LastTerm,
			Commit: state.Applied, Applied: state.Applied, RaftState: raft.StateLeader,
		},
	}
}
