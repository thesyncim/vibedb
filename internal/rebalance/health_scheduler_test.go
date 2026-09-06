package rebalance

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestFailedReplicaPlannerRequiresCommittedQuorumAndChoosesDeterministically(t *testing.T) {
	cut := failedReplicaTestCut(t, 8)
	cut.Candidates[0].Load = 4
	cut.Candidates[1].Load = 4
	cut.Candidates[0].Member = 10
	cut.Candidates[1].Member = 9

	planned, err := PlanFailedReplicaReplacement(cut)
	if err != nil {
		t.Fatal(err)
	}
	if planned.Operation == (OperationID{}) || planned.Evidence == ([32]byte{}) ||
		planned.Placement == ([32]byte{}) ||
		planned.Plan == nil || len(planned.Intent) == 0 {
		t.Fatalf("incomplete planned intent: %+v", planned)
	}
	if planned.Plan.RetiringMember() != 1 || planned.Plan.SnapshotSourceMember() != 2 ||
		planned.Plan.TargetMember() != 9 {
		t.Fatalf("members retiring=%d donor=%d target=%d", planned.Plan.RetiringMember(),
			planned.Plan.SnapshotSourceMember(), planned.Plan.TargetMember())
	}
	again, err := PlanFailedReplicaReplacement(cut)
	if err != nil || again.Operation != planned.Operation || again.Evidence != planned.Evidence ||
		again.Placement != planned.Placement ||
		string(again.Intent) != string(planned.Intent) {
		t.Fatalf("retry changed intent: operation=%x/%x evidence=%x/%x err=%v",
			again.Operation, planned.Operation, again.Evidence, planned.Evidence, err)
	}
	for left, right := 0, len(cut.Candidates)-1; left < right; left, right = left+1, right-1 {
		cut.Candidates[left], cut.Candidates[right] = cut.Candidates[right], cut.Candidates[left]
	}
	reordered, err := PlanFailedReplicaReplacement(cut)
	if err != nil || reordered.Operation != planned.Operation ||
		reordered.Placement != planned.Placement {
		t.Fatalf("candidate order changed selection: operation=%x/%x placement=%x/%x err=%v",
			reordered.Operation, planned.Operation, reordered.Placement, planned.Placement, err)
	}
}

func TestFailedReplicaPlannerRejectsPartitionAndStaleEvidence(t *testing.T) {
	base := failedReplicaTestCut(t, 4)
	tests := map[string]func(*FailedReplicaPlanningCut){
		"single reachability epoch": func(cut *FailedReplicaPlanningCut) {
			cut.Certificate.FirstFailureEpoch = cut.Certificate.ConfirmedEpoch
			for index := range cut.Certificate.Confirmations {
				cut.Certificate.Confirmations[index].FirstFailureEpoch = cut.Certificate.ConfirmedEpoch
			}
		},
		"one voter is not quorum": func(cut *FailedReplicaPlanningCut) {
			cut.Certificate.Confirmations = cut.Certificate.Confirmations[:1]
		},
		"leader did not confirm": func(cut *FailedReplicaPlanningCut) {
			cut.Leader.MemberID, cut.Leader.LeaderID = 3, 3
			cut.Certificate.Confirmations[1].Member = 4
		},
		"isolated old leader term": func(cut *FailedReplicaPlanningCut) {
			cut.Leader.Term++
		},
		"uncommitted failure record": func(cut *FailedReplicaPlanningCut) {
			cut.Certificate.CommitIndex = cut.Leader.Commit + 1
			for index := range cut.Certificate.Confirmations {
				cut.Certificate.Confirmations[index].CommitIndex = cut.Certificate.CommitIndex
			}
		},
		"stale replica set": func(cut *FailedReplicaPlanningCut) {
			cut.Certificate.ReplicaSetVersion--
		},
		"duplicate quorum vote": func(cut *FailedReplicaPlanningCut) {
			cut.Certificate.Confirmations[1].Member = cut.Certificate.Confirmations[0].Member
		},
		"suspect vote": func(cut *FailedReplicaPlanningCut) {
			cut.Certificate.Confirmations[0].Member = cut.Certificate.SuspectMember
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cut := cloneFailedReplicaCut(base)
			mutate(&cut)
			if _, err := PlanFailedReplicaReplacement(cut); !errors.Is(err, ErrFailureEvidence) {
				t.Fatalf("error=%v, want ErrFailureEvidence", err)
			}
		})
	}
}

func TestFailedReplicaPlannerRequiresFreshAntiAffineCandidate(t *testing.T) {
	base := failedReplicaTestCut(t, 1)
	tests := map[string]func(*ReplacementCandidate){
		"stale health":       func(candidate *ReplacementCandidate) { candidate.HealthEpoch-- },
		"old topology":       func(candidate *ReplacementCandidate) { candidate.TopologyRecoveryEpoch-- },
		"member collision":   func(candidate *ReplacementCandidate) { candidate.Member = 2 },
		"node collision":     func(candidate *ReplacementCandidate) { candidate.Node = [16]byte{2} },
		"store collision":    func(candidate *ReplacementCandidate) { candidate.StoreID = [16]byte{12} },
		"endpoint collision": func(candidate *ReplacementCandidate) { candidate.Endpoint = "source" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cut := cloneFailedReplicaCut(base)
			mutate(&cut.Candidates[0])
			if _, err := PlanFailedReplicaReplacement(cut); !errors.Is(err, ErrReplacementUnavailable) {
				t.Fatalf("error=%v, want ErrReplacementUnavailable", err)
			}
		})
	}
}

func TestFailedReplicaPlannerRejectsConflictingCurrentCandidateIdentity(t *testing.T) {
	cut := failedReplicaTestCut(t, 2)
	cut.Candidates[1].Load = cut.Candidates[0].Load + 1
	cut.Candidates[1].Member = cut.Candidates[0].Member
	if _, err := PlanFailedReplicaReplacement(cut); !errors.Is(err, ErrReplacementUnavailable) {
		t.Fatalf("error=%v, want ErrReplacementUnavailable", err)
	}
	cut.Candidates[1] = cut.Candidates[0]
	if _, err := PlanFailedReplicaReplacement(cut); err != nil {
		t.Fatalf("exact duplicate observation rejected: %v", err)
	}
}

func TestFailedReplicaPlannerHasNoHiddenCandidateCountCap(t *testing.T) {
	const candidates = 4097
	cut := failedReplicaTestCut(t, candidates)
	for index := range cut.Candidates {
		cut.Candidates[index].Load = uint64(candidates - index)
	}
	planned, err := PlanFailedReplicaReplacement(cut)
	if err != nil {
		t.Fatal(err)
	}
	want := cut.Candidates[candidates-1].Member
	if planned.Plan.TargetMember() != want {
		t.Fatalf("target=%d want=%d", planned.Plan.TargetMember(), want)
	}
}

func TestFailedReplicaSchedulerHandsExactIntentToDurableSink(t *testing.T) {
	cut := failedReplicaTestCut(t, 2)
	sink := new(memoryFailedReplicaSink)
	first, err := ScheduleFailedReplicaReplacement(context.Background(), cut, sink)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ScheduleFailedReplicaReplacement(context.Background(), cut, sink)
	if err != nil || second.Operation != first.Operation || sink.submissions != 2 ||
		sink.record.Operation != first.Operation || sink.record.Evidence != first.Evidence ||
		sink.record.Placement != first.Placement {
		t.Fatalf("retry=%+v submissions=%d record=%+v err=%v", second, sink.submissions, sink.record, err)
	}
	conflict := first
	conflict.Evidence[0] ^= 0xff
	if err = sink.SubmitFailedReplicaMove(context.Background(), conflict); err == nil {
		t.Fatal("durable sink accepted conflicting evidence for one operation")
	}
}

func TestFailedReplicaAuthorizationSurvivesRestartAndRejectsTampering(t *testing.T) {
	cut := failedReplicaTestCut(t, 3)
	planned, err := PlanFailedReplicaReplacement(cut)
	if err != nil {
		t.Fatal(err)
	}
	intent, _, err := openPersistedPlanIntent(planned.Intent)
	if err != nil || len(intent.FailureAuthority) == 0 {
		t.Fatalf("missing persisted failure authority: bytes=%d err=%v",
			len(intent.FailureAuthority), err)
	}
	recovered, err := OpenReplicaMoveIntent(
		planned.Intent, cut.Catalog, cut.Publication, nil,
	)
	if err != nil || recovered.OperationID() != planned.Operation {
		t.Fatalf("restart operation=%x want=%x err=%v",
			recovered.OperationID(), planned.Operation, err)
	}

	intent.FailureAuthority[len(intent.FailureAuthority)-1] ^= 1
	tampered, err := appendPersistedPlanIntent(nil, intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenReplicaMoveIntent(tampered, cut.Catalog, cut.Publication, nil); !errors.Is(err, ErrPlanIntent) && !errors.Is(err, ErrFailureEvidence) {
		t.Fatalf("tampered failure authority accepted: %v", err)
	}
}

type memoryFailedReplicaSink struct {
	mu          sync.Mutex
	record      FailedReplicaMoveIntent
	submissions int
}

func (sink *memoryFailedReplicaSink) SubmitFailedReplicaMove(
	_ context.Context, intent FailedReplicaMoveIntent,
) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.submissions++
	if sink.record.Operation != (OperationID{}) &&
		(sink.record.Operation != intent.Operation || sink.record.Evidence != intent.Evidence ||
			sink.record.Placement != intent.Placement ||
			string(sink.record.Intent) != string(intent.Intent)) {
		return errors.New("conflicting durable intent")
	}
	sink.record = intent
	sink.record.Intent = append([]byte(nil), intent.Intent...)
	return nil
}

func failedReplicaTestCut(t testing.TB, candidateCount int) FailedReplicaPlanningCut {
	t.Helper()
	manifest, err := distribution.NewManifest("data", 7, []distribution.Shard{{
		ID: "all", AllocationGeneration: 11,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"source", "donor", "other"}, Epoch: 13,
	}})
	if err != nil {
		t.Fatal(err)
	}
	group := moveTestGroup()
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
		Placements:    []distribution.TablePlacement{{Table: "docs", Distribution: "data", Columns: []string{"/id"}}},
		Manifests:     []*distribution.Manifest{manifest},
	}
	endpoints := map[distribution.EndpointID]string{
		"source": "127.0.0.1:7001", "source-native": "127.0.0.1:7101", "source-control": "127.0.0.1:7201",
		"donor": "127.0.0.1:7002", "donor-native": "127.0.0.1:7102", "donor-control": "127.0.0.1:7202",
		"other": "127.0.0.1:7003", "other-native": "127.0.0.1:7103", "other-control": "127.0.0.1:7203",
	}
	candidates := make([]ReplacementCandidate, candidateCount)
	for index := range candidates {
		member := uint64(index + 10)
		endpoint := distribution.EndpointID(fmt.Sprintf("candidate-%d", member))
		endpoints[endpoint] = fmt.Sprintf("127.0.1.%d:%d", index%250+1, 8000+index)
		var node rafttransport.NodeID
		var store [16]byte
		binary.LittleEndian.PutUint64(node[:8], member+1000)
		binary.LittleEndian.PutUint64(store[:8], member+2000)
		candidates[index] = ReplacementCandidate{
			Member: member, Node: node, StoreID: store, NodeIncarnation: member + 1,
			Endpoint: endpoint, TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			HealthEpoch: 12, Load: member,
		}
	}
	descriptor := gateway.ReplicatedShardDescriptor{
		Distribution: "data", Shard: "all", Group: group, AllocationGeneration: 11,
		RangeIdentity: [32]byte{0x71}, LineageDigest: [32]byte{0x72},
		ForwardingRuleDigest: [32]byte{0x73},
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 7, ActivePolicyGeneration: 2, ProtectionEpoch: 3,
			OwnershipEpoch: 13, SchemaGeneration: 4, RelationManifestDigest: [32]byte{5},
			RoutingVersion: 7, RouteGeneration: 9,
		},
		Replicas: []gateway.ReplicatedReplicaDescriptor{
			{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{11}, NodeIncarnation: 21, Endpoint: "source", NativeEndpoint: "source-native", ControlEndpoint: "source-control"},
			{Member: 2, Node: [16]byte{2}, StoreID: [16]byte{12}, NodeIncarnation: 22, Endpoint: "donor", NativeEndpoint: "donor-native", ControlEndpoint: "donor-control"},
			{Member: 3, Node: [16]byte{3}, StoreID: [16]byte{13}, NodeIncarnation: 23, Endpoint: "other", NativeEndpoint: "other-native", ControlEndpoint: "other-control"},
		},
	}
	catalog, err := gateway.NewSnapshotWithReplicatedMetadata(
		config, endpoints, 9, nil, nil, []gateway.ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	certificate := FailureQuorumCertificate{
		Distribution: "data", Shard: "all", Group: group,
		CatalogGeneration: 9, ReplicaSetVersion: 7, LeaderTerm: 4, CommitIndex: 25,
		FirstFailureEpoch: 10, ConfirmedEpoch: 12, SuspectMember: 1,
		Confirmations: []FailureConfirmation{
			{Member: 2, FirstFailureEpoch: 10, ConfirmedEpoch: 12, LeaderTerm: 4, ReplicaSetVersion: 7, CommitIndex: 25},
			{Member: 3, FirstFailureEpoch: 10, ConfirmedEpoch: 12, LeaderTerm: 4, ReplicaSetVersion: 7, CommitIndex: 25},
		},
	}
	return FailedReplicaPlanningCut{
		Catalog: catalog,
		Publication: raftmodel.Publication{
			Applied: 30, ReplicaSetVersion: 7, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
		},
		Leader: raftLeaderStatus(2, 4, 30), Certificate: certificate,
		Healthy: []HealthyReplica{
			{Member: 2, LeaderTerm: 4, ReplicaSetVersion: 7, HealthyThrough: 12, Applied: 30, RecentActive: true},
			{Member: 3, LeaderTerm: 4, ReplicaSetVersion: 7, HealthyThrough: 12, Applied: 29, RecentActive: true},
		},
		Candidates: candidates,
	}
}

func raftLeaderStatus(member, term, applied uint64) raftmember.RuntimeStatus {
	return raftmember.RuntimeStatus{
		MemberID: member, LeaderID: member, Term: term, Commit: applied, Applied: applied,
	}
}

func cloneFailedReplicaCut(cut FailedReplicaPlanningCut) FailedReplicaPlanningCut {
	clone := cut
	clone.Certificate.Confirmations = append([]FailureConfirmation(nil), cut.Certificate.Confirmations...)
	clone.Healthy = append([]HealthyReplica(nil), cut.Healthy...)
	clone.Candidates = append([]ReplacementCandidate(nil), cut.Candidates...)
	return clone
}

func failedReplicaEnrolledTestCut(t testing.TB) FailedReplicaPlanningCut {
	t.Helper()
	cut := failedReplicaTestCut(t, 1)
	descriptor := cut.Catalog.ReplicatedShardDescriptors()[0]
	descriptor.LogicalSchemaDigest = [32]byte{99}
	descriptor.RequestLedgerRanges = []gateway.DurableRequestLedgerRangeDescriptor{{Identity: [32]byte{98}}}
	target := cut.Candidates[0]
	descriptor.EnrolledTarget = &gateway.ReplicatedReplicaDescriptor{Member: target.Member, Node: target.Node, StoreID: target.StoreID, NodeIncarnation: target.NodeIncarnation, Endpoint: target.Endpoint, NativeEndpoint: target.Endpoint + "-native", ControlEndpoint: target.Endpoint + "-control"}
	endpoints := make(map[distribution.EndpointID]string)
	for _, replica := range descriptor.Replicas {
		for _, endpoint := range []distribution.EndpointID{replica.Endpoint, replica.NativeEndpoint, replica.ControlEndpoint} {
			address, err := cut.Catalog.Address(endpoint)
			if err != nil {
				t.Fatal(err)
			}
			endpoints[endpoint] = address
		}
	}
	endpoints[target.Endpoint], _ = cut.Catalog.Address(target.Endpoint)
	endpoints[descriptor.EnrolledTarget.NativeEndpoint] = "127.0.1.1:9000"
	endpoints[descriptor.EnrolledTarget.ControlEndpoint] = "127.0.1.1:9001"
	manifest, _ := cut.Catalog.Manifest("data")
	config := distribution.ClusterConfig{Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}}, Placements: []distribution.TablePlacement{{Table: "docs", Distribution: "data", Columns: []string{"/id"}}}, Manifests: []*distribution.Manifest{manifest}}
	var err error
	cut.Catalog, err = gateway.NewSnapshotWithReplicatedMetadata(config, endpoints, 9, nil, nil, []gateway.ReplicatedShardDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	return cut
}

func TestFailedReplicaPlanTransitionUsesAuthorizedOperationIdentity(t *testing.T) {
	cut := failedReplicaEnrolledTestCut(t)
	planned, err := PlanFailedReplicaReplacement(cut)
	if err != nil {
		t.Fatal(err)
	}
	transition, found := planned.Plan.TransitionIntent()
	if !found {
		t.Fatal("enrolled RF3 plan has no durable transition")
	}
	if transition.Key.OperationID != [32]byte(planned.Operation) {
		t.Fatal("failure authorization changed operation without updating transition ownership")
	}
	recovered, err := OpenReplicaMoveIntent(planned.Intent, cut.Catalog, cut.Publication, nil)
	if err != nil {
		t.Fatalf("recover authorized transition: %v", err)
	}
	if recovered.OperationID() != planned.Operation {
		t.Fatal("recovery changed authorized operation")
	}
}
