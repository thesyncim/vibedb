package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	pb "go.etcd.io/raft/v3/raftpb"
)

type testReplicaHealthCatalog struct{ snapshot *gateway.Snapshot }

func (catalog testReplicaHealthCatalog) Read(context.Context) (*gateway.Snapshot, error) {
	return catalog.snapshot, nil
}

type testFailureAuthority struct {
	certificates []rebalance.FailureQuorumCertificate
}

func (authority testFailureAuthority) VisitFailureCertificates(
	_ context.Context, _ *gateway.Snapshot, visit func(rebalance.FailureQuorumCertificate) error,
) error {
	for _, certificate := range authority.certificates {
		if err := visit(certificate); err != nil {
			return err
		}
	}
	return nil
}

type testHealthObserver struct{ rejected distribution.ShardID }

func (observer testHealthObserver) ObserveReplicaHealth(
	_ context.Context, _ *gateway.Snapshot, certificate rebalance.FailureQuorumCertificate,
) (gatewayReplicaHealthObservation, error) {
	if certificate.Shard == observer.rejected {
		return gatewayReplicaHealthObservation{}, errors.New("injected observation failure")
	}
	return gatewayReplicaHealthObservation{Publication: raftmodel.Publication{Applied: 1}}, nil
}

type testCandidateInventory struct{}

func (testCandidateInventory) ReplacementCandidates(
	context.Context, *gateway.Snapshot, rebalance.FailureQuorumCertificate,
) ([]rebalance.ReplacementCandidate, error) {
	return []rebalance.ReplacementCandidate{{Member: 9}}, nil
}

type testFailedReplicaSink struct{}

func (testFailedReplicaSink) SubmitFailedReplicaMove(context.Context, rebalance.FailedReplicaMoveIntent) error {
	return nil
}

type captureFailedReplicaSink struct {
	intents []rebalance.FailedReplicaMoveIntent
}

type orderedFailedGrantAuthority struct {
	snapshot *gateway.Snapshot
	grant    membershipgrant.Grant
	events   []string
	pending  bool
}

func (authority *orderedFailedGrantAuthority) Read(context.Context) (*gateway.Snapshot, error) {
	return authority.snapshot, nil
}

func (authority *orderedFailedGrantAuthority) PublishMembershipGrant(
	_ context.Context, grant membershipgrant.Grant,
) error {
	authority.events = append(authority.events, "publish-grant")
	if authority.grant.Valid() && authority.grant != grant {
		return gateway.ErrReplicatedCatalogConflict
	}
	authority.grant = grant
	if authority.pending {
		authority.pending = false
		return gateway.ErrReplicatedCatalogPending
	}
	return nil
}

func (authority *orderedFailedGrantAuthority) ReadMembershipGrant(
	context.Context, raftmember.GroupKey,
) (membershipgrant.Grant, bool, error) {
	authority.events = append(authority.events, "read-grant")
	return authority.grant, authority.grant.Valid(), nil
}

func (authority *orderedFailedGrantAuthority) RetryPending(context.Context) error {
	authority.events = append(authority.events, "settle-grant")
	return nil
}

type orderedFailedGrantInstaller struct {
	authority *orderedFailedGrantAuthority
	nodes     []rafttransport.NodeID
}

func (installer *orderedFailedGrantInstaller) InstallMembershipGrant(
	_ context.Context, node rafttransport.NodeID, grant membershipgrant.Grant,
) error {
	if installer.authority.grant != grant {
		return errors.New("grant was not durable before fanout")
	}
	installer.authority.events = append(installer.authority.events, "install-grant")
	installer.nodes = append(installer.nodes, node)
	if node == (rafttransport.NodeID{1}) {
		return errors.New("certified failed source unavailable")
	}
	return nil
}

type orderedFailedMoveSubmitter struct{ authority *orderedFailedGrantAuthority }

func (submitter orderedFailedMoveSubmitter) Submit(
	_ context.Context, _ *rebalance.Plan,
) (rebalance.Action, error) {
	if !submitter.authority.grant.Valid() {
		return rebalance.Action{}, errors.New("move submitted before grant")
	}
	submitter.authority.events = append(submitter.authority.events, "submit-move")
	return rebalance.Action{Kind: rebalance.ActionAddLearner}, nil
}

func (sink *captureFailedReplicaSink) SubmitFailedReplicaMove(
	_ context.Context, intent rebalance.FailedReplicaMoveIntent,
) error {
	sink.intents = append(sink.intents, intent)
	return nil
}

type testCertifiedHealthAuthority struct{ testHealthRevisionAuthority }

func (authority *testCertifiedHealthAuthority) VisitReplicaFailureCertificates(
	_ context.Context, _ *gateway.Snapshot,
	visit func(gateway.ReplicatedFailureCertificate) error,
) error {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if len(authority.published) == 0 {
		return nil
	}
	revision := authority.published[len(authority.published)-1]
	if revision.Revision < gateway.FailureConfirmationRevisions {
		return nil
	}
	certificate := gateway.ReplicatedFailureCertificate{
		Distribution: revision.Distribution, Shard: revision.Shard, Group: revision.Group,
		CatalogGeneration: revision.CatalogGeneration, ReplicaSetVersion: revision.ReplicaSetVersion,
		LeaderMember: revision.LeaderMember, LeaderTerm: revision.LeaderTerm,
		CommitIndex:       revision.CommitIndex,
		FirstRevision:     revision.Revision - gateway.FailureConfirmationRevisions + 1,
		ConfirmedRevision: revision.Revision, SuspectMember: revision.SuspectMember,
		SuspectNode: revision.SuspectNode, SuspectIncarnation: revision.SuspectIncarnation,
		Confirmations: make([]gateway.ReplicatedFailureConfirmation, len(revision.Attestations)),
	}
	for index, attestation := range revision.Attestations {
		certificate.Confirmations[index] = gateway.ReplicatedFailureConfirmation{
			Member: attestation.Member, FirstRevision: certificate.FirstRevision,
			ConfirmedRevision: certificate.ConfirmedRevision, LeaderTerm: certificate.LeaderTerm,
			ReplicaSetVersion: certificate.ReplicaSetVersion, CommitIndex: certificate.CommitIndex,
		}
	}
	return visit(certificate)
}

type testEndToEndCandidateInventory struct{}

func (testEndToEndCandidateInventory) ReplacementCandidates(
	_ context.Context, _ *gateway.Snapshot, certificate rebalance.FailureQuorumCertificate,
) ([]rebalance.ReplacementCandidate, error) {
	return []rebalance.ReplacementCandidate{{Member: 9, Node: [16]byte{9}, StoreID: [16]byte{19},
		NodeIncarnation: 29, Endpoint: "target",
		TopologyRecoveryEpoch: certificate.Group.TopologyRecoveryEpoch,
		HealthEpoch:           certificate.ConfirmedEpoch}}, nil
}

func TestReplicaHealthRevisionsScheduleExactDurableReplacement(t *testing.T) {
	snapshot, _ := testReplicatedHealthSnapshot(t)
	authority := &testCertifiedHealthAuthority{testHealthRevisionAuthority{
		revisions: make(map[uint64]uint64),
	}}
	revisions, err := newGatewayReplicaHealthRevisionController(
		testReplicaHealthCatalog{snapshot}, testAuthenticatedHealthClient{}, authority,
	)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint64(0); index < gateway.FailureConfirmationRevisions; index++ {
		pass, runErr := revisions.RunPass(context.Background())
		if runErr != nil || pass.Published != 1 {
			t.Fatalf("revision pass=%+v err=%v", pass, runErr)
		}
	}
	sink := new(captureFailedReplicaSink)
	health, err := newGatewayReplicaHealthController(
		testReplicaHealthCatalog{snapshot},
		rebalance.ReplicatedFailureAuthority{Source: authority},
		gatewayAuthenticatedHealthObserver{client: testAuthenticatedHealthClient{}},
		testEndToEndCandidateInventory{}, sink,
	)
	if err != nil {
		t.Fatal(err)
	}
	pass, err := health.RunPass(context.Background())
	if err != nil || pass.Certificates != 1 || pass.Submitted != 1 || len(sink.intents) != 1 {
		t.Fatalf("health pass=%+v intents=%d err=%v", pass, len(sink.intents), err)
	}
	intent := sink.intents[0]
	evidence, placement, ok := intent.Plan.FailedReplicaAuthorizationDigests()
	if !ok || evidence != intent.Evidence || placement != intent.Placement ||
		intent.Plan.RetiringMember() != 1 || intent.Plan.TargetMember() != 9 {
		t.Fatalf("intent is not bound to exact authority: %+v", intent)
	}
	grantAuthority := &orderedFailedGrantAuthority{snapshot: snapshot, pending: true}
	installer := &orderedFailedGrantInstaller{authority: grantAuthority}
	durableSink := gatewayFailedReplicaMoveSink{
		controller: orderedFailedMoveSubmitter{authority: grantAuthority},
		grants:     grantAuthority, installer: installer,
	}
	if err = durableSink.SubmitFailedReplicaMove(t.Context(), intent); err != nil {
		t.Fatalf("durable failed-replica submission: %v", err)
	}
	wantEvents := []string{"publish-grant", "settle-grant", "read-grant",
		"install-grant", "install-grant", "install-grant", "install-grant", "submit-move"}
	if !slices.Equal(grantAuthority.events, wantEvents) || len(installer.nodes) != 4 {
		t.Fatalf("events=%v nodes=%x", grantAuthority.events, installer.nodes)
	}
	firstGrant := grantAuthority.grant
	grantAuthority.events = nil
	installer.nodes = nil
	if err = durableSink.SubmitFailedReplicaMove(t.Context(), intent); err != nil {
		t.Fatalf("restart replay of durable failed-replica submission: %v", err)
	}
	wantReplayEvents := []string{"publish-grant", "read-grant",
		"install-grant", "install-grant", "install-grant", "install-grant", "submit-move"}
	if grantAuthority.grant != firstGrant ||
		!slices.Equal(grantAuthority.events, wantReplayEvents) || len(installer.nodes) != 4 {
		t.Fatalf("replay grant=%+v events=%v nodes=%x",
			grantAuthority.grant, grantAuthority.events, installer.nodes)
	}
}

func TestReplicaHealthControllerStreamsIndependentCertifiedFailures(t *testing.T) {
	snapshot := testReplicaHealthSnapshot(t)
	controller, err := newGatewayReplicaHealthController(
		testReplicaHealthCatalog{snapshot},
		testFailureAuthority{certificates: []rebalance.FailureQuorumCertificate{{Shard: "a"}, {Shard: "b"}}},
		testHealthObserver{rejected: "a"}, testCandidateInventory{}, testFailedReplicaSink{},
	)
	if err != nil {
		t.Fatal(err)
	}
	var scheduled int
	controller.schedule = func(
		_ context.Context, cut rebalance.FailedReplicaPlanningCut, _ rebalance.FailedReplicaMoveSink,
	) (rebalance.FailedReplicaMoveIntent, error) {
		scheduled++
		if cut.Catalog != snapshot || cut.Certificate.Shard != "b" || len(cut.Candidates) != 1 {
			t.Fatalf("wrong detached cut: %+v", cut)
		}
		return rebalance.FailedReplicaMoveIntent{}, nil
	}
	pass, err := controller.RunPass(context.Background())
	if err == nil || pass.Certificates != 2 || pass.Eligible != 1 || pass.Submitted != 1 || scheduled != 1 {
		t.Fatalf("pass=%+v scheduled=%d err=%v", pass, scheduled, err)
	}
}

type oneReplicaHealthPass struct {
	cancel context.CancelFunc
	calls  int
}

func (runner *oneReplicaHealthPass) RunPass(context.Context) (gatewayReplicaHealthPass, error) {
	runner.calls++
	runner.cancel()
	return gatewayReplicaHealthPass{Certificates: 2, Submitted: 1}, nil
}

func TestRunReplicaHealthControllerStartsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &oneReplicaHealthPass{cancel: cancel}
	var logs int
	runReplicaHealthController(ctx, runner, time.Hour, func(string, ...any) { logs++ })
	if runner.calls != 1 || logs != 1 {
		t.Fatalf("calls=%d logs=%d", runner.calls, logs)
	}
}

func TestReplicaControllerLifecycleJoinsBothLoops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	moves := &oneReplicaMovePass{cancel: cancel}
	health := &oneReplicaHealthPass{cancel: cancel}
	revisions := &oneReplicaHealthRevisionPass{cancel: cancel}
	done, err := startGatewayReplicaControllers(
		ctx, revisions, moves, health, time.Hour, func(string, ...any) {},
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("replica controllers did not join after cancellation")
	}
	if revisions.calls != 1 || moves.calls != 1 || health.calls != 1 {
		t.Fatalf("revision calls=%d move calls=%d health calls=%d",
			revisions.calls, moves.calls, health.calls)
	}
}

type oneReplicaHealthRevisionPass struct {
	cancel context.CancelFunc
	calls  int
}

func (runner *oneReplicaHealthRevisionPass) RunPass(
	context.Context,
) (gatewayReplicaHealthRevisionPass, error) {
	runner.calls++
	runner.cancel()
	return gatewayReplicaHealthRevisionPass{Groups: 1, Published: 1}, nil
}

func TestReplicaHealthRuntimeFailsClosedWithoutAuthorityOrTransport(t *testing.T) {
	if controller, err := newGatewayReplicaHealthRuntime(
		testReplicaHealthCatalog{testReplicaHealthSnapshot(t)}, nil, nil,
		testCandidateInventory{}, nil, nil, nil,
	); controller != nil || !errors.Is(err, errGatewayReplicaHealth) {
		t.Fatalf("controller=%v err=%v", controller, err)
	}
}

type testAuthenticatedHealthClient struct{}

func (testAuthenticatedHealthClient) Observe(
	_ context.Context, _ rafttransport.NodeID, request replicacontrol.Request,
) (replicacontrol.Observation, error) {
	if request.TargetMember == 1 {
		return replicacontrol.Observation{}, errors.New("failed member unavailable")
	}
	status := raftmember.RuntimeStatus{MemberID: request.TargetMember, LeaderID: 2,
		Term: 4, Commit: 30, Applied: 30}
	return replicacontrol.Observation{Request: request, Status: status,
		Publication: raftmodel.Publication{Applied: 30, ReplicaSetVersion: 7,
			ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}, nil
}

func TestAuthenticatedHealthObserverRequiresCurrentQuorumAndLeader(t *testing.T) {
	snapshot, certificate := testReplicatedHealthSnapshot(t)
	observer := gatewayAuthenticatedHealthObserver{client: testAuthenticatedHealthClient{}}
	cut, err := observer.ObserveReplicaHealth(context.Background(), snapshot, certificate)
	if err != nil || cut.Leader.MemberID != 2 || len(cut.Healthy) != 2 ||
		cut.Healthy[0].Member != 2 || cut.Healthy[1].Member != 3 {
		t.Fatalf("cut=%+v err=%v", cut, err)
	}
	certificate.LeaderTerm++
	if _, err = observer.ObserveReplicaHealth(context.Background(), snapshot, certificate); err == nil {
		t.Fatal("stale certificate term accepted")
	}
}

func testReplicaHealthSnapshot(t testing.TB) *gateway.Snapshot {
	t.Helper()
	manifest, err := distribution.NewManifest("data", 1, []distribution.Shard{{
		ID: "a", AllocationGeneration: 1,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"endpoint"}, Epoch: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := gateway.NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
		Manifests:     []*distribution.Manifest{manifest},
	}, map[distribution.EndpointID]string{"endpoint": "127.0.0.1:1"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func testReplicatedHealthSnapshot(t testing.TB) (*gateway.Snapshot, rebalance.FailureQuorumCertificate) {
	t.Helper()
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	manifest, err := distribution.NewManifest("data", 7, []distribution.Shard{{
		ID: "all", AllocationGeneration: 11,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"one", "two", "three"}, Epoch: 13,
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoints := map[distribution.EndpointID]string{
		"one": "127.0.0.1:1", "one-native": "127.0.0.1:11", "one-control": "127.0.0.1:21",
		"two": "127.0.0.1:2", "two-native": "127.0.0.1:12", "two-control": "127.0.0.1:22",
		"three": "127.0.0.1:3", "three-native": "127.0.0.1:13", "three-control": "127.0.0.1:23",
		"target": "127.0.0.1:9", "target-native": "127.0.0.1:19", "target-control": "127.0.0.1:29",
	}
	replicas := make([]gateway.ReplicatedReplicaDescriptor, 3)
	for index, name := range []string{"one", "two", "three"} {
		member := uint64(index + 1)
		replicas[index] = gateway.ReplicatedReplicaDescriptor{Member: member,
			Node: [16]byte{byte(member)}, StoreID: [16]byte{byte(member + 10)},
			NodeIncarnation: member + 20, Endpoint: distribution.EndpointID(name),
			NativeEndpoint:  distribution.EndpointID(name + "-native"),
			ControlEndpoint: distribution.EndpointID(name + "-control")}
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
		Manifests:     []*distribution.Manifest{manifest},
	}, endpoints, 9, nil, nil, []gateway.ReplicatedShardDescriptor{{
		Distribution: "data", Shard: "all", Group: group, AllocationGeneration: 11,
		RangeIdentity: [32]byte{2}, LineageDigest: [32]byte{3},
		ForwardingRuleDigest: [32]byte{4},
		Command: raftservice.CommandFence{ReplicaSetVersion: 7, OwnershipEpoch: 13,
			RoutingVersion: 7, RouteGeneration: 9, ActivePolicyGeneration: 1,
			ProtectionEpoch: 1, SchemaGeneration: 1, RelationManifestDigest: [32]byte{1}},
		Replicas: replicas,
		EnrolledTarget: &gateway.ReplicatedReplicaDescriptor{
			Member: 9, Node: [16]byte{9}, StoreID: [16]byte{19}, NodeIncarnation: 29,
			Endpoint: "target", NativeEndpoint: "target-native", ControlEndpoint: "target-control",
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, rebalance.FailureQuorumCertificate{Distribution: "data", Shard: "all",
		Group: group, CatalogGeneration: 9, ReplicaSetVersion: 7, LeaderTerm: 4,
		CommitIndex: 25, FirstFailureEpoch: 10, ConfirmedEpoch: 12, SuspectMember: 1,
		Confirmations: []rebalance.FailureConfirmation{
			{Member: 2, FirstFailureEpoch: 10, ConfirmedEpoch: 12, LeaderTerm: 4, ReplicaSetVersion: 7, CommitIndex: 25},
			{Member: 3, FirstFailureEpoch: 10, ConfirmedEpoch: 12, LeaderTerm: 4, ReplicaSetVersion: 7, CommitIndex: 25},
		}}
}
