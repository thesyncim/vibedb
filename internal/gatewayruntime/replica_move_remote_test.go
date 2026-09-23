package gatewayruntime

import (
	"context"
	"encoding/asn1"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync/atomic"
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
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type gatewayTestGrantSource struct{ grant membershipgrant.Grant }

type gatewayCloseTrackingConn struct {
	net.Conn
	closed atomic.Bool
}

func (connection *gatewayCloseTrackingConn) Close() error {
	connection.closed.Store(true)
	return connection.Conn.Close()
}

func (source gatewayTestGrantSource) ReadMembershipGrant(
	context.Context, raftmember.GroupKey,
) (membershipgrant.Grant, bool, error) {
	return source.grant, true, nil
}

type gatewayTestGrantInstaller struct {
	nodes    []rafttransport.NodeID
	failAt   int
	failFrom int
}

func (installer *gatewayTestGrantInstaller) InstallMembershipGrant(
	_ context.Context, node rafttransport.NodeID, _ membershipgrant.Grant,
) error {
	installer.nodes = append(installer.nodes, node)
	position := len(installer.nodes)
	if installer.failAt != 0 && position == installer.failAt ||
		installer.failFrom != 0 && position >= installer.failFrom {
		return errors.New("injected install failure")
	}
	return nil
}

type gatewayTestNodeRecordReader struct{ records []gateway.NodeRecord }

func (reader gatewayTestNodeRecordReader) ListNodes(context.Context) ([]gateway.NodeRecord, error) {
	return reader.records, nil
}

type gatewayTestEnrollmentInstaller struct {
	nodes  []rafttransport.NodeID
	failAt int
}

func (installer *gatewayTestEnrollmentInstaller) EnrollMember(
	_ context.Context, node rafttransport.NodeID, _ rafttransport.EnrollmentIntent,
) (rafttransport.EnrollmentAck, error) {
	installer.nodes = append(installer.nodes, node)
	if installer.failAt != 0 && len(installer.nodes) == installer.failAt {
		return rafttransport.EnrollmentAck{}, errors.New("injected enrollment failure")
	}
	return rafttransport.EnrollmentAck{}, nil
}

type gatewayTestMembershipApplier struct{ calls int }

func (applier *gatewayTestMembershipApplier) ApplyMembership(
	context.Context, gateway.ReplicatedMembershipRoute, shardservice.ReplicatedMembershipRequest,
) (gateway.ReplicatedMembershipResult, error) {
	applier.calls++
	return gateway.ReplicatedMembershipResult{}, nil
}

func TestGatewayGrantedMembershipInstallsEveryPeerBeforeProposal(t *testing.T) {
	grant, route, request := gatewayMembershipFixture()
	installer := new(gatewayTestGrantInstaller)
	applier := new(gatewayTestMembershipApplier)
	nodes := gatewayTestNodeRecordReader{records: []gateway.NodeRecord{
		{NodeID: rafttransport.NodeID(grant.TargetNode), Incarnation: 1, Revision: 1,
			DataAddress: "127.0.0.1:1", Lifecycle: gateway.NodeActive},
	}}
	enroller := new(gatewayTestEnrollmentInstaller)
	client := gatewayGrantedMembershipClient{grants: gatewayTestGrantSource{grant},
		installer: installer, applier: applier, nodes: nodes, enroller: enroller}
	if _, err := client.ApplyMembership(t.Context(), route, request); err != nil {
		t.Fatal(err)
	}
	// AddLearner is voters-only: the enrolled empty target has no group
	// authority yet. The target becomes a known peer via the enrollment
	// fanout checked separately below, and receives the grant on the next
	// membership action after RegisterExecutionGroup.
	want := []rafttransport.NodeID{{1}, {2}, {3}}
	if !slices.Equal(installer.nodes, want) || applier.calls != 1 {
		t.Fatalf("installed=%v apply=%d", installer.nodes, applier.calls)
	}
	wantEnrolled := []rafttransport.NodeID{{1}, {2}, {3}}
	if !slices.Equal(enroller.nodes, wantEnrolled) {
		t.Fatalf("enrolled=%v", enroller.nodes)
	}
	installer.nodes = nil
	installer.failAt = 3
	if _, err := client.ApplyMembership(t.Context(), route, request); err != nil || applier.calls != 2 {
		t.Fatalf("one failed voter err=%v apply=%d", err, applier.calls)
	}
	installer.nodes = nil
	installer.failAt = 0
	installer.failFrom = 2
	if _, err := client.ApplyMembership(t.Context(), route, request); err == nil || applier.calls != 2 {
		t.Fatalf("quorum not reached but proposal still applied err=%v apply=%d", err, applier.calls)
	}
	enroller.nodes = nil
	enroller.failAt = 1
	installer.nodes = nil
	installer.failAt = 0
	installer.failFrom = 0
	if _, err := client.ApplyMembership(t.Context(), route, request); err != nil || applier.calls != 3 {
		t.Fatalf("retiring replica enrollment is optional err=%v apply=%d enrolled=%v", err, applier.calls, enroller.nodes)
	}
	enroller.nodes = nil
	enroller.failAt = 3
	if _, err := client.ApplyMembership(t.Context(), route, request); err == nil || applier.calls != 3 {
		t.Fatalf("snapshot donor enrollment skipped err=%v apply=%d enrolled=%v", err, applier.calls, enroller.nodes)
	}
}

func gatewayMembershipFixture() (membershipgrant.Grant, gateway.ReplicatedMembershipRoute,
	shardservice.ReplicatedMembershipRequest) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	voters := [3]membershipgrant.RosterMember{{Member: 1, Node: [16]byte{1}},
		{Member: 2, Node: [16]byte{2}}, {Member: 3, Node: [16]byte{3}}}
	grant := membershipgrant.Grant{Group: group, TransitionID: [16]byte{6}, MetadataEpoch: 7,
		CatalogGeneration: 8, InitialReplicaSetVersion: 9, InitialVoters: [3]uint64{1, 2, 3},
		InitialRosterDigest:     membershipgrant.CertifiedRosterDigest(group, 9, voters),
		InitialDescriptorDigest: [32]byte{10}, SourceMember: 1, TargetMember: 4,
		TargetNode: [16]byte{4}}
	route := gateway.ReplicatedMembershipRoute{Serving: gateway.ReplicatedRoute{Group: group,
		Replicas: []gateway.ReplicatedEndpoint{{Member: 1, Node: [16]byte{1}},
			{Member: 2, Node: [16]byte{2}}, {Member: 3, Node: [16]byte{3}}}},
		EnrolledTarget: gateway.ReplicatedEndpoint{Member: 4, Node: [16]byte{4}}, HasEnrolledTarget: true}
	request := shardservice.ReplicatedMembershipRequest{Kind: raftservice.MembershipAddLearner,
		TransitionID: grant.TransitionID, MetadataEpoch: grant.MetadataEpoch,
		CatalogGeneration: grant.CatalogGeneration, ExpectedReplicaSetVersion: 9,
		SourceMember: 1, TargetMember: 4}
	return grant, route, request
}

func TestGatewayGrantedMembershipInstallsPublishedTargetBeforeSourceRemoval(t *testing.T) {
	grant, route, request := gatewayMembershipFixture()
	route.Serving.Replicas[0] = route.EnrolledTarget
	route.HasEnrolledTarget = false
	route.EnrolledTarget = gateway.ReplicatedEndpoint{}
	request.Kind = raftservice.MembershipRemoveVoter
	installer := new(gatewayTestGrantInstaller)
	applier := new(gatewayTestMembershipApplier)
	client := gatewayGrantedMembershipClient{grants: gatewayTestGrantSource{grant}, installer: installer, applier: applier}
	if _, err := client.ApplyMembership(t.Context(), route, request); err != nil {
		t.Fatalf("published target lost its membership grant route: %v", err)
	}
	if applier.calls != 1 || !slices.Contains(installer.nodes, rafttransport.NodeID(grant.TargetNode)) {
		t.Fatalf("target not installed before removal: nodes=%v calls=%d", installer.nodes, applier.calls)
	}
}

type gatewayTestObservationClient struct {
	observation replicacontrol.Observation
	request     *replicacontrol.Request
}

type gatewayReplicaMoveObserverAuthority struct {
	catalog *gateway.Snapshot
	grant   membershipgrant.Grant
}

func (authority gatewayReplicaMoveObserverAuthority) Read(context.Context) (*gateway.Snapshot, error) {
	return authority.catalog, nil
}

func (authority gatewayReplicaMoveObserverAuthority) ReadMembershipGrant(
	_ context.Context, group raftmember.GroupKey,
) (membershipgrant.Grant, bool, error) {
	return authority.grant, authority.grant.Group == group, nil
}

func (gatewayReplicaMoveObserverAuthority) ReadGroupPublicationReceipt(
	context.Context, gateway.GroupTransitionKey,
) (gateway.GroupPublicationReceipt, bool, error) {
	return gateway.GroupPublicationReceipt{}, false, nil
}

type gatewayReplicaMoveTargetErrorObservation struct {
	target      rafttransport.NodeID
	err         error
	observation replicacontrol.Observation
}

func (client gatewayReplicaMoveTargetErrorObservation) Observe(
	_ context.Context, node rafttransport.NodeID, request replicacontrol.Request,
) (replicacontrol.Observation, error) {
	if node == client.target {
		return replicacontrol.Observation{}, client.err
	}
	if node != (rafttransport.NodeID{2}) {
		return replicacontrol.Observation{}, errors.New("not leader")
	}
	result := client.observation
	result.Request = request
	return result, nil
}

type gatewayReplicaMoveTestDrainer struct{}

func (gatewayReplicaMoveTestDrainer) CertifyClusterCatalogDrain(
	context.Context, gateway.ClusterCatalogDrainRequest,
) (gateway.ClusterCatalogDrainCertificate, error) {
	return gateway.ClusterCatalogDrainCertificate{}, nil
}

func TestGatewayReplicaMoveObserverPropagatesTargetObservationFailure(t *testing.T) {
	catalog, _, observed := gatewayHotShardMoveFixture(t)
	plan, err := rebalance.PlanReplicaMove(catalog, observed.publication, rebalance.MoveRequest{
		Distribution: "data", Shard: "all", Group: observed.grant.Group,
		RetiringMember: 1, SnapshotSourceMember: 2, TargetMember: 4,
		Source: "one", Target: "target",
	})
	if err != nil {
		t.Fatal(err)
	}
	observed.publication.ConfState.Voters = append(observed.publication.ConfState.Voters, 4)
	targetErr := errors.New("target observation deadline")
	observer := gatewayReplicaMoveObserver{
		authority: gatewayReplicaMoveObserverAuthority{catalog: catalog, grant: observed.grant},
		remote: gatewayReplicaMoveTargetErrorObservation{
			target: rafttransport.NodeID{4}, err: targetErr,
			observation: replicacontrol.Observation{
				Publication: observed.publication,
				Status:      raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 4},
			},
		},
		drainer: gatewayReplicaMoveTestDrainer{},
	}
	record := gateway.ReplicatedOperationRecord{State: gateway.ReplicatedOperationRunning,
		Revision: 4, Cursor: [8]uint64{uint64(rebalance.ActionAdvanceOwnership), 4, 0, 1, 7, 10, 8, 0}}
	_, err = observer.ObserveReplicaMove(t.Context(), plan.OperationID(), record, plan)
	if !errors.Is(err, targetErr) {
		t.Fatalf("target observation failure was hidden: %v", err)
	}
	for _, evidence := range []string{
		"operation=" + fmt.Sprintf("%x", plan.OperationID()),
		"group=" + fmt.Sprintf("%x", observed.grant.Group.GroupID),
		"member=4", "journal_state=2", "journal_revision=4", "journal_cursor=",
		"catalog_route_command=", "leader_command=", "target_binding=unavailable",
	} {
		if !strings.Contains(err.Error(), evidence) {
			t.Fatalf("target diagnostic lacks %q: %v", evidence, err)
		}
	}
}

func TestGatewayReplicaMoveObserverAllowsUnhostedTargetBeforeAddLearner(t *testing.T) {
	catalog, _, observed := gatewayHotShardMoveFixture(t)
	plan, err := rebalance.PlanReplicaMove(catalog, observed.publication, rebalance.MoveRequest{
		Distribution: "data", Shard: "all", Group: observed.grant.Group,
		RetiringMember: 1, SnapshotSourceMember: 2, TargetMember: 4,
		Source: "one", Target: "target",
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := gatewayReplicaMoveObserver{
		authority: gatewayReplicaMoveObserverAuthority{catalog: catalog, grant: observed.grant},
		remote: gatewayReplicaMoveTargetErrorObservation{
			target: rafttransport.NodeID{4}, err: errors.New("empty target has no runtime yet"),
			observation: replicacontrol.Observation{
				Publication: observed.publication,
				Status: raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 4,
					Commit: observed.publication.Applied, Applied: observed.publication.Applied,
					RaftState: raft.StateLeader},
			},
		},
		drainer: gatewayReplicaMoveTestDrainer{},
	}
	cut, err := observer.ObserveReplicaMove(t.Context(), plan.OperationID(), gateway.ReplicatedOperationRecord{}, plan)
	if err != nil {
		t.Fatalf("unhosted target prevented leader-side learner admission: %v", err)
	}
	if cut.Observation.TargetState != (replicatedstate.State{}) || cut.Observation.TargetStatus != (raftmember.RuntimeStatus{}) {
		t.Fatalf("failed target observation invented target state: %+v", cut.Observation)
	}
	action, err := rebalance.Reconcile(plan, cut.Observation)
	if err != nil || action.Kind != rebalance.ActionAddLearner || action.Member != plan.TargetMember() {
		t.Fatalf("unhosted target action=%+v err=%v", action, err)
	}
}

type gatewayReplicaMoveMutableObservation struct {
	target      rafttransport.NodeID
	targetErr   error
	targetCut   replicacontrol.Observation
	targetFound bool
	leader      replicacontrol.Observation
}

func (client *gatewayReplicaMoveMutableObservation) Observe(
	_ context.Context, node rafttransport.NodeID, request replicacontrol.Request,
) (replicacontrol.Observation, error) {
	if node == client.target {
		if client.targetErr != nil {
			return replicacontrol.Observation{}, client.targetErr
		}
		if client.targetFound {
			result := client.targetCut
			result.Request = request
			return result, nil
		}
		return replicacontrol.Observation{}, errors.New("target observation not configured")
	}
	if node != (rafttransport.NodeID{2}) {
		return replicacontrol.Observation{}, errors.New("not leader")
	}
	result := client.leader
	result.Request = request
	return result, nil
}

type gatewayReplicaMoveMemoryJournal struct {
	record gateway.ReplicatedOperationRecord
	found  bool
}

func (journal *gatewayReplicaMoveMemoryJournal) ReadOperation(
	_ context.Context, id [32]byte,
) (gateway.ReplicatedOperationRecord, error) {
	if !journal.found || journal.record.ID != id {
		return gateway.ReplicatedOperationRecord{}, gateway.ErrReplicatedOperationMissing
	}
	return journal.record, nil
}

func (journal *gatewayReplicaMoveMemoryJournal) SubmitOperation(
	_ context.Context, record gateway.ReplicatedOperationRecord,
) error {
	journal.record, journal.found = record, true
	return nil
}

func (journal *gatewayReplicaMoveMemoryJournal) PublishOperation(
	_ context.Context, expected uint64, record gateway.ReplicatedOperationRecord,
) error {
	if !journal.found || journal.record.Revision != expected {
		return errors.New("move journal revision mismatch")
	}
	journal.record = record
	return nil
}

func (journal *gatewayReplicaMoveMemoryJournal) DeleteOperation(
	_ context.Context, id [32]byte, expected uint64,
) error {
	if !journal.found || journal.record.ID != id || journal.record.Revision != expected {
		return errors.New("move journal delete mismatch")
	}
	journal.found = false
	return nil
}

func (*gatewayReplicaMoveMemoryJournal) RetryPending(context.Context) error { return nil }

type gatewayReplicaMoveStepExecutor struct {
	observation *replicacontrol.Observation
	actions     []rebalance.ActionKind
}

func (executor *gatewayReplicaMoveStepExecutor) ExecuteReplicaMove(
	_ context.Context, _ rebalance.OperationID, _ *rebalance.Plan,
	execution rebalance.ReplicatedMoveExecution,
) error {
	executor.actions = append(executor.actions, execution.Action.Kind)
	if execution.Action.Kind == rebalance.ActionAddLearner {
		executor.observation.Publication.ConfState = &pb.ConfState{
			Voters: []uint64{1, 2, 3}, Learners: []uint64{4},
		}
		executor.observation.Publication.Applied++
		executor.observation.Publication.ReplicaSetVersion++
		executor.observation.Status.Applied++
		executor.observation.Status.Commit++
		executor.observation.SnapshotBase = &replicatedstate.SnapshotBaseCertificate{
			Digest: [32]byte{1},
		}
	}
	return nil
}

func gatewayReplicaMoveTestSnapshotCertificate(
	plan *rebalance.Plan, catalog *gateway.Snapshot, publication raftmodel.Publication,
) *replicatedstate.SnapshotBaseCertificate {
	descriptor := catalog.ReplicatedShardDescriptors()[0]
	digest := [32]byte{0x5a}
	state := replicatedstate.State{
		Binding: replicatedstate.Binding{
			ClusterID: plan.Group().ClusterID, ClusterIncarnation: plan.Group().ClusterIncarnation,
			TopologyRecoveryEpoch: plan.Group().TopologyRecoveryEpoch,
			Distribution:          "data", Shard: "all", AllocationGeneration: uint64(descriptor.AllocationGeneration),
			ShardIncarnation: plan.Group().ShardIncarnation, GroupID: plan.Group().GroupID,
			ActivePolicyGeneration: descriptor.Command.ActivePolicyGeneration,
			ProtectionEpoch:        descriptor.Command.ProtectionEpoch,
			OwnershipEpoch:         descriptor.Command.OwnershipEpoch,
			SchemaGeneration:       descriptor.Command.SchemaGeneration,
			RoutingVersion:         uint64(descriptor.Command.RoutingVersion),
			RouteGeneration:        descriptor.Command.RouteGeneration,
			OwnedRange:             distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
		ConfState: proto.Clone(publication.ConfState).(*pb.ConfState),
		Applied:   publication.Applied, ReplicaSetVersion: publication.ReplicaSetVersion,
		LastTerm: 4, SnapshotBaseDigest: digest,
	}
	return &replicatedstate.SnapshotBaseCertificate{
		Manifest: replicatedstate.SnapshotArtifactManifest{State: state}, Digest: digest,
	}
}

func TestGatewayReplicaMoveObserverAdvancesAppliedLearnerToSnapshotBeforeBootstrap(t *testing.T) {
	catalog, _, observed := gatewayHotShardMoveFixture(t)
	plan, err := rebalance.PlanReplicaMove(catalog, observed.publication, rebalance.MoveRequest{
		Distribution: "data", Shard: "all", Group: observed.grant.Group,
		RetiringMember: 1, SnapshotSourceMember: 2, TargetMember: 4,
		Source: "one", Target: "target",
	})
	if err != nil {
		t.Fatal(err)
	}
	leaderObservation := replicacontrol.Observation{
		Publication: observed.publication,
		Status: raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 4,
			Commit: observed.publication.Applied, Applied: observed.publication.Applied,
			RaftState: raft.StateLeader},
	}
	remote := &gatewayReplicaMoveMutableObservation{
		target: rafttransport.NodeID{4}, targetErr: errors.New("pre-bootstrap target EOF"),
		leader: leaderObservation,
	}
	observer := gatewayReplicaMoveObserver{
		authority: gatewayReplicaMoveObserverAuthority{catalog: catalog, grant: observed.grant},
		remote:    remote, drainer: gatewayReplicaMoveTestDrainer{},
	}
	initial, err := rebalance.PrepareReplicatedMoveRecord(t.Context(), plan, observer)
	if err != nil || rebalance.ActionKind(initial.Cursor[0]) != rebalance.ActionAddLearner {
		t.Fatalf("prepared record action=%d err=%v", initial.Cursor[0], err)
	}
	journal := &gatewayReplicaMoveMemoryJournal{record: initial, found: true}
	executor := &gatewayReplicaMoveStepExecutor{observation: &remote.leader}

	first, err := rebalance.ExecuteReplicatedMoveStep(
		t.Context(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || first.Kind != rebalance.ActionAddLearner ||
		journal.record.Cursor[3] != 3 {
		t.Fatalf("AddLearner action=%+v record=%+v err=%v", first, journal.record, err)
	}
	if remote.leader.Publication.ConfState.GetLearners()[0] != plan.TargetMember() {
		t.Fatalf("test executor did not apply learner: %+v", remote.leader.Publication.ConfState)
	}

	cut, err := observer.ObserveReplicaMove(t.Context(), plan.OperationID(), journal.record, nil)
	if err != nil || cut.SnapshotBase != nil ||
		cut.Observation.TargetStatus != (raftmember.RuntimeStatus{}) ||
		cut.Observation.TargetState != (replicatedstate.State{}) {
		t.Fatalf("pre-bootstrap learner cut=%+v err=%v", cut, err)
	}
	action, err := rebalance.Reconcile(plan, cut.Observation)
	if err != nil || action.Kind != rebalance.ActionCreateSnapshotBase {
		t.Fatalf("pre-bootstrap learner reconcile=%+v err=%v", action, err)
	}

	second, err := rebalance.ExecuteReplicatedMoveStep(
		t.Context(), plan.OperationID(), nil, journal, observer, executor,
	)
	if err != nil || second.Kind != rebalance.ActionCreateSnapshotBase ||
		len(executor.actions) != 2 || executor.actions[1] != rebalance.ActionCreateSnapshotBase ||
		rebalance.ActionKind(journal.record.Cursor[0]) != rebalance.ActionCreateSnapshotBase ||
		journal.record.Cursor[3] != 3 {
		t.Fatalf("snapshot action=%+v actions=%v record=%+v err=%v", second, executor.actions, journal.record, err)
	}
}

func TestGatewayReplicaMoveObserverRejectsMissingCertificateForBoundWitness(t *testing.T) {
	catalog, _, observed := gatewayHotShardMoveFixture(t)
	plan, err := rebalance.PlanReplicaMove(catalog, observed.publication, rebalance.MoveRequest{
		Distribution: "data", Shard: "all", Group: observed.grant.Group,
		RetiringMember: 1, SnapshotSourceMember: 2, TargetMember: 4,
		Source: "one", Target: "target",
	})
	if err != nil {
		t.Fatal(err)
	}
	learnerPublication := observed.publication
	learnerPublication.Applied++
	learnerPublication.ReplicaSetVersion++
	learnerPublication.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	certificate := gatewayReplicaMoveTestSnapshotCertificate(plan, catalog, learnerPublication)
	intent, err := rebalance.AppendReplicaMoveIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	boundPlan, err := rebalance.OpenReplicaMoveIntent(intent, catalog, learnerPublication, certificate)
	if err != nil || !boundPlan.SnapshotBaseBound() {
		t.Fatalf("bound plan=%t err=%v", boundPlan != nil && boundPlan.SnapshotBaseBound(), err)
	}
	leaderObservation := replicacontrol.Observation{
		Publication: learnerPublication,
		Status: raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 4,
			Commit: learnerPublication.Applied, Applied: learnerPublication.Applied,
			RaftState: raft.StateLeader},
		SnapshotBase: certificate,
	}
	remote := &gatewayReplicaMoveMutableObservation{
		target: rafttransport.NodeID{4}, targetFound: true,
		leader:    leaderObservation,
		targetCut: replicacontrol.Observation{SnapshotBase: certificate},
	}
	observer := gatewayReplicaMoveObserver{
		authority: gatewayReplicaMoveObserverAuthority{catalog: catalog, grant: observed.grant},
		remote:    remote, drainer: gatewayReplicaMoveTestDrainer{},
	}
	boundRecord, err := rebalance.PrepareReplicatedMoveRecord(t.Context(), boundPlan, observer)
	if err != nil || rebalance.ActionKind(boundRecord.Cursor[0]) != rebalance.ActionAwaitSnapshotInstall {
		t.Fatalf("bound action=%d err=%v", boundRecord.Cursor[0], err)
	}
	journal := &gatewayReplicaMoveMemoryJournal{record: boundRecord, found: true}
	executor := new(gatewayReplicaMoveStepExecutor)
	remote.targetFound = false
	remote.targetErr = errors.New("pre-bootstrap target EOF")
	cut, err := observer.ObserveReplicaMove(t.Context(), plan.OperationID(), journal.record, nil)
	if err != nil || cut.SnapshotBase != nil {
		t.Fatalf("unobserved target certificate was reused: certificate=%v err=%v", cut.SnapshotBase, err)
	}
	_, err = rebalance.ExecuteReplicatedMoveStep(
		t.Context(), plan.OperationID(), nil, journal, observer, executor,
	)
	if !errors.Is(err, rebalance.ErrReplicatedMove) || len(executor.actions) != 0 ||
		!journal.record.Equal(boundRecord) {
		t.Fatalf("missing bound certificate was not fail-closed: actions=%v record_changed=%t err=%v",
			executor.actions, !journal.record.Equal(boundRecord), err)
	}
}

func TestGatewayReplicaMoveObserverFailsClosedOutsideSourceLearnerStage(t *testing.T) {
	for _, name := range []string{"voter", "voter-outgoing", "learner-next", "target-published"} {
		t.Run(name, func(t *testing.T) {
			catalog, _, observed := gatewayHotShardMoveFixture(t)
			plan, err := rebalance.PlanReplicaMove(catalog, observed.publication, rebalance.MoveRequest{
				Distribution: "data", Shard: "all", Group: observed.grant.Group,
				RetiringMember: 1, SnapshotSourceMember: 2, TargetMember: 4,
				Source: "one", Target: "target",
			})
			if err != nil {
				t.Fatal(err)
			}
			leaderPublication := observed.publication
			switch name {
			case "voter":
				leaderPublication.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
			case "voter-outgoing":
				leaderPublication.ConfState = &pb.ConfState{
					Voters: []uint64{1, 2, 3}, VotersOutgoing: []uint64{4}, Learners: []uint64{4},
				}
			case "learner-next":
				leaderPublication.ConfState = &pb.ConfState{
					Voters: []uint64{1, 2, 3}, Learners: []uint64{4}, LearnersNext: []uint64{4},
				}
			case "target-published":
				descriptor := catalog.ReplicatedShardDescriptors()[0]
				target := *descriptor.EnrolledTarget
				command := descriptor.Command
				command.ReplicaSetVersion += 3
				command.OwnershipEpoch++
				command.RoutingVersion++
				command.RouteGeneration++
				catalog, err = gateway.BuildReplicaReplacementTransition(
					catalog, plan.TargetManifest(), catalog.Generation()+1,
					observed.grant, target, command,
				)
				if err != nil {
					t.Fatal(err)
				}
				leaderPublication.Applied = command.ReplicaSetVersion
				leaderPublication.ReplicaSetVersion = command.ReplicaSetVersion
				leaderPublication.ConfState = &pb.ConfState{
					Voters: []uint64{1, 2, 3}, Learners: []uint64{4},
				}
			}
			targetErr := errors.New("target observation unavailable")
			remote := gatewayReplicaMoveTargetErrorObservation{
				target: rafttransport.NodeID{4}, err: targetErr,
				observation: replicacontrol.Observation{
					Publication: leaderPublication,
					Status: raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 4,
						Commit: leaderPublication.Applied, Applied: leaderPublication.Applied,
						RaftState: raft.StateLeader},
				},
			}
			observer := gatewayReplicaMoveObserver{
				authority: gatewayReplicaMoveObserverAuthority{catalog: catalog, grant: observed.grant},
				remote:    remote, drainer: gatewayReplicaMoveTestDrainer{},
			}
			_, err = observer.ObserveReplicaMove(t.Context(), plan.OperationID(), gateway.ReplicatedOperationRecord{}, plan)
			if !errors.Is(err, targetErr) {
				t.Fatalf("target failure was hidden outside source learner stage: %v", err)
			}
		})
	}
}

func TestGatewayReplicaMoveObserverRejectsInvalidLearnerRoster(t *testing.T) {
	catalog, _, observed := gatewayHotShardMoveFixture(t)
	plan, err := rebalance.PlanReplicaMove(catalog, observed.publication, rebalance.MoveRequest{
		Distribution: "data", Shard: "all", Group: observed.grant.Group,
		RetiringMember: 1, SnapshotSourceMember: 2, TargetMember: 4,
		Source: "one", Target: "target",
	})
	if err != nil {
		t.Fatal(err)
	}
	publication := observed.publication
	publication.Applied++
	publication.ReplicaSetVersion++
	publication.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4, 5}}
	remote := gatewayReplicaMoveTargetErrorObservation{
		target: rafttransport.NodeID{4}, err: errors.New("target not hosted"),
		observation: replicacontrol.Observation{
			Publication: publication,
			Status: raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 4,
				Commit: publication.Applied, Applied: publication.Applied, RaftState: raft.StateLeader},
		},
	}
	observer := gatewayReplicaMoveObserver{
		authority: gatewayReplicaMoveObserverAuthority{catalog: catalog, grant: observed.grant},
		remote:    remote, drainer: gatewayReplicaMoveTestDrainer{},
	}
	cut, err := observer.ObserveReplicaMove(t.Context(), plan.OperationID(), gateway.ReplicatedOperationRecord{}, plan)
	if err != nil {
		t.Fatalf("leader-side learner cut failed before roster validation: %v", err)
	}
	if _, err = rebalance.Reconcile(plan, cut.Observation); !errors.Is(err, rebalance.ErrTopologyConflict) {
		t.Fatalf("invalid learner roster reconcile error=%v", err)
	}
}

func (client gatewayTestObservationClient) Observe(
	_ context.Context, _ rafttransport.NodeID, request replicacontrol.Request,
) (replicacontrol.Observation, error) {
	if client.request != nil {
		*client.request = request
	}
	result := client.observation
	result.Request = request
	return result, nil
}

type gatewayTestActionClient struct {
	node    rafttransport.NodeID
	request replicaaction.Request
	err     error
	calls   int
	queued  []error
}

type gatewayTestMembershipLeader struct {
	state shardservice.ReplicatedMemberState
}

func (client gatewayTestMembershipLeader) ObserveMembershipLeader(context.Context, gateway.ReplicatedMembershipRoute) (shardservice.ReplicatedMemberState, error) {
	return client.state, nil
}

func (client *gatewayTestActionClient) Execute(
	_ context.Context, node rafttransport.NodeID, request replicaaction.Request,
) error {
	client.calls++
	if _, err := replicaaction.AppendRequest(nil, request); err != nil {
		return err
	}
	client.node, client.request = node, request
	if len(client.queued) != 0 {
		err := client.queued[0]
		client.queued = client.queued[1:]
		return err
	}
	return client.err
}

func TestGatewayReplicaRemoteActionsBuildExactOwnershipAndRetirementFences(t *testing.T) {
	binding := replicatedstate.Binding{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, Distribution: "data", Shard: "all",
		AllocationGeneration: 5, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{6},
		ActivePolicyGeneration: 7, ProtectionEpoch: 8, OwnershipEpoch: 9,
		SchemaGeneration: 10, RoutingVersion: 11, RouteGeneration: 12,
		OwnedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}
	command, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: binding, ExpectedReplicaSetVersion: 13, SourceMember: 1, TargetMember: 4,
		ToOwnershipEpoch: 10, ToRoutingVersion: 12, ToRouteGeneration: 13,
		ToOwnedRange: binding.OwnedRange})
	if err != nil {
		t.Fatal(err)
	}
	commandFence := raftservice.CommandFence{ReplicaSetVersion: 13,
		ActivePolicyGeneration: 7, ProtectionEpoch: 8, OwnershipEpoch: 9,
		SchemaGeneration: 10, RoutingVersion: 11, RouteGeneration: 12,
		RelationManifestDigest: [32]byte{14}}
	target := gateway.ReplicatedEndpoint{Member: 4, Node: [16]byte{4}, StoreID: [16]byte{15}, NodeIncarnation: 16}
	leader := gateway.ReplicatedEndpoint{Member: 2, Node: [16]byte{2}, StoreID: [16]byte{21}, NodeIncarnation: 22}
	route := gateway.ReplicatedMembershipRoute{Serving: gateway.ReplicatedRoute{
		Group: raftmember.GroupKey{ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
			TopologyRecoveryEpoch: 3, ShardIncarnation: binding.ShardIncarnation, GroupID: binding.GroupID},
		AllocationGeneration: 5, Command: commandFence,
		Replicas: []gateway.ReplicatedEndpoint{
			{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{19}, NodeIncarnation: 20}, leader,
			{Member: 3, Node: [16]byte{3}, StoreID: [16]byte{23}, NodeIncarnation: 24},
		}}, EnrolledTarget: target, HasEnrolledTarget: true}
	actions := new(gatewayTestActionClient)
	remote := gatewayReplicaRemoteActions{observer: gatewayTestObservationClient{observation: replicacontrol.Observation{
		Status: raftmember.RuntimeStatus{MemberID: leader.Member, LeaderID: leader.Member, Term: 17},
	}}, actions: actions, native: gatewayTestMembershipLeader{state: shardservice.ReplicatedMemberState{
		LeaderID: leader.Member, Fence: shardservice.ReplicatedFence{Group: route.Serving.Group,
			AllocationGeneration: route.Serving.AllocationGeneration, Command: commandFence,
			MemberID: leader.Member, StoreID: leader.StoreID, NodeIncarnation: leader.NodeIncarnation + 1, Term: 17}}}}
	operation := rebalance.OperationID{18}
	step := [32]byte{19}
	if err = remote.ProposeReplicaMoveOwnership(t.Context(), operation, step, route, command); err != nil {
		t.Fatal(err)
	}
	if actions.node != leader.Node || actions.request.Kind != replicaaction.OwnershipTransition ||
		actions.request.Fence.Term != 17 || actions.request.SourceMember != 1 ||
		actions.request.TargetMember != 4 || actions.request.Fence.MemberID != leader.Member ||
		actions.request.Fence.StoreID != leader.StoreID ||
		actions.request.Fence.NodeIncarnation != leader.NodeIncarnation+1 {
		t.Fatalf("ownership request=%+v node=%x", actions.request, actions.node)
	}
	staleLeader := &raftservice.NotLeaderError{Status: raftmember.RuntimeStatus{
		MemberID: leader.Member, LeaderID: 3, Term: 18,
	}}
	actions.err = staleLeader
	if err = remote.ProposeReplicaMoveOwnership(
		t.Context(), operation, step, route, command,
	); err != staleLeader {
		t.Fatalf("changed leader err=%v, want exact retryable witness", err)
	}
	actions.err = nil
	source := gateway.ReplicatedEndpoint{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{20}, NodeIncarnation: 21}
	if err = remote.RetireReplicaSource(t.Context(), rebalanceexec.SourceRetirementRequest{
		Operation: [32]byte(operation), Step: step, Group: route.Serving.Group,
		AllocationGeneration: 5, Command: commandFence, Source: source, Target: target, Term: 22,
		Survivors: []gateway.ReplicatedEndpoint{{Member: 4, ControlAddress: "127.0.0.1:14004"}, {Member: 2, ControlAddress: "127.0.0.1:14002"}, {Member: 3, ControlAddress: "127.0.0.1:14003"}},
	}); err != nil {
		t.Fatal(err)
	}
	if actions.node != source.Node || actions.request.Kind != replicaaction.SourceRetirement ||
		actions.request.Fence.Command != commandFence || actions.request.Fence.Term != 22 {
		t.Fatalf("retirement request=%+v node=%x", actions.request, actions.node)
	}
	locators, locatorErr := replicaaction.OpenRetirementLocators(actions.request.Command)
	if locatorErr != nil || len(locators) != 3 || locators[0].Member != 2 || locators[1].Member != 3 || locators[2].Member != 4 || locators[2].Address != "127.0.0.1:14004" {
		t.Fatalf("retirement discovery locators=%+v err=%v", locators, locatorErr)
	}

}

func TestGatewayRetirementRefreshesSourceOnlyAfterUnknownAction(t *testing.T) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	store := [16]byte{9}
	command := raftservice.CommandFence{ReplicaSetVersion: 11, ActivePolicyGeneration: 2,
		ProtectionEpoch: 3, OwnershipEpoch: 4, SchemaGeneration: 5, RoutingVersion: 6,
		RouteGeneration: 7, RelationManifestDigest: [32]byte{8}}
	source := gateway.ReplicatedEndpoint{Member: 1, Node: [16]byte{1}, StoreID: store, NodeIncarnation: 1}
	target := gateway.ReplicatedEndpoint{Member: 4, Node: [16]byte{4}}
	state := replicatedstate.State{Binding: replicatedstate.Binding{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, ShardIncarnation: group.ShardIncarnation,
		GroupID: group.GroupID, AllocationGeneration: 6,
	}}
	observation := replicacontrol.Observation{Status: raftmember.RuntimeStatus{MemberID: source.Member},
		StoreID: store, NodeIncarnation: 2,
		Publication: raftmodel.Publication{ReplicaSetVersion: command.ReplicaSetVersion - 1}, State: state}
	actions := &gatewayTestActionClient{queued: []error{replicaaction.ErrOutcomeUnknown}}
	var observedRequest replicacontrol.Request
	remote := gatewayReplicaRemoteActions{actions: actions,
		observer: gatewayTestObservationClient{observation: observation, request: &observedRequest}}
	request := rebalanceexec.SourceRetirementRequest{Operation: rebalance.OperationID{10}, Step: [32]byte{11},
		Group: group, AllocationGeneration: state.Binding.AllocationGeneration, Command: command,
		Source: source, Target: target, Term: 12}
	if err := remote.RetireReplicaSource(t.Context(), request); err != nil {
		t.Fatalf("newer source incarnation retry: %v", err)
	}
	if actions.calls != 2 || actions.request.Fence.NodeIncarnation != observation.NodeIncarnation {
		t.Fatalf("action calls=%d final fence incarnation=%d", actions.calls, actions.request.Fence.NodeIncarnation)
	}
	if observedRequest.ExpectedReplicaSetVersion != 0 {
		t.Fatalf("identity refresh requested membership version %d; want discovery", observedRequest.ExpectedReplicaSetVersion)
	}

	for name, mutate := range map[string]func(*replicacontrol.Observation){
		"foreign store":     func(candidate *replicacontrol.Observation) { candidate.StoreID[0]++ },
		"older incarnation": func(candidate *replicacontrol.Observation) { candidate.NodeIncarnation = source.NodeIncarnation },
		"wrong member":      func(candidate *replicacontrol.Observation) { candidate.Status.MemberID++ },
		"future publication": func(candidate *replicacontrol.Observation) {
			candidate.Publication.ReplicaSetVersion = command.ReplicaSetVersion + 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := observation
			mutate(&candidate)
			candidateActions := &gatewayTestActionClient{queued: []error{replicaaction.ErrOutcomeUnknown}}
			candidateRemote := gatewayReplicaRemoteActions{actions: candidateActions,
				observer: gatewayTestObservationClient{observation: candidate}}
			if err := candidateRemote.RetireReplicaSource(t.Context(), request); !errors.Is(err, rebalanceexec.ErrExecutionFence) {
				t.Fatalf("mismatched source identity err=%v", err)
			}
			if candidateActions.calls != 1 {
				t.Fatalf("mismatched source retried action calls=%d", candidateActions.calls)
			}
		})
	}

	// A source that already completed retirement may expose only the retired
	// control mux, which has no observation handler. The exact first action
	// attempt must still settle its durable completion.
	completed := &gatewayTestActionClient{}
	if err := (gatewayReplicaRemoteActions{actions: completed}).RetireReplicaSource(t.Context(), request); err != nil {
		t.Fatalf("completed source replay: %v", err)
	}
	if completed.calls != 1 {
		t.Fatalf("completed source replay calls=%d", completed.calls)
	}
}

func TestGatewayShardControlOpenerBoundsAndReleasesAuthenticatedStreams(t *testing.T) {
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	clientNode := rafttransport.NodeID{1}
	serverNode := rafttransport.NodeID{2}
	oid := asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}
	credentials, roots, err := rf3testfixture.WriteCredentials(
		t.TempDir(), oid, domain, []rafttransport.NodeID{clientNode, serverNode},
	)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := servicetls.LoadProfile(
		credentials[0].Certificate, credentials[0].Key, roots, oid.String(), time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, err := servicetls.LoadProfile(
		credentials[1].Certificate, credentials[1].Key, roots, oid.String(), time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	serverConnections := make(chan rafttransport.PeerConnection, 2)
	serverErrors := make(chan error, 2)
	var dials atomic.Int32
	dial := func(ctx context.Context, address string) (net.Conn, error) {
		if address != "authenticated-control" {
			return nil, errors.New("wrong control address")
		}
		dials.Add(1)
		client, server := net.Pipe()
		go func() {
			connection, serveErr := serverTLS.Server(
				ctx, server, rafttransport.TrafficShardControl, deadline,
			)
			if serveErr != nil {
				serverErrors <- serveErr
				return
			}
			serverConnections <- connection
		}()
		return client, nil
	}
	opener, err := newGatewayShardControlOpener(
		clientTLS, deadline, dial, []gateway.ReplicatedEndpoint{{
			Node: serverNode, ControlAddress: "authenticated-control",
		}}, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := opener.OpenShardControl(t.Context(), serverNode)
	if err != nil {
		t.Fatal(err)
	}
	firstServer := <-serverConnections

	blockedCtx, cancelBlocked := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancelBlocked()
	blocked, err := opener.OpenShardControl(blockedCtx, serverNode)
	if blocked != nil || !errors.Is(err, context.DeadlineExceeded) || dials.Load() != 1 {
		t.Fatalf("saturated open connection=%v dials=%d err=%v", blocked, dials.Load(), err)
	}
	_ = firstServer.SetWriteDeadline(time.Now())
	_ = firstServer.Close()
	_ = first.Close()
	// Close is deliberately idempotent for capacity accounting: a duplicate
	// cleanup call must not release a second semaphore slot.
	_ = first.Close()
	second, err := opener.OpenShardControl(t.Context(), serverNode)
	if err != nil || dials.Load() != 2 {
		t.Fatalf("open after release connection=%v dials=%d err=%v", second, dials.Load(), err)
	}
	secondServer := <-serverConnections
	_ = secondServer.SetWriteDeadline(time.Now())
	_ = secondServer.Close()
	_ = second.Close()
	select {
	case serveErr := <-serverErrors:
		t.Fatalf("authenticated server handshake: %v", serveErr)
	default:
	}

	var failed *gatewayCloseTrackingConn
	opener.dial = func(context.Context, string) (net.Conn, error) {
		client, server := net.Pipe()
		failed = &gatewayCloseTrackingConn{Conn: client}
		_ = server.Close()
		return failed, nil
	}
	if connection, err := opener.OpenShardControl(t.Context(), serverNode); err == nil || connection != nil {
		t.Fatalf("failed TLS handshake returned connection=%v err=%v", connection, err)
	}
	if failed == nil || !failed.closed.Load() || len(opener.slots) != 0 {
		t.Fatalf("failed TLS transport retained: connection=%v closed=%t slots=%d",
			failed, failed != nil && failed.closed.Load(), len(opener.slots))
	}
}

func TestGatewaySnapshotBootstrapIgnoresShortRPCDeadline(t *testing.T) {
	parent, stopParent := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stopParent()
	ctx, cancel := gatewaySnapshotBootstrapContext(parent)
	defer cancel()
	time.Sleep(50 * time.Millisecond)
	if err := ctx.Err(); err != nil {
		t.Fatalf("parent RPC deadline cancelled snapshot bootstrap: %v", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) < time.Minute {
		t.Fatalf("snapshot bootstrap deadline=%v remaining=%v ok=%v", deadline, time.Until(deadline), ok)
	}
}

func TestGatewaySnapshotBootstrapStopsOnParentCancel(t *testing.T) {
	parent, stopParent := context.WithCancel(t.Context())
	ctx, cancel := gatewaySnapshotBootstrapContext(parent)
	defer cancel()
	stopParent()
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("cause=%v err=%v", context.Cause(ctx), ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("canceled parent did not stop snapshot bootstrap")
	}
}
