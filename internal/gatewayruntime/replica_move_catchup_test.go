package gatewayruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/rebalanceexec"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type gatewayCatchUpGrantFixture struct {
	cut        rebalanceexec.MoveRoute
	grant      membershipgrant.Grant
	learner    *rafttransport.StaticRegistry
	frame      []byte
	installs   int
	probes     int
	installErr error
}

func (fixture *gatewayCatchUpGrantFixture) ReadMembershipGrant(context.Context, raftmember.GroupKey) (membershipgrant.Grant, bool, error) {
	return fixture.grant, true, nil
}

func (fixture *gatewayCatchUpGrantFixture) ResolveReplicaMove(context.Context, rebalance.OperationID, *rebalance.Plan, rebalance.ReplicatedMoveExecution) (rebalanceexec.MoveRoute, error) {
	return fixture.cut, nil
}

func (fixture *gatewayCatchUpGrantFixture) InstallMembershipGrant(_ context.Context, node rafttransport.NodeID, grant membershipgrant.Grant) error {
	fixture.installs++
	if fixture.installErr != nil {
		return fixture.installErr
	}
	if node != fixture.learner.LocalNode() || grant != fixture.grant {
		return errors.New("grant delivered to the wrong learner")
	}
	return fixture.learner.InstallTransitionGrant(grant)
}

func (fixture *gatewayCatchUpGrantFixture) Observe(_ context.Context, node rafttransport.NodeID, request replicacontrol.Request) (replicacontrol.Observation, error) {
	fixture.probes++
	if node != (rafttransport.NodeID{2}) {
		return replicacontrol.Observation{}, nil
	}
	// Progress depends on the real transport admitting the leader's first
	// append probe, which replays the AddLearner entry in the installed snapshot.
	if _, err := fixture.learner.DecodeInbound(rafttransport.PeerIdentity{
		Node: node, TrustDomain: fixture.learner.TrustDomain(),
	}, fixture.frame); err != nil {
		return replicacontrol.Observation{}, err
	}
	return replicacontrol.Observation{Request: request,
		Status:        raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 4},
		ProgressFound: true, Progress: raftmodel.MemberProgress{Match: 9, RecentActive: true, Learner: true},
	}, nil
}

func TestGatewayReplicaCatchUpRestoresLearnerGrantBeforeConfigurationReplay(t *testing.T) {
	catalog, _, observations := gatewayHotShardMoveFixture(t)
	membership, found := catalog.ResolveReplicatedMembershipRoute("data", "all", make([]gateway.ReplicatedEndpoint, 0, 3))
	if !found {
		t.Fatal("missing membership route")
	}
	retiring := membership.Serving.Replicas[0]
	plan, err := rebalance.PlanReplicaMove(catalog, observations.publication, rebalance.MoveRequest{
		Distribution: "data", Shard: "all", Group: membership.Serving.Group,
		RetiringMember: 1, SnapshotSourceMember: 2, TargetMember: 4, Source: "one", Target: "target",
		RetiringReplica: rebalance.ReplicaIdentity{Member: 1, Node: retiring.Node, StoreID: retiring.StoreID,
			NodeIncarnation: retiring.NodeIncarnation, ControlEndpoint: distribution.EndpointID(retiring.ControlEndpoint)},
	})
	if err != nil {
		t.Fatal(err)
	}
	var members []rafttransport.Member
	for _, endpoint := range gatewayReplicaMoveObservationCandidates(membership) {
		role := rafttransport.MemberVoter
		if endpoint.Member == 4 {
			role = rafttransport.MemberLearner
		}
		members = append(members, rafttransport.Member{Group: plan.Group(), ReplicaSetVersion: 9,
			MemberID: endpoint.Member, Node: endpoint.Node, Role: role})
	}
	openRegistry := func(local rafttransport.NodeID) *rafttransport.StaticRegistry {
		registry, err := rafttransport.NewStaticRegistry(local, members, rafttransport.Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		return registry
	}
	leader, learner := openRegistry(rafttransport.NodeID{2}), openRegistry(rafttransport.NodeID{4})
	if err := leader.InstallTransitionGrant(observations.grant); err != nil {
		t.Fatal(err)
	}
	digest := observations.grant.Digest()
	change, err := proto.MarshalOptions{Deterministic: true}.Marshal(&pb.ConfChange{
		Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: proto.Uint64(4), Context: digest[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, _, err := leader.EncodeOutbound(nil, raftmember.OutboundMessage{Group: plan.Group(), From: 2, To: 4,
		Message: &pb.Message{Type: pb.MsgApp.Enum(), From: proto.Uint64(2), To: proto.Uint64(4),
			Term: proto.Uint64(4), LogTerm: proto.Uint64(4), Index: proto.Uint64(8), Commit: proto.Uint64(9),
			Entries: []*pb.Entry{{Type: pb.EntryConfChange.Enum(), Term: proto.Uint64(4), Index: proto.Uint64(9), Data: change}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &gatewayCatchUpGrantFixture{grant: observations.grant, learner: learner, frame: frame,
		cut: rebalanceexec.MoveRoute{Catalog: catalog, Membership: membership, Target: membership.EnrolledTarget}}
	request := replicacontrol.Request{Group: plan.Group(), TargetMember: 4}
	if _, err := fixture.Observe(t.Context(), rafttransport.NodeID{2}, request); !errors.Is(err, rafttransport.ErrUnauthorized) {
		t.Fatalf("ungranted restored learner accepted configuration replay: %v", err)
	}
	remote := gatewayReplicaRemoteActions{observer: fixture, routes: fixture, grants: fixture, grantInstaller: fixture}
	execution := rebalance.ReplicatedMoveExecution{Action: rebalance.Action{Kind: rebalance.ActionAwaitCatchUp, Member: 4},
		PublicationApplied: 9, PublicationReplicaSet: 9, Proof: [32]byte{1}}
	for range 2 {
		if err := remote.AwaitReplicaMove(t.Context(), plan.OperationID(), plan, execution); err != nil {
			t.Fatalf("restored learner remained stuck before promotion: %v", err)
		}
	}
	if fixture.installs != 2 {
		t.Fatalf("grant reinstall count=%d", fixture.installs)
	}
	fixture.installErr = errors.New("grant delivery failed")
	probes := fixture.probes
	if err := remote.AwaitReplicaMove(t.Context(), plan.OperationID(), plan, execution); !errors.Is(err, fixture.installErr) || fixture.probes != probes {
		t.Fatalf("failed grant delivery allowed catch-up: err=%v probes=%d", err, fixture.probes)
	}
	fixture.installErr = nil
	fixture.grant.CatalogGeneration++
	installs := fixture.installs
	if err := remote.AwaitReplicaMove(t.Context(), plan.OperationID(), plan, execution); !errors.Is(err, errGatewayReplicaControl) || fixture.installs != installs {
		t.Fatalf("foreign grant installed: err=%v installs=%d", err, fixture.installs)
	}
	// Final retirement removes the catalog grant. A published serving target
	// may still need a normal progress poll after a later data append.
	fixture.cut.Membership.Serving.Replicas[0] = fixture.cut.Target
	fixture.cut.Membership.HasEnrolledTarget = false
	fixture.cut.Membership.EnrolledTarget = gateway.ReplicatedEndpoint{}
	fixture.grant = membershipgrant.Grant{}
	if err := remote.AwaitReplicaMove(t.Context(), plan.OperationID(), plan, execution); err != nil || fixture.installs != installs {
		t.Fatalf("published target required retired grant: err=%v installs=%d", err, fixture.installs)
	}
}
