package rebalance

import (
	"fmt"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ActionKind identifies the only safe next operation proved by Reconcile.
// Await actions mutate nothing; every mutating action must still pass its
// underlying Raft or exact-generation catalog CAS.
type ActionKind uint8

const (
	ActionAwaitLeader ActionKind = iota + 1
	ActionAddLearner
	ActionCreateSnapshotBase
	ActionAwaitSnapshotInstall
	ActionAwaitCatchUp
	ActionPromoteVoter
	ActionTransferLeader
	ActionAdvanceOwnership
	ActionPublishCatalog
	ActionAwaitCatalogDrain
	ActionRemoveSource
	ActionRefreshCatalogFence
	ActionRetireSource
	ActionComplete
)

func (kind ActionKind) String() string {
	switch kind {
	case ActionAwaitLeader:
		return "await-leader"
	case ActionAddLearner:
		return "add-learner"
	case ActionCreateSnapshotBase:
		return "create-snapshot-base"
	case ActionAwaitSnapshotInstall:
		return "await-snapshot-install"
	case ActionAwaitCatchUp:
		return "await-catch-up"
	case ActionPromoteVoter:
		return "promote-voter"
	case ActionTransferLeader:
		return "transfer-leader"
	case ActionAdvanceOwnership:
		return "advance-ownership"
	case ActionPublishCatalog:
		return "publish-catalog"
	case ActionAwaitCatalogDrain:
		return "await-catalog-drain"
	case ActionRemoveSource:
		return "remove-source"
	case ActionRefreshCatalogFence:
		return "refresh-catalog-fence"
	case ActionRetireSource:
		return "retire-source"
	case ActionComplete:
		return "complete"
	default:
		return fmt.Sprintf("action(%d)", uint8(kind))
	}
}

// Action is a constant-size next-step result. Member is populated for Raft
// membership and leader-transfer actions. CatalogGeneration is populated for
// publication and drain actions.
type Action struct {
	Kind              ActionKind
	Member            uint64
	CatalogGeneration uint64
	ReplicaSetVersion uint64
}

// ConfChange constructs the single-member Raft command for a membership
// action. It returns nil for every non-membership action.
func (action Action) ConfChange() pb.ConfChangeI {
	member := action.Member
	var changeType pb.ConfChangeType
	switch action.Kind {
	case ActionAddLearner:
		changeType = pb.ConfChangeAddLearnerNode
	case ActionPromoteVoter:
		changeType = pb.ConfChangeAddNode
	case ActionRemoveSource:
		changeType = pb.ConfChangeRemoveNode
	default:
		return nil
	}
	return &pb.ConfChange{Type: changeType.Enum(), NodeId: &member}
}

// Observation is one detached controller cut. Publication, LeaderStatus, and
// TargetProgress must come from the current leader; TargetStatus and
// TargetState come from the destination member. DrainedCatalogGeneration names
// the exact generation whose older holders have drained; it cannot be reused
// across the pre-remove and post-remove catalog publications.
type Observation struct {
	Catalog        *gateway.Snapshot
	Publication    raftmodel.Publication
	LeaderStatus   raftmember.RuntimeStatus
	TargetStatus   raftmember.RuntimeStatus
	TargetState    replicatedstate.State
	TargetProgress raftmodel.MemberProgress
	ProgressFound  bool

	DrainedCatalogGeneration uint64
	RetiringReplicaRetired   bool
}

type membershipStage uint8

const (
	membershipInitial membershipStage = iota + 1
	membershipLearner
	membershipVoter
	membershipRemoved
)

type catalogStage uint8

const (
	catalogSource catalogStage = iota + 1
	catalogTargetPreRemove
	catalogTargetPostRemove
)

// Reconcile proves the single safe next operation from current durable
// evidence. It is intentionally stateless: after a controller crash the same
// plan and recovered authorities produce the same step, while unrelated
// membership or catalog changes fail closed as ErrTopologyConflict.
func Reconcile(plan *Plan, observed Observation) (Action, error) {
	if plan == nil || observed.Catalog == nil || observed.Publication.ConfState == nil ||
		observed.Publication.Applied == 0 || observed.Publication.ReplicaSetVersion == 0 ||
		observed.Publication.ReplicaSetVersion > observed.Publication.Applied {
		return Action{}, ErrInvalidPlan
	}
	catalog, err := plan.catalogStage(observed.Catalog)
	if err != nil {
		return Action{}, err
	}
	membership, err := plan.membershipStage(observed.Publication.ConfState)
	if err != nil {
		return Action{}, err
	}
	if !isLeaderObservation(observed.LeaderStatus) {
		return Action{Kind: ActionAwaitLeader}, nil
	}

	if catalog == catalogTargetPreRemove || catalog == catalogTargetPostRemove {
		if !plan.targetBindingApplied(observed.TargetState) ||
			(catalog == catalogTargetPreRemove &&
				membership != membershipVoter && membership != membershipRemoved) ||
			(catalog == catalogTargetPostRemove && membership != membershipRemoved) {
			return Action{}, ErrTopologyConflict
		}
		if catalog == catalogTargetPostRemove &&
			!plan.postRemoveCatalogFence(observed.Catalog, observed.Publication.ReplicaSetVersion) {
			return Action{}, ErrTopologyConflict
		}
		if !targetPublicationApplied(observed) {
			return Action{Kind: ActionAwaitCatchUp, Member: plan.request.TargetMember}, nil
		}
		if observed.LeaderStatus.MemberID != plan.request.TargetMember {
			if !plan.targetCaughtUp(observed, false) {
				return Action{Kind: ActionAwaitCatchUp, Member: plan.request.TargetMember}, nil
			}
			return Action{Kind: ActionTransferLeader, Member: plan.request.TargetMember}, nil
		}
		drainGeneration := plan.nextCatalogGeneration
		if catalog == catalogTargetPostRemove {
			drainGeneration = plan.postRemoveGeneration
		}
		if observed.DrainedCatalogGeneration != drainGeneration {
			return Action{
				Kind: ActionAwaitCatalogDrain, CatalogGeneration: drainGeneration,
			}, nil
		}
		if catalog == catalogTargetPreRemove && membership == membershipVoter {
			return Action{Kind: ActionRemoveSource, Member: plan.request.RetiringMember}, nil
		}
		if catalog == catalogTargetPreRemove {
			return Action{
				Kind: ActionRefreshCatalogFence, CatalogGeneration: plan.postRemoveGeneration,
				ReplicaSetVersion: observed.Publication.ReplicaSetVersion,
			}, nil
		}
		if !observed.RetiringReplicaRetired {
			return Action{Kind: ActionRetireSource, Member: plan.request.RetiringMember}, nil
		}
		return Action{Kind: ActionComplete}, nil
	}

	switch membership {
	case membershipInitial:
		if plan.baseBound {
			return Action{}, ErrTopologyConflict
		}
		return Action{Kind: ActionAddLearner, Member: plan.request.TargetMember}, nil
	case membershipLearner:
		if !plan.baseBound {
			return Action{
				Kind: ActionCreateSnapshotBase, Member: plan.request.SnapshotSourceMember,
			}, nil
		}
		if !plan.snapshotInstalled(observed.TargetState) {
			return Action{Kind: ActionAwaitSnapshotInstall, Member: plan.request.TargetMember}, nil
		}
		if !plan.targetCaughtUp(observed, true) {
			return Action{Kind: ActionAwaitCatchUp, Member: plan.request.TargetMember}, nil
		}
		return Action{Kind: ActionPromoteVoter, Member: plan.request.TargetMember}, nil
	case membershipVoter:
		if !plan.baseBound || !plan.snapshotInstalled(observed.TargetState) {
			return Action{}, ErrTopologyConflict
		}
		if !targetPublicationApplied(observed) {
			return Action{Kind: ActionAwaitCatchUp, Member: plan.request.TargetMember}, nil
		}
		if observed.LeaderStatus.MemberID != plan.request.TargetMember {
			if !plan.targetCaughtUp(observed, false) {
				return Action{Kind: ActionAwaitCatchUp, Member: plan.request.TargetMember}, nil
			}
			return Action{Kind: ActionTransferLeader, Member: plan.request.TargetMember}, nil
		}
		if plan.sourceBindingApplied(observed.TargetState) {
			return Action{Kind: ActionAdvanceOwnership, Member: plan.request.TargetMember}, nil
		}
		if !plan.targetBindingApplied(observed.TargetState) {
			return Action{}, ErrTopologyConflict
		}
		return Action{
			Kind: ActionPublishCatalog, CatalogGeneration: plan.nextCatalogGeneration,
		}, nil
	case membershipRemoved:
		return Action{}, ErrTopologyConflict
	default:
		return Action{}, ErrTopologyConflict
	}
}

func (p *Plan) catalogStage(snapshot *gateway.Snapshot) (catalogStage, error) {
	manifest, ok := snapshot.Manifest(p.request.Distribution)
	if !ok {
		return 0, ErrTopologyConflict
	}
	switch snapshot.Generation() {
	case p.catalogGeneration:
		if !manifest.Equal(p.sourceManifest) {
			return 0, ErrTopologyConflict
		}
		return catalogSource, nil
	case p.nextCatalogGeneration:
		if !manifest.Equal(p.targetManifest) {
			return 0, ErrTopologyConflict
		}
		return catalogTargetPreRemove, nil
	case p.postRemoveGeneration:
		if !manifest.Equal(p.targetManifest) {
			return 0, ErrTopologyConflict
		}
		return catalogTargetPostRemove, nil
	default:
		return 0, ErrTopologyConflict
	}
}

func (p *Plan) postRemoveCatalogFence(snapshot *gateway.Snapshot, replicaSetVersion uint64) bool {
	if p == nil || snapshot == nil || replicaSetVersion == 0 {
		return false
	}
	var workspace [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(
		p.request.Distribution, p.request.Shard, workspace[:0],
	)
	if !ok || route.Group != p.request.Group ||
		route.AllocationGeneration != p.baseState.Binding.AllocationGeneration ||
		route.Command.ReplicaSetVersion != replicaSetVersion ||
		route.Command.ActivePolicyGeneration != p.baseState.Binding.ActivePolicyGeneration ||
		route.Command.ProtectionEpoch != p.baseState.Binding.ProtectionEpoch ||
		route.Command.OwnershipEpoch != p.baseState.Binding.OwnershipEpoch+1 ||
		route.Command.SchemaGeneration != p.baseState.Binding.SchemaGeneration ||
		route.Command.RoutingVersion != p.baseState.Binding.RoutingVersion+1 ||
		route.Command.RouteGeneration != p.baseState.Binding.RouteGeneration+1 ||
		len(route.Replicas) != len(p.removedConf.GetVoters()) {
		return false
	}
	for _, replica := range route.Replicas {
		if replica.Member == p.request.RetiringMember ||
			!memberInSorted(p.removedConf.GetVoters(), replica.Member) {
			return false
		}
	}
	return memberInSorted(p.removedConf.GetVoters(), p.request.SnapshotSourceMember) &&
		memberInSorted(p.removedConf.GetVoters(), p.request.TargetMember)
}

func (p *Plan) membershipStage(conf *pb.ConfState) (membershipStage, error) {
	switch {
	case proto.Equal(conf, p.initialConf):
		return membershipInitial, nil
	case proto.Equal(conf, p.learnerConf):
		return membershipLearner, nil
	case proto.Equal(conf, p.voterConf):
		return membershipVoter, nil
	case proto.Equal(conf, p.removedConf):
		return membershipRemoved, nil
	default:
		return 0, ErrTopologyConflict
	}
}

func isLeaderObservation(status raftmember.RuntimeStatus) bool {
	return status.MemberID != 0 && status.MemberID == status.LeaderID &&
		status.RaftState == raft.StateLeader && status.Term != 0 &&
		status.Applied <= status.Commit
}

func (p *Plan) snapshotInstalled(state replicatedstate.State) bool {
	if !p.baseBound || state.SnapshotBaseDigest != p.baseDigest ||
		state.Applied < p.baseState.Applied || state.LastTerm < p.baseState.LastTerm ||
		!sameImmutableBinding(state.Binding, p.baseState.Binding) {
		return false
	}
	return p.sourceBinding(state.Binding) || p.targetBinding(state.Binding)
}

func (p *Plan) targetCaughtUp(observed Observation, learner bool) bool {
	return observed.ProgressFound && observed.TargetProgress.Learner == learner &&
		observed.TargetProgress.PendingSnapshot == 0 &&
		observed.TargetProgress.RecentActive && !observed.TargetProgress.FlowPaused &&
		observed.TargetProgress.Match >= observed.LeaderStatus.Commit &&
		observed.TargetStatus.MemberID == p.request.TargetMember &&
		observed.TargetStatus.Applied >= observed.LeaderStatus.Commit &&
		targetPublicationApplied(observed)
}

func targetPublicationApplied(observed Observation) bool {
	return observed.TargetState.Applied >= observed.Publication.Applied &&
		observed.TargetState.ReplicaSetVersion == observed.Publication.ReplicaSetVersion &&
		proto.Equal(observed.TargetState.ConfState, observed.Publication.ConfState)
}

func (p *Plan) sourceBindingApplied(state replicatedstate.State) bool {
	return p.snapshotInstalled(state) && p.sourceBinding(state.Binding)
}

func (p *Plan) targetBindingApplied(state replicatedstate.State) bool {
	return p.snapshotInstalled(state) && p.targetBinding(state.Binding)
}

func (p *Plan) sourceBinding(binding replicatedstate.Binding) bool {
	return binding == p.baseState.Binding
}

func (p *Plan) targetBinding(binding replicatedstate.Binding) bool {
	expected := p.baseState.Binding
	expected.OwnershipEpoch++
	expected.RoutingVersion++
	expected.RouteGeneration++
	return binding == expected
}

func sameImmutableBinding(left, right replicatedstate.Binding) bool {
	return left.ClusterID == right.ClusterID &&
		left.ClusterIncarnation == right.ClusterIncarnation &&
		left.TopologyRecoveryEpoch == right.TopologyRecoveryEpoch &&
		left.Distribution == right.Distribution && left.Shard == right.Shard &&
		left.AllocationGeneration == right.AllocationGeneration &&
		left.ShardIncarnation == right.ShardIncarnation && left.GroupID == right.GroupID &&
		left.ActivePolicyGeneration == right.ActivePolicyGeneration &&
		left.ProtectionEpoch == right.ProtectionEpoch &&
		left.SchemaGeneration == right.SchemaGeneration
}
