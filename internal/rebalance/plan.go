// Package rebalance implements stateless, evidence-driven replica movement for
// one intact shard allocation. It never treats membership, replication
// progress, leadership, or catalog publication alone as serving authority.
package rebalance

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidPlan       = errors.New("rebalance: invalid replica move plan")
	ErrTopologyConflict  = errors.New("rebalance: topology differs from replica move plan")
	ErrSnapshotBaseBound = errors.New("rebalance: replica move snapshot base is already bound")
)

// MoveRequest names one destination for the existing shard Raft group. The
// target endpoint must already be present in the catalog endpoint directory,
// but it is not routed until the final topology CAS.
type MoveRequest struct {
	Distribution distribution.DistributionName
	Shard        distribution.ShardID
	Group        raftmember.GroupKey
	SourceMember uint64
	TargetMember uint64
	Source       distribution.EndpointID
	Target       distribution.EndpointID
}

// Plan is an immutable replica-movement intent. A plan starts without a bulk
// snapshot base; after the learner is applied, BindSnapshotBase attaches the
// exact verified certificate needed to qualify that learner.
type Plan struct {
	request               MoveRequest
	catalogGeneration     uint64
	nextCatalogGeneration uint64
	sourceManifest        *distribution.Manifest
	targetManifest        *distribution.Manifest
	initialConf           *pb.ConfState
	learnerConf           *pb.ConfState
	voterConf             *pb.ConfState
	removedConf           *pb.ConfState
	baseBound             bool
	baseDigest            [32]byte
	baseState             replicatedstate.State
}

// PlanReplicaMove validates one exact catalog and Raft membership cut. It does
// not mutate either authority and performs no network or storage work.
func PlanReplicaMove(
	current *gateway.Snapshot,
	publication raftmodel.Publication,
	request MoveRequest,
) (*Plan, error) {
	if current == nil || invalidMoveRequest(request) || publication.ConfState == nil ||
		publication.Applied == 0 || publication.ReplicaSetVersion == 0 ||
		publication.ReplicaSetVersion > publication.Applied ||
		current.Generation() == math.MaxUint64 {
		return nil, ErrInvalidPlan
	}
	if err := simpleConfState(publication.ConfState, publication.Applied); err != nil ||
		!memberInSorted(publication.ConfState.GetVoters(), request.SourceMember) {
		return nil, ErrInvalidPlan
	}
	initial := proto.Clone(publication.ConfState).(*pb.ConfState)
	switch {
	case !memberInConf(initial, request.TargetMember):
	case memberInSorted(initial.GetLearners(), request.TargetMember):
		initial.Learners = removeMember(initial.Learners, request.TargetMember)
	default:
		return nil, ErrInvalidPlan
	}
	manifest, ok := current.Manifest(request.Distribution)
	if !ok {
		return nil, ErrInvalidPlan
	}
	if _, err := current.Address(request.Source); err != nil {
		return nil, ErrInvalidPlan
	}
	if _, err := current.Address(request.Target); err != nil {
		return nil, ErrInvalidPlan
	}
	targetManifest, err := targetManifestForMove(manifest, request)
	if err != nil {
		return nil, err
	}
	return newPlan(
		request, current.Generation(), manifest, targetManifest, initial, publication.Applied,
	)
}

func newPlan(
	request MoveRequest,
	catalogGeneration uint64,
	sourceManifest, targetManifest *distribution.Manifest,
	initial *pb.ConfState,
	validationIndex uint64,
) (*Plan, error) {
	if invalidMoveRequest(request) || sourceManifest == nil || targetManifest == nil ||
		initial == nil || catalogGeneration == math.MaxUint64 ||
		targetManifest.Distribution() != sourceManifest.Distribution() ||
		sourceManifest.Version() == ^distribution.RoutingVersion(0) ||
		targetManifest.Version() != sourceManifest.Version()+1 {
		return nil, ErrInvalidPlan
	}
	learner := proto.Clone(initial).(*pb.ConfState)
	learner.Learners = insertMember(learner.Learners, request.TargetMember)
	voter := proto.Clone(learner).(*pb.ConfState)
	voter.Learners = removeMember(voter.Learners, request.TargetMember)
	voter.Voters = insertMember(voter.Voters, request.TargetMember)
	removed := proto.Clone(voter).(*pb.ConfState)
	removed.Voters = removeMember(removed.Voters, request.SourceMember)
	if validationIndex > math.MaxUint64-3 {
		return nil, ErrInvalidPlan
	}
	if err := raftmodel.ValidateConfState(learner, validationIndex+1); err != nil {
		return nil, fmt.Errorf("%w: learner membership: %v", ErrInvalidPlan, err)
	}
	if err := raftmodel.ValidateConfState(voter, validationIndex+2); err != nil {
		return nil, fmt.Errorf("%w: voter membership: %v", ErrInvalidPlan, err)
	}
	if err := raftmodel.ValidateConfState(removed, validationIndex+3); err != nil {
		return nil, fmt.Errorf("%w: final membership: %v", ErrInvalidPlan, err)
	}
	return &Plan{
		request: request, catalogGeneration: catalogGeneration,
		nextCatalogGeneration: catalogGeneration + 1,
		sourceManifest:        sourceManifest, targetManifest: targetManifest,
		initialConf: initial, learnerConf: learner, voterConf: voter, removedConf: removed,
	}, nil
}

// BindSnapshotBase returns a new plan bound to one strictly verified learner
// certificate. The certificate must describe the same shard/group/catalog
// fence and the exact expected learner membership.
func BindSnapshotBase(plan *Plan, snapshot *pb.Snapshot) (*Plan, error) {
	if plan == nil || snapshot == nil {
		return nil, ErrInvalidPlan
	}
	if plan.baseBound {
		return nil, ErrSnapshotBaseBound
	}
	certificate, err := replicatedstate.OpenSnapshotBase(snapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPlan, err)
	}
	return bindCertificate(plan, certificate)
}

func bindCertificate(
	plan *Plan,
	certificate replicatedstate.SnapshotBaseCertificate,
) (*Plan, error) {
	state := certificate.Manifest.State
	binding := state.Binding
	if binding.ClusterID != plan.request.Group.ClusterID ||
		binding.ClusterIncarnation != plan.request.Group.ClusterIncarnation ||
		binding.TopologyRecoveryEpoch != plan.request.Group.TopologyRecoveryEpoch ||
		binding.ShardIncarnation != plan.request.Group.ShardIncarnation ||
		binding.GroupID != plan.request.Group.GroupID ||
		binding.Distribution != string(plan.request.Distribution) ||
		binding.Shard != string(plan.request.Shard) ||
		binding.RouteGeneration != plan.catalogGeneration ||
		binding.RoutingVersion != uint64(plan.sourceManifest.Version()) ||
		!proto.Equal(state.ConfState, plan.learnerConf) ||
		state.ReplicaSetVersion == 0 || state.ReplicaSetVersion > state.Applied ||
		certificate.Digest == ([32]byte{}) {
		return nil, ErrInvalidPlan
	}
	ordinal, ok := exactShard(plan.sourceManifest, plan.request.Shard)
	if !ok {
		return nil, ErrInvalidPlan
	}
	metadata, _ := plan.sourceManifest.ShardMetadataAt(ordinal)
	if binding.AllocationGeneration != uint64(metadata.AllocationGeneration) ||
		binding.OwnershipEpoch != uint64(metadata.Epoch) {
		return nil, ErrInvalidPlan
	}
	next := *plan
	next.baseBound = true
	next.baseDigest = certificate.Digest
	next.baseState = state
	return &next, nil
}

// RecoverReplicaMove reconstructs a bound plan after a controller restart.
// The verified certificate supplies the pre-promotion learner configuration
// and source serving fence. Current may be either the source catalog or the
// exact already-published target catalog; any other topology fails closed.
func RecoverReplicaMove(
	current *gateway.Snapshot,
	publication raftmodel.Publication,
	request MoveRequest,
	snapshot *pb.Snapshot,
) (*Plan, error) {
	if current == nil || snapshot == nil || invalidMoveRequest(request) ||
		publication.ConfState == nil || publication.Applied == 0 ||
		publication.ReplicaSetVersion == 0 ||
		publication.ReplicaSetVersion > publication.Applied {
		return nil, ErrInvalidPlan
	}
	if _, err := current.Address(request.Source); err != nil {
		return nil, ErrInvalidPlan
	}
	if _, err := current.Address(request.Target); err != nil {
		return nil, ErrInvalidPlan
	}
	certificate, err := replicatedstate.OpenSnapshotBase(snapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPlan, err)
	}
	return recoverReplicaMoveCertificate(current, publication, request, certificate)
}

func recoverReplicaMoveCertificate(
	current *gateway.Snapshot,
	publication raftmodel.Publication,
	request MoveRequest,
	certificate replicatedstate.SnapshotBaseCertificate,
) (*Plan, error) {
	if current == nil || invalidMoveRequest(request) || publication.ConfState == nil ||
		publication.Applied == 0 || publication.ReplicaSetVersion == 0 ||
		publication.ReplicaSetVersion > publication.Applied {
		return nil, ErrInvalidPlan
	}
	if _, err := current.Address(request.Source); err != nil {
		return nil, ErrInvalidPlan
	}
	if _, err := current.Address(request.Target); err != nil {
		return nil, ErrInvalidPlan
	}
	state := certificate.Manifest.State
	if simpleConfState(state.ConfState, state.Applied) != nil ||
		state.Binding.RouteGeneration == math.MaxUint64 ||
		state.Binding.RoutingVersion == math.MaxUint64 ||
		state.Binding.OwnershipEpoch == math.MaxUint64 ||
		!memberInSorted(state.ConfState.GetVoters(), request.SourceMember) ||
		!memberInSorted(state.ConfState.GetLearners(), request.TargetMember) ||
		memberInSorted(state.ConfState.GetVoters(), request.TargetMember) {
		return nil, ErrInvalidPlan
	}
	if publication.Applied < state.Applied ||
		publication.ReplicaSetVersion < state.ReplicaSetVersion ||
		simpleConfState(publication.ConfState, publication.Applied) != nil {
		return nil, ErrTopologyConflict
	}
	initial := proto.Clone(state.ConfState).(*pb.ConfState)
	initial.Learners = removeMember(initial.Learners, request.TargetMember)
	manifest, ok := current.Manifest(request.Distribution)
	if !ok {
		return nil, ErrTopologyConflict
	}
	var sourceManifest, targetManifest *distribution.Manifest
	var err error
	switch current.Generation() {
	case state.Binding.RouteGeneration:
		sourceManifest = manifest
		targetManifest, err = targetManifestForMove(sourceManifest, request)
	case state.Binding.RouteGeneration + 1:
		targetManifest = manifest
		sourceManifest, err = sourceManifestForRecovery(targetManifest, request)
		if err == nil {
			var rebuilt *distribution.Manifest
			rebuilt, err = targetManifestForMove(sourceManifest, request)
			if err == nil && !rebuilt.Equal(targetManifest) {
				err = ErrTopologyConflict
			}
		}
	default:
		return nil, ErrTopologyConflict
	}
	if err != nil {
		return nil, err
	}
	plan, err := newPlan(
		request, state.Binding.RouteGeneration, sourceManifest, targetManifest,
		initial, state.Applied,
	)
	if err != nil {
		return nil, err
	}
	plan, err = bindCertificate(plan, certificate)
	if err != nil {
		return nil, err
	}
	catalog, err := plan.catalogStage(current)
	if err != nil {
		return nil, err
	}
	stage, err := plan.membershipStage(publication.ConfState)
	if err != nil || stage == membershipInitial {
		return nil, ErrTopologyConflict
	}
	if (catalog == catalogSource && stage == membershipRemoved) ||
		(catalog == catalogTarget && stage == membershipLearner) {
		return nil, ErrTopologyConflict
	}
	return plan, nil
}

func (p *Plan) Group() raftmember.GroupKey {
	if p == nil {
		return raftmember.GroupKey{}
	}
	return p.request.Group
}

func (p *Plan) CatalogGeneration() uint64 {
	if p == nil {
		return 0
	}
	return p.catalogGeneration
}

func (p *Plan) NextCatalogGeneration() uint64 {
	if p == nil {
		return 0
	}
	return p.nextCatalogGeneration
}

func (p *Plan) SourceMember() uint64 {
	if p == nil {
		return 0
	}
	return p.request.SourceMember
}

func (p *Plan) TargetMember() uint64 {
	if p == nil {
		return 0
	}
	return p.request.TargetMember
}

func (p *Plan) TargetManifest() *distribution.Manifest {
	if p == nil {
		return nil
	}
	return p.targetManifest
}

func (p *Plan) SnapshotBaseBound() bool { return p != nil && p.baseBound }

// CatalogSnapshot builds the exact unpublished cutover catalog after checking
// current still equals the plan's source topology.
func (p *Plan) CatalogSnapshot(current *gateway.Snapshot) (*gateway.Snapshot, error) {
	if p == nil || current == nil || current.Generation() != p.catalogGeneration {
		return nil, ErrTopologyConflict
	}
	manifest, ok := current.Manifest(p.request.Distribution)
	if !ok || !manifest.Equal(p.sourceManifest) {
		return nil, ErrTopologyConflict
	}
	return gateway.BuildManifestTransition(current, p.targetManifest, p.nextCatalogGeneration)
}

// OwnershipCommand constructs the exact ordered state-machine fence to commit
// after target leadership and before catalog publication.
func (p *Plan) OwnershipCommand(replicaSetVersion uint64) ([]byte, error) {
	if p == nil || !p.baseBound || replicaSetVersion == 0 {
		return nil, ErrInvalidPlan
	}
	return replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: p.baseState.Binding, ExpectedReplicaSetVersion: replicaSetVersion,
		SourceMember: p.request.SourceMember, TargetMember: p.request.TargetMember,
		ToOwnershipEpoch:  p.baseState.Binding.OwnershipEpoch + 1,
		ToRoutingVersion:  p.baseState.Binding.RoutingVersion + 1,
		ToRouteGeneration: p.baseState.Binding.RouteGeneration + 1,
	})
}

func invalidGroup(group raftmember.GroupKey) bool {
	return group.ClusterID == ([16]byte{}) || group.ClusterIncarnation == ([16]byte{}) ||
		group.TopologyRecoveryEpoch == 0 || group.ShardIncarnation == ([16]byte{}) ||
		group.GroupID == ([16]byte{})
}

func invalidMoveRequest(request MoveRequest) bool {
	return request.Distribution == "" || request.Shard == "" || invalidGroup(request.Group) ||
		request.SourceMember == request.TargetMember ||
		raft.IsLocalMsgTarget(request.SourceMember) ||
		raft.IsLocalMsgTarget(request.TargetMember) ||
		request.Source == "" || request.Target == "" || request.Source == request.Target
}

func targetManifestForMove(
	sourceManifest *distribution.Manifest,
	request MoveRequest,
) (*distribution.Manifest, error) {
	if sourceManifest == nil || sourceManifest.Distribution() != request.Distribution ||
		sourceManifest.Version() == ^distribution.RoutingVersion(0) {
		return nil, ErrInvalidPlan
	}
	ordinal, ok := exactShard(sourceManifest, request.Shard)
	if !ok {
		return nil, ErrInvalidPlan
	}
	source, _ := sourceManifest.ShardMetadataAt(ordinal)
	if source.Epoch == ^distribution.OwnershipEpoch(0) ||
		!unambiguousManifestMoveLeaders(
			sourceManifest, ordinal, source.LeaderCount, request.Source, request.Target,
		) {
		return nil, ErrInvalidPlan
	}
	target, err := sourceManifest.ReplaceShardLeader(
		ordinal, sourceManifest.Version()+1, 0, request.Target, source.Epoch+1,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPlan, err)
	}
	return target, nil
}

func sourceManifestForRecovery(
	targetManifest *distribution.Manifest,
	request MoveRequest,
) (*distribution.Manifest, error) {
	if targetManifest == nil || targetManifest.Distribution() != request.Distribution ||
		targetManifest.Version() == 0 {
		return nil, ErrTopologyConflict
	}
	ordinal, ok := exactShard(targetManifest, request.Shard)
	if !ok {
		return nil, ErrTopologyConflict
	}
	target, _ := targetManifest.ShardMetadataAt(ordinal)
	if target.Epoch == 0 || !unambiguousManifestMoveLeaders(
		targetManifest, ordinal, target.LeaderCount, request.Target, request.Source,
	) {
		return nil, ErrTopologyConflict
	}
	source, err := targetManifest.ReplaceShardLeader(
		ordinal, targetManifest.Version()-1, 0, request.Source, target.Epoch-1,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTopologyConflict, err)
	}
	return source, nil
}

func unambiguousManifestMoveLeaders(
	manifest *distribution.Manifest,
	shard, count int,
	first, excluded distribution.EndpointID,
) bool {
	if count == 0 {
		return false
	}
	for index := 0; index < count; index++ {
		leader, ok := manifest.ShardLeaderAt(shard, index)
		if !ok || leader == "" || leader == excluded || index == 0 && leader != first {
			return false
		}
		for prior := 0; prior < index; prior++ {
			priorLeader, _ := manifest.ShardLeaderAt(shard, prior)
			if priorLeader == leader {
				return false
			}
		}
	}
	return true
}

func simpleConfState(conf *pb.ConfState, lastIndex uint64) error {
	if conf == nil || len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 ||
		conf.GetAutoLeave() {
		return ErrInvalidPlan
	}
	return raftmodel.ValidateConfState(conf, lastIndex)
}

func memberInConf(conf *pb.ConfState, member uint64) bool {
	return memberInSorted(conf.GetVoters(), member) || memberInSorted(conf.GetLearners(), member) ||
		memberInSorted(conf.GetVotersOutgoing(), member) || memberInSorted(conf.GetLearnersNext(), member)
}

func memberInSorted(members []uint64, member uint64) bool {
	_, found := slices.BinarySearch(members, member)
	return found
}

func insertMember(members []uint64, member uint64) []uint64 {
	position, found := slices.BinarySearch(members, member)
	if found {
		return slices.Clone(members)
	}
	result := make([]uint64, len(members)+1)
	copy(result, members[:position])
	result[position] = member
	copy(result[position+1:], members[position:])
	return result
}

func removeMember(members []uint64, member uint64) []uint64 {
	position, found := slices.BinarySearch(members, member)
	if !found {
		return slices.Clone(members)
	}
	result := make([]uint64, len(members)-1)
	copy(result, members[:position])
	copy(result[position:], members[position+1:])
	return result
}

func exactShard(manifest *distribution.Manifest, id distribution.ShardID) (int, bool) {
	for i := 0; i < manifest.ShardCount(); i++ {
		metadata, _ := manifest.ShardMetadataAt(i)
		if metadata.ID == id {
			return i, true
		}
	}
	return 0, false
}
