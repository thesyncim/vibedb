package rebalance

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Recovery retains the original operation provenance while checking the owned
// distribution against the durable transition. Other distributions may have
// advanced the shared head without changing this move's authority.
func recoverOwnedReplicaMove(intent persistedPlanIntent, request MoveRequest, current *gateway.Snapshot, publication raftmodel.Publication, certificate *replicatedstate.SnapshotBaseCertificate) (*Plan, error) {
	transition, err := gateway.OpenGroupTransitionIntent(intent.Transition)
	if err != nil || transition.Key.OperationID != intent.Operation || transition.SourceHeadGeneration != intent.SourceGeneration || current.Generation() < intent.SourceGeneration {
		return nil, errors.Join(err, ErrPlanIntent)
	}
	manifest, found := current.Manifest(request.Distribution)
	if !found {
		return nil, fmt.Errorf("%w: distribution %q is absent", ErrTopologyConflict, request.Distribution)
	}
	var source, target *distribution.Manifest
	switch manifest.Version() {
	case transition.SourceDistributionVersion:
		source = manifest
		target, err = targetManifestForMove(source, request)
		descriptor, found := transitionDescriptor(current, request.Group)
		if !found || gateway.DigestReplicatedShardDescriptor(descriptor) != transition.Key.SourceDescriptorDigest {
			return nil, fmt.Errorf("%w: source group descriptor changed", ErrTopologyConflict)
		}
	case transition.TargetDistributionVersion:
		if certificate == nil {
			return nil, fmt.Errorf("%w: published target has no snapshot certificate", ErrTopologyConflict)
		}
		target = manifest
		source, err = sourceManifestForRecovery(target, request)
	default:
		return nil, fmt.Errorf("%w: distribution version %d differs from source %d and target %d", ErrTopologyConflict, manifest.Version(), transition.SourceDistributionVersion, transition.TargetDistributionVersion)
	}
	if err != nil || source == nil || gateway.DigestRoute(source, request.Shard) != transition.SourceRouteDigest {
		return nil, fmt.Errorf("source route differs from transition: %w", errors.Join(err, ErrTopologyConflict))
	}
	rebuilt, err := targetManifestForMove(source, request)
	if err != nil || !rebuilt.Equal(target) {
		return nil, fmt.Errorf("target manifest differs from transition: %w", errors.Join(err, ErrTopologyConflict))
	}
	if publication.ConfState == nil || publication.Applied == 0 || publication.ReplicaSetVersion == 0 || publication.ReplicaSetVersion > publication.Applied || simpleConfState(publication.ConfState, publication.Applied) != nil {
		return nil, fmt.Errorf("%w: invalid observed membership at applied %d replica set %d", ErrTopologyConflict, publication.Applied, publication.ReplicaSetVersion)
	}
	initial := proto.Clone(publication.ConfState).(*pb.ConfState)
	validationIndex := publication.Applied
	if certificate != nil {
		state := certificate.Manifest.State
		if state.ConfState == nil || simpleConfState(state.ConfState, state.Applied) != nil || !memberInSorted(state.ConfState.GetLearners(), request.TargetMember) {
			return nil, fmt.Errorf("%w: snapshot membership does not certify target learner %d", ErrTopologyConflict, request.TargetMember)
		}
		if publication.Applied < state.Applied || publication.ReplicaSetVersion < state.ReplicaSetVersion {
			return nil, fmt.Errorf("%w: publication applied %d replica set %d precedes snapshot applied %d replica set %d", ErrTopologyConflict, publication.Applied, publication.ReplicaSetVersion, state.Applied, state.ReplicaSetVersion)
		}
		initial = proto.Clone(state.ConfState).(*pb.ConfState)
		validationIndex = state.Applied
	}
	if !memberInSorted(initial.GetVoters(), request.RetiringMember) || !memberInSorted(initial.GetVoters(), request.SnapshotSourceMember) || memberInSorted(initial.GetVoters(), request.TargetMember) {
		return nil, fmt.Errorf("%w: initial membership differs from retiring member %d, donor %d, and learner %d", ErrTopologyConflict, request.RetiringMember, request.SnapshotSourceMember, request.TargetMember)
	}
	initial.Learners = removeMember(initial.Learners, request.TargetMember)
	plan, err := newPlan(request, intent.SourceGeneration, source, target, initial, validationIndex)
	if err != nil {
		return nil, err
	}
	// The caller restores failure authorization and verifies the final operation
	// hash before binding this same persisted transition through its strict API.
	plan.transition, plan.transitionReady = transition, true
	if certificate != nil {
		plan, err = bindCertificate(plan, *certificate)
		if err != nil {
			return nil, err
		}
	}
	if _, err := plan.membershipStage(publication.ConfState); err != nil {
		return nil, err
	}
	return plan, nil
}
