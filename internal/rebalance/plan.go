// Package rebalance implements stateless, evidence-driven replica movement for
// one intact shard allocation. It never treats membership, replication
// progress, leadership, or catalog publication alone as serving authority.
package rebalance

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
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

// MoveRequest names one failed/proactively retired voter, one distinct healthy
// voter that certifies the snapshot, and one replacement learner. Source is
// the routed endpoint replaced by Target at catalog cutover; it need not be the
// endpoint that serves the snapshot.
type MoveRequest struct {
	Distribution         distribution.DistributionName
	Shard                distribution.ShardID
	Group                raftmember.GroupKey
	RetiringMember       uint64
	SnapshotSourceMember uint64
	TargetMember         uint64
	Source               distribution.EndpointID
	Target               distribution.EndpointID
	RetiringReplica      ReplicaIdentity
}

// ReplicaIdentity is the immutable control identity required to address a
// replica after it leaves the serving catalog. Keeping it in the move intent
// prevents a controller restart after either catalog cut from rediscovering a
// stale process that reused the old endpoint or member ID.
type ReplicaIdentity struct {
	Member          uint64
	Node            rafttransport.NodeID
	StoreID         [16]byte
	NodeIncarnation uint64
	ControlEndpoint distribution.EndpointID
}

// Plan is an immutable replica-movement intent. A plan starts without a bulk
// snapshot base; after the learner is applied, BindSnapshotBase attaches the
// exact verified certificate needed to qualify that learner.
type Plan struct {
	request               MoveRequest
	catalogGeneration     uint64
	nextCatalogGeneration uint64
	postRemoveGeneration  uint64
	sourceManifest        *distribution.Manifest
	targetManifest        *distribution.Manifest
	initialConf           *pb.ConfState
	learnerConf           *pb.ConfState
	voterConf             *pb.ConfState
	removedConf           *pb.ConfState
	baseBound             bool
	baseDigest            [32]byte
	baseState             replicatedstate.State
	certificate           replicatedstate.SnapshotBaseCertificate
	failureAuthorization  []byte
	operation             OperationID
	// transition is present when the detached catalog carries the RF3
	// descriptor. Legacy unit fixtures without replicated metadata still use
	// the original plan surface; production admission requires this provenance
	// before a receipt-aware publication can be executed.
	transition      gateway.GroupTransitionIntent
	transitionReady bool
}

// OperationID is the fixed byte-native identity of one exact replica move.
// It is independent of controller process state and of the later snapshot-base
// binding, so every retry and restart retains the same key.
type OperationID [sha256.Size]byte

// PlanReplicaMove validates one exact catalog and Raft membership cut. It does
// not mutate either authority and performs no network or storage work.
func PlanReplicaMove(
	current *gateway.Snapshot,
	publication raftmodel.Publication,
	request MoveRequest,
) (*Plan, error) {
	if current == nil || invalidMoveRequestBase(request) || publication.ConfState == nil ||
		publication.Applied == 0 || publication.ReplicaSetVersion == 0 ||
		publication.ReplicaSetVersion > publication.Applied ||
		current.Generation() > math.MaxUint64-2 {
		return nil, ErrInvalidPlan
	}
	if err := simpleConfState(publication.ConfState, publication.Applied); err != nil ||
		!memberInSorted(publication.ConfState.GetVoters(), request.RetiringMember) ||
		!memberInSorted(publication.ConfState.GetVoters(), request.SnapshotSourceMember) {
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
	var err error
	request, err = bindRetiringReplica(current, request)
	if err != nil {
		return nil, err
	}
	targetManifest, err := targetManifestForMove(manifest, request)
	if err != nil {
		return nil, err
	}
	plan, err := newPlan(
		request, current.Generation(), manifest, targetManifest, initial, publication.Applied,
	)
	if err != nil {
		return nil, err
	}
	plan.installTransitionIntent(current)
	return plan, nil
}

func newPlan(
	request MoveRequest,
	catalogGeneration uint64,
	sourceManifest, targetManifest *distribution.Manifest,
	initial *pb.ConfState,
	validationIndex uint64,
) (*Plan, error) {
	if invalidMoveRequest(request) || sourceManifest == nil || targetManifest == nil ||
		initial == nil || catalogGeneration > math.MaxUint64-2 ||
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
	removed.Voters = removeMember(removed.Voters, request.RetiringMember)
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
	plan := &Plan{
		request: request, catalogGeneration: catalogGeneration,
		nextCatalogGeneration: catalogGeneration + 1,
		postRemoveGeneration:  catalogGeneration + 2,
		sourceManifest:        sourceManifest, targetManifest: targetManifest,
		initialConf: initial, learnerConf: learner, voterConf: voter, removedConf: removed,
	}
	plan.operation = replicaMoveOperationID(plan)
	if plan.operation == (OperationID{}) {
		return nil, ErrInvalidPlan
	}
	return plan, nil
}

// installTransitionIntent snapshots the full source provenance while the
// caller still has the exact detached catalog cut. It is intentionally best
// effort for old in-memory fixtures that predate replicated shard metadata;
// the receipt-aware executor rejects such plans before publication.
func (p *Plan) installTransitionIntent(current *gateway.Snapshot) {
	if p == nil || current == nil || p.operation == (OperationID{}) {
		return
	}
	var descriptor gateway.ReplicatedShardDescriptor
	found := false
	for _, candidate := range current.ReplicatedShardDescriptors() {
		if candidate.Group == p.request.Group && candidate.Distribution == p.request.Distribution && candidate.Shard == p.request.Shard {
			descriptor = candidate
			found = true
			break
		}
	}
	if !found {
		return
	}
	ordinal, ok := exactShard(p.sourceManifest, p.request.Shard)
	if !ok {
		return
	}
	metadata, ok := p.sourceManifest.ShardMetadataAt(ordinal)
	if !ok {
		return
	}
	route := make([]distribution.EndpointID, metadata.LeaderCount)
	for index := range route {
		leader, found := p.sourceManifest.ShardLeaderAt(ordinal, index)
		if !found {
			return
		}
		route[index] = leader
	}
	var replacement gateway.ReplicatedReplicaDescriptor
	for _, candidate := range current.ReplicatedShardDescriptors() {
		if candidate.Group != p.request.Group {
			continue
		}
		if candidate.EnrolledTarget != nil &&
			(candidate.EnrolledTarget.Endpoint == p.request.Target ||
				candidate.EnrolledTarget.NativeEndpoint == p.request.Target ||
				candidate.EnrolledTarget.ControlEndpoint == p.request.Target) {
			replacement = *candidate.EnrolledTarget
			break
		}
	}
	if replacement.Member == 0 {
		// Some callers carry the target identity in the cold endpoint directory
		// before enrollment. Keep the intent unavailable until the authority has
		// supplied a complete authenticated target rather than fabricating it.
		return
	}
	headDigest, err := gateway.CatalogSnapshotDigest(current)
	if err != nil {
		return
	}
	groupDigest := gateway.DigestReplicatedShardDescriptor(descriptor)
	rosterDigest := gateway.DigestReplicaRoster(descriptor.Replicas)
	routeDigest := gateway.DigestRoute(p.sourceManifest, p.request.Shard)
	commandDigest := gateway.DigestCommandFence(descriptor.Command)
	intent := gateway.GroupTransitionIntent{
		Key: gateway.GroupTransitionKey{
			OperationID:                [32]byte(p.operation),
			Distribution:               p.request.Distribution,
			Shard:                      p.request.Shard,
			Group:                      p.request.Group,
			SourceAllocationGeneration: uint64(metadata.AllocationGeneration),
			SourceDescriptorDigest:     groupDigest,
			SourceCommandFenceDigest:   commandDigest,
		},
		SourceMember: p.request.RetiringMember, TargetMember: p.request.TargetMember,
		SourceHeadGeneration: current.Generation(), SourceHeadDigest: headDigest,
		SourceDistributionVersion: p.sourceManifest.Version(), SourceGroupDigest: groupDigest,
		SourceRosterDigest: rosterDigest, SourceRouteDigest: routeDigest,
		SourceCommandFenceDigest: commandDigest, SourceDescriptor: descriptor,
		SourceRoute: route, Replacement: replacement,
		TargetDistributionVersion: p.targetManifest.Version(),
	}
	if !intent.Valid() {
		return
	}
	p.transition = intent
	p.transitionReady = true
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
	sourceRouteGeneration := plan.catalogGeneration
	if plan.transitionReady {
		sourceRouteGeneration = plan.transition.SourceDescriptor.Command.RouteGeneration
	}
	if binding.ClusterID != plan.request.Group.ClusterID ||
		binding.ClusterIncarnation != plan.request.Group.ClusterIncarnation ||
		binding.TopologyRecoveryEpoch != plan.request.Group.TopologyRecoveryEpoch ||
		binding.ShardIncarnation != plan.request.Group.ShardIncarnation ||
		binding.GroupID != plan.request.Group.GroupID ||
		binding.Distribution != string(plan.request.Distribution) ||
		binding.Shard != string(plan.request.Shard) ||
		binding.RouteGeneration != sourceRouteGeneration ||
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
	next.certificate = certificate
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
		state.Binding.RouteGeneration > math.MaxUint64-2 ||
		state.Binding.RoutingVersion == math.MaxUint64 ||
		state.Binding.OwnershipEpoch == math.MaxUint64 ||
		!memberInSorted(state.ConfState.GetVoters(), request.RetiringMember) ||
		!memberInSorted(state.ConfState.GetVoters(), request.SnapshotSourceMember) ||
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
	case state.Binding.RouteGeneration + 1, state.Binding.RouteGeneration + 2:
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
		((catalog == catalogTargetPreRemove || catalog == catalogTargetPostRemove) &&
			stage == membershipLearner) ||
		catalog == catalogTargetPostRemove && stage != membershipRemoved {
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

// OperationID returns the stable idempotency identity for this exact move.
func (p *Plan) OperationID() OperationID {
	if p == nil {
		return OperationID{}
	}
	return p.operation
}

// TransitionIntent returns the immutable source provenance required by a
// receipt-aware catalog authority. The boolean is false for legacy detached
// fixtures that do not carry a replicated shard descriptor.
func (p *Plan) TransitionIntent() (gateway.GroupTransitionIntent, bool) {
	if p == nil || !p.transitionReady {
		return gateway.GroupTransitionIntent{}, false
	}
	intent := p.transition
	intent.SourceDescriptor.Replicas = slices.Clone(intent.SourceDescriptor.Replicas)
	intent.SourceDescriptor.RequestLedgerRanges = slices.Clone(intent.SourceDescriptor.RequestLedgerRanges)
	if intent.SourceDescriptor.EnrolledTarget != nil {
		target := *intent.SourceDescriptor.EnrolledTarget
		intent.SourceDescriptor.EnrolledTarget = &target
	}
	intent.SourceRoute = slices.Clone(intent.SourceRoute)
	return intent, true
}

// TransitionKey returns the durable per-distribution ownership identity.
func (p *Plan) TransitionKey() (gateway.GroupTransitionKey, bool) {
	intent, ok := p.TransitionIntent()
	if !ok {
		return gateway.GroupTransitionKey{}, false
	}
	return intent.Key, true
}

func (p *Plan) bindTransitionIntent(intent gateway.GroupTransitionIntent) error {
	if p == nil || !intent.Valid() || intent.Key.OperationID != [32]byte(p.operation) ||
		intent.Key.Distribution != p.request.Distribution || intent.Key.Shard != p.request.Shard ||
		intent.Key.Group != p.request.Group || intent.SourceMember != p.request.RetiringMember ||
		intent.TargetMember != p.request.TargetMember ||
		intent.SourceDistributionVersion != p.sourceManifest.Version() ||
		intent.TargetDistributionVersion != p.targetManifest.Version() {
		return ErrPlanIntent
	}
	p.transition = intent
	p.transitionReady = true
	return nil
}

// TransitionRequired reports whether a real replicated catalog requires the
// durable source/receipt contract. Metadata-free unit fixtures are the only
// supported legacy shape; a production RF3 snapshot must never silently use
// guessed global successor generations.
func (p *Plan) TransitionRequired(catalog *gateway.Snapshot) bool {
	// A plan becomes receipt-capable only after the target's authenticated
	// enrollment descriptor is present. Before that point the request is an
	// admission candidate, not an executable placement. Keeping the legacy
	// metadata-free shape here lets the existing failure scheduler persist its
	// candidate; OpenReplicaMoveIntent fails closed if execution later observes
	// enrollment without a matching durable transition intent.
	return p != nil && catalog != nil && p.transitionReady && catalog.ReplicatedRouteCount() != 0
}

// CatalogSnapshotAtHead applies the owned endpoint delta to the actual
// catalog head. It is the receipt-aware replacement for CatalogSnapshot;
// unrelated catalog generations are preserved and the gateway builder changes
// only this group's shard.
func (p *Plan) CatalogSnapshotAtHead(
	current *gateway.Snapshot,
	phase gateway.TransitionPhase,
	replacement gateway.ReplicatedReplicaDescriptor,
	command raftservice.CommandFence,
) (*gateway.Snapshot, error) {
	if p == nil || !p.transitionReady || current == nil {
		return nil, ErrTopologyConflict
	}
	intent := p.transition
	return gateway.BuildGroupOwnedShardTransition(current, intent, phase, replacement, command)
}

func replicaMoveOperationID(plan *Plan) OperationID {
	if plan == nil || plan.sourceManifest == nil || plan.initialConf == nil {
		return OperationID{}
	}
	ordinal, ok := exactShard(plan.sourceManifest, plan.request.Shard)
	if !ok {
		return OperationID{}
	}
	shard, ok := plan.sourceManifest.ShardInfo(ordinal)
	if !ok {
		return OperationID{}
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/rebalance/replica-move-operation\x00"))
	writeOperationString(hash, string(plan.request.Distribution))
	writeOperationString(hash, string(plan.request.Shard))
	for _, id := range [...][16]byte{
		plan.request.Group.ClusterID, plan.request.Group.ClusterIncarnation,
		plan.request.Group.ShardIncarnation, plan.request.Group.GroupID,
	} {
		_, _ = hash.Write(id[:])
	}
	for _, value := range [...]uint64{
		plan.request.Group.TopologyRecoveryEpoch, plan.request.RetiringMember,
		plan.request.SnapshotSourceMember, plan.request.TargetMember,
		plan.request.RetiringReplica.Member, plan.request.RetiringReplica.NodeIncarnation,
		plan.catalogGeneration, plan.nextCatalogGeneration, plan.postRemoveGeneration,
		uint64(plan.sourceManifest.Version()), uint64(shard.AllocationGeneration),
		uint64(shard.Epoch),
	} {
		writeOperationUint64(hash, value)
	}
	_, _ = hash.Write(plan.request.RetiringReplica.Node[:])
	_, _ = hash.Write(plan.request.RetiringReplica.StoreID[:])
	writeOperationString(hash, string(plan.request.RetiringReplica.ControlEndpoint))
	writeOperationString(hash, string(plan.request.Source))
	writeOperationString(hash, string(plan.request.Target))
	_, _ = hash.Write(shard.Range.Start[:])
	if shard.Range.End.Max {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	_, _ = hash.Write(shard.Range.End.Point[:])
	writeOperationUint64(hash, uint64(len(shard.Leaders)))
	for _, leader := range shard.Leaders {
		writeOperationString(hash, string(leader))
	}
	writeOperationMembers(hash, plan.initialConf.GetVoters())
	writeOperationMembers(hash, plan.initialConf.GetLearners())
	if len(plan.failureAuthorization) == 0 {
		_, _ = hash.Write([]byte{0})
	} else {
		_, _ = hash.Write([]byte{1})
		writeOperationUint64(hash, uint64(len(plan.failureAuthorization)))
		_, _ = hash.Write(plan.failureAuthorization)
	}
	var result OperationID
	_ = hash.Sum(result[:0])
	return result
}

type operationHash interface{ Write([]byte) (int, error) }

func writeOperationUint64(hash operationHash, value uint64) {
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], value)
	_, _ = hash.Write(raw[:])
}

func writeOperationString(hash operationHash, value string) {
	writeOperationUint64(hash, uint64(len(value)))
	_, _ = hash.Write([]byte(value))
}

func writeOperationMembers(hash operationHash, members []uint64) {
	writeOperationUint64(hash, uint64(len(members)))
	for _, member := range members {
		writeOperationUint64(hash, member)
	}
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

func (p *Plan) PostRemoveCatalogGeneration() uint64 {
	if p == nil {
		return 0
	}
	return p.postRemoveGeneration
}

func (p *Plan) RetiringMember() uint64 {
	if p == nil {
		return 0
	}
	return p.request.RetiringMember
}

// RetiringReplica returns the exact durable control identity captured before
// catalog cutover. The returned value owns no mutable backing storage.
func (p *Plan) RetiringReplica() ReplicaIdentity {
	if p == nil {
		return ReplicaIdentity{}
	}
	return p.request.RetiringReplica
}

// Request returns the immutable placement identity for controller-side route
// resolution. Every field is scalar or value-owned, so callers cannot mutate
// the plan through the returned copy.
func (p *Plan) Request() MoveRequest {
	if p == nil {
		return MoveRequest{}
	}
	return p.request
}

func (p *Plan) SnapshotSourceMember() uint64 {
	if p == nil {
		return 0
	}
	return p.request.SnapshotSourceMember
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
		SourceMember: p.request.SnapshotSourceMember, TargetMember: p.request.TargetMember,
		ToOwnershipEpoch:  p.baseState.Binding.OwnershipEpoch + 1,
		ToRoutingVersion:  p.baseState.Binding.RoutingVersion + 1,
		ToRouteGeneration: p.baseState.Binding.RouteGeneration + 1,
		ToOwnedRange:      p.baseState.Binding.OwnedRange,
	})
}

func invalidGroup(group raftmember.GroupKey) bool {
	return group.ClusterID == ([16]byte{}) || group.ClusterIncarnation == ([16]byte{}) ||
		group.TopologyRecoveryEpoch == 0 || group.ShardIncarnation == ([16]byte{}) ||
		group.GroupID == ([16]byte{})
}

func invalidMoveRequestBase(request MoveRequest) bool {
	return request.Distribution == "" || request.Shard == "" || invalidGroup(request.Group) ||
		request.RetiringMember == request.SnapshotSourceMember ||
		request.RetiringMember == request.TargetMember ||
		request.SnapshotSourceMember == request.TargetMember ||
		raft.IsLocalMsgTarget(request.RetiringMember) ||
		raft.IsLocalMsgTarget(request.SnapshotSourceMember) ||
		raft.IsLocalMsgTarget(request.TargetMember) ||
		request.Source == "" || request.Target == "" || request.Source == request.Target
}

func invalidMoveRequest(request MoveRequest) bool {
	return invalidMoveRequestBase(request) || !validReplicaIdentity(request.RetiringReplica) ||
		request.RetiringReplica.Member != request.RetiringMember ||
		request.RetiringReplica.ControlEndpoint == request.Source
}

func validReplicaIdentity(identity ReplicaIdentity) bool {
	return !raft.IsLocalMsgTarget(identity.Member) && identity.Node != (rafttransport.NodeID{}) &&
		identity.StoreID != ([16]byte{}) && identity.NodeIncarnation != 0 &&
		identity.ControlEndpoint != ""
}

func bindRetiringReplica(current *gateway.Snapshot, request MoveRequest) (MoveRequest, error) {
	var workspace [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := current.ResolveReplicatedMembershipRoute(
		request.Distribution, request.Shard, workspace[:0],
	)
	var exact ReplicaIdentity
	if ok {
		if route.Serving.Group != request.Group {
			return MoveRequest{}, ErrInvalidPlan
		}
		for _, replica := range route.Serving.Replicas {
			if replica.Member != request.RetiringMember {
				continue
			}
			if distribution.EndpointID(replica.Endpoint) != request.Source {
				return MoveRequest{}, ErrInvalidPlan
			}
			exact = ReplicaIdentity{
				Member: replica.Member, Node: replica.Node, StoreID: replica.StoreID,
				NodeIncarnation: replica.NodeIncarnation,
				ControlEndpoint: distribution.EndpointID(replica.ControlEndpoint),
			}
			break
		}
		if !validReplicaIdentity(exact) {
			return MoveRequest{}, ErrInvalidPlan
		}
		if request.RetiringReplica != (ReplicaIdentity{}) && request.RetiringReplica != exact {
			return MoveRequest{}, ErrInvalidPlan
		}
		request.RetiringReplica = exact
	} else if !validReplicaIdentity(request.RetiringReplica) ||
		request.RetiringReplica.Member != request.RetiringMember {
		return MoveRequest{}, ErrInvalidPlan
	}
	if _, err := current.Address(request.RetiringReplica.ControlEndpoint); err != nil {
		return MoveRequest{}, ErrInvalidPlan
	}
	return request, nil
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
