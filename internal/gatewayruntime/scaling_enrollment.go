package gatewayruntime

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/scaling"
)

var (
	errScalingEnrollmentUnavailable = errors.New("gatewayruntime: scaling enrollment source is unavailable")
	errScalingEnrollmentDrift       = errors.New("gatewayruntime: source preparation payload drifted from enrollment")
)

// ScalingEnrollmentOptions contains the narrow runtime dependencies needed to
// construct both sides of the node-control adapter. Source is called while a
// draft intent is being built and again for every Prepare/Adopt retry; it must
// read the source's retained catalog or an exact durable payload.
type ScalingEnrollmentOptions struct {
	Catalog       scalingCatalogReader
	Source        nodecontrol.PayloadProvider
	ControlOpener nodecontrol.StreamOpener
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
}

// ScalingEnrollmentRuntime is the shipped gateway adapter. The builder owns
// no process-local transition state: all immutable identities are derived from
// the current catalog cut and all payload bytes are refetched through Source.
type ScalingEnrollmentRuntime struct {
	catalog     scalingCatalogReader
	source      nodecontrol.PayloadProvider
	provisioner gateway.NodeProvisioner
}

// NewScalingEnrollmentRuntime constructs the authenticated target provisioner
// and source-certified enrollment builder as one unit. Requiring the source
// and transport together prevents a runtime from accidentally installing a
// controller that can reserve catalog rows but cannot execute the exact
// preparation protocol.
func NewScalingEnrollmentRuntime(options ScalingEnrollmentOptions) (*ScalingEnrollmentRuntime, error) {
	if options.Catalog == nil || options.Source == nil || options.ControlOpener == nil ||
		options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, errScalingEnrollmentUnavailable
	}
	client, err := nodecontrol.NewClient(nodecontrol.ClientOptions{
		Opener: options.ControlOpener, ReadDeadline: options.ReadDeadline,
		WriteDeadline: options.WriteDeadline,
	})
	if err != nil {
		return nil, err
	}
	verified := nodecontrol.VerifiedPayloadProvider(options.Source)
	provisioner, err := nodecontrol.NewProvisioner(client, verified)
	if err != nil {
		return nil, err
	}
	return &ScalingEnrollmentRuntime{
		catalog: options.Catalog, source: options.Source, provisioner: provisioner,
	}, nil
}

var _ ScalingEnrollmentBuilder = (*ScalingEnrollmentRuntime)(nil)

func (runtime *ScalingEnrollmentRuntime) Provisioner() gateway.NodeProvisioner {
	if runtime == nil {
		return nil
	}
	return runtime.provisioner
}

// BuildEnrollment derives one immutable enrollment from the current
// authority-backed catalog. Target member/store identities are deterministic
// hashes with explicit collision checks; no max+1 or prior store identity is
// reused, and the resulting values are persisted in the intent before any
// node-control side effect.
func (runtime *ScalingEnrollmentRuntime) BuildEnrollment(
	ctx context.Context,
	parent gateway.ScalingIntent,
	move scaling.ReplicaMove,
	membership gateway.ReplicatedMembershipRoute,
	target gateway.NodeRecord,
) (gateway.GroupEnrollmentIntent, error) {
	if runtime == nil || runtime.catalog == nil || runtime.source == nil || ctx == nil ||
		parent.ID == ([32]byte{}) || !parent.Request.Valid() || !target.Valid() ||
		!move.TargetNode.Valid() || move.TargetNode != (gateway.NodeReference{NodeID: target.NodeID, Incarnation: target.Incarnation}) {
		return gateway.GroupEnrollmentIntent{}, errScalingEnrollmentUnavailable
	}
	if err := context.Cause(ctx); err != nil {
		return gateway.GroupEnrollmentIntent{}, err
	}
	snapshot, err := runtime.catalog.Read(ctx)
	if err != nil || snapshot == nil {
		return gateway.GroupEnrollmentIntent{}, errors.Join(err, errScalingEnrollmentUnavailable)
	}
	if move.ExpectedCatalogGeneration == 0 || snapshot.Generation() != move.ExpectedCatalogGeneration ||
		parent.CatalogGeneration == 0 || parent.CatalogGeneration > snapshot.Generation() ||
		move.Group != membership.Serving.Group || move.Distribution != membership.Serving.Distribution ||
		move.Shard != membership.Serving.Shard || uint64(move.AllocationGeneration) != membership.Serving.AllocationGeneration ||
		membership.HasEnrolledTarget || len(membership.Serving.Replicas) != gateway.ServingReplicaCount {
		return gateway.GroupEnrollmentIntent{}, gateway.ErrScalingIdentity
	}
	if target.NodeID == move.SourceNode.NodeID && target.Incarnation == move.SourceNode.Incarnation {
		return gateway.GroupEnrollmentIntent{}, gateway.ErrScalingIdentity
	}
	var source gateway.ReplicatedEndpoint
	foundSource := false
	for _, replica := range membership.Serving.Replicas {
		if replica.Member == move.Source.Member && replica.Node == move.Source.Node &&
			replica.NodeIncarnation == move.Source.NodeIncarnation && replica.StoreID == move.Source.StoreID {
			if replica.Endpoint != string(move.Source.Endpoint) || replica.NativeEndpoint != string(move.Source.NativeEndpoint) ||
				replica.ControlEndpoint != string(move.Source.ControlEndpoint) {
				return gateway.GroupEnrollmentIntent{}, gateway.ErrScalingIdentity
			}
			source, foundSource = replica, true
			break
		}
	}
	if !foundSource || move.ReplicaOrdinal >= gateway.ServingReplicaCount {
		return gateway.GroupEnrollmentIntent{}, gateway.ErrScalingIdentity
	}
	// The retiring source cannot also provide the snapshot base. The move
	// planner requires a distinct current voter for that certificate, and the
	// enrollment row must retain that choice across every retry and restart.
	// Pick the lowest numbered remaining voter so the immutable intent is
	// deterministic even if a catalog adapter returns the roster in another
	// valid order.
	var snapshotSourceMember uint64
	for _, replica := range membership.Serving.Replicas {
		if replica.Member == 0 || replica.Member == source.Member {
			continue
		}
		if snapshotSourceMember == 0 || replica.Member < snapshotSourceMember {
			snapshotSourceMember = replica.Member
		}
	}
	if snapshotSourceMember == 0 {
		return gateway.GroupEnrollmentIntent{}, gateway.ErrScalingIdentity
	}
	rosterDigest, descriptorDigest, ok := gateway.ReplicatedInitialMembershipDigests(snapshot, move.Group)
	if !ok {
		return gateway.GroupEnrollmentIntent{}, gateway.ErrReplicatedCatalogConflict
	}
	headDigest, err := gateway.ReplicatedCatalogHeadDigest(snapshot)
	if err != nil || headDigest == (replication.Digest{}) {
		return gateway.GroupEnrollmentIntent{}, errors.Join(err, gateway.ErrReplicatedCatalogConflict)
	}
	member, store, err := allocateEnrollmentIdentity(parent.ID, move, membership.Serving.Replicas)
	if err != nil {
		return gateway.GroupEnrollmentIntent{}, err
	}
	targetIdentity := gateway.ReplicaIdentity{
		Member: member, Node: target.NodeID, NodeIncarnation: target.Incarnation,
		StoreID: store, Endpoint: target.DataEndpoint,
		NativeEndpoint: target.NativeEndpoint, ControlEndpoint: target.ControlEndpoint,
	}
	if !targetIdentity.Valid() {
		return gateway.GroupEnrollmentIntent{}, gateway.ErrScalingIdentity
	}
	draft := gateway.GroupEnrollmentIntent{
		Group: move.Group, Distribution: move.Distribution, Shard: move.Shard,
		AllocationGeneration: move.AllocationGeneration, CatalogGeneration: snapshot.Generation(),
		ExpectedCatalogHeadDigest: headDigest, ReplicaOrdinal: move.ReplicaOrdinal,
		Source: endpointIdentity(source), SnapshotSourceMember: snapshotSourceMember,
		Target: targetIdentity, ExpectedRosterDigest: rosterDigest,
		ExpectedDescriptorDigest: descriptorDigest, ExpectedCommand: membership.Serving.Command,
		TargetNodeRevision: move.TargetNodeRevision, State: gateway.EnrollmentReserved, Revision: 1,
	}
	if draft.TargetNodeRevision == 0 || target.Revision != draft.TargetNodeRevision {
		return gateway.GroupEnrollmentIntent{}, gateway.ErrScalingRevision
	}
	draft.IntentID = scalingEnrollmentID(parent.ID, draft)
	if draft.IntentID == ([32]byte{}) {
		return gateway.GroupEnrollmentIntent{}, errScalingEnrollmentUnavailable
	}
	payload, err := runtime.source(ctx, draft)
	if err != nil {
		return gateway.GroupEnrollmentIntent{}, err
	}
	if len(payload) == 0 || len(payload) > nodecontrol.MaxPayloadBytes {
		return gateway.GroupEnrollmentIntent{}, nodecontrol.ErrBound
	}
	spec, err := nodecontrol.OpenPreparationSpec(payload)
	if err != nil {
		return gateway.GroupEnrollmentIntent{}, err
	}
	if err = validateDraftPreparation(spec, draft, membership.Serving.Replicas); err != nil {
		return gateway.GroupEnrollmentIntent{}, errors.Join(errScalingEnrollmentDrift, err)
	}
	canonical, err := nodecontrol.AppendPreparationSpec(nil, spec)
	if err != nil || !slices.Equal(canonical, payload) {
		return gateway.GroupEnrollmentIntent{}, errors.Join(errScalingEnrollmentDrift, err)
	}
	draft.ExpectedManifestDigest = replication.Digest(sha256.Sum256(canonical))
	if !draft.Valid() {
		return gateway.GroupEnrollmentIntent{}, errors.Join(errScalingEnrollmentDrift, gateway.ErrInvalidScalingMetadata)
	}
	return draft, nil
}

func endpointIdentity(replica gateway.ReplicatedEndpoint) gateway.ReplicaIdentity {
	return gateway.ReplicaIdentity{Member: replica.Member, Node: replica.Node,
		NodeIncarnation: replica.NodeIncarnation, StoreID: replica.StoreID,
		Endpoint: distribution.EndpointID(replica.Endpoint), NativeEndpoint: distribution.EndpointID(replica.NativeEndpoint),
		ControlEndpoint: distribution.EndpointID(replica.ControlEndpoint)}
}

func validateDraftPreparation(
	spec nodecontrol.PreparationSpec,
	intent gateway.GroupEnrollmentIntent,
	serving []gateway.ReplicatedEndpoint,
) error {
	if spec.Kind != nodecontrol.PreparationSpecKind || spec.Group != intent.Group ||
		spec.Distribution != intent.Distribution || spec.Shard != intent.Shard ||
		spec.AllocationGeneration != intent.AllocationGeneration ||
		spec.ReplicaOrdinal != intent.ReplicaOrdinal || spec.SourceCommand != intent.ExpectedCommand ||
		spec.Target.MemberID != intent.Target.Member || spec.Target.Node != intent.Target.Node ||
		spec.Target.PeerEndpoint != intent.Target.Endpoint ||
		spec.TargetNodeIncarnation != intent.Target.NodeIncarnation ||
		spec.TargetStoreID != intent.Target.StoreID ||
		spec.LogicalSchemaDigest != intent.ExpectedCommand.RelationManifestDigest {
		return nodecontrol.ErrStale
	}
	if len(serving) != gateway.ServingReplicaCount {
		return nodecontrol.ErrControl
	}
	for _, replica := range serving {
		found := false
		for _, member := range spec.InitialVoters {
			if member.MemberID == replica.Member && member.Node == replica.Node && member.PeerEndpoint == distribution.EndpointID(replica.Endpoint) && member.PeerAddress == replica.DataAddress {
				found = true
				break
			}
		}
		if !found {
			return nodecontrol.ErrConflict
		}
	}
	return nil
}

const enrollmentIdentityDomain = "vibedb/scaling/enrollment-identity/v1\x00"

func allocateEnrollmentIdentity(
	parent [32]byte, move scaling.ReplicaMove, serving []gateway.ReplicatedEndpoint,
) (uint64, [16]byte, error) {
	if parent == ([32]byte{}) || len(serving) != gateway.ServingReplicaCount {
		return 0, [16]byte{}, gateway.ErrScalingIdentity
	}
	base := make([]byte, 0, len(enrollmentIdentityDomain)+32+16+8+len(move.Distribution)+len(move.Shard)+32+8)
	base = append(base, enrollmentIdentityDomain...)
	base = append(base, parent[:]...)
	base = append(base, move.Group.ClusterID[:]...)
	base = append(base, move.Group.ClusterIncarnation[:]...)
	base = append(base, move.Group.ShardIncarnation[:]...)
	base = append(base, move.Group.GroupID[:]...)
	base = append(base, []byte(move.Distribution)...)
	base = append(base, 0)
	base = append(base, []byte(move.Shard)...)
	base = append(base, 0)
	base = append(base, move.TargetNode.NodeID[:]...)
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], move.TargetNode.Incarnation)
	base = append(base, scalar[:]...)
	binary.LittleEndian.PutUint64(scalar[:], uint64(move.ReplicaOrdinal))
	base = append(base, scalar[:]...)
	usedMembers := make(map[uint64]struct{}, len(serving)+1)
	usedStores := make(map[[16]byte]struct{}, len(serving)+1)
	usedEndpoints := make(map[string]struct{}, len(serving)*3)
	for _, replica := range serving {
		usedMembers[replica.Member] = struct{}{}
		usedStores[replica.StoreID] = struct{}{}
		for _, endpoint := range []string{string(replica.Endpoint), string(replica.NativeEndpoint), string(replica.ControlEndpoint)} {
			usedEndpoints[endpoint] = struct{}{}
		}
	}
	for salt := uint64(0); salt < 1024; salt++ {
		var saltBytes [8]byte
		binary.LittleEndian.PutUint64(saltBytes[:], salt)
		hash := sha256.New()
		_, _ = hash.Write(base)
		_, _ = hash.Write(saltBytes[:])
		sum := hash.Sum(nil)
		member := binary.LittleEndian.Uint64(sum[:8])
		if member == 0 {
			continue
		}
		var store [16]byte
		copy(store[:], sum[8:24])
		if store == ([16]byte{}) {
			continue
		}
		if _, exists := usedMembers[member]; exists {
			continue
		}
		if _, exists := usedStores[store]; exists {
			continue
		}
		return member, store, nil
	}
	return 0, [16]byte{}, fmt.Errorf("%w: no collision-free target identity", gateway.ErrScalingIdentity)
}

func (runtime *Runtime) openScalingEnrollment(opener nodecontrol.StreamOpener, read, write rafttransport.DeadlineFunc) error {
	if runtime.config.ScalingProvisioner != nil && runtime.config.ScalingEnrollment != nil {
		return nil
	}
	if runtime.config.ScalingProvisioner != nil || runtime.config.ScalingEnrollment != nil {
		return errScalingEnrollmentUnavailable
	}
	client, err := nodecontrol.NewPreparationSourceClient(nodecontrol.ClientOptions{Opener: opener, ReadDeadline: read, WriteDeadline: write})
	if err != nil {
		return err
	}
	source := func(ctx context.Context, intent gateway.GroupEnrollmentIntent) ([]byte, error) {
		var voters [3]nodecontrol.PreparationMember
		var target nodecontrol.PreparationMember
		if intent.ExpectedManifestDigest == (replication.Digest{}) {
			snapshot, err := runtime.authority.Read(ctx)
			if err != nil {
				return nil, err
			}
			var endpoints [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
			route, found := snapshot.ResolveReplicatedRoute(intent.Distribution, intent.Shard, endpoints[:0])
			if !found || route.Group != intent.Group || len(route.Replicas) != len(voters) {
				return nil, errScalingEnrollmentDrift
			}
			for i, member := range route.Replicas {
				voters[i] = nodecontrol.PreparationMember{MemberID: member.Member, Node: member.Node, PeerEndpoint: distribution.EndpointID(member.Endpoint), NativeEndpoint: distribution.EndpointID(member.NativeEndpoint), ControlEndpoint: distribution.EndpointID(member.ControlEndpoint), PeerAddress: member.DataAddress, NativeAddress: member.Address, ControlAddress: member.ControlAddress}
			}
			node, err := runtime.authority.ReadNode(ctx, intent.Target.Node, intent.Target.NodeIncarnation)
			if err != nil {
				return nil, err
			}
			if node.Revision != intent.TargetNodeRevision || node.DataEndpoint != intent.Target.Endpoint || node.NativeEndpoint != intent.Target.NativeEndpoint || node.ControlEndpoint != intent.Target.ControlEndpoint {
				return nil, errScalingEnrollmentDrift
			}
			target = nodecontrol.PreparationMember{MemberID: intent.Target.Member, Node: node.NodeID, PeerEndpoint: node.DataEndpoint, NativeEndpoint: node.NativeEndpoint, ControlEndpoint: node.ControlEndpoint, PeerAddress: node.DataAddress, NativeAddress: node.NativeAddress, ControlAddress: node.ControlAddress}
		}
		slices.SortFunc(voters[:], func(a, b nodecontrol.PreparationMember) int { return cmp.Compare(a.MemberID, b.MemberID) })
		return client.Read(ctx, intent, voters, target)
	}
	enrollment, err := NewScalingEnrollmentRuntime(ScalingEnrollmentOptions{Catalog: runtime.authority, Source: source, ControlOpener: opener, ReadDeadline: read, WriteDeadline: write})
	if err != nil {
		return err
	}
	runtime.config.ScalingEnrollment = enrollment
	runtime.config.ScalingProvisioner = enrollment.Provisioner()
	return nil
}
