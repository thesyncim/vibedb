package gatewayruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// frontendDrainPreparedAckDiscriminator is a separately authenticated command:
// unlike a participant scan, success proves the receiver installed the exact
// Prepared service cut before the owner can expose a continuation credential.
var frontendDrainPreparedAckDiscriminator = [...]byte{'V', 'B', 'D', 'D', 'A', 'C', 'K', 1}

const (
	frontendDrainPreparedAckRequestWireSize  = frontendParticipantRequestWireSize + 64
	frontendDrainPreparedAckResponseWireSize = 16 + 32 + 32 + 8 + 32
)

type frontendDrainPreparedAckRequest struct {
	frontendParticipantScanRequest
	DrainID     [32]byte
	GrantDigest [32]byte
}

type frontendDrainPreparedAckPhysicalOpener interface {
	OpenShardControlEndpoint(context.Context, gateway.ReplicatedEndpoint) (rafttransport.PeerConnection, error)
}

type frontendDrainPreparedAckPhysicalInstaller interface {
	LocalNodeID() rafttransport.NodeID
	InstallFrontendDrainServiceCut(context.Context, frontenddrain.PreparedAckCut) (uint64, error)
	ServiceCutCoordinates() (frontenddrain.PreparedAckCutReadFloor, bool)
}

type frontendDrainPreparedAckReceiver struct {
	node     gateway.NodeRecord
	endpoint gateway.ReplicatedEndpoint
}

func frontendDrainRecordGatewayKey(record gateway.FrontendDrainRecord) replication.Digest {
	if record.PeerKeyDigest != (replication.Digest{}) {
		return record.PeerKeyDigest
	}
	return record.GatewayServiceKeyDigest
}

func (request frontendDrainPreparedAckRequest) valid() bool {
	// The same authenticated route is replayed after the atomic Enforcing and
	// Retired commits. An empty-token drain has no bearer digest; its canonical
	// child subject in the source cut is the proof instead.
	return request.frontendParticipantScanRequest.valid() && request.Lifecycle.Valid() &&
		request.DrainID != ([32]byte{})
}

func (request frontendDrainPreparedAckRequest) marshal() []byte {
	raw := make([]byte, frontendDrainPreparedAckRequestWireSize)
	copy(raw[:frontendParticipantRequestWireSize], request.frontendParticipantScanRequest.marshalWithDiscriminator(frontendDrainPreparedAckDiscriminator))
	copy(raw[frontendParticipantRequestWireSize:frontendParticipantRequestWireSize+32], request.DrainID[:])
	copy(raw[frontendParticipantRequestWireSize+32:], request.GrantDigest[:])
	return raw
}

func openFrontendDrainPreparedAckRequest(raw []byte) (frontendDrainPreparedAckRequest, error) {
	if len(raw) != frontendDrainPreparedAckRequestWireSize {
		return frontendDrainPreparedAckRequest{}, errFrontendParticipantWire
	}
	base, err := unmarshalFrontendParticipantScanRequestWithDiscriminator(raw[:frontendParticipantRequestWireSize], frontendDrainPreparedAckDiscriminator)
	if err != nil {
		return frontendDrainPreparedAckRequest{}, err
	}
	request := frontendDrainPreparedAckRequest{frontendParticipantScanRequest: base}
	copy(request.DrainID[:], raw[frontendParticipantRequestWireSize:frontendParticipantRequestWireSize+32])
	copy(request.GrantDigest[:], raw[frontendParticipantRequestWireSize+32:])
	if !request.valid() {
		return frontendDrainPreparedAckRequest{}, errFrontendParticipantWire
	}
	return request, nil
}

type frontendDrainPreparedAckResponse struct {
	Nonce       [frontendParticipantNonceSize]byte
	DrainID     [32]byte
	GrantDigest [32]byte
	Revision    uint64
}

func (response frontendDrainPreparedAckResponse) valid(request frontendDrainPreparedAckRequest) bool {
	return response.Nonce == request.Nonce && response.DrainID == request.DrainID &&
		response.GrantDigest == request.GrantDigest && response.Revision != 0
}

func (response frontendDrainPreparedAckResponse) marshal() []byte {
	raw := make([]byte, frontendDrainPreparedAckResponseWireSize)
	copy(raw[:16], response.Nonce[:])
	copy(raw[16:48], response.DrainID[:])
	copy(raw[48:80], response.GrantDigest[:])
	binary.LittleEndian.PutUint64(raw[80:88], response.Revision)
	digest := sha256.Sum256(raw[:88])
	copy(raw[88:], digest[:])
	return raw
}

func openFrontendDrainPreparedAckResponse(raw []byte, request frontendDrainPreparedAckRequest) (frontendDrainPreparedAckResponse, error) {
	if len(raw) != frontendDrainPreparedAckResponseWireSize {
		return frontendDrainPreparedAckResponse{}, errFrontendParticipantWire
	}
	digest := sha256.Sum256(raw[:88])
	if string(digest[:]) != string(raw[88:]) {
		return frontendDrainPreparedAckResponse{}, errFrontendParticipantWire
	}
	var response frontendDrainPreparedAckResponse
	copy(response.Nonce[:], raw[:16])
	copy(response.DrainID[:], raw[16:48])
	copy(response.GrantDigest[:], raw[48:80])
	response.Revision = binary.LittleEndian.Uint64(raw[80:88])
	if !response.valid(request) {
		return frontendDrainPreparedAckResponse{}, errFrontendParticipantWire
	}
	return response, nil
}

// acknowledgePreparedFrontendDrain applies the exact Active/Prepared service
// cut locally. It deliberately does not install the frontend credential: the
// caller does that only after every member in the captured gateway roster has
// returned this acknowledgement.
func (runtime *Runtime) acknowledgePreparedFrontendDrain(ctx context.Context, request frontendDrainPreparedAckRequest) (uint64, error) {
	if runtime == nil || ctx == nil || runtime.authority == nil || runtime.serviceDirectory == nil ||
		runtime.config.TLSProfile == nil || runtime.config.Authorization == nil || !request.valid() {
		return 0, gateway.ErrScalingState
	}
	node, err := runtime.authority.ReadNode(ctx, request.NodeID, request.Incarnation)
	if err != nil || !request.frontendParticipantScanRequest.matches(node) {
		return 0, errors.Join(gateway.ErrScalingIdentity, errFrontendParticipantWire)
	}
	record, err := runtime.authority.ReadFrontendDrainRecord(ctx, request.DrainID)
	if err != nil || !frontendDrainRecordMatchesAckNode(record, node) {
		return 0, errors.Join(gateway.ErrScalingState, err)
	}
	if record.ContinuationGrant != nil {
		if request.GrantDigest != record.ContinuationGrant.GrantDigest {
			return 0, errors.Join(gateway.ErrScalingState, errFrontendParticipantWire)
		}
	} else if request.GrantDigest != ([32]byte{}) {
		return 0, errors.Join(gateway.ErrScalingState, errFrontendParticipantWire)
	}
	// Read the complete authority cut once. The effective catalog generation is
	// allowed to advance without rewriting NodeRecords, so a node-only read
	// would reject a valid catalog publication or project a mixed service cut.
	sourceCut, err := runtime.frontendDrainPreparedAckSourceCut(ctx, node, record)
	if err != nil {
		return 0, err
	}
	serviceCut := sourceCut.ServiceDirectory
	if !frontendDrainPreparedAckServiceRecordMatches(sourceCut, record) ||
		runtime.serviceDirectory.ApplyCommittedCut(serviceCut) != nil {
		return 0, gateway.ErrScalingState
	}
	return serviceCut.Revision, nil
}

// frontendDrainRecordMatchesAckNode binds a lifecycle ACK to the exact
// physical identity. RetireNode increments the terminal NodeRecord revision
// while retaining the Enforcing child revision as the drain proof's node
// coordinate, so the terminal case accepts precisely that one-step transition.
func frontendDrainRecordMatchesAckNode(record gateway.FrontendDrainRecord, node gateway.NodeRecord) bool {
	if record.Lifecycle != gateway.FrontendDrainRetired {
		return record.ValidForNode(node)
	}
	if node.Lifecycle != gateway.NodeDecommissioned || record.NodeRevision == ^uint64(0) ||
		node.Revision != record.NodeRevision+1 {
		return false
	}
	prior := node
	prior.Revision = record.NodeRevision
	return record.ValidForNode(prior)
}

// AcknowledgeFrontendDrainLifecycle is the controller-facing lifecycle
// barrier. It resolves the durable child by the intent-derived drain ID and
// replays the canonical exact-cut acknowledgement against the current
// serving roster. Gateway nodes require the barrier; storage-only nodes have
// no frontend admission surface and therefore complete this seam as a no-op.
func (runtime *Runtime) AcknowledgeFrontendDrainLifecycle(
	ctx context.Context, node gateway.NodeRecord, drainID [32]byte,
) error {
	if runtime == nil || ctx == nil || runtime.authority == nil || !node.Valid() || drainID == ([32]byte{}) {
		return gateway.ErrScalingState
	}
	if node.Roles&gateway.NodeRoleGateway == 0 {
		return nil
	}
	record, err := runtime.authority.ReadFrontendDrainRecord(ctx, drainID)
	if err != nil {
		return err
	}
	if record.DrainID != drainID || record.PhysicalNode != node.NodeID ||
		record.PhysicalIncarnation != node.Incarnation ||
		record.GatewayServiceID != node.Gateway.NodeID ||
		record.GatewayIncarnation != node.Gateway.Incarnation {
		return gateway.ErrScalingIdentity
	}
	if record.Lifecycle != gateway.FrontendDrainPrepared &&
		record.Lifecycle != gateway.FrontendDrainEnforcing &&
		record.Lifecycle != gateway.FrontendDrainRetired {
		return gateway.ErrScalingState
	}
	return runtime.acknowledgeFrontendDrainLifecycleRoster(ctx, node, record)
}

func (runtime *Runtime) serveFrontendDrainPreparedAckConnection(ctx context.Context, connection rafttransport.PeerConnection) error {
	// The retiring gateway distributes its already committed proof. It need
	// not be the topology controller allowed to initiate a drain; the handler
	// rereads the exact child before applying any service-directory change.
	if runtime == nil || ctx == nil || connection == nil || !runtime.authorizeFrontendParticipantPeer(connection) {
		return errFrontendParticipantAuth
	}
	var configuredReadDeadline time.Time
	if runtime.controlReadDeadline != nil {
		configuredReadDeadline = runtime.controlReadDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredReadDeadline); deadline.IsZero() {
		return errFrontendParticipantWire
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	raw := make([]byte, frontendDrainPreparedAckRequestWireSize)
	if _, err := io.ReadFull(connection, raw); err != nil {
		return err
	}
	request, err := openFrontendDrainPreparedAckRequest(raw)
	if err != nil {
		return err
	}
	revision, err := runtime.acknowledgePreparedFrontendDrain(ctx, request)
	if err != nil {
		return err
	}
	var configuredWriteDeadline time.Time
	if runtime.controlWriteDeadline != nil {
		configuredWriteDeadline = runtime.controlWriteDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredWriteDeadline); deadline.IsZero() {
		return errFrontendParticipantWire
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return writeFrontendParticipantFrame(connection, frontendDrainPreparedAckResponse{Nonce: request.Nonce,
		DrainID: request.DrainID, GrantDigest: request.GrantDigest, Revision: revision}.marshal())
}

// frontendDrainPreparedAckSourceCut reads the one coherent authority cut that
// is sent to every physical receiver in a prepared round. The receiver still
// re-reads this cut through the authenticated source gateway; these checks
// keep the controller request bound to the same durable roster/head fence that
// EnforceFrontendDrain will CAS.
func (runtime *Runtime) frontendDrainPreparedAckSourceCut(
	ctx context.Context, node gateway.NodeRecord, record gateway.FrontendDrainRecord,
) (frontenddrain.PreparedAckCut, error) {
	if runtime == nil || ctx == nil || runtime.authority == nil || runtime.config.TLSProfile == nil ||
		runtime.config.Authorization == nil || !node.Valid() || !frontendDrainRecordMatchesAckNode(record, node) ||
		!record.Lifecycle.Valid() {
		return frontenddrain.PreparedAckCut{}, gateway.ErrScalingState
	}
	source, err := runtime.authority.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil {
		return frontenddrain.PreparedAckCut{}, err
	}
	return runtime.frontendDrainPreparedAckSourceCutFromRuntimeCut(ctx, source, node, record)
}

func (runtime *Runtime) frontendDrainPreparedAckSourceCutFromRuntimeCut(
	ctx context.Context, source gateway.FrontendDrainRuntimeCut,
	node gateway.NodeRecord, record gateway.FrontendDrainRecord,
) (frontenddrain.PreparedAckCut, error) {
	if runtime == nil || ctx == nil || runtime.config.TLSProfile == nil || runtime.config.Authorization == nil ||
		!node.Valid() || !frontendDrainRecordMatchesAckNode(record, node) || !record.Lifecycle.Valid() {
		return frontenddrain.PreparedAckCut{}, gateway.ErrScalingState
	}
	if !source.Nodes.Valid() || source.Catalog == nil || source.CatalogHeadDigest == (replication.Digest{}) ||
		source.Catalog.Generation() != source.Nodes.CatalogGeneration ||
		!sourceContainsExactNode(source, node) ||
		!frontendDrainPreparedAckSourceRecordMatches(source, record) {
		return frontenddrain.PreparedAckCut{}, gateway.ErrScalingRevision
	}
	if record.Lifecycle == gateway.FrontendDrainPrepared &&
		(source.Nodes.Revision != record.ReceiverDirectoryRevision ||
			source.Nodes.Digest != record.ReceiverDirectoryDigest ||
			source.Nodes.CatalogGeneration != record.ReceiverCatalogGeneration ||
			source.CatalogHeadDigest != record.ReceiverCatalogHeadDigest) {
		return frontenddrain.PreparedAckCut{}, gateway.ErrScalingRevision
	}
	serviceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		ctx, source, runtime.config.TLSProfile, runtime.config.Authorization.Generation(),
	)
	if err != nil {
		return frontenddrain.PreparedAckCut{}, err
	}
	cut := frontenddrain.PreparedAckCut{
		DirectoryRevision: source.Nodes.Revision, DirectoryDigest: source.Nodes.Digest,
		CatalogGeneration: source.Nodes.CatalogGeneration, CatalogHeadDigest: source.CatalogHeadDigest,
		ServiceDirectoryRevision: serviceCut.Revision, ServiceDirectory: serviceCut,
		SourceRoster: frontendDrainPreparedAckSourceRoster(source),
		Subjects:     frontendDrainPreparedAckSubjects(source),
	}
	if !cut.Valid() || !frontendDrainPreparedAckServiceRecordMatches(cut, record) {
		return frontenddrain.PreparedAckCut{}, gateway.ErrScalingState
	}
	return cut, nil
}

func frontendDrainPreparedAckSourceRecordMatches(
	source gateway.FrontendDrainRuntimeCut, record gateway.FrontendDrainRecord,
) bool {
	if record.ContinuationGrant != nil {
		wantState, ok := frontendDrainContinuationState(record.Lifecycle)
		if !ok {
			return false
		}
		for _, grant := range source.ContinuationGrants {
			if grant.GrantDigest != record.ContinuationGrant.GrantDigest {
				continue
			}
			return grant.Valid() && grant.DrainID == record.DrainID && grant.State == wantState
		}
		return false
	}
	for _, candidate := range source.DrainRecords {
		if candidate.DrainID != record.DrainID {
			continue
		}
		candidateKey := candidate.PeerKeyDigest
		if candidateKey == (replication.Digest{}) {
			candidateKey = candidate.GatewayServiceKeyDigest
		}
		recordKey := record.PeerKeyDigest
		if recordKey == (replication.Digest{}) {
			recordKey = record.GatewayServiceKeyDigest
		}
		return candidate.Valid() && candidate.IntentID == record.IntentID &&
			candidate.DecommissionIntentID == record.DecommissionIntentID &&
			candidate.PhysicalNode == record.PhysicalNode &&
			candidate.PhysicalIncarnation == record.PhysicalIncarnation &&
			candidate.GatewayServiceID == record.GatewayServiceID &&
			candidate.GatewayIncarnation == record.GatewayIncarnation &&
			candidateKey == recordKey &&
			candidate.GatewayIdentityServiceID == record.GatewayIdentityServiceID &&
			candidate.GatewaySessionID == record.GatewaySessionID &&
			candidate.GatewaySessionRevision == record.GatewaySessionRevision &&
			candidate.NodeRevision == record.NodeRevision && candidate.Revision == record.Revision &&
			candidate.AdmissionEpoch == record.AdmissionEpoch &&
			candidate.AdmissionClosedProofDigest == record.AdmissionClosedProofDigest &&
			candidate.DrainFence == record.DrainFence &&
			candidate.Lifecycle == record.Lifecycle && candidate.ContinuationGrant == nil
	}
	return false
}

func frontendDrainPreparedAckServiceRecordMatches(
	cut frontenddrain.PreparedAckCut, record gateway.FrontendDrainRecord,
) bool {
	if record.ContinuationGrant != nil {
		wantState, ok := frontendDrainContinuationState(record.Lifecycle)
		if !ok {
			return false
		}
		for _, grant := range cut.ServiceDirectory.ContinuationGrants {
			if grant.GrantDigest == record.ContinuationGrant.GrantDigest {
				return grant.Valid() && grant.DrainID == record.DrainID && grant.State == wantState
			}
		}
		return false
	}
	for _, subject := range cut.Subjects {
		if subject.DrainID == record.DrainID {
			return subject.Valid() && subject.GrantDigest == ([32]byte{}) &&
				subject.PhysicalNode == record.PhysicalNode &&
				subject.PhysicalIncarnation == record.PhysicalIncarnation &&
				subject.Lifecycle == uint8(record.Lifecycle)
		}
	}
	return false
}

func frontendDrainContinuationState(
	lifecycle gateway.FrontendDrainLifecycle,
) (serviceauthz.ContinuationGrantState, bool) {
	switch lifecycle {
	case gateway.FrontendDrainPrepared:
		return serviceauthz.ContinuationGrantPrepared, true
	case gateway.FrontendDrainEnforcing:
		return serviceauthz.ContinuationGrantEnforcing, true
	case gateway.FrontendDrainRetired:
		return serviceauthz.ContinuationGrantRetired, true
	default:
		return 0, false
	}
}

// frontendDrainPreparedAckServingReceiversFromServiceCut projects physical
// storage receivers from the certified catalog image and the matching live
// service directory. A route replica or Active storage service is an actual
// native receiver even when its physical node has no Gateway role. Enrolled
// targets and Joining bindings are excluded because they are not serving.
func (runtime *Runtime) frontendDrainPreparedAckServingReceiversFromServiceCut(
	cut gateway.NodeDirectoryCut, snapshot *gateway.Snapshot, serviceCut *serviceauthz.ServiceDirectoryCut,
) ([]frontendDrainPreparedAckReceiver, error) {
	return runtime.frontendDrainPreparedAckReceiversFromServiceCut(cut, snapshot, serviceCut, false)
}

// frontendDrainPreparedAckPublicationReceiversFromServiceCut is the empty-DrainID
// recovery/publication roster. Joining storage nodes must install the serving
// cut before NodeInfo promotion, so they are included here and excluded from
// drain-lifecycle ACKs.
func (runtime *Runtime) frontendDrainPreparedAckPublicationReceiversFromServiceCut(
	cut gateway.NodeDirectoryCut, snapshot *gateway.Snapshot, serviceCut *serviceauthz.ServiceDirectoryCut,
) ([]frontendDrainPreparedAckReceiver, error) {
	return runtime.frontendDrainPreparedAckReceiversFromServiceCut(cut, snapshot, serviceCut, true)
}

func (runtime *Runtime) frontendDrainPreparedAckReceiversFromServiceCut(
	cut gateway.NodeDirectoryCut, snapshot *gateway.Snapshot, serviceCut *serviceauthz.ServiceDirectoryCut,
	includeJoining bool,
) ([]frontendDrainPreparedAckReceiver, error) {
	var snapshotGeneration uint64
	if snapshot != nil {
		snapshotGeneration = snapshot.Generation()
	}
	if runtime == nil || snapshot == nil || !cut.Valid() || snapshot.Generation() != cut.CatalogGeneration {
		return nil, fmt.Errorf("%w: receiver roster input cut valid=%t snapshot-nil=%t snapshot-generation=%d cut-catalog-generation=%d",
			gateway.ErrScalingRevision, cut.Valid(), snapshot == nil, snapshotGeneration, cut.CatalogGeneration)
	}
	current := make(map[rafttransport.NodeID]gateway.NodeRecord, len(cut.CurrentNodes()))
	for _, node := range cut.CurrentNodes() {
		if !node.Valid() {
			return nil, fmt.Errorf("%w: receiver roster has invalid current node %s incarnation=%d",
				gateway.ErrScalingRevision, nodeIDHex(node.NodeID), node.Incarnation)
		}
		current[node.NodeID] = node
	}
	byIdentity := make(map[frontendDrainPreparedAckReceiverIdentity]frontendDrainPreparedAckReceiver)
	add := func(node gateway.NodeRecord, route *gateway.ReplicatedEndpoint) error {
		if node.Lifecycle == gateway.NodeJoining {
			if !includeJoining {
				return nil
			}
		} else if node.Lifecycle != gateway.NodeActive && node.Lifecycle != gateway.NodeDraining {
			return gateway.ErrScalingState
		}
		if node.Roles&gateway.NodeRoleStorage == 0 || node.ControlAddress == "" {
			return gateway.ErrScalingState
		}
		endpoint := gateway.ReplicatedEndpoint{
			Node: node.NodeID, NodeIncarnation: node.Incarnation,
			Endpoint: string(node.DataEndpoint), DataAddress: node.DataAddress,
			NativeEndpoint: string(node.NativeEndpoint), Address: node.NativeAddress,
			ControlEndpoint: string(node.ControlEndpoint), ControlAddress: node.ControlAddress,
		}
		if route != nil {
			if route.Node != node.NodeID || route.NodeIncarnation != node.Incarnation ||
				route.DataAddress != node.DataAddress || route.Address != node.NativeAddress ||
				route.ControlAddress != node.ControlAddress {
				return fmt.Errorf("%w: receiver route physical identity mismatch node=%s incarnation=%d route-node=%s route-incarnation=%d route-data=%q node-data=%q route-native=%q node-native=%q route-control=%q node-control=%q route-handles=(%q,%q,%q) node-handles=(%q,%q,%q)",
					gateway.ErrScalingRevision, nodeIDHex(node.NodeID), node.Incarnation, nodeIDHex(route.Node), route.NodeIncarnation,
					route.DataAddress, node.DataAddress, route.Address, node.NativeAddress,
					route.ControlAddress, node.ControlAddress,
					route.Endpoint, route.NativeEndpoint, route.ControlEndpoint,
					node.DataEndpoint, node.NativeEndpoint, node.ControlEndpoint)
			}
		}
		identity := frontendDrainPreparedAckReceiverIdentity{node: node.NodeID, incarnation: node.Incarnation}
		if prior, found := byIdentity[identity]; found {
			if prior.node.ServiceKeyDigest != node.ServiceKeyDigest || prior.endpoint.ControlAddress != endpoint.ControlAddress {
				return gateway.ErrScalingIdentity
			}
			return nil
		}
		byIdentity[identity] = frontendDrainPreparedAckReceiver{node: node, endpoint: endpoint}
		return nil
	}
	for index := 0; index < snapshot.ReplicatedRouteCount(); index++ {
		route, ok := snapshot.ReplicatedRouteAt(index, nil)
		if !ok || len(route.Replicas) != gateway.ServingReplicaCount {
			return nil, fmt.Errorf("%w: receiver roster route index=%d valid=%t replicas=%d want=%d",
				gateway.ErrScalingRevision, index, ok, len(route.Replicas), gateway.ServingReplicaCount)
		}
		for replicaIndex := range route.Replicas {
			replica := route.Replicas[replicaIndex]
			node, found := current[replica.Node]
			if !found || node.Incarnation != replica.NodeIncarnation {
				return nil, fmt.Errorf("%w: receiver roster route index=%d replica index=%d node=%s found=%t node-incarnation=%d replica-incarnation=%d",
					gateway.ErrScalingRevision, index, replicaIndex, nodeIDHex(replica.Node), found, node.Incarnation, replica.NodeIncarnation)
			}
			if err := add(node, &replica); err != nil {
				return nil, err
			}
		}
	}
	if serviceCut != nil {
		for _, binding := range serviceCut.Bindings {
			if binding.Roles&serviceauthz.ServiceRoleStorage == 0 ||
				(binding.Lifecycle != serviceauthz.ServiceActive && binding.Lifecycle != serviceauthz.ServiceDraining &&
					!(includeJoining && binding.Lifecycle == serviceauthz.ServiceJoining)) {
				continue
			}
			node, found := current[binding.PhysicalNode]
			if !found || node.Incarnation != binding.PhysicalIncarnation ||
				node.ServiceKeyDigest != replication.Digest(binding.KeyDigest) {
				return nil, fmt.Errorf("%w: receiver service binding node=%s found=%t node-incarnation=%d binding-incarnation=%d node-key=%x binding-key=%x",
					gateway.ErrScalingIdentity, nodeIDHex(binding.PhysicalNode), found, node.Incarnation, binding.PhysicalIncarnation,
					node.ServiceKeyDigest, binding.KeyDigest)
			}
			if err := add(node, nil); err != nil {
				return nil, err
			}
		}
	}
	if includeJoining {
		for _, node := range cut.CurrentNodes() {
			if node.Lifecycle != gateway.NodeJoining || node.Roles&gateway.NodeRoleStorage == 0 {
				continue
			}
			if err := add(node, nil); err != nil {
				return nil, err
			}
		}
	}
	result := make([]frontendDrainPreparedAckReceiver, 0, len(byIdentity))
	for _, receiver := range byIdentity {
		result = append(result, receiver)
	}
	slices.SortFunc(result, func(left, right frontendDrainPreparedAckReceiver) int {
		return bytes.Compare(left.node.NodeID[:], right.node.NodeID[:])
	})
	return result, nil
}

type frontendDrainPreparedAckReceiverIdentity struct {
	node        rafttransport.NodeID
	incarnation uint64
}

func frontendDrainPreparedAckDirectoryDigest(cut serviceauthz.ServiceDirectoryCut) [32]byte {
	return frontenddrain.ServiceDirectoryCutDigest(cut)
}

func (runtime *Runtime) frontendDrainPreparedAckRequest(
	ctx context.Context, node gateway.NodeRecord, record gateway.FrontendDrainRecord,
	receiver frontendDrainPreparedAckReceiver, sourceCut frontenddrain.PreparedAckCut,
) (frontenddrain.PreparedAckRequest, error) {
	if runtime == nil || ctx == nil || !sourceCut.Valid() || !node.Valid() || !receiver.node.Valid() ||
		(receiver.node.Lifecycle != gateway.NodeActive && receiver.node.Lifecycle != gateway.NodeDraining) ||
		receiver.node.Roles&gateway.NodeRoleStorage == 0 {
		return frontenddrain.PreparedAckRequest{}, gateway.ErrScalingState
	}
	base, err := newFrontendParticipantScanRequest(node)
	if err != nil {
		return frontenddrain.PreparedAckRequest{}, err
	}
	sourcePrincipal, sourceKey, ok := runtime.frontendDrainPreparedAckPublisher(sourceCut)
	if !ok {
		return frontenddrain.PreparedAckRequest{}, gateway.ErrScalingIdentity
	}
	grantDigest := [32]byte{}
	if record.ContinuationGrant != nil {
		grantDigest = record.ContinuationGrant.GrantDigest
	}
	request := frontenddrain.PreparedAckRequest{
		Nonce: base.Nonce, DrainID: record.DrainID, GrantDigest: grantDigest,
		SourcePrincipal: sourcePrincipal, SourcePrincipalKeyDigest: sourceKey,
		ReceiverNode: receiver.node.NodeID, ReceiverIncarnation: receiver.node.Incarnation,
		ReceiverServiceKeyDigest: [32]byte(receiver.node.ServiceKeyDigest), ReceiverNodeRevision: receiver.node.Revision,
		RequirePrepared: record.Lifecycle == gateway.FrontendDrainPrepared,
		SourceCut:       sourceCut,
	}
	if !request.Valid() || !frontendDrainPreparedAckCutContainsReceiver(sourceCut, receiver.node) {
		return frontenddrain.PreparedAckRequest{}, gateway.ErrScalingState
	}
	return request, nil
}

func (runtime *Runtime) frontendDrainPreparedAckPublisher(
	cut frontenddrain.PreparedAckCut,
) (rafttransport.NodeID, [32]byte, bool) {
	if runtime == nil || !cut.Valid() {
		return rafttransport.NodeID{}, [32]byte{}, false
	}
	localNode := rafttransport.NodeID{}
	var localKey [32]byte
	if runtime.config.TLSProfile != nil {
		localNode = runtime.config.TLSProfile.LocalIdentity().Node
		localKey = runtime.config.TLSProfile.LocalServiceKeyDigest()
	}
	for _, binding := range cut.ServiceDirectory.Bindings {
		if binding.Roles&serviceauthz.ServiceRoleGateway == 0 ||
			(binding.Lifecycle != serviceauthz.ServiceActive && binding.Lifecycle != serviceauthz.ServiceDraining) {
			continue
		}
		if binding.Principal == localNode && binding.KeyDigest == localKey {
			return binding.Principal, binding.KeyDigest, true
		}
	}
	// A terminal cut deliberately excludes the retired subject from the
	// serving roster. If no current gateway remains, publication must be
	// retried by an eligible local publisher; reusing a remote physical identity
	// would turn a closed process into a source of authority.
	return rafttransport.NodeID{}, [32]byte{}, false
}

func frontendDrainPreparedAckCutContainsReceiver(cut frontenddrain.PreparedAckCut, receiver gateway.NodeRecord) bool {
	for _, binding := range cut.ServiceDirectory.Bindings {
		if binding.Principal == receiver.NodeID && binding.PhysicalNode == receiver.NodeID &&
			binding.PhysicalIncarnation == receiver.Incarnation &&
			binding.KeyDigest == [32]byte(receiver.ServiceKeyDigest) &&
			binding.Roles&serviceauthz.ServiceRoleStorage != 0 &&
			(binding.Lifecycle == serviceauthz.ServiceActive || binding.Lifecycle == serviceauthz.ServiceDraining) {
			return true
		}
	}
	return false
}

func frontendDrainPreparedAckReceiverUnreachable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	var op *net.OpError
	if errors.As(err, &op) && op != nil && op.Err != nil {
		return frontendDrainPreparedAckReceiverUnreachable(op.Err)
	}
	return false
}

func (runtime *Runtime) acknowledgeFrontendDrainPreparedAckPhysicalReceiver(
	ctx context.Context, receiver frontendDrainPreparedAckReceiver, request frontenddrain.PreparedAckRequest,
) error {
	if runtime == nil || ctx == nil || runtime.config.TLSProfile == nil || !request.Valid() {
		return gateway.ErrScalingState
	}
	if installer, ok := runtime.config.Transport.(frontendDrainPreparedAckPhysicalInstaller); ok &&
		receiver.node.NodeID == installer.LocalNodeID() {
		applied, err := installer.InstallFrontendDrainServiceCut(ctx, request.SourceCut)
		if err != nil || applied != request.SourceCut.ServiceDirectoryRevision {
			return errors.Join(gateway.ErrScalingState, err)
		}
		coordinates, present := installer.ServiceCutCoordinates()
		if !present || coordinates != request.SourceCut.ReadFloor() {
			return gateway.ErrScalingRevision
		}
		return nil
	}
	opener := runtime.preparedAckPhysicalOpener
	if opener == nil && runtime.controlOpener != nil {
		opener = runtime.controlOpener
	}
	if opener == nil {
		return gateway.ErrScalingState
	}
	connection, err := opener.OpenShardControlEndpoint(ctx, receiver.endpoint)
	if err != nil || connection == nil {
		return fmt.Errorf("prepared frontend drain storage receiver %s: %w", nodeIDHex(receiver.node.NodeID), frontendParticipantRemoteError(ctx, err))
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	local := runtime.config.TLSProfile.LocalIdentity()
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		connection.PeerIdentity().TrustDomain != local.TrustDomain ||
		connection.PeerIdentity().Node != receiver.node.NodeID ||
		replication.Digest(connection.PeerKeyDigest()) != receiver.node.ServiceKeyDigest {
		return gateway.ErrScalingIdentity
	}
	encoded, err := request.Marshal()
	if err != nil {
		return errors.Join(gateway.ErrScalingState, err)
	}
	var configuredWriteDeadline time.Time
	if runtime.controlWriteDeadline != nil {
		configuredWriteDeadline = runtime.controlWriteDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredWriteDeadline); deadline.IsZero() {
		return errFrontendParticipantWire
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := writeFrontendParticipantFrame(connection, encoded); err != nil {
		return frontendParticipantRemoteError(ctx, err)
	}
	var configuredReadDeadline time.Time
	if runtime.controlReadDeadline != nil {
		configuredReadDeadline = runtime.controlReadDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredReadDeadline); deadline.IsZero() {
		return errFrontendParticipantWire
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	responseRaw := make([]byte, frontenddrain.PreparedAckResponseBytes)
	if _, err := io.ReadFull(connection, responseRaw); err != nil {
		return frontendParticipantRemoteError(ctx, err)
	}
	response, err := frontenddrain.OpenPreparedAckResponse(responseRaw, request)
	if err != nil {
		return errors.Join(gateway.ErrScalingState, err)
	}
	if response.CutMoved() {
		return frontenddrain.ErrPreparedAckCutMoved
	}
	if response.AppliedRevision != request.SourceCut.ServiceDirectoryRevision {
		return gateway.ErrScalingState
	}
	return nil
}

func (runtime *Runtime) acknowledgePreparedFrontendDrainRoster(ctx context.Context, node gateway.NodeRecord, record gateway.FrontendDrainRecord) error {
	if record.Lifecycle != gateway.FrontendDrainPrepared {
		return gateway.ErrScalingState
	}
	return runtime.acknowledgeFrontendDrainLifecycleRoster(ctx, node, record)
}

// acknowledgeFrontendDrainLifecycleRoster performs one exact-cut lifecycle
// round. Prepared retries may refresh the captured receiver fence; enforcing
// and terminal retries always start from the latest committed cut and retain
// the lifecycle child as their only authority.
func (runtime *Runtime) acknowledgeFrontendDrainLifecycleRoster(ctx context.Context, node gateway.NodeRecord, record gateway.FrontendDrainRecord) error {
	if runtime == nil || ctx == nil || runtime.authority == nil ||
		runtime.config.TLSProfile == nil || runtime.config.Authorization == nil || !node.Valid() ||
		((record.Lifecycle != gateway.FrontendDrainRetired) && !record.ValidForNode(node)) ||
		(record.Lifecycle == gateway.FrontendDrainRetired && !frontendDrainRecordMatchesAckNode(record, node)) {
		return gateway.ErrScalingState
	}
	if !record.Lifecycle.Valid() {
		return gateway.ErrScalingState
	}
	const maxLifecycleAckAttempts = 3
	currentRecord := record
	for attempt := 0; attempt < maxLifecycleAckAttempts; attempt++ {
		err := runtime.acknowledgeFrontendDrainLifecycleRosterOnce(ctx, node, currentRecord)
		if err == nil {
			return nil
		}
		retryableRevision := errors.Is(err, gateway.ErrScalingRevision) && !errors.Is(err, gateway.ErrScalingIdentity)
		if (!retryableRevision && !errors.Is(err, frontenddrain.ErrPreparedAckCutMoved)) || attempt+1 == maxLifecycleAckAttempts {
			return err
		}
		if currentRecord.Lifecycle != gateway.FrontendDrainPrepared {
			// An ordinary revision error does not prove that the source floor
			// advanced. Retrying it with the same lifecycle child would send the
			// same full roster again and can spin on an unchanged receiver. A
			// source endpoint that observed a newer exact cut returns the bound
			// CutMoved error instead, which is safe to retry from a fresh source
			// read.
			if !errors.Is(err, frontenddrain.ErrPreparedAckCutMoved) {
				return err
			}
			continue
		}
		refreshed, refreshErr := runtime.refreshPreparedFrontendDrainRosterFence(ctx, node, currentRecord)
		if refreshErr != nil {
			return refreshErr
		}
		if refreshed == currentRecord && !errors.Is(err, frontenddrain.ErrPreparedAckCutMoved) {
			return err
		}
		currentRecord = refreshed
	}
	return gateway.ErrScalingRevision
}

func (runtime *Runtime) acknowledgeFrontendDrainLifecycleRosterOnce(
	ctx context.Context, node gateway.NodeRecord, record gateway.FrontendDrainRecord,
) error {
	// Read one complete source cut and derive both the effective physical
	// roster and the service projection from it. A node-only read would miss a
	// catalog-only generation advance and could pair a new head with old rows.
	source, err := runtime.authority.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil {
		return err
	}
	sourceCut, err := runtime.frontendDrainPreparedAckSourceCutFromRuntimeCut(ctx, source, node, record)
	if err != nil {
		return err
	}
	cut := source.Nodes
	if !cut.Valid() || record.Lifecycle == gateway.FrontendDrainPrepared &&
		(record.ReceiverDirectoryRevision == 0 ||
			record.ReceiverDirectoryRevision != cut.Revision || record.ReceiverDirectoryDigest != cut.Digest ||
			record.ReceiverCatalogGeneration != cut.CatalogGeneration ||
			record.ReceiverCatalogHeadDigest != source.CatalogHeadDigest) {
		return gateway.ErrScalingRevision
	}
	initialFloor := sourceCut.ReadFloor()
	initialCutDigest := sourceCut.Digest()
	physicalReceivers, err := runtime.frontendDrainPreparedAckServingReceiversFromServiceCut(
		cut, source.Catalog, &sourceCut.ServiceDirectory)
	if err != nil {
		return err
	}

	// Gateway services remain a distinct acknowledgement surface. A fused
	// gateway/storage process therefore acknowledges twice: its gateway gate
	// over GatewayControl and its native ReplicatedServer gate over
	// TrafficShardControl. Joining and decommissioned nodes have no serving
	// surface and are the only lifecycle entries skipped here.
	var gatewayRequest frontendDrainPreparedAckRequest
	var gatewayRequestReady bool
	for _, receiver := range cut.CurrentNodes() {
		if receiver.Lifecycle == gateway.NodeDecommissioned || receiver.Lifecycle == gateway.NodeJoining ||
			receiver.Roles&gateway.NodeRoleGateway == 0 {
			continue
		}
		if !gatewayRequestReady {
			requestBase, requestErr := newFrontendParticipantScanRequest(node)
			if requestErr != nil {
				return requestErr
			}
			grantDigest := [32]byte{}
			if record.ContinuationGrant != nil {
				grantDigest = record.ContinuationGrant.GrantDigest
			}
			gatewayRequest = frontendDrainPreparedAckRequest{
				frontendParticipantScanRequest: requestBase,
				DrainID:                        record.DrainID,
				GrantDigest:                    grantDigest,
			}
			gatewayRequestReady = true
		}
		if err := runtime.acknowledgeFrontendDrainPreparedAckGatewayReceiver(
			ctx, node, record, receiver, gatewayRequest,
		); err != nil {
			return err
		}
	}

	for _, receiver := range physicalReceivers {
		request, requestErr := runtime.frontendDrainPreparedAckRequest(ctx, node, record, receiver, sourceCut)
		if requestErr != nil {
			return requestErr
		}
		if err := runtime.acknowledgeFrontendDrainPreparedAckPhysicalReceiver(ctx, receiver, request); err != nil {
			return err
		}
	}
	// Do not expose the Prepared credential after a receiver join/route change
	// raced the final acknowledgement. EnforceFrontendDrain repeats the same
	// directory/head checks inside its catalog mutation; this read keeps the
	// local frontend from entering the brief pre-CAS mixed-roster window too.
	latest, err := runtime.authority.ReadFrontendDrainRuntimeCut(ctx)
	if err != nil {
		return err
	}
	latestCut, latestErr := runtime.frontendDrainPreparedAckSourceCutFromRuntimeCut(ctx, latest, node, record)
	if latestErr != nil || !latestCut.Valid() {
		return gateway.ErrScalingRevision
	}
	if latestCut.ReadFloor() != initialFloor || latestCut.Digest() != initialCutDigest {
		// The final source read is authenticated by the same authority and
		// complete projection as the round's initial read. Mark only a strict
		// scalar-floor advance as moved; an equal-floor digest mismatch is an
		// equivocation/roster inconsistency and must remain terminal for this
		// round rather than becoming a retry loop.
		if latestCut.AtLeastFloor(initialFloor) &&
			frontendDrainPreparedAckCutFloorStrictlyAdvanced(latestCut, initialFloor) {
			return errors.Join(gateway.ErrScalingRevision, frontenddrain.ErrPreparedAckCutMoved)
		}
		return gateway.ErrScalingRevision
	}
	return nil
}

func (runtime *Runtime) acknowledgeFrontendDrainPreparedAckGatewayReceiver(
	ctx context.Context, node gateway.NodeRecord, record gateway.FrontendDrainRecord,
	receiver gateway.NodeRecord, request frontendDrainPreparedAckRequest,
) error {
	if runtime == nil || ctx == nil || runtime.config.TLSProfile == nil ||
		receiver.Lifecycle == gateway.NodeJoining ||
		receiver.Lifecycle == gateway.NodeDecommissioned || receiver.Roles&gateway.NodeRoleGateway == 0 ||
		!receiver.Valid() || !request.valid() {
		return gateway.ErrScalingState
	}
	member := gateway.ClusterCatalogDrainMember{Node: receiver.Gateway.NodeID, Incarnation: receiver.Gateway.Incarnation}
	if member.Node == (rafttransport.NodeID{}) || member.Incarnation == 0 {
		return gateway.ErrScalingIdentity
	}
	local := runtime.config.TLSProfile.LocalIdentity()
	if member.Node == local.Node && receiver.Gateway.ServiceKeyDigest == runtime.config.TLSProfile.LocalServiceKeyDigest() {
		if _, err := runtime.acknowledgePreparedFrontendDrain(ctx, request); err != nil {
			return err
		}
		return nil
	}
	if runtime.clusterControlOpener == nil {
		return gateway.ErrScalingState
	}
	runtime.clusterControlOpener.mu.RLock()
	address, found := runtime.clusterControlOpener.members[member]
	runtime.clusterControlOpener.mu.RUnlock()
	if !found || address != receiver.GatewayAddress {
		return gateway.ErrScalingRevision
	}
	connection, openErr := runtime.clusterControlOpener.OpenGatewayControlMember(ctx, member)
	if openErr != nil || connection == nil {
		return fmt.Errorf("prepared frontend drain receiver %s: %w", nodeIDHex(member.Node), frontendParticipantRemoteError(ctx, openErr))
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	peer := connection.PeerIdentity()
	if connection.TrafficClass() != rafttransport.TrafficGatewayControl || peer.TrustDomain != local.TrustDomain ||
		peer.Node != member.Node || connection.PeerKeyDigest() != [32]byte(receiver.Gateway.ServiceKeyDigest) {
		return gateway.ErrScalingIdentity
	}
	var configuredWriteDeadline time.Time
	if runtime.controlWriteDeadline != nil {
		configuredWriteDeadline = runtime.controlWriteDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredWriteDeadline); deadline.IsZero() {
		return errFrontendParticipantWire
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := writeFrontendParticipantFrame(connection, request.marshal()); err != nil {
		return frontendParticipantRemoteError(ctx, err)
	}
	var configuredReadDeadline time.Time
	if runtime.controlReadDeadline != nil {
		configuredReadDeadline = runtime.controlReadDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredReadDeadline); deadline.IsZero() {
		return errFrontendParticipantWire
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	raw := make([]byte, frontendDrainPreparedAckResponseWireSize)
	if _, err := io.ReadFull(connection, raw); err != nil {
		return frontendParticipantRemoteError(ctx, err)
	}
	response, err := openFrontendDrainPreparedAckResponse(raw, request)
	if err != nil {
		return err
	}
	if response.Revision == 0 {
		return gateway.ErrScalingRevision
	}
	return nil
}
