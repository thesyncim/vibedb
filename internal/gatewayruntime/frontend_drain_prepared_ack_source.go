package gatewayruntime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var (
	errFrontendDrainPreparedAckSourceWire  = errors.New("gatewayruntime: invalid frontend drain source-cut wire")
	errFrontendDrainPreparedAckSourceAuth  = errors.New("gatewayruntime: frontend drain source-cut authorization failed")
	errFrontendDrainPreparedAckSourceState = errors.New("gatewayruntime: frontend drain source-cut state mismatch")
)

// FrontendDrainPreparedAckCutReadServiceOptions configures the canonical
// source-cut reader on a physical control listener. ReadCut must return one
// complete authority cut from a single serving epoch; the service never
// accepts directory or catalog coordinates from the caller.
type FrontendDrainPreparedAckCutReadServiceOptions struct {
	Authorize              func(rafttransport.PeerConnection) bool
	ReadCut                func(context.Context) (gateway.FrontendDrainRuntimeCut, error)
	Profile                *rafttransport.PeerTLS
	PolicyGeneration       uint64
	TrafficClass           rafttransport.TrafficClass
	SourceNode             rafttransport.NodeID
	SourceIncarnation      uint64
	SourceServiceKeyDigest [32]byte
	ReadDeadline           rafttransport.DeadlineFunc
	WriteDeadline          rafttransport.DeadlineFunc
}

// FrontendDrainPreparedAckCutReadService is the fixed canonical source
// protocol used by a storage process before an embedded gateway has opened.
// Its source identity is checked against the returned node directory, so a
// static gateway role or an echoed sender cut cannot satisfy this endpoint.
type FrontendDrainPreparedAckCutReadService struct {
	options FrontendDrainPreparedAckCutReadServiceOptions
}

func NewFrontendDrainPreparedAckCutReadService(
	options FrontendDrainPreparedAckCutReadServiceOptions,
) (*FrontendDrainPreparedAckCutReadService, error) {
	if options.Authorize == nil || options.ReadCut == nil || options.Profile == nil ||
		options.PolicyGeneration == 0 || options.TrafficClass == 0 ||
		options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.SourceNode == (rafttransport.NodeID{}) || options.SourceIncarnation == 0 ||
		options.SourceServiceKeyDigest == ([32]byte{}) {
		return nil, errFrontendDrainPreparedAckSourceAuth
	}
	if options.TrafficClass != rafttransport.TrafficShardControl &&
		options.TrafficClass != rafttransport.TrafficGatewayControl {
		return nil, errFrontendDrainPreparedAckSourceAuth
	}
	return &FrontendDrainPreparedAckCutReadService{options: options}, nil
}

func (service *FrontendDrainPreparedAckCutReadService) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if service == nil {
		return errFrontendDrainPreparedAckSourceAuth
	}
	return serveFrontendDrainPreparedAckCutReadConnectionWithOptions(ctx, connection,
		frontendDrainPreparedAckCutReadConnectionOptions{
			authorize:              service.options.Authorize,
			readCut:                service.options.ReadCut,
			profile:                service.options.Profile,
			policyGeneration:       service.options.PolicyGeneration,
			trafficClass:           service.options.TrafficClass,
			requireCurrentGateway:  false,
			sourceNode:             service.options.SourceNode,
			sourceIncarnation:      service.options.SourceIncarnation,
			sourceServiceKeyDigest: service.options.SourceServiceKeyDigest,
			readDeadline:           service.options.ReadDeadline,
			writeDeadline:          service.options.WriteDeadline,
		})
}

// authorizeFrontendDrainPreparedAckCutReadPeer admits only an authenticated
// physical storage identity. The source-cut reader is a gateway-control
// endpoint, but a gateway principal or a role-only roster entry cannot ask it
// to manufacture a receiver proof.
func (runtime *Runtime) authorizeFrontendDrainPreparedAckCutReadPeer(
	connection rafttransport.PeerConnection,
) bool {
	if runtime == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficGatewayControl ||
		runtime.config.TLSProfile == nil || runtime.authority == nil ||
		runtime.serviceDirectory == nil {
		return false
	}
	peer := connection.PeerIdentity()
	if peer.TrustDomain != runtime.config.TLSProfile.LocalIdentity().TrustDomain ||
		peer.Node == (rafttransport.NodeID{}) || connection.PeerKeyDigest() == ([32]byte{}) {
		return false
	}
	return runtime.serviceDirectory.CheckBootstrapPeer(serviceauthz.AuthenticatedPeer{
		Identity: peer, KeyDigest: connection.PeerKeyDigest(),
	}) == serviceauthz.DecisionAllow
}

// serveFrontendDrainPreparedAckCutReadConnection returns the source gateway's
// complete canonical cut to one physical receiver. Every coordinate in the
// query is checked against the authenticated peer and the authority before
// the response is assembled; the source cut is never copied from the query.
func (runtime *Runtime) serveFrontendDrainPreparedAckCutReadConnection(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if runtime == nil || ctx == nil || connection == nil ||
		runtime.authority == nil || runtime.config.TLSProfile == nil || runtime.config.Authorization == nil {
		return errFrontendDrainPreparedAckSourceAuth
	}
	return serveFrontendDrainPreparedAckCutReadConnectionWith(ctx, connection,
		runtime.authorizeFrontendDrainPreparedAckCutReadPeer,
		runtime.authority.ReadNode, runtime.authority.ReadFrontendDrainRuntimeCut,
		runtime.config.TLSProfile, runtime.config.Authorization.Generation(),
		runtime.controlReadDeadline, runtime.controlWriteDeadline)
}

func serveFrontendDrainPreparedAckCutReadConnectionWith(
	ctx context.Context, connection rafttransport.PeerConnection,
	authorize func(rafttransport.PeerConnection) bool,
	readNode func(context.Context, rafttransport.NodeID, uint64) (gateway.NodeRecord, error),
	readCut func(context.Context) (gateway.FrontendDrainRuntimeCut, error),
	profile *rafttransport.PeerTLS, policyGeneration uint64,
	readDeadline, writeDeadline rafttransport.DeadlineFunc,
) error {
	if readNode == nil {
		return errFrontendDrainPreparedAckSourceAuth
	}
	return serveFrontendDrainPreparedAckCutReadConnectionWithOptions(ctx, connection,
		frontendDrainPreparedAckCutReadConnectionOptions{
			authorize: authorize, readNode: readNode, readCut: readCut,
			profile: profile, policyGeneration: policyGeneration,
			trafficClass:          rafttransport.TrafficGatewayControl,
			requireCurrentGateway: true, readDeadline: readDeadline, writeDeadline: writeDeadline,
		})
}

type frontendDrainPreparedAckCutReadConnectionOptions struct {
	authorize              func(rafttransport.PeerConnection) bool
	readNode               func(context.Context, rafttransport.NodeID, uint64) (gateway.NodeRecord, error)
	readCut                func(context.Context) (gateway.FrontendDrainRuntimeCut, error)
	profile                *rafttransport.PeerTLS
	policyGeneration       uint64
	trafficClass           rafttransport.TrafficClass
	requireCurrentGateway  bool
	sourceNode             rafttransport.NodeID
	sourceIncarnation      uint64
	sourceServiceKeyDigest [32]byte
	readDeadline           rafttransport.DeadlineFunc
	writeDeadline          rafttransport.DeadlineFunc
}

func serveFrontendDrainPreparedAckCutReadConnectionWithOptions(
	ctx context.Context, connection rafttransport.PeerConnection,
	options frontendDrainPreparedAckCutReadConnectionOptions,
) error {
	if ctx == nil || connection == nil || options.authorize == nil || options.readCut == nil ||
		options.profile == nil || options.policyGeneration == 0 ||
		(options.trafficClass != 0 && connection.TrafficClass() != options.trafficClass) ||
		!options.authorize(connection) {
		return errFrontendDrainPreparedAckSourceAuth
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	var configuredReadDeadline time.Time
	if options.readDeadline != nil {
		configuredReadDeadline = options.readDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredReadDeadline); deadline.IsZero() {
		return errFrontendDrainPreparedAckSourceWire
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	requestBytes := make([]byte, frontenddrain.PreparedAckCutReadRequestBytes)
	if _, err := io.ReadFull(connection, requestBytes); err != nil {
		return err
	}
	request, err := frontenddrain.OpenPreparedAckCutReadRequest(requestBytes)
	if err != nil {
		return errors.Join(errFrontendDrainPreparedAckSourceWire, err)
	}
	peer := connection.PeerIdentity()
	peerKey := connection.PeerKeyDigest()
	if request.ReceiverNode != peer.Node || request.ReceiverServiceKeyDigest != peerKey ||
		request.ReceiverIncarnation == 0 {
		return errFrontendDrainPreparedAckSourceAuth
	}
	operation := request.Operation
	var node gateway.NodeRecord
	if options.readNode != nil {
		var err error
		node, err = options.readNode(ctx, request.ReceiverNode, request.ReceiverIncarnation)
		if err != nil {
			return errors.Join(errFrontendDrainPreparedAckSourceState, err)
		}
	}
	source, err := options.readCut(ctx)
	if err != nil {
		return errors.Join(errFrontendDrainPreparedAckSourceState, err)
	}
	if options.readNode == nil {
		found := false
		for _, candidate := range source.Nodes.Nodes {
			if candidate.NodeID == request.ReceiverNode && candidate.Incarnation == request.ReceiverIncarnation {
				if found {
					return errFrontendDrainPreparedAckSourceState
				}
				node, found = candidate, true
			}
		}
		if !found {
			return errFrontendDrainPreparedAckSourceState
		}
	}
	if !node.Valid() || node.NodeID != request.ReceiverNode ||
		node.Incarnation != request.ReceiverIncarnation ||
		node.ServiceKeyDigest != replication.Digest(request.ReceiverServiceKeyDigest) ||
		node.Roles&gateway.NodeRoleStorage == 0 || node.Lifecycle == gateway.NodeDecommissioned ||
		(operation == frontenddrain.CutOperationInstallExact &&
			(node.Lifecycle != gateway.NodeActive && node.Lifecycle != gateway.NodeDraining)) ||
		(operation == frontenddrain.CutOperationReadLatest &&
			(node.Lifecycle != gateway.NodeJoining && node.Lifecycle != gateway.NodeActive && node.Lifecycle != gateway.NodeDraining)) ||
		(operation == frontenddrain.CutOperationInstallExact &&
			(request.ReceiverNodeRevision == 0 || node.Revision != request.ReceiverNodeRevision)) {
		return errFrontendDrainPreparedAckSourceState
	}
	if !source.Nodes.Valid() || source.Catalog == nil ||
		source.CatalogHeadDigest == (replication.Digest{}) ||
		source.Catalog.Generation() != source.Nodes.CatalogGeneration {
		return errFrontendDrainPreparedAckSourceState
	}
	if !sourceContainsExactNode(source, node) {
		return errFrontendDrainPreparedAckSourceState
	}
	if !options.requireCurrentGateway &&
		(options.sourceNode == (rafttransport.NodeID{}) || options.sourceIncarnation == 0 ||
			options.sourceServiceKeyDigest == ([32]byte{}) ||
			!sourceContainsCanonicalCatalogSource(source, options.sourceNode, options.sourceIncarnation, options.sourceServiceKeyDigest) ||
			!sourceRosterContains(source, options.sourceNode, options.sourceIncarnation, options.sourceServiceKeyDigest)) {
		return errFrontendDrainPreparedAckSourceAuth
	}
	serviceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		ctx, source, options.profile, options.policyGeneration)
	if err != nil {
		return errors.Join(errFrontendDrainPreparedAckSourceState, err)
	}
	if options.requireCurrentGateway && !serviceCutContainsCurrentGateway(serviceCut, options.profile) {
		return errFrontendDrainPreparedAckSourceAuth
	}
	if !serviceCutContainsActiveStorage(serviceCut, node, peerKey) {
		return errFrontendDrainPreparedAckSourceState
	}
	if !frontendDrainPreparedAckSourceSubjectMatches(source, request) ||
		!frontendDrainPreparedAckServiceSubjectMatches(serviceCut, request) {
		return errFrontendDrainPreparedAckSourceState
	}
	cut := frontenddrain.PreparedAckCut{
		DirectoryRevision: source.Nodes.Revision, DirectoryDigest: source.Nodes.Digest,
		CatalogGeneration: source.Nodes.CatalogGeneration, CatalogHeadDigest: source.CatalogHeadDigest,
		ServiceDirectoryRevision: serviceCut.Revision, ServiceDirectory: serviceCut,
		SourceRoster: frontendDrainPreparedAckSourceRoster(source),
		Subjects:     frontendDrainPreparedAckSubjects(source),
	}
	if !cut.Valid() || !frontendDrainPreparedAckCutAtLeastFloor(cut, request.SourceFloor) {
		return errFrontendDrainPreparedAckSourceState
	}
	if operation == frontenddrain.CutOperationInstallExact && cut.Digest() != request.SourceCutDigest {
		return writeFrontendDrainPreparedAckCutReadMovedResponse(ctx, connection, request, cut, options.writeDeadline)
	}
	response := frontenddrain.PreparedAckCutReadResponse{
		Operation: operation, RequirePrepared: request.RequirePrepared,
		Nonce: request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
		ReceiverNodeRevision:     node.Revision, DirectoryRevision: cut.DirectoryRevision,
		DirectoryDigest: cut.DirectoryDigest, CatalogGeneration: cut.CatalogGeneration,
		CatalogHeadDigest: cut.CatalogHeadDigest, ServiceDirectoryRevision: cut.ServiceDirectoryRevision,
		ServiceDirectoryDigest: cut.ServiceDirectoryDigestValue(), SourceCutDigest: cut.Digest(), Cut: cut,
	}
	encoded, err := response.Marshal()
	if err != nil || !response.Valid(request) {
		return errors.Join(errFrontendDrainPreparedAckSourceWire, err)
	}
	var configuredWriteDeadline time.Time
	if options.writeDeadline != nil {
		configuredWriteDeadline = options.writeDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredWriteDeadline); deadline.IsZero() {
		return errFrontendDrainPreparedAckSourceWire
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return writeFrontendParticipantFrame(connection, encoded)
}

func writeFrontendDrainPreparedAckCutReadMovedResponse(
	ctx context.Context, connection rafttransport.PeerConnection,
	request frontenddrain.PreparedAckCutReadRequest, cut frontenddrain.PreparedAckCut,
	writeDeadline rafttransport.DeadlineFunc,
) error {
	if ctx == nil || connection == nil || !request.Valid() || request.Operation != frontenddrain.CutOperationInstallExact ||
		!cut.Valid() || !cut.AtLeastFloor(request.SourceFloor) || cut.Digest() == request.SourceCutDigest ||
		!frontendDrainPreparedAckCutFloorStrictlyAdvanced(cut, request.SourceFloor) {
		return errFrontendDrainPreparedAckSourceState
	}
	response := frontenddrain.PreparedAckCutReadMovedResponse{
		Operation: frontenddrain.CutOperationInstallExact, RequirePrepared: request.RequirePrepared,
		Nonce: request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
		ReceiverNodeRevision:     request.ReceiverNodeRevision, SourceFloor: cut.ReadFloor(), SourceCutDigest: cut.Digest(),
	}
	encoded, err := response.Marshal()
	if err != nil || !response.Valid(request) {
		return errors.Join(errFrontendDrainPreparedAckSourceWire, err)
	}
	var configuredWriteDeadline time.Time
	if writeDeadline != nil {
		configuredWriteDeadline = writeDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredWriteDeadline); deadline.IsZero() {
		return errFrontendDrainPreparedAckSourceWire
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return writeFrontendParticipantFrame(connection, encoded)
}

func frontendDrainPreparedAckCutFloorStrictlyAdvanced(
	cut frontenddrain.PreparedAckCut, prior frontenddrain.PreparedAckCutReadFloor,
) bool {
	if !cut.Valid() {
		return false
	}
	if prior == (frontenddrain.PreparedAckCutReadFloor{}) {
		return true
	}
	current := cut.ReadFloor()
	return current.DirectoryRevision > prior.DirectoryRevision ||
		current.CatalogGeneration > prior.CatalogGeneration ||
		current.ServiceDirectoryRevision > prior.ServiceDirectoryRevision
}

func frontendDrainPreparedAckSourceRoster(source gateway.FrontendDrainRuntimeCut) []frontenddrain.PreparedAckSource {
	if !source.Nodes.Valid() {
		return nil
	}
	result := make([]frontenddrain.PreparedAckSource, 0, len(source.Nodes.CurrentNodes()))
	for _, node := range source.Nodes.CurrentNodes() {
		if node.Roles&(gateway.NodeRoleStorage|gateway.NodeRoleCatalog) !=
			(gateway.NodeRoleStorage|gateway.NodeRoleCatalog) ||
			(node.Lifecycle != gateway.NodeActive && node.Lifecycle != gateway.NodeDraining) {
			continue
		}
		result = append(result, frontenddrain.PreparedAckSource{
			NodeID: node.NodeID, Incarnation: node.Incarnation, ControlAddress: node.ControlAddress,
			SPKIPinDigest: [32]byte(node.ServiceKeyDigest),
		})
	}
	if len(result) == 0 {
		return nil
	}
	slices.SortFunc(result, func(left, right frontenddrain.PreparedAckSource) int {
		return bytes.Compare(left.NodeID[:], right.NodeID[:])
	})
	return result
}

func sourceContainsExactNode(source gateway.FrontendDrainRuntimeCut, want gateway.NodeRecord) bool {
	for _, node := range source.Nodes.Nodes {
		if node.NodeID == want.NodeID && node.Incarnation == want.Incarnation {
			return node == want
		}
	}
	return false
}

func sourceContainsCanonicalCatalogSource(
	source gateway.FrontendDrainRuntimeCut, nodeID rafttransport.NodeID,
	incarnation uint64, key [32]byte,
) bool {
	found := false
	for _, node := range source.Nodes.Nodes {
		if node.NodeID != nodeID || node.Incarnation != incarnation {
			continue
		}
		if found || !node.Valid() || node.ServiceKeyDigest != replication.Digest(key) ||
			node.Roles&(gateway.NodeRoleStorage|gateway.NodeRoleCatalog) !=
				(gateway.NodeRoleStorage|gateway.NodeRoleCatalog) ||
			(node.Lifecycle != gateway.NodeActive && node.Lifecycle != gateway.NodeDraining) {
			return false
		}
		found = true
	}
	return found
}

func sourceRosterContains(
	source gateway.FrontendDrainRuntimeCut, nodeID rafttransport.NodeID,
	incarnation uint64, key [32]byte,
) bool {
	for _, candidate := range frontendDrainPreparedAckSourceRoster(source) {
		if candidate.NodeID == nodeID && candidate.Incarnation == incarnation &&
			candidate.SPKIPinDigest == key {
			return true
		}
	}
	return false
}

func serviceCutContainsActiveStorage(
	cut serviceauthz.ServiceDirectoryCut, node gateway.NodeRecord, peerKey [32]byte,
) bool {
	for _, binding := range cut.Bindings {
		if binding.Principal == node.NodeID && binding.PhysicalNode == node.NodeID &&
			binding.PhysicalIncarnation == node.Incarnation && binding.KeyDigest == peerKey &&
			binding.Roles&serviceauthz.ServiceRoleStorage != 0 &&
			(binding.Lifecycle == serviceauthz.ServiceJoining || binding.Lifecycle == serviceauthz.ServiceActive ||
				binding.Lifecycle == serviceauthz.ServiceDraining) {
			return true
		}
	}
	return false
}

func serviceCutContainsCurrentGateway(cut serviceauthz.ServiceDirectoryCut, profile *rafttransport.PeerTLS) bool {
	if profile == nil {
		return false
	}
	identity := profile.LocalIdentity()
	key := [32]byte(profile.LocalServiceKeyDigest())
	for _, binding := range cut.Bindings {
		if binding.Principal == identity.Node && binding.KeyDigest == key &&
			binding.Roles&serviceauthz.ServiceRoleGateway != 0 &&
			(binding.Lifecycle == serviceauthz.ServiceActive || binding.Lifecycle == serviceauthz.ServiceDraining) &&
			binding.GatewayIncarnation != 0 {
			return true
		}
	}
	return false
}

func frontendDrainPreparedAckSourceSubjectMatches(
	source gateway.FrontendDrainRuntimeCut, request frontenddrain.PreparedAckCutReadRequest,
) bool {
	if request.DrainID == ([32]byte{}) {
		return true
	}
	for _, record := range source.DrainRecords {
		if record.DrainID != request.DrainID {
			continue
		}
		if !record.Valid() {
			return false
		}
		if request.RequirePrepared && record.Lifecycle != gateway.FrontendDrainPrepared {
			return false
		}
		if request.GrantDigest != ([32]byte{}) {
			return record.ContinuationGrant != nil && record.ContinuationGrant.GrantDigest == request.GrantDigest
		}
		return record.ContinuationGrant == nil
	}
	if request.GrantDigest != ([32]byte{}) {
		for _, grant := range source.ContinuationGrants {
			if grant.GrantDigest == request.GrantDigest {
				return grant.Valid() && grant.DrainID == request.DrainID &&
					(!request.RequirePrepared || grant.State == serviceauthz.ContinuationGrantPrepared)
			}
		}
	}
	return false
}

func frontendDrainPreparedAckServiceSubjectMatches(
	cut serviceauthz.ServiceDirectoryCut, request frontenddrain.PreparedAckCutReadRequest,
) bool {
	if request.DrainID == ([32]byte{}) {
		return true
	}
	if request.GrantDigest == ([32]byte{}) {
		// The source child is checked against the authority-owned drain record
		// above; its compact proof is copied into the returned PreparedAckCut.
		return true
	}
	for _, grant := range cut.ContinuationGrants {
		if grant.GrantDigest == request.GrantDigest {
			return grant.Valid() && grant.DrainID == request.DrainID
		}
	}
	return false
}

func frontendDrainPreparedAckSubjects(source gateway.FrontendDrainRuntimeCut) []frontenddrain.PreparedAckSubject {
	result := make([]frontenddrain.PreparedAckSubject, 0, len(source.DrainRecords))
	for _, record := range source.DrainRecords {
		if !record.Valid() {
			continue
		}
		result = append(result, frontenddrain.PreparedAckSubject{
			DrainID: record.DrainID, PhysicalNode: record.PhysicalNode,
			PhysicalIncarnation: record.PhysicalIncarnation, GatewayServiceID: record.GatewayServiceID,
			GatewayIncarnation: record.GatewayIncarnation, GatewaySessionID: record.GatewaySessionID,
			GatewaySessionRevision: record.GatewaySessionRevision, NodeRevision: record.NodeRevision,
			Lifecycle: uint8(record.Lifecycle), DrainFenceDigest: [32]byte(record.DrainFence.FenceDigest),
		})
	}
	slices.SortFunc(result, func(left, right frontenddrain.PreparedAckSubject) int {
		return bytes.Compare(left.DrainID[:], right.DrainID[:])
	})
	return result
}

func frontendDrainPreparedAckCutAtLeastFloor(
	cut frontenddrain.PreparedAckCut, floor frontenddrain.PreparedAckCutReadFloor,
) bool {
	return cut.AtLeastFloor(floor)
}

func preparedAckSourceGrant(
	source gateway.FrontendDrainRuntimeCut, drainID, grantDigest [32]byte,
) bool {
	for _, grant := range source.ContinuationGrants {
		if grant.GrantDigest == grantDigest {
			return grant.Valid() && grant.DrainID == drainID &&
				grant.State == serviceauthz.ContinuationGrantPrepared
		}
	}
	return false
}

func preparedAckServiceCutGrant(
	cut serviceauthz.ServiceDirectoryCut, drainID, grantDigest [32]byte,
) bool {
	for _, grant := range cut.ContinuationGrants {
		if grant.GrantDigest == grantDigest {
			return grant.Valid() && grant.DrainID == drainID &&
				grant.State == serviceauthz.ContinuationGrantPrepared
		}
	}
	return false
}
