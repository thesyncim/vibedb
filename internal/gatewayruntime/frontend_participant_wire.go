package gatewayruntime

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// frontendParticipantDiscriminator selects the authenticated gateway-control
// participant RPC. It is deliberately separate from catalog-drain envelopes:
// the latter carry only a catalog fence and cannot attest to live frontend
// sessions.
var frontendParticipantDiscriminator = [...]byte{'V', 'B', 'D', 'P', 'A', 'R', 'T', 1}

const (
	frontendParticipantRequestWireSize  = 225
	frontendParticipantResponseWireSize = 297
	frontendParticipantNonceSize        = 16
	frontendParticipantReadTimeout      = 5 * time.Second
)

var (
	errFrontendParticipantWire = errors.New("gatewayruntime: invalid frontend participant wire")
	errFrontendParticipantAuth = errors.New("gatewayruntime: frontend participant authorization failed")
)

type frontendParticipantScanRequest struct {
	Nonce                   [frontendParticipantNonceSize]byte
	NodeID                  rafttransport.NodeID
	Incarnation             uint64
	ServiceKeyDigest        replication.Digest
	NodeRevision            uint64
	CatalogGeneration       uint64
	Lifecycle               gateway.NodeLifecycle
	GatewayNodeID           rafttransport.NodeID
	GatewayIncarnation      uint64
	GatewayServiceKeyDigest replication.Digest
	ServiceID               [16]byte
	SessionID               [16]byte
	SessionRevision         uint64
	ParticipantDigest       replication.Digest
}

func newFrontendParticipantScanRequest(record gateway.NodeRecord) (frontendParticipantScanRequest, error) {
	if !record.Valid() {
		return frontendParticipantScanRequest{}, gateway.ErrInvalidScalingMetadata
	}
	var request frontendParticipantScanRequest
	if _, err := io.ReadFull(cryptorand.Reader, request.Nonce[:]); err != nil {
		return frontendParticipantScanRequest{}, err
	}
	request.NodeID = record.NodeID
	request.Incarnation = record.Incarnation
	request.ServiceKeyDigest = record.ServiceKeyDigest
	request.NodeRevision = record.Revision
	request.CatalogGeneration = record.CatalogGeneration
	request.Lifecycle = record.Lifecycle
	request.GatewayNodeID = record.Gateway.NodeID
	request.GatewayIncarnation = record.Gateway.Incarnation
	request.GatewayServiceKeyDigest = record.Gateway.ServiceKeyDigest
	request.ServiceID = record.Gateway.ServiceID
	request.SessionID = record.Gateway.SessionID
	request.SessionRevision = record.Gateway.SessionRevision
	request.ParticipantDigest = record.Gateway.ParticipantDigest
	return request, nil
}

func (request frontendParticipantScanRequest) valid() bool {
	return request.NodeID != (rafttransport.NodeID{}) && request.Incarnation != 0 &&
		request.ServiceKeyDigest != (replication.Digest{}) && request.NodeRevision != 0 &&
		request.CatalogGeneration != 0 && request.Lifecycle.Valid() &&
		request.GatewayNodeID != (rafttransport.NodeID{}) && request.GatewayIncarnation != 0 &&
		request.GatewayServiceKeyDigest != (replication.Digest{}) && request.ServiceID != ([16]byte{}) &&
		request.SessionID != ([16]byte{}) && request.SessionRevision != 0 &&
		request.ParticipantDigest != (replication.Digest{})
}

func (request frontendParticipantScanRequest) matches(record gateway.NodeRecord) bool {
	return record.Valid() && request.valid() && request.NodeID == record.NodeID &&
		request.Incarnation == record.Incarnation && request.ServiceKeyDigest == record.ServiceKeyDigest &&
		request.NodeRevision == record.Revision && request.CatalogGeneration == record.CatalogGeneration &&
		request.Lifecycle == record.Lifecycle && request.GatewayNodeID == record.Gateway.NodeID &&
		request.GatewayIncarnation == record.Gateway.Incarnation &&
		request.GatewayServiceKeyDigest == record.Gateway.ServiceKeyDigest &&
		request.ServiceID == record.Gateway.ServiceID && request.SessionID == record.Gateway.SessionID &&
		request.SessionRevision == record.Gateway.SessionRevision &&
		request.ParticipantDigest == record.Gateway.ParticipantDigest
}

func (request frontendParticipantScanRequest) marshal() []byte {
	encoded := make([]byte, frontendParticipantRequestWireSize)
	copy(encoded[:8], frontendParticipantDiscriminator[:])
	copy(encoded[8:24], request.Nonce[:])
	copy(encoded[24:40], request.NodeID[:])
	binary.LittleEndian.PutUint64(encoded[40:48], request.Incarnation)
	copy(encoded[48:80], request.ServiceKeyDigest[:])
	binary.LittleEndian.PutUint64(encoded[80:88], request.NodeRevision)
	binary.LittleEndian.PutUint64(encoded[88:96], request.CatalogGeneration)
	encoded[96] = byte(request.Lifecycle)
	copy(encoded[97:113], request.GatewayNodeID[:])
	binary.LittleEndian.PutUint64(encoded[113:121], request.GatewayIncarnation)
	copy(encoded[121:153], request.GatewayServiceKeyDigest[:])
	copy(encoded[153:169], request.ServiceID[:])
	copy(encoded[169:185], request.SessionID[:])
	binary.LittleEndian.PutUint64(encoded[185:193], request.SessionRevision)
	copy(encoded[193:225], request.ParticipantDigest[:])
	return encoded
}

func unmarshalFrontendParticipantScanRequest(encoded []byte) (frontendParticipantScanRequest, error) {
	if len(encoded) != frontendParticipantRequestWireSize {
		return frontendParticipantScanRequest{}, errFrontendParticipantWire
	}
	if string(encoded[:8]) != string(frontendParticipantDiscriminator[:]) {
		return frontendParticipantScanRequest{}, errFrontendParticipantWire
	}
	var request frontendParticipantScanRequest
	copy(request.Nonce[:], encoded[8:24])
	copy(request.NodeID[:], encoded[24:40])
	request.Incarnation = binary.LittleEndian.Uint64(encoded[40:48])
	copy(request.ServiceKeyDigest[:], encoded[48:80])
	request.NodeRevision = binary.LittleEndian.Uint64(encoded[80:88])
	request.CatalogGeneration = binary.LittleEndian.Uint64(encoded[88:96])
	request.Lifecycle = gateway.NodeLifecycle(encoded[96])
	copy(request.GatewayNodeID[:], encoded[97:113])
	request.GatewayIncarnation = binary.LittleEndian.Uint64(encoded[113:121])
	copy(request.GatewayServiceKeyDigest[:], encoded[121:153])
	copy(request.ServiceID[:], encoded[153:169])
	copy(request.SessionID[:], encoded[169:185])
	request.SessionRevision = binary.LittleEndian.Uint64(encoded[185:193])
	copy(request.ParticipantDigest[:], encoded[193:225])
	if !request.valid() {
		return frontendParticipantScanRequest{}, errFrontendParticipantWire
	}
	return request, nil
}

type frontendParticipantScanResponse struct {
	Nonce    [frontendParticipantNonceSize]byte
	Evidence gateway.GatewayParticipantEvidence
}

type frontendParticipantMemberOpener interface {
	OpenGatewayControlMember(context.Context, gateway.ClusterCatalogDrainMember) (rafttransport.PeerConnection, error)
}

type frontendParticipantMemberAddress func(gateway.ClusterCatalogDrainMember) (string, bool)

func (response frontendParticipantScanResponse) valid() bool {
	evidence := response.Evidence
	return evidence.NodeID != (rafttransport.NodeID{}) && evidence.Incarnation != 0 &&
		evidence.ServiceKeyDigest != (replication.Digest{}) && evidence.NodeRevision != 0 &&
		evidence.CatalogGeneration != 0 && evidence.GatewayNodeID != (rafttransport.NodeID{}) &&
		evidence.GatewayIncarnation != 0 && evidence.GatewayServiceKeyDigest != (replication.Digest{}) &&
		evidence.ServiceID != ([16]byte{}) && evidence.SessionID != ([16]byte{}) &&
		evidence.SessionRevision != 0 && evidence.ParticipantDigest != (replication.Digest{}) &&
		evidence.DirectoryRevision != 0 && evidence.Digest != (replication.Digest{})
}

func (response frontendParticipantScanResponse) marshal() []byte {
	encoded := make([]byte, frontendParticipantResponseWireSize)
	copy(encoded[:8], frontendParticipantDiscriminator[:])
	copy(encoded[8:24], response.Nonce[:])
	evidence := response.Evidence
	copy(encoded[24:40], evidence.NodeID[:])
	binary.LittleEndian.PutUint64(encoded[40:48], evidence.Incarnation)
	copy(encoded[48:80], evidence.ServiceKeyDigest[:])
	binary.LittleEndian.PutUint64(encoded[80:88], evidence.NodeRevision)
	binary.LittleEndian.PutUint64(encoded[88:96], evidence.CatalogGeneration)
	copy(encoded[96:112], evidence.GatewayNodeID[:])
	binary.LittleEndian.PutUint64(encoded[112:120], evidence.GatewayIncarnation)
	copy(encoded[120:152], evidence.GatewayServiceKeyDigest[:])
	copy(encoded[152:168], evidence.ServiceID[:])
	copy(encoded[168:184], evidence.SessionID[:])
	binary.LittleEndian.PutUint64(encoded[184:192], evidence.SessionRevision)
	copy(encoded[192:224], evidence.ParticipantDigest[:])
	binary.LittleEndian.PutUint64(encoded[224:232], evidence.DirectoryRevision)
	if evidence.Active {
		encoded[232] = 1
	}
	copy(encoded[233:265], evidence.Digest[:])
	digest := sha256.Sum256(encoded[:265])
	copy(encoded[265:], digest[:])
	return encoded
}

func unmarshalFrontendParticipantScanResponse(encoded []byte) (frontendParticipantScanResponse, error) {
	if len(encoded) != frontendParticipantResponseWireSize ||
		string(encoded[:8]) != string(frontendParticipantDiscriminator[:]) {
		return frontendParticipantScanResponse{}, errFrontendParticipantWire
	}
	expected := sha256.Sum256(encoded[:265])
	if string(expected[:]) != string(encoded[265:]) || encoded[232] > 1 {
		return frontendParticipantScanResponse{}, errFrontendParticipantWire
	}
	var response frontendParticipantScanResponse
	copy(response.Nonce[:], encoded[8:24])
	copy(response.Evidence.NodeID[:], encoded[24:40])
	response.Evidence.Incarnation = binary.LittleEndian.Uint64(encoded[40:48])
	copy(response.Evidence.ServiceKeyDigest[:], encoded[48:80])
	response.Evidence.NodeRevision = binary.LittleEndian.Uint64(encoded[80:88])
	response.Evidence.CatalogGeneration = binary.LittleEndian.Uint64(encoded[88:96])
	copy(response.Evidence.GatewayNodeID[:], encoded[96:112])
	response.Evidence.GatewayIncarnation = binary.LittleEndian.Uint64(encoded[112:120])
	copy(response.Evidence.GatewayServiceKeyDigest[:], encoded[120:152])
	copy(response.Evidence.ServiceID[:], encoded[152:168])
	copy(response.Evidence.SessionID[:], encoded[168:184])
	response.Evidence.SessionRevision = binary.LittleEndian.Uint64(encoded[184:192])
	copy(response.Evidence.ParticipantDigest[:], encoded[192:224])
	response.Evidence.DirectoryRevision = binary.LittleEndian.Uint64(encoded[224:232])
	response.Evidence.Active = encoded[232] != 0
	copy(response.Evidence.Digest[:], encoded[233:265])
	if response.Evidence.NodeID == (rafttransport.NodeID{}) || response.Evidence.Incarnation == 0 ||
		response.Evidence.ServiceKeyDigest == (replication.Digest{}) || response.Evidence.NodeRevision == 0 ||
		response.Evidence.CatalogGeneration == 0 || response.Evidence.GatewayNodeID == (rafttransport.NodeID{}) ||
		response.Evidence.GatewayIncarnation == 0 || response.Evidence.GatewayServiceKeyDigest == (replication.Digest{}) ||
		response.Evidence.ServiceID == ([16]byte{}) || response.Evidence.SessionID == ([16]byte{}) ||
		response.Evidence.SessionRevision == 0 || response.Evidence.ParticipantDigest == (replication.Digest{}) ||
		response.Evidence.DirectoryRevision == 0 || response.Evidence.Digest == (replication.Digest{}) {
		return frontendParticipantScanResponse{}, errFrontendParticipantWire
	}
	return response, nil
}

func frontendParticipantDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		configured = time.Now().Add(frontendParticipantReadTimeout)
	}
	if deadline, found := ctx.Deadline(); found && deadline.Before(configured) {
		return deadline
	}
	return configured
}

func writeFrontendParticipantFrame(connection rafttransport.PeerConnection, frame []byte) error {
	for len(frame) != 0 {
		written, err := connection.Write(frame)
		if written > 0 {
			frame = frame[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func frontendParticipantRemoteError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return errors.Join(ctx.Err(), gateway.ErrScalingRevision, err)
	}
	return errors.Join(gateway.ErrScalingRevision, err)
}

func (runtime *Runtime) scanRemoteGatewayParticipant(
	ctx context.Context, record gateway.NodeRecord,
) (gateway.GatewayParticipantEvidence, error) {
	profile := runtime.config.TLSProfile
	if profile == nil || runtime.clusterControlOpener == nil {
		return gateway.GatewayParticipantEvidence{}, fmt.Errorf(
			"%w: remote gateway participant control is unavailable", gateway.ErrScalingRevision,
		)
	}
	addressOf := func(member gateway.ClusterCatalogDrainMember) (string, bool) {
		runtime.clusterControlOpener.mu.RLock()
		address, found := runtime.clusterControlOpener.members[member]
		runtime.clusterControlOpener.mu.RUnlock()
		return address, found
	}
	return scanRemoteGatewayParticipantOverControl(ctx, record, profile, runtime.clusterControlOpener, addressOf,
		runtime.controlReadDeadline, runtime.controlWriteDeadline)
}

func scanRemoteGatewayParticipantOverControl(
	ctx context.Context, record gateway.NodeRecord, profile *rafttransport.PeerTLS,
	opener frontendParticipantMemberOpener, addressOf frontendParticipantMemberAddress,
	readDeadline, writeDeadline rafttransport.DeadlineFunc,
) (gateway.GatewayParticipantEvidence, error) {
	if ctx == nil || !record.Valid() || profile == nil || opener == nil || addressOf == nil {
		return gateway.GatewayParticipantEvidence{}, fmt.Errorf(
			"%w: remote gateway participant control is unavailable", gateway.ErrScalingRevision,
		)
	}
	member := gateway.ClusterCatalogDrainMember{Node: record.Gateway.NodeID, Incarnation: record.Gateway.Incarnation}
	address, found := addressOf(member)
	if !found || address != record.GatewayAddress {
		return gateway.GatewayParticipantEvidence{}, fmt.Errorf(
			"%w: gateway participant endpoint is not the authenticated directory endpoint", gateway.ErrScalingRevision,
		)
	}
	request, err := newFrontendParticipantScanRequest(record)
	if err != nil {
		return gateway.GatewayParticipantEvidence{}, err
	}
	connection, err := opener.OpenGatewayControlMember(ctx, member)
	if err != nil {
		return gateway.GatewayParticipantEvidence{}, frontendParticipantRemoteError(ctx, err)
	}
	if connection == nil {
		return gateway.GatewayParticipantEvidence{}, fmt.Errorf("%w: nil gateway participant connection", gateway.ErrScalingRevision)
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	peer := connection.PeerIdentity()
	if connection.TrafficClass() != rafttransport.TrafficGatewayControl ||
		peer.TrustDomain != profile.LocalIdentity().TrustDomain || peer.Node != member.Node ||
		replication.Digest(connection.PeerKeyDigest()) != record.Gateway.ServiceKeyDigest {
		return gateway.GatewayParticipantEvidence{}, fmt.Errorf("%w: gateway participant peer binding differs from the directory", gateway.ErrScalingIdentity)
	}
	var configuredWriteDeadline time.Time
	if writeDeadline != nil {
		configuredWriteDeadline = writeDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredWriteDeadline); deadline.IsZero() {
		return gateway.GatewayParticipantEvidence{}, errFrontendParticipantWire
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return gateway.GatewayParticipantEvidence{}, err
	}
	if err := writeFrontendParticipantFrame(connection, request.marshal()); err != nil {
		return gateway.GatewayParticipantEvidence{}, frontendParticipantRemoteError(ctx, err)
	}
	var configuredReadDeadline time.Time
	if readDeadline != nil {
		configuredReadDeadline = readDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredReadDeadline); deadline.IsZero() {
		return gateway.GatewayParticipantEvidence{}, errFrontendParticipantWire
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return gateway.GatewayParticipantEvidence{}, err
	}
	encoded := make([]byte, frontendParticipantResponseWireSize)
	if _, err := io.ReadFull(connection, encoded); err != nil {
		return gateway.GatewayParticipantEvidence{}, frontendParticipantRemoteError(ctx, err)
	}
	response, err := unmarshalFrontendParticipantScanResponse(encoded)
	if err != nil || response.Nonce != request.Nonce || !response.Evidence.ValidFor(record) {
		return gateway.GatewayParticipantEvidence{}, errors.Join(gateway.ErrScalingRevision, errFrontendParticipantWire)
	}
	return response.Evidence, nil
}

func (runtime *Runtime) serveFrontendParticipantConnection(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if runtime == nil || ctx == nil || connection == nil {
		return errFrontendParticipantAuth
	}
	return serveFrontendParticipantConnectionWith(ctx, connection,
		runtime.authorizeFrontendParticipantPeer,
		func(readContext context.Context, node rafttransport.NodeID, incarnation uint64) (gateway.NodeRecord, error) {
			if runtime.authority == nil {
				return gateway.NodeRecord{}, errFrontendParticipantAuth
			}
			return runtime.authority.ReadNode(readContext, node, incarnation)
		}, runtime.scanLocalGatewayParticipant, runtime.controlReadDeadline, runtime.controlWriteDeadline)
}

func serveFrontendParticipantConnectionWith(
	ctx context.Context, connection rafttransport.PeerConnection,
	authorize func(rafttransport.PeerConnection) bool,
	readRecord func(context.Context, rafttransport.NodeID, uint64) (gateway.NodeRecord, error),
	scanLocal func(context.Context, gateway.NodeRecord) (gateway.GatewayParticipantEvidence, error),
	readDeadline, writeDeadline rafttransport.DeadlineFunc,
) error {
	if ctx == nil || connection == nil || authorize == nil || readRecord == nil || scanLocal == nil {
		return errFrontendParticipantAuth
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if !authorize(connection) {
		return errFrontendParticipantAuth
	}
	var configuredReadDeadline time.Time
	if readDeadline != nil {
		configuredReadDeadline = readDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredReadDeadline); deadline.IsZero() {
		return errFrontendParticipantWire
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	requestBytes := make([]byte, frontendParticipantRequestWireSize)
	if _, err := io.ReadFull(connection, requestBytes); err != nil {
		return err
	}
	request, err := unmarshalFrontendParticipantScanRequest(requestBytes)
	if err != nil {
		return err
	}
	record, err := readRecord(ctx, request.NodeID, request.Incarnation)
	if err != nil {
		return errors.Join(gateway.ErrScalingRevision, err)
	}
	if !request.matches(record) {
		return errors.Join(gateway.ErrScalingIdentity, errFrontendParticipantAuth)
	}
	evidence, err := scanLocal(ctx, record)
	if err != nil {
		return err
	}
	response := frontendParticipantScanResponse{Nonce: request.Nonce, Evidence: evidence}
	if !response.valid() || !evidence.ValidFor(record) {
		return errors.Join(gateway.ErrScalingRevision, errFrontendParticipantWire)
	}
	var configuredWriteDeadline time.Time
	if writeDeadline != nil {
		configuredWriteDeadline = writeDeadline()
	}
	if deadline := frontendParticipantDeadline(ctx, configuredWriteDeadline); deadline.IsZero() {
		return errFrontendParticipantWire
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return writeFrontendParticipantFrame(connection, response.marshal())
}

func (runtime *Runtime) authorizeFrontendParticipantPeer(connection rafttransport.PeerConnection) bool {
	if runtime == nil || connection == nil || connection.TrafficClass() != rafttransport.TrafficGatewayControl ||
		runtime.config.TLSProfile == nil || connection.PeerIdentity().TrustDomain != runtime.config.TLSProfile.LocalIdentity().TrustDomain {
		return false
	}
	peer := connection.PeerIdentity()
	runtime.controlRosterMu.RLock()
	_, rostered := runtime.controlRoster[peer.Node]
	runtime.controlRosterMu.RUnlock()
	if !rostered || runtime.config.Authorization == nil ||
		runtime.config.Authorization.Check(peer.Node, serviceauthz.CapabilityTopology) != serviceauthz.DecisionAllow {
		return false
	}
	if runtime.serviceDirectory != nil {
		binding := rafttransport.Binding(connection)
		if runtime.serviceDirectory.CheckGatewayPeer(serviceauthz.AuthenticatedPeer{
			Identity: binding.Identity, KeyDigest: binding.ServiceKeyDigest,
		}) != serviceauthz.DecisionAllow {
			return false
		}
	}
	return true
}
