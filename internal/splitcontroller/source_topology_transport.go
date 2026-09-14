package splitcontroller

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	maxSourceTopologyMetadataBytes = 8 << 10
	maxSourceTopologyNativeBytes   = replication.MaxCommandBytes + (16 << 10)
	maxSourceTopologyReplyBytes    = 1 << 20
	maxSourceTopologyConcurrent    = 16
)

var sourceTopologyMagic = [8]byte{'V', 'S', 'T', 'O', 'P', 'O', 1, 0}
var sourceTopologyReplyMagic = [8]byte{'V', 'S', 'T', 'O', 'P', 'R', 1, 0}

func SourceTopologyRequestDiscriminator() [8]byte { return sourceTopologyMagic }

type SourceTopologyServiceOptions struct {
	Catalog       SourceTopologyCatalog
	Directory     *serviceauthz.ServiceDirectoryGate
	TrustDomain   rafttransport.TrustDomain
	Native        SourceTopologyNativeClient
	Authority     serviceauthz.Authority
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	MaxConcurrent int
}

type SourceTopologyService struct {
	options SourceTopologyServiceOptions
	slots   chan struct{}
}

func NewSourceTopologyService(options SourceTopologyServiceOptions) (*SourceTopologyService, error) {
	if options.Catalog == nil || options.Directory == nil || options.Native == nil || !options.Authority.Valid() ||
		options.ReadDeadline == nil || options.WriteDeadline == nil || options.TrustDomain.ClusterID == ([16]byte{}) ||
		options.TrustDomain.ClusterIncarnation == ([16]byte{}) || options.MaxConcurrent <= 0 || options.MaxConcurrent > maxSourceTopologyConcurrent {
		return nil, ErrSourceTopology
	}
	return &SourceTopologyService{options: options, slots: make(chan struct{}, options.MaxConcurrent)}, nil
}

func (service *SourceTopologyService) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil || connection.TrafficClass() != rafttransport.TrafficGatewayControl ||
		connection.PeerIdentity().TrustDomain != service.options.TrustDomain {
		return ErrSourceTopology
	}
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return ErrSourceTopology
	}
	peer := rafttransport.Binding(connection)
	if !service.authorizedPeer(peer) {
		return ErrSourceTopology
	}
	deadline := service.options.ReadDeadline()
	if deadline.IsZero() {
		return ErrSourceTopology
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if err := sourceTopologyReadDeadline(ctx, connection, service.options.ReadDeadline()); err != nil {
		return err
	}
	request, nonce, err := readSourceTopologyRequest(connection)
	if err != nil {
		return err
	}
	response, executeErr := service.execute(ctx, peer, request)
	if err := sourceTopologyWriteDeadline(ctx, connection, service.options.WriteDeadline()); err != nil {
		return err
	}
	if err := writeSourceTopologyReply(connection, nonce, response, executeErr); err != nil {
		return err
	}
	return executeErr
}

func (service *SourceTopologyService) authorizedPeer(peer rafttransport.PeerBinding) bool {
	return service.options.Directory.CheckBootstrapPeer(serviceauthz.AuthenticatedPeer{Identity: peer.Identity, KeyDigest: peer.ServiceKeyDigest}) == serviceauthz.DecisionAllow
}

func (service *SourceTopologyService) execute(ctx context.Context, peer rafttransport.PeerBinding, request sourceTopologyRequest) (*shardservice.ReplicatedResponse, error) {
	record, err := service.options.Catalog.ReadOperation(ctx, [32]byte(request.Step.Operation))
	if err != nil {
		return nil, errors.Join(ErrSourceTopology, err)
	}
	catalog, head, err := service.options.Catalog.ReadReplicatedCatalogHead(ctx)
	if err != nil {
		return nil, errors.Join(ErrSourceTopology, err)
	}
	directory, err := service.options.Catalog.ReadNodeDirectoryCut(ctx)
	if err != nil {
		return nil, errors.Join(ErrSourceTopology, err)
	}
	route, destination, err := authorizeSourceTopology(record, catalog, directory, peer, request)
	if err != nil || !service.authorizedPeer(peer) {
		return nil, errors.Join(ErrSourceTopology, err)
	}
	authorized, err := serviceauthz.WithAuthority(ctx, service.options.Authority)
	if err != nil {
		return nil, err
	}
	// A process incarnation may advance after catalog publication. Observe the
	// exact source over authenticated native TLS before accepting its claimed
	// incarnation; a stale process cannot borrow a successor's catalog entry.
	for _, endpoint := range route.Replicas {
		if endpoint.Member != request.Source.MemberID {
			continue
		}
		probe, probeErr := service.options.Native.ProbeReplicated(authorized, route, endpoint, serviceauthz.CapabilityTopology)
		if probeErr != nil || !sourceTopologyProbeMatches(probe, route, endpoint, request.Source.NodeIncarnation) {
			return nil, errors.Join(ErrSourceTopology, probeErr)
		}
	}
	probe, err := service.options.Native.ProbeReplicated(authorized, route, destination, serviceauthz.CapabilityTopology)
	if err != nil || probe == nil || !probe.HasState {
		return nil, errors.Join(ErrSourceTopology, err)
	}
	currentIncarnation := probe.State.Fence.NodeIncarnation
	if !sourceTopologyProbeMatches(probe, route, destination, currentIncarnation) || currentIncarnation < destination.NodeIncarnation {
		return nil, ErrSourceTopology
	}
	if request.Native.Operation != shardservice.ReplicatedProbe && currentIncarnation != request.Destination.Incarnation {
		return nil, ErrSourceTopology
	}
	destination.NodeIncarnation = currentIncarnation
	// Recheck the complete authority cut after network observation and before
	// forwarding. A closed/replaced wave or retired storage principal cannot
	// use a previously authenticated connection as residual authority.
	next, err := service.options.Catalog.ReadOperation(ctx, record.ID)
	if err != nil || !sameSourceTopologyRecord(record, next) {
		return nil, errors.Join(ErrSourceTopology, err)
	}
	_, nextHead, err := service.options.Catalog.ReadReplicatedCatalogHead(ctx)
	if err != nil || head != nextHead {
		return nil, errors.Join(ErrSourceTopology, err)
	}
	nextDirectory, err := service.options.Catalog.ReadNodeDirectoryCut(ctx)
	if err != nil || directory.Revision != nextDirectory.Revision || directory.Digest != nextDirectory.Digest || !service.authorizedPeer(peer) {
		return nil, errors.Join(ErrSourceTopology, err)
	}
	if request.Native.Operation == shardservice.ReplicatedProbe {
		return probe, nil
	}
	forward := *request.Native
	forward.Authority = service.options.Authority
	return service.options.Native.DoReplicated(authorized, destination, &forward)
}

func sourceTopologyProbeMatches(response *shardservice.ReplicatedResponse, route gateway.ReplicatedRoute, endpoint gateway.ReplicatedEndpoint, incarnation uint64) bool {
	if response == nil || !response.HasState || response.Kind != shardservice.ReplicatedHandshake || shardservice.ValidateReplicatedResponse(response) != nil {
		return false
	}
	fence := response.State.Fence
	return fence.Group == route.Group && fence.AllocationGeneration == route.AllocationGeneration && fence.Command == route.Command &&
		fence.MemberID == endpoint.Member && fence.StoreID == endpoint.StoreID && fence.NodeIncarnation == incarnation && incarnation >= endpoint.NodeIncarnation
}

func sameSourceTopologyRecord(left, right gateway.ReplicatedOperationRecord) bool {
	return left.ID == right.ID && left.Kind == right.Kind && left.State == right.State && left.Revision == right.Revision &&
		left.CatalogGeneration == right.CatalogGeneration && left.Cursor == right.Cursor && left.Proof == right.Proof &&
		left.IntentDigest == right.IntentDigest && bytes.Equal(left.Intent, right.Intent) && left.ExecutionRevision == right.ExecutionRevision &&
		left.ExecutionSettled == right.ExecutionSettled && bytes.Equal(left.Execution, right.Execution)
}

type SourceTopologyClientOptions struct {
	Opener        nodecontrol.BootstrapReadStreamOpener
	Seeds         []nodecontrol.BootstrapGatewaySeed
	TrustDomain   rafttransport.TrustDomain
	Source        raftmember.RuntimeIdentity
	Step          SourceTopologyStep
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	MaxConcurrent int
}

type SourceTopologyClient struct {
	options SourceTopologyClientOptions
	slots   chan struct{}
	closed  atomic.Bool
}

func NewSourceTopologyClient(options SourceTopologyClientOptions) (*SourceTopologyClient, error) {
	if options.Opener == nil || !options.Step.valid() || options.Source.MemberID == 0 || options.Source.StoreID == ([16]byte{}) ||
		options.Source.NodeIncarnation == 0 || options.Source.Group.ClusterID != options.TrustDomain.ClusterID ||
		options.Source.Group.ClusterIncarnation != options.TrustDomain.ClusterIncarnation || len(options.Seeds) == 0 ||
		len(options.Seeds) > nodecontrol.MaxBootstrapGatewaySeeds || options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.MaxConcurrent <= 0 || options.MaxConcurrent > maxSourceTopologyConcurrent {
		return nil, ErrSourceTopology
	}
	options.Seeds = append([]nodecontrol.BootstrapGatewaySeed(nil), options.Seeds...)
	for _, seed := range options.Seeds {
		if !seed.Valid() {
			return nil, ErrSourceTopology
		}
	}
	return &SourceTopologyClient{options: options, slots: make(chan struct{}, options.MaxConcurrent)}, nil
}

func (client *SourceTopologyClient) Close() error {
	if client != nil {
		client.closed.Store(true)
	}
	return nil
}

func (client *SourceTopologyClient) DoReplicated(ctx context.Context, endpoint gateway.ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	if client == nil || ctx == nil || client.closed.Load() || request == nil {
		return nil, ErrSourceTopology
	}
	select {
	case client.slots <- struct{}{}:
		defer func() { <-client.slots }()
	default:
		return nil, ErrSourceTopology
	}
	wire := sourceTopologyRequest{Step: client.options.Step, Source: client.options.Source,
		Destination: sourceTopologyMember{Node: endpoint.Node, Member: endpoint.Member, Store: endpoint.StoreID, Incarnation: endpoint.NodeIncarnation}, Native: request}
	var last error = ErrSourceTopology
	for _, seed := range client.options.Seeds {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		connection, err := client.options.Opener.OpenBootstrapGatewayControl(ctx, seed)
		if err != nil {
			if connection != nil {
				_ = connection.Close()
			}
			last = err
			continue
		}
		response, err := client.roundTrip(ctx, connection, seed, wire)
		// Once a stream has opened, a lost reply can hide a committed command.
		// Let the native executor retain that ambiguity; a second gateway's
		// definite refusal must not erase the first proposal's unknown outcome.
		if err == nil && response != nil && response.HasState {
			fence := response.State.Fence
			if fence.Group != request.Fence.Group || fence.AllocationGeneration != request.Fence.AllocationGeneration ||
				fence.MemberID != endpoint.Member || fence.StoreID != endpoint.StoreID ||
				fence.NodeIncarnation < endpoint.NodeIncarnation || request.Operation != shardservice.ReplicatedProbe && fence.NodeIncarnation != endpoint.NodeIncarnation {
				return nil, ErrSourceTopology
			}
		}
		return response, err
	}
	return nil, last
}

func (client *SourceTopologyClient) ProbeReplicated(ctx context.Context, route gateway.ReplicatedRoute, endpoint gateway.ReplicatedEndpoint, capability serviceauthz.Capability) (*shardservice.ReplicatedResponse, error) {
	authority, ok := serviceauthz.FromContext(ctx)
	if !ok || capability != serviceauthz.CapabilityTopology {
		return nil, ErrSourceTopology
	}
	return client.DoReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe, Authority: authority, Capability: capability,
		Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration}})
}

func (client *SourceTopologyClient) roundTrip(ctx context.Context, connection rafttransport.PeerConnection, seed nodecontrol.BootstrapGatewaySeed, request sourceTopologyRequest) (*shardservice.ReplicatedResponse, error) {
	if connection == nil {
		return nil, ErrSourceTopology
	}
	defer connection.Close()
	if connection.TrafficClass() != rafttransport.TrafficGatewayControl || connection.PeerIdentity().Node != seed.NodeID ||
		connection.PeerIdentity().TrustDomain != client.options.TrustDomain || replication.Digest(connection.PeerKeyDigest()) != seed.SPKIPinDigest {
		return nil, ErrSourceTopology
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	var nonce [16]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return nil, err
	}
	if err := sourceTopologyWriteDeadline(ctx, connection, client.options.WriteDeadline()); err != nil {
		return nil, err
	}
	if err := writeSourceTopologyRequest(connection, request, nonce); err != nil {
		return nil, err
	}
	if err := sourceTopologyReadDeadline(ctx, connection, client.options.ReadDeadline()); err != nil {
		return nil, err
	}
	return readSourceTopologyReply(connection, nonce)
}

// A fixed header bounds both allocations before decoding. The native request
// remains its original canonical frame; JSON contains only fixed authority
// fields and the two bounded source names, never a dial address or path.
type sourceTopologyMetadata struct {
	Step        SourceTopologyStep
	Source      raftmember.RuntimeIdentity
	Destination sourceTopologyMember
}

func writeSourceTopologyRequest(writer io.Writer, request sourceTopologyRequest, nonce [16]byte) error {
	metadata, err := appendCanonicalVibeJSON(nil, &sourceTopologyMetadata{request.Step, request.Source, request.Destination})
	if err != nil || len(metadata) > maxSourceTopologyMetadataBytes || nonce == ([16]byte{}) {
		return ErrSourceTopology
	}
	var native bytes.Buffer
	if err := shardservice.EncodeReplicatedRequest(&native, request.Native); err != nil {
		return err
	}
	if native.Len() > maxSourceTopologyNativeBytes {
		return ErrSourceTopology
	}
	var header [32]byte
	copy(header[:8], sourceTopologyMagic[:])
	copy(header[8:24], nonce[:])
	binary.BigEndian.PutUint32(header[24:28], uint32(len(metadata)))
	binary.BigEndian.PutUint32(header[28:32], uint32(native.Len()))
	for _, part := range [][]byte{header[:], metadata, native.Bytes()} {
		if err := sourceTopologyWriteFull(writer, part); err != nil {
			return err
		}
	}
	return nil
}

func readSourceTopologyRequest(reader io.Reader) (sourceTopologyRequest, [16]byte, error) {
	var header [32]byte
	var nonce [16]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return sourceTopologyRequest{}, nonce, err
	}
	copy(nonce[:], header[8:24])
	metadataBytes, nativeBytes := binary.BigEndian.Uint32(header[24:28]), binary.BigEndian.Uint32(header[28:32])
	if !bytes.Equal(header[:8], sourceTopologyMagic[:]) || nonce == ([16]byte{}) || metadataBytes == 0 || metadataBytes > maxSourceTopologyMetadataBytes || nativeBytes == 0 || nativeBytes > maxSourceTopologyNativeBytes {
		return sourceTopologyRequest{}, nonce, ErrSourceTopology
	}
	raw := make([]byte, int(metadataBytes)+int(nativeBytes))
	if _, err := io.ReadFull(reader, raw); err != nil {
		return sourceTopologyRequest{}, nonce, err
	}
	var metadata sourceTopologyMetadata
	if err := vibejson.Unmarshal(raw[:metadataBytes], &metadata); err != nil {
		return sourceTopologyRequest{}, nonce, err
	}
	canonical, err := appendCanonicalVibeJSON(nil, &metadata)
	if err != nil || !bytes.Equal(canonical, raw[:metadataBytes]) {
		return sourceTopologyRequest{}, nonce, ErrSourceTopology
	}
	nativeReader := bytes.NewReader(raw[metadataBytes:])
	native, err := shardservice.DecodeReplicatedRequest(nativeReader)
	if err != nil || nativeReader.Len() != 0 {
		return sourceTopologyRequest{}, nonce, errors.Join(ErrSourceTopology, err)
	}
	return sourceTopologyRequest{Step: metadata.Step, Source: metadata.Source, Destination: metadata.Destination, Native: native}, nonce, nil
}

func writeSourceTopologyReply(writer io.Writer, nonce [16]byte, response *shardservice.ReplicatedResponse, failure error) error {
	var body bytes.Buffer
	status := byte(0)
	if failure != nil {
		status = 1
		body.WriteString(ErrSourceTopology.Error())
	} else if err := shardservice.EncodeReplicatedResponse(&body, response); err != nil {
		return err
	}
	if body.Len() > maxSourceTopologyReplyBytes {
		return ErrSourceTopology
	}
	var header [64]byte
	copy(header[:8], sourceTopologyReplyMagic[:])
	copy(header[8:24], nonce[:])
	header[24] = status
	binary.BigEndian.PutUint32(header[28:32], uint32(body.Len()))
	digest := sha256.Sum256(body.Bytes())
	copy(header[32:], digest[:])
	if err := sourceTopologyWriteFull(writer, header[:]); err != nil {
		return err
	}
	return sourceTopologyWriteFull(writer, body.Bytes())
}

func readSourceTopologyReply(reader io.Reader, nonce [16]byte) (*shardservice.ReplicatedResponse, error) {
	var header [64]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[28:32])
	if !bytes.Equal(header[:8], sourceTopologyReplyMagic[:]) || !bytes.Equal(header[8:24], nonce[:]) || header[24] > 1 ||
		header[25] != 0 || header[26] != 0 || header[27] != 0 || size == 0 || size > maxSourceTopologyReplyBytes {
		return nil, ErrSourceTopology
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(body)
	if !bytes.Equal(header[32:], digest[:]) || header[24] != 0 {
		return nil, ErrSourceTopology
	}
	data := bytes.NewReader(body)
	response, err := shardservice.DecodeReplicatedResponse(data)
	if err != nil || data.Len() != 0 {
		return nil, errors.Join(ErrSourceTopology, err)
	}
	return response, nil
}

func sourceTopologyReadDeadline(ctx context.Context, connection rafttransport.PeerConnection, deadline time.Time) error {
	if deadline.IsZero() {
		return ErrSourceTopology
	}
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	return connection.SetReadDeadline(deadline)
}
func sourceTopologyWriteDeadline(ctx context.Context, connection rafttransport.PeerConnection, deadline time.Time) error {
	if deadline.IsZero() {
		return ErrSourceTopology
	}
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	return connection.SetWriteDeadline(deadline)
}
func sourceTopologyWriteFull(writer io.Writer, raw []byte) error {
	for len(raw) > 0 {
		n, err := writer.Write(raw)
		if n > 0 {
			raw = raw[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
