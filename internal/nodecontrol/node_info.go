package nodecontrol

// NodeInfo is the read-only physical-node readiness protocol.  It is kept
// separate from enrollment control because a controller may inspect a node's
// durable identity and measured capacity before it has a group roster.  The
// provider is the only source of these facts; the wire service never turns an
// unavailable measurement into a zero-valued ready node.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
)

var (
	ErrNodeInfo             = errors.New("nodecontrol: invalid node-info request")
	ErrNodeInfoUnauthorized = errors.New("nodecontrol: node-info request is unauthorized")
	ErrNodeInfoStale        = errors.New("nodecontrol: node-info observation is stale")
	ErrNodeInfoUnavailable  = errors.New("nodecontrol: node-info observation is unavailable")
	ErrNodeInfoConflict     = errors.New("nodecontrol: node-info observation conflicts")
	ErrNodeInfoBound        = errors.New("nodecontrol: node-info concurrency bound exceeded")
)

const (
	NodeInfoMaxReplyBytes      = 64 << 10
	NodeInfoMaxEndpointBytes   = 1024
	NodeInfoMaxConcurrent      = 64
	nodeInfoVersion             = 1
	nodeInfoRequestHeaderBytes  = 64
	nodeInfoResponseHeaderBytes = 64
	nodeInfoNonceBytes          = 16
	nodeInfoResponseSuccess     = 1
)

var (
	nodeInfoRequestMagic  = [8]byte{'V', 'D', 'N', 'I', 'N', 'F', 'Q', 0}
	nodeInfoResponseMagic = [8]byte{'V', 'D', 'N', 'I', 'N', 'F', 'R', 0}
)

// NodeInfoOperation is deliberately a one-value grammar. A request captured
// on another shard-control route must never become a readiness read by
// changing only the discriminator.
type NodeInfoOperation uint8

const OpNodeInfo NodeInfoOperation = 1

func (operation NodeInfoOperation) valid() bool { return operation == OpNodeInfo }

// NodeInfoRequest identifies one physical-node observation cut. A zero
// MinimumInventoryRevision accepts the current bounded inventory; a nonzero
// value makes a controller reject a reply that has not observed its prior
// durable inventory publication.
type NodeInfoRequest struct {
	Nonce                    [nodeInfoNonceBytes]byte
	Operation                NodeInfoOperation
	NodeID                   rafttransport.NodeID
	Incarnation              uint64
	MinimumInventoryRevision uint64
}

func (request NodeInfoRequest) valid() bool {
	return request.Nonce != ([nodeInfoNonceBytes]byte{}) && request.Operation.valid() &&
		request.NodeID != (rafttransport.NodeID{}) && request.Incarnation != 0
}

// NodeInfoStoreIdentity is copied from the opened physical node store. It is
// not inferred from a group descriptor, which is absent on an empty node.
type NodeInfoStoreIdentity struct {
	ClusterID          [16]byte `json:"cluster_id"`
	ClusterIncarnation [16]byte `json:"cluster_incarnation"`
	NodeID             [16]byte `json:"node_id"`
}

func (identity NodeInfoStoreIdentity) valid(node rafttransport.NodeID) bool {
	return identity.ClusterID != ([16]byte{}) && identity.ClusterIncarnation != ([16]byte{}) &&
		identity.NodeID != ([16]byte{}) && rafttransport.NodeID(identity.NodeID) == node
}

func NodeInfoStoreIdentityFromNodeStore(store *raftstore.NodeStore) (NodeInfoStoreIdentity, error) {
	if store == nil {
		return NodeInfoStoreIdentity{}, ErrNodeInfoUnavailable
	}
	identity := store.NodeIdentity()
	result := NodeInfoStoreIdentity{ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation, NodeID: identity.NodeID}
	if !result.valid(rafttransport.NodeID(identity.NodeID)) {
		return NodeInfoStoreIdentity{}, ErrNodeInfoUnavailable
	}
	return result, nil
}

// NodeInfoEndpoints names the public listener coordinates currently bound by
// the process. Values are addresses, not authority: TLS peer identity and the
// response's physical identity remain the source of authorization.
type NodeInfoEndpoints struct {
	Peer     string `json:"peer"`
	Native   string `json:"native"`
	Snapshot string `json:"snapshot"`
	Control  string `json:"control"`
	Gateway  string `json:"gateway,omitempty"`
}

func (endpoints NodeInfoEndpoints) valid() bool {
	for _, address := range []string{endpoints.Peer, endpoints.Native, endpoints.Snapshot, endpoints.Control} {
		if !nodeInfoAddressValid(address) {
			return false
		}
	}
	return endpoints.Gateway == "" || nodeInfoAddressValid(endpoints.Gateway)
}

func nodeInfoAddressValid(address string) bool {
	if len(address) == 0 || len(address) > NodeInfoMaxEndpointBytes || bytes.IndexByte([]byte(address), 0) >= 0 {
		return false
	}
	if host, port, err := net.SplitHostPort(address); err == nil {
		return host != "" && port != ""
	}
	// Unix listener paths are valid public coordinates in local deployments;
	// they remain bounded and are never used as a peer identity.
	return bytes.IndexByte([]byte(address), '\r') < 0 && bytes.IndexByte([]byte(address), '\n') < 0
}

// NodeInfoReadiness reports independently durable gates. A node may report
// serving or reserved groups while one gate is false during a restart; the
// controller must inspect every field before promoting Joining to Active.
type NodeInfoReadiness struct {
	NodeJournalReady    bool `json:"node_journal_ready"`
	PhysicalStoreReady  bool `json:"physical_store_ready"`
	BoundListenersReady bool `json:"bound_listeners_ready"`
}

func (readiness NodeInfoReadiness) ready() bool {
	return readiness.NodeJournalReady && readiness.PhysicalStoreReady && readiness.BoundListenersReady
}

// NodeInfoObservation is one detached physical-node cut. Declared capacity
// is the operator/catalog limit; actual capacity and usage come from the
// opened store's sealed measurement source. The two are carried separately so
// readiness cannot be inferred from a copied manifest alone.
type NodeInfoObservation struct {
	Nonce                     [nodeInfoNonceBytes]byte `json:"nonce"`
	Operation                 NodeInfoOperation        `json:"operation"`
	NodeID                    rafttransport.NodeID    `json:"node_id"`
	Incarnation               uint64                  `json:"incarnation"`
	SPKIPinDigest             replication.Digest      `json:"spki_pin_digest"`
	Store                     NodeInfoStoreIdentity   `json:"store"`
	Endpoints                 NodeInfoEndpoints       `json:"endpoints"`
	Readiness                 NodeInfoReadiness       `json:"readiness"`
	ServingGroups             uint32                  `json:"serving_groups"`
	ReservedGroups            uint32                  `json:"reserved_groups"`
	InventoryRevision         uint64                  `json:"inventory_revision"`
	ActualCapacity             autosplit.CapacityVector `json:"actual_capacity"`
	ActualUsage                autosplit.CapacityVector `json:"actual_usage"`
	DeclaredCapacity           autosplit.CapacityVector `json:"declared_capacity"`
	ActualMigrationCapacity    uint64                  `json:"actual_migration_capacity"`
	ActualMigrationUsed        uint64                  `json:"actual_migration_used"`
	DeclaredMigrationCapacity  uint64                  `json:"declared_migration_capacity"`
	ActualActiveReceives       uint32                  `json:"actual_active_receives"`
	DeclaredMaxReceives        uint32                  `json:"declared_max_receives"`
	ObservationDigest           replication.Digest      `json:"observation_digest"`
}

func (observation NodeInfoObservation) valid() bool {
	if observation.Nonce == ([nodeInfoNonceBytes]byte{}) || !observation.Operation.valid() ||
		observation.NodeID == (rafttransport.NodeID{}) || observation.Incarnation == 0 ||
		observation.SPKIPinDigest == (replication.Digest{}) || !observation.Store.valid(observation.NodeID) ||
		!observation.Endpoints.valid() || observation.InventoryRevision == 0 ||
		observation.ActualMigrationCapacity == 0 || observation.ActualMigrationUsed > observation.ActualMigrationCapacity ||
		observation.DeclaredMigrationCapacity == 0 || observation.DeclaredMaxReceives == 0 ||
		observation.ActualActiveReceives > observation.DeclaredMaxReceives ||
		observation.ServingGroups > 1<<20 || observation.ReservedGroups > 1<<20 ||
		observation.ObservationDigest != observation.computedDigest() {
		return false
	}
	for index := range autosplit.ResourceCount {
		if observation.ActualUsage[index] > observation.ActualCapacity[index] ||
			observation.ActualCapacity[index] > observation.DeclaredCapacity[index] {
			return false
		}
	}
	return true
}

func (observation NodeInfoObservation) computedDigest() replication.Digest {
	copyOfObservation := observation
	copyOfObservation.ObservationDigest = replication.Digest{}
	raw, err := vibejson.Marshal(&copyOfObservation)
	if err != nil {
		return replication.Digest{}
	}
	return replication.Digest(sha256.Sum256(raw))
}

func (observation NodeInfoObservation) ReadyForEnrollment() bool {
	return observation.valid() && observation.Readiness.ready()
}

// NodeInfoProvider is the only source used by NodeInfoService. Live and empty
// runtimes provide concrete implementations from their opened NodeStore,
// durable journals, listener bindings, and measured capacity counters.
type NodeInfoProvider interface {
	ObserveNodeInfo(context.Context, NodeInfoRequest) (NodeInfoObservation, error)
}

type NodeInfoProviderFunc func(context.Context, NodeInfoRequest) (NodeInfoObservation, error)

func (provider NodeInfoProviderFunc) ObserveNodeInfo(ctx context.Context, request NodeInfoRequest) (NodeInfoObservation, error) {
	if provider == nil {
		return NodeInfoObservation{}, ErrNodeInfoUnavailable
	}
	return provider(ctx, request)
}

// NodeInfoStoreFacts is the stable node-store portion a runtime provider may
// fill without exposing the store handle to a protocol client.
type NodeInfoStoreFacts struct {
	Identity                  NodeInfoStoreIdentity
	SPKIPinDigest             replication.Digest
	Endpoints                 NodeInfoEndpoints
	Readiness                 NodeInfoReadiness
	ServingGroups             uint32
	ReservedGroups            uint32
	InventoryRevision         uint64
	ActualCapacity             autosplit.CapacityVector
	ActualUsage                autosplit.CapacityVector
	DeclaredCapacity           autosplit.CapacityVector
	ActualMigrationCapacity    uint64
	ActualMigrationUsed        uint64
	DeclaredMigrationCapacity  uint64
	ActualActiveReceives       uint32
	DeclaredMaxReceives        uint32
}

func (facts NodeInfoStoreFacts) Observation(request NodeInfoRequest) (NodeInfoObservation, error) {
	if !request.valid() || facts.SPKIPinDigest == (replication.Digest{}) {
		return NodeInfoObservation{}, ErrNodeInfoUnavailable
	}
	result := NodeInfoObservation{
		Nonce: request.Nonce, Operation: request.Operation, NodeID: request.NodeID, Incarnation: request.Incarnation,
		SPKIPinDigest: facts.SPKIPinDigest, Store: facts.Identity, Endpoints: facts.Endpoints, Readiness: facts.Readiness,
		ServingGroups: facts.ServingGroups, ReservedGroups: facts.ReservedGroups, InventoryRevision: facts.InventoryRevision,
		ActualCapacity: facts.ActualCapacity, ActualUsage: facts.ActualUsage, DeclaredCapacity: facts.DeclaredCapacity,
		ActualMigrationCapacity: facts.ActualMigrationCapacity, ActualMigrationUsed: facts.ActualMigrationUsed,
		DeclaredMigrationCapacity: facts.DeclaredMigrationCapacity, ActualActiveReceives: facts.ActualActiveReceives,
		DeclaredMaxReceives: facts.DeclaredMaxReceives,
	}
	if result.Store.NodeID != request.NodeID {
		return NodeInfoObservation{}, ErrNodeInfoConflict
	}
	result.ObservationDigest = result.computedDigest()
	if !result.valid() || result.InventoryRevision < request.MinimumInventoryRevision {
		return NodeInfoObservation{}, ErrNodeInfoStale
	}
	return result, nil
}

type NodeInfoAuthorizeFunc func(rafttransport.PeerIdentity, NodeInfoRequest) bool

type NodeInfoServiceOptions struct {
	Provider      NodeInfoProvider
	TrustDomain   rafttransport.TrustDomain
	LocalNode     rafttransport.NodeID
	Incarnation   uint64
	Authorize     NodeInfoAuthorizeFunc
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	MaxConcurrent int
}

type NodeInfoService struct {
	provider      NodeInfoProvider
	trustDomain   rafttransport.TrustDomain
	localNode     rafttransport.NodeID
	incarnation   uint64
	authorize     NodeInfoAuthorizeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
}

func NewNodeInfoService(options NodeInfoServiceOptions) (*NodeInfoService, error) {
	if options.Provider == nil || options.TrustDomain == (rafttransport.TrustDomain{}) ||
		options.LocalNode == (rafttransport.NodeID{}) || options.Incarnation == 0 || options.Authorize == nil ||
		options.ReadDeadline == nil || options.WriteDeadline == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > NodeInfoMaxConcurrent {
		return nil, ErrNodeInfo
	}
	return &NodeInfoService{provider: options.Provider, trustDomain: options.TrustDomain, localNode: options.LocalNode,
		incarnation: options.Incarnation, authorize: options.Authorize, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline, slots: make(chan struct{}, options.MaxConcurrent)}, nil
}

func (service *NodeInfoService) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil || connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrNodeInfoUnauthorized
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return ErrNodeInfoBound
	}
	if deadline := nodeInfoBoundedDeadline(ctx, service.readDeadline()); deadline.IsZero() {
		return ErrNodeInfo
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	request, err := ReadNodeInfoRequest(connection)
	if err != nil {
		return err
	}
	peer := connection.PeerIdentity()
	if peer.TrustDomain != service.trustDomain || request.NodeID != service.localNode || request.Incarnation != service.incarnation ||
		!service.authorize(peer, request) {
		return ErrNodeInfoUnauthorized
	}
	observation, err := service.provider.ObserveNodeInfo(ctx, request)
	if err != nil {
		return err
	}
	if !observation.valid() || observation.Nonce != request.Nonce || observation.Operation != request.Operation ||
		observation.NodeID != request.NodeID || observation.Incarnation != request.Incarnation ||
		observation.InventoryRevision < request.MinimumInventoryRevision {
		return ErrNodeInfoStale
	}
	if deadline := nodeInfoBoundedDeadline(ctx, service.writeDeadline()); deadline.IsZero() {
		return ErrNodeInfo
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return WriteNodeInfoReply(connection, observation)
}

type NodeInfoStreamOpener interface {
	OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type NodeInfoClientOptions struct {
	Opener        NodeInfoStreamOpener
	TrustDomain   rafttransport.TrustDomain
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	Nonce         func() ([nodeInfoNonceBytes]byte, error)
}

type NodeInfoClient struct {
	opener        NodeInfoStreamOpener
	trustDomain   rafttransport.TrustDomain
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	nonce         func() ([nodeInfoNonceBytes]byte, error)
}

func NewNodeInfoClient(options NodeInfoClientOptions) (*NodeInfoClient, error) {
	if options.Opener == nil || options.TrustDomain == (rafttransport.TrustDomain{}) ||
		options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrNodeInfo
	}
	nonce := options.Nonce
	if nonce == nil {
		nonce = func() ([nodeInfoNonceBytes]byte, error) {
			var result [nodeInfoNonceBytes]byte
			_, err := io.ReadFull(rand.Reader, result[:])
			return result, err
		}
	}
	return &NodeInfoClient{opener: options.Opener, trustDomain: options.TrustDomain,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline, nonce: nonce}, nil
}

func (client *NodeInfoClient) Observe(ctx context.Context, nodeID rafttransport.NodeID, request NodeInfoRequest) (NodeInfoObservation, error) {
	if client == nil || ctx == nil || nodeID == (rafttransport.NodeID{}) || !request.valid() || request.NodeID != nodeID {
		return NodeInfoObservation{}, ErrNodeInfo
	}
	if cause := context.Cause(ctx); cause != nil {
		return NodeInfoObservation{}, cause
	}
	connection, err := client.opener.OpenShardControl(ctx, nodeID)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return NodeInfoObservation{}, errors.Join(ErrNodeInfoUnavailable, err)
	}
	if connection == nil {
		return NodeInfoObservation{}, ErrNodeInfoUnavailable
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	if connection.TrafficClass() != rafttransport.TrafficShardControl || peer.TrustDomain != client.trustDomain || peer.Node != nodeID {
		return NodeInfoObservation{}, ErrNodeInfoUnauthorized
	}
	if deadline := nodeInfoBoundedDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return NodeInfoObservation{}, ErrNodeInfo
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return NodeInfoObservation{}, err
	}
	if err := WriteNodeInfoRequest(connection, request); err != nil {
		return NodeInfoObservation{}, errors.Join(ErrNodeInfoConflict, err)
	}
	if deadline := nodeInfoBoundedDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return NodeInfoObservation{}, ErrNodeInfoConflict
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return NodeInfoObservation{}, err
	}
	observation, err := ReadNodeInfoReply(connection)
	if err != nil {
		return NodeInfoObservation{}, errors.Join(ErrNodeInfoConflict, err)
	}
	if observation.Nonce != request.Nonce || observation.Operation != request.Operation ||
		observation.NodeID != nodeID || observation.Incarnation != request.Incarnation ||
		observation.InventoryRevision < request.MinimumInventoryRevision || !observation.valid() {
		return NodeInfoObservation{}, ErrNodeInfoConflict
	}
	return observation, nil
}

func AppendNodeInfoRequest(dst []byte, request NodeInfoRequest) ([]byte, error) {
	if !request.valid() || len(dst) > math.MaxInt-nodeInfoRequestHeaderBytes {
		return dst, ErrNodeInfo
	}
	start := len(dst)
	dst = append(dst, make([]byte, nodeInfoRequestHeaderBytes)...)
	raw := dst[start:]
	copy(raw[:8], nodeInfoRequestMagic[:])
	raw[8] = nodeInfoVersion
	raw[9] = byte(request.Operation)
	copy(raw[12:28], request.Nonce[:])
	copy(raw[28:44], request.NodeID[:])
	binary.BigEndian.PutUint64(raw[44:52], request.Incarnation)
	binary.BigEndian.PutUint64(raw[52:60], request.MinimumInventoryRevision)
	return dst, nil
}

func OpenNodeInfoRequest(raw []byte) (NodeInfoRequest, error) {
	if len(raw) != nodeInfoRequestHeaderBytes || !bytes.Equal(raw[:8], nodeInfoRequestMagic[:]) ||
		raw[8] != nodeInfoVersion || raw[10] != 0 || raw[11] != 0 || !allNodeInfoZero(raw[60:]) {
		return NodeInfoRequest{}, ErrNodeInfo
	}
	var request NodeInfoRequest
	request.Operation = NodeInfoOperation(raw[9])
	copy(request.Nonce[:], raw[12:28])
	copy(request.NodeID[:], raw[28:44])
	request.Incarnation = binary.BigEndian.Uint64(raw[44:52])
	request.MinimumInventoryRevision = binary.BigEndian.Uint64(raw[52:60])
	if !request.valid() {
		return NodeInfoRequest{}, ErrNodeInfo
	}
	return request, nil
}

func ReadNodeInfoRequest(reader io.Reader) (NodeInfoRequest, error) {
	var raw [nodeInfoRequestHeaderBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return NodeInfoRequest{}, errors.Join(ErrNodeInfo, err)
	}
	return OpenNodeInfoRequest(raw[:])
}

func WriteNodeInfoRequest(writer io.Writer, request NodeInfoRequest) error {
	raw, err := AppendNodeInfoRequest(nil, request)
	if err != nil {
		return err
	}
	return writeNodeInfoFull(writer, raw)
}

func AppendNodeInfoReply(dst []byte, observation NodeInfoObservation) ([]byte, error) {
	if !observation.valid() {
		return dst, ErrNodeInfoStale
	}
	payload, err := vibejson.Marshal(&observation)
	if err != nil || len(payload) == 0 || len(payload) > NodeInfoMaxReplyBytes ||
		len(dst) > math.MaxInt-nodeInfoResponseHeaderBytes-len(payload) {
		return dst, errors.Join(ErrNodeInfo, err)
	}
	start := len(dst)
	dst = append(dst, make([]byte, nodeInfoResponseHeaderBytes+len(payload))...)
	raw := dst[start:]
	copy(raw[:8], nodeInfoResponseMagic[:])
	raw[8] = nodeInfoVersion
	raw[9] = nodeInfoResponseSuccess
	copy(raw[12:28], observation.Nonce[:])
	binary.BigEndian.PutUint32(raw[28:32], uint32(len(payload)))
	digest := sha256.Sum256(payload)
	copy(raw[32:64], digest[:])
	copy(raw[64:], payload)
	return dst, nil
}

func OpenNodeInfoReply(raw []byte) (NodeInfoObservation, error) {
	if len(raw) < nodeInfoResponseHeaderBytes || !bytes.Equal(raw[:8], nodeInfoResponseMagic[:]) ||
		raw[8] != nodeInfoVersion || raw[9] != nodeInfoResponseSuccess || raw[10] != 0 || raw[11] != 0 {
		return NodeInfoObservation{}, ErrNodeInfo
	}
	payloadBytes := int(binary.BigEndian.Uint32(raw[28:32]))
	if payloadBytes == 0 || payloadBytes > NodeInfoMaxReplyBytes || len(raw) != nodeInfoResponseHeaderBytes+payloadBytes {
		return NodeInfoObservation{}, ErrNodeInfo
	}
	if sha256.Sum256(raw[64:]) != [sha256.Size]byte(raw[32:64]) {
		return NodeInfoObservation{}, ErrNodeInfo
	}
	var observation NodeInfoObservation
	if err := vibejson.Unmarshal(raw[64:], &observation); err != nil {
		return NodeInfoObservation{}, errors.Join(ErrNodeInfo, err)
	}
	canonical, err := vibejson.Marshal(&observation)
	if err != nil || !bytes.Equal(canonical, raw[64:]) || !observation.valid() {
		return NodeInfoObservation{}, errors.Join(ErrNodeInfoStale, err)
	}
	if !bytes.Equal(observation.Nonce[:], raw[12:28]) {
		return NodeInfoObservation{}, ErrNodeInfoConflict
	}
	return observation, nil
}

func ReadNodeInfoReply(reader io.Reader) (NodeInfoObservation, error) {
	var header [nodeInfoResponseHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return NodeInfoObservation{}, errors.Join(ErrNodeInfo, err)
	}
	payloadBytes := int(binary.BigEndian.Uint32(header[28:32]))
	if payloadBytes == 0 || payloadBytes > NodeInfoMaxReplyBytes {
		return NodeInfoObservation{}, ErrNodeInfo
	}
	raw := make([]byte, nodeInfoResponseHeaderBytes+payloadBytes)
	copy(raw, header[:])
	if _, err := io.ReadFull(reader, raw[nodeInfoResponseHeaderBytes:]); err != nil {
		return NodeInfoObservation{}, errors.Join(ErrNodeInfo, err)
	}
	return OpenNodeInfoReply(raw)
}

func WriteNodeInfoReply(writer io.Writer, observation NodeInfoObservation) error {
	raw, err := AppendNodeInfoReply(nil, observation)
	if err != nil {
		return err
	}
	return writeNodeInfoFull(writer, raw)
}

func NodeInfoRequestDiscriminator() [8]byte { return nodeInfoRequestMagic }

func nodeInfoBoundedDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		return time.Time{}
	}
	if deadline, found := ctx.Deadline(); found && deadline.Before(configured) {
		return deadline
	}
	return configured
}

func allNodeInfoZero(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}

func writeNodeInfoFull(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if written > 0 {
			data = data[written:]
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
