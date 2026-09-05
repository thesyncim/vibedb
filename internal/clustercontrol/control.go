// Package clustercontrol contains the authenticated operator control contract.
//
// Cluster operations use the same mutually authenticated gateway-client ALPN
// and newline-delimited vibejson framing as the query endpoint.  The operation
// envelope is deliberately separate from query requests at the field level,
// while sharing the listener, TLS capability, canonical encoding, and bounded
// request/response memory rules.  The gateway server imports this package when
// dispatching the five cluster operations.
package clustercontrol

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/servicetls"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	// Format is the operator control envelope version.  This is a payload
	// version; the authenticated listener's ALPN remains gateway-client.
	Format uint16 = 1

	// MaxRequestBytes and MaxResponseBytes bound complete newline-delimited
	// documents before decoding.  Status remains bounded even when a cluster
	// has the maximum supported physical-node inventory.
	MaxRequestBytes  = 1 << 20
	MaxResponseBytes = 4 << 20
	MaxNodeFileBytes = 1 << 20
	MaxNodes         = 4096
	MaxBlockers      = 256
	MaxStringBytes   = 4096
	MaxErrorBytes    = 4096
	MaxWaitMillis    = uint64(24 * 60 * 60 * 1000)
	MaxTargets       = 256
)

var (
	ErrInvalidRequest        = errors.New("clustercontrol: invalid request")
	ErrInvalidResponse       = errors.New("clustercontrol: invalid response")
	ErrInvalidProfile        = errors.New("clustercontrol: invalid auth-client profile")
	ErrInvalidNodeDescriptor = errors.New("clustercontrol: invalid node descriptor")
	ErrFrameBound            = errors.New("clustercontrol: frame exceeds bound")
	ErrNonCanonical          = errors.New("clustercontrol: non-canonical vibejson")
	ErrRemote                = errors.New("clustercontrol: remote operation failed")
)

const (
	OpNodes        = "cluster_nodes"
	OpJoin         = "cluster_join"
	OpRebalance    = "cluster_rebalance"
	OpDecommission = "cluster_decommission"
	OpStatus       = "cluster_status"
)

var validOperations = map[string]struct{}{
	OpNodes: {}, OpJoin: {}, OpRebalance: {}, OpDecommission: {}, OpStatus: {},
}

// Request is one idempotent operator round trip.  RequestID is required for
// every operation, including reads, so a response can always be correlated in
// logs.  Mutating callers should persist and retry the same ID after a lost
// response.  WaitMillis only controls how long the server waits to observe
// progress; it never cancels or rolls back a durable operation.
type Request struct {
	Format            uint16          `json:"format"`
	Op                string          `json:"op"`
	RequestID         string          `json:"request_id"`
	OperationID       string          `json:"operation_id,omitempty"`
	WaitMillis        uint64          `json:"wait_ms,omitempty"`
	NodeDescriptor    *NodeDescriptor `json:"node_descriptor,omitempty"`
	NodeID            string          `json:"node_id,omitempty"`
	NodeIncarnation   uint64          `json:"node_incarnation,omitempty"`
	DesiredNodeCount  uint16          `json:"desired_node_count,omitempty"`
	MaxMoves          uint16          `json:"max_moves,omitempty"`
	MaxMigrationBytes uint64          `json:"max_migration_bytes,omitempty"`
	HysteresisPPM     uint64          `json:"hysteresis_ppm,omitempty"`
}

// NodeDescriptor is the public enrollment descriptor read by cluster join.
// It contains only immutable identity, listener, capacity, and role facts.
// Certificate/key paths, private keys, WAL paths, and process-root paths are
// intentionally absent from this type and therefore cannot cross the wire.
type NodeDescriptor struct {
	Format      uint16 `json:"format"`
	NodeID      string `json:"node_id"`
	Incarnation uint64 `json:"incarnation"`
	// ServiceKeyDigest is the SPKI SHA-256 pin for this physical node
	// incarnation. It is public enrollment metadata, never a private key.
	ServiceKeyDigest  string    `json:"service_key_digest"`
	FailureDomain     string    `json:"failure_domain"`
	Roles             []string  `json:"roles"`
	DataEndpoint      string    `json:"data_endpoint,omitempty"`
	NativeEndpoint    string    `json:"native_endpoint,omitempty"`
	ControlEndpoint   string    `json:"control_endpoint,omitempty"`
	GatewayEndpoint   string    `json:"gateway_endpoint,omitempty"`
	DataAddress       string    `json:"data_address"`
	NativeAddress     string    `json:"native_address"`
	ControlAddress    string    `json:"control_address"`
	GatewayAddress    string    `json:"gateway_address,omitempty"`
	Capacity          [7]uint64 `json:"capacity"`
	MigrationCapacity uint64    `json:"migration_capacity"`
	MaxReceives       uint32    `json:"max_receives"`
}

// NodeStatus is the bounded physical-node view returned by nodes and status.
type NodeStatus struct {
	NodeID            string `json:"node_id"`
	Incarnation       uint64 `json:"incarnation"`
	Lifecycle         string `json:"lifecycle"`
	Revision          uint64 `json:"revision"`
	CatalogGeneration uint64 `json:"catalog_generation"`
	SafeToStop        bool   `json:"safe_to_stop"`
}

// Blocker identifies a concrete reference or revision preventing progress.
type Blocker struct {
	Code            string `json:"code"`
	Detail          string `json:"detail"`
	NodeID          string `json:"node_id,omitempty"`
	NodeIncarnation uint64 `json:"node_incarnation,omitempty"`
	Revision        uint64 `json:"revision,omitempty"`
}

// SafeToStopEvidence is the server's proof summary for a retiring physical
// node.  Counts are not interpreted as success unless the response also has
// safe_to_stop=true and no blockers.
type SafeToStopEvidence struct {
	NodeID                 string `json:"node_id"`
	NodeIncarnation        uint64 `json:"node_incarnation"`
	CatalogGeneration      uint64 `json:"catalog_generation"`
	DirectoryRevision      uint64 `json:"directory_revision"`
	ServingReplicas        uint32 `json:"serving_replicas"`
	LearnerReplicas        uint32 `json:"learner_replicas"`
	EnrolledTargets        uint32 `json:"enrolled_targets"`
	OutstandingMoves       uint32 `json:"outstanding_moves"`
	CatalogVoters          uint32 `json:"catalog_voters"`
	ControlVoters          uint32 `json:"control_voters"`
	GatewayParticipants    uint32 `json:"gateway_participants"`
	DrainAcknowledged      bool   `json:"drain_acknowledged"`
	RetiredAcknowledged    bool   `json:"retired_acknowledged"`
	CatalogControlMigrated bool   `json:"catalog_control_migrated"`
	Digest                 string `json:"digest"`
}

// BudgetStatus proves that the migration controller exercised node-wide
// pacing.  A zero value is valid for operations with no migration yet.
type BudgetStatus struct {
	ThrottledCalls uint64 `json:"throttled_calls"`
	ThrottledBytes uint64 `json:"throttled_bytes"`
	PeakActive     uint32 `json:"peak_active"`
	MaxActive      uint32 `json:"max_active"`
}

// Response is returned for every accepted envelope.  A transport-level
// failure has no Response; an operation-level failure has OK=false and an
// explanatory Error while still echoing request and operation IDs whenever
// the server has them.
type Response struct {
	Format            uint16              `json:"format"`
	Op                string              `json:"op"`
	OK                bool                `json:"ok"`
	Error             string              `json:"error,omitempty"`
	RequestID         string              `json:"request_id"`
	OperationID       string              `json:"operation_id,omitempty"`
	State             string              `json:"state,omitempty"`
	CatalogGeneration uint64              `json:"catalog_generation,omitempty"`
	DirectoryRevision uint64              `json:"directory_revision,omitempty"`
	Nodes             []NodeStatus        `json:"nodes,omitempty"`
	Blockers          []Blocker           `json:"blockers,omitempty"`
	SafeToStop        bool                `json:"safe_to_stop"`
	Evidence          *SafeToStopEvidence `json:"evidence,omitempty"`
	Budget            *BudgetStatus       `json:"budget,omitempty"`
	// Progress fields are populated by status responses while a scaling
	// operation is active. They are omitted for legacy node-only responses and
	// are intentionally typed so a qualification cannot infer movement from a
	// final node count.
	Phase                  string `json:"phase,omitempty"`
	ApplicationGroupsMoved uint32 `json:"application_groups_moved,omitempty"`
	InternalGroupsMoved    uint32 `json:"internal_groups_moved,omitempty"`
	RetiringReferences     uint32 `json:"retiring_references,omitempty"`
	GroupInventoryDigest   string `json:"group_inventory_digest,omitempty"`
}

// Profile is the canonical on-disk auth-client configuration consumed by
// vibedb cluster commands.  Key is a local path only: it is loaded into TLS
// and is never included in Request or any wire payload.
type Profile struct {
	Format      uint16 `json:"format"`
	Address     string `json:"address"`
	ServerNode  string `json:"server_node"`
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}

// RemoteError preserves a bounded server error while allowing callers to
// inspect the complete Response and recover the durable operation ID.
type RemoteError struct {
	Op          string
	RequestID   string
	OperationID string
	Message     string
}

func (err *RemoteError) Error() string {
	if err == nil {
		return ErrRemote.Error()
	}
	if err.Message == "" {
		return fmt.Sprintf("%s: op=%s request_id=%s", ErrRemote, err.Op, err.RequestID)
	}
	return fmt.Sprintf("%s: op=%s request_id=%s: %s", ErrRemote, err.Op, err.RequestID, err.Message)
}

func (err *RemoteError) Unwrap() error { return ErrRemote }

// NewRequestID returns a cryptographically random 256-bit lowercase hex ID.
// Callers that need replay-safe retries persist this value before dispatch.
func NewRequestID() (string, error) {
	var id [32]byte
	if _, err := io.ReadFull(rand.Reader, id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func validOperation(op string) bool {
	_, ok := validOperations[op]
	return ok
}

func validHexID(value string, bytesCount int) bool {
	if len(value) != hex.EncodedLen(bytesCount) {
		return false
	}
	for _, char := range value {
		if char >= 'A' && char <= 'F' {
			return false
		}
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.Trim(value, "0") != ""
}

func validText(value string, maximum int, required bool) bool {
	return len(value) <= maximum && (!required || len(value) != 0) &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func validNodeID(value string) bool { return validHexID(value, 16) }

func validOperationID(value string) bool { return validHexID(value, 32) }

func validRequestID(value string) bool { return validHexID(value, 32) }

func (descriptor NodeDescriptor) Valid() bool {
	if descriptor.Format != Format || !validNodeID(descriptor.NodeID) ||
		descriptor.Incarnation == 0 || !validHexID(descriptor.ServiceKeyDigest, sha256.Size) ||
		!validText(descriptor.FailureDomain, MaxStringBytes, true) ||
		len(descriptor.Roles) == 0 || len(descriptor.Roles) > 8 ||
		!validText(descriptor.DataAddress, MaxStringBytes, true) ||
		!validText(descriptor.NativeAddress, MaxStringBytes, true) ||
		!validText(descriptor.ControlAddress, MaxStringBytes, true) ||
		!validText(descriptor.GatewayAddress, MaxStringBytes, false) ||
		descriptor.MigrationCapacity == 0 || descriptor.MaxReceives == 0 {
		return false
	}
	roles := append([]string(nil), descriptor.Roles...)
	sort.Strings(roles)
	for index, role := range roles {
		if role == "" || !validText(role, 64, true) || role != descriptor.Roles[index] {
			return false
		}
		if index > 0 && roles[index-1] == role {
			return false
		}
		switch role {
		case "storage", "catalog", "gateway", "control":
		default:
			return false
		}
	}
	for _, endpoint := range []string{descriptor.DataEndpoint, descriptor.NativeEndpoint, descriptor.ControlEndpoint, descriptor.GatewayEndpoint} {
		if !validText(endpoint, MaxStringBytes, false) {
			return false
		}
	}
	return true
}

func (request Request) Valid() bool {
	if request.Format != Format || !validOperation(request.Op) || !validRequestID(request.RequestID) ||
		(request.OperationID != "" && !validOperationID(request.OperationID)) || request.WaitMillis > MaxWaitMillis ||
		(request.NodeID != "" && !validNodeID(request.NodeID)) ||
		(request.NodeID != "" && request.NodeIncarnation == 0) ||
		(request.NodeID == "" && request.NodeIncarnation != 0) ||
		request.HysteresisPPM > 1_000_000 || request.MaxMoves > 4096 || request.DesiredNodeCount > 4096 {
		return false
	}
	switch request.Op {
	case OpNodes:
		return request.OperationID == "" && request.NodeDescriptor == nil && request.NodeID == "" &&
			request.MaxMoves == 0 && request.MaxMigrationBytes == 0 && request.DesiredNodeCount == 0 && request.HysteresisPPM == 0
	case OpJoin:
		return request.OperationID == "" && request.NodeDescriptor != nil && request.NodeDescriptor.Valid() &&
			request.NodeID == "" && request.MaxMoves == 0 && request.MaxMigrationBytes == 0 && request.DesiredNodeCount == 0 && request.HysteresisPPM == 0
	case OpRebalance:
		return request.OperationID == "" && request.NodeDescriptor == nil && request.NodeID == "" &&
			request.MaxMoves != 0 && request.MaxMigrationBytes != 0
	case OpDecommission:
		return request.OperationID == "" && request.NodeDescriptor == nil && request.NodeID != "" &&
			request.NodeIncarnation != 0 && request.MaxMoves == 0 && request.MaxMigrationBytes == 0 &&
			request.DesiredNodeCount == 0 && request.HysteresisPPM == 0
	case OpStatus:
		return request.OperationID != "" && request.NodeDescriptor == nil && request.NodeID == "" &&
			request.NodeIncarnation == 0 && request.MaxMoves == 0 && request.MaxMigrationBytes == 0 &&
			request.DesiredNodeCount == 0 && request.HysteresisPPM == 0
	default:
		return false
	}
}

func (status NodeStatus) valid() bool {
	return validNodeID(status.NodeID) && status.Incarnation != 0 &&
		validText(status.Lifecycle, 64, true) && status.Revision != 0 && status.CatalogGeneration != 0
}

func (blocker Blocker) valid() bool {
	return validText(blocker.Code, 128, true) && validText(blocker.Detail, MaxStringBytes, false) &&
		(blocker.NodeID == "" || validNodeID(blocker.NodeID)) &&
		(blocker.NodeID == "" || blocker.NodeIncarnation != 0) &&
		(blocker.NodeID == "" || blocker.Revision != 0)
}

func (evidence *SafeToStopEvidence) valid() bool {
	if evidence == nil {
		return true
	}
	return validNodeID(evidence.NodeID) && evidence.NodeIncarnation != 0 &&
		evidence.CatalogGeneration != 0 && evidence.DirectoryRevision != 0 &&
		validHexID(evidence.Digest, 32)
}

func (response Response) Valid() bool {
	if response.Format != Format || !validOperation(response.Op) || !validRequestID(response.RequestID) ||
		(response.OperationID != "" && !validOperationID(response.OperationID)) ||
		!validText(response.Error, MaxErrorBytes, !response.OK) || len(response.Nodes) > MaxNodes || len(response.Blockers) > MaxBlockers ||
		!response.Evidence.valid() {
		return false
	}
	if response.OK && response.Error != "" {
		return false
	}
	if response.Op == OpStatus && response.OperationID == "" {
		return false
	}
	for index, node := range response.Nodes {
		if !node.valid() || index > 0 && !nodeStatusFollows(response.Nodes[index-1], node) {
			return false
		}
	}
	for _, blocker := range response.Blockers {
		if !blocker.valid() {
			return false
		}
	}
	if response.Budget != nil && response.Budget.MaxActive == 0 {
		return false
	}
	return true
}

// nodeStatusFollows keeps historical incarnations in one response canonical
// without allowing the same identity to be repeated. Nodes/status returns the
// complete directory cut, so two records may legitimately share NodeID while
// carrying different lifecycle incarnations.
func nodeStatusFollows(previous, next NodeStatus) bool {
	if previous.NodeID < next.NodeID {
		return true
	}
	return previous.NodeID == next.NodeID && previous.Incarnation < next.Incarnation
}

var (
	requestDecoder    = mustDecoder[Request]()
	responseDecoder   = mustDecoder[Response]()
	profileDecoder    = mustDecoder[Profile]()
	descriptorDecoder = mustDecoder[NodeDescriptor]()
)

func mustDecoder[T any]() vibejson.Decoder[T] {
	decoder, err := vibejson.CompileDecoder[T](vibejson.DecoderOptions{
		MaxDepth: 32, DisallowUnknownFields: true, CaseSensitive: true, Replace: true,
	})
	if err != nil {
		panic(err)
	}
	return decoder
}

func canonicalLine(raw []byte, maximum int) ([]byte, error) {
	if len(raw) == 0 || len(raw) > maximum {
		return nil, ErrFrameBound
	}
	if raw[len(raw)-1] == '\n' {
		raw = raw[:len(raw)-1]
	}
	if len(raw) == 0 || raw[len(raw)-1] == '\r' {
		return nil, ErrNonCanonical
	}
	return raw, nil
}

// EncodeRequest returns one complete NDJSON request line, including newline.
func EncodeRequest(request Request) ([]byte, error) {
	if !request.Valid() {
		return nil, ErrInvalidRequest
	}
	raw, err := vibejson.Marshal(&request)
	if err != nil || len(raw)+1 > MaxRequestBytes {
		return nil, errors.Join(ErrInvalidRequest, err)
	}
	return append(raw, '\n'), nil
}

// DecodeRequest strictly decodes one canonical request line.  The server uses
// this function before capability dispatch, so unknown fields cannot smuggle
// authority into an operation.
func DecodeRequest(line []byte) (Request, error) {
	var request Request
	raw, err := canonicalLine(line, MaxRequestBytes)
	if err != nil {
		return request, err
	}
	if err := requestDecoder.Decode(raw, &request); err != nil {
		return request, errors.Join(ErrInvalidRequest, err)
	}
	canonical, err := vibejson.Marshal(&request)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Request{}, errors.Join(ErrInvalidRequest, ErrNonCanonical, err)
	}
	if !request.Valid() {
		return Request{}, ErrInvalidRequest
	}
	return request, nil
}

// EncodeResponse returns one complete NDJSON response line.
func EncodeResponse(response Response) ([]byte, error) {
	if !response.Valid() {
		return nil, ErrInvalidResponse
	}
	raw, err := vibejson.Marshal(&response)
	if err != nil || len(raw)+1 > MaxResponseBytes {
		return nil, errors.Join(ErrInvalidResponse, err)
	}
	return append(raw, '\n'), nil
}

// DecodeResponse strictly decodes one canonical response line.
func DecodeResponse(line []byte) (Response, error) {
	var response Response
	raw, err := canonicalLine(line, MaxResponseBytes)
	if err != nil {
		return response, err
	}
	if err := responseDecoder.Decode(raw, &response); err != nil {
		return response, errors.Join(ErrInvalidResponse, err)
	}
	canonical, err := vibejson.Marshal(&response)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Response{}, errors.Join(ErrInvalidResponse, ErrNonCanonical, err)
	}
	if !response.Valid() {
		return Response{}, ErrInvalidResponse
	}
	return response, nil
}

func (profile Profile) Valid() bool {
	return profile.Format == Format && validText(profile.Address, MaxStringBytes, true) &&
		validNodeID(profile.ServerNode) && validText(profile.Certificate, MaxStringBytes, true) &&
		validText(profile.Key, MaxStringBytes, true) && validText(profile.Roots, MaxStringBytes, true) &&
		validText(profile.IdentityOID, 128, true)
}

// LoadProfile reads one canonical bounded auth-client profile.  The file is
// never sent over the operator connection; only the loaded TLS identity and
// public request descriptor are used on the wire.
func LoadProfile(path string) (Profile, error) {
	var profile Profile
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return profile, ErrInvalidProfile
	}
	file, err := os.Open(path)
	if err != nil {
		return profile, errors.Join(ErrInvalidProfile, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > MaxNodeFileBytes {
		return profile, errors.Join(ErrInvalidProfile, err)
	}
	raw := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, raw); err != nil {
		return profile, errors.Join(ErrInvalidProfile, err)
	}
	var trailing [1]byte
	if count, readErr := file.Read(trailing[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return profile, ErrInvalidProfile
	}
	canonical, err := canonicalLine(raw, MaxNodeFileBytes)
	if err != nil || profileDecoder.Decode(canonical, &profile) != nil {
		return profile, errors.Join(ErrInvalidProfile, err)
	}
	encoded, err := vibejson.Marshal(&profile)
	if err != nil || !bytes.Equal(encoded, canonical) || !profile.Valid() {
		return Profile{}, errors.Join(ErrInvalidProfile, err)
	}
	return profile, nil
}

// LoadNodeDescriptor reads a canonical public descriptor.  Since the decoder
// rejects unknown fields and this type has no secret-bearing fields, a key or
// private process path is rejected before any connection is opened.
func LoadNodeDescriptor(path string) (NodeDescriptor, error) {
	var descriptor NodeDescriptor
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return descriptor, ErrInvalidNodeDescriptor
	}
	file, err := os.Open(path)
	if err != nil {
		return descriptor, errors.Join(ErrInvalidNodeDescriptor, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > MaxNodeFileBytes {
		return descriptor, errors.Join(ErrInvalidNodeDescriptor, err)
	}
	raw := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, raw); err != nil {
		return descriptor, errors.Join(ErrInvalidNodeDescriptor, err)
	}
	var trailing [1]byte
	if count, readErr := file.Read(trailing[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return descriptor, ErrInvalidNodeDescriptor
	}
	canonical, err := canonicalLine(raw, MaxNodeFileBytes)
	if err != nil || descriptorDecoder.Decode(canonical, &descriptor) != nil {
		return descriptor, errors.Join(ErrInvalidNodeDescriptor, err)
	}
	encoded, err := vibejson.Marshal(&descriptor)
	if err != nil || !bytes.Equal(encoded, canonical) || !descriptor.Valid() {
		return NodeDescriptor{}, errors.Join(ErrInvalidNodeDescriptor, err)
	}
	return descriptor, nil
}

// NewClient constructs an authenticated operator client using the existing
// gateway-client ALPN and bounded connection pool.
func NewClient(profile Profile) (*Client, error) {
	if !profile.Valid() {
		return nil, ErrInvalidProfile
	}
	tlsProfile, err := servicetls.LoadProfile(profile.Certificate, profile.Key, profile.Roots, profile.IdentityOID, time.Now)
	if err != nil {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	serverNode, err := parseNodeID(profile.ServerNode)
	if err != nil {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	transport, err := servicetls.NewClient(servicetls.ClientOptions{
		TLS: tlsProfile, Class: rafttransport.TrafficGatewayClient,
		Endpoints: []servicetls.Endpoint{{Address: profile.Address, Node: serverNode}},
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", address)
		},
		HandshakeDeadline: func() time.Time { return time.Now().Add(5 * time.Second) },
		MaxConnections:    1, MaxHandshakes: 1,
	})
	if err != nil {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	return &Client{profile: profile, transport: transport}, nil
}

// Client performs one bounded request per connection.  Connections are closed
// after the response, so a long server wait cannot pin a stale client stream
// after credential rotation.
type Client struct {
	profile   Profile
	transport *servicetls.Client
}

func (client *Client) Close() error {
	if client == nil || client.transport == nil {
		return nil
	}
	return client.transport.Close()
}

// Execute writes one canonical request and waits for exactly one canonical
// response.  Context cancellation only ends this wait; it does not cancel the
// durable operation identified by RequestID/OperationID.
func (client *Client) Execute(ctx context.Context, request Request) (Response, error) {
	var response Response
	if client == nil || client.transport == nil || ctx == nil || !request.Valid() {
		return response, ErrInvalidRequest
	}
	line, err := EncodeRequest(request)
	if err != nil {
		return response, err
	}
	connection, err := client.transport.Dial(ctx, client.profile.Address)
	if err != nil {
		return response, err
	}
	defer connection.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			return response, err
		}
	}
	if _, err := connection.Write(line); err != nil {
		return response, err
	}
	reader := bufio.NewReaderSize(connection, MaxResponseBytes+1)
	responseLine, err := reader.ReadBytes('\n')
	if err != nil {
		return response, err
	}
	if len(responseLine) > MaxResponseBytes {
		return response, ErrFrameBound
	}
	response, err = DecodeResponse(responseLine)
	if err != nil {
		return Response{}, err
	}
	if !response.OK {
		return response, &RemoteError{Op: response.Op, RequestID: response.RequestID,
			OperationID: response.OperationID, Message: response.Error}
	}
	return response, nil
}

func parseNodeID(value string) (rafttransport.NodeID, error) {
	var node rafttransport.NodeID
	if !validNodeID(value) {
		return node, ErrInvalidProfile
	}
	_, err := hex.Decode(node[:], []byte(value))
	return node, err
}
