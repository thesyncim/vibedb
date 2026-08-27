package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

var errGatewayReplicaControlManifest = errors.New("vibedb-gateway: invalid replica control manifest")

const maxGatewayReplicaControlManifestBytes = 4 << 20

const (
	gatewayMaxSplitBatchDocuments = 64
	gatewayMaxSplitBatchBytes     = replication.MaxCommandBytes +
		gatewayMaxSplitBatchDocuments*replication.MaxMutationKeyBytes
)

type persistedGatewayReplicaControlManifest struct {
	Generation       uint64                                 `json:"generation"`
	LocalGateway     persistedGatewayControlEndpoint        `json:"local_gateway"`
	TLS              persistedGatewayReplicaTLS             `json:"tls"`
	Bounds           persistedGatewayReplicaBounds          `json:"bounds"`
	ShardEndpoints   []persistedGatewayShardControlEndpoint `json:"shard_endpoints"`
	GatewayEndpoints []persistedGatewayControlEndpoint      `json:"gateway_endpoints"`
	Candidates       []persistedGatewayReplacementCandidate `json:"candidates"`
	SplitTemplate    persistedGatewaySplitTemplate          `json:"split_template"`
}

type persistedGatewayReplicaTLS struct {
	Certificate         string `json:"certificate"`
	Key                 string `json:"key"`
	Roots               string `json:"roots"`
	IdentityOID         string `json:"identity_oid"`
	AuthorizationPolicy string `json:"authorization_policy"`
}

type persistedGatewayReplicaBounds struct {
	MaxConnections      uint32 `json:"max_connections"`
	MaxHandshakes       uint32 `json:"max_handshakes"`
	MaxConcurrentDrains uint32 `json:"max_concurrent_drains"`
	ControllerInterval  uint64 `json:"controller_interval_millis"`
	ReadTimeout         uint64 `json:"read_timeout_millis"`
	WriteTimeout        uint64 `json:"write_timeout_millis"`
}

type persistedGatewayShardControlEndpoint struct {
	Node           string `json:"node"`
	ControlAddress string `json:"control_address"`
	SplitChildRoot string `json:"split_child_root"`
}

type persistedGatewaySplitTemplate struct {
	MaxSessions       uint64            `json:"max_sessions"`
	RetryWindow       uint16            `json:"retry_window"`
	TxnLimits         durable.TxnLimits `json:"txn_limits"`
	Format            uint16            `json:"format"`
	ShardKey          string            `json:"shard_key"`
	MaxBatchDocuments int               `json:"max_batch_documents"`
	MaxBatchBytes     int               `json:"max_batch_bytes"`
	TupleVersion      uint16            `json:"tuple_version"`
	MapperVersion     uint16            `json:"mapper_version"`
}

type persistedGatewayControlEndpoint struct {
	Node           string `json:"node"`
	Incarnation    uint64 `json:"incarnation"`
	ControlAddress string `json:"control_address"`
}

type persistedGatewayReplacementCandidate struct {
	Member          uint64 `json:"member"`
	Node            string `json:"node"`
	Store           string `json:"store"`
	NodeIncarnation uint64 `json:"node_incarnation"`
	Endpoint        string `json:"endpoint"`
	Load            uint64 `json:"load"`
}

type gatewayReplicaTLSReferences struct {
	Certificate, Key, Roots, IdentityOID, AuthorizationPolicy string
}

type gatewayReplicaControlManifest struct {
	Generation      uint64
	Local           gatewayControlEndpoint
	TLS             gatewayReplicaTLSReferences
	Bounds          persistedGatewayReplicaBounds
	Shards          []gateway.ReplicatedEndpoint
	SplitChildRoots []string
	Gateways        []gatewayControlEndpoint
	Candidates      []gatewayReplicaCandidate
	SplitTemplate   persistedGatewaySplitTemplate
}

type gatewayReplicaCandidate struct {
	Member          uint64
	Node            rafttransport.NodeID
	StoreID         [16]byte
	NodeIncarnation uint64
	Endpoint        distribution.EndpointID
	Load            uint64
}

func loadGatewayReplicaControlManifest(path string, local rafttransport.NodeID) (gatewayReplicaControlManifest, error) {
	if path == "" || local == (rafttransport.NodeID{}) {
		return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
	}
	file, err := os.Open(path)
	if err != nil {
		return gatewayReplicaControlManifest{}, errors.Join(err, errGatewayReplicaControlManifest)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > maxGatewayReplicaControlManifestBytes {
		return gatewayReplicaControlManifest{}, errors.Join(err, errGatewayReplicaControlManifest)
	}
	raw := make([]byte, int(info.Size()))
	if _, err = io.ReadFull(file, raw); err != nil {
		return gatewayReplicaControlManifest{}, errors.Join(err, errGatewayReplicaControlManifest)
	}
	return openGatewayReplicaControlManifest(raw, local)
}

func openGatewayReplicaControlManifest(raw []byte, local rafttransport.NodeID) (gatewayReplicaControlManifest, error) {
	if len(raw) == 0 || len(raw) > maxGatewayReplicaControlManifestBytes || local == (rafttransport.NodeID{}) {
		return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
	}
	var persisted persistedGatewayReplicaControlManifest
	if err := vibejson.Unmarshal(raw, &persisted); err != nil {
		return gatewayReplicaControlManifest{}, errors.Join(err, errGatewayReplicaControlManifest)
	}
	canonical, err := vibejson.Marshal(&persisted)
	if err != nil || !bytes.Equal(raw, canonical) {
		return gatewayReplicaControlManifest{}, errors.Join(err, errGatewayReplicaControlManifest)
	}
	manifest := gatewayReplicaControlManifest{Generation: persisted.Generation,
		SplitTemplate: persisted.SplitTemplate,
		Bounds:        persisted.Bounds, TLS: gatewayReplicaTLSReferences{
			Certificate: persisted.TLS.Certificate, Key: persisted.TLS.Key,
			Roots: persisted.TLS.Roots, IdentityOID: persisted.TLS.IdentityOID,
			AuthorizationPolicy: persisted.TLS.AuthorizationPolicy,
		}}
	if manifest.Generation == 0 || !validGatewayReplicaTLSReferences(manifest.TLS) ||
		!validGatewayReplicaBounds(manifest.Bounds) || len(persisted.ShardEndpoints) == 0 ||
		len(persisted.GatewayEndpoints) == 0 || len(persisted.Candidates) == 0 ||
		!validGatewaySplitTemplate(manifest.SplitTemplate) {
		return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
	}
	manifest.Shards = make([]gateway.ReplicatedEndpoint, len(persisted.ShardEndpoints))
	manifest.SplitChildRoots = make([]string, len(persisted.ShardEndpoints))
	shardAddresses := make(map[string]struct{}, len(persisted.ShardEndpoints))
	for index, encoded := range persisted.ShardEndpoints {
		node, parseErr := parseGatewayReplicaNode(encoded.Node)
		if parseErr != nil || !validGatewayReplicaAddress(encoded.ControlAddress) ||
			!filepath.IsAbs(encoded.SplitChildRoot) || filepath.Clean(encoded.SplitChildRoot) != encoded.SplitChildRoot ||
			encoded.SplitChildRoot == string(filepath.Separator) {
			return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
		}
		manifest.Shards[index] = gateway.ReplicatedEndpoint{Node: node,
			ControlAddress: strings.Clone(encoded.ControlAddress)}
		manifest.SplitChildRoots[index] = strings.Clone(encoded.SplitChildRoot)
		if index != 0 && bytes.Compare(manifest.Shards[index-1].Node[:], node[:]) >= 0 {
			return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
		}
		if _, duplicate := shardAddresses[encoded.ControlAddress]; duplicate {
			return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
		}
		shardAddresses[encoded.ControlAddress] = struct{}{}
	}
	manifest.Gateways = make([]gatewayControlEndpoint, len(persisted.GatewayEndpoints))
	gatewayAddresses := make(map[string]struct{}, len(persisted.GatewayEndpoints))
	for index, encoded := range persisted.GatewayEndpoints {
		endpoint, parseErr := openGatewayControlEndpoint(encoded)
		if parseErr != nil || index != 0 && bytes.Compare(manifest.Gateways[index-1].Member.Node[:], endpoint.Member.Node[:]) >= 0 {
			return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
		}
		manifest.Gateways[index] = endpoint
		if _, duplicate := gatewayAddresses[endpoint.Address]; duplicate {
			return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
		}
		gatewayAddresses[endpoint.Address] = struct{}{}
		if endpoint.Member.Node == local {
			manifest.Local = endpoint
		}
	}
	declaredLocal, err := openGatewayControlEndpoint(persisted.LocalGateway)
	if err != nil || manifest.Local.Member.Node == (rafttransport.NodeID{}) || declaredLocal != manifest.Local {
		return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
	}
	manifest.Candidates = make([]gatewayReplicaCandidate, len(persisted.Candidates))
	for index, encoded := range persisted.Candidates {
		candidate, parseErr := openGatewayReplicaCandidate(encoded)
		if parseErr != nil || index != 0 && compareGatewayReplicaCandidate(manifest.Candidates[index-1], candidate) >= 0 {
			return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
		}
		manifest.Candidates[index] = candidate
		_, enrolledNode := slices.BinarySearchFunc(manifest.Shards, candidate.Node,
			func(endpoint gateway.ReplicatedEndpoint, node rafttransport.NodeID) int {
				return bytes.Compare(endpoint.Node[:], node[:])
			})
		if !enrolledNode {
			return gatewayReplicaControlManifest{}, errGatewayReplicaControlManifest
		}
	}
	return manifest, nil
}

func validGatewaySplitTemplate(template persistedGatewaySplitTemplate) bool {
	pointer, err := vibejson.CompilePointer(template.ShardKey)
	return template.MaxSessions != 0 && template.RetryWindow != 0 &&
		template.TxnLimits.MaxCollections > 0 && template.TxnLimits.MaxDocuments > 0 &&
		template.TxnLimits.MaxBytes > 0 && err == nil && len(pointer.Tokens) == 1 &&
		pointer.String() == template.ShardKey &&
		template.MaxBatchDocuments > 0 && template.MaxBatchDocuments <= gatewayMaxSplitBatchDocuments &&
		template.MaxBatchBytes > 0 && template.MaxBatchBytes <= gatewayMaxSplitBatchBytes &&
		template.TupleVersion == uint16(distribution.CurrentTupleVersion) &&
		template.MapperVersion == uint16(distribution.NativeMapperVersion)
}

func validGatewayReplicaTLSReferences(references gatewayReplicaTLSReferences) bool {
	return references.Certificate != "" && references.Key != "" && references.Roots != "" &&
		references.IdentityOID != "" && references.AuthorizationPolicy != ""
}

func validGatewayReplicaBounds(bounds persistedGatewayReplicaBounds) bool {
	return bounds.MaxConnections != 0 && bounds.MaxConnections <= gateway.AbsoluteMaxCatalogDrainConcurrency &&
		bounds.MaxHandshakes != 0 && bounds.MaxHandshakes <= bounds.MaxConnections &&
		bounds.MaxConcurrentDrains != 0 && bounds.MaxConcurrentDrains <= bounds.MaxConnections &&
		bounds.ControllerInterval != 0 && bounds.ReadTimeout != 0 && bounds.WriteTimeout != 0 &&
		bounds.ControllerInterval <= uint64((24*time.Hour)/time.Millisecond) &&
		bounds.ReadTimeout <= uint64((5*time.Minute)/time.Millisecond) &&
		bounds.WriteTimeout <= uint64((5*time.Minute)/time.Millisecond)
}

func openGatewayControlEndpoint(encoded persistedGatewayControlEndpoint) (gatewayControlEndpoint, error) {
	node, err := parseGatewayReplicaNode(encoded.Node)
	if err != nil || encoded.Incarnation == 0 || !validGatewayReplicaAddress(encoded.ControlAddress) {
		return gatewayControlEndpoint{}, errGatewayReplicaControlManifest
	}
	return gatewayControlEndpoint{Member: gateway.ClusterCatalogDrainMember{Node: node,
		Incarnation: encoded.Incarnation}, Address: strings.Clone(encoded.ControlAddress)}, nil
}

func openGatewayReplicaCandidate(encoded persistedGatewayReplacementCandidate) (gatewayReplicaCandidate, error) {
	node, err := parseGatewayReplicaNode(encoded.Node)
	if err != nil || encoded.Member == 0 || encoded.NodeIncarnation == 0 || encoded.Endpoint == "" {
		return gatewayReplicaCandidate{}, errGatewayReplicaControlManifest
	}
	var store [16]byte
	if err = decodeGatewayReplicaHex(encoded.Store, store[:]); err != nil {
		return gatewayReplicaCandidate{}, err
	}
	return gatewayReplicaCandidate{Member: encoded.Member, Node: node, StoreID: store,
		NodeIncarnation: encoded.NodeIncarnation,
		Endpoint:        distribution.EndpointID(strings.Clone(encoded.Endpoint)), Load: encoded.Load}, nil
}

func parseGatewayReplicaNode(encoded string) (rafttransport.NodeID, error) {
	var node rafttransport.NodeID
	if err := decodeGatewayReplicaHex(encoded, node[:]); err != nil || node == (rafttransport.NodeID{}) {
		return rafttransport.NodeID{}, errGatewayReplicaControlManifest
	}
	return node, nil
}

func decodeGatewayReplicaHex(encoded string, destination []byte) error {
	if len(encoded) != hex.EncodedLen(len(destination)) {
		return errGatewayReplicaControlManifest
	}
	written, err := hex.Decode(destination, []byte(encoded))
	if err != nil || written != len(destination) {
		return errGatewayReplicaControlManifest
	}
	return nil
}

func validGatewayReplicaAddress(address string) bool {
	if address == "" || len(address) > 1024 || strings.IndexByte(address, 0) >= 0 {
		return false
	}
	_, port, err := net.SplitHostPort(address)
	return err == nil && port != ""
}

func compareGatewayReplicaCandidate(left, right gatewayReplicaCandidate) int {
	if order := bytes.Compare(left.Node[:], right.Node[:]); order != 0 {
		return order
	}
	if left.Member < right.Member {
		return -1
	}
	if left.Member > right.Member {
		return 1
	}
	return 0
}

func (manifest gatewayReplicaControlManifest) ReplacementCandidates(
	_ context.Context, catalog *gateway.Snapshot, certificate rebalance.FailureQuorumCertificate,
) ([]rebalance.ReplacementCandidate, error) {
	if catalog == nil || certificate.CatalogGeneration != catalog.Generation() ||
		certificate.ConfirmedEpoch == 0 || certificate.Group.TopologyRecoveryEpoch == 0 ||
		len(manifest.Candidates) == 0 {
		return nil, errGatewayReplicaControlManifest
	}
	result := make([]rebalance.ReplacementCandidate, len(manifest.Candidates))
	for index, candidate := range manifest.Candidates {
		result[index] = rebalance.ReplacementCandidate{Member: candidate.Member, Node: candidate.Node,
			StoreID: candidate.StoreID, NodeIncarnation: candidate.NodeIncarnation,
			Endpoint: candidate.Endpoint, TopologyRecoveryEpoch: certificate.Group.TopologyRecoveryEpoch,
			HealthEpoch: certificate.ConfirmedEpoch, Load: candidate.Load}
	}
	return result[:len(result):len(result)], nil
}

func (manifest gatewayReplicaControlManifest) ValidateCatalog(snapshot *gateway.Snapshot) error {
	if snapshot == nil || len(manifest.Shards) == 0 {
		return errGatewayReplicaControlManifest
	}
	addresses := make(map[rafttransport.NodeID]string, len(manifest.Shards))
	for _, endpoint := range manifest.Shards {
		addresses[endpoint.Node] = endpoint.ControlAddress
	}
	descriptors := snapshot.ReplicatedShardDescriptors()
	if len(descriptors) == 0 {
		return errGatewayReplicaControlManifest
	}
	for _, descriptor := range descriptors {
		for _, replica := range descriptor.Replicas {
			if err := validateGatewayReplicaCatalogEndpoint(snapshot, addresses, replica); err != nil {
				return err
			}
		}
		if descriptor.EnrolledTarget != nil {
			if err := validateGatewayReplicaCatalogEndpoint(snapshot, addresses,
				*descriptor.EnrolledTarget); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateGatewayReplicaCatalogEndpoint(
	snapshot *gateway.Snapshot,
	addresses map[rafttransport.NodeID]string,
	replica gateway.ReplicatedReplicaDescriptor,
) error {
	want, found := addresses[replica.Node]
	actual, err := snapshot.Address(replica.ControlEndpoint)
	if err != nil || !found || want != actual {
		return errors.Join(err, errGatewayReplicaControlManifest)
	}
	return nil
}

func (manifest gatewayReplicaControlManifest) gatewayMembers() []gateway.ClusterCatalogDrainMember {
	result := make([]gateway.ClusterCatalogDrainMember, len(manifest.Gateways))
	for index := range manifest.Gateways {
		result[index] = manifest.Gateways[index].Member
	}
	slices.SortFunc(result, func(left, right gateway.ClusterCatalogDrainMember) int {
		return bytes.Compare(left.Node[:], right.Node[:])
	})
	return result
}
