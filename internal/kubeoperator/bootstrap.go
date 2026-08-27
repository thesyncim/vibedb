package kubeoperator

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

var ErrBootstrap = errors.New("kubeoperator: invalid bootstrap")

const (
	bootstrapFormat        = 1
	bootstrapOID           = "1.3.6.1.4.1.32473.1.1"
	bootstrapBundleName    = "bootstrap.yaml"
	bootstrapStateName     = "bootstrap-state.vibejson"
	bootstrapBundlePending = ".bootstrap.yaml.pending"
	bootstrapStatePending  = ".bootstrap-state.vibejson.pending"
	bootstrapBundleWrite   = ".bootstrap.yaml.write"
	bootstrapStateWrite    = ".bootstrap-state.vibejson.write"
)

type BootstrapConfig struct {
	Namespace         string
	StateDirectory    string
	ManifestConfigMap string
	TLSSecret         string
	GatewayConfigMap  string
	GatewayTLSSecret  string
	Now               func() time.Time
}

type BootstrapResult struct {
	ShardNodeIDs  [9]string
	GatewayNodeID string
	ClientNodeID  string
	Bytes         int
}

type bootstrapState struct {
	Format            uint16    `json:"format"`
	Namespace         string    `json:"namespace"`
	ManifestConfigMap string    `json:"manifest_config_map"`
	TLSSecret         string    `json:"tls_secret"`
	GatewayConfigMap  string    `json:"gateway_config_map"`
	GatewayTLSSecret  string    `json:"gateway_tls_secret"`
	ShardNodeIDs      [9]string `json:"shard_node_ids"`
	GatewayNodeID     string    `json:"gateway_node"`
	ClientNodeID      string    `json:"client_node"`
	BundleBytes       uint64    `json:"bundle_bytes"`
	BundleDigest      string    `json:"bundle_digest"`
}

// LoadBootstrapState opens the authenticated committed bootstrap cut and
// returns the renderer identities without exposing private key material.
func LoadBootstrapState(stateDirectory string) (BootstrapResult, error) {
	if !validBootstrapStateDirectory(stateDirectory) {
		return BootstrapResult{}, ErrBootstrap
	}
	raw, err := os.ReadFile(filepath.Join(stateDirectory, bootstrapStateName))
	if err != nil {
		return BootstrapResult{}, err
	}
	var state bootstrapState
	if vibejson.Unmarshal(raw, &state) != nil {
		return BootstrapResult{}, ErrBootstrap
	}
	config := BootstrapConfig{Namespace: state.Namespace, StateDirectory: stateDirectory,
		ManifestConfigMap: state.ManifestConfigMap, TLSSecret: state.TLSSecret,
		GatewayConfigMap: state.GatewayConfigMap, GatewayTLSSecret: state.GatewayTLSSecret}
	bundle, recovered, found, err := recoverBootstrap(config)
	if err != nil || !found || recovered != state {
		return BootstrapResult{}, errors.Join(ErrBootstrap, err)
	}
	return bootstrapResult(state, len(bundle)), nil
}

type bootstrapPrepare struct {
	Root                  string             `json:"root"`
	Distribution          string             `json:"distribution"`
	Shard                 string             `json:"shard"`
	ClusterID             string             `json:"cluster_id"`
	ClusterIncarnation    string             `json:"cluster_incarnation"`
	TopologyRecoveryEpoch uint64             `json:"topology_recovery_epoch"`
	AllocationGeneration  uint64             `json:"allocation_generation"`
	ShardIncarnation      string             `json:"shard_incarnation"`
	GroupID               string             `json:"group_id"`
	MemberID              uint64             `json:"member_id"`
	StoreID               string             `json:"store_id"`
	Table                 string             `json:"table"`
	CreateTable           string             `json:"create_table"`
	Authority             bootstrapAuthority `json:"authority"`
	WAL                   bootstrapWAL       `json:"wal"`
	Apply                 bootstrapApply     `json:"apply"`
	Listeners             bootstrapListeners `json:"listeners"`
	TLS                   bootstrapTLS       `json:"tls"`
	AuthorizationPolicy   string             `json:"authorization_policy"`
	SplitControl          bootstrapSplit     `json:"split_control"`
	Members               []bootstrapMember  `json:"members"`
}
type bootstrapAuthority struct {
	ActivePolicyGeneration uint64 `json:"active_policy_generation"`
	ProtectionEpoch        uint64 `json:"protection_epoch"`
	OwnershipEpoch         uint64 `json:"ownership_epoch"`
	SchemaGeneration       uint64 `json:"schema_generation"`
	RoutingVersion         uint64 `json:"routing_version"`
	RouteGeneration        uint64 `json:"route_generation"`
}
type bootstrapWAL struct {
	KeyID           string `json:"key_id"`
	KeyMaterialPath string `json:"key_material_path"`
	WrappedKey      string `json:"wrapped_key"`
	MaxFileBytes    int64  `json:"max_file_bytes"`
	MaxRecordBytes  int    `json:"max_record_bytes"`
	MaxRecords      uint64 `json:"max_records"`
	MaxEntries      uint64 `json:"max_entries"`
	MaxLiveBytes    int64  `json:"max_live_bytes"`
}
type bootstrapApply struct {
	MaxSessions                      uint64 `json:"max_sessions"`
	RetryWindow                      uint16 `json:"retry_window"`
	MaxCollections                   int    `json:"max_collections"`
	MaxDocuments                     int    `json:"max_documents"`
	MaxBytes                         int64  `json:"max_bytes"`
	ShardKey                         string `json:"shard_key"`
	RequestLedgerCapacityBytes       uint64 `json:"request_ledger_capacity_bytes"`
	RequestLedgerCleanupReserveBytes uint64 `json:"request_ledger_cleanup_reserve_bytes"`
	RequestLedgerRangeStart          string `json:"request_ledger_range_start"`
	RequestLedgerRangeEnd            string `json:"request_ledger_range_end"`
	RequestLedgerRangeIdentity       string `json:"request_ledger_range_identity"`
}
type bootstrapListeners struct {
	Peer     string `json:"peer"`
	Native   string `json:"native"`
	Snapshot string `json:"snapshot"`
	Control  string `json:"control"`
}
type bootstrapTLS struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}
type bootstrapMember struct {
	MemberID    uint64 `json:"member_id"`
	NodeID      string `json:"node_id"`
	PeerAddress string `json:"peer_address"`
}
type bootstrapSplit struct {
	MaxRecords           int              `json:"max_records"`
	MaxFileBytes         int64            `json:"max_file_bytes"`
	Grants               []bootstrapGrant `json:"grants"`
	MaxChildOperations   int              `json:"max_child_operations"`
	StageCheckpointBytes uint64           `json:"stage_checkpoint_bytes"`
}
type bootstrapGrant struct {
	NodeID  string `json:"node_id"`
	Actions uint16 `json:"actions"`
}
type bootstrapPolicy struct {
	Generation uint64               `json:"generation"`
	Principals []bootstrapPrincipal `json:"principals"`
}
type bootstrapPrincipal struct {
	Node         string   `json:"node"`
	Capabilities []string `json:"capabilities"`
}

type bootstrapControl struct {
	Generation       uint64                   `json:"generation"`
	LocalGateway     bootstrapEndpoint        `json:"local_gateway"`
	TLS              bootstrapControlTLS      `json:"tls"`
	Bounds           bootstrapBounds          `json:"bounds"`
	ShardEndpoints   []bootstrapShardEndpoint `json:"shard_endpoints"`
	GatewayEndpoints []bootstrapEndpoint      `json:"gateway_endpoints"`
	Candidates       []bootstrapCandidate     `json:"candidates"`
	SplitTemplate    bootstrapSplitTemplate   `json:"split_template"`
}
type bootstrapEndpoint struct {
	Node           string `json:"node"`
	Incarnation    uint64 `json:"incarnation"`
	ControlAddress string `json:"control_address"`
}
type bootstrapControlTLS struct {
	Certificate         string `json:"certificate"`
	Key                 string `json:"key"`
	Roots               string `json:"roots"`
	IdentityOID         string `json:"identity_oid"`
	AuthorizationPolicy string `json:"authorization_policy"`
}
type bootstrapBounds struct {
	MaxConnections      uint32 `json:"max_connections"`
	MaxHandshakes       uint32 `json:"max_handshakes"`
	MaxConcurrentDrains uint32 `json:"max_concurrent_drains"`
	ControllerInterval  uint64 `json:"controller_interval_millis"`
	ReadTimeout         uint64 `json:"read_timeout_millis"`
	WriteTimeout        uint64 `json:"write_timeout_millis"`
}
type bootstrapShardEndpoint struct {
	Node                 string `json:"node"`
	ControlAddress       string `json:"control_address"`
	SplitSnapshotAddress string `json:"split_snapshot_address"`
	SplitChildRoot       string `json:"split_child_root"`
}
type bootstrapCandidate struct {
	Member          uint64 `json:"member"`
	Node            string `json:"node"`
	Store           string `json:"store"`
	NodeIncarnation uint64 `json:"node_incarnation"`
	Endpoint        string `json:"endpoint"`
	Load            uint64 `json:"load"`
}
type bootstrapSplitTemplate struct {
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

type bootstrapRole struct {
	name                   string
	distribution           distribution.DistributionName
	shard                  distribution.ShardID
	table, create, primary string
	group                  raftmember.GroupKey
	nodes                  [3]rafttransport.NodeID
	stores                 [3][16]byte
	candidateStores        [3][16]byte
	digest                 replication.Digest
	limits                 sqldriver.ReplicatedShardStoreLimits
}

func Bootstrap(writer io.Writer, config BootstrapConfig) (BootstrapResult, error) {
	if writer == nil || !validBootstrapConfig(config) {
		return BootstrapResult{}, ErrBootstrap
	}
	if err := os.MkdirAll(config.StateDirectory, 0o700); err != nil {
		return BootstrapResult{}, err
	}
	if !validBootstrapStateDirectory(config.StateDirectory) {
		return BootstrapResult{}, ErrBootstrap
	}
	if bundle, state, found, err := recoverBootstrap(config); err != nil {
		return BootstrapResult{}, err
	} else if found {
		_, err = writer.Write(bundle)
		return bootstrapResult(state, len(bundle)), err
	}
	bundle, state, err := buildBootstrap(config, rand.Reader)
	if err != nil {
		return BootstrapResult{}, err
	}
	state.BundleBytes = uint64(len(bundle))
	digest := sha256.Sum256(bundle)
	state.BundleDigest = hex.EncodeToString(digest[:])
	stateRaw, err := vibejson.Marshal(&state)
	if err != nil {
		return BootstrapResult{}, err
	}
	if err = stageBootstrap(config.StateDirectory, bundle, stateRaw); err != nil {
		return BootstrapResult{}, err
	}
	if err = commitBootstrap(config.StateDirectory); err != nil {
		return BootstrapResult{}, err
	}
	_, err = writer.Write(bundle)
	return bootstrapResult(state, len(bundle)), err
}

func bootstrapResult(state bootstrapState, size int) BootstrapResult {
	return BootstrapResult{ShardNodeIDs: state.ShardNodeIDs, GatewayNodeID: state.GatewayNodeID,
		ClientNodeID: state.ClientNodeID, Bytes: size}
}

func validBootstrapConfig(c BootstrapConfig) bool {
	return dnsLabel.MatchString(c.Namespace) && dnsLabel.MatchString(c.ManifestConfigMap) && dnsLabel.MatchString(c.TLSSecret) && dnsLabel.MatchString(c.GatewayConfigMap) && dnsLabel.MatchString(c.GatewayTLSSecret) && filepath.IsAbs(c.StateDirectory) && filepath.Clean(c.StateDirectory) == c.StateDirectory && c.StateDirectory != "/"
}
func validBootstrapStateDirectory(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return false
	}
	info, err := os.Lstat(path)
	return err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm()&0o077 == 0
}
func validBootstrapState(s bootstrapState, c BootstrapConfig, bundle []byte) bool {
	if s.Format != bootstrapFormat || s.Namespace != c.Namespace || s.ManifestConfigMap != c.ManifestConfigMap || s.TLSSecret != c.TLSSecret || s.GatewayConfigMap != c.GatewayConfigMap || s.GatewayTLSSecret != c.GatewayTLSSecret || s.BundleBytes != uint64(len(bundle)) || len(s.BundleDigest) != 64 {
		return false
	}
	sum := sha256.Sum256(bundle)
	return s.BundleDigest == hex.EncodeToString(sum[:])
}

func recoverBootstrap(config BootstrapConfig) ([]byte, bootstrapState, bool, error) {
	root := config.StateDirectory
	path := func(name string) string { return filepath.Join(root, name) }
	cleaned := false
	for _, name := range []string{bootstrapBundleWrite, bootstrapStateWrite} {
		if err := os.Remove(path(name)); err == nil {
			cleaned = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, bootstrapState{}, false, err
		}
	}
	if cleaned {
		if err := syncDirectory(root); err != nil {
			return nil, bootstrapState{}, false, err
		}
	}
	stateRaw, stateErr := os.ReadFile(path(bootstrapStateName))
	bundle, bundleErr := os.ReadFile(path(bootstrapBundleName))
	if stateErr == nil && bundleErr == nil {
		var state bootstrapState
		if vibejson.Unmarshal(stateRaw, &state) != nil || !validBootstrapState(state, config, bundle) {
			return nil, bootstrapState{}, false, ErrBootstrap
		}
		return bundle, state, true, nil
	}
	if stateErr != nil && !errors.Is(stateErr, os.ErrNotExist) ||
		bundleErr != nil && !errors.Is(bundleErr, os.ErrNotExist) {
		return nil, bootstrapState{}, false, errors.Join(stateErr, bundleErr)
	}
	pendingStateRaw, pendingStateErr := os.ReadFile(path(bootstrapStatePending))
	pendingBundle, pendingBundleErr := os.ReadFile(path(bootstrapBundlePending))
	if pendingStateErr == nil {
		candidate := pendingBundle
		if pendingBundleErr != nil && bundleErr == nil {
			candidate = bundle
		}
		if len(candidate) == 0 && errors.Is(pendingBundleErr, os.ErrNotExist) &&
			errors.Is(bundleErr, os.ErrNotExist) {
			if err := os.Remove(path(bootstrapStatePending)); err != nil {
				return nil, bootstrapState{}, false, err
			}
			if err := syncDirectory(root); err != nil {
				return nil, bootstrapState{}, false, err
			}
			return nil, bootstrapState{}, false, nil
		}
		var state bootstrapState
		if len(candidate) == 0 || vibejson.Unmarshal(pendingStateRaw, &state) != nil ||
			!validBootstrapState(state, config, candidate) {
			return nil, bootstrapState{}, false, ErrBootstrap
		}
		if bundleErr != nil {
			if err := os.Rename(path(bootstrapBundlePending), path(bootstrapBundleName)); err != nil {
				return nil, bootstrapState{}, false, err
			}
		}
		if err := os.Rename(path(bootstrapStatePending), path(bootstrapStateName)); err != nil {
			return nil, bootstrapState{}, false, err
		}
		if err := syncDirectory(root); err != nil {
			return nil, bootstrapState{}, false, err
		}
		return candidate, state, true, nil
	}
	if pendingStateErr != nil && !errors.Is(pendingStateErr, os.ErrNotExist) ||
		pendingBundleErr != nil && !errors.Is(pendingBundleErr, os.ErrNotExist) {
		return nil, bootstrapState{}, false, errors.Join(pendingStateErr, pendingBundleErr)
	}
	if bundleErr == nil || pendingBundleErr == nil {
		return nil, bootstrapState{}, false, ErrBootstrap
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, bootstrapState{}, false, err
	}
	if len(entries) != 0 {
		return nil, bootstrapState{}, false, ErrBootstrap
	}
	return nil, bootstrapState{}, false, nil
}

func stageBootstrap(root string, bundle, state []byte) error {
	path := func(name string) string { return filepath.Join(root, name) }
	if err := writeExclusive(path(bootstrapStateWrite), state, 0o600); err != nil {
		return err
	}
	if err := os.Rename(path(bootstrapStateWrite), path(bootstrapStatePending)); err != nil {
		return err
	}
	if err := writeExclusive(path(bootstrapBundleWrite), bundle, 0o600); err != nil {
		return err
	}
	if err := os.Rename(path(bootstrapBundleWrite), path(bootstrapBundlePending)); err != nil {
		return err
	}
	return syncDirectory(root)
}

func commitBootstrap(root string) error {
	path := func(name string) string { return filepath.Join(root, name) }
	if err := os.Rename(path(bootstrapBundlePending), path(bootstrapBundleName)); err != nil {
		return err
	}
	if err := syncDirectory(root); err != nil {
		return err
	}
	if err := os.Rename(path(bootstrapStatePending), path(bootstrapStateName)); err != nil {
		return err
	}
	return syncDirectory(root)
}

func readBootstrap16(reader io.Reader) (v [16]byte, err error) {
	_, err = io.ReadFull(reader, v[:])
	return
}
func readBootstrapNode(reader io.Reader) (v rafttransport.NodeID, err error) {
	_, err = io.ReadFull(reader, v[:])
	return
}

func buildBootstrap(c BootstrapConfig, random io.Reader) ([]byte, bootstrapState, error) {
	cluster, err := readBootstrap16(random)
	if err != nil {
		return nil, bootstrapState{}, err
	}
	incarnation, err := readBootstrap16(random)
	if err != nil {
		return nil, bootstrapState{}, err
	}
	gatewayNode, err := readBootstrapNode(random)
	if err != nil {
		return nil, bootstrapState{}, err
	}
	clientNode, err := readBootstrapNode(random)
	if err != nil {
		return nil, bootstrapState{}, err
	}
	state := bootstrapState{Format: bootstrapFormat, Namespace: c.Namespace, ManifestConfigMap: c.ManifestConfigMap, TLSSecret: c.TLSSecret, GatewayConfigMap: c.GatewayConfigMap, GatewayTLSSecret: c.GatewayTLSSecret, GatewayNodeID: hex.EncodeToString(gatewayNode[:]), ClientNodeID: hex.EncodeToString(clientNode[:])}
	roles := [3]bootstrapRole{{name: "catalog", distribution: gateway.ReplicatedCatalogDistribution, shard: gateway.ReplicatedCatalogShard, table: gateway.ReplicatedCatalogTable, create: "CREATE TABLE controlplane (PRIMARY KEY (id))", primary: "/id"}, {name: "ledger", distribution: "request-ledger", shard: "all", table: "request_ledger", create: "CREATE TABLE request_ledger (PRIMARY KEY (id))", primary: "/id"}, {name: "data", distribution: "data", shard: "all", table: "documents", create: "CREATE TABLE documents (PRIMARY KEY (id))", primary: "/id"}}
	allNodes := make([]rafttransport.NodeID, 0, 10)
	for ri := range roles {
		shardIncarnation, readErr := readBootstrap16(random)
		if readErr != nil {
			return nil, state, readErr
		}
		groupID, readErr := readBootstrap16(random)
		if readErr != nil {
			return nil, state, readErr
		}
		roles[ri].group = raftmember.GroupKey{ClusterID: cluster, ClusterIncarnation: incarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: shardIncarnation, GroupID: groupID}
		for mi := 0; mi < 3; mi++ {
			roles[ri].nodes[mi], readErr = readBootstrapNode(random)
			if readErr != nil {
				return nil, state, readErr
			}
			roles[ri].stores[mi], readErr = readBootstrap16(random)
			if readErr != nil {
				return nil, state, readErr
			}
			roles[ri].candidateStores[mi], readErr = readBootstrap16(random)
			if readErr != nil {
				return nil, state, readErr
			}
			allNodes = append(allNodes, roles[ri].nodes[mi])
			state.ShardNodeIDs[ri*3+mi] = hex.EncodeToString(roles[ri].nodes[mi][:])
		}
	}
	allNodes = append(allNodes, gatewayNode, clientNode)
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now().UTC()
	}
	certs, roots, err := bootstrapCredentials(random, now, rafttransport.TrustDomain{ClusterID: cluster, ClusterIncarnation: incarnation}, allNodes)
	if err != nil {
		return nil, state, err
	}
	policy := bootstrapPolicy{Generation: 1, Principals: make([]bootstrapPrincipal, 0, len(allNodes))}
	caps := []string{"data_read", "data_write", "schema", "delegate", "membership", "topology", "transaction_recovery", "request_ledger", "execution_pin"}
	for index, node := range allNodes {
		nodeCaps := caps
		if index == len(allNodes)-1 {
			nodeCaps = []string{"data_read", "data_write"}
		}
		policy.Principals = append(policy.Principals, bootstrapPrincipal{Node: hex.EncodeToString(node[:]), Capabilities: nodeCaps})
	}
	sort.Slice(policy.Principals, func(i, j int) bool { return policy.Principals[i].Node < policy.Principals[j].Node })
	policyRaw, err := vibejson.Marshal(&policy)
	if err != nil {
		return nil, state, err
	}
	ledgerIdentity := bootstrapLedgerIdentity(roles[1].group)
	for i := range roles {
		digest, limits, e := probeBootstrapRole(roles[i])
		if e != nil {
			return nil, state, e
		}
		roles[i].digest, roles[i].limits = digest, limits
	}
	catalogRaw, err := bootstrapCatalog(c.StateDirectory, roles)
	if err != nil {
		return nil, state, err
	}
	var walKey [32]byte
	if _, err = io.ReadFull(random, walKey[:]); err != nil {
		return nil, state, err
	}
	var ack [32]byte
	if _, err = io.ReadFull(random, ack[:]); err != nil {
		return nil, state, err
	}
	manifests := map[string][]byte{}
	shardSecret := map[string][]byte{"cluster-roots.pem": roots, "wal-key-source": walKey[:]}
	gatewaySecret := map[string][]byte{"cluster-roots.pem": roots, "gateway-cert.pem": certs[9].cert, "gateway-key.pem": certs[9].key, "durable-ack-key": []byte(hex.EncodeToString(ack[:]))}
	clientSecret := map[string][]byte{"cluster-roots.pem": roots, "client-cert.pem": certs[10].cert, "client-key.pem": certs[10].key}
	for ri := range roles {
		members := make([]bootstrapMember, 3)
		grants := make([]bootstrapGrant, 3)
		for mi := 0; mi < 3; mi++ {
			host := fmt.Sprintf("vibedb-%s-%d.vibedb-%s-peer", roles[ri].name, mi, roles[ri].name)
			members[mi] = bootstrapMember{MemberID: uint64(mi + 1), NodeID: state.ShardNodeIDs[ri*3+mi], PeerAddress: host + ":7411"}
			grants[mi] = bootstrapGrant{NodeID: state.ShardNodeIDs[ri*3+mi], Actions: ^uint16(0)}
		}
		for mi := 0; mi < 3; mi++ {
			prefix := roles[ri].name + "-" + fmt.Sprint(mi)
			certName, keyName := prefix+"-cert.pem", prefix+"-key.pem"
			shardSecret[certName], shardSecret[keyName] = certs[ri*3+mi].cert, certs[ri*3+mi].key
			apply := bootstrapApply{MaxSessions: 128, RetryWindow: 8, MaxCollections: 16, MaxDocuments: 4096, MaxBytes: 384 << 20, ShardKey: roles[ri].primary}
			if ri == 1 {
				z := strings.Repeat("0", 64)
				apply.RequestLedgerCapacityBytes = 64 << 20
				apply.RequestLedgerCleanupReserveBytes = 8 << 20
				apply.RequestLedgerRangeStart = z
				apply.RequestLedgerRangeEnd = z
				apply.RequestLedgerRangeIdentity = hex.EncodeToString(ledgerIdentity[:])
			}
			p := bootstrapPrepare{Root: "/var/lib/vibedb/member", Distribution: string(roles[ri].distribution), Shard: string(roles[ri].shard), ClusterID: hex.EncodeToString(cluster[:]), ClusterIncarnation: hex.EncodeToString(incarnation[:]), TopologyRecoveryEpoch: 1, AllocationGeneration: 1, ShardIncarnation: hex.EncodeToString(roles[ri].group.ShardIncarnation[:]), GroupID: hex.EncodeToString(roles[ri].group.GroupID[:]), MemberID: uint64(mi + 1), StoreID: hex.EncodeToString(roles[ri].stores[mi][:]), Table: roles[ri].table, CreateTable: roles[ri].create, Authority: bootstrapAuthority{1, 1, 1, 1, 1, 1}, WAL: bootstrapWAL{"kubernetes-test-key", "/run/secrets/vibedb/wal-key-source", "development-only", raftstore.DefaultMaxFileBytes, raftstore.DefaultMaxRecordBytes, raftstore.DefaultMaxRecords, raftstore.DefaultMaxEntries, raftstore.DefaultMaxLiveBytes}, Apply: apply, Listeners: bootstrapListeners{"0.0.0.0:7411", "0.0.0.0:7511", "0.0.0.0:7611", "0.0.0.0:7711"}, TLS: bootstrapTLS{"/run/secrets/vibedb/" + certName, "/run/secrets/vibedb/" + keyName, "/run/secrets/vibedb/cluster-roots.pem", bootstrapOID}, AuthorizationPolicy: "/bootstrap/authorization-policy.vibejson", SplitControl: bootstrapSplit{4096, 64 << 20, grants, 8, 32 << 20}, Members: members}
			raw, e := vibejson.Marshal(&p)
			if e != nil {
				return nil, state, e
			}
			manifests[fmt.Sprintf("%s-%d.vibejson", roles[ri].name, mi)] = raw
		}
	}
	controlRaw, err := bootstrapReplicaControl(roles, state)
	if err != nil {
		return nil, state, err
	}
	gatewayConfig := map[string][]byte{"cluster.vibejson": catalogRaw, "authorization-policy.vibejson": policyRaw, "replica-control.vibejson": controlRaw}
	manifests["authorization-policy.vibejson"] = policyRaw
	var out bytes.Buffer
	appendConfigMap(&out, c.Namespace, c.ManifestConfigMap, manifests)
	appendSecret(&out, c.Namespace, c.TLSSecret, shardSecret)
	appendConfigMap(&out, c.Namespace, c.GatewayConfigMap, gatewayConfig)
	appendSecret(&out, c.Namespace, c.GatewayTLSSecret, gatewaySecret)
	appendSecret(&out, c.Namespace, "vibedb-qualification-client-tls", clientSecret)
	return out.Bytes(), state, nil
}

type bootstrapCert struct{ cert, key []byte }

func bootstrapCredentials(random io.Reader, now time.Time, domain rafttransport.TrustDomain, nodes []rafttransport.NodeID) ([]bootstrapCert, []byte, error) {
	oid := asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}
	caKey, e := ecdsa.GenerateKey(elliptic.P256(), random)
	if e != nil {
		return nil, nil, e
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "vibedb kubernetes test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(7 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, e := x509.CreateCertificate(random, tmpl, tmpl, &caKey.PublicKey, caKey)
	if e != nil {
		return nil, nil, e
	}
	ca, e := x509.ParseCertificate(der)
	if e != nil {
		return nil, nil, e
	}
	roots := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	result := make([]bootstrapCert, len(nodes))
	for i, node := range nodes {
		key, x := ecdsa.GenerateKey(elliptic.P256(), random)
		if x != nil {
			return nil, nil, x
		}
		ext, x := rafttransport.PeerIdentityExtension(oid, rafttransport.PeerIdentity{TrustDomain: domain, Node: node})
		if x != nil {
			return nil, nil, x
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: "vibedb kubernetes test identity"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(7 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, ExtraExtensions: []pkix.Extension{ext}}
		leafDER, x := x509.CreateCertificate(random, leaf, ca, &key.PublicKey, caKey)
		if x != nil {
			return nil, nil, x
		}
		keyDER, x := x509.MarshalECPrivateKey(key)
		if x != nil {
			return nil, nil, x
		}
		result[i] = bootstrapCert{append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), roots...), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})}
	}
	return result, roots, nil
}

func appendConfigMap(w *bytes.Buffer, ns, name string, data map[string][]byte) {
	fmt.Fprintf(w, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s, namespace: %s}\nbinaryData:\n", name, ns)
	keys := sortedKeys(data)
	for _, k := range keys {
		fmt.Fprintf(w, "  %s: %s\n", k, base64.StdEncoding.EncodeToString(data[k]))
	}
	w.WriteString("---\n")
}
func appendSecret(w *bytes.Buffer, ns, name string, data map[string][]byte) {
	fmt.Fprintf(w, "apiVersion: v1\nkind: Secret\nmetadata: {name: %s, namespace: %s}\ntype: Opaque\ndata:\n", name, ns)
	keys := sortedKeys(data)
	for _, k := range keys {
		fmt.Fprintf(w, "  %s: %s\n", k, base64.StdEncoding.EncodeToString(data[k]))
	}
	w.WriteString("---\n")
}
func sortedKeys(m map[string][]byte) []string {
	r := make([]string, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	sort.Strings(r)
	return r
}

func writeExclusive(path string, raw []byte, mode os.FileMode) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if e != nil {
		return e
	}
	_, we := f.Write(raw)
	se := f.Sync()
	ce := f.Close()
	return errors.Join(we, se, ce)
}
func syncDirectory(path string) error {
	d, e := os.Open(path)
	if e != nil {
		return e
	}
	return errors.Join(d.Sync(), d.Close())
}

func probeBootstrapRole(role bootstrapRole) (replication.Digest, sqldriver.ReplicatedShardStoreLimits, error) {
	digest, limits, err := sqldriver.InitialReplicatedRelationManifest(role.table)
	return replication.Digest(digest), limits, err
}

func bootstrapLedgerIdentity(group raftmember.GroupKey) (result replication.Digest) {
	h := sha256.New()
	h.Write([]byte("vibedb/dev/request-ledger-home/format-0\x00"))
	appendGroup(h, group)
	var width [4]byte
	binary.BigEndian.PutUint32(width[:], uint32(len("request-ledger")))
	h.Write(width[:])
	h.Write([]byte("request-ledger"))
	binary.BigEndian.PutUint32(width[:], 3)
	h.Write(width[:])
	h.Write([]byte("all"))
	h.Write(make([]byte, 64))
	copy(result[:], h.Sum(nil))
	return
}
func appendGroup(w io.Writer, g raftmember.GroupKey) {
	w.Write(g.ClusterID[:])
	w.Write(g.ClusterIncarnation[:])
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], g.TopologyRecoveryEpoch)
	w.Write(b[:])
	w.Write(g.ShardIncarnation[:])
	w.Write(g.GroupID[:])
}
func authorityDigest(domain string, g raftmember.GroupKey, d distribution.DistributionName, s distribution.ShardID, m replication.Digest) (r replication.Digest) {
	h := sha256.New()
	h.Write([]byte(domain))
	appendGroup(h, g)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(d)))
	h.Write(b[:])
	h.Write([]byte(d))
	binary.BigEndian.PutUint32(b[:], uint32(len(s)))
	h.Write(b[:])
	h.Write([]byte(s))
	var gen [16]byte
	binary.BigEndian.PutUint64(gen[:8], 1)
	binary.BigEndian.PutUint64(gen[8:], 1)
	h.Write(gen[:])
	h.Write(m[:])
	copy(r[:], h.Sum(nil))
	return
}

func bootstrapCatalog(root string, roles [3]bootstrapRole) ([]byte, error) {
	endpoints := map[distribution.EndpointID]string{}
	manifests := make([]*distribution.Manifest, 3)
	descriptors := make([]gateway.ReplicatedShardDescriptor, 3)
	for ri, r := range roles {
		leaders := make([]distribution.EndpointID, 3)
		replicas := make([]gateway.ReplicatedReplicaDescriptor, 3)
		for mi := 0; mi < 3; mi++ {
			prefix := fmt.Sprintf("%s-member-%d", r.name, mi+1)
			leaders[mi] = distribution.EndpointID(prefix)
			native, control := distribution.EndpointID(prefix+"-native"), distribution.EndpointID(prefix+"-control")
			host := fmt.Sprintf("vibedb-%s-%d.vibedb-%s-peer", r.name, mi, r.name)
			endpoints[leaders[mi]] = host + ":7411"
			endpoints[native] = host + ":7511"
			endpoints[control] = host + ":7711"
			replicas[mi] = gateway.ReplicatedReplicaDescriptor{Member: uint64(mi + 1), Node: r.nodes[mi], StoreID: r.stores[mi], NodeIncarnation: 1, Endpoint: leaders[mi], NativeEndpoint: native, ControlEndpoint: control}
		}
		manifest, e := distribution.NewManifest(r.distribution, 1, []distribution.Shard{{ID: r.shard, AllocationGeneration: 1, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: leaders, Epoch: 1}})
		if e != nil {
			return nil, e
		}
		manifests[ri] = manifest
		rangeID := authorityDigest("vibedb/dev/range-identity/format-0\x00", r.group, r.distribution, r.shard, r.digest)
		lineage := authorityDigest("vibedb/dev/range-lineage/format-0\x00", r.group, r.distribution, r.shard, r.digest)
		forward := authorityDigest("vibedb/dev/forwarding-rule/format-0\x00", r.group, r.distribution, r.shard, r.digest)
		desc := gateway.ReplicatedShardDescriptor{Distribution: r.distribution, Shard: r.shard, Group: r.group, AllocationGeneration: 1, Command: raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RelationManifestDigest: r.digest, RoutingVersion: 1, RouteGeneration: 1}, RangeIdentity: rangeID, LineageDigest: lineage, ForwardingRuleDigest: forward, Replicas: replicas}
		if ri == 1 {
			desc.RequestLedgerRanges = []gateway.DurableRequestLedgerRangeDescriptor{{Identity: bootstrapLedgerIdentity(r.group)}}
		}
		descriptors[ri] = desc
	}
	config := distribution.ClusterConfig{Distributions: []distribution.DistributionSpec{{Name: roles[0].distribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion}, {Name: roles[1].distribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion}, {Name: roles[2].distribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion}}, Placements: []distribution.TablePlacement{{Table: roles[0].table, Distribution: roles[0].distribution, Columns: []string{roles[0].primary}}, {Table: roles[1].table, Distribution: roles[1].distribution, Columns: []string{roles[1].primary}}, {Table: roles[2].table, Distribution: roles[2].distribution, Columns: []string{roles[2].primary}}}, Manifests: manifests}
	profile := gateway.ReplicatedTableProfile{Table: roles[2].table, Relation: 1, PrimaryKey: roles[2].primary, SchemaGeneration: 1, RelationManifestDigest: roles[2].digest, MaxKeyBytes: uint16(roles[2].limits.MaxKeyBytes), MaxDocumentBytes: uint32(roles[2].limits.MaxDocumentBytes)}
	snapshot, e := gateway.NewSnapshotWithReplicatedTableMetadata(config, endpoints, 1, nil, nil, descriptors, []gateway.ReplicatedTableProfile{profile})
	if e != nil {
		return nil, e
	}
	path := filepath.Join(root, ".catalog.vibejson")
	if e = gateway.SaveSnapshot(path, snapshot); e != nil {
		return nil, e
	}
	raw, e := os.ReadFile(path)
	_ = os.Remove(path)
	_ = os.Remove(path + ".lock")
	return raw, e
}

func bootstrapReplicaControl(roles [3]bootstrapRole, state bootstrapState) ([]byte, error) {
	local := bootstrapEndpoint{
		Node: state.GatewayNodeID, Incarnation: 1,
		ControlAddress: "vibedb-gateway-0.vibedb-gateway-peer:7401",
	}
	m := bootstrapControl{
		Generation: 1, LocalGateway: local,
		TLS: bootstrapControlTLS{
			"/run/secrets/vibedb/gateway-cert.pem",
			"/run/secrets/vibedb/gateway-key.pem",
			"/run/secrets/vibedb/cluster-roots.pem",
			bootstrapOID,
			"/etc/vibedb/authorization-policy.vibejson",
		},
		Bounds:           bootstrapBounds{64, 16, 8, 100, 2000, 5000},
		GatewayEndpoints: []bootstrapEndpoint{local},
		SplitTemplate: bootstrapSplitTemplate{
			128, 8, durable.TxnLimits{MaxCollections: 16, MaxDocuments: 4096, MaxBytes: 384 << 20},
			1, "/id", 64, 16<<20 + 64*replication.MaxMutationKeyBytes,
			uint16(distribution.CurrentTupleVersion), uint16(distribution.NativeMapperVersion),
		},
	}
	for roleIndex, role := range roles {
		for member := 0; member < 3; member++ {
			host := fmt.Sprintf("vibedb-%s-%d.vibedb-%s-peer", role.name, member, role.name)
			node := state.ShardNodeIDs[roleIndex*3+member]
			m.ShardEndpoints = append(m.ShardEndpoints, bootstrapShardEndpoint{
				node, host + ":7711", host + ":7611", "/var/lib/vibedb/member/split-children",
			})
			m.Candidates = append(m.Candidates, bootstrapCandidate{
				uint64(100 + roleIndex*3 + member), node,
				hex.EncodeToString(role.candidateStores[member][:]), 1,
				fmt.Sprintf("%s-member-%d", role.name, member+1), 0,
			})
		}
	}
	sort.Slice(m.ShardEndpoints, func(i, j int) bool { return m.ShardEndpoints[i].Node < m.ShardEndpoints[j].Node })
	sort.Slice(m.Candidates, func(i, j int) bool { return m.Candidates[i].Node < m.Candidates[j].Node })
	return vibejson.Marshal(&m)
}
