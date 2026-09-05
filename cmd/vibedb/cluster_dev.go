package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3qualification"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

const (
	// Format 2 is the first format that records physical-node composition.
	// Format 1 described one OS process per role/member and is deliberately
	// rejected on restart; there is no migration path for that unreleased
	// development layout.
	devClusterFormat                                           = 2
	devClusterRF3                                              = 3
	devClusterRF1                                              = 1
	devClusterPhysicalNodes3                                   = 3
	devClusterPhysicalNodes6                                   = 6
	devClusterOID                                              = "1.3.6.1.4.1.32473.1.1"
	devReadyTimeout                                            = 30 * time.Second
	devChildDiagnosticBytes                                    = 64 << 10
	devChildLogDrainTimeout                                    = 250 * time.Millisecond
	devChildReadyMarkerBytes                                   = 256
	devLedgerDistribution        distribution.DistributionName = "request-ledger"
	devLedgerShard               distribution.ShardID          = "all"
	devLedgerTable                                             = "request_ledger"
	devLedgerPrimaryKey                                        = "/id"
	devLedgerCapacityBytes                                     = 64 << 20
	devLedgerCleanupReserveBytes                               = 8 << 20
	devDataDistribution          distribution.DistributionName = "data"
	devDataShard                 distribution.ShardID          = "all"
	devDataTable                                               = "documents"
	devDataPrimaryKey                                          = "/id"
	devSQLCatalogMaxBytes                                      = 16 << 20
)

var errDevCluster = errors.New("vibedb: invalid local development cluster")

type devClusterManifest struct {
	additionalCatalogs []string
	dataServeManifests []string
	Format             uint16 `json:"format"`
	// Nodes retains the historical replication-factor spelling used by the
	// catalog and existing operator scripts. PhysicalNodes is the number of
	// serving OS processes and is independent from RF.
	Nodes               uint8              `json:"nodes"`
	Replicas            uint8              `json:"replicas,omitempty"`
	PhysicalNodes       uint8              `json:"physical_nodes,omitempty"`
	NodeLog             bool               `json:"node_log,omitempty"`
	ClientEndpoint      string             `json:"client_endpoint"`
	CatalogPath         string             `json:"catalog_path"`
	GatewayCertificate  string             `json:"gateway_certificate"`
	GatewayKey          string             `json:"gateway_key"`
	ClientCertificate   string             `json:"client_certificate"`
	ClientKey           string             `json:"client_key"`
	ClientNode          string             `json:"client_node"`
	Roots               string             `json:"roots"`
	AuthorizationPolicy string             `json:"authorization_policy"`
	HotShardCapacity    string             `json:"hot_shard_capacity"`
	ReplicaControl      string             `json:"replica_control"`
	DurableAckKey       string             `json:"durable_ack_key"`
	GatewayNode         string             `json:"gateway_node"`
	GatewayControl      string             `json:"gateway_control"`
	ReadAuthority       *devReadAuthority  `json:"read_authority,omitempty"`
	Members             []devClusterMember `json:"members"`
	LedgerMembers       []devClusterMember `json:"ledger_members"`
	DataMembers         []devClusterMember `json:"data_members"`
	NodeManifests       []devPhysicalNode  `json:"node_manifests,omitempty"`
}

type devClusterMember struct {
	Member        uint64 `json:"member"`
	Node          string `json:"node"`
	Store         string `json:"store"`
	PhysicalNode  string `json:"physical_node,omitempty"`
	GroupRoot     string `json:"group_root,omitempty"`
	Peer          string `json:"peer"`
	Native        string `json:"native"`
	Snapshot      string `json:"snapshot"`
	Control       string `json:"control"`
	ServeManifest string `json:"serve_manifest"`
}

// devReadAuthority is copied verbatim into every RF3 member preparation. The
// flag is opt-in; an absent section is the durable default-off contract.
type devReadAuthority struct {
	Enabled              bool                         `json:"enabled"`
	FeatureVersion       uint32                       `json:"feature_version"`
	PolicyVersion        uint32                       `json:"policy_version"`
	MaxGrantMillis       uint64                       `json:"max_grant_millis"`
	ClockRatePPM         uint32                       `json:"clock_rate_ppm"`
	RoundingMarginMillis uint64                       `json:"rounding_margin_millis"`
	Voters               []uint64                     `json:"voters"`
	Capabilities         []devReadAuthorityCapability `json:"capabilities"`
}

type devReadAuthorityCapability struct {
	MemberID      uint64 `json:"member_id"`
	PolicyVersion uint32 `json:"policy_version"`
	Enabled       bool   `json:"enabled"`
}

func newDevReadAuthority(enabled bool) *devReadAuthority {
	if !enabled {
		return nil
	}
	return &devReadAuthority{
		Enabled: true, FeatureVersion: 1, PolicyVersion: 1,
		MaxGrantMillis: 5000, ClockRatePPM: 100000, RoundingMarginMillis: 1,
		Voters: []uint64{1, 2, 3},
		Capabilities: []devReadAuthorityCapability{
			{MemberID: 1, PolicyVersion: 1, Enabled: true},
			{MemberID: 2, PolicyVersion: 1, Enabled: true},
			{MemberID: 3, PolicyVersion: 1, Enabled: true},
		},
	}
}

func validDevReadAuthority(config devReadAuthority) bool {
	want := newDevReadAuthority(true)
	if want == nil || config.Enabled != want.Enabled || config.FeatureVersion != want.FeatureVersion ||
		config.PolicyVersion != want.PolicyVersion || config.MaxGrantMillis != want.MaxGrantMillis ||
		config.ClockRatePPM != want.ClockRatePPM || config.RoundingMarginMillis != want.RoundingMarginMillis ||
		!slices.Equal(config.Voters, want.Voters) || !slices.Equal(config.Capabilities, want.Capabilities) {
		return false
	}
	return true
}

func devReadAuthorityEqual(left, right *devReadAuthority) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftRaw, leftErr := vibejson.Marshal(left)
	rightRaw, rightErr := vibejson.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

func cloneDevReadAuthority(config *devReadAuthority) *devReadAuthority {
	if config == nil {
		return nil
	}
	clone := *config
	clone.Voters = slices.Clone(config.Voters)
	clone.Capabilities = slices.Clone(config.Capabilities)
	return &clone
}

// validateDevReadAuthorityRaw keeps a cluster-level opt-in from being lost
// when an old prepared member or serve manifest is reused. The shard parser
// performs the complete strict grammar check; this launcher check binds every
// generated child artifact back to the cluster's immutable feature section.
func validateDevReadAuthorityRaw(raw []byte, expected *devReadAuthority) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		// Before the read-authority section was introduced, restart validation
		// retained opaque child artifacts as-is. Preserve that default-off
		// resume contract, while enabled clusters still require a structured
		// artifact below and therefore never accept an opaque replacement.
		if expected == nil {
			return nil
		}
		return errors.Join(errDevCluster, err)
	}
	encoded, present := fields["read_authority"]
	if !present {
		if expected == nil {
			return nil
		}
		return fmt.Errorf("%w: prepared artifact omits enabled read authority", errDevCluster)
	}
	if len(encoded) == 0 || bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
		return fmt.Errorf("%w: invalid read authority section", errDevCluster)
	}
	var actual devReadAuthority
	if err := vibejson.Unmarshal(encoded, &actual); err != nil {
		return errors.Join(errDevCluster, err)
	}
	if expected == nil || !validDevReadAuthority(actual) || !devReadAuthorityEqual(&actual, expected) {
		return fmt.Errorf("%w: prepared artifact read authority differs from cluster policy", errDevCluster)
	}
	return nil
}

func validateDevReadAuthorityFile(path string, expected *devReadAuthority) error {
	raw, err := readDevFile(path, 4<<20)
	if err != nil {
		return err
	}
	return validateDevReadAuthorityRaw(raw, expected)
}

// devPhysicalNode is the supervisor inventory for one serving process. The
// storage identity and gateway identity intentionally have separate keypairs.
// Groups remain listed in the node's canonical serve manifest; this inventory
// only supplies the stable process-level paths and listener endpoints needed
// by the development supervisor and diagnostics.
type devPhysicalNode struct {
	Node                  string   `json:"node"`
	Certificate           string   `json:"certificate"`
	Key                   string   `json:"key"`
	GatewayNode           string   `json:"gateway_node"`
	GatewayCertificate    string   `json:"gateway_certificate"`
	GatewayKey            string   `json:"gateway_key"`
	GatewayControl        string   `json:"gateway_control"`
	FrontendListen        string   `json:"frontend_listen"`
	ServeManifest         string   `json:"serve_manifest"`
	CatalogSessionJournal string   `json:"catalog_session_journal"`
	DirectIssuerJournal   string   `json:"direct_issuer_journal"`
	FallbackJournal       string   `json:"fallback_journal"`
	ExecutionPinJournal   string   `json:"execution_pin_journal"`
	Groups                []string `json:"groups"`
}

// devGatewayConfig is the serializable subset of gatewayruntime.Config that
// belongs in a prepared node manifest. Function, listener, and transport
// interfaces are assembled by serve-node; paths and scalar bounds remain
// explicit so startup can authenticate and validate each frontend before it
// starts public admission.
type devGatewayConfig struct {
	CatalogPath                 string                `json:"catalog_path"`
	CatalogRouteSeedPath        string                `json:"catalog_route_seed_path"`
	CatalogBootstrapIfMissing   bool                  `json:"catalog_bootstrap_if_missing"`
	CatalogRelation             uint64                `json:"catalog_relation"`
	CatalogAttempts             uint64                `json:"catalog_attempts"`
	CatalogAttemptTimeoutMillis uint64                `json:"catalog_attempt_timeout_millis"`
	CatalogSessionLeaseMillis   uint64                `json:"catalog_session_lease_millis"`
	CatalogSessionJournal       string                `json:"catalog_session_journal"`
	CatalogClientID             string                `json:"catalog_client_id"`
	CatalogRetryHome            string                `json:"catalog_retry_home"`
	DurableAckKey               string                `json:"durable_ack_key"`
	Listen                      string                `json:"listen"`
	PGListen                    string                `json:"pg_listen"`
	PGDDLSocket                 string                `json:"pg_ddl_socket"`
	TLS                         devGatewayTLS         `json:"tls"`
	TLSHandshakeTimeoutMillis   uint64                `json:"tls_handshake_timeout_millis"`
	AuthorizationPolicy         string                `json:"authorization_policy"`
	ShardPeers                  []devGatewayShardPeer `json:"shard_peers"`
	MaxConnections              uint64                `json:"max_connections"`
	MaxHandshakes               uint64                `json:"max_handshakes"`
	MaxShardConnections         uint64                `json:"max_shard_connections"`
	MaxShardHandshakes          uint64                `json:"max_shard_handshakes"`
	MaxNativeReadConcurrency    uint64                `json:"max_native_read_concurrency"`
	MaxNativeReadBytes          uint64                `json:"max_native_read_bytes"`
	MaxNativeScatterConcurrency uint64                `json:"max_native_scatter_concurrency"`
	TableCatalogs               []string              `json:"table_catalogs"`
	TableCatalogsPath           string                `json:"table_catalogs_path"`
	HotShardCapacity            string                `json:"hot_shard_capacity"`
	HotShardIntervalMillis      uint64                `json:"hot_shard_interval_millis"`
	ReplicaControlManifest      string                `json:"replica_control_manifest"`
	ControlParticipantOnly      bool                  `json:"control_participant_only"`
	DDLOwnerAddress             string                `json:"ddl_owner_address"`
	DDLOwnerNode                string                `json:"ddl_owner_node"`
	BackupRepository            string                `json:"backup_repository"`
	BackupMaxBackups            uint64                `json:"backup_max_backups"`
	BackupMaxArtifacts          uint64                `json:"backup_max_artifacts"`
	BackupMaxArtifactBytes      uint64                `json:"backup_max_artifact_bytes"`
	BackupMaxDiskBytes          uint64                `json:"backup_max_disk_bytes"`
	ControllerIntervalMillis    uint64                `json:"controller_interval_millis"`
	SchemaRolloutPlan           string                `json:"schema_rollout_plan"`
	SchemaRolloutOnce           bool                  `json:"schema_rollout_once"`
}

type devGatewayTLS struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}

type devGatewayShardPeer struct {
	Address string `json:"address"`
	NodeID  string `json:"node_id"`
}

// These persisted types deliberately mirror the gateway's strict public
// replica-control grammar without importing command-local implementation.
// The gateway canonical-decodes this file again before granting authority.
type devReplicaControlManifest struct {
	Generation       uint64                       `json:"generation"`
	LocalGateway     devReplicaControlEndpoint    `json:"local_gateway"`
	TLS              devReplicaControlTLS         `json:"tls"`
	Bounds           devReplicaControlBounds      `json:"bounds"`
	ShardEndpoints   []devReplicaControlShard     `json:"shard_endpoints"`
	GatewayEndpoints []devReplicaControlEndpoint  `json:"gateway_endpoints"`
	Candidates       []devReplicaControlCandidate `json:"candidates"`
	SplitSources     []devReplicaSplitSource      `json:"split_sources"`
}

type devReplicaControlTLS struct {
	Certificate         string `json:"certificate"`
	Key                 string `json:"key"`
	Roots               string `json:"roots"`
	IdentityOID         string `json:"identity_oid"`
	AuthorizationPolicy string `json:"authorization_policy"`
}

type devReplicaControlBounds struct {
	MaxConnections      uint32 `json:"max_connections"`
	MaxHandshakes       uint32 `json:"max_handshakes"`
	MaxConcurrentDrains uint32 `json:"max_concurrent_drains"`
	ControllerInterval  uint64 `json:"controller_interval_millis"`
	ReadTimeout         uint64 `json:"read_timeout_millis"`
	WriteTimeout        uint64 `json:"write_timeout_millis"`
}

type devReplicaControlShard struct {
	Node                 string `json:"node"`
	ControlAddress       string `json:"control_address"`
	SplitSnapshotAddress string `json:"split_snapshot_address"`
}

type devReplicaSplitSource struct {
	ClusterID              [16]byte                               `json:"cluster_id"`
	ClusterIncarnation     [16]byte                               `json:"cluster_incarnation"`
	TopologyRecoveryEpoch  uint64                                 `json:"topology_recovery_epoch"`
	ShardIncarnation       [16]byte                               `json:"shard_incarnation"`
	GroupID                [16]byte                               `json:"group_id"`
	SchemaGeneration       uint64                                 `json:"schema_generation"`
	RelationManifestDigest [32]byte                               `json:"relation_manifest_digest"`
	Table                  string                                 `json:"table"`
	SQL                    sqldriver.ReplicatedShardStoreIdentity `json:"sql"`
	Placement              devReplicaSplitPlacement               `json:"placement"`
	LocalIndexes           []devReplicaSplitIndex                 `json:"local_indexes,omitempty"`
	Template               devReplicaSplitTemplate                `json:"template"`
	Replicas               []devReplicaSplitSourceReplica         `json:"replicas"`
}

type devReplicaSplitPlacement struct {
	Format        uint16  `json:"format"`
	ShardKey      string  `json:"shard_key"`
	TupleVersion  uint16  `json:"tuple_version"`
	MapperVersion uint16  `json:"mapper_version"`
	RangeStart    [8]byte `json:"range_start"`
	RangeEnd      [8]byte `json:"range_end"`
	RangeEndMax   bool    `json:"range_end_max"`
}

type devReplicaSplitIndex struct {
	Name  string   `json:"name"`
	Paths []string `json:"paths"`
}

type devReplicaSplitSourceReplica struct {
	Node      string `json:"node"`
	ChildRoot string `json:"child_root"`
}

type devReplicaControlEndpoint struct {
	Node           string `json:"node"`
	Incarnation    uint64 `json:"incarnation"`
	ControlAddress string `json:"control_address"`
}

type devReplicaControlCandidate struct {
	Member          uint64 `json:"member"`
	Node            string `json:"node"`
	Store           string `json:"store"`
	NodeIncarnation uint64 `json:"node_incarnation"`
	Endpoint        string `json:"endpoint"`
	Load            uint64 `json:"load"`
}

type devReplicaSplitTemplate struct {
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

type devPreparedRoute struct {
	leaders          []distribution.EndpointID
	replicas         []gateway.ReplicatedReplicaDescriptor
	digest           [sha256.Size]byte
	applyDigest      [sha256.Size]byte
	schemaGeneration uint64
	table            gateway.ReplicatedTableProfile
}

type devPrepareManifest struct {
	Root                  string                 `json:"root"`
	Distribution          string                 `json:"distribution"`
	Shard                 string                 `json:"shard"`
	ClusterID             string                 `json:"cluster_id"`
	ClusterIncarnation    string                 `json:"cluster_incarnation"`
	TopologyRecoveryEpoch uint64                 `json:"topology_recovery_epoch"`
	AllocationGeneration  uint64                 `json:"allocation_generation"`
	ShardIncarnation      string                 `json:"shard_incarnation"`
	GroupID               string                 `json:"group_id"`
	MemberID              uint64                 `json:"member_id"`
	StoreID               string                 `json:"store_id"`
	Table                 string                 `json:"table"`
	CreateTable           string                 `json:"create_table"`
	Authority             devPrepareAuthority    `json:"authority"`
	WAL                   devPrepareWAL          `json:"wal"`
	Apply                 devPrepareApply        `json:"apply"`
	Listeners             devPrepareListeners    `json:"listeners"`
	TLS                   devPrepareTLS          `json:"tls"`
	AuthorizationPolicy   string                 `json:"authorization_policy"`
	SplitControl          devPrepareSplitControl `json:"split_control"`
	DevelopmentOnly       bool                   `json:"development_only,omitempty"`
	ReadAuthority         *devReadAuthority      `json:"read_authority,omitempty"`
	Members               []devPrepareMember     `json:"members"`
}
type devPrepareAuthority struct {
	ActivePolicyGeneration uint64 `json:"active_policy_generation"`
	ProtectionEpoch        uint64 `json:"protection_epoch"`
	OwnershipEpoch         uint64 `json:"ownership_epoch"`
	SchemaGeneration       uint64 `json:"schema_generation"`
	RoutingVersion         uint64 `json:"routing_version"`
	RouteGeneration        uint64 `json:"route_generation"`
}
type devPrepareWAL struct {
	KeyID           string `json:"key_id"`
	KeyMaterialPath string `json:"key_material_path"`
	WrappedKey      string `json:"wrapped_key"`
	MaxFileBytes    int64  `json:"max_file_bytes"`
	MaxRecordBytes  int    `json:"max_record_bytes"`
	MaxRecords      uint64 `json:"max_records"`
	MaxEntries      uint64 `json:"max_entries"`
	MaxLiveBytes    int64  `json:"max_live_bytes"`
}
type devPrepareApply struct {
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
type devPrepareListeners struct {
	Peer     string `json:"peer"`
	Native   string `json:"native"`
	Snapshot string `json:"snapshot"`
	Control  string `json:"control"`
}
type devPrepareTLS struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}
type devPrepareSplitControl struct {
	MaxRecords           int                     `json:"max_records"`
	MaxFileBytes         int64                   `json:"max_file_bytes"`
	Grants               []devPrepareActionGrant `json:"grants"`
	MaxChildOperations   int                     `json:"max_child_operations"`
	StageCheckpointBytes uint64                  `json:"stage_checkpoint_bytes"`
}
type devPrepareActionGrant struct {
	NodeID  string `json:"node_id"`
	Actions uint16 `json:"actions"`
}
type devPrepareMember struct {
	MemberID      uint64 `json:"member_id"`
	NodeID        string `json:"node_id"`
	PeerAddress   string `json:"peer_address"`
	StoreID       string `json:"store_id,omitempty"`
	NativeAddress string `json:"native_address,omitempty"`
}
type devPolicy struct {
	Generation uint64         `json:"generation"`
	Principals []devPrincipal `json:"principals"`
}
type devPrincipal struct {
	Node         string   `json:"node"`
	Capabilities []string `json:"capabilities"`
}

type devClusterOptions struct {
	root, shardBinary, gatewayBinary string
	replicas                         int
	nodeLog                          bool
	physicalNodes                    int
	pgListen                         string
	pgListens                        []string
	readAuthority                    bool
}

func runClusterDev(args []string) int {
	fs := flag.NewFlagSet("cluster dev", flag.ContinueOnError)
	root := fs.String("root", "", "absolute durable cluster directory")
	replicas := fs.Int("replicas", devClusterRF3, "Raft replicas: 1 for dev-only/no-HA or 3 for RF3")
	physicalNodes := fs.Int("physical-nodes", 0, "serving physical nodes: 3 or 6 for RF3; defaults to 3")
	nodeLog := fs.Bool("node-log", false, "use shared node logs for a fresh RF3 cluster")
	nodes := fs.Int("nodes", 0, "deprecated alias for --replicas")
	shardBinary := fs.String("shard-binary", "", "vibedb-shard executable; defaults beside vibedb or PATH")
	gatewayBinary := fs.String("gateway-binary", "", "vibedb-gateway executable; defaults beside vibedb or PATH")
	diagnosticsOnExit := fs.Bool("diagnostics-on-exit", false, "print bounded shard and gateway log tails when the development cluster stops")
	pgListen := fs.String("pg-listen", "", "optional loopback PostgreSQL endpoint with durable auto-commit writes (RF3 only)")
	pgListens := fs.String("pg-listens", "", "comma-separated PostgreSQL loopback endpoints, one per physical node (RF3 only)")
	readAuthority := fs.Bool("read-authority", false, "explicitly enable quorum read authority on every RF3 physical-node voter")
	var tableSchemas []string
	fs.Func("table-schema", "CREATE TABLE file to provision as an additional RF3 group; repeatable and retained on restart", func(path string) error {
		tableSchemas = append(tableSchemas, path)
		return nil
	})
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *root == "" {
		usage()
		return 2
	}
	if *readAuthority && !rf3qualification.ReadAuthorityEnabled {
		fmt.Fprintf(os.Stderr, "cluster dev: --read-authority requires the explicitly tagged laboratory build %q\n",
			rf3qualification.ReadAuthorityLabBuildTag)
		return 2
	}
	replicasSet, nodesSet := false, false
	fs.Visit(func(f *flag.Flag) {
		replicasSet = replicasSet || f.Name == "replicas"
		nodesSet = nodesSet || f.Name == "nodes"
	})
	if nodesSet {
		if (*nodes != devClusterRF1 && *nodes != devClusterRF3) ||
			replicasSet && *nodes != *replicas {
			usage()
			return 2
		}
		*replicas = *nodes
	}
	if *replicas != devClusterRF1 && *replicas != devClusterRF3 || *nodeLog && *replicas != devClusterRF3 || *readAuthority && *replicas != devClusterRF3 {
		usage()
		return 2
	}
	if *replicas == devClusterRF1 {
		if *physicalNodes != 0 {
			usage()
			return 2
		}
		*physicalNodes = 0
	} else {
		if *physicalNodes == 0 {
			*physicalNodes = devClusterPhysicalNodes3
		}
		if *physicalNodes != devClusterPhysicalNodes3 && *physicalNodes != devClusterPhysicalNodes6 {
			fmt.Fprintln(os.Stderr, "cluster dev: --physical-nodes requires 3 or 6 for RF3")
			return 2
		}
	}
	listeners, err := devPhysicalPGListens(*pgListen, *pgListens, *replicas, *physicalNodes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster dev: PostgreSQL endpoints: %v\n", err)
		return 2
	}
	if !filepath.IsAbs(*root) || filepath.Clean(*root) != *root {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", errDevCluster)
		return 2
	}
	abs, err := filepath.Abs(*root)
	if err != nil || filepath.Clean(abs) != abs || abs == string(filepath.Separator) {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", errDevCluster)
		return 2
	}
	shard, err := resolveDevBinary(*shardBinary, "vibedb-shard")
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", err)
		return 1
	}
	gw := ""
	// RF3 frontends are embedded in the shard process. Keep resolving the
	// optional gateway binary only for callers that explicitly request the
	// legacy RF1 path; RF3 must not start a ninth role process.
	if *replicas == devClusterRF1 && *gatewayBinary != "" {
		gw, err = resolveDevBinary(*gatewayBinary, "vibedb-gateway")
		if err != nil {
			fmt.Fprintf(os.Stderr, "cluster dev: %v\n", err)
			return 1
		}
	}
	unlock, err := lockDevCluster(abs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster dev: acquire supervisor ownership: %v\n", err)
		return 1
	}
	defer unlock()
	manifest, err := ensureDevCluster(devClusterOptions{root: abs, replicas: *replicas, shardBinary: shard, gatewayBinary: gw, nodeLog: *nodeLog, physicalNodes: *physicalNodes, pgListen: *pgListen, pgListens: listeners, readAuthority: *readAuthority})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", err)
		return 1
	}
	if len(tableSchemas) == 0 {
		tableSchemas = []string{""}
	}
	for _, schema := range tableSchemas {
		if err := ensureDevTables(abs, shard, &manifest, schema); err != nil {
			fmt.Fprintf(os.Stderr, "cluster dev: prepare additional tables: %v\n", err)
			return 1
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var diagnostics io.Writer
	if *diagnosticsOnExit {
		diagnostics = os.Stderr
	}
	if err := serveDevCluster(ctx, manifest, shard, gw, diagnostics, *pgListen); err != nil {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", err)
		return 1
	}
	return 0
}

func resolveDevBinary(explicit, name string) (string, error) {
	if explicit != "" {
		path, err := filepath.Abs(explicit)
		if err != nil {
			return "", err
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
			return "", errors.Join(errDevCluster, err)
		}
		return path, nil
	}
	self, err := os.Executable()
	if err == nil {
		sibling := filepath.Join(filepath.Dir(self), name)
		if info, e := os.Stat(sibling); e == nil && info.Mode().IsRegular() && info.Mode()&0o111 != 0 {
			return sibling, nil
		}
	}
	return exec.LookPath(name)
}

func ensureDevCluster(options devClusterOptions) (devClusterManifest, error) {
	if options.readAuthority && !rf3qualification.ReadAuthorityEnabled {
		return devClusterManifest{}, fmt.Errorf("%w: read authority requires the explicitly tagged laboratory build %q", errDevCluster, rf3qualification.ReadAuthorityLabBuildTag)
	}
	if options.replicas != devClusterRF1 && options.replicas != devClusterRF3 {
		return devClusterManifest{}, errDevCluster
	}
	if options.replicas == devClusterRF3 && options.physicalNodes != 0 &&
		options.physicalNodes != devClusterPhysicalNodes3 && options.physicalNodes != devClusterPhysicalNodes6 {
		return devClusterManifest{}, fmt.Errorf("%w: physical node count must be 3 or 6", errDevCluster)
	}
	if options.replicas == devClusterRF1 && options.physicalNodes != 0 {
		return devClusterManifest{}, fmt.Errorf("%w: RF1 does not support physical-node composition", errDevCluster)
	}
	if options.readAuthority && (options.replicas != devClusterRF3 || options.physicalNodes == 0) {
		return devClusterManifest{}, fmt.Errorf("%w: read authority requires RF3 physical-node serving", errDevCluster)
	}
	manifestPath := filepath.Join(options.root, "cluster.vibejson")
	if raw, err := readDevFile(manifestPath, 1<<20); err == nil {
		var m devClusterManifest
		if vibejson.Unmarshal(raw, &m) != nil {
			return m, errDevCluster
		}
		canonical, e := vibejson.Marshal(&m)
		if e != nil || !bytes.Equal(raw, canonical) || !validDevManifest(m, options.root) ||
			m.Nodes != uint8(options.replicas) ||
			options.physicalNodes == 0 && m.NodeLog != options.nodeLog ||
			options.physicalNodes != 0 && !m.NodeLog ||
			options.physicalNodes != 0 && m.PhysicalNodes != uint8(options.physicalNodes) {
			return m, errDevCluster
		}
		if (m.ReadAuthority != nil) != options.readAuthority || options.readAuthority && !validDevReadAuthority(*m.ReadAuthority) {
			return m, errDevCluster
		}
		if m.PhysicalNodes != 0 {
			if err := validateDevPhysicalPGOptions(m, options); err != nil {
				return m, err
			}
			return m, completeDevPhysicalCluster(options, m)
		}
		return m, completeDevCluster(options, m)
	} else if !errors.Is(err, os.ErrNotExist) {
		return devClusterManifest{}, err
	}
	if entries, err := os.ReadDir(options.root); err == nil {
		for _, entry := range entries {
			if entry.Name() != devClusterLockName || !entry.Type().IsRegular() {
				return devClusterManifest{}, errDevCluster
			}
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return devClusterManifest{}, err
	}
	if err := os.MkdirAll(options.root, 0o700); err != nil {
		return devClusterManifest{}, err
	}
	return initializeDevCluster(options, manifestPath)
}

func initializeDevCluster(options devClusterOptions, manifestPath string) (devClusterManifest, error) {
	if options.readAuthority && !rf3qualification.ReadAuthorityEnabled {
		return devClusterManifest{}, fmt.Errorf("%w: read authority requires the explicitly tagged laboratory build %q", errDevCluster, rf3qualification.ReadAuthorityLabBuildTag)
	}
	if options.replicas == devClusterRF3 && options.physicalNodes != 0 {
		return initializeDevPhysicalCluster(options, manifestPath)
	}
	var clusterID, clusterIncarnation [16]byte
	var catalogShardIncarnation, catalogGroupID [16]byte
	var ledgerShardIncarnation, ledgerGroupID [16]byte
	var dataShardIncarnation, dataGroupID [16]byte
	for _, dst := range [][]byte{
		clusterID[:], clusterIncarnation[:], catalogShardIncarnation[:], catalogGroupID[:],
		ledgerShardIncarnation[:], ledgerGroupID[:], dataShardIncarnation[:], dataGroupID[:],
	} {
		if _, err := io.ReadFull(cryptorand.Reader, dst); err != nil {
			return devClusterManifest{}, err
		}
	}
	// Each independently serving process owns one physical NodeID. Sharing a
	// NodeID across role-local control listeners would make authenticated
	// replica routing ambiguous and cannot be represented by the strict control
	// manifest. The final two identities belong to the gateway and its client.
	// A client must never authenticate to a service using that service's key.
	nodes := make([]rafttransport.NodeID, options.replicas*3+2)
	gatewayIndex, clientIndex := options.replicas*3, options.replicas*3+1
	stores := make([][16]byte, options.replicas*3)
	for i := range nodes {
		if _, err := io.ReadFull(cryptorand.Reader, nodes[i][:]); err != nil {
			return devClusterManifest{}, err
		}
	}
	for i := range stores {
		if _, err := io.ReadFull(cryptorand.Reader, stores[i][:]); err != nil {
			return devClusterManifest{}, err
		}
	}
	ports, err := reserveDevPorts(2 + options.replicas*12)
	if err != nil {
		return devClusterManifest{}, err
	}
	credentials, roots, err := writeDevCredentials(options.root, rafttransport.TrustDomain{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation}, nodes)
	if err != nil {
		return devClusterManifest{}, err
	}
	policyPath := filepath.Join(options.root, "authorization-policy.vibejson")
	if err := writeDevPolicy(policyPath, nodes[:clientIndex], nodes[clientIndex]); err != nil {
		return devClusterManifest{}, err
	}
	durableAckKeyPath := filepath.Join(options.root, "durable-ack-key")
	var durableAckKey [sha256.Size]byte
	if _, err := io.ReadFull(cryptorand.Reader, durableAckKey[:]); err != nil {
		return devClusterManifest{}, err
	}
	var durableAckKeyHex [sha256.Size * 2]byte
	hex.Encode(durableAckKeyHex[:], durableAckKey[:])
	clear(durableAckKey[:])
	if err := writeDevExclusive(durableAckKeyPath, durableAckKeyHex[:], 0o600); err != nil {
		clear(durableAckKeyHex[:])
		return devClusterManifest{}, err
	}
	clear(durableAckKeyHex[:])
	keySource := filepath.Join(options.root, "wal-key-source")
	keyMaterial := make([]byte, 32)
	if _, err := io.ReadFull(cryptorand.Reader, keyMaterial); err != nil {
		return devClusterManifest{}, err
	}
	if err := writeDevExclusive(keySource, keyMaterial, 0o600); err != nil {
		return devClusterManifest{}, err
	}
	clear(keyMaterial)
	m := devClusterManifest{Format: devClusterFormat, Nodes: uint8(options.replicas), NodeLog: options.nodeLog, ReadAuthority: newDevReadAuthority(options.readAuthority), ClientEndpoint: ports[0], CatalogPath: filepath.Join(options.root, "catalog.vibejson"), GatewayCertificate: credentials[gatewayIndex][0], GatewayKey: credentials[gatewayIndex][1], Roots: roots, AuthorizationPolicy: policyPath, HotShardCapacity: filepath.Join(options.root, "hot-shard-capacity.vibejson"), ReplicaControl: filepath.Join(options.root, "replica-control.vibejson"), DurableAckKey: durableAckKeyPath, GatewayNode: hex.EncodeToString(nodes[gatewayIndex][:]), GatewayControl: ports[1+options.replicas*12], Members: make([]devClusterMember, options.replicas), LedgerMembers: make([]devClusterMember, options.replicas), DataMembers: make([]devClusterMember, options.replicas)}
	m.ClientCertificate, m.ClientKey = credentials[clientIndex][0], credentials[clientIndex][1]
	m.ClientNode = hex.EncodeToString(nodes[clientIndex][:])
	catalogPrepareMembers := make([]devPrepareMember, options.replicas)
	ledgerPrepareMembers := make([]devPrepareMember, options.replicas)
	dataPrepareMembers := make([]devPrepareMember, options.replicas)
	for i := 0; i < options.replicas; i++ {
		catalogBase := 1 + i*4
		ledgerBase := 1 + options.replicas*4 + i*4
		dataBase := 1 + options.replicas*8 + i*4
		catalogPrepareMembers[i] = devPrepareMember{MemberID: uint64(i + 1), NodeID: hex.EncodeToString(nodes[i][:]), PeerAddress: ports[catalogBase]}
		ledgerPrepareMembers[i] = devPrepareMember{MemberID: uint64(i + 1), NodeID: hex.EncodeToString(nodes[options.replicas+i][:]), PeerAddress: ports[ledgerBase]}
		dataPrepareMembers[i] = devPrepareMember{MemberID: uint64(i + 1), NodeID: hex.EncodeToString(nodes[options.replicas*2+i][:]), PeerAddress: ports[dataBase]}
		if options.readAuthority {
			catalogPrepareMembers[i].NativeAddress = ports[catalogBase+1]
			ledgerPrepareMembers[i].NativeAddress = ports[ledgerBase+1]
			dataPrepareMembers[i].NativeAddress = ports[dataBase+1]
		}
	}
	authority := devPrepareAuthority{ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1}
	ledgerGroup := raftmember.GroupKey{
		ClusterID: clusterID, ClusterIncarnation: clusterIncarnation,
		TopologyRecoveryEpoch: 1, ShardIncarnation: ledgerShardIncarnation,
		GroupID: ledgerGroupID,
	}
	ledgerHomeIdentity := deriveDevLedgerHomeIdentity(ledgerGroup)
	for roleIndex, role := range []struct {
		name, distribution, shard, table, createTable, shardKey string
		shardIncarnation, groupID                               [16]byte
		prepareMembers                                          []devPrepareMember
		members                                                 *[]devClusterMember
		requestLedger                                           bool
	}{
		{"catalog", string(gateway.ReplicatedCatalogDistribution), string(gateway.ReplicatedCatalogShard), gateway.ReplicatedCatalogTable, "CREATE TABLE controlplane (PRIMARY KEY (id))", gateway.ReplicatedCatalogPrimaryKey, catalogShardIncarnation, catalogGroupID, catalogPrepareMembers, &m.Members, false},
		{"ledger", string(devLedgerDistribution), string(devLedgerShard), devLedgerTable, "CREATE TABLE request_ledger (PRIMARY KEY (id))", devLedgerPrimaryKey, ledgerShardIncarnation, ledgerGroupID, ledgerPrepareMembers, &m.LedgerMembers, true},
		{"data", string(devDataDistribution), string(devDataShard), devDataTable, "CREATE TABLE documents (PRIMARY KEY (id))", devDataPrimaryKey, dataShardIncarnation, dataGroupID, dataPrepareMembers, &m.DataMembers, false},
	} {
		apply := devPrepareApplyProfile(role.shardKey, replication.Digest{})
		if role.requestLedger {
			apply = devPrepareApplyProfile(role.shardKey, ledgerHomeIdentity)
		}
		splitControl := devPrepareSplitControlProfile(role.prepareMembers, m.GatewayNode)
		for i := 0; i < options.replicas; i++ {
			base := 1 + roleIndex*options.replicas*4 + i*4
			memberRoot := filepath.Join(options.root, fmt.Sprintf("%s-member-%d", role.name, i+1))
			if options.nodeLog {
				memberRoot = filepath.Join(memberRoot, "group-0")
			}
			identityIndex := roleIndex*options.replicas + i
			members := make([]devPrepareMember, len(role.prepareMembers))
			copy(members, role.prepareMembers)
			if options.readAuthority {
				for memberIndex := range members {
					storeIndex := roleIndex*options.replicas + memberIndex
					members[memberIndex].StoreID = hex.EncodeToString(stores[storeIndex][:])
				}
			}
			prep := devPrepareManifest{Root: memberRoot, Distribution: role.distribution, Shard: role.shard, ClusterID: hex.EncodeToString(clusterID[:]), ClusterIncarnation: hex.EncodeToString(clusterIncarnation[:]), TopologyRecoveryEpoch: 1, AllocationGeneration: 1, ShardIncarnation: hex.EncodeToString(role.shardIncarnation[:]), GroupID: hex.EncodeToString(role.groupID[:]), MemberID: uint64(i + 1), StoreID: hex.EncodeToString(stores[identityIndex][:]), Table: role.table, CreateTable: role.createTable, Authority: authority, WAL: devPrepareWAL{KeyID: "dev-cluster-key", KeyMaterialPath: keySource, WrappedKey: "local-development-only", MaxFileBytes: raftstore.DefaultMaxFileBytes, MaxRecordBytes: raftstore.DefaultMaxRecordBytes, MaxRecords: raftstore.DefaultMaxRecords, MaxEntries: raftstore.DefaultMaxEntries, MaxLiveBytes: raftstore.DefaultMaxLiveBytes}, Apply: apply, Listeners: devPrepareListeners{Peer: ports[base], Native: ports[base+1], Snapshot: ports[base+2], Control: ports[base+3]}, TLS: devPrepareTLS{Certificate: credentials[identityIndex][0], Key: credentials[identityIndex][1], Roots: roots, IdentityOID: devClusterOID}, AuthorizationPolicy: policyPath, SplitControl: splitControl, DevelopmentOnly: options.replicas == devClusterRF1, ReadAuthority: newDevReadAuthority(options.readAuthority), Members: members}
			prepPath := filepath.Join(options.root, fmt.Sprintf("prepare-%s-member-%d.vibejson", role.name, i+1))
			raw, e := vibejson.Marshal(&prep)
			if e != nil {
				return m, e
			}
			if e = writeDevExclusive(prepPath, raw, 0o600); e != nil {
				return m, e
			}
			(*role.members)[i] = devClusterMember{Member: uint64(i + 1), Node: role.prepareMembers[i].NodeID, Store: hex.EncodeToString(stores[identityIndex][:]), Peer: ports[base], Native: ports[base+1], Snapshot: ports[base+2], Control: ports[base+3], ServeManifest: filepath.Join(memberRoot, "serve-rf3.vibejson")}
		}
	}
	if err = writeDevHotShardCapacity(m.HotShardCapacity, m.Members, m.LedgerMembers, m.DataMembers); err != nil {
		return m, err
	}
	if m.Nodes == devClusterRF1 {
		if err = writeDevReplicaControl(m.ReplicaControl, m); err != nil {
			return m, err
		}
	}
	raw, err := vibejson.Marshal(&m)
	if err != nil {
		return m, err
	}
	if err = writeDevExclusive(manifestPath, raw, 0o600); err != nil {
		return m, err
	}
	if err = syncDevDir(options.root); err != nil {
		return m, err
	}
	return m, completeDevCluster(options, m)
}

func devPrepareApplyProfile(
	shardKey string,
	requestLedgerIdentity replication.Digest,
) devPrepareApply {
	result := devPrepareApply{
		MaxSessions: 128, RetryWindow: 8, MaxCollections: 16,
		MaxDocuments: 4096, MaxBytes: 384 << 20, ShardKey: shardKey,
	}
	if requestLedgerIdentity == (replication.Digest{}) {
		return result
	}
	var boundary [sha256.Size]byte
	result.RequestLedgerCapacityBytes = devLedgerCapacityBytes
	result.RequestLedgerCleanupReserveBytes = devLedgerCleanupReserveBytes
	result.RequestLedgerRangeStart = hex.EncodeToString(boundary[:])
	result.RequestLedgerRangeEnd = hex.EncodeToString(boundary[:])
	result.RequestLedgerRangeIdentity = hex.EncodeToString(requestLedgerIdentity[:])
	return result
}

func devPrepareSplitControlProfile(members []devPrepareMember, gatewayNode string) devPrepareSplitControl {
	result := devPrepareSplitControl{
		MaxRecords: 4096, MaxFileBytes: 64 << 20,
		MaxChildOperations: 8, StageCheckpointBytes: 32 << 20,
		Grants: make([]devPrepareActionGrant, len(members)+1),
	}
	for index, member := range members {
		result.Grants[index] = devPrepareActionGrant{NodeID: member.NodeID, Actions: ^uint16(0)}
	}
	// The gateway owns reconciliation and submits the fenced actions. TLS and
	// the broad service policy do not replace this exact node/action grant.
	result.Grants[len(members)] = devPrepareActionGrant{NodeID: gatewayNode, Actions: ^uint16(0)}
	return result
}

func completeDevCluster(options devClusterOptions, manifest devClusterManifest) error {
	for _, role := range []struct {
		name    string
		members []devClusterMember
	}{{"catalog", manifest.Members}, {"ledger", manifest.LedgerMembers}, {"data", manifest.DataMembers}} {
		for index, member := range role.members {
			if _, err := os.Stat(member.ServeManifest); err == nil {
				if err := validateDevReadAuthorityFile(member.ServeManifest, manifest.ReadAuthority); err != nil {
					return err
				}
				preparePath := filepath.Join(options.root, fmt.Sprintf("prepare-%s-member-%d.vibejson", role.name, index+1))
				if _, prepareErr := os.Stat(preparePath); prepareErr == nil {
					if err := validateDevReadAuthorityFile(preparePath, manifest.ReadAuthority); err != nil {
						return err
					}
				} else if !errors.Is(prepareErr, os.ErrNotExist) {
					return prepareErr
				}
				continue
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			preparePath := filepath.Join(options.root, fmt.Sprintf("prepare-%s-member-%d.vibejson", role.name, index+1))
			if _, prepareErr := os.Stat(preparePath); prepareErr == nil {
				if err := validateDevReadAuthorityFile(preparePath, manifest.ReadAuthority); err != nil {
					return err
				}
			} else if !errors.Is(prepareErr, os.ErrNotExist) {
				return prepareErr
			}
			if manifest.NodeLog {
				if err := prepareDevNode(options.shardBinary, preparePath); err != nil {
					return err
				}
			} else if err := runDevCommand(options.shardBinary, "prepare-rf3", "-manifest", preparePath); err != nil {
				return err
			}
		}
	}
	if manifest.Nodes == devClusterRF1 {
		return nil
	}
	// RF3 source inventory includes real prepared storage identities. Keep
	// creation resumable after the durable cluster manifest, before serving.
	if _, err := os.Stat(manifest.ReplicaControl); errors.Is(err, os.ErrNotExist) {
		if err := writeDevReplicaControl(manifest.ReplicaControl, manifest); err != nil {
			return err
		}
		if err := syncDevDir(options.root); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	raw, err := readDevFile(filepath.Join(options.root, "prepare-catalog-member-1.vibejson"), 1<<20)
	if err != nil {
		return err
	}
	var prepare devPrepareManifest
	if err := vibejson.Unmarshal(raw, &prepare); err != nil {
		return errors.Join(errDevCluster, err)
	}
	clusterID, err := decodeDev16(prepare.ClusterID)
	if err != nil {
		return err
	}
	clusterIncarnation, err := decodeDev16(prepare.ClusterIncarnation)
	if err != nil {
		return err
	}
	catalogShardIncarnation, err := decodeDev16(prepare.ShardIncarnation)
	if err != nil {
		return err
	}
	catalogGroupID, err := decodeDev16(prepare.GroupID)
	if err != nil {
		return err
	}
	ledgerRaw, err := readDevFile(filepath.Join(options.root, "prepare-ledger-member-1.vibejson"), 1<<20)
	if err != nil {
		return err
	}
	var ledgerPrepare devPrepareManifest
	if err := vibejson.Unmarshal(ledgerRaw, &ledgerPrepare); err != nil {
		return errors.Join(errDevCluster, err)
	}
	ledgerShardIncarnation, err := decodeDev16(ledgerPrepare.ShardIncarnation)
	if err != nil {
		return err
	}
	ledgerGroupID, err := decodeDev16(ledgerPrepare.GroupID)
	if err != nil {
		return err
	}
	if ledgerPrepare.ClusterID != prepare.ClusterID || ledgerPrepare.ClusterIncarnation != prepare.ClusterIncarnation {
		return fmt.Errorf("%w: request-ledger group belongs to another cluster", errDevCluster)
	}
	dataRaw, err := readDevFile(filepath.Join(options.root, "prepare-data-member-1.vibejson"), 1<<20)
	if err != nil {
		return err
	}
	var dataPrepare devPrepareManifest
	if err := vibejson.Unmarshal(dataRaw, &dataPrepare); err != nil {
		return errors.Join(errDevCluster, err)
	}
	dataShardIncarnation, err := decodeDev16(dataPrepare.ShardIncarnation)
	if err != nil {
		return err
	}
	dataGroupID, err := decodeDev16(dataPrepare.GroupID)
	if err != nil {
		return err
	}
	if dataPrepare.ClusterID != prepare.ClusterID || dataPrepare.ClusterIncarnation != prepare.ClusterIncarnation {
		return fmt.Errorf("%w: data group belongs to another cluster", errDevCluster)
	}
	catalogGroup := raftmember.GroupKey{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: catalogShardIncarnation, GroupID: catalogGroupID}
	ledgerGroup := raftmember.GroupKey{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: ledgerShardIncarnation, GroupID: ledgerGroupID}
	dataGroup := raftmember.GroupKey{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: dataShardIncarnation, GroupID: dataGroupID}
	if _, err := os.Stat(manifest.CatalogPath); err == nil {
		return validateExistingDevCatalog(
			manifest.CatalogPath, manifest, catalogGroup, ledgerGroup, dataGroup,
		)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writeDevCatalog(
		manifest, clusterID, clusterIncarnation,
		catalogShardIncarnation, catalogGroupID,
		ledgerShardIncarnation, ledgerGroupID,
		dataShardIncarnation, dataGroupID,
	)
}

func validateExistingDevCatalog(
	path string,
	manifest devClusterManifest,
	catalogGroup, ledgerGroup, dataGroup raftmember.GroupKey,
) error {
	catalogRoute, err := inspectDevPreparedRoute(
		nil, "catalog", gateway.ReplicatedCatalogDistribution,
		gateway.ReplicatedCatalogShard, gateway.ReplicatedCatalogTable,
		gateway.ReplicatedCatalogPrimaryKey, catalogGroup, replication.Digest{},
		false, manifest.Members,
	)
	if err != nil {
		return err
	}
	ledgerHomeIdentity := deriveDevLedgerHomeIdentity(ledgerGroup)
	ledgerRoute, err := inspectDevPreparedRoute(
		nil, "ledger", devLedgerDistribution, devLedgerShard, devLedgerTable,
		devLedgerPrimaryKey, ledgerGroup, ledgerHomeIdentity, false,
		manifest.LedgerMembers,
	)
	if err != nil {
		return err
	}
	dataRoute, err := inspectDevPreparedRoute(
		nil, "data", devDataDistribution, devDataShard, devDataTable,
		devDataPrimaryKey, dataGroup, replication.Digest{}, true,
		manifest.DataMembers,
	)
	if err != nil {
		return err
	}
	snapshot, err := gateway.LoadSnapshot(path)
	if err != nil {
		return errors.Join(errDevCluster, err)
	}
	var replicaScratch [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	key, ok := orderedkey.AppendString(nil, []byte("dev-catalog-probe"), orderedkey.Ascending)
	if !ok {
		return errDevCluster
	}
	var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
	resolved, ok := snapshot.ResolveReplicatedTableKey(
		[]byte(devDataTable), key, scalarScratch[:0], replicaScratch[:0],
	)
	if !ok || resolved.Profile != dataRoute.table ||
		resolved.Profile.Table != devDataTable ||
		resolved.Profile.PrimaryKey != devDataPrimaryKey || resolved.Profile.Relation != 1 ||
		len(resolved.Route.Replicas) != gateway.ServingReplicaCount ||
		resolved.Route.Group != dataGroup ||
		resolved.Profile.SchemaGeneration != resolved.Route.Command.SchemaGeneration ||
		resolved.Profile.LogicalSchemaDigest != resolved.Route.LogicalSchemaDigest {
		return errDevCluster
	}
	if !matchesExistingDevRoute(
		snapshot, gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard,
		"catalog", manifest.Members, catalogGroup, catalogRoute, replicaScratch[:0],
	) || !matchesExistingDevRoute(
		snapshot, devLedgerDistribution, devLedgerShard,
		"ledger", manifest.LedgerMembers, ledgerGroup, ledgerRoute, replicaScratch[:0],
	) || !matchesExistingDevRoute(
		snapshot, devDataDistribution, devDataShard,
		"data", manifest.DataMembers, dataGroup, dataRoute, replicaScratch[:0],
	) {
		return errDevCluster
	}
	topology, ok := snapshot.DurableRequestLedgerTopology()
	if !ok || len(topology.Ranges) != 1 ||
		topology.Ranges[0].Start != ([sha256.Size]byte{}) ||
		topology.Ranges[0].End != ([sha256.Size]byte{}) ||
		topology.Ranges[0].Identity != deriveDevLedgerHomeIdentity(ledgerGroup) ||
		topology.Ranges[0].Route.Group != ledgerGroup {
		return errDevCluster
	}
	for _, controlTable := range []string{gateway.ReplicatedCatalogTable, devLedgerTable} {
		if _, exposed := snapshot.ResolveReplicatedTableKey(
			[]byte(controlTable), key, scalarScratch[:0], replicaScratch[:0],
		); exposed {
			return errDevCluster
		}
	}
	return nil
}

func matchesExistingDevRoute(
	snapshot *gateway.Snapshot,
	distributionName distribution.DistributionName,
	shard distribution.ShardID,
	role string,
	members []devClusterMember,
	group raftmember.GroupKey,
	prepared devPreparedRoute,
	scratch []gateway.ReplicatedEndpoint,
) bool {
	route, ok := snapshot.ResolveReplicatedRoute(distributionName, shard, scratch)
	if !ok || route.Distribution != distributionName || route.Shard != shard ||
		route.Group != group || route.AllocationGeneration != 1 ||
		len(route.Replicas) != len(members) ||
		route.Command.ReplicaSetVersion != 1 ||
		route.Command.ActivePolicyGeneration != 1 || route.Command.ProtectionEpoch != 1 ||
		route.Command.OwnershipEpoch != 1 ||
		route.Command.SchemaGeneration != prepared.schemaGeneration ||
		route.Command.RelationManifestDigest != prepared.digest ||
		route.LogicalSchemaDigest != prepared.table.LogicalSchemaDigest ||
		route.Command.RoutingVersion != 1 || route.Command.RouteGeneration != 1 {
		return false
	}
	wantRange, wantLineage, wantForwarding := deriveDevLogicalRangeAuthority(
		group, distributionName, shard, route.Command.RelationManifestDigest,
	)
	if route.RangeIdentity != wantRange || route.LineageDigest != wantLineage ||
		route.ForwardingRuleDigest != wantForwarding {
		return false
	}
	for index, member := range members {
		node, nodeErr := decodeDev16(member.Node)
		store, storeErr := decodeDev16(member.Store)
		prefix := role + "-member-" + strconv.Itoa(index+1)
		replica := route.Replicas[index]
		if nodeErr != nil || storeErr != nil || replica.Member != member.Member ||
			replica.Node != rafttransport.NodeID(node) || replica.StoreID != store ||
			replica.NodeIncarnation != 1 || replica.Endpoint != prefix ||
			replica.DataAddress != member.Peer || replica.NativeEndpoint != prefix+"-native" ||
			replica.Address != member.Native || replica.ControlEndpoint != prefix+"-control" ||
			replica.ControlAddress != member.Control {
			return false
		}
	}
	return true
}

func inspectDevPreparedRoute(
	endpoints map[distribution.EndpointID]string,
	role string,
	distributionName distribution.DistributionName,
	shard distribution.ShardID,
	table string,
	primaryKey string,
	group raftmember.GroupKey,
	requestLedgerIdentity replication.Digest,
	publishTable bool,
	members []devClusterMember,
) (devPreparedRoute, error) {
	result := devPreparedRoute{
		leaders:  make([]distribution.EndpointID, len(members)),
		replicas: make([]gateway.ReplicatedReplicaDescriptor, len(members)),
	}
	for index, member := range members {
		prefix := fmt.Sprintf("%s-member-%d", role, index+1)
		result.leaders[index] = distribution.EndpointID(prefix)
		native := distribution.EndpointID(prefix + "-native")
		control := distribution.EndpointID(prefix + "-control")
		if endpoints != nil {
			endpoints[result.leaders[index]] = member.Peer
			endpoints[native] = member.Native
			endpoints[control] = member.Control
		}
		node, nodeErr := decodeDev16(member.Node)
		store, storeErr := decodeDev16(member.Store)
		if nodeErr != nil || storeErr != nil {
			return devPreparedRoute{}, errDevCluster
		}
		result.replicas[index] = gateway.ReplicatedReplicaDescriptor{
			Member: member.Member, Node: rafttransport.NodeID(node), StoreID: store,
			NodeIncarnation: 1, Endpoint: result.leaders[index],
			NativeEndpoint: native, ControlEndpoint: control,
		}
		imageRaw, readErr := readDevFile(
			filepath.Join(devMemberRoot(member), "member.vdb"),
			devSQLCatalogMaxBytes,
		)
		if readErr != nil {
			return devPreparedRoute{}, errors.Join(errDevCluster, readErr)
		}
		image, imageErr := sqldriver.ValidateReplicatedSchemaCatalogImage(imageRaw)
		if imageErr != nil {
			return devPreparedRoute{}, errors.Join(errDevCluster, imageErr)
		}
		profile, machineDigest, profileErr := readDevReplicatedTableProfile(
			member, distributionName, shard, table, primaryKey, group,
			requestLedgerIdentity, image,
		)
		if profileErr != nil {
			return devPreparedRoute{}, profileErr
		}
		if index == 0 {
			result.digest = machineDigest
			result.applyDigest = image.ApplyProfileDigest
			result.schemaGeneration = image.SchemaGeneration
			if publishTable {
				result.table = profile
			}
		} else if result.digest != machineDigest ||
			result.applyDigest != image.ApplyProfileDigest ||
			result.schemaGeneration != image.SchemaGeneration ||
			publishTable && result.table != profile {
			return devPreparedRoute{}, errDevCluster
		}
	}
	return result, nil
}

func writeDevCatalog(
	m devClusterManifest,
	clusterID, clusterIncarnation,
	catalogShardIncarnation, catalogGroupID,
	ledgerShardIncarnation, ledgerGroupID,
	dataShardIncarnation, dataGroupID [16]byte,
) error {
	endpoints := make(map[distribution.EndpointID]string,
		(len(m.Members)+len(m.LedgerMembers)+len(m.DataMembers))*3)
	catalogGroup := raftmember.GroupKey{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: catalogShardIncarnation, GroupID: catalogGroupID}
	ledgerGroup := raftmember.GroupKey{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: ledgerShardIncarnation, GroupID: ledgerGroupID}
	dataGroup := raftmember.GroupKey{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: dataShardIncarnation, GroupID: dataGroupID}
	ledgerHomeIdentity := deriveDevLedgerHomeIdentity(ledgerGroup)
	catalogRoute, err := inspectDevPreparedRoute(endpoints,
		"catalog", gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard,
		gateway.ReplicatedCatalogTable, gateway.ReplicatedCatalogPrimaryKey,
		catalogGroup, replication.Digest{}, false, m.Members,
	)
	if err != nil {
		return err
	}
	ledgerRoute, err := inspectDevPreparedRoute(endpoints,
		"ledger", devLedgerDistribution, devLedgerShard, devLedgerTable,
		devLedgerPrimaryKey, ledgerGroup, ledgerHomeIdentity, false, m.LedgerMembers,
	)
	if err != nil {
		return err
	}
	dataRoute, err := inspectDevPreparedRoute(endpoints,
		"data", devDataDistribution, devDataShard, devDataTable, devDataPrimaryKey,
		dataGroup, replication.Digest{}, true, m.DataMembers,
	)
	if err != nil {
		return err
	}
	snapshot, err := newDevCatalogSnapshot(
		endpoints, catalogGroup, ledgerGroup, dataGroup,
		catalogRoute, ledgerRoute, dataRoute,
	)
	if err != nil {
		return err
	}
	return gateway.SaveSnapshot(m.CatalogPath, snapshot)
}

func newDevCatalogSnapshot(
	endpoints map[distribution.EndpointID]string,
	catalogGroup, ledgerGroup, dataGroup raftmember.GroupKey,
	catalogRoute, ledgerRoute, dataRoute devPreparedRoute,
) (*gateway.Snapshot, error) {
	catalogManifest, err := distribution.NewManifest(gateway.ReplicatedCatalogDistribution, 1, []distribution.Shard{{ID: gateway.ReplicatedCatalogShard, AllocationGeneration: 1, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: catalogRoute.leaders, Epoch: 1}})
	if err != nil {
		return nil, err
	}
	ledgerManifest, err := distribution.NewManifest(devLedgerDistribution, 1, []distribution.Shard{{ID: devLedgerShard, AllocationGeneration: 1, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: ledgerRoute.leaders, Epoch: 1}})
	if err != nil {
		return nil, err
	}
	dataManifest, err := distribution.NewManifest(devDataDistribution, 1, []distribution.Shard{{ID: devDataShard, AllocationGeneration: 1, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: dataRoute.leaders, Epoch: 1}})
	if err != nil {
		return nil, err
	}
	catalogRange, catalogLineage, catalogForwarding := deriveDevLogicalRangeAuthority(catalogGroup, gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard, catalogRoute.digest)
	ledgerRange, ledgerLineage, ledgerForwarding := deriveDevLogicalRangeAuthority(ledgerGroup, devLedgerDistribution, devLedgerShard, ledgerRoute.digest)
	dataRange, dataLineage, dataForwarding := deriveDevLogicalRangeAuthority(dataGroup, devDataDistribution, devDataShard, dataRoute.digest)
	ledgerHomeIdentity := deriveDevLedgerHomeIdentity(ledgerGroup)
	command := func(route devPreparedRoute) raftservice.CommandFence {
		return raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: route.schemaGeneration, RelationManifestDigest: route.digest, RoutingVersion: 1, RouteGeneration: 1}
	}
	return gateway.NewSnapshotWithReplicatedTableMetadata(
		distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{
				{Name: gateway.ReplicatedCatalogDistribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion},
				{Name: devLedgerDistribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion},
				{Name: devDataDistribution, Arity: 1, MapperVersion: distribution.NativeMapperVersion},
			},
			Placements: []distribution.TablePlacement{
				{Table: gateway.ReplicatedCatalogTable, Distribution: gateway.ReplicatedCatalogDistribution, Columns: []string{gateway.ReplicatedCatalogPrimaryKey}},
				{Table: devLedgerTable, Distribution: devLedgerDistribution, Columns: []string{devLedgerPrimaryKey}},
				{Table: devDataTable, Distribution: devDataDistribution, Columns: []string{devDataPrimaryKey}},
			},
			Manifests: []*distribution.Manifest{catalogManifest, ledgerManifest, dataManifest},
		},
		endpoints, 1, nil, nil,
		[]gateway.ReplicatedShardDescriptor{
			{Distribution: gateway.ReplicatedCatalogDistribution, Shard: gateway.ReplicatedCatalogShard, Group: catalogGroup, AllocationGeneration: 1, Command: command(catalogRoute), RangeIdentity: catalogRange, LineageDigest: catalogLineage, ForwardingRuleDigest: catalogForwarding, Replicas: catalogRoute.replicas},
			{Distribution: devLedgerDistribution, Shard: devLedgerShard, Group: ledgerGroup, AllocationGeneration: 1, Command: command(ledgerRoute), RangeIdentity: ledgerRange, LineageDigest: ledgerLineage, ForwardingRuleDigest: ledgerForwarding, RequestLedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{Identity: ledgerHomeIdentity}}, Replicas: ledgerRoute.replicas},
			{Distribution: devDataDistribution, Shard: devDataShard, Group: dataGroup, AllocationGeneration: 1, Command: command(dataRoute), LogicalSchemaDigest: dataRoute.table.LogicalSchemaDigest, RangeIdentity: dataRange, LineageDigest: dataLineage, ForwardingRuleDigest: dataForwarding, Replicas: dataRoute.replicas},
		},
		[]gateway.ReplicatedTableProfile{dataRoute.table},
	)
}

func readDevReplicatedTableProfile(
	member devClusterMember,
	distributionName distribution.DistributionName,
	shard distribution.ShardID,
	table, primaryKey string,
	group raftmember.GroupKey,
	requestLedgerIdentity replication.Digest,
	image sqldriver.ReplicatedSchemaCatalogImage,
) (gateway.ReplicatedTableProfile, [32]byte, error) {
	root := devMemberRoot(member)
	identityRaw, err := readDevFile(filepath.Join(root, "sql-identity.vibejson"), 1<<20)
	if err != nil {
		return gateway.ReplicatedTableProfile{}, [32]byte{}, errors.Join(errDevCluster, err)
	}
	var identity sqldriver.ReplicatedShardStoreIdentity
	if err := identity.UnmarshalJSON(identityRaw); err != nil {
		return gateway.ReplicatedTableProfile{}, [32]byte{}, errors.Join(errDevCluster, err)
	}
	applyRaw, err := readDevFile(filepath.Join(root, "apply-identity.vibejson"), 1<<20)
	if err != nil {
		return gateway.ReplicatedTableProfile{}, [32]byte{}, errors.Join(errDevCluster, err)
	}
	var apply sqldriver.ReplicatedApplyIdentity
	if err := apply.UnmarshalJSON(applyRaw); err != nil {
		return gateway.ReplicatedTableProfile{}, [32]byte{}, errors.Join(errDevCluster, err)
	}
	storeID, err := decodeDev16(member.Store)
	if err != nil {
		return gateway.ReplicatedTableProfile{}, [32]byte{}, err
	}
	expectedBinding := sqldriver.ReplicatedShardStoreBinding{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		Distribution:          string(distributionName), Shard: string(shard),
		AllocationGeneration: 1, ShardIncarnation: group.ShardIncarnation,
		GroupID: group.GroupID, MemberID: member.Member, StoreID: storeID,
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1,
			SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1,
		},
	}
	expectedPlacement := sqldriver.ReplicatedPlacementProfile{
		Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: primaryKey,
		TupleVersion:  distribution.CurrentTupleVersion,
		MapperVersion: distribution.NativeMapperVersion,
		Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
	}
	ledgerMatches := apply.RequestLedgerCapacityBytes == 0 &&
		apply.RequestLedgerCleanupReserveBytes == 0 &&
		apply.RequestLedgerRangeStart == ([sha256.Size]byte{}) &&
		apply.RequestLedgerRangeEnd == ([sha256.Size]byte{}) &&
		apply.RequestLedgerRangeIdentity == ([sha256.Size]byte{})
	if requestLedgerIdentity != (replication.Digest{}) {
		ledgerMatches = apply.RequestLedgerCapacityBytes == devLedgerCapacityBytes &&
			apply.RequestLedgerCleanupReserveBytes == devLedgerCleanupReserveBytes &&
			apply.RequestLedgerRangeStart == ([sha256.Size]byte{}) &&
			apply.RequestLedgerRangeEnd == ([sha256.Size]byte{}) &&
			apply.RequestLedgerRangeIdentity == [sha256.Size]byte(requestLedgerIdentity)
	}
	if identity.Binding != expectedBinding || identity.UserTable != table ||
		identity.UserPrimaryKey != primaryKey || identity.RelationCount != 1 ||
		len(identity.Relations) != 1 || identity.RelationSchemaGeneration != image.SchemaGeneration ||
		identity.RelationManifestDigest != image.LocalRelationManifestDigest ||
		apply.ValidationDigest != image.ApplyProfileDigest || apply.MaxSessions != 128 ||
		apply.RetryWindow != 8 || apply.TxnLimits.MaxCollections != 16 ||
		apply.TxnLimits.MaxDocuments != 4096 || apply.TxnLimits.MaxBytes != 384<<20 ||
		apply.Placement != expectedPlacement ||
		!ledgerMatches {
		return gateway.ReplicatedTableProfile{}, [32]byte{}, errDevCluster
	}
	relation := identity.Relations[0]
	if relation.Relation != 1 || relation.Kind != sqldriver.ReplicatedShardRelationJSON ||
		relation.Table != table || relation.Limits != identity.UserLimits ||
		identity.UserLimits.MaxKeyBytes <= 0 ||
		identity.UserLimits.MaxKeyBytes > replication.MaxMutationKeyBytes ||
		identity.UserLimits.MaxDocumentBytes <= 0 ||
		identity.UserLimits.MaxDocumentBytes > replication.MaxMutationValueBytes {
		return gateway.ReplicatedTableProfile{}, [32]byte{}, errDevCluster
	}
	database, err := sqldriver.OpenReplicatedShardStoreWithApply(
		filepath.Join(root, "member.vdb"), identity, apply,
	)
	if err != nil {
		return gateway.ReplicatedTableProfile{}, [32]byte{}, errors.Join(errDevCluster, err)
	}
	manifestDigest, manifestErr := database.ReplicatedRelationManifestForBinding(identity, apply.Placement, identity.Binding)
	if err := errors.Join(manifestErr, database.Close()); err != nil {
		return gateway.ReplicatedTableProfile{}, [32]byte{}, errors.Join(errDevCluster, err)
	}
	logicalDigest, err := devLogicalSchemaForPreparedImage(identity, manifestDigest, image)
	if err != nil {
		return gateway.ReplicatedTableProfile{}, [32]byte{}, errors.Join(errDevCluster, err)
	}
	return gateway.ReplicatedTableProfile{
		Table: table, Relation: replication.RelationID(relation.Relation),
		PrimaryKey: primaryKey, SchemaGeneration: image.SchemaGeneration,
		LogicalSchemaDigest: replication.Digest(logicalDigest),
		MaxKeyBytes:         uint16(identity.UserLimits.MaxKeyBytes),
		MaxDocumentBytes:    uint32(identity.UserLimits.MaxDocumentBytes),
	}, manifestDigest, nil
}

// devMemberRoot points at the group-local SQL/WAL directory. Legacy role
// manifests kept those artifacts beside serve-rf3.vibejson; grouped physical
// nodes record the independent root explicitly while sharing one node log.
func devMemberRoot(member devClusterMember) string {
	if member.GroupRoot != "" {
		return member.GroupRoot
	}
	return filepath.Dir(member.ServeManifest)
}

func devLogicalSchemaForPreparedImage(identity sqldriver.ReplicatedShardStoreIdentity,
	machine [sha256.Size]byte, image sqldriver.ReplicatedSchemaCatalogImage,
) ([sha256.Size]byte, error) {
	if machine == ([sha256.Size]byte{}) || machine != image.RelationManifestDigest ||
		identity.RelationManifestDigest != image.LocalRelationManifestDigest || identity.RelationSchemaGeneration != image.SchemaGeneration {
		return [sha256.Size]byte{}, fmt.Errorf("%w: prepared catalog schema differs from opened store", errDevCluster)
	}
	// The image certifies the placement-bound machine manifest. Public table
	// profiles instead use the portable logical schema derived from the exact
	// authenticated SQL identity, never that machine digest.
	return sqldriver.ReplicatedRelationManifestDigest(identity)
}

// deriveDevLogicalRangeAuthority certifies the three independent logical
// authorities from the already-authenticated Raft group, manifest locator, and
// relation grammar. The domains are permanent and disjoint; no time, process,
// endpoint, member-local store, or caller-provided randomness participates, so
// every replica and a crash-resumed bootstrap derives byte-identical values.
func deriveDevLogicalRangeAuthority(
	group raftmember.GroupKey,
	distributionName distribution.DistributionName,
	shard distribution.ShardID,
	relationManifestDigest [32]byte,
) (replication.Digest, replication.Digest, replication.Digest) {
	return deriveDevAuthorityDigest("vibedb/dev/range-identity/format-0\x00", group, distributionName, shard, relationManifestDigest),
		deriveDevAuthorityDigest("vibedb/dev/range-lineage/format-0\x00", group, distributionName, shard, relationManifestDigest),
		deriveDevAuthorityDigest("vibedb/dev/forwarding-rule/format-0\x00", group, distributionName, shard, relationManifestDigest)
}

func deriveDevLedgerHomeIdentity(
	group raftmember.GroupKey,
) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/dev/request-ledger-home/format-0\x00"))
	appendDevGroupAuthority(hash, group)
	var width [4]byte
	binary.BigEndian.PutUint32(width[:], uint32(len(devLedgerDistribution)))
	_, _ = hash.Write(width[:])
	_, _ = hash.Write([]byte(devLedgerDistribution))
	binary.BigEndian.PutUint32(width[:], uint32(len(devLedgerShard)))
	_, _ = hash.Write(width[:])
	_, _ = hash.Write([]byte(devLedgerShard))
	var fullRange [sha256.Size * 2]byte
	_, _ = hash.Write(fullRange[:])
	var result replication.Digest
	copy(result[:], hash.Sum(nil))
	return result
}

func deriveDevAuthorityDigest(
	domain string,
	group raftmember.GroupKey,
	distributionName distribution.DistributionName,
	shard distribution.ShardID,
	relationManifestDigest [32]byte,
) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	appendDevGroupAuthority(hash, group)
	var width [4]byte
	binary.BigEndian.PutUint32(width[:], uint32(len(distributionName)))
	_, _ = hash.Write(width[:])
	_, _ = hash.Write([]byte(distributionName))
	binary.BigEndian.PutUint32(width[:], uint32(len(shard)))
	_, _ = hash.Write(width[:])
	_, _ = hash.Write([]byte(shard))
	// The bootstrap manifest is generation one and covers [0,+infinity).
	var generation [16]byte
	binary.BigEndian.PutUint64(generation[0:8], 1)
	binary.BigEndian.PutUint64(generation[8:16], 1)
	_, _ = hash.Write(generation[:])
	_, _ = hash.Write(relationManifestDigest[:])
	var result replication.Digest
	copy(result[:], hash.Sum(nil))
	return result
}

func appendDevGroupAuthority(hash interface{ Write([]byte) (int, error) }, group raftmember.GroupKey) {
	_, _ = hash.Write(group.ClusterID[:])
	_, _ = hash.Write(group.ClusterIncarnation[:])
	var epoch [8]byte
	binary.BigEndian.PutUint64(epoch[:], group.TopologyRecoveryEpoch)
	_, _ = hash.Write(epoch[:])
	_, _ = hash.Write(group.ShardIncarnation[:])
	_, _ = hash.Write(group.GroupID[:])
}

type devChild struct {
	command     *exec.Cmd
	ready       chan error
	done        chan struct{}
	diagnostics *devDiagnostics
	waitMu      sync.Mutex
	waitErr     error
}

type devChildExit struct {
	name string
	err  error
}

func serveDevCluster(ctx context.Context, m devClusterManifest, shardBinary, gatewayBinary string, diagnostics io.Writer, pgListen ...string) error {
	if m.PhysicalNodes != 0 {
		return serveDevPhysicalCluster(ctx, m, shardBinary, diagnostics)
	}
	memberCount := len(m.Members) + len(m.LedgerMembers) + len(m.DataMembers)
	children := make([]*devChild, 0, memberCount+1)
	var dataChildren []*devChild
	exits := make(chan devChildExit, memberCount+1)
	defer func() {
		stopDevChildren(children)
		if diagnostics != nil {
			for index, child := range children {
				fmt.Fprintf(diagnostics, "development child %d (%s), last %d bytes:\n%s\n",
					index+1, filepath.Base(child.command.Path), devChildDiagnosticBytes, child.diagnostics.String())
			}
		}
	}()
	for _, role := range []struct {
		name    string
		members []devClusterMember
	}{{"catalog", m.Members}, {"request-ledger", m.LedgerMembers}, {"data", m.DataMembers}} {
		for index, member := range role.members {
			marker := "vibedb-shard RF3 ready"
			if m.Nodes == devClusterRF1 {
				marker = "vibedb-shard RF1-development-only-no-HA ready"
			}
			serveManifest := member.ServeManifest
			if role.name == "data" && len(m.dataServeManifests) != 0 {
				serveManifest = m.dataServeManifests[index]
			}
			childArgs := []string{"serve-rf3", "-manifest", serveManifest}
			if role.name == "data" && m.Nodes == devClusterRF3 && len(m.dataServeManifests) != 0 {
				childArgs = append(childArgs, "-reload-prepared-groups")
			}
			child, err := startDevChild(shardBinary, childArgs, marker)
			if err != nil {
				return err
			}
			children = append(children, child)
			if role.name == "data" {
				dataChildren = append(dataChildren, child)
			}
			watchDevChildExit(exits, fmt.Sprintf("%s shard member %d", role.name, member.Member), child)
		}
	}
	for _, child := range children {
		if err := waitDevReadyOrExit(ctx, child, exits); err != nil {
			return err
		}
	}
	if m.Nodes == devClusterRF1 {
		fmt.Fprintf(os.Stdout, "VibeDB development RF1 ready (no HA): %s\n", m.DataMembers[0].Native)
		select {
		case <-ctx.Done():
			return nil
		case exit := <-exits:
			return errors.Join(fmt.Errorf("%s exited", exit.name), exit.err)
		}
	}
	args := []string{"serve", "-catalog", m.CatalogPath, "-catalog-route-seed", m.CatalogPath + ".route-seed", "-catalog-bootstrap-if-missing", "-catalog-relation", "1", "-catalog-session-journal", filepath.Join(filepath.Dir(m.CatalogPath), "gateway-session"), "-catalog-client-id", m.GatewayNode, "-catalog-retry-home", m.GatewayNode[:16], "-durable-ack-key", m.DurableAckKey, "-listen", m.ClientEndpoint, "-tls-certificate", m.GatewayCertificate, "-tls-key", m.GatewayKey, "-tls-roots", m.Roots, "-tls-identity-oid", devClusterOID, "-authorization-policy", m.AuthorizationPolicy, "-hot-shard-capacity", m.HotShardCapacity, "-replica-control-manifest", m.ReplicaControl}
	for _, members := range [][]devClusterMember{m.Members, m.LedgerMembers, m.DataMembers} {
		for _, member := range members {
			args = append(args, "-shard-peer", member.Native+"="+member.Node)
		}
	}
	if len(pgListen) != 0 && pgListen[0] != "" {
		args = append(args, "-pg-dev-listen", pgListen[0])
		if len(m.dataServeManifests) == devClusterRF3 {
			socket, stopDDL, err := startDevDDL(ctx, m, shardBinary, dataChildren)
			if err != nil {
				return err
			}
			defer stopDDL()
			args = append(args, "-pg-dev-ddl-socket", socket)
		}
	}
	for _, catalog := range m.additionalCatalogs {
		args = append(args, "-register-table-catalog", catalog)
	}
	gatewayChild, err := startDevChild(gatewayBinary, args, "vibedb-gateway serving")
	if err != nil {
		return err
	}
	children = append(children, gatewayChild)
	watchDevChildExit(exits, "gateway", gatewayChild)
	if err = waitDevReadyOrExit(ctx, gatewayChild, exits); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "VibeDB development cluster ready: %s\n", m.ClientEndpoint)
	select {
	case <-ctx.Done():
		return nil
	case exit := <-exits:
		return errors.Join(fmt.Errorf("%s exited", exit.name), exit.err)
	}
}

func serveDevPhysicalCluster(ctx context.Context, manifest devClusterManifest, shardBinary string, diagnostics io.Writer) error {
	children := make([]*devChild, 0, len(manifest.NodeManifests))
	exits := make(chan devChildExit, len(manifest.NodeManifests)+1)
	defer func() {
		stopDevChildren(children)
		if diagnostics != nil {
			for index, child := range children {
				fmt.Fprintf(diagnostics, "development physical node %d (%s), last %d bytes:\n%s\n",
					index+1, filepath.Base(child.command.Path), devChildDiagnosticBytes, child.diagnostics.String())
			}
		}
	}()
	for index, node := range manifest.NodeManifests {
		child, err := startDevChild(shardBinary, []string{"serve-node", "-manifest", node.ServeManifest, "-reload-prepared-groups"}, "vibedb-shard RF3 ready")
		if err != nil {
			return err
		}
		children = append(children, child)
		watchDevChildExit(exits, fmt.Sprintf("physical node %d", index+1), child)
	}
	ddlSocket, err := devPhysicalDDLSocket(manifest)
	if err != nil {
		return err
	}
	if ddlSocket != "" {
		_, stopDDL, err := startDevDDLAt(ctx, manifest, shardBinary, children, ddlSocket)
		if err != nil {
			return err
		}
		defer stopDDL()
	}
	for _, child := range children {
		if err := waitDevReadyOrExit(ctx, child, exits); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stdout, "VibeDB development RF3 physical cluster ready: %s (%d nodes)\n", manifest.ClientEndpoint, manifest.PhysicalNodes)
	select {
	case <-ctx.Done():
		return nil
	case exit := <-exits:
		return errors.Join(fmt.Errorf("%s exited", exit.name), exit.err)
	}
}

// devPhysicalDDLSocket extracts the canonical gateway settings from every
// generated node manifest. Only node zero may own a PostgreSQL listener and
// its one supervisor socket; accepting a second endpoint would make a table
// DDL request race two local inventories.
func devPhysicalDDLSocket(manifest devClusterManifest) (string, error) {
	if len(manifest.NodeManifests) == 0 {
		return "", errDevCluster
	}
	var socket string
	for index, node := range manifest.NodeManifests {
		raw, err := readDevFile(node.ServeManifest, 4<<20)
		if err != nil {
			return "", err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return "", errors.Join(errDevCluster, err)
		}
		gatewayRaw, ok := fields["gateway"]
		if !ok || len(gatewayRaw) == 0 {
			return "", errDevCluster
		}
		var gatewayConfig devGatewayConfig
		if err := vibejson.Unmarshal(gatewayRaw, &gatewayConfig); err != nil {
			return "", errors.Join(errDevCluster, err)
		}
		if gatewayConfig.PGListen == "" {
			if gatewayConfig.PGDDLSocket != "" {
				return "", fmt.Errorf("%w: PostgreSQL DDL socket without listener on physical node %d", errDevCluster, index+1)
			}
			continue
		}
		want := filepath.Join(filepath.Dir(manifest.NodeManifests[0].ServeManifest), "pg-ddl.sock")
		if gatewayConfig.PGDDLSocket != want || index > 0 && (!gatewayConfig.ControlParticipantOnly ||
			gatewayConfig.DDLOwnerNode != manifest.GatewayNode || gatewayConfig.DDLOwnerAddress != manifest.ClientEndpoint) {
			return "", fmt.Errorf("%w: PostgreSQL DDL ownership must resolve to physical node 1", errDevCluster)
		}
		socket = gatewayConfig.PGDDLSocket
	}
	return socket, nil
}

func watchDevChildExit(exits chan<- devChildExit, name string, child *devChild) {
	go func() {
		<-child.done
		err := child.waitError()
		if tail := child.diagnostics.String(); tail != "" {
			err = errors.Join(err, fmt.Errorf("last child output (at most %d bytes):\n%s", devChildDiagnosticBytes, tail))
		}
		exits <- devChildExit{name: name, err: err}
	}()
}

func waitDevReadyOrExit(ctx context.Context, child *devChild, exits <-chan devChildExit) error {
	timer := time.NewTimer(devReadyTimeout)
	defer timer.Stop()
	select {
	case err := <-child.ready:
		return err
	case exit := <-exits:
		return errors.Join(fmt.Errorf("%s exited before cluster readiness", exit.name), exit.err)
	case <-timer.C:
		return errors.New("development cluster readiness timeout")
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func startDevChild(binary string, args []string, marker string) (*devChild, error) {
	if len(marker) == 0 || len(marker) > devChildReadyMarkerBytes {
		return nil, errors.New("invalid development child readiness marker")
	}
	diagnostics := &devDiagnostics{maximum: devChildDiagnosticBytes}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	command := exec.Command(binary, args...)
	// Own both streams so exec.Wait cannot hang on an inherited stdout pipe
	// held by a descendant after the supervised process has exited.
	command.Stdout = writer
	command.Stderr = writer
	child := &devChild{command: command, ready: make(chan error, 1), done: make(chan struct{}), diagnostics: diagnostics}
	if err = command.Start(); err != nil {
		reader.Close()
		writer.Close()
		return nil, err
	}
	writer.Close()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		defer reader.Close()
		captureDevChildOutput(reader, diagnostics, marker, child.ready)
	}()
	go func() {
		err := command.Wait()
		// Preserve final fatal output before publishing the exit, but bound
		// draining if a descendant inherited either stream.
		timer := time.AfterFunc(devChildLogDrainTimeout, func() { _ = reader.Close() })
		<-drained
		timer.Stop()
		child.waitMu.Lock()
		child.waitErr = err
		child.waitMu.Unlock()
		close(child.done)
	}()
	return child, nil
}

func captureDevChildOutput(reader io.Reader, diagnostics *devDiagnostics, marker string, ready chan<- error) {
	var buffer [4096 + devChildReadyMarkerBytes]byte
	markerBytes := []byte(marker)
	overlap, found := 0, false
	for {
		n, err := reader.Read(buffer[overlap : overlap+4096])
		if n != 0 {
			_, _ = diagnostics.Write(buffer[overlap : overlap+n])
			total := overlap + n
			if !found && bytes.Contains(buffer[:total], markerBytes) {
				found = true
				ready <- nil
			}
			if !found {
				overlap = min(len(marker)-1, total)
				copy(buffer[:overlap], buffer[total-overlap:total])
			} else {
				overlap = 0
			}
		}
		if err != nil {
			if !found {
				if errors.Is(err, io.EOF) {
					err = nil
				}
				ready <- errors.Join(errors.New(diagnostics.String()), err)
			}
			return
		}
	}
}

func (child *devChild) waitError() error {
	child.waitMu.Lock()
	defer child.waitMu.Unlock()
	return child.waitErr
}
func waitDevReady(ctx context.Context, child *devChild) error {
	timer := time.NewTimer(devReadyTimeout)
	defer timer.Stop()
	select {
	case err := <-child.ready:
		return err
	case <-timer.C:
		return errors.New("development cluster readiness timeout")
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func stopDevChildren(children []*devChild) {
	for i := len(children) - 1; i >= 0; i-- {
		if children[i] != nil && children[i].command.Process != nil {
			_ = children[i].command.Process.Signal(syscall.SIGTERM)
		}
	}
	for i := len(children) - 1; i >= 0; i-- {
		select {
		case <-children[i].done:
		case <-time.After(10 * time.Second):
			_ = children[i].command.Process.Kill()
			<-children[i].done
		}
	}
}

type devDiagnostics struct {
	mu      sync.Mutex
	data    []byte
	maximum int
	write   int
	count   int
}

func (d *devDiagnostics) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	inputBytes := len(p)
	if d.maximum <= 0 || len(p) == 0 {
		return inputBytes, nil
	}
	if d.data == nil {
		d.data = make([]byte, d.maximum)
	}
	if len(p) >= d.maximum {
		copy(d.data, p[len(p)-d.maximum:])
		d.write, d.count = 0, d.maximum
		return inputBytes, nil
	}
	first := copy(d.data[d.write:], p)
	copy(d.data, p[first:])
	d.write = (d.write + len(p)) % d.maximum
	d.count = min(d.maximum, d.count+len(p))
	return inputBytes, nil
}
func (d *devDiagnostics) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.count == 0 {
		return ""
	}
	start := (d.write - d.count + d.maximum) % d.maximum
	var tail strings.Builder
	tail.Grow(d.count)
	first := min(d.count, d.maximum-start)
	tail.Write(d.data[start : start+first])
	tail.Write(d.data[:d.count-first])
	return tail.String()
}

func reserveDevPorts(count int, pgAddresses ...string) ([]string, error) {
	return reserveDevPortsUsing(count, pgAddresses, net.Listen)
}

func reserveDevPortsUsing(count int, pgAddresses []string, listen func(string, string) (net.Listener, error)) (addresses []string, err error) {
	if count < 0 {
		return nil, errDevCluster
	}
	seen := make(map[string]bool, len(pgAddresses))
	for _, address := range pgAddresses {
		if address == "" {
			continue
		}
		if !validDevPGAddress(address) || seen[address] {
			return nil, fmt.Errorf("%w: PostgreSQL endpoints must be distinct literal loopback addresses", errDevCluster)
		}
		seen[address] = true
	}
	listeners := make([]net.Listener, 0, count+len(seen))
	defer func() {
		for index := len(listeners) - 1; index >= 0; index-- {
			err = errors.Join(err, listeners[index].Close())
		}
		if err != nil {
			addresses = nil
		}
	}()
	// Hold the requested PG bindings while the kernel assigns every internal
	// ephemeral port. Binding the actual addresses also handles address-family
	// aliases according to the same socket rules used by the child processes.
	for _, address := range pgAddresses {
		if address == "" {
			continue
		}
		listener, listenErr := listen("tcp", address)
		if listenErr != nil {
			return nil, fmt.Errorf("reserve PostgreSQL listener %q: %w", address, listenErr)
		}
		listeners = append(listeners, listener)
	}
	addresses = make([]string, count)
	for index := range addresses {
		listener, listenErr := listen("tcp", "127.0.0.1:0")
		if listenErr != nil {
			return nil, listenErr
		}
		listeners = append(listeners, listener)
		addresses[index] = listener.Addr().String()
	}
	return addresses, nil
}
func writeDevCredentials(root string, domain rafttransport.TrustDomain, nodes []rafttransport.NodeID) ([][2]string, string, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return nil, "", err
	}
	now := time.Now().UTC()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "VibeDB local development CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(30 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	caDER, err := x509.CreateCertificate(cryptorand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, "", err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, "", err
	}
	roots := filepath.Join(root, "roots.pem")
	if err = writeDevExclusive(roots, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		return nil, "", err
	}
	result := make([][2]string, len(nodes))
	for i, node := range nodes {
		key, e := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
		if e != nil {
			return nil, "", e
		}
		extension, e := rafttransport.PeerIdentityExtension(devIdentityOID, rafttransport.PeerIdentity{TrustDomain: domain, Node: node})
		if e != nil {
			return nil, "", e
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: "VibeDB local node"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(30 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, ExtraExtensions: []pkix.Extension{extension}}
		der, e := x509.CreateCertificate(cryptorand.Reader, leaf, caCert, &key.PublicKey, caKey)
		if e != nil {
			return nil, "", e
		}
		certPath := filepath.Join(root, fmt.Sprintf("node-%d-cert.pem", i+1))
		keyPath := filepath.Join(root, fmt.Sprintf("node-%d-key.pem", i+1))
		certPEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})...)
		keyDER, e := x509.MarshalECPrivateKey(key)
		if e != nil {
			return nil, "", e
		}
		if e = writeDevExclusive(certPath, certPEM, 0o600); e != nil {
			return nil, "", e
		}
		if e = writeDevExclusive(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); e != nil {
			return nil, "", e
		}
		result[i] = [2]string{certPath, keyPath}
	}
	return result, roots, nil
}

var devIdentityOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}

func writeDevPolicy(path string, nodes []rafttransport.NodeID, client rafttransport.NodeID) error {
	if len(nodes) == 0 || client == (rafttransport.NodeID{}) {
		return errDevCluster
	}
	caps := []string{"data_read", "data_write", "schema", "delegate", "membership", "topology", "transaction_recovery", "request_ledger", "execution_pin"}
	policy := devPolicy{Generation: 1, Principals: make([]devPrincipal, len(nodes)+1)}
	for i, n := range nodes {
		if n == (rafttransport.NodeID{}) || n == client {
			return errDevCluster
		}
		policy.Principals[i] = devPrincipal{Node: hex.EncodeToString(n[:]), Capabilities: caps}
	}
	policy.Principals[len(nodes)] = devPrincipal{Node: hex.EncodeToString(client[:]), Capabilities: []string{"data_read", "data_write"}}
	sort.Slice(policy.Principals, func(i, j int) bool { return policy.Principals[i].Node < policy.Principals[j].Node })
	for i := 1; i < len(policy.Principals); i++ {
		if policy.Principals[i-1].Node == policy.Principals[i].Node {
			return errDevCluster
		}
	}
	raw, err := vibejson.Marshal(&policy)
	if err != nil {
		return err
	}
	return writeDevExclusive(path, raw, 0o600)
}

func writeDevHotShardCapacity(
	path string, catalog, ledger, data []devClusterMember,
) error {
	if path == "" || len(catalog) == 0 || len(catalog) != len(ledger) ||
		len(catalog) != len(data) {
		return errDevCluster
	}
	var capacity autosplit.CapacityVector
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 64
	}
	config := hotshard.StaticCapacityConfig{Format: hotshard.StaticCapacityFormat,
		RecorderLanes: 4, WindowCapacity: capacity, NodeCapacity: capacity,
		MigrationCapacity: 1 << 30, ShardMigrationBytes: 384 << 20, MaxReceives: 2,
		Nodes: make([]hotshard.StaticCapacityNode, 0, len(catalog)+len(ledger)+len(data))}
	prefixes := [...]string{"catalog-member-", "data-member-", "ledger-member-"}
	for role, members := range [][]devClusterMember{catalog, data, ledger} {
		prefix := prefixes[role]
		for index := range members {
			config.Nodes = append(config.Nodes, hotshard.StaticCapacityNode{
				Endpoint:      distribution.EndpointID(prefix + strconv.Itoa(index+1)),
				FailureDomain: uint32(index + 1),
			})
		}
	}
	raw, err := hotshard.AppendStaticCapacityConfig(nil, config)
	if err != nil {
		return errors.Join(errDevCluster, err)
	}
	return writeDevExclusive(path, raw, 0o600)
}

func writeDevReplicaControl(path string, cluster devClusterManifest) error {
	manifest, err := devReplicaControlConfig(cluster)
	if err != nil {
		return err
	}
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		return err
	}
	return writeDevExclusive(path, raw, 0o600)
}

func devReplicaControlConfig(cluster devClusterManifest) (devReplicaControlManifest, error) {
	members := [][]devClusterMember{cluster.Members, cluster.LedgerMembers, cluster.DataMembers}
	if len(cluster.Members) == 0 || len(cluster.Members) != len(cluster.LedgerMembers) ||
		len(cluster.Members) != len(cluster.DataMembers) ||
		!validDevLoopbackAddress(cluster.GatewayControl) {
		return devReplicaControlManifest{}, errDevCluster
	}
	local := devReplicaControlEndpoint{Node: cluster.GatewayNode, Incarnation: 1,
		ControlAddress: cluster.GatewayControl}
	manifest := devReplicaControlManifest{Generation: 1, LocalGateway: local,
		TLS: devReplicaControlTLS{Certificate: cluster.GatewayCertificate, Key: cluster.GatewayKey,
			Roots: cluster.Roots, IdentityOID: devClusterOID,
			AuthorizationPolicy: cluster.AuthorizationPolicy},
		Bounds: devReplicaControlBounds{MaxConnections: 64, MaxHandshakes: 16,
			MaxConcurrentDrains: 8, ControllerInterval: 100, ReadTimeout: 2_000, WriteTimeout: 5_000},
		GatewayEndpoints: []devReplicaControlEndpoint{local}}
	if cluster.Nodes == devClusterRF3 {
		source, err := devReplicaSplitSourceForCluster(cluster)
		if err != nil {
			return devReplicaControlManifest{}, err
		}
		manifest.SplitSources = []devReplicaSplitSource{source}
	}
	// A physical node can host one member in each independent group. The
	// gateway control grammar is node-wide, so collapse those repeated roster
	// entries while proving that every group agrees on its shared listeners.
	shards := make(map[string]devReplicaControlShard, len(cluster.Members))
	for _, roleMembers := range members {
		for _, member := range roleMembers {
			candidate := devReplicaControlShard{Node: member.Node, ControlAddress: member.Control,
				SplitSnapshotAddress: member.Snapshot}
			if prior, found := shards[member.Node]; found {
				if prior != candidate {
					return devReplicaControlManifest{}, errDevCluster
				}
				continue
			}
			shards[member.Node] = candidate
		}
	}
	for _, shard := range shards {
		manifest.ShardEndpoints = append(manifest.ShardEndpoints, shard)
	}
	sort.Slice(manifest.ShardEndpoints, func(left, right int) bool {
		return manifest.ShardEndpoints[left].Node < manifest.ShardEndpoints[right].Node
	})
	return manifest, nil
}

// Build from immutable preparation authority and the actual prepared SQL
// identity before processes start. No storage names or log IDs are synthesized.
func devReplicaSplitSourceForCluster(cluster devClusterManifest) (devReplicaSplitSource, error) {
	if cluster.Nodes != devClusterRF3 || len(cluster.DataMembers) != devClusterRF3 {
		return devReplicaSplitSource{}, errDevCluster
	}
	preparePath := filepath.Join(filepath.Dir(cluster.CatalogPath), "prepare-data-member-1.vibejson")
	if cluster.PhysicalNodes != 0 && len(cluster.DataMembers) != 0 {
		member := cluster.DataMembers[0]
		if member.GroupRoot == "" {
			return devReplicaSplitSource{}, errDevCluster
		}
		preparePath = filepath.Dir(member.GroupRoot) + "." + filepath.Base(member.GroupRoot) + ".prepare.vibejson"
	}
	raw, err := readDevFile(preparePath, 1<<20)
	if err != nil {
		return devReplicaSplitSource{}, err
	}
	var prepare devPrepareManifest
	if err = vibejson.Unmarshal(raw, &prepare); err != nil {
		return devReplicaSplitSource{}, err
	}
	member := cluster.DataMembers[0]
	if prepare.MemberID != member.Member || prepare.StoreID != member.Store ||
		prepare.Root != devMemberRoot(member) ||
		prepare.Distribution != string(devDataDistribution) || prepare.Shard != string(devDataShard) ||
		prepare.Table != devDataTable {
		return devReplicaSplitSource{}, errDevCluster
	}
	var entry devReplicaSplitSource
	for _, item := range []struct {
		encoded string
		target  *[16]byte
	}{
		{prepare.ClusterID, &entry.ClusterID}, {prepare.ClusterIncarnation, &entry.ClusterIncarnation},
		{prepare.ShardIncarnation, &entry.ShardIncarnation}, {prepare.GroupID, &entry.GroupID},
	} {
		*item.target, err = decodeDev16(item.encoded)
		if err != nil {
			return devReplicaSplitSource{}, err
		}
	}
	storeID, err := decodeDev16(prepare.StoreID)
	if err != nil {
		return devReplicaSplitSource{}, err
	}
	entry.TopologyRecoveryEpoch, entry.SchemaGeneration, entry.Table = prepare.TopologyRecoveryEpoch, prepare.Authority.SchemaGeneration, prepare.Table
	binding := sqldriver.ReplicatedShardStoreBinding{
		ClusterID: entry.ClusterID, ClusterIncarnation: entry.ClusterIncarnation, TopologyRecoveryEpoch: entry.TopologyRecoveryEpoch,
		Distribution: prepare.Distribution, Shard: prepare.Shard, AllocationGeneration: prepare.AllocationGeneration,
		ShardIncarnation: entry.ShardIncarnation, GroupID: entry.GroupID, MemberID: prepare.MemberID, StoreID: storeID,
		Authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: prepare.Authority.ActivePolicyGeneration,
			ProtectionEpoch: prepare.Authority.ProtectionEpoch, OwnershipEpoch: prepare.Authority.OwnershipEpoch,
			SchemaGeneration: prepare.Authority.SchemaGeneration, RoutingVersion: prepare.Authority.RoutingVersion,
			RouteGeneration: prepare.Authority.RouteGeneration}}
	identityRaw, err := readDevFile(filepath.Join(prepare.Root, "sql-identity.vibejson"), 1<<20)
	if err != nil {
		return devReplicaSplitSource{}, err
	}
	if err := entry.SQL.UnmarshalJSON(identityRaw); err != nil {
		return devReplicaSplitSource{}, errors.Join(errDevCluster, err)
	}
	if entry.SQL.Binding != binding || entry.SQL.UserTable != prepare.Table ||
		entry.SQL.UserPrimaryKey != devDataPrimaryKey || entry.SQL.RelationCount != 1 {
		return devReplicaSplitSource{}, errDevCluster
	}
	placement := sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat,
		ShardKey: prepare.Apply.ShardKey, TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
		Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}
	digest, limits, err := sqldriver.InitialReplicatedRelationManifest(binding, placement,
		sqldriver.InitialReplicatedRelationSchema{Table: prepare.Table, PrimaryKey: devDataPrimaryKey})
	if err != nil {
		return devReplicaSplitSource{}, err
	}
	actualDigest, err := sqldriver.ReplicatedSchemaManifest(entry.SQL, placement, nil)
	if err != nil || actualDigest != digest || entry.SQL.UserLimits != limits {
		return devReplicaSplitSource{}, errors.Join(errDevCluster, err)
	}
	entry.RelationManifestDigest = digest
	entry.Placement = devReplicaSplitPlacement{
		Format: placement.Format, ShardKey: placement.ShardKey,
		TupleVersion: uint16(placement.TupleVersion), MapperVersion: uint16(placement.MapperVersion),
		RangeStart: [8]byte(placement.Range.Start), RangeEnd: [8]byte(placement.Range.End.Point),
		RangeEndMax: placement.Range.End.Max,
	}
	entry.Template = devReplicaSplitTemplate{MaxSessions: prepare.Apply.MaxSessions, RetryWindow: prepare.Apply.RetryWindow,
		TxnLimits: durable.TxnLimits{MaxCollections: prepare.Apply.MaxCollections, MaxDocuments: prepare.Apply.MaxDocuments, MaxBytes: prepare.Apply.MaxBytes},
		Format:    placement.Format, ShardKey: placement.ShardKey, TupleVersion: uint16(placement.TupleVersion), MapperVersion: uint16(placement.MapperVersion),
		MaxBatchDocuments: limits.MaxBatchDocuments, MaxBatchBytes: limits.MaxBatchBytes}
	for _, member := range cluster.DataMembers {
		entry.Replicas = append(entry.Replicas, devReplicaSplitSourceReplica{Node: member.Node,
			ChildRoot: filepath.Join(devMemberRoot(member), "split-children")})
	}
	sort.Slice(entry.Replicas, func(i, j int) bool { return entry.Replicas[i].Node < entry.Replicas[j].Node })
	return entry, nil
}

func validDevManifest(m devClusterManifest, root string) bool {
	if m.Format != devClusterFormat || (m.Nodes != devClusterRF1 && m.Nodes != devClusterRF3) ||
		(m.NodeLog && m.Nodes != devClusterRF3) || len(m.Members) != int(m.Nodes) ||
		len(m.LedgerMembers) != int(m.Nodes) || len(m.DataMembers) != int(m.Nodes) ||
		!validDevLoopbackAddress(m.ClientEndpoint) || !validDevLoopbackAddress(m.GatewayControl) || m.ClientEndpoint == m.GatewayControl {
		return false
	}
	if _, err := decodeDev16(m.GatewayNode); err != nil {
		return false
	}
	if _, err := decodeDev16(m.ClientNode); err != nil || m.ClientNode == m.GatewayNode {
		return false
	}
	if m.ReadAuthority != nil && (m.Nodes != devClusterRF3 || !validDevReadAuthority(*m.ReadAuthority)) {
		return false
	}
	paths := []string{m.CatalogPath, m.GatewayCertificate, m.GatewayKey, m.ClientCertificate, m.ClientKey, m.Roots, m.AuthorizationPolicy, m.HotShardCapacity, m.ReplicaControl, m.DurableAckKey}
	physical := m.PhysicalNodes != 0 || len(m.NodeManifests) != 0
	if physical {
		if m.Nodes != devClusterRF3 || m.Replicas != devClusterRF3 || !m.NodeLog ||
			(m.PhysicalNodes != devClusterPhysicalNodes3 && m.PhysicalNodes != devClusterPhysicalNodes6) ||
			len(m.NodeManifests) != int(m.PhysicalNodes) {
			return false
		}
	}
	// The gateway-control socket is a different service from a physical
	// node's shard-control socket. Address ownership therefore allows repeated
	// role entries only when they resolve to the same physical node.
	addressOwners := map[string]string{m.ClientEndpoint: "client", m.GatewayControl: "gateway-control"}
	nodes := map[string]struct{}{m.GatewayNode: {}, m.ClientNode: {}}
	stores := make(map[string]struct{}, len(m.Members)+len(m.LedgerMembers)+len(m.DataMembers))
	physicalByNode := make(map[string]int, len(m.NodeManifests))
	physicalListeners := make(map[string]devPrepareListeners, len(m.NodeManifests))
	seenPhysicalNodes := make(map[string]struct{}, len(m.NodeManifests))
	gatewayByNode := make(map[string]struct{}, len(m.NodeManifests))
	if physical {
		for index, node := range m.NodeManifests {
			if _, err := decodeDev16(node.Node); err != nil || node.Node == m.ClientNode {
				return false
			}
			if _, duplicate := physicalByNode[node.Node]; duplicate {
				return false
			}
			physicalByNode[node.Node] = index
			if _, err := decodeDev16(node.GatewayNode); err != nil || node.GatewayNode == node.Node || node.GatewayNode == m.ClientNode {
				return false
			}
			if _, duplicate := gatewayByNode[node.GatewayNode]; duplicate {
				return false
			}
			gatewayByNode[node.GatewayNode] = struct{}{}
			if node.GatewayNode == m.GatewayNode && index != 0 {
				return false
			}
			for _, path := range []string{node.Certificate, node.Key, node.ServeManifest, node.CatalogSessionJournal, node.DirectIssuerJournal, node.FallbackJournal, node.ExecutionPinJournal} {
				paths = append(paths, path)
			}
			// The cluster-level gateway credential names the designated
			// controller's frontend credential; the other frontend credentials
			// remain node-local. Node manifests are shared by the role groups
			// hosted on one physical process and are already included above.
			if index == 0 && (node.GatewayCertificate != m.GatewayCertificate || node.GatewayKey != m.GatewayKey || node.FrontendListen != m.ClientEndpoint) {
				return false
			}
			if index != 0 {
				paths = append(paths, node.GatewayCertificate)
				paths = append(paths, node.GatewayKey)
			}
			if !validDevLoopbackAddress(node.GatewayControl) || !validDevLoopbackAddress(node.FrontendListen) {
				return false
			}
			if node.ServeManifest != filepath.Join(root, fmt.Sprintf("node-%d", index+1), "serve-rf3.vibejson") ||
				len(node.Groups) == 0 || len(node.Groups) > devPhysicalMaxGroups {
				return false
			}
			if node.CatalogSessionJournal != filepath.Join(filepath.Dir(node.ServeManifest), "gateway", "catalog-session") ||
				node.DirectIssuerJournal != node.CatalogSessionJournal+".pg-writes.direct" ||
				node.FallbackJournal != node.CatalogSessionJournal+".pg-writes" ||
				node.ExecutionPinJournal != node.CatalogSessionJournal+".durable-pins" {
				return false
			}
			if node.GatewayNode == m.GatewayNode && node.GatewayControl != m.GatewayControl {
				return false
			}
			for _, address := range []string{node.GatewayControl, node.FrontendListen} {
				if owner, exists := addressOwners[address]; exists && owner != "client" && owner != "gateway-control" {
					return false
				}
				if address == m.ClientEndpoint && index != 0 || address == m.GatewayControl && node.GatewayNode != m.GatewayNode {
					return false
				}
				addressOwners[address] = "physical:" + node.Node
			}
			for ordinal, groupRoot := range node.Groups {
				if groupRoot != filepath.Join(filepath.Dir(node.ServeManifest), fmt.Sprintf("group-%d", ordinal)) {
					return false
				}
				paths = append(paths, groupRoot)
			}
		}
		if len(physicalByNode) != int(m.PhysicalNodes) || physicalByNode[m.NodeManifests[0].Node] != 0 || m.NodeManifests[0].GatewayNode != m.GatewayNode {
			return false
		}
	}
	for roleIndex, members := range [][]devClusterMember{m.Members, m.LedgerMembers, m.DataMembers} {
		roleNodes := make(map[string]struct{}, len(members))
		for index, member := range members {
			if !physical {
				paths = append(paths, member.ServeManifest)
			}
			if member.Member != uint64(index+1) {
				return false
			}
			if _, err := decodeDev16(member.Node); err != nil {
				return false
			}
			if _, err := decodeDev16(member.Store); err != nil {
				return false
			}
			if _, duplicate := roleNodes[member.Node]; duplicate {
				return false
			}
			roleNodes[member.Node] = struct{}{}
			if physical {
				physicalIndex, found := physicalByNode[member.Node]
				if !found || member.PhysicalNode != member.Node || member.ServeManifest != m.NodeManifests[physicalIndex].ServeManifest {
					return false
				}
				seenPhysicalNodes[member.Node] = struct{}{}
				placement := devPhysicalPlacement(int(m.PhysicalNodes), roleIndex)
				if index >= len(placement) || placement[index] != physicalIndex {
					return false
				}
				if member.GroupRoot == "" || filepath.Dir(member.GroupRoot) != filepath.Dir(member.ServeManifest) {
					return false
				}
				if !containsString(m.NodeManifests[physicalIndex].Groups, member.GroupRoot) {
					return false
				}
				listeners := devPrepareListeners{Peer: member.Peer, Native: member.Native, Snapshot: member.Snapshot, Control: member.Control}
				if prior, exists := physicalListeners[member.Node]; exists && prior != listeners {
					return false
				}
				physicalListeners[member.Node] = listeners
				localAddresses := map[string]bool{m.NodeManifests[physicalIndex].FrontendListen: true, m.NodeManifests[physicalIndex].GatewayControl: true}
				for _, address := range []string{member.Peer, member.Native, member.Snapshot, member.Control} {
					if localAddresses[address] {
						return false
					}
					localAddresses[address] = true
				}
			} else if member.PhysicalNode != "" || member.GroupRoot != "" {
				return false
			}
			if _, duplicate := stores[member.Store]; duplicate {
				return false
			}
			stores[member.Store] = struct{}{}
			for _, address := range [...]string{member.Peer, member.Native, member.Snapshot, member.Control} {
				if !validDevLoopbackAddress(address) {
					return false
				}
				if owner, duplicate := addressOwners[address]; duplicate {
					if !physical || owner != "physical:"+member.Node {
						return false
					}
				}
				addressOwners[address] = "physical:" + member.Node
			}
		}
	}
	if physical {
		if len(seenPhysicalNodes) != len(m.NodeManifests) {
			return false
		}
		for index, node := range m.NodeManifests {
			if _, storageIdentity := physicalByNode[node.GatewayNode]; storageIdentity || node.GatewayNode == m.ClientNode || node.GatewayNode == m.GatewayNode && index != 0 {
				return false
			}
		}
	} else {
		for _, members := range [][]devClusterMember{m.Members, m.LedgerMembers, m.DataMembers} {
			for _, member := range members {
				if _, duplicate := nodes[member.Node]; duplicate {
					return false
				}
				nodes[member.Node] = struct{}{}
			}
		}
	}
	seenPaths := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		if !filepath.IsAbs(p) || p != filepath.Clean(p) || !strings.HasPrefix(p, root+string(filepath.Separator)) {
			return false
		}
		if _, duplicate := seenPaths[p]; duplicate {
			return false
		}
		seenPaths[p] = struct{}{}
	}
	return true
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func validDevLoopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		return false
	}
	value, err := strconv.Atoi(port)
	return err == nil && value > 0 && value <= 65535 && strconv.Itoa(value) == port
}

func devIDString(raw []byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, len(raw)*2)
	for index, value := range raw {
		result[index*2], result[index*2+1] = digits[value>>4], digits[value&15]
	}
	return string(result)
}

func decodeDev16(value string) ([16]byte, error) {
	var out [16]byte
	if len(value) != 32 {
		return out, errDevCluster
	}
	_, err := hex.Decode(out[:], []byte(value))
	if err == nil && (out == ([16]byte{}) || hex.EncodeToString(out[:]) != value) {
		err = errDevCluster
	}
	return out, err
}
func runDevCommand(binary string, args ...string) error {
	command := exec.Command(binary, args...)
	diagnostic := &devDiagnostics{maximum: devChildDiagnosticBytes}
	command.Stdout = diagnostic
	command.Stderr = diagnostic
	if err := command.Run(); err != nil {
		return errors.Join(err, errors.New(diagnostic.String()))
	}
	return nil
}
func writeDevExclusive(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, werr := file.Write(data)
	serr := file.Sync()
	cerr := file.Close()
	return errors.Join(werr, serr, cerr)
}
func readDevFile(path string, max int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > int64(max) {
		return nil, errors.Join(errDevCluster, err)
	}
	raw := make([]byte, info.Size())
	_, err = io.ReadFull(file, raw)
	return raw, err
}
func syncDevDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
