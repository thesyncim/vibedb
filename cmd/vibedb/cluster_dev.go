package main

import (
	"bufio"
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
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

const (
	devClusterFormat                                           = 1
	devClusterRF3                                              = 3
	devClusterRF1                                              = 1
	devClusterOID                                              = "1.3.6.1.4.1.32473.1.1"
	devReadyTimeout                                            = 30 * time.Second
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
	Format              uint16             `json:"format"`
	Nodes               uint8              `json:"nodes"`
	ClientEndpoint      string             `json:"client_endpoint"`
	CatalogPath         string             `json:"catalog_path"`
	GatewayCertificate  string             `json:"gateway_certificate"`
	GatewayKey          string             `json:"gateway_key"`
	Roots               string             `json:"roots"`
	AuthorizationPolicy string             `json:"authorization_policy"`
	HotShardCapacity    string             `json:"hot_shard_capacity"`
	DurableAckKey       string             `json:"durable_ack_key"`
	GatewayNode         string             `json:"gateway_node"`
	Members             []devClusterMember `json:"members"`
	LedgerMembers       []devClusterMember `json:"ledger_members"`
	DataMembers         []devClusterMember `json:"data_members"`
}

type devClusterMember struct {
	Member        uint64 `json:"member"`
	Node          string `json:"node"`
	Store         string `json:"store"`
	Peer          string `json:"peer"`
	Native        string `json:"native"`
	Snapshot      string `json:"snapshot"`
	Control       string `json:"control"`
	ServeManifest string `json:"serve_manifest"`
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
	MemberID    uint64 `json:"member_id"`
	NodeID      string `json:"node_id"`
	PeerAddress string `json:"peer_address"`
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
}

func runClusterDev(args []string) int {
	fs := flag.NewFlagSet("cluster dev", flag.ContinueOnError)
	root := fs.String("root", "", "absolute durable cluster directory")
	replicas := fs.Int("replicas", devClusterRF3, "Raft replicas: 1 for dev-only/no-HA or 3 for RF3")
	nodes := fs.Int("nodes", 0, "deprecated alias for --replicas")
	shardBinary := fs.String("shard-binary", "", "vibedb-shard executable; defaults beside vibedb or PATH")
	gatewayBinary := fs.String("gateway-binary", "", "vibedb-gateway executable; defaults beside vibedb or PATH")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *root == "" {
		usage()
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
	if *replicas != devClusterRF1 && *replicas != devClusterRF3 {
		usage()
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
	if *replicas == devClusterRF3 {
		gw, err = resolveDevBinary(*gatewayBinary, "vibedb-gateway")
		if err != nil {
			fmt.Fprintf(os.Stderr, "cluster dev: %v\n", err)
			return 1
		}
	}
	manifest, err := ensureDevCluster(devClusterOptions{root: abs, replicas: *replicas, shardBinary: shard, gatewayBinary: gw})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster dev: %v\n", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := serveDevCluster(ctx, manifest, shard, gw); err != nil {
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
	if options.replicas != devClusterRF1 && options.replicas != devClusterRF3 {
		return devClusterManifest{}, errDevCluster
	}
	manifestPath := filepath.Join(options.root, "cluster.vibejson")
	if raw, err := readDevFile(manifestPath, 1<<20); err == nil {
		var m devClusterManifest
		if vibejson.Unmarshal(raw, &m) != nil {
			return m, errDevCluster
		}
		canonical, e := vibejson.Marshal(&m)
		if e != nil || !bytes.Equal(raw, canonical) || !validDevManifest(m, options.root) ||
			m.Nodes != uint8(options.replicas) {
			return m, errDevCluster
		}
		return m, completeDevCluster(options, m)
	} else if !errors.Is(err, os.ErrNotExist) {
		return devClusterManifest{}, err
	}
	if entries, err := os.ReadDir(options.root); err == nil && len(entries) != 0 {
		return devClusterManifest{}, errDevCluster
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return devClusterManifest{}, err
	}
	if err := os.MkdirAll(options.root, 0o700); err != nil {
		return devClusterManifest{}, err
	}
	return initializeDevCluster(options, manifestPath)
}

func initializeDevCluster(options devClusterOptions, manifestPath string) (devClusterManifest, error) {
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
	nodes := make([]rafttransport.NodeID, options.replicas+1)
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
	ports, err := reserveDevPorts(1 + options.replicas*12)
	if err != nil {
		return devClusterManifest{}, err
	}
	credentials, roots, err := writeDevCredentials(options.root, rafttransport.TrustDomain{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation}, nodes)
	if err != nil {
		return devClusterManifest{}, err
	}
	policyPath := filepath.Join(options.root, "authorization-policy.vibejson")
	if err := writeDevPolicy(policyPath, nodes); err != nil {
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
	gatewayIndex := options.replicas
	m := devClusterManifest{Format: devClusterFormat, Nodes: uint8(options.replicas), ClientEndpoint: ports[0], CatalogPath: filepath.Join(options.root, "catalog.vibejson"), GatewayCertificate: credentials[gatewayIndex][0], GatewayKey: credentials[gatewayIndex][1], Roots: roots, AuthorizationPolicy: policyPath, HotShardCapacity: filepath.Join(options.root, "hot-shard-capacity.vibejson"), DurableAckKey: durableAckKeyPath, GatewayNode: hex.EncodeToString(nodes[gatewayIndex][:]), Members: make([]devClusterMember, options.replicas), LedgerMembers: make([]devClusterMember, options.replicas), DataMembers: make([]devClusterMember, options.replicas)}
	catalogPrepareMembers := make([]devPrepareMember, options.replicas)
	ledgerPrepareMembers := make([]devPrepareMember, options.replicas)
	dataPrepareMembers := make([]devPrepareMember, options.replicas)
	for i := 0; i < options.replicas; i++ {
		catalogBase := 1 + i*4
		ledgerBase := 1 + options.replicas*4 + i*4
		dataBase := 1 + options.replicas*8 + i*4
		catalogPrepareMembers[i] = devPrepareMember{MemberID: uint64(i + 1), NodeID: hex.EncodeToString(nodes[i][:]), PeerAddress: ports[catalogBase]}
		ledgerPrepareMembers[i] = devPrepareMember{MemberID: uint64(i + 1), NodeID: hex.EncodeToString(nodes[i][:]), PeerAddress: ports[ledgerBase]}
		dataPrepareMembers[i] = devPrepareMember{MemberID: uint64(i + 1), NodeID: hex.EncodeToString(nodes[i][:]), PeerAddress: ports[dataBase]}
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
		splitControl := devPrepareSplitControlProfile(role.prepareMembers)
		for i := 0; i < options.replicas; i++ {
			base := 1 + roleIndex*options.replicas*4 + i*4
			memberRoot := filepath.Join(options.root, fmt.Sprintf("%s-member-%d", role.name, i+1))
			prep := devPrepareManifest{Root: memberRoot, Distribution: role.distribution, Shard: role.shard, ClusterID: hex.EncodeToString(clusterID[:]), ClusterIncarnation: hex.EncodeToString(clusterIncarnation[:]), TopologyRecoveryEpoch: 1, AllocationGeneration: 1, ShardIncarnation: hex.EncodeToString(role.shardIncarnation[:]), GroupID: hex.EncodeToString(role.groupID[:]), MemberID: uint64(i + 1), StoreID: hex.EncodeToString(stores[roleIndex*options.replicas+i][:]), Table: role.table, CreateTable: role.createTable, Authority: authority, WAL: devPrepareWAL{KeyID: "dev-cluster-key", KeyMaterialPath: keySource, WrappedKey: "local-development-only", MaxFileBytes: raftstore.DefaultMaxFileBytes, MaxRecordBytes: raftstore.DefaultMaxRecordBytes, MaxRecords: raftstore.DefaultMaxRecords, MaxEntries: raftstore.DefaultMaxEntries, MaxLiveBytes: raftstore.DefaultMaxLiveBytes}, Apply: apply, Listeners: devPrepareListeners{Peer: ports[base], Native: ports[base+1], Snapshot: ports[base+2], Control: ports[base+3]}, TLS: devPrepareTLS{Certificate: credentials[i][0], Key: credentials[i][1], Roots: roots, IdentityOID: devClusterOID}, AuthorizationPolicy: policyPath, SplitControl: splitControl, DevelopmentOnly: options.replicas == devClusterRF1, Members: role.prepareMembers}
			prepPath := filepath.Join(options.root, fmt.Sprintf("prepare-%s-member-%d.vibejson", role.name, i+1))
			raw, e := vibejson.Marshal(&prep)
			if e != nil {
				return m, e
			}
			if e = writeDevExclusive(prepPath, raw, 0o600); e != nil {
				return m, e
			}
			(*role.members)[i] = devClusterMember{Member: uint64(i + 1), Node: role.prepareMembers[i].NodeID, Store: hex.EncodeToString(stores[roleIndex*options.replicas+i][:]), Peer: ports[base], Native: ports[base+1], Snapshot: ports[base+2], Control: ports[base+3], ServeManifest: filepath.Join(memberRoot, "serve-rf3.vibejson")}
		}
	}
	if err = writeDevHotShardCapacity(m.HotShardCapacity, m.Members, m.LedgerMembers, m.DataMembers); err != nil {
		return m, err
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

func devPrepareSplitControlProfile(members []devPrepareMember) devPrepareSplitControl {
	result := devPrepareSplitControl{
		MaxRecords: 4096, MaxFileBytes: 64 << 20,
		MaxChildOperations: 8, StageCheckpointBytes: 32 << 20,
		Grants: make([]devPrepareActionGrant, len(members)),
	}
	for index, member := range members {
		result.Grants[index] = devPrepareActionGrant{NodeID: member.NodeID, Actions: ^uint16(0)}
	}
	return result
}

func completeDevCluster(options devClusterOptions, manifest devClusterManifest) error {
	for _, role := range []struct {
		name    string
		members []devClusterMember
	}{{"catalog", manifest.Members}, {"ledger", manifest.LedgerMembers}, {"data", manifest.DataMembers}} {
		for index, member := range role.members {
			if _, err := os.Stat(member.ServeManifest); err == nil {
				continue
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			preparePath := filepath.Join(options.root, fmt.Sprintf("prepare-%s-member-%d.vibejson", role.name, index+1))
			if err := runDevCommand(options.shardBinary, "prepare-rf3", "-manifest", preparePath); err != nil {
				return err
			}
		}
	}
	if manifest.Nodes == devClusterRF1 {
		return nil
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
	if !ok || resolved.Profile.Table != devDataTable ||
		resolved.Profile.PrimaryKey != devDataPrimaryKey || resolved.Profile.Relation != 1 ||
		len(resolved.Route.Replicas) != gateway.ServingReplicaCount ||
		resolved.Route.Group != dataGroup ||
		resolved.Profile.SchemaGeneration != resolved.Route.Command.SchemaGeneration ||
		resolved.Profile.RelationManifestDigest !=
			replication.Digest(resolved.Route.Command.RelationManifestDigest) {
		return errDevCluster
	}
	if !matchesExistingDevRoute(
		snapshot, gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard,
		"catalog", manifest.Members, catalogGroup, replicaScratch[:0],
	) || !matchesExistingDevRoute(
		snapshot, devLedgerDistribution, devLedgerShard,
		"ledger", manifest.LedgerMembers, ledgerGroup, replicaScratch[:0],
	) || !matchesExistingDevRoute(
		snapshot, devDataDistribution, devDataShard,
		"data", manifest.DataMembers, dataGroup, replicaScratch[:0],
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
	scratch []gateway.ReplicatedEndpoint,
) bool {
	route, ok := snapshot.ResolveReplicatedRoute(distributionName, shard, scratch)
	if !ok || route.Distribution != distributionName || route.Shard != shard ||
		route.Group != group || route.AllocationGeneration != 1 ||
		len(route.Replicas) != len(members) ||
		route.Command.ReplicaSetVersion != 1 ||
		route.Command.ActivePolicyGeneration != 1 || route.Command.ProtectionEpoch != 1 ||
		route.Command.OwnershipEpoch != 1 || route.Command.SchemaGeneration != 1 ||
		route.Command.RelationManifestDigest == ([sha256.Size]byte{}) ||
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
			filepath.Join(filepath.Dir(member.ServeManifest), "member.vdb"),
			devSQLCatalogMaxBytes,
		)
		if readErr != nil {
			return devPreparedRoute{}, errors.Join(errDevCluster, readErr)
		}
		image, imageErr := sqldriver.ValidateReplicatedSchemaCatalogImage(imageRaw)
		if imageErr != nil {
			return devPreparedRoute{}, errors.Join(errDevCluster, imageErr)
		}
		profile, profileErr := readDevReplicatedTableProfile(
			member, distributionName, shard, table, primaryKey, group,
			requestLedgerIdentity, image,
		)
		if profileErr != nil {
			return devPreparedRoute{}, profileErr
		}
		if index == 0 {
			result.digest = image.RelationManifestDigest
			result.applyDigest = image.ApplyProfileDigest
			result.schemaGeneration = image.SchemaGeneration
			if publishTable {
				result.table = profile
			}
		} else if result.digest != image.RelationManifestDigest ||
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
			{Distribution: devDataDistribution, Shard: devDataShard, Group: dataGroup, AllocationGeneration: 1, Command: command(dataRoute), RangeIdentity: dataRange, LineageDigest: dataLineage, ForwardingRuleDigest: dataForwarding, Replicas: dataRoute.replicas},
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
) (gateway.ReplicatedTableProfile, error) {
	root := filepath.Dir(member.ServeManifest)
	identityRaw, err := readDevFile(filepath.Join(root, "sql-identity.vibejson"), 1<<20)
	if err != nil {
		return gateway.ReplicatedTableProfile{}, errors.Join(errDevCluster, err)
	}
	var identity sqldriver.ReplicatedShardStoreIdentity
	if err := identity.UnmarshalJSON(identityRaw); err != nil {
		return gateway.ReplicatedTableProfile{}, errors.Join(errDevCluster, err)
	}
	applyRaw, err := readDevFile(filepath.Join(root, "apply-identity.vibejson"), 1<<20)
	if err != nil {
		return gateway.ReplicatedTableProfile{}, errors.Join(errDevCluster, err)
	}
	var apply sqldriver.ReplicatedApplyIdentity
	if err := apply.UnmarshalJSON(applyRaw); err != nil {
		return gateway.ReplicatedTableProfile{}, errors.Join(errDevCluster, err)
	}
	storeID, err := decodeDev16(member.Store)
	if err != nil {
		return gateway.ReplicatedTableProfile{}, err
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
		return gateway.ReplicatedTableProfile{}, errDevCluster
	}
	relation := identity.Relations[0]
	if relation.Relation != 1 || relation.Kind != sqldriver.ReplicatedShardRelationJSON ||
		relation.Table != table || relation.Limits != identity.UserLimits ||
		identity.UserLimits.MaxKeyBytes <= 0 ||
		identity.UserLimits.MaxKeyBytes > replication.MaxMutationKeyBytes ||
		identity.UserLimits.MaxDocumentBytes <= 0 ||
		identity.UserLimits.MaxDocumentBytes > replication.MaxMutationValueBytes {
		return gateway.ReplicatedTableProfile{}, errDevCluster
	}
	database, err := sqldriver.OpenReplicatedShardStoreWithApply(
		filepath.Join(root, "member.vdb"), identity, apply,
	)
	if err != nil {
		return gateway.ReplicatedTableProfile{}, errors.Join(errDevCluster, err)
	}
	if err := database.Close(); err != nil {
		return gateway.ReplicatedTableProfile{}, errors.Join(errDevCluster, err)
	}
	return gateway.ReplicatedTableProfile{
		Table: table, Relation: replication.RelationID(relation.Relation),
		PrimaryKey: primaryKey, SchemaGeneration: image.SchemaGeneration,
		RelationManifestDigest: replication.Digest(image.RelationManifestDigest),
		MaxKeyBytes:            uint16(identity.UserLimits.MaxKeyBytes),
		MaxDocumentBytes:       uint32(identity.UserLimits.MaxDocumentBytes),
	}, nil
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

func serveDevCluster(ctx context.Context, m devClusterManifest, shardBinary, gatewayBinary string) error {
	memberCount := len(m.Members) + len(m.LedgerMembers) + len(m.DataMembers)
	children := make([]*devChild, 0, memberCount+1)
	exits := make(chan devChildExit, memberCount+1)
	defer stopDevChildren(children)
	for _, role := range []struct {
		name    string
		members []devClusterMember
	}{{"catalog", m.Members}, {"request-ledger", m.LedgerMembers}, {"data", m.DataMembers}} {
		for _, member := range role.members {
			marker := "vibedb-shard RF3 ready"
			if m.Nodes == devClusterRF1 {
				marker = "vibedb-shard RF1-development-only-no-HA ready"
			}
			child, err := startDevChild(shardBinary, []string{"serve-rf3", "-manifest", member.ServeManifest}, marker)
			if err != nil {
				return err
			}
			children = append(children, child)
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
	args := []string{"serve", "-catalog", m.CatalogPath, "-catalog-bootstrap-if-missing", "-catalog-relation", "1", "-catalog-session-journal", filepath.Join(filepath.Dir(m.CatalogPath), "gateway-session"), "-catalog-client-id", m.GatewayNode, "-catalog-retry-home", m.GatewayNode[:16], "-durable-ack-key", m.DurableAckKey, "-listen", m.ClientEndpoint, "-tls-certificate", m.GatewayCertificate, "-tls-key", m.GatewayKey, "-tls-roots", m.Roots, "-tls-identity-oid", devClusterOID, "-authorization-policy", m.AuthorizationPolicy, "-hot-shard-capacity", m.HotShardCapacity}
	for _, members := range [][]devClusterMember{m.Members, m.LedgerMembers, m.DataMembers} {
		for _, member := range members {
			args = append(args, "-shard-peer", member.Native+"="+member.Node)
		}
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

func watchDevChildExit(exits chan<- devChildExit, name string, child *devChild) {
	go func() {
		<-child.done
		exits <- devChildExit{name: name, err: child.waitError()}
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
	diagnostics := &devDiagnostics{maximum: 64 << 10}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	command := exec.Command(binary, args...)
	command.Stdout = diagnostics
	command.Stderr = writer
	child := &devChild{command: command, ready: make(chan error, 1), done: make(chan struct{}), diagnostics: diagnostics}
	if err = command.Start(); err != nil {
		reader.Close()
		writer.Close()
		return nil, err
	}
	writer.Close()
	go func() {
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 4096), 64<<10)
		found := false
		for scanner.Scan() {
			line := scanner.Text()
			diagnostics.Write([]byte(line + "\n"))
			if !found && strings.Contains(line, marker) {
				found = true
				child.ready <- nil
			}
		}
		if !found {
			child.ready <- errors.Join(errors.New(diagnostics.String()), scanner.Err())
		}
		reader.Close()
	}()
	go func() {
		err := command.Wait()
		child.waitMu.Lock()
		child.waitErr = err
		child.waitMu.Unlock()
		close(child.done)
	}()
	return child, nil
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
}

func (d *devDiagnostics) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if n := d.maximum - len(d.data); n > 0 {
		d.data = append(d.data, p[:min(n, len(p))]...)
	}
	return len(p), nil
}
func (d *devDiagnostics) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return string(bytes.Clone(d.data))
}

func reserveDevPorts(count int) ([]string, error) {
	listeners := make([]net.Listener, count)
	addresses := make([]string, count)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, open := range listeners {
				if open != nil {
					open.Close()
				}
			}
			return nil, err
		}
		listeners[i] = l
		addresses[i] = l.Addr().String()
	}
	for _, l := range listeners {
		if err := l.Close(); err != nil {
			return nil, err
		}
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

func writeDevPolicy(path string, nodes []rafttransport.NodeID) error {
	encoded := make([]string, len(nodes))
	for i, n := range nodes {
		encoded[i] = hex.EncodeToString(n[:])
	}
	sort.Strings(encoded)
	caps := []string{"data_read", "data_write", "schema", "delegate", "membership", "topology", "transaction_recovery", "request_ledger", "execution_pin"}
	policy := devPolicy{Generation: 1, Principals: make([]devPrincipal, len(encoded))}
	for i, n := range encoded {
		policy.Principals[i] = devPrincipal{Node: n, Capabilities: caps}
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
		capacity[resource] = 1_000_000
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

func validDevManifest(m devClusterManifest, root string) bool {
	if m.Format != devClusterFormat || m.Nodes != devClusterRF1 && m.Nodes != devClusterRF3 ||
		len(m.Members) != int(m.Nodes) || len(m.LedgerMembers) != int(m.Nodes) ||
		len(m.DataMembers) != int(m.Nodes) ||
		!validDevLoopbackAddress(m.ClientEndpoint) {
		return false
	}
	if _, err := decodeDev16(m.GatewayNode); err != nil {
		return false
	}
	paths := []string{m.CatalogPath, m.GatewayCertificate, m.GatewayKey, m.Roots, m.AuthorizationPolicy, m.HotShardCapacity, m.DurableAckKey}
	addresses := map[string]struct{}{m.ClientEndpoint: {}}
	nodes := map[string]struct{}{m.GatewayNode: {}}
	stores := make(map[string]struct{}, len(m.Members)+len(m.LedgerMembers)+len(m.DataMembers))
	for roleIndex, members := range [][]devClusterMember{m.Members, m.LedgerMembers, m.DataMembers} {
		for index, member := range members {
			paths = append(paths, member.ServeManifest)
			if member.Member != uint64(index+1) {
				return false
			}
			if _, err := decodeDev16(member.Node); err != nil {
				return false
			}
			if _, err := decodeDev16(member.Store); err != nil {
				return false
			}
			if roleIndex == 0 {
				if _, duplicate := nodes[member.Node]; duplicate {
					return false
				}
				nodes[member.Node] = struct{}{}
			} else if member.Node != m.Members[index].Node {
				// The independently replicated groups share the provisioned
				// physical node identity, but never a store or listener.
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
				if _, duplicate := addresses[address]; duplicate {
					return false
				}
				addresses[address] = struct{}{}
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

func validDevLoopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		return false
	}
	value, err := strconv.Atoi(port)
	return err == nil && value > 0 && value <= 65535 && strconv.Itoa(value) == port
}
func decodeDev16(value string) ([16]byte, error) {
	var out [16]byte
	if len(value) != 32 {
		return out, errDevCluster
	}
	_, err := hex.Decode(out[:], []byte(value))
	if err == nil && out == ([16]byte{}) {
		err = errDevCluster
	}
	return out, err
}
func runDevCommand(binary string, args ...string) error {
	command := exec.Command(binary, args...)
	diagnostic := &devDiagnostics{maximum: 64 << 10}
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
