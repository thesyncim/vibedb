package main

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3qualification"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

// Physical-node preparation is kept separate from the original RF1/role
// fixture generator. That makes the fresh RF3 topology explicit while still
// allowing old development fixtures to be inspected and rejected cleanly.
type devPhysicalRole struct {
	name, distribution, shard, table, createTable, shardKey string
	shardIncarnation, groupID                               [16]byte
	requestLedger                                           bool
}

// Physical storage and frontend identities are separate principals. Storage
// listeners need the data service capabilities only; in particular they must
// not inherit Delegate, which would let a storage certificate impersonate a
// frontend. Every frontend needs the gateway runtime capabilities, while the
// designated controller (node zero) additionally owns membership and backup.
// Other frontends can forward schema requests; only node zero owns recovery
// and performs the authenticated schema operations.
func writeDevPhysicalPolicy(path string, storage, gateways []rafttransport.NodeID, client rafttransport.NodeID, controller bool) error {
	if len(storage) == 0 || len(gateways) == 0 || client == (rafttransport.NodeID{}) {
		return errDevCluster
	}
	seen := make(map[rafttransport.NodeID]struct{}, len(storage)+len(gateways)+1)
	for _, node := range append(append(append([]rafttransport.NodeID{}, storage...), gateways...), client) {
		if node == (rafttransport.NodeID{}) {
			return errDevCluster
		}
		if _, ok := seen[node]; ok {
			return errDevCluster
		}
		seen[node] = struct{}{}
	}
	storageCapabilities := []string{"data_read", "data_write"}
	frontendCapabilities := []string{"data_read", "data_write", "schema", "delegate", "topology", "transaction_recovery", "request_ledger", "execution_pin"}
	controllerCapabilities := append([]string(nil), frontendCapabilities...)
	if controller {
		// Keep the capability order in the policy grammar's canonical bit order.
		controllerCapabilities = []string{"data_read", "data_write", "schema", "delegate", "membership", "topology", "transaction_recovery", "request_ledger", "execution_pin", "backup"}
	}
	principals := make([]devPrincipal, 0, len(storage)+len(gateways)+1)
	for _, node := range storage {
		principals = append(principals, devPrincipal{Node: devIDString(node[:]), Capabilities: append([]string(nil), storageCapabilities...)})
	}
	for index, node := range gateways {
		caps := frontendCapabilities
		if controller && index == 0 {
			caps = controllerCapabilities
		}
		principals = append(principals, devPrincipal{Node: devIDString(node[:]), Capabilities: append([]string(nil), caps...)})
	}
	principals = append(principals, devPrincipal{Node: devIDString(client[:]), Capabilities: []string{"data_read", "data_write"}})
	sort.Slice(principals, func(i, j int) bool { return principals[i].Node < principals[j].Node })
	policy := devPolicy{Generation: 1, Principals: principals}
	raw, err := vibejson.Marshal(&policy)
	if err != nil {
		return err
	}
	return writeDevExclusive(path, raw, 0o600)
}

func initializeDevPhysicalCluster(options devClusterOptions, manifestPath string) (devClusterManifest, error) {
	if options.readAuthority && !rf3qualification.ReadAuthorityEnabled {
		return devClusterManifest{}, fmt.Errorf("%w: read authority requires the explicitly tagged laboratory build %q", errDevCluster, rf3qualification.ReadAuthorityLabBuildTag)
	}
	if options.replicas != devClusterRF3 ||
		(options.physicalNodes != devClusterPhysicalNodes3 && options.physicalNodes != devClusterPhysicalNodes6) {
		return devClusterManifest{}, fmt.Errorf("%w: RF3 physical nodes must be 3 or 6", errDevCluster)
	}
	if len(options.pgListens) != 0 && len(options.pgListens) != options.physicalNodes {
		return devClusterManifest{}, errDevCluster
	}
	pgEnabled := options.pgListen != ""
	for _, address := range options.pgListens {
		pgEnabled = pgEnabled || address != ""
	}
	if pgEnabled && len(filepath.Join(options.root, "node-1", "pg-ddl.sock")) >= len(syscall.RawSockaddrUnix{}.Path) {
		return devClusterManifest{}, fmt.Errorf("%w: cluster root is too long for the PostgreSQL DDL Unix socket", errDevCluster)
	}
	// Each process owns four shard listeners, one embedded frontend listener,
	// and a separate gateway-control listener. Reserve requested PG bindings
	// before assigning those ports, and fail before writing durable identities
	// when a caller's requested endpoint is unavailable.
	physical := options.physicalNodes
	pgAddresses := options.pgListens
	if len(pgAddresses) == 0 && options.pgListen != "" {
		pgAddresses = []string{options.pgListen}
	}
	ports, err := reserveDevPorts(1+physical*6, pgAddresses...)
	if err != nil {
		return devClusterManifest{}, err
	}
	if err := os.MkdirAll(options.root, 0o700); err != nil {
		return devClusterManifest{}, err
	}
	var clusterID, clusterIncarnation [16]byte
	var shardIncarnations [3][16]byte
	var groupIDs [3][16]byte
	for _, target := range [][]byte{clusterID[:], clusterIncarnation[:]} {
		if _, err := io.ReadFull(cryptorand.Reader, target); err != nil {
			return devClusterManifest{}, err
		}
	}
	for i := range shardIncarnations {
		if _, err := io.ReadFull(cryptorand.Reader, shardIncarnations[i][:]); err != nil {
			return devClusterManifest{}, err
		}
		if _, err := io.ReadFull(cryptorand.Reader, groupIDs[i][:]); err != nil {
			return devClusterManifest{}, err
		}
	}

	storageNodes := make([]rafttransport.NodeID, physical)
	gatewayNodes := make([]rafttransport.NodeID, physical)
	for i := 0; i < physical; i++ {
		if _, err := io.ReadFull(cryptorand.Reader, storageNodes[i][:]); err != nil {
			return devClusterManifest{}, err
		}
		if _, err := io.ReadFull(cryptorand.Reader, gatewayNodes[i][:]); err != nil {
			return devClusterManifest{}, err
		}
	}
	var clientNode rafttransport.NodeID
	if _, err := io.ReadFull(cryptorand.Reader, clientNode[:]); err != nil {
		return devClusterManifest{}, err
	}
	stores := make([][3][16]byte, 3)
	for role := range stores {
		for member := range stores[role] {
			if _, err := io.ReadFull(cryptorand.Reader, stores[role][member][:]); err != nil {
				return devClusterManifest{}, err
			}
		}
	}
	identities := append(append(append([]rafttransport.NodeID{}, storageNodes...), gatewayNodes...), clientNode)
	credentials, roots, err := writeDevCredentialsWithCA(options.root,
		rafttransport.TrustDomain{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation}, identities,
		options.tlsCACertificate, options.tlsCAKey)
	if err != nil {
		return devClusterManifest{}, err
	}
	policyPath := filepath.Join(options.root, "authorization-policy.vibejson")
	if err := writeDevPhysicalPolicy(policyPath, storageNodes, gatewayNodes, clientNode, true); err != nil {
		return devClusterManifest{}, err
	}
	ackPath := filepath.Join(options.root, "durable-ack-key")
	var ack [32]byte
	if _, err := io.ReadFull(cryptorand.Reader, ack[:]); err != nil {
		return devClusterManifest{}, err
	}
	ackHex := make([]byte, hex.EncodedLen(len(ack)))
	hex.Encode(ackHex, ack[:])
	clear(ack[:])
	if err := writeDevExclusive(ackPath, ackHex, 0o600); err != nil {
		clear(ackHex)
		return devClusterManifest{}, err
	}
	clear(ackHex)
	keySource := filepath.Join(options.root, "wal-key-source")
	keyMaterial := make([]byte, 32)
	if _, err := io.ReadFull(cryptorand.Reader, keyMaterial); err != nil {
		return devClusterManifest{}, err
	}
	if err := writeDevExclusive(keySource, keyMaterial, 0o600); err != nil {
		clear(keyMaterial)
		return devClusterManifest{}, err
	}
	clear(keyMaterial)

	frontend := make([]string, physical)
	for i := range frontend {
		frontend[i] = ports[1+i*6+5]
	}
	cluster := devClusterManifest{
		Format: devClusterFormat, Nodes: devClusterRF3, Replicas: devClusterRF3,
		PhysicalNodes: uint8(physical), NodeLog: true,
		ReadAuthority:  newDevReadAuthority(options.readAuthority),
		ClientEndpoint: frontend[0], CatalogPath: filepath.Join(options.root, "catalog.vibejson"),
		GatewayCertificate: credentials[physical][0], GatewayKey: credentials[physical][1],
		Roots: roots, AuthorizationPolicy: policyPath, HotShardCapacity: filepath.Join(options.root, "hot-shard-capacity.vibejson"),
		ReplicaControl: filepath.Join(options.root, "replica-control.vibejson"), DurableAckKey: ackPath,
		GatewayNode: devIDString(gatewayNodes[0][:]), GatewayControl: ports[1+0*6+4],
		Members: make([]devClusterMember, devClusterRF3), LedgerMembers: make([]devClusterMember, devClusterRF3), DataMembers: make([]devClusterMember, devClusterRF3),
		NodeManifests: make([]devPhysicalNode, physical),
	}
	cluster.ClientCertificate, cluster.ClientKey = credentials[len(identities)-1][0], credentials[len(identities)-1][1]
	cluster.ClientNode = devIDString(clientNode[:])

	roles := [3]devPhysicalRole{
		{name: "catalog", distribution: string(gateway.ReplicatedCatalogDistribution), shard: string(gateway.ReplicatedCatalogShard), table: gateway.ReplicatedCatalogTable, createTable: "CREATE TABLE controlplane (PRIMARY KEY (id))", shardKey: gateway.ReplicatedCatalogPrimaryKey, shardIncarnation: shardIncarnations[0], groupID: groupIDs[0]},
		{name: "ledger", distribution: string(devLedgerDistribution), shard: string(devLedgerShard), table: devLedgerTable, createTable: "CREATE TABLE request_ledger (PRIMARY KEY (id))", shardKey: devLedgerPrimaryKey, shardIncarnation: shardIncarnations[1], groupID: groupIDs[1], requestLedger: true},
		{name: "data", distribution: string(devDataDistribution), shard: string(devDataShard), table: devDataTable, createTable: "CREATE TABLE documents (PRIMARY KEY (id))", shardKey: devDataPrimaryKey, shardIncarnation: shardIncarnations[2], groupID: groupIDs[2]},
	}
	roleMembers := [3][]devClusterMember{make([]devClusterMember, 3), make([]devClusterMember, 3), make([]devClusterMember, 3)}
	groupRoots := [3][]string{make([]string, physical), make([]string, physical), make([]string, physical)}
	for roleIndex := range roles {
		placement := devPhysicalPlacement(physical, roleIndex)
		for memberIndex, nodeIndex := range placement {
			roleMembers[roleIndex][memberIndex] = devClusterMember{
				Member: uint64(memberIndex + 1), Node: devIDString(storageNodes[nodeIndex][:]),
				PhysicalNode: devIDString(storageNodes[nodeIndex][:]), Store: devIDString(stores[roleIndex][memberIndex][:]),
				Peer: ports[1+nodeIndex*6], Native: ports[1+nodeIndex*6+1], Snapshot: ports[1+nodeIndex*6+2], Control: ports[1+nodeIndex*6+3],
			}
		}
	}
	cluster.Members, cluster.LedgerMembers, cluster.DataMembers = roleMembers[0], roleMembers[1], roleMembers[2]

	// Every physical node owns one shared node log and one embedded frontend.
	// The frontend's persistent session and recovery journals are private to
	// that gateway principal, including when all nodes use one catalog file.
	for nodeIndex := 0; nodeIndex < physical; nodeIndex++ {
		nodeRoot := filepath.Join(options.root, fmt.Sprintf("node-%d", nodeIndex+1))
		nodeManifestPath := filepath.Join(nodeRoot, "serve-rf3.vibejson")
		gatewayBase := filepath.Join(nodeRoot, "gateway")
		gatewayConfig := devGatewayConfig{
			InitialNodeDirectoryPath: filepath.Join(options.root, "initial-node-directory.vibejson"),
			CatalogPath:              filepath.Join(options.root, "catalog.vibejson"), CatalogRouteSeedPath: filepath.Join(gatewayBase, "catalog-route-seed"),
			CatalogBootstrapIfMissing: nodeIndex == 0, CatalogRelation: 1, CatalogAttempts: 8,
			CatalogAttemptTimeoutMillis: 5000, CatalogSessionLeaseMillis: uint64((24 * time.Hour) / time.Millisecond),
			CatalogSessionJournal: filepath.Join(gatewayBase, "catalog-session"), CatalogClientID: devIDString(gatewayNodes[nodeIndex][:]),
			CatalogRetryHome: devRetryHomeString(gatewayRetryHome(clusterID, gatewayNodes[nodeIndex])), DurableAckKey: ackPath,
			Listen: frontend[nodeIndex], PGListen: "", PGDDLSocket: "", TLS: devGatewayTLS{Certificate: credentials[physical+nodeIndex][0], Key: credentials[physical+nodeIndex][1], Roots: roots, IdentityOID: devClusterOID},
			TLSHandshakeTimeoutMillis: 5000,
			AuthorizationPolicy:       policyPath, MaxConnections: 1024, MaxHandshakes: 64, MaxShardConnections: 4096, MaxShardHandshakes: 64,
			MaxNativeReadConcurrency: 64, MaxNativeReadBytes: 64 << 20, MaxNativeScatterConcurrency: 32,
			ShardPeers: make([]devGatewayShardPeer, physical), TableCatalogs: []string{},
			ReplicaControlManifest: devPhysicalControlPath(cluster, nodeIndex), ControlParticipantOnly: nodeIndex != 0,
		}
		if len(options.pgListens) != 0 {
			gatewayConfig.PGListen = options.pgListens[nodeIndex]
		} else if nodeIndex == 0 {
			gatewayConfig.PGListen = options.pgListen
		}
		if gatewayConfig.PGListen != "" {
			gatewayConfig.PGDDLSocket = filepath.Join(options.root, "node-1", "pg-ddl.sock")
			if nodeIndex != 0 {
				gatewayConfig.DDLOwnerAddress, gatewayConfig.DDLOwnerNode = cluster.ClientEndpoint, cluster.GatewayNode
			}
		}
		for peerIndex := range gatewayConfig.ShardPeers {
			gatewayConfig.ShardPeers[peerIndex] = devGatewayShardPeer{Address: ports[1+peerIndex*6+1], NodeID: devIDString(storageNodes[peerIndex][:])}
		}
		if nodeIndex == 0 {
			gatewayConfig.TableCatalogsPath = filepath.Join(options.root, "table-catalogs.vibejson")
			gatewayConfig.HotShardCapacity = cluster.HotShardCapacity
			gatewayConfig.HotShardIntervalMillis = 1000
			gatewayConfig.ReplicaControlManifest = cluster.ReplicaControl
			gatewayConfig.BackupRepository = filepath.Join(options.root, "backups")
			gatewayConfig.BackupMaxBackups, gatewayConfig.BackupMaxArtifacts = 16, 4096
			gatewayConfig.BackupMaxArtifactBytes, gatewayConfig.BackupMaxDiskBytes = 64<<30, 256<<30
			gatewayConfig.ControllerIntervalMillis = 1000
		}
		node := devPhysicalNode{
			Node: devIDString(storageNodes[nodeIndex][:]), Certificate: credentials[nodeIndex][0], Key: credentials[nodeIndex][1],
			GatewayNode: devIDString(gatewayNodes[nodeIndex][:]), GatewayCertificate: credentials[physical+nodeIndex][0], GatewayKey: credentials[physical+nodeIndex][1],
			GatewayControl: ports[1+nodeIndex*6+4], FrontendListen: frontend[nodeIndex], ServeManifest: nodeManifestPath,
			CatalogSessionJournal: gatewayConfig.CatalogSessionJournal, DirectIssuerJournal: gatewayConfig.CatalogSessionJournal + ".pg-writes.direct",
			FallbackJournal: gatewayConfig.CatalogSessionJournal + ".pg-writes", ExecutionPinJournal: gatewayConfig.CatalogSessionJournal + ".durable-pins",
		}
		for roleIndex, role := range roles {
			if !containsDevPhysical(devPhysicalPlacement(physical, roleIndex), nodeIndex) {
				continue
			}
			ordinal := len(node.Groups)
			groupRoot := filepath.Join(nodeRoot, fmt.Sprintf("group-%d", ordinal))
			groupRoots[roleIndex][nodeIndex] = groupRoot
			node.Groups = append(node.Groups, groupRoot)
			members := make([]devPrepareMember, 3)
			for memberIndex, member := range roleMembers[roleIndex] {
				members[memberIndex] = devPrepareMember{MemberID: member.Member, NodeID: member.Node, PeerAddress: member.Peer}
				if options.readAuthority {
					members[memberIndex].StoreID = member.Store
					members[memberIndex].NativeAddress = member.Native
				}
			}
			// Split authorization is node-wide. Every group on one physical
			// node must carry the same grant set so prepare-node-rf3 can prove
			// that the shared sequencer/control owner was not changed by a
			// group-local manifest.
			sharedMembers := make([]devPrepareMember, physical)
			for memberIndex := range storageNodes {
				sharedMembers[memberIndex] = devPrepareMember{MemberID: uint64(memberIndex + 1), NodeID: devIDString(storageNodes[memberIndex][:]), PeerAddress: ports[1+memberIndex*6]}
			}
			apply := devPrepareApplyProfile(role.shardKey, replication.Digest{})
			if role.requestLedger {
				ledgerGroup := raftmember.GroupKey{ClusterID: clusterID, ClusterIncarnation: clusterIncarnation, TopologyRecoveryEpoch: 1, ShardIncarnation: role.shardIncarnation, GroupID: role.groupID}
				apply = devPrepareApplyProfile(role.shardKey, deriveDevLedgerHomeIdentity(ledgerGroup))
			}
			prep := devPrepareManifest{
				Root: groupRoot, Distribution: role.distribution, Shard: role.shard, ClusterID: devIDString(clusterID[:]), ClusterIncarnation: devIDString(clusterIncarnation[:]),
				TopologyRecoveryEpoch: 1, AllocationGeneration: 1, ShardIncarnation: devIDString(role.shardIncarnation[:]), GroupID: devIDString(role.groupID[:]),
				MemberID: uint64(indexOfDevPhysical(devPhysicalPlacement(physical, roleIndex), nodeIndex) + 1), StoreID: roleMembers[roleIndex][indexOfDevPhysical(devPhysicalPlacement(physical, roleIndex), nodeIndex)].Store,
				Table: role.table, CreateTable: role.createTable, Authority: devPrepareAuthority{ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1},
				WAL:   devPrepareWAL{KeyID: "dev-cluster-key", KeyMaterialPath: keySource, WrappedKey: "local-development-only", MaxFileBytes: raftstore.DefaultMaxFileBytes, MaxRecordBytes: raftstore.DefaultMaxRecordBytes, MaxRecords: raftstore.DefaultMaxRecords, MaxEntries: raftstore.DefaultMaxEntries, MaxLiveBytes: raftstore.DefaultMaxLiveBytes},
				Apply: apply, Listeners: devPrepareListeners{Peer: ports[1+nodeIndex*6], Native: ports[1+nodeIndex*6+1], Snapshot: ports[1+nodeIndex*6+2], Control: ports[1+nodeIndex*6+3]},
				TLS: devPrepareTLS{Certificate: credentials[nodeIndex][0], Key: credentials[nodeIndex][1], Roots: roots, IdentityOID: devClusterOID}, AuthorizationPolicy: policyPath,
				SplitControl: devPrepareSplitControlProfile(sharedMembers, devIDString(gatewayNodes[0][:])), ReadAuthority: newDevReadAuthority(options.readAuthority), Members: members,
			}
			_ = prep // The input is emitted below after all role groups are collected.
			// Keep the group preparation in a sidecar inventory. The node input
			// below is assembled from the exact same values.
			if err := appendDevPhysicalGroupPreparation(nodeRoot, prep); err != nil {
				return devClusterManifest{}, err
			}
		}
		// Re-read the sidecar preparations in the deterministic group order.
		groups := make([]devPrepareManifest, 0, len(node.Groups))
		for ordinal := range node.Groups {
			raw, err := readDevFile(nodeRoot+"."+fmt.Sprintf("group-%d.prepare.vibejson", ordinal), 1<<20)
			if err != nil {
				return devClusterManifest{}, err
			}
			var group devPrepareManifest
			if err := vibejson.Unmarshal(raw, &group); err != nil {
				return devClusterManifest{}, err
			}
			groups = append(groups, group)
		}
		for roleIndex := range roles {
			for memberIndex := range roleMembers[roleIndex] {
				member := &roleMembers[roleIndex][memberIndex]
				placement := devPhysicalPlacement(physical, roleIndex)
				if memberIndex >= len(placement) || placement[memberIndex] < 0 || placement[memberIndex] >= physical {
					return devClusterManifest{}, errDevCluster
				}
				member.GroupRoot = groupRoots[roleIndex][placement[memberIndex]]
				member.ServeManifest = filepath.Join(options.root,
					fmt.Sprintf("node-%d", placement[memberIndex]+1), "serve-rf3.vibejson")
			}
		}
		gatewayPtr := gatewayConfig
		nodeInput := devPrepareNodeManifest{Root: nodeRoot, NodeLog: devNodeLogManifest{Format: 1, Path: filepath.Join(nodeRoot, "node-log"), KeyID: "dev-cluster-key", KeyMaterialPath: keySource, Options: raftstore.NodeStoreOptions{MaxGroups: 64}}, Gateway: &gatewayPtr, Groups: groups}
		if err := persistDevPhysicalNodeInput(nodeRoot, nodeInput); err != nil {
			return devClusterManifest{}, err
		}
		node.Groups = node.Groups[:0]
		for _, group := range groups {
			node.Groups = append(node.Groups, group.Root)
		}
		cluster.NodeManifests[nodeIndex] = node
	}
	cluster.Members, cluster.LedgerMembers, cluster.DataMembers = roleMembers[0], roleMembers[1], roleMembers[2]
	if err := writeDevPhysicalCapacity(cluster); err != nil {
		return devClusterManifest{}, err
	}
	raw, err := vibejson.Marshal(&cluster)
	if err != nil {
		return devClusterManifest{}, err
	}
	if err := writeDevExclusive(manifestPath, raw, 0o600); err != nil {
		return devClusterManifest{}, err
	}
	if err := syncDevDir(options.root); err != nil {
		return devClusterManifest{}, err
	}
	return cluster, completeDevPhysicalCluster(options, cluster)
}

func appendDevPhysicalGroupPreparation(nodeRoot string, prep devPrepareManifest) error {
	if !filepath.IsAbs(prep.Root) || filepath.Dir(prep.Root) != nodeRoot {
		return errDevCluster
	}
	path := nodeRoot + "." + filepath.Base(prep.Root) + ".prepare.vibejson"
	raw, err := vibejson.Marshal(&prep)
	if err != nil {
		return err
	}
	return writeDevFileOnce(path, raw)
}

func persistDevPhysicalNodeInput(nodeRoot string, input devPrepareNodeManifest) error {
	raw, err := vibejson.Marshal(&input)
	if err != nil {
		return err
	}
	path := nodeRoot + ".prepare-node.vibejson"
	return writeDevFileOnce(path, raw)
}

func completeDevPhysicalCluster(options devClusterOptions, manifest devClusterManifest) error {
	if manifest.PhysicalNodes == 0 || len(manifest.NodeManifests) != int(manifest.PhysicalNodes) {
		return errDevCluster
	}
	for _, node := range manifest.NodeManifests {
		if _, err := os.Stat(node.ServeManifest); err == nil {
			if err := validateDevReadAuthorityFile(node.ServeManifest, manifest.ReadAuthority); err != nil {
				return err
			}
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		root := filepath.Dir(node.ServeManifest)
		inputPath := root + ".prepare-node.vibejson"
		raw, err := readDevFile(inputPath, 4<<20)
		if err != nil {
			return err
		}
		var input devPrepareNodeManifest
		if err := vibejson.Unmarshal(raw, &input); err != nil {
			return errors.Join(errDevCluster, err)
		}
		if len(input.Groups) == 0 {
			return fmt.Errorf("%w: physical node has no prepared groups", errDevCluster)
		}
		for _, group := range input.Groups {
			if !devReadAuthorityEqual(group.ReadAuthority, manifest.ReadAuthority) ||
				group.ReadAuthority != nil && !validDevReadAuthority(*group.ReadAuthority) {
				return fmt.Errorf("%w: physical node read authority differs from cluster policy", errDevCluster)
			}
		}
		if err := prepareDevPhysicalNode(options.shardBinary, input); err != nil {
			return err
		}
	}
	for index := range manifest.NodeManifests {
		if err := validateDevPhysicalNodeConfig(manifest, index); err != nil {
			return err
		}
	}
	if err := writeDevPhysicalControls(manifest); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(options.root, "table-catalogs.vibejson")); errors.Is(err, os.ErrNotExist) {
		if err := updateDevPhysicalGatewayCatalogs(manifest, nil); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	// The catalog is written only after every node has published its durable
	// group identities. This prevents a partial physical inventory from becoming
	// a routable fresh cluster.
	if _, err := os.Stat(manifest.CatalogPath); errors.Is(err, os.ErrNotExist) {
		if err := writeDevPhysicalCatalog(manifest); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if err := validateDevPhysicalCatalog(manifest); err != nil {
		return err
	}
	return writeDevInitialNodeDirectory(manifest)
}

func writeDevPhysicalCatalog(cluster devClusterManifest) error {
	if cluster.PhysicalNodes == 0 || len(cluster.Members) != devClusterRF3 || len(cluster.LedgerMembers) != devClusterRF3 || len(cluster.DataMembers) != devClusterRF3 {
		return errDevCluster
	}
	groups := [3]raftmember.GroupKey{mustDevGroup(cluster.Members), mustDevGroup(cluster.LedgerMembers), mustDevGroup(cluster.DataMembers)}
	return writeDevCatalog(cluster, groups[0].ClusterID, groups[0].ClusterIncarnation,
		groups[0].ShardIncarnation, groups[0].GroupID,
		groups[1].ShardIncarnation, groups[1].GroupID,
		groups[2].ShardIncarnation, groups[2].GroupID)
}

func validateDevPhysicalCatalog(cluster devClusterManifest) error {
	// Reuse the strict existing catalog witness checks. It verifies each route,
	// store binding, schema image, and request-ledger home against the inventory.
	return validateExistingDevCatalogPhysical(cluster)
}

func mustDevGroup(members []devClusterMember) (group raftmember.GroupKey) {
	if len(members) == 0 {
		return group
	}
	raw, err := readDevFile(filepath.Join(devMemberRoot(members[0]), "sql-identity.vibejson"), 1<<20)
	if err != nil {
		return group
	}
	var identity sqldriver.ReplicatedShardStoreIdentity
	if err := identity.UnmarshalJSON(raw); err != nil {
		return group
	}
	group.ClusterID, group.ClusterIncarnation = identity.Binding.ClusterID, identity.Binding.ClusterIncarnation
	group.TopologyRecoveryEpoch, group.ShardIncarnation, group.GroupID = identity.Binding.TopologyRecoveryEpoch, identity.Binding.ShardIncarnation, identity.Binding.GroupID
	return group
}

// validateExistingDevCatalogPhysical keeps route verification in cluster_dev.go
// while deriving the three group keys from the immutable prepared identities.
func validateExistingDevCatalogPhysical(cluster devClusterManifest) error {
	groups := [3]raftmember.GroupKey{mustDevGroup(cluster.Members), mustDevGroup(cluster.LedgerMembers), mustDevGroup(cluster.DataMembers)}
	if groups[0] == (raftmember.GroupKey{}) || groups[1] == (raftmember.GroupKey{}) || groups[2] == (raftmember.GroupKey{}) {
		return errDevCluster
	}
	return validateExistingDevCatalog(cluster.CatalogPath, cluster, groups[0], groups[1], groups[2])
}

func devPhysicalPlacement(physical, groupOrdinal int) []int {
	if physical == devClusterPhysicalNodes3 {
		return []int{0, 1, 2}
	}
	start := (groupOrdinal * 2) % physical
	return []int{start, (start + 1) % physical, (start + 2) % physical}
}

func containsDevPhysical(values []int, target int) bool {
	return indexOfDevPhysical(values, target) >= 0
}

func indexOfDevPhysical(values []int, target int) int {
	for index, value := range values {
		if value == target {
			return index
		}
	}
	return -1
}

func gatewayRetryHome(clusterID [16]byte, gatewayNode rafttransport.NodeID) [8]byte {
	var raw [24]byte
	copy(raw[:16], clusterID[:])
	copy(raw[16:], gatewayNode[:8])
	// A stable nonzero home is sufficient here; the catalog authority still
	// binds it to the per-frontend client identity during startup.
	return [8]byte{raw[0] ^ raw[16], raw[1] ^ raw[17], raw[2] ^ raw[18], raw[3] ^ raw[19], raw[4] ^ raw[20], raw[5] ^ raw[21], raw[6] ^ raw[22], raw[7] ^ raw[23]}
}

func devRetryHomeString(home [8]byte) string {
	result := make([]byte, hex.EncodedLen(len(home)))
	hex.Encode(result, home[:])
	return string(result)
}

func devPhysicalControlPath(cluster devClusterManifest, index int) string {
	if index == 0 {
		return cluster.ReplicaControl
	}
	return filepath.Join(filepath.Dir(cluster.CatalogPath), fmt.Sprintf("node-%d-replica-control.vibejson", index+1))
}

func writeDevPhysicalControls(cluster devClusterManifest) error {
	base, err := devReplicaControlConfig(cluster)
	if err != nil {
		return err
	}
	base.GatewayEndpoints = make([]devReplicaControlEndpoint, len(cluster.NodeManifests))
	for index, node := range cluster.NodeManifests {
		base.GatewayEndpoints[index] = devReplicaControlEndpoint{Node: node.GatewayNode, Incarnation: 1, ControlAddress: node.GatewayControl}
	}
	sort.Slice(base.GatewayEndpoints, func(i, j int) bool { return base.GatewayEndpoints[i].Node < base.GatewayEndpoints[j].Node })
	for index, node := range cluster.NodeManifests {
		manifest := base
		manifest.LocalGateway = devReplicaControlEndpoint{Node: node.GatewayNode, Incarnation: 1, ControlAddress: node.GatewayControl}
		manifest.TLS.Certificate, manifest.TLS.Key = node.GatewayCertificate, node.GatewayKey
		if index != 0 {
			manifest.SplitSources = nil
		}
		raw, err := vibejson.Marshal(&manifest)
		if err != nil {
			return err
		}
		if err := writeDevFileOnce(devPhysicalControlPath(cluster, index), raw); err != nil {
			return err
		}
	}
	return syncDevDir(filepath.Dir(cluster.CatalogPath))
}

func writeDevPhysicalCapacity(cluster devClusterManifest) error {
	var capacity autosplit.CapacityVector
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 64
	}
	config := hotshard.StaticCapacityConfig{Format: hotshard.StaticCapacityFormat, RecorderLanes: 4,
		WindowCapacity: capacity, NodeCapacity: capacity, MigrationCapacity: 1 << 30, ShardMigrationBytes: 384 << 20, MaxReceives: 2}
	for nodeIndex, node := range cluster.NodeManifests {
		var endpoint distribution.EndpointID
		for roleIndex, members := range [][]devClusterMember{cluster.Members, cluster.LedgerMembers, cluster.DataMembers} {
			for index, member := range members {
				if member.Node == node.Node && endpoint == "" {
					endpoint = distribution.EndpointID(fmt.Sprintf("%s-member-%d", []string{"catalog", "ledger", "data"}[roleIndex], index+1))
				}
			}
		}
		if endpoint == "" {
			return errDevCluster
		}
		config.Nodes = append(config.Nodes, hotshard.StaticCapacityNode{Endpoint: endpoint, FailureDomain: uint32(nodeIndex + 1)})
	}
	sort.Slice(config.Nodes, func(i, j int) bool { return config.Nodes[i].Endpoint < config.Nodes[j].Endpoint })
	raw, err := hotshard.AppendStaticCapacityConfig(nil, config)
	if err != nil {
		return err
	}
	return writeDevFileOnce(cluster.HotShardCapacity, raw)
}

func devPhysicalPGListens(single, multiple string, replicas, physical int) ([]string, error) {
	if single != "" && multiple != "" {
		return nil, fmt.Errorf("%w: --pg-listen and --pg-listens are mutually exclusive", errDevCluster)
	}
	if single == "" && multiple == "" {
		return nil, nil
	}
	if replicas != devClusterRF3 || physical != devClusterPhysicalNodes3 && physical != devClusterPhysicalNodes6 {
		return nil, errDevCluster
	}
	result := make([]string, physical)
	if multiple == "" {
		result[0] = single
	} else {
		result = strings.Split(multiple, ",")
		if len(result) != physical {
			return nil, fmt.Errorf("%w: --pg-listens needs exactly %d endpoints", errDevCluster, physical)
		}
	}
	seen := map[string]bool{}
	for _, address := range result {
		if address == "" && multiple == "" {
			continue
		}
		if !validDevPGAddress(address) || seen[address] {
			return nil, fmt.Errorf("%w: PostgreSQL endpoints must be distinct literal loopback addresses", errDevCluster)
		}
		seen[address] = true
	}
	return result, nil
}

func validDevPGAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	value, portErr := strconv.Atoi(port)
	return err == nil && ip != nil && ip.IsLoopback() && host == ip.String() &&
		portErr == nil && value > 0 && value <= 65535 && port == strconv.Itoa(value)
}

func validateDevPhysicalPGOptions(cluster devClusterManifest, options devClusterOptions) error {
	requested := options.pgListens
	if len(requested) == 0 && options.pgListen != "" {
		requested = make([]string, len(cluster.NodeManifests))
		requested[0] = options.pgListen
	}
	if len(requested) == 0 {
		return nil // An omitted flag retains the persisted endpoints.
	}
	if len(requested) != len(cluster.NodeManifests) {
		return errDevCluster
	}
	for index, node := range cluster.NodeManifests {
		// Inputs survive incomplete preparation and have the same immutable
		// gateway fields as the serving manifest.
		raw, err := readDevFile(filepath.Dir(node.ServeManifest)+".prepare-node.vibejson", 4<<20)
		if err != nil {
			return err
		}
		var input devPrepareNodeManifest
		if err := vibejson.Unmarshal(raw, &input); err != nil || input.Gateway == nil || input.Gateway.PGListen != requested[index] {
			return fmt.Errorf("%w: PostgreSQL endpoints differ from retained node configuration", errDevCluster)
		}
	}
	return nil
}

func devTableCatalogPath(root, table string) (string, error) {
	raw, err := readDevFile(filepath.Join(root, "tables.vibejson"), 4<<20)
	if err != nil {
		return "", err
	}
	var inventory devTableInventory
	if err := vibejson.Unmarshal(raw, &inventory); err != nil {
		return "", err
	}
	for _, entry := range inventory.Tables {
		if entry.Table == table {
			return filepath.Join(root, entry.artifactStem()+"-catalog.vibejson"), nil
		}
	}
	return "", errDevCluster
}

// Keep process inventory and emitted configuration in agreement on recovery.
// In particular a valid cluster file cannot redirect one frontend's journals,
// TLS identity, controller role, or listeners through a changed sidecar.
func validateDevPhysicalNodeConfig(cluster devClusterManifest, index int) error {
	node := cluster.NodeManifests[index]
	raw, err := readDevFile(node.ServeManifest, 4<<20)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	var gatewayConfig devGatewayConfig
	if err := vibejson.Unmarshal(fields["gateway"], &gatewayConfig); err != nil {
		return err
	}
	canonical, err := vibejson.Marshal(&gatewayConfig)
	if err != nil || !bytes.Equal(canonical, fields["gateway"]) || gatewayConfig.CatalogClientID != node.GatewayNode ||
		gatewayConfig.CatalogSessionJournal != node.CatalogSessionJournal || gatewayConfig.Listen != node.FrontendListen ||
		gatewayConfig.TLS.Certificate != node.GatewayCertificate || gatewayConfig.TLS.Key != node.GatewayKey ||
		gatewayConfig.ReplicaControlManifest != devPhysicalControlPath(cluster, index) || gatewayConfig.ControlParticipantOnly != (index != 0) {
		return errors.Join(errDevCluster, err)
	}
	inputRaw, err := readDevFile(filepath.Dir(node.ServeManifest)+".prepare-node.vibejson", 4<<20)
	if err != nil {
		return err
	}
	var input devPrepareNodeManifest
	if err := vibejson.Unmarshal(inputRaw, &input); err != nil || input.Gateway == nil ||
		input.Root != filepath.Dir(node.ServeManifest) || len(input.Groups) == 0 {
		return errors.Join(errDevCluster, err)
	}
	expected, err := vibejson.Marshal(input.Gateway)
	if err != nil || !bytes.Equal(canonical, expected) {
		return errors.Join(errDevCluster, err)
	}
	nodeLog := input.NodeLog
	nodeLog.KeyMaterialPath = filepath.Join(input.Root, "node-key")
	if err := errors.Join(
		validateDevPhysicalField(fields, "node_log", &nodeLog),
		validateDevPhysicalField(fields, "listeners", &input.Groups[0].Listeners),
		validateDevPhysicalField(fields, "tls", &input.Groups[0].TLS),
		validateDevPhysicalField(fields, "authorization_policy", &cluster.AuthorizationPolicy),
	); err != nil {
		return fmt.Errorf("physical node %d: %w", index+1, err)
	}
	var replica map[string]json.RawMessage
	if err := json.Unmarshal(fields["replica_control"], &replica); err != nil {
		return err
	}
	for field, path := range map[string]string{
		"source_data_root":       input.Root,
		"action_journal_path":    filepath.Join(input.Root, "replica-actions"),
		"source_journal_path":    filepath.Join(input.Root, "source-exports"),
		"source_repository_path": filepath.Join(input.Root, "source-artifacts"),
	} {
		var actual string
		if err := json.Unmarshal(replica[field], &actual); err != nil || actual != path {
			return fmt.Errorf("%w: physical node %d changes shared %s", errDevCluster, index+1, field)
		}
	}
	var groups []struct {
		SQL struct {
			Path string `json:"path"`
		} `json:"sql"`
	}
	if err := json.Unmarshal(fields["groups"], &groups); err != nil || len(groups) < len(input.Groups) || len(groups) > len(node.Groups) {
		return errors.Join(errDevCluster, err)
	}
	for ordinal, group := range groups {
		if group.SQL.Path != filepath.Join(node.Groups[ordinal], "member.vdb") {
			return fmt.Errorf("%w: physical node %d group %d changes its SQL root", errDevCluster, index+1, ordinal)
		}
	}
	return nil
}

func validateDevPhysicalField[T any](fields map[string]json.RawMessage, name string, value *T) error {
	expected, err := vibejson.Marshal(value)
	if err != nil || !bytes.Equal(expected, fields[name]) {
		return fmt.Errorf("%w: changed shared %s", errDevCluster, name)
	}
	return nil
}
