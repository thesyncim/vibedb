package rf3testfixture

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const ProcessIdentityOID = "1.3.6.1.4.1.32473.1.1"

type ProcessListeners struct {
	Peer     string `json:"peer"`
	Native   string `json:"native"`
	Snapshot string `json:"snapshot"`
	Control  string `json:"control"`
}

type ProcessTarget struct {
	MemberID        uint64
	NodeID          rafttransport.NodeID
	StoreID         [16]byte
	NodeIncarnation uint64
	Listeners       ProcessListeners
}

type ProcessMemberOptions struct {
	Root, ControlRoot, Table, CreateTable string
	SchemaStatements                      []string
	GlobalIndexes                         []sqldriver.ReplicatedGlobalIndexRelation
	Identity                              raftstore.Identity
	Key                                   raftstore.Key
	WAL                                   raftstore.Options
	Bootstrap                             raftstore.Bootstrap
	Authority                             sqldriver.ReplicatedAuthorityProfile
	Apply                                 sqldriver.ReplicatedApplyOptions
	Listeners                             ProcessListeners
	Credential                            Credential
	Roots, AuthorizationPolicy            string
	Nodes                                 [3]rafttransport.NodeID
	PeerAddresses                         [3]string
	Target                                *ProcessTarget
	SeedDocuments                         [][]byte
}

type PreparedProcessMember struct {
	ManifestPath, WALPath, SQLPath string
	RelationManifestDigest         [32]byte
	LogicalSchemaDigest            [32]byte
	SystemRecoveryJournalBytes     uint64
}

type PreparedColdTarget struct {
	PreparedProcessMember
	BootstrapManifestPath, StaticBootstrapPath string
}

// EmptyNodeOptions describes the public, path-free input to the shipped
// prepare-node-rf3 empty-capacity grammar.  The helper derives every node
// owned journal and storage path from Root; callers supply only the physical
// credentials, listeners, and the exact certificate principals that may use
// the node-wide split control.
type EmptyNodeOptions struct {
	Root                string
	NodeIncarnation     uint64
	Key                 raftstore.Key
	NodeStore           raftstore.NodeStoreOptions
	Listeners           ProcessListeners
	Credential          Credential
	Roots               string
	AuthorizationPolicy string
	GrantNodes          []rafttransport.NodeID
	GatewaySeeds        []nodecontrol.BootstrapGatewaySeed
}

// PreparedEmptyNode is the durable input consumed by
// `vibedb-shard prepare-node-rf3 -manifest`.  Preparation itself remains a
// CLI operation so the fixture exercises the same atomic node-root publish as
// an operator invocation.
type PreparedEmptyNode struct {
	PreparationPath, KeyMaterialPath string
}

type emptyNodePreparationManifest struct {
	Root     string               `json:"root"`
	NodeLog  emptyNodeLogManifest `json:"node_log"`
	Services emptyNodeServices    `json:"services"`
	Groups   []struct{}           `json:"groups"`
}

type emptyNodeLogManifest struct {
	Format          uint16                     `json:"format"`
	Path            string                     `json:"path"`
	KeyID           string                     `json:"key_id"`
	WrappedKey      string                     `json:"wrapped_key,omitempty"`
	KeyMaterialPath string                     `json:"key_material_path"`
	Options         raftstore.NodeStoreOptions `json:"options"`
}

type emptyNodeServices struct {
	NodeIncarnation     uint64                             `json:"node_incarnation"`
	Listeners           ProcessListeners                   `json:"listeners"`
	TLS                 emptyNodeTLS                       `json:"tls"`
	AuthorizationPolicy string                             `json:"authorization_policy"`
	GatewaySeeds        []nodecontrol.BootstrapGatewaySeed `json:"bootstrap_gateway_seeds"`
	ReplicaControl      emptyNodeReplicaControl            `json:"replica_control"`
	SplitControl        emptyNodeSplitControl              `json:"split_control"`
}

type emptyNodeTLS struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}

type emptyNodeReplicaControl struct {
	ActionJournalPath      string             `json:"action_journal_path"`
	MaxActionRecords       int                `json:"max_action_records"`
	SourceDataRoot         string             `json:"source_data_root"`
	SourceJournalPath      string             `json:"source_journal_path"`
	MaxSourceRecords       int                `json:"max_source_records"`
	SourceRepositoryPath   string             `json:"source_repository_path"`
	MaxSourceArtifacts     int                `json:"max_source_artifacts"`
	MaxSourceConcurrent    int                `json:"max_source_concurrent"`
	MaxSourceArtifactBytes uint64             `json:"max_source_artifact_bytes"`
	MaxSourceDiskBytes     uint64             `json:"max_source_disk_bytes"`
	SourceChunkBytes       uint32             `json:"source_chunk_bytes"`
	Migration              emptyNodeMigration `json:"migration_budget"`
}

type emptyNodeMigration struct {
	MaxActive      int                `json:"max_active"`
	CPU            emptyNodeRateLimit `json:"cpu"`
	DiskRead       emptyNodeRateLimit `json:"disk_read"`
	DiskWrite      emptyNodeRateLimit `json:"disk_write"`
	NetworkSend    emptyNodeRateLimit `json:"network_send"`
	NetworkReceive emptyNodeRateLimit `json:"network_receive"`
}

type emptyNodeRateLimit struct {
	BytesPerSecond uint64 `json:"bytes_per_second"`
	BurstBytes     uint64 `json:"burst_bytes"`
}

type emptyNodeSplitControl struct {
	JournalPath   string           `json:"journal_path"`
	MaxRecords    int              `json:"max_records"`
	MaxFileBytes  int64            `json:"max_file_bytes"`
	Grants        []emptyNodeGrant `json:"grants"`
	MaxOperations int              `json:"max_operations"`
}

type emptyNodeGrant struct {
	NodeID  string `json:"node_id"`
	Actions uint16 `json:"actions"`
}

// EmptyNodePreparationManifest returns canonical input for the node
// preparation command. keyMaterialPath must point to the 32-byte local key
// file; it is intentionally separate from Root because provision-node creates
// Root atomically and refuses a pre-existing root.
func EmptyNodePreparationManifest(options EmptyNodeOptions, keyMaterialPath string) ([]byte, error) {
	if err := validateEmptyNodeOptions(options, keyMaterialPath); err != nil {
		return nil, err
	}
	grants := make([]emptyNodeGrant, len(options.GrantNodes))
	for index, node := range options.GrantNodes {
		grants[index] = emptyNodeGrant{NodeID: fmt.Sprintf("%x", node), Actions: ^uint16(0)}
	}
	root := filepath.Clean(options.Root)
	control := emptyNodeReplicaControl{
		ActionJournalPath: filepath.Join(root, "replica-actions"), MaxActionRecords: 4096,
		SourceDataRoot: root, SourceJournalPath: filepath.Join(root, "source-exports"), MaxSourceRecords: 4096,
		SourceRepositoryPath: filepath.Join(root, "source-artifacts"), MaxSourceArtifacts: 8, MaxSourceConcurrent: 2,
		MaxSourceArtifactBytes: 1 << 30, MaxSourceDiskBytes: 4 << 30, SourceChunkBytes: 1 << 20,
		Migration: emptyNodeMigration{MaxActive: 2,
			CPU:            emptyNodeRateLimit{BytesPerSecond: 64 << 20, BurstBytes: 4 << 20},
			DiskRead:       emptyNodeRateLimit{BytesPerSecond: 64 << 20, BurstBytes: 4 << 20},
			DiskWrite:      emptyNodeRateLimit{BytesPerSecond: 64 << 20, BurstBytes: 4 << 20},
			NetworkSend:    emptyNodeRateLimit{BytesPerSecond: 32 << 20, BurstBytes: 2 << 20},
			NetworkReceive: emptyNodeRateLimit{BytesPerSecond: 32 << 20, BurstBytes: 2 << 20}},
	}
	services := emptyNodeServices{
		NodeIncarnation: options.NodeIncarnation, Listeners: options.Listeners,
		TLS: emptyNodeTLS{Certificate: options.Credential.Certificate, Key: options.Credential.Key,
			Roots: options.Roots, IdentityOID: ProcessIdentityOID},
		AuthorizationPolicy: options.AuthorizationPolicy, GatewaySeeds: options.GatewaySeeds, ReplicaControl: control,
		SplitControl: emptyNodeSplitControl{JournalPath: filepath.Join(root, "split-control.journal"),
			MaxRecords: 4096, MaxFileBytes: 64 << 20, Grants: grants, MaxOperations: 8},
	}
	manifest := emptyNodePreparationManifest{Root: root,
		NodeLog: emptyNodeLogManifest{Format: 1, Path: filepath.Join(root, "node-log"), KeyID: options.Key.ID,
			WrappedKey: fmt.Sprintf("%x", options.Key.Wrapped), KeyMaterialPath: keyMaterialPath,
			Options: func() raftstore.NodeStoreOptions {
				result := options.NodeStore
				if result.MaxGroups == 0 {
					result.MaxGroups = 64
				}
				return result
			}()},
		Services: services, Groups: []struct{}{},
	}
	return vibejson.Marshal(&manifest)
}

// PrepareEmptyNode writes the exact canonical input and its local key source.
// It does not invoke a binary; callers then run the real prepare-node-rf3 CLI
// against PreparationPath and can start the resulting serve-rf3.vibejson.
func PrepareEmptyNode(options EmptyNodeOptions) (PreparedEmptyNode, error) {
	if err := validateEmptyNodeOptions(options, options.Root+".node-key"); err != nil {
		return PreparedEmptyNode{}, err
	}
	keyMaterialPath := options.Root + ".node-key"
	if err := writeFixtureOnce(keyMaterialPath, options.Key.Material[:], 0o600); err != nil {
		return PreparedEmptyNode{}, err
	}
	raw, err := EmptyNodePreparationManifest(options, keyMaterialPath)
	if err != nil {
		return PreparedEmptyNode{}, err
	}
	preparationPath := options.Root + ".prepare-node.vibejson"
	if err := writeFixtureOnce(preparationPath, raw, 0o600); err != nil {
		return PreparedEmptyNode{}, err
	}
	return PreparedEmptyNode{PreparationPath: preparationPath, KeyMaterialPath: keyMaterialPath}, nil
}

func validateEmptyNodeOptions(options EmptyNodeOptions, keyMaterialPath string) error {
	if options.Root == "" || !filepath.IsAbs(options.Root) || filepath.Clean(options.Root) != options.Root || options.Root == string(filepath.Separator) ||
		options.NodeIncarnation == 0 || options.Key.ID == "" || len(options.Key.Wrapped) == 0 || len(options.Key.Wrapped) > raftstore.MaxWrappedKeyBytes ||
		len(options.GrantNodes) == 0 || options.Credential.Certificate == "" || options.Credential.Key == "" || options.Roots == "" || options.AuthorizationPolicy == "" ||
		!filepath.IsAbs(keyMaterialPath) || filepath.Clean(keyMaterialPath) != keyMaterialPath || keyMaterialPath == options.Root {
		return errors.New("rf3 process fixture: invalid empty node options")
	}
	if len(options.GrantNodes) > 65536 {
		return errors.New("rf3 process fixture: too many empty node grants")
	}
	if len(options.GatewaySeeds) == 0 || len(options.GatewaySeeds) > nodecontrol.MaxBootstrapGatewaySeeds {
		return errors.New("rf3 process fixture: empty node requires bounded gateway seeds")
	}
	seenSeeds := make(map[rafttransport.NodeID]struct{}, len(options.GatewaySeeds))
	for _, seed := range options.GatewaySeeds {
		if !seed.Valid() {
			return errors.New("rf3 process fixture: invalid gateway seed")
		}
		if _, found := seenSeeds[seed.NodeID]; found {
			return errors.New("rf3 process fixture: duplicate gateway seed")
		}
		seenSeeds[seed.NodeID] = struct{}{}
	}
	seen := make(map[rafttransport.NodeID]struct{}, len(options.GrantNodes))
	for _, node := range options.GrantNodes {
		if node == (rafttransport.NodeID{}) {
			return errors.New("rf3 process fixture: empty node grant has zero identity")
		}
		if _, found := seen[node]; found {
			return errors.New("rf3 process fixture: duplicate empty node grant")
		}
		seen[node] = struct{}{}
	}
	for _, address := range []string{options.Listeners.Peer, options.Listeners.Native, options.Listeners.Snapshot, options.Listeners.Control} {
		if address == "" {
			return errors.New("rf3 process fixture: empty node listener is missing")
		}
	}
	return nil
}

func writeFixtureOnce(path string, raw []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(raw); err != nil {
		return err
	}
	return file.Sync()
}

func PrepareProcessMember(options ProcessMemberOptions) (PreparedProcessMember, error) {
	prepared, err := prepareProcessMember(options, options.Bootstrap, false)
	if err != nil {
		return PreparedProcessMember{}, err
	}
	return prepared, nil
}

func PrepareColdProcessTarget(
	options ProcessMemberOptions,
	sourceNode rafttransport.NodeID,
	sourceSnapshotAddress string,
	maxArtifactBytes uint64,
) (PreparedColdTarget, error) {
	if options.Target == nil || options.Identity.MemberID != options.Target.MemberID ||
		sourceNode == (rafttransport.NodeID{}) || sourceSnapshotAddress == "" {
		return PreparedColdTarget{}, errors.New("rf3 process fixture: invalid cold target")
	}
	if maxArtifactBytes == 0 {
		maxArtifactBytes = 1 << 30
	}
	static := options.Bootstrap
	if static.Snapshot == nil || static.Snapshot.Metadata == nil ||
		static.Snapshot.Metadata.ConfState == nil {
		return PreparedColdTarget{}, errors.New("rf3 process fixture: missing static bootstrap")
	}
	preparation := static
	preparation.Snapshot = proto.Clone(static.Snapshot).(*pb.Snapshot)
	preparation.Snapshot.Metadata.ConfState.Learners = []uint64{options.Target.MemberID}
	return prepareColdProcessTarget(options, static, preparation, sourceNode,
		sourceSnapshotAddress, maxArtifactBytes)
}

func prepareColdProcessTarget(
	options ProcessMemberOptions,
	static, preparation raftstore.Bootstrap,
	sourceNode rafttransport.NodeID,
	sourceSnapshotAddress string,
	maxArtifactBytes uint64,
) (PreparedColdTarget, error) {
	prepared, err := prepareProcessMember(options, preparation, true)
	if err != nil {
		return PreparedColdTarget{}, err
	}
	staticPath := filepath.Join(options.Root, "static-bootstrap.pb")
	staticRaw, err := proto.MarshalOptions{Deterministic: true}.Marshal(static.Snapshot)
	if err != nil {
		return PreparedColdTarget{}, err
	}
	if err = os.WriteFile(staticPath, staticRaw, 0o600); err != nil {
		return PreparedColdTarget{}, err
	}
	bootstrapPath := filepath.Join(options.Root, "bootstrap-rf3.json")
	document := []byte(fmt.Sprintf(
		`{"member_manifest":%q,"control_listener":%q,"source_node":"%x","source_snapshot_address":%q,"repository_path":%q,"cursor_path":%q,"journal_path":%q,"static_bootstrap_path":%q,"wal_wrapped_key":"%x","max_artifact_bytes":%d}`,
		prepared.ManifestPath, options.Listeners.Control, sourceNode, sourceSnapshotAddress,
		filepath.Join(options.Root, "target-artifacts"), filepath.Join(options.Root, "snapshot-cursor"),
		filepath.Join(options.Root, "bootstrap-journal"), staticPath, options.Key.Wrapped, maxArtifactBytes,
	))
	if err = os.WriteFile(bootstrapPath, document, 0o600); err != nil {
		return PreparedColdTarget{}, err
	}
	return PreparedColdTarget{PreparedProcessMember: prepared,
		BootstrapManifestPath: bootstrapPath, StaticBootstrapPath: staticPath}, nil
}

func prepareProcessMember(
	options ProcessMemberOptions, bootstrap raftstore.Bootstrap, coldSnapshot bool,
) (PreparedProcessMember, error) {
	if options.Root == "" || options.Table == "" || options.CreateTable == "" ||
		options.Key.ID == "" || bootstrap.Snapshot == nil || options.Listeners.Peer == "" ||
		options.Listeners.Native == "" || options.Listeners.Snapshot == "" ||
		options.Listeners.Control == "" || options.Credential.Certificate == "" ||
		options.Credential.Key == "" || options.Roots == "" || options.AuthorizationPolicy == "" {
		return PreparedProcessMember{}, errors.New("rf3 process fixture: invalid member options")
	}
	if err := os.MkdirAll(options.Root, 0o700); err != nil {
		return PreparedProcessMember{}, err
	}
	// The shipped server opens this provisioned namespace without creating it.
	// Keep that fail-closed startup contract even in external process fixtures.
	if err := PrepareSplitRuntime(options.Root, options.Bootstrap); err != nil {
		return PreparedProcessMember{}, err
	}
	prepare := PrepareMember
	if coldSnapshot {
		prepare = PrepareSnapshotTarget
	}
	prepared, err := prepare(MemberOptions{Root: options.Root, Table: options.Table,
		CreateTable: options.CreateTable, Identity: options.Identity, Key: options.Key,
		WAL: options.WAL, Bootstrap: bootstrap, Authority: options.Authority, Apply: options.Apply,
		SchemaStatements: options.SchemaStatements, GlobalIndexes: options.GlobalIndexes,
		SeedDocuments: options.SeedDocuments})
	if err != nil {
		return PreparedProcessMember{}, err
	}
	defer prepared.Close()
	basePath := filepath.Join(options.Root, "sql-identity.json")
	applyPath := filepath.Join(options.Root, "apply-identity.json")
	keyPath := filepath.Join(options.Root, "wal-key")
	if err = writeProcessJSON(basePath, prepared.Base); err != nil {
		return PreparedProcessMember{}, err
	}
	if err = writeProcessJSON(applyPath, prepared.ApplyIdentity); err != nil {
		return PreparedProcessMember{}, err
	}
	if err = os.WriteFile(keyPath, options.Key.Material[:], 0o600); err != nil {
		return PreparedProcessMember{}, err
	}
	if options.ControlRoot != "" {
		controlRoot, absErr := filepath.Abs(options.ControlRoot)
		if absErr != nil {
			return PreparedProcessMember{}, absErr
		}
		if err = os.MkdirAll(controlRoot, 0o700); err != nil {
			return PreparedProcessMember{}, err
		}
	}
	manifestPath := filepath.Join(options.Root, "serve-rf3.json")
	document := ProcessMemberManifest(options)
	if err = os.WriteFile(manifestPath, document, 0o600); err != nil {
		return PreparedProcessMember{}, err
	}
	// A cold target has no live apply machine until certified installation.
	// Do not try to read (or invent) its serving machine digest at reservation.
	var relationDigest [32]byte
	if !coldSnapshot {
		relationDigest, err = prepared.Apply.RangeSplitRelationManifestDigest()
		if err != nil {
			return PreparedProcessMember{}, err
		}
	}
	logicalDigest, err := sqldriver.ReplicatedRelationManifestDigest(prepared.Base)
	if err != nil {
		return PreparedProcessMember{}, err
	}
	if err = prepared.Close(); err != nil {
		return PreparedProcessMember{}, err
	}
	return PreparedProcessMember{ManifestPath: manifestPath,
		WALPath: prepared.WALPath, SQLPath: prepared.SQLPath,
		RelationManifestDigest: relationDigest, LogicalSchemaDigest: logicalDigest,
		SystemRecoveryJournalBytes: prepared.ApplyIdentity.Sidecars.SystemRecoveryJournalBytes}, nil
}

func writeProcessJSON(path string, value interface{ MarshalJSON() ([]byte, error) }) error {
	raw, err := value.MarshalJSON()
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}

// ProcessMemberManifest renders the shipped fixture grammar without opening
// a WAL or allocating strict sidecars, so host preflights exercise it too.
func ProcessMemberManifest(options ProcessMemberOptions) []byte {
	return processMemberManifest(options, &PreparedMember{
		WALPath: filepath.Join(options.Root, "member.wal"),
		SQLPath: filepath.Join(options.Root, "member.vdb"),
	}, filepath.Join(options.Root, "sql-identity.json"),
		filepath.Join(options.Root, "apply-identity.json"), filepath.Join(options.Root, "wal-key"))
}

func processMemberManifest(
	options ProcessMemberOptions,
	prepared *PreparedMember,
	basePath, applyPath, keyPath string,
) []byte {
	dataRoot, _ := filepath.Abs(filepath.Dir(prepared.SQLPath))
	controlRoot := dataRoot
	if options.ControlRoot != "" {
		controlRoot, _ = filepath.Abs(options.ControlRoot)
	}
	var ledgerStart, ledgerEnd, ledgerIdentity string
	if options.Apply.RequestLedgerCapacityBytes != 0 {
		ledgerStart = fmt.Sprintf("%x", options.Apply.RequestLedgerRangeStart)
		ledgerEnd = fmt.Sprintf("%x", options.Apply.RequestLedgerRangeEnd)
		ledgerIdentity = fmt.Sprintf("%x", options.Apply.RequestLedgerRangeIdentity)
	}
	document := []byte(fmt.Sprintf(`{"wal":{"path":%q,"key_id":%q,"key_material_path":%q,"max_file_bytes":%d,"max_record_bytes":%d,"max_records":%d,"max_entries":%d,"max_live_bytes":%d},"sql":{"path":%q,"identity_path":%q,"apply_identity_path":%q},"route":{"cluster_id":"%x","cluster_incarnation":"%x","topology_recovery_epoch":%d,"shard_incarnation":"%x","group_id":"%x","distribution":%q,"shard":%q,"allocation_generation":%d,"member_id":%d,"store_id":"%x","member_root":%q,"split_runtime_root":%q,"membership_grant_path":%q},"listeners":{"peer":%q,"native":%q,"snapshot":%q,"control":%q},"tls":{"certificate":%q,"key":%q,"roots":%q,"identity_oid":%q%s},"authorization_policy":%q,"replica_control":{"action_journal_path":%q,"max_action_records":4096,"source_data_root":%q,"source_journal_path":%q,"max_source_records":4096,"source_repository_path":%q,"max_source_artifacts":8,"max_source_concurrent":2,"max_source_artifact_bytes":1073741824,"max_source_disk_bytes":4294967296,"source_chunk_bytes":1048576,"migration_budget":{"max_active":2,"cpu":{"bytes_per_second":67108864,"burst_bytes":4194304},"disk_read":{"bytes_per_second":67108864,"burst_bytes":4194304},"disk_write":{"bytes_per_second":67108864,"burst_bytes":4194304},"network_send":{"bytes_per_second":33554432,"burst_bytes":2097152},"network_receive":{"bytes_per_second":33554432,"burst_bytes":2097152}}},"split_control":{"journal_path":%q,"max_records":4096,"max_file_bytes":67108864,"grants":[{"node_id":"%x","actions":65535},{"node_id":"%x","actions":65535},{"node_id":"%x","actions":65535}],"child_registry":{"root":%q,"max_operations":8,"stage_checkpoint_bytes":33554432,"table":%q,"create_table":%q,"wal":{"key_id":%q,"key_material_path":%q,"max_file_bytes":%d,"max_record_bytes":%d,"max_records":%d,"max_entries":%d,"max_live_bytes":%d},"apply":{"max_sessions":%d,"retry_window":%d,"max_collections":%d,"max_documents":%d,"max_bytes":%d,"request_ledger_capacity_bytes":%d,"request_ledger_cleanup_reserve_bytes":%d,"request_ledger_range_start":%q,"request_ledger_range_end":%q,"request_ledger_range_identity":%q,"format":%d,"shard_key":%q,"tuple_version":%d,"mapper_version":%d},"static_bootstrap_path":%q,"replica_set_version":1,"members":[{"member_id":1,"node_id":"%x","peer_address":%q},{"member_id":2,"node_id":"%x","peer_address":%q},{"member_id":3,"node_id":"%x","peer_address":%q}]}},"members":[{"member_id":1,"node_id":"%x","peer_address":%q},{"member_id":2,"node_id":"%x","peer_address":%q},{"member_id":3,"node_id":"%x","peer_address":%q}]}`,
		prepared.WALPath, options.Key.ID, keyPath, options.WAL.MaxFileBytes,
		options.WAL.MaxRecordBytes, options.WAL.MaxRecords, options.WAL.MaxEntries,
		options.WAL.MaxLiveBytes, prepared.SQLPath, basePath, applyPath,
		options.Identity.ClusterID, options.Identity.ClusterIncarnation,
		options.Bootstrap.TopologyRecoveryEpoch, options.Identity.ShardIncarnation,
		options.Identity.GroupID, options.Identity.Distribution, options.Identity.Shard,
		options.Identity.AllocationGeneration, options.Identity.MemberID, options.Identity.StoreID,
		dataRoot, filepath.Join(dataRoot, "split-runtime"), filepath.Join(dataRoot, "membership-grant"),
		options.Listeners.Peer, options.Listeners.Native, options.Listeners.Snapshot,
		options.Listeners.Control, options.Credential.Certificate, options.Credential.Key,
		options.Roots, ProcessIdentityOID, credentialPeerKeysJSON(options.Credential), options.AuthorizationPolicy,
		filepath.Join(controlRoot, "replica-actions"), controlRoot,
		filepath.Join(controlRoot, "source-exports"), filepath.Join(controlRoot, "source-artifacts"),
		filepath.Join(controlRoot, "split-control.journal"), options.Nodes[0], options.Nodes[1],
		options.Nodes[2], filepath.Join(dataRoot, "split-children"), options.Table, options.CreateTable,
		options.Key.ID, keyPath,
		options.WAL.MaxFileBytes, options.WAL.MaxRecordBytes, options.WAL.MaxRecords,
		options.WAL.MaxEntries, options.WAL.MaxLiveBytes,
		options.Apply.MaxSessions, options.Apply.RetryWindow, options.Apply.TxnLimits.MaxCollections,
		options.Apply.TxnLimits.MaxDocuments, options.Apply.TxnLimits.MaxBytes,
		options.Apply.RequestLedgerCapacityBytes, options.Apply.RequestLedgerCleanupReserveBytes,
		ledgerStart, ledgerEnd, ledgerIdentity,
		options.Apply.Placement.Format, options.Apply.Placement.ShardKey,
		options.Apply.Placement.TupleVersion, options.Apply.Placement.MapperVersion,
		filepath.Join(dataRoot, "split-children", "static-bootstrap.pb"),
		options.Nodes[0], options.PeerAddresses[0], options.Nodes[1], options.PeerAddresses[1],
		options.Nodes[2], options.PeerAddresses[2], options.Nodes[0], options.PeerAddresses[0],
		options.Nodes[1], options.PeerAddresses[1], options.Nodes[2], options.PeerAddresses[2]))
	if len(options.SchemaStatements) != 0 || len(options.GlobalIndexes) != 0 {
		fields, err := vibejson.Marshal(&struct {
			SchemaStatements []string                                  `json:"schema_statements,omitempty"`
			GlobalIndexes    []sqldriver.ReplicatedGlobalIndexRelation `json:"global_indexes,omitempty"`
		}{options.SchemaStatements, options.GlobalIndexes})
		if err != nil {
			panic(err)
		}
		// This marker occurs only in the child template (the top-level WAL
		// begins with path), and keeps the optional fields in canonical order.
		position := bytes.Index(document, []byte(`,"wal":{"key_id":`))
		if position < 0 {
			panic("missing split child WAL template")
		}
		withSchema := make([]byte, 0, len(document)+len(fields))
		withSchema = append(withSchema, document[:position]...)
		withSchema = append(withSchema, ',')
		withSchema = append(withSchema, fields[1:len(fields)-1]...)
		document = append(withSchema, document[position:]...)
	}
	if options.Target == nil {
		return document
	}
	document = document[:len(document)-1]
	return fmt.Appendf(document, `,"enrolled_target":{"member_id":%d,"node_id":"%x","store_id":"%x","node_incarnation":%d,"peer_address":%q,"native_address":%q,"snapshot_address":%q,"control_address":%q}}`,
		options.Target.MemberID, options.Target.NodeID, options.Target.StoreID,
		options.Target.NodeIncarnation, options.Target.Listeners.Peer, options.Target.Listeners.Native,
		options.Target.Listeners.Snapshot, options.Target.Listeners.Control)
}

func credentialPeerKeysJSON(credential Credential) []byte {
	if len(credential.PeerKeys) == 0 {
		return nil
	}
	encoded, err := vibejson.Marshal(&credential.PeerKeys)
	if err != nil {
		panic(err)
	}
	return append([]byte(`,"peer_keys":`), encoded...)
}
