package rf3testfixture

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const ProcessIdentityOID = "1.3.6.1.4.1.32473.1.1"

type ProcessListeners struct {
	Peer, Native, Snapshot, Control string
}

type ProcessTarget struct {
	MemberID        uint64
	NodeID          rafttransport.NodeID
	StoreID         [16]byte
	NodeIncarnation uint64
	Listeners       ProcessListeners
}

type ProcessMemberOptions struct {
	Root, Table, CreateTable   string
	Identity                   raftstore.Identity
	Key                        raftstore.Key
	WAL                        raftstore.Options
	Bootstrap                  raftstore.Bootstrap
	Authority                  sqldriver.ReplicatedAuthorityProfile
	Apply                      sqldriver.ReplicatedApplyOptions
	Listeners                  ProcessListeners
	Credential                 Credential
	Roots, AuthorizationPolicy string
	Nodes                      [3]rafttransport.NodeID
	PeerAddresses              [3]string
	Target                     *ProcessTarget
	SeedDocuments              [][]byte
}

type PreparedProcessMember struct {
	ManifestPath, WALPath, SQLPath string
}

type PreparedColdTarget struct {
	PreparedProcessMember
	BootstrapManifestPath, StaticBootstrapPath string
}

func PrepareProcessMember(options ProcessMemberOptions) (PreparedProcessMember, error) {
	prepared, err := prepareProcessMember(options, options.Bootstrap)
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
	prepared, err := prepareProcessMember(options, preparation)
	if err != nil {
		return PreparedColdTarget{}, err
	}
	if err = os.Remove(prepared.WALPath); err != nil {
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
		`{"member_manifest":%q,"control_listener":%q,"source_node":"%x","source_snapshot_address":%q,"repository_path":%q,"cursor_path":%q,"journal_path":%q,"static_bootstrap_path":%q,"max_artifact_bytes":%d}`,
		prepared.ManifestPath, options.Listeners.Control, sourceNode, sourceSnapshotAddress,
		filepath.Join(options.Root, "target-artifacts"), filepath.Join(options.Root, "snapshot-cursor"),
		filepath.Join(options.Root, "bootstrap-journal"), staticPath, maxArtifactBytes,
	))
	if err = os.WriteFile(bootstrapPath, document, 0o600); err != nil {
		return PreparedColdTarget{}, err
	}
	return PreparedColdTarget{PreparedProcessMember: prepared,
		BootstrapManifestPath: bootstrapPath, StaticBootstrapPath: staticPath}, nil
}

func prepareProcessMember(
	options ProcessMemberOptions, bootstrap raftstore.Bootstrap,
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
	prepared, err := PrepareMember(MemberOptions{Root: options.Root, Table: options.Table,
		CreateTable: options.CreateTable, Identity: options.Identity, Key: options.Key,
		WAL: options.WAL, Bootstrap: bootstrap, Authority: options.Authority, Apply: options.Apply,
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
	manifestPath := filepath.Join(options.Root, "serve-rf3.json")
	document := processMemberManifest(options, prepared, basePath, applyPath, keyPath)
	if err = os.WriteFile(manifestPath, document, 0o600); err != nil {
		return PreparedProcessMember{}, err
	}
	if err = prepared.Close(); err != nil {
		return PreparedProcessMember{}, err
	}
	return PreparedProcessMember{ManifestPath: manifestPath,
		WALPath: prepared.WALPath, SQLPath: prepared.SQLPath}, nil
}

func writeProcessJSON(path string, value interface{ MarshalJSON() ([]byte, error) }) error {
	raw, err := value.MarshalJSON()
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}

func processMemberManifest(
	options ProcessMemberOptions,
	prepared *PreparedMember,
	basePath, applyPath, keyPath string,
) []byte {
	dataRoot, _ := filepath.Abs(filepath.Dir(prepared.SQLPath))
	document := []byte(fmt.Sprintf(`{"wal":{"path":%q,"key_id":%q,"key_material_path":%q,"max_file_bytes":%d,"max_record_bytes":%d,"max_records":%d,"max_entries":%d,"max_live_bytes":%d},"sql":{"path":%q,"identity_path":%q,"apply_identity_path":%q},"route":{"cluster_id":"%x","cluster_incarnation":"%x","topology_recovery_epoch":%d,"shard_incarnation":"%x","group_id":"%x","distribution":%q,"shard":%q,"allocation_generation":%d,"member_id":%d,"store_id":"%x","member_root":%q,"split_runtime_root":%q,"membership_grant_path":%q},"listeners":{"peer":%q,"native":%q,"snapshot":%q,"control":%q},"tls":{"certificate":%q,"key":%q,"roots":%q,"identity_oid":%q},"authorization_policy":%q,"replica_control":{"action_journal_path":%q,"max_action_records":4096,"source_data_root":%q,"source_journal_path":%q,"max_source_records":4096,"source_repository_path":%q,"max_source_artifacts":8,"max_source_concurrent":2,"max_source_artifact_bytes":1073741824,"max_source_disk_bytes":4294967296,"source_chunk_bytes":1048576},"split_control":{"journal_path":%q,"max_records":4096,"max_file_bytes":67108864,"grants":[{"node_id":"%x","actions":65535},{"node_id":"%x","actions":65535},{"node_id":"%x","actions":65535}],"child_registry":{"root":%q,"max_operations":8,"stage_checkpoint_bytes":33554432,"table":"docs","create_table":"CREATE TABLE docs (PRIMARY KEY (id))","wal":{"key_id":%q,"key_material_path":%q,"max_file_bytes":%d,"max_record_bytes":%d,"max_records":%d,"max_entries":%d,"max_live_bytes":%d},"apply":{"max_sessions":32,"retry_window":8,"max_collections":16,"max_documents":1024,"max_bytes":402653184,"format":0,"shard_key":"id","tuple_version":1,"mapper_version":1},"static_bootstrap_path":%q,"replica_set_version":1,"members":[{"member_id":1,"node_id":"%x","peer_address":%q},{"member_id":2,"node_id":"%x","peer_address":%q},{"member_id":3,"node_id":"%x","peer_address":%q}]}},"members":[{"member_id":1,"node_id":"%x","peer_address":%q},{"member_id":2,"node_id":"%x","peer_address":%q},{"member_id":3,"node_id":"%x","peer_address":%q}]}`,
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
		options.Roots, ProcessIdentityOID, options.AuthorizationPolicy,
		filepath.Join(dataRoot, "replica-actions"), dataRoot,
		filepath.Join(dataRoot, "source-exports"), filepath.Join(dataRoot, "source-artifacts"),
		filepath.Join(dataRoot, "split-control.journal"), options.Nodes[0], options.Nodes[1],
		options.Nodes[2], filepath.Join(dataRoot, "split-children"), options.Key.ID, keyPath,
		options.WAL.MaxFileBytes, options.WAL.MaxRecordBytes, options.WAL.MaxRecords,
		options.WAL.MaxEntries, options.WAL.MaxLiveBytes,
		filepath.Join(dataRoot, "split-children", "static-bootstrap.pb"),
		options.Nodes[0], options.PeerAddresses[0], options.Nodes[1], options.PeerAddresses[1],
		options.Nodes[2], options.PeerAddresses[2], options.Nodes[0], options.PeerAddresses[0],
		options.Nodes[1], options.PeerAddresses[1], options.Nodes[2], options.PeerAddresses[2]))
	if options.Target == nil {
		return document
	}
	document = document[:len(document)-1]
	return fmt.Appendf(document, `,"enrolled_target":{"member_id":%d,"node_id":"%x","store_id":"%x","node_incarnation":%d,"peer_address":%q,"native_address":%q,"snapshot_address":%q,"control_address":%q}}`,
		options.Target.MemberID, options.Target.NodeID, options.Target.StoreID,
		options.Target.NodeIncarnation, options.Target.Listeners.Peer, options.Target.Listeners.Native,
		options.Target.Listeners.Snapshot, options.Target.Listeners.Control)
}
