package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	protocol "github.com/thesyncim/vibedb/shardcontrol"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

const maxPrepareRF3ManifestBytes = 64 << 10

var errPrepareRF3 = errors.New("vibedb-shard: invalid RF3 preparation manifest")

// prepareRF3Manifest is deliberately a single-member manifest. An operator
// writes three canonical documents for RF3. The local development command may
// instead write one explicitly marked RF1 document; it cannot enroll or replace
// replicas. Artifact names and placement are fixed by the command so an unsafe
// cross-directory publication cannot be requested.
type prepareRF3Manifest struct {
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
	Authority             prepareRF3Authority    `json:"authority"`
	WAL                   prepareRF3WAL          `json:"wal"`
	Apply                 prepareRF3Apply        `json:"apply"`
	Listeners             rf3ManifestListeners   `json:"listeners"`
	TLS                   rf3ManifestTLS         `json:"tls"`
	AuthorizationPolicy   string                 `json:"authorization_policy"`
	SplitControl          prepareRF3SplitControl `json:"split_control"`
	DevelopmentOnly       bool                   `json:"development_only,omitempty"`
	Members               []prepareRF3Member     `json:"members"`
}

type prepareRF3SplitControl struct {
	MaxRecords   int                     `json:"max_records"`
	MaxFileBytes int64                   `json:"max_file_bytes"`
	Grants       []prepareRF3ActionGrant `json:"grants"`
}

type prepareRF3ActionGrant struct {
	NodeID  string `json:"node_id"`
	Actions uint16 `json:"actions"`
}

type prepareRF3Authority struct {
	ActivePolicyGeneration uint64 `json:"active_policy_generation"`
	ProtectionEpoch        uint64 `json:"protection_epoch"`
	OwnershipEpoch         uint64 `json:"ownership_epoch"`
	SchemaGeneration       uint64 `json:"schema_generation"`
	RoutingVersion         uint64 `json:"routing_version"`
	RouteGeneration        uint64 `json:"route_generation"`
}

type prepareRF3WAL struct {
	KeyID           string `json:"key_id"`
	KeyMaterialPath string `json:"key_material_path"`
	WrappedKey      string `json:"wrapped_key"`
	MaxFileBytes    int64  `json:"max_file_bytes"`
	MaxRecordBytes  int    `json:"max_record_bytes"`
	MaxRecords      uint64 `json:"max_records"`
	MaxEntries      uint64 `json:"max_entries"`
	MaxLiveBytes    int64  `json:"max_live_bytes"`
}

type prepareRF3Apply struct {
	MaxSessions    uint64 `json:"max_sessions"`
	RetryWindow    uint16 `json:"retry_window"`
	MaxCollections int    `json:"max_collections"`
	MaxDocuments   int    `json:"max_documents"`
	MaxBytes       int    `json:"max_bytes"`
	ShardKey       string `json:"shard_key"`
}

type prepareRF3Member struct {
	MemberID    uint64 `json:"member_id"`
	NodeID      string `json:"node_id"`
	PeerAddress string `json:"peer_address"`
}

type persistedRF3Manifest struct {
	WAL                 persistedRF3WAL            `json:"wal"`
	SQL                 persistedRF3SQL            `json:"sql"`
	Route               persistedRF3GroupRoute     `json:"route"`
	Listeners           rf3ManifestListeners       `json:"listeners"`
	TLS                 rf3ManifestTLS             `json:"tls"`
	AuthorizationPolicy string                     `json:"authorization_policy"`
	ReplicaControl      persistedRF3ReplicaControl `json:"replica_control"`
	SplitControl        persistedRF3SplitControl   `json:"split_control"`
	DevelopmentOnly     bool                       `json:"development_only,omitempty"`
	Members             []persistedRF3Member       `json:"members"`
}

type persistedRF3SplitControl struct {
	JournalPath  string                    `json:"journal_path"`
	MaxRecords   int                       `json:"max_records"`
	MaxFileBytes int64                     `json:"max_file_bytes"`
	Grants       []persistedRF3ActionGrant `json:"grants"`
}

type persistedRF3ActionGrant struct {
	NodeID  string `json:"node_id"`
	Actions uint16 `json:"actions"`
}

type persistedRF3GroupRoute struct {
	ClusterID             string `json:"cluster_id"`
	ClusterIncarnation    string `json:"cluster_incarnation"`
	TopologyRecoveryEpoch uint64 `json:"topology_recovery_epoch"`
	ShardIncarnation      string `json:"shard_incarnation"`
	GroupID               string `json:"group_id"`
	Distribution          string `json:"distribution"`
	Shard                 string `json:"shard"`
	AllocationGeneration  uint64 `json:"allocation_generation"`
	MemberID              uint64 `json:"member_id"`
	StoreID               string `json:"store_id"`
	MemberRoot            string `json:"member_root"`
	SplitRuntimeRoot      string `json:"split_runtime_root"`
	MembershipGrantPath   string `json:"membership_grant_path"`
}

type persistedRF3WAL struct {
	Path            string `json:"path"`
	KeyID           string `json:"key_id"`
	KeyMaterialPath string `json:"key_material_path"`
	MaxFileBytes    int64  `json:"max_file_bytes"`
	MaxRecordBytes  int    `json:"max_record_bytes"`
	MaxRecords      uint64 `json:"max_records"`
	MaxEntries      uint64 `json:"max_entries"`
	MaxLiveBytes    int64  `json:"max_live_bytes"`
}
type persistedRF3SQL struct {
	Path              string `json:"path"`
	IdentityPath      string `json:"identity_path"`
	ApplyIdentityPath string `json:"apply_identity_path"`
}
type persistedRF3ReplicaControl struct {
	ActionJournalPath      string `json:"action_journal_path"`
	MaxActionRecords       int    `json:"max_action_records"`
	SourceDataRoot         string `json:"source_data_root"`
	SourceJournalPath      string `json:"source_journal_path"`
	MaxSourceRecords       int    `json:"max_source_records"`
	SourceRepositoryPath   string `json:"source_repository_path"`
	MaxSourceArtifacts     int    `json:"max_source_artifacts"`
	MaxSourceConcurrent    int    `json:"max_source_concurrent"`
	MaxSourceArtifactBytes uint64 `json:"max_source_artifact_bytes"`
	MaxSourceDiskBytes     uint64 `json:"max_source_disk_bytes"`
	SourceChunkBytes       uint32 `json:"source_chunk_bytes"`
}
type persistedRF3Member struct {
	MemberID    uint64 `json:"member_id"`
	NodeID      string `json:"node_id"`
	PeerAddress string `json:"peer_address"`
}

func runPrepareRF3(args []string) int {
	fs := flag.NewFlagSet("prepare-rf3", flag.ContinueOnError)
	path := fs.String("manifest", "", "canonical vibejson RF3 member preparation manifest")
	if err := fs.Parse(args); err != nil || *path == "" || fs.NArg() != 0 {
		return 2
	}
	manifest, err := loadPrepareRF3Manifest(*path)
	if err == nil {
		err = provisionRF3Member(manifest)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error prepare RF3 member: %v\n", err)
		return 1
	}
	topology := "RF3"
	if manifest.DevelopmentOnly {
		topology = "RF1 development-only/no-HA"
	}
	fmt.Fprintf(os.Stderr, "vibedb-shard %s member prepared root=%q manifest=%q\n", topology, manifest.Root, filepath.Join(manifest.Root, "serve-rf3.vibejson"))
	return 0
}

func loadPrepareRF3Manifest(path string) (prepareRF3Manifest, error) {
	raw, err := readPrepareRF3File(path, maxPrepareRF3ManifestBytes)
	if err != nil {
		return prepareRF3Manifest{}, errors.Join(errPrepareRF3, err)
	}
	var result prepareRF3Manifest
	if err := vibejson.Unmarshal(raw, &result); err != nil {
		return result, errors.Join(errPrepareRF3, err)
	}
	canonical, err := vibejson.Marshal(&result)
	if err != nil || !bytes.Equal(raw, canonical) {
		return result, errPrepareRF3
	}
	return result, nil
}

func provisionRF3Member(input prepareRF3Manifest) (resultErr error) {
	identity, authority, nodes, options, apply, keyMaterial, err := validatePrepareRF3(input)
	if err != nil {
		return err
	}
	defer clear(keyMaterial)
	final := input.Root
	paths := map[string]string{"wal": filepath.Join(final, "member.wal"), "sql": filepath.Join(final, "member.vdb"), "identity": filepath.Join(final, "sql-identity.vibejson"), "apply": filepath.Join(final, "apply-identity.vibejson"), "key": filepath.Join(final, "wal-key")}
	manifest := buildPreparedRF3Manifest(input, nodes, paths)
	manifestRaw, err := vibejson.Marshal(&manifest)
	if err != nil {
		return err
	}
	if _, err := parseRF3Manifest(manifestRaw); err != nil {
		return errors.Join(errPrepareRF3, err)
	}
	if _, statErr := os.Lstat(final); statErr == nil {
		return verifyPreparedRF3Member(final, manifestRaw)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return errors.Join(errPrepareRF3, statErr)
	}
	parent, base := filepath.Dir(input.Root), filepath.Base(input.Root)
	stage, err := os.MkdirTemp(parent, "."+base+".prepare-")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			resultErr = errors.Join(resultErr, os.RemoveAll(stage))
		}
	}()
	if err := os.Chmod(stage, 0o700); err != nil {
		return err
	}
	for _, directory := range [...]string{
		"replica-actions", "source-exports", "source-artifacts", "split-runtime",
	} {
		if err := os.Mkdir(filepath.Join(stage, directory), 0o700); err != nil {
			return err
		}
	}
	if err := writePrepareRF3File(filepath.Join(stage, "split-control.journal"), nil, 0o600); err != nil {
		return err
	}
	walPath, sqlPath := filepath.Join(stage, "member.wal"), filepath.Join(stage, "member.vdb")
	key := raftstore.Key{ID: input.WAL.KeyID, Wrapped: []byte(input.WAL.WrappedKey)}
	copy(key.Material[:], keyMaterial)
	index, term := uint64(1), uint64(1)
	voters := make([]uint64, len(input.Members))
	for index := range input.Members {
		voters[index] = input.Members[index].MemberID
	}
	wal, err := raftstore.Create(walPath, identity, key, raftstore.Bootstrap{TopologyRecoveryEpoch: input.TopologyRecoveryEpoch, Snapshot: &pb.Snapshot{Data: []byte("vibedb-rf3-bootstrap"), Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: voters}}}}, options)
	clear(key.Material[:])
	clear(key.Wrapped)
	if err != nil {
		return err
	}
	database, err := sqldriver.InitializeShardStore(sqlPath, sqldriver.ShardStoreBinding{Distribution: distribution.DistributionName(input.Distribution), Shard: distribution.ShardID(input.Shard), AllocationGeneration: distribution.ShardAllocationGeneration(input.AllocationGeneration)})
	if err != nil {
		return errors.Join(err, wal.Close())
	}
	closeBase := func(cause error) error { return errors.Join(cause, database.Close(), wal.Close()) }
	session, err := database.NewSession(context.Background())
	if err != nil {
		return closeBase(err)
	}
	statement, err := session.Prepare(context.Background(), input.CreateTable)
	if err == nil {
		_, err = statement.Exec(context.Background(), nil)
	}
	if statement != nil {
		err = errors.Join(err, statement.Close())
	}
	err = errors.Join(err, session.Close())
	if err != nil {
		return closeBase(err)
	}
	baseIdentity, err := raftmember.BindPreparedSQL(wal, database, authority, input.Table)
	if err != nil {
		return closeBase(err)
	}
	applyHandle, applyIdentity, err := raftmember.OpenPreparedApply(wal, database, authority, baseIdentity, apply)
	if err != nil {
		return closeBase(err)
	}
	snapshot, err := wal.Snapshot()
	if err == nil {
		_, err = applyHandle.InstallSnapshot(snapshot)
	}
	if err != nil {
		return errors.Join(err, applyHandle.Close(), database.Close(), wal.Close())
	}
	if err := errors.Join(applyHandle.Close(), database.Close(), wal.Close()); err != nil {
		return err
	}
	baseRaw, err := baseIdentity.MarshalJSON()
	if err != nil {
		return err
	}
	applyRaw, err := applyIdentity.MarshalJSON()
	if err != nil {
		return err
	}
	if err := writePrepareRF3File(filepath.Join(stage, "sql-identity.vibejson"), baseRaw, 0o600); err != nil {
		return err
	}
	if err := writePrepareRF3File(filepath.Join(stage, "apply-identity.vibejson"), applyRaw, 0o600); err != nil {
		return err
	}
	if err := writePrepareRF3File(filepath.Join(stage, "wal-key"), keyMaterial, 0o600); err != nil {
		return err
	}
	if err := writePrepareRF3File(filepath.Join(stage, "serve-rf3.vibejson"), manifestRaw, 0o600); err != nil {
		return err
	}
	if err := syncPrepareRF3Directory(stage); err != nil {
		return err
	}
	if err := os.Rename(stage, final); err != nil {
		// Another identical preparer may have won publication after our initial
		// existence check. Accept only a byte-identical, completely retained
		// member and sync the parent ourselves before reporting success.
		if verifyErr := verifyPreparedRF3Member(final, manifestRaw); verifyErr != nil {
			return errors.Join(errPrepareRF3, err, verifyErr)
		}
		return syncPrepareRF3Directory(parent)
	}
	published = true
	return syncPrepareRF3Directory(parent)
}

func verifyPreparedRF3Member(root string, manifestRaw []byte) error {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(errPrepareRF3, err)
	}
	retained, err := readPrepareRF3File(filepath.Join(root, "serve-rf3.vibejson"), maxRF3ManifestBytes)
	if err != nil || !bytes.Equal(retained, manifestRaw) {
		return errors.Join(errPrepareRF3, err)
	}
	for _, name := range [...]string{
		"member.wal", "member.vdb", "sql-identity.vibejson", "apply-identity.vibejson",
		"wal-key", "split-control.journal",
	} {
		artifact, statErr := os.Lstat(filepath.Join(root, name))
		if statErr != nil || !artifact.Mode().IsRegular() || artifact.Mode()&os.ModeSymlink != 0 {
			return errors.Join(errPrepareRF3, statErr)
		}
	}
	for _, name := range [...]string{
		"replica-actions", "source-exports", "source-artifacts", "split-runtime",
	} {
		directory, statErr := os.Lstat(filepath.Join(root, name))
		if statErr != nil || !directory.IsDir() || directory.Mode()&os.ModeSymlink != 0 {
			return errors.Join(errPrepareRF3, statErr)
		}
	}
	if _, err = loadRF3Manifest(filepath.Join(root, "serve-rf3.vibejson")); err != nil {
		return errors.Join(errPrepareRF3, err)
	}
	return nil
}

func validatePrepareRF3(input prepareRF3Manifest) (raftstore.Identity, sqldriver.ReplicatedAuthorityProfile, [3]rafttransport.NodeID, raftstore.Options, sqldriver.ReplicatedApplyOptions, []byte, error) {
	bad := func() (raftstore.Identity, sqldriver.ReplicatedAuthorityProfile, [3]rafttransport.NodeID, raftstore.Options, sqldriver.ReplicatedApplyOptions, []byte, error) {
		return raftstore.Identity{}, sqldriver.ReplicatedAuthorityProfile{}, [3]rafttransport.NodeID{}, raftstore.Options{}, sqldriver.ReplicatedApplyOptions{}, nil, errPrepareRF3
	}
	if !filepath.IsAbs(input.Root) || filepath.Clean(input.Root) != input.Root || input.Root == string(filepath.Separator) || input.Distribution == "" || input.Shard == "" || input.MemberID == 0 || input.AllocationGeneration == 0 || input.TopologyRecoveryEpoch == 0 || input.Table == "" || input.CreateTable == "" || input.Apply.ShardKey == "" {
		return bad()
	}
	for _, path := range [...]string{input.WAL.KeyMaterialPath, input.TLS.Certificate, input.TLS.Key, input.TLS.Roots, input.AuthorizationPolicy} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return bad()
		}
	}
	var id raftstore.Identity
	if !decodePrepareRF3ID(input.ClusterID, id.ClusterID[:]) || !decodePrepareRF3ID(input.ClusterIncarnation, id.ClusterIncarnation[:]) || !decodePrepareRF3ID(input.ShardIncarnation, id.ShardIncarnation[:]) || !decodePrepareRF3ID(input.GroupID, id.GroupID[:]) || !decodePrepareRF3ID(input.StoreID, id.StoreID[:]) {
		return bad()
	}
	id.Distribution, id.Shard, id.AllocationGeneration, id.MemberID = input.Distribution, input.Shard, input.AllocationGeneration, input.MemberID
	a := sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: input.Authority.ActivePolicyGeneration, ProtectionEpoch: input.Authority.ProtectionEpoch, OwnershipEpoch: input.Authority.OwnershipEpoch, SchemaGeneration: input.Authority.SchemaGeneration, RoutingVersion: input.Authority.RoutingVersion, RouteGeneration: input.Authority.RouteGeneration}
	if a.ActivePolicyGeneration == 0 || a.ProtectionEpoch == 0 || a.OwnershipEpoch == 0 || a.SchemaGeneration == 0 || a.RoutingVersion == 0 || a.RouteGeneration == 0 {
		return bad()
	}
	if len(input.Members) != rf3ManifestMembers &&
		(!input.DevelopmentOnly || len(input.Members) != 1) {
		return bad()
	}
	if input.DevelopmentOnly && len(input.Members) != 1 {
		return bad()
	}
	var nodes [3]rafttransport.NodeID
	found := false
	for i, member := range input.Members {
		if member.MemberID == 0 || member.PeerAddress == "" || !decodePrepareRF3ID(member.NodeID, nodes[i][:]) || i > 0 && member.MemberID <= input.Members[i-1].MemberID {
			return bad()
		}
		if member.MemberID == input.MemberID {
			found = true
		}
	}
	if !found {
		return bad()
	}
	if input.SplitControl.MaxRecords <= 0 || input.SplitControl.MaxRecords > 1<<20 ||
		input.SplitControl.MaxFileBytes < int64(protocol.MaxPayloadBytes) ||
		input.SplitControl.MaxFileBytes > 1<<40 || len(input.SplitControl.Grants) == 0 ||
		len(input.SplitControl.Grants) > protocol.AbsoluteMaxGrants {
		return bad()
	}
	grants := make([]protocol.ActionGrant, len(input.SplitControl.Grants))
	for index, grant := range input.SplitControl.Grants {
		if !decodePrepareRF3ID(grant.NodeID, grants[index].Node[:]) || grant.Actions == 0 {
			return bad()
		}
		grants[index].Actions = grant.Actions
	}
	if _, err := protocol.NewAuthorizer(grants); err != nil {
		return bad()
	}
	for index := range input.Members {
		for prior := 0; prior < index; prior++ {
			if nodes[index] == nodes[prior] {
				return bad()
			}
		}
	}
	material, err := readPrepareRF3File(input.WAL.KeyMaterialPath, 32)
	if err != nil || len(material) != 32 || input.WAL.KeyID == "" || len(input.WAL.WrappedKey) == 0 {
		clear(material)
		return bad()
	}
	profile, err := servicetls.LoadProfile(input.TLS.Certificate, input.TLS.Key, input.TLS.Roots, input.TLS.IdentityOID, time.Now)
	if err != nil {
		clear(material)
		return bad()
	}
	wantDomain := rafttransport.TrustDomain{ClusterID: id.ClusterID, ClusterIncarnation: id.ClusterIncarnation}
	localNode := profile.LocalIdentity().Node
	memberNode := rafttransport.NodeID{}
	for i, m := range input.Members {
		if m.MemberID == input.MemberID {
			memberNode = nodes[i]
		}
	}
	if profile.LocalIdentity().TrustDomain != wantDomain || localNode != memberNode {
		clear(material)
		return bad()
	}
	policy, err := serviceauthz.LoadFile(input.AuthorizationPolicy)
	if err != nil || policy.Generation() != a.ActivePolicyGeneration {
		clear(material)
		return bad()
	}
	o := raftstore.Options{MaxFileBytes: input.WAL.MaxFileBytes, MaxRecordBytes: input.WAL.MaxRecordBytes, MaxRecords: input.WAL.MaxRecords, MaxEntries: input.WAL.MaxEntries, MaxLiveBytes: input.WAL.MaxLiveBytes}
	ap := sqldriver.ReplicatedApplyOptions{MaxSessions: input.Apply.MaxSessions, RetryWindow: input.Apply.RetryWindow, TxnLimits: durable.TxnLimits{MaxCollections: input.Apply.MaxCollections, MaxDocuments: input.Apply.MaxDocuments, MaxBytes: int64(input.Apply.MaxBytes)}, Placement: sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: input.Apply.ShardKey, TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}}
	return id, a, nodes, o, ap, material, nil
}

func buildPreparedRF3Manifest(input prepareRF3Manifest, nodes [3]rafttransport.NodeID, p map[string]string) persistedRF3Manifest {
	m := persistedRF3Manifest{WAL: persistedRF3WAL{Path: p["wal"], KeyID: input.WAL.KeyID, KeyMaterialPath: p["key"], MaxFileBytes: input.WAL.MaxFileBytes, MaxRecordBytes: input.WAL.MaxRecordBytes, MaxRecords: input.WAL.MaxRecords, MaxEntries: input.WAL.MaxEntries, MaxLiveBytes: input.WAL.MaxLiveBytes}, SQL: persistedRF3SQL{Path: p["sql"], IdentityPath: p["identity"], ApplyIdentityPath: p["apply"]}, Route: persistedRF3GroupRoute{ClusterID: input.ClusterID, ClusterIncarnation: input.ClusterIncarnation, TopologyRecoveryEpoch: input.TopologyRecoveryEpoch, ShardIncarnation: input.ShardIncarnation, GroupID: input.GroupID, Distribution: input.Distribution, Shard: input.Shard, AllocationGeneration: input.AllocationGeneration, MemberID: input.MemberID, StoreID: input.StoreID, MemberRoot: input.Root, SplitRuntimeRoot: filepath.Join(input.Root, "split-runtime"), MembershipGrantPath: filepath.Join(input.Root, "membership-grant")}, Listeners: input.Listeners, TLS: input.TLS, AuthorizationPolicy: input.AuthorizationPolicy, ReplicaControl: persistedRF3ReplicaControl{ActionJournalPath: filepath.Join(input.Root, "replica-actions"), MaxActionRecords: 4096, SourceDataRoot: input.Root, SourceJournalPath: filepath.Join(input.Root, "source-exports"), MaxSourceRecords: 4096, SourceRepositoryPath: filepath.Join(input.Root, "source-artifacts"), MaxSourceArtifacts: 8, MaxSourceConcurrent: 2, MaxSourceArtifactBytes: 1 << 30, MaxSourceDiskBytes: 4 << 30, SourceChunkBytes: 1 << 20}, SplitControl: persistedRF3SplitControl{JournalPath: filepath.Join(input.Root, "split-control.journal"), MaxRecords: input.SplitControl.MaxRecords, MaxFileBytes: input.SplitControl.MaxFileBytes, Grants: make([]persistedRF3ActionGrant, len(input.SplitControl.Grants))}, DevelopmentOnly: input.DevelopmentOnly, Members: make([]persistedRF3Member, len(input.Members))}
	for index, grant := range input.SplitControl.Grants {
		m.SplitControl.Grants[index] = persistedRF3ActionGrant{NodeID: grant.NodeID, Actions: grant.Actions}
	}
	for i, member := range input.Members {
		m.Members[i] = persistedRF3Member{MemberID: member.MemberID, NodeID: hex.EncodeToString(nodes[i][:]), PeerAddress: member.PeerAddress}
	}
	return m
}

func decodePrepareRF3ID(value string, dst []byte) bool {
	if len(value) != hex.EncodedLen(len(dst)) || strings.ToLower(value) != value {
		return false
	}
	n, err := hex.Decode(dst, []byte(value))
	if err != nil || n != len(dst) {
		return false
	}
	return !bytes.Equal(dst, make([]byte, len(dst)))
}
func readPrepareRF3File(path string, maximum int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > int64(maximum) {
		return nil, errPrepareRF3
	}
	b := make([]byte, info.Size())
	if _, err = io.ReadFull(f, b); err != nil {
		return nil, err
	}
	var extra [1]byte
	if n, e := f.Read(extra[:]); n != 0 || !errors.Is(e, io.EOF) {
		return nil, errPrepareRF3
	}
	return b, nil
}
func writePrepareRF3File(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	serr := f.Sync()
	cerr := f.Close()
	return errors.Join(werr, serr, cerr)
}
func syncPrepareRF3Directory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
