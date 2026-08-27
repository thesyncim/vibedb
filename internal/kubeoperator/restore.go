package kubeoperator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/restoreservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	// RestoreTemplateMaxBytes bounds the canonical operator schema template.
	RestoreTemplateMaxBytes = 1 << 20
	restoreReceiptMaxBytes  = 4 << 20
)

type RestoreGroupConfig struct {
	Root      string
	Template  []byte
	Operation clusterrestore.Operation
	Ordinal   uint32
	Artifact  io.Reader
}

type RestoreGroupResult struct {
	Witness clusterrestore.RootWitness
}

type restoreSchemaSet struct {
	Format uint16              `json:"format"`
	Groups []restoreSchemaSlot `json:"groups"`
}

type restoreSchemaSlot struct {
	Ordinal uint32                `json:"ordinal"`
	Schema  restoreSchemaTemplate `json:"schema"`
}

type restoreSchemaTemplate struct {
	Format               uint16                       `json:"format"`
	Distribution         string                       `json:"distribution"`
	Shard                string                       `json:"shard"`
	AllocationGeneration uint64                       `json:"allocation_generation"`
	BaseTable            string                       `json:"base_table"`
	DDL                  []string                     `json:"ddl"`
	GlobalIndexes        []restoreGlobalIndexTemplate `json:"global_indexes"`
	Apply                bootstrapApply               `json:"apply"`
}

type restoreGlobalIndexTemplate struct {
	Relation      uint16 `json:"relation"`
	Table         string `json:"table"`
	IndexID       uint64 `json:"index_id"`
	Incarnation   uint64 `json:"incarnation"`
	LocatorCount  uint8  `json:"locator_count"`
	Unique        bool   `json:"unique"`
	KeyEncoding   uint8  `json:"key_encoding"`
	KeyArity      uint8  `json:"key_arity"`
	TupleVersion  uint32 `json:"tuple_version"`
	MapperVersion uint32 `json:"mapper_version"`
	BucketBits    uint8  `json:"bucket_bits"`
}

// RestoredReplicaState is the durable non-serving handoff consumed by the
// shard-side adopter. SnapshotBase contains only fresh target authority and the
// sanitized relation image certified by the restore operation.
type RestoredReplicaState struct {
	Identity        sqldriver.ReplicatedShardStoreIdentity
	Apply           sqldriver.ReplicatedApplyIdentity
	SnapshotBase    *pb.Snapshot
	OperationDigest [sha256.Size]byte
	GroupOrdinal    uint32
	ReplicaOrdinal  uint8
	Targets         [3]RestoredReplicaTarget
}

type RestoredReplicaTarget struct {
	Member          uint64
	Node            [16]byte
	Store           [16]byte
	NodeIncarnation uint64
}

// OpenRestoredReplicaState authenticates the canonical allocation and
// activation receipts retained beside one restored member.vdb.
func OpenRestoredReplicaState(root string) (RestoredReplicaState, error) {
	if !validBootstrapStateDirectory(root) {
		return RestoredReplicaState{}, ErrBootstrap
	}
	var allocation restoreReplicaAllocation
	found, err := readRestoreVibe(filepath.Join(root, "allocation.vibejson"), &allocation)
	if err != nil || !found || allocation.Format != 1 || len(allocation.Operation) != 64 ||
		len(allocation.LogID) != 32 || len(allocation.UserStorage) != 32 {
		return RestoredReplicaState{}, errors.Join(ErrBootstrap, err)
	}
	var receipt restoreReplicaReceipt
	found, err = readRestoreVibe(filepath.Join(root, "activation.vibejson"), &receipt)
	if err != nil || !found || receipt.Format != 1 || receipt.Operation != allocation.Operation ||
		receipt.GroupOrdinal != allocation.GroupOrdinal || receipt.Replica != allocation.ReplicaOrdinal ||
		len(receipt.RootDigest) != 64 || receipt.Apply.Format == 0 || receipt.Apply.Storage == "" {
		return RestoredReplicaState{}, errors.Join(ErrBootstrap, err)
	}
	var logID [16]byte
	if decodeRestoreHex(logID[:], allocation.LogID) != nil || receipt.Identity.LogID != logID {
		return RestoredReplicaState{}, ErrBootstrap
	}
	var snapshot pb.Snapshot
	if err := proto.Unmarshal(receipt.SnapshotBase, &snapshot); err != nil {
		return RestoredReplicaState{}, errors.Join(ErrBootstrap, err)
	}
	if _, err := replicatedstate.OpenSnapshotBase(&snapshot); err != nil {
		return RestoredReplicaState{}, errors.Join(ErrBootstrap, err)
	}
	var operation [sha256.Size]byte
	if decodeRestoreHex(operation[:], receipt.Operation) != nil {
		return RestoredReplicaState{}, ErrBootstrap
	}
	var targets [3]RestoredReplicaTarget
	for index, target := range receipt.Targets {
		if decodeRestoreHex(targets[index].Node[:], target.Node) != nil ||
			decodeRestoreHex(targets[index].Store[:], target.Store) != nil || target.Member == 0 || target.NodeIncarnation == 0 {
			return RestoredReplicaState{}, ErrBootstrap
		}
		targets[index].Member, targets[index].NodeIncarnation = target.Member, target.NodeIncarnation
	}
	return RestoredReplicaState{Identity: receipt.Identity, Apply: receipt.Apply, SnapshotBase: &snapshot,
		OperationDigest: operation, GroupOrdinal: receipt.GroupOrdinal, ReplicaOrdinal: receipt.Replica,
		Targets: targets}, nil
}

// RestoreGroup imports one certified artifact into three fresh non-serving SQL
// roots. Template is the explicit canonical target-schema projection; its
// digest must be Operation.TargetCatalogDigest. Ordered DDL and relation
// descriptors are explicit; no schema or index definition is inferred.
func RestoreGroup(ctx context.Context, config RestoreGroupConfig) (RestoreGroupResult, error) {
	if ctx == nil || config.Artifact == nil || !validBootstrapStateDirectory(config.Root) ||
		len(config.Template) == 0 || len(config.Template) > RestoreTemplateMaxBytes ||
		sha256.Sum256(config.Template) != config.Operation.TargetCatalogDigest {
		return RestoreGroupResult{}, ErrBootstrap
	}
	template, err := openRestoreSchemaSet(config.Template, config.Operation, config.Ordinal)
	if err != nil {
		return RestoreGroupResult{}, err
	}
	if result, found, err := recoverSealedRestoreGroup(ctx, config, template); found || err != nil {
		return result, err
	}
	staging := filepath.Join(config.Root, "staging")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return RestoreGroupResult{}, err
	}
	if !validBootstrapStateDirectory(staging) {
		return RestoreGroupResult{}, ErrBootstrap
	}
	factory := &restoreReplicaFactory{root: config.Root, template: template,
		templateDigest: config.Operation.TargetCatalogDigest}
	witness, err := (restoreservice.GroupInstaller{
		Root: staging, Factory: factory,
	}).Install(ctx, config.Operation, config.Ordinal, config.Artifact)
	if err != nil {
		return RestoreGroupResult{}, err
	}
	if err := sealRestoreGroup(config, witness); err != nil {
		return RestoreGroupResult{}, err
	}
	return RestoreGroupResult{Witness: witness}, nil
}

// ValidateRestoreSchemaSet checks one canonical, operation-bound schema-set
// projection. Every source group has exactly one dense ordinal, allowing
// catalog, ledger, and user groups to retain different explicit schemas.
func ValidateRestoreSchemaSet(raw []byte, operation clusterrestore.Operation, ordinal uint32) error {
	_, err := openRestoreSchemaSet(raw, operation, ordinal)
	return err
}

func openRestoreSchemaSet(raw []byte, operation clusterrestore.Operation, ordinal uint32) (restoreSchemaTemplate, error) {
	if len(raw) == 0 || len(raw) > RestoreTemplateMaxBytes ||
		sha256.Sum256(raw) != operation.TargetCatalogDigest || int(ordinal) >= len(operation.Targets) {
		return restoreSchemaTemplate{}, ErrBootstrap
	}
	var set restoreSchemaSet
	if err := vibejson.Unmarshal(raw, &set); err != nil {
		return restoreSchemaTemplate{}, errors.Join(ErrBootstrap, err)
	}
	canonical, err := vibejson.Marshal(&set)
	if err != nil || !bytes.Equal(canonical, raw) || set.Format != 1 || len(set.Groups) != len(operation.Targets) {
		return restoreSchemaTemplate{}, errors.Join(ErrBootstrap, err)
	}
	for index, slot := range set.Groups {
		if slot.Ordinal != uint32(index) || !validRestoreTemplate(slot.Schema) {
			return restoreSchemaTemplate{}, ErrBootstrap
		}
	}
	return set.Groups[ordinal].Schema, nil
}

func validRestoreTemplate(t restoreSchemaTemplate) bool {
	if t.Format != 1 || t.Distribution == "" || t.Shard == "" || t.AllocationGeneration == 0 ||
		t.BaseTable == "" || len(t.DDL) == 0 || t.Apply.MaxSessions == 0 ||
		t.Apply.RetryWindow == 0 || t.Apply.MaxCollections == 0 ||
		t.Apply.MaxDocuments == 0 || t.Apply.MaxBytes == 0 || t.Apply.ShardKey == "" {
		return false
	}
	for index, ddl := range t.DDL {
		if ddl == "" || index > int(^uint16(0)) {
			return false
		}
	}
	for index, relation := range t.GlobalIndexes {
		if relation.Relation != uint16(index+2) || relation.Table == "" || relation.IndexID == 0 ||
			relation.Incarnation == 0 || relation.LocatorCount == 0 || relation.KeyEncoding == 0 ||
			relation.KeyArity == 0 || relation.TupleVersion == 0 || relation.MapperVersion == 0 || relation.BucketBits == 0 {
			return false
		}
	}
	return true
}

func (f *restoreReplicaFactory) manifestMatchesTemplate(manifest replicatedstate.SnapshotArtifactManifest) bool {
	bundle := len(f.template.GlobalIndexes) != 0
	if manifest.Bundle != bundle {
		return false
	}
	if !bundle {
		return len(manifest.Relations) == 0 && manifest.RelationManifestDigest == ([sha256.Size]byte{})
	}
	if len(manifest.Relations) != len(f.template.GlobalIndexes)+1 ||
		manifest.RelationManifestDigest == ([sha256.Size]byte{}) {
		return false
	}
	for index, relation := range manifest.Relations {
		if relation.Relation != replication.RelationID(index+1) {
			return false
		}
		if index == 0 {
			if relation.Kind != replicatedstate.RelationJSON || string(relation.Collection) != f.template.BaseTable {
				return false
			}
		} else if relation.Kind != replicatedstate.RelationGlobalIndex ||
			string(relation.Collection) != f.template.GlobalIndexes[index-1].Table {
			return false
		}
	}
	return true
}

func (f *restoreReplicaFactory) globalRelations() []sqldriver.ReplicatedGlobalIndexRelation {
	result := make([]sqldriver.ReplicatedGlobalIndexRelation, len(f.template.GlobalIndexes))
	for index, relation := range f.template.GlobalIndexes {
		result[index] = sqldriver.ReplicatedGlobalIndexRelation{
			Relation: relation.Relation, Table: relation.Table, IndexID: relation.IndexID,
			Incarnation: relation.Incarnation, LocatorCount: relation.LocatorCount, Unique: relation.Unique,
			KeyEncoding: sqldriver.ReplicatedRelationKeyEncoding(relation.KeyEncoding), KeyArity: relation.KeyArity,
			TupleVersion:  distribution.TupleVersion(relation.TupleVersion),
			MapperVersion: distribution.MapperVersion(relation.MapperVersion), BucketBits: relation.BucketBits,
		}
	}
	return result
}

type restoreReplicaAllocation struct {
	Format         uint16 `json:"format"`
	Operation      string `json:"operation"`
	GroupOrdinal   uint32 `json:"group_ordinal"`
	ReplicaOrdinal uint8  `json:"replica_ordinal"`
	LogID          string `json:"log_id"`
	UserStorage    string `json:"user_storage"`
}

type restoreReplicaReceipt struct {
	Format       uint16                                 `json:"format"`
	Operation    string                                 `json:"operation"`
	GroupOrdinal uint32                                 `json:"group_ordinal"`
	Replica      uint8                                  `json:"replica"`
	Identity     sqldriver.ReplicatedShardStoreIdentity `json:"identity"`
	Apply        sqldriver.ReplicatedApplyIdentity      `json:"apply"`
	Targets      [3]restoreReplicaTarget                `json:"targets"`
	SnapshotBase []byte                                 `json:"snapshot_base"`
	RootDigest   string                                 `json:"root_digest"`
}

type restoreReplicaTarget struct {
	Member          uint64 `json:"member"`
	Node            string `json:"node"`
	Store           string `json:"store"`
	NodeIncarnation uint64 `json:"node_incarnation"`
}

type restoreReplicaFactory struct {
	root           string
	template       restoreSchemaTemplate
	templateDigest [sha256.Size]byte
}

func (f *restoreReplicaFactory) OpenReplica(
	ctx context.Context, operation clusterrestore.Operation, group uint32, replica uint8,
	manifest replicatedstate.SnapshotArtifactManifest,
) (restoreservice.ReplicaRoot, error) {
	if cause := context.Cause(ctx); cause != nil {
		return restoreservice.ReplicaRoot{}, cause
	}
	if f == nil || int(group) >= len(operation.Targets) || replica >= 3 ||
		string(manifest.UserCollection) != f.template.BaseTable || !f.manifestMatchesTemplate(manifest) ||
		operation.TargetCatalogDigest != f.templateDigest ||
		operation.Certificate.Groups[group].RelationManifestDigest != manifest.RelationManifestDigest {
		return restoreservice.ReplicaRoot{}, ErrBootstrap
	}
	directory := filepath.Join(f.root, "roots", fmt.Sprintf("group-%08d", group), "replica-"+strconv.Itoa(int(replica+1)))
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return restoreservice.ReplicaRoot{}, err
	}
	if !validBootstrapStateDirectory(directory) {
		return restoreservice.ReplicaRoot{}, ErrBootstrap
	}
	path := filepath.Join(directory, "member.vdb")
	binding, err := f.binding(operation, group, replica)
	if err != nil {
		return restoreservice.ReplicaRoot{}, err
	}
	allocationPath, receiptPath := filepath.Join(directory, "allocation.vibejson"), filepath.Join(directory, "activation.vibejson")
	options, err := restoreApplyOptions(f.template.Apply)
	if err != nil {
		return restoreservice.ReplicaRoot{}, err
	}
	allocation, database, identity, err := f.openOrCreateRoot(path, allocationPath, operation, group, replica, binding)
	if err != nil {
		return restoreservice.ReplicaRoot{}, err
	}
	if manifest.Bundle {
		digest, digestErr := sqldriver.ReplicatedRelationManifestDigest(identity)
		if digestErr != nil || digest != manifest.RelationManifestDigest {
			_ = database.Close()
			return restoreservice.ReplicaRoot{}, errors.Join(ErrBootstrap, digestErr)
		}
	}
	receipt, committed, err := readRestoreReceipt(receiptPath, operation, group, replica)
	if err != nil {
		_ = database.Close()
		return restoreservice.ReplicaRoot{}, err
	}
	if committed {
		if err = database.Close(); err != nil {
			return restoreservice.ReplicaRoot{}, err
		}
		var applyIdentity sqldriver.ReplicatedApplyIdentity
		database, applyIdentity, err = sqldriver.OpenReplicatedShardStoreWithApplyForSettlement(path, identity, options)
		if err != nil || applyIdentity != receipt.Apply {
			if database != nil {
				_ = database.Close()
			}
			return restoreservice.ReplicaRoot{}, errors.Join(ErrBootstrap, err)
		}
	}
	bootstrap := restoreRF3Bootstrap(operation, group, f.templateDigest)
	root := restoreservice.ReplicaRoot{
		Database: database, Identity: identity, ApplyOptions: options, Bootstrap: bootstrap,
	}
	root.Recover = func(ctx context.Context, source replicatedstate.SnapshotArtifactManifest) (
		sqldriver.ReplicatedChildActivation, [sha256.Size]byte, bool, error,
	) {
		return recoverRestoreReplica(ctx, receiptPath, database, identity, options, bootstrap,
			operation, group, replica, source)
	}
	root.Commit = func(ctx context.Context, activation sqldriver.ReplicatedChildActivation) ([sha256.Size]byte, error) {
		digest, commitErr := commitRestoreReplica(ctx, receiptPath, allocation, identity, activation, operation, group, replica)
		if commitErr == nil {
			commitErr = errors.Join(activation.Apply.Close(), database.Close())
		}
		return digest, commitErr
	}
	return root, nil
}

func (f *restoreReplicaFactory) binding(
	operation clusterrestore.Operation, group uint32, replica uint8,
) (sqldriver.ReplicatedShardStoreBinding, error) {
	target, local := operation.Targets[group], operation.Targets[group].Replicas[replica]
	cut := operation.Certificate.Groups[group]
	if cut.SchemaGeneration == 0 || operation.PolicyGeneration == 0 {
		return sqldriver.ReplicatedShardStoreBinding{}, ErrBootstrap
	}
	return sqldriver.ReplicatedShardStoreBinding{
		ClusterID: target.Group.ClusterID, ClusterIncarnation: target.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: target.Group.TopologyRecoveryEpoch,
		Distribution:          f.template.Distribution, Shard: f.template.Shard,
		AllocationGeneration: f.template.AllocationGeneration,
		ShardIncarnation:     target.Group.ShardIncarnation, GroupID: target.Group.GroupID,
		MemberID: local.Member, StoreID: local.Store,
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: operation.PolicyGeneration, ProtectionEpoch: 1,
			OwnershipEpoch: 1, SchemaGeneration: cut.SchemaGeneration,
			RoutingVersion: 1, RouteGeneration: 1,
		},
	}, nil
}

func (f *restoreReplicaFactory) openOrCreateRoot(
	path, allocationPath string, operation clusterrestore.Operation, group uint32, replica uint8,
	binding sqldriver.ReplicatedShardStoreBinding,
) (restoreReplicaAllocation, *sqldriver.Database, sqldriver.ReplicatedShardStoreIdentity, error) {
	allocation, found, err := readRestoreAllocation(allocationPath, operation, group, replica)
	if err != nil {
		return allocation, nil, sqldriver.ReplicatedShardStoreIdentity{}, err
	}
	shardBinding := sqldriver.ShardStoreBinding{
		Distribution:         distribution.DistributionName(binding.Distribution),
		Shard:                distribution.ShardID(binding.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration),
	}
	if found {
		var logID [16]byte
		if decodeRestoreHex(logID[:], allocation.LogID) != nil {
			return allocation, nil, sqldriver.ReplicatedShardStoreIdentity{}, ErrBootstrap
		}
		if database, identity, settleErr := sqldriver.OpenReplicatedShardStoreForSettlement(
			path, binding, logID, f.template.BaseTable,
		); settleErr == nil {
			return allocation, database, identity, nil
		}
	}
	database, err := sqldriver.OpenShardStore(path, shardBinding)
	if err != nil {
		if !errors.Is(err, sqldriver.ErrShardStoreUnbound) {
			return allocation, nil, sqldriver.ReplicatedShardStoreIdentity{}, err
		}
		database, err = sqldriver.InitializeShardStore(path, shardBinding)
		if err != nil {
			return allocation, nil, sqldriver.ReplicatedShardStoreIdentity{}, err
		}
		if err = executeRestoreDDLs(database, f.template.DDL); err != nil {
			_ = database.Close()
			return allocation, nil, sqldriver.ReplicatedShardStoreIdentity{}, err
		}
	}
	local, err := database.ShardStoreIdentity()
	if err != nil {
		_ = database.Close()
		return allocation, nil, sqldriver.ReplicatedShardStoreIdentity{}, err
	}
	if !found {
		storage := restoreStorageIdentity(operation.Digest, group, replica)
		allocation = restoreReplicaAllocation{Format: 1, Operation: hex.EncodeToString(operation.Digest[:]),
			GroupOrdinal: group, ReplicaOrdinal: replica, LogID: hex.EncodeToString(local.LogID[:]), UserStorage: storage}
		if err := writeRestoreVibe(allocationPath, &allocation); err != nil {
			_ = database.Close()
			return allocation, nil, sqldriver.ReplicatedShardStoreIdentity{}, err
		}
	}
	var identity sqldriver.ReplicatedShardStoreIdentity
	if len(f.template.GlobalIndexes) == 0 {
		identity, err = database.BindReplicatedShardStoreStorageIdentity(binding, f.template.BaseTable, allocation.UserStorage)
	} else {
		identity, err = database.BindReplicatedShardStoreBundle(binding, f.template.BaseTable, f.globalRelations())
	}
	if err != nil {
		_ = database.Close()
		return allocation, nil, identity, err
	}
	return allocation, database, identity, nil
}

func executeRestoreDDLs(database *sqldriver.Database, ddls []string) error {
	session, err := database.NewSession(context.Background())
	if err != nil {
		return err
	}
	for _, ddl := range ddls {
		statement, prepareErr := session.Prepare(context.Background(), ddl)
		if prepareErr == nil {
			_, prepareErr = statement.Exec(context.Background(), nil)
		}
		if statement != nil {
			prepareErr = errors.Join(prepareErr, statement.Close())
		}
		if prepareErr != nil {
			return errors.Join(prepareErr, session.Close())
		}
	}
	return session.Close()
}

func restoreApplyOptions(in bootstrapApply) (sqldriver.ReplicatedApplyOptions, error) {
	options := sqldriver.ReplicatedApplyOptions{
		MaxSessions: in.MaxSessions, RetryWindow: in.RetryWindow,
		TxnLimits: durable.TxnLimits{MaxCollections: in.MaxCollections, MaxDocuments: in.MaxDocuments, MaxBytes: in.MaxBytes},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format:   sqldriver.ReplicatedPlacementProfileFormat,
			ShardKey: in.ShardKey, TupleVersion: distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
		RequestLedgerCapacityBytes:       in.RequestLedgerCapacityBytes,
		RequestLedgerCleanupReserveBytes: in.RequestLedgerCleanupReserveBytes,
	}
	for raw, destination := range map[string]*[sha256.Size]byte{
		in.RequestLedgerRangeStart:    &options.RequestLedgerRangeStart,
		in.RequestLedgerRangeEnd:      &options.RequestLedgerRangeEnd,
		in.RequestLedgerRangeIdentity: &options.RequestLedgerRangeIdentity,
	} {
		if raw != "" && decodeRestoreHex(destination[:], raw) != nil {
			return sqldriver.ReplicatedApplyOptions{}, ErrBootstrap
		}
	}
	return options, nil
}

func restoreRF3Bootstrap(operation clusterrestore.Operation, group uint32, template [sha256.Size]byte) *pb.Snapshot {
	index, term := uint64(1), uint64(1)
	data := make([]byte, 0, len(restoreBootstrapMagic)+32+4+32)
	data = append(data, restoreBootstrapMagic[:]...)
	data = append(data, operation.Digest[:]...)
	data = append(data, byte(group>>24), byte(group>>16), byte(group>>8), byte(group))
	data = append(data, template[:]...)
	return &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{
		Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
	}}
}

func recoverRestoreReplica(
	ctx context.Context, receiptPath string, database *sqldriver.Database, identity sqldriver.ReplicatedShardStoreIdentity,
	options sqldriver.ReplicatedApplyOptions, bootstrap *pb.Snapshot,
	operation clusterrestore.Operation, group uint32, replica uint8,
	source replicatedstate.SnapshotArtifactManifest,
) (sqldriver.ReplicatedChildActivation, [sha256.Size]byte, bool, error) {
	if cause := context.Cause(ctx); cause != nil {
		return sqldriver.ReplicatedChildActivation{}, [sha256.Size]byte{}, false, cause
	}
	receipt, found, err := readRestoreReceipt(receiptPath, operation, group, replica)
	if err != nil || !found {
		return sqldriver.ReplicatedChildActivation{}, [sha256.Size]byte{}, false, err
	}
	var snapshot pb.Snapshot
	if err = proto.Unmarshal(receipt.SnapshotBase, &snapshot); err != nil {
		return sqldriver.ReplicatedChildActivation{}, [sha256.Size]byte{}, false, err
	}
	certificate, err := replicatedstate.OpenSnapshotBase(&snapshot)
	if err != nil || certificate.Manifest.ImageDigest != source.ImageDigest {
		return sqldriver.ReplicatedChildActivation{}, [sha256.Size]byte{}, false, errors.Join(ErrBootstrap, err)
	}
	activation, resumed, err := database.ResumeReplicatedSnapshotActivation(identity, certificate.Manifest, bootstrap, options)
	if err != nil || !resumed {
		_ = database.Close()
		return sqldriver.ReplicatedChildActivation{}, [sha256.Size]byte{}, false, errors.Join(ErrBootstrap, err)
	}
	if err = errors.Join(activation.Apply.Close(), database.Close()); err != nil {
		return sqldriver.ReplicatedChildActivation{}, [sha256.Size]byte{}, false, err
	}
	digestBytes, _ := hex.DecodeString(receipt.RootDigest)
	var digest [sha256.Size]byte
	copy(digest[:], digestBytes)
	return activation, digest, true, nil
}

func commitRestoreReplica(
	ctx context.Context, receiptPath string, allocation restoreReplicaAllocation,
	identity sqldriver.ReplicatedShardStoreIdentity,
	activation sqldriver.ReplicatedChildActivation, operation clusterrestore.Operation,
	group uint32, replica uint8,
) ([sha256.Size]byte, error) {
	if cause := context.Cause(ctx); cause != nil {
		return [sha256.Size]byte{}, cause
	}
	if activation.Apply == nil || activation.SnapshotBase == nil {
		return [sha256.Size]byte{}, ErrBootstrap
	}
	base, err := proto.MarshalOptions{Deterministic: true}.Marshal(activation.SnapshotBase)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	digest := restoreRootDigest(allocation, base, operation, group, replica)
	receipt := restoreReplicaReceipt{Format: 1, Operation: hex.EncodeToString(operation.Digest[:]),
		GroupOrdinal: group, Replica: replica, Identity: identity, Apply: activation.ApplyIdentity,
		SnapshotBase: base, RootDigest: hex.EncodeToString(digest[:])}
	for index, target := range operation.Targets[group].Replicas {
		receipt.Targets[index] = restoreReplicaTarget{Member: target.Member,
			Node: hex.EncodeToString(target.Node[:]), Store: hex.EncodeToString(target.Store[:]),
			NodeIncarnation: target.NodeIncarnation}
	}
	if err := writeRestoreVibe(receiptPath, &receipt); err != nil {
		return [sha256.Size]byte{}, err
	}
	return digest, nil
}

func restoreRootDigest(allocation restoreReplicaAllocation, snapshot []byte, operation clusterrestore.Operation, group uint32, replica uint8) (digest [sha256.Size]byte) {
	h := sha256.New()
	h.Write([]byte("vibedb/kubeoperator/restore-root/format-1\x00"))
	h.Write(operation.Digest[:])
	h.Write([]byte{byte(group >> 24), byte(group >> 16), byte(group >> 8), byte(group), replica})
	h.Write([]byte(allocation.LogID))
	h.Write([]byte(allocation.UserStorage))
	h.Write(snapshot)
	copy(digest[:], h.Sum(nil))
	return digest
}

func readRestoreAllocation(path string, operation clusterrestore.Operation, group uint32, replica uint8) (restoreReplicaAllocation, bool, error) {
	var value restoreReplicaAllocation
	found, err := readRestoreVibe(path, &value)
	if err != nil || !found {
		return value, found, err
	}
	if value.Format != 1 || value.Operation != hex.EncodeToString(operation.Digest[:]) ||
		value.GroupOrdinal != group || value.ReplicaOrdinal != replica || len(value.UserStorage) != 32 {
		return value, false, ErrBootstrap
	}
	return value, true, nil
}

func readRestoreReceipt(path string, operation clusterrestore.Operation, group uint32, replica uint8) (restoreReplicaReceipt, bool, error) {
	var value restoreReplicaReceipt
	found, err := readRestoreVibe(path, &value)
	if err != nil || !found {
		return value, found, err
	}
	if value.Format != 1 || value.Operation != hex.EncodeToString(operation.Digest[:]) ||
		value.GroupOrdinal != group || value.Replica != replica || len(value.RootDigest) != 64 {
		return value, false, ErrBootstrap
	}
	return value, true, nil
}

func readRestoreVibe[T any](path string, destination *T) (bool, error) {
	raw, err := readRestoreBounded(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || len(raw) == 0 || len(raw) > restoreReceiptMaxBytes {
		return false, errors.Join(ErrBootstrap, err)
	}
	if err := vibejson.Unmarshal[T](raw, destination); err != nil {
		return false, errors.Join(ErrBootstrap, err)
	}
	canonical, err := vibejson.Marshal[T](destination)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(canonical, raw) {
		return false, ErrBootstrap
	}
	return true, nil
}

func writeRestoreVibe[T any](path string, value *T) error {
	raw, err := vibejson.Marshal[T](value)
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, raw, 0o600); err != nil {
		return err
	}
	file, err := os.OpenFile(temporary, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = errors.Join(file.Sync(), file.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(temporary, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func restoreStorageIdentity(operation [sha256.Size]byte, group uint32, replica uint8) string {
	h := sha256.New()
	h.Write([]byte("vibedb/kubeoperator/restore-storage/format-1\x00"))
	h.Write(operation[:])
	h.Write([]byte{byte(group >> 24), byte(group >> 16), byte(group >> 8), byte(group), replica})
	return hex.EncodeToString(h.Sum(nil)[:16])
}

func decodeRestoreHex(destination []byte, raw string) error {
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != len(destination) {
		return ErrBootstrap
	}
	copy(destination, decoded)
	return nil
}
