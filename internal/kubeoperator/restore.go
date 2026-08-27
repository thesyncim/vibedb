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

// RestoreGroup imports one certified artifact into three fresh non-serving SQL
// roots. Template is the explicit canonical target-schema projection; its
// digest must be Operation.TargetCatalogDigest. The current Kubernetes prepare
// grammar is singleton-only, so bundle artifacts fail closed.
func RestoreGroup(ctx context.Context, config RestoreGroupConfig) (RestoreGroupResult, error) {
	if ctx == nil || config.Artifact == nil || !validBootstrapStateDirectory(config.Root) ||
		len(config.Template) == 0 || len(config.Template) > RestoreTemplateMaxBytes ||
		sha256.Sum256(config.Template) != config.Operation.TargetCatalogDigest {
		return RestoreGroupResult{}, ErrBootstrap
	}
	var template bootstrapPrepare
	if err := vibejson.Unmarshal(config.Template, &template); err != nil {
		return RestoreGroupResult{}, errors.Join(ErrBootstrap, err)
	}
	canonical, err := vibejson.Marshal(&template)
	if err != nil || !bytes.Equal(canonical, config.Template) || !validRestoreTemplate(template) {
		return RestoreGroupResult{}, errors.Join(ErrBootstrap, err)
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
	return RestoreGroupResult{Witness: witness}, nil
}

func validRestoreTemplate(t bootstrapPrepare) bool {
	return t.Distribution != "" && t.Shard != "" && t.AllocationGeneration != 0 &&
		t.Table != "" && t.CreateTable != "" && t.Apply.MaxSessions != 0 &&
		t.Apply.RetryWindow != 0 && t.Apply.MaxCollections != 0 &&
		t.Apply.MaxDocuments != 0 && t.Apply.MaxBytes != 0 && t.Apply.ShardKey != ""
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
	Format       uint16                            `json:"format"`
	Operation    string                            `json:"operation"`
	GroupOrdinal uint32                            `json:"group_ordinal"`
	Replica      uint8                             `json:"replica"`
	Apply        sqldriver.ReplicatedApplyIdentity `json:"apply"`
	SnapshotBase []byte                            `json:"snapshot_base"`
	RootDigest   string                            `json:"root_digest"`
}

type restoreReplicaFactory struct {
	root           string
	template       bootstrapPrepare
	templateDigest [sha256.Size]byte
}

func (f *restoreReplicaFactory) OpenReplica(
	ctx context.Context, operation clusterrestore.Operation, group uint32, replica uint8,
	manifest replicatedstate.SnapshotArtifactManifest,
) (restoreservice.ReplicaRoot, error) {
	if cause := context.Cause(ctx); cause != nil {
		return restoreservice.ReplicaRoot{}, cause
	}
	if f == nil || int(group) >= len(operation.Targets) || replica >= 3 || manifest.Bundle ||
		len(manifest.Relations) != 0 || string(manifest.UserCollection) != f.template.Table ||
		operation.TargetCatalogDigest != f.templateDigest ||
		operation.Certificate.Groups[group].RelationManifestDigest != ([sha256.Size]byte{}) {
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
		digest, commitErr := commitRestoreReplica(ctx, receiptPath, allocation, activation, operation, group, replica)
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
			path, binding, logID, f.template.Table,
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
		if err = executeRestoreDDL(database, f.template.CreateTable); err != nil {
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
	identity, err := database.BindReplicatedShardStoreStorageIdentity(
		binding, f.template.Table, allocation.UserStorage,
	)
	if err != nil {
		_ = database.Close()
		return allocation, nil, identity, err
	}
	return allocation, database, identity, nil
}

func executeRestoreDDL(database *sqldriver.Database, ddl string) error {
	session, err := database.NewSession(context.Background())
	if err != nil {
		return err
	}
	statement, err := session.Prepare(context.Background(), ddl)
	if err == nil {
		_, err = statement.Exec(context.Background(), nil)
	}
	if statement != nil {
		err = errors.Join(err, statement.Close())
	}
	return errors.Join(err, session.Close())
}

func restoreApplyOptions(in bootstrapApply) (sqldriver.ReplicatedApplyOptions, error) {
	options := sqldriver.ReplicatedApplyOptions{
		MaxSessions: in.MaxSessions, RetryWindow: in.RetryWindow,
		TxnLimits: durable.TxnLimits{MaxCollections: in.MaxCollections, MaxDocuments: in.MaxDocuments, MaxBytes: in.MaxBytes},
		Placement: sqldriver.ReplicatedPlacementProfile{
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
	data := make([]byte, 0, 32+4+32)
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
	h := sha256.New()
	h.Write([]byte("vibedb/kubeoperator/restore-root/format-1\x00"))
	h.Write(operation.Digest[:])
	h.Write([]byte{byte(group >> 24), byte(group >> 16), byte(group >> 8), byte(group), replica})
	h.Write([]byte(allocation.LogID))
	h.Write([]byte(allocation.UserStorage))
	h.Write(base)
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	receipt := restoreReplicaReceipt{Format: 1, Operation: hex.EncodeToString(operation.Digest[:]),
		GroupOrdinal: group, Replica: replica, Apply: activation.ApplyIdentity,
		SnapshotBase: base, RootDigest: hex.EncodeToString(digest[:])}
	if err := writeRestoreVibe(receiptPath, &receipt); err != nil {
		return [sha256.Size]byte{}, err
	}
	return digest, nil
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
	raw, err := os.ReadFile(path)
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
