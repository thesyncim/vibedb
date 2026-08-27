package kubeoperator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"google.golang.org/protobuf/proto"
)

// A seal is published only after all three import commits are durable. It
// permits read-only witness recovery while shard processes own their SQL/WAL
// roots. It is staging evidence, never catalog or serving authority.
type restoreGroupSeal struct {
	Format    uint16                     `json:"format"`
	Operation string                     `json:"operation"`
	SchemaSet string                     `json:"schema_set"`
	Ordinal   uint32                     `json:"ordinal"`
	Receipts  [3]string                  `json:"receipts"`
	Witness   clusterrestore.RootWitness `json:"witness"`
}

func restoreGroupDirectory(root string, ordinal uint32) string {
	return filepath.Join(root, "roots", fmt.Sprintf("group-%08d", ordinal))
}

func sealRestoreGroup(config RestoreGroupConfig, witness clusterrestore.RootWitness) error {
	seal := restoreGroupSeal{Format: 1, Operation: hex.EncodeToString(config.Operation.Digest[:]),
		SchemaSet: hex.EncodeToString(config.Operation.TargetCatalogDigest[:]), Ordinal: config.Ordinal, Witness: witness}
	directory := restoreGroupDirectory(config.Root, config.Ordinal)
	for index := range seal.Receipts {
		raw, err := readRestoreBounded(filepath.Join(directory, fmt.Sprintf("replica-%d", index+1), "activation.vibejson"))
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		seal.Receipts[index] = hex.EncodeToString(digest[:])
	}
	return writeRestoreVibe(filepath.Join(directory, "restore-group.vibejson"), &seal)
}

func recoverSealedRestoreGroup(ctx context.Context, config RestoreGroupConfig, template restoreSchemaTemplate) (RestoreGroupResult, bool, error) {
	directory := restoreGroupDirectory(config.Root, config.Ordinal)
	var seal restoreGroupSeal
	found, err := readRestoreVibe(filepath.Join(directory, "restore-group.vibejson"), &seal)
	if err != nil || !found {
		return RestoreGroupResult{}, found, err
	}
	if cause := context.Cause(ctx); cause != nil {
		return RestoreGroupResult{}, true, cause
	}
	if seal.Format != 1 || seal.Operation != hex.EncodeToString(config.Operation.Digest[:]) ||
		seal.SchemaSet != hex.EncodeToString(config.Operation.TargetCatalogDigest[:]) || seal.Ordinal != config.Ordinal ||
		int(config.Ordinal) >= len(config.Operation.Certificate.Groups) {
		return RestoreGroupResult{}, true, ErrBootstrap
	}
	cut := config.Operation.Certificate.Groups[config.Ordinal]
	hash := sha256.New()
	reader := &restoreCountingReader{reader: io.TeeReader(config.Artifact, hash)}
	var source replicatedstate.SnapshotArtifactManifest
	var importedImage [32]byte
	if template.projection == nil {
		first, stateErr := OpenRestoredReplicaState(filepath.Join(directory, "replica-1"))
		if stateErr != nil {
			return RestoreGroupResult{}, true, stateErr
		}
		source, importedImage, err = sqldriver.VerifyReplicatedRestoreImage(first.Identity, first.Apply, reader)
	} else {
		source, err = replicatedstate.VerifySnapshotArtifact(reader, replicatedstate.SnapshotArtifactCallbacks{})
	}
	if err != nil || reader.bytes != cut.ArtifactBytes || !bytes.Equal(hash.Sum(nil), cut.ArtifactHash[:]) ||
		source.Digest != cut.ArtifactManifestDigest || source.RelationManifestDigest != cut.RelationManifestDigest ||
		source.State.Applied != cut.SnapshotIndex || source.State.LastTerm != cut.SnapshotTerm {
		return RestoreGroupResult{}, true, errors.Join(ErrBootstrap, err)
	}
	factory := restoreReplicaFactory{root: config.Root, template: template, templateDigest: config.Operation.TargetCatalogDigest}
	if !factory.manifestMatchesTemplate(source) || string(source.UserCollection) != template.BaseTable ||
		seal.Witness.ArtifactManifest != source.Digest || template.projection == nil && seal.Witness.SanitizedImageDigest != importedImage ||
		seal.Witness.SnapshotIndex != cut.SnapshotIndex || seal.Witness.SnapshotTerm != cut.SnapshotTerm ||
		seal.Witness.TargetGroup != restoreEncodedTargetGroup(config.Operation, config.Ordinal) {
		return RestoreGroupResult{}, true, ErrBootstrap
	}
	for index := range seal.Receipts {
		root := filepath.Join(directory, fmt.Sprintf("replica-%d", index+1))
		raw, readErr := readRestoreBounded(filepath.Join(root, "activation.vibejson"))
		digest := sha256.Sum256(raw)
		if readErr != nil || hex.EncodeToString(digest[:]) != seal.Receipts[index] {
			return RestoreGroupResult{}, true, errors.Join(ErrBootstrap, readErr)
		}
		receipt, present, readErr := readRestoreReceipt(filepath.Join(root, "activation.vibejson"), config.Operation, config.Ordinal, uint8(index))
		state, stateErr := OpenRestoredReplicaState(root)
		allocation, allocated, allocationErr := readRestoreAllocation(filepath.Join(root, "allocation.vibejson"), config.Operation, config.Ordinal, uint8(index))
		binding, bindingErr := factory.binding(config.Operation, config.Ordinal, uint8(index))
		if readErr != nil || !present || stateErr != nil || bindingErr != nil || allocationErr != nil || !allocated || state.Identity.Binding != binding ||
			state.OperationDigest != config.Operation.Digest || state.GroupOrdinal != config.Ordinal || state.ReplicaOrdinal != uint8(index) ||
			state.Identity.UserTable != template.BaseTable || receipt.RootDigest != hex.EncodeToString(seal.Witness.ReplicaRoots[index][:]) {
			return RestoreGroupResult{}, true, errors.Join(ErrBootstrap, readErr, stateErr, bindingErr, allocationErr)
		}
		logical, logicalErr := sqldriver.ReplicatedRelationManifestDigest(state.Identity)
		if logicalErr != nil || logical != template.logicalSchemaDigest {
			return RestoreGroupResult{}, true, errors.Join(ErrBootstrap, logicalErr)
		}
		if restoreRootDigest(allocation, receipt.SnapshotBase, config.Operation, config.Ordinal, uint8(index)) != seal.Witness.ReplicaRoots[index] ||
			allocation.UserStorage != restoreStorageIdentity(config.Operation.Digest, config.Ordinal, uint8(index)) {
			return RestoreGroupResult{}, true, ErrBootstrap
		}
		for targetIndex, target := range config.Operation.Targets[config.Ordinal].Replicas {
			retained := state.Targets[targetIndex]
			if retained.Member != target.Member || retained.Node != [16]byte(target.Node) || retained.Store != target.Store || retained.NodeIncarnation != target.NodeIncarnation {
				return RestoreGroupResult{}, true, ErrBootstrap
			}
		}
		certificate, certificateErr := replicatedstate.OpenSnapshotBase(state.SnapshotBase)
		expectedImage := importedImage
		if template.projection != nil {
			var imageErr error
			expectedImage, imageErr = replicatedstate.ProjectionImageDigest(template.BaseTable, state.Apply.ValidationDigest, template.projection)
			if imageErr != nil {
				return RestoreGroupResult{}, true, imageErr
			}
		}
		options, optionsErr := restoreApplyOptions(template.Apply)
		expectedBinding := replicatedstate.Binding{
			ClusterID: replication.ID128(binding.ClusterID), ClusterIncarnation: replication.ID128(binding.ClusterIncarnation),
			TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch, Distribution: binding.Distribution, Shard: binding.Shard,
			AllocationGeneration: binding.AllocationGeneration, ShardIncarnation: replication.ID128(binding.ShardIncarnation), GroupID: replication.ID128(binding.GroupID),
			ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration, ProtectionEpoch: binding.Authority.ProtectionEpoch,
			OwnershipEpoch: binding.Authority.OwnershipEpoch, SchemaGeneration: binding.Authority.SchemaGeneration,
			RoutingVersion: binding.Authority.RoutingVersion, RouteGeneration: binding.Authority.RouteGeneration, OwnedRange: options.Placement.Range,
		}
		if certificateErr != nil || certificate.Digest != seal.Witness.GenesisProof ||
			optionsErr != nil || certificate.Manifest.State.Binding != expectedBinding || certificate.Manifest.SystemRows != 1 || certificate.Manifest.CaptureRows != 0 ||
			certificate.Manifest.ImageDigest != expectedImage || seal.Witness.SanitizedImageDigest != expectedImage ||
			certificate.Manifest.State.Applied != source.State.Applied || certificate.Manifest.State.LastTerm != source.State.LastTerm ||
			!proto.Equal(certificate.StaticBootstrap, restoreRF3Bootstrap(config.Operation, config.Ordinal, config.Operation.TargetCatalogDigest)) {
			return RestoreGroupResult{}, true, errors.Join(ErrBootstrap, certificateErr)
		}
		if receipt.MachineManifest != template.relationDigest || source.Bundle && certificate.Manifest.RelationManifestDigest != template.relationDigest {
			return RestoreGroupResult{}, true, ErrBootstrap
		}
	}
	return RestoreGroupResult{Witness: seal.Witness}, true, nil
}

func restoreEncodedTargetGroup(operation clusterrestore.Operation, ordinal uint32) (raw [72]byte) {
	group := operation.Targets[ordinal].Group
	copy(raw[:16], group.ClusterID[:])
	copy(raw[16:32], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(raw[32:40], group.TopologyRecoveryEpoch)
	copy(raw[40:56], group.ShardIncarnation[:])
	copy(raw[56:], group.GroupID[:])
	return raw
}

type restoreCountingReader struct {
	reader io.Reader
	bytes  uint64
}

func (reader *restoreCountingReader) Read(dst []byte) (int, error) {
	n, err := reader.reader.Read(dst)
	reader.bytes += uint64(n)
	return n, err
}

func readRestoreBounded(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > restoreReceiptMaxBytes {
		return nil, ErrBootstrap
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, restoreReceiptMaxBytes+1))
	err = errors.Join(readErr, file.Close())
	if err != nil || len(raw) == 0 || len(raw) > restoreReceiptMaxBytes {
		return nil, errors.Join(ErrBootstrap, err)
	}
	return raw, nil
}
