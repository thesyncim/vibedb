package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	vibejson "github.com/thesyncim/vibejson"
)

var (
	ErrSchemaRollout         = errors.New("gateway: invalid replicated schema rollout")
	ErrSchemaRolloutConflict = errors.New("gateway: replicated schema rollout conflict")
)

const maxSchemaRolloutIntentBytes = 4 << 10

// These binary domains intentionally contain no public revision label. A
// rollout is compatible only when every installer reports the exact contract
// digest implemented by this grammar and state machine.
var (
	schemaRolloutContractDomain = [...]byte{
		0x56, 0x44, 0x42, 0x2d, 0x53, 0x43, 0x48, 0x45,
		0x4d, 0x41, 0x2d, 0x52, 0x4f, 0x4c, 0x4c, 0x4f,
		0x55, 0x54, 0x2d, 0x43, 0x4f, 0x4e, 0x54, 0x52,
		0x41, 0x43, 0x54, 0x2d, 0x52, 0x46, 0x33, 0x01,
	}
	schemaRolloutReceiptDomain = [...]byte{
		0x56, 0x44, 0x42, 0x2d, 0x53, 0x43, 0x48, 0x45,
		0x4d, 0x41, 0x2d, 0x50, 0x52, 0x45, 0x50, 0x41,
		0x52, 0x45, 0x44, 0x2d, 0x47, 0x52, 0x4f, 0x55,
		0x50, 0x2d, 0x52, 0x4f, 0x4f, 0x54, 0x00, 0x01,
	}
)

// SchemaRolloutContractDigest is the exact rollout/install behavior supported
// by this binary. A mixed rolling fleet fails prepare instead of guessing that
// two builds interpret a relation bundle the same way.
func SchemaRolloutContractDigest() [32]byte {
	return schemainstall.ContractDigest()
}

// SchemaRolloutPreparedGroup is the install receipt supplied by one RF3 shard
// group before the catalog is allowed to make its new relation manifest
// routable. InstallationDigest binds the replica-orchestrator's exact durable
// install result; the rollout root binds all receipts without retaining an
// O(group-count) operation value.
type SchemaRolloutPreparedGroup struct {
	Group                      raftmember.GroupKey
	AllocationGeneration       distribution.ShardAllocationGeneration
	FromSchemaGeneration       uint64
	FromRelationManifestDigest replication.Digest
	ToSchemaGeneration         uint64
	ToRelationManifestDigest   replication.Digest
	InstallationDigest         [32]byte
	ContractDigest             [32]byte
}

type schemaRolloutChange struct {
	group                      raftmember.GroupKey
	allocation                 distribution.ShardAllocationGeneration
	fromSchemaGeneration       uint64
	fromRelationManifestDigest replication.Digest
	toSchemaGeneration         uint64
	toRelationManifestDigest   replication.Digest
}

type schemaRolloutDistributionCut struct {
	fromSchemaGeneration       uint64
	fromRelationManifestDigest replication.Digest
	toSchemaGeneration         uint64
	toRelationManifestDigest   replication.Digest
}

type schemaRolloutIntent struct {
	BaseCatalogGeneration   uint64
	BaseHeadBytes           uint64
	BaseHeadDigest          [32]byte
	TargetCatalogGeneration uint64
	TargetHeadBytes         uint64
	TargetHeadDigest        [32]byte
	PreparedGroupCount      uint64
	PreparedGroupRoot       [32]byte
	ContractDigest          [32]byte
}

type persistedSchemaRolloutIntent struct {
	BaseCatalogGeneration   uint64 `json:"base_catalog_generation"`
	BaseHeadBytes           uint64 `json:"base_head_bytes"`
	BaseHeadDigest          []byte `json:"base_head_digest"`
	TargetCatalogGeneration uint64 `json:"target_catalog_generation"`
	TargetHeadBytes         uint64 `json:"target_head_bytes"`
	TargetHeadDigest        []byte `json:"target_head_digest"`
	PreparedGroupCount      uint64 `json:"prepared_group_count"`
	PreparedGroupRoot       []byte `json:"prepared_group_root"`
	ContractDigest          []byte `json:"contract_digest"`
}

func validSchemaRolloutIntent(intent schemaRolloutIntent) bool {
	return intent.BaseCatalogGeneration != 0 &&
		intent.TargetCatalogGeneration > intent.BaseCatalogGeneration &&
		intent.BaseHeadBytes != 0 && intent.TargetHeadBytes != 0 &&
		intent.BaseHeadDigest != ([32]byte{}) && intent.TargetHeadDigest != ([32]byte{}) &&
		intent.BaseHeadDigest != intent.TargetHeadDigest && intent.PreparedGroupCount != 0 &&
		intent.PreparedGroupRoot != ([32]byte{}) && intent.ContractDigest != ([32]byte{})
}

func appendSchemaRolloutIntent(dst []byte, intent schemaRolloutIntent) ([]byte, error) {
	if !validSchemaRolloutIntent(intent) {
		return dst, ErrSchemaRollout
	}
	persisted := persistedSchemaRolloutIntent{
		BaseCatalogGeneration: intent.BaseCatalogGeneration,
		BaseHeadBytes:         intent.BaseHeadBytes, BaseHeadDigest: intent.BaseHeadDigest[:],
		TargetCatalogGeneration: intent.TargetCatalogGeneration,
		TargetHeadBytes:         intent.TargetHeadBytes, TargetHeadDigest: intent.TargetHeadDigest[:],
		PreparedGroupCount: intent.PreparedGroupCount,
		PreparedGroupRoot:  intent.PreparedGroupRoot[:], ContractDigest: intent.ContractDigest[:],
	}
	raw, err := vibejson.Marshal(&persisted)
	if err != nil {
		return dst, err
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start > maxSchemaRolloutIntentBytes {
		return dst[:start], errors.Join(err, ErrSchemaRollout)
	}
	return dst, nil
}

func openSchemaRolloutIntent(raw []byte) (schemaRolloutIntent, error) {
	if len(raw) == 0 || len(raw) > maxSchemaRolloutIntentBytes {
		return schemaRolloutIntent{}, ErrSchemaRollout
	}
	var persisted persistedSchemaRolloutIntent
	if err := vibejson.Unmarshal(raw, &persisted); err != nil ||
		len(persisted.BaseHeadDigest) != sha256.Size ||
		len(persisted.TargetHeadDigest) != sha256.Size ||
		len(persisted.PreparedGroupRoot) != sha256.Size ||
		len(persisted.ContractDigest) != sha256.Size {
		return schemaRolloutIntent{}, errors.Join(err, ErrSchemaRollout)
	}
	intent := schemaRolloutIntent{
		BaseCatalogGeneration:   persisted.BaseCatalogGeneration,
		BaseHeadBytes:           persisted.BaseHeadBytes,
		TargetCatalogGeneration: persisted.TargetCatalogGeneration,
		TargetHeadBytes:         persisted.TargetHeadBytes,
		PreparedGroupCount:      persisted.PreparedGroupCount,
	}
	copy(intent.BaseHeadDigest[:], persisted.BaseHeadDigest)
	copy(intent.TargetHeadDigest[:], persisted.TargetHeadDigest)
	copy(intent.PreparedGroupRoot[:], persisted.PreparedGroupRoot)
	copy(intent.ContractDigest[:], persisted.ContractDigest)
	if !validSchemaRolloutIntent(intent) {
		return schemaRolloutIntent{}, ErrSchemaRollout
	}
	canonical, err := appendSchemaRolloutIntent(nil, intent)
	if err != nil || !bytes.Equal(raw, canonical) {
		return schemaRolloutIntent{}, errors.Join(err, ErrSchemaRollout)
	}
	return intent, nil
}

func schemaRolloutCatalogDocument(snapshot *Snapshot) ([]byte, error) {
	return appendReplicatedCatalogDocument(nil, snapshot, maxReplicatedCatalogBytes)
}

func schemaRolloutHeadMatches(
	generation, size uint64, digest [32]byte, snapshot *Snapshot, raw []byte,
) bool {
	return snapshot != nil && snapshot.Generation() == generation &&
		uint64(len(raw)) == size && sha256.Sum256(raw) == digest
}

func sameSchemaRolloutDescriptorIdentity(
	left, right ReplicatedShardDescriptor,
) bool {
	if left.Distribution != right.Distribution || left.Shard != right.Shard ||
		left.Group != right.Group || left.AllocationGeneration != right.AllocationGeneration ||
		len(left.Replicas) != len(right.Replicas) {
		return false
	}
	leftCommand, rightCommand := left.Command, right.Command
	leftCommand.SchemaGeneration, rightCommand.SchemaGeneration = 0, 0
	leftCommand.RelationManifestDigest, rightCommand.RelationManifestDigest = [32]byte{}, [32]byte{}
	if leftCommand != rightCommand || !slices.Equal(left.Replicas, right.Replicas) {
		return false
	}
	if left.EnrolledTarget == nil || right.EnrolledTarget == nil {
		return left.EnrolledTarget == nil && right.EnrolledTarget == nil
	}
	return *left.EnrolledTarget == *right.EnrolledTarget
}

func schemaRolloutChanges(base, target *Snapshot) ([]schemaRolloutChange, error) {
	if base == nil || target == nil || target.Generation() <= base.Generation() {
		return nil, ErrSchemaRollout
	}
	if _, err := advanceCatalogState(base, target); err != nil {
		return nil, errors.Join(err, ErrSchemaRollout)
	}
	baseDescriptors := base.replicatedDescriptors()
	targetDescriptors := target.replicatedDescriptors()
	if len(baseDescriptors) == 0 || len(baseDescriptors) != len(targetDescriptors) {
		return nil, ErrSchemaRollout
	}
	targetByGroup := make(map[raftmember.GroupKey]ReplicatedShardDescriptor, len(targetDescriptors))
	for index := range targetDescriptors {
		targetByGroup[targetDescriptors[index].Group] = targetDescriptors[index]
	}
	distributionCuts := make(map[distribution.DistributionName]schemaRolloutDistributionCut)
	changes := make([]schemaRolloutChange, 0, len(baseDescriptors))
	for index := range baseDescriptors {
		before := baseDescriptors[index]
		after, found := targetByGroup[before.Group]
		if !found || !sameSchemaRolloutDescriptorIdentity(before, after) {
			return nil, ErrSchemaRollout
		}
		cut := schemaRolloutDistributionCut{
			fromSchemaGeneration: before.Command.SchemaGeneration,
			fromRelationManifestDigest: replication.Digest(
				before.Command.RelationManifestDigest,
			),
			toSchemaGeneration: after.Command.SchemaGeneration,
			toRelationManifestDigest: replication.Digest(
				after.Command.RelationManifestDigest,
			),
		}
		if prior, exists := distributionCuts[before.Distribution]; exists && prior != cut {
			// One relation bundle cannot be partly old and partly new within a
			// distribution cut. This also rejects an already mixed base fleet.
			return nil, ErrSchemaRollout
		}
		distributionCuts[before.Distribution] = cut
		if cut.fromSchemaGeneration == cut.toSchemaGeneration {
			if cut.fromRelationManifestDigest != cut.toRelationManifestDigest {
				return nil, ErrSchemaRollout
			}
			continue
		}
		if cut.toSchemaGeneration < cut.fromSchemaGeneration ||
			cut.fromRelationManifestDigest == cut.toRelationManifestDigest {
			return nil, ErrSchemaRollout
		}
		changes = append(changes, schemaRolloutChange{
			group: before.Group, allocation: before.AllocationGeneration,
			fromSchemaGeneration:       cut.fromSchemaGeneration,
			fromRelationManifestDigest: cut.fromRelationManifestDigest,
			toSchemaGeneration:         cut.toSchemaGeneration,
			toRelationManifestDigest:   cut.toRelationManifestDigest,
		})
	}
	if len(changes) == 0 {
		return nil, ErrSchemaRollout
	}
	slices.SortFunc(changes, func(left, right schemaRolloutChange) int {
		return compareMembershipGrantGroup(left.group, right.group)
	})
	return changes, nil
}

func writeSchemaRolloutGroup(hash hash.Hash, scratch *[8]byte, group raftmember.GroupKey) {
	_, _ = hash.Write(group.ClusterID[:])
	_, _ = hash.Write(group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(scratch[:], group.TopologyRecoveryEpoch)
	_, _ = hash.Write(scratch[:])
	_, _ = hash.Write(group.ShardIncarnation[:])
	_, _ = hash.Write(group.GroupID[:])
}

func schemaRolloutPreparedRoot(
	changes []schemaRolloutChange, receipts []SchemaRolloutPreparedGroup,
) ([32]byte, error) {
	if len(changes) == 0 || len(receipts) != len(changes) {
		return [32]byte{}, ErrSchemaRollout
	}
	byGroup := make(map[raftmember.GroupKey]SchemaRolloutPreparedGroup, len(receipts))
	contract := SchemaRolloutContractDigest()
	for index := range receipts {
		receipt := receipts[index]
		if receipt.Group == (raftmember.GroupKey{}) ||
			receipt.AllocationGeneration == 0 || receipt.FromSchemaGeneration == 0 ||
			receipt.ToSchemaGeneration <= receipt.FromSchemaGeneration ||
			receipt.FromRelationManifestDigest == (replication.Digest{}) ||
			receipt.ToRelationManifestDigest == (replication.Digest{}) ||
			receipt.FromRelationManifestDigest == receipt.ToRelationManifestDigest ||
			receipt.InstallationDigest == ([32]byte{}) || receipt.ContractDigest != contract {
			return [32]byte{}, ErrSchemaRollout
		}
		if _, duplicate := byGroup[receipt.Group]; duplicate {
			return [32]byte{}, ErrSchemaRollout
		}
		byGroup[receipt.Group] = receipt
	}
	hasher := sha256.New()
	_, _ = hasher.Write(schemaRolloutReceiptDomain[:])
	_, _ = hasher.Write(contract[:])
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], uint64(len(changes)))
	_, _ = hasher.Write(scratch[:])
	for index := range changes {
		change := changes[index]
		receipt, found := byGroup[change.group]
		if !found || receipt.AllocationGeneration != change.allocation ||
			receipt.FromSchemaGeneration != change.fromSchemaGeneration ||
			receipt.FromRelationManifestDigest != change.fromRelationManifestDigest ||
			receipt.ToSchemaGeneration != change.toSchemaGeneration ||
			receipt.ToRelationManifestDigest != change.toRelationManifestDigest {
			return [32]byte{}, ErrSchemaRollout
		}
		writeSchemaRolloutGroup(hasher, &scratch, change.group)
		binary.BigEndian.PutUint64(scratch[:], uint64(change.allocation))
		_, _ = hasher.Write(scratch[:])
		binary.BigEndian.PutUint64(scratch[:], change.fromSchemaGeneration)
		_, _ = hasher.Write(scratch[:])
		_, _ = hasher.Write(change.fromRelationManifestDigest[:])
		binary.BigEndian.PutUint64(scratch[:], change.toSchemaGeneration)
		_, _ = hasher.Write(scratch[:])
		_, _ = hasher.Write(change.toRelationManifestDigest[:])
		_, _ = hasher.Write(receipt.InstallationDigest[:])
	}
	var root [32]byte
	hasher.Sum(root[:0])
	return root, nil
}

func schemaRolloutOperation(
	id [32]byte, baseRaw, targetRaw []byte, base, target *Snapshot,
	changes []schemaRolloutChange, preparedRoot [32]byte,
) (ReplicatedOperationRecord, error) {
	if id == ([32]byte{}) || base == nil || target == nil || len(changes) == 0 ||
		preparedRoot == ([32]byte{}) {
		return ReplicatedOperationRecord{}, ErrSchemaRollout
	}
	intent := schemaRolloutIntent{
		BaseCatalogGeneration: base.Generation(), BaseHeadBytes: uint64(len(baseRaw)),
		BaseHeadDigest: sha256.Sum256(baseRaw), TargetCatalogGeneration: target.Generation(),
		TargetHeadBytes: uint64(len(targetRaw)), TargetHeadDigest: sha256.Sum256(targetRaw),
		PreparedGroupCount: uint64(len(changes)), PreparedGroupRoot: preparedRoot,
		ContractDigest: SchemaRolloutContractDigest(),
	}
	intentRaw, err := appendSchemaRolloutIntent(nil, intent)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return ReplicatedOperationRecord{
		ID: id, Kind: ReplicatedOperationSchema, State: ReplicatedOperationPlanned,
		Revision: 1, CatalogGeneration: base.Generation(), Proof: preparedRoot,
		IntentDigest: sha256.Sum256(intentRaw), Intent: intentRaw,
	}, nil
}

func openSchemaRolloutOperation(
	record ReplicatedOperationRecord,
) (schemaRolloutIntent, error) {
	if !validReplicatedOperation(record) || record.Kind != ReplicatedOperationSchema {
		return schemaRolloutIntent{}, ErrSchemaRollout
	}
	intent, err := openSchemaRolloutIntent(record.Intent)
	if err != nil || record.CatalogGeneration != intent.BaseCatalogGeneration ||
		record.Proof != intent.PreparedGroupRoot ||
		intent.ContractDigest != SchemaRolloutContractDigest() {
		return schemaRolloutIntent{}, errors.Join(err, ErrSchemaRollout)
	}
	return intent, nil
}

// PrepareSchemaRollout publishes one compact replicated certificate only after
// every affected RF3 group proves the exact old/new schema generation and
// relation-manifest digest. The operation row remains constant-size as shard
// count grows; the target catalog's exact digest reconstructs the full plan.
func (authority *ReplicatedCatalogAuthority) PrepareSchemaRollout(
	ctx context.Context, id [32]byte, target *Snapshot,
	receipts []SchemaRolloutPreparedGroup,
) (ReplicatedOperationRecord, error) {
	if authority == nil || ctx == nil || target == nil || id == ([32]byte{}) {
		return ReplicatedOperationRecord{}, ErrSchemaRollout
	}
	if authority.session.Status().Pending {
		return ReplicatedOperationRecord{}, ErrReplicatedCatalogPending
	}
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	targetRaw, err := schemaRolloutCatalogDocument(target)
	if err != nil {
		return ReplicatedOperationRecord{}, errors.Join(err, ErrSchemaRollout)
	}
	changes, err := schemaRolloutChanges(cut.snapshot, target)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	preparedRoot, err := schemaRolloutPreparedRoot(changes, receipts)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	record, err := schemaRolloutOperation(
		id, cut.head, targetRaw, cut.snapshot, target, changes, preparedRoot,
	)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	if err = authority.SubmitOperation(ctx, record); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return record, nil
}

func validateSchemaRolloutTarget(
	intent schemaRolloutIntent, base, target *Snapshot, targetRaw []byte,
) error {
	if !schemaRolloutHeadMatches(
		intent.TargetCatalogGeneration, intent.TargetHeadBytes,
		intent.TargetHeadDigest, target, targetRaw,
	) {
		return ErrSchemaRolloutConflict
	}
	if base == nil {
		return nil
	}
	changes, err := schemaRolloutChanges(base, target)
	if err != nil || uint64(len(changes)) != intent.PreparedGroupCount {
		return errors.Join(err, ErrSchemaRolloutConflict)
	}
	return nil
}

// AuthorizeSchemaRollout crosses the no-return boundary without publishing the
// target catalog. A controller must durably reach this state before issuing
// shard activation certificates; after it succeeds Abort is forbidden.
func (authority *ReplicatedCatalogAuthority) AuthorizeSchemaRollout(
	ctx context.Context, id [32]byte, target *Snapshot,
) (ReplicatedOperationRecord, error) {
	if authority == nil || ctx == nil || target == nil || id == ([32]byte{}) {
		return ReplicatedOperationRecord{}, ErrSchemaRollout
	}
	if authority.session.Status().Pending {
		return ReplicatedOperationRecord{}, ErrReplicatedCatalogPending
	}
	record, err := authority.ReadOperation(ctx, id)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	intent, err := openSchemaRolloutOperation(record)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	if record.State == ReplicatedOperationCancelled {
		return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
	}
	targetRaw, err := schemaRolloutCatalogDocument(target)
	if err != nil {
		return ReplicatedOperationRecord{}, errors.Join(err, ErrSchemaRollout)
	}
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	baseActive := schemaRolloutHeadMatches(
		intent.BaseCatalogGeneration, intent.BaseHeadBytes,
		intent.BaseHeadDigest, cut.snapshot, cut.head,
	)
	targetActive := schemaRolloutHeadMatches(intent.TargetCatalogGeneration,
		intent.TargetHeadBytes, intent.TargetHeadDigest, cut.snapshot, cut.head)
	if record.State == ReplicatedOperationComplete {
		if !targetActive {
			return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
		}
		return record, nil
	}
	if record.State == ReplicatedOperationRunning {
		if !baseActive && !targetActive {
			return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
		}
		return record, nil
	}
	if !baseActive {
		return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
	}
	if err = validateSchemaRolloutTarget(intent, cut.snapshot, target, targetRaw); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	if record.State != ReplicatedOperationPlanned {
		return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
	}
	running := record
	running.State, running.Revision = ReplicatedOperationRunning, record.Revision+1
	if err = authority.PublishOperation(ctx, record.Revision, running); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return running, nil
}

// CommitSchemaRollout CAS-publishes the exact target only after the separate
// Running witness proves shard activation was authorized. A restart can resume
// from either side of the catalog CAS without selecting a different target.
func (authority *ReplicatedCatalogAuthority) CommitSchemaRollout(
	ctx context.Context, id [32]byte, target *Snapshot,
) (ReplicatedOperationRecord, error) {
	if authority == nil || ctx == nil || target == nil || id == ([32]byte{}) {
		return ReplicatedOperationRecord{}, ErrSchemaRollout
	}
	if authority.session.Status().Pending {
		return ReplicatedOperationRecord{}, ErrReplicatedCatalogPending
	}
	record, err := authority.ReadOperation(ctx, id)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	intent, err := openSchemaRolloutOperation(record)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	if record.State == ReplicatedOperationCancelled || record.State == ReplicatedOperationPlanned {
		return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
	}
	targetRaw, err := schemaRolloutCatalogDocument(target)
	if err != nil {
		return ReplicatedOperationRecord{}, errors.Join(err, ErrSchemaRollout)
	}
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	baseActive := schemaRolloutHeadMatches(intent.BaseCatalogGeneration, intent.BaseHeadBytes,
		intent.BaseHeadDigest, cut.snapshot, cut.head)
	targetActive := schemaRolloutHeadMatches(intent.TargetCatalogGeneration, intent.TargetHeadBytes,
		intent.TargetHeadDigest, cut.snapshot, cut.head)
	if !baseActive && !targetActive {
		return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
	}
	var base *Snapshot
	if baseActive {
		base = cut.snapshot
	}
	if err = validateSchemaRolloutTarget(intent, base, target, targetRaw); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	if record.State == ReplicatedOperationComplete {
		if !targetActive {
			return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
		}
		return record, nil
	}
	if record.State != ReplicatedOperationRunning {
		return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
	}
	if baseActive {
		if err = authority.Publish(ctx, intent.BaseCatalogGeneration, target); err != nil {
			return ReplicatedOperationRecord{}, err
		}
	} else {
		if _, err = authority.publishReadCatalogCut(ctx, cut.snapshot, cut.head); err != nil {
			return ReplicatedOperationRecord{}, err
		}
	}
	complete := record
	complete.State, complete.Revision = ReplicatedOperationComplete, record.Revision+1
	if err = authority.PublishOperation(ctx, record.Revision, complete); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return complete, nil
}

// ActivateSchemaRollout preserves the single-process convenience API while
// using the same two durable boundaries required by the network controller.
func (authority *ReplicatedCatalogAuthority) ActivateSchemaRollout(
	ctx context.Context, id [32]byte, target *Snapshot,
) (ReplicatedOperationRecord, error) {
	if _, err := authority.AuthorizeSchemaRollout(ctx, id, target); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return authority.CommitSchemaRollout(ctx, id, target)
}

// AbortSchemaRollout is a forward-safe rollback of a prepared plan. Once the
// Running witness exists, activation is authorized and an old controller may
// still complete it, so abort refuses to race that boundary.
func (authority *ReplicatedCatalogAuthority) AbortSchemaRollout(
	ctx context.Context, id [32]byte,
) (ReplicatedOperationRecord, error) {
	if authority == nil || ctx == nil || id == ([32]byte{}) {
		return ReplicatedOperationRecord{}, ErrSchemaRollout
	}
	if authority.session.Status().Pending {
		return ReplicatedOperationRecord{}, ErrReplicatedCatalogPending
	}
	record, err := authority.ReadOperation(ctx, id)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	intent, err := openSchemaRolloutOperation(record)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	if record.State == ReplicatedOperationCancelled {
		return record, nil
	}
	if record.State != ReplicatedOperationPlanned {
		return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
	}
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	if !schemaRolloutHeadMatches(
		intent.BaseCatalogGeneration, intent.BaseHeadBytes,
		intent.BaseHeadDigest, cut.snapshot, cut.head,
	) {
		return ReplicatedOperationRecord{}, ErrSchemaRolloutConflict
	}
	cancelled := record
	cancelled.State, cancelled.Revision = ReplicatedOperationCancelled, record.Revision+1
	if err = authority.PublishOperation(ctx, record.Revision, cancelled); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return cancelled, nil
}
