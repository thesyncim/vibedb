package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/raftmember"
	vibejson "github.com/thesyncim/vibejson"
)

var ErrBackupOperation = errors.New("gateway: invalid backup operation")

const (
	backupStageCollecting uint64 = iota + 1
	backupStageCertified
	backupStageExported
	backupStageRestoreStaged
)

type backupOperationIntent struct {
	CatalogDigest    []byte `json:"catalog_digest"`
	PolicyGeneration uint64 `json:"policy_generation"`
	GroupCount       uint64 `json:"group_count"`
	InventoryDigest  []byte `json:"inventory_digest"`
}

func backupInventoryDigest(groups []raftmember.GroupKey) [sha256.Size]byte {
	hash := sha256.New()
	var scalar [8]byte
	for _, group := range groups {
		hash.Write(group.ClusterID[:])
		hash.Write(group.ClusterIncarnation[:])
		binary.BigEndian.PutUint64(scalar[:], group.TopologyRecoveryEpoch)
		hash.Write(scalar[:])
		hash.Write(group.ShardIncarnation[:])
		hash.Write(group.GroupID[:])
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

// NewBackupOperation binds a new replicated lifecycle to one exact complete
// catalog inventory. The intent retains only a constant-size inventory digest;
// the certified vector lives in the bounded backup repository.
func NewBackupOperation(id [sha256.Size]byte, cut clusterbackup.CatalogCut) (ReplicatedOperationRecord, error) {
	if id == ([sha256.Size]byte{}) || cut.Generation == 0 || cut.Digest == ([sha256.Size]byte{}) ||
		cut.PolicyGeneration == 0 || len(cut.Groups) == 0 || len(cut.Groups) > clusterbackup.AbsoluteMaxGroupCuts {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	for index, group := range cut.Groups {
		if index != 0 && bytes.Compare(group.GroupID[:], cut.Groups[index-1].GroupID[:]) <= 0 {
			return ReplicatedOperationRecord{}, ErrBackupOperation
		}
	}
	inventory := backupInventoryDigest(cut.Groups)
	intent, err := vibejson.Marshal(&backupOperationIntent{CatalogDigest: cut.Digest[:],
		PolicyGeneration: cut.PolicyGeneration, GroupCount: uint64(len(cut.Groups)),
		InventoryDigest: inventory[:]})
	if err != nil {
		return ReplicatedOperationRecord{}, errors.Join(ErrBackupOperation, err)
	}
	return ReplicatedOperationRecord{ID: id, Kind: ReplicatedOperationBackup,
		State: ReplicatedOperationPlanned, Revision: 1, CatalogGeneration: cut.Generation,
		Cursor: [8]uint64{backupStageCollecting, uint64(len(cut.Groups))}, Proof: inventory,
		IntentDigest: sha256.Sum256(intent), Intent: intent}, nil
}

type BackupOperationAuthority interface {
	ReadOperation(context.Context, [32]byte) (ReplicatedOperationRecord, error)
	SubmitOperation(context.Context, ReplicatedOperationRecord) error
	AdvanceOperation(context.Context, uint64, ReplicatedOperationRecord) error
}

// BackupOperationController advances only after the external repository has
// durably published the exact certificate/artifacts represented by each proof.
type BackupOperationController struct{ Authority BackupOperationAuthority }

func (controller BackupOperationController) Submit(ctx context.Context, record ReplicatedOperationRecord) error {
	if controller.Authority == nil || !validBackupRecord(record, backupStageCollecting) {
		return ErrBackupOperation
	}
	return controller.Authority.SubmitOperation(ctx, record)
}

func (controller BackupOperationController) PublishCertified(ctx context.Context,
	record ReplicatedOperationRecord, certificate clusterbackup.Certificate, certificateBytes uint64,
) (ReplicatedOperationRecord, error) {
	if controller.Authority == nil || !validBackupRecord(record, backupStageCollecting) ||
		certificate.Digest == ([32]byte{}) || certificate.Operation != record.ID ||
		certificate.CatalogGeneration != record.CatalogGeneration || certificateBytes == 0 ||
		len(certificate.Groups) != int(record.Cursor[1]) {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	next := record
	next.State = ReplicatedOperationRunning
	next.Revision++
	next.Cursor[0] = backupStageCertified
	next.Cursor[2] = uint64(len(certificate.Groups))
	next.Cursor[3] = certificateBytes
	next.Proof = certificate.Digest
	if err := controller.Authority.AdvanceOperation(ctx, record.Revision, next); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return next, nil
}

func (controller BackupOperationController) PublishExported(ctx context.Context,
	record ReplicatedOperationRecord, exportDigest [sha256.Size]byte,
) (ReplicatedOperationRecord, error) {
	if controller.Authority == nil || !validBackupRecord(record, backupStageCertified) ||
		exportDigest == ([sha256.Size]byte{}) {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	next := record
	next.Revision++
	next.Cursor[0] = backupStageExported
	next.Proof = exportDigest
	if err := controller.Authority.AdvanceOperation(ctx, record.Revision, next); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return next, nil
}

func (controller BackupOperationController) PublishRestoreStaged(ctx context.Context,
	record ReplicatedOperationRecord, permit clusterbackup.RestoreStagingPermit,
) (ReplicatedOperationRecord, error) {
	if controller.Authority == nil || !validBackupRecord(record, backupStageExported) ||
		permit.CertificateDigest == ([32]byte{}) || permit.Groups != uint32(record.Cursor[1]) {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	var proofInput [sha256.Size * 2]byte
	copy(proofInput[:sha256.Size], permit.Restore[:])
	copy(proofInput[sha256.Size:], permit.CertificateDigest[:])
	proof := sha256.Sum256(proofInput[:])
	next := record
	next.Revision++
	next.Cursor[0] = backupStageRestoreStaged
	next.Proof = proof
	if err := controller.Authority.AdvanceOperation(ctx, record.Revision, next); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return next, nil
}

func (controller BackupOperationController) Complete(ctx context.Context, record ReplicatedOperationRecord) (ReplicatedOperationRecord, error) {
	if controller.Authority == nil || !validBackupRecord(record, backupStageExported) && !validBackupRecord(record, backupStageRestoreStaged) {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	next := record
	next.Revision++
	next.State = ReplicatedOperationComplete
	if err := controller.Authority.AdvanceOperation(ctx, record.Revision, next); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return next, nil
}

func validBackupRecord(record ReplicatedOperationRecord, stage uint64) bool {
	return validReplicatedOperation(record) && record.Kind == ReplicatedOperationBackup &&
		record.State < ReplicatedOperationComplete && record.Cursor[0] == stage && record.Cursor[1] != 0
}
