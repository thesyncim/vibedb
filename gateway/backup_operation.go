package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

var ErrBackupOperation = errors.New("gateway: invalid backup operation")

// BackupCatalogCut derives the complete RF3 inventory from one immutable
// catalog snapshot, including reserved replicated routes that are not exposed
// as user tables. Routes and Groups are returned in the same portable order.
func BackupCatalogCut(snapshot *Snapshot) (clusterbackup.CatalogCut, []ReplicatedRoute, error) {
	if snapshot == nil || snapshot.Generation() == 0 || snapshot.ReplicatedRouteCount() == 0 ||
		snapshot.ReplicatedRouteCount() > clusterbackup.AbsoluteMaxGroupCuts {
		return clusterbackup.CatalogCut{}, nil, ErrBackupOperation
	}
	digest, err := CatalogSnapshotDigest(snapshot)
	if err != nil || digest == ([sha256.Size]byte{}) {
		return clusterbackup.CatalogCut{}, nil, errors.Join(ErrBackupOperation, err)
	}
	routes := make([]ReplicatedRoute, snapshot.ReplicatedRouteCount())
	for index := range routes {
		var workspace [ServingReplicaCount]ReplicatedEndpoint
		route, ok := snapshot.ReplicatedRouteAt(index, workspace[:0])
		if !ok || route.Command.ActivePolicyGeneration == 0 {
			return clusterbackup.CatalogCut{}, nil, ErrBackupOperation
		}
		route.Replicas = append([]ReplicatedEndpoint(nil), route.Replicas...)
		routes[index] = route
	}
	slices.SortFunc(routes, func(left, right ReplicatedRoute) int {
		return compareBackupGroup(left.Group, right.Group)
	})
	groups := make([]raftmember.GroupKey, len(routes))
	policy := routes[0].Command.ActivePolicyGeneration
	for index, route := range routes {
		if route.Command.ActivePolicyGeneration != policy ||
			index != 0 && compareBackupGroup(routes[index-1].Group, route.Group) >= 0 {
			return clusterbackup.CatalogCut{}, nil, ErrBackupOperation
		}
		groups[index] = route.Group
	}
	return clusterbackup.CatalogCut{Generation: snapshot.Generation(), Digest: digest,
		PolicyGeneration: policy, Groups: groups}, routes, nil
}

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
		if !validBackupGroup(group) || index != 0 && compareBackupGroup(cut.Groups[index-1], group) >= 0 {
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
type BackupOperationController struct {
	authority BackupOperationAuthority
	gate      *serviceauthz.Gate
	operator  serviceauthz.Authority
}

func NewBackupOperationController(authority BackupOperationAuthority, gate *serviceauthz.Gate,
	operator serviceauthz.Authority) (*BackupOperationController, error) {
	if authority == nil || gate == nil || !operator.Valid() ||
		gate.CheckAuthority(operator, serviceauthz.CapabilityBackup) != serviceauthz.DecisionAllow {
		return nil, ErrBackupOperation
	}
	return &BackupOperationController{authority: authority, gate: gate, operator: operator}, nil
}

func (controller *BackupOperationController) authorized() bool {
	return controller != nil && controller.authority != nil && controller.gate != nil &&
		controller.gate.CheckAuthority(controller.operator, serviceauthz.CapabilityBackup) == serviceauthz.DecisionAllow
}

func (controller *BackupOperationController) Submit(ctx context.Context, record ReplicatedOperationRecord) error {
	if !controller.authorized() || !validBackupRecord(record, backupStageCollecting) {
		return ErrBackupOperation
	}
	return controller.authority.SubmitOperation(ctx, record)
}

func (controller *BackupOperationController) PublishCertified(ctx context.Context,
	record ReplicatedOperationRecord, certificate clusterbackup.Certificate, certificateBytes uint64,
) (ReplicatedOperationRecord, error) {
	if !controller.authorized() || !validBackupRecord(record, backupStageCollecting) ||
		certificate.Digest == ([32]byte{}) || certificate.Operation != record.ID ||
		certificate.CatalogGeneration != record.CatalogGeneration || certificateBytes == 0 ||
		len(certificate.Groups) != int(record.Cursor[1]) || !backupCertificateMatchesIntent(record, certificate) {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	next := record
	next.State = ReplicatedOperationRunning
	next.Revision++
	next.Cursor[0] = backupStageCertified
	next.Cursor[2] = uint64(len(certificate.Groups))
	next.Cursor[3] = certificateBytes
	putBackupCertificateDigest(&next.Cursor, certificate.Digest)
	next.Proof = certificate.Digest
	if err := controller.authority.AdvanceOperation(ctx, record.Revision, next); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return next, nil
}

func backupCertificateMatchesIntent(record ReplicatedOperationRecord, certificate clusterbackup.Certificate) bool {
	var intent backupOperationIntent
	if vibejson.Unmarshal(record.Intent, &intent) != nil || len(intent.CatalogDigest) != sha256.Size ||
		len(intent.InventoryDigest) != sha256.Size || intent.PolicyGeneration == 0 ||
		intent.GroupCount != uint64(len(certificate.Groups)) ||
		!bytes.Equal(intent.CatalogDigest, certificate.CatalogDigest[:]) ||
		intent.PolicyGeneration != certificate.PolicyGeneration {
		return false
	}
	groups := make([]raftmember.GroupKey, len(certificate.Groups))
	for index := range certificate.Groups {
		groups[index] = certificate.Groups[index].Group
	}
	digest := backupInventoryDigest(groups)
	return bytes.Equal(intent.InventoryDigest, digest[:])
}

func (controller *BackupOperationController) PublishExported(ctx context.Context,
	record ReplicatedOperationRecord, exportDigest [sha256.Size]byte,
) (ReplicatedOperationRecord, error) {
	certificateDigest := backupCertificateDigest(record.Cursor)
	if !controller.authorized() || !validBackupRecord(record, backupStageCertified) ||
		certificateDigest == ([sha256.Size]byte{}) || record.Proof != certificateDigest ||
		exportDigest == ([sha256.Size]byte{}) {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	next := record
	next.Revision++
	next.Cursor[0] = backupStageExported
	next.Proof = exportDigest
	if err := controller.authority.AdvanceOperation(ctx, record.Revision, next); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return next, nil
}

func (controller *BackupOperationController) PublishRestoreStaged(ctx context.Context,
	record ReplicatedOperationRecord, permit clusterbackup.RestoreStagingPermit,
) (ReplicatedOperationRecord, error) {
	certificateDigest := backupCertificateDigest(record.Cursor)
	if !controller.authorized() || !validBackupRecord(record, backupStageExported) ||
		certificateDigest == ([sha256.Size]byte{}) ||
		permit.CertificateDigest != certificateDigest || permit.Groups != uint32(record.Cursor[1]) {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	proof := backupRestoreProof(permit)
	next := record
	next.Revision++
	next.Cursor[0] = backupStageRestoreStaged
	next.Proof = proof
	if err := controller.authority.AdvanceOperation(ctx, record.Revision, next); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return next, nil
}

func backupRestoreProof(permit clusterbackup.RestoreStagingPermit) [sha256.Size]byte {
	var proofInput [sha256.Size * 2]byte
	copy(proofInput[:sha256.Size], permit.Restore[:])
	copy(proofInput[sha256.Size:], permit.CertificateDigest[:])
	return sha256.Sum256(proofInput[:])
}

func (controller *BackupOperationController) Complete(ctx context.Context, record ReplicatedOperationRecord) (ReplicatedOperationRecord, error) {
	if !controller.authorized() || !validBackupRecord(record, backupStageExported) && !validBackupRecord(record, backupStageRestoreStaged) {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	next := record
	next.Revision++
	next.State = ReplicatedOperationComplete
	if err := controller.authority.AdvanceOperation(ctx, record.Revision, next); err != nil {
		return ReplicatedOperationRecord{}, err
	}
	return next, nil
}

func validBackupRecord(record ReplicatedOperationRecord, stage uint64) bool {
	return validReplicatedOperation(record) && record.Kind == ReplicatedOperationBackup &&
		record.State < ReplicatedOperationComplete && record.Cursor[0] == stage && record.Cursor[1] != 0
}

func validBackupGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func compareBackupGroup(left, right raftmember.GroupKey) int {
	var a, b [72]byte
	offset := 0
	for _, pair := range [][2][]byte{{left.ClusterID[:], right.ClusterID[:]},
		{left.ClusterIncarnation[:], right.ClusterIncarnation[:]}} {
		copy(a[offset:], pair[0])
		copy(b[offset:], pair[1])
		offset += len(pair[0])
	}
	binary.BigEndian.PutUint64(a[offset:offset+8], left.TopologyRecoveryEpoch)
	binary.BigEndian.PutUint64(b[offset:offset+8], right.TopologyRecoveryEpoch)
	offset += 8
	copy(a[offset:offset+16], left.ShardIncarnation[:])
	copy(b[offset:offset+16], right.ShardIncarnation[:])
	offset += 16
	copy(a[offset:], left.GroupID[:])
	copy(b[offset:], right.GroupID[:])
	return bytesCompare72(a, b)
}

func bytesCompare72(left, right [72]byte) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

func putBackupCertificateDigest(cursor *[8]uint64, digest [sha256.Size]byte) {
	for index := range 4 {
		cursor[index+4] = binary.BigEndian.Uint64(digest[index*8 : index*8+8])
	}
}

func backupCertificateDigest(cursor [8]uint64) (digest [sha256.Size]byte) {
	for index := range 4 {
		binary.BigEndian.PutUint64(digest[index*8:index*8+8], cursor[index+4])
	}
	return digest
}

// BackupOperationCertificateDigest returns the immutable repository certificate
// selected after certificate publication. Later lifecycle proofs cannot replace
// this identity because it remains encoded in the replicated cursor.
func BackupOperationCertificateDigest(record ReplicatedOperationRecord) ([sha256.Size]byte, bool) {
	if record.Kind != ReplicatedOperationBackup || record.ID == ([sha256.Size]byte{}) ||
		record.Revision < 2 || record.Cursor[0] < backupStageCertified ||
		record.Cursor[0] > backupStageRestoreStaged || record.Cursor[1] == 0 {
		return [sha256.Size]byte{}, false
	}
	digest := backupCertificateDigest(record.Cursor)
	return digest, digest != ([sha256.Size]byte{})
}
