package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

type backupAuthorityStub struct {
	record         ReplicatedOperationRecord
	failAfterApply bool
}

func (authority *backupAuthorityStub) ReadOperation(context.Context, [32]byte) (ReplicatedOperationRecord, error) {
	return authority.record, nil
}
func (authority *backupAuthorityStub) SubmitOperation(_ context.Context, record ReplicatedOperationRecord) error {
	if authority.record.ID != ([32]byte{}) {
		return ErrReplicatedCatalogConflict
	}
	authority.record = record
	return nil
}
func (authority *backupAuthorityStub) AdvanceOperation(_ context.Context, expected uint64, record ReplicatedOperationRecord) error {
	if authority.record.Revision == record.Revision && authority.record.Equal(record) {
		return nil
	}
	if authority.record.Revision != expected {
		return ErrReplicatedCatalogConflict
	}
	authority.record = record
	if authority.failAfterApply {
		authority.failAfterApply = false
		return ErrReplicatedCatalogPending
	}
	return nil
}

func backupTest32(value byte) (result [32]byte) {
	for index := range result {
		result[index] = value
	}
	return
}
func backupTest16(value byte) (result [16]byte) {
	for index := range result {
		result[index] = value
	}
	return
}
func backupTestGroup(value byte) raftmember.GroupKey {
	return raftmember.GroupKey{ClusterID: backupTest16(1),
		ClusterIncarnation: backupTest16(2), TopologyRecoveryEpoch: 3,
		ShardIncarnation: backupTest16(value), GroupID: backupTest16(value)}
}
func backupTestCut(group raftmember.GroupKey, value byte) clusterbackup.GroupCut {
	return clusterbackup.GroupCut{Group: group, SourceMember: 1, SchemaGeneration: 2,
		ReplicaSetVersion: 3, SnapshotIndex: 4, SnapshotTerm: 5, Lineage: backupTest32(value),
		RelationManifestDigest: backupTest32(value + 1), ArtifactHash: backupTest32(value + 2),
		ArtifactBytes: 4096, ArtifactManifestDigest: backupTest32(value + 3)}
}

func TestBackupOperationCatalogLifecycleResumesOutcomeUnknown(t *testing.T) {
	groups := []raftmember.GroupKey{backupTestGroup(1), backupTestGroup(2)}
	cut := clusterbackup.CatalogCut{Generation: 7, Digest: backupTest32(8), PolicyGeneration: 9, Groups: groups}
	record, err := NewBackupOperation(backupTest32(10), cut)
	if err != nil || record.Kind != ReplicatedOperationBackup || record.Cursor[1] != 2 {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	authority := new(backupAuthorityStub)
	controller := BackupOperationController{Authority: authority}
	if err = controller.Submit(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	certificate, err := clusterbackup.Certify(record.ID, cut, []clusterbackup.GroupCut{
		backupTestCut(groups[0], 11), backupTestCut(groups[1], 12)})
	if err != nil {
		t.Fatal(err)
	}
	authority.failAfterApply = true
	if _, err = controller.PublishCertified(t.Context(), record, certificate, 1024); !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("first certified err=%v", err)
	}
	replayed, err := authority.ReadOperation(t.Context(), record.ID)
	if err != nil || replayed.Cursor[0] != backupStageCertified {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
	// A replacement controller resumes from the catalog record, never local phase state.
	controller = BackupOperationController{Authority: authority}
	exported, err := controller.PublishExported(t.Context(), replayed, backupTest32(20))
	if err != nil || exported.Cursor[0] != backupStageExported {
		t.Fatalf("exported=%+v err=%v", exported, err)
	}
	permit := clusterbackup.RestoreStagingPermit{Restore: backupTest32(21), CertificateDigest: certificate.Digest, Groups: 2}
	staged, err := controller.PublishRestoreStaged(t.Context(), exported, permit)
	if err != nil || staged.Cursor[0] != backupStageRestoreStaged {
		t.Fatalf("staged=%+v err=%v", staged, err)
	}
	complete, err := controller.Complete(t.Context(), staged)
	if err != nil || complete.State != ReplicatedOperationComplete {
		t.Fatalf("complete=%+v err=%v", complete, err)
	}
}

func TestBackupOperationRejectsPartialCertificateAndWrongCatalog(t *testing.T) {
	groups := []raftmember.GroupKey{backupTestGroup(1), backupTestGroup(2)}
	cut := clusterbackup.CatalogCut{Generation: 7, Digest: backupTest32(8), PolicyGeneration: 9, Groups: groups}
	record, err := NewBackupOperation(backupTest32(10), cut)
	if err != nil {
		t.Fatal(err)
	}
	partialCut := clusterbackup.CatalogCut{Generation: 7, Digest: backupTest32(8), PolicyGeneration: 9, Groups: groups[:1]}
	certificate, err := clusterbackup.Certify(record.ID, partialCut, []clusterbackup.GroupCut{backupTestCut(groups[0], 11)})
	if err != nil {
		t.Fatal(err)
	}
	controller := BackupOperationController{Authority: new(backupAuthorityStub)}
	if _, err = controller.PublishCertified(t.Context(), record, certificate, 1024); !errors.Is(err, ErrBackupOperation) {
		t.Fatalf("partial certificate err=%v", err)
	}
	certificate.CatalogGeneration++
	if _, err = controller.PublishCertified(t.Context(), record, certificate, 1024); !errors.Is(err, ErrBackupOperation) {
		t.Fatalf("wrong catalog err=%v", err)
	}
}
