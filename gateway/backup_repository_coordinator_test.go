package gateway

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

func TestBackupRepositoryCoordinatorSettlesAndResumesCatalogPublication(t *testing.T) {
	payloads := [][]byte{bytes.Repeat([]byte{1}, 8192), bytes.Repeat([]byte{2}, 12288)}
	groups := []raftmember.GroupKey{backupTestGroup(1), backupTestGroup(2)}
	cut := clusterbackup.CatalogCut{Generation: 7, Digest: backupTest32(8), PolicyGeneration: 9, Groups: groups}
	record, err := NewBackupOperation(backupTest32(10), cut)
	if err != nil {
		t.Fatal(err)
	}
	cuts := []clusterbackup.GroupCut{backupTestCut(groups[0], 11), backupTestCut(groups[1], 12)}
	inputs := make([]clusterbackup.ArtifactInput, len(payloads))
	for index := range payloads {
		cuts[index].ArtifactBytes = uint64(len(payloads[index]))
		cuts[index].ArtifactHash = sha256.Sum256(payloads[index])
		inputs[index].Reader = bytes.NewReader(payloads[index])
	}
	certificate, err := clusterbackup.Certify(record.ID, cut, cuts)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := clusterbackup.OpenBackupRepository(t.TempDir(), clusterbackup.RepositoryLimits{
		MaxBackups: 2, MaxArtifacts: 8, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	authority := new(backupAuthorityStub)
	lifecycle := backupTestController(t, authority)
	if err = lifecycle.Submit(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewBackupRepositoryCoordinator(lifecycle, repository)
	if err != nil {
		t.Fatal(err)
	}
	authority.failAfterApply = true
	exported, err := coordinator.Publish(t.Context(), record, certificate, inputs...)
	if err != nil || exported.Cursor[0] != backupStageExported ||
		backupCertificateDigest(exported.Cursor) != certificate.Digest {
		t.Fatalf("exported=%+v err=%v", exported, err)
	}

	// A replacement process uses only the replicated record and repository.
	replacement := backupTestController(t, authority)
	coordinator, err = NewBackupRepositoryCoordinator(replacement, repository)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := coordinator.Publish(t.Context(), exported, certificate)
	if err != nil || !replayed.Equal(exported) {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
}

func TestBackupCertificateMustMatchCatalogIntent(t *testing.T) {
	group := backupTestGroup(1)
	cut := clusterbackup.CatalogCut{Generation: 7, Digest: backupTest32(8), PolicyGeneration: 9,
		Groups: []raftmember.GroupKey{group}}
	record, err := NewBackupOperation(backupTest32(10), cut)
	if err != nil {
		t.Fatal(err)
	}
	wrong := cut
	wrong.Digest = backupTest32(99)
	certificate, err := clusterbackup.Certify(record.ID, wrong, []clusterbackup.GroupCut{backupTestCut(group, 11)})
	if err != nil {
		t.Fatal(err)
	}
	controller := backupTestController(t, new(backupAuthorityStub))
	if _, err = controller.PublishCertified(t.Context(), record, certificate, 1024); err != ErrBackupOperation {
		t.Fatalf("wrong catalog digest err=%v", err)
	}
}

func TestBackupRepositoryCoordinatorRejectsCorruptRestoreBeforeStaging(t *testing.T) {
	payload := bytes.Repeat([]byte{7}, 4096)
	group := backupTestGroup(1)
	cut := clusterbackup.CatalogCut{Generation: 7, Digest: backupTest32(8), PolicyGeneration: 9,
		Groups: []raftmember.GroupKey{group}}
	record, err := NewBackupOperation(backupTest32(10), cut)
	if err != nil {
		t.Fatal(err)
	}
	groupCut := backupTestCut(group, 11)
	groupCut.ArtifactBytes = uint64(len(payload))
	groupCut.ArtifactHash = sha256.Sum256(payload)
	certificate, err := clusterbackup.Certify(record.ID, cut, []clusterbackup.GroupCut{groupCut})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := clusterbackup.OpenBackupRepository(filepath.Join(t.TempDir(), "backup"),
		clusterbackup.RepositoryLimits{MaxBackups: 2, MaxArtifacts: 4,
			MaxArtifactBytes: 1 << 20, MaxDiskBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	authority := new(backupAuthorityStub)
	lifecycle := backupTestController(t, authority)
	if err = lifecycle.Submit(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewBackupRepositoryCoordinator(lifecycle, repository)
	if err != nil {
		t.Fatal(err)
	}
	exported, err := coordinator.Publish(t.Context(), record, certificate,
		clusterbackup.ArtifactInput{Reader: bytes.NewReader(payload)})
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	_, _, err = coordinator.StageRestore(t.Context(), exported, certificate, RestoreStagingOptions{
		Path: target, RepositoryLimits: clusterbackup.RepositoryLimits{MaxBackups: 2, MaxArtifacts: 4,
			MaxArtifactBytes: 1 << 20, MaxDiskBytes: 4 << 20},
		Restore: backupTest32(20), TargetClusterID: backupTest16(21),
		TargetClusterIncarnation: backupTest16(22), MaxArtifactBytes: 1 << 20,
		MaxTotalBytes: 4 << 20, PayloadBuffer: make([]byte, 64<<10)})
	if !errors.Is(err, clusterbackup.ErrRestoreVerify) || authority.record.Cursor[0] != backupStageExported {
		t.Fatalf("corrupt restore err=%v record=%+v", err, authority.record)
	}
}
