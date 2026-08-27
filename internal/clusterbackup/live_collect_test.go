package clusterbackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

type liveCollectExporter struct {
	payload []byte
	cut     GroupCut
	err     error
}

func (exporter liveCollectExporter) Export(_ context.Context, request LiveRequest, writer io.Writer) (GroupCut, error) {
	_, err := writer.Write(exporter.payload)
	if err != nil {
		return GroupCut{}, err
	}
	cut := exporter.cut
	cut.Group, cut.SourceMember = request.Group, request.SourceMember
	cut.ArtifactBytes = uint64(len(exporter.payload))
	cut.ArtifactHash = sha256.Sum256(exporter.payload)
	return cut, errors.Join(exporter.err)
}

func TestCollectLivePublishesCompleteVectorWithoutSecondDiskCopy(t *testing.T) {
	payloads := [][]byte{bytes.Repeat([]byte{1}, 8192), bytes.Repeat([]byte{2}, 12288)}
	groups := []raftmember.GroupKey{backupGroup(1), backupGroup(2)}
	authority := CatalogCut{Generation: 7, Digest: filled32(8), PolicyGeneration: 9, Groups: groups}
	sources := make([]LiveArtifactSource, len(groups))
	for index := range groups {
		sources[index] = LiveArtifactSource{Group: groups[index], SourceMember: uint64(index + 1),
			Exporter: liveCollectExporter{payload: payloads[index], cut: backupCut(byte(index + 1))}}
	}
	repository, err := OpenBackupRepository(t.TempDir(), repositoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	certificate, err := repository.CollectLive(t.Context(), filled32(10), authority, sources)
	if err != nil || len(certificate.Groups) != 2 || repository.Stats().Artifacts != 2 {
		t.Fatalf("certificate=%+v stats=%+v err=%v", certificate, repository.Stats(), err)
	}
	for index, payload := range payloads {
		artifact, err := repository.OpenArtifact(certificate.Digest, index)
		if err != nil {
			t.Fatal(err)
		}
		got, readErr := io.ReadAll(artifact)
		if closeErr := artifact.Close(); readErr != nil || closeErr != nil || !bytes.Equal(got, payload) {
			t.Fatalf("artifact %d bytes=%d read=%v close=%v", index, len(got), readErr, closeErr)
		}
	}
}

func TestCollectLiveFailureAndCrashDraftRecoveryRemainUnpublished(t *testing.T) {
	root := t.TempDir()
	group := backupGroup(1)
	authority := CatalogCut{Generation: 7, Digest: filled32(8), PolicyGeneration: 9,
		Groups: []raftmember.GroupKey{group}}
	repository, err := OpenBackupRepository(root, repositoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	source := LiveArtifactSource{Group: group, SourceMember: 1,
		Exporter: liveCollectExporter{payload: bytes.Repeat([]byte{1}, 8192), cut: backupCut(1), err: io.ErrUnexpectedEOF}}
	if _, err = repository.CollectLive(t.Context(), filled32(10), authority,
		[]LiveArtifactSource{source}); !errors.Is(err, ErrArtifactEvidence) {
		t.Fatalf("partial export err=%v", err)
	}
	if repository.Stats().Backups != 0 || repository.Stats().DiskBytes != 0 {
		t.Fatalf("partial stats=%+v", repository.Stats())
	}
	if err = repository.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate an external kill after a draft reached disk but before a
	// certificate existed. Recovery owns and durably removes it.
	if err = os.WriteFile(filepath.Join(root, liveDraftName(filled32(10), 0)), []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	repository, err = OpenBackupRepository(root, repositoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if _, err = os.Stat(filepath.Join(root, liveDraftName(filled32(10), 0))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan draft stat=%v", err)
	}
}

func TestCollectLiveExternalExitBeforeCertificateRecoversNoBackup(t *testing.T) {
	if os.Getenv("VIBEDB_LIVE_BACKUP_EXIT_HELPER") == "1" {
		root := os.Getenv("VIBEDB_LIVE_BACKUP_EXIT_ROOT")
		repository, err := openBackupRepository(root, repositoryLimits(), func(point repositoryFault) error {
			if point == faultAfterArtifactsSync {
				os.Exit(97)
			}
			return nil
		})
		if err != nil {
			os.Exit(96)
		}
		group := backupGroup(1)
		authority := CatalogCut{Generation: 7, Digest: filled32(8), PolicyGeneration: 9,
			Groups: []raftmember.GroupKey{group}}
		source := LiveArtifactSource{Group: group, SourceMember: 1,
			Exporter: liveCollectExporter{payload: bytes.Repeat([]byte{1}, 8192), cut: backupCut(1)}}
		_, _ = repository.CollectLive(context.Background(), filled32(10), authority,
			[]LiveArtifactSource{source})
		os.Exit(95)
	}
	root := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestCollectLiveExternalExitBeforeCertificateRecoversNoBackup$")
	command.Env = append(os.Environ(), "VIBEDB_LIVE_BACKUP_EXIT_HELPER=1",
		"VIBEDB_LIVE_BACKUP_EXIT_ROOT="+root)
	err := command.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 97 {
		t.Fatalf("helper err=%v", err)
	}
	repository, err := OpenBackupRepository(root, repositoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if repository.Stats().Backups != 0 || repository.Stats().Artifacts != 0 || repository.Stats().DiskBytes != 0 {
		t.Fatalf("post-exit stats=%+v", repository.Stats())
	}
}
