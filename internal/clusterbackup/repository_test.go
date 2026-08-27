package clusterbackup

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

func repositoryCertificate(t *testing.T, payloads ...[]byte) Certificate {
	t.Helper()
	cuts := make([]GroupCut, len(payloads))
	groups := make([]raftmember.GroupKey, len(payloads))
	for index, payload := range payloads {
		cuts[index] = backupCut(byte(index + 1))
		cuts[index].ArtifactHash = sha256.Sum256(payload)
		cuts[index].ArtifactBytes = uint64(len(payload))
		groups[index] = cuts[index].Group
	}
	certificate, err := Certify(filled32(61), CatalogCut{Generation: 17,
		Digest: filled32(62), PolicyGeneration: 18, Groups: groups}, cuts)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func repositoryLimits() RepositoryLimits {
	return RepositoryLimits{MaxBackups: 4, MaxArtifacts: 16,
		MaxArtifactBytes: 1 << 20, MaxDiskBytes: 8 << 20}
}

func artifactInputs(payloads ...[]byte) []ArtifactInput {
	result := make([]ArtifactInput, len(payloads))
	for index := range payloads {
		result[index].Reader = bytes.NewReader(payloads[index])
	}
	return result
}

func TestRepositoryPublishesReopensAndExportsExactAuthenticatedArtifacts(t *testing.T) {
	directory := t.TempDir()
	payloads := [][]byte{bytes.Repeat([]byte("first"), 711), bytes.Repeat([]byte("second"), 913)}
	certificate := repositoryCertificate(t, payloads...)
	repository, err := OpenBackupRepository(directory, repositoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err = repository.Publish(certificate, artifactInputs(payloads...)...); err != nil {
		t.Fatal(err)
	}
	stats := repository.Stats()
	if stats.Backups != 1 || stats.Artifacts != 2 || stats.DiskBytes != uint64(len(payloads[0])+len(payloads[1])+HeaderBytes+2*GroupCutBytes+TrailerBytes) {
		t.Fatalf("stats=%+v", stats)
	}
	// An exact retry must neither consume the readers nor rewrite bytes.
	if err = repository.Publish(certificate, []ArtifactInput{{Reader: nil}, {Reader: nil}}...); err != nil {
		t.Fatalf("idempotent publish: %v", err)
	}
	if err = repository.Close(); err != nil {
		t.Fatal(err)
	}
	repository, err = OpenBackupRepository(directory, repositoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	opened, err := repository.Certificate(certificate.Digest)
	if err != nil || opened.Digest != certificate.Digest || len(opened.Groups) != 2 {
		t.Fatalf("certificate=%+v err=%v", opened, err)
	}
	// Certificate() owns its result.
	opened.Groups[0] = GroupCut{}
	again, err := repository.Certificate(certificate.Digest)
	if err != nil || !again.Groups[0].Valid() {
		t.Fatalf("aliased certificate err=%v", err)
	}
	for index, want := range payloads {
		artifact, err := repository.OpenArtifact(certificate.Digest, index)
		if err != nil {
			t.Fatal(err)
		}
		got, readErr := io.ReadAll(artifact)
		closeErr := artifact.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("artifact %d bytes=%d read=%v close=%v", index, len(got), readErr, closeErr)
		}
	}
	artifact, err := repository.OpenBackupArtifact(t.Context(), certificate.Operation, certificate.Groups[1].Group)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(artifact)
	if closeErr := artifact.Close(); readErr != nil || closeErr != nil || !bytes.Equal(got, payloads[1]) {
		t.Fatalf("artifact source got=%d read=%v close=%v", len(got), readErr, closeErr)
	}
}

func TestRepositoryRejectsWrongSizeHashAndTrailingBytesWithoutPublication(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "short", data: []byte("payloa")},
		{name: "wrong-hash", data: []byte("payloae")},
		{name: "trailing", data: []byte("payload!")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			certificate := repositoryCertificate(t, []byte("payload"))
			repository, err := OpenBackupRepository(directory, repositoryLimits())
			if err != nil {
				t.Fatal(err)
			}
			if err = repository.Publish(certificate, artifactInputs(test.data)...); !errors.Is(err, ErrArtifactEvidence) {
				t.Fatalf("err=%v", err)
			}
			if repository.Stats().Backups != 0 {
				t.Fatal("failed bytes became visible")
			}
			if err = repository.Close(); err != nil {
				t.Fatal(err)
			}
			repository, err = OpenBackupRepository(directory, repositoryLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer repository.Close()
			if repository.Stats().Backups != 0 {
				t.Fatal("failed bytes recovered")
			}
		})
	}
}

func TestRepositoryCrashBoundariesNeverExposePartialPublicationOrReleasedBackup(t *testing.T) {
	injected := errors.New("injected crash")
	payload := bytes.Repeat([]byte{7}, 4096)
	certificate := repositoryCertificate(t, payload)

	t.Run("artifacts-before-certificate", func(t *testing.T) {
		directory := t.TempDir()
		repository, err := openBackupRepository(directory, repositoryLimits(), func(point repositoryFault) error {
			if point == faultAfterArtifactsSync {
				return injected
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = repository.Publish(certificate, artifactInputs(payload)...); !errors.Is(err, injected) {
			t.Fatalf("err=%v", err)
		}
		_ = repository.Close()
		repository, err = OpenBackupRepository(directory, repositoryLimits())
		if err != nil {
			t.Fatal(err)
		}
		defer repository.Close()
		if repository.Stats().Backups != 0 {
			t.Fatal("artifact-only cut became visible")
		}
	})

	t.Run("certificate-rename", func(t *testing.T) {
		directory := t.TempDir()
		repository, err := openBackupRepository(directory, repositoryLimits(), func(point repositoryFault) error {
			if point == faultAfterCertificateRename {
				return injected
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = repository.Publish(certificate, artifactInputs(payload)...); !errors.Is(err, injected) {
			t.Fatalf("err=%v", err)
		}
		_ = repository.Close()
		repository, err = OpenBackupRepository(directory, repositoryLimits())
		if err != nil || repository.Stats().Backups != 1 {
			t.Fatalf("stats=%+v err=%v", repository.Stats(), err)
		}
		_ = repository.Close()
	})

	t.Run("release-rename", func(t *testing.T) {
		directory := t.TempDir()
		repository, err := OpenBackupRepository(directory, repositoryLimits())
		if err != nil {
			t.Fatal(err)
		}
		if err = repository.Publish(certificate, artifactInputs(payload)...); err != nil {
			t.Fatal(err)
		}
		repository.fault = func(point repositoryFault) error {
			if point == faultAfterReleaseRename {
				return injected
			}
			return nil
		}
		if err = repository.Release(certificate.Digest); !errors.Is(err, injected) {
			t.Fatalf("err=%v", err)
		}
		_ = repository.Close()
		repository, err = OpenBackupRepository(directory, repositoryLimits())
		if err != nil {
			t.Fatal(err)
		}
		defer repository.Close()
		if repository.Stats().Backups != 0 {
			t.Fatal("released backup resurrected")
		}
	})
}

func TestRepositoryRejectsSymlinkCorruptionAndMissingArtifactOnReopen(t *testing.T) {
	payload := bytes.Repeat([]byte{9}, 128)
	certificate := repositoryCertificate(t, payload)

	t.Run("symlink", func(t *testing.T) {
		directory := t.TempDir()
		if err := os.Symlink(filepath.Join(directory, "target"), filepath.Join(directory, certificateName(certificate.Digest))); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenBackupRepository(directory, repositoryLimits()); !errors.Is(err, ErrRepository) {
			t.Fatalf("err=%v", err)
		}
	})

	for _, test := range []struct {
		name   string
		mutate func(string) error
	}{
		{name: "corrupt-artifact", mutate: func(directory string) error {
			return os.WriteFile(filepath.Join(directory, artifactName(certificate.Digest, 0)), []byte("corrupt"), 0o600)
		}},
		{name: "missing-artifact", mutate: func(directory string) error {
			return os.Remove(filepath.Join(directory, artifactName(certificate.Digest, 0)))
		}},
		{name: "corrupt-certificate", mutate: func(directory string) error {
			path := filepath.Join(directory, certificateName(certificate.Digest))
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			raw[len(raw)-1] ^= 1
			return os.WriteFile(path, raw, 0o600)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			repository, err := OpenBackupRepository(directory, repositoryLimits())
			if err != nil {
				t.Fatal(err)
			}
			if err = repository.Publish(certificate, artifactInputs(payload)...); err != nil {
				t.Fatal(err)
			}
			if err = repository.Close(); err != nil {
				t.Fatal(err)
			}
			if err = test.mutate(directory); err != nil {
				t.Fatal(err)
			}
			if _, err = OpenBackupRepository(directory, repositoryLimits()); !errors.Is(err, ErrRepository) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestRepositoryRecoveryRemovesPartialTemporaryAndUncommittedArtifactState(t *testing.T) {
	directory := t.TempDir()
	payload := []byte("uncommitted")
	certificate := repositoryCertificate(t, payload)
	for name, raw := range map[string][]byte{
		certificateTempName(certificate.Digest): []byte("partial certificate"),
		artifactTempName(certificate.Digest, 0): []byte("partial artifact"),
		artifactName(certificate.Digest, 0):     payload,
	} {
		if err := os.WriteFile(filepath.Join(directory, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	repository, err := OpenBackupRepository(directory, repositoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if repository.Stats().Backups != 0 || repository.Stats().DiskBytes != 0 {
		t.Fatalf("stats=%+v", repository.Stats())
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "repository.lock" {
		t.Fatalf("entries=%v", entries)
	}
}

func TestRepositoryEnforcesResourceBoundsBeforeWriting(t *testing.T) {
	payload := bytes.Repeat([]byte{1}, 1024)
	certificate := repositoryCertificate(t, payload)
	raw, _ := AppendCertificate(nil, certificate)
	tests := []RepositoryLimits{
		{MaxBackups: 1, MaxArtifacts: 1, MaxArtifactBytes: 1023, MaxDiskBytes: 4096},
		{MaxBackups: 1, MaxArtifacts: 1, MaxArtifactBytes: 1024, MaxDiskBytes: uint64(len(raw) + len(payload) - 1)},
	}
	for _, limits := range tests {
		directory := t.TempDir()
		repository, err := OpenBackupRepository(directory, limits)
		if err != nil {
			// A nonsensical repository-wide bound may be rejected at open.
			if !errors.Is(err, ErrBound) {
				t.Fatal(err)
			}
			continue
		}
		if err = repository.Publish(certificate, artifactInputs(payload)...); !errors.Is(err, ErrBound) {
			t.Fatalf("limits=%+v err=%v", limits, err)
		}
		_ = repository.Close()
	}

	directory := t.TempDir()
	limits := repositoryLimits()
	limits.MaxBackups = 1
	repository, err := OpenBackupRepository(directory, limits)
	if err != nil {
		t.Fatal(err)
	}
	if err = repository.Publish(certificate, artifactInputs(payload)...); err != nil {
		t.Fatal(err)
	}
	other := repositoryCertificate(t, bytes.Repeat([]byte{2}, 1024))
	if err = repository.Publish(other, artifactInputs(bytes.Repeat([]byte{2}, 1024))...); !errors.Is(err, ErrBound) {
		t.Fatalf("backup count err=%v", err)
	}
	_ = repository.Close()
}
