package clusterbackup

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func stagingPermit(certificate Certificate) RestoreStagingPermit {
	return RestoreStagingPermit{Restore: filled32(71), CertificateDigest: certificate.Digest,
		CatalogGeneration: certificate.CatalogGeneration, CatalogDigest: certificate.CatalogDigest,
		TargetClusterID: filled16(72), TargetClusterIncarnation: filled16(73),
		Groups: uint32(len(certificate.Groups))}
}

func TestRestoreStagingRootPublishesPermitLastAndReopensExactArtifacts(t *testing.T) {
	payloads := [][]byte{bytes.Repeat([]byte("first"), 1024), bytes.Repeat([]byte("second"), 1536)}
	certificate := repositoryCertificate(t, payloads...)
	source, err := OpenBackupRepository(filepath.Join(t.TempDir(), "source"), repositoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err = source.Publish(certificate, artifactInputs(payloads...)...); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	staged, err := BuildRestoreStagingRoot(t.Context(), target, repositoryLimits(),
		certificate, stagingPermit(certificate), source)
	if err != nil {
		t.Fatal(err)
	}
	if staged.Repository.Stats().Artifacts != len(payloads) {
		t.Fatalf("stats=%+v", staged.Repository.Stats())
	}
	if err = staged.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenRestoreStagingRoot(target, repositoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	artifact, err := reopened.Repository.OpenBackupArtifact(t.Context(), certificate.Operation,
		certificate.Groups[1].Group)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(artifact)
	if closeErr := artifact.Close(); readErr != nil || closeErr != nil || !bytes.Equal(got, payloads[1]) {
		t.Fatalf("artifact=%d read=%v close=%v", len(got), readErr, closeErr)
	}
}

func TestRestoreStagingRootRejectsPartialCorruptAndSymlinkState(t *testing.T) {
	payload := bytes.Repeat([]byte{9}, 4096)
	certificate := repositoryCertificate(t, payload)
	source, err := OpenBackupRepository(filepath.Join(t.TempDir(), "source"), repositoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err = source.Publish(certificate, artifactInputs(payload)...); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "restore")
	if err = os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenRestoreStagingRoot(root, repositoryLimits()); !errors.Is(err, ErrRestoreStagingRoot) {
		t.Fatalf("partial root err=%v", err)
	}
	staged, err := BuildRestoreStagingRoot(t.Context(), root, repositoryLimits(), certificate,
		stagingPermit(certificate), source)
	if err != nil {
		t.Fatal(err)
	}
	if err = staged.Close(); err != nil {
		t.Fatal(err)
	}
	permitPath := filepath.Join(root, "permit")
	raw, err := os.ReadFile(permitPath)
	if err != nil {
		t.Fatal(err)
	}
	raw[80] ^= 1
	if err = os.WriteFile(permitPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenRestoreStagingRoot(root, repositoryLimits()); !errors.Is(err, ErrRestoreStagingRoot) {
		t.Fatalf("corrupt permit err=%v", err)
	}

	symlinkRoot := filepath.Join(t.TempDir(), "symlink")
	if err = os.Symlink(root, symlinkRoot); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenRestoreStagingRoot(symlinkRoot, repositoryLimits()); !errors.Is(err, ErrRestoreStagingRoot) {
		t.Fatalf("symlink root err=%v", err)
	}
}

func TestRestoreStagingPermitCanonicalCorruptionAndTrailingRejection(t *testing.T) {
	certificate := repositoryCertificate(t, []byte("artifact"))
	permit := stagingPermit(certificate)
	raw, err := AppendRestoreStagingPermit(nil, permit)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenRestoreStagingPermit(raw)
	if err != nil || opened != permit {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	for _, malformed := range [][]byte{raw[:len(raw)-1], append(append([]byte(nil), raw...), 0)} {
		if _, err = OpenRestoreStagingPermit(malformed); !errors.Is(err, ErrRestoreStagingRoot) {
			t.Fatalf("malformed length=%d err=%v", len(malformed), err)
		}
	}
	corrupt := append([]byte(nil), raw...)
	corrupt[10] ^= 1
	if _, err = OpenRestoreStagingPermit(corrupt); !errors.Is(err, ErrRestoreStagingRoot) {
		t.Fatalf("corrupt err=%v", err)
	}
}
