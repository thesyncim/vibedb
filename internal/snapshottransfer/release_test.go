package snapshottransfer

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func testReleaseRequest(d Descriptor) ArtifactReleaseRequest {
	return ArtifactReleaseRequest{
		Operation:  sha256.Sum256([]byte("replica-move-operation")),
		Step:       sha256.Sum256([]byte("certified-learner-install")),
		Descriptor: d,
	}
}

func TestRepositoryReleasePublishedIsExactBoundedAndIdempotent(t *testing.T) {
	payload := bytes.Repeat([]byte("release"), MinChunkBytes)
	d := testDescriptor(payload)
	r := openTestRepository(t, filepath.Join(t.TempDir(), "repo"))
	appendAll(t, r, d, payload, 0)

	reader, err := r.OpenPublished(d, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.ReleasePublished(testReleaseRequest(d)); !errors.Is(err, ErrArtifactBusy) {
		t.Fatalf("release with reader = %v", err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	request := testReleaseRequest(d)
	if err = r.ReleasePublished(request); err != nil {
		t.Fatal(err)
	}
	if err = r.ReleasePublished(request); err != nil {
		t.Fatalf("idempotent retry = %v", err)
	}
	if stats := r.Stats(); stats.Artifacts != 0 || stats.DiskBytes != 0 {
		t.Fatalf("stats after release = %+v", stats)
	}
	request.Operation = [sha256.Size]byte{}
	if err = r.ReleasePublished(request); !errors.Is(err, ErrDescriptor) {
		t.Fatalf("anonymous release = %v", err)
	}
}

func TestRepositoryReleaseNeverDeletesStagedOrStaleArtifact(t *testing.T) {
	payload := bytes.Repeat([]byte{9}, MinChunkBytes*2)
	d := testDescriptor(payload)
	r := openTestRepository(t, filepath.Join(t.TempDir(), "repo"))
	chunk := payload[:MinChunkBytes]
	if _, _, err := r.Append(d, 0, chunk, sha256.Sum256(chunk)); err != nil {
		t.Fatal(err)
	}
	if err := r.ReleasePublished(testReleaseRequest(d)); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("staged release = %v", err)
	}
	stale := d
	stale.SchemaGeneration++
	if err := r.ReleasePublished(testReleaseRequest(stale)); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale release = %v", err)
	}
	if stats := r.Stats(); stats.Staged != 1 {
		t.Fatalf("staged artifact was lost: %+v", stats)
	}
}

func TestRepositoryRecoversEveryReleaseNamespacePhase(t *testing.T) {
	for _, phase := range []repositoryFault{
		faultAfterReleaseRename,
		faultAfterReleaseUnlink,
		faultAfterReleaseSync,
	} {
		t.Run(string(rune('0'+phase)), func(t *testing.T) {
			payload := bytes.Repeat([]byte{byte(phase)}, MinChunkBytes)
			d := testDescriptor(payload)
			path := filepath.Join(t.TempDir(), "repo")
			limits := Limits{MaxArtifacts: 1, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 2 << 20}
			verify := func(*os.File, Descriptor) error { return nil }
			r, err := openRepository(path, limits, verify)
			if err != nil {
				t.Fatal(err)
			}
			appendAll(t, r, d, payload, 0)
			r.fault = func(got repositoryFault) error {
				if got == phase {
					return errors.New("injected release crash")
				}
				return nil
			}
			if err = r.ReleasePublished(testReleaseRequest(d)); !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("fault %d = %v", phase, err)
			}
			if err = r.Close(); err != nil {
				t.Fatal(err)
			}
			r, err = openRepository(path, limits, verify)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if stats := r.Stats(); stats.Artifacts != 0 || stats.DiskBytes != 0 {
				t.Fatalf("recovered stats = %+v", stats)
			}
			if err = r.ReleasePublished(testReleaseRequest(d)); err != nil {
				t.Fatalf("retry after recovery = %v", err)
			}
			for _, name := range []string{deletingArtifactName(d.ArtifactHash)} {
				if _, statErr := os.Stat(filepath.Join(path, name)); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("retained %s: %v", name, statErr)
				}
			}
		})
	}
}
