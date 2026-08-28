package replicatedstate

import (
	"bytes"
	"testing"
)

func TestRestoreRehashPreservesSingletonAndBundleImageGrammar(t *testing.T) {
	for _, bundle := range []bool{false, true} {
		t.Run(map[bool]string{false: "singleton", true: "bundle"}[bundle], func(t *testing.T) {
			var raw []byte
			var manifest SnapshotArtifactManifest
			var snapshot *ReadSnapshot
			if bundle {
				raw, manifest, snapshot = bundleSnapshotArtifactFixture(t)
				defer snapshot.Close()
			} else {
				_, snapshot = snapshotArtifactFixture(t)
				raw, manifest = writeSnapshotArtifactFixture(t, snapshot)
			}
			profiles := make([]RestoreImageProfile, len(snapshot.relations))
			for i, r := range snapshot.relations {
				profiles[i] = RestoreImageProfile{Name: r.name, ValidationDigest: r.target.ValidationDigest}
			}
			verified, digest, err := RehashSnapshotArtifact(bytes.NewReader(raw), profiles)
			if err != nil || verified.Digest != manifest.Digest || digest != manifest.ImageDigest {
				t.Fatalf("noop image=%x want=%x err=%v", digest, manifest.ImageDigest, err)
			}
			profiles[0].ValidationDigest[0] ^= 1
			verified, digest, err = RehashSnapshotArtifact(bytes.NewReader(raw), profiles)
			if err != nil || verified.Digest != manifest.Digest || digest == manifest.ImageDigest {
				t.Fatalf("fresh domain did not rehash %v", err)
			}
			corrupt := bytes.Clone(raw)
			corrupt[len(corrupt)/2] ^= 1
			if _, _, err := RehashSnapshotArtifact(bytes.NewReader(corrupt), profiles); err == nil {
				t.Fatal("corrupt source accepted")
			}
		})
	}
}
