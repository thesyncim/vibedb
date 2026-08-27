package driver

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func TestReplicatedActivatedSnapshotImageAccountsForSeedStateRow(t *testing.T) {
	seed := replicatedstate.SnapshotArtifactManifest{
		Seeded: true, UserCollection: []byte("docs"), UserRows: 3,
		ImageDigest: [32]byte{1}, CaptureImageDigest: [32]byte{2},
	}
	current := seed.Clone()
	current.Seeded, current.SystemRows = false, 1
	if !equalReplicatedActivatedSnapshotImage(current, seed) || seed.SystemRows != 0 {
		t.Fatal("exact activated seed rejected or authenticated manifest mutated")
	}
	for _, test := range []struct {
		name   string
		change func(*replicatedstate.SnapshotArtifactManifest)
	}{
		{"missing state", func(m *replicatedstate.SnapshotArtifactManifest) { m.SystemRows = 0 }},
		{"extra hidden row", func(m *replicatedstate.SnapshotArtifactManifest) { m.SystemRows = 2 }},
		{"not streamed", func(m *replicatedstate.SnapshotArtifactManifest) { m.Seeded = true }},
		{"bundle", func(m *replicatedstate.SnapshotArtifactManifest) { m.Bundle = true }},
		{"user rows", func(m *replicatedstate.SnapshotArtifactManifest) { m.UserRows++ }},
		{"user image", func(m *replicatedstate.SnapshotArtifactManifest) { m.ImageDigest[0]++ }},
		{"capture rows", func(m *replicatedstate.SnapshotArtifactManifest) { m.CaptureRows++ }},
		{"capture image", func(m *replicatedstate.SnapshotArtifactManifest) { m.CaptureImageDigest[0]++ }},
		{"name", func(m *replicatedstate.SnapshotArtifactManifest) { m.UserCollection = []byte("foreign") }},
		{"relation digest", func(m *replicatedstate.SnapshotArtifactManifest) { m.RelationManifestDigest[0]++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := current.Clone()
			test.change(&bad)
			if equalReplicatedActivatedSnapshotImage(bad, seed) {
				t.Fatal("different activated image accepted")
			}
		})
	}
	badSeed := seed.Clone()
	badSeed.SystemRows = 1
	if equalReplicatedActivatedSnapshotImage(current, badSeed) {
		t.Fatal("noncanonical seeded row count accepted")
	}
	streamed := current.Clone()
	streamed.SystemRows = 4
	if !equalReplicatedActivatedSnapshotImage(streamed, streamed) ||
		equalReplicatedActivatedSnapshotImage(current, streamed) {
		t.Fatal("streamed row counts no longer compared exactly")
	}
}
