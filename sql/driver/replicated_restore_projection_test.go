package driver

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestReplicatedRestoreProjectionDropsSourceRowsAndResumes(t *testing.T) {
	sourceStage, sourceDB, _, artifact, _ := newReplicatedSnapshotStageFixture(t)
	source := sourceStage.expected.Clone()
	_ = sourceStage.Close()
	_ = sourceDB.Close()
	target, identity := openRestoreSingletonRoot(t, "projection-target", 83)
	defer target.Close()
	document := []byte(`{"id":"fresh-catalog"}`)
	key := testReplicatedApplyKey(t, target, document)
	rows := []replicatedstate.ProjectionRow{{Key: key, Value: document}}
	options := testReplicatedApplyOptions()
	options.RequestLedgerCapacityBytes = 64 << 20
	options.RequestLedgerCleanupReserveBytes = 8 << 20
	options.RequestLedgerRangeIdentity = [32]byte{91}
	stage, _, err := target.OpenReplicatedRestoreProjection(identity, source, nil, options, rows)
	if err != nil {
		t.Fatal(err)
	}
	var cursor []byte
	prefix := artifact[:len(artifact)-replicatedstate.SnapshotArtifactFooterBytes]
	_, err = stage.Receive(bytes.NewReader(prefix), func(raw []byte) error { cursor = bytes.Clone(raw); return nil })
	if err == nil || len(cursor) == 0 {
		t.Fatal("truncated artifact accepted or cursor absent")
	}
	offset := stage.Offset()
	if stage.table.collection.Len() != 0 {
		t.Fatal("source catalog authority imported")
	}
	if err = stage.Close(); err != nil {
		t.Fatal(err)
	}
	stage, _, err = target.OpenReplicatedRestoreProjection(identity, source, cursor, options, rows)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	if _, err = stage.Receive(bytes.NewReader(artifact[offset:]), func(raw []byte) error { cursor = bytes.Clone(raw); return nil }); err != nil {
		t.Fatal(err)
	}
	if stage.table.collection.Len() != 0 {
		t.Fatal("source rows survived full verification")
	}
	// Crash after a projection row is durable but before the seeded checkpoint.
	if err = stage.table.collection.Update(func(batch *durable.WriteBatch) error { return batch.Put(key, document) }); err != nil {
		t.Fatal(err)
	}
	offset = stage.Offset()
	if err = stage.Close(); err != nil {
		t.Fatal(err)
	}
	changed := []replicatedstate.ProjectionRow{{Key: key, Value: []byte(`{"id":"different"}`)}}
	if _, _, err = target.OpenReplicatedRestoreProjection(identity, source, cursor, options, changed); err == nil {
		t.Fatal("changed projection accepted on resume")
	}
	stage, _, err = target.OpenReplicatedRestoreProjection(identity, source, cursor, options, rows)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	if _, err = stage.Receive(bytes.NewReader(artifact[offset:]), func([]byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	activation, err := stage.Activate(restoreBootstrap(83), replicatedstate.StagedSnapshotCut{Applied: 2, Term: 1, EntryDigest: sha256.Sum256([]byte("fresh-projection"))}, replicatedstate.SnapshotArtifactOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer activation.Apply.Close()
	usage, err := activation.Apply.machine.RequestLedgerUsage()
	if err != nil || !usage.Enabled || usage.CapacityBytes != options.RequestLedgerCapacityBytes || usage.CleanupReserveBytes != options.RequestLedgerCleanupReserveBytes || [32]byte(usage.Range.Identity) != options.RequestLedgerRangeIdentity {
		t.Fatalf("staged ledger contract lost: %+v %v", usage, err)
	}
	expected, err := replicatedstate.ProjectionImageDigest(identity.UserTable, activation.ApplyIdentity.ValidationDigest, rows)
	if err != nil || !activation.ArtifactManifest.Seeded || activation.ArtifactManifest.ImageDigest != expected || activation.ArtifactManifest.ImageDigest == source.ImageDigest || activation.ArtifactManifest.UserRows != 1 || activation.ArtifactManifest.SystemRows != 0 || activation.ArtifactManifest.CaptureRows != 0 {
		t.Fatalf("projection image mismatch: %+v %v", activation.ArtifactManifest, err)
	}
	// The compact seeded certificate has no streamed geometry. Verify the
	// actual target system row separately through a complete streamed export.
	if cut, err := activation.Apply.SnapshotArtifactCut(); cut != nil || !errors.Is(err, ErrReplicatedApplyBasePending) {
		t.Fatalf("export before exact Raft snapshot-base installation: cut=%v err=%v", cut, err)
	}
	if _, err := activation.Apply.InstallSnapshot(activation.SnapshotBase); err != nil {
		t.Fatalf("install exact activated snapshot base: %v", err)
	}
	cut, err := activation.Apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var exported bytes.Buffer
	streamed, writeErr := replicatedstate.WriteSnapshotArtifact(&exported, cut, replicatedstate.SnapshotArtifactOptions{})
	closeErr := cut.Close()
	if writeErr != nil || closeErr != nil || streamed.SystemRows != 1 || streamed.UserRows != 1 || streamed.ImageDigest != expected {
		t.Fatalf("streamed projection target: %+v, %v, %v", streamed, writeErr, closeErr)
	}
	value, found, err := stage.table.collection.AppendRaw(nil, key)
	if err != nil || !found || !bytes.Equal(value, document) || stage.table.collection.Len() != 1 {
		t.Fatal("fresh projection missing or source authority retained")
	}
	if err = activation.Apply.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, ok, err := target.ResumeReplicatedSnapshotActivation(identity, activation.ArtifactManifest, restoreBootstrap(83), options)
	if err != nil || !ok {
		t.Fatalf("ledger snapshot resume=%t err=%v", ok, err)
	}
	defer resumed.Apply.Close()
	usage, err = resumed.Apply.machine.RequestLedgerUsage()
	if err != nil || !usage.Enabled || usage.CapacityBytes != options.RequestLedgerCapacityBytes || [32]byte(usage.Range.Identity) != options.RequestLedgerRangeIdentity {
		t.Fatalf("resumed ledger contract lost: %+v %v", usage, err)
	}
}
