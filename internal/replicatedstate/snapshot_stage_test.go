package replicatedstate

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestSnapshotArtifactStageResumesIntoNonServingFilesAndOpensCandidate(t *testing.T) {
	source, snapshot := snapshotArtifactFixture(t)
	artifact, expected := writeSnapshotArtifactFixture(t, snapshot)
	var checkpoints []SnapshotArtifactCheckpoint
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Chunk: func(checkpoint SnapshotArtifactCheckpoint, _ *SnapshotArtifactCursor) error {
			checkpoints = append(checkpoints, checkpoint)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	split := checkpoints[len(checkpoints)/2]

	dir := t.TempDir()
	collectionOptions := durable.Options{MaxBatchDocuments: 2}
	system := createTargetAt(t, dir, "system", collectionOptions)
	system = systemTargetOf(system.Collection)
	user := createTargetAt(t, dir, "user", collectionOptions)
	stage, err := NewSnapshotArtifactStage(expected, system, user, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stage.Offset() != 0 {
		t.Fatalf("new stage offset = %d", stage.Offset())
	}
	cursorPath := filepath.Join(dir, "snapshot.cursor")
	cursorStore, err := OpenSnapshotCursorStore(cursorPath)
	if err != nil {
		t.Fatal(err)
	}
	if manifest, err := stage.Receive(bytes.NewReader(artifact[:split.EndOffset]), cursorStore.Persist); manifest.State.Applied != 0 || !errors.Is(err, ErrSnapshotArtifact) {
		t.Fatalf("prefix receive = %+v, %v", manifest, err)
	}
	persisted, err := cursorStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	if stage.Offset() != split.EndOffset || len(persisted) == 0 {
		t.Fatalf("prefix offset = %d, persisted bytes = %d", stage.Offset(), len(persisted))
	}
	if system.Collection.Len() == 0 ||
		system.Collection.Len() > expected.SystemRows || user.Collection.Len() > expected.UserRows {
		t.Fatalf("prefix rows system=%d user=%d", system.Collection.Len(), user.Collection.Len())
	}

	// Reopen only the logical stager around the same durable handles. The
	// persisted cursor is sufficient; no artifact prefix copy is retained.
	if err := cursorStore.Close(); err != nil {
		t.Fatal(err)
	}
	if err := system.Collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := user.Collection.Close(); err != nil {
		t.Fatal(err)
	}
	reopen := func(name string) *durable.Collection {
		file, err := os.OpenFile(filepath.Join(dir, name+".vdb"), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Open(file, collectionOptions)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		return collection
	}
	system = systemTargetOf(reopen("system"))
	user = targetOf(reopen("user"))
	cursorStore, err = OpenSnapshotCursorStore(cursorPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cursorStore.Close()
	persisted, err = cursorStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	stage, err = NewSnapshotArtifactStage(expected, system, user, persisted)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := stage.Receive(bytes.NewReader(artifact[stage.Offset():]), cursorStore.Persist)
	if err != nil || !equalSnapshotArtifactManifest(manifest, expected) {
		t.Fatalf("resumed receive = %+v, %v", manifest, err)
	}
	if system.Collection.Len() != expected.SystemRows || user.Collection.Len() != expected.UserRows {
		t.Fatalf("final rows system=%d/%d user=%d/%d",
			system.Collection.Len(), expected.SystemRows, user.Collection.Len(), expected.UserRows)
	}

	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	candidate, err := stage.OpenCandidate(source.bootstrap, log, machineOptionsFor(user))
	if err != nil {
		t.Fatal(err)
	}
	publication := candidate.Published()
	if !equalStatePublication(
		expected.State, publication.Applied, publication.LogicalDigest,
		publication.ConfState, publication.ReplicaSetVersion,
	) {
		t.Fatalf("candidate publication = %+v, expected state = %+v", publication, expected.State)
	}
	if _, err := stage.OpenCandidate(source.bootstrap, log, machineOptionsFor(user)); !errors.Is(err, ErrSnapshotStage) {
		t.Fatalf("second OpenCandidate error = %v", err)
	}
}

func TestSnapshotArtifactStagePersistenceFailureReplaysIdempotently(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	artifact, expected := writeSnapshotArtifactFixture(t, snapshot)
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	system = systemTargetOf(system.Collection)
	user := createTargetAt(t, dir, "user", durable.Options{})
	stage, err := NewSnapshotArtifactStage(expected, system, user, nil)
	if err != nil {
		t.Fatal(err)
	}
	persistFailure := errors.New("cursor sync failed")
	persistCalls := 0
	if _, err := stage.Receive(bytes.NewReader(artifact), func([]byte) error {
		persistCalls++
		return persistFailure
	}); !errors.Is(err, persistFailure) {
		t.Fatalf("persistence failure = %v", err)
	}
	if persistCalls != 1 {
		t.Fatalf("default checkpoint persistence calls = %d, want 1", persistCalls)
	}
	if system.Collection.Len() == 0 {
		t.Fatal("chunk rows did not become durable before cursor persistence")
	}
	// No cursor was durably published. Starting from zero replays the exact puts
	// and reaches the same final image without duplicate rows.
	stage, err = NewSnapshotArtifactStage(expected, system, user, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	manifest, err := stage.Receive(bytes.NewReader(artifact), func(raw []byte) error {
		persisted = bytes.Clone(raw)
		return nil
	})
	if err != nil || !equalSnapshotArtifactManifest(manifest, expected) || len(persisted) == 0 {
		t.Fatalf("replay receive = %+v cursor=%d error=%v", manifest, len(persisted), err)
	}
	if system.Collection.Len() != expected.SystemRows || user.Collection.Len() != expected.UserRows {
		t.Fatalf("replay rows system=%d user=%d", system.Collection.Len(), user.Collection.Len())
	}
}

func TestSnapshotArtifactStageRejectsPathologicalCheckpointCadence(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	_, expected := writeSnapshotArtifactFixture(t, snapshot)
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	system = systemTargetOf(system.Collection)
	user := createTargetAt(t, dir, "user", durable.Options{})
	if _, err := NewSnapshotArtifactStageWithOptions(
		expected, system, user, nil,
		SnapshotArtifactStageOptions{CheckpointBytes: MaxSnapshotArtifactChunkBytes - 1},
	); !errors.Is(err, ErrSnapshotStage) {
		t.Fatalf("small checkpoint cadence error = %v", err)
	}
}

func TestSnapshotArtifactStageRejectsWrongExpectationCursorAndLostRows(t *testing.T) {
	_, snapshot := snapshotArtifactFixture(t)
	artifact, expected := writeSnapshotArtifactFixture(t, snapshot)
	newTargets := func(t *testing.T) (string, CollectionTarget, CollectionTarget) {
		dir := t.TempDir()
		system := createTargetAt(t, dir, "system", durable.Options{})
		system = systemTargetOf(system.Collection)
		user := createTargetAt(t, dir, "user", durable.Options{})
		return dir, system, user
	}

	_, system, user := newTargets(t)
	wrong := cloneSnapshotArtifactManifest(expected)
	wrong.Digest[0] ^= 1
	stage, err := NewSnapshotArtifactStage(wrong, system, user, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stage.Receive(bytes.NewReader(artifact), func([]byte) error { return nil }); !errors.Is(err, ErrSnapshotStage) {
		t.Fatalf("wrong final digest error = %v", err)
	}

	var checkpoints []SnapshotArtifactCheckpoint
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Chunk: func(checkpoint SnapshotArtifactCheckpoint, _ *SnapshotArtifactCursor) error {
			checkpoints = append(checkpoints, checkpoint)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	_, system, user = newTargets(t)
	var cursorBytes []byte
	stage, err = NewSnapshotArtifactStage(expected, system, user, nil)
	if err != nil {
		t.Fatal(err)
	}
	cut := checkpoints[0].EndOffset
	_, receiveErr := stage.Receive(bytes.NewReader(artifact[:cut]), func(raw []byte) error {
		cursorBytes = bytes.Clone(raw)
		return nil
	})
	if !errors.Is(receiveErr, ErrSnapshotArtifact) || len(cursorBytes) == 0 {
		t.Fatalf("stopped receive cursor=%d error=%v", len(cursorBytes), receiveErr)
	}

	foreign := cloneSnapshotArtifactManifest(expected)
	foreign.UserCollection = []byte("other")
	stateEnvelope, err := AppendState(nil, foreign.State)
	if err != nil {
		t.Fatal(err)
	}
	_, foreign.HeaderDigest, err = makeSnapshotArtifactHeader(
		stateEnvelope, string(foreign.UserCollection), int(foreign.TargetChunkBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewSnapshotArtifactStage(foreign, system, user, cursorBytes); !errors.Is(err, ErrSnapshotStage) {
		t.Fatalf("foreign cursor error = %v", err)
	}

	// A cursor cannot be paired with empty/replaced durable files: it would skip
	// rows whose acknowledged storage effect is gone.
	_, emptySystem, emptyUser := newTargets(t)
	if _, err := NewSnapshotArtifactStage(expected, emptySystem, emptyUser, cursorBytes); !errors.Is(err, ErrSnapshotStage) {
		t.Fatalf("lost durable rows error = %v", err)
	}
}

func TestSnapshotArtifactStageRequiresCompletionBeforeCandidateOpen(t *testing.T) {
	source, snapshot := snapshotArtifactFixture(t)
	_, expected := writeSnapshotArtifactFixture(t, snapshot)
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	system = systemTargetOf(system.Collection)
	user := createTargetAt(t, dir, "user", durable.Options{})
	stage, err := NewSnapshotArtifactStage(expected, system, user, nil)
	if err != nil {
		t.Fatal(err)
	}
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if _, err := stage.OpenCandidate(source.bootstrap, log, machineOptionsFor(user)); !errors.Is(err, ErrSnapshotStageIncomplete) {
		t.Fatalf("incomplete candidate error = %v", err)
	}
}
