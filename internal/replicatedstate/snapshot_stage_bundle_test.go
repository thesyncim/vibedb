package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func snapshotStageBundleSpecs(
	base, global CollectionTarget,
	index store.IndexDefinition,
) []RelationCollection {
	return []RelationCollection{
		{
			Relation: 1, Kind: RelationJSON, Name: "base", Target: base,
			LocalIndexes: []store.IndexDefinition{index},
		},
		{
			Relation: 2, Kind: RelationGlobalIndex, Name: "global", Target: global,
			GlobalIndex: testGlobalIndexProfile(91, 7, 1, true),
		},
	}
}

func snapshotStageBundleTargets(
	t testing.TB,
) (string, CollectionTarget, CollectionTarget, CollectionTarget, store.IndexDefinition) {
	t.Helper()
	dir := t.TempDir()
	index := store.IndexDefinition{Name: "by_email", Paths: []string{"/email"}}
	system := createTargetAt(t, dir, "system", durable.Options{})
	base := createTargetAt(t, dir, "base", durable.Options{
		Indexes: []store.IndexDefinition{index},
	})
	global := createTargetAt(t, dir, "global", durable.Options{})
	return dir, systemTargetOf(system.Collection), base, global, index
}

func snapshotStageBundleOptions(relations []RelationCollection) Options {
	relationDocuments := 0
	for i := range relations {
		relationDocuments += relations[i].Target.Limits.MaxDistinctMutations
	}
	relationDocuments = min(relationDocuments, replication.MaxMutations)
	documents, err := RequiredBundleTransactionDocuments(relationDocuments, 8, false)
	if err != nil {
		panic(err)
	}
	return Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: len(relations) + 1,
			MaxDocuments:   documents,
			MaxBytes:       64 << 20,
		},
		MaxSessions: 128, RetryWindow: 8,
	}
}

func TestBundleSnapshotArtifactStageRoutesAndOpensCandidate(t *testing.T) {
	source := newRelationBundleFixture(t, true)
	baseKey, baseValue := []byte("doc"), []byte(`{"email":"a","n":1}`)
	globalKey, globalValue := []byte{0x91, 0x01, 'a'}, []byte(`["doc"]`)
	command := source.command(t, 1,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: baseKey, Value: baseValue,
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: globalValue,
		}}},
	)
	if _, err := source.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	artifact, expected := writeSnapshotArtifactFixture(t, snapshot)
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if !expected.Bundle || len(expected.Relations) != 2 {
		t.Fatalf("artifact bundle=%v relations=%d", expected.Bundle, len(expected.Relations))
	}

	dir, system, base, global, index := snapshotStageBundleTargets(t)
	relations := snapshotStageBundleSpecs(base, global, index)
	stage, err := NewBundleSnapshotArtifactStage(expected, system, relations, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stage.OpenCandidateBundle(
		testBootstrap(), nil, snapshotStageBundleOptions(relations),
	); !errors.Is(err, ErrSnapshotStageIncomplete) {
		t.Fatalf("incomplete open error=%v", err)
	}
	var cursor []byte
	manifest, err := stage.Receive(bytes.NewReader(artifact), func(raw []byte) error {
		cursor = bytes.Clone(raw)
		return nil
	})
	if err != nil || !equalSnapshotArtifactManifest(manifest, expected) || len(cursor) == 0 {
		t.Fatalf("receive manifest=%+v cursor=%d err=%v", manifest, len(cursor), err)
	}
	if base.Collection.Len() != expected.Relations[0].Rows ||
		global.Collection.Len() != expected.Relations[1].Rows {
		t.Fatalf("relation rows base=%d/%d global=%d/%d",
			base.Collection.Len(), expected.Relations[0].Rows,
			global.Collection.Len(), expected.Relations[1].Rows)
	}
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	if _, err := stage.OpenCandidate(testBootstrap(), log, snapshotStageBundleOptions(relations)); !errors.Is(err, ErrSnapshotStage) {
		t.Fatalf("singleton open accepted bundle: %v", err)
	}
	candidate, err := stage.OpenCandidateBundle(
		testBootstrap(), log, snapshotStageBundleOptions(relations),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		relation replication.RelationID
		key      []byte
		want     []byte
		limit    int
	}{
		{1, baseKey, baseValue, base.Limits.MaxDocumentBytes},
		{2, globalKey, globalValue, global.Limits.MaxDocumentBytes},
	} {
		got, err := candidate.PointReadInto(
			check.relation, check.key, expected.State.Applied, check.limit, nil,
		)
		if err != nil || !got.Found || !bytes.Equal(got.Value, check.want) {
			t.Fatalf("relation %d read=%+v err=%v", check.relation, got, err)
		}
	}
}

func TestBundleSnapshotArtifactStageRejectsRelationSwapAndForgery(t *testing.T) {
	source := newRelationBundleFixture(t, false)
	snapshot, err := source.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	_, expected := writeSnapshotArtifactFixture(t, snapshot)
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	_, system, base, global, index := snapshotStageBundleTargets(t)
	relations := snapshotStageBundleSpecs(base, global, index)

	if _, err := NewSnapshotArtifactStage(expected, system, base, nil); !errors.Is(err, ErrSnapshotStage) {
		t.Fatalf("singleton constructor accepted bundle: %v", err)
	}
	for name, mutate := range map[string]func([]RelationCollection, *SnapshotArtifactManifest){
		"reordered": func(specs []RelationCollection, _ *SnapshotArtifactManifest) {
			specs[0], specs[1] = specs[1], specs[0]
		},
		"swapped_targets": func(specs []RelationCollection, _ *SnapshotArtifactManifest) {
			specs[0].Target, specs[1].Target = specs[1].Target, specs[0].Target
		},
		"renamed": func(specs []RelationCollection, _ *SnapshotArtifactManifest) {
			specs[1].Name = "forged"
		},
		"manifest_digest": func(_ []RelationCollection, manifest *SnapshotArtifactManifest) {
			manifest.RelationManifestDigest[0] ^= 1
		},
		"certificate_name": func(_ []RelationCollection, manifest *SnapshotArtifactManifest) {
			manifest.Relations[1].Collection = []byte("forged")
		},
	} {
		t.Run(name, func(t *testing.T) {
			specs := append([]RelationCollection(nil), relations...)
			manifest := cloneSnapshotArtifactManifest(expected)
			mutate(specs, &manifest)
			if _, err := NewBundleSnapshotArtifactStage(manifest, system, specs, nil); !errors.Is(err, ErrSnapshotStage) {
				t.Fatalf("forgery accepted: %v", err)
			}
		})
	}
}

func TestBundleSnapshotArtifactStageCrashResumeKeepsRelationIdentity(t *testing.T) {
	source := newRelationBundleFixture(t, false)
	for sequence := uint64(1); sequence <= 4; sequence++ {
		key := []byte{'d', byte('0' + sequence)}
		globalKey := []byte{0x91, 0x01, byte('a' + sequence)}
		command := source.command(t, sequence,
			replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
				Kind: replication.MutationPut, Key: key,
				Value: []byte(`{"email":"resume"}`),
			}}},
			replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
				Kind: replication.MutationPutAbsentOrEqual, Key: globalKey,
				Value: []byte{'[', '"', key[0], key[1], '"', ']'},
			}}},
		)
		if _, err := source.machine.ApplyNormal(normalMeta(sequence+2), command); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := source.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	artifact, expected := writeSnapshotArtifactFixture(t, snapshot)
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	var split uint64
	if _, err := VerifySnapshotArtifact(bytes.NewReader(artifact), SnapshotArtifactCallbacks{
		Chunk: func(checkpoint SnapshotArtifactCheckpoint, _ *SnapshotArtifactCursor) error {
			if checkpoint.Relation == 2 && split == 0 {
				split = checkpoint.EndOffset
			}
			return nil
		},
	}); err != nil || split == 0 {
		t.Fatalf("find relation split=%d err=%v", split, err)
	}
	_, system, base, global, index := snapshotStageBundleTargets(t)
	relations := snapshotStageBundleSpecs(base, global, index)
	stage, err := NewBundleSnapshotArtifactStage(expected, system, relations, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	if _, err := stage.Receive(bytes.NewReader(artifact[:split]), func(raw []byte) error {
		persisted = bytes.Clone(raw)
		return nil
	}); !errors.Is(err, ErrSnapshotArtifact) || len(persisted) == 0 {
		t.Fatalf("prefix cursor=%d err=%v", len(persisted), err)
	}
	baseRows, globalRows := base.Collection.Len(), global.Collection.Len()
	if baseRows == 0 || globalRows == 0 ||
		baseRows > expected.Relations[0].Rows || globalRows > expected.Relations[1].Rows {
		t.Fatalf("prefix relation rows base=%d global=%d", baseRows, globalRows)
	}
	stage, err = NewBundleSnapshotArtifactStage(expected, system, relations, persisted)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := stage.Receive(bytes.NewReader(artifact[stage.Offset():]), func(raw []byte) error {
		persisted = bytes.Clone(raw)
		return nil
	})
	if err != nil || !equalSnapshotArtifactManifest(manifest, expected) {
		t.Fatalf("resume manifest=%+v err=%v", manifest, err)
	}
	if base.Collection.Len() != expected.Relations[0].Rows ||
		global.Collection.Len() != expected.Relations[1].Rows {
		t.Fatalf("resumed rows base=%d/%d global=%d/%d",
			base.Collection.Len(), expected.Relations[0].Rows,
			global.Collection.Len(), expected.Relations[1].Rows)
	}
}
