package driver

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
)

// Pure catalog/command fixture: runs on hosts without sealed allocation. The
// physical rollover fixture additionally exercises the same publication path.
func schemaFenceFixture(t *testing.T) (string, []byte, []byte, replicatedSchemaStageMarker, replicatedSchemaActivation) {
	t.Helper()
	catalog, _ := childReservationCatalogFixture(t)
	catalog.ReplicatedApply, catalog.ReplicatedChildApply = catalog.ReplicatedChildApply, nil
	source, err := appendCatalogJSON(nil, catalog)
	if err != nil {
		t.Fatal(err)
	}
	sourceImage, err := ValidateReplicatedSchemaCatalogImage(source)
	if err != nil {
		t.Fatal(err)
	}
	from := replicatedStateBindingAt(*catalog.ReplicatedShardStore, catalog.ReplicatedApply.Placement.Range)
	catalog.ReplicatedShardStore.Binding.Authority.SchemaGeneration++
	catalog.ReplicatedShardStore.RelationSchemaGeneration++
	catalog.ReplicatedShardStore.RelationManifestDigest = replicatedRelationManifestDigest(*catalog.ReplicatedShardStore)
	catalog.ReplicatedApply.ValidationDigest = replicatedApplyProfileDigest(*catalog.ReplicatedShardStore, catalog.ReplicatedApply.Placement)
	target, err := appendCatalogJSON(nil, catalog)
	if err != nil {
		t.Fatal(err)
	}
	image, err := ValidateReplicatedSchemaCatalogImage(target)
	if err != nil {
		t.Fatal(err)
	}
	marker := replicatedSchemaStageMarker{schemaGeneration: image.SchemaGeneration, sourceApplied: 11,
		membership:    durable.CheckpointMembershipWitness{Sequence: 2, Source: [32]byte{1}, Target: [32]byte{2}},
		catalogDigest: image.Digest, relationWitness: [32]byte{3}, applyContract: [32]byte{4},
		authorization: [32]byte{5}, targetWitness: [32]byte{6}, storages: [][32]byte{{7}}, sourceStorages: [][32]byte{{8}}}
	transition := replicatedstate.SchemaTransition{From: from, ToSchemaGeneration: image.SchemaGeneration,
		ExpectedReplicaSetVersion: 1,
		MembershipSequence:        marker.membership.Sequence, MembershipSource: marker.membership.Source, MembershipTarget: marker.membership.Target,
		FromManifest: sourceImage.RelationManifestDigest, FromApplyContract: [32]byte{9},
		ToManifest: image.RelationManifestDigest, ToApplyContract: marker.applyContract,
		RequestDigest: marker.authorization, AuthorizationDigest: [32]byte{10}}
	transition.CatalogCASDigest = replicatedSchemaCatalogCASDigest(sourceImage.Digest, image.Digest,
		transition.RequestDigest, transition.AuthorizationDigest)
	command, err := replicatedstate.AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "schema.vdb")
	path, err = canonicalCatalogPath(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path+".tables", 0o700); err != nil {
		t.Fatal(err)
	}
	return path, source, target, marker, replicatedSchemaActivation{targetDigest: image.Digest, command: command}
}

func TestReplicatedSchemaActivationEmptySuffixRoundTrip(t *testing.T) {
	_, _, _, _, activation := schemaFenceFixture(t)
	activation.preparedApplied, activation.preCommandApplied = 7, 11
	raw, err := encodeReplicatedSchemaActivation(activation)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeReplicatedSchemaActivation(raw)
	if err != nil || decoded.preparedApplied != 7 || decoded.preCommandApplied != 11 ||
		decoded.targetDigest != activation.targetDigest || !bytes.Equal(decoded.command, activation.command) {
		t.Fatalf("activation round trip: %+v err=%v", decoded, err)
	}
}

func TestReplicatedSchemaArtifactRetriesRequireSuccessfulDirectoryFence(t *testing.T) {
	for _, name := range []string{"target-catalog", "stage-marker", "activation"} {
		t.Run(name, func(t *testing.T) {
			path, _, target, marker, activation := schemaFenceFixture(t)
			image, err := ValidateReplicatedSchemaCatalogImage(target)
			if err != nil {
				t.Fatal(err)
			}
			write := func() error { return writeReplicatedSchemaTargetCatalog(path+".tables", target, image) }
			file := replicatedSchemaTargetCatalogName
			if name == "stage-marker" {
				write = func() error { return writeReplicatedSchemaStageMarker(path+".tables", marker) }
				file = replicatedSchemaStageMarkerName
			} else if name == "activation" {
				write = func() error { return writeReplicatedSchemaActivation(path+".tables", activation) }
				file = replicatedSchemaActivationName
			}
			injected := errors.New("schema directory unavailable")
			blocked, attempts := true, 0
			replicatedSchemaDirectorySyncHook = func(string) error {
				attempts++
				if blocked {
					return injected
				}
				return nil
			}
			t.Cleanup(func() { replicatedSchemaDirectorySyncHook = nil })
			for range 3 {
				if err := write(); !errors.Is(err, durable.ErrCommitOutcomeUnknown) || !errors.Is(err, injected) {
					t.Fatalf("readable artifact acknowledged without durability: %v", err)
				}
				if _, err := os.Stat(filepath.Join(path+".tables", file)); err != nil {
					t.Fatal("fixture did not reach post-rename uncertainty", err)
				}
			}
			if attempts != 3 {
				t.Fatalf("retry skipped directory fence: %d", attempts)
			}
			blocked = false
			if err := write(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReplicatedSchemaObservationsFenceAuthorityAndResumeExactSource(t *testing.T) {
	path, source, target, marker, activation := schemaFenceFixture(t)
	image, _ := ValidateReplicatedSchemaCatalogImage(target)
	if err := writeReplicatedSchemaTargetCatalog(path+".tables", target, image); err != nil {
		t.Fatal(err)
	}
	if err := writeReplicatedSchemaStageMarker(path+".tables", marker); err != nil {
		t.Fatal(err)
	}
	if err := writeReplicatedSchemaActivation(path+".tables", activation); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, source, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, published, err := ObservePublishedReplicatedSchemaTransition(path); err != nil || published {
		t.Fatalf("exact source must resume as unpublished: %t %v", published, err)
	}
	observed, found, err := ObservePersistedReplicatedSchemaTransition(path)
	if err != nil || !found || !bytes.Equal(observed.Bytes(), activation.command) {
		t.Fatalf("original command lost on prepublication restart: %t %v", found, err)
	}
	// A canonical command with substituted authority does not authenticate the
	// same source CAS, even though its own frame checksum is valid.
	foreign := observed.SchemaTransition
	foreign.CatalogCASDigest[0] ^= 1
	foreignCommand, err := replicatedstate.AppendSchemaTransition(nil, foreign)
	if err != nil {
		t.Fatal(err)
	}
	foreignRaw, _ := encodeReplicatedSchemaActivation(replicatedSchemaActivation{targetDigest: activation.targetDigest, command: foreignCommand})
	if err := os.WriteFile(filepath.Join(path+".tables", replicatedSchemaActivationName), foreignRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, published, err := ObservePublishedReplicatedSchemaTransition(path); published || !errors.Is(err, ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("foreign source CAS accepted: %t %v", published, err)
	}
	activationRaw, _ := encodeReplicatedSchemaActivation(activation)
	if err := os.WriteFile(filepath.Join(path+".tables", replicatedSchemaActivationName), activationRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, target, 0o600); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("schema publication fence unavailable")
	replicatedSchemaDirectorySyncHook = func(string) error { return injected }
	t.Cleanup(func() { replicatedSchemaDirectorySyncHook = nil })
	for range 2 {
		if _, found, err := ObservePreparedReplicatedSchemaTarget(path, target, marker.authorization); found || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			t.Fatalf("prepare observation skipped fence: %t %v", found, err)
		}
		if _, found, err := ObservePersistedReplicatedSchemaTransition(path); found || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			t.Fatalf("command observation skipped fence: %t %v", found, err)
		}
		if _, found, err := ObservePublishedReplicatedSchemaTransition(path); found || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			t.Fatalf("publication observation skipped fence: %t %v", found, err)
		}
		if _, _, found, err := PublishedReplicatedSchemaActivationIdentity(path); found || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			t.Fatalf("identity observation skipped fence: %t %v", found, err)
		}
	}
	// Artifact names are durable but the serving catalog's parent still is not.
	replicatedSchemaDirectorySyncHook = func(directory string) error {
		if directory == filepath.Dir(path) {
			return injected
		}
		return nil
	}
	if _, found, err := ObservePublishedReplicatedSchemaTransition(path); found || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("readable target catalog authorized selection before parent fence: %t %v", found, err)
	}
	replicatedSchemaDirectorySyncHook = nil
	if got, found, err := ObservePublishedReplicatedSchemaTransition(path); err != nil || !found || sha256.Sum256(got.Bytes()) != sha256.Sum256(activation.command) {
		t.Fatalf("repaired publication not observable: %t %v", found, err)
	}
}

func testPublishSchemaCatalogFence(t *testing.T, claim *ReplicatedApply, core *database) {
	t.Helper()
	injected := errors.New("catalog directory fence unavailable")
	oldSync := core.syncDir
	core.syncDir = func(string) error { return injected }
	t.Cleanup(func() { core.syncDir = oldSync })
	for range 3 {
		published, err := claim.PublishReplicatedSchemaCatalog()
		if !published || !errors.Is(err, durable.ErrCommitOutcomeUnknown) || !errors.Is(err, injected) {
			t.Fatalf("catalog CAS retry did not retain uncertain publication: %t %v", published, err)
		}
	}
	core.syncDir = oldSync
}

func testSchemaTargetSelectionFence(t *testing.T, path string, source, target ReplicatedShardStoreIdentity, apply ReplicatedApplyIdentity) {
	t.Helper()
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("selected catalog name not durable")
	replicatedSchemaDirectorySyncHook = func(directory string) error {
		if directory == filepath.Dir(absolute) {
			return injected
		}
		return nil
	}
	t.Cleanup(func() { replicatedSchemaDirectorySyncHook = nil })
	for range 2 {
		opened, err := OpenReplicatedShardStoreWithSchemaTransition(path, target, apply)
		if opened != nil || !errors.Is(err, durable.ErrCommitOutcomeUnknown) || !errors.Is(err, injected) {
			t.Fatalf("target selected before catalog parent fence: %v %v", opened, err)
		}
		for _, relation := range source.Relations {
			if _, err := os.Stat(filepath.Join(path+".tables", relation.Storage+".vjc")); err != nil {
				t.Fatal("source moved before selected catalog was durable", err)
			}
		}
		for _, relation := range target.Relations {
			if _, err := os.Stat(filepath.Join(path+".tables", relation.Storage+".vjc")); !os.IsNotExist(err) {
				t.Fatal("target moved before selected catalog was durable", err)
			}
		}
	}
	replicatedSchemaDirectorySyncHook = nil
}
