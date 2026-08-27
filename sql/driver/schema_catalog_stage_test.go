package driver

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestReplicatedSchemaTargetCertifiesFreshImmutableRelationImage(t *testing.T) {
	path, database, identity := bindReplicatedApplyTestRoot(t, "schema-target-proof")
	claim, applyIdentity, err := database.OpenReplicatedApply(
		identity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	targetDirectory, err := claim.ReplicatedSchemaTargetDirectory()
	if err != nil {
		t.Fatal(err)
	}
	core := database.connector.db
	core.mu.Lock()
	raw, err := appendCatalogJSON(nil, core.catalog)
	if err != nil {
		core.mu.Unlock()
		t.Fatal(err)
	}
	var decoded catalogFileVibe
	if err = decodeCatalogJSON(raw, &decoded); err != nil {
		core.mu.Unlock()
		t.Fatal(err)
	}
	target := catalogFile(decoded)
	target.ReplicatedShardStore.Binding.Authority.SchemaGeneration++
	target.ReplicatedShardStore.RelationSchemaGeneration++
	storage, err := core.newStorageIdentityLocked()
	if err != nil {
		core.mu.Unlock()
		t.Fatal(err)
	}
	target.ReplicatedShardStore.UserStorage = storage
	target.ReplicatedShardStore.Relations[0].Storage = storage
	target.Tables[target.ReplicatedShardStore.UserTable].Storage = storage
	target.ReplicatedShardStore.RelationManifestDigest =
		replicatedRelationManifestDigest(*target.ReplicatedShardStore)
	target.ReplicatedApply.ValidationDigest = replicatedApplyProfileDigest(
		*target.ReplicatedShardStore, target.ReplicatedApply.Placement,
	)
	candidate := &table{meta: target.Tables[target.ReplicatedShardStore.UserTable]}
	core.mu.Unlock()
	// Backfill images are built outside the fixed serving namespace. This
	// source is empty, so its exact replacement is an independently closed root.
	candidate.file, err = os.OpenFile(filepath.Join(targetDirectory, storage+".vjc"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	candidate.collection, err = durable.Create(candidate.file, durableOptions(candidate))
	if err != nil {
		_ = candidate.file.Close()
		t.Fatal(err)
	}
	if err = candidate.collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err = candidate.file.Close(); err != nil {
		t.Fatal(err)
	}
	targetRaw, err := appendCatalogJSON(nil, target)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := claim.CertifyReplicatedSchemaTarget(targetRaw)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Catalog.SchemaGeneration != identity.RelationSchemaGeneration+1 ||
		proof.Relations.RelationCount != 1 ||
		proof.Relations.ManifestDigest != proof.Catalog.RelationManifestDigest ||
		proof.Relations.Witness == ([32]byte{}) || proof.ApplyContract == ([32]byte{}) ||
		proof.Witness == ([32]byte{}) {
		t.Fatalf("target proof = %+v", proof)
	}
	prepared, err := claim.PrepareReplicatedSchemaTarget(
		targetRaw, claim.Applied(), [32]byte{0xa5},
	)
	if err != nil || prepared.SourceApplied != claim.Applied() ||
		prepared.Membership.Sequence == 0 || prepared.Membership.Source == ([32]byte{}) ||
		prepared.Membership.Target == ([32]byte{}) || prepared.Witness == proof.Witness {
		t.Fatalf("prepared target proof = %+v, %v", prepared, err)
	}
	observedWitness, found, err := ObservePreparedReplicatedSchemaTarget(
		path, targetRaw, [32]byte{0xa5},
	)
	if err != nil || !found || observedWitness != prepared.Witness {
		t.Fatalf("observed target witness=%x found=%t err=%v",
			observedWitness, found, err)
	}
	stagedCatalog, err := readReplicatedSchemaTargetCatalog(
		path+".tables", prepared.Catalog,
	)
	if err != nil || !bytes.Equal(stagedCatalog, targetRaw) || cap(stagedCatalog) != len(stagedCatalog) {
		t.Fatalf("staged target catalog bytes=%d/%d err=%v",
			len(stagedCatalog), cap(stagedCatalog), err)
	}
	catalogCAS, err := claim.ReplicatedSchemaCatalogCASDigest(
		prepared, [32]byte{0xa5}, [32]byte{0xb6},
	)
	if err != nil || catalogCAS == ([32]byte{}) {
		t.Fatalf("catalog CAS digest=%x err=%v", catalogCAS, err)
	}
	command, err := claim.AppendReplicatedSchemaTransition(nil, prepared,
		ReplicatedSchemaTransitionAuthority{
			RequestDigest: [32]byte{0xa5}, AuthorizationDigest: [32]byte{0xb6},
			CatalogCASDigest: catalogCAS,
		})
	opened, openErr := replicatedstate.OpenSchemaTransition(command)
	if err != nil || openErr != nil || opened.ToManifest != prepared.Catalog.RelationManifestDigest ||
		opened.ToApplyContract != prepared.ApplyContract ||
		opened.ToPlacementDigest != prepared.Relations.PlacementDigest ||
		opened.MembershipSequence != prepared.Membership.Sequence {
		t.Fatalf("schema transition = %+v, append=%v open=%v", opened, err, openErr)
	}
	spliced := opened.SchemaTransition
	spliced.ToPlacementDigest[0] ^= 1
	splicedCommand, err := replicatedstate.AppendSchemaTransition(nil, spliced)
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.PersistReplicatedSchemaTransition(splicedCommand); !errors.Is(err, ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("spliced target placement persisted: %v", err)
	}
	if err = claim.PersistReplicatedSchemaTransition(command); err != nil {
		t.Fatal(err)
	}
	targetIdentity := target.ReplicatedShardStore.Clone()
	targetApplyIdentity := target.ReplicatedApply.identity()
	assertUnselected := func() {
		t.Helper()
		opened, err := OpenReplicatedShardStoreWithSchemaTransition(path, targetIdentity, targetApplyIdentity)
		if opened != nil || err == nil {
			t.Fatalf("unpublished target opened: database=%v err=%v", opened, err)
		}
		if _, err := os.Stat(filepath.Join(path+".tables", identity.UserStorage+".vjc")); err != nil {
			t.Fatalf("source moved before target catalog authority: %v", err)
		}
	}
	assertUnselected()
	activation, found, err := readReplicatedSchemaActivation(path + ".tables")
	if err != nil || !found || activation.targetDigest != prepared.Catalog.Digest ||
		!bytes.Equal(activation.command, command) || cap(activation.command) != len(activation.command) {
		t.Fatalf("activation found=%t target=%x command=%d/%d err=%v", found,
			activation.targetDigest, len(activation.command), cap(activation.command), err)
	}
	if err = claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, identity, applyIdentity)
	if err != nil {
		t.Fatal(err)
	}
	marker, found, err := readReplicatedSchemaStageMarker(path + ".tables")
	if err != nil || !found || marker.membership != prepared.Membership ||
		marker.catalogDigest != prepared.Catalog.Digest ||
		marker.relationWitness != prepared.Relations.Witness ||
		marker.placementDigest != prepared.Relations.PlacementDigest ||
		marker.applyContract != prepared.ApplyContract || marker.targetWitness != prepared.Witness {
		t.Fatalf("reopened stage marker = %+v, found=%v err=%v", marker, found, err)
	}
	if _, err = os.Stat(filepath.Join(targetDirectory, storage+".vjc")); err != nil {
		t.Fatalf("reopen removed prepared target: %v", err)
	}
	stagedCatalog, err = readReplicatedSchemaTargetCatalog(
		path+".tables", prepared.Catalog,
	)
	if err != nil || !bytes.Equal(stagedCatalog, targetRaw) {
		t.Fatalf("reopened target catalog bytes=%d err=%v", len(stagedCatalog), err)
	}
	reopenedClaim, _, err := reopened.OpenReplicatedApply(
		identity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := reopenedClaim.RecoverPreparedReplicatedSchemaTarget(
		targetRaw, [32]byte{0xa5},
	)
	if err != nil || recovered != prepared {
		t.Fatalf("recovered target proof = %+v, want %+v, err=%v",
			recovered, prepared, err)
	}
	if published, err := reopenedClaim.PublishReplicatedSchemaCatalog(); published || err == nil {
		t.Fatalf("catalog finalized before committed schema entry: published=%v err=%v", published, err)
	}
	if _, err = reopenedClaim.ApplyNormal(
		testReplicatedApplyMeta(prepared.SourceApplied+1), command,
	); err != nil {
		t.Fatal(err)
	}
	// A canonical command with the same target generation is still not the
	// exact committed entry. It must fail before membership finalization.
	substituted := opened.SchemaTransition
	substituted.AuthorizationDigest[0] ^= 1
	substitutedCommand, err := replicatedstate.AppendSchemaTransition(nil, substituted)
	if err != nil {
		t.Fatal(err)
	}
	substitutedRecord, err := encodeReplicatedSchemaActivation(replicatedSchemaActivation{
		targetDigest: prepared.Catalog.Digest, command: substitutedCommand,
	})
	if err != nil {
		t.Fatal(err)
	}
	activationPath := filepath.Join(path+".tables", replicatedSchemaActivationName)
	originalRecord, err := os.ReadFile(activationPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(activationPath, substitutedRecord, 0o600); err != nil {
		t.Fatal(err)
	}
	if published, err := reopenedClaim.PublishReplicatedSchemaCatalog(); published || err == nil {
		t.Fatalf("substituted canonical command finalized membership: published=%v err=%v", published, err)
	}
	if err := durable.ValidateFinalizedCheckpointMembershipTransition(path+".tables", prepared.Membership, [32]byte{0xa5}, prepared.SourceApplied+1, sha256.Sum256(command)); err == nil {
		t.Fatal("substituted canonical command wrote finalization")
	}
	if err := os.WriteFile(activationPath, originalRecord, 0o600); err != nil {
		t.Fatal(err)
	}
	assertUnselected()
	published, err := reopenedClaim.PublishReplicatedSchemaCatalog()
	if err != nil || !published {
		t.Fatalf("catalog publish=%t err=%v", published, err)
	}
	publishedRaw, found, err := readCatalogFile(path)
	if err != nil || !found || !bytes.Equal(publishedRaw, targetRaw) {
		t.Fatalf("published catalog found=%t bytes=%d err=%v", found, len(publishedRaw), err)
	}
	observedTransition, found, err := ObservePublishedReplicatedSchemaTransition(path)
	if err != nil || !found || observedTransition.SchemaTransition != opened.SchemaTransition {
		t.Fatalf("published transition=%+v found=%t err=%v",
			observedTransition, found, err)
	}
	// The source is now fenced, but its live descriptors still own journal
	// paths. Namespace promotion must wait for actual writer-lease release.
	if active, err := OpenReplicatedShardStoreWithSchemaTransition(path, targetIdentity, targetApplyIdentity); active != nil || !errors.Is(err, storeio.ErrWriterLocked) {
		t.Fatalf("promotion while source handles live: database=%v err=%v", active, err)
	}
	if err = reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	// Crash at each actual link/unlink in turn. Every reopening must resume
	// from the durable target catalog without selecting an incomplete image.
	injected := errors.New("schema publication interrupted")
	t.Cleanup(func() { replicatedSchemaNamespaceFaultHook = nil })
	var activated *Database
	interruptions := 0
	for attempt := 0; attempt < 9; attempt++ {
		replicatedSchemaNamespaceFaultHook = func(stage string) error {
			if stage == "linked" || stage == "unlinked" {
				return injected
			}
			return nil
		}
		activated, err = OpenReplicatedShardStoreWithApply(path, targetIdentity, targetApplyIdentity)
		if err == nil {
			break
		}
		if activated != nil || !errors.Is(err, injected) || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			t.Fatalf("interrupted activation %d: database=%v err=%v", attempt, activated, err)
		}
		if drained, drainErr := DrainPublishedReplicatedSchemaSource(path, command); drained || drainErr == nil {
			t.Fatalf("drained source before selected target checkpoint: drained=%v err=%v", drained, drainErr)
		}
		interruptions++
	}
	replicatedSchemaNamespaceFaultHook = nil
	if activated == nil || err != nil || interruptions != 8 {
		t.Fatalf("target resume interruptions=%d database=%v err=%v", interruptions, activated, err)
	}
	activatedClaim, gotApplyIdentity, err := activated.OpenReplicatedApply(
		targetIdentity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if gotApplyIdentity != targetApplyIdentity ||
		activatedClaim.Applied() != prepared.SourceApplied+1 {
		t.Fatalf("target activation identity=%+v applied=%d err=%v",
			gotApplyIdentity, activatedClaim.Applied(), err)
	}
	if _, err := activatedClaim.ApplyNormal(testReplicatedApplyMeta(prepared.SourceApplied+2), nil); err != nil {
		t.Fatalf("target normal apply after schema activation: %v", err)
	}
	if err = activatedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = activated.Close(); err != nil {
		t.Fatal(err)
	}
	activated, err = OpenReplicatedShardStoreWithApply(path, targetIdentity, targetApplyIdentity)
	if err != nil {
		t.Fatalf("reopen target after later normal apply: %v", err)
	}
	activatedClaim, _, err = activated.OpenReplicatedApply(targetIdentity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil || activatedClaim.Applied() != prepared.SourceApplied+2 {
		t.Fatalf("reopened target after later normal apply: claim=%v err=%v", activatedClaim, err)
	}
	if err = activatedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = activated.Close(); err != nil {
		t.Fatal(err)
	}
	if drained, err := ObserveDrainedReplicatedSchemaSource(path, command); err != nil || drained {
		t.Fatalf("source drain before authority=%t err=%v", drained, err)
	}
	if drained, err := DrainPublishedReplicatedSchemaSource(path, command); err != nil || !drained {
		t.Fatalf("source drain=%t err=%v", drained, err)
	}
	if _, err = os.Stat(filepath.Join(path+".tables", replicatedSchemaSourcesDirectory, identity.Relations[0].Storage+".vjc")); !os.IsNotExist(err) {
		t.Fatalf("source relation survived drain: %v", err)
	}
	if _, err = os.Stat(path + ".tables/" + storage + ".vjc"); err != nil {
		t.Fatalf("target relation removed by source drain: %v", err)
	}
}
