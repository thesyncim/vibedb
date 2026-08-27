package driver

import (
	"bytes"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
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
	err = core.buildReplacementStorageLocked(
		t.Context(), target.ReplicatedShardStore.UserTable,
		core.tables[target.ReplicatedShardStore.UserTable], candidate, true,
	)
	core.mu.Unlock()
	if err != nil {
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
		opened.MembershipSequence != prepared.Membership.Sequence {
		t.Fatalf("schema transition = %+v, append=%v open=%v", opened, err, openErr)
	}
	if err = claim.PersistReplicatedSchemaTransition(command); err != nil {
		t.Fatal(err)
	}
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
		marker.applyContract != prepared.ApplyContract || marker.targetWitness != prepared.Witness {
		t.Fatalf("reopened stage marker = %+v, found=%v err=%v", marker, found, err)
	}
	if _, err = os.Stat(path + ".tables/" + storage + ".vjc"); err != nil {
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
	if _, err = reopenedClaim.ApplyNormal(
		testReplicatedApplyMeta(prepared.SourceApplied+1), command,
	); err != nil {
		t.Fatal(err)
	}
	published, err := reopenedClaim.PublishReplicatedSchemaCatalog()
	if err != nil || !published {
		t.Fatalf("catalog publish=%t err=%v", published, err)
	}
	publishedRaw, found, err := readCatalogFile(path)
	if err != nil || !found || !bytes.Equal(publishedRaw, targetRaw) {
		t.Fatalf("published catalog found=%t bytes=%d err=%v", found, len(publishedRaw), err)
	}
	if err = reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	targetIdentity := target.ReplicatedShardStore.Clone()
	targetApplyIdentity := target.ReplicatedApply.identity()
	activated, err := OpenReplicatedShardStoreWithApply(
		path, targetIdentity, targetApplyIdentity,
	)
	if err != nil {
		t.Fatal(err)
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
	if err = activatedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = activated.Close(); err != nil {
		t.Fatal(err)
	}
}
