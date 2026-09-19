package main

import (
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

func TestRF3ChildAdmissionPersistsDynamicDiscriminator(t *testing.T) {
	manifest := rf3Manifest{NodeLog: &rf3NodeLogManifest{}, Digest: [32]byte{1}, ReplicaControl: rf3ManifestReplicaControl{SourceDataRoot: t.TempDir()}}
	store, slots, err := openRF3ChildAdmissionStore(manifest.ReplicaControl.SourceDataRoot, manifest.Digest, 1, manifest)
	if err != nil {
		t.Fatal(err)
	}
	slots[0] = rf3GroupChildPrepareSlot{operation: [32]byte{3}, group: rf3DynamicTemplateSlot,
		certificates: [3][32]byte{{4}}, requests: [3][32]byte{{5}}}
	if err := store.save(slots); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, recovered, err := openRF3ChildAdmissionStore(manifest.ReplicaControl.SourceDataRoot, manifest.Digest, 1, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != slots {
		t.Fatal("dynamic discriminator became a startup group index")
	}
	slots[0].group = rf3DynamicTemplateSlot + 1
	if err := store.save(slots); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, _, err := openRF3ChildAdmissionStore(manifest.ReplicaControl.SourceDataRoot, manifest.Digest, 1, manifest); err == nil {
		_ = reopened.Close()
		t.Fatal("accepted unknown persisted template discriminator")
	}
}

func TestRF3DynamicChildTemplateFencesOperationSourceAcrossChildren(t *testing.T) {
	fixture := testRF3DynamicTemplateFixture(t)
	catalog, err := openRF3DynamicChildTemplateCatalog(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	if _, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err != nil {
		t.Fatal(err)
	}
	target := fixture.preparation.Target()
	target.Child = 2
	for index := range target.Replicas {
		replica := &target.Replicas[index]
		registry := fixture.registry
		registry.Root = filepath.Dir(filepath.Dir(replica.RuntimeRoot))
		paths, err := registry.childPaths(fixture.preparation.OperationID(), target.Child)
		if err != nil {
			t.Fatal(err)
		}
		replica.RuntimeRoot, replica.SQLPath, replica.WALPath = paths.Root, paths.Database, paths.WAL
	}
	makeSibling := func(allocation [32]byte) splitcontroller.ChildPreparation {
		preparation, err := splitcontroller.NewChildPreparation(fixture.preparation.OperationID(), allocation,
			fixture.preparation.Descriptor(), fixture.preparation.Collection(), target, fixture.preparation.ReplicaIndex())
		if err != nil {
			t.Fatal(err)
		}
		return preparation
	}
	sibling := makeSibling(fixture.preparation.AllocationDigest())
	changedSource := fixture.source
	changedSource.GroupID[0]++
	if _, err := catalog.Publish(sibling, changedSource, fixture.registry, fixture.bootstrap, fixture.peers); err == nil {
		t.Fatal("same operation acquired another source through a different child")
	}
	changedAllocation := fixture.preparation.AllocationDigest()
	changedAllocation[0]++
	if _, err := catalog.Publish(makeSibling(changedAllocation), fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err == nil {
		t.Fatal("same operation acquired a different allocation through a sibling")
	}
	if _, found, err := catalog.Read(fixture.preparation.OperationID(), target.Child); err != nil || found {
		t.Fatalf("rejected sibling left authority: found=%v err=%v", found, err)
	}
	if _, err := catalog.Publish(sibling, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err != nil {
		t.Fatalf("valid sibling rejected: %v", err)
	}
}
