//go:build linux

package main

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/servicetls"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestRF3RetiredFirstGroupPreservesSurvivorLogIdentity(t *testing.T) {
	input := prepareRF3NodeTestInput(t)
	if err := provisionRF3Node(input); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadRF3Manifest(filepath.Join(input.Root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := servicetls.LoadProfile(manifest.TLS.Certificate, manifest.TLS.Key,
		manifest.TLS.Roots, manifest.TLS.IdentityOID, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := openRF3NodeOwner(manifest, profile)
	if err != nil {
		t.Fatal(err)
	}
	set, err := prepareRF3GroupSetOnNode(manifest, profile, sqldriver.ReplicatedOpenOptions{}, owner)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.groups) != 2 {
		t.Fatalf("prepared groups=%d", len(set.groups))
	}
	var descriptors [2]raftstore.GroupDescriptor
	var identities [2]raftmember.RuntimeIdentity
	for index := range set.groups {
		item := &set.groups[index]
		descriptors[index], err = item.nodeLog.Descriptor()
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := item.adoptRuntime()
		if err != nil {
			t.Fatal(err)
		}
		identities[index] = runtime.Identity()
		if !slices.Contains(item.publication.ConfState.GetVoters(), identities[index].MemberID) {
			t.Fatal("fixture source already removed itself")
		}
		if err = runtime.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	command := commandFenceFromPublication(set.groups[0].base.Binding.Authority, identities[0], set.groups[0].publication.ReplicaSetVersion+3)
	record := replicaaction.Record{Revision: 1, State: replicaaction.Running,
		Request: replicaaction.Request{Operation: [32]byte{41}, Step: [32]byte{42}, Kind: replicaaction.SourceRetirement,
			Fence: raftservice.ServingFence{Group: identities[0].Group, AllocationGeneration: identities[0].AllocationGeneration,
				MemberID: identities[0].MemberID, StoreID: identities[0].StoreID, NodeIncarnation: identities[0].NodeIncarnation,
				Command: command, Term: 3}, SourceMember: identities[0].MemberID, TargetMember: 4}}
	journalPath := t.TempDir()
	journal, err := replicaaction.OpenFileJournal(journalPath, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.PublishReplicaAction(t.Context(), 0, record); err != nil {
		t.Fatal(err)
	}
	record.State, record.Revision = replicaaction.RetirementAuthorized, 2
	if err = journal.PublishReplicaAction(t.Context(), 1, record); err != nil {
		t.Fatal(err)
	}
	// Crash after the proof marker: the old group's durable Raft configuration
	// still includes this member and the immutable manifest still lists it first.
	for restart := 0; restart < 2; restart++ {
		if err = journal.Close(); err != nil {
			t.Fatal(err)
		}
		journal, err = replicaaction.OpenFileJournal(journalPath, 8)
		if err != nil {
			t.Fatal(err)
		}
		retirements, err := journal.SourceRetirements(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		owner, err = openRF3NodeOwner(manifest, profile)
		if err != nil {
			t.Fatal(err)
		}
		recovered, err := prepareRF3GroupSetOnNodeWithRetirements(manifest, profile,
			sqldriver.ReplicatedOpenOptions{}, owner, retirements)
		if err != nil || len(recovered.groups) != 1 {
			t.Fatalf("restart %d groups=%d err=%v", restart, len(recovered.groups), err)
		}
		survivor := &recovered.groups[0]
		descriptor, err := survivor.nodeLog.Descriptor()
		if err != nil || descriptor != descriptors[1] || !survivor.base.Equal(set.groups[1].base) ||
			survivor.applyIdentity != set.groups[1].applyIdentity || survivor.manifest.SQL.Path != manifest.Groups[1].SQL.Path {
			t.Fatalf("restart %d renumbered or rebound survivor: descriptor=%+v want=%+v err=%v", restart, descriptor, descriptors[1], err)
		}
		runtime, err := survivor.adoptRuntime()
		if err != nil {
			t.Fatal(err)
		}
		identity := runtime.Identity()
		if identity.Group != identities[1].Group || identity.StoreID != identities[1].StoreID ||
			identity.NodeIncarnation != identities[1].NodeIncarnation+uint64(restart+1) {
			t.Fatalf("wrong recovered survivor: %+v", identity)
		}
		retired, found := owner.store.GroupByID(identities[0].Group.GroupID)
		if !found {
			t.Fatal("retirement deleted retained source storage")
		}
		if incarnation, err := retired.NodeIncarnation(); err != nil || incarnation != identities[0].NodeIncarnation {
			t.Fatalf("retired runtime reopened: incarnation=%d err=%v", incarnation, err)
		}
		if err = runtime.Close(); err != nil {
			t.Fatal(err)
		}
		if err = owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
}
