package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestRF3NativeChildPublicationIsDurableAndExact(t *testing.T) {
	registry, gate, prepared, states := nativeAuthorityFixture(t, 3)
	inventory := testRF3AdoptedInventory(t)
	authority, err := newRF3NativeAuthorities(registry, gate, prepared[:1], nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	authority.adopted = inventory
	state := states[1]
	if authority.serving(state) {
		t.Fatal("unpublished child admitted")
	}
	// Exercise the post-certificate publication seam used by
	// CheckpointChildAdoption, including the real durable inventory writer.
	entry := testRF3AdoptedEntry(1)
	runtime := rf3AdoptedRuntime{identity: state.Identity}
	if err = inventory.recordNativeChild(entry, runtime); err != nil {
		t.Fatal(err)
	}
	first := inventory.nativeChildren.Load()
	if !authority.serving(state) {
		t.Fatal("durably adopted child refused")
	}
	if err = inventory.recordNativeChild(entry, runtime); err != nil || inventory.nativeChildren.Load() != first {
		t.Fatal("exact retry rebuilt immutable snapshot", err)
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = authority.serving(state) }); allocs != 0 {
		t.Fatalf("child serving allocations: %g", allocs)
	}
	for name, mutate := range map[string]func(*raftservice.ServingState){
		"cluster":             func(s *raftservice.ServingState) { s.Identity.Group.ClusterID[0]++ },
		"cluster incarnation": func(s *raftservice.ServingState) { s.Identity.Group.ClusterIncarnation[0]++ },
		"topology epoch":      func(s *raftservice.ServingState) { s.Identity.Group.TopologyRecoveryEpoch++ },
		"group":               func(s *raftservice.ServingState) { s.Identity.Group.GroupID[0]++ },
		"shard incarnation":   func(s *raftservice.ServingState) { s.Identity.Group.ShardIncarnation[0]++ },
		"member":              func(s *raftservice.ServingState) { s.Identity.MemberID++ },
		"store":               func(s *raftservice.ServingState) { s.Identity.StoreID[0]++ },
		"node incarnation":    func(s *raftservice.ServingState) { s.Identity.NodeIncarnation++ },
		"allocation":          func(s *raftservice.ServingState) { s.Identity.AllocationGeneration++ },
		"distribution":        func(s *raftservice.ServingState) { s.Identity.Distribution += "foreign" },
		"shard":               func(s *raftservice.ServingState) { s.Identity.Shard += "foreign" },
		"runtime digest":      func(s *raftservice.ServingState) { s.Identity.RelationManifestDigest[0]++ },
		"command digest":      func(s *raftservice.ServingState) { s.Command.RelationManifestDigest[0]++ },
		"replica version":     func(s *raftservice.ServingState) { s.Command.ReplicaSetVersion++ },
		"invalid command":     func(s *raftservice.ServingState) { s.Command.SchemaGeneration = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := state
			mutate(&changed)
			if authority.serving(changed) {
				t.Fatal("foreign child admitted")
			}
		})
	}
	installed := state
	installed.Identity.RelationManifestDigest[0]++
	installed.Command.RelationManifestDigest = installed.Identity.RelationManifestDigest
	installed.Command.SchemaGeneration++
	if !authority.serving(installed) {
		t.Fatal("owner-installed schema generation was frozen by child adoption digest")
	}
	changed := runtime
	changed.identity.NodeIncarnation++
	if err = inventory.recordNativeChild(entry, changed); err == nil || inventory.nativeChildren.Load() != first {
		t.Fatal("conflicting runtime replaced certified child", err)
	}
	if err = inventory.recordNativeChild(testRF3AdoptedEntry(2), rf3AdoptedRuntime{identity: states[2].Identity}); err != nil {
		t.Fatal(err)
	}
	if len(*first) != 1 || len(*inventory.nativeChildren.Load()) != 2 {
		t.Fatal("published snapshot mutated")
	}
	if !authority.serving(states[2]) {
		t.Fatal("second child not published")
	}
	if err = inventory.Close(); err != nil {
		t.Fatal(err)
	}
	if authority.serving(state) || authority.serving(states[2]) {
		t.Fatal("closed inventory retained native authority")
	}
}

func TestRF3NativeChildFailedPublicationExposesNoNewAuthority(t *testing.T) {
	registry, _, _, states := nativeAuthorityFixture(t, 3)
	inventory := testRF3AdoptedInventory(t)
	if err := inventory.recordNativeChild(testRF3AdoptedEntry(1), rf3AdoptedRuntime{identity: states[1].Identity}); err != nil {
		t.Fatal(err)
	}
	first := inventory.nativeChildren.Load()
	path := filepath.Join(inventory.manifest.ReplicaControl.SourceDataRoot, "adopted-groups.state")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := inventory.recordNativeChild(testRF3AdoptedEntry(2), rf3AdoptedRuntime{identity: states[2].Identity}); err == nil || !inventory.failed {
		t.Fatal("uncertain publication accepted")
	}
	if inventory.nativeChildren.Load() != first || len(inventory.runtimes) != 1 || inventory.nativeServing(registry, states[2]) {
		t.Fatal("failed publication exposed child authority")
	}
	if !inventory.nativeServing(registry, states[1]) {
		t.Fatal("unrelated committed child authority lost")
	}
}

func TestRF3NativeChildRequiresCurrentVoterRegistry(t *testing.T) {
	_, _, _, states := nativeAuthorityFixture(t, 2)
	inventory := testRF3AdoptedInventory(t)
	state := states[1]
	if err := inventory.recordNativeChild(testRF3AdoptedEntry(1), rf3AdoptedRuntime{identity: state.Identity}); err != nil {
		t.Fatal(err)
	}
	for _, role := range []rafttransport.MemberRole{rafttransport.MemberEnrolled, rafttransport.MemberLearner, rafttransport.MemberVoter} {
		registry, err := rafttransport.NewStaticRegistry(rafttransport.NodeID{1}, []rafttransport.Member{
			{Group: state.Identity.Group, MemberID: state.Identity.MemberID, Node: rafttransport.NodeID{1}, Role: role, ReplicaSetVersion: state.Command.ReplicaSetVersion},
			{Group: state.Identity.Group, MemberID: 2, Node: rafttransport.NodeID{2}, Role: rafttransport.MemberVoter, ReplicaSetVersion: state.Command.ReplicaSetVersion},
		}, rafttransport.Limits{MaxGroups: 1, MaxMembers: 2})
		if err != nil {
			t.Fatal(err)
		}
		if inventory.nativeServing(registry, state) != (role == rafttransport.MemberVoter) {
			t.Fatalf("role %v authority mismatch", role)
		}
	}
}

func TestRF3NativeChildCapacityAndConcurrentSnapshots(t *testing.T) {
	registry, _, _, states := nativeAuthorityFixture(t, maxRF3ManifestGroups)
	inventory := testRF3AdoptedInventory(t)
	if err := inventory.recordNativeChild(testRF3AdoptedEntry(1), rf3AdoptedRuntime{identity: states[1].Identity}); err != nil {
		t.Fatal(err)
	}
	var readers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for index := 0; index < 2000; index++ {
				if !inventory.nativeServing(registry, states[1]) {
					t.Error("committed child disappeared")
					return
				}
			}
		}()
	}
	limit := maxRF3ManifestGroups - len(inventory.manifest.groupBundles())
	for index := 2; index <= limit; index++ {
		if err := inventory.recordNativeChild(testRF3AdoptedEntry(byte(index)), rf3AdoptedRuntime{identity: states[index].Identity}); err != nil {
			t.Fatal(err)
		}
	}
	readers.Wait()
	before := inventory.nativeChildren.Load()
	if err := inventory.recordNativeChild(testRF3AdoptedEntry(byte(limit+1)), rf3AdoptedRuntime{identity: states[limit+1].Identity}); err == nil {
		t.Fatal("native child capacity exceeded")
	}
	if len(*before) != limit || inventory.nativeChildren.Load() != before || inventory.liveCount() != limit {
		t.Fatal("capacity failure mutated published inventory")
	}
	if !inventory.nativeChildCapacity(states[1].Identity) {
		t.Fatal("full capacity rejected exact retry")
	}
	if inventory.nativeChildCapacity(raftmember.RuntimeIdentity{}) {
		t.Fatal("full capacity admitted another identity")
	}
}
