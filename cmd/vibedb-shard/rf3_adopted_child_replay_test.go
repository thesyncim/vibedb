package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type rf3AdoptedTestApply struct {
	identity sqldriver.ReplicatedApplyIdentity
	profile  sqldriver.ReplicatedApplyCapacityProfile
	reads    int
}

func (apply *rf3AdoptedTestApply) Identity() (sqldriver.ReplicatedApplyIdentity, error) {
	apply.reads++
	return apply.identity, nil
}
func (apply *rf3AdoptedTestApply) CapacityQualificationProfile() (sqldriver.ReplicatedApplyCapacityProfile, error) {
	apply.reads++
	return apply.profile, nil
}

func TestRF3AdoptedChildReplayObservesExistingOwnerWithoutReopeningStage(t *testing.T) {
	resolver, descriptor, owner, _ := testRF3AdoptedSource(t)
	identity := owner.observation.Identity
	apply := &rf3AdoptedTestApply{identity: sqldriver.ReplicatedApplyIdentity{Storage: "apply"}, profile: sqldriver.ReplicatedApplyCapacityProfile{Initialized: true, RelationManifestDigest: identity.RelationManifestDigest}}
	apply.profile.Binding.MemberID = identity.MemberID
	apply.profile.Binding.StoreID = identity.StoreID
	target := splitcontroller.ChildReplicaTarget{Member: identity.MemberID, StoreID: identity.StoreID, Apply: apply.identity}
	target.SQL.Binding = apply.profile.Binding
	executor := &rf3AdoptedChildReplay{child: 1, owners: owner, live: rf3RetainedSource{runtime: resolver.inventory.runtimes[descriptor.Group]}, target: target, apply: apply}
	observed, err := executor.observe(t.Context())
	if err != nil || observed == nil || observed.Phase != splitcontroller.ChildPhaseRuntimeAdopted || observed.RuntimeIdentity != identity || observed.WALBinding != target.SQL.Binding {
		t.Fatalf("observation=%+v err=%v", observed, err)
	}
	if apply.reads != 2 {
		t.Fatal("did not use bounded live apply proof")
	}
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.observe(t.Context()); err != nil {
		t.Fatal("operation close retired live runtime", err)
	}
	owner.observation.Identity.NodeIncarnation++
	before := apply.reads
	if _, err := executor.observe(t.Context()); err == nil {
		t.Fatal("stale owner accepted")
	}
	if apply.reads != before {
		t.Fatal("accessed closed/stale apply before checking owner incarnation")
	}
}
