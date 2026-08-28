package gateway

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func TestDurableRequestMembershipModeIsSealedAndReplayable(t *testing.T) {
	build := durableRequestProgramBuildFixture(t)
	legacy, err := BuildDurableRequestLogicalProgram(build)
	if err != nil {
		t.Fatal(err)
	}
	build.MembershipStable = true
	stable, err := BuildDurableRequestLogicalProgram(build)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Identity != stable.Identity || legacy.RequestID != stable.RequestID || legacy.KeyDigest != stable.KeyDigest || legacy.RequestDigest != stable.RequestDigest {
		t.Fatal("command mode changed durable client or transaction identity")
	}
	if legacy.Contract.ProtocolProgramDigest == stable.Contract.ProtocolProgramDigest || legacy.Contract.PinDigest == stable.Contract.PinDigest {
		t.Fatal("command mode was not bound into the program and execution pin")
	}
	var sizes []uint64
	for _, program := range []DurableRequestLogicalProgram{legacy, stable} {
		wantStable := program.Contract == stable.Contract
		measurement, pages := durableLogicalStreamBuild(t, build.Key, program)
		sizes = append(sizes, measurement.descriptor().TotalBytes)
		reader, err := openDurableRequestRecipeStream(build.Key, measurement.descriptor(), durableRequestPlanPageSource(pages))
		if err != nil {
			t.Fatal(err)
		}
		if reader.Contract != program.Contract || durableRequestMembershipStableProgram(reader.Contract) != wantStable {
			t.Fatal("reopen lost command mode")
		}
		sealed, err := SealDurableRequestLogicalProgram(program)
		if err != nil || sealed.Contract != program.Contract {
			t.Fatalf("reseal changed protocol mode: %v", err)
		}
		route, control := replicatedTransactionEncoderFixture(t)
		encoder := replicatedTransactionCommandEncoder{tenant: program.Tenant, membershipStable: durableRequestMembershipStableProgram(reader.Contract)}
		first, err := encoder.appendExact(nil, program.Identity.RetryHome, route, control, nil)
		if err != nil {
			t.Fatal(err)
		}
		fresh := replicatedTransactionCommandEncoder{tenant: program.Tenant, membershipStable: durableRequestMembershipStableProgram(reader.Contract)}
		replayed, err := fresh.appendExact(nil, program.Identity.RetryHome, route, control, nil)
		if err != nil || !bytes.Equal(first, replayed) {
			t.Fatalf("fresh encoder changed retained mode: %v", err)
		}
		command, err := replication.OpenCommand(first)
		if err != nil || replication.IsMembershipStableAuthority(command.AuthorityClass) != wantStable {
			t.Fatalf("wrong command authority: %v", err)
		}
		corrupt := program.Contract
		corrupt.ProtocolProgramDigest[0] ^= 1
		if validDurableRequestProtocolProgram(corrupt) {
			t.Fatal("unknown protocol program accepted")
		}
	}
	if sizes[0] != sizes[1] {
		t.Fatalf("mode increased durable plan bytes: %v", sizes)
	}
}

func TestDurableRequestRunnerRejectsUnknownCommandModeBeforeSideEffects(t *testing.T) {
	execution := typedExecutionFixture(t)
	execution.Recipe.Contract.ProtocolProgramDigest[0] ^= 1
	// No dependencies: reaching a ledger, authority, or proposer would panic.
	_, err := (&DurableRequestDistributedRunner{}).RunTyped(t.Context(), execution)
	if !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("unknown mode was not rejected: %v", err)
	}
}

func TestMembershipStableTransactionWitnessIgnoresOnlyMembership(t *testing.T) {
	route, _ := replicatedTransactionEncoderFixture(t)
	base := replicatedTransactionRouteAuthorityWitness(route, true)
	legacy := replicatedRouteAuthorityWitness(route)
	if base == legacy {
		t.Fatal("stable and legacy witness domains collide")
	}
	changed := route
	changed.Command.ReplicaSetVersion++
	if replicatedTransactionRouteAuthorityWitness(changed, true) != base || replicatedRouteAuthorityWitness(changed) == legacy {
		t.Fatal("membership semantics changed for the wrong command mode")
	}
	for name, mutate := range map[string]func(*ReplicatedRoute){
		"group":      func(r *ReplicatedRoute) { r.Group.GroupID[0]++ },
		"allocation": func(r *ReplicatedRoute) { r.AllocationGeneration++ },
		"schema":     func(r *ReplicatedRoute) { r.Command.SchemaGeneration++ },
		"manifest":   func(r *ReplicatedRoute) { r.Command.RelationManifestDigest[0]++ },
		"policy":     func(r *ReplicatedRoute) { r.Command.ActivePolicyGeneration++ },
		"protection": func(r *ReplicatedRoute) { r.Command.ProtectionEpoch++ },
		"ownership":  func(r *ReplicatedRoute) { r.Command.OwnershipEpoch++ },
		"routing":    func(r *ReplicatedRoute) { r.Command.RoutingVersion++ },
		"generation": func(r *ReplicatedRoute) { r.Command.RouteGeneration++ },
	} {
		t.Run(name, func(t *testing.T) {
			different := route
			mutate(&different)
			if replicatedTransactionRouteAuthorityWitness(different, true) == base {
				t.Fatal("logical authority dropped from witness")
			}
		})
	}
}
