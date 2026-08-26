package gateway

import (
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func durableRequestProgramBuildFixture(t *testing.T) DurableRequestLogicalProgramBuild {
	t.Helper()
	participants := durableFaultParticipants(t)
	request := durableFaultRequest(t, participants)
	topology := durableFaultTopology(t, participants)
	point, err := requestledger.Home(request.Key.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	home, _, ok := topology.Lookup(point)
	if !ok {
		t.Fatal("request ledger home is missing")
	}
	return DurableRequestLogicalProgramBuild{
		Home: home, Key: request.Key, Tenant: slices.Clone(request.Program.Tenant),
		CatalogGeneration: request.Program.Identity.CatalogGeneration,
		RecoveryDeadline:  3, PlanningLeaseExpiryIndex: 100,
		PlanningLeaseGeneration: 1, PinEpoch: 1, Participants: participants,
	}
}

func TestBuildDurableRequestLogicalProgramDerivesCompleteAggregateContract(t *testing.T) {
	build := durableRequestProgramBuildFixture(t)
	program, err := BuildDurableRequestLogicalProgram(build)
	if err != nil || !validDurableRequestLogicalProgram(program) {
		t.Fatalf("program valid=%v err=%v", validDurableRequestLogicalProgram(program), err)
	}
	contract := program.Contract
	if contract.ApplyContractDigest == (replication.Digest{}) ||
		contract.RouteSchemaCertificateDigest == (replication.Digest{}) ||
		contract.InitialStateDigest == (replication.Digest{}) ||
		contract.CommitTerminalStateDigest != replication.Digest(requestledger.NextStateDigest(
			durableRequestCommitTransitionTag,
			appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedCommitted}),
		)) ||
		contract.AbortTerminalStateDigest != replication.Digest(requestledger.NextStateDigest(
			durableRequestAbortTransitionTag,
			appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedAborted}),
		)) {
		t.Fatalf("incomplete derived contract: %+v", contract)
	}
	binding := executionpin.Binding{
		RequestKeyDigest:          executionpin.Digest(program.KeyDigest),
		RequestDigest:             executionpin.Digest(program.RequestDigest),
		CatalogGeneration:         program.Identity.CatalogGeneration,
		SchemaManifestDigest:      executionpin.Digest(contract.SchemaManifestDigest),
		TransactionManifestDigest: executionpin.Digest(contract.TransactionManifestDigest),
		ParticipantAuthorityRoot:  executionpin.Digest(contract.LineageForwardingDigest),
		ParticipantCount:          contract.ParticipantCount,
		ExecutionContractDigest:   executionpin.Digest(contract.ProtocolProgramDigest),
		LedgerHomeGroup:           executionpin.ID(build.Home.borrowedRoute().Group.GroupID),
	}
	digest, err := executionpin.BindingDigest(binding)
	if err != nil || replication.Digest(digest) != contract.PinDigest {
		t.Fatalf("pin digest=%x want=%x err=%v", contract.PinDigest, digest, err)
	}
}

func TestBuildDurableRequestLogicalProgramIsOrderIndependentAndOwnsMutationBytes(t *testing.T) {
	build := durableRequestProgramBuildFixture(t)
	first, err := BuildDurableRequestLogicalProgram(build)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(build.Participants)
	second, err := BuildDurableRequestLogicalProgram(build)
	if err != nil {
		t.Fatal(err)
	}
	if first.Contract != second.Contract || first.Identity != second.Identity ||
		len(first.Participants) != len(second.Participants) {
		t.Fatal("caller participant order changed the sealed aggregate")
	}
	for index := range first.Participants {
		if compareDurableRequestLogicalParticipant(first.Participants[index], second.Participants[index]) != 0 ||
			first.Participants[index].MutationDigest != second.Participants[index].MutationDigest {
			t.Fatalf("participant %d changed under input reorder", index)
		}
	}
	before := first.Participants[0].Batches[0].Mutations[0].Key[0]
	build.Participants[0].Batches[0].Mutations[0].Key[0]++
	if first.Participants[0].Batches[0].Mutations[0].Key[0] != before {
		t.Fatal("sealed recipe borrowed caller mutation storage")
	}
}

func TestBuildDurableRequestLogicalProgramBindsLedgerHomeGroup(t *testing.T) {
	build := durableRequestProgramBuildFixture(t)
	first, err := BuildDurableRequestLogicalProgram(build)
	if err != nil {
		t.Fatal(err)
	}
	build.Home.route.Group.GroupID[0]++
	second, err := BuildDurableRequestLogicalProgram(build)
	if err != nil {
		t.Fatal(err)
	}
	if first.Contract.PinDigest == second.Contract.PinDigest ||
		first.Contract.TransactionManifestDigest != second.Contract.TransactionManifestDigest {
		t.Fatal("ledger-home group was not isolated to aggregate execution-pin authority")
	}
}
