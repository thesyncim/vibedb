package gateway

import (
	"fmt"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestBuildDurableRequestLogicalProgramReservesOneLiveWave(t *testing.T) {
	var liveBytes, liveChunks uint64
	for _, count := range []int{1, 2, 65, 512} {
		t.Run(fmt.Sprintf("participants_%d", count), func(t *testing.T) {
			build := durableRequestProgramBuildFixture(t)
			build.Participants = durableFaultParticipantsN(t, count)
			program, err := BuildDurableRequestLogicalProgram(build)
			if err != nil {
				t.Fatal(err)
			}
			measurement, err := measureDurableRequestPlan(build.Key, program)
			if err != nil {
				t.Fatal(err)
			}
			head, err := durableRequestHeadForMeasurement(build.Key, measurement)
			if err != nil {
				t.Fatal(err)
			}
			resident, future, err := requestledger.Reservation(head)
			// Keep the ordinary transaction within the real RF3 fixture's 64 MiB
			// capacity, including its separately protected 8 MiB cleanup reserve.
			if err != nil || resident+future > 56<<20 {
				t.Fatalf("admission reservation resident=%d future=%d: %v", resident, future, err)
			}
			if count == 1 {
				liveBytes, liveChunks = head.MaxActivePayloadBytes, head.MaxActivePayloadChunks
			}
			if head.MaxActivePayloadBytes != liveBytes || head.MaxActivePayloadChunks != liveChunks ||
				head.MaxPendingWaveBytes != requestledger.SingleStepPendingWaveRecordBytes {
				t.Fatal("sequential participant count inflated the live wave reservation")
			}
			t.Logf("participants=%d plan=%d reserved=%d live_payload=%d chunks=%d",
				count, head.TotalPlanBytes, resident+future, liveBytes, liveChunks)
			if count != 1 {
				return
			}
			if head.Phase != requestledger.PhaseSealed {
				t.Fatal("single-participant fixture must use the inline sealed plan")
			}
			// The tighter contract still admits the largest encoded command plus
			// its exact group-ID target, including the final partial payload page.
			payload, err := requestledger.NewPayloadBuild(head, requestledger.Digest{1}, liveBytes, liveChunks)
			if err != nil || payload.TotalBytes != uint64(len(replication.ID128{}))+replication.MaxCommandBytes {
				t.Fatalf("maximum one-command wave was under-reserved: %+v %v", payload, err)
			}
			if _, err = requestledger.NewPayloadBuild(head, requestledger.Digest{1}, liveBytes+1, liveChunks); err == nil {
				t.Fatal("accepted payload above the authenticated runner bound")
			}
		})
	}
}

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
		RecoveryDeadline:  3, PlanningLeaseSpan: 100,
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
