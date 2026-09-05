package gateway

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

// Compose the shipped builder, admission adapter, runner terminal call and
// actual ledger/pin kernels. The in-memory ports apply their real transitions;
// they do not fabricate successful terminal results or claim quorum coverage.
func TestDurableRequestBuiltProgramCompletesExactTerminalContract(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "abort"
		if committed {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			build := durableRequestProgramBuildFixture(t)
			build.Targets = durableFaultTargetsN(t, 1)
			program, err := BuildDurableRequestLogicalProgram(build)
			if err != nil {
				t.Fatal(err)
			}
			if program.Contract.RetirementWitnessDigest == program.Contract.TerminalSummaryDigest {
				t.Fatal("fixture aliases distinct authenticated digest domains")
			}
			measurement, err := measureDurableRequestPlan(build.Key, program)
			if err != nil {
				t.Fatal(err)
			}
			head, err := durableRequestHeadForMeasurement(build.Key, measurement)
			if err != nil {
				t.Fatal(err)
			}
			state := durableDistributedState{branch: durableDistributedAborted}
			waves, tag := head.AbortFinalWaveCount, head.AbortTerminalTransitionTag
			if committed {
				state.branch, state.affected = durableDistributedCommitted, 12
				waves, tag = head.FinalWaveCount, head.TerminalTransitionTag
			}
			cursor := appendDurableDistributedState(nil, durableDistributedState{branch: state.branch})
			var continuation requestledger.ContinuationRecord
			for wave := uint64(0); wave < waves; wave++ {
				head, continuation = settleProgramBoundWave(t, head, tag, cursor, maximumProgramTransactionCompletion(t, committed))
			}
			execution := DurableRequestTypedExecutionContext{Home: build.Home, Key: build.Key,
				Recipe: DurableRequestRecipe{Identity: program.Identity, CatalogGeneration: build.CatalogGeneration,
					Contract: program.Contract, TargetCount: program.Contract.TargetCount,
					KeyDigest: program.KeyDigest, RequestDigest: program.RequestDigest, Tenant: program.Tenant}}
			binding, err := BuildDurableRequestExecutionPinBinding(execution)
			if err != nil {
				t.Fatal(err)
			}
			pinID, err := executionpin.DerivePinID(binding)
			if err != nil {
				t.Fatal(err)
			}
			acquire := executionpin.Command{Operation: executionpin.OperationAcquire, Binding: binding, PinID: pinID,
				AuthorityNode: executionpin.ID{4}, AuthorityGeneration: 5,
				NextController: executionpin.ID{6}, NextControllerEpoch: 7, NextLeaseSpan: 1000}
			acquired := executionpin.Apply(executionpin.Record{}, false, acquire, 10, executionpin.Digest{11}, executionpin.Digest{12})
			certificate, ok := acquired.Record.AcquireCertificate()
			if acquired.Reason != executionpin.ReasonApplied || !ok {
				t.Fatal("actual pin acquire failed")
			}
			lease, ok := acquired.Record.LeaseCertificate()
			if !ok {
				t.Fatal("missing acquired lease")
			}
			execution, err = BindDurableRequestExecutionPin(execution, build.Home.borrowedRoute(), certificate, lease)
			if err != nil {
				t.Fatal(err)
			}
			release := acquire
			release.Operation = executionpin.OperationRelease
			release.NextController, release.NextControllerEpoch, release.NextLeaseSpan = executionpin.ID{}, 0, 0
			release.ExpectedController, release.ExpectedControllerEpoch = lease.Controller, lease.ControllerEpoch
			release.ExpectedLeaseAppliedThrough, release.ExpectedLeaseRevision = lease.LeaseAppliedThrough, lease.Revision
			release.AcquireCertificateDigest = lease.AcquireCertificateDigest
			authority, err := NewDurableRequestTerminalAuthority(execution, DurableRequestAckDerivationKey{1},
				appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedCommitted}),
				appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedAborted}), release)
			if err != nil {
				t.Fatal(err)
			}
			ledger := &terminalCoordinatorLedger{head: head, continuation: continuation}
			pin := &terminalCoordinatorPin{t: t, route: build.Home.borrowedRoute(), tenant: program.Tenant,
				retryHome: program.Identity.RetryHome, clientID: replication.ID128{3}, epoch: 2, sequence: 2, record: acquired.Record}
			coordinator, err := newDurableRequestTerminalCoordinator(ledger, pin)
			if err != nil {
				t.Fatal(err)
			}
			runner := &DurableRequestDistributedRunner{terminal: coordinator}
			for _, foreign := range []replication.Digest{program.Contract.TerminalSummaryDigest, {0x99}} {
				wrong := execution
				wrong.Recipe.Contract.RetirementWitnessDigest = foreign
				if _, err := runner.completeTerminal(t.Context(), wrong, authority, state); !errors.Is(err, ErrDurableRequestConflict) ||
					len(ledger.operations) != 0 || len(pin.attempts) != 0 {
					t.Fatalf("foreign witness reached durable mutation: %v", err)
				}
			}
			result, err := runner.completeTerminal(t.Context(), execution, authority, state)
			if err != nil {
				t.Fatalf("shipped terminal composition: %v", err)
			}
			if ledger.head.Phase != requestledger.PhaseTerminal || result.Terminal.Revision == 0 ||
				result.Terminal.RetirementWitnessDigest != requestledger.Digest(program.Contract.RetirementWitnessDigest) ||
				result.Terminal.AckToken != authority.AckToken || result.Terminal.AffectedRows != state.affected {
				t.Fatal("terminal lost exact result/retirement/ACK authority")
			}
			encoded, err := requestledger.AppendTerminal(nil, result.Terminal)
			if err != nil {
				t.Fatal(err)
			}
			retry, err := runner.completeTerminal(t.Context(), execution, authority, state)
			if err != nil {
				t.Fatal(err)
			}
			again, err := requestledger.AppendTerminal(nil, retry.Terminal)
			if err != nil || !bytes.Equal(encoded, again) || len(pin.attempts) != 1 || len(ledger.operations) != 4 {
				t.Fatalf("exact terminal retry changed proof or repeated side effects: %v", err)
			}
			runBuiltTerminalRecoveryCases(t, execution, authority, state, head, continuation, acquired.Record)
		})
	}
}
