package replicatedstate

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/store/durable"
)

func transactionRetirementPayload(
	t testing.TB,
	summary distributedtxn.ReplicatedRetirementSummary,
) []byte {
	t.Helper()
	payload, err := distributedtxn.AppendReplicatedRetirementSummary(nil, summary)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestTransactionRetirementAffectedRowsReopenRecoveryAndExactRetry(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "aborted"
		if committed {
			name = "committed"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			id, coordinatorPayload := transactionCodecCoordinatorPayload(t)
			stage := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
				Role:      distributedtxn.ReplicatedRoleCoordinator,
				Operation: distributedtxn.ReplicatedStageCoordinator, ID: id,
				PayloadKind: distributedtxn.ReplicatedPayloadCoordinator,
				Payload:     coordinatorPayload,
			}, nil)
			applyTransactionCommand(t, fixture.machine, 2, stage)
			decision := distributedtxn.ReplicatedAbortCoordinator
			if committed {
				decision = distributedtxn.ReplicatedCommitCoordinator
			}
			decide := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
				Role: distributedtxn.ReplicatedRoleCoordinator, Operation: decision, ID: id,
				ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
			}, nil)
			applyTransactionCommand(t, fixture.machine, 3, decide)

			summary := distributedtxn.ReplicatedRetirementSummary{}
			if committed {
				summary.AffectedRows, summary.AffectedRowsValid = 4096, true
			}
			retireControl := distributedtxn.ReplicatedCommand{
				Role:      distributedtxn.ReplicatedRoleCoordinator,
				Operation: distributedtxn.ReplicatedRetireCoordinator, ID: id,
				ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadRetirement,
				Payload: transactionRetirementPayload(t, summary),
			}
			retire := transactionCompletionCommand(t, fixture.binding, retireControl, nil)
			result := applyTransactionCommand(t, fixture.machine, 4, retire)
			if result.AffectedRowsValid != summary.AffectedRowsValid ||
				result.AffectedRows != summary.AffectedRows || result.Revision != 3 {
				t.Fatalf("retirement completion=%+v want=%+v", result, summary)
			}

			assertRecovery := func(machine *Machine) {
				t.Helper()
				var records [1]TransactionRecoveryRecord
				recovery, err := machine.TransactionRecoveryReadInto(TransactionRecoveryReadRequest{
					Kind: TransactionRecoveryLookupCoordinator, ID: id, MinimumApplied: 4,
					MaxRows: 1, MaxBytes: TransactionRecoverySummaryBytes,
				}, records[:0], make([]byte, 0, MaxTransactionRecoveryPayloadArenaBytes))
				if err != nil || len(recovery.Records) != 1 ||
					recovery.Records[0].AffectedRowsValid != summary.AffectedRowsValid ||
					recovery.Records[0].AffectedRows != summary.AffectedRows ||
					len(recovery.Records[0].Payload) != 0 {
					t.Fatalf("retirement recovery=%+v err=%v", recovery, err)
				}
			}
			assertRecovery(fixture.machine)

			reopened, err := Open(
				fixture.binding, fixture.bootstrap, fixture.system,
				UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
				Options{TxnLimits: durable.TxnLimits{
					MaxCollections: 2,
					MaxDocuments:   fixture.user.Limits.MaxDistinctMutations + 4,
					MaxBytes:       64 << 20,
				}, MaxSessions: 128, RetryWindow: 8},
			)
			if err != nil {
				t.Fatalf("reopen retired coordinator: %v", err)
			}
			assertRecovery(reopened)
			_, retry := openTransactionCompletion(t, reopened, retire)
			if retry.AffectedRowsValid != summary.AffectedRowsValid ||
				retry.AffectedRows != summary.AffectedRows || retry.Revision != 3 {
				t.Fatalf("retirement retry=%+v want=%+v", retry, summary)
			}

			other := distributedtxn.ReplicatedRetirementSummary{
				AffectedRows: 1, AffectedRowsValid: true,
			}
			if committed {
				other = distributedtxn.ReplicatedRetirementSummary{}
			}
			retireControl.Payload = transactionRetirementPayload(t, other)
			competing := transactionCompletionCommand(t, fixture.binding, retireControl, nil)
			completion, competingResult := openTransactionCompletion(t, reopened, competing)
			if completion.ResultCode != ResultTransactionConflict ||
				competingResult.AffectedRowsValid || competingResult.AffectedRows != 0 {
				t.Fatalf("altered retirement completion=%+v result=%+v",
					completion, competingResult)
			}
		})
	}
}
