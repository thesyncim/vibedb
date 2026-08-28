package gateway

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestDurableRequestProgramExactContinuationAndTerminalBounds(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "abort"
		if committed {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			build := durableRequestProgramBuildFixture(t)
			build.Participants = durableFaultParticipantsN(t, 1)
			program, err := BuildDurableRequestLogicalProgram(build)
			if err != nil {
				t.Fatal(err)
			}
			measurement, err := measureDurableRequestPlan(build.Key, program)
			if err != nil {
				t.Fatal(err)
			}
			head, err := durableRequestHeadForMeasurement(build.Key, measurement)
			if err != nil || head.Phase != requestledger.PhaseSealed {
				t.Fatalf("builder head: %+v %v", head, err)
			}
			branch, outcome := durableDistributedAborted, requestledger.OutcomeAborted
			waves, transition := head.AbortFinalWaveCount, head.AbortTerminalTransitionTag
			if committed {
				branch, outcome = durableDistributedCommitted, requestledger.OutcomeCommitted
				waves, transition = head.FinalWaveCount, head.TerminalTransitionTag
			}
			cursor := appendDurableDistributedState(nil, durableDistributedState{branch: branch})
			observation := maximumProgramTransactionCompletion(t, committed)
			var continuation requestledger.ContinuationRecord
			for wave := uint64(0); wave < waves; wave++ {
				head, continuation = settleProgramBoundWave(t, head, transition, cursor, observation)
			}
			progress := durableDistributedProgress{execution: DurableRequestTypedExecutionContext{
				Recipe: DurableRequestRecipe{Identity: program.Identity, CatalogGeneration: build.CatalogGeneration,
					ParticipantCount: program.Contract.ParticipantCount, Contract: program.Contract},
			}, state: durableDistributedState{branch: branch}}
			result, err := progress.result()
			if err != nil || len(result) != durableRequestResultHeaderBytes {
				t.Fatalf("fixed terminal result: bytes=%d err=%v", len(result), err)
			}
			prepared, err := requestledger.NewPreparedTerminal(head, continuation, head.Revision+1,
				outcome, 0, committed, result, requestledger.Digest(program.Contract.RetirementWitnessDigest), requestledger.AckToken{1})
			if err != nil {
				t.Fatal(err)
			}
			preparedBytes, err := requestledger.AppendPreparedTerminal(nil, prepared)
			if err != nil || uint64(len(preparedBytes)) > head.MaxTerminalBytes {
				t.Fatalf("prepared wrapper exceeds reservation: %d/%d %v", len(preparedBytes), head.MaxTerminalBytes, err)
			}
			openedPrepared, err := requestledger.OpenPreparedTerminal(preparedBytes)
			if err != nil {
				t.Fatal(err)
			}
			again, err := requestledger.AppendPreparedTerminal(nil, openedPrepared)
			if err != nil || !bytes.Equal(again, preparedBytes) {
				t.Fatal("prepared terminal encoding is not canonical", err)
			}
			short := head
			short.MaxTerminalBytes = uint64(len(preparedBytes) - 1)
			if _, err := requestledger.NewPreparedTerminal(short, continuation, short.Revision+1,
				outcome, 0, committed, result, short.TerminalSummaryDigest, requestledger.AckToken{1}); !errors.Is(err, requestledger.ErrTooLarge) {
				t.Fatalf("one-byte-short prepared bound: %v", err)
			}
			head, err = requestledger.MarkTerminalPrepared(head, continuation, prepared)
			if err != nil {
				t.Fatal(err)
			}
			// These opaque bytes stand for independently verified pin evidence;
			// this test exercises ledger encoding/reservation, not authentication.
			release, err := requestledger.NewSchemaPinRelease(head, prepared, head.Revision+1, []byte{1})
			if err != nil {
				t.Fatal(err)
			}
			head, err = requestledger.InstallSchemaPinRelease(head, prepared, release)
			if err != nil {
				t.Fatal(err)
			}
			released, err := requestledger.RecordVerifiedSchemaPinReleased(release, release.Revision+1, []byte{2})
			if err != nil {
				t.Fatal(err)
			}
			head, err = requestledger.MarkSchemaPinReleased(head, prepared, release, released)
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := requestledger.NewTerminal(head, prepared, released, head.Revision+1)
			if err != nil {
				t.Fatal(err)
			}
			terminalBytes, err := requestledger.AppendTerminal(nil, terminal)
			if err != nil || uint64(len(terminalBytes)) != head.MaxTerminalBytes {
				t.Fatalf("terminal wrapper must exactly fill reservation: %d/%d %v", len(terminalBytes), head.MaxTerminalBytes, err)
			}
			openedTerminal, err := requestledger.OpenTerminal(terminalBytes)
			if err != nil {
				t.Fatal(err)
			}
			again, err = requestledger.AppendTerminal(nil, openedTerminal)
			if err != nil || !bytes.Equal(again, terminalBytes) {
				t.Fatal("terminal encoding is not canonical", err)
			}
			short = head
			short.MaxTerminalBytes--
			if _, err := requestledger.NewTerminal(short, prepared, released, short.Revision+1); !errors.Is(err, requestledger.ErrTooLarge) {
				t.Fatalf("one-byte-short terminal bound: %v", err)
			}
		})
	}
}

func maximumProgramTransactionCompletion(t *testing.T, committed bool) []byte {
	t.Helper()
	_, encoded, _ := testReplicatedRouteCommand(t)
	command, err := replication.OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	command.Distribution = bytes.Repeat([]byte{'d'}, replication.MaxIdentityBytes)
	command.Shard = bytes.Repeat([]byte{'s'}, replication.MaxIdentityBytes)
	command.Tenant = bytes.Repeat([]byte{'t'}, replication.MaxIdentityBytes)
	affected := int64(-1)
	if committed {
		affected = 0
	}
	result := transactionOrchestratorResult(distributedtxn.ReplicatedRoleCoordinator,
		distributedtxn.ReplicatedRetireCoordinator, 2, affected)
	raw := appendTransactionOrchestratorCompletion(command, replicatedstate.ResultApplied, result[:], 3)
	opened, err := replication.OpenCompletion(raw)
	if err != nil || len(raw) != replicatedstate.MaxTransactionCompletionEnvelopeBytes {
		t.Fatalf("maximum canonical transaction completion: %d/%d %v", len(raw), replicatedstate.MaxTransactionCompletionEnvelopeBytes, err)
	}
	if _, err := replicatedstate.OpenTransactionCompletionResult(opened.ResultCode, opened.InlineResult); err != nil {
		t.Fatal(err)
	}
	return raw
}

func settleProgramBoundWave(t *testing.T, head requestledger.HeadRecord, transition uint32,
	cursor, observation []byte,
) (requestledger.HeadRecord, requestledger.ContinuationRecord) {
	t.Helper()
	route, err := requestledger.NewRoutePinAcquiring(head, head.PinID, head.PinDigest, requestledger.Digest{1}, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, requestledger.RoutePinRecord{}, route, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := requestledger.RecordVerifiedRoutePinAcquired(route, route.Revision+1, []byte{2})
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, route, acquired, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	step := requestledger.StepRef{TargetSource: requestledger.PayloadSourcePlan, CommandSource: requestledger.PayloadSourcePlan,
		TargetLength: 16, CommandOffset: 16, CommandLength: 1,
		TargetDigest: requestledger.Digest(sha256.Sum256(head.InlinePlan[:16])), CommandDigest: requestledger.Digest(sha256.Sum256(head.InlinePlan[16:17]))}
	pending, err := requestledger.NewPendingWaveWithRoutePin(head, requestledger.PayloadBuildRecord{}, head.Revision+1, acquired, []requestledger.StepRef{step})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := requestledger.AppendPendingWave(nil, pending)
	if err != nil || uint64(len(encoded)) != head.MaxPendingWaveBytes {
		t.Fatalf("single step must exactly fill pending bound: %d/%d %v", len(encoded), head.MaxPendingWaveBytes, err)
	}
	if _, err := requestledger.NewPendingWaveWithRoutePin(head, requestledger.PayloadBuildRecord{}, head.Revision+1, acquired,
		[]requestledger.StepRef{step, step}); !errors.Is(err, requestledger.ErrTooLarge) {
		t.Fatalf("second live step was admitted: %v", err)
	}
	head, err = requestledger.InstallPendingWave(head, pending, requestledger.PayloadBuildRecord{}, acquired)
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := requestledger.NewContinuation(head, pending, acquired, head.Revision+1, transition, cursor, observation)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = requestledger.AppendContinuation(nil, continuation)
	if err != nil || uint64(len(encoded)) != head.MaxContinuationBytes {
		t.Fatalf("maximum continuation must exactly fill bound: %d/%d %v", len(encoded), head.MaxContinuationBytes, err)
	}
	opened, err := requestledger.OpenContinuation(encoded)
	if err != nil {
		t.Fatal(err)
	}
	again, err := requestledger.AppendContinuation(nil, opened)
	if err != nil || !bytes.Equal(encoded, again) {
		t.Fatal("continuation encoding is not canonical", err)
	}
	short := head
	short.MaxContinuationBytes--
	if _, err := requestledger.NewContinuation(short, pending, acquired, short.Revision+1, transition, cursor, observation); !errors.Is(err, requestledger.ErrTooLarge) {
		t.Fatalf("one-byte-short continuation bound: %v", err)
	}
	head, err = requestledger.AdvancePending(head, pending, continuation)
	if err != nil {
		t.Fatal(err)
	}
	release, err := requestledger.BeginRoutePinRelease(acquired, acquired.Revision+1, []byte{3})
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, acquired, release, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	released, err := requestledger.RecordVerifiedRoutePinReleased(release, release.Revision+1, []byte{4})
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkRoutePinReleased(head, released, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	return head, continuation
}
