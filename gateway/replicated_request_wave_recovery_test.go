package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
)

// Compose the real dynamic payload store with the existing deterministic
// lifecycle fixture. The ledger keeps every build/chunk/continuation across a
// reconstructed runner; it does not substitute a fake Stage implementation.
type advancedWaveLedger struct {
	*lifecycleRunnerLedger
	payload     dynamicPayloadLedger
	stageWrites int
}

func (l *advancedWaveLedger) ApplyCAS(ctx context.Context, home DurableRequestLedgerHome, key requestledger.RequestKey, cas DurableRequestLifecycleCAS) (DurableRequestLifecycleCASResult, error) {
	switch cas.Operation {
	case requestledger.OperationBeginPayloadBuild, requestledger.OperationStagePayloadChunk, requestledger.OperationSealPayload, requestledger.OperationCleanupPayload:
		if cas.Operation != requestledger.OperationCleanupPayload {
			l.stageWrites++
		}
		l.payload.head = l.head
		result, err := l.payload.ApplyCAS(ctx, home, key, cas)
		l.head, l.build = l.payload.head, l.payload.build
		return result, err
	default:
		return l.lifecycleRunnerLedger.ApplyCAS(ctx, home, key, cas)
	}
}

func (l *advancedWaveLedger) ReadRow(ctx context.Context, home DurableRequestLedgerHome, read DurableRequestLifecycleRead) (DurableRequestLifecycleRow, error) {
	switch read.Kind {
	case replicatedstate.RequestLedgerReadPayloadBuild, replicatedstate.RequestLedgerReadPayloadChunk:
		l.payload.head = l.head
		return l.payload.ReadRow(ctx, home, read)
	default:
		return l.lifecycleRunnerLedger.ReadRow(ctx, home, read)
	}
}

type advancedWaveParticipantStream struct {
	participant DurableRequestLogicalParticipant
	consumed    bool
}

func (s *advancedWaveParticipantStream) Reset() error { s.consumed = false; return nil }
func (s *advancedWaveParticipantStream) Next() bool {
	if s.consumed {
		return false
	}
	s.consumed = true
	return true
}
func (s *advancedWaveParticipantStream) Current() DurableRequestLogicalParticipant {
	return s.participant
}
func (*advancedWaveParticipantStream) Err() error         { return nil }
func (*advancedWaveParticipantStream) BufferedBytes() int { return 0 }
func (s *advancedWaveParticipantStream) Complete() bool   { return s.consumed }

func advancedExecution(wave DurableRequestWave) DurableRequestTypedExecutionContext {
	return DurableRequestTypedExecutionContext{
		Home: wave.Home, Key: DurableRequestLedgerKey{RequestKey: wave.Key},
		Recipe: DurableRequestRecipe{Identity: wave.Identity, Tenant: wave.Tenant, ParticipantCount: 1,
			Contract: DurableRequestExecutionContract{PinID: wave.PinID, PinDigest: replication.Digest(wave.Binding)}},
		Participants:      &advancedWaveParticipantStream{participant: wave.Participant},
		ExecutionPinRoute: wave.ExecutionPinRoute, ExecutionPinLease: wave.ExecutionPinLease,
	}
}

// Observe the outer runner's recovery dispatch, then resume the exact fixture
// wave through the real lifecycle and payload store. Stop after that wave: the
// fixture command is a mutation, not a complete distributed transaction.
type preAdvanceDispatchWaves struct {
	runner                     *DurableRequestLifecycleRunner
	wave                       DurableRequestWave
	advancedCalls, normalCalls int
}

func (w *preAdvanceDispatchWaves) ResumeAdvancedWave(context.Context, DurableRequestTypedExecutionContext) error {
	w.advancedCalls++
	return ErrDurableRequestConflict
}

func (w *preAdvanceDispatchWaves) RunStagedWave(ctx context.Context, _ DurableRequestWave) (DurableRequestWaveResult, error) {
	w.normalCalls++
	result, err := w.runner.RunStagedWave(ctx, w.wave)
	if err != nil {
		return result, err
	}
	return result, errDynamicPayloadFault
}

func TestDurableRequestDistributedRunnerResumesPreAdvanceRoute(t *testing.T) {
	for _, cut := range []requestledger.Operation{requestledger.OperationRecordRoutePinAcquiredPutPending} {
		t.Run(fmt.Sprint(cut), func(t *testing.T) {
			wave, head, route := lifecycleRunnerFixture(t)
			events := new(lifecycleRunnerEvents)
			ledger := &advancedWaveLedger{lifecycleRunnerLedger: &lifecycleRunnerLedger{head: head, events: events, fault: cut},
				payload: dynamicPayloadLedger{chunks: make(map[uint64]requestledger.PayloadChunkRecord)}}
			proposer := &lifecycleRunnerProposer{t: t, events: events, faultKind: -1, attempts: make(map[replication.CommandKind][][]byte)}
			resolver := &lifecycleRunnerResolver{route: route, events: events}
			inner, err := newDurableRequestLifecycleRunner(ledger, resolver, proposer)
			if err != nil {
				t.Fatal(err)
			}
			wave.Step, wave.Build = requestledger.StepRef{}, requestledger.PayloadBuildRecord{}
			if _, err = inner.RunStagedWave(t.Context(), wave); !errors.Is(err, errLifecycleRunnerFault) {
				t.Fatal(err)
			}
			if ledger.route.Phase != requestledger.RoutePinAcquired || ledger.head.OutstandingRoutePinDigest != (requestledger.Digest{}) || ledger.head.CleanupBuildDigest != (requestledger.Digest{}) || ledger.head.NextStepOrdinal != wave.Ordinal {
				t.Fatal("fixture did not retain the exact pre-advance route/head state")
			}
			inner, err = newDurableRequestLifecycleRunner(ledger, resolver, proposer)
			if err != nil {
				t.Fatal(err)
			}
			waves := &preAdvanceDispatchWaves{runner: inner, wave: wave}
			outer, err := newDurableRequestDistributedRunner(ledger, distributedRunnerResolver{base: route}, waves,
				distributedRunnerPayloads{}, &distributedRunnerTerminal{}, distributedRunnerAuthority{})
			if err != nil {
				t.Fatal(err)
			}
			execution := typedExecutionFixture(t)
			execution.Recipe.Contract.CommitFinalWaveCount, execution.Recipe.Contract.AbortFinalWaveCount = 8, 11
			execution, _ = bindTypedExecutionPin(t, execution, route)
			_, err = outer.RunTyped(t.Context(), execution)
			if !errors.Is(err, errDynamicPayloadFault) || waves.advancedCalls != 0 || waves.normalCalls != 1 {
				t.Fatalf("advanced=%d normal=%d err=%v", waves.advancedCalls, waves.normalCalls, err)
			}
			if ledger.head.NextStepOrdinal != wave.Ordinal+1 || ledger.route.Phase != requestledger.RoutePinReleased || ledger.head.OutstandingRoutePinDigest != (requestledger.Digest{}) {
				t.Fatal("ordinary retry did not finish retained wave")
			}
		})
	}
}

func TestDurableRequestStagedWaveRejectsByteBoundsBeforeAdmission(t *testing.T) {
	for _, kind := range []string{"empty-target", "empty-command", "oversized-command"} {
		t.Run(kind, func(t *testing.T) {
			wave, head, route := lifecycleRunnerFixture(t)
			events := new(lifecycleRunnerEvents)
			ledger := &advancedWaveLedger{lifecycleRunnerLedger: &lifecycleRunnerLedger{head: head, events: events}}
			proposer := &lifecycleRunnerProposer{t: t, events: events}
			resolver := &lifecycleRunnerResolver{route: route, events: events}
			runner, err := newDurableRequestLifecycleRunner(ledger, resolver, proposer)
			if err != nil {
				t.Fatal(err)
			}
			wave.Step, wave.Build = requestledger.StepRef{}, requestledger.PayloadBuildRecord{}
			switch kind {
			case "empty-target":
				wave.Target = nil
			case "empty-command":
				wave.Command = nil
			case "oversized-command":
				wave.Command = make([]byte, replication.MaxCommandBytes+1)
			}
			if _, err = runner.RunStagedWave(t.Context(), wave); !errors.Is(err, ErrDurableRequest) {
				t.Fatalf("bound accepted: %v", err)
			}
			if proposer.fenceCalls != 0 || resolver.calls != 0 || ledger.stageWrites != 0 {
				t.Fatal("invalid byte bounds reached admission")
			}
		})
	}
}

func TestDurableRequestStagedWaveResumesAdvancedReleaseWithoutRestaging(t *testing.T) {
	for _, cut := range []struct {
		name      string
		operation requestledger.Operation
		proposal  bool
	}{
		{"advance", requestledger.OperationAdvance, false},
		{"release-intent", requestledger.OperationBeginRoutePinRelease, false},
		{"release-proposal", requestledger.OperationInvalid, true},
		{"release-proof", requestledger.OperationRecordRoutePinReleased, false},
	} {
		for _, typed := range []bool{false, true} {
			t.Run(cut.name+map[bool]string{false: "/same-invocation", true: "/typed-restart"}[typed], func(t *testing.T) {
				wave, head, route := lifecycleRunnerFixture(t)
				events := new(lifecycleRunnerEvents)
				ledger := &advancedWaveLedger{lifecycleRunnerLedger: &lifecycleRunnerLedger{head: head, events: events, fault: cut.operation},
					payload: dynamicPayloadLedger{chunks: make(map[uint64]requestledger.PayloadChunkRecord)}}
				resolver := &lifecycleRunnerResolver{route: route, events: events}
				proposer := &lifecycleRunnerProposer{t: t, events: events, faultKind: -1, attempts: make(map[replication.CommandKind][][]byte)}
				if cut.proposal {
					proposer.faultKind, proposer.faultGate = int(replication.CommandRouteGate), routegate.OperationReleaseShared
				}
				runner, err := newDurableRequestLifecycleRunner(ledger, resolver, proposer)
				if err != nil {
					t.Fatal(err)
				}
				settles := 0
				transition, cursor := wave.Transition, wave.Cursor
				wave.Step, wave.Build = requestledger.StepRef{}, requestledger.PayloadBuildRecord{}
				wave.Transition, wave.Cursor = 0, nil
				wave.Settle = func([]byte) (uint32, []byte, error) { settles++; return transition, cursor, nil }
				if _, err = runner.RunStagedWave(t.Context(), wave); !errors.Is(err, errLifecycleRunnerFault) {
					t.Fatalf("first: %v", err)
				}
				if ledger.head.NextStepOrdinal != wave.Ordinal+1 || ledger.head.CleanupBuildDigest == (requestledger.Digest{}) || settles != 1 {
					t.Fatal("did not reach retained advanced state")
				}
				writes, work := ledger.stageWrites, len(proposer.attempts[replication.CommandMutationBatch])
				observation := bytes.Clone(ledger.continuation.Observation)
				// New runner has no process-local admission or staging state.
				runner, err = newDurableRequestLifecycleRunner(ledger, resolver, proposer)
				if err != nil {
					t.Fatal(err)
				}
				// A restarted call never inherits the old invocation's admission.
				proposer.fenceFaultAt = proposer.fenceCalls + 1
				if err = runner.ResumeAdvancedWave(t.Context(), advancedExecution(wave)); !errors.Is(err, ErrDurableRequestConflict) {
					t.Fatalf("expired recovery admission: %v", err)
				}
				if ledger.stageWrites != writes || len(proposer.attempts[replication.CommandMutationBatch]) != work {
					t.Fatal("expired recovery staged or submitted work")
				}
				proposer.fenceFaultAt = 0
				if typed {
					err = runner.ResumeAdvancedWave(t.Context(), advancedExecution(wave))
				} else {
					_, err = runner.RunStagedWave(t.Context(), wave)
				}
				if err != nil {
					t.Fatalf("resume: %v", err)
				}
				if ledger.stageWrites != writes || len(proposer.attempts[replication.CommandMutationBatch]) != work || settles != 1 || !bytes.Equal(observation, ledger.continuation.Observation) {
					t.Fatal("resume restaged, reproposed, or resettled work")
				}
				if ledger.head.OutstandingRoutePinDigest != (requestledger.Digest{}) || ledger.route.Phase != requestledger.RoutePinReleased {
					t.Fatal("route not released")
				}
				store, _ := NewDurableRequestDynamicPayloadStore(ledger)
				if _, err = store.Cleanup(t.Context(), wave.Home, wave.Key); err != nil {
					t.Fatal(err)
				}
				if ledger.head.CleanupBuildDigest != (requestledger.Digest{}) || len(ledger.payload.chunks) != 0 || ledger.payload.build != (requestledger.PayloadBuildRecord{}) {
					t.Fatal("payload not reclaimed")
				}
			})
		}
	}
}

func TestDurableRequestAdvancedPayloadRejectsSubstitutedWitnesses(t *testing.T) {
	wave, head, route := lifecycleRunnerFixture(t)
	events := new(lifecycleRunnerEvents)
	ledger := &advancedWaveLedger{lifecycleRunnerLedger: &lifecycleRunnerLedger{head: head, events: events, fault: requestledger.OperationAdvance},
		payload: dynamicPayloadLedger{chunks: make(map[uint64]requestledger.PayloadChunkRecord)}}
	proposer := &lifecycleRunnerProposer{t: t, events: events, faultKind: -1, attempts: make(map[replication.CommandKind][][]byte)}
	runner, err := newDurableRequestLifecycleRunner(ledger, &lifecycleRunnerResolver{route: route, events: events}, proposer)
	if err != nil {
		t.Fatal(err)
	}
	wave.Step, wave.Build = requestledger.StepRef{}, requestledger.PayloadBuildRecord{}
	if _, err = runner.RunStagedWave(t.Context(), wave); !errors.Is(err, errLifecycleRunnerFault) {
		t.Fatal(err)
	}
	cut, err := runner.advancedWaveCut(t.Context(), wave)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runner.openAdvancedWave(t.Context(), wave, cut, uint64(len(wave.Target))); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*durableAdvancedWaveCut){
		func(c *durableAdvancedWaveCut) { c.head.CleanupBuildDigest[0] ^= 1 },
		func(c *durableAdvancedWaveCut) { c.head.NextStepOrdinal++ },
		func(c *durableAdvancedWaveCut) { c.head.CleanupNextChunk++ },
		func(c *durableAdvancedWaveCut) { c.route.PriorContinuationDigest[0] ^= 1 },
		func(c *durableAdvancedWaveCut) { c.continuation.RoutePinDigest[0] ^= 1 },
		func(c *durableAdvancedWaveCut) { c.continuation.WaveRevision++ },
		func(c *durableAdvancedWaveCut) {
			c.continuation.Observation = bytes.Clone(c.continuation.Observation)
			c.continuation.Observation[0] ^= 1
		},
	} {
		bad := cut
		mutate(&bad)
		if _, err = runner.openAdvancedWave(t.Context(), wave, bad, uint64(len(wave.Target))); err == nil {
			t.Fatal("substituted recovery witness accepted")
		}
	}
}
