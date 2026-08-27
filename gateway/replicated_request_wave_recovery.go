package gateway

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

type durableRequestAdvancedPayloadReader interface {
	OpenAdvanced(context.Context, DurableRequestLedgerHome, requestledger.RequestKey,
		requestledger.HeadRecord, requestledger.RoutePinRecord, requestledger.ContinuationRecord,
		uint64, uint64) (DurableRequestDynamicPayload, error)
}

type durableAdvancedWaveCut struct {
	head         requestledger.HeadRecord
	route        requestledger.RoutePinRecord
	continuation requestledger.ContinuationRecord
	applied      uint64
}

func (runner *DurableRequestLifecycleRunner) advancedWaveCut(
	ctx context.Context, wave DurableRequestWave,
) (durableAdvancedWaveCut, error) {
	keyDigest, err := requestledger.KeyDigest(wave.Key)
	if err != nil {
		return durableAdvancedWaveCut{}, err
	}
	head, route, pending, applied, err := runner.openWaveRows(ctx, wave, keyDigest)
	if err != nil {
		return durableAdvancedWaveCut{}, err
	}
	if pending.Revision != 0 || route.Revision == 0 || route.WaveOrdinal == math.MaxUint64 ||
		route.WaveOrdinal+1 != head.NextStepOrdinal || route.PinID != wave.PinID ||
		route.BindingDigest != wave.Binding || head.PinID != wave.PinID ||
		route.KeyDigest != head.KeyDigest || route.RequestDigest != head.RequestDigest || route.PlanRoot != head.PlanRoot ||
		(route.Phase != requestledger.RoutePinAcquired && route.Phase != requestledger.RoutePinReleasing && route.Phase != requestledger.RoutePinReleased) ||
		(route.Phase != requestledger.RoutePinReleased && head.OutstandingRoutePinDigest != route.AcquiredEvidenceDigest) ||
		(route.Phase == requestledger.RoutePinReleased && head.OutstandingRoutePinDigest != (requestledger.Digest{})) {
		return durableAdvancedWaveCut{}, ErrDurableRequestConflict
	}
	row, err := runner.ledger.ReadRow(ctx, wave.Home, DurableRequestLifecycleRead{
		Key: wave.Key, Kind: replicatedstate.RequestLedgerReadContinuation, MinimumApplied: applied,
	})
	if err != nil || !row.Found || row.Kind != replicatedstate.RequestLedgerReadContinuation {
		return durableAdvancedWaveCut{}, errors.Join(err, ErrDurableRequestConflict)
	}
	continuation := row.Continuation
	if continuation.KeyDigest != head.KeyDigest || continuation.RequestDigest != head.RequestDigest ||
		continuation.PlanRoot != head.PlanRoot || continuation.ContinuationDigest != head.ContinuationDigest ||
		continuation.WaveRevision != head.ContinuationRevision ||
		continuation.SettledOrdinal != route.WaveOrdinal || continuation.RoutePinDigest != route.AcquiredEvidenceDigest ||
		continuation.PriorContinuationDigest != route.PriorContinuationDigest {
		return durableAdvancedWaveCut{}, ErrDurableRequestConflict
	}
	return durableAdvancedWaveCut{head: head, route: route, continuation: continuation, applied: applied}, nil
}

func (runner *DurableRequestLifecycleRunner) openAdvancedWave(
	ctx context.Context, wave DurableRequestWave, cut durableAdvancedWaveCut, targetLength uint64,
) (DurableRequestWave, error) {
	reader, ok := runner.payloads.(durableRequestAdvancedPayloadReader)
	if !ok {
		return DurableRequestWave{}, ErrDurableRequestUnresolved
	}
	payload, err := reader.OpenAdvanced(ctx, wave.Home, wave.Key, cut.head, cut.route, cut.continuation, targetLength, cut.applied)
	if err != nil {
		return DurableRequestWave{}, err
	}
	command, err := replication.OpenCommand(payload.Command)
	gate, gateErr := replication.OpenCommand(cut.route.Command)
	completion, completionErr := replication.OpenCompletion(cut.continuation.Observation)
	if err != nil || gateErr != nil || command.GroupID != gate.GroupID ||
		!bytes.Equal(command.Tenant, wave.Tenant) || command.ClientID != replication.ID128(wave.Identity.ID) ||
		command.RetryHome != wave.Identity.RetryHome ||
		completionErr != nil || !nativeCompletionMatches(command, completion) {
		return DurableRequestWave{}, errors.Join(err, gateErr, completionErr, ErrDurableRequestConflict)
	}
	wave.Ordinal, wave.Build, wave.Step = cut.route.WaveOrdinal, payload.Build, payload.Step
	wave.Target, wave.Command = payload.Target, payload.Command
	return wave, nil
}

// ResumeAdvancedWave completes only the retained route release. Participant
// work and its settlement callback have already completed at OperationAdvance.
// The payload stays immutable and pinned until the release proof is durable.
func (runner *DurableRequestLifecycleRunner) ResumeAdvancedWave(
	ctx context.Context, execution DurableRequestTypedExecutionContext,
) error {
	if runner == nil || runner.ledger == nil || runner.resolver == nil || runner.proposer == nil ||
		runner.pinFencer == nil || runner.gateSessions == nil || ctx == nil || execution.Participants == nil {
		return ErrDurableRequest
	}
	wave := DurableRequestWave{
		Home: execution.Home, Key: execution.Key.RequestKey, Identity: execution.Recipe.Identity,
		Tenant: execution.Recipe.Tenant, PinID: execution.Recipe.Contract.PinID,
		Binding:           requestledger.Digest(execution.Recipe.Contract.PinDigest),
		ExecutionPinRoute: execution.ExecutionPinRoute, ExecutionPinLease: execution.ExecutionPinLease,
		Settle: func([]byte) (uint32, []byte, error) { return 0, nil, ErrDurableRequestConflict },
	}
	cut, err := runner.advancedWaveCut(ctx, wave)
	if err != nil {
		return err
	}
	outer, err := replication.OpenCommand(cut.route.Command)
	if err != nil {
		return err
	}
	gate, err := outer.OpenRouteGate()
	if err != nil {
		return err
	}
	wave.GateEpoch = gate.Epoch
	if err := execution.Participants.Reset(); err != nil {
		return err
	}
	found := false
	for visited := uint64(0); visited < execution.Recipe.ParticipantCount && execution.Participants.Next(); visited++ {
		participant := execution.Participants.Current()
		if participant.Group.GroupID == outer.GroupID && bytes.Equal([]byte(participant.Distribution), outer.Distribution) &&
			bytes.Equal([]byte(participant.Shard), outer.Shard) {
			wave.Participant, found = participant, true
			break
		}
	}
	if !found {
		return errors.Join(execution.Participants.Err(), ErrDurableRequestConflict)
	}
	wave, err = runner.openAdvancedWave(ctx, wave, cut, uint64(len(outer.GroupID)))
	if err != nil || !bytes.Equal(wave.Target, outer.GroupID[:]) {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	keyDigest, err := validateDurableRequestWave(wave)
	if err != nil {
		return err
	}
	if err = runner.fenceWaveSideEffect(ctx, wave); err != nil {
		return err
	}
	_, err = runner.runAdmittedWave(ctx, wave, keyDigest)
	return err
}
