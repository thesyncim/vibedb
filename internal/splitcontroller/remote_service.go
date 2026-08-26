package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/shardcontrol"
	vibejson "github.com/thesyncim/vibejson"
)

// ShardActionRuntime reconstructs plan and observation from local durable
// authorities, then executes one idempotent existing split action.
type ShardActionRuntime interface {
	ObserveSplit(context.Context, OperationID, [32]byte, ShardActionTarget) (*Plan, Observation, error)
	ExecuteSplitAction(context.Context, ShardActionTarget, *Plan, Observation, Action) error
}

// WitnessedShardActionRuntime is the shipped authority boundary. The gateway
// has already reconciled the global cut; the shard verifies and durably orders
// the witness, then executes only local proof-checking primitives.
type WitnessedShardActionRuntime interface {
	ExecuteWitnessedAction(context.Context, shardcontrol.Request, remoteStepPayload, Observation) error
}

// RemoteActionService validates the independently reconstructed action before
// dispatch. JournalExecutor wraps this service to persist intent/result.
type RemoteActionService struct{ runtime ShardActionRuntime }

func NewRemoteActionService(runtime ShardActionRuntime) (*RemoteActionService, error) {
	if runtime == nil {
		return nil, ErrRemoteExecution
	}
	return &RemoteActionService{runtime: runtime}, nil
}

func (service *RemoteActionService) ExecuteAction(
	ctx context.Context,
	_ rafttransport.PeerIdentity,
	request shardcontrol.Request,
) (shardcontrol.Response, error) {
	if service == nil || ctx == nil {
		return shardcontrol.Response{}, ErrRemoteExecution
	}
	payload, err := openRemoteStepPayload(request)
	if err != nil {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	operation := OperationID(request.Operation)
	if witnessed, ok := service.runtime.(WitnessedShardActionRuntime); ok {
		observed, openErr := openRemoteWitnessObservation(payload)
		if openErr != nil {
			return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, openErr)
		}
		if err = witnessed.ExecuteWitnessedAction(ctx, request, payload, observed); err != nil {
			return shardcontrol.Response{}, err
		}
		resultPayload := []byte(`{"accepted":true}`)
		resultDigest := sha256.Sum256(append(request.Step[:], resultPayload...))
		return shardcontrol.Response{
			Code: shardcontrol.ResultAccepted, Operation: request.Operation, Step: request.Step,
			ResultDigest: resultDigest, Payload: resultPayload,
		}, nil
	}
	plan, observed, err := service.runtime.ObserveSplit(ctx, operation, request.PlanDigest, payload.Target)
	if err != nil || plan == nil || plan.OperationID() != operation {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	if request.Action == shardcontrol.Action(ActionPruneRetained) {
		if observed.Certificate == nil {
			return shardcontrol.Response{}, ErrRemoteExecution
		}
		certificate, openErr := gateway.OpenRetainedPruneCertificate(payload.RetainedPrune)
		if openErr != nil || !validRetainedPruneCertificate(
			plan, observed.Catalog, *observed.Certificate, certificate,
		) {
			return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, openErr)
		}
		observed.OlderCatalogDrained = true
		observed.CatalogDrainCertificate = certificate.CatalogDrain()
		observed.RetainedPruneCertificate = certificate
	} else if len(payload.RetainedPrune) != 0 {
		return shardcontrol.Response{}, ErrRemoteExecution
	}
	action, err := Reconcile(plan, observed)
	if err != nil || shardcontrol.Action(action.Kind) != request.Action || action.Child != request.Child ||
		observed.Catalog == nil || observed.Catalog.Generation() != payload.Catalog {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	expectedTarget, err := remoteActionTarget(plan, observed, action)
	if err != nil || expectedTarget != payload.Target ||
		remoteActionTargetsChild(action.Kind) && !targetMatchesChild(payload.Target, plan.targets[action.Child]) ||
		!remoteActionTargetsChild(action.Kind) && !targetMatchesSourceState(payload.Target, observed.SourceState) {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	expected, err := appendRemoteStepRequest(nil, plan, observed, action)
	if err != nil || expected.Operation != request.Operation || expected.Step != request.Step ||
		expected.PlanDigest != request.PlanDigest || expected.Fence != request.Fence ||
		!bytes.Equal(expected.Payload, request.Payload) {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	if err = service.runtime.ExecuteSplitAction(ctx, payload.Target, plan, observed, action); err != nil {
		return shardcontrol.Response{}, err
	}
	resultPayload := []byte(`{"accepted":true}`)
	resultDigest := sha256.Sum256(append(request.Step[:], resultPayload...))
	return shardcontrol.Response{
		Code: shardcontrol.ResultAccepted, Operation: request.Operation, Step: request.Step,
		ResultDigest: resultDigest, Payload: resultPayload,
	}, nil
}

func openRemoteStepPayload(request shardcontrol.Request) (remoteStepPayload, error) {
	if len(request.Payload) == 0 || len(request.Payload) > MaxRemoteStepPayloadBytes {
		return remoteStepPayload{}, ErrRemoteExecution
	}
	var payload remoteStepPayload
	if err := vibejson.Unmarshal(request.Payload, &payload); err != nil {
		return remoteStepPayload{}, err
	}
	canonical, err := vibejson.Marshal(&payload)
	if err != nil {
		return remoteStepPayload{}, err
	}
	canonical, err = vibejson.AppendCanonicalize(nil, canonical)
	if err != nil || !bytes.Equal(canonical, request.Payload) ||
		payload.Action != uint8(request.Action) || payload.Child != request.Child ||
		!payload.Target.valid() || payload.Catalog == 0 || payload.CatalogDigest == ([32]byte{}) ||
		payload.AdmissionRevision == 0 || payload.Sequence != remoteActionWitnessSequence(Action{
		Kind: ActionKind(request.Action), Child: request.Child,
	}) || payload.PredecessorDigest == ([32]byte{}) ||
		payload.PredecessorDigest != remoteStepPredecessorDigest(payload) {
		return remoteStepPayload{}, errors.Join(ErrRemoteExecution, err)
	}
	return payload, nil
}

func openRemoteWitnessObservation(payload remoteStepPayload) (Observation, error) {
	state, err := replicatedstate.OpenState(payload.State)
	if err != nil {
		return Observation{}, err
	}
	serving, err := openWireServingState(payload.Serving)
	if err != nil {
		return Observation{}, err
	}
	result := Observation{SourceState: state, SourceStatus: serving.Status, SourceServing: serving}
	if len(payload.Artifacts) != 0 {
		value, openErr := rangesplit.OpenChildArtifactSet(payload.Artifacts)
		if openErr != nil {
			return Observation{}, openErr
		}
		result.Artifacts = &value
	}
	if len(payload.Tail) != 0 {
		value, openErr := rangesplit.OpenTailCursor(payload.Tail)
		if openErr != nil {
			return Observation{}, openErr
		}
		result.Tail = &value
	}
	if len(payload.Certificate) != 0 {
		value, openErr := rangesplit.OpenCutoverCertificate(payload.Certificate)
		if openErr != nil {
			return Observation{}, openErr
		}
		result.Certificate = value
	}
	for index, stage := range payload.Stages {
		if stage.Child >= uint8(len(result.Stages)) || index != 0 && payload.Stages[index-1].Child >= stage.Child {
			return Observation{}, ErrRemoteExecution
		}
		value, openErr := rangesplit.OpenChildStageCursor(stage.Value)
		if openErr != nil || value == nil || value.Child() != stage.Child {
			return Observation{}, errors.Join(ErrRemoteExecution, openErr)
		}
		result.Stages[stage.Child] = value
	}
	return result, nil
}

// OpenRemoteActionTarget verifies the complete canonical remote-step envelope
// and returns its fixed-width routing authority. The returned value contains no
// borrowed strings or payload slices.
func OpenRemoteActionTarget(request shardcontrol.Request) (ShardActionTarget, error) {
	payload, err := openRemoteStepPayload(request)
	return payload.Target, err
}
