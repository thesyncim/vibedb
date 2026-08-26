package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardcontrol"
	vibejson "github.com/thesyncim/vibejson"
)

// ShardActionRuntime reconstructs plan and observation from local durable
// authorities, then executes one idempotent existing split action.
type ShardActionRuntime interface {
	ObserveSplit(context.Context, OperationID, [32]byte, ShardActionTarget) (*Plan, Observation, error)
	ExecuteSplitAction(context.Context, ShardActionTarget, *Plan, Observation, Action) error
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
	plan, observed, err := service.runtime.ObserveSplit(ctx, operation, request.PlanDigest, payload.Target)
	if err != nil || plan == nil || plan.OperationID() != operation {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	action, err := Reconcile(plan, observed)
	if err != nil || shardcontrol.Action(action.Kind) != request.Action || action.Child != request.Child ||
		action.CatalogGeneration != payload.Catalog {
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
		!payload.Target.valid() {
		return remoteStepPayload{}, errors.Join(ErrRemoteExecution, err)
	}
	return payload, nil
}

// OpenRemoteActionTarget verifies the complete canonical remote-step envelope
// and returns its fixed-width routing authority. The returned value contains no
// borrowed strings or payload slices.
func OpenRemoteActionTarget(request shardcontrol.Request) (ShardActionTarget, error) {
	payload, err := openRemoteStepPayload(request)
	return payload.Target, err
}
