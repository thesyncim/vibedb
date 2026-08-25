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
	ObserveSplit(context.Context, OperationID, [32]byte) (*Plan, Observation, error)
	ExecuteSplitAction(context.Context, *Plan, Observation, Action) error
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
	var payload remoteStepPayload
	if err := vibejson.Unmarshal(request.Payload, &payload); err != nil {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	canonical, err := vibejson.Marshal(&payload)
	if err != nil {
		return shardcontrol.Response{}, err
	}
	canonical, err = vibejson.AppendCanonicalize(nil, canonical)
	if err != nil || !bytes.Equal(canonical, request.Payload) ||
		payload.Action != uint8(request.Action) || payload.Child != request.Child {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	operation := OperationID(request.Operation)
	plan, observed, err := service.runtime.ObserveSplit(ctx, operation, request.PlanDigest)
	if err != nil || plan == nil || plan.OperationID() != operation {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	action, err := Reconcile(plan, observed)
	if err != nil || shardcontrol.Action(action.Kind) != request.Action || action.Child != request.Child ||
		action.CatalogGeneration != payload.Catalog {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	expected, err := appendRemoteStepRequest(nil, plan, observed, action)
	if err != nil || expected.Operation != request.Operation || expected.Step != request.Step ||
		expected.PlanDigest != request.PlanDigest || expected.Fence != request.Fence ||
		!bytes.Equal(expected.Payload, request.Payload) {
		return shardcontrol.Response{}, errors.Join(ErrRemoteExecution, err)
	}
	if err = service.runtime.ExecuteSplitAction(ctx, plan, observed, action); err != nil {
		return shardcontrol.Response{}, err
	}
	resultPayload := []byte(`{"accepted":true}`)
	resultDigest := sha256.Sum256(append(request.Step[:], resultPayload...))
	return shardcontrol.Response{
		Code: shardcontrol.ResultAccepted, Operation: request.Operation, Step: request.Step,
		ResultDigest: resultDigest, Payload: resultPayload,
	}, nil
}
