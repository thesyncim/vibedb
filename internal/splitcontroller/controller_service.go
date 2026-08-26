package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardcontrol"
	vibejson "github.com/thesyncim/vibejson"
)

var ErrControllerTrigger = errors.New("splitcontroller: invalid replicated controller trigger")

type ControllerCatalog interface {
	ReplicatedOperationJournal
	Read(context.Context) (*gateway.Snapshot, error)
}

// PlanObserver returns one coherent cut from the local durable split, SQL,
// WAL, runtime, and catalog-drain authorities. It receives the plan decoded
// from RF3; controller correctness never depends on remembered process state.
type PlanObserver interface {
	ObservePlan(context.Context, *Plan) (Observation, error)
}

type reconcileTriggerPayload struct {
	Revision     uint64   `json:"revision"`
	IntentDigest [32]byte `json:"intent_digest"`
}

type reconcileTriggerResult struct {
	Action   uint8  `json:"action"`
	Child    uint8  `json:"child"`
	Revision uint64 `json:"revision"`
}

// AppendReconcileTrigger constructs the source-host request from one exact
// replicated directory record. The request carries no guessed data fence; the
// source obtains a coherent observation before emitting an ordinary fenced
// per-action RPC.
func AppendReconcileTrigger(
	dst []byte, record gateway.ReplicatedOperationRecord,
) (shardcontrol.Request, error) {
	if record.ID == ([32]byte{}) || record.Kind != gateway.ReplicatedOperationSplit ||
		record.Revision == 0 || record.IntentDigest == ([32]byte{}) {
		return shardcontrol.Request{}, ErrControllerTrigger
	}
	payload := reconcileTriggerPayload{
		Revision: record.Revision, IntentDigest: record.IntentDigest,
	}
	raw, err := vibejson.Marshal(&payload)
	if err != nil {
		return shardcontrol.Request{}, err
	}
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst) > shardcontrol.MaxPayloadBytes {
		return shardcontrol.Request{}, errors.Join(err, ErrControllerTrigger)
	}
	step := reconcileTriggerStep(record.ID, record.Revision, record.IntentDigest)
	return shardcontrol.Request{
		Action:    shardcontrol.ActionReconcileSplit,
		Operation: record.ID, Step: step, PlanDigest: record.IntentDigest, Payload: dst,
	}, nil
}

// ControllerService is the gateway-local replicated split controller. Catalog
// reads/publication remain in the catalog RF3 authority; shard processes expose
// only observation, artifact/tail data, and idempotent fenced actions.
type ControllerService struct {
	catalog   ControllerCatalog
	observer  PlanObserver
	router    ShardControlRouter
	admission PlanAdmissionCoordinator
	gateway   GatewaySplitActionExecutor
}

// ExecuteReplicatedOperation reconstructs and advances one operation directly
// from catalog RF3. It owns no local progress record: ExecuteRemoteReplicatedStep
// persists Running before the shard RPC and the next pass resumes after a
// gateway crash or an outcome-unknown response.
func (service *ControllerService) ExecuteReplicatedOperation(
	ctx context.Context,
	operation [32]byte,
) (Action, error) {
	if service == nil || ctx == nil || operation == ([32]byte{}) {
		return Action{}, ErrControllerTrigger
	}
	record, err := service.catalog.ReadOperation(ctx, operation)
	if err != nil {
		return Action{}, errors.Join(err, ErrControllerTrigger)
	}
	catalog, err := service.catalog.Read(ctx)
	if err != nil {
		return Action{}, err
	}
	plan, err := OpenPlanIntent(record.Intent, catalog)
	if err != nil || [32]byte(plan.OperationID()) != operation ||
		record.IntentDigest != sha256.Sum256(record.Intent) {
		return Action{}, errors.Join(err, ErrControllerTrigger)
	}
	if service.admission != nil {
		admission, admissionErr := NewPlanAdmission(catalog, plan)
		if admissionErr != nil {
			return Action{}, admissionErr
		}
		if admissionErr = service.admission.AdmitPlan(ctx, catalog, plan, admission); admissionErr != nil {
			return Action{}, admissionErr
		}
	}
	observed, err := service.observer.ObservePlan(ctx, plan)
	if err != nil {
		return Action{}, err
	}
	if service.gateway != nil {
		return ExecuteCoordinatedReplicatedStep(
			ctx, service.catalog, plan, observed, service.router, service.gateway,
		)
	}
	return ExecuteRemoteReplicatedStep(ctx, service.catalog, plan, observed, service.router)
}

func NewServingControllerService(
	catalog ControllerCatalog,
	observer PlanObserver,
	router ShardControlRouter,
	admission PlanAdmissionCoordinator,
	gateway GatewaySplitActionExecutor,
) (*ControllerService, error) {
	if catalog == nil || observer == nil || router == nil || admission == nil || gateway == nil {
		return nil, ErrControllerTrigger
	}
	return &ControllerService{
		catalog: catalog, observer: observer, router: router,
		admission: admission, gateway: gateway,
	}, nil
}

func NewControllerService(
	catalog ControllerCatalog, observer PlanObserver, router ShardControlRouter,
) (*ControllerService, error) {
	if catalog == nil || observer == nil || router == nil {
		return nil, ErrControllerTrigger
	}
	return &ControllerService{catalog: catalog, observer: observer, router: router}, nil
}

func (service *ControllerService) ExecuteAction(
	ctx context.Context, _ rafttransport.PeerIdentity, request shardcontrol.Request,
) (shardcontrol.Response, error) {
	if service == nil || ctx == nil || request.Action != shardcontrol.ActionReconcileSplit ||
		request.Fence != (shardcontrol.Fence{}) || request.Child != 0 {
		return shardcontrol.Response{}, ErrControllerTrigger
	}
	var payload reconcileTriggerPayload
	if err := vibejson.Unmarshal(request.Payload, &payload); err != nil {
		return shardcontrol.Response{}, errors.Join(err, ErrControllerTrigger)
	}
	canonical, err := vibejson.Marshal(&payload)
	if err != nil {
		return shardcontrol.Response{}, err
	}
	canonical, err = vibejson.AppendCanonicalize(nil, canonical)
	if err != nil || !bytes.Equal(canonical, request.Payload) || payload.Revision == 0 ||
		payload.IntentDigest == ([32]byte{}) || request.PlanDigest != payload.IntentDigest ||
		request.Step != reconcileTriggerStep(request.Operation, payload.Revision, payload.IntentDigest) {
		return shardcontrol.Response{}, errors.Join(err, ErrControllerTrigger)
	}
	record, err := service.catalog.ReadOperation(ctx, request.Operation)
	if err != nil || record.Revision != payload.Revision ||
		record.IntentDigest != payload.IntentDigest {
		return shardcontrol.Response{}, errors.Join(err, ErrControllerTrigger)
	}
	action, err := service.ExecuteReplicatedOperation(ctx, request.Operation)
	if err != nil {
		return shardcontrol.Response{}, err
	}
	result := reconcileTriggerResult{
		Action: uint8(action.Kind), Child: action.Child, Revision: payload.Revision,
	}
	resultRaw, err := vibejson.Marshal(&result)
	if err != nil {
		return shardcontrol.Response{}, err
	}
	resultRaw, err = vibejson.AppendCanonicalize(nil, resultRaw)
	if err != nil {
		return shardcontrol.Response{}, err
	}
	digest := sha256.Sum256(append(request.Step[:], resultRaw...))
	return shardcontrol.Response{
		Code: shardcontrol.ResultAccepted, Operation: request.Operation, Step: request.Step,
		ResultDigest: digest, Payload: resultRaw,
	}, nil
}

func reconcileTriggerStep(operation [32]byte, revision uint64, intent [32]byte) [32]byte {
	var raw [32 + 8 + 32]byte
	copy(raw[:32], operation[:])
	binary.LittleEndian.PutUint64(raw[32:40], revision)
	copy(raw[40:], intent[:])
	return sha256.Sum256(raw[:])
}
