package splitcontroller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/shardcontrol"
	vibejson "github.com/thesyncim/vibejson"
)

var ErrRemoteExecution = errors.New("splitcontroller: remote shard action was not durably accepted")

// MaxRemoteStepPayloadBytes is deliberately far below the generic control
// frame bound. The remote envelope is fixed-width authority and digests; a
// string-bearing PlanIntent or route accidentally embedded here fails closed.
const MaxRemoteStepPayloadBytes = 256 << 10

// ShardControlRouter selects the exact authenticated shard-control endpoint
// for one already-reconciled action. It must not rebuild or reinterpret request.
type ShardControlRouter interface {
	ExecuteShardControl(context.Context, Action, shardcontrol.Request) (shardcontrol.Response, error)
}

type remoteStepPayload struct {
	Action            uint8                           `json:"action"`
	Child             uint8                           `json:"child"`
	Catalog           uint64                          `json:"catalog"`
	CatalogDigest     [32]byte                        `json:"catalog_digest"`
	AdmissionRevision uint64                          `json:"admission_revision"`
	Sequence          uint64                          `json:"sequence"`
	ExecutionRevision uint64                          `json:"execution_revision,omitempty"`
	PredecessorDigest [32]byte                        `json:"predecessor_digest"`
	Target            ShardActionTarget               `json:"target"`
	SourceNode        rafttransport.NodeID            `json:"source_node"`
	CaptureHead       uint64                          `json:"capture_head"`
	State             []byte                          `json:"state"`
	Serving           planObservationWireServingState `json:"serving"`
	Artifacts         []byte                          `json:"artifacts,omitempty"`
	Tail              []byte                          `json:"tail,omitempty"`
	Certificate       []byte                          `json:"certificate,omitempty"`
	Stages            []remoteWitnessStage            `json:"stages,omitempty"`
	Prune             []byte                          `json:"prune,omitempty"`
	RetainedPrune     []byte                          `json:"retained_prune,omitempty"`
}

type remoteWitnessStage struct {
	Child uint8  `json:"child"`
	Value []byte `json:"value"`
}

// ExecuteRemoteReplicatedStep is the serving controller composition: intent
// and running state settle in catalog RF3 before one authenticated shard RPC.
// Exact request identity is derived only from the immutable Plan and coherent
// Observation used by Reconcile.
func ExecuteRemoteReplicatedStep(
	ctx context.Context,
	journal ReplicatedOperationJournal,
	plan *Plan,
	observed Observation,
	router ShardControlRouter,
) (Action, error) {
	if router == nil {
		return Action{}, ErrRemoteExecution
	}
	return ExecuteReplicatedStep(ctx, journal, plan, observed,
		func(ctx context.Context, operation OperationID, action Action) error {
			if action.Kind == ActionComplete {
				return nil
			}
			return executeRemoteActionWave(ctx, journal, plan, observed, action, router, false)
		})
}

func appendRemoteStepRequest(
	dst []byte, plan *Plan, observed Observation, action Action,
) (shardcontrol.Request, error) {
	if plan == nil || observed.Catalog == nil || action.Kind < ActionAwaitSourceLeader ||
		action.Kind > ActionComplete {
		return shardcontrol.Request{}, ErrRemoteExecution
	}
	target, err := remoteActionTarget(plan, observed, action)
	if err != nil {
		return shardcontrol.Request{}, err
	}
	return appendRemoteStepRequestForTarget(dst, plan, observed, action, target)
}

func appendRemoteStepRequestForTarget(
	dst []byte, plan *Plan, observed Observation, action Action, target ShardActionTarget,
) (shardcontrol.Request, error) {
	if plan == nil || observed.Catalog == nil || !target.valid() ||
		action.Kind < ActionAwaitSourceLeader || action.Kind > ActionComplete {
		return shardcontrol.Request{}, ErrRemoteExecution
	}
	catalogDigest, err := gateway.CatalogSnapshotDigest(observed.Catalog)
	if err != nil {
		return shardcontrol.Request{}, errors.Join(ErrRemoteExecution, err)
	}
	payload := remoteStepPayload{
		Action: uint8(action.Kind), Child: action.Child,
		Catalog: observed.Catalog.Generation(), CatalogDigest: catalogDigest,
		AdmissionRevision: observed.Catalog.Generation(),
		Sequence:          remoteActionWitnessSequence(action), Target: target,
	}
	payload.SourceNode, err = remoteObservationSourceNode(plan, observed)
	if err != nil {
		return shardcontrol.Request{}, err
	}
	payload.CaptureHead = observed.CaptureHead
	if observed.Capture != nil {
		payload.CaptureHead = observed.Capture.Head()
	}
	if payload.State, err = replicatedstate.AppendState(nil, observed.SourceState); err != nil {
		return shardcontrol.Request{}, errors.Join(ErrRemoteExecution, err)
	}
	payload.Serving = appendWireServingState(observed.SourceServing)
	if payload.Artifacts, err = appendOptionalArtifacts(observed.Artifacts); err != nil {
		return shardcontrol.Request{}, err
	}
	if payload.Tail, err = appendOptionalTail(observed.Tail); err != nil {
		return shardcontrol.Request{}, err
	}
	if payload.Certificate, err = appendOptionalCertificate(observed.Certificate); err != nil {
		return shardcontrol.Request{}, err
	}
	if observed.Prune != nil {
		payload.Prune, err = rangesplit.AppendRetainedPruneCursor(nil, observed.Prune)
		if err != nil {
			return shardcontrol.Request{}, errors.Join(ErrRemoteExecution, err)
		}
	}
	for child, stage := range observed.Stages {
		if stage == nil {
			continue
		}
		raw, appendErr := rangesplit.AppendChildStageCursor(nil, stage)
		if appendErr != nil {
			return shardcontrol.Request{}, errors.Join(ErrRemoteExecution, appendErr)
		}
		payload.Stages = append(payload.Stages, remoteWitnessStage{Child: uint8(child), Value: raw})
	}
	payload.PredecessorDigest = remoteStepPredecessorDigest(payload)
	if action.Kind == ActionPruneRetained {
		if !observed.OlderCatalogDrained || observed.Certificate == nil ||
			!validRetainedPruneCertificate(
				plan, observed.Catalog, *observed.Certificate, observed.RetainedPruneCertificate,
			) {
			return shardcontrol.Request{}, ErrRemoteExecution
		}
		payload.RetainedPrune, err = gateway.AppendRetainedPruneCertificate(
			nil, observed.RetainedPruneCertificate,
		)
		if err != nil {
			return shardcontrol.Request{}, errors.Join(ErrRemoteExecution, err)
		}
	}
	raw, err := vibejson.Marshal(&payload)
	if err != nil {
		return shardcontrol.Request{}, err
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start == 0 || len(dst)-start > MaxRemoteStepPayloadBytes {
		return shardcontrol.Request{}, errors.Join(ErrRemoteExecution, err)
	}
	if len(dst) > shardcontrol.MaxPayloadBytes {
		return shardcontrol.Request{}, errors.Join(ErrRemoteExecution, err)
	}
	id := [32]byte(plan.OperationID())
	intent, err := AppendPlanIntent(nil, observed.Catalog, plan)
	if err != nil {
		return shardcontrol.Request{}, errors.Join(ErrRemoteExecution, err)
	}
	cursor := replicatedActionCursor(action)
	binding := observed.SourceState.Binding
	return shardcontrol.Request{
		Action: shardcontrol.Action(action.Kind), Child: action.Child,
		Operation: id, Step: replicatedActionProof(id, cursor), PlanDigest: sha256.Sum256(intent),
		Fence: shardcontrol.Fence{
			CatalogGeneration: observed.Catalog.Generation(),
			Allocation:        binding.AllocationGeneration, OwnershipEpoch: binding.OwnershipEpoch,
			SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
			RouteGeneration:   binding.RouteGeneration,
			ReplicaSetVersion: observed.SourceState.ReplicaSetVersion,
			Applied:           observed.SourceState.Applied,
		},
		Payload: dst,
	}, nil
}

func remoteActionWitnessSequence(action Action) uint64 {
	if action.Kind < ActionAwaitSourceLeader || action.Kind > ActionComplete {
		return 0
	}
	return uint64(action.Kind)<<8 | uint64(action.Child)
}

func remoteStepPredecessorDigest(payload remoteStepPayload) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/splitcontroller/remote-predecessor\x00"))
	for _, raw := range [][]byte{payload.State, payload.Artifacts, payload.Tail, payload.Certificate} {
		var length [8]byte
		binary.LittleEndian.PutUint64(length[:], uint64(len(raw)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(raw)
	}
	serving, _ := vibejson.Marshal(&payload.Serving)
	serving, _ = vibejson.AppendCanonicalize(nil, serving)
	_, _ = hash.Write(serving)
	_, _ = hash.Write(payload.SourceNode[:])
	var capture [8]byte
	binary.LittleEndian.PutUint64(capture[:], payload.CaptureHead)
	_, _ = hash.Write(capture[:])
	for _, stage := range payload.Stages {
		_, _ = hash.Write([]byte{stage.Child})
		_, _ = hash.Write(stage.Value)
	}
	// Preserve pre-prune and older witness digests while binding every byte
	// of the durable prune cursor when one is present.
	if len(payload.Prune) != 0 {
		_, _ = hash.Write([]byte("\x00retained-prune-cursor\x00"))
		_, _ = hash.Write(payload.Prune)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func remoteObservationSourceNode(plan *Plan, observed Observation) (rafttransport.NodeID, error) {
	if observed.SourceNode != (rafttransport.NodeID{}) {
		return observed.SourceNode, nil
	}
	if plan == nil || observed.Catalog == nil || observed.SourceStatus.MemberID == 0 {
		return rafttransport.NodeID{}, ErrRemoteExecution
	}
	route, ok := observed.Catalog.ResolveReplicatedRoute(
		plan.source.Distribution, plan.source.Shard,
		make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount),
	)
	if !ok {
		return rafttransport.NodeID{}, ErrRemoteExecution
	}
	for _, replica := range route.Replicas {
		if replica.Member == observed.SourceStatus.MemberID && replica.Node != (rafttransport.NodeID{}) {
			return replica.Node, nil
		}
	}
	return rafttransport.NodeID{}, ErrRemoteExecution
}

func remoteObservationStateDigest(state replicatedstate.State) [32]byte {
	hash := sha256.New()
	binding := state.Binding
	_, _ = hash.Write(binding.ClusterID[:])
	_, _ = hash.Write(binding.ClusterIncarnation[:])
	_, _ = hash.Write(binding.ShardIncarnation[:])
	_, _ = hash.Write(binding.GroupID[:])
	_, _ = hash.Write(state.LastEntryDigest[:])
	_, _ = hash.Write(state.DataChainDigest[:])
	_, _ = hash.Write(state.SnapshotBaseDigest[:])
	_, _ = hash.Write(binding.OwnedRange.Start[:])
	_, _ = hash.Write(binding.OwnedRange.End.Point[:])
	if binding.OwnedRange.End.Max {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	var scalars [11 * 8]byte
	for index, value := range [...]uint64{
		state.Applied, state.LastTerm, state.ReplicaSetVersion,
		binding.TopologyRecoveryEpoch, binding.AllocationGeneration,
		binding.ActivePolicyGeneration, binding.ProtectionEpoch, binding.OwnershipEpoch,
		binding.SchemaGeneration, binding.RoutingVersion, binding.RouteGeneration,
	} {
		binary.LittleEndian.PutUint64(scalars[index*8:], value)
	}
	_, _ = hash.Write(scalars[:])
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(binding.Distribution)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(binding.Distribution))
	binary.LittleEndian.PutUint64(length[:], uint64(len(binding.Shard)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(binding.Shard))
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}
