package shardservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type routeSettlementOwner struct {
	*fakeReplicatedOwner
	result  raftservice.RouteReleaseReceiptReadResult
	request raftservice.RouteReleaseReceiptReadRequest
	lease   raftservice.RouteReleaseReceiptReadLease
	err     error
}

func (owner *routeSettlementOwner) ReadRouteReleaseReceipt(
	_ context.Context,
	request raftservice.RouteReleaseReceiptReadRequest,
) (raftservice.RouteReleaseReceiptReadResult, raftservice.RouteReleaseReceiptReadLease, error) {
	owner.request = request
	result := owner.result
	result.State = owner.state
	return result, owner.lease, owner.err
}

func routeSettlementReleaseCommand(t *testing.T, fence ReplicatedFence) []byte {
	t.Helper()
	gate, err := routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationReleaseShared,
		Epoch:     1,
		Identity:  routegate.Identity{1},
		Binding:   routegate.Binding{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	command := replication.Command{
		Kind:                   replication.CommandRouteGate,
		AuthorityClass:         replication.CommandAuthorityRouteSession,
		ClusterID:              fence.Group.ClusterID,
		ClusterIncarnation:     fence.Group.ClusterIncarnation,
		TopologyRecoveryEpoch:  fence.Group.TopologyRecoveryEpoch,
		Distribution:           "orders",
		Shard:                  "0000-ffff",
		AllocationGeneration:   fence.AllocationGeneration,
		ShardIncarnation:       fence.Group.ShardIncarnation,
		GroupID:                fence.Group.GroupID,
		ReplicaSetVersion:      fence.Command.ReplicaSetVersion,
		ActivePolicyGeneration: fence.Command.ActivePolicyGeneration,
		ProtectionEpoch:        fence.Command.ProtectionEpoch,
		OwnershipEpoch:         fence.Command.OwnershipEpoch,
		SchemaGeneration:       fence.Command.SchemaGeneration,
		RoutingVersion:         fence.Command.RoutingVersion,
		RouteGeneration:        fence.Command.RouteGeneration,
		Tenant:                 []byte("tenant"),
		ClientID:               replication.ID128{1},
		ClientEpoch:            2,
		ClientSequence:         3,
		AckThrough:             2,
		Fingerprint:            replication.Digest(sha256.Sum256([]byte("release"))),
		RouteGate:              gate,
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func routeSettlementRequest(t *testing.T) *ReplicatedRequest {
	fence := testReplicatedFence()
	command := routeSettlementReleaseCommand(t, fence)
	return &ReplicatedRequest{
		Operation:  ReplicatedRouteSettlement,
		Authority:  serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17},
		Capability: serviceauthz.CapabilityRequestLedger,
		Fence:      fence,
		RouteSettlement: ReplicatedRouteSettlementRequest{
			Mode:           ReplicatedRouteSettlementReadReleaseReceipt,
			MinimumApplied: 17,
			Command:        command,
		},
	}
}

func TestReplicatedRouteSettlementReceiptRoundTripsExactCommand(t *testing.T) {
	request := routeSettlementRequest(t)
	var raw bytes.Buffer
	if err := EncodeReplicatedRequest(&raw, request); err != nil {
		t.Fatal(err)
	}
	if raw.Bytes()[0] != tagReplicatedRouteSettlement || len(raw.Bytes()) != 5+replicatedRouteSettlementReadRequestBodyBytes+len(request.RouteSettlement.Command) {
		t.Fatalf("frame tag/length=%x/%d", raw.Bytes()[0], raw.Len())
	}
	decoded, err := DecodeReplicatedRequest(bytes.NewReader(raw.Bytes()))
	if err != nil || decoded.Operation != ReplicatedRouteSettlement ||
		decoded.Capability != serviceauthz.CapabilityRequestLedger ||
		decoded.RouteSettlement.Mode != ReplicatedRouteSettlementReadReleaseReceipt ||
		decoded.RouteSettlement.MinimumApplied != request.RouteSettlement.MinimumApplied ||
		!bytes.Equal(decoded.RouteSettlement.Command, request.RouteSettlement.Command) {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	scope, ok := FrontendContinuationScopeForReplicatedRequest(request)
	if !ok || scope.Action != serviceauthz.FrontendActionGatewayRouteSettlement ||
		scope.Operation != serviceauthz.ServiceOperationRouteSettlement ||
		scope.Capability != serviceauthz.CapabilityRequestLedger {
		t.Fatalf("settlement scope=%+v ok=%t", scope, ok)
	}
	for _, mutate := range []func(*ReplicatedRequest){
		func(r *ReplicatedRequest) { r.RouteSettlement.Mode = 0 },
		func(r *ReplicatedRequest) { r.RouteSettlement.MinimumApplied = 0 },
		func(r *ReplicatedRequest) { r.RouteSettlement.Command = nil },
		func(r *ReplicatedRequest) { r.Capability = serviceauthz.CapabilityDataWrite },
		func(r *ReplicatedRequest) { r.RouteSettlement.Command[0] ^= 1 },
	} {
		bad := *request
		bad.RouteSettlement = request.RouteSettlement
		bad.RouteSettlement.Command = bytes.Clone(request.RouteSettlement.Command)
		mutate(&bad)
		if validReplicatedRequest(&bad) {
			t.Fatalf("invalid settlement accepted: %+v", bad.RouteSettlement)
		}
	}

	var outcomeBytes [routegate.OutcomeBytes]byte
	completionResult, err := routegate.AppendOutcome(outcomeBytes[:0], routegate.Outcome{
		Reason: routegate.ReasonReleased, Mutated: true,
		Status: routegate.Status{Revision: 1, Epoch: 1, ReleasedPins: 1, RetainedRecords: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	completion := testReplicatedCompletionWithResult(t, request.Fence, 4,
		replicatedstate.ResultRouteGate, replicatedstate.ResultFormatRouteGate, completionResult)
	value, err := AppendReplicatedRouteSettlementValue(nil, ReplicatedRouteSettlementValue{
		Mode: ReplicatedRouteSettlementReadReleaseReceipt, CompletionAppliedSequence: 4, Completion: completion,
	})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenReplicatedRouteSettlementValue(value)
	if err != nil || opened.Mode != ReplicatedRouteSettlementReadReleaseReceipt ||
		opened.CompletionAppliedSequence != 4 || !bytes.Equal(opened.Completion, completion) {
		t.Fatalf("value=%+v err=%v", opened, err)
	}
	response := &ReplicatedResponse{Kind: ReplicatedRouteSettlementResult,
		HasState: true, State: replicatedWireState(testReplicatedServingState()), ReadApplied: 9, Value: value}
	if err := ValidateReplicatedResponse(response); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReplicatedRouteSettlementValue([]byte{1, 2, 3}); !errors.Is(err, ErrReplicatedWire) {
		t.Fatalf("malformed value err=%v", err)
	}
}

func TestReplicatedRouteSettlementReceiptDispatchRetainsLease(t *testing.T) {
	request := routeSettlementRequest(t)
	var outcomeBytes [routegate.OutcomeBytes]byte
	outcome, err := routegate.AppendOutcome(outcomeBytes[:0], routegate.Outcome{
		Reason: routegate.ReasonReleased, Mutated: true,
		Status: routegate.Status{Revision: 1, Epoch: 1, ReleasedPins: 1, RetainedRecords: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	completion := testReplicatedCompletionWithResult(t, request.Fence, 4,
		replicatedstate.ResultRouteGate, replicatedstate.ResultFormatRouteGate, outcome)
	lease := &testPointReadLease{}
	owner := &routeSettlementOwner{
		fakeReplicatedOwner: &fakeReplicatedOwner{state: testReplicatedServingState()},
		result: raftservice.RouteReleaseReceiptReadResult{
			Applied: 23, AppliedSequence: 4, Completion: completion,
		}, lease: lease,
	}
	server := testReplicatedServer(owner)
	response := server.executeReplicated(context.Background(), request)
	if response.Kind != ReplicatedRouteSettlementResult || !validReplicatedResponse(response) ||
		response.ReadApplied != 23 || response.State.Applied != 23 ||
		!bytes.Equal(owner.request.Command, request.RouteSettlement.Command) ||
		owner.request.Capability != serviceauthz.CapabilityRequestLedger ||
		owner.probeCalls.Load() != 0 || lease.released.Load() {
		t.Fatalf("response=%+v request=%+v leaseReleased=%t", response, owner.request, lease.released.Load())
	}
	response.readLease.Release()
	if !lease.released.Load() {
		t.Fatal("receipt read lease was not retained")
	}
}

func TestReplicatedRouteSettlementReceiptDispatchRequiresSourceResult(t *testing.T) {
	request := routeSettlementRequest(t)
	owner := &routeSettlementOwner{
		fakeReplicatedOwner: &fakeReplicatedOwner{state: testReplicatedServingState()},
		result:              raftservice.RouteReleaseReceiptReadResult{Applied: 23},
	}
	server := testReplicatedServer(owner)
	response := server.executeReplicated(context.Background(), request)
	if response.Kind != ReplicatedRefusal || response.Refusal != ReplicatedRefusalUnavailable ||
		!response.HasState || len(response.Value) != 0 || owner.request.Command == nil ||
		owner.probeCalls.Load() != 0 {
		t.Fatalf("missing source result response=%+v owner=%+v", response, owner.request)
	}
}
