package gateway

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type transactionRecoveryReadClient struct {
	states     map[string]shardservice.ReplicatedMemberState
	value      []byte
	operation  shardservice.ReplicatedOperation
	capability serviceauthz.Capability
	member     uint64
	fence      shardservice.ReplicatedFence
}

func (client *transactionRecoveryReadClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	state := client.states[endpoint.Address]
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	client.operation, client.capability, client.member =
		request.Operation, request.Capability, endpoint.Member
	client.fence = request.Fence
	return &shardservice.ReplicatedResponse{
		Kind:     shardservice.ReplicatedTransactionReadResult,
		HasState: true, State: state, ReadApplied: state.Applied,
		Value: client.value,
	}, nil
}

func TestMembershipStableTransactionRecoveryRetainsLogicalAndServingFences(t *testing.T) {
	for _, test := range []struct {
		name    string
		stable  bool
		mutate  func(*shardservice.ReplicatedMemberState)
		allowed bool
	}{
		{"legacy-membership", false, func(s *shardservice.ReplicatedMemberState) { s.Fence.Command.ReplicaSetVersion++ }, false},
		{"stable-membership", true, func(s *shardservice.ReplicatedMemberState) { s.Fence.Command.ReplicaSetVersion++ }, true},
		{"schema", true, func(s *shardservice.ReplicatedMemberState) { s.Fence.Command.SchemaGeneration++ }, false},
		{"ownership", true, func(s *shardservice.ReplicatedMemberState) { s.Fence.Command.OwnershipEpoch++ }, false},
		{"protection", true, func(s *shardservice.ReplicatedMemberState) { s.Fence.Command.ProtectionEpoch++ }, false},
		{"allocation", true, func(s *shardservice.ReplicatedMemberState) { s.Fence.AllocationGeneration++ }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			route, _, states := testReplicatedRouteCommand(t)
			route.membershipStable = test.stable
			for address, state := range states {
				state.Commit, state.Applied, state.CheckpointApplied = 11, 11, 10
				test.mutate(&state)
				states[address] = state
			}
			record := replicatedTransactionParticipantRecord()
			value, err := shardservice.AppendReplicatedTransactionReadValue(nil, shardservice.ReplicatedTransactionReadValue{
				Kind: shardservice.ReplicatedTransactionLookupParticipant, Complete: true,
				Records: []replicatedstate.TransactionRecoveryRecord{record},
			})
			if err != nil {
				t.Fatal(err)
			}
			client := &transactionRecoveryReadClient{states: states, value: value}
			executor, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.ReadTransactionRecovery(t.Context(), route, replicatedstate.TransactionRecoveryReadRequest{
				Kind: replicatedstate.TransactionRecoveryLookupParticipant, ID: record.ID, MinimumApplied: 10,
				MaxRows: 1, MaxBytes: replicatedstate.TransactionRecoverySummaryBytes,
			})
			if !test.allowed {
				if err == nil {
					t.Fatal("changed authority accepted")
				}
				return
			}
			if err != nil || !result.Complete || len(result.Records) != 1 || !reflect.DeepEqual(result.Records[0], record) {
				t.Fatalf("recovery=%+v err=%v", result, err)
			}
			if client.member != 2 || client.fence != states["m2"].Fence {
				t.Fatal("read did not bind the current leader's exact physical serving fence")
			}
		})
	}
}

func TestReplicatedExecutorTransactionRecoveryIsLeaderOnlyAndCanonical(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	for address, state := range states {
		state.Commit, state.Applied, state.CheckpointApplied = 11, 11, 10
		states[address] = state
	}
	record := replicatedTransactionParticipantRecord()
	value, err := shardservice.AppendReplicatedTransactionReadValue(nil,
		shardservice.ReplicatedTransactionReadValue{
			Kind:     shardservice.ReplicatedTransactionLookupParticipant,
			Complete: true, Records: []replicatedstate.TransactionRecoveryRecord{record},
		})
	if err != nil {
		t.Fatal(err)
	}
	client := &transactionRecoveryReadClient{states: states, value: value}
	executor, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.ReadTransactionRecovery(context.Background(), route,
		replicatedstate.TransactionRecoveryReadRequest{
			Kind: replicatedstate.TransactionRecoveryLookupParticipant,
			ID:   record.ID, MinimumApplied: 10, MaxRows: 1,
			MaxBytes: replicatedstate.TransactionRecoverySummaryBytes,
		})
	if err != nil || !result.Complete || result.Applied != 11 ||
		len(result.Records) != 1 || !reflect.DeepEqual(result.Records[0], record) ||
		client.member != 2 || client.operation != shardservice.ReplicatedTransactionRead ||
		client.capability != serviceauthz.CapabilityTransactionRecovery {
		t.Fatalf("recovery result=%+v member=%d operation=%d capability=%x err=%v",
			result, client.member, client.operation, client.capability, err)
	}
}

func TestReplicatedExecutorTransactionRecoveryRejectsMalformedResult(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	state.Commit, state.Applied = 11, 11
	states["m2"] = state
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	record := replicatedTransactionParticipantRecord()
	value, err := shardservice.AppendReplicatedTransactionReadValue(nil,
		shardservice.ReplicatedTransactionReadValue{
			Kind:     shardservice.ReplicatedTransactionLookupParticipant,
			Complete: true, Records: []replicatedstate.TransactionRecoveryRecord{record},
		})
	if err != nil {
		t.Fatal(err)
	}
	wrongID := append([]byte(nil), value...)
	wrongID[shardservice.ReplicatedTransactionReadValueHeaderBytes+15]++
	for name, malformed := range map[string][]byte{
		"wrong-id": wrongID,
		"trailing": append(append([]byte(nil), value...), 0),
	} {
		t.Run(name, func(t *testing.T) {
			executor, constructErr := NewReplicatedExecutor(
				&transactionRecoveryReadClient{states: states, value: malformed}, 1, time.Second,
			)
			if constructErr != nil {
				t.Fatal(constructErr)
			}
			_, readErr := executor.ReadTransactionRecovery(context.Background(), route,
				replicatedstate.TransactionRecoveryReadRequest{
					Kind: replicatedstate.TransactionRecoveryLookupParticipant,
					ID:   record.ID, MinimumApplied: 10, MaxRows: 1,
					MaxBytes: replicatedstate.TransactionRecoverySummaryBytes,
				})
			if !errors.Is(readErr, ErrReplicatedRoute) {
				t.Fatalf("malformed recovery error=%T %v", readErr, readErr)
			}
		})
	}
}

func TestReplicatedTransactionRecoveryScanBindsExclusiveCursor(t *testing.T) {
	cursor := distributedtxn.ID{9}
	after := cursor
	after[15] = 1
	read := replicatedstate.TransactionRecoveryReadRequest{
		Kind: replicatedstate.TransactionRecoveryScanCoordinator, ID: cursor,
	}
	value := shardservice.ReplicatedTransactionReadValue{
		Kind:    shardservice.ReplicatedTransactionScanCoordinators,
		Records: []replicatedstate.TransactionRecoveryRecord{{ID: after}},
	}
	if !replicatedTransactionRecoveryResultMatches(read, value) {
		t.Fatal("strictly later scan page rejected")
	}
	value.Records[0].ID = cursor
	if replicatedTransactionRecoveryResultMatches(read, value) {
		t.Fatal("scan page repeated its exclusive cursor")
	}
}

func replicatedTransactionParticipantRecord() replicatedstate.TransactionRecoveryRecord {
	id := distributedtxn.ID{0x72, 0x65, 0x63, 0x6f, 0x76, 0x65, 0x72, 0x79, 1}
	return replicatedstate.TransactionRecoveryRecord{
		ID: id, Role: distributedtxn.ReplicatedRoleParticipant,
		State: uint8(distributedtxn.ParticipantStaged), Revision: 1,
		PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage, PayloadCount: 1,
		CoordinatorGroup: [16]byte{1}, CoordinatorShardIncarnation: [16]byte{2},
		CoordinatorAllocation: 3, MutationDigest: distributedtxn.Digest{4},
	}
}
