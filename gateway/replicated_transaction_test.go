package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type transactionOrchestratorClient struct {
	mu                  sync.Mutex
	states              map[string]shardservice.ReplicatedMemberState
	applied             map[raftmember.GroupKey]uint64
	participants        map[raftmember.GroupKey]replicatedstate.TransactionRecoveryRecord
	coordinators        map[raftmember.GroupKey]replicatedstate.TransactionRecoveryRecord
	manifestPages       map[raftmember.GroupKey]map[uint32][]byte
	commands            [][]byte
	authorities         []serviceauthz.Authority
	capabilities        []serviceauthz.Capability
	completions         map[[sha256.Size]byte]transactionOrchestratorCachedCompletion
	loseOperation       distributedtxn.ReplicatedOperation
	loseShard           string
	loseBeforeApply     bool
	loseEveryMatching   bool
	indexConflictShard  string
	failParticipantRead bool
	lost                bool
	calls               int
}

type transactionOrchestratorCachedCompletion struct {
	bytes   []byte
	applied uint64
}

func (client *transactionOrchestratorClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.calls++
	client.authorities = append(client.authorities, request.Authority)
	client.capabilities = append(client.capabilities, request.Capability)
	state := client.states[endpoint.Address]
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	if request.Operation == shardservice.ReplicatedTransactionRead {
		return client.transactionRead(state, request.TransactionRead)
	}
	command, err := replication.OpenCommand(request.Command)
	if err != nil {
		return nil, err
	}
	control, err := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
	if err != nil {
		return nil, err
	}
	client.commands = append(client.commands, bytes.Clone(request.Command))
	commandDigest := sha256.Sum256(request.Command)
	if cached, ok := client.completions[commandDigest]; ok {
		state = client.states[endpoint.Address]
		return transactionOrchestratorCompletionResponse(
			state, request.Command, cached.bytes, cached.applied,
		), nil
	}
	if control.Operation == client.loseOperation &&
		(client.loseShard == "" || string(command.Shard) == client.loseShard) &&
		client.loseBeforeApply && (!client.lost || client.loseEveryMatching) {
		client.lost = true
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: state,
		}, nil
	}
	group := state.Fence.Group
	client.applied[group]++
	applied := client.applied[group]
	code, revision, affected := client.apply(command, control.Command(), applied)
	for address, candidate := range client.states {
		if candidate.Fence.Group == group {
			candidate.Commit, candidate.Applied = applied, applied
			client.states[address] = candidate
		}
	}
	state = client.states[endpoint.Address]
	result := transactionOrchestratorResult(
		control.Role, control.Operation, revision, affected,
	)
	completion := appendTransactionOrchestratorCompletion(
		command, code, result[:], applied,
	)
	client.completions[commandDigest] = transactionOrchestratorCachedCompletion{
		bytes: bytes.Clone(completion), applied: applied,
	}
	if control.Operation == client.loseOperation &&
		(client.loseShard == "" || string(command.Shard) == client.loseShard) &&
		(!client.lost || client.loseEveryMatching) {
		client.lost = true
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: state,
		}, nil
	}
	return transactionOrchestratorCompletionResponse(
		state, request.Command, completion, applied,
	), nil
}

func transactionOrchestratorCompletionResponse(
	state shardservice.ReplicatedMemberState,
	command, completion []byte,
	applied uint64,
) *shardservice.ReplicatedResponse {
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedCompletion, HasState: true, State: state,
		RequestDigest: replicatedRequestDigest(command),
		Outcome: raftserve.Outcome{
			Code: raftserve.OutcomeCompletion, AppliedIndex: applied,
			CompletionAppliedSequence: applied, CompletionBytes: len(completion),
		},
		Completion: completion,
	}
}

func (client *transactionOrchestratorClient) apply(
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	applied uint64,
) (uint32, uint64, int64) {
	group := raftmember.GroupKey{
		ClusterID: command.ClusterID, ClusterIncarnation: command.ClusterIncarnation,
		TopologyRecoveryEpoch: command.TopologyRecoveryEpoch,
		ShardIncarnation:      command.ShardIncarnation, GroupID: command.GroupID,
	}
	participant := client.participants[group]
	coordinator := client.coordinators[group]
	switch control.Operation {
	case distributedtxn.ReplicatedBeginPrepareCoordinator:
		coordinator = transactionOrchestratorCoordinatorRecord(command, control, 1)
		participant = transactionOrchestratorParticipantRecord(command, control, 2)
		client.coordinators[group], client.participants[group] = coordinator, participant
		return replicatedstate.ResultApplied, 1, 0
	case distributedtxn.ReplicatedBeginPrepareManifestCoordinator:
		prefix, segments, _ := distributedtxn.OpenReplicatedManifestStart(control.Payload)
		client.storeManifestSegments(group, segments)
		coordinator = transactionOrchestratorCoordinatorRecord(
			command, control, uint64(segments.Count()),
		)
		coordinator.Payload = bytes.Clone(prefix)
		participant = transactionOrchestratorParticipantRecord(command, control, 2)
		client.coordinators[group], client.participants[group] = coordinator, participant
		return replicatedstate.ResultApplied, coordinator.Revision, 0
	case distributedtxn.ReplicatedAppendManifestSegments:
		segments, _ := distributedtxn.OpenManifestSegmentSequence(control.Payload)
		client.storeManifestSegments(group, segments)
		coordinator.Revision += uint64(segments.Count())
		client.coordinators[group] = coordinator
		return replicatedstate.ResultApplied, coordinator.Revision, 0
	case distributedtxn.ReplicatedStagePrepareParticipant:
		if participant.ID == control.ID {
			return replicatedstate.ResultTransactionConflict, participant.Revision, 0
		}
		if string(command.Shard) == client.indexConflictShard {
			participant = transactionOrchestratorParticipantRecord(command, control, 3)
			participant.State = uint8(distributedtxn.ParticipantReleased)
			client.participants[group] = participant
			return replicatedstate.ResultIndexConflict, 3, 0
		}
		participant = transactionOrchestratorParticipantRecord(command, control, 2)
		client.participants[group] = participant
		return replicatedstate.ResultApplied, 2, 0
	case distributedtxn.ReplicatedCommitCoordinator:
		coordinator.State = uint8(distributedtxn.CoordinatorCommitted)
		coordinator.Revision = control.ExpectedRevision + 1
		coordinator.CoordinatorDecision = distributedtxn.CoordinatorCommitted
		client.coordinators[group] = coordinator
		return replicatedstate.ResultApplied, coordinator.Revision, 0
	case distributedtxn.ReplicatedAbortCoordinator:
		coordinator.State = uint8(distributedtxn.CoordinatorAborted)
		coordinator.Revision = control.ExpectedRevision + 1
		coordinator.CoordinatorDecision = distributedtxn.CoordinatorAborted
		client.coordinators[group] = coordinator
		return replicatedstate.ResultApplied, coordinator.Revision, 0
	case distributedtxn.ReplicatedPulseCoordinator:
		if distributedtxn.CoordinatorState(coordinator.State) != distributedtxn.CoordinatorStaging ||
			control.ExpectedRevision != coordinator.Revision ||
			control.RecoveryPulse != coordinator.RecoveryPulse+1 {
			return replicatedstate.ResultTransactionConflict, max(1, coordinator.Revision), 0
		}
		coordinator.RecoveryPulse = control.RecoveryPulse
		client.coordinators[group] = coordinator
		return replicatedstate.ResultApplied, coordinator.Revision, 0
	case distributedtxn.ReplicatedApplyReleaseParticipant:
		if participant.ID != control.ID ||
			distributedtxn.ParticipantState(participant.State) != distributedtxn.ParticipantPrepared {
			return replicatedstate.ResultTransactionConflict, max(1, participant.Revision), 0
		}
		participant.State, participant.Revision = uint8(distributedtxn.ParticipantReleased), 4
		participant.AffectedRows, participant.AffectedRowsValid = 1, true
		client.participants[group] = participant
		return replicatedstate.ResultApplied, 4, 1
	case distributedtxn.ReplicatedAbortReleaseParticipant:
		if control.ExpectedRevision == 0 {
			if participant.ID == control.ID {
				return replicatedstate.ResultTransactionConflict, max(1, participant.Revision), 0
			}
			participant = transactionOrchestratorParticipantRecord(command, control, 1)
			participant.State = uint8(distributedtxn.ParticipantReleased)
			participant.PayloadCount = 0
			participant.CancellationWitness = true
			participant.ParticipantOrdinal = control.Participant.ParticipantOrdinal
			client.participants[group] = participant
			return replicatedstate.ResultApplied, 1, 0
		}
		if participant.ID != control.ID ||
			distributedtxn.ParticipantState(participant.State) != distributedtxn.ParticipantPrepared {
			return replicatedstate.ResultTransactionConflict, max(1, participant.Revision), 0
		}
		participant.State, participant.Revision = uint8(distributedtxn.ParticipantReleased), 4
		client.participants[group] = participant
		return replicatedstate.ResultApplied, 4, 0
	case distributedtxn.ReplicatedRetireCoordinator:
		summary, err := distributedtxn.OpenReplicatedRetirementSummary(control.Payload)
		committed := coordinator.CoordinatorDecision == distributedtxn.CoordinatorCommitted
		if err != nil || summary.AffectedRowsValid != committed ||
			!committed && summary.AffectedRows != 0 {
			return replicatedstate.ResultTransactionConflict, max(1, coordinator.Revision), -1
		}
		coordinator.State = uint8(distributedtxn.CoordinatorRetired)
		coordinator.Revision = control.ExpectedRevision + 1
		coordinator.Payload = nil
		coordinator.AffectedRows = summary.AffectedRows
		coordinator.AffectedRowsValid = summary.AffectedRowsValid
		client.coordinators[group] = coordinator
		if summary.AffectedRowsValid {
			return replicatedstate.ResultApplied, coordinator.Revision, summary.AffectedRows
		}
		return replicatedstate.ResultApplied, coordinator.Revision, -1
	default:
		return replicatedstate.ResultTransactionConflict, 1, 0
	}
}

func (client *transactionOrchestratorClient) storeManifestSegments(
	group raftmember.GroupKey,
	segments distributedtxn.ManifestSegmentSequence,
) {
	pages := client.manifestPages[group]
	if pages == nil {
		pages = make(map[uint32][]byte)
		client.manifestPages[group] = pages
	}
	iterator := segments.Iterator()
	for iterator.Next() {
		segment := iterator.Segment()
		pages[segment.Index] = bytes.Clone(segment.Raw)
	}
}

func (client *transactionOrchestratorClient) transactionRead(
	state shardservice.ReplicatedMemberState,
	read shardservice.ReplicatedTransactionReadRequest,
) (*shardservice.ReplicatedResponse, error) {
	if read.Kind == shardservice.ReplicatedTransactionLookupParticipant &&
		client.failParticipantRead {
		return nil, errors.New("injected participant read failure")
	}
	var records []replicatedstate.TransactionRecoveryRecord
	group := state.Fence.Group
	switch read.Kind {
	case shardservice.ReplicatedTransactionLookupCoordinator:
		if record := client.coordinators[group]; !record.ID.IsZero() {
			record.Payload = bytes.Clone(record.Payload)
			records = []replicatedstate.TransactionRecoveryRecord{record}
		}
	case shardservice.ReplicatedTransactionLookupParticipant:
		if record := client.participants[group]; !record.ID.IsZero() {
			records = []replicatedstate.TransactionRecoveryRecord{record}
		}
	case shardservice.ReplicatedTransactionReadManifestPage:
		if page := client.manifestPages[group][read.SegmentIndex]; len(page) != 0 {
			record := client.coordinators[group]
			record.ManifestPage = read.SegmentIndex
			record.Payload = bytes.Clone(page)
			records = []replicatedstate.TransactionRecoveryRecord{record}
		}
	}
	value, err := shardservice.AppendReplicatedTransactionReadValue(nil,
		shardservice.ReplicatedTransactionReadValue{
			Kind: read.Kind, Complete: true, Records: records,
		})
	if err != nil {
		return nil, err
	}
	return &shardservice.ReplicatedResponse{
		Kind:     shardservice.ReplicatedTransactionReadResult,
		HasState: true, State: state, ReadApplied: state.Applied, Value: value,
	}, nil
}

func transactionOrchestratorCoordinatorRecord(
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	revision uint64,
) replicatedstate.TransactionRecoveryRecord {
	payload := bytes.Clone(control.Payload)
	count := uint64(0)
	if control.PayloadKind == distributedtxn.ReplicatedPayloadCoordinator {
		var scratch [distributedtxn.MaxInlineParticipants]distributedtxn.ParticipantRef
		record, _ := distributedtxn.OpenCoordinatorInto(payload, scratch[:])
		count = uint64(len(record.Participants))
	} else {
		prefix, _, _ := distributedtxn.OpenReplicatedManifestStart(payload)
		record, _ := distributedtxn.OpenManifestCoordinator(prefix)
		count, payload = record.Manifest.ParticipantCount, bytes.Clone(prefix)
	}
	return replicatedstate.TransactionRecoveryRecord{
		ID: control.ID, Role: distributedtxn.ReplicatedRoleCoordinator,
		State: uint8(distributedtxn.CoordinatorStaging), Revision: revision,
		PayloadKind: control.PayloadKind, PayloadCount: count,
		CoordinatorGroup:            command.GroupID,
		CoordinatorShardIncarnation: command.ShardIncarnation,
		CoordinatorAllocation:       command.AllocationGeneration,
		MutationDigest:              distributedtxn.Digest(sha256.Sum256(payload)),
		Payload:                     payload,
	}
}

func transactionOrchestratorParticipantRecord(
	command replication.CommandView,
	control distributedtxn.ReplicatedCommand,
	revision uint64,
) replicatedstate.TransactionRecoveryRecord {
	return replicatedstate.TransactionRecoveryRecord{
		ID: control.ID, Role: distributedtxn.ReplicatedRoleParticipant,
		State: uint8(distributedtxn.ParticipantPrepared), Revision: revision,
		PayloadKind:                 distributedtxn.ReplicatedPayloadParticipantStage,
		PayloadCount:                uint64(command.MutationCount()),
		CoordinatorGroup:            replication.ID128(control.Participant.CoordinatorGroup),
		CoordinatorShardIncarnation: replication.ID128(control.Participant.CoordinatorShardIncarnation),
		CoordinatorAllocation:       control.Participant.CoordinatorAllocation,
		MutationDigest:              control.Participant.MutationDigest,
	}
}

func transactionOrchestratorResult(
	role distributedtxn.ReplicatedRole,
	operation distributedtxn.ReplicatedOperation,
	revision uint64,
	affected int64,
) [24]byte {
	var result [24]byte
	result[0], result[1], result[2] = byte(role), byte(operation), 2
	binary.LittleEndian.PutUint64(result[8:16], revision)
	if (operation == distributedtxn.ReplicatedApplyReleaseParticipant ||
		operation == distributedtxn.ReplicatedRetireCoordinator) && affected >= 0 {
		result[2] |= 1
		binary.LittleEndian.PutUint64(result[16:24], uint64(affected))
	}
	return result
}

func appendTransactionOrchestratorCompletion(
	command replication.CommandView,
	resultCode uint32,
	result []byte,
	applied uint64,
) []byte {
	digest := replication.CompletionResultDigest(
		resultCode, replicatedstate.ResultFormatTransaction, result,
	)
	encoded, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID: command.ClusterID, ClusterIncarnation: command.ClusterIncarnation,
		TopologyRecoveryEpoch: command.TopologyRecoveryEpoch,
		Distribution:          command.Distribution, Shard: command.Shard,
		AllocationGeneration: command.AllocationGeneration,
		ShardIncarnation:     command.ShardIncarnation, GroupID: command.GroupID,
		ReplicaSetVersion:      command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch,
		RoutingVersion:         command.RoutingVersion, RouteGeneration: command.RouteGeneration,
		Tenant: command.Tenant, ClientID: command.ClientID, ClientEpoch: command.ClientEpoch,
		ClientSequence: command.ClientSequence, Fingerprint: command.Fingerprint,
		RetryHome: command.RetryHome, AppliedSequence: applied,
		ResultCode: resultCode, ResultFormat: replicatedstate.ResultFormatTransaction,
		Storage: replication.CompletionInline, ResultLength: uint64(len(result)),
		ResultDigest: digest, InlineResult: result,
	})
	if err != nil {
		panic(err)
	}
	return encoded
}

func transactionOrchestratorRoutes(
	t testing.TB,
	count int,
) ([]ReplicatedTransactionParticipant, *transactionOrchestratorClient) {
	t.Helper()
	client := &transactionOrchestratorClient{
		states:        make(map[string]shardservice.ReplicatedMemberState, count*3),
		applied:       make(map[raftmember.GroupKey]uint64, count),
		participants:  make(map[raftmember.GroupKey]replicatedstate.TransactionRecoveryRecord, count),
		coordinators:  make(map[raftmember.GroupKey]replicatedstate.TransactionRecoveryRecord, 1),
		manifestPages: make(map[raftmember.GroupKey]map[uint32][]byte, 1),
		completions:   make(map[[sha256.Size]byte]transactionOrchestratorCachedCompletion),
	}
	participants := make([]ReplicatedTransactionParticipant, count)
	var clusterID, incarnation replication.ID128
	for index := range clusterID {
		clusterID[index], incarnation[index] = byte(index+1), byte(index+21)
	}
	for ordinal := range participants {
		group := raftmember.GroupKey{
			ClusterID: clusterID, ClusterIncarnation: incarnation,
			TopologyRecoveryEpoch: 3,
		}
		binary.LittleEndian.PutUint64(group.ShardIncarnation[:8], uint64(ordinal+1))
		binary.LittleEndian.PutUint64(group.GroupID[:8], uint64(ordinal+1))
		route := ReplicatedRoute{
			Distribution: distribution.DistributionName("data"),
			Shard:        distribution.ShardID(fmt.Sprintf("s%08d", ordinal)),
			Group:        group, AllocationGeneration: uint64(ordinal + 1),
			Command: raftservice.CommandFence{
				ReplicaSetVersion: 1, ActivePolicyGeneration: 1,
				ProtectionEpoch: 1, OwnershipEpoch: uint64(ordinal + 1),
				SchemaGeneration: 1, RelationManifestDigest: [32]byte{1},
				RoutingVersion: 1, RouteGeneration: 1,
			},
			RangeIdentity:        replication.Digest{byte(ordinal + 1), 1},
			LineageDigest:        replication.Digest{byte(ordinal + 1), 2},
			ForwardingRuleDigest: replication.Digest{byte(ordinal + 1), 3},
		}
		for member := 1; member <= 3; member++ {
			address := fmt.Sprintf("g%08d-m%d", ordinal, member)
			endpoint := ReplicatedEndpoint{
				Member: uint64(member), Node: [16]byte{byte(member)},
				StoreID:         [16]byte{byte(ordinal + 1), byte(member)},
				NodeIncarnation: uint64(member), NativeEndpoint: address, Address: address,
			}
			route.Replicas = append(route.Replicas, endpoint)
			client.states[address] = shardservice.ReplicatedMemberState{
				Fence: shardservice.ReplicatedFence{
					Group: group, AllocationGeneration: route.AllocationGeneration,
					Command: route.Command, MemberID: uint64(member),
					StoreID: endpoint.StoreID, NodeIncarnation: uint64(member), Term: 1,
				},
				LeaderID: 1, Commit: 1, Applied: 1, CheckpointApplied: 1,
			}
		}
		client.applied[group] = 1
		participants[ordinal] = ReplicatedTransactionParticipant{
			Route: route,
			Batches: []replication.RelationMutationBatch{{
				Relation: 1,
				Mutations: []replication.Mutation{{
					Kind:  replication.MutationPutAbsentOrEqual,
					Key:   []byte{byte(ordinal>>8 + 1), byte(ordinal + 1)},
					Value: []byte(`{"id":1}`),
				}},
			}},
		}
	}
	return participants, client
}

func recoverReplicatedTransactionAfterLogicalLease(
	t *testing.T,
	orchestrator *ReplicatedTransactionOrchestrator,
	handle *ReplicatedTransactionRecoveryHandle,
) (ReplicatedTransactionResult, error) {
	t.Helper()
	for pulse := uint8(1); pulse < replicatedTransactionRecoveryPulseLimit; pulse++ {
		result, err := orchestrator.Recover(context.Background(), handle)
		if !errors.Is(err, ErrRecoveryNotReady) || result.Recovery != handle {
			t.Fatalf("logical recovery pulse %d result=%+v err=%v", pulse, result, err)
		}
	}
	return orchestrator.Recover(context.Background(), handle)
}

func TestReplicatedTransactionOrchestratorExecutesExactFusedSchedule(t *testing.T) {
	for _, count := range []int{1, 2, 65, 4097} {
		t.Run(fmt.Sprintf("participants-%d", count), func(t *testing.T) {
			participants, client := transactionOrchestratorRoutes(t, count)
			executor, err := NewReplicatedExecutorWithOptions(
				client, ReplicatedExecutorOptions{
					MaxAttempts: 1, AttemptTimeout: time.Second,
					LeaderHintCapacity: max(128, count),
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			ids := bytes.Repeat([]byte{byte(count%251 + 1)}, 16)
			orchestrator, err := NewReplicatedTransactionOrchestrator(
				ReplicatedTransactionOrchestratorOptions{
					Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 16,
					MaxInFlightBytes: 64 << 20,
					MaxMutations:     uint64(count), MaxMutationBytes: uint64(count * 64),
					RecoveryTimeout: time.Minute, IDSource: bytes.NewReader(ids),
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			result, err := orchestrator.Execute(context.Background(), 7, participants)
			if err != nil || !result.Committed || result.AffectedRows != int64(count) ||
				result.Recovery != nil {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			client.mu.Lock()
			commands := append([][]byte(nil), client.commands...)
			client.mu.Unlock()
			critical, retire, manifestAppend := 0, 0, 0
			seenPrepare := make(map[uint32]struct{}, count)
			seenFinish := make(map[string]struct{}, count)
			for _, raw := range commands {
				command, openErr := replication.OpenCommand(raw)
				if openErr != nil {
					t.Fatal(openErr)
				}
				control, openErr := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
				if openErr != nil {
					t.Fatal(openErr)
				}
				switch control.Operation {
				case distributedtxn.ReplicatedRetireCoordinator:
					retire++
				case distributedtxn.ReplicatedAppendManifestSegments:
					manifestAppend++
				default:
					critical++
				}
				if control.Operation == distributedtxn.ReplicatedStagePrepareParticipant ||
					control.Operation == distributedtxn.ReplicatedBeginPrepareCoordinator ||
					control.Operation == distributedtxn.ReplicatedBeginPrepareManifestCoordinator {
					seenPrepare[control.Participant.ParticipantOrdinal] = struct{}{}
				}
				if control.Operation == distributedtxn.ReplicatedApplyReleaseParticipant {
					seenFinish[string(command.Distribution)+"/"+string(command.Shard)] = struct{}{}
				}
			}
			if critical != 2*count+1 || retire != 1 || manifestAppend != 0 ||
				len(seenPrepare) != count || len(seenFinish) != count {
				t.Fatalf("critical=%d retire=%d manifest=%d prepares=%d finishes=%d",
					critical, retire, manifestAppend, len(seenPrepare), len(seenFinish))
			}
		})
	}
}

func TestReplicatedTransactionSingletonRecoveryRetriesUnknownCommit(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 1)
	client.loseOperation = distributedtxn.ReplicatedCommitCoordinator
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 1,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     1, MaxMutationBytes: 64, RecoveryTimeout: time.Minute,
			IDSource: bytes.NewReader(bytes.Repeat([]byte{0x17}, 16)),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestrator.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil ||
		transactionErr.Committed || len(transactionErr.Recovery.Participants) != 1 ||
		len(transactionErr.Recovery.Pending) != 1 {
		t.Fatalf("execute error=%T %+v", executeErr, transactionErr)
	}
	result, err := orchestrator.Recover(context.Background(), transactionErr.Recovery)
	if err != nil || !result.Committed || result.AffectedRows != 1 || result.Recovery != nil {
		t.Fatalf("recovery result=%+v err=%v", result, err)
	}
	if len(transactionErr.Recovery.Pending) != 0 {
		t.Fatalf("settled recovery retained %d pending operations",
			len(transactionErr.Recovery.Pending))
	}
}

func TestReplicatedTransactionRecoveryUsesConfiguredServiceAuthority(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 1)
	client.loseOperation = distributedtxn.ReplicatedCommitCoordinator
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	caller := serviceauthz.Authority{Node: [16]byte{0x31}, Generation: 7}
	recovery := serviceauthz.Authority{Node: [16]byte{0x72}, Generation: 7}
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 1,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     1, MaxMutationBytes: 64, RecoveryTimeout: time.Minute,
			RecoveryAuthority: recovery,
			IDSource:          bytes.NewReader(bytes.Repeat([]byte{0x73}, 16)),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(context.Background(), caller)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestrator.Execute(ctx, 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil {
		t.Fatalf("execute error=%T %+v", executeErr, transactionErr)
	}
	client.mu.Lock()
	executeCalls := len(client.authorities)
	for index, authority := range client.authorities {
		if authority != caller {
			client.mu.Unlock()
			t.Fatalf("execute authority[%d]=%+v want caller %+v", index, authority, caller)
		}
	}
	for index, capability := range client.capabilities {
		if capability != serviceauthz.CapabilityDataWrite {
			client.mu.Unlock()
			t.Fatalf("execute capability[%d]=%x want data-write %x", index,
				capability, serviceauthz.CapabilityDataWrite)
		}
	}
	client.mu.Unlock()

	result, recoverErr := orchestrator.Recover(ctx, transactionErr.Recovery)
	if recoverErr != nil || !result.Committed || result.AffectedRows != 1 {
		t.Fatalf("recovery result=%+v err=%v", result, recoverErr)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.authorities) == executeCalls {
		t.Fatal("recovery made no replicated calls")
	}
	for index, authority := range client.authorities[executeCalls:] {
		if authority != recovery {
			t.Fatalf("recovery authority[%d]=%+v want service %+v", index, authority, recovery)
		}
	}
	for index, capability := range client.capabilities[executeCalls:] {
		if capability != serviceauthz.CapabilityTransactionRecovery {
			t.Fatalf("recovery capability[%d]=%x want transaction-recovery %x", index,
				capability, serviceauthz.CapabilityTransactionRecovery)
		}
	}
}

func TestReplicatedTransactionRecoveryRejectsAuthenticatedCallerWithoutServiceAuthority(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 1)
	client.loseOperation = distributedtxn.ReplicatedCommitCoordinator
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 1,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     1, MaxMutationBytes: 64, RecoveryTimeout: time.Minute,
			IDSource: bytes.NewReader(bytes.Repeat([]byte{0x74}, 16)),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	caller := serviceauthz.Authority{Node: [16]byte{0x41}, Generation: 9}
	ctx, err := serviceauthz.WithAuthority(context.Background(), caller)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestrator.Execute(ctx, 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil {
		t.Fatalf("execute error=%T %+v", executeErr, transactionErr)
	}
	client.mu.Lock()
	calls := client.calls
	client.mu.Unlock()
	if _, recoverErr := orchestrator.Recover(ctx, transactionErr.Recovery); !errors.Is(
		recoverErr, ErrReplicatedTransaction,
	) {
		t.Fatalf("recovery error=%v", recoverErr)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.calls != calls {
		t.Fatalf("unauthorized recovery made %d replicated calls", client.calls-calls)
	}
}

func TestReplicatedTransactionPrepareConflictStopsUntouchedWideShards(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 65)
	idBytes := bytes.Repeat([]byte{0x52}, 16)
	var id distributedtxn.ID
	copy(id[:], idBytes)
	coordinator := replicatedTransactionCoordinatorOrdinal(id, len(participants))
	conflict := 0
	if conflict == coordinator {
		conflict++
	}
	client.indexConflictShard = string(participants[conflict].Route.Shard)
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 1,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     65, MaxMutationBytes: 65 * 64,
			RecoveryTimeout: time.Minute, IDSource: bytes.NewReader(idBytes),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, executeErr := orchestrator.Execute(context.Background(), 7, participants)
	if !errors.Is(executeErr, ErrReplicatedTransactionConflict) ||
		errors.Is(executeErr, ErrReplicatedTransactionUnknown) || result.Committed ||
		result.Recovery != nil {
		t.Fatalf("result=%+v err=%v", result, executeErr)
	}
	client.mu.Lock()
	commands := append([][]byte(nil), client.commands...)
	client.mu.Unlock()
	remotePrepares := 0
	for _, raw := range commands {
		command, openErr := replication.OpenCommand(raw)
		if openErr != nil {
			t.Fatal(openErr)
		}
		control, openErr := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
		if openErr != nil {
			t.Fatal(openErr)
		}
		if control.Operation == distributedtxn.ReplicatedStagePrepareParticipant {
			remotePrepares++
		}
	}
	if remotePrepares != 1 {
		t.Fatalf("remote prepares=%d, want only the first deterministic conflict", remotePrepares)
	}
}

func TestReplicatedTransactionIncompleteFinishDoesNotExposePartialRows(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 3)
	client.loseOperation = distributedtxn.ReplicatedApplyReleaseParticipant
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	idBytes := bytes.Repeat([]byte{0x53}, 16)
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 1,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     3, MaxMutationBytes: 3 * 64,
			RecoveryTimeout: time.Minute, IDSource: bytes.NewReader(idBytes),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, executeErr := orchestrator.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || !transactionErr.Committed ||
		!result.Committed || result.AffectedRows != 0 || result.Recovery == nil {
		t.Fatalf("partial result=%+v err=%v transaction=%+v", result, executeErr, transactionErr)
	}
	for index := range result.Recovery.Pending {
		view, openErr := replication.OpenCommand(result.Recovery.Pending[index].Command)
		if openErr != nil || view.Fingerprint != nativeCommandViewFingerprint(view) {
			t.Fatalf("pending %d fingerprint mismatch open=%v got=%x want=%x", index,
				openErr, view.Fingerprint, nativeCommandViewFingerprint(view))
		}
	}
	recovered, recoverErr := orchestrator.Recover(context.Background(), result.Recovery)
	if recoverErr != nil || !recovered.Committed || recovered.AffectedRows != 3 ||
		recovered.Recovery != nil {
		t.Fatalf("recovered=%+v err=%v", recovered, recoverErr)
	}
}

func TestReplicatedTransactionManifestBuffersScaleWithEmittedPages(t *testing.T) {
	for _, participantsCount := range []int{65, 4097} {
		t.Run(fmt.Sprintf("participants-%d", participantsCount), func(t *testing.T) {
			participants, _ := transactionOrchestratorRoutes(t, participantsCount)
			orchestrator := &ReplicatedTransactionOrchestrator{
				maxMutations: uint64(participantsCount), maxMutationBytes: uint64(participantsCount * 64),
			}
			plan, err := orchestrator.plan(participants)
			if err != nil {
				t.Fatal(err)
			}
			descriptor, initial, err := replicatedTransactionManifest(plan)
			if err != nil || descriptor.ParticipantCount != uint64(participantsCount) ||
				len(initial) == 0 || cap(initial) >= distributedtxn.MaxManifestSegmentSequenceBytes {
				t.Fatalf("descriptor=%+v initial=%d/%d err=%v",
					descriptor, len(initial), cap(initial), err)
			}
		})
	}
}

func TestReplicatedTransactionRecoverRetriesUnknownCommitByteIdentically(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 65)
	client.loseOperation = distributedtxn.ReplicatedCommitCoordinator
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	idBytes := bytes.Repeat([]byte{0x79}, 16)
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 16,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     65, MaxMutationBytes: 65 * 64, RecoveryTimeout: time.Minute,
			IDSource: bytes.NewReader(idBytes),
		})
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := orchestrator.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(execErr, &transactionErr) || transactionErr.Recovery == nil ||
		transactionErr.Committed || len(transactionErr.Recovery.Pending) != 1 {
		t.Fatalf("execute error=%T %+v", execErr, transactionErr)
	}
	handle := transactionErr.Recovery
	client.mu.Lock()
	commandsBeforeInvalidRecovery := len(client.commands)
	client.mu.Unlock()
	originalID := handle.ID
	handle.ID[0] ^= 0xff
	if _, invalidErr := orchestrator.Recover(context.Background(), handle); !errors.Is(
		invalidErr, ErrReplicatedTransaction,
	) {
		t.Fatalf("mutated transaction ID error=%v", invalidErr)
	}
	handle.ID = originalID
	originalOrdinal := handle.Pending[0].Ordinal
	handle.Pending[0].Ordinal = uint32((int(originalOrdinal) + 1) % len(handle.Participants))
	if _, invalidErr := orchestrator.Recover(context.Background(), handle); !errors.Is(
		invalidErr, ErrReplicatedTransaction,
	) {
		t.Fatalf("mutated pending ordinal error=%v", invalidErr)
	}
	handle.Pending[0].Ordinal = originalOrdinal
	originalRoute := handle.Pending[0].Route
	handle.Pending[0].Route.Shard += "-injected"
	if _, invalidErr := orchestrator.Recover(context.Background(), handle); !errors.Is(
		invalidErr, ErrReplicatedTransaction,
	) {
		t.Fatalf("mutated pending route error=%v", invalidErr)
	}
	handle.Pending[0].Route = originalRoute
	originalCommand := handle.Pending[0].Command
	handle.Pending[0].Command = bytes.Clone(originalCommand)
	handle.Pending[0].Command[len(handle.Pending[0].Command)-1] ^= 0xff
	if _, invalidErr := orchestrator.Recover(context.Background(), handle); !errors.Is(
		invalidErr, ErrReplicatedTransaction,
	) {
		t.Fatalf("mutated pending command error=%v", invalidErr)
	}
	handle.Pending[0].Command = originalCommand
	client.mu.Lock()
	commandsAfterInvalidRecovery := len(client.commands)
	client.mu.Unlock()
	if commandsAfterInvalidRecovery != commandsBeforeInvalidRecovery {
		t.Fatalf("invalid recovery performed network writes: before=%d after=%d",
			commandsBeforeInvalidRecovery, commandsAfterInvalidRecovery)
	}
	pending := bytes.Clone(handle.Pending[0].Command)
	pendingBacking := handle.Pending[:cap(handle.Pending)]
	result, err := orchestrator.Recover(context.Background(), handle)
	if err != nil || !result.Committed || result.AffectedRows != 65 {
		t.Fatalf("recovery result=%+v err=%v", result, err)
	}
	if len(handle.Pending) != 0 || pendingBacking[0].Command != nil ||
		pendingBacking[0].Route.Replicas != nil || pendingBacking[0].Route.Distribution != "" {
		t.Fatalf("settled pending tail retained owned command/route bytes: %+v", pendingBacking[0])
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	var commits [][]byte
	for _, raw := range client.commands {
		command, openErr := replication.OpenCommand(raw)
		if openErr != nil {
			t.Fatal(openErr)
		}
		control, openErr := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
		if openErr == nil && control.Operation == distributedtxn.ReplicatedCommitCoordinator {
			commits = append(commits, raw)
		}
	}
	if len(commits) != 2 || !bytes.Equal(commits[0], pending) ||
		!bytes.Equal(commits[0], commits[1]) {
		t.Fatalf("commit attempts=%d byte-identical=%v", len(commits),
			len(commits) == 2 && bytes.Equal(commits[0], commits[1]))
	}
}

func TestReplicatedTransactionRecoverRejectsTamperedUnknownBeginBeforeNetwork(t *testing.T) {
	for _, count := range []int{2, 65} {
		t.Run(fmt.Sprintf("participants-%d", count), func(t *testing.T) {
			participants, client := transactionOrchestratorRoutes(t, count)
			if count <= distributedtxn.MaxInlineParticipants {
				client.loseOperation = distributedtxn.ReplicatedBeginPrepareCoordinator
			} else {
				client.loseOperation = distributedtxn.ReplicatedBeginPrepareManifestCoordinator
			}
			client.loseBeforeApply = true
			executor, err := NewReplicatedExecutorWithOptions(client,
				ReplicatedExecutorOptions{
					MaxAttempts: 1, AttemptTimeout: time.Second,
					LeaderHintCapacity: max(16, count*2),
				})
			if err != nil {
				t.Fatal(err)
			}
			orchestrator, err := NewReplicatedTransactionOrchestrator(
				ReplicatedTransactionOrchestratorOptions{
					Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 8,
					MaxInFlightBytes: 64 << 20,
					MaxMutations:     uint64(count), MaxMutationBytes: uint64(count * 64),
					RecoveryTimeout: time.Minute,
					IDSource:        bytes.NewReader(bytes.Repeat([]byte{byte(count + 7)}, 16)),
				})
			if err != nil {
				t.Fatal(err)
			}
			_, executeErr := orchestrator.Execute(context.Background(), 7, participants)
			var transactionErr *ReplicatedTransactionError
			if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil ||
				transactionErr.Recovery.CoordinatorMinimumApplied != 0 ||
				len(transactionErr.Recovery.Pending) != 1 {
				t.Fatalf("execute error=%v transaction=%+v", executeErr, transactionErr)
			}
			handle := transactionErr.Recovery
			client.mu.Lock()
			calls := client.calls
			client.mu.Unlock()
			assertRejected := func(name string) {
				t.Helper()
				if _, recoverErr := orchestrator.Recover(context.Background(), handle); !errors.Is(recoverErr, ErrReplicatedTransaction) {
					t.Fatalf("%s recovery error=%v", name, recoverErr)
				}
				client.mu.Lock()
				gotCalls := client.calls
				client.mu.Unlock()
				if gotCalls != calls {
					t.Fatalf("%s performed network calls: before=%d after=%d",
						name, calls, gotCalls)
				}
			}

			unselected := (int(handle.CoordinatorOrdinal) + 1) % len(handle.Participants)
			originalDigest := handle.Participants[unselected].MutationDigest
			handle.Participants[unselected].MutationDigest[0] ^= 0xff
			assertRejected("unselected participant")
			handle.Participants[unselected].MutationDigest = originalDigest

			handle.CatalogGeneration++
			assertRejected("catalog generation")
			handle.CatalogGeneration--

			handle.RecoveryDeadline++
			assertRejected("recovery deadline")
			handle.RecoveryDeadline--

			originalCommand := handle.Pending[0].Command
			for _, tamper := range []string{
				"unselected participant", "catalog generation", "recovery deadline", "fingerprint",
				"authority class",
			} {
				handle.Pending[0].Command = tamperedReplicatedTransactionBeginCommand(
					t, orchestrator, handle, unselected, tamper,
				)
				assertRejected("canonical pending " + tamper)
				handle.Pending[0].Command = originalCommand
			}

			if err = orchestrator.DiscardRecovery(handle); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func tamperedReplicatedTransactionBeginCommand(
	t *testing.T,
	orchestrator *ReplicatedTransactionOrchestrator,
	handle *ReplicatedTransactionRecoveryHandle,
	unselected int,
	tamper string,
) []byte {
	t.Helper()
	pending := handle.Pending[0]
	outer, err := replication.OpenCommand(pending.Command)
	if err != nil {
		t.Fatal(err)
	}
	controlView, err := distributedtxn.OpenReplicatedCommand(outer.TransactionBytes())
	if err != nil {
		t.Fatal(err)
	}
	control := controlView.Command()
	plan := make([]replicatedTransactionPlanParticipant, len(handle.Participants))
	for ordinal := range handle.Participants {
		plan[ordinal].ref = replicatedTransactionHandleParticipantRef(
			&handle.Participants[ordinal],
		)
	}
	if tamper == "unselected participant" {
		plan[unselected].ref.MutationDigest[0] ^= 0xff
	}
	switch control.Operation {
	case distributedtxn.ReplicatedBeginPrepareCoordinator:
		record, openErr := distributedtxn.OpenCoordinator(control.Payload)
		if openErr != nil {
			t.Fatal(openErr)
		}
		record.Participants = make([]distributedtxn.ParticipantRef, len(plan))
		for ordinal := range plan {
			record.Participants[ordinal] = plan[ordinal].ref
		}
		if tamper == "catalog generation" {
			record.CatalogGeneration++
		}
		if tamper == "recovery deadline" {
			record.RecoveryDeadline++
		}
		control.Payload, err = distributedtxn.AppendCoordinator(nil, record)
	case distributedtxn.ReplicatedBeginPrepareManifestCoordinator:
		coordinator, _, openErr := distributedtxn.OpenReplicatedManifestStart(control.Payload)
		if openErr != nil {
			t.Fatal(openErr)
		}
		record, openErr := distributedtxn.OpenManifestCoordinator(coordinator)
		if openErr != nil {
			t.Fatal(openErr)
		}
		var initial []byte
		record.Manifest, initial, err = replicatedTransactionManifest(plan)
		if err != nil {
			t.Fatal(err)
		}
		if tamper == "catalog generation" {
			record.CatalogGeneration++
		}
		if tamper == "recovery deadline" {
			record.RecoveryDeadline++
		}
		prefix, appendErr := distributedtxn.AppendManifestCoordinator(nil, record)
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		control.Payload = append(prefix, initial...)
	default:
		t.Fatalf("unexpected begin operation %d", control.Operation)
	}
	if err != nil {
		t.Fatal(err)
	}
	controlBytes, err := distributedtxn.AppendReplicatedCommand(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := replication.TransactionClientSequence(controlBytes)
	if err != nil {
		t.Fatal(err)
	}
	batches := make([]replication.RelationMutationBatch, 0, outer.RelationCount())
	relations := outer.RelationBatches()
	for relations.Next() {
		batchView := relations.Batch()
		batch := replication.RelationMutationBatch{Relation: batchView.Relation}
		batch.Mutations = make([]replication.Mutation, 0, batchView.MutationCount())
		mutations := batchView.Mutations()
		for mutations.Next() {
			mutation := mutations.Mutation()
			batch.Mutations = append(batch.Mutations, replication.Mutation{
				Kind: mutation.Kind, Key: mutation.Key, Value: mutation.Value,
				ExpectedValueLength: mutation.ExpectedValueLength,
				ExpectedValueDigest: mutation.ExpectedValueDigest,
			})
		}
		batches = append(batches, batch)
	}
	command := replicatedTransactionCommandHeader(
		pending.Route, orchestrator.tenant, orchestrator.retryHome,
		outer.ClientID, outer.ClientEpoch, sequence,
	)
	command.Kind = replication.CommandTransaction
	command.Transaction = controlBytes
	command.Batches = batches
	if tamper == "authority class" {
		command.AuthorityClass = replication.CommandAuthorityTopology
	}
	command.Fingerprint = nativeCommandFingerprint(command)
	if tamper == "fingerprint" {
		command.Fingerprint[0] ^= 0xff
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil || len(encoded) != len(pending.Command) {
		t.Fatalf("tampered command bytes=%d want=%d err=%v",
			len(encoded), len(pending.Command), err)
	}
	return encoded
}

func replicatedTransactionBudgetSnapshot(
	orchestrator *ReplicatedTransactionOrchestrator,
) (retained, active, retainedPeak, activePeak uint64) {
	orchestrator.byteBudget.mu.Lock()
	retained, retainedPeak = orchestrator.byteBudget.used, orchestrator.byteBudget.peak
	orchestrator.byteBudget.mu.Unlock()
	orchestrator.activeByteBudget.mu.Lock()
	active, activePeak = orchestrator.activeByteBudget.used,
		orchestrator.activeByteBudget.peak
	orchestrator.activeByteBudget.mu.Unlock()
	return retained, active, retainedPeak, activePeak
}

func TestReplicatedTransactionActiveBudgetPreservesRecoveryValidation(t *testing.T) {
	const proposalBytes = uint64(4096)
	var budget replicatedTransactionByteBudget
	budget.reset(proposalBytes + replicatedTransactionRecoveryValidationBytes)
	budget.reserve = replicatedTransactionRecoveryValidationBytes
	if err := budget.acquire(context.Background(), proposalBytes); err != nil {
		t.Fatal(err)
	}
	if budget.tryAcquire(1) {
		t.Fatal("normal admission consumed recovery validation reserve")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := budget.acquireReserved(ctx, replicatedTransactionRecoveryValidationBytes); err != nil {
		t.Fatalf("validation admission behind full proposal partition: %v", err)
	}
	budget.release(replicatedTransactionRecoveryValidationBytes)
	budget.release(proposalBytes)
	if budget.used != 0 {
		t.Fatalf("active budget retained=%d", budget.used)
	}
}

func TestReplicatedTransactionUnknownRetryKeepsAndReleasesExactBudget(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 2)
	client.loseOperation = distributedtxn.ReplicatedBeginPrepareCoordinator
	client.loseBeforeApply = true
	client.loseEveryMatching = true
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 2,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     2, MaxMutationBytes: 128, RecoveryTimeout: time.Minute,
			IDSource: bytes.NewReader(bytes.Repeat([]byte{0x6b}, 16)),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestrator.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	var executeUnknown *raftservice.UnknownOutcomeError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil ||
		!errors.As(executeErr, &executeUnknown) || executeUnknown.Command != nil {
		t.Fatalf("execute error=%v transaction=%+v unknown=%+v",
			executeErr, transactionErr, executeUnknown)
	}
	handle := transactionErr.Recovery
	if handle.ownership == nil || len(handle.ownership.pending) != 1 ||
		len(handle.Pending) != 1 {
		t.Fatalf("ownership=%+v pending=%d", handle.ownership, len(handle.Pending))
	}
	reservation := handle.Pending[0].reservation
	if reservation.bytes != uint64(cap(handle.Pending[0].Command))+
		replicatedTransactionPendingLogicalBytes {
		t.Fatalf("pending reservation=%d command-cap=%d logical=%d",
			reservation.bytes, cap(handle.Pending[0].Command),
			replicatedTransactionPendingLogicalBytes)
	}
	retained, active, retainedPeak, activePeak :=
		replicatedTransactionBudgetSnapshot(orchestrator)
	wantRetained := handle.ownership.handle.bytes
	if retained != wantRetained || active != reservation.bytes ||
		retainedPeak > orchestrator.byteBudget.limit ||
		activePeak > orchestrator.activeByteBudget.limit {
		t.Fatalf("post-return retained=%d/%d peak=%d/%d active=%d/%d peak=%d/%d",
			retained, wantRetained, retainedPeak, orchestrator.byteBudget.limit,
			active, reservation.bytes, activePeak, orchestrator.activeByteBudget.limit)
	}
	pendingPointer := &handle.Pending[0].Command[0]
	_, recoverErr := orchestrator.Recover(context.Background(), handle)
	var retryUnknown *raftservice.UnknownOutcomeError
	if !errors.As(recoverErr, &retryUnknown) || retryUnknown.Command != nil ||
		len(handle.Pending) != 1 || &handle.Pending[0].Command[0] != pendingPointer {
		t.Fatalf("failed retry error=%v unknown=%+v pending=%d",
			recoverErr, retryUnknown, len(handle.Pending))
	}
	retainedAfter, activeAfter, _, _ := replicatedTransactionBudgetSnapshot(orchestrator)
	if retainedAfter != retained || activeAfter != active || reservation.released.Load() {
		t.Fatalf("failed retry changed quota retained=%d/%d active=%d/%d released=%v",
			retainedAfter, retained, activeAfter, active, reservation.released.Load())
	}
	client.mu.Lock()
	client.loseEveryMatching = false
	client.mu.Unlock()
	result, recoverErr := recoverReplicatedTransactionAfterLogicalLease(t, orchestrator, handle)
	if recoverErr != nil || result.Committed || result.AffectedRows != 0 {
		t.Fatalf("recovery result=%+v err=%v", result, recoverErr)
	}
	retainedAfter, activeAfter, _, _ = replicatedTransactionBudgetSnapshot(orchestrator)
	if retainedAfter != 0 || activeAfter != 0 || !reservation.released.Load() ||
		handle.ownership != nil || len(handle.Pending) != 0 || len(handle.Participants) != 0 {
		t.Fatalf("terminal quota retained=%d active=%d released=%v handle=%+v",
			retainedAfter, activeAfter, reservation.released.Load(), handle)
	}
}

func TestReplicatedTransactionDiscardUsesPrivateLeaseRegistry(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 2)
	client.loseOperation = distributedtxn.ReplicatedBeginPrepareCoordinator
	client.loseBeforeApply = true
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 2,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     2, MaxMutationBytes: 128, RecoveryTimeout: time.Minute,
			IDSource: bytes.NewReader(bytes.Repeat([]byte{0x7b}, 16)),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestrator.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil ||
		len(transactionErr.Recovery.Pending) != 1 {
		t.Fatalf("execute error=%v transaction=%+v", executeErr, transactionErr)
	}
	handle := transactionErr.Recovery
	shallow := *handle
	original := handle.Pending[0]
	// Exported recovery material is mutable: duplicate, reorder/replace, and
	// finally drop the visible entry. Quota ownership must remain in the private
	// shared registry rather than following this slice.
	handle.Pending = append(handle.Pending, original)
	handle.Pending[0], handle.Pending[1] = handle.Pending[1], ReplicatedTransactionPendingCommand{}
	handle.Pending = handle.Pending[:0]
	if err = orchestrator.DiscardRecovery(handle); err != nil {
		t.Fatal(err)
	}
	if err = orchestrator.DiscardRecovery(&shallow); err != nil {
		t.Fatal(err)
	}
	if err = orchestrator.DiscardRecovery(handle); err != nil {
		t.Fatal(err)
	}
	retained, active, _, _ := replicatedTransactionBudgetSnapshot(orchestrator)
	if retained != 0 || active != 0 || !original.reservation.released.Load() {
		t.Fatalf("discard quota retained=%d active=%d released=%v",
			retained, active, original.reservation.released.Load())
	}
}

func detachedReplicatedTransactionRecoveryHandle(
	source *ReplicatedTransactionRecoveryHandle,
) *ReplicatedTransactionRecoveryHandle {
	detached := &ReplicatedTransactionRecoveryHandle{
		ID: source.ID, CatalogGeneration: source.CatalogGeneration,
		Phase: source.Phase, CoordinatorOrdinal: source.CoordinatorOrdinal,
		DecisionRevision:          source.DecisionRevision,
		CoordinatorMinimumApplied: source.CoordinatorMinimumApplied,
		RecoveryDeadline:          source.RecoveryDeadline,
		Participants:              make([]ReplicatedTransactionRouteWitness, len(source.Participants)),
		Pending:                   make([]ReplicatedTransactionPendingCommand, len(source.Pending)),
	}
	for index := range source.Participants {
		detached.Participants[index] = source.Participants[index]
		detached.Participants[index].Route = cloneReplicatedTransactionRoute(
			source.Participants[index].Route,
		)
	}
	for index := range source.Pending {
		detached.Pending[index] = ReplicatedTransactionPendingCommand{
			Route:   cloneReplicatedTransactionRoute(source.Pending[index].Route),
			Ordinal: source.Pending[index].Ordinal,
			Command: bytes.Clone(source.Pending[index].Command),
		}
	}
	return detached
}

func TestReplicatedTransactionDetachedHandleRecoversOnAnotherOrchestrator(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 2)
	client.loseOperation = distributedtxn.ReplicatedBeginPrepareCoordinator
	client.loseBeforeApply = true
	executorOne, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	options := ReplicatedTransactionOrchestratorOptions{
		Executor: executorOne, Tenant: []byte("tenant"), MaxConcurrency: 2,
		MaxInFlightBytes: 64 << 20,
		MaxMutations:     2, MaxMutationBytes: 128, RecoveryTimeout: time.Minute,
		IDSource: bytes.NewReader(bytes.Repeat([]byte{0x3f}, 16)),
	}
	orchestratorOne, err := NewReplicatedTransactionOrchestrator(options)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestratorOne.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil {
		t.Fatalf("execute error=%v transaction=%+v", executeErr, transactionErr)
	}
	detached := detachedReplicatedTransactionRecoveryHandle(transactionErr.Recovery)
	if err = orchestratorOne.DiscardRecovery(transactionErr.Recovery); err != nil {
		t.Fatal(err)
	}
	executorTwo, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	options.Executor = executorTwo
	options.MaxConcurrency = 1
	options.IDSource = bytes.NewReader(bytes.Repeat([]byte{0x4f}, 16))
	orchestratorTwo, err := NewReplicatedTransactionOrchestrator(options)
	if err != nil {
		t.Fatal(err)
	}
	result, recoverErr := recoverReplicatedTransactionAfterLogicalLease(t, orchestratorTwo, detached)
	if recoverErr != nil || result.Committed || result.AffectedRows != 0 {
		t.Fatalf("detached recovery result=%+v err=%v", result, recoverErr)
	}
	retained, active, _, _ := replicatedTransactionBudgetSnapshot(orchestratorTwo)
	if retained != 0 || active != 0 || detached.ownership != nil ||
		len(detached.Pending) != 0 || len(detached.Participants) != 0 {
		t.Fatalf("detached terminal retained=%d active=%d handle=%+v",
			retained, active, detached)
	}
}

func TestReplicatedTransactionDetachedAdoptionCanonicalizesOwnedBacking(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 2)
	client.loseOperation = distributedtxn.ReplicatedBeginPrepareCoordinator
	client.loseBeforeApply = true
	client.loseEveryMatching = true
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	options := ReplicatedTransactionOrchestratorOptions{
		Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 2,
		MaxInFlightBytes: 64 << 20,
		MaxMutations:     2, MaxMutationBytes: 128, RecoveryTimeout: time.Minute,
		IDSource: bytes.NewReader(bytes.Repeat([]byte{0x2f}, 16)),
	}
	producer, err := NewReplicatedTransactionOrchestrator(options)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := producer.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil {
		t.Fatalf("execute error=%v transaction=%+v", executeErr, transactionErr)
	}
	detached := detachedReplicatedTransactionRecoveryHandle(transactionErr.Recovery)
	if err = producer.DiscardRecovery(transactionErr.Recovery); err != nil {
		t.Fatal(err)
	}

	wideParticipants := make([]ReplicatedTransactionRouteWitness,
		len(detached.Participants), len(detached.Participants)+(1<<16))
	copy(wideParticipants, detached.Participants)
	detached.Participants = wideParticipants
	for index := range detached.Participants {
		route := &detached.Participants[index].Route
		wideReplicas := make([]ReplicatedEndpoint, len(route.Replicas), len(route.Replicas)+(1<<16))
		copy(wideReplicas, route.Replicas)
		route.Replicas = wideReplicas
		owner := strings.Repeat("x", 1<<20) + string(route.Distribution)
		route.Distribution = distribution.DistributionName(owner[1<<20:])
	}
	widePending := make([]ReplicatedTransactionPendingCommand,
		len(detached.Pending), len(detached.Pending)+(1<<16))
	copy(widePending, detached.Pending)
	detached.Pending = widePending
	detached.Pending[0].Route = cloneReplicatedTransactionRoute(
		detached.Participants[detached.Pending[0].Ordinal].Route,
	)
	commandOwner := make([]byte, len(detached.Pending[0].Command)+(1<<20))
	copy(commandOwner, detached.Pending[0].Command)
	detached.Pending[0].Command = commandOwner[:len(detached.Pending[0].Command):len(detached.Pending[0].Command)]

	options.MaxConcurrency = 1
	consumer, err := NewReplicatedTransactionOrchestrator(options)
	if err != nil {
		t.Fatal(err)
	}
	_, recoverErr := consumer.Recover(context.Background(), detached)
	if recoverErr == nil || detached.ownership == nil {
		t.Fatalf("recovery error=%v handle=%+v", recoverErr, detached)
	}
	if cap(detached.Participants) != len(detached.Participants) ||
		cap(detached.Pending) != len(detached.Pending) ||
		cap(detached.Pending[0].Command) != len(detached.Pending[0].Command) ||
		&detached.Pending[0].Command[0] == &commandOwner[0] {
		t.Fatalf("noncanonical adopted backing participants=%d/%d pending=%d/%d command=%d/%d",
			len(detached.Participants), cap(detached.Participants),
			len(detached.Pending), cap(detached.Pending),
			len(detached.Pending[0].Command), cap(detached.Pending[0].Command))
	}
	for index := range detached.Participants {
		if cap(detached.Participants[index].Route.Replicas) !=
			len(detached.Participants[index].Route.Replicas) {
			t.Fatalf("participant %d replica backing=%d/%d", index,
				len(detached.Participants[index].Route.Replicas),
				cap(detached.Participants[index].Route.Replicas))
		}
	}
	if err = consumer.DiscardRecovery(detached); err != nil {
		t.Fatal(err)
	}
	retained, active, _, _ := replicatedTransactionBudgetSnapshot(consumer)
	if retained != 0 || active != 0 {
		t.Fatalf("discarded canonical quota retained=%d active=%d", retained, active)
	}
}

func TestReplicatedTransactionRecoverSettlesUnknownRetireWithAuthoritativeRows(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 2)
	client.loseOperation = distributedtxn.ReplicatedRetireCoordinator
	client.loseBeforeApply = true
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 2,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     2, MaxMutationBytes: 128, RecoveryTimeout: time.Minute,
			IDSource: bytes.NewReader(bytes.Repeat([]byte{0x5d}, 16)),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestrator.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || !transactionErr.Committed ||
		transactionErr.Recovery == nil || len(transactionErr.Recovery.Pending) != 1 {
		t.Fatalf("unknown retire error=%v transaction=%+v", executeErr, transactionErr)
	}
	pendingBacking := transactionErr.Recovery.Pending[:cap(transactionErr.Recovery.Pending)]
	pending := bytes.Clone(transactionErr.Recovery.Pending[0].Command)
	view, err := replication.OpenCommand(pending)
	if err != nil {
		t.Fatal(err)
	}
	control, err := distributedtxn.OpenReplicatedCommand(view.TransactionBytes())
	if err != nil || control.Operation != distributedtxn.ReplicatedRetireCoordinator {
		t.Fatalf("pending retire=%+v err=%v", control.ReplicatedCommand, err)
	}
	summary, err := distributedtxn.OpenReplicatedRetirementSummary(control.Payload)
	if err != nil || !summary.AffectedRowsValid || summary.AffectedRows != 2 {
		t.Fatalf("pending retirement summary=%+v err=%v", summary, err)
	}
	coordinatorOrdinal := transactionErr.Recovery.CoordinatorOrdinal
	group := transactionErr.Recovery.Participants[coordinatorOrdinal].Route.Group
	client.mu.Lock()
	retired := client.coordinators[group]
	client.mu.Unlock()
	retired.State = uint8(distributedtxn.CoordinatorRetired)
	for _, test := range []struct {
		name     string
		decision distributedtxn.CoordinatorState
		rows     int64
		valid    bool
		wantOK   bool
	}{
		{name: "committed", decision: distributedtxn.CoordinatorCommitted, rows: 2, valid: true, wantOK: true},
		{name: "committed-missing", decision: distributedtxn.CoordinatorCommitted},
		{name: "aborted", decision: distributedtxn.CoordinatorAborted, wantOK: true},
		{name: "aborted-with-rows", decision: distributedtxn.CoordinatorAborted, rows: 2, valid: true},
		{name: "invalid-decision", decision: distributedtxn.CoordinatorInvalid},
	} {
		t.Run("retired-summary-"+test.name, func(t *testing.T) {
			record := retired
			record.CoordinatorDecision = test.decision
			record.AffectedRows, record.AffectedRowsValid = test.rows, test.valid
			err := orchestrator.validateCoordinatorWitnesses(
				context.Background(), transactionErr.Recovery, record,
			)
			if (err == nil) != test.wantOK {
				t.Fatalf("record=%+v err=%v want-ok=%v", record, err, test.wantOK)
			}
		})
	}
	result, err := orchestrator.Recover(context.Background(), transactionErr.Recovery)
	if err != nil || !result.Committed || result.AffectedRows != 2 || result.Recovery != nil {
		t.Fatalf("retire recovery result=%+v err=%v", result, err)
	}
	if len(transactionErr.Recovery.Pending) != 0 || pendingBacking[0].Command != nil ||
		pendingBacking[0].Route.Replicas != nil {
		t.Fatalf("retire pending ownership retained: %+v", pendingBacking[0])
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	var retires [][]byte
	for _, raw := range client.commands {
		command, openErr := replication.OpenCommand(raw)
		if openErr != nil {
			t.Fatal(openErr)
		}
		retirement, openErr := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
		if openErr == nil && retirement.Operation == distributedtxn.ReplicatedRetireCoordinator {
			retires = append(retires, raw)
		}
	}
	if len(retires) != 2 || !bytes.Equal(retires[0], pending) ||
		!bytes.Equal(retires[0], retires[1]) {
		t.Fatalf("retire attempts=%d exact=%v", len(retires),
			len(retires) == 2 && bytes.Equal(retires[0], retires[1]))
	}
}

func TestReplicatedTransactionTerminalProofRequiresEveryFreshRead(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 2)
	client.loseOperation = distributedtxn.ReplicatedRetireCoordinator
	client.loseBeforeApply = true
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 2,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     2, MaxMutationBytes: 128, RecoveryTimeout: time.Minute,
			IDSource: bytes.NewReader(bytes.Repeat([]byte{0x6d}, 16)),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestrator.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil ||
		!transactionErr.Committed {
		t.Fatalf("execute error=%v transaction=%+v", executeErr, transactionErr)
	}
	handle := transactionErr.Recovery
	for index := range handle.Participants {
		handle.Participants[index].Terminal = true
	}
	client.mu.Lock()
	client.failParticipantRead = true
	client.mu.Unlock()
	affected, proofErr := orchestrator.proveTerminalParticipants(
		context.Background(), handle, true,
	)
	if proofErr == nil || affected != 0 {
		t.Fatalf("fresh terminal proof affected=%d err=%v", affected, proofErr)
	}
	for index := range handle.Participants {
		if handle.Participants[index].Terminal {
			t.Fatalf("participant %d trusted exported terminal flag", index)
		}
	}
	if err = orchestrator.DiscardRecovery(handle); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedTransactionRecoverAbortedPartialManifestThenRetires(t *testing.T) {
	const participantCount = 4097
	participants, client := transactionOrchestratorRoutes(t, participantCount)
	client.loseOperation = distributedtxn.ReplicatedCommitCoordinator
	client.loseBeforeApply = true
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: participantCount * 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 32,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     participantCount, MaxMutationBytes: participantCount * 64,
			RecoveryTimeout: time.Minute,
			IDSource:        bytes.NewReader(bytes.Repeat([]byte{0x6e}, 16)),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestrator.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil {
		t.Fatalf("execute error=%v transaction=%+v", executeErr, transactionErr)
	}
	handle := transactionErr.Recovery
	group := handle.Participants[handle.CoordinatorOrdinal].Route.Group
	client.mu.Lock()
	record := client.coordinators[group]
	manifest, openErr := distributedtxn.OpenManifestCoordinator(record.Payload)
	if openErr != nil || manifest.Manifest.SegmentCount < 3 {
		client.mu.Unlock()
		t.Fatalf("manifest=%+v err=%v", manifest.Manifest, openErr)
	}
	// Model an abort committed after exactly two manifest pages were sealed.
	// Suffix pages are absent and must never be requested during recovery.
	record.State = uint8(distributedtxn.CoordinatorAborted)
	record.CoordinatorDecision = distributedtxn.CoordinatorAborted
	record.Revision = 3
	client.coordinators[group] = record
	for page := uint32(2); page < manifest.Manifest.SegmentCount; page++ {
		delete(client.manifestPages[group], page)
	}
	client.mu.Unlock()

	result, err := orchestrator.Recover(context.Background(), handle)
	if err != nil || result.Committed || result.AffectedRows != 0 || result.Recovery != nil {
		t.Fatalf("partial-manifest recovery result=%+v err=%v", result, err)
	}
	client.mu.Lock()
	retired := client.coordinators[group]
	client.mu.Unlock()
	if distributedtxn.CoordinatorState(retired.State) != distributedtxn.CoordinatorRetired ||
		retired.CoordinatorDecision != distributedtxn.CoordinatorAborted ||
		retired.AffectedRowsValid || retired.AffectedRows != 0 {
		t.Fatalf("retired aborted coordinator=%+v", retired)
	}
}

func TestReplicatedCoordinatorManifestRecoveryPagesAreStateExact(t *testing.T) {
	tests := []struct {
		name     string
		state    distributedtxn.CoordinatorState
		revision uint64
		want     uint32
		valid    bool
	}{
		{name: "staging-prefix", state: distributedtxn.CoordinatorStaging, revision: 2, want: 2, valid: true},
		{name: "aborted-excludes-decision", state: distributedtxn.CoordinatorAborted, revision: 3, want: 2, valid: true},
		{name: "committed-full", state: distributedtxn.CoordinatorCommitted, revision: 7, want: 6, valid: true},
		{name: "staging-past-descriptor", state: distributedtxn.CoordinatorStaging, revision: 7},
		{name: "aborted-without-decision", state: distributedtxn.CoordinatorAborted, revision: 0},
		{name: "aborted-past-descriptor", state: distributedtxn.CoordinatorAborted, revision: 8},
		{name: "retired-not-page-backed", state: distributedtxn.CoordinatorRetired, revision: 8},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := replicatedCoordinatorManifestRecoveryPages(test.state, test.revision, 6)
			if (err == nil) != test.valid || got != test.want {
				t.Fatalf("pages=%d err=%v want=%d valid=%v", got, err, test.want, test.valid)
			}
		})
	}
}

func TestReplicatedTransactionRecoverReadsStagingBeforeUnknownCommitRetry(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 2)
	client.loseOperation = distributedtxn.ReplicatedCommitCoordinator
	client.loseBeforeApply = true
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 2,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     2, MaxMutationBytes: 128, RecoveryTimeout: time.Minute,
			IDSource: bytes.NewReader(bytes.Repeat([]byte{0x6d}, 16)),
		})
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestrator.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil ||
		len(transactionErr.Recovery.Pending) != 1 {
		t.Fatalf("execute error=%v", executeErr)
	}
	result, recoverErr := recoverReplicatedTransactionAfterLogicalLease(
		t, orchestrator, transactionErr.Recovery,
	)
	if recoverErr != nil || result.Committed || result.Recovery != nil {
		t.Fatalf("recover result=%+v err=%v", result, recoverErr)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	commits := 0
	for _, raw := range client.commands {
		command, openErr := replication.OpenCommand(raw)
		if openErr != nil {
			t.Fatal(openErr)
		}
		control, openErr := distributedtxn.OpenReplicatedCommand(command.TransactionBytes())
		if openErr == nil && control.Operation == distributedtxn.ReplicatedCommitCoordinator {
			commits++
		}
	}
	if commits != 1 {
		t.Fatalf("unknown commit was retried before authority read: attempts=%d", commits)
	}
}

func TestReplicatedTransactionRecoveryRejectsRouteAuthorityTamperingBeforeNetwork(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*ReplicatedRoute)
	}{
		{"cluster-id", func(route *ReplicatedRoute) { route.Group.ClusterID[15] ^= 0x80 }},
		{"cluster-incarnation", func(route *ReplicatedRoute) {
			route.Group.ClusterIncarnation[15] ^= 0x80
		}},
		{"topology-recovery", func(route *ReplicatedRoute) {
			route.Group.TopologyRecoveryEpoch++
		}},
		{"shard-incarnation", func(route *ReplicatedRoute) {
			route.Group.ShardIncarnation[15] ^= 0x80
		}},
		{"group-id", func(route *ReplicatedRoute) { route.Group.GroupID[15] ^= 0x80 }},
		{"allocation", func(route *ReplicatedRoute) { route.AllocationGeneration++ }},
		{"replica-set", func(route *ReplicatedRoute) { route.Command.ReplicaSetVersion++ }},
		{"policy", func(route *ReplicatedRoute) { route.Command.ActivePolicyGeneration++ }},
		{"protection", func(route *ReplicatedRoute) { route.Command.ProtectionEpoch++ }},
		{"ownership", func(route *ReplicatedRoute) { route.Command.OwnershipEpoch++ }},
		{"schema", func(route *ReplicatedRoute) { route.Command.SchemaGeneration++ }},
		{"relation-manifest", func(route *ReplicatedRoute) {
			route.Command.RelationManifestDigest[31] ^= 0x80
		}},
		{"routing", func(route *ReplicatedRoute) { route.Command.RoutingVersion++ }},
		{"route-generation", func(route *ReplicatedRoute) { route.Command.RouteGeneration++ }},
	}
	for _, participantsCount := range []int{2, 65} {
		t.Run(fmt.Sprintf("participants-%d", participantsCount), func(t *testing.T) {
			participants, client := transactionOrchestratorRoutes(t, participantsCount)
			client.loseOperation = distributedtxn.ReplicatedCommitCoordinator
			executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
				MaxAttempts: 1, AttemptTimeout: time.Second,
				LeaderHintCapacity: max(16, participantsCount*2),
			})
			if err != nil {
				t.Fatal(err)
			}
			orchestrator, err := NewReplicatedTransactionOrchestrator(
				ReplicatedTransactionOrchestratorOptions{
					Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 8,
					MaxInFlightBytes: 64 << 20,
					MaxMutations:     uint64(participantsCount),
					MaxMutationBytes: uint64(participantsCount * 64),
					RecoveryTimeout:  time.Minute,
					IDSource:         bytes.NewReader(bytes.Repeat([]byte{byte(participantsCount)}, 16)),
				})
			if err != nil {
				t.Fatal(err)
			}
			_, executeErr := orchestrator.Execute(context.Background(), 7, participants)
			var transactionErr *ReplicatedTransactionError
			if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil {
				t.Fatalf("execute error=%v", executeErr)
			}
			handle := transactionErr.Recovery
			ordinal := 0
			if uint32(ordinal) == handle.CoordinatorOrdinal {
				ordinal++
			}
			for _, mutation := range mutations {
				t.Run(mutation.name, func(t *testing.T) {
					original := handle.Participants[ordinal].Route
					mutation.mutate(&handle.Participants[ordinal].Route)
					client.mu.Lock()
					before := client.calls
					client.mu.Unlock()
					_, recoverErr := orchestrator.Recover(context.Background(), handle)
					handle.Participants[ordinal].Route = original
					client.mu.Lock()
					after := client.calls
					client.mu.Unlock()
					if !errors.Is(recoverErr, ErrReplicatedTransaction) || after != before {
						t.Fatalf("error=%v network-calls=%d->%d", recoverErr, before, after)
					}
				})
			}
		})
	}
}

func TestReplicatedTransactionAbortFenceWinsBothPrepareRaceOrders(t *testing.T) {
	for _, before := range []bool{false, true} {
		name := "prepare-first"
		if before {
			name = "cancellation-first"
		}
		t.Run(name, func(t *testing.T) {
			participants, client := transactionOrchestratorRoutes(t, 3)
			idBytes := bytes.Repeat([]byte{0x53}, 16)
			var id distributedtxn.ID
			copy(id[:], idBytes)
			coordinator := replicatedTransactionCoordinatorOrdinal(id, len(participants))
			remotes := make([]int, 0, 2)
			for ordinal := range participants {
				if ordinal != coordinator {
					remotes = append(remotes, ordinal)
				}
			}
			client.loseOperation = distributedtxn.ReplicatedStagePrepareParticipant
			client.loseShard = string(participants[remotes[0]].Route.Shard)
			client.loseBeforeApply = before
			client.indexConflictShard = string(participants[remotes[1]].Route.Shard)
			executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
				MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 16,
			})
			if err != nil {
				t.Fatal(err)
			}
			orchestrator, err := NewReplicatedTransactionOrchestrator(
				ReplicatedTransactionOrchestratorOptions{
					Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 3,
					MaxInFlightBytes: 64 << 20,
					MaxMutations:     3, MaxMutationBytes: 192, RecoveryTimeout: time.Minute,
					IDSource: bytes.NewReader(idBytes),
				})
			if err != nil {
				t.Fatal(err)
			}
			result, err := orchestrator.Execute(context.Background(), 7, participants)
			var transactionErr *ReplicatedTransactionError
			if err == nil || result.Committed || result.Recovery != nil ||
				errors.As(err, &transactionErr) && transactionErr.Recovery != nil {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			client.mu.Lock()
			defer client.mu.Unlock()
			for ordinal := range participants {
				record := client.participants[participants[ordinal].Route.Group]
				if record.ID != id ||
					distributedtxn.ParticipantState(record.State) != distributedtxn.ParticipantReleased {
					t.Fatalf("participant %d record=%+v", ordinal, record)
				}
				if ordinal == remotes[0] &&
					(record.CancellationWitness != before ||
						before && record.ParticipantOrdinal != uint32(ordinal)) {
					t.Fatalf("participant %d cancellation record=%+v before=%v",
						ordinal, record, before)
				}
			}
		})
	}
}

func TestReplicatedTransactionAbortUnknownRetentionIsWorkerBounded(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 65)
	idBytes := bytes.Repeat([]byte{0x41}, 16)
	var id distributedtxn.ID
	copy(id[:], idBytes)
	coordinator := replicatedTransactionCoordinatorOrdinal(id, len(participants))
	conflict := 0
	if conflict == coordinator {
		conflict++
	}
	client.indexConflictShard = string(participants[conflict].Route.Shard)
	client.loseOperation = distributedtxn.ReplicatedAbortReleaseParticipant
	client.loseBeforeApply = true
	client.loseEveryMatching = true
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	const workers = 4
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: workers,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     65, MaxMutationBytes: 65 * 64, RecoveryTimeout: time.Minute,
			IDSource: bytes.NewReader(idBytes),
		})
	if err != nil {
		t.Fatal(err)
	}
	_, executeErr := orchestrator.Execute(context.Background(), 7, participants)
	var transactionErr *ReplicatedTransactionError
	if !errors.As(executeErr, &transactionErr) || transactionErr.Recovery == nil ||
		transactionErr.Committed {
		t.Fatalf("execute error=%v transaction=%+v", executeErr, transactionErr)
	}
	handle := transactionErr.Recovery
	for ordinal := 0; len(handle.Pending) < workers && ordinal < len(handle.Participants); ordinal++ {
		alreadyPending := false
		for index := range handle.Pending {
			alreadyPending = alreadyPending || int(handle.Pending[index].Ordinal) == ordinal
		}
		if alreadyPending {
			continue
		}
		coordinatorRoute := handle.Participants[handle.CoordinatorOrdinal].Route
		control := distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleParticipant,
			Operation: distributedtxn.ReplicatedAbortReleaseParticipant,
			ID:        handle.ID, ExpectedRevision: 0,
			PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
			Participant: distributedtxn.ParticipantStage{
				CoordinatorGroup: distributedtxn.ID(coordinatorRoute.Group.GroupID),
				CoordinatorShardIncarnation: distributedtxn.ID(
					coordinatorRoute.Group.ShardIncarnation,
				),
				CoordinatorAllocation: coordinatorRoute.AllocationGeneration,
				MutationDigest:        handle.Participants[ordinal].MutationDigest,
				ParticipantOrdinal:    uint32(ordinal),
			},
		}
		proposal := orchestrator.propose(context.Background(),
			handle.Participants[ordinal].Route, control, nil, uint32(ordinal),
			replicatedTransactionWorkerScratch{})
		if proposal.pending == nil {
			t.Fatalf("ordinal %d did not retain injected unknown: code=%d err=%v",
				ordinal, proposal.code, proposal.err)
		}
		orchestrator.capturePending(handle, proposal)
	}
	pending := transactionErr.Recovery.Pending
	if len(pending) <= 2*1+1 || len(pending) > workers {
		t.Fatalf("pending commands=%d workers=%d", len(pending), workers)
	}
	totalBytes := 0
	for index := range pending {
		totalBytes += len(pending[index].Command)
		if !pendingReplicatedTransactionOperation(
			pending[index], distributedtxn.ReplicatedAbortReleaseParticipant,
		) {
			t.Fatalf("pending %d is not exact abort fence/release", index)
		}
	}
	if totalBytes > workers*replication.MaxCommandBytes {
		t.Fatalf("pending bytes=%d worker envelope=%d", totalBytes,
			workers*replication.MaxCommandBytes)
	}
	detached := detachedReplicatedTransactionRecoveryHandle(transactionErr.Recovery)
	if err = orchestrator.DiscardRecovery(transactionErr.Recovery); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	client.loseEveryMatching = false
	client.mu.Unlock()
	recoveryExecutor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: time.Second, LeaderHintCapacity: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: recoveryExecutor, Tenant: []byte("tenant"), MaxConcurrency: 1,
			MaxInFlightBytes: 64 << 20,
			MaxMutations:     65, MaxMutationBytes: 65 * 64, RecoveryTimeout: time.Minute,
			IDSource: bytes.NewReader(bytes.Repeat([]byte{0x42}, 16)),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	recovered, recoverErr := replacement.Recover(context.Background(), detached)
	if recoverErr != nil || recovered.Committed || recovered.AffectedRows != 0 {
		t.Fatalf("C=1 replacement result=%+v err=%v pending=%d",
			recovered, recoverErr, len(pending))
	}
}

func TestReplicatedTransactionWaveUsesWorkerBoundedResultWindow(t *testing.T) {
	orchestrator := &ReplicatedTransactionOrchestrator{
		maxConcurrency: 4, maxWorkerRetainedBytes: 8 << 10,
	}
	orchestrator.byteBudget.reset(32 << 10)
	var active atomic.Int64
	var peak atomic.Int64
	var commandGrowths atomic.Int64
	consumed := 0
	count := orchestrator.runWave(context.Background(), 4097, -1, false,
		func(_ context.Context, ordinal int, scratch replicatedTransactionWorkerScratch) replicatedTransactionProposal {
			if cap(scratch.command) < 4096 {
				commandGrowths.Add(1)
				scratch.command = make([]byte, 0, 4096)
			}
			current := active.Add(1)
			for {
				prior := peak.Load()
				if current <= prior || peak.CompareAndSwap(prior, current) {
					break
				}
			}
			active.Add(-1)
			return replicatedTransactionProposal{
				ordinal: uint32(ordinal), code: replicatedstate.ResultApplied, scratch: scratch,
			}
		}, func(replicatedTransactionProposal) { consumed++ })
	if count != 4097 || consumed != 4097 || peak.Load() > 4 || commandGrowths.Load() > 4 {
		t.Fatalf("count=%d consumed=%d peak=%d command-growths=%d",
			count, consumed, peak.Load(), commandGrowths.Load())
	}
}

func TestReplicatedTransactionConcurrentWavesShareScratchByteBudget(t *testing.T) {
	const limit = uint64(64 << 10)
	orchestrator := &ReplicatedTransactionOrchestrator{
		maxConcurrency: 8, maxWorkerRetainedBytes: 8 << 10,
	}
	orchestrator.byteBudget.reset(limit)
	var peak atomic.Uint64
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			got := orchestrator.runWave(context.Background(), 4097, -1, false,
				func(ctx context.Context, ordinal int, scratch replicatedTransactionWorkerScratch) replicatedTransactionProposal {
					_ = ctx
					if cap(scratch.control) < 512 {
						scratch.control = make([]byte, 0, 512)
					}
					if cap(scratch.command) < 4096 {
						scratch.command = make([]byte, 0, 4096)
					}
					orchestrator.byteBudget.mu.Lock()
					used := orchestrator.byteBudget.used
					orchestrator.byteBudget.mu.Unlock()
					for prior := peak.Load(); used > prior && !peak.CompareAndSwap(prior, used); prior = peak.Load() {
					}
					return replicatedTransactionProposal{
						ordinal: uint32(ordinal), code: replicatedstate.ResultApplied, scratch: scratch,
					}
				}, func(proposal replicatedTransactionProposal) {
					if proposal.err != nil {
						t.Errorf("wave proposal: %v", proposal.err)
					}
				})
			if got != 4097 {
				t.Errorf("wave results=%d", got)
			}
		}()
	}
	wait.Wait()
	orchestrator.byteBudget.mu.Lock()
	used := orchestrator.byteBudget.used
	orchestrator.byteBudget.mu.Unlock()
	if peak.Load() > limit || used != 0 {
		t.Fatalf("peak=%d limit=%d retained-after-waves=%d", peak.Load(), limit, used)
	}
}

func TestReplicatedTransactionTightBudgetLargeProposalsDoNotDeadlock(t *testing.T) {
	participants, client := transactionOrchestratorRoutes(t, 2)
	for ordinal := range participants {
		participants[ordinal].Batches[0].Mutations[0].Value =
			bytes.Repeat([]byte{byte(ordinal + 1)}, replication.MaxMutationValueBytes)
	}
	executor, err := NewReplicatedExecutorWithOptions(client, ReplicatedExecutorOptions{
		MaxAttempts: 1, AttemptTimeout: 5 * time.Second, LeaderHintCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	activeBytes := uint64(replication.MaxCommandBytes) +
		uint64(distributedtxn.MaxReplicatedCommandBytes) +
		replicatedTransactionPendingLogicalBytes +
		replicatedTransactionRecoveryValidationBytes
	orchestrator, err := NewReplicatedTransactionOrchestrator(
		ReplicatedTransactionOrchestratorOptions{
			Executor: executor, Tenant: []byte("tenant"), MaxConcurrency: 2,
			MaxInFlightBytes: activeBytes,
			MaxMutations:     2, MaxMutationBytes: 2*replication.MaxMutationValueBytes + 1024,
			RecoveryTimeout: time.Minute,
		})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := orchestrator.plan(participants)
	if err != nil {
		t.Fatal(err)
	}
	id := distributedtxn.ID{1}
	coordinator := replicatedTransactionCoordinatorOrdinal(id, len(plan))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan replicatedTransactionProposal, len(plan))
	for ordinal := range plan {
		go func() {
			control := distributedtxn.ReplicatedCommand{
				Role:      distributedtxn.ReplicatedRoleParticipant,
				Operation: distributedtxn.ReplicatedStagePrepareParticipant,
				ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
				Participant: orchestrator.participantStage(
					plan[ordinal], *plan[coordinator].route, uint32(ordinal),
				),
			}
			results <- orchestrator.propose(ctx, *plan[ordinal].route, control,
				plan[ordinal].batches, uint32(ordinal), replicatedTransactionWorkerScratch{})
		}()
	}
	for range plan {
		select {
		case proposal := <-results:
			if proposal.err != nil || proposal.code != replicatedstate.ResultApplied {
				t.Fatalf("proposal code=%d err=%v", proposal.code, proposal.err)
			}
		case <-ctx.Done():
			t.Fatal("tight byte budget deadlocked concurrent large proposals")
		}
	}
}

func BenchmarkReplicatedTransactionWaveScratchReuse(b *testing.B) {
	for _, participants := range []int{65, 4097} {
		b.Run(fmt.Sprintf("participants-%d", participants), func(b *testing.B) {
			orchestrator := &ReplicatedTransactionOrchestrator{
				maxConcurrency: 8, maxWorkerRetainedBytes: 8 << 10,
			}
			orchestrator.byteBudget.reset(64 << 10)
			b.ReportAllocs()
			for range b.N {
				orchestrator.runWave(context.Background(), participants, -1, false,
					func(ctx context.Context, ordinal int, scratch replicatedTransactionWorkerScratch) replicatedTransactionProposal {
						_ = ctx
						if cap(scratch.control) < 512 {
							scratch.control = make([]byte, 0, 512)
						}
						if cap(scratch.command) < 4096 {
							scratch.command = make([]byte, 0, 4096)
						}
						return replicatedTransactionProposal{ordinal: uint32(ordinal), scratch: scratch}
					}, func(replicatedTransactionProposal) {})
			}
		})
	}
}

func TestReplicatedTransactionIDZeroSourceIsBounded(t *testing.T) {
	if _, err := newReplicatedTransactionID(bytes.NewReader(make([]byte,
		16*maxReplicatedTransactionIDAttempts))); !errors.Is(err, ErrReplicatedTransaction) {
		t.Fatalf("zero entropy error=%v", err)
	}
}
