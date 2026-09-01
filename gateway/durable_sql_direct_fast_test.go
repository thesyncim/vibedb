package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type directSQLProposalClient struct {
	t             testing.TB
	route         ReplicatedRoute
	applied       uint64
	proposals     int
	exact         []byte
	completion    []byte
	originalApply uint64
}

func (client *directSQLProposalClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	state := shardservice.ReplicatedMemberState{
		Fence: shardservice.ReplicatedFence{
			Group: client.route.Group, AllocationGeneration: client.route.AllocationGeneration,
			Command: client.route.Command, MemberID: endpoint.Member, StoreID: endpoint.StoreID,
			NodeIncarnation: endpoint.NodeIncarnation, Term: 1,
		},
		LeaderID: client.route.Replicas[0].Member,
		Commit:   client.applied, Applied: client.applied, CheckpointApplied: client.applied,
	}
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	view, err := replication.OpenCommand(request.Command)
	if err != nil {
		return nil, err
	}
	control, err := distributedtxn.OpenReplicatedCommand(view.TransactionBytes())
	if err != nil || control.Operation != distributedtxn.ReplicatedApplySingleParticipant {
		client.t.Fatalf("direct SQL command operation=%d err=%v", control.Operation, err)
	}
	client.proposals++
	client.applied++
	state.Commit, state.Applied, state.CheckpointApplied = client.applied, client.applied, client.applied
	if len(client.exact) == 0 {
		client.exact = bytes.Clone(request.Command)
		client.originalApply = client.applied
		var result [24]byte
		result[0] = byte(distributedtxn.ReplicatedRoleParticipant)
		result[1] = byte(distributedtxn.ReplicatedApplySingleParticipant)
		result[2] = 3 // control revision and affected rows are present.
		binary.LittleEndian.PutUint64(result[8:16], control.ExpectedRevision)
		binary.LittleEndian.PutUint64(result[16:24], 1)
		client.completion = bytes.Clone(appendNativeTransactionCompletion(
			client.t, view, replicatedstate.ResultApplied, result[:],
		).Bytes())
	} else if !bytes.Equal(client.exact, request.Command) {
		client.t.Fatal("direct SQL retry changed canonical command bytes")
	}
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedCompletion, HasState: true, State: state,
		RequestDigest: replicatedRequestDigest(request.Command), Completion: bytes.Clone(client.completion),
		Outcome: raftserve.Outcome{
			Code: raftserve.OutcomeCompletion, AppliedIndex: client.applied,
			CompletionAppliedSequence: client.originalApply, CompletionBytes: len(client.completion),
		},
	}, nil
}

func TestDurableSQLSingleParticipantFastPathSkipsLedgerAndReplaysExactly(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute("data", "all", replicas[:0])
	if !ok {
		t.Fatal("missing direct SQL route")
	}
	client := &directSQLProposalClient{t: t, route: route, applied: 1}
	data, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ledger := new(typedServiceLedger)
	pins := new(typedServicePinStop)
	topology := durableFaultTopology(t, durableFaultParticipants(t))
	current := topology.Current()
	current.Generation = snapshot.Generation()
	if err = topology.Publish(*current); err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(topology, ledger, typedServiceRunnerStop{}, pins)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewDurableSQLRequestExecutor(DurableSQLRequestExecutorOptions{
		Planner: planner, ReplicatedData: data, Requests: service,
		RecoveryPulseLimit: 3, PlanningLeaseSpan: 64, SingleParticipantFastPath: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenant := []byte("direct-sql-tenant")
	requestKey := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{0x41},
		Request: requestledger.RequestID{0x42}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		IssuerEpoch: 7, IssuerLane: requestledger.IssuerLane{0x43}, IssuerSequence: 9,
	}
	queries := []Query{{
		SQL: `INSERT INTO messages VALUES (?)`, Class: ClassInteractive,
		Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"message-1","n":1}`)},
	}}
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := executor.Execute(ctx, requestKey, tenant, queries)
	if err != nil || !first.Direct || first.Result == nil || first.Result.RowsAffected != 1 ||
		first.TerminalRevision != 0 || first.ResultDigest != (replication.Digest{}) ||
		first.AckToken != (DurableRequestAckToken{}) || ledger.applies != 0 ||
		ledger.reads != 0 || pins.called != 0 || client.proposals != 1 {
		t.Fatalf("direct SQL first=%+v ledger=%+v pins=%d proposals=%d err=%v",
			first, ledger, pins.called, client.proposals, err)
	}
	replayed, found, err := executor.ReplayRequestWithTenant(ctx, requestKey, tenant, queries)
	if err != nil || !found || !replayed.Direct || replayed.Result == nil ||
		replayed.Result.TransactionID != first.Result.TransactionID ||
		replayed.Result.RowsAffected != first.Result.RowsAffected || ledger.applies != 0 ||
		pins.called != 0 || client.proposals != 2 {
		t.Fatalf("direct SQL replay=%+v found=%v pins=%d proposals=%d err=%v",
			replayed, found, pins.called, client.proposals, err)
	}
}
