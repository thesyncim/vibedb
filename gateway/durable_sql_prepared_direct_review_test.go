package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
)

type reviewedPreparedRead struct {
	operation shardservice.ReplicatedOperation
	address   string
}

type reviewedPreparedReadClient struct {
	delegate           *replicatedSQLIndexedReadClient
	stale              []byte
	fresh              []byte
	calls              []reviewedPreparedRead
	afterCommittedRead func()
}

func (client *reviewedPreparedReadClient) DoReplicated(
	ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	client.calls = append(client.calls, reviewedPreparedRead{operation: request.Operation, address: endpoint.Address})
	if request.Capability != serviceauthz.CapabilityDataRead || request.Authority != (serviceauthz.Authority{Node: [16]byte{7}, Generation: 1}) {
		return nil, ErrReplicatedUnauthorized
	}
	response, err := client.delegate.DoReplicated(ctx, endpoint, request)
	if err != nil || request.Operation == shardservice.ReplicatedProbe {
		return response, err
	}
	value := client.fresh
	if request.Operation == shardservice.ReplicatedReadFollower {
		value = client.stale
	}
	response.Kind = shardservice.ReplicatedReadFound
	if value == nil {
		response.Kind = shardservice.ReplicatedReadMissing
	}
	response.Value = bytes.Clone(value)
	if request.Operation == shardservice.ReplicatedReadFollower && client.afterCommittedRead != nil {
		client.afterCommittedRead()
	}
	return response, nil
}

func reviewedPreparedFixture(t *testing.T, stale, fresh []byte, indexed bool) (*DurableSQLRequestExecutor, *reviewedPreparedReadClient, context.Context) {
	t.Helper()
	snapshot, planner := replicatedSQLTransactionFixture(t, true, indexed)
	delegate, _ := attachReplicatedSQLIndexedReadClient(t, snapshot, fresh)
	client := &reviewedPreparedReadClient{delegate: delegate, stale: stale, fresh: fresh}
	data, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	return &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}, client, ctx
}

func TestPreparedDirectReviewStaleEvaluationErrorFallsBackToFreshRow(t *testing.T) {
	for _, test := range []struct {
		name, sql, stale, fresh, result string
	}{
		{"division", `UPDATE messages SET n=42/n WHERE id='message-1'`, `{"id":"message-1","n":0}`, `{"id":"message-1","n":2}`, `{"id":"message-1","n":21}`},
		{"type", `UPDATE messages SET n=n+1 WHERE id='message-1'`, `{"id":"message-1","n":"bad"}`, `{"id":"message-1","n":41}`, `{"id":"message-1","n":42}`},
		{"cast", `UPDATE messages SET n=CAST(n AS NUMERIC)+1 WHERE id='message-1'`, `{"id":"message-1","n":"bad"}`, `{"id":"message-1","n":"41"}`, `{"id":"message-1","n":42}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor, client, ctx := reviewedPreparedFixture(t, []byte(test.stale), []byte(test.fresh), false)
			tenant := []byte("reviewed-fallback")
			queries := []Query{{SQL: test.sql}}
			plan, err := executor.PrepareDirect(ctx, preparedDirectTestKey(tenant), tenant, queries)
			if err != nil || plan == nil {
				t.Fatalf("stale error escaped instead of evaluating fresh row: %v", err)
			}
			var operations []shardservice.ReplicatedOperation
			for _, call := range client.calls {
				if call.operation != shardservice.ReplicatedProbe {
					operations = append(operations, call.operation)
				}
			}
			if len(operations) != 2 || operations[0] != shardservice.ReplicatedReadFollower || operations[1] != shardservice.ReplicatedReadLeader {
				t.Fatalf("fallback operations=%v", operations)
			}
			mutation := plan.Target.Batches[0].Mutations[0]
			if mutation.Kind != replication.MutationPutDigestEqual || mutation.ExpectedValueLength != uint64(len(test.fresh)) ||
				mutation.ExpectedValueDigest != replication.Digest(sha256.Sum256([]byte(test.fresh))) || string(mutation.Value) != test.result {
				t.Fatalf("recipe did not retain exact fresh evaluation and guard: %+v", mutation)
			}
		})
	}
}

func TestPreparedDirectReviewPersistentEvaluationErrorRetainsLinearizableError(t *testing.T) {
	row := []byte(`{"id":"message-1","n":0}`)
	executor, client, ctx := reviewedPreparedFixture(t, row, row, false)
	queries := []Query{{SQL: `UPDATE messages SET n=42/n WHERE id='message-1'`}}
	_, _, expected := executor.planner.planReplicatedSQLTransactionWithData(ctx, executor.planner.catalog.Current(), queries, executor.planner.profileFor(ClassInteractive), executor.data)
	if expected == nil {
		t.Fatal("fixture must fail linearizable expression evaluation")
	}
	client.calls = nil
	tenant := []byte("reviewed-error")
	plan, err := executor.PrepareDirect(ctx, preparedDirectTestKey(tenant), tenant, queries)
	if plan != nil || !errors.Is(err, ErrDurableSQLNotAdmitted) || errors.Is(err, errPreparedDirectFallback) {
		t.Fatalf("stale fallback escaped or admitted an error: plan=%v err=%v", plan, err)
	}
	var expectedZero, actualZero *query.ScalarDivisionByZeroError
	if !errors.As(expected, &expectedZero) || !errors.As(err, &actualZero) || expectedZero.Pos != actualZero.Pos {
		t.Fatalf("original linearizable error changed: got %v, want %v", err, expected)
	}
	if len(client.calls) != 2 || client.calls[0].operation != shardservice.ReplicatedReadFollower || client.calls[1].operation != shardservice.ReplicatedReadLeader {
		t.Fatalf("cached fallback did not read both cuts: %+v", client.calls)
	}
}

func TestPreparedDirectReviewCancelledEvaluationDoesNotExposeStaleError(t *testing.T) {
	executor, client, ctx := reviewedPreparedFixture(t, []byte(`{"id":"message-1","n":0}`), []byte(`{"id":"message-1","n":2}`), false)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	client.afterCommittedRead = cancel
	tenant := []byte("reviewed-cancel")
	plan, err := executor.PrepareDirect(ctx, preparedDirectTestKey(tenant), tenant, []Query{{SQL: `UPDATE messages SET n=42/n WHERE id='message-1'`}})
	if plan != nil || !errors.Is(err, context.Canceled) || errors.Is(err, query.ErrScalarDivisionByZero) {
		t.Fatalf("cancelled speculative evaluation exposed a stale SQL error: plan=%v err=%v", plan, err)
	}
}

func TestPreparedDirectReviewUsesCachedLeaderWithoutChangingPublicReads(t *testing.T) {
	row := []byte(`{"id":"message-1","n":41}`)
	executor, client, ctx := reviewedPreparedFixture(t, row, row, false)
	var leaderAddress string
	for address, state := range client.delegate.states {
		state.LeaderID = 2
		client.delegate.states[address] = state
		if state.Fence.MemberID == 2 {
			leaderAddress = address
		}
	}
	queries := []Query{{SQL: `UPDATE messages SET n=n+1 WHERE id='message-1'`}}
	targets, handled, err := executor.planner.planReplicatedSQLTransactionWithData(ctx, executor.planner.catalog.Current(), queries, executor.planner.profileFor(ClassInteractive), executor.data)
	if err != nil || !handled || len(targets) != 1 || leaderAddress == "" {
		t.Fatalf("linearizable warmup failed: %v", err)
	}
	client.calls = nil
	tenant := []byte("reviewed-affinity")
	plan, err := executor.PrepareDirect(ctx, preparedDirectTestKey(tenant), tenant, queries)
	if err != nil || plan == nil || len(client.calls) != 1 || client.calls[0].operation != shardservice.ReplicatedReadFollower || client.calls[0].address != leaderAddress {
		t.Fatalf("private read lost cached leader affinity: calls=%+v err=%v", client.calls, err)
	}
	for _, linearizable := range []bool{true, false} {
		client.calls = nil
		_, err = executor.data.ReadPoint(ctx, plan.Target.Route, ReplicatedPointRead{
			Relation: plan.Target.Batches[0].Relation, Key: plan.Target.Batches[0].Mutations[0].Key,
			MinimumApplied: 1, MaxValueBytes: 4 << 20, Linearizable: linearizable,
		})
		if err != nil || len(client.calls) == 0 {
			t.Fatalf("public read failed: %v", err)
		}
		last := client.calls[len(client.calls)-1]
		if linearizable && (last.operation != shardservice.ReplicatedReadLeader || last.address != leaderAddress) {
			t.Fatalf("public linearizable read weakened: %+v", client.calls)
		}
		if !linearizable && (last.operation != shardservice.ReplicatedReadFollower || last.address == leaderAddress || client.calls[0].operation != shardservice.ReplicatedProbe) {
			t.Fatalf("public follower selection changed: %+v", client.calls)
		}
	}
}

func TestPreparedDirectReviewIndexedAndCoordinatedLoweringStayLinearizable(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		row := []byte(`{"id":"message-1","email":"old@example.test","n":41}`)
		executor, client, ctx := reviewedPreparedFixture(t, row, row, indexed)
		queries := []Query{{SQL: `UPDATE messages SET email=email || '.invalid', n=n+1 WHERE id='message-1'`}}
		if indexed {
			tenant := []byte("reviewed-indexed")
			if plan, err := executor.PrepareDirect(ctx, preparedDirectTestKey(tenant), tenant, queries); plan != nil || !errors.Is(err, ErrDurableSQLDirectIneligible) {
				t.Fatalf("indexed update entered private direct path: %v", err)
			}
		} else {
			if _, handled, err := executor.planner.planReplicatedSQLTransactionWithData(ctx, executor.planner.catalog.Current(), queries, executor.planner.profileFor(ClassInteractive), executor.data); err != nil || !handled {
				t.Fatalf("coordinated lowering failed: %v", err)
			}
		}
		reads := 0
		for _, call := range client.calls {
			if call.operation == shardservice.ReplicatedReadFollower {
				t.Fatalf("unrelated lowering used committed read: indexed=%t calls=%+v", indexed, client.calls)
			}
			if call.operation == shardservice.ReplicatedReadLeader {
				reads++
			}
		}
		if reads != 1 {
			t.Fatalf("original lowering performed duplicate reads: indexed=%t reads=%d", indexed, reads)
		}
	}
}

func TestPreparedDirectReviewRequiresNonzeroGuardFields(t *testing.T) {
	queries := []Query{{SQL: `UPDATE messages SET n=n+1 WHERE id='message-1'`}}
	for _, mutation := range []replication.Mutation{
		{Kind: replication.MutationPutDigestEqual, ExpectedValueLength: 10},
		{Kind: replication.MutationPutDigestEqual, ExpectedValueDigest: replication.Digest{1}},
		{Kind: replication.MutationPutPresent, ExpectedValueLength: 10, ExpectedValueDigest: replication.Digest{1}},
	} {
		targets := []ReplicatedTransactionTarget{{Batches: []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{mutation}}}}}
		if preparedDirectEligible(queries, targets) {
			t.Fatalf("incomplete guard accepted: %+v", mutation)
		}
	}
}
