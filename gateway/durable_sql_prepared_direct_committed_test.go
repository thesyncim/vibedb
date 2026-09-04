package gateway

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type preparedDirectReadRecorder struct {
	delegate        *replicatedSQLIndexedReadClient
	operations      []shardservice.ReplicatedOperation
	missingFollower bool
}

func (client *preparedDirectReadRecorder) DoReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	client.operations = append(client.operations, request.Operation)
	if client.missingFollower && request.Operation == shardservice.ReplicatedReadFollower {
		state, ok := client.delegate.states[endpoint.Address]
		if !ok {
			return nil, ErrReplicatedRoute
		}
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedReadMissing, HasState: true,
			State: state, ReadApplied: state.Applied,
		}, nil
	}
	return client.delegate.DoReplicated(ctx, endpoint, request)
}

func TestPrepareDirectUsesCommittedLeaderReadAndFullRowGuard(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	reader, base := attachReplicatedSQLIndexedReadClient(
		t, snapshot, []byte(`{"id":"message-1","n":41}`),
	)
	recorder := &preparedDirectReadRecorder{delegate: reader}
	data, err := NewReplicatedExecutor(recorder, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	executor := &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}
	tenant := []byte("prepared-committed")
	key := preparedDirectTestKey(tenant)
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := executor.PrepareDirect(ctx, key, tenant, []Query{{
		SQL: `UPDATE messages SET n=n+1 WHERE id=?`,
		Params: []shardservice.Param{
			shardservice.StringParam("message-1"),
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(recorder.operations) != 2 || recorder.operations[0] != shardservice.ReplicatedProbe ||
		recorder.operations[1] != shardservice.ReplicatedReadFollower {
		t.Fatalf("private read operations=%v, want probe then follower", recorder.operations)
	}
	mutation := plan.Target.Batches[0].Mutations[0]
	if mutation.Kind != replication.MutationPutDigestEqual ||
		mutation.ExpectedValueLength != uint64(len(reader.value)) ||
		mutation.ExpectedValueDigest != replication.Digest(sha256.Sum256(reader.value)) {
		t.Fatalf("mutation guard=%+v", mutation)
	}
	if base == nil || reader.reads != 1 {
		t.Fatalf("reads=%d base=%v", reader.reads, base)
	}
}

func TestPrepareDirectCommittedMissingFallsBackToLinearizableOnce(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	reader, _ := attachReplicatedSQLIndexedReadClient(
		t, snapshot, []byte(`{"id":"message-1","n":41}`),
	)
	recorder := &preparedDirectReadRecorder{delegate: reader, missingFollower: true}
	data, err := NewReplicatedExecutor(recorder, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	executor := &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}
	tenant := []byte("prepared-fallback")
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := executor.PrepareDirect(ctx, preparedDirectTestKey(tenant), tenant, []Query{{
		SQL: `UPDATE messages SET n=n+1 WHERE id=?`,
		Params: []shardservice.Param{
			shardservice.StringParam("message-1"),
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(recorder.operations) != 3 || recorder.operations[0] != shardservice.ReplicatedProbe ||
		recorder.operations[1] != shardservice.ReplicatedReadFollower ||
		recorder.operations[2] != shardservice.ReplicatedReadLeader {
		t.Fatalf("fallback read operations=%v, want probe, follower, leader", recorder.operations)
	}
	if plan.Target.Batches[0].Mutations[0].Kind != replication.MutationPutDigestEqual {
		t.Fatalf("fallback mutation=%+v", plan.Target.Batches[0].Mutations[0])
	}
}

func preparedDirectTestKey(tenant []byte) requestledger.RequestKey {
	return requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{1},
		Request: requestledger.RequestID{2}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		IssuerEpoch: 1, IssuerLane: requestledger.IssuerLane{3}, IssuerSequence: 1,
	}
}
