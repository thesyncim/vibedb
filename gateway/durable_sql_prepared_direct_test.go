package gateway

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibejson"
)

func TestDurableSQLPreparedUpdateReplaysPersistedMutation(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	old := []byte(`{"id":"message-1","n":41}`)
	reader, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
	executor := &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}
	tenant := []byte("prepared-direct")
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{1}, Request: requestledger.RequestID{2}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)), IssuerEpoch: 1, IssuerLane: requestledger.IssuerLane{3}, IssuerSequence: 1}
	ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	queries := []Query{{SQL: `UPDATE messages SET n=n+1 WHERE id=?`, Class: ClassInteractive, Params: []shardservice.Param{shardservice.StringParam("message-1")}}}
	plan, err := executor.PrepareDirect(ctx, key, tenant, queries)
	if err != nil || reader.reads != 1 {
		t.Fatalf("prepare reads=%d err=%v", reader.reads, err)
	}
	mutation := plan.Participant.Batches[0].Mutations[0]
	if mutation.Kind != replication.MutationPutDigestEqual || string(mutation.Value) != `{"id":"message-1","n":42}` || mutation.ExpectedValueDigest != replication.Digest(sha256.Sum256(old)) {
		t.Fatalf("mutation=%+v", mutation)
	}
	raw, err := vibejson.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var recovered DurableSQLDirectPlan
	if err = vibejson.Unmarshal(raw, &recovered); err != nil {
		t.Fatal(err)
	}
	command := func(p *DurableSQLDirectPlan) []byte {
		encoded, _, err := appendReplicatedDirectMutationCommand(nil, ReplicatedDirectMutation{Key: p.Key, RequestDigest: p.RequestDigest, Tenant: tenant, Participant: p.Participant})
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	if !bytes.Equal(command(plan), command(&recovered)) {
		t.Fatal("journal round trip changed native command")
	}
	reader.value = []byte(`{"id":"message-1","n":99}`)
	proposer := &directSQLProposalClient{t: t, route: plan.Participant.Route, applied: 1}
	executor.data, err = NewReplicatedExecutor(proposer, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, err := executor.ExecutePreparedDirect(ctx, key, tenant, queries, &recovered)
		if err != nil || !result.Direct || result.Result.RowsAffected != 1 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	if reader.reads != 1 || proposer.proposals != 2 {
		t.Fatal("execution reevaluated SQL")
	}
	queries[0].SQL = `UPDATE messages SET n=n+2 WHERE id=?`
	if _, err = executor.ExecutePreparedDirect(ctx, key, tenant, queries, &recovered); err == nil || proposer.proposals != 2 {
		t.Fatal("changed caller input accepted")
	}
}

func TestDurableSQLPreparedUpdateRequiresExactPreimageGuard(t *testing.T) {
	query := []Query{{SQL: `UPDATE messages SET n=n+1 WHERE id='missing'`}}
	participants := []ReplicatedTransactionParticipant{{Batches: []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPutPresent, Key: []byte("missing"), Value: []byte(`{}`)}}}}}}
	if preparedDirectEligible(query, participants) {
		t.Fatal("a missing-row placeholder is not a guarded update")
	}
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	reader, data := attachReplicatedSQLIndexedReadClient(t, snapshot, nil)
	executor := &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}
	tenant := []byte("prepared-missing")
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{1}, Request: requestledger.RequestID{2}, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)), IssuerEpoch: 1, IssuerLane: requestledger.IssuerLane{3}, IssuerSequence: 1}
	plan, err := executor.PrepareDirect(t.Context(), key, tenant, query)
	if plan != nil || !errors.Is(err, ErrDurableSQLDirectIneligible) || !errors.Is(err, ErrDurableSQLNotAdmitted) || reader.reads != 1 {
		t.Fatalf("missing preimage plan=%+v reads=%d err=%v", plan, reader.reads, err)
	}
}
