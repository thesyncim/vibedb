package gateway

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestScalarInt64DeltaAcceptsOnlyExactClosedArithmetic(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		args []any
		want int64
		ok   bool
	}{
		{name: "literal add", sql: `UPDATE messages SET score = score + 1 WHERE id = ?`, want: 1, ok: true},
		{name: "reversed add", sql: `UPDATE messages SET score = 1 + score WHERE id = ?`, want: 1, ok: true},
		{name: "subtract", sql: `UPDATE messages SET score = score - 2 WHERE id = ?`, want: -2, ok: true},
		{name: "bound integer", sql: `UPDATE messages SET score = score + ? WHERE id = ?`, args: []any{int64(3)}, want: 3, ok: true},
		{name: "decimal", sql: `UPDATE messages SET score = score + 1.5 WHERE id = ?`},
		{name: "multiply", sql: `UPDATE messages SET score = score * 2 WHERE id = ?`},
		{name: "overflow literal", sql: `UPDATE messages SET score = score + 9223372036854775808 WHERE id = ?`},
		{name: "subtract min", sql: `UPDATE messages SET score = score - 9223372036854775808 WHERE id = ?`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := sqlast.ParseStatement(test.sql)
			if err != nil || statement.Update == nil || len(statement.Update.Assignments) != 1 {
				t.Fatalf("parse=%+v err=%v", statement, err)
			}
			got, ok := scalarInt64Delta(statement.Update.Assignments[0].Expr, "score", test.args)
			if ok != test.ok || (ok && got != test.want) {
				t.Fatalf("delta=%d ok=%v, want %d %v", got, ok, test.want, test.ok)
			}
		})
	}
}

func TestPreparedDirectIntegerUpdateUsesApplyTimeDeltaAndFallbackKeepsCAS(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	if err := snapshot.attachReplicatedTableDeclarations([]ReplicatedTableDeclaration{{
		Table:       "messages",
		CreateTable: `CREATE TABLE messages (id TEXT PRIMARY KEY, score INTEGER, keep TEXT)`,
	}}); err != nil {
		t.Fatal(err)
	}
	old := []byte(`{"id":"message-1","score":41,"keep":"x"}`)
	reader, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
	query := Query{
		SQL:    `UPDATE messages SET score = score + 1 WHERE id = ?`,
		Params: []shardservice.Param{shardservice.StringParam("message-1")},
	}
	profile := executor.profileFor(ClassInteractive)
	targets, handled, err := executor.planReplicatedSQLTransactionWithDataMode(
		t.Context(), snapshot, []Query{query}, profile, data,
		replicatedSQLCommittedLeaderPreimage,
	)
	if err != nil || !handled || len(targets) != 1 || reader.reads != 0 {
		t.Fatalf("delta plan=%d handled=%v reads=%d err=%v", len(targets), handled, reader.reads, err)
	}
	mutation := targets[0].Batches[0].Mutations[0]
	if mutation.Kind != replication.MutationJSONInt64Delta ||
		mutation.ExpectedValueLength != 0 || mutation.ExpectedValueDigest != (replication.Digest{}) {
		t.Fatalf("delta mutation=%+v", mutation)
	}
	column, delta, ok := replication.OpenJSONInt64Delta(mutation.Value)
	if !ok || !bytes.Equal(column, []byte("score")) || delta != 1 {
		t.Fatalf("delta descriptor column=%q delta=%d ok=%v", column, delta, ok)
	}

	// The ordinary linearizable planner remains the differential reference for
	// this SQL shape and still emits a full-row digest guard.
	casReader, casData := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
	casTargets, casHandled, err := executor.planReplicatedSQLTransactionWithData(
		t.Context(), snapshot, []Query{query}, profile, casData,
	)
	if err != nil || !casHandled || len(casTargets) != 1 || casReader.reads != 1 {
		t.Fatalf("CAS plan=%d handled=%v reads=%d err=%v", len(casTargets), casHandled, casReader.reads, err)
	}
	casMutation := casTargets[0].Batches[0].Mutations[0]
	if casMutation.Kind != replication.MutationPutDigestEqual ||
		casMutation.ExpectedValueLength != uint64(len(old)) ||
		casMutation.ExpectedValueDigest != replication.Digest(sha256.Sum256(old)) {
		t.Fatalf("CAS mutation=%+v", casMutation)
	}
}

func TestPreparedDirectIntegerUpdatePublishesJID1ThroughDirectExecutor(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	if err := snapshot.attachReplicatedTableDeclarations([]ReplicatedTableDeclaration{{
		Table:       "messages",
		CreateTable: `CREATE TABLE messages (id TEXT PRIMARY KEY, score INTEGER NOT NULL, keep TEXT)`,
	}}); err != nil {
		t.Fatal(err)
	}
	// CatalogHolder snapshots are immutable and clone their initial state; the
	// declaration must be attached before constructing the production planner.
	planner = NewExecutor(nil, NewCatalogHolder(snapshot), Options{})
	old := []byte(`{"id":"message-1","score":41,"keep":"x"}`)
	reader, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
	executor := &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}
	tenant := []byte("prepared-jid1")
	key := preparedDirectTestKey(tenant)
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	queries := []Query{{
		SQL:    `UPDATE messages SET score = score + 1 WHERE id = ?`,
		Params: []shardservice.Param{shardservice.StringParam("message-1")},
	}}
	plan, err := executor.PrepareDirect(ctx, key, tenant, queries)
	if err != nil {
		t.Fatal(err)
	}
	if reader.reads != 0 || plan.Target.Batches[0].Mutations[0].Kind != replication.MutationJSONInt64Delta {
		t.Fatalf("prepared plan reads=%d mutation=%+v", reader.reads, plan.Target.Batches[0].Mutations[0])
	}

	proposal := &directSQLProposalClient{t: t, route: plan.Target.Route, applied: 1}
	executor.data, err = NewReplicatedExecutor(proposal, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.ExecutePreparedDirect(ctx, key, tenant, queries, plan)
	if err != nil || !result.Direct || result.Result == nil || result.Result.RowsAffected != 1 {
		t.Fatalf("direct result=%+v err=%v", result, err)
	}
	if len(proposal.kinds) != 1 || proposal.kinds[0] != replication.MutationJSONInt64Delta {
		t.Fatalf("published mutation kinds=%v, want [%d]", proposal.kinds, replication.MutationJSONInt64Delta)
	}
}

func TestPreparedDirectIntegerUpdateReplaysSameJID1AfterGatewayReplan(t *testing.T) {
	snapshot, planner := replicatedSQLTransactionFixture(t, true)
	if err := snapshot.attachReplicatedTableDeclarations([]ReplicatedTableDeclaration{{
		Table:       "messages",
		CreateTable: `CREATE TABLE messages (id TEXT PRIMARY KEY, score INTEGER NOT NULL)`,
	}}); err != nil {
		t.Fatal(err)
	}
	planner = NewExecutor(nil, NewCatalogHolder(snapshot), Options{})
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute("data", "all", replicas[:0])
	if !ok {
		t.Fatal("missing direct SQL route")
	}
	proposal := &directSQLProposalClient{t: t, route: route, applied: 1}
	data, err := NewReplicatedExecutor(proposal, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	executor := &DurableSQLRequestExecutor{planner: planner, data: data, singleFast: true}
	tenant := []byte("replay-jid1")
	key := preparedDirectTestKey(tenant)
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	queries := []Query{{
		SQL:    `UPDATE messages SET score = score + 1 WHERE id = ?`,
		Params: []shardservice.Param{shardservice.StringParam("message-1")},
	}}
	plan, err := executor.PrepareDirect(ctx, key, tenant, queries)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = executor.ExecutePreparedDirect(ctx, key, tenant, queries, plan); err != nil {
		t.Fatal(err)
	}
	replayed, found, err := executor.ReplayRequestWithTenant(ctx, key, tenant, queries)
	if err != nil || !found || !replayed.Direct || replayed.Result == nil ||
		len(proposal.kinds) != 2 || proposal.kinds[0] != replication.MutationJSONInt64Delta ||
		proposal.kinds[1] != replication.MutationJSONInt64Delta {
		t.Fatalf("replayed=%+v found=%v kinds=%v err=%v", replayed, found, proposal.kinds, err)
	}
}
