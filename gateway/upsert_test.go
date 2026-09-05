package gateway

import (
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestSingleShardUpsertUsesAtomicConflictAction(t *testing.T) {
	client, _ := newTwoShardCluster(t, 3)
	snapshot := twoShardSnapshot(t, 1, 3)
	executor := NewExecutor(client, NewCatalogHolder(snapshot), Options{})
	key := shardKeyFor(t, snapshot, "-80")
	for _, tc := range []struct {
		sql    string
		params []shardservice.Param
		want   string
	}{
		{`INSERT INTO messages (tenant_id,n) VALUES (?,2) ON CONFLICT (tenant_id) DO UPDATE SET n=GREATEST(messages.n,EXCLUDED.n)`, []shardservice.Param{shardservice.StringParam(key)}, "2"},
		{`INSERT INTO messages (tenant_id,n) VALUES (?,9) ON CONFLICT (tenant_id) DO UPDATE SET n=GREATEST(messages.n,EXCLUDED.n)`, []shardservice.Param{shardservice.StringParam(key)}, "9"},
		{`INSERT INTO messages (tenant_id,n) VALUES (?,1) ON CONFLICT DO UPDATE SET tenant_id=EXCLUDED.tenant_id,n=messages.n+EXCLUDED.n`, []shardservice.Param{shardservice.StringParam(key)}, "10"},
		{`INSERT INTO messages VALUES (?) ON CONFLICT DO UPDATE SET "$doc"=EXCLUDED."$doc"`, []shardservice.Param{shardservice.DocumentParam(fmt.Sprintf(`{"tenant_id":%q,"n":20}`, key))}, "20"},
	} {
		result, err := executor.Exec(t.Context(), Query{SQL: tc.sql, Params: tc.params})
		if err != nil || result == nil || result.RowsAffected != 1 || result.ShardsFanned != 1 {
			t.Fatalf("upsert result=%+v err=%v", result, err)
		}
		result, err = executor.Query(t.Context(), Query{SQL: `SELECT n FROM messages WHERE tenant_id=?`, Params: []shardservice.Param{shardservice.StringParam(key)}})
		if err != nil || result == nil || len(result.Rows) != 1 || joinedSQLCells(result.Rows[0]) != tc.want {
			t.Fatalf("read result=%+v err=%v want=%s", result, err, tc.want)
		}
	}
	if _, err := executor.Exec(t.Context(), Query{SQL: `INSERT INTO messages (tenant_id,n) VALUES (?,NULL) ON CONFLICT DO UPDATE SET n=1`, Params: []shardservice.Param{shardservice.StringParam(key)}}); err == nil {
		t.Fatal("invalid candidate bypassed schema validation on conflict")
	}
	if _, err := snapshot.Prepare(t.Context(), `INSERT INTO messages (tenant_id,n) VALUES ('a',1) ON CONFLICT DO UPDATE SET tenant_id='another'`); !errors.Is(err, ErrWriteShardKeyMove) {
		t.Fatalf("key movement was not refused: %v", err)
	}
}

func TestReplicatedWholeDocumentUpsertRoutesCanonicalAtomicPuts(t *testing.T) {
	snapshot, executor, keys := replicatedSQLSplitTransactionFixture(t)
	for _, tc := range []Query{
		{SQL: `INSERT INTO messages VALUES (?),(?) ON CONFLICT (id) DO UPDATE SET "$doc"=EXCLUDED."$doc"`, Params: []shardservice.Param{
			shardservice.DocumentParam(fmt.Sprintf(`{"id":%q,"n":1}`, keys[0])),
			shardservice.DocumentParam(fmt.Sprintf(`{"id":%q,"n":2}`, keys[1])),
		}},
		{SQL: `INSERT INTO messages (id,n) VALUES (?,1),(?,2) ON CONFLICT (id) DO UPDATE SET "$doc"=EXCLUDED."$doc"`, Params: []shardservice.Param{shardservice.StringParam(keys[0]), shardservice.StringParam(keys[1])}},
	} {
		targets, handled, err := executor.planReplicatedSQLTransaction(t.Context(), snapshot, []Query{tc}, executor.profileFor(ClassInteractive))
		if err != nil || !handled || len(targets) != 2 {
			t.Fatalf("targets=%d handled=%v err=%v", len(targets), handled, err)
		}
		for _, target := range targets {
			if len(target.Batches) != 1 || len(target.Batches[0].Mutations) != 1 {
				t.Fatalf("unexpected participant: %+v", target)
			}
			mutation := target.Batches[0].Mutations[0]
			if mutation.Kind != replication.MutationPut {
				t.Fatalf("upsert did not retain atomic put: %+v", mutation)
			}
		}
	}
}

func TestReplicatedColumnUpsertRoutesConflictProgramWithoutPreimageRead(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(config, endpoints, 5, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
		[]ReplicatedTableDeclaration{{Table: "messages", CreateTable: `CREATE TABLE messages (id TEXT PRIMARY KEY, n INTEGER, city TEXT)`}})
	if err != nil {
		t.Fatal(err)
	}
	// No client exists: preparing the atomic conflict must not read a preimage.
	executor := NewExecutor(nil, NewCatalogHolder(snapshot), Options{})
	targets, handled, err := executor.planReplicatedSQLTransaction(t.Context(), snapshot, []Query{{
		SQL:    `INSERT INTO messages (id,n,city) VALUES (?,1,'candidate') ON CONFLICT (id) DO UPDATE SET n=messages.n+?,city=COALESCE(EXCLUDED.city,messages.city) WHERE messages.n<EXCLUDED.n`,
		Params: []shardservice.Param{shardservice.StringParam("a"), shardservice.NumberParam("9007199254740993")},
	}}, executor.profileFor(ClassInteractive))
	if err != nil || !handled || len(targets) != 1 || len(targets[0].Batches) != 1 || len(targets[0].Batches[0].Mutations) != 1 {
		t.Fatalf("targets=%+v handled=%v err=%v", targets, handled, err)
	}
	mutation := targets[0].Batches[0].Mutations[0]
	candidate, program, ok := replication.OpenConflictValue(mutation.Value)
	if mutation.Kind != replication.MutationPutConflict || !ok || string(candidate) != `{"city":"candidate","id":"a","n":1}` || len(program) == 0 {
		t.Fatalf("invalid conflict program: kind=%v candidate=%s program=%x", mutation.Kind, candidate, program)
	}
	for _, statement := range []string{
		`INSERT INTO messages (id,n) VALUES ('a',1) ON CONFLICT DO UPDATE SET absent=1`,
		`INSERT INTO messages (id,n) VALUES ('a',1) ON CONFLICT DO UPDATE SET n=1 WHERE messages.absent=1`,
		`INSERT INTO messages (id,n) VALUES ('a',1) ON CONFLICT DO UPDATE SET "$doc"=EXCLUDED."$doc" WHERE EXCLUDED.absent=1`,
		`INSERT INTO messages (id,n) VALUES ('a',1) ON CONFLICT DO UPDATE SET n=EXCLUDED.absent`,
	} {
		if _, err := snapshot.Prepare(t.Context(), statement); err == nil {
			t.Fatalf("invalid conflict action accepted: %s", statement)
		}
	}
}

func TestReplicatedConflictKeyExpressionsRouteOnlyToCandidateOwner(t *testing.T) {
	snapshot, executor, keys := replicatedSQLSplitTransactionFixture(t, ReplicatedTableDeclaration{
		Table: "messages", CreateTable: `CREATE TABLE messages (id TEXT PRIMARY KEY,n INTEGER)`,
	})
	for _, action := range []string{
		`id=COALESCE(messages.id,EXCLUDED.id),n=messages.n+EXCLUDED.n`,
		`id=CASE WHEN messages.n>0 THEN CAST(messages.id AS TEXT) ELSE EXCLUDED.id END`,
		`id='moved' WHERE false`,
		`id='moved'`, // Whether this branch executes is known only at atomic apply.
	} {
		for ordinal, key := range keys {
			targets, handled, err := executor.planReplicatedSQLTransaction(t.Context(), snapshot, []Query{{
				SQL:    `INSERT INTO messages (id,n) VALUES (?,1) ON CONFLICT DO UPDATE SET ` + action,
				Params: []shardservice.Param{shardservice.StringParam(key)},
			}}, executor.profileFor(ClassInteractive))
			if err != nil || !handled || len(targets) != 1 || len(targets[0].Batches) != 1 || len(targets[0].Batches[0].Mutations) != 1 {
				t.Fatalf("action=%s targets=%+v handled=%v err=%v", action, targets, handled, err)
			}
			wantOwner := [2]string{"left", "right"}[ordinal]
			if string(targets[0].Route.Shard) != wantOwner {
				t.Fatalf("key %s owner=%s want=%s", key, targets[0].Route.Shard, wantOwner)
			}
			mutation := targets[0].Batches[0].Mutations[0]
			candidate, _, ok := replication.OpenConflictValue(mutation.Value)
			if !ok || mutation.Kind != replication.MutationPutConflict || string(candidate) != fmt.Sprintf(`{"id":%q,"n":1}`, key) {
				t.Fatalf("wrong owner input: %+v candidate=%s", mutation, candidate)
			}
		}
	}
}
