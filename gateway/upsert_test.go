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
