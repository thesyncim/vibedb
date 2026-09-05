package gateway

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestReplicatedInsertIgnoreRoutesAndRetainsAtomicDecision(t *testing.T) {
	snapshot, executor, keys := replicatedSQLSplitTransactionFixture(t)
	targets, handled, err := executor.planReplicatedSQLTransaction(t.Context(), snapshot, []Query{{
		SQL:    `INSERT INTO messages (id,n) VALUES (?,NULL),(?,2) ON CONFLICT (id) DO NOTHING`,
		Params: []shardservice.Param{shardservice.StringParam(keys[0]), shardservice.StringParam(keys[1])},
	}}, executor.profileFor(ClassInteractive))
	if err != nil || !handled || len(targets) != 2 {
		t.Fatalf("targets=%d handled=%v err=%v", len(targets), handled, err)
	}
	for _, target := range targets {
		for _, batch := range target.Batches {
			for _, mutation := range batch.Mutations {
				if mutation.Kind != replication.MutationPutIfAbsent {
					t.Fatalf("mutation=%+v", mutation)
				}
			}
		}
	}
	if _, err := snapshot.Prepare(t.Context(), `INSERT INTO messages (id,n) VALUES ('a',1) ON CONFLICT (n) DO NOTHING`); err == nil {
		t.Fatal("accepted non-primary conflict target")
	}
}
