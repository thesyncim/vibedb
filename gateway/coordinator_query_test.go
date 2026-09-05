package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestCoordinatorQueryMatchesGlobalSQLSemantics(t *testing.T) {
	client, _ := newTwoShardCluster(t, 3)
	executor := NewExecutor(client, NewCatalogHolder(twoShardSnapshot(t, 1, 3)), Options{})
	for _, tc := range []struct {
		sql  string
		want []string
	}{
		{`SELECT COALESCE(SUM(n), 0) FROM messages`, []string{"10"}},
		{`SELECT COUNT(*), SUM(n), AVG(n) FROM messages WHERE COALESCE(n,0)>1`, []string{"3|9|3"}},
		{`SELECT n, COUNT(*) FROM messages WHERE n IS DISTINCT FROM NULL GROUP BY n ORDER BY -SUM(n) LIMIT 2`, []string{"4|1", "3|1"}},
		{`SELECT COUNT(*), COALESCE(SUM(n),0) FROM messages WHERE COALESCE(n,0)>100`, []string{"0|0"}},
		{`WITH c AS (SELECT n FROM messages) SELECT SUM(n) FROM c WHERE COALESCE(n,0)>2`, []string{"7"}},
		{`SELECT n, COUNT(*), ROW_NUMBER() OVER (ORDER BY n) AS rn FROM messages WHERE COALESCE(n,0)>2 GROUP BY n ORDER BY n`, []string{"3|1|1", "4|1|2"}},
		{`SELECT SUM(c.n) FROM messages AS a JOIN messages AS b ON a.n=b.n JOIN messages AS c ON b.n=c.n WHERE COALESCE(c.n,0)>2`, []string{"7"}},
		{`SELECT GREATEST(SUM(n), 5), LEAST(MAX(n), 3), NULLIF(COUNT(*), 0) FROM messages`, []string{"10|3|4"}},
		{`SELECT n FROM messages WHERE n=1 OR COALESCE(n,0)=4 ORDER BY n`, []string{"1", "4"}},
		{`SELECT d.n FROM (SELECT n, CASE WHEN n>2 THEN TRUE ELSE FALSE END AS visible FROM messages) AS d WHERE d.visible ORDER BY d.n`, []string{"3", "4"}},
		{`SELECT n FROM messages ORDER BY GREATEST(n,0) DESC LIMIT 2`, []string{"4", "3"}},
		{`SELECT n FROM messages WHERE n IS DISTINCT FROM NULL ORDER BY -n LIMIT 2`, []string{"4", "3"}},
		{`SELECT n FROM messages WHERE COALESCE(n,0) IS NOT DISTINCT FROM 4`, []string{"4"}},
		{`SELECT n FROM messages ORDER BY -n LIMIT 2 OFFSET 1`, []string{"3", "2"}},
		{`SELECT n FROM messages GROUP BY n ORDER BY COALESCE(SUM(n),0) DESC LIMIT 2`, []string{"4", "3"}},
		{`SELECT a.n FROM messages AS a JOIN messages AS b ON a.n=b.n ORDER BY -b.n LIMIT 2`, []string{"4", "3"}},
		{`SELECT n, ROW_NUMBER() OVER (ORDER BY n) AS rn FROM messages ORDER BY -n LIMIT 2`, []string{"4|4", "3|3"}},
		{`SELECT AVG(n) FROM messages`, []string{"2.5"}},
		{`SELECT n FROM messages ORDER BY n DESC LIMIT 2 OFFSET 1`, []string{"3", "2"}},
		{`SELECT DISTINCT n FROM messages ORDER BY n`, []string{"1", "2", "3", "4"}},
		{`SELECT COUNT(*) FROM messages HAVING COUNT(*) > 3`, []string{"4"}},
		{`WITH c AS (SELECT n FROM messages) SELECT SUM(n) FROM c`, []string{"10"}},
		{`SELECT SUM(d.n) FROM (SELECT n FROM messages) AS d`, []string{"10"}},
		{`SELECT n FROM messages WHERE n < 3 UNION ALL SELECT n FROM messages WHERE n > 2 ORDER BY 1 DESC`, []string{"4", "3", "2", "1"}},
		{`SELECT n, ROW_NUMBER() OVER (ORDER BY n DESC) AS rn FROM messages ORDER BY rn`, []string{"4|1", "3|2", "2|3", "1|4"}},
		{`SELECT a.n, b.n FROM messages AS a CROSS JOIN messages AS b WHERE a.n = 1 AND b.n = 4`, []string{"1|4"}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			result, err := executor.Query(context.Background(), Query{SQL: tc.sql, Class: ClassBatch})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, row := range result.Rows {
				got = append(got, joinedSQLCells(row))
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			if result.ShardsFanned != 2 {
				t.Fatalf("read %d shards", result.ShardsFanned)
			}
		})
	}
}

func TestCoordinatorRejectsDeclaredDomainMismatchBeforeSourceIO(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(config, endpoints, 5, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile},
		[]ReplicatedTableDeclaration{{Table: "messages", CreateTable: `CREATE TABLE messages (id TEXT PRIMARY KEY, score INTEGER)`}})
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(func(context.Context, string) (net.Conn, error) {
		t.Error("schema analysis opened a shard connection")
		return nil, errors.New("unexpected shard read")
	})
	executor := NewExecutor(client, NewCatalogHolder(snapshot), Options{})
	for _, statement := range []string{
		`WITH c AS (SELECT id FROM messages WHERE id=score) SELECT id FROM c LIMIT 0`,
		`WITH c AS (SELECT id,score FROM messages ORDER BY CASE WHEN id=score THEN 1 ELSE 0 END) SELECT id FROM c LIMIT 0`,
		`SELECT COALESCE(SUM(score),0) FROM messages WHERE id=score`,
		`SELECT id FROM messages WHERE id IS DISTINCT FROM score LIMIT 0`,
	} {
		_, err := executor.Query(t.Context(), Query{SQL: statement, Class: ClassBatch})
		var mismatch *sqlast.UndefinedOperatorError
		if !errors.As(err, &mismatch) {
			t.Fatalf("%s: expected schema operator mismatch, got %T %v", statement, err, err)
		}
	}
}
func joinedSQLCells(row []shardservice.Cell) string {
	var out string
	for i, cell := range row {
		if i != 0 {
			out += "|"
		}
		if cell.Null {
			out += "null"
		} else {
			out += string(cell.Bytes)
		}
	}
	return out
}

func TestCoordinatorQueryPrunesNestedShardKeysWithoutCrossingLimit(t *testing.T) {
	client, nodes := newTwoShardCluster(t, 3)
	snapshot := twoShardSnapshot(t, 1, 3)
	executor := NewExecutor(client, NewCatalogHolder(snapshot), Options{})
	mapper := distribution.NewNativeMapperWithBucketBits(1, distribution.DefaultVirtualBucketBits)
	var keys [2]string
	for i := 0; keys[0] == "" || keys[1] == ""; i++ {
		key := fmt.Sprintf("routed-%d", i)
		point, err := mapper.PointFor([]distribution.Scalar{distribution.NewString(key)})
		if err != nil {
			t.Fatal(err)
		}
		target, ok := snapshot.config.Manifests[0].ResolvePointTarget(point)
		if !ok {
			t.Fatal("no target")
		}
		index := 0
		if target.Shard == "80-" {
			index = 1
		}
		if keys[index] != "" {
			continue
		}
		keys[index] = key
		node := nodes["shard-a"]
		if index == 1 {
			node = nodes["shard-b"]
		}
		seed(t, client, node, seedStmt{sql: `INSERT INTO messages (tenant_id,n) VALUES (?,100)`, params: []shardservice.Param{shardservice.StringParam(key)}})
	}
	// More documents are present on the selected shard than this source cap
	// allows. Passing proves the remote scan applies the propagated key filter.
	t.Run("source key filtering", func(t *testing.T) {
		profile := executor.profileFor(ClassBatch)
		profile.PerShardRows = 1
		result, err := executor.queryWithProfile(t.Context(), Query{SQL: `WITH c AS (SELECT tenant_id,n FROM messages) SELECT SUM(n)+1 FROM c WHERE tenant_id=?`, Params: []shardservice.Param{shardservice.StringParam(keys[0])}, Class: ClassBatch}, profile)
		if err != nil || result == nil || result.ShardsFanned != 1 || len(result.Rows) != 1 || joinedSQLCells(result.Rows[0]) != "1.01e2" {
			t.Fatalf("source filter: result=%+v err=%v", result, err)
		}
	})
	for _, tc := range []struct {
		sql    string
		params []shardservice.Param
		shards int
		want   []string
	}{
		{`WITH c(owner,n) AS (SELECT tenant_id,n FROM messages) SELECT SUM(n)+1 FROM c WHERE owner=?`, []shardservice.Param{shardservice.StringParam(keys[0])}, 1, []string{"1.01e2"}},
		{`WITH c(owner,n) AS (SELECT tenant_id,n FROM messages) SELECT COUNT(*), SUM(n) FROM c WHERE owner IS NOT DISTINCT FROM ? AND COALESCE(n,0)>?`, []shardservice.Param{shardservice.StringParam(keys[0]), shardservice.NumberParam("50")}, 1, []string{"1|100"}},
		{`SELECT COUNT(*) FROM messages WHERE tenant_id IS NOT DISTINCT FROM NULL`, nil, 0, []string{"0"}},
		{`SELECT SUM(c.n) FROM messages AS a JOIN messages AS b ON a.tenant_id=b.tenant_id JOIN messages AS c ON b.tenant_id=c.tenant_id WHERE a.tenant_id=? AND COALESCE(c.n,0)>50`, []shardservice.Param{shardservice.StringParam(keys[0])}, 1, []string{"100"}},
		{`WITH c(owner,n) AS (SELECT tenant_id,n FROM messages) SELECT n+1 FROM c WHERE owner IS NOT DISTINCT FROM ?`, []shardservice.Param{shardservice.StringParam(keys[0])}, 1, []string{"1.01e2"}},
		{`SELECT n FROM messages WHERE tenant_id IS NOT DISTINCT FROM ?`, []shardservice.Param{shardservice.StringParam(keys[1])}, 1, []string{"100"}},
		{`WITH c(owner,n) AS (SELECT tenant_id,n FROM messages) SELECT n+1 FROM c WHERE owner IS NOT DISTINCT FROM NULL`, nil, 0, nil},
		{`WITH unused AS (SELECT * FROM messages), c AS (SELECT tenant_id,n FROM messages) SELECT SUM(n)+1 FROM c WHERE tenant_id=?`, []shardservice.Param{shardservice.StringParam(keys[0])}, 1, []string{"1.01e2"}},
		{`WITH c AS (SELECT tenant_id AS owner,n FROM messages) SELECT SUM(n)+1 FROM c WHERE owner=?`, []shardservice.Param{shardservice.StringParam(keys[0])}, 1, []string{"1.01e2"}},
		{`SELECT SUM(d.n)+1 FROM (SELECT tenant_id,n FROM messages) AS d WHERE d.tenant_id=?`, []shardservice.Param{shardservice.StringParam(keys[1])}, 1, []string{"1.01e2"}},
		{`WITH unused AS (SELECT ?), c AS (SELECT n FROM messages WHERE tenant_id=?) SELECT SUM(n)+? FROM c`, []shardservice.Param{shardservice.NumberParam("999"), shardservice.StringParam(keys[0]), shardservice.NumberParam("2")}, 1, []string{"1.02e2"}},
		{`SELECT COALESCE(SUM(b.n),0) FROM messages AS a JOIN messages AS b ON a.tenant_id=b.tenant_id WHERE a.tenant_id=?`, []shardservice.Param{shardservice.StringParam(keys[0])}, 1, []string{"100"}},
		{`SELECT COALESCE(SUM(n),0) FROM messages WHERE tenant_id=? OR tenant_id=?`, []shardservice.Param{shardservice.StringParam(keys[0]), shardservice.StringParam(keys[0])}, 1, []string{"100"}},
		{`SELECT COALESCE(SUM(n),0) FROM messages WHERE tenant_id=? OR tenant_id=?`, []shardservice.Param{shardservice.StringParam(keys[0]), shardservice.StringParam(keys[1])}, 2, []string{"200"}},
		{`SELECT n+? FROM messages WHERE tenant_id=? UNION ALL SELECT n+? FROM messages WHERE tenant_id=? ORDER BY 1`, []shardservice.Param{shardservice.NumberParam("1"), shardservice.StringParam(keys[0]), shardservice.NumberParam("2"), shardservice.StringParam(keys[0])}, 1, []string{"1.01e2", "1.02e2"}},
		{`SELECT n FROM messages WHERE tenant_id=? UNION ALL SELECT n FROM messages WHERE tenant_id=?`, []shardservice.Param{shardservice.StringParam(keys[0]), shardservice.StringParam(keys[1])}, 2, []string{"100", "100"}},
		{`WITH c AS (SELECT tenant_id,n FROM messages) SELECT COALESCE(SUM(a.n)+SUM(b.n),0) FROM c AS a CROSS JOIN c AS b WHERE a.tenant_id=? AND b.tenant_id=?`, []shardservice.Param{shardservice.StringParam(keys[0]), shardservice.StringParam(keys[1])}, 2, []string{"2e2"}},
		{`SELECT COALESCE(SUM(n),0) FROM messages WHERE tenant_id=? OR n=4`, []shardservice.Param{shardservice.StringParam(keys[0])}, 2, []string{"104"}},
		{`SELECT d.n FROM (SELECT tenant_id,n FROM messages ORDER BY n LIMIT 1) AS d WHERE d.tenant_id=?`, []shardservice.Param{shardservice.StringParam(keys[0])}, 2, nil},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			result, err := executor.Query(t.Context(), Query{SQL: tc.sql, Params: tc.params, Class: ClassBatch})
			if err != nil {
				t.Fatal(err)
			}
			explain, err := executor.Explain(t.Context(), Query{SQL: tc.sql, Params: tc.params, Class: ClassBatch})
			if err != nil {
				t.Fatal(err)
			}
			if explain.Shards != result.ShardsFanned || explain.RouteKind != result.RouteKind || explain.ScatterReason != result.ScatterReason || explain.PlanFingerprint == "" || explain.PlanFingerprint != result.PlanFingerprint {
				t.Fatalf("EXPLAIN and execution disagree: explain=%+v result=%+v", explain, result)
			}
			var got []string
			for _, row := range result.Rows {
				got = append(got, joinedSQLCells(row))
			}
			if !reflect.DeepEqual(got, tc.want) || result.ShardsFanned != tc.shards {
				t.Fatalf("got=%v shards=%d; want=%v shards=%d", got, result.ShardsFanned, tc.want, tc.shards)
			}
		})
	}
}
