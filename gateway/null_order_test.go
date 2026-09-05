package gateway

import (
	"testing"

	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestNullOrderingDistributedMergeAndPlanner(t *testing.T) {
	for _, tc := range []struct {
		source            string
		desc              bool
		nulls             sqlast.WindowNullOrder
		left, right, want []string
	}{
		{`SELECT k FROM docs ORDER BY k ASC NULLS LAST`, false, sqlast.WindowNullsLast, []string{"1", "null"}, []string{"2", "null"}, []string{"1", "2", "null", "null"}},
		{`SELECT k FROM docs ORDER BY k DESC NULLS FIRST`, true, sqlast.WindowNullsFirst, []string{"null", "2"}, []string{"null", "1"}, []string{"null", "null", "2", "1"}},
	} {
		stmt, err := sqlast.Parse(tc.source)
		if err != nil {
			t.Fatal(err)
		}
		order, reason := planOrder(stmt, "")
		if reason != "" || len(order) != 1 || order[0].Nulls != tc.nulls || order[0].Desc != tc.desc {
			t.Fatalf("order = %+v/%s", order, reason)
		}
		properties := plannerOrdering(order)
		if properties[0].NullsFirst != (tc.nulls == sqlast.WindowNullsFirst) {
			t.Fatalf("planner null placement = %+v", properties)
		}
		_, rows, err := mergeRows([]*shardservice.ShardResponse{rowsOf(tc.left...), rowsOf(tc.right...)}, order, 10)
		if err != nil || len(rows) != len(tc.want) {
			t.Fatalf("merge = %v/%v", rows, err)
		}
		for i := range rows {
			if string(rows[i][0].Bytes) != tc.want[i] {
				t.Fatalf("merge[%d] = %q, want %q", i, rows[i][0].Bytes, tc.want[i])
			}
		}
	}
}
