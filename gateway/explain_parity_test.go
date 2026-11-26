package gateway

import (
	"context"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/sql/driver"
)

func TestStripLeadingExplain(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"EXPLAIN SELECT 1", "SELECT 1"},
		{"  explain   analyze  SELECT id FROM t", "SELECT id FROM t"},
		{"SELECT 1", "SELECT 1"},
	}
	for _, tc := range cases {
		if got := stripLeadingExplain(tc.in); got != tc.want {
			t.Fatalf("stripLeadingExplain(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestPostgresLocalExplainAndAnalyze(t *testing.T) {
	session := &postgresSession{
		authority:    serviceauthz.Authority{Node: rafttransport.NodeID{1}, Generation: 1},
		state:        driver.SessionIdle,
		rows:         1000,
		bytes:        1 << 20,
		intermediate: 1 << 20,
		statements:   make(map[pgwire.BackendStatement]struct{}),
	}
	for _, sqlText := range []string{"EXPLAIN SELECT 1 + 2", "EXPLAIN ANALYZE SELECT 1 + 2"} {
		st, err := session.Prepare(context.Background(), sqlText)
		if err != nil {
			t.Fatalf("Prepare(%q): %v", sqlText, err)
		}
		cols := st.Columns()
		if len(cols) != 1 || cols[0] != "QUERY PLAN" {
			t.Fatalf("Columns(%q) = %v, want [QUERY PLAN]", sqlText, cols)
		}
		schema := st.AppendSchema(nil)
		if len(schema) != 1 || schema[0].Header != "QUERY PLAN" || schema[0].Representation != query.OutputSQLText {
			t.Fatalf("AppendSchema(%q) = %+v, want QUERY PLAN text", sqlText, schema)
		}
		var rows pgwire.BackendRows
		if err := st.QueryInto(context.Background(), nil, &rows); err != nil {
			t.Fatalf("QueryInto(%q): %v", sqlText, err)
		}
		var lines []string
		for rows.Next() {
			lines = append(lines, string(rows.Cell(0).AppendJSON(nil)))
		}
		_ = rows.Close()
		_ = st.Close()
		if len(lines) == 0 {
			t.Fatalf("QueryInto(%q) returned 0 plan lines", sqlText)
		}
		joined := strings.Join(lines, "\n")
		if strings.Contains(sqlText, "ANALYZE") && !strings.Contains(joined, "local-scalar") {
			t.Fatalf("EXPLAIN ANALYZE plan missing actual access path: %s", joined)
		}
	}
}
