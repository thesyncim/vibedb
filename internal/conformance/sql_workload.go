package conformance

// SQLWorkloadGap is a reduced, synthetic SQL shape required by the Chat product
// or its shared database layer. Source locations and the full scope caveat are
// retained in docs/compatibility/sql-workload.md and sql-workload-evidence.json.
// These are remaining execution gaps, not accepted parser-only checkboxes.
// Full-text search is deliberately absent from this corpus.
type SQLWorkloadGap struct {
	ID  string
	SQL string
}

var SQLWorkloadGaps = []SQLWorkloadGap{
	{"mutation_cte", `WITH removed AS (DELETE FROM compatibility_docs WHERE id = 'a' RETURNING n) SELECT n FROM removed`},
	{"composite_conflict_target", `INSERT INTO compatibility_docs (id, n) VALUES ('a', 0) ON CONFLICT (id, n) DO NOTHING`},
	{"copy", `COPY compatibility_docs FROM STDIN`},
}
