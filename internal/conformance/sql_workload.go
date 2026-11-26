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
	{"composite_primary_key", `CREATE TABLE compatibility_members (app_pk INT, channel_cid TEXT, user_id TEXT, PRIMARY KEY (app_pk, channel_cid, user_id))`},
	{"partial_index", `CREATE INDEX compatibility_partial ON compatibility_docs(n) WHERE n > 0`},
	{"expression_index", `CREATE INDEX compatibility_expression ON compatibility_docs(GREATEST(n, 0))`},
	{"covering_index", `CREATE INDEX compatibility_covering ON compatibility_docs(n) INCLUDE (hidden)`},
	{"descending_index", `CREATE INDEX compatibility_desc ON compatibility_docs(n DESC)`},
	{"unnest", `SELECT value FROM unnest(ARRAY['x', 'y']) AS value`},
	{"row_value_comparison", `SELECT id FROM compatibility_docs WHERE (n, id) > (1, 'a')`},
	{"update_from", `UPDATE compatibility_docs SET n = changes.n FROM compatibility_changes AS changes WHERE compatibility_docs.id = changes.id`},
	{"delete_using", `DELETE FROM compatibility_docs USING compatibility_changes WHERE compatibility_docs.id = compatibility_changes.id`},
	{"mutation_cte", `WITH removed AS (DELETE FROM compatibility_docs WHERE id = 'a' RETURNING n) SELECT n FROM removed`},
	{"insert_select_columns", `INSERT INTO compatibility_docs (id, n) SELECT id, n FROM compatibility_changes`},
	{"time_arithmetic", `SELECT NOW() + INTERVAL '1 second'`},
	{"derived_wildcard_scalar_projection", `SELECT d.*, COALESCE(d.n, 0) FROM (SELECT * FROM compatibility_docs) AS d`},
	{"composite_conflict_target", `INSERT INTO compatibility_docs (id, n) VALUES ('a', 0) ON CONFLICT (id, n) DO NOTHING`},
	{"copy", `COPY compatibility_docs FROM STDIN`},
}
