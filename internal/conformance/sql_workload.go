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
	{"update_from", `UPDATE compatibility_docs SET n = changes.n FROM compatibility_changes AS changes WHERE compatibility_docs.id = changes.id`},
	{"delete_using", `DELETE FROM compatibility_docs USING compatibility_changes WHERE compatibility_docs.id = compatibility_changes.id`},
	{"mutation_cte", `WITH removed AS (DELETE FROM compatibility_docs WHERE id = 'a' RETURNING n) SELECT n FROM removed`},
	{"composite_conflict_target", `INSERT INTO compatibility_docs (id, n) VALUES ('a', 0) ON CONFLICT (id, n) DO NOTHING`},
	{"copy", `COPY compatibility_docs FROM STDIN`},
}
