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
	{"jsonb_type", `CREATE TABLE compatibility_json (id TEXT PRIMARY KEY, custom JSONB)`},
	{"timestamp_type", `CREATE TABLE compatibility_times (id TEXT PRIMARY KEY, created_at TIMESTAMPTZ)`},
	{"uuid_type", `CREATE TABLE compatibility_uuid (id UUID PRIMARY KEY)`},
	{"varchar_modifier", `CREATE TABLE compatibility_names (id VARCHAR(255) PRIMARY KEY)`},
	{"column_defaults", `CREATE TABLE compatibility_defaults (id TEXT PRIMARY KEY, hidden BOOL DEFAULT FALSE)`},
	{"check_constraint", `CREATE TABLE compatibility_checks (id TEXT PRIMARY KEY, n INT CHECK (n >= 0))`},
	{"partial_index", `CREATE INDEX compatibility_partial ON compatibility_docs(n) WHERE n > 0`},
	{"expression_index", `CREATE INDEX compatibility_expression ON compatibility_docs(GREATEST(n, 0))`},
	{"covering_index", `CREATE INDEX compatibility_covering ON compatibility_docs(n) INCLUDE (hidden)`},
	{"descending_index", `CREATE INDEX compatibility_desc ON compatibility_docs(n DESC)`},
	{"jsonb_mutation", `UPDATE compatibility_docs SET custom = jsonb_set(custom, '{name}', '"x"') WHERE id = 'a'`},
	{"json_typeof", `SELECT jsonb_typeof(custom) FROM compatibility_docs`},
	{"json_key_existence", `SELECT id FROM compatibility_docs WHERE custom ? 'name'`},
	{"array_any", `SELECT id FROM compatibility_docs WHERE 'x' = ANY(tags)`},
	{"unnest", `SELECT value FROM unnest(ARRAY['x', 'y']) AS value`},
	{"row_value_comparison", `SELECT id FROM compatibility_docs WHERE (n, id) > (1, 'a')`},
	{"lock_skip_locked", `SELECT id FROM compatibility_docs FOR UPDATE SKIP LOCKED`},
	{"update_from", `UPDATE compatibility_docs SET n = changes.n FROM compatibility_changes AS changes WHERE compatibility_docs.id = changes.id`},
	{"delete_using", `DELETE FROM compatibility_docs USING compatibility_changes WHERE compatibility_docs.id = compatibility_changes.id`},
	{"mutation_cte", `WITH removed AS (DELETE FROM compatibility_docs WHERE id = 'a' RETURNING n) SELECT n FROM removed`},
	{"insert_select_columns", `INSERT INTO compatibility_docs (id, n) SELECT id, n FROM compatibility_changes`},
	{"distinct_on", `SELECT DISTINCT ON (n) id, n FROM compatibility_docs ORDER BY n, id`},
	{"count_distinct", `SELECT COUNT(DISTINCT n) FROM compatibility_docs`},
	{"aggregate_filter", `SELECT COUNT(*) FILTER (WHERE hidden = TRUE) FROM compatibility_docs`},
	{"string_functions", `SELECT lower(id) FROM compatibility_docs`},
	{"time_arithmetic", `SELECT NOW() + INTERVAL '1 second'`},
	{"computed_group_key", `SELECT COALESCE(n, 0), COUNT(*) FROM compatibility_docs GROUP BY COALESCE(n, 0)`},
	{"derived_wildcard_scalar_projection", `SELECT d.*, COALESCE(d.n, 0) FROM (SELECT * FROM compatibility_docs) AS d`},
	{"composite_conflict_target", `INSERT INTO compatibility_docs (id, n) VALUES ('a', 0) ON CONFLICT (id, n) DO NOTHING`},
	{"alter_column", `ALTER TABLE compatibility_docs ALTER COLUMN n SET DEFAULT 0`},
	{"copy", `COPY compatibility_docs FROM STDIN`},
}
