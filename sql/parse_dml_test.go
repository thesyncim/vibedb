package sql

import (
	"errors"
	"strings"
	"testing"
)

// The acceptance and rejection contract for the statement kinds that are not
// SELECT. It follows parse_test.go and reject_test.go exactly: an accepted
// statement is asserted as a whole tree, and a refused one is asserted to name
// a position and to say something the author can act on.

func TestDMLGrammarShapes(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "insert a bound document",
			src:  `INSERT INTO users VALUES ('u1', ?)`,
			want: `insert into users (s"u1", ?0) params=1`,
		},
		{
			name: "insert with the pseudo-column list",
			src:  `INSERT INTO users ("$key", "$doc") VALUES ('u1', {"a":1})`,
			want: `insert into users explicit (s"u1", j{"a":1}) params=0`,
		},
		{
			name: "insert a quoted JSON string",
			src:  `INSERT INTO users VALUES (?, '{"a":1}')`,
			want: `insert into users (?0, s"{\"a\":1}") params=1`,
		},
		{
			name: "several rows in one statement",
			src:  `INSERT INTO users VALUES ('a', {"x":1}), ('b', ?)`,
			want: `insert into users (s"a", j{"x":1}) (s"b", ?0) params=1`,
		},
		{
			name: "update every document",
			src:  `UPDATE users SET "$doc" = ?`,
			want: `update users set ?0 <no target> params=1`,
		},
		{
			name: "update by condition",
			src:  `UPDATE users SET "$doc" = ? WHERE tier = 'free'`,
			want: `update users set ?0 where (cmp = 0:tier s"free") params=1`,
		},
		{
			name: "update by primary key",
			src:  `UPDATE users SET "$doc" = ? WHERE "$key" = 'u1'`,
			want: `update users set ?0 keys s"u1" params=1`,
		},
		{
			name: "delete everything",
			src:  `DELETE FROM users`,
			want: `delete from users all params=0`,
		},
		{
			name: "delete by condition",
			src:  `DELETE FROM users WHERE age > 30 AND NOT (name = 'x')`,
			want: `delete from users where (and (cmp > 0:age n30) (not (cmp = 0:name s"x"))) params=0`,
		},
		{
			name: "delete by primary key",
			src:  `DELETE FROM users WHERE "$key" = ?`,
			want: `delete from users keys ?0 params=1`,
		},
		{
			name: "delete by primary-key membership",
			src:  `DELETE FROM users WHERE "$key" IN ('a', 'b')`,
			want: `delete from users keys s"a" s"b" params=0`,
		},
		{
			name: "a nested path in a condition",
			src:  `DELETE FROM users WHERE profile.region = 'eu'`,
			want: `delete from users where (cmp = 0:profile.region s"eu") params=0`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := ParseStatement(tc.src)
			if err != nil {
				t.Fatalf("ParseStatement(%q) = %v", tc.src, err)
			}
			if got := dumpAny(stmt); got != tc.want {
				t.Errorf("ParseStatement(%q)\n got %s\nwant %s", tc.src, got, tc.want)
			}
		})
	}
}

func TestDDLGrammarShapes(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a collection with no schema",
			src:  `CREATE TABLE users`,
			want: `create table users`,
		},
		{
			name: "if not exists",
			src:  `CREATE TABLE IF NOT EXISTS users`,
			want: `create table users ifnotexists`,
		},
		{
			name: "a column list with a column-level key",
			src:  `CREATE TABLE users (id STRING PRIMARY KEY, name TEXT, age INTEGER NOT NULL)`,
			want: `create table users 0:id:STRING:required:pk 0:name:STRING 0:age:INTEGER:required primary 0:id`,
		},
		{
			name: "a table-level composite key",
			src:  `CREATE TABLE events (tenant STRING, id STRING, PRIMARY KEY (tenant, id))`,
			want: `create table events 0:tenant:STRING 0:id:STRING primary 0:tenant 0:id`,
		},
		{
			name: "a nested column",
			src:  `CREATE TABLE users (profile.region STRING, flags ARRAY)`,
			want: `create table users 0:profile.region:STRING 0:flags:ARRAY`,
		},
		{
			name: "SQL type aliases",
			src:  `CREATE TABLE t (a VARCHAR, b BIGINT, c DOUBLE, d BOOLEAN, e JSONB)`,
			want: `create table t 0:a:STRING 0:b:INTEGER 0:c:NUMBER 0:d:BOOL 0:e:ANY`,
		},
		{
			name: "an unnamed index",
			src:  `CREATE INDEX ON users (age)`,
			want: `create index on users 0:age/age`,
		},
		{
			name: "a named compound index over nested paths",
			src:  `CREATE INDEX by_region ON users (profile.region, tier)`,
			want: `create index by_region on users 0:profile.region/profile/region 0:tier/tier`,
		},
		{
			name: "an index over a subscript",
			src:  `CREATE INDEX ON users (tags[0])`,
			want: `create index on users 0:/tags/0/tags/0`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := ParseStatement(tc.src)
			if err != nil {
				t.Fatalf("ParseStatement(%q) = %v", tc.src, err)
			}
			if got := dumpAny(stmt); got != tc.want {
				t.Errorf("ParseStatement(%q)\n got %s\nwant %s", tc.src, got, tc.want)
			}
		})
	}
}

// dmlRejection is reject_test.go's rejection against ParseStatement.
type dmlRejection struct {
	name string
	src  string
	pos  int
	want string
}

func runDMLRejections(t *testing.T, cases []dmlRejection) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseStatement(tc.src)
			if err == nil {
				t.Fatalf("ParseStatement(%q) = nil, want a rejection", tc.src)
			}
			var parseErr *ParseError
			if !errors.As(err, &parseErr) {
				t.Fatalf("ParseStatement(%q) = %T, want *ParseError", tc.src, err)
			}
			if tc.pos >= 0 && parseErr.Pos != tc.pos {
				t.Errorf("ParseStatement(%q) reported offset %d, want %d (message: %s)",
					tc.src, parseErr.Pos, tc.pos, parseErr.Msg)
			}
			if !strings.Contains(parseErr.Msg, tc.want) {
				t.Errorf("ParseStatement(%q) said %q, want it to mention %q", tc.src, parseErr.Msg, tc.want)
			}
		})
	}
}

// TestRejectsMutationsTheEngineCannotExecute asserts every refusal the DML
// grammar makes. Each of these is a construct another dialect accepts, so each
// is a place where an author will arrive with an expectation, and the message
// is what they get instead of a result.
func TestRejectsMutationsTheEngineCannotExecute(t *testing.T) {
	runDMLRejections(t, []dmlRejection{
		{"a column list", `INSERT INTO t (name, age) VALUES (?, ?)`, -1, "this store has none"},
		{"a one-value row", `INSERT INTO t VALUES (?)`, -1, "the primary key and the document"},
		{"a three-value row", `INSERT INTO t VALUES ('k', ?, ?)`, -1, "exactly two values"},
		{"a numeric key", `INSERT INTO t VALUES (1, ?)`, -1, "keys are opaque bytes"},
		{"a NULL document", `INSERT INTO t VALUES ('k', NULL)`, -1, "not a document"},
		{"INSERT ... SELECT", `INSERT INTO t SELECT a FROM u`, -1, "nowhere to send"},
		{"DEFAULT VALUES", `INSERT INTO t DEFAULT VALUES`, -1, "no declared columns"},
		{"ON CONFLICT", `INSERT INTO t VALUES ('k', ?) ON CONFLICT DO NOTHING`, -1, "ON CONFLICT"},
		{"RETURNING", `DELETE FROM t RETURNING a`, -1, "RETURNING"},

		{"a top-level path assignment", `UPDATE t SET name = 'x'`, -1, "partial document update"},
		{"a nested path assignment", `UPDATE t SET profile.region = ?`, -1, "no JSON path-set operation"},
		{"assigning the key", `UPDATE t SET "$key" = 'x'`, -1, "identity"},
		{"two assignments", `UPDATE t SET "$doc" = ?, "$doc" = ?`, -1, "the whole document once"},
		{"UPDATE ... FROM", `UPDATE t SET "$doc" = ? FROM u`, -1, "never from another collection"},

		{"a key conjoined with a condition", `DELETE FROM t WHERE "$key" = 'a' AND b = 1`, -1, "primary key, not a field"},
		{"a key under NOT", `DELETE FROM t WHERE NOT "$key" = 'a'`, -1, "primary key, not a field"},
		{"a key inequality", `DELETE FROM t WHERE "$key" > 'a'`, -1, "primary key, not a field"},
		{"a key NOT IN", `DELETE FROM t WHERE "$key" NOT IN ('a')`, -1, "primary key, not a field"},
		{"DELETE ... USING", `DELETE FROM t USING u WHERE t.a = u.a`, -1, "never by a join"},
		{"DELETE ... LIMIT", `DELETE FROM t WHERE a = 1 LIMIT 5`, -1, "no LIMIT"},
		{"DELETE ... ORDER BY", `DELETE FROM t WHERE a = 1 ORDER BY a`, -1, "no ORDER BY"},
		{"a table alias", `UPDATE t AS x SET "$doc" = ?`, -1, "nothing to qualify"},

		{"MERGE", `MERGE INTO t USING u ON (t.a = u.a)`, 0, "MERGE"},
		{"REPLACE", `REPLACE INTO t VALUES ('k', ?)`, 0, "REPLACE"},
		{"TRUNCATE", `TRUNCATE TABLE t`, 0, "TRUNCATE"},
		{"DROP", `DROP TABLE t`, 0, "DROP"},
		{"ALTER", `ALTER TABLE t ADD COLUMN a STRING`, 0, "ALTER"},
	})
}

// TestRejectsDefinitionsTheEngineCannotEnforce asserts the DDL refusals. Every
// one of them is a constraint another dialect would have accepted and enforced,
// so accepting it here and enforcing nothing is the failure mode each message
// exists to prevent.
func TestRejectsDefinitionsTheEngineCannotEnforce(t *testing.T) {
	runDMLRejections(t, []dmlRejection{
		{"a length", `CREATE TABLE t (a VARCHAR(255))`, -1, "never enforced"},
		{"a precision", `CREATE TABLE t (a NUMERIC(10, 2))`, -1, "never enforced"},
		{"DATE", `CREATE TABLE t (a DATE)`, -1, "no date or time value"},
		{"TIMESTAMP", `CREATE TABLE t (a TIMESTAMP)`, -1, "no date or time value"},
		{"UUID", `CREATE TABLE t (a UUID)`, -1, "store it as STRING"},
		{"BYTEA", `CREATE TABLE t (a BYTEA)`, -1, "no byte string"},
		{"ENUM", `CREATE TABLE t (a ENUM)`, -1, "never checked"},
		{"an unknown type", `CREATE TABLE t (a WIDGET)`, -1, "unknown type"},
		{"a column with no type", `CREATE TABLE t (a)`, -1, "expected a column type"},
		{"an empty column list", `CREATE TABLE t ()`, -1, "may not be empty"},
		{"a duplicate column", `CREATE TABLE t (a STRING, a NUMBER)`, -1, "declared twice"},
		{"two primary keys", `CREATE TABLE t (a STRING PRIMARY KEY, b STRING PRIMARY KEY)`, -1, "declared twice"},
		{"a container key", `CREATE TABLE t (a OBJECT PRIMARY KEY)`, -1, "no ordering to derive one from"},
		{"a nullable key", `CREATE TABLE t (a NULL, PRIMARY KEY (a))`, -1, "must be present"},
		{"DEFAULT", `CREATE TABLE t (a STRING DEFAULT 'x')`, -1, "DEFAULT is not supported"},
		{"UNIQUE", `CREATE TABLE t (a STRING UNIQUE)`, -1, "UNIQUE is not supported"},
		{"CHECK", `CREATE TABLE t (a STRING, CHECK (a > 1))`, -1, "CHECK is not supported"},
		{"REFERENCES", `CREATE TABLE t (a STRING, FOREIGN KEY (a) REFERENCES u (b))`, -1, "FOREIGN is not supported"},
		{"CREATE TABLE AS", `CREATE TABLE t AS SELECT a FROM u`, -1, "created empty"},
		{"CREATE VIEW", `CREATE VIEW v AS SELECT a FROM t`, -1, "CREATE VIEW"},

		{"a unique index", `CREATE UNIQUE INDEX ON t (a)`, -1, "no uniqueness constraint"},
		{"a partial index", `CREATE INDEX ON t (a) WHERE b = 1`, -1, "cover every document"},
		{"an index method", `CREATE INDEX ON t (a) USING btree`, -1, "no method to choose"},
		{"an index direction", `CREATE INDEX ON t (a DESC)`, -1, "no direction"},
		{"an index over the whole document", `CREATE INDEX ON t (*)`, -1, "must stand alone"},
		{"a duplicate index path", `CREATE INDEX ON t (a, a)`, -1, "named twice"},
		{"too many index paths", `CREATE INDEX ON t (a, b, c, d, e)`, -1, "at most 4"},
	})
}
