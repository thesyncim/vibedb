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
			src:  `INSERT INTO users VALUES (?)`,
			want: `insert into users (?0) params=1`,
		},
		{
			name: "insert a flat document",
			src:  `INSERT INTO users (id, name) VALUES ('u1', 'Ana')`,
			want: `insert into users fields -1:id -1:name (s"u1", s"Ana") params=0`,
		},
		{
			name: "insert a quoted JSON string",
			src:  `INSERT INTO users VALUES ('{"id":"u1","a":1}')`,
			want: `insert into users (s"{\"id\":\"u1\",\"a\":1}") params=0`,
		},
		{
			name: "several rows in one statement",
			src:  `INSERT INTO users VALUES ({"id":"a","x":1}), (?)`,
			want: `insert into users (j{"id":"a","x":1}) (?0) params=1`,
		},
		{
			name: "skip a conflicting document",
			src:  `INSERT INTO users VALUES (?) ON CONFLICT DO NOTHING RETURNING id`,
			want: `insert into users (?0) on conflict do nothing returning path(0:id) params=1`,
		},
		{
			name: "insert returning projected fields",
			src:  `INSERT INTO users VALUES (?) RETURNING id, profile.name AS name`,
			want: `insert into users (?0) returning path(0:id), path(0:profile.name) as name params=1`,
		},
		{
			name: "insert returning whole document",
			src:  `INSERT INTO users (id, name) VALUES ('u1', 'Ana') RETURNING *`,
			want: `insert into users fields -1:id -1:name (s"u1", s"Ana") returning path(0:) params=0`,
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
			name: "update returning",
			src:  `UPDATE users SET "$doc" = ? WHERE tier = 'free' RETURNING id`,
			want: `update users set ?0 where (cmp = 0:tier s"free") returning path(0:id) params=1`,
		},
		{
			name: "update ordered limit",
			src:  `UPDATE users SET "$doc" = ? WHERE tier = 'free' ORDER BY id DESC LIMIT ?`,
			want: `update users set ?0 where (cmp = 0:tier s"free") order 0:id:desc limit ?1 params=2`,
		},
		{
			name: "update by a JSON field named dollar key",
			src:  `UPDATE users SET "$doc" = ? WHERE "$key" = 'u1'`,
			want: `update users set ?0 where (cmp = 0:/$key s"u1") params=1`,
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
			name: "delete returning",
			src:  `DELETE FROM users WHERE age > 30 RETURNING id`,
			want: `delete from users where (cmp > 0:age n30) returning path(0:id) params=0`,
		},
		{
			name: "delete ordered limit",
			src:  `DELETE FROM users ORDER BY id LIMIT 2`,
			want: `delete from users all order 0:id:asc limit n2 params=0`,
		},
		{
			name: "delete by a JSON field named dollar key",
			src:  `DELETE FROM users WHERE "$key" = ?`,
			want: `delete from users where (cmp = 0:/$key ?0) params=1`,
		},
		{
			name: "delete by dollar-key field membership",
			src:  `DELETE FROM users WHERE "$key" IN ('a', 'b')`,
			want: `delete from users where (in 0:/$key s"a" s"b") params=0`,
		},
		{
			name: "drop a table",
			src:  `DROP TABLE users`,
			want: `drop table users`,
		},
		{
			name: "drop a table if it exists",
			src:  `DROP TABLE IF EXISTS users`,
			want: `drop table if exists users`,
		},
		{
			name: "truncate without table keyword",
			src:  `TRUNCATE users`,
			want: `truncate users`,
		},
		{
			name: "truncate with table keyword",
			src:  `TRUNCATE TABLE users`,
			want: `truncate users`,
		},
		{
			name: "drop an index",
			src:  `DROP INDEX by_age`,
			want: `drop index by_age`,
		},
		{
			name: "drop an index from a table if it exists",
			src:  `DROP INDEX IF EXISTS by_age ON users`,
			want: `drop index if exists by_age on users`,
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
			want: `create table users 0:id:STRING:required:pk 0:name:NULL|STRING 0:age:INTEGER:required primary 0:id`,
		},
		{
			name: "a table-level composite key",
			src:  `CREATE TABLE events (tenant STRING, id STRING, PRIMARY KEY (tenant, id))`,
			want: `create table events 0:tenant:STRING:required 0:id:STRING:required primary 0:tenant 0:id`,
		},
		{
			name: "a nested column",
			src:  `CREATE TABLE users (profile.region STRING, flags ARRAY)`,
			want: `create table users 0:profile.region:NULL|STRING 0:flags:NULL|ARRAY`,
		},
		{
			name: "SQL type aliases",
			src:  `CREATE TABLE t (a VARCHAR, b BIGINT, c DOUBLE, d BOOLEAN, e JSON)`,
			want: `create table t 0:a:NULL|STRING 0:b:NULL|INTEGER 0:c:NULL|NUMBER 0:d:NULL|BOOL 0:e:ANY`,
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
		{"the removed key/document row", `INSERT INTO t VALUES ('k', ?)`, -1, "one complete JSON document"},
		{"the removed bound key/document row", `INSERT INTO t VALUES (?, ?)`, -1, "declared primary-key field"},
		{"the removed pseudo-column pair", `INSERT INTO t ("$key", "$doc") VALUES ('k', ?)`, -1, "one complete document"},
		{"a three-value row", `INSERT INTO t VALUES ('k', ?, ?)`, -1, "one complete JSON document"},
		{"a NULL document", `INSERT INTO t VALUES (NULL)`, -1, "not a document"},
		{"INSERT ... SELECT", `INSERT INTO t SELECT a FROM u`, -1, "nowhere to send"},
		{"DEFAULT VALUES", `INSERT INTO t DEFAULT VALUES`, -1, "no declared columns"},
		{"conflict target", `INSERT INTO t VALUES (?) ON CONFLICT (id) DO NOTHING`, -1, "CONFLICT targets"},
		{"aggregate RETURNING", `INSERT INTO t VALUES (?) RETURNING COUNT(*)`, -1, "aggregate"},

		{"a top-level path assignment", `UPDATE t SET name = 'x'`, -1, "partial document update"},
		{"a nested path assignment", `UPDATE t SET profile.region = ?`, -1, "no JSON path-set operation"},
		{"assigning a path", `UPDATE t SET "$key" = 'x'`, -1, "partial document update"},
		{"two assignments", `UPDATE t SET "$doc" = ?, "$doc" = ?`, -1, "the whole document once"},
		{"UPDATE ... FROM", `UPDATE t SET "$doc" = ? FROM u`, -1, "never from another collection"},

		{"DELETE ... USING", `DELETE FROM t USING u WHERE t.a = u.a`, -1, "never by a join"},
		{"mutation ORDER BY without LIMIT", `DELETE FROM t WHERE a = 1 ORDER BY a`, -1, "ORDER BY requires LIMIT"},
		{"mutation OFFSET", `DELETE FROM t WHERE a = 1 LIMIT 5 OFFSET 1`, -1, "does not support OFFSET"},
		{"a table alias", `UPDATE t AS x SET "$doc" = ?`, -1, "nothing to qualify"},

		{"MERGE", `MERGE INTO t USING u ON (t.a = u.a)`, 0, "MERGE"},
		{"REPLACE", `REPLACE INTO t VALUES ('k', ?)`, 0, "REPLACE"},
		{"ALTER", `ALTER TABLE t ADD COLUMN a STRING`, 0, "ALTER"},
	})
}

func TestRejectsUnboundedCatalogDDLSyntax(t *testing.T) {
	for _, test := range []struct {
		name   string
		source string
		kind   Kind
	}{
		{"ordinary CREATE VIEW", `CREATE VIEW v AS SELECT a FROM t`, KindCreateView},
		{"ordinary DROP VIEW", `DROP VIEW v`, KindDropView},
		{"bounded DROP VIEW", `DROP VIEW IF EXISTS v RESTRICT`, KindDropView},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement, err := ParseStatement(test.source)
			if err != nil {
				t.Fatalf("ParseStatement(%q) = %v", test.source, err)
			}
			if statement.Kind != test.kind {
				t.Fatalf("ParseStatement(%q) kind = %v, want %v",
					test.source, statement.Kind, test.kind)
			}
		})
	}

	runDMLRejections(t, []dmlRejection{
		{"TRUNCATE without a table", `TRUNCATE`, -1, "collection name"},
		{"TRUNCATE TABLE without a table", `TRUNCATE TABLE`, -1, "collection name"},
		{"TRUNCATE several tables", `TRUNCATE t, u`, -1, "multiple tables"},
		{"TRUNCATE restart identity", `TRUNCATE t RESTART IDENTITY`, -1, "identity options"},
		{"TRUNCATE cascade", `TRUNCATE t CASCADE`, -1, "CASCADE/RESTRICT"},
		{"TRUNCATE placeholder", `TRUNCATE ?`, -1, "collection name"},
		{"DROP without an object kind", `DROP`, -1, "TABLE, INDEX, VIEW, or MATERIALIZED VIEW"},
		{"DROP VIEW without a name", `DROP VIEW`, -1, "view name"},
		{"DROP several views", `DROP VIEW v, w`, strings.Index(`DROP VIEW v, w`, ","), "trailing input"},
		{"DROP another object kind", `DROP SEQUENCE v`, -1, "DROP SEQUENCE"},
		{"DROP TABLE dangling comma", `DROP TABLE docs,`, -1, "collection name"},
		{"DROP INDEX without a name", `DROP INDEX`, -1, "index name"},
		{"DROP INDEX with incomplete IF", `DROP INDEX IF by_age`, -1, "EXISTS after IF"},
		{"DROP INDEX IF EXISTS without a name", `DROP INDEX IF EXISTS`, -1, "index name"},
		{"DROP several indexes", `DROP INDEX a, b`, -1, "multiple indexes"},
		{"DROP INDEX dangling comma", `DROP INDEX a,`, -1, "index name"},
		{"DROP INDEX ON without a table", `DROP INDEX by_age ON`, -1, "collection name"},
		{"DROP INDEX cascade", `DROP INDEX by_age CASCADE`, -1, "CASCADE/RESTRICT"},
		{"DROP INDEX restrict", `DROP INDEX by_age RESTRICT`, -1, "CASCADE/RESTRICT"},
		{"DROP INDEX placeholder", `DROP INDEX ?`, -1, "index name"},
		{"DROP INDEX table placeholder", `DROP INDEX by_age ON ?`, -1, "collection name"},
		{"DROP INDEX qualified table", `DROP INDEX by_age ON public.users`, -1, "qualified collection"},
		{"TRUNCATE incomplete identity option", `TRUNCATE docs RESTART`, -1, "IDENTITY"},
	})
}

func TestDocumentDerivedInsertGrammar(t *testing.T) {
	cases := []struct {
		name    string
		src     string
		columns int
		values  int
	}{
		{"whole document", `INSERT INTO t VALUES (?)`, 0, 1},
		{"flat document", `INSERT INTO t (name, age) VALUES (?, ?)`, 2, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			statement, err := ParseStatement(tc.src)
			if err != nil {
				t.Fatalf("ParseStatement(%q) = %v", tc.src, err)
			}
			if got := len(statement.Insert.Columns); got != tc.columns {
				t.Fatalf("len(Columns) = %d, want %d", got, tc.columns)
			}
			if got := len(statement.Insert.Rows[0].Values); got != tc.values {
				t.Fatalf("len(Values) = %d, want %d", got, tc.values)
			}
		})
	}
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
		{"SERIAL", `CREATE TABLE t (a SERIAL)`, -1, "sequence-backed generated default"},
		{"BIGSERIAL", `CREATE TABLE t (a BIGSERIAL)`, -1, "sequence-backed generated default"},
		{"MONEY", `CREATE TABLE t (a MONEY)`, -1, "fixed fractional"},
		{"bare CHAR", `CREATE TABLE t (a CHAR)`, -1, "fixed-width"},
		{"bare CHARACTER", `CREATE TABLE t (a CHARACTER)`, -1, "fixed-width"},
		{"bare NCHAR", `CREATE TABLE t (a NCHAR)`, -1, "fixed-width"},
		{"NVARCHAR", `CREATE TABLE t (a NVARCHAR)`, -1, "omitted-length semantics"},
		{"JSONB", `CREATE TABLE t (a JSONB)`, -1, "normalizes"},
		{"RECORD", `CREATE TABLE t (a RECORD)`, -1, "declared field shape"},
		{"STRUCT", `CREATE TABLE t (a STRUCT)`, -1, "declared field shape"},
		{"an unknown type", `CREATE TABLE t (a WIDGET)`, -1, "unknown type"},
		{"a column with no type", `CREATE TABLE t (a)`, -1, "expected a column type"},
		{"an empty column list", `CREATE TABLE t ()`, -1, "may not be empty"},
		{"a duplicate column", `CREATE TABLE t (a STRING, a NUMBER)`, -1, "declared twice"},
		{"two primary keys", `CREATE TABLE t (a STRING PRIMARY KEY, b STRING PRIMARY KEY)`, -1, "declared twice"},
		{"a container key", `CREATE TABLE t (a OBJECT PRIMARY KEY)`, -1, "no ordering to derive one from"},
		{"a nullable key", `CREATE TABLE t (a NULL, PRIMARY KEY (a))`, -1, "must be present"},
		{"an explicit nullable key", `CREATE TABLE t (a STRING NULL, PRIMARY KEY (a))`, -1, "explicit NULL"},
		{"NULL then NOT NULL", `CREATE TABLE t (a STRING NULL NOT NULL)`, -1, "contradictory"},
		{"NOT NULL then NULL", `CREATE TABLE t (a STRING NOT NULL NULL)`, -1, "contradictory"},
		{"column primary key then NULL", `CREATE TABLE t (a STRING PRIMARY KEY NULL)`, -1, "contradictory"},
		{"DEFAULT", `CREATE TABLE t (a STRING DEFAULT 'x')`, -1, "DEFAULT is not supported"},
		{"UNIQUE", `CREATE TABLE t (a STRING UNIQUE)`, -1, "UNIQUE is not supported"},
		{"CHECK", `CREATE TABLE t (a STRING, CHECK (a > 1))`, -1, "CHECK is not supported"},
		{"REFERENCES", `CREATE TABLE t (a STRING, FOREIGN KEY (a) REFERENCES u (b))`, -1, "FOREIGN is not supported"},
		{"CREATE TABLE AS", `CREATE TABLE t AS SELECT a FROM u`, -1, "created empty"},
		{"a unique index", `CREATE UNIQUE INDEX ON t (a)`, -1, "no uniqueness constraint"},
		{"a partial index", `CREATE INDEX ON t (a) WHERE b = 1`, -1, "cover every document"},
		{"an index method", `CREATE INDEX ON t (a) USING btree`, -1, "no method to choose"},
		{"an index direction", `CREATE INDEX ON t (a DESC)`, -1, "no direction"},
		{"an index over the whole document", `CREATE INDEX ON t (*)`, -1, "must stand alone"},
		{"a duplicate index path", `CREATE INDEX ON t (a, a)`, -1, "named twice"},
		{"too many index paths", `CREATE INDEX ON t (a, b, c, d, e)`, -1, "at most 4"},
	})
}
