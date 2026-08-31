package sql

import (
	"errors"
	"strconv"
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
			name: "replace a conflicting document",
			src:  `INSERT INTO users VALUES (?) ON CONFLICT DO UPDATE SET "$doc" = EXCLUDED."$doc" RETURNING id`,
			want: `insert into users (?0) on conflict do update set "$doc"=excluded."$doc" returning path(0:id) params=1`,
		},
		{
			name: "patch conflicting columns",
			src:  `INSERT INTO users (id, name) VALUES (?, ?) ON CONFLICT DO UPDATE SET name = EXCLUDED.name, touched = ?, note = NULL RETURNING id`,
			want: `insert into users fields -1:id -1:name (?0, ?1) on conflict do update set "name"=excluded."name", "touched"=?2, "note"=null returning path(0:id) params=3`,
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
			name: "insert from an independent query source",
			src:  `INSERT INTO users SELECT * FROM staged WHERE ready = ? ON CONFLICT DO NOTHING RETURNING id`,
			want: `insert into users source select path(0:) from staged where (cmp = 0:ready ?0) params=1 on conflict do nothing returning path(0:id) params=1`,
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
			name: "add a nullable column",
			src:  `ALTER TABLE users ADD COLUMN city TEXT`,
			want: `alter table users add column 0:city NULL|STRING`,
		},
		{
			name: "add a required nested column if absent",
			src:  `ALTER TABLE users ADD COLUMN IF NOT EXISTS profile.score INTEGER NOT NULL`,
			want: `alter table users add column if not exists 0:profile.score INTEGER not null`,
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
		{
			name: "an unnamed unique index",
			src:  `CREATE UNIQUE INDEX ON users (email)`,
			want: `create unique index on users 0:email/email`,
		},
		{
			name: "a named unique index",
			src:  `CREATE UNIQUE INDEX by_email ON users (email)`,
			want: `create unique index by_email on users 0:email/email`,
		},
		{
			name: "a unique index with if not exists",
			src:  `CREATE UNIQUE INDEX IF NOT EXISTS by_email ON users (email)`,
			want: `create unique index by_email ifnotexists on users 0:email/email`,
		},
		{
			name: "a composite unique index",
			src:  `CREATE UNIQUE INDEX tenant_email ON users (tenant, profile.email)`,
			want: `create unique index tenant_email on users 0:tenant/tenant 0:profile.email/profile/email`,
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
		{"DEFAULT VALUES", `INSERT INTO t DEFAULT VALUES`, -1, "no declared columns"},
		{"conflict target", `INSERT INTO t VALUES (?) ON CONFLICT (id) DO NOTHING`, -1, "CONFLICT targets"},
		{"conflict constraint", `INSERT INTO t VALUES (?) ON CONFLICT ON CONSTRAINT t_pkey DO UPDATE SET value = EXCLUDED.value`, -1, "ON CONSTRAINT"},
		{"conflict update where", `INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET value = EXCLUDED.value WHERE value = 'old'`, -1, "DO UPDATE WHERE"},
		{"nested conflict target", `INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET profile.name = EXCLUDED.name`, -1, "top-level column"},
		{"nested excluded source", `INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET name = EXCLUDED.profile.name`, -1, "nested paths"},
		{"reserved excluded document", `INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET name = EXCLUDED."$doc"`, -1, "reserved"},
		{"reserved excluded key", `INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET name = EXCLUDED."$key"`, -1, "reserved"},
		{"quoted excluded relation", `INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET name = "EXCLUDED".name`, -1, "EXCLUDED"},
		{"duplicate conflict target", `INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET name = EXCLUDED.name, name = 'again'`, -1, "more than once"},
		{"mixed whole conflict update", `INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET "$doc" = EXCLUDED."$doc", name = EXCLUDED.name`, -1, "cannot be combined"},
		{"wrong whole conflict source", `INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET "$doc" = EXCLUDED.payload`, -1, `EXCLUDED."$doc"`},
		{"aggregate RETURNING", `INSERT INTO t VALUES (?) RETURNING COUNT(*)`, -1, "aggregate"},

		{"a nested path assignment", `UPDATE t SET profile.region = ?`, -1, "one declared top-level column"},
		{"assigning a reserved path", `UPDATE t SET "$key" = 'x'`, -1, "reserved column"},
		{"two whole-document assignments", `UPDATE t SET "$doc" = ?, "$doc" = ?`, -1, "cannot be combined"},
		{"UPDATE ... FROM", `UPDATE t SET "$doc" = ? FROM u`, -1, "never from another collection"},

		{"DELETE ... USING", `DELETE FROM t USING u WHERE t.a = u.a`, -1, "never by a join"},
		{"mutation ORDER BY without LIMIT", `DELETE FROM t WHERE a = 1 ORDER BY a`, -1, "ORDER BY requires LIMIT"},
		{"mutation OFFSET", `DELETE FROM t WHERE a = 1 LIMIT 5 OFFSET 1`, -1, "does not support OFFSET"},
		{"a DELETE target alias", `DELETE FROM t AS x WHERE x.a = 1`, -1, "nothing to qualify"},
		{"a bare INSERT target alias", `INSERT INTO t x VALUES (?)`, -1, "VALUES or SELECT"},

		{"MERGE", `MERGE INTO t USING u ON (t.a = u.a)`, 0, "MERGE"},
		{"REPLACE", `REPLACE INTO t VALUES ('k', ?)`, 0, "REPLACE"},
	})
}

func TestInsertConflictUpdateASTAndParameterAccounting(t *testing.T) {
	const source = `
		INSERT INTO employees (id, Name, note) VALUES (?, ?, ?)
		ON CONFLICT DO UPDATE SET
			Name = eXcLuDeD.Name,
			note = ?,
			active = TRUE,
			score = 7,
			label = 'ready',
			optional = NULL
		RETURNING id`
	statement, err := ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	insert := statement.Insert
	if insert == nil || insert.OnConflictDoNothing ||
		insert.OnConflictUpdate == nil || !insert.HasConflictAction() {
		t.Fatalf("conflict action = %#v", insert)
	}
	if want := strings.Index(source, "ON CONFLICT"); insert.OnConflictPos != want {
		t.Fatalf("OnConflictPos = %d, want %d", insert.OnConflictPos, want)
	}
	update := insert.OnConflictUpdate
	if update.WholeDocument() || len(update.Assignments) != 6 {
		t.Fatalf("conflict update = %#v", update)
	}
	wantKinds := []OperandKind{
		OperandExcluded, OperandParam, OperandBool, OperandNumber, OperandString,
		OperandNull,
	}
	for i := range update.Assignments {
		if update.Assignments[i].Value.Kind != wantKinds[i] {
			t.Fatalf("assignment %d = %#v, want kind %d",
				i, update.Assignments[i], wantKinds[i])
		}
	}
	if update.Assignments[0].Column != "Name" ||
		update.Assignments[0].Value.Text != "Name" ||
		update.Assignments[0].Value.Pos != strings.Index(source, "eXcLuDeD.Name") {
		t.Fatalf("case-sensitive EXCLUDED assignment = %#v", update.Assignments[0])
	}
	if statement.Params() != 4 {
		t.Fatalf("Params = %d, want 4", statement.Params())
	}

	doNothingSource := `INSERT INTO employees VALUES (?) ON /* authored */ CONFLICT DO NOTHING`
	doNothing, err := ParseStatement(doNothingSource)
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.Index(doNothingSource, "ON /* authored */"); doNothing.Insert.OnConflictPos != want {
		t.Fatalf(
			"comment-separated DO NOTHING OnConflictPos = %d, want %d",
			doNothing.Insert.OnConflictPos, want,
		)
	}

	whole, err := ParseStatement(
		`INSERT INTO employees VALUES (?) ON CONFLICT DO UPDATE SET "$doc" = excluded."$doc"`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if whole.Insert.OnConflictUpdate == nil ||
		!whole.Insert.OnConflictUpdate.WholeDocument() ||
		whole.Insert.OnConflictUpdate.Doc.Kind != OperandExcluded ||
		whole.Insert.OnConflictUpdate.Doc.Text != DocumentColumn {
		t.Fatalf("whole-document conflict update = %#v", whole.Insert.OnConflictUpdate)
	}
}

func TestInsertConflictUpdateUnsupportedFormsArePositioned(t *testing.T) {
	tests := []struct {
		sql    string
		marker string
	}{
		{
			`INSERT INTO t VALUES (?) ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value`,
			`(id)`,
		},
		{
			`INSERT INTO t VALUES (?) ON CONFLICT ON CONSTRAINT t_pkey DO UPDATE SET value = EXCLUDED.value`,
			`ON CONSTRAINT`,
		},
		{
			`INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET value = EXCLUDED.profile.name`,
			`.name`,
		},
		{
			`INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET value = EXCLUDED.profile ->> 'name'`,
			`->>`,
		},
		{
			`INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET value = EXCLUDED."$doc".name`,
			`"$doc"`,
		},
		{
			`INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET value = EXCLUDED."$doc" -> 'name'`,
			`"$doc"`,
		},
		{
			`INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET value = EXCLUDED."$doc"['name']`,
			`"$doc"`,
		},
		{
			`INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET value = EXCLUDED.value WHERE value = 'old'`,
			`WHERE`,
		},
	}
	for _, test := range tests {
		_, err := ParseStatement(test.sql)
		var unsupported *FeatureNotSupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("ParseStatement(%q) = %T %v, want *FeatureNotSupportedError",
				test.sql, err, err)
		}
		want := strings.Index(test.sql, test.marker)
		if unsupported.Pos != want {
			t.Fatalf("ParseStatement(%q) position = %d, want %d",
				test.sql, unsupported.Pos, want)
		}
	}

	const excludedCollision = `INSERT INTO excluded VALUES (?) ON CONFLICT DO UPDATE SET value = EXCLUDED.value`
	_, err := ParseStatement(excludedCollision)
	var ambiguousAlias *AmbiguousAliasError
	if !errors.As(err, &ambiguousAlias) ||
		ambiguousAlias.Pos != strings.LastIndex(excludedCollision, "EXCLUDED") {
		t.Fatalf(
			"excluded target collision = %T %+v, want positioned AmbiguousAliasError",
			err, ambiguousAlias,
		)
	}
}

func TestDeclaredColumnUpdateAssignments(t *testing.T) {
	statement, err := ParseStatement(`UPDATE employees SET department = 'platform', level = 7, note = NULL WHERE id = 'employee-0001'`)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Update == nil || len(statement.Update.Assignments) != 3 {
		t.Fatalf("assignments = %#v", statement.Update)
	}
	if statement.Update.Assignments[0].Column != "department" ||
		statement.Update.Assignments[1].Column != "level" ||
		statement.Update.Assignments[2].Column != "note" ||
		statement.Update.Assignments[2].Value.Kind != OperandNull {
		t.Fatalf("assignments = %#v", statement.Update.Assignments)
	}
	if _, err := ParseStatement(`SELECT id FROM employees WHERE note = NULL`); err == nil {
		t.Fatal("comparison accepted NULL after assignment-only support")
	}
}

func TestMutationAssignmentListsAreBounded(t *testing.T) {
	for _, prefix := range []string{
		`UPDATE t SET `,
		`INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET `,
	} {
		var source strings.Builder
		source.WriteString(prefix)
		for i := 0; i <= maxClauseItems; i++ {
			if i != 0 {
				source.WriteString(", ")
			}
			source.WriteByte('c')
			source.WriteString(strconv.Itoa(i))
			source.WriteString(" = 1")
		}
		_, err := ParseStatement(source.String())
		if err == nil || !strings.Contains(err.Error(), "at most 1024 columns") {
			t.Fatalf("oversized assignment list error = %v", err)
		}
	}
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
		{"column UNIQUE", `CREATE TABLE t (a STRING UNIQUE)`, -1, "UNIQUE is not supported"},
		{"table UNIQUE", `CREATE TABLE t (a STRING, UNIQUE (a))`, -1, "UNIQUE is not supported"},
		{"CHECK", `CREATE TABLE t (a STRING, CHECK (a > 1))`, -1, "CHECK is not supported"},
		{"REFERENCES", `CREATE TABLE t (a STRING, FOREIGN KEY (a) REFERENCES u (b))`, -1, "FOREIGN is not supported"},
		{"CREATE TABLE AS", `CREATE TABLE t AS SELECT a FROM u`, -1, "created empty"},
		{"a partial index", `CREATE INDEX ON t (a) WHERE b = 1`, -1, "cover every document"},
		{"an index method", `CREATE INDEX ON t (a) USING btree`, -1, "no method to choose"},
		{"an index direction", `CREATE INDEX ON t (a DESC)`, -1, "no direction"},
		{"a unique partial index", `CREATE UNIQUE INDEX ON t (a) WHERE b = 1`, -1, "cover every document"},
		{"a unique index method", `CREATE UNIQUE INDEX ON t (a) USING btree`, -1, "no method to choose"},
		{"a unique index direction", `CREATE UNIQUE INDEX ON t (a DESC)`, -1, "no direction"},
		{"a unique index collation", `CREATE UNIQUE INDEX ON t (a COLLATE c)`, -1, "COLLATE is not supported"},
		{"a unique index null order", `CREATE UNIQUE INDEX ON t (a NULLS FIRST)`, -1, "NULLS FIRST/LAST"},
		{"an index over the whole document", `CREATE INDEX ON t (*)`, -1, "must stand alone"},
		{"a duplicate index path", `CREATE INDEX ON t (a, a)`, -1, "named twice"},
		{"too many index paths", `CREATE INDEX ON t (a, b, c, d, e)`, -1, "at most 4"},
		{"ALTER DROP", `ALTER TABLE t DROP COLUMN a`, -1, "supports ADD COLUMN"},
		{"ALTER primary key", `ALTER TABLE t ADD COLUMN other TEXT PRIMARY KEY`, -1, "changing document identity"},
	})
}
