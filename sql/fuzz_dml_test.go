package sql

import (
	"errors"
	"testing"
	"unicode/utf8"
)

// FuzzParseStatement is [FuzzParseSQL] for the entry point that accepts every
// statement kind.
//
// It is a second target rather than a widening of the first because the two
// assert different invariants over different trees, and because the corpus a
// SELECT-shaped fuzzer builds is not the corpus that finds a bug in the VALUES
// list. What both share is the guarantee that matters: arbitrary bytes never
// panic and never hang. Termination is structural here for exactly the reason
// it is there — the lexer consumes a byte per token, every loop in this file
// advances on that stream or returns, and the only recursion is the predicate
// grammar's, which maxExprDepth bounds.
//
// The accepted-statement invariants are the ones a lowering pass relies on
// without re-checking: that the tagged union's pointer agrees with its kind,
// that an INSERT row agrees with its optional field list, and that the
// placeholder count matches the placeholders actually in the tree. That last
// one is a real hazard rather than a formality: a driver
// validates its argument count against Params before it binds anything, so a
// tree holding an ordinal past the end would index out of range at execution.
func FuzzParseStatement(f *testing.F) {
	seeds := []string{
		``,
		`INSERT INTO t VALUES (?)`,
		`INSERT INTO t VALUES (?) RETURNING id, *`,
		`INSERT INTO t ("$key", "$doc") VALUES ('k', {"a":1})`,
		`INSERT INTO t VALUES ({"id":"a"}), ('{"id":"b"}')`,
		`INSERT INTO t SELECT returning, returning, returning, returning, returning, returning, returning, returning FROM src WHERE`,
		`INSERT INTO t (id, active) VALUES ('a', TRUE), ('b', FALSE)`,
		`INSERT INTO t (id, name) VALUES (?, ?) ON CONFLICT DO UPDATE SET name = EXCLUDED.name`,
		`INSERT INTO t (id, n) VALUES (?, ?) ON CONFLICT DO UPDATE SET n = t.n + EXCLUDED.n + ?`,
		`INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET state = CASE WHEN t.ready = TRUE THEN EXCLUDED.state ELSE t.state END`,
		`INSERT INTO t VALUES (?) ON CONFLICT DO UPDATE SET "$doc" = EXCLUDED."$doc" RETURNING id`,
		`INSERT INTO t VALUES (`,
		`UPDATE t SET "$doc" = ?`,
		`UPDATE t SET "$doc" = ? WHERE a = 1 AND NOT b IS NULL`,
		`UPDATE t SET "$doc" = ? WHERE "$key" = ?`,
		`UPDATE t SET a.b = 1`,
		`UPDATE t SET n = n + ? * 2, label = label || '!' WHERE id = ?`,
		`UPDATE t SET a = CASE WHEN ready = TRUE THEN b ELSE a END`,
		`DELETE FROM t`,
		`DELETE FROM t WHERE "$key" IN ('a', 'b', ?)`,
		`DELETE FROM t WHERE a @> {"k": [1, null]}`,
		`DELETE FROM t WHERE "$key" = 'a' OR b = 1`,
		`VALUES ('standalone query expression')`,
		`CREATE TABLE t`,
		`CREATE TABLE t (a STRING PRIMARY KEY, b INTEGER NOT NULL, c ANY)`,
		`CREATE TABLE t (a STRING, b NUMBER, PRIMARY KEY (a, b))`,
		`CREATE TABLE IF NOT EXISTS t (a VARCHAR(255))`,
		`CREATE INDEX ON t (a)`,
		`CREATE INDEX n ON t (a.b[0], c)`,
		`CREATE INDEX IF NOT EXISTS ON t (`,
		`CREATE UNIQUE INDEX ON t (email)`,
		`CREATE UNIQUE INDEX by_tenant_email ON t (tenant, profile.email)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS by_email ON t (email)`,
		`CREATE UNIQUE INDEX ON t (a DESC)`,
		`TRUNCATE t`,
		`TRUNCATE TABLE "events"`,
		`DROP INDEX by_age`,
		`DROP INDEX IF EXISTS by_age ON users`,
		`SELECT DISTINCT ALL a FROM t`,
		`SELECT a FROM t WHERE b = ?`,
		`MERGE INTO t`,
		"\x00\x80\xff",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, src string) {
		var p Parser
		var stmt Statement
		err := p.ParseStatement(&stmt, src)
		if !utf8.ValidString(src) && err == nil {
			t.Fatal("ParseStatement accepted invalid UTF-8")
		}
		if err != nil {
			var parseErr *ParseError
			if !errors.As(err, &parseErr) {
				t.Fatalf("ParseStatement returned %T, want *ParseError", err)
			}
			if parseErr.Pos < 0 || parseErr.Pos > len(src) {
				t.Fatalf("error offset %d is outside [0, %d]", parseErr.Pos, len(src))
			}
			if parseErr.Line < 1 || parseErr.Col < 1 {
				t.Fatalf("error position %d:%d is not 1-based", parseErr.Line, parseErr.Col)
			}
			if parseErr.Msg == "" {
				t.Fatal("error carries no message")
			}
			_ = parseErr.Error()
			if stmt != (Statement{}) {
				t.Fatal("a rejected statement left fields behind")
			}
			return
		}
		checkAnyStatement(t, &stmt)

		// KindOf routes without parsing, so it must agree with the parse. A
		// disagreement would send a mutation to Query or a SELECT to Exec, and
		// the failure would be a wrong entry point rather than a wrong answer.
		if want := stmt.Kind; KindOf(src).IsQuery() != want.IsQuery() {
			t.Fatalf("KindOf said IsQuery=%v and the parse produced %v", KindOf(src).IsQuery(), want)
		}

		// A second parse into the same warmed Parser must produce the same
		// tree, which is where a stale arena or a scratch buffer that outlived
		// its statement would show.
		first := dumpAny(&stmt)
		if err := p.ParseStatement(&stmt, src); err != nil {
			t.Fatalf("reparse of an accepted statement failed: %v", err)
		}
		if second := dumpAny(&stmt); second != first {
			t.Fatalf("reparse differs:\nfirst  %s\nsecond %s", first, second)
		}
	})
}

// checkAnyStatement asserts what an accepted statement of any kind promises.
func checkAnyStatement(t *testing.T, s *Statement) {
	t.Helper()
	bodies := 0
	for _, present := range []bool{
		s.Select != nil, s.Insert != nil, s.Update != nil, s.Delete != nil,
		s.CreateTable != nil, s.CreateIndex != nil, s.DropTable != nil,
		s.Truncate != nil, s.DropIndex != nil, s.CreateView != nil,
		s.DropView != nil,
	} {
		if present {
			bodies++
		}
	}
	if bodies != 1 {
		t.Fatalf("an accepted statement carries %d bodies, want exactly 1", bodies)
	}
	if s.Table() == "" && s.Kind != KindDropIndex &&
		!(s.Kind == KindSelect && s.Select != nil && s.Select.Set != nil) {
		t.Fatal("an accepted statement names no collection")
	}
	switch s.Kind {
	case KindSelect:
		if s.Select == nil {
			t.Fatal("KindSelect with no SelectStmt")
		}
		checkStatementInvariants(t, s.Select)
	case KindInsert:
		if s.Insert == nil {
			t.Fatal("KindInsert with no InsertStmt")
		}
		checkInsert(t, s.Insert)
	case KindUpdate:
		if s.Update == nil {
			t.Fatal("KindUpdate with no UpdateStmt")
		}
		checkUpdate(t, s.Update)
	case KindDelete:
		if s.Delete == nil {
			t.Fatal("KindDelete with no DeleteStmt")
		}
		checkDelete(t, s.Delete)
	case KindCreateTable:
		if s.CreateTable == nil {
			t.Fatal("KindCreateTable with no CreateTableStmt")
		}
		checkCreateTable(t, s.CreateTable)
	case KindCreateIndex:
		if s.CreateIndex == nil {
			t.Fatal("KindCreateIndex with no CreateIndexStmt")
		}
		checkCreateIndex(t, s.CreateIndex)
	case KindDropTable:
		if s.DropTable == nil || s.DropTable.Table == "" {
			t.Fatal("KindDropTable with no table name")
		}
	case KindTruncate:
		if s.Truncate == nil || s.Truncate.Table == "" {
			t.Fatal("KindTruncate with no table name")
		}
	case KindDropIndex:
		if s.DropIndex == nil || s.DropIndex.Name == "" {
			t.Fatal("KindDropIndex with no index name")
		}
		if s.DropIndex.HasTable != (s.DropIndex.Table != "") {
			t.Fatal("KindDropIndex has inconsistent ON table state")
		}
	case KindCreateView:
		if s.CreateView == nil || s.CreateView.Name == "" ||
			s.CreateView.Query == nil || s.CreateView.QuerySQL == "" {
			t.Fatal("KindCreateView has an incomplete definition")
		}
		checkStatementInvariants(t, s.CreateView.Query)
	case KindDropView:
		if s.DropView == nil || s.DropView.Name == "" {
			t.Fatal("KindDropView with no view name")
		}
	default:
		t.Fatalf("unknown statement kind %d", s.Kind)
	}
}

func checkInsert(t *testing.T, s *InsertStmt) {
	t.Helper()
	if (s.Source == nil) == (len(s.Rows) == 0) {
		t.Fatal("an accepted INSERT must own exactly one of VALUES rows and a query source")
	}
	if s.Source != nil {
		if len(s.Columns) != 0 {
			t.Fatal("INSERT SELECT retained a VALUES column list")
		}
		checkStatementInvariants(t, s.Source)
	}
	seen := 0
	if s.Source != nil {
		seen = s.Source.Params
	}
	for i := range s.Rows {
		row := &s.Rows[i]
		wantValues := 1
		if len(s.Columns) != 0 {
			wantValues = len(s.Columns)
		}
		if len(row.Values) != wantValues {
			t.Fatalf("row %d has %d values, want %d", i, len(row.Values), wantValues)
		}
		for _, value := range row.Values {
			switch value.Kind {
			case OperandString, OperandNumber, OperandBool, OperandJSON:
			case OperandParam:
				seen++
			default:
				t.Fatalf("row %d has a value of kind %d", i, value.Kind)
			}
		}
	}
	if s.OnConflictDoNothing && s.OnConflictUpdate != nil {
		t.Fatal("INSERT carries two ON CONFLICT actions")
	}
	if update := s.OnConflictUpdate; update != nil {
		if len(update.Assignments) == 0 {
			if !update.WholeDocument() {
				t.Fatal("whole-document conflict update has an invalid source")
			}
		} else {
			scope := SelectStmt{From: []TableRef{
				{Name: s.Table, Alias: s.Table},
				{Name: "excluded", Alias: "excluded"},
			}}
			seenTargets := make(map[string]struct{}, len(update.Assignments))
			for i := range update.Assignments {
				assignment := &update.Assignments[i]
				if _, duplicate := seenTargets[assignment.Column]; duplicate {
					t.Fatalf("conflict target %q is assigned twice", assignment.Column)
				}
				seenTargets[assignment.Column] = struct{}{}
				if assignment.Expr != nil {
					if assignment.Value.Kind != OperandExpression {
						t.Fatalf("conflict expression assignment %d has value kind %d", i, assignment.Value.Kind)
					}
					seen += checkScalarInvariants(t, &scope, assignment.Expr, nil)
					continue
				}
				if assignment.Value.Kind == OperandExpression {
					t.Fatalf("conflict assignment %d has an expression marker without an expression", i)
				}
				switch assignment.Value.Kind {
				case OperandString, OperandNumber, OperandBool, OperandNull:
				case OperandParam:
					seen++
				case OperandExcluded:
					if assignment.Value.Text == DocumentColumn ||
						assignment.Value.Text == "$key" {
						t.Fatalf("conflict assignment reads reserved EXCLUDED source %q", assignment.Value.Text)
					}
				default:
					t.Fatalf("conflict assignment %d has value kind %d", i, assignment.Value.Kind)
				}
			}
		}
	}
	if seen != s.Params {
		t.Fatalf("INSERT reports %d placeholders and holds %d", s.Params, seen)
	}
	if s.Returning != nil {
		checkStatementInvariants(t, s.Returning)
		for i := range s.Returning.Columns {
			if s.Returning.Columns[i].Agg != AggNone {
				t.Fatalf("RETURNING column %d is aggregate kind %d",
					i, s.Returning.Columns[i].Agg)
			}
		}
	}
}

func checkUpdate(t *testing.T, s *UpdateStmt) {
	t.Helper()
	seen := 0
	if len(s.Assignments) != 0 {
		seenTargets := make(map[string]struct{}, len(s.Assignments))
		for i := range s.Assignments {
			assignment := &s.Assignments[i]
			if _, duplicate := seenTargets[assignment.Column]; duplicate {
				t.Fatalf("UPDATE target %q is assigned twice", assignment.Column)
			}
			seenTargets[assignment.Column] = struct{}{}
			if assignment.Expr != nil {
				if assignment.Value.Kind != OperandExpression {
					t.Fatalf("UPDATE expression assignment %d has value kind %d", i, assignment.Value.Kind)
				}
				seen += checkScalarInvariants(t, s.Filter, assignment.Expr, nil)
				continue
			}
			switch assignment.Value.Kind {
			case OperandString, OperandNumber, OperandBool, OperandNull:
			case OperandParam:
				seen++
			default:
				t.Fatalf("UPDATE assignment %d has value kind %d", i, assignment.Value.Kind)
			}
		}
	} else {
		switch s.Doc.Kind {
		case OperandString, OperandJSON:
		case OperandParam:
			seen++
		default:
			t.Fatalf("the assigned document has kind %d", s.Doc.Kind)
		}
	}
	if s.Filter != nil {
		seen += checkFilter(t, s.Filter)
	}
	for _, term := range s.OrderBy {
		if term.Path == nil {
			t.Fatal("UPDATE ORDER BY has no path")
		}
	}
	if s.Limit != nil && s.Limit.Kind == OperandParam {
		seen++
	}
	if s.Returning != nil {
		checkStatementInvariants(t, s.Returning)
		for i := range s.Returning.Columns {
			if s.Returning.Columns[i].Agg != AggNone {
				t.Fatalf("RETURNING column %d is aggregate kind %d",
					i, s.Returning.Columns[i].Agg)
			}
		}
	}
	if seen != s.Params {
		t.Fatalf("UPDATE reports %d placeholders and holds %d", s.Params, seen)
	}
}

func checkDelete(t *testing.T, s *DeleteStmt) {
	t.Helper()
	seen := 0
	if s.Filter != nil {
		seen += checkFilter(t, s.Filter)
	}
	for _, term := range s.OrderBy {
		if term.Path == nil {
			t.Fatal("DELETE ORDER BY has no path")
		}
	}
	if s.Limit != nil && s.Limit.Kind == OperandParam {
		seen++
	}
	if s.Returning != nil {
		checkStatementInvariants(t, s.Returning)
		for i := range s.Returning.Columns {
			if s.Returning.Columns[i].Agg != AggNone {
				t.Fatalf("RETURNING column %d is aggregate kind %d",
					i, s.Returning.Columns[i].Agg)
			}
		}
	}
	if s.All && s.Filter != nil && s.Filter.Where != nil {
		t.Fatal("a DELETE marked as acting on everything carries a condition")
	}
	if seen != s.Params {
		t.Fatalf("DELETE reports %d placeholders and holds %d", s.Params, seen)
	}
}

// checkFilter asserts that a DML statement's synthetic SELECT is the shape the
// lowering expects: exactly one COUNT(*) column over exactly one collection.
// Anything else would make the filter extract a column nothing reads, or bind a
// path to a range variable that does not exist.
func checkFilter(t *testing.T, s *SelectStmt) int {
	t.Helper()
	if len(s.From) != 1 {
		t.Fatalf("a DML filter reads %d collections, want 1", len(s.From))
	}
	if len(s.Columns) != 1 || s.Columns[0].Agg != AggCount || s.Columns[0].Path != nil {
		t.Fatal("a DML filter does not project the synthetic COUNT(*)")
	}
	if s.GroupBy != nil || s.Having != nil || s.OrderBy != nil || s.Limit != nil || s.Offset != nil {
		t.Fatal("a DML filter carries a clause the grammar refuses")
	}
	if s.Where == nil {
		return 0
	}
	return checkExpr(t, s, s.Where, false)
}

func checkCreateTable(t *testing.T, s *CreateTableStmt) {
	t.Helper()
	for i := range s.Columns {
		column := &s.Columns[i]
		if column.Path == nil || len(column.Path.Segments) == 0 {
			t.Fatalf("column %d names no path", i)
		}
		if column.Type == 0 {
			t.Fatalf("column %d has no type", i)
		}
		if column.Type&^TypeAll != 0 {
			t.Fatalf("column %d has unknown type bits %#x", i, column.Type)
		}
	}
	for i, key := range s.PrimaryKey {
		if key == nil || len(key.Segments) == 0 {
			t.Fatalf("primary key path %d names no path", i)
		}
	}
	if len(s.PrimaryKey) > maxIndexColumns {
		t.Fatalf("primary key names %d paths, past the bound", len(s.PrimaryKey))
	}
}

func checkCreateIndex(t *testing.T, s *CreateIndexStmt) {
	t.Helper()
	if len(s.Paths) == 0 {
		t.Fatal("an accepted CREATE INDEX names no path")
	}
	if len(s.Paths) > maxIndexColumns {
		t.Fatalf("an index names %d paths, past the bound", len(s.Paths))
	}
	for i, path := range s.Paths {
		if path == nil || len(path.Segments) == 0 {
			t.Fatalf("index path %d names no path", i)
		}
		// A schema field and an index column are both handed to
		// vibejson.CompilePointer, which refuses anything without a leading
		// '/', so the pointer rendering has to produce one for every path the
		// grammar accepts.
		if pointer := string(path.AppendPointer(nil)); len(pointer) == 0 || pointer[0] != '/' {
			t.Fatalf("index path %d renders as %q, which is not a JSON Pointer", i, pointer)
		}
	}
	if s.HasName && s.Name == "" {
		t.Fatal("an index reports a name it does not have")
	}
}
