package vibesql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// Given a database read through database/sql and the same query expressed with
// the native builder API, when both run over one corpus, then every row and
// every cell agrees — the driver is a front end and adds no semantics of its
// own. A second battery checks the parts database/sql owns rather than the
// engine: the value mapping, the placeholder contract, the read-only refusals,
// the per-connection isolation, and the DSN forms.

// --- fixtures --------------------------------------------------------------

// corpus is the document set every differential runs over. It carries explicit
// nulls, absent fields, strings, bools, containers, an integer past float64's
// mantissa, and a non-integer number, because those are the shapes the value
// mapping has a decision to make about.
var corpus = []string{
	`{"id":1,"name":"amy","age":30,"tier":"pro","tags":["a","b"],"meta":{"x":1}}`,
	`{"id":2,"name":"bob","age":21,"tier":"free","tags":[],"meta":{"x":2}}`,
	`{"id":3,"name":"cy","age":null,"tier":"pro"}`,
	`{"id":4,"name":"","age":17,"tier":"free","big":9007199254740993}`,
	`{"id":5,"name":"dee","age":45,"big":9007199254740992,"ratio":0.1}`,
	`{"id":6,"tier":"pro","age":30,"flag":true}`,
	`{"id":7,"name":"eve","age":21,"tier":"free","flag":false,"ratio":2.5}`,
}

// memoryDatabase builds a heap database holding the corpus under the given
// collection names, and attaches it under a name unique to the test.
func memoryDatabase(t testing.TB, collections map[string][]string) *sql.DB {
	t.Helper()
	db := &store.Database{}
	for name, docs := range collections {
		collection, err := db.CreateCollection(name, store.Options{})
		if err != nil {
			t.Fatalf("CreateCollection(%q): %v", name, err)
		}
		for i, doc := range docs {
			if _, err := collection.Put(strconv.Itoa(i+1), []byte(doc)); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
	}
	name := fmt.Sprintf("t%p", db)
	dsn, err := Attach(name, db)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	t.Cleanup(func() { Detach(name) })
	handle, err := sql.Open("vibejson", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	return handle
}

// segmentOf builds the Segment the native-builder side of the differential runs
// over, in the same document order as the heap collection.
func segmentOf(t testing.TB, docs []string) *store.Segment {
	t.Helper()
	set := &store.Segment{}
	for _, doc := range docs {
		if _, err := set.Append([]byte(doc)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	return set
}

// --- the differential ------------------------------------------------------

// scanRows renders a *sql.Rows the way builderRows renders a query.Result, so
// the two are comparable as text. Every value is scanned into an any, which is
// where the driver's type mapping shows through.
func scanRows(t testing.TB, rows *sql.Rows) string {
	t.Helper()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for rows.Next() {
		cells := make([]any, len(columns))
		into := make([]any, len(columns))
		for i := range cells {
			into[i] = &cells[i]
		}
		if err := rows.Scan(into...); err != nil {
			t.Fatal(err)
		}
		for _, cell := range cells {
			b.WriteString(renderScanned(cell))
			b.WriteByte('|')
		}
		b.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// renderScanned turns a scanned driver value into the JSON text the engine's
// Cell.JSON would have produced for it, which is what makes the two sides
// comparable without the comparison itself knowing the mapping.
func renderScanned(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case bool:
		if x {
			return "true"
		}
		return "false"
	case int64:
		return strconv.FormatInt(x, 10)
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("<%T %v>", v, v)
	}
}

// builderRows renders a query.Result the way scanRows renders a *sql.Rows.
//
// A string cell is quoted, because the driver hands strings back decoded and
// the engine's JSON keeps them encoded; quoting the decoded text with
// encoding/json is an independent re-encoding rather than a copy of the
// engine's bytes.
func builderRows(t testing.TB, r query.Result) string {
	t.Helper()
	var b strings.Builder
	for row := 0; row < r.RowCount; row++ {
		for _, column := range r.Columns {
			cell := column.Cells[row]
			if text, ok := cell.Text(); ok {
				b.WriteString(text)
			} else {
				b.Write(cell.JSON())
			}
			b.WriteByte('|')
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// TestDriverMatchesBuilder is the differential: every statement must return
// exactly the rows, in the same order, that the equivalent builder query does.
func TestDriverMatchesBuilder(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	set := segmentOf(t, corpus)
	cases := []struct {
		sql     string
		args    []any
		builder *query.Query
	}{
		{sql: `SELECT name FROM users`, builder: query.Select(query.Path("name"))},
		{sql: `SELECT id, name, age FROM users`,
			builder: query.Select(query.Path("id"), query.Path("name"), query.Path("age"))},
		{sql: `SELECT tags, meta FROM users`,
			builder: query.Select(query.Path("tags"), query.Path("meta"))},
		{sql: `SELECT big, ratio, flag FROM users`,
			builder: query.Select(query.Path("big"), query.Path("ratio"), query.Path("flag"))},
		{sql: `SELECT name FROM users WHERE age >= 21`,
			builder: query.Select(query.Path("name")).Where(query.Cmp("age", query.Ge, 21))},
		{sql: `SELECT name FROM users WHERE age >= ?`, args: []any{21},
			builder: query.Select(query.Path("name")).Where(query.Cmp("age", query.Ge, 21))},
		{sql: `SELECT name FROM users WHERE tier = ?`, args: []any{"pro"},
			builder: query.Select(query.Path("name")).Where(query.Cmp("tier", query.Eq, "pro"))},
		{sql: `SELECT name FROM users WHERE age IN (21, 30)`,
			builder: query.Select(query.Path("name")).Where(query.In("age", 21, 30))},
		{sql: `SELECT name FROM users WHERE age IS NULL`,
			builder: query.Select(query.Path("name")).Where(query.IsNull("age"))},
		{sql: `SELECT name FROM users WHERE big IS MISSING`,
			builder: query.Select(query.Path("name")).Where(query.Not(query.Exists("big")))},
		{sql: `SELECT id FROM users WHERE meta @> {"x":1}`,
			builder: query.Select(query.Path("id")).Where(query.Contains("meta", `{"x":1}`))},
		{sql: `SELECT id FROM users WHERE big = 9007199254740993`,
			builder: query.Select(query.Path("id")).
				Where(query.Cmp("big", query.Eq, query.Number("9007199254740993")))},
		{sql: `SELECT name, age FROM users ORDER BY age DESC, id ASC`,
			builder: query.Select(query.Path("name"), query.Path("age")).
				OrderBy("age", query.Desc).OrderBy("id", query.Asc)},
		{sql: `SELECT name FROM users ORDER BY id LIMIT 3`,
			builder: query.Select(query.Path("name")).OrderBy("id", query.Asc).Limit(3)},
		{sql: `SELECT COUNT(*), COUNT(age), SUM(age), AVG(age), MIN(age), MAX(age) FROM users`,
			builder: query.Select(query.Count(), query.Count("age"), query.Sum("age"),
				query.Avg("age"), query.Min("age"), query.Max("age"))},
		{sql: `SELECT tier, COUNT(*), SUM(age) FROM users GROUP BY tier ORDER BY tier`,
			builder: query.Select(query.Path("tier"), query.Count(), query.Sum("age")).
				GroupBy("tier").OrderBy("tier", query.Asc)},
		{sql: `SELECT id FROM users WHERE NOT (age = 21) ORDER BY id`,
			// The engine's own NOT keeps a null age; SQL drops it, so the
			// builder twin has to spell the three-valued reading out.
			builder: query.Select(query.Path("id")).
				Where(query.And(query.Not(query.IsNull("age")), query.Not(query.Cmp("age", query.Eq, 21)))).
				OrderBy("id", query.Asc)},
		{sql: `SELECT meta.x FROM users`, builder: query.Select(query.Path("meta.x"))},
		{sql: `SELECT tags[0] FROM users`, builder: query.Select(query.Path("/tags/0"))},
	}
	for _, tc := range cases {
		rows, err := db.Query(tc.sql, tc.args...)
		if err != nil {
			t.Fatalf("Query(%q): %v", tc.sql, err)
		}
		got := scanRows(t, rows)
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		result, err := tc.builder.Run(query.FromSegment(set))
		if err != nil {
			t.Fatalf("builder for %q: %v", tc.sql, err)
		}
		if want := builderRows(t, result); got != want {
			t.Fatalf("%q:\n got %s\nwant %s", tc.sql, got, want)
		}
	}
}

// TestDriverPreparedMatchesAdHoc asserts the prepared and unprepared paths
// answer identically over many bindings. They take different code paths — one
// caches a plan and re-lowers, the other builds a statement per call — and a
// divergence between them would be a caching bug invisible to a single-shot
// test.
func TestDriverPreparedMatchesAdHoc(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	const src = `SELECT id, name FROM users WHERE age >= ? AND tier = ? ORDER BY id`
	prepared, err := db.Prepare(src)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for _, age := range []int{0, 17, 21, 30, 46} {
		for _, tier := range []string{"pro", "free", "none"} {
			rows, err := prepared.Query(age, tier)
			if err != nil {
				t.Fatal(err)
			}
			got := scanRows(t, rows)
			_ = rows.Close()

			rows, err = db.Query(src, age, tier)
			if err != nil {
				t.Fatal(err)
			}
			want := scanRows(t, rows)
			_ = rows.Close()

			if got != want {
				t.Fatalf("age=%d tier=%q: prepared %q, ad-hoc %q", age, tier, got, want)
			}
		}
	}
}

// --- the value mapping -----------------------------------------------------

// TestDriverValueMapping pins each JSON kind to the driver.Value it becomes,
// because the mapping is a contract callers write Scan destinations against.
func TestDriverValueMapping(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"v": {
		`{"k":"null","v":null}`,
		`{"k":"absent"}`,
		`{"k":"true","v":true}`,
		`{"k":"int","v":42}`,
		`{"k":"bigint","v":9007199254740993}`,
		`{"k":"huge","v":123456789012345678901234567890}`,
		`{"k":"float","v":0.1}`,
		`{"k":"negexp","v":-1.5e-3}`,
		`{"k":"string","v":"hi"}`,
		`{"k":"empty","v":""}`,
		`{"k":"escaped","v":"a\"b"}`,
		`{"k":"array","v":[1,2]}`,
		`{"k":"object","v":{"a":1}}`,
	}})
	rows, err := db.Query(`SELECT k, v FROM v`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := map[string]struct {
		kind string
		text string
	}{
		"null":    {"<nil>", ""},
		"absent":  {"<nil>", ""},
		"true":    {"bool", "true"},
		"int":     {"int64", "42"},
		"bigint":  {"int64", "9007199254740993"},
		"huge":    {"[]uint8", "123456789012345678901234567890"},
		"float":   {"[]uint8", "0.1"},
		"negexp":  {"[]uint8", "-1.5e-3"},
		"string":  {"[]uint8", "hi"},
		"empty":   {"[]uint8", ""},
		"escaped": {"[]uint8", `a"b`},
		"array":   {"[]uint8", "[1,2]"},
		"object":  {"[]uint8", `{"a":1}`},
	}
	seen := 0
	for rows.Next() {
		var key []byte
		var value any
		if err := rows.Scan(&key, &value); err != nil {
			t.Fatal(err)
		}
		expect, ok := want[string(key)]
		if !ok {
			t.Fatalf("unexpected row %q", key)
		}
		if got := fmt.Sprintf("%T", value); got != expect.kind {
			t.Fatalf("%s scanned as %s, want %s", key, got, expect.kind)
		}
		if raw, ok := value.([]byte); ok && string(raw) != expect.text {
			t.Fatalf("%s scanned as %q, want %q", key, raw, expect.text)
		}
		seen++
	}
	if seen != len(want) {
		t.Fatalf("saw %d rows, want %d", seen, len(want))
	}
}

// TestDriverBigIntegerSurvivesTheRoundTrip is the reason numbers do not go
// through float64: two integers that differ past float64's 53-bit mantissa must
// still differ after a Scan. Routing them through float64 would collapse both
// onto 9007199254740992, which is a value the corpus also contains, so the
// failure would be a wrong answer rather than an error.
func TestDriverBigIntegerSurvivesTheRoundTrip(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"n": {
		`{"i":1,"v":9007199254740992}`,
		`{"i":2,"v":9007199254740993}`,
		`{"i":3,"v":-9007199254740993}`,
		`{"i":4,"v":123456789012345678901234567890}`,
	}})
	rows, err := db.Query(`SELECT i, v FROM n ORDER BY i`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := []string{"9007199254740992", "9007199254740993", "-9007199254740993",
		"123456789012345678901234567890"}
	for i := 0; rows.Next(); i++ {
		var index int64
		var digits string
		if err := rows.Scan(&index, &digits); err != nil {
			t.Fatal(err)
		}
		if digits != want[i] {
			t.Fatalf("row %d scanned %q, want %q", i, digits, want[i])
		}
		// An independent check that the digits are the exact value, not a
		// rounded rendering of it.
		if _, ok := new(big.Int).SetString(digits, 10); !ok {
			t.Fatalf("row %d scanned %q, which is not an exact integer", i, digits)
		}
	}
}

// TestDriverColumnTypes checks the schema metadata is the honest one: a
// schemaless projection has no single Go type and says so, and COUNT does.
func TestDriverColumnTypes(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	rows, err := db.Query(`SELECT COUNT(*), SUM(age), MIN(age) FROM users`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		name     string
		scan     string
		dbType   string
		nullable bool
	}{
		{"count(*)", "int64", "BIGINT", false},
		{"sum(age)", "interface {}", "NUMERIC", true},
		{"min(age)", "interface {}", "NUMERIC", true},
	}
	for i, w := range want {
		if got := types[i].Name(); got != w.name {
			t.Fatalf("column %d name %q, want %q", i, got, w.name)
		}
		if got := types[i].ScanType().String(); got != w.scan {
			t.Fatalf("column %d scan type %q, want %q", i, got, w.scan)
		}
		if got := types[i].DatabaseTypeName(); got != w.dbType {
			t.Fatalf("column %d database type %q, want %q", i, got, w.dbType)
		}
		nullable, ok := types[i].Nullable()
		if !ok || nullable != w.nullable {
			t.Fatalf("column %d nullable (%v,%v), want (%v,true)", i, nullable, ok, w.nullable)
		}
	}

	projection, err := db.Query(`SELECT name FROM users`)
	if err != nil {
		t.Fatal(err)
	}
	defer projection.Close()
	types, err = projection.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if got := types[0].ScanType().String(); got != "interface {}" {
		t.Fatalf("a schemaless projection reports scan type %q; it has no single type", got)
	}
}

// TestDriverContainerScansAsJSON asserts a projected object or array is exactly
// the document's bytes, so json.Unmarshal reads it without the driver having
// invented a wrapper type.
func TestDriverContainerScansAsJSON(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	rows, err := db.Query(`SELECT meta FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("no row")
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]int
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("projected object %q does not unmarshal: %v", raw, err)
	}
	if decoded["x"] != 1 {
		t.Fatalf("projected object decoded to %v", decoded)
	}
}

// --- routing and refusals ----------------------------------------------------

// TestDriverRefusesTheWrongEntryPoint asserts the refusals are refusals and not
// silent no-ops. Each of these pairs a statement with the database/sql entry
// point that cannot execute it, and each has a silent failure mode if it is
// accepted: an Exec'd SELECT runs and discards its rows while reporting nothing
// happened, and a Queried mutation would have to report an empty result.
func TestDriverRefusesTheWrongEntryPoint(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	if _, err := db.Exec(`SELECT name FROM users`); err == nil {
		t.Fatal("Exec of a SELECT succeeded; it should say to use Query")
	}
	for _, src := range []string{
		`INSERT INTO users VALUES ('k', {"a":1})`,
		`UPDATE users SET "$doc" = ?`,
		`DELETE FROM users`,
		`CREATE TABLE t (a STRING)`,
	} {
		if _, err := db.Query(src); err == nil {
			t.Fatalf("Query of %q was accepted; it returns no rows", src)
		}
	}
	// The statements the dialect genuinely has no execution for stay refused at
	// parse time, through either entry point.
	for _, src := range []string{
		`UPDATE users SET age = 1`,
		`INSERT INTO users (name) VALUES (?)`,
		`DROP TABLE users`,
	} {
		if _, err := db.Exec(src); err == nil {
			t.Fatalf("%q was accepted; the grammar refuses it", src)
		}
	}
}

// TestDriverPlaceholderCount asserts database/sql's own arity check fires,
// which it can only do because NumInput reports the real count.
func TestDriverPlaceholderCount(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	stmt, err := db.Prepare(`SELECT id FROM users WHERE age >= ? AND tier = ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, args := range [][]any{nil, {21}, {21, "pro", "extra"}} {
		if _, err := stmt.Query(args...); err == nil {
			t.Fatalf("binding %d arguments to a 2-placeholder statement succeeded", len(args))
		}
	}
	rows, err := stmt.Query(21, "pro")
	if err != nil {
		t.Fatalf("binding 2 arguments: %v", err)
	}
	_ = rows.Close()
}

// TestDriverNamedParametersAreRefused asserts the refusal is explicit. Silently
// binding a named argument positionally would compare against the wrong
// literal.
func TestDriverNamedParametersAreRefused(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	_, err := db.Query(`SELECT id FROM users WHERE age >= ?`, sql.Named("age", 21))
	if err == nil {
		t.Fatal("a named parameter was accepted")
	}
	if !strings.Contains(err.Error(), "named parameters") {
		t.Fatalf("err = %v, want a message naming the refusal", err)
	}
}

// TestDriverNullArgument asserts a nil argument reads as SQL NULL: never TRUE,
// so the comparison keeps nothing, and never FALSE, so its negation keeps
// nothing either.
func TestDriverNullArgument(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	for _, src := range []string{
		`SELECT id FROM users WHERE age = ?`,
		`SELECT id FROM users WHERE NOT (age = ?)`,
	} {
		rows, err := db.Query(src, nil)
		if err != nil {
			t.Fatalf("%q: %v", src, err)
		}
		if got := scanRows(t, rows); got != "" {
			t.Fatalf("%q with a NULL argument returned %q; SQL keeps nothing", src, got)
		}
		_ = rows.Close()
	}
}

// TestDriverUnknownCollection asserts a FROM naming a collection the connection
// does not hold is an error rather than an empty result, which would read as
// "no matching rows".
func TestDriverUnknownCollection(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	if _, err := db.Query(`SELECT id FROM missing`); err == nil {
		t.Fatal("a query over an unknown collection returned rows")
	}
}

// TestDriverParseErrorReachesTheCaller asserts the parser's positioned message
// survives the driver boundary, which is the whole reason the parser refuses
// early.
func TestDriverParseErrorReachesTheCaller(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	_, err := db.Query(`SELECT id FROM users WHERE name LIKE 'a%'`)
	if err == nil {
		t.Fatal("LIKE was accepted")
	}
	if !strings.Contains(err.Error(), "offset") {
		t.Fatalf("err = %v, want a positioned parse error", err)
	}
}

// --- joins ------------------------------------------------------------------

// TestDriverInnerJoin runs the shape the driver exists for, checked against the
// nested loop written out here rather than derived.
func TestDriverInnerJoin(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{
		"users": {
			`{"id":"1","name":"amy"}`,
			`{"id":"2","name":"bob"}`,
			`{"id":"3","name":"cy"}`,
		},
		"orders": {
			`{"user_id":"1","total":10}`,
			`{"user_id":"1","total":20}`,
			`{"user_id":"2","total":30}`,
			`{"user_id":"9","total":40}`,
		},
	})
	for _, tc := range []struct {
		sql  string
		want string
	}{
		// One row per matching pair, in driving then joined order. cy has no
		// order and is dropped; the order for user 9 has no user and is too.
		{`SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id`,
			"amy|10|\namy|20|\nbob|30|\n"},
		// A joined column need not be projected for the cardinality to hold:
		// COUNT(*) counts pairs, which is what SQL counts.
		{`SELECT COUNT(*) FROM users u JOIN orders o ON o.user_id = u.id`, "3|\n"},
		{`SELECT u.name FROM users u JOIN orders o ON o.user_id = u.id`,
			"amy|\namy|\nbob|\n"},
		// A WHERE over the joined collection: lowering moves it into the join
		// clause's own filter, which selects the same pairs.
		{`SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE o.total >= 20`,
			"amy|20|\nbob|30|\n"},
		{`SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id ` +
			`WHERE u.name = 'amy' AND o.total >= 20`, "amy|20|\n"},
		{`SELECT u.name, SUM(o.total) FROM users u JOIN orders o ON o.user_id = u.id ` +
			`GROUP BY u.name ORDER BY u.name`, "amy|30|\nbob|30|\n"},
		{`SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id ` +
			`ORDER BY o.total DESC LIMIT 2`, "bob|30|\namy|20|\n"},
		{`SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id ` +
			`ORDER BY o.total DESC OFFSET 1`, "amy|20|\namy|10|\n"},
		{`SELECT u.name, SUM(o.total) FROM users u JOIN orders o ON o.user_id = u.id ` +
			`GROUP BY u.name HAVING SUM(o.total) > 30 ORDER BY u.name`, ""},
	} {
		rows, err := db.Query(tc.sql)
		if err != nil {
			t.Fatalf("%q: %v", tc.sql, err)
		}
		if got := scanRows(t, rows); got != tc.want {
			t.Fatalf("%q returned %q, want %q", tc.sql, got, tc.want)
		}
		_ = rows.Close()
	}
}

// TestDriverPrimaryKeySemiJoin checks the shape the planner answers with the
// cheaper operator: joining on the joined collection's primary key matches at
// most one document, so with nothing outside the clause reading it, one row per
// pair and one row per driving row are the same rows.
func TestDriverPrimaryKeySemiJoin(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{
		"orders": {
			`{"id":1,"cust":"1","total":10}`,
			`{"id":2,"cust":"2","total":20}`,
			`{"id":3,"cust":"3","total":30}`,
			`{"id":4,"cust":"9","total":40}`,
		},
		"customers": {
			`{"tier":"pro"}`,
			`{"tier":"free"}`,
			`{"tier":"pro"}`,
		},
	})
	rows, err := db.Query(
		`SELECT o.id FROM orders o JOIN customers c ON c."$key" = o.cust ` +
			`WHERE c.tier = 'pro' ORDER BY o.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	// customers are keyed "1".."3"; only "1" and "3" are pro, and order 4
	// references a customer that does not exist.
	if got, want := scanRows(t, rows), "1|\n3|\n"; got != want {
		t.Fatalf("primary-key join returned %q, want %q", got, want)
	}
}

// TestDriverJoinRefusals asserts the shapes with no plan are refused rather
// than answered.
func TestDriverJoinRefusals(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{
		"orders":    {`{"id":1,"cust":"1"}`},
		"customers": {`{"tier":"pro"}`},
		"parts":     {`{"n":1}`},
	})
	for _, tc := range []struct {
		sql  string
		want string
	}{
		{`SELECT o.id FROM orders o LEFT JOIN customers c ON c.k = o.cust`, "join"},
		{`SELECT o.id, c.tier, p.n FROM orders o JOIN customers c ON c.k = o.cust ` +
			`JOIN parts p ON p.k = o.cust`, "only once"},
		{`SELECT o.id, p.n FROM orders o JOIN customers c ON c.k = o.cust ` +
			`JOIN parts p ON p.k = c.tier`, "chained join"},
		{`SELECT o.id, c.tier FROM orders o JOIN customers c ON c.k = o.cust ` +
			`WHERE o.id = 1 OR c.tier = 'pro'`, "two collections"},
	} {
		if _, err := db.Query(tc.sql); err == nil {
			t.Fatalf("%q was accepted; it has no plan", tc.sql)
		} else if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%q = %v, want a message naming %q", tc.sql, err, tc.want)
		}
	}
}

// TestDriverJoinNeedsADatabase asserts a durable single-collection connection
// refuses a join rather than resolving the second collection from a snapshot
// its driving side never saw.
func TestDriverJoinNeedsADatabase(t *testing.T) {
	db := durableDatabase(t, "orders", []string{`{"id":1,"cust":"1"}`})
	_, err := db.Query(`SELECT o.id FROM orders o JOIN customers c ON c."$key" = o.cust`)
	if err == nil {
		t.Fatal("a join over a single durable collection was accepted")
	}
	if !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("err = %v, want a message about the consistent snapshot", err)
	}
}

// --- the DSN ----------------------------------------------------------------

// durableDatabase creates a durable collection file holding docs and opens it
// through the path form of the DSN.
func durableDatabase(t testing.TB, name string, docs []string) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".vj")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, durable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range docs {
		if _, err := collection.Put(strconv.Itoa(i+1), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	handle, err := sql.Open("vibejson", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	return handle
}

// TestDriverFileDSN asserts a path opens the collection it names, that the
// collection's name is the file's, and that several connections share one
// handle — which they must, because a durable collection holds an exclusive
// writer lock on its file.
func TestDriverFileDSN(t *testing.T) {
	db := durableDatabase(t, "users", corpus)
	db.SetMaxOpenConns(4)
	rows, err := db.Query(`SELECT name FROM users WHERE age >= 21 ORDER BY age, id`)
	if err != nil {
		t.Fatal(err)
	}
	got := scanRows(t, rows)
	_ = rows.Close()
	if want := "bob|\neve|\namy|\nnull|\ndee|\n"; got != want {
		t.Fatalf("file DSN returned %q, want %q", got, want)
	}

	// A second, concurrent connection must work rather than fail on the
	// writer lock.
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rows, err := db.Query(`SELECT COUNT(*) FROM users`)
			if err != nil {
				errs[i] = err
				return
			}
			defer rows.Close()
			var n int64
			if rows.Next() {
				errs[i] = rows.Scan(&n)
			}
			if n != int64(len(corpus)) {
				errs[i] = fmt.Errorf("count = %d, want %d", n, len(corpus))
			}
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if _, err := db.Query(`SELECT id FROM other`); err == nil {
		t.Fatal("a file connection answered a query over a different collection")
	}
}

// TestDriverUnknownDSN asserts an unregistered memory source is named as such
// rather than treated as a path.
func TestDriverUnknownDSN(t *testing.T) {
	// sql.Open resolves the DSN eagerly rather than lazily, because a durable
	// collection's exclusive writer lock has to be taken once and shared by
	// every connection; there is nowhere later that could do it once.
	_, err := sql.Open("vibejson", "memory:nothing-is-attached-here")
	if !errors.Is(err, ErrUnknownSource) {
		t.Fatalf("sql.Open = %v, want ErrUnknownSource", err)
	}
	if _, err := sql.Open("vibejson", filepath.Join(t.TempDir(), "absent.vj")); err == nil {
		t.Fatal("a path to no file was accepted")
	}
}

// --- concurrency -------------------------------------------------------------

// TestDriverConcurrentConnections runs many queries at once over one database.
// It is the shape the per-connection Exec exists for: with -race, a shared
// workspace would be reported here.
func TestDriverConcurrentConnections(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	db.SetMaxOpenConns(8)
	stmt, err := db.Prepare(`SELECT id, name, age FROM users WHERE age >= ? ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	want := map[int]string{}
	for _, age := range []int{0, 17, 21, 30, 45} {
		rows, err := stmt.Query(age)
		if err != nil {
			t.Fatal(err)
		}
		want[age] = scanRows(t, rows)
		_ = rows.Close()
	}

	var wg sync.WaitGroup
	fail := make(chan string, 64)
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				age := []int{0, 17, 21, 30, 45}[(worker+i)%5]
				rows, err := stmt.Query(age)
				if err != nil {
					fail <- err.Error()
					return
				}
				var b strings.Builder
				columns, _ := rows.Columns()
				for rows.Next() {
					cells := make([]any, len(columns))
					into := make([]any, len(columns))
					for j := range cells {
						into[j] = &cells[j]
					}
					if err := rows.Scan(into...); err != nil {
						fail <- err.Error()
						_ = rows.Close()
						return
					}
					for _, cell := range cells {
						b.WriteString(renderScanned(cell))
						b.WriteByte('|')
					}
					b.WriteByte('\n')
				}
				_ = rows.Close()
				if b.String() != want[age] {
					fail <- fmt.Sprintf("age=%d got %q want %q", age, b.String(), want[age])
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(fail)
	for msg := range fail {
		t.Fatal(msg)
	}
}

// TestDriverScannedValuesSurviveTheNextQuery is the ownership check the []byte
// mapping depends on. A projected string borrows the collection or the
// execution workspace; database/sql copies it at Scan, and the copy has to
// still read correctly after the workspace behind it has been reused.
func TestDriverScannedValuesSurviveTheNextQuery(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	db.SetMaxOpenConns(1) // force every query onto one connection and one Exec
	rows, err := db.Query(`SELECT name FROM users ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for rows.Next() {
		// []byte rather than string, because a projection may be null and
		// database/sql refuses to convert NULL to a string.
		var name []byte
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		kept = append(kept, string(name))
	}
	_ = rows.Close()

	// Run several more queries on the same connection, which reuses the Exec
	// the strings above were read out of.
	for i := 0; i < 8; i++ {
		other, err := db.Query(`SELECT tier, meta FROM users ORDER BY id DESC`)
		if err != nil {
			t.Fatal(err)
		}
		_ = scanRows(t, other)
		_ = other.Close()
	}

	rows, err = db.Query(`SELECT name FROM users ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		var name []byte
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if string(name) != kept[i] {
			t.Fatalf("row %d was scanned as %q and now reads %q", i, kept[i], name)
		}
	}
}

// TestDriverContextCancellation asserts a cancelled context stops a query
// before it starts. The executor has no cancellation hook, so this is the whole
// of what the driver claims.
func TestDriverContextCancellation(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"users": corpus})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.QueryContext(ctx, `SELECT id FROM users`); err == nil {
		t.Fatal("a cancelled context did not stop the query")
	}
	if _, err := db.PrepareContext(ctx, `SELECT id FROM users`); err == nil {
		t.Fatal("a cancelled context did not stop the prepare")
	}
}

// TestDriverEmptyStringIsNotNull pins the one place a zero-length value could
// be mistaken for a missing one.
//
// database/sql clones a []byte into a *[]byte destination, and its clone of a
// nil slice is nil — which is also what it writes for SQL NULL. An empty JSON
// string whose bytes arrived as a nil slice would therefore be indistinguishable
// from an absent field to any caller that tests the destination for nil, which
// is an ordinary thing to write. So an empty string leaves this driver as a
// non-nil zero-length slice, and null leaves it as nil.
func TestDriverEmptyStringIsNotNull(t *testing.T) {
	db := memoryDatabase(t, map[string][]string{"e": {
		`{"k":1,"v":""}`,
		`{"k":2,"v":null}`,
		`{"k":3}`,
	}})
	rows, err := db.Query(`SELECT k, v FROM e ORDER BY k`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	wantNil := map[int64]bool{1: false, 2: true, 3: true}
	seen := 0
	for rows.Next() {
		var key int64
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			t.Fatal(err)
		}
		if (value == nil) != wantNil[key] {
			t.Fatalf("row %d scanned %#v; nil should mean null and only null", key, value)
		}
		if len(value) != 0 {
			t.Fatalf("row %d scanned %q, want an empty value", key, value)
		}
		seen++
	}
	if seen != 3 {
		t.Fatalf("saw %d rows, want 3", seen)
	}
}
