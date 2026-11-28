package query

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func mustRunScalarSQL(t testing.TB, docs []string, sql string) *Result {
	t.Helper()
	seg := FromSegment(mustSegment(t, docs...))
	stmt, err := PrepareStatement(sql)
	if err != nil {
		t.Fatalf("PrepareStatement(%q): %v", sql, err)
	}
	defer stmt.Release()
	var exec Exec
	exec.Options.Workers = 1
	if _, err := stmt.RunInto(&exec, seg, nil); err != nil {
		t.Fatalf("RunInto(%q): %v", sql, err)
	}
	return &exec.Result
}

func TestSQLScalarFuncWarmZeroAllocs(t *testing.T) {
	docs := make([]string, 32)
	for i := range docs {
		docs[i] = fmt.Sprintf(
			`{"id":%d,"name":"  Ada Lovelace %d  ","uname":"  Éclair Café %d  ","score":%d.75,"neg":-12.345,"ts":"2026-11-26T15:30:45Z","csv":"a,b,c","tags":["alpha","beta","gamma"],"meta":{"nested":{"k":"v%d"},"drop":null,"arr":[1,2,3]}}`,
			i+1, i+1, i+1, i+1, i+1,
		)
	}
	seg := FromSegment(mustSegment(t, docs...))

	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "string_ascii",
			sql: `SELECT LOWER(TRIM(name)), UPPER(TRIM(name)), LTRIM(name), RTRIM(name),
			             LENGTH(name), OCTET_LENGTH(name), SUBSTR(TRIM(name), 5, 4),
			             LEFT(TRIM(name), 3), RIGHT(TRIM(name), 2), REPLACE(TRIM(name), 'Ada', 'Grace'),
			             CONCAT('u:', id, ':', TRIM(name)), CONCAT_WS('-', 'u', id, NULL, 'ok'),
			             SPLIT_PART(csv, ',', 2), SPLIT_PART(csv, ',', -1), STRPOS(name, 'Love'),
			             REVERSE(csv), REPEAT('ab', 3), LPAD(csv, 8, '0'), RPAD(csv, 8, '.'),
			             INITCAP('hello world'), MD5(csv), ASCII('A'), CHR(65),
			             STARTS_WITH(csv, 'a,'), TRANSLATE(csv, 'ac', 'xz')
			      FROM docs`,
		},
		{
			name: "string_utf8",
			sql: `SELECT LOWER(TRIM(uname)), UPPER(TRIM(uname)), LENGTH(TRIM(uname)),
			             OCTET_LENGTH(TRIM(uname)), SUBSTR(TRIM(uname), 1, 6),
			             LEFT(TRIM(uname), 6), RIGHT(TRIM(uname), 4),
			             REVERSE(TRIM(uname)), LPAD('é', 4, 'ü'), RPAD('é', 4, 'ü'),
			             INITCAP('éclair café'), TRANSLATE('café', 'é', 'e')
			      FROM docs`,
		},
		{
			name: "math_numeric",
			sql: `SELECT ABS(neg), ABS(id), CEIL(neg), FLOOR(neg), ROUND(neg), ROUND(neg, 2),
			             ROUND(score, -1), TRUNC(neg), TRUNC(neg, 1), POWER(id, 2),
			             SQRT(score), MOD(id, 5), SIGN(neg), LN(score), LOG10(score),
			             LOG(2, score), EXP(1), PI()
			      FROM docs`,
		},
		{
			name: "conditional_cast",
			sql: `SELECT COALESCE(NULL, TRIM(name), 'x'), NULLIF(id, 5),
			             GREATEST(id, 10), LEAST(id, 10),
			             CAST(id AS TEXT), CAST(score AS NUMERIC)
			      FROM docs`,
		},
		{
			name: "timestamp_interval",
			sql: `SELECT DATE_TRUNC('day', ts), EXTRACT(YEAR FROM ts),
			             ts + '1 day 2 hours', ts - '30 minutes'
			      FROM docs`,
		},
		{
			name: "json_array_zero_alloc",
			sql: `SELECT JSON_BUILD_ARRAY(id, TRIM(name), NULL),
			             JSON_BUILD_OBJECT('id', id, 'name', TRIM(name)),
			             JSONB_BUILD_OBJECT('id', id, 'name', TRIM(name), 'id', id),
			             TO_JSON(TRIM(name)),
			             JSON_TYPEOF(meta), JSON_TYPEOF(tags), JSON_TYPEOF(id),
			             JSON_ARRAY_LENGTH(tags), CARDINALITY(tags), ARRAY_LENGTH(tags, 1),
			             ARRAY_TO_STRING(tags, '|'), STRING_TO_ARRAY(csv, ','),
			             JSON_EXTRACT_PATH(meta, 'nested', 'k'),
			             JSON_EXTRACT_PATH_TEXT(meta, 'nested', 'k'),
			             JSON_STRIP_NULLS(meta),
			             JSONB_PRETTY(meta),
			             JSONB_SET(meta, '{nested,k}', '"updated"')
			      FROM docs`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := PrepareStatement(tc.sql)
			if err != nil {
				t.Fatalf("PrepareStatement: %v", err)
			}
			defer stmt.Release()
			var exec Exec
			exec.Options.Workers = 1

			// Warm up arenas and caches.
			for i := 0; i < 3; i++ {
				if _, err := stmt.RunInto(&exec, seg, nil); err != nil {
					t.Fatalf("warm RunInto: %v", err)
				}
			}

			allocs := testing.AllocsPerRun(50, func() {
				if _, err := stmt.RunInto(&exec, seg, nil); err != nil {
					t.Fatalf("RunInto: %v", err)
				}
			})
			if allocs != 0 {
				t.Errorf("%s: warm RunInto allocated %.1f allocs/run, want 0", tc.name, allocs)
			}
		})
	}

	// Verify warm regex cache lookup is 0 allocs even under -race
	// (stdlib regexp.Regexp.MatchString uses sync.Pool which randomly drops under -race).
	t.Run("regexp_cache_zero_alloc", func(t *testing.T) {
		if _, err := cachedPGRegexp("lovelace", "i"); err != nil {
			t.Fatalf("cachedPGRegexp: %v", err)
		}
		allocs := testing.AllocsPerRun(50, func() {
			re, err := cachedPGRegexp("lovelace", "i")
			if err != nil || re == nil {
				t.Fatalf("cachedPGRegexp: %v", err)
			}
		})
		if allocs != 0 {
			t.Errorf("warm cachedPGRegexp allocated %.1f allocs/run, want 0", allocs)
		}
	})
}

func TestSQLScalarFuncMultiRowJSONIsolation(t *testing.T) {
	// Regression test: JSON-producing scalar functions previously returned direct:true
	// referencing evalArena, which caused subsequent rows to overwrite earlier rows' cells.
	docs := make([]string, 16)
	for i := range docs {
		docs[i] = fmt.Sprintf(`{"id":%d,"name":"user_%02d","csv":"x%d,y%d","meta":{"k":"v%d","drop":null}}`, i, i, i, i, i)
	}
	res := mustRunScalarSQL(t, docs, `
		SELECT JSON_BUILD_OBJECT('id', id, 'name', name),
		       JSON_BUILD_ARRAY(id, name),
		       TO_JSON(name),
		       STRING_TO_ARRAY(csv, ','),
		       JSON_EXTRACT_PATH(meta, 'k'),
		       JSON_STRIP_NULLS(meta),
		       JSONB_SET(meta, '{k}', '"new"')
		FROM docs
		ORDER BY id
	`)
	if res.RowCount != len(docs) {
		t.Fatalf("RowCount = %d, want %d", res.RowCount, len(docs))
	}
	for i := 0; i < res.RowCount; i++ {
		wantObj := fmt.Sprintf(`{"id":%d,"name":"user_%02d"}`, i, i)
		if got := res.Columns[0].Cells[i].String(); got != wantObj {
			t.Fatalf("row %d json_build_object = %s, want %s", i, got, wantObj)
		}
		wantArr := fmt.Sprintf(`[%d,"user_%02d"]`, i, i)
		if got := res.Columns[1].Cells[i].String(); got != wantArr {
			t.Fatalf("row %d json_build_array = %s, want %s", i, got, wantArr)
		}
		wantToJSON := fmt.Sprintf(`"user_%02d"`, i)
		if got := res.Columns[2].Cells[i].String(); got != wantToJSON {
			t.Fatalf("row %d to_json = %s, want %s", i, got, wantToJSON)
		}
		wantSplit := fmt.Sprintf(`["x%d","y%d"]`, i, i)
		if got := res.Columns[3].Cells[i].String(); got != wantSplit {
			t.Fatalf("row %d string_to_array = %s, want %s", i, got, wantSplit)
		}
		wantExtract := fmt.Sprintf(`"v%d"`, i)
		if got := res.Columns[4].Cells[i].String(); got != wantExtract {
			t.Fatalf("row %d json_extract_path = %s, want %s", i, got, wantExtract)
		}
		wantStrip := fmt.Sprintf(`{"k":"v%d"}`, i)
		if got := res.Columns[5].Cells[i].String(); got != wantStrip {
			t.Fatalf("row %d json_strip_nulls = %s, want %s", i, got, wantStrip)
		}
		wantSet := `{"k":"new","drop":null}`
		if got := res.Columns[6].Cells[i].String(); got != wantSet {
			t.Fatalf("row %d jsonb_set = %s, want %s", i, got, wantSet)
		}
	}
}

func TestSQLScalarFuncUTF8AndEdgeCases(t *testing.T) {
	docs := []string{
		`{"id":1,"s":"héllo 世界","emoji":"🚀🔥✨","empty":"","csv":"a,b,c"}`,
	}
	res := mustRunScalarSQL(t, docs, `
		SELECT LENGTH(s), OCTET_LENGTH(s),
		       SUBSTR(s, 1, 5), SUBSTR(s, 7, 2), SUBSTR(s, 100, 5),
		       LEFT(s, 2), LEFT(s, -3), RIGHT(s, 2), RIGHT(s, -6),
		       REVERSE(s), REVERSE(emoji),
		       LOWER('ÉCLAIR Ω'), UPPER('éclair ω'),
		       LPAD('é', 4, 'ü'), RPAD('é', 4, 'ü'),
		       INITCAP('éclair café'),
		       TRANSLATE('café 世界', 'é世', 'e'),
		       STRPOS(s, '世'),
		       SPLIT_PART('héllo', '', 2),
		       REPLACE('aé', '', '-')
		FROM docs
	`)
	if res.RowCount != 1 {
		t.Fatalf("RowCount = %d, want 1", res.RowCount)
	}
	checkText := func(col int, want string) {
		t.Helper()
		c := res.Columns[col].Cells[0]
		if c.Kind() != TypeString {
			t.Fatalf("col %d kind = %v, want TypeString (val=%s)", col, c.Kind(), c.String())
		}
		got, _ := c.Text()
		if got != want {
			t.Fatalf("col %d = %q, want %q", col, got, want)
		}
	}
	checkInt := func(col int, want int64) {
		t.Helper()
		c := res.Columns[col].Cells[0]
		if c.Kind() != TypeNumber {
			t.Fatalf("col %d kind = %v, want TypeNumber (val=%s)", col, c.Kind(), c.String())
		}
		if got := c.String(); got != fmt.Sprintf("%d", want) {
			t.Fatalf("col %d = %s, want %d", col, got, want)
		}
	}

	checkInt(0, 8)               // LENGTH('héllo 世界') = 8 runes
	checkInt(1, 13)              // OCTET_LENGTH = 13 bytes
	checkText(2, "héllo")        // SUBSTR(s, 1, 5)
	checkText(3, "世界")           // SUBSTR(s, 7, 2)
	checkText(4, "")             // SUBSTR(s, 100, 5)
	checkText(5, "hé")           // LEFT(s, 2)
	checkText(6, "héllo")        // LEFT(s, -3)
	checkText(7, "世界")           // RIGHT(s, 2)
	checkText(8, "世界")           // RIGHT(s, -6)
	checkText(9, "界世 olléh")     // REVERSE(s)
	checkText(10, "✨🔥🚀")         // REVERSE(emoji)
	checkText(11, "éclair ω")    // LOWER('ÉCLAIR Ω')
	checkText(12, "ÉCLAIR Ω")    // UPPER('éclair ω')
	checkText(13, "üüüé")        // LPAD
	checkText(14, "éüüü")        // RPAD
	checkText(15, "Éclair Café") // INITCAP
	checkText(16, "cafe 界")      // TRANSLATE ('é'->'e', '世' deleted)
	checkInt(17, 7)              // STRPOS(s, '世') = 7 (1-based rune pos)
	checkText(18, "é")           // SPLIT_PART('héllo', '', 2)
	checkText(19, "-a-é-")       // REPLACE('aé', '', '-')
}

func TestSQLScalarFuncNullPropagation(t *testing.T) {
	docs := []string{`{"id":1,"a":null,"b":"ok"}`}
	res := mustRunScalarSQL(t, docs, `
		SELECT CONCAT(a, b, NULL),
		       CONCAT_WS('-', a, b, NULL, 'z'),
		       CONCAT_WS(a, b, 'z'),
		       LOWER(a),
		       UPPER(a),
		       LENGTH(a),
		       ABS(a),
		       JSON_TYPEOF(a),
		       JSON_TYPEOF(TO_JSON(a)),
		       COALESCE(a, b),
		       NULLIF(b, 'ok'),
		       NULLIF(b, 'no')
		FROM docs
	`)
	if res.RowCount != 1 {
		t.Fatalf("RowCount = %d, want 1", res.RowCount)
	}
	row := func(col int) Cell { return res.Columns[col].Cells[0] }
	if s, _ := row(0).Text(); s != "ok" {
		t.Fatalf("CONCAT = %q, want ok", s)
	}
	if s, _ := row(1).Text(); s != "ok-z" {
		t.Fatalf("CONCAT_WS = %q, want ok-z", s)
	}
	if !row(2).IsNull() {
		t.Fatalf("CONCAT_WS(NULL, ...) = %s, want NULL", row(2).String())
	}
	for _, col := range []int{3, 4, 5, 6, 7, 10} {
		if !row(col).IsNull() {
			t.Fatalf("col %d = %s, want NULL", col, row(col).String())
		}
	}
	if s, _ := row(8).Text(); s != "null" {
		t.Fatalf("JSON_TYPEOF(TO_JSON(NULL)) = %q, want null", s)
	}
	if s, _ := row(9).Text(); s != "ok" {
		t.Fatalf("COALESCE = %q, want ok", s)
	}
	if s, _ := row(11).Text(); s != "ok" {
		t.Fatalf("NULLIF(b, 'no') = %q, want ok", s)
	}
}

func TestSQLScalarFuncNumericEdgeCases(t *testing.T) {
	docs := []string{
		fmt.Sprintf(`{"id":1,"min_i64":%d,"max_i64":%d,"x":1234.567}`, int64(math.MinInt64), int64(math.MaxInt64)),
	}
	res := mustRunScalarSQL(t, docs, `
		SELECT ABS(min_i64), ABS(max_i64),
		       ROUND(x, -2), ROUND(x, 500), ROUND(x, -500),
		       TRUNC(x, -2), TRUNC(x, 500), TRUNC(x, -500),
		       ROUND(-0.1)
		FROM docs
	`)
	if got := res.Columns[0].Cells[0].String(); got != "9223372036854775808" {
		t.Fatalf("ABS(MinInt64) = %s, want 9223372036854775808", got)
	}
	if got := res.Columns[1].Cells[0].String(); got != "9223372036854775807" {
		t.Fatalf("ABS(MaxInt64) = %s, want 9223372036854775807", got)
	}
	if got := res.Columns[2].Cells[0].String(); got != "1200" {
		t.Fatalf("ROUND(1234.567, -2) = %s, want 1200", got)
	}
	if got := res.Columns[3].Cells[0].String(); got != "1234.567" {
		t.Fatalf("ROUND(1234.567, 500) = %s, want 1234.567", got)
	}
	if got := res.Columns[4].Cells[0].String(); got != "0" {
		t.Fatalf("ROUND(1234.567, -500) = %s, want 0", got)
	}
	if got := res.Columns[5].Cells[0].String(); got != "1200" {
		t.Fatalf("TRUNC(1234.567, -2) = %s, want 1200", got)
	}
	if got := res.Columns[6].Cells[0].String(); got != "1234.567" {
		t.Fatalf("TRUNC(1234.567, 500) = %s, want 1234.567", got)
	}
	if got := res.Columns[7].Cells[0].String(); got != "0" {
		t.Fatalf("TRUNC(1234.567, -500) = %s, want 0", got)
	}
	if got := res.Columns[8].Cells[0].String(); got != "0" {
		t.Fatalf("ROUND(-0.1) = %s, want 0", got)
	}

	// Error paths: NaN/Inf, division by zero, range overflow, invalid args.
	seg := FromSegment(mustSegment(t, docs...))
	errCases := []struct {
		sql     string
		wantErr error
	}{
		{`SELECT MOD(10, 0) FROM docs`, ErrScalarDivisionByZero},
		{`SELECT SQRT(-1) FROM docs`, ErrScalarNumericRange},
		{`SELECT EXP(1000) FROM docs`, ErrScalarNumericRange},
		{`SELECT POWER(10, 400) FROM docs`, ErrScalarNumericRange},
		{`SELECT LN(0) FROM docs`, ErrScalarNumericRange},
		{`SELECT LOG(1, 10) FROM docs`, ErrScalarNumericRange},
		{`SELECT SUBSTR('abc', 1, -1) FROM docs`, ErrScalarNumericRange},
		{`SELECT SPLIT_PART('a,b', ',', 0) FROM docs`, ErrScalarNumericRange},
		{`SELECT REPEAT('abc', 1000000) FROM docs`, ErrScalarNumericRange},
		{`SELECT JSON_BUILD_OBJECT('a') FROM docs`, ErrScalarType},
		{`SELECT JSON_BUILD_OBJECT(NULL, 1) FROM docs`, ErrScalarType},
	}
	for _, tc := range errCases {
		stmt, err := PrepareStatement(tc.sql)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", tc.sql, err)
		}
		var exec Exec
		_, err = stmt.RunInto(&exec, seg, nil)
		stmt.Release()
		if !errors.Is(err, tc.wantErr) {
			t.Fatalf("%q: err = %v, want %v", tc.sql, err, tc.wantErr)
		}
	}

	// Direct NaN / Inf / invalid Number parameter rejection in requireNumberArg.
	for _, bad := range []string{"NaN", "Inf", "-Inf", "1e400", "notanumber"} {
		v := statementScalarValue{value: scalar{kind: kindNumber, num: []byte(bad)}}
		if _, err := requireNumberArg("abs", v); !errors.Is(err, ErrScalarNumericRange) {
			t.Fatalf("requireNumberArg(%q) = %v, want ErrScalarNumericRange", bad, err)
		}
	}
}

func TestSQLScalarFuncNilRawCells(t *testing.T) {
	// Verify TypeString / TypeBool / TypeNull cells with raw == nil work cleanly
	// in all scalar helper paths (display, JSON, typeof, isNull).
	strCell := Cell{kind: TypeString, text: "hello\nworld"}
	boolTrue := Cell{kind: TypeBool, flag: cellTrue}
	boolFalse := Cell{kind: TypeBool}
	nullC := Cell{kind: TypeNull}

	var arena []byte
	vStr := statementScalarValue{cell: strCell, direct: true, value: scalarFromResultCell(strCell, &arena)}
	vTrue := statementScalarValue{cell: boolTrue, direct: true, value: scalarFromResultCell(boolTrue, &arena)}
	vFalse := statementScalarValue{cell: boolFalse, direct: true, value: scalarFromResultCell(boolFalse, &arena)}
	vNull := statementScalarValue{cell: nullC, direct: true, value: scalarFromResultCell(nullC, &arena)}

	if vStr.isNull() || vTrue.isNull() || vFalse.isNull() || !vNull.isNull() {
		t.Fatalf("unexpected isNull results: str=%v true=%v false=%v null=%v",
			vStr.isNull(), vTrue.isNull(), vFalse.isNull(), vNull.isNull())
	}
	if got := string(appendScalarDisplay(nil, vStr)); got != "hello\nworld" {
		t.Fatalf("appendScalarDisplay(str) = %q", got)
	}
	if got := string(appendScalarDisplay(nil, vTrue)); got != "true" {
		t.Fatalf("appendScalarDisplay(true) = %q", got)
	}
	if got := string(appendScalarDisplay(nil, vFalse)); got != "false" {
		t.Fatalf("appendScalarDisplay(false) = %q", got)
	}
	if got := string(appendScalarJSON(nil, vStr)); got != `"hello\nworld"` {
		t.Fatalf("appendScalarJSON(str) = %q", got)
	}
	if got := string(appendScalarJSON(nil, vTrue)); got != "true" {
		t.Fatalf("appendScalarJSON(true) = %q", got)
	}
	if got := string(appendScalarJSON(nil, vFalse)); got != "false" {
		t.Fatalf("appendScalarJSON(false) = %q", got)
	}
	if got := string(appendScalarJSON(nil, vNull)); got != "null" {
		t.Fatalf("appendScalarJSON(null) = %q", got)
	}
	if got := scalarJSONTypeOf(vStr); got != "string" {
		t.Fatalf("typeof(str) = %q", got)
	}
	if got := scalarJSONTypeOf(vTrue); got != "boolean" {
		t.Fatalf("typeof(bool) = %q", got)
	}
	if got := scalarJSONTypeOf(vNull); got != "null" {
		t.Fatalf("typeof(null) = %q", got)
	}
	if got := string(scalarJSONBytes(vStr, &arena)); got != `"hello\nworld"` {
		t.Fatalf("scalarJSONBytes(str) = %q", got)
	}
	if got := string(scalarJSONBytes(vTrue, &arena)); got != "true" {
		t.Fatalf("scalarJSONBytes(true) = %q", got)
	}
	if got := string(scalarJSONBytes(vNull, &arena)); got != "null" {
		t.Fatalf("scalarJSONBytes(null) = %q", got)
	}
}

func TestSQLScalarFuncCancellation(t *testing.T) {
	docs := make([]string, 256)
	for i := range docs {
		docs[i] = fmt.Sprintf(`{"id":%d,"name":"Row %d"}`, i, i)
	}
	seg := FromSegment(mustSegment(t, docs...))
	stmt, err := PrepareStatement(`SELECT LOWER(TRIM(name)), JSON_BUILD_OBJECT('id', id) FROM docs`)
	if err != nil {
		t.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var cancel CancelFlag
	cancel.Cancel()

	var exec Exec
	exec.Options.Workers = 1
	exec.Options.Cancel = &cancel
	_, err = stmt.RunInto(&exec, seg, nil)
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("RunInto with canceled flag: err = %v, want ErrCanceled", err)
	}
}

func TestSQLScalarFuncJSONBPrettyAndSet(t *testing.T) {
	docs := []string{
		`{"id":1,"meta":{"a":1,"b":{"c":2},"drop":null},"arr":[10,20,30]}`,
	}
	res := mustRunScalarSQL(t, docs, `
		SELECT JSONB_PRETTY(meta),
		       JSONB_SET(meta, '{b,c}', '99'),
		       JSONB_SET(meta, '{b,d}', '"added"', true),
		       JSONB_SET(meta, '{b,d}', '"nope"', false),
		       JSONB_SET(arr, '{-1}', '99'),
		       JSON_STRIP_NULLS(meta)
		FROM docs
	`)
	pretty, _ := res.Columns[0].Cells[0].Text()
	if !strings.Contains(pretty, "\n    \"a\": 1") {
		t.Fatalf("JSONB_PRETTY unexpected: %q", pretty)
	}
	if got := res.Columns[1].Cells[0].String(); got != `{"a":1,"b":{"c":99},"drop":null}` {
		t.Fatalf("JSONB_SET replace = %s", got)
	}
	if got := res.Columns[2].Cells[0].String(); got != `{"a":1,"b":{"c":2,"d":"added"},"drop":null}` {
		t.Fatalf("JSONB_SET createMissing=true = %s", got)
	}
	if got := res.Columns[3].Cells[0].String(); got != `{"a":1,"b":{"c":2},"drop":null}` {
		t.Fatalf("JSONB_SET createMissing=false = %s", got)
	}
	if got := res.Columns[4].Cells[0].String(); got != `[10,20,99]` {
		t.Fatalf("JSONB_SET array neg index = %s", got)
	}
	if got := res.Columns[5].Cells[0].String(); got != `{"a":1,"b":{"c":2}}` {
		t.Fatalf("JSON_STRIP_NULLS = %s", got)
	}
}

func TestSQLScalarFuncReviewEdgeCases(t *testing.T) {
	t.Run("negative_scale_precision", func(t *testing.T) {
		docs := []string{`{"id":1,"a":100000,"b":50000,"c":-100000,"d":-50000}`}
		res := mustRunScalarSQL(t, docs, `
			SELECT TRUNC(a, -5), ROUND(b, -5), TRUNC(c, -5), ROUND(d, -5)
			FROM docs
		`)
		if got := res.Columns[0].Cells[0].String(); got != "100000" {
			t.Errorf("TRUNC(100000, -5) = %s, want 100000", got)
		}
		if got := res.Columns[1].Cells[0].String(); got != "100000" {
			t.Errorf("ROUND(50000, -5) = %s, want 100000", got)
		}
		if got := res.Columns[2].Cells[0].String(); got != "-100000" {
			t.Errorf("TRUNC(-100000, -5) = %s, want -100000", got)
		}
		if got := res.Columns[3].Cells[0].String(); got != "-100000" {
			t.Errorf("ROUND(-50000, -5) = %s, want -100000", got)
		}
	})

	t.Run("jsonb_build_object_duplicate_keys", func(t *testing.T) {
		docs := []string{`{"id":1}`}
		res := mustRunScalarSQL(t, docs, `
			SELECT JSONB_BUILD_OBJECT('a', 1, 'b', 2, 'a', 3),
			       JSONB_BUILD_OBJECT(1, 'first', '1', 'second')
			FROM docs
		`)
		if got := res.Columns[0].Cells[0].String(); got != `{"b":2,"a":3}` {
			t.Errorf("JSONB_BUILD_OBJECT dup keys = %s, want {\"b\":2,\"a\":3}", got)
		}
		if got := res.Columns[1].Cells[0].String(); got != `{"1":"second"}` {
			t.Errorf("JSONB_BUILD_OBJECT non-string dup key = %s, want {\"1\":\"second\"}", got)
		}
	})

	t.Run("json_whitespace_and_escapes", func(t *testing.T) {
		docs := []string{`{"id":1,"obj":{"a\"b":1,"\u0041":2,"dup":1,"dup":9},"nested":{"arr":[{"x":1},2]}}`}
		res := mustRunScalarSQL(t, docs, `
			SELECT JSON_EXTRACT_PATH_TEXT(obj, 'a"b'),
			       JSON_EXTRACT_PATH_TEXT(obj, 'A'),
			       JSON_EXTRACT_PATH_TEXT(obj, 'dup'),
			       JSONB_SET(TO_JSON('  {"a":1}  '), '{a}', '2'),
			       JSONB_SET(nested, '{arr,0,x}', '99', false),
			       JSONB_SET(nested, '{arr,0,missing}', '99', false),
			       JSONB_SET(nested, '{arr,99}', '3', true),
			       JSONB_SET(nested, '{arr,-99}', '0', true),
			       JSONB_SET(nested, '{arr,99}', '3', false)
			FROM docs
		`)
		if s, _ := res.Columns[0].Cells[0].Text(); s != "1" {
			t.Errorf("escaped quote key = %q, want 1", s)
		}
		if s, _ := res.Columns[1].Cells[0].Text(); s != "2" {
			t.Errorf("unicode escape key = %q, want 2", s)
		}
		if s, _ := res.Columns[2].Cells[0].Text(); s != "9" {
			t.Errorf("dup key = %q, want 9", s)
		}
		if got := res.Columns[3].Cells[0].String(); got != `{"a":2}` {
			t.Errorf("JSONB_SET on whitespace JSON string = %s, want {\"a\":2}", got)
		}
		if got := res.Columns[4].Cells[0].String(); got != `{"arr":[{"x":99},2]}` {
			t.Errorf("JSONB_SET nested array obj update = %s", got)
		}
		if got := res.Columns[5].Cells[0].String(); got != `{"arr":[{"x":1},2]}` {
			t.Errorf("JSONB_SET nested createMissing=false = %s", got)
		}
		if got := res.Columns[6].Cells[0].String(); got != `{"arr":[{"x":1},2,3]}` {
			t.Errorf("JSONB_SET array append = %s", got)
		}
		if got := res.Columns[7].Cells[0].String(); got != `{"arr":[0,{"x":1},2]}` {
			t.Errorf("JSONB_SET array prepend = %s", got)
		}
		if got := res.Columns[8].Cells[0].String(); got != `{"arr":[{"x":1},2]}` {
			t.Errorf("JSONB_SET array oob createMissing=false = %s", got)
		}
		if vt := builtinScalarFuncValueType("jsonb_set"); vt != TypeAny {
			t.Errorf("builtinScalarFuncValueType(jsonb_set) = %v, want TypeAny", vt)
		}
		if vt := builtinScalarFuncValueType("json_set"); vt != TypeAny {
			t.Errorf("builtinScalarFuncValueType(json_set) = %v, want TypeAny", vt)
		}
	})

	t.Run("interval_nan_inf_rejected", func(t *testing.T) {
		for _, bad := range []string{"NaN days", "Inf hours", "-Inf minutes"} {
			if _, _, _, _, ok := parsePGInterval(bad); ok {
				t.Errorf("parsePGInterval(%q) = ok, want false", bad)
			}
		}
	})

	t.Run("uuid_and_regexp_replace", func(t *testing.T) {
		docs := []string{`{"id":1,"s":"foo-12 bar-34"}`}
		res := mustRunScalarSQL(t, docs, `
			SELECT GEN_RANDOM_UUID(),
			       REGEXP_REPLACE(s, '([a-z]+)-([0-9]+)', '$2:$1'),
			       REGEXP_REPLACE(s, '([a-z]+)-([0-9]+)', '$2:$1', 'g')
			FROM docs
		`)
		uuid, _ := res.Columns[0].Cells[0].Text()
		if len(uuid) != 36 || uuid[8] != '-' || uuid[13] != '-' || uuid[18] != '-' || uuid[23] != '-' {
			t.Errorf("invalid uuid format: %q", uuid)
		}
		if uuid[14] != '4' {
			t.Errorf("uuid version char = %c, want '4' (%q)", uuid[14], uuid)
		}
		if !strings.ContainsRune("89ab", rune(uuid[19])) {
			t.Errorf("uuid variant char = %c, want 8/9/a/b (%q)", uuid[19], uuid)
		}
		if s, _ := res.Columns[1].Cells[0].Text(); s != "12:foo bar-34" {
			t.Errorf("REGEXP_REPLACE single = %q, want %q", s, "12:foo bar-34")
		}
		if s, _ := res.Columns[2].Cells[0].Text(); s != "12:foo 34:bar" {
			t.Errorf("REGEXP_REPLACE global = %q, want %q", s, "12:foo 34:bar")
		}
	})
}

func benchmarkScalarFuncQuery(b *testing.B, rows int, sql string) {
	docs := make([]string, rows)
	for i := range docs {
		docs[i] = fmt.Sprintf(
			`{"id":%d,"name":"  Ada Lovelace %d  ","uname":"  Éclair Café %d  ","score":%d.75,"neg":-12.345,"csv":"a,b,c","tags":["alpha","beta","gamma"],"meta":{"nested":{"k":"v%d"},"drop":null}}`,
			i+1, i+1, i+1, i+1, i+1,
		)
	}
	seg := FromSegment(mustSegment(b, docs...))
	stmt, err := PrepareStatement(sql)
	if err != nil {
		b.Fatalf("PrepareStatement: %v", err)
	}
	defer stmt.Release()

	var exec Exec
	exec.Options.Workers = 1
	for i := 0; i < 3; i++ {
		if _, err := stmt.RunInto(&exec, seg, nil); err != nil {
			b.Fatalf("warm RunInto: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := stmt.RunInto(&exec, seg, nil); err != nil {
			b.Fatalf("RunInto: %v", err)
		}
	}
}

func BenchmarkScalarFuncStringASCII(b *testing.B) {
	benchmarkScalarFuncQuery(b, 256, `
		SELECT LOWER(TRIM(name)), UPPER(LEFT(TRIM(name), 3)),
		       SUBSTR(TRIM(name), 5, 4), REPLACE(TRIM(name), 'Ada', 'Grace'),
		       CONCAT_WS('-', 'u', id, 'ok'), LENGTH(TRIM(name))
		FROM docs
	`)
}

func BenchmarkScalarFuncStringUTF8(b *testing.B) {
	benchmarkScalarFuncQuery(b, 256, `
		SELECT LOWER(TRIM(uname)), UPPER(TRIM(uname)),
		       LENGTH(TRIM(uname)), SUBSTR(TRIM(uname), 1, 6), REVERSE(TRIM(uname))
		FROM docs
	`)
}

func BenchmarkScalarFuncMath(b *testing.B) {
	benchmarkScalarFuncQuery(b, 256, `
		SELECT ABS(neg), ROUND(neg, 2), FLOOR(score), CEIL(score), MOD(id, 7), SQRT(score)
		FROM docs
	`)
}

func BenchmarkScalarFuncJSON(b *testing.B) {
	benchmarkScalarFuncQuery(b, 256, `
		SELECT JSON_BUILD_OBJECT('id', id, 'name', TRIM(name)),
		       JSON_BUILD_ARRAY(id, TRIM(name)),
		       JSON_EXTRACT_PATH_TEXT(meta, 'nested', 'k'),
		       JSON_ARRAY_LENGTH(tags),
		       JSON_STRIP_NULLS(meta),
		       JSONB_SET(meta, '{nested,k}', '"x"')
		FROM docs
	`)
}

func BenchmarkScalarFuncConditionalCast(b *testing.B) {
	benchmarkScalarFuncQuery(b, 256, `
		SELECT COALESCE(NULL, TRIM(name), 'x'), NULLIF(id, 5),
		       GREATEST(id, 10), LEAST(id, 10), CAST(id AS TEXT)
		FROM docs
	`)
}
