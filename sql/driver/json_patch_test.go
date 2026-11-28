package driver

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"math"
	"testing"
	"time"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestAppendJSONStringParityWithStdlib(t *testing.T) {
	cases := []string{
		"",
		"plain_ascii_identifier_123",
		`quote"backslash\slash/`,
		"line\nreturn\rtab\tbackspace\bformfeed\f",
		"html<tag>&amp;>end",
		"ctrl:\x00\x01\x07\x0b\x1f",
		"unicode: café 🚀 日本語",
		"separators:\u2028and\u2029end",
		"long_unquoted_prefix_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_then\"escape",
	}
	for _, s := range cases {
		want, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("json.Marshal(%q): %v", s, err)
		}
		got := appendJSONString(nil, s)
		if !bytes.Equal(got, want) {
			t.Fatalf("appendJSONString(%q) = %s, want %s", s, got, want)
		}
		gotLen := jsonStringEncodedLen(byteview.Bytes(s))
		if gotLen != len(got) {
			t.Fatalf("jsonStringEncodedLen(%q) = %d, want %d", s, gotLen, len(got))
		}
	}
}

func TestValidJSONNumberStrict(t *testing.T) {
	valid := []string{
		"0", "-0", "1", "-1", "42", "9007199254740993",
		"0.0", "-0.5", "3.141592653589793",
		"1e10", "1E10", "1e+10", "1e-10", "1.23e+45", "-0.0e0",
	}
	for _, s := range valid {
		if !validJSONNumber(s) {
			t.Fatalf("validJSONNumber(%q) = false, want true", s)
		}
	}

	invalid := []string{
		"", " ", " 1", "1 ", "1\n",
		"NaN", "Inf", "+Inf", "-Inf", "Infinity", "-Infinity",
		"01", "-01", "+1", ".5", "1.", "1e", "1e+", "1e-", "--1",
		"123abc", "123 456", "1,2",
		`"123"`, "true", "false", "null", "{}", "[]",
	}
	for _, s := range invalid {
		if validJSONNumber(s) {
			t.Fatalf("validJSONNumber(%q) = true, want false", s)
		}
	}
}

func TestEncodeAssignmentScalarAndErrors(t *testing.T) {
	bTrue := true
	i64 := int64(-42)
	f64 := 3.5
	str := "hello \"world\" <x>"
	num := query.Number("12345.6789")
	var nilStr *string
	var nilBool *bool
	var nilInt *int64
	var nilFloat *float64
	var nilNum *query.Number

	args := []any{
		nil,
		true,
		false,
		int(-7),
		int8(-8),
		int16(-16),
		int32(-32),
		int64(-64),
		uint(7),
		uint8(8),
		uint16(16),
		uint32(32),
		uint64(64),
		float32(1.25),
		float64(2.5),
		str,
		[]byte("bytes\nval"),
		num,
		vibejson.RawValue{Src: []byte("999.01")},
		&bTrue,
		&i64,
		&f64,
		&str,
		&num,
		nilStr,
		nilBool,
		nilInt,
		nilFloat,
		nilNum,
	}
	want := []string{
		"null",
		"true",
		"false",
		"-7",
		"-8",
		"-16",
		"-32",
		"-64",
		"7",
		"8",
		"16",
		"32",
		"64",
		"1.25",
		"2.5",
		`"hello \"world\" \u003cx\u003e"`,
		`"bytes\nval"`,
		"12345.6789",
		"999.01",
		"true",
		"-42",
		"3.5",
		`"hello \"world\" \u003cx\u003e"`,
		"12345.6789",
		"null",
		"null",
		"null",
		"null",
		"null",
	}
	for i := range args {
		got, err := encodeAssignmentScalar(sqlast.Operand{Kind: sqlast.OperandParam, Ordinal: i}, args)
		if err != nil {
			t.Fatalf("arg[%d] (%T): unexpected error: %v", i, args[i], err)
		}
		if string(got) != want[i] {
			t.Fatalf("arg[%d] (%T) = %s, want %s", i, args[i], got, want[i])
		}
	}

	badArgs := []any{
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
		float32(math.NaN()),
		float32(math.Inf(1)),
		query.Number("NaN"),
		query.Number("123abc"),
		query.Number(`"123"`),
		query.Number(" 123"),
		vibejson.RawValue{Src: []byte("not-a-number")},
		string([]byte{0xff, 0xfe}),
		[]byte{0xff, 0xfe},
	}
	for i, bad := range badArgs {
		_, err := encodeAssignmentScalar(sqlast.Operand{Kind: sqlast.OperandParam, Ordinal: 0}, []any{bad})
		if err == nil {
			t.Fatalf("badArgs[%d] (%T = %v): expected error, got nil", i, bad, bad)
		}
	}
}

func TestApplyColumnDefaultsEdgeCases(t *testing.T) {
	meta := &tableMeta{
		Schema: &schemaMeta{
			Fields: []schemaFieldMeta{
				{Path: "/id"},
				{Path: "/status", DefaultSQL: "'active'"},
				{Path: "/score", DefaultSQL: "10 + 5"},
				{Path: `/weird"key`, DefaultSQL: "'ok'"},
			},
		},
	}

	// 1. Omitted columns get defaults; explicit NULL is preserved and NOT overwritten.
	out, err := applyColumnDefaults([]byte(`{"id":1,"status":null}`), meta, 4096)
	if err != nil {
		t.Fatalf("applyColumnDefaults explicit null: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal %s: %v", out, err)
	}
	if val, exists := got["status"]; !exists || val != nil {
		t.Fatalf("explicit null status should stay null, got exists=%v val=%v (%s)", exists, val, out)
	}
	if got["score"] != float64(15) {
		t.Fatalf("score default = %v, want 15 (%s)", got["score"], out)
	}
	if got[`weird"key`] != "ok" {
		t.Fatalf("weird\"key default = %v, want ok (%s)", got[`weird"key`], out)
	}

	// 2. Escaped JSON key matching a defaulted column must be recognized as present.
	escapedDoc := []byte(`{"id":1,"\u0073tatus":"manual","score":99,"weird\"key":"preset"}`)
	out, err = applyColumnDefaults(escapedDoc, meta, 4096)
	if err != nil {
		t.Fatalf("applyColumnDefaults escaped keys: %v", err)
	}
	if !bytes.Equal(out, escapedDoc) {
		t.Fatalf("expected unchanged document when all escaped keys present, got %s", out)
	}

	// 3. Duplicate keys in document are preserved and recognized as present.
	dupDoc := []byte(`{"id":1,"status":"first","status":"second","score":1,"weird\"key":"x"}`)
	out, err = applyColumnDefaults(dupDoc, meta, 4096)
	if err != nil {
		t.Fatalf("applyColumnDefaults duplicate keys: %v", err)
	}
	if !bytes.Equal(out, dupDoc) {
		t.Fatalf("expected unchanged document for duplicate keys, got %s", out)
	}

	// 4. Empty object {} and null document inject all defaults cleanly.
	for _, input := range [][]byte{[]byte(`{}`), []byte(`  {  }  `), []byte(`null`)} {
		out, err = applyColumnDefaults(input, meta, 4096)
		if err != nil {
			t.Fatalf("applyColumnDefaults(%s): %v", input, err)
		}
		var m map[string]any
		if err := json.Unmarshal(out, &m); err != nil {
			t.Fatalf("unmarshal(%s) -> %s: %v", input, out, err)
		}
		if m["status"] != "active" || m["score"] != float64(15) || m[`weird"key`] != "ok" {
			t.Fatalf("applyColumnDefaults(%s) = %s, unexpected values", input, out)
		}
	}

	// 5. Non-object JSON is a no-op.
	arr := []byte(`[1,2,3]`)
	out, err = applyColumnDefaults(arr, meta, 4096)
	if err != nil || !bytes.Equal(out, arr) {
		t.Fatalf("non-object array should be untouched: out=%s err=%v", out, err)
	}

	// 6. Document size limit is enforced before emitting oversized doc (exact boundary vs limit-1).
	boundaryMeta := &tableMeta{
		Schema: &schemaMeta{
			Fields: []schemaFieldMeta{
				{Path: "/status", DefaultSQL: "'active'"},
			},
		},
	}
	// {"id":1,"status":"active"} is exactly 26 bytes.
	out, err = applyColumnDefaults([]byte(`{"id":1}`), boundaryMeta, 26)
	if err != nil {
		t.Fatalf("applyColumnDefaults exact limit: %v", err)
	}
	if len(out) != 26 {
		t.Fatalf("applyColumnDefaults exact limit len = %d, want 26 (%s)", len(out), out)
	}
	_, err = applyColumnDefaults([]byte(`{"id":1}`), boundaryMeta, 25)
	if err != durable.ErrDocumentTooLarge {
		t.Fatalf("expected ErrDocumentTooLarge at limit-1, got %v", err)
	}

	// 7. Surrogate-pair unicode escape in key matches UTF-8 column name.
	emojiMeta := &tableMeta{
		Schema: &schemaMeta{
			Fields: []schemaFieldMeta{
				{Path: "/🚀", DefaultSQL: "'default_rocket'"},
				{Path: "/missing", DefaultSQL: "'filled'"},
			},
		},
	}
	surrogateDoc := []byte(`{"\uD83D\uDE80":"custom_rocket"}`)
	out, err = applyColumnDefaults(surrogateDoc, emojiMeta, 4096)
	if err != nil {
		t.Fatalf("applyColumnDefaults surrogate key: %v", err)
	}
	var emojiMap map[string]any
	if err := json.Unmarshal(out, &emojiMap); err != nil {
		t.Fatalf("unmarshal surrogate %s: %v", out, err)
	}
	if emojiMap["🚀"] != "custom_rocket" || emojiMap["missing"] != "filled" {
		t.Fatalf("surrogate key result = %#v (raw=%s)", emojiMap, out)
	}

	// 8. Duplicate schema field path follows first-declaration-wins.
	dupFieldMeta := &tableMeta{
		Schema: &schemaMeta{
			Fields: []schemaFieldMeta{
				{Path: "/status", DefaultSQL: "'first'"},
				{Path: "/status", DefaultSQL: "'second'"},
			},
		},
	}
	out, err = applyColumnDefaults([]byte(`{"id":1}`), dupFieldMeta, 4096)
	if err != nil {
		t.Fatalf("applyColumnDefaults duplicate field: %v", err)
	}
	var dupFieldMap map[string]any
	if err := json.Unmarshal(out, &dupFieldMap); err != nil {
		t.Fatalf("unmarshal duplicate field %s: %v", out, err)
	}
	if dupFieldMap["status"] != "first" {
		t.Fatalf("duplicate field default = %v, want first", dupFieldMap["status"])
	}

	// 9. Deeply nested document with >64 index entries (exercises ErrIndexFull pool growth)
	// and escaped quotes/braces inside nested values.
	var largeBuf bytes.Buffer
	largeBuf.WriteString(`{"nested":{"text":"quote\"brace}colon:","arr":[`)
	for i := 0; i < 80; i++ {
		if i > 0 {
			largeBuf.WriteByte(',')
		}
		largeBuf.WriteString(`{"status":"inner_should_not_count"}`)
	}
	largeBuf.WriteString(`]}}`)
	out, err = applyColumnDefaults(largeBuf.Bytes(), boundaryMeta, 64*1024)
	if err != nil {
		t.Fatalf("applyColumnDefaults large nested: %v", err)
	}
	var largeMap map[string]any
	if err := json.Unmarshal(out, &largeMap); err != nil {
		t.Fatalf("unmarshal large nested: %v", err)
	}
	if largeMap["status"] != "active" {
		t.Fatalf("large nested top-level status = %v, want active", largeMap["status"])
	}
}

func TestPatchJSONObjectEdgeCases(t *testing.T) {
	// 1. Overwrite existing key + add new key + escaped key match + duplicate key in doc.
	doc := []byte(`{"id":1,"\u0061":"old_a","dup":1,"dup":2,"q\"k":"old_q"}`)
	patches := []jsonObjectPatch{
		{column: "a", value: []byte(`"new_a"`)},
		{column: "dup", value: []byte(`99`)},
		{column: `q"k`, value: []byte(`"new_q"`)},
		{column: "added", value: []byte(`true`)},
		{column: "added", value: []byte(`false`)}, // last patch wins
	}
	out, err := patchJSONObject(doc, patches, 4096)
	if err != nil {
		t.Fatalf("patchJSONObject: %v", err)
	}
	if err := vibejson.Validate(out); err != nil {
		t.Fatalf("patched JSON invalid: %v (%s)", err, out)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", out, err)
	}
	if m["id"] != float64(1) || m["a"] != "new_a" || m["dup"] != float64(99) || m[`q"k`] != "new_q" || m["added"] != false {
		t.Fatalf("unexpected patched map: %#v (raw=%s)", m, out)
	}

	// 2. Patching empty object {}.
	out, err = patchJSONObject([]byte(`{}`), []jsonObjectPatch{{column: "k", value: []byte(`123`)}}, 4096)
	if err != nil {
		t.Fatalf("patchJSONObject empty: %v", err)
	}
	if string(out) != `{"k":123}` {
		t.Fatalf("patchJSONObject empty = %s, want {\"k\":123}", out)
	}

	// 3. > 8 patches (map-backed path).
	many := make([]jsonObjectPatch, 10)
	for i := range many {
		many[i] = jsonObjectPatch{column: string(rune('a' + i)), value: []byte(`1`)}
	}
	many[9] = jsonObjectPatch{column: "a", value: []byte(`2`)} // shadow first patch
	out, err = patchJSONObject([]byte(`{"a":0,"b":0}`), many, 4096)
	if err != nil {
		t.Fatalf("patchJSONObject many: %v", err)
	}
	m = nil
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal many: %v", err)
	}
	if m["a"] != float64(2) || m["b"] != float64(1) || m["i"] != float64(1) {
		t.Fatalf("unexpected many-patch result: %#v", m)
	}

	// 4. MaxDocumentBytes exact boundary vs limit-1.
	// {"a":1,"b":"ok"} is exactly 16 bytes.
	out, err = patchJSONObject([]byte(`{"a":1}`), []jsonObjectPatch{{column: "b", value: []byte(`"ok"`)}}, 16)
	if err != nil {
		t.Fatalf("patchJSONObject exact limit: %v", err)
	}
	if len(out) != 16 {
		t.Fatalf("patchJSONObject exact limit len = %d, want 16 (%s)", len(out), out)
	}
	_, err = patchJSONObject([]byte(`{"a":1}`), []jsonObjectPatch{{column: "b", value: []byte(`"ok"`)}}, 15)
	if err != durable.ErrDocumentTooLarge {
		t.Fatalf("expected ErrDocumentTooLarge at limit-1, got %v", err)
	}

	// 5. Surrogate-pair unicode escape in key matches UTF-8 patch column.
	out, err = patchJSONObject(
		[]byte(`{"\uD83D\uDE80":"old","keep":1}`),
		[]jsonObjectPatch{{column: "🚀", value: []byte(`"new"`)}},
		4096,
	)
	if err != nil {
		t.Fatalf("patchJSONObject surrogate key: %v", err)
	}
	m = nil
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal surrogate patch %s: %v", out, err)
	}
	if m["🚀"] != "new" || m["keep"] != float64(1) {
		t.Fatalf("unexpected surrogate patch result: %#v (raw=%s)", m, out)
	}

	// 6. Large nested document (>64 index entries) with escaped quotes/braces.
	var largeBuf bytes.Buffer
	largeBuf.WriteString(`{"id":1,"nested":{"text":"quote\"brace}colon:","items":[`)
	for i := 0; i < 80; i++ {
		if i > 0 {
			largeBuf.WriteByte(',')
		}
		largeBuf.WriteString(`{"id":999}`)
	}
	largeBuf.WriteString(`]}}`)
	out, err = patchJSONObject(
		largeBuf.Bytes(),
		[]jsonObjectPatch{
			{column: "id", value: []byte(`2`)},
			{column: "patched", value: []byte(`true`)},
		},
		64*1024,
	)
	if err != nil {
		t.Fatalf("patchJSONObject large nested: %v", err)
	}
	m = nil
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal large nested patch: %v", err)
	}
	if m["id"] != float64(2) || m["patched"] != true {
		t.Fatalf("unexpected large nested patch result: id=%v patched=%v", m["id"], m["patched"])
	}
}

type customJSONMarshaler struct {
	Tag string
}

func (c customJSONMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(`"custom:` + c.Tag + `"`), nil
}

func TestSQLLiteralForCTEValue(t *testing.T) {
	ts := time.Date(2026, 11, 27, 22, 3, 0, 0, time.UTC)
	if got := sqlLiteralForCTEValue(nil); got != "NULL" {
		t.Fatalf("nil = %q", got)
	}
	if got := sqlLiteralForCTEValue(true); got != "TRUE" {
		t.Fatalf("true = %q", got)
	}
	if got := sqlLiteralForCTEValue(int64(42)); got != "42" {
		t.Fatalf("int64 = %q", got)
	}
	if got := sqlLiteralForCTEValue(ts); got != `'"2026-11-27T22:03:00Z"'` {
		t.Fatalf("time.Time = %q", got)
	}
	if got := sqlLiteralForCTEValue(map[string]int{"a": 1}); got != `'{"a":1}'` {
		t.Fatalf("map = %q", got)
	}
	if got := sqlLiteralForCTEValue(customJSONMarshaler{Tag: "it's"}); got != `'"custom:it''s"'` {
		t.Fatalf("customJSONMarshaler = %q", got)
	}
	// Also verify appendScalarJSON fallback with custom json.Marshaler and -0.0 float.
	raw, err := appendScalarJSON(nil, customJSONMarshaler{Tag: "ok"})
	if err != nil || string(raw) != `"custom:ok"` {
		t.Fatalf("appendScalarJSON(customJSONMarshaler) = (%q, %v)", raw, err)
	}
	negZero, err := appendScalarJSON(nil, math.Copysign(0, -1))
	if err != nil || string(negZero) != "-0" {
		t.Fatalf("appendScalarJSON(-0.0) = (%q, %v), want -0", negZero, err)
	}
}

func TestEndToEndSQLDefaultsUpsertAndJoinUpdate(t *testing.T) {
	db, err := sql.Open("vibedb", t.TempDir()+"/test.db")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	stmts := []string{
		`CREATE TABLE items (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'pending', qty INTEGER DEFAULT 1, note TEXT)`,
		`CREATE TABLE adjustments (id INTEGER PRIMARY KEY, delta INTEGER, new_note TEXT)`,
		// Insert omitting defaulted columns
		`INSERT INTO items (id) VALUES (1)`,
		// Insert with explicit NULL on defaulted column
		`INSERT INTO items (id, status, qty) VALUES (2, NULL, 5)`,
		// Insert with expression value
		`INSERT INTO items (id, status, qty) VALUES (3, 'init', 2 + 3)`,
		// ON CONFLICT DO UPDATE (overwrite existing + add new key + EXCLUDED)
		`INSERT INTO items (id, status, qty, note) VALUES (1, 'shipped', 9, 'express') ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, qty = EXCLUDED.qty, note = EXCLUDED.note`,
		// Seed adjustment for UPDATE ... FROM join (exercises patchJSONObject in updateJoinLocked)
		`INSERT INTO adjustments (id, delta, new_note) VALUES (3, 10, 'adjusted')`,
		`UPDATE items SET qty = items.qty + adjustments.delta, note = adjustments.new_note FROM adjustments WHERE items.id = adjustments.id`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	// Verify row 1 (upserted)
	var status, note sql.NullString
	var qty int64
	if err := db.QueryRow(`SELECT status, qty, note FROM items WHERE id = 1`).Scan(&status, &qty, &note); err != nil {
		t.Fatalf("query row 1: %v", err)
	}
	if !status.Valid || status.String != "shipped" || qty != 9 || !note.Valid || note.String != "express" {
		t.Fatalf("row 1 = (%v, %d, %v), want (shipped, 9, express)", status, qty, note)
	}

	// Verify row 2 (explicit NULL preserved)
	if err := db.QueryRow(`SELECT status, qty FROM items WHERE id = 2`).Scan(&status, &qty); err != nil {
		t.Fatalf("query row 2: %v", err)
	}
	if status.Valid || qty != 5 {
		t.Fatalf("row 2 = (%v, %d), want (NULL, 5)", status, qty)
	}

	// Verify row 3 (expr insert + join update)
	if err := db.QueryRow(`SELECT status, qty, note FROM items WHERE id = 3`).Scan(&status, &qty, &note); err != nil {
		t.Fatalf("query row 3: %v", err)
	}
	if !status.Valid || status.String != "init" || qty != 15 || !note.Valid || note.String != "adjusted" {
		t.Fatalf("row 3 = (%v, %d, %v), want (init, 15, adjusted)", status, qty, note)
	}
}

func BenchmarkApplyColumnDefaults(b *testing.B) {
	meta := &tableMeta{
		Schema: &schemaMeta{
			Fields: []schemaFieldMeta{
				{Path: "/id"},
				{Path: "/status", DefaultSQL: "'active'"},
				{Path: "/retries", DefaultSQL: "0"},
			},
		},
	}
	allPresent := []byte(`{"id":42,"status":"ok","retries":3,"name":"alice"}`)
	missingOne := []byte(`{"id":42,"retries":3,"name":"alice"}`)

	b.Run("AllPresent", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			out, err := applyColumnDefaults(allPresent, meta, 64*1024)
			if err != nil || len(out) == 0 {
				b.Fatalf("err=%v", err)
			}
		}
	})

	b.Run("MissingOne", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			out, err := applyColumnDefaults(missingOne, meta, 64*1024)
			if err != nil || len(out) == 0 {
				b.Fatalf("err=%v", err)
			}
		}
	})
}

func BenchmarkPatchConflictDocument(b *testing.B) {
	doc := []byte(`{"id":42,"status":"pending","qty":1,"owner":"alice","region":"us-east-1"}`)
	single := []jsonObjectPatch{
		{column: "status", value: []byte(`"shipped"`)},
	}
	multi := []jsonObjectPatch{
		{column: "status", value: []byte(`"shipped"`)},
		{column: "qty", value: []byte(`99`)},
		{column: "note", value: []byte(`"express"`)},
	}

	b.Run("SingleOverwrite", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			out, err := patchJSONObject(doc, single, 64*1024)
			if err != nil || len(out) == 0 {
				b.Fatalf("err=%v", err)
			}
		}
	})

	b.Run("MultiOverwriteAndAdd", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			out, err := patchJSONObject(doc, multi, 64*1024)
			if err != nil || len(out) == 0 {
				b.Fatalf("err=%v", err)
			}
		}
	})
}

func BenchmarkEncodeAssignmentScalar(b *testing.B) {
	op := sqlast.Operand{Kind: sqlast.OperandParam, Ordinal: 0}
	b.Run("Int64", func(b *testing.B) {
		args := []any{int64(123456789)}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := encodeAssignmentScalar(op, args); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Float64", func(b *testing.B) {
		args := []any{float64(3.1415926535)}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := encodeAssignmentScalar(op, args); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("StringASCII", func(b *testing.B) {
		args := []any{"order_status_shipped_fast"}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := encodeAssignmentScalar(op, args); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Bool", func(b *testing.B) {
		args := []any{true}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := encodeAssignmentScalar(op, args); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Null", func(b *testing.B) {
		args := []any{nil}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := encodeAssignmentScalar(op, args); err != nil {
				b.Fatal(err)
			}
		}
	})
}
