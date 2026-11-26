package query

import (
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestMaterializedWholeDocumentBorrowsPayloadWithoutAllocations(t *testing.T) {
	raw := []byte(`{"id":"example","name":"Alex","team":"Engineering","score":92,"active":true}`)
	var cell Cell
	allocs := testing.AllocsPerRun(100, func() {
		var err error
		cell, err = ParseJSONCell(raw)
		if err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("whole-document materialization: %g allocations", allocs)
	}
	if payload := cell.Payload(); len(payload) != len(raw) || &payload[0] != &raw[0] {
		t.Fatal("whole-document payload was copied or rewritten")
	}
}

func BenchmarkMaterializedWholeDocument(b *testing.B) {
	raw := []byte(`{"id":"example","name":"Alex","team":"Engineering","score":92,"active":true}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := ParseJSONCell(raw); err != nil {
			b.Fatal(err)
		}
	}
}

func TestMaterializedNativeCellsPreservePrecisionAndCursorOrder(t *testing.T) {
	if c := NullCell(); !c.IsNull() || string(c.JSON()) != "null" {
		t.Fatalf("materialized NULL: %+v", c)
	}
	number, err := ParseJSONCell([]byte(`9007199254740993`))
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := number.Int64(); !ok || n != 9007199254740993 {
		t.Fatalf("lost integer precision: %s", number.JSON())
	}
	text, err := ParseJSONCell([]byte(`"line\n\u263a"`))
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := text.Text(); !ok || s != "line\n☺" {
		t.Fatalf("lost string decoding: %q", s)
	}
	for _, raw := range []string{"", "{", "1 2", "01", "-01", `"unterminated`, `"bad\x"`, "true1", "null "} {
		if raw == "null " {
			c, err := ParseJSONCell([]byte(raw))
			if err != nil || !c.IsNull() {
				t.Fatalf("whitespace null = %+v, %v", c, err)
			}
			continue
		}
		if _, err := ParseJSONCell([]byte(raw)); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	// Verify fast-path parity for ints, simple strings, bools, and null.
	for _, sample := range []string{
		"0", "-1", "42", "9223372036854775807", "-9223372036854775808",
		"-0", "9223372036854775808", "1.5", "1e3",
		`""`, `"row-000001"`, `"hello world"`, `"a\"b"`, `"utf8☺"`,
		"true", "false", "null",
	} {
		got, err := ParseJSONCell([]byte(sample))
		if err != nil {
			t.Fatalf("sample %q err=%v", sample, err)
		}
		var decoded []byte
		want := cellFromScalar(classifyRawInto(vibejson.RawValue{Src: []byte(sample)}, &decoded))
		if got.Kind() != want.Kind() || string(got.JSON()) != string(want.JSON()) {
			t.Fatalf("sample %q mismatch: got kind=%v json=%q, want kind=%v json=%q", sample, got.Kind(), got.JSON(), want.Kind(), want.JSON())
		}
		if s1, ok1 := got.Text(); ok1 {
			if s2, ok2 := want.Text(); !ok2 || s1 != s2 {
				t.Fatalf("sample %q text got=%q/%t want=%q/%t", sample, s1, ok1, s2, ok2)
			}
		}
		if n1, ok1 := got.Int64(); ok1 {
			if n2, ok2 := want.Int64(); !ok2 || n1 != n2 {
				t.Fatalf("sample %q int64 got=%d/%t want=%d/%t", sample, n1, ok1, n2, ok2)
			}
		}
	}
	result := &Result{RowCount: 2, Columns: []ResultColumn{{Header: "value", Cells: []Cell{text, number}}}}
	cursor, err := NewResultCursor(result)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() || !cursor.Next() || cursor.Next() {
		t.Fatal("materialized cursor lost rows")
	}
	result.RowCount++
	if _, err := NewResultCursor(result); err == nil {
		t.Fatal("accepted wrong row arity")
	}
}
