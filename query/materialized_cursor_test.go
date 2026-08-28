package query

import "testing"

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
	for _, raw := range []string{"", "{", "1 2"} {
		if _, err := ParseJSONCell([]byte(raw)); err == nil {
			t.Fatalf("accepted %q", raw)
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
