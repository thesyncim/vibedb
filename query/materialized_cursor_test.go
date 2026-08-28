package query

import "testing"

func TestMaterializedNativeCellsPreservePrecisionAndCursorOrder(t *testing.T) {
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
