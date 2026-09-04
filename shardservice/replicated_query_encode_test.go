package shardservice

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

// This is the former collectRows + EncodeResponse path, retained as the wire
// oracle and benchmark comparison for direct cursor encoding.
func materializeSQLCursor(cursor query.Cursor, names []string) ([]byte, error) {
	columns := make([]Column, len(names))
	for i, name := range names {
		columns[i] = Column{Name: name, TypeOID: pgOIDJSON}
	}
	var rows [][]Cell
	for cursor.Next() {
		row := make([]Cell, len(names))
		for i := range names {
			cell := cursor.Cell(i)
			row[i].Null = cell.IsNull()
			if !cell.IsNull() {
				row[i].Bytes = cell.AppendJSON(nil)
			}
		}
		rows = append(rows, row)
	}
	var encoded bytes.Buffer
	err := EncodeResponse(&encoded, RowsResponse(columns, rows))
	return encoded.Bytes(), err
}

func TestSQLReadCursorEncodingMatchesCodec(t *testing.T) {
	var docs store.Segment
	for _, raw := range []string{
		`{"id":1,"g":1,"text":"a\"é","value":9007199254740993,"obj":{"a":1}}`,
		`{"id":2,"g":1,"text":null,"value":-1.25,"obj":[1,null,true]}`,
		`{"id":3,"g":2,"text":"","value":3.125}`,
		`{"id":4,"g":3,"value":null}`,
	} {
		if _, err := docs.Append([]byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{
		`SELECT id,text,value,obj FROM docs ORDER BY id`,
		`SELECT id,text FROM docs ORDER BY id LIMIT 2 OFFSET 1`,
		`SELECT g,COUNT(*),AVG(value) FROM docs GROUP BY g HAVING COUNT(*) > 1 ORDER BY g`,
		`SELECT id FROM docs WHERE id > 99`,
		`SELECT COUNT(*) FROM docs`,
	} {
		t.Run(sql, func(t *testing.T) {
			statement, err := query.PrepareStatement(sql)
			if err != nil {
				t.Fatal(err)
			}
			var exec query.Exec
			defer exec.Release()
			cursor, err := statement.RunInto(&exec, query.FromSegment(&docs), nil)
			if err != nil {
				t.Fatal(err)
			}
			want, err := materializeSQLCursor(cursor, statement.Columns())
			if err != nil {
				t.Fatal(err)
			}
			got, err := encodeSQLReadCursor(cursor, statement.Columns(), len(want), nil)
			if err != nil || !bytes.Equal(got, want) || cap(got) != len(got) {
				t.Fatalf("wire mismatch: %v\n got %x\nwant %x", err, got, want)
			}
			if partial, err := encodeSQLReadCursor(cursor, statement.Columns(), len(want)-1, nil); partial != nil || !errors.Is(err, errSQLReadFrameBound) {
				t.Fatalf("exact frame bound: bytes=%d err=%v", len(partial), err)
			}
			var cancel query.CancelFlag
			cancel.Cancel()
			if partial, err := encodeSQLReadCursor(cursor, statement.Columns(), len(want), &cancel); partial != nil || !errors.Is(err, query.ErrCanceled) {
				t.Fatalf("cancellation: bytes=%d err=%v", len(partial), err)
			}
			// Once encoded, neither source nor execution storage is retained.
			exec.Release()
			if !bytes.Equal(got, want) {
				t.Fatal("encoded result lost ownership")
			}
		})
	}
}

func TestOrdinaryResponseOwnsFrameAndClipsCells(t *testing.T) {
	want := RowsResponse([]Column{{Name: "x", TypeOID: pgOIDJSON}}, [][]Cell{
		{{Bytes: []byte(`"first"`)}}, {{Bytes: []byte(`"second"`)}}, {{Null: true}}, {{Bytes: []byte{}}},
	})
	raw := encodeResponse(t, want)
	got, err := DecodeResponse(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	clear(raw)
	for i, row := range got.Rows {
		if cap(row) != len(row) || cap(row[0].Bytes) != len(row[0].Bytes) ||
			row[0].Null != want.Rows[i][0].Null || !bytes.Equal(row[0].Bytes, want.Rows[i][0].Bytes) {
			t.Fatalf("owned row %d: %+v", i, row)
		}
	}
	_ = append(got.Rows[0][0].Bytes, 'x')
	_ = append(got.Rows[0], Cell{Null: true})
	if string(got.Rows[1][0].Bytes) != `"second"` {
		t.Fatal("append crossed an ownership boundary")
	}
}

func indexedWireCursor(b *testing.B, count int) (query.Cursor, []string) {
	b.Helper()
	result := query.Result{RowCount: count, Columns: []query.ResultColumn{{Header: "id"}, {Header: "score"}}}
	for i := range count {
		for col, raw := range []string{fmt.Sprintf(`"key-%08d"`, i), fmt.Sprint(i % 100)} {
			cell, err := query.ParseJSONCell([]byte(raw))
			if err != nil {
				b.Fatal(err)
			}
			result.Columns[col].Cells = append(result.Columns[col].Cells, cell)
		}
	}
	cursor, err := query.NewResultCursor(&result)
	if err != nil {
		b.Fatal(err)
	}
	return cursor, []string{"id", "score"}
}

func BenchmarkSQLReadCursorEncoding(b *testing.B) {
	for _, count := range []int{1, 32, 64, 256} {
		cursor, names := indexedWireCursor(b, count)
		for _, direct := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/direct=%t", count, direct), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var err error
					if direct {
						_, err = encodeSQLReadCursor(cursor, names, 64<<10, nil)
					} else {
						_, err = materializeSQLCursor(cursor, names)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
