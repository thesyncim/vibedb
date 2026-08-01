package query

import (
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func sqlUsingDatabase(t *testing.T) *store.Database {
	t.Helper()
	db := &store.Database{}
	put := func(name string, docs ...string) {
		t.Helper()
		collection, err := db.CreateCollection(name, store.Options{ChunkDocuments: 2})
		if err != nil {
			t.Fatalf("CreateCollection(%s): %v", name, err)
		}
		for i, doc := range docs {
			if _, err := collection.Put(fmt.Sprintf("%s-%d", name, i), []byte(doc)); err != nil {
				t.Fatalf("Put(%s, %d): %v", name, i, err)
			}
		}
	}
	put("lefts",
		`{"id":1,"label":"left-1"}`,
		`{"id":2,"label":"left-2"}`,
		`{"id":4,"label":"left-4"}`,
		`{"id":null,"label":"left-null"}`,
	)
	put("rights",
		`{"id":2,"label":"right-2a"}`,
		`{"id":2,"label":"right-2b"}`,
		`{"id":3,"label":"right-3"}`,
		`{"id":null,"label":"right-null"}`,
	)
	return db
}

func runUsingSQL(t *testing.T, db *store.Database, src string) []string {
	t.Helper()
	statement, err := PrepareStatement(src)
	if err != nil {
		t.Fatalf("PrepareStatement(%q): %v", src, err)
	}
	if columns := statement.Columns(); len(columns) == 0 || columns[0] != "id" {
		t.Fatalf("Columns(%q) = %q, want the merged USING column headed id", src, columns)
	}
	var exec Exec
	cursor, err := statement.RunInto(
		&exec,
		FromDatabase(db.Snapshot(), statement.Collection()),
		nil,
	)
	if err != nil {
		t.Fatalf("RunInto(%q): %v", src, err)
	}
	rows := make([]string, 0, exec.Result.RowCount)
	for cursor.Next() {
		cells := make([]string, len(statement.Columns()))
		for i := range cells {
			cells[i] = string(cursor.Cell(i).JSON())
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	return rows
}

func TestSQLUsingCoalescesProjectionFilterAndOrder(t *testing.T) {
	db := sqlUsingDatabase(t)
	for _, tc := range []struct {
		name string
		join string
		want []string
	}{
		{
			name: "inner",
			join: "JOIN",
			want: []string{`2,2,2,"left-2","right-2a"`, `2,2,2,"left-2","right-2b"`},
		},
		{
			name: "left",
			join: "LEFT JOIN",
			want: []string{`2,2,2,"left-2","right-2a"`, `2,2,2,"left-2","right-2b"`, `4,4,null,"left-4",null`},
		},
		{
			name: "right",
			join: "RIGHT JOIN",
			want: []string{`2,2,2,"left-2","right-2a"`, `2,2,2,"left-2","right-2b"`, `3,null,3,null,"right-3"`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runUsingSQL(t, db, `
				SELECT id, l.id, r.id, l.label, r.label
				FROM lefts AS l `+tc.join+` rights AS r USING (id)
				WHERE id >= 2
				ORDER BY id, r.label`)
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Fatalf("%s USING rows:\n got %q\nwant %q", tc.join, got, tc.want)
			}
		})
	}
}

func TestSQLUsingCoalescesGroupKeys(t *testing.T) {
	db := sqlUsingDatabase(t)
	for _, tc := range []struct {
		name string
		join string
		want []string
	}{
		{
			name: "inner",
			join: "JOIN",
			want: []string{"2,2"},
		},
		{
			name: "left",
			join: "LEFT JOIN",
			want: []string{"null,1", "1,1", "2,2", "4,1"},
		},
		{
			name: "right",
			join: "RIGHT JOIN",
			want: []string{"null,1", "2,2", "3,1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runUsingSQL(t, db, `
				SELECT id, COUNT(*)
				FROM lefts AS l `+tc.join+` rights AS r USING (id)
				GROUP BY id
				ORDER BY id`)
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Fatalf("%s USING groups:\n got %q\nwant %q", tc.join, got, tc.want)
			}
		})
	}
}
