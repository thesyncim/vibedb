package query

import "testing"

func TestPrepareStorageReplacementDDL(t *testing.T) {
	tests := []struct {
		sql        string
		kind       DMLKind
		collection string
	}{
		{`TRUNCATE docs`, DDLTruncate, "docs"},
		{`TRUNCATE TABLE docs`, DDLTruncate, "docs"},
		{`DROP INDEX by_kind`, DDLDropIndex, ""},
		{`DROP INDEX IF EXISTS by_kind ON docs`, DDLDropIndex, "docs"},
	}
	for _, test := range tests {
		t.Run(test.sql, func(t *testing.T) {
			statement, err := PrepareDML(test.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			if statement.Kind() != test.kind ||
				statement.Kind().String() == "?" ||
				statement.Collection() != test.collection ||
				statement.NumParams() != 0 ||
				statement.ScansEveryDocument() {
				t.Fatalf(
					"prepared DDL = kind %s collection %q params %d scan %t",
					statement.Kind(), statement.Collection(),
					statement.NumParams(), statement.ScansEveryDocument(),
				)
			}
		})
	}
}
