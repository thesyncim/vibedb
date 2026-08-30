package query

import (
	"errors"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestPrepareDMLCompilesInsertSelectSeparately(t *testing.T) {
	dml, err := PrepareDML(
		"INSERT INTO dst SELECT * FROM src WHERE id = ? RETURNING id",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dml.Release()
	source := dml.InsertSource()
	if source == nil || source == dml.filter {
		t.Fatalf("source=%p filter=%p", source, dml.filter)
	}
	if source.Collection() != "src" || source.NumParams() != 1 {
		t.Fatalf("source collection=%q params=%d", source.Collection(), source.NumParams())
	}
	if dml.NumParams() != 1 || dml.Tree().Insert.Returning == nil {
		t.Fatalf("DML params=%d returning=%p", dml.NumParams(), dml.Tree().Insert.Returning)
	}
}

func TestPrepareDMLRejectsInsertSelectWidthAndStaticScalar(t *testing.T) {
	for _, source := range []string{
		"INSERT INTO dst SELECT id, value FROM src",
		"INSERT INTO dst SELECT n + 1 FROM src",
	} {
		_, err := PrepareDML(source)
		var shape *InsertSelectShapeError
		if !errors.As(err, &shape) || !errors.Is(err, ErrInsertSelectShape) {
			t.Fatalf("PrepareDML(%q) = %#v", source, err)
		}
		if shape.Position() <= 0 {
			t.Fatalf("PrepareDML(%q) position = %d", source, shape.Position())
		}
	}
}

func TestPrepareDMLRejectsInsertSelectConflictUpdate(t *testing.T) {
	const source = `INSERT INTO dst SELECT * FROM src ` +
		`ON CONFLICT DO UPDATE SET "$doc" = EXCLUDED."$doc"`
	_, err := PrepareDML(source)
	var unsupported *sqlast.FeatureNotSupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("PrepareDML = %T %v, want FeatureNotSupportedError", err, err)
	}
	if unsupported.Pos != strings.Index(source, "UPDATE") {
		t.Fatalf("position = %d, want %d", unsupported.Pos, strings.Index(source, "UPDATE"))
	}
}

func TestInsertSelectDocumentParameterLineageCrossesPreparedRelations(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		documents []int
		scalars   []int
	}{
		{
			name: "derived values",
			statement: `INSERT INTO dst ` +
				`SELECT v.* FROM (` +
				`SELECT * FROM seed UNION ALL VALUES (?)) AS v`,
			documents: []int{0},
		},
		{
			name: "cte values",
			statement: `INSERT INTO dst ` +
				`WITH supplied(doc) AS ((VALUES (?))) ` +
				`SELECT doc FROM supplied`,
			documents: []int{0},
		},
		{
			name: "select leaves groups and binary tree",
			statement: `INSERT INTO dst ` +
				`(SELECT v.* FROM (` +
				`SELECT * FROM seed UNION ALL VALUES (?)) AS v) ` +
				`UNION ALL ((VALUES (?)) UNION ALL ` +
				`(SELECT c.* FROM (` +
				`SELECT * FROM seed UNION ALL VALUES (?)) AS c))`,
			documents: []int{0, 1, 2},
		},
		{
			name: "non-output scalar remains scalar",
			statement: `INSERT INTO dst ` +
				`SELECT v.* FROM (` +
				`SELECT * FROM seed WHERE id = ? ` +
				`UNION ALL VALUES (?)) AS v`,
			documents: []int{1},
			scalars:   []int{0},
		},
		{
			name: "recursive child",
			statement: `INSERT INTO dst ` +
				`WITH RECURSIVE supplied(doc) AS (` +
				`SELECT v.* FROM (` +
				`SELECT * FROM seed UNION ALL VALUES (?)) AS v ` +
				`UNION SELECT doc FROM supplied` +
				`) SELECT doc FROM supplied`,
			documents: []int{0},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dml, err := PrepareDML(test.statement)
			if err != nil {
				t.Fatal(err)
			}
			defer dml.Release()
			for _, ordinal := range test.documents {
				position, ok := dml.InsertDocumentParameterPosition(ordinal)
				if !ok {
					t.Fatalf("parameter %d lost document lineage", ordinal+1)
				}
				want := nthIndex(test.statement, "?", ordinal)
				if position != want {
					t.Fatalf("parameter %d position = %d, want %d",
						ordinal+1, position, want)
				}
			}
			for _, ordinal := range test.scalars {
				if position, ok := dml.InsertDocumentParameterPosition(ordinal); ok {
					t.Fatalf("scalar parameter %d acquired document position %d",
						ordinal+1, position)
				}
			}
		})
	}
}

func nthIndex(source, token string, ordinal int) int {
	position := -1
	for range ordinal + 1 {
		next := strings.Index(source[position+1:], token)
		if next < 0 {
			return -1
		}
		position += next + 1
	}
	return position
}
