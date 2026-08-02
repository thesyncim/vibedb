package query

import (
	"errors"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func FuzzSQLViewExpansionNeverPartiallyPublishes(f *testing.F) {
	f.Add("SELECT id FROM docs", "SELECT id FROM v")
	f.Add("SELECT * FROM docs", "SELECT id FROM v")
	f.Add("SELECT id FROM v", "SELECT id FROM v")
	f.Fuzz(func(t *testing.T, definition, source string) {
		if len(definition) > 4096 || len(source) > 4096 {
			t.Skip()
		}
		tree, err := sqlast.Parse(source)
		if err != nil {
			return
		}
		before := make([]sqlast.TableRef, len(tree.From))
		copy(before, tree.From)
		_, err = ExpandSQLViews(source, tree, sqlViewMap{
			"v": {Name: "v", Query: definition},
		}, SQLViewExpansionOptions{})
		if err == nil {
			return
		}
		for i := range before {
			if before[i].Kind != tree.From[i].Kind ||
				before[i].Name != tree.From[i].Name ||
				before[i].Query != tree.From[i].Query {
				t.Fatalf("failed expansion published relation %d", i)
			}
		}
		if errors.Is(err, ErrSQLViewCycle) && len(tree.From) != len(before) {
			t.Fatal("cycle failure changed relation arity")
		}
	})
}
