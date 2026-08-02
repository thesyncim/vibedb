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

func FuzzSQLViewExpansionMemoizedDAGLongestPath(f *testing.F) {
	f.Add(uint8(28), uint8(5), true)
	f.Add(uint8(27), uint8(5), true)
	f.Add(uint8(16), uint8(16), false)
	f.Fuzz(func(t *testing.T, rawShared, rawDeep uint8, shallowFirst bool) {
		sharedHeight := int(rawShared%40) + 1
		deepHeight := int(rawDeep%40) + 1
		views := sqlViewMemoizedDepthDAG(sharedHeight, deepHeight)
		source := `SELECT s.id FROM shallow AS s ` +
			`JOIN deep_0 AS d ON s.id = d.id`
		if !shallowFirst {
			source = `SELECT d.id FROM deep_0 AS d ` +
				`JOIN shallow AS s ON d.id = s.id`
		}
		tree, err := sqlast.Parse(source)
		if err != nil {
			t.Fatal(err)
		}
		before := append([]sqlast.TableRef(nil), tree.From...)
		_, err = ExpandSQLViews(
			source, tree, views, SQLViewExpansionOptions{},
		)
		overLimit := sharedHeight+deepHeight > maxSQLViewExpansionDepth
		if overLimit != errors.Is(err, ErrSQLViewExpansionLimit) {
			t.Fatalf(
				"shared=%d deep=%d shallowFirst=%t error=%v, overLimit=%t",
				sharedHeight, deepHeight, shallowFirst, err, overLimit,
			)
		}
		if err != nil {
			for i := range before {
				if tree.From[i].Kind != before[i].Kind ||
					tree.From[i].Name != before[i].Name ||
					tree.From[i].Query != before[i].Query {
					t.Fatalf("failed longest-path admission published relation %d", i)
				}
			}
			return
		}
		for i := range tree.From {
			if tree.From[i].Kind != sqlast.RelationDerived ||
				tree.From[i].Query == nil {
				t.Fatalf("admitted relation %d was not expanded", i)
			}
		}
	})
}
