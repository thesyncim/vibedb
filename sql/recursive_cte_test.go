package sql

import (
	"errors"
	"strings"
	"testing"
)

const recursiveCTEParseSQL = `WITH RECURSIVE reachable(node) AS MATERIALIZED (
SELECT node FROM seeds WHERE node = ?
UNION
SELECT e.dst AS node FROM reachable r JOIN edges e ON r.node = e.src WHERE e.enabled = ?
)
SELECT node FROM reachable WHERE node >= ?`

func TestRecursiveCTEParsesStableSetLeavesScopeAndParamBases(t *testing.T) {
	statement, err := Parse(recursiveCTEParseSQL)
	if err != nil {
		t.Fatal(err)
	}
	if statement.With == nil || !statement.With.Recursive ||
		len(statement.With.CTEs) != 1 || statement.Params != 3 {
		t.Fatalf("recursive WITH metadata = %+v, params %d", statement.With, statement.Params)
	}
	definition := &statement.With.CTEs[0]
	recursive := definition.Recursive
	if recursive.Anchor == nil || recursive.Term == nil ||
		recursive.Operation != SetUnionDistinct || definition.Query == recursive.Anchor ||
		definition.Query.Set == nil || definition.Query.ParamBase != 0 ||
		recursive.Anchor.ParamBase != 0 || recursive.Anchor.Params != 1 ||
		recursive.Term.ParamBase != 1 || recursive.Term.Params != 1 {
		t.Fatalf("recursive definition metadata = %+v body=%+v", recursive, definition.Query)
	}
	if len(recursive.Term.From) != 2 {
		t.Fatalf("recursive FROM width = %d", len(recursive.Term.From))
	}
	self := recursive.Term.From[0]
	if self.Kind != RelationCTE || self.Name != "reachable" ||
		self.Query != recursive.Anchor || self.UnresolvedCTE.Kind != CTEReferenceNone {
		t.Fatalf("recursive self identity = %+v", self)
	}
	outer := statement.From[0]
	if outer.Kind != RelationCTE || outer.Query != definition.Query {
		t.Fatalf("outer CTE identity = %+v, want authored body", outer)
	}
}

func TestWithRecursiveMayContainOrdinaryDefinitionsAndDeferredWildcards(t *testing.T) {
	ordinary, err := Parse(
		`WITH RECURSIVE c AS (SELECT id FROM docs) SELECT id FROM c`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if ordinary.With == nil || !ordinary.With.Recursive ||
		ordinary.With.CTEs[0].Recursive.Anchor != nil {
		t.Fatalf("ordinary definition in recursive scope = %+v", ordinary.With)
	}

	deferred, err := Parse(`WITH RECURSIVE c(a, b) AS (
		SELECT * FROM docs UNION ALL SELECT * FROM c
	) SELECT a FROM c`)
	if err != nil {
		t.Fatal(err)
	}
	definition := deferred.With.CTEs[0]
	if !definition.ColumnArityDeferred ||
		definition.Recursive.Operation != SetUnionAll {
		t.Fatalf("deferred recursive wildcard metadata = %+v", definition)
	}
}

func TestRecursiveCTEUnsupportedShapesArePositionedAndTyped(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		marker string
	}{
		{
			name:   "anchor self reference",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM r UNION ALL SELECT n FROM base) SELECT n FROM r`,
			marker: "FROM r",
		},
		{
			name:   "multiple recursive references",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM base UNION ALL SELECT a.n FROM r a JOIN r b ON a.n = b.n) SELECT n FROM r`,
			marker: "JOIN r",
		},
		{
			name:   "mutual forward reference",
			sql:    `WITH RECURSIVE a(n) AS (SELECT n FROM b), b(n) AS (SELECT n FROM base) SELECT n FROM a`,
			marker: "FROM b",
		},
		{
			name:   "no union",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM r) SELECT n FROM r`,
			marker: "FROM r",
		},
		{
			name:   "intersect",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM base INTERSECT SELECT n FROM r) SELECT n FROM r`,
			marker: "INTERSECT",
		},
		{
			name:   "nested self reference",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM base UNION ALL SELECT d.n FROM (SELECT n FROM r) d) SELECT n FROM r`,
			marker: "FROM r",
		},
		{
			name:   "aggregate recursive term",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM base UNION ALL SELECT COUNT(*) FROM r) SELECT n FROM r`,
			marker: "COUNT",
		},
		{
			name:   "grouped recursive term",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM base UNION ALL SELECT n FROM r GROUP BY n) SELECT n FROM r`,
			marker: "GROUP BY n",
		},
		{
			name:   "nullable left join side",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM base UNION ALL SELECT b.n FROM base b LEFT JOIN r ON b.n = r.n) SELECT n FROM r`,
			marker: "r ON",
		},
		{
			name:   "nullable through right join",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM base UNION ALL SELECT b.n FROM r RIGHT JOIN base b ON r.n = b.n) SELECT n FROM r`,
			marker: "base b",
		},
		{
			name:   "search clause",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM base UNION ALL SELECT n FROM r) SEARCH DEPTH FIRST BY n SET ord SELECT n FROM r`,
			marker: "SEARCH",
		},
		{
			name:   "cycle clause",
			sql:    `WITH RECURSIVE r(n) AS (SELECT n FROM base UNION ALL SELECT n FROM r) CYCLE n SET cycle USING path SELECT n FROM r`,
			marker: "CYCLE",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.sql)
			var unsupported *FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want *FeatureNotSupportedError", err, err)
			}
			want := strings.Index(test.sql, test.marker)
			if unsupported.Pos < want || unsupported.Pos >= want+len(test.marker) {
				t.Fatalf("error position = %d, want inside %q at %d: %v",
					unsupported.Pos, test.marker, want, err)
			}
		})
	}
}

func TestRecursiveCTEPreservedOuterJoinSideIsLegal(t *testing.T) {
	_, err := Parse(`WITH RECURSIVE r(n) AS (
		SELECT n FROM base
		UNION ALL
		SELECT b.n FROM r LEFT JOIN base b ON r.n = b.n
	) SELECT n FROM r`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRecursiveCTEParserWarmReuseAllocatesZero(t *testing.T) {
	var parser Parser
	var statement SelectStmt
	run := func() {
		if err := parser.Parse(&statement, recursiveCTEParseSQL); err != nil {
			panic(err)
		}
		if statement.With.CTEs[0].Recursive.Anchor == nil {
			panic("recursive metadata disappeared")
		}
	}
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warm recursive parse allocated %.1f times, want 0", got)
	}
}

func FuzzRecursiveCTEParser(f *testing.F) {
	f.Add(recursiveCTEParseSQL)
	f.Add(`WITH RECURSIVE r(n) AS (SELECT n FROM base UNION ALL SELECT n FROM r) SELECT n FROM r`)
	f.Add(`WITH RECURSIVE r AS (SELECT * FROM base UNION SELECT * FROM r) SELECT * FROM r`)
	f.Fuzz(func(t *testing.T, source string) {
		if len(source) > 16<<10 {
			source = source[:16<<10]
		}
		statement, err := Parse(source)
		if err != nil || statement.With == nil {
			return
		}
		for i := range statement.With.CTEs {
			definition := &statement.With.CTEs[i]
			recursive := definition.Recursive
			if recursive.Anchor == nil {
				continue
			}
			if recursive.Term == nil || definition.Query == recursive.Anchor ||
				(recursive.Operation != SetUnionAll && recursive.Operation != SetUnionDistinct) {
				t.Fatalf("invalid recursive metadata: %+v", definition)
			}
			scan := scanRecursiveCTESelf(recursive.Term)
			if scan.count != 0 {
				t.Fatalf("published recursive term retained unresolved self metadata")
			}
			bound := 0
			for j := range recursive.Term.From {
				ref := &recursive.Term.From[j]
				if ref.Kind == RelationCTE && ref.Name == definition.Name &&
					ref.Query == recursive.Anchor {
					bound++
				}
			}
			if bound != 1 {
				t.Fatalf("recursive term has %d stable self identities", bound)
			}
		}
	})
}
