package sql

import (
	"errors"
	"testing"
)

func TestLeadingFeatureRefusalsAreTypedAndPositioned(t *testing.T) {
	for _, src := range []string{
		"EXPLAIN SELECT * FROM docs",
		"COPY docs TO STDOUT",
		"SAVEPOINT nested",
		"ALTER TABLE docs ADD COLUMN n STRING",
		"WITH x AS (SELECT 1) SELECT * FROM x",
	} {
		t.Run(src, func(t *testing.T) {
			_, err := ParseStatement(src)
			var unsupported *FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("ParseStatement error = %T %v, want *FeatureNotSupportedError", err, err)
			}
			var parse *ParseError
			if !errors.As(err, &parse) || parse.Pos != 0 || parse.Msg == "" {
				t.Fatalf("typed refusal lost parse diagnostics: %+v", parse)
			}
		})
	}
}

func TestLeadingFeatureRefusalAllocatesOnlyItsTypedError(t *testing.T) {
	var parser Parser
	var statement Statement
	const src = "EXPLAIN SELECT * FROM docs"
	allocations := testing.AllocsPerRun(200, func() {
		if err := parser.ParseStatement(&statement, src); err == nil {
			panic("EXPLAIN unexpectedly parsed")
		}
	})
	if allocations > 1 {
		t.Fatalf("typed feature refusal allocated %.1f times, want at most 1", allocations)
	}
}
