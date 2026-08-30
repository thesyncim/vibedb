package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestLeadingFeatureRefusalsAreTypedAndPositioned(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  string
		want string
	}{
		{"copy", "COPY docs TO STDOUT", "COPY is not supported"},
		{"lock", "LOCK TABLE docs IN ACCESS EXCLUSIVE MODE", "there is no SQL lock manager"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseStatement(tc.src)
			var unsupported *FeatureNotSupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("ParseStatement error = %T %v, want *FeatureNotSupportedError", err, err)
			}
			var parse *ParseError
			if !errors.As(err, &parse) || parse.Pos != 0 || !strings.Contains(parse.Msg, tc.want) {
				t.Fatalf("typed refusal lost parse diagnostics: %+v", parse)
			}
		})
	}
}

func TestLeadingFeatureRefusalAllocatesOnlyItsTypedError(t *testing.T) {
	var parser Parser
	var statement Statement
	const src = "COPY docs TO STDOUT"
	allocations := testing.AllocsPerRun(200, func() {
		if err := parser.ParseStatement(&statement, src); err == nil {
			panic("COPY unexpectedly parsed")
		}
	})
	if allocations > 1 {
		t.Fatalf("typed feature refusal allocated %.1f times, want at most 1", allocations)
	}
}

func TestNewFeatureNotSupportedErrorPreservesTypedPosition(t *testing.T) {
	const src = "SELECT id\nFROM docs"
	pos := strings.Index(src, "docs")
	err := NewFeatureNotSupportedError(src, pos, "lowering is unavailable")
	var unsupported *FeatureNotSupportedError
	var parse *ParseError
	if !errors.As(err, &unsupported) || !errors.As(err, &parse) {
		t.Fatalf("error chain = %T %v, want feature and parse errors", err, err)
	}
	if parse.Pos != pos || parse.Line != 2 || parse.Col != 6 ||
		parse.Msg != "lowering is unavailable" {
		t.Fatalf("positioned feature refusal = %+v", parse)
	}
}
