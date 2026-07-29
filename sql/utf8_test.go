package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestEveryParserEntryPointRejectsInvalidUTF8AtItsFirstBadByte(t *testing.T) {
	selectSQL := "SELECT café FROM docs\nWHERE café = '\xff'"
	selectBad := strings.IndexByte(selectSQL, 0xff)
	dmlSQL := "INSERT INTO docs VALUES ({\"name\":\"\xff\"})"
	dmlBad := strings.IndexByte(dmlSQL, 0xff)

	t.Run("reusable SELECT parser", func(t *testing.T) {
		var parser Parser
		dst := SelectStmt{Columns: []ResultColumn{{}}}
		err := parser.Parse(&dst, selectSQL)
		expectInvalidUTF8ParseError(t, err, selectBad)
		if len(dst.Columns) != 0 || len(dst.From) != 0 {
			t.Fatal("rejected invalid UTF-8 left a partial SELECT behind")
		}
	})

	t.Run("owning SELECT parser", func(t *testing.T) {
		stmt, err := Parse(selectSQL)
		expectInvalidUTF8ParseError(t, err, selectBad)
		if stmt != nil {
			t.Fatal("Parse returned a statement for invalid UTF-8")
		}
	})

	t.Run("reusable all-statement parser", func(t *testing.T) {
		var parser Parser
		dst := Statement{Kind: KindInsert, Insert: &InsertStmt{}}
		err := parser.ParseStatement(&dst, dmlSQL)
		expectInvalidUTF8ParseError(t, err, dmlBad)
		if dst != (Statement{}) {
			t.Fatal("rejected invalid UTF-8 left a partial statement behind")
		}
	})

	t.Run("owning all-statement parser", func(t *testing.T) {
		stmt, err := ParseStatement(dmlSQL)
		expectInvalidUTF8ParseError(t, err, dmlBad)
		if stmt != nil {
			t.Fatal("ParseStatement returned a statement for invalid UTF-8")
		}
	})
}

func expectInvalidUTF8ParseError(t *testing.T, err error, wantPos int) {
	t.Helper()
	if err == nil {
		t.Fatal("invalid UTF-8 parsed successfully")
	}
	var parse *ParseError
	if !errors.As(err, &parse) {
		t.Fatalf("invalid UTF-8 returned %T, want *ParseError", err)
	}
	if parse.Pos != wantPos {
		t.Fatalf("invalid UTF-8 position = %d, want first bad byte %d", parse.Pos, wantPos)
	}
	if parse.Line < 1 || parse.Col < 1 || !strings.Contains(parse.Msg, "UTF-8") {
		t.Fatalf("invalid UTF-8 error is not positioned/actionable: %+v", parse)
	}
}
