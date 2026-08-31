package sql

import (
	"errors"
	"strings"
	"testing"
)

func TestWherePathComparisonsPreserveBothPathsAndOperatorPosition(t *testing.T) {
	tests := []struct {
		op   string
		want CmpOp
	}{
		{"=", OpEq},
		{"<>", OpNe},
		{"<", OpLt},
		{"<=", OpLe},
		{">", OpGt},
		{">=", OpGe},
	}
	for _, test := range tests {
		t.Run(test.op, func(t *testing.T) {
			source := `SELECT d.id FROM docs AS d WHERE d."left value" ` + test.op +
				` d."right value"`
			statement, err := Parse(source)
			if err != nil {
				t.Fatal(err)
			}
			leaf := statement.Where
			if leaf == nil || leaf.Kind != ExprCompare || leaf.Op != test.want ||
				leaf.Path == nil || leaf.RightPath == nil {
				t.Fatalf("WHERE = %+v", leaf)
			}
			if got := leaf.Path.AppendSpec(nil); string(got) != "left value" {
				t.Fatalf("left path = %q", got)
			}
			if got := leaf.RightPath.AppendSpec(nil); string(got) != "right value" {
				t.Fatalf("right path = %q", got)
			}
			if leaf.Path.Source != 0 || leaf.RightPath.Source != 0 {
				t.Fatalf("path sources = %d, %d", leaf.Path.Source, leaf.RightPath.Source)
			}
			if want := strings.Index(source, test.op); leaf.Value.Pos != want {
				t.Fatalf("operator position = %d, want %d", leaf.Value.Pos, want)
			}
		})
	}
}

func TestUndefinedOperatorCarriesPostgreSQLHint(t *testing.T) {
	err := NewUndefinedOperatorError("SELECT a = b", 9, "numeric", "text")
	var hinted interface{ SQLHint() string }
	if !errors.As(err, &hinted) || hinted.SQLHint() != undefinedOperatorHint {
		t.Fatalf("undefined-operator hint = %q", hinted.SQLHint())
	}
}

func TestWherePathComparisonsReachUpdateAndDelete(t *testing.T) {
	for _, source := range []string{
		`UPDATE docs SET "$doc" = ? WHERE old_value = new_value`,
		`DELETE FROM docs WHERE "old value" <> "new value"`,
	} {
		statement, err := ParseStatement(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		var filter *SelectStmt
		if statement.Update != nil {
			filter = statement.Update.Filter
		} else if statement.Delete != nil {
			filter = statement.Delete.Filter
		}
		if filter == nil || filter.Where == nil || filter.Where.RightPath == nil {
			t.Fatalf("Parse(%q) filter = %+v", source, filter)
		}
	}
}
