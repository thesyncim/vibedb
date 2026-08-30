package query

import (
	"errors"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestStatementTypedValuesParameterMetadata(t *testing.T) {
	statement, err := PrepareStatement(
		`VALUES (BOOL 't', TEXT 'x'), (?, ?)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	if statement.ParameterType(0) != ParameterTypeBool ||
		statement.ParameterType(1) != ParameterTypeText {
		t.Fatalf("typed parameter metadata = %s/%s, want boolean/text",
			statement.ParameterType(0), statement.ParameterType(1))
	}
	if statement.ParameterType(-1) != ParameterTypeInvalid ||
		statement.ParameterType(2) != ParameterTypeInvalid {
		t.Fatalf("out-of-range metadata = %s/%s, want invalid",
			statement.ParameterType(-1), statement.ParameterType(2))
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if statement.ParameterType(0) != ParameterTypeBool ||
			statement.ParameterType(1) != ParameterTypeText {
			panic("unstable parameter metadata")
		}
	}); allocs != 0 {
		t.Fatalf("parameter metadata lookup allocated %.2f times, want zero", allocs)
	}
}

func TestStatementAllUnknownValuesFinalizesText(t *testing.T) {
	statement, err := PrepareStatement(`VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if got := statement.ParameterType(0); got != ParameterTypeText {
		t.Fatalf("all-unknown VALUES parameter type = %s, want text", got)
	}
	schema := statement.AppendSchema(nil)
	if len(schema) != 1 || schema[0].Type != TypeString ||
		schema[0].Representation != OutputSQLText {
		t.Fatalf("all-unknown VALUES schema = %+v, want SQL text", schema)
	}
}

func TestStatementStandaloneUnknownScalarAndSetFinalizeText(t *testing.T) {
	for _, source := range []string{
		`SELECT ?`,
		`SELECT ? UNION ALL SELECT ?`,
	} {
		statement, err := PrepareStatement(source)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", source, err)
		}
		for parameter := 0; parameter < statement.NumParams(); parameter++ {
			if got := statement.ParameterType(parameter); got != ParameterTypeText {
				statement.Release()
				t.Fatalf("ParameterType(%q, %d) = %s, want text", source, parameter, got)
			}
		}
		schema := statement.AppendSchema(nil)
		if len(schema) != 1 || schema[0].Type != TypeString ||
			schema[0].Representation != OutputSQLText {
			statement.Release()
			t.Fatalf("schema(%q) = %+v, want SQL text", source, schema)
		}
		statement.Release()
	}

	statement, err := PrepareStatement(`SELECT ?`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var execution Exec
	value := "7"
	args := []any{&value}
	run := func() {
		cursor, runErr := statement.RunInto(&execution, Source{}, args)
		if runErr != nil || !cursor.Next() {
			panic("standalone unknown scalar execution failed")
		}
		if value, ok := cursor.Cell(0).Text(); !ok || value != "7" || cursor.Next() {
			panic("standalone unknown scalar was not coerced to text")
		}
	}
	run()
	if allocations := testing.AllocsPerRun(100, run); allocations != 0 {
		t.Fatalf("warmed standalone text parameter allocated %.2f/run", allocations)
	}
}

func TestStatementDeclaredStringScalarOutputPreservesExactDomain(t *testing.T) {
	const source = `SELECT ?`
	tree, err := sqlast.ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		parameter      ParameterType
		representation OutputRepresentation
	}{
		{ParameterTypeVarchar, OutputSQLVarchar},
		{ParameterTypeName, OutputSQLName},
		{ParameterTypeBPChar, OutputSQLBPChar},
	} {
		statement, err := PrepareParsedStatementWithParameterTypes(
			source, tree.Select, []ParameterType{test.parameter},
		)
		if err != nil {
			t.Fatalf("%s: %v", test.parameter, err)
		}
		schema := statement.AppendSchema(nil)
		if len(schema) != 1 || schema[0].Type != TypeString ||
			schema[0].Representation != test.representation {
			statement.Release()
			t.Fatalf("%s schema = %+v", test.parameter, schema)
		}
		statement.Release()
	}
}

func TestStatementSetStringCategorySelectionMatchesPostgreSQL(t *testing.T) {
	const source = `SELECT ? UNION ALL SELECT ?`
	tree, err := sqlast.ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		hints  []ParameterType
		want   ParameterType
		output OutputRepresentation
	}{
		{"varchar plus unknown", []ParameterType{ParameterTypeVarchar}, ParameterTypeVarchar, OutputSQLVarchar},
		{"varchar then name", []ParameterType{ParameterTypeVarchar, ParameterTypeName}, ParameterTypeName, OutputSQLName},
		{"text then varchar", []ParameterType{ParameterTypeText, ParameterTypeVarchar}, ParameterTypeText, OutputSQLText},
		{"bpchar then varchar", []ParameterType{ParameterTypeBPChar, ParameterTypeVarchar}, ParameterTypeBPChar, OutputSQLBPChar},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareParsedStatementWithParameterTypes(
				source, tree.Select, test.hints,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			for parameter := range statement.NumParams() {
				if got := statement.ParameterType(parameter); got != test.want {
					t.Fatalf("parameter %d = %s, want %s", parameter, got, test.want)
				}
			}
			schema := statement.AppendSchema(nil)
			if len(schema) != 1 || schema[0].Representation != test.output {
				t.Fatalf("schema = %+v, want representation %d", schema, test.output)
			}
		})
	}
}

func TestStatementSetCommonTypePropagatesToParameterMetadata(t *testing.T) {
	for _, test := range []struct {
		source string
		want   ParameterType
	}{
		{`SELECT ? UNION ALL SELECT BOOL 't'`, ParameterTypeBool},
		{`(SELECT true UNION SELECT ? ORDER BY 1) UNION ALL SELECT BOOL 'f'`, ParameterTypeBool},
		{`SELECT * FROM (SELECT BOOL 't' UNION ALL SELECT ?) AS q`, ParameterTypeBool},
		{`WITH q AS (SELECT BOOL 't' UNION ALL SELECT ?) SELECT * FROM q`, ParameterTypeBool},
		{
			`SELECT id FROM users WHERE id IN ` +
				`(SELECT TEXT 'x' UNION ALL SELECT ?) UNION ALL SELECT id FROM users`,
			ParameterTypeText,
		},
	} {
		statement, err := PrepareStatement(test.source)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", test.source, err)
		}
		if got := statement.ParameterType(0); got != test.want {
			statement.Release()
			t.Fatalf("ParameterType(%q) = %s, want %s", test.source, got, test.want)
		}
		statement.Release()
	}
}

func TestDMLStatementFilterParameterMetadata(t *testing.T) {
	for _, source := range []string{
		`DELETE FROM users WHERE id IN ` +
			`(SELECT TEXT 'x' UNION ALL SELECT ? FROM other)`,
		`UPDATE users SET enabled = true WHERE id IN ` +
			`(SELECT TEXT 'x' UNION ALL SELECT ? FROM other)`,
	} {
		statement, err := PrepareDML(source)
		if err != nil {
			t.Fatalf("PrepareDML(%q): %v", source, err)
		}
		if got := statement.ParameterType(0); got != ParameterTypeText {
			statement.Release()
			t.Fatalf("ParameterType(%q) = %s, want text", source, got)
		}
		statement.Release()
	}
}

func TestDMLNilParameterTypesPreserveAbsentMetadata(t *testing.T) {
	const source = `DELETE FROM users WHERE id = ?`
	tree, err := sqlast.ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := PrepareParsedDMLWithParameterTypes(source, tree, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.paramTypes != nil || statement.paramTypePositions != nil ||
		statement.ParameterType(0) != ParameterTypeUnspecified ||
		statement.ParameterTypePosition(0) != -1 {
		t.Fatalf("nil-typed DML retained metadata: types=%v positions=%v type=%s position=%d",
			statement.paramTypes, statement.paramTypePositions,
			statement.ParameterType(0), statement.ParameterTypePosition(0))
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if statement.ParameterType(0) != ParameterTypeUnspecified ||
			statement.ParameterTypePosition(0) != -1 {
			panic("unstable absent DML parameter metadata")
		}
	}); allocations != 0 {
		t.Fatalf("absent DML parameter metadata lookup allocated %.2f times, want zero",
			allocations)
	}
}

func TestDMLParameterTypeHintsAcceptEverySupportedDomain(t *testing.T) {
	const source = `DELETE FROM users WHERE id = ?`
	tree, err := sqlast.ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, parameterType := range []ParameterType{
		ParameterTypeBool,
		ParameterTypeText,
		ParameterTypeVarchar,
		ParameterTypeName,
		ParameterTypeBPChar,
		ParameterTypeOther,
	} {
		statement, err := PrepareParsedDMLWithParameterTypes(
			source, tree, []ParameterType{parameterType},
		)
		if err != nil {
			t.Fatalf("parameter type %s: %v", parameterType, err)
		}
		if got := statement.ParameterType(0); got != parameterType {
			statement.Release()
			t.Fatalf("parameter type %s forwarded as %s", parameterType, got)
		}
		statement.Release()
	}
	if _, err := PrepareParsedDMLWithParameterTypes(
		source, tree, []ParameterType{ParameterTypeInvalid},
	); !errors.Is(err, ErrParameterType) {
		t.Fatalf("invalid DML parameter type = %T %v, want ErrParameterType", err, err)
	}
}

func TestDMLDeclaredParameterTypesReachSelectChildren(t *testing.T) {
	for _, source := range []string{
		`DELETE FROM users WHERE flag IN (SELECT ? UNION ALL SELECT ?)`,
		`UPDATE users SET flag = true WHERE flag IN (SELECT ? UNION ALL SELECT ?)`,
		`INSERT INTO target SELECT * FROM users WHERE flag IN ` +
			`(SELECT ? UNION ALL SELECT ?)`,
	} {
		tree, err := sqlast.ParseStatement(source)
		if err != nil {
			t.Fatalf("ParseStatement(%q): %v", source, err)
		}
		statement, err := PrepareParsedDMLWithParameterTypes(
			source, tree,
			[]ParameterType{ParameterTypeBool, ParameterTypeUnspecified},
		)
		if err != nil {
			t.Fatalf("PrepareParsedDMLWithParameterTypes(%q): %v", source, err)
		}
		if statement.ParameterType(0) != ParameterTypeBool ||
			statement.ParameterType(1) != ParameterTypeBool {
			statement.Release()
			t.Fatalf("declared DML parameter types for %q = %s/%s, want boolean/boolean",
				source, statement.ParameterType(0), statement.ParameterType(1))
		}
		if statement.ParameterTypePosition(0) != strings.Index(source, "?") ||
			statement.ParameterTypePosition(1) != strings.LastIndex(source, "?") {
			statement.Release()
			t.Fatalf("declared DML parameter positions for %q = %d/%d, want %d/%d",
				source, statement.ParameterTypePosition(0),
				statement.ParameterTypePosition(1), strings.Index(source, "?"),
				strings.LastIndex(source, "?"))
		}
		statement.Release()
	}

	source := `DELETE FROM users WHERE flag IN (SELECT ? UNION ALL SELECT ?)`
	tree, err := sqlast.ParseStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = PrepareParsedDMLWithParameterTypes(source, tree,
		[]ParameterType{ParameterTypeBool, ParameterTypeText})
	var mismatch *ScalarTypeError
	position := -1
	if errors.As(err, &mismatch) {
		position = mismatch.Position()
	}
	if position != strings.LastIndex(source, "?") {
		t.Fatalf("declared DML mismatch = %T %v at %d, want ScalarTypeError at %d",
			err, err, position, strings.LastIndex(source, "?"))
	}
}
