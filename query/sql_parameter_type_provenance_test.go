package query

import "testing"

func TestStatementParameterTypeTargetDefaultProvenance(t *testing.T) {
	for _, test := range []struct {
		name          string
		source        string
		wantType      ParameterType
		wantDefaulted bool
	}{
		{
			name:          "unresolved target",
			source:        `SELECT ?`,
			wantType:      ParameterTypeText,
			wantDefaulted: true,
		},
		{
			name:     "simple CASE comparison",
			source:   `SELECT CASE BOOL 't' WHEN ? THEN 1 ELSE 0 END`,
			wantType: ParameterTypeBool,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareStatement(test.source)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			if got := statement.ParameterType(0); got != test.wantType {
				t.Fatalf("ParameterType(0) = %s, want %s", got, test.wantType)
			}
			if got := statement.ParameterTypeTargetDefault(0); got != test.wantDefaulted {
				t.Fatalf("ParameterTypeTargetDefault(0) = %v, want %v",
					got, test.wantDefaulted)
			}
			if allocations := testing.AllocsPerRun(1000, func() {
				_ = statement.ParameterTypeTargetDefault(0)
			}); allocations != 0 {
				t.Fatalf("provenance lookup allocated %.2f times, want zero", allocations)
			}
		})
	}
}

func TestWindowSchemaPreservesTypedInputsAndValueFunctions(t *testing.T) {
	for _, source := range []string{
		`SELECT q.flag,
			LAG(q.flag) OVER () AS previous_flag,
			q.label,
			FIRST_VALUE(q.label) OVER () AS first_label,
			ROW_NUMBER() OVER () AS position
		FROM (SELECT BOOL 't' AS flag, TEXT 'x' AS label) AS q`,
		`WITH q AS (SELECT BOOL 't' AS flag, TEXT 'x' AS label)
		SELECT q.flag,
			LAG(q.flag) OVER () AS previous_flag,
			q.label,
			FIRST_VALUE(q.label) OVER () AS first_label,
			ROW_NUMBER() OVER () AS position
		FROM q`,
	} {
		statement, err := PrepareStatement(source)
		if err != nil {
			t.Fatalf("PrepareStatement(%q): %v", source, err)
		}
		schema := statement.AppendSchema(nil)
		statement.Release()
		if len(schema) != 5 {
			t.Fatalf("window schema width = %d, want 5: %+v", len(schema), schema)
		}
		wantTypes := [...]ValueType{TypeBool, TypeBool, TypeString, TypeString, TypeNumber}
		wantRepresentations := [...]OutputRepresentation{
			OutputSQLBool, OutputSQLBool, OutputSQLText, OutputSQLText, OutputJSON,
		}
		for index := range schema {
			if schema[index].Type != wantTypes[index] ||
				schema[index].Representation != wantRepresentations[index] {
				t.Fatalf("window schema[%d] = %+v, want type %d representation %d",
					index, schema[index], wantTypes[index], wantRepresentations[index])
			}
		}
		if schema[4].Reduction != ReductionWindowInteger {
			t.Fatalf("ROW_NUMBER reduction = %d, want %d",
				schema[4].Reduction, ReductionWindowInteger)
		}
	}
}
