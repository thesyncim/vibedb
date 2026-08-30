package gateway

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestGatewayTypedValidationPrecedesRouting(t *testing.T) {
	executor := NewExecutor(nil, nil, Options{})
	invalid := Query{
		SQL:        "SELECT ?",
		Params:     []shardservice.Param{shardservice.NullParam()},
		ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeInvalid},
		Class:      ClassInteractive,
	}
	tests := []struct {
		name string
		run  func() error
	}{
		{"query", func() error { _, err := executor.Query(t.Context(), invalid); return err }},
		{"exec", func() error { _, err := executor.Exec(t.Context(), invalid); return err }},
		{"explain", func() error { _, err := executor.Explain(t.Context(), invalid); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); !errors.Is(err, ErrPlanParameters) {
				t.Fatalf("error = %v, want typed admission refusal", err)
			}
		})
	}

	// A valid transport enum that contradicts its authored scalar context must
	// also fail during semantic validation, before the nil catalog is touched.
	contradictory := Query{
		SQL: "SELECT ? UNION ALL SELECT ?",
		Params: []shardservice.Param{
			shardservice.NullParam(), shardservice.NullParam(),
		},
		ParamTypes: []sqldriver.ParamType{
			sqldriver.ParamTypeBool, sqldriver.ParamTypeText,
		},
		Class: ClassInteractive,
	}
	if _, err := executor.Query(t.Context(), contradictory); err == nil || errors.Is(err, ErrNoCatalog) {
		t.Fatalf("typed semantic error = %v, want refusal before catalog routing", err)
	}
}

func TestValidateSQLParameterTypesRejectsNonCanonicalMetadata(t *testing.T) {
	tests := []struct {
		name   string
		params []shardservice.Param
		types  []sqldriver.ParamType
	}{
		{
			name:   "count mismatch",
			params: []shardservice.Param{shardservice.NullParam()},
			types:  []sqldriver.ParamType{sqldriver.ParamTypeBool, sqldriver.ParamTypeText},
		},
		{
			name:   "all unspecified",
			params: []shardservice.Param{shardservice.NullParam()},
			types:  []sqldriver.ParamType{sqldriver.ParamTypeUnspecified},
		},
		{
			name:   "invalid enum",
			params: []shardservice.Param{shardservice.NullParam()},
			types:  []sqldriver.ParamType{sqldriver.ParamTypeInvalid},
		},
		{
			name:   "typed document",
			params: []shardservice.Param{shardservice.DocumentParam(`{"id":"a"}`)},
			types:  []sqldriver.ParamType{sqldriver.ParamTypeOther},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateSQLParameterTypes(test.params, test.types); !errors.Is(err, ErrPlanParameters) {
				t.Fatalf("error = %v, want ErrPlanParameters", err)
			}
		})
	}
}

func TestExecBatchAdmitsEveryItemBeforeCatalogOrSemanticPreparation(t *testing.T) {
	executor := NewExecutor(nil, nil, Options{})
	queries := []Query{
		{
			// This statement is intentionally analyzer-invalid. The later
			// transport error must win without parsing this SQL or pinning a
			// catalog generation.
			SQL: "DELETE FROM messages WHERE id IN (SELECT BOOL 't' UNION ALL SELECT ?)",
			Params: []shardservice.Param{
				shardservice.NullParam(),
			},
			ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeText},
			Class:      ClassInteractive,
		},
		{
			SQL:    "DELETE FROM messages WHERE id = ?",
			Params: []shardservice.Param{shardservice.NullParam()},
			ParamTypes: []sqldriver.ParamType{
				sqldriver.ParamTypeInvalid,
			},
			Class: ClassInteractive,
		},
	}
	if _, err := executor.ExecBatch(t.Context(), queries); !errors.Is(err, ErrPlanParameters) {
		t.Fatalf("ExecBatch error = %v, want later metadata refusal before parse/pin", err)
	}
}
