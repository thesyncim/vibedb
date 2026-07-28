package driver

import (
	"encoding/json"
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func primaryPredicateKeys(where *sqlast.Expr, primaryKey string, args []any) ([]string, bool, error) {
	if where == nil || where.Path == nil ||
		string(where.Path.AppendPointer(nil)) != primaryKey {
		return nil, false, nil
	}
	var operands []sqlast.Operand
	switch where.Kind {
	case sqlast.ExprCompare:
		if where.Op != sqlast.OpEq {
			return nil, false, nil
		}
		operands = []sqlast.Operand{where.Value}
	case sqlast.ExprIn:
		if where.Negated {
			return nil, false, nil
		}
		operands = where.List
	default:
		return nil, false, nil
	}
	keys := make([]string, 0, len(operands))
	for _, operand := range operands {
		value, err := operandValue(operand, args)
		if err != nil {
			return nil, true, err
		}
		if value == nil {
			return nil, true, fmt.Errorf("vibedb: a primary key predicate cannot be NULL")
		}
		switch value.(type) {
		case string, bool, int64, float64, json.Number:
		default:
			return nil, true, fmt.Errorf("vibedb: %T is not a scalar primary key", value)
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, true, err
		}
		key := string(raw)
		duplicate := false
		for _, prior := range keys {
			if prior == key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			keys = append(keys, key)
		}
	}
	return keys, true, nil
}
