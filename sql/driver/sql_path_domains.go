package driver

import (
	"context"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

// proveSQLPathComparisonDomains authenticates the one optimization that may
// prune a runtime-typed decorrelated equality: both physical columns must have
// the same single declared live scalar domain. The query retains only that
// tiny proof, never catalog/schema pointers or this resolver closure.
func (c *conn) proveSQLPathComparisonDomains(
	ctx context.Context,
	source string,
	tree *sqlast.SelectStmt,
	statement *query.Statement,
) error {
	if c == nil || c.db == nil || statement == nil {
		return nil
	}
	if err := rlockContext(ctx, &c.db.mu); err != nil {
		return err
	}
	defer c.db.mu.RUnlock()
	resolve := func(
		collection, pointer string,
	) query.SQLPathDomain {
		return c.resolveDeclaredSQLPathDomain(collection, pointer)
	}
	if err := validateDeclaredSQLPathComparisonDomains(source, tree, resolve); err != nil {
		return err
	}
	statement.ProveSQLPathComparisonDomains(resolve)
	return nil
}

func (c *conn) validateSQLPathComparisonDomains(
	ctx context.Context,
	source string,
	tree *sqlast.SelectStmt,
) error {
	if c == nil || c.db == nil || tree == nil {
		return nil
	}
	if err := rlockContext(ctx, &c.db.mu); err != nil {
		return err
	}
	defer c.db.mu.RUnlock()
	return validateDeclaredSQLPathComparisonDomains(
		source, tree, c.resolveDeclaredSQLPathDomain,
	)
}

// resolveDeclaredSQLPathDomain requires the database read lock. Keeping the
// transaction-incarnation rule in one resolver makes SELECT and DML analysis
// use the exact same catalog snapshot as their later execution.
func (c *conn) resolveDeclaredSQLPathDomain(
	collection, pointer string,
) query.SQLPathDomain {
	var relation *table
	if c.tx != nil {
		if layout, exists := c.tx.tableLayoutAtBegin(collection); exists {
			relation = layout.incarnation
		}
	}
	if relation == nil {
		relation = c.db.tables[collection]
	}
	if relation == nil {
		return query.SQLPathDomainUnknown
	}
	return declaredSQLPathDomain(relation.meta, pointer)
}

// validateDeclaredSQLPathComparisonDomains is PostgreSQL's analysis-time lane:
// when both authored operands name physical columns with single declared live
// domains, the catalog can resolve the operator without reading a row. This is
// what makes an incompatible query fail on an empty table and before LIMIT 0.
// ANY, unions, derived outputs, and correlations deliberately remain on the
// complete runtime scan rather than accepting a guess as a type proof.
func validateDeclaredSQLPathComparisonDomains(
	source string,
	statement *sqlast.SelectStmt,
	resolve func(collection, pointer string) query.SQLPathDomain,
) error {
	if statement == nil || resolve == nil {
		return nil
	}
	validate := func(expr *sqlast.Expr) error {
		return walkDeclaredSQLPathComparisons(expr, func(comparison *sqlast.Expr) error {
			left := declaredPathDomain(statement, comparison.Path, resolve)
			right := declaredPathDomain(statement, comparison.RightPath, resolve)
			if left == query.SQLPathDomainUnknown || right == query.SQLPathDomainUnknown ||
				left == right {
				return nil
			}
			position := comparison.Value.Pos
			if position == 0 {
				position = comparison.Pos
			}
			return sqlast.NewUndefinedComparisonOperatorError(
				source, position, sqlPathDomainName(left),
				comparison.Op.String(), sqlPathDomainName(right),
			)
		})
	}
	// transformSelectStmt resolves FROM/JOIN expressions first, then target
	// entries, WHERE/HAVING, and finally sort expressions. Preserve that order
	// so two independent 42883 candidates choose the same authored operator as
	// PostgreSQL rather than whichever walk was most convenient here.
	for i := range statement.From {
		if statement.From[i].On != nil {
			if err := validate(statement.From[i].On.Expr); err != nil {
				return err
			}
		}
	}
	for i := range statement.Columns {
		if err := walkDeclaredSQLScalarPathComparisons(
			statement.Columns[i].Scalar,
			func(comparison *sqlast.Expr) error { return validate(comparison) },
		); err != nil {
			return err
		}
	}
	if err := validate(statement.Where); err != nil {
		return err
	}
	if err := validate(statement.Having); err != nil {
		return err
	}
	for i := range statement.OrderBy {
		if err := walkDeclaredSQLScalarPathComparisons(
			statement.OrderBy[i].Scalar,
			func(comparison *sqlast.Expr) error { return validate(comparison) },
		); err != nil {
			return err
		}
	}
	return nil
}

func walkDeclaredSQLScalarPathComparisons(
	expr *sqlast.ScalarExpr,
	visit func(*sqlast.Expr) error,
) error {
	if expr == nil {
		return nil
	}
	if err := walkDeclaredSQLScalarPathComparisons(expr.Left, visit); err != nil {
		return err
	}
	if err := walkDeclaredSQLScalarPathComparisons(expr.Right, visit); err != nil {
		return err
	}
	for i := range expr.Whens {
		arm := &expr.Whens[i]
		if err := walkDeclaredSQLPathComparisons(arm.Predicate, visit); err != nil {
			return err
		}
		if err := walkDeclaredSQLScalarPathComparisons(arm.Match, visit); err != nil {
			return err
		}
		if err := walkDeclaredSQLScalarPathComparisons(arm.Result, visit); err != nil {
			return err
		}
	}
	return walkDeclaredSQLScalarPathComparisons(expr.Else, visit)
}

func walkDeclaredSQLPathComparisons(
	expr *sqlast.Expr,
	visit func(*sqlast.Expr) error,
) error {
	if expr == nil {
		return nil
	}
	if expr.Kind == sqlast.ExprCompare && expr.RightPath != nil {
		if err := visit(expr); err != nil {
			return err
		}
	}
	for _, child := range expr.Kids {
		if err := walkDeclaredSQLPathComparisons(child, visit); err != nil {
			return err
		}
	}
	if err := walkDeclaredSQLScalarPathComparisons(expr.ScalarLeft, visit); err != nil {
		return err
	}
	if err := walkDeclaredSQLScalarPathComparisons(expr.ScalarRight, visit); err != nil {
		return err
	}
	return nil
}

func declaredPathDomain(
	statement *sqlast.SelectStmt,
	path *sqlast.PathExpr,
	resolve func(collection, pointer string) query.SQLPathDomain,
) query.SQLPathDomain {
	if statement == nil || path == nil || path.Source < 0 ||
		path.Source >= len(statement.From) {
		return query.SQLPathDomainUnknown
	}
	relation := &statement.From[path.Source]
	if relation.Kind != sqlast.RelationCollection || relation.Name == "" {
		return query.SQLPathDomainUnknown
	}
	return resolve(relation.Name, string(path.AppendPointer(nil)))
}

func sqlPathDomainName(domain query.SQLPathDomain) string {
	switch domain {
	case query.SQLPathDomainBoolean:
		return "boolean"
	case query.SQLPathDomainNumeric:
		return "numeric"
	case query.SQLPathDomainText:
		return "text"
	default:
		return "unknown"
	}
}

func declaredSQLPathDomain(meta *tableMeta, pointer string) query.SQLPathDomain {
	if meta == nil || meta.Schema == nil {
		return query.SQLPathDomainUnknown
	}
	for i := range meta.Schema.Fields {
		field := &meta.Schema.Fields[i]
		if field.Path != pointer {
			continue
		}
		types := store.SchemaType(field.Types) &^ store.SchemaNull
		switch {
		case types == store.SchemaBool:
			return query.SQLPathDomainBoolean
		case types != 0 && types&^(store.SchemaNumber|store.SchemaInteger) == 0:
			return query.SQLPathDomainNumeric
		case types == store.SchemaString:
			return query.SQLPathDomainText
		default:
			return query.SQLPathDomainUnknown
		}
	}
	return query.SQLPathDomainUnknown
}

// ValidateSQLPathComparisonDomains applies the driver's analysis-time operator
// resolution to a complete parsed SELECT graph. Adapters provide domains from
// their pinned, authenticated schema catalog; unknown domains remain runtime
// checks. The resolver and catalog are never retained by this function.
func ValidateSQLPathComparisonDomains(source string, statement *sqlast.SelectStmt, resolve func(collection, pointer string) query.SQLPathDomain) error {
	return sqlast.WalkSelectStatements(statement, func(s *sqlast.SelectStmt) error {
		if s.Set != nil {
			return nil
		}
		return validateDeclaredSQLPathComparisonDomains(source, s, resolve)
	})
}
