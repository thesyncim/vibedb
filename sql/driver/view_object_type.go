package driver

import (
	"context"
	"errors"
	"fmt"

	sqlast "github.com/thesyncim/vibedb/sql"
)

// wrongObjectTypeError is the catalog-boundary diagnostic for a relation that
// exists under the authored name but cannot serve the operation's required
// object kind. Keeping the operation and hint on the error lets wire adapters
// preserve one 42809 taxonomy without guessing intent from message text.
type wrongObjectTypeError struct {
	operation string
	name      string
	actual    string
	expected  string
	hint      string
	pos       int
}

func (e *wrongObjectTypeError) Error() string {
	return fmt.Sprintf(
		"%v: %s requires a %s; relation %q is a %s",
		ErrWrongObjectType, e.operation, e.expected, e.name, e.actual,
	)
}

func (e *wrongObjectTypeError) Unwrap() error   { return ErrWrongObjectType }
func (e *wrongObjectTypeError) Position() int   { return e.pos }
func (e *wrongObjectTypeError) SQLHint() string { return e.hint }

func wrongDropViewObjectType(name string, pos int) error {
	return &wrongObjectTypeError{
		operation: "DROP VIEW",
		name:      name,
		actual:    "table",
		expected:  "view",
		hint:      "use DROP TABLE to remove a table",
		pos:       pos,
	}
}

type viewTableTarget struct {
	operation string
	name      string
	hint      string
	pos       int
}

// preparedDMLViewSourceError closes the catalog-generation race for a
// prepared UPDATE or DELETE whose authored nested relation was a base table at
// prepare time but resolves to a durable view at execution. It is both a typed
// FeatureNotSupported refusal and an ErrViewChanged generation failure:
// database/sql callers retain the semantic class and source position, while
// pgwire retains SQLSTATE 0A000 and its established reprepare hint.
//
// ParseError's line and column cannot be reconstructed here because the hot
// prepared statement intentionally does not retain a second copy of its SQL.
// Position is authoritative at this layer and pgwire converts it against the
// portal's original SQL text. Error deliberately avoids formatting the
// incomplete ParseError location.
type preparedDMLViewSourceError struct {
	unsupported sqlast.FeatureNotSupportedError
	hint        string
}

func newPreparedDMLViewSourceError(
	operation, name string,
	pos int,
) *preparedDMLViewSourceError {
	hint := fmt.Sprintf(
		"prepare the %s statement again so it binds the current relation kind",
		operation,
	)
	return &preparedDMLViewSourceError{
		unsupported: sqlast.FeatureNotSupportedError{ParseError: sqlast.ParseError{
			Pos: pos,
			Msg: fmt.Sprintf(
				"prepared %s query source relation %q now resolves to a durable view; %s",
				operation, name, hint,
			),
		}},
		hint: hint,
	}
}

func (e *preparedDMLViewSourceError) Error() string {
	return e.unsupported.Msg
}

func (e *preparedDMLViewSourceError) Unwrap() error {
	return &e.unsupported
}

func (e *preparedDMLViewSourceError) Is(target error) bool {
	return target == ErrViewChanged
}

func (e *preparedDMLViewSourceError) Position() int   { return e.unsupported.Pos }
func (e *preparedDMLViewSourceError) SQLHint() string { return e.hint }

func tableTargetRequiringBaseRelation(statement *sqlast.Statement) (viewTableTarget, bool) {
	if statement == nil {
		return viewTableTarget{}, false
	}
	switch statement.Kind {
	case sqlast.KindDropTable:
		if statement.DropTable != nil {
			return viewTableTarget{
				operation: "DROP TABLE", name: statement.DropTable.Table,
				hint: "use DROP VIEW to remove a view", pos: statement.DropTable.Pos,
			}, true
		}
	case sqlast.KindTruncate:
		if statement.Truncate != nil {
			return viewTableTarget{
				operation: "TRUNCATE", name: statement.Truncate.Table,
				hint: "truncate a base table instead; views are read-only",
				pos:  statement.Truncate.Pos,
			}, true
		}
	case sqlast.KindCreateIndex:
		if statement.CreateIndex != nil {
			return viewTableTarget{
				operation: "CREATE INDEX", name: statement.CreateIndex.Table,
				hint: "create the index on a base table instead; views have no independent storage",
				pos:  statement.CreateIndex.Pos,
			}, true
		}
	case sqlast.KindDropIndex:
		if statement.DropIndex != nil && statement.DropIndex.HasTable {
			return viewTableTarget{
				operation: "DROP INDEX", name: statement.DropIndex.Table,
				hint: "drop the index from its base table instead; views have no independent indexes",
				pos:  statement.DropIndex.TablePos,
			}, true
		}
	case sqlast.KindInsert:
		if statement.Insert != nil {
			return viewTableTarget{
				operation: "INSERT", name: statement.Insert.Table,
				hint: "insert into a base table instead; views are read-only",
				pos:  statement.Insert.Pos,
			}, true
		}
	case sqlast.KindUpdate:
		if statement.Update != nil {
			return viewTableTarget{
				operation: "UPDATE", name: statement.Update.Table,
				hint: "update a base table instead; views are read-only",
				pos:  statement.Update.Pos,
			}, true
		}
	case sqlast.KindDelete:
		if statement.Delete != nil {
			return viewTableTarget{
				operation: "DELETE", name: statement.Delete.Table,
				hint: "delete from a base table instead; views are read-only",
				pos:  statement.Delete.Pos,
			}, true
		}
	}
	return viewTableTarget{}, false
}

func wrongViewTableTarget(
	statement *sqlast.Statement,
	views map[string]*viewMeta,
) error {
	target, relevant := tableTargetRequiringBaseRelation(statement)
	if !relevant || views[target.name] == nil {
		return nil
	}
	return &wrongObjectTypeError{
		operation: target.operation,
		name:      target.name,
		actual:    "view",
		expected:  "table",
		hint:      target.hint,
		pos:       target.pos,
	}
}

// validateViewStatementContext performs the exact view-catalog checks needed
// before generic table validation. Ordinary SELECT preparation never enters
// this path. INSERT enters only for its write target; its Source is deliberately
// excluded from refusal because expandPreparedViews owns that supported path.
func (c *conn) validateViewStatementContext(
	ctx context.Context,
	source string,
	statement *sqlast.Statement,
) error {
	_, hasTableTarget := tableTargetRequiringBaseRelation(statement)
	hasUnexpandedQuery := statement != nil &&
		(statement.Kind == sqlast.KindUpdate || statement.Kind == sqlast.KindDelete)
	if !hasTableTarget && !hasUnexpandedQuery {
		return nil
	}
	var views map[string]*viewMeta
	if c.tx != nil {
		if c.tx.done {
			return errors.New("vibedb: transaction is finished")
		}
		views = c.tx.views
	} else {
		if err := rlockContext(ctx, &c.db.mu); err != nil {
			return err
		}
		defer c.db.mu.RUnlock()
		views = c.db.catalog.Views
	}
	if err := wrongViewTableTarget(statement, views); err != nil {
		return err
	}
	return rejectUnexpandedDMLViewReferences(source, statement, views)
}

// validateViewTableTargetLocked closes the prepare/execute race. Callers hold
// d.mu, so the object-kind decision and the following table operation observe
// one catalog generation.
func (d *database) validateViewTableTargetLocked(statement *sqlast.Statement) error {
	if err := wrongViewTableTarget(statement, d.catalog.Views); err != nil {
		return err
	}
	return rejectReboundPreparedDMLViewReferences(statement, d.catalog.Views)
}

func (t *tx) validateViewTableTarget(statement *sqlast.Statement) error {
	if err := wrongViewTableTarget(statement, t.views); err != nil {
		return err
	}
	t.conn.db.mu.RLock()
	err := wrongViewTableTarget(statement, t.conn.db.catalog.Views)
	if err == nil {
		err = rejectReboundPreparedDMLViewReferences(statement, t.views)
	}
	if err == nil {
		err = rejectReboundPreparedDMLViewReferences(
			statement, t.conn.db.catalog.Views,
		)
	}
	t.conn.db.mu.RUnlock()
	return err
}
