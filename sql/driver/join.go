package driver

import (
	"context"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	driverDefaultQueryMemory = int64(64 << 20)
	driverMinimumQueryMemory = int64(64 << 10)

	// A heap collection retains more than its source JSON: structural tapes,
	// immutable chunk metadata, key directories, and build-time scratch all
	// contribute. Charging sixteen source bytes plus a fixed row allowance is
	// deliberately conservative. It turns ExecOptions.MemoryBytes into an
	// admission bound for the fallback instead of pretending raw JSON size is
	// the materialized working set.
	joinMaterializationExpansion = int64(16)
	joinMaterializationRowBytes  = int64(512)
)

type joinRowVisitor func(table string, key, document []byte) error
type joinRowSource func(joinRowVisitor) error

// physicalDependency is one distinct catalog collection and the position of
// its first physical reference. Definitions are walked in stable source order;
// repeated references do not multiply snapshots or validation work.
type physicalDependency struct {
	name string
	pos  int
}

// materializeDurableJoinSource copies one already-consistent durable catalog
// cut into the heap executor that supports fan-out. It measures the complete
// input first, so an oversized join fails before a Query starts executing and
// before any heap collection is built.
func (c *conn) materializeDurableJoinSource(
	ctx context.Context,
	catalog *durable.DatabaseSnapshot,
	driving string,
	dependencies []physicalDependency,
) (query.Source, error) {
	rows := func(visit joinRowVisitor) error {
		for _, dependency := range dependencies {
			snapshot, ok := catalog.Collection(dependency.name)
			if !ok {
				return fmt.Errorf(
					"%w: %q is absent from the captured join snapshot",
					ErrTableNotFound, dependency.name)
			}
			if snapshot == nil {
				continue
			}
			if err := snapshot.RangeRaw(func(key, document []byte) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return visit(dependency.name, key, document)
			}); err != nil {
				return err
			}
		}
		return nil
	}
	return c.materializeJoinRows(dependencies, driving, rows)
}

// materializeTransactionJoinSource is the transactional twin. Every base
// snapshot came from BEGIN while the driver catalog held all writers out, and
// pending rows are overlaid exactly as tx.view does. No fresh durable snapshot
// is taken here, so repeatable reads and read-your-writes both survive a join.
func (c *conn) materializeTransactionJoinSource(
	ctx context.Context,
	transaction *tx,
	driving string,
	dependencies []physicalDependency,
) (query.Source, error) {
	rows := func(visit joinRowVisitor) error {
		if transaction.done {
			return fmt.Errorf("vibedb: transaction is finished")
		}
		for _, dependency := range dependencies {
			state, ok := transaction.tables[dependency.name]
			if !ok {
				return missingTableDependency(
					dependency.name, dependency.pos, true,
				)
			}
			if state.snapshot != nil {
				if err := state.snapshot.RangeRaw(func(key, document []byte) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					if _, shadowed := state.pending[string(key)]; shadowed {
						return nil
					}
					return visit(dependency.name, key, document)
				}); err != nil {
					return err
				}
			}
			for _, key := range state.order {
				if err := ctx.Err(); err != nil {
					return err
				}
				mutation := state.pending[key]
				if mutation.remove {
					continue
				}
				if err := visit(dependency.name, []byte(key), mutation.document); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return c.materializeJoinRows(dependencies, driving, rows)
}

func (c *conn) materializeJoinRows(
	dependencies []physicalDependency,
	driving string,
	rows joinRowSource,
) (query.Source, error) {
	if driving == "" && len(dependencies) != 0 {
		// Prepared statements normally resolve this recursively. Retaining a
		// deterministic physical fallback keeps this boundary robust if another
		// relation kind is introduced without its own top-level collection.
		driving = dependencies[0].name
	}
	if driving == "" {
		return query.Source{}, errors.New(
			"vibedb: query has no physical driving collection")
	}
	limit, err := driverQueryMemory(c.exec.Options)
	if err != nil {
		return query.Source{}, err
	}
	budget := joinMaterializationBudget{limit: limit}
	if err := rows(func(table string, key, document []byte) error {
		return budget.add(table, key, document)
	}); err != nil {
		return query.Source{}, err
	}

	var database store.Database
	collections := make(map[string]*store.Collection, len(dependencies))
	for _, dependency := range dependencies {
		collection, err := database.CreateCollection(dependency.name, store.Options{})
		if err != nil {
			return query.Source{}, err
		}
		collections[dependency.name] = collection
	}
	if err := rows(func(table string, key, document []byte) error {
		collection := collections[table]
		if collection == nil {
			return fmt.Errorf("vibedb: join materializer lost table %q", table)
		}
		_, err := collection.Put(string(key), document)
		return err
	}); err != nil {
		return query.Source{}, err
	}
	return query.FromDatabase(database.Snapshot(), driving), nil
}

type joinMaterializationBudget struct {
	limit int64
	used  int64
}

func (b *joinMaterializationBudget) add(
	table string,
	key, document []byte,
) error {
	raw := int64(len(key)) + int64(len(document))
	remaining := b.limit - b.used
	if remaining < joinMaterializationRowBytes ||
		raw > (remaining-joinMaterializationRowBytes)/joinMaterializationExpansion {
		return fmt.Errorf(
			"%w: table %q exceeds %d bytes",
			ErrJoinMaterializationTooLarge, table, b.limit)
	}
	b.used += raw*joinMaterializationExpansion + joinMaterializationRowBytes
	return nil
}

func driverQueryMemory(options query.ExecOptions) (int64, error) {
	memory := options.MemoryBytes
	if memory == 0 {
		memory = driverDefaultQueryMemory
	}
	if memory < driverMinimumQueryMemory {
		return 0, fmt.Errorf("query: MemoryBytes must be at least 64 KiB")
	}
	return memory, nil
}

func selectPhysicalDependencies(statement *sqlast.SelectStmt) []physicalDependency {
	walk := physicalDependencyWalk{includeDefinitions: true}
	if statement != nil {
		walk.dependencies = make([]physicalDependency, 0, len(statement.From))
	}
	walk.appendSelect(statement)
	return walk.dependencies
}

// selectExecutablePhysicalDependencies excludes dormant WITH definitions. It
// is the source-routing twin of selectPhysicalDependencies, whose lexical walk
// is retained for PostgreSQL-style name validation and durable view metadata.
func selectExecutablePhysicalDependencies(
	statement *sqlast.SelectStmt,
) []physicalDependency {
	walk := physicalDependencyWalk{}
	if statement != nil {
		walk.dependencies = make([]physicalDependency, 0, len(statement.From))
	}
	walk.appendSelect(statement)
	return walk.dependencies
}

// physicalDependencyWalk follows relation edges with an optional lexical WITH
// walk. visited closes recursive-CTE self edges and prevents a referenced
// definition from being expanded again after its lexical validation pass.
type physicalDependencyWalk struct {
	dependencies       []physicalDependency
	visited            []*sqlast.SelectStmt
	includeDefinitions bool
}

func (w *physicalDependencyWalk) appendSelect(statement *sqlast.SelectStmt) {
	if statement == nil || w.saw(statement) {
		return
	}
	w.visited = append(w.visited, statement)
	if w.includeDefinitions && statement.With != nil {
		for i := range statement.With.CTEs {
			w.appendSelect(statement.With.CTEs[i].Query)
		}
	}
	if statement.Set != nil {
		w.appendSet(statement.Set.Root)
		return
	}
	for i := range statement.From {
		relation := &statement.From[i]
		switch relation.Kind {
		case sqlast.RelationCollection:
			if relation.Name == "" {
				continue
			}
			if hasPhysicalDependency(w.dependencies, relation.Name) {
				continue
			}
			w.dependencies = append(w.dependencies, physicalDependency{
				name: relation.Name,
				pos:  relation.Pos,
			})
		case sqlast.RelationDerived, sqlast.RelationCTE:
			w.appendSelect(relation.Query)
		}
	}
	w.appendExpr(statement.Where)
	w.appendExpr(statement.Having)
}

func (w *physicalDependencyWalk) appendSet(expression *sqlast.SetExpr) {
	if expression == nil {
		return
	}
	switch expression.Kind {
	case sqlast.SetSelectExpr:
		w.appendSelect(expression.Select)
	case sqlast.SetValuesExpr:
		return
	case sqlast.SetTableExpr:
		w.appendSelect(expression.Select)
	case sqlast.SetBinaryExpr:
		w.appendSet(expression.Left)
		w.appendSet(expression.Right)
	case sqlast.SetGroupExpr:
		w.appendSet(expression.Child)
	}
}

func (w *physicalDependencyWalk) appendExpr(e *sqlast.Expr) {
	if e == nil {
		return
	}
	if e.Subquery != nil {
		w.appendSelect(e.Subquery)
	}
	for _, kid := range e.Kids {
		w.appendExpr(kid)
	}
}

func (w *physicalDependencyWalk) saw(statement *sqlast.SelectStmt) bool {
	for i := range w.visited {
		if w.visited[i] == statement {
			return true
		}
	}
	return false
}

func hasPhysicalDependency(dependencies []physicalDependency, name string) bool {
	for i := range dependencies {
		if dependencies[i].name == name {
			return true
		}
	}
	return false
}

func selectContainsJoin(statement *sqlast.SelectStmt) bool {
	if statement == nil {
		return false
	}
	if statement.Set != nil {
		return setContainsJoin(statement.Set.Root)
	}
	if len(statement.From) > 1 {
		return true
	}
	if statement.With != nil {
		for i := range statement.With.CTEs {
			definition := &statement.With.CTEs[i]
			if definition.Query != nil && selectContainsJoin(definition.Query) {
				return true
			}
		}
	}
	for i := range statement.From {
		relation := &statement.From[i]
		if relation.Kind == sqlast.RelationDerived &&
			relation.Query != nil && selectContainsJoin(relation.Query) {
			return true
		}
	}
	return exprContainsJoin(statement.Where) || exprContainsJoin(statement.Having)
}

func setContainsJoin(expression *sqlast.SetExpr) bool {
	if expression == nil {
		return false
	}
	switch expression.Kind {
	case sqlast.SetSelectExpr:
		return selectContainsJoin(expression.Select)
	case sqlast.SetValuesExpr:
		return false
	case sqlast.SetTableExpr:
		return selectContainsJoin(expression.Select)
	case sqlast.SetBinaryExpr:
		return setContainsJoin(expression.Left) || setContainsJoin(expression.Right)
	case sqlast.SetGroupExpr:
		return setContainsJoin(expression.Child)
	default:
		return false
	}
}

func exprContainsJoin(e *sqlast.Expr) bool {
	if e == nil {
		return false
	}
	if e.Subquery != nil && selectContainsJoin(e.Subquery) {
		return true
	}
	for _, kid := range e.Kids {
		if exprContainsJoin(kid) {
			return true
		}
	}
	return false
}

// joinTableNames remains as a test/debug compatibility helper. Execution uses
// physicalDependency directly so positions are never discarded.
func joinTableNames(statement *sqlast.SelectStmt) []string {
	dependencies := selectPhysicalDependencies(statement)
	names := make([]string, len(dependencies))
	for i := range dependencies {
		names[i] = dependencies[i].name
	}
	return names
}
