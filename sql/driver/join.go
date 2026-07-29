package driver

import (
	"context"
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

// materializeDurableJoinSource copies one already-consistent durable catalog
// cut into the heap executor that supports fan-out. It measures the complete
// input first, so an oversized join fails before a Query starts executing and
// before any heap collection is built.
func (c *conn) materializeDurableJoinSource(
	ctx context.Context,
	catalog durable.DatabaseSnapshot,
	statement *sqlast.SelectStmt,
	names []string,
) (query.Source, error) {
	rows := func(visit joinRowVisitor) error {
		for _, name := range names {
			snapshot, ok := catalog.Collection(name)
			if !ok {
				return fmt.Errorf(
					"%w: %q is absent from the captured join snapshot",
					ErrTableNotFound, name)
			}
			if snapshot == nil {
				continue
			}
			if err := snapshot.RangeRaw(func(key, document []byte) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return visit(name, key, document)
			}); err != nil {
				return err
			}
		}
		return nil
	}
	return c.materializeJoinRows(names, statement.From[0].Name, rows)
}

// materializeTransactionJoinSource is the transactional twin. Every base
// snapshot came from BEGIN while the driver catalog held all writers out, and
// pending rows are overlaid exactly as tx.view does. No fresh durable snapshot
// is taken here, so repeatable reads and read-your-writes both survive a join.
func (c *conn) materializeTransactionJoinSource(
	ctx context.Context,
	transaction *tx,
	statement *sqlast.SelectStmt,
	names []string,
) (query.Source, error) {
	rows := func(visit joinRowVisitor) error {
		if transaction.done {
			return fmt.Errorf("vibedb: transaction is finished")
		}
		for _, name := range names {
			state, ok := transaction.tables[name]
			if !ok {
				return fmt.Errorf(
					"%w: %q was not present when the transaction began",
					ErrTableNotFound, name)
			}
			if state.snapshot != nil {
				if err := state.snapshot.RangeRaw(func(key, document []byte) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					if _, shadowed := state.pending[string(key)]; shadowed {
						return nil
					}
					return visit(name, key, document)
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
				if err := visit(name, []byte(key), mutation.document); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return c.materializeJoinRows(names, statement.From[0].Name, rows)
}

func (c *conn) materializeJoinRows(
	names []string,
	driving string,
	rows joinRowSource,
) (query.Source, error) {
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
	collections := make(map[string]*store.Collection, len(names))
	for _, name := range names {
		collection, err := database.CreateCollection(name, store.Options{})
		if err != nil {
			return query.Source{}, err
		}
		collections[name] = collection
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

func joinTableNames(statement *sqlast.SelectStmt) []string {
	names := make([]string, 0, len(statement.From))
	return appendSelectTableNames(names, statement)
}

func appendSelectTableNames(names []string, statement *sqlast.SelectStmt) []string {
	for i := range statement.From {
		name := statement.From[i].Name
		duplicate := false
		for _, prior := range names {
			if prior == name {
				duplicate = true
				break
			}
		}
		if !duplicate {
			names = append(names, name)
		}
	}
	names = appendExprTableNames(names, statement.Where)
	return names
}

func appendExprTableNames(names []string, e *sqlast.Expr) []string {
	if e == nil {
		return names
	}
	if e.Subquery != nil {
		return appendSelectTableNames(names, e.Subquery)
	}
	for _, kid := range e.Kids {
		names = appendExprTableNames(names, kid)
	}
	return names
}
