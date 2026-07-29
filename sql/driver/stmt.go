package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

type stmt struct {
	conn         *conn
	tree         *sqlast.Statement
	query        *query.Statement
	mutation     *query.DMLStatement
	primaryPoint bool
	params       int
	paramKinds   []ParamKind
	joinNames    []string
	closed       bool
}

var _ sqldriver.Stmt = (*stmt)(nil)

func (s *stmt) NumInput() int { return s.params }

func (s *stmt) checkArgumentCount(got int) error {
	want := s.NumInput()
	if got == want {
		return nil
	}
	return fmt.Errorf(
		"vibedb: statement expects %d arguments, got %d",
		want, got,
	)
}

func (s *stmt) preflightExec(got int) error {
	if s.closed {
		return errors.New("vibedb: statement is closed")
	}
	if s.mutation == nil {
		return errors.New("vibedb: SELECT returns rows; use Query")
	}
	return s.checkArgumentCount(got)
}

func (s *stmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.query != nil {
		s.query.Release()
	}
	if s.mutation != nil {
		s.mutation.Release()
	}
	// Both prepared forms borrow the parsed tree, whose arenas can be as large
	// as the bounded SQL input. Keep only the scalar parameter count needed by
	// NumInput after Close; a caller retaining a closed Stmt must not retain the
	// parser and compiler high-water storage with it.
	s.tree = nil
	s.query = nil
	s.mutation = nil
	s.primaryPoint = false
	s.paramKinds = nil
	s.joinNames = nil
	s.conn = nil
	return nil
}

func (s *stmt) Query(values []sqldriver.Value) (sqldriver.Rows, error) {
	if s.closed {
		return nil, errors.New("vibedb: statement is closed")
	}
	if s.query == nil {
		return nil, fmt.Errorf("vibedb: %s returns no rows; use Exec", s.tree.Kind)
	}
	if err := s.checkArgumentCount(len(values)); err != nil {
		return nil, err
	}
	if err := checkSQLValues(values); err != nil {
		return nil, err
	}
	args := s.conn.legacyValues(values)
	return s.queryRows(backgroundContext, args)
}

// queryRows is the typed query primitive shared by database/sql and the public
// runtime. args must be connection-owned storage: it is cleared on return so an
// idle session never pins caller values. Runtime callers copy interface headers
// into conn.args before arriving here.
func (s *stmt) queryRows(ctx context.Context, args []any) (*rows, error) {
	defer clear(args)
	if s.closed {
		return nil, errors.New("vibedb: statement is closed")
	}
	if s.query == nil {
		return nil, fmt.Errorf("vibedb: %s returns no rows; use Exec", s.tree.Kind)
	}
	if err := s.checkArgumentCount(len(args)); err != nil {
		return nil, err
	}
	if err := s.conn.usable(ctx); err != nil {
		return nil, err
	}
	if s.conn.open {
		return nil, errors.New("vibedb: close the current rows before querying again on this connection")
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	var err error
	if s.conn.tx != nil {
		if s.query.NumJoins() != 0 {
			source, err := s.conn.materializeTransactionJoinSource(
				ctx, s.conn.tx, s.tree.Select, s.joinNames)
			if err != nil {
				return nil, err
			}
			cursor, err := s.query.RunInto(&s.conn.exec, source, args)
			if err != nil {
				return nil, err
			}
			if err := contextCheckpoint(ctx); err != nil {
				return nil, err
			}
			s.conn.open = true
			return s.conn.resetRows(s, cursor, nil), nil
		}
		state, ok := s.conn.tx.tables[s.query.Collection()]
		if !ok {
			return nil, fmt.Errorf(
				"%w: %q was not present when the transaction began",
				ErrTableNotFound, s.query.Collection())
		}
		var source query.Source
		if s.primaryPoint {
			keys, keyErr := s.conn.bindPointPredicateKeys(
				s.tree.Select.Where, args, state.limits.MaxKeyBytes)
			if keyErr != nil {
				return nil, keyErr
			}
			source, err = s.conn.pointTransactionSource(ctx, state, keys)
			if errors.Is(err, errPointMaterializationTooLarge) {
				source, err = s.conn.tx.querySource(s.query.Collection())
			}
		} else {
			source, err = s.conn.tx.querySource(s.query.Collection())
		}
		if err != nil {
			return nil, err
		}
		cursor, err := s.query.RunInto(&s.conn.exec, source, args)
		if err != nil {
			return nil, err
		}
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		s.conn.open = true
		return s.conn.resetRows(s, cursor, nil), nil
	}
	if err := rlockContext(ctx, &s.conn.db.mu); err != nil {
		return nil, err
	}
	if s.query.NumJoins() != 0 {
		clear(s.conn.joinCatalog)
		collections := s.conn.joinCatalog[:0]
		for _, name := range s.joinNames {
			t, ok := s.conn.db.tables[name]
			if !ok {
				s.conn.releaseJoinCatalog(collections)
				s.conn.db.mu.RUnlock()
				return nil, fmt.Errorf("%w: %q", ErrTableNotFound, name)
			}
			collections = append(collections, durable.NamedCollection{
				Name: name, Collection: t.collection,
			})
		}
		if err := contextCheckpoint(ctx); err != nil {
			s.conn.releaseJoinCatalog(collections)
			s.conn.db.mu.RUnlock()
			return nil, err
		}
		catalog, snapshotErr := durable.SnapshotCollections(collections)
		s.conn.releaseJoinCatalog(collections)
		s.conn.db.mu.RUnlock()
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		source, materializeErr := s.conn.materializeDurableJoinSource(
			ctx, catalog, s.tree.Select, s.joinNames)
		closeErr := catalog.Close()
		if materializeErr != nil {
			return nil, materializeErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		cursor, runErr := s.query.RunInto(&s.conn.exec, source, args)
		if runErr != nil {
			return nil, runErr
		}
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		s.conn.open = true
		return s.conn.resetRows(s, cursor, nil), nil
	}
	t, ok := s.conn.db.tables[s.query.Collection()]
	if !ok {
		s.conn.db.mu.RUnlock()
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, s.query.Collection())
	}
	var (
		source   query.Source
		snapshot *durable.Snapshot
	)
	if s.primaryPoint {
		limits, limitErr := tableMutationLimits(t)
		if limitErr != nil {
			err = limitErr
		} else {
			keys, keyErr := s.conn.bindPointPredicateKeys(
				s.tree.Select.Where, args, limits.MaxKeyBytes)
			if keyErr != nil {
				err = keyErr
			} else if t.collection == nil {
				source = query.FromSnapshot(store.Snapshot{})
			} else {
				source, err = s.conn.pointCollectionSource(ctx, t.collection, keys)
				if errors.Is(err, errPointMaterializationTooLarge) {
					snapshot, err = t.collection.Snapshot()
					if err == nil {
						source = query.FromFile(snapshot)
					}
				}
			}
		}
	} else if t.collection == nil {
		source = query.FromSnapshot(store.Snapshot{})
	} else {
		snapshot, err = t.collection.Snapshot()
		if err == nil {
			source = query.FromFile(snapshot)
		}
	}
	s.conn.db.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	cursor, err := s.query.RunInto(&s.conn.exec, source, args)
	if err != nil {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		return nil, err
	}
	s.conn.open = true
	return s.conn.resetRows(s, cursor, snapshot), nil
}

func (c *conn) releaseJoinCatalog(collections []durable.NamedCollection) {
	c.joinCatalog = collections
	clear(c.joinCatalog)
	c.joinCatalog = c.joinCatalog[:0]
}

// pointCollectionSource copies only the primary-key candidates into a
// connection-owned scratch Segment. Segment.Reset retains its arenas, so after
// one warm execution this preserves the O(candidate keys) point-read path
// without constructing a heap Collection for every query.
//
// Result cells from a Segment borrow it. The connection's one-open-rows rule
// therefore is also the lifetime fence: another query cannot Reset pointDocs
// until the current rows have closed.
func (c *conn) pointCollectionSource(
	ctx context.Context,
	collection *durable.Collection,
	keys []string,
) (query.Source, error) {
	c.pointDocs.Reset()
	limit, err := driverQueryMemory(c.exec.Options)
	if err != nil {
		return query.Source{}, err
	}
	budget := pointMaterializationBudget{limit: limit}
	document := c.pointRaw[:0]
	for _, key := range keys {
		if err := contextCheckpoint(ctx); err != nil {
			c.pointRaw = document
			c.pointDocs.Reset()
			return query.Source{}, err
		}
		var (
			found bool
			err   error
		)
		document, found, err = collection.AppendRaw(document[:0], key)
		if err != nil {
			c.pointRaw = document
			return query.Source{}, err
		}
		if found {
			if err := budget.add(key, document); err != nil {
				c.pointRaw = document
				c.pointDocs.Reset()
				return query.Source{}, err
			}
			if _, err := c.pointDocs.Append(document); err != nil {
				c.pointRaw = document
				return query.Source{}, err
			}
		}
	}
	c.pointRaw = document
	return query.FromSegment(&c.pointDocs), nil
}

func (c *conn) pointTransactionSource(
	ctx context.Context,
	state *txTable,
	keys []string,
) (query.Source, error) {
	c.pointDocs.Reset()
	limit, err := driverQueryMemory(c.exec.Options)
	if err != nil {
		return query.Source{}, err
	}
	budget := pointMaterializationBudget{limit: limit}
	document := c.pointRaw[:0]
	for _, key := range keys {
		if err := contextCheckpoint(ctx); err != nil {
			c.pointRaw = document
			c.pointDocs.Reset()
			return query.Source{}, err
		}
		var (
			found bool
			err   error
		)
		document, found, err = state.appendRaw(document[:0], key)
		if err != nil {
			c.pointRaw = document
			return query.Source{}, err
		}
		if found {
			if err := budget.add(key, document); err != nil {
				c.pointRaw = document
				c.pointDocs.Reset()
				return query.Source{}, err
			}
			if _, err := c.pointDocs.Append(document); err != nil {
				c.pointRaw = document
				return query.Source{}, err
			}
		}
	}
	c.pointRaw = document
	return query.FromSegment(&c.pointDocs), nil
}

var errPointMaterializationTooLarge = errors.New(
	"vibedb: primary point-source materialization exceeds the query memory bound")

type pointMaterializationBudget struct {
	limit int64
	used  int64
}

func (b *pointMaterializationBudget) add(key string, document []byte) error {
	raw := int64(len(key)) + int64(len(document))
	remaining := b.limit - b.used
	if remaining < joinMaterializationRowBytes ||
		raw > (remaining-joinMaterializationRowBytes)/
			joinMaterializationExpansion {
		return errPointMaterializationTooLarge
	}
	b.used += raw*joinMaterializationExpansion + joinMaterializationRowBytes
	return nil
}

func (s *stmt) Exec(values []sqldriver.Value) (sqldriver.Result, error) {
	if err := s.preflightExec(len(values)); err != nil {
		return nil, err
	}
	if err := checkSQLValues(values); err != nil {
		return nil, err
	}
	return s.exec(backgroundContext, s.conn.legacyValues(values))
}

func (s *stmt) execContext(ctx context.Context, arguments []sqldriver.NamedValue) (sqldriver.Result, error) {
	if err := s.preflightExec(len(arguments)); err != nil {
		return nil, err
	}
	if err := checkSQLNamedValues(arguments); err != nil {
		return nil, err
	}
	args, err := s.conn.values(arguments)
	if err != nil {
		return nil, err
	}
	return s.exec(ctx, args)
}

func (s *stmt) exec(ctx context.Context, args []any) (sqldriver.Result, error) {
	defer clear(args)
	if s.closed {
		return nil, errors.New("vibedb: statement is closed")
	}
	if s.mutation == nil {
		return nil, errors.New("vibedb: SELECT returns rows; use Query")
	}
	if err := s.conn.usable(ctx); err != nil {
		return nil, err
	}
	if s.conn.open {
		return nil, errors.New("vibedb: close the current rows before executing another statement on this connection")
	}
	if s.conn.tx != nil {
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		return s.conn.tx.execMutation(s.mutation, args)
	}
	return s.conn.execMutationContext(ctx, s.mutation, args)
}
