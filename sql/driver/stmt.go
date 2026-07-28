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
	conn     *conn
	tree     *sqlast.Statement
	query    *query.Statement
	mutation *query.DMLStatement
	adhoc    bool
	closed   bool
}

var (
	_ sqldriver.Stmt             = (*stmt)(nil)
	_ sqldriver.StmtQueryContext = (*stmt)(nil)
	_ sqldriver.StmtExecContext  = (*stmt)(nil)
)

func (s *stmt) NumInput() int { return s.tree.Params() }

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
	return nil
}

func (s *stmt) Query(values []sqldriver.Value) (sqldriver.Rows, error) {
	return s.QueryContext(context.Background(), named(values))
}

func (s *stmt) QueryContext(ctx context.Context, arguments []sqldriver.NamedValue) (sqldriver.Rows, error) {
	if s.closed {
		return nil, errors.New("vibedb: statement is closed")
	}
	if s.query == nil {
		return nil, fmt.Errorf("vibedb: %s returns no rows; use Exec", s.tree.Kind)
	}
	if err := s.conn.usable(ctx); err != nil {
		return nil, err
	}
	if s.conn.open {
		return nil, errors.New("vibedb: close the current rows before querying again on this connection")
	}
	args, err := s.conn.values(arguments)
	if err != nil {
		return nil, err
	}
	if s.conn.tx != nil {
		source, err := s.conn.tx.querySource(s.query.Collection())
		if err != nil {
			return nil, err
		}
		cursor, err := s.query.RunInto(&s.conn.exec, source, args)
		if err != nil {
			return nil, err
		}
		s.conn.open = true
		return &rows{stmt: s, cursor: cursor}, nil
	}
	s.conn.db.mu.RLock()
	t, ok := s.conn.db.tables[s.query.Collection()]
	if !ok {
		s.conn.db.mu.RUnlock()
		return nil, fmt.Errorf("vibedb: table %q does not exist", s.query.Collection())
	}
	var (
		source   query.Source
		snapshot *durable.Snapshot
	)
	if t.collection == nil {
		source = query.FromSnapshot(store.Snapshot{})
	} else {
		snapshot, err = t.collection.Snapshot()
		if err == nil {
			keys, point, keyErr := primaryPredicateKeys(s.tree.Select.Where, t.meta.PrimaryKey, args)
			if keyErr != nil {
				err = keyErr
			} else if point {
				var heap *store.Collection
				heap, err = store.New(store.Options{})
				var document []byte
				for _, key := range keys {
					if err != nil {
						break
					}
					var found bool
					document, found, err = snapshot.AppendRaw(document[:0], key)
					if err == nil && found {
						_, err = heap.Put(key, document)
					}
				}
				if err == nil {
					var view store.Snapshot
					view, err = heap.Snapshot()
					source = query.FromSnapshot(view)
				}
				_ = snapshot.Close()
				snapshot = nil
			} else {
				source = query.FromFile(snapshot)
			}
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
	s.conn.open = true
	return &rows{stmt: s, cursor: cursor, snapshot: snapshot}, nil
}

func (s *stmt) Exec(values []sqldriver.Value) (sqldriver.Result, error) {
	return s.ExecContext(context.Background(), named(values))
}

func (s *stmt) ExecContext(ctx context.Context, arguments []sqldriver.NamedValue) (sqldriver.Result, error) {
	if s.closed {
		return nil, errors.New("vibedb: statement is closed")
	}
	if s.mutation == nil {
		return nil, errors.New("vibedb: SELECT returns rows; use Query")
	}
	if err := s.conn.usable(ctx); err != nil {
		return nil, err
	}
	args, err := s.conn.values(arguments)
	if err != nil {
		return nil, err
	}
	if s.conn.tx != nil {
		return s.conn.tx.execMutation(s.mutation, args)
	}
	return s.conn.execMutation(s.mutation, args)
}

func named(values []sqldriver.Value) []sqldriver.NamedValue {
	out := make([]sqldriver.NamedValue, len(values))
	for i, value := range values {
		out[i] = sqldriver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return out
}
