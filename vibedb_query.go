package vibedb

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// ErrInvalidQuery reports a nil compiled query passed to Run.
var ErrInvalidQuery = errors.New("vibedb: invalid query")

// Run executes a compiled query against one immutable current generation.
// It is the one-off form: the returned Result owns the execution buffers that
// must outlive the call. Use [Collection.NewSession] when the same collection
// is queried repeatedly and retaining warmed bounded workspace matters.
//
// Run never creates a missing lazy collection. A query over one returns an
// ordinary empty result.
func (c *Collection) Run(compiled *query.Query) (query.Result, error) {
	if compiled == nil {
		return query.Result{}, ErrInvalidQuery
	}
	memory, disk, err := c.backend(false)
	if err != nil {
		return query.Result{}, err
	}
	if memory != nil {
		snapshot, err := memory.Snapshot()
		if err != nil {
			return query.Result{}, facadeError(err)
		}
		result, runErr := compiled.Run(query.FromSnapshot(snapshot))
		return result, facadeError(runErr)
	}
	if disk != nil {
		snapshot, err := disk.Snapshot()
		if err != nil {
			return query.Result{}, facadeError(err)
		}
		result, runErr := compiled.Run(query.FromFile(snapshot))
		return result, errors.Join(
			facadeError(runErr), facadeError(snapshot.Close()),
		)
	}
	result, runErr := compiled.Run(query.FromSnapshot(store.Snapshot{}))
	return result, facadeError(runErr)
}

// Session binds one Collection to reusable query execution storage. It is
// single-consumer: a compiled query may be shared, but concurrent callers need
// one Session each. A Session must not be copied after first use.
//
// Every Run takes a fresh immutable collection snapshot. Durable snapshots are
// closed before Run returns; their result cells are owned copies. Heap result
// cells and decoded workspace belong to the Session and remain valid only until
// its next Run or Release. Copy cell JSON/text first if it must outlive that
// boundary.
type Session struct {
	collection *Collection
	exec       query.Exec
	snapshot   durable.Snapshot
	released   bool
}

// NewSession returns a reusable query session bound to c. It performs no I/O
// and does not create a missing lazy collection.
func (c *Collection) NewSession() *Session {
	return &Session{collection: c}
}

// Run executes compiled against a fresh immutable current generation and
// returns the Session-owned Result. The pointer remains valid until the next
// Run or Release.
func (s *Session) Run(compiled *query.Query) (*query.Result, error) {
	if s == nil || s.released || s.collection == nil {
		return nil, ErrClosed
	}
	if compiled == nil {
		return nil, ErrInvalidQuery
	}
	memory, disk, err := s.collection.backend(false)
	if err != nil {
		return nil, err
	}
	if memory != nil {
		snapshot, err := memory.Snapshot()
		if err != nil {
			return nil, facadeError(err)
		}
		err = facadeError(compiled.RunInto(
			&s.exec, query.FromSnapshot(snapshot),
		))
	} else if disk != nil {
		if openErr := disk.SnapshotInto(&s.snapshot); openErr != nil {
			return nil, facadeError(openErr)
		}
		err = facadeError(compiled.RunInto(
			&s.exec, query.FromFile(&s.snapshot),
		))
		err = errors.Join(err, facadeError(s.snapshot.Close()))
	} else {
		err = facadeError(compiled.RunInto(
			&s.exec, query.FromSnapshot(store.Snapshot{}),
		))
	}
	if err != nil {
		return nil, fmt.Errorf("vibedb: query execution: %w", err)
	}
	return &s.exec.Result, nil
}

// Release drops every retained result and execution buffer. It is idempotent;
// a released Session cannot be reused.
func (s *Session) Release() {
	if s == nil || s.released {
		return
	}
	_ = s.snapshot.Close()
	s.snapshot = durable.Snapshot{}
	s.exec.Release()
	s.collection = nil
	s.released = true
}
