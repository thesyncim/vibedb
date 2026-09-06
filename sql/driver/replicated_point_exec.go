package driver

import (
	"bytes"
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

// A warm point execution consumes the detached result directly. It has no
// transaction, table map, snapshot, or second storage-key lookup.
type replicatedPointExecution struct {
	key, raw    []byte
	found       bool
	table, path string
	maxKeyBytes int
	layout      *catalogLayoutIdentity
}

func (p *replicatedPointExecution) reset() {
	key, raw := p.key, p.raw
	clear(key[:cap(key)])
	clear(raw[:cap(raw)])
	if cap(key)+cap(raw) > replicatedReadReuseResultBytes {
		key, raw = nil, nil
	}
	*p = replicatedPointExecution{key: key[:0], raw: raw[:0]}
}

func (a *ReplicatedApply) bindPointExecution(ctx context.Context, relation replication.RelationID,
	key []byte, found bool, raw, path []byte, options query.ExecOptions, reader *ReplicatedReadSession,
) error {
	if a == nil || a.database == nil || ctx == nil || reader == nil {
		return ErrReplicatedApplyClosed
	}
	if err := contextCheckpoint(ctx); err != nil {
		return err
	}
	if reader.session.current != nil || reader.conn.open || reader.conn.tx != nil {
		return ErrCursorOpen
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return err
	}
	capacity, err := a.machine.SessionCapacityState()
	if err != nil {
		return err
	}
	if !capacity.Initialized {
		return ErrReplicatedApplyMismatch
	}
	layout, _, _, err := a.pointReadSessionLayoutLocked(relation, path, key, found, raw, &reader.pointProof)
	if err != nil {
		return err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return err
	}
	if options.Workers == 0 {
		options.Workers = driverQueryWorkers
	}
	p := &reader.pointExecution
	p.key = append(p.key[:0], key...)
	p.raw = append(p.raw[:0], raw...)
	p.found, p.table, p.path = found, reader.pointProof.base.UserTable, reader.pointProof.base.UserPrimaryKey
	p.maxKeyBytes, p.layout = layout.limits.MaxKeyBytes, layoutIdentityToken(a.database.layoutEpoch)
	reader.conn.pointRead = p
	reader.conn.exec.Options = options
	reader.session.state = SessionIdle
	return nil
}

func (s *stmt) queryPointExecution(ctx context.Context, args []any, path []byte, keys [][]byte) (*rows, error) {
	p := s.conn.pointRead
	if !s.replicatedReadReuseEligible() || s.query.Collection() != p.table {
		return nil, ErrReplicatedReadReuseUnsupported
	}
	found := p.found
	if keys != nil {
		if string(path) != p.path {
			return nil, errors.New("vibedb: candidate primary path does not match the live table")
		}
		for _, key := range keys {
			if len(key) == 0 || len(key) > p.maxKeyBytes {
				return nil, errors.New("vibedb: candidate primary key exceeds the live table bound")
			}
		}
		if len(keys) == 1 {
			found = found && bytes.Equal(keys[0], p.key)
		}
	}
	source := query.FromSnapshot(store.Snapshot{})
	if found {
		if len(keys) <= 1 {
			if keys != nil {
				limit, err := driverQueryMemory(s.conn.exec.Options)
				if err != nil {
					return nil, err
				}
				budget := pointMaterializationBudget{limit: limit}
				if err := budget.add(string(p.key), p.raw); err != nil {
					return nil, err
				}
			}
			s.conn.pointSource.Bind(p.raw)
			defer s.conn.pointSource.Bind(nil)
			source = query.FromValidatedRaw(&s.conn.pointSource)
		} else {
			// The RF3 point request has one key. Preserve the public lease's
			// multi-candidate semantics through the ordinary bounded materializer.
			s.conn.pointDocs.Reset()
			limit, err := driverQueryMemory(s.conn.exec.Options)
			if err != nil {
				return nil, err
			}
			budget := pointMaterializationBudget{limit: limit}
			for _, key := range keys {
				if err := contextCheckpoint(ctx); err != nil {
					return nil, err
				}
				if !bytes.Equal(key, p.key) {
					continue
				}
				if err := budget.add(string(key), p.raw); err != nil {
					return nil, err
				}
				if _, err := s.conn.pointDocs.Append(p.raw); err != nil {
					return nil, err
				}
			}
			source = query.FromSegment(&s.conn.pointDocs)
		}
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
