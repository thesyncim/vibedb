package driver

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// ReplicatedReadSession executes SQL against a borrowed, quorum-fenced cut.
// It never opens an ordinary SQL connector or takes independent snapshots.
// The owner must keep the cut and its serving-generation lease alive until
// Close returns. Only SELECT preparation is exposed; writes remain canonical
// replicated commands, not direct SQL mutations on a replica.
type ReplicatedReadSession struct {
	session Session
	conn    conn
}

func (a *ReplicatedApply) NewDataReadSession(
	ctx context.Context, cut *replicatedstate.DataReadCut, options query.ExecOptions,
) (*ReplicatedReadSession, error) {
	if a == nil || a.database == nil || ctx == nil {
		return nil, ErrReplicatedApplyClosed
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return nil, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return nil, err
	}
	base := a.database.catalog.ReplicatedShardStore
	if base == nil || !a.machine.OwnsDataReadCut(cut) ||
		cut.Fence().Binding.SchemaGeneration != base.Binding.Authority.SchemaGeneration {
		return nil, ErrReplicatedApplyMismatch
	}
	if options.Workers == 0 {
		options.Workers = driverQueryWorkers
	}
	reader := &ReplicatedReadSession{}
	reader.conn = conn{db: a.database, directWritesFenced: true, exec: query.Exec{Options: options}}
	transaction := &tx{
		conn: &reader.conn, readOnly: true, isolation: IsolationRepeatableRead,
		borrowedSnapshots: true, layoutEpoch: a.database.layoutEpoch,
		tables: make(map[string]*txTable, int(base.RelationCount)),
	}
	transaction.views = transaction.layoutEpoch.views
	for ordinal := uint16(0); ordinal < base.RelationCount; ordinal++ {
		id := replication.RelationID(ordinal + 1)
		snapshot, admitted := cut.Relation(id)
		if !admitted {
			continue
		}
		name := base.Relations[ordinal].Table
		layout, exists := transaction.layoutEpoch.tables[name]
		if !exists || snapshot == nil {
			return nil, ErrReplicatedApplyMismatch
		}
		state, err := newTransactionTableState(name, layout, false)
		if err != nil {
			return nil, err
		}
		state.snapshot, state.readCut, state.readRelation = snapshot, cut, id
		state.filterSource = query.NewFileFilterSource(state)
		transaction.tables[name] = state
	}
	reader.conn.tx = transaction
	reader.session = Session{conn: &reader.conn, state: SessionInTransaction}
	return reader, nil
}

func (reader *ReplicatedReadSession) Prepare(ctx context.Context, text string) (*Prepared, error) {
	return reader.prepare(ctx, text, false)
}

func (reader *ReplicatedReadSession) PreparePartialAggregate(ctx context.Context, text string) (*Prepared, error) {
	return reader.prepare(ctx, text, true)
}

func (reader *ReplicatedReadSession) prepare(ctx context.Context, text string, partial bool) (*Prepared, error) {
	if reader == nil {
		return nil, ErrSessionClosed
	}
	var prepared *Prepared
	var err error
	if partial {
		prepared, err = reader.session.PreparePartialAggregate(ctx, text)
	} else {
		prepared, err = reader.session.Prepare(ctx, text)
	}
	if err != nil {
		return nil, err
	}
	if prepared.Kind() != sqlast.KindSelect {
		return nil, errors.Join(ErrReadOnlyTransaction, prepared.Close())
	}
	return prepared, nil
}

func (reader *ReplicatedReadSession) Close() error {
	if reader == nil {
		return nil
	}
	return reader.session.Close()
}
