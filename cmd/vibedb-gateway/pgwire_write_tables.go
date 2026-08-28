package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// Independent tables must not share an outcome-unknown outbox. Keep ordering
// within a table and retain the legacy journal under its original identity.
// The local endpoint has the same fixed 64-table bound as its data processes.
const maxPostgresTableWriters = 64

type postgresTableWriters struct {
	mu        sync.Mutex
	path      string
	authority serviceauthz.Authority
	service   postgresDurableService
	writers   map[string]*postgresDurableWriter
	ctx       context.Context
	cancel    context.CancelFunc
	workers   sync.WaitGroup
	closed    bool
}

func postgresWriteTable(q gateway.Query) (string, error) {
	s, err := sqlast.ParseStatement(q.SQL)
	if err != nil {
		return "", err
	}
	switch s.Kind {
	case sqlast.KindInsert:
		return s.Insert.Table, nil
	case sqlast.KindUpdate:
		return s.Update.Table, nil
	case sqlast.KindDelete:
		return s.Delete.Table, nil
	default:
		return "", errInvalidDurableRequestAdapter
	}
}

func postgresTableJournalName(table string) string {
	return fmt.Sprintf("%x.journal", sha256.Sum256([]byte(table)))
}

func openPostgresTableWriters(path string, authority serviceauthz.Authority, service postgresDurableService) (*postgresTableWriters, error) {
	legacy, err := openPostgresDurableWriter(path, authority, service)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &postgresTableWriters{path: path + ".tables", authority: authority, service: service,
		writers: make(map[string]*postgresDurableWriter), ctx: ctx, cancel: cancel}
	fail := func(err error) (*postgresTableWriters, error) { _ = w.Close(); return nil, err }
	// Before multi-table support the only writable table was documents. A
	// retained pending command is stronger evidence and must never be rerouted.
	table := legacy.record.Table
	if table == "" {
		table = "documents"
	}
	if legacy.record.Query != nil {
		pending, parseErr := postgresWriteTable(*legacy.record.Query)
		if parseErr != nil || legacy.record.Table != "" && pending != table {
			_ = legacy.Close()
			return fail(errors.Join(parseErr, errInvalidDurableRequestAdapter))
		}
		table = pending
	}
	w.writers[table] = legacy
	if legacy.record.Table == "" {
		legacy.record.Table = table
		if err = legacy.save(); err != nil {
			return fail(err)
		}
	}
	if err = os.MkdirAll(w.path, 0700); err != nil {
		return fail(err)
	}
	info, err := os.Lstat(w.path)
	if err != nil || !info.IsDir() {
		return fail(errors.Join(err, errInvalidDurableRequestAdapter))
	}
	parent, err := os.Open(filepath.Dir(w.path))
	if err != nil {
		return fail(err)
	}
	if err = errors.Join(parent.Sync(), parent.Close()); err != nil {
		return fail(err)
	}
	dir, err := os.Open(w.path)
	if err != nil {
		return fail(err)
	}
	entries, readErr := dir.ReadDir(3*maxPostgresTableWriters + 1)
	err = errors.Join(dir.Close(), readErr)
	if err != nil && !errors.Is(err, io.EOF) {
		return fail(err)
	}
	if len(entries) > 3*maxPostgresTableWriters {
		return fail(errInvalidDurableRequestAdapter)
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".lock") || strings.HasSuffix(name, ".pending") {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(name, ".journal") || len(w.writers) >= maxPostgresTableWriters {
			return fail(errInvalidDurableRequestAdapter)
		}
		lane, openErr := openPostgresDurableWriter(filepath.Join(w.path, name), authority, service)
		if openErr != nil {
			return fail(openErr)
		}
		table = lane.record.Table
		if table == "" || name != postgresTableJournalName(table) || w.writers[table] != nil {
			_ = lane.Close()
			return fail(errInvalidDurableRequestAdapter)
		}
		if lane.record.Query != nil {
			pending, parseErr := postgresWriteTable(*lane.record.Query)
			if parseErr != nil || pending != table {
				_ = lane.Close()
				return fail(errors.Join(parseErr, errInvalidDurableRequestAdapter))
			}
		}
		w.writers[table] = lane
	}
	for _, lane := range w.writers {
		w.start(lane)
	}
	return w, nil
}

func (w *postgresTableWriters) start(lane *postgresDurableWriter) {
	w.workers.Add(1)
	go func() { defer w.workers.Done(); lane.Run(w.ctx) }()
}

func (w *postgresTableWriters) Write(ctx context.Context, authority serviceauthz.Authority, q gateway.Query) (*gateway.Result, error) {
	if authority != w.authority {
		return nil, gateway.ErrReplicatedUnauthorized
	}
	table, err := postgresWriteTable(q)
	if err != nil || table == "" {
		return nil, errors.Join(err, errInvalidDurableRequestAdapter)
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, errInvalidDurableRequestAdapter
	}
	lane := w.writers[table]
	if lane == nil {
		if len(w.writers) >= maxPostgresTableWriters {
			w.mu.Unlock()
			return nil, gateway.ErrTransactionByteLimit
		}
		lane, err = openPostgresDurableWriter(filepath.Join(w.path, postgresTableJournalName(table)), authority, w.service, table)
		if err == nil {
			w.writers[table] = lane
			w.start(lane)
		}
	}
	w.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return lane.Write(ctx, authority, q)
}

func (w *postgresTableWriters) Run(ctx context.Context) {
	select {
	case <-ctx.Done():
		w.cancel()
	case <-w.ctx.Done():
	}
}

func (w *postgresTableWriters) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	w.cancel()
	w.workers.Wait()
	var err error
	for _, lane := range w.writers {
		err = errors.Join(err, lane.Close())
	}
	return err
}
