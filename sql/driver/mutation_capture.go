package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	// ErrDocumentScanPageTooSmall reports a positive page budget that cannot
	// hold even one key/document pair. Retrying with the returned cursor would
	// make no progress, so the scan fails instead of looping.
	ErrDocumentScanPageTooSmall = errors.New("vibedb: document scan page cannot hold its next row")
	errDocumentScanPageFull     = errors.New("vibedb: document scan page is full")
)

// ScanDocumentsAfter visits a bounded page of canonical documents in native
// primary-key order, starting strictly after the supplied cursor. It seeks the
// durable primary graph directly and returns an owned resume key. Complete is
// true only when the captured snapshot has no later row.
func (s *Session) ScanDocumentsAfter(
	ctx context.Context,
	relation string,
	after []byte,
	maxRows int,
	maxBytes int64,
	visit func(key, document []byte) error,
) (next []byte, complete bool, err error) {
	if err := s.ready(ctx); err != nil {
		return nil, false, err
	}
	if s.state != SessionIdle || s.conn.tx != nil {
		return nil, false, s.fail(ErrTransactionActive)
	}
	if relation == "" || maxRows <= 0 || maxBytes <= 0 || visit == nil {
		return nil, false, s.fail(errors.New("vibedb: invalid bounded document scan"))
	}
	d := s.conn.db
	if err := lockContext(ctx, &d.mu); err != nil {
		return nil, false, s.fail(err)
	}
	if d.closed {
		d.mu.Unlock()
		return nil, false, s.fail(sqldriver.ErrBadConn)
	}
	if err := d.settleCatalogLocked(); err != nil {
		d.mu.Unlock()
		return nil, false, s.fail(err)
	}
	t, ok := d.tables[relation]
	if !ok {
		d.mu.Unlock()
		return nil, false, s.fail(fmt.Errorf("%w: %q", ErrTableNotFound, relation))
	}
	if t.collection == nil {
		d.mu.Unlock()
		return nil, true, nil
	}
	snapshot, err := t.collection.Snapshot()
	d.mu.Unlock()
	if err != nil {
		return nil, false, s.fail(err)
	}
	defer func() {
		err = errors.Join(err, snapshot.Close())
		if err != nil {
			err = s.fail(err)
		}
	}()
	ctx = withCooperativeCancellation(ctx, s.conn.exec.Options.Cancel)
	rows := 0
	var retained int64
	err = snapshot.RangeAfterRaw(after, func(key, document []byte) error {
		if err := contextCheckpoint(ctx); err != nil {
			return err
		}
		rowBytes := int64(len(key) + len(document))
		if rows >= maxRows || rowBytes > maxBytes-retained {
			if rows == 0 {
				return ErrDocumentScanPageTooSmall
			}
			return errDocumentScanPageFull
		}
		if err := visit(key, document); err != nil {
			return err
		}
		next = append(next[:0], key...)
		rows++
		retained += rowBytes
		return nil
	})
	if errors.Is(err, errDocumentScanPageFull) {
		return next, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return next, true, nil
}

// CaptureMutationInto visits the exact primary keys and current documents an
// UPDATE or DELETE would target without publishing the mutation. Selection is
// performed by the same lowered DML statement as Exec, including its WHERE,
// ORDER BY, and LIMIT. Key and document bytes borrow session scratch and are
// valid only until visit returns.
func (p *Prepared) CaptureMutationInto(
	ctx context.Context,
	values []any,
	visit func(key, document []byte) error,
) error {
	if err := p.usable(); err != nil {
		return p.fail(err)
	}
	if visit == nil {
		return p.fail(errors.New("vibedb: mutation capture requires a visitor"))
	}
	if p.statement.mutation == nil ||
		(p.Kind() != sqlast.KindUpdate && p.Kind() != sqlast.KindDelete) {
		return p.fail(errors.New("vibedb: mutation capture requires UPDATE or DELETE"))
	}
	if err := p.session.ready(ctx); err != nil {
		return p.fail(err)
	}
	if p.session.state != SessionIdle || p.session.conn.tx != nil {
		return p.fail(ErrTransactionActive)
	}
	if err := p.statement.checkArgumentCount(len(values)); err != nil {
		return p.fail(err)
	}
	args, err := p.session.conn.runtimeValues(p.statement.paramKinds, values)
	if err != nil {
		return p.fail(err)
	}
	scope, err := p.session.conn.beginContextCancellation(ctx)
	if err != nil {
		clear(args)
		return p.fail(err)
	}
	err = p.statement.captureMutationInto(ctx, args, visit)
	err = scope.finish(err)
	if err != nil {
		return p.fail(err)
	}
	return nil
}

func (s *stmt) captureMutationInto(
	ctx context.Context,
	args []any,
	visit func(key, document []byte) error,
) error {
	defer clear(args)
	ctx = withCooperativeCancellation(ctx, s.conn.exec.Options.Cancel)
	d := s.conn.db
	if err := lockContext(ctx, &d.mu); err != nil {
		return err
	}
	defer d.mu.Unlock()
	if d.closed {
		return sqldriver.ErrBadConn
	}
	if err := contextCheckpoint(ctx); err != nil {
		return err
	}
	if err := d.settleCatalogLocked(); err != nil {
		return err
	}
	if err := s.validateViewDependenciesLocked(); err != nil {
		return err
	}
	if err := d.validateViewTableTargetLocked(s.mutation.Tree()); err != nil {
		return err
	}
	t, ok := d.tables[s.mutation.Collection()]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTableNotFound, s.mutation.Collection())
	}
	limits, err := tableMutationLimits(t)
	if err != nil {
		return err
	}
	var assignments []sqlast.UpdateAssignment
	valueBytes := 0
	if s.mutation.Kind() == query.DMLUpdate {
		assignments = s.mutation.Tree().Update.Assignments
		if len(assignments) != 0 {
			if err := validateDeclaredColumnAssignments(
				s.mutation.Collection(), t.meta, assignments,
			); err != nil {
				return err
			}
			if err := validateColumnAssignmentBindings(assignments, args); err != nil {
				return err
			}
			if err := s.conn.routeDelete(s.mutation, args); err != nil {
				return err
			}
		} else {
			document, documentErr := operandDocument(
				s.mutation, s.mutation.Tree().Update.Doc, args,
			)
			if documentErr != nil {
				return documentErr
			}
			if len(document) > limits.MaxDocumentBytes {
				return durable.ErrDocumentTooLarge
			}
			if err := s.conn.routeUpdate(s.mutation, args, document); err != nil {
				return err
			}
			valueBytes = len(document)
		}
	} else if err := s.conn.routeDelete(s.mutation, args); err != nil {
		return err
	}
	keys, err := s.conn.matchingKeysLocked(
		ctx, s.mutation, args, t, limits, valueBytes,
	)
	if err != nil {
		return err
	}
	if s.mutation.Kind() == query.DMLUpdate && len(assignments) == 0 {
		document, documentErr := operandDocument(
			s.mutation, s.mutation.Tree().Update.Doc, args,
		)
		if documentErr != nil {
			return documentErr
		}
		newKey, keyErr := documentKey(
			document, t.meta.PrimaryKey, t.primary, limits.MaxKeyBytes,
		)
		if keyErr != nil {
			return keyErr
		}
		for _, key := range keys {
			if key != newKey {
				return fmt.Errorf(
					"%w: replacement key %q does not match selected key %q",
					ErrUpdatePrimaryKey, newKey, key,
				)
			}
		}
	}
	if t.collection == nil || len(keys) == 0 {
		return nil
	}
	snapshot, err := t.collection.Snapshot()
	if err != nil {
		return err
	}
	defer snapshot.Close()
	scratch := s.conn.pointRaw[:0]
	defer func() { s.conn.pointRaw = scratch[:0] }()
	if len(assignments) != 0 {
		// Preflight every post-image before exposing any old row to the visitor.
		// A later invalid replacement must not leave the caller with a prefix of
		// a capture that cannot describe one atomic mutation.
		stagedBytes := 0
		var routeScratch []byte
		for _, key := range keys {
			if err := contextCheckpoint(ctx); err != nil {
				return err
			}
			var found bool
			scratch, found, err = snapshot.AppendRaw(
				scratch[:0], byteview.Bytes(key),
			)
			if err != nil {
				return err
			}
			if !found {
				return ErrTransactionConflict
			}
			replacement, err := ApplyColumnAssignments(
				scratch, assignments, args, limits.MaxDocumentBytes,
			)
			if err != nil {
				return err
			}
			if err := validateDocument(
				t.schema, replacement, limits.MaxDocumentBytes,
				&s.conn.insertTape,
			); err != nil {
				return err
			}
			routeScratch, err = s.conn.routeUpdateInto(
				s.mutation, args, replacement, routeScratch[:0],
			)
			if err != nil {
				return err
			}
			newKey, err := documentKey(
				replacement, t.meta.PrimaryKey, t.primary,
				limits.MaxKeyBytes,
			)
			if err != nil {
				return err
			}
			if key != newKey {
				return fmt.Errorf(
					"%w: replacement key %q does not match selected key %q",
					ErrUpdatePrimaryKey, newKey, key,
				)
			}
			if len(key) > limits.MaxBatchBytes-stagedBytes ||
				len(replacement) > limits.MaxBatchBytes-stagedBytes-len(key) {
				return fmt.Errorf(
					"vibedb: UPDATE exceeds the %d-byte mutation batch limit for table %q: %w",
					limits.MaxBatchBytes, s.mutation.Collection(), durable.ErrBatchTooLarge,
				)
			}
			stagedBytes += len(key) + len(replacement)
		}
	}
	for _, key := range keys {
		if err := contextCheckpoint(ctx); err != nil {
			return err
		}
		var found bool
		scratch, found, err = snapshot.AppendRaw(scratch[:0], byteview.Bytes(key))
		if err != nil {
			return err
		}
		if !found {
			// The catalog mutex excludes every driver-mediated publisher. Treat a
			// disappearing selected key as a conflict instead of returning a
			// capture that could not describe the eventual mutation.
			return ErrTransactionConflict
		}
		if err := visit(byteview.Bytes(key), scratch); err != nil {
			return err
		}
	}
	return nil
}

// ValidatePrimaryDocumentDigests compares a bounded, sorted primary-key set
// with the documents visible in the active serializable transaction. Exact
// point dependencies are retained through commit, closing the capture-to-apply
// race without copying full documents into the distributed transaction log.
func (s *Session) ValidatePrimaryDocumentDigests(
	ctx context.Context,
	relation string,
	primaryPath []byte,
	keys [][]byte,
	digests [][sha256.Size]byte,
) error {
	return s.validatePrimaryDocumentDigests(
		ctx, relation, primaryPath, keys, digests, true,
	)
}

// CheckPrimaryDocumentDigests retains the same serializable point
// dependencies as ValidatePrimaryDocumentDigests without arming a following
// SQL mutation guard. Online backfill uses it as the base participant of a
// compare-and-put transaction.
func (s *Session) CheckPrimaryDocumentDigests(
	ctx context.Context,
	relation string,
	primaryPath []byte,
	keys [][]byte,
	digests [][sha256.Size]byte,
) error {
	return s.validatePrimaryDocumentDigests(
		ctx, relation, primaryPath, keys, digests, false,
	)
}

func (s *Session) validatePrimaryDocumentDigests(
	ctx context.Context,
	relation string,
	primaryPath []byte,
	keys [][]byte,
	digests [][sha256.Size]byte,
	installGuard bool,
) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	if s.state != SessionInTransaction || s.conn.tx == nil {
		return s.fail(ErrNoTransaction)
	}
	tx := s.conn.tx
	if tx.readOnly {
		return s.fail(ErrReadOnlyTransaction)
	}
	state := tx.tables[relation]
	if state == nil {
		return s.fail(fmt.Errorf("%w: %q", ErrTableNotFound, relation))
	}
	if relation == "" || len(primaryPath) == 0 ||
		!bytes.Equal(primaryPath, byteview.Bytes(state.primaryKey)) ||
		len(keys) != len(digests) || tx.primaryMutationGuard != nil {
		return s.fail(ErrDistributedTransactionConflict)
	}
	var guard *primaryMutationGuard
	if installGuard {
		guard = &primaryMutationGuard{relation: relation, keys: make([]string, len(keys))}
	}
	scratch := s.conn.pointRaw[:0]
	defer func() { s.conn.pointRaw = scratch[:0] }()
	previous := ""
	for i := range keys {
		if err := contextCheckpoint(ctx); err != nil {
			return s.fail(err)
		}
		if len(keys[i]) == 0 || len(keys[i]) > state.limits.MaxKeyBytes {
			return s.fail(ErrDistributedTransactionConflict)
		}
		key := byteview.String(keys[i])
		if i != 0 && previous >= key {
			return s.fail(ErrDistributedTransactionConflict)
		}
		previous = key
		if installGuard {
			guard.keys[i] = key
		}
		tx.trackSerializablePointRead(state, key)
		var found bool
		var err error
		scratch, found, err = state.appendRaw(scratch[:0], key)
		if err != nil {
			return s.fail(err)
		}
		if !found || sha256.Sum256(scratch) != digests[i] {
			return s.fail(ErrDistributedTransactionConflict)
		}
	}
	if installGuard {
		tx.primaryMutationGuard = guard
	}
	return nil
}
