package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"fmt"
	"strconv"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
)

type preparedSequenceDDL struct {
	source string
	create *sqlast.CreateSequenceStmt
	alter  *sqlast.AlterSequenceStmt
	drop   *sqlast.DropSequenceStmt
}

func (c *conn) prepareSequenceDDL(
	source string,
	tree *sqlast.Statement,
) (*preparedSequenceDDL, error) {
	prepared := &preparedSequenceDDL{source: source}
	switch tree.Kind {
	case sqlast.KindCreateSequence:
		if tree.CreateSequence == nil {
			return nil, fmt.Errorf("vibedb: invalid CREATE SEQUENCE descriptor")
		}
		prepared.create = tree.CreateSequence
	case sqlast.KindAlterSequence:
		if tree.AlterSequence == nil {
			return nil, fmt.Errorf("vibedb: invalid ALTER SEQUENCE descriptor")
		}
		prepared.alter = tree.AlterSequence
	case sqlast.KindDropSequence:
		if tree.DropSequence == nil {
			return nil, fmt.Errorf("vibedb: invalid DROP SEQUENCE descriptor")
		}
		prepared.drop = tree.DropSequence
	default:
		return nil, fmt.Errorf("vibedb: statement is not sequence DDL")
	}
	return prepared, nil
}

func (c *conn) execSequenceDDL(
	ctx context.Context,
	prepared *preparedSequenceDDL,
) (sqldriver.Result, error) {
	if err := c.requireDirectWriteAllowed(); err != nil {
		return nil, err
	}
	ctx = withCooperativeCancellation(ctx, c.exec.Options.Cancel)
	if prepared == nil {
		return nil, fmt.Errorf("vibedb: missing prepared sequence DDL")
	}
	if c.tx != nil {
		return nil, ErrDDLInTransaction
	}
	if err := lockContext(ctx, &c.db.mu); err != nil {
		return nil, err
	}
	defer c.db.mu.Unlock()
	if c.db.closed {
		return nil, sqldriver.ErrBadConn
	}
	if err := c.db.settleCatalogLocked(); err != nil {
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	switch {
	case prepared.create != nil:
		return c.db.createSequenceLocked(ctx, prepared.create)
	case prepared.alter != nil:
		return c.db.alterSequenceLocked(ctx, prepared.alter)
	case prepared.drop != nil:
		return c.db.dropSequenceLocked(ctx, prepared.drop)
	default:
		return nil, fmt.Errorf("vibedb: invalid sequence DDL")
	}
}

func buildSequenceMetaFromOptions(
	name string,
	opts sqlast.SequenceOptions,
	defaultDataType string,
	ownerTable string,
	ownerColumn string,
	gen uint64,
) (*sequenceMeta, error) {
	dt := defaultDataType
	if opts.HasDataType {
		dt = opts.DataType
	}
	typeMin, typeMax := sequenceTypeBounds(dt)

	inc := int64(1)
	if opts.HasIncrement {
		inc = opts.Increment
	}
	if inc == 0 {
		return nil, fmt.Errorf("%w: INCREMENT must not be zero", ErrInvalidSequenceDefinition)
	}

	var minVal, maxVal int64
	if inc > 0 {
		minVal = 1
		maxVal = typeMax
	} else {
		minVal = typeMin
		maxVal = -1
	}
	if opts.HasMinValue {
		minVal = opts.MinValue
	}
	if opts.HasMaxValue {
		maxVal = opts.MaxValue
	}
	if minVal < typeMin {
		return nil, fmt.Errorf("%w: MINVALUE (%d) is out of range for sequence data type %s", ErrInvalidSequenceDefinition, minVal, dt)
	}
	if maxVal > typeMax {
		return nil, fmt.Errorf("%w: MAXVALUE (%d) is out of range for sequence data type %s", ErrInvalidSequenceDefinition, maxVal, dt)
	}
	if minVal >= maxVal {
		return nil, fmt.Errorf("%w: MINVALUE (%d) must be less than MAXVALUE (%d)", ErrInvalidSequenceDefinition, minVal, maxVal)
	}

	var start int64
	if inc > 0 {
		start = minVal
	} else {
		start = maxVal
	}
	if opts.HasStart {
		start = opts.Start
	}
	if start < minVal {
		return nil, fmt.Errorf("%w: START value (%d) cannot be less than MINVALUE (%d)", ErrInvalidSequenceDefinition, start, minVal)
	}
	if start > maxVal {
		return nil, fmt.Errorf("%w: START value (%d) cannot be greater than MAXVALUE (%d)", ErrInvalidSequenceDefinition, start, maxVal)
	}

	cache := int64(1)
	if opts.HasCache {
		cache = opts.Cache
	}
	if cache < 1 {
		return nil, fmt.Errorf("%w: CACHE (%d) must be greater than zero", ErrInvalidSequenceDefinition, cache)
	}

	meta := &sequenceMeta{
		DataType:       dt,
		Increment:      inc,
		MinValue:       minVal,
		MaxValue:       maxVal,
		Start:          start,
		Cache:          cache,
		Cycle:          opts.HasCycle && opts.Cycle,
		IsCalled:       false,
		PersistedValue: start,
		Generation:     gen,
		OwnerTable:     ownerTable,
		OwnerColumn:    ownerColumn,
	}
	if err := validateSequenceMeta(name, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (d *database) createSequenceLocked(
	ctx context.Context,
	stmt *sqlast.CreateSequenceStmt,
) (sqldriver.Result, error) {
	name := stmt.Name
	if err := validateCatalogTableName(name); err != nil {
		return nil, fmt.Errorf("vibedb: CREATE SEQUENCE: %w", err)
	}
	if _, exists := d.catalog.Sequences[name]; exists {
		if stmt.IfNotExists {
			return result{}, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrSequenceExists, name)
	}
	if _, exists := d.tables[name]; exists {
		return nil, fmt.Errorf("%w: relation %q is a table", ErrSequenceExists, name)
	}
	if _, exists := d.catalog.Views[name]; exists {
		return nil, fmt.Errorf("%w: relation %q is a view", ErrSequenceExists, name)
	}
	if err := checkCatalogSequenceCount(len(d.catalog.Sequences) + 1); err != nil {
		return nil, fmt.Errorf("vibedb: CREATE SEQUENCE %q: %w", name, err)
	}

	d.seqGen++
	gen := d.seqGen
	meta, err := buildSequenceMetaFromOptions(name, stmt.Options, "BIGINT", "", "", gen)
	if err != nil {
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}

	previousPending := d.catalogWritePending
	d.catalog.Sequences[name] = meta
	published, err := d.persistCatalogLocked()
	if err != nil {
		if !published {
			delete(d.catalog.Sequences, name)
			d.catalogWritePending = previousPending
		}
		return nil, err
	}

	d.seqMu.Lock()
	if d.sequences == nil {
		d.sequences = make(map[string]*runtimeSequence)
	}
	d.sequences[name] = &runtimeSequence{
		name:         name,
		meta:         *meta,
		curValue:     meta.Start,
		curCalled:    false,
		logRemaining: 0,
	}
	d.seqMu.Unlock()

	if err := d.persistSequenceEntry(name, meta.Start, false, gen); err != nil {
		return nil, err
	}
	return result{}, nil
}

func (d *database) alterSequenceLocked(
	ctx context.Context,
	stmt *sqlast.AlterSequenceStmt,
) (sqldriver.Result, error) {
	name := stmt.Name
	oldMeta, exists := d.catalog.Sequences[name]
	if !exists {
		if stmt.IfExists {
			return result{}, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}

	d.seqMu.RLock()
	rt := d.sequences[name]
	d.seqMu.RUnlock()
	if rt == nil {
		return nil, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.dropped {
		if stmt.IfExists {
			return result{}, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}

	opts := stmt.Options
	updated := rt.meta

	dt := updated.DataType
	if dt == "" {
		dt = "BIGINT"
	}
	if opts.HasDataType {
		dt = opts.DataType
		updated.DataType = dt
	}
	typeMin, typeMax := sequenceTypeBounds(dt)

	if opts.HasIncrement {
		if opts.Increment == 0 {
			return nil, fmt.Errorf("%w: INCREMENT must not be zero", ErrInvalidSequenceDefinition)
		}
		updated.Increment = opts.Increment
	}
	if opts.NoMinValue {
		if updated.Increment > 0 {
			updated.MinValue = 1
		} else {
			updated.MinValue = typeMin
		}
	} else if opts.HasMinValue {
		updated.MinValue = opts.MinValue
	}
	if opts.NoMaxValue {
		if updated.Increment > 0 {
			updated.MaxValue = typeMax
		} else {
			updated.MaxValue = -1
		}
	} else if opts.HasMaxValue {
		updated.MaxValue = opts.MaxValue
	}
	if opts.HasDataType {
		if updated.MinValue < typeMin {
			updated.MinValue = typeMin
		}
		if updated.MaxValue > typeMax {
			updated.MaxValue = typeMax
		}
	}
	if updated.MinValue >= updated.MaxValue {
		return nil, fmt.Errorf("%w: MINVALUE (%d) must be less than MAXVALUE (%d)", ErrInvalidSequenceDefinition, updated.MinValue, updated.MaxValue)
	}
	if opts.HasStart {
		if opts.Start < updated.MinValue || opts.Start > updated.MaxValue {
			return nil, fmt.Errorf("%w: START value (%d) must be between MINVALUE (%d) and MAXVALUE (%d)", ErrInvalidSequenceDefinition, opts.Start, updated.MinValue, updated.MaxValue)
		}
		updated.Start = opts.Start
	} else {
		if updated.Start < updated.MinValue {
			updated.Start = updated.MinValue
		} else if updated.Start > updated.MaxValue {
			updated.Start = updated.MaxValue
		}
	}
	if opts.HasCache {
		if opts.Cache < 1 {
			return nil, fmt.Errorf("%w: CACHE (%d) must be greater than zero", ErrInvalidSequenceDefinition, opts.Cache)
		}
		updated.Cache = opts.Cache
	}
	if opts.HasCycle {
		updated.Cycle = opts.Cycle
	}

	newCur := rt.curValue
	newCalled := rt.curCalled
	if opts.HasRestart {
		if opts.HasRestartValue {
			if opts.RestartValue < updated.MinValue || opts.RestartValue > updated.MaxValue {
				return nil, fmt.Errorf("%w: RESTART value (%d) must be between MINVALUE (%d) and MAXVALUE (%d)", ErrInvalidSequenceDefinition, opts.RestartValue, updated.MinValue, updated.MaxValue)
			}
			newCur = opts.RestartValue
		} else {
			newCur = updated.Start
		}
		newCalled = false
	} else {
		if newCur < updated.MinValue || newCur > updated.MaxValue {
			return nil, fmt.Errorf("%w: sequence %q current value (%d) is out of new bounds [%d, %d]", ErrSequenceValueOutOfRange, name, newCur, updated.MinValue, updated.MaxValue)
		}
	}

	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}

	d.seqGen++
	updated.Generation = d.seqGen
	updated.PersistedValue = newCur
	updated.IsCalled = newCalled

	metaPtr := new(sequenceMeta)
	*metaPtr = updated

	previousPending := d.catalogWritePending
	d.catalog.Sequences[name] = metaPtr
	published, err := d.persistCatalogLocked()
	if err != nil {
		if !published {
			d.catalog.Sequences[name] = oldMeta
			d.catalogWritePending = previousPending
		}
		return nil, err
	}

	rt.meta = updated
	rt.curValue = newCur
	rt.curCalled = newCalled
	rt.logRemaining = 0

	if err := d.persistSequenceEntry(name, newCur, newCalled, updated.Generation); err != nil {
		return nil, err
	}
	return result{}, nil
}

func (d *database) dropSequenceLocked(
	ctx context.Context,
	stmt *sqlast.DropSequenceStmt,
) (sqldriver.Result, error) {
	name := stmt.Name
	oldMeta, exists := d.catalog.Sequences[name]
	if !exists {
		if stmt.IfExists {
			return result{}, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}
	if oldMeta != nil && oldMeta.OwnerTable != "" {
		if _, tableExists := d.tables[oldMeta.OwnerTable]; tableExists {
			return nil, fmt.Errorf(
				"%w: cannot drop sequence %q because column %s.%s requires it",
				ErrDependentObjects, name, oldMeta.OwnerTable, oldMeta.OwnerColumn,
			)
		}
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}

	previousPending := d.catalogWritePending
	delete(d.catalog.Sequences, name)
	published, err := d.persistCatalogLocked()
	if err != nil {
		if !published {
			d.catalog.Sequences[name] = oldMeta
			d.catalogWritePending = previousPending
		}
		return nil, err
	}

	d.seqMu.Lock()
	if rt, ok := d.sequences[name]; ok {
		rt.mu.Lock()
		rt.dropped = true
		rt.mu.Unlock()
		delete(d.sequences, name)
	}
	d.seqMu.Unlock()

	if err := d.deletePersistedSequenceEntries([]string{name}); err != nil {
		return nil, err
	}
	return result{}, nil
}

// allocateOwnedSequenceNameLocked generates a unique PostgreSQL-style sequence
// name for a SERIAL or IDENTITY column (table_col_seq, or table_col_seq1, ...).
// Caller must hold d.mu.Lock().
func (d *database) allocateOwnedSequenceNameLocked(tableName, colName string, pending map[string]*sequenceMeta) string {
	base := tableName + "_" + colName + "_seq"
	exists := func(candidate string) bool {
		if _, ok := d.catalog.Sequences[candidate]; ok {
			return true
		}
		if _, ok := pending[candidate]; ok {
			return true
		}
		if _, ok := d.tables[candidate]; ok {
			return true
		}
		if _, ok := d.catalog.Views[candidate]; ok {
			return true
		}
		return false
	}
	if !exists(base) {
		return base
	}
	for i := 1; ; i++ {
		candidate := base + strconv.Itoa(i)
		if !exists(candidate) {
			return candidate
		}
	}
}

func serialDefaultDataType(serial sqlast.SerialKind) string {
	switch serial {
	case sqlast.SerialSmall:
		return "SMALLINT"
	case sqlast.SerialInt:
		return "INTEGER"
	default:
		return "BIGINT"
	}
}

func escapeSQLSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
