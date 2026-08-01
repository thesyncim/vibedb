package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

var errNoLastInsertID = errors.New("vibedb: LastInsertId is unavailable; primary keys come from JSON documents")

type result struct{ affected int64 }

var _ sqldriver.Result = result{}

func (result) LastInsertId() (int64, error) { return 0, errNoLastInsertID }
func (r result) RowsAffected() (int64, error) {
	return r.affected, nil
}

func (c *conn) execMutation(statement *query.DMLStatement, args []any) (sqldriver.Result, error) {
	return c.execMutationContext(backgroundContext, statement, args)
}

func (c *conn) execMutationContext(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
) (sqldriver.Result, error) {
	d := c.db
	if statement.Kind() == query.DDLCreateIndex {
		return d.createIndexContext(ctx, statement)
	}
	if err := lockContext(ctx, &d.mu); err != nil {
		return nil, err
	}
	defer d.mu.Unlock()
	if d.closed {
		return nil, sqldriver.ErrBadConn
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if err := d.settleCatalogLocked(); err != nil {
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	switch statement.Kind() {
	case query.DDLCreateTable:
		return d.createTableLockedContext(ctx, statement)
	}
	t, ok := d.tables[statement.Collection()]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, statement.Collection())
	}
	switch statement.Kind() {
	case query.DMLInsert:
		return c.insertLocked(ctx, statement, args, t, nil, nil)
	case query.DMLUpdate:
		return c.updateLocked(ctx, statement, args, t)
	case query.DMLDelete:
		return c.deleteLocked(ctx, statement, args, t)
	}
	return nil, fmt.Errorf("vibedb: unsupported statement %s", statement.Kind())
}

func (d *database) createTableLocked(statement *query.DMLStatement) (sqldriver.Result, error) {
	return d.createTableLockedContext(backgroundContext, statement)
}

func (d *database) createTableLockedContext(
	ctx context.Context,
	statement *query.DMLStatement,
) (sqldriver.Result, error) {
	definition, err := statement.LowerTable()
	if err != nil {
		return nil, err
	}
	if err := validateCatalogTableName(definition.Name); err != nil {
		return nil, fmt.Errorf("vibedb: CREATE TABLE: %w", err)
	}
	if _, exists := d.tables[definition.Name]; exists {
		if definition.IfNotExists {
			return result{}, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrTableExists, definition.Name)
	}
	if err := checkCatalogTableCount(len(d.tables) + 1); err != nil {
		return nil, fmt.Errorf("vibedb: CREATE TABLE %q: %w",
			definition.Name, err)
	}
	if len(definition.PrimaryKey) != 1 {
		return nil, fmt.Errorf("vibedb: CREATE TABLE requires exactly one PRIMARY KEY JSON path")
	}
	path := statement.Tree().CreateTable.PrimaryKey[0]
	pointer := string(path.AppendPointer(nil))
	primary, err := vibejson.CompilePointer(pointer)
	if err != nil {
		return nil, fmt.Errorf("vibedb: compile primary-key path %q: %w", pointer, err)
	}
	if err := validatePrimarySchema(pointer, definition.Schema); err != nil {
		return nil, fmt.Errorf(
			"vibedb: CREATE TABLE %q primary key: %w",
			definition.Name, err,
		)
	}
	meta := &tableMeta{
		PrimaryKey: pointer,
		Schema:     schemaMetaFrom(definition.Schema),
	}
	candidate := &table{
		meta: meta, schema: definition.Schema, primary: primary,
	}
	if err := durable.ValidateOptions(durableOptions(candidate)); err != nil {
		return nil, fmt.Errorf(
			"vibedb: CREATE TABLE %q cannot be represented durably: %w",
			definition.Name, err)
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	d.catalog.Tables[definition.Name] = meta
	d.tables[definition.Name] = candidate
	if published, err := d.persistCatalogLocked(); err != nil {
		if !published {
			delete(d.catalog.Tables, definition.Name)
			delete(d.tables, definition.Name)
		}
		return nil, err
	}
	return result{}, nil
}

func (d *database) createIndexLocked(statement *query.DMLStatement) (sqldriver.Result, error) {
	return d.createIndexLockedContext(backgroundContext, statement)
}

// createIndexContext keeps the catalog mutex only around validation and the
// final metadata publication. A materialized table delegates the expensive
// scan to durable.Collection.CreateIndexContext, whose optimistic leaf
// reconciliation leaves concurrent SQL reads and writes running.
func (d *database) createIndexContext(
	ctx context.Context,
	statement *query.DMLStatement,
) (sqldriver.Result, error) {
	if err := lockContext(ctx, &d.mu); err != nil {
		return nil, err
	}
	if d.closed {
		d.mu.Unlock()
		return nil, sqldriver.ErrBadConn
	}
	if err := d.settleCatalogLocked(); err != nil {
		d.mu.Unlock()
		return nil, err
	}
	definition, err := statement.LowerIndex()
	if err != nil {
		d.mu.Unlock()
		return nil, err
	}
	if _, err := store.CompileExactIndex(definition.Definition); err != nil {
		d.mu.Unlock()
		return nil, fmt.Errorf("vibedb: CREATE INDEX: %w", err)
	}
	t, exists := d.tables[definition.Table]
	if !exists {
		d.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, definition.Table)
	}
	for _, index := range t.meta.Indexes {
		if index.Name == definition.Definition.Name {
			d.mu.Unlock()
			if definition.IfNotExists {
				return result{}, nil
			}
			return nil, fmt.Errorf("%w: %q", ErrIndexExists, index.Name)
		}
	}
	if t.collection == nil {
		defer d.mu.Unlock()
		return d.createIndexLockedContext(ctx, statement)
	}
	collection := t.collection
	if err := contextCheckpoint(ctx); err != nil {
		d.mu.Unlock()
		return nil, err
	}
	d.mu.Unlock()

	_, err = collection.CreateIndexContext(ctx, definition.Definition)
	if errors.Is(err, store.ErrIndexExists) && definition.IfNotExists {
		err = nil
	}
	if errors.Is(err, store.ErrIndexExists) {
		return nil, fmt.Errorf(
			"%w: %q", ErrIndexExists, definition.Definition.Name,
		)
	}
	if errors.Is(err, durable.ErrIndexBuildInProgress) {
		return nil, fmt.Errorf(
			"%w: table %q", ErrIndexBuildInProgress, definition.Table,
		)
	}
	if err != nil {
		return nil, err
	}

	if err := lockContext(ctx, &d.mu); err != nil {
		// The durable catalog is already authoritative. A canceled SQL metadata
		// mirror is repaired on reopen or by the next catalog settlement.
		d.mu.Lock()
		_, _ = syncTableIndexMeta(t)
		d.catalogWritePending = true
		d.mu.Unlock()
		return nil, fmt.Errorf(
			"%w: durable index committed before SQL catalog cancellation: %v",
			durable.ErrCommitOutcomeUnknown, err,
		)
	}
	defer d.mu.Unlock()
	if _, syncErr := syncTableIndexMeta(t); syncErr != nil {
		d.catalogWritePending = true
		return nil, fmt.Errorf(
			"%w: durable index committed before SQL catalog refresh: %v",
			durable.ErrCommitOutcomeUnknown, syncErr,
		)
	}
	published, persistErr := d.persistCatalogLocked()
	if persistErr != nil {
		d.catalogWritePending = !published
		if published {
			return nil, persistErr
		}
		return nil, fmt.Errorf(
			"%w: durable index committed before SQL catalog publication: %v",
			durable.ErrCommitOutcomeUnknown, persistErr,
		)
	}
	return result{}, nil
}

func (d *database) createIndexLockedContext(
	ctx context.Context,
	statement *query.DMLStatement,
) (sqldriver.Result, error) {
	definition, err := statement.LowerIndex()
	if err != nil {
		return nil, err
	}
	if _, err := store.CompileExactIndex(definition.Definition); err != nil {
		return nil, fmt.Errorf("vibedb: CREATE INDEX: %w", err)
	}
	t, exists := d.tables[definition.Table]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, definition.Table)
	}
	for _, index := range t.meta.Indexes {
		if index.Name == definition.Definition.Name {
			if definition.IfNotExists {
				return result{}, nil
			}
			return nil, fmt.Errorf("%w: %q", ErrIndexExists, index.Name)
		}
	}
	if t.collection != nil {
		return nil, errors.New(
			"vibedb: materialized CREATE INDEX reached the catalog-only path",
		)
	}
	proposed := append([]indexMeta(nil), t.meta.Indexes...)
	proposed = append(proposed, indexMeta{
		Name: definition.Definition.Name, Paths: append([]string(nil), definition.Definition.Paths...),
	})
	candidateMeta := *t.meta
	candidateMeta.Indexes = proposed
	if err := durable.ValidateOptions(durableOptions(&table{
		meta: &candidateMeta, schema: t.schema,
	})); err != nil {
		return nil, fmt.Errorf(
			"vibedb: CREATE INDEX %q cannot be represented durably: %w",
			definition.Definition.Name, err)
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	previous := t.meta.Indexes
	t.meta.Indexes = proposed
	if published, err := d.persistCatalogLocked(); err != nil {
		if !published {
			t.meta.Indexes = previous
		}
		return nil, err
	}
	return result{}, nil
}

func (c *conn) insertLocked(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
	returning *query.Statement,
	returned *query.Cursor,
) (sqldriver.Result, error) {
	tree := statement.Tree().Insert
	limits, err := tableMutationLimits(t)
	if err != nil {
		return nil, err
	}
	if len(tree.Rows) > limits.MaxBatchDocuments {
		return nil, fmt.Errorf(
			"vibedb: INSERT has %d rows, durable batch limit is %d: %w",
			len(tree.Rows), limits.MaxBatchDocuments, durable.ErrBatchTooLarge,
		)
	}
	seeds := make([]seedDocument, 0, len(tree.Rows))
	var seen map[string]struct{}
	if len(tree.Rows) > 1 {
		seen = make(map[string]struct{}, len(tree.Rows))
	}
	stagedBytes := 0
	cancellable := ctx.Done() != nil
	for i := range tree.Rows {
		if cancellable {
			if err := contextCheckpoint(ctx); err != nil {
				return nil, err
			}
		}
		document, key, err := resolveInsertRow(
			statement, tree, &tree.Rows[i], args,
			t.meta.PrimaryKey, t.primary, limits,
		)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf(
				"%w: %q appears twice in one VALUES batch",
				ErrDuplicatePrimaryKey, key,
			)
		}
		if seen != nil {
			seen[key] = struct{}{}
		}
		stagedBytes += len(key) + len(document)
		if stagedBytes > limits.MaxBatchBytes {
			return nil, fmt.Errorf(
				"vibedb: INSERT would stage %d key/document bytes, durable batch limit is %d: %w",
				stagedBytes, limits.MaxBatchBytes, durable.ErrBatchTooLarge,
			)
		}
		seeds = append(seeds, seedDocument{key: key, document: document})
	}
	if returning != nil {
		if returned == nil {
			return nil, errors.New("vibedb: internal RETURNING cursor is nil")
		}
		c.pointDocs.Reset()
		for i := range seeds {
			if _, err := c.pointDocs.Append(seeds[i].document); err != nil {
				return nil, err
			}
		}
		cursor, err := returning.RunInto(
			&c.exec, query.FromSegment(&c.pointDocs), nil,
		)
		if err != nil {
			return nil, err
		}
		*returned = cursor
	}
	if t.collection == nil {
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		_, err := c.db.materializeLocked(tree.Table, seeds)
		if t.collection != nil {
			// materializeLocked retains the live collection when the file was
			// published but its namespace fence could not be acknowledged.
			// Such an unknown outcome is still a logical write for every
			// transaction that began before it.
			t.conflicts.recordSeeds(seeds)
		}
		if err != nil {
			return nil, err
		}
		return result{affected: int64(len(seeds))}, nil
	}
	if err := c.rejectExistingSeeds(ctx, t.collection, seeds); err != nil {
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	beforeGeneration := t.collection.Generation()
	err = putSeedsAtomic(t.collection, seeds)
	if collectionMutationPublished(t.collection, beforeGeneration, err) {
		t.conflicts.recordSeeds(seeds)
	}
	if err != nil {
		return nil, err
	}
	return result{affected: int64(len(seeds))}, nil
}

// insertReturningContext executes an INSERT and materializes its RETURNING
// projection from the already-resolved documents before publication. A
// projection or result-budget failure therefore leaves storage untouched, and
// the successful path performs no storage reread.
func (c *conn) insertReturningContext(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	returning *query.Statement,
) (query.Cursor, error) {
	var cursor query.Cursor
	d := c.db
	if err := lockContext(ctx, &d.mu); err != nil {
		return cursor, err
	}
	defer d.mu.Unlock()
	if d.closed {
		return cursor, sqldriver.ErrBadConn
	}
	if err := contextCheckpoint(ctx); err != nil {
		return cursor, err
	}
	if err := d.settleCatalogLocked(); err != nil {
		return cursor, err
	}
	if statement.Kind() != query.DMLInsert || returning == nil {
		return cursor, errors.New(
			"vibedb: internal returning execution requires INSERT RETURNING",
		)
	}
	t, ok := d.tables[statement.Collection()]
	if !ok {
		return cursor, fmt.Errorf(
			"%w: %q", ErrTableNotFound, statement.Collection(),
		)
	}
	if _, err := c.insertLocked(
		ctx, statement, args, t, returning, &cursor,
	); err != nil {
		return query.Cursor{}, err
	}
	return cursor, nil
}

func (c *conn) rejectExistingSeeds(
	ctx context.Context,
	collection *durable.Collection,
	seeds []seedDocument,
) error {
	scratch := c.pointRaw[:0]
	cancellable := ctx.Done() != nil
	for _, seed := range seeds {
		if cancellable {
			if err := contextCheckpoint(ctx); err != nil {
				c.pointRaw = scratch
				return err
			}
		}
		var found bool
		var err error
		scratch, found, err = collection.AppendRaw(scratch[:0], []byte(seed.key))
		if err != nil {
			c.pointRaw = scratch
			return err
		}
		if found {
			c.pointRaw = scratch
			return fmt.Errorf("%w: %q", ErrDuplicatePrimaryKey, seed.key)
		}
	}
	c.pointRaw = scratch
	return nil
}

func putSeedsAtomic(collection *durable.Collection, seeds []seedDocument) error {
	switch len(seeds) {
	case 0:
		return nil
	case 1:
		_, err := collection.Put([]byte(seeds[0].key), seeds[0].document)
		return err
	}
	return collection.Update(func(batch *durable.WriteBatch) error {
		for _, seed := range seeds {
			if err := batch.Put([]byte(seed.key), seed.document); err != nil {
				return err
			}
		}
		return nil
	})
}

func resolveInsertRow(
	statement *query.DMLStatement,
	insert *sqlast.InsertStmt,
	row *sqlast.InsertRow,
	args []any,
	primaryKey string,
	primary vibejson.CompiledPointer,
	limits durable.Options,
) ([]byte, string, error) {
	var document []byte
	var err error
	if len(insert.Columns) == 0 {
		document, err = operandDocument(statement, row.Values[0], args)
	} else {
		value := make(map[string]any, len(insert.Columns))
		inputBytes := 2
		for i, column := range insert.Columns {
			value[column.Segments[0].Key], err = jsonOperandValue(row.Values[i], args)
			if err != nil {
				return nil, "", err
			}
			inputBytes += len(column.Segments[0].Key) + 4 +
				scalarInputBytes(value[column.Segments[0].Key])
			if inputBytes > limits.MaxDocumentBytes {
				return nil, "", durable.ErrDocumentTooLarge
			}
		}
		document, err = json.Marshal(value)
	}
	if err != nil {
		return nil, "", err
	}
	if len(document) > limits.MaxDocumentBytes {
		return nil, "", durable.ErrDocumentTooLarge
	}
	key, err := documentKey(
		document, primaryKey, primary, limits.MaxKeyBytes,
	)
	return document, key, err
}

func scalarInputBytes(value any) int {
	switch value := value.(type) {
	case string:
		return len(value)
	case json.Number:
		return len(value)
	case nil:
		return 4
	default:
		return 8
	}
}

func tableMutationLimits(t *table) (durable.Options, error) {
	if t.collection != nil {
		return durable.Options{
			MaxKeyBytes:       t.collection.MaxKeyBytes(),
			MaxDocumentBytes:  t.collection.MaxDocumentBytes(),
			MaxBatchDocuments: t.collection.MaxBatchDocuments(),
			MaxBatchBytes:     t.collection.MaxBatchBytes(),
		}, nil
	}
	return durable.NormalizeOptions(durableOptions(t))
}

func operandDocument(statement *query.DMLStatement, operand sqlast.Operand, args []any) ([]byte, error) {
	if operand.Kind == sqlast.OperandParam && operand.Ordinal < len(args) {
		switch value := args[operand.Ordinal].(type) {
		case *string:
			if value == nil {
				return nil, errors.New(
					"vibedb: a document cannot be bound from a nil *string")
			}
			return byteview.Bytes(*value), nil
		case *[]byte:
			if value == nil {
				return nil, errors.New(
					"vibedb: a document cannot be bound from a nil *[]byte")
			}
			return *value, nil
		}
	}
	return statement.Document(operand, args)
}

func operandValue(operand sqlast.Operand, args []any) (any, error) {
	switch operand.Kind {
	case sqlast.OperandParam:
		if operand.Ordinal >= len(args) {
			return nil, fmt.Errorf("vibedb: placeholder %d was not bound", operand.Ordinal+1)
		}
		arg := args[operand.Ordinal]
		switch value := arg.(type) {
		case nil, bool,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			// Return the caller's existing interface box. Returning the
			// type-switched local would allocate a second box for every warm
			// predicate bind.
			return arg, nil
		case float32:
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return nil, errors.New(
					"vibedb: numeric parameters must be finite JSON numbers")
			}
			return arg, nil
		case float64:
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, errors.New(
					"vibedb: numeric parameters must be finite JSON numbers")
			}
			return arg, nil
		case string:
			if !utf8.ValidString(value) {
				return nil, errors.New(
					"vibedb: a flat INSERT string must be valid UTF-8")
			}
			return arg, nil
		case query.Number, json.Number:
			return arg, nil
		case []byte:
			if !utf8.Valid(value) {
				return nil, errors.New(
					"vibedb: a flat INSERT string must be valid UTF-8")
			}
			return arg, nil
		case *bool:
			if value == nil {
				return nil, nil
			}
			return arg, nil
		case *int64:
			if value == nil {
				return nil, nil
			}
			return arg, nil
		case *float64:
			if value == nil {
				return nil, nil
			}
			if math.IsNaN(*value) || math.IsInf(*value, 0) {
				return nil, errors.New(
					"vibedb: numeric parameters must be finite JSON numbers")
			}
			return arg, nil
		case *string:
			if value == nil {
				return nil, nil
			}
			if !utf8.ValidString(*value) {
				return nil, errors.New(
					"vibedb: a flat INSERT string must be valid UTF-8")
			}
			return arg, nil
		case *query.Number:
			if value == nil {
				return nil, nil
			}
			return arg, nil
		default:
			return nil, fmt.Errorf("vibedb: %T is not a JSON scalar driver value", value)
		}
	case sqlast.OperandString:
		if !utf8.ValidString(operand.Text) {
			return nil, errors.New(
				"vibedb: a flat INSERT string must be valid UTF-8")
		}
		return operand.Text, nil
	case sqlast.OperandBool:
		return operand.Bool, nil
	case sqlast.OperandNumber:
		return json.Number(operand.Text), nil
	}
	return nil, errors.New("vibedb: flat INSERT values must be JSON scalars or placeholders")
}

// jsonOperandValue is the flat-INSERT encoder boundary. operandValue preserves
// caller interface headers and pointer-shaped protocol slots for the
// allocation-free primary-key path; encoding/json instead needs exact numbers
// identified as json.Number and byte strings identified as strings.
func jsonOperandValue(operand sqlast.Operand, args []any) (any, error) {
	value, err := operandValue(operand, args)
	if err != nil {
		return nil, err
	}
	switch value := value.(type) {
	case query.Number:
		return json.Number(value), nil
	case *query.Number:
		return json.Number(*value), nil
	case []byte:
		return string(value), nil
	case float32:
		// Match query's float32 literal rule: it widens to float64 before
		// choosing the exact decimal spelling.
		return float64(value), nil
	case *bool:
		return *value, nil
	case *int64:
		return *value, nil
	case *float64:
		return *value, nil
	case *string:
		return *value, nil
	default:
		return value, nil
	}
}

func documentKey(
	document []byte,
	pointer string,
	primary vibejson.CompiledPointer,
	maxKeyBytes int,
) (string, error) {
	value, found, err := primary.GetRaw(document)
	if err != nil {
		return "", fmt.Errorf("vibedb: invalid JSON document: %w", err)
	}
	if !found {
		return "", fmt.Errorf("vibedb: primary key path %q is missing", pointer)
	}
	if value.IsNull() {
		return "", fmt.Errorf("vibedb: primary key path %q is null", pointer)
	}
	switch value.Kind() {
	case jsondoc.String:
		raw := value.Bytes()
		size, ok := orderedkey.JSONStringEncodedSize(raw)
		if !ok {
			return "", fmt.Errorf(
				"vibedb: primary key path %q has an invalid JSON string",
				pointer,
			)
		}
		if size > maxKeyBytes {
			return "", durable.ErrKeyTooLarge
		}
		key, _ := orderedkey.AppendJSONString(
			nil, raw, orderedkey.Ascending,
		)
		return string(key), nil
	case jsondoc.Bool:
		if maxKeyBytes < 1 {
			return "", durable.ErrKeyTooLarge
		}
		boolean, _ := value.Bool()
		key, _ := orderedkey.AppendBool(nil, boolean, orderedkey.Ascending)
		return string(key), nil
	case jsondoc.Number:
		number, _ := value.NumberBytes()
		size, ok := orderedkey.NumberEncodedSize(number)
		if !ok {
			return "", fmt.Errorf(
				"vibedb: primary key path %q is not a valid JSON number",
				pointer,
			)
		}
		if size > maxKeyBytes {
			return "", durable.ErrKeyTooLarge
		}
		key, _ := orderedkey.AppendNumber(nil, number, orderedkey.Ascending)
		return string(key), nil
	default:
		return "", fmt.Errorf("vibedb: primary key path %q must hold a JSON string, number, or bool", pointer)
	}
}

func validateDocument(
	schema *store.Schema,
	document []byte,
	maxDocumentBytes int,
	tape *[]vibejson.IndexEntry,
) error {
	if len(document) > maxDocumentBytes {
		return durable.ErrDocumentTooLarge
	}
	need := len(document)/8 + 8
	limit := len(document) + 2
	for {
		if cap(*tape) < need {
			*tape = make([]vibejson.IndexEntry, need)
		}
		index, err := vibejson.BuildIndex(
			document, (*tape)[:cap(*tape)],
		)
		if err != jsondoc.ErrIndexFull || cap(*tape) >= limit {
			if err != nil {
				return fmt.Errorf("vibedb: invalid JSON document: %w", err)
			}
			if schema == nil {
				return nil
			}
			return schema.ValidateIndex(index)
		}
		need = min(2*cap(*tape), limit)
	}
}

func (c *conn) updateLocked(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
) (sqldriver.Result, error) {
	limits, err := tableMutationLimits(t)
	if err != nil {
		return nil, err
	}
	document, err := operandDocument(
		statement, statement.Tree().Update.Doc, args,
	)
	if err != nil {
		return nil, err
	}
	if len(document) > limits.MaxDocumentBytes {
		return nil, durable.ErrDocumentTooLarge
	}
	keys, err := c.matchingKeysLocked(
		ctx, statement, args, t, limits, len(document))
	if err != nil {
		return nil, err
	}
	newKey, err := documentKey(
		document, t.meta.PrimaryKey, t.primary, limits.MaxKeyBytes,
	)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if key != newKey {
			return nil, fmt.Errorf(
				"%w: replacement key %q does not match selected key %q",
				ErrUpdatePrimaryKey, newKey, key,
			)
		}
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if t.collection == nil {
		return result{}, nil
	}
	beforeGeneration := t.collection.Generation()
	var mutationErr error
	switch len(keys) {
	case 0:
	case 1:
		_, mutationErr = t.collection.Put([]byte(keys[0]), document)
	default:
		mutationErr = t.collection.Update(func(batch *durable.WriteBatch) error {
			for _, key := range keys {
				if err := batch.Put([]byte(key), document); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if collectionMutationPublished(
		t.collection, beforeGeneration, mutationErr,
	) {
		t.conflicts.recordKeys(keys)
	}
	if mutationErr != nil {
		return nil, mutationErr
	}
	return result{affected: int64(len(keys))}, nil
}

func (c *conn) deleteLocked(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
) (sqldriver.Result, error) {
	limits, err := tableMutationLimits(t)
	if err != nil {
		return nil, err
	}
	keys, err := c.matchingKeysLocked(ctx, statement, args, t, limits, 0)
	if err != nil {
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if t.collection == nil {
		return result{}, nil
	}
	beforeGeneration := t.collection.Generation()
	affected := len(keys)
	var mutationErr error
	switch len(keys) {
	case 0:
	case 1:
		var deleted bool
		deleted, mutationErr = t.collection.Delete([]byte(keys[0]))
		if !deleted {
			affected = 0
		}
	default:
		mutationErr = t.collection.Update(func(batch *durable.WriteBatch) error {
			for _, key := range keys {
				if err := batch.Delete([]byte(key)); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if collectionMutationPublished(
		t.collection, beforeGeneration, mutationErr,
	) {
		t.conflicts.recordKeys(keys)
	}
	if mutationErr != nil {
		return nil, mutationErr
	}
	return result{affected: int64(affected)}, nil
}

func (c *conn) matchingKeysLocked(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
	limits durable.Options,
	valueBytes int,
) ([]string, error) {
	budget := mutationMatchBudget{
		table:        statement.Collection(),
		maxDocuments: limits.MaxBatchDocuments,
		maxBytes:     limits.MaxBatchBytes,
		valueBytes:   valueBytes,
	}
	var where *sqlast.Expr
	switch statement.Tree().Kind {
	case sqlast.KindUpdate:
		if statement.Tree().Update.Filter != nil {
			where = statement.Tree().Update.Filter.Where
		}
	case sqlast.KindDelete:
		if statement.Tree().Delete.Filter != nil {
			where = statement.Tree().Delete.Filter.Where
		}
	}
	keys, point, err := primaryPredicateKeys(
		where, t.meta.PrimaryKey, args, c.pointKeys,
		limits.MaxKeyBytes)
	c.pointKeys = keys
	if point || err != nil {
		if err != nil {
			return nil, err
		}
		if t.collection == nil {
			clear(keys)
			c.pointKeys = keys[:0]
			return c.pointKeys, nil
		}
		present := keys[:0]
		scratch := c.pointRaw[:0]
		cancellable := ctx.Done() != nil
		for _, key := range keys {
			if cancellable {
				if err := contextCheckpoint(ctx); err != nil {
					c.pointRaw = scratch
					return nil, err
				}
			}
			var found bool
			scratch, found, err = t.collection.AppendRaw(scratch[:0], []byte(key))
			if err != nil {
				c.pointRaw = scratch
				return nil, err
			}
			if found {
				if err := budget.admit(len(key)); err != nil {
					c.pointRaw = scratch
					return nil, err
				}
				present = append(present, key)
			}
		}
		clear(keys[len(present):])
		c.pointRaw = scratch
		c.pointKeys = present
		return present, nil
	}

	clear(c.matchKeys)
	keys = c.matchKeys[:0]
	filter, err := statement.Filter(&c.exec, args, func(key, _ []byte) error {
		if err := budget.admit(len(key)); err != nil {
			return err
		}
		keys = append(keys, string(key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if t.collection == nil {
		if err := filter.Done(); err != nil {
			return nil, err
		}
		c.matchKeys = keys
		return keys, nil
	}
	snapshot, err := t.collection.Snapshot()
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()
	if err := snapshot.RangeRaw(func(key, document []byte) error {
		return filter.Add(key, document)
	}); err != nil {
		return nil, err
	}
	if err := filter.Done(); err != nil {
		return nil, err
	}
	c.matchKeys = keys
	return keys, nil
}

// mutationMatchBudget bounds the result of a filtered UPDATE or DELETE while
// the table is still being scanned. The durable batch has the same limits, but
// discovering them only after retaining every matching key would let a
// statement that must fail grow memory with table cardinality.
type mutationMatchBudget struct {
	table        string
	documents    int
	maxDocuments int
	bytes        int
	maxBytes     int
	valueBytes   int
}

func (b *mutationMatchBudget) admit(keyBytes int) error {
	if b.documents >= b.maxDocuments {
		return fmt.Errorf(
			"vibedb: statement matches more than %d documents in table %q: %w",
			b.maxDocuments, b.table, durable.ErrBatchTooLarge,
		)
	}
	if keyBytes > b.maxBytes-b.bytes ||
		b.valueBytes > b.maxBytes-b.bytes-keyBytes {
		return fmt.Errorf(
			"vibedb: statement exceeds the %d-byte mutation batch limit for table %q: %w",
			b.maxBytes, b.table, durable.ErrBatchTooLarge,
		)
	}
	b.documents++
	b.bytes += keyBytes + b.valueBytes
	return nil
}
