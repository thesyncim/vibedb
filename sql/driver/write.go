package driver

import (
	"bytes"
	sqldriver "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

var errNoLastInsertID = errors.New("vibedb: LastInsertId is unavailable; primary keys come from JSON documents")

type result struct{ affected int64 }

var _ sqldriver.Result = result{}

func (result) LastInsertId() (int64, error) { return 0, errNoLastInsertID }
func (r result) RowsAffected() (int64, error) {
	return r.affected, nil
}

func (c *conn) execMutation(statement *query.DMLStatement, args []any) (sqldriver.Result, error) {
	d := c.db
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, sqldriver.ErrBadConn
	}
	switch statement.Kind() {
	case query.DDLCreateTable:
		return d.createTableLocked(statement)
	case query.DDLCreateIndex:
		return d.createIndexLocked(statement)
	}
	t, ok := d.tables[statement.Collection()]
	if !ok {
		return nil, fmt.Errorf("vibedb: table %q does not exist", statement.Collection())
	}
	if len(t.meta.Indexes) != 0 && t.collection != nil {
		return nil, ErrIndexedTableReadOnly
	}
	switch statement.Kind() {
	case query.DMLInsert:
		return c.insertLocked(statement, args, t)
	case query.DMLUpdate:
		return c.updateLocked(statement, args, t)
	case query.DMLDelete:
		return c.deleteLocked(statement, args, t)
	}
	return nil, fmt.Errorf("vibedb: unsupported statement %s", statement.Kind())
}

func (d *database) createTableLocked(statement *query.DMLStatement) (sqldriver.Result, error) {
	definition, err := statement.LowerTable()
	if err != nil {
		return nil, err
	}
	if _, exists := d.tables[definition.Name]; exists {
		if definition.IfNotExists {
			return result{}, nil
		}
		return nil, fmt.Errorf("vibedb: table %q already exists", definition.Name)
	}
	if len(definition.PrimaryKey) != 1 {
		return nil, fmt.Errorf("vibedb: CREATE TABLE requires exactly one PRIMARY KEY JSON path")
	}
	path := statement.Tree().CreateTable.PrimaryKey[0]
	pointer := string(path.AppendPointer(nil))
	meta := &tableMeta{PrimaryKey: pointer}
	d.catalog.Tables[definition.Name] = meta
	d.tables[definition.Name] = &table{meta: meta}
	if err := d.persistCatalogLocked(); err != nil {
		delete(d.catalog.Tables, definition.Name)
		delete(d.tables, definition.Name)
		return nil, err
	}
	return result{}, nil
}

func (d *database) createIndexLocked(statement *query.DMLStatement) (sqldriver.Result, error) {
	definition, err := statement.LowerIndex()
	if err != nil {
		return nil, err
	}
	t, exists := d.tables[definition.Table]
	if !exists {
		return nil, fmt.Errorf("vibedb: table %q does not exist", definition.Table)
	}
	for _, index := range t.meta.Indexes {
		if index.Name == definition.Definition.Name {
			if definition.IfNotExists {
				return result{}, nil
			}
			return nil, fmt.Errorf("vibedb: index %q already exists", index.Name)
		}
	}
	if t.collection != nil {
		return nil, errors.New("vibedb: CREATE INDEX must be declared before the first INSERT; durable exact indexes are frozen at collection creation")
	}
	t.meta.Indexes = append(t.meta.Indexes, indexMeta{
		Name: definition.Definition.Name, Paths: append([]string(nil), definition.Definition.Paths...),
	})
	if err := d.persistCatalogLocked(); err != nil {
		t.meta.Indexes = t.meta.Indexes[:len(t.meta.Indexes)-1]
		return nil, err
	}
	return result{}, nil
}

func (c *conn) insertLocked(statement *query.DMLStatement, args []any, t *table) (sqldriver.Result, error) {
	tree := statement.Tree().Insert
	seeds := make([]seedDocument, 0, len(tree.Rows))
	for i := range tree.Rows {
		row := &tree.Rows[i]
		document, key, err := resolveInsertRow(statement, tree, row, args, t.meta.PrimaryKey)
		if err != nil {
			return nil, err
		}
		seeds = append(seeds, seedDocument{key: key, document: document})
	}
	if t.collection == nil {
		if _, err := c.db.materializeLocked(tree.Table, seeds); err != nil {
			return nil, err
		}
		return result{affected: int64(len(seeds))}, nil
	}
	var affected int64
	for _, seed := range seeds {
		if _, err := t.collection.Put(seed.key, seed.document); err != nil {
			return nil, err
		}
		affected++
	}
	return result{affected: affected}, nil
}

func resolveInsertRow(
	statement *query.DMLStatement,
	insert *sqlast.InsertStmt,
	row *sqlast.InsertRow,
	args []any,
	primaryKey string,
) ([]byte, string, error) {
	if row.Values == nil {
		key, err := statement.Key(row.Key, args)
		if err != nil {
			return nil, "", err
		}
		document, err := statement.Document(row.Doc, args)
		return document, key, err
	}
	var document []byte
	var err error
	if len(insert.Columns) == 0 {
		document, err = operandDocument(statement, row.Values[0], args)
	} else {
		value := make(map[string]any, len(insert.Columns))
		for i, column := range insert.Columns {
			value[column.Segments[0].Key], err = operandValue(row.Values[i], args)
			if err != nil {
				return nil, "", err
			}
		}
		document, err = json.Marshal(value)
	}
	if err != nil {
		return nil, "", err
	}
	key, err := documentKey(document, primaryKey)
	return document, key, err
}

func operandDocument(statement *query.DMLStatement, operand sqlast.Operand, args []any) ([]byte, error) {
	return statement.Document(operand, args)
}

func operandValue(operand sqlast.Operand, args []any) (any, error) {
	switch operand.Kind {
	case sqlast.OperandParam:
		if operand.Ordinal >= len(args) {
			return nil, fmt.Errorf("vibedb: placeholder %d was not bound", operand.Ordinal+1)
		}
		switch value := args[operand.Ordinal].(type) {
		case nil, bool, int64, float64, string:
			return value, nil
		case []byte:
			return string(value), nil
		default:
			return nil, fmt.Errorf("vibedb: %T is not a JSON scalar driver value", value)
		}
	case sqlast.OperandString:
		return operand.Text, nil
	case sqlast.OperandBool:
		return operand.Bool, nil
	case sqlast.OperandNumber:
		return json.Number(operand.Text), nil
	}
	return nil, errors.New("vibedb: flat INSERT values must be JSON scalars or placeholders")
}

func documentKey(document []byte, pointer string) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", fmt.Errorf("vibedb: invalid JSON document: %w", err)
	}
	current := value
	if pointer != "" {
		for _, raw := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
			part := strings.ReplaceAll(strings.ReplaceAll(raw, "~1", "/"), "~0", "~")
			switch node := current.(type) {
			case map[string]any:
				var ok bool
				current, ok = node[part]
				if !ok {
					return "", fmt.Errorf("vibedb: primary key path %q is missing", pointer)
				}
			case []any:
				index, err := strconv.Atoi(part)
				if err != nil || index < 0 || index >= len(node) {
					return "", fmt.Errorf("vibedb: primary key path %q is missing", pointer)
				}
				current = node[index]
			default:
				return "", fmt.Errorf("vibedb: primary key path %q is missing", pointer)
			}
		}
	}
	if current == nil {
		return "", fmt.Errorf("vibedb: primary key path %q is null", pointer)
	}
	switch current.(type) {
	case string, bool, json.Number:
	default:
		return "", fmt.Errorf("vibedb: primary key path %q must hold a JSON string, number, or bool", pointer)
	}
	raw, err := json.Marshal(current)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (c *conn) updateLocked(statement *query.DMLStatement, args []any, t *table) (sqldriver.Result, error) {
	if t.collection == nil {
		return result{}, nil
	}
	document, err := statement.Document(statement.Tree().Update.Doc, args)
	if err != nil {
		return nil, err
	}
	keys, err := c.matchingKeysLocked(statement, args, t)
	if err != nil {
		return nil, err
	}
	newKey, err := documentKey(document, t.meta.PrimaryKey)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if key != newKey {
			return nil, errors.New("vibedb: UPDATE cannot change a document's primary key")
		}
		if _, err := t.collection.Put(key, document); err != nil {
			return nil, err
		}
	}
	return result{affected: int64(len(keys))}, nil
}

func (c *conn) deleteLocked(statement *query.DMLStatement, args []any, t *table) (sqldriver.Result, error) {
	if t.collection == nil {
		return result{}, nil
	}
	keys, err := c.matchingKeysLocked(statement, args, t)
	if err != nil {
		return nil, err
	}
	var affected int64
	for _, key := range keys {
		deleted, err := t.collection.Delete(key)
		if err != nil {
			return nil, err
		}
		if deleted {
			affected++
		}
	}
	return result{affected: affected}, nil
}

func (c *conn) matchingKeysLocked(
	statement *query.DMLStatement,
	args []any,
	t *table,
) ([]string, error) {
	snapshot, err := t.collection.Snapshot()
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()
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
	if keys, point, err := primaryPredicateKeys(where, t.meta.PrimaryKey, args); point || err != nil {
		if err != nil {
			return nil, err
		}
		present := keys[:0]
		var scratch []byte
		for _, key := range keys {
			var found bool
			scratch, found, err = snapshot.AppendRaw(scratch[:0], key)
			if err != nil {
				return nil, err
			}
			if found {
				present = append(present, key)
			}
		}
		return present, nil
	}
	var keys []string
	filter, err := statement.Filter(&c.exec, args, func(key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := snapshot.RangeRaw(func(key, document []byte) error {
		return filter.Add(key, document)
	}); err != nil {
		return nil, err
	}
	if err := filter.Done(); err != nil {
		return nil, err
	}
	return keys, nil
}

func (c *conn) matchingKeysSnapshot(
	statement *query.DMLStatement,
	args []any,
	t *table,
	snapshot store.Snapshot,
) ([]string, error) {
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
	if keys, point, err := primaryPredicateKeys(where, t.meta.PrimaryKey, args); point || err != nil {
		if err != nil {
			return nil, err
		}
		present := keys[:0]
		var scratch []byte
		for _, key := range keys {
			var found bool
			scratch, found = snapshot.AppendRaw(scratch[:0], key)
			if found {
				present = append(present, key)
			}
		}
		return present, nil
	}
	var keys []string
	filter, err := statement.Filter(&c.exec, args, func(key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	var filterErr error
	snapshot.Range(func(key string, document vibejson.RawValue) bool {
		filterErr = filter.Add([]byte(key), document.Bytes())
		return filterErr == nil
	})
	if filterErr != nil {
		return nil, filterErr
	}
	if err := filter.Done(); err != nil {
		return nil, err
	}
	return keys, nil
}
