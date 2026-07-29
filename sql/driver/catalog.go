package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

const catalogVersion = 2

const maxCatalogTableNameBytes = 1<<16 - 1

// maxCatalogTables bounds both the in-memory catalog and the descriptors an
// eagerly opened database can own. Every materialized SQL table has one live
// durable file handle for the database lifetime; a byte-only catalog limit
// would otherwise permit a valid catalog with tens of thousands of tables that
// cannot be reopened under ordinary process descriptor limits.
//
// 128 leaves substantial headroom under the common conservative soft limit of
// 256 descriptors for the catalog lock, temporary publication files, spill
// runs, sockets, and the embedding process's own files.
const maxCatalogTables = 128

// maxCatalogBytes bounds both untrusted catalog reads and prospective catalog
// rewrites. DDL is cold, so persistCatalogLocked computes a conservative
// allocation-free upper bound before asking encoding/json to allocate.
const maxCatalogBytes = 16 << 20

type catalogFile struct {
	Version int                   `json:"version"`
	Tables  map[string]*tableMeta `json:"tables"`
}

type tableMeta struct {
	PrimaryKey   string      `json:"primary_key"`
	Schema       *schemaMeta `json:"schema,omitempty"`
	Indexes      []indexMeta `json:"indexes,omitempty"`
	Materialized bool        `json:"materialized,omitempty"`
}

type schemaMeta struct {
	Root   uint16            `json:"root"`
	Fields []schemaFieldMeta `json:"fields,omitempty"`
}

type schemaFieldMeta struct {
	Path     string `json:"path"`
	Types    uint16 `json:"types"`
	Required bool   `json:"required,omitempty"`
}

type indexMeta struct {
	Name  string   `json:"name"`
	Paths []string `json:"paths"`
}

type table struct {
	meta       *tableMeta
	schema     *store.Schema
	primary    vibejson.CompiledPointer
	file       *os.File
	collection *durable.Collection
	conflicts  txConflictClock
}

type database struct {
	mu                   sync.RWMutex
	path                 string
	dataDir              string
	lockFile             *os.File
	syncDir              func(string) error
	closeCollection      func(*durable.Collection) error
	catalog              catalogFile
	tables               map[string]*table
	catalogWritePending  bool
	catalogFencePending  bool
	tableDirFencePending bool
	closed               bool
	closeDone            bool
}

func openDatabase(path string) (*database, error) {
	return openDatabaseWithSync(path, nil)
}

// openDatabaseWithSync is openDatabase with an injectable directory fence for
// crash-consistency tests. Production callers always pass nil.
func openDatabaseWithSync(
	path string,
	syncDir func(string) error,
) (*database, error) {
	if path == "" {
		return nil, errors.New("vibedb: the DSN must be a file path")
	}
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return nil, err
	}
	lockFile, err := os.OpenFile(absolute+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := storeio.LockWriter(lockFile); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("vibedb: lock SQL catalog %s: %w", absolute, err)
	}
	d := &database{
		path: absolute, dataDir: absolute + ".tables", lockFile: lockFile,
		catalog: catalogFile{Version: catalogVersion, Tables: make(map[string]*tableMeta)},
		tables:  make(map[string]*table), syncDir: syncDir,
	}
	opened := false
	defer func() {
		if !opened {
			// A failed open has no caller that can retry cleanup. Release the
			// catalog writer lease and every descriptor even if a partially
			// opened collection reports a close error.
			_ = d.closeTerminal()
		}
	}()
	if err := d.recoverVisibleNamespace(); err != nil {
		return nil, err
	}
	raw, exists, err := readCatalogFile(absolute)
	if err != nil {
		return nil, err
	}
	if exists {
		if !utf8.Valid(raw) {
			return nil, fmt.Errorf(
				"vibedb: read SQL catalog %s: catalog text is not valid UTF-8",
				absolute,
			)
		}
		if err := json.Unmarshal(raw, &d.catalog); err != nil {
			return nil, fmt.Errorf("vibedb: read SQL catalog %s: %w", absolute, err)
		}
		if d.catalog.Version != catalogVersion || d.catalog.Tables == nil {
			return nil, fmt.Errorf("vibedb: unsupported SQL catalog version %d", d.catalog.Version)
		}
	}
	if err := checkCatalogTableCount(len(d.catalog.Tables)); err != nil {
		return nil, err
	}
	for name, meta := range d.catalog.Tables {
		if nameErr := validateCatalogTableName(name); nameErr != nil {
			_ = d.close()
			return nil, fmt.Errorf(
				"vibedb: SQL catalog table name %q: %w", name, nameErr)
		}
		if meta == nil {
			_ = d.close()
			return nil, fmt.Errorf("vibedb: SQL catalog table %q has null metadata", name)
		}
		primary, primaryErr := vibejson.CompilePointer(meta.PrimaryKey)
		if primaryErr != nil || len(primary.Tokens) == 0 {
			_ = d.close()
			if primaryErr == nil {
				primaryErr = errors.New("the primary-key path is empty")
			}
			return nil, fmt.Errorf(
				"vibedb: SQL catalog table %q primary key %q: %w",
				name, meta.PrimaryKey, primaryErr)
		}
		schema, schemaErr := compileSchemaMeta(meta.Schema)
		if schemaErr != nil {
			_ = d.close()
			return nil, fmt.Errorf("vibedb: SQL catalog table %q schema: %w", name, schemaErr)
		}
		if schemaErr := validatePrimarySchema(meta.PrimaryKey, schema); schemaErr != nil {
			_ = d.close()
			return nil, fmt.Errorf(
				"vibedb: SQL catalog table %q primary key: %w",
				name, schemaErr)
		}
		for _, index := range meta.Indexes {
			if _, indexErr := store.CompileExactIndex(store.IndexDefinition{
				Name: index.Name, Paths: index.Paths,
			}); indexErr != nil {
				_ = d.close()
				return nil, fmt.Errorf("vibedb: SQL catalog table %q index %q: %w", name, index.Name, indexErr)
			}
		}
		t := &table{meta: meta, schema: schema, primary: primary}
		if optionsErr := durable.ValidateOptions(durableOptions(t)); optionsErr != nil {
			_ = d.close()
			return nil, fmt.Errorf(
				"vibedb: SQL catalog table %q durable definition: %w",
				name, optionsErr)
		}
		dataPath := d.tablePath(name)
		file, openErr := os.OpenFile(dataPath, os.O_RDWR, 0)
		if os.IsNotExist(openErr) {
			if meta.Materialized {
				_ = d.close()
				return nil, fmt.Errorf(
					"vibedb: materialized SQL table %q is missing its data file",
					name,
				)
			}
			d.tables[name] = t
			continue
		}
		if openErr != nil {
			_ = d.close()
			return nil, openErr
		}
		openOptions := durableOptions(t)
		// The durable page catalog is the atomic authority for online indexes.
		// Passing nil lets Open rehydrate its complete definition after a crash
		// between durable publication and the SQL catalog mirror.
		openOptions.Indexes = nil
		collection, openErr := durable.Open(file, openOptions)
		if openErr != nil {
			_ = file.Close()
			_ = d.close()
			return nil, fmt.Errorf("vibedb: open table %q: %w", name, openErr)
		}
		t.file, t.collection = file, collection
		changed, syncErr := syncTableIndexMeta(t)
		if syncErr != nil {
			_ = d.close()
			return nil, fmt.Errorf(
				"vibedb: read durable table %q index catalog: %w",
				name, syncErr,
			)
		}
		if changed {
			d.catalogWritePending = true
		}
		if !meta.Materialized {
			// Catalog v2 predates the explicit materialization bit. Presence
			// of a valid durable file is authoritative and can be upgraded
			// safely before this handle is exposed.
			meta.Materialized = true
			d.catalogWritePending = true
		}
		d.tables[name] = t
	}
	if d.catalogWritePending {
		if _, err := d.persistCatalogLocked(); err != nil {
			return nil, fmt.Errorf(
				"vibedb: persist recovered SQL table identity: %w", err,
			)
		}
	}
	opened = true
	return d, nil
}

// recoverVisibleNamespace re-establishes every namespace fence that a prior
// process may have lost after publishing a rename but before a terminal close.
// Pending-fence flags are necessarily in-memory, so a reopen cannot know which
// one failed. Fencing both possible directories is the bounded recovery record:
// the catalog parent first, then the table directory before any visible table
// file is adopted or a legacy Materialized bit is upgraded.
func (d *database) recoverVisibleNamespace() error {
	parent := filepath.Dir(d.path)
	if err := d.directorySync(parent); err != nil {
		return fmt.Errorf(
			"%w: recover SQL catalog namespace fence: %w",
			durable.ErrCommitOutcomeUnknown, err,
		)
	}
	info, err := os.Stat(d.dataDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("vibedb: inspect SQL table directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf(
			"vibedb: SQL table path %s is not a directory",
			d.dataDir,
		)
	}
	if err := d.directorySync(d.dataDir); err != nil {
		return fmt.Errorf(
			"%w: recover SQL table namespace fence: %w",
			durable.ErrCommitOutcomeUnknown, err,
		)
	}
	return nil
}

func readCatalogFile(path string) ([]byte, bool, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, false, err
	}
	if info.Size() > maxCatalogBytes {
		_ = file.Close()
		return nil, false, fmt.Errorf(
			"%w: %s is %d bytes, maximum is %d",
			ErrCatalogTooLarge, path, info.Size(), maxCatalogBytes,
		)
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maxCatalogBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, false, readErr
	}
	if closeErr != nil {
		return nil, false, closeErr
	}
	if len(raw) > maxCatalogBytes {
		return nil, false, fmt.Errorf(
			"%w: %s grew beyond %d bytes while it was read",
			ErrCatalogTooLarge, path, maxCatalogBytes,
		)
	}
	return raw, true, nil
}

// canonicalCatalogPath gives the catalog, writer lock, and table directory one
// filesystem identity. An existing catalog is resolved through every symlink;
// a new catalog resolves its already-existing parent before the three sibling
// paths are derived. Silently creating missing ancestors would make CREATE
// TABLE acknowledge data whose ancestor directory entries were never fenced,
// and a lexical path through a symlink would permit a second lock beside the
// same real catalog.
func canonicalCatalogPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if _, err := os.Lstat(absolute); err == nil {
		resolved, resolveErr := filepath.EvalSymlinks(absolute)
		if resolveErr != nil {
			return "", fmt.Errorf(
				"vibedb: resolve SQL catalog path %s: %w",
				absolute, resolveErr,
			)
		}
		return resolved, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	parent := filepath.Dir(absolute)
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", fmt.Errorf(
			"vibedb: SQL catalog parent directory must already exist: %w",
			err,
		)
	}
	info, err := os.Stat(resolvedParent)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf(
			"vibedb: SQL catalog parent %s is not a directory",
			resolvedParent,
		)
	}
	return filepath.Join(resolvedParent, filepath.Base(absolute)), nil
}

func durableOptions(t *table) durable.Options {
	options := durable.Options{
		Collection:     store.Options{Schema: t.schema},
		DocumentFormat: durable.DocumentFormatVerbatim,
	}
	for _, index := range t.meta.Indexes {
		options.Indexes = append(options.Indexes, store.IndexDefinition{
			Name: index.Name, Paths: append([]string(nil), index.Paths...),
		})
	}
	return options
}

func syncTableIndexMeta(t *table) (bool, error) {
	if t == nil || t.collection == nil {
		return false, nil
	}
	snapshot, err := t.collection.Snapshot()
	if err != nil {
		return false, err
	}
	infos := snapshot.AppendIndexes(nil)
	if err := snapshot.Close(); err != nil {
		return false, err
	}
	next := make([]indexMeta, len(infos))
	for i := range infos {
		next[i].Name = infos[i].Name
		next[i].Paths = append(
			[]string(nil), infos[i].Columns[:infos[i].ColumnCount]...,
		)
	}
	if len(next) == len(t.meta.Indexes) {
		equal := true
		for i := range next {
			if next[i].Name != t.meta.Indexes[i].Name ||
				!slices.Equal(next[i].Paths, t.meta.Indexes[i].Paths) {
				equal = false
				break
			}
		}
		if equal {
			return false, nil
		}
	}
	t.meta.Indexes = next
	return true, nil
}

func compileSchemaMeta(meta *schemaMeta) (*store.Schema, error) {
	if meta == nil {
		return nil, nil
	}
	definition := store.SchemaDefinition{
		Root:   store.SchemaType(meta.Root),
		Fields: make([]store.SchemaField, len(meta.Fields)),
	}
	for i, field := range meta.Fields {
		definition.Fields[i] = store.SchemaField{
			Path: field.Path, Types: store.SchemaType(field.Types),
			Required: field.Required,
		}
	}
	return store.CompileSchema(definition)
}

func schemaMetaFrom(schema *store.Schema) *schemaMeta {
	if schema == nil {
		return nil
	}
	definition := schema.Definition()
	meta := &schemaMeta{
		Root:   uint16(definition.Root),
		Fields: make([]schemaFieldMeta, len(definition.Fields)),
	}
	for i, field := range definition.Fields {
		meta.Fields[i] = schemaFieldMeta{
			Path: field.Path, Types: uint16(field.Types),
			Required: field.Required,
		}
	}
	return meta
}

func validatePrimarySchema(primaryKey string, schema *store.Schema) error {
	if schema == nil {
		return errors.New("the persisted schema is absent")
	}
	definition := schema.Definition()
	if definition.Root != store.SchemaObject {
		return fmt.Errorf("schema root is %s, want object", definition.Root)
	}
	const scalarTypes = store.SchemaBool | store.SchemaNumber |
		store.SchemaInteger | store.SchemaString
	for _, field := range definition.Fields {
		if field.Path != primaryKey {
			continue
		}
		if !field.Required {
			return fmt.Errorf("schema path %q is not required", primaryKey)
		}
		if field.Types == 0 || field.Types&^scalarTypes != 0 {
			return fmt.Errorf(
				"schema path %q has non-scalar type set %s",
				primaryKey, field.Types)
		}
		return nil
	}
	return fmt.Errorf("schema does not constrain primary-key path %q", primaryKey)
}

func (d *database) tablePath(name string) string {
	sum := sha256.Sum256([]byte(name))
	return filepath.Join(d.dataDir, hex.EncodeToString(sum[:])+".vjc")
}

func validateCatalogTableName(name string) error {
	switch {
	case name == "":
		return errors.New("table name is empty")
	case !utf8.ValidString(name):
		return errors.New("table name must be valid UTF-8")
	case len(name) > maxCatalogTableNameBytes:
		return fmt.Errorf(
			"table name is %d bytes, maximum is %d",
			len(name), maxCatalogTableNameBytes)
	default:
		return nil
	}
}

func (d *database) directorySync(path string) error {
	if d.syncDir != nil {
		return d.syncDir(path)
	}
	return syncDirectory(path)
}

func (d *database) collectionClose(collection *durable.Collection) error {
	if d.closeCollection != nil {
		return d.closeCollection(collection)
	}
	return collection.Close()
}

// retryNamespaceFencesLocked closes the crash-consistency dependency chain
// after a published namespace mutation whose directory sync failed. No later
// mutation may acknowledge success while either fence is pending: a durable
// table file is useless if its table-directory entry can disappear, and that
// directory is useless if the catalog parent can recover without it.
func (d *database) retryNamespaceFencesLocked() error {
	if d.catalogFencePending {
		if err := d.directorySync(filepath.Dir(d.path)); err != nil {
			return fmt.Errorf(
				"%w: retry SQL catalog namespace fence: %w",
				durable.ErrCommitOutcomeUnknown, err,
			)
		}
		d.catalogFencePending = false
	}
	if d.tableDirFencePending {
		if err := d.directorySync(d.dataDir); err != nil {
			return fmt.Errorf(
				"%w: retry SQL table namespace fence: %w",
				durable.ErrCommitOutcomeUnknown, err,
			)
		}
		d.tableDirFencePending = false
	}
	return nil
}

// settleCatalogLocked completes any full-catalog rewrite that became required
// only after a table file was already committed. This ordering prevents a
// durable catalog from claiming a data file that was never published, while
// ensuring later acknowledged writes cannot depend on an unrecorded file.
func (d *database) settleCatalogLocked() error {
	if err := d.retryNamespaceFencesLocked(); err != nil {
		return err
	}
	if !d.catalogWritePending {
		return nil
	}
	published, err := d.persistCatalogLocked()
	if err == nil {
		return nil
	}
	if published || errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		return err
	}
	return fmt.Errorf(
		"%w: persist committed SQL table identity: %w",
		durable.ErrCommitOutcomeUnknown, err,
	)
}

// persistCatalogLocked returns published once the rename has made the new
// catalog visible. A later directory-fence failure has an unknown crash
// outcome, so callers must retain the matching in-memory definition.
func (d *database) persistCatalogLocked() (published bool, err error) {
	if err := d.retryNamespaceFencesLocked(); err != nil {
		return false, err
	}
	if err := checkCatalogSize(d.catalog); err != nil {
		return false, err
	}
	raw, err := json.MarshalIndent(d.catalog, "", "  ")
	if err != nil {
		return false, err
	}
	if len(raw) > maxCatalogBytes {
		// checkCatalogSize is deliberately conservative, so reaching this
		// branch means its structural allowance needs to be strengthened.
		return false, fmt.Errorf(
			"%w: encoded catalog is %d bytes, maximum is %d",
			ErrCatalogTooLarge, len(raw), maxCatalogBytes,
		)
	}
	tmp, err := os.CreateTemp(filepath.Dir(d.path), "."+filepath.Base(d.path)+".tmp-*")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return false, err
	}
	if _, err := tmp.Write(raw); err != nil {
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	if err := replaceCatalogFile(tmpName, d.path); err != nil {
		return false, err
	}
	ok = true
	d.catalogWritePending = false
	d.catalogFencePending = true
	if err := d.directorySync(filepath.Dir(d.path)); err != nil {
		return true, fmt.Errorf(
			"%w: sync SQL catalog directory: %w",
			durable.ErrCommitOutcomeUnknown, err,
		)
	}
	d.catalogFencePending = false
	return true, nil
}

// checkCatalogSize computes a conservative upper bound for MarshalIndent
// without allocating. The fixed allowances cover field names, punctuation,
// booleans/numbers, newlines, and indentation at each structural level;
// encodedJSONStringBytes accounts exactly for encoding/json's string escaping.
func checkCatalogSize(catalog catalogFile) error {
	_, err := catalogSizeUpperBound(catalog)
	return err
}

func catalogSizeUpperBound(catalog catalogFile) (int, error) {
	if err := checkCatalogTableCount(len(catalog.Tables)); err != nil {
		return maxCatalogBytes + 1, err
	}
	size := 256
	add := func(part int) bool {
		if part < 0 || part > maxCatalogBytes-size {
			size = maxCatalogBytes + 1
			return false
		}
		size += part
		return true
	}
	for name, meta := range catalog.Tables {
		if !add(encodedJSONStringBytes(name) + 512) {
			return size, catalogSizeError(size)
		}
		if meta == nil {
			continue
		}
		if !add(encodedJSONStringBytes(meta.PrimaryKey)) {
			return size, catalogSizeError(size)
		}
		if meta.Schema != nil {
			if !add(256) {
				return size, catalogSizeError(size)
			}
			for i := range meta.Schema.Fields {
				if !add(encodedJSONStringBytes(meta.Schema.Fields[i].Path) + 256) {
					return size, catalogSizeError(size)
				}
			}
		}
		for i := range meta.Indexes {
			index := &meta.Indexes[i]
			if !add(encodedJSONStringBytes(index.Name) + 256) {
				return size, catalogSizeError(size)
			}
			for _, path := range index.Paths {
				if !add(encodedJSONStringBytes(path) + 64) {
					return size, catalogSizeError(size)
				}
			}
		}
	}
	return size, nil
}

func checkCatalogTableCount(tables int) error {
	if tables <= maxCatalogTables {
		return nil
	}
	return fmt.Errorf(
		"%w: catalog has %d tables, maximum is %d",
		ErrTooManyTables, tables, maxCatalogTables,
	)
}

func catalogSizeError(estimated int) error {
	return fmt.Errorf(
		"%w: prospective encoded upper bound exceeds %d bytes (at least %d)",
		ErrCatalogTooLarge, maxCatalogBytes, estimated,
	)
}

func encodedJSONStringBytes(value string) int {
	size := 2 // surrounding quotes
	for len(value) != 0 {
		r, width := utf8.DecodeRuneInString(value)
		switch {
		case r == utf8.RuneError && width == 1:
			size += 6 // encoding/json emits \ufffd for invalid UTF-8
		case r == '"' || r == '\\':
			size += 2
		case r == '\b' || r == '\f' || r == '\n' || r == '\r' || r == '\t':
			size += 2
		case r < 0x20 || r == '<' || r == '>' || r == '&' ||
			r == '\u2028' || r == '\u2029':
			size += 6
		default:
			size += width
		}
		value = value[width:]
	}
	return size
}

type seedDocument struct {
	key      string
	document []byte
}

func (d *database) ensureDataDir() error {
	if err := d.settleCatalogLocked(); err != nil {
		return err
	}
	info, err := os.Stat(d.dataDir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf(
				"vibedb: SQL table path %s is not a directory",
				d.dataDir,
			)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(d.dataDir)
	tmp, err := os.MkdirTemp(parent, "."+filepath.Base(d.dataDir)+".tmp-")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			_ = os.Remove(tmp)
		}
	}()
	if err := os.Chmod(tmp, 0o700); err != nil {
		return err
	}
	if err := publishNewPath(tmp, d.dataDir); err != nil {
		return fmt.Errorf("vibedb: publish SQL table directory: %w", err)
	}
	published = true
	d.catalogFencePending = true
	if err := d.directorySync(parent); err != nil {
		return fmt.Errorf(
			"%w: sync SQL table parent directory: %w",
			durable.ErrCommitOutcomeUnknown, err,
		)
	}
	d.catalogFencePending = false
	return nil
}

func (d *database) materializeLocked(name string, documents []seedDocument) (*table, error) {
	if err := d.settleCatalogLocked(); err != nil {
		return nil, err
	}
	t, ok := d.tables[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, name)
	}
	if t.collection != nil {
		return t, nil
	}
	if err := d.ensureDataDir(); err != nil {
		return nil, err
	}
	path := d.tablePath(name)
	file, err := createPublishableTableTemp(
		d.dataDir, "."+filepath.Base(path)+".tmp-",
	)
	if err != nil {
		return nil, err
	}
	tmpPath := file.Name()
	options := durableOptions(t)
	collection, err := durable.Create(file, options)
	if err == nil {
		switch {
		case len(documents) == 0:
		case len(documents) == 1:
			_, err = collection.Put([]byte(documents[0].key), documents[0].document)
		case collection.SupportsUpdate():
			err = collection.Update(func(batch *durable.WriteBatch) error {
				for _, document := range documents {
					if putErr := batch.Put([]byte(document.key), document.document); putErr != nil {
						return putErr
					}
				}
				return nil
			})
		default:
			// An indexed ordered-primary collection cannot batch, but its single-
			// document Put maintains the exact indexes, and this create discards
			// the whole file on any error below, so seeding a fresh indexed table
			// through a sequence of Puts is atomic at the file boundary — the only
			// place a first INSERT of several rows can land on an indexed table.
			for _, document := range documents {
				if _, putErr := collection.Put([]byte(document.key), document.document); putErr != nil {
					err = putErr
					break
				}
			}
		}
	}
	if err != nil {
		var cleanupErr error
		if collection != nil {
			cleanupErr = errors.Join(cleanupErr, collection.Close())
		}
		cleanupErr = errors.Join(cleanupErr, file.Close())
		removeErr := os.Remove(tmpPath)
		if removeErr != nil && !os.IsNotExist(removeErr) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf(
				"remove failed temporary SQL table file: %w",
				removeErr,
			))
		} else if syncErr := d.directorySync(d.dataDir); syncErr != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf(
				"sync failed temporary SQL table cleanup: %w",
				syncErr,
			))
		}
		return nil, errors.Join(err, cleanupErr)
	}
	if err := publishNewPath(tmpPath, path); err != nil {
		// A platform rename error should mean the final name was not
		// published. Check anyway: if the namespace says otherwise, retain the
		// live handle and report an unknown outcome rather than make a retry
		// collide with an acknowledged-looking table file.
		if _, statErr := os.Stat(path); statErr == nil {
			t.file, t.collection = file, collection
			d.tableDirFencePending = true
			t.meta.Materialized = true
			d.catalogWritePending = true
			return t, fmt.Errorf(
				"%w: publish first SQL table file: %w",
				durable.ErrCommitOutcomeUnknown, err,
			)
		}
		cleanupErr := errors.Join(collection.Close(), file.Close())
		if removeErr := os.Remove(tmpPath); removeErr != nil &&
			!os.IsNotExist(removeErr) {
			cleanupErr = errors.Join(cleanupErr, removeErr)
		}
		return nil, errors.Join(err, cleanupErr)
	}
	t.file, t.collection = file, collection
	d.tableDirFencePending = true
	t.meta.Materialized = true
	d.catalogWritePending = true
	// A synchronous (the driver's default) or journal-opted collection writes a
	// recovery-journal sibling beside the store file, and reopen resolves it by
	// the store file's final path, so it must move with the rename above.
	// Ordering is not crash-critical: the table becomes durably known only when
	// the catalog write below records it, and a crash before that leaves both
	// files as harmless orphans no reopen consults. The live handle is retained
	// on failure exactly as the directory-sync failure below does.
	if err := publishJournalSibling(tmpPath, path); err != nil {
		return t, fmt.Errorf(
			"%w: publish first SQL table journal: %w",
			durable.ErrCommitOutcomeUnknown, err,
		)
	}
	if err := d.directorySync(d.dataDir); err != nil {
		// The durable file and its live namespace entry now exist. Retain the
		// matching live table state: removing it would turn a failed fence into
		// another unfenced namespace mutation and could make a retry duplicate
		// an already committed first write.
		return t, fmt.Errorf(
			"%w: sync SQL table directory: %w",
			durable.ErrCommitOutcomeUnknown, err,
		)
	}
	d.tableDirFencePending = false
	if err := d.settleCatalogLocked(); err != nil {
		return t, err
	}
	return t, nil
}

// publishJournalSibling relocates a store file's recovery-journal sibling when
// the store file is published from fromStore to toStore. An async collection
// writes no journal, so an absent source is a no-op rather than an error; a
// present one is moved with the same platform-atomic rename the store file uses,
// because a journaled root cannot reopen without its journal at the store file's
// path.
func publishJournalSibling(fromStore, toStore string) error {
	from := durable.RecoveryJournalPath(fromStore)
	if _, err := os.Stat(from); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return publishNewPath(from, durable.RecoveryJournalPath(toStore))
}

// close is the retryable close used by direct database owners. A pending
// namespace fence or a collection with an outstanding lease leaves the
// database intact so the caller can clear the condition and retry.
func (d *database) close() error {
	return d.closeWithPolicy(false)
}

// closeTerminal is the one-shot close used after a database/sql connector has
// lost its final connection reference, and while unwinding a failed open.
// database/sql never retries Connector.Close, so retaining the catalog writer
// lease after reporting a durability or descriptor error would permanently
// prevent this process from reopening the DSN.
//
// The connector reference invariant proves there are no row or transaction
// snapshots at this point: conn.Close rejects open rows and rolls back an open
// transaction before releasing its reference. SQL collections use synchronous
// durability, so Collection.Close either releases its internal resources or
// reports an invariant/resource error; the caller-owned table descriptor is
// then closed here as the final ownership boundary.
func (d *database) closeTerminal() error {
	return d.closeWithPolicy(true)
}

func (d *database) closeWithPolicy(terminal bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closeDone {
		return nil
	}
	var result error
	if err := d.settleCatalogLocked(); err != nil {
		if !terminal {
			return err
		}
		result = errors.Join(result, err)
	}
	d.closed = true
	for _, t := range d.tables {
		if t.collection != nil {
			result = errors.Join(result, d.collectionClose(t.collection))
		}
	}
	if result != nil && !terminal {
		// Collection.Close is retryable after outstanding leases or transient
		// checkpoint failures clear. Keep every caller-owned descriptor and the
		// catalog writer lease alive so a later close can finish coherently.
		return result
	}
	for _, t := range d.tables {
		if t.file != nil {
			result = errors.Join(result, t.file.Close())
			t.file = nil
		}
		if terminal {
			// No handle can be exposed after a terminal close, even when its
			// final Close returned an error.
			t.collection = nil
		}
	}
	if d.lockFile != nil {
		result = errors.Join(result, storeio.UnlockWriter(d.lockFile))
		result = errors.Join(result, d.lockFile.Close())
		d.lockFile = nil
	}
	if terminal || result == nil {
		d.closeDone = true
	}
	return result
}
