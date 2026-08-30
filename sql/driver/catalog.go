package driver

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

const catalogVersion = 0

const maxCatalogTableNameBytes = 1<<16 - 1

const (
	maxCatalogViews            = 256
	maxCatalogViewQueryBytes   = 1 << 20
	maxCatalogViewColumns      = 1024
	maxCatalogViewDependencies = 1024
)

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
// allocation-free upper bound before allocating the canonical image.
const maxCatalogBytes = 16 << 20

type catalogFile struct {
	Version              int                           `json:"version"`
	Tables               map[string]*tableMeta         `json:"tables"`
	Views                map[string]*viewMeta          `json:"views,omitempty"`
	ShardStore           *ShardStoreIdentity           `json:"shard_store,omitempty"`
	ShardStoreFence      *ShardStoreFence              `json:"shard_store_fence,omitempty"`
	ReplicatedShardStore *ReplicatedShardStoreIdentity `json:"replicated_shard_store,omitempty"`
	ReplicatedChildApply *replicatedApplyMeta          `json:"replicated_child_apply,omitempty"`
	ReplicatedApply      *replicatedApplyMeta          `json:"replicated_apply,omitempty"`
}

// viewMeta is immutable after publication. Prepared statements retain its
// pointer as an in-process definition generation: DROP/recreate installs a new
// object, so an old plan can never silently execute against a replacement.
type viewMeta struct {
	Query             string   `json:"query"`
	Columns           []string `json:"columns,omitempty"`
	Outputs           []string `json:"outputs"`
	ViewDependencies  []string `json:"view_dependencies,omitempty"`
	TableDependencies []string `json:"table_dependencies,omitempty"`
}

type tableMeta struct {
	PrimaryKey                 string      `json:"primary_key"`
	Schema                     *schemaMeta `json:"schema,omitempty"`
	Indexes                    []indexMeta `json:"indexes,omitempty"`
	Storage                    string      `json:"storage,omitempty"`
	Materialized               bool        `json:"materialized,omitempty"`
	SealedRecoveryJournalBytes uint64      `json:"sealed_recovery_journal_bytes,omitempty"`
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
	Name   string   `json:"name"`
	Paths  []string `json:"paths"`
	Unique bool     `json:"unique,omitempty"`
}

type table struct {
	meta       *tableMeta
	schema     *store.Schema
	primary    vibejson.CompiledPointer
	file       *os.File
	collection *durable.Collection
	conflicts  txConflictClock
}

// transactionTableLayout is the immutable part of one table incarnation that
// a transaction used to copy eagerly at BEGIN. Keeping it in the shared layout
// epoch lets Read Committed materialize only statement dependencies while
// retaining the exact primary-key, schema, admission, and incarnation identity
// that existed when the transaction began.
type transactionTableLayout struct {
	incarnation   *table
	primaryKey    string
	primary       vibejson.CompiledPointer
	schema        *store.Schema
	uniqueIndexes []indexMeta
	limits        durable.Options
	limitsErr     error
}

// catalogLayoutEpoch is an immutable in-process catalog/layout generation.
// Transactions retain one pointer instead of copying every table and view at
// BEGIN. DDL is the cold copy-on-publish boundary: table layouts and the view
// generation map are rebuilt once and then never mutated. Pointer identity has
// no counter to wrap and no wall-clock dependency.
type catalogLayoutEpoch struct {
	generation byte
	tables     map[string]transactionTableLayout
	views      map[string]*viewMeta
}

func newCatalogLayoutEpoch(
	tables map[string]*table,
	views map[string]*viewMeta,
) *catalogLayoutEpoch {
	epoch := &catalogLayoutEpoch{generation: 1}
	if len(tables) != 0 {
		epoch.tables = make(map[string]transactionTableLayout, len(tables))
		for name, table := range tables {
			limits, err := tableMutationLimits(table)
			epoch.tables[name] = transactionTableLayout{
				incarnation:   table,
				primaryKey:    table.meta.PrimaryKey,
				primary:       table.primary,
				schema:        table.schema,
				uniqueIndexes: cloneUniqueIndexMeta(table.meta.Indexes),
				limits:        limits,
				limitsErr:     err,
			}
		}
	}
	if len(views) != 0 {
		epoch.views = make(map[string]*viewMeta, len(views))
		for name, meta := range views {
			epoch.views[name] = meta
		}
	}
	return epoch
}

func cloneUniqueIndexMeta(indexes []indexMeta) []indexMeta {
	var unique []indexMeta
	for i := range indexes {
		if indexes[i].Unique {
			unique = append(unique, indexMeta{
				Name:   indexes[i].Name,
				Paths:  slices.Clone(indexes[i].Paths),
				Unique: true,
			})
		}
	}
	return unique
}

// retiredTable keeps the physical resources or unresolved namespace paths of a
// dropped, replaced, or unpublished table incarnation until teardown and its
// directory fence complete. Existing SQL cursors and transactions may still
// hold generation leases after the catalog stops naming this storage identity.
type retiredTable struct {
	name         string
	path         string
	journal      string
	extraPath    string
	extraJournal string
	file         *os.File
	collection   *durable.Collection
	removed      bool
}

type database struct {
	mu               sync.RWMutex
	path             string
	dataDir          string
	lockFile         *os.File
	syncDir          func(string) error
	closeCollection  func(*durable.Collection) error
	adoptCollection  func(*durable.Collection) error
	detachCollection func(*durable.Collection) error
	catalog          catalogFile
	tables           map[string]*table
	retired          []retiredTable
	// txnReattach contains authoritative catalog collections that were
	// successfully detached for a tentative DDL cut but could not be re-adopted
	// after that cut definitely failed. The collection remains owned by tables;
	// this list is only a fail-closed registration barrier. Every later catalog
	// or data mutation retries it before doing work.
	txnReattach          []*durable.Collection
	catalogWritePending  bool
	catalogFencePending  bool
	tableDirFencePending bool
	closed               bool
	closeDone            bool
	// txnLog is the caller-owned decision log for multi-table commits. Initial
	// recovery opens the complete hidden+user collection set as one private
	// phase before this handle becomes reachable.
	txnLog *durable.TxnLog
	// checkpointGroup exclusively owns the hidden replicated-state collection,
	// its sole user collection, and txnLog after replicated apply activation.
	// It is recovered before any collection becomes reachable and gracefully
	// checkpointed before collection/TxnLog close.
	checkpointGroup *durable.CheckpointGroup
	// replicatedSeedRecovery authorizes only the exact child-stage reopen path
	// to inspect a missing certificate beside a clean staged user image.
	// replicatedSeedPending keeps that image non-serving until a sealed stage
	// proves the seed and installs its exact immutable snapshot base through the
	// group-owned same-index transition.
	replicatedSeedRecovery bool
	replicatedSeedPending  bool
	// Snapshot recovery can include imported hidden rows before certification,
	// but must reject ordinary initialized checkpoints before replay begins.
	replicatedSnapshotRecovery bool
	// txnLimits is the driver's normalized cross-table commit bound, matching
	// durable's package defaults. UpdateCollections is fail-closed at zero.
	txnLimits durable.TxnLimits
	// cluster is the optional local-cluster routing state attached by the
	// OpenCluster facade. It is nil for the default single-store driver, whose
	// write path is unchanged.
	cluster *clusterRouting
	// layoutEpoch changes whenever a retained table, view, or index layout
	// publication can affect prepared dependency identity. It is protected by
	// mu and deliberately independent of durable data generations.
	layoutEpoch *catalogLayoutEpoch
	// servingClaim is an in-process exclusion guard for one shard service over
	// this writer-owning catalog. Its coordinates are also checked against the
	// durable ShardStoreFence before the pointer is installed.
	servingClaim *ShardStoreServingClaim
	// replicatedApplyCollection is the catalog-owned, non-SQL-visible system
	// participant paired atomically with the sole replicated user table. It is
	// opened through txnDecisions and closed before txnLog, exactly like catalog
	// tables, but never enters the SQL namespace or layout epoch.
	replicatedApplyFile         *os.File
	replicatedApplyCollection   *durable.Collection
	replicatedCaptureFile       *os.File
	replicatedCaptureCollection *durable.Collection
	replicatedApplyClaim        *ReplicatedApply
	// replicatedChildStageClaim exclusively owns the sole user collection
	// while a certified split child is received and converted in place into
	// replicated apply. It is never a SQL or serving capability.
	replicatedChildStageClaim *ReplicatedChildStage
	// replicatedSnapshotStageClaim exclusively owns the hidden participant and
	// sole user relation while a certified RF learner snapshot is materialized.
	// It grants neither SQL sessions nor serving authority.
	replicatedSnapshotStageClaim *ReplicatedSnapshotStage
	// replicatedRestoreStageClaim exclusively owns fresh relation files while
	// an authenticated source artifact is imported without source authority.
	replicatedRestoreStageClaim *ReplicatedRestoreStage
	// schemaTransition is populated only by the explicit post-Raft catalog-CAS
	// recovery opener. It authorizes target checkpoint membership selection and
	// exact target-machine replay before any serving claim can be minted.
	schemaTransition          []byte
	schemaMembershipSelected  bool
	schemaMembership          durable.CheckpointMembershipWitness
	schemaCheckpointAuthority [32]byte
	schemaAuthorization       [32]byte
	schemaCatalogCAS          [32]byte
	schemaSourceRecovery      *replicatedstate.SchemaSourceRecoveryProof
	// distributedTxnCollection is the raw-ID keyed, SQL-invisible participant
	// state joined atomically with user-table publication. The larger staged
	// mutation remains in the append-only transaction journal.
	distributedTxnFile       *os.File
	distributedTxnCollection *durable.Collection
}

func (d *database) advanceLayoutEpochLocked() {
	d.layoutEpoch = newCatalogLayoutEpoch(d.tables, d.catalog.Views)
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
	return openDatabaseWithShardStorePolicy(path, syncDir, shardStoreOpenPolicy{
		mode: shardStoreOpenGeneric,
	})
}

func openDatabaseWithShardStorePolicy(
	path string,
	syncDir func(string) error,
	shardPolicy shardStoreOpenPolicy,
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
	if err := shardPolicy.openOptions.lockWriter(lockFile); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("vibedb: lock SQL catalog %s: %w", absolute, err)
	}
	d := &database{
		path: absolute, dataDir: absolute + ".tables", lockFile: lockFile,
		catalog: catalogFile{Version: catalogVersion, Tables: make(map[string]*tableMeta)},
		tables:  make(map[string]*table), syncDir: syncDir,
		layoutEpoch:                newCatalogLayoutEpoch(nil, nil),
		txnLimits:                  defaultDriverTxnLimits(),
		replicatedSeedRecovery:     shardPolicy.mode == shardStoreOpenReplicatedChildStageResume || shardPolicy.mode == shardStoreOpenReplicatedSnapshotTarget,
		replicatedSeedPending:      shardPolicy.mode == shardStoreOpenReplicatedChildStageResume || shardPolicy.mode == shardStoreOpenReplicatedSnapshotTarget,
		replicatedSnapshotRecovery: shardPolicy.mode == shardStoreOpenReplicatedSnapshotTarget,
		schemaTransition:           bytes.Clone(shardPolicy.schemaTransition),
		schemaMembershipSelected:   shardPolicy.schemaMembershipSelected,
		schemaMembership:           shardPolicy.schemaMembership,
		schemaCheckpointAuthority:  shardPolicy.schemaCheckpointAuthority,
		schemaAuthorization:        shardPolicy.schemaAuthorization,
		schemaCatalogCAS:           shardPolicy.schemaCatalogCAS,
		schemaSourceRecovery:       shardPolicy.schemaSourceRecovery,
	}
	d.catalog.Views = make(map[string]*viewMeta)
	opened := false
	defer func() {
		if !opened {
			// A failed open has no caller that can retry cleanup. Completed
			// teardown releases the catalog writer immediately; a retryable
			// collection phase is transferred to the process retirement registry
			// so this stack frame never becomes its last owner.
			_ = d.closeTerminal()
			if !d.closeCompleted() {
				retainTerminalDatabase(d)
			}
		}
	}()
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
		if err := decodeCatalogJSON(raw, (*catalogFileVibe)(&d.catalog)); err != nil {
			return nil, fmt.Errorf("vibedb: read SQL catalog %s: %w", absolute, err)
		}
		if d.catalog.Version != catalogVersion || d.catalog.Tables == nil {
			return nil, fmt.Errorf("vibedb: unsupported SQL catalog version %d", d.catalog.Version)
		}
		if d.catalog.Views == nil {
			d.catalog.Views = make(map[string]*viewMeta)
		}
	}
	var initializeIdentity *ShardStoreIdentity
	switch shardPolicy.mode {
	case shardStoreOpenGeneric:
		if d.catalog.ShardStore != nil {
			return nil, &ShardStoreError{
				Op: "open generic catalog", Path: absolute,
				Actual: *d.catalog.ShardStore,
				Err:    ErrShardStoreIdentityMismatch,
			}
		}
	case shardStoreOpenInitialize:
		if shardPolicy.expectedIdentity != (ShardStoreIdentity{}) &&
			(shardPolicy.expectedIdentity.Binding() != shardPolicy.expected ||
				validateShardStoreIdentity(shardPolicy.expectedIdentity) != nil) {
			return nil, ErrShardStoreIdentityMismatch
		}
		if d.catalog.ReplicatedShardStore != nil {
			return nil, fmt.Errorf(
				"vibedb: initialize local shard store %s: %w",
				absolute, ErrDirectWriteFenced,
			)
		}
		if d.catalog.ShardStore != nil &&
			(d.catalog.ShardStore.Binding() != shardPolicy.expected ||
				shardPolicy.expectedIdentity != (ShardStoreIdentity{}) &&
					*d.catalog.ShardStore != shardPolicy.expectedIdentity) {
			return nil, &ShardStoreError{
				Op: "initialize", Path: absolute, Expected: shardPolicy.expected,
				Actual: *d.catalog.ShardStore,
				Err:    ErrShardStoreIdentityMismatch,
			}
		}
		if d.catalog.ShardStore == nil {
			if exists {
				return nil, &ShardStoreError{
					Op: "initialize existing unbound catalog", Path: absolute,
					Expected: shardPolicy.expected,
					Err:      ErrShardStoreIdentityMismatch,
				}
			}
			if _, statErr := os.Lstat(d.dataDir); statErr == nil {
				return nil, &ShardStoreError{
					Op: "initialize existing unbound storage root", Path: absolute,
					Expected: shardPolicy.expected,
					Err:      ErrShardStoreIdentityMismatch,
				}
			} else if !os.IsNotExist(statErr) {
				return nil, statErr
			}
			identity := shardPolicy.expectedIdentity
			if identity == (ShardStoreIdentity{}) {
				var identityErr error
				identity, identityErr = randomShardStoreIdentity(shardPolicy.expected)
				if identityErr != nil {
					return nil, identityErr
				}
			}
			initializeIdentity = &identity
		}
	case shardStoreOpenExisting:
		if !exists || d.catalog.ShardStore == nil {
			return nil, &ShardStoreError{
				Op: "open", Path: absolute, Expected: shardPolicy.expected,
				Err: ErrShardStoreUnbound,
			}
		}
		if d.catalog.ReplicatedShardStore != nil {
			return nil, fmt.Errorf(
				"vibedb: open local shard store %s: %w",
				absolute, ErrDirectWriteFenced,
			)
		}
		if d.catalog.ShardStore.Binding() != shardPolicy.expected {
			return nil, &ShardStoreError{
				Op: "open", Path: absolute, Expected: shardPolicy.expected,
				Actual: *d.catalog.ShardStore,
				Err:    ErrShardStoreIdentityMismatch,
			}
		}
	case shardStoreOpenReplicatedExisting:
		if !exists || d.catalog.ReplicatedShardStore == nil {
			return nil, fmt.Errorf(
				"%w: %s", ErrReplicatedShardStoreUnbound, absolute,
			)
		}
		if !d.catalog.ReplicatedShardStore.Equal(shardPolicy.expectedReplicated) {
			return nil, fmt.Errorf(
				"%w: %s", ErrReplicatedShardStoreIdentityMismatch, absolute,
			)
		}
		if d.catalog.ReplicatedApply != nil {
			return nil, fmt.Errorf(
				"%w: activated root requires exact replicated apply identity",
				ErrReplicatedApplyMismatch,
			)
		}
	case shardStoreOpenReplicatedSettlement:
		if !exists || d.catalog.ReplicatedShardStore == nil {
			return nil, fmt.Errorf(
				"%w: %s", ErrReplicatedShardStoreUnbound, absolute,
			)
		}
		actual := d.catalog.ReplicatedShardStore
		if actual.Binding != shardPolicy.expectedReplicated.Binding ||
			actual.LogID != shardPolicy.expectedReplicatedLogID ||
			actual.UserTable != shardPolicy.expectedReplicatedUserTable ||
			actual.Sidecars != shardPolicy.expectedReplicated.Sidecars {
			return nil, fmt.Errorf(
				"%w: %s", ErrReplicatedShardStoreIdentityMismatch, absolute,
			)
		}
		if d.catalog.ReplicatedApply != nil {
			return nil, fmt.Errorf(
				"%w: activated root requires replicated apply settlement",
				ErrReplicatedApplyMismatch,
			)
		}
	case shardStoreOpenReplicatedApplyExisting, shardStoreOpenReplicatedSchemaTransition:
		if !exists || d.catalog.ReplicatedShardStore == nil || d.catalog.ReplicatedApply == nil {
			return nil, fmt.Errorf("%w: %s", ErrReplicatedApplyUninitialized, absolute)
		}
		if !d.catalog.ReplicatedShardStore.Equal(shardPolicy.expectedReplicated) ||
			d.catalog.ReplicatedApply.identity() != shardPolicy.expectedReplicatedApply {
			return nil, fmt.Errorf("%w: %s", ErrReplicatedApplyMismatch, absolute)
		}
	case shardStoreOpenReplicatedSnapshotTarget:
		if !exists || d.catalog.ReplicatedShardStore == nil ||
			!d.catalog.ReplicatedShardStore.Equal(shardPolicy.expectedReplicated) {
			return nil, ErrReplicatedShardStoreIdentityMismatch
		}
		retained := d.catalog.ReplicatedChildApply
		if d.catalog.ReplicatedApply != nil {
			retained = d.catalog.ReplicatedApply
		}
		if retained == nil || retained.identity() != shardPolicy.expectedReplicatedApply {
			return nil, ErrReplicatedApplyMismatch
		}
	case shardStoreOpenReplicatedApplySettlement, shardStoreOpenReplicatedChildStageResume:
		if !exists || d.catalog.ReplicatedShardStore == nil || d.catalog.ReplicatedApply == nil {
			return nil, fmt.Errorf("%w: %s", ErrReplicatedApplyUninitialized, absolute)
		}
		if !d.catalog.ReplicatedShardStore.Equal(shardPolicy.expectedReplicated) ||
			!replicatedApplyMetaMatchesOptions(
				d.catalog.ReplicatedApply,
				*d.catalog.ReplicatedShardStore,
				shardPolicy.expectedReplicatedOptions,
			) {
			return nil, fmt.Errorf("%w: %s", ErrReplicatedApplyMismatch, absolute)
		}
	default:
		return nil, errors.New("vibedb: invalid shard store open policy")
	}
	if initializeIdentity != nil {
		// The binding is the first durable record in a new shard root. Publish it
		// before namespace fencing creates the private table directory, before
		// transaction recovery, and before any durable table can be opened or
		// repaired. A published-but-unfenced error is intentionally retryable only
		// through an exact-coordinate InitializeShardStore call.
		d.catalog.ShardStore = initializeIdentity
		d.catalogWritePending = true
		var published bool
		if shardPolicy.persistIdentity != nil {
			published, err = shardPolicy.persistIdentity(d)
		} else {
			published, err = d.persistCatalogLocked()
		}
		if err != nil {
			if !published {
				// The binding is still tentative. Failed-open teardown normally
				// settles committed catalog mirrors; do not let that generic path
				// retry a definitely unpublished initialization after its caller
				// has already received an ordinary failure.
				d.catalog.ShardStore = nil
				d.catalogWritePending = false
			}
			return nil, fmt.Errorf("vibedb: initialize shard store identity: %w", err)
		}
	}

	if err := d.recoverVisibleNamespace(); err != nil {
		return nil, err
	}
	// The transaction recovery path pins an os.Root to the private table
	// directory. On a brand-new SQL catalog that directory is still absent;
	// publish it through the same namespace-fenced helper used by first table
	// materialization before asking durable recovery to open the root.
	if err := d.ensureDataDir(); err != nil {
		return nil, err
	}
	if err := checkCatalogTableCount(len(d.catalog.Tables)); err != nil {
		return nil, err
	}
	if err := checkCatalogViewCount(len(d.catalog.Views)); err != nil {
		return nil, err
	}
	paths := make(map[string]string, len(d.catalog.Tables))
	for name, meta := range d.catalog.Tables {
		if nameErr := validateCatalogTableName(name); nameErr != nil {
			return nil, fmt.Errorf(
				"vibedb: SQL catalog table name %q: %w", name, nameErr)
		}
		if meta == nil {
			return nil, fmt.Errorf("vibedb: SQL catalog table %q has null metadata", name)
		}
		if storageErr := validateStorageIdentity(meta.Storage); storageErr != nil {
			return nil, fmt.Errorf(
				"vibedb: SQL catalog table %q storage identity: %w",
				name, storageErr,
			)
		}
		primary, primaryErr := vibejson.CompilePointer(meta.PrimaryKey)
		if primaryErr != nil || len(primary.Tokens) == 0 {
			if primaryErr == nil {
				primaryErr = errors.New("the primary-key path is empty")
			}
			return nil, fmt.Errorf(
				"vibedb: SQL catalog table %q primary key %q: %w",
				name, meta.PrimaryKey, primaryErr)
		}
		schema, schemaErr := compileSchemaMeta(meta.Schema)
		if schemaErr != nil {
			return nil, fmt.Errorf("vibedb: SQL catalog table %q schema: %w", name, schemaErr)
		}
		if schemaErr := validatePrimarySchema(meta.PrimaryKey, schema); schemaErr != nil {
			return nil, fmt.Errorf(
				"vibedb: SQL catalog table %q primary key: %w",
				name, schemaErr)
		}
		for _, index := range meta.Indexes {
			if _, indexErr := store.CompileExactIndex(store.IndexDefinition{
				Name: index.Name, Paths: index.Paths,
			}); indexErr != nil {
				return nil, fmt.Errorf("vibedb: SQL catalog table %q index %q: %w", name, index.Name, indexErr)
			}
		}
		t := &table{meta: meta, schema: schema, primary: primary}
		if optionsErr := durable.ValidateOptions(durableOptions(t)); optionsErr != nil {
			return nil, fmt.Errorf(
				"vibedb: SQL catalog table %q durable definition: %w",
				name, optionsErr)
		}
		dataPath := d.tablePathForMeta(meta)
		if previous, duplicate := paths[dataPath]; duplicate {
			return nil, fmt.Errorf(
				"vibedb: SQL catalog tables %q and %q share storage identity %q",
				previous, name, filepath.Base(dataPath),
			)
		}
		paths[dataPath] = name
		d.tables[name] = t
	}
	if apply := d.catalog.ReplicatedApply; apply != nil {
		path := d.replicatedApplyPath(apply)
		if previous, duplicate := paths[path]; duplicate {
			return nil, fmt.Errorf(
				"vibedb: SQL catalog storage %q aliases replicated apply storage %q",
				previous, filepath.Base(path),
			)
		}
		paths[path] = "replicated apply"
		capturePath := d.replicatedCapturePath(apply)
		if previous, duplicate := paths[capturePath]; duplicate {
			return nil, fmt.Errorf(
				"vibedb: SQL catalog storage %q aliases replicated capture storage %q",
				previous, filepath.Base(capturePath),
			)
		}
		paths[capturePath] = "replicated capture"
	}
	if reserved := d.catalog.ReplicatedChildApply; reserved != nil {
		if d.catalog.ReplicatedApply != nil {
			return nil, ErrReplicatedApplyMismatch
		}
		if err := validateReplicatedApplyMeta(reserved, d.catalog.ReplicatedShardStore); err != nil {
			return nil, err
		}
		path := d.replicatedApplyPath(reserved)
		if previous, duplicate := paths[path]; duplicate {
			return nil, fmt.Errorf("vibedb: SQL catalog storage %q aliases reserved apply storage %q", previous, filepath.Base(path))
		}
		paths[path] = "reserved replicated apply"
		capturePath := d.replicatedCapturePath(reserved)
		if previous, duplicate := paths[capturePath]; duplicate {
			return nil, fmt.Errorf("vibedb: SQL catalog storage %q aliases reserved capture storage %q", previous, filepath.Base(capturePath))
		}
		paths[capturePath] = "reserved replicated capture"
	}
	for name, meta := range d.catalog.Views {
		if err := validateCatalogViewMeta(name, meta); err != nil {
			return nil, err
		}
		if _, exists := d.tables[name]; exists {
			return nil, fmt.Errorf(
				"vibedb: SQL catalog relation %q is both a table and a view",
				name,
			)
		}
	}
	if err := d.validateViewCatalog(); err != nil {
		return nil, err
	}
	if err := d.openCatalogCollectionsWithTransactionsLocked(shardPolicy.openOptions); err != nil {
		return nil, err
	}
	if err := addReplicatedSchemaStageProtection(d.dataDir, paths); err != nil {
		return nil, fmt.Errorf("vibedb: recover prepared schema target: %w", err)
	}
	// Namespace cleanup follows complete transaction membership validation and
	// recovery. Before that proof, a catalog-omitted path may still be an
	// unretired decision participant and must not be deleted as an orphan.
	if err := d.recoverOrphanedTableStorage(paths); err != nil {
		return nil, err
	}
	if d.catalog.ReplicatedShardStore != nil {
		if err := validateOpenedReplicatedCatalog(d); err != nil {
			return nil, fmt.Errorf("vibedb: open replicated SQL catalog: %w", err)
		}
		// Bind publishes the complete catalog identity before it mints the
		// transaction marker. An exact settlement open is the only reopen path
		// allowed to finish that interrupted bind; every ordinary replicated open
		// must continue to reject an absent marker. EnsureMinted retains the same
		// pinned-directory and complete-catalog proofs as qualification before it
		// creates the marker.
		var markerErr error
		if shardPolicy.mode == shardStoreOpenReplicatedSettlement {
			markerErr = d.txnLog.EnsureMinted()
		} else {
			markerErr = d.txnLog.QualifyMinted()
		}
		if markerErr != nil {
			return nil, fmt.Errorf(
				"vibedb: qualify replicated transaction marker: %w", markerErr,
			)
		}
	}
	if d.catalogWritePending {
		if _, err := d.persistCatalogLocked(); err != nil {
			return nil, fmt.Errorf(
				"vibedb: persist recovered SQL table identity: %w", err,
			)
		}
	}
	// Publish the first immutable in-process layout only after catalog recovery,
	// durable opens, and any index-mirror repair have all completed.
	d.advanceLayoutEpochLocked()
	opened = true
	return d, nil
}

// recoverVisibleNamespace re-establishes every namespace fence that a prior
// process may have lost after publishing a rename but before a terminal close.
// Pending-fence flags are necessarily in-memory, so a reopen cannot know which
// one failed. Fencing both possible directories is the bounded recovery record:
// the catalog parent first, then the table directory before any visible table
// file is opened.
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

type catalogCollectionOpen struct {
	name        string
	table       *table
	file        *os.File
	apply       bool
	capture     bool
	distributed bool
}

func (d *database) openCatalogCollectionsWithTransactionsLocked(openOptions ReplicatedOpenOptions) error {
	requests := make([]durable.TransactionCollectionOpen, 0, len(d.tables)+3)
	opened := make([]catalogCollectionOpen, 0, len(d.tables)+3)
	abortFiles := func(cause error) error {
		for i := range opened {
			cause = errors.Join(cause, opened[i].file.Close())
		}
		return cause
	}
	if path := d.distributedTransactionStatePath(); path != "" {
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err == nil {
			opened = append(opened, catalogCollectionOpen{file: file, distributed: true})
			requests = append(requests, durable.TransactionCollectionOpen{
				File: file, Options: distributedTransactionStateOptions(),
			})
		} else if !os.IsNotExist(err) {
			return abortFiles(err)
		}
	}
	if meta := d.catalog.ReplicatedApply; meta != nil {
		path := d.replicatedApplyPath(meta)
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if os.IsNotExist(err) {
			return fmt.Errorf(
				"%w: hidden collection %s is missing",
				ErrReplicatedApplyMismatch, path,
			)
		}
		if err != nil {
			return err
		}
		opened = append(opened, catalogCollectionOpen{file: file, apply: true})
		requests = append(requests, durable.TransactionCollectionOpen{
			File: file, Options: replicatedApplyDurableOptions(meta.SystemLimits),
		})
		capturePath := d.replicatedCapturePath(meta)
		captureFile, captureErr := os.OpenFile(capturePath, os.O_RDWR, 0)
		if os.IsNotExist(captureErr) {
			return abortFiles(fmt.Errorf(
				"%w: hidden capture collection %s is missing",
				ErrReplicatedApplyMismatch, capturePath,
			))
		}
		if captureErr != nil {
			return abortFiles(captureErr)
		}
		opened = append(opened, catalogCollectionOpen{file: captureFile, capture: true})
		requests = append(requests, durable.TransactionCollectionOpen{
			File: captureFile, Options: replicatedCaptureDurableOptions(meta.CaptureLimits),
		})
	}
	names := make([]string, 0, len(d.tables))
	for name := range d.tables {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		t := d.tables[name]
		path := d.tablePathForMeta(t.meta)
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if os.IsNotExist(err) {
			if t.meta.Materialized {
				return abortFiles(fmt.Errorf(
					"vibedb: materialized SQL table %q is missing its data file",
					name,
				))
			}
			continue
		}
		if err != nil {
			return abortFiles(err)
		}
		options := durableOptions(t)
		// The durable page catalog is authoritative after an interrupted online
		// index publication. Nil rehydrates that complete durable definition.
		options.Indexes = nil
		opened = append(opened, catalogCollectionOpen{
			name: name, table: t, file: file,
		})
		requests = append(requests, durable.TransactionCollectionOpen{
			File: file, Options: options,
		})
	}
	for i := range requests {
		requests[i].Options.OpenWriterLockContext = openOptions.WriterLockContext
		requests[i].Options.OpenWriterLockDeadline = openOptions.WriterLockDeadline
	}
	txnOptions := durable.TxnLogOptions{}
	if replicated := d.catalog.ReplicatedShardStore; replicated != nil {
		txnOptions = durable.TxnLogOptions{
			Capacity:       replicated.Sidecars.TransactionMarkerBytes,
			SealedCapacity: true,
		}
	}
	var collections []*durable.Collection
	var txnLog *durable.TxnLog
	var checkpointGroup *durable.CheckpointGroup
	var err error
	if d.catalog.ReplicatedApply != nil {
		groupNames := make([]string, len(opened))
		for i, entry := range opened {
			switch {
			case entry.apply:
				groupNames[i] = replicatedstate.SystemCollectionName
			case entry.capture:
				groupNames[i] = replicatedstate.TransitionCaptureCollectionName
			case entry.name != "":
				groupNames[i] = entry.name
			default:
				return abortFiles(fmt.Errorf(
					"%w: unsupported checkpoint-group participant",
					ErrReplicatedApplyMismatch,
				))
			}
		}
		if len(d.schemaTransition) != 0 && !d.schemaMembershipSelected {
			collections, txnLog, checkpointGroup, err =
				durable.OpenCollectionsWithCheckpointMembershipTransition(
					d.dataDir, txnOptions, requests, groupNames,
					d.schemaMembership, d.schemaCheckpointAuthority,
					durable.CheckpointGroupOptions{},
				)
		} else if d.replicatedSnapshotRecovery {
			collections, txnLog, checkpointGroup, err =
				durable.OpenCollectionsWithSnapshotCheckpointGroup(
					d.dataDir, txnOptions, requests, groupNames,
					replicatedstate.SystemCollectionName,
					durable.CheckpointGroupOptions{},
				)
			if errors.Is(err, durable.ErrCheckpointGroupSeedChanged) {
				err = errors.Join(ErrReplicatedSnapshotStageProof, err)
			}
		} else if d.replicatedSeedRecovery {
			collections, txnLog, checkpointGroup, err =
				durable.OpenCollectionsWithSeededCheckpointGroup(
					d.dataDir, txnOptions, requests, groupNames,
					replicatedstate.SystemCollectionName,
					durable.CheckpointGroupOptions{},
				)
		} else {
			collections, txnLog, checkpointGroup, err =
				durable.OpenCollectionsWithCheckpointGroup(
					d.dataDir, txnOptions, requests, groupNames,
					durable.CheckpointGroupOptions{},
				)
		}
		if errors.Is(err, durable.ErrCheckpointGroupMissing) {
			d.replicatedSeedPending = d.replicatedSeedRecovery
			collections, txnLog, err = durable.OpenCollectionsWithTransactions(
				d.dataDir, txnOptions, requests,
			)
		}
	} else {
		collections, txnLog, err = durable.OpenCollectionsWithTransactions(
			d.dataDir, txnOptions, requests,
		)
	}
	if err != nil {
		return abortFiles(err)
	}
	// Transfer every successful batch-open resource before the first fallible
	// profile or catalog-mirror validation. Failed-open teardown then owns the
	// complete set and cannot leak a later collection or the transaction log.
	for i := range opened {
		entry := opened[i]
		collection := collections[i]
		if entry.distributed {
			d.distributedTxnFile = entry.file
			d.distributedTxnCollection = collection
			continue
		}
		if entry.apply {
			d.replicatedApplyFile = entry.file
			d.replicatedApplyCollection = collection
			continue
		}
		if entry.capture {
			d.replicatedCaptureFile = entry.file
			d.replicatedCaptureCollection = collection
			continue
		}
		entry.table.file = entry.file
		entry.table.collection = collection
	}
	d.txnLog = txnLog
	d.checkpointGroup = checkpointGroup
	if checkpointGroup != nil && checkpointGroup.SeedActivationPending() {
		d.replicatedSeedPending = true
	}
	for i := range opened {
		entry := opened[i]
		collection := collections[i]
		if entry.distributed {
			if err := validateDistributedTransactionStateCollection(collection); err != nil {
				return err
			}
			continue
		}
		if entry.apply {
			if err := validateReplicatedApplyCollection(
				collection, d.catalog.ReplicatedApply.SystemLimits,
				d.catalog.ReplicatedApply.Sidecars,
			); err != nil {
				return err
			}
			continue
		}
		if entry.capture {
			options := replicatedCaptureDurableOptions(d.catalog.ReplicatedApply.CaptureLimits)
			if collection == nil || !collection.HasOpaqueValues() ||
				!collection.HasSynchronousDurability() || !collection.SupportsUpdate() ||
				collection.MaxKeyBytes() != options.MaxKeyBytes ||
				collection.MaxDocumentBytes() != options.MaxDocumentBytes ||
				collection.MaxBatchDocuments() != options.MaxBatchDocuments ||
				collection.MaxBatchBytes() != options.MaxBatchBytes {
				return ErrReplicatedApplyMismatch
			}
			continue
		}
		if d.catalog.ReplicatedShardStore != nil {
			continue
		}
		if !entry.table.meta.Materialized {
			entry.table.meta.Materialized = true
			d.catalogWritePending = true
		}
		changed, syncErr := syncTableIndexMeta(entry.table)
		if syncErr != nil {
			return fmt.Errorf(
				"vibedb: read durable table %q index catalog: %w",
				entry.name, syncErr,
			)
		}
		if changed {
			d.catalogWritePending = true
		}
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
		Collection: store.Options{Schema: t.schema},
		// SQL acknowledges a mutation only after its recovery record or root is
		// durable. Replacement rebuilds use bounded WriteBatch chunks under this
		// same contract; batching changes barrier count, not durability semantics.
		Durability:                 durable.DurabilitySync,
		SealedRecoveryJournalBytes: t.meta.SealedRecoveryJournalBytes,
	}
	for _, index := range t.meta.Indexes {
		options.Indexes = append(options.Indexes, store.IndexDefinition{
			Name: index.Name, Paths: append([]string(nil), index.Paths...), Unique: index.Unique,
		})
	}
	return options
}

// defaultDriverTxnLimits mirrors durable's package defaults for Database.Update.
// The numbers are pinned here because UpdateCollections is fail-closed at zero
// and does not substitute defaults for caller-owned catalogs.
func defaultDriverTxnLimits() durable.TxnLimits {
	const defaultBatchValueBytes = 16 << 20
	return durable.TxnLimits{
		MaxCollections: 16,
		MaxDocuments:   4 * store.MaxChunkDocuments,
		MaxBytes:       4 * (int64(store.MaxChunkDocuments)*256 + defaultBatchValueBytes),
	}
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
		next[i].Unique = infos[i].Unique
	}
	if len(next) == len(t.meta.Indexes) {
		equal := true
		for i := range next {
			if next[i].Name != t.meta.Indexes[i].Name ||
				!slices.Equal(next[i].Paths, t.meta.Indexes[i].Paths) ||
				next[i].Unique != t.meta.Indexes[i].Unique {
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
		// Schema-free JSON tables still have a driver-owned physical primary
		// key. documentKey validates that the pointer exists and resolves to a
		// scalar on every mutation; a durable document schema is optional.
		return nil
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

// tablePath returns the cataloged storage incarnation. An absent table has no
// current storage path; current catalogs never derive storage identity from a
// SQL name.
func (d *database) tablePath(name string) string {
	if meta := d.catalog.Tables[name]; meta != nil {
		return d.tablePathForMeta(meta)
	}
	return ""
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

func validateCatalogViewMeta(name string, meta *viewMeta) error {
	if err := validateCatalogTableName(name); err != nil {
		return fmt.Errorf("vibedb: SQL catalog view name %q: %w", name, err)
	}
	if meta == nil {
		return fmt.Errorf("vibedb: SQL catalog view %q has null metadata", name)
	}
	if meta.Query == "" {
		return fmt.Errorf("vibedb: SQL catalog view %q has an empty query", name)
	}
	if !utf8.ValidString(meta.Query) {
		return fmt.Errorf("vibedb: SQL catalog view %q query is not valid UTF-8", name)
	}
	if len(meta.Query) > maxCatalogViewQueryBytes {
		return fmt.Errorf(
			"vibedb: SQL catalog view %q query is %d bytes, maximum is %d",
			name, len(meta.Query), maxCatalogViewQueryBytes,
		)
	}
	if len(meta.Columns) > maxCatalogViewColumns || len(meta.Outputs) > maxCatalogViewColumns {
		return fmt.Errorf(
			"vibedb: SQL catalog view %q exceeds the %d-column bound",
			name, maxCatalogViewColumns,
		)
	}
	if len(meta.Outputs) == 0 {
		return fmt.Errorf("vibedb: SQL catalog view %q has no outputs", name)
	}
	if len(meta.Columns) > len(meta.Outputs) {
		return fmt.Errorf(
			"vibedb: SQL catalog view %q has %d aliases for %d outputs",
			name, len(meta.Columns), len(meta.Outputs),
		)
	}
	if err := validateCatalogViewNames(name, "column", meta.Columns, false); err != nil {
		return err
	}
	if err := validateCatalogViewNames(name, "output", meta.Outputs, false); err != nil {
		return err
	}
	if len(meta.ViewDependencies) > maxCatalogViewDependencies ||
		len(meta.TableDependencies) > maxCatalogViewDependencies {
		return fmt.Errorf(
			"vibedb: SQL catalog view %q exceeds the %d-dependency bound",
			name, maxCatalogViewDependencies,
		)
	}
	if err := validateCatalogViewNames(
		name, "view dependency", meta.ViewDependencies, true,
	); err != nil {
		return err
	}
	return validateCatalogViewNames(
		name, "table dependency", meta.TableDependencies, true,
	)
}

func validateCatalogViewNames(
	view string,
	kind string,
	names []string,
	allowView bool,
) error {
	for i, name := range names {
		if err := validateCatalogTableName(name); err != nil {
			return fmt.Errorf(
				"vibedb: SQL catalog view %q %s %q: %w",
				view, kind, name, err,
			)
		}
		if allowView && name == view {
			return fmt.Errorf(
				"vibedb: SQL catalog view %q depends on itself", view,
			)
		}
		for previous := 0; previous < i; previous++ {
			if names[previous] == name {
				return fmt.Errorf(
					"vibedb: SQL catalog view %q repeats %s %q",
					view, kind, name,
				)
			}
		}
	}
	return nil
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

func (d *database) collectionCloseState(
	collection *durable.Collection,
) (error, bool) {
	err := d.collectionClose(collection)
	if d.closeCollection != nil {
		// The injection seam predates CloseCompleted. A nil injected result means
		// the test double consumed the resource; a non-nil result models a
		// retryable phase. Production always consults the engine's exact state.
		return err, err == nil || collection.CloseCompleted()
	}
	return err, collection.CloseCompleted()
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

// settleDroppedTablesLocked finishes physical retirement after DROP TABLE,
// TRUNCATE, or a storage-replacing DROP INDEX has durably published its catalog
// state. A live snapshot may keep the old collection from closing immediately;
// that is not a catalog failure and is retried on a later catalog operation or
// final database close. The old file is never removed before catalog
// publication, so a crash in this cleanup window leaves only an unreachable
// orphan, not a catalog entry whose data file disappeared.
func (d *database) settleDroppedTablesLocked() error {
	if err := d.settleTxnReattachLocked(); err != nil {
		return err
	}
	if len(d.retired) == 0 {
		return nil
	}
	remaining := d.retired[:0]
	removedAny := false
	retainFailure := func(at int, err error) error {
		d.retired = append(remaining, d.retired[at:]...)
		if removedAny {
			// At least one namespace deletion happened before this later cleanup
			// failed. Preserve its durability fence so retryNamespaceFencesLocked
			// syncs the directory before any removed marker can be discarded.
			d.tableDirFencePending = true
		}
		return err
	}
	for i := range d.retired {
		retired := &d.retired[i]
		if retired.collection != nil {
			// Normal DROP/replacement already detached before catalog publication.
			// An unpublished candidate retained after a failed detach may still be
			// registered; retry that ownership barrier before any Close/unlink.
			if d.txnLog != nil {
				if err := d.txnLog.DetachCollection(retired.collection); err != nil {
					return retainFailure(i, fmt.Errorf(
						"%w: detach retiring SQL table incarnation %q: %w",
						durable.ErrCommitOutcomeUnknown, retired.name, err,
					))
				}
			}
			closeErr, completed := d.collectionCloseState(retired.collection)
			if !completed {
				if errors.Is(closeErr, storeio.ErrLeasesActive) {
					remaining = append(remaining, *retired)
					continue
				}
				return retainFailure(i, fmt.Errorf(
					"%w: finish retiring SQL table incarnation %q: %w",
					durable.ErrCommitOutcomeUnknown, retired.name, closeErr,
				))
			}
			retired.collection = nil
			if closeErr != nil {
				return retainFailure(i, fmt.Errorf(
					"%w: retired SQL table incarnation %q closed with a terminal error: %w",
					durable.ErrCommitOutcomeUnknown, retired.name, closeErr,
				))
			}
		}
		if retired.file != nil {
			if err := retired.file.Close(); err != nil {
				return retainFailure(i, fmt.Errorf(
					"%w: close retired SQL table incarnation %q: %v",
					durable.ErrCommitOutcomeUnknown, retired.name, err,
				))
			}
			retired.file = nil
		}
		if retired.removed {
			continue
		}
		for _, candidate := range [...]string{
			retired.path, retired.journal,
			retired.extraPath, retired.extraJournal,
		} {
			if candidate == "" {
				continue
			}
			if err := os.Remove(candidate); err != nil {
				if !os.IsNotExist(err) {
					return retainFailure(i, fmt.Errorf(
						"%w: remove retired SQL storage %q for %q: %v",
						durable.ErrCommitOutcomeUnknown,
						filepath.Base(candidate), retired.name, err,
					))
				}
			} else {
				removedAny = true
			}
		}
		retired.removed = true
		remaining = append(remaining, *retired)
	}
	if removedAny {
		d.tableDirFencePending = true
		if err := d.directorySync(d.dataDir); err != nil {
			d.retired = remaining
			return fmt.Errorf(
				"%w: sync SQL table directory after table retirement: %w",
				durable.ErrCommitOutcomeUnknown, err,
			)
		}
		d.tableDirFencePending = false
	}
	// Entries marked removed have crossed both namespace fences and no longer
	// own a resource. Entries retained above are only the ones waiting on a
	// live snapshot or a retryable cleanup error.
	if len(remaining) != 0 {
		kept := remaining[:0]
		for _, retired := range remaining {
			if !retired.removed || retired.collection != nil || retired.file != nil {
				kept = append(kept, retired)
			}
		}
		remaining = kept
	}
	d.retired = remaining
	return nil
}

func (d *database) retainTxnReattachLocked(collection *durable.Collection) {
	if collection == nil {
		return
	}
	for _, pending := range d.txnReattach {
		if pending == collection {
			return
		}
	}
	d.txnReattach = append(d.txnReattach, collection)
}

// settleTxnReattachLocked restores transaction-log ownership for authoritative
// collections detached by a catalog mutation that definitely did not publish.
// Until this succeeds, allowing even an unrelated write could create decisions
// whose poison/discharge scope omits a live catalog member.
func (d *database) settleTxnReattachLocked() error {
	if len(d.txnReattach) == 0 {
		return nil
	}
	if d.txnLog == nil {
		return fmt.Errorf(
			"%w: transaction log is unavailable for %d pending collection registration(s)",
			durable.ErrCommitOutcomeUnknown, len(d.txnReattach),
		)
	}
	for len(d.txnReattach) != 0 {
		collection := d.txnReattach[0]
		if err := d.txnLog.AdoptCollection(collection); err != nil {
			return fmt.Errorf(
				"%w: restore authoritative collection transaction ownership: %w",
				durable.ErrCommitOutcomeUnknown, err,
			)
		}
		copy(d.txnReattach, d.txnReattach[1:])
		d.txnReattach[len(d.txnReattach)-1] = nil
		d.txnReattach = d.txnReattach[:len(d.txnReattach)-1]
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
	if err := d.settleDroppedTablesLocked(); err != nil {
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
	bound, err := catalogSizeUpperBound(d.catalog)
	if err != nil {
		return false, err
	}
	raw, err := appendCatalogJSON(make([]byte, 0, bound), d.catalog)
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

// checkCatalogSize computes a conservative upper bound for canonical encoding
// without allocating. The fixed allowances cover field names, punctuation,
// booleans/numbers, newlines, and indentation at each structural level;
// encodedJSONStringBytes conservatively accounts for JSON string escaping.
func checkCatalogSize(catalog catalogFile) error {
	_, err := catalogSizeUpperBound(catalog)
	return err
}

func catalogSizeUpperBound(catalog catalogFile) (int, error) {
	if catalog.ShardStore != nil {
		if err := validateShardStoreIdentity(*catalog.ShardStore); err != nil {
			return maxCatalogBytes + 1, fmt.Errorf(
				"vibedb: invalid shard store identity: %w", err,
			)
		}
	}
	if catalog.ShardStoreFence != nil {
		if catalog.ShardStore == nil {
			return maxCatalogBytes + 1, errors.New(
				"vibedb: shard store fence requires a shard store identity",
			)
		}
		if err := validateShardStoreFence(*catalog.ShardStoreFence); err != nil {
			return maxCatalogBytes + 1, fmt.Errorf(
				"vibedb: invalid shard store fence: %w", err,
			)
		}
	}
	if err := validateReplicatedCatalog(catalog); err != nil {
		return maxCatalogBytes + 1, fmt.Errorf(
			"vibedb: invalid replicated shard store identity: %w", err,
		)
	}
	if err := checkCatalogTableCount(len(catalog.Tables)); err != nil {
		return maxCatalogBytes + 1, err
	}
	if err := checkCatalogViewCount(len(catalog.Views)); err != nil {
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
	if catalog.ShardStore != nil {
		if !add(encodedJSONStringBytes(string(catalog.ShardStore.Distribution)) +
			encodedJSONStringBytes(string(catalog.ShardStore.Shard)) + 512) {
			return size, catalogSizeError(size)
		}
	}
	if catalog.ShardStoreFence != nil {
		if !add(256) {
			return size, catalogSizeError(size)
		}
	}
	if catalog.ReplicatedShardStore != nil {
		r := catalog.ReplicatedShardStore
		relationBytes := 0
		for ordinal := 0; ordinal < int(r.RelationCount); ordinal++ {
			relation := r.Relations[ordinal]
			part := encodedJSONStringBytes(relation.Table) +
				encodedJSONStringBytes(relation.Storage) + 1024
			if part < 0 || relationBytes > maxCatalogBytes-part {
				relationBytes = maxCatalogBytes + 1
				break
			}
			relationBytes += part
		}
		if !add(encodedJSONStringBytes(r.Binding.Distribution) +
			encodedJSONStringBytes(r.Binding.Shard) +
			encodedJSONStringBytes(r.UserTable) +
			encodedJSONStringBytes(r.UserStorage) +
			encodedJSONStringBytes(r.UserPrimaryKey) + relationBytes + 4096) {
			return size, catalogSizeError(size)
		}
	}
	if catalog.ReplicatedApply != nil {
		apply := catalog.ReplicatedApply
		placementBytes := encodedJSONStringBytes(apply.Placement.ShardKey)
		if !add(encodedJSONStringBytes(apply.Storage) +
			encodedJSONStringBytes(apply.CaptureStorage) + placementBytes + 3072) {
			return size, catalogSizeError(size)
		}
	}
	if catalog.ReplicatedChildApply != nil {
		apply := catalog.ReplicatedChildApply
		placementBytes := encodedJSONStringBytes(apply.Placement.ShardKey)
		if !add(encodedJSONStringBytes(apply.Storage) +
			encodedJSONStringBytes(apply.CaptureStorage) + placementBytes + 3072) {
			return size, catalogSizeError(size)
		}
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
		if !add(encodedJSONStringBytes(meta.Storage) + 32) {
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
			part := encodedJSONStringBytes(index.Name) + 256
			if index.Unique {
				part += len(`,"unique":true`)
			}
			if !add(part) {
				return size, catalogSizeError(size)
			}
			for _, path := range index.Paths {
				if !add(encodedJSONStringBytes(path) + 64) {
					return size, catalogSizeError(size)
				}
			}
		}
	}
	for name, meta := range catalog.Views {
		if !add(encodedJSONStringBytes(name) + 512) {
			return size, catalogSizeError(size)
		}
		if meta == nil {
			continue
		}
		if !add(encodedJSONStringBytes(meta.Query) + 256) {
			return size, catalogSizeError(size)
		}
		for _, values := range [...][]string{
			meta.Columns, meta.Outputs,
			meta.ViewDependencies, meta.TableDependencies,
		} {
			for _, value := range values {
				if !add(encodedJSONStringBytes(value) + 32) {
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

func checkCatalogViewCount(views int) error {
	if views <= maxCatalogViews {
		return nil
	}
	return fmt.Errorf(
		"%w: catalog has %d views, maximum is %d",
		ErrTooManyViews, views, maxCatalogViews,
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
			size += 6 // canonical encoding replaces invalid UTF-8 with \ufffd
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
	if d.catalog.ReplicatedShardStore != nil {
		return nil, ErrDirectWriteFenced
	}
	t, ok := d.tables[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, name)
	}
	if t.collection != nil {
		return t, nil
	}
	// A candidate whose engine Close stops at a retryable lifecycle phase must
	// remain owned by the database. Reserve its bounded cleanup slot before any
	// engine resource is created, so no error path can strand a writer lock or
	// caller-owned descriptor.
	if err := d.checkRetirementCapacityLocked(1); err != nil {
		return nil, err
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
		default:
			err = collection.Update(func(batch *durable.WriteBatch) error {
				for _, document := range documents {
					if putErr := batch.Put([]byte(document.key), document.document); putErr != nil {
						return putErr
					}
				}
				return nil
			})
		}
	}
	if err != nil {
		return nil, errors.Join(err, d.discardUnpublishedStorageLocked(
			collection, file, tmpPath,
		))
	}
	file, collection, err = d.publishTableStorageLocked(
		tmpPath, path, file, collection, durableOptions(t),
	)
	if err != nil {
		return nil, fmt.Errorf("vibedb: publish first SQL table: %w", err)
	}
	if err := d.txnLog.AdoptCollection(collection); err != nil {
		return nil, errors.Join(err,
			d.discardUnpublishedStorageLocked(collection, file, path))
	}
	t.file, t.collection = file, collection
	t.meta.Materialized = true
	d.catalogWritePending = true
	// First materialization changes the readable physical layout while retaining
	// the same logical table object. Dependency-scoped Read Committed capture
	// resolves that object directly, while the epoch records the retained
	// nil-to-live transition for catalog-generation fencing.
	d.advanceLayoutEpochLocked()
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

// closeTerminal is the terminal close attempted after a database/sql connector
// loses its final connection reference, and while unwinding a failed open.
// Completed collection teardown releases every descriptor and the catalog
// writer even when it reports a sticky persistence error. Retryable incomplete
// teardown retains ownership instead: closing its caller-owned descriptor or
// dropping the final Go reference would violate durable.Collection's lifecycle
// contract and can strand engine resources invisibly.
//
// The connector reference invariant proves there are no row or transaction
// snapshots at this point: conn.Close rejects open rows and rolls back an open
// transaction before releasing its reference. SQL collections use synchronous
// durability, so the no-reader invariant makes retryable teardown exceptional;
// CloseCompleted remains the authoritative ownership boundary rather than an
// assumption about a particular error.
func (d *database) closeTerminal() error {
	return d.closeWithPolicy(true)
}

func (d *database) closeCompleted() bool {
	if d == nil {
		return true
	}
	d.mu.Lock()
	done := d.closeDone
	d.mu.Unlock()
	return done
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
	if d.checkpointGroup != nil {
		closeErr := d.checkpointGroup.Close()
		if closeErr != nil && !d.checkpointGroup.CloseCompleted() {
			// A checkpoint failure precedes the terminal transition, so the group
			// remains attached and member resource close is still forbidden.
			return errors.Join(result, closeErr)
		}
		// A certificate-descriptor Close error is sticky but occurs after the
		// retired fence. Preserve it while releasing every member resource.
		result = errors.Join(result, closeErr)
		d.checkpointGroup = nil
	}
	d.closed = true
	retryable := false
	if d.replicatedApplyCollection != nil {
		closeErr, completed := d.collectionCloseState(d.replicatedApplyCollection)
		result = errors.Join(result, closeErr)
		if completed {
			d.replicatedApplyCollection = nil
		} else {
			retryable = true
		}
	}
	if d.replicatedCaptureCollection != nil {
		closeErr, completed := d.collectionCloseState(d.replicatedCaptureCollection)
		result = errors.Join(result, closeErr)
		if completed {
			d.replicatedCaptureCollection = nil
		} else {
			retryable = true
		}
	}
	if d.distributedTxnCollection != nil {
		closeErr, completed := d.collectionCloseState(d.distributedTxnCollection)
		result = errors.Join(result, closeErr)
		if completed {
			d.distributedTxnCollection = nil
		} else {
			retryable = true
		}
	}
	for _, t := range d.tables {
		if t.collection != nil {
			closeErr, completed := d.collectionCloseState(t.collection)
			result = errors.Join(result, closeErr)
			if completed {
				t.collection = nil
			} else {
				retryable = true
			}
		}
	}
	for i := range d.retired {
		if d.retired[i].collection != nil {
			closeErr, completed := d.collectionCloseState(d.retired[i].collection)
			result = errors.Join(result, closeErr)
			if completed {
				d.retired[i].collection = nil
			} else {
				retryable = true
			}
		}
	}
	if retryable {
		// Collection.Close is retryable after outstanding leases or transient
		// checkpoint failures clear. This applies even to a terminal connector
		// attempt: invalidating its descriptor or dropping the last Go owner while
		// engine teardown is incomplete would violate the collection lifecycle.
		return result
	}
	if d.replicatedApplyFile != nil {
		result = errors.Join(result, d.replicatedApplyFile.Close())
		d.replicatedApplyFile = nil
	}
	if d.replicatedCaptureFile != nil {
		result = errors.Join(result, d.replicatedCaptureFile.Close())
		d.replicatedCaptureFile = nil
	}
	if d.distributedTxnFile != nil {
		result = errors.Join(result, d.distributedTxnFile.Close())
		d.distributedTxnFile = nil
	}
	for _, t := range d.tables {
		if t.file != nil {
			result = errors.Join(result, t.file.Close())
			t.file = nil
		}
	}
	if terminal {
		completed, cleanupErr := d.settleTerminalRetiredLocked()
		result = errors.Join(result, cleanupErr)
		if !completed {
			if result == nil {
				result = fmt.Errorf(
					"%w: terminal SQL storage retirement remains incomplete",
					durable.ErrCommitOutcomeUnknown,
				)
			}
			return result
		}
	} else {
		for i := range d.retired {
			if d.retired[i].file != nil {
				result = errors.Join(result, d.retired[i].file.Close())
				d.retired[i].file = nil
			}
		}
	}
	if d.txnLog != nil {
		result = errors.Join(result, d.txnLog.Close())
		d.txnLog = nil
	}
	if d.lockFile != nil {
		result = errors.Join(result, storeio.UnlockWriter(d.lockFile))
		result = errors.Join(result, d.lockFile.Close())
		d.lockFile = nil
	}
	d.closeDone = true
	return result
}

// settleTerminalRetiredLocked is the best-effort, retryable retirement drain
// used after every collection has completed engine teardown. Unlike the normal
// mutation path it keeps visiting later paths after one removal fails: terminal
// ownership must not detach merely because an earlier sticky close error made
// settleDroppedTablesLocked return before namespace cleanup.
//
// A false result means d still owns either an undeleted path or an unfenced
// namespace mutation. The terminal-retirement registry retains d and retries;
// the catalog writer lock is deliberately released only after this drain
// completes, preventing a new opener from racing the old owner's cleanup.
func (d *database) settleTerminalRetiredLocked() (bool, error) {
	if len(d.retired) == 0 && !d.tableDirFencePending {
		return true, nil
	}
	remaining := d.retired[:0]
	removedAny := false
	var result error
	for i := range d.retired {
		retired := d.retired[i]
		if retired.collection != nil {
			remaining = append(remaining, retired)
			continue
		}
		if retired.file != nil {
			// os.File.Close renders the descriptor unusable even when it reports
			// an error. Clear the pointer, preserve the sticky error, and continue
			// removing every namespace entry owned by this retirement.
			result = errors.Join(result, retired.file.Close())
			retired.file = nil
		}
		if !retired.removed {
			removed := true
			for _, candidate := range [...]string{
				retired.path, retired.journal,
				retired.extraPath, retired.extraJournal,
			} {
				if candidate == "" {
					continue
				}
				if err := os.Remove(candidate); err != nil {
					if !os.IsNotExist(err) {
						removed = false
						result = errors.Join(result, fmt.Errorf(
							"remove terminal SQL storage %q for %q: %w",
							filepath.Base(candidate), retired.name, err,
						))
					}
				} else {
					removedAny = true
				}
			}
			retired.removed = removed
		}
		if !retired.removed {
			remaining = append(remaining, retired)
		}
	}
	d.retired = remaining
	if removedAny {
		d.tableDirFencePending = true
	}
	if d.tableDirFencePending {
		if err := d.directorySync(d.dataDir); err != nil {
			return false, errors.Join(result, fmt.Errorf(
				"%w: fence terminal SQL storage retirement: %w",
				durable.ErrCommitOutcomeUnknown, err,
			))
		}
		d.tableDirFencePending = false
	}
	return len(d.retired) == 0, result
}
