package gateway

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/maphash"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The distributed write path: one mutating statement routed to the single shard
// that owns its rows, dispatched through the same pinned-generation machinery
// as reads, and executed by the shard as one local statement.
//
// A write is dispatchable only when every row it touches is provably resident on
// exactly one shard of the pinned generation:
//
//   - an INSERT routes every VALUES row by its shard key and is admitted only
//     when all rows resolve to the same leader target;
//   - an UPDATE or DELETE routes its WHERE predicate and is admitted only when
//     it resolves to one shard (an empty route is a local no-op), and an
//     UPDATE's whole-document replacement must not move a row to another shard.
//
// Everything else — a scatter, an unbounded predicate, a cross-shard batch, or
// a statement kind with no single-shard form (DDL, TRUNCATE, INSERT ... SELECT)
// — is refused before any network I/O, so a write never partially commits.
// Cross-shard batches use this same per-statement proof before ExecBatch groups
// their single-owner statements into durable participants.

// ErrDistributedWriteUnsupported reports a write shape the distributed layer
// cannot execute against more than zero shards of a generation. It wraps a
// typed refusal; the planner fails before dispatch rather than dispatching a
// semantically incomplete scatter.
var ErrDistributedWriteUnsupported = errors.New("gateway: distributed write plan is unsupported")

// ErrReplicatedSQLWriteUnavailable fences the legacy SQL mutation path from an
// RF3 allocation. Replicated writes require canonical native commands and
// deterministic completion semantics; SQL text is never copied into Raft.
var ErrReplicatedSQLWriteUnavailable = errors.New("gateway: SQL writes are unavailable for replicated shards")

// ErrWriteScatter reports an UPDATE or DELETE whose predicate does not resolve
// to exactly one shard.
var ErrWriteScatter = errors.New("gateway: write predicate does not resolve to a single shard")

// ErrWriteCrossShard reports an INSERT whose VALUES rows do not all route to
// the same shard.
var ErrWriteCrossShard = errors.New("gateway: insert rows route to more than one shard")

// ErrWriteShardKeyMove reports a whole-document UPDATE whose replacement
// document would move the row to a different shard.
var ErrWriteShardKeyMove = errors.New("gateway: update replacement document would move the row to another shard")

// ErrExecRequiresMutation reports a non-mutating statement submitted to
// [Executor.Exec].
var ErrExecRequiresMutation = errors.New("gateway: Exec requires a mutating statement")

// ErrGlobalIndexMaintenanceUnsupported reports an indexed mutation whose old
// and new rows cannot yet be proven before target staging. Refusing it is
// a correctness fence: a READY global index is never allowed to drift behind
// its base table.
var ErrGlobalIndexMaintenanceUnsupported = errors.New("gateway: global index maintenance for this mutation shape is unsupported")

type preparedGlobalIndex struct {
	program        GlobalIndexProgram
	keyColumns     [4]int
	locatorColumns [distribution.KeyspaceWidth]int
	wholeDocument  bool
}

type flatInsertColumnOrdinal struct {
	start   int
	end     int
	ordinal int
}

type flatInsertColumnIndex struct {
	seed    maphash.Seed
	arena   []byte
	columns []flatInsertColumnOrdinal
	slots   []int
}

func buildFlatInsertColumnIndex(
	columns []*sqlast.PathExpr,
) (flatInsertColumnIndex, bool) {
	return buildFlatInsertColumnIndexSeed(columns, maphash.MakeSeed())
}

func buildFlatInsertColumnIndexSeed(
	columns []*sqlast.PathExpr,
	seed maphash.Seed,
) (flatInsertColumnIndex, bool) {
	if len(columns) == 0 {
		return flatInsertColumnIndex{}, false
	}
	maxInt := int(^uint(0) >> 1)
	arenaCapacity := len(columns)
	if arenaCapacity <= maxInt/16 {
		arenaCapacity *= 16
	}
	slotCount := 2
	for slotCount < len(columns) && slotCount <= maxInt/2 {
		slotCount <<= 1
	}
	if slotCount <= maxInt/2 {
		slotCount <<= 1
	}
	index := flatInsertColumnIndex{
		seed:    seed,
		arena:   make([]byte, 0, arenaCapacity),
		columns: make([]flatInsertColumnOrdinal, len(columns)),
		slots:   make([]int, slotCount),
	}
	mask := uint64(slotCount - 1)
	for i := range columns {
		column := &index.columns[i]
		column.start = len(index.arena)
		index.arena = columns[i].AppendPointer(index.arena)
		column.end = len(index.arena)
		column.ordinal = i
		slot := int(maphash.Bytes(index.seed, index.arena[column.start:column.end]) & mask)
		for {
			stored := index.slots[slot]
			if stored == 0 {
				index.slots[slot] = i + 1
				break
			}
			prior := index.columns[stored-1]
			if bytes.Equal(
				index.arena[prior.start:prior.end], index.arena[column.start:column.end],
			) {
				return index, true
			}
			slot = (slot + 1) & int(mask)
		}
	}
	return index, false
}

func (index *flatInsertColumnIndex) find(path string) (int, bool) {
	if index == nil || len(index.slots) == 0 {
		return 0, false
	}
	mask := uint64(len(index.slots) - 1)
	slot := int(maphash.String(index.seed, path) & mask)
	for {
		stored := index.slots[slot]
		if stored == 0 {
			return 0, false
		}
		column := index.columns[stored-1]
		if vibejson.BytesEqualString(index.arena[column.start:column.end], path) {
			return column.ordinal, true
		}
		slot = (slot + 1) & int(mask)
	}
}

type boundGlobalIndexMutation struct {
	metadata IndexMetadata
	kind     shardservice.MutationKind

	distribution         distribution.DistributionName
	target               distribution.Target
	address              string
	routingVersion       distribution.RoutingVersion
	bucketBits           uint8
	scope                distributedtxn.IntentScope
	entryStart, entryEnd int
	valueStart, valueEnd int
}

// prepareWrite validates one mutating statement against the pinned catalog
// generation and records the routing metadata [PreparedPlan.BindWrite] binds
// against later. Like the read path it fails closed: a table with no placement,
// a statement kind with no single-shard form, or a shape that cannot be proven
// single-shard never becomes a dispatchable plan.
func (s *Snapshot) prepareWrite(plan *PreparedPlan, source string) error {
	stmt := &plan.statement
	var (
		where       *sqlast.Expr
		hasWhere    bool
		wholeDocIns bool
	)
	switch stmt.Kind {
	case sqlast.KindInsert:
		ins := stmt.Insert
		if ins == nil {
			return &PlanError{Reason: "malformed insert statement", cause: ErrDistributedWriteUnsupported}
		}
		if ins.Source != nil {
			plan.alwaysReason = "INSERT with a query source requires a distributed source plan"
			break
		}
		if len(ins.Rows) == 0 {
			plan.alwaysReason = "an INSERT requires at least one VALUES row"
			break
		}
		wholeDocIns = len(ins.Columns) == 0
	case sqlast.KindUpdate:
		upd := stmt.Update
		if upd == nil {
			return &PlanError{Reason: "malformed update statement", cause: ErrDistributedWriteUnsupported}
		}
		if upd.Filter == nil || upd.Filter.Where == nil {
			plan.alwaysReason = "an UPDATE without a shard-key predicate is a cross-shard scatter"
			break
		}
		where = upd.Filter.Where
		hasWhere = true
	case sqlast.KindDelete:
		del := stmt.Delete
		if del == nil {
			return &PlanError{Reason: "malformed delete statement", cause: ErrDistributedWriteUnsupported}
		}
		if del.Filter == nil || del.Filter.Where == nil {
			plan.alwaysReason = "a DELETE without a shard-key predicate is a cross-shard scatter"
			break
		}
		where = del.Filter.Where
		hasWhere = true
	default:
		return &WriteNotSupportedError{Kind: stmt.Kind}
	}

	plan.table = stmt.Table()
	placement, spec, manifest, ok := s.plannerTableFor(plan.table)
	if !ok {
		return &PlanError{
			Table: plan.table, Reason: "no placement in pinned catalog generation",
			cause: ErrTableNotPlaced,
		}
	}
	if stmt.Kind == sqlast.KindInsert && stmt.Insert.OnConflictUpdate != nil {
		if _, replicated := s.replicatedTableAtBytes(byteview.Bytes(plan.table)); replicated {
			return &PlanError{Table: plan.table,
				Reason: "RF3 ON CONFLICT DO UPDATE requires branch-aware replicated writes",
				cause:  ErrDistributedWriteUnsupported}
		}
		if err := validateConflictShardKeyAssignments(stmt.Insert.OnConflictUpdate, placement.Columns); err != nil {
			return err
		}
	}
	if stmt.Kind == sqlast.KindInsert && stmt.Insert.ConflictTarget != nil {
		if entry, replicated := s.replicatedTableAtBytes(byteview.Bytes(plan.table)); replicated {
			profile, ok := s.replicatedTableProfileAt(entry)
			if !ok || !bytes.Equal(stmt.Insert.ConflictTarget.AppendPointer(nil), []byte(profile.PrimaryKey)) {
				return sqlast.NewFeatureNotSupportedError(source, stmt.Insert.ConflictTarget.Pos,
					"ON CONFLICT target must be the declared primary key")
			}
		}
	}
	plan.distribution = placement.Distribution
	plan.spec = spec
	plan.manifest = manifest
	plan.params = stmt.Params()
	if hasWhere {
		plan.constraints = sqldriver.CompileConstraintProgram(placement.Columns, where)
	}

	// Compile the shard-key pointers once per plan: whole-document inserts and
	// UPDATE replacement documents both route by reading them out of a JSON
	// document.
	if stmt.Kind == sqlast.KindUpdate || wholeDocIns {
		pointers := make([]vibejson.CompiledPointer, len(placement.Columns))
		for i, col := range placement.Columns {
			p, err := vibejson.CompilePointer(col)
			if err != nil {
				return &PlanError{
					Table: plan.table, Reason: "shard-key column " + col + " is not a compilable JSON pointer",
					cause: ErrDistributedWriteUnsupported,
				}
			}
			pointers[i] = p
		}
		plan.writeKeyPointers = pointers
	}
	// A flat insert supplies each shard-key ordinal from one named top-level
	// column. Build one byte-native ordinal index for both shard and global-index
	// routing. Repeated columns are ambiguous and fail before binding. A missing
	// shard ordinal means the row cannot be proven single-shard.
	var insertColumns flatInsertColumnIndex
	if stmt.Kind == sqlast.KindInsert && !wholeDocIns {
		ins := stmt.Insert
		var duplicate bool
		insertColumns, duplicate = buildFlatInsertColumnIndex(ins.Columns)
		if duplicate {
			return &PlanError{
				Table:  plan.table,
				Reason: "flat INSERT repeats a named column",
				cause:  ErrDistributedWriteUnsupported,
			}
		}
		keyColumns := make([]int, len(placement.Columns))
		for ordinal, col := range placement.Columns {
			i, ok := insertColumns.find(col)
			if !ok {
				plan.alwaysReason = "shard-key column " + col + " is not a top-level insert column"
				break
			}
			keyColumns[ordinal] = i
		}
		if plan.alwaysReason == "" {
			plan.writeKeyColumns = keyColumns
		}
	}
	if err := s.prepareGlobalIndexWrites(plan, wholeDocIns, insertColumns); err != nil {
		return err
	}
	if stmt.Kind == sqlast.KindInsert && stmt.Insert.OnConflictDoNothing &&
		len(plan.writeGlobalIndexes) != 0 {
		return &PlanError{
			Table:  plan.table,
			Reason: "ON CONFLICT DO NOTHING with global indexes requires branch-aware index maintenance",
			cause:  ErrDistributedWriteUnsupported,
		}
	}
	if stmt.Kind == sqlast.KindInsert && stmt.Insert.OnConflictUpdate != nil && len(plan.writeGlobalIndexes) != 0 {
		return &PlanError{Table: plan.table,
			Reason: "ON CONFLICT DO UPDATE with global indexes requires branch-aware index maintenance",
			cause:  ErrDistributedWriteUnsupported}
	}
	return nil
}

// The shard executes the complete conflict action atomically. Its current row
// and EXCLUDED row both belong to the selected owner, so copying either key is
// safe. An arbitrary expression assigning an ancestor of a shard-key pointer
// needs a postimage placement proof before it can be dispatched.
func validateConflictShardKeyAssignments(action *sqlast.InsertConflictUpdate, keys []string) error {
	if action.WholeDocument() {
		return nil // EXCLUDED is exactly the already-routed candidate document.
	}
	for _, pointer := range keys {
		root := strings.TrimPrefix(pointer, "/")
		if end := strings.IndexByte(root, '/'); end >= 0 {
			root = root[:end]
		}
		root = strings.ReplaceAll(strings.ReplaceAll(root, "~1", "/"), "~0", "~")
		for _, assignment := range action.Assignments {
			if assignment.Column != root {
				continue
			}
			if assignment.Value.Kind == sqlast.OperandExcluded && assignment.Value.Text == root {
				continue
			}
			if expr := assignment.Expr; expr != nil && expr.Kind == sqlast.ScalarPath && expr.Path != nil &&
				(expr.Path.Source == 0 || expr.Path.Source == 1) && len(expr.Path.Segments) == 1 && expr.Path.Segments[0].Key == root {
				continue
			}
			return ErrWriteShardKeyMove
		}
	}
	return nil
}

func hasComputedUpdateAssignments(statement *sqlast.Statement) bool {
	if statement == nil || statement.Kind != sqlast.KindUpdate ||
		statement.Update == nil {
		return false
	}
	for i := range statement.Update.Assignments {
		if statement.Update.Assignments[i].Expr != nil {
			return true
		}
	}
	return false
}

func (s *Snapshot) prepareGlobalIndexWrites(
	plan *PreparedPlan,
	wholeDocument bool,
	columns flatInsertColumnIndex,
) error {
	indexes := s.Indexes(plan.table)
	if indexes.Len() == 0 {
		return nil
	}
	for ordinal := 0; ordinal < indexes.Len(); ordinal++ {
		metadata, _ := indexes.At(ordinal)
		if !metadata.Global() {
			continue
		}
		if !updateMayChangeGlobalIndex(&plan.statement, metadata) {
			continue
		}
		program, err := s.compileGlobalIndex(plan.table, metadata.Name, false)
		if err != nil {
			return err
		}
		prepared := preparedGlobalIndex{
			program:       program,
			wholeDocument: wholeDocument || plan.statement.Kind != sqlast.KindInsert,
		}
		if plan.statement.Kind == sqlast.KindInsert && !wholeDocument {
			for i := uint8(0); i < metadata.PathCount; i++ {
				column, ok := columns.find(metadata.Paths[i])
				if !ok {
					return &PlanError{
						Table: plan.table,
						Reason: "global index " + metadata.Name +
							" key path " + metadata.Paths[i] + " is absent from INSERT",
						cause: ErrGlobalIndexMaintenanceUnsupported,
					}
				}
				prepared.keyColumns[i] = column
			}
			for i := uint8(0); i < metadata.LocatorCount; i++ {
				column, ok := columns.find(metadata.LocatorPaths[i])
				if !ok {
					return &PlanError{
						Table: plan.table,
						Reason: "global index " + metadata.Name +
							" locator path " + metadata.LocatorPaths[i] + " is absent from INSERT",
						cause: ErrGlobalIndexMaintenanceUnsupported,
					}
				}
				prepared.locatorColumns[i] = column
			}
		}
		plan.writeGlobalIndexes = append(plan.writeGlobalIndexes, prepared)
	}
	return nil
}

// BoundWritePlan is the immutable execution-specific result of binding typed
// parameters to a prepared write plan. rowKeys holds one full shard key per
// INSERT VALUES row; constraints hold the UPDATE or DELETE predicate domains.
// A write plan routes to at most one shard, so neither field can carry a
// scatter.
type BoundWritePlan struct {
	generation   uint64
	table        string
	distribution distribution.DistributionName
	spec         distribution.DistributionSpec
	manifest     *distribution.Manifest

	kind        sqlast.Kind
	constraints distribution.BoundConstraints
	rowKeys     [][]distribution.Scalar
	// insertDoc borrows the exact whole-document operand for a single-row INSERT.
	// insertDocs borrows every operand for a multi-row whole-document INSERT.
	// Keeping the singleton inline avoids another allocation on the ordinary
	// write lane; the multi-row lane allocates only its already-bounded slice of
	// byte views and never creates a second JSON representation.
	insertDoc  []byte
	insertDocs [][]byte
	// keyPointers holds one compiled shard-key pointer per ordinal for a
	// whole-document insert or UPDATE; it is nil otherwise.
	keyPointers []vibejson.CompiledPointer
	// updateDoc holds the UPDATE's whole-document replacement bytes, materialized
	// from its bound operand. It is nil unless the plan is a whole-document UPDATE;
	// the executor re-reads its shard key from it to prove the replacement cannot
	// move a row to another shard.
	updateDoc []byte
	// updateArgs are borrowed for the lifetime of one execution and are used
	// only when updateAssignments materializes a declared-column replacement
	// from the leader's current document.
	updateAssignments []sqlast.UpdateAssignment
	updateArgs        []any

	globalIndexes    []boundGlobalIndexMutation
	globalIndexArena []byte
	primaryPath      []byte
	expectedKeys     [][]byte
	expectedDigests  [][sha256.Size]byte
	postimageKeys    [][]byte
	postimageDigests [][sha256.Size]byte

	// globalIndexLocatorJSON is batch-local encoder scratch. It is embedded in
	// the already-returned bound plan so a flat indexed write does not allocate
	// a separate custom-hook receiver on its first row.
	globalIndexLocatorJSON globalIndexLocatorDocument
}

func (b *BoundWritePlan) requiresIndexTransaction() bool {
	return b != nil && (len(b.primaryPath) != 0 || len(b.globalIndexes) != 0)
}

// BindWrite applies params to a prepared write plan without reparsing SQL.
// args use the gateway's closed runtime scalar vocabulary. An INSERT extracts
// each row's shard key from its bound values. An UPDATE or DELETE binds its WHERE
// predicate and, for a whole-document UPDATE, proves the replacement document
// cannot move the row to another shard.
func (p *PreparedPlan) BindWrite(args []any) (*BoundWritePlan, error) {
	if p == nil || p.manifest == nil {
		return nil, &PlanError{Reason: "incomplete prepared write plan", cause: ErrDistributedWriteUnsupported}
	}
	if len(args) != p.params {
		return nil, &PlanError{
			Table:  p.table,
			Reason: fmt.Sprintf("got %d parameters, want %d", len(args), p.params),
			cause:  ErrPlanParameters,
		}
	}
	if err := validateGatewayBindArgs(args); err != nil {
		return nil, err
	}
	if p.alwaysReason != "" {
		// A shape refused at prepare time is unbindable by design: it can never
		// resolve to one shard for any parameter values, so refuse before binding
		// or routing rather than carrying a plan that routeWrite would reject.
		return nil, &PlanError{
			Table: p.table, Reason: p.alwaysReason, cause: ErrDistributedWriteUnsupported,
		}
	}
	bound := &BoundWritePlan{
		generation:   p.generation,
		table:        p.table,
		distribution: p.distribution,
		spec:         p.spec,
		manifest:     p.manifest,
		kind:         p.statement.Kind,
		keyPointers:  p.writeKeyPointers,
	}
	switch p.statement.Kind {
	case sqlast.KindInsert:
		keys, document, documents, err := p.bindInsertRowKeys(args)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrPlanParameters, err)
		}
		bound.rowKeys = keys
		bound.insertDoc = document
		bound.insertDocs = documents
		if err := p.bindGlobalIndexInserts(bound, args); err != nil {
			return nil, err
		}
	case sqlast.KindUpdate, sqlast.KindDelete:
		if p.constraints == nil {
			return nil, &PlanError{
				Table:  p.table,
				Reason: "write predicate was not compiled at prepare time",
				cause:  ErrDistributedWriteUnsupported,
			}
		}
		cons, err := p.constraints.Bind(args)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrPlanParameters, err)
		}
		bound.constraints = cons
		if p.statement.Kind == sqlast.KindUpdate {
			if len(p.statement.Update.Assignments) == 0 {
				doc, err := writeOperandDocument(p.statement.Update.Doc, args)
				if err != nil {
					return nil, fmt.Errorf("%w: %w", ErrPlanParameters, err)
				}
				bound.updateDoc = doc
			} else {
				bound.updateAssignments = p.statement.Update.Assignments
				bound.updateArgs = args
			}
		}
	}
	return bound, nil
}

func (p *PreparedPlan) bindGlobalIndexInserts(
	bound *BoundWritePlan,
	args []any,
) error {
	if len(p.writeGlobalIndexes) == 0 {
		return nil
	}
	rows := p.statement.Insert.Rows
	bound.globalIndexes = make([]boundGlobalIndexMutation, 0, len(rows)*len(p.writeGlobalIndexes))
	workspace := GlobalIndexWorkspace{locatorJSON: &bound.globalIndexLocatorJSON}
	var keyStorage [4]distribution.Scalar
	var locatorStorage [distribution.KeyspaceWidth]distribution.Scalar
	baseMapper := distribution.NewNativeMapperWithBucketBits(
		bound.spec.Arity, bound.spec.EffectiveBucketBits(),
	)
	for rowOrdinal := range rows {
		row := &rows[rowOrdinal]
		basePoint, err := baseMapper.PointFor(bound.rowKeys[rowOrdinal])
		if err != nil {
			return fmt.Errorf("global index base row %d: %w", rowOrdinal, err)
		}
		baseTarget, ok := bound.manifest.ResolvePointTarget(basePoint)
		if !ok {
			return &CatalogError{Reason: "global index base row maps outside active manifest"}
		}
		for indexOrdinal := range p.writeGlobalIndexes {
			prepared := &p.writeGlobalIndexes[indexOrdinal]
			var route GlobalIndexRoute
			if prepared.wholeDocument {
				document, docErr := writeOperandDocument(row.Values[0], args)
				if docErr != nil {
					return fmt.Errorf("global index row %d: %w", rowOrdinal, docErr)
				}
				route, err = prepared.program.RouteDocument(document, &workspace)
			} else {
				metadata := prepared.program.metadata
				key := keyStorage[:metadata.PathCount]
				locator := locatorStorage[:metadata.LocatorCount]
				for i := range key {
					key[i], err = writeScalarFromValue(writeOperandValue(
						row.Values[prepared.keyColumns[i]], args,
					))
					if err != nil {
						break
					}
				}
				if err == nil {
					for i := range locator {
						locator[i], err = writeScalarFromValue(writeOperandValue(
							row.Values[prepared.locatorColumns[i]], args,
						))
						if err != nil {
							break
						}
					}
				}
				if err == nil {
					route, err = prepared.program.RouteScalars(key, locator, &workspace)
				}
			}
			if err != nil {
				return fmt.Errorf(
					"global index %s row %d: %w",
					prepared.program.metadata.Name, rowOrdinal, err,
				)
			}
			if !sameWriteTarget(route.BaseTarget, baseTarget) {
				return &CatalogError{Reason: "global index locator does not identify the inserted base owner"}
			}
			mutation := boundGlobalIndexMutation{
				metadata:     prepared.program.metadata,
				kind:         shardservice.MutationGlobalIndexPut,
				distribution: prepared.program.indexSpec.Name,
				target:       route.IndexTarget, address: route.IndexAddress,
				routingVersion: prepared.program.indexManifest.Version(),
				bucketBits:     route.IndexBucketBits, scope: route.IndexScope,
				entryStart: len(bound.globalIndexArena),
			}
			bound.globalIndexArena = append(bound.globalIndexArena, route.EntryKey...)
			mutation.entryEnd = len(bound.globalIndexArena)
			mutation.valueStart = mutation.entryEnd
			bound.globalIndexArena = append(bound.globalIndexArena, route.LocatorValue...)
			mutation.valueEnd = len(bound.globalIndexArena)
			bound.globalIndexes = append(bound.globalIndexes, mutation)
		}
	}
	return nil
}

type capturedPrimaryExpectation struct {
	key             []byte
	digest          [sha256.Size]byte
	postimageDigest [sha256.Size]byte
}

// bindGlobalIndexCapture converts one base-shard before/post-image capture into
// a compact compare-and-maintain transaction. Documents are consumed
// immediately by the compiled vibejson programs; only native primary keys and
// SHA-256 before-image digests are retained in the durable base precondition.
// The shard-produced post-image is authoritative: the coordinator never
// re-evaluates an UPDATE SET list while deriving index mutations.
func (p *PreparedPlan) bindGlobalIndexCapture(
	bound *BoundWritePlan,
	baseTarget distribution.Target,
	rows [][]shardservice.Cell,
) error {
	if len(p.writeGlobalIndexes) == 0 {
		return nil
	}
	if bound == nil || (bound.kind != sqlast.KindUpdate && bound.kind != sqlast.KindDelete) {
		return ErrGlobalIndexMaintenanceUnsupported
	}
	firstProgram := &p.writeGlobalIndexes[0].program
	primaryPath := firstProgram.metadata.LocatorPaths[firstProgram.primary]
	if primaryPath == "" {
		return ErrGlobalIndexMaintenanceUnsupported
	}
	for i := range p.writeGlobalIndexes {
		program := &p.writeGlobalIndexes[i].program
		if program.metadata.LocatorPaths[program.primary] != primaryPath {
			return &CatalogError{Reason: "READY global indexes disagree on the base primary path"}
		}
	}
	bound.primaryPath = byteview.Bytes(primaryPath)
	expected := make([]capturedPrimaryExpectation, 0, len(rows))
	bound.globalIndexes = make(
		[]boundGlobalIndexMutation, 0, len(rows)*len(p.writeGlobalIndexes)*2,
	)
	var oldWorkspace, newWorkspace GlobalIndexWorkspace
	for rowOrdinal := range rows {
		row := rows[rowOrdinal]
		if len(row) != 3 || row[0].Null || row[1].Null || len(row[0].Bytes) == 0 ||
			len(row[1].Bytes) == 0 || !vibejson.Valid(row[1].Bytes) {
			return &PlanError{
				Table: p.table, Reason: "base shard returned a malformed mutation capture row",
				cause: ErrGlobalIndexMaintenanceUnsupported,
			}
		}
		var replacement []byte
		switch bound.kind {
		case sqlast.KindUpdate:
			if row[2].Null || len(row[2].Bytes) == 0 || !vibejson.Valid(row[2].Bytes) {
				return &PlanError{
					Table: p.table, Reason: "base shard returned a malformed UPDATE post-image",
					cause: ErrGlobalIndexMaintenanceUnsupported,
				}
			}
			replacement = row[2].Bytes
		case sqlast.KindDelete:
			if !row[2].Null || len(row[2].Bytes) != 0 {
				return &PlanError{
					Table: p.table, Reason: "base shard returned a DELETE post-image",
					cause: ErrGlobalIndexMaintenanceUnsupported,
				}
			}
		default:
			return ErrGlobalIndexMaintenanceUnsupported
		}
		expectation := capturedPrimaryExpectation{
			key: row[0].Bytes, digest: sha256.Sum256(row[1].Bytes),
		}
		if bound.kind == sqlast.KindUpdate {
			expectation.postimageDigest = sha256.Sum256(replacement)
		}
		expected = append(expected, expectation)
		oldIndex, err := oldWorkspace.indexDocument(row[1].Bytes)
		if err != nil {
			return err
		}
		var newIndex vibejson.Index
		if bound.kind == sqlast.KindUpdate {
			newIndex, err = newWorkspace.indexDocument(replacement)
			if err != nil {
				return err
			}
		}
		for indexOrdinal := range p.writeGlobalIndexes {
			prepared := &p.writeGlobalIndexes[indexOrdinal]
			oldRoute, err := prepared.program.routeIndexedDocument(oldIndex, len(row[1].Bytes), &oldWorkspace)
			if err != nil {
				return fmt.Errorf("global index %s captured row %d: %w",
					prepared.program.metadata.Name, rowOrdinal, err)
			}
			if !sameWriteTarget(oldRoute.BaseTarget, baseTarget) ||
				!bytes.Equal(oldRoute.BasePrimaryKey, row[0].Bytes) {
				return &CatalogError{Reason: "global index locator does not identify the captured base row"}
			}
			if bound.kind == sqlast.KindDelete {
				bound.appendGlobalIndexRoute(
					prepared, oldRoute,
					shardservice.MutationGlobalIndexDelete,
				)
				continue
			}
			newRoute, err := prepared.program.routeIndexedDocument(newIndex, len(replacement), &newWorkspace)
			if err != nil {
				return fmt.Errorf("global index %s replacement row %d: %w",
					prepared.program.metadata.Name, rowOrdinal, err)
			}
			if !sameWriteTarget(newRoute.BaseTarget, baseTarget) ||
				!bytes.Equal(newRoute.BasePrimaryKey, row[0].Bytes) {
				return &CatalogError{Reason: "global index replacement locator does not identify the captured base row"}
			}
			if bytes.Equal(oldRoute.EntryKey, newRoute.EntryKey) &&
				bytes.Equal(oldRoute.LocatorValue, newRoute.LocatorValue) {
				continue
			}
			bound.appendGlobalIndexRoute(
				prepared, oldRoute,
				shardservice.MutationGlobalIndexDelete,
			)
			bound.appendGlobalIndexRoute(
				prepared, newRoute,
				shardservice.MutationGlobalIndexPut,
			)
		}
	}
	slices.SortFunc(expected, func(a, b capturedPrimaryExpectation) int {
		return bytes.Compare(a.key, b.key)
	})
	bound.expectedKeys = make([][]byte, len(expected))
	bound.expectedDigests = make([][sha256.Size]byte, len(expected))
	if bound.kind == sqlast.KindUpdate {
		bound.postimageKeys = make([][]byte, len(expected))
		bound.postimageDigests = make([][sha256.Size]byte, len(expected))
	}
	for i := range expected {
		if i != 0 && bytes.Equal(expected[i-1].key, expected[i].key) {
			return &PlanError{
				Table: p.table, Reason: "base shard returned duplicate captured primary keys",
				cause: ErrGlobalIndexMaintenanceUnsupported,
			}
		}
		bound.expectedKeys[i] = expected[i].key
		bound.expectedDigests[i] = expected[i].digest
		if bound.kind == sqlast.KindUpdate {
			bound.postimageKeys[i] = expected[i].key
			bound.postimageDigests[i] = expected[i].postimageDigest
		}
	}
	return nil
}

func (b *BoundWritePlan) appendGlobalIndexRoute(
	prepared *preparedGlobalIndex,
	route GlobalIndexRoute,
	kind shardservice.MutationKind,
) {
	mutation := boundGlobalIndexMutation{
		metadata: prepared.program.metadata, kind: kind,
		distribution: prepared.program.indexSpec.Name,
		target:       route.IndexTarget, address: route.IndexAddress,
		routingVersion: prepared.program.indexManifest.Version(),
		bucketBits:     route.IndexBucketBits, scope: route.IndexScope,
		entryStart: len(b.globalIndexArena),
	}
	b.globalIndexArena = append(b.globalIndexArena, route.EntryKey...)
	mutation.entryEnd = len(b.globalIndexArena)
	mutation.valueStart = mutation.entryEnd
	b.globalIndexArena = append(b.globalIndexArena, route.LocatorValue...)
	mutation.valueEnd = len(b.globalIndexArena)
	b.globalIndexes = append(b.globalIndexes, mutation)
}

func sameWriteTarget(a, b distribution.Target) bool {
	return a.Shard == b.Shard &&
		a.AllocationGeneration == b.AllocationGeneration &&
		a.OwnershipEpoch == b.OwnershipEpoch && a.Endpoint == b.Endpoint &&
		a.Role == b.Role
}

// bindInsertRowKeys extracts one full shard key per VALUES row. A whole-document
// row parses the document once and reads every shard-key pointer out of it; a
// flat row reads each shard-key ordinal from the bound operand of its named
// top-level column. A missing, null, or non-scalar shard-key value is a routing
// error: the row cannot be proven single-shard.
func (p *PreparedPlan) bindInsertRowKeys(
	args []any,
) ([][]distribution.Scalar, []byte, [][]byte, error) {
	ins := p.statement.Insert
	if ins == nil {
		return nil, nil, nil, errors.New("insert has no statement body")
	}
	keys := make([][]distribution.Scalar, len(ins.Rows))
	var doc, singleDocument []byte
	var documents [][]byte
	var indexEntries []vibejson.IndexEntry
	if len(ins.Columns) == 0 && len(ins.Rows) > 1 {
		documents = make([][]byte, len(ins.Rows))
	}
	for i, row := range ins.Rows {
		var (
			key []distribution.Scalar
			err error
		)
		if len(ins.Columns) == 0 {
			doc, err = writeOperandDocument(row.Values[0], args)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("row %d: %w", i, err)
			}
			if len(ins.Rows) == 1 {
				singleDocument = doc
			} else {
				documents[i] = doc
			}
			key, indexEntries, err = writeDocShardKeyWorkspace(doc, p.writeKeyPointers, indexEntries)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("row %d: %w", i, err)
			}
		} else {
			key = make([]distribution.Scalar, 0, len(p.writeKeyColumns))
			for ordinal, colIdx := range p.writeKeyColumns {
				value := writeOperandValue(row.Values[colIdx], args)
				scalar, err := writeScalarFromValue(value)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("row %d: shard-key column ordinal %d: %w", i, ordinal, err)
				}
				key = append(key, scalar)
			}
		}
		keys[i] = key
	}
	return keys, singleDocument, documents, nil
}

// writeOperandDocument returns the JSON document bytes an insert operand
// carries: the bound document parameter for a placeholder, or the operand's
// literal text for an inline literal.
func writeOperandDocument(op sqlast.Operand, args []any) ([]byte, error) {
	switch op.Kind {
	case sqlast.OperandParam:
		if op.Ordinal < 0 || op.Ordinal >= len(args) {
			return nil, fmt.Errorf("parameter %d is out of range", op.Ordinal+1)
		}
		switch value := args[op.Ordinal].(type) {
		case vibejson.RawValue:
			if err := vibejson.Validate(value.Bytes()); err != nil {
				return nil, fmt.Errorf("document parameter is invalid JSON: %w", err)
			}
			return value.Bytes(), nil
		case []byte:
			if err := vibejson.Validate(value); err != nil {
				return nil, fmt.Errorf("document parameter is invalid JSON: %w", err)
			}
			return value, nil
		case string:
			document := byteview.Bytes(value)
			if err := vibejson.Validate(document); err != nil {
				return nil, fmt.Errorf("document parameter is invalid JSON: %w", err)
			}
			return document, nil
		default:
			return nil, errors.New("document parameter is not a JSON document")
		}
	case sqlast.OperandString, sqlast.OperandJSON:
		document := byteview.Bytes(op.Text)
		if err := vibejson.Validate(document); err != nil {
			return nil, fmt.Errorf("document literal is invalid JSON: %w", err)
		}
		return document, nil
	default:
		return nil, fmt.Errorf("operand kind %d is not a JSON document", op.Kind)
	}
}

// writeOperandValue materializes one insert operand in the same byte-native
// vocabulary used by shardservice: unquoted strings are []byte and exact
// numbers/JSON literals are vibejson.RawValue.
func writeOperandValue(op sqlast.Operand, args []any) any {
	switch op.Kind {
	case sqlast.OperandParam:
		if op.Ordinal < 0 || op.Ordinal >= len(args) {
			return nil
		}
		return args[op.Ordinal]
	case sqlast.OperandString:
		return byteview.Bytes(op.Text)
	case sqlast.OperandNumber:
		return vibejson.RawValue{Src: byteview.Bytes(op.Text)}
	case sqlast.OperandJSON:
		return vibejson.RawValue{Src: byteview.Bytes(op.Text)}
	case sqlast.OperandBool:
		return op.Bool
	default:
		return nil
	}
}

// writeScalarFromValue converts a bound shard-key value to a placement scalar.
// It accepts the closed gateway parameter vocabulary — UTF-8 bytes and a
// vibejson exact number —
// and refuses every other value, so a non-scalar shard key never narrows or
// misroutes a write.
func writeScalarFromValue(value any) (distribution.Scalar, error) {
	switch v := value.(type) {
	case []byte:
		return distribution.NewString(byteview.String(v)), nil
	case string:
		return distribution.NewString(v), nil
	case vibejson.RawValue:
		number, ok := v.NumberBytes()
		if !ok {
			return distribution.Scalar{}, errors.New("shard-key value is not a JSON number")
		}
		return distribution.NewNumber(byteview.String(number))
	default:
		return distribution.Scalar{}, errors.New("shard-key value is not a string or number")
	}
}

// writeDocShardKey reads every shard-key pointer out of one JSON document,
// mirroring the driver's primary-key extraction: a missing, null, or
// non-scalar value is a routing error.
func writeDocShardKey(
	doc []byte,
	pointers []vibejson.CompiledPointer,
) ([]distribution.Scalar, error) {
	key, _, err := writeDocShardKeyWorkspace(doc, pointers, nil)
	return key, err
}

func writeDocShardKeyWorkspace(
	doc []byte,
	pointers []vibejson.CompiledPointer,
	entries []vibejson.IndexEntry,
) ([]distribution.Scalar, []vibejson.IndexEntry, error) {
	needed, err := vibejson.RequiredIndexEntries(doc)
	if err != nil {
		return nil, entries, fmt.Errorf("invalid JSON document: %w", err)
	}
	if cap(entries) < needed {
		entries = make([]vibejson.IndexEntry, needed)
	} else {
		entries = entries[:needed]
	}
	index, err := vibejson.BuildIndex(doc, entries)
	if err != nil {
		return nil, entries, fmt.Errorf("invalid JSON document: %w", err)
	}
	root := index.Root()
	key := make([]distribution.Scalar, 0, len(pointers))
	var scratch []byte
	for i, ptr := range pointers {
		node, found, err := root.PointerCompiled(ptr)
		if err != nil {
			return nil, entries, fmt.Errorf("shard-key column %d: %w", i, err)
		}
		if !found {
			return nil, entries, fmt.Errorf("shard-key column %d is missing", i)
		}
		value := node.Raw()
		if value.IsNull() {
			return nil, entries, fmt.Errorf("shard-key column %d is null", i)
		}
		switch value.Kind() {
		case jsondoc.String:
			if text, ok := value.StringBytes(); ok {
				key = append(key, distribution.NewString(byteview.String(text)))
				break
			}
			if cap(scratch) < len(doc) {
				scratch = make([]byte, 0, len(doc))
			}
			start := len(scratch)
			scratch, ok, err := value.AppendText(scratch)
			if err != nil {
				return nil, entries, fmt.Errorf("shard-key column %d has an invalid JSON string: %w", i, err)
			}
			if !ok {
				return nil, entries, fmt.Errorf("shard-key column %d has an invalid JSON string", i)
			}
			key = append(key, distribution.NewString(byteview.String(scratch[start:])))
		case jsondoc.Number:
			number, ok := value.NumberText()
			if !ok {
				return nil, entries, fmt.Errorf("shard-key column %d is not a valid JSON number", i)
			}
			scalar, err := distribution.NewNumber(number)
			if err != nil {
				return nil, entries, fmt.Errorf("shard-key column %d: %w", i, err)
			}
			key = append(key, scalar)
		default:
			return nil, entries, fmt.Errorf("shard-key column %d must be a JSON string or number", i)
		}
	}
	return key, entries, nil
}

// updateMayChangeGlobalIndex proves maintenance unnecessary only from the SET
// targets, never from selectivity estimates. A top-level assignment can alter
// every descendant pointer. Locator paths include the primary and shard keys.
func updateMayChangeGlobalIndex(stmt *sqlast.Statement, metadata IndexMetadata) bool {
	if stmt == nil || stmt.Kind != sqlast.KindUpdate || stmt.Update == nil || len(stmt.Update.Assignments) == 0 {
		return true
	}
	overlaps := func(path string) bool {
		if path == "" || path[0] != '/' {
			return true
		}
		root := path[1:]
		if end := strings.IndexByte(root, '/'); end >= 0 {
			root = root[:end]
		}
		root = strings.ReplaceAll(strings.ReplaceAll(root, "~1", "/"), "~0", "~")
		for _, assignment := range stmt.Update.Assignments {
			if assignment.Column == root {
				return true
			}
		}
		return false
	}
	for _, path := range metadata.Paths[:metadata.PathCount] {
		if overlaps(path) {
			return true
		}
	}
	for _, path := range metadata.LocatorPaths[:metadata.LocatorCount] {
		if overlaps(path) {
			return true
		}
	}
	return false
}
