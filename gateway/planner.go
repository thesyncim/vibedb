package gateway

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/thesyncim/vibedb/distribution"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// ErrTableNotPlaced reports a physical table with no distributed placement in
// the pinned catalog generation.
var ErrTableNotPlaced = errors.New("gateway: table has no distributed placement")

// ErrDistributedPlanUnsupported reports a SQL shape that cannot yet be merged
// correctly across more than one shard. The planner fails before dispatch; it
// never substitutes a plausible but semantically incomplete merge.
var ErrDistributedPlanUnsupported = errors.New("gateway: distributed query plan is unsupported")

// ErrPlanParameters reports a mismatch or invalid value while binding a
// compiled distributed plan.
var ErrPlanParameters = errors.New("gateway: invalid distributed plan parameters")

// PlanError is a typed planner refusal. It wraps one of the planner sentinels
// above and carries a stable diagnostic without exposing parser internals.
type PlanError struct {
	Table  string
	Reason string
	cause  error
}

func (e *PlanError) Error() string {
	if e.Table != "" {
		return fmt.Sprintf("gateway: plan table %q: %s", e.Table, e.Reason)
	}
	return "gateway: plan: " + e.Reason
}

func (e *PlanError) Unwrap() error { return e.cause }

// PreparedPlan is one immutable compile-once distributed SQL plan pinned to a
// catalog generation. It owns the parser arena backing statement and routing,
// so it can be shared across concurrent binds until the generation is retired.
// The wire still carries SQL rather than this object; shards parse and plan
// independently.
type PreparedPlan struct {
	parser    sqlast.Parser
	statement sqlast.Statement

	generation      uint64
	table           string
	tables          []string
	distribution    distribution.DistributionName
	spec            distribution.DistributionSpec
	manifest        *distribution.Manifest
	constraints     *sqldriver.ConstraintProgram
	statConstraints *sqldriver.ConstraintProgram
	statPaths       []string
	groupPaths      []string
	groupLocal      bool
	order           []OrderKey
	limit           *sqlast.Operand
	offset          *sqlast.Operand
	aggregates      []sqlast.AggKind
	groupKeys       []int
	aggHeaders      []string
	params          int
	alwaysReason    string
	emptyReason     string
	multiReason     string

	// writeKeyPointers holds one compiled shard-key pointer per ordinal for a
	// whole-document insert or a whole-document UPDATE; it is nil for every
	// other shape.
	writeKeyPointers []vibejson.CompiledPointer
	// writeKeyColumns maps each shard-key ordinal to the named top-level insert
	// column index that supplies it; it is set only for a flat single-document
	// INSERT.
	writeKeyColumns []int
	// writeGlobalIndexes contains every maintained global-index incarnation:
	// Building, CatchingUp, Ready, and Draining. Local indexes add no
	// prepared-plan footprint.
	writeGlobalIndexes []preparedGlobalIndex
	// readGlobalIndexes contains finite-domain routing programs for READY global
	// indexes on the driving table. Bind selects at most one complete key domain;
	// the ordinary base-shard route remains preferred when it is already a point.
	readGlobalIndexes []preparedGlobalIndexRead
}

type preparedGlobalIndexRead struct {
	program     GlobalIndexProgram
	constraints *sqldriver.ConstraintProgram
}

type boundGlobalIndexRead struct {
	program     GlobalIndexProgram
	constraints distribution.BoundConstraints
}

// BoundPlan is the immutable execution-specific result of binding typed
// parameters to a PreparedPlan. Its canonical ordinal domains and merge
// instructions stay private so callers cannot mutate a cached plan's shared
// ORDER BY metadata.
type BoundPlan struct {
	generation      uint64
	table           string
	tables          []string
	distribution    distribution.DistributionName
	constraints     distribution.BoundConstraints
	statConstraints distribution.BoundConstraints
	statPaths       []string
	groupPaths      []string
	groupLocal      bool
	order           []OrderKey
	limit           int
	offset          int
	hasLimit        bool
	aggregates      []sqlast.AggKind
	groupKeys       []int
	aggHeaders      []string
	globalIndex     *boundGlobalIndexRead
	globalEmpty     bool

	spec         distribution.DistributionSpec
	manifest     *distribution.Manifest
	alwaysReason string
	emptyReason  string
	multiReason  string
}

// The plan cache is deliberately per immutable catalog generation: a hit can
// never observe placement or manifest metadata from another generation. It is
// lazy and direct-mapped, so an unused catalog pays zero cache storage and a
// busy one retains a hard-bounded 8 KiB pointer directory rather than a map
// whose cardinality follows attacker-controlled SQL text. A randomized hash
// makes deliberate collision sets impractical; exact string comparison keeps
// collisions semantically harmless.
const preparedPlanCacheSlots = 1024

// maxCachedSQLBytes bounds both cloned cache keys and parser arenas retained by
// adversarial unique statements. Larger statements still compile and execute;
// they simply do not become generation-lifetime cache residents.
const maxCachedSQLBytes = 4 << 10

type preparedPlanCache struct {
	entries [preparedPlanCacheSlots]atomic.Pointer[cachedPreparedPlan]
}

type cachedPreparedPlan struct {
	hash uint64
	sql  string
	plan *PreparedPlan
}

func (s *Snapshot) cachedPreparedPlan(sqlText string) (*PreparedPlan, uint64) {
	hash := maphash.String(s.planSeed, sqlText)
	cache := s.planCache.Load()
	if cache == nil {
		return nil, hash
	}
	entry := cache.entries[hash&(preparedPlanCacheSlots-1)].Load()
	if entry != nil && entry.hash == hash && entry.sql == sqlText {
		return entry.plan, hash
	}
	return nil, hash
}

// sqlText must be the bounded owned source passed to this plan's parser. Share
// that same allocation with the cache key rather than cloning a second copy.
func (s *Snapshot) cachePreparedPlan(sqlText string, hash uint64, plan *PreparedPlan) *PreparedPlan {
	cache := s.planCache.Load()
	if cache == nil {
		candidate := new(preparedPlanCache)
		if s.planCache.CompareAndSwap(nil, candidate) {
			cache = candidate
		} else {
			cache = s.planCache.Load()
		}
	}
	slot := &cache.entries[hash&(preparedPlanCacheSlots-1)]
	if current := slot.Load(); current != nil && current.hash == hash && current.sql == sqlText {
		return current.plan
	}
	entry := &cachedPreparedPlan{hash: hash, sql: sqlText, plan: plan}
	slot.Store(entry)
	return plan
}

// Prepare compiles SQL against this immutable catalog generation. The planner
// accepts a physical driving table, proves shard-key colocation for supported
// joins, and records route-dependent semantic gates checked after binding.
func (s *Snapshot) Prepare(ctx context.Context, sqlText string) (*PreparedPlan, error) {
	if s == nil {
		return nil, ErrNoCatalog
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	cacheable := len(sqlText) <= maxCachedSQLBytes
	var hash uint64
	if cacheable {
		var cached *PreparedPlan
		cached, hash = s.cachedPreparedPlan(sqlText)
		if cached != nil {
			return cached, nil
		}
		// Ingress SQL may alias a much larger Scanner buffer or escape arena.
		// The parser retains its source as well as AST spans, so detach once on
		// a cacheable miss, before parsing. Cache hits remain allocation-free.
		sqlText = strings.Clone(sqlText)
	}
	plan := &PreparedPlan{generation: s.Generation()}
	if ctx != nil {
		plan.parser.SetCancellationCheck(ctx.Err)
	}
	err := plan.parser.ParseStatement(&plan.statement, sqlText)
	// PreparedPlan owns parser-backed AST storage for its lifetime, but it must
	// not retain a request context after parsing or call it during concurrent
	// binds and cache hits.
	plan.parser.SetCancellationCheck(nil)
	if err != nil {
		return nil, err
	}
	if plan.statement.Kind != sqlast.KindSelect || plan.statement.Select == nil {
		// A mutating statement is planned for the distributed write path: it is
		// admitted only when its shape is provably single-shard, refused before
		// any dispatch otherwise.
		if err := s.prepareWrite(plan, sqlText); err != nil {
			return nil, err
		}
		if cacheable {
			return s.cachePreparedPlan(sqlText, hash, plan), nil
		}
		return plan, nil
	}
	selectStmt := plan.statement.Select
	if err := validatePlanPhysicalTables(s, selectStmt, make(map[*sqlast.SelectStmt]struct{})); err != nil {
		return nil, err
	}
	if len(selectStmt.From) == 0 || selectStmt.From[0].Kind != sqlast.RelationCollection {
		return nil, &PlanError{
			Reason: "a physical driving table is required",
			cause:  ErrDistributedPlanUnsupported,
		}
	}
	plan.table = selectStmt.From[0].Name
	plan.tables = append(plan.tables, plan.table)
	placement, spec, manifest, ok := s.plannerTableFor(plan.table)
	if !ok {
		return nil, &PlanError{
			Table: plan.table, Reason: "no placement in pinned catalog generation",
			cause: ErrTableNotPlaced,
		}
	}
	plan.distribution = placement.Distribution
	plan.spec = spec
	plan.manifest = manifest
	plan.params = plan.statement.Params()
	plan.constraints = sqldriver.CompileConstraintProgram(placement.Columns, selectStmt.Where)
	if stats, ok := s.Statistics(plan.table); ok {
		plan.statPaths = appendStatisticsPredicatePaths(nil, stats.AppendColumnPaths(nil), selectStmt.Where)
		if len(plan.statPaths) != 0 {
			plan.statConstraints = sqldriver.CompileConstraintProgram(plan.statPaths, selectStmt.Where)
		}
	}
	// Group statistics are valid only for base-table keys, before any join
	// can change their domain or introduce NULL-extended values.
	if len(selectStmt.From) == 1 {
		for _, path := range selectStmt.GroupBy {
			if path == nil || path.Source != 0 || path.MergedUsing != 0 {
				plan.groupPaths = nil
				break
			}
			plan.groupPaths = append(plan.groupPaths, string(path.AppendPointer(nil)))
		}
	}
	if len(plan.groupPaths) != 0 {
		plan.groupLocal = true
		for _, path := range placement.Columns {
			if !slices.Contains(plan.groupPaths, path) {
				plan.groupLocal = false
				break
			}
		}
	}
	if err := s.prepareGlobalIndexReads(plan, selectStmt.Where); err != nil {
		return nil, err
	}
	placements := make([]distribution.TablePlacement, len(selectStmt.From))
	placements[0] = placement

	for i := 1; i < len(selectStmt.From); i++ {
		relation := selectStmt.From[i]
		if relation.Kind != sqlast.RelationCollection {
			plan.alwaysReason = firstPlanReason(plan.alwaysReason,
				"derived and CTE joins require a distributed relation plan")
			continue
		}
		joined, _, _, ok := s.plannerTableFor(relation.Name)
		if !ok {
			return nil, &PlanError{
				Table: relation.Name, Reason: "joined table has no placement",
				cause: ErrTableNotPlaced,
			}
		}
		if joined.Distribution != plan.distribution {
			return nil, &PlanError{
				Table: relation.Name,
				Reason: fmt.Sprintf("distribution %q is not colocated with %q",
					joined.Distribution, plan.distribution),
				cause: ErrDistributedPlanUnsupported,
			}
		}
		if (placement.AffinityGroup != "" || joined.AffinityGroup != "") &&
			joined.AffinityGroup != placement.AffinityGroup {
			return nil, &PlanError{
				Table: relation.Name,
				Reason: fmt.Sprintf("affinity group %q is not colocated with %q",
					joined.AffinityGroup, placement.AffinityGroup),
				cause: ErrDistributedPlanUnsupported,
			}
		}
		plan.tables = append(plan.tables, relation.Name)
		placements[i] = joined
		if relation.Join != sqlast.JoinInner && relation.Join != sqlast.JoinLeft {
			plan.alwaysReason = firstPlanReason(plan.alwaysReason,
				"RIGHT, FULL, and CROSS joins require an all-shard relation plan")
			continue
		}
		if !joinProvesColocation(relation.On, i, placements) {
			plan.alwaysReason = firstPlanReason(plan.alwaysReason,
				"join does not equate every colocated shard-key ordinal")
		}
	}

	plan.order, plan.multiReason = planOrder(selectStmt, plan.multiReason)
	plan.aggregates, plan.groupKeys, plan.aggHeaders, plan.multiReason =
		planDistributedAggregates(selectStmt, plan.multiReason)
	plan.limit = selectStmt.Limit
	plan.offset = selectStmt.Offset
	plan.alwaysReason = allRouteSemanticBoundary(selectStmt, plan.alwaysReason)
	plan.multiReason = multiShardSemanticBoundary(
		selectStmt, plan.multiReason, len(plan.aggregates) != 0, len(plan.groupKeys) != 0,
	)
	plan.emptyReason = emptyRouteSemanticBoundary(selectStmt, len(plan.aggregates) != 0,
		len(plan.groupKeys) != 0, plan.multiReason)
	if cacheable {
		return s.cachePreparedPlan(sqlText, hash, plan), nil
	}
	return plan, nil
}

// Bind applies params to a prepared plan without reparsing SQL or looking up
// metadata. args use the gateway's closed runtime scalar vocabulary. Exact
// wire numbers arrive as vibejson.RawValue.
func (p *PreparedPlan) Bind(args []any) (*BoundPlan, error) {
	if p == nil || p.constraints == nil || p.manifest == nil {
		return nil, &PlanError{Reason: "incomplete prepared plan", cause: ErrDistributedPlanUnsupported}
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
	constraints, err := p.constraints.Bind(args)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPlanParameters, err)
	}
	var statConstraints distribution.BoundConstraints
	if p.statConstraints != nil {
		statConstraints, err = p.statConstraints.Bind(args)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrPlanParameters, err)
		}
	}
	limit, err := bindPlanLimit(p.limit, args)
	if err != nil {
		return nil, err
	}
	offset, err := bindPlanOperand(p.offset, args, "OFFSET")
	if err != nil {
		return nil, err
	}
	var globalIndex *boundGlobalIndexRead
	var globalEmpty bool
	if !boundConstraintsSinglePoint(constraints, p.spec.Arity) {
		globalIndex, globalEmpty, err = p.bindGlobalIndexRead(args)
		if err != nil {
			return nil, err
		}
	}
	return &BoundPlan{
		generation: p.generation, table: p.table,
		tables:          p.tables,
		statConstraints: statConstraints, statPaths: p.statPaths, groupPaths: p.groupPaths, groupLocal: p.groupLocal,
		distribution: p.distribution, constraints: constraints,
		order: p.order, limit: limit, offset: offset, hasLimit: p.limit != nil,
		aggregates: p.aggregates, groupKeys: p.groupKeys,
		aggHeaders:  p.aggHeaders,
		globalIndex: globalIndex, globalEmpty: globalEmpty,
		spec: p.spec, manifest: p.manifest,
		alwaysReason: p.alwaysReason, emptyReason: p.emptyReason, multiReason: p.multiReason,
	}, nil
}

func validateGatewayBindArgs(args []any) error {
	for ordinal, value := range args {
		switch value.(type) {
		case nil, bool, string, []byte, vibejson.RawValue,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64:
			continue
		default:
			return fmt.Errorf(
				"%w: parameter %d has unsupported gateway type %T",
				ErrPlanParameters, ordinal+1, value,
			)
		}
	}
	return nil
}

func boundConstraintsSinglePoint(
	constraints distribution.BoundConstraints,
	arity int,
) bool {
	if len(constraints) != arity {
		return false
	}
	for i := range constraints {
		domain := constraints[i]
		if domain.Kind != distribution.DomainFinite || len(domain.Values) != 1 {
			return false
		}
	}
	return true
}

func (s *Snapshot) prepareGlobalIndexReads(
	plan *PreparedPlan,
	where *sqlast.Expr,
) error {
	indexes := s.Indexes(plan.table)
	for ordinal := 0; ordinal < indexes.Len(); ordinal++ {
		metadata, _ := indexes.At(ordinal)
		if !metadata.Global() || !metadata.Ready() {
			continue
		}
		program, err := s.CompileGlobalIndex(plan.table, metadata.Name)
		if err != nil {
			return err
		}
		paths := metadata.Paths[:metadata.PathCount]
		plan.readGlobalIndexes = append(plan.readGlobalIndexes, preparedGlobalIndexRead{
			program: program, constraints: sqldriver.CompileConstraintProgram(paths, where),
		})
	}
	return nil
}

func (p *PreparedPlan) bindGlobalIndexRead(
	args []any,
) (*boundGlobalIndexRead, bool, error) {
	var best *boundGlobalIndexRead
	bestCost := math.MaxFloat64
	for i := range p.readGlobalIndexes {
		candidate := &p.readGlobalIndexes[i]
		constraints, err := candidate.constraints.Bind(args)
		if err != nil {
			return nil, false, fmt.Errorf("%w: %w", ErrPlanParameters, err)
		}
		if len(constraints) == 0 || len(constraints) > 4 {
			continue
		}
		complete := true
		for _, domain := range constraints {
			if domain.Kind == distribution.DomainEmpty {
				return nil, true, nil
			}
			if domain.Kind != distribution.DomainFinite || len(domain.Values) == 0 {
				complete = false
			}
		}
		if !complete {
			continue
		}
		cost := globalIndexAccessCost(candidate.program, constraints)
		if best == nil || cost < bestCost {
			best = &boundGlobalIndexRead{program: candidate.program, constraints: constraints}
			bestCost = cost
		}
	}
	return best, false, nil
}

// ValidateRoute refuses a cross-shard route whose result semantics the current
// merge layer cannot prove. Single-shard execution needs no distributed merge
// and retains the full local SQL surface.
func (p *BoundPlan) ValidateRoute(route distribution.Route) error {
	if p == nil {
		return &PlanError{Reason: "nil bound plan", cause: ErrDistributedPlanUnsupported}
	}
	if p.alwaysReason != "" {
		return &PlanError{Table: p.table, Reason: p.alwaysReason, cause: ErrDistributedPlanUnsupported}
	}
	if len(route.Targets) == 0 {
		if p.emptyReason != "" {
			return &PlanError{Table: p.table, Reason: p.emptyReason, cause: ErrDistributedPlanUnsupported}
		}
		return nil
	}
	if len(route.Targets) <= 1 || p.multiReason == "" {
		return nil
	}
	return &PlanError{Table: p.table, Reason: p.multiReason, cause: ErrDistributedPlanUnsupported}
}

// emptyRouteSemanticBoundary separates route pruning from SQL cardinality.
// Ordinary scans and grouped aggregates over no shards are empty, while an
// ungrouped global aggregate still produces one identity row. Only an exact
// algebraic program handled by emptyAggregateResult may use either shortcut;
// every other aggregate shape fails closed instead of erasing SQL semantics.
func emptyRouteSemanticBoundary(
	stmt *sqlast.SelectStmt,
	aggregateSupported bool,
	groupedSupported bool,
	multiReason string,
) string {
	if stmt == nil || !selectHasAggregate(stmt) {
		return ""
	}
	if aggregateSupported && stmt.Having == nil && len(stmt.Windows) == 0 &&
		(groupedSupported || (!stmt.Distinct && stmt.Offset == nil && stmt.Limit == nil)) {
		return ""
	}
	if multiReason != "" {
		return multiReason
	}
	return "aggregate over an empty route requires coordinator evaluation"
}

func selectHasAggregate(stmt *sqlast.SelectStmt) bool {
	for i := range stmt.Columns {
		if stmt.Columns[i].Agg != sqlast.AggNone || scalarHasAggregate(stmt.Columns[i].Scalar) {
			return true
		}
	}
	return false
}

// joinProvesColocation accepts a newly joined physical relation only when each
// of its shard-key ordinals is equated to the same ordinal of an already proven
// colocated input. Hash/range placement then guarantees every matching pair is
// resident on the same shard, so shard-local joins plus a gateway merge are
// globally complete for INNER and LEFT joins.
func joinProvesColocation(on *sqlast.JoinCond, source int, placements []distribution.TablePlacement) bool {
	if on == nil || source <= 0 || source >= len(placements) {
		return false
	}
	joined := placements[source]
	for ordinal, column := range joined.Columns {
		matched := false
		for _, key := range on.Keys {
			if key.Right == nil || key.Right.Source != source ||
				!vibejson.BytesEqualString(key.Right.AppendPointer(nil), column) || key.Left == nil ||
				key.Left.Source < 0 || key.Left.Source >= source {
				continue
			}
			left := placements[key.Left.Source]
			if ordinal < len(left.Columns) &&
				vibejson.BytesEqualString(key.Left.AppendPointer(nil), left.Columns[ordinal]) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return len(joined.Columns) != 0
}

func planOrder(stmt *sqlast.SelectStmt, reason string) ([]OrderKey, string) {
	if len(stmt.OrderBy) == 0 {
		return nil, reason
	}
	order := make([]OrderKey, len(stmt.OrderBy))
	for i, term := range stmt.OrderBy {
		output := term.Output - 1
		if term.Output == 0 {
			output = projectedPathIndex(stmt.Columns, term.Path)
		}
		if output < 0 || output >= len(stmt.Columns) {
			return nil, firstPlanReason(reason,
				"ORDER BY keys outside the projected row require hidden merge columns")
		}
		order[i] = OrderKey{Column: output, Desc: term.Desc}
	}
	return order, reason
}

func projectedPathIndex(columns []sqlast.ResultColumn, want *sqlast.PathExpr) int {
	for i := range columns {
		if samePlanPath(columns[i].Path, want) {
			return i
		}
	}
	return -1
}

func samePlanPath(a, b *sqlast.PathExpr) bool {
	if a == nil || b == nil || a.Source != b.Source || a.MergedUsing != b.MergedUsing ||
		len(a.Segments) != len(b.Segments) {
		return false
	}
	for i := range a.Segments {
		if a.Segments[i] != b.Segments[i] {
			return false
		}
	}
	return true
}

func multiShardSemanticBoundary(
	stmt *sqlast.SelectStmt,
	reason string,
	aggregateSupported bool,
	groupedAggregateSupported bool,
) string {
	switch {
	case stmt.Distinct && !groupedAggregateSupported:
		return firstPlanReason(reason, "DISTINCT requires a bounded global deduplicator")
	case len(stmt.GroupBy) != 0 && !groupedAggregateSupported:
		return firstPlanReason(reason, "GROUP BY requires a partial-aggregate combiner")
	case stmt.Having != nil:
		return firstPlanReason(reason, "HAVING requires global aggregate evaluation")
	case len(stmt.Windows) != 0:
		return firstPlanReason(reason, "window functions require global partition planning")
	case stmt.Offset != nil && !groupedAggregateSupported:
		return firstPlanReason(reason, "OFFSET requires distributed top-k rewriting")
	case aggregateSupported && !groupedAggregateSupported && stmt.Limit != nil:
		return firstPlanReason(reason, "aggregate LIMIT requires coordinator-side rewriting")
	}
	if aggregateSupported {
		return reason
	}
	for i := range stmt.Columns {
		if stmt.Columns[i].Agg != sqlast.AggNone {
			return firstPlanReason(reason, "aggregates require an algebraic partial-aggregate combiner")
		}
		if scalarHasAggregate(stmt.Columns[i].Scalar) {
			return firstPlanReason(reason, "scalar aggregates require an algebraic partial-aggregate combiner")
		}
		if stmt.Columns[i].Window != nil {
			return firstPlanReason(reason, "window functions require global partition planning")
		}
	}
	return reason
}

// planDistributedAggregates recognizes the algebraic aggregate subset whose
// shard-local states can be merged exactly without rewriting the SQL fragment:
// COUNT by addition, SUM by exact-decimal addition, and MIN/MAX by exact value
// comparison. AVG needs SUM+COUNT state and remains refused until fragment SQL
// projection is available.
func planDistributedAggregates(
	stmt *sqlast.SelectStmt,
	reason string,
) ([]sqlast.AggKind, []int, []string, string) {
	if stmt == nil || len(stmt.Columns) == 0 {
		return nil, nil, nil, reason
	}
	grouped := len(stmt.GroupBy) != 0
	groupKeys := make([]int, 0, len(stmt.GroupBy))
	for key := range stmt.GroupBy {
		projectedKey := false
		for column := range stmt.Columns {
			projected := &stmt.Columns[column]
			if projected.Agg == sqlast.AggNone && projected.Scalar == nil && projected.Window == nil &&
				samePlanPath(projected.Path, stmt.GroupBy[key]) {
				projectedKey = true
				break
			}
		}
		if !projectedKey {
			return nil, nil, nil, firstPlanReason(reason,
				"every GROUP BY key must be projected for distributed finalization")
		}
	}
	kinds := make([]sqlast.AggKind, len(stmt.Columns))
	headers := make([]string, len(stmt.Columns))
	hasAggregate := false
	for i := range stmt.Columns {
		column := &stmt.Columns[i]
		if column.Scalar != nil || column.Window != nil {
			if scalarHasAggregate(column.Scalar) {
				return nil, nil, nil, firstPlanReason(reason,
					"scalar aggregates require an algebraic expression combiner")
			}
			return nil, nil, nil, reason
		}
		switch column.Agg {
		case sqlast.AggCount, sqlast.AggSum, sqlast.AggMin, sqlast.AggMax:
			kinds[i] = column.Agg
			hasAggregate = true
		case sqlast.AggAvg:
			return nil, nil, nil, firstPlanReason(reason,
				"AVG requires shard-local SUM and COUNT state")
		case sqlast.AggNone:
			if !grouped || column.Path == nil {
				return nil, nil, nil, reason
			}
			isKey := false
			for _, key := range stmt.GroupBy {
				if samePlanPath(column.Path, key) {
					isKey = true
					break
				}
			}
			if !isKey {
				return nil, nil, nil, reason
			}
			groupKeys = append(groupKeys, i)
		default:
			return nil, nil, nil, firstPlanReason(reason, "aggregate has no distributed combiner")
		}
		headers[i] = distributedResultHeader(column)
	}
	if !grouped && !hasAggregate {
		return nil, nil, nil, reason
	}
	return kinds, groupKeys, headers, reason
}

func distributedResultHeader(column *sqlast.ResultColumn) string {
	if column.Alias != "" {
		return column.Alias
	}
	if column.Agg == sqlast.AggNone && column.Path != nil {
		return string(column.Path.AppendSpec(nil))
	}
	if column.Agg == sqlast.AggCount && column.Path == nil {
		return "count(*)"
	}
	spec := ""
	if column.Path != nil {
		spec = string(column.Path.AppendSpec(nil))
	}
	return strings.ToLower(column.Agg.String()) + "(" + spec + ")"
}

func scalarHasAggregate(expr *sqlast.ScalarExpr) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == sqlast.ScalarAggregate || expr.Agg != sqlast.AggNone ||
		scalarHasAggregate(expr.Left) || scalarHasAggregate(expr.Right) ||
		scalarHasAggregate(expr.Else) {
		return true
	}
	for i := range expr.Whens {
		if exprHasAggregate(expr.Whens[i].Predicate) ||
			scalarHasAggregate(expr.Whens[i].Match) || scalarHasAggregate(expr.Whens[i].Result) {
			return true
		}
	}
	return false
}

func exprHasAggregate(expr *sqlast.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.Agg != sqlast.AggNone || scalarHasAggregate(expr.ScalarLeft) ||
		scalarHasAggregate(expr.ScalarRight) {
		return true
	}
	for _, kid := range expr.Kids {
		if exprHasAggregate(kid) {
			return true
		}
	}
	return false
}

// allRouteSemanticBoundary identifies shapes that are not made complete merely
// by pruning the driving table to one shard. Their nested relation can still
// require data owned elsewhere, so they fail before any dispatch until the
// planner can build an explicit distributed subplan.
func allRouteSemanticBoundary(stmt *sqlast.SelectStmt, reason string) string {
	switch {
	case stmt.Set != nil:
		return firstPlanReason(reason, "set expressions require a distributed set combiner")
	case stmt.With != nil:
		return firstPlanReason(reason, "CTEs require a distributed relation plan")
	case selectHasPhysicalPredicateSubquery(stmt):
		return firstPlanReason(reason, "physical predicate subqueries require a distributed subplan")
	default:
		return reason
	}
}

func selectHasPhysicalPredicateSubquery(stmt *sqlast.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	if exprHasPhysicalSubquery(stmt.Where) || exprHasPhysicalSubquery(stmt.Having) {
		return true
	}
	for i := range stmt.From {
		if stmt.From[i].On != nil && exprHasPhysicalSubquery(stmt.From[i].On.Expr) {
			return true
		}
	}
	for i := range stmt.Columns {
		if scalarHasPhysicalSubquery(stmt.Columns[i].Scalar) {
			return true
		}
	}
	return false
}

func exprHasPhysicalSubquery(expr *sqlast.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.Subquery != nil && selectHasPhysicalRead(expr.Subquery) {
		return true
	}
	if scalarHasPhysicalSubquery(expr.ScalarLeft) || scalarHasPhysicalSubquery(expr.ScalarRight) {
		return true
	}
	for _, kid := range expr.Kids {
		if exprHasPhysicalSubquery(kid) {
			return true
		}
	}
	return false
}

func scalarHasPhysicalSubquery(expr *sqlast.ScalarExpr) bool {
	if expr == nil {
		return false
	}
	if scalarHasPhysicalSubquery(expr.Left) || scalarHasPhysicalSubquery(expr.Right) ||
		scalarHasPhysicalSubquery(expr.Else) {
		return true
	}
	for i := range expr.Whens {
		if exprHasPhysicalSubquery(expr.Whens[i].Predicate) ||
			scalarHasPhysicalSubquery(expr.Whens[i].Match) ||
			scalarHasPhysicalSubquery(expr.Whens[i].Result) {
			return true
		}
	}
	return false
}

func selectHasPhysicalRead(stmt *sqlast.SelectStmt) bool {
	return selectHasPhysicalReadSeen(stmt, make(map[*sqlast.SelectStmt]struct{}))
}

func selectHasPhysicalReadSeen(stmt *sqlast.SelectStmt, seen map[*sqlast.SelectStmt]struct{}) bool {
	if stmt == nil {
		return false
	}
	if _, ok := seen[stmt]; ok {
		return false
	}
	seen[stmt] = struct{}{}
	for i := range stmt.From {
		ref := &stmt.From[i]
		if ref.Kind == sqlast.RelationCollection ||
			(ref.Kind == sqlast.RelationDerived && selectHasPhysicalReadSeen(ref.Query, seen)) {
			return true
		}
	}
	if stmt.With != nil {
		for i := range stmt.With.CTEs {
			if selectHasPhysicalReadSeen(stmt.With.CTEs[i].Query, seen) {
				return true
			}
		}
	}
	if stmt.Set != nil && setExprHasPhysicalRead(stmt.Set.Root, seen) {
		return true
	}
	return selectHasPhysicalPredicateSubquery(stmt)
}

func setExprHasPhysicalRead(expr *sqlast.SetExpr, seen map[*sqlast.SelectStmt]struct{}) bool {
	if expr == nil {
		return false
	}
	if selectHasPhysicalReadSeen(expr.Select, seen) || selectHasPhysicalReadSeen(expr.First, seen) {
		return true
	}
	if expr.Table != nil {
		if expr.Table.Ref.Kind == sqlast.RelationCollection ||
			(expr.Table.Ref.Kind == sqlast.RelationDerived &&
				selectHasPhysicalReadSeen(expr.Table.Ref.Query, seen)) {
			return true
		}
	}
	return setExprHasPhysicalRead(expr.Left, seen) || setExprHasPhysicalRead(expr.Right, seen) ||
		setExprHasPhysicalRead(expr.Child, seen)
}

// validatePlanPhysicalTables performs lexical catalog binding before routing.
// Even a dormant CTE or a predicate made unreachable by contradictory shard
// constraints must still report an unknown table, matching ordinary SQL name
// resolution rather than letting route pruning erase a semantic error.
func validatePlanPhysicalTables(
	snap *Snapshot,
	stmt *sqlast.SelectStmt,
	seen map[*sqlast.SelectStmt]struct{},
) error {
	if stmt == nil {
		return nil
	}
	if _, ok := seen[stmt]; ok {
		return nil
	}
	seen[stmt] = struct{}{}
	for i := range stmt.From {
		ref := &stmt.From[i]
		switch ref.Kind {
		case sqlast.RelationCollection:
			if _, _, _, ok := snap.plannerTableFor(ref.Name); !ok {
				return &PlanError{Table: ref.Name, Reason: "no placement in pinned catalog generation", cause: ErrTableNotPlaced}
			}
		case sqlast.RelationDerived:
			if err := validatePlanPhysicalTables(snap, ref.Query, seen); err != nil {
				return err
			}
		}
	}
	if stmt.With != nil {
		for i := range stmt.With.CTEs {
			if err := validatePlanPhysicalTables(snap, stmt.With.CTEs[i].Query, seen); err != nil {
				return err
			}
		}
	}
	if err := validatePlanSetPhysicalTables(snap, stmt.Set, seen); err != nil {
		return err
	}
	if err := validatePlanExprPhysicalTables(snap, stmt.Where, seen); err != nil {
		return err
	}
	if err := validatePlanExprPhysicalTables(snap, stmt.Having, seen); err != nil {
		return err
	}
	for i := range stmt.From {
		if stmt.From[i].On != nil {
			if err := validatePlanExprPhysicalTables(snap, stmt.From[i].On.Expr, seen); err != nil {
				return err
			}
		}
	}
	for i := range stmt.Columns {
		if err := validatePlanScalarPhysicalTables(snap, stmt.Columns[i].Scalar, seen); err != nil {
			return err
		}
	}
	return nil
}

func validatePlanSetPhysicalTables(snap *Snapshot, set *sqlast.SetExpression, seen map[*sqlast.SelectStmt]struct{}) error {
	if set == nil {
		return nil
	}
	var walk func(*sqlast.SetExpr) error
	walk = func(expr *sqlast.SetExpr) error {
		if expr == nil {
			return nil
		}
		if err := validatePlanPhysicalTables(snap, expr.Select, seen); err != nil {
			return err
		}
		if err := validatePlanPhysicalTables(snap, expr.First, seen); err != nil {
			return err
		}
		if expr.Table != nil {
			ref := &expr.Table.Ref
			if ref.Kind == sqlast.RelationCollection {
				if _, _, _, ok := snap.plannerTableFor(ref.Name); !ok {
					return &PlanError{Table: ref.Name, Reason: "no placement in pinned catalog generation", cause: ErrTableNotPlaced}
				}
			} else if ref.Kind == sqlast.RelationDerived {
				if err := validatePlanPhysicalTables(snap, ref.Query, seen); err != nil {
					return err
				}
			}
		}
		if err := walk(expr.Left); err != nil {
			return err
		}
		if err := walk(expr.Right); err != nil {
			return err
		}
		return walk(expr.Child)
	}
	return walk(set.Root)
}

func validatePlanExprPhysicalTables(snap *Snapshot, expr *sqlast.Expr, seen map[*sqlast.SelectStmt]struct{}) error {
	if expr == nil {
		return nil
	}
	if err := validatePlanPhysicalTables(snap, expr.Subquery, seen); err != nil {
		return err
	}
	if err := validatePlanScalarPhysicalTables(snap, expr.ScalarLeft, seen); err != nil {
		return err
	}
	if err := validatePlanScalarPhysicalTables(snap, expr.ScalarRight, seen); err != nil {
		return err
	}
	for _, kid := range expr.Kids {
		if err := validatePlanExprPhysicalTables(snap, kid, seen); err != nil {
			return err
		}
	}
	return nil
}

func validatePlanScalarPhysicalTables(snap *Snapshot, expr *sqlast.ScalarExpr, seen map[*sqlast.SelectStmt]struct{}) error {
	if expr == nil {
		return nil
	}
	if err := validatePlanScalarPhysicalTables(snap, expr.Left, seen); err != nil {
		return err
	}
	if err := validatePlanScalarPhysicalTables(snap, expr.Right, seen); err != nil {
		return err
	}
	if err := validatePlanScalarPhysicalTables(snap, expr.Else, seen); err != nil {
		return err
	}
	for i := range expr.Whens {
		arm := &expr.Whens[i]
		if err := validatePlanExprPhysicalTables(snap, arm.Predicate, seen); err != nil {
			return err
		}
		if err := validatePlanScalarPhysicalTables(snap, arm.Match, seen); err != nil {
			return err
		}
		if err := validatePlanScalarPhysicalTables(snap, arm.Result, seen); err != nil {
			return err
		}
	}
	return nil
}

func firstPlanReason(current, candidate string) string {
	if current != "" {
		return current
	}
	return candidate
}

func bindPlanLimit(limit *sqlast.Operand, args []any) (int, error) {
	return bindPlanOperand(limit, args, "LIMIT")
}

func bindPlanOperand(limit *sqlast.Operand, args []any, name string) (int, error) {
	if limit == nil {
		return 0, nil
	}
	var spelling []byte
	switch limit.Kind {
	case sqlast.OperandNumber:
		spelling = byteview.Bytes(limit.Text)
	case sqlast.OperandParam:
		if limit.Ordinal < 0 || limit.Ordinal >= len(args) {
			return 0, fmt.Errorf("%w: %s parameter %d is not bound", ErrPlanParameters, name, limit.Ordinal+1)
		}
		switch value := args[limit.Ordinal].(type) {
		case vibejson.RawValue:
			var ok bool
			spelling, ok = value.NumberBytes()
			if !ok {
				return 0, fmt.Errorf("%w: %s requires an integer", ErrPlanParameters, name)
			}
		case int:
			if value < 0 || uint64(value) > 1<<31-1 {
				return 0, fmt.Errorf("%w: invalid %s integer", ErrPlanParameters, name)
			}
			return value, nil
		case int64:
			if value < 0 || uint64(value) > 1<<31-1 {
				return 0, fmt.Errorf("%w: invalid %s integer", ErrPlanParameters, name)
			}
			return int(value), nil
		case uint64:
			if value > 1<<31-1 {
				return 0, fmt.Errorf("%w: invalid %s integer", ErrPlanParameters, name)
			}
			return int(value), nil
		default:
			return 0, fmt.Errorf("%w: %s requires an integer, got %T", ErrPlanParameters, name, value)
		}
	default:
		return 0, fmt.Errorf("%w: %s is not numeric", ErrPlanParameters, name)
	}
	value, ok := parseLimitUint31(spelling)
	if !ok {
		return 0, fmt.Errorf("%w: invalid %s %q", ErrPlanParameters, name, spelling)
	}
	return value, nil
}

// parseLimitUint31 accepts only the SQL runtime's unsigned base-10 LIMIT
// spelling without materializing a string.
func parseLimitUint31(spelling []byte) (int, bool) {
	if len(spelling) == 0 {
		return 0, false
	}
	var value uint64
	for _, c := range spelling {
		if c < '0' || c > '9' {
			return 0, false
		}
		value = value*10 + uint64(c-'0')
		if value > 1<<31-1 {
			return 0, false
		}
	}
	return int(value), true
}
