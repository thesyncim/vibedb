package query

import (
	"fmt"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// ExplainOptions supplies immutable source metadata to an EXPLAIN request.
// It is deliberately cold: ordinary execution does not construct or inspect
// one. When IndexCatalogKnown is false, the renderer reports logical
// capabilities only; when it is true, access-path candidates are checked
// against the exact catalog the source exposes.
type ExplainOptions struct {
	IndexCatalogKnown bool
	Indexes           []store.IndexInfo
	PrimaryPoint      bool
}

// ExplainAnalysis is the measured result attached to EXPLAIN ANALYZE. The
// driver fills it from the same Exec that ran the target statement; no second
// scan or approximate counter path exists.
type ExplainAnalysis struct {
	ElapsedNanoseconds int64
	Rows               int
	ActualAccessPath   string
	Stats              ExecStats
}

// Explain renders the prepared logical plan as compact, versioned JSON. It
// inspects only immutable plan metadata; it does not open a source, collect
// runtime counters, or add work to ordinary execution.
func (q *Query) Explain() (string, error) {
	if q == nil {
		return "", errNilQueryExplain
	}
	p, err := q.compiled()
	if err != nil {
		return "", err
	}
	return p.explainJSON("", len(p.headers), ExplainOptions{})
}

var errNilQueryExplain = queryExplainError("query: cannot explain a nil Query")

type queryExplainError string

func (e queryExplainError) Error() string { return string(e) }

type explainDocument struct {
	Version uint8       `json:"version"`
	Plan    explainPlan `json:"plan"`
}

type explainPlan struct {
	Node          string            `json:"node"`
	Collection    string            `json:"collection,omitempty"`
	AccessPath    string            `json:"access_path"`
	Scope         string            `json:"scope"`
	FilterColumns []string          `json:"filter_columns,omitempty"`
	LateColumns   []string          `json:"late_columns,omitempty"`
	Output        []string          `json:"output,omitempty"`
	Where         string            `json:"where,omitempty"`
	Predicate     *explainPredicate `json:"predicate,omitempty"`
	GroupBy       []string          `json:"group_by,omitempty"`
	OrderBy       []string          `json:"order_by,omitempty"`
	Limit         *int              `json:"limit,omitempty"`
	Aggregate     bool              `json:"aggregate,omitempty"`
	SingleRow     bool              `json:"single_row,omitempty"`
	Joins         []explainJoin     `json:"joins,omitempty"`
	Marks         []explainMark     `json:"marks,omitempty"`
	CTEs          []explainCTE      `json:"ctes,omitempty"`
	Windows       []explainWindow   `json:"windows,omitempty"`
	Analyze       *explainAnalyze   `json:"analyze,omitempty"`
}

type explainWindow struct {
	Algorithm   string                  `json:"algorithm"`
	PartitionBy []string                `json:"partition_by,omitempty"`
	OrderBy     []explainWindowOrder    `json:"order_by,omitempty"`
	Functions   []explainWindowFunction `json:"functions"`
	SortReused  bool                    `json:"sort_reused"`
	Rows        *uint64                 `json:"rows,omitempty"`
}

type explainWindowOrder struct {
	Path      string `json:"path"`
	Direction string `json:"direction"`
	Nulls     string `json:"nulls"`
}

type explainWindowFunction struct {
	Name     string              `json:"name"`
	Window   string              `json:"window,omitempty"`
	Argument string              `json:"argument,omitempty"`
	Offset   *int                `json:"offset,omitempty"`
	Buckets  *int                `json:"buckets,omitempty"`
	Nth      *int                `json:"nth,omitempty"`
	Default  bool                `json:"default"`
	Frame    *explainWindowFrame `json:"frame,omitempty"`
}

type explainWindowFrame struct {
	Mode      string `json:"mode"`
	Unit      string `json:"unit"`
	Start     string `json:"start"`
	End       string `json:"end"`
	Exclusion string `json:"exclusion"`
}

type explainCTE struct {
	Name        string `json:"name"`
	Mode        string `json:"mode"`
	Reason      string `json:"reason"`
	References  int    `json:"references"`
	Evaluations uint64 `json:"evaluations,omitempty"`
}

type explainJoin struct {
	Collection      string           `json:"collection"`
	Type            string           `json:"type"`
	AccessPath      string           `json:"access_path"`
	OuterPath       string           `json:"outer_path,omitempty"`
	InnerPath       string           `json:"inner_path,omitempty"`
	Algorithm       string           `json:"algorithm,omitempty"`
	ActualAlgorithm string           `json:"actual_algorithm,omitempty"`
	BuildSide       string           `json:"build_side,omitempty"`
	Keys            []explainJoinKey `json:"keys,omitempty"`
	KeyCount        int              `json:"key_count"`
	Using           []string         `json:"using,omitempty"`
	Residual        bool             `json:"residual"`
	Cross           bool             `json:"cross"`
	Pairs           *uint64          `json:"pairs,omitempty"`
}

// explainMark reports a grouped predicate-subquery operator separately from a
// relation join. A mark filters one driving row at most once and never emits a
// relation pair, so presenting it as a join would make cardinality and runtime
// counters misleading. The access-path spelling is the stable public contract;
// Kind and Operator retain the authored SQL distinction needed to interpret
// NULL and cardinality behavior.
type explainMark struct {
	Collection string           `json:"collection"`
	Kind       string           `json:"kind"`
	AccessPath string           `json:"access_path"`
	Keys       []explainJoinKey `json:"keys"`
	KeyCount   int              `json:"key_count"`
	Probe      string           `json:"probe,omitempty"`
	Value      string           `json:"value,omitempty"`
	Operator   string           `json:"operator,omitempty"`
}

type explainJoinKey struct {
	Left  string `json:"left"`
	Right string `json:"right"`
}

type explainPredicate struct {
	Kind       string             `json:"kind"`
	Path       string             `json:"path,omitempty"`
	Operator   string             `json:"operator,omitempty"`
	ValueCount int                `json:"value_count,omitempty"`
	Children   []explainPredicate `json:"children,omitempty"`
}

func (p *plan) explainJSON(collection string, outputCount int, options ExplainOptions) (string, error) {
	return p.explainJSONAnalysis(collection, outputCount, options, nil, nil)
}

type explainStatementContext struct {
	ctes          []explainCTE
	relationJoins []explainJoin
	windows       []explainWindow
	windowInput   *plan
}

type explainAnalyze struct {
	ElapsedNS               int64  `json:"elapsed_ns"`
	Rows                    int    `json:"rows"`
	ActualAccessPath        string `json:"actual_access_path,omitempty"`
	Workers                 int    `json:"workers"`
	RowsTotal               uint64 `json:"rows_total"`
	RowsScanned             uint64 `json:"rows_scanned"`
	Batches                 uint64 `json:"batches"`
	PeakBatchRows           int    `json:"peak_batch_rows"`
	PeakBatchBytes          int64  `json:"peak_batch_bytes"`
	BufferedBytes           int64  `json:"buffered_bytes"`
	SpillRuns               uint64 `json:"spill_runs"`
	SpilledBytes            int64  `json:"spilled_bytes"`
	IndexBounded            bool   `json:"index_bounded"`
	IndexLookups            int    `json:"index_lookups"`
	IndexPostingPages       int    `json:"index_posting_pages"`
	IndexCertificateRows    uint64 `json:"index_certificate_rows"`
	IndexRecheckRows        uint64 `json:"index_recheck_rows"`
	CandidateRows           uint64 `json:"candidate_rows"`
	CandidateChunks         int    `json:"candidate_chunks"`
	CoveringColumns         int    `json:"covering_columns"`
	TokenFilterRows         uint64 `json:"token_filter_rows"`
	TokenFilterFallbackRows uint64 `json:"token_filter_fallback_rows"`
	JoinMemberships         int    `json:"join_memberships"`
	JoinLookups             int    `json:"join_lookups"`
	JoinKeys                uint64 `json:"join_keys"`
	JoinProbes              uint64 `json:"join_probes"`
	JoinFilters             int    `json:"join_filters"`
	JoinFilterKeys          uint64 `json:"join_filter_keys"`
	JoinFilterRejected      uint64 `json:"join_filter_rejected"`
	JoinBuilds              int    `json:"join_builds"`
	JoinBuildRows           uint64 `json:"join_build_rows"`
	JoinPairs               uint64 `json:"join_pairs"`
}

func newExplainAnalyze(analysis *ExplainAnalysis) *explainAnalyze {
	if analysis == nil {
		return nil
	}
	s := analysis.Stats
	return &explainAnalyze{
		ElapsedNS:               analysis.ElapsedNanoseconds,
		Rows:                    analysis.Rows,
		ActualAccessPath:        analysis.ActualAccessPath,
		Workers:                 s.Workers,
		RowsTotal:               s.RowsTotal,
		RowsScanned:             s.RowsScanned,
		Batches:                 s.Batches,
		PeakBatchRows:           s.PeakBatchRows,
		PeakBatchBytes:          s.PeakBatchBytes,
		BufferedBytes:           s.BufferedBytes,
		SpillRuns:               s.SpillRuns,
		SpilledBytes:            s.SpilledBytes,
		IndexBounded:            s.IndexBounded,
		IndexLookups:            s.IndexLookups,
		IndexPostingPages:       s.IndexPostingPages,
		IndexCertificateRows:    s.IndexCertificateRows,
		IndexRecheckRows:        s.IndexRecheckRows,
		CandidateRows:           s.CandidateRows,
		CandidateChunks:         s.CandidateChunks,
		CoveringColumns:         s.CoveringColumns,
		TokenFilterRows:         s.TokenFilterRows,
		TokenFilterFallbackRows: s.TokenFilterFallbackRows,
		JoinMemberships:         s.JoinMemberships,
		JoinLookups:             s.JoinLookups,
		JoinKeys:                s.JoinKeys,
		JoinProbes:              s.JoinProbes,
		JoinFilters:             s.JoinFilters,
		JoinFilterKeys:          s.JoinFilterKeys,
		JoinFilterRejected:      s.JoinFilterRejected,
		JoinBuilds:              s.JoinBuilds,
		JoinBuildRows:           s.JoinBuildRows,
		JoinPairs:               s.JoinPairs,
	}
}

func (p *plan) explainJSONAnalysis(
	collection string,
	outputCount int,
	options ExplainOptions,
	analysis *ExplainAnalysis,
	context *explainStatementContext,
) (string, error) {
	sourcePlan := p
	node := "scan"
	var ctes []explainCTE
	var relationJoins []explainJoin
	var windows []explainWindow
	if context != nil {
		ctes = context.ctes
		relationJoins = context.relationJoins
		windows = context.windows
		if context.windowInput != nil {
			sourcePlan = context.windowInput
			node = "window"
		}
	}
	plan := explainPlan{
		Node:          node,
		Collection:    collection,
		AccessPath:    explainAccessPath(sourcePlan.where, sourcePlan.valuePaths, len(sourcePlan.joins), options),
		Scope:         "logical",
		FilterColumns: explainColumns(sourcePlan.valuePaths, sourcePlan.filterCols),
		LateColumns:   explainColumns(sourcePlan.valuePaths, sourcePlan.lateCols),
		Aggregate:     sourcePlan.hasAggregate,
		SingleRow:     sourcePlan.singleRow,
		CTEs:          ctes,
		Joins:         relationJoins,
		Windows:       windows,
		Analyze:       newExplainAnalyze(analysis),
	}
	if outputCount < 0 || outputCount > len(p.headers) {
		outputCount = len(p.headers)
	}
	plan.Output = append([]string(nil), p.headers[:outputCount]...)
	if sourcePlan.where != nil {
		plan.Where = explainPredicateSummary(sourcePlan.where)
		plan.Predicate = explainPredicateTree(sourcePlan.where, sourcePlan.valuePaths)
	}
	if options.IndexCatalogKnown {
		plan.Scope = "source-aware"
	}
	for _, col := range sourcePlan.groupCols {
		if col >= 0 && col < len(sourcePlan.valuePaths) {
			plan.GroupBy = append(plan.GroupBy, sourcePlan.valuePaths[col].spec)
		}
	}
	for _, order := range p.order {
		if order.value < 0 || order.value >= len(p.valuePaths) {
			continue
		}
		direction := "ASC"
		if order.dir == Desc {
			direction = "DESC"
		}
		plan.OrderBy = append(plan.OrderBy,
			p.valuePaths[order.value].spec+" "+direction)
	}
	if p.hasLimit {
		plan.Limit = &p.limit
	}
	for _, join := range sourcePlan.joins {
		inner := "PRIMARY KEY"
		if join.inner != nil && join.innerPath >= 0 &&
			join.innerPath < len(join.inner.valuePaths) {
			inner = join.inner.valuePaths[join.innerPath].spec
		}
		joinType := "inner"
		if join.left {
			joinType = "left"
		}
		access := "adaptive-semi-join"
		if join.fanOut {
			access = "hash-build-and-probe"
		} else if join.origin == joinOriginDecorrelatedExists {
			access = "decorrelated-exists-semi"
			if join.anti {
				access = "decorrelated-exists-anti"
			}
		}
		plan.Joins = append(plan.Joins, explainJoin{
			Collection: join.collection,
			Type:       joinType,
			AccessPath: access,
			OuterPath:  sourcePlan.valuePaths[join.outerPath].spec,
			InnerPath:  inner,
			Algorithm:  access,
			BuildSide:  "right",
			KeyCount:   1,
		})
	}
	for i := range sourcePlan.marks {
		mark := &sourcePlan.marks[i]
		explained := explainMark{
			Collection: mark.collection,
			Kind:       explainCorrelatedMarkKind(mark.kind),
			AccessPath: explainCorrelatedMarkAccessPath(mark.kind),
			KeyCount:   min(len(mark.outer), len(mark.innerKeys)),
		}
		for key := 0; key < explained.KeyCount; key++ {
			explained.Keys = append(explained.Keys, explainJoinKey{
				Left:  explainPlanPath(sourcePlan, mark.outer[key]),
				Right: explainPlanPath(mark.inner, mark.innerKeys[key]),
			})
		}
		if mark.probe >= 0 {
			explained.Probe = explainPlanPath(sourcePlan, mark.probe)
		}
		if mark.value >= 0 {
			explained.Value = explainPlanPath(mark.inner, mark.value)
		}
		if mark.kind == correlatedMarkScalar {
			explained.Operator = explainOperator(mark.op)
		}
		plan.Marks = append(plan.Marks, explained)
	}
	encoded, err := vibejson.Marshal(&explainDocument{Version: 1, Plan: plan})
	if err != nil {
		return "", err
	}
	return byteview.String(encoded), nil
}

func explainPlanPath(plan *plan, column int) string {
	if plan == nil || column < 0 || column >= len(plan.valuePaths) {
		return ""
	}
	return plan.valuePaths[column].spec
}

func explainCorrelatedMarkKind(kind correlatedMarkKind) string {
	switch kind {
	case correlatedMarkExists:
		return "exists"
	case correlatedMarkNotExists:
		return "not-exists"
	case correlatedMarkIn:
		return "in"
	case correlatedMarkNotIn:
		return "not-in"
	case correlatedMarkScalar:
		return "scalar"
	default:
		return "unknown"
	}
}

func explainCorrelatedMarkAccessPath(kind correlatedMarkKind) string {
	switch kind {
	case correlatedMarkExists:
		return "decorrelated-composite-exists-semi"
	case correlatedMarkNotExists:
		return "decorrelated-composite-exists-anti"
	case correlatedMarkIn:
		return "decorrelated-correlated-in"
	case correlatedMarkNotIn:
		return "decorrelated-correlated-not-in"
	case correlatedMarkScalar:
		return "decorrelated-correlated-scalar"
	default:
		return "decorrelated-correlated-unknown"
	}
}

func (s *Statement) Explain() (string, error) {
	return s.ExplainWith(ExplainOptions{})
}

// ExplainWith renders the current compiled plan with immutable source
// metadata. It does not bind or execute.
func (s *Statement) ExplainWith(options ExplainOptions) (string, error) {
	if s == nil || s.tree == nil {
		return "", queryExplainError("query: cannot explain a nil or released Statement")
	}
	if set := s.setSQL(); set != nil {
		return set.explain(options, nil)
	}
	p, err := s.q.compiled()
	if err != nil {
		return "", err
	}
	return s.explainJSON(p, options, nil)
}

// ExplainBound renders the plan after binding args, without opening a source
// or reading a row. It is the accurate entry point for prepared statements:
// value-dependent LIMIT/OFFSET clauses are resolved from the supplied bind,
// while the normal execution path keeps using RunInto unchanged.
func (s *Statement) ExplainBound(args []any) (string, error) {
	return s.ExplainBoundWith(args, ExplainOptions{})
}

// ExplainBoundWith is ExplainBound plus immutable source metadata. Adapters
// use it when they can describe the source catalog without scanning it.
func (s *Statement) ExplainBoundWith(args []any, options ExplainOptions) (string, error) {
	if s == nil || s.tree == nil {
		return "", queryExplainError("query: cannot explain a nil or released Statement")
	}
	if len(args) != s.params {
		return "", queryExplainError("query: explain argument count does not match statement")
	}
	if set := s.setSQL(); set != nil {
		if err := set.bindForExplain(args); err != nil {
			return "", err
		}
		return set.explain(options, nil)
	}
	if err := s.bind(args); err != nil {
		return "", err
	}
	if err := s.bindFusedExplain(args); err != nil {
		return "", err
	}
	p, err := s.q.compiled()
	if err != nil {
		return "", err
	}
	return s.explainJSON(p, options, nil)
}

// ExplainAnalyze renders the current compiled plan with measured execution
// work. The target must already have run; this method only formats metadata.
func (s *Statement) ExplainAnalyze(options ExplainOptions, analysis ExplainAnalysis) (string, error) {
	if s == nil || s.tree == nil {
		return "", queryExplainError("query: cannot explain a nil or released Statement")
	}
	if set := s.setSQL(); set != nil {
		return set.explain(options, &analysis)
	}
	p, err := s.q.compiled()
	if err != nil {
		return "", err
	}
	return s.explainJSON(p, options, &analysis)
}

func (s *Statement) explainJSON(
	p *plan,
	options ExplainOptions,
	analysis *ExplainAnalysis,
) (string, error) {
	if s.canFuseCTE() {
		var err error
		p, err = s.cteReference().def.stmt.q.compiled()
		if err != nil {
			return "", err
		}
	}
	context := explainStatementContext{
		ctes:          s.explainCTEs(),
		relationJoins: s.explainRelationJoins(analysis != nil),
	}
	if window := s.window(); window != nil {
		inputPlan, err := window.input.explainSourcePlan()
		if err != nil {
			return "", err
		}
		context.windowInput = inputPlan
		context.windows = window.explain(analysis != nil)
		context.ctes = window.input.explainCTEs()
		context.relationJoins = window.input.explainRelationJoins(analysis != nil)
	}
	return p.explainJSONAnalysis(
		s.Collection(), s.outputs, options, analysis, &context,
	)
}

func (s *Statement) explainSourcePlan() (*plan, error) {
	if s == nil {
		return nil, queryExplainError("query: cannot explain a nil Statement")
	}
	if s.canFuseCTE() {
		return s.cteReference().def.stmt.explainSourcePlan()
	}
	return s.q.compiled()
}

func (w *statementWindow) explain(analyze bool) []explainWindow {
	if w == nil {
		return nil
	}
	stages := make([]explainWindow, 0, len(w.stages))
	for i := range w.stages {
		stage := &w.stages[i]
		explained := explainWindow{
			Algorithm:  "stable-merge-sort",
			SortReused: len(stage.exprs) > 1,
			Functions:  make([]explainWindowFunction, 0, len(stage.exprs)),
		}
		for _, path := range stage.spec.PartitionBy {
			explained.PartitionBy = append(explained.PartitionBy, w.input.spec(path))
		}
		for at := range stage.spec.OrderBy {
			term := &stage.spec.OrderBy[at]
			order := explainWindowOrder{
				Path: w.input.spec(term.Path), Direction: "asc", Nulls: "last",
			}
			if term.Desc {
				order.Direction = "desc"
				order.Nulls = "first"
			}
			if term.Nulls == sqlast.WindowNullsFirst {
				order.Nulls = "first"
			} else if term.Nulls == sqlast.WindowNullsLast {
				order.Nulls = "last"
			}
			explained.OrderBy = append(explained.OrderBy, order)
		}
		for at, expr := range stage.exprs {
			physical := &stage.plan.functions[at]
			function := explainWindowFunction{
				Name: strings.ToLower(expr.Kind.String()), Window: expr.Spec.Name,
			}
			if expr.Argument != nil {
				function.Argument = w.input.spec(expr.Argument)
			}
			switch expr.Kind {
			case sqlast.WindowLag, sqlast.WindowLead:
				offset := physical.offset
				function.Offset = &offset
				function.Default = physical.hasDefault
			case sqlast.WindowNTile:
				buckets := physical.buckets
				function.Buckets = &buckets
			case sqlast.WindowNthValue:
				nth := physical.nth
				function.Nth = &nth
			}
			if sqlWindowFunctionUsesFrame(expr.Kind) {
				mode := "explicit"
				if !expr.Spec.Frame.Explicit {
					mode = "default-full-partition"
					if len(expr.Spec.OrderBy) != 0 {
						mode = "default-peer-prefix"
					}
				}
				function.Frame = &explainWindowFrame{
					Mode: mode, Unit: explainWindowFrameUnit(physical.frame.unit),
					Start: explainWindowFrameBound(physical.frame.start),
					End:   explainWindowFrameBound(physical.frame.end),
					Exclusion: explainWindowFrameExclusion(
						physical.frame.exclusion,
					),
				}
			}
			explained.Functions = append(explained.Functions, function)
		}
		if analyze {
			rows := w.lastRows
			explained.Rows = &rows
		}
		stages = append(stages, explained)
	}
	return stages
}

func explainWindowFrameUnit(unit windowFrameUnit) string {
	switch unit {
	case windowFrameRows:
		return "rows"
	case windowFrameGroups:
		return "groups"
	case windowFrameRange:
		return "range"
	default:
		return "unknown"
	}
}

func explainWindowFrameBound(bound windowFrameBound) string {
	switch bound.kind {
	case windowUnboundedPreceding:
		return "unbounded preceding"
	case windowPreceding:
		if bound.rangeOffset.kind == kindNumber {
			return byteview.String(bound.rangeOffset.num) + " preceding"
		}
		return fmt.Sprintf("%d preceding", bound.offset)
	case windowCurrentRow:
		return "current row"
	case windowFollowing:
		if bound.rangeOffset.kind == kindNumber {
			return byteview.String(bound.rangeOffset.num) + " following"
		}
		return fmt.Sprintf("%d following", bound.offset)
	case windowUnboundedFollowing:
		return "unbounded following"
	default:
		return "unknown"
	}
}

func explainWindowFrameExclusion(exclusion windowFrameExclusion) string {
	switch exclusion {
	case windowExcludeNoOthers:
		return "no others"
	case windowExcludeCurrentRow:
		return "current row"
	case windowExcludeGroup:
		return "group"
	case windowExcludeTies:
		return "ties"
	default:
		return "unknown"
	}
}

func (s *Statement) bindFusedExplain(args []any) error {
	if !s.canFuseCTE() {
		return nil
	}
	def := s.cteReference().def
	n := def.stmt.NumParams()
	if def.argBase < 0 || def.argBase+n > len(args) {
		return queryExplainError("query: invalid fused CTE placeholder range")
	}
	return def.stmt.bind(args[def.argBase : def.argBase+n])
}

func (s *Statement) explainCTEs() []explainCTE {
	if s == nil || s.tree == nil || s.tree.With == nil || s.cteCatalog() == nil {
		return nil
	}
	result := make([]explainCTE, 0, len(s.tree.With.CTEs))
	for i := range s.tree.With.CTEs {
		definition := &s.tree.With.CTEs[i]
		def := s.cteCatalog().find(definition.Query)
		if def == nil {
			continue
		}
		mode, reason := explainCTEMode(def)
		result = append(result, explainCTE{
			Name: definition.Name, Mode: mode, Reason: reason,
			References: def.references, Evaluations: def.runEvaluations,
		})
	}
	return result
}

func explainCTEMode(def *statementCTE) (string, string) {
	if def == nil || def.definition == nil {
		return "unused", "definition is not referenced"
	}
	switch def.definition.Materialization {
	case sqlast.CTEMaterialized:
		return "materialized", "MATERIALIZED forces one lazy shared evaluation"
	case sqlast.CTENotMaterialized:
		return "not-materialized", "NOT MATERIALIZED evaluates each syntactic reference independently"
	}
	if def.references == 0 {
		return "unused", "definition is validated but never evaluated"
	}
	if def.references > 1 {
		return "materialized", "default policy shares a multiply referenced definition"
	}
	if def.firstReference != nil && def.firstReference.mode() == cteFused {
		return "fused", "single safe identity projection is fused into the child plan"
	}
	return "reference-local", "single reference retains a semantic materialization boundary"
}

func explainColumns(paths []compiledPath, columns []int) []string {
	result := make([]string, 0, len(columns))
	for _, column := range columns {
		if column >= 0 && column < len(paths) {
			result = append(result, paths[column].spec)
		}
	}
	return result
}

func explainAccessPath(predicate *compiledPredicate, paths []compiledPath, joins int, options ExplainOptions) string {
	if options.PrimaryPoint {
		// Point materialization is the driver's first choice, with a bounded
		// fallback to the normal source when the point set exceeds its memory
		// budget. EXPLAIN cannot know that budget outcome without doing the
		// materialization, so say both truthful alternatives.
		return "primary-key-point-or-scan"
	}
	if predicate == nil {
		return "full-scan"
	}
	if options.IndexCatalogKnown {
		var workspace Workspace
		if predicate.canBound(paths, options.Indexes, &workspace) {
			return "adaptive-exact-index-or-scan"
		}
		if joins != 0 {
			return "adaptive-join-or-scan"
		}
		return "full-scan"
	}
	if joins != 0 && !explainHasPosting(predicate) && !explainHasExactCandidate(predicate) {
		return "adaptive-join-or-scan"
	}
	if explainHasPosting(predicate) {
		return "adaptive-posting-or-scan"
	}
	if explainHasExactCandidate(predicate) {
		return "adaptive-exact-index-or-scan"
	}
	return "full-scan"
}

func explainHasPosting(predicate *compiledPredicate) bool {
	if predicate == nil {
		return false
	}
	if predicate.probe.kind != postNone {
		return true
	}
	for _, child := range predicate.kids {
		if explainHasPosting(child) {
			return true
		}
	}
	return false
}

func explainHasExactCandidate(predicate *compiledPredicate) bool {
	if predicate == nil {
		return false
	}
	switch predicate.kind {
	case predCmp:
		if predicate.op == Eq && len(predicate.needle.Entries) != 0 {
			return true
		}
	case predIn:
		if len(predicate.needles) == len(predicate.lits) && len(predicate.lits) != 0 {
			return true
		}
	case predContains:
		if len(predicate.needle.Entries) != 0 || predicate.containPlan != nil {
			return true
		}
	}
	for _, child := range predicate.kids {
		if explainHasExactCandidate(child) {
			return true
		}
	}
	return false
}

func explainPredicateSummary(predicate *compiledPredicate) string {
	if predicate == nil {
		return ""
	}
	switch predicate.kind {
	case predCmp:
		return "comparison"
	case predCmpBound:
		return "correlation-comparison"
	case predCorrelationKnown:
		return "correlation-known"
	case predContains:
		return "contains"
	case predExists:
		return "exists"
	case predIsNull:
		return "is-null"
	case predAnd:
		return "and"
	case predOr:
		return "or"
	case predNot:
		return "not"
	case predIn:
		return "in"
	case predInBound:
		return "join-match"
	case predAntiBound:
		return "join-anti-match"
	default:
		return "unknown"
	}
}

func explainPredicateTree(predicate *compiledPredicate, paths []compiledPath) *explainPredicate {
	if predicate == nil {
		return nil
	}
	node := &explainPredicate{Kind: explainPredicateKind(predicate.kind)}
	if predicate.col >= 0 && predicate.col < len(paths) {
		node.Path = paths[predicate.col].spec
	} else if predicate.boundPath != "" {
		node.Path = predicate.boundPath
	}
	switch predicate.kind {
	case predCmp, predCmpBound:
		node.Operator = explainOperator(predicate.op)
	case predCorrelationKnown:
		node.Operator = "IS KNOWN"
	case predContains:
		node.Operator = "@>"
	case predExists:
		node.Operator = "EXISTS"
	case predIsNull:
		node.Operator = "IS NULL"
	case predIn, predInBound:
		node.Operator = "IN"
		node.ValueCount = len(predicate.lits)
	case predAntiBound:
		node.Operator = "NO MATCH"
	}
	for _, child := range predicate.kids {
		if explained := explainPredicateTree(child, paths); explained != nil {
			node.Children = append(node.Children, *explained)
		}
	}
	return node
}

func explainPredicateKind(kind predKind) string {
	switch kind {
	case predCmp:
		return "comparison"
	case predCmpBound:
		return "correlation-comparison"
	case predCorrelationKnown:
		return "correlation-known"
	case predContains:
		return "contains"
	case predExists:
		return "exists"
	case predIsNull:
		return "is-null"
	case predAnd:
		return "and"
	case predOr:
		return "or"
	case predNot:
		return "not"
	case predIn:
		return "in"
	case predInBound:
		return "join-match"
	case predAntiBound:
		return "join-anti-match"
	default:
		return "unknown"
	}
}

func explainOperator(op Op) string {
	switch op {
	case Eq:
		return "="
	case Ne:
		return "!="
	case Lt:
		return "<"
	case Le:
		return "<="
	case Gt:
		return ">"
	case Ge:
		return ">="
	default:
		return "?"
	}
}
