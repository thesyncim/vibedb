package query

import (
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
	Analyze       *explainAnalyze   `json:"analyze,omitempty"`
}

type explainJoin struct {
	Collection string `json:"collection"`
	Type       string `json:"type"`
	AccessPath string `json:"access_path"`
	OuterPath  string `json:"outer_path"`
	InnerPath  string `json:"inner_path"`
}

type explainPredicate struct {
	Kind       string             `json:"kind"`
	Path       string             `json:"path,omitempty"`
	Operator   string             `json:"operator,omitempty"`
	ValueCount int                `json:"value_count,omitempty"`
	Children   []explainPredicate `json:"children,omitempty"`
}

func (p *plan) explainJSON(collection string, outputCount int, options ExplainOptions) (string, error) {
	return p.explainJSONAnalysis(collection, outputCount, options, nil)
}

type explainAnalyze struct {
	ElapsedNS            int64  `json:"elapsed_ns"`
	Rows                 int    `json:"rows"`
	ActualAccessPath     string `json:"actual_access_path,omitempty"`
	Workers              int    `json:"workers"`
	RowsTotal            uint64 `json:"rows_total"`
	RowsScanned          uint64 `json:"rows_scanned"`
	Batches              uint64 `json:"batches"`
	PeakBatchRows        int    `json:"peak_batch_rows"`
	PeakBatchBytes       int64  `json:"peak_batch_bytes"`
	BufferedBytes        int64  `json:"buffered_bytes"`
	SpillRuns            uint64 `json:"spill_runs"`
	SpilledBytes         int64  `json:"spilled_bytes"`
	IndexBounded         bool   `json:"index_bounded"`
	IndexLookups         int    `json:"index_lookups"`
	IndexPostingPages    int    `json:"index_posting_pages"`
	IndexCertificateRows uint64 `json:"index_certificate_rows"`
	IndexRecheckRows     uint64 `json:"index_recheck_rows"`
	CandidateRows        uint64 `json:"candidate_rows"`
	CandidateChunks      int    `json:"candidate_chunks"`
	CoveringColumns      int    `json:"covering_columns"`
	JoinMemberships      int    `json:"join_memberships"`
	JoinLookups          int    `json:"join_lookups"`
	JoinKeys             uint64 `json:"join_keys"`
	JoinProbes           uint64 `json:"join_probes"`
	JoinFilters          int    `json:"join_filters"`
	JoinFilterKeys       uint64 `json:"join_filter_keys"`
	JoinFilterRejected   uint64 `json:"join_filter_rejected"`
	JoinBuilds           int    `json:"join_builds"`
	JoinBuildRows        uint64 `json:"join_build_rows"`
	JoinPairs            uint64 `json:"join_pairs"`
}

func newExplainAnalyze(analysis *ExplainAnalysis) *explainAnalyze {
	if analysis == nil {
		return nil
	}
	s := analysis.Stats
	return &explainAnalyze{
		ElapsedNS:            analysis.ElapsedNanoseconds,
		Rows:                 analysis.Rows,
		ActualAccessPath:     analysis.ActualAccessPath,
		Workers:              s.Workers,
		RowsTotal:            s.RowsTotal,
		RowsScanned:          s.RowsScanned,
		Batches:              s.Batches,
		PeakBatchRows:        s.PeakBatchRows,
		PeakBatchBytes:       s.PeakBatchBytes,
		BufferedBytes:        s.BufferedBytes,
		SpillRuns:            s.SpillRuns,
		SpilledBytes:         s.SpilledBytes,
		IndexBounded:         s.IndexBounded,
		IndexLookups:         s.IndexLookups,
		IndexPostingPages:    s.IndexPostingPages,
		IndexCertificateRows: s.IndexCertificateRows,
		IndexRecheckRows:     s.IndexRecheckRows,
		CandidateRows:        s.CandidateRows,
		CandidateChunks:      s.CandidateChunks,
		CoveringColumns:      s.CoveringColumns,
		JoinMemberships:      s.JoinMemberships,
		JoinLookups:          s.JoinLookups,
		JoinKeys:             s.JoinKeys,
		JoinProbes:           s.JoinProbes,
		JoinFilters:          s.JoinFilters,
		JoinFilterKeys:       s.JoinFilterKeys,
		JoinFilterRejected:   s.JoinFilterRejected,
		JoinBuilds:           s.JoinBuilds,
		JoinBuildRows:        s.JoinBuildRows,
		JoinPairs:            s.JoinPairs,
	}
}

func (p *plan) explainJSONAnalysis(
	collection string,
	outputCount int,
	options ExplainOptions,
	analysis *ExplainAnalysis,
) (string, error) {
	plan := explainPlan{
		Node:          "scan",
		Collection:    collection,
		AccessPath:    explainAccessPath(p.where, p.valuePaths, len(p.joins), options),
		Scope:         "logical",
		FilterColumns: explainColumns(p.valuePaths, p.filterCols),
		LateColumns:   explainColumns(p.valuePaths, p.lateCols),
		Aggregate:     p.hasAggregate,
		SingleRow:     p.singleRow,
		Analyze:       newExplainAnalyze(analysis),
	}
	if outputCount < 0 || outputCount > len(p.headers) {
		outputCount = len(p.headers)
	}
	plan.Output = append([]string(nil), p.headers[:outputCount]...)
	if p.where != nil {
		plan.Where = explainPredicateSummary(p.where)
		plan.Predicate = explainPredicateTree(p.where, p.valuePaths)
	}
	if options.IndexCatalogKnown {
		plan.Scope = "source-aware"
	}
	for _, col := range p.groupCols {
		if col >= 0 && col < len(p.valuePaths) {
			plan.GroupBy = append(plan.GroupBy, p.valuePaths[col].spec)
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
	for _, join := range p.joins {
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
		}
		plan.Joins = append(plan.Joins, explainJoin{
			Collection: join.collection,
			Type:       joinType,
			AccessPath: access,
			OuterPath:  p.valuePaths[join.outerPath].spec,
			InnerPath:  inner,
		})
	}
	encoded, err := vibejson.Marshal(&explainDocument{Version: 1, Plan: plan})
	if err != nil {
		return "", err
	}
	return byteview.String(encoded), nil
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
	p, err := s.q.compiled()
	if err != nil {
		return "", err
	}
	return p.explainJSON(s.Collection(), s.outputs, options)
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
	if err := s.bind(args); err != nil {
		return "", err
	}
	p, err := s.q.compiled()
	if err != nil {
		return "", err
	}
	return p.explainJSON(s.Collection(), s.outputs, options)
}

// ExplainAnalyze renders the current compiled plan with measured execution
// work. The target must already have run; this method only formats metadata.
func (s *Statement) ExplainAnalyze(options ExplainOptions, analysis ExplainAnalysis) (string, error) {
	if s == nil || s.tree == nil {
		return "", queryExplainError("query: cannot explain a nil or released Statement")
	}
	p, err := s.q.compiled()
	if err != nil {
		return "", err
	}
	return p.explainJSONAnalysis(s.Collection(), s.outputs, options, &analysis)
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
	case predCmp:
		node.Operator = explainOperator(predicate.op)
	case predContains:
		node.Operator = "@>"
	case predExists:
		node.Operator = "EXISTS"
	case predIsNull:
		node.Operator = "IS NULL"
	case predIn, predInBound:
		node.Operator = "IN"
		node.ValueCount = len(predicate.lits)
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
