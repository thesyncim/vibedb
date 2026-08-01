package query

import (
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

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
	return p.explainJSON("", len(p.headers))
}

var errNilQueryExplain = queryExplainError("query: cannot explain a nil Query")

type queryExplainError string

func (e queryExplainError) Error() string { return string(e) }

type explainDocument struct {
	Version uint8       `json:"version"`
	Plan    explainPlan `json:"plan"`
}

type explainPlan struct {
	Node          string        `json:"node"`
	Collection    string        `json:"collection,omitempty"`
	AccessPath    string        `json:"access_path"`
	FilterColumns []string      `json:"filter_columns,omitempty"`
	LateColumns   []string      `json:"late_columns,omitempty"`
	Output        []string      `json:"output,omitempty"`
	Where         string        `json:"where,omitempty"`
	GroupBy       []string      `json:"group_by,omitempty"`
	OrderBy       []string      `json:"order_by,omitempty"`
	Limit         *int          `json:"limit,omitempty"`
	Aggregate     bool          `json:"aggregate,omitempty"`
	SingleRow     bool          `json:"single_row,omitempty"`
	Joins         []explainJoin `json:"joins,omitempty"`
}

type explainJoin struct {
	Collection string `json:"collection"`
	Type       string `json:"type"`
	AccessPath string `json:"access_path"`
	OuterPath  string `json:"outer_path"`
	InnerPath  string `json:"inner_path"`
}

func (p *plan) explainJSON(collection string, outputCount int) (string, error) {
	plan := explainPlan{
		Node:          "scan",
		Collection:    collection,
		AccessPath:    explainAccessPath(p.where),
		FilterColumns: explainColumns(p.valuePaths, p.filterCols),
		LateColumns:   explainColumns(p.valuePaths, p.lateCols),
		Aggregate:     p.hasAggregate,
		SingleRow:     p.singleRow,
	}
	if outputCount < 0 || outputCount > len(p.headers) {
		outputCount = len(p.headers)
	}
	plan.Output = append([]string(nil), p.headers[:outputCount]...)
	if p.where != nil {
		plan.Where = explainPredicate(p.where)
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
	if s == nil || s.tree == nil {
		return "", queryExplainError("query: cannot explain a nil or released Statement")
	}
	p, err := s.q.compiled()
	if err != nil {
		return "", err
	}
	return p.explainJSON(s.Collection(), s.outputs)
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

func explainAccessPath(predicate *compiledPredicate) string {
	if predicate == nil {
		return "full-scan"
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

func explainPredicate(predicate *compiledPredicate) string {
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
