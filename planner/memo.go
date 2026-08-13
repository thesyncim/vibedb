package planner

import (
	"errors"
	"fmt"
	"slices"
	"unsafe"
)

var (
	// ErrSearchBudget reports that a hard memo, rule, or plan-search bound was
	// reached. The optimizer never silently returns the best plan seen so far.
	ErrSearchBudget = errors.New("planner: search budget exceeded")
	// ErrInvalidMemo reports a malformed group or expression reference.
	ErrInvalidMemo = errors.New("planner: invalid memo")
	// ErrMemoSealed reports a mutation or second rule exploration after a memo
	// has become the immutable input to physical search.
	ErrMemoSealed = errors.New("planner: memo is sealed")
)

// Limits bound optimizer work and retained search state. Zero selects a
// conservative default; negative values are not representable.
type Limits struct {
	MaxGroups           uint32
	MaxExpressions      uint32
	MaxRuleApplications uint32
	MaxPlans            uint32
	MaxPropertyStates   uint32
	MaxEnforcerSteps    uint32
	MaxSearchDepth      uint32
	// MaxMemoPayloadBytes bounds deterministic memo-owned records and slice
	// elements. Runtime map buckets and allocator capacity are bounded
	// indirectly by the cardinality limits and reported separately where stable.
	MaxMemoPayloadBytes uint64
	// MaxSearchPayloadBytes bounds deterministic property-state and physical
	// plan records allocated during one top-down search. Runtime map buckets,
	// model-owned alternatives, and allocator slack are bounded indirectly by
	// cardinality limits and are not charged here.
	MaxSearchPayloadBytes uint64
}

func (l Limits) withDefaults() Limits {
	if l.MaxGroups == 0 {
		l.MaxGroups = 4096
	}
	if l.MaxExpressions == 0 {
		l.MaxExpressions = 32768
	}
	if l.MaxRuleApplications == 0 {
		l.MaxRuleApplications = 131072
	}
	if l.MaxPlans == 0 {
		l.MaxPlans = 65536
	}
	if l.MaxPropertyStates == 0 {
		l.MaxPropertyStates = 65536
	}
	if l.MaxEnforcerSteps == 0 {
		l.MaxEnforcerSteps = 65536
	}
	if l.MaxSearchDepth == 0 {
		l.MaxSearchDepth = 1024
	}
	if l.MaxMemoPayloadBytes == 0 {
		l.MaxMemoPayloadBytes = 64 << 20
	}
	if l.MaxSearchPayloadBytes == 0 {
		l.MaxSearchPayloadBytes = 256 << 20
	}
	return l
}

type memoState uint8

const (
	memoBuilding memoState = iota
	memoExploring
	memoSealed
)

type memoGroup struct {
	logical         LogicalProperties
	firstExpression ExprID
	lastExpression  ExprID
	expressionCount uint32
}

type memoExpression struct {
	group GroupID
	expr  Expression
	next  ExprID
}

// Memo compactly stores equivalent relational expressions. It is mutable only
// while being built and during its single atomic rule exploration. Successful
// exploration seals it for physical search. Memo is not safe for concurrent
// use.
type Memo struct {
	limits       Limits
	groups       []memoGroup
	expressions  []memoExpression
	byHash       map[uint64][]ExprID
	ruleApps     uint32
	payloadBytes uint64
	state        memoState
}

// MemoStatistics reports bounded optimizer state. RetainedBytes covers the
// memo's owned flat slices and nested logical-property/child runs; Go map bucket
// implementation overhead is deliberately excluded because it is runtime
// version-specific.
type MemoStatistics struct {
	Groups           uint32
	Expressions      uint32
	RuleApplications uint32
	PayloadBytes     uint64
	RetainedBytes    uint64
}

// Statistics returns an instantaneous search-space snapshot.
func (m *Memo) Statistics() MemoStatistics {
	if m == nil {
		return MemoStatistics{}
	}
	stats := MemoStatistics{
		Groups: uint32(len(m.groups)), Expressions: uint32(len(m.expressions)),
		RuleApplications: m.ruleApps, PayloadBytes: m.payloadBytes,
		RetainedBytes: uint64(cap(m.groups))*uint64(unsafe.Sizeof(memoGroup{})) +
			uint64(cap(m.expressions))*uint64(unsafe.Sizeof(memoExpression{})),
	}
	for i := range m.groups {
		group := &m.groups[i]
		stats.RetainedBytes += uint64(cap(group.logical.Columns)) * uint64(unsafe.Sizeof(ColumnID(0)))
		stats.RetainedBytes += uint64(cap(group.logical.UniqueKeys)) * uint64(unsafe.Sizeof([]ColumnID(nil)))
		for _, key := range group.logical.UniqueKeys {
			stats.RetainedBytes += uint64(cap(key)) * uint64(unsafe.Sizeof(ColumnID(0)))
		}
	}
	for i := range m.expressions {
		stats.RetainedBytes += uint64(cap(m.expressions[i].expr.Children)) * uint64(unsafe.Sizeof(GroupID(0)))
	}
	return stats
}

func NewMemo(limits Limits) *Memo {
	return &Memo{
		limits: limits.withDefaults(), byHash: make(map[uint64][]ExprID),
	}
}

// NewGroup creates an empty equivalence group with immutable logical facts.
func (m *Memo) NewGroup(logical LogicalProperties) (GroupID, error) {
	if m == nil {
		return NoGroup, ErrInvalidMemo
	}
	if m.state == memoSealed {
		return NoGroup, ErrMemoSealed
	}
	if uint64(len(m.groups)) >= uint64(m.limits.MaxGroups) {
		return NoGroup, fmt.Errorf("%w: groups reached %d", ErrSearchBudget, m.limits.MaxGroups)
	}
	payload, ok := groupPayloadBytes(logical)
	if !ok || m.payloadBytes > m.limits.MaxMemoPayloadBytes ||
		payload > m.limits.MaxMemoPayloadBytes-m.payloadBytes {
		return NoGroup, fmt.Errorf("%w: memo payload exceeds %d bytes", ErrSearchBudget, m.limits.MaxMemoPayloadBytes)
	}
	group := GroupID(len(m.groups))
	m.groups = append(m.groups, memoGroup{
		logical: cloneLogicalProperties(logical), firstExpression: NoExpr, lastExpression: NoExpr,
	})
	m.payloadBytes += payload
	return group, nil
}

// Intern creates a group for expr, or reuses a group holding an identical root
// expression and identical logical properties.
func (m *Memo) Intern(expr Expression, logical LogicalProperties) (GroupID, ExprID, error) {
	if m == nil {
		return NoGroup, NoExpr, ErrInvalidMemo
	}
	if m.state == memoSealed {
		return NoGroup, NoExpr, ErrMemoSealed
	}
	hash := expressionHash(expr)
	for _, id := range m.byHash[hash] {
		record := m.expressions[id]
		if expressionEqual(record.expr, expr) && logicalEqual(m.groups[record.group].logical, logical) {
			return record.group, id, nil
		}
	}
	if expr.Op == OpInvalid {
		return NoGroup, NoExpr, ErrInvalidMemo
	}
	for _, child := range expr.Children {
		if int(child) >= len(m.groups) {
			return NoGroup, NoExpr, fmt.Errorf("%w: expression child group %d does not exist", ErrInvalidMemo, child)
		}
	}
	if uint64(len(m.expressions)) >= uint64(m.limits.MaxExpressions) {
		return NoGroup, NoExpr, fmt.Errorf("%w: expressions reached %d", ErrSearchBudget, m.limits.MaxExpressions)
	}
	group, err := m.NewGroup(logical)
	if err != nil {
		return NoGroup, NoExpr, err
	}
	id, _, err := m.Add(group, expr)
	if err != nil {
		// Add validates and checks its budget before mutation. Rolling back the
		// last group keeps Intern atomic if those invariants ever change.
		m.rollbackLastGroup()
		return NoGroup, NoExpr, err
	}
	return group, id, err
}

// Add inserts an expression equivalent to group. added is false for an exact
// duplicate already in that group.
func (m *Memo) Add(group GroupID, expr Expression) (id ExprID, added bool, err error) {
	if m == nil || int(group) >= len(m.groups) || expr.Op == OpInvalid {
		return NoExpr, false, ErrInvalidMemo
	}
	if m.state == memoSealed {
		return NoExpr, false, ErrMemoSealed
	}
	for _, child := range expr.Children {
		if int(child) >= len(m.groups) {
			return NoExpr, false, fmt.Errorf("%w: expression child group %d does not exist", ErrInvalidMemo, child)
		}
	}
	hash := expressionHash(expr)
	for _, candidate := range m.byHash[hash] {
		record := m.expressions[candidate]
		if record.group == group && expressionEqual(record.expr, expr) {
			return candidate, false, nil
		}
	}
	if uint64(len(m.expressions)) >= uint64(m.limits.MaxExpressions) {
		return NoExpr, false, fmt.Errorf("%w: expressions reached %d", ErrSearchBudget, m.limits.MaxExpressions)
	}
	payload, ok := expressionPayloadBytes(expr)
	if !ok || m.payloadBytes > m.limits.MaxMemoPayloadBytes ||
		payload > m.limits.MaxMemoPayloadBytes-m.payloadBytes {
		return NoExpr, false, fmt.Errorf("%w: memo payload exceeds %d bytes", ErrSearchBudget, m.limits.MaxMemoPayloadBytes)
	}
	id = ExprID(len(m.expressions))
	m.expressions = append(m.expressions, memoExpression{group: group, expr: cloneExpression(expr), next: NoExpr})
	owner := &m.groups[group]
	if owner.firstExpression == NoExpr {
		owner.firstExpression = id
	} else {
		m.expressions[owner.lastExpression].next = id
	}
	owner.lastExpression = id
	owner.expressionCount++
	m.byHash[hash] = append(m.byHash[hash], id)
	m.payloadBytes += payload
	return id, true, nil
}

func (m *Memo) rollbackLastGroup() {
	if m == nil || len(m.groups) == 0 {
		return
	}
	last := &m.groups[len(m.groups)-1]
	payload, ok := groupPayloadBytes(last.logical)
	if !ok || payload > m.payloadBytes {
		panic("planner: corrupt memo payload accounting")
	}
	m.payloadBytes -= payload
	m.groups = m.groups[:len(m.groups)-1]
}

// rollbackExploration restores the exact build-state boundary that preceded a
// failed or canceled rule run. Rules can only append groups and expressions,
// so rebuilding the compact links and hash directory is sufficient.
func (m *Memo) rollbackExploration(groups, expressions int, payload uint64, ruleApps uint32) {
	clear(m.groups[groups:])
	clear(m.expressions[expressions:])
	m.groups = m.groups[:groups]
	m.expressions = m.expressions[:expressions]
	m.payloadBytes = payload
	m.ruleApps = ruleApps
	m.byHash = make(map[uint64][]ExprID, expressions)
	for i := range m.groups {
		m.groups[i].firstExpression = NoExpr
		m.groups[i].lastExpression = NoExpr
		m.groups[i].expressionCount = 0
	}
	for i := range m.expressions {
		id := ExprID(i)
		record := &m.expressions[i]
		record.next = NoExpr
		owner := &m.groups[record.group]
		if owner.firstExpression == NoExpr {
			owner.firstExpression = id
		} else {
			m.expressions[owner.lastExpression].next = id
		}
		owner.lastExpression = id
		owner.expressionCount++
		hash := expressionHash(record.expr)
		m.byHash[hash] = append(m.byHash[hash], id)
	}
}

func groupPayloadBytes(logical LogicalProperties) (uint64, bool) {
	total := uint64(unsafe.Sizeof(memoGroup{}))
	var ok bool
	if total, ok = addMemoRun(total, len(logical.Columns), unsafe.Sizeof(ColumnID(0))); !ok {
		return 0, false
	}
	if total, ok = addMemoRun(total, len(logical.UniqueKeys), unsafe.Sizeof([]ColumnID(nil))); !ok {
		return 0, false
	}
	for _, key := range logical.UniqueKeys {
		if total, ok = addMemoRun(total, len(key), unsafe.Sizeof(ColumnID(0))); !ok {
			return 0, false
		}
	}
	return total, true
}

func expressionPayloadBytes(expr Expression) (uint64, bool) {
	total := uint64(unsafe.Sizeof(memoExpression{}))
	return addMemoRun(total, len(expr.Children), unsafe.Sizeof(GroupID(0)))
}

func addMemoRun(total uint64, count int, elementBytes uintptr) (uint64, bool) {
	if count < 0 || elementBytes == 0 || uint64(count) > ^uint64(0)/uint64(elementBytes) {
		return 0, false
	}
	run := uint64(count) * uint64(elementBytes)
	if run > ^uint64(0)-total {
		return 0, false
	}
	return total + run, true
}

// GroupCount and ExpressionCount expose bounded search-state cardinalities.
func (m *Memo) GroupCount() int {
	if m == nil {
		return 0
	}
	return len(m.groups)
}

func (m *Memo) ExpressionCount() int {
	if m == nil {
		return 0
	}
	return len(m.expressions)
}

// Logical returns a defensive copy of a group's logical properties.
func (m *Memo) Logical(group GroupID) (LogicalProperties, bool) {
	if m == nil || int(group) >= len(m.groups) {
		return LogicalProperties{}, false
	}
	return cloneLogicalProperties(m.groups[group].logical), true
}

func (m *Memo) logical(group GroupID) LogicalProperties { return m.groups[group].logical }

// Expression returns an expression and its owning equivalence group. The
// returned expression owns its Children slice.
func (m *Memo) Expression(id ExprID) (GroupID, Expression, bool) {
	if m == nil || int(id) >= len(m.expressions) {
		return NoGroup, Expression{}, false
	}
	record := m.expressions[id]
	return record.group, cloneExpression(record.expr), true
}

func (m *Memo) expression(id ExprID) memoExpression { return m.expressions[id] }

// Expressions returns the stable insertion-order expression run for group.
func (m *Memo) Expressions(group GroupID) []ExprID {
	if m == nil || int(group) >= len(m.groups) {
		return nil
	}
	record := &m.groups[group]
	if record.expressionCount == 0 {
		return nil
	}
	out := make([]ExprID, 0, record.expressionCount)
	for id := record.firstExpression; id != NoExpr; id = m.expressions[id].next {
		out = append(out, id)
	}
	return out
}

func expressionEqual(a, b Expression) bool {
	return a.Op == b.Op && a.Private == b.Private && slices.Equal(a.Children, b.Children)
}

func expressionHash(expr Expression) uint64 {
	h := hashUint32(fnv64Offset, uint32(expr.Op))
	h = hashUint32(h, uint32(expr.Private))
	for _, child := range expr.Children {
		h = hashUint32(h, uint32(child))
	}
	return h
}

func logicalEqual(a, b LogicalProperties) bool {
	if a.Rows != b.Rows || a.RowBytes != b.RowBytes || !slices.Equal(a.Columns, b.Columns) ||
		len(a.UniqueKeys) != len(b.UniqueKeys) {
		return false
	}
	for i := range a.UniqueKeys {
		if !slices.Equal(a.UniqueKeys[i], b.UniqueKeys[i]) {
			return false
		}
	}
	return true
}
