package planner

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

var (
	// ErrNoPlan reports that no physical expression and enforcer chain can
	// satisfy the requested properties under the active objective.
	ErrNoPlan = errors.New("planner: no physical plan satisfies the required properties")
	// ErrInvalidCost reports a model that returned NaN, infinity, or a negative
	// cost component.
	ErrInvalidCost = errors.New("planner: model returned an invalid cost")
	// ErrInvalidObjective reports negative, NaN, or infinite weights or memory.
	ErrInvalidObjective = errors.New("planner: invalid cost objective")
)

// Enforcer is one physical glue operator applied above an otherwise valid
// physical plan. Chains are applied in slice order.
type Enforcer struct {
	Op       Operator
	Private  PrivateID
	Provided PhysicalProperties
	Cost     Cost
}

type EnforcerChain []Enforcer

// Model supplies engine-specific physical semantics and costs. Returning no
// child-property alternatives makes an expression ineligible. A leaf normally
// returns one empty alternative: [][]PhysicalProperties{nil}.
type Model interface {
	IsPhysical(Expression) bool
	ChildPropertyAlternatives(*Memo, GroupID, ExprID, Expression, PhysicalProperties) ([][]PhysicalProperties, error)
	Provided(*Memo, GroupID, ExprID, Expression, []*Plan) (PhysicalProperties, error)
	LocalCost(*Memo, GroupID, ExprID, Expression, []*Plan) (Cost, error)
	Enforcers(*Memo, GroupID, PhysicalProperties, PhysicalProperties) ([]EnforcerChain, error)
}

// Plan is one immutable winning physical tree.
type Plan struct {
	Group       GroupID
	Expr        ExprID
	Expression  Expression
	Children    []*Plan
	Provided    PhysicalProperties
	Cost        Cost
	fingerprint string
}

// Fingerprint is a stable structural tie-breaker and diagnostic identity.
func (p *Plan) Fingerprint() string {
	if p == nil {
		return ""
	}
	return p.fingerprint
}

// String renders a compact, deterministic physical tree.
func (p *Plan) String() string {
	if p == nil {
		return "<nil>"
	}
	var b strings.Builder
	p.appendText(&b, 0)
	return b.String()
}

func (p *Plan) appendText(b *strings.Builder, depth int) {
	b.WriteString(strings.Repeat("  ", depth))
	b.WriteString(p.Expression.Op.String())
	if p.Expression.Private != 0 {
		fmt.Fprintf(b, " private=%d", p.Expression.Private)
	}
	fmt.Fprintf(b, " [%s] score-cost={startup=%.3f cpu=%.3f io=%.3f net=%.3f memory=%.0f}",
		p.Provided.String(), p.Cost.Startup, p.Cost.CPU, p.Cost.IO, p.Cost.Network, p.Cost.Memory)
	for _, child := range p.Children {
		b.WriteByte('\n')
		child.appendText(b, depth+1)
	}
}

// Optimizer performs one bounded rule exploration followed by top-down
// property-aware dynamic programming.
type Optimizer struct {
	Memo      *Memo
	Rules     []Rule
	Model     Model
	Objective Objective

	cache  map[bestKey][]bestEntry
	active map[bestKey][]PhysicalProperties
	plans  uint32
	ctx    context.Context
	stats  OptimizerStatistics
	depth  uint32
}

// OptimizerStatistics makes planning work and space directly benchmarkable.
// It contains no wall-clock timing; callers can time Optimize without making a
// deterministic planner depend on a clock.
type OptimizerStatistics struct {
	Memo                 MemoStatistics
	PhysicalAlternatives uint32
	PropertyCacheHits    uint32
	EnforcerPlans        uint32
	MemoryRejected       uint32
	PeakSearchDepth      uint32
}

// Statistics returns the completed or most recent optimization counters.
func (o *Optimizer) Statistics() OptimizerStatistics {
	if o == nil {
		return OptimizerStatistics{}
	}
	return o.stats
}

type bestKey struct {
	group GroupID
	hash  uint64
}

type bestEntry struct {
	required PhysicalProperties
	plan     *Plan
}

// Optimize explores rules, then returns the cheapest plan satisfying required.
func (o *Optimizer) Optimize(ctx context.Context, root GroupID, required PhysicalProperties) (*Plan, error) {
	if o == nil || o.Memo == nil || o.Model == nil || int(root) >= o.Memo.GroupCount() {
		return nil, ErrInvalidMemo
	}
	if ctx == nil {
		ctx = context.Background()
	}
	o.stats = OptimizerStatistics{}
	o.plans = 0
	defer func() {
		o.stats.Memo = o.Memo.Statistics()
		o.stats.PhysicalAlternatives = o.plans
	}()
	if err := o.Memo.Explore(ctx, o.Rules); err != nil {
		return nil, err
	}
	o.ctx = ctx
	o.Objective = o.Objective.withDefaults()
	if !o.Objective.valid() {
		return nil, ErrInvalidObjective
	}
	o.cache = make(map[bestKey][]bestEntry)
	o.active = make(map[bestKey][]PhysicalProperties)
	o.depth = 0
	plan, err := o.optimizeGroup(root, clonePhysicalProperties(required))
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (o *Optimizer) optimizeGroup(group GroupID, required PhysicalProperties) (*Plan, error) {
	o.depth++
	o.stats.PeakSearchDepth = max(o.stats.PeakSearchDepth, o.depth)
	defer func() { o.depth-- }()
	if err := o.ctx.Err(); err != nil {
		return nil, err
	}
	key := bestKey{group: group, hash: required.hash()}
	for _, entry := range o.cache[key] {
		if entry.required.Equal(required) {
			o.stats.PropertyCacheHits++
			return entry.plan, nil
		}
	}
	for _, active := range o.active[key] {
		if active.Equal(required) {
			return nil, fmt.Errorf("%w: cyclic dependency at group %d requiring %s", ErrInvalidMemo, group, required.String())
		}
	}
	o.active[key] = append(o.active[key], clonePhysicalProperties(required))
	defer func() {
		values := o.active[key]
		values = values[:len(values)-1]
		if len(values) == 0 {
			delete(o.active, key)
		} else {
			o.active[key] = values
		}
	}()

	var best *Plan
	for _, id := range o.Memo.groups[group].expressions {
		record := o.Memo.expression(id)
		if !o.Model.IsPhysical(record.expr) {
			continue
		}
		alternatives, err := o.Model.ChildPropertyAlternatives(o.Memo, group, id, record.expr, required)
		if err != nil {
			return nil, fmt.Errorf("planner: child properties for %s: %w", record.expr.Op, err)
		}
		for _, childRequired := range alternatives {
			if o.plans >= o.Memo.limits.MaxPlans {
				return nil, fmt.Errorf("%w: physical alternatives reached %d", ErrSearchBudget, o.Memo.limits.MaxPlans)
			}
			o.plans++
			if o.plans&63 == 0 {
				if err := o.ctx.Err(); err != nil {
					return nil, err
				}
			}
			if len(childRequired) != len(record.expr.Children) {
				return nil, fmt.Errorf("%w: %s has %d children but model returned %d requirements",
					ErrInvalidMemo, record.expr.Op, len(record.expr.Children), len(childRequired))
			}
			children := make([]*Plan, len(record.expr.Children))
			eligible := true
			for i, childGroup := range record.expr.Children {
				child, childErr := o.optimizeGroup(childGroup, childRequired[i])
				if childErr != nil {
					if errors.Is(childErr, ErrNoPlan) {
						eligible = false
						break
					}
					return nil, childErr
				}
				children[i] = child
			}
			if !eligible {
				continue
			}
			candidate, err := o.buildCandidate(group, id, record.expr, children, required)
			if err != nil {
				return nil, err
			}
			if betterPlan(candidate, best, o.Objective) {
				best = candidate
			}
		}
	}
	if best == nil {
		return nil, fmt.Errorf("%w: group %d requires %s", ErrNoPlan, group, required.String())
	}
	o.cache[key] = append(o.cache[key], bestEntry{required: clonePhysicalProperties(required), plan: best})
	return best, nil
}

func (o *Optimizer) buildCandidate(
	group GroupID,
	id ExprID,
	expr Expression,
	children []*Plan,
	required PhysicalProperties,
) (*Plan, error) {
	provided, err := o.Model.Provided(o.Memo, group, id, expr, children)
	if err != nil {
		return nil, fmt.Errorf("planner: properties for %s: %w", expr.Op, err)
	}
	local, err := o.Model.LocalCost(o.Memo, group, id, expr, children)
	if err != nil {
		return nil, fmt.Errorf("planner: cost for %s: %w", expr.Op, err)
	}
	if !local.valid() {
		return nil, fmt.Errorf("%w: %s returned %+v", ErrInvalidCost, expr.Op, local)
	}
	cost := local
	for _, child := range children {
		cost = cost.Plus(child.Cost)
	}
	base := &Plan{
		Group: group, Expr: id, Expression: cloneExpression(expr),
		Children: slices.Clone(children), Provided: clonePhysicalProperties(provided), Cost: cost,
	}
	base.fingerprint = planFingerprint(base)
	if provided.Satisfies(required) {
		if !o.Objective.feasible(cost) {
			o.stats.MemoryRejected++
			return nil, nil
		}
		return base, nil
	}
	chains, err := o.Model.Enforcers(o.Memo, group, provided, required)
	if err != nil {
		return nil, fmt.Errorf("planner: enforcers for group %d: %w", group, err)
	}
	var best *Plan
	for _, chain := range chains {
		if o.stats.EnforcerPlans >= o.Memo.limits.MaxPlans {
			return nil, fmt.Errorf("%w: enforcer alternatives reached %d", ErrSearchBudget, o.Memo.limits.MaxPlans)
		}
		o.stats.EnforcerPlans++
		if o.stats.EnforcerPlans&63 == 0 {
			if err := o.ctx.Err(); err != nil {
				return nil, err
			}
		}
		candidate := base
		for _, step := range chain {
			if step.Op == OpInvalid || !step.Cost.valid() {
				return nil, fmt.Errorf("%w: invalid enforcer for group %d", ErrInvalidCost, group)
			}
			candidate = &Plan{
				Group: group, Expr: NoExpr,
				Expression: Expression{Op: step.Op, Private: step.Private, Children: []GroupID{group}},
				Children:   []*Plan{candidate}, Provided: clonePhysicalProperties(step.Provided),
				Cost: candidate.Cost.Plus(step.Cost),
			}
			candidate.fingerprint = planFingerprint(candidate)
		}
		if !o.Objective.feasible(candidate.Cost) {
			o.stats.MemoryRejected++
			continue
		}
		if candidate.Provided.Satisfies(required) &&
			betterPlan(candidate, best, o.Objective) {
			best = candidate
		}
	}
	return best, nil
}

func betterPlan(candidate, current *Plan, objective Objective) bool {
	if candidate == nil {
		return false
	}
	if current == nil {
		return true
	}
	a, b := objective.Score(candidate.Cost), objective.Score(current.Cost)
	if a != b {
		return a < b
	}
	// Stable dimension comparisons avoid a platform-dependent winner if two
	// weighted sums round to the same float.
	for _, pair := range [][2]float64{
		{candidate.Cost.Network, current.Cost.Network},
		{candidate.Cost.IO, current.Cost.IO},
		{candidate.Cost.CPU, current.Cost.CPU},
		{candidate.Cost.Startup, current.Cost.Startup},
		{candidate.Cost.Memory, current.Cost.Memory},
	} {
		if pair[0] != pair[1] {
			return pair[0] < pair[1]
		}
	}
	return candidate.fingerprint < current.fingerprint
}

func planFingerprint(p *Plan) string {
	var b strings.Builder
	b.WriteString(strconv.FormatUint(uint64(p.Expression.Op), 10))
	b.WriteByte(':')
	b.WriteString(strconv.FormatUint(uint64(p.Expression.Private), 10))
	b.WriteByte('[')
	for i, child := range p.Children {
		if i != 0 {
			b.WriteByte(',')
		}
		b.WriteString(child.fingerprint)
	}
	b.WriteByte(']')
	return b.String()
}

// Score reports a plan's objective score.
func (o Objective) PlanScore(plan *Plan) float64 {
	if plan == nil {
		return math.Inf(1)
	}
	return o.withDefaults().Score(plan.Cost)
}
