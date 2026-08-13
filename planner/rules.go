package planner

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// RulePhase separates equivalence exploration from physical implementation.
type RulePhase uint8

const (
	ExplorePhase RulePhase = iota
	ImplementPhase
)

// Rule adds equivalent logical or physical expressions to a Memo. Priority is
// descending; Name breaks ties and makes scheduling independent of registration
// order.
type Rule interface {
	Name() string
	Phase() RulePhase
	Priority() int
	Match(*Memo, GroupID, ExprID, Expression) bool
	Apply(*RuleCall) error
}

// RuleCall is the bounded mutation surface available to one rule invocation.
type RuleCall struct {
	memo  *Memo
	group GroupID
	id    ExprID
	expr  Expression
}

func (c *RuleCall) Memo() *Memo      { return c.memo }
func (c *RuleCall) Group() GroupID   { return c.group }
func (c *RuleCall) ExprID() ExprID   { return c.id }
func (c *RuleCall) Expr() Expression { return cloneExpression(c.expr) }
func (c *RuleCall) Logical() LogicalProperties {
	return cloneLogicalProperties(c.memo.logical(c.group))
}

// Yield inserts another expression equivalent to the matched group.
func (c *RuleCall) Yield(expr Expression) (ExprID, bool, error) {
	return c.memo.Add(c.group, expr)
}

// NewGroup creates a child equivalence group and inserts its first expression.
func (c *RuleCall) NewGroup(logical LogicalProperties, expr Expression) (GroupID, ExprID, error) {
	group, err := c.memo.NewGroup(logical)
	if err != nil {
		return NoGroup, NoExpr, err
	}
	id, _, err := c.memo.Add(group, expr)
	if err != nil {
		c.memo.rollbackLastGroup()
		return NoGroup, NoExpr, err
	}
	return group, id, err
}

// FuncRule is the compact rule form for rules that do not need their own type.
type FuncRule struct {
	RuleName     string
	RulePhase    RulePhase
	RulePriority int
	MatchFunc    func(*Memo, GroupID, ExprID, Expression) bool
	ApplyFunc    func(*RuleCall) error
}

func (r FuncRule) Name() string     { return r.RuleName }
func (r FuncRule) Phase() RulePhase { return r.RulePhase }
func (r FuncRule) Priority() int    { return r.RulePriority }
func (r FuncRule) Match(m *Memo, g GroupID, id ExprID, e Expression) bool {
	return r.MatchFunc == nil || r.MatchFunc(m, g, id, e)
}
func (r FuncRule) Apply(call *RuleCall) error {
	if r.ApplyFunc == nil {
		return nil
	}
	return r.ApplyFunc(call)
}

// Explore applies each matching rule once to each expression, including
// expressions yielded during the run, until a fixed point or a hard budget.
func (m *Memo) Explore(ctx context.Context, rules []Rule) error {
	if m == nil {
		return ErrInvalidMemo
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ordered := slices.Clone(rules)
	for i := range ordered {
		if ordered[i] == nil || ordered[i].Name() == "" {
			return errors.New("planner: every rule requires a name")
		}
		if ordered[i].Phase() != ExplorePhase && ordered[i].Phase() != ImplementPhase {
			return fmt.Errorf("planner: rule %q has invalid phase %d", ordered[i].Name(), ordered[i].Phase())
		}
		for previous := 0; previous < i; previous++ {
			if ordered[previous].Name() == ordered[i].Name() {
				return fmt.Errorf("planner: duplicate rule %q", ordered[i].Name())
			}
		}
	}
	slices.SortStableFunc(ordered, func(a, b Rule) int {
		if a.Phase() != b.Phase() {
			return int(a.Phase()) - int(b.Phase())
		}
		if a.Priority() != b.Priority() {
			return b.Priority() - a.Priority()
		}
		if a.Name() < b.Name() {
			return -1
		}
		if a.Name() > b.Name() {
			return 1
		}
		return 0
	})
	// Reach an exploration fixed point before any implementation rule runs.
	// Otherwise an implementation rule attached to an earlier expression can
	// miss a logical equivalent yielded later in the same group.
	for _, phase := range [...]RulePhase{ExplorePhase, ImplementPhase} {
		for cursor := 0; cursor < len(m.expressions); cursor++ {
			if cursor&63 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			record := m.expressions[cursor]
			for _, rule := range ordered {
				if rule.Phase() != phase {
					continue
				}
				id := ExprID(cursor)
				if !rule.Match(m, record.group, id, record.expr) {
					continue
				}
				if m.ruleApps >= m.limits.MaxRuleApplications {
					return fmt.Errorf("%w: rule applications reached %d", ErrSearchBudget, m.limits.MaxRuleApplications)
				}
				m.ruleApps++
				call := &RuleCall{memo: m, group: record.group, id: id, expr: record.expr}
				if err := rule.Apply(call); err != nil {
					return fmt.Errorf("planner: rule %s: %w", rule.Name(), err)
				}
			}
		}
	}
	return ctx.Err()
}
