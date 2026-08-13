package planner

import (
	"context"
	"errors"
	"math"
	"testing"
	"unsafe"
)

type testPhysical struct {
	properties PhysicalProperties
	cost       Cost
}

type testModel struct {
	physical  map[PrivateID]testPhysical
	enforcers []EnforcerChain
}

type unaryDepthModel struct{}

type repeatedDeadChildModel struct{}

type compositionTestModel struct {
	costs map[PrivateID]Cost
}

type concurrentCompositionTestModel struct {
	compositionTestModel
}

func (m compositionTestModel) IsPhysical(expr Expression) bool { return expr.Op == OpRemoteQuery }
func (m compositionTestModel) ChildPropertyAlternatives(
	_ *Memo, _ GroupID, _ ExprID, expr Expression, _ PhysicalProperties,
) ([][]PhysicalProperties, error) {
	if len(expr.Children) == 0 {
		return [][]PhysicalProperties{nil}, nil
	}
	required := PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}
	return [][]PhysicalProperties{{required}}, nil
}
func (m compositionTestModel) Provided(
	_ *Memo, _ GroupID, _ ExprID, _ Expression, _ []*Plan,
) (PhysicalProperties, error) {
	return PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}, nil
}
func (m compositionTestModel) LocalCost(
	_ *Memo, _ GroupID, _ ExprID, expr Expression, _ []*Plan,
) (Cost, error) {
	return m.costs[expr.Private], nil
}
func (m compositionTestModel) Enforcers(
	_ *Memo, _ GroupID, _, _ PhysicalProperties,
) ([]EnforcerChain, error) {
	return nil, nil
}

func (m concurrentCompositionTestModel) ComposeCost(
	_ *Memo, _ GroupID, _ ExprID, _ Expression, local Cost, children []*Plan,
) (Cost, error) {
	cost := local
	memory := local.Memory
	for _, child := range children {
		cost = cost.Plus(child.Cost)
		memory += child.Cost.Memory
	}
	cost.Memory = memory
	return cost, nil
}

func (repeatedDeadChildModel) IsPhysical(expr Expression) bool {
	return expr.Op == OpRemoteQuery || expr.Op == OpTableScan
}
func (repeatedDeadChildModel) ChildPropertyAlternatives(
	_ *Memo, _ GroupID, _ ExprID, expr Expression, _ PhysicalProperties,
) ([][]PhysicalProperties, error) {
	if len(expr.Children) == 0 {
		return [][]PhysicalProperties{nil}, nil
	}
	required := PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}
	return [][]PhysicalProperties{{required}, {required}}, nil
}
func (repeatedDeadChildModel) Provided(
	_ *Memo, _ GroupID, _ ExprID, expr Expression, _ []*Plan,
) (PhysicalProperties, error) {
	if expr.Op == OpTableScan {
		return PhysicalProperties{Distribution: Distribution{Kind: DistributionRandom}}, nil
	}
	return PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}, nil
}
func (repeatedDeadChildModel) LocalCost(
	_ *Memo, _ GroupID, _ ExprID, _ Expression, _ []*Plan,
) (Cost, error) {
	return Cost{}, nil
}
func (repeatedDeadChildModel) Enforcers(
	_ *Memo, _ GroupID, _, _ PhysicalProperties,
) ([]EnforcerChain, error) {
	return nil, nil
}

func (unaryDepthModel) IsPhysical(expr Expression) bool { return expr.Op == OpRemoteQuery }
func (unaryDepthModel) ChildPropertyAlternatives(
	_ *Memo, _ GroupID, _ ExprID, expr Expression, _ PhysicalProperties,
) ([][]PhysicalProperties, error) {
	if len(expr.Children) == 0 {
		return [][]PhysicalProperties{nil}, nil
	}
	return [][]PhysicalProperties{{{
		Distribution: Distribution{Kind: DistributionSingleton},
	}}}, nil
}
func (unaryDepthModel) Provided(
	_ *Memo, _ GroupID, _ ExprID, _ Expression, _ []*Plan,
) (PhysicalProperties, error) {
	return PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}, nil
}
func (unaryDepthModel) LocalCost(
	_ *Memo, _ GroupID, _ ExprID, _ Expression, _ []*Plan,
) (Cost, error) {
	return Cost{}, nil
}
func (unaryDepthModel) Enforcers(
	_ *Memo, _ GroupID, _, _ PhysicalProperties,
) ([]EnforcerChain, error) {
	return nil, nil
}

func (m testModel) IsPhysical(expr Expression) bool {
	return expr.Op == OpTableScan || expr.Op == OpIndexScan || expr.Op == OpRemoteQuery
}

func (m testModel) ChildPropertyAlternatives(
	_ *Memo, _ GroupID, _ ExprID, expr Expression, _ PhysicalProperties,
) ([][]PhysicalProperties, error) {
	if len(expr.Children) == 0 {
		return [][]PhysicalProperties{nil}, nil
	}
	return nil, nil
}

func (m testModel) Provided(
	_ *Memo, _ GroupID, _ ExprID, expr Expression, _ []*Plan,
) (PhysicalProperties, error) {
	return m.physical[expr.Private].properties, nil
}

func (m testModel) LocalCost(
	_ *Memo, _ GroupID, _ ExprID, expr Expression, _ []*Plan,
) (Cost, error) {
	return m.physical[expr.Private].cost, nil
}

func (m testModel) Enforcers(
	_ *Memo, _ GroupID, provided, required PhysicalProperties,
) ([]EnforcerChain, error) {
	if m.enforcers != nil {
		return m.enforcers, nil
	}
	if required.Distribution.Kind == DistributionSingleton &&
		provided.Distribution.Kind != DistributionSingleton {
		if len(required.Ordering) <= len(provided.Ordering) {
			return []EnforcerChain{{{
				Op: OpMergeGather, Provided: required,
				Cost: Cost{CPU: 5, Network: 20, Memory: 2},
			}}}, nil
		}
		return []EnforcerChain{{
			{Op: OpGather, Provided: PhysicalProperties{Distribution: required.Distribution}, Cost: Cost{Network: 20}},
			{Op: OpSort, Provided: required, Cost: Cost{CPU: 50, Memory: 100}},
		}}, nil
	}
	if provided.Distribution.Kind == DistributionSingleton &&
		!provided.Satisfies(required) {
		return []EnforcerChain{{{
			Op: OpSort, Provided: required, Cost: Cost{CPU: 50, Memory: 100},
		}}}, nil
	}
	return nil, nil
}

func implementationRule(name string, private PrivateID, op Operator) Rule {
	return FuncRule{
		RuleName: name, RulePhase: ImplementPhase,
		MatchFunc: func(_ *Memo, _ GroupID, _ ExprID, expr Expression) bool {
			return expr.Op == OpLogicalScan
		},
		ApplyFunc: func(call *RuleCall) error {
			_, _, err := call.Yield(Expression{Op: op, Private: private})
			return err
		},
	}
}

func TestOptimizerChoosesPropertyAwareWinner(t *testing.T) {
	memo := NewMemo(Limits{})
	root, err := memo.NewGroup(LogicalProperties{
		Rows: ExactEstimate(10_000), RowBytes: ExactEstimate(64), Columns: []ColumnID{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := memo.Add(root, Expression{Op: OpLogicalScan}); err != nil {
		t.Fatal(err)
	}
	ordered := []OrderingColumn{{Column: 1, Direction: Ascending, NullsFirst: true}}
	model := testModel{physical: map[PrivateID]testPhysical{
		1: {
			properties: PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton, Partitions: 1}},
			cost:       Cost{CPU: 5},
		},
		2: {
			properties: PhysicalProperties{
				Distribution: Distribution{Kind: DistributionSingleton, Partitions: 1}, Ordering: ordered,
			},
			cost: Cost{CPU: 20},
		},
	}}
	optimizer := Optimizer{
		Memo: memo, Model: model,
		Rules: []Rule{
			implementationRule("index-scan", 2, OpIndexScan),
			implementationRule("table-scan", 1, OpTableScan),
		},
	}
	plan, err := optimizer.Optimize(t.Context(), root, PhysicalProperties{
		Distribution: Distribution{Kind: DistributionSingleton, Partitions: 1}, Ordering: ordered,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Expression.Op != OpIndexScan {
		t.Fatalf("winner = %s\n%s", plan.Expression.Op, plan)
	}
	if !plan.Provided.Satisfies(PhysicalProperties{
		Distribution: Distribution{Kind: DistributionSingleton, Partitions: 1}, Ordering: ordered,
	}) {
		t.Fatalf("winner properties = %s", plan.Provided)
	}
	stats := optimizer.Statistics()
	if stats.Memo.Groups != 1 || stats.Memo.Expressions != 3 ||
		stats.Memo.RuleApplications != 2 || stats.PhysicalAlternatives != 2 ||
		stats.Memo.RetainedBytes == 0 || stats.PeakSearchDepth != 1 {
		t.Fatalf("optimizer statistics = %+v", stats)
	}
}

func TestOptimizerExploresOnceAndSupportsMultipleRequirements(t *testing.T) {
	memo := NewMemo(Limits{})
	root, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(root, Expression{Op: OpLogicalScan})
	model := testModel{physical: map[PrivateID]testPhysical{
		1: {
			properties: PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}},
			cost:       Cost{CPU: 1},
		},
	}}
	optimizer := &Optimizer{
		Memo: memo, Model: model,
		Rules: []Rule{implementationRule("table", 1, OpTableScan)},
	}
	for _, required := range []PhysicalProperties{
		{},
		{Distribution: Distribution{Kind: DistributionSingleton}},
	} {
		if _, err := optimizer.Optimize(t.Context(), root, required); err != nil {
			t.Fatal(err)
		}
		if got := optimizer.Statistics().Memo.RuleApplications; got != 1 {
			t.Fatalf("rule applications after requirement %s = %d, want 1", required, got)
		}
	}
	if memo.ExpressionCount() != 2 {
		t.Fatalf("repeated search replayed implementation rules: expressions=%d", memo.ExpressionCount())
	}
	if _, _, err := memo.Add(root, Expression{Op: OpIndexScan}); !errors.Is(err, ErrMemoSealed) {
		t.Fatalf("Add after exploration error = %v, want ErrMemoSealed", err)
	}
	if _, err := memo.NewGroup(LogicalProperties{}); !errors.Is(err, ErrMemoSealed) {
		t.Fatalf("NewGroup after exploration error = %v, want ErrMemoSealed", err)
	}
	if err := memo.Explore(t.Context(), nil); !errors.Is(err, ErrMemoSealed) {
		t.Fatalf("second Explore error = %v, want ErrMemoSealed", err)
	}
}

func TestRuleExplorationRollsBackAtomically(t *testing.T) {
	memo := NewMemo(Limits{})
	root, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(root, Expression{Op: OpLogicalScan})
	wantErr := errors.New("rule failed")
	rules := []Rule{
		FuncRule{
			RuleName: "a-yield", RulePhase: ExplorePhase,
			MatchFunc: func(_ *Memo, _ GroupID, _ ExprID, expr Expression) bool {
				return expr.Op == OpLogicalScan
			},
			ApplyFunc: func(call *RuleCall) error {
				_, _, err := call.Yield(Expression{Op: OpLogicalFilter})
				return err
			},
		},
		FuncRule{
			RuleName: "b-fail", RulePhase: ExplorePhase,
			MatchFunc: func(_ *Memo, _ GroupID, _ ExprID, expr Expression) bool {
				return expr.Op == OpLogicalScan
			},
			ApplyFunc: func(*RuleCall) error { return wantErr },
		},
	}
	if err := memo.Explore(t.Context(), rules); !errors.Is(err, wantErr) {
		t.Fatalf("Explore error = %v, want %v", err, wantErr)
	}
	stats := memo.Statistics()
	if stats.Groups != 1 || stats.Expressions != 1 || stats.RuleApplications != 0 {
		t.Fatalf("failed exploration leaked state: %+v", stats)
	}
	if err := memo.Explore(t.Context(), []Rule{implementationRule("table", 1, OpTableScan)}); err != nil {
		t.Fatalf("retry after atomic rollback: %v", err)
	}
	if memo.ExpressionCount() != 2 {
		t.Fatalf("retry expressions = %d, want 2", memo.ExpressionCount())
	}
}

func TestOptimizerCachesNegativePropertyStates(t *testing.T) {
	memo := NewMemo(Limits{})
	leaf, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(leaf, Expression{Op: OpTableScan})
	root, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(root, Expression{Op: OpRemoteQuery, Children: []GroupID{leaf}})
	optimizer := &Optimizer{Memo: memo, Model: repeatedDeadChildModel{}}
	_, err := optimizer.Optimize(t.Context(), root,
		PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}})
	if !errors.Is(err, ErrNoPlan) {
		t.Fatalf("Optimize error = %v, want ErrNoPlan", err)
	}
	stats := optimizer.Statistics()
	if stats.PropertyStates != 2 || stats.PropertyCacheEntries != 2 || stats.PropertyCacheHits != 1 {
		t.Fatalf("negative property memoization statistics = %+v", stats)
	}
}

func TestOptimizerSelectsMergeGatherForOrderedPartitions(t *testing.T) {
	memo := NewMemo(Limits{})
	root, _ := memo.NewGroup(LogicalProperties{Rows: ExactEstimate(100), RowBytes: ExactEstimate(16)})
	_, _, _ = memo.Add(root, Expression{Op: OpRemoteQuery, Private: 1})
	order := []OrderingColumn{{Column: 0, Direction: Descending}}
	model := testModel{physical: map[PrivateID]testPhysical{
		1: {
			properties: PhysicalProperties{
				Distribution: Distribution{Kind: DistributionRandom, Partitions: 8}, Ordering: order,
			},
			cost: Cost{CPU: 10},
		},
	}}
	plan, err := (&Optimizer{Memo: memo, Model: model}).Optimize(t.Context(), root, PhysicalProperties{
		Distribution: Distribution{Kind: DistributionSingleton, Partitions: 1}, Ordering: order,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Expression.Op != OpMergeGather || len(plan.Children) != 1 ||
		plan.Children[0].Expression.Op != OpRemoteQuery {
		t.Fatalf("physical plan:\n%s", plan)
	}
}

func TestMemoInternAndRuleDeduplication(t *testing.T) {
	memo := NewMemo(Limits{})
	logical := LogicalProperties{Rows: ExactEstimate(1)}
	firstGroup, firstExpr, err := memo.Intern(Expression{Op: OpLogicalScan, Private: 7}, logical)
	if err != nil {
		t.Fatal(err)
	}
	secondGroup, secondExpr, err := memo.Intern(Expression{Op: OpLogicalScan, Private: 7}, logical)
	if err != nil {
		t.Fatal(err)
	}
	if firstGroup != secondGroup || firstExpr != secondExpr || memo.GroupCount() != 1 || memo.ExpressionCount() != 1 {
		t.Fatalf("intern = %d/%d then %d/%d, groups=%d exprs=%d",
			firstGroup, firstExpr, secondGroup, secondExpr, memo.GroupCount(), memo.ExpressionCount())
	}
	rule := implementationRule("same", 9, OpTableScan)
	if err := memo.Explore(t.Context(), []Rule{rule, rule}); err == nil {
		t.Fatal("duplicate rule names were accepted")
	}
	if err := memo.Explore(t.Context(), []Rule{nil}); err == nil {
		t.Fatal("nil rule was accepted")
	}
}

func TestMemoGroupCreationIsAtomic(t *testing.T) {
	memo := NewMemo(Limits{MaxExpressions: 1})
	group, _, err := memo.Intern(Expression{Op: OpLogicalScan}, LogicalProperties{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := memo.Intern(Expression{Op: OpLogicalScan, Private: 2}, LogicalProperties{}); !errors.Is(err, ErrSearchBudget) {
		t.Fatalf("Intern error = %v, want ErrSearchBudget", err)
	}
	if memo.GroupCount() != 1 || group != 0 {
		t.Fatalf("failed Intern retained a group: groups=%d", memo.GroupCount())
	}
}

func TestMemoPayloadBudget(t *testing.T) {
	memo := NewMemo(Limits{MaxMemoPayloadBytes: 1})
	if _, err := memo.NewGroup(LogicalProperties{Columns: []ColumnID{1}}); !errors.Is(err, ErrSearchBudget) {
		t.Fatalf("NewGroup error = %v, want ErrSearchBudget", err)
	}
	if memo.GroupCount() != 0 || memo.Statistics().PayloadBytes != 0 {
		t.Fatalf("rejected group changed memo statistics: %+v", memo.Statistics())
	}

	memo = NewMemo(Limits{MaxMemoPayloadBytes: 1024})
	group, err := memo.NewGroup(LogicalProperties{Columns: []ColumnID{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	before := memo.Statistics().PayloadBytes
	memo.limits.MaxMemoPayloadBytes = before + uint64(unsafe.Sizeof(memoExpression{})) - 1
	if _, _, err := memo.Add(group, Expression{Op: OpLogicalScan}); !errors.Is(err, ErrSearchBudget) {
		t.Fatalf("Add error = %v, want ErrSearchBudget", err)
	}
	if got := memo.Statistics().PayloadBytes; got != before || memo.ExpressionCount() != 0 {
		t.Fatalf("rejected expression changed memo: bytes=%d expressions=%d", got, memo.ExpressionCount())
	}
}

func TestRulePhasesReachLogicalFixedPointBeforeImplementation(t *testing.T) {
	memo := NewMemo(Limits{})
	group, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(group, Expression{Op: OpLogicalScan})
	hasOperator := func(m *Memo, group GroupID, operator Operator) bool {
		for id := m.groups[group].firstExpression; id != NoExpr; id = m.expressions[id].next {
			if m.expression(id).expr.Op == operator {
				return true
			}
		}
		return false
	}
	rules := []Rule{
		FuncRule{
			RuleName: "explore-scan-to-filter", RulePhase: ExplorePhase,
			MatchFunc: func(_ *Memo, _ GroupID, _ ExprID, expr Expression) bool {
				return expr.Op == OpLogicalScan
			},
			ApplyFunc: func(call *RuleCall) error {
				_, _, err := call.Yield(Expression{Op: OpLogicalFilter})
				return err
			},
		},
		FuncRule{
			RuleName: "explore-filter-to-project", RulePhase: ExplorePhase,
			MatchFunc: func(_ *Memo, _ GroupID, _ ExprID, expr Expression) bool {
				return expr.Op == OpLogicalFilter
			},
			ApplyFunc: func(call *RuleCall) error {
				_, _, err := call.Yield(Expression{Op: OpLogicalProject})
				return err
			},
		},
		FuncRule{
			RuleName: "implement-scan-after-exploration", RulePhase: ImplementPhase,
			MatchFunc: func(m *Memo, group GroupID, _ ExprID, expr Expression) bool {
				return expr.Op == OpLogicalScan && hasOperator(m, group, OpLogicalProject)
			},
			ApplyFunc: func(call *RuleCall) error {
				_, _, err := call.Yield(Expression{Op: OpTableScan})
				return err
			},
		},
	}
	if err := memo.Explore(t.Context(), rules); err != nil {
		t.Fatal(err)
	}
	if !hasOperator(memo, group, OpTableScan) {
		t.Fatal("implementation ran before exploration reached a fixed point")
	}
}

func TestMemoInternReusesExpressionAddedDirectly(t *testing.T) {
	memo := NewMemo(Limits{})
	logical := LogicalProperties{Rows: ExactEstimate(3)}
	group, _ := memo.NewGroup(logical)
	direct, _, err := memo.Add(group, Expression{Op: OpLogicalScan, Private: 4})
	if err != nil {
		t.Fatal(err)
	}
	internedGroup, internedExpr, err := memo.Intern(Expression{Op: OpLogicalScan, Private: 4}, logical)
	if err != nil {
		t.Fatal(err)
	}
	if internedGroup != group || internedExpr != direct || memo.GroupCount() != 1 {
		t.Fatalf("interned = %d/%d, direct = %d/%d", internedGroup, internedExpr, group, direct)
	}
}

func TestMemoIndexSeparatesIdenticalExpressionsAcrossGroups(t *testing.T) {
	const count = 1024
	memo := NewMemo(Limits{})
	groups := make([]GroupID, count)
	for i := range count {
		logical := LogicalProperties{Rows: ExactEstimate(float64(i + 1))}
		group, err := memo.NewGroup(logical)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := memo.Add(group, Expression{Op: OpLogicalScan}); err != nil {
			t.Fatal(err)
		}
		groups[i] = group
	}
	for i := count - 1; i >= 0; i-- {
		group, _, err := memo.Intern(
			Expression{Op: OpLogicalScan},
			LogicalProperties{Rows: ExactEstimate(float64(i + 1))},
		)
		if err != nil {
			t.Fatal(err)
		}
		if group != groups[i] {
			t.Fatalf("Intern row estimate %d returned group %d, want %d", i+1, group, groups[i])
		}
	}
	stats := memo.Statistics()
	if stats.ExpressionIndexEntries != count*2 || stats.Expressions != count {
		t.Fatalf("memo index statistics = %+v", stats)
	}
}

func TestPlannerBudgetsAndCancellation(t *testing.T) {
	t.Run("expression budget", func(t *testing.T) {
		memo := NewMemo(Limits{MaxExpressions: 2, MaxRuleApplications: 10})
		group, _ := memo.NewGroup(LogicalProperties{})
		_, _, _ = memo.Add(group, Expression{Op: OpLogicalScan})
		rules := []Rule{
			implementationRule("one", 1, OpTableScan),
			implementationRule("two", 2, OpIndexScan),
		}
		if err := memo.Explore(t.Context(), rules); !errors.Is(err, ErrSearchBudget) {
			t.Fatalf("Explore error = %v, want ErrSearchBudget", err)
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		memo := NewMemo(Limits{})
		group, _ := memo.NewGroup(LogicalProperties{})
		_, _, _ = memo.Add(group, Expression{Op: OpLogicalScan})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := memo.Explore(ctx, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("Explore error = %v, want context.Canceled", err)
		}
	})

	t.Run("search depth", func(t *testing.T) {
		memo := NewMemo(Limits{MaxSearchDepth: 1})
		leaf, _ := memo.NewGroup(LogicalProperties{})
		_, _, _ = memo.Add(leaf, Expression{Op: OpRemoteQuery})
		root, _ := memo.NewGroup(LogicalProperties{})
		_, _, _ = memo.Add(root, Expression{Op: OpRemoteQuery, Children: []GroupID{leaf}})
		_, err := (&Optimizer{Memo: memo, Model: unaryDepthModel{}}).Optimize(t.Context(), root,
			PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}})
		if !errors.Is(err, ErrSearchBudget) {
			t.Fatalf("Optimize error = %v, want ErrSearchBudget", err)
		}
	})

	t.Run("property states", func(t *testing.T) {
		memo := NewMemo(Limits{MaxPropertyStates: 1})
		leaf, _ := memo.NewGroup(LogicalProperties{})
		_, _, _ = memo.Add(leaf, Expression{Op: OpTableScan})
		root, _ := memo.NewGroup(LogicalProperties{})
		_, _, _ = memo.Add(root, Expression{Op: OpRemoteQuery, Children: []GroupID{leaf}})
		_, err := (&Optimizer{Memo: memo, Model: repeatedDeadChildModel{}}).Optimize(t.Context(), root,
			PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}})
		if !errors.Is(err, ErrSearchBudget) {
			t.Fatalf("Optimize error = %v, want ErrSearchBudget", err)
		}
	})

	t.Run("search payload", func(t *testing.T) {
		required := PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}
		stateBytes, ok := propertyStatePayloadBytes(required)
		if !ok {
			t.Fatal("property payload overflow")
		}
		memo := NewMemo(Limits{MaxSearchPayloadBytes: stateBytes})
		root, _ := memo.NewGroup(LogicalProperties{})
		_, _, _ = memo.Add(root, Expression{Op: OpTableScan, Private: 1})
		optimizer := &Optimizer{
			Memo: memo,
			Model: testModel{physical: map[PrivateID]testPhysical{1: {
				properties: required,
			}}},
		}
		if _, err := optimizer.Optimize(t.Context(), root, required); !errors.Is(err, ErrSearchBudget) {
			t.Fatalf("Optimize error = %v, want ErrSearchBudget", err)
		}
		stats := optimizer.Statistics()
		if stats.SearchPayloadBytes != stateBytes || stats.PlanNodes != 0 {
			t.Fatalf("search payload statistics = %+v, want %d bytes and zero plan nodes", stats, stateBytes)
		}
	})
}

func TestPhysicalPropertySatisfaction(t *testing.T) {
	provided := PhysicalProperties{
		Distribution: Distribution{Kind: DistributionHash, Keys: []ColumnID{1, 2}, Partitions: 8},
		Ordering:     []OrderingColumn{{Column: 3}, {Column: 4, Direction: Descending}},
	}
	if !provided.Satisfies(PhysicalProperties{
		Distribution: Distribution{Kind: DistributionHash, Keys: []ColumnID{1, 2}},
		Ordering:     []OrderingColumn{{Column: 3}},
	}) {
		t.Fatal("provided properties did not satisfy wildcard partitions and ordering prefix")
	}
	if provided.Satisfies(PhysicalProperties{
		Distribution: Distribution{Kind: DistributionHash, Keys: []ColumnID{2, 1}},
	}) {
		t.Fatal("hash key order was treated as interchangeable")
	}
}

func TestPhysicalPropertyValidation(t *testing.T) {
	tests := []PhysicalProperties{
		{Distribution: Distribution{Kind: DistributionAny, Partitions: 1}},
		{Distribution: Distribution{Kind: DistributionSingleton, Keys: []ColumnID{1}}},
		{Distribution: Distribution{Kind: DistributionHash}},
		{Distribution: Distribution{Kind: DistributionRange, Keys: []ColumnID{1, 1}}},
		{Distribution: Distribution{Kind: DistributionRandom}, Ordering: []OrderingColumn{{Direction: 99}}},
	}
	for _, properties := range tests {
		if err := properties.Validate(); !errors.Is(err, ErrInvalidPhysicalProperties) {
			t.Fatalf("Validate(%+v) error = %v", properties, err)
		}
	}
	valid := PhysicalProperties{
		Distribution: Distribution{Kind: DistributionHash, Keys: []ColumnID{1, 2}, Partitions: 16},
		Ordering:     []OrderingColumn{{Column: 3, Direction: Descending}},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid properties rejected: %v", err)
	}
}

func TestOptimizerRejectsInvalidObjective(t *testing.T) {
	memo := NewMemo(Limits{})
	root, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(root, Expression{Op: OpTableScan, Private: 1})
	optimizer := Optimizer{
		Memo: memo,
		Model: testModel{physical: map[PrivateID]testPhysical{
			1: {properties: PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}},
		}},
		Objective: Objective{CPUWeight: -1},
	}
	if _, err := optimizer.Optimize(t.Context(), root, PhysicalProperties{}); !errors.Is(err, ErrInvalidObjective) {
		t.Fatalf("Optimize error = %v, want ErrInvalidObjective", err)
	}
}

func TestModelCanComposeConcurrentChildMemory(t *testing.T) {
	buildMemo := func() (*Memo, GroupID) {
		memo := NewMemo(Limits{})
		leaf, _ := memo.NewGroup(LogicalProperties{})
		_, _, _ = memo.Add(leaf, Expression{Op: OpRemoteQuery, Private: 1})
		root, _ := memo.NewGroup(LogicalProperties{})
		_, _, _ = memo.Add(root, Expression{
			Op: OpRemoteQuery, Private: 2, Children: []GroupID{leaf},
		})
		return memo, root
	}
	base := compositionTestModel{costs: map[PrivateID]Cost{
		1: {Memory: 80},
		2: {Memory: 30},
	}}
	required := PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}
	memo, root := buildMemo()
	sequential, err := (&Optimizer{
		Memo: memo, Model: base, Objective: Objective{CPUWeight: 1, MaxMemory: 100},
	}).Optimize(t.Context(), root, required)
	if err != nil {
		t.Fatal(err)
	}
	if sequential.Cost.Memory != 80 {
		t.Fatalf("default sequential memory = %v, want 80", sequential.Cost.Memory)
	}

	memo, root = buildMemo()
	optimizer := &Optimizer{
		Memo: memo, Model: concurrentCompositionTestModel{compositionTestModel: base},
		Objective: Objective{CPUWeight: 1, MaxMemory: 100},
	}
	if _, err := optimizer.Optimize(t.Context(), root, required); !errors.Is(err, ErrNoPlan) {
		t.Fatalf("concurrent Optimize error = %v, want ErrNoPlan", err)
	}
	if optimizer.Statistics().MemoryRejected != 1 {
		t.Fatalf("concurrent memory statistics = %+v", optimizer.Statistics())
	}
}

func TestOptimizerRejectsCostCompositionOverflow(t *testing.T) {
	memo := NewMemo(Limits{})
	leaf, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(leaf, Expression{Op: OpRemoteQuery, Private: 1})
	root, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(root, Expression{Op: OpRemoteQuery, Private: 2, Children: []GroupID{leaf}})
	model := compositionTestModel{costs: map[PrivateID]Cost{
		1: {CPU: math.MaxFloat64},
		2: {CPU: math.MaxFloat64},
	}}
	_, err := (&Optimizer{Memo: memo, Model: model}).Optimize(t.Context(), root,
		PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}})
	if !errors.Is(err, ErrInvalidCost) {
		t.Fatalf("Optimize error = %v, want ErrInvalidCost", err)
	}
}

func TestOptimizerRejectsEnforcerCostOverflow(t *testing.T) {
	memo := NewMemo(Limits{})
	root, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(root, Expression{Op: OpTableScan, Private: 1})
	required := PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}
	model := testModel{
		physical: map[PrivateID]testPhysical{1: {
			properties: PhysicalProperties{Distribution: Distribution{Kind: DistributionRandom}},
			cost:       Cost{CPU: math.MaxFloat64},
		}},
		enforcers: []EnforcerChain{{{
			Op: OpGather, Provided: required, Cost: Cost{CPU: math.MaxFloat64},
		}}},
	}
	if _, err := (&Optimizer{Memo: memo, Model: model}).Optimize(t.Context(), root, required); !errors.Is(err, ErrInvalidCost) {
		t.Fatalf("Optimize error = %v, want ErrInvalidCost", err)
	}
}

func TestOptimizerBoundsEnforcerAlternatives(t *testing.T) {
	memo := NewMemo(Limits{MaxPlans: 1})
	root, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(root, Expression{Op: OpTableScan, Private: 1})
	required := PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}
	model := testModel{
		physical: map[PrivateID]testPhysical{1: {
			properties: PhysicalProperties{Distribution: Distribution{Kind: DistributionRandom}},
		}},
		enforcers: []EnforcerChain{
			{{Op: OpGather, Provided: required}},
			{{Op: OpGather, Provided: required}},
		},
	}
	if _, err := (&Optimizer{Memo: memo, Model: model}).Optimize(t.Context(), root, required); !errors.Is(err, ErrSearchBudget) {
		t.Fatalf("Optimize error = %v, want ErrSearchBudget", err)
	}
}

func TestOptimizerBoundsEnforcerSteps(t *testing.T) {
	memo := NewMemo(Limits{MaxEnforcerSteps: 1})
	root, _ := memo.NewGroup(LogicalProperties{})
	_, _, _ = memo.Add(root, Expression{Op: OpTableScan, Private: 1})
	required := PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}
	model := testModel{
		physical: map[PrivateID]testPhysical{1: {
			properties: PhysicalProperties{Distribution: Distribution{Kind: DistributionRandom}},
		}},
		enforcers: []EnforcerChain{{
			{Op: OpGather, Provided: required},
			{Op: OpSort, Provided: required},
		}},
	}
	optimizer := &Optimizer{Memo: memo, Model: model}
	if _, err := optimizer.Optimize(t.Context(), root, required); !errors.Is(err, ErrSearchBudget) {
		t.Fatalf("Optimize error = %v, want ErrSearchBudget", err)
	}
	if got := optimizer.Statistics().EnforcerSteps; got != 1 {
		t.Fatalf("enforcer steps = %d, want 1", got)
	}
}

func TestPlanWinnerIndependentOfEnforcerOrder(t *testing.T) {
	optimize := func(enforcers []EnforcerChain) *Plan {
		memo := NewMemo(Limits{})
		root, _ := memo.NewGroup(LogicalProperties{})
		_, _, _ = memo.Add(root, Expression{Op: OpTableScan, Private: 1})
		model := testModel{
			physical: map[PrivateID]testPhysical{1: {
				properties: PhysicalProperties{Distribution: Distribution{Kind: DistributionRandom}},
			}},
			enforcers: enforcers,
		}
		plan, err := (&Optimizer{Memo: memo, Model: model}).Optimize(t.Context(), root,
			PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}})
		if err != nil {
			t.Fatal(err)
		}
		return plan
	}
	first := EnforcerChain{{
		Op: OpGather,
		Provided: PhysicalProperties{
			Distribution: Distribution{Kind: DistributionSingleton},
			Ordering:     []OrderingColumn{{Column: 1}},
		},
	}}
	second := EnforcerChain{{
		Op: OpGather,
		Provided: PhysicalProperties{
			Distribution: Distribution{Kind: DistributionSingleton},
			Ordering:     []OrderingColumn{{Column: 2}},
		},
	}}
	forward := optimize([]EnforcerChain{first, second})
	reverse := optimize([]EnforcerChain{second, first})
	if forward.Fingerprint() != reverse.Fingerprint() || !forward.Provided.Equal(reverse.Provided) {
		t.Fatalf("winner follows enforcer order:\nforward=%s\nreverse=%s", forward, reverse)
	}
}

func TestPlanFingerprintIsFixedWidthAndCollisionSafeForTieBreaking(t *testing.T) {
	left := &Plan{Expression: Expression{Op: OpTableScan}, Cost: Cost{CPU: 1}}
	right := &Plan{Expression: Expression{Op: OpIndexScan}, Cost: Cost{CPU: 1}}
	left.fingerprint = planFingerprint(left)
	right.fingerprint = planFingerprint(right)
	if len(left.Fingerprint()) != 32 || left.Fingerprint() == right.Fingerprint() {
		t.Fatalf("fingerprints = %q and %q", left.Fingerprint(), right.Fingerprint())
	}

	// Force the digest collision path: exact structure, not insertion order,
	// remains the final authority.
	right.fingerprint = left.fingerprint
	objective := Objective{CPUWeight: 1}
	if !betterPlan(left, right, objective) || betterPlan(right, left, objective) {
		t.Fatal("constructed digest collision did not use exact structural ordering")
	}
}

func BenchmarkMemoizedPropertyOptimization(b *testing.B) {
	var ownedMemoBytes uint64
	for range b.N {
		memo := NewMemo(Limits{})
		root, _ := memo.NewGroup(LogicalProperties{Rows: ExactEstimate(10_000), RowBytes: ExactEstimate(64)})
		_, _, _ = memo.Add(root, Expression{Op: OpLogicalScan})
		model := testModel{physical: map[PrivateID]testPhysical{
			1: {properties: PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}, cost: Cost{CPU: 100}},
			2: {properties: PhysicalProperties{Distribution: Distribution{Kind: DistributionSingleton}}, cost: Cost{CPU: 10}},
		}}
		optimizer := Optimizer{
			Memo: memo, Model: model,
			Rules: []Rule{
				implementationRule("table", 1, OpTableScan),
				implementationRule("index", 2, OpIndexScan),
			},
		}
		if _, err := optimizer.Optimize(context.Background(), root, PhysicalProperties{
			Distribution: Distribution{Kind: DistributionSingleton},
		}); err != nil {
			b.Fatal(err)
		}
		ownedMemoBytes = optimizer.Statistics().Memo.RetainedBytes
	}
	b.ReportMetric(float64(ownedMemoBytes), "owned-memo-B/op")
}

func BenchmarkMemoAddSameExpressionAcross1KGroups(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		memo := NewMemo(Limits{})
		for i := range 1024 {
			group, err := memo.NewGroup(LogicalProperties{Rows: ExactEstimate(float64(i + 1))})
			if err != nil {
				b.Fatal(err)
			}
			if _, _, err := memo.Add(group, Expression{Op: OpLogicalScan}); err != nil {
				b.Fatal(err)
			}
		}
	}
}
