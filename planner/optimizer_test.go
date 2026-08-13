package planner

import (
	"context"
	"errors"
	"testing"
)

type testPhysical struct {
	properties PhysicalProperties
	cost       Cost
}

type testModel struct {
	physical  map[PrivateID]testPhysical
	enforcers []EnforcerChain
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
