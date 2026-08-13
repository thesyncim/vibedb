package gateway

import (
	"context"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/distribution"
	queryplanner "github.com/thesyncim/vibedb/planner"
	bootstrap "github.com/thesyncim/vibedb/sql"
)

// distributedPrivate is immutable metadata behind one memo PrivateID. The
// gateway currently has one remote fragment, but the representation naturally
// extends to separately costed join/aggregate fragments and exchanges.
type distributedPrivate struct {
	targets    int
	shards     int
	scanRows   float64
	outputRows float64
	rowBytes   float64
	order      []OrderKey
	aggregates []bootstrap.AggKind
}

type distributedCostModel struct {
	private map[queryplanner.PrivateID]distributedPrivate
}

func (m *distributedCostModel) IsPhysical(expr queryplanner.Expression) bool {
	return expr.Op == queryplanner.OpRemoteQuery || expr.Op == queryplanner.OpFinalAggregate
}

func (m *distributedCostModel) ChildPropertyAlternatives(
	_ *queryplanner.Memo,
	_ queryplanner.GroupID,
	_ queryplanner.ExprID,
	expr queryplanner.Expression,
	_ queryplanner.PhysicalProperties,
) ([][]queryplanner.PhysicalProperties, error) {
	switch expr.Op {
	case queryplanner.OpRemoteQuery:
		return [][]queryplanner.PhysicalProperties{nil}, nil
	case queryplanner.OpFinalAggregate:
		return [][]queryplanner.PhysicalProperties{{{
			Distribution: queryplanner.Distribution{Kind: queryplanner.DistributionAny},
		}}}, nil
	default:
		return nil, nil
	}
}

func (m *distributedCostModel) Provided(
	_ *queryplanner.Memo,
	_ queryplanner.GroupID,
	_ queryplanner.ExprID,
	expr queryplanner.Expression,
	_ []*queryplanner.Plan,
) (queryplanner.PhysicalProperties, error) {
	metadata, ok := m.private[expr.Private]
	if !ok {
		return queryplanner.PhysicalProperties{}, fmt.Errorf("missing private metadata %d", expr.Private)
	}
	switch expr.Op {
	case queryplanner.OpRemoteQuery:
		distributionProperty := queryplanner.Distribution{Kind: queryplanner.DistributionSingleton, Partitions: 1}
		if metadata.targets > 1 {
			distributionProperty = queryplanner.Distribution{
				Kind: queryplanner.DistributionRandom, Partitions: uint32(metadata.targets),
			}
		}
		return queryplanner.PhysicalProperties{
			Distribution: distributionProperty,
			Ordering:     plannerOrdering(metadata.order),
		}, nil
	case queryplanner.OpFinalAggregate:
		return queryplanner.PhysicalProperties{
			Distribution: queryplanner.Distribution{Kind: queryplanner.DistributionSingleton, Partitions: 1},
		}, nil
	default:
		return queryplanner.PhysicalProperties{}, fmt.Errorf("unknown physical operator %s", expr.Op)
	}
}

func (m *distributedCostModel) LocalCost(
	_ *queryplanner.Memo,
	_ queryplanner.GroupID,
	_ queryplanner.ExprID,
	expr queryplanner.Expression,
	_ []*queryplanner.Plan,
) (queryplanner.Cost, error) {
	metadata, ok := m.private[expr.Private]
	if !ok {
		return queryplanner.Cost{}, fmt.Errorf("missing private metadata %d", expr.Private)
	}
	switch expr.Op {
	case queryplanner.OpRemoteQuery:
		// Shards execute in parallel, so Startup follows fan-out width while CPU
		// and IO retain total cluster work. Network belongs to the exchange.
		return queryplanner.Cost{
			Startup: float64(metadata.targets),
			CPU:     metadata.scanRows,
			IO:      metadata.scanRows * metadata.rowBytes,
			Memory:  min(metadata.scanRows*metadata.rowBytes, 1<<20),
		}, nil
	case queryplanner.OpFinalAggregate:
		rows := max(1, float64(metadata.targets))
		width := max(16, metadata.rowBytes)
		return queryplanner.Cost{
			CPU: rows * float64(len(metadata.aggregates)), Network: rows * width,
			Memory: width * float64(len(metadata.aggregates)),
		}, nil
	default:
		return queryplanner.Cost{}, fmt.Errorf("unknown physical operator %s", expr.Op)
	}
}

func (m *distributedCostModel) Enforcers(
	memo *queryplanner.Memo,
	group queryplanner.GroupID,
	provided queryplanner.PhysicalProperties,
	required queryplanner.PhysicalProperties,
) ([]queryplanner.EnforcerChain, error) {
	logical, ok := memo.Logical(group)
	if !ok {
		return nil, queryplanner.ErrInvalidMemo
	}
	rows := logical.Rows.Normalize(1000).Upper
	width := logical.RowBytes.Normalize(128).Upper
	if rows < 0 || width < 0 {
		return nil, queryplanner.ErrInvalidStatistics
	}
	if required.Distribution.Kind == queryplanner.DistributionSingleton &&
		provided.Distribution.Kind != queryplanner.DistributionSingleton {
		if len(required.Ordering) != 0 && orderingPrefix(provided.Ordering, required.Ordering) {
			return []queryplanner.EnforcerChain{{{
				Op: queryplanner.OpMergeGather,
				Provided: queryplanner.PhysicalProperties{
					Distribution: required.Distribution, Ordering: required.Ordering,
				},
				Cost: queryplanner.Cost{
					CPU:     rows * math.Log2(max(2, float64(provided.Distribution.Partitions))),
					Network: rows * width,
					Memory:  width * max(1, float64(provided.Distribution.Partitions)),
				},
			}}}, nil
		}
		gather := queryplanner.Enforcer{
			Op: queryplanner.OpGather,
			Provided: queryplanner.PhysicalProperties{
				Distribution: required.Distribution,
			},
			Cost: queryplanner.Cost{CPU: rows, Network: rows * width, Memory: width},
		}
		if len(required.Ordering) == 0 {
			return []queryplanner.EnforcerChain{{gather}}, nil
		}
		sort := queryplanner.Enforcer{
			Op: queryplanner.OpSort,
			Provided: queryplanner.PhysicalProperties{
				Distribution: required.Distribution, Ordering: required.Ordering,
			},
			Cost: queryplanner.Cost{
				CPU: rows * math.Log2(max(2, rows)), Memory: rows * width,
			},
		}
		return []queryplanner.EnforcerChain{{gather, sort}}, nil
	}
	if provided.Distribution.Kind == queryplanner.DistributionSingleton &&
		!orderingPrefix(provided.Ordering, required.Ordering) {
		return []queryplanner.EnforcerChain{{{
			Op: queryplanner.OpSort,
			Provided: queryplanner.PhysicalProperties{
				Distribution: provided.Distribution, Ordering: required.Ordering,
			},
			Cost: queryplanner.Cost{
				CPU: rows * math.Log2(max(2, rows)), Memory: rows * width,
			},
		}}}, nil
	}
	return nil, nil
}

func orderingPrefix(provided, required []queryplanner.OrderingColumn) bool {
	if len(required) > len(provided) {
		return false
	}
	for i := range required {
		if provided[i] != required[i] {
			return false
		}
	}
	return true
}

func plannerOrdering(order []OrderKey) []queryplanner.OrderingColumn {
	if len(order) == 0 {
		return nil
	}
	out := make([]queryplanner.OrderingColumn, len(order))
	for i := range order {
		direction := queryplanner.Ascending
		if order[i].Desc {
			direction = queryplanner.Descending
		}
		out[i] = queryplanner.OrderingColumn{
			Column: queryplanner.ColumnID(order[i].Column), Direction: direction,
			NullsFirst: !order[i].Desc,
		}
	}
	return out
}

func optimizeDistributedPlan(
	ctx context.Context,
	snap *Snapshot,
	bound *BoundPlan,
	route distribution.Route,
	profile Profile,
) (*queryplanner.Plan, queryplanner.OptimizerStatistics, error) {
	const privateID queryplanner.PrivateID = 1
	rows, rowBytes := distributedEstimates(snap, bound, route)
	metadata := distributedPrivate{
		targets: len(route.Targets), shards: bound.manifest.ShardCount(),
		scanRows: rows, outputRows: rows, rowBytes: rowBytes,
		order: bound.order, aggregates: bound.aggregates,
	}
	if bound.limit > 0 && len(bound.aggregates) == 0 {
		metadata.outputRows = min(rows, float64(bound.limit*max(1, len(route.Targets))))
	}
	if len(bound.aggregates) != 0 {
		metadata.outputRows = float64(len(route.Targets))
		metadata.rowBytes = max(metadata.rowBytes, float64(len(bound.aggregates))*16)
	}
	memo := queryplanner.NewMemo(queryplanner.Limits{})
	remoteLogical := queryplanner.LogicalProperties{
		Rows:     queryplanner.ExactEstimate(metadata.outputRows),
		RowBytes: queryplanner.ExactEstimate(metadata.rowBytes),
	}
	remoteGroup, err := memo.NewGroup(remoteLogical)
	if err != nil {
		return nil, queryplanner.OptimizerStatistics{}, err
	}
	if _, _, err := memo.Add(remoteGroup, queryplanner.Expression{
		Op: queryplanner.OpLogicalRemoteQuery, Private: privateID,
	}); err != nil {
		return nil, queryplanner.OptimizerStatistics{}, err
	}
	root := remoteGroup
	if len(bound.aggregates) != 0 && len(route.Targets) > 1 {
		root, err = memo.NewGroup(queryplanner.LogicalProperties{
			Rows:     queryplanner.ExactEstimate(1),
			RowBytes: queryplanner.ExactEstimate(metadata.rowBytes),
		})
		if err != nil {
			return nil, queryplanner.OptimizerStatistics{}, err
		}
		if _, _, err := memo.Add(root, queryplanner.Expression{
			Op: queryplanner.OpLogicalAggregate, Private: privateID,
			Children: []queryplanner.GroupID{remoteGroup},
		}); err != nil {
			return nil, queryplanner.OptimizerStatistics{}, err
		}
	}
	rules := []queryplanner.Rule{
		queryplanner.FuncRule{
			RuleName: "implement-remote-query", RulePhase: queryplanner.ImplementPhase,
			MatchFunc: func(_ *queryplanner.Memo, _ queryplanner.GroupID, _ queryplanner.ExprID, expr queryplanner.Expression) bool {
				return expr.Op == queryplanner.OpLogicalRemoteQuery
			},
			ApplyFunc: func(call *queryplanner.RuleCall) error {
				expr := call.Expr()
				expr.Op = queryplanner.OpRemoteQuery
				_, _, err := call.Yield(expr)
				return err
			},
		},
		queryplanner.FuncRule{
			RuleName: "implement-final-aggregate", RulePhase: queryplanner.ImplementPhase,
			MatchFunc: func(_ *queryplanner.Memo, _ queryplanner.GroupID, _ queryplanner.ExprID, expr queryplanner.Expression) bool {
				return expr.Op == queryplanner.OpLogicalAggregate
			},
			ApplyFunc: func(call *queryplanner.RuleCall) error {
				expr := call.Expr()
				expr.Op = queryplanner.OpFinalAggregate
				_, _, err := call.Yield(expr)
				return err
			},
		},
	}
	required := queryplanner.PhysicalProperties{
		Distribution: queryplanner.Distribution{Kind: queryplanner.DistributionSingleton, Partitions: 1},
	}
	if len(bound.aggregates) == 0 {
		required.Ordering = plannerOrdering(bound.order)
	}
	optimizer := queryplanner.Optimizer{
		Memo: memo, Rules: rules,
		Model:     &distributedCostModel{private: map[queryplanner.PrivateID]distributedPrivate{privateID: metadata}},
		Objective: queryplanner.Objective{MaxMemory: float64(profile.MaxAggregateBytes)},
	}
	plan, err := optimizer.Optimize(ctx, root, required)
	return plan, optimizer.Statistics(), err
}

func distributedEstimates(snap *Snapshot, bound *BoundPlan, route distribution.Route) (rows, rowBytes float64) {
	rows, rowBytes = 1000*float64(max(1, len(route.Targets))), 128
	statistics, ok := snap.Statistics(bound.table)
	if !ok {
		return rows, rowBytes
	}
	rows = statistics.Rows().Normalize(rows).Upper
	rowBytes = statistics.RowBytes().Normalize(rowBytes).Upper
	shards := max(1, bound.manifest.ShardCount())
	rows *= float64(len(route.Targets)) / float64(shards)
	return max(0, rows), max(1, rowBytes)
}
