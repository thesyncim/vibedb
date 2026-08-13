package gateway

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"

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
	scanRows, outputRows, rowBytes := distributedEstimates(snap, bound, route)
	metadata := distributedPrivate{
		targets: len(route.Targets), shards: bound.manifest.ShardCount(),
		scanRows: scanRows, outputRows: outputRows, rowBytes: rowBytes,
		order: bound.order, aggregates: bound.aggregates,
	}
	if bound.limit > 0 && len(bound.aggregates) == 0 {
		metadata.outputRows = min(outputRows,
			float64(bound.limit)*float64(max(1, len(route.Targets))))
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

func distributedEstimates(
	snap *Snapshot,
	bound *BoundPlan,
	route distribution.Route,
) (scanRows, outputRows, rowBytes float64) {
	rowBytes = 128
	if len(route.Targets) == 0 {
		if statistics, ok := snap.Statistics(bound.table); ok {
			rowBytes = max(1, statistics.RowBytes().Normalize(rowBytes).Upper)
		}
		return 0, 0, rowBytes
	}
	scanRows = 1000 * float64(len(route.Targets))
	outputRows = scanRows
	statistics, ok := snap.Statistics(bound.table)
	if !ok {
		return scanRows, outputRows, rowBytes
	}
	tableRows := statistics.Rows().Normalize(scanRows).Upper
	rowBytes = statistics.RowBytes().Normalize(rowBytes).Upper
	partitionRows, completePartitions := 0.0, true
	for _, target := range route.Targets {
		estimate, exists := statistics.PartitionRows(string(target.Shard))
		if !exists {
			completePartitions = false
			break
		}
		partitionRows += estimate.Normalize(tableRows).Upper
	}
	if completePartitions {
		scanRows = min(tableRows, partitionRows)
	} else {
		// A whole-table upper bound cannot be divided by shard count under skew.
		// Without every selected partition estimate, retain the conservative
		// table bound rather than assuming uniform placement.
		scanRows = tableRows
	}
	outputRows = scanRows

	placement, _, _, placed := snap.plannerTableFor(bound.table)
	if !placed {
		return max(0, scanRows), max(0, outputRows), max(1, rowBytes)
	}
	selectivities := make([]float64, 0, len(bound.constraints))
	for ordinal, domain := range bound.constraints {
		if domain.Kind == distribution.DomainEmpty {
			return max(0, scanRows), 0, max(1, rowBytes)
		}
		if domain.Kind != distribution.DomainFinite || ordinal >= len(placement.Columns) {
			continue
		}
		column, exists := statistics.Column(placement.Columns[ordinal])
		if !exists {
			continue
		}
		selectivity := 0.0
		complete := true
		for _, value := range domain.Values {
			canonical, valid := boundStatisticScalar(value)
			if !valid {
				complete = false
				break
			}
			selectivity += column.EqualitySelectivityEstimate(canonical).Upper
		}
		if complete {
			selectivities = append(selectivities, min(1, selectivity))
		}
	}
	if len(selectivities) != 0 {
		// Exponential backoff applies the strongest predicate fully and reduces
		// the independence assumption for each additional key correlation.
		slices.Sort(selectivities)
		combined, exponent := 1.0, 1.0
		for _, selectivity := range selectivities {
			combined *= math.Pow(selectivity, exponent)
			exponent *= .5
		}
		outputRows = min(scanRows, tableRows*combined)
	}
	return max(0, scanRows), max(0, outputRows), max(1, rowBytes)
}

func boundStatisticScalar(value distribution.Scalar) (string, bool) {
	var encoded string
	switch value.Kind() {
	case distribution.KindString:
		raw, _ := value.StringValue()
		encoded = strconv.Quote(raw)
	case distribution.KindNumber:
		encoded, _ = value.NumberSpelling()
	default:
		return "", false
	}
	canonical, err := queryplanner.CanonicalScalarJSON(encoded)
	return canonical, err == nil
}
