package gateway

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	queryplanner "github.com/thesyncim/vibedb/planner"
	bootstrap "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson/x/byteview"
)

// distributedPrivate is immutable metadata behind one memo PrivateID. The
// gateway currently has one remote fragment, but the representation naturally
// extends to separately costed join/aggregate fragments and exchanges.
type distributedPrivate struct {
	targets     int
	shards      int
	scanRows    float64
	scanBytes   float64
	outputRows  float64
	rowBytes    float64
	order       []OrderKey
	aggregates  []bootstrap.AggKind
	groupKeys   []int
	limit       int
	indexLookup bool
}

type distributedCostModel struct {
	private map[queryplanner.PrivateID]distributedPrivate
}

func (m *distributedCostModel) IsPhysical(expr queryplanner.Expression) bool {
	return expr.Op == queryplanner.OpRemoteQuery || expr.Op == queryplanner.OpIndexScan ||
		expr.Op == queryplanner.OpFinalAggregate
}

func (m *distributedCostModel) ChildPropertyAlternatives(
	_ *queryplanner.Memo,
	_ queryplanner.GroupID,
	_ queryplanner.ExprID,
	expr queryplanner.Expression,
	_ queryplanner.PhysicalProperties,
) ([][]queryplanner.PhysicalProperties, error) {
	switch expr.Op {
	case queryplanner.OpRemoteQuery, queryplanner.OpIndexScan:
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
	case queryplanner.OpRemoteQuery, queryplanner.OpIndexScan:
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
	case queryplanner.OpRemoteQuery, queryplanner.OpIndexScan:
		// Shards execute in parallel, so Startup follows fan-out width while CPU
		// and IO retain total cluster work. Network belongs to the exchange.
		startup := float64(metadata.targets)
		network := 0.0
		if metadata.indexLookup {
			startup++
			network = boundedProduct(max(1, metadata.outputRows), max(16, metadata.rowBytes))
		}
		return queryplanner.Cost{
			Startup: startup,
			CPU:     metadata.scanRows,
			IO:      metadata.scanBytes,
			Network: network,
			Memory:  min(metadata.scanBytes, 1<<20),
		}, nil
	case queryplanner.OpFinalAggregate:
		rows := max(1, float64(metadata.targets))
		if len(metadata.groupKeys) != 0 {
			rows = max(1, metadata.outputRows)
		}
		width := max(16, metadata.rowBytes)
		return queryplanner.Cost{
			CPU:     boundedProduct(rows, float64(len(metadata.aggregates))),
			Network: boundedProduct(rows, width),
			Memory:  boundedProduct(width, float64(len(metadata.aggregates))),
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
					CPU:     boundedProduct(rows, math.Log2(max(2, float64(provided.Distribution.Partitions)))),
					Network: boundedProduct(rows, width),
					Memory:  boundedProduct(width, max(1, float64(provided.Distribution.Partitions))),
				},
			}}}, nil
		}
		gather := queryplanner.Enforcer{
			Op: queryplanner.OpGather,
			Provided: queryplanner.PhysicalProperties{
				Distribution: required.Distribution,
			},
			Cost: queryplanner.Cost{CPU: rows, Network: boundedProduct(rows, width), Memory: width},
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
				CPU: boundedProduct(rows, math.Log2(max(2, rows))), Memory: boundedProduct(rows, width),
			},
		}
		return []queryplanner.EnforcerChain{{gather, sort}}, nil
	}
	if provided.Distribution.Kind == queryplanner.DistributionSingleton &&
		!orderingPrefix(provided.Ordering, required.Ordering) {
		op := queryplanner.OpSort
		cpuRows, memoryRows := rows, rows
		if metadata, ok := m.private[1]; ok && len(metadata.groupKeys) != 0 && metadata.limit > 0 {
			op = queryplanner.OpTopK
			memoryRows = min(rows, float64(metadata.limit))
			cpuRows = max(2, memoryRows)
		}
		return []queryplanner.EnforcerChain{{{
			Op: op,
			Provided: queryplanner.PhysicalProperties{
				Distribution: provided.Distribution, Ordering: required.Ordering,
			},
			Cost: queryplanner.Cost{
				CPU: boundedProduct(rows, math.Log2(cpuRows)), Memory: boundedProduct(memoryRows, width),
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
	return optimizeDistributedAccessPlan(
		ctx, snap, bound, route, profile,
		queryplanner.OpLogicalRemoteQuery, queryplanner.OpRemoteQuery, false, -1,
	)
}

func optimizeGlobalIndexPlan(
	ctx context.Context,
	snap *Snapshot,
	bound *BoundPlan,
	route distribution.Route,
	profile Profile,
	candidateRows int,
) (*queryplanner.Plan, queryplanner.OptimizerStatistics, error) {
	return optimizeDistributedAccessPlan(
		ctx, snap, bound, route, profile,
		queryplanner.OpLogicalScan, queryplanner.OpIndexScan, true, candidateRows,
	)
}

func optimizeDistributedAccessPlan(
	ctx context.Context,
	snap *Snapshot,
	bound *BoundPlan,
	route distribution.Route,
	profile Profile,
	logicalAccess queryplanner.Operator,
	physicalAccess queryplanner.Operator,
	indexLookup bool,
	candidateRows int,
) (*queryplanner.Plan, queryplanner.OptimizerStatistics, error) {
	const privateID queryplanner.PrivateID = 1
	scanRows, scanBytes, outputRows, rowBytes := distributedEstimates(snap, bound, route)
	if indexLookup && candidateRows >= 0 {
		scanRows = float64(candidateRows)
		scanBytes = boundedProduct(scanRows, max(1, rowBytes))
		outputRows = min(outputRows, scanRows)
	}
	metadata := distributedPrivate{
		targets: len(route.Targets), shards: bound.manifest.ShardCount(),
		scanRows: scanRows, scanBytes: scanBytes, outputRows: outputRows, rowBytes: rowBytes,
		order: bound.order, aggregates: bound.aggregates, groupKeys: bound.groupKeys,
		limit:       bound.limit,
		indexLookup: indexLookup,
	}
	if bound.limit > 0 && len(bound.aggregates) == 0 {
		metadata.outputRows = min(outputRows,
			float64(bound.limit)*float64(max(1, len(route.Targets))))
	}
	if len(bound.aggregates) != 0 {
		if len(bound.groupKeys) == 0 {
			metadata.outputRows = float64(len(route.Targets))
		}
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
		Op: logicalAccess, Private: privateID,
	}); err != nil {
		return nil, queryplanner.OptimizerStatistics{}, err
	}
	root := remoteGroup
	if len(bound.aggregates) != 0 && len(route.Targets) > 1 {
		finalRows := metadata.outputRows
		if len(bound.groupKeys) == 0 {
			finalRows = 1
		}
		root, err = memo.NewGroup(queryplanner.LogicalProperties{
			Rows:     queryplanner.ExactEstimate(finalRows),
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
			RuleName: "implement-distributed-access", RulePhase: queryplanner.ImplementPhase,
			MatchFunc: func(_ *queryplanner.Memo, _ queryplanner.GroupID, _ queryplanner.ExprID, expr queryplanner.Expression) bool {
				return expr.Op == logicalAccess
			},
			ApplyFunc: func(call *queryplanner.RuleCall) error {
				expr := call.Expr()
				expr.Op = physicalAccess
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
	if len(bound.aggregates) == 0 || len(bound.groupKeys) != 0 {
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
) (scanRows, scanBytes, outputRows, rowBytes float64) {
	tableCount := len(bound.tables)
	if tableCount == 0 {
		tableCount = 1
	}
	var (
		drivingRows       float64
		drivingTableRows  float64
		joinExpansion     = 1.0
		drivingStatistics TableStatistic
		hasDrivingStats   bool
	)
	for tableIndex := 0; tableIndex < tableCount; tableIndex++ {
		table := bound.table
		if len(bound.tables) != 0 {
			table = bound.tables[tableIndex]
		}
		selectedRows, tableRows, width, statistics, hasStatistics :=
			routedTableEstimates(snap, table, route)
		scanRows = boundedAdd(scanRows, selectedRows)
		scanBytes = boundedAdd(scanBytes, boundedProduct(selectedRows, width))
		rowBytes = boundedAdd(rowBytes, width)
		if tableIndex == 0 {
			drivingRows, drivingTableRows = selectedRows, tableRows
			drivingStatistics, hasDrivingStats = statistics, hasStatistics
		} else {
			// Colocation proves where a join executes, not uniqueness. Without
			// published join-key correlation, product cardinality is the safe
			// upper bound; max(1, rows) also preserves LEFT-join output.
			joinExpansion = boundedProduct(joinExpansion, max(1, selectedRows))
		}
	}
	rowBytes = max(1, rowBytes)
	if len(route.Targets) == 0 {
		return 0, 0, 0, rowBytes
	}
	outputRows = boundedProduct(drivingRows, joinExpansion)
	if !hasDrivingStats {
		return scanRows, scanBytes, outputRows, rowBytes
	}

	placement, _, _, placed := snap.plannerTableFor(bound.table)
	if !placed {
		return scanRows, scanBytes, outputRows, rowBytes
	}
	var inlineSelectivities [8]float64
	selectivities := inlineSelectivities[:0]
	if len(bound.constraints) > len(inlineSelectivities) {
		selectivities = make([]float64, 0, len(bound.constraints))
	}
	var scalarScratch [256]byte
	for ordinal, domain := range bound.constraints {
		if domain.Kind == distribution.DomainEmpty {
			return scanRows, scanBytes, 0, rowBytes
		}
		if domain.Kind != distribution.DomainFinite || ordinal >= len(placement.Columns) {
			continue
		}
		column, exists := drivingStatistics.Column(placement.Columns[ordinal])
		if !exists {
			continue
		}
		selectivity := 0.0
		complete := true
		for _, value := range domain.Values {
			canonical, valid := appendBoundStatisticScalar(scalarScratch[:0], value)
			if !valid {
				complete = false
				break
			}
			selectivity += column.EqualitySelectivityEstimateBytes(canonical).Upper
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
		drivingOutput := min(drivingRows, boundedProduct(drivingTableRows, combined))
		outputRows = boundedProduct(drivingOutput, joinExpansion)
	}
	return scanRows, scanBytes, outputRows, rowBytes
}

func routedTableEstimates(
	snap *Snapshot,
	table string,
	route distribution.Route,
) (selectedRows, tableRows, rowBytes float64, statistics TableStatistic, ok bool) {
	fallbackRows := boundedProduct(1000, float64(len(route.Targets)))
	rowBytes = 128
	statistics, ok = snap.Statistics(table)
	if !ok {
		return fallbackRows, fallbackRows, rowBytes, TableStatistic{}, false
	}
	tableRows = statistics.Rows().Normalize(fallbackRows).Upper
	rowBytes = max(1, statistics.RowBytes().Normalize(rowBytes).Upper)
	if len(route.Targets) == 0 {
		return 0, tableRows, rowBytes, statistics, true
	}
	partitionRows, completePartitions := 0.0, true
	for _, target := range route.Targets {
		estimate, exists := statistics.PartitionRows(string(target.Shard))
		if !exists {
			completePartitions = false
			break
		}
		partitionRows = boundedAdd(partitionRows, estimate.Normalize(tableRows).Upper)
	}
	if completePartitions {
		selectedRows = min(tableRows, partitionRows)
	} else {
		// A whole-table upper bound cannot be divided by shard count under skew.
		selectedRows = tableRows
	}
	return selectedRows, tableRows, rowBytes, statistics, true
}

func boundedAdd(left, right float64) float64 {
	if left >= math.MaxFloat64-right {
		return math.MaxFloat64
	}
	return left + right
}

func boundedProduct(left, right float64) float64 {
	if left == 0 || right == 0 {
		return 0
	}
	if left >= math.MaxFloat64/right {
		return math.MaxFloat64
	}
	return left * right
}

func appendBoundStatisticScalar(dst []byte, value distribution.Scalar) ([]byte, bool) {
	switch value.Kind() {
	case distribution.KindString:
		raw, _ := value.StringValue()
		canonical, err := queryplanner.AppendCanonicalStatisticStringBytes(dst, byteview.Bytes(raw))
		return canonical, err == nil
	case distribution.KindNumber:
		spelling, _ := value.NumberSpelling()
		canonical, err := queryplanner.AppendCanonicalStatisticNumber(dst, byteview.Bytes(spelling))
		return canonical, err == nil
	default:
		return dst, false
	}
}
