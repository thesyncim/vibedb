package gateway

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type coordinatorSourcePlan struct {
	tables      []string
	dispatch    *plan
	explanation Explanation
}

// planCoordinator is shared by execution and EXPLAIN. Every physical source is
// routed from its complete consumer domain before any shard is contacted.
func (e *Executor) planCoordinator(ctx context.Context, snap *Snapshot, q Query, prepared *PreparedPlan, profile Profile, args []any) (*coordinatorSourcePlan, error) {
	tables := coordinatorPhysicalTables(prepared.statement.Select)
	groups := make(map[distribution.DistributionName][]int)
	var distributions []distribution.DistributionName
	for i, table := range tables {
		placement, _, _, ok := snap.plannerTableFor(table)
		if !ok {
			return nil, &PlanError{Table: table, Reason: "no placement in pinned catalog generation", cause: ErrTableNotPlaced}
		}
		if _, ok := groups[placement.Distribution]; !ok {
			distributions = append(distributions, placement.Distribution)
		}
		groups[placement.Distribution] = append(groups[placement.Distribution], i)
	}
	constraints, err := coordinatorConstraints(snap, prepared.statement.Select, args)
	if err != nil {
		return nil, err
	}
	all := &plan{generation: snap.Generation()}
	var description strings.Builder
	description.WriteString("CoordinatorQuery (shared SQL engine; bounded source materialization)")
	var explanation Explanation
	available := 0
	for _, name := range distributions {
		byShard := make(map[distribution.ShardID]int)
		countedManifest := false
		for _, index := range groups[name] {
			scan := Query{SQL: "SELECT * FROM " + quoteSQLIdentifier(tables[index]), Class: q.Class}
			scanPrepared, prepareErr := snap.Prepare(ctx, scan.SQL)
			if prepareErr != nil {
				return nil, prepareErr
			}
			bound, bindErr := scanPrepared.Bind(nil)
			if bindErr != nil {
				return nil, bindErr
			}
			bound.constraints = constraints[tables[index]]
			routed, routeErr := e.routeContext(ctx, snap, &scan, bound, profile)
			if routeErr != nil {
				return nil, routeErr
			}
			if !countedManifest {
				available += bound.manifest.ShardCount()
				countedManifest = true
			}
			if routed.scatter > all.scatter {
				all.scatter = routed.scatter
			}
			fmt.Fprintf(&description, "\n  Source %q distribution=%q shards=%d/%d route=%s reason=%s", tables[index], name, len(routed.calls), bound.manifest.ShardCount(), routed.kind, routed.scatter)
			explanation.Cost = explanation.Cost.Plus(routed.physical.Cost)
			explanation.Planning.PlanNodes += routed.planning.PlanNodes
			fragment := fmt.Sprintf("SELECT *, %d AS __source FROM %s", index, quoteSQLIdentifier(tables[index]))
			placement, _, _, _ := snap.plannerTableFor(tables[index])
			filter, params := coordinatorSourceFilter(placement.Columns, bound.constraints)
			fragment += filter
			for _, call := range routed.calls {
				fmt.Fprintf(&description, "\n    Shard %q allocation=%d epoch=%d", call.target.Shard, call.target.AllocationGeneration, call.target.OwnershipEpoch)
				if prior, exists := byShard[call.target.Shard]; exists {
					all.calls[prior].req.SQL += " UNION ALL " + fragment
					all.calls[prior].req.Params = append(all.calls[prior].req.Params, params...)
					// Each relation can route via a different subset of keys.
					// The common snapshot must fence their complete union.
					// The existing call already owns the full shard access scope.
					continue
				}
				call.req.SQL = fragment
				call.req.Params = append([]shardservice.Param(nil), params...)
				call.req.PrimaryKeyRead = shardservice.PrimaryKeyReadRequest{}
				// Full shard scopes remain a conservative fence for the combined
				// relation scan, including unbound placement ordinals.
				call.req.BucketBits, call.req.AccessScopes = wholeShardAccessScope(bound.manifest, call.target, bound.spec.EffectiveBucketBits())
				byShard[call.target.Shard] = len(all.calls)
				all.calls = append(all.calls, call)
			}
		}
	}

	switch {
	case len(all.calls) == 0:
		all.kind, all.scatter = distribution.RouteEmpty, ScatterNone
	case len(all.calls) == 1:
		all.kind, all.scatter = distribution.RouteSingle, ScatterNone
	case len(all.calls) == available || all.scatter != ScatterNone:
		all.kind = distribution.RouteScatter
		if all.scatter == ScatterNone {
			all.scatter = ScatterAllShards
		}
	default:
		all.kind = distribution.RouteTargeted
	}
	if all.kind == distribution.RouteScatter && profile.Policy.Admission == distribution.AdmissionTargetedOnly {
		return nil, distribution.ErrScatterRejected
	}
	limits := distribution.NewRoutePolicy(profile.Policy.Admission, profile.Policy.Limits).Limits
	if len(all.calls) > limits.MaxTargetShards {
		return nil, &distribution.TargetLimitError{Limit: limits.MaxTargetShards, Count: len(all.calls)}
	}
	explanation.PhysicalPlan = description.String()
	explanation.PlanFingerprint = fmt.Sprintf("%x", sha256.Sum256([]byte(q.SQL+"\x00"+explanation.PhysicalPlan)))
	explanation.RouteKind, explanation.ScatterReason = all.kind, all.scatter
	explanation.Shards, explanation.Generation = len(all.calls), snap.Generation()
	explanation.Planning.PlanNodes++
	return &coordinatorSourcePlan{tables: tables, dispatch: all, explanation: explanation}, nil
}

func (e *Executor) explainCoordinator(ctx context.Context, snap *Snapshot, q Query, prepared *PreparedPlan, profile Profile, args []any) (*Explanation, error) {
	types, err := postgresQueryParameterTypes(q.ParamTypes, prepared.params)
	if err != nil {
		return nil, err
	}
	compiled, err := query.PrepareParsedStatementWithParameterTypes(q.SQL, prepared.statement.Select, types)
	if err != nil {
		return nil, err
	}
	compiled.Release()
	if err := sqldriver.ValidateSQLPathComparisonDomains(q.SQL, prepared.statement.Select, snap.coordinatorPathDomain); err != nil {
		return nil, err
	}
	sourcePlan, err := e.planCoordinator(ctx, snap, q, prepared, profile, args)
	if err != nil {
		return nil, err
	}
	return &sourcePlan.explanation, nil
}

// coordinatorSourceFilter emits necessary key predicates for source rows. Only
// top-level pointer fields are translated: nested array/object pointer spelling
// needs a separate semantic proof. Shard routing still uses every ordinal.
func coordinatorSourceFilter(columns []string, domains distribution.BoundConstraints) (string, []shardservice.Param) {
	var b strings.Builder
	var params []shardservice.Param
	for i, pointer := range columns {
		if i >= len(domains) || domains[i].Kind != distribution.DomainFinite || len(domains[i].Values) == 0 || !strings.HasPrefix(pointer, "/") || strings.Contains(pointer[1:], "/") {
			continue
		}
		field := strings.ReplaceAll(strings.ReplaceAll(pointer[1:], "~1", "/"), "~0", "~")
		if b.Len() == 0 {
			b.WriteString(" WHERE ")
		} else {
			b.WriteString(" AND ")
		}
		b.WriteString(quoteSQLIdentifier(field))
		b.WriteString(" IN (")
		for j, value := range domains[i].Values {
			if j != 0 {
				b.WriteByte(',')
			}
			b.WriteByte('?')
			if text, ok := value.StringValue(); ok {
				params = append(params, shardservice.StringParam(text))
			} else if number, ok := value.NumberSpelling(); ok {
				params = append(params, shardservice.NumberParam(number))
			}
		}
		b.WriteByte(')')
	}
	return b.String(), params
}
