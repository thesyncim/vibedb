package gateway

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibejson/x/byteview"
)

// ErrGlobalIndexResult reports a malformed locator row from an index shard.
// Serving such a result could route a base read to the wrong owner, so the
// gateway fails the complete operation rather than skipping the entry.
var ErrGlobalIndexResult = errors.New("gateway: malformed global-index lookup result")

type globalIndexExecution struct {
	result       *Result
	baseTargets  []distribution.Target
	shardsFanned int
	routeKind    distribution.RouteKind
}

func useGlobalIndexRead(bound *BoundPlan) bool {
	if bound == nil || bound.globalEmpty {
		return bound != nil && bound.globalEmpty
	}
	if bound.globalIndex == nil {
		return false
	}
	return !boundConstraintsSinglePoint(bound.constraints, bound.spec.Arity)
}

func globalIndexExplainRoute(bound *BoundPlan) (distribution.Route, int) {
	route := distribution.Route{
		Kind: distribution.RouteEmpty, Distribution: bound.distribution,
		RoutingVersion: bound.manifest.Version(),
	}
	if bound.globalEmpty || bound.globalIndex == nil {
		return route, 0
	}
	baseShards := bound.manifest.ShardCount()
	if bound.globalIndex.program.metadata.Flags&IndexUnique != 0 {
		baseShards = min(baseShards, 1)
	}
	for i := 0; i < baseShards; i++ {
		shard, ok := bound.manifest.ShardInfo(i)
		if !ok || len(shard.Leaders) == 0 {
			continue
		}
		route.Targets = append(route.Targets, distribution.Target{
			Shard: shard.ID, AllocationGeneration: shard.AllocationGeneration,
			Endpoint: shard.Leaders[0], OwnershipEpoch: shard.Epoch,
			Role: distribution.RoleLeader,
		})
	}
	if len(route.Targets) == 1 {
		route.Kind = distribution.RouteSingle
	} else if len(route.Targets) > 1 {
		route.Kind = distribution.RouteTargeted
	}
	return route, len(route.Targets) + 1
}

func admitGlobalIndexTargets(
	count, total int,
	profile Profile,
) (distribution.RouteKind, error) {
	if count == 0 {
		return distribution.RouteEmpty, nil
	}
	if total > 1 && count == total {
		if profile.Policy.Admission == distribution.AdmissionTargetedOnly {
			return 0, distribution.ErrScatterRejected
		}
		return distribution.RouteScatter, nil
	}
	limit := profile.Policy.Limits.MaxTargetShards
	if limit <= 0 {
		limit = distribution.DefaultMaxTargetShards
	}
	if count > limit {
		return 0, &distribution.TargetLimitError{Limit: limit, Count: count}
	}
	if count == 1 {
		return distribution.RouteSingle, nil
	}
	return distribution.RouteTargeted, nil
}

// queryGlobalIndex executes a dynamic two-level read cut. It fences and probes
// the independently sharded index first, groups decoded locators by base owner,
// then acquires only those base scopes under the same identity. A conflict
// releases the partial cut and retries from a fresh index snapshot, preventing
// both distributed lock cycles and mixed locator/base generations.
func (e *Executor) queryGlobalIndex(
	ctx context.Context,
	q *Query,
	bound *BoundPlan,
	profile Profile,
) (globalIndexExecution, error) {
	if bound == nil || q == nil {
		return globalIndexExecution{}, &PlanError{
			Reason: "nil global-index read plan", cause: ErrDistributedPlanUnsupported,
		}
	}
	if bound.alwaysReason != "" {
		return globalIndexExecution{}, &PlanError{
			Table: bound.table, Reason: bound.alwaysReason,
			cause: ErrDistributedPlanUnsupported,
		}
	}
	if bound.globalEmpty {
		if bound.emptyReason != "" {
			return globalIndexExecution{}, &PlanError{
				Table: bound.table, Reason: bound.emptyReason,
				cause: ErrDistributedPlanUnsupported,
			}
		}
		plan := &plan{aggregates: bound.aggregates, aggHeaders: bound.aggHeaders}
		if len(bound.aggregates) != 0 {
			return globalIndexExecution{result: emptyAggregateResult(plan)}, nil
		}
		return globalIndexExecution{result: &Result{Kind: shardservice.ResponseRows}}, nil
	}
	if bound.globalIndex == nil {
		return globalIndexExecution{}, &PlanError{
			Table: bound.table, Reason: "global index key is not fully bound",
			cause: ErrDistributedPlanUnsupported,
		}
	}

	indexCall := globalIndexShardCall(bound.globalIndex, profile)
	for attempt := 0; ; attempt++ {
		id, err := newTransactionID(cryptorand.Reader)
		if err != nil {
			return globalIndexExecution{}, err
		}
		if err := e.acquireReadFencesOnce(ctx, []shardCall{indexCall}, profile, id); err != nil {
			if errors.Is(err, ErrReadFenceBusy) {
				if waitErr := waitReadFenceRetry(ctx, id, attempt); waitErr != nil {
					return globalIndexExecution{}, waitErr
				}
				continue
			}
			return globalIndexExecution{}, err
		}

		indexCall.req.ReadFenceID = id
		response, lookupErr := e.globalIndexRoundTrip(ctx, indexCall, profile)
		indexCall.req.ReadFenceID = distributedtxn.ID{}
		if lookupErr != nil {
			releaseErr := e.releaseReadFences(ctx, []shardCall{indexCall}, profile, id)
			return globalIndexExecution{}, globalIndexReadError(lookupErr, releaseErr)
		}
		baseCalls, targets, locatorErr := globalIndexBaseCalls(
			bound.globalIndex.program, response, q, profile,
		)
		if locatorErr != nil {
			releaseErr := e.releaseReadFences(ctx, []shardCall{indexCall}, profile, id)
			return globalIndexExecution{}, globalIndexReadError(locatorErr, releaseErr)
		}
		baseKind, admissionErr := admitGlobalIndexTargets(
			len(baseCalls), bound.manifest.ShardCount(), profile,
		)
		if admissionErr != nil {
			releaseErr := e.releaseReadFences(ctx, []shardCall{indexCall}, profile, id)
			return globalIndexExecution{}, globalIndexReadError(admissionErr, releaseErr)
		}
		if len(baseCalls) > 1 && bound.multiReason != "" {
			releaseErr := e.releaseReadFences(ctx, []shardCall{indexCall}, profile, id)
			return globalIndexExecution{}, globalIndexReadError(&PlanError{
				Table: bound.table, Reason: bound.multiReason,
				cause: ErrDistributedPlanUnsupported,
			}, releaseErr)
		}
		if len(baseCalls) == 0 {
			releaseErr := e.releaseReadFences(ctx, []shardCall{indexCall}, profile, id)
			if releaseErr != nil {
				return globalIndexExecution{}, releaseErr
			}
			if bound.emptyReason != "" {
				return globalIndexExecution{}, &PlanError{
					Table: bound.table, Reason: bound.emptyReason,
					cause: ErrDistributedPlanUnsupported,
				}
			}
			plan := &plan{aggregates: bound.aggregates, aggHeaders: bound.aggHeaders}
			result := &Result{Kind: shardservice.ResponseRows}
			if len(bound.aggregates) != 0 {
				result = emptyAggregateResult(plan)
			}
			return globalIndexExecution{
				result: result, shardsFanned: 1, routeKind: distribution.RouteSingle,
			}, nil
		}

		if err := e.acquireReadFencesOnce(ctx, baseCalls, profile, id); err != nil {
			releaseErr := e.releaseReadFences(ctx, []shardCall{indexCall}, profile, id)
			if errors.Is(err, ErrReadFenceBusy) && releaseErr == nil {
				if waitErr := waitReadFenceRetry(ctx, id, attempt); waitErr != nil {
					return globalIndexExecution{}, waitErr
				}
				continue
			}
			return globalIndexExecution{}, globalIndexReadError(err, releaseErr)
		}
		for i := range baseCalls {
			baseCalls[i].req.ReadFenceID = id
		}
		basePlan := &plan{
			calls: baseCalls, order: bound.order, limit: bound.limit,
			aggregates: bound.aggregates, aggHeaders: bound.aggHeaders,
		}
		var result *Result
		var queryErr error
		if len(baseCalls) == 1 {
			result, queryErr = e.single(ctx, baseCalls[0], profile)
		} else {
			result, queryErr = e.fanout(ctx, basePlan, profile)
		}
		allCalls := make([]shardCall, 0, len(baseCalls)+1)
		allCalls = append(allCalls, baseCalls...)
		allCalls = append(allCalls, indexCall)
		releaseErr := e.releaseReadFences(ctx, allCalls, profile, id)
		if queryErr != nil || releaseErr != nil {
			return globalIndexExecution{}, globalIndexReadError(queryErr, releaseErr)
		}
		return globalIndexExecution{
			result: result, baseTargets: targets, shardsFanned: len(baseCalls) + 1,
			routeKind: baseKind,
		}, nil
	}
}

// globalIndexReadError preserves retry classification only when every held
// fence was released successfully. A release failure takes precedence so a
// stale routing refusal cannot trigger another attempt while an uncertain old
// cut remains leased.
func globalIndexReadError(primary, release error) error {
	if release == nil {
		return primary
	}
	if primary == nil {
		return release
	}
	return fmt.Errorf(
		"gateway: global-index read failed (%v) and fence release failed: %w",
		primary, release,
	)
}

func globalIndexShardCall(
	bound *boundGlobalIndexRead,
	profile Profile,
) shardCall {
	program := bound.program
	route := bound.route
	return shardCall{
		target: route.IndexTarget, address: route.IndexAddress,
		req: &shardservice.ShardRequest{
			Distribution:         program.indexManifest.Distribution(),
			Shard:                route.IndexTarget.Shard,
			AllocationGeneration: route.IndexTarget.AllocationGeneration,
			RoutingVersion:       program.indexManifest.Version(),
			OwnershipEpoch:       route.IndexTarget.OwnershipEpoch,
			ReadPolicy:           profile.ReadPolicy, ExecutionMode: shardservice.ExecutionReadOnly,
			Deadline: profile.PerShardDeadline,
			MaxRows:  profile.PerShardRows, MaxResultBytes: profile.PerShardBytes,
			BucketBits:   route.IndexBucketBits,
			AccessScopes: []distributedtxn.IntentScope{route.IndexScope},
			GlobalIndexLookup: shardservice.GlobalIndexLookupRequest{
				Relation:     byteview.Bytes(program.metadata.Relation),
				IndexID:      program.metadata.IndexID,
				Incarnation:  program.metadata.Incarnation,
				KeyTuple:     route.KeyTuple,
				LocatorCount: program.metadata.LocatorCount,
				Unique:       program.metadata.Flags&IndexUnique != 0,
			},
		},
	}
}

func (e *Executor) globalIndexRoundTrip(
	ctx context.Context,
	call shardCall,
	profile Profile,
) (*shardservice.ShardResponse, error) {
	requestContext, cancel := context.WithTimeout(ctx, profile.PerShardDeadline)
	defer cancel()
	response, err := e.client.Do(requestContext, call.address, call.req)
	if err != nil {
		return nil, err
	}
	if err := checkAggregate(profile, uint64(len(response.Rows)), responseBytes(response)); err != nil {
		return nil, err
	}
	return response, nil
}

func globalIndexBaseCalls(
	program GlobalIndexProgram,
	response *shardservice.ShardResponse,
	query *Query,
	profile Profile,
) ([]shardCall, []distribution.Target, error) {
	if response == nil || response.Kind != shardservice.ResponseRows ||
		len(response.Columns) != 1 || response.Columns[0].Name != "locator" {
		return nil, nil, ErrGlobalIndexResult
	}
	calls := make([]shardCall, 0, min(len(response.Rows), program.baseManifest.ShardCount()))
	positions := make(map[distribution.ShardID]int, cap(calls))
	var workspace GlobalIndexWorkspace
	for rowOrdinal := range response.Rows {
		row := response.Rows[rowOrdinal]
		if len(row) != 1 || row[0].Null || len(row[0].Bytes) == 0 {
			return nil, nil, fmt.Errorf("%w: row %d", ErrGlobalIndexResult, rowOrdinal)
		}
		route, err := program.RouteLocatorValue(row[0].Bytes, &workspace)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: row %d: %v", ErrGlobalIndexResult, rowOrdinal, err)
		}
		position, found := positions[route.BaseTarget.Shard]
		if !found {
			position = len(calls)
			positions[route.BaseTarget.Shard] = position
			calls = append(calls, shardCall{
				target: route.BaseTarget, address: route.BaseAddress,
				req: &shardservice.ShardRequest{
					SQL: query.SQL, Params: query.Params,
					Distribution:         program.baseManifest.Distribution(),
					Shard:                route.BaseTarget.Shard,
					AllocationGeneration: route.BaseTarget.AllocationGeneration,
					RoutingVersion:       program.baseManifest.Version(),
					OwnershipEpoch:       route.BaseTarget.OwnershipEpoch,
					ReadPolicy:           profile.ReadPolicy,
					ExecutionMode:        shardservice.ExecutionReadOnly,
					Deadline:             profile.PerShardDeadline,
					MaxRows:              profile.PerShardRows, MaxResultBytes: profile.PerShardBytes,
					BucketBits: route.BaseBucketBits,
				},
			})
		}
		calls[position].req.AccessScopes = append(
			calls[position].req.AccessScopes, route.BaseScope,
		)
	}
	for i := range calls {
		call := &calls[i]
		call.req.AccessScopes = coalesceIntentScopes(call.req.AccessScopes)
		if len(call.req.AccessScopes) > distributedtxn.MaxIntentScopes {
			call.req.BucketBits, call.req.AccessScopes = wholeShardAccessScope(
				program.baseManifest, call.target, call.req.BucketBits,
			)
		}
	}
	slices.SortFunc(calls, func(a, b shardCall) int {
		if a.target.Shard < b.target.Shard {
			return -1
		}
		if a.target.Shard > b.target.Shard {
			return 1
		}
		return 0
	})
	targets := make([]distribution.Target, len(calls))
	for i := range calls {
		targets[i] = calls[i].target
	}
	return calls, targets, nil
}
