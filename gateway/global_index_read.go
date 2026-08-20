package gateway

import (
	"bytes"
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
	result        *Result
	baseTargets   []distribution.Target
	shardsFanned  int
	routeKind     distribution.RouteKind
	candidateRows int
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

func globalIndexExplainRoute(
	bound *BoundPlan,
	profile Profile,
) (distribution.Route, int, int, error) {
	route := distribution.Route{
		Kind: distribution.RouteEmpty, Distribution: bound.distribution,
		RoutingVersion: bound.manifest.Version(),
	}
	if bound.globalEmpty || bound.globalIndex == nil {
		return route, 0, 0, nil
	}
	indexCalls, keyCount, err := planGlobalIndexShardCalls(bound.globalIndex, profile)
	if err != nil {
		return distribution.Route{}, 0, 0, err
	}
	baseShards := bound.manifest.ShardCount()
	if bound.globalIndex.program.metadata.Flags&IndexUnique != 0 {
		baseShards = min(baseShards, keyCount)
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
	return route, len(route.Targets) + len(indexCalls), keyCount, nil
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

	indexCalls, _, err := planGlobalIndexShardCalls(bound.globalIndex, profile)
	if err != nil {
		return globalIndexExecution{}, err
	}
	indexKind, err := admitGlobalIndexTargets(
		len(indexCalls), bound.globalIndex.program.indexManifest.ShardCount(), profile,
	)
	if err != nil {
		return globalIndexExecution{}, err
	}
	for attempt := 0; ; attempt++ {
		id, err := newTransactionID(cryptorand.Reader)
		if err != nil {
			return globalIndexExecution{}, err
		}
		if err := e.acquireReadFencesOnce(ctx, indexCalls, profile, id); err != nil {
			if errors.Is(err, ErrReadFenceBusy) {
				if waitErr := waitReadFenceRetry(ctx, id, attempt); waitErr != nil {
					return globalIndexExecution{}, waitErr
				}
				continue
			}
			return globalIndexExecution{}, err
		}

		for i := range indexCalls {
			indexCalls[i].req.ReadFenceID = id
		}
		response, lookupErr := e.globalIndexRoundTrip(ctx, indexCalls, profile)
		for i := range indexCalls {
			indexCalls[i].req.ReadFenceID = distributedtxn.ID{}
		}
		if lookupErr != nil {
			releaseErr := e.releaseReadFences(ctx, indexCalls, profile, id)
			return globalIndexExecution{}, globalIndexReadError(lookupErr, releaseErr)
		}
		baseCalls, targets, locatorErr := globalIndexBaseCalls(
			bound.globalIndex.program, response, q, profile,
		)
		if locatorErr != nil {
			releaseErr := e.releaseReadFences(ctx, indexCalls, profile, id)
			return globalIndexExecution{}, globalIndexReadError(locatorErr, releaseErr)
		}
		baseKind, admissionErr := admitGlobalIndexTargets(
			len(baseCalls), bound.manifest.ShardCount(), profile,
		)
		if admissionErr != nil {
			releaseErr := e.releaseReadFences(ctx, indexCalls, profile, id)
			return globalIndexExecution{}, globalIndexReadError(admissionErr, releaseErr)
		}
		if len(baseCalls) > 1 && bound.multiReason != "" {
			releaseErr := e.releaseReadFences(ctx, indexCalls, profile, id)
			return globalIndexExecution{}, globalIndexReadError(&PlanError{
				Table: bound.table, Reason: bound.multiReason,
				cause: ErrDistributedPlanUnsupported,
			}, releaseErr)
		}
		if len(baseCalls) == 0 {
			releaseErr := e.releaseReadFences(ctx, indexCalls, profile, id)
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
				result: result, shardsFanned: len(indexCalls), routeKind: indexKind,
				candidateRows: 0,
			}, nil
		}

		if err := e.acquireReadFencesOnce(ctx, baseCalls, profile, id); err != nil {
			releaseErr := e.releaseReadFences(ctx, indexCalls, profile, id)
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
		allCalls := make([]shardCall, 0, len(baseCalls)+len(indexCalls))
		allCalls = append(allCalls, baseCalls...)
		allCalls = append(allCalls, indexCalls...)
		releaseErr := e.releaseReadFences(ctx, allCalls, profile, id)
		if queryErr != nil || releaseErr != nil {
			return globalIndexExecution{}, globalIndexReadError(queryErr, releaseErr)
		}
		return globalIndexExecution{
			result: result, baseTargets: targets, shardsFanned: len(baseCalls) + len(indexCalls),
			routeKind: baseKind, candidateRows: len(response.Rows),
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

type globalIndexLookupGroup struct {
	target  distribution.Target
	address string
	bits    uint8
	scopes  []distributedtxn.IntentScope
	arena   []byte
	ends    []int
}

// planGlobalIndexShardCalls expands one fully finite index-key domain under
// the route candidate bound, then groups its locator keys by independently
// sharded index owner. Each owner receives one sorted batch and one shared
// relation snapshot instead of one request per key.
func planGlobalIndexShardCalls(
	bound *boundGlobalIndexRead,
	profile Profile,
) ([]shardCall, int, error) {
	if bound == nil || bound.program.snapshot == nil || len(bound.constraints) == 0 ||
		len(bound.constraints) != len(bound.program.keyPointers) {
		return nil, 0, ErrGlobalIndexResult
	}
	limit := profile.Policy.Limits.MaxCandidateMappings
	if limit <= 0 {
		limit = distribution.DefaultMaxCandidateMappings
	}
	keyCount := 1
	for i := range bound.constraints {
		domain := bound.constraints[i]
		if domain.Kind != distribution.DomainFinite || len(domain.Values) == 0 {
			return nil, 0, ErrGlobalIndexResult
		}
		if keyCount > limit/len(domain.Values) {
			return nil, 0, &distribution.ExpansionLimitError{Limit: limit}
		}
		keyCount *= len(domain.Values)
	}

	groups := make([]globalIndexLookupGroup, 0,
		min(keyCount, bound.program.indexManifest.ShardCount()))
	positions := make(map[distribution.ShardID]int, cap(groups))
	var (
		workspace GlobalIndexWorkspace
		key       [4]distribution.Scalar
		indices   [4]int
	)
	for produced := 0; produced < keyCount; produced++ {
		for ordinal := range bound.constraints {
			key[ordinal] = bound.constraints[ordinal].Values[indices[ordinal]]
		}
		route, err := bound.program.RouteKey(key[:len(bound.constraints)], &workspace)
		if err != nil {
			return nil, 0, err
		}
		position, found := positions[route.IndexTarget.Shard]
		if !found {
			position = len(groups)
			positions[route.IndexTarget.Shard] = position
			groups = append(groups, globalIndexLookupGroup{
				target: route.IndexTarget, address: route.IndexAddress,
				bits: route.IndexBucketBits,
			})
		}
		group := &groups[position]
		group.scopes = append(group.scopes, route.IndexScope)
		group.arena = append(group.arena, route.KeyTuple...)
		group.ends = append(group.ends, len(group.arena))

		for ordinal := len(bound.constraints) - 1; ordinal >= 0; ordinal-- {
			indices[ordinal]++
			if indices[ordinal] < len(bound.constraints[ordinal].Values) {
				break
			}
			indices[ordinal] = 0
		}
	}
	if _, err := admitGlobalIndexTargets(
		len(groups), bound.program.indexManifest.ShardCount(), profile,
	); err != nil {
		return nil, 0, err
	}

	calls := make([]shardCall, len(groups))
	for i := range groups {
		group := &groups[i]
		keys := make([][]byte, len(group.ends))
		start := 0
		for keyOrdinal, end := range group.ends {
			keys[keyOrdinal] = group.arena[start:end:end]
			start = end
		}
		slices.SortFunc(keys, func(a, b []byte) int { return bytes.Compare(a, b) })
		scopes := coalesceIntentScopes(group.scopes)
		if len(scopes) > distributedtxn.MaxIntentScopes {
			group.bits, scopes = wholeShardAccessScope(
				bound.program.indexManifest, group.target, group.bits,
			)
		}
		calls[i] = shardCall{
			target: group.target, address: group.address,
			req: &shardservice.ShardRequest{
				Distribution:         bound.program.indexManifest.Distribution(),
				Shard:                group.target.Shard,
				AllocationGeneration: group.target.AllocationGeneration,
				RoutingVersion:       bound.program.indexManifest.Version(),
				OwnershipEpoch:       group.target.OwnershipEpoch,
				ReadPolicy:           profile.ReadPolicy,
				ExecutionMode:        shardservice.ExecutionReadOnly,
				Deadline:             profile.PerShardDeadline,
				MaxRows:              profile.PerShardRows,
				MaxResultBytes:       profile.PerShardBytes,
				BucketBits:           group.bits,
				AccessScopes:         scopes,
				GlobalIndexLookup: shardservice.GlobalIndexLookupRequest{
					Relation: byteview.Bytes(bound.program.metadata.Relation),
					IndexID:  bound.program.metadata.IndexID, Incarnation: bound.program.metadata.Incarnation,
					KeyTuples: keys, LocatorCount: bound.program.metadata.LocatorCount,
					Unique: bound.program.metadata.Flags&IndexUnique != 0,
				},
			},
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
	return calls, keyCount, nil
}

func (e *Executor) globalIndexRoundTrip(
	ctx context.Context,
	calls []shardCall,
	profile Profile,
) (*shardservice.ShardResponse, error) {
	if len(calls) == 0 {
		return nil, ErrGlobalIndexResult
	}
	if len(calls) == 1 {
		requestContext, cancel := context.WithTimeout(ctx, profile.PerShardDeadline)
		defer cancel()
		response, err := e.client.Do(requestContext, calls[0].address, calls[0].req)
		if err != nil {
			return nil, err
		}
		if err := checkAggregate(profile, uint64(len(response.Rows)), responseBytes(response)); err != nil {
			return nil, err
		}
		return response, nil
	}
	result, err := e.fanout(ctx, &plan{calls: calls}, profile)
	if err != nil {
		return nil, err
	}
	return &shardservice.ShardResponse{
		Kind: result.Kind, Columns: result.Columns, Rows: result.Rows,
	}, nil
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
	type baseGroup struct {
		call  shardCall
		arena []byte
		ends  []int
	}
	groups := make([]baseGroup, 0, min(len(response.Rows), program.baseManifest.ShardCount()))
	positions := make(map[distribution.ShardID]int, cap(groups))
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
			position = len(groups)
			positions[route.BaseTarget.Shard] = position
			groups = append(groups, baseGroup{call: shardCall{
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
			}})
		}
		group := &groups[position]
		group.call.req.AccessScopes = append(
			group.call.req.AccessScopes, route.BaseScope,
		)
		group.arena = append(group.arena, route.BasePrimaryKey...)
		group.ends = append(group.ends, len(group.arena))
	}
	calls := make([]shardCall, len(groups))
	for i := range groups {
		group := &groups[i]
		call := &group.call
		call.req.AccessScopes = coalesceIntentScopes(call.req.AccessScopes)
		if len(call.req.AccessScopes) > distributedtxn.MaxIntentScopes {
			call.req.BucketBits, call.req.AccessScopes = wholeShardAccessScope(
				program.baseManifest, call.target, call.req.BucketBits,
			)
		}
		call.req.PrimaryKeyRead.PrimaryPath = byteview.Bytes(
			program.metadata.LocatorPaths[program.primary],
		)
		call.req.PrimaryKeyRead.Keys = make([][]byte, len(group.ends))
		start := 0
		for key := range group.ends {
			end := group.ends[key]
			call.req.PrimaryKeyRead.Keys[key] = group.arena[start:end:end]
			start = end
		}
		slices.SortFunc(call.req.PrimaryKeyRead.Keys, func(a, b []byte) int {
			return bytes.Compare(a, b)
		})
		calls[i] = *call
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
