package gateway

import (
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

func writeAccessScopes(
	bound *BoundWritePlan,
	target distribution.Target,
) (uint8, []distributedtxn.IntentScope) {
	if bound == nil {
		return 0, nil
	}
	bits, scopes := placementAccessScopes(
		bound.spec, bound.manifest, bound.constraints, bound.rowKeys, target,
	)
	if bits == 0 || len(bound.updateDoc) == 0 {
		return bits, scopes
	}
	key, err := writeDocShardKey(bound.updateDoc, bound.keyPointers)
	if err != nil {
		return 0, nil
	}
	mapper := distribution.NewNativeMapperWithBucketBits(bound.spec.Arity, bits)
	point, err := mapper.PointFor(key)
	if err != nil {
		return 0, nil
	}
	scopes = appendOwnedPointScope(scopes, point, bound.manifest, target, bits)
	scopes = coalesceIntentScopes(scopes)
	if len(scopes) == 0 || len(scopes) > distributedtxn.MaxIntentScopes {
		return wholeShardAccessScope(bound.manifest, target, bits)
	}
	return bits, scopes
}

func readAccessScopes(
	bound *BoundPlan,
	target distribution.Target,
) (uint8, []distributedtxn.IntentScope) {
	if bound == nil {
		return 0, nil
	}
	return placementAccessScopes(bound.spec, bound.manifest, bound.constraints, nil, target)
}

func placementAccessScopes(
	spec distribution.DistributionSpec,
	manifest *distribution.Manifest,
	constraints distribution.BoundConstraints,
	rowKeys [][]distribution.Scalar,
	target distribution.Target,
) (uint8, []distributedtxn.IntentScope) {
	bits := spec.EffectiveBucketBits()
	mapper := distribution.NewNativeMapperWithBucketBits(spec.Arity, bits)
	var scopes []distributedtxn.IntentScope
	if len(rowKeys) != 0 {
		scopes = make([]distributedtxn.IntentScope, 0, min(len(rowKeys), distributedtxn.MaxIntentScopes))
		for i := range rowKeys {
			point, err := mapper.PointFor(rowKeys[i])
			if err != nil {
				return wholeShardAccessScope(manifest, target, bits)
			}
			scopes = appendOwnedPointScope(scopes, point, manifest, target, bits)
		}
	} else {
		var ok bool
		scopes, ok = finiteConstraintScopes(constraints, mapper, manifest, target, bits)
		if !ok {
			return wholeShardAccessScope(manifest, target, bits)
		}
	}
	scopes = coalesceIntentScopes(scopes)
	if len(scopes) == 0 || len(scopes) > distributedtxn.MaxIntentScopes {
		return wholeShardAccessScope(manifest, target, bits)
	}
	return bits, scopes
}

func finiteConstraintScopes(
	constraints distribution.BoundConstraints,
	mapper *distribution.NativeMapper,
	manifest *distribution.Manifest,
	target distribution.Target,
	bits uint8,
) ([]distributedtxn.IntentScope, bool) {
	if mapper == nil || len(constraints) < mapper.Arity() {
		return nil, false
	}
	count := 1
	for i := 0; i < mapper.Arity(); i++ {
		if constraints[i].Kind != distribution.DomainFinite || len(constraints[i].Values) == 0 ||
			count > distributedtxn.MaxIntentScopes/len(constraints[i].Values) {
			return nil, false
		}
		count *= len(constraints[i].Values)
	}
	scopes := make([]distributedtxn.IntentScope, 0, count)
	var values [distribution.KeyspaceWidth]distribution.Scalar
	var expand func(int) bool
	expand = func(ordinal int) bool {
		if ordinal == mapper.Arity() {
			point, err := mapper.PointFor(values[:mapper.Arity()])
			if err != nil {
				return false
			}
			scopes = appendOwnedPointScope(scopes, point, manifest, target, bits)
			return true
		}
		for i := range constraints[ordinal].Values {
			values[ordinal] = constraints[ordinal].Values[i]
			if !expand(ordinal + 1) {
				return false
			}
		}
		return true
	}
	if !expand(0) {
		return nil, false
	}
	return scopes, true
}

func appendOwnedPointScope(
	scopes []distributedtxn.IntentScope,
	point distribution.KeyspacePoint,
	manifest *distribution.Manifest,
	target distribution.Target,
	bits uint8,
) []distributedtxn.IntentScope {
	bucket, ok := distribution.VirtualBucketForPoint(point, bits)
	if !ok {
		return scopes
	}
	owner, ok := manifest.ResolveVirtualBucket(bucket, bits)
	if !ok || owner.Shard != target.Shard || owner.AllocationGeneration != target.AllocationGeneration {
		return scopes
	}
	start := uint32(bucket)
	return append(scopes, distributedtxn.IntentScope{Start: start, End: start + 1})
}

func wholeShardAccessScope(
	manifest *distribution.Manifest,
	target distribution.Target,
	bits uint8,
) (uint8, []distributedtxn.IntentScope) {
	if manifest == nil {
		return 0, nil
	}
	for i := 0; i < manifest.ShardCount(); i++ {
		shard, ok := manifest.ShardInfo(i)
		if !ok || shard.ID != target.Shard ||
			shard.AllocationGeneration != target.AllocationGeneration {
			continue
		}
		interval, ok := manifest.ShardBucketInterval(i, bits)
		if !ok {
			return 0, nil
		}
		return bits, []distributedtxn.IntentScope{{Start: interval.Start, End: interval.End}}
	}
	return 0, nil
}

func coalesceIntentScopes(scopes []distributedtxn.IntentScope) []distributedtxn.IntentScope {
	if len(scopes) < 2 {
		return scopes
	}
	slices.SortFunc(scopes, func(a, b distributedtxn.IntentScope) int {
		if a.Start < b.Start {
			return -1
		}
		if a.Start > b.Start {
			return 1
		}
		if a.End < b.End {
			return -1
		}
		if a.End > b.End {
			return 1
		}
		return 0
	})
	write := 0
	for i := 1; i < len(scopes); i++ {
		if scopes[i].Start <= scopes[write].End {
			if scopes[i].End > scopes[write].End {
				scopes[write].End = scopes[i].End
			}
			continue
		}
		write++
		scopes[write] = scopes[i]
	}
	return scopes[:write+1]
}
