package distribution

import "slices"

// Router turns bound constraints into a Route against one immutable manifest
// under a RoutePolicy. Route returns freshly owned targets; RouteInto may reuse
// caller-owned target storage. The router reuses internal scratch buffers across
// calls, so it is not safe for concurrent use: use one Router per goroutine. The
// manifest it reads remains safe to share.
type Router struct {
	codec TupleCodec
	dom   []canonSet // per-ordinal deduplicated finite domains for the chosen prefix
	combo []Scalar   // current Cartesian combination
	idx   []int      // Cartesian odometer
	dest  DestinationSet
	hits  []int // selected shard indices
	// mapPoints/mapRanges are the one-destination scratch guaranteed sufficient
	// for NativeMapper. Keeping them on Router prevents slices passed through the
	// optional MapperInto interface from forcing per-call stack arrays to escape.
	mapPoints [1]KeyspacePoint
	mapRanges [1]KeyRange
}

// NewRouter returns a Router that encodes canonical bytes with the frozen
// version 1 tuple codec.
func NewRouter() *Router {
	return &Router{codec: V1}
}

// Route resolves cons through mapper against man under policy and returns an
// immutable Route.
//
// It yields RouteEmpty without mapper or network work when any ordinal domain
// is empty. Otherwise it maps the longest mapper-supported bound leading
// prefix, deduplicating each finite domain by canonical bytes before an
// overflow-safe Cartesian estimate, enforces the candidate and target limits,
// resolves the destinations against the manifest, and classifies the physical
// route. A route covering every active shard (of more than one) is RouteScatter
// even when finite exact values produced it. Admission is applied per the
// documented truth table, and leader-only targets carry each shard's ownership
// epoch from the manifest.
func (r *Router) Route(cons BoundConstraints, mapper Mapper, man *Manifest, policy RoutePolicy) (Route, error) {
	return r.route(cons, mapper, man, policy, nil)
}

// RouteInto resolves cons like [Router.Route], but materializes the returned
// targets into targetScratch when its capacity is sufficient. The returned
// Route owns that caller-provided slice until the caller reuses it. This removes
// target allocation for short-lived routing decisions. A warmed single-
// destination route is allocation-free when mapper implements [MapperInto];
// routes that require destination normalization may still allocate. Route keeps
// its immutable freshly-owned result contract for plans that retain a route.
func (r *Router) RouteInto(
	cons BoundConstraints,
	mapper Mapper,
	man *Manifest,
	policy RoutePolicy,
	targetScratch []Target,
) (Route, error) {
	return r.route(cons, mapper, man, policy, targetScratch)
}

func (r *Router) route(
	cons BoundConstraints,
	mapper Mapper,
	man *Manifest,
	policy RoutePolicy,
	targetScratch []Target,
) (Route, error) {
	policy = policy.normalized()

	var dist DistributionName
	var ver RoutingVersion
	if man != nil {
		dist = man.Distribution()
		ver = man.Version()
	}

	// 1. Any empty ordinal makes the whole key unsatisfiable: no mapper or
	//    network work, regardless of the mapper or manifest.
	for i := range cons {
		if cons[i].Kind == DomainEmpty {
			return Route{Kind: RouteEmpty, Distribution: dist, RoutingVersion: ver}, nil
		}
	}

	if mapper == nil {
		return Route{}, &MapperError{Reason: "nil mapper"}
	}
	if man == nil {
		return Route{}, &MapperError{Reason: "nil manifest"}
	}

	// 2. Choose the longest supported leading prefix among the leading
	//    consecutive finite ordinals: a missing non-leading component never
	//    permits skipping ahead.
	avail := leadingFiniteLen(cons, mapper.Arity())
	chosen := mapper.SupportedPrefixes().LongestAtMost(avail)
	if chosen == 0 {
		// No usable prefix: the route is unknown and therefore scatter.
		return r.scatterOrReject(policy, man, dist, ver, targetScratch)
	}

	// 3. Deduplicate each finite domain by canonical bytes before expansion,
	//    then bound the Cartesian product with overflow-safe arithmetic.
	r.growDom(chosen)
	for i := 0; i < chosen; i++ {
		cs := &r.dom[i]
		cs.reset()
		for _, v := range cons[i].Values {
			if err := cs.add(r.codec, v); err != nil {
				return Route{}, err
			}
		}
		cs.sortDedup()
		if len(cs.items) == 0 {
			// A finite domain with no values is empty in effect.
			return Route{Kind: RouteEmpty, Distribution: dist, RoutingVersion: ver}, nil
		}
	}
	if productExceeds(r.dom[:chosen], policy.Limits.MaxCandidateMappings) {
		if policy.Admission == AdmissionAllowScatterOnOverflow {
			return r.scatterAll(man, dist, ver, targetScratch), nil
		}
		return Route{}, &ExpansionLimitError{Limit: policy.Limits.MaxCandidateMappings}
	}

	// 4. Expand every candidate, map it, and accumulate the destinations.
	r.dest.Points = r.dest.Points[:0]
	r.dest.Ranges = r.dest.Ranges[:0]
	if err := r.expand(chosen, mapper); err != nil {
		return Route{}, err
	}
	norm := r.dest
	if len(norm.Points) > 1 || len(norm.Ranges) > 0 {
		var err error
		norm, err = r.dest.Normalize()
		if err != nil {
			return Route{}, err
		}
	}

	// 5. Resolve the destinations to selected shard indices.
	var err error
	r.hits = r.hits[:0]
	r.hits, err = resolveShardIndices(man, norm, r.hits)
	if err != nil {
		return Route{}, err
	}
	sel := len(r.hits)
	total := man.ShardCount()
	if sel == 0 {
		return Route{Kind: RouteEmpty, Distribution: dist, RoutingVersion: ver}, nil
	}

	// 6. A route physically covering every active shard is scatter, even from
	//    finite exact values, whenever there is real fan-out (more than one
	//    active shard).
	if total > 1 && sel == total {
		if policy.Admission == AdmissionTargetedOnly {
			return Route{}, ErrScatterRejected
		}
		return r.buildRoute(
			RouteScatter, man, r.hits, dist, ver, targetScratch,
		), nil
	}

	// 7. Otherwise enforce the target-shard limit, then classify.
	if sel > policy.Limits.MaxTargetShards {
		if policy.Admission == AdmissionAllowScatterOnOverflow {
			return r.scatterAll(man, dist, ver, targetScratch), nil
		}
		return Route{}, &TargetLimitError{Limit: policy.Limits.MaxTargetShards, Count: sel}
	}
	kind := RouteTargeted
	if sel == 1 {
		kind = RouteSingle
	}
	return r.buildRoute(kind, man, r.hits, dist, ver, targetScratch), nil
}

// scatterOrReject handles an unknown route: rejected under targeted-only
// admission, otherwise a scatter over every active shard.
func (r *Router) scatterOrReject(
	policy RoutePolicy,
	man *Manifest,
	dist DistributionName,
	ver RoutingVersion,
	targetScratch []Target,
) (Route, error) {
	if policy.Admission == AdmissionTargetedOnly {
		return Route{}, ErrScatterRejected
	}
	return r.scatterAll(man, dist, ver, targetScratch), nil
}

// scatterAll builds a RouteScatter over every active shard.
func (r *Router) scatterAll(
	man *Manifest,
	dist DistributionName,
	ver RoutingVersion,
	targetScratch []Target,
) Route {
	r.hits = r.hits[:0]
	for i := 0; i < man.ShardCount(); i++ {
		r.hits = append(r.hits, i)
	}
	return r.buildRoute(
		RouteScatter, man, r.hits, dist, ver, targetScratch,
	)
}

// expand walks the Cartesian product of the deduplicated finite domains,
// admitting and mapping each combination and accumulating its destinations.
func (r *Router) expand(chosen int, mapper Mapper) error {
	mapperInto, hasInto := mapper.(MapperInto)
	r.combo = r.combo[:0]
	r.idx = r.idx[:0]
	for i := 0; i < chosen; i++ {
		r.combo = append(r.combo, Scalar{})
		r.idx = append(r.idx, 0)
	}
	for {
		for i := 0; i < chosen; i++ {
			r.combo[i] = r.dom[i].items[r.idx[i]].scalar
		}
		if err := mapper.Admits(chosen, r.combo); err != nil {
			return err
		}
		var ds DestinationSet
		var err error
		if hasInto {
			ds, err = mapperInto.MapPrefixInto(
				r.combo, r.mapPoints[:0], r.mapRanges[:0],
			)
		} else {
			ds, err = mapper.MapPrefix(r.combo)
		}
		if err != nil {
			return err
		}
		r.dest.Points = append(r.dest.Points, ds.Points...)
		r.dest.Ranges = append(r.dest.Ranges, ds.Ranges...)

		k := chosen - 1
		for k >= 0 {
			r.idx[k]++
			if r.idx[k] < len(r.dom[k].items) {
				break
			}
			r.idx[k] = 0
			k--
		}
		if k < 0 {
			return nil
		}
	}
}

// buildRoute selects leader-only targets for the given shard indices, carrying
// each shard's ownership epoch. It reuses targetScratch when capacity permits;
// otherwise it returns a freshly allocated Targets slice.
func (r *Router) buildRoute(
	kind RouteKind,
	man *Manifest,
	idx []int,
	dist DistributionName,
	ver RoutingVersion,
	targetScratch []Target,
) Route {
	var targets []Target
	if cap(targetScratch) < len(idx) {
		targets = make([]Target, len(idx))
	} else {
		targets = targetScratch[:len(idx)]
	}
	for k, i := range idx {
		s := man.shards[i]
		targets[k] = Target{
			Shard:          s.ID,
			Endpoint:       s.Leaders[0],
			OwnershipEpoch: s.Epoch,
			Role:           RoleLeader,
		}
	}
	return Route{Kind: kind, Distribution: dist, RoutingVersion: ver, Targets: targets}
}

// growDom ensures the per-ordinal domain scratch holds at least n entries.
func (r *Router) growDom(n int) {
	for len(r.dom) < n {
		r.dom = append(r.dom, canonSet{})
	}
}

// leadingFiniteLen counts the leading consecutive finite ordinals, capped by the
// mapper arity. It stops at the first non-finite ordinal, so an unknown
// component bounds the usable prefix.
func leadingFiniteLen(cons BoundConstraints, arity int) int {
	n := 0
	for i := 0; i < len(cons) && i < arity; i++ {
		if cons[i].Kind != DomainFinite {
			break
		}
		n++
	}
	return n
}

// productExceeds reports whether the product of the domain sizes exceeds limit,
// computed without overflowing int: each step tests p > limit/n before p*n so
// no intermediate product is ever formed once the bound is passed. Every domain
// size is at least one here, since empty domains are handled earlier.
func productExceeds(dom []canonSet, limit int) bool {
	p := 1
	for i := range dom {
		n := len(dom[i].items)
		if p > limit/n {
			return true
		}
		p *= n
	}
	return false
}

// resolveShardIndices resolves d's points and ranges to the sorted, deduplicated
// indices of the shards they touch, appending into dst. It reuses the manifest's
// binary search and never scans every shard for point lookups. It reports a
// *DestinationError for a malformed range.
func resolveShardIndices(m *Manifest, d DestinationSet, dst []int) ([]int, error) {
	for _, p := range d.Points {
		i := m.searchStart(p)
		if i < 0 || !m.shards[i].Range.Contains(p) {
			continue
		}
		dst = append(dst, i)
	}
	for _, rg := range d.Ranges {
		if !rg.Valid() {
			return dst, &DestinationError{Reason: "range start is not below its end"}
		}
		i := m.searchStart(rg.Start)
		if i < 0 {
			i = 0
		}
		for ; i < len(m.shards); i++ {
			if !pointBelowEnd(m.shards[i].Range.Start, rg.End) {
				break
			}
			if pointBelowEnd(rg.Start, m.shards[i].Range.End) {
				dst = append(dst, i)
			}
		}
	}
	if len(dst) <= 1 {
		return dst, nil
	}
	slices.Sort(dst)
	w := 1
	for k := 1; k < len(dst); k++ {
		if dst[k] != dst[w-1] {
			dst[w] = dst[k]
			w++
		}
	}
	return dst[:w], nil
}
