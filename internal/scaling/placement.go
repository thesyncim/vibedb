// Package scaling contains the pure scaling admission planners.  The package
// reads a catalog cut and node directory, then returns advisory moves.  It
// does not publish a catalog, assign a Raft member, or grant authority.
package scaling

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const (
	// MaxPlacementGroups is the complete bounded inventory this planner will
	// inspect in one catalog cut.  A larger inventory is an explicit bound
	// failure; silently truncating it would make safe-to-stop claims unsound.
	MaxPlacementGroups = gateway.MaxScalingMovesPerIntent
	// MaxPlacementCandidates accounts for every serving replica in every
	// inspected group.  It is deliberately separate from MaxMoves: a batch may
	// reject early candidates and must continue scanning later groups.
	MaxPlacementCandidates = MaxPlacementGroups * gateway.ServingReplicaCount
)

var (
	// ErrInvalidPlacementInput reports malformed request, catalog, node, or
	// demand evidence.  A caller should repair the directory rather than retry
	// the same input.
	ErrInvalidPlacementInput = errors.New("scaling: invalid placement input")
	// ErrPlacementBound reports an inventory or output beyond the planner's
	// fixed bound.  No partial result is returned for an incomplete cut.
	ErrPlacementBound = errors.New("scaling: placement inventory bound exceeded")
)

// PlacementState classifies a valid planner result.  Blocked retains a
// concrete bounded reason for every blocked prefix that fits the diagnostic
// bound; NoWork means that the request is valid and no replica requires a
// move in this catalog generation.
type PlacementState uint8

const (
	PlacementNoWork PlacementState = iota + 1
	PlacementMoves
	PlacementBlocked
)

func (state PlacementState) Valid() bool {
	return state >= PlacementNoWork && state <= PlacementBlocked
}

// PlacementPolicy controls only hard admission and rebalance hysteresis. A
// zero per-node limit means that the node's durable MaxReceives is the only
// concurrency bound. A zero pressure bound leaves normalized pressure
// unconstrained; capacity itself is always enforced.
type PlacementPolicy struct {
	MaxMovesPerSource       uint16
	MaxMovesPerTarget       uint16
	MaxProjectedPressurePPM uint64
	MinImprovementPPM       uint64
	DistinctFailureDomains  bool
}

// DefaultPlacementPolicy uses failure-domain diversity and a small pressure
// hysteresis.  The request's HysteresisPPM is always applied in addition to
// this floor, so repeated plans cannot bounce a nearly equal allocation.
func DefaultPlacementPolicy() PlacementPolicy {
	return PlacementPolicy{
		MaxProjectedPressurePPM: 850_000,
		MinImprovementPPM:       10_000,
		DistinctFailureDomains:  true,
	}
}

// ReplicaDemand is an exact, generation-fenced demand measurement for one
// serving replica. The LiveBytes component represents the measured physical
// resident allocation used for placement admission; it is required for every
// non-empty replica, including a cold replica with no request traffic. A
// KnownEmpty record is the only valid zero-size allocation. MigrationBytes is
// the measured transfer bound for the same allocation.
type ReplicaDemand struct {
	// CatalogGeneration fences this measurement to the same immutable cut as
	// PlacementInput.Snapshot. A zero generation is never treated as current.
	CatalogGeneration uint64
	Group             raftmember.GroupKey
	ReplicaOrdinal    uint8
	Demand            autosplit.CapacityVector
	MigrationBytes    uint64
	// KnownEmpty is an authenticated measurement for an allocation that has
	// zero resident bytes and therefore legitimately has zero demand. It is
	// different from a missing observation: a cold, non-empty allocation must
	// carry its measured storage and migration bytes.
	KnownEmpty bool
}

// PlacementInput is one immutable planning cut. Nodes may include durable
// decommissioned incarnation tombstones; identity matching always uses the
// node ID and incarnation pair. InFlight contains durable reservations whose
// measured demand must also be present in Demands. ActiveReceives in a node
// record is the observed receive count; callers must provide a cut where
// those observations and durable InFlight reservations are disjoint. Snapshot
// is the sole routing authority.
type PlacementInput struct {
	Snapshot *gateway.Snapshot
	Nodes    []gateway.NodeRecord
	Request  gateway.ScalingIntentRequest
	Demands  []ReplicaDemand
	InFlight []gateway.GroupEnrollmentIntent
	Policy   PlacementPolicy
}

// ReplicaMove is an advisory exact source/target selection. Source is copied
// from the catalog route. Target contains the complete node record observed at
// TargetNodeRevision; it remains advisory until the controller reserves a
// certified enrollment and rechecks both revisions.
type ReplicaMove struct {
	Group                raftmember.GroupKey
	Distribution         distribution.DistributionName
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
	ReplicaOrdinal       uint8

	Source     gateway.ReplicatedReplicaDescriptor
	SourceNode gateway.NodeReference
	TargetNode gateway.NodeReference
	Target     gateway.NodeRecord

	ExpectedCatalogGeneration uint64
	SourceNodeRevision        uint64
	TargetNodeRevision        uint64
	Demand                    autosplit.CapacityVector
	MigrationBytes            uint64
	SourcePressurePPM         uint64
	TargetPressurePPM         uint64
	ImprovementPPM            uint64
}

// PlacementBlocker is a bounded, concrete explanation for work that could
// not be selected. Code values are stable enough for operator diagnostics;
// Detail carries the exact node/group fence that caused the refusal.
type PlacementBlocker struct {
	Code           string
	Detail         string
	Group          raftmember.GroupKey
	Distribution   distribution.DistributionName
	Shard          distribution.ShardID
	ReplicaOrdinal uint8
	Node           rafttransport.NodeID
	Revision       uint64
	SourceNode     rafttransport.NodeID
	TargetNode     rafttransport.NodeID
}

// PlacementPlan is a detached advisory result for one catalog generation.
// Moves are ordered canonically and can be applied in waves: after a move is
// certified and published, a fresh plan removes that group from the cut.
type PlacementPlan struct {
	State                     PlacementState
	ExpectedCatalogGeneration uint64
	Moves                     []ReplicaMove
	Blockers                  []PlacementBlocker
	ConsideredReplicas        uint32
	RemainingReplicas         uint32
}

func (plan PlacementPlan) HasMoves() bool { return len(plan.Moves) != 0 }

// Plan computes deterministic scale-in, scale-out, and rebalance moves. It
// enumerates the complete ReplicatedRouteAt inventory, including internal and
// request-ledger groups, and every serving replica ordinal. A valid blocked
// result has nil error and at least one concrete blocker.
func Plan(input PlacementInput) (PlacementPlan, error) {
	if err := validatePlacementInput(input); err != nil {
		return PlacementPlan{}, err
	}
	snapshot := input.Snapshot
	generation := snapshot.Generation()
	policy := input.Policy
	if policy == (PlacementPolicy{}) {
		policy = DefaultPlacementPolicy()
	}

	nodes, nodeByIdentity, nodeIDs, nodeOrder, err := prepareNodes(input.Nodes)
	if err != nil {
		return PlacementPlan{}, err
	}
	demandByKey, err := prepareDemands(input.Demands, generation)
	if err != nil {
		return PlacementPlan{}, err
	}
	intents, err := prepareIntents(input.InFlight)
	if err != nil {
		return PlacementPlan{}, err
	}

	plan := PlacementPlan{
		State:                     PlacementNoWork,
		ExpectedCatalogGeneration: generation,
	}
	if input.Request.Kind == gateway.ScalingScaleIn || input.Request.Kind == gateway.ScalingDecommission {
		if !prepareDrain(input.Request.Drain, nodes, nodeByIdentity, nodeIDs, generation, &plan) {
			plan.State = PlacementBlocked
			return plan, nil
		}
	}

	// Build the complete route inventory through the allocation-free route
	// seam. ReplicatedShardDescriptors would duplicate all replica strings and
	// make the bound less meaningful for a controller scan.
	routeCount := snapshot.ReplicatedRouteCount()
	if routeCount > MaxPlacementGroups {
		return PlacementPlan{}, ErrPlacementBound
	}
	candidates := make([]placementCandidate, 0, min(routeCount, MaxPlacementGroups))
	residents := make(map[rafttransport.NodeID]uint32, len(nodes))
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	for routeIndex := 0; routeIndex < routeCount; routeIndex++ {
		route, ok := snapshot.ReplicatedRouteAt(routeIndex, replicas[:0])
		if !ok || route.Group == (raftmember.GroupKey{}) || route.Distribution == "" ||
			route.Shard == "" || len(route.Replicas) != gateway.ServingReplicaCount {
			return PlacementPlan{}, fmt.Errorf("%w: invalid route at ordinal %d", ErrInvalidPlacementInput, routeIndex)
		}
		// ReplicatedRouteAt borrows the caller-provided replica workspace. A
		// candidate retains its route until all waves and target checks finish,
		// so detach this fixed three-element slice before the next route call.
		route.Replicas = slices.Clone(route.Replicas)
		for ordinal, replica := range route.Replicas {
			if replica.Member == 0 || replica.Node == (rafttransport.NodeID{}) ||
				replica.NodeIncarnation == 0 || replica.StoreID == ([16]byte{}) ||
				replica.Endpoint == "" || replica.NativeEndpoint == "" || replica.ControlEndpoint == "" ||
				replica.Endpoint == replica.NativeEndpoint || replica.Endpoint == replica.ControlEndpoint ||
				replica.NativeEndpoint == replica.ControlEndpoint {
				return PlacementPlan{}, fmt.Errorf("%w: invalid replica %d in route %d", ErrInvalidPlacementInput, ordinal, routeIndex)
			}
			for prior := 0; prior < ordinal; prior++ {
				if route.Replicas[prior].Node == replica.Node {
					return PlacementPlan{}, fmt.Errorf("%w: route %d repeats a physical node", ErrInvalidPlacementInput, routeIndex)
				}
			}
			residents[replica.Node]++
			if input.Request.Kind == gateway.ScalingScaleIn || input.Request.Kind == gateway.ScalingDecommission {
				if replica.Node != input.Request.Drain.NodeID {
					continue
				}
				if replica.NodeIncarnation != input.Request.Drain.Incarnation {
					addBlocker(&plan, PlacementBlocker{
						Code: BlockerStaleIncarnation, Detail: "route replica incarnation differs from requested drain",
						Group: route.Group, Distribution: route.Distribution, Shard: route.Shard,
						ReplicaOrdinal: uint8(ordinal), Node: replica.Node,
						Revision:   latestNodeRevision(input.Nodes, replica.Node),
						SourceNode: replica.Node,
					})
					continue
				}
			}
			demand, migration, evidenceOK := demandForReplica(
				demandByKey, route.Group, uint8(ordinal),
			)
			candidates = append(candidates, placementCandidate{
				route: route, replica: replica, ordinal: uint8(ordinal),
				demand: demand, migrationBytes: migration, demandOK: evidenceOK,
			})
		}
	}
	if len(candidates) > MaxPlacementCandidates {
		return PlacementPlan{}, ErrPlacementBound
	}
	plan.ConsideredReplicas = uint32(len(candidates))

	// The route inventory is authoritative; node records are only eligibility
	// and capacity evidence. Prepare an ordered target list once so every
	// candidate sees exactly the same deterministic target order.
	targets := targetOrdinals(input.Request, nodes, nodeByIdentity, nodeIDs, nodeOrder, generation, residents, &plan)
	if len(targets) == 0 {
		addBlocker(&plan, PlacementBlocker{
			Code: BlockerNoTarget, Detail: "no current-generation storage node is eligible for this scaling mode",
		})
	}
	freshScaleOutTargets := make(map[int]bool, len(targets))
	if input.Request.Kind == gateway.ScalingScaleOut {
		for _, target := range targets {
			freshScaleOutTargets[target] = residents[nodes[target].NodeID] == 0 &&
				zeroCapacityVector(nodes[target].Used) && nodes[target].ActiveReceives == 0 &&
				nodes[target].MigrationUsed == 0
		}
	}

	for index := range candidates {
		candidates[index].sourceNode, candidates[index].sourceOK = resolveSourceNode(
			candidates[index], nodes, nodeByIdentity, nodeIDs, generation, input.Request, &plan,
		)
		if candidates[index].sourceOK {
			candidates[index].initialPressure, candidates[index].initialRelief = initialSourcePressure(
				candidates[index], nodes, candidates[index].sourceNode,
			)
		}
	}
	slices.SortStableFunc(candidates, func(left, right placementCandidate) int {
		return compareCandidates(left, right, input.Request.Kind)
	})

	state := placementState{
		reserved:       make(map[int]autosplit.CapacityVector, len(nodes)),
		released:       make(map[int]autosplit.CapacityVector, len(nodes)),
		migration:      make(map[int]uint64, len(nodes)),
		sourceMoves:    make(map[int]uint32, len(nodes)),
		targetMoves:    make(map[int]uint32, len(nodes)),
		groups:         make(map[raftmember.GroupKey]struct{}, len(input.InFlight)),
		groupTargets:   make(map[groupNodeKey]struct{}, len(input.InFlight)),
		invalidTargets: make(map[int]PlacementBlocker, len(input.InFlight)),
	}
	activeIntents := make([]gateway.GroupEnrollmentIntent, 0, len(intents))
	for _, intent := range intents {
		activeIntents = append(activeIntents, intent)
	}
	slices.SortStableFunc(activeIntents, func(left, right gateway.GroupEnrollmentIntent) int {
		if order := compareGroups(left.Group, right.Group); order != 0 {
			return order
		}
		if order := cmp.Compare(left.ReplicaOrdinal, right.ReplicaOrdinal); order != 0 {
			return order
		}
		if order := bytes.Compare(left.Target.Node[:], right.Target.Node[:]); order != 0 {
			return order
		}
		if order := cmp.Compare(left.Target.NodeIncarnation, right.Target.NodeIncarnation); order != 0 {
			return order
		}
		return bytes.Compare(left.IntentID[:], right.IntentID[:])
	})
	for _, intent := range activeIntents {
		group := intent.Group
		if intent.State == gateway.EnrollmentComplete {
			continue
		}
		state.groups[group] = struct{}{}
		targetIndex, ok := nodeByIdentity[nodeIdentity{node: intent.Target.Node, incarnation: intent.Target.NodeIncarnation}]
		if !ok {
			code := BlockerMissingNode
			if _, hasNode := nodeIDs[intent.Target.Node]; hasNode {
				code = BlockerStaleIncarnation
			}
			addBlocker(&plan, PlacementBlocker{
				Code: code, Detail: "in-flight target identity is absent from the directory",
				Group: intent.Group, Distribution: intent.Distribution, Shard: intent.Shard,
				ReplicaOrdinal: intent.ReplicaOrdinal, Node: intent.Target.Node,
				Revision: intent.TargetNodeRevision, TargetNode: intent.Target.Node,
			})
			continue
		}
		targetNode := nodes[targetIndex]
		invalidateTarget := func(blocker PlacementBlocker) {
			addBlocker(&plan, blocker)
			state.invalidTargets[targetIndex] = blocker
		}
		if targetNode.CatalogGeneration != generation || intent.CatalogGeneration != generation {
			invalidateTarget(PlacementBlocker{
				Code: BlockerStaleGeneration, Detail: "in-flight enrollment is fenced to another catalog generation",
				Group: intent.Group, Distribution: intent.Distribution, Shard: intent.Shard,
				ReplicaOrdinal: intent.ReplicaOrdinal, Node: targetNode.NodeID,
				Revision: targetNode.Revision, TargetNode: targetNode.NodeID,
			})
			continue
		}
		if targetNode.Revision != intent.TargetNodeRevision || targetNode.Incarnation != intent.Target.NodeIncarnation {
			invalidateTarget(PlacementBlocker{
				Code: BlockerStaleRevision, Detail: "in-flight target revision or incarnation changed",
				Group: intent.Group, Distribution: intent.Distribution, Shard: intent.Shard,
				ReplicaOrdinal: intent.ReplicaOrdinal, Node: targetNode.NodeID,
				Revision: targetNode.Revision, TargetNode: targetNode.NodeID,
			})
			continue
		}
		if targetNode.Roles&gateway.NodeRoleStorage == 0 || targetNode.Lifecycle != gateway.NodeActive {
			invalidateTarget(PlacementBlocker{
				Code: BlockerInvalidLifecycle, Detail: "in-flight enrollment target is not an Active storage node",
				Group: intent.Group, Distribution: intent.Distribution, Shard: intent.Shard,
				ReplicaOrdinal: intent.ReplicaOrdinal, Node: targetNode.NodeID,
				Revision: targetNode.Revision, TargetNode: targetNode.NodeID,
			})
			continue
		}
		sourceIndex, sourceFound := nodeByIdentity[nodeIdentity{node: intent.Source.Node, incarnation: intent.Source.NodeIncarnation}]
		if !sourceFound {
			code := BlockerMissingNode
			if _, hasNode := nodeIDs[intent.Source.Node]; hasNode {
				code = BlockerStaleIncarnation
			}
			invalidateTarget(PlacementBlocker{
				Code: code, Detail: "in-flight source identity is absent from the directory",
				Group: intent.Group, Distribution: intent.Distribution, Shard: intent.Shard,
				ReplicaOrdinal: intent.ReplicaOrdinal, Node: intent.Source.Node,
				Revision: targetNode.Revision, SourceNode: intent.Source.Node,
			})
			continue
		}
		demand, migrationBytes, demandOK := demandForReplica(demandByKey, intent.Group, intent.ReplicaOrdinal)
		if !demandOK {
			invalidateTarget(PlacementBlocker{
				Code: BlockerCapacityEvidence, Detail: "in-flight enrollment has no generation-fenced storage and migration measurement",
				Group: intent.Group, Distribution: intent.Distribution, Shard: intent.Shard,
				ReplicaOrdinal: intent.ReplicaOrdinal, Node: targetNode.NodeID,
				Revision: targetNode.Revision, TargetNode: targetNode.NodeID,
			})
			continue
		}
		state.sourceMoves[sourceIndex]++
		state.targetMoves[targetIndex]++
		state.groupTargets[groupNodeKey{group: intent.Group, node: targetIndex}] = struct{}{}
		if ok, reason := reserveInFlight(&state, targetIndex, targetNode, demand, migrationBytes); !ok {
			invalidateTarget(PlacementBlocker{
				Code: reason, Detail: "in-flight reservation exceeds a hard target bound",
				Group: intent.Group, Distribution: intent.Distribution, Shard: intent.Shard,
				ReplicaOrdinal: intent.ReplicaOrdinal, Node: targetNode.NodeID,
				Revision: targetNode.Revision, TargetNode: targetNode.NodeID,
			})
		}
	}

	maxMoves := int(input.Request.MaxMoves)
	if maxMoves > len(plan.Moves) {
		// This is only a capacity hint. The output is grown to the exact bounded
		// request below and cannot exceed MaxScalingMovesPerIntent.
		plan.Moves = make([]ReplicaMove, 0, min(maxMoves, gateway.MaxScalingMovesPerIntent))
	}
	for index := range candidates {
		candidate := &candidates[index]
		if len(plan.Moves) >= maxMoves {
			plan.RemainingReplicas++
			continue
		}
		if !candidate.sourceOK {
			plan.RemainingReplicas++
			continue
		}
		if !candidate.demandOK {
			addCandidateBlocker(&plan, *candidate, PlacementBlocker{
				Code: BlockerCapacityEvidence, Detail: "replica has no generation-fenced storage and migration measurement",
			})
			plan.RemainingReplicas++
			continue
		}
		if _, found := state.groups[candidate.route.Group]; found {
			addCandidateBlocker(&plan, *candidate, PlacementBlocker{
				Code: BlockerConcurrentGroup, Detail: "group already has an in-flight or selected enrollment",
			})
			plan.RemainingReplicas++
			continue
		}
		if input.Request.Kind == gateway.ScalingScaleIn || input.Request.Kind == gateway.ScalingDecommission {
			if candidate.replica.Node != input.Request.Drain.NodeID ||
				candidate.replica.NodeIncarnation != input.Request.Drain.Incarnation {
				continue
			}
		}
		if input.Request.MaxMigrationBytes != 0 &&
			(candidate.migrationBytes > input.Request.MaxMigrationBytes ||
				state.plannedMigration > input.Request.MaxMigrationBytes-candidate.migrationBytes) {
			addCandidateBlocker(&plan, *candidate, PlacementBlocker{
				Code: BlockerMigrationBudget, Detail: "migration budget is exhausted for this wave",
			})
			plan.RemainingReplicas++
			continue
		}
		if state.totalMigration > math.MaxUint64-candidate.migrationBytes {
			addCandidateBlocker(&plan, *candidate, PlacementBlocker{
				Code: BlockerOverflow, Detail: "migration total would overflow the bounded cut",
			})
			plan.RemainingReplicas++
			continue
		}
		selected, blocker := chooseTarget(
			candidate, targets, nodes, freshScaleOutTargets, &state, input.Request, policy,
		)
		if blocker.Code != "" {
			addCandidateBlocker(&plan, *candidate, blocker)
			plan.RemainingReplicas++
			continue
		}
		if selected < 0 {
			addCandidateBlocker(&plan, *candidate, PlacementBlocker{
				Code: BlockerNoSafeTarget, Detail: "all eligible targets violate a hard placement constraint",
			})
			plan.RemainingReplicas++
			continue
		}
		move := makeReplicaMove(*candidate, nodes[candidate.sourceNode], nodes[selected], generation)
		if sourceBefore, sourceAfter, ok := projectedSource(candidate, nodes[candidate.sourceNode], candidate.sourceNode, &state); ok {
			if _, targetPressure, targetOK, _ := projectedTarget(*candidate, nodes[selected], selected, &state); targetOK {
				move.SourcePressurePPM = sourceBefore
				move.TargetPressurePPM = targetPressure
				objective := max(sourceAfter, targetPressure)
				if sourceBefore > objective {
					move.ImprovementPPM = sourceBefore - objective
				}
			}
		}
		plan.Moves = append(plan.Moves, move)
		reserveMove(&state, candidate.sourceNode, selected, nodes[candidate.sourceNode], move)
		if input.Request.Kind == gateway.ScalingScaleOut {
			// Empty-node bootstrap is a one-copy admission exception. Once a
			// measured replica is reserved, subsequent copies in this wave use
			// the same improving objective as an activated target.
			freshScaleOutTargets[selected] = false
		}
		state.groups[candidate.route.Group] = struct{}{}
		state.groupTargets[groupNodeKey{group: candidate.route.Group, node: selected}] = struct{}{}
	}
	if len(plan.Moves) != 0 {
		plan.State = PlacementMoves
	} else if plan.ConsideredReplicas == 0 && len(plan.Blockers) == 0 ||
		len(plan.Blockers) != 0 && onlyNoImprovementBlockers(plan.Blockers) {
		plan.State = PlacementNoWork
		plan.RemainingReplicas = 0
	} else {
		plan.State = PlacementBlocked
	}
	return plan, nil
}

// PlanPlacements is a descriptive alias used by controllers that prefer an
// operation-oriented name.
func PlanPlacements(input PlacementInput) (PlacementPlan, error) { return Plan(input) }

const (
	BlockerStaleGeneration   = "stale-generation"
	BlockerStaleIncarnation  = "stale-incarnation"
	BlockerStaleRevision     = "stale-revision"
	BlockerMissingNode       = "missing-node"
	BlockerInvalidLifecycle  = "invalid-lifecycle"
	BlockerNoTarget          = "no-target"
	BlockerNoSafeTarget      = "no-safe-target"
	BlockerTargetCapacity    = "target-capacity"
	BlockerTargetPressure    = "target-pressure"
	BlockerFailureDomain     = "failure-domain"
	BlockerExistingMember    = "existing-member"
	BlockerMigrationCapacity = "migration-capacity"
	BlockerMigrationBudget   = "migration-budget"
	BlockerConcurrency       = "concurrency"
	BlockerConcurrentGroup   = "concurrent-group"
	BlockerNoImprovement     = "no-improvement"
	BlockerOverflow          = "arithmetic-overflow"
	BlockerTargetNotEmpty    = "target-not-empty"
	// BlockerJoiningNotEmpty is retained as a source-compatible spelling for
	// callers that used the pre-readiness name. ScaleOut now requires Active.
	BlockerJoiningNotEmpty  = BlockerTargetNotEmpty
	BlockerCapacityEvidence = "awaiting-capacity-evidence"
	BlockerSourceCapacity   = "source-capacity"
)

type placementCandidate struct {
	route           gateway.ReplicatedRoute
	replica         gateway.ReplicatedEndpoint
	ordinal         uint8
	demand          autosplit.CapacityVector
	migrationBytes  uint64
	demandOK        bool
	sourceNode      int
	sourceOK        bool
	initialPressure uint64
	initialRelief   uint64
}

type placementState struct {
	reserved         map[int]autosplit.CapacityVector
	released         map[int]autosplit.CapacityVector
	migration        map[int]uint64
	totalMigration   uint64
	plannedMigration uint64
	sourceMoves      map[int]uint32
	targetMoves      map[int]uint32
	groups           map[raftmember.GroupKey]struct{}
	groupTargets     map[groupNodeKey]struct{}
	invalidTargets   map[int]PlacementBlocker
}

type groupNodeKey struct {
	group raftmember.GroupKey
	node  int
}

type demandKey struct {
	group   raftmember.GroupKey
	ordinal uint8
}

type nodeIdentity struct {
	node        rafttransport.NodeID
	incarnation uint64
}

func validatePlacementInput(input PlacementInput) error {
	if input.Snapshot == nil || !input.Request.Valid() || len(input.Nodes) == 0 ||
		len(input.Nodes) > gateway.MaxScalingNodes || len(input.Demands) > MaxPlacementCandidates ||
		len(input.InFlight) > gateway.MaxScalingMovesPerIntent {
		return ErrInvalidPlacementInput
	}
	// DesiredNodeCount is resolved to an immutable target set by the
	// controller before this stateless planner runs. Accepting it without
	// targets would make a valid-looking request silently depend on an
	// unbounded directory scan and could choose a different node on retry.
	if input.Request.Kind == gateway.ScalingScaleOut && len(input.Request.Targets) == 0 {
		return ErrInvalidPlacementInput
	}
	if input.Snapshot.Generation() == 0 {
		return ErrInvalidPlacementInput
	}
	if input.Request.MaxMoves == 0 || input.Request.MaxMoves > gateway.MaxScalingMovesPerIntent {
		return ErrInvalidPlacementInput
	}
	if input.Policy.MaxProjectedPressurePPM > 1_000_000 || input.Policy.MinImprovementPPM > 1_000_000 {
		return ErrInvalidPlacementInput
	}
	return nil
}

func prepareNodes(input []gateway.NodeRecord) ([]gateway.NodeRecord, map[nodeIdentity]int, map[rafttransport.NodeID]struct{}, []int, error) {
	nodes := slices.Clone(input)
	byIdentity := make(map[nodeIdentity]int, len(nodes))
	nodeIDs := make(map[rafttransport.NodeID]struct{}, len(nodes))
	order := make([]int, len(nodes))
	for index := range nodes {
		if !nodes[index].Valid() {
			return nil, nil, nil, nil, fmt.Errorf("%w: node %d is invalid", ErrInvalidPlacementInput, index)
		}
		key := nodeIdentity{node: nodes[index].NodeID, incarnation: nodes[index].Incarnation}
		if _, duplicate := byIdentity[key]; duplicate {
			return nil, nil, nil, nil, fmt.Errorf("%w: duplicate node identity", ErrInvalidPlacementInput)
		}
		byIdentity[key] = index
		nodeIDs[nodes[index].NodeID] = struct{}{}
		order[index] = index
	}
	slices.SortFunc(order, func(left, right int) int {
		if result := bytes.Compare(nodes[left].NodeID[:], nodes[right].NodeID[:]); result != 0 {
			return result
		}
		return cmp.Compare(nodes[left].Incarnation, nodes[right].Incarnation)
	})
	return nodes, byIdentity, nodeIDs, order, nil
}

func prepareDemands(input []ReplicaDemand, generation uint64) (map[demandKey]ReplicaDemand, error) {
	if len(input) == 0 {
		return nil, nil
	}
	result := make(map[demandKey]ReplicaDemand, len(input))
	for _, demand := range input {
		if !validGroup(demand.Group) || demand.ReplicaOrdinal >= gateway.ServingReplicaCount ||
			demand.CatalogGeneration != generation || demand.KnownEmpty &&
			(nonzeroDemand(demand.Demand) || demand.MigrationBytes != 0) {
			return nil, fmt.Errorf("%w: invalid replica demand key", ErrInvalidPlacementInput)
		}
		if !demand.KnownEmpty && (demand.Demand[autosplit.ResourceLiveBytes] == 0 || demand.MigrationBytes == 0) {
			return nil, fmt.Errorf("%w: replica demand lacks measured storage or migration bytes", ErrInvalidPlacementInput)
		}
		key := demandKey{group: demand.Group, ordinal: demand.ReplicaOrdinal}
		if _, duplicate := result[key]; duplicate {
			return nil, fmt.Errorf("%w: duplicate replica demand", ErrInvalidPlacementInput)
		}
		result[key] = demand
	}
	return result, nil
}

func prepareIntents(input []gateway.GroupEnrollmentIntent) (map[raftmember.GroupKey]gateway.GroupEnrollmentIntent, error) {
	result := make(map[raftmember.GroupKey]gateway.GroupEnrollmentIntent, len(input))
	for _, intent := range input {
		if !intent.Valid() {
			return nil, fmt.Errorf("%w: invalid enrollment intent", ErrInvalidPlacementInput)
		}
		// Completed rows are durable history, not active reservations. A
		// directory can retain more than one completed incarnation for a group;
		// only the active state participates in admission and uniqueness.
		if intent.State == gateway.EnrollmentComplete {
			continue
		}
		if _, duplicate := result[intent.Group]; duplicate {
			return nil, fmt.Errorf("%w: duplicate in-flight group", ErrInvalidPlacementInput)
		}
		result[intent.Group] = intent
	}
	return result, nil
}

func prepareDrain(
	drain gateway.NodeReference,
	nodes []gateway.NodeRecord,
	byIdentity map[nodeIdentity]int,
	nodeIDs map[rafttransport.NodeID]struct{},
	generation uint64,
	plan *PlacementPlan,
) bool {
	index, found := byIdentity[nodeIdentity{node: drain.NodeID, incarnation: drain.Incarnation}]
	if !found {
		code := BlockerMissingNode
		detail := "requested drain node is absent from the directory"
		if _, hasNode := nodeIDs[drain.NodeID]; hasNode {
			code = BlockerStaleIncarnation
			detail = "requested drain incarnation differs from the directory"
		}
		addBlocker(plan, PlacementBlocker{Code: code, Detail: detail, Node: drain.NodeID, Revision: latestNodeRevision(nodes, drain.NodeID)})
		return false
	}
	node := nodes[index]
	if node.CatalogGeneration != generation {
		addBlocker(plan, PlacementBlocker{Code: BlockerStaleGeneration, Detail: "drain node evidence is from another catalog generation", Node: drain.NodeID, Revision: node.Revision})
		return false
	}
	if node.Lifecycle != gateway.NodeDraining {
		addBlocker(plan, PlacementBlocker{Code: BlockerInvalidLifecycle, Detail: "evacuation requires the node to be durably Draining", Node: drain.NodeID, Revision: node.Revision})
		return false
	}
	return true
}

func targetOrdinals(
	request gateway.ScalingIntentRequest,
	nodes []gateway.NodeRecord,
	byIdentity map[nodeIdentity]int,
	nodeIDs map[rafttransport.NodeID]struct{},
	order []int,
	generation uint64,
	residents map[rafttransport.NodeID]uint32,
	plan *PlacementPlan,
) []int {
	requested := make(map[rafttransport.NodeID]gateway.NodeReference, len(request.Targets))
	for _, target := range request.Targets {
		requested[target.NodeID] = target
	}
	for _, target := range request.Targets {
		if _, ok := nodeIDs[target.NodeID]; !ok {
			addBlocker(plan, PlacementBlocker{
				Code: BlockerMissingNode, Detail: "requested target is absent from the directory",
				Node: target.NodeID,
			})
		} else if _, ok := byIdentity[nodeIdentity{node: target.NodeID, incarnation: target.Incarnation}]; !ok {
			addBlocker(plan, PlacementBlocker{
				Code: BlockerStaleIncarnation, Detail: "requested target incarnation differs from the directory",
				Node: target.NodeID, Revision: latestNodeRevision(nodes, target.NodeID),
			})
		}
	}
	result := make([]int, 0, len(nodes))
	for _, index := range order {
		node := nodes[index]
		if len(requested) != 0 {
			target, ok := requested[node.NodeID]
			if !ok {
				continue
			}
			if target.Incarnation != node.Incarnation {
				continue
			}
		}
		if node.CatalogGeneration != generation {
			addBlocker(plan, PlacementBlocker{Code: BlockerStaleGeneration, Detail: "target evidence is from another catalog generation", Node: node.NodeID, Revision: node.Revision})
			continue
		}
		if node.Roles&gateway.NodeRoleStorage == 0 || node.Lifecycle == gateway.NodeDraining || node.Lifecycle == gateway.NodeDecommissioned {
			if len(requested) != 0 {
				addBlocker(plan, PlacementBlocker{Code: BlockerInvalidLifecycle, Detail: "target is not a live storage node", Node: node.NodeID, Revision: node.Revision})
			}
			continue
		}
		if len(requested) != 0 && node.Lifecycle != gateway.NodeActive {
			addBlocker(plan, PlacementBlocker{Code: BlockerInvalidLifecycle, Detail: "target must be Active before enrollment reservation", Node: node.NodeID, Revision: node.Revision})
			continue
		}
		switch request.Kind {
		case gateway.ScalingScaleOut:
			// The provisioner owns Joining readiness and endpoint
			// certification. Placement only sees the post-registration Active
			// record, which is deliberately empty until this first wave.
			if node.Lifecycle != gateway.NodeActive {
				continue
			}
			// A fresh target must be empty. An explicitly named target may be
			// non-empty after an earlier wave of the same durable intent; its
			// identity and revision are still checked above and hard capacity
			// admission below applies to every subsequent copy.
			freshPhysical := residents[node.NodeID] == 0 && zeroCapacityVector(node.Used)
			if freshPhysical && (node.ActiveReceives != 0 || node.MigrationUsed != 0) {
				addBlocker(plan, PlacementBlocker{Code: BlockerTargetNotEmpty, Detail: "scale-out target has active work before first enrollment", Node: node.NodeID, Revision: node.Revision})
				continue
			}
			if len(requested) == 0 && !freshPhysical {
				addBlocker(plan, PlacementBlocker{Code: BlockerTargetNotEmpty, Detail: "scale-out target must be an empty active storage node", Node: node.NodeID, Revision: node.Revision})
				continue
			}
		case gateway.ScalingScaleIn, gateway.ScalingDecommission, gateway.ScalingRebalance:
			if node.Lifecycle != gateway.NodeActive {
				continue
			}
		}
		if _, ok := byIdentity[nodeIdentity{node: node.NodeID, incarnation: node.Incarnation}]; ok {
			result = append(result, index)
		}
	}
	return result
}

func demandForReplica(
	demands map[demandKey]ReplicaDemand,
	group raftmember.GroupKey,
	ordinal uint8,
) (autosplit.CapacityVector, uint64, bool) {
	if demand, ok := demands[demandKey{group: group, ordinal: ordinal}]; ok {
		return demand.Demand, demand.MigrationBytes, true
	}
	return autosplit.CapacityVector{}, 0, false
}

func latestNodeRevision(nodes []gateway.NodeRecord, nodeID rafttransport.NodeID) uint64 {
	var revision uint64
	for _, node := range nodes {
		if node.NodeID == nodeID && node.Revision > revision {
			revision = node.Revision
		}
	}
	return revision
}

func resolveSourceNode(
	candidate placementCandidate,
	nodes []gateway.NodeRecord,
	byIdentity map[nodeIdentity]int,
	nodeIDs map[rafttransport.NodeID]struct{},
	generation uint64,
	request gateway.ScalingIntentRequest,
	plan *PlacementPlan,
) (int, bool) {
	index, found := byIdentity[nodeIdentity{node: candidate.replica.Node, incarnation: candidate.replica.NodeIncarnation}]
	if !found {
		code := BlockerMissingNode
		detail := "route replica has no physical node record"
		if _, hasNode := nodeIDs[candidate.replica.Node]; hasNode {
			code = BlockerStaleIncarnation
			detail = "route replica incarnation differs from source node record"
		}
		addCandidateBlocker(plan, candidate, PlacementBlocker{Code: code, Detail: detail, Node: candidate.replica.Node, Revision: latestNodeRevision(nodes, candidate.replica.Node), SourceNode: candidate.replica.Node})
		return 0, false
	}
	node := nodes[index]
	if node.CatalogGeneration != generation {
		addCandidateBlocker(plan, candidate, PlacementBlocker{Code: BlockerStaleGeneration, Detail: "source node evidence is from another catalog generation", Node: node.NodeID, Revision: node.Revision, SourceNode: node.NodeID})
		return 0, false
	}
	if node.Roles&gateway.NodeRoleStorage == 0 {
		addCandidateBlocker(plan, candidate, PlacementBlocker{Code: BlockerInvalidLifecycle, Detail: "route replica source is not a storage node", Node: node.NodeID, Revision: node.Revision, SourceNode: node.NodeID})
		return 0, false
	}
	if node.Lifecycle != gateway.NodeActive {
		// A scale-in source is handled separately and may be overloaded. No
		// other mode may use a non-Active node as a source or refill it.
		if !(node.Lifecycle == gateway.NodeDraining &&
			(request.Kind == gateway.ScalingScaleIn || request.Kind == gateway.ScalingDecommission)) {
			addCandidateBlocker(plan, candidate, PlacementBlocker{Code: BlockerInvalidLifecycle, Detail: "source node is not Active or the requested Draining source", Node: node.NodeID, Revision: node.Revision, SourceNode: node.NodeID})
			return 0, false
		}
	}
	return index, true
}

func initialSourcePressure(candidate placementCandidate, nodes []gateway.NodeRecord, source int) (uint64, uint64) {
	node := nodes[source]
	used := node.Used
	for resource := range autosplit.ResourceCount {
		if candidate.demand[resource] > used[resource] {
			return pressure(node.Capacity, used), 0
		}
		used[resource] -= candidate.demand[resource]
	}
	return pressure(node.Capacity, nodes[source].Used), pressure(node.Capacity, nodes[source].Used) - pressure(node.Capacity, used)
}

func compareCandidates(left, right placementCandidate, kind gateway.ScalingKind) int {
	if kind == gateway.ScalingRebalance {
		if order := cmp.Compare(right.initialRelief, left.initialRelief); order != 0 {
			return order
		}
		if order := cmp.Compare(right.initialPressure, left.initialPressure); order != 0 {
			return order
		}
	}
	if order := cmp.Compare(left.route.Distribution, right.route.Distribution); order != 0 {
		return order
	}
	if order := cmp.Compare(left.route.Shard, right.route.Shard); order != 0 {
		return order
	}
	if order := compareGroups(left.route.Group, right.route.Group); order != 0 {
		return order
	}
	if order := cmp.Compare(left.ordinal, right.ordinal); order != 0 {
		return order
	}
	if order := bytes.Compare(left.replica.Node[:], right.replica.Node[:]); order != 0 {
		return order
	}
	return cmp.Compare(left.replica.Member, right.replica.Member)
}

func chooseTarget(
	candidate *placementCandidate,
	targets []int,
	nodes []gateway.NodeRecord,
	freshScaleOutTargets map[int]bool,
	state *placementState,
	request gateway.ScalingIntentRequest,
	policy PlacementPolicy,
) (int, PlacementBlocker) {
	best := -1
	bestObjective := uint64(math.MaxUint64)
	bestPressure := uint64(math.MaxUint64)
	bestReceives := uint32(math.MaxUint32)
	var firstBlocker PlacementBlocker
	var firstHardBlocker PlacementBlocker
	var firstNoImprovement PlacementBlocker
	sourceNode := candidate.sourceNode
	sourceCurrent, sourceAfter, sourceOK := projectedSource(
		candidate, nodes[sourceNode], sourceNode, state,
	)
	if !sourceOK {
		return -1, PlacementBlocker{Code: BlockerSourceCapacity, Detail: "measured source demand exceeds the source's resident usage"}
	}
	if policy.MaxMovesPerSource != 0 && state.sourceMoves[sourceNode] >= uint32(policy.MaxMovesPerSource) {
		return -1, PlacementBlocker{
			Code: BlockerConcurrency, Detail: "source move concurrency limit reached",
			Node: nodes[sourceNode].NodeID, Revision: nodes[sourceNode].Revision,
			SourceNode: nodes[sourceNode].NodeID,
		}
	}
	for _, target := range targets {
		node := &nodes[target]
		if target == sourceNode {
			continue
		}
		if blocker, invalid := state.invalidTargets[target]; invalid {
			if firstBlocker.Code == "" {
				firstBlocker = blocker
				firstBlocker.TargetNode = node.NodeID
			}
			continue
		}
		if _, duplicate := state.groupTargets[groupNodeKey{group: candidate.route.Group, node: target}]; duplicate {
			if firstBlocker.Code == "" {
				firstBlocker = PlacementBlocker{Code: BlockerConcurrentGroup, Detail: "target already selected for this group"}
			}
			continue
		}
		if policy.MaxMovesPerTarget != 0 && state.targetMoves[target] >= uint32(policy.MaxMovesPerTarget) {
			if firstBlocker.Code == "" {
				firstBlocker = PlacementBlocker{Code: BlockerConcurrency, Detail: "target move concurrency limit reached", Node: node.NodeID, Revision: node.Revision, TargetNode: node.NodeID}
			}
			continue
		}
		if node.ActiveReceives > node.MaxReceives || uint64(node.ActiveReceives)+uint64(state.targetMoves[target]) >= uint64(node.MaxReceives) {
			if firstBlocker.Code == "" {
				firstBlocker = PlacementBlocker{Code: BlockerConcurrency, Detail: "target receive slots are exhausted", Node: node.NodeID, Revision: node.Revision, TargetNode: node.NodeID}
			}
			continue
		}
		if memberOnRoute(candidate.route, node.NodeID) {
			if firstBlocker.Code == "" {
				firstBlocker = PlacementBlocker{Code: BlockerExistingMember, Detail: "target physical node already hosts this group", Node: node.NodeID, Revision: node.Revision, TargetNode: node.NodeID}
			}
			continue
		}
		if policy.DistinctFailureDomains {
			ok, stale := targetDomainAvailable(candidate.route, candidate.ordinal, node, nodes)
			if !ok {
				if firstBlocker.Code == "" {
					code := BlockerFailureDomain
					detail := "target failure domain is already used by another replica"
					if stale {
						code, detail = BlockerStaleIncarnation, "another route replica has no matching current node identity"
					}
					firstBlocker = PlacementBlocker{Code: code, Detail: detail, Node: node.NodeID, Revision: node.Revision, TargetNode: node.NodeID}
				}
				continue
			}
		}
		projected, targetPressure, ok, reason := projectedTarget(*candidate, *node, target, state)
		if !ok {
			if firstHardBlocker.Code == "" {
				firstHardBlocker = PlacementBlocker{Code: reason, Detail: "target projected usage violates a hard capacity or migration bound", Node: node.NodeID, Revision: node.Revision, TargetNode: node.NodeID}
			}
			if firstBlocker.Code == "" {
				firstBlocker = PlacementBlocker{Code: reason, Detail: "target projected usage violates a hard capacity or migration bound", Node: node.NodeID, Revision: node.Revision, TargetNode: node.NodeID}
			}
			continue
		}
		if request.MaxMigrationBytes != 0 {
			// The total cut is checked at reservation time; this branch only
			// keeps a concrete reason available if every target otherwise fits.
			// The actual remaining budget is attached by reserveMove.
		}
		objective := max(sourceAfter, targetPressure)
		before := max(sourceCurrent, pressureWithReservation(*node, target, state))
		freshScaleOut := request.Kind == gateway.ScalingScaleOut && freshScaleOutTargets[target]
		if request.Kind == gateway.ScalingRebalance || request.Kind == gateway.ScalingScaleOut && !freshScaleOut {
			threshold := max(request.HysteresisPPM, policy.MinImprovementPPM)
			if !strictImprovement(before, objective, threshold) {
				if firstNoImprovement.Code == "" {
					firstNoImprovement = PlacementBlocker{Code: BlockerNoImprovement, Detail: "normalized capacity objective does not improve beyond hysteresis", Node: node.NodeID, Revision: node.Revision, TargetNode: node.NodeID}
				}
				continue
			}
			if policy.MaxProjectedPressurePPM != 0 && targetPressure > policy.MaxProjectedPressurePPM {
				if firstHardBlocker.Code == "" {
					firstHardBlocker = PlacementBlocker{Code: BlockerTargetPressure, Detail: "projected target pressure exceeds policy", Node: node.NodeID, Revision: node.Revision, TargetNode: node.NodeID}
				}
				if firstBlocker.Code == "" {
					firstBlocker = PlacementBlocker{Code: BlockerTargetPressure, Detail: "projected target pressure exceeds policy", Node: node.NodeID, Revision: node.Revision, TargetNode: node.NodeID}
				}
				continue
			}
		}
		if best < 0 || objective < bestObjective || objective == bestObjective &&
			(targetPressure < bestPressure || targetPressure == bestPressure &&
				(state.targetMoves[target] < bestReceives || state.targetMoves[target] == bestReceives && bytes.Compare(node.NodeID[:], nodes[best].NodeID[:]) < 0)) {
			best, bestObjective, bestPressure, bestReceives = target, objective, targetPressure, state.targetMoves[target]
		}
		_ = projected
	}
	if best >= 0 {
		return best, PlacementBlocker{}
	}
	if firstHardBlocker.Code != "" {
		return -1, firstHardBlocker
	}
	if firstNoImprovement.Code != "" {
		return -1, firstNoImprovement
	}
	return -1, firstBlocker
}

func projectedSource(candidate *placementCandidate, node gateway.NodeRecord, index int, state *placementState) (uint64, uint64, bool) {
	base, ok := projectedBase(node, index, state, true)
	if !ok {
		return 0, 0, false
	}
	after := base
	for resource := range autosplit.ResourceCount {
		release := candidate.demand[resource]
		if release > after[resource] {
			return 0, 0, false
		}
		after[resource] -= release
	}
	return pressure(node.Capacity, base), pressure(node.Capacity, after), true
}

func projectedTarget(candidate placementCandidate, node gateway.NodeRecord, index int, state *placementState) (autosplit.CapacityVector, uint64, bool, string) {
	// A target copy is admitted against transient physical usage. A source
	// release is credited only after that move retires, so two simultaneous
	// copies cannot use the same bytes twice.
	base, ok := projectedBase(node, index, state, false)
	if !ok {
		return autosplit.CapacityVector{}, 0, false, BlockerOverflow
	}
	projected := base
	for resource := range autosplit.ResourceCount {
		if candidate.demand[resource] > math.MaxUint64-projected[resource] {
			return autosplit.CapacityVector{}, 0, false, BlockerOverflow
		}
		projected[resource] += candidate.demand[resource]
		if node.Capacity[resource] == 0 && projected[resource] != 0 ||
			node.Capacity[resource] != 0 && projected[resource] > node.Capacity[resource] {
			return autosplit.CapacityVector{}, 0, false, BlockerTargetCapacity
		}
	}
	migration := node.MigrationUsed
	reservation := state.migration[index]
	if migration > math.MaxUint64-reservation {
		return autosplit.CapacityVector{}, 0, false, BlockerOverflow
	}
	migration += reservation
	if candidate.migrationBytes > math.MaxUint64-migration {
		return autosplit.CapacityVector{}, 0, false, BlockerOverflow
	}
	migration += candidate.migrationBytes
	if node.MigrationCapacity == 0 && migration != 0 || node.MigrationCapacity != 0 && migration > node.MigrationCapacity {
		return autosplit.CapacityVector{}, 0, false, BlockerMigrationCapacity
	}
	return projected, pressure(node.Capacity, projected), true, ""
}

func projectedBase(node gateway.NodeRecord, index int, state *placementState, creditRelease bool) (autosplit.CapacityVector, bool) {
	base := node.Used
	released := state.released[index]
	reserved := state.reserved[index]
	for resource := range autosplit.ResourceCount {
		if creditRelease {
			if released[resource] > base[resource] {
				return autosplit.CapacityVector{}, false
			}
			base[resource] -= released[resource]
		}
		if reserved[resource] > math.MaxUint64-base[resource] {
			return autosplit.CapacityVector{}, false
		}
		base[resource] += reserved[resource]
	}
	return base, true
}

func pressure(capacity, used autosplit.CapacityVector) uint64 {
	result := uint64(0)
	for resource := range autosplit.ResourceCount {
		if used[resource] == 0 {
			continue
		}
		if capacity[resource] == 0 {
			return math.MaxUint64
		}
		hi, lo := bits.Mul64(used[resource], 1_000_000)
		if hi >= capacity[resource] {
			return math.MaxUint64
		}
		value, _ := bits.Div64(hi, lo, capacity[resource])
		if value > result {
			result = value
		}
	}
	return result
}

func pressureWithReservation(node gateway.NodeRecord, index int, state *placementState) uint64 {
	base, ok := projectedBase(node, index, state, false)
	if !ok {
		return math.MaxUint64
	}
	return pressure(node.Capacity, base)
}

func targetDomainAvailable(route gateway.ReplicatedRoute, ordinal uint8, target *gateway.NodeRecord, nodes []gateway.NodeRecord) (bool, bool) {
	for index, replica := range route.Replicas {
		if uint8(index) == ordinal {
			continue
		}
		nodeIndex := -1
		for candidateIndex := range nodes {
			if nodes[candidateIndex].NodeID == replica.Node && nodes[candidateIndex].Incarnation == replica.NodeIncarnation {
				nodeIndex = candidateIndex
				break
			}
		}
		if nodeIndex < 0 {
			return false, true
		}
		if nodes[nodeIndex].FailureDomain == target.FailureDomain {
			return false, false
		}
	}
	return true, false
}

func reserveInFlight(state *placementState, target int, node gateway.NodeRecord, demand autosplit.CapacityVector, migrationBytes uint64) (bool, string) {
	reserved := state.reserved[target]
	for resource := range autosplit.ResourceCount {
		if demand[resource] > math.MaxUint64-reserved[resource] {
			return false, BlockerOverflow
		}
		projected := node.Used[resource] + reserved[resource]
		if projected < node.Used[resource] || demand[resource] > math.MaxUint64-projected {
			return false, BlockerOverflow
		}
		projected += demand[resource]
		if node.Capacity[resource] == 0 && projected != 0 ||
			node.Capacity[resource] != 0 && projected > node.Capacity[resource] {
			return false, BlockerTargetCapacity
		}
		reserved[resource] += demand[resource]
	}
	if migrationBytes > math.MaxUint64-state.migration[target] ||
		migrationBytes > math.MaxUint64-state.totalMigration {
		return false, BlockerOverflow
	}
	migration := node.MigrationUsed
	if migration > math.MaxUint64-state.migration[target] {
		return false, BlockerOverflow
	}
	migration += state.migration[target]
	if migrationBytes > math.MaxUint64-migration {
		return false, BlockerOverflow
	}
	migration += migrationBytes
	if node.MigrationCapacity == 0 && migration != 0 ||
		node.MigrationCapacity != 0 && migration > node.MigrationCapacity {
		return false, BlockerMigrationCapacity
	}
	state.reserved[target] = reserved
	state.migration[target] += migrationBytes
	state.totalMigration += migrationBytes
	return true, ""
}

func reserveMove(state *placementState, source, target int, sourceNode gateway.NodeRecord, move ReplicaMove) {
	released := state.released[source]
	for resource := range autosplit.ResourceCount {
		release := move.Demand[resource]
		base := sourceNode.Used[resource]
		if prior := released[resource]; prior < base {
			base -= prior
		} else {
			base = 0
		}
		if release > base {
			// A selected move has already passed projectedSource. Keep this
			// defensive clamp so a future accounting change cannot over-release
			// a source and make a later target check unsound.
			release = base
		}
		released[resource] = released[resource] + release
	}
	state.released[source] = released
	reserved := state.reserved[target]
	for resource := range autosplit.ResourceCount {
		reserved[resource] = reserved[resource] + move.Demand[resource]
	}
	state.reserved[target] = reserved
	state.migration[target] += move.MigrationBytes
	state.totalMigration += move.MigrationBytes
	state.plannedMigration += move.MigrationBytes
	state.sourceMoves[source]++
	state.targetMoves[target]++
}

func makeReplicaMove(candidate placementCandidate, source, target gateway.NodeRecord, generation uint64) ReplicaMove {
	return ReplicaMove{
		Group: candidate.route.Group, Distribution: candidate.route.Distribution,
		Shard: candidate.route.Shard, AllocationGeneration: distribution.ShardAllocationGeneration(candidate.route.AllocationGeneration),
		ReplicaOrdinal: candidate.ordinal,
		Source: gateway.ReplicatedReplicaDescriptor{
			Member: candidate.replica.Member, Node: candidate.replica.Node,
			StoreID: candidate.replica.StoreID, NodeIncarnation: candidate.replica.NodeIncarnation,
			Endpoint:        distribution.EndpointID(candidate.replica.Endpoint),
			NativeEndpoint:  distribution.EndpointID(candidate.replica.NativeEndpoint),
			ControlEndpoint: distribution.EndpointID(candidate.replica.ControlEndpoint),
		},
		SourceNode: gateway.NodeReference{NodeID: source.NodeID, Incarnation: source.Incarnation},
		TargetNode: gateway.NodeReference{NodeID: target.NodeID, Incarnation: target.Incarnation},
		Target:     target, ExpectedCatalogGeneration: generation,
		SourceNodeRevision: source.Revision, TargetNodeRevision: target.Revision,
		Demand: candidate.demand, MigrationBytes: candidate.migrationBytes,
	}
}

func memberOnRoute(route gateway.ReplicatedRoute, node rafttransport.NodeID) bool {
	for _, replica := range route.Replicas {
		if replica.Node == node {
			return true
		}
	}
	return false
}

func strictImprovement(before, after, threshold uint64) bool {
	if after >= before {
		return false
	}
	return before-after >= threshold+1 || threshold == 0
}

func addCandidateBlocker(plan *PlacementPlan, candidate placementCandidate, blocker PlacementBlocker) {
	blocker.Group = candidate.route.Group
	blocker.Distribution = candidate.route.Distribution
	blocker.Shard = candidate.route.Shard
	blocker.ReplicaOrdinal = candidate.ordinal
	if blocker.SourceNode == (rafttransport.NodeID{}) {
		blocker.SourceNode = candidate.replica.Node
	}
	addBlocker(plan, blocker)
}

func addBlocker(plan *PlacementPlan, blocker PlacementBlocker) {
	if len(plan.Blockers) >= gateway.MaxScalingBlockers {
		return
	}
	if blocker.Code == "" {
		return
	}
	for _, existing := range plan.Blockers {
		if existing.Code == blocker.Code && existing.Group == blocker.Group &&
			existing.ReplicaOrdinal == blocker.ReplicaOrdinal && existing.Node == blocker.Node &&
			existing.TargetNode == blocker.TargetNode {
			return
		}
	}
	plan.Blockers = append(plan.Blockers, blocker)
}

func onlyNoImprovementBlockers(blockers []PlacementBlocker) bool {
	if len(blockers) == 0 {
		return false
	}
	for _, blocker := range blockers {
		if blocker.Code != BlockerNoImprovement {
			return false
		}
	}
	return true
}

func compareGroups(left, right raftmember.GroupKey) int {
	for _, pair := range [][2][]byte{
		{left.ClusterID[:], right.ClusterID[:]},
		{left.ClusterIncarnation[:], right.ClusterIncarnation[:]},
	} {
		if order := bytes.Compare(pair[0], pair[1]); order != 0 {
			return order
		}
	}
	if order := cmp.Compare(left.TopologyRecoveryEpoch, right.TopologyRecoveryEpoch); order != 0 {
		return order
	}
	for _, pair := range [][2][]byte{
		{left.ShardIncarnation[:], right.ShardIncarnation[:]},
		{left.GroupID[:], right.GroupID[:]},
	} {
		if order := bytes.Compare(pair[0], pair[1]); order != 0 {
			return order
		}
	}
	return 0
}

func validGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func nonzeroDemand(demand autosplit.CapacityVector) bool {
	for resource := range autosplit.ResourceCount {
		if demand[resource] != 0 {
			return true
		}
	}
	return false
}

func zeroCapacityVector(vector autosplit.CapacityVector) bool {
	for resource := range autosplit.ResourceCount {
		if vector[resource] != 0 {
			return false
		}
	}
	return true
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
