package topologyscheduler

import (
	"cmp"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

const MaxReplicaMoveCandidates = 1024

// ErrInvalidReplicaMove reports malformed, stale, or mutated capacity
// evidence at the replica-move scheduling boundary.
var ErrInvalidReplicaMove = errors.New("topologyscheduler: invalid replica move cut")

// ReplicaMoveCandidate binds one allocation's measured resource demand and
// migration estimate to an exact immutable topology generation. Demand is the
// load removed from the current first leader and projected onto its successor.
type ReplicaMoveCandidate struct {
	CatalogGeneration uint64
	Source            autosplit.SourceIdentity
	Demand            autosplit.CapacityVector
	MigrationBytes    uint64
}

// ReplicaMovePolicy bounds one deterministic scheduling cut. The unit of
// scheduling is always a physical allocation and endpoint, never a tenant.
type ReplicaMovePolicy struct {
	MaxMoves                  uint8
	MaxMovesPerSourceNode     uint8
	MaxMovesPerTargetNode     uint8
	MinProjectedReliefPPM     uint64
	MaxProjectedPressurePPM   uint64
	MaxPhysicalMigrationBytes uint64
	DistinctFailureDomains    bool
}

// DefaultReplicaMovePolicy returns conservative hot-node relief bounds.
func DefaultReplicaMovePolicy() ReplicaMovePolicy {
	return ReplicaMovePolicy{
		MaxMoves: 16, MaxMovesPerSourceNode: 2, MaxMovesPerTargetNode: 4,
		MinProjectedReliefPPM: 50_000, MaxProjectedPressurePPM: 850_000,
		MaxPhysicalMigrationBytes: 1 << 30,
		DistinctFailureDomains:    true,
	}
}

type replicaMovePlacement struct {
	candidate  uint16
	sourceNode uint16
	targetNode uint16
}

// ReplicaMoveCut is a compact, immutable selection into caller-owned
// candidate and node evidence. Diagnostics summarize why otherwise valid work
// was not selected without allocating per-candidate reason records.
type ReplicaMoveCut struct {
	catalogGeneration      uint64
	physicalMigrationBytes uint64
	evidence               capacityPlacementEvidence
	moves                  [MaxBatch]replicaMovePlacement
	count                  uint8

	Stale     uint16
	Invalid   uint16
	Duplicate uint16
	Relief    uint16
	Budget    uint16
	Saturated uint16
}

func (c ReplicaMoveCut) CatalogGeneration() uint64 { return c.catalogGeneration }
func (c ReplicaMoveCut) Count() int                { return int(c.count) }
func (c ReplicaMoveCut) PhysicalMigrationBytes() uint64 {
	return c.physicalMigrationBytes
}

// MoveAt returns candidate, source-node, and target-node ordinals for one
// selected move. ResolveReplicaMove must still be used for the fenced endpoint
// handoff to a membership owner.
func (c ReplicaMoveCut) MoveAt(index int) (uint16, uint16, uint16, bool) {
	if index < 0 || index >= int(c.count) {
		return 0, 0, 0, false
	}
	move := c.moves[index]
	return move.candidate, move.sourceNode, move.targetNode, true
}

// ReplicaMoveSelection is the allocation-level handoff to an external Raft
// membership owner. Group and member identities remain outside this advisory
// scheduler and must be attached under their own authority fences.
type ReplicaMoveSelection struct {
	CandidateOrdinal uint16
	Source           autosplit.SourceIdentity
	SourceEndpoint   distribution.EndpointID
	TargetEndpoint   distribution.EndpointID
	Demand           autosplit.CapacityVector
	MigrationBytes   uint64
}

type replicaMoveWork struct {
	sourceNode     uint16
	sourcePressure uint64
	initialRelief  uint64
	valid          bool
}

type replicaSourceReservation struct {
	released autosplit.CapacityVector
	moves    uint8
}

// ReplicaMoveWorkspace is caller-owned fixed scratch. It reuses the placement
// node index and per-node reservation vectors; warm scheduling allocates no
// heap memory for any admitted candidate or node count.
type ReplicaMoveWorkspace struct {
	placement   CapacityPlacementWorkspace
	order       [MaxReplicaMoveCandidates]uint16
	work        [MaxReplicaMoveCandidates]replicaMoveWork
	sourceSlot  [MaxPlacementNodes]uint8
	sources     [MaxBatch]replicaSourceReservation
	sourceCount uint8
}

// SelectReplicaMoves chooses allocation moves that reduce projected dominant
// steady-state pressure after accounting for every reservation in this cut.
// Transient receive and migration pressure are separately hard-capped.
func SelectReplicaMoves(
	catalog *gateway.Snapshot,
	candidates []ReplicaMoveCandidate,
	nodes []NodeCapacity,
	policy ReplicaMovePolicy,
	workspace *ReplicaMoveWorkspace,
) (ReplicaMoveCut, error) {
	if invalidReplicaMoveInputs(catalog, candidates, nodes, policy, workspace) {
		return ReplicaMoveCut{}, ErrInvalidReplicaMove
	}
	placementPolicy := CapacityPlacementPolicy{
		DistinctFailureDomains: policy.DistinctFailureDomains,
	}
	if !preparePlacementNodes(catalog, nodes, placementPolicy, &workspace.placement) {
		return ReplicaMoveCut{}, ErrInvalidReplicaMove
	}
	clear(workspace.sourceSlot[:len(nodes)])
	workspace.sourceCount = 0
	for index := range workspace.sources {
		workspace.sources[index] = replicaSourceReservation{}
	}
	for index := range candidates {
		workspace.order[index] = uint16(index)
		workspace.work[index] = prepareReplicaMoveWork(
			catalog, &candidates[index], nodes, policy, workspace,
		)
	}
	slices.SortFunc(workspace.order[:len(candidates)], func(left, right uint16) int {
		return compareReplicaMoveWork(
			&candidates[left], workspace.work[left],
			&candidates[right], workspace.work[right],
		)
	})

	cut := ReplicaMoveCut{
		catalogGeneration: catalog.Generation(),
		evidence:          replicaMoveFingerprint(candidates, nodes, policy),
	}
	for _, candidateOrdinal := range workspace.order[:len(candidates)] {
		candidate := &candidates[candidateOrdinal]
		work := workspace.work[candidateOrdinal]
		switch {
		case candidate.CatalogGeneration != catalog.Generation():
			cut.Stale++
			continue
		case !work.valid:
			cut.Invalid++
			continue
		case selectedReplicaMoveSource(&cut, candidates, candidate.Source):
			cut.Duplicate++
			continue
		case cut.physicalMigrationBytes > policy.MaxPhysicalMigrationBytes ||
			candidate.MigrationBytes > policy.MaxPhysicalMigrationBytes-cut.physicalMigrationBytes:
			cut.Budget++
			continue
		}

		var emptySourceReservation replicaSourceReservation
		sourceReservation, exists := replicaMoveSourceReservation(
			workspace, int(work.sourceNode), false,
		)
		if !exists {
			sourceReservation = &emptySourceReservation
		}
		if sourceReservation.moves >= policy.MaxMovesPerSourceNode {
			cut.Saturated++
			continue
		}
		sourceCurrent, sourceAfter, ok := projectedReplicaMoveSource(
			&nodes[work.sourceNode], int(work.sourceNode), candidate.Demand,
			sourceReservation, workspace,
		)
		if !ok || sourceCurrent <= sourceAfter ||
			sourceCurrent-sourceAfter < policy.MinProjectedReliefPPM {
			cut.Relief++
			continue
		}
		manifest, _ := catalog.Manifest(candidate.Source.Distribution)
		shard, _ := manifest.ShardOrdinalForRange(candidate.Source.Range)
		target, found := chooseReplicaMoveTarget(
			manifest, shard, int(work.sourceNode), sourceCurrent, sourceAfter,
			candidate, nodes, policy, workspace,
		)
		if !found {
			cut.Saturated++
			continue
		}
		if !exists {
			sourceReservation, exists = replicaMoveSourceReservation(
				workspace, int(work.sourceNode), true,
			)
			if !exists {
				cut.Saturated++
				continue
			}
		}
		reserveReplicaMove(
			target, candidate, sourceReservation, workspace,
		)
		cut.moves[cut.count] = replicaMovePlacement{
			candidate: candidateOrdinal, sourceNode: work.sourceNode,
			targetNode: uint16(target),
		}
		cut.count++
		cut.physicalMigrationBytes += candidate.MigrationBytes
		if cut.count == policy.MaxMoves {
			break
		}
	}
	return cut, nil
}

// ResolveReplicaMove rechecks the complete evidence fingerprint and exact
// allocation/endpoint membership before exposing one selected destination.
func ResolveReplicaMove(
	catalog *gateway.Snapshot,
	candidates []ReplicaMoveCandidate,
	nodes []NodeCapacity,
	policy ReplicaMovePolicy,
	cut ReplicaMoveCut,
	index int,
) (ReplicaMoveSelection, error) {
	if catalog == nil || index < 0 || index >= int(cut.count) ||
		cut.catalogGeneration != catalog.Generation() ||
		len(candidates) > MaxReplicaMoveCandidates || len(nodes) > MaxPlacementNodes ||
		cut.evidence != replicaMoveFingerprint(candidates, nodes, policy) {
		return ReplicaMoveSelection{}, ErrInvalidReplicaMove
	}
	move := cut.moves[index]
	if int(move.candidate) >= len(candidates) || int(move.sourceNode) >= len(nodes) ||
		int(move.targetNode) >= len(nodes) || move.sourceNode == move.targetNode {
		return ReplicaMoveSelection{}, ErrInvalidReplicaMove
	}
	candidate := candidates[move.candidate]
	if candidate.CatalogGeneration != catalog.Generation() ||
		!exactSource(catalog, candidate.Source) {
		return ReplicaMoveSelection{}, ErrInvalidReplicaMove
	}
	manifest, _ := catalog.Manifest(candidate.Source.Distribution)
	shard, _ := manifest.ShardOrdinalForRange(candidate.Source.Range)
	source, ok := manifest.ShardLeaderAt(shard, 0)
	if !ok || source != nodes[move.sourceNode].Endpoint ||
		nodes[move.sourceNode].CatalogGeneration != catalog.Generation() ||
		nodes[move.targetNode].CatalogGeneration != catalog.Generation() ||
		nodes[move.targetNode].Flags&NodePlacementReady == 0 ||
		manifestHasLeader(manifest, shard, nodes[move.targetNode].Endpoint) {
		return ReplicaMoveSelection{}, ErrInvalidReplicaMove
	}
	if _, err := catalog.Address(source); err != nil {
		return ReplicaMoveSelection{}, ErrInvalidReplicaMove
	}
	target := nodes[move.targetNode].Endpoint
	if _, err := catalog.Address(target); err != nil {
		return ReplicaMoveSelection{}, ErrInvalidReplicaMove
	}
	return ReplicaMoveSelection{
		CandidateOrdinal: move.candidate, Source: candidate.Source,
		SourceEndpoint: source, TargetEndpoint: target,
		Demand: candidate.Demand, MigrationBytes: candidate.MigrationBytes,
	}, nil
}

func invalidReplicaMoveInputs(
	catalog *gateway.Snapshot,
	candidates []ReplicaMoveCandidate,
	nodes []NodeCapacity,
	policy ReplicaMovePolicy,
	workspace *ReplicaMoveWorkspace,
) bool {
	return catalog == nil || workspace == nil || len(candidates) > MaxReplicaMoveCandidates ||
		len(nodes) == 0 || len(nodes) > MaxPlacementNodes || policy.MaxMoves == 0 ||
		policy.MaxMoves > MaxBatch || policy.MaxMovesPerSourceNode == 0 ||
		policy.MaxMovesPerTargetNode == 0 || policy.MinProjectedReliefPPM == 0 ||
		policy.MaxProjectedPressurePPM == 0 || policy.MaxPhysicalMigrationBytes == 0
}

func prepareReplicaMoveWork(
	catalog *gateway.Snapshot,
	candidate *ReplicaMoveCandidate,
	nodes []NodeCapacity,
	policy ReplicaMovePolicy,
	workspace *ReplicaMoveWorkspace,
) replicaMoveWork {
	if candidate.CatalogGeneration != catalog.Generation() ||
		!validReplicaMoveSource(candidate.Source) ||
		!exactSource(catalog, candidate.Source) || !nonzeroMoveDemand(candidate.Demand) {
		return replicaMoveWork{}
	}
	manifest, _ := catalog.Manifest(candidate.Source.Distribution)
	shard, _ := manifest.ShardOrdinalForRange(candidate.Source.Range)
	metadata, _ := manifest.ShardMetadataAt(shard)
	if metadata.LeaderCount == 0 || metadata.LeaderCount > MaxPlacementReplicas {
		return replicaMoveWork{}
	}
	for leader := 0; leader < metadata.LeaderCount; leader++ {
		endpoint, ok := manifest.ShardLeaderAt(shard, leader)
		if !ok {
			return replicaMoveWork{}
		}
		node, ok := placementNodeOrdinal(&workspace.placement, nodes, endpoint)
		if !ok {
			return replicaMoveWork{}
		}
		for prior := 0; prior < leader; prior++ {
			priorEndpoint, _ := manifest.ShardLeaderAt(shard, prior)
			if priorEndpoint == endpoint {
				return replicaMoveWork{}
			}
		}
		if leader == 0 {
			for resource := range autosplit.ResourceCount {
				if candidate.Demand[resource] > nodes[node].Used[resource] {
					return replicaMoveWork{}
				}
			}
		}
		if policy.DistinctFailureDomains && leader > 0 {
			for prior := 1; prior < leader; prior++ {
				priorEndpoint, _ := manifest.ShardLeaderAt(shard, prior)
				priorNode, _ := placementNodeOrdinal(
					&workspace.placement, nodes, priorEndpoint,
				)
				if nodes[priorNode].FailureDomain == nodes[node].FailureDomain {
					return replicaMoveWork{}
				}
			}
		}
	}
	sourceEndpoint, _ := manifest.ShardLeaderAt(shard, 0)
	sourceNode, _ := placementNodeOrdinal(&workspace.placement, nodes, sourceEndpoint)
	current, ok := replicaNodeSteadyPressure(&nodes[sourceNode], nodes[sourceNode].Used)
	if !ok {
		return replicaMoveWork{}
	}
	afterUsed := nodes[sourceNode].Used
	for resource := range autosplit.ResourceCount {
		afterUsed[resource] -= candidate.Demand[resource]
	}
	after, ok := replicaNodeSteadyPressure(&nodes[sourceNode], afterUsed)
	if !ok || current <= after {
		return replicaMoveWork{}
	}
	return replicaMoveWork{
		sourceNode: uint16(sourceNode), sourcePressure: current,
		initialRelief: current - after, valid: true,
	}
}

func validReplicaMoveSource(source autosplit.SourceIdentity) bool {
	return source.Distribution != "" && source.Shard != "" &&
		source.AllocationGeneration != 0 && source.Range.Valid() &&
		distribution.ValidVirtualBucketBits(source.BucketBits) &&
		source.RoutingVersion != 0 && source.OwnershipEpoch != 0
}

func nonzeroMoveDemand(demand autosplit.CapacityVector) bool {
	for resource := range autosplit.ResourceCount {
		if demand[resource] != 0 {
			return true
		}
	}
	return false
}

func compareReplicaMoveWork(
	left *ReplicaMoveCandidate,
	leftWork replicaMoveWork,
	right *ReplicaMoveCandidate,
	rightWork replicaMoveWork,
) int {
	if order := cmp.Compare(rightWork.initialRelief, leftWork.initialRelief); order != 0 {
		return order
	}
	if order := cmp.Compare(rightWork.sourcePressure, leftWork.sourcePressure); order != 0 {
		return order
	}
	if order := cmp.Compare(left.MigrationBytes, right.MigrationBytes); order != 0 {
		return order
	}
	if order := cmp.Compare(left.Source.Distribution, right.Source.Distribution); order != 0 {
		return order
	}
	if order := distribution.ComparePoints(left.Source.Range.Start, right.Source.Range.Start); order != 0 {
		return order
	}
	if order := compareRangeEnds(left.Source.Range.End, right.Source.Range.End); order != 0 {
		return order
	}
	if order := cmp.Compare(left.Source.AllocationGeneration, right.Source.AllocationGeneration); order != 0 {
		return order
	}
	if order := cmp.Compare(left.Source.Shard, right.Source.Shard); order != 0 {
		return order
	}
	if order := cmp.Compare(left.Source.OwnershipEpoch, right.Source.OwnershipEpoch); order != 0 {
		return order
	}
	if order := cmp.Compare(left.Source.RoutingVersion, right.Source.RoutingVersion); order != 0 {
		return order
	}
	if order := cmp.Compare(left.Source.BucketBits, right.Source.BucketBits); order != 0 {
		return order
	}
	for resource := range autosplit.ResourceCount {
		if order := cmp.Compare(left.Demand[resource], right.Demand[resource]); order != 0 {
			return order
		}
	}
	return cmp.Compare(left.CatalogGeneration, right.CatalogGeneration)
}

func selectedReplicaMoveSource(
	cut *ReplicaMoveCut,
	candidates []ReplicaMoveCandidate,
	source autosplit.SourceIdentity,
) bool {
	for index := 0; index < int(cut.count); index++ {
		if candidates[cut.moves[index].candidate].Source == source {
			return true
		}
	}
	return false
}

func placementNodeOrdinal(
	workspace *CapacityPlacementWorkspace,
	nodes []NodeCapacity,
	endpoint distribution.EndpointID,
) (int, bool) {
	mask := placementNodeIndexSlots - 1
	start := int(placementEndpointHash(endpoint)) & mask
	for probe := 0; probe < placementNodeIndexSlots; probe++ {
		position := (start + probe) & mask
		reference := workspace.nodeIndex[position]
		if reference == 0 {
			return 0, false
		}
		if nodes[reference-1].Endpoint == endpoint {
			return int(reference - 1), true
		}
	}
	return 0, false
}

func replicaMoveSourceReservation(
	workspace *ReplicaMoveWorkspace,
	node int,
	create bool,
) (*replicaSourceReservation, bool) {
	slot := workspace.sourceSlot[node]
	if slot != 0 {
		return &workspace.sources[slot-1], true
	}
	if !create || workspace.sourceCount == MaxBatch {
		return nil, false
	}
	workspace.sourceCount++
	workspace.sourceSlot[node] = workspace.sourceCount
	return &workspace.sources[workspace.sourceCount-1], true
}

func projectedReplicaMoveSource(
	node *NodeCapacity,
	nodeIndex int,
	demand autosplit.CapacityVector,
	reservation *replicaSourceReservation,
	workspace *ReplicaMoveWorkspace,
) (uint64, uint64, bool) {
	currentUsed := autosplit.CapacityVector{}
	afterUsed := autosplit.CapacityVector{}
	for resource := range autosplit.ResourceCount {
		if reservation.released[resource] > node.Used[resource] ||
			demand[resource] > node.Used[resource]-reservation.released[resource] {
			return 0, 0, false
		}
		base := node.Used[resource] - reservation.released[resource]
		var ok bool
		currentUsed[resource], ok = checkedPlacementAdd(
			base, workspace.placement.reserved[nodeIndex][resource], 0,
		)
		if !ok {
			return 0, 0, false
		}
		afterBase := base - demand[resource]
		afterUsed[resource], ok = checkedPlacementAdd(
			afterBase, workspace.placement.reserved[nodeIndex][resource], 0,
		)
		if !ok {
			return 0, 0, false
		}
	}
	current, ok := replicaNodeSteadyPressure(node, currentUsed)
	if !ok {
		return 0, 0, false
	}
	after, ok := replicaNodeSteadyPressure(node, afterUsed)
	return current, after, ok
}

func chooseReplicaMoveTarget(
	manifest *distribution.Manifest,
	shard int,
	sourceNode int,
	sourceCurrent uint64,
	sourceAfter uint64,
	candidate *ReplicaMoveCandidate,
	nodes []NodeCapacity,
	policy ReplicaMovePolicy,
	workspace *ReplicaMoveWorkspace,
) (int, bool) {
	bestNode := -1
	best := placementScore{pressure: ^uint64(0)}
	for nodeIndex := range nodes {
		node := &nodes[nodeIndex]
		if nodeIndex == sourceNode || node.Flags&NodePlacementReady == 0 ||
			workspace.placement.replicas[nodeIndex] >= uint16(policy.MaxMovesPerTargetNode) ||
			uint32(node.ActiveReceives)+uint32(workspace.placement.receives[nodeIndex]) >=
				uint32(node.MaxReceives) || manifestHasLeader(manifest, shard, node.Endpoint) ||
			(policy.DistinctFailureDomains &&
				remainingReplicaDomain(manifest, shard, node.FailureDomain, nodes, workspace)) {
			continue
		}
		steady, migration, ok := projectedReplicaMoveTarget(
			node, nodeIndex, candidate, workspace,
		)
		if !ok || steady > policy.MaxProjectedPressurePPM ||
			migration > policy.MaxProjectedPressurePPM {
			continue
		}
		objective := max(sourceAfter, steady)
		if sourceCurrent <= objective ||
			sourceCurrent-objective < policy.MinProjectedReliefPPM {
			continue
		}
		score := placementScore{
			pressure: objective,
			replicas: workspace.placement.replicas[nodeIndex] + 1,
			endpoint: node.Endpoint,
		}
		if bestNode < 0 || comparePlacementScore(score, best) < 0 {
			bestNode, best = nodeIndex, score
		}
	}
	return bestNode, bestNode >= 0
}

func projectedReplicaMoveTarget(
	node *NodeCapacity,
	nodeIndex int,
	candidate *ReplicaMoveCandidate,
	workspace *ReplicaMoveWorkspace,
) (uint64, uint64, bool) {
	released := autosplit.CapacityVector{}
	if reservation, ok := replicaMoveSourceReservation(workspace, nodeIndex, false); ok {
		released = reservation.released
	}
	used := autosplit.CapacityVector{}
	for resource := range autosplit.ResourceCount {
		if released[resource] > node.Used[resource] {
			return 0, 0, false
		}
		base := node.Used[resource] - released[resource]
		var ok bool
		used[resource], ok = checkedPlacementAdd(
			base, workspace.placement.reserved[nodeIndex][resource], candidate.Demand[resource],
		)
		if !ok {
			return 0, 0, false
		}
	}
	steady, ok := replicaNodeSteadyPressure(node, used)
	if !ok {
		return 0, 0, false
	}
	migration, ok := checkedPlacementAdd(
		node.MigrationUsed, workspace.placement.migration[nodeIndex], candidate.MigrationBytes,
	)
	if !ok || node.MigrationCapacity == 0 && migration != 0 {
		return 0, 0, false
	}
	return steady, placementRatioPPM(migration, node.MigrationCapacity), true
}

func replicaNodeSteadyPressure(
	node *NodeCapacity,
	used autosplit.CapacityVector,
) (uint64, bool) {
	pressure := uint64(0)
	for resource := range autosplit.ResourceCount {
		if node.Capacity[resource] == 0 && used[resource] != 0 {
			return 0, false
		}
		if node.Capacity[resource] != 0 {
			pressure = max(pressure, placementRatioPPM(used[resource], node.Capacity[resource]))
		}
	}
	return pressure, true
}

func remainingReplicaDomain(
	manifest *distribution.Manifest,
	shard int,
	domain uint32,
	nodes []NodeCapacity,
	workspace *ReplicaMoveWorkspace,
) bool {
	metadata, _ := manifest.ShardMetadataAt(shard)
	for leader := 1; leader < metadata.LeaderCount; leader++ {
		endpoint, _ := manifest.ShardLeaderAt(shard, leader)
		node, ok := placementNodeOrdinal(&workspace.placement, nodes, endpoint)
		if !ok || nodes[node].FailureDomain == domain {
			return true
		}
	}
	return false
}

func reserveReplicaMove(
	targetNode int,
	candidate *ReplicaMoveCandidate,
	source *replicaSourceReservation,
	workspace *ReplicaMoveWorkspace,
) {
	for resource := range autosplit.ResourceCount {
		source.released[resource] += candidate.Demand[resource]
		workspace.placement.reserved[targetNode][resource] += candidate.Demand[resource]
	}
	source.moves++
	workspace.placement.migration[targetNode] += candidate.MigrationBytes
	workspace.placement.receives[targetNode]++
	workspace.placement.replicas[targetNode]++
}

func replicaMoveFingerprint(
	candidates []ReplicaMoveCandidate,
	nodes []NodeCapacity,
	policy ReplicaMovePolicy,
) capacityPlacementEvidence {
	fingerprint := capacityPlacementEvidence{
		low: 0x84222325cbf29ce4, high: 0x94d049bb133111eb,
	}
	mix := func(value uint64) {
		fingerprint.low = mixCapacityPlacement(fingerprint.low, value, 0x100000001b3)
		fingerprint.high = mixCapacityPlacement(fingerprint.high, value, 0x9e3779b185ebca87)
	}
	mixString := func(value string) {
		mix(uint64(len(value)))
		for index := 0; index < len(value); index++ {
			mix(uint64(value[index]))
		}
	}
	mixPoint := func(point distribution.KeyspacePoint) {
		for index := range point {
			mix(uint64(point[index]))
		}
	}
	mix(uint64(len(candidates)))
	for index := range candidates {
		candidate := &candidates[index]
		mix(candidate.CatalogGeneration)
		mixString(string(candidate.Source.Distribution))
		mixString(string(candidate.Source.Shard))
		mix(uint64(candidate.Source.AllocationGeneration))
		mixPoint(candidate.Source.Range.Start)
		mixPoint(candidate.Source.Range.End.Point)
		mix(boolPlacementValue(candidate.Source.Range.End.Max))
		mix(uint64(candidate.Source.BucketBits))
		mix(uint64(candidate.Source.RoutingVersion))
		mix(uint64(candidate.Source.OwnershipEpoch))
		for resource := range autosplit.ResourceCount {
			mix(candidate.Demand[resource])
		}
		mix(candidate.MigrationBytes)
	}
	mix(uint64(len(nodes)))
	for index := range nodes {
		node := &nodes[index]
		mix(node.CatalogGeneration)
		mixString(string(node.Endpoint))
		mix(uint64(node.FailureDomain))
		mix(uint64(node.Flags))
		for resource := range autosplit.ResourceCount {
			mix(node.Capacity[resource])
			mix(node.Used[resource])
		}
		mix(node.MigrationCapacity)
		mix(node.MigrationUsed)
		mix(uint64(node.MaxReceives))
		mix(uint64(node.ActiveReceives))
	}
	mix(uint64(policy.MaxMoves))
	mix(uint64(policy.MaxMovesPerSourceNode))
	mix(uint64(policy.MaxMovesPerTargetNode))
	mix(policy.MinProjectedReliefPPM)
	mix(policy.MaxProjectedPressurePPM)
	mix(policy.MaxPhysicalMigrationBytes)
	mix(boolPlacementValue(policy.DistinctFailureDomains))
	return fingerprint
}
