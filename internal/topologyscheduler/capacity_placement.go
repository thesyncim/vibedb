package topologyscheduler

import (
	"cmp"
	"errors"
	"fmt"
	"math/bits"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
)

const (
	MaxPlacementNodes       = 4096
	MaxPlacementReplicas    = 5
	MaxPlacedChildren       = MaxBatch * (autosplit.MaxSplitChildren - 1)
	placementNodeIndexSlots = MaxPlacementNodes * 2
)

// ErrInvalidCapacityPlacement reports stale evidence, unsatisfied hard
// constraints, exhausted capacity, or a mutated placement handoff.
var ErrInvalidCapacityPlacement = errors.New("topologyscheduler: invalid capacity placement cut")

// NodeFlags is a compact eligibility mask for one placement report.
type NodeFlags uint8

const NodePlacementReady NodeFlags = 1 << iota

// NodeCapacity is one generation-fenced, fixed-width placement report. Used
// and Capacity share SABLE's resource units. Migration capacity is the bytes
// this node can ingest during the scheduler cut, independent of durable space.
type NodeCapacity struct {
	CatalogGeneration uint64
	Endpoint          distribution.EndpointID
	FailureDomain     uint32
	Flags             NodeFlags

	Capacity autosplit.CapacityVector
	Used     autosplit.CapacityVector

	MigrationCapacity uint64
	MigrationUsed     uint64
	MaxReceives       uint16
	ActiveReceives    uint16
}

// DestinationReservation is a topology owner-provided identity plus projected
// steady-state and migration demand for one non-retained split child.
type DestinationReservation struct {
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
	OwnershipEpoch       distribution.OwnershipEpoch
	Demand               autosplit.CapacityVector
	MigrationBytes       uint64
}

// SplitReservation aligns destination identities with one admitted split.
// Destinations omit the retained child and remain in child key-range order.
type SplitReservation struct {
	RetainChild      uint8
	DestinationCount uint8
	Destinations     [autosplit.MaxSplitChildren - 1]DestinationReservation
}

// CapacityPlacementPolicy bounds projected load and topology concentration.
type CapacityPlacementPolicy struct {
	ReplicaCount              uint8
	MaxProjectedPressurePPM   uint64
	MaxPhysicalMigrationBytes uint64
	MaxNewReplicasPerNode     uint16
	MaxNewPrimariesPerNode    uint16
	ExcludeSourceLeaders      bool
	DistinctFailureDomains    bool
}

// DefaultCapacityPlacementPolicy returns conservative single-replica bounds.
func DefaultCapacityPlacementPolicy() CapacityPlacementPolicy {
	return CapacityPlacementPolicy{
		ReplicaCount: 1, MaxProjectedPressurePPM: 850_000,
		MaxPhysicalMigrationBytes: 1 << 30,
		MaxNewReplicasPerNode:     16, MaxNewPrimariesPerNode: 4,
		ExcludeSourceLeaders: true, DistinctFailureDomains: true,
	}
}

type placedDestination struct {
	decisionIndex    uint8
	destinationIndex uint8
	replicaCount     uint8
	nodes            [MaxPlacementReplicas]uint16
}

// CapacityPlacementCut is a fixed-size endpoint-ordinal decision. Node reports
// are advisory; the later plan bridge rechecks endpoint membership and all
// catalog/source/allocation fences.
type CapacityPlacementCut struct {
	catalogGeneration      uint64
	physicalMigrationBytes uint64
	evidence               capacityPlacementEvidence
	count                  uint8
	replicaCount           uint8
	destinations           [MaxPlacedChildren]placedDestination
}

// CatalogGeneration reports the catalog cut all node evidence was fenced to.
func (c CapacityPlacementCut) CatalogGeneration() uint64 { return c.catalogGeneration }

// Count reports placed non-retained children.
func (c CapacityPlacementCut) Count() int { return int(c.count) }

// PhysicalMigrationBytes reports logical child bytes multiplied by replicas.
func (c CapacityPlacementCut) PhysicalMigrationBytes() uint64 {
	return c.physicalMigrationBytes
}

// EndpointOrdinal returns a selected NodeCapacity ordinal.
func (c CapacityPlacementCut) EndpointOrdinal(
	destination, replica int,
) (uint16, bool) {
	if destination < 0 || destination >= int(c.count) || replica < 0 ||
		replica >= int(c.destinations[destination].replicaCount) {
		return 0, false
	}
	return c.destinations[destination].nodes[replica], true
}

// BuildCapacityPlacedSplitPlanBatch materializes the cut's endpoint ordinals
// into one compact temporary leader backing and invokes the exact fenced split
// plan builder. Capacity evidence remains advisory; topology identity and
// endpoint membership are rechecked against catalog.
func BuildCapacityPlacedSplitPlanBatch(
	catalog *gateway.Snapshot,
	candidates []SplitCandidate,
	decision Decision,
	reservations []SplitReservation,
	nodes []NodeCapacity,
	policy CapacityPlacementPolicy,
	cut CapacityPlacementCut,
) (*SplitPlanBatch, error) {
	if catalog == nil || cut.catalogGeneration != catalog.Generation() ||
		decision.CatalogGeneration != catalog.Generation() || decision.Count == 0 ||
		decision.Count > MaxBatch || len(candidates) > MaxCandidates ||
		cut.replicaCount == 0 || cut.replicaCount > MaxPlacementReplicas ||
		len(reservations) != int(decision.Count) ||
		cut.evidence != capacityPlacementFingerprint(
			candidates, decision, reservations, nodes, policy,
		) {
		return nil, ErrInvalidCapacityPlacement
	}
	expectedChildren := 0
	for index := range reservations {
		if reservations[index].DestinationCount == 0 ||
			reservations[index].DestinationCount > autosplit.MaxSplitChildren-1 {
			return nil, ErrInvalidCapacityPlacement
		}
		expectedChildren += int(reservations[index].DestinationCount)
	}
	if expectedChildren != int(cut.count) || expectedChildren > MaxPlacedChildren {
		return nil, ErrInvalidCapacityPlacement
	}

	placements := make([]SplitPlacement, len(reservations))
	leaders := make(
		[]distribution.EndpointID,
		expectedChildren*int(cut.replicaCount),
	)
	physicalMigration := uint64(0)
	output := 0
	for decisionIndex := range reservations {
		reservation := &reservations[decisionIndex]
		placement := &placements[decisionIndex]
		placement.RetainChild = reservation.RetainChild
		placement.DestinationCount = reservation.DestinationCount
		for destinationIndex := 0; destinationIndex < int(reservation.DestinationCount); destinationIndex++ {
			placed := &cut.destinations[output]
			if placed.decisionIndex != uint8(decisionIndex) ||
				placed.destinationIndex != uint8(destinationIndex) ||
				placed.replicaCount != cut.replicaCount {
				return nil, ErrInvalidCapacityPlacement
			}
			child := &reservation.Destinations[destinationIndex]
			start := output * int(cut.replicaCount)
			childLeaders := leaders[start : start+int(cut.replicaCount)]
			for replica := 0; replica < int(cut.replicaCount); replica++ {
				nodeOrdinal := int(placed.nodes[replica])
				if nodeOrdinal >= len(nodes) ||
					nodes[nodeOrdinal].CatalogGeneration != catalog.Generation() ||
					nodes[nodeOrdinal].Flags&NodePlacementReady == 0 {
					return nil, ErrInvalidCapacityPlacement
				}
				endpoint := nodes[nodeOrdinal].Endpoint
				if _, err := catalog.Address(endpoint); err != nil {
					return nil, ErrInvalidCapacityPlacement
				}
				for prior := 0; prior < replica; prior++ {
					if childLeaders[prior] == endpoint {
						return nil, ErrInvalidCapacityPlacement
					}
				}
				childLeaders[replica] = endpoint
				if child.MigrationBytes > ^uint64(0)-physicalMigration {
					return nil, ErrInvalidCapacityPlacement
				}
				physicalMigration += child.MigrationBytes
			}
			placement.Destinations[destinationIndex] = autosplit.Destination{
				Shard: child.Shard, AllocationGeneration: child.AllocationGeneration,
				OwnershipEpoch: child.OwnershipEpoch, Leaders: childLeaders,
			}
			output++
		}
	}
	if physicalMigration != cut.physicalMigrationBytes {
		return nil, ErrInvalidCapacityPlacement
	}
	batch, err := BuildSplitPlanBatch(catalog, candidates, decision, placements)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCapacityPlacement, err)
	}
	return batch, nil
}

type placementWork struct {
	decisionIndex    uint8
	destinationIndex uint8
	outputIndex      uint8
	dominantSharePPM uint64
	migrationBytes   uint64
}

// CapacityPlacementWorkspace is caller-owned fixed scratch. Reuse keeps
// placement allocation-free even for the maximum node and child counts.
type CapacityPlacementWorkspace struct {
	nodeIndex [placementNodeIndexSlots]uint16
	workOrder [MaxPlacedChildren]uint8
	work      [MaxPlacedChildren]placementWork

	reserved       [MaxPlacementNodes]autosplit.CapacityVector
	migration      [MaxPlacementNodes]uint64
	receives       [MaxPlacementNodes]uint16
	replicas       [MaxPlacementNodes]uint16
	primaries      [MaxPlacementNodes]uint16
	totalCapacity  autosplit.CapacityVector
	totalMigration uint64
}

// PlaceSplitDestinations chooses topology-prepared nodes for every non-retained
// child in an admission decision. It uses dominant-resource fairness for work
// order, then minimizes projected node pressure with deterministic spreading.
func PlaceSplitDestinations(
	catalog *gateway.Snapshot,
	candidates []SplitCandidate,
	decision Decision,
	reservations []SplitReservation,
	nodes []NodeCapacity,
	policy CapacityPlacementPolicy,
	workspace *CapacityPlacementWorkspace,
) (CapacityPlacementCut, error) {
	if invalidCapacityPlacementInputs(
		catalog, candidates, decision, reservations, nodes, policy, workspace,
	) {
		return CapacityPlacementCut{}, ErrInvalidCapacityPlacement
	}
	if !preparePlacementNodes(catalog, nodes, policy, workspace) {
		return CapacityPlacementCut{}, ErrInvalidCapacityPlacement
	}
	workCount, ok := preparePlacementWork(
		catalog, candidates, decision, reservations, workspace,
	)
	if !ok {
		return CapacityPlacementCut{}, ErrInvalidCapacityPlacement
	}
	cut := CapacityPlacementCut{
		catalogGeneration: catalog.Generation(), count: uint8(workCount),
		replicaCount: policy.ReplicaCount,
		evidence: capacityPlacementFingerprint(
			candidates, decision, reservations, nodes, policy,
		),
	}
	for _, workOrdinal := range workspace.workOrder[:workCount] {
		work := workspace.work[workOrdinal]
		reservation := &reservations[work.decisionIndex].Destinations[work.destinationIndex]
		placed := &cut.destinations[work.outputIndex]
		placed.decisionIndex = work.decisionIndex
		placed.destinationIndex = work.destinationIndex
		placed.replicaCount = policy.ReplicaCount

		candidate := &candidates[decision.Ordinals[work.decisionIndex]]
		manifest, _ := catalog.Manifest(candidate.Recommendation.Source.Distribution)
		sourceOrdinal, _ := manifest.ShardOrdinalForRange(candidate.Recommendation.Source.Range)
		for replica := 0; replica < int(policy.ReplicaCount); replica++ {
			node, projected, found := choosePlacementNode(
				manifest, sourceOrdinal, nodes, policy, workspace,
				reservation, placed, replica,
			)
			if !found || !reservePlacementNode(
				node, replica == 0, projected, reservation, policy, workspace, &cut,
			) {
				return CapacityPlacementCut{}, ErrInvalidCapacityPlacement
			}
			placed.nodes[replica] = uint16(node)
		}
	}
	return cut, nil
}

type capacityPlacementEvidence struct {
	low  uint64
	high uint64
}

func capacityPlacementFingerprint(
	candidates []SplitCandidate,
	decision Decision,
	reservations []SplitReservation,
	nodes []NodeCapacity,
	policy CapacityPlacementPolicy,
) capacityPlacementEvidence {
	fingerprint := capacityPlacementEvidence{
		low: 0xcbf29ce484222325, high: 0x6eed0e9da4d94a4f,
	}
	mixUint64 := func(value uint64) {
		fingerprint.low = mixCapacityPlacement(
			fingerprint.low, value, 0x100000001b3,
		)
		fingerprint.high = mixCapacityPlacement(
			fingerprint.high, value, 0x9e3779b185ebca87,
		)
	}
	mixString := func(value string) {
		mixUint64(uint64(len(value)))
		for index := 0; index < len(value); index++ {
			mixUint64(uint64(value[index]))
		}
	}
	mixPoint := func(point distribution.KeyspacePoint) {
		for index := range point {
			mixUint64(uint64(point[index]))
		}
	}
	mixSource := func(source autosplit.SourceIdentity) {
		mixString(string(source.Distribution))
		mixString(string(source.Shard))
		mixUint64(uint64(source.AllocationGeneration))
		mixPoint(source.Range.Start)
		mixPoint(source.Range.End.Point)
		mixUint64(boolPlacementValue(source.Range.End.Max))
		mixUint64(uint64(source.BucketBits))
		mixUint64(uint64(source.RoutingVersion))
		mixUint64(uint64(source.OwnershipEpoch))
	}

	mixUint64(decision.CatalogGeneration)
	mixUint64(uint64(decision.Count))
	for index := 0; index < int(decision.Count); index++ {
		ordinal := decision.Ordinals[index]
		mixUint64(uint64(ordinal))
		if int(ordinal) >= len(candidates) {
			continue
		}
		candidate := &candidates[ordinal]
		recommendation := candidate.Recommendation
		mixUint64(candidate.CatalogGeneration)
		mixUint64(candidate.MigrationBytes)
		mixSource(recommendation.Source)
		mixUint64(recommendation.WindowSequence)
		mixUint64(uint64(recommendation.Kind))
		mixUint64(uint64(recommendation.Reason))
		mixUint64(uint64(recommendation.BoundaryCount))
		for boundary := range recommendation.Boundaries {
			mixPoint(recommendation.Boundaries[boundary])
		}
		mixUint64(uint64(recommendation.CandidateBin))
		mixPoint(recommendation.HotBucketStart)
		mixUint64(recommendation.CurrentPressurePPM)
		mixUint64(recommendation.PredictedPressurePPM)
		mixUint64(recommendation.BenefitPPM)
		mixUint64(recommendation.FanoutTaxPPM)
		mixUint64(recommendation.MigrationTaxPPM)
	}
	mixUint64(uint64(len(reservations)))
	for index := range reservations {
		reservation := &reservations[index]
		mixUint64(uint64(reservation.RetainChild))
		mixUint64(uint64(reservation.DestinationCount))
		for destination := range reservation.Destinations {
			child := &reservation.Destinations[destination]
			mixString(string(child.Shard))
			mixUint64(uint64(child.AllocationGeneration))
			mixUint64(uint64(child.OwnershipEpoch))
			for resource := range autosplit.ResourceCount {
				mixUint64(child.Demand[resource])
			}
			mixUint64(child.MigrationBytes)
		}
	}
	mixUint64(uint64(len(nodes)))
	for index := range nodes {
		node := &nodes[index]
		mixUint64(node.CatalogGeneration)
		mixString(string(node.Endpoint))
		mixUint64(uint64(node.FailureDomain))
		mixUint64(uint64(node.Flags))
		for resource := range autosplit.ResourceCount {
			mixUint64(node.Capacity[resource])
			mixUint64(node.Used[resource])
		}
		mixUint64(node.MigrationCapacity)
		mixUint64(node.MigrationUsed)
		mixUint64(uint64(node.MaxReceives))
		mixUint64(uint64(node.ActiveReceives))
	}
	mixUint64(uint64(policy.ReplicaCount))
	mixUint64(policy.MaxProjectedPressurePPM)
	mixUint64(policy.MaxPhysicalMigrationBytes)
	mixUint64(uint64(policy.MaxNewReplicasPerNode))
	mixUint64(uint64(policy.MaxNewPrimariesPerNode))
	mixUint64(boolPlacementValue(policy.ExcludeSourceLeaders))
	mixUint64(boolPlacementValue(policy.DistinctFailureDomains))
	return fingerprint
}

func mixCapacityPlacement(hash, value, prime uint64) uint64 {
	hash ^= value
	return hash * prime
}

func boolPlacementValue(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func invalidCapacityPlacementInputs(
	catalog *gateway.Snapshot,
	candidates []SplitCandidate,
	decision Decision,
	reservations []SplitReservation,
	nodes []NodeCapacity,
	policy CapacityPlacementPolicy,
	workspace *CapacityPlacementWorkspace,
) bool {
	return catalog == nil || workspace == nil ||
		decision.CatalogGeneration != catalog.Generation() || decision.Count == 0 ||
		decision.Count > MaxBatch || len(candidates) > MaxCandidates ||
		len(reservations) != int(decision.Count) || len(nodes) == 0 ||
		len(nodes) > MaxPlacementNodes || policy.ReplicaCount == 0 ||
		policy.ReplicaCount > MaxPlacementReplicas ||
		policy.MaxProjectedPressurePPM == 0 ||
		policy.MaxPhysicalMigrationBytes == 0 ||
		policy.MaxNewReplicasPerNode == 0 || policy.MaxNewPrimariesPerNode == 0
}

func preparePlacementNodes(
	catalog *gateway.Snapshot,
	nodes []NodeCapacity,
	policy CapacityPlacementPolicy,
	workspace *CapacityPlacementWorkspace,
) bool {
	workspace.totalCapacity = autosplit.CapacityVector{}
	workspace.totalMigration = 0
	clear(workspace.nodeIndex[:])
	for index := range nodes {
		node := &nodes[index]
		workspace.reserved[index] = autosplit.CapacityVector{}
		workspace.migration[index] = 0
		workspace.receives[index] = 0
		workspace.replicas[index] = 0
		workspace.primaries[index] = 0
		if node.CatalogGeneration != catalog.Generation() || node.Endpoint == "" ||
			node.Flags&^NodePlacementReady != 0 ||
			(policy.DistinctFailureDomains && node.FailureDomain == 0) {
			return false
		}
		if _, err := catalog.Address(node.Endpoint); err != nil {
			return false
		}
		if !indexPlacementEndpoint(workspace, nodes, index) {
			return false
		}
		for resource := range autosplit.ResourceCount {
			if node.Capacity[resource] == 0 && node.Used[resource] != 0 {
				return false
			}
		}
		if node.MigrationCapacity == 0 && node.MigrationUsed != 0 {
			return false
		}
		if node.Flags&NodePlacementReady == 0 {
			continue
		}
		for resource := range autosplit.ResourceCount {
			workspace.totalCapacity[resource] = saturatingPlacementAdd(
				workspace.totalCapacity[resource], node.Capacity[resource],
			)
		}
		workspace.totalMigration = saturatingPlacementAdd(
			workspace.totalMigration, node.MigrationCapacity,
		)
	}
	return true
}

func indexPlacementEndpoint(
	workspace *CapacityPlacementWorkspace,
	nodes []NodeCapacity,
	node int,
) bool {
	endpoint := nodes[node].Endpoint
	mask := placementNodeIndexSlots - 1
	start := int(placementEndpointHash(endpoint)) & mask
	for probe := 0; probe < placementNodeIndexSlots; probe++ {
		position := (start + probe) & mask
		reference := workspace.nodeIndex[position]
		if reference == 0 {
			workspace.nodeIndex[position] = uint16(node + 1)
			return true
		}
		if nodes[reference-1].Endpoint == endpoint {
			return false
		}
	}
	return false
}

func placementEndpointHash(endpoint distribution.EndpointID) uint64 {
	hash := uint64(0xcbf29ce484222325)
	for index := 0; index < len(endpoint); index++ {
		hash ^= uint64(endpoint[index])
		hash *= 0x100000001b3
	}
	return hash
}

func preparePlacementWork(
	catalog *gateway.Snapshot,
	candidates []SplitCandidate,
	decision Decision,
	reservations []SplitReservation,
	workspace *CapacityPlacementWorkspace,
) (int, bool) {
	workCount := 0
	for decisionIndex := 0; decisionIndex < int(decision.Count); decisionIndex++ {
		ordinal := int(decision.Ordinals[decisionIndex])
		if ordinal >= len(candidates) {
			return 0, false
		}
		candidate := &candidates[ordinal]
		recommendation := candidate.Recommendation
		reservation := &reservations[decisionIndex]
		if candidate.CatalogGeneration != catalog.Generation() ||
			!recommendation.Actionable() || !exactSource(catalog, recommendation.Source) ||
			selectedOrdinalBefore(decision, decisionIndex, uint16(ordinal)) ||
			selectedSourceBefore(candidates, decision, decisionIndex, recommendation.Source) ||
			reservation.DestinationCount != recommendation.BoundaryCount ||
			reservation.DestinationCount == 0 ||
			reservation.RetainChild > reservation.DestinationCount {
			return 0, false
		}
		var migration uint64
		for destination := 0; destination < int(reservation.DestinationCount); destination++ {
			child := &reservation.Destinations[destination]
			if child.Shard == "" || child.AllocationGeneration == 0 ||
				child.OwnershipEpoch == 0 ||
				child.MigrationBytes > ^uint64(0)-migration {
				return 0, false
			}
			migration += child.MigrationBytes
			share := dominantPlacementShare(
				child.Demand, child.MigrationBytes,
				workspace.totalCapacity, workspace.totalMigration,
			)
			workspace.work[workCount] = placementWork{
				decisionIndex: uint8(decisionIndex), destinationIndex: uint8(destination),
				outputIndex: uint8(workCount), dominantSharePPM: share,
				migrationBytes: child.MigrationBytes,
			}
			workspace.workOrder[workCount] = uint8(workCount)
			workCount++
		}
		if migration != candidate.MigrationBytes {
			return 0, false
		}
	}
	slices.SortFunc(workspace.workOrder[:workCount], func(left, right uint8) int {
		a, b := workspace.work[left], workspace.work[right]
		if order := cmp.Compare(b.dominantSharePPM, a.dominantSharePPM); order != 0 {
			return order
		}
		if order := cmp.Compare(b.migrationBytes, a.migrationBytes); order != 0 {
			return order
		}
		if order := cmp.Compare(a.decisionIndex, b.decisionIndex); order != 0 {
			return order
		}
		return cmp.Compare(a.destinationIndex, b.destinationIndex)
	})
	return workCount, true
}

type placementScore struct {
	pressure uint64
	primary  uint16
	replicas uint16
	endpoint distribution.EndpointID
}

func choosePlacementNode(
	manifest *distribution.Manifest,
	sourceOrdinal int,
	nodes []NodeCapacity,
	policy CapacityPlacementPolicy,
	workspace *CapacityPlacementWorkspace,
	reservation *DestinationReservation,
	placed *placedDestination,
	replica int,
) (int, uint64, bool) {
	bestNode := -1
	best := placementScore{pressure: ^uint64(0)}
	for nodeIndex := range nodes {
		node := &nodes[nodeIndex]
		if node.Flags&NodePlacementReady == 0 ||
			workspace.replicas[nodeIndex] >= policy.MaxNewReplicasPerNode ||
			(replica == 0 && workspace.primaries[nodeIndex] >= policy.MaxNewPrimariesPerNode) ||
			uint32(node.ActiveReceives)+uint32(workspace.receives[nodeIndex]) >=
				uint32(node.MaxReceives) ||
			(policy.ExcludeSourceLeaders && manifestHasLeader(manifest, sourceOrdinal, node.Endpoint)) ||
			placedContainsEndpoint(placed, replica, nodes, node.Endpoint) ||
			(policy.DistinctFailureDomains &&
				placedContainsDomain(placed, replica, nodes, node.FailureDomain)) {
			continue
		}
		pressure, ok := projectedNodePressure(node, nodeIndex, reservation, workspace)
		if !ok || pressure > policy.MaxProjectedPressurePPM {
			continue
		}
		score := placementScore{
			pressure: pressure, primary: workspace.primaries[nodeIndex],
			replicas: workspace.replicas[nodeIndex], endpoint: node.Endpoint,
		}
		if replica == 0 {
			score.primary++
		}
		score.replicas++
		if bestNode < 0 || comparePlacementScore(score, best) < 0 {
			bestNode, best = nodeIndex, score
		}
	}
	return bestNode, best.pressure, bestNode >= 0
}

func projectedNodePressure(
	node *NodeCapacity,
	nodeIndex int,
	reservation *DestinationReservation,
	workspace *CapacityPlacementWorkspace,
) (uint64, bool) {
	pressure := uint64(0)
	for resource := range autosplit.ResourceCount {
		used, ok := checkedPlacementAdd(
			node.Used[resource], workspace.reserved[nodeIndex][resource],
			reservation.Demand[resource],
		)
		if !ok || (node.Capacity[resource] == 0 && used != 0) {
			return 0, false
		}
		if node.Capacity[resource] != 0 {
			pressure = max(pressure, placementRatioPPM(used, node.Capacity[resource]))
		}
	}
	migration, ok := checkedPlacementAdd(
		node.MigrationUsed, workspace.migration[nodeIndex], reservation.MigrationBytes,
	)
	if !ok || (node.MigrationCapacity == 0 && migration != 0) {
		return 0, false
	}
	if node.MigrationCapacity != 0 {
		pressure = max(pressure, placementRatioPPM(migration, node.MigrationCapacity))
	}
	return pressure, true
}

func reservePlacementNode(
	node int,
	primary bool,
	projected uint64,
	reservation *DestinationReservation,
	policy CapacityPlacementPolicy,
	workspace *CapacityPlacementWorkspace,
	cut *CapacityPlacementCut,
) bool {
	if node < 0 || projected > policy.MaxProjectedPressurePPM ||
		reservation.MigrationBytes >
			policy.MaxPhysicalMigrationBytes-cut.physicalMigrationBytes {
		return false
	}
	for resource := range autosplit.ResourceCount {
		workspace.reserved[node][resource] += reservation.Demand[resource]
	}
	workspace.migration[node] += reservation.MigrationBytes
	workspace.receives[node]++
	workspace.replicas[node]++
	if primary {
		workspace.primaries[node]++
	}
	cut.physicalMigrationBytes += reservation.MigrationBytes
	return true
}

func comparePlacementScore(left, right placementScore) int {
	if order := cmp.Compare(left.pressure, right.pressure); order != 0 {
		return order
	}
	if order := cmp.Compare(left.primary, right.primary); order != 0 {
		return order
	}
	if order := cmp.Compare(left.replicas, right.replicas); order != 0 {
		return order
	}
	return cmp.Compare(left.endpoint, right.endpoint)
}

func manifestHasLeader(
	manifest *distribution.Manifest,
	shard int,
	endpoint distribution.EndpointID,
) bool {
	metadata, ok := manifest.ShardMetadataAt(shard)
	if !ok {
		return false
	}
	for leader := 0; leader < metadata.LeaderCount; leader++ {
		candidate, _ := manifest.ShardLeaderAt(shard, leader)
		if candidate == endpoint {
			return true
		}
	}
	return false
}

func placedContainsEndpoint(
	placed *placedDestination,
	count int,
	nodes []NodeCapacity,
	endpoint distribution.EndpointID,
) bool {
	for index := 0; index < count; index++ {
		if nodes[placed.nodes[index]].Endpoint == endpoint {
			return true
		}
	}
	return false
}

func placedContainsDomain(
	placed *placedDestination,
	count int,
	nodes []NodeCapacity,
	domain uint32,
) bool {
	for index := 0; index < count; index++ {
		if nodes[placed.nodes[index]].FailureDomain == domain {
			return true
		}
	}
	return false
}

func dominantPlacementShare(
	demand autosplit.CapacityVector,
	migration uint64,
	total autosplit.CapacityVector,
	totalMigration uint64,
) uint64 {
	share := uint64(0)
	for resource := range autosplit.ResourceCount {
		if demand[resource] != 0 {
			share = max(share, placementRatioPPM(demand[resource], total[resource]))
		}
	}
	if migration != 0 {
		share = max(share, placementRatioPPM(migration, totalMigration))
	}
	return share
}

func placementRatioPPM(value, capacity uint64) uint64 {
	if capacity == 0 {
		if value == 0 {
			return 0
		}
		return ^uint64(0)
	}
	hi, low := bits.Mul64(value, autosplit.PPM)
	if hi >= capacity {
		return ^uint64(0)
	}
	quotient, _ := bits.Div64(hi, low, capacity)
	return quotient
}

func checkedPlacementAdd(a, b, c uint64) (uint64, bool) {
	if b > ^uint64(0)-a {
		return 0, false
	}
	sum := a + b
	if c > ^uint64(0)-sum {
		return 0, false
	}
	return sum + c, true
}

func saturatingPlacementAdd(left, right uint64) uint64 {
	if right > ^uint64(0)-left {
		return ^uint64(0)
	}
	return left + right
}
