package gateway

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	queryplanner "github.com/thesyncim/vibedb/planner"
)

// persistedDurableRequestLedgerTopology is part of the canonical catalog
// document. A range persists the exact logical route authority in addition to
// its route locator, so decode cannot silently bind a durable request home to a
// different allocation or lineage present under the same shard name.
type persistedDurableRequestLedgerTopology struct {
	Generation uint64                               `json:"generation"`
	Ranges     []persistedDurableRequestLedgerRange `json:"ranges"`
}

type persistedDurableRequestLedgerRange struct {
	Start                string `json:"start"`
	End                  string `json:"end,omitempty"`
	Identity             string `json:"identity"`
	Distribution         string `json:"distribution"`
	Shard                string `json:"shard"`
	AllocationGeneration uint64 `json:"allocation_generation"`
	RangeIdentity        string `json:"range_identity"`
	LineageDigest        string `json:"lineage_digest"`
	ForwardingRuleDigest string `json:"forwarding_rule_digest"`
}

// DurableRequestLedgerRangeDescriptor is the provisioned logical range
// identity carried by one ReplicatedShardDescriptor. It deliberately contains
// no route fields: the containing shard descriptor is the sole exact route.
type DurableRequestLedgerRangeDescriptor struct {
	Start    requestledger.LedgerHome
	End      requestledger.LedgerHome
	Identity replication.Digest
}

func (snapshot *Snapshot) attachDurableRequestLedgerRangesFromDescriptors(
	descriptors []ReplicatedShardDescriptor,
) error {
	count := 0
	for index := range descriptors {
		count += len(descriptors[index].RequestLedgerRanges)
	}
	if count == 0 {
		return nil
	}
	topology := DurableRequestLedgerTopology{
		Generation: snapshot.generation,
		Ranges:     make([]DurableRequestLedgerRange, 0, count),
	}
	var workspace [ServingReplicaCount]ReplicatedEndpoint
	for index := range descriptors {
		descriptor := descriptors[index]
		route, ok := snapshot.ResolveReplicatedRoute(
			descriptor.Distribution, descriptor.Shard, workspace[:0],
		)
		if !ok {
			return &CatalogError{Reason: "request-ledger range references an unresolved shard"}
		}
		for _, value := range descriptor.RequestLedgerRanges {
			topology.Ranges = append(topology.Ranges, DurableRequestLedgerRange{
				Start: value.Start, End: value.End, Identity: value.Identity,
				Route: cloneDurableRequestRoute(route),
			})
		}
	}
	slices.SortFunc(topology.Ranges, func(left, right DurableRequestLedgerRange) int {
		return bytes.Compare(left.Start[:], right.Start[:])
	})
	return snapshot.attachDurableRequestLedgerTopology(topology)
}

// NewSnapshotWithReplicatedRequestLedgerMetadata constructs one immutable
// catalog generation with both RF3 serving coordinates and the authenticated
// request-ledger range directory. The topology generation is the catalog
// generation: it cannot be published or recovered independently of the routes
// which authenticate it.
func NewSnapshotWithReplicatedRequestLedgerMetadata(
	config distribution.ClusterConfig,
	endpoints map[distribution.EndpointID]string,
	generation uint64,
	indexes []IndexDescriptor,
	statistics []queryplanner.TableStatistics,
	replicated []ReplicatedShardDescriptor,
	tables []ReplicatedTableProfile,
	topology DurableRequestLedgerTopology,
) (*Snapshot, error) {
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, generation, indexes, statistics, replicated, tables,
	)
	if err != nil {
		return nil, err
	}
	if err := snapshot.attachDurableRequestLedgerTopology(topology); err != nil {
		return nil, err
	}
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// DurableRequestLedgerTopology returns a defensive cold-path copy. Hot request
// routing uses DurableRequestLedgerTopologyHolder after startup publication.
func (snapshot *Snapshot) DurableRequestLedgerTopology() (*DurableRequestLedgerTopology, bool) {
	if snapshot == nil || snapshot.durableRequestLedgerTopology == nil {
		return nil, false
	}
	return cloneDurableRequestLedgerTopology(snapshot.durableRequestLedgerTopology), true
}

func (snapshot *Snapshot) attachDurableRequestLedgerTopology(
	topology DurableRequestLedgerTopology,
) error {
	if snapshot == nil || topology.Generation == 0 ||
		topology.Generation != snapshot.generation || len(topology.Ranges) == 0 {
		return &CatalogError{Reason: "request-ledger topology generation is missing or stale"}
	}
	ranges := make([]DurableRequestLedgerRange, len(topology.Ranges))
	identities := make(map[replication.Digest]struct{}, len(topology.Ranges))
	var workspace [ServingReplicaCount]ReplicatedEndpoint
	for index := range topology.Ranges {
		value := topology.Ranges[index]
		resolved, ok := snapshot.ResolveReplicatedRoute(
			value.Route.Distribution, value.Route.Shard, workspace[:0],
		)
		if !ok || value.Identity == (replication.Digest{}) ||
			!sameReplicatedCatalogRoute(value.Route, resolved) ||
			value.Route.RangeIdentity == (replication.Digest{}) ||
			value.Route.LineageDigest == (replication.Digest{}) ||
			value.Route.ForwardingRuleDigest == (replication.Digest{}) {
			return &CatalogError{Reason: "request-ledger range has invalid route authority"}
		}
		if _, duplicate := identities[value.Identity]; duplicate {
			return &CatalogError{Reason: "request-ledger range identity is duplicated"}
		}
		identities[value.Identity] = struct{}{}
		if index == 0 && value.Start != (requestledger.LedgerHome{}) {
			return &CatalogError{Reason: "request-ledger topology does not start at zero"}
		}
		if index != 0 && topology.Ranges[index-1].End != value.Start {
			return &CatalogError{Reason: "request-ledger topology has a gap, overlap, or noncanonical order"}
		}
		last := index+1 == len(topology.Ranges)
		if last {
			if value.End != (requestledger.LedgerHome{}) {
				return &CatalogError{Reason: "request-ledger topology does not end at infinity"}
			}
		} else if value.End == (requestledger.LedgerHome{}) ||
			bytes.Compare(value.Start[:], value.End[:]) >= 0 {
			return &CatalogError{Reason: "request-ledger range is empty or unbounded before the final range"}
		}
		ranges[index] = DurableRequestLedgerRange{
			Start: value.Start, End: value.End, Identity: value.Identity,
			Route: cloneDurableRequestRoute(resolved),
		}
	}
	snapshot.durableRequestLedgerTopology = &DurableRequestLedgerTopology{
		Generation: topology.Generation, Ranges: ranges,
	}
	return nil
}

func cloneDurableRequestLedgerTopology(
	topology *DurableRequestLedgerTopology,
) *DurableRequestLedgerTopology {
	if topology == nil {
		return nil
	}
	cloned := &DurableRequestLedgerTopology{
		Generation: topology.Generation,
		Ranges:     make([]DurableRequestLedgerRange, len(topology.Ranges)),
	}
	for index := range topology.Ranges {
		cloned.Ranges[index] = topology.Ranges[index]
		cloned.Ranges[index].Route = cloneDurableRequestRoute(topology.Ranges[index].Route)
	}
	return cloned
}

func persistedDurableRequestLedgerTopologyFromSnapshot(
	snapshot *Snapshot,
) *persistedDurableRequestLedgerTopology {
	if snapshot == nil || snapshot.durableRequestLedgerTopology == nil {
		return nil
	}
	topology := snapshot.durableRequestLedgerTopology
	persisted := &persistedDurableRequestLedgerTopology{
		Generation: topology.Generation,
		Ranges:     make([]persistedDurableRequestLedgerRange, len(topology.Ranges)),
	}
	for index, value := range topology.Ranges {
		entry := persistedDurableRequestLedgerRange{
			Start:        hex.EncodeToString(value.Start[:]),
			Identity:     hex.EncodeToString(value.Identity[:]),
			Distribution: string(value.Route.Distribution), Shard: string(value.Route.Shard),
			AllocationGeneration: value.Route.AllocationGeneration,
			RangeIdentity:        hex.EncodeToString(value.Route.RangeIdentity[:]),
			LineageDigest:        hex.EncodeToString(value.Route.LineageDigest[:]),
			ForwardingRuleDigest: hex.EncodeToString(value.Route.ForwardingRuleDigest[:]),
		}
		if value.End != (requestledger.LedgerHome{}) {
			entry.End = hex.EncodeToString(value.End[:])
		}
		persisted.Ranges[index] = entry
	}
	return persisted
}

func (snapshot *Snapshot) attachPersistedDurableRequestLedgerTopology(
	persisted *persistedDurableRequestLedgerTopology,
) error {
	if persisted == nil {
		return nil
	}
	if persisted.Generation == 0 || len(persisted.Ranges) == 0 {
		return &CatalogError{Reason: "request-ledger topology is empty"}
	}
	topology := DurableRequestLedgerTopology{
		Generation: persisted.Generation,
		Ranges:     make([]DurableRequestLedgerRange, len(persisted.Ranges)),
	}
	var workspace [ServingReplicaCount]ReplicatedEndpoint
	for index, entry := range persisted.Ranges {
		value := &topology.Ranges[index]
		if err := decodeLedgerHomeHex(entry.Start, &value.Start); err != nil {
			return &CatalogError{Reason: "request-ledger range start: " + err.Error()}
		}
		if entry.End != "" {
			if err := decodeLedgerHomeHex(entry.End, &value.End); err != nil {
				return &CatalogError{Reason: "request-ledger range end: " + err.Error()}
			}
		}
		if err := decodeDigestHex(entry.Identity, &value.Identity); err != nil {
			return &CatalogError{Reason: "request-ledger range identity: " + err.Error()}
		}
		resolved, ok := snapshot.ResolveReplicatedRoute(
			distribution.DistributionName(entry.Distribution),
			distribution.ShardID(entry.Shard), workspace[:0],
		)
		if !ok || entry.AllocationGeneration == 0 ||
			resolved.AllocationGeneration != entry.AllocationGeneration {
			return &CatalogError{Reason: "request-ledger range route is absent or stale"}
		}
		var rangeIdentity, lineageDigest, forwardingDigest replication.Digest
		for _, field := range []struct {
			name        string
			raw         string
			destination *replication.Digest
		}{
			{"range identity", entry.RangeIdentity, &rangeIdentity},
			{"lineage digest", entry.LineageDigest, &lineageDigest},
			{"forwarding rule digest", entry.ForwardingRuleDigest, &forwardingDigest},
		} {
			if err := decodeDigestHex(field.raw, field.destination); err != nil {
				return &CatalogError{Reason: "request-ledger route " + field.name + ": " + err.Error()}
			}
		}
		if resolved.RangeIdentity != rangeIdentity || resolved.LineageDigest != lineageDigest ||
			resolved.ForwardingRuleDigest != forwardingDigest {
			return &CatalogError{Reason: "request-ledger range route authority does not match its shard"}
		}
		value.Route = cloneDurableRequestRoute(resolved)
	}
	return snapshot.attachDurableRequestLedgerTopology(topology)
}

func decodeLedgerHomeHex(raw string, destination *requestledger.LedgerHome) error {
	if destination == nil || len(raw) != hex.EncodedLen(len(destination)) {
		return fmt.Errorf("invalid fixed-width home")
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != len(destination) {
		return fmt.Errorf("invalid fixed-width home")
	}
	copy(destination[:], decoded)
	return nil
}

func decodeDigestHex(raw string, destination *replication.Digest) error {
	if destination == nil || len(raw) != hex.EncodedLen(len(destination)) {
		return fmt.Errorf("invalid fixed-width digest")
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != len(destination) {
		return fmt.Errorf("invalid fixed-width digest")
	}
	copy(destination[:], decoded)
	if *destination == (replication.Digest{}) {
		return fmt.Errorf("zero digest")
	}
	return nil
}

func validateDurableRequestLedgerCatalogTransition(current, next *Snapshot) error {
	if next == nil {
		return &CatalogError{Reason: "next request-ledger catalog is nil"}
	}
	oldTopology, newTopology := (*DurableRequestLedgerTopology)(nil), next.durableRequestLedgerTopology
	if current != nil {
		oldTopology = current.durableRequestLedgerTopology
	}
	if oldTopology == nil {
		return nil
	}
	if newTopology == nil {
		return &CatalogError{Reason: "request-ledger topology was removed"}
	}
	if newTopology.Generation != next.generation || newTopology.Generation <= oldTopology.Generation ||
		len(newTopology.Ranges) != len(oldTopology.Ranges) {
		return &CatalogError{Reason: "request-ledger topology did not advance with the catalog CAS"}
	}
	for index := range oldTopology.Ranges {
		before, after := oldTopology.Ranges[index], newTopology.Ranges[index]
		if before.Start != after.Start || before.End != after.End || before.Identity != after.Identity ||
			before.Route.RangeIdentity != after.Route.RangeIdentity ||
			before.Route.LineageDigest != after.Route.LineageDigest ||
			before.Route.ForwardingRuleDigest != after.Route.ForwardingRuleDigest {
			return &CatalogError{Reason: "request-ledger range authority changed without a forwarding protocol"}
		}
	}
	return nil
}

func validateDurableRequestLedgerCatalogPresence(snapshot *Snapshot) error {
	if snapshot == nil {
		return nil
	}
	if len(snapshot.replicatedShards) == 0 {
		if snapshot.durableRequestLedgerTopology != nil {
			return &CatalogError{Reason: "request-ledger topology has no replicated shard directory"}
		}
		return nil
	}
	if snapshot.durableRequestLedgerTopology == nil ||
		len(snapshot.durableRequestLedgerTopology.Ranges) == 0 ||
		snapshot.durableRequestLedgerTopology.Generation != snapshot.generation {
		return &CatalogError{Reason: "RF3 catalog is missing its request-ledger topology"}
	}
	return nil
}
