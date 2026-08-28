package hotshard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/topologyscheduler"
)

// CapacityProvider supplies like-unit capacity and migration evidence for one
// detached recorder window. It is advisory; catalog and source identity are
// rechecked again by Controller before any admission.
type CapacityProvider interface {
	PressureCapacity(autosplit.Pulse) (autosplit.CapacitySet, autosplit.CapacityVector, uint64, bool)
}

type PressurePublisher interface {
	ReadPressureRecord(context.Context) (gateway.ReplicatedPressureRecord, error)
	PublishPressureRecord(context.Context, uint64, gateway.ReplicatedPressureRecord) error
	RetryPending(context.Context) error
}

type collectorEntry struct {
	group    raftmember.GroupKey
	source   autosplit.SourceIdentity
	leader   distribution.EndpointID
	recorder *autosplit.Recorder
}

// Collector is the bounded gateway intake for routed read/write pressure. One
// striped recorder is retained per physical RF3 allocation in the catalog cut;
// neither requests nor tenants can increase its cardinality.
type Collector struct {
	mu       sync.Mutex
	provider CapacityProvider
	policy   autosplit.Policy
	entries  []collectorEntry
	bySource map[autosplit.SourceIdentity]int

	catalogGeneration uint64
	sequence          uint64
	pending           *gateway.ReplicatedPressureRecord
	poisoned          bool
}

func NewCollector(
	catalog *gateway.Snapshot, firstSequence uint64, lanes int,
	provider CapacityProvider, policy autosplit.Policy,
) (*Collector, error) {
	if catalog == nil || firstSequence == 0 || provider == nil ||
		lanes < 0 || lanes > autosplit.MaxRecorderLanes {
		return nil, ErrInvalidPressureCut
	}
	descriptors := catalog.ReplicatedShardDescriptors()
	if len(descriptors) == 0 || len(descriptors) > MaxReports {
		return nil, ErrInvalidPressureCut
	}
	collector := &Collector{provider: provider, policy: policy, sequence: firstSequence,
		catalogGeneration: catalog.Generation(),
		entries:           make([]collectorEntry, 0, len(descriptors)),
		bySource:          make(map[autosplit.SourceIdentity]int, len(descriptors))}
	for _, descriptor := range descriptors {
		source, leader, ok := sourceForDescriptor(catalog, descriptor)
		if !ok {
			return nil, ErrInvalidPressureCut
		}
		recorder, err := autosplit.NewRecorder(source, firstSequence, lanes)
		if err != nil {
			return nil, errors.Join(err, ErrInvalidPressureCut)
		}
		collector.entries = append(collector.entries, collectorEntry{
			group: descriptor.Group, source: source, leader: leader, recorder: recorder,
		})
	}
	slices.SortFunc(collector.entries, func(left, right collectorEntry) int {
		return compareGroup(left.group, right.group)
	})
	for index := range collector.entries {
		entry := &collector.entries[index]
		if index != 0 && compareGroup(collector.entries[index-1].group, entry.group) >= 0 {
			return nil, ErrInvalidPressureCut
		}
		if _, duplicate := collector.bySource[entry.source]; duplicate {
			return nil, ErrInvalidPressureCut
		}
		collector.bySource[entry.source] = index
	}
	return collector, nil
}

func sourceForDescriptor(
	catalog *gateway.Snapshot, descriptor gateway.ReplicatedShardDescriptor,
) (autosplit.SourceIdentity, distribution.EndpointID, bool) {
	manifest, ok := catalog.Manifest(descriptor.Distribution)
	if !ok || descriptor.Group == (raftmember.GroupKey{}) {
		return autosplit.SourceIdentity{}, "", false
	}
	var bucketBits uint8
	for ordinal := 0; ordinal < catalog.DistributionCount(); ordinal++ {
		spec, found := catalog.DistributionAt(ordinal)
		if found && spec.Name == descriptor.Distribution {
			bucketBits = spec.EffectiveBucketBits()
			break
		}
	}
	for ordinal := 0; ordinal < manifest.ShardCount(); ordinal++ {
		metadata, found := manifest.ShardMetadataAt(ordinal)
		if found && metadata.ID == descriptor.Shard &&
			metadata.AllocationGeneration == descriptor.AllocationGeneration {
			leader, leaderOK := manifest.ShardLeaderAt(ordinal, 0)
			if !leaderOK {
				return autosplit.SourceIdentity{}, "", false
			}
			return autosplit.SourceIdentity{Distribution: descriptor.Distribution,
				Shard: descriptor.Shard, AllocationGeneration: descriptor.AllocationGeneration,
				Range: metadata.Range, BucketBits: bucketBits,
				RoutingVersion: manifest.Version(), OwnershipEpoch: metadata.Epoch}, leader, bucketBits != 0
		}
	}
	return autosplit.SourceIdentity{}, "", false
}

// ObservePressure implements gateway.PressureObserver. Exact single-bucket
// scopes retain locality; wider or absent scopes contribute exact total demand
// plus conservative fanout without inventing key placement.
func (collector *Collector) ObservePressure(observation gateway.PressureObservation) {
	if collector == nil {
		return
	}
	index, found := collector.bySource[observation.Source]
	if !found {
		return
	}
	load := autosplit.LoadVector{autosplit.ResourceRequests: 1}
	if observation.Write {
		load[autosplit.ResourceWriteCPU] = 1
	} else {
		load[autosplit.ResourceReadCPU] = 1
	}
	recorder := collector.entries[index].recorder
	if observation.HasPoint {
		// Locality comes from the same native mapper and pinned catalog that
		// selected the RF3 owner, never from SQL text or a guessed primary key.
		bucket, valid := distribution.VirtualBucketForPoint(observation.Point, observation.Source.BucketBits)
		if valid && len(observation.AccessScopes) == 0 {
			interval, ok := distribution.VirtualBucketRange(bucket, observation.Source.BucketBits)
			if ok && recorder.ObserveBucket(interval.Start, load, 1, 1) {
				return
			}
		}
		recorder.ObserveUnknown(load, 1)
		return
	}
	if len(observation.AccessScopes) == 1 &&
		observation.AccessScopes[0].End == observation.AccessScopes[0].Start+1 {
		bucket, ok := distribution.VirtualBucketRange(
			distribution.VirtualBucket(observation.AccessScopes[0].Start), observation.Source.BucketBits,
		)
		if ok && recorder.ObserveBucket(bucket.Start, load, 1, 1) {
			return
		}
	}
	if !distributedtxn.ValidateIntentScopes(observation.AccessScopes, observation.Source.BucketBits) {
		recorder.ObserveUnknown(load, 1)
		return
	}
	recorder.ObserveUniform(load)
	for _, scope := range observation.AccessScopes {
		startRange, startOK := distribution.VirtualBucketRange(
			distribution.VirtualBucket(scope.Start), observation.Source.BucketBits,
		)
		if !startOK {
			continue
		}
		var end distribution.KeyspaceEnd
		if scope.End == distribution.VirtualBucketCount(observation.Source.BucketBits) {
			end.Max = true
		} else if endRange, ok := distribution.VirtualBucketRange(
			distribution.VirtualBucket(scope.End), observation.Source.BucketBits,
		); ok {
			end.Point = endRange.Start
		} else {
			continue
		}
		recorder.ObserveSpan(startRange.Start, end, 1)
	}
}

// Publish rotates every recorder into one canonical view and CAS-publishes it
// in catalog RF3. An outcome-unknown publish is settled through the native
// session and exact record read; the detached payload is retained until then.
func (collector *Collector) Publish(
	ctx context.Context, publisher PressurePublisher,
	nodes []topologyscheduler.NodeCapacity,
) (gateway.ReplicatedPressureRecord, error) {
	if collector == nil || ctx == nil || publisher == nil ||
		len(nodes) == 0 || len(nodes) > topologyscheduler.MaxPlacementNodes ||
		capacityNodesGeneration(nodes) != collector.catalogGeneration {
		return gateway.ReplicatedPressureRecord{}, ErrInvalidPressureCut
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.poisoned {
		return gateway.ReplicatedPressureRecord{}, ErrInvalidPressureCut
	}
	if collector.pending == nil {
		view, err := collector.rotate(nodes)
		if err != nil {
			collector.poisoned = true
			return gateway.ReplicatedPressureRecord{}, err
		}
		raw, err := AppendView(nil, view)
		if err != nil {
			return gateway.ReplicatedPressureRecord{}, err
		}
		record := gateway.ReplicatedPressureRecord{CatalogGeneration: view.CatalogGeneration,
			AuthorityRevision: view.AuthorityRevision, PayloadDigest: sha256.Sum256(raw), Payload: raw}
		collector.pending = &record
	}
	record := *collector.pending
	expected := record.AuthorityRevision - 1
	err := publisher.PublishPressureRecord(ctx, expected, record)
	if errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		if retryErr := publisher.RetryPending(ctx); retryErr != nil {
			return record, retryErr
		}
		settled, readErr := publisher.ReadPressureRecord(ctx)
		if readErr != nil || !equalPressureRecord(settled, record) {
			return record, errors.Join(readErr, ErrInvalidPressureCut)
		}
		err = nil
	}
	if err == nil {
		collector.pending = nil
	}
	return record, err
}

func (collector *Collector) rotate(nodes []topologyscheduler.NodeCapacity) (View, error) {
	if collector.sequence == ^uint64(0) {
		return View{}, ErrInvalidPressureCut
	}
	view := View{CatalogGeneration: collector.catalogGeneration,
		AuthorityRevision: collector.sequence,
		Reports:           make([]Report, len(collector.entries)),
		Nodes:             slices.Clone(nodes)}
	for index := range collector.entries {
		entry := &collector.entries[index]
		window, err := entry.recorder.Rotate(collector.sequence + 1)
		if err != nil {
			return View{}, errors.Join(err, ErrInvalidPressureCut)
		}
		pulse := window.Pulse()
		capacities, demand, migration, ok := collector.provider.PressureCapacity(pulse)
		if !ok || capacities.Source != entry.source ||
			capacities.WindowSequence != collector.sequence {
			return View{}, ErrInvalidPressureCut
		}
		view.Reports[index] = Report{Group: entry.group,
			Recommendation: autosplit.Recommend(window, capacities, collector.policy),
			Demand:         demand, MigrationBytes: migration}
		if !addNodeDemand(view.Nodes, entry.leader, demand) {
			return View{}, ErrInvalidPressureCut
		}
	}
	collector.sequence++
	return view, nil
}

func addNodeDemand(
	nodes []topologyscheduler.NodeCapacity,
	endpoint distribution.EndpointID,
	demand autosplit.CapacityVector,
) bool {
	for index := range nodes {
		if nodes[index].Endpoint != endpoint {
			continue
		}
		for resource := range autosplit.ResourceCount {
			if demand[resource] > ^uint64(0)-nodes[index].Used[resource] {
				nodes[index].Used[resource] = ^uint64(0)
			} else {
				nodes[index].Used[resource] += demand[resource]
			}
		}
		return true
	}
	return false
}

// Node evidence and sources are both fenced to one catalog generation. Every
// node report must carry the same generation; no wall-clock freshness exists.
func capacityNodesGeneration(nodes []topologyscheduler.NodeCapacity) uint64 {
	if len(nodes) == 0 || nodes[0].CatalogGeneration == 0 {
		return 0
	}
	generation := nodes[0].CatalogGeneration
	for index := 1; index < len(nodes); index++ {
		if nodes[index].CatalogGeneration != generation {
			return 0
		}
	}
	return generation
}

func equalPressureRecord(left, right gateway.ReplicatedPressureRecord) bool {
	return left.CatalogGeneration == right.CatalogGeneration &&
		left.AuthorityRevision == right.AuthorityRevision &&
		left.PayloadDigest == right.PayloadDigest && bytes.Equal(left.Payload, right.Payload)
}
