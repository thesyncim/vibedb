package hotshard

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/topologyscheduler"
)

func TestCollectorNativePointPreservesExactBucketLocality(t *testing.T) {
	_, source, nodes := hotCatalog(t)
	interval, ok := distribution.VirtualBucketRange(17, source.BucketBits)
	if !ok {
		t.Fatal("invalid test bucket")
	}
	point := interval.Start
	point[len(point)-1] = 1 // a point inside the bucket, not its start boundary
	var payloads [2][]byte
	for kind := range payloads {
		recorder, err := autosplit.NewRecorder(source, 1, 4)
		if err != nil {
			t.Fatal(err)
		}
		collector := &Collector{provider: pulseCapacity{}, sequence: 1, catalogGeneration: 10,
			policy:   autosplit.Policy{TriggerPressurePPM: 1, MaxChildPressurePPM: 900_000, CelebritySharePPM: 1},
			entries:  []collectorEntry{{group: hotGroup(1), source: source, leader: "source", recorder: recorder}},
			bySource: map[autosplit.SourceIdentity]int{source: 0}}
		observation := gateway.PressureObservation{Source: source,
			AccessScopes: []distributedtxn.IntentScope{{Start: 17, End: 18}}}
		if kind == 1 {
			observation.AccessScopes = nil
			observation.Point, observation.HasPoint = point, true
		}
		for range 16 {
			collector.ObservePressure(observation)
		}
		record, err := collector.Publish(t.Context(), &pressurePublisher{}, nodes)
		if err != nil {
			t.Fatal(err)
		}
		payloads[kind] = record.Payload
	}
	if !bytes.Equal(payloads[0], payloads[1]) {
		t.Fatalf("native point lost bucket locality:\nscoped=%s\nnative=%s", payloads[0], payloads[1])
	}
}

type pulseCapacity struct{}

func (pulseCapacity) PressureCapacity(pulse autosplit.Pulse) (
	autosplit.CapacitySet, autosplit.CapacityVector, uint64, bool,
) {
	set := autosplit.CapacitySet{Source: pulse.Source, WindowSequence: pulse.Sequence}
	for resource := range autosplit.ResourceCount {
		if pulse.Total[resource] != 0 {
			set.Current[resource], set.Left[resource], set.Right[resource], set.Isolated[resource] = 1, 2, 2, 2
		}
	}
	return set, autosplit.CapacityVector{autosplit.ResourceRequests: pulse.Total[autosplit.ResourceRequests]}, 64, true
}

type pressurePublisher struct {
	record      gateway.ReplicatedPressureRecord
	publish     int
	unknownOnce bool
	pending     bool
}

func (publisher *pressurePublisher) ReadPressureRecord(context.Context) (gateway.ReplicatedPressureRecord, error) {
	if publisher.record.AuthorityRevision == 0 {
		return gateway.ReplicatedPressureRecord{}, gateway.ErrReplicatedPressureMissing
	}
	return publisher.record, nil
}
func (publisher *pressurePublisher) PublishPressureRecord(_ context.Context, expected uint64, record gateway.ReplicatedPressureRecord) error {
	publisher.publish++
	if record.AuthorityRevision != expected+1 {
		return gateway.ErrReplicatedCatalogConflict
	}
	publisher.record = record
	if publisher.unknownOnce {
		publisher.unknownOnce = false
		publisher.pending = true
		return gateway.ErrReplicatedCatalogPending
	}
	return nil
}
func (publisher *pressurePublisher) RetryPending(context.Context) error {
	if !publisher.pending {
		return errors.New("nothing pending")
	}
	publisher.pending = false
	return nil
}

func TestCollectorIntakesExactGatewayBucketsAndPublishesReplicatedView(t *testing.T) {
	_, source, nodes := hotCatalog(t)
	recorder, err := autosplit.NewRecorder(source, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	collector := &Collector{provider: pulseCapacity{}, sequence: 1, catalogGeneration: 10,
		policy:   autosplit.Policy{TriggerPressurePPM: 1, MaxChildPressurePPM: 900_000, CelebritySharePPM: 1},
		entries:  []collectorEntry{{group: hotGroup(1), source: source, leader: "source", recorder: recorder}},
		bySource: map[autosplit.SourceIdentity]int{source: 0}}
	for range 16 {
		collector.ObservePressure(gateway.PressureObservation{Source: source, Write: true,
			AccessScopes: []distributedtxn.IntentScope{{Start: 17, End: 18}}})
	}
	publisher := &pressurePublisher{unknownOnce: true}
	record, err := collector.Publish(context.Background(), publisher, nodes)
	if err != nil || publisher.publish != 1 || record.AuthorityRevision != 1 {
		t.Fatalf("publish record=%+v calls=%d err=%v", record, publisher.publish, err)
	}
	view, err := OpenView(record.Payload)
	if err != nil || len(view.Reports) != 1 || view.Reports[0].Recommendation.WindowSequence != 1 ||
		view.Reports[0].Demand[autosplit.ResourceRequests] != 16 ||
		view.Nodes[0].Used[autosplit.ResourceRequests] != 16 {
		t.Fatalf("view=%+v err=%v", view, err)
	}
	want, _ := distribution.VirtualBucketRange(17, source.BucketBits)
	if view.Reports[0].Recommendation.HotBucketStart != want.Start {
		t.Fatalf("hot bucket=%x want=%x", view.Reports[0].Recommendation.HotBucketStart, want.Start)
	}
}

func TestCollectorRejectsNodeGenerationBeforeRotatingWindow(t *testing.T) {
	_, source, nodes := hotCatalog(t)
	recorder, _ := autosplit.NewRecorder(source, 1, 1)
	collector := &Collector{provider: pulseCapacity{}, sequence: 1, catalogGeneration: 10,
		entries:  []collectorEntry{{group: hotGroup(1), source: source, leader: "source", recorder: recorder}},
		bySource: map[autosplit.SourceIdentity]int{source: 0}}
	nodes[0].CatalogGeneration++
	if _, err := collector.Publish(context.Background(), &pressurePublisher{}, nodes); !errors.Is(err, ErrInvalidPressureCut) {
		t.Fatalf("stale nodes=%v", err)
	}
	if _, err := recorder.Rotate(2); err != nil {
		t.Fatalf("invalid publish consumed recorder window: %v", err)
	}
}

func TestCapacityNodesGenerationRequiresOneExactCut(t *testing.T) {
	nodes := []topologyscheduler.NodeCapacity{{CatalogGeneration: 7}, {CatalogGeneration: 7}}
	if got := capacityNodesGeneration(nodes); got != 7 {
		t.Fatalf("generation=%d", got)
	}
	nodes[1].CatalogGeneration = 8
	if got := capacityNodesGeneration(nodes); got != 0 {
		t.Fatalf("mixed generation=%d", got)
	}
}
