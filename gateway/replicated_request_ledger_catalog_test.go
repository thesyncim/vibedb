package gateway

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	vibejson "github.com/thesyncim/vibejson"
)

func TestRequestLedgerTopologyCatalogRoundTripAndExactRouteBinding(t *testing.T) {
	snapshot, topology := testRequestLedgerCatalogSnapshot(t, 5)
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveSnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := loaded.DurableRequestLedgerTopology()
	if !ok || got.Generation != 5 || len(got.Ranges) != 1 {
		t.Fatalf("loaded topology = %+v,%v", got, ok)
	}
	want := topology.Ranges[0]
	value := got.Ranges[0]
	if value.Start != want.Start || value.End != want.End || value.Identity != want.Identity ||
		!sameReplicatedCatalogRoute(value.Route, want.Route) {
		t.Fatalf("loaded range = %+v, want %+v", value, want)
	}

	// Returned storage is detached from the immutable catalog.
	got.Ranges[0].Identity[0]++
	got.Ranges[0].Route.Replicas[0].Member++
	again, _ := loaded.DurableRequestLedgerTopology()
	if again.Ranges[0].Identity != want.Identity ||
		again.Ranges[0].Route.Replicas[0].Member != want.Route.Replicas[0].Member {
		t.Fatal("caller mutated catalog-owned request-ledger topology")
	}
}

func TestRequestLedgerTopologyCatalogRejectsMalformedCoverageAndAuthority(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	descriptor.RequestLedgerRanges = nil
	base, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Validate(); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("RF3 snapshot without request-ledger topology err=%v", err)
	}
	var workspace [ServingReplicaCount]ReplicatedEndpoint
	route, ok := base.ResolveReplicatedRoute(descriptor.Distribution, descriptor.Shard, workspace[:0])
	if !ok {
		t.Fatal("missing replicated route")
	}
	identity := replication.Digest{0x91}
	middle := requestledger.LedgerHome{0x80}
	valid := DurableRequestLedgerTopology{Generation: 5, Ranges: []DurableRequestLedgerRange{{
		Identity: identity, Route: route,
	}}}

	for name, mutate := range map[string]func(*DurableRequestLedgerTopology){
		"zero generation":  func(value *DurableRequestLedgerTopology) { value.Generation = 0 },
		"stale generation": func(value *DurableRequestLedgerTopology) { value.Generation = 4 },
		"zero identity": func(value *DurableRequestLedgerTopology) {
			value.Ranges[0].Identity = replication.Digest{}
		},
		"stale allocation": func(value *DurableRequestLedgerTopology) {
			value.Ranges[0].Route.AllocationGeneration++
		},
		"route authority mismatch": func(value *DurableRequestLedgerTopology) {
			value.Ranges[0].Route.LineageDigest[0]++
		},
		"does not start at zero": func(value *DurableRequestLedgerTopology) {
			value.Ranges[0].Start[0] = 1
		},
		"does not end at infinity": func(value *DurableRequestLedgerTopology) {
			value.Ranges[0].End = middle
		},
		"gap": func(value *DurableRequestLedgerTopology) {
			second := value.Ranges[0]
			value.Ranges[0].End = middle
			second.Start = requestledger.LedgerHome{0x81}
			second.Identity[0]++
			value.Ranges = append(value.Ranges, second)
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := DurableRequestLedgerTopology{
				Generation: valid.Generation,
				Ranges:     append([]DurableRequestLedgerRange(nil), valid.Ranges...),
			}
			candidate.Ranges[0].Route = cloneDurableRequestRoute(valid.Ranges[0].Route)
			mutate(&candidate)
			if _, err := NewSnapshotWithReplicatedRequestLedgerMetadata(
				config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor}, nil,
				candidate,
			); !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("err=%v, want ErrInvalidCatalog", err)
			}
		})
	}
}

func TestRequestLedgerTopologyPersistedFieldsFailClosed(t *testing.T) {
	snapshot, _ := testRequestLedgerCatalogSnapshot(t, 5)
	for name, mutate := range map[string]func(*persistedCatalog){
		"missing topology": func(value *persistedCatalog) {
			value.RequestLedger = nil
		},
		"zero topology generation": func(value *persistedCatalog) {
			value.RequestLedger.Generation = 0
		},
		"missing range identity": func(value *persistedCatalog) {
			value.RequestLedger.Ranges[0].Identity = ""
		},
		"zero lineage digest": func(value *persistedCatalog) {
			value.RequestLedger.Ranges[0].LineageDigest = strings.Repeat("0", 64)
		},
		"stale allocation": func(value *persistedCatalog) {
			value.RequestLedger.Ranges[0].AllocationGeneration++
		},
	} {
		t.Run(name, func(t *testing.T) {
			persisted := toPersisted(snapshot)
			mutate(&persisted)
			raw, err := vibejson.Marshal(&persisted)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeSnapshotBytes(raw); !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("decode err=%v, want ErrInvalidCatalog", err)
			}
		})
	}
}

func TestRequestLedgerTopologyAdvancesOnlyWithCatalogGenerationCAS(t *testing.T) {
	current, _ := testRequestLedgerCatalogSnapshot(t, 5)
	next, _ := testRequestLedgerCatalogSnapshot(t, 6)
	holder := NewCatalogHolder(current)
	if err := holder.PublishAfter(4, next); !errors.Is(err, ErrCatalogGenerationMismatch) {
		t.Fatalf("stale CAS err=%v", err)
	}
	if err := holder.PublishAfter(5, next); err != nil {
		t.Fatal(err)
	}
	got, ok := holder.Current().DurableRequestLedgerTopology()
	if !ok || got.Generation != 6 {
		t.Fatalf("published topology = %+v,%v", got, ok)
	}

	config, endpoints, descriptor, _ := testRequestLedgerCatalogInput(t, 7)
	descriptor.RequestLedgerRanges = nil
	removed, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 7, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.PublishAfter(6, removed); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("topology removal err=%v, want ErrInvalidCatalog", err)
	}
}

func testRequestLedgerCatalogSnapshot(
	t testing.TB, generation uint64,
) (*Snapshot, DurableRequestLedgerTopology) {
	t.Helper()
	config, endpoints, descriptor, route := testRequestLedgerCatalogInput(t, generation)
	topology := DurableRequestLedgerTopology{
		Generation: generation,
		Ranges: []DurableRequestLedgerRange{{
			Identity: replication.Digest{0x91}, Route: route,
		}},
	}
	snapshot, err := NewSnapshotWithReplicatedRequestLedgerMetadata(
		config, endpoints, generation, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, nil, topology,
	)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, topology
}

func testRequestLedgerCatalogInput(
	t testing.TB, generation uint64,
) (distribution.ClusterConfig, map[distribution.EndpointID]string, ReplicatedShardDescriptor, ReplicatedRoute) {
	t.Helper()
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	base, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, generation, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	var workspace [ServingReplicaCount]ReplicatedEndpoint
	route, ok := base.ResolveReplicatedRoute(
		descriptor.Distribution, descriptor.Shard, workspace[:0],
	)
	if !ok {
		t.Fatal("missing replicated route")
	}
	return config, endpoints, descriptor, route
}
