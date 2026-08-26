package gateway

import (
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestCatalogRequestLedgerTopologyRefreshesEndpointAndAllocatesNothingSteadyState(t *testing.T) {
	initial, _ := testRequestLedgerCatalogSnapshot(t, 5)
	catalog := NewCatalogHolder(initial)
	holder, err := NewCatalogDurableRequestLedgerTopologyHolder(catalog)
	if err != nil {
		t.Fatal(err)
	}
	point := requestledger.LedgerHome{0x44}
	before, _, ok := holder.Lookup(point)
	if !ok {
		t.Fatal("initial request-ledger home is absent")
	}

	config, endpoints, descriptor, _ := testRequestLedgerCatalogInput(t, 6)
	endpoints[descriptor.Replicas[0].NativeEndpoint] = "127.0.0.1:8199"
	next, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	installCatalogRefreshTestSnapshot(t, catalog, next)
	if err := holder.RefreshFromCatalog(); err != nil {
		t.Fatal(err)
	}
	after, generation, ok := holder.Lookup(point)
	if !ok || generation != 6 || after.Identity != before.Identity ||
		after.borrowedRoute().Replicas[0].Address != "127.0.0.1:8199" {
		t.Fatalf("refreshed home=%+v generation=%d ok=%v", after, generation, ok)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := holder.RefreshFromCatalog(); err != nil {
			panic(err)
		}
		_, _, _ = holder.Lookup(point)
	}); allocations != 0 {
		t.Fatalf("steady refresh+lookup allocations=%f, want 0", allocations)
	}
}

func TestCatalogRequestLedgerTopologyAcceptsCertifiedRangeSplitWhileGenericPublishRefuses(t *testing.T) {
	initial, topology := testRequestLedgerCatalogSnapshot(t, 5)
	catalog := NewCatalogHolder(initial)
	holder, err := NewCatalogDurableRequestLedgerTopologyHolder(catalog)
	if err != nil {
		t.Fatal(err)
	}
	middle := requestledger.LedgerHome{0x80}
	split := DurableRequestLedgerTopology{Generation: 6, Ranges: []DurableRequestLedgerRange{
		{End: middle, Identity: topology.Ranges[0].Identity, Route: topology.Ranges[0].Route},
		{Start: middle, Identity: replication.Digest{0x92}, Route: topology.Ranges[0].Route},
	}}
	if err := holder.Publish(split); !errors.Is(err, ErrDurableRequest) {
		t.Fatalf("generic split publication err=%v, want ErrDurableRequest", err)
	}
	config, endpoints, descriptor, _ := testRequestLedgerCatalogInput(t, 6)
	certified, err := NewSnapshotWithReplicatedRequestLedgerMetadata(
		config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{descriptor}, nil, split,
	)
	if err != nil {
		t.Fatal(err)
	}
	// This installs the result of the catalog authority's certified cut. The
	// topology holder is not itself responsible for validating split evidence;
	// it accepts only the immutable Snapshot exposed by CatalogHolder.Current.
	installCatalogRefreshTestSnapshot(t, catalog, certified)
	if err := holder.RefreshFromCatalog(); err != nil {
		t.Fatal(err)
	}
	left, leftGeneration, leftOK := holder.Lookup(requestledger.LedgerHome{0x20})
	right, rightGeneration, rightOK := holder.Lookup(requestledger.LedgerHome{0xc0})
	if !leftOK || !rightOK || leftGeneration != 6 || rightGeneration != 6 ||
		left.Identity != split.Ranges[0].Identity || right.Identity != split.Ranges[1].Identity {
		t.Fatalf("split homes left=%+v/%d/%v right=%+v/%d/%v",
			left, leftGeneration, leftOK, right, rightGeneration, rightOK)
	}
}

func TestCatalogRequestLedgerTopologyConcurrentRefreshConvergesAndNeverRegresses(t *testing.T) {
	initial, _ := testRequestLedgerCatalogSnapshot(t, 5)
	catalog := NewCatalogHolder(initial)
	holder, err := NewCatalogDurableRequestLedgerTopologyHolder(catalog)
	if err != nil {
		t.Fatal(err)
	}
	const finalGeneration = 32
	var wait sync.WaitGroup
	for generation := uint64(6); generation <= finalGeneration; generation++ {
		next, _ := testRequestLedgerCatalogSnapshot(t, generation)
		wait.Add(1)
		go func(snapshot *Snapshot) {
			defer wait.Done()
			_ = catalog.PublishNewer(snapshot)
			_ = holder.RefreshFromCatalog()
		}(next)
	}
	wait.Wait()
	if err := holder.RefreshFromCatalog(); err != nil {
		t.Fatal(err)
	}
	current := holder.Current()
	if current == nil || current.Generation != catalog.Current().Generation() ||
		current.Generation != finalGeneration {
		t.Fatalf("holder generation=%v catalog=%d", current, catalog.Current().Generation())
	}
}

func TestCatalogRequestLedgerTopologyRejectsLocallyInjectedGenerationAheadOfCatalog(t *testing.T) {
	initial, topology := testRequestLedgerCatalogSnapshot(t, 5)
	catalog := NewCatalogHolder(initial)
	holder, err := NewCatalogDurableRequestLedgerTopologyHolder(catalog)
	if err != nil {
		t.Fatal(err)
	}
	topology.Generation = 6
	if err := holder.Publish(topology); err != nil {
		t.Fatal(err)
	}
	if err := holder.RefreshFromCatalog(); !errors.Is(err, ErrDurableRequestUnavailable) {
		t.Fatalf("ahead-of-catalog refresh err=%v", err)
	}
	if holder.Current() != nil {
		t.Fatal("locally injected generation remained serving ahead of catalog authority")
	}
}

func TestCatalogRequestLedgerTopologyAbsentFailsClosed(t *testing.T) {
	initial, _ := testRequestLedgerCatalogSnapshot(t, 5)
	catalog := NewCatalogHolder(initial)
	holder, err := NewCatalogDurableRequestLedgerTopologyHolder(catalog)
	if err != nil {
		t.Fatal(err)
	}
	installCatalogRefreshTestSnapshot(t, catalog, testSnapshot(t, 6))
	if err := holder.RefreshFromCatalog(); !errors.Is(err, ErrDurableRequestUnavailable) {
		t.Fatalf("absent topology refresh err=%v", err)
	}
	if current := holder.Current(); current != nil {
		t.Fatalf("absent topology retained stale generation %+v", current)
	}
	if _, _, ok := holder.Lookup(requestledger.LedgerHome{0x40}); ok {
		t.Fatal("absent catalog topology continued serving a stale home")
	}
}

// installCatalogRefreshTestSnapshot models the output boundary of a certified
// catalog-Raft transition without duplicating the certificate protocol in the
// topology-holder unit tests.
func installCatalogRefreshTestSnapshot(t testing.TB, catalog *CatalogHolder, snapshot *Snapshot) {
	t.Helper()
	state, err := initialCatalogState(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	catalog.leaseMu.Lock()
	catalog.ptr.Store(state)
	catalog.signalLeaseChangeLocked()
	catalog.leaseMu.Unlock()
}
