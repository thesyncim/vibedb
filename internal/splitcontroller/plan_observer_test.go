package splitcontroller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
)

type coherentObserverCatalog struct {
	*memoryReplicatedOperationJournal
	mu        sync.Mutex
	snapshots []*gateway.Snapshot
	reads     int
}

func (catalog *coherentObserverCatalog) Read(context.Context) (*gateway.Snapshot, error) {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if len(catalog.snapshots) == 0 {
		return nil, ErrPlanObservation
	}
	index := catalog.reads
	if index >= len(catalog.snapshots) {
		index = len(catalog.snapshots) - 1
	}
	catalog.reads++
	return catalog.snapshots[index], nil
}

type coherentObservationClient struct {
	mu             sync.Mutex
	state          SourcePlanObservation
	sourceRequests []PlanObservationRequest
	childRequests  []PlanObservationRequest
	inFlight       int
	maximum        int
	badDigest      bool
}

func (client *coherentObservationClient) enter() func() {
	client.mu.Lock()
	client.inFlight++
	if client.inFlight > client.maximum {
		client.maximum = client.inFlight
	}
	client.mu.Unlock()
	time.Sleep(time.Millisecond)
	return func() {
		client.mu.Lock()
		client.inFlight--
		client.mu.Unlock()
	}
}

func (client *coherentObservationClient) ObserveSplitSource(
	_ context.Context, request PlanObservationRequest,
) (SourcePlanObservation, error) {
	leave := client.enter()
	defer leave()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.sourceRequests = append(client.sourceRequests, request)
	result := client.state
	result.RequestDigest = request.RequestDigest
	if client.badDigest {
		result.RequestDigest[0] ^= 0xff
	}
	return result, nil
}

func (client *coherentObservationClient) ObserveSplitChild(
	_ context.Context, request PlanObservationRequest,
) (ChildPlanObservation, error) {
	leave := client.enter()
	defer leave()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.childRequests = append(client.childRequests, request)
	result := ChildPlanObservation{RequestDigest: request.RequestDigest}
	if client.badDigest {
		result.RequestDigest[0] ^= 0xff
	}
	return result, nil
}

type coherentDrainAuthority struct {
	mu       sync.Mutex
	requests []PlanCatalogDrainRequest
	proof    PlanCatalogDrainProof
}

func (authority *coherentDrainAuthority) ObservePlanCatalogDrain(
	_ context.Context, request PlanCatalogDrainRequest,
) (PlanCatalogDrainProof, error) {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	authority.requests = append(authority.requests, request)
	result := authority.proof
	result.RequestDigest = request.RequestDigest
	return result, nil
}

func TestCoherentPlanObserverUsesBoundedParallelExactWaveAndRestarts(t *testing.T) {
	plan, snapshot, _, _ := testPlan(t)
	state := testSourceState(plan)
	catalog := &coherentObserverCatalog{
		memoryReplicatedOperationJournal: new(memoryReplicatedOperationJournal),
		snapshots:                        []*gateway.Snapshot{snapshot},
	}
	client := &coherentObservationClient{state: SourcePlanObservation{
		State: state, Status: testLeaderStatus(state),
	}}
	drain := new(coherentDrainAuthority)
	newObserver := func() *CoherentPlanObserver {
		observer, err := NewCoherentPlanObserver(CoherentPlanObserverOptions{
			Catalog: catalog, Observations: client, CatalogDrain: drain,
			MaxConcurrent: 2, MaxAttempts: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		return observer
	}
	first, err := newObserver().ObservePlan(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newObserver().ObservePlan(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceState != second.SourceState || first.Catalog.Generation() != 19 ||
		second.Catalog.Generation() != 19 {
		t.Fatalf("restart observations differ: first=%+v second=%+v", first, second)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.maximum != 2 || len(client.sourceRequests) != 2 || len(client.childRequests) != 2 {
		t.Fatalf("bounded wave max=%d source=%d child=%d", client.maximum,
			len(client.sourceRequests), len(client.childRequests))
	}
	for _, request := range append(client.sourceRequests, client.childRequests...) {
		if request.Operation != plan.OperationID() || request.CatalogGeneration != 19 ||
			request.CatalogDigest == ([32]byte{}) || request.RequestDigest == ([32]byte{}) ||
			len(request.ControlEndpoints) != 1 {
			t.Fatalf("unbound observation request: %+v", request)
		}
	}
	if client.sourceRequests[0].Distribution != "orders" ||
		client.sourceRequests[0].Shard != "source" ||
		client.sourceRequests[0].ControlEndpoints[0] != "node-a" ||
		client.childRequests[0].Shard != "right" ||
		client.childRequests[0].ControlEndpoints[0] != "node-b" {
		t.Fatalf("wrong exact endpoints: source=%+v child=%+v",
			client.sourceRequests[0], client.childRequests[0])
	}
}

func TestCoherentPlanObserverRetriesMixedCatalogCut(t *testing.T) {
	plan, source, _, _ := testPlan(t)
	changed := plan.targetSnapshotForTest(t)
	state := testSourceState(plan)
	catalog := &coherentObserverCatalog{
		memoryReplicatedOperationJournal: new(memoryReplicatedOperationJournal),
		// Attempt one changes under the observation wave. Attempt two reads one
		// stable source image and must reconstruct the same safe action.
		snapshots: []*gateway.Snapshot{source, changed, source, source},
	}
	client := &coherentObservationClient{state: SourcePlanObservation{
		State: state, Status: testLeaderStatus(state),
	}}
	observer, err := NewCoherentPlanObserver(CoherentPlanObserverOptions{
		Catalog: catalog, Observations: client, CatalogDrain: new(coherentDrainAuthority),
		MaxConcurrent: 2, MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := observer.ObservePlan(t.Context(), plan)
	if err != nil || observed.Catalog.Generation() != source.Generation() {
		t.Fatalf("observation=%+v err=%v", observed, err)
	}
	if catalog.reads != 4 {
		t.Fatalf("catalog reads=%d want=4", catalog.reads)
	}
}

func TestCoherentPlanObserverRejectsUnboundResponse(t *testing.T) {
	plan, snapshot, _, _ := testPlan(t)
	state := testSourceState(plan)
	catalog := &coherentObserverCatalog{
		memoryReplicatedOperationJournal: new(memoryReplicatedOperationJournal),
		snapshots:                        []*gateway.Snapshot{snapshot},
	}
	client := &coherentObservationClient{badDigest: true, state: SourcePlanObservation{
		State: state, Status: testLeaderStatus(state),
	}}
	observer, err := NewCoherentPlanObserver(CoherentPlanObserverOptions{
		Catalog: catalog, Observations: client, CatalogDrain: new(coherentDrainAuthority),
		MaxConcurrent: 1, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = observer.ObservePlan(t.Context(), plan); !errors.Is(err, ErrPlanObservation) {
		t.Fatalf("unbound response error=%v", err)
	}
}

func TestCoherentPlanObserverRequiresExplicitBoundsAndAuthorities(t *testing.T) {
	if _, err := NewCoherentPlanObserver(CoherentPlanObserverOptions{}); !errors.Is(err, ErrPlanObservation) {
		t.Fatalf("empty options error=%v", err)
	}
}
