package gateway

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type frontendDrainRuntimeCatalogOwnerFixture struct {
	state     raftservice.ServingState
	probeErr  error
	readErr   error
	readFence raftservice.ServingFence
	readCap   serviceauthz.Capability
	readAuth  func(raftservice.ServingState) bool
}

func (fixture *frontendDrainRuntimeCatalogOwnerFixture) Probe(
	_ context.Context, _ raftmember.GroupKey,
) (raftservice.ServingState, error) {
	return fixture.state, fixture.probeErr
}

func (fixture *frontendDrainRuntimeCatalogOwnerFixture) ReadLinearizablePointInto(
	_ context.Context, request raftservice.LinearizablePointReadRequest, _ *raftservice.LinearizablePointReadCut,
) error {
	fixture.readFence = request.Fence
	fixture.readCap = request.Capability
	fixture.readAuth = request.Authorize
	return fixture.readErr
}

func frontendDrainRuntimeCatalogRowRouteForTest() FrontendDrainRuntimeCatalogRoute {
	return FrontendDrainRuntimeCatalogRoute{
		Group:                raftmember.GroupKey{GroupID: [16]byte{1}},
		AllocationGeneration: 7,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 2, ProtectionEpoch: 3,
			OwnershipEpoch: 4, SchemaGeneration: 5, RelationManifestDigest: [32]byte{6},
			RoutingVersion: 7, RouteGeneration: 8,
		},
		Relation: 1,
	}
}

func frontendDrainRuntimeCatalogOwnerStateForTest(
	route FrontendDrainRuntimeCatalogRoute,
) raftservice.ServingState {
	return raftservice.ServingState{
		Identity: raftmember.RuntimeIdentity{
			Group: route.Group, AllocationGeneration: route.AllocationGeneration,
			MemberID: 3, StoreID: [16]byte{9}, NodeIncarnation: 11,
		},
		Command: route.Command,
		Status:  raftmember.RuntimeStatus{MemberID: 3, LeaderID: 3, Term: 13},
	}
}

func TestFrontendDrainRuntimeCatalogRowReaderRefreshesServingRoute(t *testing.T) {
	route := frontendDrainRuntimeCatalogRowRouteForTest()
	owner := &frontendDrainRuntimeCatalogOwnerFixture{
		state: frontendDrainRuntimeCatalogOwnerStateForTest(route),
	}
	reader, err := newFrontendDrainRuntimeCatalogRowReader(owner, route)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.ReadFrontendDrainRuntimeCatalogRoute(t.Context())
	if err != nil {
		t.Fatalf("read route: %v", err)
	}
	want := route
	want.Command = owner.state.Command
	if got != want {
		t.Fatalf("route=%+v want=%+v", got, want)
	}
	if _, err := reader.ReadFrontendDrainRuntimeRow(t.Context(), FrontendDrainRuntimeRowKey{
		Kind: FrontendDrainRuntimeCatalogHeadRow,
	}); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("zero cut error=%v", err)
	}
	if owner.readFence != owner.state.Fence() || owner.readCap != serviceauthz.CapabilityDataRead || owner.readAuth == nil {
		t.Fatalf("point request fence=%+v cap=%v auth=%v", owner.readFence, owner.readCap, owner.readAuth != nil)
	}
	if !owner.readAuth(owner.state) {
		t.Fatal("exact owner serving state was rejected by point authorization")
	}
	changed := owner.state
	changed.Status.Term++
	if owner.readAuth(changed) {
		t.Fatal("changed serving term accepted by point authorization")
	}
}

func TestFrontendDrainRuntimeCatalogRowReaderRejectsStaleOrNonLeaderOwner(t *testing.T) {
	route := frontendDrainRuntimeCatalogRowRouteForTest()
	tests := []struct {
		name  string
		state raftservice.ServingState
	}{
		{name: "wrong group", state: func() raftservice.ServingState {
			state := frontendDrainRuntimeCatalogOwnerStateForTest(route)
			state.Identity.Group.GroupID[0]++
			return state
		}()},
		{name: "wrong allocation", state: func() raftservice.ServingState {
			state := frontendDrainRuntimeCatalogOwnerStateForTest(route)
			state.Identity.AllocationGeneration++
			return state
		}()},
		{name: "nonleader", state: func() raftservice.ServingState {
			state := frontendDrainRuntimeCatalogOwnerStateForTest(route)
			state.Status.LeaderID++
			return state
		}()},
		{name: "command rollback", state: func() raftservice.ServingState {
			state := frontendDrainRuntimeCatalogOwnerStateForTest(route)
			state.Command.RouteGeneration--
			return state
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owner := &frontendDrainRuntimeCatalogOwnerFixture{state: test.state}
			reader, err := newFrontendDrainRuntimeCatalogRowReader(owner, route)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = reader.ReadFrontendDrainRuntimeCatalogRoute(t.Context()); !errors.Is(err, ErrReplicatedCatalogConflict) {
				t.Fatalf("route error=%v", err)
			}
		})
	}
}

func TestNewFrontendDrainRuntimeCatalogRowReaderRequiresReservedRoute(t *testing.T) {
	owner := &raftservice.ExecutionOwners{}
	base := ReplicatedRoute{
		Distribution: ReplicatedCatalogDistribution, Shard: ReplicatedCatalogShard,
		Group:                frontendDrainRuntimeCatalogRowRouteForTest().Group,
		AllocationGeneration: 1,
		Command:              frontendDrainRuntimeCatalogRowRouteForTest().Command,
		Replicas:             []ReplicatedEndpoint{{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{2}, NodeIncarnation: 1}},
	}
	if _, err := NewFrontendDrainRuntimeCatalogRowReader(owner, base, replication.RelationID(0)); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("zero relation error=%v", err)
	}
	base.Distribution = "data"
	if _, err := NewFrontendDrainRuntimeCatalogRowReader(owner, base, 1); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("noncatalog route error=%v", err)
	}
}
