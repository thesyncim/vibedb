package gatewayruntime

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/scaling"
)

type scalingBudgetDirectory struct {
	gateway.DirectoryReader
	nodes []gateway.NodeRecord
}

func (directory scalingBudgetDirectory) ListNodes(context.Context) ([]gateway.NodeRecord, error) {
	return slices.Clone(directory.nodes), nil
}

func (scalingBudgetDirectory) ListEnrollmentIntents(context.Context, raftmember.GroupKey) ([]gateway.GroupEnrollmentIntent, error) {
	return nil, nil
}

type scalingBudgetCapacityFunc func(context.Context, rafttransport.NodeID, replicacontrol.CapacityRequest) (replicacontrol.CapacityObservation, error)

func (observe scalingBudgetCapacityFunc) Observe(ctx context.Context, node rafttransport.NodeID, request replicacontrol.CapacityRequest) (replicacontrol.CapacityObservation, error) {
	return observe(ctx, node, request)
}

func TestScalingPlanExhaustedBudgetRequiresFreshNoWork(t *testing.T) {
	for _, budget := range []string{"moves", "migration_bytes"} {
		t.Run(budget, func(t *testing.T) {
			for _, scenario := range []struct {
				name            string
				targetUsed      uint64
				capacityErr     error
				planErr         error
				unmeasured      bool
				unexhaustedPlan scaling.PlacementState
			}{
				{name: "balanced", targetUsed: 500, unexhaustedPlan: scaling.PlacementNoWork},
				{name: "remaining_work", targetUsed: 0, unexhaustedPlan: scaling.PlacementMoves},
				{name: "target_full", targetUsed: 1000, unexhaustedPlan: scaling.PlacementBlocked},
				{name: "capacity_unavailable", targetUsed: 500, capacityErr: replicacontrol.ErrCapacityUnavailable},
				{name: "capacity_unmeasured", targetUsed: 500, unmeasured: true, planErr: scaling.ErrInvalidPlacementInput},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					snapshot := catalogRouteSeedSnapshot(t, 2, "127.0.0.1:7101")
					route, ok := snapshot.ReplicatedRouteAt(0, nil)
					if !ok {
						t.Fatal("missing fixture route")
					}
					nodes := make([]gateway.NodeRecord, 0, gateway.ServingReplicaCount+1)
					for _, replica := range route.Replicas {
						nodes = append(nodes, gateway.NodeRecord{
							NodeID: replica.Node, Incarnation: replica.NodeIncarnation,
							ServiceKeyDigest: [32]byte{replica.Node[0]},
							DataEndpoint:     distribution.EndpointID(replica.Endpoint), DataAddress: replica.DataAddress,
							NativeEndpoint: distribution.EndpointID(replica.NativeEndpoint), NativeAddress: replica.Address,
							ControlEndpoint: distribution.EndpointID(replica.ControlEndpoint), ControlAddress: replica.ControlAddress,
							FailureDomain: replica.Endpoint, Roles: gateway.NodeRoleStorage,
							Capacity:          autosplit.CapacityVector{autosplit.ResourceLiveBytes: 1000},
							Used:              autosplit.CapacityVector{autosplit.ResourceLiveBytes: 500},
							MigrationCapacity: 1000, MaxReceives: 1, Lifecycle: gateway.NodeActive,
							Revision: 1, CatalogGeneration: snapshot.Generation(),
						})
					}
					target := nodes[0]
					target.NodeID, target.Incarnation = rafttransport.NodeID{4}, 24
					target.ServiceKeyDigest = [32]byte{4}
					target.DataEndpoint, target.DataAddress = "four", "127.0.0.1:7004"
					target.NativeEndpoint, target.NativeAddress = "four-native", "127.0.0.1:7104"
					target.ControlEndpoint, target.ControlAddress = "four-control", "127.0.0.1:7204"
					target.FailureDomain = "four"
					target.Used[autosplit.ResourceLiveBytes] = scenario.targetUsed
					nodes = append(nodes, target)
					observations := 0
					controller := &ScalingController{
						directory: scalingBudgetDirectory{nodes: nodes},
						catalog:   scalingEnrollmentCatalogFixture{snapshot: snapshot},
						capacity: scalingBudgetCapacityFunc(func(_ context.Context, nodeID rafttransport.NodeID, request replicacontrol.CapacityRequest) (replicacontrol.CapacityObservation, error) {
							observations++
							if scenario.capacityErr != nil {
								return replicacontrol.CapacityObservation{}, scenario.capacityErr
							}
							for ordinal, replica := range route.Replicas {
								if replica.Node != nodeID || replica.Member != request.TargetMember || request.Group != route.Group {
									continue
								}
								node := nodes[ordinal]
								observation := replicacontrol.CapacityObservation{
									Request: request, CatalogGeneration: snapshot.Generation(), Applied: 1, SourceRevision: 1,
									Identity: raftmember.RuntimeIdentity{Group: route.Group, MemberID: replica.Member,
										AllocationGeneration: route.AllocationGeneration, StoreID: replica.StoreID,
										NodeIncarnation: replica.NodeIncarnation},
									Demand: autosplit.CapacityVector{autosplit.ResourceLiveBytes: 100}, MigrationBytes: 100,
									DemandKind: replicacontrol.CapacityDemandMeasured,
									Node: replicacontrol.NodeCapacity{NodeID: nodeID, NodeIncarnation: node.Incarnation, Revision: node.Revision,
										Capacity: node.Capacity, Used: node.Used, MigrationCapacity: node.MigrationCapacity, MaxReceives: node.MaxReceives},
								}
								if scenario.unmeasured {
									observation.Demand, observation.MigrationBytes = autosplit.CapacityVector{}, 0
								}
								return observation, nil
							}
							t.Fatalf("unexpected capacity request: node=%x request=%+v", nodeID, request)
							return replicacontrol.CapacityObservation{}, replicacontrol.ErrCapacityUnavailable
						}),
					}
					intent := gateway.ScalingIntent{ID: [32]byte{1}, State: gateway.ScalingRunning,
						Request: gateway.ScalingIntentRequest{Kind: gateway.ScalingRebalance, RequestID: [32]byte{2}, MaxMoves: 2, MaxMigrationBytes: 200}}
					// Verify the fixture has the expected placement result before the
					// exhaustion fence; a malformed fixture must not prove safety by accident.
					before, err := controller.plan(t.Context(), intent)
					initialErr := scenario.capacityErr
					if initialErr == nil {
						initialErr = scenario.planErr
					}
					if initialErr != nil {
						if !errors.Is(err, initialErr) {
							t.Fatalf("unexhausted plan failure=%v", err)
						}
					} else if err != nil || before.State != scenario.unexhaustedPlan {
						t.Fatalf("unexhausted plan=%+v err=%v", before, err)
					}
					if budget == "moves" {
						intent.PlannedReplicas, intent.CompletedReplicas = 2, 2
					} else {
						intent.PlannedReplicas, intent.CompletedReplicas = 1, 1
						intent.AdmittedMigrationBytes = intent.Request.MaxMigrationBytes
					}
					observations = 0
					plan, err := controller.plan(t.Context(), intent)
					if scenario.name == "balanced" {
						if err != nil || plan.State != scaling.PlacementNoWork || plan.HasMoves() ||
							plan.RemainingReplicas != 0 || plan.ConsideredReplicas != gateway.ServingReplicaCount ||
							plan.ExpectedCatalogGeneration != snapshot.Generation() {
							t.Fatalf("completed budget failed to prove convergence: plan=%+v err=%v", plan, err)
						}
					} else {
						wantErr := scenario.capacityErr
						if scenario.planErr != nil {
							wantErr = scenario.planErr
						}
						if wantErr == nil {
							wantErr = ErrScalingControllerBlocked
						}
						if !errors.Is(err, wantErr) || plan.State != 0 || plan.HasMoves() {
							t.Fatalf("exhausted budget admitted or completed uncertain work: plan=%+v err=%v", plan, err)
						}
					}
					wantObservations := gateway.ServingReplicaCount
					if scenario.capacityErr != nil {
						wantObservations = 1
					}
					if observations != wantObservations {
						t.Fatalf("fresh capacity observations=%d want=%d", observations, wantObservations)
					}
				})
			}
		})
	}
}
