package gatewayruntime

import (
	"context"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/scaling"
)

type scalingEnrollmentCatalogFixture struct{ snapshot *gateway.Snapshot }

func (fixture scalingEnrollmentCatalogFixture) Read(context.Context) (*gateway.Snapshot, error) {
	return fixture.snapshot, nil
}

func TestScalingEnrollmentChoosesDistinctDeterministicSnapshotSource(t *testing.T) {
	snapshot := catalogRouteSeedSnapshot(t, 1, "127.0.0.1:7101")
	membership, ok := snapshot.ResolveReplicatedMembershipRoute(
		gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard, nil,
	)
	if !ok {
		t.Fatal("catalog self-route is missing")
	}
	target := gateway.NodeRecord{
		NodeID:            rafttransport.NodeID{4},
		Incarnation:       1,
		ServiceKeyDigest:  [32]byte{4},
		DataEndpoint:      "target",
		NativeEndpoint:    "target-native",
		ControlEndpoint:   "target-control",
		DataAddress:       "127.0.0.1:7004",
		NativeAddress:     "127.0.0.1:7104",
		ControlAddress:    "127.0.0.1:7204",
		FailureDomain:     "zone-target",
		Roles:             gateway.NodeRoleStorage,
		Lifecycle:         gateway.NodeActive,
		Revision:          7,
		CatalogGeneration: snapshot.Generation(),
	}
	if !target.Valid() {
		t.Fatal("target fixture is invalid")
	}
	request := gateway.ScalingIntentRequest{
		Kind: gateway.ScalingRebalance, RequestID: [32]byte{1}, MaxMoves: 1,
	}
	parent := gateway.ScalingIntent{
		ID: request.ID(), Request: request, CatalogGeneration: snapshot.Generation(),
		Revision: 1, DirectoryRevision: 1, State: gateway.ScalingRunning,
	}
	source := membership.Serving.Replicas[0]
	move := scaling.ReplicaMove{
		Group: membership.Serving.Group, Distribution: membership.Serving.Distribution,
		Shard: membership.Serving.Shard, AllocationGeneration: distribution.ShardAllocationGeneration(membership.Serving.AllocationGeneration),
		ReplicaOrdinal: 0,
		Source: gateway.ReplicatedReplicaDescriptor{
			Member: source.Member, Node: source.Node, StoreID: source.StoreID,
			NodeIncarnation: source.NodeIncarnation,
			Endpoint:        distribution.EndpointID(source.Endpoint),
			NativeEndpoint:  distribution.EndpointID(source.NativeEndpoint),
			ControlEndpoint: distribution.EndpointID(source.ControlEndpoint),
		},
		SourceNode: gateway.NodeReference{NodeID: source.Node, Incarnation: source.NodeIncarnation},
		TargetNode: gateway.NodeReference{NodeID: target.NodeID, Incarnation: target.Incarnation},
		Target:     target, ExpectedCatalogGeneration: snapshot.Generation(),
		SourceNodeRevision: 1, TargetNodeRevision: target.Revision,
	}
	runtime := &ScalingEnrollmentRuntime{
		catalog: scalingEnrollmentCatalogFixture{snapshot: snapshot},
		source: func(_ context.Context, intent gateway.GroupEnrollmentIntent) ([]byte, error) {
			voters := [3]nodecontrol.PreparationMember{}
			for index, replica := range membership.Serving.Replicas {
				voters[index] = nodecontrol.PreparationMember{
					MemberID: replica.Member, Node: replica.Node,
					PeerEndpoint:    distribution.EndpointID(replica.Endpoint),
					NativeEndpoint:  distribution.EndpointID(replica.NativeEndpoint),
					ControlEndpoint: distribution.EndpointID(replica.ControlEndpoint),
					PeerAddress:     replica.DataAddress, NativeAddress: replica.Address,
					ControlAddress: replica.ControlAddress,
				}
			}
			slices.SortFunc(voters[:], func(left, right nodecontrol.PreparationMember) int {
				if left.MemberID < right.MemberID {
					return -1
				}
				if left.MemberID > right.MemberID {
					return 1
				}
				return 0
			})
			spec := nodecontrol.PreparationSpec{
				Kind: nodecontrol.PreparationSpecKind, Group: intent.Group,
				Distribution: intent.Distribution, Shard: intent.Shard,
				AllocationGeneration: intent.AllocationGeneration,
				ReplicaOrdinal:       intent.ReplicaOrdinal, SourceCommand: intent.ExpectedCommand,
				LogicalSchemaDigest: intent.ExpectedCommand.RelationManifestDigest,
				InitialVoters:       voters,
				Target: nodecontrol.PreparationMember{
					MemberID: intent.Target.Member, Node: intent.Target.Node,
					PeerEndpoint: intent.Target.Endpoint, NativeEndpoint: intent.Target.NativeEndpoint,
					ControlEndpoint: intent.Target.ControlEndpoint,
					PeerAddress:     target.DataAddress, NativeAddress: target.NativeAddress,
					ControlAddress: target.ControlAddress,
				},
				TargetNodeIncarnation: intent.Target.NodeIncarnation, TargetStoreID: intent.Target.StoreID,
				Table: "catalog", CreateTable: "CREATE TABLE catalog (id TEXT PRIMARY KEY)",
				Apply: nodecontrol.PreparationApplyProfile{MaxSessions: 1, RetryWindow: 1, MaxCollections: 1, MaxDocuments: 1, MaxBytes: 1, ShardKey: "id"},
				Log:   nodecontrol.PreparationLogProfile{MaxFileBytes: 1, MaxRecordBytes: 1, MaxRecords: 1, MaxEntries: 1, MaxLiveBytes: 1},
			}
			return nodecontrol.AppendPreparationSpec(nil, spec)
		},
	}
	first, err := runtime.BuildEnrollment(t.Context(), parent, move, membership, target)
	if err != nil {
		t.Fatalf("build enrollment: %v", err)
	}
	if first.SnapshotSourceMember == source.Member || first.SnapshotSourceMember != 2 {
		t.Fatalf("snapshot source member=%d, want distinct lowest voter 2", first.SnapshotSourceMember)
	}
	if !first.Valid() {
		t.Fatal("built enrollment is invalid")
	}

	// Catalog adapters may return a valid roster in another order. The choice
	// remains stable because it is based on the member identity, not position.
	reordered := membership
	reordered.Serving.Replicas = slices.Clone(reordered.Serving.Replicas)
	slices.Reverse(reordered.Serving.Replicas)
	second, err := runtime.BuildEnrollment(t.Context(), parent, move, reordered, target)
	if err != nil {
		t.Fatalf("build reordered enrollment: %v", err)
	}
	if second.SnapshotSourceMember != first.SnapshotSourceMember || second.IntentID != first.IntentID {
		t.Fatalf("reordered enrollment changed donor or identity: first=%d/%x second=%d/%x", first.SnapshotSourceMember, first.IntentID, second.SnapshotSourceMember, second.IntentID)
	}
	if first.Digest() != second.Digest() {
		t.Fatalf("reordered enrollment changed immutable digest: %x/%x", first.Digest(), second.Digest())
	}
}
