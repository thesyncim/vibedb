//go:build linux

package main

import (
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store"
)

// Use real sealed source SQL/apply bundles; no synthetic backend or opt-in can
// hide disagreement between live source, allocation, plan, intent and prepare.
func TestGatewayHotSplitComposedLocalGlobalBundle(t *testing.T) {
	for _, global := range []bool{false, true} {
		name := "local"
		if global {
			name = "base-local-global"
		}
		t.Run(name, func(t *testing.T) {
			initial, source, profile, work := gatewayHotSplitFactoryFixture(t)
			entry := gatewaySplitSourceFixture(t, source, profile)
			options := rf3testfixture.MemberOptions{Root: t.TempDir(), Table: profile.Table,
				CreateTable: "CREATE TABLE messages (PRIMARY KEY (id))", SchemaStatements: []string{"CREATE INDEX by_email ON messages (email)"},
				Identity: raftstore.Identity{ClusterID: source.Group.ClusterID, ClusterIncarnation: source.Group.ClusterIncarnation,
					ShardIncarnation: source.Group.ShardIncarnation, GroupID: source.Group.GroupID, Distribution: string(source.Distribution),
					Shard: string(source.Shard), AllocationGeneration: uint64(source.AllocationGeneration), MemberID: source.Replicas[0].Member, StoreID: source.Replicas[0].StoreID},
				Authority: entry.SQL.Binding.Authority, Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
				WAL: rf3testfixture.DurableGatewayWALOptions(), Key: raftstore.Key{ID: "composed-schema-key", Wrapped: []byte("wrapped")},
				Apply: sqldriver.ReplicatedApplyOptions{MaxSessions: entry.Template.MaxSessions, RetryWindow: entry.Template.RetryWindow,
					TxnLimits: entry.Template.TxnLimits, Placement: sqldriver.ReplicatedPlacementProfile{Format: entry.Template.Format,
						ShardKey: "/id", TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
						Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}}}
			options.Bootstrap.TopologyRecoveryEpoch = source.Group.TopologyRecoveryEpoch
			options.Key.Material[0] = 7
			if global {
				options.SchemaStatements = append(options.SchemaStatements, "CREATE TABLE emails (PRIMARY KEY (key))")
				options.GlobalIndexes = rf3testfixture.DurableGatewayMemberProfiles()[rf3testfixture.DurableGatewayDataAGroup].GlobalIndexes
				options.GlobalIndexes[0].Table = "emails"
			}
			prepared, err := rf3testfixture.PrepareMember(options)
			if err != nil {
				t.Fatal(err)
			}
			entry.SQL = prepared.Base.Clone()
			entry.LocalIndexes = []store.IndexDefinition{{Name: "by_email", Paths: []string{"/email"}}}
			entry.RelationManifestDigest, err = prepared.Apply.RangeSplitRelationManifestDigest()
			if err != nil {
				t.Fatal(err)
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			entry.Template.MaxBatchDocuments, entry.Template.MaxBatchBytes = entry.SQL.UserLimits.MaxBatchDocuments, entry.SQL.UserLimits.MaxBatchBytes
			source.Command.RelationManifestDigest = entry.RelationManifestDigest
			logical, err := sqldriver.ReplicatedRelationManifestDigest(entry.SQL)
			if err != nil {
				t.Fatal(err)
			}
			source.LogicalSchemaDigest, profile.LogicalSchemaDigest = replication.Digest(logical), replication.Digest(logical)
			if logical == entry.RelationManifestDigest {
				t.Fatal("logical and serving schema identity collapsed")
			}
			profile.MaxKeyBytes, profile.MaxDocumentBytes = uint16(entry.SQL.UserLimits.MaxKeyBytes), uint32(entry.SQL.UserLimits.MaxDocumentBytes)
			config := distribution.ClusterConfig{}
			spec, _ := initial.Spec(source.Distribution)
			placement, _ := initial.Placement(profile.Table)
			route, _ := initial.Manifest(source.Distribution)
			config.Distributions, config.Placements, config.Manifests = []distribution.DistributionSpec{spec}, []distribution.TablePlacement{placement}, []*distribution.Manifest{route}
			addresses := make(map[distribution.EndpointID]string)
			for _, replica := range source.Replicas {
				for _, endpoint := range []distribution.EndpointID{replica.Endpoint, replica.NativeEndpoint, replica.ControlEndpoint} {
					addresses[endpoint], err = initial.Address(endpoint)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			catalog, err := gateway.NewSnapshotWithReplicatedTableMetadata(config, addresses, initial.Generation(), nil, nil,
				[]gateway.ReplicatedShardDescriptor{source}, []gateway.ReplicatedTableProfile{profile})
			if err != nil {
				t.Fatal(err)
			}
			inventory := gatewayReplicaControlManifest{SplitSources: []gatewaySplitSource{entry}}
			for _, replica := range source.Replicas {
				inventory.Shards = append(inventory.Shards, gateway.ReplicatedEndpoint{Node: replica.Node})
				inventory.SplitSnapshots = append(inventory.SplitSnapshots, "127.0.0.1:9301")
			}
			sources, err := gatewayHotSplitSources(inventory, catalog)
			if err != nil {
				t.Fatal(err)
			}
			factory := gatewayHotSplitFactory{sources: sources}
			split, err := factory.allocateSplit(catalog, [32]byte{7}, work, source)
			if err != nil {
				t.Fatal(err)
			}
			partitioner, err := rangesplit.NewPartitioner(split, profile.Table, placement.Columns, distribution.DefaultVirtualBucketBits)
			if err != nil {
				t.Fatal(err)
			}
			operation, err := splitcontroller.OperationIDForSplit(catalog.Generation(), split, partitioner)
			if err != nil {
				t.Fatal(err)
			}
			child, _ := split.Child(1)
			target, err := factory.buildChildTarget(catalog, [32]byte(operation), 1, child, source, profile)
			if err != nil {
				t.Fatal(err)
			}
			retained := sources[source.Group]
			plan, err := splitcontroller.NewPlan(catalog, split, partitioner, []splitcontroller.ChildTarget{target}, splitcontroller.PlanSourceSchema{
				SQL: retained.SQL, Placement: retained.Placement, LocalIndexes: retained.LocalIndexes})
			if err != nil || plan.OperationID() != operation {
				t.Fatalf("composed plan: %v", err)
			}
			raw, err := splitcontroller.AppendPlanIntent(nil, catalog, plan)
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := splitcontroller.OpenPlanIntent(raw, catalog)
			if err != nil || reopened.OperationID() != operation {
				t.Fatalf("composed intent reopen: %v", err)
			}
			for i, replica := range target.Replicas {
				if replica.SQL.RelationCount != entry.SQL.RelationCount {
					t.Fatal("allocator lost bundle relation")
				}
				if err := sqldriver.ValidateReplicatedChildSchema(replica.SQL, options.CreateTable, options.SchemaStatements, options.GlobalIndexes); err != nil {
					t.Fatal(err)
				}
				actual, err := sqldriver.ReplicatedSchemaManifest(replica.SQL, replica.Apply.Placement, entry.LocalIndexes)
				if err != nil || actual != target.RelationManifestDigest || actual == entry.RelationManifestDigest {
					t.Fatalf("child domain mismatch: %v", err)
				}
				if _, err := splitcontroller.NewChildPreparation(operation, gatewayHotSplitDigest("allocation", [32]byte(operation), 1, 0), child, profile.Table, target, uint8(i)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
