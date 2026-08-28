package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// Exercise the exact startup comparison without a platform-specific WAL or
// strict-allocation dependency. The Linux prepared-store test covers the same
// seam with the real catalog image and opened durable store.
func TestDevPreparedSchemaWitnessUsesMachineNotLogicalDigest(t *testing.T) {
	for _, role := range []struct {
		table, primaryKey string
		distribution      distribution.DistributionName
		shard             distribution.ShardID
	}{
		{gateway.ReplicatedCatalogTable, gateway.ReplicatedCatalogPrimaryKey, gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard},
		{devLedgerTable, devLedgerPrimaryKey, devLedgerDistribution, devLedgerShard},
		{devDataTable, devDataPrimaryKey, devDataDistribution, devDataShard},
	} {
		t.Run(role.table, func(t *testing.T) {
			binding := sqldriver.ReplicatedShardStoreBinding{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
				TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4},
				Distribution: string(role.distribution), Shard: string(role.shard), AllocationGeneration: 1, MemberID: 1, StoreID: [16]byte{5},
				Authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1}}
			placement := sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat,
				ShardKey: role.primaryKey, TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
				Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}
			machine, limits, err := sqldriver.InitialReplicatedRelationManifest(binding, placement,
				sqldriver.InitialReplicatedRelationSchema{Table: role.table, PrimaryKey: role.primaryKey})
			if err != nil {
				t.Fatal(err)
			}
			identity, err := sqldriver.NewReplicatedChildShardStoreIdentity(sqldriver.ShardStoreIdentity{
				Distribution: role.distribution, Shard: role.shard, AllocationGeneration: 1, LogID: [16]byte{6}},
				binding, role.table, strings.Repeat("a", 64), role.primaryKey, limits)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := sqldriver.ReplicatedSchemaManifest(identity, placement, nil)
			if err != nil || actual != machine {
				t.Fatalf("initial and bound machine schema differ: %v", err)
			}
			logical, err := sqldriver.ReplicatedRelationManifestDigest(identity)
			if err != nil || logical == machine || logical == identity.RelationManifestDigest {
				t.Fatalf("schema domains collapsed: %v", err)
			}
			image := sqldriver.ReplicatedSchemaCatalogImage{SchemaGeneration: identity.RelationSchemaGeneration,
				RelationManifestDigest: machine, LocalRelationManifestDigest: identity.RelationManifestDigest}
			if got, err := devLogicalSchemaForPreparedImage(identity, machine, image); err != nil || got != logical {
				t.Fatalf("correct machine witness refused or returned as logical: %v", err)
			}
			for name, mutate := range map[string]func(*sqldriver.ReplicatedSchemaCatalogImage){
				"logical as machine": func(i *sqldriver.ReplicatedSchemaCatalogImage) { i.RelationManifestDigest = logical },
				"local as machine": func(i *sqldriver.ReplicatedSchemaCatalogImage) {
					i.RelationManifestDigest = identity.RelationManifestDigest
				},
				"foreign machine":    func(i *sqldriver.ReplicatedSchemaCatalogImage) { i.RelationManifestDigest[0]++ },
				"foreign local":      func(i *sqldriver.ReplicatedSchemaCatalogImage) { i.LocalRelationManifestDigest[0]++ },
				"foreign generation": func(i *sqldriver.ReplicatedSchemaCatalogImage) { i.SchemaGeneration++ },
			} {
				t.Run(name, func(t *testing.T) {
					wrong := image
					mutate(&wrong)
					if _, err := devLogicalSchemaForPreparedImage(identity, machine, wrong); !errors.Is(err, errDevCluster) {
						t.Fatalf("substituted schema witness accepted: %v", err)
					}
				})
			}
		})
	}
}
