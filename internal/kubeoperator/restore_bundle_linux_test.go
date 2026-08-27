//go:build linux

package kubeoperator

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestRestoreReplicaFactoryBindsExactBundle(t *testing.T) {
	template := validRestoreTestTemplate()
	template.DDL = append(template.DDL,
		"CREATE INDEX by_email ON items (email)",
		"CREATE TABLE email_claims (PRIMARY KEY (key))",
	)
	template.GlobalIndexes = []restoreGlobalIndexTemplate{{
		Relation: 2, Table: "email_claims", IndexID: 41, Incarnation: 7,
		LocatorCount: 1, Unique: true, KeyEncoding: uint8(sqldriver.ReplicatedRelationKeyCanonicalTuple),
		KeyArity: 1, TupleVersion: uint32(distribution.CurrentTupleVersion),
		MapperVersion: uint32(distribution.NativeMapperVersion), BucketBits: distribution.DefaultVirtualBucketBits,
	}}
	root := t.TempDir()
	factory := restoreReplicaFactory{root: root, template: template}
	operation := clusterrestore.Operation{Digest: sha256.Sum256([]byte("bundle-operation"))}
	binding := sqldriver.ReplicatedShardStoreBinding{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 1,
		Distribution: template.Distribution, Shard: template.Shard,
		AllocationGeneration: template.AllocationGeneration,
		ShardIncarnation:     [16]byte{3}, GroupID: [16]byte{4}, MemberID: 1, StoreID: [16]byte{5},
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1,
			SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1,
		},
	}
	directory := filepath.Join(root, "replica")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	_, database, identity, err := factory.openOrCreateRoot(
		filepath.Join(directory, "member.vdb"), filepath.Join(directory, "allocation.vibejson"),
		operation, 0, 0, binding,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if identity.RelationCount != 2 || identity.RelationManifestDigest == ([32]byte{}) ||
		identity.Relations[1].Kind != sqldriver.ReplicatedShardRelationGlobalIndex ||
		identity.Relations[1].IndexID != 41 || identity.Relations[1].Table != "email_claims" {
		t.Fatalf("identity=%+v", identity)
	}
}
