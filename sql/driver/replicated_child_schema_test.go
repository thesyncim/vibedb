package driver

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func childSchemaIdentity(t *testing.T, localIndex, globalIndex bool) (ReplicatedShardStoreIdentity, []string, []ReplicatedGlobalIndexRelation) {
	t.Helper()
	binding := testReplicatedBinding(61)
	identity, err := NewReplicatedChildShardStoreIdentity(ShardStoreIdentity{
		Distribution: distribution.DistributionName(binding.Distribution), Shard: distribution.ShardID(binding.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration), LogID: [16]byte{71},
	}, binding, "docs", strings.Repeat("a1", 32), "/id", ReplicatedShardStoreLimits{
		MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20, MaxBatchDocuments: 64, MaxBatchBytes: replicatedMaxBatchBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	var statements []string
	var globals []ReplicatedGlobalIndexRelation
	if localIndex {
		statements = append(statements, "CREATE INDEX by_email ON docs (email)")
		identity.Relations[0].LocalIndexDigest = replicatedLocalIndexDigest([]indexMeta{{Name: "by_email", Paths: []string{"/email"}}})
	}
	if globalIndex {
		statements = append(statements, "CREATE TABLE emails (PRIMARY KEY (key))")
		globals = []ReplicatedGlobalIndexRelation{{Relation: 2, Table: "emails", IndexID: 42,
			Incarnation: 3, LocatorCount: 1, Unique: true, KeyEncoding: ReplicatedRelationKeyCanonicalTuple,
			KeyArity: 1, TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
			BucketBits: distribution.DefaultVirtualBucketBits}}
		g := globals[0]
		identity.RelationCount = 2
		identity.Relations = append(identity.Relations, ReplicatedShardRelationIdentity{
			Relation: g.Relation, Kind: ReplicatedShardRelationGlobalIndex, Table: g.Table,
			Storage: strings.Repeat("b2", 32), Limits: identity.UserLimits, IndexID: g.IndexID,
			Incarnation: g.Incarnation, LocatorCount: g.LocatorCount, Unique: g.Unique, KeyEncoding: g.KeyEncoding,
			KeyArity: g.KeyArity, TupleVersion: g.TupleVersion, MapperVersion: g.MapperVersion, BucketBits: g.BucketBits,
		})
	}
	identity.RelationManifestDigest = replicatedRelationManifestDigest(identity)
	if err := validateReplicatedShardStoreIdentity(identity); err != nil {
		t.Fatal(err)
	}
	return identity, statements, globals
}

func TestReplicatedChildSchemaAuthenticatesSingletonLocalAndGlobal(t *testing.T) {
	for _, indexes := range []struct {
		name          string
		local, global bool
	}{
		{"singleton", false, false}, {"local", true, false}, {"global", false, true}, {"base-local-global", true, true},
	} {
		t.Run(indexes.name, func(t *testing.T) {
			source, statements, globals := childSchemaIdentity(t, indexes.local, indexes.global)
			if err := ValidateReplicatedChildSchema(source, "CREATE TABLE docs (PRIMARY KEY (id))", statements, globals); err != nil {
				t.Fatal(err)
			}
			binding := source.Binding
			binding.Shard, binding.GroupID, binding.ShardIncarnation = "child", [16]byte{81}, [16]byte{82}
			binding.MemberID, binding.StoreID = 3, [16]byte{83}
			storages := []string{strings.Repeat("c3", 32)}
			if indexes.global {
				storages = append(storages, strings.Repeat("d4", 32))
			}
			child, err := NewReplicatedChildShardStoreBundleIdentity(ShardStoreIdentity{
				Distribution: distribution.DistributionName(binding.Distribution), Shard: "child",
				AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration), LogID: [16]byte{84},
			}, binding, source, storages)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateReplicatedChildSchema(child, "CREATE TABLE docs (PRIMARY KEY (id))", statements, globals); err != nil {
				t.Fatal(err)
			}
			for i, relation := range child.Relations {
				want := source.Relations[i]
				want.Storage = storages[i]
				if relation != want {
					t.Fatalf("relation %d schema changed: %+v != %+v", i, relation, want)
				}
			}
			child.Relations[0].Table = "foreign"
			if source.Relations[0].Table != "docs" {
				t.Fatal("child aliases source relation slice")
			}
		})
	}
}

func TestReplicatedChildSchemaRejectsUnboundDDLAndMetadata(t *testing.T) {
	source, statements, globals := childSchemaIdentity(t, true, true)
	const base = "CREATE TABLE docs (PRIMARY KEY (id))"
	for _, test := range []struct {
		name, base string
		statements []string
		globals    []ReplicatedGlobalIndexRelation
	}{
		{"foreign base", "CREATE TABLE other (PRIMARY KEY (id))", statements, globals},
		{"wrong primary", "CREATE TABLE docs (PRIMARY KEY (other))", statements, globals},
		{"typed schema", "CREATE TABLE docs (id STRING PRIMARY KEY)", statements, globals},
		{"if not exists", "CREATE TABLE IF NOT EXISTS docs (PRIMARY KEY (id))", statements, globals},
		{"missing global DDL", base, statements[:1], globals},
		{"missing local DDL", base, statements[1:], globals},
		{"foreign global DDL", base, []string{statements[0], "CREATE TABLE other (PRIMARY KEY (key))"}, globals},
		{"different local path", base, []string{"CREATE INDEX by_email ON docs (other)", statements[1]}, globals},
		{"different local name", base, []string{"CREATE INDEX other ON docs (email)", statements[1]}, globals},
		{"unnamed local index", base, []string{"CREATE INDEX ON docs (email)", statements[1]}, globals},
		{"duplicate local", base, append(slices.Clone(statements), statements[0]), globals},
		{"duplicate global", base, append(slices.Clone(statements), statements[1]), globals},
		{"arbitrary DML", base, append(slices.Clone(statements), "DELETE FROM docs"), globals},
		{"oversized total DDL", base, append(slices.Clone(statements), strings.Repeat(" ", ReplicatedChildSchemaMaxBytes)), globals},
		{"missing global descriptor", base, statements, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateReplicatedChildSchema(source, test.base, test.statements, test.globals); !errors.Is(err, ErrReplicatedShardStoreProfile) {
				t.Fatalf("mismatch accepted: %v", err)
			}
		})
	}
	for _, change := range []func(*ReplicatedGlobalIndexRelation){
		func(g *ReplicatedGlobalIndexRelation) { g.Relation++ }, func(g *ReplicatedGlobalIndexRelation) { g.IndexID++ },
		func(g *ReplicatedGlobalIndexRelation) { g.Incarnation++ }, func(g *ReplicatedGlobalIndexRelation) { g.Unique = false },
		func(g *ReplicatedGlobalIndexRelation) { g.BucketBits-- },
	} {
		foreign := slices.Clone(globals)
		change(&foreign[0])
		if err := ValidateReplicatedChildSchema(source, base, statements, foreign); err == nil {
			t.Fatal("foreign global descriptor accepted")
		}
	}
}

func TestReplicatedChildBundleAllocatorRejectsAliasingAndSchemaDrift(t *testing.T) {
	source, _, _ := childSchemaIdentity(t, true, true)
	local := ShardStoreIdentity{Distribution: distribution.DistributionName(source.Binding.Distribution), Shard: distribution.ShardID(source.Binding.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(source.Binding.AllocationGeneration), LogID: [16]byte{99}}
	for _, storages := range [][]string{nil, {strings.Repeat("c", 64)}, {strings.Repeat("c", 64), strings.Repeat("c", 64)}, {"bad", strings.Repeat("d", 64)}} {
		if _, err := NewReplicatedChildShardStoreBundleIdentity(local, source.Binding, source, storages); err == nil {
			t.Fatal("invalid storage allocation accepted")
		}
	}
	binding := source.Binding
	binding.Authority.SchemaGeneration++
	if _, err := NewReplicatedChildShardStoreBundleIdentity(local, binding, source, []string{strings.Repeat("c", 64), strings.Repeat("d", 64)}); err == nil {
		t.Fatal("schema generation drift accepted")
	}
}
