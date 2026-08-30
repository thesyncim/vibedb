package driver

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/store"
)

func TestReplicatedSchemaManifestSeparatesSourceChildAndLocalIdentities(t *testing.T) {
	for _, shape := range []struct {
		name          string
		local, global bool
	}{
		{"singleton", false, false}, {"local", true, false}, {"base-local-global", true, true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			source, _, _ := childSchemaIdentity(t, shape.local, shape.global)
			var indexes []store.IndexDefinition
			if shape.local {
				indexes = []store.IndexDefinition{{Name: "by_email", Paths: []string{"/email"}}}
			}
			placement := testReplicatedApplyOptions().Placement
			digest, err := ReplicatedSchemaManifest(source, placement, indexes)
			if err != nil {
				t.Fatal(err)
			}
			if digest == source.RelationManifestDigest {
				t.Fatal("machine and replica-local SQL domains conflated")
			}
			if !shape.global {
				initial, _, err := InitialReplicatedRelationManifest(source.Binding, placement, InitialReplicatedRelationSchema{
					Table: source.UserTable, PrimaryKey: source.UserPrimaryKey, Limits: source.UserLimits, LocalIndexes: indexes})
				if err != nil || initial != digest {
					t.Fatalf("existing singleton manifest differs: %v", err)
				}
			}
			binding := source.Binding
			binding.Shard, binding.ShardIncarnation, binding.GroupID = "child", [16]byte{0x51}, [16]byte{0x52}
			binding.AllocationGeneration++
			binding.Authority.RoutingVersion++
			binding.Authority.RouteGeneration++
			storages := []string{strings.Repeat("c", 64)}
			if shape.global {
				storages = append(storages, strings.Repeat("d", 64))
			}
			child, err := NewReplicatedChildShardStoreBundleIdentity(ShardStoreIdentity{Distribution: distribution.DistributionName(binding.Distribution),
				Shard: "child", AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration), LogID: [16]byte{0x53}}, binding, source, storages)
			if err != nil {
				t.Fatal(err)
			}
			placement.Range.Start = distribution.KeyspacePoint{0x80}
			childDigest, err := ReplicatedSchemaManifest(child, placement, indexes)
			if err != nil || childDigest == digest || childDigest == child.RelationManifestDigest {
				t.Fatalf("child serving domain not distinct: %v", err)
			}
			logicalSource, err := ReplicatedRelationManifestDigest(source)
			if err != nil {
				t.Fatal(err)
			}
			logicalChild, err := ReplicatedRelationManifestDigest(child)
			if err != nil || logicalSource != logicalChild {
				t.Fatalf("split changed logical relation schema: %v", err)
			}
			other := child.Clone()
			other.Binding.MemberID++
			other.Binding.StoreID[0]++
			other.LogID[0]++
			other.UserStorage = strings.Repeat("e", 64)
			other.Relations[0].Storage = other.UserStorage
			other.RelationManifestDigest = replicatedRelationManifestDigest(other)
			otherDigest, err := ReplicatedSchemaManifest(other, placement, indexes)
			if err != nil || otherDigest != childDigest {
				t.Fatalf("replica-local identity changed serving schema: %v", err)
			}
			if shape.local {
				if _, err := ReplicatedSchemaManifest(source, placement, nil); err == nil {
					t.Fatal("missing exact local indexes accepted")
				}
				if _, err := ReplicatedSchemaManifest(source, placement, []store.IndexDefinition{{Name: "by_email", Paths: []string{"/other"}}}); err == nil {
					t.Fatal("foreign local index accepted")
				}
			}
		})
	}
}

func TestReplicatedSchemaManifestRejectsLocalUniqueIndex(t *testing.T) {
	source, _, _ := childSchemaIdentity(t, true, false)
	_, err := ReplicatedSchemaManifest(
		source, testReplicatedApplyOptions().Placement,
		[]store.IndexDefinition{{
			Name: "by_email", Paths: []string{"/email"}, Unique: true,
		}},
	)
	if !errors.Is(err, ErrReplicatedShardStoreProfile) {
		t.Fatalf("serving manifest unique index = %v, want %v",
			err, ErrReplicatedShardStoreProfile)
	}
}
