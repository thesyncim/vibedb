package driver

import (
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func testReplicatedGlobalRelationPlacement(unique bool) ReplicatedShardRelationIdentity {
	return ReplicatedShardRelationIdentity{
		Relation: 2, Kind: ReplicatedShardRelationGlobalIndex,
		IndexID: 91, Incarnation: 7, LocatorCount: 2, Unique: unique,
		KeyEncoding: ReplicatedRelationKeyCanonicalTuple, KeyArity: 2,
		TupleVersion:  distribution.CurrentTupleVersion,
		MapperVersion: distribution.NativeMapperVersion,
		BucketBits:    17,
	}
}

func TestReplicatedGlobalRelationMapsStoredKeysWithoutDecode(t *testing.T) {
	key := []distribution.Scalar{
		distribution.NewString("tenant-a"),
		distribution.NewString("email@example.test"),
	}
	locatorNumber, err := distribution.NewNumber("42.0")
	if err != nil {
		t.Fatal(err)
	}
	locator := []distribution.Scalar{distribution.NewString("tenant-a"), locatorNumber}
	keyTuple, err := distribution.CurrentTupleCodec.AppendTuple(nil, key)
	if err != nil {
		t.Fatal(err)
	}
	locatorTuple, err := distribution.CurrentTupleCodec.AppendTuple(nil, locator)
	if err != nil {
		t.Fatal(err)
	}
	nonUniqueKey := append(append([]byte(nil), keyTuple...), locatorTuple...)
	want, err := distribution.NewNativeMapperWithBucketBits(2, 17).PointFor(key)
	if err != nil {
		t.Fatal(err)
	}

	nonUnique := testReplicatedGlobalRelationPlacement(false)
	if got, ok := nonUnique.GlobalIndexStorageKeyPoint(nonUniqueKey); !ok || got != want {
		t.Fatalf("non-unique stored-key point = %x,%v, want %x", got, ok, want)
	}
	unique := testReplicatedGlobalRelationPlacement(true)
	if got, ok := unique.GlobalIndexStorageKeyPoint(keyTuple); !ok || got != want {
		t.Fatalf("unique stored-key point = %x,%v, want %x", got, ok, want)
	}
	if _, ok := unique.GlobalIndexStorageKeyPoint(nonUniqueKey); ok {
		t.Fatal("unique relation accepted an appended locator tuple")
	}
	if _, ok := nonUnique.GlobalIndexStorageKeyPoint(keyTuple); ok {
		t.Fatal("non-unique relation accepted a missing locator tuple")
	}
	if _, ok := nonUnique.GlobalIndexStorageKeyPoint(nonUniqueKey[:len(nonUniqueKey)-1]); ok {
		t.Fatal("non-unique relation accepted a truncated locator tuple")
	}

	for name, mutate := range map[string]func(*ReplicatedShardRelationIdentity){
		"kind":     func(r *ReplicatedShardRelationIdentity) { r.Kind = ReplicatedShardRelationJSON },
		"encoding": func(r *ReplicatedShardRelationIdentity) { r.KeyEncoding++ },
		"arity":    func(r *ReplicatedShardRelationIdentity) { r.KeyArity = 0 },
		"tuple":    func(r *ReplicatedShardRelationIdentity) { r.TupleVersion++ },
		"mapper":   func(r *ReplicatedShardRelationIdentity) { r.MapperVersion++ },
		"buckets":  func(r *ReplicatedShardRelationIdentity) { r.BucketBits = 7 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := nonUnique
			mutate(&candidate)
			if _, ok := candidate.GlobalIndexStorageKeyPoint(nonUniqueKey); ok {
				t.Fatal("stale or malformed placement metadata mapped a stored key")
			}
		})
	}
}

func TestReplicatedGlobalRelationStoredKeyMappingAllocations(t *testing.T) {
	relation := testReplicatedGlobalRelationPlacement(false)
	key, err := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{
		distribution.NewString("tenant-a"), distribution.NewString("email@example.test"),
		distribution.NewString("tenant-a"), distribution.NewString("row-7"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_, _ = relation.GlobalIndexStorageKeyPoint(key)
	}); got != 0 {
		t.Fatalf("stored global-index key mapping allocations = %v", got)
	}
}

func TestReplicatedGlobalPlacementChangesPortableManifest(t *testing.T) {
	identity := ReplicatedShardStoreIdentity{
		RelationCount: 2, RelationSchemaGeneration: 11,
		Relations: []ReplicatedShardRelationIdentity{
			{Relation: 1, Kind: ReplicatedShardRelationJSON},
			testReplicatedGlobalRelationPlacement(false),
		},
	}
	baseline := replicatedRelationApplyManifestDigest(identity)
	for name, mutate := range map[string]func(*ReplicatedShardRelationIdentity){
		"encoding": func(r *ReplicatedShardRelationIdentity) { r.KeyEncoding++ },
		"arity":    func(r *ReplicatedShardRelationIdentity) { r.KeyArity++ },
		"tuple":    func(r *ReplicatedShardRelationIdentity) { r.TupleVersion++ },
		"mapper":   func(r *ReplicatedShardRelationIdentity) { r.MapperVersion++ },
		"buckets":  func(r *ReplicatedShardRelationIdentity) { r.BucketBits++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := identity.Clone()
			mutate(&candidate.Relations[1])
			if replicatedRelationApplyManifestDigest(candidate) == baseline {
				t.Fatal("placement change did not alter portable manifest")
			}
		})
	}
}
