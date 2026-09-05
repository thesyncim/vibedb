package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/query"
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
			apply := ReplicatedApplyIdentity{
				ValidationProfile: uint8(replicatedstate.ValidationDeterministicMutation),
				ValidationDigest:  replicatedApplyProfileDigest(source, placement),
				Placement:         placement,
			}
			validatedDigest, err := replicatedSchemaManifestValidated(source, apply, indexes)
			if err != nil || validatedDigest != digest {
				t.Fatalf("validated helper differs from public manifest: got=%x want=%x err=%v", validatedDigest, digest, err)
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
			childApply := ReplicatedApplyIdentity{
				ValidationProfile: uint8(replicatedstate.ValidationDeterministicMutation),
				ValidationDigest:  replicatedApplyProfileDigest(child, placement),
				Placement:         placement,
			}
			if validated, validateErr := replicatedSchemaManifestValidated(child, childApply, indexes); validateErr != nil || validated != childDigest {
				t.Fatalf("child validated helper differs: got=%x want=%x err=%v", validated, childDigest, validateErr)
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
			otherApply := ReplicatedApplyIdentity{
				ValidationProfile: uint8(replicatedstate.ValidationDeterministicMutation),
				ValidationDigest:  replicatedApplyProfileDigest(other, placement),
				Placement:         placement,
			}
			if validated, validateErr := replicatedSchemaManifestValidated(other, otherApply, indexes); validateErr != nil || validated != otherDigest {
				t.Fatalf("replica-local validated helper differs: got=%x want=%x err=%v", validated, otherDigest, validateErr)
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

func TestReplicatedSchemaManifestRejectsMalformedAndOversizedIndexes(t *testing.T) {
	source, _, _ := childSchemaIdentity(t, true, false)
	placement := testReplicatedApplyOptions().Placement
	malformed := source
	malformed.Relations = nil
	malformed.RelationCount = 0
	if _, err := ReplicatedSchemaManifest(malformed, placement, nil); err == nil {
		t.Fatal("malformed relation manifest accepted")
	}
	apply := ReplicatedApplyIdentity{
		ValidationProfile: uint8(replicatedstate.ValidationDeterministicMutation),
		ValidationDigest:  replicatedApplyProfileDigest(source, placement), Placement: placement,
	}
	for _, test := range []struct {
		name    string
		indexes []store.IndexDefinition
	}{
		{"missing", nil},
		{"foreign", []store.IndexDefinition{{Name: "by_email", Paths: []string{"/other"}}}},
		{"malformed", []store.IndexDefinition{{Name: "by_email"}}},
		{"oversized", make([]store.IndexDefinition, 4097)},
		{"unique", []store.IndexDefinition{{Name: "by_email", Paths: []string{"/email"}, Unique: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, publicErr := ReplicatedSchemaManifest(source, placement, test.indexes)
			_, helperErr := replicatedSchemaManifestValidated(source, apply, test.indexes)
			if publicErr == nil || helperErr == nil || publicErr.Error() != helperErr.Error() {
				t.Fatalf("index rejection differs: public=%v helper=%v", publicErr, helperErr)
			}
		})
	}
}

func TestReplicatedSchemaManifestPointValidationGuards(t *testing.T) {
	fixture := newReplicatedPointSessionFixture(t, `{"id":"a","value":10}`)
	claim, core := fixture.claim, fixture.claim.database
	base := core.catalog.ReplicatedShardStore
	for _, test := range []struct {
		name       string
		mutate     func()
		validApply bool
	}{
		{"base", func() { base.RelationManifestDigest[0] ^= 1 }, false},
		{"apply", func() { claim.identity.ValidationDigest[0] ^= 1 }, false},
		{"placement", func() { claim.identity.Placement.Format++ }, false},
		{"live manifest", func() {
			claim.identity.Placement.Range.Start = distribution.KeyspacePoint{1}
			claim.identity.ValidationDigest = replicatedApplyProfileDigest(*base, claim.identity.Placement)
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			core.mu.Lock()
			oldBase, oldApply := base.Clone(), claim.identity
			test.mutate()
			validationErr := validateReplicatedApplyIdentity(claim.identity, *base)
			core.mu.Unlock()
			defer func() {
				core.mu.Lock()
				*base, claim.identity = oldBase, oldApply
				core.mu.Unlock()
			}()
			if test.validApply && validationErr != nil {
				t.Fatalf("live-manifest test did not preserve valid apply identity: %v", validationErr)
			}
			reader, err := claim.NewPointReadSession(context.Background(), 1, fixture.key, true, fixture.raw,
				[]byte(fixture.base.UserPrimaryKey), query.ExecOptions{})
			if reader != nil {
				_ = reader.Close()
			}
			if !errors.Is(err, ErrReplicatedApplyMismatch) {
				t.Fatalf("corrupted %s admitted: %v", test.name, err)
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
