package driver

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestInitialReplicatedRelationManifestMatchesServingIdentity(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			var first [sha256.Size]byte
			for member := uint64(1); member <= 3; member++ {
				_, database, binding, _ := prepareReplicatedTestRoot(t, fmt.Sprintf("manifest-%d", member), false)
				defer database.Close()
				binding.MemberID = member
				binding.StoreID[0] += byte(member)
				schema := InitialReplicatedRelationSchema{Table: "docs", PrimaryKey: "/id"}
				if indexed {
					session, err := database.NewSession(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					err = testRuntimeExec(session, "CREATE INDEX by_email ON docs (email)", nil)
					closeErr := session.Close()
					if err != nil || closeErr != nil {
						t.Fatalf("index schema: %v %v", err, closeErr)
					}
					schema.LocalIndexes = []store.IndexDefinition{{Name: "by_email", Paths: []string{"/email"}}}
				}
				options := testReplicatedApplyOptions()
				digest, limits, err := InitialReplicatedRelationManifest(binding, options.Placement, schema)
				if err != nil {
					t.Fatal(err)
				}
				identity := requireReplicatedShardStoreBind(t, database, binding, "docs")
				claim, _, err := database.OpenReplicatedApply(identity, testReplicatedApplyBootstrap(), options)
				if err != nil {
					t.Fatal(err)
				}
				defer claim.Close()
				if _, err = claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
					t.Fatal(err)
				}
				profile, err := claim.CapacityQualificationProfile()
				if err != nil || digest == ([sha256.Size]byte{}) || digest != profile.RelationManifestDigest || limits != identity.UserLimits {
					t.Fatalf("member %d initial=%x live=%x limits=%+v err=%v", member, digest, profile.RelationManifestDigest, limits, err)
				}
				if member == 1 {
					first = digest
				} else if digest != first {
					t.Fatal("member-local identities entered the machine digest")
				}
			}
		})
	}
}

func TestInitialReplicatedRelationManifestPureContract(t *testing.T) {
	binding := testReplicatedBinding(31)
	placement := testReplicatedApplyOptions().Placement
	schema := InitialReplicatedRelationSchema{Table: "docs", PrimaryKey: "/id"}
	digest, limits, err := InitialReplicatedRelationManifest(binding, placement, schema)
	if err != nil || digest == ([sha256.Size]byte{}) {
		t.Fatalf("initial schema: %x %v", digest, err)
	}
	for member := uint64(1); member <= 3; member++ {
		local := binding
		local.MemberID = member
		local.StoreID[0] += byte(member)
		got, gotLimits, err := InitialReplicatedRelationManifest(local, placement, schema)
		if err != nil || got != digest || gotLimits != limits {
			t.Fatalf("replica %d: %x %v", member, got, err)
		}
	}
	tests := []struct {
		name    string
		invalid bool
		change  func(*ReplicatedShardStoreBinding, *ReplicatedPlacementProfile, *InitialReplicatedRelationSchema)
	}{
		{"schema_generation", false, func(b *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, _ *InitialReplicatedRelationSchema) {
			b.Authority.SchemaGeneration++
		}},
		{"routing", false, func(b *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, _ *InitialReplicatedRelationSchema) {
			b.Authority.RoutingVersion++
		}},
		{"route_generation", false, func(b *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, _ *InitialReplicatedRelationSchema) {
			b.Authority.RouteGeneration++
		}},
		{"allocation", false, func(b *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, _ *InitialReplicatedRelationSchema) {
			b.AllocationGeneration++
		}},
		{"table", false, func(_ *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			s.Table = "other"
		}},
		{"primary", false, func(_ *ReplicatedShardStoreBinding, p *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			p.ShardKey = "/home"
			s.PrimaryKey = "/home"
		}},
		{"range", false, func(_ *ReplicatedShardStoreBinding, p *ReplicatedPlacementProfile, _ *InitialReplicatedRelationSchema) {
			p.Range.Start[0] = 1
		}},
		{"limits", false, func(_ *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			s.Limits = limits
			s.Limits.MaxKeyBytes--
		}},
		{"index", false, func(_ *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			s.LocalIndexes = []store.IndexDefinition{{Name: "by_email", Paths: []string{"/email"}}}
		}},
		{"missing_binding", true, func(b *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, _ *InitialReplicatedRelationSchema) {
			*b = ReplicatedShardStoreBinding{}
		}},
		{"missing_generation", true, func(b *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, _ *InitialReplicatedRelationSchema) {
			b.Authority.SchemaGeneration = 0
		}},
		{"empty_table", true, func(_ *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			s.Table = ""
		}},
		{"nul_table", true, func(_ *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			s.Table = "docs\x00other"
		}},
		{"oversized_table", true, func(_ *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			s.Table = strings.Repeat("x", 1<<20)
		}},
		{"wrong_primary", true, func(_ *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			s.PrimaryKey = "/home"
		}},
		{"noncanonical_pointer", true, func(_ *ReplicatedShardStoreBinding, p *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			p.ShardKey = "id"
			s.PrimaryKey = "id"
		}},
		{"partial_limits", true, func(_ *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			s.Limits.MaxKeyBytes = 1
		}},
		{"duplicate_index", true, func(_ *ReplicatedShardStoreBinding, _ *ReplicatedPlacementProfile, s *InitialReplicatedRelationSchema) {
			s.LocalIndexes = []store.IndexDefinition{{Name: "a", Paths: []string{"/a"}}, {Name: "a", Paths: []string{"/b"}}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, p, s := binding, placement, schema
			test.change(&b, &p, &s)
			got, gotLimits, err := InitialReplicatedRelationManifest(b, p, s)
			if test.invalid {
				if err == nil || got != ([sha256.Size]byte{}) || gotLimits != (ReplicatedShardStoreLimits{}) {
					t.Fatalf("accepted invalid contract: %x %+v %v", got, gotLimits, err)
				}
			} else if err != nil || got == digest {
				t.Fatalf("changed contract=%x err=%v", got, err)
			}
		})
	}
}
