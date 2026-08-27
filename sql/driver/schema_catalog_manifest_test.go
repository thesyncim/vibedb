package driver

import (
	"bytes"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

// This guard needs no Linux-only sealed journals: the catalog and live apply
// must authenticate the same schema before any image is opened or certified.
func TestReplicatedSchemaCatalogUsesExactMachineManifest(t *testing.T) {
	for _, shape := range []struct {
		name          string
		local, global bool
	}{
		{"singleton", false, false}, {"local", true, false}, {"global", false, true}, {"base-local-global", true, true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			catalog, reserved := childReservationCatalogFixture(t)
			base := catalog.ReplicatedShardStore
			if shape.local {
				catalog.Tables[base.UserTable].Indexes = []indexMeta{{Name: "by_email", Paths: []string{"/email"}}}
				base.Relations[0].LocalIndexDigest = replicatedLocalIndexDigest(catalog.Tables[base.UserTable].Indexes)
			}
			if shape.global {
				schema, _, _ := childSchemaIdentity(t, false, true)
				global := schema.Relations[1]
				global.Storage = strings.Repeat("c", storageIdentityBytes*2)
				global.Limits = base.UserLimits
				base.Relations = append(base.Relations, global)
				base.RelationCount++
				meta := *catalog.Tables[base.UserTable]
				meta.PrimaryKey = "/key"
				meta.Storage = global.Storage
				meta.Indexes = nil
				catalog.Tables[global.Table] = &meta
			}
			for generation := 0; generation < 2; generation++ {
				if generation != 0 {
					base.Binding.Authority.SchemaGeneration++
					base.RelationSchemaGeneration++
				}
				base.RelationManifestDigest = replicatedRelationManifestDigest(*base)
				apply, err := NewReplicatedChildApplyIdentity(*base, reserved.Storage, reserved.CaptureStorage, testReplicatedApplyOptions())
				if err != nil {
					t.Fatal(err)
				}
				meta := replicatedApplyMetaFromIdentity(apply)
				catalog.ReplicatedApply = &meta
				catalog.ReplicatedChildApply = nil
				raw, err := appendCatalogJSON(nil, catalog)
				if err != nil {
					t.Fatal(err)
				}
				decoded, image, err := openReplicatedSchemaCatalogImage(raw)
				if err != nil {
					t.Fatal(err)
				}
				want, err := ReplicatedSchemaManifest(*base, apply.Placement, replicatedApplyLocalIndexes(&table{meta: catalog.Tables[base.UserTable]}))
				if err != nil || image.RelationManifestDigest != want || image.LocalRelationManifestDigest != base.RelationManifestDigest {
					t.Fatalf("schema domains got=%x want=%x err=%v", image.RelationManifestDigest, want, err)
				}
				for _, foreign := range [][32]byte{base.RelationManifestDigest, replicatedRelationApplyManifestDigest(*base)} {
					if foreign == want || image.MatchesRolloutTarget(image.Bytes, image.Digest, image.SchemaGeneration, foreign) {
						t.Fatal("SQL descriptor digest accepted as serving schema")
					}
				}
				again, err := appendCatalogJSON(nil, decoded)
				if err != nil || !bytes.Equal(again, raw) {
					t.Fatalf("catalog roundtrip changed: %v", err)
				}
				catalog.ReplicatedApply.Placement.Range.Start = distribution.KeyspacePoint{0x80}
				catalog.ReplicatedApply.ValidationDigest = replicatedApplyProfileDigest(*base, catalog.ReplicatedApply.Placement)
				narrowRaw, err := appendCatalogJSON(nil, catalog)
				if err != nil {
					t.Fatal(err)
				}
				narrow, err := ValidateReplicatedSchemaCatalogImage(narrowRaw)
				if err != nil || narrow.RelationManifestDigest == want {
					t.Fatalf("placement omitted from schema: %v", err)
				}
			}
		})
	}
}
