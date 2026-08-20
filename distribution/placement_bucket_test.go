package distribution

import (
	"errors"
	"testing"
)

func TestClusterConfigVirtualBucketAndAffinityValidation(t *testing.T) {
	aligned := fullManifest(t, "tenant_data")
	valid := ClusterConfig{
		Distributions: []DistributionSpec{{
			Name: "tenant_data", Arity: 2, MapperVersion: NativeMapperVersion,
			BucketBits: DefaultVirtualBucketBits,
		}},
		Placements: []TablePlacement{
			{Table: "orders", Distribution: "tenant_data", Columns: []string{"/tenant", "/order"}, TenantPath: "/tenant", AffinityGroup: "commerce"},
			{Table: "items", Distribution: "tenant_data", Columns: []string{"/tenant", "/order"}, TenantPath: "/tenant", AffinityGroup: "commerce"},
		},
		Manifests: []*Manifest{aligned},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	if got := valid.Distributions[0].EffectiveBucketBits(); got != DefaultVirtualBucketBits {
		t.Fatalf("effective bucket bits = %d", got)
	}

	invalidBits := valid
	invalidBits.Distributions = append([]DistributionSpec(nil), valid.Distributions...)
	invalidBits.Distributions[0].BucketBits = MaxVirtualBucketBits + 1
	if err := invalidBits.Validate(); !errors.Is(err, ErrInvalidPlacement) {
		t.Fatalf("invalid bucket bits err = %v", err)
	}

	cut := KeyspacePoint{}
	cut[0] = 0x80
	cut[7] = 1
	misalignedManifest, err := NewManifest("tenant_data", 1, []Shard{
		{ID: "left", AllocationGeneration: 1, Range: KeyRange{End: KeyspaceEnd{Point: cut}}, Leaders: []EndpointID{"a"}},
		{ID: "right", AllocationGeneration: 2, Range: KeyRange{Start: cut, End: KeyspaceEnd{Max: true}}, Leaders: []EndpointID{"b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	misaligned := valid
	misaligned.Manifests = []*Manifest{misalignedManifest}
	if err := misaligned.Validate(); !errors.Is(err, ErrInvalidPlacement) {
		t.Fatalf("misaligned bucket boundary err = %v", err)
	}

	tenantOnly := ClusterConfig{
		Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
		Placements: []TablePlacement{{
			Table: "events", Distribution: "tenant_data", Columns: []string{"/tenant"}, TenantPath: "/tenant",
		}},
		Manifests: []*Manifest{fullManifest(t, "tenant_data")},
	}
	if err := tenantOnly.Validate(); !errors.Is(err, ErrInvalidPlacement) {
		t.Fatalf("tenant-only physical key err = %v", err)
	}

	missingTenantPath := valid
	missingTenantPath.Placements = append([]TablePlacement(nil), valid.Placements...)
	missingTenantPath.Placements[0].TenantPath = "/missing"
	if err := missingTenantPath.Validate(); !errors.Is(err, ErrInvalidPlacement) {
		t.Fatalf("missing tenant path err = %v", err)
	}

	other := fullManifest(t, "other")
	crossDistribution := ClusterConfig{
		Distributions: []DistributionSpec{
			{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion},
			{Name: "other", Arity: 1, MapperVersion: NativeMapperVersion},
		},
		Placements: []TablePlacement{
			{Table: "a", Distribution: "tenant_data", Columns: []string{"/id"}, AffinityGroup: "shared"},
			{Table: "b", Distribution: "other", Columns: []string{"/id"}, AffinityGroup: "shared"},
		},
		Manifests: []*Manifest{fullManifest(t, "tenant_data"), other},
	}
	if err := crossDistribution.Validate(); !errors.Is(err, ErrInvalidPlacement) {
		t.Fatalf("cross-distribution affinity err = %v", err)
	}
}
