package distribution

import (
	"errors"
	"testing"
)

// fullManifest builds a single-shard manifest covering the whole keyspace for
// distribution name, the degenerate one-shard layout the local facade uses.
func fullManifest(tb testing.TB, name DistributionName) *Manifest {
	tb.Helper()
	m, err := NewManifest(name, 1, shardsFromBounds())
	if err != nil {
		tb.Fatalf("NewManifest(%q): %v", name, err)
	}
	return m
}

func TestClusterConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     ClusterConfig
		wantErr bool
	}{
		{
			name: "empty config",
			cfg:  ClusterConfig{},
		},
		{
			name: "valid single table",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
				Placements:    []TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}}},
				Manifests:     []*Manifest{fullManifest(t, "tenant_data")},
			},
		},
		{
			name: "valid colocated tables",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
				Placements: []TablePlacement{
					{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}},
					{Table: "channels", Distribution: "tenant_data", Columns: []string{"/tenant_id"}},
				},
				Manifests: []*Manifest{fullManifest(t, "tenant_data")},
			},
		},
		{
			name: "valid compound shard key",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 2, MapperVersion: NativeMapperVersion}},
				Placements:    []TablePlacement{{Table: "members", Distribution: "tenant_data", Columns: []string{"/tenant_id", "/channel_id"}}},
				Manifests:     []*Manifest{fullManifest(t, "tenant_data")},
			},
		},
		{
			name: "empty distribution name",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "", Arity: 1, MapperVersion: NativeMapperVersion}},
			},
			wantErr: true,
		},
		{
			name: "duplicate distribution name",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{
					{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion},
					{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion},
				},
				Manifests: []*Manifest{fullManifest(t, "tenant_data")},
			},
			wantErr: true,
		},
		{
			name: "arity too small",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 0, MapperVersion: NativeMapperVersion}},
			},
			wantErr: true,
		},
		{
			name: "arity too large",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: KeyspaceWidth + 1, MapperVersion: NativeMapperVersion}},
			},
			wantErr: true,
		},
		{
			name: "manifest for unknown distribution",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
				Manifests:     []*Manifest{fullManifest(t, "tenant_data"), fullManifest(t, "orphan")},
			},
			wantErr: true,
		},
		{
			name: "duplicate manifest for distribution",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
				Manifests:     []*Manifest{fullManifest(t, "tenant_data"), fullManifest(t, "tenant_data")},
			},
			wantErr: true,
		},
		{
			name: "distribution without manifest",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
			},
			wantErr: true,
		},
		{
			name: "nil manifest",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
				Manifests:     []*Manifest{nil},
			},
			wantErr: true,
		},
		{
			name: "empty table name",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
				Placements:    []TablePlacement{{Table: "", Distribution: "tenant_data", Columns: []string{"/tenant_id"}}},
				Manifests:     []*Manifest{fullManifest(t, "tenant_data")},
			},
			wantErr: true,
		},
		{
			name: "duplicate table placement",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
				Placements: []TablePlacement{
					{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}},
					{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}},
				},
				Manifests: []*Manifest{fullManifest(t, "tenant_data")},
			},
			wantErr: true,
		},
		{
			name: "placement references unknown distribution",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
				Placements:    []TablePlacement{{Table: "messages", Distribution: "other", Columns: []string{"/tenant_id"}}},
				Manifests:     []*Manifest{fullManifest(t, "tenant_data")},
			},
			wantErr: true,
		},
		{
			name: "column count disagrees with arity",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 2, MapperVersion: NativeMapperVersion}},
				Placements:    []TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}}},
				Manifests:     []*Manifest{fullManifest(t, "tenant_data")},
			},
			wantErr: true,
		},
		{
			name: "empty shard-key column",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 2, MapperVersion: NativeMapperVersion}},
				Placements:    []TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id", ""}}},
				Manifests:     []*Manifest{fullManifest(t, "tenant_data")},
			},
			wantErr: true,
		},
		{
			name: "repeated shard-key column",
			cfg: ClusterConfig{
				Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 2, MapperVersion: NativeMapperVersion}},
				Placements:    []TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id", "/tenant_id"}}},
				Manifests:     []*Manifest{fullManifest(t, "tenant_data")},
			},
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.cfg.Validate()
			if c.wantErr {
				if err == nil {
					t.Fatalf("Validate() = nil, want error")
				}
				if !errors.Is(err, ErrInvalidPlacement) {
					t.Fatalf("Validate() error = %v, want match for ErrInvalidPlacement", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestClusterConfigLookups(t *testing.T) {
	man := fullManifest(t, "tenant_data")
	cfg := ClusterConfig{
		Distributions: []DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: NativeMapperVersion}},
		Placements:    []TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}}},
		Manifests:     []*Manifest{man},
	}

	if spec, ok := cfg.Spec("tenant_data"); !ok || spec.Arity != 1 {
		t.Fatalf("Spec(tenant_data) = %+v, %v; want arity 1, true", spec, ok)
	}
	if _, ok := cfg.Spec("missing"); ok {
		t.Fatalf("Spec(missing) = _, true; want false")
	}

	if p, ok := cfg.Placement("messages"); !ok || p.Distribution != "tenant_data" {
		t.Fatalf("Placement(messages) = %+v, %v; want distribution tenant_data, true", p, ok)
	}
	if _, ok := cfg.Placement("missing"); ok {
		t.Fatalf("Placement(missing) = _, true; want false")
	}

	if m, ok := cfg.Manifest("tenant_data"); !ok || m != man {
		t.Fatalf("Manifest(tenant_data) = %p, %v; want %p, true", m, ok, man)
	}
	if _, ok := cfg.Manifest("missing"); ok {
		t.Fatalf("Manifest(missing) = _, true; want false")
	}
}
