package distribution

import (
	"strconv"
	"strings"
)

// DistributionSpec is the immutable identity of a logical keyspace: its name,
// the shard-key column count (arity), and the frozen mapper revision routing it.
type DistributionSpec struct {
	Name          DistributionName
	Arity         int
	MapperVersion MapperVersion
	// BucketBits is the high-bit width of the virtual bucket space. Zero selects
	// DefaultVirtualBucketBits; an explicit value is immutable placement identity.
	BucketBits uint8
}

// EffectiveBucketBits returns the virtual-bucket width used by the mapper.
func (s DistributionSpec) EffectiveBucketBits() uint8 {
	if s.BucketBits == 0 {
		return DefaultVirtualBucketBits
	}
	return s.BucketBits
}

// TablePlacement binds a table to a distribution over an ordered shard-key
// column list. Columns are canonical JSON-pointer spellings ("/tenant_id") in
// significant order, matching the driver's column identity.
type TablePlacement struct {
	Table        string
	Distribution DistributionName
	Columns      []string
	// TenantPath marks a tenant-scoped table. It must be one placement column,
	// but never the only column: tenant identity cannot be the physical shard key.
	TenantPath string
	// AffinityGroup explicitly names tables intended to share placement tuples
	// for colocated joins and transactions. Empty retains the distribution-wide
	// legacy colocation contract.
	AffinityGroup string
}

// ClusterConfig is the static placement metadata a local cluster facade
// consumes: the distributions it routes, the tables placed on them, and one
// immutable manifest per distribution. It is pure data with no catalog
// persistence, validated by Validate.
type ClusterConfig struct {
	Distributions []DistributionSpec
	Placements    []TablePlacement
	Manifests     []*Manifest
}

// Validate reports the first way the configuration is internally inconsistent as
// a *PlacementError (matching ErrInvalidPlacement): a distribution has an empty
// or duplicate name or an arity outside 1..KeyspaceWidth; a table placement is
// empty, duplicated, references an unknown distribution, or lists a column count
// that disagrees with the distribution's arity, or has an empty or repeated
// column; a manifest routes an unknown distribution, more than one manifest
// routes the same distribution, or a distribution has no manifest. It is pure
// metadata validation and does not check the unique-key locality invariant,
// which needs the catalog.
func (c ClusterConfig) Validate() error {
	specs := make(map[DistributionName]DistributionSpec, len(c.Distributions))
	for _, spec := range c.Distributions {
		if spec.Name == "" {
			return &PlacementError{Reason: "distribution has an empty name"}
		}
		if _, dup := specs[spec.Name]; dup {
			return &PlacementError{Reason: "duplicate distribution " + string(spec.Name)}
		}
		if spec.Arity < 1 || spec.Arity > KeyspaceWidth {
			return &PlacementError{Reason: "distribution " + string(spec.Name) + " has arity " + strconv.Itoa(spec.Arity) + " outside 1.." + strconv.Itoa(KeyspaceWidth)}
		}
		if bits := spec.BucketBits; bits != 0 && !ValidVirtualBucketBits(bits) {
			return &PlacementError{Reason: "distribution " + string(spec.Name) + " has virtual bucket bits " + strconv.Itoa(int(bits)) + " outside " + strconv.Itoa(int(MinVirtualBucketBits)) + ".." + strconv.Itoa(int(MaxVirtualBucketBits))}
		}
		specs[spec.Name] = spec
	}

	manifests := make(map[DistributionName]struct{}, len(c.Manifests))
	for _, m := range c.Manifests {
		if m == nil {
			return &PlacementError{Reason: "manifest is nil"}
		}
		d := m.Distribution()
		if _, ok := specs[d]; !ok {
			return &PlacementError{Reason: "manifest routes unknown distribution " + string(d)}
		}
		if _, dup := manifests[d]; dup {
			return &PlacementError{Reason: "more than one manifest for distribution " + string(d)}
		}
		// Explicit bucket metadata requires physical ownership boundaries to be
		// whole-bucket boundaries. Zero-valued metadata remains loadable while
		// being interpreted by the mapper with the current default.
		if spec := specs[d]; spec.BucketBits != 0 {
			for i := 0; i < m.ShardCount(); i++ {
				shard, _ := m.ShardMetadataAt(i)
				if !VirtualBucketBoundary(shard.Range.Start, spec.BucketBits) {
					return &PlacementError{Reason: "distribution " + string(d) + " shard " + string(shard.ID) + " starts inside a virtual bucket"}
				}
				if !shard.Range.End.Max && !VirtualBucketBoundary(shard.Range.End.Point, spec.BucketBits) {
					return &PlacementError{Reason: "distribution " + string(d) + " shard " + string(shard.ID) + " ends inside a virtual bucket"}
				}
			}
		}
		manifests[d] = struct{}{}
	}
	for _, spec := range c.Distributions {
		if _, ok := manifests[spec.Name]; !ok {
			return &PlacementError{Reason: "distribution " + string(spec.Name) + " has no manifest"}
		}
	}

	tables := make(map[string]struct{}, len(c.Placements))
	type affinityIdentity struct {
		distribution  DistributionName
		tenantOrdinal int
	}
	affinity := make(map[string]affinityIdentity)
	for _, p := range c.Placements {
		if p.Table == "" {
			return &PlacementError{Reason: "table placement has an empty table name"}
		}
		if _, dup := tables[p.Table]; dup {
			return &PlacementError{Reason: "more than one placement for table " + p.Table}
		}
		tables[p.Table] = struct{}{}
		spec, ok := specs[p.Distribution]
		if !ok {
			return &PlacementError{Reason: "table " + p.Table + " references unknown distribution " + string(p.Distribution)}
		}
		if len(p.Columns) != spec.Arity {
			return &PlacementError{Reason: "table " + p.Table + " has " + strconv.Itoa(len(p.Columns)) + " shard-key columns but distribution " + string(spec.Name) + " has arity " + strconv.Itoa(spec.Arity)}
		}
		seen := make(map[string]struct{}, len(p.Columns))
		tenantOrdinal := -1
		for ordinal, col := range p.Columns {
			if col == "" {
				return &PlacementError{Reason: "table " + p.Table + " has an empty shard-key column"}
			}
			if _, dup := seen[col]; dup {
				return &PlacementError{Reason: "table " + p.Table + " repeats shard-key column " + col}
			}
			seen[col] = struct{}{}
			if col == p.TenantPath {
				tenantOrdinal = ordinal
			}
		}
		if p.TenantPath != "" {
			if tenantOrdinal < 0 {
				return &PlacementError{Reason: "table " + p.Table + " tenant path is not part of its placement tuple"}
			}
			if len(p.Columns) < 2 {
				return &PlacementError{Reason: "table " + p.Table + " uses tenant identity as its complete physical shard key"}
			}
		}
		if p.AffinityGroup != "" {
			if len(p.AffinityGroup) > 128 || strings.TrimSpace(p.AffinityGroup) != p.AffinityGroup || strings.IndexByte(p.AffinityGroup, 0) >= 0 {
				return &PlacementError{Reason: "table " + p.Table + " has an invalid affinity group"}
			}
			identity := affinityIdentity{distribution: p.Distribution, tenantOrdinal: tenantOrdinal}
			if prior, exists := affinity[p.AffinityGroup]; exists && prior != identity {
				return &PlacementError{Reason: "affinity group " + p.AffinityGroup + " has inconsistent distribution or tenant ordinal"}
			}
			affinity[p.AffinityGroup] = identity
		}
	}
	return nil
}

// Spec returns the distribution spec named name and reports whether it exists.
func (c ClusterConfig) Spec(name DistributionName) (DistributionSpec, bool) {
	for _, spec := range c.Distributions {
		if spec.Name == name {
			return spec, true
		}
	}
	return DistributionSpec{}, false
}

// Placement returns the placement for table and reports whether it exists.
func (c ClusterConfig) Placement(table string) (TablePlacement, bool) {
	for _, p := range c.Placements {
		if p.Table == table {
			return p, true
		}
	}
	return TablePlacement{}, false
}

// Manifest returns the manifest routing distribution name and reports whether it
// exists.
func (c ClusterConfig) Manifest(name DistributionName) (*Manifest, bool) {
	for _, m := range c.Manifests {
		if m.Distribution() == name {
			return m, true
		}
	}
	return nil, false
}
