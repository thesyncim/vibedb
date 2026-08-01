package distribution

import "strconv"

// DistributionSpec is the immutable identity of a logical keyspace: its name,
// the shard-key column count (arity), and the frozen mapper revision routing it.
type DistributionSpec struct {
	Name          DistributionName
	Arity         int
	MapperVersion MapperVersion
}

// TablePlacement binds a table to a distribution over an ordered shard-key
// column list. Columns are canonical JSON-pointer spellings ("/tenant_id") in
// significant order, matching the driver's column identity.
type TablePlacement struct {
	Table        string
	Distribution DistributionName
	Columns      []string
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
		specs[spec.Name] = spec
	}

	manifests := make(map[DistributionName]struct{}, len(c.Manifests))
	for _, m := range c.Manifests {
		d := m.Distribution()
		if _, ok := specs[d]; !ok {
			return &PlacementError{Reason: "manifest routes unknown distribution " + string(d)}
		}
		if _, dup := manifests[d]; dup {
			return &PlacementError{Reason: "more than one manifest for distribution " + string(d)}
		}
		manifests[d] = struct{}{}
	}
	for _, spec := range c.Distributions {
		if _, ok := manifests[spec.Name]; !ok {
			return &PlacementError{Reason: "distribution " + string(spec.Name) + " has no manifest"}
		}
	}

	tables := make(map[string]struct{}, len(c.Placements))
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
		for _, col := range p.Columns {
			if col == "" {
				return &PlacementError{Reason: "table " + p.Table + " has an empty shard-key column"}
			}
			if _, dup := seen[col]; dup {
				return &PlacementError{Reason: "table " + p.Table + " repeats shard-key column " + col}
			}
			seen[col] = struct{}{}
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
