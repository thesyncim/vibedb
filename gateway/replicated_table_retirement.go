package gateway

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

var replicatedTableRetirementProofDomain = []byte("vibedb/replicated-table-retirement-proof/1\x00")

// replicatedTableRetirement is the bounded, single-operation witness that lets
// every catalog reader distinguish an authorized table lifecycle cut from a
// corrupt snapshot that merely lost routing metadata. It blocks all other
// catalog transitions until the supervisor confirms durable inventory cleanup;
// distribution identities allocated for provisioned tables are never reused.
type replicatedTableRetirement struct {
	Operation        [32]byte
	Proof            [32]byte
	SourceGeneration uint64
	Table            string
	Distribution     distribution.DistributionName
}

func (r replicatedTableRetirement) valid() bool {
	return r.Operation != ([32]byte{}) && r.Proof != ([32]byte{}) &&
		r.SourceGeneration != 0 && r.SourceGeneration != math.MaxUint64 &&
		r.Table != "" && len(r.Table) <= 63 && r.Distribution != ""
}

type persistedReplicatedTableRetirement struct {
	Operation        string `json:"operation"`
	Proof            string `json:"proof"`
	SourceGeneration uint64 `json:"source_generation"`
	Table            string `json:"table"`
	Distribution     string `json:"distribution"`
}

func persistedReplicatedTableRetirementFromSnapshot(s *Snapshot) *persistedReplicatedTableRetirement {
	if s == nil || s.tableRetirement == nil {
		return nil
	}
	r := s.tableRetirement
	return &persistedReplicatedTableRetirement{
		Operation: hex.EncodeToString(r.Operation[:]), Proof: hex.EncodeToString(r.Proof[:]),
		SourceGeneration: r.SourceGeneration, Table: r.Table, Distribution: string(r.Distribution),
	}
}

func (s *Snapshot) attachPersistedReplicatedTableRetirement(p *persistedReplicatedTableRetirement) error {
	if p == nil {
		return nil
	}
	if s == nil {
		return ErrInvalidCatalog
	}
	r := replicatedTableRetirement{
		SourceGeneration: p.SourceGeneration, Table: p.Table,
		Distribution: distribution.DistributionName(p.Distribution),
	}
	if err := decodeFixed32Hex(p.Operation, &r.Operation); err != nil {
		return &CatalogError{Reason: "table retirement operation: " + err.Error()}
	}
	if err := decodeFixed32Hex(p.Proof, &r.Proof); err != nil {
		return &CatalogError{Reason: "table retirement proof: " + err.Error()}
	}
	if !r.valid() || s.Generation() != r.SourceGeneration+1 {
		return &CatalogError{Reason: "table retirement witness is invalid"}
	}
	if _, found := s.Placement(r.Table); found {
		return &CatalogError{Reason: "retired table remains placed"}
	}
	if _, found := s.Spec(r.Distribution); found {
		return &CatalogError{Reason: "retired table distribution remains active"}
	}
	s.tableRetirement = &r
	return nil
}

// buildReplicatedTableRetirementTarget constructs the exact catalog image for
// one independently provisioned table. Shared distributions fail closed: they
// require a relation-level protocol rather than topology deprovisioning.
func buildReplicatedTableRetirementTarget(current *Snapshot, retirement replicatedTableRetirement) (*Snapshot, error) {
	if current == nil || !retirement.valid() || retirement.SourceGeneration != current.Generation() ||
		current.Generation() == math.MaxUint64 {
		return nil, ErrInvalidCatalog
	}
	placement, found := current.Placement(retirement.Table)
	if !found || placement.Distribution != retirement.Distribution {
		return nil, ErrInvalidCatalog
	}
	placementCount := 0
	for _, candidate := range current.config.Placements {
		if candidate.Distribution == retirement.Distribution {
			placementCount++
			if candidate.Table != retirement.Table {
				return nil, &CatalogError{Reason: "table retirement cannot remove a shared distribution"}
			}
		}
	}
	if placementCount != 1 {
		return nil, ErrInvalidCatalog
	}
	descriptorCount, profileCount := 0, 0
	for _, descriptor := range current.replicatedDescriptors() {
		if descriptor.Distribution == retirement.Distribution {
			descriptorCount++
		}
	}
	for _, profile := range current.replicatedTableProfiles() {
		if profile.Table == retirement.Table {
			profileCount++
			continue
		}
		if other, ok := current.Placement(profile.Table); ok && other.Distribution == retirement.Distribution {
			return nil, &CatalogError{Reason: "table retirement distribution owns another replicated relation"}
		}
	}
	if descriptorCount == 0 || profileCount != 1 {
		return nil, ErrInvalidCatalog
	}
	if ledger, ok := current.DurableRequestLedgerTopology(); ok {
		for _, value := range ledger.Ranges {
			if value.Route.Distribution == retirement.Distribution {
				return nil, &CatalogError{Reason: "table retirement cannot remove request-ledger topology"}
			}
		}
	}

	config := cloneConfig(current.config)
	config.Distributions = slices.DeleteFunc(config.Distributions, func(value distribution.DistributionSpec) bool {
		return value.Name == retirement.Distribution
	})
	config.Placements = slices.DeleteFunc(config.Placements, func(value distribution.TablePlacement) bool {
		return value.Table == retirement.Table
	})
	config.Manifests = slices.DeleteFunc(config.Manifests, func(value *distribution.Manifest) bool {
		return value.Distribution() == retirement.Distribution
	})
	indexes := slices.DeleteFunc(current.indexDescriptors(), func(value IndexDescriptor) bool {
		return value.Table == retirement.Table
	})
	statistics := slices.DeleteFunc(current.statistics.Descriptors(), func(value TableStatistics) bool {
		return value.Table == retirement.Table
	})
	descriptors := slices.DeleteFunc(current.replicatedDescriptors(), func(value ReplicatedShardDescriptor) bool {
		return value.Distribution == retirement.Distribution
	})
	profiles := slices.DeleteFunc(current.replicatedTableProfiles(), func(value ReplicatedTableProfile) bool {
		return value.Table == retirement.Table
	})
	declarations := slices.DeleteFunc(current.ReplicatedTableDeclarations(), func(value ReplicatedTableDeclaration) bool {
		return value.Table == retirement.Table
	})
	target, err := NewSnapshotWithReplicatedTableMetadata(
		config, current.endpoints, current.Generation()+1, indexes, statistics,
		descriptors, profiles, declarations,
	)
	if err != nil {
		return nil, err
	}
	target.tableRetirement = &retirement
	return target, nil
}

func advanceCatalogStateTableRetirement(current, next *Snapshot) (*Snapshot, error) {
	if current == nil || next == nil || next.tableRetirement == nil {
		return nil, ErrInvalidCatalog
	}
	state, err := initialCatalogState(current)
	if err != nil {
		return nil, err
	}
	expected, err := buildReplicatedTableRetirementTarget(state, *next.tableRetirement)
	if err != nil {
		return nil, err
	}
	indexHighWater, err := advanceIndexIDHighWater(state, expected)
	if err != nil {
		return nil, err
	}
	shardHighWaters, err := advanceShardGenerationHighWaters(state, expected)
	if err != nil {
		return nil, err
	}
	expected = snapshotWithCatalogLineage(expected, indexHighWater, shardHighWaters)
	expectedRaw, err := AppendSnapshotDocument(nil, expected)
	if err != nil {
		return nil, err
	}
	nextRaw, err := AppendSnapshotDocument(nil, next)
	if err != nil || !bytes.Equal(expectedRaw, nextRaw) {
		return nil, &CatalogError{Reason: fmt.Sprintf("table retirement target differs from certified removal: %v", err)}
	}
	return expected, nil
}

func buildReplicatedTableRetirementCleanupTarget(current *Snapshot) (*Snapshot, error) {
	if current == nil || current.tableRetirement == nil || current.Generation() == math.MaxUint64 {
		return nil, ErrInvalidCatalog
	}
	target, err := NewSnapshotWithReplicatedTableMetadata(
		cloneConfig(current.config), current.endpoints, current.Generation()+1,
		current.indexDescriptors(), current.statistics.Descriptors(), current.replicatedDescriptors(),
		current.replicatedTableProfiles(), current.ReplicatedTableDeclarations(),
	)
	if err != nil {
		return nil, err
	}
	return snapshotWithCatalogLineage(target, current.indexIDHighWater, current.shardGenerationHighWaters), nil
}

func advanceCatalogStateTableRetirementCleanup(current, next *Snapshot) (*Snapshot, error) {
	expected, err := buildReplicatedTableRetirementCleanupTarget(current)
	if err != nil {
		return nil, err
	}
	expectedRaw, err := AppendSnapshotDocument(nil, expected)
	if err != nil {
		return nil, err
	}
	nextRaw, err := AppendSnapshotDocument(nil, next)
	if err != nil || !bytes.Equal(expectedRaw, nextRaw) {
		return nil, &CatalogError{Reason: fmt.Sprintf("table retirement cleanup target differs from exact catalog carry: %v", err)}
	}
	return expected, nil
}

// RetireProvisionedTable publishes the namespace cut only after every shard in
// the table's independent distribution proves that this operation owns an
// active exclusive gate and that no execution pins remain. The catalog CAS is
// the irreversible boundary; physical runtime reclamation may safely follow.
func (authority *ReplicatedCatalogAuthority) RetireProvisionedTable(
	ctx context.Context, table string, operation [32]byte,
) error {
	if authority == nil || authority.executor == nil || ctx == nil || table == "" || operation == ([32]byte{}) {
		return ErrInvalidCatalog
	}
	current, err := authority.Read(ctx)
	if err != nil {
		return err
	}
	placement, found := current.Placement(table)
	if !found {
		return sqldriver.ErrTableNotFound
	}
	type observation struct {
		route   ReplicatedRoute
		applied uint64
		status  routegate.Status
	}
	observations := make([]observation, 0, 1)
	for _, descriptor := range current.replicatedDescriptors() {
		if descriptor.Distribution != placement.Distribution {
			continue
		}
		var replicas [ServingReplicaCount]ReplicatedEndpoint
		route, ok := current.ResolveReplicatedRoute(descriptor.Distribution, descriptor.Shard, replicas[:0])
		if !ok || route.Group != descriptor.Group {
			return ErrReplicatedRoute
		}
		observed, readErr := authority.executor.ReadRouteGate(ctx, route, 1)
		if readErr != nil {
			return readErr
		}
		identity, binding := schemainstall.SchemaDDLRouteGateIdentity(operation, route.Group)
		drain := observed.Status.Drain
		if drain.State != routegate.DrainActive || drain.Identity != identity || drain.Binding != binding ||
			drain.Epoch != observed.Status.Epoch || observed.Status.ActivePins != 0 {
			return ErrSchemaRolloutConflict
		}
		observations = append(observations, observation{route: route, applied: observed.Applied, status: observed.Status})
	}
	if len(observations) == 0 {
		return ErrReplicatedRoute
	}
	slices.SortFunc(observations, func(left, right observation) int {
		if order := bytes.Compare(left.route.Group.GroupID[:], right.route.Group.GroupID[:]); order != 0 {
			return order
		}
		return cmp.Compare(string(left.route.Shard), string(right.route.Shard))
	})
	h := sha256.New()
	_, _ = h.Write(replicatedTableRetirementProofDomain)
	_, _ = h.Write(operation[:])
	_, _ = h.Write([]byte(table))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(placement.Distribution))
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], current.Generation())
	_, _ = h.Write(scalar[:])
	for _, observed := range observations {
		_, _ = h.Write(observed.route.Group.GroupID[:])
		binary.BigEndian.PutUint64(scalar[:], observed.applied)
		_, _ = h.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], observed.status.Revision)
		_, _ = h.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], observed.status.Epoch)
		_, _ = h.Write(scalar[:])
		_, _ = h.Write(observed.status.Drain.Identity[:])
		_, _ = h.Write(observed.status.Drain.Binding[:])
	}
	retirement := replicatedTableRetirement{Operation: operation, SourceGeneration: current.Generation(),
		Table: table, Distribution: placement.Distribution}
	h.Sum(retirement.Proof[:0])
	next, err := buildReplicatedTableRetirementTarget(current, retirement)
	if err != nil {
		return err
	}
	err = authority.Publish(ctx, current.Generation(), next)
	for retry := 0; retry < 3 && errors.Is(err, ErrReplicatedCatalogPending); retry++ {
		err = authority.RetryPending(ctx)
	}
	return err
}

// ConfirmProvisionedTableRetirement clears the catalog witness only after the
// supervisor has durably removed the old group from its restart inventory.
// Until this CAS lands, every unrelated catalog transition fails closed.
func (authority *ReplicatedCatalogAuthority) ConfirmProvisionedTableRetirement(ctx context.Context, table string) error {
	if authority == nil || ctx == nil || table == "" {
		return ErrInvalidCatalog
	}
	current, err := authority.Read(ctx)
	if err != nil {
		return err
	}
	if current.tableRetirement == nil {
		return nil
	}
	if current.tableRetirement.Table != table {
		return ErrSchemaRolloutConflict
	}
	next, err := buildReplicatedTableRetirementCleanupTarget(current)
	if err != nil {
		return err
	}
	err = authority.Publish(ctx, current.Generation(), next)
	for retry := 0; retry < 3 && errors.Is(err, ErrReplicatedCatalogPending); retry++ {
		err = authority.RetryPending(ctx)
	}
	return err
}
