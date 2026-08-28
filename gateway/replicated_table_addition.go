package gateway

import (
	"bytes"
	"context"
	"errors"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibejson"
)

// A provisioning fragment is deliberately not a complete serving catalog: it
// cannot stand in for the authenticated ledger/control-plane topology.
type replicatedTableProvisionDocument struct {
	TableProvision persistedCatalog `json:"table_provision"`
}

func AppendReplicatedTableProvision(dst []byte, fragment *Snapshot) ([]byte, error) {
	if fragment == nil {
		return nil, ErrInvalidCatalog
	}
	document := replicatedTableProvisionDocument{TableProvision: toPersisted(fragment)}
	raw, err := vibejson.Marshal(&document)
	if err != nil {
		return nil, err
	}
	if len(raw) > 4<<20 {
		return nil, ErrCatalogTooLarge
	}
	return append(dst, raw...), nil
}

func OpenReplicatedTableProvision(raw []byte) (*Snapshot, error) {
	if len(raw) == 0 || len(raw) > 4<<20 {
		return nil, ErrCatalogTooLarge
	}
	var document replicatedTableProvisionDocument
	if err := vibejson.Unmarshal(raw, &document); err != nil {
		return nil, err
	}
	canonical, err := vibejson.Marshal(&document)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, ErrInvalidCatalog
	}
	pc := document.TableProvision
	if pc.Version != catalogVersion || pc.Generation != 1 || pc.RequestLedger != nil {
		return nil, ErrInvalidCatalog
	}
	config, endpoints, indexes, statistics, err := pc.toConfig()
	if err != nil {
		return nil, err
	}
	descriptors, err := pc.replicatedDescriptors()
	if err != nil {
		return nil, err
	}
	profiles, err := pc.replicatedTableProfiles()
	if err != nil {
		return nil, err
	}
	return NewSnapshotWithReplicatedTableMetadata(config, endpoints, 1, indexes, statistics, descriptors, profiles, pc.replicatedTableDeclarations())
}

// BuildReplicatedTableAddition merges one explicitly provisioned independent
// distribution into the current catalog. It cannot replace a table, alter a
// route, or change an existing endpoint. An identical declaration is resumable.
// This is a cold topology operation, not SQL execution or query fanout.
func BuildReplicatedTableAddition(current, addition *Snapshot) (*Snapshot, error) {
	if current == nil || addition == nil || current.generation == math.MaxUint64 ||
		len(addition.config.Distributions) != 1 || len(addition.config.Placements) != 1 ||
		len(addition.config.Manifests) != 1 || len(addition.replicatedTables) != 1 ||
		len(addition.replicatedShards) != 1 || len(addition.plannerIndexes) != 0 || addition.durableRequestLedgerTopology != nil {
		return nil, &CatalogError{Reason: "table addition requires one independent provisioned RF3 table"}
	}
	profile := addition.replicatedTableProfiles()[0]
	placement := addition.config.Placements[0]
	for _, existing := range current.replicatedTableProfiles() {
		if existing.Table != profile.Table {
			continue
		}
		old, ok := current.Placement(profile.Table)
		if !ok || existing != profile || old.Distribution != placement.Distribution || !slices.Equal(old.Columns, placement.Columns) {
			return nil, &CatalogError{Reason: "table addition conflicts with an existing table"}
		}
		var oldDeclarations []ReplicatedTableDeclaration
		for _, d := range current.ReplicatedTableDeclarations() {
			if d.Table == profile.Table {
				oldDeclarations = append(oldDeclarations, d)
			}
		}
		if !slices.Equal(oldDeclarations, addition.ReplicatedTableDeclarations()) {
			return nil, &CatalogError{Reason: "table declaration differs on resume"}
		}
		return current, nil
	}
	if _, exists := current.Spec(placement.Distribution); exists {
		return nil, &CatalogError{Reason: "table addition distribution already exists"}
	}
	config := cloneConfig(current.config)
	config.Distributions = append(config.Distributions, addition.config.Distributions...)
	config.Placements = append(config.Placements, addition.config.Placements...)
	config.Manifests = append(config.Manifests, addition.config.Manifests...)
	endpoints := cloneEndpoints(current.endpoints)
	for id, address := range addition.endpoints {
		if previous, ok := endpoints[id]; ok && previous != address {
			return nil, &CatalogError{Reason: "table addition changes an endpoint"}
		}
		endpoints[id] = address
	}
	descriptors := append(current.replicatedDescriptors(), addition.replicatedDescriptors()...)
	profiles := append(current.replicatedTableProfiles(), profile)
	declarations := append(current.ReplicatedTableDeclarations(), addition.ReplicatedTableDeclarations()...)
	next, err := NewSnapshotWithReplicatedTableMetadata(config, endpoints, current.generation+1, current.indexDescriptors(), current.statistics.Descriptors(), descriptors, profiles, declarations)
	if err != nil {
		return nil, err
	}
	return advanceCatalogState(current, next)
}

// RegisterProvisionedTable publishes only after a linearizable native read
// proves the prepared group's serving fence. Publish uses the normal RF3
// compare-and-swap and retains unknown commands for exact retry.
func (authority *ReplicatedCatalogAuthority) RegisterProvisionedTable(ctx context.Context, addition *Snapshot) error {
	current, err := authority.Read(ctx)
	if err != nil {
		return err
	}
	next, err := BuildReplicatedTableAddition(current, addition)
	if err != nil {
		return err
	}
	authorized, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	route, ok := addition.ReplicatedRouteAt(0, replicas[:0])
	if !ok {
		return ErrReplicatedRoute
	}
	key, ok := orderedkey.AppendString(nil, []byte("provisioning-readiness-probe"), orderedkey.Ascending)
	if !ok {
		return ErrReplicatedRoute
	}
	_, err = authority.executor.ReadPoint(authorized, route, ReplicatedPointRead{
		Relation: addition.replicatedTableProfiles()[0].Relation, Key: key, MinimumApplied: 1,
		MaxValueBytes: addition.replicatedTableProfiles()[0].MaxDocumentBytes, Linearizable: true,
	})
	if err != nil {
		return err
	}
	// A catalog entry survives a process restart, but its Raft leader does not.
	// Idempotent registration must still prove the live serving fence before
	// the supervisor advertises this table as ready.
	if next == current {
		return nil
	}
	err = authority.Publish(ctx, current.generation, next)
	for retry := 0; retry < 3 && errors.Is(err, ErrReplicatedCatalogPending); retry++ {
		err = authority.RetryPending(ctx)
	}
	return err
}
