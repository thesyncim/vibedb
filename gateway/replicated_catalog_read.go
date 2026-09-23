package gateway

import (
	"bytes"
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftservice"
)

// Readers install the complete committed catalog, whether cold or already
// serving. readCatalogCut authenticates its linearizable head, exact witness
// and genesis; publishers validate membership grants, owner leases and the
// predecessor CAS before committing that head. Replaying write authorization
// at a reader would require an unbounded history of every intermediate head.
func (authority *ReplicatedCatalogAuthority) prepareReadCatalogCut(
	ctx context.Context, snapshot *Snapshot, raw []byte,
) (*Snapshot, func() error, error) {
	if authority == nil || authority.holder == nil || ctx == nil || snapshot == nil {
		return nil, nil, ErrReplicatedCatalog
	}
	if err := context.Cause(ctx); err != nil {
		return nil, nil, err
	}
	certified, err := initialCatalogState(snapshot)
	if err != nil {
		return nil, nil, err
	}
	canonical, err := appendReplicatedCatalogDocument(nil, certified, maxReplicatedCatalogBytes)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, nil, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	current := authority.holder.Current()
	if current != nil {
		if certified.Generation() < current.Generation() {
			return nil, nil, ErrStaleGeneration
		}
		if certified.Generation() == current.Generation() {
			installed, err := appendReplicatedCatalogDocument(nil, current, maxReplicatedCatalogBytes)
			if err != nil || !bytes.Equal(installed, canonical) {
				return nil, nil, errors.Join(err, ErrReplicatedCatalogConflict)
			}
			return current, func() error { return nil }, nil
		}
		if err := validateCommittedCatalogAdvance(current, certified); err != nil {
			return nil, nil, err
		}
	}
	return certified, func() error {
		holder := authority.holder
		holder.leaseMu.Lock()
		defer holder.leaseMu.Unlock()
		holder.initLeaseTrackerLocked()
		installed := holder.ptr.Load()
		if installed != nil {
			if installed.Generation() > certified.Generation() {
				return ErrCatalogGenerationNotNewer
			}
			if installed.Generation() == certified.Generation() {
				installedRaw, err := appendReplicatedCatalogDocument(nil, installed, maxReplicatedCatalogBytes)
				if err != nil || !bytes.Equal(installedRaw, canonical) {
					return errors.Join(err, ErrReplicatedCatalogConflict)
				}
				return nil
			}
			if err := validateCommittedCatalogAdvance(installed, certified); err != nil {
				return err
			}
		}
		holder.ptr.Store(certified)
		holder.signalLeaseChangeLocked()
		return nil
	}, nil
}

// These are monotone observations, not adjacent mutation rules: a reader may
// miss whole table lifecycles or several membership transitions. The current
// persisted lineage and complete snapshot remain authoritative, while known
// surviving identities must never regress or fork at an unchanged fence.
func validateCommittedCatalogAdvance(current, next *Snapshot) error {
	current, err := initialCatalogState(current)
	if err != nil {
		return err
	}
	if _, err = advanceIndexIDHighWater(current, next); err != nil {
		return err
	}
	for i, old := range current.config.Distributions {
		for j, candidate := range next.config.Distributions {
			if old.Name == candidate.Name && next.shardGenerationHighWaters[j] < current.shardGenerationHighWaters[i] {
				return &CatalogError{Reason: "committed shard allocation high-water regressed"}
			}
		}
	}
	for _, old := range current.config.Manifests {
		_, candidate := next.manifestOrdinal(old.Distribution())
		if candidate != nil && (candidate.Version() < old.Version() || candidate.Version() == old.Version() && !candidate.Equal(old)) {
			return &CatalogError{Reason: "committed routing version regressed or forked"}
		}
	}
	for _, old := range current.replicatedShards {
		manifest := current.config.Manifests[old.manifest]
		metadata, _ := manifest.ShardMetadataAt(int(old.shard))
		candidate, found := next.replicatedShardAt(manifest.Distribution(), metadata.ID)
		if !found {
			continue
		}
		if candidate.allocation < old.allocation {
			return &CatalogError{Reason: "committed replica allocation regressed"}
		}
		if candidate.allocation != old.allocation {
			continue
		}
		if replicatedCommandFenceRegresses(old.command, candidate.command) {
			return &CatalogError{Reason: "committed replica identity regressed or forked"}
		}
		if candidate.group != old.group || candidate.rangeIdentity != old.rangeIdentity ||
			!sameReplicatedSplitOrigin(candidate.splitOrigin, old.splitOrigin) {
			return &CatalogError{Reason: "committed replica identity regressed or forked"}
		}
		// A later membership, ownership, or route publication may replace the
		// roster and forwarding identity. Only an unchanged fence can fork
		// those fields, or the schema at the same generation.
		if committedCommandFenceAdvanced(old.command, candidate.command) {
			continue
		}
		if candidate.lineageDigest != old.lineageDigest || candidate.forwardingDigest != old.forwardingDigest ||
			candidate.command.SchemaGeneration == old.command.SchemaGeneration && candidate.logicalSchema != old.logicalSchema ||
			!sameReplicatedCatalogRoster(current, old, next, candidate) {
			return &CatalogError{Reason: "committed replica identity regressed or forked"}
		}
	}
	return nil
}

func committedCommandFenceAdvanced(old, next raftservice.CommandFence) bool {
	return next.ReplicaSetVersion > old.ReplicaSetVersion ||
		next.OwnershipEpoch > old.OwnershipEpoch ||
		next.RoutingVersion > old.RoutingVersion ||
		next.RouteGeneration > old.RouteGeneration ||
		next.SchemaGeneration > old.SchemaGeneration
}
