package driver

import (
	"errors"
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/store/durable"
)

// ReserveReplicatedChildApply durably freezes the exact hidden apply and
// capture storage identities before any split artifact is staged. The caller
// supplies the complete identity authenticated by the allocation authority;
// this method never mints or substitutes an identity. Exact retries are
// read-only validation after any unresolved catalog publication is settled.
func (d *Database) ReserveReplicatedChildApply(
	expected ReplicatedShardStoreIdentity,
	reserved ReplicatedApplyIdentity,
) error {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return err
	}
	meta := replicatedApplyMetaFromIdentity(reserved)
	if err := validateReplicatedApplyMeta(&meta, &expected); err != nil {
		return err
	}
	expected = ownedReplicatedShardStoreIdentity(expected)
	if d == nil || d.connector == nil {
		return ErrDatabaseClosed
	}
	connector := d.connector
	connector.mu.Lock()
	defer connector.mu.Unlock()
	if connector.closed || connector.db == nil || connector.refs != 0 || connector.exclusive {
		return ErrReplicatedChildStageBusy
	}
	core := connector.db
	core.mu.Lock()
	defer core.mu.Unlock()
	if core.closed || core.replicatedChildStageClaim != nil || core.replicatedApplyClaim != nil ||
		core.catalog.ReplicatedShardStore == nil || !core.catalog.ReplicatedShardStore.Equal(expected) {
		return ErrReplicatedShardStoreIdentityMismatch
	}
	if core.catalog.ReplicatedApply != nil {
		if core.catalog.ReplicatedApply.identity() == reserved {
			return core.settleCatalogLocked()
		}
		return ErrReplicatedApplyMismatch
	}
	if current := core.catalog.ReplicatedChildApply; current != nil {
		if current.identity() == reserved {
			return core.settleCatalogLocked()
		}
		return ErrReplicatedApplyMismatch
	}
	if err := core.settleCatalogLocked(); err != nil {
		return err
	}
	for _, path := range [...]string{core.replicatedApplyPath(&meta), core.replicatedCapturePath(&meta)} {
		if _, err := os.Lstat(path); err == nil || !os.IsNotExist(err) {
			return errors.Join(ErrReplicatedApplyMismatch, err)
		}
	}
	owned := meta
	core.catalog.ReplicatedChildApply = &owned
	core.catalogWritePending = true
	published, err := core.persistCatalogLocked()
	if err != nil || !published {
		return fmt.Errorf("vibedb: publish replicated child apply reservation: %w", err)
	}
	return nil
}

// ReplicatedChildApplyReservation reports the exact durable reservation.
func (d *Database) ReplicatedChildApplyReservation(
	expected ReplicatedShardStoreIdentity,
) (ReplicatedApplyIdentity, bool, error) {
	if d == nil || d.connector == nil {
		return ReplicatedApplyIdentity{}, false, ErrDatabaseClosed
	}
	connector := d.connector
	connector.mu.Lock()
	defer connector.mu.Unlock()
	if connector.closed || connector.db == nil {
		return ReplicatedApplyIdentity{}, false, ErrDatabaseClosed
	}
	core := connector.db
	core.mu.RLock()
	defer core.mu.RUnlock()
	if core.catalog.ReplicatedShardStore == nil || !core.catalog.ReplicatedShardStore.Equal(expected) {
		return ReplicatedApplyIdentity{}, false, ErrReplicatedShardStoreIdentityMismatch
	}
	if core.catalogWritePending || core.catalogFencePending {
		return ReplicatedApplyIdentity{}, false, durable.ErrCommitOutcomeUnknown
	}
	if core.catalog.ReplicatedChildApply == nil {
		return ReplicatedApplyIdentity{}, false, nil
	}
	return core.catalog.ReplicatedChildApply.identity(), true, nil
}
