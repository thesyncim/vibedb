package gateway

// NewCatalogDurableRequestLedgerTopologyHolder constructs the serving
// request-ledger directory from the exact catalog generation currently
// published by catalog. Unlike Publish, later RefreshFromCatalog calls may
// install changed range geometry and identities: CatalogHolder is the
// authenticated topology authority which validated that transition.
func NewCatalogDurableRequestLedgerTopologyHolder(
	catalog *CatalogHolder,
) (*DurableRequestLedgerTopologyHolder, error) {
	if catalog == nil {
		return nil, ErrDurableRequest
	}
	holder := &DurableRequestLedgerTopologyHolder{catalog: catalog}
	if err := holder.RefreshFromCatalog(); err != nil {
		return nil, err
	}
	return holder, nil
}

// RefreshFromCatalog publishes the immutable request-ledger directory carried
// by CatalogHolder.Current. It performs no remote I/O. The steady-state path is
// two atomic loads and allocates nothing; cloning is confined to a new catalog
// generation.
//
// This is intentionally the only publication path that can change range
// boundaries or identities. Generic Publish retains its same-home restriction,
// so a caller cannot rehome durable identities by presenting an unauthenticated
// topology value.
func (holder *DurableRequestLedgerTopologyHolder) RefreshFromCatalog() error {
	if holder == nil || holder.catalog == nil {
		return ErrDurableRequest
	}
	for {
		snapshot := holder.catalog.Current()
		current := holder.current.Load()
		if snapshot != nil && snapshot.durableRequestLedgerTopology != nil &&
			current != nil && current.Generation == snapshot.generation &&
			holder.catalogSnapshot.Load() == snapshot {
			return nil
		}

		holder.mu.Lock()
		// A concurrent catalog publisher won the race. Retry from one coherent
		// immutable snapshot rather than cloning or exposing a mixed generation.
		if holder.catalog.Current() != snapshot {
			holder.mu.Unlock()
			continue
		}
		if snapshot == nil || snapshot.durableRequestLedgerTopology == nil ||
			snapshot.generation == 0 ||
			snapshot.durableRequestLedgerTopology.Generation != snapshot.generation {
			holder.catalogSnapshot.Store(nil)
			holder.current.Store(nil)
			holder.mu.Unlock()
			return ErrDurableRequestUnavailable
		}
		current = holder.current.Load()
		if current != nil && current.Generation == snapshot.generation &&
			holder.catalogSnapshot.Load() == snapshot {
			holder.mu.Unlock()
			return nil
		}
		// A catalog-bound holder must never retain a locally injected generation
		// ahead of its authority. Clearing it makes the failure a refusal rather
		// than serving an unproved home.
		if current != nil && current.Generation > snapshot.generation {
			holder.catalogSnapshot.Store(nil)
			holder.current.Store(nil)
			holder.mu.Unlock()
			return ErrDurableRequestUnavailable
		}
		next := cloneDurableRequestLedgerTopology(snapshot.durableRequestLedgerTopology)
		if holder.catalog.Current() != snapshot {
			holder.mu.Unlock()
			continue
		}
		holder.current.Store(next)
		holder.catalogSnapshot.Store(snapshot)
		holder.mu.Unlock()
		return nil
	}
}
