package gateway

import (
	"context"
	"errors"
)

// ErrInvalidRefreshSource reports a file refresher without the path or holder
// required to load and publish catalog generations.
var ErrInvalidRefreshSource = errors.New("gateway: invalid catalog refresh source")

// FileCatalogRefresher loads atomically persisted catalog snapshots on demand
// after a shard refuses stale routing metadata. Refreshes are coalesced: one
// caller performs file I/O while later callers reuse the generation it
// published. Malformed, equal, or older files leave the last valid generation
// installed and fail closed.
type FileCatalogRefresher struct {
	path   string
	holder *CatalogHolder
	gate   chan struct{}
}

// NewFileCatalogRefresher returns an on-demand refresher for path and holder.
// Configuration is validated by Refresh so construction stays allocation-only
// and fits directly into process startup wiring.
func NewFileCatalogRefresher(path string, holder *CatalogHolder) *FileCatalogRefresher {
	r := &FileCatalogRefresher{
		path:   path,
		holder: holder,
		gate:   make(chan struct{}, 1),
	}
	r.gate <- struct{}{}
	return r
}

// Refresh returns and publishes a catalog generation strictly newer than
// staleGen. It first reuses a generation another caller already published,
// otherwise it acquires a context-aware single-loader gate and reads the
// crash-safe snapshot file. A failed load never replaces the current snapshot.
func (r *FileCatalogRefresher) Refresh(ctx context.Context, staleGen uint64) (*Snapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil || r.path == "" || r.holder == nil || r.gate == nil {
		return nil, ErrInvalidRefreshSource
	}
	if current := r.holder.Current(); current != nil && current.Generation() > staleGen {
		return current, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.gate:
	}
	defer func() { r.gate <- struct{}{} }()

	// A concurrent loader may have published while this caller waited.
	if current := r.holder.Current(); current != nil && current.Generation() > staleGen {
		return current, nil
	}

	snap, err := LoadSnapshot(r.path)
	if err != nil {
		return nil, err
	}
	if snap.Generation() <= staleGen {
		return nil, ErrStaleGeneration
	}
	r.holder.PublishNewer(snap)
	current := r.holder.Current()
	if current == nil || current.Generation() <= staleGen {
		return nil, ErrStaleGeneration
	}
	return current, nil
}
