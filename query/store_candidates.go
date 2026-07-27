package query

import "github.com/thesyncim/vibedb/store"

// storeCandidateMasks and storeCandidateMasksMode are the heap-Snapshot
// entry points into the shared generic planner (candidates_mask.go).
// Keep the snapshot parameter concrete (store.Snapshot, not store.IndexSource)
// here rather than accepting an interface: boxing a Snapshot at this call
// boundary would make execute.go's heap-snapshot callers pay an interface
// conversion on every call, on top of what the generic dispatch inside
// candidates_mask.go already costs. The instantiation `candidatesFor[store.Snapshot]`
// is resolved once at compile time regardless of how this wrapper is spelled.
func (p *plan) storeCandidateMasks(snapshot store.Snapshot, w *Workspace) ([]store.Mask, error) {
	masks, _, err := p.storeCandidateMasksMode(snapshot, w, false)
	return masks, err
}

func (p *plan) storeCandidateMasksMode(snapshot store.Snapshot, w *Workspace, requireExact bool) ([]store.Mask, bool, error) {
	// A heap Snapshot carries chunk summaries and can materialize its live-row
	// universe, so it is its own zone and live-mask source. Both are stated
	// here, where the type is concrete and the compiler checks them, rather than
	// discovered by assertion inside the generic planner; see sourceCaps.
	return snapshotCandidateMasks(
		p, snapshot, sourceCaps{zone: snapshot, live: snapshot}, w, requireExact)
}
