package durable

import "github.com/thesyncim/vibedb/internal/storeio"

// enterReadEpoch is the direct-read fast path's entry: it selects the visible
// state and protects it with one epoch slot instead of the snapshot-gate
// round trip plus a mutex-guarded generation lease that the slow path pays.
//
// The entry protocol (publish the slot, then check the divert word, then
// re-load the visible state) is what makes the lock-free entry safe against
// the serialized writer; see the ReadEpochs contract for the two store/load
// orderings it completes. Any ambiguity — a full table, a writer fence, a
// state pointer that moved mid-entry, a persistence failure — returns
// ok=false and the caller runs the gated lease path instead, which remains
// correct for every case the fast path declines.
func (c *Collection) enterReadEpoch() (
	*fileStoreState, storeio.ReadEpoch, bool,
) {
	state, err := c.readerFileState()
	if err != nil || state == nil {
		return nil, storeio.ReadEpoch{}, false
	}
	epoch, ok := c.readEpochs.Enter(state.root.Generation)
	if !ok {
		return nil, storeio.ReadEpoch{}, false
	}
	// Order matters and mirrors the writer: a fence is raised before the
	// writer's slot scan, and a new state is published before a reclaim scan.
	// Loading divert and re-loading the state only after the slot claim means
	// a reader this entry lets through was either seen by the scan or observed
	// the writer's newer publication.
	if c.readEpochs.Diverted() || c.visibleState.Load() != state {
		epoch.Exit()
		return nil, storeio.ReadEpoch{}, false
	}
	return state, epoch, true
}

// anyActiveReaders combines both reader registries for writer decisions that
// veto on any concurrent reader. Meaningful as an action veto only while the
// snapshot gate is write-held and a reader fence is raised: the gate freezes
// lease acquisition and the fence diverts new epoch entries, so the combined
// answer stays true until the fence drops.
func (c *Collection) anyActiveReaders() bool {
	return c.leases.AnyActive() || c.readEpochs.AnyActive()
}

// safeFromReaders reports whether a page first published at generation is
// invisible to every active reader in either registry. Same fence requirement
// as anyActiveReaders.
func (c *Collection) safeFromReaders(generation uint64) bool {
	return c.leases.SafeFromSnapshots(generation) &&
		c.readEpochs.SafeFrom(generation)
}

// beginReaderFence diverts new epoch readers to the gated slow path. The
// caller must hold snapshotGate's write side, so diverted readers block on the
// gate instead of spinning, and must pair it with endReaderFence after its
// dependent publication (router update, visible state, cache unreachability)
// is complete.
func (c *Collection) beginReaderFence() { c.readEpochs.BeginWriterFence() }

// endReaderFence releases the divert raised by beginReaderFence.
func (c *Collection) endReaderFence() { c.readEpochs.EndWriterFence() }
