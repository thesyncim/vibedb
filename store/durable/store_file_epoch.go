package durable

import "github.com/thesyncim/vibedb/internal/storeio"

// enterPhysicalReadEpoch is the original one-pointer epoch entry for every
// lane that cannot publish a packed logical suffix. Keeping it separate makes
// the mature async/indexed/schema point-read instruction path pay no cut load,
// large view return, or extra pointer validation.
func (c *Collection) enterPhysicalReadEpoch() (
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
	if c.readEpochs.Diverted() || c.visibleState.Load() != state {
		epoch.Exit()
		return nil, storeio.ReadEpoch{}, false
	}
	return state, epoch, true
}

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
// correct for every case the fast path declines. The slot protects the physical
// root generation even when the logical overlay cut is newer; overlay filtering
// continues to use view.generation.
func (c *Collection) enterReadEpoch() (
	fileLogicalView, storeio.ReadEpoch, bool,
) {
	if !c.packedLogicalCutEnabled() {
		state, epoch, ok := c.enterPhysicalReadEpoch()
		if !ok {
			return fileLogicalView{}, storeio.ReadEpoch{}, false
		}
		return fileLogicalView{
			state: state, generation: state.root.Generation,
			documentCount: state.root.DocumentCount,
		}, epoch, true
	}
	return c.enterPackedReadEpoch()
}

// enterPackedReadEpoch is enterReadEpoch after one outer active-certificate
// check. Avoiding repeated sticky-disable loads keeps the competitive packed
// point-read lane to the one cut load its semantics require.
func (c *Collection) enterPackedReadEpoch() (
	fileLogicalView, storeio.ReadEpoch, bool,
) {
	view, err := c.sampleVisiblePackedLogicalView()
	if err != nil || view.state == nil {
		return fileLogicalView{}, storeio.ReadEpoch{}, false
	}
	epoch, ok := c.readEpochs.Enter(view.retentionGeneration())
	if !ok {
		return fileLogicalView{}, storeio.ReadEpoch{}, false
	}
	// Order matters and mirrors the writer: a fence is raised before the
	// writer's slot scan, and a new state/cut is published before a reclaim scan.
	// Rechecking both publication words after the slot claim prevents a reader
	// from pinning a generation assembled from different fold boundaries.
	if c.readEpochs.Diverted() || c.visibleState.Load() != view.state ||
		c.logicalCut.Load() != view.cut {
		epoch.Exit()
		return fileLogicalView{}, storeio.ReadEpoch{}, false
	}
	return view, epoch, true
}

// anyActiveReaders combines both reader registries for writer decisions that
// veto on any concurrent reader. Meaningful as an action veto only while the
// snapshot gate is write-held and a reader fence is raised: the gate freezes
// lease acquisition and the fence diverts new epoch entries, so the combined
// answer stays true until the fence drops.
func (c *Collection) anyActiveReaders() bool {
	return c.leases.AnyActive() || c.readEpochs.AnyActive()
}

type fileReaderSummary struct {
	snapshots uint64
	direct    uint64
	active    uint64
	minimum   uint64
}

// readerSummary combines both lifetime registries. Logical generation remains
// the age/reporting reference, while a packed direct reader contributes its
// older physical retention floor through ReadEpochs.Stats.
func (c *Collection) readerSummary(current uint64) fileReaderSummary {
	leases := c.leases.Stats(current)
	epochs := c.readEpochs.Stats(current)
	minimum := min(leases.MinimumGeneration, epochs.MinimumGeneration)
	return fileReaderSummary{
		snapshots: leases.Active,
		direct:    epochs.Active,
		active:    leases.Active + epochs.Active,
		minimum:   minimum,
	}
}

// beginReaderFence diverts new epoch readers to the gated slow path. The
// caller must hold snapshotGate's write side, so diverted readers block on the
// gate instead of spinning, and must pair it with endReaderFence after its
// dependent publication (router update, visible state, cache unreachability)
// is complete.
func (c *Collection) beginReaderFence() { c.readEpochs.BeginWriterFence() }

// endReaderFence releases the divert raised by beginReaderFence.
func (c *Collection) endReaderFence() { c.readEpochs.EndWriterFence() }
