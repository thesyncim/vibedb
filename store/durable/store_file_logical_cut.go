package durable

import "github.com/thesyncim/vibedb/internal/storeio"

// Package-test seams for forcing the two otherwise nanosecond-scale load
// boundaries. Production leaves them nil.
var (
	fileLogicalViewAfterStateLoadHook func()
	fileLogicalViewAfterCutLoadHook   func()
	fileLogicalCutBeforeResetHook     func(*fileStoreState)
)

const (
	fileLogicalCutGenerationBits = 48
	fileLogicalCutGenerationMask = uint64(1)<<fileLogicalCutGenerationBits - 1
	fileLogicalCutMinDelta       = -1 << 15
	fileLogicalCutMaxDelta       = 1<<15 - 1
)

// fileLogicalView binds one immutable physical state pointer to the packed
// logical publication word observed with it. Keeping the raw word lets epoch
// entry recheck the exact cut after claiming its slot, not merely an equivalent
// generation/count pair.
type fileLogicalView struct {
	state         *fileStoreState
	cut           uint64
	generation    uint64
	documentCount uint64
	delta         int
}

// retentionGeneration is a conservative lower bound on physical generations
// the view may dereference. It is intentionally distinct from generation while
// a packed overlay suffix is pending: generation filters logical overlay
// records, whereas this older generation must remain pinned against reuse. It
// is a reclamation floor, not a SafeFrom proof for any particular newer page.
func (v fileLogicalView) retentionGeneration() uint64 {
	if v.state == nil {
		return 0
	}
	return v.state.root.Generation
}

func packFileLogicalCut(generation uint64, delta int) (uint64, bool) {
	if generation == 0 || generation > fileLogicalCutGenerationMask ||
		delta < fileLogicalCutMinDelta || delta > fileLogicalCutMaxDelta {
		return 0, false
	}
	return generation | uint64(uint16(int16(delta)))<<fileLogicalCutGenerationBits, true
}

func fileLogicalCutGeneration(cut uint64) uint64 {
	return cut & fileLogicalCutGenerationMask
}

func fileLogicalCutDelta(cut uint64) int {
	return int(int16(uint16(cut >> fileLogicalCutGenerationBits)))
}

func fileLogicalDocumentCount(base uint64, delta int) (uint64, bool) {
	if delta < 0 {
		magnitude := uint64(-int64(delta))
		if magnitude > base {
			return 0, false
		}
		return base - magnitude, true
	}
	magnitude := uint64(delta)
	if magnitude > ^uint64(0)-base {
		return 0, false
	}
	return base + magnitude, true
}

// packedLogicalCutEnabled combines the construction-time fast-lane certificate
// with its one-way online-index cutover bit. It deliberately does not consult
// journalEnabled: BufferedVisible with RecoveryJournal=false still owns the
// smaller sibling journal used by Flush to persist a complete overlay suffix.
// It simply has no per-mutation durable-journal acknowledgement.
func (c *Collection) packedLogicalCutEnabled() bool {
	return c != nil && c.primaryConcurrentContexts != nil &&
		!c.packedLogicalCutDisabled.Load()
}

func (c *Collection) logicalViewOf(
	state *fileStoreState, cut uint64,
) (fileLogicalView, bool) {
	if state == nil {
		return fileLogicalView{}, true
	}
	if !c.packedLogicalCutEnabled() {
		return filePhysicalView(state), true
	}
	return fileLogicalViewOfPacked(state, cut)
}

func filePhysicalView(state *fileStoreState) fileLogicalView {
	if state == nil {
		return fileLogicalView{}
	}
	return fileLogicalView{
		state: state, generation: state.root.Generation,
		documentCount: state.root.DocumentCount,
	}
}

// fileLogicalViewOfPacked interprets cut after its caller has selected the
// packed lane once. Keeping the active-certificate load outside this helper is
// material on point reads; online-index disable is sticky and happens only
// after publishing a physical state with an equal zero-delta cut, so a reader
// that observed the former active value may safely finish this protocol.
func fileLogicalViewOfPacked(
	state *fileStoreState, cut uint64,
) (fileLogicalView, bool) {
	if state == nil {
		return fileLogicalView{}, true
	}
	view := fileLogicalView{
		state: state, cut: cut,
		generation:    state.root.Generation,
		documentCount: state.root.DocumentCount,
	}
	cutGeneration := fileLogicalCutGeneration(cut)
	// Physical publication wins while the reset Store is pending. Equality is
	// the fold boundary: the old cut still carries a delta relative to the old
	// physical state, but the new physical root already includes it. Greater-than
	// covers structural publication's equally short old-cut window.
	if state.root.Generation >= cutGeneration {
		return view, true
	}
	delta := fileLogicalCutDelta(cut)
	documentCount, ok := fileLogicalDocumentCount(
		state.root.DocumentCount, delta,
	)
	if !ok {
		return fileLogicalView{}, false
	}
	view.generation = cutGeneration
	view.documentCount = documentCount
	view.delta = delta
	return view, true
}

// visibleLogicalView uses state-cut-state sampling. It rules out the only
// invalid fold observation: an old physical pointer paired with the reset cut
// installed after a new pointer. A cut that advances after the second pointer
// load is a valid older linearization point.
func (c *Collection) visibleLogicalView() (fileLogicalView, error) {
	for {
		state, err := c.readerFileState()
		if err != nil {
			return fileLogicalView{}, err
		}
		if !c.packedLogicalCutEnabled() {
			return filePhysicalView(state), nil
		}
		if fileLogicalViewAfterStateLoadHook != nil {
			fileLogicalViewAfterStateLoadHook()
		}
		cut := c.logicalCut.Load()
		if fileLogicalViewAfterCutLoadHook != nil {
			fileLogicalViewAfterCutLoadHook()
		}
		if c.visibleState.Load() != state {
			continue
		}
		view, ok := fileLogicalViewOfPacked(state, cut)
		if !ok {
			return fileLogicalView{}, storeio.ErrGenerationOrder
		}
		return view, nil
	}
}

// sampleVisiblePackedLogicalView is the first half of packed direct epoch
// entry. Its caller has already selected the active packed lane. It omits
// the state-cut-state retry because enterReadEpoch performs the stronger check
// after publishing its generation slot: exact physical pointer and exact raw
// cut must both still match. Avoiding a redundant pointer load keeps the packed
// point-read tax to the one cut load its semantics require.
func (c *Collection) sampleVisiblePackedLogicalView() (fileLogicalView, error) {
	state, err := c.readerFileState()
	if err != nil {
		return fileLogicalView{}, err
	}
	view, ok := fileLogicalViewOfPacked(state, c.logicalCut.Load())
	if !ok {
		return fileLogicalView{}, storeio.ErrGenerationOrder
	}
	return view, nil
}

func (c *Collection) visibleLogicalViewNoError() fileLogicalView {
	if c == nil {
		return fileLogicalView{}
	}
	if !c.packedLogicalCutEnabled() {
		return filePhysicalView(c.readerFileStateNoError())
	}
	for {
		state := c.readerFileStateNoError()
		if fileLogicalViewAfterStateLoadHook != nil {
			fileLogicalViewAfterStateLoadHook()
		}
		cut := c.logicalCut.Load()
		if fileLogicalViewAfterCutLoadHook != nil {
			fileLogicalViewAfterCutLoadHook()
		}
		if c.visibleState.Load() != state {
			continue
		}
		view, ok := fileLogicalViewOfPacked(state, cut)
		if !ok {
			return fileLogicalView{}
		}
		return view
	}
}

// writerLogicalView is used only while writer is held (shared by the packed
// publisher or exclusive by a fold/structural path), so the physical pointer
// cannot transition around the cut load.
func (c *Collection) writerLogicalView() (fileLogicalView, bool) {
	if c == nil {
		return fileLogicalView{}, false
	}
	state := c.state.Load()
	if !c.packedLogicalCutEnabled() {
		return filePhysicalView(state), true
	}
	return fileLogicalViewOfPacked(state, c.logicalCut.Load())
}

func (c *Collection) packedLogicalCutPending() bool {
	if !c.packedLogicalCutEnabled() {
		return false
	}
	view, ok := c.writerLogicalView()
	return ok && view.state != nil &&
		view.generation > view.state.root.Generation
}

func (c *Collection) resetPackedLogicalCut(state *fileStoreState) {
	if !c.packedLogicalCutEnabled() || state == nil {
		return
	}
	cut, ok := packFileLogicalCut(state.root.Generation, 0)
	if !ok {
		return
	}
	c.logicalCut.Store(cut)
}
