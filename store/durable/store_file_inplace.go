package durable

import (
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// tryBufferedFileInplace replaces one same-size inline value in the current
// chunk primary only after the frame has already taken an ordinary COW update
// in this checkpoint window. The first touch does no in-place reservation and
// asks the caller to remember the newly published COW ref. The second and later
// touches patch that write-pinned frame-native page.
//
// handled=false is an honest eligibility or exclusive-ownership fallback. The
// caller proceeds through putLocked unchanged.
func (c *Collection) tryBufferedFileInplace(
	state *fileStoreState,
	src []byte,
	match *fileFingerprintMatch,
) (handled bool, trackFirstTouch bool, err error) {
	if c == nil || state == nil || match == nil || !c.buffered() {
		return false, false, nil
	}
	c.bufferedInplaceAttempts.Add(1)
	fallback := func() (bool, bool, error) {
		c.bufferedInplaceFallbacks.Add(1)
		return false, false, nil
	}
	if len(c.options.indexes) != 0 ||
		len(c.options.float64Columns) != 0 ||
		c.options.Collection.Schema != nil ||
		match.documentRef.Kind != storeio.PageDocument ||
		match.view.grouped ||
		match.value.value.Overflow != (storeio.PageRef{}) ||
		len(match.value.value.Inline) == 0 ||
		len(match.value.value.Inline) != len(src) {
		return fallback()
	}
	valueOffset, ok := match.document.OffsetOf(match.value.value.Inline)
	if !ok {
		return false, false, storeio.ErrDocumentPageCorrupt
	}

	seen := c.bufferedFirstTouchContains(match.documentRef)
	if !seen {
		if !c.bufferedFirstTouchCapacityAvailable() {
			c.bufferedFirstTouchOverflows.Add(1)
			return fallback()
		}
		return false, true, nil
	}
	if err := c.ensureDirtyCapacityFor(
		c.options.singleDocumentTransactionPages,
		c.options.singleDocumentTransactionBytes,
	); err != nil {
		return false, false, err
	}
	// A capacity checkpoint clears the window. Re-evaluate after the fence so
	// this mutation becomes the first ordinary COW touch in the new window.
	if !c.bufferedFirstTouchContains(match.documentRef) {
		if !c.bufferedFirstTouchCapacityAvailable() {
			c.bufferedFirstTouchOverflows.Add(1)
			return fallback()
		}
		return false, true, nil
	}

	handled, err = c.replaceBufferedFileInplace(
		state, src, valueOffset, match,
	)
	if handled || err != nil {
		return handled, false, err
	}
	// A contended frame falls back to COW. Retire the old first-touch entry and
	// let the caller remember the newly published frame instead.
	c.takeBufferedFirstTouch(match.documentRef)
	return false, true, nil
}

func (c *Collection) replaceBufferedFileInplace(
	state *fileStoreState,
	src []byte,
	valueOffset uint32,
	match *fileFingerprintMatch,
) (handled bool, err error) {
	generation := state.root.Generation + 1
	if generation == 0 {
		return false, storeio.ErrGenerationOrder
	}
	tx, err := c.beginWriteTransaction(
		0,
		storeio.WriteTransactionOptions{
			StoreID:       c.storeID,
			Generation:    generation,
			PageSize:      uint32(c.options.PageSize),
			FileEnd:       state.super.FileEnd,
			NextLogicalID: state.root.NextLogicalID,
		},
	)
	if err != nil {
		return false, fmt.Errorf("vibejson: begin buffered in-place token: %w", err)
	}
	abort := true
	defer func() {
		if abort {
			err = errors.Join(err, tx.Abort())
		}
	}()

	nextRoot := state.root
	nextRoot.Generation = generation
	nextSuper := state.super
	nextSuper.Generation = generation
	nextState := &fileStoreState{
		root: nextRoot, super: nextSuper,
		keyRoot: state.keyRoot, chunkRoot: state.chunkRoot,
		indexRoot: state.indexRoot, freeHead: state.freeHead,
	}
	c.bufferedValueBefore = append(
		c.bufferedValueBefore[:0], match.value.value.Inline...,
	)
	before := c.bufferedValueBefore

	// Put, automatic capacity checkpoints, Flush, and Close all hold c.writer
	// in buffered mode. Consequently no checkpoint seal can overlap this
	// admission. snapshotGate plus the reader fence close the remaining race:
	// while they are held no new snapshot lease can be acquired and new epoch
	// readers divert to the gated path, so the per-frame observability test
	// taken here stays true through cache replacement and root publication.
	//
	// A snapshot at generation S observes only frames whose identity was
	// published at or before S. This frame keeps the immutable birth generation
	// assigned by its first-touch COW (match.documentRef.Generation); an in-place
	// patch preserves that identity rather than rehoming it. The patch is thus
	// invisible to every active reader exactly when the birth generation is newer
	// than every active lease and epoch slot -- safeFromReaders. The former
	// collection-wide AnyActive veto was over-broad: a frame born after the
	// newest active snapshot can never be one that snapshot reads, so holding
	// any lease no longer forfeits the small-update win for frames born inside
	// the window.
	c.snapshotGate.Lock()
	c.beginReaderFence()
	if !c.safeFromReaders(match.documentRef.Generation) {
		c.endReaderFence()
		c.snapshotGate.Unlock()
		c.bufferedInplaceFallbacks.Add(1)
		return false, nil
	}
	previousDirty, replaceErr := c.cache.ReplaceLeasedCanonicalDirty(
		&match.document,
		match.documentRef, match.documentRef,
		valueOffset, before, src, generation,
	)
	if replaceErr != nil {
		c.endReaderFence()
		c.snapshotGate.Unlock()
		if errors.Is(replaceErr, storeio.ErrCanonicalPageBusy) ||
			errors.Is(replaceErr, storeio.ErrCanonicalPageChanged) {
			c.bufferedInplaceFallbacks.Add(1)
			return false, nil
		}
		return false, replaceErr
	}
	if publishErr := tx.PublishInline(nextRoot, c.inlineFree); publishErr != nil {
		restoreErr := c.cache.RestoreLeasedCanonicalDirty(
			&match.document,
			match.documentRef, match.documentRef,
			valueOffset, before, src, generation, previousDirty,
		)
		c.endReaderFence()
		c.snapshotGate.Unlock()
		return false, fmt.Errorf(
			"vibejson: publish buffered in-place token: %w",
			errors.Join(publishErr, restoreErr),
		)
	}
	abort = false
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.endReaderFence()
	c.snapshotGate.Unlock()
	c.bufferedInplaceUpdates.Add(1)
	return true, nil
}

func (c *Collection) bufferedFirstTouchContains(ref storeio.PageRef) bool {
	return slices.Contains(c.bufferedFirstTouches, ref)
}

func (c *Collection) takeBufferedFirstTouch(ref storeio.PageRef) bool {
	for index := range c.bufferedFirstTouches {
		if c.bufferedFirstTouches[index] != ref {
			continue
		}
		last := len(c.bufferedFirstTouches) - 1
		c.bufferedFirstTouches[index] = c.bufferedFirstTouches[last]
		c.bufferedFirstTouches[last] = storeio.PageRef{}
		c.bufferedFirstTouches = c.bufferedFirstTouches[:last]
		return true
	}
	return false
}

func (c *Collection) bufferedFirstTouchCapacityAvailable() bool {
	return len(c.bufferedFirstTouches) < cap(c.bufferedFirstTouches)
}

func (c *Collection) rememberBufferedFirstTouch(ref storeio.PageRef) {
	if ref == (storeio.PageRef{}) || c.bufferedFirstTouchContains(ref) {
		return
	}
	if !c.bufferedFirstTouchCapacityAvailable() {
		c.bufferedFirstTouchOverflows.Add(1)
		c.bufferedInplaceFallbacks.Add(1)
		return
	}
	c.bufferedFirstTouches = append(c.bufferedFirstTouches, ref)
}

func (c *Collection) clearBufferedInplaceLocked() {
	clear(c.bufferedFirstTouches)
	c.bufferedFirstTouches = c.bufferedFirstTouches[:0]
}

func (c *Collection) checkpointBufferedLocked() error {
	if err := c.materializePrimaryParentsLocked(); err != nil {
		return err
	}
	if err := c.committer.Flush(); err != nil {
		return err
	}
	c.cache.MarkDurable(c.committer.DurableGeneration())
	// The store root is now durable through this generation. Recycling the
	// journal head past it is the journal half of the same publication: a crash
	// between the root fence and this recycle leaves the old header, whose records
	// recovery re-applies idempotently onto the newer root.
	if err := c.recycleRecoveryJournalLocked(
		c.committer.DurableGeneration(),
	); err != nil {
		return err
	}
	c.clearBufferedInplaceLocked()
	return nil
}
