package storeio

import (
	"fmt"
	"slices"
)

// PreparedInlinePublication is a two-phase committer publication. Prepare
// validates and encodes the root while retaining the committer publication
// lock; PublishInline only installs the already-admitted batch after an
// enclosing logical decision. Cancel releases the reservation without
// publishing. The token is single-use.
type PreparedInlinePublication struct {
	committer   *Committer
	transaction *WriteTransaction
	batch       *Batch
	generation  uint64
	tail        uint64
	active      bool
}

// PublishInline completes a prepared non-retiring publication. All ordinary
// validation, root encoding, and queue admission checks ran in PrepareInline;
// a returned error here indicates an internal invariant failure after the
// caller's decision and must be treated as an unknown outcome by that caller.
func (p *PreparedInlinePublication) PublishInline() error {
	_, err := p.publish(false, nil, nil, nil)
	return err
}

// PublishInlineRetiring completes a prepared publication while supplying the
// exact PageRefs and allocator extents that became unreachable. The caller
// must hold its reader gate across the active-reader proof and this call.
func (p *PreparedInlinePublication) PublishInlineRetiring(
	retired []PageRef, retirements []FreeExtent, superseded []FreeExtent,
) ([]FreeExtent, error) {
	return p.publish(true, retired, retirements, superseded)
}

func (p *PreparedInlinePublication) publish(
	retiring bool, retired []PageRef, retirements []FreeExtent,
	superseded []FreeExtent,
) ([]FreeExtent, error) {
	if p == nil || !p.active || p.committer == nil || p.batch == nil {
		return superseded, ErrBatchState
	}
	c := p.committer
	if c.options.ManualCheckpoint {
		c.manualMu.Lock()
	}
	result := superseded
	if retiring {
		result = c.publishPreparedUnconditionalLocked(
			p.batch, p.generation, p.tail, retired, retirements, superseded,
		)
	} else {
		result = c.publishPreparedUnconditionalLocked(
			p.batch, p.generation, p.tail, nil, nil, superseded,
		)
	}
	if c.options.ManualCheckpoint {
		c.manualMu.Unlock()
	}
	if p.transaction != nil {
		p.transaction.active = false
		p.transaction.batch = nil
	}
	p.active = false
	c.publishers.Add(^uint32(0))
	c.publishMu.Unlock()
	return result, nil
}

// Cancel abandons a prepared publication reservation. The owning
// WriteTransaction remains active so its caller can run the normal Abort
// cleanup, including allocator rollback and dirty-frame discard.
func (p *PreparedInlinePublication) Cancel() error {
	if p == nil || !p.active {
		return nil
	}
	p.active = false
	p.committer.publishers.Add(^uint32(0))
	p.committer.publishMu.Unlock()
	return nil
}

// SetInlineSuperblock encodes a checksummed StateRoot and cumulative free delta
// directly into the alternate fixed-root page, with no separately allocated
// state or routine free-delta page.
func (b *Batch) SetInlineSuperblock(root InlineSuperblock) error {
	if b == nil || b.state.Load() != batchOwned {
		return ErrBatchState
	}
	buffer := b.committer.buffers[b.root.Buffer]
	if uint64(root.PageSize) > uint64(len(buffer)) {
		return ErrInvalidWrite
	}
	page := buffer[:root.PageSize]
	clear(page)
	if _, err := EncodeInlineSuperblock(page, root); err != nil {
		return err
	}
	offset, err := superblockOffset(root.Generation, root.PageSize)
	if err != nil {
		return err
	}
	b.root.Offset = offset
	b.root.Length = root.PageSize
	b.rootGeneration = root.Generation
	return nil
}

// PrepareInlinePublication performs the fallible portion of an inline-root
// publication while retaining the committer reservation. The caller may then
// append its enclosing logical decision and complete the publication with the
// returned token. On error the transaction remains owned and must be aborted.
func (t *WriteTransaction) PrepareInlinePublication(
	state StateRoot, free InlineFreeDelta,
) (*PreparedInlinePublication, error) {
	if t == nil || !t.active || t.batch == nil ||
		state.StoreID != t.options.StoreID ||
		state.Generation != t.options.Generation ||
		state.PageSize != t.options.PageSize ||
		state.NextLogicalID != t.nextID {
		return nil, ErrBatchState
	}
	if err := t.resizePages(t.allocated); err != nil {
		return nil, err
	}
	if !t.batch.materialized {
		slices.SortFunc(t.fullWrites(), func(a, b Write) int {
			if a.Offset < b.Offset {
				return -1
			}
			if a.Offset > b.Offset {
				return 1
			}
			return 0
		})
	}
	root := InlineSuperblock{
		StoreID: t.options.StoreID, Generation: t.options.Generation,
		FileEnd: t.fileEnd, PageSize: t.options.PageSize, State: state,
		FreeDelta: free,
	}
	if err := t.batch.SetInlineSuperblock(root); err != nil {
		return nil, err
	}
	prepared, err := t.committer.prepareInlinePublication(
		t.batch, t.options.Generation,
	)
	if err != nil {
		return nil, err
	}
	prepared.transaction = t
	return prepared, nil
}

// PublishInline selects state through an inline alternate superblock. It
// accepts the decoded StateRoot and cumulative inline free delta. All ordinary
// data pages must already be staged.
func (t *WriteTransaction) PublishInline(state StateRoot, free InlineFreeDelta) error {
	_, err := t.publishInline(state, free, nil, nil, nil)
	return err
}

// PublishInlineConditional atomically selects state only when expected is
// still the committer's newest published generation.
func (t *WriteTransaction) PublishInlineConditional(state StateRoot, free InlineFreeDelta, expected uint64) error {
	if t == nil || !t.active || t.batch == nil {
		return ErrBatchState
	}
	if err := t.batch.SetExpectedPreviousGeneration(expected); err != nil {
		return err
	}
	_, err := t.publishInline(state, free, nil, nil, nil)
	return err
}

// PublishInlineRetiring is PublishInline with a conservative buffered-
// checkpoint optimization. retired must be the exact physical extents the
// state being published makes unreachable. A manual committer may recycle an
// older queued write only when its offset and length exactly match one of
// these extents and that write is outside every checkpoint cut already handed
// to the worker.
//
// The caller must exclude snapshot acquisition from before this call through
// publication of the corresponding reader-visible state and must prove that no
// snapshot of the preceding state is active. retired and retirements are
// borrowed only for this call and need not be index-parallel: exact
// offset/length matching binds the physical PageRef proof to its authoritative
// retired-generation record. superseded is caller-owned bounded output; the
// returned slice appends only records whose pending writes this call actually
// made impossible to checkpoint. Automatic committers leave it unchanged.
func (t *WriteTransaction) PublishInlineRetiring(
	state StateRoot,
	free InlineFreeDelta,
	retired []PageRef,
	retirements []FreeExtent,
	superseded []FreeExtent,
) ([]FreeExtent, error) {
	return t.publishInline(
		state, free, retired, retirements, superseded,
	)
}

func (t *WriteTransaction) publishInline(
	state StateRoot,
	free InlineFreeDelta,
	retired []PageRef,
	retirements []FreeExtent,
	superseded []FreeExtent,
) ([]FreeExtent, error) {
	if t == nil || !t.active || state.StoreID != t.options.StoreID ||
		state.Generation != t.options.Generation || state.PageSize != t.options.PageSize ||
		state.NextLogicalID != t.nextID {
		return superseded, ErrBatchState
	}
	for i := 0; i < t.allocated; i++ {
		write := *t.writeAt(i)
		if write.Length == 0 {
			return superseded,
				fmt.Errorf("%w: unstaged transaction page", ErrInvalidWrite)
		}
	}
	if err := t.resizePages(t.allocated); err != nil {
		return superseded, err
	}
	if !t.batch.materialized {
		slices.SortFunc(t.fullWrites(), func(a, b Write) int {
			if a.Offset < b.Offset {
				return -1
			}
			if a.Offset > b.Offset {
				return 1
			}
			return 0
		})
	}
	root := InlineSuperblock{
		StoreID: t.options.StoreID, Generation: t.options.Generation,
		FileEnd: t.fileEnd, PageSize: t.options.PageSize, State: state,
		FreeDelta: free,
	}
	if err := t.batch.SetInlineSuperblock(root); err != nil {
		return superseded, err
	}
	var err error
	superseded, err = t.committer.publishRetiring(
		t.batch, t.options.Generation,
		retired, retirements, superseded,
	)
	if err != nil {
		return superseded, err
	}
	t.active = false
	t.batch = nil
	return superseded, nil
}
