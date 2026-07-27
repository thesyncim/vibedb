package storeio

import (
	"fmt"
	"slices"
)

// SetInlineSuperblock encodes a checksummed StateRoot and cumulative free delta
// directly into the alternate fixed-root page. It has the same slot and
// full-page write discipline as SetSuperblock, but requires no separately
// allocated state or routine free-delta page.
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
	offset, err := SuperblockOffset(root.Generation, root.PageSize)
	if err != nil {
		return err
	}
	b.root.Offset = offset
	b.root.Length = root.PageSize
	b.rootGeneration = root.Generation
	return nil
}

// PublishInline selects state through an inline alternate superblock. Unlike
// Publish, it accepts the decoded StateRoot and cumulative inline free delta
// and requires that the transaction did not allocate a PageStateRoot extent.
// All ordinary data pages must already be staged.
func (t *WriteTransaction) PublishInline(state StateRoot, free InlineFreeDelta) error {
	return t.publishInline(state, free, nil)
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
// snapshot of the preceding state is active. The slice is borrowed only for
// this call. Automatic committers ignore it.
func (t *WriteTransaction) PublishInlineRetiring(
	state StateRoot,
	free InlineFreeDelta,
	retired []PageRef,
) error {
	return t.publishInline(state, free, retired)
}

func (t *WriteTransaction) publishInline(
	state StateRoot,
	free InlineFreeDelta,
	retired []PageRef,
) error {
	if t == nil || !t.active || state.StoreID != t.options.StoreID ||
		state.Generation != t.options.Generation || state.PageSize != t.options.PageSize ||
		state.NextLogicalID != t.nextID {
		return ErrBatchState
	}
	for i := 0; i < t.allocated; i++ {
		write := *t.writeAt(i)
		if write.Length == 0 {
			return fmt.Errorf("%w: unstaged transaction page", ErrInvalidWrite)
		}
		if write.kind == PageStateRoot {
			return fmt.Errorf("%w: inline publication allocated a state page", ErrInvalidWrite)
		}
	}
	if err := t.resizePages(t.allocated); err != nil {
		return err
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
		return err
	}
	if err := t.committer.publish(
		t.batch, t.options.Generation, retired,
	); err != nil {
		return err
	}
	t.active = false
	t.batch = nil
	return nil
}
