package storeio

import (
	"bytes"
	"fmt"
)

// KeyTreeEdit is one mutation inside a sorted batch. Delete removes the key;
// otherwise Location replaces or inserts it.
type KeyTreeEdit struct {
	Key      []byte
	Location KeyLocation
	Delete   bool
}

// KeyTreeBatchMutation is the result of applying a whole sorted edit batch in
// one copy-on-write descent.
//
// Retired names every page the descent rewrote, appended to caller storage so
// one batch is not bounded by the fixed sixteen-entry scratch a single-key
// mutation carries. Like KeyTreeMutation, those pages stay protected by the old
// generation and must reach the extent reclaimer only after publication.
type KeyTreeBatchMutation struct {
	Root    PageRef
	Retired []PageRef
	Changed bool
}

// MutateKeyTreeBatch applies edits, which must be sorted strictly ascending by
// raw key bytes, in a single descent that rewrites every visited page exactly
// once.
//
// Applying the same batch as a sequence of UpsertKeyTree calls is correct but
// costs one full root-to-leaf copy per key: inserting sixty-four keys into a
// three-level tree allocated, wrote, and immediately retired roughly one
// hundred and ninety pages, of which fewer than ten survived the commit. Those
// pages are not merely wasted device bytes — they are charged against the
// transaction's worst-case page reservation, which is what made a
// single-transaction batch of any useful size impossible to bound. This
// descent writes only the pages the published tree keeps, plus splits.
func MutateKeyTreeBatch(
	cache *PageCache, tx *WriteTransaction, root PageRef,
	edits []KeyTreeEdit, bounds KeyTreeBounds, retired []PageRef,
) (KeyTreeBatchMutation, error) {
	mutation := KeyTreeBatchMutation{Root: root, Retired: retired}
	if tx == nil || !tx.active || (cache == nil && root != (PageRef{})) ||
		bounds.ChunkHighWater == 0 || bounds.ChunkDocuments == 0 || bounds.ChunkDocuments > 64 {
		return mutation, fmt.Errorf("%w: key-tree batch bounds", ErrInvalidWrite)
	}
	for i, edit := range edits {
		if i != 0 && bytes.Compare(edits[i-1].Key, edit.Key) >= 0 {
			return mutation, fmt.Errorf("%w: unsorted key-tree batch", ErrInvalidWrite)
		}
		if !edit.Delete &&
			(edit.Location.Chunk >= bounds.ChunkHighWater || edit.Location.Slot >= bounds.ChunkDocuments) {
			return mutation, fmt.Errorf("%w: key-tree batch location", ErrInvalidWrite)
		}
	}
	if len(edits) == 0 {
		return mutation, nil
	}

	batch := keyTreeBatch{cache: cache, tx: tx, bounds: bounds, retired: retired}
	if root == (PageRef{}) {
		// An empty tree has no page to merge against, so the batch is its own
		// leaf content. Deletes against a missing key are dropped here rather
		// than rejected: the collection resolves every key before staging, so a
		// delete that survives to this point can only be a same-batch
		// insert-then-delete pair, which must publish as no row at all.
		batch.scratchEntries = batch.scratchEntries[:0]
		for _, edit := range edits {
			if edit.Delete {
				continue
			}
			batch.scratchEntries = append(batch.scratchEntries,
				KeyDirectoryEntry{Key: edit.Key, Location: edit.Location})
		}
		if len(batch.scratchEntries) == 0 {
			return mutation, nil
		}
		children, err := batch.encodeLeaves(nil, 0, batch.scratchEntries)
		if err != nil {
			return mutation, err
		}
		return batch.finish(children)
	}

	children, err := batch.rewritePage(nil, root, edits, 0)
	if err != nil {
		mutation.Retired = batch.retired
		return mutation, err
	}
	return batch.finish(children)
}

// keyTreeBatch carries the descent's shared scratch. Assembly buffers are kept
// per depth because a branch holds its child list live across the recursive
// call that fills the next depth's list.
type keyTreeBatch struct {
	cache          *PageCache
	tx             *WriteTransaction
	bounds         KeyTreeBounds
	retired        []PageRef
	scratchEntries []KeyDirectoryEntry
	assembly       [keyDirectoryMaxLevel + 2][]KeyDirectoryChild
}

func (b *keyTreeBatch) finish(children []KeyDirectoryChild) (KeyTreeBatchMutation, error) {
	mutation := KeyTreeBatchMutation{Retired: b.retired, Changed: true}
	level := uint8(0)
	if len(children) > 1 {
		var err error
		level, err = b.pageLevel(children[0].Ref)
		if err != nil {
			return mutation, err
		}
	}
	// A descent that split the old root leaves several same-level pages with no
	// parent. Growing one level at a time — rather than assuming two children
	// fit one branch, which a wide batch of long keys violates — keeps the
	// invariant that every published branch fits its page.
	for len(children) > 1 {
		if level >= keyDirectoryMaxLevel {
			return mutation, ErrKeyTreeDepth
		}
		level++
		next, err := b.encodeBranches(nil, level, 0, children)
		if err != nil {
			return mutation, err
		}
		children = next
	}
	if len(children) == 1 {
		mutation.Root = children[0].Ref
	}
	return mutation, nil
}

// rewritePage merges the edits belonging to ref into it and appends the
// replacement pages to dst. An empty return means the subtree became empty.
func (b *keyTreeBatch) rewritePage(
	dst []KeyDirectoryChild, ref PageRef, edits []KeyTreeEdit, depth int,
) ([]KeyDirectoryChild, error) {
	if depth > int(keyDirectoryMaxLevel) {
		return nil, ErrKeyTreeDepth
	}
	lease, err := b.cache.Acquire(ref)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	view, err := OpenKeyDirectoryPage(
		lease.Page(), b.bounds.FileEnd, b.bounds.NextLogicalID,
		b.bounds.ChunkHighWater, b.bounds.ChunkDocuments,
	)
	if err != nil {
		return nil, err
	}
	if view.Header().Level == 0 {
		return b.rewriteLeaf(dst, ref, view, edits)
	}
	return b.rewriteBranch(dst, ref, view, edits, depth)
}

func (b *keyTreeBatch) rewriteLeaf(
	dst []KeyDirectoryChild, ref PageRef, view KeyDirectoryView, edits []KeyTreeEdit,
) ([]KeyDirectoryChild, error) {
	entries := b.scratchEntries[:0]
	at := 0
	for i := 0; i < view.Len(); i++ {
		entry, _ := view.EntryAt(i)
		for at < len(edits) && bytes.Compare(edits[at].Key, entry.Key) < 0 {
			if !edits[at].Delete {
				entries = append(entries, KeyDirectoryEntry{
					Key: edits[at].Key, Location: edits[at].Location,
				})
			}
			at++
		}
		if at < len(edits) && bytes.Equal(edits[at].Key, entry.Key) {
			if !edits[at].Delete {
				entry.Location = edits[at].Location
				entries = append(entries, entry)
			}
			at++
			continue
		}
		entries = append(entries, entry)
	}
	for ; at < len(edits); at++ {
		if edits[at].Delete {
			continue
		}
		entries = append(entries, KeyDirectoryEntry{
			Key: edits[at].Key, Location: edits[at].Location,
		})
	}
	b.scratchEntries = entries
	b.retired = append(b.retired, ref)
	if len(entries) == 0 {
		return dst, nil
	}
	return b.encodeLeaves(dst, ref.LogicalID, entries)
}

func (b *keyTreeBatch) rewriteBranch(
	dst []KeyDirectoryChild, ref PageRef, view KeyDirectoryView, edits []KeyTreeEdit, depth int,
) ([]KeyDirectoryChild, error) {
	count := view.Len()
	if count == 0 {
		return nil, ErrKeyDirectoryCorrupt
	}
	assembled := b.assembly[depth][:0]
	at := 0
	for rank := 0; rank < count; rank++ {
		child, _ := view.ChildAt(rank)
		// Every edit below the first child's lower bound belongs to that child:
		// the branch owns the whole key space beneath it, and a batch inserting
		// a new minimum must not fall out of the tree.
		end := len(edits)
		if rank+1 < count {
			next, _ := view.ChildAt(rank + 1)
			end = at
			for end < len(edits) && bytes.Compare(edits[end].Key, next.Lower) < 0 {
				end++
			}
		}
		if end == at {
			assembled = append(assembled, child)
			continue
		}
		var err error
		assembled, err = b.rewritePage(assembled, child.Ref, edits[at:end], depth+1)
		if err != nil {
			return nil, err
		}
		at = end
	}
	b.assembly[depth] = assembled
	b.retired = append(b.retired, ref)
	if len(assembled) == 0 {
		return dst, nil
	}
	return b.encodeBranches(dst, view.Header().Level, ref.LogicalID, assembled)
}

// encodeLeaves packs entries into as few leaf pages as fit and appends each as
// a child record for the caller's level.
func (b *keyTreeBatch) encodeLeaves(
	dst []KeyDirectoryChild, logicalID uint64, entries []KeyDirectoryEntry,
) ([]KeyDirectoryChild, error) {
	pageSize := b.tx.options.PageSize
	for len(entries) != 0 {
		limit := 0
		for limit < len(entries) && keyTreeLeafFits(pageSize, entries[:limit+1]) {
			limit++
		}
		if limit == 0 {
			return nil, fmt.Errorf("%w: key does not fit a directory page", ErrInvalidWrite)
		}
		take := treeBatchShare(len(entries), limit)
		page, err := encodeKeyTreeLeaf(b.tx, logicalID, entries[:take], b.bounds)
		if err != nil {
			return nil, err
		}
		lower, err := transactionKeyPageLower(page, b.tx, b.bounds)
		if err != nil {
			return nil, err
		}
		dst = append(dst, KeyDirectoryChild{Lower: lower, Ref: page.Ref()})
		entries = entries[take:]
		logicalID = 0
	}
	return dst, nil
}

func (b *keyTreeBatch) encodeBranches(
	dst []KeyDirectoryChild, level uint8, logicalID uint64, children []KeyDirectoryChild,
) ([]KeyDirectoryChild, error) {
	pageSize := b.tx.options.PageSize
	for len(children) != 0 {
		limit := 0
		for limit < len(children) && keyTreeBranchFits(pageSize, children[:limit+1]) {
			limit++
		}
		if limit == 0 {
			return nil, fmt.Errorf("%w: key lower bounds do not fit a directory page", ErrInvalidWrite)
		}
		take := treeBatchShare(len(children), limit)
		page, err := encodeKeyTreeBranch(b.tx, logicalID, level, children[:take])
		if err != nil {
			return nil, err
		}
		lower, err := transactionKeyPageLower(page, b.tx, b.bounds)
		if err != nil {
			return nil, err
		}
		dst = append(dst, KeyDirectoryChild{Lower: lower, Ref: page.Ref()})
		children = children[take:]
		logicalID = 0
	}
	return dst, nil
}

// treeBatchShare spreads remaining records evenly over the fewest pages that
// can hold them instead of filling each page to its limit.
//
// Greedy maximum packing is optimal for space at the instant it runs, but it
// publishes leaves with no free bytes, so the very next single-key Put into
// that range splits a page it did not have to. Even distribution keeps the
// batch's page count identical while leaving every published page some room.
func treeBatchShare(remaining, limit int) int {
	if limit >= remaining {
		return remaining
	}
	pages := (remaining + limit - 1) / limit
	share := (remaining + pages - 1) / pages
	if share > limit {
		return limit
	}
	return share
}

func (b *keyTreeBatch) pageLevel(ref PageRef) (uint8, error) {
	// A page this descent just staged is still owned by the transaction, and
	// the cache admitted it as dirty, so one lookup answers for both new and
	// untouched roots.
	return keyTreePageLevel(b.cache, ref, b.tx, b.bounds)
}

// KeyTreeBatchPages bounds the pages one MutateKeyTreeBatch descent can
// publish. The descent rewrites each visited page once, so the count is
// bounded by the touched leaves and their ancestors, plus one split per edit
// and one new root level per existing level.
func KeyTreeBatchPages(edits int) int {
	if edits <= 0 {
		return 0
	}
	return 2*edits + 2*int(keyDirectoryMaxLevel) + 2
}
