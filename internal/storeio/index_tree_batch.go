package storeio

import (
	"bytes"
	"fmt"
)

// IndexTreeEdit is one routing-key mutation inside a sorted batch. Delete
// removes the key; otherwise Entry replaces or inserts it wholesale, with
// Entry.Cert addressing the caller's certificate arena.
//
// A batch edit carries the final mask rather than bits to set or clear. The
// mask a posting ends up with depends on the exact-value certificate already
// stored under the same tuple hash, and that comparison is semantic JSON
// equality, which this package cannot perform. The collection therefore
// resolves each routing key against the tree before the descent starts —
// reads only, no pages written — and hands down decided entries.
type IndexTreeEdit struct {
	Entry  IndexDirectoryEntry
	Delete bool
}

// IndexTreeBatchMutation is the result of one batched index-directory descent.
type IndexTreeBatchMutation struct {
	Root    PageRef
	Retired []PageRef
	Changed bool
}

// MutateIndexTreeBatch applies edits, sorted strictly ascending by routing key,
// in one copy-on-write descent that rewrites every visited page exactly once.
//
// The sequential alternative is what makes batched writes impossible for an
// indexed collection rather than merely slower: tuple hashes are uniformly
// distributed, so sixty-four documents touch sixty-four different leaves, and
// one UpsertIndexTree per document rewrites the shared branch path sixty-four
// times. Those pages are charged against the transaction's page reservation,
// so the batch would need a reservation several hundred pages wide to publish a
// tree that keeps fewer than eighty.
func MutateIndexTreeBatch(
	cache *PageCache, tx *WriteTransaction, root PageRef,
	edits []IndexTreeEdit, certificates []byte, bounds IndexTreeBounds, retired []PageRef,
) (IndexTreeBatchMutation, error) {
	mutation := IndexTreeBatchMutation{Root: root, Retired: retired}
	if tx == nil || !tx.active || bounds.IndexHighWater == 0 || (cache == nil && root != (PageRef{})) {
		return mutation, fmt.Errorf("%w: index-tree batch bounds", ErrInvalidWrite)
	}
	for i, edit := range edits {
		if edit.Entry.Key.IndexID >= bounds.IndexHighWater {
			return mutation, fmt.Errorf("%w: index-tree batch index id", ErrInvalidWrite)
		}
		if i != 0 && compareIndexDirectoryKey(edits[i-1].Entry.Key, edit.Entry.Key) >= 0 {
			return mutation, fmt.Errorf("%w: unsorted index-tree batch", ErrInvalidWrite)
		}
		if edit.Delete {
			continue
		}
		if _, ok := indexDirectoryArenaCertificate(certificates, edit.Entry.Cert); !ok || edit.Entry.Bits == 0 {
			return mutation, fmt.Errorf("%w: index-tree batch entry mask or certificate", ErrInvalidWrite)
		}
	}
	if len(edits) == 0 {
		return mutation, nil
	}

	batch := indexTreeBatch{
		cache: cache, tx: tx, bounds: bounds, certificates: certificates, retired: retired,
	}
	if root == (PageRef{}) {
		batch.entries = batch.entries[:0]
		batch.arena = batch.arena[:0]
		for _, edit := range edits {
			if edit.Delete {
				continue
			}
			certificate, _ := indexDirectoryArenaCertificate(certificates, edit.Entry.Cert)
			batch.appendMerged(edit.Entry, certificate)
		}
		if len(batch.entries) == 0 {
			return mutation, nil
		}
		children, err := batch.encodeLeaves(nil, 0)
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

type indexTreeBatch struct {
	cache        *PageCache
	tx           *WriteTransaction
	bounds       IndexTreeBounds
	certificates []byte
	retired      []PageRef
	entries      []IndexDirectoryEntry
	arena        []byte
	assembly     [indexTreeMaxLevel + 2][]IndexDirectoryChild
}

func (b *indexTreeBatch) finish(children []IndexDirectoryChild) (IndexTreeBatchMutation, error) {
	mutation := IndexTreeBatchMutation{Retired: b.retired, Changed: true}
	level := uint8(0)
	if len(children) > 1 {
		var err error
		level, err = indexTreePageLevel(b.cache, children[0].Ref, b.tx, b.bounds)
		if err != nil {
			return mutation, err
		}
	}
	for len(children) > 1 {
		if level >= indexTreeMaxLevel {
			return mutation, ErrIndexTreeDepth
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

func (b *indexTreeBatch) rewritePage(
	dst []IndexDirectoryChild, ref PageRef, edits []IndexTreeEdit, depth int,
) ([]IndexDirectoryChild, error) {
	if depth > int(indexTreeMaxLevel) {
		return nil, ErrIndexTreeDepth
	}
	lease, err := b.cache.Acquire(ref)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	view, err := OpenIndexDirectoryPage(
		lease.Page(), b.bounds.FileEnd, b.bounds.NextLogicalID, b.bounds.IndexHighWater,
	)
	if err != nil {
		return nil, err
	}
	if view.Header().Level == 0 {
		return b.rewriteLeaf(dst, ref, view, edits)
	}
	return b.rewriteBranch(dst, ref, view, edits, depth)
}

// appendMerged copies one entry and its certificate into the batch's private
// arena, repeating the encoder's neighbour dedup so a representative shared by
// a whole hash stream is stored once rather than once per chunk.
func (b *indexTreeBatch) appendMerged(entry IndexDirectoryEntry, certificate []byte) {
	span := CertSpan{}
	if len(certificate) != 0 {
		if count := len(b.entries); count != 0 {
			previous := b.entries[count-1].Cert
			if int(previous.Length) == len(certificate) &&
				bytes.Equal(b.arena[previous.Offset:int(previous.Offset)+len(certificate)], certificate) {
				span = previous
			}
		}
		if span.Length == 0 {
			b.arena, span = appendIndexCertificate(b.arena, certificate)
		}
	}
	entry.Cert = span
	b.entries = append(b.entries, entry)
}

func (b *indexTreeBatch) rewriteLeaf(
	dst []IndexDirectoryChild, ref PageRef, view IndexDirectoryView, edits []IndexTreeEdit,
) ([]IndexDirectoryChild, error) {
	b.entries = b.entries[:0]
	b.arena = b.arena[:0]
	at := 0
	emit := func(edit IndexTreeEdit) {
		if edit.Delete {
			return
		}
		certificate, _ := indexDirectoryArenaCertificate(b.certificates, edit.Entry.Cert)
		b.appendMerged(edit.Entry, certificate)
	}
	for i := 0; i < view.Len(); i++ {
		current, _ := view.EntryAt(i)
		for at < len(edits) && compareIndexDirectoryKey(edits[at].Entry.Key, current.Key) < 0 {
			emit(edits[at])
			at++
		}
		if at < len(edits) && compareIndexDirectoryKey(edits[at].Entry.Key, current.Key) == 0 {
			emit(edits[at])
			at++
			continue
		}
		b.appendMerged(current, view.Certificate(current.Cert))
	}
	for ; at < len(edits); at++ {
		emit(edits[at])
	}
	b.retired = append(b.retired, ref)
	if len(b.entries) == 0 {
		return dst, nil
	}
	return b.encodeLeaves(dst, ref.LogicalID)
}

func (b *indexTreeBatch) rewriteBranch(
	dst []IndexDirectoryChild, ref PageRef, view IndexDirectoryView, edits []IndexTreeEdit, depth int,
) ([]IndexDirectoryChild, error) {
	count := view.Len()
	if count == 0 {
		return nil, ErrIndexDirectoryCorrupt
	}
	assembled := b.assembly[depth][:0]
	at := 0
	for rank := range count {
		child, _ := view.ChildAt(rank)
		end := len(edits)
		if rank+1 < count {
			next, _ := view.ChildAt(rank + 1)
			end = at
			for end < len(edits) &&
				compareIndexDirectoryKey(edits[end].Entry.Key, next.Lower) < 0 {
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

// encodeLeaves packs b.entries into as few leaf pages as fit. The entry and
// arena scratch are consumed here, so no recursion may run between the merge
// that filled them and this call.
func (b *indexTreeBatch) encodeLeaves(
	dst []IndexDirectoryChild, logicalID uint64,
) ([]IndexDirectoryChild, error) {
	entries := b.entries
	for len(entries) != 0 {
		limit, err := IndexDirectoryLeafPrefix(entries, b.arena, b.tx.options.PageSize)
		if err != nil {
			return nil, err
		}
		if limit == 0 {
			return nil, fmt.Errorf("%w: index entry does not fit a directory page", ErrInvalidWrite)
		}
		take := treeBatchShare(len(entries), limit)
		page, err := encodeIndexTreeLeaf(b.tx, logicalID, entries[:take], b.arena, b.bounds)
		if err != nil {
			return nil, err
		}
		dst = append(dst, IndexDirectoryChild{Lower: entries[0].Key, Ref: page.Ref()})
		entries = entries[take:]
		logicalID = 0
	}
	return dst, nil
}

func (b *indexTreeBatch) encodeBranches(
	dst []IndexDirectoryChild, level uint8, logicalID uint64, children []IndexDirectoryChild,
) ([]IndexDirectoryChild, error) {
	limit := indexTreeBranchLimit(b.tx.options.PageSize)
	if limit == 0 {
		return nil, fmt.Errorf("%w: index branch does not fit a directory page", ErrInvalidWrite)
	}
	for len(children) != 0 {
		take := treeBatchShare(len(children), limit)
		page, err := encodeIndexTreeBranch(b.tx, logicalID, level, children[:take])
		if err != nil {
			return nil, err
		}
		dst = append(dst, IndexDirectoryChild{Lower: children[0].Lower, Ref: page.Ref()})
		children = children[take:]
		logicalID = 0
	}
	return dst, nil
}

// indexTreeBranchLimit is how many fixed-size children one branch page holds.
func indexTreeBranchLimit(pageSize uint32) int {
	room := int(pageSize) - PageHeaderSize - PageTrailerSize - IndexDirectoryPayloadHeaderSize
	if room < 0 {
		return 0
	}
	return min(64, room/IndexDirectoryBranchRecordSize)
}

// IndexTreeBatchPages bounds the pages one MutateIndexTreeBatch descent can
// publish, on the same reasoning as KeyTreeBatchPages.
func IndexTreeBatchPages(edits int) int {
	if edits <= 0 {
		return 0
	}
	return 2*edits + 2*int(indexTreeMaxLevel) + 2
}
