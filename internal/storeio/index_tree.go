package storeio

import (
	"bytes"
	"errors"
	"fmt"
)

const indexTreeMaxLevel = uint8(10)

// ErrIndexTreeDepth reports that an index-tree edit would exceed the maximum
// branch depth, a fail-closed guard against an unbounded tree.
var ErrIndexTreeDepth = errors.New("vibejson: Store index tree depth exhausted")

// ErrIndexProbeCertificates reports a probe whose copied certificates would
// exceed the 32-bit arena the returned spans address.
var ErrIndexProbeCertificates = errors.New("vibejson: Store index probe certificate arena exhausted")

// IndexTreeBounds are the selecting state-root bounds an index-tree edit
// validates its PageRefs against: the file end, the next logical ID to assign,
// and the index high-water mark. They fail-close a mutation that references a
// page outside the current generation.
type IndexTreeBounds struct {
	FileEnd        uint64
	NextLogicalID  uint64
	IndexHighWater uint32
}

// IndexTreeMutation is the result of an index-tree edit: the new root PageRef
// and the fixed-capacity set of pages the edit retired. The committer publishes
// Root and frees the retired extents as one atomic step, so no reachable page is
// ever reused.
type IndexTreeMutation struct {
	Root         PageRef
	Retired      [16]PageRef
	RetiredCount uint8
	Found        bool
	Changed      bool
}

// IndexTreeProbe is the caller-owned storage for one range append. Leaf
// certificates borrow an evictable page frame, so a traversal that releases
// its lease before returning must copy them; Entries[i].Cert therefore
// addresses Certificates rather than any page. Both slices are appended to and
// may be reused across probes by reslicing them to zero length.
type IndexTreeProbe struct {
	Entries      []IndexDirectoryEntry
	Certificates []byte
	// Leaves counts the distinct leaf pages the traversal admitted, which is
	// the physical read work a probe performed.
	Leaves int
}

// appendEntry copies one borrowed leaf entry into the probe. Neighbouring
// entries of one hash stream share a leaf heap region, so the copy repeats the
// encoder's dedup and keeps a long stream's arena at one representative.
func (p *IndexTreeProbe) appendEntry(view IndexDirectoryView, entry IndexDirectoryEntry) error {
	certificate := view.Certificate(entry.Cert)
	span := CertSpan{}
	if len(certificate) != 0 {
		if count := len(p.Entries); count != 0 {
			previous := p.Entries[count-1].Cert
			if int(previous.Length) == len(certificate) &&
				bytes.Equal(p.Certificates[previous.Offset:int(previous.Offset)+len(certificate)], certificate) {
				span = previous
			}
		}
		if span.Length == 0 {
			// Compared in uint64: the bound is a uint32 maximum, which does
			// not fit an int on a 32-bit platform and would not compile there.
			if uint64(len(p.Certificates)) > uint64(^uint32(0))-uint64(len(certificate)) {
				return ErrIndexProbeCertificates
			}
			span = CertSpan{Offset: uint32(len(p.Certificates)), Length: uint16(len(certificate))}
			p.Certificates = append(p.Certificates, certificate...)
		}
	}
	entry.Cert = span
	p.Entries = append(p.Entries, entry)
	return nil
}

func (m *IndexTreeMutation) retire(ref PageRef) error {
	if int(m.RetiredCount) == len(m.Retired) {
		return ErrIndexTreeDepth
	}
	m.Retired[m.RetiredCount] = ref
	m.RetiredCount++
	return nil
}

// LookupIndexTree resolves one exact routing key and appends its certificate
// to certificates, returning the grown arena. The copy is mandatory: the leaf
// payload is borrowed from the page cache and the lease is released before
// this returns, so a span left addressing the page would be read back from a
// frame that may already hold an unrelated page. The returned entry's Cert
// addresses the returned arena.
func LookupIndexTree(cache *PageCache, root PageRef, key IndexDirectoryKey, bounds IndexTreeBounds, certificates []byte) (IndexDirectoryEntry, []byte, bool, error) {
	if root == (PageRef{}) {
		return IndexDirectoryEntry{}, certificates, false, nil
	}
	ref := root
	for depth := uint8(0); depth <= indexTreeMaxLevel; depth++ {
		lease, err := cache.Acquire(ref)
		if err != nil {
			return IndexDirectoryEntry{}, certificates, false, err
		}
		view, err := OpenIndexDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID, bounds.IndexHighWater)
		if err != nil {
			lease.Release()
			return IndexDirectoryEntry{}, certificates, false, err
		}
		if view.Header().Level == 0 {
			entry, ok := view.Lookup(key)
			if ok {
				certificates, entry.Cert = appendIndexCertificate(certificates, view.Certificate(entry.Cert))
			}
			lease.Release()
			return entry, certificates, ok, nil
		}
		next, ok := view.Child(key)
		lease.Release()
		if !ok {
			return IndexDirectoryEntry{}, certificates, false, nil
		}
		ref = next
	}
	return IndexDirectoryEntry{}, certificates, false, ErrIndexTreeDepth
}

// AppendIndexTreeHash appends every chunk entry for one (index, hash) prefix
// without visiting unrelated leaf subtrees. limit is a hard memory bound.
func AppendIndexTreeHash(cache *PageCache, root PageRef, indexID uint32, tupleHash uint64, bounds IndexTreeBounds, probe *IndexTreeProbe, limit int) error {
	if root == (PageRef{}) || probe == nil {
		return nil
	}
	low := IndexDirectoryKey{IndexID: indexID, TupleHash: tupleHash}
	high := IndexDirectoryKey{IndexID: indexID, TupleHash: tupleHash, Chunk: ^uint32(0)}
	return appendIndexTreeRange(cache, root, low, high, bounds, probe, limit, 0)
}

// WalkIndexTreeIndex visits every leaf intersecting one index id in routing
// order. The borrowed view is valid only during fn. A boundary leaf may also
// contain adjacent index ids; callers must select entries by IndexID.
//
// Keeping the directory page leased across its complete callback lets
// analytical consumers stream a large index without retaining one Go object
// per posting. The traversal stack is bounded by indexTreeMaxLevel.
func WalkIndexTreeIndex(
	cache *PageCache,
	root PageRef,
	indexID uint32,
	bounds IndexTreeBounds,
	fn func(IndexDirectoryView) error,
) error {
	if root == (PageRef{}) || fn == nil {
		return nil
	}
	if indexID >= bounds.IndexHighWater {
		return fmt.Errorf("%w: index-tree walk id", ErrInvalidWrite)
	}
	low := IndexDirectoryKey{IndexID: indexID}
	high := IndexDirectoryKey{
		IndexID: indexID, TupleHash: ^uint64(0), Chunk: ^uint32(0),
	}
	return walkIndexTreeRange(cache, root, low, high, bounds, fn, 0)
}

func walkIndexTreeRange(
	cache *PageCache,
	ref PageRef,
	low, high IndexDirectoryKey,
	bounds IndexTreeBounds,
	fn func(IndexDirectoryView) error,
	depth uint8,
) error {
	if depth > indexTreeMaxLevel {
		return ErrIndexTreeDepth
	}
	lease, err := cache.Acquire(ref)
	if err != nil {
		return err
	}
	view, err := OpenIndexDirectoryPage(
		lease.Page(), bounds.FileEnd, bounds.NextLogicalID, bounds.IndexHighWater,
	)
	if err != nil {
		lease.Release()
		return err
	}
	if view.Header().Level == 0 {
		err = fn(view)
		lease.Release()
		return err
	}
	var childStorage [64]IndexDirectoryChild
	children := childStorage[:view.Len()]
	for i := range children {
		children[i], _ = view.ChildAt(i)
	}
	lease.Release()
	for i, child := range children {
		if i+1 < len(children) &&
			compareIndexDirectoryKey(children[i+1].Lower, low) <= 0 {
			continue
		}
		if compareIndexDirectoryKey(child.Lower, high) > 0 {
			break
		}
		if err := walkIndexTreeRange(
			cache, child.Ref, low, high, bounds, fn, depth+1,
		); err != nil {
			return err
		}
	}
	return nil
}

func appendIndexTreeRange(cache *PageCache, ref PageRef, low, high IndexDirectoryKey, bounds IndexTreeBounds, probe *IndexTreeProbe, limit int, depth uint8) error {
	if depth > indexTreeMaxLevel {
		return ErrIndexTreeDepth
	}
	lease, err := cache.Acquire(ref)
	if err != nil {
		return err
	}
	view, err := OpenIndexDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID, bounds.IndexHighWater)
	if err != nil {
		lease.Release()
		return err
	}
	if view.Header().Level == 0 {
		probe.Leaves++
		for i := 0; i < view.Len(); i++ {
			entry, _ := view.EntryAt(i)
			if compareIndexDirectoryKey(entry.Key, low) < 0 {
				continue
			}
			if compareIndexDirectoryKey(entry.Key, high) > 0 {
				break
			}
			if len(probe.Entries) == limit {
				lease.Release()
				return ErrRetiredExtentCapacity
			}
			if err := probe.appendEntry(view, entry); err != nil {
				lease.Release()
				return err
			}
		}
		lease.Release()
		return nil
	}
	var childStorage [64]IndexDirectoryChild
	children := childStorage[:view.Len()]
	for i := range children {
		children[i], _ = view.ChildAt(i)
	}
	lease.Release()
	for i, child := range children {
		nextLowerBeyond := i+1 < len(children) && compareIndexDirectoryKey(children[i+1].Lower, low) <= 0
		if nextLowerBeyond {
			continue
		}
		if compareIndexDirectoryKey(child.Lower, high) > 0 {
			break
		}
		if err := appendIndexTreeRange(cache, child.Ref, low, high, bounds, probe, limit, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// UpsertIndexTree installs one routing entry. entry.Cert addresses
// certificates, which the tree copies into the rewritten leaf; the caller
// keeps ownership of the arena.
func UpsertIndexTree(cache *PageCache, tx *WriteTransaction, root PageRef, entry IndexDirectoryEntry, certificates []byte, bounds IndexTreeBounds) (IndexTreeMutation, error) {
	return mutateIndexTree(cache, tx, root, entry.Key, entry, certificates, false, bounds)
}

func DeleteIndexTree(cache *PageCache, tx *WriteTransaction, root PageRef, key IndexDirectoryKey, bounds IndexTreeBounds) (IndexTreeMutation, error) {
	return mutateIndexTree(cache, tx, root, key, IndexDirectoryEntry{}, nil, true, bounds)
}

type indexTreeRewrite struct {
	ref                   PageRef
	lower                 IndexDirectoryKey
	rightRef              PageRef
	rightLower            IndexDirectoryKey
	found, changed, empty bool
}

func mutateIndexTree(cache *PageCache, tx *WriteTransaction, root PageRef, key IndexDirectoryKey, entry IndexDirectoryEntry, certificates []byte, deleting bool, bounds IndexTreeBounds) (IndexTreeMutation, error) {
	var mutation IndexTreeMutation
	if tx == nil || !tx.active || bounds.IndexHighWater == 0 || key.IndexID >= bounds.IndexHighWater || cache == nil && root != (PageRef{}) {
		return mutation, fmt.Errorf("%w: index-tree mutation", ErrInvalidWrite)
	}
	if !deleting {
		// A mask of zero is a live key that answers "no rows" — the same answer
		// a missing key gives, except that it survives every later probe and
		// hides the rows that ought to have been re-established. Writers route
		// an emptied mask to DeleteIndexTree instead.
		if _, ok := indexDirectoryArenaCertificate(certificates, entry.Cert); !ok || entry.Bits == 0 {
			return mutation, fmt.Errorf("%w: index-tree entry mask or certificate", ErrInvalidWrite)
		}
	}
	if root == (PageRef{}) {
		if deleting {
			return mutation, nil
		}
		page, err := encodeIndexTreeLeaf(tx, 0, []IndexDirectoryEntry{entry}, certificates, bounds)
		if err != nil {
			return mutation, err
		}
		mutation.Root, mutation.Changed = page.Ref(), true
		return mutation, nil
	}
	result, err := rewriteIndexTreePage(cache, tx, root, key, entry, certificates, deleting, bounds, true, &mutation)
	if err != nil {
		return mutation, err
	}
	mutation.Found, mutation.Changed = result.found, result.changed
	if !result.changed {
		mutation.Root = root
		return mutation, nil
	}
	if result.empty {
		return mutation, nil
	}
	mutation.Root = result.ref
	if result.rightRef == (PageRef{}) {
		return mutation, nil
	}
	level, err := indexTreePageLevel(cache, result.ref, tx, bounds)
	if err != nil {
		return mutation, err
	}
	if level == indexTreeMaxLevel {
		return mutation, ErrIndexTreeDepth
	}
	page, err := encodeIndexTreeBranch(tx, 0, level+1, []IndexDirectoryChild{{Lower: result.lower, Ref: result.ref}, {Lower: result.rightLower, Ref: result.rightRef}})
	if err != nil {
		return mutation, err
	}
	mutation.Root = page.Ref()
	return mutation, nil
}

func rewriteIndexTreePage(cache *PageCache, tx *WriteTransaction, ref PageRef, key IndexDirectoryKey, entry IndexDirectoryEntry, certificates []byte, deleting bool, bounds IndexTreeBounds, root bool, mutation *IndexTreeMutation) (indexTreeRewrite, error) {
	lease, err := cache.Acquire(ref)
	if err != nil {
		return indexTreeRewrite{}, err
	}
	defer lease.Release()
	view, err := OpenIndexDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID, bounds.IndexHighWater)
	if err != nil {
		return indexTreeRewrite{}, err
	}
	if view.Header().Level == 0 {
		return rewriteIndexTreeLeaf(tx, ref, view, key, entry, certificates, deleting, bounds, mutation)
	}
	return rewriteIndexTreeBranch(cache, tx, ref, view, key, entry, certificates, deleting, bounds, root, mutation)
}

// rewriteIndexTreeLeaf merges one mutation into a borrowed leaf. Certificates
// are copied into a private arena rather than referenced, because the merged
// sequence mixes representatives from the old page with the caller's and the
// encoder needs one arena addressing both — and because a split re-derives
// each half's heap, which the borrowed page cannot describe.
func rewriteIndexTreeLeaf(tx *WriteTransaction, oldRef PageRef, view IndexDirectoryView, key IndexDirectoryKey, entry IndexDirectoryEntry, certificates []byte, deleting bool, bounds IndexTreeBounds, mutation *IndexTreeMutation) (indexTreeRewrite, error) {
	entries := make([]IndexDirectoryEntry, 0, view.Len()+1)
	arena := make([]byte, 0, indexDirectoryLeafBudget(tx.options.PageSize)+int(entry.Cert.Length))
	appendMerged := func(merged IndexDirectoryEntry, certificate []byte) {
		span := CertSpan{}
		if len(certificate) != 0 {
			// Repeat the encoder's neighbour dedup while copying so the arena
			// stays bounded by one leaf's heap plus the inserted
			// representative. Copying each entry independently would multiply
			// a shared representative by the leaf's fanout.
			if count := len(entries); count != 0 {
				previous := entries[count-1].Cert
				if int(previous.Length) == len(certificate) &&
					bytes.Equal(arena[previous.Offset:int(previous.Offset)+len(certificate)], certificate) {
					span = previous
				}
			}
			if span.Length == 0 {
				arena, span = appendIndexCertificate(arena, certificate)
			}
		}
		merged.Cert = span
		entries = append(entries, merged)
	}
	inserted, _ := indexDirectoryArenaCertificate(certificates, entry.Cert)
	found, emitted := false, deleting
	for i := 0; i < view.Len(); i++ {
		current, _ := view.EntryAt(i)
		comparison := compareIndexDirectoryKey(current.Key, key)
		if comparison == 0 {
			found = true
			if !emitted {
				appendMerged(entry, inserted)
				emitted = true
			}
			continue
		}
		if comparison > 0 && !emitted {
			appendMerged(entry, inserted)
			emitted = true
		}
		appendMerged(current, view.Certificate(current.Cert))
	}
	if !emitted {
		appendMerged(entry, inserted)
	}
	if deleting && !found {
		return indexTreeRewrite{ref: oldRef}, nil
	}
	if err := mutation.retire(oldRef); err != nil {
		return indexTreeRewrite{}, err
	}
	if len(entries) == 0 {
		return indexTreeRewrite{found: true, changed: true, empty: true}, nil
	}
	fits, err := indexTreeLeafFits(tx.options.PageSize, entries, arena)
	if err != nil {
		return indexTreeRewrite{}, err
	}
	if fits {
		page, err := encodeIndexTreeLeaf(tx, oldRef.LogicalID, entries, arena, bounds)
		if err != nil {
			return indexTreeRewrite{}, err
		}
		return indexTreeRewrite{ref: page.Ref(), lower: entries[0].Key, found: found, changed: true}, nil
	}
	split, err := indexTreeLeafSplit(tx.options.PageSize, entries, arena)
	if err != nil {
		return indexTreeRewrite{}, err
	}
	left, err := encodeIndexTreeLeaf(tx, oldRef.LogicalID, entries[:split], arena, bounds)
	if err != nil {
		return indexTreeRewrite{}, err
	}
	right, err := encodeIndexTreeLeaf(tx, 0, entries[split:], arena, bounds)
	if err != nil {
		return indexTreeRewrite{}, err
	}
	return indexTreeRewrite{ref: left.Ref(), lower: entries[0].Key, rightRef: right.Ref(), rightLower: entries[split].Key, found: found, changed: true}, nil
}

func rewriteIndexTreeBranch(cache *PageCache, tx *WriteTransaction, oldRef PageRef, view IndexDirectoryView, key IndexDirectoryKey, entry IndexDirectoryEntry, certificates []byte, deleting bool, bounds IndexTreeBounds, root bool, mutation *IndexTreeMutation) (indexTreeRewrite, error) {
	var children [65]IndexDirectoryChild
	count := view.Len()
	for i := 0; i < count; i++ {
		children[i], _ = view.ChildAt(i)
	}
	rank := indexTreeChildRank(children[:count], key)
	if rank < 0 {
		if deleting {
			return indexTreeRewrite{ref: oldRef}, nil
		}
		rank = 0
	}
	child, err := rewriteIndexTreePage(cache, tx, children[rank].Ref, key, entry, certificates, deleting, bounds, false, mutation)
	if err != nil {
		return indexTreeRewrite{}, err
	}
	if !child.changed {
		return indexTreeRewrite{ref: oldRef, found: child.found}, nil
	}
	if child.empty {
		copy(children[rank:], children[rank+1:count])
		count--
	} else {
		children[rank] = IndexDirectoryChild{Lower: child.lower, Ref: child.ref}
		if child.rightRef != (PageRef{}) {
			copy(children[rank+2:], children[rank+1:count])
			children[rank+1] = IndexDirectoryChild{Lower: child.rightLower, Ref: child.rightRef}
			count++
		}
	}
	if err := mutation.retire(oldRef); err != nil {
		return indexTreeRewrite{}, err
	}
	if count == 0 {
		return indexTreeRewrite{found: child.found, changed: true, empty: true}, nil
	}
	if root && count == 1 {
		return indexTreeRewrite{ref: children[0].Ref, lower: children[0].Lower, found: child.found, changed: true}, nil
	}
	level := view.Header().Level
	if count <= 64 {
		page, err := encodeIndexTreeBranch(tx, oldRef.LogicalID, level, children[:count])
		if err != nil {
			return indexTreeRewrite{}, err
		}
		return indexTreeRewrite{ref: page.Ref(), lower: children[0].Lower, found: child.found, changed: true}, nil
	}
	split := count / 2
	left, err := encodeIndexTreeBranch(tx, oldRef.LogicalID, level, children[:split])
	if err != nil {
		return indexTreeRewrite{}, err
	}
	right, err := encodeIndexTreeBranch(tx, 0, level, children[split:count])
	if err != nil {
		return indexTreeRewrite{}, err
	}
	return indexTreeRewrite{ref: left.Ref(), lower: children[0].Lower, rightRef: right.Ref(), rightLower: children[split].Lower, found: child.found, changed: true}, nil
}

func encodeIndexTreeLeaf(tx *WriteTransaction, logicalID uint64, entries []IndexDirectoryEntry, certificates []byte, bounds IndexTreeBounds) (TransactionPage, error) {
	page, err := tx.Allocate(PageIndexDirectory, tx.options.PageSize, logicalID)
	if err != nil {
		return TransactionPage{}, err
	}
	header := IndexDirectoryHeader{StoreID: tx.options.StoreID, Generation: tx.options.Generation, LogicalID: page.Ref().LogicalID, PageSize: page.Ref().Length}
	if _, err := EncodeIndexDirectoryLeaf(page.Bytes(), header, entries, certificates, tx.NextLogicalID(), bounds.IndexHighWater); err != nil {
		return TransactionPage{}, err
	}
	if err := page.Stage(); err != nil {
		return TransactionPage{}, err
	}
	return page, nil
}

func encodeIndexTreeBranch(tx *WriteTransaction, logicalID uint64, level uint8, children []IndexDirectoryChild) (TransactionPage, error) {
	page, err := tx.Allocate(PageIndexDirectory, tx.options.PageSize, logicalID)
	if err != nil {
		return TransactionPage{}, err
	}
	header := IndexDirectoryHeader{StoreID: tx.options.StoreID, Generation: tx.options.Generation, LogicalID: page.Ref().LogicalID, PageSize: page.Ref().Length, Level: level}
	if _, err := EncodeIndexDirectoryBranch(page.Bytes(), header, children, tx.FileEnd(), tx.NextLogicalID()); err != nil {
		return TransactionPage{}, err
	}
	if err := page.Stage(); err != nil {
		return TransactionPage{}, err
	}
	return page, nil
}

func indexTreePageLevel(cache *PageCache, ref PageRef, tx *WriteTransaction, bounds IndexTreeBounds) (uint8, error) {
	lease, err := cache.Acquire(ref)
	if err != nil {
		return 0, err
	}
	defer lease.Release()
	view, err := OpenIndexDirectoryPage(lease.Page(), tx.FileEnd(), tx.NextLogicalID(), bounds.IndexHighWater)
	if err != nil {
		return 0, err
	}
	return view.Header().Level, nil
}

func appendIndexCertificate(dst, certificate []byte) ([]byte, CertSpan) {
	if len(certificate) == 0 {
		return dst, CertSpan{}
	}
	span := CertSpan{Offset: uint32(len(dst)), Length: uint16(len(certificate))}
	return append(dst, certificate...), span
}

// indexTreeLeafFits answers the byte question, not an entry-count question:
// records are fixed but the certificate heap is not, so a leaf that holds n
// short representatives may not hold n long ones.
func indexTreeLeafFits(pageSize uint32, entries []IndexDirectoryEntry, certificates []byte) (bool, error) {
	used, err := indexDirectoryLeafBytes(entries, certificates, IndexDirectoryMaxCertificate(pageSize))
	if err != nil {
		return false, err
	}
	return used <= indexDirectoryLeafBudget(pageSize), nil
}

// indexTreeLeafSplit picks the cut that leaves the two halves closest in
// bytes among the cuts where both halves fit. Splitting by cumulative bytes
// rather than by entry count is what keeps both halves admissible when
// representatives differ in length, and balancing rather than filling the left
// half to capacity is what keeps occupancy up: a maximal left half is full the
// moment it is written and splits again on the very next key that routes to
// it, which for randomly ordered tuple hashes collapses a leaf to a handful of
// entries. IndexDirectoryMaxCertificate guarantees at least one feasible cut.
//
// Cutting at k costs the right half the dedup its first entry enjoyed inside
// the whole sequence, so its size is the untouched remainder plus that one
// re-materialized representative. Both halves are therefore priced in a single
// pass over the sequence without materializing prefix sums.
func indexTreeLeafSplit(pageSize uint32, entries []IndexDirectoryEntry, certificates []byte) (int, error) {
	budget := indexDirectoryLeafBudget(pageSize)
	maxCertificate := IndexDirectoryMaxCertificate(pageSize)
	total, err := indexDirectoryLeafBytes(entries, certificates, maxCertificate)
	if err != nil {
		return 0, err
	}
	split, skew, left := 0, 0, 0
	var previous []byte
	for i := range entries {
		certificate, _ := indexDirectoryArenaCertificate(certificates, entries[i].Cert)
		unshared := IndexDirectoryLeafRecordSize + len(certificate)
		cost := unshared
		if i != 0 && bytes.Equal(certificate, previous) {
			cost = IndexDirectoryLeafRecordSize
		}
		previous = certificate
		if i != 0 {
			right := total - left - cost + unshared
			if difference := left - right; left <= budget && right <= budget {
				if difference < 0 {
					difference = -difference
				}
				if split == 0 || difference < skew {
					split, skew = i, difference
				}
			}
		}
		left += cost
	}
	if split == 0 {
		return 0, fmt.Errorf("%w: index leaf split", ErrInvalidWrite)
	}
	return split, nil
}

func indexTreeChildRank(children []IndexDirectoryChild, key IndexDirectoryKey) int {
	low, high := 0, len(children)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if compareIndexDirectoryKey(children[middle].Lower, key) <= 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low - 1
}
