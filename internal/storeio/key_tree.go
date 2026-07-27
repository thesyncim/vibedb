package storeio

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrKeyTreeDepth reports a key tree whose configured durable level bound or
// fixed retirement scratch would be exceeded.
var ErrKeyTreeDepth = errors.New("vibejson: Store key tree depth exhausted")

// KeyTreeBounds are copied from the currently selected state root and
// superblock when old pages are admitted.
type KeyTreeBounds struct {
	FileEnd        uint64
	NextLogicalID  uint64
	ChunkHighWater uint32
	ChunkDocuments uint8
}

// KeyTreeMutation is the result of one copy-on-write upsert or delete. Retired
// pages remain protected by the old generation and must enter the extent
// reclaimer only after publication succeeds.
type KeyTreeMutation struct {
	Root         PageRef
	Retired      [16]PageRef
	RetiredCount uint8
	Found        bool
	Changed      bool
}

func (m *KeyTreeMutation) retire(ref PageRef) error {
	if int(m.RetiredCount) == len(m.Retired) {
		return ErrKeyTreeDepth
	}
	m.Retired[m.RetiredCount] = ref
	m.RetiredCount++
	return nil
}

// LookupKeyTree resolves a complete key through bounded leased pages.
func LookupKeyTree(cache *PageCache, root PageRef, key []byte, bounds KeyTreeBounds) (KeyLocation, bool, error) {
	if root == (PageRef{}) {
		return KeyLocation{}, false, nil
	}
	if cache == nil {
		return KeyLocation{}, false, fmt.Errorf("%w: nil key-tree cache", ErrInvalidWrite)
	}
	// Whether admission already proved these pages well formed is a property of
	// the cache, not of a level, so it is read once rather than per descent.
	admitted := cache.ValidatesOnAdmission()
	ref := root
	for depth := uint8(0); depth <= keyDirectoryMaxLevel; depth++ {
		// The kind guard is what lets the admitted reconstruction below be safe:
		// Acquire proves the resident page's own header equals ref, so refusing a
		// non-directory ref here refuses reinterpreting some other schema's bytes
		// as key-directory records.
		if ref.Kind != PageKeyDirectory {
			return KeyLocation{}, false, fmt.Errorf("%w: key-tree reference kind", ErrKeyDirectoryCorrupt)
		}
		lease, err := cache.Acquire(ref)
		if err != nil {
			return KeyLocation{}, false, err
		}
		var view KeyDirectoryView
		if admitted {
			view = AdmittedKeyDirectoryPage(lease.Page())
		} else if view, err = OpenKeyDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID, bounds.ChunkHighWater, bounds.ChunkDocuments); err != nil {
			lease.Release()
			return KeyLocation{}, false, err
		}
		if view.Header().Level == 0 {
			location, ok := view.Lookup(key)
			lease.Release()
			return location, ok, nil
		}
		next, ok := view.Child(key)
		lease.Release()
		if !ok {
			return KeyLocation{}, false, nil
		}
		ref = next
	}
	return KeyLocation{}, false, ErrKeyTreeDepth
}

// UpsertKeyTree inserts or replaces key and returns a new immutable root.
func UpsertKeyTree(cache *PageCache, tx *WriteTransaction, root PageRef, key []byte, location KeyLocation, bounds KeyTreeBounds) (KeyTreeMutation, error) {
	return mutateKeyTree(cache, tx, root, key, location, false, bounds)
}

// DeleteKeyTree removes key. A missing key produces Changed=false and writes
// no pages.
func DeleteKeyTree(cache *PageCache, tx *WriteTransaction, root PageRef, key []byte, bounds KeyTreeBounds) (KeyTreeMutation, error) {
	return mutateKeyTree(cache, tx, root, key, KeyLocation{}, true, bounds)
}

type keyTreeRewrite struct {
	ref        PageRef
	lower      []byte
	rightRef   PageRef
	rightLower []byte
	found      bool
	changed    bool
	empty      bool
}

func mutateKeyTree(cache *PageCache, tx *WriteTransaction, root PageRef, key []byte, location KeyLocation, deleting bool, bounds KeyTreeBounds) (KeyTreeMutation, error) {
	var mutation KeyTreeMutation
	if tx == nil || !tx.active || cache == nil && root != (PageRef{}) ||
		bounds.ChunkHighWater == 0 || bounds.ChunkDocuments == 0 || bounds.ChunkDocuments > 64 ||
		!deleting && (location.Chunk >= bounds.ChunkHighWater || location.Slot >= bounds.ChunkDocuments) {
		return mutation, fmt.Errorf("%w: key-tree mutation bounds", ErrInvalidWrite)
	}
	if root == (PageRef{}) {
		if deleting {
			return mutation, nil
		}
		page, err := tx.Allocate(PageKeyDirectory, tx.options.PageSize, 0)
		if err != nil {
			return mutation, err
		}
		entry := [1]KeyDirectoryEntry{{Key: key, Location: location}}
		if _, err := EncodeKeyDirectoryLeaf(page.Bytes(), keyTreeHeader(tx, page.Ref(), 0), entry[:], tx.NextLogicalID(), bounds.ChunkHighWater, bounds.ChunkDocuments); err != nil {
			return mutation, err
		}
		if err := page.Stage(); err != nil {
			return mutation, err
		}
		mutation.Root = page.Ref()
		mutation.Changed = true
		return mutation, nil
	}

	result, err := rewriteKeyTreePage(cache, tx, root, key, location, deleting, bounds, true, &mutation)
	if err != nil {
		return mutation, err
	}
	mutation.Found = result.found
	mutation.Changed = result.changed
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
	level, err := keyTreePageLevel(cache, result.ref, tx, bounds)
	if err != nil {
		return mutation, err
	}
	if level == keyDirectoryMaxLevel {
		return mutation, ErrKeyTreeDepth
	}
	rootPage, err := tx.Allocate(PageKeyDirectory, tx.options.PageSize, 0)
	if err != nil {
		return mutation, err
	}
	children := [2]KeyDirectoryChild{
		{Lower: result.lower, Ref: result.ref},
		{Lower: result.rightLower, Ref: result.rightRef},
	}
	if _, err := EncodeKeyDirectoryBranch(rootPage.Bytes(), keyTreeHeader(tx, rootPage.Ref(), level+1), children[:], tx.FileEnd(), tx.NextLogicalID()); err != nil {
		return mutation, err
	}
	if err := rootPage.Stage(); err != nil {
		return mutation, err
	}
	mutation.Root = rootPage.Ref()
	return mutation, nil
}

func rewriteKeyTreePage(cache *PageCache, tx *WriteTransaction, ref PageRef, key []byte, location KeyLocation, deleting bool, bounds KeyTreeBounds, root bool, mutation *KeyTreeMutation) (keyTreeRewrite, error) {
	lease, err := cache.Acquire(ref)
	if err != nil {
		return keyTreeRewrite{}, err
	}
	defer lease.Release()
	view, err := OpenKeyDirectoryPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID, bounds.ChunkHighWater, bounds.ChunkDocuments)
	if err != nil {
		return keyTreeRewrite{}, err
	}
	if view.Header().Level == 0 {
		return rewriteKeyTreeLeaf(tx, ref, view, key, location, deleting, bounds, mutation)
	}
	return rewriteKeyTreeBranch(cache, tx, ref, view, key, location, deleting, bounds, root, mutation)
}

func rewriteKeyTreeLeaf(tx *WriteTransaction, oldRef PageRef, view KeyDirectoryView, key []byte, location KeyLocation, deleting bool, bounds KeyTreeBounds, mutation *KeyTreeMutation) (keyTreeRewrite, error) {
	entries := make([]KeyDirectoryEntry, view.Len()+1)
	position := 0
	found := false
	for i := 0; i < view.Len(); i++ {
		entry, _ := view.EntryAt(i)
		comparison := bytes.Compare(entry.Key, key)
		if comparison < 0 {
			entries[position] = entry
			position++
			continue
		}
		if comparison == 0 {
			found = true
			if !deleting {
				entry.Location = location
				entries[position] = entry
				position++
			}
			for j := i + 1; j < view.Len(); j++ {
				entries[position], _ = view.EntryAt(j)
				position++
			}
			break
		}
		if !deleting {
			entries[position] = KeyDirectoryEntry{Key: key, Location: location}
			position++
		}
		for j := i; j < view.Len(); j++ {
			entries[position], _ = view.EntryAt(j)
			position++
		}
		break
	}
	if position == view.Len() && !found && !deleting {
		entries[position] = KeyDirectoryEntry{Key: key, Location: location}
		position++
	}
	if deleting && !found {
		return keyTreeRewrite{ref: oldRef}, nil
	}
	entries = entries[:position]
	if err := mutation.retire(oldRef); err != nil {
		return keyTreeRewrite{}, err
	}
	if len(entries) == 0 {
		return keyTreeRewrite{found: true, changed: true, empty: true}, nil
	}
	if keyTreeLeafFits(tx.options.PageSize, entries) {
		page, err := encodeKeyTreeLeaf(tx, oldRef.LogicalID, entries, bounds)
		if err != nil {
			return keyTreeRewrite{}, err
		}
		lower, err := transactionKeyPageLower(page, tx, bounds)
		if err != nil {
			return keyTreeRewrite{}, err
		}
		return keyTreeRewrite{ref: page.Ref(), lower: lower, found: found, changed: true}, nil
	}
	split := keyTreeLeafSplit(tx.options.PageSize, entries)
	if split == 0 {
		return keyTreeRewrite{}, fmt.Errorf("%w: key does not fit a directory page", ErrInvalidWrite)
	}
	left, err := encodeKeyTreeLeaf(tx, oldRef.LogicalID, entries[:split], bounds)
	if err != nil {
		return keyTreeRewrite{}, err
	}
	right, err := encodeKeyTreeLeaf(tx, 0, entries[split:], bounds)
	if err != nil {
		return keyTreeRewrite{}, err
	}
	leftLower, err := transactionKeyPageLower(left, tx, bounds)
	if err != nil {
		return keyTreeRewrite{}, err
	}
	rightLower, err := transactionKeyPageLower(right, tx, bounds)
	if err != nil {
		return keyTreeRewrite{}, err
	}
	return keyTreeRewrite{
		ref: left.Ref(), lower: leftLower, rightRef: right.Ref(), rightLower: rightLower,
		found: found, changed: true,
	}, nil
}

func rewriteKeyTreeBranch(cache *PageCache, tx *WriteTransaction, oldRef PageRef, view KeyDirectoryView, key []byte, location KeyLocation, deleting bool, bounds KeyTreeBounds, root bool, mutation *KeyTreeMutation) (keyTreeRewrite, error) {
	var children [65]KeyDirectoryChild
	count := view.Len()
	for i := 0; i < count; i++ {
		children[i], _ = view.ChildAt(i)
	}
	rank := keyTreeChildRank(children[:count], key)
	if rank < 0 {
		if deleting {
			return keyTreeRewrite{ref: oldRef}, nil
		}
		rank = 0
	}
	child, err := rewriteKeyTreePage(cache, tx, children[rank].Ref, key, location, deleting, bounds, false, mutation)
	if err != nil {
		return keyTreeRewrite{}, err
	}
	if !child.changed {
		return keyTreeRewrite{ref: oldRef, found: child.found}, nil
	}
	if child.empty {
		copy(children[rank:], children[rank+1:count])
		count--
	} else {
		children[rank] = KeyDirectoryChild{Lower: child.lower, Ref: child.ref}
		if child.rightRef != (PageRef{}) {
			copy(children[rank+2:], children[rank+1:count])
			children[rank+1] = KeyDirectoryChild{Lower: child.rightLower, Ref: child.rightRef}
			count++
		}
	}
	if err := mutation.retire(oldRef); err != nil {
		return keyTreeRewrite{}, err
	}
	if count == 0 {
		return keyTreeRewrite{found: child.found, changed: true, empty: true}, nil
	}
	if root && count == 1 {
		return keyTreeRewrite{
			ref: children[0].Ref, lower: children[0].Lower,
			found: child.found, changed: true,
		}, nil
	}
	level := view.Header().Level
	if count <= 64 && keyTreeBranchFits(tx.options.PageSize, children[:count]) {
		page, err := encodeKeyTreeBranch(tx, oldRef.LogicalID, level, children[:count])
		if err != nil {
			return keyTreeRewrite{}, err
		}
		lower, err := transactionKeyPageLower(page, tx, bounds)
		if err != nil {
			return keyTreeRewrite{}, err
		}
		return keyTreeRewrite{ref: page.Ref(), lower: lower, found: child.found, changed: true}, nil
	}
	split := keyTreeBranchSplit(tx.options.PageSize, children[:count])
	if split == 0 {
		return keyTreeRewrite{}, fmt.Errorf("%w: key lower bounds do not fit a directory page", ErrInvalidWrite)
	}
	left, err := encodeKeyTreeBranch(tx, oldRef.LogicalID, level, children[:split])
	if err != nil {
		return keyTreeRewrite{}, err
	}
	right, err := encodeKeyTreeBranch(tx, 0, level, children[split:count])
	if err != nil {
		return keyTreeRewrite{}, err
	}
	leftLower, err := transactionKeyPageLower(left, tx, bounds)
	if err != nil {
		return keyTreeRewrite{}, err
	}
	rightLower, err := transactionKeyPageLower(right, tx, bounds)
	if err != nil {
		return keyTreeRewrite{}, err
	}
	return keyTreeRewrite{
		ref: left.Ref(), lower: leftLower, rightRef: right.Ref(), rightLower: rightLower,
		found: child.found, changed: true,
	}, nil
}

func encodeKeyTreeLeaf(tx *WriteTransaction, logicalID uint64, entries []KeyDirectoryEntry, bounds KeyTreeBounds) (TransactionPage, error) {
	page, err := tx.Allocate(PageKeyDirectory, tx.options.PageSize, logicalID)
	if err != nil {
		return TransactionPage{}, err
	}
	if _, err := EncodeKeyDirectoryLeaf(page.Bytes(), keyTreeHeader(tx, page.Ref(), 0), entries, tx.NextLogicalID(), bounds.ChunkHighWater, bounds.ChunkDocuments); err != nil {
		return TransactionPage{}, err
	}
	if err := page.Stage(); err != nil {
		return TransactionPage{}, err
	}
	return page, nil
}

func encodeKeyTreeBranch(tx *WriteTransaction, logicalID uint64, level uint8, children []KeyDirectoryChild) (TransactionPage, error) {
	page, err := tx.Allocate(PageKeyDirectory, tx.options.PageSize, logicalID)
	if err != nil {
		return TransactionPage{}, err
	}
	if _, err := EncodeKeyDirectoryBranch(page.Bytes(), keyTreeHeader(tx, page.Ref(), level), children, tx.FileEnd(), tx.NextLogicalID()); err != nil {
		return TransactionPage{}, err
	}
	if err := page.Stage(); err != nil {
		return TransactionPage{}, err
	}
	return page, nil
}

func keyTreeHeader(tx *WriteTransaction, ref PageRef, level uint8) KeyDirectoryHeader {
	return KeyDirectoryHeader{
		StoreID: tx.options.StoreID, Generation: tx.options.Generation,
		LogicalID: ref.LogicalID, PageSize: ref.Length, Level: level,
	}
}

func transactionKeyPageLower(page TransactionPage, tx *WriteTransaction, bounds KeyTreeBounds) ([]byte, error) {
	view, err := OpenKeyDirectoryPage(page.Bytes(), tx.FileEnd(), tx.NextLogicalID(), bounds.ChunkHighWater, bounds.ChunkDocuments)
	if err != nil {
		return nil, err
	}
	if view.Header().Level == 0 {
		entry, ok := view.EntryAt(0)
		if !ok {
			return nil, ErrKeyDirectoryCorrupt
		}
		return entry.Key, nil
	}
	child, ok := view.ChildAt(0)
	if !ok {
		return nil, ErrKeyDirectoryCorrupt
	}
	return child.Lower, nil
}

func keyTreePageLevel(cache *PageCache, ref PageRef, tx *WriteTransaction, bounds KeyTreeBounds) (uint8, error) {
	lease, err := cache.Acquire(ref)
	if err != nil {
		return 0, err
	}
	defer lease.Release()
	view, err := OpenKeyDirectoryPage(lease.Page(), tx.FileEnd(), tx.NextLogicalID(), bounds.ChunkHighWater, bounds.ChunkDocuments)
	if err != nil {
		return 0, err
	}
	return view.Header().Level, nil
}

func keyTreeLeafFits(pageSize uint32, entries []KeyDirectoryEntry) bool {
	used := uint64(PageHeaderSize + PageTrailerSize + KeyDirectoryPayloadHeaderSize + len(entries)*KeyDirectoryLeafRecordSize)
	for _, entry := range entries {
		used += uint64(len(entry.Key))
	}
	return used <= uint64(pageSize)
}

func keyTreeLeafSplit(pageSize uint32, entries []KeyDirectoryEntry) int {
	best := 0
	bestDistance := len(entries)
	for split := 1; split < len(entries); split++ {
		if keyTreeLeafFits(pageSize, entries[:split]) && keyTreeLeafFits(pageSize, entries[split:]) {
			distance := split*2 - len(entries)
			if distance < 0 {
				distance = -distance
			}
			if distance < bestDistance {
				best = split
				bestDistance = distance
			}
		}
	}
	return best
}

func keyTreeBranchFits(pageSize uint32, children []KeyDirectoryChild) bool {
	if len(children) > 64 {
		return false
	}
	used := uint64(PageHeaderSize + PageTrailerSize + KeyDirectoryPayloadHeaderSize + len(children)*KeyDirectoryBranchRecordSize)
	for _, child := range children {
		used += uint64(len(child.Lower))
	}
	return used <= uint64(pageSize)
}

func keyTreeBranchSplit(pageSize uint32, children []KeyDirectoryChild) int {
	best := 0
	bestDistance := len(children)
	for split := 1; split < len(children); split++ {
		if keyTreeBranchFits(pageSize, children[:split]) && keyTreeBranchFits(pageSize, children[split:]) {
			distance := split*2 - len(children)
			if distance < 0 {
				distance = -distance
			}
			if distance < bestDistance {
				best = split
				bestDistance = distance
			}
		}
	}
	return best
}

func keyTreeChildRank(children []KeyDirectoryChild, key []byte) int {
	low, high := 0, len(children)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if bytes.Compare(children[middle].Lower, key) <= 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low - 1
}
