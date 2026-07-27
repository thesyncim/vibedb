package storeio

import (
	"bytes"
	"fmt"
)

// KeyTreeLookup is one key and its resolved result inside a sorted batch.
// Key is borrowed for the call; Location and Found are overwritten.
type KeyTreeLookup struct {
	Key      []byte
	Location KeyLocation
	Found    bool
}

// LookupKeyTreeBatch resolves lookups, which must be sorted strictly ascending
// by raw key bytes, in one read-only descent. Each directory page covered by
// the batch is acquired once no matter how many requested keys route through
// it.
//
// This is deliberately a writer-side companion to LookupKeyTree rather than a
// replacement for it. Point reads keep their single-key binary-search path;
// mutation planning can amortize shared branch and leaf visits before it
// stages any copy-on-write pages.
func LookupKeyTreeBatch(
	cache *PageCache, root PageRef, lookups []KeyTreeLookup, bounds KeyTreeBounds,
) error {
	for i := range lookups {
		if i != 0 && bytes.Compare(lookups[i-1].Key, lookups[i].Key) >= 0 {
			return fmt.Errorf("%w: unsorted key-tree lookup batch", ErrInvalidWrite)
		}
		lookups[i].Location = KeyLocation{}
		lookups[i].Found = false
	}
	if len(lookups) == 0 || root == (PageRef{}) {
		return nil
	}
	if cache == nil {
		return fmt.Errorf("%w: nil key-tree cache", ErrInvalidWrite)
	}
	batch := keyTreeBatchLookup{
		cache: cache, bounds: bounds, admitted: cache.ValidatesOnAdmission(),
	}
	return batch.lookupPage(root, lookups, 0)
}

type keyTreeBatchLookup struct {
	cache    *PageCache
	bounds   KeyTreeBounds
	admitted bool
}

func (b *keyTreeBatchLookup) lookupPage(
	ref PageRef, lookups []KeyTreeLookup, depth uint8,
) error {
	if depth > keyDirectoryMaxLevel {
		return ErrKeyTreeDepth
	}
	if ref.Kind != PageKeyDirectory {
		return fmt.Errorf("%w: key-tree reference kind", ErrKeyDirectoryCorrupt)
	}
	lease, err := b.cache.Acquire(ref)
	if err != nil {
		return err
	}
	var view KeyDirectoryView
	if b.admitted {
		view = AdmittedKeyDirectoryPage(lease.Page())
	} else if view, err = OpenKeyDirectoryPage(
		lease.Page(), b.bounds.FileEnd, b.bounds.NextLogicalID,
		b.bounds.ChunkHighWater, b.bounds.ChunkDocuments,
	); err != nil {
		lease.Release()
		return err
	}
	if view.Header().Level == 0 {
		lookupKeyTreeLeafBatch(view, lookups)
		lease.Release()
		return nil
	}

	// Copy only value-sized routing information before releasing the parent.
	// Keeping every ancestor pinned during recursive reads would make the batch
	// require one extra resident frame per tree level, unlike point lookup.
	var refs [64]PageRef
	var starts [64]int
	var ends [64]int
	tasks := 0
	at := 0
	first, _ := view.ChildAt(0)
	for at < len(lookups) && bytes.Compare(lookups[at].Key, first.Lower) < 0 {
		// A point before the first inclusive subtree bound is absent. Mutation
		// descent routes a new minimum into that child, but lookup must not.
		at++
	}
	for rank := 0; rank < view.Len() && at < len(lookups); rank++ {
		child, _ := view.ChildAt(rank)
		end := len(lookups)
		if rank+1 < view.Len() {
			next, _ := view.ChildAt(rank + 1)
			end = at
			for end < len(lookups) && bytes.Compare(lookups[end].Key, next.Lower) < 0 {
				end++
			}
		}
		if end != at {
			refs[tasks] = child.Ref
			starts[tasks] = at
			ends[tasks] = end
			tasks++
		}
		at = end
	}
	lease.Release()

	for task := 0; task < tasks; task++ {
		if err := b.lookupPage(
			refs[task], lookups[starts[task]:ends[task]], depth+1,
		); err != nil {
			return err
		}
	}
	return nil
}

func lookupKeyTreeLeafBatch(view KeyDirectoryView, lookups []KeyTreeLookup) {
	at := 0
	for rank := 0; rank < view.Len() && at < len(lookups); rank++ {
		entry, _ := view.EntryAt(rank)
		for at < len(lookups) && bytes.Compare(lookups[at].Key, entry.Key) < 0 {
			at++
		}
		if at < len(lookups) && bytes.Equal(lookups[at].Key, entry.Key) {
			lookups[at].Location = entry.Location
			lookups[at].Found = true
			at++
		}
	}
}
