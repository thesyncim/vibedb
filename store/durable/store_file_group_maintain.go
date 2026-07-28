package durable

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

var fileIndexGroupNull = [...]byte{'n', 'u', 'l', 'l'}

// maintainFileIndexGroups applies one document mutation to the bounded
// aggregate catalog. The catalog is O(groups), never O(rows): ordinary churn
// therefore keeps the clean grouping lane without adding a per-key object or
// scanning the corpus.
//
// An index declines independently when the new value is a container, its
// summary no longer fits one configured extent, or a delete removes the
// recorded first row from a group that still has members. The last case is
// deliberately conservative. Exact postings plus residual JSON remain
// authoritative, while the other covered indexes retain their fast path.
//
// The returned changed bit means the old catalog extent became unreachable
// and must be retired with the transaction. A false bit permits immutable
// reuse when the mutation does not alter any covered value.
func (c *Collection) maintainFileIndexGroups(
	tx *storeio.WriteTransaction,
	state *fileStoreState,
	location storeio.KeyLocation,
	oldIndex, newIndex *vibejson.Index,
	documentCount uint64,
	chunkHighWater uint32,
) (head storeio.PageRef, changed bool, err error) {
	oldHead := state.root.IndexGroupHead
	if oldHead == (storeio.PageRef{}) {
		return storeio.PageRef{}, false, nil
	}
	if documentCount == 0 {
		return storeio.PageRef{}, true, nil
	}

	lease, err := c.cache.Acquire(oldHead)
	if err != nil {
		return storeio.PageRef{}, false, err
	}
	defer lease.Release()
	catalog := storeio.AdmittedIndexGroupCatalog(lease.Page())
	header := catalog.Header()
	if header.DocumentCount != state.root.DocumentCount {
		return storeio.PageRef{}, false, storeio.ErrIndexGroupCatalogCorrupt
	}
	if catalog.Segmented() {
		// Segmented high-cardinality covers are immutable in this phase.
		// Ordinary one-page catalogs retain the transactional O(groups)
		// maintenance below; a mutation over a segmented cover falls back to
		// exact postings and retires the compact chain safely.
		return storeio.PageRef{}, true, nil
	}

	c.indexGroupSource = c.indexGroupSource[:0]
	iterator := catalog.Iterator()
	for {
		entry, ok := iterator.Next()
		if !ok {
			break
		}
		c.indexGroupSource = append(c.indexGroupSource, entry)
	}
	if len(c.indexGroupSource) == 0 {
		return storeio.PageRef{}, false, storeio.ErrIndexGroupCatalogCorrupt
	}

	covered := header.CoveredIndexes
	c.indexGroupEntries = c.indexGroupEntries[:0]
	required := storeio.PageHeaderSize + storeio.PageTrailerSize +
		storeio.IndexGroupCatalogPayloadHeaderSize
	token := uint64(location.Chunk)<<6 | uint64(location.Slot)
	sourceAt := 0
	for sourceAt < len(c.indexGroupSource) {
		indexID := c.indexGroupSource[sourceAt].IndexID
		sourceEnd := sourceAt + 1
		for sourceEnd < len(c.indexGroupSource) &&
			c.indexGroupSource[sourceEnd].IndexID == indexID {
			sourceEnd++
		}
		if int(indexID) >= len(c.options.indexes) ||
			covered&(uint64(1)<<indexID) == 0 {
			return storeio.PageRef{}, false, storeio.ErrIndexGroupCatalogCorrupt
		}
		exact := c.options.indexes[indexID]
		if exact == nil || exact.N != 1 {
			return storeio.PageRef{}, false, storeio.ErrIndexGroupCatalogCorrupt
		}

		outputStart := len(c.indexGroupEntries)
		c.indexGroupEntries = append(
			c.indexGroupEntries,
			c.indexGroupSource[sourceAt:sourceEnd]...,
		)
		oldValue, oldPresent, oldEligible, valueErr :=
			fileIndexGroupMutationValue(oldIndex, exact)
		if valueErr != nil {
			return storeio.PageRef{}, false, valueErr
		}
		if oldPresent && !oldEligible {
			return storeio.PageRef{}, false, storeio.ErrIndexGroupCatalogCorrupt
		}
		newValue, newPresent, newEligible, valueErr :=
			fileIndexGroupMutationValue(newIndex, exact)
		if valueErr != nil {
			return storeio.PageRef{}, false, valueErr
		}

		keep := newEligible
		same := oldPresent && newPresent && oldEligible && newEligible &&
			fileIndexRawValuesEqual(oldValue, newValue)
		if keep && !same {
			if oldPresent {
				position := fileIndexGroupEntry(
					c.indexGroupEntries, outputStart, oldValue,
				)
				if position < 0 {
					return storeio.PageRef{}, false, storeio.ErrIndexGroupCatalogCorrupt
				}
				entry := &c.indexGroupEntries[position]
				switch {
				case entry.Count == 0:
					return storeio.PageRef{}, false, storeio.ErrIndexGroupCatalogCorrupt
				case entry.Count == 1:
					copy(
						c.indexGroupEntries[position:],
						c.indexGroupEntries[position+1:],
					)
					last := len(c.indexGroupEntries) - 1
					c.indexGroupEntries[last] = storeio.IndexGroupCatalogEntry{}
					c.indexGroupEntries = c.indexGroupEntries[:last]
				case entry.First == token:
					// Count remains exact, but stable first-row ordering cannot
					// be reconstructed from an aggregate-only page.
					keep = false
				default:
					entry.Count--
				}
			}
			if keep && newPresent {
				position := fileIndexGroupEntry(
					c.indexGroupEntries, outputStart, newValue,
				)
				if position < 0 {
					c.indexGroupEntries = append(
						c.indexGroupEntries,
						storeio.IndexGroupCatalogEntry{
							IndexID: indexID,
							Value:   newValue.Bytes(),
							Count:   1,
							First:   token,
						},
					)
				} else {
					entry := &c.indexGroupEntries[position]
					if entry.Count == ^uint64(0) {
						return storeio.PageRef{}, false, store.ErrTooLarge
					}
					entry.Count++
					entry.First = min(entry.First, token)
				}
			}
		}

		candidateBytes := 0
		if keep {
			for _, entry := range c.indexGroupEntries[outputStart:] {
				size, sizeErr := storeio.IndexGroupCatalogEntryEncodedSize(entry)
				if sizeErr != nil ||
					candidateBytes > c.options.MaxPageSize-size {
					keep = false
					break
				}
				candidateBytes += size
			}
			if keep && required > c.options.MaxPageSize-candidateBytes {
				keep = false
			}
		}
		if !keep {
			clear(c.indexGroupEntries[outputStart:])
			c.indexGroupEntries = c.indexGroupEntries[:outputStart]
			covered &^= uint64(1) << indexID
		} else {
			required += candidateBytes
		}
		sourceAt = sourceEnd
	}

	if covered == 0 {
		return storeio.PageRef{}, true, nil
	}
	if covered == header.CoveredIndexes &&
		documentCount == state.root.DocumentCount &&
		fileIndexGroupEntriesUnchanged(c.indexGroupSource, c.indexGroupEntries) {
		return oldHead, false, nil
	}
	pageSize, ok := fileStoreBulkExtent(
		required, c.options.PageSize, c.options.MaxPageSize,
	)
	if !ok {
		return storeio.PageRef{}, false, fmt.Errorf(
			"%w: incremental index group catalog",
			storeio.ErrInvalidWrite,
		)
	}
	page, err := tx.Allocate(
		storeio.PageIndexGroupCatalog, pageSize, oldHead.LogicalID,
	)
	if err != nil {
		return storeio.PageRef{}, false, err
	}
	if _, err := storeio.EncodeIndexGroupCatalogPage(
		page.Bytes(),
		storeio.IndexGroupCatalogHeader{
			StoreID: c.storeID, Generation: tx.Generation(),
			LogicalID: page.Ref().LogicalID, PageSize: page.Ref().Length,
			CoveredIndexes: covered, DocumentCount: documentCount,
		},
		c.indexGroupEntries, uint32(len(c.options.indexes)),
		chunkHighWater, uint32(c.options.Collection.ChunkDocuments),
	); err != nil {
		return storeio.PageRef{}, false, err
	}
	if err := page.Stage(); err != nil {
		return storeio.PageRef{}, false, err
	}
	return page.Ref(), true, nil
}

func fileIndexGroupMutationValue(
	index *vibejson.Index,
	exact *store.ExactIndex,
) (value vibejson.RawValue, present, eligible bool, err error) {
	if index == nil {
		return vibejson.RawValue{}, false, true, nil
	}
	node, found, err := index.PointerCompiled(exact.Paths[0])
	if err != nil {
		return vibejson.RawValue{}, false, false, err
	}
	if !found || len(node.Raw().Bytes()) == 0 {
		return vibejson.RawValue{Src: fileIndexGroupNull[:]}, true, true, nil
	}
	value = node.Raw()
	return value, true, fileIndexCertificateScalar(value), nil
}

func fileIndexGroupEntry(
	entries []storeio.IndexGroupCatalogEntry,
	first int,
	value vibejson.RawValue,
) int {
	for position := first; position < len(entries); position++ {
		if fileIndexRawValuesEqual(
			vibejson.RawValue{Src: entries[position].Value}, value,
		) {
			return position
		}
	}
	return -1
}

func fileIndexGroupEntriesUnchanged(
	left, right []storeio.IndexGroupCatalogEntry,
) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].IndexID != right[i].IndexID ||
			left[i].Count != right[i].Count ||
			left[i].First != right[i].First ||
			len(left[i].Value) != len(right[i].Value) {
			return false
		}
		for j := range left[i].Value {
			if left[i].Value[j] != right[i].Value[j] {
				return false
			}
		}
	}
	return true
}
