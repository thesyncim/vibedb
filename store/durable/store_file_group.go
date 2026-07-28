package durable

import (
	"fmt"
	"math"
	"math/bits"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

// IndexScalarGroup is one collision-certified scalar group read directly
// from a frozen single-column exact index. Value borrows the supplied
// IndexWorkspace until its next use or Release. Count includes only rows
// certified by the posting representative. First is an opaque token ordered
// like the snapshot's stable chunk/slot traversal.
type IndexScalarGroup struct {
	Value vibejson.RawValue
	Count uint64
	First uint64
}

type fileIndexScalarGroupState struct {
	hash          uint64
	certificateAt uint32
	certificateN  uint32
	count         uint64
	first         uint64
}

// AppendIndexScalarGroupsInto appends exact grouped counts from one frozen
// single-column exact index. A compact generation may answer from bounded
// aggregate pages containing O(groups) representatives, counts, and first-row
// tokens. Otherwise the method streams certified postings and residual
// receives ordered stable-slot candidates for missing and container values,
// legacy postings without representatives, and hash collisions.
// Feeding residual to RangeMasksRawRowsBuffer and grouping the selected path
// completes the exact result without reading certified JSON.
//
// The posting lane retains only two pointer-free words per stable chunk plus
// one compact representative per distinct certified value. Neither lane
// duplicates a per-row index in memory. Reusing workspace and caller
// destinations makes a warmed call allocation-free once their observed
// high-water marks fit. Returned Value slices borrow workspace.
func (s *Snapshot) AppendIndexScalarGroupsInto(
	dst []IndexScalarGroup,
	residual []store.Mask,
	workspace *IndexWorkspace,
	name string,
) ([]IndexScalarGroup, []store.Mask, bool, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return dst, residual, false, ErrClosed
	}
	if workspace == nil {
		workspace = &IndexWorkspace{}
	}
	indexID, ok := s.collection.options.indexNameIDs[name]
	if !ok {
		return dst, residual, false, store.ErrIndexNotFound
	}
	exact := s.collection.options.indexes[indexID]
	if exact == nil || exact.N != 1 {
		return dst, residual, false, store.ErrIndexArity
	}
	state := s.state
	workspace.groupArena = workspace.groupArena[:0]
	workspace.groupState = workspace.groupState[:0]
	workspace.lastProbe = IndexProbeStats{}
	if catalogGroups, covered, err := s.appendIndexCatalogScalarGroups(
		dst, workspace, indexID,
	); err != nil || covered {
		return catalogGroups, residual, covered, err
	}
	if uint64(state.root.ChunkHighWater) > uint64(math.MaxInt) {
		return dst, residual, false, store.ErrTooLarge
	}
	chunks := int(state.root.ChunkHighWater)
	workspace.indexCoverage = resizeFileIndexWords(workspace.indexCoverage, chunks)
	workspace.certifiedCoverage = resizeFileIndexWords(workspace.certifiedCoverage, chunks)
	clear(workspace.indexCoverage)
	clear(workspace.certifiedCoverage)
	var (
		haveHash    bool
		currentHash uint64
		hashStart   int
	)
	bounds := storeio.IndexTreeBounds{
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		IndexHighWater: state.root.IndexCount,
	}
	err := storeio.WalkIndexTreeIndex(
		s.collection.cache, state.indexRoot, indexID, bounds,
		func(directory storeio.IndexDirectoryView) error {
			workspace.lastProbe.PostingPages++
			for rank := 0; rank < directory.Len(); rank++ {
				entry, ok := directory.EntryAt(rank)
				if !ok {
					return storeio.ErrIndexDirectoryCorrupt
				}
				if entry.Key.IndexID < indexID {
					continue
				}
				if entry.Key.IndexID > indexID {
					break
				}
				if entry.Key.Chunk >= state.root.ChunkHighWater ||
					entry.Bits&^fileStoreLiveMask(state.root.ChunkDocuments) != 0 {
					return storeio.ErrIndexDirectoryCorrupt
				}
				chunk := int(entry.Key.Chunk)
				if workspace.indexCoverage[chunk]&entry.Bits != 0 {
					return fmt.Errorf(
						"%w: overlapping scalar index streams",
						storeio.ErrIndexDirectoryCorrupt,
					)
				}
				workspace.indexCoverage[chunk] |= entry.Bits
				rows := uint64(bits.OnesCount64(entry.Bits))
				workspace.lastProbe.CandidateRows += rows
				workspace.lastProbe.CandidateChunks++

				certificate := directory.Certificate(entry.Cert)
				if entry.Flags&storeio.IndexEntryCollision != 0 ||
					len(certificate) == 0 {
					continue
				}
				if !fileIndexCertificateValid(certificate, 1) {
					return storeio.ErrIndexDirectoryCorrupt
				}
				if !haveHash || currentHash != entry.Key.TupleHash {
					haveHash = true
					currentHash = entry.Key.TupleHash
					hashStart = len(workspace.groupState)
				}
				group := -1
				for candidate := hashStart; candidate < len(workspace.groupState); candidate++ {
					existing := workspace.groupState[candidate]
					if existing.hash != currentHash {
						break
					}
					start := int(existing.certificateAt)
					end := start + int(existing.certificateN)
					if fileIndexCertificatesEqual(
						workspace.groupArena[start:end:end], certificate, 1,
					) {
						group = candidate
						break
					}
				}
				first := uint64(entry.Key.Chunk)<<6 |
					uint64(bits.TrailingZeros64(entry.Bits))
				if group < 0 {
					if uint64(len(workspace.groupArena))+uint64(len(certificate)) >
						uint64(^uint32(0)) {
						return store.ErrTooLarge
					}
					start := len(workspace.groupArena)
					workspace.groupArena = append(workspace.groupArena, certificate...)
					workspace.groupState = append(
						workspace.groupState,
						fileIndexScalarGroupState{
							hash: currentHash, certificateAt: uint32(start),
							certificateN: uint32(len(certificate)), first: first,
						},
					)
					group = len(workspace.groupState) - 1
				}
				groupState := &workspace.groupState[group]
				if groupState.count > ^uint64(0)-rows {
					return store.ErrTooLarge
				}
				groupState.count += rows
				if first < groupState.first {
					groupState.first = first
				}
				workspace.certifiedCoverage[chunk] |= entry.Bits
				workspace.lastProbe.CertificateRows += rows
				workspace.lastProbe.MatchedRows += rows
			}
			return nil
		},
	)
	if err != nil {
		return dst, residual, true, err
	}

	limit := fileStoreLiveMask(state.root.ChunkDocuments)
	err = storeio.WalkChunkTree(
		s.collection.cache, state.chunkRoot,
		storeio.ChunkTreeBounds{
			FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		},
		func(chunk uint32, _ storeio.PageRef) error {
			candidates := limit &^ workspace.certifiedCoverage[chunk]
			if candidates != 0 {
				residual = append(residual, store.Mask{Chunk: chunk, Bits: candidates})
			}
			return nil
		},
	)
	if err != nil {
		return dst, residual, true, err
	}
	for _, group := range workspace.groupState {
		start := int(group.certificateAt)
		end := start + int(group.certificateN)
		dst = append(dst, IndexScalarGroup{
			Value: vibejson.RawValue{Src: workspace.groupArena[start:end:end]},
			Count: group.count, First: group.first,
		})
	}
	return dst, residual, true, nil
}

// appendIndexCatalogScalarGroups serves the compact-generation fast path.
// Representatives stream from bounded linked pages into workspace, so
// cardinality changes page count rather than one giant allocation. Results
// never borrow evictable page-cache storage.
func (s *Snapshot) appendIndexCatalogScalarGroups(
	dst []IndexScalarGroup,
	workspace *IndexWorkspace,
	indexID uint32,
) ([]IndexScalarGroup, bool, error) {
	state := s.state
	catalogRef := state.root.IndexGroupHead
	if catalogRef == (storeio.PageRef{}) {
		return dst, false, nil
	}
	total := uint64(0)
	var (
		coveredIndexes uint64
		previousIndex  uint32
		havePrevious   bool
		previousRef    storeio.PageRef
	)
	for catalogRef != (storeio.PageRef{}) {
		lease, err := s.collection.cache.Acquire(catalogRef)
		if err != nil {
			return dst, true, err
		}
		catalog := storeio.AdmittedIndexGroupCatalog(lease.Page())
		header := catalog.Header()
		if previousRef == (storeio.PageRef{}) {
			coveredIndexes = header.CoveredIndexes
			if header.DocumentCount != state.root.DocumentCount {
				lease.Release()
				return dst, true, storeio.ErrIndexGroupCatalogCorrupt
			}
			if !catalog.Covered(indexID) {
				lease.Release()
				return dst, false, nil
			}
		} else if !catalog.Segmented() ||
			header.CoveredIndexes != coveredIndexes ||
			header.DocumentCount != state.root.DocumentCount ||
			header.Generation != previousRef.Generation {
			lease.Release()
			return dst, true, storeio.ErrIndexGroupCatalogCorrupt
		}
		iterator := catalog.Iterator()
		for {
			entry, ok := iterator.Next()
			if !ok {
				break
			}
			if havePrevious && entry.IndexID < previousIndex {
				lease.Release()
				return dst, true, storeio.ErrIndexGroupCatalogCorrupt
			}
			havePrevious = true
			previousIndex = entry.IndexID
			if entry.IndexID != indexID {
				continue
			}
			if !fileIndexCertificateValid(entry.Value, 1) ||
				uint64(len(workspace.groupArena))+uint64(len(entry.Value)) >
					uint64(^uint32(0)) ||
				total > ^uint64(0)-entry.Count {
				lease.Release()
				return dst, true, storeio.ErrIndexGroupCatalogCorrupt
			}
			start := len(workspace.groupArena)
			workspace.groupArena = append(
				workspace.groupArena, entry.Value...,
			)
			workspace.groupState = append(
				workspace.groupState,
				fileIndexScalarGroupState{
					certificateAt: uint32(start),
					certificateN:  uint32(len(entry.Value)),
					count:         entry.Count,
					first:         entry.First,
				},
			)
			total += entry.Count
		}
		next := header.Next
		if next != (storeio.PageRef{}) &&
			(!catalog.Segmented() ||
				next.LogicalID <= catalogRef.LogicalID ||
				next.Offset <= catalogRef.Offset) {
			lease.Release()
			return dst, true, storeio.ErrIndexGroupCatalogCorrupt
		}
		lease.Release()
		previousRef = catalogRef
		catalogRef = next
	}
	if len(workspace.groupState) == 0 || total != state.root.DocumentCount {
		return dst, true, storeio.ErrIndexGroupCatalogCorrupt
	}
	for _, group := range workspace.groupState {
		start := int(group.certificateAt)
		end := start + int(group.certificateN)
		dst = append(dst, IndexScalarGroup{
			Value: vibejson.RawValue{Src: workspace.groupArena[start:end:end]},
			Count: group.count, First: group.first,
		})
	}
	workspace.lastProbe.CertificateRows = total
	workspace.lastProbe.MatchedRows = total
	return dst, true, nil
}

func resizeFileIndexWords(words []uint64, length int) []uint64 {
	if cap(words) < length {
		return make([]uint64, length)
	}
	return words[:length]
}
