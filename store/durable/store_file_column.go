package durable

import (
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// HasFloat64Path reports whether path names a persistent typed covering
// column in this snapshot. Paths use the RFC 6901 spelling from
// [Options.Float64Columns].
func (s *Snapshot) HasFloat64Path(path string) bool {
	if s == nil || s.collection == nil || s.state == nil ||
		s.state.root.Options&storeio.StateOptionFloat64Columns == 0 {
		return false
	}
	return s.float64ColumnOrdinal(path) >= 0
}

// ReduceFloat64Path reduces one persistent typed covering column without
// reading or parsing JSON values. Missing and non-numeric cells were omitted
// when their document generation was written, so the result has the same
// acceptance semantics as RawValue.Float64. The returned boolean is false
// when path is not configured; corruption and I/O errors are never converted
// to a fallback miss.
//
// A warmed scan allocates nothing. Cold reads remain bounded by the collection
// page cache and may evict unrelated document extents.
func (s *Snapshot) ReduceFloat64Path(path string) (store.Float64Aggregate, bool, error) {
	var totals [1]store.Float64Aggregate
	covered, err := s.ReduceFloat64PathsInto(totals[:], []string{path})
	return totals[0], covered, err
}

// ReduceFloat64PathsInto fuses several persistent typed covering columns into
// one page walk. dst and paths must have equal lengths. The method preflights
// the complete path list before admitting a document page: false therefore
// means no scan occurred and callers may safely choose one coherent fallback.
// Duplicate paths are allowed, though callers should normally deduplicate
// them. A warmed call allocates nothing.
func (s *Snapshot) ReduceFloat64PathsInto(dst []store.Float64Aggregate, paths []string) (bool, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return false, ErrClosed
	}
	if len(dst) != len(paths) || len(paths) > fileStoreMaxFloat64Columns {
		return false, fmt.Errorf("vibejson: invalid float64 covering reduction buffers")
	}
	if len(paths) == 0 {
		return true, nil
	}
	if s.state.root.DocumentCount > uint64(math.MaxInt) {
		return false, store.ErrTooLarge
	}
	if s.state.root.Options&storeio.StateOptionFloat64Columns == 0 {
		return false, nil
	}
	var ordinals [fileStoreMaxFloat64Columns]uint16
	for i, path := range paths {
		column := s.float64ColumnOrdinal(path)
		if column < 0 {
			return false, nil
		}
		ordinals[i] = uint16(column)
	}
	clear(dst)
	state := s.state
	if state.root.Float64ScanHead != (storeio.PageRef{}) {
		if err := s.reduceFloat64ScanChain(dst, ordinals[:len(paths)]); err != nil {
			clear(dst)
			return true, err
		}
		return true, nil
	}
	err := storeio.WalkChunkTreeFloat64Runs(s.collection.cache, state.chunkRoot, storeio.ChunkTreeBounds{
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
	}, uint32(s.collection.options.PageSize), func(
		first, chunks uint32, ref storeio.PageRef, detached bool,
	) error {
		return s.reduceFloat64MappedRun(
			dst, ordinals[:len(paths)], first, chunks, ref, detached,
		)
	})
	if err != nil {
		clear(dst)
		return true, err
	}
	return true, nil
}

// reduceFloat64ScanChain is the compact dense-projection fast path. The
// state-root ordered directory and value-only stripes omit the stable-slot
// tree walk. Incremental copy-on-write stripe replacement keeps this path
// active through ordinary updates/deletes; documented fallback cases clear
// the root and make the general overlay-aware path above authoritative.
func (s *Snapshot) reduceFloat64ScanChain(dst []store.Float64Aggregate, ordinals []uint16) error {
	state := s.state
	nextChunk := uint32(0)
	err := storeio.WalkFloat64DirectoryLeaves(
		s.collection.cache, state.root.Float64ScanHead,
		storeio.Float64DirectoryBounds{
			FileEnd:       state.super.FileEnd,
			NextLogicalID: state.root.NextLogicalID,
		},
		uint32(s.collection.options.PageSize),
		func(leaf storeio.Float64DirectoryView) error {
			var refs [storeio.Float64DirectoryFanout]storeio.PageRef
			for i := 0; i < leaf.Len(); i++ {
				entry, _ := leaf.EntryAt(i)
				refs[i] = entry.Ref
			}
			prefetch := refs[:leaf.Len()]
			slices.SortFunc(
				prefetch,
				func(a, b storeio.PageRef) int {
					switch {
					case a.Offset < b.Offset:
						return -1
					case a.Offset > b.Offset:
						return 1
					default:
						return 0
					}
				},
			)
			if _, err := s.collection.cache.Prefetch(
				prefetch,
			); err != nil {
				return err
			}
			for i := 0; i < leaf.Len(); i++ {
				entry, _ := leaf.EntryAt(i)
				groupLease, acquireErr := s.collection.cache.Acquire(
					entry.Ref,
				)
				if acquireErr != nil {
					return acquireErr
				}
				stripe := storeio.AdmittedFloat64Stripe(
					groupLease.Page(),
				)
				stripeHeader := stripe.Header()
				if entry.FirstChunk != nextChunk ||
					stripeHeader.FirstChunk != entry.FirstChunk ||
					stripeHeader.ColumnCount !=
						uint16(len(s.collection.options.float64Columns)) {
					groupLease.Release()
					return fmt.Errorf(
						"%w: float64 stripe coverage",
						storeio.ErrFloat64StripeCorrupt,
					)
				}
				for column, ordinal := range ordinals {
					values, encoding, found := stripe.ColumnValues(
						int(ordinal),
					)
					if !found ||
						!dst[column].AddPackedFloat64Width(
							values, encoding.ByteWidth(),
						) {
						groupLease.Release()
						return fmt.Errorf(
							"%w: float64 stripe column",
							storeio.ErrFloat64StripeCorrupt,
						)
					}
				}
				coveredChunks := stripeHeader.ChunkCount
				if uint64(nextChunk)+uint64(coveredChunks) >
					uint64(^uint32(0)) {
					groupLease.Release()
					return fmt.Errorf(
						"%w: float64 scan chunk overflow",
						storeio.ErrFloat64GroupCorrupt,
					)
				}
				nextChunk += coveredChunks
				groupLease.Release()
			}
			return nil
		},
	)
	if err != nil {
		return err
	}
	if nextChunk != state.root.ChunkHighWater {
		return fmt.Errorf("%w: incomplete float64 stripe coverage", storeio.ErrFloat64StripeCorrupt)
	}
	return nil
}

func (s *Snapshot) reduceFloat64MappedRun(
	dst []store.Float64Aggregate,
	ordinals []uint16,
	first, chunks uint32,
	ref storeio.PageRef,
	detached bool,
) error {
	if chunks == 0 || ref.Kind == storeio.PageDocument && chunks != 1 {
		return storeio.ErrChunkDirectoryCorrupt
	}
	lease, err := s.collection.cache.Acquire(ref)
	if err != nil {
		return err
	}
	if detached {
		group := storeio.AdmittedFloat64Group(lease.Page())
		header := group.Header()
		runEnd := uint64(first) + uint64(chunks)
		groupEnd := uint64(header.FirstChunk) + uint64(header.ChunkCount)
		if header.ColumnCount != uint16(len(s.collection.options.float64Columns)) ||
			first < header.FirstChunk || runEnd > groupEnd {
			lease.Release()
			return fmt.Errorf("%w: detached float64 covering catalog", storeio.ErrFloat64GroupCorrupt)
		}
		for i, ordinal := range ordinals {
			values, encoding, found := group.Float64ColumnRangeValues(int(ordinal), first, chunks)
			if !found || !dst[i].AddPackedFloat64Width(values, encoding.ByteWidth()) {
				lease.Release()
				return fmt.Errorf("%w: detached float64 packed column", storeio.ErrFloat64GroupCorrupt)
			}
		}
		lease.Release()
		return nil
	}
	for chunkOrdinal := range chunks {
		view, viewErr := admittedFileDocumentChunk(lease.Page(), ref, first+chunkOrdinal)
		if viewErr != nil {
			lease.Release()
			return viewErr
		}
		if view.float64ColumnCount() != len(s.collection.options.float64Columns) {
			lease.Release()
			return fmt.Errorf("%w: float64 covering catalog", storeio.ErrDocumentPageCorrupt)
		}
		for i, ordinal := range ordinals {
			covered, ok := view.float64Column(int(ordinal))
			if !ok {
				lease.Release()
				return fmt.Errorf("%w: float64 covering ordinal", storeio.ErrDocumentPageCorrupt)
			}
			iterator := covered.Values()
			for {
				value, present := iterator.Next()
				if !present {
					break
				}
				dst[i].Add(value)
			}
		}
	}
	lease.Release()
	return nil
}

func (s *Snapshot) float64ColumnOrdinal(path string) int {
	for i, column := range s.collection.options.float64Columns {
		if column.spec == path {
			return i
		}
	}
	return -1
}
