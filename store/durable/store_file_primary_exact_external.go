package durable

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/x/byteview"
)

// stagePrimaryExactRunWindow lowers one borrowed primary scan window directly
// into the bounded external sorter. The builder copies canonical binary keys
// before return; documents and vibejson views remain borrowed and no string
// identity is created.
func stagePrimaryExactRunWindow(
	builder *storeio.GenerationMigrationExactRunBuilder,
	records []storeio.PrimaryGraphRecord,
	placements []storeio.PrimaryGraphPlacement,
	indexes []*store.ExactIndex,
) error {
	if builder == nil || len(records) != len(placements) || len(indexes) == 0 || len(indexes) > 64 {
		return storeio.ErrInvalidWrite
	}
	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	var canonical [storeio.IndexTermMaxKeyBytes]byte
	for row := range records {
		placement := placements[row]
		tileID := uint32(placement.Bucket)<<2 | uint32(placement.Slot>>6)
		mask := uint64(1) << uint(placement.Slot&63)
		for indexID, exact := range indexes {
			key, present, err := appendPrimaryExactDocumentTerm(canonical[:0], components[:], exact, byteview.Bytes(records[row].Value))
			if err != nil {
				return err
			}
			if present {
				if err := builder.Add(uint32(indexID), key, tileID, mask); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// buildPrimaryExactIndexesFromMergedRun stages a complete exact-index graph
// directly from one externally merged run. Its retained state is one output
// leaf, one level-0 catalog page, and the bounded level-1 child vector.
func buildPrimaryExactIndexesFromMergedRun(
	sink storeio.PrimaryGraphBuildSink,
	read storeio.GenerationMigrationExactRunReader,
	region storeio.GenerationMigrationExactRunRegion,
	indexCount uint32,
	pageSize, maxPageSize uint32,
	live storeio.IndexTermLeafLiveLookup,
) (storeio.PageRef, error) {
	if sink == nil || indexCount == 0 || indexCount > 64 {
		return storeio.PageRef{}, storeio.ErrInvalidWrite
	}
	rootEntries := make([]storeio.PrimaryExactRootEntry, indexCount)
	encodedScratch := make([]byte, 0, storeio.IndexTermLeafCutBudget(maxPageSize))
	var catalog *primaryExactCatalogStream
	var currentIndex uint32
	haveIndex := false
	finishIndex := func() error {
		if !haveIndex {
			return nil
		}
		ref, leaves, err := catalog.Finish()
		if err != nil {
			return err
		}
		rootEntries[currentIndex] = storeio.PrimaryExactRootEntry{Catalog: ref, LeafCount: leaves}
		return nil
	}
	err := storeio.StreamGenerationMigrationExactLeaves(
		read, region, sink.StoreIdentity(), sink.BuildGeneration(),
		storeio.IndexTermLeafCutBudget(maxPageSize), live,
		func(indexID uint32, leaf []storeio.IndexTermLeafTerm, piece bool) error {
			if indexID >= indexCount || haveIndex && indexID < currentIndex {
				return storeio.ErrPrimaryExactIndexCorrupt
			}
			if !haveIndex || indexID != currentIndex {
				if err := finishIndex(); err != nil {
					return err
				}
				var err error
				catalog, err = newPrimaryExactCatalogStream(sink, pageSize, maxPageSize)
				if err != nil {
					return err
				}
				currentIndex, haveIndex = indexID, true
			}
			encodedScratch = encodedScratch[:0]
			encoded, err := storeio.AppendIndexTermLeaf(encodedScratch, sink.StoreIdentity(), leaf)
			if err != nil {
				return err
			}
			ref, err := stagePrimaryExactLeafPage(sink, encoded, pageSize, maxPageSize)
			if err != nil {
				return err
			}
			first := leaf[0]
			firstTile := first.Postings[0].Posting.TileID
			return catalog.Add(ref, first.Key.Canonical, firstTile, piece, storeio.IndexTermLeafRunCut(first.Key.RouteHash))
		},
	)
	if err != nil {
		return storeio.PageRef{}, err
	}
	if err := finishIndex(); err != nil {
		return storeio.PageRef{}, err
	}
	rootPage, err := sink.AllocatePage(storeio.PagePrimaryExactRoot, pageSize, 0)
	if err != nil {
		return storeio.PageRef{}, err
	}
	if _, err := storeio.EncodePrimaryExactRootPage(rootPage.Bytes(), sink.StoreIdentity(), sink.BuildGeneration(), rootPage.Ref().LogicalID, rootEntries); err != nil {
		return storeio.PageRef{}, err
	}
	if err := rootPage.Stage(); err != nil {
		return storeio.PageRef{}, err
	}
	return rootPage.Ref(), nil
}

type primaryExactCatalogStream struct {
	sink                  storeio.PrimaryGraphBuildSink
	pageSize, maxPageSize uint32
	entries               []storeio.PrimaryExactCatalogEntry
	prefixArena           []byte
	entryBytes            int
	children              []storeio.PageRef
	leaves                uint32
	finished              bool
}

func newPrimaryExactCatalogStream(sink storeio.PrimaryGraphBuildSink, pageSize, maxPageSize uint32) (*primaryExactCatalogStream, error) {
	entryCapacity := (int(maxPageSize) - storeio.PageHeaderSize - storeio.PageTrailerSize - primaryExactCatalogHeaderReserve) / storeio.PrimaryExactCatalogEntryBytes(0)
	childCapacity := (int(maxPageSize) - storeio.PageHeaderSize - storeio.PageTrailerSize - primaryExactCatalogHeaderReserve) / storeio.PageRefSize
	if sink == nil || entryCapacity < 1 || childCapacity < 2 {
		return nil, storeio.ErrInvalidWrite
	}
	return &primaryExactCatalogStream{sink: sink, pageSize: pageSize, maxPageSize: maxPageSize, entries: make([]storeio.PrimaryExactCatalogEntry, 0, entryCapacity), prefixArena: make([]byte, 0, entryCapacity*storeio.PrimaryExactCatalogPrefixBytes), children: make([]storeio.PageRef, 0, childCapacity)}, nil
}

func (s *primaryExactCatalogStream) Add(ref storeio.PageRef, firstKey []byte, firstTile uint32, piece, runCut bool) error {
	if s == nil || s.finished || ref.Kind != storeio.PagePrimaryExactLeaf || len(firstKey) == 0 || s.leaves == ^uint32(0) {
		return storeio.ErrInvalidWrite
	}
	prefix := firstKey
	if len(prefix) > storeio.PrimaryExactCatalogPrefixBytes {
		prefix = prefix[:storeio.PrimaryExactCatalogPrefixBytes]
	}
	entryBytes := storeio.PrimaryExactCatalogEntryBytes(len(prefix))
	budget := int(s.maxPageSize) - storeio.PageHeaderSize - storeio.PageTrailerSize
	if len(s.entries) != 0 && primaryExactCatalogHeaderReserve+s.entryBytes+entryBytes > budget {
		if err := s.flushLeaf(); err != nil {
			return err
		}
	}
	if len(s.entries) == cap(s.entries) || len(prefix) > cap(s.prefixArena)-len(s.prefixArena) {
		return storeio.ErrPrimaryExactIndexCorrupt
	}
	at := len(s.prefixArena)
	s.prefixArena = append(s.prefixArena, prefix...)
	flags := uint8(0)
	if piece {
		flags |= storeio.PrimaryExactCatalogPiece
	}
	if runCut {
		flags |= storeio.PrimaryExactCatalogRunCut
	}
	s.entries = append(s.entries, storeio.PrimaryExactCatalogEntry{Leaf: ref, FirstTile: firstTile, Flags: flags, Prefix: s.prefixArena[at:len(s.prefixArena):len(s.prefixArena)]})
	s.entryBytes += entryBytes
	s.leaves++
	return nil
}

func (s *primaryExactCatalogStream) flushLeaf() error {
	if len(s.entries) == 0 {
		return nil
	}
	payloadBytes := primaryExactCatalogHeaderReserve + s.entryBytes
	extent, ok := primaryExactExtent(payloadBytes+storeio.PageHeaderSize+storeio.PageTrailerSize, s.pageSize, s.maxPageSize)
	if !ok || len(s.children) == cap(s.children) {
		return storeio.ErrPrimaryExactIndexCorrupt
	}
	page, err := s.sink.AllocatePage(storeio.PagePrimaryExactCatalog, extent, 0)
	if err != nil {
		return err
	}
	if _, err := storeio.EncodePrimaryExactCatalogLeafPage(page.Bytes(), s.sink.StoreIdentity(), s.sink.BuildGeneration(), page.Ref().LogicalID, s.entries); err != nil {
		return err
	}
	if err := page.Stage(); err != nil {
		return err
	}
	s.children = append(s.children, page.Ref())
	s.entries = s.entries[:0]
	s.prefixArena = s.prefixArena[:0]
	s.entryBytes = 0
	return nil
}

func (s *primaryExactCatalogStream) Finish() (storeio.PageRef, uint32, error) {
	if s == nil || s.finished || s.leaves == 0 {
		return storeio.PageRef{}, 0, storeio.ErrInvalidWrite
	}
	s.finished = true
	if err := s.flushLeaf(); err != nil {
		return storeio.PageRef{}, 0, err
	}
	if len(s.children) == 1 {
		return s.children[0], s.leaves, nil
	}
	payloadBytes := primaryExactCatalogHeaderReserve + len(s.children)*storeio.PageRefSize
	extent, ok := primaryExactExtent(payloadBytes+storeio.PageHeaderSize+storeio.PageTrailerSize, s.pageSize, s.maxPageSize)
	if !ok {
		return storeio.PageRef{}, 0, fmt.Errorf("%w: exact streamed catalog depth", storeio.ErrPrimaryExactIndexCorrupt)
	}
	page, err := s.sink.AllocatePage(storeio.PagePrimaryExactCatalog, extent, 0)
	if err != nil {
		return storeio.PageRef{}, 0, err
	}
	if _, err := storeio.EncodePrimaryExactCatalogIndexPage(page.Bytes(), s.sink.StoreIdentity(), s.sink.BuildGeneration(), page.Ref().LogicalID, s.children); err != nil {
		return storeio.PageRef{}, 0, err
	}
	if err := page.Stage(); err != nil {
		return storeio.PageRef{}, 0, err
	}
	return page.Ref(), s.leaves, nil
}
