package storeio

import (
	"bytes"
	"fmt"
	"math/bits"
)

type GenerationMigrationExactLeafEmitter func(indexID uint32, leaf []IndexTermLeafTerm, piece bool) error

// StreamGenerationMigrationExactLeaves consumes one fully merged run and
// reproduces CutIndexTermLeaves incrementally. Non-giant state is bounded by
// one output leaf; giant state is bounded by one absolute 2,048-tile stripe.
func StreamGenerationMigrationExactLeaves(
	read GenerationMigrationExactRunReader,
	region GenerationMigrationExactRunRegion,
	storeID [16]byte,
	generation uint64,
	budget int,
	live IndexTermLeafLiveLookup,
	emit GenerationMigrationExactLeafEmitter,
) error {
	if read == nil || region.Runs != 1 || region.Pages == 0 || storeID == ([16]byte{}) || generation == 0 || budget <= indexTermLeafHeaderBytes || live == nil || emit == nil {
		return fmt.Errorf("%w: exact run leaf stream", ErrInvalidWrite)
	}
	postingCapacity := budget/(1+5+10) + 2
	state := generationMigrationExactLeafStream{
		storeID: storeID, budget: budget, live: live, emit: emit,
		leafTerms:    make([]IndexTermLeafTerm, 0, budget/(indexTermLeafDescriptorBytes+2)+2),
		leafKeys:     make([]byte, 0, budget),
		leafPostings: make([]IndexTermLeafPosting, 0, postingCapacity),
		termPostings: make([]IndexTermLeafPosting, 0, max(postingCapacity, IndexTermLeafStripeTiles)+2),
	}
	page := make([]byte, region.First.Length)
	var previous GenerationMigrationExactRunRecord
	var previousKey [IndexTermMaxKeyBytes]byte
	havePrevious := false
	var runID uint64
	for pageAt := uint64(0); pageAt < region.Pages; pageAt++ {
		ref, ok := region.RefAt(pageAt)
		if !ok {
			return ErrGenerationMigrationManifestCorrupt
		}
		if err := read(ref, page); err != nil {
			return err
		}
		view, err := OpenGenerationMigrationExactRunPage(page, ref, storeID, generation)
		if err != nil || view.PageOrdinal() != uint32(pageAt) || view.Last() != (pageAt+1 == region.Pages) {
			if err != nil {
				return err
			}
			return ErrGenerationMigrationManifestCorrupt
		}
		if pageAt == 0 {
			runID = view.RunID()
		} else if view.RunID() != runID {
			return ErrGenerationMigrationManifestCorrupt
		}
		it := view.Iterator()
		for {
			record, ok := it.Next()
			if !ok {
				break
			}
			if havePrevious && compareGenerationMigrationExactRunRecord(previous, record) >= 0 {
				return ErrGenerationMigrationManifestCorrupt
			}
			if !state.hasTerm || state.indexID != record.IndexID || !bytes.Equal(state.termKey[:state.termKeyLen], record.Key) {
				if err := state.finishTerm(); err != nil {
					return err
				}
				if state.hasIndex && state.indexID != record.IndexID {
					if err := state.flushLeaf(); err != nil {
						return err
					}
				}
				state.indexID, state.hasIndex, state.hasTerm = record.IndexID, true, true
				state.termKeyLen = copy(state.termKey[:], record.Key)
			}
			if err := state.appendPosting(record.TileID, record.Mask); err != nil {
				return err
			}
			previous = record
			keyBytes := copy(previousKey[:], record.Key)
			previous.Key = previousKey[:keyBytes:keyBytes]
			havePrevious = true
		}
	}
	if err := state.finishTerm(); err != nil {
		return err
	}
	return state.flushLeaf()
}

type generationMigrationExactLeafStream struct {
	storeID                  [16]byte
	budget                   int
	live                     IndexTermLeafLiveLookup
	emit                     GenerationMigrationExactLeafEmitter
	indexID                  uint32
	hasIndex, hasTerm, giant bool
	termKey                  [IndexTermMaxKeyBytes]byte
	termKeyLen               int
	termPostings             []IndexTermLeafPosting
	leafTerms                []IndexTermLeafTerm
	leafKeys                 []byte
	leafPostings             []IndexTermLeafPosting
	leafTermBytes            int
}

func (s *generationMigrationExactLeafStream) appendPosting(tileID uint32, mask uint64) error {
	liveMask := s.live(tileID)
	if liveMask == nil || mask == 0 || mask&^liveMask[0] != 0 || len(s.termPostings) == cap(s.termPostings) {
		return ErrPrimaryExactIndexCorrupt
	}
	posting := IndexTermLeafPosting{Posting: TermPosting{TileID: tileID, Rows: uint16(bits.OnesCount64(mask))}, Live: liveMask, Chunk0Bits: mask, Chunk0Only: true}
	if s.giant && len(s.termPostings) != 0 && IndexTermLeafStripe(s.termPostings[len(s.termPostings)-1].Posting.TileID) != IndexTermLeafStripe(tileID) {
		if err := s.emitGiantStripe(); err != nil {
			return err
		}
	}
	s.termPostings = append(s.termPostings, posting)
	if !s.giant && IndexTermLeafGiant(IndexTermLeafEstimateChunk0TermBytes(s.termKeyLen, len(s.termPostings)), s.budget) {
		if err := s.flushLeaf(); err != nil {
			return err
		}
		s.giant = true
		lastStripe := IndexTermLeafStripe(s.termPostings[len(s.termPostings)-1].Posting.TileID)
		keep := len(s.termPostings) - 1
		for keep > 0 && IndexTermLeafStripe(s.termPostings[keep-1].Posting.TileID) == lastStripe {
			keep--
		}
		if keep != 0 {
			prefix := s.termPostings[keep:]
			complete := s.termPostings[:keep]
			s.termPostings = complete
			if err := s.emitGiantStripe(); err != nil {
				return err
			}
			s.termPostings = append(s.termPostings[:0], prefix...)
		}
	}
	return nil
}

func (s *generationMigrationExactLeafStream) finishTerm() error {
	if !s.hasTerm {
		return nil
	}
	if s.giant {
		if err := s.emitGiantStripe(); err != nil {
			return err
		}
	} else if err := s.appendLeafTerm(); err != nil {
		return err
	}
	s.termPostings = s.termPostings[:0]
	s.hasTerm, s.giant, s.termKeyLen = false, false, 0
	return nil
}

func (s *generationMigrationExactLeafStream) appendLeafTerm() error {
	record, ok := OpenIndexTermKeyRecord(s.storeID, s.termKey[:s.termKeyLen])
	if !ok || len(s.termPostings) == 0 {
		return ErrPrimaryExactIndexCorrupt
	}
	estimate := IndexTermLeafEstimateChunk0TermBytes(s.termKeyLen, len(s.termPostings))
	if IndexTermLeafRunCut(record.RouteHash) || len(s.leafTerms) != 0 && indexTermLeafEstimateLeafBytes(len(s.leafTerms)+1, s.leafTermBytes+estimate) > s.budget {
		if err := s.flushLeaf(); err != nil {
			return err
		}
	}
	if len(s.leafTerms) == cap(s.leafTerms) || s.termKeyLen > cap(s.leafKeys)-len(s.leafKeys) || len(s.termPostings) > cap(s.leafPostings)-len(s.leafPostings) {
		return fmt.Errorf("%w: exact leaf stream bound", ErrInvalidWrite)
	}
	keyAt := len(s.leafKeys)
	s.leafKeys = append(s.leafKeys, s.termKey[:s.termKeyLen]...)
	postingAt := len(s.leafPostings)
	s.leafPostings = append(s.leafPostings, s.termPostings...)
	record.Canonical = s.leafKeys[keyAt:len(s.leafKeys):len(s.leafKeys)]
	s.leafTerms = append(s.leafTerms, IndexTermLeafTerm{Key: record, Postings: s.leafPostings[postingAt:len(s.leafPostings):len(s.leafPostings)]})
	s.leafTermBytes += estimate
	return nil
}

func (s *generationMigrationExactLeafStream) emitGiantStripe() error {
	if len(s.termPostings) == 0 {
		return nil
	}
	record, ok := OpenIndexTermKeyRecord(s.storeID, s.termKey[:s.termKeyLen])
	if !ok {
		return ErrPrimaryExactIndexCorrupt
	}
	term := IndexTermLeafTerm{Key: record, Postings: s.termPostings}
	if err := CutGiantIndexTerm(&term, s.budget, func(leaf []IndexTermLeafTerm, piece bool) error { return s.emit(s.indexID, leaf, piece) }); err != nil {
		return err
	}
	s.termPostings = s.termPostings[:0]
	return nil
}

func (s *generationMigrationExactLeafStream) flushLeaf() error {
	if len(s.leafTerms) == 0 {
		return nil
	}
	if err := s.emit(s.indexID, s.leafTerms, false); err != nil {
		return err
	}
	s.leafTerms = s.leafTerms[:0]
	s.leafKeys = s.leafKeys[:0]
	s.leafPostings = s.leafPostings[:0]
	s.leafTermBytes = 0
	return nil
}
