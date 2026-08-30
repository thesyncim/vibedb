package durable

import (
	"bytes"
	"fmt"
	mathbits "math/bits"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/x/byteview"
)

type primaryUniqueTermSpan struct {
	offset int
	length int
}

// validatePrimaryUniquePutCurrent performs unique admission before a Put opens
// or expands the routed compact leaf. A highly compressed leaf can be larger
// than the bounded raw mutation workspace; a conflicting value must still be
// rejected as a constraint violation rather than first failing an unnecessary
// structural split. The current posting lookup supplies the existing row's
// stable slot so a replacement may retain its own terms.
func (c *Collection) validatePrimaryUniquePutCurrent(
	key, src []byte,
	resident storeio.ResidentPrimaryRoute,
) error {
	if len(c.options.uniqueIndexIDs) == 0 ||
		c.journalReplaying && c.primaryUniqueReplayValidated {
		return nil
	}
	tileID, postingBits, found, err := c.primaryUniqueCurrentPosting(key)
	if err != nil {
		return err
	}
	oldSlot := uint8(0)
	if found {
		if postingBits == 0 || postingBits&(postingBits-1) != 0 ||
			storeio.BucketID(tileID>>2) != resident.Bucket {
			return storeio.ErrPrimaryExactIndexCorrupt
		}
		oldSlot = uint8((tileID&3)<<6) |
			uint8(mathbits.TrailingZeros64(postingBits))
	}
	return c.validatePrimaryUniquePut(src, found, resident, oldSlot)
}

// validatePrimaryUniquePut checks one canonical final document against the
// current exact-index epoch while the collection writer is held. A replacement
// may retain its own current posting; every other live bit is a conflict.
func (c *Collection) validatePrimaryUniquePut(
	src []byte,
	found bool,
	resident storeio.ResidentPrimaryRoute,
	oldSlot uint8,
) error {
	if c.journalReplaying && c.primaryUniqueReplayValidated {
		return nil
	}
	indexIDs := c.options.uniqueIndexIDs
	if len(indexIDs) == 0 {
		return nil
	}
	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	var canonical [storeio.IndexTermMaxKeyBytes]byte
	for _, indexID := range indexIDs {
		exact := c.options.indexes[indexID]
		term, present, termErr := appendOnlineIndexDocumentTerm(
			canonical[:0], components[:], exact, src, true,
		)
		if termErr != nil {
			return termErr
		}
		if !present {
			continue
		}
		containsNull, valid := storeio.IndexTermKeyContainsNull(term)
		if !valid {
			return storeio.ErrPrimaryExactIndexCorrupt
		}
		if containsNull {
			continue
		}
		ignoredTile := ^uint32(0)
		ignoredBits := uint64(0)
		if found {
			ignoredTile = uint32(resident.Bucket)<<2 | uint32(oldSlot>>6)
			ignoredBits = uint64(1) << uint(oldSlot&63)
		}
		if err := c.rejectPrimaryUniqueTermConflict(
			indexID, term, ignoredTile, ignoredBits, nil,
		); err != nil {
			return err
		}
	}
	return nil
}

// validatePrimaryUniqueRecoveryBatch validates one oversized atomic journal
// record as a final image before its entries are replayed one at a time. Every
// current posting owned by an affected primary key is subtracted, so swaps and
// cycles are admitted; duplicate final terms and conflicts with untouched rows
// remain violations. Open recomputes this certificate on every replay attempt.
func (c *Collection) validatePrimaryUniqueRecoveryBatch(
	entries []storeio.RecoveryBatchEntry,
) error {
	indexIDs := c.options.uniqueIndexIDs
	if len(indexIDs) == 0 {
		return nil
	}
	ignored := make(map[uint32]uint64, len(entries))
	for i := range entries {
		kind := entries[i].Kind
		if kind != storeio.RecoveryRecordKindPut &&
			kind != storeio.RecoveryRecordKindDelete {
			return fmt.Errorf(
				"%w: unknown replay kind %d",
				storeio.ErrRecoveryJournalRecord, kind,
			)
		}
		tileID, bits, found, err := c.primaryUniqueCurrentPosting(
			entries[i].Key,
		)
		if err != nil {
			return err
		}
		if found {
			ignored[tileID] |= bits
		}
	}

	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	var canonical [storeio.IndexTermMaxKeyBytes]byte
	for _, indexID := range indexIDs {
		exact := c.options.indexes[indexID]
		terms := make([]primaryUniqueTermSpan, 0, len(entries))
		arena := make([]byte, 0, len(entries)*32)
		for i := range entries {
			entry := &entries[i]
			if entry.Kind == storeio.RecoveryRecordKindDelete {
				continue
			}
			term, present, termErr := appendOnlineIndexDocumentTerm(
				canonical[:0], components[:], exact, entry.Value, true,
			)
			if termErr != nil {
				return termErr
			}
			if !present {
				continue
			}
			containsNull, valid := storeio.IndexTermKeyContainsNull(term)
			if !valid {
				return storeio.ErrPrimaryExactIndexCorrupt
			}
			if containsNull {
				continue
			}
			start := len(arena)
			arena = append(arena, term...)
			terms = append(terms, primaryUniqueTermSpan{
				offset: start, length: len(term),
			})
		}
		slices.SortFunc(terms, func(a, b primaryUniqueTermSpan) int {
			return bytes.Compare(
				arena[a.offset:a.offset+a.length],
				arena[b.offset:b.offset+b.length],
			)
		})
		for i := 1; i < len(terms); i++ {
			previous, current := terms[i-1], terms[i]
			if bytes.Equal(
				arena[previous.offset:previous.offset+previous.length],
				arena[current.offset:current.offset+current.length],
			) {
				return fmt.Errorf(
					"%w: duplicate recovery final term",
					store.ErrUniqueIndexViolation,
				)
			}
		}
		for _, span := range terms {
			term := arena[span.offset : span.offset+span.length]
			if err := c.rejectPrimaryUniqueTermConflict(
				indexID, term, ^uint32(0), 0, ignored,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Collection) primaryUniqueCurrentPosting(
	key []byte,
) (tileID uint32, bits uint64, found bool, err error) {
	state := c.state.Load()
	if state == nil {
		return 0, 0, false, storeio.ErrPrimaryExactIndexCorrupt
	}
	if state.root.PrimaryRoot == (storeio.PageRef{}) {
		return 0, 0, false, nil
	}
	route, err := c.currentPrimaryResidentRoute(state, key)
	if err != nil {
		return 0, 0, false, err
	}
	router := c.primaryRouter.Load()
	if router == nil {
		return 0, 0, false, storeio.ErrPrimaryExactIndexCorrupt
	}
	lease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return 0, 0, false, err
	}
	defer lease.Release()
	stripe, ok := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), c.storeID, route.Bucket,
	)
	if !ok {
		return 0, 0, false, storeio.ErrPrimaryExactIndexCorrupt
	}
	baseRank, baseFound := stripe.FindKey(key)
	baseSlot := uint8(0)
	if baseFound {
		var slotOK bool
		baseSlot, slotOK = stripe.PostingSlot(baseRank)
		if !slotOK {
			return 0, 0, false, storeio.ErrPrimaryExactIndexCorrupt
		}
	}
	_, disposition, overlaySlot := c.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, state.root.Generation,
	)
	slot := baseSlot
	switch disposition {
	case primaryUnifiedOverlayValue:
		found = true
		slot = overlaySlot
	case primaryUnifiedOverlayDeleted:
		return 0, 0, false, nil
	case primaryUnifiedOverlayMissing:
		found = baseFound
	default:
		return 0, 0, false, storeio.ErrPrimaryExactIndexCorrupt
	}
	if !found {
		return 0, 0, false, nil
	}
	tileID = uint32(route.Bucket)<<2 | uint32(slot>>6)
	bits = uint64(1) << uint(slot&63)
	return tileID, bits, true, nil
}

// validatePrimaryUniqueBatch checks final batch values, not statement order.
// Every current posting owned by a key in the batch is subtracted before the
// probe result is judged, allowing swaps and delete-then-reuse atomically.
func (c *Collection) validatePrimaryUniqueBatch() error {
	if c.journalReplaying && c.primaryUniqueReplayValidated {
		return nil
	}
	indexIDs := c.options.uniqueIndexIDs
	if len(indexIDs) == 0 {
		return nil
	}
	ignored := make(map[uint32]uint64, len(c.batchPrimaryMutations))
	for i := range c.batchPrimaryMutations {
		mutation := &c.batchPrimaryMutations[i]
		if !mutation.found {
			continue
		}
		tileID := uint32(mutation.resident.Bucket)<<2 |
			uint32(mutation.oldSlot>>6)
		ignored[tileID] |= uint64(1) << uint(mutation.oldSlot&63)
	}

	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	var canonical [storeio.IndexTermMaxKeyBytes]byte
	for _, indexID := range indexIDs {
		exact := c.options.indexes[indexID]
		terms := make([]primaryUniqueTermSpan, 0, len(c.batchPrimaryMutations))
		arena := make([]byte, 0, len(c.batchPrimaryMutations)*32)
		for i := range c.batchPrimaryMutations {
			mutation := &c.batchPrimaryMutations[i]
			if mutation.remove {
				continue
			}
			term, present, termErr := appendOnlineIndexDocumentTerm(
				canonical[:0], components[:], exact, mutation.value, true,
			)
			if termErr != nil {
				return termErr
			}
			if !present {
				continue
			}
			containsNull, valid := storeio.IndexTermKeyContainsNull(term)
			if !valid {
				return storeio.ErrPrimaryExactIndexCorrupt
			}
			if containsNull {
				continue
			}
			start := len(arena)
			arena = append(arena, term...)
			terms = append(terms, primaryUniqueTermSpan{
				offset: start, length: len(term),
			})
		}
		slices.SortFunc(terms, func(a, b primaryUniqueTermSpan) int {
			return bytes.Compare(
				arena[a.offset:a.offset+a.length],
				arena[b.offset:b.offset+b.length],
			)
		})
		for i := 1; i < len(terms); i++ {
			previous, current := terms[i-1], terms[i]
			if bytes.Equal(
				arena[previous.offset:previous.offset+previous.length],
				arena[current.offset:current.offset+current.length],
			) {
				return fmt.Errorf(
					"%w: duplicate final batch term",
					store.ErrUniqueIndexViolation,
				)
			}
		}
		for _, span := range terms {
			term := arena[span.offset : span.offset+span.length]
			if err := c.rejectPrimaryUniqueTermConflict(
				indexID, term, ^uint32(0), 0, ignored,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Collection) rejectPrimaryUniqueTermConflict(
	indexID uint32,
	term []byte,
	ignoredTile uint32,
	ignoredBits uint64,
	ignored map[uint32]uint64,
) error {
	state := c.state.Load()
	if state == nil || int(indexID) >= len(c.options.indexes) {
		return storeio.ErrPrimaryExactIndexCorrupt
	}
	snapshot := Snapshot{
		collection: c,
		state:      state,
		epoch:      c.primaryEpoch,
		indexes:    c.options.indexes,
	}
	var workspace IndexWorkspace
	var maskStorage [2]store.Mask
	masks, err := snapshot.appendPrimaryExactMasksKey(
		maskStorage[:0], &workspace, indexID, term,
	)
	workspace.Release()
	if err != nil {
		return err
	}
	for _, mask := range masks {
		bits := mask.Bits
		if mask.Chunk == ignoredTile {
			bits &^= ignoredBits
		}
		if ignored != nil {
			bits &^= ignored[mask.Chunk]
		}
		if bits != 0 {
			return fmt.Errorf(
				"%w: duplicate canonical term",
				store.ErrUniqueIndexViolation,
			)
		}
	}
	return nil
}

func validatePrimaryBulkUnique(
	records []storeio.PrimaryGraphRecord,
	options normalizedFileStoreOptions,
) error {
	indexIDs := options.uniqueIndexIDs
	if len(indexIDs) == 0 {
		return nil
	}
	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	var canonical [storeio.IndexTermMaxKeyBytes]byte
	for _, indexID := range indexIDs {
		exact := options.indexes[indexID]
		seen := make(map[string]struct{}, len(records))
		for i := range records {
			term, present, termErr := appendOnlineIndexDocumentTerm(
				canonical[:0], components[:], exact,
				byteview.Bytes(records[i].Value), true,
			)
			if termErr != nil {
				return termErr
			}
			if !present {
				continue
			}
			containsNull, valid := storeio.IndexTermKeyContainsNull(term)
			if !valid {
				return storeio.ErrPrimaryExactIndexCorrupt
			}
			if containsNull {
				continue
			}
			identity := string(term)
			if _, exists := seen[identity]; exists {
				return fmt.Errorf(
					"%w: duplicate bulk term",
					store.ErrUniqueIndexViolation,
				)
			}
			seen[identity] = struct{}{}
		}
	}
	return nil
}
