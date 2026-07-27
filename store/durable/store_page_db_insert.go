package durable

import (
	"errors"
	"fmt"
	"math/bits"

	"github.com/thesyncim/vibejson/x/byteview"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// storePageDBInsertSlot owns at most one admitted document lease while an
// insert is planned. The stable location is selected from the persistent
// FreeChunkHint without building a heap-side free list or retaining one
// pointer per key.
type storePageDBInsertSlot struct {
	chunk    uint32
	slot     uint8
	document storeio.PageRef
	lease    storeio.PageLease
	existing bool
}

type storePageDBKeyRootPlan struct {
	header  storeio.PageKeyDirectoryHeader
	entries [2]storeio.PageKeyBranch
	count   int
	ref     storeio.PageRef
}

func (db *StorePageDB) insertLocked(pages *storeio.PageFile, root storeio.StateRoot,
	fileEnd uint64, key string, src []byte, hash uint64) (err error) {
	if root.DocumentCount == ^uint64(0) {
		return fmt.Errorf("%w: document count exhausted", store.ErrTooLarge)
	}
	selected, err := db.findInsertSlot(pages, root)
	if err != nil {
		return err
	}
	leaseOwned := selected.existing
	defer func() {
		clear(db.rows[:])
		if leaseOwned {
			err = errors.Join(err, selected.lease.Close())
		}
	}()

	depth := 0
	rowCount := 1
	live := uint64(1) << selected.slot
	required := uint64(storeio.PageHeaderSize + storeio.PageTrailerSize +
		storeio.DocumentPagePayloadHeaderSize + storeio.DocumentPageRecordSize + len(key) + len(src))
	if selected.existing {
		doc := storeio.AdmittedDocumentPage(selected.lease.Bytes())
		if doc.Header().ChunkID != selected.chunk || doc.Header().Live&(uint64(1)<<selected.slot) != 0 {
			return corruptStorePage("insert stable slot", storeio.ErrDocumentPageCorrupt)
		}
		depth, err = db.loadDirectoryPath(pages, root, selected.chunk, selected.document)
		if err != nil {
			return err
		}
		rowCount, live, required, err = db.buildInsertedDocumentRows(doc, selected.slot, key, src)
		if err != nil {
			return err
		}
	} else {
		depth, err = db.loadDirectoryInsertPath(pages, root, selected.chunk)
		if err != nil {
			return err
		}
		db.rows[0] = storeio.DocumentRecord{Key: byteview.Bytes(key), JSON: src, Slot: selected.slot}
	}
	docSize, ok := storePageExtent(required, db.options.Open.MaxDocumentPageBytes)
	if !ok {
		return fmt.Errorf("%w: chunk=%d bytes=%d max=%d", ErrStoreDocumentPageTooLarge,
			selected.chunk, required, db.options.Open.MaxDocumentPageBytes)
	}
	location := storeio.PageKeyLocation{Hash: hash, Chunk: selected.chunk, Slot: selected.slot}
	keyDepth, reuseKey, err := db.loadKeyInsertPath(pages, root, location)
	if err != nil {
		return err
	}
	if err := db.commitInsert(root, fileEnd, selected, docSize, rowCount, live, depth,
		keyDepth, reuseKey, location, &selected.lease); err != nil {
		return err
	}
	leaseOwned = false
	return nil
}

func (db *StorePageDB) findInsertSlot(pages *storeio.PageFile, root storeio.StateRoot) (storePageDBInsertSlot, error) {
	mask := ^uint64(0)
	if root.ChunkDocuments < 64 {
		mask = uint64(1)<<root.ChunkDocuments - 1
	}
	for chunk := root.FreeChunkHint; chunk < root.ChunkHighWater; chunk++ {
		document, ok, err := resolveStoreDocumentPage(pages, root.ChunkDirectory, chunk)
		if err != nil {
			return storePageDBInsertSlot{}, err
		}
		if !ok {
			return storePageDBInsertSlot{chunk: chunk}, nil
		}
		lease, err := pages.Cache().Pin(document)
		if err != nil {
			return storePageDBInsertSlot{}, storePageReadError(err)
		}
		doc := storeio.AdmittedDocumentPage(lease.Bytes())
		if doc.Header().ChunkID != chunk {
			_ = lease.Close()
			return storePageDBInsertSlot{}, corruptStorePage("insert document identity", storeio.ErrDocumentPageCorrupt)
		}
		free := ^doc.Header().Live & mask
		if free != 0 {
			return storePageDBInsertSlot{
				chunk: chunk, slot: uint8(bits.TrailingZeros64(free)), document: document,
				lease: lease, existing: true,
			}, nil
		}
		if err := lease.Close(); err != nil {
			return storePageDBInsertSlot{}, err
		}
	}
	if root.ChunkHighWater == ^uint32(0) {
		return storePageDBInsertSlot{}, store.ErrTooLarge
	}
	return storePageDBInsertSlot{chunk: root.ChunkHighWater}, nil
}

func (db *StorePageDB) buildInsertedDocumentRows(doc storeio.DocumentPageView, slot uint8,
	key string, src []byte) (count int, live uint64, required uint64, err error) {
	live = doc.Header().Live | uint64(1)<<slot
	required = uint64(storeio.PageHeaderSize + storeio.PageTrailerSize +
		storeio.DocumentPagePayloadHeaderSize)
	inserted := false
	for rank := 0; rank < doc.Len(); rank++ {
		row, ok := doc.RecordAt(rank)
		if !ok || row.Slot == slot {
			return 0, 0, 0, corruptStorePage("insert document record", storeio.ErrDocumentPageCorrupt)
		}
		if !inserted && row.Slot > slot {
			db.rows[count] = storeio.DocumentRecord{Key: byteview.Bytes(key), JSON: src, Slot: slot}
			count++
			inserted = true
		}
		db.rows[count] = row
		count++
	}
	if !inserted {
		db.rows[count] = storeio.DocumentRecord{Key: byteview.Bytes(key), JSON: src, Slot: slot}
		count++
	}
	for i := 0; i < count; i++ {
		required += uint64(storeio.DocumentPageRecordSize + len(db.rows[i].Key) + len(db.rows[i].JSON))
	}
	return count, live, required, nil
}

func storePageChunkPrefix(chunk uint32, shift uint8) uint32 {
	covered := shift + 6
	if covered >= 32 {
		return 0
	}
	return chunk &^ (uint32(1)<<covered - 1)
}

func (db *StorePageDB) initInsertDirectoryNode(node *storePageDBDirectoryNode,
	root storeio.StateRoot, chunk uint32, shift uint8) {
	*node = storePageDBDirectoryNode{}
	lane := uint8(chunk >> shift & 63)
	node.header = storeio.ChunkDirectoryHeader{
		StoreID: root.StoreID, Generation: root.Generation, PageSize: storePageQuantum,
		Prefix: storePageChunkPrefix(chunk, shift), Bitmap: uint64(1) << lane, Shift: shift,
	}
	node.newBitmap = node.header.Bitmap
	node.rank = 0
}

// loadDirectoryInsertPath builds a root-to-leaf radix path containing both
// admitted existing nodes and value-only new nodes. Sequential high-water
// growth needs at most one new root level; reuse of an empty historical chunk
// attaches below an existing node.
func (db *StorePageDB) loadDirectoryInsertPath(pages *storeio.PageFile,
	root storeio.StateRoot, chunk uint32) (int, error) {
	if root.ChunkDirectory == (storeio.PageRef{}) {
		db.initInsertDirectoryNode(&db.path[0], root, chunk, 0)
		return 1, nil
	}
	ref := root.ChunkDirectory
	lease, err := pages.Cache().Pin(ref)
	if err != nil {
		return 0, storePageReadError(err)
	}
	rootView := storeio.AdmittedChunkDirectoryPage(lease.Bytes())
	rootHeader := rootView.Header()
	if closeErr := lease.Close(); closeErr != nil {
		return 0, closeErr
	}
	if storePageChunkPrefix(chunk, rootHeader.Shift) != rootHeader.Prefix {
		if rootHeader.Shift >= 30 {
			return 0, corruptStorePage("insert chunk radix range", storeio.ErrChunkDirectoryCorrupt)
		}
		shift := rootHeader.Shift + 6
		node := &db.path[0]
		*node = storePageDBDirectoryNode{}
		oldLane := uint8(rootHeader.Prefix >> shift & 63)
		newLane := uint8(chunk >> shift & 63)
		if oldLane == newLane {
			return 0, corruptStorePage("insert chunk radix root", storeio.ErrChunkDirectoryCorrupt)
		}
		node.header = storeio.ChunkDirectoryHeader{
			StoreID: root.StoreID, Generation: root.Generation, PageSize: storePageQuantum,
			Prefix: storePageChunkPrefix(chunk, shift), Bitmap: uint64(1)<<oldLane | uint64(1)<<newLane,
			Shift: shift,
		}
		node.newBitmap = node.header.Bitmap
		node.rank = bits.OnesCount64(node.newBitmap & (uint64(1)<<newLane - 1))
		node.refs[0] = ref
		node.count = 1
		depth := 1
		for childShift := rootHeader.Shift; ; childShift -= 6 {
			if depth == len(db.path) {
				return 0, corruptStorePage("insert chunk radix depth", storeio.ErrChunkDirectoryCorrupt)
			}
			db.initInsertDirectoryNode(&db.path[depth], root, chunk, childShift)
			depth++
			if childShift == 0 {
				return depth, nil
			}
		}
	}

	depth := 0
	for {
		if depth == len(db.path) {
			return 0, corruptStorePage("insert chunk radix depth", storeio.ErrChunkDirectoryCorrupt)
		}
		lease, err := pages.Cache().Pin(ref)
		if err != nil {
			return 0, storePageReadError(err)
		}
		view := storeio.AdmittedChunkDirectoryPage(lease.Bytes())
		header := view.Header()
		if storePageChunkPrefix(chunk, header.Shift) != header.Prefix {
			_ = lease.Close()
			return 0, corruptStorePage("insert chunk radix prefix", storeio.ErrChunkDirectoryCorrupt)
		}
		lane := uint8(chunk >> header.Shift & 63)
		bit := uint64(1) << lane
		node := &db.path[depth]
		*node = storePageDBDirectoryNode{header: header, count: view.Len(),
			rank: bits.OnesCount64(header.Bitmap & (bit - 1)), newBitmap: header.Bitmap | bit,
			hadChild: header.Bitmap&bit != 0}
		for rank := 0; rank < node.count; rank++ {
			child, ok := view.RefAt(rank)
			if !ok {
				_ = lease.Close()
				return 0, corruptStorePage("insert chunk radix reference", storeio.ErrChunkDirectoryCorrupt)
			}
			node.refs[rank] = child
		}
		depth++
		if !node.hadChild {
			if closeErr := lease.Close(); closeErr != nil {
				return 0, closeErr
			}
			for shift := int(header.Shift) - 6; shift >= 0; shift -= 6 {
				if depth == len(db.path) {
					return 0, corruptStorePage("insert chunk radix depth", storeio.ErrChunkDirectoryCorrupt)
				}
				db.initInsertDirectoryNode(&db.path[depth], root, chunk, uint8(shift))
				depth++
			}
			return depth, nil
		}
		child := node.refs[node.rank]
		if closeErr := lease.Close(); closeErr != nil {
			return 0, closeErr
		}
		if header.Shift == 0 {
			return 0, corruptStorePage("insert unexpectedly live chunk", storeio.ErrChunkDirectoryCorrupt)
		}
		if child.Kind != storeio.PageChunkDirectory || child.Offset >= ref.Offset {
			return 0, corruptStorePage("insert chunk radix child", storeio.ErrChunkDirectoryCorrupt)
		}
		ref = child
	}
}

func compareStorePageKeyLocation(a, b storeio.PageKeyLocation) int {
	if a.Hash < b.Hash {
		return -1
	}
	if a.Hash > b.Hash {
		return 1
	}
	if a.Chunk < b.Chunk {
		return -1
	}
	if a.Chunk > b.Chunk {
		return 1
	}
	return int(a.Slot) - int(b.Slot)
}

func (db *StorePageDB) loadKeyInsertPath(pages *storeio.PageFile, root storeio.StateRoot,
	location storeio.PageKeyLocation) (depth int, reuse bool, err error) {
	if root.KeyDirectory == (storeio.PageRef{}) {
		db.keyLeaf = storePageDBKeyLeaf{
			header: storeio.PageKeyDirectoryHeader{StoreID: root.StoreID, Generation: root.Generation,
				PageSize: storePageQuantum, MinHash: location.Hash, MaxHash: location.Hash},
			count: 1, newCount: 1,
		}
		db.keyLeaf.entries[0] = location
		return 0, false, nil
	}
	ref := root.KeyDirectory
	for {
		lease, err := pages.Cache().Pin(ref)
		if err != nil {
			return 0, false, storePageReadError(err)
		}
		view := storeio.AdmittedPageKeyDirectory(lease.Bytes())
		header := view.Header()
		if header.Level == 0 {
			leaf := &db.keyLeaf
			*leaf = storePageDBKeyLeaf{header: header, count: view.Len()}
			leaf.rank = leaf.count
			for rank := 0; rank < leaf.count; rank++ {
				entry, ok := view.LocationAt(rank)
				if !ok {
					_ = lease.Close()
					return 0, false, corruptStorePage("insert key leaf", storeio.ErrKeyDirectoryCorrupt)
				}
				leaf.entries[rank] = entry
				if leaf.rank == leaf.count && compareStorePageKeyLocation(entry, location) >= 0 {
					leaf.rank = rank
				}
			}
			if closeErr := lease.Close(); closeErr != nil {
				return 0, false, closeErr
			}
			if leaf.rank < leaf.count && leaf.entries[leaf.rank] == location {
				leaf.newCount = leaf.count
				return depth, true, nil
			}
			copy(leaf.entries[leaf.rank+1:], leaf.entries[leaf.rank:leaf.count])
			leaf.entries[leaf.rank] = location
			leaf.count++
			leaf.newCount = leaf.count
			leaf.header.MinHash = leaf.entries[0].Hash
			leaf.header.MaxHash = leaf.entries[leaf.count-1].Hash
			return depth, false, nil
		}
		if depth == len(db.keyPath) {
			_ = lease.Close()
			return 0, false, corruptStorePage("insert key depth", storeio.ErrKeyDirectoryCorrupt)
		}
		node := &db.keyPath[depth]
		*node = storePageDBKeyBranchNode{header: header, count: view.Len(), rank: -1}
		for rank := 0; rank < node.count; rank++ {
			entry, ok := view.BranchAt(rank)
			if !ok {
				_ = lease.Close()
				return 0, false, corruptStorePage("insert key branch", storeio.ErrKeyDirectoryCorrupt)
			}
			node.entries[rank] = entry
			if node.rank < 0 && entry.MaxHash >= location.Hash {
				node.rank = rank
			}
		}
		if node.rank < 0 {
			node.rank = node.count - 1
		}
		node.newCount = node.count
		child := node.entries[node.rank].Child
		if closeErr := lease.Close(); closeErr != nil {
			return 0, false, closeErr
		}
		if child.Kind != storeio.PageKeyDirectory || child.Offset >= ref.Offset {
			return 0, false, corruptStorePage("insert key child", storeio.ErrKeyDirectoryCorrupt)
		}
		ref = child
		depth++
	}
}

func (db *StorePageDB) commitInsert(root storeio.StateRoot, oldFileEnd uint64,
	selected storePageDBInsertSlot, docSize uint32, rowCount int, live uint64,
	directoryDepth, keyDepth int, reuseKey bool, location storeio.PageKeyLocation,
	oldLease *storeio.PageLease) (err error) {
	if root.Generation == ^uint64(0) {
		return fmt.Errorf("%w: generation exhausted", store.ErrTooLarge)
	}
	generation := root.Generation + 1
	offset := oldFileEnd
	nextLogical := root.NextLogicalID
	newDocument := storeio.PageRef{Offset: offset, Generation: generation, Length: docSize, Kind: storeio.PageDocument}
	if selected.existing {
		newDocument.LogicalID = selected.document.LogicalID
	} else {
		newDocument.LogicalID = nextLogical
		nextLogical++
	}
	offset += uint64(docSize)
	for level := directoryDepth - 1; level >= 0; level-- {
		node := &db.path[level]
		logical := node.header.LogicalID
		if logical == 0 {
			logical = nextLogical
			nextLogical++
		}
		node.newRef = storeio.PageRef{Offset: offset, LogicalID: logical, Generation: generation,
			Length: storePageQuantum, Kind: storeio.PageChunkDirectory}
		offset += uint64(storePageQuantum)
	}

	keyPages := 0
	var propagated [2]storeio.PageKeyBranch
	propagatedCount := 0
	if !reuseKey {
		leaf := &db.keyLeaf
		leaf.rightRef = storeio.PageRef{}
		leaf.rightCount = 0
		leftLogical := leaf.header.LogicalID
		if leftLogical == 0 {
			leftLogical = nextLogical
			nextLogical++
		}
		leaf.newCount = leaf.count
		if leaf.count > storePageDBKeyLeafCapacity {
			leaf.newCount = leaf.count / 2
			leaf.rightCount = leaf.count - leaf.newCount
		}
		leaf.newRef = storeio.PageRef{Offset: offset, LogicalID: leftLogical, Generation: generation,
			Length: storePageQuantum, Kind: storeio.PageKeyDirectory}
		offset += uint64(storePageQuantum)
		keyPages++
		propagated[0] = storeio.PageKeyBranch{MaxHash: leaf.entries[leaf.newCount-1].Hash, Child: leaf.newRef}
		propagatedCount = 1
		if leaf.rightCount != 0 {
			leaf.rightRef = storeio.PageRef{Offset: offset, LogicalID: nextLogical, Generation: generation,
				Length: storePageQuantum, Kind: storeio.PageKeyDirectory}
			nextLogical++
			offset += uint64(storePageQuantum)
			keyPages++
			propagated[1] = storeio.PageKeyBranch{MaxHash: leaf.entries[leaf.count-1].Hash, Child: leaf.rightRef}
			propagatedCount = 2
		}
		for level := keyDepth - 1; level >= 0; level-- {
			node := &db.keyPath[level]
			if propagatedCount == 1 {
				node.entries[node.rank] = propagated[0]
			} else {
				copy(node.entries[node.rank+2:], node.entries[node.rank+1:node.count])
				node.entries[node.rank] = propagated[0]
				node.entries[node.rank+1] = propagated[1]
				node.count++
			}
			node.header.MinHash = min(node.header.MinHash, location.Hash)
			node.newCount = node.count
			node.rightCount = 0
			if node.count > storePageDBKeyBranchCapacity {
				node.newCount = node.count / 2
				node.rightCount = node.count - node.newCount
			}
			node.newRef = storeio.PageRef{Offset: offset, LogicalID: node.header.LogicalID, Generation: generation,
				Length: storePageQuantum, Kind: storeio.PageKeyDirectory}
			offset += uint64(storePageQuantum)
			keyPages++
			propagated[0] = storeio.PageKeyBranch{MaxHash: node.entries[node.newCount-1].MaxHash, Child: node.newRef}
			propagatedCount = 1
			if node.rightCount != 0 {
				node.rightRef = storeio.PageRef{Offset: offset, LogicalID: nextLogical, Generation: generation,
					Length: storePageQuantum, Kind: storeio.PageKeyDirectory}
				nextLogical++
				offset += uint64(storePageQuantum)
				keyPages++
				propagated[1] = storeio.PageKeyBranch{MaxHash: node.entries[node.count-1].MaxHash, Child: node.rightRef}
				propagatedCount = 2
			}
		}
	}

	var keyRootPlan storePageDBKeyRootPlan
	keyRoot := root.KeyDirectory
	if !reuseKey {
		keyRoot = propagated[0].Child
		if propagatedCount == 2 {
			level := uint8(1)
			minHash := db.keyLeaf.entries[0].Hash
			if keyDepth != 0 {
				level = db.keyPath[0].header.Level + 1
				minHash = min(db.keyPath[0].header.MinHash, location.Hash)
			}
			keyRootPlan = storePageDBKeyRootPlan{
				header: storeio.PageKeyDirectoryHeader{StoreID: root.StoreID, Generation: generation,
					LogicalID: nextLogical, PageSize: storePageQuantum, MinHash: minHash,
					MaxHash: propagated[1].MaxHash, Level: level},
				entries: propagated, count: 2,
				ref: storeio.PageRef{Offset: offset, LogicalID: nextLogical, Generation: generation,
					Length: storePageQuantum, Kind: storeio.PageKeyDirectory},
			}
			nextLogical++
			offset += uint64(storePageQuantum)
			keyPages++
			keyRoot = keyRootPlan.ref
		}
	}
	stateOffset := offset
	fileEnd := stateOffset + uint64(storePageQuantum)
	pageCount := 1 + directoryDepth + keyPages + 1 // document, radix, keys, state
	batch, err := db.committer.Begin(pageCount)
	if err != nil {
		return err
	}
	owned := true
	defer func() {
		if owned {
			err = errors.Join(err, batch.Abort())
		}
	}()

	next := root
	next.Generation = generation
	next.DocumentCount++
	next.NextLogicalID = nextLogical
	next.ChunkDirectory = db.path[0].newRef
	next.KeyDirectory = keyRoot
	if !selected.existing {
		next.LiveChunks++
		if selected.chunk == root.ChunkHighWater {
			next.ChunkHighWater++
		}
	}
	if rowCount == int(root.ChunkDocuments) {
		next.FreeChunkHint = selected.chunk + 1
	} else {
		next.FreeChunkHint = selected.chunk
	}

	pageIndex := 0
	buffer, err := batch.PageBuffer(pageIndex)
	if err != nil {
		return err
	}
	documentPage, err := storeio.EncodeDocumentPage(buffer[:docSize], storeio.DocumentPageHeader{
		StoreID: root.StoreID, Generation: generation, LogicalID: newDocument.LogicalID,
		PageSize: docSize, ChunkID: selected.chunk, Live: live,
	}, db.rows[:rowCount], nextLogical)
	if err != nil {
		return err
	}
	if err := batch.SetPage(pageIndex, int64(newDocument.Offset), len(documentPage)); err != nil {
		return err
	}
	pageIndex++

	child := newDocument
	for level := directoryDepth - 1; level >= 0; level-- {
		node := &db.path[level]
		if node.hadChild {
			node.refs[node.rank] = child
		} else {
			copy(node.refs[node.rank+1:], node.refs[node.rank:node.count])
			node.refs[node.rank] = child
			node.count++
		}
		buffer, err := batch.PageBuffer(pageIndex)
		if err != nil {
			return err
		}
		header := node.header
		header.Generation = generation
		header.LogicalID = node.newRef.LogicalID
		header.Bitmap = node.newBitmap
		page, err := storeio.EncodeChunkDirectoryPage(buffer[:storePageQuantum], header,
			node.refs[:node.count], fileEnd, nextLogical)
		if err != nil {
			return err
		}
		if err := batch.SetPage(pageIndex, int64(node.newRef.Offset), len(page)); err != nil {
			return err
		}
		pageIndex++
		child = node.newRef
	}

	if !reuseKey {
		leaf := &db.keyLeaf
		leftHeader := leaf.header
		leftHeader.Generation = generation
		leftHeader.LogicalID = leaf.newRef.LogicalID
		leftHeader.MinHash = leaf.entries[0].Hash
		leftHeader.MaxHash = leaf.entries[leaf.newCount-1].Hash
		if leaf.rightCount != 0 {
			leftHeader.Next = leaf.rightRef
		}
		buffer, err := batch.PageBuffer(pageIndex)
		if err != nil {
			return err
		}
		page, err := storeio.EncodePageKeyLeaf(buffer[:storePageQuantum], leftHeader,
			leaf.entries[:leaf.newCount], fileEnd, nextLogical, next.ChunkHighWater, next.ChunkDocuments)

		if err != nil {
			return err
		}
		if err := batch.SetPage(pageIndex, int64(leaf.newRef.Offset), len(page)); err != nil {
			return err
		}
		pageIndex++
		if leaf.rightCount != 0 {
			rightHeader := leaf.header
			rightHeader.Generation = generation
			rightHeader.LogicalID = leaf.rightRef.LogicalID
			rightHeader.MinHash = leaf.entries[leaf.newCount].Hash
			rightHeader.MaxHash = leaf.entries[leaf.count-1].Hash
			buffer, err := batch.PageBuffer(pageIndex)
			if err != nil {
				return err
			}
			page, err := storeio.EncodePageKeyLeaf(buffer[:storePageQuantum], rightHeader,
				leaf.entries[leaf.newCount:leaf.count], fileEnd, nextLogical, next.ChunkHighWater, next.ChunkDocuments)

			if err != nil {
				return err
			}
			if err := batch.SetPage(pageIndex, int64(leaf.rightRef.Offset), len(page)); err != nil {
				return err
			}
			pageIndex++
		}
		for level := keyDepth - 1; level >= 0; level-- {
			node := &db.keyPath[level]
			leftHeader := node.header
			leftHeader.Generation = generation
			leftHeader.LogicalID = node.newRef.LogicalID
			leftHeader.MinHash = min(leftHeader.MinHash, location.Hash)
			leftHeader.MaxHash = node.entries[node.newCount-1].MaxHash
			buffer, err := batch.PageBuffer(pageIndex)
			if err != nil {
				return err
			}
			page, err := storeio.EncodePageKeyBranch(buffer[:storePageQuantum], leftHeader,
				node.entries[:node.newCount], fileEnd, nextLogical)

			if err != nil {
				return err
			}
			if err := batch.SetPage(pageIndex, int64(node.newRef.Offset), len(page)); err != nil {
				return err
			}
			pageIndex++
			if node.rightCount != 0 {
				rightHeader := node.header
				rightHeader.Generation = generation
				rightHeader.LogicalID = node.rightRef.LogicalID
				rightHeader.MinHash = node.entries[node.newCount-1].MaxHash
				rightHeader.MaxHash = node.entries[node.count-1].MaxHash
				buffer, err := batch.PageBuffer(pageIndex)
				if err != nil {
					return err
				}
				page, err := storeio.EncodePageKeyBranch(buffer[:storePageQuantum], rightHeader,
					node.entries[node.newCount:node.count], fileEnd, nextLogical)

				if err != nil {
					return err
				}
				if err := batch.SetPage(pageIndex, int64(node.rightRef.Offset), len(page)); err != nil {
					return err
				}
				pageIndex++
			}
		}
		if keyRootPlan.count != 0 {
			buffer, err := batch.PageBuffer(pageIndex)
			if err != nil {
				return err
			}
			page, err := storeio.EncodePageKeyBranch(buffer[:storePageQuantum], keyRootPlan.header,
				keyRootPlan.entries[:keyRootPlan.count], fileEnd, nextLogical)

			if err != nil {
				return err
			}
			if err := batch.SetPage(pageIndex, int64(keyRootPlan.ref.Offset), len(page)); err != nil {
				return err
			}
			pageIndex++
		}
	}

	stateBuffer, err := batch.PageBuffer(pageIndex)
	if err != nil {
		return err
	}
	statePage, err := storeio.EncodeStateRootPage(stateBuffer[:storePageQuantum], next, fileEnd)
	if err != nil {
		return err
	}
	if err := batch.SetPage(pageIndex, int64(stateOffset), len(statePage)); err != nil {
		return err
	}
	pageIndex++
	if pageIndex != pageCount {
		return fmt.Errorf("%w: planned %d insert pages, encoded %d", storeio.ErrInvalidWrite, pageCount, pageIndex)
	}
	if err := batch.SetSuperblock(storeio.Superblock{
		StoreID: root.StoreID, Generation: generation, StateOffset: stateOffset,
		StateLength: storePageQuantum, StateChecksum: storeio.PageChecksum(statePage),
		FileEnd: fileEnd, PageSize: storePageQuantum,
	}); err != nil {
		return err
	}
	if err := batch.Publish(generation); err != nil {
		return err
	}
	owned = false
	clear(db.rows[:])
	closeErr := error(nil)
	if selected.existing {
		closeErr = oldLease.Close()
	}
	if err := db.committer.Wait(generation); err != nil {
		return errors.Join(err, closeErr)
	}
	db.publish(next, fileEnd)
	return closeErr
}
