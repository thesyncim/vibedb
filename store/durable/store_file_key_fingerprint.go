package durable

import (
	"bytes"
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// fileFingerprintMatch owns the document leases used to prove that a hash
// candidate names the requested complete key. Keeping the admitted view and
// value beside the location lets the caller consume the document without
// opening the same cold extent a second time.
type fileFingerprintMatch struct {
	location    storeio.PageKeyLocation
	documentRef storeio.PageRef
	view        fileDocumentChunk
	document    storeio.PageLease
	columns     storeio.PageLease
	detached    bool
	value       fileDocumentValue
}

func (m *fileFingerprintMatch) Release() {
	if m == nil {
		return
	}
	if m.detached {
		m.columns.Release()
	}
	m.document.Release()
	m.detached = false
	m.view = fileDocumentChunk{}
	m.value = fileDocumentValue{}
	m.documentRef = storeio.PageRef{}
}

func (m *fileFingerprintMatch) keyLocation() storeio.KeyLocation {
	if m == nil {
		return storeio.KeyLocation{}
	}
	return storeio.KeyLocation{
		Chunk: m.location.Chunk, Slot: m.location.Slot,
	}
}

func filePageKeyTreeBounds(state *fileStoreState) storeio.PageKeyTreeBounds {
	return storeio.PageKeyTreeBounds{
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		ChunkHighWater: state.root.ChunkHighWater,
		ChunkDocuments: state.root.ChunkDocuments,
	}
}

// resolveFileFingerprint treats the fingerprint as a pruning hint only. Every
// candidate is resolved through the chunk tree and compared against the full
// key stored with the document. A directory entry that points at no live row is
// corruption rather than a miss: accepting it would allow the primary and
// document directories to diverge silently.
func (c *Collection) resolveFileFingerprint(
	state *fileStoreState, key []byte,
) (fileFingerprintMatch, bool, error) {
	if c == nil || state == nil || state.keyRoot == (storeio.PageRef{}) {
		return fileFingerprintMatch{}, false, nil
	}
	return c.resolveFileFingerprintCandidates(
		state, key, storeio.KeyHashBytes(state.root.StoreID, key), true,
	)
}

// resolveFileFingerprintHash is split out so collision tests can put several
// different complete keys in one explicitly chosen hash run. Production calls
// always enter through resolveFileFingerprint and derive the keyed hash from
// the durable StoreID.
func (c *Collection) resolveFileFingerprintHash(
	state *fileStoreState, key []byte, hash uint64,
) (fileFingerprintMatch, bool, error) {
	return c.resolveFileFingerprintCandidates(state, key, hash, false)
}

func (c *Collection) resolveFileFingerprintCandidates(
	state *fileStoreState, key []byte, hash uint64, verifyCandidateHash bool,
) (fileFingerprintMatch, bool, error) {
	location, ok, err := storeio.FirstPageKeyTreeCandidate(
		c.cache, state.keyRoot, hash, filePageKeyTreeBounds(state),
	)
	if err != nil || !ok {
		return fileFingerprintMatch{}, false, err
	}
	match, exact, err := c.resolveFileFingerprintLocation(
		state, key, location, verifyCandidateHash,
	)
	if err != nil || exact {
		return match, exact, err
	}

	// A complete-key mismatch is the exceptional hash-collision path. Reopen a
	// collision cursor only then; the ordinary hit avoids constructing and
	// retaining its sixteen-level continuation state.
	cursor, err := storeio.OpenPageKeyTreeCursor(
		c.cache, state.keyRoot, hash, filePageKeyTreeBounds(state),
	)
	if err != nil {
		return fileFingerprintMatch{}, false, err
	}
	defer cursor.Close()
	for {
		location, ok, nextErr := cursor.Next()
		if nextErr != nil || !ok {
			return fileFingerprintMatch{}, false, nextErr
		}
		match, exact, resolveErr := c.resolveFileFingerprintLocation(
			state, key, location, verifyCandidateHash,
		)
		if resolveErr != nil || exact {
			return match, exact, resolveErr
		}
	}
}

func (c *Collection) resolveFileFingerprintLocation(
	state *fileStoreState, key []byte, location storeio.PageKeyLocation,
	verifyCandidateHash bool,
) (fileFingerprintMatch, bool, error) {
	documentRef, found, err := storeio.LookupChunkTree(
		c.cache, state.chunkRoot, location.Chunk, storeio.ChunkTreeBounds{
			FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		},
	)
	if err != nil {
		return fileFingerprintMatch{}, false, err
	}
	if !found || documentRef == (storeio.PageRef{}) {
		return fileFingerprintMatch{}, false, fmt.Errorf(
			"%w: fingerprint candidate has no document chunk",
			storeio.ErrKeyDirectoryCorrupt,
		)
	}
	lease, err := c.cache.Acquire(documentRef)
	if err != nil {
		return fileFingerprintMatch{}, false, err
	}
	view, err := admittedFileDocumentChunk(
		lease.Page(), documentRef, location.Chunk,
	)
	if err != nil {
		lease.Release()
		return fileFingerprintMatch{}, false, err
	}
	record, occupied := view.lookup(location.Slot)
	if !occupied {
		lease.Release()
		return fileFingerprintMatch{}, false, fmt.Errorf(
			"%w: fingerprint candidate has no live document slot",
			storeio.ErrKeyDirectoryCorrupt,
		)
	}
	if !bytes.Equal(record.key, key) {
		if verifyCandidateHash &&
			storeio.KeyHashBytes(state.root.StoreID, record.key) != location.Hash {
			lease.Release()
			return fileFingerprintMatch{}, false, fmt.Errorf(
				"%w: fingerprint candidate points at a different hash",
				storeio.ErrKeyDirectoryCorrupt,
			)
		}
		lease.Release()
		return fileFingerprintMatch{}, false, nil
	}
	return fileFingerprintMatch{
		location: location, documentRef: documentRef,
		view: view, document: lease, value: record.value,
	}, true, nil
}

// attachColumns upgrades a point-read match to the complete mutable chunk
// view only when a grouped document actually carries a detached numeric
// sidecar. AppendRaw never calls it, preserving the one-document-read path.
func (m *fileFingerprintMatch) attachColumns(c *Collection) error {
	if m == nil || c == nil || m.documentRef == (storeio.PageRef{}) || m.detached {
		return nil
	}
	columnsRef, detached, err := storeio.DocumentGroupFloat64Sidecar(
		m.documentRef, uint32(c.options.PageSize),
	)
	if err != nil || !detached {
		return err
	}
	columns, err := c.cache.Acquire(columnsRef)
	if err != nil {
		return err
	}
	if err := m.view.attachFloat64Group(columns.Page()); err != nil {
		columns.Release()
		return err
	}
	m.columns = columns
	m.detached = true
	return nil
}

func filePageKeyLocation(
	storeID [16]byte, key []byte, location storeio.KeyLocation,
) storeio.PageKeyLocation {
	return storeio.PageKeyLocation{
		Hash:  storeio.KeyHashBytes(storeID, key),
		Chunk: location.Chunk, Slot: location.Slot,
	}
}
