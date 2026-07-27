package durable

import (
	"bytes"
	"math/bits"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// AppendIndexes appends the frozen exact-index catalog visible to this file
// snapshot. Collection indexes are complete from generation one and therefore
// always report Ready.
func (s *Snapshot) AppendIndexes(dst []store.IndexInfo) []store.IndexInfo {
	if s == nil || s.collection == nil || s.state == nil {
		return dst
	}
	for _, definition := range s.collection.options.Indexes {
		info := store.IndexInfo{
			Name: definition.Name, Kind: store.IndexExact, State: store.IndexReady,
			TotalChunks: s.state.root.LiveChunks, CoveredChunks: s.state.root.LiveChunks,
			ColumnCount: uint8(len(definition.Paths)),
		}
		copy(info.Columns[:], definition.Paths)
		dst = append(dst, info)
	}
	return dst
}

// AppendIndexMasks appends exact stable-slot masks for a frozen collection
// index. A collision-free routing certificate decides the complete stream
// without opening JSON. Missing, oversized, or collision-marked certificates
// fall back to exact document recheck.
func (s *Snapshot) AppendIndexMasks(dst []store.Mask, name string, values ...vibejson.Index) ([]store.Mask, error) {
	var workspace IndexWorkspace
	return s.AppendIndexMasksInto(dst, &workspace, name, values...)
}

// AppendIndexMasksInto is AppendIndexMasks with reusable transient storage.
// A tuple hash alone is never a final answer: the probe either verifies the
// stream's exact scalar/compound certificate or compares every candidate
// document. With sufficient dst and workspace capacity, a warmed cache-hit
// probe allocates nothing.
func (s *Snapshot) AppendIndexMasksInto(dst []store.Mask, workspace *IndexWorkspace, name string, values ...vibejson.Index) ([]store.Mask, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return dst, ErrClosed
	}
	if workspace == nil {
		var local IndexWorkspace
		workspace = &local
	}
	workspace.lastProbe = IndexProbeStats{}
	probe, err := s.prepareFileIndexProbe(workspace, name, values)
	if err != nil {
		return dst, err
	}
	if err := s.loadFileIndexPostings(workspace, probe, values, true); err != nil {
		return dst, err
	}
	for _, decision := range workspace.postings {
		posting := decision.mask
		workspace.lastProbe.CandidateRows += uint64(bits.OnesCount64(posting.Bits))
		workspace.lastProbe.CandidateChunks++
		if decision.flags&fileIndexProbeCertified != 0 {
			workspace.lastProbe.CertificateRows += uint64(bits.OnesCount64(posting.Bits))
			if decision.flags&fileIndexProbeCertificateMatch != 0 {
				workspace.lastProbe.MatchedRows += uint64(bits.OnesCount64(posting.Bits))
				dst = append(dst, store.Mask{Chunk: posting.Chunk, Bits: posting.Bits})
			}
			continue
		}
		workspace.lastProbe.DocumentRecheckRows += uint64(bits.OnesCount64(posting.Bits))
		documentRef, ok, lookupErr := storeio.LookupChunkTree(s.collection.cache, probe.state.chunkRoot, posting.Chunk, storeio.ChunkTreeBounds{
			FileEnd: probe.state.super.FileEnd, NextLogicalID: probe.state.root.NextLogicalID,
		})
		if lookupErr != nil {
			return dst, lookupErr
		}
		if !ok {
			return dst, storeio.ErrIndexDirectoryCorrupt
		}
		documentLease, acquireErr := s.collection.cache.Acquire(documentRef)
		if acquireErr != nil {
			return dst, acquireErr
		}
		documentPage, viewErr := admittedFileDocumentChunk(
			documentLease.Page(), documentRef, posting.Chunk,
		)
		if viewErr != nil {
			documentLease.Release()
			return dst, viewErr
		}
		verified := uint64(0)
		for bitsLeft := posting.Bits; bitsLeft != 0; bitsLeft &= bitsLeft - 1 {
			slot := uint8(bits.TrailingZeros64(bitsLeft))
			record, present := documentPage.lookup(slot)
			if !present {
				documentLease.Release()
				return dst, storeio.ErrIndexDirectoryCorrupt
			}
			workspace.document = workspace.document[:0]
			workspace.document, err = s.collection.appendFileDocumentValue(
				workspace.document, probe.state, documentPage, record.value,
				storeio.KeyLocation{Chunk: posting.Chunk, Slot: slot},
			)
			if err != nil {
				documentLease.Release()
				return dst, err
			}
			matches := true
			if probe.exact.N == 1 {
				var raw vibejson.RawValue
				var found bool
				var pointerErr error
				if record.value.grouped || record.value.value.Overflow == (storeio.PageRef{}) {
					raw, found, pointerErr = probe.exact.Paths[0].GetRawTrusted(workspace.document)
				} else {
					raw, found, pointerErr = probe.exact.Paths[0].GetRaw(workspace.document)
				}
				if pointerErr != nil {
					documentLease.Release()
					return dst, pointerErr
				}
				matches = found && fileIndexRawScalarEqual(raw, values[0].Root())
			} else {
				index, buildErr := buildIndexGrowing(
					&workspace.tape, workspace.document,
					s.collection.options.Collection.IndexOptions,
				)
				if buildErr != nil {
					documentLease.Release()
					return dst, buildErr
				}
				for column := 0; column < int(probe.exact.N); column++ {
					node, found, pointerErr := index.PointerCompiled(probe.exact.Paths[column])
					if pointerErr != nil || !found ||
						!node.Contains(values[column].Root()) ||
						!values[column].Root().Contains(node) {
						matches = false
						break
					}
				}
			}
			if matches {
				verified |= uint64(1) << slot
			}
		}
		documentLease.Release()
		if verified != 0 {
			workspace.lastProbe.MatchedRows += uint64(bits.OnesCount64(verified))
			dst = append(dst, store.Mask{Chunk: posting.Chunk, Bits: verified})
		}
	}
	return dst, nil
}

// buildIndexGrowing indexes src into the caller's retained tape, growing that
// tape on [document.ErrIndexFull] instead of sizing it first with
// [vibejson.RequiredIndexEntries].
//
// The count that sizing needs is a second complete validation pass over the
// same bytes, and both callers run once per document: the multi-column
// recheck above runs it per candidate row, and buildOldFileIndex runs it per
// updated document. Measured on the 245-byte benchmark document the pre-count
// costs 257 ns against BuildIndexOptions's 134.5 ns, so sizing exactly nearly
// tripled the price of the build it was sizing. Build-and-retry is the shape
// Collection.validateDocument and store.Segment.buildDoc already use, and for
// the same reason.
//
// A workspace's tape outlives the probe, so a retry is warmup rather than a
// per-document cost: the first documents a cold probe meets double the tape a
// few times, and every document after that builds once. The tape only ever
// grows, so a size outlier cannot make it thrash, and growth doubles and stops
// at the width no document can exceed — one entry per value and per object
// key, each spanning at least one distinct source byte, so len(src)+2 always
// suffice — which is what bounds the loop.
func buildIndexGrowing(tape *[]vibejson.IndexEntry, src []byte, options document.IndexOptions) (vibejson.Index, error) {
	return buildIndexGrowingFrom(tape, src, options, len(src)/8+8)
}

// buildIndexGrowingFrom is buildIndexGrowing with the cold-start width spelled
// out, the seam its growth test drives with a width too small for any real
// document.
//
// seed is deliberately an underestimate of a dense document rather than a
// bound on one: it is the same len/8+8 Collection.validateDocument opens with,
// and it is what keeps a retained tape near the widest document actually seen
// rather than near the widest one imaginable. Sizing to the theoretical
// maximum would leave every workspace holding several times the tape it needs
// for the rest of its life, which moves the cost rather than removing it.
func buildIndexGrowingFrom(
	tape *[]vibejson.IndexEntry, src []byte, options document.IndexOptions, seed int,
) (vibejson.Index, error) {
	need := seed
	limit := len(src) + 2
	for {
		if cap(*tape) < need {
			*tape = make([]vibejson.IndexEntry, need)
		}
		index, err := vibejson.BuildIndexOptions(src, (*tape)[:cap(*tape)], options)
		if err != document.ErrIndexFull || cap(*tape) >= limit {
			// At or above limit the tape cannot be why a build failed, so the
			// error is reported rather than retried; that is what keeps a
			// broken width invariant from spinning here forever.
			return index, err
		}
		need = min(2*cap(*tape), limit)
	}
}

// fileIndexRawScalarEqual is the collision verifier for a single-column exact
// index. The raw seeker has already validated the complete document and
// resolved duplicate keys with last-wins semantics. Comparing the borrowed
// scalar directly avoids constructing a full document tape while retaining
// the same exact value relation as mutual Node.Contains.
func fileIndexRawScalarEqual(raw vibejson.RawValue, needle vibejson.Node) bool {
	return fileIndexRawValuesEqual(raw, needle.Raw())
}

func fileIndexRawValuesEqual(left, right vibejson.RawValue) bool {
	if left.Kind() != right.Kind() {
		return false
	}
	switch left.Kind() {
	case document.Invalid:
		return false
	case document.Null:
		return true
	case document.Bool:
		leftValue, leftOK := left.Bool()
		rightValue, rightOK := right.Bool()
		return leftOK && rightOK && leftValue == rightValue
	case document.Number:
		leftNumber, leftOK := left.NumberBytes()
		rightNumber, rightOK := right.NumberBytes()
		return leftOK && rightOK && vibejson.JSONNumberEqual(leftNumber, rightNumber)
	case document.String:
		leftRaw := left.Bytes()
		rightRaw := right.Bytes()
		leftFlags := uint8(0)
		if bytes.IndexByte(leftRaw, '\\') >= 0 {
			leftFlags = vibejson.TapeFlagEscaped
		}
		rightFlags := uint8(0)
		if bytes.IndexByte(rightRaw, '\\') >= 0 {
			rightFlags = vibejson.TapeFlagEscaped
		}
		return vibejson.RawJSONStringEqual(leftRaw, leftFlags, rightRaw, rightFlags)
	default:
		return false
	}
}

// AppendIndexCandidateMasks appends hash-bounded stable-slot candidates
// without reopening documents. It may return false positives and must be
// followed by an exact predicate recheck; it never turns a non-match into a
// public query result by itself. This lane exists for query engines that will
// immediately parse every candidate and avoids reading each document twice.
func (s *Snapshot) AppendIndexCandidateMasks(dst []store.Mask, name string, values ...vibejson.Index) ([]store.Mask, error) {
	var workspace IndexWorkspace
	return s.AppendIndexCandidateMasksInto(dst, &workspace, name, values...)
}

// AppendIndexCandidateMasksInto is AppendIndexCandidateMasks with reusable
// directory storage. The returned masks are ordered, non-zero posting
// candidates, not exact answers.
func (s *Snapshot) AppendIndexCandidateMasksInto(dst []store.Mask, workspace *IndexWorkspace, name string, values ...vibejson.Index) ([]store.Mask, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return dst, ErrClosed
	}
	if workspace == nil {
		var local IndexWorkspace
		workspace = &local
	}
	workspace.lastProbe = IndexProbeStats{}
	probe, err := s.prepareFileIndexProbe(workspace, name, values)
	if err != nil {
		return dst, err
	}
	if err := s.loadFileIndexPostings(workspace, probe, nil, false); err != nil {
		return dst, err
	}
	for _, decision := range workspace.postings {
		posting := decision.mask
		workspace.lastProbe.CandidateRows += uint64(bits.OnesCount64(posting.Bits))
		workspace.lastProbe.CandidateChunks++
		dst = append(dst, store.Mask{Chunk: posting.Chunk, Bits: posting.Bits})
	}
	return dst, nil
}

type fileIndexProbe struct {
	state   *fileStoreState
	exact   *store.ExactIndex
	indexID uint32
	hash    uint64
}

type fileIndexProbePosting struct {
	mask  store.Mask
	flags uint8
}

const (
	fileIndexProbeCertified uint8 = 1 << iota
	fileIndexProbeCertificateMatch
)

func (s *Snapshot) prepareFileIndexProbe(workspace *IndexWorkspace, name string, values []vibejson.Index) (fileIndexProbe, error) {
	indexID, ok := s.collection.options.indexNameIDs[name]
	if !ok {
		return fileIndexProbe{}, store.ErrIndexNotFound
	}
	exact := s.collection.options.indexes[indexID]
	hash, err := fileIndexNeedleHash(exact, values)
	if err != nil {
		return fileIndexProbe{}, err
	}
	state := s.state
	probe := fileIndexProbe{state: state, exact: exact, indexID: indexID, hash: hash}
	workspace.probe.Entries = workspace.probe.Entries[:0]
	workspace.probe.Certificates = workspace.probe.Certificates[:0]
	workspace.probe.Leaves = 0
	if state.indexRoot == (storeio.PageRef{}) {
		return probe, nil
	}
	if uint64(state.root.LiveChunks) > uint64(^uint(0)>>1) {
		return fileIndexProbe{}, store.ErrTooLarge
	}
	if err := storeio.AppendIndexTreeHash(
		s.collection.cache, state.indexRoot, probe.indexID, hash,
		storeio.IndexTreeBounds{
			FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
			IndexHighWater: state.root.IndexCount,
		}, &workspace.probe, int(state.root.LiveChunks),
	); err != nil {
		return fileIndexProbe{}, err
	}
	workspace.lastProbe.PostingPages = workspace.probe.Leaves
	return probe, nil
}

// loadFileIndexPostings turns the routing entries copied out of the index
// tree into ordered probe decisions. The masks are now inline in those
// entries, so nothing further is read from disk here; the loop exists to keep
// the certificate decision in one place and to reject a leaf whose record
// contradicts the state root before its bits reach a caller.
func (s *Snapshot) loadFileIndexPostings(
	workspace *IndexWorkspace,
	probe fileIndexProbe,
	values []vibejson.Index,
	verifyCertificate bool,
) error {
	workspace.postings = workspace.postings[:0]
	live := fileStoreLiveMask(probe.state.root.ChunkDocuments)
	for _, entry := range workspace.probe.Entries {
		// A chunk beyond the high-water mark or a bit outside the chunk's live
		// slot range names a row that cannot exist. Left unchecked it would be
		// handed to the document recheck as a lookup that silently finds
		// nothing, or worse, be returned as a candidate row of some future
		// chunk. The group lane has always rejected both; the probe lane must
		// agree or the two answer differently from one leaf.
		if entry.Key.IndexID != probe.indexID || entry.Key.TupleHash != probe.hash ||
			entry.Bits == 0 || entry.Key.Chunk >= probe.state.root.ChunkHighWater ||
			entry.Bits&^live != 0 {
			return storeio.ErrIndexDirectoryCorrupt
		}
		flags := uint8(0)
		if verifyCertificate && entry.Flags&storeio.IndexEntryCollision == 0 &&
			entry.Cert.Length != 0 {
			end := int(entry.Cert.Offset) + int(entry.Cert.Length)
			certificate := workspace.probe.Certificates[entry.Cert.Offset:end:end]
			if !fileIndexCertificateValid(certificate, int(probe.exact.N)) {
				return storeio.ErrIndexDirectoryCorrupt
			}
			flags |= fileIndexProbeCertified
			if fileIndexCertificateMatches(certificate, values, int(probe.exact.N)) {
				flags |= fileIndexProbeCertificateMatch
			}
		}
		workspace.postings = append(workspace.postings, fileIndexProbePosting{
			mask:  store.Mask{Chunk: entry.Key.Chunk, Bits: entry.Bits},
			flags: flags,
		})
	}
	return nil
}

func fileIndexCertificateScalar(raw vibejson.RawValue) bool {
	switch raw.Kind() {
	case document.Null, document.Bool, document.Number, document.String:
		return true
	default:
		return false
	}
}

// AppendIndexMasks acquires a temporary snapshot and returns exact masks. Hot
// callers should retain a Snapshot and IndexWorkspace instead.
func (c *Collection) AppendIndexMasks(dst []store.Mask, name string, values ...vibejson.Index) ([]store.Mask, error) {
	snapshot, err := c.Snapshot()
	if err != nil {
		return dst, err
	}
	defer snapshot.Close()
	return snapshot.AppendIndexMasks(dst, name, values...)
}
