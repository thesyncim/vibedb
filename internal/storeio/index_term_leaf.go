package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/bits"
	"sync"
	"unsafe"
)

// The exact-term leaf is an isolated format candidate. It proves compact,
// canonical term/posting storage and direct equality routes, but it is not yet
// part of the durable index tree. Its benchmarks intentionally retain the
// current packed-record lower bound: promotion requires closing the ordered
// traversal gap without giving back the measured space reduction.

const (
	// IndexTermLeafMaxBytes keeps every internal offset bounded to sixteen
	// bits. The codec is intentionally independent of today's physical page
	// size: a caller may place a leaf in one page or in a bounded extent.
	IndexTermLeafMaxBytes = 1<<16 - 1

	// Restarting every eight terms bounds an exact binary-search comparison to
	// eight suffix copies while retaining most adjacent-key prefix savings.
	IndexTermLeafRestartInterval = 8

	indexTermLeafHeaderBytes     = 32
	indexTermLeafDescriptorBytes = 12
	indexTermLeafDictionaryBytes = 8

	indexTermLeafVersion = byte(DevelopmentFormatVersion)

	indexTermLeafDirect1 = byte(iota)
	indexTermLeafDirectN
	indexTermLeafAdaptiveInline
	indexTermLeafAdaptiveDictionary
	indexTermLeafDirect1Contiguous
	indexTermLeafDirectN1Contiguous
	indexTermLeafDirectN2Contiguous
	indexTermLeafDirectN1SameChunk
	indexTermLeafDirect1SameRow
	indexTermLeafDirectN1SameMask

	indexTermLeafNoDirectBlock = byte(0xff)

	indexTermLeafFlagGlobalDirect1  = uint16(1)
	indexTermLeafFlagGlobalDirectN1 = uint16(2)
	indexTermLeafFlagGlobalDirectN2 = uint16(3)
	indexTermLeafFlagGlobalSameN1   = uint16(4)
	indexTermLeafFlagGlobalSameRow  = uint16(8)
	indexTermLeafFlagGlobalSameMask = uint16(16)
	indexTermLeafKnownFlags         = uint16(31)
)

var (
	indexTermLeafMagic              = [4]byte{'V', 'J', 'T', 'L'}
	indexTermLeafCRC                = crc32.MakeTable(crc32.Castagnoli)
	indexTermLeafNativeLittleEndian = func() bool {
		value := uint16(1)
		return *(*byte)(unsafe.Pointer(&value)) == 1
	}()

	// ErrIndexTermLeafCorrupt reports a checksum-valid but structurally or
	// semantically invalid exact-term leaf as well as checksum damage.
	ErrIndexTermLeafCorrupt = errors.New("vibedb: corrupt exact-term index leaf")
)

// IndexTermLeafLiveLookup resolves the immutable stable-slot liveness tile
// that was used to build a posting. It is retained by an admitted leaf view;
// callers must therefore keep both the function and returned masks valid for
// the view's lifetime.
type IndexTermLeafLiveLookup func(tileID uint32) *[TermPostingTileChunks]uint64

// IndexTermLeafPosting is one already canonical adaptive posting supplied to
// the leaf builder. Component is empty for inline TermPosting records and is
// the exact content-addressed payload for manifest records. Live is required
// so the builder admits the TermPosting before selecting a leaf-local direct
// representation.
type IndexTermLeafPosting struct {
	Posting   TermPosting
	Component []byte
	Live      *[TermPostingTileChunks]uint64
	// Chunk0Bits/Chunk0Only let a builder that just derived Posting from a
	// single chunk-0 mask — the ordered primary graph's whole posting
	// universe (slots are uint8: tile = bucket<<2 | slot>>6, bit = slot&63)
	// — hand that mask straight back, skipping the per-posting payload
	// re-decode and codec re-selection OpenTermPosting would repeat on bytes
	// BuildTermPosting produced in the same call chain. Foreign postings (device
	// reads, other chunks) leave Chunk0Only false and keep the full admission; a
	// mismatched caller mask is still caught by the built leaf's own admission,
	// which re-validates every posting against liveness. The measured reason for
	// preserving this handoff lives in docs/performance.md.
	Chunk0Bits uint64
	Chunk0Only bool
}

// IndexTermLeafTerm groups all non-empty tile postings for one exact canonical
// scalar tuple. Terms must be strictly ordered by Key.Canonical. Postings must
// be strictly ordered by TileID.
type IndexTermLeafTerm struct {
	Key      IndexTermKeyRecord
	Postings []IndexTermLeafPosting
}

// IndexTermLeafView is a fully admitted immutable exact-term leaf. It borrows
// encoded and the liveness masks returned by live for its lifetime.
type IndexTermLeafView struct {
	encoded          []byte
	storeID          [16]byte
	live             IndexTermLeafLiveLookup
	termCount        uint16
	postingCount     uint16
	descriptorAt     uint16
	equalityAt       uint16
	equalitySlots    uint16
	keyAt            uint16
	postingAt        uint16
	dictionaryAt     uint16
	dictionaryN      uint16
	dictionaryDataAt uint16
	flags            uint16
	globalPayloadAt  uint16
	globalBase       uint32
	globalStride     uint8
	globalChunk      uint8
	globalRow        uint16
	globalMask       uint64
}

// IndexTermLeafMatch is the posting aggregate for one exact term. Its iterator
// starts directly at the term's first posting; equality never scans an
// unrelated posting.
type IndexTermLeafMatch struct {
	leaf  IndexTermLeafView
	first uint16
	count uint16
}

// IndexTermLeafIterator streams distinct exact terms in canonical order. Key
// borrows iterator-owned scratch and is valid until the next Next call.
type IndexTermLeafIterator struct {
	leaf IndexTermLeafView
	next uint16
	end  uint16
	key  [IndexTermMaxKeyBytes]byte
}

// IndexTermLeafPostingIterator streams one exact term's tile postings.
type IndexTermLeafPostingIterator struct {
	leaf          IndexTermLeafView
	position      int
	remaining     uint16
	previousTile  uint32
	havePrevious  bool
	sequenceKind  byte
	sequenceTile  uint32
	sequenceChunk uint8
	sequenceRow   uint16
	sequenceMask  uint64
}

// IndexTermLeafAggregateIterator fuses tile-posting and mask traversal for
// equality probes. Direct1 returns after one trusted record decode and DirectN
// retains its remaining mask bytes in the cursor; neither constructs an
// intermediate posting view or TermPosting iterator.
type IndexTermLeafAggregateIterator struct {
	leaf          IndexTermLeafView
	position      int
	remaining     uint16
	previousTile  uint32
	havePrevious  bool
	pendingTile   uint32
	pending       []byte
	pendingAt     int
	adaptive      TermPostingIterator
	haveAdaptive  bool
	sequenceKind  byte
	sequenceTile  uint32
	sequenceChunk uint8
	sequenceRow   uint16
	sequenceMask  uint64
}

// IndexTermLeafPostingView is one trusted tile posting obtained from an
// admitted leaf. Direct encodings avoid reconstructing or validating a
// TermPosting on the hot path; adaptive encodings reuse TermPosting's iterator.
type IndexTermLeafPostingView struct {
	tileID      uint32
	rows        uint16
	kind        byte
	direct      []byte
	directChunk uint8
	adaptive    TermPosting
	component   []byte
	live        *[TermPostingTileChunks]uint64
}

// IndexTermLeafMaskIterator streams ordered non-empty 64-row masks.
type IndexTermLeafMaskIterator struct {
	kind     byte
	direct   []byte
	position int
	one      TermPostingMask
	haveOne  bool
	adaptive TermPostingIterator
}

// IndexTermLeafDirectBlockView is a contiguous tile run whose postings all
// have the same direct physical shape.
type IndexTermLeafDirectBlockView struct {
	payload []byte
	base    uint32
	count   uint16
	kind    byte
	chunk   uint8
	row     uint16
	mask    uint64
}

// IndexTermLeafDirectBlockIterator stays separate from the adaptive union
// cursor so its fixed-format Next method remains small enough to inline.
type IndexTermLeafDirectBlockIterator struct {
	payload   []byte
	tile      uint32
	remaining uint16
	kind      byte
	chunk     uint8
	row       uint16
	mask      uint64
	second    bool
}

type indexTermLeafDerivedPosting struct {
	tileID uint32
	rows   uint16
	kind   byte
	codec  TermPostingCodec
	// payload borrows the input posting's inline array or component for the
	// duration of one build. dictionaryEligible marks payloads long enough
	// to be dictionary candidates; payloadHash keys the builder's
	// use-counting table so no per-posting identity string is materialized.
	payload            []byte
	direct             [2]TermPostingMask
	directCount        uint8
	dictionaryEligible bool
	payloadHash        uint64
	dictionary         uint16
}

type indexTermLeafDerivedTerm struct {
	key          []byte
	shared       uint16
	suffixAt     uint16
	firstPosting uint16
	postings     []indexTermLeafDerivedPosting
	directBlock  byte
}

type indexTermLeafDictionary struct {
	codec   TermPostingCodec
	rows    uint16
	payload []byte
}

type indexTermLeafDecodedPosting struct {
	kind       byte
	tileID     uint32
	rows       uint16
	codec      TermPostingCodec
	payload    []byte
	dictionary uint16
	direct     []byte
	next       int
}

// AppendIndexTermLeaf appends one deterministic exact-term leaf. Keys are
// prefix-compressed once per distinct term. The descriptor's first/count pair
// is the leaf-local posting aggregate used by equality. Repeated adaptive
// payloads larger than TermPostingInlineBytes enter a leaf dictionary iff
// (uses-1)*payloadBytes exceeds the eight-byte dictionary descriptor.
//
// On failure dst is returned unchanged.
func AppendIndexTermLeaf(
	dst []byte,
	storeID [16]byte,
	terms []IndexTermLeafTerm,
) ([]byte, error) {
	original := dst
	if len(terms) == 0 || len(terms) > int(^uint16(0)) {
		return original, fmt.Errorf("%w: exact-term leaf term count", ErrInvalidWrite)
	}

	// The builder runs once per bulk build AND once per exact-index
	// checkpoint fold, so its working set comes from a pooled scratch and
	// the derived postings live in one flat slab. This avoids per-call map,
	// string, and per-term slice churn on dirty indexed checkpoints; measured
	// allocation provenance lives in docs/performance.md.
	totalPostings := 0
	for i := range terms {
		if totalPostings > int(^uint16(0))-len(terms[i].Postings) {
			return original, fmt.Errorf("%w: exact-term leaf posting count", ErrInvalidWrite)
		}
		totalPostings += len(terms[i].Postings)
	}
	scratch := indexTermLeafBuilderPool.Get().(*indexTermLeafBuilderScratch)
	defer scratch.release()
	derived := scratch.derivedTerms(len(terms))
	postingSlab := scratch.derivedPostings(totalPostings)
	scratch.resetUses(totalPostings)

	keyBytes := 0
	slabAt := 0
	for i := range terms {
		term := &terms[i]
		key := term.Key.Canonical
		if !ValidIndexTermKey(key) ||
			term.Key.RouteHash != IndexTermRouteHash(storeID, key) ||
			i != 0 && bytes.Compare(terms[i-1].Key.Canonical, key) >= 0 ||
			len(term.Postings) == 0 {
			return original, fmt.Errorf("%w: exact-term leaf key or order", ErrInvalidWrite)
		}
		shared := 0
		if i%IndexTermLeafRestartInterval != 0 {
			restart := i - i%IndexTermLeafRestartInterval
			shared = commonIndexTermLeafPrefix(
				terms[restart].Key.Canonical, key,
			)
		}
		derived[i] = indexTermLeafDerivedTerm{
			key: key, shared: uint16(shared),
			postings:    postingSlab[slabAt : slabAt+len(term.Postings) : slabAt+len(term.Postings)],
			directBlock: indexTermLeafNoDirectBlock,
		}
		keyBytes += len(key) - shared

		var previousTile uint32
		for j := range term.Postings {
			input := &term.Postings[j]
			if input.Live == nil ||
				j != 0 && input.Posting.TileID <= previousTile ||
				input.Posting.Rows == 0 {
				return original, fmt.Errorf("%w: exact-term leaf posting order", ErrInvalidWrite)
			}
			if input.Chunk0Only {
				// The caller derived this record from a single chunk-0 mask
				// in this same call chain (the primary graph's whole posting
				// universe); re-decoding, re-selecting the codec, and
				// scanning all 64 chunks here would repeat work whose
				// outcome is fixed — a single-chunk posting always lands on
				// a direct representation. The row-count cross-check plus
				// the built leaf's admission keep a mismatched mask
				// fail-closed. The specialized derive also skips the
				// per-posting 512-byte mask-array zeroing, which was a
				// measured slice of the checkpoint fold at the 10k shape.
				if input.Posting.Rows != uint16(bits.OnesCount64(input.Chunk0Bits)) {
					return original, fmt.Errorf("%w: exact-term leaf posting masks", ErrInvalidWrite)
				}
				derived[i].postings[j] = deriveChunk0IndexTermLeafPosting(
					input.Posting.TileID, input.Posting.Rows, input.Chunk0Bits,
				)
			} else {
				var masks [TermPostingTileChunks]uint64
				view, err := OpenTermPosting(input.Posting, input.Component, input.Live)
				if err != nil {
					return original, fmt.Errorf("%w: exact-term leaf posting: %v", ErrInvalidWrite, err)
				}
				if !view.MasksInto(&masks) {
					return original, fmt.Errorf("%w: exact-term leaf posting masks", ErrInvalidWrite)
				}
				posting := deriveIndexTermLeafPosting(input.Posting, input.Component, &masks)
				derived[i].postings[j] = posting
				if posting.dictionaryEligible {
					scratch.countUse(&derived[i].postings[j], int32(slabAt+j))
				}
			}
			previousTile = input.Posting.TileID
		}
		slabAt += len(term.Postings)
		derived[i].directBlock = selectIndexTermLeafDirectBlock(
			derived[i].postings,
		)
	}
	globalDirect := selectIndexTermLeafGlobalDirect(derived)

	dictionaries := scratch.dictionaries[:0]
	postingBytes := 0
	for i := range derived {
		if globalDirect != indexTermLeafNoDirectBlock {
			if i == 0 {
				postingBytes = indexTermLeafGlobalDirectBytes(
					globalDirect, derived,
				)
			}
			continue
		}
		if derived[i].directBlock != indexTermLeafNoDirectBlock {
			postingBytes += indexTermLeafDirectBlockBytes(
				derived[i].directBlock, derived[i].postings,
			)
			continue
		}
		var previousTile uint32
		for j := range derived[i].postings {
			posting := &derived[i].postings[j]
			if posting.dictionaryEligible {
				use := scratch.lookupUse(posting)
				if int(use.uses-1)*len(posting.payload) >
					indexTermLeafDictionaryBytes {
					if !use.assigned {
						use.assigned = true
						use.dictionary = uint16(len(dictionaries))
						dictionaries = append(dictionaries, indexTermLeafDictionary{
							codec: posting.codec, rows: posting.rows, payload: posting.payload,
						})
					}
					posting.kind = indexTermLeafAdaptiveDictionary
					posting.dictionary = use.dictionary
				}
			}
			delta := uint64(posting.tileID)
			if j != 0 {
				delta = uint64(posting.tileID - previousTile - 1)
			}
			postingBytes += indexTermLeafPostingBytes(posting, delta)
			previousTile = posting.tileID
		}
	}
	scratch.dictionaries = dictionaries

	dictionaryPayloadBytes := 0
	for i := range dictionaries {
		dictionaryPayloadBytes += len(dictionaries[i].payload)
	}
	descriptorAt := indexTermLeafHeaderBytes
	equalityAt := descriptorAt + len(derived)*indexTermLeafDescriptorBytes
	equalitySlots := indexTermLeafEqualitySlots(len(derived))
	keyAt := equalityAt + equalitySlots*2
	postingAt := keyAt + keyBytes
	if globalDirect == indexTermLeafDirectN1SameChunk {
		prefixBytes := 1 + indexTermLeafUvarintBytes(
			uint64(derived[0].postings[0].tileID),
		) + 1
		postingBytes += indexTermLeafAlign8Padding(postingAt + prefixBytes)
	}
	dictionaryAt := postingAt + postingBytes
	dictionaryDataAt := dictionaryAt + len(dictionaries)*indexTermLeafDictionaryBytes
	totalBytes := dictionaryDataAt + dictionaryPayloadBytes
	if totalBytes > IndexTermLeafMaxBytes {
		return original, fmt.Errorf("%w: exact-term leaf size", ErrInvalidWrite)
	}

	// Carve the output straight out of dst (zeroed: alignment padding and
	// the equality table's untouched tails are admitted as all-zero), so a
	// caller reusing its buffer pays no intermediate 64 KiB copy per build.
	dstLen := len(dst)
	if cap(dst)-dstLen < totalBytes {
		grown := make([]byte, dstLen, dstLen+totalBytes)
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:dstLen+totalBytes]
	encoded := dst[dstLen:]
	clear(encoded)
	copy(encoded[0:4], indexTermLeafMagic[:])
	encoded[4] = indexTermLeafVersion
	encoded[5] = IndexTermLeafRestartInterval
	binary.LittleEndian.PutUint16(
		encoded[6:8], indexTermLeafGlobalDirectFlag(globalDirect),
	)
	binary.LittleEndian.PutUint32(encoded[8:12], uint32(totalBytes))
	binary.LittleEndian.PutUint16(encoded[12:14], uint16(len(derived)))
	binary.LittleEndian.PutUint16(encoded[14:16], uint16(totalPostings))
	binary.LittleEndian.PutUint16(encoded[16:18], uint16(descriptorAt))
	binary.LittleEndian.PutUint16(encoded[18:20], uint16(keyAt))
	binary.LittleEndian.PutUint16(encoded[20:22], uint16(postingAt))
	binary.LittleEndian.PutUint16(encoded[22:24], uint16(dictionaryAt))
	binary.LittleEndian.PutUint16(encoded[24:26], uint16(len(dictionaries)))
	binary.LittleEndian.PutUint16(encoded[26:28], uint16(dictionaryDataAt))
	for position := equalityAt; position < keyAt; position += 2 {
		binary.LittleEndian.PutUint16(encoded[position:position+2], ^uint16(0))
	}
	for i := range terms {
		slot := int(terms[i].Key.RouteHash & uint64(equalitySlots-1))
		for binary.LittleEndian.Uint16(
			encoded[equalityAt+slot*2:equalityAt+slot*2+2],
		) != ^uint16(0) {
			slot = (slot + 1) & (equalitySlots - 1)
		}
		binary.LittleEndian.PutUint16(
			encoded[equalityAt+slot*2:equalityAt+slot*2+2], uint16(i),
		)
	}

	keyPosition := keyAt
	postingPosition := postingAt
	if globalDirect != indexTermLeafNoDirectBlock {
		postingPosition = appendIndexTermLeafGlobalDirect(
			encoded, postingPosition, globalDirect, derived,
		)
	}
	for i := range derived {
		term := &derived[i]
		term.suffixAt = uint16(keyPosition - keyAt)
		if globalDirect != indexTermLeafNoDirectBlock {
			term.firstPosting = uint16(i)
		} else {
			term.firstPosting = uint16(postingPosition - postingAt)
		}
		suffix := term.key[int(term.shared):]
		copy(encoded[keyPosition:], suffix)
		keyPosition += len(suffix)

		if globalDirect != indexTermLeafNoDirectBlock {
			// The leaf-wide direct column was emitted once above.
		} else if term.directBlock != indexTermLeafNoDirectBlock {
			postingPosition = appendIndexTermLeafDirectBlock(
				encoded, postingPosition, term.directBlock, term.postings,
			)
		} else {
			var previousTile uint32
			for j := range term.postings {
				posting := &term.postings[j]
				delta := uint64(posting.tileID)
				if j != 0 {
					delta = uint64(posting.tileID - previousTile - 1)
				}
				postingPosition = appendIndexTermLeafPosting(
					encoded, postingPosition, posting, delta,
				)
				previousTile = posting.tileID
			}
		}
		record := encoded[descriptorAt+i*indexTermLeafDescriptorBytes:]
		binary.LittleEndian.PutUint16(record[0:2], term.suffixAt)
		binary.LittleEndian.PutUint16(record[2:4], term.firstPosting)
		binary.LittleEndian.PutUint16(record[4:6], uint16(len(term.key)))
		binary.LittleEndian.PutUint16(record[6:8], term.shared)
		binary.LittleEndian.PutUint16(record[8:10], uint16(len(suffix)))
		binary.LittleEndian.PutUint16(record[10:12], uint16(len(term.postings)))
	}

	dictionaryPosition := dictionaryDataAt
	for i := range dictionaries {
		dictionary := &dictionaries[i]
		record := encoded[dictionaryAt+i*indexTermLeafDictionaryBytes:]
		binary.LittleEndian.PutUint16(record[0:2], uint16(dictionaryPosition-dictionaryDataAt))
		binary.LittleEndian.PutUint16(record[2:4], uint16(len(dictionary.payload)))
		binary.LittleEndian.PutUint16(record[4:6], dictionary.rows)
		record[6] = byte(dictionary.codec)
		copy(encoded[dictionaryPosition:], dictionary.payload)
		dictionaryPosition += len(dictionary.payload)
	}
	binary.LittleEndian.PutUint32(encoded[28:32], indexTermLeafChecksum(encoded))
	return dst, nil
}

// indexTermLeafBuilderScratch is AppendIndexTermLeaf's pooled working set:
// the derived term/posting slabs, the dictionary accumulator, and the
// open-addressed payload-identity table that replaced the per-posting string
// keyed maps. release clears every borrowed reference (payload and key
// slices point into caller storage) before returning to the pool.
type indexTermLeafBuilderScratch struct {
	derived      []indexTermLeafDerivedTerm
	postings     []indexTermLeafDerivedPosting
	dictionaries []indexTermLeafDictionary
	uses         []indexTermLeafPayloadUse
	useTable     []int32
}

type indexTermLeafPayloadUse struct {
	hash       uint64
	first      int32
	uses       int32
	dictionary uint16
	assigned   bool
}

var indexTermLeafBuilderPool = sync.Pool{
	New: func() any { return &indexTermLeafBuilderScratch{} },
}

func (s *indexTermLeafBuilderScratch) derivedTerms(
	n int,
) []indexTermLeafDerivedTerm {
	if cap(s.derived) < n {
		s.derived = make([]indexTermLeafDerivedTerm, n)
	}
	s.derived = s.derived[:n]
	return s.derived
}

func (s *indexTermLeafBuilderScratch) derivedPostings(
	n int,
) []indexTermLeafDerivedPosting {
	if cap(s.postings) < n {
		s.postings = make([]indexTermLeafDerivedPosting, n)
	}
	s.postings = s.postings[:n]
	return s.postings
}

func (s *indexTermLeafBuilderScratch) resetUses(totalPostings int) {
	size := 8
	for size < 2*totalPostings {
		size <<= 1
	}
	if cap(s.useTable) < size {
		s.useTable = make([]int32, size)
	}
	s.useTable = s.useTable[:size]
	for i := range s.useTable {
		s.useTable[i] = -1
	}
	s.uses = s.uses[:0]
}

// countUse records one dictionary-eligible payload occurrence, resolving
// hash collisions by full identity comparison against the representative
// posting already in the slab.
func (s *indexTermLeafBuilderScratch) countUse(
	posting *indexTermLeafDerivedPosting, slabIndex int32,
) {
	mask := uint64(len(s.useTable) - 1)
	for at := posting.payloadHash & mask; ; at = (at + 1) & mask {
		index := s.useTable[at]
		if index < 0 {
			s.useTable[at] = int32(len(s.uses))
			s.uses = append(s.uses, indexTermLeafPayloadUse{
				hash: posting.payloadHash, first: slabIndex, uses: 1,
			})
			return
		}
		use := &s.uses[index]
		if use.hash == posting.payloadHash &&
			indexTermLeafPayloadIdentityEqual(
				&s.postings[use.first], posting,
			) {
			use.uses++
			return
		}
	}
}

func (s *indexTermLeafBuilderScratch) lookupUse(
	posting *indexTermLeafDerivedPosting,
) *indexTermLeafPayloadUse {
	mask := uint64(len(s.useTable) - 1)
	for at := posting.payloadHash & mask; ; at = (at + 1) & mask {
		index := s.useTable[at]
		if index < 0 {
			// Unreachable by construction: every eligible posting was
			// counted in the first pass.
			return &indexTermLeafPayloadUse{}
		}
		use := &s.uses[index]
		if use.hash == posting.payloadHash &&
			indexTermLeafPayloadIdentityEqual(
				&s.postings[use.first], posting,
			) {
			return use
		}
	}
}

func (s *indexTermLeafBuilderScratch) release() {
	clear(s.derived[:cap(s.derived)])
	clear(s.postings[:cap(s.postings)])
	clear(s.dictionaries[:cap(s.dictionaries)])
	s.dictionaries = s.dictionaries[:0]
	indexTermLeafBuilderPool.Put(s)
}

// OpenIndexTermLeaf validates checksum, exact section layout, restart-front
// coding, canonical keys and ordering, posting order and adaptive codec,
// live-slot containment, and profitable deterministic dictionary selection.
// It allocates nothing on success.
func OpenIndexTermLeaf(
	encoded []byte,
	storeID [16]byte,
	live IndexTermLeafLiveLookup,
) (IndexTermLeafView, error) {
	if len(encoded) < indexTermLeafHeaderBytes ||
		len(encoded) > IndexTermLeafMaxBytes ||
		!bytes.Equal(encoded[0:4], indexTermLeafMagic[:]) ||
		encoded[4] != indexTermLeafVersion ||
		encoded[5] != IndexTermLeafRestartInterval ||
		binary.LittleEndian.Uint16(encoded[6:8])&^indexTermLeafKnownFlags != 0 ||
		int(binary.LittleEndian.Uint32(encoded[8:12])) != len(encoded) ||
		binary.LittleEndian.Uint32(encoded[28:32]) != indexTermLeafChecksum(encoded) {
		return IndexTermLeafView{}, indexTermLeafCorrupt("header or checksum")
	}
	termCount := binary.LittleEndian.Uint16(encoded[12:14])
	postingCount := binary.LittleEndian.Uint16(encoded[14:16])
	descriptorAt := binary.LittleEndian.Uint16(encoded[16:18])
	keyAt := binary.LittleEndian.Uint16(encoded[18:20])
	postingAt := binary.LittleEndian.Uint16(encoded[20:22])
	dictionaryAt := binary.LittleEndian.Uint16(encoded[22:24])
	dictionaryN := binary.LittleEndian.Uint16(encoded[24:26])
	dictionaryDataAt := binary.LittleEndian.Uint16(encoded[26:28])
	flags := binary.LittleEndian.Uint16(encoded[6:8])
	equalityAt := uint32(descriptorAt) +
		uint32(termCount)*indexTermLeafDescriptorBytes
	equalitySlots := indexTermLeafEqualitySlots(int(termCount))
	if termCount == 0 || postingCount < termCount || live == nil ||
		descriptorAt != indexTermLeafHeaderBytes ||
		uint32(keyAt) != equalityAt+uint32(equalitySlots)*2 ||
		keyAt > postingAt || postingAt > dictionaryAt ||
		uint32(dictionaryDataAt) != uint32(dictionaryAt)+
			uint32(dictionaryN)*indexTermLeafDictionaryBytes ||
		int(dictionaryDataAt) > len(encoded) {
		return IndexTermLeafView{}, indexTermLeafCorrupt("section layout")
	}
	view := IndexTermLeafView{
		encoded: encoded, storeID: storeID, live: live, termCount: termCount,
		postingCount: postingCount, descriptorAt: descriptorAt,
		equalityAt: uint16(equalityAt), equalitySlots: uint16(equalitySlots),
		keyAt: keyAt, postingAt: postingAt, dictionaryAt: dictionaryAt,
		dictionaryN: dictionaryN, dictionaryDataAt: dictionaryDataAt,
		flags: flags,
	}
	if flags != 0 {
		if postingAt >= dictionaryAt {
			return IndexTermLeafView{}, indexTermLeafCorrupt(
				"global direct extent",
			)
		}
		base, n := canonicalTermPostingUvarint(
			encoded[int(postingAt)+1 : int(dictionaryAt)],
		)
		if n <= 0 || base > uint64(^uint32(0)) {
			return IndexTermLeafView{}, indexTermLeafCorrupt(
				"global direct base",
			)
		}
		view.globalPayloadAt = uint16(int(postingAt) + 1 + n)
		view.globalBase = uint32(base)
		view.globalStride = 2
		switch indexTermLeafGlobalDirectKind(flags) {
		case indexTermLeafDirectN1Contiguous:
			view.globalStride = 9
		case indexTermLeafDirectN2Contiguous:
			view.globalStride = 18
		case indexTermLeafDirectN1SameChunk:
			if view.globalPayloadAt >= dictionaryAt {
				return IndexTermLeafView{}, indexTermLeafCorrupt(
					"global direct chunk",
				)
			}
			view.globalChunk = encoded[view.globalPayloadAt]
			view.globalPayloadAt++
			padding := indexTermLeafAlign8Padding(int(view.globalPayloadAt))
			if int(view.globalPayloadAt)+padding > int(dictionaryAt) {
				return IndexTermLeafView{}, indexTermLeafCorrupt(
					"global direct alignment",
				)
			}
			view.globalPayloadAt += uint16(padding)
			view.globalStride = 8
		case indexTermLeafDirect1SameRow:
			if int(view.globalPayloadAt)+2 > int(dictionaryAt) {
				return IndexTermLeafView{}, indexTermLeafCorrupt(
					"global direct row",
				)
			}
			view.globalRow = binary.LittleEndian.Uint16(
				encoded[view.globalPayloadAt : view.globalPayloadAt+2],
			)
			view.globalStride = 0
		case indexTermLeafDirectN1SameMask:
			if int(view.globalPayloadAt)+9 > int(dictionaryAt) {
				return IndexTermLeafView{}, indexTermLeafCorrupt(
					"global direct mask",
				)
			}
			view.globalChunk = encoded[view.globalPayloadAt]
			view.globalMask = binary.LittleEndian.Uint64(
				encoded[view.globalPayloadAt+1 : view.globalPayloadAt+9],
			)
			view.globalStride = 0
		}
	}
	if err := view.admitDictionaries(); err != nil {
		return IndexTermLeafView{}, err
	}
	if err := view.admitTermsAndPostings(); err != nil {
		return IndexTermLeafView{}, err
	}
	if err := view.admitEqualityTable(); err != nil {
		return IndexTermLeafView{}, err
	}
	if err := view.admitCanonicalDictionaryChoice(); err != nil {
		return IndexTermLeafView{}, err
	}
	return view, nil
}

// WithLive returns a copy of an already admitted view bound to a different
// liveness lookup. It re-validates nothing: the caller asserts that every
// posting in the leaf is still contained by the new lookup's masks at its
// generation. The spanned checkpoint fold carries untouched leaves across
// index epochs with exactly this call — an untouched leaf's terms received no
// records in the fold window, so no bit its postings reference was cleared
// (a delete or value move always records the affected term, dirtying its
// leaf), and the probe's per-mask liveness recheck still fails closed if the
// assertion is ever violated.
func (v IndexTermLeafView) WithLive(
	live IndexTermLeafLiveLookup,
) IndexTermLeafView {
	v.live = live
	return v
}

func (v IndexTermLeafView) Len() int           { return int(v.termCount) }
func (v IndexTermLeafView) PostingLen() int    { return int(v.postingCount) }
func (v IndexTermLeafView) DictionaryLen() int { return int(v.dictionaryN) }
func (v IndexTermLeafView) EncodedBytes() int  { return len(v.encoded) }

// Lookup returns the exact term's first/count posting aggregate. Its internally
// computed route selects candidates only; complete canonical bytes remain the
// proof of identity, so a collision cannot produce a false match.
func (v IndexTermLeafView) Lookup(canonical []byte) (IndexTermLeafMatch, bool) {
	return v.LookupRecord(IndexTermKeyRecord{
		RouteHash: IndexTermRouteHash(v.storeID, canonical),
		Canonical: canonical,
	})
}

// LookupRecord reuses a query planner's already computed StoreID-keyed route.
// The compact table selects candidates only; complete canonical bytes remain
// the proof of identity.
func (v IndexTermLeafView) LookupRecord(
	key IndexTermKeyRecord,
) (IndexTermLeafMatch, bool) {
	index, ok := v.lookupRecordIndex(key)
	if !ok {
		return IndexTermLeafMatch{}, false
	}
	return v.matchAt(index), true
}

// LookupDirectBlock combines exact lookup with selection of the compact
// contiguous direct representation.
func (v IndexTermLeafView) LookupDirectBlock(
	key IndexTermKeyRecord,
) (IndexTermLeafDirectBlockView, bool) {
	index, ok := v.lookupRecordIndex(key)
	if !ok {
		return IndexTermLeafDirectBlockView{}, false
	}
	if v.flags != 0 {
		position := int(v.globalPayloadAt) + index*int(v.globalStride)
		end := position + int(v.globalStride)
		return IndexTermLeafDirectBlockView{
			payload: v.encoded[position:end:end],
			base:    v.globalBase + uint32(index),
			count:   1,
			kind:    indexTermLeafGlobalDirectKind(v.flags),
			chunk:   v.globalChunk,
			row:     v.globalRow,
			mask:    v.globalMask,
		}, true
	}
	return v.directBlockAt(index)
}

// LookupGlobalMask is the fused exact point path for a uniform one-posting
// singleton or one-mask leaf.
func (v *IndexTermLeafView) LookupGlobalMask(
	key IndexTermKeyRecord,
) (tileID uint32, mask TermPostingMask, ok bool) {
	if v.flags != indexTermLeafFlagGlobalDirect1 &&
		v.flags != indexTermLeafFlagGlobalDirectN1 &&
		v.flags != indexTermLeafFlagGlobalSameN1 &&
		v.flags != indexTermLeafFlagGlobalSameRow &&
		v.flags != indexTermLeafFlagGlobalSameMask {
		return 0, TermPostingMask{}, false
	}
	slot := int(key.RouteHash & uint64(v.equalitySlots-1))
	index16 := binary.LittleEndian.Uint16(
		v.encoded[int(v.equalityAt)+slot*2 : int(v.equalityAt)+slot*2+2],
	)
	if index16 == ^uint16(0) {
		return 0, TermPostingMask{}, false
	}
	index := int(index16)
	if !v.exactKeyAt(index, key.Canonical) {
		var ok bool
		index, ok = v.lookupRecordIndexAfter(key, slot)
		if !ok {
			return 0, TermPostingMask{}, false
		}
	}
	position := int(v.globalPayloadAt) + index*int(v.globalStride)
	tileID = v.globalBase + uint32(index)
	if v.flags == indexTermLeafFlagGlobalSameRow {
		row := int(v.globalRow)
		return tileID, TermPostingMask{
			Chunk: uint8(row >> 6), Bits: uint64(1) << uint(row&63),
		}, true
	}
	if v.flags == indexTermLeafFlagGlobalSameMask {
		return tileID, TermPostingMask{
			Chunk: v.globalChunk, Bits: v.globalMask,
		}, true
	}
	if v.flags == indexTermLeafFlagGlobalDirect1 {
		row := int(binary.LittleEndian.Uint16(
			v.encoded[position : position+2],
		))
		return tileID, TermPostingMask{
			Chunk: uint8(row >> 6), Bits: uint64(1) << uint(row&63),
		}, true
	}
	if v.flags == indexTermLeafFlagGlobalSameN1 {
		return tileID, TermPostingMask{
			Chunk: v.globalChunk,
			Bits: binary.LittleEndian.Uint64(
				v.encoded[position : position+8],
			),
		}, true
	}
	return tileID, TermPostingMask{
		Chunk: v.encoded[position],
		Bits: binary.LittleEndian.Uint64(
			v.encoded[position+1 : position+9],
		),
	}, true
}

// lookupRecordIndexAfter continues after a first-slot collision. The common
// hit path is fused into LookupGlobalMask so it does not pay another call.
func (v *IndexTermLeafView) lookupRecordIndexAfter(
	key IndexTermKeyRecord,
	slot int,
) (int, bool) {
	for {
		slot = (slot + 1) & (int(v.equalitySlots) - 1)
		index := binary.LittleEndian.Uint16(
			v.encoded[int(v.equalityAt)+slot*2 : int(v.equalityAt)+slot*2+2],
		)
		if index == ^uint16(0) {
			return 0, false
		}
		if v.exactKeyAt(int(index), key.Canonical) {
			return int(index), true
		}
	}
}

func (v IndexTermLeafView) lookupRecordIndex(
	key IndexTermKeyRecord,
) (int, bool) {
	if v.equalitySlots == 0 {
		return 0, false
	}
	slot := int(key.RouteHash & uint64(v.equalitySlots-1))
	for {
		index := binary.LittleEndian.Uint16(v.encoded[int(v.equalityAt)+slot*2 : int(v.equalityAt)+slot*2+2])
		if index == ^uint16(0) {
			return 0, false
		}
		if v.exactKeyAt(int(index), key.Canonical) {
			return int(index), true
		}
		slot = (slot + 1) & (int(v.equalitySlots) - 1)
	}
}

// Range returns [lower, upper) in canonical key order. A nil bound is open.
func (v IndexTermLeafView) Range(lower, upper []byte) IndexTermLeafIterator {
	start, end := 0, int(v.termCount)
	if lower != nil {
		start = v.lowerBound(lower)
	}
	if upper != nil {
		end = v.lowerBound(upper)
	}
	if end < start {
		end = start
	}
	return IndexTermLeafIterator{leaf: v, next: uint16(start), end: uint16(end)}
}

func (v IndexTermLeafView) Ordered() IndexTermLeafIterator {
	return IndexTermLeafIterator{leaf: v, end: v.termCount}
}

func (it *IndexTermLeafIterator) Next() (
	key []byte,
	match IndexTermLeafMatch,
	ok bool,
) {
	if it == nil || it.next >= it.end {
		return nil, IndexTermLeafMatch{}, false
	}
	index := int(it.next)
	key, ok = it.leaf.reconstructKey(index, it.key[:0])
	if !ok {
		return nil, IndexTermLeafMatch{}, false
	}
	match = it.leaf.matchAt(index)
	it.next++
	return key, match, true
}

func (m IndexTermLeafMatch) Len() int { return int(m.count) }

func (m IndexTermLeafMatch) Iterator() IndexTermLeafPostingIterator {
	if m.leaf.flags != 0 {
		block, position, ok := m.leaf.globalDirectElementAt(int(m.first))
		if ok {
			return IndexTermLeafPostingIterator{
				leaf: m.leaf, position: position, remaining: 1,
				sequenceKind: block.kind, sequenceTile: block.base,
				sequenceChunk: block.chunk, sequenceRow: block.row,
				sequenceMask: block.mask,
			}
		}
	}
	return IndexTermLeafPostingIterator{
		leaf: m.leaf, position: int(m.leaf.postingAt) + int(m.first),
		remaining: m.count,
	}
}

// MaskIterator is the preferred equality hot path when the consumer needs
// masks rather than posting metadata.
func (m IndexTermLeafMatch) MaskIterator() IndexTermLeafAggregateIterator {
	if m.leaf.flags != 0 {
		block, position, ok := m.leaf.globalDirectElementAt(int(m.first))
		if ok {
			return IndexTermLeafAggregateIterator{
				leaf: m.leaf, position: position, remaining: 1,
				sequenceKind: block.kind, sequenceTile: block.base,
				sequenceChunk: block.chunk, sequenceRow: block.row,
				sequenceMask: block.mask,
			}
		}
	}
	return IndexTermLeafAggregateIterator{
		leaf: m.leaf, position: int(m.leaf.postingAt) + int(m.first),
		remaining: m.count,
	}
}

func (m IndexTermLeafMatch) DirectBlock() (
	IndexTermLeafDirectBlockView,
	bool,
) {
	if m.leaf.flags != 0 {
		return m.leaf.directBlockAt(int(m.first))
	}
	position := int(m.leaf.postingAt) + int(m.first)
	return openIndexTermLeafDirectBlock(
		m.leaf.encoded, position, m.count,
	)
}

func (b IndexTermLeafDirectBlockView) Len() int { return int(b.count) }

// SingletonRows exposes count little-endian uint16 row ids for a contiguous
// tile run. The byte slice borrows the admitted leaf.
func (b IndexTermLeafDirectBlockView) SingletonRows() (
	baseTile uint32,
	rows []byte,
	ok bool,
) {
	if b.kind != indexTermLeafDirect1Contiguous {
		return 0, nil, false
	}
	return b.base, b.payload, true
}

// SingletonRun exposes a repeated row shared by every tile in the contiguous
// run. This removes both per-posting row bytes and variable shifts in scans.
func (b IndexTermLeafDirectBlockView) SingletonRun() (
	baseTile uint32,
	row uint16,
	count int,
	ok bool,
) {
	if b.kind != indexTermLeafDirect1SameRow {
		return 0, 0, 0, false
	}
	return b.base, b.row, int(b.count), true
}

// OneMasks exposes count packed (chunk byte, little-endian uint64 mask)
// records for a contiguous tile run.
func (b IndexTermLeafDirectBlockView) OneMasks() (
	baseTile uint32,
	masks []byte,
	ok bool,
) {
	if b.kind != indexTermLeafDirectN1Contiguous {
		return 0, nil, false
	}
	return b.base, b.payload, true
}

// OneMaskBits exposes a shared chunk id and count contiguous little-endian
// uint64 masks. It is the scan-optimized global one-mask representation.
func (b IndexTermLeafDirectBlockView) OneMaskBits() (
	baseTile uint32,
	chunk uint8,
	masks []byte,
	ok bool,
) {
	if b.kind != indexTermLeafDirectN1SameChunk {
		return 0, 0, nil, false
	}
	return b.base, b.chunk, b.payload, true
}

// OneMaskRun exposes one repeated chunk/mask pair shared by every tile.
func (b IndexTermLeafDirectBlockView) OneMaskRun() (
	baseTile uint32,
	mask TermPostingMask,
	count int,
	ok bool,
) {
	if b.kind != indexTermLeafDirectN1SameMask {
		return 0, TermPostingMask{}, 0, false
	}
	return b.base, TermPostingMask{
		Chunk: b.chunk, Bits: b.mask,
	}, int(b.count), true
}

// OneMaskWords exposes the aligned mask column as native uint64 words on
// little-endian hosts. Callers that need portable encoded bytes can use
// OneMaskBits instead.
func (b IndexTermLeafDirectBlockView) OneMaskWords() (
	baseTile uint32,
	chunk uint8,
	masks []uint64,
	ok bool,
) {
	if b.kind != indexTermLeafDirectN1SameChunk ||
		!indexTermLeafNativeLittleEndian || len(b.payload)%8 != 0 {
		return 0, 0, nil, false
	}
	if len(b.payload) == 0 {
		return b.base, b.chunk, nil, true
	}
	pointer := unsafe.Pointer(&b.payload[0])
	if uintptr(pointer)%unsafe.Alignof(uint64(0)) != 0 {
		return 0, 0, nil, false
	}
	return b.base, b.chunk, unsafe.Slice(
		(*uint64)(pointer),
		len(b.payload)/8,
	), true
}

func (b IndexTermLeafDirectBlockView) Iterator() IndexTermLeafDirectBlockIterator {
	return IndexTermLeafDirectBlockIterator{
		payload: b.payload, tile: b.base, remaining: b.count, kind: b.kind,
		chunk: b.chunk, row: b.row, mask: b.mask,
	}
}

func (it *IndexTermLeafDirectBlockIterator) Next() (
	tileID uint32,
	mask TermPostingMask,
	ok bool,
) {
	if it == nil || it.remaining == 0 {
		return 0, TermPostingMask{}, false
	}
	switch it.kind {
	case indexTermLeafDirect1Contiguous:
		row := int(binary.LittleEndian.Uint16(it.payload[:2]))
		it.payload = it.payload[2:]
		tile := it.tile
		it.tile++
		it.remaining--
		return tile, TermPostingMask{
			Chunk: uint8(row >> 6), Bits: uint64(1) << uint(row&63),
		}, true
	case indexTermLeafDirect1SameRow:
		row := int(it.row)
		tile := it.tile
		it.tile++
		it.remaining--
		return tile, TermPostingMask{
			Chunk: uint8(row >> 6), Bits: uint64(1) << uint(row&63),
		}, true
	case indexTermLeafDirectN1Contiguous:
		record := it.payload[:9]
		it.payload = it.payload[9:]
		tile := it.tile
		it.tile++
		it.remaining--
		return tile, TermPostingMask{
			Chunk: record[0], Bits: binary.LittleEndian.Uint64(record[1:9]),
		}, true
	case indexTermLeafDirectN1SameChunk:
		mask := binary.LittleEndian.Uint64(it.payload[:8])
		it.payload = it.payload[8:]
		tile := it.tile
		it.tile++
		it.remaining--
		return tile, TermPostingMask{Chunk: it.chunk, Bits: mask}, true
	case indexTermLeafDirectN1SameMask:
		tile := it.tile
		it.tile++
		it.remaining--
		return tile, TermPostingMask{Chunk: it.chunk, Bits: it.mask}, true
	case indexTermLeafDirectN2Contiguous:
		record := it.payload
		if !it.second {
			it.second = true
			return it.tile, TermPostingMask{
				Chunk: record[0], Bits: binary.LittleEndian.Uint64(record[1:9]),
			}, true
		}
		it.second = false
		it.payload = it.payload[18:]
		tile := it.tile
		it.tile++
		it.remaining--
		return tile, TermPostingMask{
			Chunk: record[9],
			Bits:  binary.LittleEndian.Uint64(record[10:18]),
		}, true
	default:
		return 0, TermPostingMask{}, false
	}
}

func (it *IndexTermLeafAggregateIterator) Next() (
	tileID uint32,
	mask TermPostingMask,
	ok bool,
) {
	if it == nil {
		return 0, TermPostingMask{}, false
	}
	if it.pendingAt < len(it.pending) {
		record := it.pending[it.pendingAt:]
		it.pendingAt += 9
		return it.pendingTile, TermPostingMask{
			Chunk: record[0], Bits: binary.LittleEndian.Uint64(record[1:9]),
		}, true
	}
	if it.haveAdaptive {
		if mask, ok := it.adaptive.Next(); ok {
			return it.pendingTile, mask, true
		}
		it.haveAdaptive = false
	}
	for it.remaining != 0 {
		if it.sequenceKind != 0 {
			tile := it.sequenceTile
			it.sequenceTile++
			it.remaining--
			switch it.sequenceKind {
			case indexTermLeafDirect1Contiguous:
				row := int(binary.LittleEndian.Uint16(
					it.leaf.encoded[it.position : it.position+2],
				))
				it.position += 2
				return tile, TermPostingMask{
					Chunk: uint8(row >> 6), Bits: uint64(1) << uint(row&63),
				}, true
			case indexTermLeafDirect1SameRow:
				row := int(it.sequenceRow)
				return tile, TermPostingMask{
					Chunk: uint8(row >> 6), Bits: uint64(1) << uint(row&63),
				}, true
			case indexTermLeafDirectN1Contiguous:
				record := it.leaf.encoded[it.position : it.position+9]
				it.position += 9
				return tile, TermPostingMask{
					Chunk: record[0],
					Bits:  binary.LittleEndian.Uint64(record[1:9]),
				}, true
			case indexTermLeafDirectN1SameChunk:
				mask := binary.LittleEndian.Uint64(
					it.leaf.encoded[it.position : it.position+8],
				)
				it.position += 8
				return tile, TermPostingMask{
					Chunk: it.sequenceChunk, Bits: mask,
				}, true
			case indexTermLeafDirectN1SameMask:
				return tile, TermPostingMask{
					Chunk: it.sequenceChunk, Bits: it.sequenceMask,
				}, true
			case indexTermLeafDirectN2Contiguous:
				it.pendingTile = tile
				it.pending = it.leaf.encoded[it.position : it.position+18]
				it.position += 18
				it.pendingAt = 9
				record := it.pending
				return tile, TermPostingMask{
					Chunk: record[0],
					Bits:  binary.LittleEndian.Uint64(record[1:9]),
				}, true
			}
		}
		// Direct records were fully admitted by Open. Decode their tiny trusted
		// shape in place so singleton/few-mask equality does not pay the
		// adaptive union decoder or construct an intermediate posting view.
		position := it.position
		kind := it.leaf.encoded[position]
		if isIndexTermLeafDirectBlock(kind) {
			base, n := trustedTermPostingUvarint(
				it.leaf.encoded[position+1 : int(it.leaf.dictionaryAt)],
			)
			it.sequenceKind = kind
			it.sequenceTile = uint32(base)
			it.position = position + 1 + n
			if kind == indexTermLeafDirect1SameRow {
				it.sequenceRow = binary.LittleEndian.Uint16(
					it.leaf.encoded[it.position : it.position+2],
				)
			} else if kind == indexTermLeafDirectN1SameMask {
				it.sequenceChunk = it.leaf.encoded[it.position]
				it.sequenceMask = binary.LittleEndian.Uint64(
					it.leaf.encoded[it.position+1 : it.position+9],
				)
			}
			continue
		}
		if kind == indexTermLeafDirect1 || kind == indexTermLeafDirectN {
			delta, n := trustedTermPostingUvarint(
				it.leaf.encoded[position+1 : int(it.leaf.dictionaryAt)],
			)
			tile := uint32(delta)
			if it.havePrevious {
				tile = it.previousTile + 1 + uint32(delta)
			}
			payloadAt := position + 1 + n
			it.previousTile = tile
			it.havePrevious = true
			it.remaining--
			if kind == indexTermLeafDirect1 {
				row := int(binary.LittleEndian.Uint16(
					it.leaf.encoded[payloadAt : payloadAt+2],
				))
				it.position = payloadAt + 2
				return tile, TermPostingMask{
					Chunk: uint8(row >> 6), Bits: uint64(1) << uint(row&63),
				}, true
			}
			count := int(it.leaf.encoded[payloadAt])
			it.position = payloadAt + 1 + count*9
			it.pendingTile = tile
			it.pending = it.leaf.encoded[payloadAt+1 : it.position]
			it.pendingAt = 9
			record := it.pending
			return tile, TermPostingMask{
				Chunk: record[0], Bits: binary.LittleEndian.Uint64(record[1:9]),
			}, true
		}
		decoded, admitted := it.leaf.decodePosting(
			it.position, int(it.leaf.dictionaryAt),
			it.previousTile, it.havePrevious, true,
		)
		if !admitted {
			return 0, TermPostingMask{}, false
		}
		it.position = decoded.next
		it.previousTile = decoded.tileID
		it.havePrevious = true
		it.remaining--
		record, component := makeIndexTermLeafPosting(
			decoded.tileID, decoded.rows, decoded.codec, decoded.payload,
		)
		it.pendingTile = decoded.tileID
		it.adaptive = TermPostingIterator{
			posting: record, component: component,
			live:  it.leaf.live(decoded.tileID),
			chunk: -1, previousRow: -1, pendingRow: -1,
		}
		it.haveAdaptive = true
		if mask, ok := it.adaptive.Next(); ok {
			return decoded.tileID, mask, true
		}
		it.haveAdaptive = false
	}
	return 0, TermPostingMask{}, false
}

func (it *IndexTermLeafPostingIterator) Next() (
	IndexTermLeafPostingView,
	bool,
) {
	if it == nil || it.remaining == 0 {
		return IndexTermLeafPostingView{}, false
	}
	if it.sequenceKind == 0 &&
		isIndexTermLeafDirectBlock(it.leaf.encoded[it.position]) {
		base, n := trustedTermPostingUvarint(
			it.leaf.encoded[it.position+1 : int(it.leaf.dictionaryAt)],
		)
		it.sequenceKind = it.leaf.encoded[it.position]
		it.sequenceTile = uint32(base)
		it.position += 1 + n
		if it.sequenceKind == indexTermLeafDirect1SameRow {
			it.sequenceRow = binary.LittleEndian.Uint16(
				it.leaf.encoded[it.position : it.position+2],
			)
		} else if it.sequenceKind == indexTermLeafDirectN1SameMask {
			it.sequenceChunk = it.leaf.encoded[it.position]
			it.sequenceMask = binary.LittleEndian.Uint64(
				it.leaf.encoded[it.position+1 : it.position+9],
			)
		}
	}
	if it.sequenceKind != 0 {
		result := IndexTermLeafPostingView{
			tileID: it.sequenceTile, kind: it.sequenceKind,
		}
		switch it.sequenceKind {
		case indexTermLeafDirect1Contiguous:
			result.rows = 1
			result.direct = it.leaf.encoded[it.position : it.position+2]
			it.position += 2
		case indexTermLeafDirect1SameRow:
			result.rows = 1
			result.direct = it.leaf.encoded[it.position : it.position+2]
		case indexTermLeafDirectN1Contiguous:
			result.direct = it.leaf.encoded[it.position : it.position+9]
			result.rows = uint16(bits.OnesCount64(
				binary.LittleEndian.Uint64(result.direct[1:9]),
			))
			it.position += 9
		case indexTermLeafDirectN1SameChunk:
			result.direct = it.leaf.encoded[it.position : it.position+8]
			result.directChunk = it.sequenceChunk
			result.rows = uint16(bits.OnesCount64(
				binary.LittleEndian.Uint64(result.direct),
			))
			it.position += 8
		case indexTermLeafDirectN1SameMask:
			result.direct = it.leaf.encoded[it.position : it.position+9]
			result.rows = uint16(bits.OnesCount64(it.sequenceMask))
		case indexTermLeafDirectN2Contiguous:
			result.direct = it.leaf.encoded[it.position : it.position+18]
			result.rows = uint16(
				bits.OnesCount64(binary.LittleEndian.Uint64(result.direct[1:9])) +
					bits.OnesCount64(binary.LittleEndian.Uint64(result.direct[10:18])),
			)
			it.position += 18
		}
		it.sequenceTile++
		it.remaining--
		return result, true
	}
	decoded, ok := it.leaf.decodePosting(
		it.position, it.postingEnd(), it.previousTile, it.havePrevious, true,
	)
	if !ok {
		return IndexTermLeafPostingView{}, false
	}
	result := IndexTermLeafPostingView{
		tileID: decoded.tileID, rows: decoded.rows, kind: decoded.kind,
		direct: decoded.direct, live: it.leaf.live(decoded.tileID),
	}
	if decoded.kind == indexTermLeafAdaptiveInline ||
		decoded.kind == indexTermLeafAdaptiveDictionary {
		result.adaptive, result.component = makeIndexTermLeafPosting(
			decoded.tileID, decoded.rows, decoded.codec, decoded.payload,
		)
	}
	it.position = decoded.next
	it.previousTile = decoded.tileID
	it.havePrevious = true
	it.remaining--
	return result, true
}

func (p IndexTermLeafPostingView) TileID() uint32 { return p.tileID }
func (p IndexTermLeafPostingView) Rows() uint16   { return p.rows }

func (p IndexTermLeafPostingView) Iterator() IndexTermLeafMaskIterator {
	switch p.kind {
	case indexTermLeafDirect1, indexTermLeafDirect1Contiguous,
		indexTermLeafDirect1SameRow:
		row := int(binary.LittleEndian.Uint16(p.direct))
		return IndexTermLeafMaskIterator{
			kind: p.kind, haveOne: true,
			one: TermPostingMask{
				Chunk: uint8(row >> 6), Bits: uint64(1) << uint(row&63),
			},
		}
	case indexTermLeafDirectN:
		return IndexTermLeafMaskIterator{kind: p.kind, direct: p.direct[1:]}
	case indexTermLeafDirectN1Contiguous,
		indexTermLeafDirectN2Contiguous:
		return IndexTermLeafMaskIterator{kind: p.kind, direct: p.direct}
	case indexTermLeafDirectN1SameChunk:
		return IndexTermLeafMaskIterator{
			kind: p.kind, haveOne: true,
			one: TermPostingMask{
				Chunk: p.directChunk,
				Bits:  binary.LittleEndian.Uint64(p.direct),
			},
		}
	case indexTermLeafDirectN1SameMask:
		return IndexTermLeafMaskIterator{
			kind: p.kind, haveOne: true,
			one: TermPostingMask{
				Chunk: p.direct[0],
				Bits:  binary.LittleEndian.Uint64(p.direct[1:9]),
			},
		}
	default:
		return IndexTermLeafMaskIterator{
			kind: p.kind,
			adaptive: TermPostingIterator{
				posting: p.adaptive, component: p.component, live: p.live,
				chunk: -1, previousRow: -1, pendingRow: -1,
			},
		}
	}
}

func (it *IndexTermLeafMaskIterator) Next() (TermPostingMask, bool) {
	if it == nil {
		return TermPostingMask{}, false
	}
	switch it.kind {
	case indexTermLeafDirect1, indexTermLeafDirect1Contiguous,
		indexTermLeafDirect1SameRow:
		if !it.haveOne {
			return TermPostingMask{}, false
		}
		it.haveOne = false
		return it.one, true
	case indexTermLeafDirectN1SameChunk:
		if !it.haveOne {
			return TermPostingMask{}, false
		}
		it.haveOne = false
		return it.one, true
	case indexTermLeafDirectN1SameMask:
		if !it.haveOne {
			return TermPostingMask{}, false
		}
		it.haveOne = false
		return it.one, true
	case indexTermLeafDirectN:
		if it.position >= len(it.direct) {
			return TermPostingMask{}, false
		}
		record := it.direct[it.position:]
		it.position += 9
		return TermPostingMask{
			Chunk: record[0], Bits: binary.LittleEndian.Uint64(record[1:9]),
		}, true
	case indexTermLeafDirectN1Contiguous,
		indexTermLeafDirectN2Contiguous:
		if it.position >= len(it.direct) {
			return TermPostingMask{}, false
		}
		record := it.direct[it.position:]
		it.position += 9
		return TermPostingMask{
			Chunk: record[0], Bits: binary.LittleEndian.Uint64(record[1:9]),
		}, true
	default:
		return it.adaptive.Next()
	}
}

func deriveIndexTermLeafPosting(
	posting TermPosting,
	component []byte,
	masks *[TermPostingTileChunks]uint64,
) indexTermLeafDerivedPosting {
	derived := indexTermLeafDerivedPosting{
		tileID: posting.TileID, rows: posting.Rows,
		codec: posting.Codec, kind: indexTermLeafAdaptiveInline,
	}
	for chunk, mask := range masks {
		if mask == 0 {
			continue
		}
		if derived.directCount < 2 {
			derived.direct[derived.directCount] = TermPostingMask{
				Chunk: uint8(chunk), Bits: mask,
			}
		}
		derived.directCount++
	}
	if posting.Rows == 1 {
		derived.kind = indexTermLeafDirect1
		return derived
	}
	if derived.directCount <= 2 {
		derived.kind = indexTermLeafDirectN
		return derived
	}
	if posting.Placement == TermPostingInline {
		derived.payload = posting.Inline[:posting.EncodedBytes:posting.EncodedBytes]
	} else {
		derived.payload = component
	}
	if len(derived.payload) > TermPostingInlineBytes {
		derived.dictionaryEligible = true
		derived.payloadHash = indexTermLeafPayloadHash(
			posting.Codec, posting.Rows, derived.payload,
		)
	}
	return derived
}

// deriveChunk0IndexTermLeafPosting is deriveIndexTermLeafPosting specialized
// for a posting whose only populated chunk is chunk 0: exactly the direct
// branches of the general derivation (one non-zero chunk can never reach the
// adaptive fallthrough), with no 64-chunk scan and no mask-array
// materialization.
func deriveChunk0IndexTermLeafPosting(
	tileID uint32, rows uint16, chunk0 uint64,
) indexTermLeafDerivedPosting {
	derived := indexTermLeafDerivedPosting{
		tileID: tileID, rows: rows,
		kind: indexTermLeafAdaptiveInline,
	}
	if chunk0 != 0 {
		derived.direct[0] = TermPostingMask{Chunk: 0, Bits: chunk0}
		derived.directCount = 1
	}
	if rows == 1 {
		derived.kind = indexTermLeafDirect1
		return derived
	}
	derived.kind = indexTermLeafDirectN
	return derived
}

// indexTermLeafPayloadHash fingerprints one dictionary-candidate payload
// identity (codec, rows, payload bytes) for the builder's use-counting
// table. Collisions are resolved by full byte comparison, never trusted.
func indexTermLeafPayloadHash(
	codec TermPostingCodec, rows uint16, payload []byte,
) uint64 {
	const offset, prime = 0xcbf29ce484222325, 0x100000001b3
	h := uint64(offset)
	h = (h ^ uint64(codec)) * prime
	h = (h ^ uint64(rows)) * prime
	for _, b := range payload {
		h = (h ^ uint64(b)) * prime
	}
	return h
}

func indexTermLeafPayloadIdentityEqual(
	a, b *indexTermLeafDerivedPosting,
) bool {
	return a.codec == b.codec && a.rows == b.rows &&
		bytes.Equal(a.payload, b.payload)
}

func indexTermLeafPostingBytes(
	posting *indexTermLeafDerivedPosting,
	tileDelta uint64,
) int {
	base := 1 + indexTermLeafUvarintBytes(tileDelta)
	switch posting.kind {
	case indexTermLeafDirect1:
		return base + 2
	case indexTermLeafDirectN:
		return base + 1 + int(posting.directCount)*9
	case indexTermLeafAdaptiveInline:
		return base + 5 + len(posting.payload)
	case indexTermLeafAdaptiveDictionary:
		return base + 5
	default:
		panic("invalid derived exact-term posting")
	}
}

func selectIndexTermLeafDirectBlock(
	postings []indexTermLeafDerivedPosting,
) byte {
	if len(postings) == 0 {
		return indexTermLeafNoDirectBlock
	}
	first := &postings[0]
	var block byte
	switch {
	case first.kind == indexTermLeafDirect1:
		block = indexTermLeafDirect1Contiguous
	case first.kind == indexTermLeafDirectN && first.directCount == 1:
		block = indexTermLeafDirectN1Contiguous
	case first.kind == indexTermLeafDirectN && first.directCount == 2:
		block = indexTermLeafDirectN2Contiguous
	default:
		return indexTermLeafNoDirectBlock
	}
	for i := 1; i < len(postings); i++ {
		posting := &postings[i]
		if posting.tileID != postings[i-1].tileID+1 {
			return indexTermLeafNoDirectBlock
		}
		switch block {
		case indexTermLeafDirect1Contiguous:
			if posting.kind != indexTermLeafDirect1 {
				return indexTermLeafNoDirectBlock
			}
		case indexTermLeafDirectN1Contiguous:
			if posting.kind != indexTermLeafDirectN ||
				posting.directCount != 1 {
				return indexTermLeafNoDirectBlock
			}
		case indexTermLeafDirectN2Contiguous:
			if posting.kind != indexTermLeafDirectN ||
				posting.directCount != 2 {
				return indexTermLeafNoDirectBlock
			}
		}
	}
	if block == indexTermLeafDirect1Contiguous {
		row := int(first.direct[0].Chunk)*64 +
			bits.TrailingZeros64(first.direct[0].Bits)
		sameRow := true
		for i := 1; i < len(postings); i++ {
			postingRow := int(postings[i].direct[0].Chunk)*64 +
				bits.TrailingZeros64(postings[i].direct[0].Bits)
			if postingRow != row {
				sameRow = false
				break
			}
		}
		if sameRow {
			return indexTermLeafDirect1SameRow
		}
	}
	if block == indexTermLeafDirectN1Contiguous {
		firstMask := first.direct[0]
		sameMask := true
		for i := 1; i < len(postings); i++ {
			if postings[i].direct[0] != firstMask {
				sameMask = false
				break
			}
		}
		if sameMask {
			return indexTermLeafDirectN1SameMask
		}
	}
	return block
}

func selectIndexTermLeafGlobalDirect(
	terms []indexTermLeafDerivedTerm,
) byte {
	if len(terms) == 0 {
		return indexTermLeafNoDirectBlock
	}
	postings := terms[0].postings
	if len(postings) != 1 {
		return indexTermLeafNoDirectBlock
	}
	block := selectIndexTermLeafDirectBlock(postings)
	if block == indexTermLeafNoDirectBlock {
		return block
	}
	previousTile := postings[0].tileID
	for i := 1; i < len(terms); i++ {
		if len(terms[i].postings) != 1 ||
			terms[i].postings[0].tileID != previousTile+1 {
			return indexTermLeafNoDirectBlock
		}
		postingBlock := selectIndexTermLeafDirectBlock(terms[i].postings)
		if postingBlock != block {
			return indexTermLeafNoDirectBlock
		}
		previousTile = terms[i].postings[0].tileID
	}
	if block == indexTermLeafDirect1SameRow {
		row := int(terms[0].postings[0].direct[0].Chunk)*64 +
			bits.TrailingZeros64(terms[0].postings[0].direct[0].Bits)
		for i := 1; i < len(terms); i++ {
			posting := &terms[i].postings[0]
			postingRow := int(posting.direct[0].Chunk)*64 +
				bits.TrailingZeros64(posting.direct[0].Bits)
			if postingRow != row {
				return indexTermLeafDirect1Contiguous
			}
		}
		return indexTermLeafDirect1SameRow
	}
	if block == indexTermLeafDirectN1SameMask {
		first := terms[0].postings[0].direct[0]
		sameChunk := true
		for i := 1; i < len(terms); i++ {
			mask := terms[i].postings[0].direct[0]
			if mask.Chunk != first.Chunk {
				return indexTermLeafDirectN1Contiguous
			}
			if mask.Bits != first.Bits {
				sameChunk = false
			}
		}
		if sameChunk {
			return indexTermLeafDirectN1SameMask
		}
		return indexTermLeafDirectN1SameChunk
	}
	if block == indexTermLeafDirectN1Contiguous {
		chunk := terms[0].postings[0].direct[0].Chunk
		sameChunk := true
		for i := 1; i < len(terms); i++ {
			if terms[i].postings[0].direct[0].Chunk != chunk {
				sameChunk = false
				break
			}
		}
		if sameChunk {
			return indexTermLeafDirectN1SameChunk
		}
	}
	return block
}

func indexTermLeafGlobalDirectFlag(kind byte) uint16 {
	switch kind {
	case indexTermLeafDirect1Contiguous:
		return indexTermLeafFlagGlobalDirect1
	case indexTermLeafDirectN1Contiguous:
		return indexTermLeafFlagGlobalDirectN1
	case indexTermLeafDirectN2Contiguous:
		return indexTermLeafFlagGlobalDirectN2
	case indexTermLeafDirectN1SameChunk:
		return indexTermLeafFlagGlobalSameN1
	case indexTermLeafDirect1SameRow:
		return indexTermLeafFlagGlobalSameRow
	case indexTermLeafDirectN1SameMask:
		return indexTermLeafFlagGlobalSameMask
	default:
		return 0
	}
}

func indexTermLeafGlobalDirectKind(flags uint16) byte {
	switch flags {
	case indexTermLeafFlagGlobalDirect1:
		return indexTermLeafDirect1Contiguous
	case indexTermLeafFlagGlobalDirectN1:
		return indexTermLeafDirectN1Contiguous
	case indexTermLeafFlagGlobalDirectN2:
		return indexTermLeafDirectN2Contiguous
	case indexTermLeafFlagGlobalSameN1:
		return indexTermLeafDirectN1SameChunk
	case indexTermLeafFlagGlobalSameRow:
		return indexTermLeafDirect1SameRow
	case indexTermLeafFlagGlobalSameMask:
		return indexTermLeafDirectN1SameMask
	default:
		return indexTermLeafNoDirectBlock
	}
}

func indexTermLeafGlobalDirectBytes(
	kind byte,
	terms []indexTermLeafDerivedTerm,
) int {
	bytesPerTerm := 2
	switch kind {
	case indexTermLeafDirectN1Contiguous:
		bytesPerTerm = 9
	case indexTermLeafDirectN2Contiguous:
		bytesPerTerm = 18
	case indexTermLeafDirectN1SameChunk:
		return 1 + indexTermLeafUvarintBytes(
			uint64(terms[0].postings[0].tileID),
		) + 1 + len(terms)*8
	case indexTermLeafDirect1SameRow:
		return 1 + indexTermLeafUvarintBytes(
			uint64(terms[0].postings[0].tileID),
		) + 2
	case indexTermLeafDirectN1SameMask:
		return 1 + indexTermLeafUvarintBytes(
			uint64(terms[0].postings[0].tileID),
		) + 9
	}
	return 1 + indexTermLeafUvarintBytes(
		uint64(terms[0].postings[0].tileID),
	) + len(terms)*bytesPerTerm
}

func appendIndexTermLeafGlobalDirect(
	dst []byte,
	position int,
	kind byte,
	terms []indexTermLeafDerivedTerm,
) int {
	dst[position] = kind
	position++
	position += putIndexTermLeafUvarint(
		dst[position:], uint64(terms[0].postings[0].tileID),
	)
	if kind == indexTermLeafDirectN1SameChunk {
		dst[position] = terms[0].postings[0].direct[0].Chunk
		position++
		position += indexTermLeafAlign8Padding(position)
		for i := range terms {
			binary.LittleEndian.PutUint64(
				dst[position:position+8],
				terms[i].postings[0].direct[0].Bits,
			)
			position += 8
		}
		return position
	}
	if kind == indexTermLeafDirect1SameRow {
		posting := &terms[0].postings[0]
		row := int(posting.direct[0].Chunk)*64 +
			bits.TrailingZeros64(posting.direct[0].Bits)
		binary.LittleEndian.PutUint16(
			dst[position:position+2], uint16(row),
		)
		return position + 2
	}
	if kind == indexTermLeafDirectN1SameMask {
		mask := terms[0].postings[0].direct[0]
		dst[position] = mask.Chunk
		binary.LittleEndian.PutUint64(
			dst[position+1:position+9], mask.Bits,
		)
		return position + 9
	}
	for i := range terms {
		posting := &terms[i].postings[0]
		switch kind {
		case indexTermLeafDirect1Contiguous:
			row := int(posting.direct[0].Chunk)*64 +
				bits.TrailingZeros64(posting.direct[0].Bits)
			binary.LittleEndian.PutUint16(
				dst[position:position+2], uint16(row),
			)
			position += 2
		case indexTermLeafDirectN1Contiguous,
			indexTermLeafDirectN2Contiguous:
			for mask := 0; mask < int(posting.directCount); mask++ {
				dst[position] = posting.direct[mask].Chunk
				binary.LittleEndian.PutUint64(
					dst[position+1:position+9], posting.direct[mask].Bits,
				)
				position += 9
			}
		}
	}
	return position
}

func indexTermLeafDirectBlockBytes(
	kind byte,
	postings []indexTermLeafDerivedPosting,
) int {
	bytesPerPosting := 2
	switch kind {
	case indexTermLeafDirectN1Contiguous:
		bytesPerPosting = 9
	case indexTermLeafDirectN2Contiguous:
		bytesPerPosting = 18
	case indexTermLeafDirectN1SameChunk:
		return 1 + indexTermLeafUvarintBytes(
			uint64(postings[0].tileID),
		) + 1 + len(postings)*8
	case indexTermLeafDirect1SameRow:
		return 1 + indexTermLeafUvarintBytes(
			uint64(postings[0].tileID),
		) + 2
	case indexTermLeafDirectN1SameMask:
		return 1 + indexTermLeafUvarintBytes(
			uint64(postings[0].tileID),
		) + 9
	}
	return 1 + indexTermLeafUvarintBytes(uint64(postings[0].tileID)) +
		len(postings)*bytesPerPosting
}

func appendIndexTermLeafDirectBlock(
	dst []byte,
	position int,
	kind byte,
	postings []indexTermLeafDerivedPosting,
) int {
	dst[position] = kind
	position++
	position += putIndexTermLeafUvarint(
		dst[position:], uint64(postings[0].tileID),
	)
	if kind == indexTermLeafDirect1SameRow {
		posting := &postings[0]
		row := int(posting.direct[0].Chunk)*64 +
			bits.TrailingZeros64(posting.direct[0].Bits)
		binary.LittleEndian.PutUint16(
			dst[position:position+2], uint16(row),
		)
		return position + 2
	}
	if kind == indexTermLeafDirectN1SameMask {
		mask := postings[0].direct[0]
		dst[position] = mask.Chunk
		binary.LittleEndian.PutUint64(
			dst[position+1:position+9], mask.Bits,
		)
		return position + 9
	}
	for i := range postings {
		posting := &postings[i]
		switch kind {
		case indexTermLeafDirect1Contiguous:
			row := int(posting.direct[0].Chunk)*64 +
				bits.TrailingZeros64(posting.direct[0].Bits)
			binary.LittleEndian.PutUint16(
				dst[position:position+2], uint16(row),
			)
			position += 2
		case indexTermLeafDirectN1Contiguous,
			indexTermLeafDirectN2Contiguous:
			for mask := 0; mask < int(posting.directCount); mask++ {
				dst[position] = posting.direct[mask].Chunk
				binary.LittleEndian.PutUint64(
					dst[position+1:position+9], posting.direct[mask].Bits,
				)
				position += 9
			}
		}
	}
	return position
}

func appendIndexTermLeafPosting(
	dst []byte,
	position int,
	posting *indexTermLeafDerivedPosting,
	tileDelta uint64,
) int {
	dst[position] = posting.kind
	position++
	position += putIndexTermLeafUvarint(dst[position:], tileDelta)
	switch posting.kind {
	case indexTermLeafDirect1:
		row := 0
		for _, mask := range posting.direct {
			if mask.Bits != 0 {
				row = int(mask.Chunk)*64 + bits.TrailingZeros64(mask.Bits)
				break
			}
		}
		binary.LittleEndian.PutUint16(dst[position:position+2], uint16(row))
		return position + 2
	case indexTermLeafDirectN:
		dst[position] = posting.directCount
		position++
		for i := 0; i < int(posting.directCount); i++ {
			dst[position] = posting.direct[i].Chunk
			binary.LittleEndian.PutUint64(dst[position+1:position+9], posting.direct[i].Bits)
			position += 9
		}
		return position
	case indexTermLeafAdaptiveInline:
		binary.LittleEndian.PutUint16(dst[position:position+2], posting.rows)
		dst[position+2] = byte(posting.codec)
		binary.LittleEndian.PutUint16(dst[position+3:position+5], uint16(len(posting.payload)))
		copy(dst[position+5:], posting.payload)
		return position + 5 + len(posting.payload)
	case indexTermLeafAdaptiveDictionary:
		binary.LittleEndian.PutUint16(dst[position:position+2], posting.rows)
		dst[position+2] = byte(posting.codec)
		binary.LittleEndian.PutUint16(dst[position+3:position+5], posting.dictionary)
		return position + 5
	default:
		panic("invalid derived exact-term posting")
	}
}

func (v IndexTermLeafView) admitDictionaries() error {
	expected := 0
	for i := 0; i < int(v.dictionaryN); i++ {
		record := v.dictionaryRecord(i)
		offset := int(binary.LittleEndian.Uint16(record[0:2]))
		length := int(binary.LittleEndian.Uint16(record[2:4]))
		rows := binary.LittleEndian.Uint16(record[4:6])
		codec := TermPostingCodec(record[6])
		if offset != expected || length <= TermPostingInlineBytes ||
			length > TermPostingMaxPayloadBytes || rows == 0 ||
			codec > TermPostingSparseRows || record[7] != 0 ||
			int(v.dictionaryDataAt)+offset+length > len(v.encoded) {
			return indexTermLeafCorrupt("dictionary layout")
		}
		payload := v.encoded[int(v.dictionaryDataAt)+offset : int(v.dictionaryDataAt)+offset+length]
		for previous := 0; previous < i; previous++ {
			other, otherCodec, otherRows, ok :=
				v.dictionaryPayload(previous)
			if !ok {
				return indexTermLeafCorrupt("dictionary predecessor")
			}
			if codec == otherCodec && rows == otherRows &&
				bytes.Equal(payload, other) {
				return indexTermLeafCorrupt("duplicate dictionary payload")
			}
		}
		expected += length
	}
	if int(v.dictionaryDataAt)+expected != len(v.encoded) {
		return indexTermLeafCorrupt("dictionary data length")
	}
	return nil
}

func (v IndexTermLeafView) admitTermsAndPostings() error {
	expectedSuffix := 0
	expectedPosting := 0
	seenPostings := 0
	var previous [IndexTermMaxKeyBytes]byte
	previousLen := 0
	var restartKey [IndexTermMaxKeyBytes]byte
	restartLen := 0
	nextDictionary := uint16(0)
	for i := 0; i < int(v.termCount); i++ {
		record := v.descriptor(i)
		suffixAt := int(binary.LittleEndian.Uint16(record[0:2]))
		firstPosting := int(binary.LittleEndian.Uint16(record[2:4]))
		keyLength := int(binary.LittleEndian.Uint16(record[4:6]))
		shared := int(binary.LittleEndian.Uint16(record[6:8]))
		suffixLength := int(binary.LittleEndian.Uint16(record[8:10]))
		count := int(binary.LittleEndian.Uint16(record[10:12]))
		globalDirect := v.flags != 0
		firstPostingInvalid := !globalDirect && firstPosting != expectedPosting ||
			globalDirect && (firstPosting != i || count != 1)
		if suffixAt != expectedSuffix || firstPostingInvalid ||
			keyLength == 0 || keyLength > IndexTermMaxKeyBytes ||
			shared+suffixLength != keyLength || count == 0 ||
			i%IndexTermLeafRestartInterval == 0 && shared != 0 ||
			i%IndexTermLeafRestartInterval != 0 && shared > previousLen ||
			int(v.keyAt)+suffixAt+suffixLength > int(v.postingAt) {
			return indexTermLeafCorrupt("term descriptor")
		}
		var current [IndexTermMaxKeyBytes]byte
		copy(current[:shared], restartKey[:shared])
		copy(current[shared:keyLength], v.encoded[int(v.keyAt)+suffixAt:int(v.keyAt)+suffixAt+suffixLength])
		key := current[:keyLength]
		if !ValidIndexTermKey(key) ||
			i != 0 && bytes.Compare(previous[:previousLen], key) >= 0 ||
			i%IndexTermLeafRestartInterval != 0 &&
				shared != commonIndexTermLeafPrefix(restartKey[:restartLen], key) {
			return indexTermLeafCorrupt("canonical term key or prefix")
		}
		if i%IndexTermLeafRestartInterval == 0 {
			copy(restartKey[:], key)
			restartLen = keyLength
		}
		expectedSuffix += suffixLength
		copy(previous[:], key)
		previousLen = keyLength

		if globalDirect {
			seenPostings++
			continue
		}
		position := int(v.postingAt) + firstPosting
		end := int(v.dictionaryAt)
		if i+1 < int(v.termCount) {
			end = int(v.postingAt) +
				int(binary.LittleEndian.Uint16(v.descriptor(i + 1)[2:4]))
		}
		if position < end && isIndexTermLeafDirectBlock(v.encoded[position]) {
			if v.encoded[position] == indexTermLeafDirectN1SameChunk {
				return indexTermLeafCorrupt("non-global same-chunk block")
			}
			if !v.admitDirectBlock(position, end, count) {
				return indexTermLeafCorrupt("direct posting block")
			}
			seenPostings += count
			expectedPosting = end - int(v.postingAt)
			continue
		}
		var previousTile uint32
		allDirect := true
		directShape := byte(0xff)
		contiguousDirect := true
		for j := range count {
			posting, ok := v.decodePosting(
				position, end, previousTile, j != 0, false,
			)
			if !ok || !v.admitPosting(posting) {
				return indexTermLeafCorrupt("posting")
			}
			if posting.kind == indexTermLeafAdaptiveDictionary {
				if posting.dictionary > nextDictionary {
					return indexTermLeafCorrupt("dictionary first-use order")
				}
				if posting.dictionary == nextDictionary {
					nextDictionary++
				}
			}
			position = posting.next
			if posting.kind == indexTermLeafDirect1 {
				if directShape == 0xff {
					directShape = indexTermLeafDirect1
				} else if directShape != indexTermLeafDirect1 {
					allDirect = false
				}
			} else if posting.kind == indexTermLeafDirectN {
				shape := posting.direct[0]
				if directShape == 0xff {
					directShape = shape
				} else if directShape != shape {
					allDirect = false
				}
			} else {
				allDirect = false
			}
			if j != 0 && posting.tileID != previousTile+1 {
				contiguousDirect = false
			}
			previousTile = posting.tileID
			seenPostings++
		}
		if position != end || allDirect && contiguousDirect {
			return indexTermLeafCorrupt("posting aggregate boundary")
		}
		expectedPosting = end - int(v.postingAt)
	}
	if v.flags != 0 {
		kind := indexTermLeafGlobalDirectKind(v.flags)
		if kind == indexTermLeafNoDirectBlock || v.dictionaryN != 0 ||
			v.postingCount != v.termCount ||
			v.encoded[v.postingAt] != kind ||
			!v.admitDirectBlock(
				int(v.postingAt), int(v.dictionaryAt), int(v.termCount),
			) {
			return indexTermLeafCorrupt("global direct column")
		}
		expectedPosting = int(v.dictionaryAt) - int(v.postingAt)
	}
	if int(v.keyAt)+expectedSuffix != int(v.postingAt) ||
		int(v.postingAt)+expectedPosting != int(v.dictionaryAt) ||
		seenPostings != int(v.postingCount) ||
		nextDictionary != v.dictionaryN {
		return indexTermLeafCorrupt("aggregate totals")
	}
	return nil
}

func isIndexTermLeafDirectBlock(kind byte) bool {
	return kind >= indexTermLeafDirect1Contiguous &&
		kind <= indexTermLeafDirectN1SameMask
}

func (v IndexTermLeafView) admitDirectBlock(
	position, end, count int,
) bool {
	kind := v.encoded[position]
	position++
	base, n := canonicalTermPostingUvarint(v.encoded[position:end])
	if n <= 0 || base > uint64(^uint32(0)) ||
		uint64(count-1) > uint64(^uint32(0))-base {
		return false
	}
	position += n
	bytesPerPosting := 2
	switch kind {
	case indexTermLeafDirect1Contiguous:
	case indexTermLeafDirectN1Contiguous:
		bytesPerPosting = 9
	case indexTermLeafDirectN2Contiguous:
		bytesPerPosting = 18
	case indexTermLeafDirectN1SameChunk:
		if position >= end {
			return false
		}
		chunk := int(v.encoded[position])
		position++
		padding := indexTermLeafAlign8Padding(position)
		if position+padding > end {
			return false
		}
		for _, value := range v.encoded[position : position+padding] {
			if value != 0 {
				return false
			}
		}
		position += padding
		if chunk >= TermPostingTileChunks ||
			position+count*8 != end {
			return false
		}
		for i := range count {
			tileID := uint32(base) + uint32(i)
			live := v.live(tileID)
			mask := binary.LittleEndian.Uint64(
				v.encoded[position+i*8 : position+i*8+8],
			)
			if live == nil || bits.OnesCount64(mask) <= 1 ||
				mask&^live[chunk] != 0 {
				return false
			}
		}
		return true
	case indexTermLeafDirect1SameRow:
		if position+2 != end {
			return false
		}
		row := int(binary.LittleEndian.Uint16(
			v.encoded[position : position+2],
		))
		if row >= TermPostingTileRows {
			return false
		}
		for i := range count {
			live := v.live(uint32(base) + uint32(i))
			if live == nil ||
				live[row>>6]&(uint64(1)<<uint(row&63)) == 0 {
				return false
			}
		}
		return true
	case indexTermLeafDirectN1SameMask:
		if position+9 != end {
			return false
		}
		chunk := int(v.encoded[position])
		mask := binary.LittleEndian.Uint64(
			v.encoded[position+1 : position+9],
		)
		if chunk >= TermPostingTileChunks || bits.OnesCount64(mask) <= 1 {
			return false
		}
		for i := range count {
			live := v.live(uint32(base) + uint32(i))
			if live == nil || mask&^live[chunk] != 0 {
				return false
			}
		}
		return true
	default:
		return false
	}
	if position+count*bytesPerPosting != end {
		return false
	}
	for i := range count {
		tileID := uint32(base) + uint32(i)
		live := v.live(tileID)
		if live == nil {
			return false
		}
		switch kind {
		case indexTermLeafDirect1Contiguous:
			row := int(binary.LittleEndian.Uint16(
				v.encoded[position : position+2],
			))
			if row >= TermPostingTileRows ||
				live[row>>6]&(uint64(1)<<uint(row&63)) == 0 {
				return false
			}
			position += 2
		case indexTermLeafDirectN1Contiguous:
			chunk := int(v.encoded[position])
			mask := binary.LittleEndian.Uint64(
				v.encoded[position+1 : position+9],
			)
			if chunk >= TermPostingTileChunks || bits.OnesCount64(mask) <= 1 ||
				mask&^live[chunk] != 0 {
				return false
			}
			position += 9
		case indexTermLeafDirectN2Contiguous:
			firstChunk := int(v.encoded[position])
			firstMask := binary.LittleEndian.Uint64(
				v.encoded[position+1 : position+9],
			)
			secondChunk := int(v.encoded[position+9])
			secondMask := binary.LittleEndian.Uint64(
				v.encoded[position+10 : position+18],
			)
			if firstChunk >= TermPostingTileChunks ||
				secondChunk <= firstChunk ||
				secondChunk >= TermPostingTileChunks ||
				firstMask == 0 || secondMask == 0 ||
				firstMask&^live[firstChunk] != 0 ||
				secondMask&^live[secondChunk] != 0 {
				return false
			}
			position += 18
		}
	}
	return true
}

func (v IndexTermLeafView) admitEqualityTable() error {
	for slot := 0; slot < int(v.equalitySlots); slot++ {
		index := binary.LittleEndian.Uint16(v.encoded[int(v.equalityAt)+slot*2 : int(v.equalityAt)+slot*2+2])
		if index != ^uint16(0) && index >= v.termCount {
			return indexTermLeafCorrupt("equality table index")
		}
	}
	var scratch [IndexTermMaxKeyBytes]byte
	for index := 0; index < int(v.termCount); index++ {
		key, ok := v.reconstructKey(index, scratch[:0])
		if !ok {
			return indexTermLeafCorrupt("equality table key")
		}
		hash := IndexTermRouteHash(v.storeID, key)
		slot := int(hash & uint64(v.equalitySlots-1))
		found := false
		for probes := 0; probes < int(v.equalitySlots); probes++ {
			candidate := binary.LittleEndian.Uint16(v.encoded[int(v.equalityAt)+slot*2 : int(v.equalityAt)+slot*2+2])
			if candidate == uint16(index) {
				found = true
				break
			}
			// Canonical construction inserts terms in key order. Therefore an
			// empty slot or a later term before this term proves a different
			// (even if lookup-equivalent) table serialization.
			if candidate == ^uint16(0) || candidate > uint16(index) {
				break
			}
			slot = (slot + 1) & (int(v.equalitySlots) - 1)
		}
		if !found {
			return indexTermLeafCorrupt("non-canonical equality table")
		}
	}
	return nil
}

func (v IndexTermLeafView) admitPosting(posting indexTermLeafDecodedPosting) bool {
	live := v.live(posting.tileID)
	if live == nil {
		return false
	}
	switch posting.kind {
	case indexTermLeafDirect1:
		row := int(binary.LittleEndian.Uint16(posting.direct))
		return row < TermPostingTileRows &&
			live[row>>6]&(uint64(1)<<uint(row&63)) != 0
	case indexTermLeafDirectN:
		count := int(posting.direct[0])
		rows := 0
		previousChunk := -1
		for i := range count {
			record := posting.direct[1+i*9:]
			chunk := int(record[0])
			mask := binary.LittleEndian.Uint64(record[1:9])
			if chunk <= previousChunk || chunk >= TermPostingTileChunks ||
				mask == 0 || mask&^live[chunk] != 0 {
				return false
			}
			rows += bits.OnesCount64(mask)
			previousChunk = chunk
		}
		return rows > 1
	case indexTermLeafAdaptiveInline, indexTermLeafAdaptiveDictionary:
		record, component := makeIndexTermLeafPosting(
			posting.tileID, posting.rows, posting.codec, posting.payload,
		)
		view, err := OpenTermPosting(record, component, live)
		if err != nil {
			return false
		}
		iterator := view.Iterator()
		masks := 0
		for {
			_, ok := iterator.Next()
			if !ok {
				break
			}
			masks++
			if masks > 2 {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func (v IndexTermLeafView) admitCanonicalDictionaryChoice() error {
	for dictionary := 0; dictionary < int(v.dictionaryN); dictionary++ {
		payload, codec, rows, ok := v.dictionaryPayload(dictionary)
		if !ok {
			return indexTermLeafCorrupt("dictionary reference")
		}
		uses := v.countAdaptiveIdentity(codec, rows, payload)
		if uses < 2 || (uses-1)*len(payload) <= indexTermLeafDictionaryBytes {
			return indexTermLeafCorrupt("unprofitable dictionary")
		}
	}
	// Any repeated out-of-line inline payload that would save bytes is a
	// second valid representation of the same logical leaf and is rejected.
	return v.walkPostings(func(posting indexTermLeafDecodedPosting) error {
		if posting.kind != indexTermLeafAdaptiveInline ||
			len(posting.payload) <= TermPostingInlineBytes {
			return nil
		}
		uses := v.countAdaptiveIdentity(posting.codec, posting.rows, posting.payload)
		if (uses-1)*len(posting.payload) > indexTermLeafDictionaryBytes {
			return indexTermLeafCorrupt("missing profitable dictionary")
		}
		return nil
	})
}

func (v IndexTermLeafView) countAdaptiveIdentity(
	codec TermPostingCodec,
	rows uint16,
	payload []byte,
) int {
	count := 0
	_ = v.walkPostings(func(posting indexTermLeafDecodedPosting) error {
		if (posting.kind == indexTermLeafAdaptiveInline ||
			posting.kind == indexTermLeafAdaptiveDictionary) &&
			posting.codec == codec && posting.rows == rows &&
			bytes.Equal(posting.payload, payload) {
			count++
		}
		return nil
	})
	return count
}

func (v IndexTermLeafView) walkPostings(
	fn func(indexTermLeafDecodedPosting) error,
) error {
	if v.flags != 0 {
		return nil
	}
	for i := 0; i < int(v.termCount); i++ {
		record := v.descriptor(i)
		position := int(v.postingAt) +
			int(binary.LittleEndian.Uint16(record[2:4]))
		count := int(binary.LittleEndian.Uint16(record[10:12]))
		end := int(v.dictionaryAt)
		if i+1 < int(v.termCount) {
			end = int(v.postingAt) +
				int(binary.LittleEndian.Uint16(v.descriptor(i + 1)[2:4]))
		}
		if position < end && isIndexTermLeafDirectBlock(v.encoded[position]) {
			continue
		}
		var previousTile uint32
		for j := range count {
			posting, ok := v.decodePosting(
				position, end, previousTile, j != 0, true,
			)
			if !ok {
				return indexTermLeafCorrupt("trusted posting walk")
			}
			if err := fn(posting); err != nil {
				return err
			}
			position = posting.next
			previousTile = posting.tileID
		}
	}
	return nil
}

func (v IndexTermLeafView) decodePosting(
	position, end int,
	previousTile uint32,
	havePrevious, trusted bool,
) (indexTermLeafDecodedPosting, bool) {
	if position < int(v.postingAt) || position >= end || end > int(v.dictionaryAt) {
		return indexTermLeafDecodedPosting{}, false
	}
	kind := v.encoded[position]
	position++
	var delta uint64
	var n int
	if trusted {
		delta, n = trustedTermPostingUvarint(v.encoded[position:end])
	} else {
		delta, n = canonicalTermPostingUvarint(v.encoded[position:end])
	}
	if n <= 0 {
		return indexTermLeafDecodedPosting{}, false
	}
	position += n
	tile := delta
	if havePrevious {
		tile = uint64(previousTile) + 1 + delta
	}
	if tile > uint64(^uint32(0)) ||
		havePrevious && uint32(tile) <= previousTile {
		return indexTermLeafDecodedPosting{}, false
	}
	result := indexTermLeafDecodedPosting{kind: kind, tileID: uint32(tile)}
	switch kind {
	case indexTermLeafDirect1:
		if position+2 > end {
			return result, false
		}
		result.rows = 1
		result.direct = v.encoded[position : position+2]
		result.next = position + 2
	case indexTermLeafDirectN:
		if position >= end {
			return result, false
		}
		count := int(v.encoded[position])
		if count < 1 || count > 2 || position+1+count*9 > end {
			return result, false
		}
		result.direct = v.encoded[position : position+1+count*9]
		for i := range count {
			result.rows += uint16(bits.OnesCount64(binary.LittleEndian.Uint64(
				result.direct[1+i*9+1 : 1+i*9+9],
			)))
		}
		result.next = position + 1 + count*9
	case indexTermLeafAdaptiveInline:
		if position+5 > end {
			return result, false
		}
		result.rows = binary.LittleEndian.Uint16(v.encoded[position : position+2])
		result.codec = TermPostingCodec(v.encoded[position+2])
		length := int(binary.LittleEndian.Uint16(v.encoded[position+3 : position+5]))
		if length > TermPostingMaxPayloadBytes || position+5+length > end {
			return result, false
		}
		result.payload = v.encoded[position+5 : position+5+length]
		result.next = position + 5 + length
	case indexTermLeafAdaptiveDictionary:
		if position+5 > end {
			return result, false
		}
		result.rows = binary.LittleEndian.Uint16(v.encoded[position : position+2])
		result.codec = TermPostingCodec(v.encoded[position+2])
		result.dictionary = binary.LittleEndian.Uint16(v.encoded[position+3 : position+5])
		payload, codec, rows, ok := v.dictionaryPayload(int(result.dictionary))
		if !ok || codec != result.codec || rows != result.rows {
			return result, false
		}
		result.payload = payload
		result.next = position + 5
	default:
		return result, false
	}
	return result, true
}

func (it *IndexTermLeafPostingIterator) postingEnd() int {
	if it.remaining == 0 {
		return it.position
	}
	// The iterator operates on an already admitted stream; dictionaryAt is a
	// safe broad bound and remaining prevents crossing into the next term.
	return int(it.leaf.dictionaryAt)
}

func (v IndexTermLeafView) dictionaryPayload(
	index int,
) ([]byte, TermPostingCodec, uint16, bool) {
	if index < 0 || index >= int(v.dictionaryN) {
		return nil, 0, 0, false
	}
	record := v.dictionaryRecord(index)
	offset := int(binary.LittleEndian.Uint16(record[0:2]))
	length := int(binary.LittleEndian.Uint16(record[2:4]))
	start := int(v.dictionaryDataAt) + offset
	if start < int(v.dictionaryDataAt) || start+length > len(v.encoded) {
		return nil, 0, 0, false
	}
	return v.encoded[start : start+length],
		TermPostingCodec(record[6]),
		binary.LittleEndian.Uint16(record[4:6]),
		true
}

func makeIndexTermLeafPosting(
	tileID uint32,
	rows uint16,
	codec TermPostingCodec,
	payload []byte,
) (TermPosting, []byte) {
	record := TermPosting{
		TileID: tileID, Rows: rows, EncodedBytes: uint16(len(payload)),
		Codec: codec, Placement: TermPostingInline,
	}
	if len(payload) <= TermPostingInlineBytes {
		copy(record.Inline[:], payload)
		return record, nil
	}
	record.Placement = TermPostingManifest
	record.ComponentID = termPostingComponentID(codec, rows, payload)
	return record, payload
}

func (v IndexTermLeafView) descriptor(index int) []byte {
	start := int(v.descriptorAt) + index*indexTermLeafDescriptorBytes
	return v.encoded[start : start+indexTermLeafDescriptorBytes]
}

func (v IndexTermLeafView) dictionaryRecord(index int) []byte {
	start := int(v.dictionaryAt) + index*indexTermLeafDictionaryBytes
	return v.encoded[start : start+indexTermLeafDictionaryBytes]
}

func (v IndexTermLeafView) reconstructKey(index int, dst []byte) ([]byte, bool) {
	if index < 0 || index >= int(v.termCount) {
		return dst, false
	}
	restart := index - index%IndexTermLeafRestartInterval
	restartRecord := v.descriptor(restart)
	restartAt := int(binary.LittleEndian.Uint16(restartRecord[0:2]))
	restartLength := int(binary.LittleEndian.Uint16(restartRecord[4:6]))
	if restartLength > cap(dst) {
		return dst, false
	}
	if index == restart {
		dst = dst[:restartLength]
		copy(dst, v.encoded[int(v.keyAt)+restartAt:int(v.keyAt)+restartAt+restartLength])
		return dst, true
	}
	record := v.descriptor(index)
	suffixAt := int(binary.LittleEndian.Uint16(record[0:2]))
	keyLength := int(binary.LittleEndian.Uint16(record[4:6]))
	shared := int(binary.LittleEndian.Uint16(record[6:8]))
	suffixLength := int(binary.LittleEndian.Uint16(record[8:10]))
	if shared > restartLength || shared+suffixLength != keyLength ||
		keyLength > cap(dst) {
		return dst, false
	}
	dst = dst[:keyLength]
	copy(dst[:shared], v.encoded[int(v.keyAt)+restartAt:int(v.keyAt)+restartAt+shared])
	copy(dst[shared:], v.encoded[int(v.keyAt)+suffixAt:int(v.keyAt)+suffixAt+suffixLength])
	return dst, true
}

func (v IndexTermLeafView) matchAt(index int) IndexTermLeafMatch {
	record := v.descriptor(index)
	return IndexTermLeafMatch{
		leaf:  v,
		first: binary.LittleEndian.Uint16(record[2:4]),
		count: binary.LittleEndian.Uint16(record[10:12]),
	}
}

func (v IndexTermLeafView) directBlockAt(
	index int,
) (IndexTermLeafDirectBlockView, bool) {
	if index < 0 || index >= int(v.termCount) {
		return IndexTermLeafDirectBlockView{}, false
	}
	if v.flags != 0 {
		block, _, ok := v.globalDirectElementAt(index)
		return block, ok
	}
	record := v.descriptor(index)
	position := int(v.postingAt) +
		int(binary.LittleEndian.Uint16(record[2:4]))
	count := binary.LittleEndian.Uint16(record[10:12])
	return openIndexTermLeafDirectBlock(v.encoded, position, count)
}

// GlobalDirectBlock returns the leaf-wide ordered direct column when present.
func (v IndexTermLeafView) GlobalDirectBlock() (
	IndexTermLeafDirectBlockView,
	bool,
) {
	kind, base, position, bytesPerPosting, ok := v.globalDirectLayout()
	if !ok {
		return IndexTermLeafDirectBlockView{}, false
	}
	if kind == indexTermLeafDirect1SameRow {
		return IndexTermLeafDirectBlockView{
			payload: v.encoded[position : position+2],
			base:    base, count: v.termCount, kind: kind,
			row: v.globalRow,
		}, true
	}
	if kind == indexTermLeafDirectN1SameMask {
		return IndexTermLeafDirectBlockView{
			payload: v.encoded[position : position+9],
			base:    base, count: v.termCount, kind: kind,
			chunk: v.globalChunk, mask: v.globalMask,
		}, true
	}
	end := position + int(v.termCount)*bytesPerPosting
	return IndexTermLeafDirectBlockView{
		payload: v.encoded[position:end:end],
		base:    base,
		count:   v.termCount,
		kind:    kind,
		chunk:   v.globalChunk,
		row:     v.globalRow,
		mask:    v.globalMask,
	}, true
}

// OnlyDirectBlock returns the single term's direct posting run without
// performing an unnecessary exact lookup or reconstructing its key.
func (v IndexTermLeafView) OnlyDirectBlock() (
	IndexTermLeafDirectBlockView,
	bool,
) {
	if v.termCount != 1 || v.flags != 0 {
		return IndexTermLeafDirectBlockView{}, false
	}
	return v.directBlockAt(0)
}

func (v IndexTermLeafView) globalDirectElementAt(
	index int,
) (IndexTermLeafDirectBlockView, int, bool) {
	if index < 0 || index >= int(v.termCount) {
		return IndexTermLeafDirectBlockView{}, 0, false
	}
	kind, base, position, bytesPerPosting, ok := v.globalDirectLayout()
	if !ok {
		return IndexTermLeafDirectBlockView{}, 0, false
	}
	if kind == indexTermLeafDirect1SameRow {
		return IndexTermLeafDirectBlockView{
			payload: v.encoded[position : position+2],
			base:    base + uint32(index), count: 1, kind: kind,
			row: v.globalRow,
		}, position, true
	}
	if kind == indexTermLeafDirectN1SameMask {
		return IndexTermLeafDirectBlockView{
			payload: v.encoded[position : position+9],
			base:    base + uint32(index), count: 1, kind: kind,
			chunk: v.globalChunk, mask: v.globalMask,
		}, position, true
	}
	position += index * bytesPerPosting
	return IndexTermLeafDirectBlockView{
		payload: v.encoded[position : position+bytesPerPosting],
		base:    base + uint32(index),
		count:   1,
		kind:    kind,
		chunk:   v.globalChunk,
		row:     v.globalRow,
		mask:    v.globalMask,
	}, position, true
}

func (v IndexTermLeafView) globalDirectLayout() (
	kind byte,
	base uint32,
	position, bytesPerPosting int,
	ok bool,
) {
	kind = indexTermLeafGlobalDirectKind(v.flags)
	if kind == indexTermLeafNoDirectBlock {
		return 0, 0, 0, 0, false
	}
	return kind, v.globalBase, int(v.globalPayloadAt),
		int(v.globalStride), true
}

func openIndexTermLeafDirectBlock(
	encoded []byte,
	position int,
	count uint16,
) (IndexTermLeafDirectBlockView, bool) {
	kind := encoded[position]
	if !isIndexTermLeafDirectBlock(kind) {
		return IndexTermLeafDirectBlockView{}, false
	}
	base, n := trustedTermPostingUvarint(encoded[position+1:])
	position += 1 + n
	bytesPerPosting := 2
	chunk := uint8(0)
	switch kind {
	case indexTermLeafDirectN1Contiguous:
		bytesPerPosting = 9
	case indexTermLeafDirectN2Contiguous:
		bytesPerPosting = 18
	case indexTermLeafDirectN1SameChunk:
		chunk = encoded[position]
		position++
		position += indexTermLeafAlign8Padding(position)
		bytesPerPosting = 8
	case indexTermLeafDirect1SameRow:
		row := binary.LittleEndian.Uint16(encoded[position : position+2])
		return IndexTermLeafDirectBlockView{
			payload: encoded[position : position+2],
			base:    uint32(base), count: count, kind: kind, row: row,
		}, true
	case indexTermLeafDirectN1SameMask:
		chunk = encoded[position]
		mask := binary.LittleEndian.Uint64(encoded[position+1 : position+9])
		return IndexTermLeafDirectBlockView{
			payload: encoded[position : position+9],
			base:    uint32(base), count: count, kind: kind,
			chunk: chunk, mask: mask,
		}, true
	}
	end := position + int(count)*bytesPerPosting
	return IndexTermLeafDirectBlockView{
		payload: encoded[position:end:end],
		base:    uint32(base),
		count:   count,
		kind:    kind,
		chunk:   chunk,
	}, true
}

func (v IndexTermLeafView) lowerBound(key []byte) int {
	low, high := 0, int(v.termCount)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if v.compareKeyAt(middle, key) >= 0 {
			high = middle
		} else {
			low = middle + 1
		}
	}
	return low
}

func (v IndexTermLeafView) compareKeyAt(index int, key []byte) int {
	const commonKeyScratch = 256
	restart := index - index%IndexTermLeafRestartInterval
	for i := restart; i <= index; i++ {
		record := v.descriptor(i)
		if binary.LittleEndian.Uint16(record[4:6]) > commonKeyScratch {
			return v.compareLongKeyAt(index, key)
		}
	}
	var scratch [commonKeyScratch]byte
	candidate, ok := v.reconstructKey(index, scratch[:0])
	if !ok {
		return 1
	}
	return bytes.Compare(candidate, key)
}

// exactKeyAt verifies one accelerator candidate without materializing its
// front-coded key. Every key in a restart block references the restart key
// directly, so a hit is exactly two bounded byte comparisons.
func (v *IndexTermLeafView) exactKeyAt(index int, key []byte) bool {
	if index < 0 || index >= int(v.termCount) {
		return false
	}
	record := v.descriptor(index)
	keyLength := int(binary.LittleEndian.Uint16(record[4:6]))
	if len(key) != keyLength {
		return false
	}
	restart := index - index%IndexTermLeafRestartInterval
	if index == restart {
		suffixAt := int(binary.LittleEndian.Uint16(record[0:2]))
		return bytes.Equal(
			key,
			v.encoded[int(v.keyAt)+suffixAt:int(v.keyAt)+suffixAt+keyLength],
		)
	}
	return v.exactNonRestartKeyAt(key, record, restart)
}

// Keeping non-restart decoding out of exactKeyAt lets the dominant restart
// probe inline into the fused equality path as one complete bytes comparison.
func (v *IndexTermLeafView) exactNonRestartKeyAt(
	key, record []byte,
	restart int,
) bool {
	keyLength := len(key)
	restartRecord := v.descriptor(restart)
	restartAt := int(binary.LittleEndian.Uint16(restartRecord[0:2]))
	restartLength := int(binary.LittleEndian.Uint16(restartRecord[4:6]))
	shared := int(binary.LittleEndian.Uint16(record[6:8]))
	suffixAt := int(binary.LittleEndian.Uint16(record[0:2]))
	suffixLength := int(binary.LittleEndian.Uint16(record[8:10]))
	if shared > restartLength || shared+suffixLength != keyLength {
		return false
	}
	suffix := v.encoded[int(v.keyAt)+suffixAt : int(v.keyAt)+suffixAt+suffixLength]
	switch suffixLength {
	case 0:
	case 1:
		if key[shared] != suffix[0] {
			return false
		}
	case 2:
		if binary.LittleEndian.Uint16(key[shared:]) !=
			binary.LittleEndian.Uint16(suffix) {
			return false
		}
	case 4:
		if binary.LittleEndian.Uint32(key[shared:]) !=
			binary.LittleEndian.Uint32(suffix) {
			return false
		}
	case 8:
		if binary.LittleEndian.Uint64(key[shared:]) !=
			binary.LittleEndian.Uint64(suffix) {
			return false
		}
	default:
		if !bytes.Equal(key[shared:], suffix) {
			return false
		}
	}
	return bytes.Equal(
		key[:shared],
		v.encoded[int(v.keyAt)+restartAt:int(v.keyAt)+restartAt+shared],
	)
}

// Keeping the uncommon maximum-key scratch in a separate frame prevents every
// ordinary equality probe from clearing four KiB merely because the format
// admits a four-KiB canonical tuple.
func (v IndexTermLeafView) compareLongKeyAt(index int, key []byte) int {
	var scratch [IndexTermMaxKeyBytes]byte
	candidate, ok := v.reconstructKey(index, scratch[:0])
	if !ok {
		return 1
	}
	return bytes.Compare(candidate, key)
}

func commonIndexTermLeafPrefix(left, right []byte) int {
	n := min(len(left), len(right))
	i := 0
	for i < n && left[i] == right[i] {
		i++
	}
	return i
}

func putIndexTermLeafUvarint(dst []byte, value uint64) int {
	i := 0
	for value >= 0x80 {
		dst[i] = byte(value) | 0x80
		value >>= 7
		i++
	}
	dst[i] = byte(value)
	return i + 1
}

func indexTermLeafUvarintBytes(value uint64) int {
	bytes := 1
	for value >= 0x80 {
		value >>= 7
		bytes++
	}
	return bytes
}

func indexTermLeafAlign8Padding(position int) int {
	return -position & 7
}

func indexTermLeafEqualitySlots(terms int) int {
	slots := 2
	for terms*4 > slots*3 {
		slots <<= 1
	}
	return slots
}

func indexTermLeafChecksum(encoded []byte) uint32 {
	checksum := crc32.Update(0, indexTermLeafCRC, encoded[:28])
	var zero [4]byte
	checksum = crc32.Update(checksum, indexTermLeafCRC, zero[:])
	return crc32.Update(checksum, indexTermLeafCRC, encoded[32:])
}

func indexTermLeafCorrupt(what string) error {
	return fmt.Errorf("%w: %s", ErrIndexTermLeafCorrupt, what)
}
