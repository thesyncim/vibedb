package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	snapshotArtifactFormat             = uint16(1)
	snapshotArtifactHeaderFixedBytes   = 64
	snapshotArtifactChunkHeaderBytes   = 96
	snapshotArtifactRelationBytes      = 128
	snapshotArtifactRelationFixedBytes = 8
	snapshotArtifactFooterBytes        = 240
	snapshotArtifactChecksumBytes      = sha256.Size
	snapshotArtifactRowHeaderBytes     = 8

	// DefaultSnapshotArtifactChunkBytes bounds the ordinary retained transfer
	// buffer. A single larger row is emitted alone from borrowed row segments,
	// up to the fixed row bound.
	DefaultSnapshotArtifactChunkBytes = 4 << 20
	// MinSnapshotArtifactChunkBytes prevents pathological framing overhead.
	MinSnapshotArtifactChunkBytes = 4 << 10
	// MaxSnapshotArtifactChunkBytes is one maximum-sized raw row frame. The
	// verifier refuses larger declared payloads before allocating its buffer.
	MaxSnapshotArtifactChunkBytes = replication.MaxMutationKeyBytes +
		MaxTransitionCaptureRecordBytes + snapshotArtifactRowHeaderBytes
	// SnapshotArtifactFooterBytes is the fixed final certificate frame. Cursor
	// persistence intentionally stops immediately before this frame: the footer
	// authenticates completion but carries no mutable receive progress.
	SnapshotArtifactFooterBytes    = snapshotArtifactFooterBytes
	maxSnapshotArtifactHeaderBytes = snapshotArtifactHeaderFixedBytes +
		MaxStateEnvelopeBytes + replication.MaxCollectionBytes + snapshotArtifactChecksumBytes +
		sha256.Size + replication.MaxRelationsPerBundle*(snapshotArtifactRelationFixedBytes+replication.MaxIdentityBytes)
)

// MaxTransitionCaptureRecordBytes is the global bound for one compact capture
// record: a replicated command payload plus one fixed 56-byte before witness
// per mutation and the 248-byte record envelope. Collection-specific limits
// are normally much smaller and remain enforced by their durable target.
const MaxTransitionCaptureRecordBytes = replication.MaxCommandBytes +
	MaxDistinctMutations*56 + 248

const (
	// SnapshotArtifactSystem identifies raw hidden system rows.
	SnapshotArtifactSystem SnapshotArtifactCollection = 1
	// SnapshotArtifactUser identifies raw user-collection rows.
	SnapshotArtifactUser SnapshotArtifactCollection = 2
	// SnapshotArtifactCapture identifies raw private transition-capture rows.
	SnapshotArtifactCapture SnapshotArtifactCollection = 3
)

var (
	snapshotArtifactHeaderMagic   = [8]byte{'V', 'D', 'B', 'S', 'N', 'A', 'P', 0}
	snapshotArtifactChunkMagic    = [8]byte{'V', 'D', 'B', 'S', 'C', 'H', 'K', 0}
	snapshotArtifactRelationMagic = [8]byte{'V', 'D', 'B', 'S', 'R', 'E', 'L', 0}
	snapshotArtifactFooterMagic   = [8]byte{'V', 'D', 'B', 'S', 'E', 'N', 'D', 0}
	snapshotArtifactHeaderDomain  = []byte(
		"vibedb/replicated-state/snapshot-artifact-header\x00",
	)
	snapshotArtifactChunkDomain = []byte(
		"vibedb/replicated-state/snapshot-artifact-chunk\x00",
	)
	snapshotArtifactRelationDomain = []byte(
		"vibedb/replicated-state/snapshot-artifact-relation\x00",
	)
	snapshotArtifactFooterDomain = []byte(
		"vibedb/replicated-state/snapshot-artifact-footer\x00",
	)
	snapshotArtifactCaptureImageDomain = []byte(
		"vibedb/replicated-state/snapshot-artifact-capture-image\x00",
	)
)

// SnapshotArtifactCollection identifies one collection without putting its
// name in every row frame. Zero and unknown values are invalid.
type SnapshotArtifactCollection uint8

// SnapshotArtifactOptions controls deterministic chunk packing and optional
// caller-owned workspace. Zero TargetChunkBytes selects the default. Rows are
// never split across logical chunks. When PayloadBuffer is non-nil, its
// capacity must cover the target and its contents are borrowed and overwritten
// for the call. A larger exceptional row is hashed and written directly from
// its borrowed key and value without entering this buffer.
type SnapshotArtifactOptions struct {
	TargetChunkBytes int
	PayloadBuffer    []byte
}

// ValidateSnapshotArtifactOptions validates deterministic artifact framing
// bounds without opening a snapshot or writing output.
func ValidateSnapshotArtifactOptions(options SnapshotArtifactOptions) error {
	target, err := normalizeSnapshotArtifactOptions(options)
	if err != nil {
		return err
	}
	if options.PayloadBuffer != nil && cap(options.PayloadBuffer) < target {
		return fmt.Errorf(
			"%w: payload-buffer capacity %d below target %d",
			ErrSnapshotArtifactBound,
			cap(options.PayloadBuffer),
			target,
		)
	}
	return nil
}

// RequiredSnapshotArtifactPayloadCapacity validates one collection's frozen
// key and document bounds and returns the fixed aggregate payload capacity.
// Exceptional rows above that target are streamed directly and cannot grow the
// aggregate. Zero targetChunkBytes selects the default artifact target.
func RequiredSnapshotArtifactPayloadCapacity(
	targetChunkBytes int,
	maxKeyBytes int,
	maxDocumentBytes int,
) (int, error) {
	target, err := normalizeSnapshotArtifactOptions(SnapshotArtifactOptions{
		TargetChunkBytes: targetChunkBytes,
	})
	if err != nil {
		return 0, err
	}
	if maxKeyBytes <= 0 || maxKeyBytes > replication.MaxMutationKeyBytes ||
		maxDocumentBytes <= 0 || maxDocumentBytes > replication.MaxMutationValueBytes {
		return 0, fmt.Errorf(
			"%w: collection row bounds %d/%d",
			ErrSnapshotArtifactBound,
			maxKeyBytes,
			maxDocumentBytes,
		)
	}
	return target, nil
}

// SnapshotArtifactCheckpoint is emitted after one complete chunk has passed
// its hash-chain and row-frame checks. EndOffset is the exact byte position at
// the end of the chunk, suitable for a durable receiver checkpoint.
type SnapshotArtifactCheckpoint struct {
	Sequence     uint64
	Collection   SnapshotArtifactCollection
	Relation     replication.RelationID
	Rows         uint64
	PayloadBytes uint64
	EndOffset    uint64
	Digest       [sha256.Size]byte
}

// SnapshotArtifactCallbacks consume verified, borrowed rows and durable chunk
// boundaries. BeginChunk is called after the entire chunk passes its digest but
// before any row is exposed. Row bytes remain valid only until Row returns.
// Chunk is called after every row has been accepted and receives the next
// immutable resume cursor; it must return only after the corresponding row
// effects and cursor are durably ordered. PayloadBuffer is optional caller-owned
// workspace and is overwritten for the call. Borrowed key/value/payload bytes
// are read-only. Row and Rows are mutually exclusive. Capacity through
// MaxSnapshotArtifactChunkBytes prevents verifier growth for every valid
// artifact.
type SnapshotArtifactCallbacks struct {
	BeginChunk    func(checkpoint SnapshotArtifactCheckpoint) error
	Row           func(collection SnapshotArtifactCollection, key, value []byte) error
	Rows          func(checkpoint SnapshotArtifactCheckpoint, rows SnapshotArtifactRows) error
	Chunk         func(checkpoint SnapshotArtifactCheckpoint, next *SnapshotArtifactCursor) error
	PayloadBuffer []byte
}

// SnapshotArtifactRows is one complete structurally verified chunk. Iterator
// borrows its payload only for the Rows callback that receives it.
type SnapshotArtifactRows struct {
	collection SnapshotArtifactCollection
	relation   replication.RelationID
	payload    []byte
	rows       uint64
}

// Collection returns the collection shared by every row in the chunk.
func (r SnapshotArtifactRows) Collection() SnapshotArtifactCollection { return r.collection }

// Relation returns the dense relation ID for user chunks and zero for hidden
// system or transition-capture chunks.
func (r SnapshotArtifactRows) Relation() replication.RelationID { return r.relation }

// Len returns the exact row count in the chunk.
func (r SnapshotArtifactRows) Len() uint64 { return r.rows }

// Iterator returns a zero-allocation forward iterator over borrowed rows.
func (r SnapshotArtifactRows) Iterator() SnapshotArtifactRowIterator {
	return SnapshotArtifactRowIterator{payload: r.payload, remaining: r.rows}
}

// SnapshotArtifactRowIterator walks one already verified chunk. The zero value
// is empty. Returned key/value bytes are borrowed until the Rows callback ends.
type SnapshotArtifactRowIterator struct {
	payload   []byte
	cursor    int
	remaining uint64
}

// Next returns the next borrowed row. ok is false after the declared count.
func (i *SnapshotArtifactRowIterator) Next() (key, value []byte, ok bool) {
	if i == nil || i.remaining == 0 {
		return nil, nil, false
	}
	keyBytes := int(binary.LittleEndian.Uint32(i.payload[i.cursor : i.cursor+4]))
	valueBytes := int(binary.LittleEndian.Uint32(i.payload[i.cursor+4 : i.cursor+8]))
	i.cursor += snapshotArtifactRowHeaderBytes
	keyEnd := i.cursor + keyBytes
	valueEnd := keyEnd + valueBytes
	key, value = i.payload[i.cursor:keyEnd], i.payload[keyEnd:valueEnd]
	i.cursor = valueEnd
	i.remaining--
	return key, value, true
}

// SnapshotArtifactCursor is the exact verified prefix from which a receiver
// may request the next artifact range. Its representation is opaque so callers
// cannot accidentally advance past durable row effects. Use
// AppendSnapshotArtifactCursor and OpenSnapshotArtifactCursor for persistence.
type SnapshotArtifactCursor struct {
	manifest              SnapshotArtifactManifest
	expectedStateDocument []byte
	nextSequence          uint64
	encodedBytes          uint64
	previousDigest        [sha256.Size]byte
	previousKey           [replication.MaxMutationKeyBytes]byte
	previousKeyBytes      uint16
	currentCollection     SnapshotArtifactCollection
	nextRelation          replication.RelationID
	stateRowSeen          bool
	routeGateRows         uint64
	captureImageDigest    [sha256.Size]byte
}

// Offset returns the exact byte offset at which the next range begins.
func (c *SnapshotArtifactCursor) Offset() uint64 {
	if c == nil {
		return 0
	}
	return c.encodedBytes
}

// NextSequence returns the next required chunk sequence.
func (c *SnapshotArtifactCursor) NextSequence() uint64 {
	if c == nil {
		return 0
	}
	return c.nextSequence
}

// PreviousDigest returns the hash-chain predecessor required by the next
// chunk. For a header-only cursor this is the header digest.
func (c *SnapshotArtifactCursor) PreviousDigest() [sha256.Size]byte {
	if c == nil {
		return [sha256.Size]byte{}
	}
	return c.previousDigest
}

// PrefixManifest returns a detached manifest for the verified prefix. Digest
// and EncodedBytes remain zero until the artifact footer is verified.
func (c *SnapshotArtifactCursor) PrefixManifest() SnapshotArtifactManifest {
	if c == nil {
		return SnapshotArtifactManifest{}
	}
	return cloneSnapshotArtifactManifest(c.manifest)
}

// SnapshotArtifactManifest certifies one coherent system/user/capture image. The
// collection name is detached raw bytes; State is the canonical publication
// embedded in both the header and hidden system row.
type SnapshotArtifactManifest struct {
	State          State
	UserCollection []byte
	// Bundle identifies one coherent fixed relation set. A zero chunk target is a
	// compact no-copy certificate; a nonzero target is one streamed bundle.
	Bundle                 bool
	RelationManifestDigest [sha256.Size]byte
	Relations              []SnapshotArtifactRelation
	// Seeded distinguishes a compact no-copy snapshot-base manifest from a
	// streamed artifact manifest. Seeded manifests carry no artifact geometry;
	// they bind the already durable user image directly.
	Seeded           bool
	TargetChunkBytes uint32
	Chunks           uint64
	SystemRows       uint64
	UserRows         uint64
	CaptureRows      uint64
	PayloadBytes     uint64
	EncodedBytes     uint64
	HeaderDigest     [sha256.Size]byte
	LastChunkDigest  [sha256.Size]byte
	// ImageDigest is the canonical validated user image computed while the
	// artifact's user rows are already being streamed.
	ImageDigest [sha256.Size]byte
	// CaptureImageDigest authenticates the exact opaque capture key/value image.
	CaptureImageDigest [sha256.Size]byte
	Digest             [sha256.Size]byte
}

// SnapshotArtifactRelation is one dense relation image committed by a compact
// bundle certificate. Collection is detached cold metadata. Apply never
// resolves it; Relation addresses the already-opened handle.
type SnapshotArtifactRelation struct {
	Relation    replication.RelationID
	Kind        RelationKind
	Collection  []byte
	Rows        uint64
	ImageDigest [sha256.Size]byte
}

// Clone returns an independently owned manifest suitable for retention across
// caller mutation, asynchronous transfer, or activation recovery.
func (m SnapshotArtifactManifest) Clone() SnapshotArtifactManifest {
	return cloneSnapshotArtifactManifest(m)
}

type snapshotArtifactWriter struct {
	w              io.Writer
	target         int
	payload        []byte
	collection     SnapshotArtifactCollection
	relation       replication.RelationID
	relationKind   RelationKind
	locatorCount   uint8
	bundle         bool
	chunkRows      uint32
	chunks         uint64
	systemRows     uint64
	userRows       uint64
	captureRows    uint64
	payloadBytes   uint64
	encodedBytes   uint64
	headerDigest   [sha256.Size]byte
	previousDigest [sha256.Size]byte
	chunkHeader    [snapshotArtifactChunkHeaderBytes]byte
	rowHeader      [snapshotArtifactRowHeaderBytes]byte
	chunkDigest    [sha256.Size]byte
	relationRecord [snapshotArtifactRelationBytes]byte
	image          *canonicalImageHasher
	captureImage   [sha256.Size]byte
}

type snapshotArtifactTransactionScratch struct {
	participants []distributedtxn.ParticipantRef
	identities   []byte
}

// WriteSnapshotArtifact writes a deterministic, bounded-memory artifact for
// snapshot. It does not close snapshot or w. Once w reports an error, the
// caller must discard or truncate the partial artifact.
func WriteSnapshotArtifact(
	w io.Writer,
	snapshot *ReadSnapshot,
	options SnapshotArtifactOptions,
) (SnapshotArtifactManifest, error) {
	if w == nil || snapshot == nil {
		return SnapshotArtifactManifest{}, fmt.Errorf("%w: nil writer or snapshot", ErrSnapshotArtifact)
	}
	target, err := normalizeSnapshotArtifactOptions(options)
	if err != nil {
		return SnapshotArtifactManifest{}, err
	}
	payload := options.PayloadBuffer
	if payload == nil {
		payload = make([]byte, 0, target)
	} else {
		if cap(payload) < target {
			return SnapshotArtifactManifest{}, fmt.Errorf(
				"%w: payload-buffer capacity %d below target %d",
				ErrSnapshotArtifactBound, cap(payload), target,
			)
		}
		payload = payload[:0]
	}
	stateEnvelope, err := AppendState(nil, snapshot.state)
	if err != nil {
		return SnapshotArtifactManifest{}, fmt.Errorf("%w: state envelope: %v", ErrSnapshotArtifact, err)
	}
	if len(snapshot.relations) == 0 {
		return SnapshotArtifactManifest{}, ErrInconsistentSnapshot
	}
	bundle := len(snapshot.relations) > 1
	relations := make([]SnapshotArtifactRelation, len(snapshot.relations))
	for i := range snapshot.relations {
		relation := &snapshot.relations[i]
		relations[i] = SnapshotArtifactRelation{
			Relation: relation.id, Kind: relation.kind, Collection: []byte(relation.name),
		}
	}
	headerRelations := relations
	headerManifestDigest := snapshot.manifestDigest
	if !bundle {
		headerRelations = nil
		headerManifestDigest = [sha256.Size]byte{}
	}
	header, headerDigest, err := makeSnapshotArtifactHeaderForRelations(
		stateEnvelope, snapshot.userName, target, headerManifestDigest, headerRelations, bundle,
	)
	if err != nil {
		return SnapshotArtifactManifest{}, err
	}
	if err := writeSnapshotArtifactBytes(w, header); err != nil {
		return SnapshotArtifactManifest{}, err
	}
	writer := snapshotArtifactWriter{
		w: w, target: target, payload: payload,
		encodedBytes: uint64(len(header)), headerDigest: headerDigest,
		previousDigest: headerDigest, bundle: bundle,
	}
	writer.captureImage = snapshotArtifactEmptyCaptureImageDigest()
	if err := writer.writeCollection(SnapshotArtifactSystem, snapshot.RangeSystem); err != nil {
		return SnapshotArtifactManifest{}, err
	}
	for i := range snapshot.relations {
		relation := &snapshot.relations[i]
		user, ok := snapshot.Relation(relation.id)
		if !ok || user == nil {
			return SnapshotArtifactManifest{}, ErrInconsistentSnapshot
		}
		writer.relation = 0
		if bundle {
			writer.relation = relation.id
		}
		writer.relationKind = relation.kind
		writer.locatorCount = relation.globalIndex.LocatorCount
		writer.image, err = newCanonicalImageHasher(
			relation.name, relation.target.Validation,
			relation.target.ValidationDigest, relation.target.Validator,
		)
		if err != nil {
			return SnapshotArtifactManifest{}, err
		}
		before := writer.userRows
		if err := writer.writeCollection(SnapshotArtifactUser, user.RangeRaw); err != nil {
			return SnapshotArtifactManifest{}, err
		}
		relations[i].Rows = writer.userRows - before
		relations[i].ImageDigest = writer.image.sum()
		if bundle {
			if err := writer.writeRelationCertificate(relations[i]); err != nil {
				return SnapshotArtifactManifest{}, err
			}
		}
	}
	writer.relation = 0
	writer.relationKind = 0
	writer.locatorCount = 0
	if err := writer.writeCollection(SnapshotArtifactCapture, snapshot.RangeCapture); err != nil {
		return SnapshotArtifactManifest{}, err
	}
	imageDigest := canonicalBundleImageDigest(relations)
	captureImageDigest := writer.captureImage
	digest, err := writer.writeFooter(imageDigest, captureImageDigest)
	if err != nil {
		return SnapshotArtifactManifest{}, err
	}
	return SnapshotArtifactManifest{
		State: cloneState(snapshot.state), UserCollection: []byte(snapshot.userName),
		Bundle: bundle, Relations: func() []SnapshotArtifactRelation {
			if !bundle {
				return nil
			}
			return relations
		}(),
		RelationManifestDigest: func() [sha256.Size]byte {
			if bundle {
				return snapshot.manifestDigest
			}
			return [sha256.Size]byte{}
		}(),
		TargetChunkBytes: uint32(target), Chunks: writer.chunks,
		SystemRows: writer.systemRows, UserRows: writer.userRows, CaptureRows: writer.captureRows,
		PayloadBytes: writer.payloadBytes, EncodedBytes: writer.encodedBytes,
		HeaderDigest: headerDigest, LastChunkDigest: writer.previousDigest,
		ImageDigest: imageDigest, CaptureImageDigest: captureImageDigest, Digest: digest,
	}, nil
}

func normalizeSnapshotArtifactOptions(options SnapshotArtifactOptions) (int, error) {
	target := options.TargetChunkBytes
	if target == 0 {
		target = DefaultSnapshotArtifactChunkBytes
	}
	if target < MinSnapshotArtifactChunkBytes || target > MaxSnapshotArtifactChunkBytes {
		return 0, fmt.Errorf("%w: target chunk bytes %d", ErrSnapshotArtifactBound, target)
	}
	return target, nil
}

func makeSnapshotArtifactHeader(
	stateEnvelope []byte,
	userName string,
	target int,
) ([]byte, [sha256.Size]byte, error) {
	return makeSnapshotArtifactHeaderForRelations(
		stateEnvelope, userName, target, [sha256.Size]byte{}, nil, false,
	)
}

func makeSnapshotArtifactHeaderForRelations(
	stateEnvelope []byte,
	userName string,
	target int,
	manifestDigest [sha256.Size]byte,
	relations []SnapshotArtifactRelation,
	bundle bool,
) ([]byte, [sha256.Size]byte, error) {
	if len(stateEnvelope) == 0 || len(stateEnvelope) > MaxStateEnvelopeBytes ||
		len(userName) == 0 || len(userName) > replication.MaxCollectionBytes {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: header fields", ErrSnapshotArtifactBound)
	}
	descriptorBytes := 0
	if bundle {
		if len(relations) < 2 || len(relations) > replication.MaxRelationsPerBundle ||
			manifestDigest == ([sha256.Size]byte{}) {
			return nil, [sha256.Size]byte{}, fmt.Errorf("%w: relation header", ErrSnapshotArtifactBound)
		}
		descriptorBytes = sha256.Size
		for i := range relations {
			relation := &relations[i]
			if relation.Relation != replication.RelationID(i+1) ||
				(relation.Kind != RelationJSON && relation.Kind != RelationGlobalIndex) ||
				len(relation.Collection) == 0 || len(relation.Collection) > replication.MaxIdentityBytes ||
				!utf8.Valid(relation.Collection) || bytes.IndexByte(relation.Collection, 0) >= 0 ||
				bytes.Equal(relation.Collection, []byte(systemCollectionName)) ||
				relation.Rows != 0 || relation.ImageDigest != ([sha256.Size]byte{}) {
				return nil, [sha256.Size]byte{}, fmt.Errorf("%w: relation header %d", ErrSnapshotArtifact, i+1)
			}
			for prior := 0; prior < i; prior++ {
				if bytes.Equal(relation.Collection, relations[prior].Collection) {
					return nil, [sha256.Size]byte{}, fmt.Errorf("%w: duplicate relation header", ErrSnapshotArtifact)
				}
			}
			descriptorBytes += snapshotArtifactRelationFixedBytes + len(relation.Collection)
		}
		if !bytes.Equal(relations[0].Collection, []byte(userName)) {
			return nil, [sha256.Size]byte{}, fmt.Errorf("%w: primary relation", ErrSnapshotArtifact)
		}
	} else if len(relations) != 0 || manifestDigest != ([sha256.Size]byte{}) {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: singleton relation header", ErrSnapshotArtifact)
	}
	total := snapshotArtifactHeaderFixedBytes + len(stateEnvelope) + len(userName) +
		descriptorBytes + snapshotArtifactChecksumBytes
	if total > maxSnapshotArtifactHeaderBytes || uint64(total) > math.MaxUint32 {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: header bytes", ErrSnapshotArtifactBound)
	}
	header := make([]byte, total)
	copy(header[0:8], snapshotArtifactHeaderMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], snapshotArtifactFormat)
	binary.LittleEndian.PutUint16(header[10:12], snapshotArtifactHeaderFixedBytes)
	binary.LittleEndian.PutUint32(header[12:16], uint32(total))
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(stateEnvelope)))
	binary.LittleEndian.PutUint16(header[20:22], uint16(len(userName)))
	if bundle {
		binary.LittleEndian.PutUint16(header[22:24], 1)
		binary.LittleEndian.PutUint16(header[32:34], uint16(len(relations)))
		binary.LittleEndian.PutUint32(header[36:40], uint32(descriptorBytes))
	}
	binary.LittleEndian.PutUint32(header[24:28], uint32(target))
	binary.LittleEndian.PutUint32(header[28:32], MaxSnapshotArtifactChunkBytes)
	cursor := snapshotArtifactHeaderFixedBytes
	cursor += copy(header[cursor:], stateEnvelope)
	cursor += copy(header[cursor:], userName)
	if bundle {
		cursor += copy(header[cursor:], manifestDigest[:])
		for i := range relations {
			relation := &relations[i]
			binary.LittleEndian.PutUint16(header[cursor:cursor+2], uint16(relation.Relation))
			header[cursor+2] = byte(relation.Kind)
			binary.LittleEndian.PutUint16(header[cursor+4:cursor+6], uint16(len(relation.Collection)))
			cursor += snapshotArtifactRelationFixedBytes
			cursor += copy(header[cursor:], relation.Collection)
		}
	}
	if cursor != len(header)-sha256.Size {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: header geometry", ErrSnapshotArtifact)
	}
	digest := snapshotArtifactDigest(snapshotArtifactHeaderDomain, header[:len(header)-sha256.Size])
	copy(header[len(header)-sha256.Size:], digest[:])
	return header, digest, nil
}

func (w *snapshotArtifactWriter) writeCollection(
	collection SnapshotArtifactCollection,
	rangeRows func(func(key, value []byte) error) error,
) error {
	if err := w.flush(); err != nil {
		return err
	}
	w.collection = collection
	err := rangeRows(func(key, value []byte) error {
		if collection == SnapshotArtifactUser {
			if w.relationKind == RelationGlobalIndex &&
				!validGlobalIndexLocator(value, w.locatorCount) {
				return ErrSchemaProfile
			}
			if err := w.image.add(key, value); err != nil {
				return err
			}
		}
		if collection == SnapshotArtifactCapture {
			w.captureImage = snapshotArtifactCaptureImageNext(w.captureImage, key, value)
		}
		rowBytes, ok := snapshotArtifactRowBytes(collection, key, value)
		if !ok {
			return fmt.Errorf("%w: row", ErrSnapshotArtifactBound)
		}
		if len(w.payload) != 0 && rowBytes > w.target-len(w.payload) {
			if err := w.flush(); err != nil {
				return err
			}
		}
		if rowBytes > w.target {
			return w.writeExceptionalRow(key, value, rowBytes)
		}
		w.payload = binary.LittleEndian.AppendUint32(w.payload, uint32(len(key)))
		w.payload = binary.LittleEndian.AppendUint32(w.payload, uint32(len(value)))
		w.payload = append(w.payload, key...)
		w.payload = append(w.payload, value...)
		if w.chunkRows == math.MaxUint32 {
			return fmt.Errorf("%w: rows per chunk", ErrSnapshotArtifactBound)
		}
		w.chunkRows++
		return nil
	})
	if err != nil {
		return err
	}
	return w.flush()
}

func snapshotArtifactRowBytes(collection SnapshotArtifactCollection, key, value []byte) (int, bool) {
	maxValueBytes := snapshotArtifactMaxValueBytes(collection)
	if len(key) == 0 || len(key) > replication.MaxMutationKeyBytes ||
		len(value) == 0 || uint64(len(value)) > maxValueBytes {
		return 0, false
	}
	rowBytes := snapshotArtifactRowHeaderBytes + len(key) + len(value)
	return rowBytes, rowBytes <= MaxSnapshotArtifactChunkBytes
}

func (w *snapshotArtifactWriter) flush() error {
	if len(w.payload) == 0 {
		if w.chunkRows != 0 {
			return fmt.Errorf("%w: empty buffered chunk", ErrSnapshotArtifact)
		}
		return nil
	}
	total, err := w.prepareChunk(len(w.payload), w.chunkRows)
	if err != nil {
		return err
	}
	w.chunkDigest = snapshotArtifactDigestParts(
		snapshotArtifactChunkDomain,
		w.chunkHeader[:],
		w.payload,
	)
	if err := writeSnapshotArtifactBytes(w.w, w.chunkHeader[:]); err != nil {
		return err
	}
	if err := writeSnapshotArtifactBytes(w.w, w.payload); err != nil {
		return err
	}
	if err := writeSnapshotArtifactBytes(w.w, w.chunkDigest[:]); err != nil {
		return err
	}
	w.commitChunk(len(w.payload), w.chunkRows, total, w.chunkDigest)
	w.payload = w.payload[:0]
	w.chunkRows = 0
	return nil
}

func (w *snapshotArtifactWriter) writeExceptionalRow(
	key []byte,
	value []byte,
	rowBytes int,
) error {
	actualRowBytes, validRow := snapshotArtifactRowBytes(w.collection, key, value)
	if len(w.payload) != 0 || w.chunkRows != 0 || rowBytes <= w.target ||
		!validRow || actualRowBytes != rowBytes {
		return fmt.Errorf("%w: exceptional row state", ErrSnapshotArtifact)
	}
	total, err := w.prepareChunk(rowBytes, 1)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(w.rowHeader[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(w.rowHeader[4:8], uint32(len(value)))
	h := sha256.New()
	_, _ = h.Write(snapshotArtifactChunkDomain)
	_, _ = h.Write(w.chunkHeader[:])
	_, _ = h.Write(w.rowHeader[:])
	_, _ = h.Write(key)
	_, _ = h.Write(value)
	_ = h.Sum(w.chunkDigest[:0])
	if err := writeSnapshotArtifactBytes(w.w, w.chunkHeader[:]); err != nil {
		return err
	}
	if err := writeSnapshotArtifactBytes(w.w, w.rowHeader[:]); err != nil {
		return err
	}
	if err := writeSnapshotArtifactBytes(w.w, key); err != nil {
		return err
	}
	if err := writeSnapshotArtifactBytes(w.w, value); err != nil {
		return err
	}
	if err := writeSnapshotArtifactBytes(w.w, w.chunkDigest[:]); err != nil {
		return err
	}
	w.commitChunk(rowBytes, 1, total, w.chunkDigest)
	return nil
}

func (w *snapshotArtifactWriter) writeRelationCertificate(
	relation SnapshotArtifactRelation,
) error {
	if !w.bundle || relation.Relation == 0 || relation.Relation != w.relation ||
		relation.Kind != w.relationKind || relation.ImageDigest == ([sha256.Size]byte{}) ||
		len(w.payload) != 0 || w.chunkRows != 0 ||
		w.encodedBytes > math.MaxUint64-snapshotArtifactRelationBytes {
		return fmt.Errorf("%w: relation certificate state", ErrSnapshotArtifact)
	}
	clear(w.relationRecord[:])
	copy(w.relationRecord[0:8], snapshotArtifactRelationMagic[:])
	binary.LittleEndian.PutUint16(w.relationRecord[8:10], snapshotArtifactFormat)
	binary.LittleEndian.PutUint16(w.relationRecord[10:12], snapshotArtifactRelationBytes)
	binary.LittleEndian.PutUint32(w.relationRecord[12:16], snapshotArtifactRelationBytes)
	binary.LittleEndian.PutUint16(w.relationRecord[16:18], uint16(relation.Relation))
	w.relationRecord[18] = byte(relation.Kind)
	binary.LittleEndian.PutUint64(w.relationRecord[24:32], relation.Rows)
	copy(w.relationRecord[32:64], relation.ImageDigest[:])
	copy(w.relationRecord[64:96], w.previousDigest[:])
	digest := snapshotArtifactDigest(snapshotArtifactRelationDomain, w.relationRecord[:96])
	copy(w.relationRecord[96:128], digest[:])
	if err := writeSnapshotArtifactBytes(w.w, w.relationRecord[:]); err != nil {
		return err
	}
	w.encodedBytes += snapshotArtifactRelationBytes
	w.previousDigest = digest
	return nil
}

func (w *snapshotArtifactWriter) prepareChunk(
	payloadBytes int,
	rows uint32,
) (int, error) {
	clear(w.chunkHeader[:])
	if rows == 0 || payloadBytes <= 0 ||
		payloadBytes > MaxSnapshotArtifactChunkBytes || w.chunks == math.MaxUint64 {
		return 0, fmt.Errorf("%w: chunk counters", ErrSnapshotArtifactBound)
	}
	rowCount := uint64(rows)
	switch w.collection {
	case SnapshotArtifactSystem:
		if w.systemRows > math.MaxUint64-rowCount {
			return 0, fmt.Errorf("%w: system rows", ErrSnapshotArtifactBound)
		}
	case SnapshotArtifactUser:
		if w.userRows > math.MaxUint64-rowCount {
			return 0, fmt.Errorf("%w: user rows", ErrSnapshotArtifactBound)
		}
	case SnapshotArtifactCapture:
		if w.captureRows > math.MaxUint64-rowCount {
			return 0, fmt.Errorf("%w: capture rows", ErrSnapshotArtifactBound)
		}
	default:
		return 0, fmt.Errorf("%w: chunk collection", ErrSnapshotArtifactBound)
	}
	total := snapshotArtifactChunkHeaderBytes + payloadBytes + sha256.Size
	if uint64(total) > math.MaxUint32 ||
		w.payloadBytes > math.MaxUint64-uint64(payloadBytes) ||
		w.encodedBytes > math.MaxUint64-uint64(total) {
		return 0, fmt.Errorf("%w: artifact counters", ErrSnapshotArtifactBound)
	}
	copy(w.chunkHeader[0:8], snapshotArtifactChunkMagic[:])
	binary.LittleEndian.PutUint16(w.chunkHeader[8:10], snapshotArtifactFormat)
	binary.LittleEndian.PutUint16(w.chunkHeader[10:12], snapshotArtifactChunkHeaderBytes)
	binary.LittleEndian.PutUint32(w.chunkHeader[12:16], uint32(total))
	binary.LittleEndian.PutUint64(w.chunkHeader[16:24], w.chunks)
	w.chunkHeader[24] = byte(w.collection)
	if w.collection == SnapshotArtifactUser {
		binary.LittleEndian.PutUint16(w.chunkHeader[26:28], uint16(w.relation))
	}
	binary.LittleEndian.PutUint32(w.chunkHeader[28:32], rows)
	binary.LittleEndian.PutUint32(w.chunkHeader[32:36], uint32(payloadBytes))
	copy(w.chunkHeader[48:80], w.previousDigest[:])
	return total, nil
}

func (w *snapshotArtifactWriter) commitChunk(
	payloadBytes int,
	rows uint32,
	total int,
	digest [sha256.Size]byte,
) {
	if w.collection == SnapshotArtifactSystem {
		w.systemRows += uint64(rows)
	} else if w.collection == SnapshotArtifactUser {
		w.userRows += uint64(rows)
	} else {
		w.captureRows += uint64(rows)
	}
	w.payloadBytes += uint64(payloadBytes)
	w.encodedBytes += uint64(total)
	w.chunks++
	w.previousDigest = digest
}

func (w *snapshotArtifactWriter) writeFooter(
	imageDigest [sha256.Size]byte,
	captureImageDigest [sha256.Size]byte,
) ([sha256.Size]byte, error) {
	if w.encodedBytes > math.MaxUint64-snapshotArtifactFooterBytes {
		return [sha256.Size]byte{}, fmt.Errorf("%w: artifact bytes", ErrSnapshotArtifactBound)
	}
	totalBytes := w.encodedBytes + snapshotArtifactFooterBytes
	footer, digest := makeSnapshotArtifactFooter(
		w.chunks, w.systemRows, w.userRows, w.captureRows, w.payloadBytes, totalBytes,
		w.previousDigest, w.headerDigest, imageDigest, captureImageDigest,
	)
	if err := writeSnapshotArtifactBytes(w.w, footer[:]); err != nil {
		return [sha256.Size]byte{}, err
	}
	w.encodedBytes = totalBytes
	return digest, nil
}

func makeSnapshotArtifactFooter(
	chunks uint64,
	systemRows uint64,
	userRows uint64,
	captureRows uint64,
	payloadBytes uint64,
	totalBytes uint64,
	lastChunkDigest [sha256.Size]byte,
	headerDigest [sha256.Size]byte,
	imageDigest [sha256.Size]byte,
	captureImageDigest [sha256.Size]byte,
) ([snapshotArtifactFooterBytes]byte, [sha256.Size]byte) {
	var footer [snapshotArtifactFooterBytes]byte
	copy(footer[0:8], snapshotArtifactFooterMagic[:])
	binary.LittleEndian.PutUint16(footer[8:10], snapshotArtifactFormat)
	binary.LittleEndian.PutUint16(footer[10:12], snapshotArtifactFooterBytes)
	binary.LittleEndian.PutUint32(footer[12:16], snapshotArtifactFooterBytes)
	binary.LittleEndian.PutUint64(footer[16:24], chunks)
	binary.LittleEndian.PutUint64(footer[24:32], chunks)
	binary.LittleEndian.PutUint64(footer[32:40], systemRows)
	binary.LittleEndian.PutUint64(footer[40:48], userRows)
	binary.LittleEndian.PutUint64(footer[48:56], captureRows)
	binary.LittleEndian.PutUint64(footer[56:64], payloadBytes)
	binary.LittleEndian.PutUint64(footer[64:72], totalBytes)
	copy(footer[72:104], lastChunkDigest[:])
	copy(footer[104:136], headerDigest[:])
	copy(footer[136:168], imageDigest[:])
	copy(footer[168:200], captureImageDigest[:])
	digest := snapshotArtifactDigest(snapshotArtifactFooterDomain, footer[:208])
	copy(footer[208:240], digest[:])
	return footer, digest
}

func snapshotArtifactEncodedBytes(
	headerBytes uint64,
	chunks uint64,
	payloadBytes uint64,
	withFooter bool,
) (uint64, bool) {
	return snapshotArtifactEncodedBytesWithRelations(
		headerBytes, chunks, payloadBytes, 0, withFooter,
	)
}

func snapshotArtifactEncodedBytesWithRelations(
	headerBytes uint64,
	chunks uint64,
	payloadBytes uint64,
	relationCertificates uint64,
	withFooter bool,
) (uint64, bool) {
	const chunkOverhead = uint64(snapshotArtifactChunkHeaderBytes + sha256.Size)
	if headerBytes == 0 || chunks > math.MaxUint64/chunkOverhead ||
		relationCertificates > math.MaxUint64/snapshotArtifactRelationBytes {
		return 0, false
	}
	total := chunks*chunkOverhead + relationCertificates*snapshotArtifactRelationBytes
	if total < chunks*chunkOverhead {
		return 0, false
	}
	if headerBytes > math.MaxUint64-total {
		return 0, false
	}
	total += headerBytes
	if payloadBytes > math.MaxUint64-total {
		return 0, false
	}
	total += payloadBytes
	if withFooter {
		if total > math.MaxUint64-snapshotArtifactFooterBytes {
			return 0, false
		}
		total += snapshotArtifactFooterBytes
	}
	return total, true
}

// VerifySnapshotArtifact streams and verifies one complete artifact. It
// authenticates transfer integrity, canonical framing, row ordering, and the
// exact hidden state row. It does not establish topology authority or validate
// user rows against an application validator; an installer must do both before
// publication.
func VerifySnapshotArtifact(
	r io.Reader,
	callbacks SnapshotArtifactCallbacks,
) (SnapshotArtifactManifest, error) {
	manifest, _, err := ContinueSnapshotArtifact(r, nil, callbacks)
	return manifest, err
}

// ContinueSnapshotArtifact verifies an artifact from its header when cursor is
// nil, or from cursor.Offset when cursor is non-nil. On failure after at least
// one complete chunk, next is the last prefix whose Chunk callback returned
// successfully. The caller may persist it and request the source range at its
// exact offset. A resumed reader must begin with the next chunk magic, not the
// artifact header.
func ContinueSnapshotArtifact(
	r io.Reader,
	cursor *SnapshotArtifactCursor,
	callbacks SnapshotArtifactCallbacks,
) (manifest SnapshotArtifactManifest, next *SnapshotArtifactCursor, resultErr error) {
	if r == nil {
		return SnapshotArtifactManifest{}, cursor, fmt.Errorf("%w: nil reader", ErrSnapshotArtifact)
	}
	if callbacks.Row != nil && callbacks.Rows != nil {
		return SnapshotArtifactManifest{}, cursor,
			fmt.Errorf("%w: row callbacks are mutually exclusive", ErrSnapshotArtifact)
	}
	var current SnapshotArtifactCursor
	if cursor == nil {
		headerManifest, expectedStateDocument, encodedBytes, err := readSnapshotArtifactHeader(r)
		if err != nil {
			return SnapshotArtifactManifest{}, nil, err
		}
		current = SnapshotArtifactCursor{
			manifest: headerManifest, expectedStateDocument: expectedStateDocument,
			encodedBytes: encodedBytes, previousDigest: headerManifest.HeaderDigest,
			currentCollection:  SnapshotArtifactSystem,
			captureImageDigest: snapshotArtifactEmptyCaptureImageDigest(),
		}
		if headerManifest.Bundle {
			current.nextRelation = 1
		}
	} else {
		if err := validateSnapshotArtifactCursor(cursor); err != nil {
			return SnapshotArtifactManifest{}, cursor, err
		}
		current = *cursor
		// A persisted cursor is an immutable verified-prefix capability. Detach
		// its small bounded relation table once per resumed range so speculative
		// chunk accounting cannot mutate the caller's prior cursor.
		current.manifest.Relations = append(
			[]SnapshotArtifactRelation(nil), cursor.manifest.Relations...,
		)
	}
	payload := callbacks.PayloadBuffer[:0]
	if payload == nil {
		payload = make([]byte, 0, min(int(current.manifest.TargetChunkBytes), 64<<10))
	}
	var transactionScratch snapshotArtifactTransactionScratch
	for {
		var magic [8]byte
		if err := readSnapshotArtifactBytes(r, magic[:], "record magic"); err != nil {
			return SnapshotArtifactManifest{}, &current, err
		}
		switch magic {
		case snapshotArtifactChunkMagic:
			var header [snapshotArtifactChunkHeaderBytes]byte
			copy(header[:8], magic[:])
			if err := readSnapshotArtifactBytes(r, header[8:], "chunk header"); err != nil {
				return SnapshotArtifactManifest{}, &current, err
			}
			chunk, payloadBytes, err := validateSnapshotArtifactChunkHeader(
				header[:], current.nextSequence, current.previousDigest,
				current.currentCollection, current.manifest.Bundle,
				current.nextRelation, len(current.manifest.Relations),
			)
			if err != nil {
				return SnapshotArtifactManifest{}, &current, err
			}
			if chunk.Collection != SnapshotArtifactSystem && !current.stateRowSeen {
				return SnapshotArtifactManifest{}, &current,
					fmt.Errorf("%w: user rows precede hidden state", ErrSnapshotArtifact)
			}
			if cap(payload) < payloadBytes {
				payload = make([]byte, payloadBytes)
			} else {
				payload = payload[:payloadBytes]
			}
			if err := readSnapshotArtifactBytes(r, payload, "chunk payload"); err != nil {
				return SnapshotArtifactManifest{}, &current, err
			}
			var storedDigest [sha256.Size]byte
			if err := readSnapshotArtifactBytes(r, storedDigest[:], "chunk digest"); err != nil {
				return SnapshotArtifactManifest{}, &current, err
			}
			wantDigest := snapshotArtifactDigestParts(snapshotArtifactChunkDomain, header[:], payload)
			if storedDigest != wantDigest {
				return SnapshotArtifactManifest{}, &current,
					fmt.Errorf("%w: chunk digest", ErrSnapshotArtifact)
			}
			chunkBytes := uint64(snapshotArtifactChunkHeaderBytes + payloadBytes + sha256.Size)
			if current.manifest.Chunks == math.MaxUint64 ||
				current.nextSequence == math.MaxUint64 ||
				current.encodedBytes > math.MaxUint64-chunkBytes ||
				current.manifest.PayloadBytes > math.MaxUint64-uint64(payloadBytes) {
				return SnapshotArtifactManifest{}, &current,
					fmt.Errorf("%w: artifact counters", ErrSnapshotArtifactBound)
			}
			if chunk.Collection == SnapshotArtifactSystem {
				if current.manifest.SystemRows > math.MaxUint64-chunk.Rows {
					return SnapshotArtifactManifest{}, &current,
						fmt.Errorf("%w: system rows", ErrSnapshotArtifactBound)
				}
			} else if chunk.Collection == SnapshotArtifactUser {
				if current.manifest.UserRows > math.MaxUint64-chunk.Rows {
					return SnapshotArtifactManifest{}, &current,
						fmt.Errorf("%w: user rows", ErrSnapshotArtifactBound)
				}
			} else if current.manifest.CaptureRows > math.MaxUint64-chunk.Rows {
				return SnapshotArtifactManifest{}, &current,
					fmt.Errorf("%w: capture rows", ErrSnapshotArtifactBound)
			}
			checkpoint := SnapshotArtifactCheckpoint{
				Sequence: chunk.Sequence, Collection: chunk.Collection, Relation: chunk.Relation,
				Rows: chunk.Rows, PayloadBytes: uint64(payloadBytes),
				EndOffset: current.encodedBytes + chunkBytes, Digest: storedDigest,
			}
			if callbacks.BeginChunk != nil {
				if err := callbacks.BeginChunk(checkpoint); err != nil {
					return SnapshotArtifactManifest{}, &current, err
				}
			}
			candidate := current
			if chunk.Collection != candidate.currentCollection {
				candidate.currentCollection = chunk.Collection
				candidate.previousKeyBytes = 0
			}
			previousKeyBytes := int(candidate.previousKeyBytes)
			visit := callbacks.Row
			if chunk.Collection == SnapshotArtifactCapture {
				visit = func(collection SnapshotArtifactCollection, key, value []byte) error {
					candidate.captureImageDigest = snapshotArtifactCaptureImageNext(
						candidate.captureImageDigest, key, value,
					)
					if callbacks.Row != nil {
						return callbacks.Row(collection, key, value)
					}
					return nil
				}
			}
			seenState, err := consumeSnapshotArtifactRows(
				chunk, payload, visit, candidate.previousKey[:], &previousKeyBytes,
				candidate.expectedStateDocument, candidate.stateRowSeen,
				&candidate.routeGateRows, &transactionScratch,
			)
			if err != nil {
				return SnapshotArtifactManifest{}, &current, err
			}
			candidate.previousKeyBytes = uint16(previousKeyBytes)
			candidate.stateRowSeen = candidate.stateRowSeen || seenState
			candidate.encodedBytes = checkpoint.EndOffset
			candidate.manifest.PayloadBytes += uint64(payloadBytes)
			var relationRows *uint64
			var priorRelationRows uint64
			if chunk.Collection == SnapshotArtifactSystem {
				candidate.manifest.SystemRows += chunk.Rows
				if candidate.routeGateRows != 0 {
					baseRows, ok := stateSystemRowCount(candidate.manifest.State)
					if !ok || baseRows > math.MaxUint64-candidate.routeGateRows ||
						candidate.manifest.SystemRows != baseRows+candidate.routeGateRows {
						return SnapshotArtifactManifest{}, &current,
							fmt.Errorf("%w: route-gate row accounting", ErrSnapshotArtifact)
					}
				}
			} else if chunk.Collection == SnapshotArtifactUser {
				candidate.manifest.UserRows += chunk.Rows
				if candidate.manifest.Bundle {
					relation := &candidate.manifest.Relations[int(chunk.Relation)-1]
					if relation.Rows > math.MaxUint64-chunk.Rows {
						return SnapshotArtifactManifest{}, &current,
							fmt.Errorf("%w: relation rows", ErrSnapshotArtifactBound)
					}
					relationRows = &relation.Rows
					priorRelationRows = relation.Rows
					relation.Rows += chunk.Rows
				}
			} else {
				candidate.manifest.CaptureRows += chunk.Rows
			}
			candidate.manifest.Chunks++
			candidate.manifest.LastChunkDigest = storedDigest
			candidate.previousDigest = storedDigest
			candidate.nextSequence++
			if callbacks.Rows != nil {
				if err := callbacks.Rows(checkpoint, SnapshotArtifactRows{
					collection: chunk.Collection, relation: chunk.Relation,
					payload: payload, rows: chunk.Rows,
				}); err != nil {
					if relationRows != nil {
						*relationRows = priorRelationRows
					}
					return SnapshotArtifactManifest{}, &current, err
				}
			}
			if callbacks.Chunk != nil {
				if err := callbacks.Chunk(checkpoint, &candidate); err != nil {
					if relationRows != nil {
						*relationRows = priorRelationRows
					}
					return SnapshotArtifactManifest{}, &current, err
				}
			}
			current = candidate
		case snapshotArtifactRelationMagic:
			var record [snapshotArtifactRelationBytes]byte
			copy(record[:8], magic[:])
			if err := readSnapshotArtifactBytes(r, record[8:], "relation certificate"); err != nil {
				return SnapshotArtifactManifest{}, &current, err
			}
			relation, digest, err := validateSnapshotArtifactRelation(
				record[:], current.manifest, current.nextRelation, current.previousDigest,
			)
			if err != nil {
				return SnapshotArtifactManifest{}, &current, err
			}
			if current.encodedBytes > math.MaxUint64-snapshotArtifactRelationBytes {
				return SnapshotArtifactManifest{}, &current,
					fmt.Errorf("%w: relation certificate bytes", ErrSnapshotArtifactBound)
			}
			candidate := current
			candidate.manifest.Relations[int(relation.Relation)-1].ImageDigest = relation.ImageDigest
			candidate.encodedBytes += snapshotArtifactRelationBytes
			candidate.previousDigest = digest
			candidate.manifest.LastChunkDigest = digest
			candidate.currentCollection = SnapshotArtifactUser
			candidate.previousKeyBytes = 0
			candidate.nextRelation++
			current = candidate
		case snapshotArtifactFooterMagic:
			var footer [snapshotArtifactFooterBytes]byte
			copy(footer[:8], magic[:])
			if err := readSnapshotArtifactBytes(r, footer[8:], "footer"); err != nil {
				return SnapshotArtifactManifest{}, &current, err
			}
			if err := validateSnapshotArtifactFooter(
				footer[:], current.manifest, current.previousDigest,
				current.encodedBytes, current.stateRowSeen, current.routeGateRows,
			); err != nil {
				return SnapshotArtifactManifest{}, &current, err
			}
			var certified [sha256.Size]byte
			copy(certified[:], footer[168:200])
			if current.captureImageDigest != certified {
				return SnapshotArtifactManifest{}, &current,
					fmt.Errorf("%w: capture image digest", ErrSnapshotArtifact)
			}
			var image [sha256.Size]byte
			copy(image[:], footer[136:168])
			if current.manifest.Bundle &&
				(current.nextRelation != replication.RelationID(len(current.manifest.Relations)+1) ||
					canonicalBundleImageDigest(current.manifest.Relations) != image) {
				return SnapshotArtifactManifest{}, &current,
					fmt.Errorf("%w: relation image", ErrSnapshotArtifact)
			}
			manifest = cloneSnapshotArtifactManifest(current.manifest)
			manifest.EncodedBytes = current.encodedBytes + snapshotArtifactFooterBytes
			copy(manifest.ImageDigest[:], footer[136:168])
			copy(manifest.CaptureImageDigest[:], footer[168:200])
			copy(manifest.Digest[:], footer[208:240])
			var trailing [1]byte
			n, readErr := io.ReadFull(r, trailing[:])
			if n != 0 || readErr == nil {
				return SnapshotArtifactManifest{}, &current,
					fmt.Errorf("%w: trailing bytes", ErrSnapshotArtifact)
			}
			if readErr != io.EOF {
				return SnapshotArtifactManifest{}, &current, readErr
			}
			return manifest, &current, nil
		default:
			return SnapshotArtifactManifest{}, &current,
				fmt.Errorf("%w: record magic", ErrSnapshotArtifact)
		}
	}
}

func validateSnapshotArtifactCursor(cursor *SnapshotArtifactCursor) error {
	if cursor == nil || cursor.encodedBytes == 0 ||
		cursor.nextSequence != cursor.manifest.Chunks ||
		cursor.previousDigest != cursor.manifest.LastChunkDigest ||
		cursor.manifest.ImageDigest != ([sha256.Size]byte{}) ||
		cursor.manifest.CaptureImageDigest != ([sha256.Size]byte{}) ||
		cursor.manifest.Digest != ([sha256.Size]byte{}) || cursor.manifest.EncodedBytes != 0 ||
		cursor.manifest.HeaderDigest == ([sha256.Size]byte{}) ||
		cursor.captureImageDigest == ([sha256.Size]byte{}) ||
		cursor.manifest.TargetChunkBytes < MinSnapshotArtifactChunkBytes ||
		cursor.manifest.TargetChunkBytes > MaxSnapshotArtifactChunkBytes ||
		(cursor.currentCollection != SnapshotArtifactSystem &&
			cursor.currentCollection != SnapshotArtifactUser &&
			cursor.currentCollection != SnapshotArtifactCapture) ||
		cursor.previousKeyBytes > replication.MaxMutationKeyBytes ||
		len(cursor.expectedStateDocument) == 0 {
		return fmt.Errorf("%w: resume cursor", ErrSnapshotArtifact)
	}
	completedRelations := 0
	headerRelations := []SnapshotArtifactRelation(nil)
	headerManifestDigest := [sha256.Size]byte{}
	if cursor.manifest.Bundle {
		if len(cursor.manifest.Relations) < 2 ||
			len(cursor.manifest.Relations) > replication.MaxRelationsPerBundle ||
			cursor.manifest.RelationManifestDigest == ([sha256.Size]byte{}) {
			return fmt.Errorf("%w: resume relations", ErrSnapshotArtifact)
		}
		headerRelations = make([]SnapshotArtifactRelation, len(cursor.manifest.Relations))
		var relationRows uint64
		for i := range cursor.manifest.Relations {
			relation := cursor.manifest.Relations[i]
			if relation.Relation != replication.RelationID(i+1) ||
				(relation.Kind != RelationJSON && relation.Kind != RelationGlobalIndex) ||
				len(relation.Collection) == 0 || len(relation.Collection) > replication.MaxIdentityBytes ||
				!utf8.Valid(relation.Collection) || bytes.IndexByte(relation.Collection, 0) >= 0 ||
				relationRows > math.MaxUint64-relation.Rows {
				return fmt.Errorf("%w: resume relation identity", ErrSnapshotArtifact)
			}
			for prior := 0; prior < i; prior++ {
				if bytes.Equal(relation.Collection, cursor.manifest.Relations[prior].Collection) {
					return fmt.Errorf("%w: duplicate resume relation", ErrSnapshotArtifact)
				}
			}
			if relation.ImageDigest != ([sha256.Size]byte{}) {
				if completedRelations != i {
					return fmt.Errorf("%w: resume relation prefix", ErrSnapshotArtifact)
				}
				completedRelations++
			} else if i < completedRelations || i > completedRelations && relation.Rows != 0 {
				return fmt.Errorf("%w: resume relation rows", ErrSnapshotArtifact)
			}
			relationRows += relation.Rows
			headerRelations[i] = SnapshotArtifactRelation{
				Relation: relation.Relation, Kind: relation.Kind,
				Collection: relation.Collection,
			}
		}
		if relationRows != cursor.manifest.UserRows ||
			cursor.nextRelation != replication.RelationID(completedRelations+1) ||
			!bytes.Equal(cursor.manifest.UserCollection, cursor.manifest.Relations[0].Collection) {
			return fmt.Errorf("%w: resume relation counters", ErrSnapshotArtifact)
		}
		headerManifestDigest = cursor.manifest.RelationManifestDigest
	} else if len(cursor.manifest.Relations) != 0 ||
		cursor.manifest.RelationManifestDigest != ([sha256.Size]byte{}) || cursor.nextRelation != 0 {
		return fmt.Errorf("%w: resume singleton relation", ErrSnapshotArtifact)
	}
	stateEnvelope, err := AppendState(nil, cursor.manifest.State)
	if err != nil || !bytes.Equal(stateEnvelope, cursor.expectedStateDocument) {
		return fmt.Errorf("%w: resume state", ErrSnapshotArtifact)
	}
	if len(cursor.manifest.UserCollection) == 0 ||
		len(cursor.manifest.UserCollection) > replication.MaxCollectionBytes ||
		!utf8.Valid(cursor.manifest.UserCollection) ||
		bytes.IndexByte(cursor.manifest.UserCollection, 0) >= 0 {
		return fmt.Errorf("%w: resume collection", ErrSnapshotArtifact)
	}
	header, headerDigest, err := makeSnapshotArtifactHeaderForRelations(
		stateEnvelope, string(cursor.manifest.UserCollection),
		int(cursor.manifest.TargetChunkBytes), headerManifestDigest,
		headerRelations, cursor.manifest.Bundle,
	)
	wantEncodedBytes, encodedBytesOK := snapshotArtifactEncodedBytesWithRelations(
		uint64(len(header)), cursor.manifest.Chunks, cursor.manifest.PayloadBytes,
		uint64(completedRelations), false,
	)
	wantSystemRows, systemRowsOK := stateSystemRowCount(cursor.manifest.State)
	if wantSystemRows > math.MaxUint64-cursor.routeGateRows {
		systemRowsOK = false
	} else {
		wantSystemRows += cursor.routeGateRows
	}
	if err != nil || headerDigest != cursor.manifest.HeaderDigest ||
		!encodedBytesOK || cursor.encodedBytes != wantEncodedBytes ||
		!systemRowsOK || cursor.manifest.SystemRows > wantSystemRows {
		return fmt.Errorf("%w: resume header identity", ErrSnapshotArtifact)
	}
	if cursor.manifest.Chunks == 0 && completedRelations == 0 {
		if cursor.encodedBytes != uint64(len(header)) || cursor.nextSequence != 0 ||
			cursor.manifest.SystemRows != 0 || cursor.manifest.UserRows != 0 ||
			cursor.manifest.CaptureRows != 0 ||
			cursor.manifest.PayloadBytes != 0 || cursor.previousKeyBytes != 0 ||
			cursor.currentCollection != SnapshotArtifactSystem || cursor.stateRowSeen ||
			cursor.routeGateRows != 0 ||
			cursor.previousDigest != cursor.manifest.HeaderDigest ||
			cursor.captureImageDigest != snapshotArtifactEmptyCaptureImageDigest() {
			return fmt.Errorf("%w: empty resume prefix", ErrSnapshotArtifact)
		}
		return nil
	}
	if cursor.encodedBytes <= uint64(len(header)) || cursor.previousKeyBytes == 0 ||
		!cursor.stateRowSeen || cursor.manifest.SystemRows == 0 ||
		cursor.currentCollection == SnapshotArtifactSystem &&
			(cursor.manifest.UserRows != 0 || cursor.manifest.CaptureRows != 0) ||
		cursor.currentCollection == SnapshotArtifactUser &&
			(cursor.manifest.SystemRows != wantSystemRows ||
				cursor.manifest.CaptureRows != 0) ||
		cursor.currentCollection == SnapshotArtifactCapture &&
			(cursor.manifest.SystemRows != wantSystemRows) ||
		(cursor.manifest.CaptureRows == 0) !=
			(cursor.captureImageDigest == snapshotArtifactEmptyCaptureImageDigest()) {
		// A relation certificate is a safe zero-row boundary and intentionally
		// clears the previous key before the next dense relation.
		if !cursor.manifest.Bundle || cursor.currentCollection != SnapshotArtifactUser ||
			cursor.previousKeyBytes != 0 || completedRelations == 0 ||
			cursor.manifest.CaptureRows != 0 || !cursor.stateRowSeen ||
			cursor.captureImageDigest != snapshotArtifactEmptyCaptureImageDigest() ||
			cursor.manifest.SystemRows != wantSystemRows ||
			completedRelations < len(cursor.manifest.Relations) &&
				cursor.manifest.Relations[completedRelations].Rows != 0 {
			return fmt.Errorf("%w: resume prefix state", ErrSnapshotArtifact)
		}
	}
	return nil
}

func cloneSnapshotArtifactManifest(manifest SnapshotArtifactManifest) SnapshotArtifactManifest {
	manifest.State = cloneState(manifest.State)
	manifest.UserCollection = bytes.Clone(manifest.UserCollection)
	manifest.Relations = append([]SnapshotArtifactRelation(nil), manifest.Relations...)
	for i := range manifest.Relations {
		manifest.Relations[i].Collection = bytes.Clone(manifest.Relations[i].Collection)
	}
	return manifest
}

type snapshotArtifactChunk struct {
	Sequence   uint64
	Collection SnapshotArtifactCollection
	Relation   replication.RelationID
	Rows       uint64
}

func readSnapshotArtifactHeader(
	r io.Reader,
) (SnapshotArtifactManifest, []byte, uint64, error) {
	var fixed [snapshotArtifactHeaderFixedBytes]byte
	if err := readSnapshotArtifactBytes(r, fixed[:], "header"); err != nil {
		return SnapshotArtifactManifest{}, nil, 0, err
	}
	if !bytes.Equal(fixed[0:8], snapshotArtifactHeaderMagic[:]) ||
		binary.LittleEndian.Uint16(fixed[8:10]) != snapshotArtifactFormat ||
		binary.LittleEndian.Uint16(fixed[10:12]) != snapshotArtifactHeaderFixedBytes {
		return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: header", ErrSnapshotArtifact)
	}
	flags := binary.LittleEndian.Uint16(fixed[22:24])
	bundle := flags == 1
	relationCount := uint64(binary.LittleEndian.Uint16(fixed[32:34]))
	descriptorBytes := uint64(binary.LittleEndian.Uint32(fixed[36:40]))
	if flags > 1 || !allZero(fixed[34:36]) || !allZero(fixed[40:64]) ||
		bundle != (relationCount != 0) || !bundle && descriptorBytes != 0 ||
		bundle && (relationCount < 2 || relationCount > replication.MaxRelationsPerBundle ||
			descriptorBytes < sha256.Size+relationCount*(snapshotArtifactRelationFixedBytes+1) ||
			descriptorBytes > sha256.Size+relationCount*(snapshotArtifactRelationFixedBytes+replication.MaxIdentityBytes)) {
		return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: relation header", ErrSnapshotArtifact)
	}
	total := uint64(binary.LittleEndian.Uint32(fixed[12:16]))
	stateBytes := uint64(binary.LittleEndian.Uint32(fixed[16:20]))
	nameBytes := uint64(binary.LittleEndian.Uint16(fixed[20:22]))
	target := uint64(binary.LittleEndian.Uint32(fixed[24:28]))
	if total != snapshotArtifactHeaderFixedBytes+stateBytes+nameBytes+descriptorBytes+sha256.Size ||
		total > maxSnapshotArtifactHeaderBytes || stateBytes == 0 ||
		stateBytes > MaxStateEnvelopeBytes || nameBytes == 0 ||
		nameBytes > replication.MaxCollectionBytes ||
		target < MinSnapshotArtifactChunkBytes || target > MaxSnapshotArtifactChunkBytes ||
		binary.LittleEndian.Uint32(fixed[28:32]) != MaxSnapshotArtifactChunkBytes {
		return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: header bounds", ErrSnapshotArtifactBound)
	}
	header := make([]byte, int(total))
	copy(header, fixed[:])
	if err := readSnapshotArtifactBytes(r, header[len(fixed):], "header body"); err != nil {
		return SnapshotArtifactManifest{}, nil, 0, err
	}
	wantDigest := snapshotArtifactDigest(snapshotArtifactHeaderDomain, header[:len(header)-sha256.Size])
	var storedDigest [sha256.Size]byte
	copy(storedDigest[:], header[len(header)-sha256.Size:])
	if storedDigest != wantDigest {
		return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: header digest", ErrSnapshotArtifact)
	}
	stateEnd := snapshotArtifactHeaderFixedBytes + int(stateBytes)
	stateEnvelope := header[snapshotArtifactHeaderFixedBytes:stateEnd]
	state, err := OpenState(stateEnvelope)
	if err != nil {
		return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: header state: %v", ErrSnapshotArtifact, err)
	}
	name := bytes.Clone(header[stateEnd : stateEnd+int(nameBytes)])
	if !utf8.Valid(name) || bytes.IndexByte(name, 0) >= 0 ||
		bytes.Equal(name, []byte(systemCollectionName)) {
		return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: user collection", ErrSnapshotArtifact)
	}
	manifest := SnapshotArtifactManifest{
		State: state, UserCollection: name, TargetChunkBytes: uint32(target),
		HeaderDigest: storedDigest, LastChunkDigest: storedDigest,
		Bundle: bundle,
	}
	if bundle {
		cursor := stateEnd + int(nameBytes)
		descriptorEnd := cursor + int(descriptorBytes)
		copy(manifest.RelationManifestDigest[:], header[cursor:cursor+sha256.Size])
		cursor += sha256.Size
		if manifest.RelationManifestDigest == ([sha256.Size]byte{}) {
			return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: relation manifest", ErrSnapshotArtifact)
		}
		manifest.Relations = make([]SnapshotArtifactRelation, int(relationCount))
		for i := range manifest.Relations {
			if cursor > descriptorEnd-snapshotArtifactRelationFixedBytes {
				return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: relation descriptor", ErrSnapshotArtifact)
			}
			fixedRelation := header[cursor : cursor+snapshotArtifactRelationFixedBytes]
			nameBytes := int(binary.LittleEndian.Uint16(fixedRelation[4:6]))
			if fixedRelation[3] != 0 || binary.LittleEndian.Uint16(fixedRelation[6:8]) != 0 ||
				nameBytes == 0 || nameBytes > replication.MaxIdentityBytes ||
				cursor+snapshotArtifactRelationFixedBytes > descriptorEnd-nameBytes {
				return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: relation descriptor bounds", ErrSnapshotArtifact)
			}
			relation := &manifest.Relations[i]
			relation.Relation = replication.RelationID(binary.LittleEndian.Uint16(fixedRelation[0:2]))
			relation.Kind = RelationKind(fixedRelation[2])
			cursor += snapshotArtifactRelationFixedBytes
			relation.Collection = bytes.Clone(header[cursor : cursor+nameBytes])
			cursor += nameBytes
			if relation.Relation != replication.RelationID(i+1) ||
				(relation.Kind != RelationJSON && relation.Kind != RelationGlobalIndex) ||
				!utf8.Valid(relation.Collection) || bytes.IndexByte(relation.Collection, 0) >= 0 ||
				bytes.Equal(relation.Collection, []byte(systemCollectionName)) {
				return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: relation identity", ErrSnapshotArtifact)
			}
			for prior := 0; prior < i; prior++ {
				if bytes.Equal(relation.Collection, manifest.Relations[prior].Collection) {
					return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: duplicate relation", ErrSnapshotArtifact)
				}
			}
		}
		if cursor != descriptorEnd || !bytes.Equal(name, manifest.Relations[0].Collection) {
			return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: relation geometry", ErrSnapshotArtifact)
		}
	}
	return manifest, stateEnvelope, total, nil
}

func validateSnapshotArtifactChunkHeader(
	header []byte,
	expectedSequence uint64,
	previousDigest [sha256.Size]byte,
	currentCollection SnapshotArtifactCollection,
	bundle bool,
	nextRelation replication.RelationID,
	relationCount int,
) (snapshotArtifactChunk, int, error) {
	if len(header) != snapshotArtifactChunkHeaderBytes ||
		!bytes.Equal(header[0:8], snapshotArtifactChunkMagic[:]) ||
		binary.LittleEndian.Uint16(header[8:10]) != snapshotArtifactFormat ||
		binary.LittleEndian.Uint16(header[10:12]) != snapshotArtifactChunkHeaderBytes ||
		header[25] != 0 ||
		!allZero(header[36:48]) || !allZero(header[80:96]) {
		return snapshotArtifactChunk{}, 0, fmt.Errorf("%w: chunk header", ErrSnapshotArtifact)
	}
	sequence := binary.LittleEndian.Uint64(header[16:24])
	collection := SnapshotArtifactCollection(header[24])
	relation := replication.RelationID(binary.LittleEndian.Uint16(header[26:28]))
	rows := uint64(binary.LittleEndian.Uint32(header[28:32]))
	payloadBytes := uint64(binary.LittleEndian.Uint32(header[32:36]))
	total := uint64(binary.LittleEndian.Uint32(header[12:16]))
	var storedPrevious [sha256.Size]byte
	copy(storedPrevious[:], header[48:80])
	if sequence != expectedSequence || rows == 0 || payloadBytes == 0 ||
		payloadBytes > MaxSnapshotArtifactChunkBytes ||
		total != snapshotArtifactChunkHeaderBytes+payloadBytes+sha256.Size ||
		storedPrevious != previousDigest {
		return snapshotArtifactChunk{}, 0, fmt.Errorf("%w: chunk sequence or bounds", ErrSnapshotArtifact)
	}
	if collection != SnapshotArtifactSystem && collection != SnapshotArtifactUser &&
		collection != SnapshotArtifactCapture ||
		collection < currentCollection {
		return snapshotArtifactChunk{}, 0, fmt.Errorf("%w: chunk collection order", ErrSnapshotArtifact)
	}
	if !bundle {
		if relation != 0 {
			return snapshotArtifactChunk{}, 0, fmt.Errorf("%w: singleton relation", ErrSnapshotArtifact)
		}
	} else if collection == SnapshotArtifactUser {
		if relation == 0 || relation != nextRelation || int(relation) > relationCount {
			return snapshotArtifactChunk{}, 0, fmt.Errorf("%w: chunk relation order", ErrSnapshotArtifact)
		}
	} else if relation != 0 || collection == SnapshotArtifactCapture &&
		nextRelation != replication.RelationID(relationCount+1) {
		return snapshotArtifactChunk{}, 0, fmt.Errorf("%w: chunk relation boundary", ErrSnapshotArtifact)
	}
	return snapshotArtifactChunk{
		Sequence: sequence, Collection: collection, Relation: relation, Rows: rows,
	}, int(payloadBytes), nil
}

func validateSnapshotArtifactRelation(
	record []byte,
	manifest SnapshotArtifactManifest,
	next replication.RelationID,
	previous [sha256.Size]byte,
) (SnapshotArtifactRelation, [sha256.Size]byte, error) {
	if len(record) != snapshotArtifactRelationBytes || !manifest.Bundle ||
		!bytes.Equal(record[0:8], snapshotArtifactRelationMagic[:]) ||
		binary.LittleEndian.Uint16(record[8:10]) != snapshotArtifactFormat ||
		binary.LittleEndian.Uint16(record[10:12]) != snapshotArtifactRelationBytes ||
		binary.LittleEndian.Uint32(record[12:16]) != snapshotArtifactRelationBytes ||
		!allZero(record[19:24]) {
		return SnapshotArtifactRelation{}, [sha256.Size]byte{},
			fmt.Errorf("%w: relation certificate", ErrSnapshotArtifact)
	}
	id := replication.RelationID(binary.LittleEndian.Uint16(record[16:18]))
	kind := RelationKind(record[18])
	if id == 0 || id != next || int(id) > len(manifest.Relations) {
		return SnapshotArtifactRelation{}, [sha256.Size]byte{},
			fmt.Errorf("%w: relation certificate order", ErrSnapshotArtifact)
	}
	want := manifest.Relations[int(id)-1]
	rows := binary.LittleEndian.Uint64(record[24:32])
	var image, storedPrevious, digest [sha256.Size]byte
	copy(image[:], record[32:64])
	copy(storedPrevious[:], record[64:96])
	copy(digest[:], record[96:128])
	if kind != want.Kind || rows != want.Rows || image == ([sha256.Size]byte{}) ||
		want.ImageDigest != ([sha256.Size]byte{}) || storedPrevious != previous ||
		digest != snapshotArtifactDigest(snapshotArtifactRelationDomain, record[:96]) {
		return SnapshotArtifactRelation{}, [sha256.Size]byte{},
			fmt.Errorf("%w: relation certificate identity", ErrSnapshotArtifact)
	}
	want.ImageDigest = image
	return want, digest, nil
}

func consumeSnapshotArtifactRows(
	chunk snapshotArtifactChunk,
	payload []byte,
	visit func(collection SnapshotArtifactCollection, key, value []byte) error,
	previousKey []byte,
	previousKeyBytes *int,
	expectedStateDocument []byte,
	stateRowAlreadySeen bool,
	routeGateRows *uint64,
	transactionScratch *snapshotArtifactTransactionScratch,
) (bool, error) {
	cursor := 0
	stateRowSeen := false
	for row := uint64(0); row < chunk.Rows; row++ {
		if cursor > len(payload)-snapshotArtifactRowHeaderBytes {
			return false, fmt.Errorf("%w: truncated row header", ErrSnapshotArtifact)
		}
		keyBytes := uint64(binary.LittleEndian.Uint32(payload[cursor : cursor+4]))
		valueBytes := uint64(binary.LittleEndian.Uint32(payload[cursor+4 : cursor+8]))
		cursor += snapshotArtifactRowHeaderBytes
		if keyBytes == 0 || keyBytes > replication.MaxMutationKeyBytes ||
			valueBytes == 0 || valueBytes > snapshotArtifactMaxValueBytes(chunk.Collection) ||
			keyBytes+valueBytes > uint64(len(payload)-cursor) {
			return false, fmt.Errorf("%w: row bounds", ErrSnapshotArtifactBound)
		}
		keyEnd := cursor + int(keyBytes)
		valueEnd := keyEnd + int(valueBytes)
		key, value := payload[cursor:keyEnd], payload[keyEnd:valueEnd]
		cursor = valueEnd
		if *previousKeyBytes != 0 && bytes.Compare(previousKey[:*previousKeyBytes], key) >= 0 {
			return false, fmt.Errorf("%w: rows not strictly ordered", ErrSnapshotArtifact)
		}
		copy(previousKey, key)
		*previousKeyBytes = len(key)
		if chunk.Collection == SnapshotArtifactSystem {
			switch {
			case bytes.Equal(key, stateKey):
				if stateRowAlreadySeen || stateRowSeen || !bytes.Equal(value, expectedStateDocument) {
					return false, fmt.Errorf("%w: hidden state row", ErrSnapshotArtifact)
				}
				stateRowSeen = true
			case len(key) == sha256.Size+1 && key[0] == 1:
			case len(key) == sha256.Size+3 && key[0] == 2:
			case len(key) == sha256.Size+1 && key[0] == 3:
				view, err := OpenAuthorityBinding(value)
				want := AuthorityBindingStorageKey(view.Digest)
				if err != nil || !bytes.Equal(key, want[:]) {
					return false, fmt.Errorf("%w: hidden authority binding: %v",
						ErrSnapshotArtifact, err)
				}
			case bytes.Equal(key, routeGateHeadKey):
				if _, err := routegate.OpenHead(value); err != nil {
					return false, errors.Join(err,
						fmt.Errorf("%w: route-gate head", ErrSnapshotArtifact))
				}
				if routeGateRows == nil || *routeGateRows == math.MaxUint64 {
					return false, fmt.Errorf("%w: route-gate row count", ErrSnapshotArtifactBound)
				}
				*routeGateRows++
			case len(key) == routeGatePinKeyBytes && key[0] == routeGatePinPrefix:
				var identity routegate.Identity
				copy(identity[:], key[1:])
				_, err := routegate.OpenStoredPin(identity, value)
				want, keyErr := routeGatePinStorageKey(identity)
				if err != nil || keyErr != nil || !bytes.Equal(key, want[:]) {
					return false, errors.Join(err, keyErr,
						fmt.Errorf("%w: route-gate pin", ErrSnapshotArtifact))
				}
				if routeGateRows == nil || *routeGateRows == math.MaxUint64 {
					return false, fmt.Errorf("%w: route-gate row count", ErrSnapshotArtifactBound)
				}
				*routeGateRows++
			case len(key) == routeGateResultKeyBytes && key[0] == routeGateResultPrefix:
				view, err := openRouteGateResult(value)
				want, keyErr := routeGateResultStorageKey(view.SessionDigest, view.Slot)
				if err != nil || keyErr != nil || !bytes.Equal(key, want[:]) {
					return false, errors.Join(err, keyErr,
						fmt.Errorf("%w: route-gate result", ErrSnapshotArtifact))
				}
				if routeGateRows == nil || *routeGateRows == math.MaxUint64 {
					return false, fmt.Errorf("%w: route-gate row count", ErrSnapshotArtifactBound)
				}
				*routeGateRows++
			case len(key) == transactionControlStorageKeyBytes && key[0] == transactionControlPrefix:
				view, err := OpenTransactionControl(value)
				want, keyErr := view.StorageKey()
				if err != nil || keyErr != nil || !bytes.Equal(key, want[:]) {
					return false, errors.Join(err, keyErr,
						fmt.Errorf("%w: transaction control", ErrSnapshotArtifact))
				}
			case len(key) == transactionPayloadStorageKeyBytes && key[0] == transactionPayloadPrefix:
				view, err := OpenTransactionCoordinatorPayload(value)
				want, keyErr := view.StorageKey()
				if err != nil || keyErr != nil || !bytes.Equal(key, want[:]) {
					return false, errors.Join(err, keyErr,
						fmt.Errorf("%w: transaction payload", ErrSnapshotArtifact))
				}
			case len(key) == transactionManifestKeyBytes && key[0] == transactionManifestPrefix:
				if len(value) < transactionManifestHeaderBytes+recordChecksumLen {
					return false, fmt.Errorf("%w: transaction manifest", ErrSnapshotArtifact)
				}
				count := int(binary.LittleEndian.Uint32(value[32:36]))
				if count <= 0 || count > distributedtxn.MaxManifestPageParticipants {
					return false, fmt.Errorf("%w: transaction manifest count", ErrSnapshotArtifact)
				}
				if transactionScratch == nil {
					return false, fmt.Errorf("%w: missing transaction scratch", ErrSnapshotArtifact)
				}
				if transactionScratch.participants == nil {
					transactionScratch.participants = make([]distributedtxn.ParticipantRef,
						distributedtxn.MaxManifestPageParticipants)
					transactionScratch.identities = make([]byte,
						distributedtxn.MaxManifestPageParticipants*distributedtxn.MaxShardIdentityBytes*2)
				}
				view, err := OpenTransactionManifestPageInto(
					value, transactionScratch.participants, transactionScratch.identities,
				)
				want, keyErr := view.StorageKey()
				if err != nil || keyErr != nil || !bytes.Equal(key, want[:]) {
					return false, errors.Join(err, keyErr,
						fmt.Errorf("%w: transaction manifest", ErrSnapshotArtifact))
				}
			case len(key) == transactionRelationPayloadKeyBytes && key[0] == transactionMutationPrefix:
				view, err := OpenTransactionRelationPayload(value)
				want, keyErr := view.StorageKey()
				if err != nil || keyErr != nil || !bytes.Equal(key, want[:]) {
					return false, errors.Join(err, keyErr,
						fmt.Errorf("%w: transaction mutation", ErrSnapshotArtifact))
				}
			case len(key) == transactionIntentKeyBytes && key[0] == transactionIntentPrefix:
				view, err := OpenTransactionIntent(value)
				want, keyErr := view.StorageKey()
				if err != nil || keyErr != nil || !bytes.Equal(key, want[:]) {
					return false, errors.Join(err, keyErr,
						fmt.Errorf("%w: transaction intent", ErrSnapshotArtifact))
				}
			case len(key) >= requestledger.FixedStorageKeyBytes && key[0] == requestledger.StoragePrefix:
				if err := validateSnapshotRequestLedgerRow(key, value); err != nil {
					return false, fmt.Errorf("%w: request ledger row: %v", ErrSnapshotArtifact, err)
				}
			case len(key) == requestledger.IssuerHighwaterKeyBytes && key[0] == requestledger.IssuerHighwaterStoragePrefix:
				home, issuer, keyErr := requestledger.OpenIssuerHighwaterKey(key)
				record, recordErr := requestledger.OpenIssuerHighwater(value)
				if keyErr != nil || recordErr != nil || record.Home != home || record.IssuerDigest != issuer {
					return false, errors.Join(keyErr, recordErr,
						fmt.Errorf("%w: request ledger issuer high-water", ErrSnapshotArtifact))
				}
			case len(key) == requestledger.IssuerSequenceKeyBytes && key[0] == requestledger.IssuerSequenceStoragePrefix:
				home, issuer, ordinal, keyErr := requestledger.OpenIssuerSequenceKey(key)
				record, recordErr := requestledger.OpenIssuerSequence(value)
				if keyErr != nil || recordErr != nil || record.Home != home ||
					record.IssuerDigest != issuer || record.Sequence != ordinal {
					return false, errors.Join(keyErr, recordErr,
						fmt.Errorf("%w: request ledger issuer sequence", ErrSnapshotArtifact))
				}
			default:
				return false, fmt.Errorf("%w: hidden system key", ErrSnapshotArtifact)
			}
		}
		if visit != nil {
			if err := visit(chunk.Collection, key, value); err != nil {
				return false, err
			}
		}
	}
	if cursor != len(payload) {
		return false, fmt.Errorf("%w: trailing chunk payload", ErrSnapshotArtifact)
	}
	if chunk.Collection == SnapshotArtifactSystem && !stateRowAlreadySeen && !stateRowSeen {
		return false, fmt.Errorf("%w: first system chunk omits hidden state", ErrSnapshotArtifact)
	}
	return stateRowSeen, nil
}

func snapshotArtifactMaxValueBytes(collection SnapshotArtifactCollection) uint64 {
	if collection == SnapshotArtifactCapture {
		return MaxTransitionCaptureRecordBytes
	}
	if collection == SnapshotArtifactSystem {
		return max(uint64(replication.MaxMutationValueBytes),
			uint64(MaxTransactionRelationPayloadRecordBytes),
			uint64(requestledger.MaxCommandBytes))
	}
	return replication.MaxMutationValueBytes
}

func validateSnapshotRequestLedgerRow(key, value []byte) error {
	view, err := requestledger.OpenStorageKey(key)
	if err != nil {
		return err
	}
	switch view.Kind {
	case requestledger.StorageHead:
		record, openErr := requestledger.OpenHead(value)
		if openErr != nil || record.KeyDigest != view.Key {
			return errors.Join(openErr, ErrStateCorrupt)
		}
		home, homeErr := requestledger.Home(record.Key)
		if homeErr != nil || home != view.Home {
			return errors.Join(homeErr, ErrStateCorrupt)
		}
	case requestledger.StoragePlanPage:
		record, openErr := requestledger.OpenPlanPage(value)
		if openErr != nil || record.KeyDigest != view.Key || record.Ordinal != view.Ordinal {
			return errors.Join(openErr, ErrStateCorrupt)
		}
	case requestledger.StoragePending:
		var scratch [requestledger.MaxPendingWaveSteps]requestledger.StepRef
		record, openErr := requestledger.OpenPendingWaveInto(value, scratch[:])
		if openErr != nil || record.Key() != view.Key {
			return errors.Join(openErr, ErrStateCorrupt)
		}
	case requestledger.StorageContinuation:
		record, openErr := requestledger.OpenContinuation(value)
		if openErr != nil || record.KeyDigest != view.Key {
			return errors.Join(openErr, ErrStateCorrupt)
		}
	case requestledger.StorageTerminal:
		record, openErr := requestledger.OpenTerminal(value)
		if openErr != nil || record.KeyDigest != view.Key {
			return errors.Join(openErr, ErrStateCorrupt)
		}
	case requestledger.StorageAck:
		record, openErr := requestledger.OpenAck(value)
		if openErr != nil || record.KeyDigest != view.Key {
			return errors.Join(openErr, ErrStateCorrupt)
		}
		home, homeErr := requestledger.Home(record.Key)
		if homeErr != nil || home != view.Home {
			return errors.Join(homeErr, ErrStateCorrupt)
		}
	case requestledger.StoragePayloadChunk:
		record, openErr := requestledger.OpenPayloadChunk(value)
		if openErr != nil || record.KeyDigest != view.Key || record.ContentRoot != view.Content ||
			record.Ordinal != view.Ordinal {
			return errors.Join(openErr, ErrStateCorrupt)
		}
	case requestledger.StoragePayloadBuild:
		record, openErr := requestledger.OpenPayloadBuild(value)
		if openErr != nil || record.KeyDigest != view.Key {
			return errors.Join(openErr, ErrStateCorrupt)
		}
	case requestledger.StorageRoutePin:
		record, openErr := requestledger.OpenRoutePin(value)
		if openErr != nil || record.KeyDigest != view.Key {
			return errors.Join(openErr, ErrStateCorrupt)
		}
	case requestledger.StoragePrepared:
		record, openErr := requestledger.OpenPreparedTerminal(value)
		if openErr != nil || record.KeyDigest != view.Key {
			return errors.Join(openErr, ErrStateCorrupt)
		}
	case requestledger.StorageSchemaPin:
		record, openErr := requestledger.OpenSchemaPinRelease(value)
		if openErr != nil || record.KeyDigest != view.Key {
			return errors.Join(openErr, ErrStateCorrupt)
		}
	default:
		return ErrStateCorrupt
	}
	return nil
}

func validateSnapshotArtifactFooter(
	footer []byte,
	manifest SnapshotArtifactManifest,
	previousDigest [sha256.Size]byte,
	encodedBeforeFooter uint64,
	stateRowSeen bool,
	routeGateRows uint64,
) error {
	if len(footer) != snapshotArtifactFooterBytes ||
		!bytes.Equal(footer[0:8], snapshotArtifactFooterMagic[:]) ||
		binary.LittleEndian.Uint16(footer[8:10]) != snapshotArtifactFormat ||
		binary.LittleEndian.Uint16(footer[10:12]) != snapshotArtifactFooterBytes ||
		binary.LittleEndian.Uint32(footer[12:16]) != snapshotArtifactFooterBytes {
		return fmt.Errorf("%w: footer header", ErrSnapshotArtifact)
	}
	if encodedBeforeFooter > math.MaxUint64-snapshotArtifactFooterBytes {
		return fmt.Errorf("%w: footer bytes", ErrSnapshotArtifactBound)
	}
	var storedPrevious, storedHeader, storedImage, storedCaptureImage [sha256.Size]byte
	copy(storedPrevious[:], footer[72:104])
	copy(storedHeader[:], footer[104:136])
	copy(storedImage[:], footer[136:168])
	copy(storedCaptureImage[:], footer[168:200])
	if !allZero(footer[200:208]) {
		return fmt.Errorf("%w: footer reserved", ErrSnapshotArtifact)
	}
	wantDigest := snapshotArtifactDigest(snapshotArtifactFooterDomain, footer[:208])
	var storedDigest [sha256.Size]byte
	copy(storedDigest[:], footer[208:240])
	if storedDigest != wantDigest ||
		storedImage == ([sha256.Size]byte{}) || storedCaptureImage == ([sha256.Size]byte{}) ||
		binary.LittleEndian.Uint64(footer[16:24]) != manifest.Chunks ||
		binary.LittleEndian.Uint64(footer[24:32]) != manifest.Chunks ||
		binary.LittleEndian.Uint64(footer[32:40]) != manifest.SystemRows ||
		binary.LittleEndian.Uint64(footer[40:48]) != manifest.UserRows ||
		binary.LittleEndian.Uint64(footer[48:56]) != manifest.CaptureRows ||
		binary.LittleEndian.Uint64(footer[56:64]) != manifest.PayloadBytes ||
		binary.LittleEndian.Uint64(footer[64:72]) != encodedBeforeFooter+snapshotArtifactFooterBytes ||
		storedPrevious != previousDigest || storedHeader != manifest.HeaderDigest {
		return fmt.Errorf("%w: footer totals or digest", ErrSnapshotArtifact)
	}
	wantSystemRows, systemRowsOK := stateSystemRowCount(manifest.State)
	if wantSystemRows > math.MaxUint64-routeGateRows {
		systemRowsOK = false
	} else {
		wantSystemRows += routeGateRows
	}
	if !stateRowSeen || !systemRowsOK || manifest.SystemRows != wantSystemRows {
		return fmt.Errorf("%w: hidden state image", ErrSnapshotArtifact)
	}
	return nil
}

func snapshotArtifactDigest(domain, body []byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(domain)
	_, _ = h.Write(body)
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	return digest
}

func snapshotArtifactDigestParts(domain, first, second []byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(domain)
	_, _ = h.Write(first)
	_, _ = h.Write(second)
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	return digest
}

func snapshotArtifactOpaqueImageDigest(snapshot *durable.Snapshot) ([sha256.Size]byte, error) {
	if snapshot == nil {
		return [sha256.Size]byte{}, ErrInconsistentSnapshot
	}
	digest := snapshotArtifactEmptyCaptureImageDigest()
	err := snapshot.RangeRaw(func(key, value []byte) error {
		digest = snapshotArtifactCaptureImageNext(digest, key, value)
		return nil
	})
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return digest, nil
}

func snapshotArtifactEmptyCaptureImageDigest() [sha256.Size]byte {
	return sha256.Sum256(snapshotArtifactCaptureImageDomain)
}

func snapshotArtifactCaptureImageNext(
	previous [sha256.Size]byte, key, value []byte,
) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(snapshotArtifactCaptureImageDomain)
	_, _ = h.Write(previous[:])
	writeHashFrame(h, key)
	writeHashFrame(h, value)
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	return digest
}

func writeSnapshotArtifactBytes(w io.Writer, src []byte) error {
	for len(src) != 0 {
		n, err := w.Write(src)
		if n < 0 || n > len(src) {
			return io.ErrShortWrite
		}
		src = src[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func readSnapshotArtifactBytes(r io.Reader, dst []byte, field string) error {
	if _, err := io.ReadFull(r, dst); err != nil {
		return fmt.Errorf("%w: truncated %s: %w", ErrSnapshotArtifact, field, err)
	}
	return nil
}
