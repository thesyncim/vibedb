package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	snapshotArtifactFormat           = uint16(1)
	snapshotArtifactHeaderFixedBytes = 64
	snapshotArtifactChunkHeaderBytes = 96
	snapshotArtifactFooterBytes      = 160
	snapshotArtifactChecksumBytes    = sha256.Size
	snapshotArtifactRowHeaderBytes   = 8

	// DefaultSnapshotArtifactChunkBytes bounds the ordinary retained transfer
	// buffer. A single larger row is emitted alone, up to the fixed row bound.
	DefaultSnapshotArtifactChunkBytes = 1 << 20
	// MinSnapshotArtifactChunkBytes prevents pathological framing overhead.
	MinSnapshotArtifactChunkBytes = 4 << 10
	// MaxSnapshotArtifactChunkBytes is one maximum-sized raw row frame. The
	// verifier refuses larger declared payloads before allocating its buffer.
	MaxSnapshotArtifactChunkBytes = replication.MaxMutationKeyBytes +
		replication.MaxMutationValueBytes + snapshotArtifactRowHeaderBytes
	maxSnapshotArtifactHeaderBytes = snapshotArtifactHeaderFixedBytes +
		MaxStateEnvelopeBytes + replication.MaxCollectionBytes + snapshotArtifactChecksumBytes
)

const (
	// SnapshotArtifactSystem identifies raw hidden system rows.
	SnapshotArtifactSystem SnapshotArtifactCollection = 1
	// SnapshotArtifactUser identifies raw user-collection rows.
	SnapshotArtifactUser SnapshotArtifactCollection = 2
)

var (
	snapshotArtifactHeaderMagic  = [8]byte{'V', 'D', 'B', 'S', 'N', 'A', 'P', 0}
	snapshotArtifactChunkMagic   = [8]byte{'V', 'D', 'B', 'S', 'C', 'H', 'K', 0}
	snapshotArtifactFooterMagic  = [8]byte{'V', 'D', 'B', 'S', 'E', 'N', 'D', 0}
	snapshotArtifactHeaderDomain = []byte(
		"vibedb/replicated-state/snapshot-artifact-header\x00",
	)
	snapshotArtifactChunkDomain = []byte(
		"vibedb/replicated-state/snapshot-artifact-chunk\x00",
	)
	snapshotArtifactFooterDomain = []byte(
		"vibedb/replicated-state/snapshot-artifact-footer\x00",
	)
)

// SnapshotArtifactCollection identifies one collection without putting its
// name in every row frame. Zero and unknown values are invalid.
type SnapshotArtifactCollection uint8

// SnapshotArtifactOptions controls deterministic chunk packing and optional
// caller-owned workspace. Zero TargetChunkBytes selects the default. Rows are
// never fragmented. When PayloadBuffer is non-nil, its capacity must cover the
// target and its contents are borrowed and overwritten for the call. Supplying
// capacity through MaxSnapshotArtifactChunkBytes prevents even an exceptional
// maximum-sized row from growing the buffer.
type SnapshotArtifactOptions struct {
	TargetChunkBytes int
	PayloadBuffer    []byte
}

// SnapshotArtifactCheckpoint is emitted after one complete chunk has passed
// its hash-chain and row-frame checks. EndOffset is the exact byte position at
// the end of the chunk, suitable for a durable receiver checkpoint.
type SnapshotArtifactCheckpoint struct {
	Sequence     uint64
	Collection   SnapshotArtifactCollection
	Rows         uint64
	PayloadBytes uint64
	EndOffset    uint64
	Digest       [sha256.Size]byte
}

// SnapshotArtifactCallbacks consume verified, borrowed rows and durable chunk
// boundaries. Row bytes remain valid only until Row returns. Chunk is called
// after every row in that chunk has been accepted. PayloadBuffer is optional
// caller-owned workspace; its contents are borrowed and overwritten for the
// call. Capacity through MaxSnapshotArtifactChunkBytes prevents growth for
// every valid artifact.
type SnapshotArtifactCallbacks struct {
	Row           func(collection SnapshotArtifactCollection, key, value []byte) error
	Chunk         func(checkpoint SnapshotArtifactCheckpoint) error
	PayloadBuffer []byte
}

// SnapshotArtifactManifest certifies one coherent system/user image. The
// collection name is detached raw bytes; State is the canonical publication
// embedded in both the header and hidden system row.
type SnapshotArtifactManifest struct {
	State            State
	UserCollection   []byte
	TargetChunkBytes uint32
	Chunks           uint64
	SystemRows       uint64
	UserRows         uint64
	PayloadBytes     uint64
	EncodedBytes     uint64
	HeaderDigest     [sha256.Size]byte
	LastChunkDigest  [sha256.Size]byte
	Digest           [sha256.Size]byte
}

type snapshotArtifactWriter struct {
	w              io.Writer
	target         int
	payload        []byte
	collection     SnapshotArtifactCollection
	chunkRows      uint32
	chunks         uint64
	systemRows     uint64
	userRows       uint64
	payloadBytes   uint64
	encodedBytes   uint64
	headerDigest   [sha256.Size]byte
	previousDigest [sha256.Size]byte
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
	header, headerDigest, err := makeSnapshotArtifactHeader(stateEnvelope, snapshot.userName, target)
	if err != nil {
		return SnapshotArtifactManifest{}, err
	}
	if err := writeSnapshotArtifactBytes(w, header); err != nil {
		return SnapshotArtifactManifest{}, err
	}
	writer := snapshotArtifactWriter{
		w: w, target: target, payload: payload,
		encodedBytes: uint64(len(header)), headerDigest: headerDigest,
		previousDigest: headerDigest,
	}
	if err := writer.writeCollection(SnapshotArtifactSystem, snapshot.RangeSystem); err != nil {
		return SnapshotArtifactManifest{}, err
	}
	user, ok := snapshot.Collection(snapshot.userName)
	if !ok || user == nil {
		return SnapshotArtifactManifest{}, ErrInconsistentSnapshot
	}
	if err := writer.writeCollection(SnapshotArtifactUser, user.RangeRaw); err != nil {
		return SnapshotArtifactManifest{}, err
	}
	digest, err := writer.writeFooter()
	if err != nil {
		return SnapshotArtifactManifest{}, err
	}
	return SnapshotArtifactManifest{
		State: cloneState(snapshot.state), UserCollection: []byte(snapshot.userName),
		TargetChunkBytes: uint32(target), Chunks: writer.chunks,
		SystemRows: writer.systemRows, UserRows: writer.userRows,
		PayloadBytes: writer.payloadBytes, EncodedBytes: writer.encodedBytes,
		HeaderDigest: headerDigest, LastChunkDigest: writer.previousDigest, Digest: digest,
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
	if len(stateEnvelope) == 0 || len(stateEnvelope) > MaxStateEnvelopeBytes ||
		len(userName) == 0 || len(userName) > replication.MaxCollectionBytes {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: header fields", ErrSnapshotArtifactBound)
	}
	total := snapshotArtifactHeaderFixedBytes + len(stateEnvelope) + len(userName) +
		snapshotArtifactChecksumBytes
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
	binary.LittleEndian.PutUint32(header[24:28], uint32(target))
	binary.LittleEndian.PutUint32(header[28:32], MaxSnapshotArtifactChunkBytes)
	cursor := snapshotArtifactHeaderFixedBytes
	cursor += copy(header[cursor:], stateEnvelope)
	copy(header[cursor:], userName)
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
		rowBytes, ok := snapshotArtifactRowBytes(key, value)
		if !ok {
			return fmt.Errorf("%w: row", ErrSnapshotArtifactBound)
		}
		if len(w.payload) != 0 && rowBytes > w.target-len(w.payload) {
			if err := w.flush(); err != nil {
				return err
			}
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

func snapshotArtifactRowBytes(key, value []byte) (int, bool) {
	if len(key) == 0 || len(key) > replication.MaxMutationKeyBytes ||
		len(value) == 0 || len(value) > replication.MaxMutationValueBytes {
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
	if w.chunkRows == 0 || len(w.payload) > MaxSnapshotArtifactChunkBytes ||
		w.chunks == math.MaxUint64 {
		return fmt.Errorf("%w: chunk counters", ErrSnapshotArtifactBound)
	}
	total := snapshotArtifactChunkHeaderBytes + len(w.payload) + sha256.Size
	var header [snapshotArtifactChunkHeaderBytes]byte
	copy(header[0:8], snapshotArtifactChunkMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], snapshotArtifactFormat)
	binary.LittleEndian.PutUint16(header[10:12], snapshotArtifactChunkHeaderBytes)
	binary.LittleEndian.PutUint32(header[12:16], uint32(total))
	binary.LittleEndian.PutUint64(header[16:24], w.chunks)
	header[24] = byte(w.collection)
	binary.LittleEndian.PutUint32(header[28:32], w.chunkRows)
	binary.LittleEndian.PutUint32(header[32:36], uint32(len(w.payload)))
	copy(header[48:80], w.previousDigest[:])
	digest := snapshotArtifactDigestParts(snapshotArtifactChunkDomain, header[:], w.payload)
	rows := uint64(w.chunkRows)
	if w.collection == SnapshotArtifactSystem {
		if w.systemRows > math.MaxUint64-rows {
			return fmt.Errorf("%w: system rows", ErrSnapshotArtifactBound)
		}
		w.systemRows += rows
	} else {
		if w.userRows > math.MaxUint64-rows {
			return fmt.Errorf("%w: user rows", ErrSnapshotArtifactBound)
		}
		w.userRows += rows
	}
	payloadBytes := uint64(len(w.payload))
	if w.payloadBytes > math.MaxUint64-payloadBytes ||
		w.encodedBytes > math.MaxUint64-uint64(total) {
		return fmt.Errorf("%w: artifact counters", ErrSnapshotArtifactBound)
	}
	if err := writeSnapshotArtifactBytes(w.w, header[:]); err != nil {
		return err
	}
	if err := writeSnapshotArtifactBytes(w.w, w.payload); err != nil {
		return err
	}
	if err := writeSnapshotArtifactBytes(w.w, digest[:]); err != nil {
		return err
	}
	w.payloadBytes += payloadBytes
	w.encodedBytes += uint64(total)
	w.chunks++
	w.previousDigest = digest
	w.payload = w.payload[:0]
	w.chunkRows = 0
	return nil
}

func (w *snapshotArtifactWriter) writeFooter() ([sha256.Size]byte, error) {
	if w.encodedBytes > math.MaxUint64-snapshotArtifactFooterBytes {
		return [sha256.Size]byte{}, fmt.Errorf("%w: artifact bytes", ErrSnapshotArtifactBound)
	}
	totalBytes := w.encodedBytes + snapshotArtifactFooterBytes
	var footer [snapshotArtifactFooterBytes]byte
	copy(footer[0:8], snapshotArtifactFooterMagic[:])
	binary.LittleEndian.PutUint16(footer[8:10], snapshotArtifactFormat)
	binary.LittleEndian.PutUint16(footer[10:12], snapshotArtifactFooterBytes)
	binary.LittleEndian.PutUint32(footer[12:16], snapshotArtifactFooterBytes)
	binary.LittleEndian.PutUint64(footer[16:24], w.chunks)
	binary.LittleEndian.PutUint64(footer[24:32], w.chunks)
	binary.LittleEndian.PutUint64(footer[32:40], w.systemRows)
	binary.LittleEndian.PutUint64(footer[40:48], w.userRows)
	binary.LittleEndian.PutUint64(footer[48:56], w.payloadBytes)
	binary.LittleEndian.PutUint64(footer[56:64], totalBytes)
	copy(footer[64:96], w.previousDigest[:])
	copy(footer[96:128], w.headerDigest[:])
	digest := snapshotArtifactDigest(snapshotArtifactFooterDomain, footer[:128])
	copy(footer[128:], digest[:])
	if err := writeSnapshotArtifactBytes(w.w, footer[:]); err != nil {
		return [sha256.Size]byte{}, err
	}
	w.encodedBytes = totalBytes
	return digest, nil
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
	if r == nil {
		return SnapshotArtifactManifest{}, fmt.Errorf("%w: nil reader", ErrSnapshotArtifact)
	}
	manifest, expectedStateDocument, encodedBytes, err := readSnapshotArtifactHeader(r)
	if err != nil {
		return SnapshotArtifactManifest{}, err
	}
	expectedSequence := uint64(0)
	previousDigest := manifest.HeaderDigest
	payload := callbacks.PayloadBuffer[:0]
	if payload == nil {
		payload = make([]byte, 0, min(int(manifest.TargetChunkBytes), 64<<10))
	}
	var previousKey [replication.MaxMutationKeyBytes]byte
	previousKeyBytes := 0
	currentCollection := SnapshotArtifactSystem
	stateRowSeen := false
	for {
		var magic [8]byte
		if err := readSnapshotArtifactBytes(r, magic[:], "record magic"); err != nil {
			return SnapshotArtifactManifest{}, err
		}
		switch magic {
		case snapshotArtifactChunkMagic:
			var header [snapshotArtifactChunkHeaderBytes]byte
			copy(header[:8], magic[:])
			if err := readSnapshotArtifactBytes(r, header[8:], "chunk header"); err != nil {
				return SnapshotArtifactManifest{}, err
			}
			chunk, payloadBytes, err := validateSnapshotArtifactChunkHeader(
				header[:], expectedSequence, previousDigest, currentCollection,
			)
			if err != nil {
				return SnapshotArtifactManifest{}, err
			}
			if chunk.Collection != currentCollection {
				currentCollection = chunk.Collection
				previousKeyBytes = 0
			}
			if cap(payload) < payloadBytes {
				payload = make([]byte, payloadBytes)
			} else {
				payload = payload[:payloadBytes]
			}
			if err := readSnapshotArtifactBytes(r, payload, "chunk payload"); err != nil {
				return SnapshotArtifactManifest{}, err
			}
			var storedDigest [sha256.Size]byte
			if err := readSnapshotArtifactBytes(r, storedDigest[:], "chunk digest"); err != nil {
				return SnapshotArtifactManifest{}, err
			}
			wantDigest := snapshotArtifactDigestParts(snapshotArtifactChunkDomain, header[:], payload)
			if storedDigest != wantDigest {
				return SnapshotArtifactManifest{}, fmt.Errorf("%w: chunk digest", ErrSnapshotArtifact)
			}
			seenState, err := consumeSnapshotArtifactRows(
				chunk, payload, callbacks.Row, previousKey[:], &previousKeyBytes,
				expectedStateDocument, stateRowSeen,
			)
			if err != nil {
				return SnapshotArtifactManifest{}, err
			}
			stateRowSeen = stateRowSeen || seenState
			chunkBytes := uint64(snapshotArtifactChunkHeaderBytes + payloadBytes + sha256.Size)
			if manifest.Chunks == math.MaxUint64 || expectedSequence == math.MaxUint64 ||
				encodedBytes > math.MaxUint64-chunkBytes ||
				manifest.PayloadBytes > math.MaxUint64-uint64(payloadBytes) {
				return SnapshotArtifactManifest{}, fmt.Errorf("%w: artifact counters", ErrSnapshotArtifactBound)
			}
			encodedBytes += chunkBytes
			manifest.PayloadBytes += uint64(payloadBytes)
			if chunk.Collection == SnapshotArtifactSystem {
				if manifest.SystemRows > math.MaxUint64-chunk.Rows {
					return SnapshotArtifactManifest{}, fmt.Errorf("%w: system rows", ErrSnapshotArtifactBound)
				}
				manifest.SystemRows += chunk.Rows
			} else {
				if manifest.UserRows > math.MaxUint64-chunk.Rows {
					return SnapshotArtifactManifest{}, fmt.Errorf("%w: user rows", ErrSnapshotArtifactBound)
				}
				manifest.UserRows += chunk.Rows
			}
			manifest.Chunks++
			manifest.LastChunkDigest = storedDigest
			previousDigest = storedDigest
			expectedSequence++
			if callbacks.Chunk != nil {
				if err := callbacks.Chunk(SnapshotArtifactCheckpoint{
					Sequence: chunk.Sequence, Collection: chunk.Collection,
					Rows: chunk.Rows, PayloadBytes: uint64(payloadBytes),
					EndOffset: encodedBytes, Digest: storedDigest,
				}); err != nil {
					return SnapshotArtifactManifest{}, err
				}
			}
		case snapshotArtifactFooterMagic:
			var footer [snapshotArtifactFooterBytes]byte
			copy(footer[:8], magic[:])
			if err := readSnapshotArtifactBytes(r, footer[8:], "footer"); err != nil {
				return SnapshotArtifactManifest{}, err
			}
			if err := validateSnapshotArtifactFooter(
				footer[:], manifest, previousDigest, encodedBytes, stateRowSeen,
			); err != nil {
				return SnapshotArtifactManifest{}, err
			}
			manifest.EncodedBytes = encodedBytes + snapshotArtifactFooterBytes
			copy(manifest.Digest[:], footer[128:160])
			var trailing [1]byte
			n, readErr := io.ReadFull(r, trailing[:])
			if n != 0 || readErr == nil {
				return SnapshotArtifactManifest{}, fmt.Errorf("%w: trailing bytes", ErrSnapshotArtifact)
			}
			if readErr != io.EOF {
				return SnapshotArtifactManifest{}, readErr
			}
			return manifest, nil
		default:
			return SnapshotArtifactManifest{}, fmt.Errorf("%w: record magic", ErrSnapshotArtifact)
		}
	}
}

type snapshotArtifactChunk struct {
	Sequence   uint64
	Collection SnapshotArtifactCollection
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
		binary.LittleEndian.Uint16(fixed[10:12]) != snapshotArtifactHeaderFixedBytes ||
		binary.LittleEndian.Uint16(fixed[22:24]) != 0 || !allZero(fixed[32:64]) {
		return SnapshotArtifactManifest{}, nil, 0, fmt.Errorf("%w: header", ErrSnapshotArtifact)
	}
	total := uint64(binary.LittleEndian.Uint32(fixed[12:16]))
	stateBytes := uint64(binary.LittleEndian.Uint32(fixed[16:20]))
	nameBytes := uint64(binary.LittleEndian.Uint16(fixed[20:22]))
	target := uint64(binary.LittleEndian.Uint32(fixed[24:28]))
	if total != snapshotArtifactHeaderFixedBytes+stateBytes+nameBytes+sha256.Size ||
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
	expectedStateDocument := wrapJSONHex(nil, stateEnvelope)
	return SnapshotArtifactManifest{
		State: state, UserCollection: name, TargetChunkBytes: uint32(target),
		HeaderDigest: storedDigest, LastChunkDigest: storedDigest,
	}, expectedStateDocument, total, nil
}

func validateSnapshotArtifactChunkHeader(
	header []byte,
	expectedSequence uint64,
	previousDigest [sha256.Size]byte,
	currentCollection SnapshotArtifactCollection,
) (snapshotArtifactChunk, int, error) {
	if len(header) != snapshotArtifactChunkHeaderBytes ||
		!bytes.Equal(header[0:8], snapshotArtifactChunkMagic[:]) ||
		binary.LittleEndian.Uint16(header[8:10]) != snapshotArtifactFormat ||
		binary.LittleEndian.Uint16(header[10:12]) != snapshotArtifactChunkHeaderBytes ||
		header[25] != 0 || binary.LittleEndian.Uint16(header[26:28]) != 0 ||
		!allZero(header[36:48]) || !allZero(header[80:96]) {
		return snapshotArtifactChunk{}, 0, fmt.Errorf("%w: chunk header", ErrSnapshotArtifact)
	}
	sequence := binary.LittleEndian.Uint64(header[16:24])
	collection := SnapshotArtifactCollection(header[24])
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
	if collection != SnapshotArtifactSystem && collection != SnapshotArtifactUser ||
		collection < currentCollection {
		return snapshotArtifactChunk{}, 0, fmt.Errorf("%w: chunk collection order", ErrSnapshotArtifact)
	}
	return snapshotArtifactChunk{
		Sequence: sequence, Collection: collection, Rows: rows,
	}, int(payloadBytes), nil
}

func consumeSnapshotArtifactRows(
	chunk snapshotArtifactChunk,
	payload []byte,
	visit func(collection SnapshotArtifactCollection, key, value []byte) error,
	previousKey []byte,
	previousKeyBytes *int,
	expectedStateDocument []byte,
	stateRowAlreadySeen bool,
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
			valueBytes == 0 || valueBytes > replication.MaxMutationValueBytes ||
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
			case len(key) != sha256.Size+1 || key[0] != 1:
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
	return stateRowSeen, nil
}

func validateSnapshotArtifactFooter(
	footer []byte,
	manifest SnapshotArtifactManifest,
	previousDigest [sha256.Size]byte,
	encodedBeforeFooter uint64,
	stateRowSeen bool,
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
	var storedPrevious, storedHeader [sha256.Size]byte
	copy(storedPrevious[:], footer[64:96])
	copy(storedHeader[:], footer[96:128])
	wantDigest := snapshotArtifactDigest(snapshotArtifactFooterDomain, footer[:128])
	var storedDigest [sha256.Size]byte
	copy(storedDigest[:], footer[128:160])
	if storedDigest != wantDigest ||
		binary.LittleEndian.Uint64(footer[16:24]) != manifest.Chunks ||
		binary.LittleEndian.Uint64(footer[24:32]) != manifest.Chunks ||
		binary.LittleEndian.Uint64(footer[32:40]) != manifest.SystemRows ||
		binary.LittleEndian.Uint64(footer[40:48]) != manifest.UserRows ||
		binary.LittleEndian.Uint64(footer[48:56]) != manifest.PayloadBytes ||
		binary.LittleEndian.Uint64(footer[56:64]) != encodedBeforeFooter+snapshotArtifactFooterBytes ||
		storedPrevious != previousDigest || storedHeader != manifest.HeaderDigest {
		return fmt.Errorf("%w: footer totals or digest", ErrSnapshotArtifact)
	}
	if !stateRowSeen || manifest.SystemRows != manifest.State.CompletionCount+1 {
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
