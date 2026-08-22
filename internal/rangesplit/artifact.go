package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	childArtifactFormat           = uint16(1)
	childArtifactHeaderFixedBytes = 272
	childArtifactChunkHeaderBytes = 96
	childArtifactFooterBytes      = 160
	childArtifactRowHeaderBytes   = 8

	// DefaultChildArtifactChunkBytes keeps transfer memory bounded while
	// amortizing checksums and durable receiver commits over large blocks.
	DefaultChildArtifactChunkBytes = 4 << 20
	// MinChildArtifactChunkBytes prevents pathological framing overhead.
	MinChildArtifactChunkBytes = 4 << 10
	// MaxChildArtifactChunkBytes holds one maximum admitted row frame. Rows are
	// never fragmented, even when one exceeds the ordinary target.
	MaxChildArtifactChunkBytes = replication.MaxMutationKeyBytes +
		replication.MaxMutationValueBytes + childArtifactRowHeaderBytes
	MaxChildArtifactHeaderBytes = 1 << 20
)

var (
	ErrChildArtifact      = errors.New("rangesplit: corrupt child artifact")
	ErrChildArtifactBound = errors.New("rangesplit: child artifact exceeds its bounded format")

	childArtifactHeaderMagic = [8]byte{'V', 'D', 'B', 'S', 'P', 'L', 'T', 0}
	childArtifactChunkMagic  = [8]byte{'V', 'D', 'B', 'S', 'P', 'C', 'H', 0}
	childArtifactFooterMagic = [8]byte{'V', 'D', 'B', 'S', 'P', 'E', 'N', 0}

	childArtifactHeaderDomain = []byte("vibedb/range-split/child-header\x00")
	childArtifactChunkDomain  = []byte("vibedb/range-split/child-chunk\x00")
	childArtifactFooterDomain = []byte("vibedb/range-split/child-footer\x00")
)

// ChildArtifactSourceCut is the exact immutable source image partitioned into
// a child. It is sufficient to fence later tail translation without copying a
// complete replicated State into every child artifact.
type ChildArtifactSourceCut struct {
	DataChainDigest [sha256.Size]byte
	BaseDigest      [sha256.Size]byte
	EntryDigest     [sha256.Size]byte
	Applied         uint64
	Term            uint64
	RouteGeneration uint64
}

// ChildArtifactManifest certifies one non-serving child image. It grants no
// ownership or routing authority.
type ChildArtifactManifest struct {
	Present              bool
	Child                uint8
	PlanDigest           [sha256.Size]byte
	PlacementDigest      [sha256.Size]byte
	Source               ChildArtifactSourceCut
	TargetRoutingVersion distribution.RoutingVersion
	Descriptor           ChildArtifactDescriptor
	TargetChunkBytes     uint32
	Chunks               uint64
	Rows                 uint64
	RowBytes             uint64
	PayloadBytes         uint64
	EncodedBytes         uint64
	HeaderDigest         [sha256.Size]byte
	LastChunkDigest      [sha256.Size]byte
	Digest               [sha256.Size]byte
}

// ChildArtifactDescriptor is the allocation-free scalar child identity. The
// exact ordered endpoint set remains authenticated by PlanDigest and the
// artifact header; LeaderCount makes accidental plan/header disagreement
// visible without cloning the endpoint slice into every manifest.
type ChildArtifactDescriptor struct {
	Range                distribution.KeyRange
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
	OwnershipEpoch       distribution.OwnershipEpoch
	LeaderCount          uint16
}

// ChildArtifactSet is the complete result of one source scan. The retained
// child has a zero manifest because its source rows are deliberately not
// copied.
type ChildArtifactSet struct {
	Partition PartitionStats
	Children  [autosplit.MaxSplitChildren]ChildArtifactManifest
}

// ChildArtifactOptions controls deterministic chunk packing. Writers and
// PayloadBuffers are indexed by child ordinal. The retained writer must be nil;
// every non-retained writer must be non-nil. A supplied buffer must have
// capacity through TargetChunkBytes. Capacity through
// MaxChildArtifactChunkBytes also prevents growth for an exceptional max row.
type ChildArtifactOptions struct {
	TargetChunkBytes int
	Writers          [autosplit.MaxSplitChildren]io.Writer
	PayloadBuffers   [autosplit.MaxSplitChildren][]byte
}

// ChildArtifactWorkspace owns the one-pass partition workspace and fixed
// writer callbacks. Reuse it serially and do not copy it after first use.
type ChildArtifactWorkspace struct {
	partition PartitionWorkspace
	writers   [autosplit.MaxSplitChildren]childArtifactWriter
	sinks     [autosplit.MaxSplitChildren]RowSink
	bound     [autosplit.MaxSplitChildren]*childArtifactWriter
}

// ChildArtifactCheckpoint identifies one fully verified chunk. A durable
// receiver should commit the rows and this checkpoint atomically in Rows.
type ChildArtifactCheckpoint struct {
	Child        uint8
	Sequence     uint64
	Rows         uint64
	PayloadBytes uint64
	EndOffset    uint64
	Digest       [sha256.Size]byte
}

// ChildArtifactRows is one completely checksum-, framing-, ordering-, and
// placement-verified chunk. Its payload is borrowed only for the callback.
type ChildArtifactRows struct {
	payload []byte
	rows    uint64
}

// Len returns the exact row count.
func (r ChildArtifactRows) Len() uint64 { return r.rows }

// Iterator returns a zero-allocation iterator over the already verified rows.
func (r ChildArtifactRows) Iterator() ChildArtifactRowIterator {
	return ChildArtifactRowIterator{payload: r.payload, remaining: r.rows}
}

// ChildArtifactRowIterator walks borrowed key/value frames.
type ChildArtifactRowIterator struct {
	payload   []byte
	cursor    int
	remaining uint64
}

// Next returns one borrowed row, or ok=false after the declared count.
func (i *ChildArtifactRowIterator) Next() (key, value []byte, ok bool) {
	if i == nil || i.remaining == 0 {
		return nil, nil, false
	}
	keyBytes := int(binary.LittleEndian.Uint32(i.payload[i.cursor : i.cursor+4]))
	valueBytes := int(binary.LittleEndian.Uint32(i.payload[i.cursor+4 : i.cursor+8]))
	i.cursor += childArtifactRowHeaderBytes
	keyEnd := i.cursor + keyBytes
	valueEnd := keyEnd + valueBytes
	key, value = i.payload[i.cursor:keyEnd], i.payload[keyEnd:valueEnd]
	i.cursor = valueEnd
	i.remaining--
	return key, value, true
}

// ChildArtifactCallbacks receives only complete semantic chunks. Rows must
// return after row effects and checkpoint are durably ordered together.
type ChildArtifactCallbacks struct {
	Rows func(ChildArtifactCheckpoint, ChildArtifactRows) error
}

// ChildArtifactVerifyWorkspace retains bounded transfer and vibejson parsing
// storage across artifacts. Buffers are caller-visible only to allow explicit
// capacity provisioning; their contents are overwritten.
type ChildArtifactVerifyWorkspace struct {
	HeaderBuffer   []byte
	PayloadBuffer  []byte
	document       distribution.DocumentPointWorkspace
	previousKey    [replication.MaxMutationKeyBytes]byte
	headerFixed    [childArtifactHeaderFixedBytes]byte
	recordMagic    [8]byte
	chunkHeader    [childArtifactChunkHeaderBytes]byte
	storedDigest   [sha256.Size]byte
	computedDigest [sha256.Size]byte
	footer         [childArtifactFooterBytes]byte
	trailing       [1]byte
	hasher         hash.Hash
}

type childArtifactWriter struct {
	w                io.Writer
	target           int
	payload          []byte
	headerBuffer     []byte
	child            uint8
	chunkRows        uint32
	chunks           uint64
	rows             uint64
	rowBytes         uint64
	payloadBytes     uint64
	encodedBytes     uint64
	headerDigest     [sha256.Size]byte
	previousDigest   [sha256.Size]byte
	previousKey      [replication.MaxMutationKeyBytes]byte
	previousKeyBytes uint16
	chunkHeader      [childArtifactChunkHeaderBytes]byte
	footer           [childArtifactFooterBytes]byte
	computedDigest   [sha256.Size]byte
	hasher           hash.Hash
}

// WriteChildArtifacts partitions snapshot once and writes one deterministic,
// hash-chained artifact per non-retained child. Any error invalidates every
// partial output; callers must discard or truncate all of them.
func (p *Partitioner) WriteChildArtifacts(
	snapshot *replicatedstate.ReadSnapshot,
	options ChildArtifactOptions,
	workspace *ChildArtifactWorkspace,
) (ChildArtifactSet, error) {
	if p == nil || snapshot == nil || workspace == nil {
		return ChildArtifactSet{}, ErrInvalidPartition
	}
	user, ok := snapshot.Collection(p.collection)
	if !ok || user == nil {
		return ChildArtifactSet{}, ErrInvalidPartition
	}
	return p.writeChildArtifacts(snapshot.State(), user.RangeRaw, options, workspace)
}

func (p *Partitioner) writeChildArtifacts(
	state replicatedstate.State,
	rangeRows func(func(key, value []byte) error) error,
	options ChildArtifactOptions,
	workspace *ChildArtifactWorkspace,
) (ChildArtifactSet, error) {
	if p == nil || rangeRows == nil || workspace == nil || !p.matchesSource(state) {
		return ChildArtifactSet{}, ErrSourceFence
	}
	target, err := normalizeChildArtifactTarget(options.TargetChunkBytes)
	if err != nil {
		return ChildArtifactSet{}, err
	}
	cut := sourceCut(state)
	for child := 0; child < int(p.childCount); child++ {
		writer := &workspace.writers[child]
		if child == int(p.retained) {
			if options.Writers[child] != nil {
				return ChildArtifactSet{}, ErrInvalidPartition
			}
			workspace.sinks[child] = nil
			*writer = childArtifactWriter{}
			continue
		}
		if options.Writers[child] == nil {
			return ChildArtifactSet{}, ErrInvalidPartition
		}
		if err := writer.prepare(
			p, uint8(child), cut, target, options.Writers[child],
			options.PayloadBuffers[child],
		); err != nil {
			return ChildArtifactSet{}, err
		}
		if workspace.bound[child] != writer {
			workspace.sinks[child] = writer.accept
			workspace.bound[child] = writer
		}
	}
	for child := 0; child < int(p.childCount); child++ {
		if child == int(p.retained) {
			continue
		}
		if err := workspace.writers[child].writeHeader(); err != nil {
			return ChildArtifactSet{}, err
		}
	}
	stats, err := p.partitionRows(
		state, rangeRows, workspace.sinks[:p.childCount], &workspace.partition,
	)
	if err != nil {
		return ChildArtifactSet{}, err
	}
	set := ChildArtifactSet{Partition: stats}
	for child := 0; child < int(p.childCount); child++ {
		if child == int(p.retained) {
			continue
		}
		writer := &workspace.writers[child]
		if writer.rows != stats.Rows[child] || writer.rowBytes != stats.Bytes[child] {
			return ChildArtifactSet{}, ErrChildArtifact
		}
		manifest, err := writer.finish(p, cut)
		if err != nil {
			return ChildArtifactSet{}, err
		}
		set.Children[child] = manifest
	}
	return set, nil
}

func sourceCut(state replicatedstate.State) ChildArtifactSourceCut {
	return ChildArtifactSourceCut{
		DataChainDigest: state.DataChainDigest, BaseDigest: state.SnapshotBaseDigest,
		EntryDigest: state.LastEntryDigest,
		Applied:     state.Applied, Term: state.LastTerm,
		RouteGeneration: state.Binding.RouteGeneration,
	}
}

func normalizeChildArtifactTarget(target int) (int, error) {
	if target == 0 {
		target = DefaultChildArtifactChunkBytes
	}
	if target < MinChildArtifactChunkBytes || target > MaxChildArtifactChunkBytes {
		return 0, fmt.Errorf("%w: target chunk bytes %d", ErrChildArtifactBound, target)
	}
	return target, nil
}

func (w *childArtifactWriter) prepare(
	p *Partitioner,
	child uint8,
	cut ChildArtifactSourceCut,
	target int,
	output io.Writer,
	buffer []byte,
) error {
	if output == nil || int(child) >= int(p.childCount) || child == p.retained {
		return ErrInvalidPartition
	}
	reusablePayload, reusableHeader, reusableHasher := w.payload, w.headerBuffer, w.hasher
	if buffer == nil {
		if cap(reusablePayload) >= target {
			buffer = reusablePayload[:0]
		} else {
			buffer = make([]byte, 0, target)
		}
	} else if cap(buffer) < target {
		return fmt.Errorf(
			"%w: child %d payload capacity %d below target %d",
			ErrChildArtifactBound, child, cap(buffer), target,
		)
	} else {
		buffer = buffer[:0]
	}
	*w = childArtifactWriter{
		w: output, target: target, payload: buffer, headerBuffer: reusableHeader, child: child,
	}
	if reusableHasher == nil {
		reusableHasher = sha256.New()
	}
	w.hasher = reusableHasher
	header, digest, err := makeChildArtifactHeader(
		p, child, cut, target, w.headerBuffer, w.hasher, &w.computedDigest,
	)
	if err != nil {
		return err
	}
	w.headerBuffer = header
	w.headerDigest = digest
	w.previousDigest = digest
	return nil
}

func (w *childArtifactWriter) writeHeader() error {
	header := w.headerBuffer
	if len(header) < childArtifactHeaderFixedBytes+sha256.Size {
		return ErrChildArtifact
	}
	if err := writeChildArtifactBytes(w.w, header); err != nil {
		return err
	}
	w.encodedBytes = uint64(len(header))
	return nil
}

func (w *childArtifactWriter) accept(key, value []byte) error {
	rowFrameBytes, ok := childArtifactRowBytes(key, value)
	if !ok {
		return fmt.Errorf("%w: row", ErrChildArtifactBound)
	}
	if w.previousKeyBytes != 0 &&
		bytes.Compare(w.previousKey[:w.previousKeyBytes], key) >= 0 {
		return fmt.Errorf("%w: source keys are not strictly ordered", ErrChildArtifact)
	}
	if len(w.payload) != 0 && rowFrameBytes > w.target-len(w.payload) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	if w.chunkRows == math.MaxUint32 || w.rows == math.MaxUint64 {
		return fmt.Errorf("%w: row counters", ErrChildArtifactBound)
	}
	rawBytes := uint64(len(key)) + uint64(len(value))
	if w.rowBytes > math.MaxUint64-rawBytes {
		return fmt.Errorf("%w: row bytes", ErrChildArtifactBound)
	}
	w.payload = binary.LittleEndian.AppendUint32(w.payload, uint32(len(key)))
	w.payload = binary.LittleEndian.AppendUint32(w.payload, uint32(len(value)))
	w.payload = append(w.payload, key...)
	w.payload = append(w.payload, value...)
	copy(w.previousKey[:], key)
	w.previousKeyBytes = uint16(len(key))
	w.chunkRows++
	w.rows++
	w.rowBytes += rawBytes
	return nil
}

func childArtifactRowBytes(key, value []byte) (int, bool) {
	if len(key) == 0 || len(key) > replication.MaxMutationKeyBytes ||
		len(value) == 0 || len(value) > replication.MaxMutationValueBytes {
		return 0, false
	}
	rowBytes := childArtifactRowHeaderBytes + len(key) + len(value)
	return rowBytes, rowBytes <= MaxChildArtifactChunkBytes
}

func (w *childArtifactWriter) flush() error {
	if len(w.payload) == 0 {
		if w.chunkRows != 0 {
			return ErrChildArtifact
		}
		return nil
	}
	if w.chunkRows == 0 || len(w.payload) > MaxChildArtifactChunkBytes ||
		w.chunks == math.MaxUint64 {
		return fmt.Errorf("%w: chunk counters", ErrChildArtifactBound)
	}
	total := childArtifactChunkHeaderBytes + len(w.payload) + sha256.Size
	header := w.chunkHeader[:]
	copy(header[0:8], childArtifactChunkMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], childArtifactFormat)
	binary.LittleEndian.PutUint16(header[10:12], childArtifactChunkHeaderBytes)
	binary.LittleEndian.PutUint32(header[12:16], uint32(total))
	binary.LittleEndian.PutUint64(header[16:24], w.chunks)
	header[24] = w.child
	binary.LittleEndian.PutUint32(header[28:32], w.chunkRows)
	binary.LittleEndian.PutUint32(header[32:36], uint32(len(w.payload)))
	copy(header[48:80], w.previousDigest[:])
	childArtifactDigestPartsInto(
		w.hasher, &w.computedDigest, childArtifactChunkDomain, header, w.payload,
	)
	payloadBytes := uint64(len(w.payload))
	if w.payloadBytes > math.MaxUint64-payloadBytes ||
		w.encodedBytes > math.MaxUint64-uint64(total) {
		return fmt.Errorf("%w: artifact counters", ErrChildArtifactBound)
	}
	if err := writeChildArtifactBytes(w.w, header); err != nil {
		return err
	}
	if err := writeChildArtifactBytes(w.w, w.payload); err != nil {
		return err
	}
	if err := writeChildArtifactBytes(w.w, w.computedDigest[:]); err != nil {
		return err
	}
	w.payloadBytes += payloadBytes
	w.encodedBytes += uint64(total)
	w.chunks++
	w.previousDigest = w.computedDigest
	w.payload = w.payload[:0]
	w.chunkRows = 0
	return nil
}

func (w *childArtifactWriter) finish(
	p *Partitioner,
	cut ChildArtifactSourceCut,
) (ChildArtifactManifest, error) {
	if err := w.flush(); err != nil {
		return ChildArtifactManifest{}, err
	}
	if w.encodedBytes > math.MaxUint64-childArtifactFooterBytes {
		return ChildArtifactManifest{}, fmt.Errorf("%w: encoded bytes", ErrChildArtifactBound)
	}
	totalBytes := w.encodedBytes + childArtifactFooterBytes
	footer := w.footer[:]
	copy(footer[0:8], childArtifactFooterMagic[:])
	binary.LittleEndian.PutUint16(footer[8:10], childArtifactFormat)
	binary.LittleEndian.PutUint16(footer[10:12], childArtifactFooterBytes)
	binary.LittleEndian.PutUint32(footer[12:16], childArtifactFooterBytes)
	binary.LittleEndian.PutUint64(footer[16:24], w.chunks)
	binary.LittleEndian.PutUint64(footer[24:32], w.rows)
	binary.LittleEndian.PutUint64(footer[32:40], w.rowBytes)
	binary.LittleEndian.PutUint64(footer[40:48], w.payloadBytes)
	binary.LittleEndian.PutUint64(footer[48:56], totalBytes)
	binary.LittleEndian.PutUint64(footer[56:64], uint64(w.child))
	copy(footer[64:96], w.previousDigest[:])
	copy(footer[96:128], w.headerDigest[:])
	childArtifactDigestPartsInto(
		w.hasher, &w.computedDigest, childArtifactFooterDomain, footer[:128], nil,
	)
	digest := w.computedDigest
	copy(footer[128:160], digest[:])
	if err := writeChildArtifactBytes(w.w, footer); err != nil {
		return ChildArtifactManifest{}, err
	}
	w.encodedBytes = totalBytes
	return ChildArtifactManifest{
		Present: true, Child: w.child, PlanDigest: p.digest,
		PlacementDigest: p.program.Digest(), Source: cut,
		TargetRoutingVersion: p.target, Descriptor: p.artifactDescriptor(w.child),
		TargetChunkBytes: uint32(w.target), Chunks: w.chunks, Rows: w.rows,
		RowBytes: w.rowBytes, PayloadBytes: w.payloadBytes,
		EncodedBytes: w.encodedBytes, HeaderDigest: w.headerDigest,
		LastChunkDigest: w.previousDigest, Digest: digest,
	}, nil
}

func (p *Partitioner) artifactDescriptor(child uint8) ChildArtifactDescriptor {
	descriptor := p.children[child]
	return ChildArtifactDescriptor{
		Range: descriptor.Range, Shard: descriptor.Shard,
		AllocationGeneration: descriptor.AllocationGeneration,
		OwnershipEpoch:       descriptor.OwnershipEpoch,
		LeaderCount:          uint16(len(descriptor.Leaders)),
	}
}

func makeChildArtifactHeader(
	p *Partitioner,
	child uint8,
	cut ChildArtifactSourceCut,
	target int,
	buffer []byte,
	hasher hash.Hash,
	computed *[sha256.Size]byte,
) ([]byte, [sha256.Size]byte, error) {
	if p == nil || int(child) >= int(p.childCount) || child == p.retained ||
		cut.Applied == 0 || cut.Term == 0 || cut.RouteGeneration == 0 ||
		cut.DataChainDigest == ([sha256.Size]byte{}) ||
		cut.BaseDigest == ([sha256.Size]byte{}) ||
		cut.EntryDigest == ([sha256.Size]byte{}) {
		return nil, [sha256.Size]byte{}, ErrInvalidPartition
	}
	descriptor := p.children[child]
	if len(descriptor.Shard) == 0 || len(descriptor.Shard) > math.MaxUint16 ||
		len(p.collection) == 0 || len(p.collection) > math.MaxUint16 ||
		len(descriptor.Leaders) == 0 || len(descriptor.Leaders) > math.MaxUint16 {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: header identity", ErrChildArtifactBound)
	}
	total := childArtifactHeaderFixedBytes + len(descriptor.Shard) + len(p.collection) + sha256.Size
	for _, endpoint := range descriptor.Leaders {
		if len(endpoint) == 0 || len(endpoint) > math.MaxUint16 ||
			total > MaxChildArtifactHeaderBytes-2-len(endpoint) {
			return nil, [sha256.Size]byte{}, fmt.Errorf("%w: endpoint identity", ErrChildArtifactBound)
		}
		total += 2 + len(endpoint)
	}
	if total > MaxChildArtifactHeaderBytes {
		return nil, [sha256.Size]byte{}, fmt.Errorf("%w: header bytes", ErrChildArtifactBound)
	}
	var header []byte
	if cap(buffer) < total {
		header = make([]byte, total)
	} else {
		header = buffer[:total]
		clear(header)
	}
	copy(header[0:8], childArtifactHeaderMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], childArtifactFormat)
	binary.LittleEndian.PutUint16(header[10:12], childArtifactHeaderFixedBytes)
	binary.LittleEndian.PutUint32(header[12:16], uint32(total))
	binary.LittleEndian.PutUint32(header[16:20], uint32(target))
	binary.LittleEndian.PutUint32(header[20:24], MaxChildArtifactChunkBytes)
	header[24], header[25], header[26] = child, p.childCount, p.source.BucketBits
	binary.LittleEndian.PutUint16(header[28:30], uint16(len(descriptor.Shard)))
	binary.LittleEndian.PutUint16(header[30:32], uint16(len(p.collection)))
	binary.LittleEndian.PutUint16(header[32:34], uint16(len(descriptor.Leaders)))
	binary.LittleEndian.PutUint64(header[40:48], cut.Applied)
	binary.LittleEndian.PutUint64(header[48:56], cut.Term)
	binary.LittleEndian.PutUint64(header[56:64], cut.RouteGeneration)
	binary.LittleEndian.PutUint64(header[64:72], uint64(descriptor.AllocationGeneration))
	binary.LittleEndian.PutUint64(header[72:80], uint64(descriptor.OwnershipEpoch))
	binary.LittleEndian.PutUint64(header[80:88], uint64(p.target))
	copy(header[88:96], descriptor.Range.Start[:])
	copy(header[96:104], descriptor.Range.End.Point[:])
	if descriptor.Range.End.Max {
		header[104] = 1
	}
	copy(header[112:144], p.digest[:])
	placement := p.program.Digest()
	copy(header[144:176], placement[:])
	copy(header[176:208], cut.DataChainDigest[:])
	copy(header[208:240], cut.BaseDigest[:])
	copy(header[240:272], cut.EntryDigest[:])
	cursor := childArtifactHeaderFixedBytes
	cursor += copy(header[cursor:], descriptor.Shard)
	cursor += copy(header[cursor:], p.collection)
	for _, endpoint := range descriptor.Leaders {
		binary.LittleEndian.PutUint16(header[cursor:cursor+2], uint16(len(endpoint)))
		cursor += 2
		cursor += copy(header[cursor:], endpoint)
	}
	childArtifactDigestPartsInto(
		hasher, computed, childArtifactHeaderDomain, header[:cursor], nil,
	)
	digest := *computed
	copy(header[cursor:], digest[:])
	return header, digest, nil
}

// VerifyChildArtifact authenticates one complete artifact, proves strict key
// order, and recomputes every row's vibejson placement before exposing any
// chunk to the durable receiver. It never publishes topology.
func (p *Partitioner) VerifyChildArtifact(
	r io.Reader,
	child uint8,
	callbacks ChildArtifactCallbacks,
	workspace *ChildArtifactVerifyWorkspace,
) (ChildArtifactManifest, error) {
	if p == nil || r == nil || int(child) >= int(p.childCount) || child == p.retained {
		return ChildArtifactManifest{}, ErrInvalidPartition
	}
	if workspace == nil {
		workspace = &ChildArtifactVerifyWorkspace{}
	}
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	cut, target, headerDigest, headerBytes, err := p.readChildArtifactHeader(r, child, workspace)
	if err != nil {
		return ChildArtifactManifest{}, err
	}
	manifest := ChildArtifactManifest{
		Present: true, Child: child, PlanDigest: p.digest,
		PlacementDigest: p.program.Digest(), Source: cut,
		TargetRoutingVersion: p.target, Descriptor: p.artifactDescriptor(child),
		TargetChunkBytes: uint32(target), EncodedBytes: uint64(headerBytes),
		HeaderDigest: headerDigest, LastChunkDigest: headerDigest,
	}
	workspace.previousKey = [replication.MaxMutationKeyBytes]byte{}
	previousKeyBytes := 0
	for {
		magic := workspace.recordMagic[:]
		if err := readChildArtifactBytes(r, magic, "record magic"); err != nil {
			return ChildArtifactManifest{}, err
		}
		switch {
		case bytes.Equal(magic, childArtifactChunkMagic[:]):
			header := workspace.chunkHeader[:]
			copy(header[:8], magic)
			if err := readChildArtifactBytes(r, header[8:], "chunk header"); err != nil {
				return ChildArtifactManifest{}, err
			}
			rows, payloadBytes, err := validateChildArtifactChunkHeader(
				header, child, manifest.Chunks, manifest.LastChunkDigest,
			)
			if err != nil {
				return ChildArtifactManifest{}, err
			}
			if cap(workspace.PayloadBuffer) < payloadBytes {
				workspace.PayloadBuffer = make([]byte, payloadBytes)
			} else {
				workspace.PayloadBuffer = workspace.PayloadBuffer[:payloadBytes]
			}
			payload := workspace.PayloadBuffer
			if err := readChildArtifactBytes(r, payload, "chunk payload"); err != nil {
				return ChildArtifactManifest{}, err
			}
			if err := readChildArtifactBytes(r, workspace.storedDigest[:], "chunk digest"); err != nil {
				return ChildArtifactManifest{}, err
			}
			storedDigest := workspace.storedDigest
			childArtifactDigestPartsInto(
				workspace.hasher, &workspace.computedDigest,
				childArtifactChunkDomain, header, payload,
			)
			if storedDigest != workspace.computedDigest {
				return ChildArtifactManifest{}, fmt.Errorf("%w: chunk digest", ErrChildArtifact)
			}
			candidateKey := workspace.previousKey
			candidateKeyBytes := previousKeyBytes
			rowBytes, err := p.validateChildArtifactRows(
				child, rows, payload, candidateKey[:], &candidateKeyBytes, &workspace.document,
			)
			if err != nil {
				return ChildArtifactManifest{}, err
			}
			chunkBytes := uint64(childArtifactChunkHeaderBytes + payloadBytes + sha256.Size)
			if manifest.EncodedBytes > math.MaxUint64-chunkBytes ||
				manifest.PayloadBytes > math.MaxUint64-uint64(payloadBytes) ||
				manifest.RowBytes > math.MaxUint64-rowBytes ||
				manifest.Rows > math.MaxUint64-rows {
				return ChildArtifactManifest{}, fmt.Errorf("%w: counters", ErrChildArtifactBound)
			}
			checkpoint := ChildArtifactCheckpoint{
				Child: child, Sequence: manifest.Chunks, Rows: rows,
				PayloadBytes: uint64(payloadBytes),
				EndOffset:    manifest.EncodedBytes + chunkBytes, Digest: storedDigest,
			}
			if callbacks.Rows != nil {
				if err := callbacks.Rows(checkpoint, ChildArtifactRows{payload: payload, rows: rows}); err != nil {
					return ChildArtifactManifest{}, err
				}
			}
			workspace.previousKey = candidateKey
			previousKeyBytes = candidateKeyBytes
			manifest.Chunks++
			manifest.Rows += rows
			manifest.RowBytes += rowBytes
			manifest.PayloadBytes += uint64(payloadBytes)
			manifest.EncodedBytes = checkpoint.EndOffset
			manifest.LastChunkDigest = storedDigest
		case bytes.Equal(magic, childArtifactFooterMagic[:]):
			footer := workspace.footer[:]
			copy(footer[:8], magic)
			if err := readChildArtifactBytes(r, footer[8:], "footer"); err != nil {
				return ChildArtifactManifest{}, err
			}
			digest, err := validateChildArtifactFooter(
				footer, manifest, workspace.hasher, &workspace.computedDigest,
			)
			if err != nil {
				return ChildArtifactManifest{}, err
			}
			manifest.EncodedBytes += childArtifactFooterBytes
			manifest.Digest = digest
			n, readErr := io.ReadFull(r, workspace.trailing[:])
			if n != 0 || readErr == nil {
				return ChildArtifactManifest{}, fmt.Errorf("%w: trailing bytes", ErrChildArtifact)
			}
			if readErr != io.EOF {
				return ChildArtifactManifest{}, readErr
			}
			return manifest, nil
		default:
			return ChildArtifactManifest{}, fmt.Errorf("%w: record magic", ErrChildArtifact)
		}
	}
}

func (p *Partitioner) readChildArtifactHeader(
	r io.Reader,
	child uint8,
	workspace *ChildArtifactVerifyWorkspace,
) (ChildArtifactSourceCut, int, [sha256.Size]byte, int, error) {
	fixed := workspace.headerFixed[:]
	if err := readChildArtifactBytes(r, fixed, "header"); err != nil {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0, err
	}
	total := int(binary.LittleEndian.Uint32(fixed[12:16]))
	if total < childArtifactHeaderFixedBytes+sha256.Size || total > MaxChildArtifactHeaderBytes {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0,
			fmt.Errorf("%w: header bytes", ErrChildArtifactBound)
	}
	if cap(workspace.HeaderBuffer) < total {
		workspace.HeaderBuffer = make([]byte, total)
	} else {
		workspace.HeaderBuffer = workspace.HeaderBuffer[:total]
	}
	header := workspace.HeaderBuffer
	copy(header, fixed)
	if err := readChildArtifactBytes(r, header[len(fixed):], "header body"); err != nil {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0, err
	}
	if !bytes.Equal(header[0:8], childArtifactHeaderMagic[:]) ||
		binary.LittleEndian.Uint16(header[8:10]) != childArtifactFormat ||
		binary.LittleEndian.Uint16(header[10:12]) != childArtifactHeaderFixedBytes ||
		header[24] != child || header[25] != p.childCount ||
		header[26] != p.source.BucketBits || header[27] != 0 ||
		binary.LittleEndian.Uint16(header[34:36]) != 0 || !allChildArtifactZero(header[36:40]) ||
		!allChildArtifactZero(header[105:112]) {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0,
			fmt.Errorf("%w: header", ErrChildArtifact)
	}
	target, err := normalizeChildArtifactTarget(int(binary.LittleEndian.Uint32(header[16:20])))
	if err != nil || binary.LittleEndian.Uint32(header[20:24]) != MaxChildArtifactChunkBytes {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0,
			fmt.Errorf("%w: header chunk bounds", ErrChildArtifactBound)
	}
	var storedDigest [sha256.Size]byte
	copy(storedDigest[:], header[total-sha256.Size:])
	childArtifactDigestPartsInto(
		workspace.hasher, &workspace.computedDigest,
		childArtifactHeaderDomain, header[:total-sha256.Size], nil,
	)
	if storedDigest != workspace.computedDigest {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0,
			fmt.Errorf("%w: header digest", ErrChildArtifact)
	}
	descriptor := p.children[child]
	var start, end distribution.KeyspacePoint
	copy(start[:], header[88:96])
	copy(end[:], header[96:104])
	endMax := header[104]
	var planDigest, placementDigest [sha256.Size]byte
	copy(planDigest[:], header[112:144])
	copy(placementDigest[:], header[144:176])
	if binary.LittleEndian.Uint64(header[64:72]) != uint64(descriptor.AllocationGeneration) ||
		binary.LittleEndian.Uint64(header[72:80]) != uint64(descriptor.OwnershipEpoch) ||
		binary.LittleEndian.Uint64(header[80:88]) != uint64(p.target) ||
		start != descriptor.Range.Start || end != descriptor.Range.End.Point ||
		endMax > 1 || (endMax == 1) != descriptor.Range.End.Max ||
		planDigest != p.digest || placementDigest != p.program.Digest() {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0,
			fmt.Errorf("%w: child identity", ErrChildArtifact)
	}
	shardBytes := int(binary.LittleEndian.Uint16(header[28:30]))
	collectionBytes := int(binary.LittleEndian.Uint16(header[30:32]))
	leaders := int(binary.LittleEndian.Uint16(header[32:34]))
	cursor := childArtifactHeaderFixedBytes
	if shardBytes != len(descriptor.Shard) || collectionBytes != len(p.collection) ||
		leaders != len(descriptor.Leaders) || cursor+shardBytes+collectionBytes > total-sha256.Size ||
		!bytes.Equal(header[cursor:cursor+shardBytes], []byte(descriptor.Shard)) {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0,
			fmt.Errorf("%w: descriptor identity", ErrChildArtifact)
	}
	cursor += shardBytes
	if !bytes.Equal(header[cursor:cursor+collectionBytes], []byte(p.collection)) {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0,
			fmt.Errorf("%w: collection identity", ErrChildArtifact)
	}
	cursor += collectionBytes
	for _, endpoint := range descriptor.Leaders {
		if cursor > total-sha256.Size-2 {
			return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0, ErrChildArtifact
		}
		endpointBytes := int(binary.LittleEndian.Uint16(header[cursor : cursor+2]))
		cursor += 2
		if endpointBytes != len(endpoint) || cursor+endpointBytes > total-sha256.Size ||
			!bytes.Equal(header[cursor:cursor+endpointBytes], []byte(endpoint)) {
			return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0,
				fmt.Errorf("%w: endpoint identity", ErrChildArtifact)
		}
		cursor += endpointBytes
	}
	if cursor != total-sha256.Size {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0,
			fmt.Errorf("%w: trailing header identity", ErrChildArtifact)
	}
	cut := ChildArtifactSourceCut{
		Applied:         binary.LittleEndian.Uint64(header[40:48]),
		Term:            binary.LittleEndian.Uint64(header[48:56]),
		RouteGeneration: binary.LittleEndian.Uint64(header[56:64]),
	}
	copy(cut.DataChainDigest[:], header[176:208])
	copy(cut.BaseDigest[:], header[208:240])
	copy(cut.EntryDigest[:], header[240:272])
	if cut.Applied == 0 || cut.Term == 0 || cut.RouteGeneration == 0 ||
		cut.DataChainDigest == ([sha256.Size]byte{}) || cut.BaseDigest == ([sha256.Size]byte{}) ||
		cut.EntryDigest == ([sha256.Size]byte{}) {
		return ChildArtifactSourceCut{}, 0, [sha256.Size]byte{}, 0,
			fmt.Errorf("%w: source cut", ErrChildArtifact)
	}
	return cut, target, storedDigest, total, nil
}

func validateChildArtifactChunkHeader(
	header []byte,
	child uint8,
	expectedSequence uint64,
	previousDigest [sha256.Size]byte,
) (uint64, int, error) {
	if len(header) != childArtifactChunkHeaderBytes ||
		!bytes.Equal(header[0:8], childArtifactChunkMagic[:]) ||
		binary.LittleEndian.Uint16(header[8:10]) != childArtifactFormat ||
		binary.LittleEndian.Uint16(header[10:12]) != childArtifactChunkHeaderBytes ||
		header[24] != child || !allChildArtifactZero(header[25:28]) ||
		!allChildArtifactZero(header[36:48]) || !allChildArtifactZero(header[80:96]) {
		return 0, 0, fmt.Errorf("%w: chunk header", ErrChildArtifact)
	}
	sequence := binary.LittleEndian.Uint64(header[16:24])
	rows := uint64(binary.LittleEndian.Uint32(header[28:32]))
	payloadBytes := uint64(binary.LittleEndian.Uint32(header[32:36]))
	total := uint64(binary.LittleEndian.Uint32(header[12:16]))
	var storedPrevious [sha256.Size]byte
	copy(storedPrevious[:], header[48:80])
	if sequence != expectedSequence || rows == 0 || payloadBytes == 0 ||
		payloadBytes > MaxChildArtifactChunkBytes ||
		total != childArtifactChunkHeaderBytes+payloadBytes+sha256.Size ||
		storedPrevious != previousDigest {
		return 0, 0, fmt.Errorf("%w: chunk sequence or bounds", ErrChildArtifact)
	}
	return rows, int(payloadBytes), nil
}

func (p *Partitioner) validateChildArtifactRows(
	child uint8,
	rows uint64,
	payload []byte,
	previousKey []byte,
	previousKeyBytes *int,
	document *distribution.DocumentPointWorkspace,
) (uint64, error) {
	cursor := 0
	rowBytes := uint64(0)
	for row := uint64(0); row < rows; row++ {
		if cursor > len(payload)-childArtifactRowHeaderBytes {
			return 0, fmt.Errorf("%w: truncated row header", ErrChildArtifact)
		}
		keyBytes := uint64(binary.LittleEndian.Uint32(payload[cursor : cursor+4]))
		valueBytes := uint64(binary.LittleEndian.Uint32(payload[cursor+4 : cursor+8]))
		cursor += childArtifactRowHeaderBytes
		if keyBytes == 0 || keyBytes > replication.MaxMutationKeyBytes ||
			valueBytes == 0 || valueBytes > replication.MaxMutationValueBytes ||
			keyBytes+valueBytes > uint64(len(payload)-cursor) {
			return 0, fmt.Errorf("%w: row bounds", ErrChildArtifactBound)
		}
		keyEnd := cursor + int(keyBytes)
		valueEnd := keyEnd + int(valueBytes)
		key, value := payload[cursor:keyEnd], payload[keyEnd:valueEnd]
		cursor = valueEnd
		if *previousKeyBytes != 0 && bytes.Compare(previousKey[:*previousKeyBytes], key) >= 0 {
			return 0, fmt.Errorf("%w: rows not strictly ordered", ErrChildArtifact)
		}
		point, err := p.program.Point(value, document)
		if err != nil || p.childFor(point) != int(child) {
			return 0, fmt.Errorf("%w: row placement", ErrChildArtifact)
		}
		copy(previousKey, key)
		*previousKeyBytes = len(key)
		rawBytes := keyBytes + valueBytes
		if rowBytes > math.MaxUint64-rawBytes {
			return 0, fmt.Errorf("%w: row bytes", ErrChildArtifactBound)
		}
		rowBytes += rawBytes
	}
	if cursor != len(payload) {
		return 0, fmt.Errorf("%w: trailing chunk payload", ErrChildArtifact)
	}
	return rowBytes, nil
}

func validateChildArtifactFooter(
	footer []byte,
	manifest ChildArtifactManifest,
	hasher hash.Hash,
	computed *[sha256.Size]byte,
) ([sha256.Size]byte, error) {
	if len(footer) != childArtifactFooterBytes ||
		!bytes.Equal(footer[0:8], childArtifactFooterMagic[:]) ||
		binary.LittleEndian.Uint16(footer[8:10]) != childArtifactFormat ||
		binary.LittleEndian.Uint16(footer[10:12]) != childArtifactFooterBytes ||
		binary.LittleEndian.Uint32(footer[12:16]) != childArtifactFooterBytes ||
		manifest.EncodedBytes > math.MaxUint64-childArtifactFooterBytes {
		return [sha256.Size]byte{}, fmt.Errorf("%w: footer header", ErrChildArtifact)
	}
	var storedPrevious, storedHeader, storedDigest [sha256.Size]byte
	copy(storedPrevious[:], footer[64:96])
	copy(storedHeader[:], footer[96:128])
	copy(storedDigest[:], footer[128:160])
	childArtifactDigestPartsInto(
		hasher, computed, childArtifactFooterDomain, footer[:128], nil,
	)
	wantDigest := *computed
	if storedDigest != wantDigest ||
		binary.LittleEndian.Uint64(footer[16:24]) != manifest.Chunks ||
		binary.LittleEndian.Uint64(footer[24:32]) != manifest.Rows ||
		binary.LittleEndian.Uint64(footer[32:40]) != manifest.RowBytes ||
		binary.LittleEndian.Uint64(footer[40:48]) != manifest.PayloadBytes ||
		binary.LittleEndian.Uint64(footer[48:56]) != manifest.EncodedBytes+childArtifactFooterBytes ||
		binary.LittleEndian.Uint64(footer[56:64]) != uint64(manifest.Child) ||
		storedPrevious != manifest.LastChunkDigest || storedHeader != manifest.HeaderDigest {
		return [sha256.Size]byte{}, fmt.Errorf("%w: footer totals or digest", ErrChildArtifact)
	}
	return storedDigest, nil
}

func childArtifactDigest(domain, body []byte) [sha256.Size]byte {
	return childArtifactDigestWith(sha256.New(), domain, body)
}

func childArtifactDigestParts(domain, first, second []byte) [sha256.Size]byte {
	return childArtifactDigestPartsWith(sha256.New(), domain, first, second)
}

func childArtifactDigestWith(hasher hash.Hash, domain, body []byte) [sha256.Size]byte {
	return childArtifactDigestPartsWith(hasher, domain, body, nil)
}

func childArtifactDigestPartsWith(
	hasher hash.Hash,
	domain, first, second []byte,
) [sha256.Size]byte {
	var digest [sha256.Size]byte
	childArtifactDigestPartsInto(hasher, &digest, domain, first, second)
	return digest
}

func childArtifactDigestPartsInto(
	hasher hash.Hash,
	digest *[sha256.Size]byte,
	domain, first, second []byte,
) {
	hasher.Reset()
	_, _ = hasher.Write(domain)
	_, _ = hasher.Write(first)
	_, _ = hasher.Write(second)
	_ = hasher.Sum(digest[:0])
}

func writeChildArtifactBytes(w io.Writer, src []byte) error {
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

func readChildArtifactBytes(r io.Reader, dst []byte, field string) error {
	if _, err := io.ReadFull(r, dst); err != nil {
		return fmt.Errorf("%w: truncated %s: %w", ErrChildArtifact, field, err)
	}
	return nil
}

func allChildArtifactZero(src []byte) bool {
	for _, value := range src {
		if value != 0 {
			return false
		}
	}
	return true
}
