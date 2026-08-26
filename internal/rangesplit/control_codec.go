package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

var ErrSplitControlRecord = errors.New("rangesplit: invalid split control record")

const (
	splitControlFormat            = uint16(0)
	captureControlHeaderBytes     = 360
	tailControlBytes              = 360
	artifactControlHeaderBytes    = 224
	artifactControlManifestBytes  = 376
	MaxArtifactControlRecordBytes = artifactControlHeaderBytes +
		autosplit.MaxSplitChildren*(artifactControlManifestBytes+replication.MaxCollectionBytes) + sha256.Size
)

var (
	captureControlMagic   = [8]byte{'V', 'D', 'B', 'S', 'C', 'A', 'P', 0}
	artifactControlMagic  = [8]byte{'V', 'D', 'B', 'S', 'A', 'R', 'T', 0}
	tailControlMagic      = [8]byte{'V', 'D', 'B', 'S', 'T', 'A', 'I', 'L'}
	captureControlDomain  = []byte("vibedb/range-split/capture-control\x00")
	artifactControlDomain = []byte("vibedb/range-split/artifact-control\x00")
	tailControlDomain     = []byte("vibedb/range-split/tail-control\x00")
)

// SourceCaptureDescriptor is the bounded durable identity and progress record
// for a source capture. It contains no captured row or document bytes.
type SourceCaptureDescriptor struct {
	PlanDigest      [sha256.Size]byte
	PlacementDigest [sha256.Size]byte
	Collection      string
	Base            ChildArtifactSourceCut
	Head            ChildArtifactSourceCut
	Coordinates     TailSourceCoordinates
}

// SplitControlRecordWorkspace retains checksum state for allocation-free warm
// appends. Reuse it serially; it contains no record bytes or authority.
type SplitControlRecordWorkspace struct {
	hasher hash.Hash
	digest [sha256.Size]byte
}

// Descriptor returns a detached control record only at a fully published
// capture boundary. A pending transition is deliberately not representable.
func (c *SourceCapture) Descriptor() (SourceCaptureDescriptor, error) {
	if c == nil {
		return SourceCaptureDescriptor{}, ErrSourceCapture
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.begun.Load() || c.pending != 0 {
		return SourceCaptureDescriptor{}, ErrSourceCapture
	}
	return SourceCaptureDescriptor{
		PlanDigest: c.partitioner.digest, PlacementDigest: c.placement,
		Collection: c.partitioner.collection, Base: c.base,
		Head: ChildArtifactSourceCut{
			DataChainDigest: c.current.dataChainDigest, BaseDigest: c.base.BaseDigest,
			EntryDigest: c.current.entryDigest, Applied: c.current.applied,
			Term: c.current.term, RouteGeneration: c.current.routeGeneration,
		},
		Coordinates: TailSourceCoordinates{
			OwnershipEpoch:  c.current.ownershipEpoch,
			RoutingVersion:  c.current.routingVersion,
			RouteGeneration: c.current.routeGeneration,
		},
	}, nil
}

// ValidateSourceCaptureDescriptor binds a decoded descriptor to the exact
// immutable partition plan. It performs no I/O and grants no authority.
func (p *Partitioner) ValidateSourceCaptureDescriptor(d SourceCaptureDescriptor) error {
	if p == nil || !validSourceCaptureDescriptor(d) || d.PlanDigest != p.digest ||
		d.PlacementDigest != p.program.Digest() || d.Collection != p.collection {
		return ErrSplitControlRecord
	}
	return nil
}

func AppendSourceCaptureDescriptor(dst []byte, d SourceCaptureDescriptor) ([]byte, error) {
	return AppendSourceCaptureDescriptorWithWorkspace(dst, d, &SplitControlRecordWorkspace{})
}

func AppendSourceCaptureDescriptorWithWorkspace(dst []byte, d SourceCaptureDescriptor, workspace *SplitControlRecordWorkspace) ([]byte, error) {
	if workspace == nil || !validSourceCaptureDescriptor(d) {
		return dst, ErrSplitControlRecord
	}
	total := captureControlHeaderBytes + len(d.Collection) + sha256.Size
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	f := dst[start:]
	appendControlHeader(f, captureControlMagic, captureControlHeaderBytes, total)
	digests := [...][sha256.Size]byte{
		d.PlanDigest, d.PlacementDigest,
		d.Base.DataChainDigest, d.Base.BaseDigest, d.Base.EntryDigest,
		d.Head.DataChainDigest, d.Head.BaseDigest, d.Head.EntryDigest,
	}
	for i := range digests {
		copy(f[24+i*32:56+i*32], digests[i][:])
	}
	values := [...]uint64{
		d.Base.Applied, d.Base.Term, d.Base.RouteGeneration,
		d.Head.Applied, d.Head.Term, d.Head.RouteGeneration,
		d.Coordinates.OwnershipEpoch, d.Coordinates.RoutingVersion,
		d.Coordinates.RouteGeneration,
	}
	for i, value := range values {
		binary.LittleEndian.PutUint64(f[280+i*8:288+i*8], value)
	}
	binary.LittleEndian.PutUint32(f[352:356], uint32(len(d.Collection)))
	copy(f[captureControlHeaderBytes:], d.Collection)
	appendControlDigest(f, captureControlDomain, workspace)
	return dst, nil
}

func OpenSourceCaptureDescriptor(raw []byte) (SourceCaptureDescriptor, error) {
	if !validControlEnvelope(raw, captureControlMagic, captureControlHeaderBytes,
		captureControlHeaderBytes+sha256.Size, captureControlHeaderBytes+replication.MaxCollectionBytes+sha256.Size,
		captureControlDomain) || binary.LittleEndian.Uint32(raw[356:360]) != 0 {
		return SourceCaptureDescriptor{}, ErrSplitControlRecord
	}
	collectionBytes := int(binary.LittleEndian.Uint32(raw[352:356]))
	if collectionBytes == 0 || collectionBytes > replication.MaxCollectionBytes ||
		captureControlHeaderBytes+collectionBytes+sha256.Size != len(raw) {
		return SourceCaptureDescriptor{}, ErrSplitControlRecord
	}
	d := SourceCaptureDescriptor{Collection: string(raw[captureControlHeaderBytes : len(raw)-sha256.Size])}
	digests := []*[sha256.Size]byte{
		&d.PlanDigest, &d.PlacementDigest,
		&d.Base.DataChainDigest, &d.Base.BaseDigest, &d.Base.EntryDigest,
		&d.Head.DataChainDigest, &d.Head.BaseDigest, &d.Head.EntryDigest,
	}
	for i := range digests {
		copy(digests[i][:], raw[24+i*32:56+i*32])
	}
	values := [9]uint64{}
	for i := range values {
		values[i] = binary.LittleEndian.Uint64(raw[280+i*8 : 288+i*8])
	}
	d.Base.Applied, d.Base.Term, d.Base.RouteGeneration = values[0], values[1], values[2]
	d.Head.Applied, d.Head.Term, d.Head.RouteGeneration = values[3], values[4], values[5]
	d.Coordinates = TailSourceCoordinates{OwnershipEpoch: values[6], RoutingVersion: values[7], RouteGeneration: values[8]}
	if !validSourceCaptureDescriptor(d) {
		return SourceCaptureDescriptor{}, ErrSplitControlRecord
	}
	return d, nil
}

// AppendChildArtifactSet appends the complete bounded artifact manifest set.
// Artifact payloads remain in their independently durable repositories.
func AppendChildArtifactSet(dst []byte, set ChildArtifactSet) ([]byte, error) {
	return AppendChildArtifactSetWithWorkspace(dst, set, &SplitControlRecordWorkspace{})
}

func AppendChildArtifactSetWithWorkspace(dst []byte, set ChildArtifactSet, workspace *SplitControlRecordWorkspace) ([]byte, error) {
	if workspace == nil || !validChildArtifactSetControl(set) {
		return dst, ErrSplitControlRecord
	}
	total := artifactControlHeaderBytes + sha256.Size
	for child := range set.Children {
		if set.Children[child].Present {
			total += artifactControlManifestBytes + len(set.Children[child].Descriptor.Shard)
		}
	}
	if total > MaxArtifactControlRecordBytes {
		return dst, ErrSplitControlRecord
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	f := dst[start:]
	appendControlHeader(f, artifactControlMagic, artifactControlHeaderBytes, total)
	copy(f[24:56], set.Partition.PlanDigest[:])
	copy(f[56:88], set.Partition.SourceDigest[:])
	copy(f[88:120], set.Partition.SourceBase[:])
	copy(f[120:152], set.Partition.SourceEntry[:])
	values := [...]uint64{set.Partition.SourceApplied, set.Partition.SourceTerm, set.Partition.RouteGeneration}
	for i, value := range values {
		binary.LittleEndian.PutUint64(f[152+i*8:160+i*8], value)
	}
	for i, value := range set.Partition.Rows {
		binary.LittleEndian.PutUint64(f[176+i*8:184+i*8], value)
	}
	for i, value := range set.Partition.Bytes {
		binary.LittleEndian.PutUint64(f[200+i*8:208+i*8], value)
	}
	at := artifactControlHeaderBytes
	for child := range set.Children {
		m := set.Children[child]
		if !m.Present {
			continue
		}
		frame := f[at : at+artifactControlManifestBytes+len(m.Descriptor.Shard)]
		frame[0], frame[1] = 1, m.Child
		binary.LittleEndian.PutUint16(frame[2:4], uint16(len(m.Descriptor.Shard)))
		binary.LittleEndian.PutUint32(frame[4:8], uint32(len(frame)))
		writeArtifactManifestControl(frame, m)
		at += len(frame)
	}
	if at != total-sha256.Size {
		panic("rangesplit: artifact control size diverged")
	}
	appendControlDigest(f, artifactControlDomain, workspace)
	return dst, nil
}

func OpenChildArtifactSet(raw []byte) (ChildArtifactSet, error) {
	if !validControlEnvelope(raw, artifactControlMagic, artifactControlHeaderBytes,
		artifactControlHeaderBytes+sha256.Size, MaxArtifactControlRecordBytes, artifactControlDomain) {
		return ChildArtifactSet{}, ErrSplitControlRecord
	}
	var set ChildArtifactSet
	copy(set.Partition.PlanDigest[:], raw[24:56])
	copy(set.Partition.SourceDigest[:], raw[56:88])
	copy(set.Partition.SourceBase[:], raw[88:120])
	copy(set.Partition.SourceEntry[:], raw[120:152])
	set.Partition.SourceApplied = binary.LittleEndian.Uint64(raw[152:160])
	set.Partition.SourceTerm = binary.LittleEndian.Uint64(raw[160:168])
	set.Partition.RouteGeneration = binary.LittleEndian.Uint64(raw[168:176])
	for i := range set.Partition.Rows {
		set.Partition.Rows[i] = binary.LittleEndian.Uint64(raw[176+i*8 : 184+i*8])
	}
	for i := range set.Partition.Bytes {
		set.Partition.Bytes[i] = binary.LittleEndian.Uint64(raw[200+i*8 : 208+i*8])
	}
	at := artifactControlHeaderBytes
	previous := -1
	for at < len(raw)-sha256.Size {
		if len(raw)-sha256.Size-at < artifactControlManifestBytes {
			return ChildArtifactSet{}, ErrSplitControlRecord
		}
		frameBytes := int(binary.LittleEndian.Uint32(raw[at+4 : at+8]))
		shardBytes := int(binary.LittleEndian.Uint16(raw[at+2 : at+4]))
		child := int(raw[at+1])
		if raw[at] != 1 || child <= previous || child >= autosplit.MaxSplitChildren ||
			frameBytes != artifactControlManifestBytes+shardBytes || shardBytes == 0 ||
			frameBytes > len(raw)-sha256.Size-at {
			return ChildArtifactSet{}, ErrSplitControlRecord
		}
		m, err := readArtifactManifestControl(raw[at : at+frameBytes])
		if err != nil {
			return ChildArtifactSet{}, err
		}
		set.Children[child] = m
		previous, at = child, at+frameBytes
	}
	if at != len(raw)-sha256.Size || !validChildArtifactSetControl(set) {
		return ChildArtifactSet{}, ErrSplitControlRecord
	}
	return set, nil
}

// ValidateChildArtifactSet binds a decoded manifest set to this partitioner.
func (p *Partitioner) ValidateChildArtifactSet(set ChildArtifactSet) error {
	if p == nil {
		return ErrSplitControlRecord
	}
	_, err := p.InitialTailCursor(set)
	if err != nil {
		return errors.Join(ErrSplitControlRecord, err)
	}
	return nil
}

func AppendTailCursor(dst []byte, c TailCursor) ([]byte, error) {
	return AppendTailCursorWithWorkspace(dst, c, &SplitControlRecordWorkspace{})
}

func AppendTailCursorWithWorkspace(dst []byte, c TailCursor, workspace *SplitControlRecordWorkspace) ([]byte, error) {
	if workspace == nil || !validTailControl(c) {
		return dst, ErrSplitControlRecord
	}
	start := len(dst)
	dst = append(dst, make([]byte, tailControlBytes)...)
	f := dst[start:]
	appendControlHeader(f, tailControlMagic, tailControlBytes-sha256.Size, tailControlBytes)
	digests := [5 + autosplit.MaxSplitChildren][sha256.Size]byte{c.planDigest, c.placementDigest, c.dataChainDigest, c.baseDigest, c.entryDigest}
	copy(digests[5:], c.childBaseDigests[:])
	for i := range digests {
		copy(f[24+i*32:56+i*32], digests[i][:])
	}
	values := [...]uint64{c.applied, c.term, c.ownershipEpoch, c.routingVersion, c.routeGeneration}
	for i, value := range values {
		binary.LittleEndian.PutUint64(f[280+i*8:288+i*8], value)
	}
	if c.sealed {
		f[320] = 1
	}
	appendControlDigest(f, tailControlDomain, workspace)
	return dst, nil
}

func OpenTailCursor(raw []byte) (TailCursor, error) {
	if !validControlEnvelope(raw, tailControlMagic, tailControlBytes-sha256.Size,
		tailControlBytes, tailControlBytes, tailControlDomain) || raw[320] > 1 || !allChildArtifactZero(raw[321:328]) {
		return TailCursor{}, ErrSplitControlRecord
	}
	var c TailCursor
	digests := []*[sha256.Size]byte{&c.planDigest, &c.placementDigest, &c.dataChainDigest, &c.baseDigest, &c.entryDigest,
		&c.childBaseDigests[0], &c.childBaseDigests[1], &c.childBaseDigests[2]}
	for i := range digests {
		copy(digests[i][:], raw[24+i*32:56+i*32])
	}
	c.applied = binary.LittleEndian.Uint64(raw[280:288])
	c.term = binary.LittleEndian.Uint64(raw[288:296])
	c.ownershipEpoch = binary.LittleEndian.Uint64(raw[296:304])
	c.routingVersion = binary.LittleEndian.Uint64(raw[304:312])
	c.routeGeneration = binary.LittleEndian.Uint64(raw[312:320])
	c.sealed = raw[320] == 1
	if !validTailControl(c) {
		return TailCursor{}, ErrSplitControlRecord
	}
	return c, nil
}

func validSourceCaptureDescriptor(d SourceCaptureDescriptor) bool {
	return d.PlanDigest != ([sha256.Size]byte{}) && d.PlacementDigest != ([sha256.Size]byte{}) &&
		d.Collection != "" && len(d.Collection) <= replication.MaxCollectionBytes && utf8.ValidString(d.Collection) &&
		validControlCut(d.Base) && validControlCut(d.Head) && d.Head.Applied >= d.Base.Applied &&
		d.Head.Term >= d.Base.Term && d.Head.BaseDigest == d.Base.BaseDigest &&
		d.Coordinates.OwnershipEpoch != 0 && d.Coordinates.RoutingVersion != 0 &&
		d.Coordinates.RouteGeneration == d.Head.RouteGeneration
}

func validControlCut(c ChildArtifactSourceCut) bool {
	return c.DataChainDigest != ([sha256.Size]byte{}) && c.BaseDigest != ([sha256.Size]byte{}) &&
		c.EntryDigest != ([sha256.Size]byte{}) && c.Applied != 0 && c.Applied != math.MaxUint64 &&
		c.Term != 0 && c.Term != math.MaxUint64 && c.RouteGeneration != 0
}

func validTailControl(c TailCursor) bool {
	if c.planDigest == ([sha256.Size]byte{}) || c.placementDigest == ([sha256.Size]byte{}) ||
		c.dataChainDigest == ([sha256.Size]byte{}) || c.baseDigest == ([sha256.Size]byte{}) ||
		c.entryDigest == ([sha256.Size]byte{}) || c.applied == 0 || c.applied == math.MaxUint64 ||
		c.term == 0 || c.term == math.MaxUint64 || c.ownershipEpoch == 0 ||
		c.routingVersion == 0 || c.routeGeneration == 0 {
		return false
	}
	// Every split has two children; only a ternary split has the third base.
	// The partitioner validator remains the authority for the exact geometry.
	return c.childBaseDigests[0] != ([sha256.Size]byte{}) &&
		c.childBaseDigests[1] != ([sha256.Size]byte{})
}

func validChildArtifactSetControl(set ChildArtifactSet) bool {
	p := set.Partition
	if p.PlanDigest == ([sha256.Size]byte{}) || p.SourceDigest == ([sha256.Size]byte{}) ||
		p.SourceBase == ([sha256.Size]byte{}) || p.SourceEntry == ([sha256.Size]byte{}) ||
		p.SourceApplied == 0 || p.SourceApplied == math.MaxUint64 || p.SourceTerm == 0 ||
		p.SourceTerm == math.MaxUint64 || p.RouteGeneration == 0 {
		return false
	}
	present := 0
	for child, m := range set.Children {
		if !m.Present {
			if m != (ChildArtifactManifest{}) {
				return false
			}
			continue
		}
		present++
		if int(m.Child) != child || m.PlanDigest != p.PlanDigest || m.PlacementDigest == ([sha256.Size]byte{}) ||
			m.Source.DataChainDigest != p.SourceDigest || m.Source.BaseDigest != p.SourceBase ||
			m.Source.EntryDigest != p.SourceEntry || m.Source.Applied != p.SourceApplied ||
			m.Source.Term != p.SourceTerm || m.Source.RouteGeneration != p.RouteGeneration ||
			m.TargetRoutingVersion == 0 || !m.Descriptor.Range.Valid() || m.Descriptor.Shard == "" ||
			len(m.Descriptor.Shard) > replication.MaxCollectionBytes || !utf8.ValidString(string(m.Descriptor.Shard)) ||
			m.Descriptor.AllocationGeneration == 0 || m.Descriptor.OwnershipEpoch == 0 || m.Descriptor.LeaderCount == 0 ||
			m.TargetChunkBytes < MinChildArtifactChunkBytes || m.TargetChunkBytes > MaxChildArtifactChunkBytes ||
			m.EncodedBytes == 0 || m.HeaderDigest == ([sha256.Size]byte{}) ||
			m.LastChunkDigest == ([sha256.Size]byte{}) || m.Digest == ([sha256.Size]byte{}) ||
			m.Rows != p.Rows[child] || m.RowBytes != p.Bytes[child] {
			return false
		}
	}
	return present >= 1 && present < autosplit.MaxSplitChildren
}

func writeArtifactManifestControl(f []byte, m ChildArtifactManifest) {
	digests := [...][sha256.Size]byte{m.PlanDigest, m.PlacementDigest, m.Source.DataChainDigest, m.Source.BaseDigest,
		m.Source.EntryDigest, m.HeaderDigest, m.LastChunkDigest, m.Digest}
	for i := range digests {
		copy(f[8+i*32:40+i*32], digests[i][:])
	}
	values := [...]uint64{m.Source.Applied, m.Source.Term, m.Source.RouteGeneration, uint64(m.TargetRoutingVersion),
		uint64(m.Descriptor.AllocationGeneration), uint64(m.Descriptor.OwnershipEpoch), m.Chunks, m.Rows,
		m.RowBytes, m.PayloadBytes, m.EncodedBytes}
	for i, value := range values {
		binary.LittleEndian.PutUint64(f[264+i*8:272+i*8], value)
	}
	copy(f[352:360], m.Descriptor.Range.Start[:])
	copy(f[360:368], m.Descriptor.Range.End.Point[:])
	if m.Descriptor.Range.End.Max {
		f[368] = 1
	}
	binary.LittleEndian.PutUint16(f[370:372], m.Descriptor.LeaderCount)
	binary.LittleEndian.PutUint32(f[372:376], m.TargetChunkBytes)
	copy(f[artifactControlManifestBytes:], m.Descriptor.Shard)
}

func readArtifactManifestControl(f []byte) (ChildArtifactManifest, error) {
	if len(f) < artifactControlManifestBytes || f[0] != 1 || !allChildArtifactZero(f[369:370]) || f[368] > 1 {
		return ChildArtifactManifest{}, ErrSplitControlRecord
	}
	m := ChildArtifactManifest{Present: true, Child: f[1]}
	digests := []*[sha256.Size]byte{&m.PlanDigest, &m.PlacementDigest, &m.Source.DataChainDigest, &m.Source.BaseDigest,
		&m.Source.EntryDigest, &m.HeaderDigest, &m.LastChunkDigest, &m.Digest}
	for i := range digests {
		copy(digests[i][:], f[8+i*32:40+i*32])
	}
	values := [11]uint64{}
	for i := range values {
		values[i] = binary.LittleEndian.Uint64(f[264+i*8 : 272+i*8])
	}
	m.Source.Applied, m.Source.Term, m.Source.RouteGeneration = values[0], values[1], values[2]
	m.TargetRoutingVersion = distribution.RoutingVersion(values[3])
	m.Descriptor.AllocationGeneration = distribution.ShardAllocationGeneration(values[4])
	m.Descriptor.OwnershipEpoch = distribution.OwnershipEpoch(values[5])
	m.Chunks, m.Rows, m.RowBytes, m.PayloadBytes, m.EncodedBytes = values[6], values[7], values[8], values[9], values[10]
	copy(m.Descriptor.Range.Start[:], f[352:360])
	copy(m.Descriptor.Range.End.Point[:], f[360:368])
	m.Descriptor.Range.End.Max = f[368] == 1
	m.Descriptor.LeaderCount = binary.LittleEndian.Uint16(f[370:372])
	m.TargetChunkBytes = binary.LittleEndian.Uint32(f[372:376])
	m.Descriptor.Shard = distribution.ShardID(string(f[artifactControlManifestBytes:]))
	return m, nil
}

func appendControlHeader(f []byte, magic [8]byte, header, total int) {
	copy(f[:8], magic[:])
	binary.LittleEndian.PutUint16(f[8:10], splitControlFormat)
	binary.LittleEndian.PutUint16(f[10:12], uint16(header))
	binary.LittleEndian.PutUint32(f[12:16], uint32(total))
}

func appendControlDigest(f, domain []byte, workspace *SplitControlRecordWorkspace) {
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	workspace.hasher.Reset()
	_, _ = workspace.hasher.Write(domain)
	_, _ = workspace.hasher.Write(f[:len(f)-sha256.Size])
	_ = workspace.hasher.Sum(workspace.digest[:0])
	copy(f[len(f)-sha256.Size:], workspace.digest[:])
}

func validControlEnvelope(raw []byte, magic [8]byte, header, minimum, maximum int, domain []byte) bool {
	if len(raw) < minimum || len(raw) > maximum || !bytes.Equal(raw[:8], magic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != splitControlFormat || binary.LittleEndian.Uint16(raw[10:12]) != uint16(header) ||
		binary.LittleEndian.Uint32(raw[12:16]) != uint32(len(raw)) || !allChildArtifactZero(raw[16:24]) {
		return false
	}
	d := sha256.New()
	_, _ = d.Write(domain)
	_, _ = d.Write(raw[:len(raw)-sha256.Size])
	var sum [sha256.Size]byte
	_ = d.Sum(sum[:0])
	return bytes.Equal(sum[:], raw[len(raw)-sha256.Size:])
}
