package distributedtxn

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"unicode/utf8"
)

const (
	// InlineManifestBytes is the existing coordinator-record fast-path bound.
	// Records which fit remain encoded by AppendCoordinator without a format
	// change. Larger manifests use independently durable segments.
	InlineManifestBytes = MaxCoordinatorRecordBytes

	// ManifestSegmentBytes bounds one independently checksummed page. Recovery
	// only needs caller scratch for one page, regardless of transaction width.
	ManifestSegmentBytes = 64 << 10

	// MaxManifestBytes is a resource-byte admission bound, not a target
	// count bound. Capacity is derived solely from exact encoded bytes and can
	// be raised independently without changing the target grammar.
	MaxManifestBytes = 64 << 20

	manifestSegmentHeaderBytes     = 32
	manifestEntryFixedBytes        = 80
	manifestCoordinatorHeaderBytes = 112
	manifestDescriptorBytes        = 56

	// MaxManifestPageTargets is a page-scratch sizing convenience, not a
	// transaction target limit.
	MaxManifestPageTargets = (ManifestSegmentBytes - manifestSegmentHeaderBytes - 4) / manifestEntryFixedBytes
)

var (
	manifestSegmentMagic     = [4]byte{'V', 'T', 'M', '1'}
	manifestCoordinatorMagic = [4]byte{'V', 'T', 'C', 'M'}
	manifestChainDomain      = [8]byte{'V', 'T', 'M', 'C', 'H', 'N', '1', 0}
	manifestRootDomain       = [8]byte{'V', 'T', 'M', 'R', 'O', 'T', '1', 0}
)

// ManifestDescriptor binds the complete ordered target set without
// retaining it in one resident slice. Root commits to every segment digest,
// its position, total encoded bytes, and total unique target count.
type ManifestDescriptor struct {
	TargetCount  uint64
	EncodedBytes uint64
	SegmentCount uint32
	Root         Digest
}

func (d ManifestDescriptor) valid() bool {
	if d.TargetCount == 0 || d.SegmentCount == 0 ||
		uint64(d.SegmentCount) > d.TargetCount || d.Root == (Digest{}) {
		return false
	}
	segments := uint64(d.SegmentCount)
	if d.TargetCount > segments*uint64(MaxManifestPageTargets) {
		return false
	}
	// Every page has a header and checksum. Its first identity needs at least
	// one distribution and shard byte; every later distinct identity needs at
	// least one suffix byte in addition to the fixed entry.
	minimumBytes := d.TargetCount*(manifestEntryFixedBytes+1) +
		segments*(manifestSegmentHeaderBytes+4+1)
	return d.EncodedBytes >= minimumBytes && d.EncodedBytes <= MaxManifestBytes &&
		d.EncodedBytes <= segments*ManifestSegmentBytes
}

// ManifestSegment describes one borrowed encoded page.
type ManifestSegment struct {
	Index       uint32
	FirstTarget uint64
	TargetCount uint32
	Digest      Digest
	Raw         []byte
}

// ManifestPage is the decoded view of one page. The TransactionTargetRef slice and
// reconstructed prefix-compressed identities are caller-owned.
type ManifestPage struct {
	Segment ManifestSegment
	Targets []TransactionTargetRef
}

// ManifestBuilder incrementally encodes a pre-sorted target stream. It
// retains only one 64 KiB page and the preceding identity. Exact adjacent
// duplicates are folded; conflicting duplicates and reordering fail closed.
type ManifestBuilder struct {
	scratch []byte
	emit    func(ManifestSegment) error

	segmentIndex uint32
	first        uint64
	totalCount   uint64
	totalBytes   uint64
	segmentCount uint32
	chain        Digest
	pageCount    uint32

	priorDistribution [MaxShardIdentityBytes]byte
	priorShard        [MaxShardIdentityBytes]byte
	priorDistLen      uint8
	priorShardLen     uint8
	prior             TransactionTargetRef
	havePrior         bool
	sealed            bool
	descriptor        ManifestDescriptor
	failed            error
}

// NewManifestBuilder uses exactly scratch[:ManifestSegmentBytes] as its page
// arena. emit must consume or copy Raw before returning; the next Append may
// overwrite it.
func NewManifestBuilder(
	scratch []byte,
	emit func(ManifestSegment) error,
) (*ManifestBuilder, error) {
	b := new(ManifestBuilder)
	if err := b.Reset(scratch, emit); err != nil {
		return nil, err
	}
	return b, nil
}

// Reset reuses a caller-owned builder and its fixed page arena.
func (b *ManifestBuilder) Reset(
	scratch []byte,
	emit func(ManifestSegment) error,
) error {
	if cap(scratch) < ManifestSegmentBytes || emit == nil {
		return ErrTooLarge
	}
	*b = ManifestBuilder{scratch: scratch[:manifestSegmentHeaderBytes], emit: emit}
	b.beginPage()
	return nil
}

func (b *ManifestBuilder) beginPage() {
	b.scratch = b.scratch[:manifestSegmentHeaderBytes]
	clear(b.scratch)
	copy(b.scratch[:4], manifestSegmentMagic[:])
	b.scratch[4] = FormatVersion
	b.pageCount = 0
	b.priorDistLen = 0
	b.priorShardLen = 0
}

// Append adds one target in strict (distribution, shard) order.
func (b *ManifestBuilder) Append(target TransactionTargetRef) error {
	if b.sealed {
		return ErrInvalidState
	}
	if b.failed != nil {
		return b.failed
	}
	if err := validateManifestTarget(target); err != nil {
		b.failed = err
		return err
	}
	if b.havePrior {
		order := compareTargetIdentity(b.prior, target)
		if order > 0 {
			b.failed = ErrCorrupt
			return b.failed
		}
		if order == 0 {
			if equalTargetRef(b.prior, target) {
				return nil
			}
			b.failed = ErrCorrupt
			return b.failed
		}
	}
	entryBytes := manifestEntryFixedBytes + len(target.Distribution) + len(target.Shard)
	if b.pageCount != 0 {
		entryBytes -= commonPrefix(b.priorDistribution[:b.priorDistLen], target.Distribution)
		entryBytes -= commonPrefix(b.priorShard[:b.priorShardLen], target.Shard)
	}
	if len(b.scratch)+entryBytes+4 > ManifestSegmentBytes {
		if err := b.flush(); err != nil {
			b.failed = err
			return err
		}
		entryBytes = manifestEntryFixedBytes + len(target.Distribution) + len(target.Shard)
	}
	if uint64(entryBytes)+b.totalBytes > MaxManifestBytes {
		b.failed = ErrTooLarge
		return b.failed
	}
	b.appendEntry(target, entryBytes)
	b.prior = target
	b.prior.Distribution = b.priorDistribution[:len(target.Distribution)]
	b.prior.Shard = b.priorShard[:len(target.Shard)]
	copy(b.priorDistribution[:], target.Distribution)
	copy(b.priorShard[:], target.Shard)
	b.priorDistLen = uint8(len(target.Distribution))
	b.priorShardLen = uint8(len(target.Shard))
	b.havePrior = true
	b.pageCount++
	b.totalCount++
	return nil
}

func (b *ManifestBuilder) appendEntry(target TransactionTargetRef, entryBytes int) {
	distPrefix, shardPrefix := 0, 0
	if b.pageCount != 0 {
		distPrefix = commonPrefix(b.priorDistribution[:b.priorDistLen], target.Distribution)
		shardPrefix = commonPrefix(b.priorShard[:b.priorShardLen], target.Shard)
	}
	distSuffix := target.Distribution[distPrefix:]
	shardSuffix := target.Shard[shardPrefix:]
	start := len(b.scratch)
	b.scratch = b.scratch[:start+entryBytes]
	out := b.scratch[start:]
	out[0], out[1] = byte(distPrefix), byte(len(distSuffix))
	out[2], out[3] = byte(shardPrefix), byte(len(shardSuffix))
	out[4] = byte(target.State)
	out[5], out[6], out[7] = 0, 0, 0
	binary.LittleEndian.PutUint64(out[8:16], target.RoutingVersion)
	binary.LittleEndian.PutUint64(out[16:24], target.AllocationGeneration)
	binary.LittleEndian.PutUint64(out[24:32], target.OwnershipEpoch)
	copy(out[32:64], target.MutationDigest[:])
	copy(out[64:80], target.AuthorityWitness[:])
	cursor := manifestEntryFixedBytes
	copy(out[cursor:], distSuffix)
	cursor += len(distSuffix)
	copy(out[cursor:], shardSuffix)
}

func (b *ManifestBuilder) flush() error {
	if b.pageCount == 0 {
		return nil
	}
	total := len(b.scratch) + 4
	if b.totalBytes+uint64(total) > MaxManifestBytes {
		return ErrTooLarge
	}
	b.scratch = b.scratch[:total]
	copy(b.scratch[:4], manifestSegmentMagic[:])
	b.scratch[4] = FormatVersion
	binary.LittleEndian.PutUint32(b.scratch[8:12], b.segmentIndex)
	binary.LittleEndian.PutUint32(b.scratch[12:16], b.pageCount)
	binary.LittleEndian.PutUint64(b.scratch[16:24], b.first)
	binary.LittleEndian.PutUint32(b.scratch[24:28], uint32(total-manifestSegmentHeaderBytes-4))
	binary.LittleEndian.PutUint32(b.scratch[total-4:], crc32.Checksum(b.scratch[:total-4], castagnoli))
	digest := sha256.Sum256(b.scratch)
	segment := ManifestSegment{
		Index: b.segmentIndex, FirstTarget: b.first,
		TargetCount: b.pageCount, Digest: digest, Raw: b.scratch,
	}
	if err := b.emit(segment); err != nil {
		return err
	}
	b.chain = appendManifestChain(b.chain, segment.Index, segment.Digest)
	b.totalBytes += uint64(total)
	b.segmentCount++
	b.segmentIndex++
	b.first += uint64(b.pageCount)
	b.beginPage()
	return nil
}

// Seal emits the final page and returns the root descriptor.
func (b *ManifestBuilder) Seal() (ManifestDescriptor, error) {
	if b.sealed {
		return b.descriptor, nil
	}
	if b.failed != nil {
		return ManifestDescriptor{}, b.failed
	}
	if b.totalCount == 0 {
		return ManifestDescriptor{}, ErrCorrupt
	}
	if err := b.flush(); err != nil {
		b.failed = err
		return ManifestDescriptor{}, err
	}
	descriptor := ManifestDescriptor{
		TargetCount: b.totalCount, EncodedBytes: b.totalBytes,
		SegmentCount: b.segmentCount,
	}
	descriptor.Root = finishManifestRoot(b.chain, descriptor)
	b.sealed = true
	b.descriptor = descriptor
	return descriptor, nil
}

// ManifestReader verifies a sequence page by page. It retains only root-chain
// state and the final target identity; callers provide each page arena.
type ManifestReader struct {
	want         ManifestDescriptor
	nextSegment  uint32
	nextFirst    uint64
	encodedBytes uint64
	chain        Digest
	lastDist     [MaxShardIdentityBytes]byte
	lastShard    [MaxShardIdentityBytes]byte
	lastDistLen  uint8
	lastShardLen uint8
	haveLast     bool
	failed       error
}

func NewManifestReader(descriptor ManifestDescriptor) (*ManifestReader, error) {
	if !descriptor.valid() {
		return nil, ErrCorrupt
	}
	return &ManifestReader{want: descriptor}, nil
}

// OpenNext validates and decodes the next canonical page into caller scratch.
func (r *ManifestReader) OpenNext(
	raw []byte,
	targets []TransactionTargetRef, identities []byte,
) (ManifestPage, error) {
	if r.failed != nil {
		return ManifestPage{}, r.failed
	}
	page, err := openManifestSegment(raw, targets, identities)
	if err != nil {
		if err == ErrTooLarge {
			return ManifestPage{}, err
		}
		r.failed = ErrCorrupt
		return ManifestPage{}, r.failed
	}
	if page.Segment.Index != r.nextSegment ||
		page.Segment.FirstTarget != r.nextFirst {
		r.failed = ErrCorrupt
		return ManifestPage{}, r.failed
	}
	if r.encodedBytes+uint64(len(raw)) > MaxManifestBytes {
		r.failed = ErrTooLarge
		return ManifestPage{}, r.failed
	}
	if r.haveLast && compareIdentityBytes(
		r.lastDist[:r.lastDistLen], r.lastShard[:r.lastShardLen],
		page.Targets[0].Distribution, page.Targets[0].Shard,
	) >= 0 {
		r.failed = ErrCorrupt
		return ManifestPage{}, r.failed
	}
	last := &page.Targets[len(page.Targets)-1]
	copy(r.lastDist[:], last.Distribution)
	copy(r.lastShard[:], last.Shard)
	r.lastDistLen, r.lastShardLen = uint8(len(last.Distribution)), uint8(len(last.Shard))
	r.haveLast = true
	r.chain = appendManifestChain(r.chain, page.Segment.Index, page.Segment.Digest)
	r.nextSegment++
	r.nextFirst += uint64(page.Segment.TargetCount)
	r.encodedBytes += uint64(len(raw))
	return page, nil
}

// Seal proves that no page is missing or trailing and verifies the root.
func (r *ManifestReader) Seal() error {
	if r.failed != nil {
		return r.failed
	}
	got := ManifestDescriptor{
		TargetCount: r.nextFirst, EncodedBytes: r.encodedBytes,
		SegmentCount: r.nextSegment,
	}
	got.Root = finishManifestRoot(r.chain, got)
	if got != r.want {
		return ErrCorrupt
	}
	return nil
}

// OpenManifestSegment decodes one page independently. Sequence and root
// verification require ManifestReader.
func OpenManifestSegment(
	raw []byte,
	targets []TransactionTargetRef, identities []byte,
) (ManifestPage, error) {
	return openManifestSegment(raw, targets, identities)
}

func openManifestSegment(
	raw []byte,
	scratch []TransactionTargetRef, identities []byte,
) (ManifestPage, error) {
	if len(raw) < manifestSegmentHeaderBytes+manifestEntryFixedBytes+4 ||
		len(raw) > ManifestSegmentBytes || !equal4(raw[:4], manifestSegmentMagic) ||
		raw[4] != FormatVersion || raw[5] != 0 || raw[6] != 0 || raw[7] != 0 ||
		!checksumOK(raw) {
		return ManifestPage{}, ErrCorrupt
	}
	index := binary.LittleEndian.Uint32(raw[8:12])
	count := binary.LittleEndian.Uint32(raw[12:16])
	first := binary.LittleEndian.Uint64(raw[16:24])
	payloadBytes := binary.LittleEndian.Uint32(raw[24:28])
	if binary.LittleEndian.Uint32(raw[28:32]) != 0 || count == 0 ||
		uint64(count) > uint64(MaxManifestPageTargets) ||
		uint64(payloadBytes)+manifestSegmentHeaderBytes+4 != uint64(len(raw)) ||
		uint64(count) > uint64(cap(scratch)) {
		return ManifestPage{}, ErrCorrupt
	}
	targets := scratch[:count]
	clear(targets)
	cursor, end := manifestSegmentHeaderBytes, len(raw)-4
	identityCursor := 0
	var prior TransactionTargetRef
	for i := range targets {
		if end-cursor < manifestEntryFixedBytes {
			return ManifestPage{}, ErrCorrupt
		}
		entry := raw[cursor:]
		distPrefix, distSuffix := int(entry[0]), int(entry[1])
		shardPrefix, shardSuffix := int(entry[2]), int(entry[3])
		if entry[5] != 0 || entry[6] != 0 || entry[7] != 0 ||
			(i == 0 && (distPrefix != 0 || shardPrefix != 0)) ||
			distPrefix > len(prior.Distribution) || shardPrefix > len(prior.Shard) ||
			distPrefix+distSuffix == 0 || distPrefix+distSuffix > MaxShardIdentityBytes ||
			shardPrefix+shardSuffix == 0 || shardPrefix+shardSuffix > MaxShardIdentityBytes ||
			end-cursor-manifestEntryFixedBytes < distSuffix+shardSuffix {
			return ManifestPage{}, ErrCorrupt
		}
		cursor += manifestEntryFixedBytes
		p := &targets[i]
		p.State = TargetState(entry[4])
		p.RoutingVersion = binary.LittleEndian.Uint64(entry[8:16])
		p.AllocationGeneration = binary.LittleEndian.Uint64(entry[16:24])
		p.OwnershipEpoch = binary.LittleEndian.Uint64(entry[24:32])
		copy(p.MutationDigest[:], entry[32:64])
		copy(p.AuthorityWitness[:], entry[64:80])
		distLen := distPrefix + distSuffix
		shardLen := shardPrefix + shardSuffix
		if len(identities)-identityCursor < distLen+shardLen {
			return ManifestPage{}, ErrTooLarge
		}
		distStart := identityCursor
		shardStart := distStart + distLen
		p.Distribution = identities[distStart:shardStart]
		p.Shard = identities[shardStart : shardStart+shardLen]
		copy(p.Distribution, prior.Distribution[:distPrefix])
		copy(p.Distribution[distPrefix:], raw[cursor:cursor+distSuffix])
		copy(p.Shard, prior.Shard[:shardPrefix])
		copy(p.Shard[shardPrefix:], raw[cursor+distSuffix:cursor+distSuffix+shardSuffix])
		identityCursor += distLen + shardLen
		cursor += distSuffix + shardSuffix
		if !utf8.Valid(p.Distribution) || !utf8.Valid(p.Shard) ||
			validateManifestTarget(*p) != nil ||
			(i != 0 && (compareTargetIdentity(prior, *p) >= 0 ||
				distPrefix != commonPrefix(prior.Distribution, p.Distribution) ||
				shardPrefix != commonPrefix(prior.Shard, p.Shard))) {
			return ManifestPage{}, ErrCorrupt
		}
		prior = *p
	}
	if cursor != end {
		return ManifestPage{}, ErrCorrupt
	}
	digest := sha256.Sum256(raw)
	return ManifestPage{
		Segment: ManifestSegment{Index: index, FirstTarget: first,
			TargetCount: count, Digest: digest, Raw: raw},
		Targets: targets,
	}, nil
}

// ManifestCoordinatorRecord is the fixed-size coordinator stage for a
// segmented manifest. The descriptor is authenticated before the stage can be
// sealed committed or aborted.
type ManifestCoordinatorRecord struct {
	ID                ID
	State             CoordinatorState
	Revision          uint64
	CatalogGeneration uint64
	// RecoveryDeadline is the legacy field name for the bounded logical pulse
	// limit. It is never compared with wall time.
	RecoveryDeadline int64
	Manifest         ManifestDescriptor
}

func AppendManifestCoordinator(dst []byte, record ManifestCoordinatorRecord) ([]byte, error) {
	if record.ID.IsZero() || record.State != CoordinatorStaging || record.Revision == 0 ||
		record.CatalogGeneration == 0 || !record.Manifest.valid() {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, manifestCoordinatorHeaderBytes+4)...)
	out := dst[start:]
	copy(out[:4], manifestCoordinatorMagic[:])
	out[4], out[5] = FormatVersion, byte(record.State)
	binary.LittleEndian.PutUint64(out[8:16], record.Revision)
	binary.LittleEndian.PutUint64(out[16:24], record.CatalogGeneration)
	binary.LittleEndian.PutUint64(out[24:32], uint64(record.RecoveryDeadline))
	copy(out[32:48], record.ID[:])
	appendManifestDescriptorTo(out[48:104], record.Manifest)
	binary.LittleEndian.PutUint32(out[len(out)-4:], crc32.Checksum(out[:len(out)-4], castagnoli))
	return dst, nil
}

func OpenManifestCoordinator(src []byte) (ManifestCoordinatorRecord, error) {
	if len(src) != manifestCoordinatorHeaderBytes+4 ||
		!equal4(src[:4], manifestCoordinatorMagic) || !checksumOK(src) ||
		src[4] != FormatVersion || src[6] != 0 || src[7] != 0 ||
		binary.LittleEndian.Uint64(src[104:112]) != 0 {
		return ManifestCoordinatorRecord{}, ErrCorrupt
	}
	record := ManifestCoordinatorRecord{
		State: CoordinatorState(src[5]), Revision: binary.LittleEndian.Uint64(src[8:16]),
		CatalogGeneration: binary.LittleEndian.Uint64(src[16:24]),
		RecoveryDeadline:  int64(binary.LittleEndian.Uint64(src[24:32])),
		Manifest:          openManifestDescriptor(src[48:104]),
	}
	copy(record.ID[:], src[32:48])
	if record.ID.IsZero() || record.State != CoordinatorStaging || record.Revision == 0 ||
		record.CatalogGeneration == 0 || !record.Manifest.valid() {
		return ManifestCoordinatorRecord{}, ErrCorrupt
	}
	return record, nil
}

func appendManifestDescriptor(dst []byte, descriptor ManifestDescriptor) ([]byte, error) {
	if !descriptor.valid() {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, manifestDescriptorBytes)...)
	appendManifestDescriptorTo(dst[start:], descriptor)
	return dst, nil
}

func appendManifestDescriptorTo(out []byte, descriptor ManifestDescriptor) {
	binary.LittleEndian.PutUint64(out[0:8], descriptor.TargetCount)
	binary.LittleEndian.PutUint64(out[8:16], descriptor.EncodedBytes)
	binary.LittleEndian.PutUint32(out[16:20], descriptor.SegmentCount)
	copy(out[24:56], descriptor.Root[:])
}

func openManifestDescriptor(src []byte) ManifestDescriptor {
	var descriptor ManifestDescriptor
	if len(src) < manifestDescriptorBytes || binary.LittleEndian.Uint32(src[20:24]) != 0 {
		return descriptor
	}
	descriptor.TargetCount = binary.LittleEndian.Uint64(src[0:8])
	descriptor.EncodedBytes = binary.LittleEndian.Uint64(src[8:16])
	descriptor.SegmentCount = binary.LittleEndian.Uint32(src[16:20])
	copy(descriptor.Root[:], src[24:56])
	return descriptor
}

func validateManifestTarget(p TransactionTargetRef) error {
	if len(p.Distribution) == 0 || len(p.Distribution) > MaxShardIdentityBytes ||
		!utf8.Valid(p.Distribution) || len(p.Shard) == 0 || len(p.Shard) > MaxShardIdentityBytes ||
		!utf8.Valid(p.Shard) || p.RoutingVersion == 0 || p.AllocationGeneration == 0 ||
		p.OwnershipEpoch == 0 || !p.State.valid() || p.MutationDigest == (Digest{}) {
		return ErrCorrupt
	}
	return nil
}

func compareTargetIdentity(a, b TransactionTargetRef) int {
	return compareIdentityBytes(a.Distribution, a.Shard, b.Distribution, b.Shard)
}

func compareIdentityBytes(aDist, aShard, bDist, bShard []byte) int {
	if order := compareBytes(aDist, bDist); order != 0 {
		return order
	}
	return compareBytes(aShard, bShard)
}

func equalTargetRef(a, b TransactionTargetRef) bool {
	return compareTargetIdentity(a, b) == 0 &&
		a.RoutingVersion == b.RoutingVersion &&
		a.AllocationGeneration == b.AllocationGeneration &&
		a.OwnershipEpoch == b.OwnershipEpoch &&
		a.AuthorityWitness == b.AuthorityWitness &&
		a.MutationDigest == b.MutationDigest && a.State == b.State
}

func commonPrefix(a, b []byte) int {
	limit := len(a)
	if len(b) < limit {
		limit = len(b)
	}
	for i := 0; i < limit; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return limit
}

func appendManifestChain(chain Digest, index uint32, segment Digest) Digest {
	var encoded [8 + 32 + 4 + 32]byte
	copy(encoded[:8], manifestChainDomain[:])
	copy(encoded[8:40], chain[:])
	binary.LittleEndian.PutUint32(encoded[40:44], index)
	copy(encoded[44:], segment[:])
	return sha256.Sum256(encoded[:])
}

func finishManifestRoot(chain Digest, descriptor ManifestDescriptor) Digest {
	var encoded [8 + 32 + 8 + 8 + 4]byte
	copy(encoded[:8], manifestRootDomain[:])
	copy(encoded[8:40], chain[:])
	binary.LittleEndian.PutUint64(encoded[40:48], descriptor.TargetCount)
	binary.LittleEndian.PutUint64(encoded[48:56], descriptor.EncodedBytes)
	binary.LittleEndian.PutUint32(encoded[56:60], descriptor.SegmentCount)
	return sha256.Sum256(encoded[:])
}
