package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// The tablet anchor map is the lexical half of the proposed primary router.
// It maps immutable interval fences to stable BucketIDs; physical PageRefs are
// deliberately absent. BucketMap is the only structure allowed to translate a
// BucketID into a current leaf page.
//
// A macro-tablet owns one exact image. At the 100-billion-row design point,
// roughly 4,300 230-row leaves per tablet fit comfortably below the 16-bit key
// arena limit. If unusually long fences exceed that bound, the tablet must
// split instead of silently widening every anchor.
const (
	TabletAnchorMapHeaderSize       = 64
	TabletAnchorMapAcceleratorSlots = 257
	TabletAnchorMapRestart          = 16
	TabletAnchorMapTrailerSize      = 8
	TabletAnchorMapMaxFences        = 65535

	tabletAnchorMapAcceleratorBytes = TabletAnchorMapAcceleratorSlots * 2
	tabletAnchorMapMagic            = "AMLAB000"
	tabletAnchorMapVersion          = DevelopmentFormatVersion
)

var (
	// ErrTabletAnchorMapCorrupt reports that an anchor-map image failed
	// checksum or structural admission; a rejected image must never be routed.
	ErrTabletAnchorMapCorrupt = errors.New(
		"vibejson: corrupt tablet anchor-map lab image",
	)
	// ErrTabletAnchorMapScratch reports that ApplyBatch was handed a scratch
	// buffer too small to stage the edited fence set.
	ErrTabletAnchorMapScratch = errors.New(
		"vibejson: tablet anchor-map lab scratch is too small",
	)
	// ErrTabletAnchorHandleCorrupt reports that an anchor-handle image failed
	// checksum or structural admission.
	ErrTabletAnchorHandleCorrupt = errors.New(
		"vibejson: corrupt tablet anchor-handle lab image",
	)
)

// TabletAnchorMapHeader is the stable identity of one immutable macro
// tablet routing image. Hash seeds remain a store-root property and are not
// repeated in every tablet.
type TabletAnchorMapHeader struct {
	TabletID   uint64
	Generation uint64
}

// TabletAnchorMapAnchor is one right-hand interval fence and the stable
// bucket entered at that fence. The first bucket covers (-infinity, fence[0]).
type TabletAnchorMapAnchor struct {
	Fence  []byte
	Bucket BucketID
}

// TabletAnchorMapEditOperation names the structural change one canonical edit
// applies to a fence.
type TabletAnchorMapEditOperation uint8

const (
	// TabletAnchorMapInsert adds a new fence and the bucket entered at it.
	TabletAnchorMapInsert TabletAnchorMapEditOperation = iota + 1
	// TabletAnchorMapDelete removes a fence and folds its right-hand bucket
	// into the preceding interval.
	TabletAnchorMapDelete
	// TabletAnchorMapReplace keeps a fence but changes the bucket it enters.
	TabletAnchorMapReplace
)

// TabletAnchorMapEdit is one canonical structural edit. Edits must be in
// strict fence order. Delete removes the right-hand bucket at Fence; Replace
// preserves the fence while changing that bucket. Ordinary document writes do
// not touch this map.
type TabletAnchorMapEdit struct {
	Operation TabletAnchorMapEditOperation
	Fence     []byte
	Bucket    BucketID
}

// TabletAnchorMapRoute carries the hash needed by the ordered leaf, so a
// caller that starts at the lexical router never hashes the primary key twice.
type TabletAnchorMapRoute struct {
	Bucket  BucketID
	Hash    uint64
	Ordinal uint16
}

// TabletAnchorMapView borrows one checksum- and structure-admitted image.
// Every read method is allocation-free.
type TabletAnchorMapView struct {
	header       TabletAnchorMapHeader
	image        []byte
	accelerator  []byte
	common       []byte
	offsets      []byte
	buckets      []byte
	keys         []byte
	fenceCount   uint16
	maxFence     uint16
	commonLength uint16
}

// TabletAnchorMapCursor walks stable bucket identities in lexical interval
// order. It never follows leaf sibling PageRefs.
type TabletAnchorMapCursor struct {
	view    TabletAnchorMapView
	ordinal uint16
	valid   bool
}

// The combined-router companion below measures the physical mapping that can
// replace BucketMap once a tablet is segmented. It is kept separate from the
// fence codec so the stable-BucketID-only and combined designs remain directly
// comparable in benchmarks.
//
// A current leaf PageRef is represented once as a 48-bit 4-KiB page number, a
// 48-bit generation, and a one-byte extent class. LogicalID is derived from
// BucketID, Kind is fixed by the tablet, and flags are zero. Four opaque zone
// bytes bring the exact handle to 17 bytes.
const (
	TabletAnchorHandleHeaderSize  = 64
	TabletAnchorHandleHandleSize  = 17
	TabletAnchorHandleTrailerSize = 8

	tabletAnchorHandleMagic   = "AHLAB000"
	tabletAnchorHandleVersion = DevelopmentFormatVersion
	tabletAnchorHandleMissing = uint16(0xffff)
	tabletAnchorHandleMaxPage = uint64(1) << 48
)

// TabletAnchorHandleLeaf is one bucket's current physical binding: the stable
// BucketID, the PageRef of the leaf that presently holds it, and the zone
// summary that lets scans prune the leaf without reading it.
type TabletAnchorHandleLeaf struct {
	Bucket BucketID
	Ref    PageRef
	Zone   BucketZone
}

// TabletAnchorHandleRoute is a lexical route resolved to physical state: the
// fence route plus the current leaf PageRef and its zone summary, so one lookup
// yields both the logical bucket and where to read it.
type TabletAnchorHandleRoute struct {
	TabletAnchorMapRoute
	Ref  PageRef
	Zone BucketZone
}

// TabletAnchorHandleView binds one lexical fence image to its compact
// current handles and dense LocalLeafID resolver. The dense value is a stable
// anchor-page/slot locator in the segmented design; this monolithic lab spells
// the same 16-bit field as a lexical ordinal to isolate lookup cost.
type TabletAnchorHandleView struct {
	anchors   TabletAnchorMapView
	image     []byte
	locators  []byte
	handles   []byte
	tabletID  uint32
	localBits uint8
	leafKind  PageKind
}

// EncodeTabletAnchorMap writes one exact immutable image. Anchors must be
// strictly ordered by Fence. The returned slice aliases dst.
func EncodeTabletAnchorMap(
	dst []byte,
	header TabletAnchorMapHeader,
	firstBucket BucketID,
	anchors []TabletAnchorMapAnchor,
) ([]byte, error) {
	if header.TabletID == 0 || header.Generation == 0 ||
		uint32(firstBucket) >= PrimaryBucketIDLimit ||
		len(anchors) > TabletAnchorMapMaxFences {
		return nil, fmt.Errorf("%w: anchor-map identity or count", ErrInvalidWrite)
	}
	commonLength, maxFence, keyBytes, err := tabletAnchorMapMeasure(anchors)
	if err != nil {
		return nil, err
	}
	total := tabletAnchorMapImageBytes(
		len(anchors), commonLength, keyBytes,
	)
	if len(dst) < total {
		return nil, fmt.Errorf(
			"%w: anchor-map buffer has %d bytes, need %d",
			ErrInvalidWrite, len(dst), total,
		)
	}
	image := dst[:total]
	clear(image)
	tabletAnchorMapWriteHeader(
		image, header, len(anchors), commonLength, maxFence, keyBytes,
	)
	layout := tabletAnchorMapLayoutFor(
		len(anchors), commonLength, keyBytes,
	)
	accelerator := image[TabletAnchorMapHeaderSize:layout.commonAt]
	common := image[layout.commonAt:layout.offsetsAt]
	offsets := image[layout.offsetsAt:layout.bucketsAt]
	buckets := image[layout.bucketsAt:layout.keysAt]
	keys := image[layout.keysAt:layout.trailerAt]
	if len(anchors) != 0 {
		copy(common, anchors[0].Fence[:commonLength])
	}
	binary.LittleEndian.PutUint32(buckets[0:4], uint32(firstBucket))

	keyAt := 0
	acceleratorByte := 0
	var restart []byte
	for rank, anchor := range anchors {
		stripped := anchor.Fence[commonLength:]
		for acceleratorByte <= int(stripped[0]) {
			binary.LittleEndian.PutUint16(
				accelerator[acceleratorByte*2:], uint16(rank),
			)
			acceleratorByte++
		}
		binary.LittleEndian.PutUint16(offsets[rank*2:], uint16(keyAt))
		binary.LittleEndian.PutUint32(
			buckets[(rank+1)*4:], uint32(anchor.Bucket),
		)
		shared := 0
		if rank%TabletAnchorMapRestart == 0 {
			restart = stripped
		} else {
			shared = tabletAnchorMapPrefix(restart, stripped)
		}
		binary.LittleEndian.PutUint16(keys[keyAt:keyAt+2], uint16(shared))
		keyAt += 2
		copy(keys[keyAt:], stripped[shared:])
		keyAt += len(stripped) - shared
	}
	for acceleratorByte < TabletAnchorMapAcceleratorSlots {
		binary.LittleEndian.PutUint16(
			accelerator[acceleratorByte*2:], uint16(len(anchors)),
		)
		acceleratorByte++
	}
	binary.LittleEndian.PutUint16(
		offsets[len(anchors)*2:], uint16(keyAt),
	)
	tabletAnchorMapSeal(image)
	return image, nil
}

// OpenTabletAnchorMap verifies checksum, canonical section geometry,
// accelerator ranks, front coding, strict lexical ordering, and every 30-bit
// BucketID before returning a borrowed view.
func OpenTabletAnchorMap(src []byte) (TabletAnchorMapView, error) {
	if len(src) < TabletAnchorMapHeaderSize+
		tabletAnchorMapAcceleratorBytes+4+TabletAnchorMapTrailerSize ||
		string(src[0:8]) != tabletAnchorMapMagic ||
		binary.LittleEndian.Uint32(src[8:12]) != tabletAnchorMapVersion ||
		binary.LittleEndian.Uint16(src[12:14]) !=
			TabletAnchorMapHeaderSize ||
		src[14] != TabletAnchorMapRestart ||
		src[15] != 0 {
		return TabletAnchorMapView{}, tabletAnchorMapCorrupt("header")
	}
	header := TabletAnchorMapHeader{
		TabletID:   binary.LittleEndian.Uint64(src[16:24]),
		Generation: binary.LittleEndian.Uint64(src[24:32]),
	}
	fenceCount := int(binary.LittleEndian.Uint16(src[32:34]))
	commonLength := int(binary.LittleEndian.Uint16(src[34:36]))
	maxFence := int(binary.LittleEndian.Uint16(src[36:38]))
	keyBytes := int(binary.LittleEndian.Uint16(src[38:40]))
	imageBytes := int(binary.LittleEndian.Uint32(src[40:44]))
	if header.TabletID == 0 || header.Generation == 0 ||
		imageBytes != len(src) || !allZero(src[44:TabletAnchorMapHeaderSize]) {
		return TabletAnchorMapView{}, tabletAnchorMapCorrupt(
			"identity, length, or reserved bytes",
		)
	}
	layout := tabletAnchorMapLayoutFor(
		fenceCount, commonLength, keyBytes,
	)
	if layout.trailerAt+TabletAnchorMapTrailerSize != len(src) {
		return TabletAnchorMapView{}, tabletAnchorMapCorrupt("section geometry")
	}
	checksum := binary.LittleEndian.Uint32(src[layout.trailerAt:])
	if binary.LittleEndian.Uint32(src[layout.trailerAt+4:]) != ^checksum ||
		PageChecksum(src[:layout.trailerAt]) != checksum {
		return TabletAnchorMapView{}, tabletAnchorMapCorrupt("checksum")
	}
	view := TabletAnchorMapView{
		header:       header,
		image:        src,
		accelerator:  src[TabletAnchorMapHeaderSize:layout.commonAt],
		common:       src[layout.commonAt:layout.offsetsAt],
		offsets:      src[layout.offsetsAt:layout.bucketsAt],
		buckets:      src[layout.bucketsAt:layout.keysAt],
		keys:         src[layout.keysAt:layout.trailerAt],
		fenceCount:   uint16(fenceCount),
		maxFence:     uint16(maxFence),
		commonLength: uint16(commonLength),
	}
	if err := view.validateCanonical(); err != nil {
		return TabletAnchorMapView{}, err
	}
	return view, nil
}

// AdmittedTabletAnchorMap reconstructs a view whose checksum, section
// geometry, accelerators, front coding, ordering, and bucket identities were
// already validated by the admitting page cache. Calling it on arbitrary
// bytes is invalid.
func AdmittedTabletAnchorMap(src []byte) TabletAnchorMapView {
	fenceCount := int(binary.LittleEndian.Uint16(src[32:34]))
	commonLength := int(binary.LittleEndian.Uint16(src[34:36]))
	keyBytes := int(binary.LittleEndian.Uint16(src[38:40]))
	layout := tabletAnchorMapLayoutFor(
		fenceCount, commonLength, keyBytes,
	)
	return TabletAnchorMapView{
		header: TabletAnchorMapHeader{
			TabletID:   binary.LittleEndian.Uint64(src[16:24]),
			Generation: binary.LittleEndian.Uint64(src[24:32]),
		},
		image:        src,
		accelerator:  src[TabletAnchorMapHeaderSize:layout.commonAt],
		common:       src[layout.commonAt:layout.offsetsAt],
		offsets:      src[layout.offsetsAt:layout.bucketsAt],
		buckets:      src[layout.bucketsAt:layout.keysAt],
		keys:         src[layout.keysAt:layout.trailerAt],
		fenceCount:   uint16(fenceCount),
		maxFence:     binary.LittleEndian.Uint16(src[36:38]),
		commonLength: uint16(commonLength),
	}
}

// Header returns the tablet identity and generation this image was sealed with.
func (v TabletAnchorMapView) Header() TabletAnchorMapHeader {
	return v.header
}

// FenceCount returns the number of interval fences, which is one fewer than the
// number of buckets the map routes to.
func (v TabletAnchorMapView) FenceCount() int {
	return int(v.fenceCount)
}

// BucketCount returns the number of stable buckets the map routes to, or zero
// for an empty image.
func (v TabletAnchorMapView) BucketCount() int {
	if len(v.image) == 0 {
		return 0
	}
	return int(v.fenceCount) + 1
}

// PersistentBytes returns the borrowed on-disk image. It aliases the admitted
// page and must not be mutated or retained past the page's lease.
func (v TabletAnchorMapView) PersistentBytes() []byte {
	return v.image
}

// BytesPerAnchor reports the sealed image size amortized over its fences, a
// space-accounting probe used by benchmarks rather than the read path.
func (v TabletAnchorMapView) BytesPerAnchor() float64 {
	if v.fenceCount == 0 {
		return 0
	}
	return float64(len(v.image)) / float64(v.fenceCount)
}

// Route hashes key exactly once and returns both the lexical interval and the
// hash consumed by the ordered leaf.
func (v TabletAnchorMapView) Route(
	seed [16]byte, key []byte,
) TabletAnchorMapRoute {
	hash := KeyHashBytes(seed, key)
	return v.RouteHashed(hash, key)
}

// RouteHashed reuses a hash already computed by a batch router or exact-index
// path. Tags never determine identity; the ordered leaf still verifies key.
func (v TabletAnchorMapView) RouteHashed(
	hash uint64, key []byte,
) TabletAnchorMapRoute {
	ordinal := v.upperBound(key)
	return TabletAnchorMapRoute{
		Bucket:  v.bucketAt(ordinal),
		Hash:    hash,
		Ordinal: uint16(ordinal),
	}
}

// LowerBound returns a lexical bucket cursor positioned at the first candidate
// interval. A shortened separator can leave a keyless gap after the left leaf;
// if that leaf is exhausted, Next advances by anchor order, never a sibling
// pointer.
func (v TabletAnchorMapView) LowerBound(
	target []byte,
) TabletAnchorMapCursor {
	if len(v.image) == 0 {
		return TabletAnchorMapCursor{}
	}
	return TabletAnchorMapCursor{
		view: v, ordinal: uint16(v.upperBound(target)), valid: true,
	}
}

// Bucket returns the stable BucketID at the cursor's current interval, or false
// once the cursor is exhausted.
func (c *TabletAnchorMapCursor) Bucket() (BucketID, bool) {
	if c == nil || !c.valid {
		return 0, false
	}
	return c.view.bucketAt(int(c.ordinal)), true
}

// Ordinal returns the cursor's current lexical position, or false once the
// cursor is exhausted.
func (c *TabletAnchorMapCursor) Ordinal() (int, bool) {
	if c == nil || !c.valid {
		return 0, false
	}
	return int(c.ordinal), true
}

// Next advances to the next interval in lexical order and reports whether one
// exists, invalidating the cursor when it walks off the end.
func (c *TabletAnchorMapCursor) Next() bool {
	if c == nil || !c.valid ||
		int(c.ordinal)+1 >= c.view.BucketCount() {
		if c != nil {
			c.valid = false
		}
		return false
	}
	c.ordinal++
	return true
}

// FenceAt returns one borrowed, non-contiguous fence spelling. The common,
// restart-prefix, and suffix parts concatenate to the exact fence without
// reconstruction or allocation.
func (v TabletAnchorMapView) FenceAt(
	rank int,
) (common, restartPrefix, suffix []byte, ok bool) {
	if rank < 0 || rank >= int(v.fenceCount) {
		return nil, nil, nil, false
	}
	common, restartPrefix, suffix = v.fenceParts(rank)
	return common, restartPrefix, suffix, true
}

// ApplyBatch performs one immutable structural rewrite with caller-owned
// scratch. scratch must hold two maximum-length fences. Inserts and deletes
// change only stable BucketID order; the physical BucketMap is independent.
func (v TabletAnchorMapView) ApplyBatch(
	dst, scratch []byte,
	generation uint64,
	edits []TabletAnchorMapEdit,
) ([]byte, error) {
	if len(v.image) == 0 || generation <= v.header.Generation {
		return nil, fmt.Errorf("%w: anchor-map generation", ErrInvalidWrite)
	}
	maxFence := int(v.maxFence)
	if err := tabletAnchorMapValidateEdits(edits, &maxFence); err != nil {
		return nil, err
	}
	if maxFence != 0 && len(scratch) < maxFence*2 {
		return nil, fmt.Errorf(
			"%w: have %d bytes, need %d",
			ErrTabletAnchorMapScratch, len(scratch), maxFence*2,
		)
	}
	base := scratch[:maxFence:maxFence]
	work := scratch[maxFence : maxFence*2 : maxFence*2]

	count := int(v.fenceCount)
	commonLength := len(v.common)
	measuredMax := int(v.maxFence)
	needsMeasure := v.fenceCount == 0
	for _, edit := range edits {
		switch edit.Operation {
		case TabletAnchorMapInsert:
			count++
			if !bytes.HasPrefix(edit.Fence, v.common) {
				needsMeasure = true
			}
			measuredMax = max(measuredMax, len(edit.Fence))
		case TabletAnchorMapDelete:
			count--
			// Removing the shortest or a common-prefix-limiting fence may
			// change both canonical prefix length and maximum length.
			needsMeasure = true
		}
	}
	if count < 0 || count > TabletAnchorMapMaxFences {
		return nil, fmt.Errorf("%w: anchor fence count", ErrInvalidWrite)
	}

	merged := tabletAnchorMapMerge{view: v, edits: edits, work: work}
	if needsMeasure {
		count, measuredMax = 0, 0
		minimum := 0
		for {
			fence, _, ok, err := merged.next()
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			if count == 0 {
				copy(base, fence)
				commonLength = len(fence)
				minimum = len(fence)
			} else {
				commonLength = min(
					commonLength,
					tabletAnchorMapPrefix(base[:commonLength], fence),
				)
				minimum = min(minimum, len(fence))
			}
			measuredMax = max(measuredMax, len(fence))
			count++
			if count > TabletAnchorMapMaxFences {
				return nil, fmt.Errorf(
					"%w: too many anchor fences", ErrInvalidWrite,
				)
			}
		}
		if count == 0 {
			commonLength = 0
		} else {
			commonLength = min(commonLength, minimum-1)
		}
	}

	// keysAt is independent of compressed key bytes, so the final merge can
	// encode directly and discover the exact trailer position. Insert/replace
	// split batches whose fences retain the existing common prefix take only
	// this single merge pass.
	layout := tabletAnchorMapLayoutFor(count, commonLength, 0)
	if len(dst) < layout.keysAt+TabletAnchorMapTrailerSize {
		return nil, fmt.Errorf(
			"%w: anchor-map buffer has %d bytes, need at least %d",
			ErrInvalidWrite, len(dst),
			layout.keysAt+TabletAnchorMapTrailerSize,
		)
	}
	clear(dst[:layout.keysAt])
	accelerator := dst[TabletAnchorMapHeaderSize:layout.commonAt]
	common := dst[layout.commonAt:layout.offsetsAt]
	offsets := dst[layout.offsetsAt:layout.bucketsAt]
	buckets := dst[layout.bucketsAt:layout.keysAt]
	keys := dst[layout.keysAt : len(dst)-TabletAnchorMapTrailerSize]
	if commonLength != 0 {
		if needsMeasure {
			copy(common, base[:commonLength])
		} else {
			copy(common, v.common)
		}
	}
	binary.LittleEndian.PutUint32(buckets[0:4], uint32(v.bucketAt(0)))

	merged.reset()
	keyAt, acceleratorByte := 0, 0
	rank, restartLength := 0, 0
	for {
		fence, bucket, ok, err := merged.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		stripped := fence[commonLength:]
		for acceleratorByte <= int(stripped[0]) {
			binary.LittleEndian.PutUint16(
				accelerator[acceleratorByte*2:], uint16(rank),
			)
			acceleratorByte++
		}
		binary.LittleEndian.PutUint16(offsets[rank*2:], uint16(keyAt))
		binary.LittleEndian.PutUint32(
			buckets[(rank+1)*4:], uint32(bucket),
		)
		shared := 0
		if rank%TabletAnchorMapRestart == 0 {
			copy(base, stripped)
			restartLength = len(stripped)
		} else {
			shared = tabletAnchorMapPrefix(
				base[:restartLength], stripped,
			)
		}
		encodedLength := 2 + len(stripped) - shared
		if keyAt+encodedLength > len(keys) ||
			keyAt+encodedLength > int(^uint16(0)) {
			return nil, fmt.Errorf(
				"%w: anchor key arena or destination is too small",
				ErrInvalidWrite,
			)
		}
		binary.LittleEndian.PutUint16(keys[keyAt:keyAt+2], uint16(shared))
		keyAt += 2
		copy(keys[keyAt:], stripped[shared:])
		keyAt += len(stripped) - shared
		rank++
	}
	for acceleratorByte < TabletAnchorMapAcceleratorSlots {
		binary.LittleEndian.PutUint16(
			accelerator[acceleratorByte*2:], uint16(count),
		)
		acceleratorByte++
	}
	binary.LittleEndian.PutUint16(offsets[count*2:], uint16(keyAt))
	total := layout.keysAt + keyAt + TabletAnchorMapTrailerSize
	image := dst[:total]
	tabletAnchorMapWriteHeader(
		image,
		TabletAnchorMapHeader{
			TabletID: v.header.TabletID, Generation: generation,
		},
		count, commonLength, measuredMax, keyAt,
	)
	tabletAnchorMapSeal(image)
	return image, nil
}

// ShortestTabletAnchorMapFence derives the shortest prefix of rightMin that
// is strictly greater than leftMax. The returned fence is <= rightMin, so it
// cannot route any right-leaf key left.
func ShortestTabletAnchorMapFence(
	dst, leftMax, rightMin []byte,
) ([]byte, error) {
	if bytes.Compare(leftMax, rightMin) >= 0 {
		return nil, fmt.Errorf("%w: overlapping split ranges", ErrInvalidWrite)
	}
	length := tabletAnchorMapPrefix(leftMax, rightMin) + 1
	if len(dst) < length {
		return nil, fmt.Errorf(
			"%w: split-fence buffer has %d bytes, need %d",
			ErrInvalidWrite, len(dst), length,
		)
	}
	copy(dst, rightMin[:length])
	return dst[:length], nil
}

// ComposeTabletAnchorBucketID encodes one hierarchical 30-bit identity. The
// two evaluated production geometries are localBits=12 (18/12) and 13 (17/13).
func ComposeTabletAnchorBucketID(
	tabletID uint32, localID uint16, localBits uint8,
) (BucketID, bool) {
	if localBits == 0 || localBits >= 16 ||
		tabletID >= uint32(1)<<(30-localBits) ||
		uint32(localID) >= uint32(1)<<localBits {
		return 0, false
	}
	return BucketID(tabletID<<localBits | uint32(localID)), true
}

// SplitTabletAnchorBucketID inverts ComposeTabletAnchorBucketID, recovering the
// tablet ID and local leaf ID from a BucketID given the tablet's local-ID bit
// width. It returns false for an out-of-range width or bucket.
func SplitTabletAnchorBucketID(
	bucket BucketID, localBits uint8,
) (tabletID uint32, localID uint16, ok bool) {
	if localBits == 0 || localBits >= 16 ||
		uint32(bucket) >= PrimaryBucketIDLimit {
		return 0, 0, false
	}
	mask := uint32(1)<<localBits - 1
	return uint32(bucket) >> localBits, uint16(uint32(bucket) & mask), true
}

// EncodeTabletAnchorHandles writes every current PageRef/zone exactly once
// and a dense LocalLeafID resolver. The fence image remains separate only so
// benchmarks can compare the old and combined physical charges; the segmented
// production codec interleaves each handle with its fence row.
func EncodeTabletAnchorHandles(
	dst []byte,
	anchors TabletAnchorMapView,
	localBits uint8,
	leafKind PageKind,
	leaves []TabletAnchorHandleLeaf,
) ([]byte, error) {
	if len(anchors.image) == 0 || anchors.header.TabletID >
		uint64(^uint32(0)) ||
		localBits < 12 || localBits > 13 ||
		!validPageKind(leafKind) ||
		len(leaves) != anchors.BucketCount() ||
		len(leaves) > 1<<localBits {
		return nil, fmt.Errorf(
			"%w: combined anchor identity or geometry", ErrInvalidWrite,
		)
	}
	tabletID := uint32(anchors.header.TabletID)
	locatorBytes := (1 << localBits) * 2
	total := TabletAnchorHandleHeaderSize + locatorBytes +
		len(leaves)*TabletAnchorHandleHandleSize +
		TabletAnchorHandleTrailerSize
	if len(dst) < total {
		return nil, fmt.Errorf(
			"%w: anchor-handle buffer has %d bytes, need %d",
			ErrInvalidWrite, len(dst), total,
		)
	}
	image := dst[:total]
	clear(image)
	copy(image[0:8], tabletAnchorHandleMagic)
	binary.LittleEndian.PutUint32(
		image[8:12], tabletAnchorHandleVersion,
	)
	binary.LittleEndian.PutUint16(
		image[12:14], TabletAnchorHandleHeaderSize,
	)
	image[14] = localBits
	image[15] = byte(leafKind)
	binary.LittleEndian.PutUint32(image[16:20], tabletID)
	binary.LittleEndian.PutUint16(image[20:22], uint16(len(leaves)))
	binary.LittleEndian.PutUint16(
		image[22:24], TabletAnchorHandleHandleSize,
	)
	binary.LittleEndian.PutUint64(
		image[24:32], anchors.header.Generation,
	)
	binary.LittleEndian.PutUint32(image[32:36], uint32(total))
	binary.LittleEndian.PutUint32(
		image[36:40], PageChecksum(anchors.image),
	)
	locators := image[TabletAnchorHandleHeaderSize : TabletAnchorHandleHeaderSize+locatorBytes]
	for at := range locators {
		locators[at] = 0xff
	}
	handles := image[TabletAnchorHandleHeaderSize+locatorBytes : total-TabletAnchorHandleTrailerSize]
	for ordinal, leaf := range leaves {
		if leaf.Bucket != anchors.bucketAt(ordinal) {
			return nil, fmt.Errorf(
				"%w: handle bucket at ordinal %d", ErrInvalidWrite, ordinal,
			)
		}
		bucketTablet, localID, ok := SplitTabletAnchorBucketID(
			leaf.Bucket, localBits,
		)
		if !ok || bucketTablet != tabletID ||
			binary.LittleEndian.Uint16(locators[int(localID)*2:]) !=
				tabletAnchorHandleMissing {
			return nil, fmt.Errorf(
				"%w: duplicate or foreign local leaf", ErrInvalidWrite,
			)
		}
		if err := tabletAnchorHandleValidateRef(
			leaf.Ref, leaf.Bucket, leafKind,
		); err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint16(
			locators[int(localID)*2:], uint16(ordinal),
		)
		tabletAnchorHandleEncode(
			handles[ordinal*TabletAnchorHandleHandleSize:],
			leaf.Ref, leaf.Zone,
		)
	}
	tabletAnchorHandleSeal(image)
	return image, nil
}

// OpenTabletAnchorHandles binds a compact handle image to the exact
// checksum-admitted fence map and validates the complete dense inverse map.
func OpenTabletAnchorHandles(
	src []byte,
	anchors TabletAnchorMapView,
) (TabletAnchorHandleView, error) {
	if len(src) < TabletAnchorHandleHeaderSize+
		TabletAnchorHandleTrailerSize ||
		string(src[0:8]) != tabletAnchorHandleMagic ||
		binary.LittleEndian.Uint32(src[8:12]) !=
			tabletAnchorHandleVersion ||
		binary.LittleEndian.Uint16(src[12:14]) !=
			TabletAnchorHandleHeaderSize ||
		binary.LittleEndian.Uint16(src[22:24]) !=
			TabletAnchorHandleHandleSize ||
		!allZero(src[40:TabletAnchorHandleHeaderSize]) {
		return TabletAnchorHandleView{},
			tabletAnchorHandleCorrupt("header")
	}
	localBits := src[14]
	leafKind := PageKind(src[15])
	tabletID := binary.LittleEndian.Uint32(src[16:20])
	count := int(binary.LittleEndian.Uint16(src[20:22]))
	generation := binary.LittleEndian.Uint64(src[24:32])
	imageBytes := int(binary.LittleEndian.Uint32(src[32:36]))
	anchorChecksum := binary.LittleEndian.Uint32(src[36:40])
	if localBits < 12 || localBits > 13 || !validPageKind(leafKind) ||
		imageBytes != len(src) || len(anchors.image) == 0 ||
		uint64(tabletID) != anchors.header.TabletID ||
		generation != anchors.header.Generation ||
		count != anchors.BucketCount() ||
		anchorChecksum != PageChecksum(anchors.image) {
		return TabletAnchorHandleView{},
			tabletAnchorHandleCorrupt("identity or binding")
	}
	locatorBytes := (1 << localBits) * 2
	trailer := TabletAnchorHandleHeaderSize + locatorBytes +
		count*TabletAnchorHandleHandleSize
	if trailer+TabletAnchorHandleTrailerSize != len(src) {
		return TabletAnchorHandleView{},
			tabletAnchorHandleCorrupt("section geometry")
	}
	checksum := binary.LittleEndian.Uint32(src[trailer:])
	if binary.LittleEndian.Uint32(src[trailer+4:]) != ^checksum ||
		PageChecksum(src[:trailer]) != checksum {
		return TabletAnchorHandleView{},
			tabletAnchorHandleCorrupt("checksum")
	}
	view := TabletAnchorHandleView{
		anchors:   anchors,
		image:     src,
		locators:  src[TabletAnchorHandleHeaderSize : TabletAnchorHandleHeaderSize+locatorBytes],
		handles:   src[TabletAnchorHandleHeaderSize+locatorBytes : trailer],
		tabletID:  tabletID,
		localBits: localBits,
		leafKind:  leafKind,
	}
	live := 0
	for localID := 0; localID < 1<<localBits; localID++ {
		ordinal := binary.LittleEndian.Uint16(view.locators[localID*2:])
		if ordinal == tabletAnchorHandleMissing {
			continue
		}
		if int(ordinal) >= count {
			return TabletAnchorHandleView{},
				tabletAnchorHandleCorrupt("locator ordinal")
		}
		live++
	}
	if live != count {
		return TabletAnchorHandleView{},
			tabletAnchorHandleCorrupt("locator cardinality")
	}
	for ordinal := range count {
		bucket := anchors.bucketAt(ordinal)
		bucketTablet, localID, ok := SplitTabletAnchorBucketID(
			bucket, localBits,
		)
		if !ok || bucketTablet != tabletID ||
			binary.LittleEndian.Uint16(view.locators[int(localID)*2:]) !=
				uint16(ordinal) {
			return TabletAnchorHandleView{},
				tabletAnchorHandleCorrupt("locator binding")
		}
		ref, _ := view.handleAt(ordinal, bucket)
		if err := tabletAnchorHandleValidateRef(
			ref, bucket, leafKind,
		); err != nil {
			return TabletAnchorHandleView{},
				tabletAnchorHandleCorrupt("leaf reference")
		}
	}
	return view, nil
}

// Route resolves key through the lexical fences and binds the result to the
// current leaf PageRef and zone in one pass, hashing the primary key only once.
func (v TabletAnchorHandleView) Route(
	seed [16]byte, key []byte,
) TabletAnchorHandleRoute {
	return v.route(v.anchors.Route(seed, key))
}

// RouteHashed is Route for a caller that already holds the primary-key hash and
// so avoids rehashing it.
func (v TabletAnchorHandleView) RouteHashed(
	hash uint64, key []byte,
) TabletAnchorHandleRoute {
	return v.route(v.anchors.RouteHashed(hash, key))
}

func (v TabletAnchorHandleView) route(
	route TabletAnchorMapRoute,
) TabletAnchorHandleRoute {
	ref, zone := v.handleAt(int(route.Ordinal), route.Bucket)
	return TabletAnchorHandleRoute{
		TabletAnchorMapRoute: route, Ref: ref, Zone: zone,
	}
}

// ResolveBucketID is the posting-driven route. Production interprets the
// 16-bit locator as stable anchor-page ID plus stable row slot, asks the tablet
// root for that page's current PageRef, and decodes this same handle.
func (v TabletAnchorHandleView) ResolveBucketID(
	bucket BucketID,
) (PageRef, BucketZone, bool) {
	tabletID, localID, ok := SplitTabletAnchorBucketID(
		bucket, v.localBits,
	)
	if !ok || tabletID != v.tabletID {
		return PageRef{}, BucketZone{}, false
	}
	ordinal := binary.LittleEndian.Uint16(v.locators[int(localID)*2:])
	if ordinal == tabletAnchorHandleMissing ||
		int(ordinal) >= v.anchors.BucketCount() ||
		v.anchors.bucketAt(int(ordinal)) != bucket {
		return PageRef{}, BucketZone{}, false
	}
	ref, zone := v.handleAt(int(ordinal), bucket)
	return ref, zone, true
}

func (v TabletAnchorHandleView) handleAt(
	ordinal int, bucket BucketID,
) (PageRef, BucketZone) {
	start := ordinal * TabletAnchorHandleHandleSize
	src := v.handles[start : start+TabletAnchorHandleHandleSize]
	page := tabletAnchorHandleGetUint48(src[0:6])
	generation := tabletAnchorHandleGetUint48(src[6:12])
	length := uint32(4096) << src[12]
	var zone BucketZone
	copy(zone[:], src[13:17])
	return PageRef{
		Offset:     page << 12,
		LogicalID:  uint64(bucket) + 1,
		Generation: generation,
		Length:     length,
		Kind:       v.leafKind,
	}, zone
}

// CombinedBytesPerAnchor projects the canonical interleaved codec: local IDs
// are two bytes rather than the comparison codec's four-byte global IDs.
func (v TabletAnchorHandleView) CombinedBytesPerAnchor() float64 {
	count := v.anchors.BucketCount()
	if count == 0 {
		return 0
	}
	projected := len(v.anchors.image) - count*2 + len(v.image)
	return float64(projected) / float64(count)
}

func tabletAnchorHandleValidateRef(
	ref PageRef, bucket BucketID, leafKind PageKind,
) error {
	if ref.Offset == 0 || ref.Offset&4095 != 0 ||
		ref.Offset>>12 >= tabletAnchorHandleMaxPage ||
		ref.Generation == 0 ||
		ref.Generation >= tabletAnchorHandleMaxPage ||
		ref.LogicalID != uint64(bucket)+1 ||
		ref.Kind != leafKind ||
		tabletAnchorHandleExtentClass(ref.Length) < 0 {
		return fmt.Errorf("%w: non-canonical compact leaf ref", ErrInvalidWrite)
	}
	return nil
}

func tabletAnchorHandleExtentClass(length uint32) int {
	switch length {
	case 4 << 10:
		return 0
	case 8 << 10:
		return 1
	case 16 << 10:
		return 2
	case 32 << 10:
		return 3
	case 64 << 10:
		return 4
	default:
		return -1
	}
}

func tabletAnchorHandleEncode(
	dst []byte, ref PageRef, zone BucketZone,
) {
	tabletAnchorHandlePutUint48(dst[0:6], ref.Offset>>12)
	tabletAnchorHandlePutUint48(dst[6:12], ref.Generation)
	dst[12] = byte(tabletAnchorHandleExtentClass(ref.Length))
	copy(dst[13:17], zone[:])
}

func tabletAnchorHandleGetUint48(src []byte) uint64 {
	return uint64(src[0]) |
		uint64(src[1])<<8 |
		uint64(src[2])<<16 |
		uint64(src[3])<<24 |
		uint64(src[4])<<32 |
		uint64(src[5])<<40
}

func tabletAnchorHandlePutUint48(dst []byte, value uint64) {
	dst[0] = byte(value)
	dst[1] = byte(value >> 8)
	dst[2] = byte(value >> 16)
	dst[3] = byte(value >> 24)
	dst[4] = byte(value >> 32)
	dst[5] = byte(value >> 40)
}

func tabletAnchorHandleSeal(image []byte) {
	trailer := len(image) - TabletAnchorHandleTrailerSize
	checksum := PageChecksum(image[:trailer])
	binary.LittleEndian.PutUint32(image[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(image[trailer+4:], ^checksum)
}

func tabletAnchorHandleCorrupt(reason string) error {
	return fmt.Errorf("%w: %s", ErrTabletAnchorHandleCorrupt, reason)
}

type tabletAnchorMapLayout struct {
	commonAt, offsetsAt, bucketsAt, keysAt, trailerAt int
}

func tabletAnchorMapLayoutFor(
	fenceCount, commonLength, keyBytes int,
) tabletAnchorMapLayout {
	layout := tabletAnchorMapLayout{
		commonAt: TabletAnchorMapHeaderSize +
			tabletAnchorMapAcceleratorBytes,
	}
	layout.offsetsAt = layout.commonAt + commonLength
	layout.bucketsAt = layout.offsetsAt + (fenceCount+1)*2
	layout.keysAt = layout.bucketsAt + (fenceCount+1)*4
	layout.trailerAt = layout.keysAt + keyBytes
	return layout
}

func tabletAnchorMapImageBytes(
	fenceCount, commonLength, keyBytes int,
) int {
	return tabletAnchorMapLayoutFor(
		fenceCount, commonLength, keyBytes,
	).trailerAt + TabletAnchorMapTrailerSize
}

func tabletAnchorMapWriteHeader(
	image []byte,
	header TabletAnchorMapHeader,
	fenceCount, commonLength, maxFence, keyBytes int,
) {
	copy(image[0:8], tabletAnchorMapMagic)
	binary.LittleEndian.PutUint32(image[8:12], tabletAnchorMapVersion)
	binary.LittleEndian.PutUint16(
		image[12:14], TabletAnchorMapHeaderSize,
	)
	image[14] = TabletAnchorMapRestart
	binary.LittleEndian.PutUint64(image[16:24], header.TabletID)
	binary.LittleEndian.PutUint64(image[24:32], header.Generation)
	binary.LittleEndian.PutUint16(image[32:34], uint16(fenceCount))
	binary.LittleEndian.PutUint16(image[34:36], uint16(commonLength))
	binary.LittleEndian.PutUint16(image[36:38], uint16(maxFence))
	binary.LittleEndian.PutUint16(image[38:40], uint16(keyBytes))
	binary.LittleEndian.PutUint32(image[40:44], uint32(len(image)))
}

func tabletAnchorMapSeal(image []byte) {
	trailer := len(image) - TabletAnchorMapTrailerSize
	checksum := PageChecksum(image[:trailer])
	binary.LittleEndian.PutUint32(image[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(image[trailer+4:], ^checksum)
}

func tabletAnchorMapMeasure(
	anchors []TabletAnchorMapAnchor,
) (commonLength, maxFence, keyBytes int, err error) {
	if len(anchors) == 0 {
		return 0, 0, 0, nil
	}
	minimum := len(anchors[0].Fence)
	commonLength = minimum
	var previous []byte
	for rank, anchor := range anchors {
		if len(anchor.Fence) == 0 ||
			uint32(anchor.Bucket) >= PrimaryBucketIDLimit ||
			len(anchor.Fence) > int(^uint16(0)) ||
			rank != 0 && bytes.Compare(previous, anchor.Fence) >= 0 {
			return 0, 0, 0, fmt.Errorf(
				"%w: non-canonical anchor at rank %d", ErrInvalidWrite, rank,
			)
		}
		if rank != 0 {
			commonLength = min(
				commonLength,
				tabletAnchorMapPrefix(
					anchors[0].Fence[:commonLength], anchor.Fence,
				),
			)
		}
		minimum = min(minimum, len(anchor.Fence))
		maxFence = max(maxFence, len(anchor.Fence))
		previous = anchor.Fence
	}
	commonLength = min(commonLength, minimum-1)
	var restart []byte
	for rank, anchor := range anchors {
		stripped := anchor.Fence[commonLength:]
		shared := 0
		if rank%TabletAnchorMapRestart == 0 {
			restart = stripped
		} else {
			shared = tabletAnchorMapPrefix(restart, stripped)
		}
		keyBytes += 2 + len(stripped) - shared
		if keyBytes > int(^uint16(0)) {
			return 0, 0, 0, fmt.Errorf(
				"%w: anchor key arena exceeds 16 bits", ErrInvalidWrite,
			)
		}
	}
	return commonLength, maxFence, keyBytes, nil
}

func (v TabletAnchorMapView) validateCanonical() error {
	count := int(v.fenceCount)
	if len(v.accelerator) != tabletAnchorMapAcceleratorBytes ||
		len(v.offsets) != (count+1)*2 ||
		len(v.buckets) != (count+1)*4 ||
		len(v.common) != int(v.commonLength) ||
		binary.LittleEndian.Uint16(v.offsets[0:2]) != 0 ||
		int(binary.LittleEndian.Uint16(v.offsets[count*2:])) != len(v.keys) {
		return tabletAnchorMapCorrupt("section sizes or terminal offset")
	}
	if uint32(v.bucketAt(0)) >= PrimaryBucketIDLimit {
		return tabletAnchorMapCorrupt("first bucket")
	}
	if count == 0 {
		if len(v.common) != 0 || len(v.keys) != 0 || v.maxFence != 0 {
			return tabletAnchorMapCorrupt("empty map metadata")
		}
		for slot := range TabletAnchorMapAcceleratorSlots {
			if binary.LittleEndian.Uint16(v.accelerator[slot*2:]) != 0 {
				return tabletAnchorMapCorrupt("empty accelerator")
			}
		}
		return nil
	}

	observedMax := 0
	minimum := int(^uint(0) >> 1)
	firstByte := -1
	acceleratorSlot := 0
	var previous [3][]byte
	for rank := range count {
		start := int(binary.LittleEndian.Uint16(v.offsets[rank*2:]))
		end := int(binary.LittleEndian.Uint16(v.offsets[(rank+1)*2:]))
		if start < 0 || end < start+2 || end > len(v.keys) ||
			uint32(v.bucketAt(rank+1)) >= PrimaryBucketIDLimit {
			return tabletAnchorMapCorrupt("entry offset or bucket")
		}
		shared := int(binary.LittleEndian.Uint16(v.keys[start : start+2]))
		restartRank := rank / TabletAnchorMapRestart *
			TabletAnchorMapRestart
		restartStart := int(binary.LittleEndian.Uint16(
			v.offsets[restartRank*2:],
		))
		restartEnd := int(binary.LittleEndian.Uint16(
			v.offsets[(restartRank+1)*2:],
		))
		if restartEnd < restartStart+2 || restartEnd > len(v.keys) {
			return tabletAnchorMapCorrupt("restart offset")
		}
		restart := v.keys[restartStart+2 : restartEnd]
		if rank%TabletAnchorMapRestart == 0 {
			if shared != 0 {
				return tabletAnchorMapCorrupt("restart sharing")
			}
		} else if shared > len(restart) {
			return tabletAnchorMapCorrupt("shared prefix")
		}
		suffix := v.keys[start+2 : end]
		length := len(v.common) + shared + len(suffix)
		if length == 0 || length > int(^uint16(0)) {
			return tabletAnchorMapCorrupt("fence length")
		}
		observedMax = max(observedMax, length)
		minimum = min(minimum, length)
		current := [3][]byte{v.common, restart[:shared], suffix}
		if rank != 0 &&
			tabletAnchorMapCompareParts(previous, current) >= 0 {
			return tabletAnchorMapCorrupt("fence order")
		}
		previous = current
		strippedFirst := 0
		if len(current[1]) == 0 {
			strippedFirst = int(current[2][0])
		} else {
			strippedFirst = int(current[1][0])
		}
		if strippedFirst < firstByte {
			return tabletAnchorMapCorrupt("accelerator order")
		}
		for acceleratorSlot <= strippedFirst {
			if int(binary.LittleEndian.Uint16(
				v.accelerator[acceleratorSlot*2:],
			)) != rank {
				return tabletAnchorMapCorrupt("accelerator rank")
			}
			acceleratorSlot++
		}
		firstByte = strippedFirst
	}
	for acceleratorSlot < TabletAnchorMapAcceleratorSlots {
		if int(binary.LittleEndian.Uint16(
			v.accelerator[acceleratorSlot*2:],
		)) != count {
			return tabletAnchorMapCorrupt("accelerator tail")
		}
		acceleratorSlot++
	}
	if observedMax != int(v.maxFence) {
		return tabletAnchorMapCorrupt("maximum fence")
	}
	// The common prefix is canonical and maximal, except that one byte remains
	// in every stripped fence for the accelerator.
	if len(v.common) >= minimum {
		return tabletAnchorMapCorrupt("common prefix length")
	}
	if minimum > len(v.common)+1 && firstByte ==
		tabletAnchorMapFirstStrippedByte(v, 0) {
		allEqual := true
		want := tabletAnchorMapFirstStrippedByte(v, 0)
		for rank := 1; rank < count; rank++ {
			if tabletAnchorMapFirstStrippedByte(v, rank) != want {
				allEqual = false
				break
			}
		}
		if allEqual {
			return tabletAnchorMapCorrupt("non-maximal common prefix")
		}
	}
	return nil
}

func (v TabletAnchorMapView) upperBound(target []byte) int {
	if v.fenceCount == 0 {
		return 0
	}
	common := v.common
	limit := min(len(common), len(target))
	if comparison := bytes.Compare(target[:limit], common[:limit]); comparison < 0 {
		return 0
	} else if comparison > 0 {
		return int(v.fenceCount)
	}
	if len(target) <= len(common) {
		return 0
	}
	stripped := target[len(common):]
	selector := int(stripped[0])
	low := int(binary.LittleEndian.Uint16(v.accelerator[selector*2:]))
	high := int(binary.LittleEndian.Uint16(
		v.accelerator[(selector+1)*2:],
	))
	for low < high {
		middle := int(uint(low+high) >> 1)
		if v.compareStrippedFence(middle, stripped) <= 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func (v TabletAnchorMapView) compareStrippedFence(
	rank int, target []byte,
) int {
	_, prefix, suffix := v.fenceParts(rank)
	at := 0
	for _, part := range [2][]byte{prefix, suffix} {
		limit := min(len(part), len(target)-at)
		if limit > 0 {
			if comparison := bytes.Compare(
				part[:limit], target[at:at+limit],
			); comparison != 0 {
				return comparison
			}
			at += limit
		}
		if limit != len(part) {
			return 1
		}
	}
	if at < len(target) {
		return -1
	}
	return 0
}

func (v TabletAnchorMapView) fenceParts(
	rank int,
) (common, restartPrefix, suffix []byte) {
	start := int(binary.LittleEndian.Uint16(v.offsets[rank*2:]))
	end := int(binary.LittleEndian.Uint16(v.offsets[(rank+1)*2:]))
	shared := int(binary.LittleEndian.Uint16(v.keys[start : start+2]))
	restartRank := rank / TabletAnchorMapRestart *
		TabletAnchorMapRestart
	restartStart := int(binary.LittleEndian.Uint16(
		v.offsets[restartRank*2:],
	))
	restartEnd := int(binary.LittleEndian.Uint16(
		v.offsets[(restartRank+1)*2:],
	))
	restart := v.keys[restartStart+2 : restartEnd]
	return v.common, restart[:shared], v.keys[start+2 : end]
}

func (v TabletAnchorMapView) materializeFence(
	rank int, dst []byte,
) []byte {
	common, prefix, suffix := v.fenceParts(rank)
	length := len(common) + len(prefix) + len(suffix)
	out := dst[:length]
	at := copy(out, common)
	at += copy(out[at:], prefix)
	copy(out[at:], suffix)
	return out
}

func (v TabletAnchorMapView) bucketAt(ordinal int) BucketID {
	return BucketID(binary.LittleEndian.Uint32(v.buckets[ordinal*4:]))
}

func tabletAnchorMapFirstStrippedByte(
	v TabletAnchorMapView, rank int,
) int {
	_, prefix, suffix := v.fenceParts(rank)
	if len(prefix) != 0 {
		return int(prefix[0])
	}
	return int(suffix[0])
}

func tabletAnchorMapCompareParts(left, right [3][]byte) int {
	leftPart, rightPart := 0, 0
	leftAt, rightAt := 0, 0
	for {
		for leftPart < len(left) && leftAt == len(left[leftPart]) {
			leftPart++
			leftAt = 0
		}
		for rightPart < len(right) && rightAt == len(right[rightPart]) {
			rightPart++
			rightAt = 0
		}
		if leftPart == len(left) || rightPart == len(right) {
			switch {
			case leftPart == len(left) && rightPart == len(right):
				return 0
			case leftPart == len(left):
				return -1
			default:
				return 1
			}
		}
		if left[leftPart][leftAt] < right[rightPart][rightAt] {
			return -1
		}
		if left[leftPart][leftAt] > right[rightPart][rightAt] {
			return 1
		}
		leftAt++
		rightAt++
	}
}

func tabletAnchorMapPrefix(left, right []byte) int {
	limit := min(len(left), len(right))
	at := 0
	for at < limit && left[at] == right[at] {
		at++
	}
	return at
}

func tabletAnchorMapCorrupt(reason string) error {
	return fmt.Errorf("%w: %s", ErrTabletAnchorMapCorrupt, reason)
}

func tabletAnchorMapValidateEdits(
	edits []TabletAnchorMapEdit, maxFence *int,
) error {
	for rank, edit := range edits {
		if len(edit.Fence) == 0 || len(edit.Fence) > int(^uint16(0)) ||
			rank != 0 && bytes.Compare(edits[rank-1].Fence, edit.Fence) >= 0 ||
			edit.Operation < TabletAnchorMapInsert ||
			edit.Operation > TabletAnchorMapReplace ||
			edit.Operation != TabletAnchorMapDelete &&
				uint32(edit.Bucket) >= PrimaryBucketIDLimit {
			return fmt.Errorf(
				"%w: non-canonical anchor edit at rank %d",
				ErrInvalidWrite, rank,
			)
		}
		*maxFence = max(*maxFence, len(edit.Fence))
	}
	return nil
}

type tabletAnchorMapMerge struct {
	view              TabletAnchorMapView
	edits             []TabletAnchorMapEdit
	work              []byte
	oldRank, editRank int
}

func (m *tabletAnchorMapMerge) reset() {
	m.oldRank, m.editRank = 0, 0
}

func (m *tabletAnchorMapMerge) next() (fence []byte, bucket BucketID, ok bool, err error) {
	for m.oldRank < int(m.view.fenceCount) ||
		m.editRank < len(m.edits) {
		haveOld := m.oldRank < int(m.view.fenceCount)
		haveEdit := m.editRank < len(m.edits)
		comparison := -1
		if haveOld && haveEdit {
			common, prefix, suffix := m.view.fenceParts(m.oldRank)
			comparison = tabletAnchorMapCompareParts(
				[3][]byte{common, prefix, suffix},
				[3][]byte{m.edits[m.editRank].Fence, nil, nil},
			)
		} else if !haveOld {
			comparison = 1
		}

		switch {
		case comparison < 0:
			rank := m.oldRank
			m.oldRank++
			fence = m.view.materializeFence(rank, m.work)
			bucket = m.view.bucketAt(rank + 1)
		case comparison > 0:
			edit := m.edits[m.editRank]
			m.editRank++
			if edit.Operation != TabletAnchorMapInsert {
				return nil, 0, false, fmt.Errorf(
					"%w: edit fence does not exist", ErrInvalidWrite,
				)
			}
			fence, bucket = edit.Fence, edit.Bucket
		default:
			edit := m.edits[m.editRank]
			m.oldRank++
			m.editRank++
			switch edit.Operation {
			case TabletAnchorMapInsert:
				return nil, 0, false, fmt.Errorf(
					"%w: inserted fence already exists", ErrInvalidWrite,
				)
			case TabletAnchorMapDelete:
				continue
			case TabletAnchorMapReplace:
				fence, bucket = edit.Fence, edit.Bucket
			}
		}
		return fence, bucket, true, nil
	}
	return nil, 0, false, nil
}
