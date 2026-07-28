package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// The tablet-local identity experiment splits the 30-bit posting identity into
// an 18-bit macro-tablet and a 12-bit stable leaf identity. The dense inverse
// map is exactly 4096 uint16 values (8 KiB), so a posting can resolve its leaf
// without a radix descent or allocation.
//
// A live uint16 is a 12-bit stable anchor location: four bits of anchor-page ID
// and eight bits of row slot. FFFE is retired and FFFF is empty. Retirement
// generations deliberately do not use truncated bits in the uint16. They live
// in a sorted, root-bound exact-generation queue covered by the same checksum.
// This keeps snapshot lifetime unbounded while preserving the hot 8-KiB array.
const (
	TabletLocalIdentityTabletBits  = 18
	TabletLocalIdentityLocalBits   = 12
	TabletLocalIdentityImageBytes  = 1 << (TabletLocalIdentityLocalBits + 1)
	TabletLocalIdentityLocalCount  = 1 << TabletLocalIdentityLocalBits
	TabletLocalIdentityTabletCount = 1 << TabletLocalIdentityTabletBits
	TabletLocalIdentityBucketCount = 1 << (TabletLocalIdentityTabletBits + TabletLocalIdentityLocalBits)
	// DescriptorBytes is the canonical packed root charge; Go struct padding
	// is not part of the format. Each outstanding retirement adds ten bytes.
	TabletLocalIdentityDescriptorBytes = 36
	TabletLocalIdentityRetirementBytes = 10
	// A snapshot that pins every formerly live LocalID can retain one exact
	// retirement record per dense slot. This is a conservative logical-ID
	// queue bound, separate from physical extent reclamation.
	TabletLocalIdentityMaxRetirementBytes = TabletLocalIdentityLocalCount *
		TabletLocalIdentityRetirementBytes
	TabletLocalIdentityMaxEnvelopeBytes = TabletLocalIdentityImageBytes +
		TabletLocalIdentityDescriptorBytes +
		TabletLocalIdentityMaxRetirementBytes

	TabletLocalIdentityAnchorPageBits = 4
	TabletLocalIdentityRowSlotBits    = 8
	TabletLocalIdentityAnchorPages    = 1 << TabletLocalIdentityAnchorPageBits
	TabletLocalIdentityRowsPerPage    = 1 << TabletLocalIdentityRowSlotBits

	tabletLocalIdentityVersion = uint32(1)
	tabletLocalIdentityRetired = uint16(0xfffe)
	tabletLocalIdentityEmpty   = uint16(0xffff)
	tabletLocalIdentityLiveEnd = uint16(1 << TabletLocalIdentityLocalBits)
)

var (
	// ErrTabletLocalIdentityCorrupt reports that an identity image failed
	// checksum or structural admission and must not be read.
	ErrTabletLocalIdentityCorrupt = errors.New(
		"vibejson: corrupt tablet-local identity image",
	)
	// ErrTabletLocalIdentityScratch reports that a retirement batch was handed a
	// scratch buffer too small to stage its work.
	ErrTabletLocalIdentityScratch = errors.New(
		"vibejson: tablet-local identity retirement scratch is too small",
	)
	// ErrTabletLocalIdentityReuse reports that a LocalID cannot be reassigned
	// yet because its retirement is still reachable by the selecting snapshot.
	ErrTabletLocalIdentityReuse = errors.New(
		"vibejson: tablet-local identity is not safe to reuse",
	)
)

// TabletLocalIdentityState is the lifecycle state of one LocalID slot in a
// tablet's local-identity image.
type TabletLocalIdentityState uint8

const (
	// TabletLocalIdentityEmpty is a LocalID that has never been assigned.
	TabletLocalIdentityEmpty TabletLocalIdentityState = iota
	// TabletLocalIdentityLive is a LocalID bound to a current anchor location.
	TabletLocalIdentityLive
	// TabletLocalIdentityRetired is a LocalID whose binding has been dropped;
	// its slot is not reusable until its retirement generation clears ReuseFloor.
	TabletLocalIdentityRetired
)

// TabletLocalIdentityLocation is stable within the tablet anchor layer.
// Physical PageRefs remain outside this image and may change on every COW.
type TabletLocalIdentityLocation struct {
	AnchorPageID uint8
	RowSlot      uint8
}

// TabletLocalIdentityAssignment seeds one live LocalID. Assignments must be
// in strictly increasing LocalID order.
type TabletLocalIdentityAssignment struct {
	LocalID  uint16
	Location TabletLocalIdentityLocation
}

// TabletLocalIdentityRetirement is the exact generation at which LocalID's
// previous meaning was last reachable. It is safe to recycle only when
// RetiredGeneration < the active snapshot floor. Records are sorted by LocalID
// and encoded as ten bytes when persisted by the owning tablet root.
type TabletLocalIdentityRetirement struct {
	LocalID           uint16
	RetiredGeneration uint64
}

// TabletLocalIdentityDescriptor belongs in the checksummed tablet root next
// to the raw 8-KiB image reference and exact retirement queue. Checksum binds
// all descriptor fields, the complete dense image, and every retirement record.
// The struct itself is pointer-free.
type TabletLocalIdentityDescriptor struct {
	TabletID           uint32
	Format             uint32
	Generation         uint64
	ReuseFloor         uint64
	LiveCount          uint16
	RetiredCount       uint16
	Checksum           uint32
	ChecksumComplement uint32
}

// TabletLocalIdentityView borrows a checksum-admitted dense image and its
// root-bound retirement queue. Resolve and live iteration touch only the image.
type TabletLocalIdentityView struct {
	image       []byte
	retirements []TabletLocalIdentityRetirement
	descriptor  TabletLocalIdentityDescriptor
}

// TabletLocalIdentityOperation names the structural edit one batch applies to a
// LocalID slot.
type TabletLocalIdentityOperation uint8

const (
	// TabletLocalIdentityAssign binds a fresh or safely reusable LocalID to an
	// anchor location.
	TabletLocalIdentityAssign TabletLocalIdentityOperation = iota + 1
	// TabletLocalIdentityMove preserves a LocalID's identity while changing the
	// anchor row it points at.
	TabletLocalIdentityMove
	// TabletLocalIdentityRetire drops a LocalID's binding, moving it to the
	// retired state.
	TabletLocalIdentityRetire
)

// TabletLocalIdentityEdit performs one simultaneous COW batch operation.
// Assign accepts an ID that was never used or whose exact retirement generation
// is below ReuseFloor. Move preserves identity while changing its anchor row.
type TabletLocalIdentityEdit struct {
	LocalID   uint16
	Operation TabletLocalIdentityOperation
	Location  TabletLocalIdentityLocation
}

// TabletLocalIdentityCursor scans live mappings in LocalID order without
// allocating. Retired and empty identities are skipped.
type TabletLocalIdentityCursor struct {
	image []byte
	next  uint16
}

// MakeTabletLocalIdentityBucket combines an 18-bit tablet and 12-bit local
// identity. The largest valid pair produces the largest 30-bit BucketID.
func MakeTabletLocalIdentityBucket(
	tabletID uint32, localID uint32,
) (uint32, bool) {
	if tabletID >= TabletLocalIdentityTabletCount ||
		localID >= TabletLocalIdentityLocalCount {
		return 0, false
	}
	return tabletID<<TabletLocalIdentityLocalBits | localID, true
}

func SplitTabletLocalIdentityBucket(
	bucketID uint32,
) (tabletID uint32, localID uint16, ok bool) {
	if bucketID >= TabletLocalIdentityBucketCount {
		return 0, 0, false
	}
	return bucketID >> TabletLocalIdentityLocalBits,
		uint16(bucketID & (TabletLocalIdentityLocalCount - 1)), true
}

// TabletLocalIdentityDocumentCapacity is the exact namespace capacity at a
// chosen average rows per leaf. A zero density is invalid.
func TabletLocalIdentityDocumentCapacity(rowsPerLeaf uint64) (uint64, bool) {
	if rowsPerLeaf == 0 ||
		rowsPerLeaf > ^uint64(0)/TabletLocalIdentityBucketCount {
		return 0, false
	}
	return rowsPerLeaf * TabletLocalIdentityBucketCount, true
}

// EncodeTabletLocalIdentity creates an initial exact 8-KiB dense image.
// dst is caller-owned and assignments must be canonical and location-unique.
func EncodeTabletLocalIdentity(
	dst []byte,
	tabletID uint32,
	generation uint64,
	reuseFloor uint64,
	assignments []TabletLocalIdentityAssignment,
) (
	[]byte,
	TabletLocalIdentityDescriptor,
	error,
) {
	if len(dst) < TabletLocalIdentityImageBytes ||
		tabletID >= TabletLocalIdentityTabletCount ||
		generation == 0 ||
		!tabletLocalIdentityValidFloor(generation, reuseFloor) ||
		len(assignments) > TabletLocalIdentityLocalCount {
		return nil, TabletLocalIdentityDescriptor{},
			fmt.Errorf("%w: initial identity or geometry", ErrInvalidWrite)
	}
	image := dst[:TabletLocalIdentityImageBytes]
	for at := range image {
		image[at] = 0xff
	}
	var occupied [TabletLocalIdentityLocalCount / 64]uint64
	var previous uint16
	for rank, assignment := range assignments {
		if uint32(assignment.LocalID) >= TabletLocalIdentityLocalCount ||
			rank != 0 && assignment.LocalID <= previous {
			return nil, TabletLocalIdentityDescriptor{},
				fmt.Errorf("%w: non-canonical assignment order", ErrInvalidWrite)
		}
		code, ok := tabletLocalIdentityEncodeLocation(assignment.Location)
		if !ok || tabletLocalIdentityLocationSeen(&occupied, code) {
			return nil, TabletLocalIdentityDescriptor{},
				fmt.Errorf("%w: invalid or duplicate anchor location", ErrInvalidWrite)
		}
		binary.LittleEndian.PutUint16(
			image[int(assignment.LocalID)*2:], code,
		)
		previous = assignment.LocalID
	}
	descriptor := TabletLocalIdentityDescriptor{
		TabletID:   tabletID,
		Format:     tabletLocalIdentityVersion,
		Generation: generation,
		ReuseFloor: reuseFloor,
		LiveCount:  uint16(len(assignments)),
	}
	tabletLocalIdentitySeal(&descriptor, image, nil)
	return image, descriptor, nil
}

// OpenTabletLocalIdentity verifies the root binding, checksum, state
// encoding, live-location uniqueness, retirement ordering, and exact state to
// retirement-record correspondence. It allocates nothing on success or error.
func OpenTabletLocalIdentity(
	image []byte,
	descriptor TabletLocalIdentityDescriptor,
	retirements []TabletLocalIdentityRetirement,
) (TabletLocalIdentityView, error) {
	if len(image) != TabletLocalIdentityImageBytes ||
		descriptor.Format != tabletLocalIdentityVersion ||
		descriptor.TabletID >= TabletLocalIdentityTabletCount ||
		descriptor.Generation == 0 ||
		!tabletLocalIdentityValidFloor(
			descriptor.Generation, descriptor.ReuseFloor,
		) ||
		int(descriptor.RetiredCount) != len(retirements) ||
		descriptor.ChecksumComplement != ^descriptor.Checksum ||
		tabletLocalIdentityChecksum(
			descriptor, image, retirements,
		) != descriptor.Checksum {
		return TabletLocalIdentityView{},
			fmt.Errorf("%w: descriptor or checksum", ErrTabletLocalIdentityCorrupt)
	}

	var occupied [TabletLocalIdentityLocalCount / 64]uint64
	live := 0
	retired := 0
	retirementAt := 0
	for localID := range TabletLocalIdentityLocalCount {
		code := binary.LittleEndian.Uint16(image[localID*2:])
		switch {
		case code < tabletLocalIdentityLiveEnd:
			if tabletLocalIdentityLocationSeen(&occupied, code) {
				return TabletLocalIdentityView{},
					fmt.Errorf("%w: duplicate live location", ErrTabletLocalIdentityCorrupt)
			}
			live++
		case code == tabletLocalIdentityRetired:
			if retirementAt >= len(retirements) ||
				int(retirements[retirementAt].LocalID) != localID ||
				retirements[retirementAt].RetiredGeneration == 0 ||
				retirements[retirementAt].RetiredGeneration >
					descriptor.Generation ||
				retirements[retirementAt].RetiredGeneration <
					descriptor.ReuseFloor {
				return TabletLocalIdentityView{},
					fmt.Errorf("%w: retirement binding or floor", ErrTabletLocalIdentityCorrupt)
			}
			retirementAt++
			retired++
		case code == tabletLocalIdentityEmpty:
		default:
			return TabletLocalIdentityView{},
				fmt.Errorf("%w: reserved locator code", ErrTabletLocalIdentityCorrupt)
		}
	}
	if live != int(descriptor.LiveCount) ||
		retired != int(descriptor.RetiredCount) ||
		retirementAt != len(retirements) {
		return TabletLocalIdentityView{},
			fmt.Errorf("%w: state cardinality", ErrTabletLocalIdentityCorrupt)
	}
	return TabletLocalIdentityView{
		image:       image,
		retirements: retirements,
		descriptor:  descriptor,
	}, nil
}

func (v TabletLocalIdentityView) Descriptor() TabletLocalIdentityDescriptor {
	return v.descriptor
}

func (v TabletLocalIdentityView) PersistentBytes() []byte {
	return v.image
}

// Resolve is the posting-driven hot path. It performs one bounds check, one
// uint16 load, and two shifts; empty and retired IDs fail closed.
func (v TabletLocalIdentityView) Resolve(
	localID uint16,
) (TabletLocalIdentityLocation, bool) {
	if uint32(localID) >= TabletLocalIdentityLocalCount {
		return TabletLocalIdentityLocation{}, false
	}
	code := binary.LittleEndian.Uint16(v.image[int(localID)*2:])
	if code >= tabletLocalIdentityLiveEnd {
		return TabletLocalIdentityLocation{}, false
	}
	return tabletLocalIdentityDecodeLocation(code), true
}

// Entry exposes all three states. Retirement lookup is binary search over the
// root-bound exact-generation queue; the live Resolve path never pays for it.
func (v TabletLocalIdentityView) Entry(
	localID uint16,
) (
	TabletLocalIdentityState,
	TabletLocalIdentityLocation,
	uint64,
	bool,
) {
	if uint32(localID) >= TabletLocalIdentityLocalCount {
		return TabletLocalIdentityEmpty,
			TabletLocalIdentityLocation{}, 0, false
	}
	code := binary.LittleEndian.Uint16(v.image[int(localID)*2:])
	switch {
	case code < tabletLocalIdentityLiveEnd:
		return TabletLocalIdentityLive,
			tabletLocalIdentityDecodeLocation(code), 0, true
	case code == tabletLocalIdentityEmpty:
		return TabletLocalIdentityEmpty,
			TabletLocalIdentityLocation{}, 0, true
	case code == tabletLocalIdentityRetired:
		at := tabletLocalIdentityFindRetirement(v.retirements, localID)
		if at < len(v.retirements) && v.retirements[at].LocalID == localID {
			return TabletLocalIdentityRetired,
				TabletLocalIdentityLocation{},
				v.retirements[at].RetiredGeneration, true
		}
	}
	return TabletLocalIdentityEmpty,
		TabletLocalIdentityLocation{}, 0, false
}

func (v TabletLocalIdentityView) Cursor() TabletLocalIdentityCursor {
	return TabletLocalIdentityCursor{image: v.image}
}

func (c *TabletLocalIdentityCursor) Next() (
	uint16,
	TabletLocalIdentityLocation,
	bool,
) {
	for uint32(c.next) < TabletLocalIdentityLocalCount {
		localID := c.next
		c.next++
		code := binary.LittleEndian.Uint16(c.image[int(localID)*2:])
		if code < tabletLocalIdentityLiveEnd {
			return localID, tabletLocalIdentityDecodeLocation(code), true
		}
	}
	return 0, TabletLocalIdentityLocation{}, false
}

// RewriteTabletLocalIdentity applies one simultaneous COW batch. The
// snapshot floor is monotonic. Eligible retirements become empty automatically;
// a Retire records base.Generation, the last root that could reach the old
// meaning. retireDst is caller-owned exact-generation scratch.
// retireDst must not alias base.retirements.
//
// Correct identity reuse depends on one publication invariant: StateRoot must
// atomically select this descriptor/image, every anchor root, and every exact
// index root. A background index builder may carry a BucketID across roots only
// while pinning the source generation; otherwise it must re-resolve under the
// destination root. The conservative retirement floor protects that boundary.
//
// Ordinary root-bound snapshots do not intrinsically require this logical-ID
// queue: an old snapshot resolves a reused number through its old locator and
// old index roots, while the new root resolves it through the new set. Production
// can reclaim LocalIDs immediately—even with old snapshots—if and only if:
//
//   - one failure-atomic StateRoot selects locator, anchors, and every index;
//   - every cache key includes the physical page generation;
//   - no background build, cursor, posting batch, or change feed carries a bare
//     BucketID across roots (it pins the source generation or re-resolves);
//   - recovery never assembles component roots from different generations.
//
// Under those invariants the exact queue can disappear. Physical PageRefs and
// extents still require the existing snapshot-floor reclaimer; only numeric
// LocalID reuse becomes immediate.
func RewriteTabletLocalIdentity(
	dst []byte,
	retireDst []TabletLocalIdentityRetirement,
	base TabletLocalIdentityView,
	generation uint64,
	reuseFloor uint64,
	edits []TabletLocalIdentityEdit,
) (
	[]byte,
	TabletLocalIdentityDescriptor,
	[]TabletLocalIdentityRetirement,
	error,
) {
	if len(dst) < TabletLocalIdentityImageBytes ||
		len(base.image) != TabletLocalIdentityImageBytes ||
		generation <= base.descriptor.Generation ||
		reuseFloor < base.descriptor.ReuseFloor ||
		// The lease floor describes the currently published base root, not
		// the generation being prepared (which may legitimately skip).
		!tabletLocalIdentityValidFloor(
			base.descriptor.Generation, reuseFloor,
		) ||
		len(edits) > TabletLocalIdentityLocalCount {
		return nil, TabletLocalIdentityDescriptor{}, retireDst[:0],
			fmt.Errorf("%w: rewrite identity or geometry", ErrInvalidWrite)
	}
	for rank, edit := range edits {
		if uint32(edit.LocalID) >= TabletLocalIdentityLocalCount ||
			rank != 0 && edit.LocalID <= edits[rank-1].LocalID ||
			edit.Operation < TabletLocalIdentityAssign ||
			edit.Operation > TabletLocalIdentityRetire {
			return nil, TabletLocalIdentityDescriptor{}, retireDst[:0],
				fmt.Errorf("%w: non-canonical edit", ErrInvalidWrite)
		}
		if edit.Operation != TabletLocalIdentityRetire {
			if _, ok := tabletLocalIdentityEncodeLocation(edit.Location); !ok {
				return nil, TabletLocalIdentityDescriptor{}, retireDst[:0],
					fmt.Errorf("%w: edit anchor location", ErrInvalidWrite)
			}
		}
	}

	image := dst[:TabletLocalIdentityImageBytes]
	copy(image, base.image)
	retirements := retireDst[:0]
	var occupied [TabletLocalIdentityLocalCount / 64]uint64
	editAt := 0
	retirementAt := 0
	liveCount := 0
	for local := range TabletLocalIdentityLocalCount {
		localID := uint16(local)
		code := binary.LittleEndian.Uint16(base.image[local*2:])
		state := TabletLocalIdentityEmpty
		var location TabletLocalIdentityLocation
		var retiredGeneration uint64
		switch {
		case code < tabletLocalIdentityLiveEnd:
			state = TabletLocalIdentityLive
			location = tabletLocalIdentityDecodeLocation(code)
		case code == tabletLocalIdentityRetired:
			if retirementAt >= len(base.retirements) ||
				base.retirements[retirementAt].LocalID != localID {
				return nil, TabletLocalIdentityDescriptor{}, retirements,
					fmt.Errorf("%w: base retirement binding", ErrTabletLocalIdentityCorrupt)
			}
			retiredGeneration =
				base.retirements[retirementAt].RetiredGeneration
			retirementAt++
			if retiredGeneration >= reuseFloor {
				state = TabletLocalIdentityRetired
			}
		case code == tabletLocalIdentityEmpty:
		default:
			return nil, TabletLocalIdentityDescriptor{}, retirements,
				fmt.Errorf("%w: base locator code", ErrTabletLocalIdentityCorrupt)
		}

		if editAt < len(edits) && edits[editAt].LocalID == localID {
			edit := edits[editAt]
			editAt++
			switch edit.Operation {
			case TabletLocalIdentityAssign:
				if state != TabletLocalIdentityEmpty {
					return nil, TabletLocalIdentityDescriptor{}, retirements,
						fmt.Errorf("%w: LocalID %d", ErrTabletLocalIdentityReuse, localID)
				}
				state = TabletLocalIdentityLive
				location = edit.Location
			case TabletLocalIdentityMove:
				if state != TabletLocalIdentityLive {
					return nil, TabletLocalIdentityDescriptor{}, retirements,
						fmt.Errorf("%w: move of non-live LocalID %d", ErrInvalidWrite, localID)
				}
				location = edit.Location
			case TabletLocalIdentityRetire:
				if state != TabletLocalIdentityLive {
					return nil, TabletLocalIdentityDescriptor{}, retirements,
						fmt.Errorf("%w: retirement of non-live LocalID %d", ErrInvalidWrite, localID)
				}
				if base.descriptor.Generation < reuseFloor {
					state = TabletLocalIdentityEmpty
				} else {
					state = TabletLocalIdentityRetired
					retiredGeneration = base.descriptor.Generation
				}
			}
		}

		switch state {
		case TabletLocalIdentityEmpty:
			binary.LittleEndian.PutUint16(image[local*2:], tabletLocalIdentityEmpty)
		case TabletLocalIdentityLive:
			liveCode, _ := tabletLocalIdentityEncodeLocation(location)
			if tabletLocalIdentityLocationSeen(&occupied, liveCode) {
				return nil, TabletLocalIdentityDescriptor{}, retirements,
					fmt.Errorf("%w: duplicate post-edit anchor location", ErrInvalidWrite)
			}
			binary.LittleEndian.PutUint16(image[local*2:], liveCode)
			liveCount++
		case TabletLocalIdentityRetired:
			if len(retirements) == cap(retirements) {
				return nil, TabletLocalIdentityDescriptor{}, retirements,
					ErrTabletLocalIdentityScratch
			}
			retirements = append(retirements, TabletLocalIdentityRetirement{
				LocalID: localID, RetiredGeneration: retiredGeneration,
			})
			binary.LittleEndian.PutUint16(image[local*2:], tabletLocalIdentityRetired)
		}
	}
	if editAt != len(edits) || retirementAt != len(base.retirements) {
		return nil, TabletLocalIdentityDescriptor{}, retirements,
			fmt.Errorf("%w: batch merge cardinality", ErrInvalidWrite)
	}
	descriptor := TabletLocalIdentityDescriptor{
		TabletID:     base.descriptor.TabletID,
		Format:       tabletLocalIdentityVersion,
		Generation:   generation,
		ReuseFloor:   reuseFloor,
		LiveCount:    uint16(liveCount),
		RetiredCount: uint16(len(retirements)),
	}
	tabletLocalIdentitySeal(&descriptor, image, retirements)
	return image, descriptor, retirements, nil
}

// TabletLocalIdentityBytesPerLive reports the dense locator charge. The
// root descriptor and ten-byte exact retirement records are reported
// separately because they are shared/temporary rather than charged per live ID.
func TabletLocalIdentityBytesPerLive(live int) float64 {
	if live <= 0 {
		return 0
	}
	return float64(TabletLocalIdentityImageBytes) / float64(live)
}

// TabletLocalIdentityEnvelopeBytes reports the exact packed durable charge
// for the root-bound descriptor, dense locator, and outstanding retirement
// records. Go slice/struct padding and physical allocator rounding are excluded.
func TabletLocalIdentityEnvelopeBytes(retired int) (int, bool) {
	if retired < 0 || retired > TabletLocalIdentityLocalCount {
		return 0, false
	}
	return TabletLocalIdentityImageBytes +
		TabletLocalIdentityDescriptorBytes +
		retired*TabletLocalIdentityRetirementBytes, true
}

func tabletLocalIdentityEncodeLocation(
	location TabletLocalIdentityLocation,
) (uint16, bool) {
	if location.AnchorPageID >= TabletLocalIdentityAnchorPages {
		return 0, false
	}
	return uint16(location.AnchorPageID)<<TabletLocalIdentityRowSlotBits |
		uint16(location.RowSlot), true
}

func tabletLocalIdentityDecodeLocation(
	code uint16,
) TabletLocalIdentityLocation {
	return TabletLocalIdentityLocation{
		AnchorPageID: uint8(code >> TabletLocalIdentityRowSlotBits),
		RowSlot:      uint8(code),
	}
}

func tabletLocalIdentityLocationSeen(
	occupied *[TabletLocalIdentityLocalCount / 64]uint64,
	code uint16,
) bool {
	word := code >> 6
	bit := uint64(1) << (code & 63)
	seen := occupied[word]&bit != 0
	occupied[word] |= bit
	return seen
}

func tabletLocalIdentityFindRetirement(
	retirements []TabletLocalIdentityRetirement,
	localID uint16,
) int {
	low, high := 0, len(retirements)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if retirements[middle].LocalID < localID {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func tabletLocalIdentityValidFloor(
	generation, reuseFloor uint64,
) bool {
	if generation == 0 || reuseFloor == 0 {
		return false
	}
	// With no active snapshot, the generation lease layer reports current+1.
	// Spell that without overflowing the last representable generation.
	return reuseFloor <= generation ||
		generation != ^uint64(0) && reuseFloor == generation+1
}

func tabletLocalIdentitySeal(
	descriptor *TabletLocalIdentityDescriptor,
	image []byte,
	retirements []TabletLocalIdentityRetirement,
) {
	descriptor.Checksum = tabletLocalIdentityChecksum(
		*descriptor, image, retirements,
	)
	descriptor.ChecksumComplement = ^descriptor.Checksum
}

func tabletLocalIdentityChecksum(
	descriptor TabletLocalIdentityDescriptor,
	image []byte,
	retirements []TabletLocalIdentityRetirement,
) uint32 {
	// CRC the large resident slice first, then append the canonical descriptor
	// and queue fields with scalar table updates. Small stack byte arrays passed
	// to hash/crc32 escape today; spelling these updates out keeps COW batches
	// allocation-free without changing the CRC32C polynomial.
	checksum := PageChecksum(image)
	checksum = tabletLocalIdentityCRC64(checksum, uint64(0x313042414c494c54)) // "TLILAB01"
	checksum = tabletLocalIdentityCRC32(checksum, descriptor.Format)
	checksum = tabletLocalIdentityCRC32(
		checksum, TabletLocalIdentityImageBytes,
	)
	checksum = tabletLocalIdentityCRC32(checksum, descriptor.TabletID)
	checksum = tabletLocalIdentityCRC32(checksum,
		TabletLocalIdentityTabletBits|
			TabletLocalIdentityLocalBits<<8|
			TabletLocalIdentityAnchorPageBits<<16|
			TabletLocalIdentityRowSlotBits<<24,
	)
	checksum = tabletLocalIdentityCRC64(checksum, descriptor.Generation)
	checksum = tabletLocalIdentityCRC64(checksum, descriptor.ReuseFloor)
	checksum = tabletLocalIdentityCRC16(checksum, descriptor.LiveCount)
	checksum = tabletLocalIdentityCRC16(checksum, descriptor.RetiredCount)
	for _, retirement := range retirements {
		checksum = tabletLocalIdentityCRC16(
			checksum, retirement.LocalID,
		)
		checksum = tabletLocalIdentityCRC64(
			checksum, retirement.RetiredGeneration,
		)
	}
	return checksum
}

func tabletLocalIdentityCRCByte(checksum uint32, value byte) uint32 {
	current := ^checksum
	current = pageChecksumTable[byte(current)^value] ^ current>>8
	return ^current
}

func tabletLocalIdentityCRC16(checksum uint32, value uint16) uint32 {
	checksum = tabletLocalIdentityCRCByte(checksum, byte(value))
	return tabletLocalIdentityCRCByte(checksum, byte(value>>8))
}

func tabletLocalIdentityCRC32(checksum uint32, value uint32) uint32 {
	checksum = tabletLocalIdentityCRC16(checksum, uint16(value))
	return tabletLocalIdentityCRC16(checksum, uint16(value>>16))
}

func tabletLocalIdentityCRC64(checksum uint32, value uint64) uint32 {
	checksum = tabletLocalIdentityCRC32(checksum, uint32(value))
	return tabletLocalIdentityCRC32(checksum, uint32(value>>32))
}
