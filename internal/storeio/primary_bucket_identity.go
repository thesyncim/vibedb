package storeio

const (
	TabletLocalIdentityTabletBits  = 18
	TabletLocalIdentityLocalBits   = 12
	TabletLocalIdentityLocalCount  = 1 << TabletLocalIdentityLocalBits
	TabletLocalIdentityTabletCount = 1 << TabletLocalIdentityTabletBits
	TabletLocalIdentityBucketCount = 1 << (TabletLocalIdentityTabletBits + TabletLocalIdentityLocalBits)

	// PrimaryBucketIDLimit is the first value outside the durable 30-bit
	// primary-leaf identity namespace. The upper bits identify a tablet and
	// the lower bits identify a stable leaf inside that tablet.
	PrimaryBucketIDLimit = uint32(1 << 30)

	// The hybrid primary owns one collision-free logical-ID namespace. Fixed
	// identities make a BucketID, TabletID, anchor coordinate, or catalog node
	// independently reconstructible without storing another uint64 in every
	// routing row. Dynamically allocated overflow, index, free, and catalog
	// pages start only after the complete fixed namespace.
	PrimaryLeafLogicalIDBase  = uint64(1)
	PrimaryLeafLogicalIDLimit = PrimaryLeafLogicalIDBase + uint64(PrimaryBucketIDLimit)

	PrimaryAnchorLogicalIDBase  = PrimaryLeafLogicalIDLimit
	PrimaryAnchorLogicalIDLimit = PrimaryAnchorLogicalIDBase + 1<<18*16

	PrimaryTabletRootLogicalIDBase  = PrimaryAnchorLogicalIDLimit
	PrimaryTabletRootLogicalIDLimit = PrimaryTabletRootLogicalIDBase + 1<<18

	PrimaryLocatorLogicalIDBase  = PrimaryTabletRootLogicalIDLimit
	PrimaryLocatorLogicalIDLimit = PrimaryLocatorLogicalIDBase + 1<<18

	PrimaryCatalogLeafLogicalIDBase  = PrimaryLocatorLogicalIDLimit
	PrimaryCatalogLeafLogicalIDLimit = PrimaryCatalogLeafLogicalIDBase + 1<<13

	PrimaryCatalogBranchLogicalIDBase  = PrimaryCatalogLeafLogicalIDLimit
	PrimaryCatalogBranchLogicalIDLimit = PrimaryCatalogBranchLogicalIDBase + 1<<9

	PrimaryCatalogRootLogicalID = PrimaryCatalogBranchLogicalIDLimit

	PrimaryTabletRouteLogicalIDBase  = PrimaryCatalogRootLogicalID + 1
	PrimaryTabletRouteLogicalIDLimit = PrimaryTabletRouteLogicalIDBase + 1<<20

	PrimaryFirstDynamicLogicalID = PrimaryTabletRouteLogicalIDLimit
)

// BucketID is the stable 30-bit identity carried by secondary posting tiles.
// It is logical: copy-on-write may move the leaf without changing this value.
type BucketID uint32

// BucketZone is the compact leaf summary carried by a primary router handle.
// Its interpretation belongs to the ordered-leaf layer.
type BucketZone [4]byte

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
