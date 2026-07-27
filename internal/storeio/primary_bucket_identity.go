package storeio

const (
	// PrimaryBucketIDLimit is the first value outside the durable 30-bit
	// primary-leaf identity namespace. The upper bits identify a tablet and
	// the lower bits identify a stable leaf inside that tablet.
	PrimaryBucketIDLimit = uint32(1 << 30)

	// The hybrid primary owns one collision-free logical-ID namespace. Fixed
	// identities make a BucketID, TabletID, anchor coordinate, or catalog node
	// independently reconstructible without storing another uint64 in every
	// routing row. Dynamically allocated overflow, index, free, and catalog
	// pages start only after the complete fixed namespace.
	PrimaryLeafLogicalIDBase  = uint64(StateRootLogicalID + 1)
	PrimaryLeafLogicalIDLimit = PrimaryLeafLogicalIDBase + uint64(PrimaryBucketIDLimit)

	PrimaryAnchorLogicalIDBase  = PrimaryLeafLogicalIDLimit
	PrimaryAnchorLogicalIDLimit = PrimaryAnchorLogicalIDBase + 1<<18*16

	// Tablet roots are retained only for the tracked catalog codec while the
	// fused route block replaces that wrapper in the production graph. Keeping
	// the temporary band explicit prevents its pages from colliding with the
	// selected locator and route identities during the cutover.
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

	PrimaryTabletDirectoryLogicalIDBase  = PrimaryTabletRouteLogicalIDLimit
	PrimaryTabletDirectoryLogicalIDLimit = PrimaryTabletDirectoryLogicalIDBase + 1<<20

	PrimaryFirstDynamicLogicalID = PrimaryTabletDirectoryLogicalIDLimit
)

// BucketID is the stable 30-bit identity carried by secondary posting tiles.
// It is logical: copy-on-write may move the leaf without changing this value.
type BucketID uint32

// BucketZone is the compact leaf summary carried by a primary router handle.
// Its interpretation belongs to the ordered-leaf layer.
type BucketZone [4]byte
