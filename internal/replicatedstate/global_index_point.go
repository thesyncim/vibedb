package replicatedstate

import "github.com/thesyncim/vibedb/distribution"

// Valid reports whether the complete global-index key/placement profile is
// supported. It does not authenticate the profile against a schema manifest.
func (p GlobalIndexProfile) Valid() bool {
	return p.IndexID != 0 && p.Incarnation != 0 && p.LocatorCount >= 1 && p.LocatorCount <= 8 &&
		p.KeyEncoding == GlobalIndexKeyCanonicalTuple && p.KeyArity >= 1 && p.KeyArity <= distribution.KeyspaceWidth &&
		p.TupleVersion == distribution.CurrentTupleVersion && p.MapperVersion == distribution.NativeMapperVersion &&
		distribution.ValidVirtualBucketBits(p.BucketBits)
}

// GlobalIndexStorageKeyPoint validates the complete stored key and maps only
// its leading index tuple. A non-unique index's appended locator is checked,
// never used for placement. The method borrows key and allocates no memory.
func (p GlobalIndexProfile) GlobalIndexStorageKeyPoint(key []byte) (distribution.KeyspacePoint, bool) {
	if !p.Valid() {
		return distribution.KeyspacePoint{}, false
	}
	point, consumed, ok := distribution.NativePointForEncodedTuplePrefix(key, int(p.KeyArity), p.BucketBits)
	if !ok {
		return distribution.KeyspacePoint{}, false
	}
	if p.Unique {
		return point, consumed == len(key)
	}
	locatorBytes, ok := distribution.CanonicalTuplePrefixLen(key[consumed:], int(p.LocatorCount))
	return point, ok && consumed+locatorBytes == len(key)
}
