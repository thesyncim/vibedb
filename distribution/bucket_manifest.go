package distribution

// BucketInterval is a half-open logical bucket run [Start, End). End may equal
// VirtualBucketCount(bits), which represents the end of the final bucket.
// uint32 is sufficient because the admitted bucket space is at most 24 bits.
type BucketInterval struct {
	Start uint32
	End   uint32
}

// ShardBucketInterval returns the exact virtual-bucket run owned by shard i.
// It reports false for an invalid width, ordinal, or manifest whose physical
// boundary cuts through a bucket. Catalog validation requires alignment for
// every distribution that states BucketBits explicitly.
func (m *Manifest) ShardBucketInterval(i int, bits uint8) (BucketInterval, bool) {
	if m == nil || i < 0 || i >= len(m.shards) || !ValidVirtualBucketBits(bits) {
		return BucketInterval{}, false
	}
	shard := &m.shards[i]
	if !VirtualBucketBoundary(shard.Range.Start, bits) ||
		(!shard.Range.End.Max && !VirtualBucketBoundary(shard.Range.End.Point, bits)) {
		return BucketInterval{}, false
	}
	start, _ := VirtualBucketForPoint(shard.Range.Start, bits)
	end := VirtualBucketCount(bits)
	if !shard.Range.End.Max {
		bucket, _ := VirtualBucketForPoint(shard.Range.End.Point, bits)
		end = uint32(bucket)
	}
	if uint32(start) >= end {
		return BucketInterval{}, false
	}
	return BucketInterval{Start: uint32(start), End: end}, true
}

// ResolveVirtualBucket returns the fenced leader target owning bucket. It is a
// direct O(log shard_count) lookup with no per-bucket directory and no
// allocation.
func (m *Manifest) ResolveVirtualBucket(bucket VirtualBucket, bits uint8) (Target, bool) {
	range_, ok := VirtualBucketRange(bucket, bits)
	if !ok {
		return Target{}, false
	}
	return m.ResolvePointTarget(range_.Start)
}
