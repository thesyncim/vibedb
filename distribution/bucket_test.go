package distribution

import (
	"testing"
)

func TestNativeMapperBucketGoldenVectors(t *testing.T) {
	tests := []struct {
		name   string
		values []Scalar
		want   VirtualBucket
	}{
		{name: "tenant", values: []Scalar{NewString("tenant-42")}, want: 1_048_478},
		{name: "tenant-order", values: []Scalar{NewString("tenant-42"), NewString("order-0001")}, want: 335_763},
		{name: "binary-boundary", values: []Scalar{NewString("a\x00b"), NewString("c")}, want: 123_683},
		{name: "exact-number", values: []Scalar{bucketNumber(t, "50e-1"), NewString("row")}, want: 32_711},
	}
	for _, test := range tests {
		mapper := NewNativeMapper(len(test.values))
		point, err := mapper.PointFor(test.values)
		if err != nil {
			t.Errorf("%s: %v", test.name, err)
			continue
		}
		bucket, _ := VirtualBucketForPoint(point, mapper.VirtualBucketBits())
		if bucket != test.want {
			t.Errorf("%s: bucket=%d point=%x", test.name, bucket, point)
		}
	}

	left, err := NewNativeMapper(2).PointFor([]Scalar{bucketNumber(t, "5"), NewString("row")})
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewNativeMapper(2).PointFor([]Scalar{bucketNumber(t, "5.0"), NewString("row")})
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatalf("equal number spellings mapped to %x and %x", left, right)
	}
}

func TestVirtualBucketGeometry(t *testing.T) {
	for _, bits := range []uint8{MinVirtualBucketBits, DefaultVirtualBucketBits, MaxVirtualBucketBits} {
		count := VirtualBucketCount(bits)
		if count != uint32(1)<<bits {
			t.Fatalf("bits %d count = %d", bits, count)
		}
		first, ok := VirtualBucketRange(0, bits)
		if !ok || first.Start != (KeyspacePoint{}) || first.End.Max ||
			!VirtualBucketBoundary(first.End.Point, bits) {
			t.Fatalf("bits %d first range = %+v,%v", bits, first, ok)
		}
		last, ok := VirtualBucketRange(VirtualBucket(count-1), bits)
		if !ok || !last.End.Max || !VirtualBucketBoundary(last.Start, bits) {
			t.Fatalf("bits %d last range = %+v,%v", bits, last, ok)
		}
		for _, bucket := range []VirtualBucket{0, 1, VirtualBucket(count / 2), VirtualBucket(count - 1)} {
			range_, _ := VirtualBucketRange(bucket, bits)
			got, valid := VirtualBucketForPoint(range_.Start, bits)
			if !valid || got != bucket {
				t.Fatalf("bits %d bucket %d recovered %d,%v", bits, bucket, got, valid)
			}
		}
	}

	for _, bits := range []uint8{0, MinVirtualBucketBits - 1, MaxVirtualBucketBits + 1, 64} {
		if VirtualBucketCount(bits) != 0 {
			t.Fatalf("invalid bits %d returned a count", bits)
		}
		if _, _, ok := VirtualBucketForHash(1, bits); ok {
			t.Fatalf("invalid bits %d accepted a hash", bits)
		}
	}
}

func TestManifestVirtualBucketIntervalsAndLookup(t *testing.T) {
	var middle KeyspacePoint
	middle[0] = 0x80
	manifest, err := NewManifest("d", 7, []Shard{
		{ID: "left", AllocationGeneration: 1, Range: KeyRange{End: KeyspaceEnd{Point: middle}}, Leaders: []EndpointID{"a"}, Epoch: 3},
		{ID: "right", AllocationGeneration: 2, Range: KeyRange{Start: middle, End: KeyspaceEnd{Max: true}}, Leaders: []EndpointID{"b"}, Epoch: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	bits := DefaultVirtualBucketBits
	left, ok := manifest.ShardBucketInterval(0, bits)
	if !ok || left != (BucketInterval{Start: 0, End: 1 << (bits - 1)}) {
		t.Fatalf("left bucket interval = %+v,%v", left, ok)
	}
	right, ok := manifest.ShardBucketInterval(1, bits)
	if !ok || right != (BucketInterval{Start: 1 << (bits - 1), End: 1 << bits}) {
		t.Fatalf("right bucket interval = %+v,%v", right, ok)
	}
	target, ok := manifest.ResolveVirtualBucket(VirtualBucket(right.Start), bits)
	if !ok || target.Shard != "right" || target.AllocationGeneration != 2 || target.OwnershipEpoch != 4 {
		t.Fatalf("right bucket target = %+v,%v", target, ok)
	}
	if _, ok := manifest.ResolveVirtualBucket(VirtualBucket(1<<bits), bits); ok {
		t.Fatal("out-of-range bucket resolved")
	}
}

func TestNativeMapperSpreadsOneTenantAcrossBuckets(t *testing.T) {
	mapper := NewNativeMapper(2)
	const rows = 1024
	seen := make(map[VirtualBucket]struct{}, rows)
	for i := 0; i < rows; i++ {
		point, err := mapper.PointFor([]Scalar{NewString("hot-tenant"), NewString(bucketKey(i))})
		if err != nil {
			t.Fatal(err)
		}
		if !VirtualBucketBoundary(point, mapper.VirtualBucketBits()) {
			t.Fatalf("point %x is not bucket aligned", point)
		}
		bucket, ok := VirtualBucketForPoint(point, mapper.VirtualBucketBits())
		if !ok {
			t.Fatal("mapped point has no bucket")
		}
		seen[bucket] = struct{}{}
	}
	// With 1,048,576 buckets, 1024 deterministic keys should have only a tiny
	// number of collisions. Keep generous headroom while detecting a mapper that
	// accidentally hashes tenant alone.
	if len(seen) < 1000 {
		t.Fatalf("one tenant occupied only %d/%d virtual buckets", len(seen), rows)
	}

	prefix, err := mapper.PrefixRangeFor([]Scalar{NewString("hot-tenant")})
	if err != nil {
		t.Fatal(err)
	}
	if prefix.Start != (KeyspacePoint{}) || !prefix.End.Max {
		t.Fatalf("tenant-only prefix = %+v, want honest full-keyspace range", prefix)
	}
}

func TestNativeMapperTypicalCompositeKeyAllocations(t *testing.T) {
	mapper := NewNativeMapper(2)
	values := []Scalar{NewString("tenant-42"), NewString("order-0000000001")}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := mapper.PointFor(values); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("PointFor allocations = %v, want 0", allocs)
	}
}

func BenchmarkNativeMapperCompositeBucket(b *testing.B) {
	mapper := NewNativeMapper(2)
	values := []Scalar{NewString("tenant-42"), NewString("order-0000000001")}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := mapper.PointFor(values); err != nil {
			b.Fatal(err)
		}
	}
}

func bucketKey(value int) string {
	const hex = "0123456789abcdef"
	var storage [8]byte
	for i := len(storage) - 1; i >= 0; i-- {
		storage[i] = hex[value&15]
		value >>= 4
	}
	return string(storage[:])
}

func bucketNumber(t testing.TB, spelling string) Scalar {
	t.Helper()
	value, err := NewNumber(spelling)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
