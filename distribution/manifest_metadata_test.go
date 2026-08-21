package distribution

import "testing"

var (
	manifestMetadataSink ShardMetadata
	manifestBoolSink     bool
)

func manifestForMetadataTest(
	tb testing.TB,
	distribution DistributionName,
	version RoutingVersion,
	shards []Shard,
) *Manifest {
	tb.Helper()
	manifest, err := NewManifest(distribution, version, shards)
	if err != nil {
		tb.Fatalf("NewManifest: %v", err)
	}
	return manifest
}

func metadataTestShards() []Shard {
	mid := pt(hb(0x80))
	return []Shard{
		{
			ID: "s0", AllocationGeneration: 1,
			Range:   KeyRange{Start: KeyspacePoint{}, End: KeyspaceEnd{Point: mid}},
			Leaders: []EndpointID{"ep-0a", "ep-0b"}, Epoch: 11,
		},
		{
			ID: "s1", AllocationGeneration: 2, Range: KeyRange{Start: mid, End: maxKeyEnd},
			Leaders: []EndpointID{"ep-1a", "ep-1b"}, Epoch: 22,
		},
	}
}

func cloneMetadataTestShards() []Shard {
	shards := metadataTestShards()
	for i := range shards {
		shards[i].Leaders = append([]EndpointID(nil), shards[i].Leaders...)
	}
	return shards
}

func TestManifestShardMetadataAt(t *testing.T) {
	manifest := manifestForMetadataTest(t, "dist", 7, metadataTestShards())
	want := ShardMetadata{
		ID: "s0", AllocationGeneration: 1,
		Range: KeyRange{
			Start: KeyspacePoint{},
			End:   KeyspaceEnd{Point: pt(hb(0x80))},
		},
		Epoch:       11,
		LeaderCount: 2,
	}
	got, ok := manifest.ShardMetadataAt(0)
	if !ok || got != want {
		t.Fatalf("ShardMetadataAt(0) = %+v, %v; want %+v, true", got, ok, want)
	}

	for _, index := range []int{-1, manifest.ShardCount(), manifest.ShardCount() + 1} {
		if got, ok := manifest.ShardMetadataAt(index); ok || got != (ShardMetadata{}) {
			t.Errorf("ShardMetadataAt(%d) = %+v, %v; want zero metadata, false", index, got, ok)
		}
	}
	if got, ok := (&Manifest{}).ShardMetadataAt(0); ok || got != (ShardMetadata{}) {
		t.Errorf("empty manifest ShardMetadataAt(0) = %+v, %v; want zero metadata, false", got, ok)
	}
}

func TestManifestShardLeaderAt(t *testing.T) {
	manifest := manifestForMetadataTest(t, "dist", 7, metadataTestShards())
	if endpoint, ok := manifest.ShardLeaderAt(0, 1); !ok || endpoint != "ep-0b" {
		t.Fatalf("ShardLeaderAt = %q, %v; want ep-0b, true", endpoint, ok)
	}
	for _, coordinates := range [][2]int{{-1, 0}, {0, -1}, {2, 0}, {0, 2}} {
		if endpoint, ok := manifest.ShardLeaderAt(coordinates[0], coordinates[1]); ok || endpoint != "" {
			t.Errorf("ShardLeaderAt(%d, %d) = %q, %v; want empty, false",
				coordinates[0], coordinates[1], endpoint, ok)
		}
	}
}

func TestManifestShardMetadataForRange(t *testing.T) {
	manifest := manifestForMetadataTest(t, "dist", 7, metadataTestShards())
	want, _ := manifest.ShardMetadataAt(1)
	if ordinal, ok := manifest.ShardOrdinalForRange(want.Range); !ok || ordinal != 1 {
		t.Fatalf("ShardOrdinalForRange = %d, %v; want 1, true", ordinal, ok)
	}
	if got, ok := manifest.ShardMetadataForRange(want.Range); !ok || got != want {
		t.Fatalf("ShardMetadataForRange = %+v, %v; want %+v, true", got, ok, want)
	}

	wrongEnd := want.Range
	wrongEnd.End = KeyspaceEnd{Point: pt(hb(0xf0))}
	wrongStart := want.Range
	wrongStart.Start = pt(hb(0x90))
	for _, keyRange := range []KeyRange{wrongEnd, wrongStart, {}} {
		if ordinal, ok := manifest.ShardOrdinalForRange(keyRange); ok || ordinal != 0 {
			t.Errorf("ShardOrdinalForRange(%+v) = %d, %v; want 0, false", keyRange, ordinal, ok)
		}
		if got, ok := manifest.ShardMetadataForRange(keyRange); ok || got != (ShardMetadata{}) {
			t.Errorf("ShardMetadataForRange(%+v) = %+v, %v; want zero, false", keyRange, got, ok)
		}
	}
	var nilManifest *Manifest
	if got, ok := nilManifest.ShardMetadataForRange(want.Range); ok || got != (ShardMetadata{}) {
		t.Errorf("nil ShardMetadataForRange = %+v, %v; want zero, false", got, ok)
	}
}

func TestManifestSameShardLeaders(t *testing.T) {
	base := manifestForMetadataTest(t, "dist", 7, metadataTestShards())
	equal := manifestForMetadataTest(t, "dist", 7, cloneMetadataTestShards())

	reorderedShards := cloneMetadataTestShards()
	reorderedShards[0].Leaders[0], reorderedShards[0].Leaders[1] =
		reorderedShards[0].Leaders[1], reorderedShards[0].Leaders[0]
	reordered := manifestForMetadataTest(t, "dist", 7, reorderedShards)

	differentShards := cloneMetadataTestShards()
	differentShards[0].Leaders[1] = "ep-other"
	different := manifestForMetadataTest(t, "dist", 7, differentShards)

	shorterShards := cloneMetadataTestShards()
	shorterShards[0].Leaders = shorterShards[0].Leaders[:1]
	shorter := manifestForMetadataTest(t, "dist", 7, shorterShards)

	if !base.SameShardLeaders(0, equal, 0) {
		t.Fatal("identical ordered leader identities did not compare equal")
	}
	if base.SameShardLeaders(0, reordered, 0) {
		t.Fatal("reordered leaders compared equal")
	}
	if base.SameShardLeaders(0, different, 0) {
		t.Fatal("different leader identity compared equal")
	}
	if base.SameShardLeaders(0, shorter, 0) {
		t.Fatal("different leader count compared equal")
	}

	var nilManifest *Manifest
	invalid := []struct {
		name  string
		left  *Manifest
		i     int
		right *Manifest
		j     int
	}{
		{"nil receiver", nilManifest, 0, base, 0},
		{"nil other", base, 0, nilManifest, 0},
		{"both nil", nilManifest, 0, nilManifest, 0},
		{"negative left index", base, -1, equal, 0},
		{"left index at bound", base, base.ShardCount(), equal, 0},
		{"negative right index", base, 0, equal, -1},
		{"right index at bound", base, 0, equal, equal.ShardCount()},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if test.left.SameShardLeaders(test.i, test.right, test.j) {
				t.Fatal("invalid leader comparison reported equality")
			}
		})
	}
}

func TestManifestEqualSemanticIdentity(t *testing.T) {
	base := manifestForMetadataTest(t, "dist", 7, metadataTestShards())
	equal := manifestForMetadataTest(t, "dist", 7, cloneMetadataTestShards())
	if !base.Equal(base) || !base.Equal(equal) || !equal.Equal(base) {
		t.Fatal("identical manifests did not compare symmetrically equal")
	}

	variant := func(change func([]Shard)) *Manifest {
		shards := cloneMetadataTestShards()
		change(shards)
		return manifestForMetadataTest(t, "dist", 7, shards)
	}
	cases := []struct {
		name     string
		manifest *Manifest
	}{
		{
			name:     "distribution",
			manifest: manifestForMetadataTest(t, "other", 7, cloneMetadataTestShards()),
		},
		{
			name:     "version",
			manifest: manifestForMetadataTest(t, "dist", 8, cloneMetadataTestShards()),
		},
		{
			name: "shard id",
			manifest: variant(func(shards []Shard) {
				shards[0].ID = "renamed"
			}),
		},
		{
			name: "shard identity order",
			manifest: variant(func(shards []Shard) {
				shards[0].ID, shards[1].ID = shards[1].ID, shards[0].ID
			}),
		},
		{
			name: "range geometry",
			manifest: variant(func(shards []Shard) {
				boundary := pt(hb(0x70))
				shards[0].Range.End = KeyspaceEnd{Point: boundary}
				shards[1].Range.Start = boundary
			}),
		},
		{
			name: "leader identity",
			manifest: variant(func(shards []Shard) {
				shards[0].Leaders[1] = "ep-other"
			}),
		},
		{
			name: "leader order",
			manifest: variant(func(shards []Shard) {
				shards[0].Leaders[0], shards[0].Leaders[1] =
					shards[0].Leaders[1], shards[0].Leaders[0]
			}),
		},
		{
			name: "allocation generation",
			manifest: variant(func(shards []Shard) {
				shards[0].AllocationGeneration = 3
			}),
		},
		{
			name: "ownership epoch",
			manifest: variant(func(shards []Shard) {
				shards[0].Epoch++
			}),
		},
		{
			name: "shard count",
			manifest: manifestForMetadataTest(t, "dist", 7, []Shard{
				{
					ID: "all", AllocationGeneration: 3,
					Range:   KeyRange{Start: KeyspacePoint{}, End: maxKeyEnd},
					Leaders: []EndpointID{"ep-all"}, Epoch: 11,
				},
			}),
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if base.Equal(test.manifest) || test.manifest.Equal(base) {
				t.Fatal("semantically different manifests compared equal")
			}
		})
	}

	var nilManifest *Manifest
	if base.Equal(nilManifest) || nilManifest.Equal(base) || nilManifest.Equal(nilManifest) {
		t.Fatal("nil manifest compared equal")
	}
}

func TestManifestInspectionZeroAlloc(t *testing.T) {
	left := manifestForMetadataTest(t, "dist", 7, metadataTestShards())
	right := manifestForMetadataTest(t, "dist", 7, cloneMetadataTestShards())
	metadataRange := metadataTestShards()[0].Range

	// Warm each path before measuring so one-time runtime setup is excluded.
	manifestMetadataSink, manifestBoolSink = left.ShardMetadataAt(0)
	_, manifestBoolSink = left.ShardLeaderAt(0, 0)
	manifestMetadataSink, manifestBoolSink = left.ShardMetadataForRange(metadataRange)
	manifestBoolSink = left.SameShardLeaders(0, right, 0)
	manifestBoolSink = left.Equal(right)

	tests := []struct {
		name string
		run  func()
	}{
		{
			name: "metadata",
			run: func() {
				manifestMetadataSink, manifestBoolSink = left.ShardMetadataAt(0)
			},
		},
		{
			name: "leader",
			run: func() {
				_, manifestBoolSink = left.ShardLeaderAt(0, 0)
			},
		},
		{
			name: "metadata by exact range",
			run: func() {
				manifestMetadataSink, manifestBoolSink = left.ShardMetadataForRange(metadataRange)
			},
		},
		{
			name: "leaders",
			run: func() {
				manifestBoolSink = left.SameShardLeaders(0, right, 0)
			},
		},
		{
			name: "equality",
			run: func() {
				manifestBoolSink = left.Equal(right)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if allocations := testing.AllocsPerRun(1000, test.run); allocations != 0 {
				t.Fatalf("allocations = %v, want 0", allocations)
			}
		})
	}
}

func BenchmarkManifestAllocationFreeInspection(b *testing.B) {
	left := manifestForMetadataTest(b, "dist", 7, metadataTestShards())
	right := manifestForMetadataTest(b, "dist", 7, cloneMetadataTestShards())

	b.Run("metadata", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			manifestMetadataSink, manifestBoolSink = left.ShardMetadataAt(0)
		}
	})
	b.Run("metadata-by-exact-range", func(b *testing.B) {
		keyRange := metadataTestShards()[0].Range
		b.ReportAllocs()
		for b.Loop() {
			manifestMetadataSink, manifestBoolSink = left.ShardMetadataForRange(keyRange)
		}
	})
	b.Run("leaders", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			manifestBoolSink = left.SameShardLeaders(0, right, 0)
		}
	})
	b.Run("equality", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			manifestBoolSink = left.Equal(right)
		}
	})
}
