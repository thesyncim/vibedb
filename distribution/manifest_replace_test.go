package distribution

import (
	"errors"
	"testing"
)

func TestManifestReplaceShardSharesOnlyImmutableUnchangedStorage(t *testing.T) {
	first := pt(hb(0x40))
	second := pt(hb(0x80))
	middle := pt(hb(0x60))
	current := manifestForMetadataTest(t, "dist", 7, []Shard{
		{ID: "left", AllocationGeneration: 1,
			Range: KeyRange{End: KeyspaceEnd{Point: first}}, Leaders: []EndpointID{"a"}, Epoch: 1},
		{ID: "source", AllocationGeneration: 2,
			Range: KeyRange{Start: first, End: KeyspaceEnd{Point: second}}, Leaders: []EndpointID{"b"}, Epoch: 2},
		{ID: "right", AllocationGeneration: 3,
			Range: KeyRange{Start: second, End: maxKeyEnd}, Leaders: []EndpointID{"c"}, Epoch: 3},
	})
	replacements := []Shard{
		{ID: "source", AllocationGeneration: 2,
			Range: KeyRange{Start: first, End: KeyspaceEnd{Point: middle}}, Leaders: []EndpointID{"b"}, Epoch: 3},
		{ID: "middle", AllocationGeneration: 4,
			Range: KeyRange{Start: middle, End: KeyspaceEnd{Point: second}}, Leaders: []EndpointID{"d"}, Epoch: 1},
	}
	next, err := current.ReplaceShard(1, 8, replacements)
	if err != nil {
		t.Fatal(err)
	}
	if next.Version() != 8 || next.ShardCount() != 4 || current.ShardCount() != 3 {
		t.Fatalf("versions/counts = %d/%d and %d/%d",
			current.Version(), current.ShardCount(), next.Version(), next.ShardCount())
	}
	if &current.shards[0].Leaders[0] != &next.shards[0].Leaders[0] ||
		&current.shards[2].Leaders[0] != &next.shards[3].Leaders[0] {
		t.Fatal("unchanged immutable leader storage was copied")
	}
	if &replacements[0].Leaders[0] == &next.shards[1].Leaders[0] ||
		&replacements[1].Leaders[0] == &next.shards[2].Leaders[0] {
		t.Fatal("replacement input leader storage was retained")
	}
	replacements[0].Leaders[0] = "mutated"
	if next.shards[1].Leaders[0] != "b" {
		t.Fatal("replacement input mutated successor")
	}
	if current.shards[1].Range.End.Point != second || current.shards[1].Epoch != 2 {
		t.Fatal("successor construction mutated source manifest")
	}
	detached := make([]Shard, next.ShardCount())
	for index := range detached {
		detached[index], _ = next.ShardInfo(index)
	}
	revalidated, err := NewManifest(next.Distribution(), next.Version(), detached)
	if err != nil || !next.Equal(revalidated) {
		t.Fatalf("copy-on-write successor did not revalidate: %v", err)
	}
}

func TestManifestReplaceShardRejectsInvalidGeometryAndIdentity(t *testing.T) {
	current := manifestForMetadataTest(t, "dist", 7, metadataTestShards())
	middle := pt(hb(0x40))
	valid := []Shard{
		{ID: "s0", AllocationGeneration: 1,
			Range: KeyRange{End: KeyspaceEnd{Point: middle}}, Leaders: []EndpointID{"a"}, Epoch: 12},
		{ID: "new", AllocationGeneration: 3,
			Range: KeyRange{Start: middle, End: KeyspaceEnd{Point: pt(hb(0x80))}}, Leaders: []EndpointID{"b"}, Epoch: 1},
	}
	tests := []struct {
		name         string
		ordinal      int
		replacements func() []Shard
	}{
		{"negative ordinal", -1, func() []Shard { return append([]Shard(nil), valid...) }},
		{"empty", 0, func() []Shard { return nil }},
		{"too many", 0, func() []Shard {
			return make([]Shard, MaxManifestReplacementShards+1)
		}},
		{"wrong source start", 0, func() []Shard {
			r := append([]Shard(nil), valid...)
			r[0].Range.Start = pt(hb(0x10))
			return r
		}},
		{"gap", 0, func() []Shard {
			r := append([]Shard(nil), valid...)
			r[1].Range.Start = pt(hb(0x50))
			return r
		}},
		{"wrong source end", 0, func() []Shard {
			r := append([]Shard(nil), valid...)
			r[1].Range.End.Point = pt(hb(0x70))
			return r
		}},
		{"empty leaders", 0, func() []Shard {
			r := append([]Shard(nil), valid...)
			r[1].Leaders = nil
			return r
		}},
		{"empty endpoint", 0, func() []Shard {
			r := append([]Shard(nil), valid...)
			r[1].Leaders = []EndpointID{""}
			return r
		}},
		{"active id", 0, func() []Shard {
			r := append([]Shard(nil), valid...)
			r[1].ID = "s1"
			return r
		}},
		{"active allocation", 0, func() []Shard {
			r := append([]Shard(nil), valid...)
			r[1].AllocationGeneration = 2
			return r
		}},
		{"duplicate replacement id", 0, func() []Shard {
			r := append([]Shard(nil), valid...)
			r[1].ID = "s0"
			return r
		}},
		{"duplicate replacement allocation", 0, func() []Shard {
			r := append([]Shard(nil), valid...)
			r[1].AllocationGeneration = 1
			return r
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := current.ReplaceShard(test.ordinal, 8, test.replacements()); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("ReplaceShard error = %v, want ErrInvalidManifest", err)
			}
		})
	}
}
