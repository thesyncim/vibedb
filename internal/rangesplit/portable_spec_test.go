package rangesplit

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	vibejson "github.com/thesyncim/vibejson"
)

func TestPortablePartitionerCanonicalReopen(t *testing.T) {
	plan := testSplitPlan(t, "node-b")
	p, err := NewPartitioner(plan, "docs", []string{"/tenant", "/sequence"}, distribution.DefaultVirtualBucketBits)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendPortablePartitioner(nil, p)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenPortablePartitioner(raw)
	if err != nil || opened.Digest() != p.Digest() || opened.CollectionName() != p.CollectionName() {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	again, err := AppendPortablePartitioner(nil, opened)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("re-encode err=%v", err)
	}
	corrupt := bytes.Clone(raw)
	corrupt[len(corrupt)/2] ^= 1
	if _, err := OpenPortablePartitioner(corrupt); !errors.Is(err, ErrPortablePartitioner) {
		t.Fatalf("corrupt error=%v", err)
	}
}

func TestPortablePartitionerRejectsDivergentTopology(t *testing.T) {
	plan := testSplitPlan(t, "node-b")
	p, _ := NewPartitioner(plan, "docs", []string{"/tenant"}, distribution.DefaultVirtualBucketBits)
	raw, _ := AppendPortablePartitioner(nil, p)
	var base portablePartitioner
	if err := vibejson.Unmarshal(raw, &base); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*portablePartitioner)
	}{{"digest", func(s *portablePartitioner) { s.Digest[0] ^= 1 }}, {"target", func(s *portablePartitioner) { s.Target++ }}, {"duplicate_child", func(s *portablePartitioner) { s.Children[1].Shard = s.Children[0].Shard }}, {"range_gap", func(s *portablePartitioner) { s.Children[1].Range.Start[7]++ }}, {"source", func(s *portablePartitioner) { s.Source.OwnershipEpoch++ }}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := base
			candidate.Children = append([]autosplit.SplitChild(nil), base.Children...)
			candidate.Manifest = append([]distribution.Shard(nil), base.Manifest...)
			tc.mutate(&candidate)
			encoded, err := vibejson.Marshal(&candidate)
			if err == nil {
				encoded, err = vibejson.AppendCanonicalize(nil, encoded)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := OpenPortablePartitioner(encoded); !errors.Is(err, ErrPortablePartitioner) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
