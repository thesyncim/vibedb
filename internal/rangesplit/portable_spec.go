package rangesplit

import (
	"bytes"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	vibejson "github.com/thesyncim/vibejson"
)

const MaxPortablePartitionerBytes = 32 << 10

var ErrPortablePartitioner = errors.New("rangesplit: invalid portable partitioner")

type portablePartitioner struct {
	Source             autosplit.SourceIdentity      `json:"source"`
	Children           []autosplit.SplitChild        `json:"children"`
	ChildCount         uint8                         `json:"child_count"`
	Retained           uint8                         `json:"retained"`
	Collection         string                        `json:"collection"`
	Columns            []string                      `json:"columns"`
	Target             distribution.RoutingVersion   `json:"target"`
	TargetDistribution distribution.DistributionName `json:"target_distribution"`
	Manifest           []distribution.Shard          `json:"manifest"`
	Digest             [32]byte                      `json:"digest"`
	SourceCoordinates  TailSourceCoordinates         `json:"source_coordinates,omitempty"`
	TargetGeneration   uint64                        `json:"target_generation,omitempty"`
}

func AppendPortablePartitioner(dst []byte, p *Partitioner) ([]byte, error) {
	if p == nil || len(p.columns) == 0 {
		return dst, ErrPortablePartitioner
	}
	s := portablePartitioner{Source: p.source, ChildCount: p.childCount, Retained: p.retained, Collection: p.collection, Columns: slices.Clone(p.columns), Target: p.target, TargetDistribution: p.targetDistribution, Manifest: slices.Clone(p.manifest), Digest: p.digest, Children: make([]autosplit.SplitChild, p.childCount)}
	s.SourceCoordinates, s.TargetGeneration = p.sourceCoordinates, p.targetGeneration
	copy(s.Children, p.children[:p.childCount])
	raw, err := vibejson.Marshal(&s)
	if err != nil {
		return dst, err
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start == 0 || len(dst)-start > MaxPortablePartitionerBytes {
		return dst[:start], errors.Join(err, ErrPortablePartitioner)
	}
	return dst, nil
}

func OpenPortablePartitioner(raw []byte) (*Partitioner, error) {
	if len(raw) == 0 || len(raw) > MaxPortablePartitionerBytes {
		return nil, ErrPortablePartitioner
	}
	var s portablePartitioner
	if err := vibejson.Unmarshal(raw, &s); err != nil {
		return nil, errors.Join(err, ErrPortablePartitioner)
	}
	can, err := vibejson.Marshal(&s)
	if err == nil {
		can, err = vibejson.AppendCanonicalize(nil, can)
	}
	if err != nil || !bytes.Equal(raw, can) || s.ChildCount < 2 || s.ChildCount > autosplit.MaxSplitChildren || s.Retained >= s.ChildCount || len(s.Children) != int(s.ChildCount) || len(s.Columns) == 0 || s.Collection == "" || s.Target == 0 || s.Digest == ([32]byte{}) {
		return nil, errors.Join(err, ErrPortablePartitioner)
	}
	program, err := distribution.CompileDocumentPointProgram(s.Columns, s.Source.BucketBits)
	if err != nil {
		return nil, errors.Join(err, ErrPortablePartitioner)
	}
	target, err := distribution.NewManifest(s.TargetDistribution, s.Target, s.Manifest)
	if err != nil || s.TargetDistribution != s.Source.Distribution || s.Target != s.Source.RoutingVersion+1 {
		return nil, errors.Join(err, ErrPortablePartitioner)
	}
	childIDs := make(map[distribution.ShardID]struct{}, len(s.Children))
	for i, c := range s.Children {
		if c.Retained != (uint8(i) == s.Retained) || !c.Range.Valid() || c.Shard == "" || len(c.Leaders) == 0 {
			return nil, ErrPortablePartitioner
		}
		if _, dup := childIDs[c.Shard]; dup {
			return nil, ErrPortablePartitioner
		}
		childIDs[c.Shard] = struct{}{}
	}
	base := make([]distribution.Shard, 0, target.ShardCount()-len(s.Children)+1)
	inserted := false
	for i := 0; i < target.ShardCount(); i++ {
		sh, _ := target.ShardInfo(i)
		if _, child := childIDs[sh.ID]; child {
			if !inserted {
				ret := s.Children[s.Retained]
				base = append(base, distribution.Shard{ID: s.Source.Shard, AllocationGeneration: s.Source.AllocationGeneration, Range: s.Source.Range, Leaders: slices.Clone(ret.Leaders), Epoch: s.Source.OwnershipEpoch})
				inserted = true
			}
			continue
		}
		base = append(base, sh)
	}
	if !inserted {
		return nil, ErrPortablePartitioner
	}
	sourceManifest, err := distribution.NewManifest(s.Source.Distribution, s.Source.RoutingVersion, base)
	if err != nil {
		return nil, errors.Join(err, ErrPortablePartitioner)
	}
	restored, err := autosplit.RestoreSplitPlan(sourceManifest, s.Source, s.Retained, s.Children)
	if err != nil {
		return nil, errors.Join(err, ErrPortablePartitioner)
	}
	digest, err := SplitPlanDigest(restored)
	if err != nil {
		return nil, errors.Join(err, ErrPortablePartitioner)
	}
	p := &Partitioner{source: s.Source, childCount: s.ChildCount, retained: s.Retained, collection: s.Collection, columns: slices.Clone(s.Columns), program: program, target: s.Target, targetDistribution: s.TargetDistribution, manifest: slices.Clone(s.Manifest), digest: digest}
	for i, c := range s.Children {
		p.children[i] = c
		p.ranges[i] = c.Range
	}
	if s.SourceCoordinates != (TailSourceCoordinates{}) || s.TargetGeneration != 0 {
		p, err = p.BindSourceFence(s.SourceCoordinates, s.TargetGeneration)
		if err != nil {
			return nil, errors.Join(err, ErrPortablePartitioner)
		}
	}
	if p.digest != s.Digest {
		return nil, ErrPortablePartitioner
	}
	return p, nil
}
