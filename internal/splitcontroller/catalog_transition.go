package splitcontroller

import (
	"cmp"
	"slices"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

type CertifiedRangeSplit struct {
	Target      *distribution.Manifest
	Partitioner *rangesplit.Partitioner
	Certificate rangesplit.CutoverCertificate
}

func BuildCertifiedRangeSplitBatch(current *gateway.Snapshot, nextGeneration uint64, splits []CertifiedRangeSplit) (*gateway.Snapshot, error) {
	if current == nil || current.Generation() == ^uint64(0) || nextGeneration != current.Generation()+1 || len(splits) == 0 || len(splits) > rangesplit.MaxComposedManifestSplits {
		return nil, rangesplit.ErrManifestTransition
	}
	var order [rangesplit.MaxComposedManifestSplits]uint8
	for index := range splits {
		split := &splits[index]
		if split.Target == nil || split.Partitioner == nil {
			return nil, rangesplit.ErrManifestTransition
		}
		source, ok := current.Manifest(split.Target.Distribution())
		if !ok {
			return nil, rangesplit.ErrManifestTransition
		}
		if err := split.Partitioner.ValidatePublicationTransition(source, split.Target, current.Generation(), nextGeneration, split.Certificate); err != nil {
			return nil, err
		}
		order[index] = uint8(index)
	}
	slices.SortFunc(order[:len(splits)], func(left, right uint8) int {
		return cmp.Compare(splits[left].Target.Distribution(), splits[right].Target.Distribution())
	})
	var manifests [rangesplit.MaxComposedManifestSplits]*distribution.Manifest
	manifestCount := 0
	for first := 0; first < len(splits); {
		name := splits[order[first]].Target.Distribution()
		last := first + 1
		for last < len(splits) && splits[order[last]].Target.Distribution() == name {
			last++
		}
		currentManifest, _ := current.Manifest(name)
		var transitions [rangesplit.MaxComposedManifestSplits]rangesplit.ManifestTransition
		for index := first; index < last; index++ {
			split := &splits[order[index]]
			transitions[index-first] = rangesplit.ManifestTransition{Partitioner: split.Partitioner, Target: split.Target}
		}
		combined, err := rangesplit.ComposeManifestTransitions(currentManifest, transitions[:last-first])
		if err != nil {
			return nil, err
		}
		manifests[manifestCount] = combined
		manifestCount++
		first = last
	}
	return gateway.BuildManifestTransitions(current, manifests[:manifestCount], nextGeneration)
}

func BuildCertifiedRangeSplitTransition(current *gateway.Snapshot, nextManifest *distribution.Manifest, nextGeneration uint64, partitioner *rangesplit.Partitioner, certificate rangesplit.CutoverCertificate) (*gateway.Snapshot, error) {
	if current == nil || nextManifest == nil || partitioner == nil {
		return nil, rangesplit.ErrManifestTransition
	}
	currentManifest, ok := current.Manifest(nextManifest.Distribution())
	if !ok {
		return nil, rangesplit.ErrManifestTransition
	}
	if err := partitioner.ValidatePublicationTransition(currentManifest, nextManifest, current.Generation(), nextGeneration, certificate); err != nil {
		return nil, err
	}
	return gateway.BuildManifestTransition(current, nextManifest, nextGeneration)
}
