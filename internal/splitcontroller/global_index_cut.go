package splitcontroller

import (
	"errors"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// validateGlobalIndexCut proves that every independently stored global-index
// key at one coherent source cut is both canonical for the schema generation
// and owned by exactly one planned child. The SQL relation identity is the
// catalog-authenticated parser: malformed, stale-format, and unprovable keys
// all fail closed before capture or source sealing can advance.
func (p *Plan) validateGlobalIndexCut(snapshot *replicatedstate.ReadSnapshot) error {
	if p == nil || snapshot == nil {
		return ErrInvalidPlan
	}
	if len(p.indexRelations) == 0 {
		return nil
	}
	if p.relationDigest == ([32]byte{}) ||
		snapshot.Fence().RelationManifestDigest != p.relationDigest {
		return ErrTopologyConflict
	}
	var childRanges [autosplit.MaxSplitChildren]distribution.KeyRange
	for child := 0; child < int(p.childCount); child++ {
		childRanges[child] = p.children[child].Range
	}
	if !canonicalSplitCoverage(p.source.Range, childRanges[:p.childCount]) {
		return ErrTopologyConflict
	}
	for index := range p.indexRelations {
		relation := p.indexRelations[index]
		_, err := snapshot.GlobalIndexPlacementProof(
			replication.RelationID(relation.Relation), p.source.Range, [32]byte(p.operation),
		)
		if err != nil {
			return errors.Join(ErrTopologyConflict, err)
		}
	}
	return nil
}

// canonicalSplitCoverage proves once, independent of relation cardinality,
// that the canonical child order partitions the exact source interval without
// a gap, overlap, or extension.
func canonicalSplitCoverage(
	source distribution.KeyRange,
	children []distribution.KeyRange,
) bool {
	if !source.Valid() || len(children) < 2 {
		return false
	}
	next := source.Start
	for index := range children {
		child := children[index]
		if !child.Valid() || child.Start != next || !source.Contains(child.Start) ||
			child.End.Max && index != len(children)-1 {
			return false
		}
		if child.End.Max {
			return source.End.Max
		}
		next = child.End.Point
	}
	return !source.End.Max && next == source.End.Point
}

// validateGlobalIndexRows is kept separate from snapshot acquisition so the
// byte grammar and exact half-open split boundaries can be qualified directly.
func validateGlobalIndexRows(
	relation sqldriver.ReplicatedShardRelationIdentity,
	source distribution.KeyRange,
	children []distribution.KeyRange,
	rangeRows func(func(key, value []byte) error) error,
) error {
	if rangeRows == nil || len(children) < 2 {
		return ErrTopologyConflict
	}
	return rangeRows(func(key, _ []byte) error {
		point, valid := relation.GlobalIndexStorageKeyPoint(key)
		if !valid || !source.Contains(point) {
			return ErrTopologyConflict
		}
		owners := 0
		for child := range children {
			if children[child].Contains(point) {
				owners++
			}
		}
		if owners != 1 {
			return ErrTopologyConflict
		}
		return nil
	})
}

func sameSplitCut(left, right replicatedstate.State) bool {
	return left.Applied == right.Applied && left.LastTerm == right.LastTerm &&
		left.ReplicaSetVersion == right.ReplicaSetVersion &&
		left.LastEntryDigest == right.LastEntryDigest &&
		left.DataChainDigest == right.DataChainDigest &&
		left.SnapshotBaseDigest == right.SnapshotBaseDigest && left.Binding == right.Binding
}
