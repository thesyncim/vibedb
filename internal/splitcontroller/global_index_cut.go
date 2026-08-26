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
	for index := range p.indexRelations {
		relation := p.indexRelations[index]
		rows, ok := snapshot.Relation(replication.RelationID(relation.Relation))
		if !ok || rows == nil {
			return ErrTopologyConflict
		}
		var childRanges [autosplit.MaxSplitChildren]distribution.KeyRange
		for child := 0; child < int(p.childCount); child++ {
			childRanges[child] = p.children[child].Range
		}
		err := validateGlobalIndexRows(
			relation, p.source.Range, childRanges[:p.childCount], rows.RangeRaw,
		)
		if err != nil {
			return errors.Join(ErrTopologyConflict, err)
		}
	}
	return nil
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
