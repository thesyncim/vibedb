package splitcontroller

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestGlobalIndexCutUsesCanonicalUniqueAndNonUniquePlacementAtBoundary(t *testing.T) {
	leftTuple, rightTuple := orderedGlobalIndexTuples(t)
	rightPoint, _, ok := distribution.NativePointForEncodedTuplePrefix(
		rightTuple, 1, distribution.DefaultVirtualBucketBits,
	)
	if !ok {
		t.Fatal("right tuple did not map")
	}
	children := []distribution.KeyRange{
		{End: distribution.KeyspaceEnd{Point: rightPoint}},
		{Start: rightPoint, End: distribution.KeyspaceEnd{Max: true}},
	}
	source := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	for _, unique := range []bool{true, false} {
		relation := splitGlobalIndexRelation(unique)
		keys := [][]byte{leftTuple, rightTuple}
		if !unique {
			locator, err := distribution.CurrentTupleCodec.AppendTuple(
				nil, []distribution.Scalar{distribution.NewString("row-1")},
			)
			if err != nil {
				t.Fatal(err)
			}
			keys = [][]byte{
				append(append([]byte(nil), leftTuple...), locator...),
				append(append([]byte(nil), rightTuple...), locator...),
			}
		}
		visited := 0
		err := validateGlobalIndexRows(relation, source, children,
			func(visit func(key, value []byte) error) error {
				for _, key := range keys {
					visited++
					if err := visit(key, nil); err != nil {
						return err
					}
				}
				return nil
			})
		if err != nil || visited != 2 {
			t.Fatalf("unique=%v visited=%d err=%v", unique, visited, err)
		}
		point, valid := relation.GlobalIndexStorageKeyPoint(keys[1])
		if !valid || point != rightPoint || !children[1].Contains(point) || children[0].Contains(point) {
			t.Fatalf("unique=%v boundary point=%x valid=%v", unique, point, valid)
		}
	}
}

func TestGlobalIndexCutRejectsMalformedAndUnprovableRows(t *testing.T) {
	left, right := orderedGlobalIndexTuples(t)
	boundary, _, _ := distribution.NativePointForEncodedTuplePrefix(
		right, 1, distribution.DefaultVirtualBucketBits,
	)
	source := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	children := []distribution.KeyRange{
		{End: distribution.KeyspaceEnd{Point: boundary}},
		{Start: boundary, End: distribution.KeyspaceEnd{Max: true}},
	}
	relation := splitGlobalIndexRelation(true)
	for name, rows := range map[string][][]byte{
		"truncated": {left, right[:len(right)-1]},
		"trailing":  {left, append(append([]byte(nil), right...), 0xff)},
	} {
		t.Run(name, func(t *testing.T) {
			completed := false
			err := validateGlobalIndexRows(relation, source, children,
				func(visit func(key, value []byte) error) error {
					for _, key := range rows {
						if err := visit(key, nil); err != nil {
							return err
						}
					}
					completed = true
					return nil
				})
			if !errors.Is(err, ErrTopologyConflict) || completed {
				t.Fatalf("err=%v completed=%v", err, completed)
			}
		})
	}

	// A gap and an overlap are both unprovable ownership, even for a canonical
	// key exactly on the split boundary.
	gap := append([]distribution.KeyRange(nil), children...)
	gap[1].Start[0]++
	overlap := append([]distribution.KeyRange(nil), children...)
	overlap[0].End = distribution.KeyspaceEnd{Max: true}
	for name, ranges := range map[string][]distribution.KeyRange{"gap": gap, "overlap": overlap} {
		t.Run(name, func(t *testing.T) {
			err := validateGlobalIndexRows(relation, source, ranges,
				func(visit func(key, value []byte) error) error { return visit(right, nil) })
			if !errors.Is(err, ErrTopologyConflict) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func splitGlobalIndexRelation(unique bool) sqldriver.ReplicatedShardRelationIdentity {
	return sqldriver.ReplicatedShardRelationIdentity{
		Relation: 2, Kind: sqldriver.ReplicatedShardRelationGlobalIndex, Table: "docs",
		IndexID: 91, Incarnation: 7, LocatorCount: 1, Unique: unique,
		KeyEncoding:   sqldriver.ReplicatedRelationKeyCanonicalTuple,
		KeyArity:      1,
		TupleVersion:  distribution.CurrentTupleVersion,
		MapperVersion: distribution.NativeMapperVersion,
		BucketBits:    distribution.DefaultVirtualBucketBits,
	}
}

func orderedGlobalIndexTuples(t testing.TB) ([]byte, []byte) {
	t.Helper()
	first, err := distribution.CurrentTupleCodec.AppendTuple(
		nil, []distribution.Scalar{distribution.NewString("alpha")},
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := distribution.CurrentTupleCodec.AppendTuple(
		nil, []distribution.Scalar{distribution.NewString("omega")},
	)
	if err != nil {
		t.Fatal(err)
	}
	firstPoint, _, firstOK := distribution.NativePointForEncodedTuplePrefix(
		first, 1, distribution.DefaultVirtualBucketBits,
	)
	secondPoint, _, secondOK := distribution.NativePointForEncodedTuplePrefix(
		second, 1, distribution.DefaultVirtualBucketBits,
	)
	if !firstOK || !secondOK || firstPoint == secondPoint {
		t.Fatal("test tuples did not produce distinct points")
	}
	if bytes.Compare(firstPoint[:], secondPoint[:]) > 0 {
		first, second = second, first
	}
	return first, second
}
