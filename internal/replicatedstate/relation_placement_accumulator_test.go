package replicatedstate

import (
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func TestRelationPlacementAccumulatorIsOrderIndependentAndTamperEvident(t *testing.T) {
	profile := testGlobalIndexProfile(91, 7, 1, true)
	first, err := distribution.CurrentTupleCodec.AppendTuple(
		nil, []distribution.Scalar{distribution.NewString("a")},
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := distribution.CurrentTupleCodec.AppendTuple(
		nil, []distribution.Scalar{distribution.NewString("b")},
	)
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{first, second}
	values := [][]byte{[]byte(`["doc-1"]`), []byte(`["doc-2"]`)}
	owned := testBinding().OwnedRange
	left := newRelationPlacementAccumulator(owned, true)
	right := newRelationPlacementAccumulator(owned, true)
	for index := range keys {
		point, ok := globalIndexProfilePoint(profile, keys[index])
		if !ok {
			t.Fatalf("key %d is not canonical", index)
		}
		left.addRaw(point, keys[index], values[index])
	}
	for index := len(keys) - 1; index >= 0; index-- {
		point, _ := globalIndexProfilePoint(profile, keys[index])
		right.addRaw(point, keys[index], values[index])
	}
	if left != right || left.rows != 2 || left.outside != 0 {
		t.Fatalf("order-dependent accumulator: left=%+v right=%+v", left, right)
	}
	relations := []relationCollection{{
		id: 2, kind: RelationGlobalIndex, contract: sha256.Sum256([]byte("contract")),
		placement: left,
	}}
	manifest := sha256.Sum256([]byte("manifest"))
	want := relationPlacementStateDigest(3, manifest, relations)
	relations[0].placement.xor[0] ^= 1
	if got := relationPlacementStateDigest(3, manifest, relations); got == want {
		t.Fatal("modified accumulator retained its durable state commitment")
	}
	relations[0].placement = left
	point, _ := globalIndexProfilePoint(profile, keys[0])
	valueDigest := sha256.Sum256(values[0])
	if err := relations[0].placement.removeDescriptor(
		point, keys[0], uint64(len(values[0])), valueDigest,
	); err != nil {
		t.Fatal(err)
	}
	if relations[0].placement.rows != 1 ||
		relationPlacementStateDigest(3, manifest, relations) == want {
		t.Fatal("delete did not change the authenticated image")
	}
}

func BenchmarkRelationPlacementAccumulatorMutation(b *testing.B) {
	profile := testGlobalIndexProfile(91, 7, 1, true)
	key, err := distribution.CurrentTupleCodec.AppendTuple(
		nil, []distribution.Scalar{distribution.NewString("a")},
	)
	if err != nil {
		b.Fatal(err)
	}
	value := []byte(`["doc-1"]`)
	point, ok := globalIndexProfilePoint(profile, key)
	if !ok {
		b.Fatal("canonical key rejected")
	}
	valueDigest := sha256.Sum256(value)
	accumulator := newRelationPlacementAccumulator(testBinding().OwnedRange, true)
	b.ReportAllocs()
	b.SetBytes(int64(len(key) + len(value)))
	for i := 0; i < b.N; i++ {
		accumulator.addRaw(point, key, value)
		if err := accumulator.removeDescriptor(
			point, key, uint64(len(value)), valueDigest,
		); err != nil {
			b.Fatal(err)
		}
	}
	if accumulator.rows != 0 || accumulator.xor != ([sha256.Size]byte{}) {
		b.Fatal("benchmark accumulator did not settle")
	}
}
