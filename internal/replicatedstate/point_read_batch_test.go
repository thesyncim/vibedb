package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestPointReadBatchGrammarAndPositionalValues(t *testing.T) {
	reads := []PointRead{
		{Relation: 1, Key: []byte("a")},
		{Relation: 2, Key: []byte{0x91, 0x01, 'a'}},
		{Relation: 1, Key: []byte("missing")},
	}
	packed, err := AppendPointReadBatch([]byte("prefix"), reads)
	if err != nil {
		t.Fatal(err)
	}
	request, err := OpenPointReadBatch(packed[len("prefix"):])
	if err != nil || request.Count() != len(reads) {
		t.Fatalf("request count=%d err=%v", request.Count(), err)
	}
	for _, corrupt := range [][]byte{
		nil,
		{0, 0, 0, 0},
		packed[len("prefix") : len(packed)-1],
		append(bytes.Clone(packed[len("prefix"):]), 0),
	} {
		if _, err := OpenPointReadBatch(corrupt); !errors.Is(err, ErrPointReadBatch) {
			t.Fatalf("corrupt request %x error=%v", corrupt, err)
		}
	}
	many := make([]PointRead, 257)
	for index := range many {
		many[index] = PointRead{Relation: 1, Key: []byte("same-key")}
	}
	manyPacked, err := AppendPointReadBatch(nil, many)
	if err != nil {
		t.Fatal(err)
	}
	if manyRequest, openErr := OpenPointReadBatch(manyPacked); openErr != nil ||
		manyRequest.Count() != len(many) {
		t.Fatalf("257-point request count=%d err=%v", manyRequest.Count(), openErr)
	}

	fixture := newRelationBundleFixture(t, true)
	baseValue := []byte(`{"email":"a","n":1}`)
	globalValue := []byte(`["a"]`)
	command := fixture.command(t, 1,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: reads[0].Key, Value: baseValue,
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: reads[1].Key, Value: globalValue,
		}}},
	)
	publication, err := fixture.machine.ApplyNormal(normalMeta(3), command)
	if err != nil {
		t.Fatal(err)
	}
	if code := bundleCompletionResult(t, fixture.machine, command); code != ResultApplied {
		t.Fatalf("apply result=%d", code)
	}
	if raw, found, readErr := fixture.base.Collection.AppendRaw(nil, reads[0].Key); readErr != nil ||
		!found || !bytes.Equal(raw, baseValue) {
		t.Fatalf("direct base read raw=%q found=%v err=%v", raw, found, readErr)
	}
	maximum := 1 << 20
	result, err := fixture.machine.PointReadBatchInto(
		packed[len("prefix"):], publication.Applied, maximum, nil,
	)
	if err != nil || result.Fence.Applied != publication.Applied {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	value, err := OpenPointReadBatchValue(result.Data)
	if err != nil || value.Count() != len(reads) {
		t.Fatalf("opened count=%d err=%v", value.Count(), err)
	}
	for index, want := range [][]byte{baseValue, globalValue, nil} {
		raw, found, ok := value.Lookup(index)
		if !ok || found != (index != 2) || !bytes.Equal(raw, want) {
			t.Fatalf("lookup %d raw=%q found=%v ok=%v", index, raw, found, ok)
		}
	}
	if _, _, ok := value.Lookup(len(reads)); ok {
		t.Fatal("out-of-range lookup succeeded")
	}
	short, err := fixture.machine.PointReadBatchInto(
		packed[len("prefix"):], publication.Applied,
		4+(len(reads)+7)/8+len(reads)*4+len(baseValue)+len(globalValue)-1, nil,
	)
	if !errors.Is(err, ErrReadBufferBound) || short.Data != nil {
		t.Fatalf("short maximum error=%v", err)
	}
}

func TestPointReadBatchRejectsWholeCutOnAnyActiveIntent(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	visibleKey := []byte("visible")
	visibleValue := []byte(`{"email":"visible"}`)
	command := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: visibleKey, Value: visibleValue,
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	blockedKey := []byte("blocked")
	stage := transactionParticipantStageCommand(t, fixture, transactionCodecID(199),
		[]replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: blockedKey,
			Value: []byte(`{"email":"blocked"}`),
		}}}},
	)
	applyTransactionCommand(t, fixture.machine, 4, stage)
	packed, err := AppendPointReadBatch(nil, []PointRead{
		{Relation: 1, Key: visibleKey}, {Relation: 1, Key: blockedKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	maximum := 1 << 20
	result, err := fixture.machine.PointReadBatchInto(packed, 4, maximum, []byte("caller"))
	if !errors.Is(err, ErrTransactionIntentActive) || result.Data != nil {
		t.Fatalf("partial result=%x err=%v", result.Data, err)
	}
}

func BenchmarkPointReadBatchIntoFourMisses(b *testing.B) {
	fixture := newRelationBundleFixtureWithCollectionOptions(
		b, true, false,
		durable.Options{MaxDocumentBytes: 64 << 10},
		durable.Options{MaxDocumentBytes: 64 << 10},
	)
	reads := []PointRead{
		{Relation: 1, Key: []byte("a")}, {Relation: 2, Key: []byte("b")},
		{Relation: 1, Key: []byte("c")}, {Relation: 2, Key: []byte("d")},
	}
	packed, err := AppendPointReadBatch(nil, reads)
	if err != nil {
		b.Fatal(err)
	}
	maximum := 1 << 20
	result, err := fixture.machine.PointReadBatchInto(packed, 2, maximum, nil)
	if err != nil {
		b.Fatal(err)
	}
	scratch := result.Data[:0]
	b.ReportAllocs()
	for b.Loop() {
		result, err = fixture.machine.PointReadBatchInto(packed, 2, maximum, scratch)
		if err != nil {
			b.Fatal(err)
		}
		scratch = result.Data[:0]
	}
}
