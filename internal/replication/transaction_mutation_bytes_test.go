package replication

import (
	"bytes"
	"testing"
)

func TestDetachedTransactionMutationBytesMatchCommandGrammar(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		batches []RelationMutationBatch
	}{
		{
			name: "singleton",
			batches: []RelationMutationBatch{{Relation: 3, Mutations: []Mutation{
				{Kind: MutationPut, Key: []byte("a"), Value: []byte("one")},
				{Kind: MutationDelete, Key: []byte("b")},
			}}},
		},
		{
			name: "multiple_relations_and_compare",
			batches: []RelationMutationBatch{
				{Relation: 2, Mutations: []Mutation{{
					Kind: MutationPutDigestEqual, Key: []byte("a"), Value: []byte("two"),
					ExpectedValueLength: 3, ExpectedValueDigest: Digest{1},
				}}},
				{Relation: 7, Mutations: []Mutation{{Kind: MutationPutAbsent, Key: []byte("z"), Value: []byte("three")}}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			layout, err := MeasureTransactionMutationBytes(test.batches)
			if err != nil {
				t.Fatal(err)
			}
			raw, appended, err := AppendTransactionMutationBytes(nil, test.batches)
			if err != nil || appended != layout || len(raw) != layout.Bytes {
				t.Fatalf("append layout=%+v bytes=%d err=%v; want %+v", appended, len(raw), err, layout)
			}
			command := testCommand()
			command.Kind = CommandMutationBatch
			command.Batches = test.batches
			encoded, err := AppendCommand(nil, command)
			if err != nil {
				t.Fatal(err)
			}
			view, err := OpenCommand(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, view.relationBytes) ||
				layout.MutationCount != view.mutationCount ||
				layout.RelationCount != view.relationCount ||
				layout.InlineRelationID != view.inlineRelationID {
				t.Fatal("detached bytes differ from canonical command relation bytes")
			}
			opened, err := OpenTransactionMutationBytes(raw, layout)
			if err != nil {
				t.Fatal(err)
			}
			wantDigest, err := TransactionMutationDigest(test.batches)
			if err != nil || opened.Digest() != wantDigest {
				t.Fatalf("digest=%x err=%v; want %x", opened.Digest(), err, wantDigest)
			}
		})
	}
}

func TestDetachedTransactionMutationBytesBoundsAndCanonicalRefusal(t *testing.T) {
	t.Parallel()
	invalid := []RelationMutationBatch{{Relation: 1, Mutations: []Mutation{{
		Kind: MutationPut, Key: nil, Value: []byte("value"),
	}}}}
	dst := []byte("prefix")
	got, _, err := AppendTransactionMutationBytes(dst, invalid)
	if err == nil || !bytes.Equal(got, dst) {
		t.Fatalf("invalid append mutated destination: %q err=%v", got, err)
	}
	valid := []RelationMutationBatch{{Relation: 1, Mutations: []Mutation{{
		Kind: MutationPut, Key: []byte("key"), Value: []byte("value"),
	}}}}
	raw, layout, err := AppendTransactionMutationBytes(nil, valid)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(raw)
	corrupt[1] = 1
	if _, err := OpenTransactionMutationBytes(corrupt, layout); err == nil {
		t.Fatal("nonzero reserved mutation byte accepted")
	}
	wrong := layout
	wrong.Bytes++
	if _, err := OpenTransactionMutationBytes(raw, wrong); err == nil {
		t.Fatal("wrong detached byte count accepted")
	}
}

func BenchmarkTransactionMutationDigesterReuse(b *testing.B) {
	batches := []RelationMutationBatch{{Relation: 1, Mutations: []Mutation{{
		Kind: MutationPut, Key: []byte("key"), Value: []byte("value"),
	}}}}
	var digester TransactionMutationDigester
	if _, err := digester.Digest(batches); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := digester.Digest(batches); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTransactionMutationDigesterReuseAllocatesZero(t *testing.T) {
	batches := []RelationMutationBatch{{Relation: 1, Mutations: []Mutation{{
		Kind: MutationPut, Key: []byte("key"), Value: []byte("value"),
	}}}}
	var digester TransactionMutationDigester
	want, err := TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	if got, digestErr := digester.Digest(batches); digestErr != nil || got != want {
		t.Fatalf("reusable digest=%x err=%v want=%x", got, digestErr, want)
	}
	if got := testing.AllocsPerRun(1000, func() {
		digest, digestErr := digester.Digest(batches)
		if digestErr != nil || digest != want {
			panic("reused transaction mutation digest changed")
		}
	}); got != 0 {
		t.Fatalf("reused transaction mutation digest allocations=%v, want 0", got)
	}
}

func BenchmarkAppendTransactionMutationBytesReuse(b *testing.B) {
	batches := []RelationMutationBatch{{Relation: 1, Mutations: []Mutation{{
		Kind: MutationPut, Key: []byte("key"), Value: []byte("value"),
	}}}}
	layout, err := MeasureTransactionMutationBytes(batches)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, 0, layout.Bytes)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := AppendTransactionMutationBytes(dst[:0], batches); err != nil {
			b.Fatal(err)
		}
	}
}
