package replication

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestJSONInt64DeltaDescriptorRoundTripAndOwnership(t *testing.T) {
	prefix := []byte("prefix")
	descriptor, err := AppendJSONInt64Delta(prefix, "score", -42)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(descriptor[:len(prefix)], prefix) {
		t.Fatalf("append changed prefix: %q", descriptor)
	}
	column, delta, ok := OpenJSONInt64Delta(descriptor[len(prefix):])
	if !ok || string(column) != "score" || delta != -42 {
		t.Fatalf("opened descriptor column=%q delta=%d ok=%v", column, delta, ok)
	}
	descriptor[len(prefix)+jsonInt64DeltaHeaderBytes] = 'X'
	if string(column) != "Xcore" {
		t.Fatalf("opened column does not borrow descriptor: %q", column)
	}

	for _, value := range []int64{0, 1, -1, -42, 1 << 62, -1 << 62, -1 << 63} {
		raw, err := AppendJSONInt64Delta(nil, "v", value)
		if err != nil {
			t.Fatalf("delta %d: %v", value, err)
		}
		_, got, ok := OpenJSONInt64Delta(raw)
		if !ok || got != value {
			t.Fatalf("delta %d opened as %d ok=%v", value, got, ok)
		}
	}
}

func TestJSONInt64DeltaDescriptorRejectsMalformedMetadata(t *testing.T) {
	valid, err := AppendJSONInt64Delta(nil, "score", 1)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"short", func(value []byte) []byte { return value[:len(value)-1] }},
		{"reserved", func(value []byte) []byte {
			binary.LittleEndian.PutUint16(value[6:8], 1)
			return value
		}},
		{"length", func(value []byte) []byte {
			binary.LittleEndian.PutUint16(value[4:6], 1)
			return value
		}},
		{"magic", func(value []byte) []byte { value[0] = 'X'; return value }},
		{"invalid utf8", func(value []byte) []byte {
			value[len(value)-1] = 0xff
			return value
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := bytes.Clone(valid)
			value = test.mutate(value)
			if _, _, ok := OpenJSONInt64Delta(value); ok {
				t.Fatal("malformed descriptor was accepted")
			}
		})
	}
}

func TestJSONInt64DeltaUsesCanonicalCommandAndTransactionFraming(t *testing.T) {
	descriptor, err := AppendJSONInt64Delta(nil, "score", -7)
	if err != nil {
		t.Fatal(err)
	}
	mutation := Mutation{Kind: MutationJSONInt64Delta, Key: []byte("row"), Value: descriptor}
	command := testCommand()
	command.Batches = []RelationMutationBatch{{Relation: 1, Mutations: []Mutation{mutation}}}
	raw, err := AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	relations := opened.RelationBatches()
	if !relations.Next() {
		t.Fatal("missing relation")
	}
	mutations := relations.Batch().Mutations()
	if !mutations.Next() {
		t.Fatal("missing mutation")
	}
	view := mutations.Mutation()
	if view.Kind != MutationJSONInt64Delta || !bytes.Equal(view.Key, mutation.Key) ||
		!bytes.Equal(view.Value, descriptor) || len(view.Compare) != 0 ||
		view.ExpectedValueLength != 0 || view.ExpectedValueDigest != (Digest{}) {
		t.Fatalf("opened delta=%+v", view)
	}

	layout, err := MeasureTransactionMutationBytes(command.Batches)
	if err != nil {
		t.Fatal(err)
	}
	detached, gotLayout, err := AppendTransactionMutationBytes(nil, command.Batches)
	if err != nil || gotLayout != layout {
		t.Fatalf("detached layout=%+v want=%+v err=%v", gotLayout, layout, err)
	}
	detachedView, err := OpenTransactionMutationBytes(detached, layout)
	if err != nil {
		t.Fatal(err)
	}
	iterator := detachedView.RelationBatches()
	if !iterator.Next() {
		t.Fatal("detached relation missing")
	}
	mutationIterator := iterator.Batch().Mutations()
	if !mutationIterator.Next() || mutationIterator.Mutation().Kind != MutationJSONInt64Delta ||
		!bytes.Equal(mutationIterator.Mutation().Value, descriptor) || mutationIterator.Next() {
		t.Fatalf("detached delta iterator invalid")
	}
}
