package replication

import (
	"bytes"
	"testing"
)

func TestConflictValueFramingAndMutationRoundTrip(t *testing.T) {
	value, err := AppendConflictValue(nil, []byte(`{"n":1}`), []byte{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(value); n++ {
		// The action grammar belongs to the validator, but an empty action or
		// a truncated candidate must fail at the envelope boundary.
		if n > len(value)-2 {
			break
		}
		if _, _, ok := OpenConflictValue(value[:n]); ok {
			t.Fatalf("truncated prefix %d accepted", n)
		}
	}
	candidate, program, ok := OpenConflictValue(value)
	if !ok || string(candidate) != `{"n":1}` || !bytes.Equal(program, []byte{1, 0}) || cap(candidate) != len(candidate) || cap(program) != len(program) {
		t.Fatal("conflict value lost framing")
	}
	mutation := Mutation{Kind: MutationPutConflict, Key: []byte("a"), Value: value}
	if err := validateMutation(mutation); err != nil {
		t.Fatal(err)
	}
	command := testCommand()
	command.Batches[0].Mutations = []Mutation{mutation}
	view, err := OpenCommand(encodeCommand(t, command))
	if err != nil {
		t.Fatal(err)
	}
	batches := view.RelationBatches()
	if !batches.Next() {
		t.Fatal("missing conflict relation batch")
	}
	iterator := batches.Batch().Mutations()
	if !iterator.Next() || iterator.Mutation().Kind != MutationPutConflict || !bytes.Equal(iterator.Mutation().Value, value) || iterator.Next() {
		t.Fatal("conflict mutation did not round trip")
	}
	mutation.ExpectedValueLength = 1
	if err := validateMutation(mutation); err == nil {
		t.Fatal("accepted unrelated digest condition")
	}
}
