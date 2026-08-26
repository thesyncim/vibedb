package replication

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
)

func testExecutionPinNested(t testing.TB) []byte {
	t.Helper()
	digest := func(seed byte) executionpin.Digest {
		var value executionpin.Digest
		value[0], value[31] = seed, seed^0xff
		return value
	}
	id := func(seed byte) executionpin.ID {
		var value executionpin.ID
		value[0], value[15] = seed, seed^0xff
		return value
	}
	binding := executionpin.Binding{
		RequestKeyDigest: digest(1), RequestDigest: digest(2),
		CatalogGeneration: 3, SchemaGeneration: 4,
		SchemaManifestDigest: digest(5), SchemaCertificateDigest: digest(6),
		LogicalGroup: id(7), LogicalRange: id(8), MutationDigest: digest(9),
	}
	pin, err := executionpin.DerivePinID(binding)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := executionpin.AppendCommand(nil, executionpin.Command{
		Operation: executionpin.OperationAcquire, Binding: binding, PinID: pin,
		AuthorityNode: id(11), AuthorityGeneration: 12,
		NextController: id(10), NextControllerEpoch: 11, NextLeaseSpan: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestExecutionPinCommandKindAuthorityAndNestedBytesAreFrozen(t *testing.T) {
	if CommandExecutionPin != 10 || commandWireExecutionPin != 10 ||
		CommandAuthorityExecutionPin != 3 {
		t.Fatalf("frozen assignments kind=%d wire=%d authority=%d",
			CommandExecutionPin, commandWireExecutionPin, CommandAuthorityExecutionPin)
	}
	command := testCommand()
	command.Kind = CommandExecutionPin
	command.AuthorityClass = CommandAuthorityExecutionPin
	command.Transaction = nil
	command.Batches = nil
	command.ExecutionPin = testExecutionPinNested(t)
	encoded, err := AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenCommand(encoded)
	if err != nil || view.Kind() != CommandExecutionPin ||
		view.AuthorityClass != CommandAuthorityExecutionPin ||
		!bytes.Equal(view.ExecutionPinBytes(), command.ExecutionPin) ||
		view.RelationCount() != 0 || view.MutationCount() != 0 {
		t.Fatalf("execution-pin view = %+v, %v", view, err)
	}
	opened, err := view.OpenExecutionPin()
	if err != nil || opened.Operation != executionpin.OperationAcquire {
		t.Fatalf("nested = %+v, %v", opened, err)
	}
	authorityDigest, ok := ExecutionPinAuthorityDigest(view)
	if !ok || authorityDigest == (Digest{}) {
		t.Fatalf("execution-pin authority digest = %x,%v", authorityDigest, ok)
	}
	ordinaryBytes, ordinaryErr := AppendCommand(nil, testCommand())
	if ordinaryErr != nil {
		t.Fatal(ordinaryErr)
	}
	ordinary, ordinaryErr := OpenCommand(ordinaryBytes)
	if ordinaryErr != nil {
		t.Fatal(ordinaryErr)
	}
	if _, ok = ExecutionPinAuthorityDigest(ordinary); ok {
		t.Fatal("ordinary data command exposed an execution-pin authority digest")
	}
	wrong := command
	wrong.AuthorityClass = CommandAuthorityTopology
	if _, err := AppendCommand(nil, wrong); err == nil {
		t.Fatal("topology authority encoded an execution-pin command")
	}
	wrong = command
	wrong.Kind = CommandMutationBatch
	if _, err := AppendCommand(nil, wrong); err == nil {
		t.Fatal("ordinary mutation encoded a hidden execution-pin body")
	}
}
