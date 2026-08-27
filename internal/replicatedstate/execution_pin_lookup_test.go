package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestExecutionPinLookupUsesBoundedCallerStorage(t *testing.T) {
	fixture := newMachineFixture(t)
	client := id128(0xd0)
	binding := executionPinTestBinding()
	pin, err := executionpin.DerivePinID(binding)
	if err != nil {
		t.Fatal(err)
	}
	command := executionPinCommand(fixture.binding, client, 2, 2, executionpin.Command{
		Operation: executionpin.OperationAcquire, Binding: binding, PinID: pin,
		AuthorityNode: executionpin.ID(client), AuthorityGeneration: 7,
		NextController: executionpin.ID(id128(0xe0)), NextControllerEpoch: 1, NextLeaseSpan: 97,
	})
	// The grammar bound is checked before any durable state lookup, even when
	// the buffer could fit a shorter identity's particular result by accident.
	short := bytes.Repeat([]byte{0xa5}, MaxExecutionPinCompletionEnvelopeBytes-1)
	lookup, err := fixture.machine.LookupCompletionInto(command, short[:7])
	if !errors.Is(err, ErrCompletionBufferSmall) || len(lookup.Bytes) != 0 ||
		!bytes.Equal(short, bytes.Repeat([]byte{0xa5}, len(short))) {
		t.Fatalf("short execution-pin storage was not rejected before lookup: %+v, %v", lookup, err)
	}
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, executionPinSessionPrototype(fixture.binding, client))
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	want, err := fixture.machine.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	storage := make([]byte, MaxExecutionPinCompletionEnvelopeBytes)
	for range 2 {
		lookup, err := fixture.machine.LookupCompletionInto(command, storage[:7])
		if err != nil || len(lookup.Bytes) == 0 || &lookup.Bytes[0] != &storage[0] ||
			!bytes.Equal(lookup.Bytes, want.Bytes) || lookup.AppliedSequence != 3 {
			t.Fatalf("execution-pin lookup did not reuse exact caller storage: %+v, %v", lookup, err)
		}
		completion, err := replication.OpenCompletion(lookup.Bytes)
		if err != nil || completion.ResultFormat != ResultFormatExecutionPin {
			t.Fatalf("execution-pin completion: %+v, %v", completion, err)
		}
		clear(storage)
	}
}
