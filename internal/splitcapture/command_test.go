package splitcapture

import (
	"bytes"
	"errors"
	"testing"
)

func testCommand() Command {
	return Command{Operation: [32]byte{1}, PlanDigest: [32]byte{2}, PartitionerDigest: [32]byte{3}, RelationManifestDigest: [32]byte{4}, LineageDigest: [32]byte{5}, BindingDigest: [32]byte{6}, PriorEntryDigest: [32]byte{7}, PriorDataChainDigest: [32]byte{8}, PriorApplied: 8, PriorTerm: 9, SourceGeneration: 10, SchemaGeneration: 11, Spec: []byte("portable-vibejson-spec")}
}

func TestCommandCanonicalRoundTripAndBounds(t *testing.T) {
	c := testCommand()
	raw, err := AppendCommand(nil, c)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenCommand(raw)
	if err != nil || view.Command.Operation != c.Operation || !bytes.Equal(view.Spec, c.Spec) {
		t.Fatalf("open=%+v err=%v", view, err)
	}
	again, err := AppendCommand(nil, view.Command)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("noncanonical retry err=%v", err)
	}
	for _, candidate := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, err := OpenCommand(candidate); !errors.Is(err, ErrCommand) {
			t.Fatalf("malformed error=%v", err)
		}
	}
	tooBig := c
	tooBig.Spec = make([]byte, MaxPortableSpecBytes+1)
	if _, err := AppendCommand(nil, tooBig); !errors.Is(err, ErrCommand) {
		t.Fatalf("bound error=%v", err)
	}
}

func TestCommandRejectsCorruptionAndZeroWitnesses(t *testing.T) {
	raw, _ := AppendCommand(nil, testCommand())
	for i := range raw {
		candidate := bytes.Clone(raw)
		candidate[i] ^= 1
		if _, err := OpenCommand(candidate); err == nil {
			t.Fatalf("accepted corruption at %d", i)
		}
	}
	c := testCommand()
	c.PriorApplied = 0
	if _, err := AppendCommand(nil, c); !errors.Is(err, ErrCommand) {
		t.Fatalf("zero applied error=%v", err)
	}
}
