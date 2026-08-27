package replication

import (
	"bytes"
	"github.com/thesyncim/vibedb/internal/splitcapture"
	"testing"
)

func testSplitCaptureCommand() Command {
	c := testSessionRetireCommand()
	c.Kind = CommandSplitCaptureActivate
	c.AuthorityClass = CommandAuthorityTopology
	nested, _ := splitcapture.AppendCommand(nil, splitcapture.Command{Operation: [32]byte{1}, PlanDigest: [32]byte{2}, PartitionerDigest: [32]byte{3}, RelationManifestDigest: [32]byte{4}, LineageDigest: [32]byte{5}, BindingDigest: [32]byte{6}, PriorEntryDigest: [32]byte{7}, PriorDataChainDigest: [32]byte{8}, PriorApplied: 8, PriorTerm: 9, SourceGeneration: 10, SchemaGeneration: 11, Spec: []byte("spec")})
	c.SplitCaptureActivation = nested
	return c
}

func TestSplitCaptureCommandRoundTrip(t *testing.T) {
	command := testSplitCaptureCommand()
	raw := encodeCommand(t, command)
	if raw[10] != 12 {
		t.Fatalf("wire kind=%d", raw[10])
	}
	view, err := OpenCommand(raw)
	if err != nil || view.Kind() != CommandSplitCaptureActivate || !bytes.Equal(view.SplitCaptureActivationBytes(), command.SplitCaptureActivation) {
		t.Fatalf("view=%+v err=%v", view, err)
	}
	if _, err := view.OpenSplitCaptureActivation(); err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if _, err := OpenCommand(raw); err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("allocs=%v", got)
	}
}
