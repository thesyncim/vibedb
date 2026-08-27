package splitcontroller

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/splitcapture"
)

func TestSourceCaptureAuthorityIsStableAndDistinctFromPrune(t *testing.T) {
	operation := OperationID{1, 2, 3}
	client := SourceCaptureClientID(operation)
	if client == ([16]byte{}) || client != SourceCaptureClientID(operation) ||
		client == RetainedPruneClientID(operation) {
		t.Fatalf("capture client=%x prune=%x", client, RetainedPruneClientID(operation))
	}
	tenant := SourceCaptureTenant(operation)
	if len(tenant) == 0 || !bytes.Equal(tenant, SourceCaptureTenant(operation)) ||
		bytes.Equal(tenant, RetainedPruneTenant(operation)) {
		t.Fatalf("capture tenant=%q prune=%q", tenant, RetainedPruneTenant(operation))
	}
	changed := operation
	changed[0]++
	if SourceCaptureClientID(changed) == client ||
		bytes.Equal(SourceCaptureTenant(changed), tenant) {
		t.Fatal("operation identity did not separate capture authority")
	}
}

func TestSourceCaptureAuthorityKeepsPendingCutAndRejectsSubstitution(t *testing.T) {
	pending := splitcapture.Command{Operation: [32]byte{1}, PlanDigest: [32]byte{2},
		PartitionerDigest: [32]byte{3}, RelationManifestDigest: [32]byte{4}, LineageDigest: [32]byte{5},
		BindingDigest: [32]byte{6}, PriorEntryDigest: [32]byte{7}, PriorDataChainDigest: [32]byte{8},
		PriorApplied: 9, PriorTerm: 10, SourceGeneration: 11, SchemaGeneration: 12, Spec: []byte("partitioner")}
	newer := pending
	newer.PriorApplied++
	newer.PriorTerm++
	newer.PriorEntryDigest[0]++
	newer.PriorDataChainDigest[0]++
	if !sameSourceCaptureAuthority(pending, newer) {
		t.Fatal("new observation prevented exact pending retry")
	}
	for _, mutate := range []func(*splitcapture.Command){
		func(c *splitcapture.Command) { c.Operation[0]++ },
		func(c *splitcapture.Command) { c.PlanDigest[0]++ },
		func(c *splitcapture.Command) { c.PartitionerDigest[0]++ },
		func(c *splitcapture.Command) { c.RelationManifestDigest[0]++ },
		func(c *splitcapture.Command) { c.LineageDigest[0]++ },
		func(c *splitcapture.Command) { c.BindingDigest[0]++ },
		func(c *splitcapture.Command) { c.SourceGeneration++ },
		func(c *splitcapture.Command) { c.SchemaGeneration++ },
		func(c *splitcapture.Command) { c.Spec = []byte("other") },
	} {
		candidate := newer
		mutate(&candidate)
		if sameSourceCaptureAuthority(pending, candidate) {
			t.Fatal("substituted activation authority accepted")
		}
	}
}
