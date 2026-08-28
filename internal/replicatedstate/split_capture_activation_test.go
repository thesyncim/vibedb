package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/splitcapture"
)

func TestSplitCaptureActivationExactReplayAndDivergence(t *testing.T) {
	f := newCapturedRelationBundleFixture(t)
	var captures int
	var active *sessionLeaseCapture
	f.machine.options.TransitionCaptureFactory = func(a SplitCaptureActivation) (TransitionCapture, error) {
		captures++
		active = &sessionLeaseCapture{target: TransitionCaptureTarget{Name: TransitionCaptureCollectionName, Collection: f.capture.Collection}}
		return active, nil
	}
	prototype := commandValue(f.binding, 1)
	prototype.AuthorityClass = replication.CommandAuthorityTopology
	prototype.ClientID = id128(99)
	_, _, epoch := applySessionOpen(t, f.machine, 3, prototype)
	state := cloneState(f.machine.state)
	nested := splitcapture.Command{Operation: [32]byte{1}, PlanDigest: [32]byte{2}, PartitionerDigest: [32]byte{3}, RelationManifestDigest: [32]byte{4}, LineageDigest: [32]byte{5}, BindingDigest: SplitCaptureBindingDigest(state.Binding), PriorEntryDigest: state.LastEntryDigest, PriorDataChainDigest: state.DataChainDigest, PriorApplied: state.Applied, PriorTerm: state.LastTerm, SourceGeneration: state.Binding.RouteGeneration, SchemaGeneration: state.Binding.SchemaGeneration, Spec: []byte("portable")}
	body, err := splitcapture.AppendCommand(nil, nested)
	if err != nil {
		t.Fatal(err)
	}
	command := prototype
	command.Kind = replication.CommandSplitCaptureActivate
	command.ClientEpoch = epoch
	command.ClientSequence = 2
	command.Batches = nil
	command.SplitCaptureActivation = body
	command.Fingerprint = sha256.Sum256([]byte("activate"))
	if _, err = f.machine.ApplyNormal(normalMeta(4), encodeCommand(t, command)); err != nil {
		t.Fatal(err)
	}
	completion, _, _ := openMutationCompletion(t, f.machine, encodeCommand(t, command))
	if completion.ResultCode != ResultApplied || completion.AppliedSequence != 4 {
		t.Fatalf("capture activation did not retain its exact completion: %+v", completion)
	}
	witness, ok := f.machine.SplitCaptureActivation()
	if !ok || witness.Applied != 4 || !bytes.Equal(witness.Command.Spec, nested.Spec) || captures != 1 || active == nil {
		t.Fatalf("witness=%+v ok=%v captures=%d", witness, ok, captures)
	}
	retry := command
	retry.ClientSequence = 3
	retry.Fingerprint = sha256.Sum256([]byte("retry"))
	if _, err = f.machine.ApplyNormal(normalMeta(5), encodeCommand(t, retry)); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if captures != 1 {
		t.Fatalf("exact replay capture begins=%d", captures)
	}
	divergentNested := nested
	divergentNested.Operation[0]++
	divergentBody, _ := splitcapture.AppendCommand(nil, divergentNested)
	divergent := command
	divergent.ClientSequence = 4
	divergent.Fingerprint = sha256.Sum256([]byte("divergent"))
	divergent.SplitCaptureActivation = divergentBody
	if _, err = f.machine.ApplyNormal(normalMeta(6), encodeCommand(t, divergent)); err != nil {
		t.Fatalf("divergent apply should settle conflict: %v", err)
	}
	after, _ := f.machine.SplitCaptureActivation()
	if after.Command.Operation != nested.Operation || captures != 1 {
		t.Fatalf("divergent activation replaced witness or capture")
	}
	options := f.options
	var recovered *sessionLeaseCapture
	options.TransitionCaptureFactory = func(a SplitCaptureActivation) (TransitionCapture, error) {
		recovered = &sessionLeaseCapture{target: TransitionCaptureTarget{Name: TransitionCaptureCollectionName, Collection: f.capture.Collection}}
		return recovered, nil
	}
	reopened, err := OpenBundle(f.binding, testBootstrap(), f.system, relationBundleCollections(f.base, f.global, f.index, RelationGlobalIndex), f.log, options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	restored, present := reopened.SplitCaptureActivation()
	if !present || restored.Applied != 4 || recovered == nil || recovered.current != 6 {
		t.Fatalf("reopen witness=%+v present=%v capture=%+v", restored, present, recovered)
	}
	if _, err = reopened.ApplyNormal(normalMeta(7), nil); err != nil || recovered.current != 7 {
		t.Fatalf("post-reopen apply capture=%d err=%v", recovered.current, err)
	}
}

func TestSplitCaptureActivationStaleCutSettlesWithoutPoisoning(t *testing.T) {
	f := newCapturedRelationBundleFixture(t)
	captures := 0
	f.machine.options.TransitionCaptureFactory = func(SplitCaptureActivation) (TransitionCapture, error) {
		captures++
		return &sessionLeaseCapture{target: TransitionCaptureTarget{Name: TransitionCaptureCollectionName, Collection: f.capture.Collection}}, nil
	}
	prototype := commandValue(f.binding, 1)
	prototype.AuthorityClass, prototype.ClientID = replication.CommandAuthorityTopology, id128(99)
	_, _, epoch := applySessionOpen(t, f.machine, 3, prototype)
	state := cloneState(f.machine.state)
	nested := splitcapture.Command{Operation: [32]byte{1}, PlanDigest: [32]byte{2}, PartitionerDigest: [32]byte{3}, RelationManifestDigest: [32]byte{4}, LineageDigest: [32]byte{5}, BindingDigest: SplitCaptureBindingDigest(state.Binding), PriorEntryDigest: state.LastEntryDigest, PriorDataChainDigest: state.DataChainDigest, PriorApplied: state.Applied, PriorTerm: state.LastTerm, SourceGeneration: state.Binding.RouteGeneration, SchemaGeneration: state.Binding.SchemaGeneration, Spec: []byte("portable")}
	command := prototype
	command.Kind, command.ClientEpoch, command.ClientSequence = replication.CommandSplitCaptureActivate, epoch, 2
	command.Batches = nil
	command.SplitCaptureActivation, _ = splitcapture.AppendCommand(nil, nested)
	command.Fingerprint = sha256.Sum256([]byte("stale-capture"))
	raw := encodeCommand(t, command)
	if err := f.machine.AdmitCommand(raw); err != nil {
		t.Fatal(err)
	}
	// An already-admitted proposal can be overtaken by another committed entry.
	if _, err := f.machine.ApplyNormal(normalMeta(4), nil); err != nil {
		t.Fatal(err)
	}
	if err := f.machine.AdmitCommand(raw); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("stale cut admission=%v", err)
	}
	if _, err := f.machine.ApplyNormal(normalMeta(5), raw); err != nil {
		t.Fatalf("stale capture poisoned apply: %v", err)
	}
	completion, _, before := openMutationCompletion(t, f.machine, raw)
	if completion.ResultCode != ResultStaleFence || completion.AppliedSequence != 5 || captures != 0 {
		t.Fatalf("stale capture outcome=%+v captures=%d", completion, captures)
	}
	if _, found := f.machine.SplitCaptureActivation(); found {
		t.Fatal("stale cut activated capture")
	}
	if _, err := f.machine.ApplyNormal(normalMeta(6), raw); err != nil {
		t.Fatal(err)
	}
	_, _, after := openMutationCompletion(t, f.machine, raw)
	if !bytes.Equal(before, after) {
		t.Fatal("stale completion changed on exact retry")
	}
	state = cloneState(f.machine.state)
	nested.PriorApplied, nested.PriorTerm = state.Applied, state.LastTerm
	nested.PriorEntryDigest, nested.PriorDataChainDigest = state.LastEntryDigest, state.DataChainDigest
	command.ClientSequence, command.AckThrough = 3, 2
	command.SplitCaptureActivation, _ = splitcapture.AppendCommand(nil, nested)
	command.Fingerprint = sha256.Sum256([]byte("fresh-capture"))
	raw = encodeCommand(t, command)
	if _, err := f.machine.ApplyNormal(normalMeta(7), raw); err != nil {
		t.Fatal(err)
	}
	completion, _, _ = openMutationCompletion(t, f.machine, raw)
	if completion.ResultCode != ResultApplied || completion.AppliedSequence != 7 || captures != 1 {
		t.Fatalf("fresh capture after refusal=%+v captures=%d", completion, captures)
	}
}
