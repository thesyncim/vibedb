package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/splitcapture"
)

// Activate through the durable command path, then reconstruct the capture on
// reopen before sealing. This must not rely on a process-local proof object.
func activateLaggedCapture(t *testing.T, p *Partitioner, f sourceCaptureFixture) (*Partitioner, sourceCaptureFixture, *SourceCapture) {
	t.Helper()
	f.binding.RoutingVersion--
	f.binding.RouteGeneration = 17
	before := TailSourceCoordinates{OwnershipEpoch: f.binding.OwnershipEpoch, RoutingVersion: f.binding.RoutingVersion, RouteGeneration: 17}
	bound, err := p.BindSourceFence(before, 20)
	if err != nil {
		t.Fatal(err)
	}
	if bound.GeometryDigest() != p.Digest() || bound.Digest() == p.Digest() {
		t.Fatal("applied fence not bound independently of geometry")
	}
	spec, err := AppendPortablePartitioner(nil, bound)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenPortablePartitioner(spec)
	if err != nil || opened.sourceCoordinates != before || opened.targetGeneration != 20 || opened.Digest() != bound.Digest() {
		t.Fatalf("portable fence: %v", err)
	}
	for _, original := range [][]byte{[]byte(`"target_generation":20`), []byte(`"RouteGeneration":17`)} {
		changed := bytes.Clone(spec)
		pos := bytes.Index(changed, original)
		if pos < 0 {
			t.Fatalf("missing portable field %s in %s", original, spec)
		}
		changed[pos+len(original)-1]++
		if _, err := OpenPortablePartitioner(changed); err == nil {
			t.Fatal("tampered applied fence accepted")
		}
	}
	var active *SourceCapture
	f.options.TransitionCaptureTarget = replicatedstate.TransitionCaptureTarget{Name: replicatedstate.TransitionCaptureCollectionName, Collection: f.capture}
	f.options.TransitionCaptureFactory = func(a replicatedstate.SplitCaptureActivation) (replicatedstate.TransitionCapture, error) {
		p, err := OpenPortablePartitioner(a.Command.Spec)
		if err != nil {
			return nil, err
		}
		active, err = NewSourceCapture(p, replicatedstate.TransitionCaptureCollectionName, f.capture)
		return active, err
	}
	open := func() {
		f.machine, err = replicatedstate.Open(f.binding, f.bootstrap, f.system, replicatedstate.UserCollection{Name: "docs", Target: f.user}, f.log, f.options)
		if err != nil {
			t.Fatal(err)
		}
	}
	open()
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := f.openSession(t, 2, []byte("tenant"), sourceCaptureID(99), replication.CommandAuthorityTopology)
	cut, err := f.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	state := cut.State()
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := splitcapture.AppendCommand(nil, splitcapture.Command{
		Operation: [32]byte{1}, PlanDigest: [32]byte{2}, PartitionerDigest: bound.Digest(), RelationManifestDigest: [32]byte{3}, LineageDigest: [32]byte{4},
		BindingDigest: replicatedstate.SplitCaptureBindingDigest(state.Binding), PriorEntryDigest: state.LastEntryDigest, PriorDataChainDigest: state.DataChainDigest,
		PriorApplied: state.Applied, PriorTerm: state.LastTerm, SourceGeneration: 17, SchemaGeneration: f.binding.SchemaGeneration, Spec: spec,
	})
	if err != nil {
		t.Fatal(err)
	}
	b := f.binding
	raw, err := replication.AppendCommand(nil, replication.Command{
		Kind: replication.CommandSplitCaptureActivate, AuthorityClass: replication.CommandAuthorityTopology,
		ClusterID: b.ClusterID, ClusterIncarnation: b.ClusterIncarnation, TopologyRecoveryEpoch: b.TopologyRecoveryEpoch,
		Distribution: b.Distribution, Shard: b.Shard, AllocationGeneration: b.AllocationGeneration, ShardIncarnation: b.ShardIncarnation, GroupID: b.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: b.ActivePolicyGeneration, ProtectionEpoch: b.ProtectionEpoch, OwnershipEpoch: b.OwnershipEpoch,
		SchemaGeneration: b.SchemaGeneration, RoutingVersion: b.RoutingVersion, RouteGeneration: b.RouteGeneration,
		Tenant: []byte("tenant"), ClientID: sourceCaptureID(99), ClientEpoch: epoch, ClientSequence: 2, Fingerprint: sha256.Sum256(body), SplitCaptureActivation: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.machine.ApplyNormal(sourceCaptureMeta(3), raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.machine.SplitCaptureActivation(); !ok || active == nil {
		t.Fatal("capture activation not durable")
	}
	active = nil
	open()
	if active == nil {
		t.Fatal("capture not reconstructed on reopen")
	}
	return bound, f, active
}

func reopenLaggedCapture(t *testing.T, f *sourceCaptureFixture) *SourceCapture {
	t.Helper()
	var active *SourceCapture
	f.options.TransitionCaptureFactory = func(a replicatedstate.SplitCaptureActivation) (replicatedstate.TransitionCapture, error) {
		p, err := OpenPortablePartitioner(a.Command.Spec)
		if err != nil {
			return nil, err
		}
		active, err = NewSourceCapture(p, replicatedstate.TransitionCaptureCollectionName, f.capture)
		return active, err
	}
	var err error
	f.machine, err = replicatedstate.Open(f.binding, f.bootstrap, f.system, replicatedstate.UserCollection{Name: "docs", Target: f.user}, f.log, f.options)
	if err != nil || active == nil {
		t.Fatalf("reopen capture: %v", err)
	}
	return active
}
