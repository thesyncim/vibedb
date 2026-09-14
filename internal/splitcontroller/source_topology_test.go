package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type sourceTopologyFixture struct {
	plan      *Plan
	catalog   *gateway.Snapshot
	record    gateway.ReplicatedOperationRecord
	directory gateway.NodeDirectoryCut
	peer      rafttransport.PeerBinding
	request   sourceTopologyRequest
	observed  Observation
	command   replication.Command
	route     gateway.ReplicatedRoute
}

func newSourceTopologyFixture(t *testing.T) sourceTopologyFixture {
	t.Helper()
	plan, catalog := testRF3AdmissionPlan(t)
	route, ok := catalog.ResolveReplicatedRoute(plan.source.Distribution, plan.source.Shard, nil)
	if !ok {
		t.Fatal("source route")
	}
	state := testSourceState(plan)
	state.ReplicaSetVersion = route.Command.ReplicaSetVersion
	endpoint := route.Replicas[0]
	identity := raftmember.RuntimeIdentity{Group: route.Group, Distribution: string(route.Distribution), Shard: string(route.Shard), AllocationGeneration: route.AllocationGeneration,
		MemberID: endpoint.Member, StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, RelationManifestDigest: route.Command.RelationManifestDigest}
	serving := raftservice.ServingState{Identity: identity, Command: route.Command, Status: testLeaderStatus(state)}
	observed := Observation{Catalog: catalog, SourceState: state, SourceStatus: serving.Status, SourceServing: serving, SourceNode: endpoint.Node}
	action := Action{Kind: ActionStartCapture, CatalogGeneration: catalog.Generation()}
	intent, err := AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	requests, execution, err := buildRemoteExecution(plan, observed, action, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	cursor := replicatedActionCursor(action)
	record := gateway.ReplicatedOperationRecord{ID: [32]byte(plan.operation), Kind: gateway.ReplicatedOperationSplit, State: gateway.ReplicatedOperationRunning,
		Revision: 3, CatalogGeneration: catalog.Generation(), Intent: intent, IntentDigest: sha256.Sum256(intent), Cursor: cursor, Proof: replicatedActionProof([32]byte(plan.operation), cursor),
		ExecutionRevision: 3, Execution: execution}
	if !record.Valid() {
		t.Fatal("record")
	}
	key := replication.Digest{7}
	node := gateway.NodeRecord{NodeID: endpoint.Node, Incarnation: endpoint.NodeIncarnation, ServiceKeyDigest: key,
		DataEndpoint: "physical-data", NativeEndpoint: "physical-native", ControlEndpoint: "physical-control", DataAddress: "127.0.0.1:10", NativeAddress: "127.0.0.1:11", ControlAddress: "127.0.0.1:12",
		FailureDomain: "zone-a", Roles: gateway.NodeRoleStorage, Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: catalog.Generation()}
	directory := gateway.NodeDirectoryCut{Revision: 1, Digest: replication.Digest{8}, CatalogGeneration: catalog.Generation(), Nodes: []gateway.NodeRecord{node}}
	peer := rafttransport.PeerBinding{Identity: rafttransport.PeerIdentity{Node: endpoint.Node, TrustDomain: rafttransport.TrustDomain{ClusterID: route.Group.ClusterID, ClusterIncarnation: route.Group.ClusterIncarnation}}, ServiceKeyDigest: [32]byte(key)}
	command := replication.Command{Kind: replication.CommandSessionOpen, AuthorityClass: replication.CommandAuthorityTopology,
		ClusterID: route.Group.ClusterID, ClusterIncarnation: route.Group.ClusterIncarnation, TopologyRecoveryEpoch: route.Group.TopologyRecoveryEpoch,
		Distribution: string(route.Distribution), Shard: string(route.Shard), AllocationGeneration: route.AllocationGeneration, ShardIncarnation: route.Group.ShardIncarnation, GroupID: route.Group.GroupID,
		ReplicaSetVersion: route.Command.ReplicaSetVersion, ActivePolicyGeneration: route.Command.ActivePolicyGeneration, ProtectionEpoch: route.Command.ProtectionEpoch, OwnershipEpoch: route.Command.OwnershipEpoch,
		SchemaGeneration: route.Command.SchemaGeneration, RoutingVersion: route.Command.RoutingVersion, RouteGeneration: route.Command.RouteGeneration,
		Tenant: SourceCaptureTenant(plan.operation), ClientID: SourceCaptureClientID(plan.operation), ClientSequence: 1, NextDeadlineUnixNano: math.MaxInt64, Fingerprint: replication.Digest{1}}
	home := sha256.Sum256(append([]byte("vibedb/split-capture/retry-home\x00"), command.ClientID[:]...))
	copy(command.RetryHome[:], home[:])
	raw, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	request := sourceTopologyRequest{Step: SourceTopologyStep{Operation: plan.operation, PlanDigest: record.IntentDigest, Step: requests[0].Step, ExecutionRevision: 3}, Source: identity,
		Destination: sourceTopologyMember{Node: endpoint.Node, Member: endpoint.Member, Store: endpoint.StoreID, Incarnation: endpoint.NodeIncarnation},
		Native: &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedPropose, Capability: serviceauthz.CapabilityTopology,
			Authority: serviceauthz.Authority{Node: endpoint.Node, Generation: 1}, Command: raw,
			Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration, Command: route.Command, MemberID: endpoint.Member, StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, Term: state.LastTerm}}}
	return sourceTopologyFixture{plan, catalog, record, directory, peer, request, observed, command, route}
}

func TestSourceTopologyAuthorityRejectsUnrelatedOrStaleRequests(t *testing.T) {
	fixture := newSourceTopologyFixture(t)
	if _, _, err := authorizeSourceTopology(fixture.record, fixture.catalog, fixture.directory, fixture.peer, fixture.request); err != nil {
		t.Fatal("valid request:", err)
	}
	cases := []struct {
		name   string
		change func(*sourceTopologyFixture)
	}{
		{"wrong TLS caller", func(f *sourceTopologyFixture) { f.peer.Identity.Node[0]++ }},
		{"wrong TLS key", func(f *sourceTopologyFixture) { f.peer.ServiceKeyDigest[0]++ }},
		{"old wave", func(f *sourceTopologyFixture) { f.request.Step.ExecutionRevision++ }},
		{"wrong step", func(f *sourceTopologyFixture) { f.request.Step.Step[0]++ }},
		{"wrong source store", func(f *sourceTopologyFixture) { f.request.Source.StoreID[0]++ }},
		{"wrong source group", func(f *sourceTopologyFixture) { f.request.Source.Group.GroupID[0]++ }},
		{"unrelated destination", func(f *sourceTopologyFixture) { f.request.Destination.Node[0]++ }},
		{"closed wave", func(f *sourceTopologyFixture) { f.record.ExecutionSettled = true }},
		{"completed operation", func(f *sourceTopologyFixture) { f.record.State = gateway.ReplicatedOperationComplete }},
		{"joining node", func(f *sourceTopologyFixture) { f.directory.Nodes[0].Lifecycle = gateway.NodeJoining }},
		{"foreign capability", func(f *sourceTopologyFixture) { f.request.Native.Capability = serviceauthz.CapabilitySchema }},
		{"unrelated session", func(f *sourceTopologyFixture) { f.command.ClientID[0]++ }},
		{"unrelated tenant", func(f *sourceTopologyFixture) { f.command.Tenant = []byte("catalog") }},
		{"wrong retry home", func(f *sourceTopologyFixture) { f.command.RetryHome[0]++ }},
		{"lease widening", func(f *sourceTopologyFixture) { f.command.NextDeadlineUnixNano = 7 }},
		{"session retire during activation", func(f *sourceTopologyFixture) {
			f.command.Kind = replication.CommandSessionRetire
			f.command.ClientEpoch = 1
			f.command.ClientSequence = 2
			f.command.NextDeadlineUnixNano = 0
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := fixture
			f.directory.Nodes = append([]gateway.NodeRecord(nil), fixture.directory.Nodes...)
			native := *fixture.request.Native
			f.request.Native = &native
			test.change(&f)
			raw, err := replication.AppendCommand(nil, f.command)
			if err != nil {
				t.Fatal(err)
			}
			f.request.Native.Command = raw
			if _, _, err := authorizeSourceTopology(f.record, f.catalog, f.directory, f.peer, f.request); !errors.Is(err, ErrSourceTopology) {
				t.Fatalf("accepted %s: %v", test.name, err)
			}
		})
	}
}

func TestSourceTopologyCaptureGeometryAndPhase(t *testing.T) {
	f := newSourceTopologyFixture(t)
	requests, err := openRemoteExecution(f.record, Action{Kind: ActionStartCapture, CatalogGeneration: f.catalog.Generation()})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := openRemoteStepPayload(requests[0])
	if err != nil {
		t.Fatal(err)
	}
	body, err := f.plan.AppendSourceCaptureActivation(nil, f.observed.SourceState)
	if err != nil {
		t.Fatal(err)
	}
	f.command.Kind = replication.CommandSplitCaptureActivate
	f.command.ClientEpoch = 1
	f.command.ClientSequence = 2
	f.command.NextDeadlineUnixNano = 0
	f.command.SplitCaptureActivation = body
	view := func(c replication.Command) replication.CommandView {
		t.Helper()
		raw, err := replication.AppendCommand(nil, c)
		if err != nil {
			t.Fatal(err)
		}
		v, err := replication.OpenCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if !validSourceTopologyCommand(f.plan, f.observed, payload, ActionStartCapture, view(f.command)) {
		t.Fatal("valid capture denied")
	}
	if validSourceTopologyCommand(f.plan, f.observed, payload, ActionBuildArtifacts, view(f.command)) {
		t.Fatal("capture outside committed phase")
	}
	other := *f.plan
	other.operation[0]++
	wrong, err := other.AppendSourceCaptureActivation(nil, f.observed.SourceState)
	if err != nil {
		t.Fatal(err)
	}
	f.command.SplitCaptureActivation = wrong
	if validSourceTopologyCommand(f.plan, f.observed, payload, ActionStartCapture, view(f.command)) {
		t.Fatal("foreign capture geometry")
	}
}

func TestSourceTopologyCommittedWaveSurvivesUnrelatedCatalogGeneration(t *testing.T) {
	f := newSourceTopologyFixture(t)
	raw, err := gateway.AppendSnapshotDocument(nil, f.catalog)
	if err != nil {
		t.Fatal(err)
	}
	needle := []byte(`"generation":` + strconv.FormatUint(f.catalog.Generation(), 10))
	// The canonical document carries the catalog generation at the document
	// root and in nested topology metadata. Advance both copies so the new
	// head remains a valid snapshot while retaining the exact route metadata
	// certified by the operation.
	updated := bytes.ReplaceAll(raw, needle, []byte(`"generation":`+strconv.FormatUint(f.catalog.Generation()+7, 10)))
	if bytes.Equal(updated, raw) {
		t.Fatal("missing canonical root generation")
	}
	raw = updated
	newer, err := gateway.OpenSnapshotDocument(raw)
	if err != nil {
		t.Fatal(err)
	}
	if newer.Generation() != f.catalog.Generation()+7 {
		t.Fatalf("head generation=%d", newer.Generation())
	}
	if _, _, err := authorizeSourceTopology(f.record, newer, f.directory, f.peer, f.request); err != nil {
		t.Fatalf("unrelated head stranded committed wave: %v", err)
	}
	requests, err := openRemoteExecution(f.record, Action{Kind: ActionStartCapture, CatalogGeneration: f.catalog.Generation()})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := openRemoteStepPayload(requests[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*Observation)
	}{
		{"schema", func(observed *Observation) { observed.SourceServing.Command.SchemaGeneration++ }},
		{"membership", func(observed *Observation) { observed.SourceServing.Command.ReplicaSetVersion++ }},
		{"ownership", func(observed *Observation) { observed.SourceServing.Command.OwnershipEpoch++ }},
		{"manifest", func(observed *Observation) { observed.SourceServing.Command.RelationManifestDigest[0]++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := f.observed
			test.change(&observed)
			if _, err := openSourceTopologyPlan(f.record.Intent, newer, payload, observed); !errors.Is(err, ErrSourceTopology) {
				t.Fatalf("changed affected source accepted: %v", err)
			}
		})
	}
}

func TestSourceTopologyWirePreservesCommandAndRejectsUnboundedFrames(t *testing.T) {
	f := newSourceTopologyFixture(t)
	var wire bytes.Buffer
	nonce := [16]byte{1}
	if err := writeSourceTopologyRequest(&wire, f.request, nonce); err != nil {
		t.Fatal(err)
	}
	decoded, got, err := readSourceTopologyRequest(bytes.NewReader(wire.Bytes()))
	if err != nil || got != nonce || !bytes.Equal(decoded.Native.Command, f.request.Native.Command) {
		t.Fatalf("roundtrip: %v", err)
	}
	if decoded.Step != f.request.Step || decoded.Source != f.request.Source || decoded.Destination != f.request.Destination {
		t.Fatal("authority changed")
	}
	for _, offset := range []int{24, 28} {
		bad := append([]byte(nil), wire.Bytes()[:32]...)
		binary.BigEndian.PutUint32(bad[offset:offset+4], math.MaxUint32)
		if _, _, err := readSourceTopologyRequest(bytes.NewReader(bad)); !errors.Is(err, ErrSourceTopology) {
			t.Fatalf("unbounded frame: %v", err)
		}
	}
	var reply bytes.Buffer
	if err := writeSourceTopologyReply(&reply, nonce, nil, ErrSourceTopology); err != nil {
		t.Fatal(err)
	}
	if _, err := readSourceTopologyReply(bytes.NewReader(reply.Bytes()), [16]byte{2}); !errors.Is(err, ErrSourceTopology) {
		t.Fatal("nonce substitution")
	}
}

type sourceTopologyTestCatalog struct {
	fixture       sourceTopologyFixture
	reads         int
	closeOnReread bool
}

func (c *sourceTopologyTestCatalog) ReadOperation(context.Context, [32]byte) (gateway.ReplicatedOperationRecord, error) {
	c.reads++
	record := c.fixture.record
	if c.closeOnReread && c.reads > 1 {
		record.ExecutionSettled = true
	}
	return record, nil
}
func (c *sourceTopologyTestCatalog) ReadReplicatedCatalogHead(context.Context) (*gateway.Snapshot, replication.Digest, error) {
	return c.fixture.catalog, replication.Digest{1}, nil
}
func (c *sourceTopologyTestCatalog) ReadNodeDirectoryCut(context.Context) (gateway.NodeDirectoryCut, error) {
	return c.fixture.directory, nil
}

type sourceTopologyTestNative struct {
	proposals   int
	incarnation uint64
	forwarded   []byte
	endpoint    gateway.ReplicatedEndpoint
	authority   serviceauthz.Authority
}

func (n *sourceTopologyTestNative) ProbeReplicated(_ context.Context, route gateway.ReplicatedRoute, endpoint gateway.ReplicatedEndpoint, _ serviceauthz.Capability) (*shardservice.ReplicatedResponse, error) {
	incarnation := endpoint.NodeIncarnation
	if n.incarnation > 0 {
		incarnation = n.incarnation
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: shardservice.ReplicatedMemberState{Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration, Command: route.Command, MemberID: endpoint.Member, StoreID: endpoint.StoreID, NodeIncarnation: incarnation, Term: 7}, LeaderID: 1, Applied: 41, Commit: 41}}, nil
}
func (n *sourceTopologyTestNative) DoReplicated(_ context.Context, endpoint gateway.ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	n.proposals++
	n.endpoint = endpoint
	n.authority = request.Authority
	n.forwarded = append([]byte(nil), request.Command...)
	return nil, nil
}

func TestSourceTopologyServiceRevalidatesWaveAndCurrentProcess(t *testing.T) {
	for _, test := range []struct {
		name             string
		closed           bool
		claimed, current uint64
		want             bool
	}{{"ordinary", false, 1, 1, true}, {"closed during probe", true, 1, 1, false}, {"restarted process", false, 5, 5, true}, {"old process", false, 1, 5, false}, {"invented process", false, 6, 5, false}} {
		t.Run(test.name, func(t *testing.T) {
			f := newSourceTopologyFixture(t)
			f.request.Source.NodeIncarnation = test.claimed
			f.request.Destination.Incarnation = test.claimed
			f.request.Native.Fence.NodeIncarnation = test.claimed
			catalog := &sourceTopologyTestCatalog{fixture: f, closeOnReread: test.closed}
			native := &sourceTopologyTestNative{incarnation: test.current}
			gate, err := serviceauthz.NewServiceDirectoryGate(serviceauthz.ServiceDirectoryCut{CatalogGeneration: f.catalog.Generation(), Revision: 1, TrustDomain: f.peer.Identity.TrustDomain, PolicyGeneration: 1, Bindings: []serviceauthz.ServiceBinding{{Principal: f.peer.Identity.Node, PhysicalNode: f.peer.Identity.Node, PhysicalIncarnation: 1, KeyDigest: f.peer.ServiceKeyDigest, Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceActive}}})
			if err != nil {
				t.Fatal(err)
			}
			deadline := func() time.Time { return time.Now().Add(time.Second) }
			authority := serviceauthz.Authority{Node: rafttransport.NodeID{99}, Generation: 1}
			service, err := NewSourceTopologyService(SourceTopologyServiceOptions{Catalog: catalog, Directory: gate, TrustDomain: f.peer.Identity.TrustDomain, Native: native, Authority: authority, ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.execute(t.Context(), f.peer, f.request)
			if test.want {
				if err != nil || native.proposals != 1 || native.authority != authority || !bytes.Equal(native.forwarded, f.request.Native.Command) || native.endpoint.Address != f.route.Replicas[0].Address {
					t.Fatalf("forward=%d err=%v", native.proposals, err)
				}
			} else if err == nil || native.proposals != 0 {
				t.Fatalf("unauthorized forward=%d err=%v", native.proposals, err)
			}
		})
	}
}

type sourceTopologyTestPeer struct {
	net.Conn
	identity rafttransport.PeerIdentity
	key      [32]byte
}

func (c *sourceTopologyTestPeer) PeerIdentity() rafttransport.PeerIdentity { return c.identity }
func (c *sourceTopologyTestPeer) PeerKeyDigest() [32]byte                  { return c.key }
func (c *sourceTopologyTestPeer) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficGatewayControl
}

type sourceTopologyTestOpener struct {
	calls int
	trust rafttransport.TrustDomain
	done  chan struct{}
}

func (o *sourceTopologyTestOpener) OpenBootstrapGatewayControl(_ context.Context, seed nodecontrol.BootstrapGatewaySeed) (rafttransport.PeerConnection, error) {
	o.calls++
	client, server := net.Pipe()
	go func() { defer close(o.done); defer server.Close(); _, _, _ = readSourceTopologyRequest(server) }()
	return &sourceTopologyTestPeer{Conn: client, identity: rafttransport.PeerIdentity{Node: seed.NodeID, TrustDomain: o.trust}, key: [32]byte(seed.SPKIPinDigest)}, nil
}

func TestSourceTopologyLostReplyDoesNotFailOverAndEraseAmbiguity(t *testing.T) {
	f := newSourceTopologyFixture(t)
	opener := &sourceTopologyTestOpener{trust: f.peer.Identity.TrustDomain, done: make(chan struct{})}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	client, err := NewSourceTopologyClient(SourceTopologyClientOptions{Opener: opener, TrustDomain: f.peer.Identity.TrustDomain, Source: f.request.Source, Step: f.request.Step,
		Seeds: []nodecontrol.BootstrapGatewaySeed{{NodeID: rafttransport.NodeID{1}, Incarnation: 1, ControlAddress: "127.0.0.1:1", SPKIPinDigest: replication.Digest{1}}, {NodeID: rafttransport.NodeID{2}, Incarnation: 1, ControlAddress: "127.0.0.1:2", SPKIPinDigest: replication.Digest{2}}}, ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DoReplicated(t.Context(), f.route.Replicas[0], f.request.Native)
	<-opener.done
	if !errors.Is(err, io.EOF) || opener.calls != 1 {
		t.Fatalf("lost reply erased by seed failover: calls=%d err=%v", opener.calls, err)
	}
}

// Reuse the real captured-seal fixture so the proxy's destructive authority
// test has a certified cutover, not a synthetic certificate-shaped value.
func testSourceTopologyPruneAuthorityWithCertificate(t *testing.T, plan *Plan, cutover rangesplit.CutoverCertificate) {
	t.Helper()
	published := plan.targetSnapshotForTest(t)
	certificate := testRetainedPruneCertificate(t, plan, published, cutover)
	digest, err := gateway.CatalogSnapshotDigest(published)
	if err != nil {
		t.Fatal(err)
	}
	payload := remoteStepPayload{Catalog: plan.next, CatalogDigest: digest}
	observed := Observation{Catalog: published, Certificate: &cutover}
	if !validSourceTopologyPruneCertificate(plan, payload, observed, certificate) {
		t.Fatal("valid committed prune certificate rejected")
	}
	payload.CatalogDigest[0]++
	if validSourceTopologyPruneCertificate(plan, payload, observed, certificate) {
		t.Fatal("prune borrowed another catalog drain")
	}
	payload.CatalogDigest = digest
	other := *plan
	other.children[other.retained].Range.Start[0]++
	if validSourceTopologyPruneCertificate(&other, payload, observed, certificate) {
		t.Fatal("prune retained geometry widened")
	}
}
