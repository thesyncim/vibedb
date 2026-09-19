package gatewayruntime

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestRefreshLiveControlDirectoryCancellationWhileWaitingForRound(t *testing.T) {
	runtime := new(Runtime)
	runtime.controlDirectoryRefreshMu.Lock()
	defer runtime.controlDirectoryRefreshMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := runtime.refreshLiveControlDirectory(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("refresh waiting on prior round err=%v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("refresh cancellation took %s while waiting on prior round", elapsed)
	}
}

// refreshDedupRuntimeCutReader is a complete source-cut reader whose current
// cut can be advanced by the test between refresh ticks.  The runtime must
// derive the service cut, certified catalog image, and receiver roster from
// each value returned here as one epoch.
type refreshDedupRuntimeCutReader struct {
	gateway.DirectoryReader
	mu  sync.Mutex
	cut gateway.FrontendDrainRuntimeCut
}

func (reader *refreshDedupRuntimeCutReader) ReadFrontendDrainRuntimeCut(
	context.Context,
) (gateway.FrontendDrainRuntimeCut, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.cut, nil
}

func (reader *refreshDedupRuntimeCutReader) set(cut gateway.FrontendDrainRuntimeCut) {
	reader.mu.Lock()
	reader.cut = cut
	reader.mu.Unlock()
}

// refreshDedupPreparedAckOpener terminates the real shard-control wire for
// each physical receiver and counts opens.  It can fail a complete round to
// prove that an unfinished publication does not leave a successful digest
// cached for the next retry.
type refreshDedupPreparedAckOpener struct {
	mu           sync.Mutex
	profile      *rafttransport.PeerTLS
	receiverKey  [32]byte
	receiverNode rafttransport.NodeID
	refuseNode   rafttransport.NodeID
	fail         bool
	calls        int
}

func (opener *refreshDedupPreparedAckOpener) OpenShardControlEndpoint(
	ctx context.Context, endpoint gateway.ReplicatedEndpoint,
) (rafttransport.PeerConnection, error) {
	opener.mu.Lock()
	opener.calls++
	fail := opener.fail
	profile := opener.profile
	refuseNode := opener.refuseNode
	receiverNode := endpoint.Node
	receiverKey := opener.receiverKey
	if receiverNode != opener.receiverNode {
		receiverKey = [32]byte{100 + receiverNode[0]}
	}
	opener.mu.Unlock()
	if refuseNode != (rafttransport.NodeID{}) && endpoint.Node == refuseNode {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	if fail {
		return nil, errors.New("injected prepared-ack receiver failure")
	}
	clientRaw, serverRaw := net.Pipe()
	client := &preparedAckWirePeer{
		Conn: clientRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: receiverNode},
		key:  receiverKey, class: rafttransport.TrafficShardControl,
	}
	server := &preparedAckWirePeer{
		Conn: serverRaw,
		peer: rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: profile.LocalIdentity().Node},
		key:  profile.LocalServiceKeyDigest(), class: rafttransport.TrafficShardControl,
	}
	service, err := shardservice.NewFrontendDrainPreparedAckService(shardservice.FrontendDrainPreparedAckServiceOptions{
		Reader: preparedAckServiceReaderTest{}, Installer: &preparedAckGateInstallerTest{},
		TrustDomain: profile.LocalIdentity().TrustDomain,
		Authorize: func(peer rafttransport.PeerIdentity, request frontenddrain.PreparedAckRequest) bool {
			return peer.Node == profile.LocalIdentity().Node &&
				request.SourcePrincipal == profile.LocalIdentity().Node &&
				request.SourcePrincipalKeyDigest == profile.LocalServiceKeyDigest() &&
				request.ReceiverNode == receiverNode &&
				request.ReceiverServiceKeyDigest == receiverKey
		},
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		_ = clientRaw.Close()
		_ = serverRaw.Close()
		return nil, err
	}
	go func() { _ = service.Serve(ctx, server) }()
	return client, nil
}

func (opener *refreshDedupPreparedAckOpener) callsObserved() int {
	opener.mu.Lock()
	defer opener.mu.Unlock()
	return opener.calls
}

func (opener *refreshDedupPreparedAckOpener) setFail(fail bool) {
	opener.mu.Lock()
	opener.fail = fail
	opener.mu.Unlock()
}

func newRefreshDedupRuntime(
	t *testing.T, source gateway.FrontendDrainRuntimeCut, reader *refreshDedupRuntimeCutReader,
	opener *refreshDedupPreparedAckOpener, profile *rafttransport.PeerTLS, policy *serviceauthz.Policy,
) *Runtime {
	t.Helper()
	snapshot, err := frontendDrainRuntimeCutSnapshot(source)
	if err != nil {
		t.Fatalf("source snapshot: %v", err)
	}
	directory, err := gateway.NewReplicatedControlDirectory(snapshot)
	if err != nil {
		t.Fatalf("control directory: %v", err)
	}
	serviceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		t.Context(), source, profile, policy.Generation(),
	)
	if err != nil {
		t.Fatalf("service cut: %v", err)
	}
	serviceDirectory, err := serviceauthz.NewServiceDirectoryGate(serviceCut)
	if err != nil {
		t.Fatalf("service directory: %v", err)
	}
	return &Runtime{
		config: Config{
			ControlDirectory: reader, TLSProfile: profile, Authorization: policy,
		},
		controlDirectory: directory, serviceDirectory: serviceDirectory,
		preparedAckPhysicalOpener: opener,
	}
}

func refreshDedupExpandedSource(t *testing.T) (
	source gateway.FrontendDrainRuntimeCut, profile *rafttransport.PeerTLS, policy *serviceauthz.Policy, grantNode gateway.NodeRecord,
) {
	t.Helper()
	_, _, source, _, _ = frontendDrainSourceTestFixture(t)
	profiles, _ := runtimeControlTLSFixture(t, []serviceauthz.Entry{{
		Node: rafttransport.NodeID{9}, Capabilities: serviceauthz.AllCapabilities,
	}})
	profile = profiles[0]
	// Use a gateway principal distinct from route member three.  This keeps
	// the complete source projection's physical and gateway bindings uniquely
	// addressable while retaining the catalog route's canonical RF3 members.
	source.Nodes.Nodes[0].Gateway.NodeID = profile.LocalIdentity().Node
	source.Nodes.Nodes[0].Gateway.ServiceKeyDigest = replication.Digest(profile.LocalServiceKeyDigest())
	for index := range source.ContinuationGrants {
		grant := source.ContinuationGrants[index]
		grant.GatewayServiceID = profile.LocalIdentity().Node
		grant.PeerKeyDigest = profile.LocalServiceKeyDigest()
		grant, reboundErr := serviceauthz.NewCommittedFrontendContinuationGrant(grant)
		if reboundErr != nil {
			t.Fatalf("rebind continuation grant: %v", reboundErr)
		}
		source.ContinuationGrants[index] = grant
	}
	grantNode = source.Nodes.Nodes[0]
	// The fixture's catalog route has the canonical RF3 members one, two, and
	// three while its continuation grant names physical receiver four.  Build
	// the complete serving roster so this test exercises both route receivers
	// and the active storage-only receiver discovered from the service cut.
	routeNodes := []gateway.NodeRecord{
		preparedAckRosterNode(1, 21, gateway.NodeActive, gateway.NodeRoleStorage),
		preparedAckRosterNode(2, 22, gateway.NodeActive, gateway.NodeRoleStorage),
		preparedAckRosterNode(3, 23, gateway.NodeActive, gateway.NodeRoleStorage),
		grantNode,
	}
	routeNodes[0].DataEndpoint, routeNodes[0].NativeEndpoint, routeNodes[0].ControlEndpoint = "one", "one-native", "one-control"
	routeNodes[0].DataAddress, routeNodes[0].NativeAddress, routeNodes[0].ControlAddress = "127.0.0.1:7001", "127.0.0.1:9101", "127.0.0.1:7201"
	routeNodes[1].DataEndpoint, routeNodes[1].NativeEndpoint, routeNodes[1].ControlEndpoint = "two", "two-native", "two-control"
	routeNodes[1].DataAddress, routeNodes[1].NativeAddress, routeNodes[1].ControlAddress = "127.0.0.1:7002", "127.0.0.1:7102", "127.0.0.1:7202"
	routeNodes[2].DataEndpoint, routeNodes[2].NativeEndpoint, routeNodes[2].ControlEndpoint = "three", "three-native", "three-control"
	routeNodes[2].DataAddress, routeNodes[2].NativeAddress, routeNodes[2].ControlAddress = "127.0.0.1:7003", "127.0.0.1:7103", "127.0.0.1:7203"
	source.Nodes.Nodes = routeNodes
	source.Nodes.Revision = 30
	if !source.Nodes.Valid() {
		t.Fatalf("expanded source node cut invalid: rev=%d digest=%x nodes=%+v", source.Nodes.Revision, source.Nodes.Digest, source.Nodes.Nodes)
	}
	var err error
	policy, err = serviceauthz.NewPolicy(1, []serviceauthz.Entry{{
		Node: profile.LocalIdentity().Node, Capabilities: serviceauthz.AllCapabilities,
	}})
	if err != nil {
		t.Fatalf("test policy: %v", err)
	}
	return source, profile, policy, grantNode
}

func TestRefreshLiveControlDirectoryFanoutDedupAndInvalidation(t *testing.T) {
	source, profile, policy, node := refreshDedupExpandedSource(t)
	reader := &refreshDedupRuntimeCutReader{cut: source}
	opener := &refreshDedupPreparedAckOpener{
		profile: profile, receiverNode: node.NodeID, receiverKey: [32]byte(node.ServiceKeyDigest),
	}
	runtime := newRefreshDedupRuntime(t, source, reader, opener, profile, policy)

	if err := runtime.refreshLiveControlDirectory(t.Context()); err != nil {
		t.Fatalf("initial publication: %v", err)
	}
	if got := opener.callsObserved(); got != 4 {
		t.Fatalf("initial receiver fanout=%d, want route RF3 plus storage-only receiver (4)", got)
	}
	if err := runtime.refreshLiveControlDirectory(t.Context()); err != nil {
		t.Fatalf("unchanged refresh: %v", err)
	}
	if got := opener.callsObserved(); got != 4 {
		t.Fatalf("unchanged-tick receiver fanout=%d, want no additional opens (4 total)", got)
	}

	changedHead := source
	changedHead.CatalogHeadDigest[0]++
	reader.set(changedHead)
	if err := runtime.refreshLiveControlDirectory(t.Context()); err != nil {
		t.Fatalf("catalog-head publication: %v", err)
	}
	if got := opener.callsObserved(); got != 8 {
		t.Fatalf("one-head-change fanout=%d, want one additional four-receiver round (8 total)", got)
	}

	changedRoster := changedHead
	changedRoster.Nodes.Nodes = append([]gateway.NodeRecord(nil), source.Nodes.Nodes...)
	changedRoster.Nodes.Nodes = append(changedRoster.Nodes.Nodes,
		preparedAckRosterNode(5, 25, gateway.NodeActive, gateway.NodeRoleStorage))
	changedRoster.Nodes.Revision++
	changedRoster.Nodes.Digest = replication.Digest{39}
	changedRoster.ServiceDirectoryRevision++
	reader.set(changedRoster)
	if err := runtime.refreshLiveControlDirectory(t.Context()); err != nil {
		t.Fatalf("roster publication: %v", err)
	}
	if got := opener.callsObserved(); got != 13 {
		t.Fatalf("roster-change fanout=%d, want five-receiver round after node addition (13 total)", got)
	}

	// A new Runtime models a restart: the prior successful digest is not
	// carried across lifecycle ownership, so the same certified cut is sent
	// once to the receiver again.
	restartedOpener := &refreshDedupPreparedAckOpener{
		profile: profile, receiverNode: node.NodeID, receiverKey: [32]byte(node.ServiceKeyDigest),
	}
	restarted := newRefreshDedupRuntime(t, changedRoster, reader, restartedOpener, profile, policy)
	if err := restarted.refreshLiveControlDirectory(t.Context()); err != nil {
		t.Fatalf("restart publication: %v", err)
	}
	if got := restartedOpener.callsObserved(); got != 5 {
		t.Fatalf("restart fanout=%d, want one five-receiver round", got)
	}

	// A failed round invalidates the in-memory success marker.  Retrying after
	// the receiver recovers must re-run the physical barrier for the same cut.
	failedOpener := &refreshDedupPreparedAckOpener{
		profile: profile, receiverNode: node.NodeID, receiverKey: [32]byte(node.ServiceKeyDigest), fail: true,
	}
	failed := newRefreshDedupRuntime(t, changedRoster, reader, failedOpener, profile, policy)
	failureErr := failed.refreshLiveControlDirectory(t.Context())
	if failureErr == nil {
		t.Fatalf("failed publication unexpectedly succeeded (receiver opens=%d)", failedOpener.callsObserved())
	}
	if failed.publishedFrontendDrainCutValid {
		t.Fatal("failed publication cached a successful cut digest")
	}
	failedOpener.setFail(false)
	if err := failed.refreshLiveControlDirectory(t.Context()); err != nil {
		t.Fatalf("retry after failed publication: %v", err)
	}
	if got := failedOpener.callsObserved(); got != 6 {
		t.Fatalf("retry fanout=%d, want one failed receiver plus recovered five-receiver round", got)
	}
}

func TestFrontendDrainPreparedAckReceiverUnreachable(t *testing.T) {
	if !frontendDrainPreparedAckReceiverUnreachable(&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}) {
		t.Fatal("connection refused must be unreachable")
	}
	if frontendDrainPreparedAckReceiverUnreachable(syscall.ECONNRESET) ||
		frontendDrainPreparedAckReceiverUnreachable(io.EOF) {
		t.Fatal("reset/EOF is a protocol close, not an unreachable peer")
	}
	if frontendDrainPreparedAckReceiverUnreachable(errors.New("injected prepared-ack receiver failure")) {
		t.Fatal("injected protocol failure must not be skipped")
	}
}

func TestRefreshLiveControlDirectorySkipsUnreachableRecoveryReceiver(t *testing.T) {
	source, profile, policy, node := refreshDedupExpandedSource(t)
	reader := &refreshDedupRuntimeCutReader{cut: source}
	opener := &refreshDedupPreparedAckOpener{
		profile: profile, receiverNode: node.NodeID, receiverKey: [32]byte(node.ServiceKeyDigest),
		refuseNode: rafttransport.NodeID{1},
	}
	runtime := newRefreshDedupRuntime(t, source, reader, opener, profile, policy)
	if err := runtime.refreshLiveControlDirectory(t.Context()); err != nil {
		t.Fatalf("publication with one unreachable receiver: %v", err)
	}
	if !runtime.publishedFrontendDrainCutValid {
		t.Fatal("reachable receiver barrier did not cache the cut")
	}
	if got := opener.callsObserved(); got != 4 {
		t.Fatalf("fanout=%d, want one four-receiver round including the refused peer", got)
	}
}
