package rafttransport

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
)

var errLifecycleFrameHandled = errors.New("stream lifecycle test: accepted frame handled")

func TestOrdinaryReceiverRevalidatesPeerBindingAfterFrameRead(t *testing.T) {
	fixture := newStreamLifecycleFixture(t)
	local, remote := net.Pipe()
	defer remote.Close()
	setLifecycleWriteDeadline(t, remote)
	connection := newLifecyclePeerConnection(local, fixture.registry, fixture.remote, fixture.oldKey)
	var deliveries atomic.Uint32
	receiver := newStreamTestReceiver(t, fixture.registry, func(context.Context, Inbound) error {
		deliveries.Add(1)
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- receiver.Serve(context.Background(), connection) }()

	if err := writeFull(remote, streamRecordHeader(len(fixture.frame))); err != nil {
		t.Fatalf("write stream header: %v", err)
	}
	waitForStreamBodyRead(t, connection.bodyReadStarted)

	fixture.retireAndRotatePeer(t)
	fixture.installGroup(t)
	if err := writeFull(remote, fixture.frame); err != nil {
		t.Fatalf("write stream body: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrPeerKeyMismatch) {
			t.Fatalf("receiver error = %v, want ErrPeerKeyMismatch", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not reject stale-key frame promptly")
	}
	if got := deliveries.Load(); got != 0 {
		t.Fatalf("delivered frames = %d, want 0", got)
	}
	select {
	case <-connection.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not closed after stale-key rejection")
	}
}

func TestOrdinaryReceiverAcceptsCurrentKeyReconnectAfterRotation(t *testing.T) {
	fixture := newStreamLifecycleFixture(t)
	fixture.retireAndRotatePeer(t)
	fixture.installGroup(t)

	local, remote := net.Pipe()
	defer remote.Close()
	setLifecycleWriteDeadline(t, remote)
	connection := newLifecyclePeerConnection(local, fixture.registry, fixture.remote, fixture.newKey)
	delivered := make(chan Inbound, 1)
	receiver := newStreamTestReceiver(t, fixture.registry, func(_ context.Context, inbound Inbound) error {
		delivered <- inbound
		return errLifecycleFrameHandled
	})
	done := make(chan error, 1)
	go func() { done <- receiver.Serve(context.Background(), connection) }()

	if err := writeFull(remote, framedStreamRecord(t, fixture.frame)); err != nil {
		t.Fatalf("write current-key record: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, errLifecycleFrameHandled) {
			t.Fatalf("current-key reconnect error = %v, want accepted-frame sentinel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("current-key frame handler did not stop the receiver")
	}
	select {
	case inbound := <-delivered:
		if inbound.Group != fixture.group || inbound.Message.GetFrom() != fixture.remoteMemberID {
			t.Fatalf("current-key delivery = %+v", inbound)
		}
	default:
		t.Fatal("current-key frame handler did not record delivery")
	}
}

func TestOrdinaryReceiverAcceptsSameKeyRevisionRefreshDuringFrameRead(t *testing.T) {
	fixture := newStreamLifecycleFixture(t)
	fixture.installGroup(t)

	local, remote := net.Pipe()
	defer remote.Close()
	setLifecycleWriteDeadline(t, remote)
	connection := newLifecyclePeerConnection(local, fixture.registry, fixture.remote, fixture.oldKey)
	delivered := make(chan Inbound, 1)
	receiver := newStreamTestReceiver(t, fixture.registry, func(context.Context, Inbound) error {
		delivered <- Inbound{}
		return errLifecycleFrameHandled
	})
	done := make(chan error, 1)
	go func() { done <- receiver.Serve(context.Background(), connection) }()

	if err := writeFull(remote, streamRecordHeader(len(fixture.frame))); err != nil {
		t.Fatalf("write stream header: %v", err)
	}
	waitForStreamBodyRead(t, connection.bodyReadStarted)
	fixture.refreshPeerRevision(t)
	if err := writeFull(remote, fixture.frame); err != nil {
		t.Fatalf("write stream body: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, errLifecycleFrameHandled) {
			t.Fatalf("same-key revision refresh error = %v, want accepted-frame sentinel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("same-key revision refresh handler did not stop the receiver")
	}
	select {
	case <-delivered:
	default:
		t.Fatal("same-key revision refresh frame handler did not record delivery")
	}
}

func TestOrdinaryReceiverCancelsPartialBodyReadPromptly(t *testing.T) {
	fixture := newStreamLifecycleFixture(t)
	fixture.installGroup(t)

	local, remote := net.Pipe()
	defer remote.Close()
	setLifecycleWriteDeadline(t, remote)
	connection := newLifecyclePeerConnection(local, fixture.registry, fixture.remote, fixture.oldKey)
	var deliveries atomic.Uint32
	receiver := newStreamTestReceiver(t, fixture.registry, func(context.Context, Inbound) error {
		deliveries.Add(1)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- receiver.Serve(ctx, connection) }()

	if err := writeFull(remote, streamRecordHeader(len(fixture.frame))); err != nil {
		t.Fatalf("write stream header: %v", err)
	}
	waitForStreamBodyRead(t, connection.bodyReadStarted)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled receiver error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not cancel a blocked partial body read promptly")
	}
	if got := deliveries.Load(); got != 0 {
		t.Fatalf("partial frame deliveries = %d, want 0", got)
	}
}

type streamLifecycleFixture struct {
	registry       *StaticRegistry
	remote         NodeID
	group          raftmember.GroupKey
	members        []Member
	remoteMemberID uint64
	frame          []byte
	oldKey         [sha256.Size]byte
	newKey         [sha256.Size]byte
}

func newStreamLifecycleFixture(t *testing.T) streamLifecycleFixture {
	t.Helper()
	group := testGroup(221)
	local := testNode(1)
	remote := testNode(2)
	domain := TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	registry, err := NewEmptyRegistry(local, domain, Limits{MaxGroups: 1, MaxMembers: 2, MaxPeers: 2})
	if err != nil {
		t.Fatalf("NewEmptyRegistry: %v", err)
	}
	oldKey := sha256.Sum256([]byte("stream-lifecycle-old-service-key"))
	newKey := sha256.Sum256([]byte("stream-lifecycle-new-service-key"))
	if err := registry.EnrollPeer(streamLifecycleEnrollment(registry, remote, 1, 1, oldKey, "old"),
		EnrollmentVerifierFunc(allowEnrollment)); err != nil {
		t.Fatalf("initial EnrollPeer: %v", err)
	}
	members := []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 11, Node: local, Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 12, Node: remote, Role: MemberVoter},
	}
	sender, err := NewStaticRegistry(remote, members, Limits{MaxGroups: 1, MaxMembers: 2, MaxPeers: 2})
	if err != nil {
		t.Fatalf("NewStaticRegistry sender: %v", err)
	}
	frame := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgHeartbeat, 12, 11))
	return streamLifecycleFixture{
		registry: registry, remote: remote, group: group, members: members,
		remoteMemberID: 12, frame: frame, oldKey: oldKey, newKey: newKey,
	}
}

func (fixture streamLifecycleFixture) installGroup(t *testing.T) {
	t.Helper()
	if err := fixture.registry.InstallGroup(fixture.members, func(publish func()) error {
		publish()
		return nil
	}); err != nil {
		t.Fatalf("InstallGroup: %v", err)
	}
}

func (fixture streamLifecycleFixture) retireAndRotatePeer(t *testing.T) {
	t.Helper()
	peer, err := fixture.registry.PhysicalPeer(fixture.remote)
	if err != nil {
		t.Fatalf("PhysicalPeer before retirement: %v", err)
	}
	proof := PeerRetirementProof{
		NodeID: fixture.remote, Incarnation: peer.Incarnation, Revision: peer.Revision,
		DirectoryRevision: fixture.registry.PeerDirectoryRevision(),
		DirectoryDigest:   fixture.registry.PeerDirectoryDigest(),
	}
	if err := fixture.registry.RetirePhysicalPeer(proof); err != nil {
		t.Fatalf("RetirePhysicalPeer: %v", err)
	}
	if err := fixture.registry.EnrollPeer(
		streamLifecycleEnrollment(fixture.registry, fixture.remote, 2, 2, fixture.newKey, "new"),
		EnrollmentVerifierFunc(allowEnrollment),
	); err != nil {
		t.Fatalf("replacement EnrollPeer: %v", err)
	}
}

func (fixture streamLifecycleFixture) refreshPeerRevision(t *testing.T) {
	t.Helper()
	peer, err := fixture.registry.PhysicalPeer(fixture.remote)
	if err != nil {
		t.Fatalf("PhysicalPeer before refresh: %v", err)
	}
	peer.Revision++
	peer.EnrollmentDigest = [sha256.Size]byte{}
	intent := EnrollmentIntent{
		Digest: sha256.Sum256([]byte("stream-lifecycle-same-key-revision-refresh")),
		Domain: fixture.registry.TrustDomain(), Peer: peer,
		DirectoryRevision: fixture.registry.PeerDirectoryRevision(),
	}
	if err := fixture.registry.EnrollPeer(intent, EnrollmentVerifierFunc(allowEnrollment)); err != nil {
		t.Fatalf("same-key revision EnrollPeer: %v", err)
	}
}

func streamLifecycleEnrollment(
	registry *StaticRegistry,
	peer NodeID,
	incarnation uint64,
	revision uint64,
	key [sha256.Size]byte,
	tag string,
) EnrollmentIntent {
	return EnrollmentIntent{
		Digest: sha256.Sum256([]byte("stream-lifecycle-enrollment/" + tag)),
		Domain: registry.TrustDomain(),
		Peer: PhysicalPeer{
			NodeID: peer, TrustDomain: registry.TrustDomain(), Incarnation: incarnation,
			Revision: revision, ServiceKeyDigest: key, Endpoint: "127.0.0.1:25021", State: PeerEnrolled,
		},
		DirectoryRevision: registry.PeerDirectoryRevision(),
	}
}

func newLifecyclePeerConnection(
	connection net.Conn,
	registry *StaticRegistry,
	peer NodeID,
	key [sha256.Size]byte,
) *lifecyclePeerConnection {
	return &lifecyclePeerConnection{
		Conn:      connection,
		identity:  PeerIdentity{TrustDomain: registry.TrustDomain(), Node: peer},
		keyDigest: key, class: TrafficOrdinary,
		bodyReadStarted: make(chan struct{}), closed: make(chan struct{}),
	}
}

type lifecyclePeerConnection struct {
	net.Conn
	identity        PeerIdentity
	keyDigest       [sha256.Size]byte
	class           TrafficClass
	bodyReadStarted chan struct{}
	closed          chan struct{}
	bodyReadOnce    sync.Once
	closeOnce       sync.Once
	closeErr        error
}

func (connection *lifecyclePeerConnection) PeerIdentity() PeerIdentity { return connection.identity }
func (connection *lifecyclePeerConnection) PeerKeyDigest() [sha256.Size]byte {
	return connection.keyDigest
}
func (connection *lifecyclePeerConnection) TrafficClass() TrafficClass { return connection.class }
func (connection *lifecyclePeerConnection) Read(buffer []byte) (int, error) {
	if len(buffer) > StreamRecordHeaderBytes {
		connection.bodyReadOnce.Do(func() { close(connection.bodyReadStarted) })
	}
	return connection.Conn.Read(buffer)
}
func (connection *lifecyclePeerConnection) Close() error {
	connection.closeOnce.Do(func() {
		connection.closeErr = connection.Conn.Close()
		close(connection.closed)
	})
	return connection.closeErr
}

func streamRecordHeader(frameBytes int) []byte {
	var header [StreamRecordHeaderBytes]byte
	binary.BigEndian.PutUint32(header[:], uint32(frameBytes))
	return header[:]
}

func framedStreamRecord(t *testing.T, frame []byte) []byte {
	t.Helper()
	record, err := appendStreamRecord(nil, frame)
	if err != nil {
		t.Fatalf("append stream record: %v", err)
	}
	return record
}

func waitForStreamBodyRead(t *testing.T, bodyReadStarted <-chan struct{}) {
	t.Helper()
	select {
	case <-bodyReadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not begin the frame body read")
	}
}

func setLifecycleWriteDeadline(t *testing.T, connection net.Conn) {
	t.Helper()
	if err := connection.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set pipe write deadline: %v", err)
	}
}

var _ PeerConnection = (*lifecyclePeerConnection)(nil)
var _ net.Conn = (*lifecyclePeerConnection)(nil)
