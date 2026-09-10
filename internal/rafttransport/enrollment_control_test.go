package rafttransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

type enrollmentTestConnection struct {
	net.Conn
	identity PeerIdentity
	key      [sha256.Size]byte
	class    TrafficClass
}

func (connection *enrollmentTestConnection) PeerIdentity() PeerIdentity {
	return connection.identity
}

func (connection *enrollmentTestConnection) PeerKeyDigest() [sha256.Size]byte {
	return connection.key
}

func (connection *enrollmentTestConnection) TrafficClass() TrafficClass {
	return connection.class
}

type enrollmentTestOpener struct {
	open func(context.Context, NodeID) (PeerConnection, error)
}

func (opener enrollmentTestOpener) OpenShardControl(
	ctx context.Context,
	target NodeID,
) (PeerConnection, error) {
	return opener.open(ctx, target)
}

func enrollmentDeadline() time.Time { return time.Now().Add(5 * time.Second) }

func enrollmentTestMembers(group raftmember.GroupKey) []Member {
	return []Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 3, Node: testNode(3), Role: MemberVoter},
	}
}

func enrollmentTestPeers(
	domain TrustDomain,
	nodes ...NodeID,
) []PhysicalPeer {
	peers := make([]PhysicalPeer, len(nodes))
	for index, node := range nodes {
		peers[index] = PhysicalPeer{
			NodeID: node, TrustDomain: domain, Incarnation: 1, Revision: 1,
			ServiceKeyDigest: sha256.Sum256(append([]byte("enrollment-test-key/"), node[:]...)),
			Endpoint:         "127.0.0.1:25001", State: PeerEnrolled,
		}
	}
	return peers
}

func TestEnrollmentControlRoundTripAndRestartReplay(t *testing.T) {
	group := testGroup(221)
	domain := TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	members := enrollmentTestMembers(group)
	source, err := NewStaticRegistryWithDirectory(
		testNode(1), members, enrollmentTestPeers(domain, testNode(1), testNode(2), testNode(3), testNode(4)),
		1, Limits{MaxGroups: 1, MaxMembers: 4, MaxPeers: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewStaticRegistryWithDirectory(
		testNode(2), members, enrollmentTestPeers(domain, testNode(1), testNode(2), testNode(3)),
		1, Limits{MaxGroups: 1, MaxMembers: 4, MaxPeers: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	targetTransport, err := NewOrdinaryTransport(OrdinaryTransportOptions{
		Registry: target, Dialer: ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
			return nil, ErrTransportClosed
		}),
		Queue: QueueLimits{
			PerPeerFrames: 4, PerPeerBytes: 1 << 16,
			GlobalFrames: 8, GlobalBytes: 1 << 17,
		},
		Coalesce: CoalesceLimits{
			MaxFrames: 2, MaxBytes: 1 << 16, RetainedBytes: DefaultRetainedFrameBytes,
		},
		Wait: WaitWithTimer, Backoff: func(uint32) time.Duration { return time.Millisecond },
		MaxReconnectDelay: time.Second, WriteDeadline: enrollmentDeadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer targetTransport.Close()
	roster, ok := target.RosterDigest(group)
	if !ok {
		t.Fatal("target roster digest missing")
	}
	intent := dynamicPeerIntent(target, testNode(4), 221, group, 4, roster)
	intent.Peer.Endpoint = "127.0.0.1:25004"
	intent.Peer.Address = intent.Peer.Endpoint
	intent.Peer.Node = intent.Peer.NodeID

	clientConn, serverConn := net.Pipe()
	clientKey, _ := source.PhysicalPeer(testNode(2))
	serverKey, _ := target.PhysicalPeer(testNode(1))
	client := &enrollmentTestConnection{
		Conn: clientConn, identity: PeerIdentity{TrustDomain: domain, Node: testNode(2)},
		key: clientKey.ServiceKeyDigest, class: TrafficShardControl,
	}
	server := &enrollmentTestConnection{
		Conn: serverConn, identity: PeerIdentity{TrustDomain: domain, Node: testNode(1)},
		key: serverKey.ServiceKeyDigest, class: TrafficShardControl,
	}
	service, err := NewEnrollmentControlService(EnrollmentControlServiceOptions{
		Registry: target, Transport: targetTransport, Verifier: EnrollmentVerifierFunc(allowEnrollment),
		Authorize: func(_ context.Context, connection PeerConnection, _ EnrollmentIntent) error {
			if connection.PeerIdentity().Node != testNode(1) {
				return ErrEnrollmentControlUnauthorized
			}
			return target.VerifyPeerConnectionBinding(connection)
		},
		ReadDeadline: enrollmentDeadline, WriteDeadline: enrollmentDeadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	clientControl, err := NewEnrollmentControlClient(EnrollmentControlClientOptions{
		Opener: enrollmentTestOpener{open: func(context.Context, NodeID) (PeerConnection, error) {
			return client, nil
		}},
		Registry: source, ReadDeadline: enrollmentDeadline, WriteDeadline: enrollmentDeadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- service.Serve(context.Background(), server) }()
	ack, err := clientControl.EnrollMember(context.Background(), testNode(2), intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	if ack.IntentDigest != intent.Digest || ack.MemberID != 4 || ack.Node != testNode(4) {
		t.Fatalf("unexpected enrollment ACK: %+v", ack)
	}
	if _, err := targetTransport.Stats(testNode(4)); err != nil {
		t.Fatalf("enrollment ACK did not publish transport queue: %v", err)
	}
	if role, err := target.Role(group, 4); !errors.Is(err, ErrMemberNotFound) || role != 0 {
		t.Fatalf("enrollment unexpectedly granted authority: role=%v err=%v", role, err)
	}

	// A process restart replays the same durable intent. The target's current
	// directory revision is newer, but the exact digest is idempotent and must
	// return a fresh ACK rather than append another historical mapping.
	clientConn, serverConn = net.Pipe()
	client.Conn = clientConn
	server.Conn = serverConn
	serverDone = make(chan error, 1)
	go func() { serverDone <- service.Serve(context.Background(), server) }()
	replay, err := clientControl.ReplayEnrollment(context.Background(), testNode(2), intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	if replay.DirectoryRevision != ack.DirectoryRevision || replay.RosterDigest != ack.RosterDigest {
		t.Fatalf("replay ACK changed exact cut: first=%+v replay=%+v", ack, replay)
	}
}

// TestEnrollmentControlRetriesStaleDirectoryRevision exercises a voter that
// has already committed one enrollment (for group1) before a second,
// unrelated group's AddLearner enrolls the same already-known physical peer
// (group2). The caller's guess - DirectoryRevision 1, the only value it can
// derive without a remote read - is stale by the time this second enrollment
// runs. EnrollMember must recover using the current-revision hint the
// rejection reports, not fail the way an ordinary hot-shard replica move
// (any cluster with more than one raft group) would otherwise fail every
// time past its target's first-ever enrollment.
func TestEnrollmentControlRetriesStaleDirectoryRevision(t *testing.T) {
	group1 := testGroup(224)
	group2 := group1
	group2.GroupID = [16]byte{224, 9}
	domain := TrustDomain{ClusterID: group1.ClusterID, ClusterIncarnation: group1.ClusterIncarnation}
	members := append(enrollmentTestMembers(group1), enrollmentTestMembers(group2)...)
	limits := Limits{MaxGroups: 2, MaxMembers: 8, MaxPeers: 4}
	source, err := NewStaticRegistryWithDirectory(
		testNode(1), members, enrollmentTestPeers(domain, testNode(1), testNode(2), testNode(3), testNode(4)),
		1, limits,
	)
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewStaticRegistryWithDirectory(
		testNode(2), members, enrollmentTestPeers(domain, testNode(1), testNode(2), testNode(3)),
		1, limits,
	)
	if err != nil {
		t.Fatal(err)
	}
	targetTransport, err := NewOrdinaryTransport(OrdinaryTransportOptions{
		Registry: target, Dialer: ordinaryDialFunc(func(context.Context, NodeID) (PeerConnection, error) {
			return nil, ErrTransportClosed
		}),
		Queue: QueueLimits{
			PerPeerFrames: 4, PerPeerBytes: 1 << 16,
			GlobalFrames: 8, GlobalBytes: 1 << 17,
		},
		Coalesce: CoalesceLimits{
			MaxFrames: 2, MaxBytes: 1 << 16, RetainedBytes: DefaultRetainedFrameBytes,
		},
		Wait: WaitWithTimer, Backoff: func(uint32) time.Duration { return time.Millisecond },
		MaxReconnectDelay: time.Second, WriteDeadline: enrollmentDeadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer targetTransport.Close()

	clientKey, _ := source.PhysicalPeer(testNode(2))
	serverKey, _ := target.PhysicalPeer(testNode(1))
	service, err := NewEnrollmentControlService(EnrollmentControlServiceOptions{
		Registry: target, Transport: targetTransport, Verifier: EnrollmentVerifierFunc(allowEnrollment),
		Authorize: func(_ context.Context, connection PeerConnection, _ EnrollmentIntent) error {
			if connection.PeerIdentity().Node != testNode(1) {
				return ErrEnrollmentControlUnauthorized
			}
			return target.VerifyPeerConnectionBinding(connection)
		},
		ReadDeadline: enrollmentDeadline, WriteDeadline: enrollmentDeadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	// EnrollMember opens one connection per attempt (its own retry included),
	// serving each on a fresh net.Pipe so it looks exactly like a fresh dial.
	// serveErrs[i] holds attempt i's Serve outcome, indexed by the order
	// attempts were opened rather than the order their Serve goroutines
	// happen to finish: EnrollMember only waits for the wire bytes it needs
	// before starting its next attempt, not for the prior attempt's Serve
	// call to fully return and record its own result, so a slower earlier
	// attempt can finish recording after a faster later one.
	var serveWG sync.WaitGroup
	var serveMu sync.Mutex
	var serveErrs []error
	openCount := 0
	clientControl, err := NewEnrollmentControlClient(EnrollmentControlClientOptions{
		Opener: enrollmentTestOpener{open: func(context.Context, NodeID) (PeerConnection, error) {
			clientConn, serverConn := net.Pipe()
			client := &enrollmentTestConnection{
				Conn: clientConn, identity: PeerIdentity{TrustDomain: domain, Node: testNode(2)},
				key: clientKey.ServiceKeyDigest, class: TrafficShardControl,
			}
			server := &enrollmentTestConnection{
				Conn: serverConn, identity: PeerIdentity{TrustDomain: domain, Node: testNode(1)},
				key: serverKey.ServiceKeyDigest, class: TrafficShardControl,
			}
			serveMu.Lock()
			index := openCount
			openCount++
			serveErrs = append(serveErrs, nil)
			serveMu.Unlock()
			serveWG.Add(1)
			go func() {
				defer serveWG.Done()
				err := service.Serve(context.Background(), server)
				serveMu.Lock()
				serveErrs[index] = err
				serveMu.Unlock()
			}()
			return client, nil
		}},
		Registry: source, ReadDeadline: enrollmentDeadline, WriteDeadline: enrollmentDeadline,
	})
	if err != nil {
		t.Fatal(err)
	}

	roster1, ok := target.RosterDigest(group1)
	if !ok {
		t.Fatal("target roster digest missing for group1")
	}
	first := dynamicPeerIntent(target, testNode(4), 224, group1, 4, roster1)
	first.Peer.Endpoint, first.Peer.Address, first.Peer.Node = "127.0.0.1:25004", "127.0.0.1:25004", testNode(4)
	firstAck, err := clientControl.EnrollMember(context.Background(), testNode(2), first)
	if err != nil {
		t.Fatalf("first enrollment (group1) failed: %v", err)
	}
	if firstAck.DirectoryRevision != 2 {
		t.Fatalf("first enrollment revision = %d, want 2 (the registry's post-commit value)", firstAck.DirectoryRevision)
	}

	// A second, unrelated group's AddLearner enrolls the same already-known
	// physical peer. The caller has no remote read of the voter's directory
	// and guesses DirectoryRevision 1 again - the same guess that was
	// correct the first time - which is now stale.
	roster2, ok := target.RosterDigest(group2)
	if !ok {
		t.Fatal("target roster digest missing for group2")
	}
	second := dynamicPeerIntent(target, testNode(4), 225, group2, 4, roster2)
	second.Peer.Endpoint, second.Peer.Address, second.Peer.Node = "127.0.0.1:25004", "127.0.0.1:25004", testNode(4)
	second.DirectoryRevision = 1 // stale: target's current revision already advanced to 2 above

	secondAck, err := clientControl.EnrollMember(context.Background(), testNode(2), second)
	if err != nil {
		t.Fatalf("EnrollMember did not recover from a stale DirectoryRevision guess: %v", err)
	}
	if secondAck.DirectoryRevision != 3 || secondAck.Group != group2 || secondAck.MemberID != 4 {
		t.Fatalf("unexpected corrected enrollment ACK: %+v", secondAck)
	}
	if role, err := target.Role(group2, 4); !errors.Is(err, ErrMemberNotFound) || role != 0 {
		t.Fatalf("enrollment unexpectedly granted authority: role=%v err=%v", role, err)
	}

	serveWG.Wait()
	serveMu.Lock()
	defer serveMu.Unlock()
	if len(serveErrs) != 3 {
		t.Fatalf("Serve call count = %d, want 3 (first enroll, stale second attempt, corrected retry): %v",
			len(serveErrs), serveErrs)
	}
	if serveErrs[0] != nil {
		t.Fatalf("first enrollment Serve error = %v, want nil", serveErrs[0])
	}
	if !errors.Is(serveErrs[1], ErrPeerConflict) {
		t.Fatalf("stale second-enrollment attempt Serve error = %v, want ErrPeerConflict", serveErrs[1])
	}
	if serveErrs[2] != nil {
		t.Fatalf("corrected retry Serve error = %v, want nil", serveErrs[2])
	}
}

func TestEnrollmentRequestCanonicalRoundTrip(t *testing.T) {
	group := testGroup(222)
	domain := TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	intent := EnrollmentIntent{
		Digest: sha256.Sum256([]byte("wire-intent")), Domain: domain,
		Peer: PhysicalPeer{
			NodeID: testNode(4), TrustDomain: domain, Incarnation: 8, Revision: 9,
			ServiceKeyDigest: sha256.Sum256([]byte("wire-key")), Endpoint: "127.0.0.1:25004",
			State: PeerEnrolled,
		},
		Group: group, Member: Member{Group: group, ReplicaSetVersion: 3, MemberID: 4, Node: testNode(4), Role: MemberEnrolled},
		ExpectedRosterDigest: sha256.Sum256([]byte("roster")), DirectoryRevision: 17,
	}
	intent.Peer.Node = intent.Peer.NodeID
	intent.Peer.EnrollmentDigest = intent.Digest
	intent.Peer.Address = intent.Peer.Endpoint
	raw, err := AppendEnrollmentRequest(nil, intent)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := OpenEnrollmentRequest(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Digest != intent.Digest || decoded.Domain != intent.Domain || decoded.Peer != intent.Peer ||
		decoded.Group != intent.Group || decoded.Member != intent.Member ||
		decoded.ExpectedRosterDigest != intent.ExpectedRosterDigest || decoded.DirectoryRevision != intent.DirectoryRevision {
		t.Fatalf("round trip changed intent: got=%+v want=%+v", decoded, intent)
	}
	if len(raw) > EnrollmentControlMaxRequestBytes {
		t.Fatalf("request bytes=%d exceed bound=%d", len(raw), EnrollmentControlMaxRequestBytes)
	}
}

func TestEnrollmentFanoutReturnsBoundedReplaySuffix(t *testing.T) {
	group := testGroup(223)
	domain := TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	intent := dynamicPeerIntent(&StaticRegistry{trustDomain: domain}, testNode(4), 223, group, 4, sha256.Sum256([]byte("roster")))
	intent.Peer.Node = intent.Peer.NodeID
	intent.DirectoryRevision = 1
	if canonical, canonicalErr := canonicalEnrollmentIntent(intent); canonicalErr != nil {
		t.Fatalf("fanout intent rejected: %#v err=%v", intent, canonicalErr)
	} else {
		intent = canonical
	}
	var mu sync.Mutex
	var calls []NodeID
	fail := true
	ack := func(node NodeID) EnrollmentAck {
		return EnrollmentAck{IntentDigest: intent.Digest, Group: group, MemberID: 4, Node: testNode(4),
			DirectoryRevision: 2, PeerDirectoryDigest: sha256.Sum256([]byte{byte(node[0]), 1}),
			RosterDigest: sha256.Sum256([]byte("roster"))}
	}
	fanout, err := NewEnrollmentFanout([]EnrollmentFanoutTarget{
		{Node: testNode(3), Enroll: func(context.Context, EnrollmentIntent) (EnrollmentAck, error) {
			mu.Lock()
			calls = append(calls, testNode(3))
			mu.Unlock()
			return ack(testNode(3)), nil
		}},
		{Node: testNode(1), Enroll: func(context.Context, EnrollmentIntent) (EnrollmentAck, error) {
			mu.Lock()
			calls = append(calls, testNode(1))
			mu.Unlock()
			return ack(testNode(1)), nil
		}},
		{Node: testNode(2), Enroll: func(context.Context, EnrollmentIntent) (EnrollmentAck, error) {
			mu.Lock()
			calls = append(calls, testNode(2))
			mu.Unlock()
			if fail {
				return EnrollmentAck{}, errors.New("temporarily unavailable")
			}
			return ack(testNode(2)), nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := fanout.Enroll(context.Background(), intent)
	if err == nil || len(result.Acks) != 1 || len(result.Pending) != 2 {
		t.Fatalf("partial fanout result=%+v err=%v", result, err)
	}
	if result.Pending[0] != testNode(2) || result.Pending[1] != testNode(3) {
		t.Fatalf("pending suffix=%v", result.Pending)
	}
	fail = false
	result, err = fanout.Replay(context.Background(), intent)
	if err != nil || len(result.Acks) != 3 || len(result.Pending) != 0 {
		t.Fatalf("replay result=%+v err=%v", result, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 5 {
		t.Fatalf("fanout calls=%d want 5", len(calls))
	}
}
