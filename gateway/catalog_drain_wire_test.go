package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type catalogDrainWirePeer struct {
	net.Conn
	identity rafttransport.PeerIdentity
}

func (peer *catalogDrainWirePeer) PeerIdentity() rafttransport.PeerIdentity { return peer.identity }
func (*catalogDrainWirePeer) PeerKeyDigest() [32]byte                       { return [32]byte{} }
func (*catalogDrainWirePeer) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficGatewayControl
}

type catalogDrainVerifier struct {
	generation uint64
	digest     [sha256.Size]byte
}

func (verifier catalogDrainVerifier) VerifyClusterCatalogDigest(
	_ context.Context, generation uint64, digest [sha256.Size]byte,
) error {
	if generation != verifier.generation || digest != verifier.digest {
		return ErrClusterCatalogDrainUnknown
	}
	return nil
}

type catalogDrainWireOpener struct {
	t             testing.TB
	trust         rafttransport.TrustDomain
	controller    rafttransport.NodeID
	members       map[rafttransport.NodeID]ClusterCatalogDrainMember
	holder        *CatalogHolder
	verifier      catalogDrainVerifier
	dropFirstRead atomic.Bool
	clientActive  atomic.Int64
	clientMaximum atomic.Int64
}

func (opener *catalogDrainWireOpener) OpenGatewayControl(
	_ context.Context, node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	member, found := opener.members[node]
	if !found {
		return nil, ErrClusterCatalogDrainAuth
	}
	clientRaw, serverRaw := net.Pipe()
	server := &catalogDrainWirePeer{Conn: serverRaw, identity: rafttransport.PeerIdentity{
		TrustDomain: opener.trust, Node: opener.controller,
	}}
	service, err := NewClusterCatalogDrainControlService(ClusterCatalogDrainControlOptions{
		Holder: opener.holder, Catalog: opener.verifier, Member: member,
		Authorize: func(peer rafttransport.PeerIdentity, request ClusterCatalogDrainRequest) bool {
			return peer.Node == opener.controller && request.Valid()
		},
		ReadDeadline:  catalogDrainTestDeadline,
		WriteDeadline: catalogDrainTestDeadline,
	})
	if err != nil {
		return nil, err
	}
	go func() {
		_ = service.Serve(context.Background(), server)
	}()
	client := rafttransport.PeerConnection(&catalogDrainWirePeer{
		Conn: clientRaw, identity: rafttransport.PeerIdentity{TrustDomain: opener.trust, Node: node},
	})
	active := opener.clientActive.Add(1)
	for {
		maximum := opener.clientMaximum.Load()
		if active <= maximum || opener.clientMaximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	client = &catalogDrainTrackedPeer{PeerConnection: client, closed: func() { opener.clientActive.Add(-1) }}
	if opener.dropFirstRead.CompareAndSwap(true, false) {
		client = &catalogDrainDropReadPeer{PeerConnection: client}
	}
	return client, nil
}

type catalogDrainDropReadPeer struct{ rafttransport.PeerConnection }

func (*catalogDrainDropReadPeer) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type catalogDrainTrackedPeer struct {
	rafttransport.PeerConnection
	once   sync.Once
	closed func()
}

func (peer *catalogDrainTrackedPeer) Close() error {
	err := peer.PeerConnection.Close()
	peer.once.Do(peer.closed)
	return err
}

func catalogDrainTestDeadline() time.Time { return time.Now().Add(2 * time.Second) }

func TestClusterCatalogDrainWireCanonicalRequestAndCertificate(t *testing.T) {
	request := ClusterCatalogDrainRequest{
		Operation: clusterDrainDigest(1), Step: clusterDrainDigest(2),
		Generation: 41, CatalogDigest: clusterDrainDigest(3),
	}
	envelope, err := NewClusterCatalogDrainEnvelope(request, clusterDrainTrust(), clusterDrainMembers(257))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendClusterCatalogDrainEnvelope([]byte{7}, envelope)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenClusterCatalogDrainEnvelope(raw[1:])
	if err != nil || opened.Request != request || opened.Fence.Digest() != envelope.Fence.Digest() ||
		opened.Fence.RosterDigest() != envelope.Fence.RosterDigest() || opened.Fence.MemberCount() != 257 {
		t.Fatalf("opened envelope=%+v err=%v", opened, err)
	}
	reencoded, err := AppendClusterCatalogDrainEnvelope(nil, opened)
	if err != nil || !bytes.Equal(reencoded, raw[1:]) {
		t.Fatal("request decode/re-encode was not byte-identical")
	}
	for _, invalid := range [][]byte{raw[1 : len(raw)-1], append(bytes.Clone(raw[1:]), 0)} {
		if _, openErr := OpenClusterCatalogDrainEnvelope(invalid); !errors.Is(openErr, ErrClusterCatalogDrainWire) {
			t.Fatalf("invalid request error=%v", openErr)
		}
	}
	tampered := bytes.Clone(raw[1:])
	tampered[40]++
	if _, openErr := OpenClusterCatalogDrainEnvelope(tampered); !errors.Is(openErr, ErrClusterCatalogDrainWire) {
		t.Fatalf("tampered request error=%v", openErr)
	}
	fenceDigest, rosterDigest := envelope.Fence.Digest(), envelope.Fence.RosterDigest()
	proofBytes := append([]byte("vibedb/catalog-drain/certificate\x00"), fenceDigest[:]...)
	proofBytes = append(proofBytes, rosterDigest[:]...)
	proof := sha256.Sum256(proofBytes)
	certificate := ClusterCatalogDrainCertificate{
		Request: request, FenceDigest: envelope.Fence.Digest(),
		RosterDigest: envelope.Fence.RosterDigest(), Proof: proof,
	}
	certificateRaw, err := AppendClusterCatalogDrainCertificate(nil, certificate)
	if err != nil || len(certificateRaw) != ClusterCatalogDrainCertificateBytes {
		t.Fatalf("append certificate bytes=%d err=%v", len(certificateRaw), err)
	}
	openedCertificate, err := OpenClusterCatalogDrainCertificate(certificateRaw)
	if err != nil || openedCertificate != certificate {
		t.Fatalf("opened certificate=%+v err=%v", openedCertificate, err)
	}
}

func TestClusterCatalogDrainWireBoundedRosterAndExactCertificate(t *testing.T) {
	members := clusterDrainMembers(129)
	request := ClusterCatalogDrainRequest{
		Operation: clusterDrainDigest(7), Step: clusterDrainDigest(8),
		Generation: 41, CatalogDigest: clusterDrainDigest(9),
	}
	opener := &catalogDrainWireOpener{
		t: t, trust: clusterDrainTrust(), controller: members[0].Node,
		members:  make(map[rafttransport.NodeID]ClusterCatalogDrainMember, len(members)),
		holder:   NewCatalogHolder(testSnapshot(t, request.Generation)),
		verifier: catalogDrainVerifier{generation: request.Generation, digest: request.CatalogDigest},
	}
	for _, member := range members {
		opener.members[member.Node] = member
	}
	client, err := NewClusterCatalogDrainClient(ClusterCatalogDrainClientOptions{
		Opener: opener, ReadDeadline: catalogDrainTestDeadline,
		WriteDeadline: catalogDrainTestDeadline, MaxConcurrent: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewClusterCatalogDrainCoordinator(clusterDrainTrust(), members, client)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := coordinator.CertifyClusterCatalogDrain(context.Background(), request)
	if err != nil || !certificate.ValidFor(request) {
		t.Fatalf("certificate=%+v err=%v", certificate, err)
	}
	if maximum := opener.clientMaximum.Load(); maximum > 7 || maximum < 2 {
		t.Fatalf("active gateway streams=%d, want 2..7", maximum)
	}
}

func TestClusterCatalogDrainWireResponseLossExactReplayAndDigestFence(t *testing.T) {
	members := clusterDrainMembers(1)
	request := ClusterCatalogDrainRequest{
		Operation: clusterDrainDigest(11), Step: clusterDrainDigest(12),
		Generation: 41, CatalogDigest: clusterDrainDigest(13),
	}
	opener := &catalogDrainWireOpener{
		t: t, trust: clusterDrainTrust(), controller: members[0].Node,
		members:  map[rafttransport.NodeID]ClusterCatalogDrainMember{members[0].Node: members[0]},
		holder:   NewCatalogHolder(testSnapshot(t, request.Generation)),
		verifier: catalogDrainVerifier{generation: request.Generation, digest: request.CatalogDigest},
	}
	opener.dropFirstRead.Store(true)
	client, err := NewClusterCatalogDrainClient(ClusterCatalogDrainClientOptions{
		Opener: opener, ReadDeadline: catalogDrainTestDeadline,
		WriteDeadline: catalogDrainTestDeadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewClusterCatalogDrainCoordinator(clusterDrainTrust(), members, client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = coordinator.CertifyClusterCatalogDrain(context.Background(), request); err == nil {
		t.Fatal("lost terminal response reported success")
	}
	first, err := coordinator.CertifyClusterCatalogDrain(context.Background(), request)
	if err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	second, err := coordinator.CertifyClusterCatalogDrain(context.Background(), request)
	if err != nil || second != first {
		t.Fatalf("settled replay first=%+v second=%+v err=%v", first, second, err)
	}
	bad := request
	bad.CatalogDigest[0]++
	if _, err = coordinator.CertifyClusterCatalogDrain(context.Background(), bad); err == nil {
		t.Fatalf("wrong catalog digest error=%v", err)
	}
}

func TestClusterCatalogDrainWireServiceRejectsWrongTrafficClassBeforeRead(t *testing.T) {
	member := clusterDrainMembers(1)[0]
	service, err := NewClusterCatalogDrainControlService(ClusterCatalogDrainControlOptions{
		Holder:  NewCatalogHolder(testSnapshot(t, 41)),
		Catalog: catalogDrainVerifier{generation: 41, digest: clusterDrainDigest(3)},
		Member:  member, Authorize: func(rafttransport.PeerIdentity, ClusterCatalogDrainRequest) bool { return true },
		ReadDeadline: catalogDrainTestDeadline, WriteDeadline: catalogDrainTestDeadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	defer right.Close()
	wrong := &catalogDrainWrongClassPeer{catalogDrainWirePeer{Conn: left}}
	if err = service.Serve(context.Background(), wrong); !errors.Is(err, ErrClusterCatalogDrainAuth) {
		t.Fatalf("wrong traffic class error=%v", err)
	}
}

type catalogDrainWrongClassPeer struct{ catalogDrainWirePeer }

func (*catalogDrainWrongClassPeer) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficShardControl
}
