package raftservice

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	pb "go.etcd.io/raft/v3/raftpb"
)

var peerServerTestNow = time.Date(2034, 2, 3, 4, 5, 6, 0, time.UTC)
var peerServerTestOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}

type peerServerTestAuthority struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	roots       *x509.CertPool
	serial      int64
}

func TestPeerServerAuthenticatesDeliversAndRejectsAboveExactStreamBound(t *testing.T) {
	group := peerServerTestGroup()
	clientNode := peerServerTestNode(1)
	serverNode := peerServerTestNode(2)
	members := []rafttransport.Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: clientNode, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: serverNode, Role: rafttransport.MemberVoter},
	}
	clientRegistry, err := rafttransport.NewStaticRegistry(
		clientNode, members, rafttransport.Limits{MaxGroups: 1, MaxMembers: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	serverRegistry, err := rafttransport.NewStaticRegistry(
		serverNode, members, rafttransport.Limits{MaxGroups: 1, MaxMembers: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	authority := newPeerServerTestAuthority(t)
	domain := serverRegistry.TrustDomain()
	clientTLS := newPeerServerTestTLS(t, authority, rafttransport.PeerIdentity{
		TrustDomain: domain, Node: clientNode,
	})
	serverTLS := newPeerServerTestTLS(t, authority, rafttransport.PeerIdentity{
		TrustDomain: domain, Node: serverNode,
	})

	delivered := make(chan rafttransport.Inbound, 1)
	receiver, err := rafttransport.NewOrdinaryReceiver(rafttransport.OrdinaryReceiverOptions{
		Registry: serverRegistry,
		ReadDeadline: func() time.Time {
			return time.Now().Add(5 * time.Second)
		},
		Handle: func(_ context.Context, inbound rafttransport.Inbound) error {
			delivered <- inbound
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	server, err := NewPeerServer(PeerServerOptions{
		Listener: listener, TLS: serverTLS, Receiver: receiver,
		HandshakeDeadline: deadline, MaxStreams: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Run(ctx) }()
	<-server.Started()

	firstRaw, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	first, err := clientTLS.Client(
		ctx, firstRaw, serverNode, rafttransport.TrafficOrdinary, deadline,
	)
	if err != nil {
		t.Fatal(err)
	}

	// The first authenticated stream retains the sole slot. A second raw
	// connection is accepted and closed without a goroutine or TLS allocation.
	second, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = second.SetDeadline(time.Now().Add(5 * time.Second))
	var one [1]byte
	if _, err := second.Read(one[:]); err == nil {
		t.Fatal("connection above stream bound remained open")
	}
	_ = second.Close()

	message := &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: peerServerU64(1), To: peerServerU64(2),
		Term: peerServerU64(3), Commit: peerServerU64(4), Context: []byte("rf3"),
	}
	frame, destination, err := clientRegistry.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 2, Message: message,
	})
	if err != nil {
		t.Fatal(err)
	}
	if destination != serverNode {
		t.Fatalf("destination = %x", destination)
	}
	record := make([]byte, rafttransport.StreamRecordHeaderBytes+len(frame))
	binary.BigEndian.PutUint32(record, uint32(len(frame)))
	copy(record[rafttransport.StreamRecordHeaderBytes:], frame)
	if _, err := io.Copy(first, bytes.NewReader(record)); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()

	select {
	case inbound := <-delivered:
		if inbound.Group != group || inbound.Message.GetFrom() != 1 || inbound.Message.GetTo() != 2 {
			t.Fatalf("delivery = %+v", inbound)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("authenticated frame was not delivered")
	}
	for deadlineAt := time.Now().Add(5 * time.Second); ; {
		stats := server.Stats()
		if stats.Accepted == 1 && stats.Rejected == 1 && stats.Active == 0 && stats.Failed == 0 {
			break
		}
		if time.Now().After(deadlineAt) {
			t.Fatalf("stats = %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	select {
	case err := <-serverDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer server did not stop")
	}
}

func newPeerServerTestAuthority(t testing.TB) *peerServerTestAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "peer-test-ca"},
		NotBefore: peerServerTestNow.Add(-time.Hour), NotAfter: peerServerTestNow.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	encoded, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(encoded)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return &peerServerTestAuthority{certificate: certificate, key: key, roots: roots, serial: 1}
}

func newPeerServerTestTLS(
	t testing.TB,
	authority *peerServerTestAuthority,
	identity rafttransport.PeerIdentity,
) *rafttransport.PeerTLS {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	extension, err := rafttransport.PeerIdentityExtension(peerServerTestOID, identity)
	if err != nil {
		t.Fatal(err)
	}
	authority.serial++
	template := &x509.Certificate{
		SerialNumber: big.NewInt(authority.serial), Subject: pkix.Name{CommonName: "unused"},
		NotBefore: peerServerTestNow.Add(-time.Hour), NotAfter: peerServerTestNow.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth,
		},
		ExtraExtensions: []pkix.Extension{extension},
	}
	encoded, err := x509.CreateCertificate(
		rand.Reader, template, authority.certificate, &key.PublicKey, authority.key,
	)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := rafttransport.NewPeerTLS(rafttransport.PeerTLSOptions{
		IdentityOID: peerServerTestOID, Identity: identity,
		Certificate: tls.Certificate{
			Certificate: [][]byte{encoded, authority.certificate.Raw}, PrivateKey: key,
		},
		Roots: authority.roots, Now: func() time.Time { return peerServerTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func peerServerTestGroup() raftmember.GroupKey {
	var group raftmember.GroupKey
	for index := range group.ClusterID {
		group.ClusterID[index] = byte(index + 1)
		group.ClusterIncarnation[index] = byte(index + 21)
		group.ShardIncarnation[index] = byte(index + 41)
		group.GroupID[index] = byte(index + 61)
	}
	group.TopologyRecoveryEpoch = 3
	return group
}

func peerServerTestNode(seed byte) (node rafttransport.NodeID) {
	for index := range node {
		node[index] = seed + byte(index)
	}
	return node
}

func peerServerU64(value uint64) *uint64 { return &value }
