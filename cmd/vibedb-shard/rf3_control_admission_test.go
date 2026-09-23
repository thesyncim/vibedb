package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

func TestRF3LiveControlAdmissionFollowsCurrentPhysicalIdentity(t *testing.T) {
	for _, class := range []rafttransport.TrafficClass{rafttransport.TrafficShardControl, rafttransport.TrafficSnapshot} {
		t.Run(fmt.Sprint(class), func(t *testing.T) { testRF3LivePeerAdmission(t, class) })
	}
}

func testRF3LivePeerAdmission(t *testing.T, class rafttransport.TrafficClass) {
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	local, remote := rafttransport.NodeID{1}, rafttransport.NodeID{4}
	credentials, roots, err := rf3testfixture.WriteCredentials(t.TempDir(), rf3CommandIdentityOID, domain, []rafttransport.NodeID{local, remote, remote})
	if err != nil {
		t.Fatal(err)
	}
	profiles := make([]*rafttransport.PeerTLS, len(credentials))
	for i, c := range credentials {
		profiles[i], err = servicetls.LoadProfile(c.Certificate, c.Key, roots, rf3testfixture.ProcessIdentityOID, time.Now)
		if err != nil {
			t.Fatal(err)
		}
	}
	registry, err := rafttransport.NewEmptyRegistry(local, domain, rafttransport.Limits{MaxGroups: 1, MaxMembers: 4, MaxPeers: 4})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{Node: local, Capabilities: serviceauthz.CapabilityMembership}})
	if err != nil {
		t.Fatal(err)
	}
	server, err := newRF3ServiceTLS(rf3Manifest{}, profiles[0], class, rf3ControlNodes(policy), func() *rafttransport.StaticRegistry { return registry })
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	deadline := servicetls.FixedDeadline(2 * time.Second)
	go func() {
		done <- server.Serve(ctx, listener, servicetls.Limits{MaxConnections: 4, MaxHandshakes: 4, HandshakeDeadline: deadline}, func(_ context.Context, c rafttransport.PeerConnection) { _, _ = c.Write([]byte{1}) })
	}()
	defer func() { cancel(); _ = listener.Close(); <-done }()
	probe := func(profile *rafttransport.PeerTLS) bool {
		t.Helper()
		raw, e := net.DialTimeout("tcp", listener.Addr().String(), 2*time.Second)
		if e != nil {
			t.Fatal(e)
		}
		c, e := profile.Client(ctx, raw, local, class, deadline)
		if e != nil {
			_ = raw.Close()
			return false
		}
		defer c.Close()
		_ = c.SetReadDeadline(deadline())
		var b [1]byte
		_, e = io.ReadFull(c, b[:])
		return e == nil && b[0] == 1
	}
	if probe(profiles[1]) {
		t.Fatal("unknown physical identity admitted")
	}
	intent := rafttransport.EnrollmentIntent{Digest: [32]byte{1}, Domain: domain, DirectoryRevision: registry.PeerDirectoryRevision(), Peer: rafttransport.PhysicalPeer{NodeID: remote, TrustDomain: domain, Incarnation: 1, Revision: 1, ServiceKeyDigest: profiles[1].LocalPeerKeyDigest(), Endpoint: "127.0.0.1:21004", State: rafttransport.PeerEnrolled}}
	if err := registry.EnrollPeer(intent, rafttransport.EnrollmentVerifierFunc(func(rafttransport.EnrollmentIntent) error { return nil })); err != nil {
		t.Fatal(err)
	}
	if !probe(profiles[1]) {
		t.Fatal("current enrolled replacement rejected before control handler")
	}
	if probe(profiles[2]) {
		t.Fatal("same node with different key admitted")
	}
	peer, err := registry.PhysicalPeer(remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.RetirePhysicalPeer(rafttransport.PeerRetirementProof{NodeID: remote, Incarnation: peer.Incarnation, Revision: peer.Revision, DirectoryRevision: registry.PeerDirectoryRevision(), DirectoryDigest: registry.PeerDirectoryDigest()}); err != nil {
		t.Fatal(err)
	}
	if probe(profiles[1]) {
		t.Fatal("retired physical identity admitted")
	}
}
