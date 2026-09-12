package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

func TestRF3PinnedRegistryUsesPreparedPhysicalEndpointsAcrossGroups(t *testing.T) {
	group := rf3CommandGroup()
	domain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	local, remote := rafttransport.NodeID{1}, rafttransport.NodeID{2}
	credentials, roots, err := rf3testfixture.WriteCredentials(t.TempDir(), rf3CommandIdentityOID, domain, []rafttransport.NodeID{local, remote})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := servicetls.LoadProfile(credentials[0].Certificate, credentials[0].Key, roots, "1.3.6.1.4.1.32473.1.1", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	// Grouped manifests have no top-level member roster. IDs are scoped to
	// each group; the physical endpoints were validated while preparing them.
	manifest := rf3Manifest{TLS: rf3ManifestTLS{PeerKeys: rf3CommandPeerKeys(credentials[0])}}
	endpoints := map[rafttransport.NodeID]string{local: "127.0.0.1:21001", remote: "127.0.0.1:21002"}
	secondGroup := group
	secondGroup.GroupID[0]++
	members := []rafttransport.Member{
		{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: local, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: remote, Role: rafttransport.MemberVoter},
		{Group: secondGroup, ReplicaSetVersion: 1, MemberID: 7, Node: local, Role: rafttransport.MemberVoter},
		{Group: secondGroup, ReplicaSetVersion: 1, MemberID: 8, Node: remote, Role: rafttransport.MemberVoter},
	}
	limits := rafttransport.Limits{MaxGroups: 2, MaxMembers: 4, MaxPeers: 2}
	registry, err := newRF3PinnedStaticRegistry(manifest, profile, members, endpoints, limits)
	if err != nil {
		t.Fatal(err)
	}
	for node, address := range endpoints {
		peer, err := registry.PhysicalPeer(node)
		if err != nil || peer.Endpoint != address {
			t.Fatalf("node %x endpoint = %q, %v; want %q", node, peer.Endpoint, err, address)
		}
	}
	delete(endpoints, remote)
	if _, err := newRF3PinnedStaticRegistry(manifest, profile, members, endpoints, limits); !errors.Is(err, errInvalidRF3Manifest) {
		t.Fatalf("missing physical endpoint = %v", err)
	}
}

func TestRF3RegistryPeerDialerTracksEnrollmentAndReincarnation(t *testing.T) {
	domain := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	local, remote := rafttransport.NodeID{1}, rafttransport.NodeID{2}
	registry, err := rafttransport.NewEmptyRegistry(local, domain, rafttransport.Limits{
		MaxGroups: 1, MaxMembers: 2, MaxPeers: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Retain the dialer before the new node exists, as a serving voter does.
	dial := rf3RegistryPeerDialer(registry)
	if _, err := dial(context.Background(), remote); !errors.Is(err, rafttransport.ErrPeerUnauthorized) {
		t.Fatalf("un-enrolled dial = %v", err)
	}
	for incarnation := uint64(1); incarnation <= 2; incarnation++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		intent := rafttransport.EnrollmentIntent{
			Domain: domain, Digest: sha256.Sum256([]byte{byte(incarnation)}),
			DirectoryRevision: registry.PeerDirectoryRevision(),
			Peer: rafttransport.PhysicalPeer{
				NodeID: remote, TrustDomain: domain, Incarnation: incarnation, Revision: incarnation,
				State: rafttransport.PeerEnrolled, Endpoint: listener.Addr().String(),
				ServiceKeyDigest: sha256.Sum256([]byte{byte(incarnation), 1}),
			},
		}
		if err := registry.EnrollPeer(intent, rafttransport.EnrollmentVerifierFunc(func(rafttransport.EnrollmentIntent) error { return nil })); err != nil {
			t.Fatalf("enroll incarnation %d: %v", incarnation, err)
		}
		connection, err := dial(context.Background(), remote)
		if err != nil {
			t.Fatalf("dial incarnation %d: %v", incarnation, err)
		}
		if got := connection.RemoteAddr().String(); got != listener.Addr().String() {
			t.Fatalf("dial reached %s, want current endpoint %s", got, listener.Addr())
		}
		_ = connection.Close()
		_ = listener.Close()
		if err := registry.RetirePhysicalPeer(rafttransport.PeerRetirementProof{
			NodeID: remote, Incarnation: incarnation, Revision: incarnation,
			DirectoryRevision: registry.PeerDirectoryRevision(), DirectoryDigest: registry.PeerDirectoryDigest(),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := dial(context.Background(), remote); !errors.Is(err, rafttransport.ErrPeerUnauthorized) {
			t.Fatalf("retired dial = %v", err)
		}
	}
}
