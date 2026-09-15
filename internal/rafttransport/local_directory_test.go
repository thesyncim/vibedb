package rafttransport

import (
	"context"
	"errors"
	"testing"
)

func TestCertifiedLocalDirectoryBindingIsExactAndMonotone(t *testing.T) {
	identity := peerTLSTestIdentity(211, 41)
	profile := newPeerTLSTestProfile(t, newPeerTLSTestAuthority(t, 212), identity)
	registry, err := NewEmptyRegistry(identity.Node, identity.TrustDomain, Limits{MaxGroups: 2, MaxMembers: 8, MaxPeers: 4})
	if err != nil {
		t.Fatal(err)
	}
	intent := EnrollmentIntent{Domain: identity.TrustDomain, Digest: [32]byte{3}, DirectoryRevision: registry.PeerDirectoryRevision(),
		Peer: PhysicalPeer{NodeID: identity.Node, TrustDomain: identity.TrustDomain, Incarnation: 5, Revision: 7,
			ServiceKeyDigest: profile.LocalServiceKeyDigest(), Endpoint: "127.0.0.1:1234", State: PeerEnrolled}}
	calls := 0
	verifier := EnrollmentVerifierFunc(func(candidate EnrollmentIntent) error {
		calls++
		if candidate.Peer.ServiceKeyDigest != intent.Peer.ServiceKeyDigest {
			return ErrPeerUnauthorized
		}
		return nil
	})
	for _, test := range []struct {
		name string
		edit func(*EnrollmentIntent)
		inc  uint64
	}{
		{"wrong TLS key", func(i *EnrollmentIntent) { i.Peer.ServiceKeyDigest[0]++ }, 5},
		{"wrong TLS node", func(i *EnrollmentIntent) { i.Peer.NodeID[0]++ }, 5},
		{"wrong incarnation", func(i *EnrollmentIntent) { i.Peer.Incarnation++ }, 5},
		{"unbound endpoint", func(i *EnrollmentIntent) { i.Peer.Endpoint = "" }, 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := intent
			test.edit(&bad)
			if err := registry.BindLocalPeerContext(t.Context(), bad, profile, test.inc, verifier); err == nil {
				t.Fatal("mismatched local proof bound")
			}
		})
	}
	denied := errors.New("uncertified")
	if err = registry.BindLocalPeerContext(t.Context(), intent, profile, 5, EnrollmentVerifierFunc(func(EnrollmentIntent) error { return denied })); !errors.Is(err, denied) {
		t.Fatal(err)
	}
	if err = registry.BindLocalPeerContext(t.Context(), intent, profile, 5, verifier); err != nil {
		t.Fatal(err)
	}
	bound, err := registry.PhysicalPeer(identity.Node)
	if err != nil || bound.Incarnation != 5 || bound.Revision != 7 || bound.ServiceKeyDigest != profile.LocalServiceKeyDigest() || bound.Endpoint != intent.Peer.Endpoint {
		t.Fatalf("bound=%+v,%v", bound, err)
	}
	revision, digest := registry.PeerDirectoryRevision(), registry.PeerDirectoryDigest()
	replay := intent
	replay.Digest = [32]byte{4}
	replay.DirectoryRevision = 1
	if err = registry.BindLocalPeerContext(t.Context(), replay, profile, 5, verifier); err != nil {
		t.Fatal(err)
	}
	if registry.PeerDirectoryRevision() != revision || registry.PeerDirectoryDigest() != digest {
		t.Fatal("exact replay changed directory")
	}
	for _, test := range []struct {
		name string
		edit func(*EnrollmentIntent)
	}{
		{"stale revision", func(i *EnrollmentIntent) { i.Peer.Revision-- }},
		{"changed endpoint same revision", func(i *EnrollmentIntent) { i.Peer.Endpoint = "127.0.0.1:1235" }},
		{"changed process incarnation", func(i *EnrollmentIntent) { i.Peer.Incarnation++; i.Peer.Revision++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := intent
			bad.DirectoryRevision = revision
			test.edit(&bad)
			if err := registry.BindLocalPeerContext(t.Context(), bad, profile, bad.Peer.Incarnation, verifier); err == nil {
				t.Fatal("conflicting local proof bound")
			}
		})
	}
	next := intent
	next.DirectoryRevision = revision
	next.Peer.Revision++
	next.Peer.Endpoint = "127.0.0.1:1235"
	if err = registry.BindLocalPeerContext(t.Context(), next, profile, 5, verifier); err != nil {
		t.Fatal(err)
	}
	if registry.PeerDirectoryRevision() != revision+1 {
		t.Fatal("new certified cut was not published")
	}
	if len(registry.physical) != 1 || registry.effectiveMemberCount(registry.dynamic.Load()) != 0 {
		t.Fatal("local binding added remote or group authority")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err = registry.BindLocalPeerContext(canceled, next, profile, 5, verifier); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls < 3 {
		t.Fatal("replay skipped certification")
	}
}
