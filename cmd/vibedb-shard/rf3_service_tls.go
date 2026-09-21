package main

import (
	"slices"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

// Physical membership is one live authenticated directory for control and
// snapshot listeners. Enrollment needs no duplicate TLS allowlist publication;
// exact operation capabilities/grants remain the responsibility of each handler.
func newRF3ServiceTLS(manifest rf3Manifest, profile *rafttransport.PeerTLS, class rafttransport.TrafficClass, principals []rafttransport.NodeID, current func() *rafttransport.StaticRegistry) (*servicetls.Server, error) {
	if profile == nil || current == nil {
		return nil, servicetls.ErrInvalidProfile
	}
	principals = slices.Clone(principals)
	domain := profile.LocalIdentity().TrustDomain
	return servicetls.NewPeerAuthorizedServer(profile, class, func(connection rafttransport.PeerConnection) bool {
		identity := connection.PeerIdentity()
		if identity.TrustDomain != domain || connection.PeerKeyDigest() == ([32]byte{}) {
			return false
		}
		if registry := current(); registry != nil {
			if _, err := registry.PhysicalPeer(identity.Node); err == nil {
				return registry.VerifyPeerConnectionBinding(connection) == nil
			}
		}
		// Explicit service principals are CA-authenticated identities with an
		// independent capability policy. They do not acquire physical group roles.
		if slices.Contains(principals, identity.Node) {
			return true
		}
		// Frontend bootstrap readers use a distinct identity from their physical
		// host. Their persisted discovery receipts bind the exact TLS key.
		for _, seed := range manifest.GatewaySeeds {
			if seed.Valid() && seed.NodeID == identity.Node && seed.SPKIPinDigest == connection.PeerKeyDigest() {
				return true
			}
		}
		return false
	})
}
