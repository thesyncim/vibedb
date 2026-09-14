package main

import (
	"context"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func (installer *rf3DynamicLearnerInstaller) bindCertifiedLocalPeer(ctx context.Context) error {
	target := installer.spec.Target
	factory := installer.factory
	registry := factory.runtime.registry
	domain := factory.profile.LocalIdentity().TrustDomain
	intent := rafttransport.EnrollmentIntent{
		Domain: domain, Digest: rf3EmptyNodePeerEnrollmentDigest(installer.intent.ExpectedManifestDigest, target.Node),
		DirectoryRevision: registry.PeerDirectoryRevision(),
		Peer: rafttransport.PhysicalPeer{NodeID: target.Node, TrustDomain: domain, Incarnation: target.NodeIncarnation,
			Revision: target.NodeRevision, ServiceKeyDigest: [32]byte(target.ServiceKeyDigest), Endpoint: target.PeerAddress, State: rafttransport.PeerEnrolled},
	}
	// The spec was already validated against the committed enrollment intent
	// and exact retained preparation receipt before this installer was created.
	verifier := rafttransport.EnrollmentVerifierFunc(func(candidate rafttransport.EnrollmentIntent) error {
		if candidate.Peer.NodeID != installer.intent.Target.Node || candidate.Peer.Incarnation != installer.intent.Target.NodeIncarnation ||
			candidate.Peer.Revision != installer.intent.TargetNodeRevision || candidate.Peer.Endpoint != target.PeerAddress ||
			candidate.Peer.ServiceKeyDigest != [32]byte(target.ServiceKeyDigest) {
			return nodecontrol.ErrStale
		}
		return nil
	})
	return registry.BindLocalPeerContext(ctx, intent, factory.profile, factory.incarnation, verifier)
}
