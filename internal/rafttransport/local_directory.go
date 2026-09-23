package rafttransport

import "context"

// BindLocalPeerContext replaces an empty-process placeholder with certified
// local directory evidence. Unlike EnrollPeer it cannot add a remote peer or
// any group authority. The running process supplies its actual TLS profile
// and physical incarnation independently of the directory proof.
func (registry *StaticRegistry) BindLocalPeerContext(ctx context.Context, intent EnrollmentIntent,
	profile *PeerTLS, incarnation uint64, verifier EnrollmentVerifier,
) error {
	if ctx == nil || registry == nil || profile == nil || incarnation == 0 {
		return ErrPeerUnauthorized
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	intent, err := registry.normalizeEnrollment(intent)
	if err != nil {
		return err
	}
	local := profile.LocalIdentity()
	if intent.Group != (Member{}).Group || intent.Peer.NodeID != registry.local || local.Node != registry.local ||
		local.TrustDomain != registry.trustDomain || intent.Peer.Incarnation != incarnation ||
		intent.Peer.ServiceKeyDigest != profile.LocalServiceKeyDigest() || intent.Peer.Endpoint == "" {
		return ErrPeerUnauthorized
	}
	if err := verifyEnrollmentContext(ctx, intent, verifier); err != nil {
		return err
	}
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	current := registry.dynamic.Load()
	existing, found := registry.physicalPeerFrom(current, registry.local)
	if !found || existing.State != PeerEnrolled {
		return ErrPeerConflict
	}
	// Identical directory cuts can arrive in distinct certified group proofs.
	// Preserve the first digest and directory revision on an exact replay.
	if samePhysicalIdentity(existing, intent.Peer) {
		return nil
	}
	if intent.DirectoryRevision != registry.currentDirectoryRevision(current) {
		return ErrPeerConflict
	}
	placeholder := existing.EnrollmentDigest == ([32]byte{}) && existing.ServiceKeyDigest == ([32]byte{}) && existing.Endpoint == ""
	if !placeholder && (existing.Incarnation != incarnation || existing.ServiceKeyDigest != intent.Peer.ServiceKeyDigest || intent.Peer.Revision <= existing.Revision) {
		return ErrPeerConflict
	}
	if registry.currentDirectoryRevision(current) == ^uint64(0) {
		return ErrPeerConflict
	}
	next := cloneDynamicEnrollment(current)
	if next.physical == nil {
		next.physical = make(map[NodeID]PhysicalPeer)
	}
	next.physical[registry.local] = intent.Peer
	next.directoryRevision = registry.currentDirectoryRevision(current) + 1
	merged := registry.mergedPhysical(current)
	merged[registry.local] = intent.Peer
	next.peerDigest = physicalPeerDigest(merged)
	registry.dynamic.Store(next)
	return nil
}
