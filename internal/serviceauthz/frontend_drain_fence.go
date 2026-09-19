package serviceauthz

import (
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// CommittedFrontendDrainFence is the empty-admission form of a frontend
// retirement proof. It carries no connection token: a gateway that had no
// accepted frontend sockets still needs an authenticated, durable fence for
// its draining service binding. The fence tuple is copied from the catalog's
// own internal-service authorization cut.
type CommittedFrontendDrainFence struct {
	TrustDomain            rafttransport.TrustDomain
	PhysicalNode           rafttransport.NodeID
	PhysicalIncarnation    uint64
	PeerKeyDigest          [32]byte
	GatewayServiceID       rafttransport.NodeID
	GatewaySessionID       [16]byte
	GatewaySessionRevision uint64
	DrainID                [32]byte
	Revision               uint64
	Fence                  ServiceFence
}

// Valid checks the complete identity and session-bound fence. It deliberately
// does not admit a caller-populated bearer token or a fence with no resource
// coordinates.
func (fence CommittedFrontendDrainFence) Valid() bool {
	return fence.TrustDomain.ClusterID != ([16]byte{}) &&
		fence.TrustDomain.ClusterIncarnation != ([16]byte{}) &&
		fence.PhysicalNode != (rafttransport.NodeID{}) && fence.PhysicalIncarnation != 0 &&
		fence.PeerKeyDigest != ([32]byte{}) && fence.GatewayServiceID != (rafttransport.NodeID{}) &&
		fence.GatewaySessionID != ([16]byte{}) && fence.GatewaySessionRevision != 0 &&
		fence.DrainID != ([32]byte{}) && fence.Revision != 0 &&
		fence.Fence.validShape() && fence.Fence.SessionID == fence.GatewaySessionID &&
		fence.Fence.SessionRevision == fence.GatewaySessionRevision
}
