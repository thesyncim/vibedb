package gatewayruntime

import (
	"context"
	"crypto/rand"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type scalingNodeInfoReader interface {
	Observe(context.Context, rafttransport.NodeID, nodecontrol.NodeInfoRequest) (nodecontrol.NodeInfoObservation, error)
}

type scalingNodeReadiness struct {
	client scalingNodeInfoReader
	domain rafttransport.TrustDomain
}

func (reader scalingNodeReadiness) VerifyNode(ctx context.Context, node gateway.NodeRecord) (gateway.NodeRecord, error) {
	if reader.client == nil || !node.Valid() {
		return gateway.NodeRecord{}, nodecontrol.ErrNodeInfoUnavailable
	}
	request := nodecontrol.NodeInfoRequest{Operation: nodecontrol.OpNodeInfo, NodeID: node.NodeID, Incarnation: node.Incarnation}
	if _, err := rand.Read(request.Nonce[:]); err != nil {
		return gateway.NodeRecord{}, err
	}
	observed, err := reader.client.Observe(ctx, node.NodeID, request)
	if err != nil {
		return gateway.NodeRecord{}, err
	}
	if !observed.ReadyForEnrollment() || observed.Nonce != request.Nonce || observed.Operation != request.Operation || observed.Store.ClusterID != reader.domain.ClusterID || observed.Store.ClusterIncarnation != reader.domain.ClusterIncarnation || observed.NodeID != node.NodeID || observed.Incarnation != node.Incarnation || observed.SPKIPinDigest != node.ServiceKeyDigest || observed.Endpoints.Peer != node.DataAddress || observed.Endpoints.Native != node.NativeAddress || observed.Endpoints.Control != node.ControlAddress || observed.Endpoints.Gateway != node.GatewayAddress {
		return gateway.NodeRecord{}, nodecontrol.ErrNodeInfoStale
	}
	node.Capacity = observed.ActualCapacity
	node.Used = observed.ActualUsage
	node.MigrationCapacity = observed.ActualMigrationCapacity
	node.MigrationUsed = observed.ActualMigrationUsed
	node.MaxReceives = observed.DeclaredMaxReceives
	node.ActiveReceives = observed.ActualActiveReceives
	return node, nil
}
