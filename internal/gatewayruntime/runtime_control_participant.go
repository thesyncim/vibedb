package gatewayruntime

import (
	"fmt"
	"net"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

// Both the reconciler and every other admitting frontend use this exact
// authenticated drain endpoint. Controller ownership does not remove a
// frontend's pinned catalog generations from the cluster drain certificate.
func (runtime *Runtime) openCatalogDrainService(manifest gatewayReplicaControlManifest,
	catalog gatewayReplicaCatalogReader,
) error {
	profile := runtime.config.TLSProfile
	if profile == nil || runtime.config.Authorization == nil || catalog == nil ||
		manifest.Local.Member.Node != profile.LocalIdentity().Node {
		return fmt.Errorf("%w: drain participation requires matching authenticated gateway identity", ErrInvalidConfig)
	}
	handshake := servicetls.FixedDeadline(runtime.config.TLSHandshakeTimeout)
	read := servicetls.FixedDeadline(time.Duration(manifest.Bounds.ReadTimeout) * time.Millisecond)
	write := servicetls.FixedDeadline(time.Duration(manifest.Bounds.WriteTimeout) * time.Millisecond)
	nodes := make([]rafttransport.NodeID, len(manifest.Gateways))
	roster := make(map[rafttransport.NodeID]uint64, len(manifest.Gateways))
	for index, endpoint := range manifest.Gateways {
		nodes[index] = endpoint.Member.Node
		roster[endpoint.Member.Node] = endpoint.Member.Incarnation
	}
	authorizer, err := servicetls.NewNodeAuthorizer(nodes)
	if err != nil {
		return fmt.Errorf("open gateway control authorizer: %w", err)
	}
	runtime.controlTLS, err = servicetls.NewServer(
		profile.WithLocalGatewayControlConnections(), rafttransport.TrafficGatewayControl, authorizer,
	)
	if err != nil {
		return fmt.Errorf("open gateway control TLS: %w", err)
	}
	runtime.controlService, err = gateway.NewClusterCatalogDrainControlService(
		gateway.ClusterCatalogDrainControlOptions{Holder: runtime.holder,
			Catalog: gatewayCatalogDigestVerifier{catalog: catalog}, Member: manifest.Local.Member,
			Authorize: func(identity rafttransport.PeerIdentity, _ gateway.ClusterCatalogDrainRequest) bool {
				incarnation, found := roster[identity.Node]
				return found && incarnation != 0 && runtime.config.Authorization.Check(
					identity.Node, serviceauthz.CapabilityTopology,
				) == serviceauthz.DecisionAllow
			}, ReadDeadline: read, WriteDeadline: write},
	)
	if err != nil {
		return fmt.Errorf("open catalog drain service: %w", err)
	}
	runtime.controlListener, err = net.Listen("tcp", manifest.Local.Address)
	if err != nil {
		return fmt.Errorf("listen gateway control %q: %w", manifest.Local.Address, err)
	}
	runtime.controlHandshakeDeadline, runtime.controlReadDeadline, runtime.controlWriteDeadline = handshake, read, write
	runtime.replicaControlManifest = &manifest
	return nil
}
