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
	// The listener admits the current directory plus the static bootstrap
	// roster. Historical identities remain usable only through an immutable
	// drain envelope; they are never reintroduced as a general TLS allowlist.
	gateways := manifest.Gateways
	if runtime.controlDirectory != nil {
		gateways = mergeGatewayControlEndpoints(
			gateways, controlDirectoryGatewayEndpoints(runtime.controlDirectory),
		)
	}
	nodes := make([]rafttransport.NodeID, len(gateways))
	roster := make(map[rafttransport.NodeID]map[uint64]struct{}, len(gateways))
	for index, endpoint := range gateways {
		nodes[index] = endpoint.Member.Node
		incarnations := roster[endpoint.Member.Node]
		if incarnations == nil {
			incarnations = make(map[uint64]struct{})
			roster[endpoint.Member.Node] = incarnations
		}
		incarnations[endpoint.Member.Incarnation] = struct{}{}
	}
	if runtime.serviceDirectory != nil {
		if cut, found := runtime.serviceDirectory.Cut(); found {
			for _, binding := range cut.Bindings {
				if binding.Roles&serviceauthz.ServiceRoleStorage != 0 &&
					binding.Lifecycle == serviceauthz.ServiceJoining {
					nodes = append(nodes, binding.Principal)
				}
			}
		}
	}
	runtime.controlRosterMu.Lock()
	runtime.controlRoster = roster
	runtime.controlRosterMu.Unlock()
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
	if runtime.serviceDirectory != nil {
		if err := runtime.controlTLS.BindPeerAuthorizer(func(connection rafttransport.PeerConnection) bool {
			binding := rafttransport.Binding(connection)
			peer := serviceauthz.AuthenticatedPeer{Identity: binding.Identity, KeyDigest: binding.ServiceKeyDigest}
			return runtime.serviceDirectory.CheckGatewayPeer(peer) == serviceauthz.DecisionAllow ||
				runtime.serviceDirectory.CheckBootstrapPeer(peer) == serviceauthz.DecisionAllow
		}); err != nil {
			return fmt.Errorf("bind gateway control service directory: %w", err)
		}
	}
	runtime.controlAuthorizer = authorizer
	if runtime.serviceDirectory != nil {
		runtime.bootstrapReadService, err = newGatewayBootstrapReadService(
			runtime.authority, profile.LocalIdentity().TrustDomain,
			func(identity rafttransport.PeerIdentity, record gateway.NodeRecord) bool {
				return identity.TrustDomain == profile.LocalIdentity().TrustDomain && identity.Node == record.NodeID
			}, servicetls.FixedDeadline(runtime.config.TLSHandshakeTimeout), read, min(int(manifest.Bounds.MaxConnections), 64),
			func(binding rafttransport.PeerBinding, _ gateway.NodeRecord) bool {
				peer := serviceauthz.AuthenticatedPeer{Identity: binding.Identity, KeyDigest: binding.ServiceKeyDigest}
				return runtime.serviceDirectory.CheckBootstrapPeer(peer) == serviceauthz.DecisionAllow
			},
		)
		if err != nil {
			return fmt.Errorf("open gateway bootstrap read service: %w", err)
		}
	}
	runtime.controlService, err = gateway.NewClusterCatalogDrainControlService(
		gateway.ClusterCatalogDrainControlOptions{Holder: runtime.holder,
			Catalog: gatewayCatalogDigestVerifier{catalog: catalog}, Member: manifest.Local.Member,
			Authorize: func(identity rafttransport.PeerIdentity, _ gateway.ClusterCatalogDrainRequest) bool {
				runtime.controlRosterMu.RLock()
				_, found := runtime.controlRoster[identity.Node]
				runtime.controlRosterMu.RUnlock()
				return found && runtime.config.Authorization.Check(
					identity.Node, serviceauthz.CapabilityTopology,
				) == serviceauthz.DecisionAllow
			},
			AuthorizeEnvelope: func(identity rafttransport.PeerIdentity, envelope gateway.ClusterCatalogDrainEnvelope) bool {
				if identity.Node == (rafttransport.NodeID{}) {
					return false
				}
				runtime.controlRosterMu.RLock()
				incarnations, found := runtime.controlRoster[identity.Node]
				runtime.controlRosterMu.RUnlock()
				if !found || runtime.config.Authorization.Check(
					identity.Node, serviceauthz.CapabilityTopology,
				) != serviceauthz.DecisionAllow {
					return false
				}
				for index := 0; index < envelope.Fence.MemberCount(); index++ {
					member, memberFound := envelope.Fence.Member(index)
					if memberFound && member.Node == identity.Node {
						if _, incarnationFound := incarnations[member.Incarnation]; incarnationFound {
							return true
						}
					}
				}
				return false
			}, AuthorizeAuthenticated: func(binding rafttransport.PeerBinding, _ gateway.ClusterCatalogDrainRequest) bool {
				if runtime.serviceDirectory == nil {
					return true
				}
				peer := serviceauthz.AuthenticatedPeer{Identity: binding.Identity, KeyDigest: binding.ServiceKeyDigest}
				return runtime.serviceDirectory.CheckGatewayPeer(peer) == serviceauthz.DecisionAllow
			}, AuthorizeAuthenticatedEnvelope: func(binding rafttransport.PeerBinding, _ gateway.ClusterCatalogDrainEnvelope) bool {
				if runtime.serviceDirectory == nil {
					return true
				}
				peer := serviceauthz.AuthenticatedPeer{Identity: binding.Identity, KeyDigest: binding.ServiceKeyDigest}
				return runtime.serviceDirectory.CheckGatewayPeer(peer) == serviceauthz.DecisionAllow
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
