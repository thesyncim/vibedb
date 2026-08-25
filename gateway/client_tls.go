package gateway

import (
	"context"
	"net"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

// ClientTLS is the shipped gateway's mutually authenticated application-client
// capability. Certificate principals are exact binary NodeIDs; authorization
// never depends on X.509 subjects, DNS names, or request strings.
type ClientTLS struct {
	server *servicetls.Server
	gate   *serviceauthz.Gate
}

// NewAuthorizedClientTLS binds TLS admission and service authorization to one
// immutable policy generation. The TLS allowlist is derived from the same
// exact binary principals; it cannot drift from the request policy.
func NewAuthorizedClientTLS(
	profile *rafttransport.PeerTLS,
	policy *serviceauthz.Policy,
) (*ClientTLS, error) {
	if policy == nil {
		return nil, servicetls.ErrInvalidProfile
	}
	authorizer, err := servicetls.NewNodeAuthorizer(policy.Nodes())
	if err != nil {
		return nil, err
	}
	server, err := servicetls.NewServer(profile, rafttransport.TrafficGatewayClient, authorizer)
	if err != nil {
		return nil, err
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		return nil, err
	}
	return &ClientTLS{server: server, gate: gate}, nil
}

type ClientTLSLimits = servicetls.Limits
type ClientTLSStats = servicetls.Stats

func NewClientTLS(profile *rafttransport.PeerTLS, allowed []rafttransport.NodeID) (*ClientTLS, error) {
	authorizer, err := servicetls.NewNodeAuthorizer(allowed)
	if err != nil {
		return nil, err
	}
	server, err := servicetls.NewServer(profile, rafttransport.TrafficGatewayClient, authorizer)
	if err != nil {
		return nil, err
	}
	return &ClientTLS{server: server}, nil
}

// RotateClientTLS publishes one new certificate/authorization generation and
// revokes every stream admitted by the preceding generation.
func (capability *ClientTLS) RotateClientTLS(profile *rafttransport.PeerTLS, allowed []rafttransport.NodeID) error {
	authorizer, err := servicetls.NewNodeAuthorizer(allowed)
	if err != nil || capability == nil {
		return servicetls.ErrInvalidProfile
	}
	return capability.server.Rotate(profile, authorizer)
}

// RotateAuthorization atomically publishes a higher policy generation and
// closes every active stream from the preceding TLS/authorization generation.
func (capability *ClientTLS) RotateAuthorization(
	profile *rafttransport.PeerTLS,
	policy *serviceauthz.Policy,
) error {
	if capability == nil || capability.server == nil || capability.gate == nil || policy == nil {
		return servicetls.ErrInvalidProfile
	}
	authorizer, err := servicetls.NewNodeAuthorizer(policy.Nodes())
	if err != nil {
		return err
	}
	if policy.Generation() <= capability.gate.Generation() {
		return serviceauthz.ErrInvalidPolicy
	}
	// Revoke the old TLS generation before publishing any new privilege. A
	// connection admitted in the narrow interval sees only the older policy and
	// therefore fails closed; it can never acquire the new generation early.
	if err = capability.server.Rotate(profile, authorizer); err != nil {
		return err
	}
	return capability.gate.Rotate(policy)
}

func (capability *ClientTLS) Stats() ClientTLSStats {
	if capability == nil {
		return ClientTLSStats{}
	}
	return capability.server.Stats()
}

func (capability *ClientTLS) Authorize(
	ctx context.Context,
	requested serviceauthz.Capability,
	audit serviceauthz.AuditSink,
) serviceauthz.DecisionCode {
	if capability == nil || capability.gate == nil {
		return serviceauthz.DecisionDenyInvalid
	}
	authority, ok := serviceauthz.FromContext(ctx)
	if !ok {
		return serviceauthz.DecisionDenyInvalid
	}
	return serviceauthz.CheckAndAudit(capability.gate, audit, authority.Node,
		authority.Generation, requested)
}

// ServeAuthorizedClients hands each stream a context containing only its exact
// fixed-width certificate principal and the admitted policy generation.
func (capability *ClientTLS) ServeAuthorizedClients(
	ctx context.Context,
	listener net.Listener,
	limits ClientTLSLimits,
	handle func(context.Context, net.Conn),
) error {
	if capability == nil || capability.gate == nil || handle == nil {
		if listener != nil {
			_ = listener.Close()
		}
		return servicetls.ErrInvalidProfile
	}
	return capability.server.Serve(ctx, listener, limits,
		func(ctx context.Context, connection rafttransport.PeerConnection) {
			authorized, err := serviceauthz.WithAuthority(ctx, serviceauthz.Authority{
				Node: connection.PeerIdentity().Node, Generation: capability.gate.Generation(),
			})
			if err == nil {
				handle(authorized, connection)
			}
		})
}

// ServeAuthenticatedClients is the only remotely reachable gateway listener
// path. The explicit development server remains a separate loopback-only call.
func (capability *ClientTLS) ServeAuthenticatedClients(
	ctx context.Context,
	listener net.Listener,
	limits ClientTLSLimits,
	handle func(context.Context, net.Conn),
) error {
	if capability == nil || handle == nil {
		if listener != nil {
			_ = listener.Close()
		}
		return servicetls.ErrInvalidProfile
	}
	return capability.server.Serve(ctx, listener, limits,
		func(ctx context.Context, connection rafttransport.PeerConnection) { handle(ctx, connection) })
}
