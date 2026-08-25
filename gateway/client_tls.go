package gateway

import (
	"context"
	"net"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

// ClientTLS is the shipped gateway's mutually authenticated application-client
// capability. Certificate principals are exact binary NodeIDs; authorization
// never depends on X.509 subjects, DNS names, or request strings.
type ClientTLS struct {
	server *servicetls.Server
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

func (capability *ClientTLS) Stats() ClientTLSStats {
	if capability == nil {
		return ClientTLSStats{}
	}
	return capability.server.Stats()
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
