package main

import (
	"context"
	"errors"
	"net"
	"slices"
	"time"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// A source node retains its native command/session journal locally. Only the
// transport changes: the gateway checks the committed action step and executes
// its restricted topology grammar with the gateway's own authority.
func newRF3ProxiedSplitTopologyActions(profile *rafttransport.PeerTLS,
	authority serviceauthz.Authority, seeds []nodecontrol.BootstrapGatewaySeed,
	identity raftmember.RuntimeIdentity, lease *splitcontroller.RuntimeStoreLease,
	apply *sqldriver.ReplicatedApply,
) (rf3SplitTopologyActions, error) {
	if profile == nil || !authority.Valid() || authority.Node != profile.LocalIdentity().Node ||
		lease == nil || apply == nil || identity.Group == (raftmember.GroupKey{}) ||
		identity.MemberID == 0 || identity.StoreID == ([16]byte{}) || identity.NodeIncarnation == 0 ||
		identity.Group.ClusterID != profile.LocalIdentity().TrustDomain.ClusterID ||
		identity.Group.ClusterIncarnation != profile.LocalIdentity().TrustDomain.ClusterIncarnation ||
		len(seeds) == 0 || len(seeds) > nodecontrol.MaxBootstrapGatewaySeeds {
		return nil, errRF3Serving
	}
	seeds = slices.Clone(seeds)
	endpoints := make([]servicetls.Endpoint, len(seeds))
	for i, seed := range seeds {
		if !seed.Valid() {
			return nil, errRF3Serving
		}
		endpoints[i] = servicetls.Endpoint{Address: seed.ControlAddress, Node: seed.NodeID}
	}
	deadline := func() time.Time { return time.Now().Add(rf3NetworkTimeout) }
	return &rf3RetainedPruneFactory{tls: profile, authority: authority, lease: lease, source: apply,
		transport: func(ctx context.Context) (rf3SplitTopologyTransport, error) {
			step, found := splitcontroller.SourceTopologyStepFromContext(ctx)
			if !found {
				return nil, errRF3Serving
			}
			transport, err := servicetls.NewClient(servicetls.ClientOptions{
				TLS: profile.WithLocalGatewayControlConnections(), Class: rafttransport.TrafficGatewayControl,
				Endpoints: endpoints, HandshakeDeadline: deadline, MaxConnections: len(seeds), MaxHandshakes: len(seeds),
				Dial: func(ctx context.Context, address string) (net.Conn, error) {
					return (&net.Dialer{Timeout: rf3NetworkTimeout}).DialContext(ctx, "tcp", address)
				},
			})
			if err != nil {
				return nil, err
			}
			client, err := splitcontroller.NewSourceTopologyClient(splitcontroller.SourceTopologyClientOptions{
				Opener: &rf3NodeBootstrapGatewayOpener{transport: transport}, Seeds: seeds,
				TrustDomain: profile.LocalIdentity().TrustDomain, Source: identity, Step: step,
				ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 4,
			})
			if err != nil {
				return nil, errors.Join(err, transport.Close())
			}
			return &rf3ProxiedSplitTopologyTransport{SourceTopologyClient: client, transport: transport}, nil
		}}, nil
}

type rf3ProxiedSplitTopologyTransport struct {
	*splitcontroller.SourceTopologyClient
	transport *servicetls.Client
}

func (client *rf3ProxiedSplitTopologyTransport) Close() error {
	if client == nil {
		return nil
	}
	return errors.Join(client.SourceTopologyClient.Close(), client.transport.Close())
}
