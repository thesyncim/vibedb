package main

import (
	"context"
	"net"
	"time"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

// rf3NodeBootstrapGatewayOpener maps only the bounded, manifest-persisted
// seed set into authenticated gateway-control streams.  It never accepts a
// redirect or a caller-supplied endpoint; BootstrapReadClient performs the
// post-handshake SPKI and identity checks.
type rf3NodeBootstrapGatewayOpener struct {
	transport *servicetls.Client
}

func (opener *rf3NodeBootstrapGatewayOpener) OpenBootstrapGatewayControl(
	ctx context.Context, seed nodecontrol.BootstrapGatewaySeed,
) (rafttransport.PeerConnection, error) {
	if opener == nil || opener.transport == nil || !seed.Valid() {
		return nil, nodecontrol.ErrBootstrapReadUnavailable
	}
	connection, err := opener.transport.Dial(ctx, seed.ControlAddress)
	if err != nil {
		return nil, err
	}
	peer, ok := connection.(rafttransport.PeerConnection)
	if !ok || peer == nil {
		_ = connection.Close()
		return nil, nodecontrol.ErrBootstrapReadUnavailable
	}
	return peer, nil
}

// newRF3NodeBootstrapClient builds the committed-directory reader used by an
// empty node.  The returned reader performs a fresh authenticated query for
// every intent and keeps no local enrollment cache.
func newRF3NodeBootstrapClient(
	profile *rafttransport.PeerTLS,
	seeds []nodecontrol.BootstrapGatewaySeed,
	target rafttransport.NodeID,
	incarnation uint64,
	readDeadline rafttransport.DeadlineFunc,
	writeDeadline rafttransport.DeadlineFunc,
) (*nodecontrol.BootstrapReadClient, *servicetls.Client, error) {
	if profile == nil || len(seeds) == 0 || target == (rafttransport.NodeID{}) || incarnation == 0 ||
		readDeadline == nil || writeDeadline == nil {
		return nil, nil, nodecontrol.ErrBootstrapRead
	}
	endpoints := make([]servicetls.Endpoint, len(seeds))
	for index, seed := range seeds {
		if !seed.Valid() {
			return nil, nil, nodecontrol.ErrBootstrapRead
		}
		endpoints[index] = servicetls.Endpoint{Address: seed.ControlAddress, Node: seed.NodeID}
	}
	dial := func(ctx context.Context, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	}
	transport, err := servicetls.NewClient(servicetls.ClientOptions{
		TLS: profile.WithLocalGatewayControlConnections(), Class: rafttransport.TrafficGatewayControl,
		Endpoints: endpoints, Dial: dial, HandshakeDeadline: writeDeadline,
		MaxConnections: len(seeds), MaxHandshakes: len(seeds),
	})
	if err != nil {
		return nil, nil, err
	}
	reader, err := nodecontrol.NewBootstrapReadClient(nodecontrol.BootstrapReadClientOptions{
		Opener: &rf3NodeBootstrapGatewayOpener{transport: transport}, Seeds: seeds,
		TrustDomain: profile.LocalIdentity().TrustDomain, PhysicalNode: target,
		Incarnation: incarnation, ReadDeadline: readDeadline, WriteDeadline: writeDeadline,
	})
	if err != nil {
		_ = transport.Close()
		return nil, nil, err
	}
	return reader, transport, nil
}

// bindRF3NodeBootstrapIntentReader must be called before constructing the
// nodecontrol Service.  A slot with no reader remains fail-closed, while this
// function publishes only the authenticated, seed-bounded implementation.
func bindRF3NodeBootstrapIntentReader(
	slot *nodecontrol.IntentReaderSlot,
	profile *rafttransport.PeerTLS,
	seeds []nodecontrol.BootstrapGatewaySeed,
	target rafttransport.NodeID,
	incarnation uint64,
	deadline rafttransport.DeadlineFunc,
) (*servicetls.Client, error) {
	if slot == nil || deadline == nil {
		return nil, nodecontrol.ErrBootstrapRead
	}
	reader, transport, err := newRF3NodeBootstrapClient(profile, seeds, target, incarnation, deadline, deadline)
	if err != nil {
		return nil, err
	}
	if err = slot.Set(reader); err != nil {
		_ = transport.Close()
		return nil, err
	}
	return transport, nil
}

func rf3BootstrapReadDeadline() rafttransport.DeadlineFunc {
	return func() time.Time { return time.Now().Add(rf3NetworkTimeout) }
}
