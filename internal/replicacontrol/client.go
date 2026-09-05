package replicacontrol

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type StreamOpener interface {
	OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type ClientOptions struct {
	// MaxHealthRoundConnections bounds idle connections retained by a health sweep.
	// Zero keeps one-shot behavior. Reserve opener capacity for other controls.
	MaxHealthRoundConnections int
	Opener                    StreamOpener
	ReadDeadline              rafttransport.DeadlineFunc
	WriteDeadline             rafttransport.DeadlineFunc
}

type Client struct {
	healthRoundActive         atomic.Bool
	maxHealthRoundConnections int
	opener                    StreamOpener
	readDeadline              rafttransport.DeadlineFunc
	writeDeadline             rafttransport.DeadlineFunc
}

func NewClient(options ClientOptions) (*Client, error) {
	if options.MaxHealthRoundConnections < 0 || options.MaxHealthRoundConnections > 2048 || options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrControl
	}
	return &Client{maxHealthRoundConnections: options.MaxHealthRoundConnections, opener: options.Opener, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline}, nil
}

// Observe performs one attempt. Once request writing begins, any failure is
// outcome-unknown only in the transport sense; the operation is read-only and
// the caller may safely replay the exact request.
func (client *Client) Observe(
	ctx context.Context,
	node rafttransport.NodeID,
	request Request,
) (Observation, error) {
	if client == nil || ctx == nil || node == (rafttransport.NodeID{}) || request.HealthOnly || !validRequest(request) {
		return Observation{}, ErrControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return Observation{}, cause
	}
	connection, err := client.opener.OpenShardControl(ctx, node)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return Observation{}, err
	}
	if connection == nil {
		return Observation{}, ErrControl
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID,
		ClusterIncarnation: request.Group.ClusterIncarnation}
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		peer.Node != node || peer.TrustDomain != wantDomain {
		return Observation{}, ErrUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := boundedDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return Observation{}, ErrControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return Observation{}, err
	}
	if err = WriteRequest(connection, request); err != nil {
		return Observation{}, err
	}
	if deadline := boundedDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return Observation{}, ErrControl
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return Observation{}, err
	}
	observation, err := ReadResponse(connection)
	if err != nil {
		return Observation{}, err
	}
	expected := request
	if expected.ExpectedReplicaSetVersion == 0 {
		// The canonical response derives this value from its authenticated
		// state. Only this read-only discovery field may differ from the request.
		expected.ExpectedReplicaSetVersion = observation.Publication.ReplicaSetVersion
	}
	if observation.Request != expected {
		return Observation{}, errors.Join(ErrStale, ErrControl)
	}
	return observation, nil
}

// ObserveHealth performs one bounded liveness-only observation. The request
// is always marked HealthOnly before it is sent, and no full observation
// fallback is permitted when the health grammar is unavailable.
func (client *Client) ObserveHealth(
	ctx context.Context,
	node rafttransport.NodeID,
	request Request,
) (HealthObservation, error) {
	request.HealthOnly = true
	if client == nil || ctx == nil || node == (rafttransport.NodeID{}) || !validRequest(request) {
		return HealthObservation{}, ErrControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return HealthObservation{}, cause
	}
	connection, err := client.opener.OpenShardControl(ctx, node)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return HealthObservation{}, err
	}
	if connection == nil {
		return HealthObservation{}, ErrControl
	}
	defer connection.Close()
	return client.observeHealthConnection(ctx, node, request, connection)
}

func (client *Client) observeHealthConnection(ctx context.Context, node rafttransport.NodeID,
	request Request, connection rafttransport.PeerConnection) (HealthObservation, error) {
	var err error
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID,
		ClusterIncarnation: request.Group.ClusterIncarnation}
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		peer.Node != node || peer.TrustDomain != wantDomain {
		return HealthObservation{}, ErrUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := boundedDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return HealthObservation{}, ErrControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return HealthObservation{}, err
	}
	if err = WriteRequest(connection, request); err != nil {
		return HealthObservation{}, err
	}
	if deadline := boundedDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return HealthObservation{}, ErrControl
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return HealthObservation{}, err
	}
	observation, err := ReadHealthObservation(connection)
	if err != nil {
		return HealthObservation{}, err
	}
	if observation.Request != request {
		return HealthObservation{}, errors.Join(ErrStale, ErrControl)
	}
	return observation, nil
}
