package replicaaction

import (
	"context"
	"errors"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type StreamOpener interface {
	OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type ClientOptions struct {
	Opener        StreamOpener
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
}

type Client struct {
	opener                      StreamOpener
	readDeadline, writeDeadline rafttransport.DeadlineFunc
}

func NewClient(options ClientOptions) (*Client, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrControl
	}
	return &Client{opener: options.Opener, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline}, nil
}

// Execute sends one exact action attempt. Any failure after request writing
// begins is outcome-unknown; replaying the same operation and step lets the
// remote durable journal settle without duplicating the mutation.
func (client *Client) Execute(
	ctx context.Context,
	node rafttransport.NodeID,
	request Request,
) error {
	if client == nil || ctx == nil || node == (rafttransport.NodeID{}) || !validRequest(request) {
		return ErrControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	connection, err := client.opener.OpenShardControl(ctx, node)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return err
	}
	if connection == nil {
		return ErrControl
	}
	defer connection.Close()
	domain := rafttransport.TrustDomain{ClusterID: request.Fence.Group.ClusterID,
		ClusterIncarnation: request.Fence.Group.ClusterIncarnation}
	peer := connection.PeerIdentity()
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		peer.Node != node || peer.TrustDomain != domain {
		return ErrUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := boundedDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return ErrControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err = WriteRequest(connection, request); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	if deadline := boundedDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return ErrOutcomeUnknown
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	record, err := ReadResponse(connection)
	if err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	if record.Request.Operation != request.Operation || record.Request.Step != request.Step ||
		record.Request.Kind != request.Kind || record.State != Complete {
		return ErrConflict
	}
	return nil
}

func boundedDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		return time.Time{}
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(configured) {
		return deadline
	}
	return configured
}
