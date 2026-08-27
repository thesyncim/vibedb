package schemainstall

import (
	"context"
	"crypto/sha256"
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
		return nil, ErrInvalid
	}
	return &Client{opener: options.Opener, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline}, nil
}

func (client *Client) Prepare(
	ctx context.Context, node rafttransport.NodeID, request Request, bundle []byte,
) (Receipt, error) {
	record, err := client.execute(ctx, node, CommandPrepare, request, bundle,
		Authorization{}, DrainProof{})
	if err != nil {
		return Receipt{}, err
	}
	return receiptFor(record), nil
}

func (client *Client) Authorize(
	ctx context.Context, node rafttransport.NodeID, request Request, authorization Authorization,
) (Record, error) {
	return client.execute(ctx, node, CommandAuthorize, request, nil, authorization, DrainProof{})
}

func (client *Client) Activate(
	ctx context.Context, node rafttransport.NodeID, request Request, authorization Authorization,
) (Record, error) {
	return client.execute(ctx, node, CommandActivate, request, nil, authorization, DrainProof{})
}

func (client *Client) Drain(
	ctx context.Context, node rafttransport.NodeID, request Request,
	authorization Authorization, proof DrainProof,
) (Record, error) {
	return client.execute(ctx, node, CommandDrain, request, nil, authorization, proof)
}

func (client *Client) execute(
	ctx context.Context, node rafttransport.NodeID, command Command, request Request, bundle []byte,
	authorization Authorization, proof DrainProof,
) (Record, error) {
	if client == nil || ctx == nil || node == (rafttransport.NodeID{}) || !validRequest(request) ||
		(command == CommandPrepare && (uint64(len(bundle)) != request.BundleBytes ||
			sha256.Sum256(bundle) != request.BundleDigest) || command != CommandPrepare && len(bundle) != 0) {
		return Record{}, ErrInvalid
	}
	if cause := context.Cause(ctx); cause != nil {
		return Record{}, cause
	}
	connection, err := client.opener.OpenShardControl(ctx, node)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return Record{}, err
	}
	if connection == nil {
		return Record{}, ErrInvalid
	}
	defer connection.Close()
	domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID,
		ClusterIncarnation: request.Group.ClusterIncarnation}
	peer := connection.PeerIdentity()
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		peer.Node != node || peer.TrustDomain != domain {
		return Record{}, rafttransport.ErrUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := boundedClientDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return Record{}, ErrInvalid
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return Record{}, err
	}
	raw, err := AppendControlRequest(nil, command, request, authorization, proof)
	if err != nil {
		return Record{}, err
	}
	if err = writeAll(connection, raw); err == nil && len(bundle) != 0 {
		err = writeAll(connection, bundle)
	}
	clear(raw)
	if err != nil {
		return Record{}, errors.Join(ErrOutcomeUnknown, err)
	}
	if deadline := boundedClientDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return Record{}, ErrOutcomeUnknown
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return Record{}, errors.Join(ErrOutcomeUnknown, err)
	}
	record, err := ReadControlResponse(connection)
	if err != nil {
		if errors.Is(err, ErrInvalid) || errors.Is(err, ErrConflict) ||
			errors.Is(err, ErrMissing) || errors.Is(err, ErrBound) ||
			errors.Is(err, rafttransport.ErrUnauthorized) {
			return Record{}, err
		}
		return Record{}, errors.Join(ErrOutcomeUnknown, err)
	}
	wantState := StatePrepared
	switch command {
	case CommandAuthorize:
		wantState = StateAuthorized
	case CommandActivate:
		wantState = StateActive
	case CommandDrain:
		wantState = StateDrained
	}
	if record.Request != request || record.State < wantState ||
		command != CommandPrepare && record.Authorization != authorization ||
		command == CommandDrain && record.DrainProof != proof {
		return Record{}, ErrConflict
	}
	return record, nil
}

func boundedClientDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		return time.Time{}
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(configured) {
		return deadline
	}
	return configured
}
