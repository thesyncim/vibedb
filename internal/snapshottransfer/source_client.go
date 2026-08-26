package snapshottransfer

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type SourceControlStreamOpener interface {
	OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type SourceControlClientOptions struct {
	Opener        SourceControlStreamOpener
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
}

type SourceControlClient struct {
	opener        SourceControlStreamOpener
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
}

func NewSourceControlClient(options SourceControlClientOptions) (*SourceControlClient, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrSourceControl
	}
	return &SourceControlClient{
		opener: options.Opener, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline,
	}, nil
}

// PrepareReplicaMoveSnapshot sends one exact command. Once request writing
// starts, transport failure is outcome-unknown: callers resolve it by replaying
// the same operation/step, which the source journal settles idempotently.
func (client *SourceControlClient) PrepareReplicaMoveSnapshot(
	ctx context.Context,
	request SourceControlRequest,
) (Descriptor, error) {
	source := request.SourceNode
	if client == nil || ctx == nil || source == (rafttransport.NodeID{}) ||
		!validSourceControlRequest(request) {
		return Descriptor{}, ErrSourceControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return Descriptor{}, cause
	}
	connection, err := client.opener.OpenShardControl(ctx, source)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return Descriptor{}, err
	}
	if connection == nil {
		return Descriptor{}, ErrSourceControl
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{
		ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation,
	}
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		peer.Node != source || peer.TrustDomain != wantDomain {
		return Descriptor{}, ErrSourceUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	writeDeadline := boundedBootstrapDeadline(ctx, client.writeDeadline())
	if writeDeadline.IsZero() {
		return Descriptor{}, ErrSourceControl
	}
	if err = connection.SetWriteDeadline(writeDeadline); err != nil {
		return Descriptor{}, err
	}
	if err = WriteSourceControlRequest(connection, request); err != nil {
		return Descriptor{}, errors.Join(ErrSourceOutcomeUnknown, err)
	}
	readDeadline := boundedBootstrapDeadline(ctx, client.readDeadline())
	if readDeadline.IsZero() {
		return Descriptor{}, ErrSourceOutcomeUnknown
	}
	if err = connection.SetReadDeadline(readDeadline); err != nil {
		return Descriptor{}, errors.Join(ErrSourceOutcomeUnknown, err)
	}
	record, err := ReadSourceControlResponse(connection)
	if err != nil {
		return Descriptor{}, errors.Join(ErrSourceOutcomeUnknown, err)
	}
	if record.Request != request || record.State != SourceControlComplete ||
		!descriptorMatchesSourceRequest(record.Descriptor, request) {
		return Descriptor{}, ErrSourceConflict
	}
	return record.Descriptor, nil
}
