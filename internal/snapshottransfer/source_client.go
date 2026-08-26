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
	if record.Request != request ||
		(record.State != SourceControlComplete && record.State != SourceControlReleased) ||
		!descriptorMatchesSourceRequest(record.Descriptor, request) {
		return Descriptor{}, ErrSourceConflict
	}
	return record.Descriptor, nil
}

// ReleaseReplicaMoveSnapshot asks the source to durably release the exact
// export after the controller has obtained the target's BootstrapComplete
// witness. Transport failure is outcome-unknown and is resolved by replaying
// the identical operation/step request.
func (client *SourceControlClient) ReleaseReplicaMoveSnapshot(
	ctx context.Context,
	request SourceControlRequest,
	descriptor Descriptor,
) error {
	if !descriptorMatchesSourceRequest(descriptor, request) {
		return ErrSourceConflict
	}
	record, err := client.executeRelease(ctx, request)
	if err != nil {
		return err
	}
	if record.State != SourceControlReleased || record.Request != request ||
		record.Descriptor != descriptor {
		return ErrSourceConflict
	}
	return nil
}

func (client *SourceControlClient) executeRelease(
	ctx context.Context,
	request SourceControlRequest,
) (SourceControlRecord, error) {
	source := request.SourceNode
	if client == nil || ctx == nil || source == (rafttransport.NodeID{}) ||
		!validSourceControlRequest(request) {
		return SourceControlRecord{}, ErrSourceControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return SourceControlRecord{}, cause
	}
	connection, err := client.opener.OpenShardControl(ctx, source)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return SourceControlRecord{}, err
	}
	if connection == nil {
		return SourceControlRecord{}, ErrSourceControl
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}
	if connection.TrafficClass() != rafttransport.TrafficShardControl || peer.Node != source || peer.TrustDomain != wantDomain {
		return SourceControlRecord{}, ErrSourceUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := boundedBootstrapDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return SourceControlRecord{}, ErrSourceControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return SourceControlRecord{}, err
	}
	var raw [SourceControlRequestBytes]byte
	encoded, err := appendSourceControlReleaseRequest(raw[:0], request)
	if err != nil {
		return SourceControlRecord{}, err
	}
	if err = writeFull(connection, encoded); err != nil {
		return SourceControlRecord{}, errors.Join(ErrSourceOutcomeUnknown, err)
	}
	if deadline := boundedBootstrapDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return SourceControlRecord{}, ErrSourceOutcomeUnknown
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return SourceControlRecord{}, errors.Join(ErrSourceOutcomeUnknown, err)
	}
	record, err := ReadSourceControlResponse(connection)
	if err != nil {
		return SourceControlRecord{}, errors.Join(ErrSourceOutcomeUnknown, err)
	}
	return record, nil
}
