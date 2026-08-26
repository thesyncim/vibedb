package snapshottransfer

import (
	"context"
	"errors"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// BootstrapControlStreamOpener is the narrow authenticated shard-control
// capability consumed by BootstrapControlClient. Implementations must open a
// fresh stream authenticated for TrafficShardControl and the requested node.
type BootstrapControlStreamOpener interface {
	OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type BootstrapControlClientOptions struct {
	Opener        BootstrapControlStreamOpener
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
}

// BootstrapControlClient performs one bounded command per authenticated
// stream. It deliberately has no retry loop: once request writing begins, any
// transport failure is outcome-unknown and must be resolved through durable
// observation or an exact caller-controlled replay.
type BootstrapControlClient struct {
	opener        BootstrapControlStreamOpener
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
}

func NewBootstrapControlClient(options BootstrapControlClientOptions) (*BootstrapControlClient, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrBootstrapControl
	}
	return &BootstrapControlClient{
		opener: options.Opener, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline,
	}, nil
}

// Execute sends the exact request once and accepts only a complete response
// that echoes operation, step, and descriptor and whose terminal runtime
// identity matches the requested learner fence.
func (client *BootstrapControlClient) Execute(
	ctx context.Context,
	target rafttransport.NodeID,
	request BootstrapRequest,
) (BootstrapRecord, error) {
	if client == nil || ctx == nil || target == (rafttransport.NodeID{}) ||
		!validBootstrapRequest(request) {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return BootstrapRecord{}, cause
	}
	connection, err := client.opener.OpenShardControl(ctx, target)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return BootstrapRecord{}, err
	}
	if connection == nil {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{
		ClusterID:          request.Descriptor.Group.ClusterID,
		ClusterIncarnation: request.Descriptor.Group.ClusterIncarnation,
	}
	if connection.TrafficClass() != rafttransport.TrafficShardControl ||
		peer.Node != target || peer.TrustDomain != wantDomain {
		return BootstrapRecord{}, ErrBootstrapUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	writeDeadline := boundedBootstrapDeadline(ctx, client.writeDeadline())
	if writeDeadline.IsZero() {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	if err = connection.SetWriteDeadline(writeDeadline); err != nil {
		return BootstrapRecord{}, err
	}
	if err = WriteBootstrapRequest(connection, request); err != nil {
		return BootstrapRecord{}, errors.Join(ErrBootstrapOutcomeUnknown, err)
	}
	readDeadline := boundedBootstrapDeadline(ctx, client.readDeadline())
	if readDeadline.IsZero() {
		return BootstrapRecord{}, ErrBootstrapOutcomeUnknown
	}
	if err = connection.SetReadDeadline(readDeadline); err != nil {
		return BootstrapRecord{}, errors.Join(ErrBootstrapOutcomeUnknown, err)
	}
	record, err := ReadBootstrapResponse(connection)
	if err != nil {
		return BootstrapRecord{}, errors.Join(ErrBootstrapOutcomeUnknown, err)
	}
	if record.Request != request || record.State != BootstrapComplete ||
		!runtimeMatchesDescriptor(record.Identity, request.Descriptor) {
		return BootstrapRecord{}, ErrBootstrapConflict
	}
	return record, nil
}

func boundedBootstrapDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		return time.Time{}
	}
	if deadline, found := ctx.Deadline(); found && deadline.Before(configured) {
		return deadline
	}
	return configured
}
