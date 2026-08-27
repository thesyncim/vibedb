// Package clusterbackupservice adapts the target-free backup wire to the RF3
// owner. Keeping it separate prevents the artifact grammar/repository from
// depending on the serving runtime.
package clusterbackupservice

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

const AbsoluteMaxWorkspaceBytes = 64 << 20

type Owner interface {
	Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error)
	ReadLinearizableSnapshot(context.Context, raftservice.LinearizableSnapshotRequest) (*raftservice.LinearizableSnapshotCut, error)
}

type AuthorizeFunc func(rafttransport.PeerIdentity, clusterbackup.LiveRequest) bool

type Options struct {
	Owner                       Owner
	Authorize                   AuthorizeFunc
	ReadDeadline, WriteDeadline rafttransport.DeadlineFunc
	ChunkBytes                  int
	MaxConcurrent               int
}

type Metrics struct {
	Requests, Faults, LogicalArtifactBytes, SnapshotScanBytes uint64
}

type Service struct {
	options Options
	tokens  chan struct{}
	pool    sync.Pool
	request atomic.Uint64
	faults  atomic.Uint64
	logical atomic.Uint64
	scanned atomic.Uint64
}

func New(options Options) (*Service, error) {
	if options.Owner == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent <= 0 || options.MaxConcurrent > 64 ||
		options.ChunkBytes <= 0 || options.ChunkBytes > AbsoluteMaxWorkspaceBytes/options.MaxConcurrent {
		return nil, clusterbackup.ErrLiveBackup
	}
	if err := replicatedstate.ValidateSnapshotArtifactOptions(replicatedstate.SnapshotArtifactOptions{
		TargetChunkBytes: options.ChunkBytes, PayloadBuffer: make([]byte, 0, options.ChunkBytes)}); err != nil {
		return nil, errors.Join(clusterbackup.ErrLiveBackup, err)
	}
	return &Service{options: options, tokens: make(chan struct{}, options.MaxConcurrent)}, nil
}

func (service *Service) Metrics() Metrics {
	if service == nil {
		return Metrics{}
	}
	return Metrics{Requests: service.request.Load(), Faults: service.faults.Load(),
		LogicalArtifactBytes: service.logical.Load(), SnapshotScanBytes: service.scanned.Load()}
}

func (service *Service) Serve(ctx context.Context, connection rafttransport.PeerConnection) (resultErr error) {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return clusterbackup.ErrLiveBackup
	}
	service.request.Add(1)
	defer func() {
		if resultErr != nil {
			service.faults.Add(1)
		}
	}()
	defer connection.Close()
	if err := connection.SetReadDeadline(service.options.ReadDeadline()); err != nil {
		return err
	}
	var raw [clusterbackup.LiveRequestBytes]byte
	if _, err := io.ReadFull(connection, raw[:]); err != nil {
		return errors.Join(clusterbackup.ErrLiveBackup, err)
	}
	request, err := clusterbackup.OpenLiveRequest(raw[:])
	if err != nil || !service.options.Authorize(connection.PeerIdentity(), request) {
		return clusterbackup.ErrLiveBackup
	}
	select {
	case service.tokens <- struct{}{}:
		defer func() { <-service.tokens }()
	default:
		return clusterbackup.ErrBound
	}
	workspace, _ := service.pool.Get().([]byte)
	if cap(workspace) < service.options.ChunkBytes {
		workspace = make([]byte, 0, service.options.ChunkBytes)
	}
	workspace = workspace[:0]
	defer service.pool.Put(workspace)
	state, err := service.options.Owner.Probe(ctx, request.Group)
	if err != nil || state.Identity.MemberID != request.SourceMember {
		return errors.Join(clusterbackup.ErrLiveBackup, err)
	}
	cut, err := service.options.Owner.ReadLinearizableSnapshot(ctx, raftservice.LinearizableSnapshotRequest{
		Fence: state.Fence(), Capability: serviceauthz.CapabilityBackup})
	if err != nil {
		return errors.Join(clusterbackup.ErrLiveBackup, err)
	}
	defer cut.Close()
	groupCut, _, err := clusterbackup.ExportLinearizableGroupCut(io.Discard, cut.Snapshot(), request.Group,
		request.SourceMember, service.options.ChunkBytes, workspace)
	if err != nil {
		return errors.Join(clusterbackup.ErrLiveBackup, err)
	}
	response := clusterbackup.AppendLiveResponse(request.Operation, groupCut)
	writer := deadlineWriter{connection: connection, deadline: service.options.WriteDeadline}
	if _, err = writer.Write(response[:]); err != nil {
		return errors.Join(clusterbackup.ErrLiveBackup, err)
	}
	replayed, _, err := clusterbackup.ExportLinearizableGroupCut(&writer, cut.Snapshot(), request.Group,
		request.SourceMember, service.options.ChunkBytes, workspace)
	if err != nil || replayed != groupCut {
		return errors.Join(clusterbackup.ErrLiveBackup, err)
	}
	service.logical.Add(groupCut.ArtifactBytes)
	service.scanned.Add(groupCut.ArtifactBytes * 2)
	return nil
}

type deadlineWriter struct {
	connection rafttransport.PeerConnection
	deadline   rafttransport.DeadlineFunc
}

func (writer *deadlineWriter) Write(raw []byte) (int, error) {
	if err := writer.connection.SetWriteDeadline(writer.deadline()); err != nil {
		return 0, err
	}
	for offset := 0; offset < len(raw); {
		n, err := writer.connection.Write(raw[offset:])
		if n > 0 {
			offset += n
		}
		if err != nil {
			return offset, err
		}
		if n == 0 {
			return offset, io.ErrNoProgress
		}
	}
	return len(raw), nil
}
