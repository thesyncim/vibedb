package clusterbackup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

const (
	LiveRequestBytes  = 160
	LiveResponseBytes = 320
)

var (
	ErrLiveBackup = errors.New("clusterbackup: live backup exchange")
	liveRequest   = [8]byte{'V', 'B', 'B', 'A', 'C', 'K', 'U', 'P'}
	liveResponse  = [8]byte{'V', 'B', 'B', 'A', 'C', 'K', 'O', 'K'}
)

func LiveRequestDiscriminator() [8]byte { return liveRequest }

type LiveRequest struct {
	Operation    [sha256.Size]byte
	Group        raftmember.GroupKey
	SourceMember uint64
}

func (request LiveRequest) Valid() bool {
	return request.Operation != ([sha256.Size]byte{}) && validGroup(request.Group) && request.SourceMember != 0
}

type LiveOwner interface {
	Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error)
	ReadLinearizableSnapshot(context.Context, raftservice.LinearizableSnapshotRequest) (*raftservice.LinearizableSnapshotCut, error)
}

type LiveAuthorizeFunc func(rafttransport.PeerIdentity, LiveRequest) bool

type LiveServiceOptions struct {
	Owner                       LiveOwner
	Authorize                   LiveAuthorizeFunc
	ReadDeadline, WriteDeadline rafttransport.DeadlineFunc
	ChunkBytes                  int
	MaxConcurrent               int
}

type LiveService struct {
	options    LiveServiceOptions
	workspaces chan []byte
}

func NewLiveService(options LiveServiceOptions) (*LiveService, error) {
	if options.Owner == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent <= 0 || options.MaxConcurrent > 64 ||
		options.ChunkBytes <= 0 {
		return nil, ErrLiveBackup
	}
	workspace := make([]byte, 0, options.ChunkBytes)
	if err := replicatedstate.ValidateSnapshotArtifactOptions(replicatedstate.SnapshotArtifactOptions{
		TargetChunkBytes: options.ChunkBytes, PayloadBuffer: workspace}); err != nil {
		return nil, errors.Join(ErrLiveBackup, err)
	}
	service := &LiveService{options: options, workspaces: make(chan []byte, options.MaxConcurrent)}
	for range options.MaxConcurrent {
		service.workspaces <- make([]byte, 0, options.ChunkBytes)
	}
	return service, nil
}

func (service *LiveService) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrLiveBackup
	}
	defer connection.Close()
	if err := connection.SetReadDeadline(service.options.ReadDeadline()); err != nil {
		return err
	}
	var raw [LiveRequestBytes]byte
	if _, err := io.ReadFull(connection, raw[:]); err != nil {
		return errors.Join(ErrLiveBackup, err)
	}
	request, err := openLiveRequest(raw[:])
	if err != nil || !service.options.Authorize(connection.PeerIdentity(), request) {
		return ErrLiveBackup
	}
	select {
	case workspace := <-service.workspaces:
		defer func() { service.workspaces <- workspace }()
		state, err := service.options.Owner.Probe(ctx, request.Group)
		if err != nil || state.Identity.MemberID != request.SourceMember {
			return errors.Join(ErrLiveBackup, err)
		}
		cut, err := service.options.Owner.ReadLinearizableSnapshot(ctx, raftservice.LinearizableSnapshotRequest{
			Fence: state.Fence(), Capability: serviceauthz.CapabilityBackup})
		if err != nil {
			return errors.Join(ErrLiveBackup, err)
		}
		defer cut.Close()
		groupCut, _, err := ExportLinearizableGroupCut(io.Discard, cut.Snapshot(), request.Group,
			request.SourceMember, service.options.ChunkBytes, workspace)
		if err != nil {
			return errors.Join(ErrLiveBackup, err)
		}
		response := appendLiveResponse(request.Operation, groupCut)
		writer := liveDeadlineWriter{connection: connection, deadline: service.options.WriteDeadline}
		if _, err = writer.Write(response[:]); err != nil {
			return errors.Join(ErrLiveBackup, err)
		}
		replayed, _, err := ExportLinearizableGroupCut(&writer, cut.Snapshot(), request.Group,
			request.SourceMember, service.options.ChunkBytes, workspace)
		if err != nil || replayed != groupCut {
			return errors.Join(ErrLiveBackup, err)
		}
		return nil
	default:
		return ErrBound
	}
}

type liveDeadlineWriter struct {
	connection rafttransport.PeerConnection
	deadline   rafttransport.DeadlineFunc
}

func (writer *liveDeadlineWriter) Write(raw []byte) (int, error) {
	if err := writer.connection.SetWriteDeadline(writer.deadline()); err != nil {
		return 0, err
	}
	if err := writeFull(writer.connection, raw); err != nil {
		return 0, err
	}
	return len(raw), nil
}

func appendLiveRequest(request LiveRequest) (raw [LiveRequestBytes]byte) {
	copy(raw[:8], liveRequest[:])
	copy(raw[8:40], request.Operation[:])
	appendGroup(raw[40:40], request.Group)
	binary.BigEndian.PutUint64(raw[112:120], request.SourceMember)
	digest := sha256.Sum256(raw[:128])
	copy(raw[128:], digest[:])
	return raw
}

func openLiveRequest(raw []byte) (LiveRequest, error) {
	if len(raw) != LiveRequestBytes || [8]byte(raw[:8]) != liveRequest ||
		sha256.Sum256(raw[:128]) != [sha256.Size]byte(raw[128:]) {
		return LiveRequest{}, ErrLiveBackup
	}
	request := LiveRequest{Group: openLiveGroup(raw[40:112]), SourceMember: binary.BigEndian.Uint64(raw[112:120])}
	copy(request.Operation[:], raw[8:40])
	if !request.Valid() || binary.BigEndian.Uint64(raw[120:128]) != 0 {
		return LiveRequest{}, ErrLiveBackup
	}
	return request, nil
}

func openLiveGroup(raw []byte) (group raftmember.GroupKey) {
	copy(group.ClusterID[:], raw[:16])
	copy(group.ClusterIncarnation[:], raw[16:32])
	group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(raw[32:40])
	copy(group.ShardIncarnation[:], raw[40:56])
	copy(group.GroupID[:], raw[56:72])
	return group
}

func appendLiveResponse(operation [sha256.Size]byte, cut GroupCut) (raw [LiveResponseBytes]byte) {
	copy(raw[:8], liveResponse[:])
	copy(raw[8:40], operation[:])
	appendGroupCut(raw[40:288], cut)
	digest := sha256.Sum256(raw[:288])
	copy(raw[288:], digest[:])
	return raw
}

func openLiveResponse(raw []byte, operation [sha256.Size]byte) (GroupCut, error) {
	if len(raw) != LiveResponseBytes || [8]byte(raw[:8]) != liveResponse ||
		[sha256.Size]byte(raw[8:40]) != operation ||
		sha256.Sum256(raw[:288]) != [sha256.Size]byte(raw[288:]) {
		return GroupCut{}, ErrLiveBackup
	}
	cut := openGroupCut(raw[40:288])
	if !cut.Valid() {
		return GroupCut{}, ErrLiveBackup
	}
	return cut, nil
}

type LiveClient struct {
	Open func(context.Context) (rafttransport.PeerConnection, error)
}

func (client LiveClient) Export(ctx context.Context, request LiveRequest, destination io.Writer) (GroupCut, error) {
	if ctx == nil || !request.Valid() || destination == nil || client.Open == nil {
		return GroupCut{}, ErrLiveBackup
	}
	connection, err := client.Open(ctx)
	if err != nil || connection == nil {
		return GroupCut{}, errors.Join(ErrLiveBackup, err)
	}
	defer connection.Close()
	raw := appendLiveRequest(request)
	if err = writeFull(connection, raw[:]); err != nil {
		return GroupCut{}, errors.Join(ErrLiveBackup, err)
	}
	var response [LiveResponseBytes]byte
	if _, err = io.ReadFull(connection, response[:]); err != nil {
		return GroupCut{}, errors.Join(ErrLiveBackup, err)
	}
	cut, err := openLiveResponse(response[:], request.Operation)
	if err != nil || cut.Group != request.Group || cut.SourceMember != request.SourceMember {
		return GroupCut{}, ErrLiveBackup
	}
	digest := sha256.New()
	written, err := io.CopyN(io.MultiWriter(destination, digest), connection, int64(cut.ArtifactBytes))
	if err != nil || uint64(written) != cut.ArtifactBytes {
		return GroupCut{}, errors.Join(ErrLiveBackup, err)
	}
	var got [sha256.Size]byte
	copy(got[:], digest.Sum(nil))
	if got != cut.ArtifactHash {
		return GroupCut{}, ErrLiveBackup
	}
	var trailing [1]byte
	n, trailingErr := connection.Read(trailing[:])
	if n != 0 || !errors.Is(trailingErr, io.EOF) {
		return GroupCut{}, errors.Join(ErrLiveBackup, trailingErr)
	}
	return cut, nil
}

func writeFull(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		n, err := writer.Write(raw)
		if n > 0 {
			raw = raw[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
