// Package servicemetrics exposes one fixed-size authenticated RF3 progress
// snapshot over the shared shard-control listener.
package servicemetrics

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const (
	RequestBytes  = 80
	ResponseBytes = 176
)

var (
	ErrMetrics    = errors.New("servicemetrics: invalid metrics exchange")
	requestMagic  = [8]byte{'V', 'B', 'M', 'E', 'T', 'R', 'I', 'C'}
	responseMagic = [8]byte{'V', 'B', 'M', 'E', 'T', 'R', 'O', 'K'}
)

func RequestDiscriminator() [8]byte { return requestMagic }

type Provider interface {
	ProgressMetrics() raftservice.ProgressMetricsSnapshot
}

type GroupProvider interface {
	GroupProgressMetrics(raftmember.GroupKey) (raftmember.RuntimeIdentity, raftservice.ProgressMetricsSnapshot, bool)
}

// Snapshot is one authenticated fixed-width node or local-member cut. A zero
// Group is the process aggregate; a non-zero Group is exact and carries the
// serving member identity that produced its counters.
type Snapshot struct {
	Group   raftmember.GroupKey
	Member  uint64
	Metrics raftservice.ProgressMetricsSnapshot
}

type AuthorizeFunc func(rafttransport.PeerIdentity) bool

type ServiceOptions struct {
	Provider                    Provider
	Authorize                   AuthorizeFunc
	ReadDeadline, WriteDeadline rafttransport.DeadlineFunc
}

type Service struct{ options ServiceOptions }

func NewService(options ServiceOptions) (*Service, error) {
	if options.Provider == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil {
		return nil, ErrMetrics
	}
	return &Service{options: options}, nil
}

func (service *Service) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrMetrics
	}
	defer connection.Close()
	if !service.options.Authorize(connection.PeerIdentity()) {
		return ErrMetrics
	}
	if err := connection.SetReadDeadline(service.options.ReadDeadline()); err != nil {
		return err
	}
	var request [RequestBytes]byte
	if _, err := io.ReadFull(connection, request[:]); err != nil {
		return errors.Join(ErrMetrics, err)
	}
	if requestMagic != [8]byte(request[:8]) {
		return ErrMetrics
	}
	group := openGroup(request[8:])
	snapshot := Snapshot{Group: group}
	if group == (raftmember.GroupKey{}) {
		snapshot.Metrics = service.options.Provider.ProgressMetrics()
	} else {
		provider, ok := service.options.Provider.(GroupProvider)
		identity, metrics, found := provider.GroupProgressMetrics(group)
		if !ok || !found || identity.Group != group || identity.MemberID == 0 {
			return ErrMetrics
		}
		snapshot.Member, snapshot.Metrics = identity.MemberID, metrics
	}
	response := appendResponse(snapshot)
	if err := connection.SetWriteDeadline(service.options.WriteDeadline()); err != nil {
		return err
	}
	if err := writeAll(connection, response[:]); err != nil {
		return errors.Join(ErrMetrics, err)
	}
	return nil
}

func appendResponse(snapshot Snapshot) (response [ResponseBytes]byte) {
	copy(response[:8], responseMagic[:])
	appendGroup(response[8:80], snapshot.Group)
	binary.BigEndian.PutUint64(response[80:88], snapshot.Member)
	metrics := snapshot.Metrics
	values := [...]uint64{metrics.ProposalCommands, metrics.ProposalBytes, metrics.AppliedEntries,
		metrics.ReadyPersisted, metrics.SnapshotsFinished, metrics.ReadCompletions, metrics.Faults}
	for index, value := range values {
		binary.BigEndian.PutUint64(response[88+index*8:96+index*8], value)
	}
	digest := sha256.Sum256(response[:144])
	copy(response[144:], digest[:])
	return response
}

func OpenResponse(response []byte) (Snapshot, error) {
	if len(response) != ResponseBytes || responseMagic != [8]byte(response[:8]) ||
		sha256.Sum256(response[:144]) != [sha256.Size]byte(response[144:]) {
		return Snapshot{}, ErrMetrics
	}
	group := openGroup(response[8:80])
	member := binary.BigEndian.Uint64(response[80:88])
	if (group == (raftmember.GroupKey{})) != (member == 0) {
		return Snapshot{}, ErrMetrics
	}
	values := [7]uint64{}
	for index := range values {
		values[index] = binary.BigEndian.Uint64(response[88+index*8 : 96+index*8])
	}
	return Snapshot{Group: group, Member: member, Metrics: raftservice.ProgressMetricsSnapshot{ProposalCommands: values[0], ProposalBytes: values[1],
		AppliedEntries: values[2], ReadyPersisted: values[3], SnapshotsFinished: values[4],
		ReadCompletions: values[5], Faults: values[6]}}, nil
}

type Client struct {
	Open func(context.Context) (rafttransport.PeerConnection, error)
}

func (client Client) Read(ctx context.Context) (raftservice.ProgressMetricsSnapshot, error) {
	snapshot, err := client.ReadGroup(ctx, raftmember.GroupKey{})
	return snapshot.Metrics, err
}

func (client Client) ReadGroup(ctx context.Context, group raftmember.GroupKey) (Snapshot, error) {
	if ctx == nil || client.Open == nil {
		return Snapshot{}, ErrMetrics
	}
	connection, err := client.Open(ctx)
	if err != nil || connection == nil {
		return Snapshot{}, errors.Join(ErrMetrics, err)
	}
	defer connection.Close()
	var request [RequestBytes]byte
	copy(request[:8], requestMagic[:])
	appendGroup(request[8:], group)
	if err = writeAll(connection, request[:]); err != nil {
		return Snapshot{}, errors.Join(ErrMetrics, err)
	}
	var response [ResponseBytes]byte
	if _, err = io.ReadFull(connection, response[:]); err != nil {
		return Snapshot{}, errors.Join(ErrMetrics, err)
	}
	snapshot, err := OpenResponse(response[:])
	if err != nil || snapshot.Group != group {
		return Snapshot{}, errors.Join(ErrMetrics, err)
	}
	return snapshot, nil
}

func appendGroup(dst []byte, group raftmember.GroupKey) {
	copy(dst[0:16], group.ClusterID[:])
	copy(dst[16:32], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(dst[32:40], group.TopologyRecoveryEpoch)
	copy(dst[40:56], group.ShardIncarnation[:])
	copy(dst[56:72], group.GroupID[:])
}

func openGroup(raw []byte) (group raftmember.GroupKey) {
	copy(group.ClusterID[:], raw[0:16])
	copy(group.ClusterIncarnation[:], raw[16:32])
	group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(raw[32:40])
	copy(group.ShardIncarnation[:], raw[40:56])
	copy(group.GroupID[:], raw[56:72])
	return group
}

func writeAll(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		written, err := writer.Write(raw)
		if written > 0 {
			raw = raw[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
