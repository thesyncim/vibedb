// Package servicemetrics exposes one fixed-size authenticated RF3 progress
// snapshot over the shared shard-control listener.
package servicemetrics

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const (
	RequestBytes  = 16
	ResponseBytes = 96
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
	if requestMagic != [8]byte(request[:8]) || binary.BigEndian.Uint64(request[8:]) != 0 {
		return ErrMetrics
	}
	response := appendResponse(service.options.Provider.ProgressMetrics())
	if err := connection.SetWriteDeadline(service.options.WriteDeadline()); err != nil {
		return err
	}
	if err := writeAll(connection, response[:]); err != nil {
		return errors.Join(ErrMetrics, err)
	}
	return nil
}

func appendResponse(metrics raftservice.ProgressMetricsSnapshot) (response [ResponseBytes]byte) {
	copy(response[:8], responseMagic[:])
	values := [...]uint64{metrics.ProposalCommands, metrics.ProposalBytes, metrics.AppliedEntries,
		metrics.ReadyPersisted, metrics.SnapshotsFinished, metrics.ReadCompletions, metrics.Faults}
	for index, value := range values {
		binary.BigEndian.PutUint64(response[8+index*8:16+index*8], value)
	}
	digest := sha256.Sum256(response[:64])
	copy(response[64:], digest[:])
	return response
}

func OpenResponse(response []byte) (raftservice.ProgressMetricsSnapshot, error) {
	if len(response) != ResponseBytes || responseMagic != [8]byte(response[:8]) ||
		sha256.Sum256(response[:64]) != [sha256.Size]byte(response[64:]) {
		return raftservice.ProgressMetricsSnapshot{}, ErrMetrics
	}
	values := [7]uint64{}
	for index := range values {
		values[index] = binary.BigEndian.Uint64(response[8+index*8 : 16+index*8])
	}
	return raftservice.ProgressMetricsSnapshot{ProposalCommands: values[0], ProposalBytes: values[1],
		AppliedEntries: values[2], ReadyPersisted: values[3], SnapshotsFinished: values[4],
		ReadCompletions: values[5], Faults: values[6]}, nil
}

type Client struct {
	Open func(context.Context) (rafttransport.PeerConnection, error)
}

func (client Client) Read(ctx context.Context) (raftservice.ProgressMetricsSnapshot, error) {
	if ctx == nil || client.Open == nil {
		return raftservice.ProgressMetricsSnapshot{}, ErrMetrics
	}
	connection, err := client.Open(ctx)
	if err != nil || connection == nil {
		return raftservice.ProgressMetricsSnapshot{}, errors.Join(ErrMetrics, err)
	}
	defer connection.Close()
	var request [RequestBytes]byte
	copy(request[:8], requestMagic[:])
	if err = writeAll(connection, request[:]); err != nil {
		return raftservice.ProgressMetricsSnapshot{}, errors.Join(ErrMetrics, err)
	}
	var response [ResponseBytes]byte
	if _, err = io.ReadFull(connection, response[:]); err != nil {
		return raftservice.ProgressMetricsSnapshot{}, errors.Join(ErrMetrics, err)
	}
	return OpenResponse(response[:])
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
