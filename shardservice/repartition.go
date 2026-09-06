package shardservice

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/thesyncim/vibedb/internal/exchange"
	queryengine "github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibejson/x/byteview"
)

var repartitionNullJSON = [...]byte{'n', 'u', 'l', 'l'}

type repartitionPartitioner struct {
	columns    []uint16
	partitions uint32
	key        []byte
	encoder    queryengine.JSONGroupKeyEncoder
}

func (p *repartitionPartitioner) Partition(row []exchange.Cell) (uint32, error) {
	p.key = p.key[:0]
	for _, ordinal := range p.columns {
		if int(ordinal) >= len(row) {
			return 0, errBadRepartition
		}
		value := row[ordinal].Bytes
		if row[ordinal].Null {
			value = repartitionNullJSON[:]
		}
		var ok bool
		p.key, ok = p.encoder.Append(p.key, value)
		if !ok {
			return 0, errBadRepartition
		}
	}
	return exchange.PartitionForKey(p.key, p.partitions)
}

type repartitionPeerSink struct {
	server  *Server
	request RepartitionRequest
	conns   []net.Conn
	// encs owns one frame arena per peer slot, parallel to conns. Arena
	// bytes never cross slots, and a redialed slot simply keeps growing
	// the arena it already owns.
	encs []FrameEncoder
}

func newRepartitionPeerSink(server *Server, request RepartitionRequest) (*repartitionPeerSink, error) {
	if server == nil || !request.canonical() {
		return nil, errBadRepartition
	}
	if server.opts.ExchangeDial == nil {
		for i := range request.Targets {
			if !loopbackExchangeAddress(request.Targets[i].Address) {
				return nil, fmt.Errorf("%w: target %d is not a loopback TCP address", errBadRepartition, i)
			}
		}
	}
	return &repartitionPeerSink{
		server: server, request: request, conns: make([]net.Conn, len(request.Targets)),
		encs: make([]FrameEncoder, len(request.Targets)),
	}, nil
}

func loopbackExchangeAddress(address []byte) bool {
	host, _, err := net.SplitHostPort(byteview.String(address))
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *repartitionPeerSink) Push(ctx context.Context, partition uint32, batch exchange.Batch) error {
	if s == nil || int(partition) >= len(s.request.Targets) {
		return exchange.ErrPartitions
	}
	conn := s.conns[partition]
	if conn == nil {
		var err error
		if s.server.opts.ExchangeDial != nil {
			conn, err = s.server.opts.ExchangeDial(ctx, s.request.Targets[partition].Address)
		} else {
			var dialer net.Dialer
			conn, err = dialer.DialContext(ctx, "tcp", byteview.String(s.request.Targets[partition].Address))
		}
		if err != nil {
			return err
		}
		s.conns[partition] = conn
	}
	target := s.request.Targets[partition]
	deadline := time.Duration(0)
	if end, ok := ctx.Deadline(); ok {
		deadline = time.Until(end)
		if deadline <= 0 {
			return context.DeadlineExceeded
		}
	}
	req := &ShardRequest{
		Distribution: target.Distribution, Shard: target.Shard,
		AllocationGeneration: target.AllocationGeneration,
		RoutingVersion:       target.RoutingVersion, OwnershipEpoch: target.OwnershipEpoch,
		ExecutionMode: ExecutionReadWrite, Deadline: deadline,
		Exchange: ExchangeRequest{
			Operation: ExchangePush,
			Key: exchange.Key{
				Operation: s.request.Operation, Stage: s.request.Stage,
				Partition: partition, Attempt: s.request.Attempt,
			},
			Batch: batch,
		},
	}
	if err := roundTripExchangePeer(ctx, conn, &s.encs[partition], req); err != nil {
		_ = conn.Close()
		s.conns[partition] = nil
		return err
	}
	return nil
}

func roundTripExchangePeer(ctx context.Context, conn net.Conn, enc *FrameEncoder, req *ShardRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if end, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(end)
	} else {
		_ = conn.SetDeadline(time.Time{})
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
		close(callbackDone)
	})
	defer func() {
		if !stop() {
			<-callbackDone
		}
	}()
	if err := enc.EncodeRequest(conn, req); err != nil {
		return firstContextError(ctx, err)
	}
	resp, err := DecodeResponse(conn)
	if err != nil {
		return firstContextError(ctx, err)
	}
	if resp.Kind == ResponseError {
		return &peerExchangeError{kind: resp.ErrorKind, message: resp.ErrorMessage}
	}
	if resp.Kind != ResponseCompletion || resp.Exchange.Operation != ExchangePush {
		return errBadExchange
	}
	return nil
}

func firstContextError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

type peerExchangeError struct {
	kind    ErrorKind
	message string
}

func (e *peerExchangeError) Error() string {
	return fmt.Sprintf("shardservice: exchange peer %s: %s", e.kind, e.message)
}

func (s *repartitionPeerSink) Close() error {
	if s == nil {
		return nil
	}
	var err error
	for i, conn := range s.conns {
		if conn != nil {
			err = errors.Join(err, conn.Close())
			s.conns[i] = nil
		}
	}
	return err
}
