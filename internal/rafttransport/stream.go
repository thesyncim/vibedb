package rafttransport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

var (
	ErrInvalidTransport = errors.New("rafttransport: invalid peer transport configuration")
	ErrBackpressure     = errors.New("rafttransport: ordinary peer queue is full")
	ErrTransportClosed  = errors.New("rafttransport: peer transport is closed")
)

const (
	// StreamRecordHeaderBytes is the fixed unsigned frame-length prefix on one
	// authenticated peer stream.
	StreamRecordHeaderBytes = 4
	// DefaultRetainedFrameBytes keeps ordinary control frames warm without
	// retaining a one-off maximum proposal in every peer or receiver.
	DefaultRetainedFrameBytes = 64 << 10
)

// DelayWaitFunc is the only delay capability used by reconnect and coalescing.
// wake is nil for reconnect delay. A coalescing wait may return early when wake
// becomes readable. Implementations must return when ctx is canceled.
type DelayWaitFunc func(ctx context.Context, delay time.Duration, wake <-chan struct{}) error

// WaitWithTimer is the ordinary production timer implementation. Transport
// core receives it explicitly and never reads wall-clock time by itself.
func WaitWithTimer(ctx context.Context, delay time.Duration, wake <-chan struct{}) error {
	if ctx == nil {
		return ErrInvalidTransport
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	case <-wake:
		return nil
	}
}

// RawPeerDialFunc opens one owned raw stream for node. It must honor ctx. The
// TLS dialer closes every returned connection that fails authentication.
type RawPeerDialFunc func(ctx context.Context, node NodeID) (net.Conn, error)

// OrdinaryDialer opens only the ordinary Raft traffic class.
type OrdinaryDialer interface {
	DialOrdinary(ctx context.Context, node NodeID) (PeerConnection, error)
}

// SnapshotStreamOpener is the separate capability reserved for snapshot data.
// OrdinaryTransport does not accept or call this interface. Deployments can
// give it a separate connector, listener, queue budget, and concurrency limit.
type SnapshotStreamOpener interface {
	OpenSnapshot(ctx context.Context, node NodeID) (PeerConnection, error)
}

// TLSOrdinaryDialer authenticates a raw connector as ordinary Raft traffic.
type TLSOrdinaryDialer struct {
	TLS               *PeerTLS
	Dial              RawPeerDialFunc
	HandshakeDeadline DeadlineFunc
}

func (dialer TLSOrdinaryDialer) DialOrdinary(
	ctx context.Context,
	node NodeID,
) (PeerConnection, error) {
	if ctx == nil || dialer.TLS == nil || dialer.Dial == nil || node == (NodeID{}) {
		return nil, ErrInvalidTransport
	}
	raw, err := dialer.Dial(ctx, node)
	if err != nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, err
	}
	if raw == nil {
		return nil, ErrInvalidTransport
	}
	return dialer.TLS.Client(
		ctx, raw, node, TrafficOrdinary, dialer.HandshakeDeadline,
	)
}

// TLSSnapshotStreamOpener authenticates a distinct raw connector as snapshot
// traffic. It is intentionally not accepted by OrdinaryTransport.
type TLSSnapshotStreamOpener struct {
	TLS               *PeerTLS
	Open              RawPeerDialFunc
	HandshakeDeadline DeadlineFunc
}

func (opener TLSSnapshotStreamOpener) OpenSnapshot(
	ctx context.Context,
	node NodeID,
) (PeerConnection, error) {
	if ctx == nil || opener.TLS == nil || opener.Open == nil || node == (NodeID{}) {
		return nil, ErrInvalidTransport
	}
	raw, err := opener.Open(ctx, node)
	if err != nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, err
	}
	if raw == nil {
		return nil, ErrInvalidTransport
	}
	return opener.TLS.Client(
		ctx, raw, node, TrafficSnapshot, opener.HandshakeDeadline,
	)
}

// InboundHandler receives one owned, fully authenticated, and statically
// admitted ordinary frame. The handler may retain Inbound and its Message.
// The transport never reuses their storage. A nonnil error stops the stream.
type InboundHandler func(ctx context.Context, inbound Inbound) error

// OrdinaryReceiverOptions bounds one long-lived ordinary peer reader.
type OrdinaryReceiverOptions struct {
	Registry           *StaticRegistry
	ReadDeadline       DeadlineFunc
	Handle             InboundHandler
	RetainedFrameBytes int
}

// OrdinaryReceiver reads current stream records and invokes StaticRegistry
// admission before delivery.
type OrdinaryReceiver struct {
	registry     *StaticRegistry
	readDeadline DeadlineFunc
	handle       InboundHandler
	frames       frameBufferPool
}

// NewOrdinaryReceiver validates and detaches a receiver profile.
func NewOrdinaryReceiver(options OrdinaryReceiverOptions) (*OrdinaryReceiver, error) {
	retain := options.RetainedFrameBytes
	if retain == 0 {
		retain = DefaultRetainedFrameBytes
	}
	if options.Registry == nil || options.ReadDeadline == nil || options.Handle == nil ||
		retain < FrameHeaderBytes || retain > MaxFrameBytes {
		return nil, ErrInvalidTransport
	}
	return &OrdinaryReceiver{
		registry: options.Registry, readDeadline: options.ReadDeadline,
		handle: options.Handle, frames: frameBufferPool{retain: retain},
	}, nil
}

// ServeTLS authenticates raw as ordinary traffic and serves it until EOF,
// cancellation, or the first invalid frame. It takes ownership of raw.
func (receiver *OrdinaryReceiver) ServeTLS(
	ctx context.Context,
	raw net.Conn,
	peerTLS *PeerTLS,
	handshakeDeadline DeadlineFunc,
) error {
	if receiver == nil || ctx == nil || raw == nil || peerTLS == nil {
		if raw != nil {
			_ = raw.Close()
		}
		return ErrInvalidTransport
	}
	connection, err := peerTLS.Server(
		ctx, raw, TrafficOrdinary, handshakeDeadline,
	)
	if err != nil {
		return err
	}
	return receiver.Serve(ctx, connection)
}

// Serve reads one owned, already authenticated ordinary connection. A clean
// record-boundary EOF returns nil. Every partial record fails closed.
func (receiver *OrdinaryReceiver) Serve(
	ctx context.Context,
	connection PeerConnection,
) error {
	if receiver == nil || ctx == nil || connection == nil ||
		connection.PeerNode() == (NodeID{}) ||
		connection.TrafficClass() != TrafficOrdinary {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrInvalidTransport
	}
	defer connection.Close()
	stopCancellation := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stopCancellation()

	var header [StreamRecordHeaderBytes]byte
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		deadline := receiver.readDeadline()
		if deadline.IsZero() {
			return ErrInvalidTransport
		}
		if err := connection.SetReadDeadline(deadline); err != nil {
			return err
		}
		read, err := io.ReadFull(connection, header[:])
		if err != nil {
			if contextErr := context.Cause(ctx); contextErr != nil {
				return contextErr
			}
			if errors.Is(err, io.EOF) && read == 0 {
				return nil
			}
			return fmt.Errorf("%w: stream record header: %w", ErrInvalidFrame, err)
		}
		frameBytes := uint64(binary.BigEndian.Uint32(header[:]))
		if frameBytes < FrameHeaderBytes || frameBytes > MaxFrameBytes {
			return ErrFrameTooLarge
		}
		ownedFrame := receiver.frames.get(int(frameBytes))
		frame := ownedFrame.bytes
		_, err = io.ReadFull(connection, frame)
		if err != nil {
			receiver.frames.put(ownedFrame)
			if contextErr := context.Cause(ctx); contextErr != nil {
				return contextErr
			}
			return fmt.Errorf("%w: stream record body: %w", ErrInvalidFrame, err)
		}
		inbound, decodeErr := receiver.registry.DecodeInbound(
			connection.PeerNode(), frame,
		)
		receiver.frames.put(ownedFrame)
		if decodeErr != nil {
			return decodeErr
		}
		if err := receiver.handle(ctx, inbound); err != nil {
			return err
		}
	}
}

type frameBufferPool struct {
	retain int
	pool   sync.Pool
}

type pooledFrameBuffer struct {
	bytes []byte
}

func (pool *frameBufferPool) get(size int) *pooledFrameBuffer {
	if size <= 0 {
		return nil
	}
	var buffer *pooledFrameBuffer
	if pooled := pool.pool.Get(); pooled != nil {
		buffer = pooled.(*pooledFrameBuffer)
	} else {
		buffer = new(pooledFrameBuffer)
	}
	if cap(buffer.bytes) < size {
		buffer.bytes = make([]byte, size)
	} else {
		buffer.bytes = buffer.bytes[:size]
	}
	return buffer
}

func (pool *frameBufferPool) put(buffer *pooledFrameBuffer) {
	if pool == nil || buffer == nil {
		return
	}
	if cap(buffer.bytes) > pool.retain {
		buffer.bytes = nil
	} else {
		clear(buffer.bytes)
		buffer.bytes = buffer.bytes[:0]
	}
	pool.pool.Put(buffer)
}

func appendStreamRecord(dst, frame []byte) ([]byte, error) {
	if len(frame) < FrameHeaderBytes || len(frame) > MaxFrameBytes {
		return dst, ErrFrameTooLarge
	}
	if len(dst) > int(^uint(0)>>1)-StreamRecordHeaderBytes-len(frame) {
		return dst, ErrFrameTooLarge
	}
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(dst[start:start+StreamRecordHeaderBytes], uint32(len(frame)))
	dst = append(dst, frame...)
	return dst, nil
}

func writeFull(writer io.Writer, buffer []byte) error {
	for len(buffer) != 0 {
		written, err := writer.Write(buffer)
		if written < 0 || written > len(buffer) {
			return io.ErrShortWrite
		}
		buffer = buffer[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
