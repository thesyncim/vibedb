package rafttransport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	peerFailurePhaseDirectory = "directory"
	peerFailurePhaseDial      = "dial"
	peerFailurePhaseWrite     = "write"
	peerFailurePhaseRead      = "read"
)

// PeerFailure is the last bounded asynchronous failure observed by one
// ordinary peer worker. It contains only routing metadata from the encoded
// frame and a stable error class; payloads, addresses, credentials, and
// authority material are never retained. The zero value means no failure has
// been observed since the transport was created.
type PeerFailure struct {
	Node        NodeID
	Phase       string
	Cause       string
	Group       raftmember.GroupKey
	From        uint64
	To          uint64
	Version     uint64
	Kind        string
	MessageType int32
	Index       uint64
	Term        uint64
}

func classifyPeerFailure(err error, fallback string) string {
	switch {
	case errors.Is(err, ErrPeerUnauthorized):
		return "unauthorized"
	case errors.Is(err, ErrPeerKeyMismatch):
		return "key-mismatch"
	case errors.Is(err, ErrPeerRetired):
		return "retired"
	case errors.Is(err, ErrWrongTrafficClass):
		return "wrong-traffic-class"
	case errors.Is(err, ErrTransportClosed):
		return "transport-closed"
	case errors.Is(err, ErrBackpressure):
		return "backpressure"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected-eof"
	case errors.Is(err, io.ErrClosedPipe):
		return "closed-pipe"
	case errors.Is(err, io.EOF):
		return "eof"
	default:
		return fallback
	}
}

func peerFailureFromFrame(node NodeID, phase string, frame []byte, err error) PeerFailure {
	failure := PeerFailure{Node: node, Phase: phase, Cause: classifyPeerFailure(err, phase+"-error")}
	if len(frame) >= StreamRecordHeaderBytes {
		frameSize := int(binary.BigEndian.Uint32(frame[:StreamRecordHeaderBytes]))
		if frameSize >= FrameHeaderBytes && frameSize <= len(frame)-StreamRecordHeaderBytes {
			frame = frame[StreamRecordHeaderBytes : StreamRecordHeaderBytes+frameSize]
		}
	}
	header, _, parseErr := parseFrame(frame)
	if parseErr != nil {
		return failure
	}
	failure.Group = header.group
	failure.From, failure.To = header.from, header.to
	failure.Version = header.version
	failure.MessageType, failure.Index, failure.Term = outboundFrameMetadata(header, frame)
	if header.kind == frameKindAuthority {
		failure.Kind = "authority"
	} else {
		failure.Kind = "ordinary"
	}
	return failure
}

// recordPeerFailure publishes one detached failure snapshot under the same
// mutex used by queue ownership. A nil frame selects the current queue head
// (or in-flight frame), which keeps directory and dial failures correlated
// without retaining another copy of the payload.
func (transport *OrdinaryTransport) recordPeerFailure(
	peer *ordinaryPeer, phase string, frame []byte, err error,
) {
	if transport == nil || peer == nil {
		return
	}
	transport.mu.Lock()
	if len(frame) == 0 {
		switch {
		case peer.directFrame != nil:
			frame = peer.directFrame.record
		case len(peer.batchFrames) != 0 && peer.batchFrames[0] != nil:
			frame = peer.batchFrames[0].record
			if len(frame) == 0 {
				frame = peer.batchFrames[0].bytes
			}
		case peer.count != 0:
			frame = peer.queue[peer.head].buffer.bytes
		}
	}
	peer.lastFailure = peerFailureFromFrame(peer.node, phase, frame, err)
	transport.mu.Unlock()
}

// outboundFrameMetadata extracts protobuf metadata only after the frame
// header has passed parseFrame. Authority frames intentionally have no Raft
// message index/term and retain zero values.
func outboundFrameMetadata(header frameHeader, frame []byte) (int32, uint64, uint64) {
	if header.kind != frameKindOrdinary || len(frame) < FrameHeaderBytes {
		return 0, 0, 0
	}
	_, payload, err := parseFrame(frame)
	if err != nil {
		return 0, 0, 0
	}
	message := new(pb.Message)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false, RecursionLimit: 8}).Unmarshal(payload, message); err != nil {
		return 0, 0, 0
	}
	return int32(message.GetType()), message.GetIndex(), message.GetTerm()
}

// outboundFailure adds only the routing metadata needed to correlate a
// synchronous send/queue refusal with the owning Raft group. The message
// payload, authority grant, and service-key material are deliberately absent.
// Backpressure and transport shutdown remain their exact sentinel values:
// owner loops use those identities to preserve their bounded retry behavior.
func outboundFailure(outbound raftmember.OutboundMessage, err error) error {
	if err == nil || errors.Is(err, ErrBackpressure) || errors.Is(err, ErrTransportClosed) {
		return err
	}
	kind := "ordinary"
	messageType := int32(0)
	var index, term uint64
	if outbound.Message != nil {
		messageType = int32(outbound.Message.GetType())
		index, term = outbound.Message.GetIndex(), outbound.Message.GetTerm()
	} else if outbound.Authority != nil {
		kind = "authority"
	}
	return fmt.Errorf("rafttransport: outbound send failed group=%x from=%d to=%d kind=%s message_type=%d index=%d term=%d: %w",
		outbound.Group.GroupID, outbound.From, outbound.To, kind, messageType, index, term, err)
}
