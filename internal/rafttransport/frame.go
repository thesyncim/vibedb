package rafttransport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrUnauthorized reports a frame whose authenticated node is not the
	// statically registered source, or whose destination is not local.
	ErrUnauthorized = errors.New("rafttransport: unauthorized Raft message")
	// ErrInvalidFrame reports malformed or noncanonical frame bytes.
	ErrInvalidFrame = errors.New("rafttransport: invalid frame")
	// ErrUnsupportedFrame reports a well-formed frame feature this static,
	// ordinary-message transport deliberately does not implement.
	ErrUnsupportedFrame = errors.New("rafttransport: unsupported frame")
	// ErrFrameTooLarge reports a frame or payload above the frozen memory bound.
	ErrFrameTooLarge = errors.New("rafttransport: frame exceeds bound")
)

const (
	// FrameHeaderBytes is the fixed current frame-header size. The numeric
	// codec discriminator is a fail-closed sentinel, not a compatibility API.
	FrameHeaderBytes = 132
	// MaxFrameBytes lets a future authenticated stream reader reject a declared
	// length before allocating a frame buffer.
	MaxFrameBytes = FrameHeaderBytes + raftmodel.MaxInboundMessageBytes

	frameCodecFormat  uint16 = 1
	frameKindOrdinary byte   = 1
)

var frameMagic = [4]byte{'V', 'D', 'R', 'F'}

// Inbound owns Message and detaches it from the caller's frame buffer.
type Inbound struct {
	Group   raftmember.GroupKey
	Message *pb.Message
}

type frameHeader struct {
	group       raftmember.GroupKey
	roster      [32]byte
	from        uint64
	to          uint64
	payloadSize uint32
}

// EncodeOutbound appends a canonical current-format frame to dst and returns
// the registered destination node. dst remains unchanged on error.
//
// The frame carries no node identity or authentication material. A later
// mutually authenticated connection supplies that identity out of band.
func (registry *StaticRegistry) EncodeOutbound(
	dst []byte,
	outbound raftmember.OutboundMessage,
) ([]byte, NodeID, error) {
	if registry == nil || outbound.Message == nil {
		return dst, NodeID{}, fmt.Errorf("%w: nil registry or message", ErrInvalidFrame)
	}
	if err := validateFrameGroup(outbound.Group); err != nil {
		return dst, NodeID{}, err
	}
	if outbound.From == 0 || outbound.To == 0 || outbound.From == outbound.To ||
		outbound.Message.GetFrom() != outbound.From || outbound.Message.GetTo() != outbound.To {
		return dst, NodeID{}, fmt.Errorf("%w: outer and protobuf member IDs differ", ErrInvalidFrame)
	}
	fromNode, err := registry.Node(outbound.Group, outbound.From)
	if err != nil || fromNode != registry.LocalNode() {
		return dst, NodeID{}, fmt.Errorf("%w: source member is not local", ErrUnauthorized)
	}
	destination, err := registry.Node(outbound.Group, outbound.To)
	if err != nil || destination == registry.LocalNode() {
		return dst, NodeID{}, fmt.Errorf("%w: destination member is not remote", ErrUnauthorized)
	}
	size, err := raftmember.MeasureOrdinaryMessage(outbound.Message)
	if err != nil {
		return dst, NodeID{}, classifyOrdinaryError(err)
	}
	if err := registry.validateStaticMessage(outbound.Group, outbound.Message); err != nil {
		return dst, NodeID{}, err
	}
	roster, ok := registry.rosterDigest(outbound.Group)
	if !ok {
		return dst, NodeID{}, fmt.Errorf("%w: missing group roster", ErrUnauthorized)
	}
	if size > raftmodel.MaxInboundMessageBytes {
		return dst, NodeID{}, fmt.Errorf("%w: payload bytes %d", ErrFrameTooLarge, size)
	}
	if len(dst) > math.MaxInt-FrameHeaderBytes-size {
		return dst, NodeID{}, ErrFrameTooLarge
	}
	if messageOverlapsAppendRegion(dst, FrameHeaderBytes+size, outbound.Message) {
		return dst, NodeID{}, fmt.Errorf("%w: message aliases frame append region", ErrInvalidFrame)
	}

	start := len(dst)
	// Grow the complete frame before any write. Besides keeping encode
	// allocation-free when the caller sized dst exactly, this prevents a
	// header-only reuse of dst's old backing array from overwriting message
	// slices before MarshalAppend relocates the payload.
	dst = slices.Grow(dst, FrameHeaderBytes+size)
	dst = append(dst, make([]byte, FrameHeaderBytes)...)
	dst, err = (proto.MarshalOptions{Deterministic: true}).MarshalAppend(dst, outbound.Message)
	if err != nil {
		return dst[:start], NodeID{}, fmt.Errorf("%w: marshal ordinary message: %w", ErrInvalidFrame, err)
	}
	if len(dst) != start+FrameHeaderBytes+size {
		return dst[:start], NodeID{}, fmt.Errorf("%w: protobuf size changed during encode", ErrInvalidFrame)
	}
	header := dst[start : start+FrameHeaderBytes]
	copy(header[0:4], frameMagic[:])
	binary.BigEndian.PutUint16(header[4:6], frameCodecFormat)
	header[6] = frameKindOrdinary
	header[7] = 0
	appendGroupKey(header[8:80], outbound.Group)
	copy(header[80:112], roster[:])
	binary.BigEndian.PutUint64(header[112:120], outbound.From)
	binary.BigEndian.PutUint64(header[120:128], outbound.To)
	binary.BigEndian.PutUint32(header[128:132], uint32(size))
	return dst, destination, nil
}

// DecodeInbound authenticates and decodes one complete canonical frame. The
// authenticated node is supplied by a future secure connection; this method
// neither performs TLS nor derives identity from frame bytes.
func (registry *StaticRegistry) DecodeInbound(authenticated NodeID, frame []byte) (Inbound, error) {
	header, payload, err := parseFrame(frame)
	if err != nil {
		return Inbound{}, err
	}
	if registry == nil || authenticated == (NodeID{}) {
		return Inbound{}, fmt.Errorf("%w: missing registry or peer identity", ErrUnauthorized)
	}
	source, err := registry.Node(header.group, header.from)
	if err != nil || source != authenticated {
		return Inbound{}, fmt.Errorf("%w: authenticated source is not registered member", ErrUnauthorized)
	}
	destination, err := registry.Node(header.group, header.to)
	if err != nil || destination != registry.LocalNode() {
		return Inbound{}, fmt.Errorf("%w: target member is not local", ErrUnauthorized)
	}
	roster, ok := registry.rosterDigest(header.group)
	if !ok || roster != header.roster {
		return Inbound{}, fmt.Errorf("%w: static roster digest differs", ErrUnauthorized)
	}
	if err := preflightOrdinaryPayload(payload); err != nil {
		return Inbound{}, err
	}

	message := new(pb.Message)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false, RecursionLimit: 8}).Unmarshal(payload, message); err != nil {
		return Inbound{}, fmt.Errorf("%w: decode protobuf: %w", ErrInvalidFrame, err)
	}
	if message.GetFrom() != header.from || message.GetTo() != header.to {
		return Inbound{}, fmt.Errorf("%w: outer and protobuf member IDs differ", ErrInvalidFrame)
	}
	if _, err := raftmember.MeasureOrdinaryMessage(message); err != nil {
		return Inbound{}, classifyOrdinaryError(err)
	}
	if err := registry.validateStaticMessage(header.group, message); err != nil {
		return Inbound{}, err
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return Inbound{}, fmt.Errorf("%w: re-encode protobuf: %w", ErrInvalidFrame, err)
	}
	if !bytes.Equal(payload, canonical) {
		return Inbound{}, fmt.Errorf("%w: noncanonical protobuf payload", ErrInvalidFrame)
	}
	return Inbound{Group: header.group, Message: message}, nil
}

func parseFrame(frame []byte) (frameHeader, []byte, error) {
	if len(frame) < FrameHeaderBytes {
		return frameHeader{}, nil, fmt.Errorf("%w: truncated header", ErrInvalidFrame)
	}
	if len(frame) > MaxFrameBytes {
		return frameHeader{}, nil, ErrFrameTooLarge
	}
	if !bytes.Equal(frame[0:4], frameMagic[:]) ||
		binary.BigEndian.Uint16(frame[4:6]) != frameCodecFormat ||
		frame[6] != frameKindOrdinary || frame[7] != 0 {
		return frameHeader{}, nil, fmt.Errorf("%w: magic, format, kind, or flags", ErrUnsupportedFrame)
	}
	header := frameHeader{
		group:       openGroupKey(frame[8:80]),
		from:        binary.BigEndian.Uint64(frame[112:120]),
		to:          binary.BigEndian.Uint64(frame[120:128]),
		payloadSize: binary.BigEndian.Uint32(frame[128:132]),
	}
	copy(header.roster[:], frame[80:112])
	if err := validateFrameGroup(header.group); err != nil {
		return frameHeader{}, nil, err
	}
	if header.from == 0 || header.to == 0 || header.from == header.to {
		return frameHeader{}, nil, fmt.Errorf("%w: invalid member IDs", ErrInvalidFrame)
	}
	if uint64(header.payloadSize) > uint64(raftmodel.MaxInboundMessageBytes) {
		return frameHeader{}, nil, ErrFrameTooLarge
	}
	if len(frame)-FrameHeaderBytes != int(header.payloadSize) {
		return frameHeader{}, nil, fmt.Errorf("%w: payload length mismatch", ErrInvalidFrame)
	}
	return header, frame[FrameHeaderBytes:], nil
}

func appendGroupKey(dst []byte, group raftmember.GroupKey) {
	copy(dst[0:16], group.ClusterID[:])
	copy(dst[16:32], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(dst[32:40], group.TopologyRecoveryEpoch)
	copy(dst[40:56], group.ShardIncarnation[:])
	copy(dst[56:72], group.GroupID[:])
}

func openGroupKey(src []byte) raftmember.GroupKey {
	var group raftmember.GroupKey
	copy(group.ClusterID[:], src[0:16])
	copy(group.ClusterIncarnation[:], src[16:32])
	group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(src[32:40])
	copy(group.ShardIncarnation[:], src[40:56])
	copy(group.GroupID[:], src[56:72])
	return group
}

func validateFrameGroup(group raftmember.GroupKey) error {
	if group.ClusterID == ([16]byte{}) ||
		group.ClusterIncarnation == ([16]byte{}) ||
		group.TopologyRecoveryEpoch == 0 ||
		group.ShardIncarnation == ([16]byte{}) ||
		group.GroupID == ([16]byte{}) {
		return fmt.Errorf("%w: incomplete group lineage", ErrInvalidFrame)
	}
	return nil
}

func classifyOrdinaryError(err error) error {
	if errors.Is(err, raftmodel.ErrAdmissionBound) {
		return fmt.Errorf("%w: %w", ErrFrameTooLarge, err)
	}
	var unsupported *raftmodel.UnsupportedError
	if errors.As(err, &unsupported) {
		return fmt.Errorf("%w: %w", ErrUnsupportedFrame, err)
	}
	return fmt.Errorf("%w: %w", ErrInvalidFrame, err)
}

func messageOverlapsAppendRegion(dst []byte, count int, message *pb.Message) bool {
	if count <= 0 || count > cap(dst)-len(dst) {
		return false
	}
	region := dst[len(dst) : len(dst)+count : len(dst)+count]
	if byteSlicesOverlap(region, message.GetContext()) {
		return true
	}
	for _, entry := range message.GetEntries() {
		if byteSlicesOverlap(region, entry.GetData()) {
			return true
		}
	}
	return false
}

func byteSlicesOverlap(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	leftStart := uintptr(unsafe.Pointer(unsafe.SliceData(left)))
	rightStart := uintptr(unsafe.Pointer(unsafe.SliceData(right)))
	if leftStart <= rightStart {
		return rightStart-leftStart < uintptr(len(left))
	}
	return leftStart-rightStart < uintptr(len(right))
}

func (registry *StaticRegistry) validateStaticMessage(group raftmember.GroupKey, message *pb.Message) error {
	fromRole, err := registry.Role(group, message.GetFrom())
	if err != nil {
		return fmt.Errorf("%w: source member role is not registered", ErrUnauthorized)
	}
	toRole, err := registry.Role(group, message.GetTo())
	if err != nil {
		return fmt.Errorf("%w: destination member role is not registered", ErrUnauthorized)
	}
	for _, entry := range message.GetEntries() {
		if entry.GetType() != pb.EntryNormal {
			return fmt.Errorf("%w: dynamic configuration entry", ErrUnsupportedFrame)
		}
	}
	switch message.GetType() {
	case pb.MsgApp, pb.MsgHeartbeat:
		if fromRole != MemberVoter {
			return fmt.Errorf("%w: learner cannot originate leader message", ErrUnauthorized)
		}
	case pb.MsgAppResp, pb.MsgHeartbeatResp:
		if toRole != MemberVoter {
			return fmt.Errorf("%w: response target is not a voter", ErrUnauthorized)
		}
	case pb.MsgVote, pb.MsgVoteResp, pb.MsgPreVote, pb.MsgPreVoteResp:
		if fromRole != MemberVoter || toRole != MemberVoter {
			return fmt.Errorf("%w: vote traffic requires voters", ErrUnauthorized)
		}
	default:
		return fmt.Errorf("%w: ordinary type %s", ErrUnsupportedFrame, message.GetType())
	}
	return nil
}
