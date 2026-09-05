package rafttransport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrUnauthorized reports a frame whose authenticated node is not the
	// statically registered source, or whose destination is not local.
	ErrUnauthorized = errors.New("rafttransport: unauthorized Raft message")
	// ErrRetiredAuthority identifies an otherwise bounded frame carrying the
	// exact authority generation revoked by the receiver's source removal.
	// Callers may drop and recover from this class, but must still apply current
	// membership checks before treating the rejection as benign.
	ErrRetiredAuthority = errors.New("rafttransport: retired replica-set authority")
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
	FrameHeaderBytes = 140
	// MaxFrameBytes lets an authenticated stream reader reject a declared length
	// before allocating a frame buffer.
	MaxFrameBytes = FrameHeaderBytes + raftmodel.MaxInboundMessageBytes

	frameCodecFormat   uint16 = 1
	frameKindOrdinary  byte   = 1
	frameKindAuthority byte   = 2
)

var frameMagic = [4]byte{'V', 'D', 'R', 'F'}

// Inbound owns Message and detaches it from the caller's frame buffer.
type Inbound struct {
	Group     raftmember.GroupKey
	From      uint64
	Message   *pb.Message
	Authority *raftauthority.Message
}

type frameHeader struct {
	kind        byte
	group       raftmember.GroupKey
	roster      [32]byte
	version     uint64
	from        uint64
	to          uint64
	payloadSize uint32
}

type outboundFramePlan struct {
	kind        byte
	destination NodeID
	roster      [32]byte
	version     uint64
	payloadSize int
	frameSize   int
}

// EncodeOutbound appends a canonical current-format frame to dst and returns
// the registered destination node. dst remains unchanged on error.
//
// The frame carries no node identity or authentication material. PeerTLS
// supplies the certificate-derived peer identity out of band.
func (registry *StaticRegistry) EncodeOutbound(
	dst []byte,
	outbound raftmember.OutboundMessage,
) ([]byte, NodeID, error) {
	plan, err := registry.preflightOutbound(outbound)
	if err != nil {
		return dst, NodeID{}, err
	}
	encoded, err := registry.appendOutbound(dst, outbound, plan)
	if err != nil {
		return encoded, NodeID{}, err
	}
	return encoded, plan.destination, nil
}

func (registry *StaticRegistry) preflightOutbound(
	outbound raftmember.OutboundMessage,
) (outboundFramePlan, error) {
	if registry == nil || (outbound.Message == nil && outbound.Authority == nil) ||
		(outbound.Message != nil && outbound.Authority != nil) {
		return outboundFramePlan{}, fmt.Errorf("%w: nil registry or mixed payload", ErrInvalidFrame)
	}
	if err := validateFrameGroup(outbound.Group); err != nil {
		return outboundFramePlan{}, err
	}
	if outbound.Group.ClusterID != registry.trustDomain.ClusterID ||
		outbound.Group.ClusterIncarnation != registry.trustDomain.ClusterIncarnation {
		return outboundFramePlan{}, fmt.Errorf("%w: group trust domain differs", ErrUnauthorized)
	}
	if outbound.From == 0 || outbound.To == 0 || outbound.From == outbound.To {
		return outboundFramePlan{}, fmt.Errorf("%w: outer and protobuf member IDs differ", ErrInvalidFrame)
	}
	if outbound.Message != nil &&
		(outbound.Message.GetFrom() != outbound.From || outbound.Message.GetTo() != outbound.To) {
		return outboundFramePlan{}, fmt.Errorf("%w: outer and protobuf member IDs differ", ErrInvalidFrame)
	}
	fromNode, err := registry.Node(outbound.Group, outbound.From)
	if err != nil || fromNode != registry.LocalNode() {
		return outboundFramePlan{}, fmt.Errorf("%w: source member is not local", ErrUnauthorized)
	}
	destination, err := registry.Node(outbound.Group, outbound.To)
	if err != nil || destination == registry.LocalNode() {
		return outboundFramePlan{}, fmt.Errorf("%w: destination member is not remote", ErrUnauthorized)
	}
	view, ok := registry.currentAuthority(outbound.Group)
	if !ok {
		return outboundFramePlan{}, fmt.Errorf("%w: missing committed authority", ErrUnauthorized)
	}
	roster, ok := registry.rosterDigest(outbound.Group)
	if !ok {
		return outboundFramePlan{}, fmt.Errorf("%w: missing group roster", ErrUnauthorized)
	}
	if outbound.Authority != nil {
		return registry.preflightAuthorityOutbound(outbound, view, destination, roster)
	}
	size, err := raftmember.MeasureOrdinaryMessage(outbound.Message)
	if err != nil {
		return outboundFramePlan{}, classifyOrdinaryError(err)
	}
	version := view.version
	// The target can still be a learner after the leader commits promotion:
	// either its configuration append or the commit notification may have
	// been lost during reconnect. A heartbeat tagged with the new generation
	// is then rejected before Raft can answer and trigger append retry. Use
	// the retained, already-authorized learner view for this exchange. This
	// never grants voting rights, and committed removal disables the prior view.
	if outbound.Message.GetType() == pb.MsgHeartbeat && view.allowPrevious &&
		view.previous != nil && view.roles[outbound.To] == MemberVoter &&
		view.previous.roles[outbound.To] == MemberLearner &&
		view.previous.roles[outbound.From] == MemberVoter {
		view = view.previous
		version = view.version
	}
	if election, electionOK := certifiedPromotionElectionAuthority(view, outbound.Message,
		view.promotionVersion()); electionOK {
		view = election
		version = election.version
	}
	if err := registry.validateAuthorizedMessage(outbound.Group, view, outbound.Message); err != nil {
		if retiredOutboundDestination(view, outbound.Message) {
			return outboundFramePlan{}, fmt.Errorf("%w: %w", err, errRetiredOutboundDestination)
		}
		return outboundFramePlan{}, err
	}
	if size > raftmodel.MaxInboundMessageBytes {
		return outboundFramePlan{}, fmt.Errorf("%w: payload bytes %d", ErrFrameTooLarge, size)
	}
	return outboundFramePlan{
		kind:        frameKindOrdinary,
		destination: destination,
		roster:      roster,
		version:     version,
		payloadSize: size,
		frameSize:   FrameHeaderBytes + size,
	}, nil
}

func (registry *StaticRegistry) preflightAuthorityOutbound(
	outbound raftmember.OutboundMessage,
	view *authorityView,
	destination NodeID,
	roster [32]byte,
) (outboundFramePlan, error) {
	message := outbound.Authority
	if message == nil || view == nil || roster == ([32]byte{}) {
		return outboundFramePlan{}, fmt.Errorf("%w: authority frame lacks current roster", ErrUnauthorized)
	}
	if err := validateAuthorityGroup(message.Request.Group); err != nil {
		return outboundFramePlan{}, err
	}
	if view.version == 0 || view.roles[outbound.From] != MemberVoter ||
		view.roles[outbound.To] != MemberVoter {
		return outboundFramePlan{}, fmt.Errorf("%w: authority endpoints are not current voters", ErrUnauthorized)
	}
	request := message.Request
	if request.Group != authorityGroup(outbound.Group) {
		return outboundFramePlan{}, fmt.Errorf("%w: authority group differs from frame group", ErrInvalidFrame)
	}
	switch message.Kind {
	case raftauthority.MessageRequest:
		if request.Holder != outbound.From {
			return outboundFramePlan{}, fmt.Errorf("%w: authority request route", ErrInvalidFrame)
		}
	case raftauthority.MessageGrant:
		if message.Grant.Voter != outbound.From || request.Holder != outbound.To {
			return outboundFramePlan{}, fmt.Errorf("%w: authority grant route", ErrInvalidFrame)
		}
	default:
		return outboundFramePlan{}, fmt.Errorf("%w: unknown authority kind", ErrUnsupportedFrame)
	}
	return outboundFramePlan{
		kind: frameKindAuthority, destination: destination, roster: roster,
		version: view.version, payloadSize: raftauthority.CanonicalMessageBytes,
		frameSize: FrameHeaderBytes + raftauthority.CanonicalMessageBytes,
	}, nil
}

func authorityGroup(group raftmember.GroupKey) raftauthority.GroupIdentity {
	return raftauthority.GroupIdentity{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		ShardIncarnation:      group.ShardIncarnation, GroupID: group.GroupID,
	}
}

func validateAuthorityGroup(group raftauthority.GroupIdentity) error {
	if group.ClusterID == ([16]byte{}) || group.ClusterIncarnation == ([16]byte{}) ||
		group.TopologyRecoveryEpoch == 0 || group.ShardIncarnation == ([16]byte{}) ||
		group.GroupID == ([16]byte{}) {
		return fmt.Errorf("%w: incomplete authority group lineage", ErrInvalidFrame)
	}
	return nil
}

func (registry *StaticRegistry) appendOutbound(
	dst []byte,
	outbound raftmember.OutboundMessage,
	plan outboundFramePlan,
) ([]byte, error) {
	if plan.frameSize < FrameHeaderBytes || plan.payloadSize != plan.frameSize-FrameHeaderBytes ||
		len(dst) > math.MaxInt-plan.frameSize {
		return dst, ErrFrameTooLarge
	}
	if plan.kind != frameKindOrdinary && plan.kind != frameKindAuthority {
		return dst, ErrUnsupportedFrame
	}
	if plan.kind == frameKindOrdinary && messageOverlapsAppendRegion(dst, plan.frameSize, outbound.Message) {
		return dst, fmt.Errorf("%w: message aliases frame append region", ErrInvalidFrame)
	}

	start := len(dst)
	// Grow the complete frame before any write. Besides keeping encode
	// allocation-free when the caller sized dst exactly, this prevents a
	// header-only reuse of dst's old backing array from overwriting message
	// slices before MarshalAppend relocates the payload.
	dst = slices.Grow(dst, plan.frameSize)
	dst = append(dst, make([]byte, FrameHeaderBytes)...)
	var err error
	if plan.kind == frameKindAuthority {
		dst, err = raftauthority.AppendCanonical(dst, *outbound.Authority)
		if err != nil {
			return dst[:start], fmt.Errorf("%w: marshal authority message: %w", ErrInvalidFrame, err)
		}
	} else {
		dst, err = (proto.MarshalOptions{Deterministic: true}).MarshalAppend(dst, outbound.Message)
		if err != nil {
			return dst[:start], fmt.Errorf("%w: marshal ordinary message: %w", ErrInvalidFrame, err)
		}
	}
	if len(dst) != start+plan.frameSize {
		return dst[:start], fmt.Errorf("%w: payload size changed during encode", ErrInvalidFrame)
	}
	header := dst[start : start+FrameHeaderBytes]
	copy(header[0:4], frameMagic[:])
	binary.BigEndian.PutUint16(header[4:6], frameCodecFormat)
	header[6] = plan.kind
	header[7] = 0
	appendGroupKey(header[8:80], outbound.Group)
	copy(header[80:112], plan.roster[:])
	binary.BigEndian.PutUint64(header[112:120], plan.version)
	binary.BigEndian.PutUint64(header[120:128], outbound.From)
	binary.BigEndian.PutUint64(header[128:136], outbound.To)
	binary.BigEndian.PutUint32(header[136:140], uint32(plan.payloadSize))
	return dst, nil
}

// DecodeInbound admits and decodes one complete canonical frame. The
// authenticated node comes from PeerTLS. This method neither performs TLS nor
// derives identity from frame bytes.
func (registry *StaticRegistry) DecodeInbound(authenticated PeerIdentity, frame []byte) (Inbound, error) {
	if registry == nil || !validPeerIdentity(authenticated) ||
		authenticated.TrustDomain != registry.trustDomain {
		return Inbound{}, fmt.Errorf("%w: missing registry or peer identity", ErrUnauthorized)
	}
	header, payload, err := parseFrame(frame)
	if err != nil {
		return Inbound{}, err
	}
	if header.group.ClusterID != registry.trustDomain.ClusterID ||
		header.group.ClusterIncarnation != registry.trustDomain.ClusterIncarnation {
		return Inbound{}, fmt.Errorf("%w: frame trust domain differs", ErrUnauthorized)
	}
	source, err := registry.Node(header.group, header.from)
	if err != nil || source != authenticated.Node {
		return Inbound{}, fmt.Errorf("%w: authenticated source is not registered member", ErrUnauthorized)
	}
	destination, err := registry.Node(header.group, header.to)
	if err != nil || destination != registry.LocalNode() {
		return Inbound{}, fmt.Errorf("%w: target member is not local", ErrUnauthorized)
	}
	roster, ok := registry.rosterDigest(header.group)
	if !ok || roster != header.roster {
		return Inbound{}, fmt.Errorf("%w: stable enrollment digest differs", ErrUnauthorized)
	}
	if header.kind == frameKindAuthority {
		return registry.decodeAuthorityInbound(header, payload)
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
	current, currentOK := registry.currentAuthority(header.group)
	view, ok := registry.authorityAt(header.group, header.version)
	if !ok {
		if currentOK {
			view, ok = certifiedPromotionElectionAuthority(current, message, header.version)
		}
	}
	if !ok {
		view, ok = registry.prospectiveAuthority(header.group, header.version, message)
	}
	if !ok {
		if currentOK && current.retiredVersion != 0 &&
			current.retiredVersion == header.version &&
			registry.validateAuthorizedMessage(header.group, current, message) == nil {
			return Inbound{}, fmt.Errorf("%w: %w", ErrUnauthorized, ErrRetiredAuthority)
		}
		return Inbound{}, fmt.Errorf("%w: replica-set generation is outside bounded authority", ErrUnauthorized)
	}
	if err := registry.validateAuthorizedMessage(header.group, view, message); err != nil {
		return Inbound{}, err
	}
	scratch := registry.canonical.get(len(payload))
	canonical, err := (proto.MarshalOptions{Deterministic: true}).MarshalAppend(
		scratch.bytes[:0], message,
	)
	if err != nil {
		registry.canonical.put(scratch)
		return Inbound{}, fmt.Errorf("%w: re-encode protobuf: %w", ErrInvalidFrame, err)
	}
	scratch.bytes = canonical
	equal := bytes.Equal(payload, canonical)
	registry.canonical.put(scratch)
	if !equal {
		return Inbound{}, fmt.Errorf("%w: noncanonical protobuf payload", ErrInvalidFrame)
	}
	return Inbound{Group: header.group, From: header.from, Message: message}, nil
}

func (registry *StaticRegistry) decodeAuthorityInbound(
	header frameHeader,
	payload []byte,
) (Inbound, error) {
	if len(payload) != raftauthority.CanonicalMessageBytes {
		return Inbound{}, fmt.Errorf("%w: authority payload bytes %d", ErrUnsupportedFrame, len(payload))
	}
	view, ok := registry.currentAuthority(header.group)
	if !ok || view == nil || header.version != view.version {
		return Inbound{}, fmt.Errorf("%w: authority generation is not current", ErrUnauthorized)
	}
	if view.roles[header.from] != MemberVoter || view.roles[header.to] != MemberVoter {
		return Inbound{}, fmt.Errorf("%w: authority endpoints are not current voters", ErrUnauthorized)
	}
	message, err := raftauthority.OpenCanonical(payload)
	if err != nil {
		return Inbound{}, fmt.Errorf("%w: authority payload: %w", ErrInvalidFrame, err)
	}
	if message.Request.Group != authorityGroup(header.group) {
		return Inbound{}, fmt.Errorf("%w: authority group differs from frame group", ErrInvalidFrame)
	}
	switch message.Kind {
	case raftauthority.MessageRequest:
		if message.Request.Holder != header.from {
			return Inbound{}, fmt.Errorf("%w: authority request route", ErrInvalidFrame)
		}
	case raftauthority.MessageGrant:
		if message.Grant.Voter != header.from || message.Request.Holder != header.to {
			return Inbound{}, fmt.Errorf("%w: authority grant route", ErrInvalidFrame)
		}
	default:
		return Inbound{}, fmt.Errorf("%w: unknown authority kind", ErrUnsupportedFrame)
	}
	return Inbound{Group: header.group, From: header.from, Authority: &message}, nil
}

func (view *authorityView) promotionVersion() uint64 {
	if view == nil || view.promotion == nil {
		return 0
	}
	return view.promotion.Version
}

func certifiedPromotionElectionAuthority(
	current *authorityView,
	message *pb.Message,
	version uint64,
) (*authorityView, bool) {
	if current == nil || current.promotion == nil || message == nil ||
		version != current.promotion.Version ||
		current.promotion.TargetMember != current.grant.TargetMember ||
		current.roles[current.promotion.TargetMember] != MemberLearner {
		return nil, false
	}
	target := current.promotion.TargetMember
	from, to := message.GetFrom(), message.GetTo()
	switch message.GetType() {
	case pb.MsgVote, pb.MsgPreVote:
		if to != target || current.roles[from] != MemberVoter {
			return nil, false
		}
	case pb.MsgVoteResp, pb.MsgPreVoteResp:
		if from != target || current.roles[to] != MemberVoter {
			return nil, false
		}
	default:
		return nil, false
	}
	roles := make(map[uint64]MemberRole, len(current.roles))
	for member, role := range current.roles {
		roles[member] = role
	}
	roles[target] = MemberVoter
	return &authorityView{version: version, roles: roles, grant: current.grant}, true
}

func (registry *StaticRegistry) prospectiveAuthority(
	group raftmember.GroupKey,
	version uint64,
	message *pb.Message,
) (*authorityView, bool) {
	current, ok := registry.currentAuthority(group)
	if !ok || version <= current.version || message == nil {
		return nil, false
	}
	for _, entry := range message.GetEntries() {
		if entry == nil || entry.GetIndex() != version ||
			(entry.GetType() != pb.EntryConfChange && entry.GetType() != pb.EntryConfChangeV2) {
			continue
		}
		change, member, digest, err := openSingleConfChange(entry)
		if err != nil || !authorizedConfChange(current, change, member, digest) {
			return nil, false
		}
		roles := make(map[uint64]MemberRole, len(current.roles)+1)
		for id, role := range current.roles {
			roles[id] = role
		}
		switch change {
		case pb.ConfChangeAddLearnerNode:
			roles[member] = MemberLearner
		case pb.ConfChangeAddNode:
			roles[member] = MemberVoter
		case pb.ConfChangeRemoveNode:
			delete(roles, member)
		default:
			return nil, false
		}
		previous := &authorityView{version: current.version, roles: current.roles, grant: current.grant}
		return &authorityView{version: version, roles: roles, grant: current.grant,
			previous: previous}, true
	}
	return nil, false
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
		(frame[6] != frameKindOrdinary && frame[6] != frameKindAuthority) || frame[7] != 0 {
		return frameHeader{}, nil, fmt.Errorf("%w: magic, format, kind, or flags", ErrUnsupportedFrame)
	}
	header := frameHeader{
		kind:        frame[6],
		group:       openGroupKey(frame[8:80]),
		version:     binary.BigEndian.Uint64(frame[112:120]),
		from:        binary.BigEndian.Uint64(frame[120:128]),
		to:          binary.BigEndian.Uint64(frame[128:136]),
		payloadSize: binary.BigEndian.Uint32(frame[136:140]),
	}
	copy(header.roster[:], frame[80:112])
	if err := validateFrameGroup(header.group); err != nil {
		return frameHeader{}, nil, err
	}
	if header.version == 0 || header.from == 0 || header.to == 0 || header.from == header.to {
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

func (registry *StaticRegistry) validateAuthorizedMessage(
	group raftmember.GroupKey,
	view *authorityView,
	message *pb.Message,
) error {
	if view == nil {
		return fmt.Errorf("%w: missing dynamic authority", ErrUnauthorized)
	}
	fromRole := view.roles[message.GetFrom()]
	toRole := view.roles[message.GetTo()]
	configuration, err := validateAuthorizedConfiguration(view, message.GetEntries())
	if err != nil {
		return err
	}
	// During learner addition, stable enrollment permits only the exact target
	// to receive the matching configuration append and answer replication. It
	// grants no vote or leader role before committed publication.
	if toRole == MemberEnrolled && configuration && message.GetTo() == view.grant.TargetMember {
		toRole = MemberLearner
	}
	if fromRole == MemberEnrolled && message.GetFrom() == view.grant.TargetMember &&
		(message.GetType() == pb.MsgAppResp || message.GetType() == pb.MsgHeartbeatResp) {
		fromRole = MemberLearner
	}
	if fromRole == MemberEnrolled || toRole == MemberEnrolled {
		return fmt.Errorf("%w: member lacks committed role", ErrUnauthorized)
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
	case pb.MsgTimeoutNow:
		if fromRole != MemberVoter || toRole != MemberVoter {
			return fmt.Errorf("%w: leader-transfer traffic requires voters", ErrUnauthorized)
		}
	default:
		return fmt.Errorf("%w: ordinary type %s", ErrUnsupportedFrame, message.GetType())
	}
	return nil
}

func validateAuthorizedConfiguration(view *authorityView, entries []*pb.Entry) (bool, error) {
	found := false
	for index := range entries {
		entry := entries[index]
		if entry.GetType() == pb.EntryNormal {
			continue
		}
		if found || (entry.GetType() != pb.EntryConfChange && entry.GetType() != pb.EntryConfChangeV2) {
			return false, fmt.Errorf("%w: unsupported configuration batch", ErrUnauthorized)
		}
		change, member, digest, err := openSingleConfChange(entry)
		authorized := err == nil && authorizedConfChange(view, change, member, digest)
		if !authorized && view.previous != nil {
			authorized = err == nil && authorizedConfChange(view.previous, change, member, digest)
		}
		if !authorized && err == nil && entry.GetIndex() != 0 && entry.GetIndex() <= view.version {
			authorized = authorizedCommittedConfReplay(view, change, member, digest)
		}
		if !authorized {
			return false, fmt.Errorf("%w: configuration differs from metadata grant", ErrUnauthorized)
		}
		found = true
	}
	return found, nil
}

// A restarted/snapshot-installed member has its committed roles but not the
// previous in-memory authority view. Raft may still probe it with an append
// containing that already committed transition. The exact retained grant and
// resulting roles authorize only historical entries; they do not grant a new
// transition, a past sender role, or an older frame generation.
func authorizedCommittedConfReplay(
	view *authorityView,
	change pb.ConfChangeType,
	member uint64,
	digest [raftmember.MembershipTransitionDigestBytes]byte,
) bool {
	grant := view.grant
	if grant == (membershipgrant.Grant{}) || grant.Digest() != digest {
		return false
	}
	targetRole := view.roles[grant.TargetMember]
	switch change {
	case pb.ConfChangeAddLearnerNode:
		return member == grant.TargetMember && (targetRole == MemberLearner || targetRole == MemberVoter)
	case pb.ConfChangeAddNode:
		return member == grant.TargetMember && targetRole == MemberVoter
	case pb.ConfChangeRemoveNode:
		_, sourcePresent := view.roles[grant.SourceMember]
		return member == grant.SourceMember && targetRole == MemberVoter && !sourcePresent
	default:
		return false
	}
}

func openSingleConfChange(
	entry *pb.Entry,
) (pb.ConfChangeType, uint64, [raftmember.MembershipTransitionDigestBytes]byte, error) {
	var digest [raftmember.MembershipTransitionDigestBytes]byte
	if entry == nil {
		return 0, 0, digest, ErrInvalidFrame
	}
	if entry.GetType() == pb.EntryConfChange {
		var change pb.ConfChange
		if err := proto.Unmarshal(entry.GetData(), &change); err != nil ||
			len(change.GetContext()) != raftmember.MembershipTransitionDigestBytes {
			return 0, 0, digest, ErrInvalidFrame
		}
		canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(&change)
		if err != nil || !bytes.Equal(canonical, entry.GetData()) {
			return 0, 0, digest, ErrInvalidFrame
		}
		copy(digest[:], change.GetContext())
		return change.GetType(), change.GetNodeId(), digest, nil
	}
	var change pb.ConfChangeV2
	if err := proto.Unmarshal(entry.GetData(), &change); err != nil ||
		len(change.GetContext()) != raftmember.MembershipTransitionDigestBytes ||
		len(change.GetChanges()) != 1 || change.GetTransition() != pb.ConfChangeTransition_ConfChangeTransitionAuto {
		return 0, 0, digest, ErrInvalidFrame
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(&change)
	if err != nil || !bytes.Equal(canonical, entry.GetData()) {
		return 0, 0, digest, ErrInvalidFrame
	}
	copy(digest[:], change.GetContext())
	return change.GetChanges()[0].GetType(), change.GetChanges()[0].GetNodeId(), digest, nil
}

func authorizedConfChange(
	view *authorityView,
	change pb.ConfChangeType,
	member uint64,
	digest [raftmember.MembershipTransitionDigestBytes]byte,
) bool {
	grant := view.grant
	if grant == (membershipgrant.Grant{}) || grant.Digest() != digest {
		return false
	}
	targetRole := view.roles[grant.TargetMember]
	switch {
	case targetRole == MemberEnrolled:
		return change == pb.ConfChangeAddLearnerNode && member == grant.TargetMember
	case targetRole == MemberLearner:
		return change == pb.ConfChangeAddNode && member == grant.TargetMember
	case targetRole == MemberVoter:
		return change == pb.ConfChangeRemoveNode && member == grant.SourceMember
	default:
		return false
	}
}
