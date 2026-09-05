package raftauthority

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"time"
)

var ErrInvalidWire = errors.New("raftauthority: invalid canonical wire message")

const (
	MessageRequest MessageKind = 1
	MessageGrant   MessageKind = 2

	// CanonicalMessageBytes is fixed so transport admission can reserve one
	// bounded record before decoding. The final four bytes are reserved and
	// must remain zero for forward-incompatible versions.
	CanonicalMessageBytes = 240
)

var wireMagic = [8]byte{'V', 'B', 'R', 'A', 'U', 'T', 'H', '1'}

// Message is the only authority payload admitted by the custom transport
// frame. Request and Grant share one fixed layout to keep the stream parser
// allocation bounded and make unknown fields impossible.
type MessageKind uint8

type Message struct {
	Kind    MessageKind
	Request AuthorityRequest
	Grant   AuthorityGrant
}

func (message Message) validShape() error {
	if message.Kind != MessageRequest && message.Kind != MessageGrant {
		return ErrInvalidWire
	}
	request := message.Request
	if message.Kind == MessageRequest && message.Grant != (AuthorityGrant{}) {
		return ErrInvalidWire
	}
	if message.Kind == MessageGrant {
		if message.Grant.Request != request || message.Grant.Voter == 0 ||
			message.Grant.GrantedAt < 0 || message.Grant.PromiseUntil <= message.Grant.GrantedAt {
			return ErrInvalidWire
		}
	}
	if request.Term == 0 || request.Holder == 0 || request.HolderIncarnation == 0 ||
		request.Nonce == 0 || request.StartAt < 0 || !request.Config.stable() ||
		request.PolicyVersion == 0 || request.PolicyDigest == ([32]byte{}) {
		return ErrInvalidWire
	}
	return nil
}

// AppendCanonical appends one fixed-width authority message. It does not
// retain any caller-owned bytes.
func AppendCanonical(dst []byte, message Message) ([]byte, error) {
	if err := message.validShape(); err != nil || len(dst) > math.MaxInt-CanonicalMessageBytes {
		return dst, ErrInvalidWire
	}
	start := len(dst)
	dst = append(dst, make([]byte, CanonicalMessageBytes)...)
	raw := dst[start:]
	copy(raw[:8], wireMagic[:])
	raw[8] = byte(message.Kind)
	// [9:16] is reserved and remains zero.
	offset := 16
	offset = appendGroup(raw, offset, message.Request.Group)
	offset = appendUint64(raw, offset, message.Request.Term)
	offset = appendUint64(raw, offset, message.Request.Holder)
	offset = appendUint64(raw, offset, message.Request.HolderIncarnation)
	offset = appendUint64(raw, offset, message.Request.Config.AppliedVersion)
	copy(raw[offset:offset+32], message.Request.Config.Digest[:])
	offset += 32
	if message.Request.Config.Joint {
		raw[offset] = 1
	}
	if message.Request.Config.Pending {
		raw[offset+1] = 1
	}
	offset += 8 // two flags plus six reserved bytes
	binary.BigEndian.PutUint32(raw[offset:offset+4], message.Request.PolicyVersion)
	offset += 4
	copy(raw[offset:offset+32], message.Request.PolicyDigest[:])
	offset += 32
	offset = appendUint64(raw, offset, message.Request.Nonce)
	offset = appendInt64(raw, offset, message.Request.StartAt)
	if message.Kind == MessageGrant {
		offset = appendUint64(raw, offset, message.Grant.Voter)
		offset = appendInt64(raw, offset, message.Grant.GrantedAt)
		offset = appendInt64(raw, offset, message.Grant.PromiseUntil)
	}
	if offset > CanonicalMessageBytes {
		return dst[:start], ErrInvalidWire
	}
	return dst, nil
}

// OpenCanonical opens exactly one complete fixed-width message and borrows no
// input bytes.
func OpenCanonical(raw []byte) (Message, error) {
	if len(raw) != CanonicalMessageBytes || !bytes.Equal(raw[:8], wireMagic[:]) ||
		!allZero(raw[9:16]) {
		return Message{}, ErrInvalidWire
	}
	message := Message{Kind: MessageKind(raw[8])}
	offset := 16
	offset, message.Request.Group = readGroup(raw, offset)
	offset, message.Request.Term = readUint64(raw, offset)
	offset, message.Request.Holder = readUint64(raw, offset)
	offset, message.Request.HolderIncarnation = readUint64(raw, offset)
	offset, message.Request.Config.AppliedVersion = readUint64(raw, offset)
	copy(message.Request.Config.Digest[:], raw[offset:offset+32])
	offset += 32
	flags := raw[offset : offset+8]
	if flags[0] > 1 || flags[1] > 1 || !allZero(flags[2:]) {
		return Message{}, ErrInvalidWire
	}
	message.Request.Config.Joint = flags[0] != 0
	message.Request.Config.Pending = flags[1] != 0
	offset += 8
	message.Request.PolicyVersion = binary.BigEndian.Uint32(raw[offset : offset+4])
	offset += 4
	copy(message.Request.PolicyDigest[:], raw[offset:offset+32])
	offset += 32
	offset, message.Request.Nonce = readUint64(raw, offset)
	offset, message.Request.StartAt = readInt64(raw, offset)
	if message.Kind == MessageGrant {
		var grant AuthorityGrant
		grant.Request = message.Request
		offset, grant.Voter = readUint64(raw, offset)
		offset, grant.GrantedAt = readInt64(raw, offset)
		offset, grant.PromiseUntil = readInt64(raw, offset)
		message.Grant = grant
		if !allZero(raw[offset:]) {
			return Message{}, ErrInvalidWire
		}
	} else if message.Kind != MessageRequest || !allZero(raw[offset:]) {
		return Message{}, ErrInvalidWire
	}
	if err := message.validShape(); err != nil {
		return Message{}, err
	}
	return message, nil
}

func appendGroup(raw []byte, offset int, group GroupIdentity) int {
	copy(raw[offset:offset+16], group.ClusterID[:])
	offset += 16
	copy(raw[offset:offset+16], group.ClusterIncarnation[:])
	offset += 16
	offset = appendUint64(raw, offset, group.TopologyRecoveryEpoch)
	copy(raw[offset:offset+16], group.ShardIncarnation[:])
	offset += 16
	copy(raw[offset:offset+16], group.GroupID[:])
	offset += 16
	return offset
}

func appendUint64(raw []byte, offset int, value uint64) int {
	binary.BigEndian.PutUint64(raw[offset:offset+8], value)
	return offset + 8
}

func appendInt64(raw []byte, offset int, value time.Duration) int {
	binary.BigEndian.PutUint64(raw[offset:offset+8], uint64(value))
	return offset + 8
}

func readGroup(raw []byte, offset int) (int, GroupIdentity) {
	var group GroupIdentity
	copy(group.ClusterID[:], raw[offset:offset+16])
	offset += 16
	copy(group.ClusterIncarnation[:], raw[offset:offset+16])
	offset += 16
	offset, group.TopologyRecoveryEpoch = readUint64(raw, offset)
	copy(group.ShardIncarnation[:], raw[offset:offset+16])
	offset += 16
	copy(group.GroupID[:], raw[offset:offset+16])
	offset += 16
	return offset, group
}

func readUint64(raw []byte, offset int) (int, uint64) {
	return offset + 8, binary.BigEndian.Uint64(raw[offset : offset+8])
}

func readInt64(raw []byte, offset int) (int, time.Duration) {
	return offset + 8, time.Duration(int64(binary.BigEndian.Uint64(raw[offset : offset+8])))
}

func allZero(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}
