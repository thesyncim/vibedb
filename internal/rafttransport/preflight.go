package rafttransport

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// preflightOrdinaryPayload rejects hostile protobuf graph shapes without
// allocating message, entry, snapshot, response, or unknown-field objects.
func preflightOrdinaryPayload(payload []byte) error {
	var seen uint16
	var messageType uint64
	var term uint64
	entryCount := 0
	payloadBytes := int64(0)
	for len(payload) != 0 {
		number, wireType, tagBytes, err := consumeCanonicalTag(payload)
		if err != nil {
			return err
		}
		payload = payload[tagBytes:]
		if number == 9 || number == 13 || number == 14 {
			return fmt.Errorf("%w: snapshot, local vote, or recursive response field", ErrUnsupportedFrame)
		}
		if number < 1 || number > 14 {
			return fmt.Errorf("%w: unknown protobuf field %d", ErrInvalidFrame, number)
		}
		if number != 7 {
			bit := uint16(1) << number
			if seen&bit != 0 {
				return fmt.Errorf("%w: duplicate protobuf field %d", ErrInvalidFrame, number)
			}
			seen |= bit
		}

		switch number {
		case 1, 2, 3, 4, 5, 6, 8, 10, 11:
			if wireType != protowire.VarintType {
				return wrongWireType(number, wireType)
			}
			value, consumed, err := consumeCanonicalVarint(payload)
			if err != nil {
				return err
			}
			if number == 10 && value > 1 {
				return fmt.Errorf("%w: non-boolean reject field", ErrInvalidFrame)
			}
			if number == 1 {
				messageType = value
			}
			if number == 4 {
				term = value
			}
			payload = payload[consumed:]
		case 7:
			if wireType != protowire.BytesType {
				return wrongWireType(number, wireType)
			}
			entryBytes, consumed, err := consumeCanonicalBytes(payload)
			if err != nil {
				return err
			}
			entryCount++
			if entryCount > raftmodel.MaxMessageEntries {
				return fmt.Errorf("%w: too many Raft entries", ErrFrameTooLarge)
			}
			dataBytes, err := preflightEntry(entryBytes)
			if err != nil {
				return err
			}
			if dataBytes > raftmodel.MaxPendingInputBytes-payloadBytes {
				return fmt.Errorf("%w: aggregate entry data", ErrFrameTooLarge)
			}
			payloadBytes += dataBytes
			payload = payload[consumed:]
		case 12:
			if wireType != protowire.BytesType {
				return wrongWireType(number, wireType)
			}
			context, consumed, err := consumeCanonicalBytes(payload)
			if err != nil {
				return err
			}
			if len(context) > raftmodel.MaxReadContextBytes {
				return fmt.Errorf("%w: Raft context bytes", ErrFrameTooLarge)
			}
			payload = payload[consumed:]
		default:
			return fmt.Errorf("%w: unsupported protobuf field %d", ErrInvalidFrame, number)
		}
	}
	if messageType == uint64(pb.MsgTimeoutNow) {
		const timeoutNowFields = uint16(1<<1 | 1<<2 | 1<<3 | 1<<4)
		if seen != timeoutNowFields || term == 0 {
			return fmt.Errorf("%w: malformed leader-transfer payload", ErrInvalidFrame)
		}
	}
	return nil
}

func preflightEntry(payload []byte) (int64, error) {
	var seen uint8
	dataBytes := int64(0)
	for len(payload) != 0 {
		number, wireType, tagBytes, err := consumeCanonicalTag(payload)
		if err != nil {
			return 0, err
		}
		payload = payload[tagBytes:]
		if number < 1 || number > 4 {
			return 0, fmt.Errorf("%w: unknown entry field %d", ErrInvalidFrame, number)
		}
		bit := uint8(1) << number
		if seen&bit != 0 {
			return 0, fmt.Errorf("%w: duplicate entry field %d", ErrInvalidFrame, number)
		}
		seen |= bit
		switch number {
		case 1, 2, 3:
			if wireType != protowire.VarintType {
				return 0, wrongWireType(number, wireType)
			}
			_, consumed, err := consumeCanonicalVarint(payload)
			if err != nil {
				return 0, err
			}
			payload = payload[consumed:]
		case 4:
			if wireType != protowire.BytesType {
				return 0, wrongWireType(number, wireType)
			}
			data, consumed, err := consumeCanonicalBytes(payload)
			if err != nil {
				return 0, err
			}
			if len(data) > raftmodel.MaxProposalBytes {
				return 0, fmt.Errorf("%w: entry data bytes", ErrFrameTooLarge)
			}
			dataBytes = int64(len(data))
			payload = payload[consumed:]
		}
	}
	return dataBytes, nil
}

func consumeCanonicalTag(src []byte) (protowire.Number, protowire.Type, int, error) {
	number, wireType, consumed := protowire.ConsumeTag(src)
	if consumed < 0 {
		return 0, 0, 0, fmt.Errorf("%w: protobuf tag: %w", ErrInvalidFrame, protowire.ParseError(consumed))
	}
	raw := uint64(number)<<3 | uint64(wireType)
	if consumed != protowire.SizeVarint(raw) {
		return 0, 0, 0, fmt.Errorf("%w: noncanonical protobuf tag", ErrInvalidFrame)
	}
	return number, wireType, consumed, nil
}

func consumeCanonicalVarint(src []byte) (uint64, int, error) {
	value, consumed := protowire.ConsumeVarint(src)
	if consumed < 0 {
		return 0, 0, fmt.Errorf("%w: protobuf varint: %w", ErrInvalidFrame, protowire.ParseError(consumed))
	}
	if consumed != protowire.SizeVarint(value) {
		return 0, 0, fmt.Errorf("%w: noncanonical protobuf varint", ErrInvalidFrame)
	}
	return value, consumed, nil
}

func consumeCanonicalBytes(src []byte) ([]byte, int, error) {
	length, prefixBytes, err := consumeCanonicalVarint(src)
	if err != nil {
		return nil, 0, err
	}
	if length > uint64(len(src)-prefixBytes) {
		return nil, 0, fmt.Errorf("%w: truncated protobuf bytes", ErrInvalidFrame)
	}
	end := prefixBytes + int(length)
	return src[prefixBytes:end], end, nil
}

func wrongWireType(number protowire.Number, wireType protowire.Type) error {
	return fmt.Errorf("%w: field %d has wire type %d", ErrInvalidFrame, number, wireType)
}
