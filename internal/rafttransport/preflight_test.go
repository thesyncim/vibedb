package rafttransport

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestPreflightAcceptsCanonicalOrdinaryPayloads(t *testing.T) {
	for _, messageType := range []pb.MessageType{
		pb.MsgApp,
		pb.MsgAppResp,
		pb.MsgVote,
		pb.MsgVoteResp,
		pb.MsgHeartbeat,
		pb.MsgHeartbeatResp,
		pb.MsgPreVote,
		pb.MsgPreVoteResp,
	} {
		t.Run(messageType.String(), func(t *testing.T) {
			payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(frameTestMessage(messageType, 12, 11))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if err := preflightOrdinaryPayload(payload); err != nil {
				t.Fatalf("preflightOrdinaryPayload: %v", err)
			}
		})
	}
}

func TestPreflightRejectsMalformedAndNoncanonicalProtobuf(t *testing.T) {
	entryUnknown := wireVarint(nil, 5, 1)
	entryWrongWire := wireBytes(nil, 2, nil)
	entryDuplicate := wireVarint(nil, 2, 5)
	entryDuplicate = wireVarint(entryDuplicate, 2, 5)
	entryNoncanonical := []byte{0x10, 0x85, 0x00}

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "unknown message field", payload: wireVarint(nil, 15, 1)},
		{name: "wrong message wire type", payload: wireBytes(nil, 1, nil)},
		{name: "noncanonical tag", payload: []byte{0x88, 0x00, 0x03}},
		{name: "noncanonical varint", payload: []byte{0x08, 0x83, 0x00}},
		{name: "noncanonical length", payload: []byte{0x62, 0x80, 0x00}},
		{name: "duplicate singular message field", payload: []byte{0x08, 0x03, 0x08, 0x03}},
		{name: "invalid boolean", payload: wireVarint(nil, 10, 2)},
		{name: "malformed tag", payload: []byte{0x80}},
		{name: "malformed varint", payload: []byte{0x08, 0x80}},
		{name: "truncated bytes", payload: []byte{0x62, 0x02, 0x01}},
		{name: "entry unknown field", payload: wireBytes(nil, 7, entryUnknown)},
		{name: "entry wrong wire type", payload: wireBytes(nil, 7, entryWrongWire)},
		{name: "entry duplicate field", payload: wireBytes(nil, 7, entryDuplicate)},
		{name: "entry noncanonical varint", payload: wireBytes(nil, 7, entryNoncanonical)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := preflightOrdinaryPayload(test.payload); !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("error = %v, want ErrInvalidFrame", err)
			}
		})
	}
}

func TestPreflightRejectsGraphAmplificationFieldsBeforeTraversal(t *testing.T) {
	recursive := []byte{0x80}
	for range 128 {
		recursive = wireBytes(nil, 14, recursive)
	}
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "snapshot", payload: wireBytes(nil, 9, []byte{0x80})},
		{name: "local vote", payload: wireVarint(nil, 13, 0)},
		{name: "recursive responses", payload: recursive},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := preflightOrdinaryPayload(test.payload); !errors.Is(err, ErrUnsupportedFrame) {
				t.Fatalf("error = %v, want ErrUnsupportedFrame", err)
			}
		})
	}
}

func TestPreflightRejectsTooManyTinyEntries(t *testing.T) {
	payload := make([]byte, 0, 2*(raftmodel.MaxMessageEntries+1))
	for range raftmodel.MaxMessageEntries + 1 {
		payload = wireBytes(payload, 7, nil)
	}
	if err := preflightOrdinaryPayload(payload); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("error = %v, want ErrFrameTooLarge", err)
	}
}

func TestPreflightRejectsContextAndEntryDataAboveSemanticBounds(t *testing.T) {
	if err := preflightOrdinaryPayload(wireBytes(nil, 12, make([]byte, raftmodel.MaxReadContextBytes+1))); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("context error = %v, want ErrFrameTooLarge", err)
	}
	entry := wireBytes(nil, 4, make([]byte, raftmodel.MaxProposalBytes+1))
	if err := preflightOrdinaryPayload(wireBytes(nil, 7, entry)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("entry data error = %v, want ErrFrameTooLarge", err)
	}
}

func TestRaftPBDescriptorFieldInventory(t *testing.T) {
	messageFields := []descriptorField{
		{1, protoreflect.EnumKind, protoreflect.Optional},
		{2, protoreflect.Uint64Kind, protoreflect.Optional},
		{3, protoreflect.Uint64Kind, protoreflect.Optional},
		{4, protoreflect.Uint64Kind, protoreflect.Optional},
		{5, protoreflect.Uint64Kind, protoreflect.Optional},
		{6, protoreflect.Uint64Kind, protoreflect.Optional},
		{7, protoreflect.MessageKind, protoreflect.Repeated},
		{8, protoreflect.Uint64Kind, protoreflect.Optional},
		{9, protoreflect.MessageKind, protoreflect.Optional},
		{10, protoreflect.BoolKind, protoreflect.Optional},
		{11, protoreflect.Uint64Kind, protoreflect.Optional},
		{12, protoreflect.BytesKind, protoreflect.Optional},
		{13, protoreflect.Uint64Kind, protoreflect.Optional},
		{14, protoreflect.MessageKind, protoreflect.Repeated},
	}
	entryFields := []descriptorField{
		{1, protoreflect.EnumKind, protoreflect.Optional},
		{2, protoreflect.Uint64Kind, protoreflect.Optional},
		{3, protoreflect.Uint64Kind, protoreflect.Optional},
		{4, protoreflect.BytesKind, protoreflect.Optional},
	}
	assertDescriptorFields(t, (&pb.Message{}).ProtoReflect().Descriptor().Fields(), messageFields)
	assertDescriptorFields(t, (&pb.Entry{}).ProtoReflect().Descriptor().Fields(), entryFields)
}

type descriptorField struct {
	number      protoreflect.FieldNumber
	kind        protoreflect.Kind
	cardinality protoreflect.Cardinality
}

func assertDescriptorFields(t testing.TB, fields protoreflect.FieldDescriptors, want []descriptorField) {
	t.Helper()
	if fields.Len() != len(want) {
		t.Fatalf("descriptor field count = %d, want %d", fields.Len(), len(want))
	}
	for _, expected := range want {
		field := fields.ByNumber(expected.number)
		if field == nil {
			t.Fatalf("descriptor has no field %d", expected.number)
		}
		if field.Kind() != expected.kind || field.Cardinality() != expected.cardinality {
			t.Fatalf("field %d = (%s, %s), want (%s, %s)",
				expected.number, field.Kind(), field.Cardinality(), expected.kind, expected.cardinality)
		}
	}
}

func wireVarint(dst []byte, number protowire.Number, value uint64) []byte {
	dst = protowire.AppendTag(dst, number, protowire.VarintType)
	return protowire.AppendVarint(dst, value)
}

func wireBytes(dst []byte, number protowire.Number, value []byte) []byte {
	dst = protowire.AppendTag(dst, number, protowire.BytesType)
	return protowire.AppendBytes(dst, value)
}
