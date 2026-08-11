package replication

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func FuzzOpenCommandV1(f *testing.F) {
	valid := encodeCommand(f, testCommand())
	ordered := testCommand()
	ordered.Mutations = []Mutation{
		{Kind: MutationPut, Key: []byte("z"), Value: []byte("first")},
		{Kind: MutationDelete, Key: []byte("z")},
		{Kind: MutationPut, Key: []byte("a"), Value: []byte("descending")},
	}
	f.Add(valid)
	f.Add(encodeCommand(f, ordered))
	f.Add(valid[:len(valid)-1])
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0}, commandHeaderBytes+envelopeChecksumBytes))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxCommandBytes+1 {
			data = data[:MaxCommandBytes+1]
		}
		view, err := OpenCommandV1(data)
		if err != nil {
			return
		}
		assertFuzzCommandView(t, data, view)
	})
}

func FuzzOpenCommandV1ResealedFields(f *testing.F) {
	valid := encodeCommand(f, testCommand())
	mutation := commandMutationOffset(valid)
	distribution := commandHeaderBytes + len(testCommand().Tenant)
	for _, seed := range []struct {
		field uint8
		value uint64
	}{
		{0, 0}, {2, commandHeaderBytes}, {3, uint64(len(valid))},
		{5, MaxMutations + 1}, {8, MaxIdentityBytes + 1},
		{14, MaxMutationKeyBytes + 1}, {15, MaxMutationValueBytes + 1},
	} {
		f.Add(seed.field, seed.value)
	}
	f.Fuzz(func(t *testing.T, field uint8, value uint64) {
		candidate := append([]byte(nil), valid...)
		switch field % 18 {
		case 0:
			candidate[10] = byte(value)
		case 1:
			candidate[11] = byte(value)
		case 2:
			binary.LittleEndian.PutUint16(candidate[12:14], uint16(value))
		case 3:
			binary.LittleEndian.PutUint32(candidate[16:20], uint32(value))
		case 4:
			binary.LittleEndian.PutUint32(candidate[20:24], uint32(value))
		case 5:
			binary.LittleEndian.PutUint32(candidate[24:28], uint32(value))
		case 6:
			candidate[248+int(value%8)] = byte(value >> 8)
		case 7:
			scalarOffsets := [...]int{64, 104, 112, 120, 128, 136, 144, 152, 160, 184, 192}
			offset := scalarOffsets[value%uint64(len(scalarOffsets))]
			binary.LittleEndian.PutUint64(candidate[offset:offset+8], value)
		case 8:
			binary.LittleEndian.PutUint16(candidate[240:242], uint16(value))
		case 9:
			binary.LittleEndian.PutUint16(candidate[242:244], uint16(value))
		case 10:
			binary.LittleEndian.PutUint16(candidate[244:246], uint16(value))
		case 11:
			binary.LittleEndian.PutUint16(candidate[246:248], uint16(value))
		case 12:
			candidate[mutation] = byte(value)
		case 13:
			candidate[mutation+1] = byte(value)
		case 14:
			binary.LittleEndian.PutUint16(candidate[mutation+2:mutation+4], uint16(value))
		case 15:
			binary.LittleEndian.PutUint32(candidate[mutation+4:mutation+8], uint32(value))
		case 16:
			candidate[distribution] = byte(value)
		case 17:
			candidate[200+int(value%32)] = byte(value >> 8)
		}
		sealEnvelope(candidate)
		view, err := OpenCommandV1(candidate)
		if err == nil {
			assertFuzzCommandView(t, candidate, view)
		}
	})
}

func assertFuzzCommandView(t *testing.T, data []byte, view CommandViewV1) {
	t.Helper()
	if !bytes.Equal(view.Bytes(), data) || cap(view.Bytes()) != len(view.Bytes()) ||
		view.MutationCount() < 1 || view.MutationCount() > MaxMutations {
		t.Fatal("accepted command view does not preserve bounded input")
	}
	for _, borrowed := range [][]byte{view.Tenant, view.Distribution, view.Shard, view.Collection} {
		if cap(borrowed) != len(borrowed) {
			t.Fatal("accepted command field is not capacity-clamped")
		}
	}
	iterator := view.Mutations()
	if cap(iterator.b) != len(iterator.b) {
		t.Fatal("accepted mutation iterator is not capacity-clamped")
	}
	count := 0
	for iterator.Next() {
		mutation := iterator.Mutation()
		if len(mutation.Key) == 0 || len(mutation.Key) > MaxMutationKeyBytes ||
			cap(mutation.Key) != len(mutation.Key) || cap(mutation.Value) != len(mutation.Value) {
			t.Fatal("accepted invalid or unclamped mutation")
		}
		switch mutation.Kind {
		case MutationPut:
			if len(mutation.Value) == 0 || len(mutation.Value) > MaxMutationValueBytes {
				t.Fatal("accepted invalid put")
			}
		case MutationDelete:
			if len(mutation.Value) != 0 {
				t.Fatal("accepted invalid delete")
			}
		default:
			t.Fatal("accepted unknown mutation kind")
		}
		if cap(iterator.b) != len(iterator.b) {
			t.Fatal("advanced mutation iterator is not capacity-clamped")
		}
		count++
	}
	if count != view.MutationCount() {
		t.Fatalf("iterated %d mutations, header declares %d", count, view.MutationCount())
	}
}

func FuzzOpenCompletionV1(f *testing.F) {
	inline := encodeCompletion(f, testInlineCompletion())
	reference := encodeCompletion(f, testReferenceCompletion())
	f.Add(inline)
	f.Add(reference)
	f.Add(inline[:len(inline)-1])
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0}, completionHeaderBytes+envelopeChecksumBytes))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxCompletionEnvelopeBytes+1 {
			data = data[:MaxCompletionEnvelopeBytes+1]
		}
		view, err := OpenCompletionV1(data)
		if err != nil {
			return
		}
		assertFuzzCompletionView(t, data, view)
	})
}

func FuzzOpenCompletionV1ResealedFields(f *testing.F) {
	valid := encodeCompletion(f, testInlineCompletion())
	inline := completionInlineOffset(valid)
	for _, seed := range []struct {
		field uint8
		value uint64
	}{
		{0, 0}, {2, completionHeaderBytes}, {4, uint64(len(valid))},
		{5, uint64(len(testInlineCompletion().InlineResult))},
		{10, MaxIdentityBytes + 1}, {13, MaxCompletionResultBytes + 1},
	} {
		f.Add(seed.field, seed.value)
	}
	f.Fuzz(func(t *testing.T, field uint8, value uint64) {
		candidate := append([]byte(nil), valid...)
		switch field % 18 {
		case 0:
			candidate[10] = byte(value)
		case 1:
			candidate[11] = byte(value)
		case 2:
			binary.LittleEndian.PutUint16(candidate[12:14], uint16(value))
		case 3:
			binary.LittleEndian.PutUint16(candidate[14:16], uint16(value))
		case 4:
			binary.LittleEndian.PutUint32(candidate[16:20], uint32(value))
		case 5:
			binary.LittleEndian.PutUint32(candidate[20:24], uint32(value))
		case 6:
			binary.LittleEndian.PutUint32(candidate[28:32], uint32(value))
		case 7:
			scalarOffsets := [...]int{64, 104, 112, 120, 128, 136, 144, 168, 176, 184}
			offset := scalarOffsets[value%uint64(len(scalarOffsets))]
			binary.LittleEndian.PutUint64(candidate[offset:offset+8], value)
		case 8:
			candidate[224+int(value%32)] = byte(value >> 8)
		case 9:
			candidate[278+int(value%10)] = byte(value >> 8)
		case 10:
			binary.LittleEndian.PutUint16(candidate[272:274], uint16(value))
		case 11:
			binary.LittleEndian.PutUint16(candidate[274:276], uint16(value))
		case 12:
			binary.LittleEndian.PutUint16(candidate[276:278], uint16(value))
		case 13:
			binary.LittleEndian.PutUint64(candidate[264:272], value)
		case 14:
			candidate[inline] = byte(value)
		case 15:
			candidate[32+int(value%16)] = byte(value >> 8)
		case 16:
			candidate[192+int(value%32)] = byte(value >> 8)
		case 17:
			binary.LittleEndian.PutUint32(candidate[24:28], uint32(value))
		}
		sealEnvelope(candidate)
		view, err := OpenCompletionV1(candidate)
		if err == nil {
			assertFuzzCompletionView(t, candidate, view)
		}
	})
}

func assertFuzzCompletionView(t *testing.T, data []byte, view CompletionViewV1) {
	t.Helper()
	if !bytes.Equal(view.Bytes(), data) || cap(view.Bytes()) != len(view.Bytes()) ||
		view.ResultLength > MaxCompletionResultBytes {
		t.Fatal("accepted completion view does not preserve bounded input")
	}
	for _, borrowed := range [][]byte{view.Tenant, view.Distribution, view.Shard, view.InlineResult} {
		if cap(borrowed) != len(borrowed) {
			t.Fatal("accepted completion field is not capacity-clamped")
		}
	}
	if view.Storage == CompletionInline {
		if uint64(len(view.InlineResult)) != view.ResultLength ||
			view.ResultLength > MaxInlineCompletionBytes {
			t.Fatal("accepted noncanonical inline completion")
		}
	} else if view.Storage != CompletionDigestReference ||
		len(view.InlineResult) != 0 || view.ResultLength <= MaxInlineCompletionBytes {
		t.Fatal("accepted noncanonical referenced completion")
	}
}
