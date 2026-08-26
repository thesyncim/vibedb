package replication

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func FuzzOpenCommand(f *testing.F) {
	valid := encodeCommand(f, testCommand())
	retire := testSessionRetireCommand()
	release := testSessionReleaseCommand()
	open := testSessionOpenCommand()
	renew := testSessionRenewCommand()
	revoke := testSessionRevokeCommand()
	ordered := testCommand()
	ordered.Batches[0].Mutations = []Mutation{
		{Kind: MutationPut, Key: []byte("z"), Value: []byte("first")},
		{Kind: MutationDelete, Key: []byte("z")},
		{Kind: MutationPut, Key: []byte("a"), Value: []byte("descending")},
	}
	f.Add(valid)
	f.Add(encodeCommand(f, retire))
	f.Add(encodeCommand(f, release))
	f.Add(encodeCommand(f, open))
	f.Add(encodeCommand(f, renew))
	f.Add(encodeCommand(f, revoke))
	f.Add(encodeCommand(f, ordered))
	conditionalWrites := testCommand()
	conditionalWrites.Batches[0].Mutations = []Mutation{
		{Kind: MutationPutAbsent, Key: []byte("insert"), Value: []byte("new")},
		{Kind: MutationPutPresent, Key: []byte("update"), Value: []byte("replacement")},
	}
	f.Add(encodeCommand(f, conditionalWrites))
	multi := testCommand()
	multi.Batches = []RelationMutationBatch{
		{Relation: 1, Mutations: []Mutation{{Kind: MutationDelete, Key: []byte("a")}}},
		{Relation: 7, Mutations: []Mutation{{Kind: MutationPut, Key: []byte("b"), Value: []byte("value")}}},
		{Relation: MaxRelationID, Mutations: []Mutation{{Kind: MutationDelete, Key: []byte("c")}}},
	}
	f.Add(encodeCommand(f, multi))
	f.Add(valid[:len(valid)-1])
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0}, commandHeaderBytes+envelopeChecksumBytes))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxCommandBytes+1 {
			data = data[:MaxCommandBytes+1]
		}
		view, err := OpenCommand(data)
		if err != nil {
			return
		}
		assertFuzzCommandView(t, data, view)
	})
}

func FuzzOpenSessionLeaseResealedFields(f *testing.F) {
	for kind, command := range []Command{
		testSessionOpenCommand(), testSessionRenewCommand(), testSessionRevokeCommand(),
	} {
		f.Add(uint8(kind), uint64(command.ExpectedDeadlineUnixNano), uint64(command.NextDeadlineUnixNano))
	}
	f.Add(uint8(0), uint64(0), uint64(1)<<63)
	f.Add(uint8(1), uint64(1)<<63, uint64(1)<<63|1)
	f.Add(uint8(2), uint64(1)<<63, uint64(0))
	f.Fuzz(func(t *testing.T, selector uint8, expected, next uint64) {
		commands := [...]Command{
			testSessionOpenCommand(), testSessionRenewCommand(), testSessionRevokeCommand(),
		}
		candidate := encodeCommand(t, commands[selector%uint8(len(commands))])
		lease := commandPayloadOffset(candidate)
		binary.LittleEndian.PutUint64(candidate[lease:lease+8], expected)
		binary.LittleEndian.PutUint64(candidate[lease+8:lease+16], next)
		sealEnvelope(candidate)
		view, err := OpenCommand(candidate)
		if err == nil {
			assertFuzzCommandView(t, candidate, view)
		}
	})
}

func FuzzOpenCommandResealedFields(f *testing.F) {
	valid := encodeCommand(f, testCommand())
	mutation := commandPayloadOffset(valid)
	distribution := commandHeaderBytes + len(testCommand().Tenant)
	for _, seed := range []struct {
		field uint8
		value uint64
	}{
		{0, 0}, {2, commandHeaderBytes}, {3, uint64(len(valid))},
		{5, MaxMutations + 1}, {8, MaxIdentityBytes + 1},
		{6, testCommand().ClientSequence}, {11, MaxRelationBatches + 1},
		{12, MaxRelationID + 1}, {13, 1},
		{16, MaxMutationKeyBytes + 1}, {17, MaxMutationValueBytes + 1},
	} {
		f.Add(seed.field, seed.value)
	}
	f.Fuzz(func(t *testing.T, field uint8, value uint64) {
		candidate := append([]byte(nil), valid...)
		switch field % 20 {
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
			binary.LittleEndian.PutUint64(candidate[248:256], value)
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
			binary.LittleEndian.PutUint16(candidate[28:30], uint16(value))
		case 12:
			binary.LittleEndian.PutUint16(candidate[30:32], uint16(value))
		case 13:
			candidate[246] = byte(value)
		case 14:
			candidate[mutation] = byte(value)
		case 15:
			candidate[mutation+1] = byte(value)
		case 16:
			binary.LittleEndian.PutUint16(candidate[mutation+2:mutation+4], uint16(value))
		case 17:
			binary.LittleEndian.PutUint32(candidate[mutation+4:mutation+8], uint32(value))
		case 18:
			candidate[distribution] = byte(value)
		case 19:
			candidate[200+int(value%32)] = byte(value >> 8)
		}
		sealEnvelope(candidate)
		view, err := OpenCommand(candidate)
		if err == nil {
			assertFuzzCommandView(t, candidate, view)
		}
	})
}

func assertFuzzCommandView(t *testing.T, data []byte, view CommandView) {
	t.Helper()
	if !bytes.Equal(view.Bytes(), data) || cap(view.Bytes()) != len(view.Bytes()) {
		t.Fatal("accepted command view does not preserve bounded input")
	}
	switch view.Kind() {
	case CommandMutationBatch:
		if view.ClientEpoch == 0 || view.ClientSequence == 0 ||
			view.AckThrough >= view.ClientSequence || view.ExpectedDeadlineUnixNano != 0 ||
			view.NextDeadlineUnixNano != 0 {
			t.Fatal("accepted mutation batch has invalid client tuple")
		}
		if view.RelationCount() < 1 || view.RelationCount() > MaxRelationBatches ||
			view.MutationCount() < 1 || view.MutationCount() > MaxMutations {
			t.Fatal("accepted mutation batch has invalid mutation count")
		}
	case CommandSessionRetire, CommandSessionRelease:
		if view.ClientEpoch == 0 || view.ClientSequence == 0 ||
			view.AckThrough >= view.ClientSequence || view.ExpectedDeadlineUnixNano != 0 ||
			view.NextDeadlineUnixNano != 0 {
			t.Fatal("accepted session lifecycle command has invalid client tuple")
		}
		if view.RelationCount() != 0 || view.MutationCount() != 0 {
			t.Fatal("accepted session lifecycle command carries mutations")
		}
	case CommandSessionOpen:
		if view.ClientEpoch != 0 || view.ClientSequence != 1 || view.AckThrough != 0 {
			t.Fatal("accepted session open has invalid client tuple")
		}
		if view.ExpectedDeadlineUnixNano != 0 || view.NextDeadlineUnixNano <= 0 {
			t.Fatal("accepted session open has invalid lease deadlines")
		}
		if view.RelationCount() != 0 || view.MutationCount() != 0 {
			t.Fatal("accepted session open carries mutations")
		}
	case CommandSessionRenew:
		if view.ClientEpoch == 0 || view.ClientSequence == 0 ||
			view.AckThrough >= view.ClientSequence || view.ExpectedDeadlineUnixNano <= 0 ||
			view.NextDeadlineUnixNano <= view.ExpectedDeadlineUnixNano {
			t.Fatal("accepted session renew has invalid tuple or lease deadlines")
		}
		if view.RelationCount() != 0 || view.MutationCount() != 0 {
			t.Fatal("accepted session renew carries mutations")
		}
	case CommandSessionRevoke:
		if view.ClientEpoch == 0 || view.ClientSequence == 0 ||
			view.AckThrough >= view.ClientSequence || view.ExpectedDeadlineUnixNano <= 0 ||
			view.NextDeadlineUnixNano != 0 {
			t.Fatal("accepted session revoke has invalid tuple or lease deadlines")
		}
		if view.RelationCount() != 0 || view.MutationCount() != 0 {
			t.Fatal("accepted session revoke carries mutations")
		}
	default:
		t.Fatal("accepted unknown command kind")
	}
	for _, borrowed := range [][]byte{view.Tenant, view.Distribution, view.Shard} {
		if cap(borrowed) != len(borrowed) {
			t.Fatal("accepted command field is not capacity-clamped")
		}
	}
	relations := view.RelationBatches()
	if cap(relations.b) != len(relations.b) {
		t.Fatal("accepted relation iterator is not capacity-clamped")
	}
	count := 0
	batchCount := 0
	var previous RelationID
	for relations.Next() {
		batch := relations.Batch()
		if batch.Relation == 0 || batch.Relation > MaxRelationID ||
			batchCount != 0 && batch.Relation <= previous || batch.MutationCount() == 0 {
			t.Fatal("accepted invalid relation batch")
		}
		iterator := batch.Mutations()
		if cap(iterator.b) != len(iterator.b) {
			t.Fatal("accepted mutation iterator is not capacity-clamped")
		}
		batchMutations := 0
		for iterator.Next() {
			mutation := iterator.Mutation()
			if len(mutation.Key) == 0 || len(mutation.Key) > MaxMutationKeyBytes ||
				cap(mutation.Key) != len(mutation.Key) || cap(mutation.Value) != len(mutation.Value) ||
				cap(mutation.Compare) != len(mutation.Compare) {
				t.Fatal("accepted invalid or unclamped mutation")
			}
			switch mutation.Kind {
			case MutationPut, MutationPutAbsentOrEqual, MutationPutAbsent, MutationPutPresent:
				if len(mutation.Value) == 0 || len(mutation.Value) > MaxMutationValueBytes {
					t.Fatal("accepted invalid put")
				}
			case MutationDelete:
				if len(mutation.Value) != 0 || len(mutation.Compare) != 0 {
					t.Fatal("accepted invalid delete")
				}
			case MutationDeleteDigestEqual:
				if len(mutation.Value) != 0 || len(mutation.Compare) != MutationDigestCompareBytes ||
					mutation.ExpectedValueLength == 0 ||
					mutation.ExpectedValueLength > MaxMutationValueBytes ||
					mutation.ExpectedValueDigest == (Digest{}) {
					t.Fatal("accepted invalid compare delete")
				}
			case MutationPutDigestEqual:
				if len(mutation.Value) == 0 || len(mutation.Value) > MaxMutationValueBytes ||
					len(mutation.Compare) != MutationDigestCompareBytes ||
					mutation.ExpectedValueLength == 0 ||
					mutation.ExpectedValueLength > MaxMutationValueBytes ||
					mutation.ExpectedValueDigest == (Digest{}) {
					t.Fatal("accepted invalid compare put")
				}
			default:
				t.Fatal("accepted unknown mutation kind")
			}
			if cap(iterator.b) != len(iterator.b) {
				t.Fatal("advanced mutation iterator is not capacity-clamped")
			}
			count++
			batchMutations++
		}
		if batchMutations != batch.MutationCount() {
			t.Fatal("relation batch count disagrees with iterator")
		}
		previous = batch.Relation
		batchCount++
	}
	if count != view.MutationCount() || batchCount != view.RelationCount() {
		t.Fatalf("iterated %d mutations, header declares %d", count, view.MutationCount())
	}
}

func FuzzOpenCompletion(f *testing.F) {
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
		view, err := OpenCompletion(data)
		if err != nil {
			return
		}
		assertFuzzCompletionView(t, data, view)
	})
}

func FuzzOpenCompletionResealedFields(f *testing.F) {
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
		view, err := OpenCompletion(candidate)
		if err == nil {
			assertFuzzCompletionView(t, candidate, view)
		}
	})
}

func assertFuzzCompletionView(t *testing.T, data []byte, view CompletionView) {
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
