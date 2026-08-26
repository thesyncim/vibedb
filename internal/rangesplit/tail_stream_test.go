package rangesplit

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func TestTailBatchWireRoundTripIsCanonicalAndVerifiable(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	left := documentsForChild(t, partitioner, 0, 2)
	right := documentsForChild(t, partitioner, 1, 2)
	entry := nextTailEntry(cursor, []TailTransition{
		{Key: []byte("a"), After: left[0]},
		{Key: []byte("b"), After: right[0]},
		{Key: []byte("c"), Before: left[0], After: right[1]},
		{Key: []byte("d"), Before: right[0]},
	}, 31)
	var batches [2]TailBatch
	var translate TailWorkspace
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, []TailSink{
		func(batch TailBatch) error { batches[0] = batch; return nil },
		func(batch TailBatch) error { batches[1] = batch; return nil },
	}, &translate); err != nil {
		t.Fatal(err)
	}
	for child := range batches {
		want := appendTailBatch(nil, batches[child])
		var codec TailBatchCodecWorkspace
		raw, err := AppendTailBatchWithWorkspace(nil, batches[child], &codec)
		if err != nil {
			t.Fatalf("encode child %d: %v", child, err)
		}
		opened, err := OpenTailBatchWithWorkspace(raw, &codec)
		if err != nil {
			t.Fatalf("open child %d: %v", child, err)
		}
		if err = partitioner.VerifyTailBatch(opened, &TailBatchVerifyWorkspace{}); err != nil {
			t.Fatalf("verify child %d: %v", child, err)
		}
		assertTailOperations(t, appendTailBatch(nil, opened), want)
		canonical, err := AppendTailBatchWithWorkspace(nil, opened, &codec)
		if err != nil || !bytes.Equal(canonical, raw) {
			t.Fatalf("canonical child %d: error=%v equal=%v", child, err, bytes.Equal(canonical, raw))
		}
	}
}

func TestTailBatchWireSupportsAuthenticatedEmptyAdvance(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	entry := nextTailEntry(cursor, nil, 32)
	entry.AfterDataChainDigest = entry.BeforeDataChainDigest
	var batch TailBatch
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, []TailSink{
		func(value TailBatch) error { batch = value; return nil },
		func(TailBatch) error { return nil },
	}, &TailWorkspace{}); err != nil {
		t.Fatal(err)
	}
	raw, err := AppendTailBatch(nil, batch)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenTailBatch(raw)
	iterator := opened.Iterator()
	if err != nil || opened.Operations != 0 || iterator.Next() {
		t.Fatalf("opened=%+v error=%v", opened, err)
	}
	if err = partitioner.VerifyTailBatch(opened, &TailBatchVerifyWorkspace{}); err != nil {
		t.Fatal(err)
	}
}

func TestTailBatchWireRejectsBoundsCorruptionAndTrailingBytes(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	document := documentsForChild(t, partitioner, 1, 1)[0]
	entry := nextTailEntry(cursor, []TailTransition{{Key: []byte("a"), After: document}}, 33)
	var batch TailBatch
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, []TailSink{
		func(TailBatch) error { return nil },
		func(value TailBatch) error { batch = value; return nil },
	}, &TailWorkspace{}); err != nil {
		t.Fatal(err)
	}
	raw, err := AppendTailBatch(nil, batch)
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		nil,
		raw[:len(raw)-1],
		append(bytes.Clone(raw), 0),
	}
	for _, offset := range []int{0, 8, 10, 12, 17, 432, 436, tailBatchWireHeaderBytes, len(raw) - 1} {
		changed := bytes.Clone(raw)
		changed[offset] ^= 1
		cases = append(cases, changed)
	}
	for index, input := range cases {
		if _, openErr := OpenTailBatch(input); !errors.Is(openErr, ErrTailBatchWire) {
			t.Fatalf("case %d error=%v", index, openErr)
		}
	}
	for name, offset := range map[string]int{
		"operation-count":    24 + 3*8,
		"operation-reserved": tailBatchWireHeaderBytes + 1,
	} {
		changed := bytes.Clone(raw)
		changed[offset] ^= 1
		workspace := &TailBatchCodecWorkspace{}
		tailBatchWireDigest(workspace, changed[:len(changed)-32])
		copy(changed[len(changed)-32:], workspace.digest[:])
		if _, openErr := OpenTailBatch(changed); !errors.Is(openErr, ErrTailBatchWire) {
			t.Fatalf("semantic %s error=%v", name, openErr)
		}
	}
	tooLarge := make([]byte, MaxTailBatchWireBytes+1)
	if _, openErr := OpenTailBatch(tooLarge); !errors.Is(openErr, ErrTailBatchWire) {
		t.Fatalf("oversize error=%v", openErr)
	}
}

func TestTailBatchWireWarmedAllocationsDoNotGrowWithOperations(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	documents := documentsForChild(t, partitioner, 1, 1)
	transitions := make([]TailTransition, 4097)
	for index := range transitions {
		transitions[index] = TailTransition{
			Key:   []byte{byte(index >> 24), byte(index >> 16), byte(index >> 8), byte(index)},
			After: documents[0],
		}
	}
	entry := nextTailEntry(cursor, transitions, 34)
	var batch TailBatch
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, []TailSink{
		func(TailBatch) error { return nil },
		func(value TailBatch) error { batch = value; return nil },
	}, &TailWorkspace{}); err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 0, MaxTailBatchWireBytes)
	workspace := &TailBatchCodecWorkspace{}
	encoded, err := AppendTailBatchWithWorkspace(raw[:0], batch, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenTailBatchWithWorkspace(encoded, workspace); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(20, func() {
		encoded, err = AppendTailBatchWithWorkspace(raw[:0], batch, workspace)
		if err != nil {
			panic(err)
		}
		opened, openErr := OpenTailBatchWithWorkspace(encoded, workspace)
		if openErr != nil || opened.Operations != batch.Operations {
			panic(openErr)
		}
	})
	if !controlRaceEnabled && allocations != 0 {
		t.Fatalf("allocations=%f, want 0", allocations)
	}
}

func TestTailBatchWireExactMaximumAndPlusOne(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	base := documentsForChild(t, partitioner, 1, 1)[0]
	payloadBytes := 252 - len(base) - 13
	if payloadBytes < 0 {
		t.Fatalf("base document is unexpectedly large: %d", len(base))
	}
	document := childArtifactDocumentPayload(base, payloadBytes)
	if len(document) != 252 {
		t.Fatalf("document bytes=%d want=252", len(document))
	}
	transitions := make([]TailTransition, replication.MaxMutations)
	for index := range transitions {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(index))
		transitions[index] = TailTransition{Key: key, After: document}
	}
	entry := nextTailEntry(cursor, transitions, 35)
	var batch TailBatch
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, []TailSink{
		func(TailBatch) error { return nil },
		func(value TailBatch) error { batch = value; return nil },
	}, &TailWorkspace{}); err != nil {
		t.Fatal(err)
	}
	if batch.Operations != replication.MaxMutations || batch.Bytes != replication.MaxCommandBytes {
		t.Fatalf("operations=%d bytes=%d", batch.Operations, batch.Bytes)
	}
	raw, err := AppendTailBatch(make([]byte, 0, MaxTailBatchWireBytes), batch)
	if err != nil || len(raw) != MaxTailBatchWireBytes {
		t.Fatalf("bytes=%d want=%d error=%v", len(raw), MaxTailBatchWireBytes, err)
	}
	if _, err = OpenTailBatch(raw); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenTailBatch(append(raw, 0)); !errors.Is(err, ErrTailBatchWire) {
		t.Fatalf("plus one error=%v", err)
	}
}

func BenchmarkTailBatchWire4097(b *testing.B) {
	partitioner, cursor, _ := testTailCursor(b)
	document := documentsForChild(b, partitioner, 1, 1)[0]
	transitions := make([]TailTransition, 4097)
	for index := range transitions {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(index))
		transitions[index] = TailTransition{Key: key, After: document}
	}
	entry := nextTailEntry(cursor, transitions, 36)
	var batch TailBatch
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, []TailSink{
		func(TailBatch) error { return nil },
		func(value TailBatch) error { batch = value; return nil },
	}, &TailWorkspace{}); err != nil {
		b.Fatal(err)
	}
	workspace := &TailBatchCodecWorkspace{}
	raw := make([]byte, 0, MaxTailBatchWireBytes)
	encoded, err := AppendTailBatchWithWorkspace(raw[:0], batch, workspace)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(encoded)))
		for range b.N {
			if _, err = AppendTailBatchWithWorkspace(raw[:0], batch, workspace); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("authenticate-open-verify", func(b *testing.B) {
		verify := &TailBatchVerifyWorkspace{}
		opened, openErr := OpenTailBatchWithWorkspace(encoded, workspace)
		if openErr != nil || partitioner.VerifyTailBatch(opened, verify) != nil {
			b.Fatal(openErr)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(encoded)))
		b.ResetTimer()
		for range b.N {
			opened, openErr = OpenTailBatchWithWorkspace(encoded, workspace)
			if openErr != nil || partitioner.VerifyTailBatch(opened, verify) != nil {
				b.Fatal(openErr)
			}
		}
	})
}
