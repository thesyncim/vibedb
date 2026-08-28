package rangesplit

import (
	"bytes"
	"errors"
	"testing"
)

func TestTailStreamRequestResponseRoundTripBindsDurableAdvance(t *testing.T) {
	partitioner, set, stage, before, batch, persist := testTailStreamFixture(t)
	operation := [32]byte{91}
	binding, err := NewTailStreamBinding(operation, set.Children[1])
	if err != nil {
		t.Fatal(err)
	}
	request := TailStreamRequest{Binding: binding, Before: before, Batch: batch}
	var codec TailStreamCodecWorkspace
	raw, err := AppendTailStreamRequestWithWorkspace(nil, request, &codec)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenTailStreamRequestWithWorkspace(raw, &codec)
	if err != nil {
		t.Fatal(err)
	}
	if err = partitioner.ValidateTailStreamRequest(
		operation, set.Children[1], opened, &TailBatchVerifyWorkspace{},
	); err != nil {
		t.Fatal(err)
	}
	canonical, err := AppendTailStreamRequestWithWorkspace(nil, opened, &codec)
	if err != nil || !bytes.Equal(canonical, raw) {
		t.Fatalf("canonical error=%v equal=%v", err, bytes.Equal(canonical, raw))
	}
	if err = stage.ApplyTailBatch(opened.Batch, persist); err != nil {
		t.Fatal(err)
	}
	after, ok := stage.Cursor()
	if !ok {
		t.Fatal("missing durable after cursor")
	}
	response := TailStreamResponse{
		Binding: binding, RequestDigest: opened.Digest(),
		BatchDigest: opened.Batch.Digest, Cursor: after,
	}
	responseRaw, err := AppendTailStreamResponseWithWorkspace(nil, response, &codec)
	if err != nil {
		t.Fatal(err)
	}
	openedResponse, err := OpenTailStreamResponseWithWorkspace(responseRaw, &codec)
	if err != nil {
		t.Fatal(err)
	}
	if err = ValidateTailStreamResponse(opened, openedResponse); err != nil {
		t.Fatal(err)
	}
	// The request cursor deliberately remains the pre-apply durable cursor.
	// Re-encoding after apply is therefore byte-identical for outcome-unknown retry.
	retry, err := AppendTailStreamRequestWithWorkspace(nil, opened, &codec)
	if err != nil || !bytes.Equal(retry, raw) {
		t.Fatalf("retry error=%v equal=%v", err, bytes.Equal(retry, raw))
	}
}

func TestTailStreamRequestRejectsEveryBindingAndFramePerturbation(t *testing.T) {
	partitioner, set, _, before, batch, _ := testTailStreamFixture(t)
	operation := [32]byte{92}
	binding, err := NewTailStreamBinding(operation, set.Children[1])
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendTailStreamRequest(nil, TailStreamRequest{
		Binding: binding, Before: before, Batch: batch,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{
		0, 8, 10, 12, 16, 20, 24, 28,
		tailStreamFrameHeaderBytes,
		tailStreamFrameHeaderBytes + 32,
		tailStreamFrameHeaderBytes + 64,
		tailStreamFrameHeaderBytes + 96,
		tailStreamFrameHeaderBytes + 128,
		tailStreamFrameHeaderBytes + 224,
		tailStreamFrameHeaderBytes + 248,
		tailStreamFrameHeaderBytes + tailStreamBindingBytes,
		len(raw) - 1,
	} {
		changed := bytes.Clone(raw)
		changed[offset] ^= 1
		if _, openErr := OpenTailStreamRequest(changed); !errors.Is(openErr, ErrTailStream) {
			t.Fatalf("offset %d error=%v", offset, openErr)
		}
	}
	for index, input := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, openErr := OpenTailStreamRequest(input); !errors.Is(openErr, ErrTailStream) {
			t.Fatalf("length case %d error=%v", index, openErr)
		}
	}
	opened, err := OpenTailStreamRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*TailStreamRequest){
		"operation": func(request *TailStreamRequest) { request.Binding.Operation[1] ^= 1 },
		"plan":      func(request *TailStreamRequest) { request.Binding.PlanDigest[1] ^= 1 },
		"placement": func(request *TailStreamRequest) { request.Binding.PlacementDigest[1] ^= 1 },
		"artifact":  func(request *TailStreamRequest) { request.Binding.ArtifactDigest[1] ^= 1 },
		"source":    func(request *TailStreamRequest) { request.Binding.Source.Applied++ },
		"child":     func(request *TailStreamRequest) { request.Binding.Child = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := opened
			mutate(&changed)
			if validateErr := partitioner.ValidateTailStreamRequest(
				operation, set.Children[1], changed, &TailBatchVerifyWorkspace{},
			); !errors.Is(validateErr, ErrTailStream) {
				t.Fatalf("error=%v", validateErr)
			}
		})
	}
}

func testTailStreamFixture(t testing.TB) (
	*Partitioner,
	ChildArtifactSet,
	*ChildStage,
	ChildStageCursor,
	TailBatch,
	ChildStageCursorPersistence,
) {
	t.Helper()
	partitioner, artifact, set, collection := testChildStageArtifact(t, 1)
	stage, err := NewChildStage(partitioner, set.Children[1], collection, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	persist := func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}
	if _, err = stage.ReceiveArtifact(bytes.NewReader(artifact), persist); err != nil {
		t.Fatal(err)
	}
	before, ok := stage.Cursor()
	if !ok {
		t.Fatal("missing tail cursor")
	}
	source, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	document := documentsForChild(t, partitioner, 1, 1)[0]
	entry := nextTailEntry(source, []TailTransition{{Key: []byte("tail-row"), After: document}}, 93)
	var batch TailBatch
	if _, _, err = partitioner.TranslateTailEntry(source, entry, []TailSink{
		func(TailBatch) error { return nil },
		func(value TailBatch) error { batch = value; return nil },
	}, &TailWorkspace{}); err != nil {
		t.Fatal(err)
	}
	return partitioner, set, stage, before, batch, persist
}
