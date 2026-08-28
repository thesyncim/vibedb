package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func TestChildStageImageAccumulatorIsOrderIndependentAndReversible(t *testing.T) {
	keyA, valueA := []byte("a"), []byte(`{"tenant":1,"value":"a"}`)
	keyB, valueB := []byte("b"), []byte(`{"tenant":1,"value":"b"}`)
	var leftWorkspace, rightWorkspace childStageImageWorkspace
	var left, right childStageImageAccumulator
	if err := left.addRow(&leftWorkspace, keyA, valueA); err != nil {
		t.Fatal(err)
	}
	if err := left.addRow(&leftWorkspace, keyB, valueB); err != nil {
		t.Fatal(err)
	}
	if err := right.addRow(&rightWorkspace, keyB, valueB); err != nil {
		t.Fatal(err)
	}
	if err := right.addRow(&rightWorkspace, keyA, valueA); err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatalf("order changed accumulator: left=%+v right=%+v", left, right)
	}
	if err := left.removeWitness(
		&leftWorkspace, keyA, uint32(len(valueA)), sha256.Sum256(valueA),
	); err != nil {
		t.Fatal(err)
	}
	var want childStageImageAccumulator
	if err := want.addRow(&rightWorkspace, keyB, valueB); err != nil {
		t.Fatal(err)
	}
	if left != want {
		t.Fatalf("remove did not restore exact image: got=%+v want=%+v", left, want)
	}
}

func TestChildStageSealDoesNotScanRows(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 2)
	stage, err := NewChildStage(partitioner, set.Children[1], collection, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	persist := func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact), persist); err != nil {
		t.Fatal(err)
	}
	visits := 0
	stage.image.visit = func(_, _ []byte) error { visits++; return nil }
	stage.image.bound = &stage.image.scan
	source, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	entry := nextTailEntry(source, nil, 73)
	entry.AfterDataChainDigest = entry.BeforeDataChainDigest
	entry.AfterOwnershipEpoch++
	entry.AfterRoutingVersion++
	entry.AfterRouteGeneration++
	var workspace TailWorkspace
	if _, _, err := partitioner.TranslateTailEntry(source, entry, []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error { return stage.ApplyTailBatch(batch, persist) },
	}, &workspace); err != nil {
		t.Fatal(err)
	}
	if visits != 0 {
		t.Fatalf("terminal seal scanned %d rows", visits)
	}
	if _, err := NewChildStage(partitioner, set.Children[1], collection, persisted); err != nil {
		t.Fatalf("recovery audit rejected incremental proof: %v", err)
	}
}

func TestChildStageImageAccumulatorUsesCompactBeforeWitness(t *testing.T) {
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
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact), persist); err != nil {
		t.Fatal(err)
	}
	before := childArtifactDocumentPayload(documentForChild(t, partitioner, 1), 3<<10)
	var pointWorkspace distribution.DocumentPointWorkspace
	point, err := partitioner.program.Point(before, &pointWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	source, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	entry := nextTailEntry(source, []TailTransition{{
		Key: []byte("row-0"),
		BeforeWitness: TailBeforeWitness{
			Present: true, Point: point, DocumentBytes: uint32(len(before)),
			Digest: sha256.Sum256(before),
		},
		After: documentForChild(t, partitioner, 0),
	}}, 74)
	var workspace TailWorkspace
	next, _, err := partitioner.TranslateTailEntry(source, entry, []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error { return stage.ApplyTailBatch(batch, persist) },
	}, &workspace)
	if err != nil || collection.Len() != 0 {
		t.Fatalf("witnessed delete: rows=%d error=%v", collection.Len(), err)
	}
	seal := nextTailEntry(next, nil, 75)
	seal.AfterDataChainDigest = seal.BeforeDataChainDigest
	seal.AfterOwnershipEpoch++
	seal.AfterRoutingVersion++
	seal.AfterRouteGeneration++
	if _, _, err := partitioner.TranslateTailEntry(next, seal, []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error { return stage.ApplyTailBatch(batch, persist) },
	}, &workspace); err != nil {
		t.Fatal(err)
	}
	cursor, ok := stage.Cursor()
	rows, bytesCount, _, imageOK := cursor.ImageProof()
	if !ok || !imageOK || rows != 0 || bytesCount != 0 {
		t.Fatalf("empty image proof: cursor=%+v rows=%d bytes=%d ok=%v", cursor, rows, bytesCount, imageOK)
	}
	if _, err := NewChildStage(partitioner, set.Children[1], collection, persisted); err != nil {
		t.Fatalf("empty recovery audit: %v", err)
	}
}
