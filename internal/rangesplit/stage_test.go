package rangesplit

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestChildStageResumesVerifiedArtifactAndValidatesExactDestination(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 2)
	expected := set.Children[1]
	stage, err := NewChildStage(partitioner, expected, collection, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	persist := func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}
	headerBytes := int(binary.LittleEndian.Uint32(artifact[12:16]))
	firstChunkBytes := int(binary.LittleEndian.Uint32(artifact[headerBytes+12 : headerBytes+16]))
	cut := headerBytes + firstChunkBytes
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact[:cut]), persist); err == nil {
		t.Fatal("truncated artifact accepted")
	}
	if len(persisted) != childStageCursorBytes {
		t.Fatalf("persisted cursor bytes = %d", len(persisted))
	}
	cursor, err := OpenChildStageCursor(persisted)
	if err != nil {
		t.Fatal(err)
	}
	sequence, rows, _, offset, _ := cursor.ArtifactProgress()
	if cursor.Phase() != ChildStageArtifact || sequence != 1 || rows != 1 || offset != uint64(cut) {
		t.Fatalf("artifact cursor = %+v", cursor)
	}

	reopened, err := NewChildStage(partitioner, expected, collection, persisted)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := reopened.ReceiveArtifact(bytes.NewReader(artifact), persist)
	if err != nil {
		t.Fatal(err)
	}
	if !equalChildArtifactManifest(manifest, expected) || !reopened.ArtifactComplete() ||
		collection.Len() != expected.Rows {
		t.Fatalf("manifest=%+v complete=%v rows=%d", manifest, reopened.ArtifactComplete(), collection.Len())
	}
	tail, ok := reopened.Cursor()
	if !ok || tail.Phase() != ChildStageTail || tail.SourceCut() != expected.Source ||
		tail.ArtifactDigest() != expected.Digest {
		t.Fatalf("tail cursor = %+v ok=%v", tail, ok)
	}
	persists := 0
	if _, err := reopened.ReceiveArtifact(bytes.NewReader(artifact), func([]byte) error {
		persists++
		return nil
	}); err != nil || persists != 0 {
		t.Fatalf("completed retry persists=%d error=%v", persists, err)
	}
}

func TestChildStageRefusesDestinationThatDoesNotMatchArtifact(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 2)
	wrong := documentForChild(t, partitioner, 1)
	if _, err := collection.Put([]byte("foreign"), wrong); err != nil {
		t.Fatal(err)
	}
	stage, err := NewChildStage(partitioner, set.Children[1], collection, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	_, err = stage.ReceiveArtifact(bytes.NewReader(artifact), func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	})
	if !errors.Is(err, ErrChildStage) || stage.ArtifactComplete() {
		t.Fatalf("error=%v complete=%v", err, stage.ArtifactComplete())
	}
	if len(persisted) == 0 {
		t.Fatal("verified artifact prefix was not checkpointed")
	}
}

func TestChildStageRequiresSynchronousDestinationDurability(t *testing.T) {
	partitioner := testChildArtifactPartitioner(t)
	document := documentForChild(t, partitioner, 1)
	_, set, _ := writeChildArtifactRows(
		t, partitioner, testSourceState(testSplitPlan(t, "node-b")),
		[]childArtifactTestRow{{key: []byte("row"), value: document}},
	)
	db, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	collection, err := db.CreateCollection("volatile", durable.Options{
		Durability: durable.DurabilityBufferedVisible, MaxBatchDocuments: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewChildStage(partitioner, set.Children[1], collection, nil); !errors.Is(err, ErrChildStage) {
		t.Fatalf("buffered destination error = %v", err)
	}
}

func TestChildStageAcceptsEmptyCertifiedArtifact(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 0)
	expected := set.Children[1]
	if expected.Chunks != 0 || expected.Rows != 0 {
		t.Fatalf("expected non-empty manifest: %+v", expected)
	}
	stage, err := NewChildStage(partitioner, expected, collection, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact), func(raw []byte) error {
		persisted = append(persisted[:0], raw...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cursor, err := OpenChildStageCursor(persisted)
	if err != nil || cursor.Phase() != ChildStageTail || collection.Len() != 0 {
		t.Fatalf("cursor=%+v rows=%d error=%v", cursor, collection.Len(), err)
	}
}

func TestChildStageTailApplyRetriesAfterCursorOutcomeUnknown(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 1)
	expected := set.Children[1]
	stage, err := NewChildStage(partitioner, expected, collection, nil)
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
	source, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	before := childArtifactDocumentPayload(documentForChild(t, partitioner, 1), 3<<10)
	after := documentForChild(t, partitioner, 0)
	entry := nextTailEntry(source, []TailTransition{{
		Key: []byte("row-0"), Before: before, After: after,
	}}, 21)
	failed := errors.New("cursor replace failed")
	failPersist := true
	var captured TailBatch
	sinks := []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error {
			captured = batch
			return stage.ApplyTailBatch(batch, func(raw []byte) error {
				cursor, err := OpenChildStageCursor(raw)
				if err != nil {
					return err
				}
				if failPersist && cursor.pendingBatchDigest == ([32]byte{}) {
					return failed
				}
				return persist(raw)
			})
		},
	}
	var workspace TailWorkspace
	next, _, err := partitioner.TranslateTailEntry(source, entry, sinks, &workspace)
	if !errors.Is(err, ErrChildStageOutcomeUnknown) || next != source || collection.Len() != 0 {
		t.Fatalf("next=%+v error=%v rows=%d", next.SourceCut(), err, collection.Len())
	}
	stageCursor, _ := stage.Cursor()
	if stageCursor.SourceCut() != expected.Source {
		t.Fatalf("failed persist advanced cursor: %+v", stageCursor.SourceCut())
	}

	failPersist = false
	next, _, err = partitioner.TranslateTailEntry(source, entry, sinks, &workspace)
	if err != nil || next.SourceCut().Applied != entry.Applied || collection.Len() != 0 {
		t.Fatalf("retry next=%+v error=%v rows=%d", next.SourceCut(), err, collection.Len())
	}
	if err := stage.ApplyTailBatch(captured, persist); err != nil {
		t.Fatalf("exact batch retry: %v", err)
	}
	stageCursor, _ = stage.Cursor()
	if stageCursor.SourceCut() != next.SourceCut() ||
		stageCursor.LastBatchDigest() != captured.Digest {
		t.Fatalf("stage cursor=%+v source=%+v", stageCursor, next.SourceCut())
	}

	reopened, err := NewChildStage(partitioner, expected, collection, persisted)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.ApplyTailBatch(captured, persist); err != nil {
		t.Fatalf("reopened exact retry: %v", err)
	}
}

func TestChildStagePersistsTailAdvanceWithNoChildOperations(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 1)
	expected := set.Children[1]
	stage, err := NewChildStage(partitioner, expected, collection, nil)
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
	source, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	entry := nextTailEntry(source, []TailTransition{{
		Key: []byte("left-only"), After: documentForChild(t, partitioner, 0),
	}}, 23)
	sinks := []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error { return stage.ApplyTailBatch(batch, persist) },
	}
	var workspace TailWorkspace
	next, _, err := partitioner.TranslateTailEntry(source, entry, sinks, &workspace)
	if err != nil || collection.Len() != expected.Rows {
		t.Fatalf("next=%+v error=%v rows=%d", next.SourceCut(), err, collection.Len())
	}
	cursor, _ := stage.Cursor()
	if cursor.SourceCut() != next.SourceCut() || cursor.LastBatchDigest() == ([32]byte{}) {
		t.Fatalf("cursor=%+v next=%+v", cursor, next.SourceCut())
	}
}

func TestChildStagePersistsTerminalOwnershipSeal(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 1)
	expected := set.Children[1]
	stage, err := NewChildStage(partitioner, expected, collection, nil)
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
	source, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	entry := nextTailEntry(source, nil, 31)
	entry.AfterDataChainDigest = entry.BeforeDataChainDigest
	entry.AfterOwnershipEpoch++
	entry.AfterRoutingVersion++
	entry.AfterRouteGeneration++
	var sealedBatch TailBatch
	sinks := []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error {
			sealedBatch = batch
			return stage.ApplyTailBatch(batch, persist)
		},
	}
	var workspace TailWorkspace
	sealed, _, err := partitioner.TranslateTailEntry(source, entry, sinks, &workspace)
	if err != nil || !sealed.Sealed() {
		t.Fatalf("sealed=%+v err=%v", sealed, err)
	}
	cursor, ok := stage.Cursor()
	if !ok || cursor.Phase() != ChildStageSealed ||
		cursor.SourceCut() != sealed.SourceCut() ||
		cursor.LastBatchDigest() != sealedBatch.Digest {
		t.Fatalf("cursor=%+v sealed=%+v", cursor, sealed)
	}
	imageRows, imageBytes, imageDigest, imageOK := cursor.ImageProof()
	if !imageOK || imageRows != collection.Len() || imageBytes == 0 ||
		imageDigest == ([32]byte{}) {
		t.Fatalf("image rows=%d bytes=%d digest=%x ok=%v", imageRows, imageBytes, imageDigest, imageOK)
	}
	if err := stage.ApplyTailBatch(sealedBatch, persist); err != nil {
		t.Fatalf("exact sealed retry: %v", err)
	}
	reopened, err := NewChildStage(partitioner, expected, collection, persisted)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.ApplyTailBatch(sealedBatch, persist); err != nil {
		t.Fatalf("reopened sealed retry: %v", err)
	}
	ordinary := nextTailEntry(source, nil, 32)
	ordinary.AfterDataChainDigest = ordinary.BeforeDataChainDigest
	var ordinaryBatch TailBatch
	if _, _, err := partitioner.TranslateTailEntry(source, ordinary, []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error { ordinaryBatch = batch; return nil },
	}, &workspace); err != nil {
		t.Fatal(err)
	}
	if err := reopened.ApplyTailBatch(ordinaryBatch, persist); !errors.Is(err, ErrChildStage) {
		t.Fatalf("post-seal batch error=%v", err)
	}
	if _, err := collection.Put(
		[]byte("foreign-after-seal"), documentForChild(t, partitioner, 1),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := NewChildStage(
		partitioner, expected, collection, persisted,
	); !errors.Is(err, ErrChildStage) {
		t.Fatalf("changed sealed image reopen error=%v", err)
	}
}

func TestChildStageSplitsLargeTailBatchIntoBoundedDurableUpdates(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 1)
	expected := set.Children[1]
	stage, err := NewChildStage(partitioner, expected, collection, nil)
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
	source, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	documents := documentsForChild(t, partitioner, 1, 2)
	entry := nextTailEntry(source, []TailTransition{
		{Key: []byte("a"), After: documents[0]},
		{Key: []byte("b"), After: documents[1]},
	}, 26)
	sinks := []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error { return stage.ApplyTailBatch(batch, persist) },
	}
	var workspace TailWorkspace
	if _, _, err := partitioner.TranslateTailEntry(source, entry, sinks, &workspace); err != nil {
		t.Fatal(err)
	}
	if collection.Len() != expected.Rows+2 {
		t.Fatalf("rows = %d, want %d", collection.Len(), expected.Rows+2)
	}
}

func TestChildStageCursorRejectsCorruptionAndTrailingBytes(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 1)
	stage, err := NewChildStage(partitioner, set.Children[1], collection, nil)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact), func(src []byte) error {
		raw = append(raw[:0], src...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(raw)
	corrupt[240] ^= 1
	for name, input := range map[string][]byte{
		"short": raw[:len(raw)-1], "trailing": append(bytes.Clone(raw), 0), "corrupt": corrupt,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := OpenChildStageCursor(input); !errors.Is(err, ErrChildStage) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestChildStageCursorStoreRecoversAndRejectsRegression(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 2)
	path := filepath.Join(t.TempDir(), "child.cursor")
	store, err := OpenChildStageCursorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(nil); err != nil {
		t.Fatal(err)
	}
	if conflict, err := OpenChildStageCursorStore(path); !errors.Is(err, ErrChildStage) {
		if conflict != nil {
			_ = conflict.Close()
		}
		t.Fatalf("conflicting writer error = %v", err)
	}
	stage, err := NewChildStage(partitioner, set.Children[1], collection, nil)
	if err != nil {
		t.Fatal(err)
	}
	headerBytes := int(binary.LittleEndian.Uint32(artifact[12:16]))
	firstChunkBytes := int(binary.LittleEndian.Uint32(artifact[headerBytes+12 : headerBytes+16]))
	if _, err := stage.ReceiveArtifact(
		bytes.NewReader(artifact[:headerBytes+firstChunkBytes]), store.Persist,
	); err == nil {
		t.Fatal("truncated artifact accepted")
	}
	prefix, ok, err := store.Load(nil)
	if err != nil || !ok {
		t.Fatalf("prefix load ok=%v error=%v", ok, err)
	}
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact), store.Persist); err != nil {
		t.Fatal(err)
	}
	complete, ok, err := store.Load(make([]byte, 0, childStageCursorBytes))
	if err != nil || !ok {
		t.Fatalf("complete load ok=%v error=%v", ok, err)
	}
	if err := store.Persist(complete); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	loadBuffer := make([]byte, 0, childStageCursorBytes)
	if allocs := testing.AllocsPerRun(1_000, func() {
		var loadErr error
		loadBuffer, ok, loadErr = store.Load(loadBuffer[:0])
		if loadErr != nil || !ok {
			panic("cursor load")
		}
	}); allocs != 0 {
		t.Fatalf("warm cursor load allocations = %v, want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(1_000, func() {
		if err := store.Persist(complete); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm exact cursor persist allocations = %v, want 0", allocs)
	}
	if err := store.Persist(prefix); !errors.Is(err, ErrChildStage) {
		t.Fatalf("regression error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenChildStageCursorStore(path)
	if err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := reopened.Load(nil)
	if err != nil || !ok || !bytes.Equal(loaded, complete) {
		t.Fatalf("recovered ok=%v equal=%v error=%v", ok, bytes.Equal(loaded, complete), err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{complete[len(complete)-1] ^ 1}, int64(len(complete)-1)); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if corrupt, err := OpenChildStageCursorStore(path); !errors.Is(err, ErrChildStage) {
		if corrupt != nil {
			_ = corrupt.Close()
		}
		t.Fatalf("corrupt cursor error = %v", err)
	}
}

func TestChildStageCursorStorePersistsTerminalSeal(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 0)
	store, err := OpenChildStageCursorStore(filepath.Join(t.TempDir(), "sealed.cursor"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	stage, err := NewChildStage(partitioner, set.Children[1], collection, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact), store.Persist); err != nil {
		t.Fatal(err)
	}
	cursor, err := partitioner.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	entry := nextTailEntry(cursor, nil, 33)
	entry.AfterDataChainDigest = entry.BeforeDataChainDigest
	entry.AfterOwnershipEpoch++
	entry.AfterRoutingVersion++
	entry.AfterRouteGeneration++
	var workspace TailWorkspace
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, []TailSink{
		func(TailBatch) error { return nil },
		func(batch TailBatch) error { return stage.ApplyTailBatch(batch, store.Persist) },
	}, &workspace); err != nil {
		t.Fatal(err)
	}
	raw, ok, err := store.Load(nil)
	if err != nil || !ok {
		t.Fatalf("load ok=%v err=%v", ok, err)
	}
	sealed, err := OpenChildStageCursor(raw)
	if err != nil {
		t.Fatal(err)
	}
	rows, bytesCount, imageDigest, imageOK := sealed.ImageProof()
	if sealed.Phase() != ChildStageSealed ||
		sealed.SourceCut().RouteGeneration != set.Partition.RouteGeneration+1 ||
		!imageOK || rows != 0 || bytesCount != 0 || imageDigest == ([32]byte{}) {
		t.Fatalf("sealed=%+v", sealed)
	}
	if err := store.Persist(raw); err != nil {
		t.Fatalf("sealed exact retry: %v", err)
	}
}

func TestAppendChildStageCursorAllocatesZeroWhenWarm(t *testing.T) {
	partitioner, artifact, set, collection := testChildStageArtifact(t, 1)
	stage, err := NewChildStage(partitioner, set.Children[1], collection, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stage.ReceiveArtifact(bytes.NewReader(artifact), func([]byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	cursor, _ := stage.Cursor()
	buffer := make([]byte, 0, childStageCursorBytes)
	var workspace ChildStageCursorWorkspace
	if _, err := AppendChildStageCursorWithWorkspace(buffer[:0], &cursor, &workspace); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1_000, func() {
		var err error
		buffer, err = AppendChildStageCursorWithWorkspace(buffer[:0], &cursor, &workspace)
		if err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm cursor encode allocations = %v, want 0", allocs)
	}
}

func TestVerifyTailBatchRejectsChangedDigestAndUntranslatedValue(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	entry := nextTailEntry(cursor, []TailTransition{{
		Key: []byte("move"), Before: documentForChild(t, partitioner, 0),
		After: documentForChild(t, partitioner, 1),
	}}, 22)
	var batches [2]TailBatch
	sinks := []TailSink{
		func(batch TailBatch) error { batches[0] = batch; return nil },
		func(batch TailBatch) error { batches[1] = batch; return nil },
	}
	var translate TailWorkspace
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &translate); err != nil {
		t.Fatal(err)
	}
	var verify TailBatchVerifyWorkspace
	for child := range batches {
		if err := partitioner.VerifyTailBatch(batches[child], &verify); err != nil {
			t.Fatalf("child %d valid batch: %v", child, err)
		}
	}
	changed := batches[1]
	changed.Digest[0] ^= 1
	if err := partitioner.VerifyTailBatch(changed, &verify); !errors.Is(err, ErrTailEntry) {
		t.Fatalf("changed digest error = %v", err)
	}
	untranslated := batches[1]
	untranslated.translated = false
	if err := partitioner.VerifyTailBatch(untranslated, &verify); !errors.Is(err, ErrTailEntry) {
		t.Fatalf("untranslated error = %v", err)
	}
}

func TestVerifyTailBatchAllocatesZeroWhenWarm(t *testing.T) {
	partitioner, cursor, _ := testTailCursor(t)
	entry := nextTailEntry(cursor, []TailTransition{{
		Key: []byte("move"), Before: documentForChild(t, partitioner, 0),
		After: documentForChild(t, partitioner, 1),
	}}, 24)
	var batch TailBatch
	sinks := []TailSink{
		func(TailBatch) error { return nil },
		func(value TailBatch) error { batch = value; return nil },
	}
	var translate TailWorkspace
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &translate); err != nil {
		t.Fatal(err)
	}
	var verify TailBatchVerifyWorkspace
	if err := partitioner.VerifyTailBatch(batch, &verify); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1_000, func() {
		if err := partitioner.VerifyTailBatch(batch, &verify); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warm tail verification allocations = %v, want 0", allocs)
	}
}

func BenchmarkVerifyTailBatch(b *testing.B) {
	partitioner, cursor, _ := testTailCursor(b)
	entry := nextTailEntry(cursor, []TailTransition{{
		Key: []byte("move"), Before: documentForChild(b, partitioner, 0),
		After: documentForChild(b, partitioner, 1),
	}}, 25)
	var batch TailBatch
	sinks := []TailSink{
		func(TailBatch) error { return nil },
		func(value TailBatch) error { batch = value; return nil },
	}
	var translate TailWorkspace
	if _, _, err := partitioner.TranslateTailEntry(cursor, entry, sinks, &translate); err != nil {
		b.Fatal(err)
	}
	var verify TailBatchVerifyWorkspace
	if err := partitioner.VerifyTailBatch(batch, &verify); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := partitioner.VerifyTailBatch(batch, &verify); err != nil {
			b.Fatal(err)
		}
	}
}

func testChildStageArtifact(
	t testing.TB,
	rowCount int,
) (*Partitioner, []byte, ChildArtifactSet, *durable.Collection) {
	t.Helper()
	partitioner := testChildArtifactPartitioner(t)
	documents := documentsForChild(t, partitioner, 1, rowCount)
	rows := make([]childArtifactTestRow, rowCount)
	for index := range rows {
		rows[index] = childArtifactTestRow{
			key:   []byte{'r', 'o', 'w', '-', byte('0' + index)},
			value: childArtifactDocumentPayload(documents[index], 3<<10),
		}
	}
	artifact, set, _ := writeChildArtifactRows(
		t, partitioner, testSourceState(testSplitPlan(t, "node-b")), rows,
	)
	db, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close child stage database: %v", err)
		}
	})
	collection, err := db.CreateCollection("child", durable.Options{MaxBatchDocuments: 1})
	if err != nil {
		t.Fatal(err)
	}
	return partitioner, artifact, set, collection
}
