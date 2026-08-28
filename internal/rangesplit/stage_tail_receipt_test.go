package rangesplit

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func witnessedStageFixture(t *testing.T) (*ChildStage, TailBatch, TailCursor, *ChildStageCursorStore) {
	t.Helper()
	p, artifact, set, collection := testChildStageArtifact(t, 3)
	store, err := OpenChildStageCursorStore(filepath.Join(t.TempDir(), "child.cursor"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	stage, err := NewChildStage(p, set.Children[1], collection, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stage.ReceiveArtifact(bytes.NewReader(artifact), store.Persist); err != nil {
		t.Fatal(err)
	}
	source, err := p.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	docs := documentsForChild(t, p, 1, 4)
	afterNew, err := vibejson.Canonicalize(docs[3])
	if err != nil {
		t.Fatal(err)
	}
	afterReplace, err := vibejson.Canonicalize(docs[1])
	if err != nil {
		t.Fatal(err)
	}
	entry := nextTailEntry(source, []TailTransition{
		{Key: []byte("new"), After: afterNew},
		{Key: []byte("row-0"), Before: childArtifactDocumentPayload(docs[0], 3<<10), After: afterReplace},
		{Key: []byte("row-1"), Before: childArtifactDocumentPayload(docs[1], 3<<10)},
		{Key: []byte("row-2"), Before: childArtifactDocumentPayload(docs[2], 3<<10), After: documentForChild(t, p, 0)},
	}, 114)
	var batch TailBatch
	next, _, err := p.TranslateTailEntry(source, entry, []TailSink{
		func(TailBatch) error { return nil },
		func(value TailBatch) error { batch = value; return nil },
	}, &TailWorkspace{})
	if err != nil {
		t.Fatal(err)
	}
	return stage, batch, next, store
}

func wireWitnessedBatch(t *testing.T, batch TailBatch) TailBatch {
	t.Helper()
	raw, err := AppendTailBatch(nil, batch)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenTailBatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	return opened
}

func reopenWitnessedStage(t *testing.T, stage *ChildStage, store *ChildStageCursorStore) *ChildStage {
	t.Helper()
	raw, ok, err := store.Load(nil)
	if err != nil || !ok {
		t.Fatalf("load present=%v error=%v", ok, err)
	}
	reopened, err := NewChildStage(stage.partitioner, stage.expected, stage.collection, raw)
	if err != nil {
		t.Fatal(err)
	}
	return reopened
}

func TestChildStageWitnessedLocalWireSealAndReopenAreIdentical(t *testing.T) {
	var want ChildStageCursor
	var wantSealed ChildStageCursor
	for _, remote := range []bool{false, true} {
		stage, batch, source, store := witnessedStageFixture(t)
		if remote {
			batch = wireWitnessedBatch(t, batch)
		}
		if err := stage.ApplyTailBatch(batch, store.Persist); err != nil {
			t.Fatal(err)
		}
		cursor, _ := stage.Cursor()
		if cursor.imageRows != 2 || stage.collection.Len() != 2 || cursor.pendingBatchDigest != ([32]byte{}) {
			t.Fatalf("bad completed cursor %+v", cursor)
		}
		if !remote {
			want = cursor
		} else if cursor != want {
			t.Fatal("local/wire image roots or cursor differ")
		}
		stage = reopenWitnessedStage(t, stage, store)
		if err := stage.ApplyTailBatch(batch, store.Persist); err != nil {
			t.Fatalf("durable exact retry: %v", err)
		}
		seal := nextTailEntry(source, nil, 115)
		seal.AfterDataChainDigest = seal.BeforeDataChainDigest
		seal.AfterOwnershipEpoch++
		seal.AfterRoutingVersion++
		seal.AfterRouteGeneration++
		_, _, err := stage.partitioner.TranslateTailEntry(source, seal, []TailSink{
			func(TailBatch) error { return nil },
			func(value TailBatch) error {
				if remote {
					value = wireWitnessedBatch(t, value)
				}
				return stage.ApplyTailBatch(value, store.Persist)
			},
		}, &TailWorkspace{})
		if err != nil {
			t.Fatal(err)
		}
		stage = reopenWitnessedStage(t, stage, store)
		sealed, _ := stage.Cursor()
		rows, _, _, valid := sealed.ImageProof()
		if !valid || rows != 2 {
			t.Fatal("sealed recovery audit failed")
		}
		if !remote {
			wantSealed = sealed
		} else if sealed != wantSealed {
			t.Fatal("local/wire sealed image roots or cursor differ")
		}
	}
}

func TestChildStageReceiptSaveUncertaintyRequiresReloadBeforeWrites(t *testing.T) {
	for _, saved := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "saved"}[saved], func(t *testing.T) {
			stage, batch, _, store := witnessedStageFixture(t)
			batch = wireWitnessedBatch(t, batch)
			before, _ := stage.Cursor()
			fault := errors.New("receipt sync outcome unknown")
			calls := 0
			err := stage.ApplyTailBatch(batch, func(raw []byte) error {
				calls++
				if saved {
					if err := store.Persist(raw); err != nil {
						t.Fatal(err)
					}
				}
				return fault
			})
			if !errors.Is(err, ErrChildStageOutcomeUnknown) || calls != 1 || stage.collection.Len() != 3 {
				t.Fatalf("receipt error=%v calls=%d", err, calls)
			}
			if err = stage.ApplyTailBatch(batch, store.Persist); !errors.Is(err, ErrChildStageOutcomeUnknown) || stage.collection.Len() != 3 {
				t.Fatal("uncertain receipt permitted writes")
			}
			stage = reopenWitnessedStage(t, stage, store)
			current, _ := stage.Cursor()
			if saved != current.ResumesTailBatch(before, batch) {
				t.Fatal("receipt did not bind exact prior cursor and batch")
			}
			foreign := batch
			foreign.Digest[0] ^= 1
			if current.ResumesTailBatch(before, foreign) {
				t.Fatal("foreign batch matched receipt")
			}
			altered := before
			altered.imageBytes++
			if current.ResumesTailBatch(altered, batch) {
				t.Fatal("altered prior image matched receipt")
			}
			if err = stage.ApplyTailBatch(batch, store.Persist); err != nil {
				t.Fatal(err)
			}
			if stage.collection.Len() != 2 {
				t.Fatal("retry cardinality")
			}
		})
	}
}

func TestChildStageReceiptRecoversEveryDurableMutationPrefix(t *testing.T) {
	for prefix := 0; prefix <= 4; prefix++ {
		stage, batch, _, store := witnessedStageFixture(t)
		batch = wireWitnessedBatch(t, batch)
		// Cut after the durable receipt but before ApplyTailBatch can write.
		err := stage.ApplyTailBatch(batch, func(raw []byte) error {
			if err := store.Persist(raw); err != nil {
				return err
			}
			return errors.New("process stopped after receipt")
		})
		if !errors.Is(err, ErrChildStageOutcomeUnknown) {
			t.Fatal(err)
		}
		iterator := batch.Iterator()
		for i := 0; i < prefix; i++ {
			if !iterator.Next() {
				t.Fatal("missing operation")
			}
			op := iterator.Operation()
			if err := stage.collection.Update(func(write *durable.WriteBatch) error {
				if op.Kind == replication.MutationDelete {
					return write.Delete(op.Key)
				}
				return write.Put(op.Key, op.Value)
			}); err != nil {
				t.Fatal(err)
			}
			if op.Kind == replication.MutationPut {
				got, _, _ := stage.collection.AppendRaw(nil, op.Key)
				if !bytes.Equal(got, op.Value) {
					t.Fatalf("stored=%s wanted=%s", got, op.Value)
				}
			}
		}
		// These real synchronous store writes are exactly the possible durable
		// chunk prefixes (the fixture has MaxBatchDocuments=1).
		stage = reopenWitnessedStage(t, stage, store)
		if err := stage.ApplyTailBatch(batch, store.Persist); err != nil {
			t.Fatalf("prefix %d: %v", prefix, err)
		}
		cursor, _ := stage.Cursor()
		if cursor.imageRows != 2 || stage.collection.Len() != 2 {
			t.Fatalf("prefix %d wrong image", prefix)
		}
	}
}

func TestChildStageRejectsUnreceiptedAfterAndForgedPreimage(t *testing.T) {
	for _, withReceipt := range []bool{false, true} {
		stage, batch, _, store := witnessedStageFixture(t)
		batch = wireWitnessedBatch(t, batch)
		if withReceipt {
			_ = stage.ApplyTailBatch(batch, func(raw []byte) error {
				if err := store.Persist(raw); err != nil {
					return err
				}
				return errors.New("cut")
			})
			stage = reopenWitnessedStage(t, stage, store)
		}
		iterator := batch.Iterator()
		iterator.Next()
		op := iterator.Operation()
		value := op.Value
		if withReceipt {
			value = childArtifactDocumentPayload(value, 100)
		}
		if err := stage.collection.Update(func(write *durable.WriteBatch) error { return write.Put(op.Key, value) }); err != nil {
			t.Fatal(err)
		}
		before, _ := stage.Cursor()
		if err := stage.ApplyTailBatch(batch, store.Persist); !errors.Is(err, ErrChildStage) {
			t.Fatalf("forged preimage accepted: %v", err)
		}
		after, _ := stage.Cursor()
		if after != before || stage.collection.Len() != 4 {
			t.Fatal("preflight rejection mutated rows or cursor")
		}
	}
}

func TestChildStageReceiptCursorGrammarAndAdvance(t *testing.T) {
	stage, batch, _, _ := witnessedStageFixture(t)
	before, _ := stage.Cursor()
	receipt := before
	receipt.pendingBatchDigest = batch.Digest
	if !validChildStageCursorAdvance(before, receipt) {
		t.Fatal("valid receipt rejected")
	}
	for _, mutate := range []func(*ChildStageCursor){
		func(c *ChildStageCursor) { c.imageBytes++ },
		func(c *ChildStageCursor) { c.imageDigest[0] ^= 1 },
		func(c *ChildStageCursor) { c.entryDigest[0] ^= 1 },
		func(c *ChildStageCursor) { c.routeGeneration++ },
	} {
		changed := receipt
		mutate(&changed)
		if validChildStageCursorAdvance(before, changed) {
			t.Fatal("receipt changed prior state")
		}
	}
	raw, err := AppendChildStageCursor(nil, &receipt)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenChildStageCursor(raw)
	if err != nil || *opened != receipt {
		t.Fatal("receipt roundtrip")
	}
	for _, input := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, err := OpenChildStageCursor(input); err == nil {
			t.Fatal("noncanonical receipt length")
		}
	}
	raw[416] ^= 1
	if _, err := OpenChildStageCursor(raw); err == nil {
		t.Fatal("receipt corruption")
	}
	sealed := receipt
	sealed.phase = ChildStageSealed
	if _, err := AppendChildStageCursor(nil, &sealed); err == nil {
		t.Fatal("pending receipt sealed")
	}
}

func TestChildStageRejectsNoncanonicalAfterBeforeReceipt(t *testing.T) {
	p, set, stage, _, _, _ := testTailStreamFixture(t)
	source, err := p.InitialTailCursor(set)
	if err != nil {
		t.Fatal(err)
	}
	canonical := documentForChild(t, p, 1)
	noncanonical := append([]byte("{ "), canonical[1:]...)
	entry := nextTailEntry(source, []TailTransition{{Key: []byte("new"), After: noncanonical}}, 121)
	calls := 0
	_, _, err = p.TranslateTailEntry(source, entry, []TailSink{func(TailBatch) error { return nil }, func(batch TailBatch) error {
		return stage.ApplyTailBatch(wireWitnessedBatch(t, batch), func([]byte) error { calls++; return nil })
	}}, &TailWorkspace{})
	if !errors.Is(err, ErrChildStage) || calls != 0 || stage.collection.Len() != 1 {
		t.Fatalf("noncanonical after: error=%v persists=%d", err, calls)
	}
}

func TestChildStageReceiptFinalCursorFailureRecoversBeforeOrAfterSave(t *testing.T) {
	for _, saved := range []bool{false, true} {
		stage, batch, _, store := witnessedStageFixture(t)
		batch = wireWitnessedBatch(t, batch)
		fault := errors.New("final cursor outcome unknown")
		err := stage.ApplyTailBatch(batch, func(raw []byte) error {
			cursor, err := OpenChildStageCursor(raw)
			if err != nil {
				return err
			}
			if cursor.pendingBatchDigest != ([32]byte{}) || saved {
				if err = store.Persist(raw); err != nil {
					return err
				}
			}
			if cursor.pendingBatchDigest == ([32]byte{}) {
				return fault
			}
			return nil
		})
		if !errors.Is(err, ErrChildStageOutcomeUnknown) || stage.collection.Len() != 2 {
			t.Fatal("final cursor fault did not follow all writes")
		}
		stage = reopenWitnessedStage(t, stage, store)
		if err = stage.ApplyTailBatch(batch, store.Persist); err != nil {
			t.Fatal(err)
		}
		cursor, _ := stage.Cursor()
		if cursor.imageRows != 2 || cursor.lastBatchDigest != batch.Digest || cursor.pendingBatchDigest != ([32]byte{}) {
			t.Fatal("final cursor retry drifted image")
		}
	}
}
