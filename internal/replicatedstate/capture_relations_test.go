package replicatedstate

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// The test encoder persists the actual relation/key/before/after tuples, not
// only an in-memory observation of the callback. The production schema encoder
// will additionally bind its operation, source cut and bounded replay cursor.
type relationTestCapture struct {
	sessionLeaseCapture
	all bool
}

func (c *relationTestCapture) CaptureAllRelations() bool { return c.all }

func (*relationTestCapture) MaxEncodedBytes(b TransitionCaptureBounds) (int, error) {
	return int(8 + 14*b.Transitions + b.KeyBytes + b.BeforeBytes + b.AfterBytes), nil
}

func (c *relationTestCapture) AppendTransition(dst []byte, transition CapturedTransition) ([]byte, error) {
	if transition.Applied != c.current+1 || c.pending != 0 {
		return dst, ErrTransitionCapture
	}
	dst = binary.LittleEndian.AppendUint64(dst, transition.Applied)
	for i := 0; i < transition.MutationCount(); i++ {
		m := transition.Mutation(i)
		dst = binary.LittleEndian.AppendUint16(dst, uint16(m.Relation))
		for _, value := range [][]byte{m.Key, m.Before, m.After} {
			dst = binary.LittleEndian.AppendUint32(dst, uint32(len(value)))
			dst = append(dst, value...)
		}
	}
	c.pending = transition.Applied
	return dst, nil
}

func readRelationCapture(t *testing.T, collection *durable.Collection, applied uint64) []TransitionMutation {
	t.Helper()
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], applied)
	raw, found, err := collection.AppendRaw(nil, key[:])
	if err != nil || !found || len(raw) < 8 || binary.LittleEndian.Uint64(raw) != applied {
		t.Fatalf("capture applied=%d found=%v err=%v", applied, found, err)
	}
	raw = raw[8:]
	var mutations []TransitionMutation
	for len(raw) != 0 {
		if len(raw) < 2 {
			t.Fatal("truncated relation")
		}
		m := TransitionMutation{Relation: replication.RelationID(binary.LittleEndian.Uint16(raw))}
		raw = raw[2:]
		for _, value := range []*[]byte{&m.Key, &m.Before, &m.After} {
			if len(raw) < 4 {
				t.Fatal("truncated length")
			}
			n := int(binary.LittleEndian.Uint32(raw))
			raw = raw[4:]
			if n > len(raw) {
				t.Fatal("truncated value")
			}
			if n != 0 {
				*value = bytes.Clone(raw[:n])
			}
			raw = raw[n:]
		}
		mutations = append(mutations, m)
	}
	return mutations
}

func TestRelationCapturePersistsEveryRelationAndKeepsLegacyBaseOnly(t *testing.T) {
	for _, all := range []bool{false, true} {
		for _, checkpoint := range []bool{false, true} {
			t.Run(fmt.Sprintf("all_%t_checkpoint_%t", all, checkpoint), func(t *testing.T) {
				limits := durable.Options{MaxDocumentBytes: 1024, InlineValueBytes: 128}
				f := newRelationBundleFixtureWithSecondKind(t, checkpoint, true, limits, limits, RelationJSON)
				capture := &relationTestCapture{sessionLeaseCapture: sessionLeaseCapture{target: f.options.TransitionCaptureTarget}, all: all}
				if err := f.machine.BeginTransitionCapture(capture); err != nil {
					t.Fatal(err)
				}
				batches := []replication.RelationMutationBatch{{Relation: 1}, {Relation: 2}}
				for i := range batches {
					// Equal keys in distinct relations must not collapse together.
					for key := 63; key >= 0; key-- {
						batches[i].Mutations = append(batches[i].Mutations, replication.Mutation{
							Kind: replication.MutationPut, Key: []byte(fmt.Sprintf("key-%02d", key)),
							Value: []byte(fmt.Sprintf(` { "n": %d, "relation": %d } `, key, i+1)),
						})
					}
				}
				command := f.command(t, 1, batches...)
				if err := f.machine.AdmitCommand(command); err != nil {
					t.Fatal(err)
				}
				if _, err := f.machine.ApplyNormal(normalMeta(3), command); err != nil {
					t.Fatal(err)
				}
				changes := readRelationCapture(t, f.capture.Collection, 3)
				want := 64
				if all {
					want = 128
				}
				if len(changes) != want {
					t.Fatalf("captured=%d want=%d", len(changes), want)
				}
				for i, m := range changes {
					relation, key := i/64+1, i%64
					if int(m.Relation) != relation || string(m.Key) != fmt.Sprintf("key-%02d", key) || len(m.Before) != 0 ||
						string(m.After) != fmt.Sprintf(`{"n":%d,"relation":%d}`, key, relation) {
						t.Fatalf("mutation %d: %+v", i, m)
					}
				}
				command = f.command(t, 2,
					replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("key-00"), Value: []byte(`{"n":99}`)}}},
					replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{Kind: replication.MutationDelete, Key: []byte("key-00")}}},
				)
				if _, err := f.machine.ApplyNormal(normalMeta(4), command); err != nil {
					t.Fatal(err)
				}
				changes = readRelationCapture(t, f.capture.Collection, 4)
				if len(changes) != want/64 || string(changes[0].Before) != `{"n":0,"relation":1}` || string(changes[0].After) != `{"n":99}` {
					t.Fatalf("update before/after=%+v", changes)
				}
				if all && (changes[1].Relation != 2 || string(changes[1].Before) != `{"n":0,"relation":2}` || len(changes[1].After) != 0) {
					t.Fatalf("delete before/after=%+v", changes[1])
				}
				// Reopen the actual machine over its durable collection state.
				fresh := &relationTestCapture{sessionLeaseCapture: sessionLeaseCapture{target: f.options.TransitionCaptureTarget}, all: all}
				options := f.options
				options.TransitionCapture = fresh
				var err error
				f.machine, err = OpenBundle(f.binding, testBootstrap(), f.system,
					relationBundleCollections(f.base, f.global, f.index, f.second), f.log, options)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.machine.ApplyNormal(normalMeta(5), command); err != nil {
					t.Fatal(err)
				}
				if got := readRelationCapture(t, f.capture.Collection, 5); len(got) != 0 {
					t.Fatal("exact retry captured duplicate mutations")
				}
				if got := readRelationCapture(t, f.capture.Collection, 4); len(got) != len(changes) {
					t.Fatal("reopen lost capture")
				}
				if _, err := f.machine.ApplyNormal(normalMeta(6), nil); err != nil {
					t.Fatal(err)
				}
				if got := readRelationCapture(t, f.capture.Collection, 6); len(got) != 0 {
					t.Fatal("empty publication gained mutations")
				}
			})
		}
	}
}

func TestRelationCaptureBoundsAndWarmWorkspace(t *testing.T) {
	limits := durable.Options{MaxDocumentBytes: 1024, InlineValueBytes: 128}
	f := newRelationBundleFixtureWithSecondKind(t, true, true, limits, limits, RelationJSON)
	capture := &relationTestCapture{sessionLeaseCapture: sessionLeaseCapture{target: f.options.TransitionCaptureTarget}, all: true}
	all := f.machine.captureCollectionLimits(capture)
	if all.MaxDistinctMutations != 128 || all.MaxDocumentBytes != 1024 {
		t.Fatalf("capture did not qualify the full bundle: %+v", all)
	}
	oldOptions := f.machine.options
	f.machine.options.TxnLimits.MaxDocuments = 132 // 128 rows + system rows, missing the capture row.
	if err := f.machine.BeginTransitionCapture(capture); err == nil || f.capture.Collection.Len() != 0 {
		t.Fatal("capture capacity shortage was not refused before publication")
	}
	f.machine.options = oldOptions
	if err := f.machine.BeginTransitionCapture(capture); err != nil {
		t.Fatal(err)
	}
	changes := []finalMutation{
		{key: []byte("same"), value: []byte(`{"n":1}`)},
		{key: []byte("same"), value: []byte(`{"n":2}`)},
	}
	spans := []plannedRelationChanges{{ordinal: 0, start: 0, end: 1}, {ordinal: 1, start: 1, end: 2}}
	next := f.machine.nextState(normalMeta(3), RecordNormal, [32]byte{9})
	if got := f.machine.capturedTransition(next, changes, spans); !validCapturedTransition(got) {
		t.Fatal("valid capture rejected")
	}
	f.machine.releaseCaptureChanges()
	if allocations := testing.AllocsPerRun(100, func() {
		if got := f.machine.capturedTransition(next, changes, spans); !validCapturedTransition(got) {
			panic("invalid capture")
		}
		f.machine.releaseCaptureChanges()
	}); allocations != 0 {
		t.Fatalf("warm capture allocations=%g", allocations)
	}
	for _, bad := range [][]plannedRelationChanges{
		nil,
		{{ordinal: 0, start: 0, end: 1}},
		{{ordinal: 0, start: 0, end: 1}, {ordinal: 1, start: 0, end: 2}},
		{{ordinal: 0, start: 0, end: 1}, {ordinal: 0, start: 1, end: 2}},
		{{ordinal: 0, start: 0, end: 1}, {ordinal: 2, start: 1, end: 2}},
	} {
		if got := f.machine.capturedTransition(next, changes, bad); validCapturedTransition(got) {
			t.Fatalf("invalid spans accepted: %+v", bad)
		}
		f.machine.releaseCaptureChanges()
	}
	for _, mutation := range f.machine.captureChanges[:cap(f.machine.captureChanges)] {
		if mutation.key != nil || mutation.before != nil || mutation.value != nil {
			t.Fatal("capture retained borrowed rows")
		}
	}
}

func TestRelationCaptureTransactionPublication(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit_%t", commit), func(t *testing.T) {
			limits := durable.Options{MaxDocumentBytes: 1024, InlineValueBytes: 128}
			f := newRelationBundleFixtureWithCollectionOptions(t, true, true, limits, limits)
			capture := &relationTestCapture{sessionLeaseCapture: sessionLeaseCapture{target: f.options.TransitionCaptureTarget}, all: true}
			if err := f.machine.BeginTransitionCapture(capture); err != nil {
				t.Fatal(err)
			}
			batches := []replication.RelationMutationBatch{
				{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPutAbsentOrEqual, Key: []byte("employee"), Value: []byte(`{"email":"employee@example.com"}`)}}},
				{Relation: 2, Mutations: []replication.Mutation{{Kind: replication.MutationPutAbsentOrEqual, Key: []byte{0x91, 0x02, 'e'}, Value: []byte(`["employee"]`)}}},
			}
			id := transactionCodecID(241)
			prepare := transactionCompletionCommand(t, f.binding, fusedTargetControl(
				t, f, id, distributedtxn.ReplicatedStagePrepareTarget, 0, batches,
			), batches)
			applyTransactionCommand(t, f.machine, 3, prepare)
			if got := readRelationCapture(t, f.capture.Collection, 3); len(got) != 0 {
				t.Fatal("capture exposed uncommitted transaction payload")
			}
			// Prepared state and the capture must both recover before settlement.
			options := f.options
			options.TransitionCapture = &relationTestCapture{sessionLeaseCapture: sessionLeaseCapture{target: options.TransitionCaptureTarget}, all: true}
			var err error
			f.machine, err = OpenBundle(f.binding, testBootstrap(), f.system,
				relationBundleCollections(f.base, f.global, f.index, f.second), f.log, options)
			if err != nil {
				t.Fatal(err)
			}
			operation := distributedtxn.ReplicatedAbortReleaseTarget
			if commit {
				operation = distributedtxn.ReplicatedApplyReleaseTarget
			}
			finish := transactionCompletionCommand(t, f.binding, fusedTargetControl(
				t, f, id, operation, 2, nil,
			), nil)
			applyTransactionCommand(t, f.machine, 4, finish)
			changes := readRelationCapture(t, f.capture.Collection, 4)
			if !commit {
				if len(changes) != 0 {
					t.Fatal("capture exposed aborted transaction payload")
				}
			} else {
				if len(changes) != len(batches) {
					t.Fatalf("committed capture rows=%d want=%d", len(changes), len(batches))
				}
				for i, batch := range batches {
					got, want := changes[i], batch.Mutations[0]
					if got.Relation != batch.Relation || !bytes.Equal(got.Key, want.Key) || got.Before != nil || !bytes.Equal(got.After, want.Value) {
						t.Fatalf("committed capture %d = %+v", i, got)
					}
				}
			}
			applyTransactionCommand(t, f.machine, 5, finish)
			if got := readRelationCapture(t, f.capture.Collection, 5); len(got) != 0 {
				t.Fatal("settlement retry captured duplicate mutations")
			}
		})
	}
}

func TestRelationCaptureCrashRecoveryIsAtomicWithRows(t *testing.T) {
	for _, test := range []struct {
		name        string
		phase       durable.CheckpointGroupFaultPhaseForFacadeTest
		duringApply bool
	}{
		{"prepare_append", durable.CheckpointGroupFaultAfterPrepareAppendForFacadeTest, true},
		{"decision_append", durable.CheckpointGroupFaultAfterDecisionAppendForFacadeTest, true},
		{"journal_sync", durable.CheckpointGroupFaultAfterJournalSyncForFacadeTest, false},
		{"physical_checkpoint", durable.CheckpointGroupFaultAfterPhysicalCheckpointForFacadeTest, false},
		{"certificate_write", durable.CheckpointGroupFaultAfterCertificateWriteForFacadeTest, false},
		{"certificate_sync", durable.CheckpointGroupFaultAfterCertificateSyncForFacadeTest, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := durable.Options{MaxDocumentBytes: 1024, InlineValueBytes: 128}
			f := newRelationBundleFixtureWithSecondKind(t, true, true, limits, limits, RelationJSON)
			capture := &relationTestCapture{sessionLeaseCapture: sessionLeaseCapture{target: f.options.TransitionCaptureTarget}, all: true}
			if err := f.machine.BeginTransitionCapture(capture); err != nil {
				t.Fatal(err)
			}
			if err := f.group.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			command := f.command(t, 1,
				replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("same"), Value: []byte(`{"n":1}`)}}},
				replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("same"), Value: []byte(`{"n":2}`)}}},
			)
			if !test.duringApply {
				if _, err := f.machine.ApplyNormal(normalMeta(3), command); err != nil {
					t.Fatal(err)
				}
			}
			fired, restore := durable.InstallCheckpointGroupFaultForFacadeTest(test.phase)
			var faultErr error
			if test.duringApply {
				_, faultErr = f.machine.ApplyNormal(normalMeta(3), command)
			} else {
				faultErr = f.group.Checkpoint()
			}
			restore()
			if faultErr == nil || !fired() {
				t.Fatalf("fault fired=%v err=%v", fired(), faultErr)
			}
			// Copy before any Close: reopening these bytes exercises the durable
			// recovery path, not an orderly shutdown of the original machine.
			dir := copyRelationBundleCrashDirectory(t, f.dir)
			names := []string{systemCollectionName, "base", "global", TransitionCaptureCollectionName}
			files := []string{"system", "base", "global", TransitionCaptureCollectionName}
			baseOptions := limits
			baseOptions.Indexes = []store.IndexDefinition{f.index}
			collectionOptions := []durable.Options{
				{OpaqueValues: true, MaxBatchDocuments: 32}, baseOptions, limits,
				{OpaqueValues: true, MaxKeyBytes: 8, MaxDocumentBytes: MaxTransitionCaptureRecordBytes, MaxBatchDocuments: 1, MaxBatchBytes: MaxTransitionCaptureRecordBytes + 8},
			}
			opens := make([]durable.TransactionCollectionOpen, len(files))
			for i, name := range files {
				file, err := os.OpenFile(filepath.Join(dir, name+".vdb"), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = file.Close() })
				opens[i] = durable.TransactionCollectionOpen{File: file, Options: collectionOptions[i]}
			}
			collections, log, group, err := durable.OpenCollectionsWithCheckpointGroup(
				dir, durable.TxnLogOptions{}, opens, names, durable.CheckpointGroupOptions{CheckpointEvery: 1024},
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = group.Close()
				for _, collection := range collections {
					_ = collection.Close()
				}
				_ = log.Close()
			})
			options := f.options
			options.CheckpointGroup = group
			options.TransitionCaptureTarget.Collection = collections[3]
			options.TransitionCapture = &relationTestCapture{sessionLeaseCapture: sessionLeaseCapture{target: options.TransitionCaptureTarget}, all: true}
			machine, err := OpenBundle(f.binding, testBootstrap(), systemTargetOf(collections[0]),
				relationBundleCollections(targetOf(collections[1]), targetOf(collections[2]), f.index, RelationJSON), log, options)
			if err != nil {
				t.Fatal(err)
			}
			applied := machine.Applied()
			if applied != 2 && applied != 3 {
				t.Fatalf("recovered unexpected cut %d", applied)
			}
			for i := 1; i <= 2; i++ {
				value, found, err := collections[i].AppendRaw(nil, []byte("same"))
				if err != nil || found != (applied == 3) || found && string(value) != fmt.Sprintf(`{"n":%d}`, i) {
					t.Fatalf("relation %d skew at %d: %q found=%v err=%v", i, applied, value, found, err)
				}
			}
			if collections[3].Len() != applied-1 {
				t.Fatal("capture and data recovered different publications")
			}
			if applied == 3 {
				changes := readRelationCapture(t, collections[3], 3)
				if len(changes) != 2 || changes[0].Relation != 1 || changes[1].Relation != 2 ||
					string(changes[0].After) != `{"n":1}` || string(changes[1].After) != `{"n":2}` {
					t.Fatalf("capture skew after recovery: %+v", changes)
				}
			}
		})
	}
}
