package query

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestCancelFlagTakeIsAtomicAndReusable(t *testing.T) {
	var nilFlag *CancelFlag
	if nilFlag.Take() {
		t.Fatal("nil CancelFlag reported a cancellation")
	}

	var cancel CancelFlag
	if cancel.Take() {
		t.Fatal("fresh CancelFlag reported a cancellation")
	}
	cancel.Cancel()
	if !cancel.Take() {
		t.Fatal("Take did not consume an armed cancellation")
	}
	if cancel.Canceled() || cancel.Take() {
		t.Fatal("consumed cancellation remained armed")
	}
	cancel.Cancel()
	if !cancel.Take() {
		t.Fatal("CancelFlag was not reusable after Take")
	}
}

func TestCursorNextWithCancelIsReusableAndNilFlagIsAllocationFree(t *testing.T) {
	base := NewTextCursor("value", "visible")
	var cancel CancelFlag
	cancel.Cancel()
	cursor := base
	if next, err := cursor.NextWithCancel(&cancel); next || !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled advance = %v, %v; want false, ErrCanceled", next, err)
	}
	cancel.Reset()
	if next, err := cursor.NextWithCancel(&cancel); !next || err != nil {
		t.Fatalf("advance after reset = %v, %v; want true, nil", next, err)
	}

	var next bool
	var err error
	allocs := testing.AllocsPerRun(1000, func() {
		cursor = base
		next, err = cursor.NextWithCancel(nil)
	})
	if err != nil || !next {
		t.Fatalf("nil-flag advance = %v, %v; want true, nil", next, err)
	}
	if allocs != 0 {
		t.Fatalf("nil-flag cursor advance allocated %.2f times, want zero", allocs)
	}
}

func TestCancelFlagStopsAndRecoversEveryScanBackend(t *testing.T) {
	documents := [][]byte{
		[]byte(`{"id":1,"active":true}`),
		[]byte(`{"id":2,"active":false}`),
		[]byte(`{"id":3,"active":true}`),
	}
	segment := buildSegment(t, documents, storageMode{"cancel", true, true})
	collection := &store.Collection{}
	for i, document := range documents {
		if _, err := collection.Put(fmt.Sprintf("k%d", i), document); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	file := durableScanCorpus(t, 32)

	query := Select(Path("id")).Where(Cmp("active", Eq, true))
	for _, test := range []struct {
		name   string
		source Source
	}{
		{"segment", FromSegment(segment)},
		{"snapshot", FromSnapshot(snapshot)},
		{"durable", FromFile(file)},
	} {
		t.Run(test.name, func(t *testing.T) {
			var cancel CancelFlag
			cancel.Cancel()
			exec := Exec{Options: ExecOptions{Workers: 2, Cancel: &cancel}}
			if err := query.RunInto(&exec, test.source); !errors.Is(err, ErrCanceled) {
				t.Fatalf("canceled RunInto error = %v, want ErrCanceled", err)
			}
			if exec.Result.RowCount != 0 {
				t.Fatalf("canceled RunInto exposed %d rows", exec.Result.RowCount)
			}

			cancel.Reset()
			if err := query.RunInto(&exec, test.source); err != nil {
				t.Fatalf("RunInto after Reset: %v", err)
			}
			if exec.Result.RowCount == 0 {
				t.Fatal("RunInto after Reset returned no rows")
			}
		})
	}
}

func TestCancellationDoesNotMaskAnObservedEvaluatorError(t *testing.T) {
	storageErr := errors.New("durable probe read failed")
	var evaluator evalScratch
	evaluator.parkError(storageErr)
	if err := evaluator.errorOr(ErrCanceled); !errors.Is(err, storageErr) {
		t.Fatalf("errorOr(ErrCanceled) = %v, want prior storage error", err)
	}
}

func TestCancellationDoesNotMaskAFileScannerError(t *testing.T) {
	scanErr := errors.New("durable range read failed")
	if err := fileExecutionError(ErrCanceled, scanErr); !errors.Is(err, scanErr) {
		t.Fatalf("fileExecutionError(ErrCanceled, scan error) = %v, want scan error", err)
	}
	if err := fileExecutionError(scanErr, ErrCanceled); !errors.Is(err, scanErr) {
		t.Fatalf("fileExecutionError(scan error, ErrCanceled) = %v, want scan error", err)
	}
	if err := fileExecutionError(nil, ErrCanceled); !errors.Is(err, ErrCanceled) {
		t.Fatalf("fileExecutionError(nil, ErrCanceled) = %v, want ErrCanceled", err)
	}
	if err := fileExecutionError(nil, errFileExecutionStopped); err != nil {
		t.Fatalf("fileExecutionError(nil, stopped) = %v, want nil", err)
	}
}

func TestMutationFilterCancellationPropagatesAndIsReusable(t *testing.T) {
	statement, err := PrepareDML(`DELETE FROM docs WHERE id >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	var cancel CancelFlag
	exec := Exec{Options: ExecOptions{Cancel: &cancel}}
	visits := 0
	filter, err := statement.Filter(&exec, nil, func(_, _ []byte) error {
		visits++
		cancel.Cancel()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < scanBatch; row++ {
		err = filter.Add(
			[]byte("row"),
			fmt.Appendf(nil, `{"id":%d}`, row),
		)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("mutation filter error = %v, want ErrCanceled", err)
	}
	if visits != 1 {
		t.Fatalf("mutation filter visited %d rows after cancellation, want 1", visits)
	}
	assertWorkspaceBorrowedViewsCleared(t, &exec.Workspace)

	cancel.Reset()
	visits = 0
	filter, err = statement.Filter(&exec, nil, func(_, _ []byte) error {
		visits++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := filter.Add([]byte("row"), []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := filter.Done(); err != nil {
		t.Fatalf("mutation filter after Reset: %v", err)
	}
	if visits != 1 {
		t.Fatalf("mutation filter after Reset visited %d rows, want 1", visits)
	}
}

func TestUnfilteredMutationCancellationDoesNotRetainAStaleFlag(t *testing.T) {
	statement, err := PrepareDML(`DELETE FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	var cancel CancelFlag
	cancel.Cancel()
	exec := Exec{Options: ExecOptions{Cancel: &cancel}}
	if _, err := statement.Filter(&exec, nil, func(_, _ []byte) error {
		t.Fatal("a pre-canceled unfiltered mutation visited a row")
		return nil
	}); !errors.Is(err, ErrCanceled) {
		t.Fatalf("pre-canceled Filter error = %v, want ErrCanceled", err)
	}
	if statement.scan.e != nil || statement.scan.visit != nil {
		t.Fatal("pre-canceled Filter retained the rejected Exec or callback")
	}

	cancel.Reset()
	visits := 0
	filter, err := statement.Filter(&exec, nil, func(_, _ []byte) error {
		visits++
		cancel.Cancel()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := filter.Add([]byte("row"), []byte(`{"id":1}`)); !errors.Is(err, ErrCanceled) {
		t.Fatalf("callback cancellation error = %v, want ErrCanceled", err)
	}
	if visits != 1 {
		t.Fatalf("callback cancellation visited %d rows, want 1", visits)
	}

	// Removing the option must disarm the reused Workspace. Cancel the old flag
	// again so this would fail if Filter retained the prior execution's pointer.
	cancel.Reset()
	exec.Options.Cancel = nil
	filter, err = statement.Filter(&exec, nil, func(_, _ []byte) error {
		visits++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel.Cancel()
	if err := filter.Add([]byte("row"), []byte(`{"id":2}`)); err != nil {
		t.Fatalf("unfiltered Add with removed option: %v", err)
	}
	if err := filter.Done(); err != nil {
		t.Fatalf("unfiltered Done with removed option: %v", err)
	}
	if visits != 2 {
		t.Fatalf("unfiltered reuse visited %d rows, want 2", visits)
	}
}

// blockingOverlay stops the durable scanner after several complete batches
// have entered the reusable pipeline. The channel handshake makes cancellation
// deterministic without sleeps or scheduler assumptions.
type blockingOverlay struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	at      int
	seen    int
}

func (o *blockingOverlay) Lookup(_ []byte) ([]byte, bool, bool) {
	o.seen++
	if o.seen == o.at {
		o.once.Do(func() { close(o.entered) })
		<-o.release
	}
	return nil, false, false
}

func (*blockingOverlay) RangeInserts(func([]byte) error) error { return nil }
func (*blockingOverlay) RangePresent(func([]byte) error) error { return nil }
func (*blockingOverlay) LenDelta() int64                       { return 0 }

func TestDurableCancellationDrainsPipelineAndRecovers(t *testing.T) {
	snapshot, segment := poolReuseCorpus(t, 900)
	query := Select(Path("id"), Path("label")).
		Where(Cmp("bucket", Ge, 0)).
		OrderBy("id", Desc)
	var cancel CancelFlag
	exec := Exec{Options: ExecOptions{
		Workers: 4, BatchRows: 8, BatchBytes: 2 << 10,
		MemoryBytes: 64 << 10, Cancel: &cancel,
	}}
	overlay := &blockingOverlay{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		at:      41,
	}
	source := NewFileOverlaySource(overlay)
	done := make(chan error, 1)
	go func() {
		done <- query.RunInto(&exec, FromFileOverlay(snapshot, &source))
	}()
	<-overlay.entered
	cancel.Cancel()
	close(overlay.release)
	if err := <-done; !errors.Is(err, ErrCanceled) {
		t.Fatalf("durable cancellation error = %v, want ErrCanceled", err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("canceled durable execution exposed %d rows", exec.Result.RowCount)
	}
	assertPoolDrained(t, &exec, "after cancellation")
	assertCanceledExecClean(t, &exec)

	cancel.Reset()
	want, err := query.Run(FromSegment(segment))
	if err != nil {
		t.Fatal(err)
	}
	if err := query.RunInto(&exec, FromFile(snapshot)); err != nil {
		t.Fatalf("durable RunInto after Reset: %v", err)
	}
	if diff := diffResults(exec.Result, want); diff != "" {
		t.Fatalf("durable result after cancellation differs: %s", diff)
	}
	assertPoolDrained(t, &exec, "after recovery")
}

func TestGroupedDurableCancellationClearsSlotIndexes(t *testing.T) {
	snapshot, _ := poolReuseCorpus(t, 900)
	query := Select(Path("bucket"), Count()).
		Where(Cmp("id", Ge, 0)).
		GroupBy("bucket")
	var cancel CancelFlag
	exec := Exec{Options: ExecOptions{
		Workers: 4, BatchRows: 8, BatchBytes: 2 << 10,
		MemoryBytes: 64 << 10, Cancel: &cancel,
	}}
	overlay := &blockingOverlay{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		at:      41,
	}
	source := NewFileOverlaySource(overlay)
	done := make(chan error, 1)
	go func() {
		done <- query.RunInto(&exec, FromFileOverlay(snapshot, &source))
	}()
	<-overlay.entered
	cancel.Cancel()
	close(overlay.release)
	if err := <-done; !errors.Is(err, ErrCanceled) {
		t.Fatalf("grouped durable cancellation error = %v, want ErrCanceled", err)
	}
	assertPoolDrained(t, &exec, "after grouped cancellation")
	assertCanceledExecClean(t, &exec)
	for i := range exec.file.slots {
		if len(exec.file.slots[i].byKey) != 0 {
			t.Fatalf(
				"canceled grouped slot %d retained %d arena-backed keys",
				i, len(exec.file.slots[i].byKey),
			)
		}
	}

	cancel.Reset()
	if err := query.RunInto(&exec, FromFile(snapshot)); err != nil {
		t.Fatalf("grouped durable RunInto after Reset: %v", err)
	}
}

func TestCanceledJoinSourcesReleaseBindingsAndRecover(t *testing.T) {
	t.Run("heap database", func(t *testing.T) {
		var database store.Database
		outer, err := database.CreateCollection("orders", store.Options{})
		if err != nil {
			t.Fatal(err)
		}
		inner, err := database.CreateCollection("customers", store.Options{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range 32 {
			key := fmt.Sprintf("c%06d", i)
			if _, err := inner.Put(key, fmt.Appendf(nil, `{"tier":"pro","seat":%d}`, i)); err != nil {
				t.Fatal(err)
			}
		}
		for i := range 512 {
			if _, err := outer.Put(
				fmt.Sprintf("o%06d", i),
				fmt.Appendf(nil, `{"id":%d,"customer":"c%06d"}`, i, i%32),
			); err != nil {
				t.Fatal(err)
			}
		}
		catalog := database.Snapshot()
		query := Select(Path("id")).Join(JoinOn("customers", "customer", JoinKey))
		assertCanceledJoinReleasesAndRecovers(
			t, query, FromDatabase(catalog, "orders"), false,
		)
	})

	t.Run("durable database", func(t *testing.T) {
		database := durableJoinCorpus(t, 512, 32)
		catalog, err := database.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = catalog.Close() }()
		query := Select(Path("id")).Join(JoinOn("customers", "customer", JoinKey))
		assertCanceledJoinReleasesAndRecovers(
			t, query, FromFileDatabase(catalog, "orders"), true,
		)
	})
}

func assertCanceledJoinReleasesAndRecovers(
	t *testing.T,
	query *Query,
	source Source,
	durable bool,
) {
	t.Helper()
	var cancel CancelFlag
	exec := Exec{Options: ExecOptions{
		Workers: 2, JoinMembershipMax: 1, Cancel: &cancel,
	}}
	if err := query.RunInto(&exec, source); err != nil {
		t.Fatal(err)
	}
	if len(exec.Workspace.joins) != 1 {
		t.Fatalf("bound %d joins, want 1", len(exec.Workspace.joins))
	}
	binding := &exec.Workspace.joins[0]
	if binding.mode != joinBindProbe {
		t.Fatalf("join mode = %v, want direct probe", binding.mode)
	}
	if durable && binding.file.snapshot == nil {
		t.Fatal("durable direct probe did not retain its bound inner snapshot")
	}
	if !durable && binding.snapshot.Len() == 0 {
		t.Fatal("heap direct probe did not retain its bound inner snapshot")
	}

	// Exercise the direct probe's own cancellation check, then enter RunInto
	// with the same flag still canceled. The latter must discard the successful
	// prior execution's binding even though it returns before a new bind starts.
	cancel.Cancel()
	var probe joinProbe
	probe.reset()
	if binding.matches(
		scalar{kind: kindString, sval: "c000000"},
		&probe,
	) {
		t.Fatal("a canceled direct join probe matched")
	}
	if !errors.Is(probe.err, ErrCanceled) {
		t.Fatalf("direct join probe error = %v, want ErrCanceled", probe.err)
	}
	if err := query.RunInto(&exec, source); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled joined RunInto error = %v, want ErrCanceled", err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("canceled joined execution exposed %d rows", exec.Result.RowCount)
	}
	assertCanceledExecClean(t, &exec)
	for i := range exec.Workspace.joins {
		binding := &exec.Workspace.joins[i]
		if binding.mode != joinBindNone || binding.file.snapshot != nil ||
			binding.snapshot.Len() != 0 || binding.plan != nil ||
			binding.scan.cancel != nil {
			t.Fatalf("join binding %d retained canceled execution state", i)
		}
	}

	cancel.Reset()
	if err := query.RunInto(&exec, source); err != nil {
		t.Fatalf("joined RunInto after Reset: %v", err)
	}
	if exec.Result.RowCount == 0 {
		t.Fatal("joined RunInto after Reset returned no rows")
	}
}

func TestCanceledSpillMergeStillRemovesEveryRun(t *testing.T) {
	var cancel CancelFlag
	directory := t.TempDir()
	manager := newSpillManager(directory, nil, nil, 0, nil, -1, &cancel)
	plan := &plan{}
	first, err := manager.writeRows(plan, []fileRow{{ordinal: 1}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.writeRows(plan, []fileRow{{ordinal: 2}})
	if err != nil {
		t.Fatal(err)
	}

	cancel.Cancel()
	if _, err := manager.mergeRowRuns(plan, []spillRun{first, second}); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled merge error = %v, want ErrCanceled", err)
	}
	files, err := manager.cleanup()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("cleanup retained %d tracked spill files", len(files))
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("cleanup left %d spill files on disk", len(entries))
	}
}

type cancelAfterWrite struct {
	dst    io.Writer
	cancel *CancelFlag
	writes int
}

func (w *cancelAfterWrite) Write(src []byte) (int, error) {
	n, err := w.dst.Write(src)
	if err == nil && n != 0 && w.writes == 0 {
		w.cancel.Cancel()
	}
	w.writes++
	return n, err
}

func TestCancellationDuringActiveSpillWriteRemovesPartialOutput(t *testing.T) {
	var cancel CancelFlag
	directory := t.TempDir()
	manager := newSpillManager(directory, nil, nil, 0, nil, -1, &cancel)
	file, err := manager.create("active-cancel-*")
	if err != nil {
		t.Fatal(err)
	}
	writer := &cancelAfterWrite{dst: manager.writer(file), cancel: &cancel}
	if _, err := writer.Write([]byte("partial spill output")); err != nil {
		t.Fatal(err)
	}
	if !cancel.Canceled() {
		t.Fatal("active writer did not arm cancellation")
	}
	if _, err := manager.finish(file); !errors.Is(err, ErrCanceled) {
		t.Fatalf("finish after active write error = %v, want ErrCanceled", err)
	}
	files, err := manager.cleanup()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("cleanup retained %d tracked spill files", len(files))
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("cleanup left %d partial spill files on disk", len(entries))
	}
}

func assertCanceledExecClean(t *testing.T, exec *Exec) {
	t.Helper()
	assertWorkspaceBorrowedViewsCleared(t, &exec.Workspace)
	if len(exec.file.rows) != 0 || len(exec.file.groupSet) != 0 ||
		len(exec.file.mergedGroups) != 0 || len(exec.file.rowRuns) != 0 ||
		len(exec.file.groupRuns) != 0 {
		t.Fatalf(
			"canceled durable frontiers retained rows=%d groups=%d merged=%d row-runs=%d group-runs=%d",
			len(exec.file.rows), len(exec.file.groupSet),
			len(exec.file.mergedGroups), len(exec.file.rowRuns),
			len(exec.file.groupRuns),
		)
	}
	for i := range exec.file.workers {
		assertWorkspaceBorrowedViewsCleared(t, &exec.file.workers[i].Workspace)
		if len(exec.file.workers[i].columns) != 0 {
			t.Fatalf("canceled durable worker %d retained its column directory", i)
		}
		if len(exec.file.workers[i].eval.binds) != 0 {
			t.Fatalf("canceled durable worker %d retained join bindings", i)
		}
	}
	for i := range exec.file.slots {
		if len(exec.file.slots[i].byKey) != 0 {
			t.Fatalf("canceled durable slot %d retained group keys", i)
		}
	}
}

func assertWorkspaceBorrowedViewsCleared(t *testing.T, workspace *Workspace) {
	t.Helper()
	if workspace.ctx.s != nil || workspace.ctx.rows != 0 ||
		len(workspace.numRaws) != 0 || len(workspace.groups) != 0 ||
		len(workspace.eval.binds) != 0 {
		t.Fatalf(
			"canceled Workspace retained source views: segment=%p rows=%d numeric-raws=%d groups=%d bindings=%d",
			workspace.ctx.s, workspace.ctx.rows, len(workspace.numRaws),
			len(workspace.groups), len(workspace.eval.binds),
		)
	}
	for i := range workspace.raws {
		if len(workspace.raws[i]) != 0 {
			t.Fatalf("canceled Workspace retained raw column %d", i)
		}
	}
	for i := range workspace.ctx.values {
		if len(workspace.ctx.values[i]) != 0 {
			t.Fatalf("canceled Workspace retained value column %d", i)
		}
	}
	for i := range workspace.ctx.nums {
		if len(workspace.ctx.nums[i].vals) != 0 {
			t.Fatalf("canceled Workspace retained numeric column %d", i)
		}
	}
	for i := range workspace.accs {
		if workspace.accs[i].num != nil &&
			(workspace.accs[i].num.n != 0 ||
				workspace.accs[i].num.extreme.kind != kindNull) {
			t.Fatalf("canceled Workspace retained aggregate %d", i)
		}
	}
}

func TestDormantCancellationProbeSteadyAllocs(t *testing.T) {
	documents := make([][]byte, 512)
	for i := range documents {
		documents[i] = fmt.Appendf(nil, `{"id":%d,"bucket":%d}`, i, i%11)
	}
	segment := buildSegment(t, documents, storageMode{"cancel-alloc", true, true})
	query := Select(Path("id")).Where(Cmp("bucket", Lt, 6))
	var cancel CancelFlag
	exec := Exec{Options: ExecOptions{Workers: 1, Cancel: &cancel}}
	if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("dormant cancellation probe allocated %.2f times, want 0", allocations)
	}
}

func TestCancelFlagConcurrentUseAndRecovery(t *testing.T) {
	const rows = 24_000
	segment := buildSegment(t, parallelCorpus(rows), storageMode{"cancel-race", true, true})
	query := Select(Path("id"), Path("tag")).Where(Cmp("sel", Lt, 500))
	var cancel CancelFlag
	exec := Exec{Options: ExecOptions{Workers: 4, Cancel: &cancel}}
	if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
		t.Fatal(err)
	}

	for range 16 {
		done := make(chan error, 1)
		go func() { done <- query.RunInto(&exec, FromSegment(segment)) }()
		runtime.Gosched()
		cancel.Cancel()
		err := <-done
		if err != nil && !errors.Is(err, ErrCanceled) {
			t.Fatalf("concurrent cancellation returned %v", err)
		}
		cancel.Reset()
	}
	if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
		t.Fatalf("final RunInto after Reset: %v", err)
	}
}

func BenchmarkCancellationProbeOverhead(b *testing.B) {
	documents := make([][]byte, 4096)
	for i := range documents {
		documents[i] = fmt.Appendf(nil, `{"id":%d,"bucket":%d}`, i, i%17)
	}
	segment := buildSegment(b, documents, storageMode{"cancel-bench", true, true})
	query := Select(Path("id")).Where(Cmp("bucket", Lt, 8))

	for _, test := range []struct {
		name   string
		cancel *CancelFlag
	}{
		{"nil", nil},
		{"dormant", new(CancelFlag)},
	} {
		b.Run(test.name, func(b *testing.B) {
			exec := Exec{Options: ExecOptions{Workers: 1, Cancel: test.cancel}}
			if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := query.RunInto(&exec, FromSegment(segment)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
