package query

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestResultRowBudgetRejectsBeforeMaterialization(t *testing.T) {
	var segment store.Segment
	for _, document := range []string{`{"n":1}`, `{"n":2}`, `{"n":3}`} {
		if _, err := segment.Append([]byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	exec := Exec{Options: ExecOptions{ResultRows: 2, ResultBytes: -1}}
	err := Select(Path("n")).RunInto(&exec, FromSegment(&segment))
	var budgetErr *ResultBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrResultBudget) {
		t.Fatalf("error = %v, want ResultBudgetError", err)
	}
	if budgetErr.Rows != 3 || budgetErr.RowLimit != 2 {
		t.Fatalf("budget error = %+v", budgetErr)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("partial result exposed %d rows", exec.Result.RowCount)
	}
}

func TestResultByteBudgetIncludesColumnAndCellStorage(t *testing.T) {
	var segment store.Segment
	if _, err := segment.Append([]byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	required := resultColumnBytes + resultCellBytes
	exec := Exec{Options: ExecOptions{
		ResultRows: -1, ResultBytes: required - 1,
	}}
	err := Select(Path("n")).RunInto(&exec, FromSegment(&segment))
	var budgetErr *ResultBudgetError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("error = %v, want ResultBudgetError", err)
	}
	if budgetErr.Bytes != required || budgetErr.ByteLimit != required-1 {
		t.Fatalf("budget error = %+v, required %d", budgetErr, required)
	}
}

func TestResultByteBudgetIncludesBorrowedHeapPayload(t *testing.T) {
	var segment store.Segment
	if _, err := segment.Append([]byte(`{"s":"payload"}`)); err != nil {
		t.Fatal(err)
	}
	cell := cellFromScalar(scalar{
		kind: kindString,
		raw:  []byte(`"payload"`),
		sval: "payload",
	})
	required := resultColumnBytes + resultCellBytes +
		resultCellPayloadBytes(cell)
	exec := Exec{Options: ExecOptions{
		ResultRows: -1, ResultBytes: required - 1,
	}}
	err := Select(Path("s")).RunInto(&exec, FromSegment(&segment))
	if !errors.Is(err, ErrResultBudget) {
		t.Fatalf("borrowed payload error = %v, want result budget", err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("borrowed payload exposed %d partial rows", exec.Result.RowCount)
	}
}

func TestDurableResultPayloadBudgetAbortsAtomically(t *testing.T) {
	snapshot := durableScanCorpus(t, 1)
	base := resultColumnBytes + resultCellBytes
	exec := Exec{Options: ExecOptions{
		ResultRows: -1, ResultBytes: base,
	}}
	err := Select(Path("label")).RunInto(&exec, FromFile(snapshot))
	var budgetErr *ResultBudgetError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("error = %v, want ResultBudgetError", err)
	}
	if budgetErr.Bytes <= base {
		t.Fatalf("payload was not charged: %+v", budgetErr)
	}
	if exec.Result.RowCount != 0 || len(exec.Result.fileData) != 0 {
		t.Fatalf(
			"partial durable result exposed: rows=%d bytes=%d",
			exec.Result.RowCount, len(exec.Result.fileData),
		)
	}
}

func TestResultBudgetDefaultsAndExplicitDisable(t *testing.T) {
	rows, bytes, err := normalizeResultBudget(ExecOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if rows != DefaultResultRows || bytes != DefaultResultBytes {
		t.Fatalf("defaults = (%d,%d)", rows, bytes)
	}
	rows, bytes, err = normalizeResultBudget(ExecOptions{
		ResultRows: -1, ResultBytes: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rows != -1 || bytes != -1 {
		t.Fatalf("disabled = (%d,%d)", rows, bytes)
	}
	for _, options := range []ExecOptions{
		{ResultRows: -2},
		{ResultBytes: -2},
	} {
		if _, _, err := normalizeResultBudget(options); err == nil {
			t.Fatalf("invalid options accepted: %+v", options)
		}
	}
}

func TestDurableProjectionIsRowBoundedWhileConsumingBatches(t *testing.T) {
	snapshot := durableScanCorpus(t, 32)
	exec := Exec{Options: ExecOptions{
		ResultRows: 2, ResultBytes: -1,
	}}
	err := Select(Path("id")).RunInto(&exec, FromFile(snapshot))
	if !errors.Is(err, ErrResultBudget) {
		t.Fatalf("durable projection error = %v, want result budget", err)
	}
	if len(exec.file.rows) != 0 {
		t.Fatalf("failed projection retained %d frontier rows", len(exec.file.rows))
	}
}

func TestDurableOrderedSpillIsBoundedWhileReadingMerge(t *testing.T) {
	snapshot := durableScanCorpus(t, 512)
	exec := Exec{Options: ExecOptions{
		Workers:     2,
		MemoryBytes: 64 << 10,
		ResultRows:  2,
		ResultBytes: -1,
	}}
	err := Select(Path("note")).
		OrderBy("note", Desc).
		RunInto(&exec, FromFile(snapshot))
	if !errors.Is(err, ErrResultBudget) {
		t.Fatalf("ordered spill error = %v, want result budget", err)
	}
	if exec.Stats.SpillRuns == 0 {
		t.Fatal("ordered result did not exercise its spill merge")
	}
}

func TestDurableGroupedSpillLimitKeepsBoundedTopN(t *testing.T) {
	snapshot := durableScanCorpus(t, 512)
	exec := Exec{Options: ExecOptions{
		Workers:     2,
		MemoryBytes: 64 << 10,
		ResultRows:  2,
		ResultBytes: -1,
	}}
	err := Select(Path("id"), Count()).
		GroupBy("id").
		Limit(2).
		RunInto(&exec, FromFile(snapshot))
	if err != nil {
		t.Fatal(err)
	}
	if exec.Result.RowCount != 2 || exec.Stats.SpillRuns == 0 {
		t.Fatalf(
			"grouped top-N = %d rows, %d spill runs",
			exec.Result.RowCount, exec.Stats.SpillRuns,
		)
	}
}

func TestDurableSpillQuotaFailsBeforeDiskGrowth(t *testing.T) {
	snapshot := durableScanCorpus(t, 512)
	spillDirectory := t.TempDir()
	exec := Exec{Options: ExecOptions{
		Workers:        2,
		MemoryBytes:    64 << 10,
		SpillBytes:     1,
		SpillDirectory: spillDirectory,
		ResultRows:     -1,
		ResultBytes:    -1,
	}}
	err := Select(Path("note")).
		OrderBy("note", Desc).
		RunInto(&exec, FromFile(snapshot))
	if !errors.Is(err, ErrSpillBudget) {
		t.Fatalf("spill quota error = %v, want spill budget", err)
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("spill quota left %d temporary files", len(entries))
	}
}

func TestDurableUnorderedHugeLimitDoesNotOverflowTrimThreshold(t *testing.T) {
	snapshot := durableScanCorpus(t, 3)
	exec := Exec{Options: ExecOptions{
		ResultRows: -1, ResultBytes: -1,
	}}
	err := Select(Path("id")).
		Limit(int(^uint(0)>>1)).
		RunInto(&exec, FromFile(snapshot))
	if err != nil {
		t.Fatal(err)
	}
	if exec.Result.RowCount != 3 {
		t.Fatalf("rows = %d, want 3", exec.Result.RowCount)
	}
}

func TestSpillRemoveFailurePreservesLiveAccounting(t *testing.T) {
	path := t.TempDir() + "/run"
	if err := os.WriteFile(path, []byte("spill"), 0o600); err != nil {
		t.Fatal(err)
	}
	removeErr := errors.New("injected remove failure")
	manager := spillManager{
		files: map[string]struct{}{path: {}},
		live:  5,
		removeFile: func(string) error {
			return removeErr
		},
	}
	run := spillRun{path: path, size: 5}
	if err := manager.remove(run); !errors.Is(err, removeErr) {
		t.Fatalf("remove error = %v, want injected failure", err)
	}
	if manager.live != 5 {
		t.Fatalf("live bytes = %d, want 5", manager.live)
	}
	if _, ok := manager.files[path]; !ok {
		t.Fatal("failed removal was dropped from the live-run set")
	}
	if _, err := manager.cleanup(); !errors.Is(err, removeErr) {
		t.Fatalf("cleanup error = %v, want injected failure", err)
	}
	if manager.live != 5 {
		t.Fatalf("cleanup live bytes = %d, want 5", manager.live)
	}
}
