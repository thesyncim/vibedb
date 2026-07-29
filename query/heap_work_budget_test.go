package query

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestScalarGroupKeyBytesMatchesEncoding(t *testing.T) {
	tests := []struct {
		name string
		cell scalar
	}{
		{name: "null", cell: scalar{kind: kindNull}},
		{name: "false", cell: scalar{kind: kindBool}},
		{name: "true", cell: scalar{kind: kindBool, bval: true}},
		{name: "zero", cell: scalar{kind: kindNumber, num: []byte("0")}},
		{name: "negative zero", cell: scalar{kind: kindNumber, num: []byte("-0")}},
		{name: "integer", cell: scalar{kind: kindNumber, num: []byte("1234567890")}},
		{name: "fraction", cell: scalar{kind: kindNumber, num: []byte("-123.4500")}},
		{name: "compact exponent", cell: scalar{kind: kindNumber, num: []byte("1e100")}},
		{
			name: "wide positive exponent",
			cell: scalar{
				kind: kindNumber,
				num:  []byte("1e999999999999999999999999999999999999"),
			},
		},
		{
			name: "wide negative exponent",
			cell: scalar{
				kind: kindNumber,
				num:  []byte("-7e-999999999999999999999999999999999999"),
			},
		},
		{name: "empty string", cell: scalar{kind: kindString}},
		{
			name: "uvarint boundary string",
			cell: scalar{
				kind: kindString,
				sval: strings.Repeat("x", 128),
			},
		},
		{
			name: "container",
			cell: scalar{
				kind: kindContainer,
				raw:  []byte(`{"nested":[true,null,1]}`),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prefix := []byte{0xaa, 0xbb}
			encoded := appendGroupKey(prefix, tc.cell)
			got := int64(len(encoded) - len(prefix))
			if want := scalarGroupKeyBytes(tc.cell); got != want {
				t.Fatalf("encoded bytes = %d, scalarGroupKeyBytes = %d", got, want)
			}
		})
	}
}

func TestHeapGroupByLimitOneIsBoundedBeforeGroupGrowth(t *testing.T) {
	segment, snapshot := heapWorkBudgetCorpus(t, 512)
	q := Select(Path("id"), Count()).GroupBy("id").Limit(1)

	tests := []struct {
		name   string
		source Source
	}{
		{name: "segment", source: FromSegment(segment)},
		{name: "snapshot", source: FromSnapshot(snapshot)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := Exec{Options: ExecOptions{
				MemoryBytes: minimumHeapMemoryBytes,
				ResultRows:  -1,
				ResultBytes: -1,
			}}
			err := q.RunInto(&exec, tc.source)
			var budgetErr *WorkBudgetError
			if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) {
				t.Fatalf("GROUP BY LIMIT 1 error = %v, want WorkBudgetError", err)
			}
			if budgetErr.Bytes <= budgetErr.Limit {
				t.Fatalf("non-exhausted work budget error: %+v", budgetErr)
			}
			if len(exec.Workspace.groups) != 0 ||
				exec.Workspace.interner.Len() != 0 ||
				len(exec.Workspace.groupOrder) != 0 {
				t.Fatalf(
					"rejected grouping grew workspace: groups=%d keys=%d order=%d",
					len(exec.Workspace.groups),
					exec.Workspace.interner.Len(),
					len(exec.Workspace.groupOrder),
				)
			}
			if exec.Result.RowCount != 0 {
				t.Fatalf("rejected grouping exposed %d result rows", exec.Result.RowCount)
			}
		})
	}
}

func TestHeapWideGroupKeyIsRejectedBeforeScratchGrowth(t *testing.T) {
	var segment store.Segment
	document := fmt.Appendf(
		nil,
		`{"g":%q}`,
		strings.Repeat("x", int(minimumHeapMemoryBytes)),
	)
	if _, err := segment.Append(document); err != nil {
		t.Fatal(err)
	}
	q := Select(Path("g"), Count()).GroupBy("g")
	exec := Exec{Options: ExecOptions{
		MemoryBytes: minimumHeapMemoryBytes,
		ResultRows:  -1,
		ResultBytes: -1,
	}}
	err := q.RunInto(&exec, FromSegment(&segment))
	var budgetErr *WorkBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) {
		t.Fatalf("wide GROUP BY key error = %v, want WorkBudgetError", err)
	}
	if len(exec.Workspace.groupKey) != 0 || cap(exec.Workspace.groupKey) != 0 {
		t.Fatalf(
			"rejected GROUP BY key grew scratch len=%d cap=%d",
			len(exec.Workspace.groupKey),
			cap(exec.Workspace.groupKey),
		)
	}
}

func TestHeapOrderByLimitOneIsBoundedBeforeColumnGrowth(t *testing.T) {
	segment, snapshot := heapWorkBudgetCorpus(t, 512)
	q := Select(Path("id")).OrderBy("id", Asc).Limit(1)

	tests := []struct {
		name   string
		source Source
	}{
		{name: "segment", source: FromSegment(segment)},
		{name: "snapshot", source: FromSnapshot(snapshot)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := Exec{Options: ExecOptions{
				MemoryBytes: minimumHeapMemoryBytes,
				ResultRows:  -1,
				ResultBytes: -1,
			}}
			err := q.RunInto(&exec, tc.source)
			var budgetErr *WorkBudgetError
			if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) {
				t.Fatalf("ORDER BY LIMIT 1 error = %v, want WorkBudgetError", err)
			}
			if budgetErr.Bytes <= budgetErr.Limit {
				t.Fatalf("non-exhausted work budget error: %+v", budgetErr)
			}
			if len(exec.Workspace.raws) != 0 ||
				len(exec.Workspace.ctx.values) != 0 ||
				len(exec.Workspace.selected) != 0 {
				t.Fatalf(
					"rejected ordering grew workspace: raws=%d values=%d selected=%d",
					len(exec.Workspace.raws),
					len(exec.Workspace.ctx.values),
					len(exec.Workspace.selected),
				)
			}
			if exec.Result.RowCount != 0 {
				t.Fatalf("rejected ordering exposed %d result rows", exec.Result.RowCount)
			}
		})
	}
}

func TestHeapLimitZeroDoesNotReserveScanWorkspace(t *testing.T) {
	segment, snapshot := heapWorkBudgetCorpus(t, 512)
	q := Select(Path("id")).OrderBy("id", Asc).Limit(0)
	for _, source := range []Source{FromSegment(segment), FromSnapshot(snapshot)} {
		exec := Exec{Options: ExecOptions{
			MemoryBytes: minimumHeapMemoryBytes,
			ResultRows:  -1,
			ResultBytes: -1,
		}}
		if err := q.RunInto(&exec, source); err != nil {
			t.Fatal(err)
		}
		if exec.Result.RowCount != 0 ||
			len(exec.Workspace.raws) != 0 ||
			len(exec.Workspace.selected) != 0 {
			t.Fatalf(
				"LIMIT 0 retained rows=%d raws=%d selected=%d",
				exec.Result.RowCount,
				len(exec.Workspace.raws),
				len(exec.Workspace.selected),
			)
		}
	}
}

func TestHeapRowAdmissionTracksRequiredBytesAcrossWorkerWidths(t *testing.T) {
	q := Select(Path("g")).Where(Exists("g"))
	compiled, err := q.compiled()
	if err != nil {
		t.Fatal(err)
	}
	const rows = 1024
	oneWorker := heapRowFixedBytes(compiled, rows, heapWorkSnapshot, 1)
	fourWorkers := heapRowFixedBytes(compiled, rows, heapWorkSnapshot, 4)
	if fourWorkers <= oneWorker {
		t.Fatalf(
			"fixture worker workspace did not grow: one=%d four=%d",
			oneWorker,
			fourWorkers,
		)
	}
	var budget heapWorkBudget
	budget.begin(fourWorkers - 1)
	if err := budget.admitRows(
		compiled, rows, heapWorkSnapshot, 1,
	); err != nil {
		t.Fatalf("one-worker admission: %v", err)
	}
	err = budget.admitRows(compiled, rows, heapWorkSnapshot, 4)
	var budgetErr *WorkBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) {
		t.Fatalf("wider worker admission error = %v, want WorkBudgetError", err)
	}
	if budget.rowBytes != oneWorker {
		t.Fatalf(
			"rejected width replaced admitted high-water: got %d want %d",
			budget.rowBytes,
			oneWorker,
		)
	}
}

func TestHeapNonPrimarySemiJoinIsBoundedBeforeMembershipGrowth(t *testing.T) {
	var database store.Database
	outer, err := database.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := database.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outer.Put(
		"outer-0",
		[]byte(`{"id":"outer-0","join":"row-000000"}`),
	); err != nil {
		t.Fatal(err)
	}
	for i := range 512 {
		document := fmt.Appendf(
			nil,
			`{"id":"inner-%06d","join":"row-%06d"}`,
			i,
			i,
		)
		if _, err := inner.Put(fmt.Sprintf("inner-%06d", i), document); err != nil {
			t.Fatal(err)
		}
	}

	q := Select(Path("id")).
		Join(JoinOn("inner", "join", "join"))
	exec := Exec{Options: ExecOptions{
		MemoryBytes: minimumHeapMemoryBytes,
		ResultRows:  -1,
		ResultBytes: -1,
	}}
	err = q.RunInto(&exec, FromDatabase(database.Snapshot(), "outer"))
	var budgetErr *WorkBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) {
		t.Fatalf("non-primary semi-join error = %v, want WorkBudgetError", err)
	}
	if len(exec.Workspace.joins) != 1 {
		t.Fatalf("join bindings = %d, want 1", len(exec.Workspace.joins))
	}
	binding := &exec.Workspace.joins[0]
	if len(binding.lits) >= 512 {
		t.Fatalf(
			"semi-join membership was not bounded before completion: lits=%d text=%d",
			len(binding.lits),
			len(binding.text),
		)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("rejected semi-join exposed %d result rows", exec.Result.RowCount)
	}
}

func TestHeapSemiJoinMembershipPayloadIsAdmittedBeforeAppend(t *testing.T) {
	var database store.Database
	outer, err := database.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := database.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	first := "000000-" + strings.Repeat("x", 512)
	if _, err := outer.Put(
		"outer-0",
		fmt.Appendf(nil, `{"id":"outer-0","join":%q}`, first),
	); err != nil {
		t.Fatal(err)
	}
	for i := range 64 {
		value := fmt.Sprintf("%06d-%s", i, strings.Repeat("x", 512))
		document := fmt.Appendf(
			nil,
			`{"id":"inner-%06d","join":%q}`,
			i,
			value,
		)
		if _, err := inner.Put(fmt.Sprintf("inner-%06d", i), document); err != nil {
			t.Fatal(err)
		}
	}

	q := Select(Path("id")).Join(JoinOn("inner", "join", "join"))
	exec := Exec{Options: ExecOptions{
		MemoryBytes: minimumHeapMemoryBytes,
		ResultRows:  -1,
		ResultBytes: -1,
	}}
	err = q.RunInto(&exec, FromDatabase(database.Snapshot(), "outer"))
	var budgetErr *WorkBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) {
		t.Fatalf("wide semi-join error = %v, want WorkBudgetError", err)
	}
	binding := &exec.Workspace.joins[0]
	if len(binding.lits) == 0 || len(binding.lits) >= 64 {
		t.Fatalf(
			"wide membership was not stopped at an admitted prefix: lits=%d text=%d",
			len(binding.lits),
			len(binding.text),
		)
	}
}

func TestJoinMembershipTextGrowthRebasesEveryBorrowedView(t *testing.T) {
	var budget heapWorkBudget
	budget.begin(1 << 20)
	var binding joinBinding
	values := make([]string, 0, 96)
	for i := range 96 {
		value := fmt.Sprintf("%03d-%s", i, strings.Repeat("x", i%17+1))
		values = append(values, value)
		if err := binding.appendString(value, &budget); err != nil {
			t.Fatalf("append member %d: %v", i, err)
		}
	}
	if binding.textReserved <= int64(cap(binding.text)) {
		t.Fatalf(
			"fixture did not exercise an admitted old+new growth peak: reserved=%d cap=%d",
			binding.textReserved,
			cap(binding.text),
		)
	}

	// Mutating the final arena is a test-only pointer-identity probe. Every
	// string must observe it; a scalar left on an abandoned generation would
	// retain the old byte instead.
	offset := 0
	for i, value := range values {
		original := binding.text[offset]
		binding.text[offset] = '!'
		if binding.lits[i].sval[0] != '!' {
			t.Fatalf("member %d still views an abandoned text generation", i)
		}
		binding.text[offset] = original
		if binding.lits[i].sval != value {
			t.Fatalf(
				"member %d = %q after rebase, want %q",
				i,
				binding.lits[i].sval,
				value,
			)
		}
		offset += len(value)
	}
}

func TestJoinBloomBudgetRejectsBeforeGrowth(t *testing.T) {
	var budget heapWorkBudget
	budget.begin(minimumHeapMemoryBytes)
	var filter joinBloom
	err := budget.resetJoinBloom(&filter, int(^uint(0)>>1))
	var budgetErr *WorkBudgetError
	if !errors.As(err, &budgetErr) || !errors.Is(err, ErrWorkBudget) {
		t.Fatalf("maximal Bloom filter error = %v, want WorkBudgetError", err)
	}
	if len(filter.blocks) != 0 || cap(filter.blocks) != 0 || filter.active {
		t.Fatalf(
			"rejected Bloom filter grew blocks len=%d cap=%d active=%v",
			len(filter.blocks),
			cap(filter.blocks),
			filter.active,
		)
	}
}

func TestJoinBuildBucketSizingRejectsIntegerAndChainOverflow(t *testing.T) {
	if buckets, err := joinBuildBuckets(5); err != nil || buckets != 16 {
		t.Fatalf("joinBuildBuckets(5) = (%d, %v), want (16, nil)", buckets, err)
	}
	maxInt := int(^uint(0) >> 1)
	if buckets, err := joinBuildBuckets(maxInt); err == nil || buckets != 0 {
		t.Fatalf(
			"joinBuildBuckets(MaxInt) = (%d, %v), want a fail-closed error",
			buckets,
			err,
		)
	}
}

func TestHeapPrimaryJoinFallsBackWhenMembershipExceedsBudget(t *testing.T) {
	var database store.Database
	outer, err := database.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := database.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	first := "000000-" + strings.Repeat("k", 512)
	if _, err := outer.Put(
		"outer-0",
		fmt.Appendf(nil, `{"id":"outer-0","ref":%q}`, first),
	); err != nil {
		t.Fatal(err)
	}
	for i := range 64 {
		key := fmt.Sprintf("%06d-%s", i, strings.Repeat("k", 512))
		if _, err := inner.Put(key, []byte(`{"active":true}`)); err != nil {
			t.Fatal(err)
		}
	}

	q := Select(Path("id")).Join(JoinOn("inner", "ref", JoinKey))
	exec := Exec{Options: ExecOptions{
		MemoryBytes:         minimumHeapMemoryBytes,
		JoinMembershipMax:   1 << 20,
		JoinFilterScanRatio: -1,
		ResultRows:          -1,
		ResultBytes:         -1,
	}}
	if err := q.RunInto(&exec, FromDatabase(database.Snapshot(), "outer")); err != nil {
		t.Fatalf("primary-key fallback: %v", err)
	}
	if exec.Result.RowCount != 1 || exec.Stats.JoinLookups != 1 {
		t.Fatalf(
			"primary-key fallback rows=%d stats=%+v, want one lookup match",
			exec.Result.RowCount,
			exec.Stats,
		)
	}
	binding := &exec.Workspace.joins[0]
	if cap(binding.lits) != 0 ||
		cap(binding.text) != 0 ||
		cap(binding.rows) != 0 ||
		cap(binding.scan.raws) != 0 ||
		cap(binding.bloom.blocks) != 0 {
		t.Fatalf(
			"fallback retained abandoned build capacities: lits=%d text=%d rows=%d scan=%d bloom=%d",
			cap(binding.lits),
			cap(binding.text),
			cap(binding.rows),
			cap(binding.scan.raws),
			cap(binding.bloom.blocks),
		)
	}
	if used := exec.Workspace.heapWorkBudget.used.Load(); used > minimumHeapMemoryBytes {
		t.Fatalf("fallback budget used %d > %d", used, minimumHeapMemoryBytes)
	}
}

func TestHeapIndexedJoinDeclinesNeedlesWhenOnlyAccelerationExceedsBudget(t *testing.T) {
	database := joinScaleDatabase(t, 1, 128, 128, true)
	catalog := database.Snapshot()
	outer, ok := catalog.Collection("orders")
	if !ok {
		t.Fatal("orders missing from database snapshot")
	}
	indexes := outer.AppendIndexes(nil)
	if _, ok := singleColumnIndex("/customer", indexes); !ok {
		t.Fatal("outer join-path index is not ready; fixture is vacuous")
	}

	// Joining on the ordinary name field forces an exact membership: unlike a
	// primary-key join, it has no lookup fallback that could hide a rejected
	// build. The membership fits 64 KiB; rendering one exact-index needle per
	// member does not, so execution must retain the sound set and decline only
	// that optional candidate-pushdown acceleration.
	q := Select(Path("id")).Join(JoinOn("customers", "customer", "name"))
	exec := Exec{Options: ExecOptions{
		MemoryBytes:       minimumHeapMemoryBytes + 2<<10,
		JoinMembershipMax: 1 << 20,
		ResultRows:        -1,
		ResultBytes:       -1,
	}}
	if err := q.RunInto(&exec, FromDatabase(catalog, "orders")); err != nil {
		t.Fatalf("tight-budget indexed join: %v", err)
	}
	if exec.Result.RowCount != 1 || exec.Stats.JoinMemberships != 1 {
		t.Fatalf(
			"rows=%d stats=%+v, want one row from an exact membership",
			exec.Result.RowCount,
			exec.Stats,
		)
	}
	binding := &exec.Workspace.joins[0]
	if binding.mode != joinBindSet || len(binding.lits) != 128 {
		t.Fatalf(
			"binding mode=%v members=%d, want a complete 128-key set",
			binding.mode,
			len(binding.lits),
		)
	}
	if len(binding.needles) != 0 ||
		len(binding.needleText) != 0 ||
		len(binding.starts) != 0 ||
		len(binding.entries) != 0 {
		t.Fatalf(
			"declined acceleration grew needles=%d text=%d starts=%d entries=%d",
			len(binding.needles),
			len(binding.needleText),
			len(binding.starts),
			len(binding.entries),
		)
	}

	// Prove that the only reason the production binding has no needles is the
	// work budget: the same finished membership and ready index render every
	// needle when the optional builder has no budget account.
	// Construct only the input view buildNeedles reads. Copying joinBinding
	// would also copy its nested Workspace and atomic budget state, which is
	// both invalid and unrelated to this test.
	unbounded := joinBinding{lits: binding.lits}
	if err := unbounded.buildNeedles("/customer", indexes, nil); err != nil {
		t.Fatalf("unbounded needle build: %v", err)
	}
	if len(unbounded.needles) != len(binding.lits) {
		t.Fatalf(
			"unbounded needle build produced %d needles for %d members",
			len(unbounded.needles),
			len(binding.lits),
		)
	}
}

func TestReusedExecClearsUnusedJoinBorrowedViews(t *testing.T) {
	var database store.Database
	outer, err := database.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := database.CreateCollection("first", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := database.CreateCollection("second", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outer.Put(
		"outer-0",
		[]byte(`{"id":"outer-0","a":"x","b":"y"}`),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Put("first-0", []byte(`{"v":"x"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Put("second-0", []byte(`{"v":"y"}`)); err != nil {
		t.Fatal(err)
	}
	catalog := database.Snapshot()
	driving, ok := catalog.Collection("outer")
	if !ok {
		t.Fatal("outer missing from database snapshot")
	}

	two := Select(Path("id")).
		Join(JoinOn("first", "a", "v")).
		Join(JoinOn("second", "b", "v"))
	one := Select(Path("id")).Join(JoinOn("first", "a", "v"))
	unjoined := Select(Path("id"))
	var exec Exec
	if err := two.RunInto(&exec, FromDatabase(catalog, "outer")); err != nil {
		t.Fatalf("two joins: %v", err)
	}
	if len(exec.Workspace.joins) != 2 ||
		exec.Workspace.joins[1].snapshot.Len() == 0 {
		t.Fatal("second binding was not populated; retention test is vacuous")
	}

	if err := one.RunInto(&exec, FromDatabase(catalog, "outer")); err != nil {
		t.Fatalf("one join: %v", err)
	}
	assertJoinBindingCleared(t, &exec.Workspace.joins[1])
	if len(exec.Workspace.eval.binds) != 1 {
		t.Fatalf("active evaluator bindings = %d, want 1", len(exec.Workspace.eval.binds))
	}

	if err := unjoined.RunInto(&exec, FromSnapshot(driving)); err != nil {
		t.Fatalf("unjoined: %v", err)
	}
	for i := range exec.Workspace.joins {
		assertJoinBindingCleared(t, &exec.Workspace.joins[i])
	}
	if exec.Workspace.eval.binds != nil {
		t.Fatalf("unjoined evaluator retained %d bindings", len(exec.Workspace.eval.binds))
	}
}

func assertJoinBindingCleared(t *testing.T, binding *joinBinding) {
	t.Helper()
	if binding.snapshot.Len() != 0 ||
		binding.file.snapshot != nil ||
		binding.plan != nil ||
		len(binding.lits) != 0 ||
		len(binding.needles) != 0 ||
		len(binding.build.values) != 0 {
		t.Fatalf("unused binding retained logical state: %+v", binding)
	}
	for i, raws := range binding.scan.raws {
		if len(raws) != 0 {
			t.Fatalf("unused binding raw column %d retained %d views", i, len(raws))
		}
	}
	for i, values := range binding.scan.ctx.values {
		if len(values) != 0 {
			t.Fatalf("unused binding scalar column %d retained %d views", i, len(values))
		}
	}
	for i, nums := range binding.scan.ctx.nums {
		if len(nums.vals) != 0 {
			t.Fatalf("unused binding numeric column %d retained %d views", i, len(nums.vals))
		}
	}
}

func TestReusedExecClearsParkedWorkerJoinProbes(t *testing.T) {
	var database store.Database
	outer, err := database.CreateCollection("outer", store.Options{ChunkDocuments: 1})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := database.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		document := fmt.Appendf(
			nil,
			`{"id":"outer-%d","ref":"inner-%d"}`,
			i,
			i%2,
		)
		if _, err := outer.Put(fmt.Sprintf("outer-%d", i), document); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 2 {
		if _, err := inner.Put(
			fmt.Sprintf("inner-%d", i),
			[]byte(`{"keep":"yes"}`),
		); err != nil {
			t.Fatal(err)
		}
	}
	catalog := database.Snapshot()
	driving, ok := catalog.Collection("outer")
	if !ok {
		t.Fatal("outer missing from database snapshot")
	}

	joined := Select(Path("id")).Join(
		JoinOn("inner", "ref", JoinKey).Where(Cmp("keep", Eq, "yes")),
	)
	unjoined := Select(Path("id")).Where(Exists("id"))
	exec := Exec{Options: ExecOptions{
		Workers:             4,
		JoinMembershipMax:   1,
		JoinFilterScanRatio: -1,
	}}
	if err := joined.RunInto(&exec, FromDatabase(catalog, "outer")); err != nil {
		t.Fatalf("joined: %v", err)
	}
	if exec.Workspace.pool == nil || len(exec.Workspace.pool.workers) != 4 {
		t.Fatal("parallel joined execution did not park four workers")
	}
	borrowed := 0
	for i := range exec.Workspace.pool.workers {
		for _, probe := range exec.Workspace.pool.workers[i].eval.probes {
			for _, column := range probe.cols {
				for _, cell := range column {
					if len(cell.raw) != 0 || len(cell.sval) != 0 {
						borrowed++
					}
				}
			}
		}
	}
	if borrowed == 0 {
		probeCounts := make([]int, len(exec.Workspace.pool.workers))
		for i := range exec.Workspace.pool.workers {
			probeCounts[i] = len(exec.Workspace.pool.workers[i].eval.probes)
		}
		t.Fatalf(
			"no parked probe borrowed inner scalars; retention test is vacuous: probes=%v stats=%+v mode=%v",
			probeCounts,
			exec.Stats,
			exec.Workspace.joins[0].mode,
		)
	}

	exec.Options.Workers = 1
	if err := unjoined.RunInto(&exec, FromSnapshot(driving)); err != nil {
		t.Fatalf("unjoined: %v", err)
	}
	for worker := range exec.Workspace.pool.workers {
		scan := &exec.Workspace.pool.workers[worker]
		eval := &scan.eval
		if len(eval.probes) != 0 || eval.binds != nil {
			t.Fatalf(
				"worker %d retained logical probes=%d binds=%d",
				worker,
				len(eval.probes),
				len(eval.binds),
			)
		}
		for probe := range eval.probes[:cap(eval.probes)] {
			for column, cells := range eval.probes[:cap(eval.probes)][probe].cols {
				for row, cell := range cells {
					if len(cell.raw) != 0 ||
						len(cell.num) != 0 ||
						len(cell.sval) != 0 {
						t.Fatalf(
							"worker %d probe %d column %d row %d retained an inner scalar",
							worker,
							probe,
							column,
							row,
						)
					}
				}
			}
		}
		for column, raws := range scan.raws {
			if len(raws) != 0 {
				t.Fatalf(
					"worker %d raw column %d retained %d logical views",
					worker,
					column,
					len(raws),
				)
			}
			for row, raw := range raws[:cap(raws)] {
				if raw.Src != nil {
					t.Fatalf(
						"worker %d raw column %d row %d retained source bytes",
						worker,
						column,
						row,
					)
				}
			}
		}
	}
}

func TestHeapSelectiveIndexChargesCandidateRowsNotUniverse(t *testing.T) {
	collection, err := store.New(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 512 {
		document := fmt.Appendf(nil, `{"id":"row-%06d","payload":%d}`, i, i)
		if _, err := collection.Put(fmt.Sprintf("key-%06d", i), document); err != nil {
			t.Fatal(err)
		}
	}
	info, err := collection.CreateIndex(store.IndexDefinition{
		Name: "id",
		Paths: []string{
			"/id",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for info.State != store.IndexReady {
		info, err = collection.BackfillIndex(info.Name, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	q := Select(Path("payload")).
		Where(Cmp("id", Eq, "row-000001")).
		OrderBy("payload", Asc).
		Limit(1)
	exec := Exec{Options: ExecOptions{
		MemoryBytes: minimumHeapMemoryBytes,
		ResultRows:  -1,
		ResultBytes: -1,
	}}
	if err := q.RunInto(&exec, FromSnapshot(snapshot)); err != nil {
		t.Fatalf("selective indexed query: %v", err)
	}
	if exec.Result.RowCount != 1 {
		t.Fatalf("selective indexed rows = %d, want 1", exec.Result.RowCount)
	}
	if got := exec.Workspace.heapWorkBudget.rowRows; got != 1 {
		t.Fatalf("admitted materialized rows = %d, want candidate count 1", got)
	}
}

func TestHeapSelectiveInnerJoinFilterChargesSurvivors(t *testing.T) {
	var database store.Database
	outer, err := database.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := database.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outer.Put(
		"outer-0",
		[]byte(`{"id":"outer-0","join":"row-000000"}`),
	); err != nil {
		t.Fatal(err)
	}
	for i := range 512 {
		document := fmt.Appendf(
			nil,
			`{"id":"inner-%06d","join":"row-%06d","keep":%t}`,
			i,
			i,
			i == 0,
		)
		if _, err := inner.Put(fmt.Sprintf("inner-%06d", i), document); err != nil {
			t.Fatal(err)
		}
	}
	// The zone-summary pruning tier that once bounded a non-indexed inner
	// filter is gone; an exact index is the surviving mechanism that lets the
	// candidate planner narrow the inner universe to its survivors before the
	// join build materializes a batch. Without it the build would have to
	// admit the whole 512-row universe, which is the honest no-summaries cost.
	info, err := inner.CreateIndex(store.IndexDefinition{
		Name:  "keep",
		Paths: []string{"/keep"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for info.State != store.IndexReady {
		info, err = inner.BackfillIndex(info.Name, 0)
		if err != nil {
			t.Fatal(err)
		}
	}

	q := Select(Path("id")).Join(
		JoinOn("inner", "join", "join").Where(Cmp("keep", Eq, true)),
	)
	exec := Exec{Options: ExecOptions{
		MemoryBytes: minimumHeapMemoryBytes,
		ResultRows:  -1,
		ResultBytes: -1,
	}}
	if err := q.RunInto(&exec, FromDatabase(database.Snapshot(), "outer")); err != nil {
		t.Fatalf("selective inner filter: %v", err)
	}
	if exec.Result.RowCount != 1 {
		t.Fatalf("selective inner filter rows = %d, want 1", exec.Result.RowCount)
	}
	if got := len(exec.Workspace.joins[0].lits); got != 1 {
		t.Fatalf("selective inner membership = %d, want 1", got)
	}
}

func heapWorkBudgetCorpus(t *testing.T, rows int) (*store.Segment, store.Snapshot) {
	t.Helper()
	segment := new(store.Segment)
	collection, err := store.New(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		document := fmt.Appendf(nil, `{"id":"row-%06d","payload":%d}`, i, i)
		if _, err := segment.Append(document); err != nil {
			t.Fatal(err)
		}
		if _, err := collection.Put(fmt.Sprintf("key-%06d", i), document); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return segment, snapshot
}
