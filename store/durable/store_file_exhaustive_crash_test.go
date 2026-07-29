package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// Exhaustive commit-crash-point sweep.
//
// TestFileStoreFreeSetSurvivesCrashAtEveryWritePoint and its neighbours tear a
// commit by copying byte prefixes of a clean image; this sweep instead drives a
// real workload through a fault-injecting Device that reproduces the portable
// device's commit sequence write by write and stops it at an exact point. The
// on-disk image is therefore the bytes a crash there would truly have left,
// including a barrier that did or did not run and a root that is whole, torn, or
// absent.
//
// The device enumerates, for every Device.Commit the workload issues, a crash
// after each individual data-page write, after the data barrier, after the root
// write, and after the final sync; plus a torn root prefix, a dropped write
// with later writes still applied (the bounded pre-barrier reordering the
// contract allows), and ENOSPC on a data write, on the root write, and on the
// first write that grows the file.
//
// For every induced image the assertion is exactly the durability contract:
// reopen either fails closed with an error, or yields a store whose full
// contents equal a legal prefix state — the content oracle captured at each
// checkpoint boundary of a clean probe pass — and never a third value, never a
// panic. A crash inside the commits of one checkpoint may only recover to the
// content just before or just after that checkpoint, because each checkpoint
// mutates exactly one key.

// crashOp is one workload mutation. An empty value is a delete.
type crashOp struct {
	label string
	key   string
	value string
}

func exhaustiveWorkloadOps() []crashOp {
	charlie := `{"k":"charlie","pad":"` + strings.Repeat("z", 1800) + `"}`
	return []crashOp{
		{"put-alpha", "alpha", `{"k":"alpha","n":1}`},
		{"put-bravo", "bravo", `{"k":"bravo","n":2}`},
		{"put-charlie", "charlie", charlie},
		{"del-alpha", "alpha", ""},
		{"put-delta", "delta", `{"k":"delta","n":4}`},
	}
}

// faultController hands the store-committer seam a fault-injecting Device and
// keeps a handle to it for the driver.
type faultController struct {
	plan   storeio.FaultPlan
	device *storeio.FaultDevice
}

func (fc *faultController) factory() func(*os.File, storeio.DeviceOptions, storeio.CommitterOptions) (*storeio.Committer, error) {
	return func(file *os.File, dopts storeio.DeviceOptions, copts storeio.CommitterOptions) (*storeio.Committer, error) {
		return storeio.NewCommitterWithDevice(
			file, dopts, copts,
			func(f *os.File, o storeio.DeviceOptions) (storeio.Device, error) {
				dev, err := storeio.OpenFaultDevice(f, o)
				if err != nil {
					return nil, err
				}
				dev.Program(fc.plan)
				fc.device = dev
				return dev, nil
			},
		)
	}
}

type faultRunResult struct {
	image      []byte
	faulted    bool
	createErr  error
	records    []storeio.FaultCommitRecord
	boundaries []int
	contents   []map[string]string
}

// runFaultWorkload runs the deterministic workload once with plan installed. It
// records the commit boundaries and content oracle on the clean probe pass, and
// snapshots the on-disk image at the induced crash on a fault pass.
func runFaultWorkload(t *testing.T, options Options, fc *faultController, plan storeio.FaultPlan, ops []crashOp) faultRunResult {
	t.Helper()
	prev := storeCommitterFactory
	storeCommitterFactory = fc.factory()
	defer func() { storeCommitterFactory = prev }()

	fc.plan = plan
	fc.device = nil

	path := filepath.Join(t.TempDir(), "store")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	coll, createErr := Create(file, options)
	if createErr != nil {
		// The initial-state commit (ordinal 0) faulted, or construction failed.
		image, _ := os.ReadFile(path)
		faulted := fc.device != nil && fc.device.Faulted()
		_ = file.Close()
		return faultRunResult{image: image, faulted: faulted, createErr: createErr}
	}
	dev := fc.device
	if dev == nil {
		_ = coll.Close()
		_ = file.Close()
		t.Fatal("fault device was not installed by the committer factory")
	}

	res := faultRunResult{
		boundaries: []int{dev.Commits()},
		contents:   []map[string]string{snapshotCollectionContent(t, coll)},
	}
	for _, op := range ops {
		var opErr error
		if op.value == "" {
			_, opErr = coll.Delete([]byte(op.key))
		} else {
			_, opErr = coll.Put([]byte(op.key), []byte(op.value))
		}
		if opErr == nil {
			opErr = coll.Flush()
		}
		if dev.Faulted() {
			break
		}
		if opErr != nil {
			_ = coll.Close()
			_ = file.Close()
			t.Fatalf("%s: unexpected workload error before any fault: %v", op.label, opErr)
		}
		res.boundaries = append(res.boundaries, dev.Commits())
		res.contents = append(res.contents, snapshotCollectionContent(t, coll))
	}
	res.faulted = dev.Faulted()
	res.records = dev.Records()
	res.image, _ = os.ReadFile(path)
	_ = coll.Close()
	_ = file.Close()
	return res
}

func snapshotCollectionContent(t *testing.T, coll *Collection) map[string]string {
	t.Helper()
	snap, err := coll.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	content := map[string]string{}
	if err := snap.RangeRaw(func(key, value []byte) error {
		content[string(key)] = string(value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return content
}

// expectedStates returns the content states a successful reopen of the crash
// image may hold. boundaries[0] is the commit count after Create; boundaries[i+1]
// the count after workload op i; contents is the content oracle captured at each
// of those boundaries.
//
// A crash before a complete, synced root is published must never expose the new
// generation: when the checkpoint is a single commit its only prior state is the
// pre-checkpoint content, so a pre-root crash may recover to that content alone
// (or fail closed). A crash at or after the root write may recover to either
// side, because the just-written root bytes may or may not have survived. A nil
// result means only a fail-closed reopen is legal — a half-created store.
func expectedStates(commit int, phase storeio.FaultPhase, boundaries []int, contents []map[string]string) []map[string]string {
	rootOrAfter := phase == storeio.FaultAfterRootWrite || phase == storeio.FaultAfterFinalSync
	if commit < boundaries[0] {
		if rootOrAfter {
			return []map[string]string{contents[0]}
		}
		return nil
	}
	for j := 0; j+1 < len(boundaries); j++ {
		if commit >= boundaries[j] && commit < boundaries[j+1] {
			before, after := contents[j], contents[j+1]
			if rootOrAfter || boundaries[j+1]-boundaries[j] != 1 {
				return []map[string]string{before, after}
			}
			return []map[string]string{before}
		}
	}
	return []map[string]string{contents[len(contents)-1]}
}

// verifyCrashImage reopens one crash image and classifies the outcome. It fails
// the test on a panic or on a successful open whose contents match no legal
// prefix state (corruption reported as success). Fail-closed and read-time
// errors are legal outcomes of the durability contract.
func verifyCrashImage(t *testing.T, options Options, image []byte, legal []map[string]string, label string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "recovered")
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var (
		coll     *Collection
		openErr  error
		content  = map[string]string{}
		readErr  error
		panicked bool
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				t.Errorf("%s: PANIC during recovery: %v", label, r)
			}
		}()
		coll, openErr = Open(file, options)
		if openErr != nil {
			return
		}
		snap, snapErr := coll.Snapshot()
		if snapErr != nil {
			readErr = snapErr
			return
		}
		readErr = snap.RangeRaw(func(key, value []byte) error {
			content[string(key)] = string(value)
			return nil
		})
		snap.Close()
	}()
	if coll != nil {
		_ = coll.Close()
	}
	switch {
	case panicked:
		return "panic"
	case openErr != nil:
		return "fail-closed"
	case readErr != nil:
		// Opened, then reported an error while reading. The caller learns not to
		// trust the state, so no wrong value is served: a legal fail-closed.
		return "read-error"
	}
	for _, want := range legal {
		if mapsEqual(content, want) {
			return "recovered"
		}
	}
	if len(legal) == 0 {
		t.Errorf("%s: reopen of a half-created image succeeded with content %v; it must fail closed "+
			"(corruption reported as success)", label, content)
	} else {
		t.Errorf("%s: Open succeeded but recovered content %v is not a legal prefix state %v "+
			"(corruption reported as success)", label, content, legal)
	}
	return "hole"
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func TestFileStoreExhaustiveCommitCrashSweep(t *testing.T) {
	options := testFileStoreOptions()
	// One generation per device commit and no coalescing so the Nth commit is
	// the same logical write on every replay of the workload.
	options.GroupLimit = 1
	options.CommitCoalesce = 0

	ops := exhaustiveWorkloadOps()
	fc := &faultController{}

	// Clean probe pass: record every commit's write plan and the content oracle
	// at each checkpoint boundary.
	probe := runFaultWorkload(t, options, fc, storeio.FaultPlan{Phase: storeio.FaultNone}, ops)
	if probe.createErr != nil {
		t.Fatalf("probe workload failed to create: %v", probe.createErr)
	}
	if probe.faulted {
		t.Fatal("probe workload faulted with no plan installed")
	}
	records := probe.records
	boundaries := probe.boundaries
	contents := probe.contents
	total := boundaries[len(boundaries)-1]
	if total != len(records) {
		t.Fatalf("commit boundary %d disagrees with %d recorded commits", total, len(records))
	}

	// Enumerate the crash points from the recorded write plans.
	type crashPoint struct {
		commit int
		plan   storeio.FaultPlan
		name   string
	}
	var points []crashPoint
	add := func(commit int, phase storeio.FaultPhase, index int, name string) {
		points = append(points, crashPoint{
			commit: commit,
			plan:   storeio.FaultPlan{Commit: commit, Phase: phase, DataIndex: index},
			name:   fmt.Sprintf("commit%d/%s", commit, name),
		})
	}
	for k, rec := range records {
		for i := range rec.DataPages {
			add(k, storeio.FaultAfterDataWrite, i, fmt.Sprintf("after-data-%d", i))
		}
		add(k, storeio.FaultAfterBarrier, 0, "after-barrier")
		add(k, storeio.FaultAfterRootWrite, 0, "after-root")
		add(k, storeio.FaultAfterFinalSync, 0, "after-final-sync")
		add(k, storeio.FaultTornRoot, 0, "torn-root")
		if len(rec.DataPages) >= 2 {
			add(k, storeio.FaultDropDataThenApply, 0, "drop-data-0-apply-rest")
		}
		nonGrowth, growth := -1, false
		for i, w := range rec.DataPages {
			if !w.Grows && nonGrowth < 0 {
				nonGrowth = i
			}
			if w.Grows {
				growth = true
			}
		}
		if nonGrowth >= 0 {
			add(k, storeio.FaultENOSPCData, nonGrowth, "enospc-data")
		}
		add(k, storeio.FaultENOSPCRoot, 0, "enospc-root")
		if growth || rec.Root.Grows {
			add(k, storeio.FaultENOSPCGrowth, 0, "enospc-growth")
		}
	}

	tally := map[string]int{}
	for _, pt := range points {
		run := runFaultWorkload(t, options, fc, pt.plan, ops)
		if !run.faulted {
			t.Errorf("%s: programmed crash never fired (commit structure diverged)", pt.name)
			tally["not-fired"]++
			continue
		}
		legal := expectedStates(pt.commit, pt.plan.Phase, boundaries, contents)
		tally[verifyCrashImage(t, options, run.image, legal, pt.name)]++
	}

	t.Logf("workload: %d device commits across %d checkpoint boundaries", total, len(boundaries))
	t.Logf("exhaustive crash-point sweep: %d points exercised; outcomes %v", len(points), tally)
}

// primarySplitCrashKey and primarySplitCrashValue drive a single seed tablet from
// one leaf to a split. The ~900-byte value fills a wide leaf in a handful of
// inserts, so the split -- and the buffered structural transaction it runs --
// lands within a small, fast crash sweep.
func primarySplitCrashKey(i int) string { return fmt.Sprintf("key-%04d", i) }

func primarySplitCrashValue(i int) []byte {
	return []byte(fmt.Sprintf(`{"i":%d,"pad":"%s"}`, i, strings.Repeat("p", 900)))
}

// runPrimarySplitFaultPass creates a fresh seed primary graph (never
// fault-injected, since CreateFromPrimary builds its own committer), opens it
// buffered with the fault device installed, and inserts up to ops keys, flushing
// after each so every boundary is durable. It records the device-commit count and
// content oracle at each boundary and the split count per op, and snapshots the
// on-disk image at whatever crash the plan induced.
func runPrimarySplitFaultPass(
	t *testing.T,
	built *store.Collection,
	options Options,
	fc *faultController,
	plan storeio.FaultPlan,
	ops int,
) (boundaries []int, contents []map[string]string, splits []uint64, image []byte, faulted bool) {
	t.Helper()
	prev := storeCommitterFactory
	storeCommitterFactory = fc.factory()
	defer func() { storeCommitterFactory = prev }()
	fc.plan = plan
	fc.device = nil

	path := filepath.Join(t.TempDir(), "primary-split-crash.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		_ = file.Close()
		t.Fatalf("seed CreateFromPrimary: %v", err)
	}
	coll, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatalf("open seed: %v", err)
	}
	dev := fc.device
	if dev == nil {
		_ = coll.Close()
		_ = file.Close()
		t.Fatal("fault device was not installed by the committer factory")
	}

	boundaries = []int{dev.Commits()}
	contents = []map[string]string{snapshotCollectionContent(t, coll)}
	splits = []uint64{coll.Stats().PrimaryLeafSplits}
	for i := 0; i < ops; i++ {
		_, opErr := coll.Put([]byte(primarySplitCrashKey(i)), primarySplitCrashValue(i))
		if opErr == nil {
			opErr = coll.Flush()
		}
		if dev.Faulted() {
			break
		}
		if opErr != nil {
			_ = coll.Close()
			_ = file.Close()
			t.Fatalf("insert %d: unexpected error before any fault: %v", i, opErr)
		}
		boundaries = append(boundaries, dev.Commits())
		contents = append(contents, snapshotCollectionContent(t, coll))
		splits = append(splits, coll.Stats().PrimaryLeafSplits)
	}
	faulted = dev.Faulted()
	image, _ = os.ReadFile(path)
	_ = coll.Close()
	_ = file.Close()
	return boundaries, contents, splits, image, faulted
}

// TestFilePrimaryLeafSplitCrashBoundary crashes the buffered leaf-split
// structural transaction at every write point of every device commit inside the
// splitting insert -- before the data barrier, after it, at the alternate root,
// after the final sync, and with a torn root -- then reopens each induced image.
// Every reopen must Verify (the offline walker proves the recovered graph's shape
// and ordering) and match the content oracle: exactly the pre-split or post-split
// key set, never a torn third value and never a panic. This is the crash cover
// for the structural-transaction retirement fix: a checkpoint that advances
// durableState across the structural boundary must leave a recoverable image at
// each step.
func TestFilePrimaryLeafSplitCrashBoundary(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("seed", []byte(`{"v":0}`)); err != nil {
		t.Fatal(err)
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Collection: store.Options{ChunkDocuments: 1},
		Backend:    BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		PageSize:   4096, MaxPageSize: 64 << 10,
		InlineValueBytes: 2048, MaxDocumentBytes: 2048,
		// One generation per commit and no coalescing so the Nth commit is the same
		// logical write on every replay.
		GroupLimit: 1, CommitCoalesce: 0,
	}

	const maxOps = 40
	fc := &faultController{}
	boundaries, contents, splits, _, faulted := runPrimarySplitFaultPass(
		t, built, options, fc, storeio.FaultPlan{Phase: storeio.FaultNone}, maxOps,
	)
	if faulted {
		t.Fatal("clean probe pass unexpectedly faulted")
	}
	splitOp := -1
	for i := 1; i < len(splits); i++ {
		if splits[i] > splits[i-1] {
			splitOp = i - 1 // op index (0-based); boundaries[i-1..i] is its window
			break
		}
	}
	if splitOp < 0 {
		t.Fatalf("no leaf split fired across %d inserts (splits=%v)", maxOps, splits)
	}
	windowLo, windowHi := boundaries[splitOp], boundaries[splitOp+1]
	t.Logf(
		"split fired on insert %d; commit window [%d,%d) of %d commits; pre=%d keys post=%d keys",
		splitOp, windowLo, windowHi, boundaries[len(boundaries)-1],
		len(contents[splitOp]), len(contents[splitOp+1]),
	)
	if windowHi <= windowLo {
		t.Fatalf("split insert issued no device commit: window [%d,%d)", windowLo, windowHi)
	}

	phases := []storeio.FaultPhase{
		storeio.FaultAfterDataWrite,
		storeio.FaultAfterBarrier,
		storeio.FaultAfterRootWrite,
		storeio.FaultAfterFinalSync,
		storeio.FaultTornRoot,
	}
	tally := map[string]int{}
	exercised := 0
	for commit := windowLo; commit < windowHi; commit++ {
		for _, phase := range phases {
			plan := storeio.FaultPlan{Commit: commit, Phase: phase, DataIndex: 0}
			_, _, _, image, didFault := runPrimarySplitFaultPass(
				t, built, options, fc, plan, splitOp+1,
			)
			if !didFault {
				// A phase that does not apply to this commit (e.g. no data page for
				// FaultAfterDataWrite) simply runs clean; skip it.
				continue
			}
			label := fmt.Sprintf("commit=%d phase=%d", commit, phase)
			legal := expectedStates(commit, phase, boundaries, contents)
			outcome := verifyCrashImage(t, options, image, legal, label)
			tally[outcome]++
			exercised++

			// The offline verify walker must accept every reopened image: a clean
			// recovery yields an ordered, non-aliasing graph; a fail-closed image is
			// allowed to carry findings but must not crash the walker.
			imagePath := filepath.Join(t.TempDir(), "verify.vibe")
			if err := os.WriteFile(imagePath, image, 0o600); err != nil {
				t.Fatal(err)
			}
			vf, vErr := os.OpenFile(imagePath, os.O_RDWR, 0o600)
			if vErr != nil {
				t.Fatal(vErr)
			}
			report, verifyErr := Verify(vf)
			_ = vf.Close()
			if verifyErr != nil {
				t.Fatalf("%s: Verify returned I/O error: %v", label, verifyErr)
			}
			if outcome == "recovered" && !report.OK() {
				t.Fatalf(
					"%s: recovered image failed verify walker: %v",
					label, report.Findings,
				)
			}
		}
	}
	if exercised == 0 {
		t.Fatal("no crash points were exercised in the split window")
	}
	t.Logf("split crash sweep: %d points exercised; outcomes %v", exercised, tally)
}
