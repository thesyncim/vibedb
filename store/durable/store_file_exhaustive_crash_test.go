package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
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
			_, opErr = coll.Delete(op.key)
		} else {
			_, opErr = coll.Put(op.key, []byte(op.value))
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
