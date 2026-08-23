package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

const (
	applyComplexityTenMillionRows = 10_000_000
	applyComplexityTestName       = "TestMachineApplyPointUpdateTenMillionQualification"

	// A cold point update can activate the committer's fixed descriptor arena
	// and perform one bounded compact-leaf topology conversion. Keep that honest
	// one-time cost bounded independently of collection cardinality, while also
	// bounding the smaller per-apply command/session bookkeeping in benchmarks.
	applyComplexityColdAllocBytes = 20 << 20
	applyComplexityAllocBytes     = 64 << 10
	applyComplexityColdAllocs     = 1_024
	applyComplexityAllocs         = 256
)

var applyComplexityValues = [2][]byte{[]byte(`{"v":0}`), []byte(`{"v":1}`)}

type applyComplexityValidator struct {
	armed       bool
	putCalls    uint64
	deleteCalls uint64
	lastKey     []byte
	lastValue   []byte
}

func (v *applyComplexityValidator) ValidatePut(key, value []byte) MutationValidation {
	if v.armed {
		v.putCalls++
		v.lastKey = append(v.lastKey[:0], key...)
		v.lastValue = append(v.lastValue[:0], value...)
	}
	return MutationValidationAccept
}

func (v *applyComplexityValidator) ValidateDelete(_, _ []byte, _ bool) MutationValidation {
	if v.armed {
		v.deleteCalls++
	}
	return MutationValidationAccept
}

func (v *applyComplexityValidator) arm() {
	v.armed = true
	v.putCalls = 0
	v.deleteCalls = 0
	v.lastKey = v.lastKey[:0]
	v.lastValue = v.lastValue[:0]
}

type applyComplexityObserver struct {
	calls         uint64
	attemptedKeys uint64
	lastKey       []byte
}

func (o *applyComplexityObserver) observe(keys AttemptedMutationKeys) {
	o.calls++
	o.attemptedKeys += uint64(keys.Len())
	if keys.Len() == 1 {
		o.lastKey = append(o.lastKey[:0], keys.Key(0)...)
	}
}

func (o *applyComplexityObserver) reset() {
	o.calls = 0
	o.attemptedKeys = 0
	o.lastKey = o.lastKey[:0]
}

type applyComplexityStats struct {
	fullScans uint64
	pageReads uint64
	readBytes uint64
	splits    uint64
	reclaims  uint64
	rows      uint64
	fileEnd   uint64
	capacity  uint64
	published uint64
	durable   uint64
}

func captureApplyComplexityStats(collection *durable.Collection) applyComplexityStats {
	stats := collection.Stats()
	return applyComplexityStats{
		fullScans: stats.SnapshotFullScanCalls,
		pageReads: stats.PageReads,
		readBytes: stats.ReadBytes,
		splits:    stats.PrimaryLeafSplits,
		reclaims:  stats.PrimaryEmptyReclaims,
		rows:      stats.DocumentCount,
		fileEnd:   stats.FileEnd,
		capacity:  stats.CapacityBytes,
		published: stats.PublishedGeneration,
		durable:   stats.DurableGeneration,
	}
}

type applyComplexityFixture struct {
	machine       *Machine
	binding       Binding
	system        *durable.Collection
	user          *durable.Collection
	systemPath    string
	userPath      string
	txnLogPath    string
	targetKey     []byte
	validator     *applyComplexityValidator
	observer      *applyComplexityObserver
	setupDuration time.Duration
	openDuration  time.Duration
	nextApplied   uint64
	nextSequence  uint64
	clientEpoch   uint64
	seedValue     []byte
	updateValue   []byte
}

func newApplyComplexityFixture(t testing.TB, rows int) *applyComplexityFixture {
	t.Helper()
	if rows < 1 {
		t.Fatalf("rows = %d, want positive", rows)
	}
	setupStarted := time.Now()
	dir := t.TempDir()
	options := durable.Options{Durability: durable.DurabilitySync}
	systemPath := filepath.Join(dir, "system.vdb")
	system := createTargetAt(t, dir, "system", options)

	validator := new(applyComplexityValidator)
	observer := new(applyComplexityObserver)
	records := make([]durable.PrimaryBulkRecord, rows)
	for row := range rows {
		records[row] = durable.PrimaryBulkRecord{
			Key: applyComplexityRowKey(row), Value: applyComplexityValues[row%2],
		}
	}
	userPath := filepath.Join(dir, "user.vdb")
	userFile, err := os.OpenFile(userPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = userFile.Close() })
	if _, err := durable.CreateFromRecords(records, userFile, options); err != nil {
		t.Fatalf("CreateFromRecords(%d): %v", rows, err)
	}
	records = nil
	if rows == applyComplexityTenMillionRows {
		runtime.GC()
	}
	userCollection, err := durable.Open(userFile, options)
	if err != nil {
		t.Fatalf("open bulk user collection: %v", err)
	}
	t.Cleanup(func() { _ = userCollection.Close() })
	user := targetOf(userCollection)
	user.Validator = validator
	user.ObserveMutationAttempt = observer.observe
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	setupDuration := time.Since(setupStarted)

	openStarted := time.Now()
	binding := testBinding()
	bootstrap := testBootstrap()
	const importedApplied = uint64(2)
	machine, base, manifest, err := InitializeStagedSnapshot(
		binding, bootstrap, system,
		UserCollection{Name: "docs", Target: user}, log, machineOptionsFor(user),
		StagedSnapshotCut{
			Applied: importedApplied, Term: 2,
			EntryDigest: sha256.Sum256([]byte("p01-cardinality-imported-cut")),
		},
		SnapshotArtifactOptions{},
	)
	if err != nil {
		t.Fatalf("InitializeStagedSnapshot(%d): %v", rows, err)
	}
	if manifest.UserRows != uint64(rows) {
		t.Fatalf("staged user rows = %d, want %d", manifest.UserRows, rows)
	}
	if publication, err := machine.InstallSnapshot(base); err != nil || publication.Applied != importedApplied {
		t.Fatalf("InstallSnapshot = %+v, %v", publication, err)
	}
	_, _, epoch := applySessionOpen(t, machine, importedApplied+1, commandValue(binding, 1))
	openDuration := time.Since(openStarted)
	if !system.Collection.HasSynchronousDurability() || !userCollection.HasSynchronousDurability() {
		t.Fatal("cardinality fixture is not synchronously durable")
	}
	validator.arm()
	observer.reset()
	targetRow := 0
	return &applyComplexityFixture{
		machine: machine, binding: binding,
		system: system.Collection, user: userCollection,
		systemPath: systemPath, userPath: userPath,
		targetKey: []byte(applyComplexityRowKey(targetRow)),
		validator: validator, observer: observer,
		setupDuration: setupDuration, openDuration: openDuration,
		nextApplied: importedApplied + 2, nextSequence: 2, clientEpoch: epoch,
		txnLogPath:  filepath.Join(dir, "txn.vtm"),
		seedValue:   applyComplexityValues[targetRow%2],
		updateValue: applyComplexityValues[1-targetRow%2],
	}
}

func applyComplexityRowKey(row int) string {
	return fmt.Sprintf("row-%08d", row)
}

func (f *applyComplexityFixture) command(t testing.TB, sequence uint64, value []byte) []byte {
	t.Helper()
	command := commandValue(f.binding, sequence-1)
	command.ClientEpoch = f.clientEpoch
	command.ClientSequence = sequence
	command.Fingerprint = sha256.Sum256([]byte(fmt.Sprintf("p01-point-update-%d", sequence)))
	command.Mutations = []replication.Mutation{{
		Kind: replication.MutationPut, Key: f.targetKey, Value: value,
	}}
	return encodeCommand(t, command)
}

type applyComplexityResult struct {
	rows                 int
	systemFileBytes      int64
	userFileBytes        int64
	setupDuration        time.Duration
	openDuration         time.Duration
	applyDuration        time.Duration
	applyAllocBytes      uint64
	applyAllocations     uint64
	systemBefore         applyComplexityStats
	systemAfter          applyComplexityStats
	userBefore           applyComplexityStats
	userAfter            applyComplexityStats
	validatorCalls       uint64
	validatorDeleteCalls uint64
	observerCalls        uint64
	attemptedKeys        uint64
	applied              uint64
	digestBefore         [sha256.Size]byte
	digestAfter          [sha256.Size]byte
	digestChanged        bool
	valueProfile         string
	updatedValueProfile  string
	keyProfile           string
}

func runApplyComplexityQualification(t *testing.T, rows int) applyComplexityResult {
	t.Helper()
	fixture := newApplyComplexityFixture(t, rows)
	beforePublication := fixture.machine.Published()
	systemBefore := captureApplyComplexityStats(fixture.system)
	userBefore := captureApplyComplexityStats(fixture.user)
	if userBefore.rows != uint64(rows) {
		t.Fatalf("rows before ApplyNormal = %d, want %d", userBefore.rows, rows)
	}
	if systemBefore.rows != 3 {
		t.Fatalf("system rows before ApplyNormal = %d, want 3", systemBefore.rows)
	}
	if _, err := os.Stat(fixture.txnLogPath); !os.IsNotExist(err) {
		t.Fatalf("transaction log before two-collection ApplyNormal = %v, want absent", err)
	}
	command := fixture.command(t, fixture.nextSequence, fixture.updateValue)
	var memoryBefore runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	applyStarted := time.Now()
	publication, err := fixture.machine.ApplyNormal(normalMeta(fixture.nextApplied), command)
	applyDuration := time.Since(applyStarted)
	var memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryAfter)
	if err != nil {
		t.Fatalf("ApplyNormal(%d rows): %v", rows, err)
	}
	applyAllocBytes := monotoneApplyComplexityDelta(
		t, "ApplyNormal allocated bytes", memoryBefore.TotalAlloc, memoryAfter.TotalAlloc,
	)
	applyAllocations := monotoneApplyComplexityDelta(
		t, "ApplyNormal allocations", memoryBefore.Mallocs, memoryAfter.Mallocs,
	)
	if applyAllocBytes > applyComplexityColdAllocBytes ||
		applyAllocations > applyComplexityColdAllocs {
		t.Fatalf("ApplyNormal allocation work bytes=%d/%d objects=%d/%d",
			applyAllocBytes, applyComplexityColdAllocBytes,
			applyAllocations, applyComplexityColdAllocs)
	}
	systemAfter := captureApplyComplexityStats(fixture.system)
	userAfter := captureApplyComplexityStats(fixture.user)
	if systemAfter.fullScans != systemBefore.fullScans || userAfter.fullScans != userBefore.fullScans {
		t.Fatalf("ApplyNormal full scans system=%d->%d user=%d->%d",
			systemBefore.fullScans, systemAfter.fullScans,
			userBefore.fullScans, userAfter.fullScans)
	}
	if userAfter.pageReads < userBefore.pageReads || userAfter.pageReads-userBefore.pageReads > 4 {
		t.Fatalf("ApplyNormal user page reads = %d->%d, want delta in [0,4]",
			userBefore.pageReads, userAfter.pageReads)
	}
	userSplits := monotoneApplyComplexityDelta(t, "user leaf splits", userBefore.splits, userAfter.splits)
	userReclaims := monotoneApplyComplexityDelta(t, "user empty reclaims", userBefore.reclaims, userAfter.reclaims)
	if userSplits > 1 || userReclaims != 0 {
		t.Fatalf("ApplyNormal structural work splits=%d reclaims=%d, want <=1/0",
			userSplits, userReclaims)
	}
	if userAfter.rows != uint64(rows) {
		t.Fatalf("rows after ApplyNormal = %d, want %d", userAfter.rows, rows)
	}
	if systemAfter.rows != 4 {
		t.Fatalf("system rows after ApplyNormal = %d, want 4", systemAfter.rows)
	}
	if systemAfter.published <= systemBefore.published ||
		userAfter.published <= userBefore.published {
		t.Fatalf("generation did not advance system=%d->%d user=%d->%d",
			systemBefore.published, systemAfter.published,
			userBefore.published, userAfter.published)
	}
	if fixture.validator.putCalls != 1 || fixture.validator.deleteCalls != 0 ||
		!bytes.Equal(fixture.validator.lastKey, fixture.targetKey) ||
		!bytes.Equal(fixture.validator.lastValue, fixture.updateValue) {
		t.Fatalf("validator puts=%d deletes=%d key=%q value=%q",
			fixture.validator.putCalls, fixture.validator.deleteCalls,
			fixture.validator.lastKey, fixture.validator.lastValue)
	}
	if fixture.observer.calls != 1 || fixture.observer.attemptedKeys != 1 ||
		!bytes.Equal(fixture.observer.lastKey, fixture.targetKey) {
		t.Fatalf("observer calls=%d attempted=%d key=%q",
			fixture.observer.calls, fixture.observer.attemptedKeys, fixture.observer.lastKey)
	}
	if publication.Applied != fixture.nextApplied ||
		publication.DataChainDigest == beforePublication.DataChainDigest {
		t.Fatalf("publication Applied=%d digest=%x, before Applied=%d digest=%x",
			publication.Applied, publication.DataChainDigest,
			beforePublication.Applied, beforePublication.DataChainDigest)
	}
	value, found, err := fixture.user.AppendRaw(nil, fixture.targetKey)
	if err != nil || !found || !bytes.Equal(value, fixture.updateValue) {
		t.Fatalf("updated value = %q, found=%t, err=%v", value, found, err)
	}
	if _, err := os.Stat(fixture.txnLogPath); err != nil {
		t.Fatalf("transaction log after two-collection ApplyNormal: %v", err)
	}
	systemInfo, err := os.Stat(fixture.systemPath)
	if err != nil {
		t.Fatal(err)
	}
	userInfo, err := os.Stat(fixture.userPath)
	if err != nil {
		t.Fatal(err)
	}
	return applyComplexityResult{
		rows: rows, systemFileBytes: systemInfo.Size(), userFileBytes: userInfo.Size(),
		setupDuration: fixture.setupDuration, openDuration: fixture.openDuration,
		applyDuration: applyDuration, applyAllocBytes: applyAllocBytes,
		applyAllocations: applyAllocations,
		systemBefore:     systemBefore, systemAfter: systemAfter,
		userBefore: userBefore, userAfter: userAfter,
		validatorCalls:       fixture.validator.putCalls,
		validatorDeleteCalls: fixture.validator.deleteCalls,
		observerCalls:        fixture.observer.calls, attemptedKeys: fixture.observer.attemptedKeys,
		applied: publication.Applied, digestBefore: beforePublication.DataChainDigest,
		digestAfter:   publication.DataChainDigest,
		digestChanged: publication.DataChainDigest != beforePublication.DataChainDigest,
		keyProfile:    "row-%08d", valueProfile: "alternating " +
			string(applyComplexityValues[0]) + "|" + string(applyComplexityValues[1]),
		updatedValueProfile: string(fixture.updateValue),
	}
}

func TestMachineApplyPointUpdateCardinalityRegression(t *testing.T) {
	for _, rows := range []int{1, 65_536} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			result := runApplyComplexityQualification(t, rows)
			if _, err := result.marshalEvidence(); err != nil {
				t.Fatalf("marshal P0.1 evidence schema: %v", err)
			}
		})
	}
}

// TestMachineApplyPointUpdateTenMillionQualification is the literal P0.1 exit
// gate. Corpus construction, full-image validation, and staged-candidate open
// are reported separately and are not part of the one-command apply duration.
func TestMachineApplyPointUpdateTenMillionQualification(t *testing.T) {
	if os.Getenv("VIBEDB_APPLY_10M") != "1" {
		t.Skip("set VIBEDB_APPLY_10M=1 to run the 10,000,000-row qualification")
	}
	result := runApplyComplexityQualification(t, applyComplexityTenMillionRows)
	result.log(t)
	if path := os.Getenv("VIBEDB_P01_EVIDENCE_PATH"); path != "" {
		if err := result.writeEvidence(path); err != nil {
			t.Fatalf("write P0.1 evidence: %v", err)
		}
	}
}

func BenchmarkMachineApplyPointUpdateCardinality(b *testing.B) {
	for _, rows := range []int{1, 65_536} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			fixture := newApplyComplexityFixture(b, rows)
			// Keep the cold conversion visible in the qualification test, but keep
			// this benchmark about steady point-apply work. Two effective warm-ups
			// activate the fixed committer descriptor arena in both storage shapes:
			// the dense leaf does so during its topology conversion, while the
			// one-row shape does so when the next transaction folds the preceding
			// conditional journal decision. They also mint txn.vtm and leave the
			// target back at seedValue.
			for index, value := range [...][]byte{
				fixture.updateValue, fixture.seedValue,
			} {
				if _, err := fixture.machine.ApplyNormal(
					normalMeta(fixture.nextApplied+uint64(index)),
					fixture.command(b, fixture.nextSequence+uint64(index), value),
				); err != nil {
					b.Fatalf("warm ApplyNormal %d: %v", index, err)
				}
			}
			fixture.validator.arm()
			fixture.observer.reset()
			beforeDigest := fixture.machine.Published().DataChainDigest
			systemBefore := captureApplyComplexityStats(fixture.system)
			userBefore := captureApplyComplexityStats(fixture.user)
			commands := make([][]byte, b.N)
			for index := range commands {
				// The warm-ups left seedValue installed, so start with updateValue
				// and alternate thereafter; every measured Put is effective.
				value := fixture.updateValue
				if index%2 != 0 {
					value = fixture.seedValue
				}
				commands[index] = fixture.command(
					b, fixture.nextSequence+2+uint64(index), value,
				)
			}
			b.ReportAllocs()
			b.ReportMetric(float64(rows), "rows")
			if len(commands) != 0 {
				b.SetBytes(int64(len(commands[0])))
			}
			var memoryBefore runtime.MemStats
			runtime.ReadMemStats(&memoryBefore)
			b.ResetTimer()
			for index, command := range commands {
				if _, err := fixture.machine.ApplyNormal(
					normalMeta(fixture.nextApplied+2+uint64(index)), command,
				); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			var memoryAfter runtime.MemStats
			runtime.ReadMemStats(&memoryAfter)
			systemAfter := captureApplyComplexityStats(fixture.system)
			userAfter := captureApplyComplexityStats(fixture.user)
			operations := uint64(b.N)
			if b.N != 0 && fixture.machine.Published().DataChainDigest == beforeDigest {
				b.Fatal("effective benchmark updates did not advance the data-chain digest")
			}
			if systemAfter.fullScans != systemBefore.fullScans ||
				userAfter.fullScans != userBefore.fullScans {
				b.Fatalf("benchmark full scans system=%d->%d user=%d->%d",
					systemBefore.fullScans, systemAfter.fullScans,
					userBefore.fullScans, userAfter.fullScans)
			}
			if fixture.validator.putCalls != operations || fixture.validator.deleteCalls != 0 ||
				fixture.observer.calls != operations || fixture.observer.attemptedKeys != operations {
				b.Fatalf("benchmark fixed work validator=%d/%d observer=%d attempted=%d operations=%d",
					fixture.validator.putCalls, fixture.validator.deleteCalls,
					fixture.observer.calls, fixture.observer.attemptedKeys, operations)
			}
			userPageReads := monotoneApplyComplexityDelta(b, "user page reads", userBefore.pageReads, userAfter.pageReads)
			userReadBytes := monotoneApplyComplexityDelta(b, "user read bytes", userBefore.readBytes, userAfter.readBytes)
			userSplits := monotoneApplyComplexityDelta(b, "user splits", userBefore.splits, userAfter.splits)
			userReclaims := monotoneApplyComplexityDelta(b, "user reclaims", userBefore.reclaims, userAfter.reclaims)
			allocatedBytes := monotoneApplyComplexityDelta(
				b, "benchmark allocated bytes", memoryBefore.TotalAlloc, memoryAfter.TotalAlloc,
			)
			allocations := monotoneApplyComplexityDelta(
				b, "benchmark allocations", memoryBefore.Mallocs, memoryAfter.Mallocs,
			)
			maxAllocatedBytes := operations * uint64(applyComplexityAllocBytes)
			maxAllocations := operations * uint64(applyComplexityAllocs)
			if userPageReads > 4*operations || userSplits != 0 || userReclaims != 0 {
				b.Fatalf("benchmark bounded point work reads=%d splits=%d reclaims=%d operations=%d",
					userPageReads, userSplits, userReclaims, operations)
			}
			if allocatedBytes > maxAllocatedBytes || allocations > maxAllocations {
				b.Fatalf("benchmark allocation work bytes=%d/%d objects=%d/%d operations=%d",
					allocatedBytes, maxAllocatedBytes, allocations, maxAllocations, operations)
			}
			if operations != 0 && (systemAfter.published <= systemBefore.published ||
				userAfter.published <= userBefore.published) || userAfter.rows != uint64(rows) {
				b.Fatalf("benchmark publication system=%d->%d user=%d->%d rows=%d",
					systemBefore.published, systemAfter.published,
					userBefore.published, userAfter.published, userAfter.rows)
			}
			perOperation := func(total uint64) float64 {
				if operations == 0 {
					return 0
				}
				return float64(total) / float64(operations)
			}
			b.ReportMetric(perOperation(userPageReads), "user_page_reads/op")
			b.ReportMetric(perOperation(userReadBytes), "user_read_bytes/op")
			b.ReportMetric(perOperation(userSplits), "leaf_splits/op")
			b.ReportMetric(perOperation(userReclaims), "empty_reclaims/op")
			b.ReportMetric(perOperation(fixture.validator.putCalls), "validator_calls/op")
			b.ReportMetric(perOperation(fixture.observer.calls), "observer_calls/op")
			b.ReportMetric(perOperation(fixture.observer.attemptedKeys), "attempted_keys/op")
			b.ReportMetric(perOperation(allocatedBytes), "apply_alloc_bytes/op")
			b.ReportMetric(perOperation(allocations), "apply_allocs/op")
		})
	}
}

func monotoneApplyComplexityDelta(
	t testing.TB,
	name string,
	before, after uint64,
) uint64 {
	t.Helper()
	if after < before {
		t.Fatalf("%s regressed %d->%d", name, before, after)
	}
	return after - before
}

func (r applyComplexityResult) log(t *testing.T) {
	t.Helper()
	t.Logf("p01-evidence rows=%d system_file_bytes=%d user_file_bytes=%d setup_ns=%d open_ns=%d apply_ns=%d apply_alloc_bytes=%d apply_allocations=%d "+
		"system_scans=%d->%d user_scans=%d->%d system_page_reads=%d->%d user_page_reads=%d->%d "+
		"system_read_bytes=%d->%d user_read_bytes=%d->%d system_splits=%d->%d user_splits=%d->%d "+
		"system_reclaims=%d->%d user_reclaims=%d->%d validator_calls=%d validator_delete_calls=%d "+
		"observer_calls=%d attempted_keys=%d applied=%d digest_before=%x digest_after=%x digest_changed=%t rows_before=%d rows_after=%d pass=true",
		r.rows, r.systemFileBytes, r.userFileBytes,
		r.setupDuration.Nanoseconds(), r.openDuration.Nanoseconds(), r.applyDuration.Nanoseconds(),
		r.applyAllocBytes, r.applyAllocations,
		r.systemBefore.fullScans, r.systemAfter.fullScans, r.userBefore.fullScans, r.userAfter.fullScans,
		r.systemBefore.pageReads, r.systemAfter.pageReads, r.userBefore.pageReads, r.userAfter.pageReads,
		r.systemBefore.readBytes, r.systemAfter.readBytes, r.userBefore.readBytes, r.userAfter.readBytes,
		r.systemBefore.splits, r.systemAfter.splits, r.userBefore.splits, r.userAfter.splits,
		r.systemBefore.reclaims, r.systemAfter.reclaims, r.userBefore.reclaims, r.userAfter.reclaims,
		r.validatorCalls, r.validatorDeleteCalls, r.observerCalls, r.attemptedKeys,
		r.applied, r.digestBefore, r.digestAfter, r.digestChanged, r.userBefore.rows, r.userAfter.rows)
}

func (r applyComplexityResult) writeEvidence(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := r.marshalEvidence()
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	for len(data) != 0 {
		written, writeErr := file.Write(data)
		if writeErr != nil {
			_ = file.Close()
			return writeErr
		}
		if written == 0 {
			_ = file.Close()
			return fmt.Errorf("zero-byte evidence write")
		}
		data = data[written:]
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func (r applyComplexityResult) marshalEvidence() ([]byte, error) {
	data, err := vibejson.Marshal(&applyComplexityEvidence{
		Schema: "vibedb.p01.apply-cardinality", Version: 1,
		TestName: applyComplexityTestName, Rows: uint64(r.rows), Pass: true,
		Durability: applyComplexityEvidenceDurability{
			System: durable.DurabilitySync, User: durable.DurabilitySync,
		},
		CacheBytes: applyComplexityEvidencePair{
			System: r.systemBefore.capacity, User: r.userBefore.capacity,
		},
		Corpus: applyComplexityEvidenceCorpus{
			KeyFormat: r.keyProfile, Values: [2]string{
				string(applyComplexityValues[0]), string(applyComplexityValues[1]),
			},
			TargetRow: 0, UpdatedValue: r.updatedValueProfile,
		},
		FileBytes: applyComplexityEvidenceFileBytes{
			System: r.systemFileBytes, User: r.userFileBytes,
		},
		DurationNS: applyComplexityEvidenceDuration{
			Setup: r.setupDuration.Nanoseconds(), Open: r.openDuration.Nanoseconds(),
			Apply: r.applyDuration.Nanoseconds(),
		},
		ApplyAllocation: applyComplexityEvidenceAllocation{
			Bytes: r.applyAllocBytes, Objects: r.applyAllocations,
		},
		Before: applyComplexityEvidenceCollections{
			System: evidenceApplyComplexityStats(r.systemBefore),
			User:   evidenceApplyComplexityStats(r.userBefore),
		},
		After: applyComplexityEvidenceCollections{
			System: evidenceApplyComplexityStats(r.systemAfter),
			User:   evidenceApplyComplexityStats(r.userAfter),
		},
		Validator: applyComplexityEvidenceValidator{
			PutCalls: r.validatorCalls, DeleteCalls: r.validatorDeleteCalls,
		},
		Observer: applyComplexityEvidenceObserver{
			Calls: r.observerCalls, AttemptedKeys: r.attemptedKeys,
		},
		Applied: r.applied, DigestBefore: fmt.Sprintf("%x", r.digestBefore),
		DigestAfter: fmt.Sprintf("%x", r.digestAfter), DigestChanged: r.digestChanged,
		RowCountBefore: r.userBefore.rows, RowCountAfter: r.userAfter.rows,
	})
	if err != nil {
		return nil, fmt.Errorf("encode evidence JSON: %w", err)
	}
	data = append(data, '\n')
	if err := vibejson.Validate(bytes.TrimSpace(data)); err != nil {
		return nil, fmt.Errorf("validate evidence JSON: %w", err)
	}
	return data, nil
}

type applyComplexityEvidence struct {
	Schema          string                             `json:"schema"`
	Version         uint16                             `json:"version"`
	TestName        string                             `json:"test_name"`
	Rows            uint64                             `json:"rows"`
	Pass            bool                               `json:"pass"`
	Durability      applyComplexityEvidenceDurability  `json:"durability"`
	CacheBytes      applyComplexityEvidencePair        `json:"cache_bytes"`
	Corpus          applyComplexityEvidenceCorpus      `json:"corpus"`
	FileBytes       applyComplexityEvidenceFileBytes   `json:"file_bytes"`
	DurationNS      applyComplexityEvidenceDuration    `json:"duration_ns"`
	ApplyAllocation applyComplexityEvidenceAllocation  `json:"apply_allocation"`
	Before          applyComplexityEvidenceCollections `json:"before"`
	After           applyComplexityEvidenceCollections `json:"after"`
	Validator       applyComplexityEvidenceValidator   `json:"validator"`
	Observer        applyComplexityEvidenceObserver    `json:"observer"`
	Applied         uint64                             `json:"applied"`
	DigestBefore    string                             `json:"digest_before"`
	DigestAfter     string                             `json:"digest_after"`
	DigestChanged   bool                               `json:"digest_changed"`
	RowCountBefore  uint64                             `json:"row_count_before"`
	RowCountAfter   uint64                             `json:"row_count_after"`
}

type applyComplexityEvidenceDurability struct {
	System durable.DurabilityMode `json:"system"`
	User   durable.DurabilityMode `json:"user"`
}

type applyComplexityEvidencePair struct {
	System uint64 `json:"system"`
	User   uint64 `json:"user"`
}

type applyComplexityEvidenceCorpus struct {
	KeyFormat    string    `json:"key_format"`
	Values       [2]string `json:"values"`
	TargetRow    uint64    `json:"target_row"`
	UpdatedValue string    `json:"updated_value"`
}

type applyComplexityEvidenceFileBytes struct {
	System int64 `json:"system"`
	User   int64 `json:"user"`
}

type applyComplexityEvidenceDuration struct {
	Setup int64 `json:"setup"`
	Open  int64 `json:"open"`
	Apply int64 `json:"apply"`
}

type applyComplexityEvidenceAllocation struct {
	Bytes   uint64 `json:"bytes"`
	Objects uint64 `json:"objects"`
}

type applyComplexityEvidenceCollections struct {
	System applyComplexityEvidenceStats `json:"system"`
	User   applyComplexityEvidenceStats `json:"user"`
}

type applyComplexityEvidenceStats struct {
	FullScanCalls       uint64 `json:"full_scan_calls"`
	PageReads           uint64 `json:"page_reads"`
	ReadBytes           uint64 `json:"read_bytes"`
	LeafSplits          uint64 `json:"leaf_splits"`
	EmptyReclaims       uint64 `json:"empty_reclaims"`
	Rows                uint64 `json:"rows"`
	FileEnd             uint64 `json:"file_end"`
	PublishedGeneration uint64 `json:"published_generation"`
	DurableGeneration   uint64 `json:"durable_generation"`
}

type applyComplexityEvidenceValidator struct {
	PutCalls    uint64 `json:"put_calls"`
	DeleteCalls uint64 `json:"delete_calls"`
}

type applyComplexityEvidenceObserver struct {
	Calls         uint64 `json:"calls"`
	AttemptedKeys uint64 `json:"attempted_keys"`
}

func evidenceApplyComplexityStats(stats applyComplexityStats) applyComplexityEvidenceStats {
	return applyComplexityEvidenceStats{
		FullScanCalls: stats.fullScans, PageReads: stats.pageReads,
		ReadBytes: stats.readBytes, LeafSplits: stats.splits,
		EmptyReclaims: stats.reclaims, Rows: stats.rows, FileEnd: stats.fileEnd,
		PublishedGeneration: stats.published, DurableGeneration: stats.durable,
	}
}
