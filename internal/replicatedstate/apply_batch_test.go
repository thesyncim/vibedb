package replicatedstate

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type normalBatchFixture struct {
	machineFixture
	group          *durable.CheckpointGroup
	systemOptions  durable.Options
	userOptions    durable.Options
	machineOptions Options
}

// The state-machine port must never admit a range that its one physical
// checkpoint-group transaction cannot certify. Both declarations are kept in
// their owning packages, so make drift a compile failure in either direction.
var (
	_ [raftmodel.MaxNormalApplyBatchEntries - durable.MaxCheckpointGroupUpdateEntries]struct{}
	_ [durable.MaxCheckpointGroupUpdateEntries - raftmodel.MaxNormalApplyBatchEntries]struct{}
)

func newNormalBatchFixture(
	t testing.TB,
	userMaxDocuments int,
	retryWindow uint16,
) normalBatchFixture {
	return newNormalBatchFixtureWithSystemDocuments(
		t, userMaxDocuments, retryWindow, 0,
	)
}

func newNormalBatchFixtureWithSystemDocuments(
	t testing.TB,
	userMaxDocuments int,
	retryWindow uint16,
	requestedSystemDocuments int,
) normalBatchFixture {
	return newNormalBatchFixtureWithOptions(
		t,
		durable.Options{MaxBatchDocuments: userMaxDocuments},
		retryWindow,
		requestedSystemDocuments,
	)
}

func newNormalBatchFixtureWithOptions(
	t testing.TB,
	userOptions durable.Options,
	retryWindow uint16,
	requestedSystemDocuments int,
) normalBatchFixture {
	t.Helper()
	if retryWindow == 0 {
		retryWindow = 8
	}
	dir := t.TempDir()
	openCollection := func(name string, options durable.Options) CollectionTarget {
		file, err := os.OpenFile(
			filepath.Join(dir, name+".vdb"),
			os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Create(file, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		return targetOf(collection)
	}
	systemDocuments := 2*int(retryWindow) + 2
	if systemDocuments < 32 {
		systemDocuments = 32
	}
	if requestedSystemDocuments > systemDocuments {
		systemDocuments = requestedSystemDocuments
	}
	systemOptions := durable.Options{
		OpaqueValues: true, MaxBatchDocuments: systemDocuments,
	}
	system := openCollection("system", systemOptions)
	system = systemTargetOf(system.Collection)
	user := openCollection("user", userOptions)
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	members := []durable.NamedCollection{
		{Name: systemCollectionName, Collection: system.Collection},
		{Name: "docs", Collection: user.Collection},
	}
	group, err := durable.NewCheckpointGroup(
		log, members, durable.CheckpointGroupOptions{CheckpointEvery: 1024},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = group.Close() })

	txnDocuments := user.Limits.MaxDistinctMutations + 4
	if systemDocuments > txnDocuments {
		txnDocuments = systemDocuments
	}
	if requestedSystemDocuments != 0 {
		txnDocuments = systemDocuments + user.Limits.MaxDistinctMutations
	}
	binding := testBinding()
	bootstrap := testBootstrap()
	options := Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 2,
			MaxDocuments:   txnDocuments,
			MaxBytes:       64 << 20,
		},
		MaxSessions:     128,
		RetryWindow:     retryWindow,
		CheckpointGroup: group,
	}
	machine, err := Open(
		binding, bootstrap, system,
		UserCollection{Name: "docs", Target: user}, log, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	return normalBatchFixture{
		machineFixture: machineFixture{
			machine: machine, binding: binding, bootstrap: bootstrap,
			system: system, user: user, log: log, dir: dir,
		},
		group: group, systemOptions: systemOptions, userOptions: userOptions,
		machineOptions: options,
	}
}

func (f normalBatchFixture) crashReopen(t testing.TB) normalBatchFixture {
	t.Helper()
	dir := t.TempDir()
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, infoErr := entry.Info()
		if infoErr != nil {
			t.Fatal(infoErr)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(f.dir, entry.Name()))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if writeErr := os.WriteFile(
			filepath.Join(dir, entry.Name()), data, info.Mode().Perm(),
		); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	openFile := func(name string) *os.File {
		file, openErr := os.OpenFile(filepath.Join(dir, name+".vdb"), os.O_RDWR, 0)
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(func() { _ = file.Close() })
		return file
	}
	requests := []durable.TransactionCollectionOpen{
		{File: openFile("system"), Options: f.systemOptions},
		{File: openFile("user"), Options: f.userOptions},
	}
	collections, log, group, err := durable.OpenCollectionsWithCheckpointGroup(
		dir, durable.TxnLogOptions{}, requests,
		[]string{systemCollectionName, "docs"},
		durable.CheckpointGroupOptions{CheckpointEvery: 1024},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, collection := range collections {
		collection := collection
		t.Cleanup(func() { _ = collection.Close() })
	}
	t.Cleanup(func() { _ = log.Close() })
	t.Cleanup(func() { _ = group.Close() })
	system := systemTargetOf(collections[0])
	user := targetOf(collections[1])
	options := f.machineOptions
	options.CheckpointGroup = group
	machine, err := Open(
		f.binding, f.bootstrap, system,
		UserCollection{Name: "docs", Target: user}, log, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	return normalBatchFixture{
		machineFixture: machineFixture{
			machine: machine, binding: f.binding, bootstrap: f.bootstrap,
			system: system, user: user, log: log, dir: dir,
		},
		group: group, systemOptions: f.systemOptions, userOptions: f.userOptions,
		machineOptions: options,
	}
}

func normalBatchEntries(
	start uint64,
	commands ...[]byte,
) []raftmodel.NormalApply {
	entries := make([]raftmodel.NormalApply, len(commands))
	for index := range commands {
		entries[index] = raftmodel.NormalApply{
			Meta: normalMeta(start + uint64(index)), Data: commands[index],
		}
	}
	return entries
}

func normalBatchWitnesses(entries []raftmodel.NormalApply) [][32]byte {
	return make([][32]byte, len(entries))
}

func assertPublicationEqual(
	t testing.TB,
	got, want raftmodel.Publication,
) {
	t.Helper()
	if got.Applied != want.Applied ||
		got.DataChainDigest != want.DataChainDigest ||
		got.ReplicaSetVersion != want.ReplicaSetVersion ||
		!proto.Equal(got.ConfState, want.ConfState) {
		t.Fatalf("publication = %+v, want %+v", got, want)
	}
}

func normalBatchRetryCommands(
	t testing.TB,
	binding Binding,
	firstSequence uint64,
	count int,
) [][]byte {
	t.Helper()
	commands := make([][]byte, count)
	for index := range commands {
		sequence := firstSequence + uint64(index)
		command := commandValue(binding, sequence)
		command.Batches[0].Mutations = []replication.Mutation{{
			Kind:  replication.MutationPut,
			Key:   []byte{0, byte(sequence % 4), 0xff},
			Value: []byte{'{', '"', 'n', '"', ':', byte('0' + sequence%10), '}'},
		}}
		commands[index] = encodeCommand(t, command)
	}
	return commands
}

func openDistinctBatchSessions(
	t testing.TB,
	machine *Machine,
	binding Binding,
	firstApplied uint64,
	count int,
) []replication.Command {
	t.Helper()
	commands := make([]replication.Command, count)
	for index := range commands {
		prototype := commandValue(binding, 1)
		prototype.ClientID = id128(byte(100 + index))
		applied := firstApplied + uint64(index)
		applySessionOpen(t, machine, applied, prototype)
		prototype.ClientEpoch = applied
		commands[index] = prototype
	}
	return commands
}

func normalBatchRetainedCapacityBytes(m *Machine) uint64 {
	if m == nil {
		return 0
	}
	return m.batchTelemetry.retainedBytes
}

func assertNormalBatchWorkspaceReleased(t testing.TB, workspace *normalBatchWorkspace) {
	t.Helper()
	releasedOverlay := func(overlay *logicalOverlay) bool {
		return overlay != nil && overlay.base == nil && len(overlay.entries) == 0 &&
			len(overlay.arena) == 0 && len(overlay.probe) == 0 &&
			len(overlay.order) == 0 && len(overlay.undo) == 0 &&
			overlay.netDocuments == 0 && overlay.netBytes == 0
	}
	if workspace == nil || !releasedOverlay(&workspace.system) ||
		!releasedOverlay(&workspace.user) || !releasedOverlay(&workspace.attempted) ||
		len(workspace.relationExtra) != 0 || len(workspace.attemptedExtra) != 0 ||
		len(workspace.relationMarks) != 0 || len(workspace.attemptedMarks) != 0 ||
		len(workspace.plan.sessionRead) != 0 || len(workspace.plan.slotRead) != 0 ||
		len(workspace.plan.sessionRecord) != 0 || len(workspace.plan.slotRecord) != 0 ||
		len(workspace.plan.currentValue) != 0 || len(workspace.plan.descriptors) != 0 ||
		len(workspace.state) != 0 ||
		len(workspace.keys) != 0 {
		t.Fatal("normal-batch scratch retained live keys, values, snapshots, or undo state")
	}
	for i := range workspace.relationExtra[:cap(workspace.relationExtra)] {
		if !releasedOverlay(&workspace.relationExtra[:cap(workspace.relationExtra)][i]) {
			t.Fatal("normal-batch relation scratch retained live state")
		}
	}
	for i := range workspace.attemptedExtra[:cap(workspace.attemptedExtra)] {
		if !releasedOverlay(&workspace.attemptedExtra[:cap(workspace.attemptedExtra)][i]) {
			t.Fatal("normal-batch attempted-relation scratch retained live state")
		}
	}
}

func TestApplyNormalBatchOnePhysicalUpdateZeroSyncAndBoundedWarmScratch(t *testing.T) {
	fixture := newNormalBatchFixture(t, 0, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	sessions := openDistinctBatchSessions(t, fixture.machine, fixture.binding, 2, 8)
	fixture.machine.user.ObserveMutationAttempt = func(AttemptedMutationKeys, error) {}

	applyRun := func(commandOrdinal, firstApplied uint64) uint64 {
		t.Helper()
		if err := fixture.group.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		commands := make([][]byte, len(sessions))
		for index := range sessions {
			command := commandValue(fixture.binding, commandOrdinal)
			command.ClientID = sessions[index].ClientID
			command.ClientEpoch = sessions[index].ClientEpoch
			command.Batches[0].Mutations = []replication.Mutation{{
				Kind:  replication.MutationPut,
				Key:   []byte{0, byte(index % 4), 0xff},
				Value: []byte{'{', '"', 'n', '"', ':', byte('0' + (index+int(commandOrdinal))%10), '}'},
			}}
			commands[index] = encodeCommand(t, command)
		}
		entries := normalBatchEntries(firstApplied, commands...)
		witnesses := normalBatchWitnesses(entries)
		before := fixture.group.Stats()
		applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
		if err != nil || applied != len(entries) ||
			publication.Applied != firstApplied+uint64(len(entries))-1 {
			t.Fatalf("batch at applied %d = %d, %+v, %v",
				firstApplied, applied, publication, err)
		}
		after := fixture.group.Stats()
		if after.TransactionHighWater != before.TransactionHighWater+1 ||
			after.Updates != before.Updates+1 ||
			after.LargestUpdateSpan != 8 ||
			after.CheckpointAppliedIndex != before.CheckpointAppliedIndex ||
			after.JournalSyncs != before.JournalSyncs ||
			after.MarkerSyncs != before.MarkerSyncs ||
			after.CertificateSyncs != before.CertificateSyncs ||
			after.BarrierSyncs != before.BarrierSyncs ||
			after.PhysicalCheckpoints != before.PhysicalCheckpoints {
			t.Fatalf("batch was not one zero-sync physical update: before=%+v after=%+v",
				before, after)
		}
		// Completion lookups acquire coherent snapshots and therefore establish
		// the next explicit checkpoint boundary only after the zero-sync batch
		// assertions above.
		if err := fixture.group.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		for index := range commands {
			lookup, lookupErr := fixture.machine.LookupCompletion(commands[index])
			if lookupErr != nil || lookup.AppliedSequence != firstApplied+uint64(index) {
				t.Fatalf("completion %d = %+v, %v", index, lookup, lookupErr)
			}
		}
		return normalBatchRetainedCapacityBytes(fixture.machine)
	}

	// Populate the fixed keys and warm every overlay lane before comparing
	// retained capacity. Later runs rewrite the same bounded retry ring and key
	// set, so no arena, table, order, or undo storage may continue growing.
	_ = applyRun(1, 10)
	warm := applyRun(2, 18)
	stable := applyRun(3, 26)
	if stable != warm {
		t.Fatalf("warm scratch grew: first=%v second=%v", warm, stable)
	}
	t.Logf("8-command pooled workspace retained %d bytes", stable)
}

func TestApplyNormalBatchFull128CommandWorkspaceIsWarmStable(t *testing.T) {
	const count = raftmodel.MaxNormalApplyBatchEntries
	fixture := newNormalBatchFixtureWithSystemDocuments(
		t, MaxDistinctMutations, 8, 3*count+1,
	)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototypes := make([]replication.Command, count)
	openEntries := make([]raftmodel.NormalApply, count)
	for index := range prototypes {
		prototype := commandValue(fixture.binding, 1)
		prototype.ClientID = id128(byte(index + 1))
		prototypes[index] = prototype
		openEntries[index] = raftmodel.NormalApply{
			Meta: normalMeta(2 + uint64(index)),
			Data: encodeCommand(t, sessionOpenFor(prototype)),
		}
	}
	before := fixture.group.Stats()
	if applied, publication, err := fixture.machine.ApplyNormalBatch(
		openEntries, normalBatchWitnesses(openEntries),
	); err != nil || applied != count || publication.Applied != 1+count {
		t.Fatalf("128 session opens = %d, %+v, %v", applied, publication, err)
	}
	if after := fixture.group.Stats(); after.Updates != before.Updates+1 ||
		after.TransactionHighWater != before.TransactionHighWater+1 {
		t.Fatalf("128 session opens were not one physical update: %+v -> %+v",
			before, after)
	}

	applyCommands := func(sequence, firstApplied uint64) (uint64, [][]byte) {
		t.Helper()
		entries := make([]raftmodel.NormalApply, count)
		commands := make([][]byte, count)
		for index := range entries {
			command := commandValue(fixture.binding, sequence)
			command.ClientID = prototypes[index].ClientID
			command.ClientEpoch = 2 + uint64(index)
			command.Batches[0].Mutations = []replication.Mutation{{
				Kind: replication.MutationPut,
				Key:  []byte{0, 'f', 'u', 'l', 'l', byte(index % MaxDistinctMutations)},
				Value: []byte{
					'{', '"', 'n', '"', ':', byte('0' + (index+int(sequence))%10), '}',
				},
			}}
			commands[index] = encodeCommand(t, command)
			entries[index] = raftmodel.NormalApply{
				Meta: normalMeta(firstApplied + uint64(index)), Data: commands[index],
			}
		}
		before := fixture.group.Stats()
		applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries, normalBatchWitnesses(entries),
		)
		if err != nil || applied != count || publication.Applied != firstApplied+count-1 {
			t.Fatalf("128 command run %d = %d, %+v, %v",
				sequence, applied, publication, err)
		}
		if after := fixture.group.Stats(); after.Updates != before.Updates+1 ||
			after.TransactionHighWater != before.TransactionHighWater+1 {
			t.Fatalf("128 command run %d was not one physical update: %+v -> %+v",
				sequence, before, after)
		}
		return normalBatchRetainedCapacityBytes(fixture.machine), commands
	}
	warm, _ := applyCommands(1, 2+count)
	stable, commands := applyCommands(2, 2+2*count)
	if stable != warm {
		t.Fatalf("128-command warm workspace grew: %d -> %d bytes", warm, stable)
	}
	for _, index := range []int{0, count / 2, count - 1} {
		lookup, err := fixture.machine.LookupCompletion(commands[index])
		if err != nil || lookup.AppliedSequence != 2+2*count+uint64(index) {
			t.Fatalf("128-command completion %d = %+v, %v", index, lookup, err)
		}
	}
	t.Logf("128-command pooled workspace retained %d bytes", stable)
}

func TestNormalBatchWorkspacePoolDensityAndWarmAllocations(t *testing.T) {
	machineBytes := unsafe.Sizeof(Machine{})
	telemetryBytes := unsafe.Sizeof(normalBatchTelemetry{})
	workspaceBytes := unsafe.Sizeof(normalBatchWorkspace{})
	batchFieldBytes := unsafe.Offsetof(Machine{}.txnLog) -
		unsafe.Offsetof(Machine{}.batchTelemetry)
	baselineMachineBytes := machineBytes - batchFieldBytes
	if telemetryBytes > 32 {
		t.Fatalf("per-Machine batch telemetry = %d bytes, want <= 32", telemetryBytes)
	}
	if batchFieldBytes != telemetryBytes || unsafe.Sizeof(finalMutation{}) != 80 {
		t.Fatalf("density geometry: telemetry=%d field=%d final-mutation=%d",
			telemetryBytes, batchFieldBytes, unsafe.Sizeof(finalMutation{}))
	}
	keys := make([][8]byte, 2*raftmodel.MaxNormalApplyBatchEntries+1)
	value := []byte(`{"n":1}`)
	var exerciseErr error
	var retained uint64
	var shape [8]int
	exercise := func() {
		workspace := normalBatchWorkspacePool.Get().(*normalBatchWorkspace)
		workspace.system.reset(nil)
		workspace.user.reset(nil)
		workspace.attempted.reset(nil)
		for index := range keys {
			keys[index] = [8]byte{0, 's', byte(index), byte(index >> 8), 0xff}
			if err := workspace.system.record(keys[index][:], value, false); err != nil {
				exerciseErr = err
				break
			}
		}
		if exerciseErr == nil {
			for index := 0; index < MaxDistinctMutations; index++ {
				if err := workspace.user.record(keys[index][:], value, false); err != nil {
					exerciseErr = err
					break
				}
				if err := workspace.attempted.record(keys[index][:], nil, false); err != nil {
					exerciseErr = err
					break
				}
			}
		}
		workspace.plan.sessionRead = append(workspace.plan.sessionRead[:0], value...)
		workspace.plan.slotRead = append(workspace.plan.slotRead[:0], value...)
		workspace.plan.sessionRecord = append(workspace.plan.sessionRecord[:0], value...)
		workspace.plan.slotRecord = append(workspace.plan.slotRecord[:0], value...)
		workspace.plan.currentValue = append(workspace.plan.currentValue[:0], value...)
		for range MaxDistinctMutations {
			workspace.plan.descriptors = append(
				workspace.plan.descriptors, mutationValueDescriptor{},
			)
		}
		workspace.state = append(workspace.state[:0], value...)
		workspace.keys = workspace.attempted.appendAttempted(workspace.keys[:0])
		shape = [8]int{
			cap(workspace.system.entries), cap(workspace.system.slots),
			cap(workspace.system.order), cap(workspace.system.undo),
			cap(workspace.user.entries), cap(workspace.user.slots),
			cap(workspace.attempted.entries), cap(workspace.attempted.slots),
		}
		retained = workspace.release()
		normalBatchWorkspacePool.Put(workspace)
	}
	exercise()
	if exerciseErr != nil {
		t.Fatal(exerciseErr)
	}
	workspace := normalBatchWorkspacePool.Get().(*normalBatchWorkspace)
	assertNormalBatchWorkspaceReleased(t, workspace)
	normalBatchWorkspacePool.Put(workspace)
	if allocations := testing.AllocsPerRun(100, exercise); !raceDetectorEnabled && allocations != 0 {
		t.Fatalf("warm 128-command workspace allocations = %.2f shape=%v", allocations, shape)
	}
	if exerciseErr != nil {
		t.Fatal(exerciseErr)
	}
	t.Logf("Machine baseline=%dB batched=%dB delta=%dB final-mutation=%dB inline=%dB pooled-workspace-header=%dB retained=%dB 4096-shard-delta=%dB",
		baselineMachineBytes, machineBytes, batchFieldBytes,
		unsafe.Sizeof(finalMutation{}), unsafe.Sizeof(Machine{}.mutationInline),
		workspaceBytes, retained, batchFieldBytes*4096)
}

func TestLogicalOverlayWarmRunIsAllocationFreeAndCapacityStable(t *testing.T) {
	var overlay logicalOverlay
	keys := make([][4]byte, 64)
	values := make([][8]byte, len(keys))
	changes := make([]finalMutation, 0, len(keys))
	var exerciseErr error
	exercise := func() {
		overlay.reset(nil)
		for index := range keys {
			keys[index] = [4]byte{0, byte(index), 0xff, byte(index >> 1)}
			values[index] = [8]byte{'{', '"', 'n', '"', ':', byte('0' + index%10), '}', 0}
			if err := overlay.record(keys[index][:], values[index][:7], false); err != nil {
				exerciseErr = err
				return
			}
		}
		mark := overlay.mark()
		if err := overlay.record(keys[0][:], nil, true); err != nil {
			exerciseErr = err
			return
		}
		if err := overlay.record([]byte{0, 0xfe, 0, 0xff}, []byte(`{"n":1}`), false); err != nil {
			exerciseErr = err
			return
		}
		overlay.rollback(mark)
		changes = overlay.appendAttempted(changes[:0])
		if len(changes) != len(keys) {
			exerciseErr = ErrStateCorrupt
			return
		}
		overlay.release()
	}
	exercise()
	if exerciseErr != nil {
		t.Fatal(exerciseErr)
	}
	warm := [7]int{
		len(overlay.slots), cap(overlay.slots), cap(overlay.entries),
		cap(overlay.arena), cap(overlay.probe), cap(overlay.order), cap(overlay.undo),
	}
	allocations := testing.AllocsPerRun(100, exercise)
	if exerciseErr != nil {
		t.Fatal(exerciseErr)
	}
	stable := [7]int{
		len(overlay.slots), cap(overlay.slots), cap(overlay.entries),
		cap(overlay.arena), cap(overlay.probe), cap(overlay.order), cap(overlay.undo),
	}
	if allocations != 0 || stable != warm {
		t.Fatalf("warm overlay = %.2f allocs/run, capacity %v -> %v",
			allocations, warm, stable)
	}
}

func TestLogicalOverlayDropsColdStructuralChurn(t *testing.T) {
	var overlay logicalOverlay
	overlay.reset(nil)
	count := maxNormalBatchRetainedOverlayEntries*2 + 1
	for index := 0; index < count; index++ {
		key := []byte{0, byte(index), byte(index >> 8), 0xff}
		value := []byte(`{"n":1}`)
		if err := overlay.record(key, value, false); err != nil {
			t.Fatal(err)
		}
		if err := overlay.record(key, nil, true); err != nil {
			t.Fatal(err)
		}
	}
	changes := overlay.appendAttempted(nil)
	if len(changes) != count || overlay.netDocuments != 0 ||
		cap(overlay.entries) <= maxNormalBatchRetainedOverlayEntries ||
		cap(overlay.slots) <= maxNormalBatchRetainedOverlaySlots {
		t.Fatalf("cold churn shape = entries %d/%d slots %d docs %d",
			len(changes), cap(overlay.entries), cap(overlay.slots), overlay.netDocuments)
	}
	overlay.release()
	if overlay.entries != nil || overlay.slots != nil || overlay.order != nil ||
		overlay.undo != nil {
		t.Fatalf("cold structural scratch retained: entries %d slots %d order %d undo %d",
			cap(overlay.entries), cap(overlay.slots), cap(overlay.order), cap(overlay.undo))
	}
}

func TestLogicalOverlayDropsColdBackingCapacityAfterLogicalTruncation(t *testing.T) {
	overlay := logicalOverlay{
		entries: make([]logicalOverlayEntry, 1, maxNormalBatchRetainedOverlayEntries+1),
		slots:   make([]uint32, 1, maxNormalBatchRetainedOverlaySlots+1),
		order:   make([]int, 1, maxNormalBatchRetainedOverlayEntries+1),
		undo:    make([]logicalOverlayUndo, 1, maxNormalBatchRetainedOverlayEntries+1),
	}
	overlay.release()
	if overlay.entries != nil || overlay.slots != nil || overlay.order != nil ||
		overlay.undo != nil {
		t.Fatalf("cold truncated capacity retained: entries %d slots %d order %d undo %d",
			cap(overlay.entries), cap(overlay.slots), cap(overlay.order), cap(overlay.undo))
	}
}

func TestApplyNormalBatchLargeDeletesBoundPeakAndDropColdBuffers(t *testing.T) {
	fixture := newNormalBatchFixture(t, 0, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))

	const documents = 3
	keys := make([][]byte, documents)
	largeValue := make([]byte, 0, maxNormalBatchRetainedBufferBytes+4096)
	largeValue = append(largeValue, `{"value":"`...)
	largeValue = append(
		largeValue,
		bytes.Repeat([]byte{'x'}, maxNormalBatchRetainedBufferBytes)...,
	)
	largeValue = append(largeValue, '"', '}')
	put := commandValue(fixture.binding, 1)
	put.Batches[0].Mutations = make([]replication.Mutation, documents)
	for index := range keys {
		keys[index] = []byte{0, 'l', 'a', 'r', 'g', 'e', byte(index)}
		put.Batches[0].Mutations[index] = replication.Mutation{
			Kind: replication.MutationPut, Key: keys[index], Value: largeValue,
		}
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), encodeCommand(t, put)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.group.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Delete planning needs only base existence. It must not copy every large
	// base value into the overlay arena merely to prove final net deletion.
	cut, _, userSnapshot, err := fixture.machine.captureApplyCut()
	if err != nil {
		t.Fatal(err)
	}
	var overlay logicalOverlay
	overlay.reset(userSnapshot)
	keyBytes := 0
	for _, key := range keys {
		keyBytes += len(key)
		if err := overlay.record(key, nil, true); err != nil {
			t.Fatal(err)
		}
	}
	if len(overlay.arena) != keyBytes || len(overlay.probe) != 0 {
		t.Fatalf("delete overlay copied base values: arena=%d probe=%d keys=%d",
			len(overlay.arena), len(overlay.probe), keyBytes)
	}
	overlay.release()
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}

	remove := commandValue(fixture.binding, 2)
	remove.Batches[0].Mutations = make([]replication.Mutation, documents)
	for index := range keys {
		remove.Batches[0].Mutations[index] = replication.Mutation{
			Kind: replication.MutationDelete, Key: keys[index],
		}
	}
	entries := normalBatchEntries(4, encodeCommand(t, remove))
	if applied, _, err := fixture.machine.ApplyNormalBatch(
		entries, normalBatchWitnesses(entries),
	); err != nil || applied != 1 {
		t.Fatalf("large delete batch = %d, %v", applied, err)
	}
	if got := fixture.machine.batchTelemetry.logicalValueReads; got != documents {
		t.Fatalf("large delete logical before-value reads = %d, want %d", got, documents)
	}
	if got := fixture.machine.batchTelemetry.physicalBaseValueReads; got != documents {
		t.Fatalf("large delete physical base reads = %d, want %d", got, documents)
	}
	if got := fixture.machine.batchTelemetry.logicalValueHashes; got != documents {
		t.Fatalf("large delete before-value hashes = %d, want %d", got, documents)
	}
	if got := fixture.machine.batchTelemetry.logicalAfterValueHashes; got != 0 {
		t.Fatalf("large delete after-value hashes = %d, want 0", got)
	}
	if retained := normalBatchRetainedCapacityBytes(fixture.machine); retained > 8*maxNormalBatchRetainedBufferBytes {
		t.Fatalf("large delete retained %d workspace bytes", retained)
	}
}

func TestApplyNormalBatchEqualPutSkipsBeforeValueHash(t *testing.T) {
	fixture := newNormalBatchFixture(t, 0, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	seed := commandValue(fixture.binding, 1)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), encodeCommand(t, seed)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	before := fixture.machine.Published()
	equal := commandValue(fixture.binding, 2)
	equal.Batches[0].Mutations = []replication.Mutation{{
		Kind:  replication.MutationPut,
		Key:   bytes.Clone(seed.Batches[0].Mutations[0].Key),
		Value: bytes.Clone(seed.Batches[0].Mutations[0].Value),
	}}
	entries := normalBatchEntries(4, encodeCommand(t, equal), nil)
	witnesses := normalBatchWitnesses(entries)
	applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
	if err != nil || applied != 2 || publication.Applied != 5 ||
		publication.DataChainDigest != before.DataChainDigest {
		t.Fatalf("equal-put batch = %d, %+v, %v", applied, publication, err)
	}
	if reads, physical, beforeHashes, afterHashes :=
		fixture.machine.batchTelemetry.logicalValueReads,
		fixture.machine.batchTelemetry.physicalBaseValueReads,
		fixture.machine.batchTelemetry.logicalValueHashes,
		fixture.machine.batchTelemetry.logicalAfterValueHashes; reads != 1 || physical != 1 || beforeHashes != 0 || afterHashes != 0 {
		t.Fatalf("equal-put work = %d logical reads, %d physical reads, %d/%d hashes",
			reads, physical, beforeHashes, afterHashes)
	}
}

func TestApplyNormalBatchHybridAdmissionRejectsBeforeDescriptorHashing(t *testing.T) {
	const (
		maxDocumentBytes = 8 << 10
		batchDocuments   = 4
		maxKeyBytes      = 256
	)
	fixture := newNormalBatchFixtureWithOptions(t, durable.Options{
		MaxKeyBytes: maxKeyBytes, MaxDocumentBytes: maxDocumentBytes,
		MaxBatchDocuments: batchDocuments,
		MaxBatchBytes:     maxDocumentBytes + batchDocuments*maxKeyBytes,
	}, 8, 0)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))

	document := func(fill byte) []byte {
		value := make([]byte, 0, 3900)
		value = append(value, `{"value":"`...)
		value = append(value, bytes.Repeat([]byte{fill}, 3880)...)
		return append(value, '"', '}')
	}
	command := commandValue(fixture.binding, 1)
	command.Batches[0].Mutations = nil
	for index, fill := range []byte{'a', 'b', 'c'} {
		command.Batches[0].Mutations = append(command.Batches[0].Mutations, replication.Mutation{
			Kind:  replication.MutationPut,
			Key:   []byte{0, 'h', 'y', 'b', 'r', 'i', 'd', byte(index)},
			Value: document(fill),
		})
	}
	if firstTwo := len(command.Batches[0].Mutations[0].Key) + len(command.Batches[0].Mutations[0].Value) +
		len(command.Batches[0].Mutations[1].Key) + len(command.Batches[0].Mutations[1].Value); firstTwo > fixture.user.Limits.MaxBatchBytes {
		t.Fatalf("first two mutations already exceed batch limit: %d > %d",
			firstTwo, fixture.user.Limits.MaxBatchBytes)
	}
	encoded := encodeCommand(t, command)
	entries := normalBatchEntries(3, encoded)
	applied, _, err := fixture.machine.ApplyNormalBatch(
		entries, normalBatchWitnesses(entries),
	)
	if err != nil || applied != 1 {
		t.Fatalf("hybrid rejected command apply = %d, %v", applied, err)
	}
	if result := completionResultCode(t, fixture.machine, encoded); result != ResultTargetBound {
		t.Fatalf("hybrid rejected completion = %d", result)
	}
	telemetry := fixture.machine.batchTelemetry
	if telemetry.hybridClassificationPasses != 1 ||
		telemetry.hybridDescriptorRereads != 0 ||
		telemetry.logicalValueReads != 3 ||
		telemetry.physicalBaseValueReads != 3 ||
		telemetry.logicalValueHashes != 0 ||
		telemetry.logicalAfterValueHashes != 0 {
		t.Fatalf("hybrid rejected work = %+v", telemetry)
	}
}

func TestApplyNormalBatchHybridAdmissionRereadsOnlyAcceptedChanges(t *testing.T) {
	const (
		maxDocumentBytes = 8 << 10
		batchDocuments   = 4
		maxKeyBytes      = 256
	)
	fixture := newNormalBatchFixtureWithOptions(t, durable.Options{
		MaxKeyBytes: maxKeyBytes, MaxDocumentBytes: maxDocumentBytes,
		MaxBatchDocuments: batchDocuments,
		MaxBatchBytes:     maxDocumentBytes + batchDocuments*maxKeyBytes,
	}, 8, 0)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))

	document := func(fill byte) []byte {
		value := make([]byte, 0, 3900)
		value = append(value, `{"value":"`...)
		value = append(value, bytes.Repeat([]byte{fill}, 3880)...)
		return append(value, '"', '}')
	}
	keys := [][]byte{
		{0, 'h', 'y', 'b', 'r', 'i', 'd', 0},
		{0, 'h', 'y', 'b', 'r', 'i', 'd', 1},
		{0, 'h', 'y', 'b', 'r', 'i', 'd', 2},
	}
	seedValues := [][]byte{document('a'), document('b'), document('c')}
	for index := range keys {
		seed := commandValue(fixture.binding, uint64(index+1))
		seed.Batches[0].Mutations = []replication.Mutation{{
			Kind: replication.MutationPut, Key: keys[index], Value: seedValues[index],
		}}
		if _, err := fixture.machine.ApplyNormal(
			normalMeta(uint64(index+3)), encodeCommand(t, seed),
		); err != nil {
			t.Fatal(err)
		}
	}

	command := commandValue(fixture.binding, 4)
	command.Batches[0].Mutations = []replication.Mutation{
		{Kind: replication.MutationPut, Key: keys[0], Value: seedValues[0]},
		{Kind: replication.MutationPut, Key: keys[1], Value: seedValues[1]},
		{Kind: replication.MutationPut, Key: keys[2], Value: document('d')},
	}
	entries := normalBatchEntries(6, encodeCommand(t, command))
	applied, _, err := fixture.machine.ApplyNormalBatch(
		entries, normalBatchWitnesses(entries),
	)
	if err != nil || applied != 1 {
		t.Fatalf("hybrid accepted command apply = %d, %v", applied, err)
	}
	telemetry := fixture.machine.batchTelemetry
	if telemetry.hybridClassificationPasses != 1 ||
		telemetry.hybridDescriptorRereads != 1 ||
		telemetry.logicalValueReads != 4 ||
		telemetry.physicalBaseValueReads != 4 ||
		telemetry.logicalValueHashes != 1 ||
		telemetry.logicalAfterValueHashes != 1 {
		t.Fatalf("hybrid accepted work = %+v", telemetry)
	}
}

func TestApplyNormalBatchDescriptorsRestoreLargeBaseWithoutReread(t *testing.T) {
	fixture := newNormalBatchFixture(t, 0, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	sessions := openDistinctBatchSessions(t, fixture.machine, fixture.binding, 2, 2)
	key := []byte{0, 'l', 'a', 'r', 'g', 'e', 0xff}
	makeValue := func(fill byte) []byte {
		value := make([]byte, 0, 900<<10)
		value = append(value, `{"value":"`...)
		value = append(value, bytes.Repeat([]byte{fill}, (900<<10)-32)...)
		return append(value, '"', '}')
	}
	original, changed := makeValue('a'), makeValue('b')
	seed := commandValue(fixture.binding, 1)
	seed.ClientID, seed.ClientEpoch = sessions[0].ClientID, sessions[0].ClientEpoch
	seed.Batches[0].Mutations = []replication.Mutation{{
		Kind: replication.MutationPut, Key: key, Value: original,
	}}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), encodeCommand(t, seed)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.group.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	first := commandValue(fixture.binding, 2)
	first.ClientID, first.ClientEpoch = sessions[0].ClientID, sessions[0].ClientEpoch
	first.Batches[0].Mutations = []replication.Mutation{{
		Kind: replication.MutationPut, Key: key, Value: changed,
	}}
	second := commandValue(fixture.binding, 1)
	second.ClientID, second.ClientEpoch = sessions[1].ClientID, sessions[1].ClientEpoch
	second.Batches[0].Mutations = []replication.Mutation{{
		Kind: replication.MutationPut, Key: key, Value: original,
	}}
	entries := normalBatchEntries(5, encodeCommand(t, first), encodeCommand(t, second))
	before := fixture.group.Stats()
	applied, publication, err := fixture.machine.ApplyNormalBatch(
		entries, normalBatchWitnesses(entries),
	)
	if err != nil || applied != len(entries) || publication.Applied != 6 {
		t.Fatalf("large restore batch = %d, %+v, %v", applied, publication, err)
	}
	if after := fixture.group.Stats(); after.Updates != before.Updates+1 ||
		after.TransactionHighWater != before.TransactionHighWater+1 {
		t.Fatalf("large restore physical stats = before %+v after %+v", before, after)
	}
	if logical, physical, beforeHashes, afterHashes :=
		fixture.machine.batchTelemetry.logicalValueReads,
		fixture.machine.batchTelemetry.physicalBaseValueReads,
		fixture.machine.batchTelemetry.logicalValueHashes,
		fixture.machine.batchTelemetry.logicalAfterValueHashes; logical != 2 || physical != 1 || beforeHashes != 2 || afterHashes != 2 {
		t.Fatalf("large restore work = %d logical, %d physical, %d/%d hashes",
			logical, physical, beforeHashes, afterHashes)
	}
	if retained := normalBatchRetainedCapacityBytes(fixture.machine); retained > 8*maxNormalBatchRetainedBufferBytes {
		t.Fatalf("large restore retained %d workspace bytes", retained)
	}
	stored, found, readErr := fixture.user.Collection.AppendRaw(nil, key)
	if readErr != nil || !found || !bytes.Equal(stored, original) {
		t.Fatalf("large restore final row = %d bytes found %v err %v",
			len(stored), found, readErr)
	}
}

func TestApplyNormalBatchMatchesSequentialLogicalPublicationsAndNetState(t *testing.T) {
	batched := newNormalBatchFixture(t, 0, 8)
	sequential := newMachineFixture(t)
	for _, fixture := range []machineFixture{batched.machineFixture, sequential} {
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
	}

	key := []byte{'b', 0, 0xff, 'k'}
	commands := openDistinctBatchSessions(t, batched.machine, batched.binding, 2, 3)
	_ = openDistinctBatchSessions(t, sequential.machine, sequential.binding, 2, 3)
	commands[0].Batches[0].Mutations = []replication.Mutation{{
		Kind: replication.MutationPut, Key: key, Value: []byte(`{"n":1}`),
	}}
	commands[1].Batches[0].Mutations = []replication.Mutation{{
		Kind: replication.MutationPut, Key: key, Value: []byte(`{"n":2}`),
	}}
	commands[2].Batches[0].Mutations = []replication.Mutation{{
		Kind: replication.MutationDelete, Key: key,
	}}
	encoded := make([][]byte, len(commands))
	want := make([]raftmodel.Publication, len(commands))
	for index := range commands {
		encoded[index] = encodeCommand(t, commands[index])
		publication, err := sequential.machine.ApplyNormal(
			normalMeta(uint64(index)+5), encoded[index],
		)
		if err != nil {
			t.Fatalf("sequential apply %d: %v", index, err)
		}
		want[index] = publication
	}

	var observed [][]byte
	var observedErr error
	batched.machine.user.ObserveMutationAttempt = func(
		keys AttemptedMutationKeys,
		updateErr error,
	) {
		observedErr = updateErr
		for index := 0; index < keys.Len(); index++ {
			observed = append(observed, bytes.Clone(keys.Key(index)))
		}
	}
	before := batched.group.Stats()
	entries := normalBatchEntries(5, encoded...)
	applied, publication, err := batched.machine.ApplyNormalBatch(
		entries, normalBatchWitnesses(entries),
	)
	if err != nil || applied != len(encoded) {
		t.Fatalf("ApplyNormalBatch = %d, %v", applied, err)
	}
	assertPublicationEqual(t, publication, want[len(want)-1])
	assertPublicationEqual(t, batched.machine.Published(), publication)
	after := batched.group.Stats()
	if after.TransactionHighWater != before.TransactionHighWater+1 ||
		after.Updates != before.Updates+1 || after.AppliedIndex != 7 {
		t.Fatalf("one physical batch transaction: before=%+v after=%+v", before, after)
	}
	if _, found, err := batched.user.Collection.AppendRaw(nil, key); err != nil || found {
		t.Fatalf("net-no-op user row = found %v, err %v", found, err)
	}
	if len(observed) != 1 || !bytes.Equal(observed[0], key) || observedErr != nil {
		t.Fatalf("attempted key union = %x, err %v", observed, observedErr)
	}
	for index := range encoded {
		lookup, err := batched.machine.LookupCompletion(encoded[index])
		if err != nil || lookup.AppliedSequence != uint64(index)+5 {
			t.Fatalf("completion %d = %+v, %v", index, lookup, err)
		}
	}
}

func TestApplyNormalBatchStopsBeforeSecondSessionCommandAndKeepsOutcome(t *testing.T) {
	fixture := newNormalBatchFixture(t, 0, 1)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	first := encodeCommand(t, commandValue(fixture.binding, 1))
	secondCommand := commandValue(fixture.binding, 2)
	secondCommand.AckThrough = 1
	second := encodeCommand(t, secondCommand)
	entries := normalBatchEntries(3, first, nil, second)
	witnesses := normalBatchWitnesses(entries)
	beforeDigest := fixture.machine.Published().DataChainDigest
	before := fixture.group.Stats()
	applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
	if err != nil || applied != 2 || publication.Applied != 4 {
		t.Fatalf("same-session boundary = %d, %+v, %v", applied, publication, err)
	}
	if witnesses[0] == beforeDigest || witnesses[1] != witnesses[0] ||
		publication.DataChainDigest != witnesses[1] || witnesses[2] != ([32]byte{}) {
		t.Fatalf("command/no-op witnesses = %x %x %x, final %x",
			witnesses[0], witnesses[1], witnesses[2], publication.DataChainDigest)
	}
	after := fixture.group.Stats()
	if after.TransactionHighWater != before.TransactionHighWater+1 ||
		after.Updates != before.Updates+1 || after.LargestUpdateSpan != 2 {
		t.Fatalf("same-session prefix stats = before %+v after %+v", before, after)
	}
	lookup, err := fixture.machine.LookupCompletion(first)
	if err != nil || lookup.AppliedSequence != 3 {
		t.Fatalf("first completion before next drive = %+v, %v", lookup, err)
	}
	applied, publication, err = fixture.machine.ApplyNormalBatch(
		entries[2:], witnesses[2:],
	)
	if err != nil || applied != 1 || publication.Applied != 5 {
		t.Fatalf("second session command = %d, %+v, %v", applied, publication, err)
	}
	lookup, err = fixture.machine.LookupCompletion(second)
	if err != nil || lookup.AppliedSequence != 5 {
		t.Fatalf("second completion = %+v, %v", lookup, err)
	}
	if _, err := fixture.machine.LookupCompletion(first); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("first completion after next drive = %v", err)
	}
}

func TestApplyNormalBatchSameSessionDuplicateAndConflictAreBoundaries(t *testing.T) {
	fixture := newNormalBatchFixture(t, 0, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	original := commandValue(fixture.binding, 1)
	originalBytes := encodeCommand(t, original)
	conflict := original
	conflict.Fingerprint[0] ^= 0xff
	conflictBytes := encodeCommand(t, conflict)
	commands := [][]byte{originalBytes, originalBytes, conflictBytes}
	entries := normalBatchEntries(3, commands...)
	witnesses := normalBatchWitnesses(entries)
	for index := range entries {
		applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries[index:], witnesses[index:],
		)
		if err != nil || applied != 1 || publication.Applied != uint64(index)+3 {
			t.Fatalf("same-session batch %d = %d, %+v, %v",
				index, applied, publication, err)
		}
	}
	lookup, err := fixture.machine.LookupCompletion(originalBytes)
	if err != nil || lookup.AppliedSequence != 3 {
		t.Fatalf("original completion = %+v, %v", lookup, err)
	}
	if _, err := fixture.machine.LookupCompletion(conflictBytes); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflicting retry = %v", err)
	}
}

func TestApplyNormalBatchSessionReleaseIsSameSessionBoundary(t *testing.T) {
	batched := newNormalBatchFixture(t, 0, 4)
	sequential := newSessionReleaseFixture(t, 128, 4)
	for _, fixture := range []machineFixture{batched.machineFixture, sequential} {
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
		first := encodeCommand(t, commandValue(fixture.binding, 1))
		if _, err := fixture.machine.ApplyNormal(normalMeta(3), first); err != nil {
			t.Fatal(err)
		}
	}
	retirement := encodeCommand(t, sessionRetirement(commandValue(batched.binding, 2)))
	releaseCommand := sessionRelease(sessionRetirement(commandValue(batched.binding, 2)))
	release := encodeCommand(t, releaseCommand)
	entries := normalBatchEntries(4, retirement, release)
	want := make([]raftmodel.Publication, len(entries))
	for index := range entries {
		publication, err := sequential.machine.ApplyNormal(entries[index].Meta, entries[index].Data)
		if err != nil {
			t.Fatalf("sequential apply %d: %v", index, err)
		}
		want[index] = publication
	}
	witnesses := normalBatchWitnesses(entries)
	applied, publication, err := batched.machine.ApplyNormalBatch(entries, witnesses)
	if err != nil || applied != 1 {
		t.Fatalf("retirement/release boundary = %d, %+v, %v", applied, publication, err)
	}
	assertPublicationEqual(t, publication, want[0])
	if lookup, lookupErr := batched.machine.LookupCompletion(retirement); lookupErr != nil || lookup.AppliedSequence != 4 {
		t.Fatalf("retirement completion = %+v, %v", lookup, lookupErr)
	}
	applied, publication, err = batched.machine.ApplyNormalBatch(
		entries[1:], witnesses[1:],
	)
	if err != nil || applied != 1 {
		t.Fatalf("release batch = %d, %+v, %v", applied, publication, err)
	}
	assertPublicationEqual(t, publication, want[1])
	capacity, err := batched.machine.SessionCapacityState()
	if err != nil || capacity.Applied != 5 || capacity.SessionCount != 0 ||
		capacity.SessionSlotCount != 0 || capacity.SessionEpochHighWater != 2 {
		t.Fatalf("released capacity = %+v, %v", capacity, err)
	}
	if _, err := batched.machine.LookupCompletion(release); !errors.Is(err, ErrSessionReleased) {
		t.Fatalf("released completion = %v", err)
	}
}

func TestApplyNormalBatchReturnsExactCapacityPrefix(t *testing.T) {
	fixture := newNormalBatchFixture(t, 1, 4)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	commands := openDistinctBatchSessions(t, fixture.machine, fixture.binding, 2, 2)
	first := commands[0]
	first.Batches[0].Mutations[0].Key = []byte("first")
	second := commands[1]
	second.Batches[0].Mutations[0].Key = []byte("second")
	entries := normalBatchEntries(
		4, encodeCommand(t, first), encodeCommand(t, second),
	)
	var observed [][]byte
	fixture.machine.user.ObserveMutationAttempt = func(
		keys AttemptedMutationKeys,
		_ error,
	) {
		for index := 0; index < keys.Len(); index++ {
			observed = append(observed, bytes.Clone(keys.Key(index)))
		}
	}
	before := fixture.group.Stats()
	witnesses := normalBatchWitnesses(entries)
	applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
	if err != nil || applied != 1 || publication.Applied != 4 {
		t.Fatalf("first bounded prefix = %d, %+v, %v", applied, publication, err)
	}
	after := fixture.group.Stats()
	if after.TransactionHighWater != before.TransactionHighWater+1 ||
		after.Updates != before.Updates+1 {
		t.Fatalf("first prefix stats = before %+v after %+v", before, after)
	}
	if _, found, err := fixture.user.Collection.AppendRaw(nil, []byte("second")); err != nil || found {
		t.Fatalf("rolled-back second key = found %v, err %v", found, err)
	}
	if len(observed) != 1 || !bytes.Equal(observed[0], []byte("first")) {
		t.Fatalf("bounded-prefix attempted keys = %q", observed)
	}
	clear(observed)
	observed = observed[:0]
	applied, publication, err = fixture.machine.ApplyNormalBatch(
		entries[1:], witnesses[1:],
	)
	if err != nil || applied != 1 || publication.Applied != 5 {
		t.Fatalf("second bounded prefix = %d, %+v, %v", applied, publication, err)
	}
	for _, key := range [][]byte{[]byte("first"), []byte("second")} {
		if _, found, err := fixture.user.Collection.AppendRaw(nil, key); err != nil || !found {
			t.Fatalf("key %q = found %v, err %v", key, found, err)
		}
	}
	if len(observed) != 1 || !bytes.Equal(observed[0], []byte("second")) {
		t.Fatalf("second-prefix attempted keys = %q", observed)
	}
}

func TestApplyNormalBatchCleanBoundariesAndEmptyEntries(t *testing.T) {
	t.Run("witness capacity", func(t *testing.T) {
		fixture := newNormalBatchFixture(t, 0, 8)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		entries := normalBatchEntries(2, nil, nil)
		short := [][32]byte{{1}}
		if applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries, short,
		); applied != 0 || publication != (raftmodel.Publication{}) ||
			!errors.Is(err, ErrAdmissionBound) || short[0] != ([32]byte{}) ||
			fixture.machine.Applied() != 1 {
			t.Fatalf("short witness capacity = %d, %+v, %x, %v",
				applied, publication, short[0], err)
		}
		witnesses := normalBatchWitnesses(entries)
		if applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries, witnesses,
		); err != nil || applied != len(entries) || publication.Applied != 3 ||
			witnesses[len(entries)-1] != publication.DataChainDigest {
			t.Fatalf("retry after short witnesses = %d, %+v, %v",
				applied, publication, err)
		}
	})

	t.Run("entry count bound", func(t *testing.T) {
		fixture := newNormalBatchFixture(t, 0, 8)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		entries := normalBatchEntries(2, nil)
		tooMany := make([]raftmodel.NormalApply, raftmodel.MaxNormalApplyBatchEntries+1)
		if applied, publication, err := fixture.machine.ApplyNormalBatch(
			tooMany, normalBatchWitnesses(tooMany),
		); applied != 0 || publication != (raftmodel.Publication{}) ||
			!errors.Is(err, ErrAdmissionBound) || fixture.machine.Applied() != 1 {
			t.Fatalf("entry count bound = %d, %+v, %v", applied, publication, err)
		}
		if applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries, normalBatchWitnesses(entries),
		); err != nil || applied != 1 || publication.Applied != 2 {
			t.Fatalf("retry after count bound = %d, %+v, %v",
				applied, publication, err)
		}
	})

	t.Run("aggregate byte bound", func(t *testing.T) {
		fixture := newNormalBatchFixture(t, 0, 8)
		payload := make([]byte, raftmodel.MaxNormalApplyBatchBytes+1)
		half := raftmodel.MaxNormalApplyBatchBytes / 2
		entries := []raftmodel.NormalApply{
			{
				Meta: raftmodel.ApplyMeta{Index: 1, Term: 1, Type: pb.EntryConfChange},
				Data: payload[:half:half],
			},
			{
				Meta: raftmodel.ApplyMeta{Index: 2, Term: 1, Type: pb.EntryConfChange},
				Data: payload[half:raftmodel.MaxNormalApplyBatchBytes],
			},
		}
		if applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries, normalBatchWitnesses(entries),
		); err != nil || applied != 0 || publication != (raftmodel.Publication{}) {
			t.Fatalf("exact aggregate byte bound = %d, %+v, %v", applied, publication, err)
		}
		entries[1].Data = payload[half:]
		if applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries, normalBatchWitnesses(entries),
		); applied != 0 || publication != (raftmodel.Publication{}) ||
			!errors.Is(err, ErrAdmissionBound) {
			t.Fatalf("aggregate byte bound + 1 = %d, %+v, %v", applied, publication, err)
		}
	})

	t.Run("not checkpoint backed", func(t *testing.T) {
		fixture := newMachineFixture(t)
		entries := normalBatchEntries(1, nil)
		if applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries, normalBatchWitnesses(entries),
		); err != nil || applied != 0 || publication != (raftmodel.Publication{}) {
			t.Fatalf("ordinary machine batch = %d, %+v, %v", applied, publication, err)
		}
	})

	t.Run("already published normal is a clean replay boundary", func(t *testing.T) {
		fixture := newNormalBatchFixture(t, 0, 8)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		meta := normalMeta(2)
		if _, err := fixture.machine.ApplyNormal(meta, nil); err != nil {
			t.Fatal(err)
		}
		before := fixture.group.Stats()
		entries := []raftmodel.NormalApply{{Meta: meta}}
		if applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries, normalBatchWitnesses(entries),
		); err != nil || applied != 0 || publication != (raftmodel.Publication{}) ||
			fixture.group.Stats() != before {
			t.Fatalf("published replay boundary = %d, %+v, %v", applied, publication, err)
		}
		if publication, err := fixture.machine.ApplyNormal(meta, nil); err != nil || publication.Applied != meta.Index {
			t.Fatalf("singleton replay = %+v, %v", publication, err)
		}
	})

	t.Run("pre-bootstrap matches singleton terminal sequence error", func(t *testing.T) {
		singleton := newNormalBatchFixture(t, 0, 8)
		if _, err := singleton.machine.ApplyNormal(normalMeta(1), nil); !errors.Is(err, ErrApplySequence) {
			t.Fatalf("singleton pre-bootstrap = %v", err)
		}
		batched := newNormalBatchFixture(t, 0, 8)
		entries := normalBatchEntries(1, nil)
		if applied, publication, err := batched.machine.ApplyNormalBatch(
			entries, normalBatchWitnesses(entries),
		); applied != 0 || publication != (raftmodel.Publication{}) ||
			!errors.Is(err, ErrApplySequence) {
			t.Fatalf("batch pre-bootstrap = %d, %+v, %v", applied, publication, err)
		}
		if _, err := singleton.machine.ApplyNormal(normalMeta(1), nil); !errors.Is(err, ErrApplyPoisoned) {
			t.Fatalf("singleton pre-bootstrap poison = %v", err)
		}
		if _, _, err := batched.machine.ApplyNormalBatch(
			entries, normalBatchWitnesses(entries),
		); !errors.Is(err, ErrApplyPoisoned) {
			t.Fatalf("batch pre-bootstrap poison = %v", err)
		}
	})

	t.Run("first non-normal", func(t *testing.T) {
		fixture := newNormalBatchFixture(t, 0, 8)
		entries := []raftmodel.NormalApply{{
			Meta: raftmodel.ApplyMeta{Index: 1, Term: 1, Type: pb.EntryConfChange},
		}}
		if applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries, normalBatchWitnesses(entries),
		); err != nil || applied != 0 || publication != (raftmodel.Publication{}) ||
			fixture.machine.Applied() != 0 {
			t.Fatalf("configuration boundary = %d, %+v, %v", applied, publication, err)
		}
	})

	t.Run("ownership after prefix", func(t *testing.T) {
		fixture := newNormalBatchFixture(t, 0, 8)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		ownership, err := AppendOwnershipTransition(
			nil, testOwnershipTransition(fixture.binding, 1),
		)
		if err != nil {
			t.Fatal(err)
		}
		entries := normalBatchEntries(2, nil, ownership)
		witnesses := normalBatchWitnesses(entries)
		applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
		if err != nil || applied != 1 || publication.Applied != 2 ||
			fixture.machine.Applied() != 2 || witnesses[0] != publication.DataChainDigest ||
			witnesses[1] != ([32]byte{}) {
			t.Fatalf("ownership prefix = %d, %+v, %v", applied, publication, err)
		}
	})

	t.Run("first ownership", func(t *testing.T) {
		fixture := newNormalBatchFixture(t, 0, 8)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		ownership, err := AppendOwnershipTransition(
			nil, testOwnershipTransition(fixture.binding, 1),
		)
		if err != nil {
			t.Fatal(err)
		}
		entries := normalBatchEntries(2, ownership)
		before := fixture.group.Stats()
		if applied, publication, err := fixture.machine.ApplyNormalBatch(
			entries, normalBatchWitnesses(entries),
		); err != nil || applied != 0 || publication != (raftmodel.Publication{}) ||
			fixture.machine.Applied() != 1 ||
			fixture.group.Stats() != before {
			t.Fatalf("first ownership boundary = %d, %+v, %v", applied, publication, err)
		}
	})

	t.Run("configuration after prefix", func(t *testing.T) {
		fixture := newNormalBatchFixture(t, 0, 8)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		entries := []raftmodel.NormalApply{
			{Meta: normalMeta(2)},
			{Meta: raftmodel.ApplyMeta{Index: 3, Term: 2, Type: pb.EntryConfChange}},
		}
		witnesses := normalBatchWitnesses(entries)
		applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
		if err != nil || applied != 1 || publication.Applied != 2 ||
			fixture.machine.Applied() != 2 || witnesses[0] != publication.DataChainDigest ||
			witnesses[1] != ([32]byte{}) {
			t.Fatalf("configuration prefix = %d, %+v, %v", applied, publication, err)
		}
	})

	t.Run("empty normals", func(t *testing.T) {
		fixture := newNormalBatchFixture(t, 0, 8)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		beforePublication := fixture.machine.Published()
		beforeStats := fixture.group.Stats()
		entries := normalBatchEntries(2, nil, nil, nil)
		witnesses := normalBatchWitnesses(entries)
		applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
		if err != nil || applied != len(entries) || publication.Applied != 4 ||
			publication.DataChainDigest != beforePublication.DataChainDigest {
			t.Fatalf("empty batch = %d, %+v, %v", applied, publication, err)
		}
		for index, witness := range witnesses {
			if witness != beforePublication.DataChainDigest {
				t.Fatalf("empty witness %d = %x, want %x",
					index, witness, beforePublication.DataChainDigest)
			}
		}
		afterStats := fixture.group.Stats()
		if afterStats.TransactionHighWater != beforeStats.TransactionHighWater+1 ||
			afterStats.Updates != beforeStats.Updates+1 {
			t.Fatalf("empty batch stats = before %+v after %+v", beforeStats, afterStats)
		}
	})
}

func TestApplyNormalBatchUnknownOutcomeCertifiesNoPrefixAndPoisons(t *testing.T) {
	fixture := newNormalBatchFixture(t, 0, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	_, open, _ := applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	beforePublication := fixture.machine.Published()
	command := encodeCommand(t, commandValue(fixture.binding, 1))
	entries := normalBatchEntries(3, command, nil)
	var observed [][]byte
	var observedErr error
	fixture.machine.user.ObserveMutationAttempt = func(
		keys AttemptedMutationKeys,
		updateErr error,
	) {
		observedErr = updateErr
		for index := 0; index < keys.Len(); index++ {
			observed = append(observed, bytes.Clone(keys.Key(index)))
		}
	}
	restore := durable.InstallCheckpointGroupDecisionAppendFaultForFacadeTest()
	witnesses := [][32]byte{{1}, {2}}
	applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
	restore()
	if applied != 0 || publication != (raftmodel.Publication{}) ||
		!errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("unknown outcome = %d, %+v, %v", applied, publication, err)
	}
	if witnesses[0] != ([32]byte{}) || witnesses[1] != ([32]byte{}) {
		t.Fatalf("unknown outcome certified stale witnesses = %x, %x",
			witnesses[0], witnesses[1])
	}
	if fixture.machine.Applied() != 2 || fixture.machine.Published().Applied != 2 {
		t.Fatalf("unknown outcome advertised applied %d/%d",
			fixture.machine.Applied(), fixture.machine.Published().Applied)
	}
	if len(observed) != 1 || !bytes.Equal(observed[0], []byte("k1")) ||
		!errors.Is(observedErr, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("unknown-outcome attempted union = %q, %v", observed, observedErr)
	}
	reopened := fixture.crashReopen(t)
	if reopened.machine.Applied() != 2 || reopened.machine.Published().Applied != 2 {
		t.Fatalf("uncertified recovery publication = %+v", reopened.machine.Published())
	}
	if reopened.machine.Published().DataChainDigest != beforePublication.DataChainDigest {
		t.Fatalf("uncertified recovery data-chain digest = %x, want %x",
			reopened.machine.Published().DataChainDigest, beforePublication.DataChainDigest)
	}
	if _, found, readErr := reopened.user.Collection.AppendRaw(nil, []byte("k1")); readErr != nil || found {
		t.Fatalf("uncertified recovery user row = found %v, err %v", found, readErr)
	}
	if lookup, lookupErr := reopened.machine.LookupCompletion(open); lookupErr != nil || lookup.AppliedSequence != 2 {
		t.Fatalf("uncertified recovery session open = %+v, %v", lookup, lookupErr)
	}
	if _, lookupErr := reopened.machine.LookupCompletion(command); !errors.Is(lookupErr, ErrCompletionNotFound) {
		t.Fatalf("uncertified recovery command = %v", lookupErr)
	}
	if applied, publication, err := fixture.machine.ApplyNormalBatch(
		entries, normalBatchWitnesses(entries),
	); applied != 0 || publication != (raftmodel.Publication{}) ||
		!errors.Is(err, ErrApplyPoisoned) {
		t.Fatalf("poisoned retry = %d, %+v, %v", applied, publication, err)
	}
	tooMany := make([]raftmodel.NormalApply, raftmodel.MaxNormalApplyBatchEntries+1)
	if applied, publication, err := fixture.machine.ApplyNormalBatch(
		tooMany, normalBatchWitnesses(tooMany),
	); applied != 0 || publication != (raftmodel.Publication{}) ||
		!errors.Is(err, ErrApplyPoisoned) {
		t.Fatalf("poison precedence over malformed batch = %d, %+v, %v", applied, publication, err)
	}
}

func TestApplyNormalBatchCheckpointRecoveryPublishesCompleteBatch(t *testing.T) {
	fixture := newNormalBatchFixture(t, 0, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	_, open, _ := applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	command := encodeCommand(t, commandValue(fixture.binding, 1))
	entries := normalBatchEntries(3, command, nil)
	witnesses := normalBatchWitnesses(entries)
	applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
	if err != nil || applied != len(entries) {
		t.Fatalf("ApplyNormalBatch = %d, %+v, %v", applied, publication, err)
	}
	if witnesses[len(entries)-1] != publication.DataChainDigest {
		t.Fatalf("final witness = %x, want %x",
			witnesses[len(entries)-1], publication.DataChainDigest)
	}
	if err := fixture.group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	want := publication
	reopened := fixture.crashReopen(t)
	assertPublicationEqual(t, reopened.machine.Published(), want)
	if reopened.machine.Applied() != want.Applied {
		t.Fatalf("recovered applied = %d, want %d", reopened.machine.Applied(), want.Applied)
	}
	if stats := reopened.group.Stats(); stats.LargestUpdateSpan != uint64(len(entries)) {
		t.Fatalf("recovered checkpoint-group stats = %+v", stats)
	}
	value, found, err := reopened.user.Collection.AppendRaw(nil, []byte("k1"))
	if err != nil || !found || !bytes.Equal(value, []byte(`{"n":1}`)) {
		t.Fatalf("recovered user row = %q, %v, %v", value, found, err)
	}
	for index, encoded := range [][]byte{open, command} {
		lookup, lookupErr := reopened.machine.LookupCompletion(encoded)
		if lookupErr != nil || lookup.AppliedSequence != uint64(index)+2 {
			t.Fatalf("recovered completion %d = %+v, %v", index, lookup, lookupErr)
		}
	}
}

func BenchmarkApplyNormalBatchCommittedRun(b *testing.B) {
	benchmarkApplyNormalBatchCommittedRun(b, 8)
}

func BenchmarkApplyNormalBatchCommittedRun128(b *testing.B) {
	benchmarkApplyNormalBatchCommittedRun(b, raftmodel.MaxNormalApplyBatchEntries)
}

func benchmarkApplyNormalBatchCommittedRun(b *testing.B, entriesPerBatch int) {
	fixture := newNormalBatchFixtureWithSystemDocuments(
		b, MaxDistinctMutations, 8, 2*entriesPerBatch+1,
	)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		b.Fatal(err)
	}
	sessions := make([]replication.Command, entriesPerBatch)
	openEntries := make([]raftmodel.NormalApply, entriesPerBatch)
	for index := range sessions {
		prototype := commandValue(fixture.binding, 1)
		prototype.ClientID = id128(byte(index + 1))
		prototype.ClientEpoch = 2 + uint64(index)
		sessions[index] = prototype
		openEntries[index] = raftmodel.NormalApply{
			Meta: normalMeta(2 + uint64(index)),
			Data: encodeCommand(b, sessionOpenFor(prototype)),
		}
	}
	if applied, _, err := fixture.machine.ApplyNormalBatch(
		openEntries, normalBatchWitnesses(openEntries),
	); err != nil || applied != entriesPerBatch {
		b.Fatalf("open session batch = %d, %v", applied, err)
	}
	entries := make([]raftmodel.NormalApply, entriesPerBatch)
	witnesses := make([][32]byte, entriesPerBatch)
	commandOrdinal := uint64(1)
	firstApplied := uint64(2 + entriesPerBatch)
	var encodedBytes int64
	initialStats := fixture.group.Stats()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		if err := fixture.group.Checkpoint(); err != nil {
			b.Fatal(err)
		}
		encodedBytes = 0
		for index := range entries {
			command := commandValue(fixture.binding, commandOrdinal)
			command.ClientID = sessions[index].ClientID
			command.ClientEpoch = sessions[index].ClientEpoch
			command.Batches[0].Mutations = []replication.Mutation{{
				Kind:  replication.MutationPut,
				Key:   []byte{0, byte(index % MaxDistinctMutations), 0xff},
				Value: []byte{'{', '"', 'n', '"', ':', byte('0' + (index+int(commandOrdinal))%10), '}'},
			}}
			encoded := encodeCommand(b, command)
			entries[index] = raftmodel.NormalApply{
				Meta: normalMeta(firstApplied + uint64(index)), Data: encoded,
			}
			encodedBytes += int64(len(encoded))
		}
		b.StartTimer()
		applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
		if err != nil || applied != entriesPerBatch {
			b.Fatalf("ApplyNormalBatch = %d, %v", applied, err)
		}
		if witnesses[entriesPerBatch-1] != publication.DataChainDigest {
			b.Fatal("final data-chain witness does not match publication")
		}
		commandOrdinal++
		firstApplied += uint64(entriesPerBatch)
	}
	b.StopTimer()
	if finalStats := fixture.group.Stats(); finalStats.Updates != initialStats.Updates+uint64(b.N) ||
		finalStats.TransactionHighWater != initialStats.TransactionHighWater+uint64(b.N) {
		b.Fatalf("physical update count = %+v -> %+v", initialStats, finalStats)
	}
	b.SetBytes(encodedBytes)
	b.ReportMetric(float64(entriesPerBatch), "logical-entries/op")
	b.ReportMetric(1, "durable-updates/op")
	b.ReportMetric(float64(normalBatchRetainedCapacityBytes(fixture.machine)), "retained-bytes")
}

func BenchmarkApplyNormalBatchLargeDeletePlanning(b *testing.B) {
	fixture := newNormalBatchFixture(b, 0, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		b.Fatal(err)
	}
	applySessionOpen(b, fixture.machine, 2, commandValue(fixture.binding, 1))
	const documents = 8
	largeValue := make([]byte, 0, 128<<10)
	largeValue = append(largeValue, `{"value":"`...)
	largeValue = append(largeValue, bytes.Repeat([]byte{'x'}, (128<<10)-32)...)
	largeValue = append(largeValue, '"', '}')
	keys := make([][]byte, documents)
	for index := range keys {
		keys[index] = []byte{0, 'b', 'e', 'n', 'c', 'h', byte(index)}
	}
	commandOrdinal := uint64(1)
	nextApplied := uint64(3)
	witnesses := make([][32]byte, 1)
	b.ReportAllocs()
	b.SetBytes(int64(documents * len(largeValue)))
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		put := commandValue(fixture.binding, commandOrdinal)
		put.Batches[0].Mutations = make([]replication.Mutation, documents)
		for index := range keys {
			put.Batches[0].Mutations[index] = replication.Mutation{
				Kind: replication.MutationPut, Key: keys[index], Value: largeValue,
			}
		}
		if _, err := fixture.machine.ApplyNormal(
			normalMeta(nextApplied), encodeCommand(b, put),
		); err != nil {
			b.Fatal(err)
		}
		commandOrdinal++
		nextApplied++
		if err := fixture.group.Checkpoint(); err != nil {
			b.Fatal(err)
		}
		remove := commandValue(fixture.binding, commandOrdinal)
		remove.Batches[0].Mutations = make([]replication.Mutation, documents)
		for index := range keys {
			remove.Batches[0].Mutations[index] = replication.Mutation{
				Kind: replication.MutationDelete, Key: keys[index],
			}
		}
		entry := normalBatchEntries(nextApplied, encodeCommand(b, remove))
		b.StartTimer()
		applied, publication, err := fixture.machine.ApplyNormalBatch(entry, witnesses)
		if err != nil || applied != 1 {
			b.Fatalf("large delete batch = %d, %v", applied, err)
		}
		if witnesses[0] != publication.DataChainDigest {
			b.Fatal("large delete final witness does not match publication")
		}
		if reads, hashes := fixture.machine.batchTelemetry.logicalValueReads,
			fixture.machine.batchTelemetry.logicalValueHashes; reads != documents || hashes != documents {
			b.Fatalf("large delete work = %d reads, %d hashes", reads, hashes)
		}
		commandOrdinal++
		nextApplied++
	}
	b.ReportMetric(documents, "logical-before-reads/op")
	b.ReportMetric(documents, "physical-base-reads/op")
	b.ReportMetric(documents, "logical-before-hashes/op")
	b.ReportMetric(0, "logical-after-hashes/op")
}
