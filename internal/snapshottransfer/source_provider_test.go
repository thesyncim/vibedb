package snapshottransfer

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
)

type retainedTestCut struct {
	cut   *replicatedstate.ReadSnapshot
	err   error
	calls int
}

func (source *retainedTestCut) SnapshotArtifactCut() (*replicatedstate.ReadSnapshot, error) {
	source.calls++
	return source.cut, source.err
}

func TestRetainedSourceProviderExportsAndObservesAfterReopen(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	path := filepath.Join(root, "source-artifacts")
	node := rafttransport.NodeID{31}
	options := retainedSourceOptions(fixture, root, path, node, &retainedTestCut{cut: cut})
	provider, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	request := retainedSourceRequest(fixture, node)
	descriptor, err := (PinnedSourceControlExporter{Provider: provider}).
		ExportReplicaMoveSnapshot(context.Background(), request)
	if err != nil || !descriptorMatchesSourceRequest(descriptor, request) {
		t.Fatalf("descriptor=%+v err=%v", descriptor, err)
	}
	if err = provider.Close(); err != nil {
		t.Fatal(err)
	}

	// The request journal may still say Running after a lost terminal publish.
	// A new process reopens repository metadata and finds the exact artifact
	// without reacquiring a state-machine cut or rescanning relation data.
	options.Cut = &retainedTestCut{err: errors.New("must not pin")}
	reopened, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	observed, found, err := reopened.ObserveSourceExport(context.Background(), request)
	if err != nil || !found || observed != descriptor {
		t.Fatalf("observed=%+v found=%t err=%v", observed, found, err)
	}

	// The reopened control provider and source data service must expose the
	// same published repository. This is the process-restart boundary used by
	// bootstrap-rf3 after a source export completed before a crash.
	targetNode := rafttransport.NodeID{32}
	members := []rafttransport.Member{
		{Group: descriptor.Group, ReplicaSetVersion: descriptor.ReplicaSetVersion,
			MemberID: descriptor.SourceMember, Node: node, Role: rafttransport.MemberVoter},
		{Group: descriptor.Group, ReplicaSetVersion: descriptor.ReplicaSetVersion,
			MemberID: descriptor.TargetMember, Node: targetNode, Role: rafttransport.MemberLearner},
	}
	registry, err := rafttransport.NewStaticRegistry(
		node, members, rafttransport.Limits{MaxGroups: 1, MaxMembers: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	service, err := reopened.NewDataService(ServiceOptions{
		Registry: registry, Authorize: func(got Descriptor) bool { return got == descriptor },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 1,
		MaxChunkBytes: MinChunkBytes, MaxInflightBytes: MinChunkBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceIdentity := rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: node}
	targetIdentity := rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: targetNode}
	opener := sourceProviderTestOpener{
		service: service, source: sourceIdentity, target: targetIdentity,
	}
	targetRepository := openTestRepository(t, filepath.Join(t.TempDir(), "target"))
	receiver := Receiver{
		Repository: targetRepository, Opener: &opener,
		ReadDeadline: deadline, WriteDeadline: deadline, Workspace: make([]byte, MinChunkBytes),
	}
	if err = receiver.Receive(context.Background(), node, descriptor); err != nil {
		t.Fatal(err)
	}
	if _, err = targetRepository.Manifest(descriptor); err != nil {
		t.Fatalf("transferred artifact is not published: %v", err)
	}
}

func TestRetainedSourceProviderAcceptsDynamicLearnerTargetRequest(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{39}
	options := retainedSourceOptions(fixture, root, filepath.Join(root, "source-artifacts"), node,
		&retainedTestCut{cut: cut})
	options.DynamicTarget = true
	options.TargetMember = 0
	options.TargetStore = [16]byte{}
	options.TargetIncarnation = 0
	provider, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	descriptor, err := (PinnedSourceControlExporter{Provider: provider}).
		ExportReplicaMoveSnapshot(context.Background(), request)
	if err != nil || !descriptorMatchesSourceRequest(descriptor, request) {
		t.Fatalf("dynamic descriptor=%+v err=%v", descriptor, err)
	}
}

func TestRetainedSourceProviderResumesOriginalCutAfterCancellationAndLiveAdvance(t *testing.T) {
	cut, machine, fixture := sourceExportFixtureWithMachine(t, sourceExportLimits())
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	applySourceProviderLargeMutation(t, machine, fixture.ExpectedFence.Binding, 2, 0, nil)
	applySourceProviderLargeMutation(t, machine, fixture.ExpectedFence.Binding, 3, 1,
		[]byte(`{"payload":"`+strings.Repeat("x", 70<<10)+`"}`))
	exportCut, err := machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	fixture.ExpectedFence = exportCut.Fence()
	source := &retainedTestCut{cut: exportCut}
	root := t.TempDir()
	node := rafttransport.NodeID{53}
	options := retainedSourceOptions(fixture, root, filepath.Join(root, "artifacts"), node, source)
	config := transferBudgetConfig(1, 1<<30, MinChunkBytes)
	config.BufferBytes = 2 * MinChunkBytes
	budget, err := migrationbudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	options.Budget = budget
	provider, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	ctx, cancel := context.WithCancel(context.Background())
	faultCalls := 0
	provider.repository.fault = func(point repositoryFault) error {
		if point == faultAfterCursorTempSync {
			faultCalls++
			if faultCalls == 2 {
				cancel()
				return context.Canceled
			}
		}
		return nil
	}
	descriptor, err := (PinnedSourceControlExporter{Provider: provider}).
		ExportReplicaMoveSnapshot(ctx, request)
	if !errors.Is(err, context.Canceled) || !descriptor.Valid() || faultCalls != 2 {
		t.Fatalf("partial export descriptor=%+v fault_calls=%d err=%v", descriptor, faultCalls, err)
	}
	offset, complete, err := provider.repository.Offset(descriptor)
	if err != nil || complete || offset == 0 {
		t.Fatalf("partial export offset=%d complete=%t err=%v", offset, complete, err)
	}
	metrics := budget.Metrics()
	if metrics.Active != 0 || metrics.BufferUsedBytes != 0 {
		t.Fatalf("canceled export retained budget credits: %+v", metrics)
	}

	// Advance the live applied index while keeping the same learner-bearing
	// replica version. A retry must use the original pinned ReadSnapshot, so
	// its descriptor and already-durable repository cursor remain identical.
	applySourceProviderLargeMutation(t, machine, fixture.ExpectedFence.Binding, 4, 2,
		[]byte(`{"payload":"advanced"}`))
	liveCut, err := machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	defer liveCut.Close()
	if liveCut.Fence().Applied <= exportCut.Fence().Applied {
		t.Fatalf("live fence did not advance: old=%+v live=%+v", exportCut.Fence(), liveCut.Fence())
	}
	source.cut = liveCut
	provider.repository.fault = nil
	retried, err := (PinnedSourceControlExporter{Provider: provider}).
		ExportReplicaMoveSnapshot(context.Background(), request)
	if err != nil || retried != descriptor || source.calls != 1 {
		t.Fatalf("retry changed immutable export: first=%+v retry=%+v cut_calls=%d err=%v",
			descriptor, retried, source.calls, err)
	}
	if offset, complete, err = provider.repository.Offset(retried); err != nil || !complete || offset != retried.ArtifactBytes {
		t.Fatalf("completed retry offset=%d complete=%t err=%v", offset, complete, err)
	}
	if len(provider.pins) != 0 {
		t.Fatalf("successful export retained pins: %d", len(provider.pins))
	}
	metrics = budget.Metrics()
	if metrics.Active != 0 || metrics.BufferUsedBytes != 0 {
		t.Fatalf("completed export retained budget credits: %+v", metrics)
	}
}

func TestRetainedSourceProviderExcludesSameRequestAndBoundsRetainedPins(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{54}
	options := retainedSourceOptions(fixture, root, filepath.Join(root, "artifacts"), node,
		&retainedTestCut{cut: cut})
	options.MaxConcurrent = 2
	provider, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	first, err := provider.PinSourceExport(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = provider.PinSourceExport(context.Background(), request); !errors.Is(err, ErrBound) {
		t.Fatalf("concurrent exact request error=%v, want ErrBound", err)
	}
	first.Release()
	second, err := provider.PinSourceExport(context.Background(), request)
	if err != nil {
		t.Fatalf("exact retry could not reuse retained cut: %v", err)
	}
	second.Release()
	if len(provider.pins) != 1 {
		t.Fatalf("exact retry pin count=%d, want 1", len(provider.pins))
	}
	other := request
	other.Operation[0]++
	third, err := provider.PinSourceExport(context.Background(), other)
	if err != nil {
		t.Fatalf("second distinct request within bound: %v", err)
	}
	third.Release()
	if len(provider.pins) != options.MaxConcurrent {
		t.Fatalf("retained pin count=%d, want bound=%d", len(provider.pins), options.MaxConcurrent)
	}
	other.Operation[0]++
	if _, err = provider.PinSourceExport(context.Background(), other); !errors.Is(err, ErrBound) {
		t.Fatalf("distinct request exceeded retained pin bound: %v", err)
	}
	if err = provider.Close(); err != nil {
		t.Fatal(err)
	}
	if len(provider.pins) != 0 {
		t.Fatalf("close retained pins: %d", len(provider.pins))
	}
}

func TestRetainedSourceProviderFinalizesCancellationAfterPublication(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{55}
	source := &retainedTestCut{cut: cut}
	provider, err := OpenRetainedSourceExportProvider(retainedSourceOptions(
		fixture, root, filepath.Join(root, "artifacts"), node, source,
	))
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	ctx, cancel := context.WithCancel(context.Background())
	provider.repository.fault = func(point repositoryFault) error {
		if point == faultAfterPublishSync {
			cancel()
		}
		return nil
	}
	descriptor, err := (PinnedSourceControlExporter{Provider: provider}).
		ExportReplicaMoveSnapshot(ctx, request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("published cancellation descriptor=%+v err=%v", descriptor, err)
	}
	if len(provider.pins) != 0 {
		t.Fatalf("cancel-after-publication retained pins: %d", len(provider.pins))
	}
	if source.calls != 1 {
		t.Fatalf("snapshot acquisitions=%d, want one", source.calls)
	}
	observed, found, observeErr := provider.ObserveSourceExport(context.Background(), request)
	if observeErr != nil || !found || !observed.Valid() {
		t.Fatalf("published artifact observation=%+v found=%t err=%v", observed, found, observeErr)
	}
	if _, complete, offsetErr := provider.repository.Offset(observed); offsetErr != nil || !complete {
		t.Fatalf("canceled publication complete=%t err=%v", complete, offsetErr)
	}
	descriptor = observed
	if err = provider.ReleaseSourceExport(context.Background(), request, descriptor); err != nil {
		t.Fatalf("release published export: %v", err)
	}
	if _, found, err = provider.ObserveSourceExport(context.Background(), request); err != nil || found {
		t.Fatalf("released artifact found=%t err=%v", found, err)
	}
}

func TestRetainedSourceProviderAbandonmentRetiresPinnedCut(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{56}
	source := &retainedTestCut{cut: cut}
	provider, err := OpenRetainedSourceExportProvider(retainedSourceOptions(
		fixture, root, filepath.Join(root, "artifacts"), node, source,
	))
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	ctx, cancel := context.WithCancel(context.Background())
	provider.repository.fault = func(point repositoryFault) error {
		if point == faultAfterCursorTempSync {
			cancel()
			return context.Canceled
		}
		return nil
	}
	descriptor, err := (PinnedSourceControlExporter{Provider: provider}).
		ExportReplicaMoveSnapshot(ctx, request)
	if !errors.Is(err, context.Canceled) || !descriptor.Valid() {
		t.Fatalf("canceled stage descriptor=%+v err=%v", descriptor, err)
	}
	witness := testAbandonmentWitness(request, descriptor)
	provider.repository.fault = nil
	if err = provider.AbandonSourceExport(context.Background(), request, witness); err != nil {
		t.Fatalf("abandon partial export: %v", err)
	}
	if len(provider.pins) != 0 || provider.repository.Stats().Staged != 0 {
		t.Fatalf("abandon retained snapshot or stage: pins=%d stats=%+v", len(provider.pins), provider.repository.Stats())
	}
}

func applySourceProviderLargeMutation(
	t testing.TB,
	machine *replicatedstate.Machine,
	binding replicatedstate.Binding,
	index, sequence uint64,
	value []byte,
) {
	t.Helper()
	command := replication.Command{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration, Tenant: []byte("source-export-test"),
		ClientID: replication.ID128{77}, ClientEpoch: 2, ClientSequence: sequence + 1,
		Fingerprint: sha256.Sum256(value),
		Batches: []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("large"), Value: value,
		}}}},
	}
	if index == 2 {
		command.Kind = replication.CommandSessionOpen
		command.ClientEpoch = 0
		command.ClientSequence = 1
		command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
		command.Fingerprint = sha256.Sum256([]byte("source-export-test-session"))
		command.Batches = nil
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatalf("encode source mutation: %v", err)
	}
	if err = machine.AdmitCommand(encoded); err != nil {
		t.Fatalf("admit source mutation: %v", err)
	}
	publication, err := machine.ApplyNormal(raftmodel.ApplyMeta{Index: index, Term: 2, Type: pb.EntryNormal}, encoded)
	if err != nil || publication.Applied != index {
		t.Fatalf("apply source mutation index=%d publication=%+v err=%v", index, publication, err)
	}
}

type sourceProviderTestOpener struct {
	service        *Service
	source, target rafttransport.PeerIdentity
}

func (opener *sourceProviderTestOpener) OpenSnapshot(
	ctx context.Context,
	node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	if node != opener.source.Node {
		return nil, rafttransport.ErrNodeNotFound
	}
	client, server := net.Pipe()
	go func() {
		_ = opener.service.Serve(ctx, &testPeerConn{
			Conn: server, identity: opener.target, class: rafttransport.TrafficSnapshot,
		})
	}()
	return &testPeerConn{
		Conn: client, identity: opener.source, class: rafttransport.TrafficSnapshot,
	}, nil
}

func TestRetainedSourceProviderRejectsWrongIdentityAndStaleMembership(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{41}
	provider, err := OpenRetainedSourceExportProvider(retainedSourceOptions(
		fixture, root, filepath.Join(root, "artifacts"), node, &retainedTestCut{cut: cut},
	))
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	wrong := request
	wrong.SourceNode[0]++
	if _, err = provider.PinSourceExport(context.Background(), wrong); !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("wrong source node err=%v", err)
	}
	// A request for membership this replica has not applied yet is transient:
	// the donor is behind, not wrong. Each exact retry takes a fresh cut, and a
	// behind cut returns its sole workspace instead of wedging later work.
	request.ReplicaSetVersion++
	source := provider.options.Cut.(*retainedTestCut)
	for attempt := 1; attempt <= 2; attempt++ {
		_, err = provider.PinSourceExport(context.Background(), request)
		if !errors.Is(err, ErrSourceNotCaughtUp) || errors.Is(err, ErrStaleFence) {
			t.Fatalf("behind replica-set version attempt %d err=%v", attempt, err)
		}
		if pin := provider.pins[request]; pin != nil {
			t.Fatalf("behind cut was retained as a pin: %+v", pin)
		}
		if source.calls != attempt {
			t.Fatalf("attempt %d did not take a fresh cut: calls=%d", attempt, source.calls)
		}
	}
}

type staleFenceSourcePlanProvider struct{ SourceExportPlanProvider }

func (provider staleFenceSourcePlanProvider) PinSourceExport(
	ctx context.Context, request SourceControlRequest,
) (SourceExportPlan, error) {
	plan, err := provider.SourceExportPlanProvider.PinSourceExport(ctx, request)
	if err == nil {
		plan.ExpectedFence.Applied++
	}
	return plan, err
}

func TestRetainedSourceProviderTerminalizesPermanentExportFailure(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{57}
	source := &retainedTestCut{cut: cut}
	options := retainedSourceOptions(fixture, root, filepath.Join(root, "artifacts"), node, source)
	config := transferBudgetConfig(1, 1<<30, MinChunkBytes)
	config.BufferBytes = 2 * MinChunkBytes
	budget, err := migrationbudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	options.Budget = budget
	provider, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	exporter := PinnedSourceControlExporter{Provider: staleFenceSourcePlanProvider{provider}}
	if _, err = exporter.ExportReplicaMoveSnapshot(context.Background(), request); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("permanent export error=%v, want ErrStaleFence", err)
	}
	pin := provider.pins[request]
	if pin == nil || pin.snapshot != nil || !errors.Is(pin.terminalErr, ErrStaleFence) || pin.busy {
		t.Fatalf("permanent failure did not close cut and retain terminal request: %+v", pin)
	}
	if metrics := budget.Metrics(); metrics.Active != 0 || metrics.BufferUsedBytes != 0 {
		t.Fatalf("permanent failure retained budget credits: %+v", metrics)
	}
	if _, err = exporter.ExportReplicaMoveSnapshot(context.Background(), request); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("exact retry changed terminal error: %v", err)
	}
	if source.calls != 1 {
		t.Fatalf("exact retry minted another source cut: calls=%d", source.calls)
	}
}

func TestRetainedSourceProviderReservesBothWorkspacesAtomically(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{46}
	config := transferBudgetConfig(2, 1<<30, MinChunkBytes)
	config.BufferBytes = 2 * MinChunkBytes
	budget, err := migrationbudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	options := retainedSourceOptions(fixture, root, filepath.Join(root, "artifacts"), node,
		&retainedTestCut{cut: cut})
	options.Budget = budget
	options.MaxConcurrent = 2
	provider, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	first, err := provider.PinSourceExport(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		second, pinErr := provider.PinSourceExport(ctx, request)
		if second.Release != nil {
			second.Release()
		}
		result <- pinErr
	}()
	deadline := time.Now().Add(time.Second)
	for budget.Metrics().BufferWaiters == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if budget.Metrics().BufferWaiters == 0 {
		t.Fatalf("second plan did not wait for one atomic workspace reservation: %+v", budget.Metrics())
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("second plan cancellation = %v", err)
	}
}

func TestRetainedSourceProviderSaturationReleasesPlan(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{47}
	options := retainedSourceOptions(fixture, root, filepath.Join(root, "artifacts"), node,
		&retainedTestCut{cut: cut})
	options.MaxConcurrent = 1
	provider, err := OpenRetainedSourceExportProvider(options)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	request := retainedSourceRequest(fixture, node)
	first, err := provider.PinSourceExport(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = provider.PinSourceExport(context.Background(), request); !errors.Is(err, ErrBound) {
		t.Fatalf("saturated pin error = %v, want ErrBound", err)
	}
	released := make(chan struct{})
	go func() {
		first.Release()
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("saturated plan release blocked returning its workspace")
	}
	second, err := provider.PinSourceExport(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second.Release()
}

func TestRetainedSourceProviderRepositoryPathCannotEscapeOrTraverseSymlink(t *testing.T) {
	cut, fixture := sourceExportFixture(t, sourceExportLimits())
	root := t.TempDir()
	node := rafttransport.NodeID{51}
	options := retainedSourceOptions(
		fixture, root, filepath.Join(filepath.Dir(root), "escape"), node,
		&retainedTestCut{cut: cut},
	)
	if _, err := OpenRetainedSourceExportProvider(options); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("escaped repository err=%v", err)
	}
	outside := t.TempDir()
	link := filepath.Join(root, "linked")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	options.RepositoryPath = filepath.Join(link, "artifacts")
	if _, err := OpenRetainedSourceExportProvider(options); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("symlink repository err=%v", err)
	}
}

func retainedSourceOptions(
	fixture SourceExportPlan,
	root, path string,
	node rafttransport.NodeID,
	cut SourceArtifactCut,
) RetainedSourceExportOptions {
	return RetainedSourceExportOptions{
		DataRoot: root, RepositoryPath: path, Limits: sourceExportLimits(),
		ChunkBytes: MinChunkBytes, MaxConcurrent: 1,
		RuntimeIdentity: raftmember.RuntimeIdentity{
			Group: fixture.Group, Distribution: "export", Shard: "all",
			AllocationGeneration: 4, MemberID: fixture.SourceMember,
			StoreID: [16]byte{61}, NodeIncarnation: 62,
			RelationManifestDigest: fixture.ExpectedFence.RelationManifestDigest,
		},
		SourceNode: node, TargetMember: fixture.TargetMember,
		TargetStore: fixture.TargetStore, TargetIncarnation: fixture.TargetIncarnation,
		Cut: cut,
	}
}

func retainedSourceRequest(
	fixture SourceExportPlan,
	node rafttransport.NodeID,
) SourceControlRequest {
	return SourceControlRequest{
		Operation: [32]byte{1}, Step: [32]byte{2}, Group: fixture.Group,
		SourceMember: fixture.SourceMember, TargetMember: fixture.TargetMember,
		TargetStore: fixture.TargetStore, TargetIncarnation: fixture.TargetIncarnation,
		ReplicaSetVersion: fixture.ExpectedFence.ReplicaSetVersion, SourceNode: node,
	}
}
