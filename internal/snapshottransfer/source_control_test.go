package snapshottransfer

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type memorySourceJournal struct {
	mu         sync.Mutex
	records    map[[32]byte]SourceControlRecord
	fault      error
	faultState SourceControlState
	faultOnce  bool
}

func (journal *memorySourceJournal) ReadSourceExport(
	_ context.Context,
	operation [32]byte,
) (SourceControlRecord, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	record, found := journal.records[operation]
	if !found {
		return SourceControlRecord{}, ErrSourceMissing
	}
	return record, nil
}

func (journal *memorySourceJournal) PublishSourceExport(
	_ context.Context,
	expected uint64,
	record SourceControlRecord,
) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	current, found := journal.records[record.Request.Operation]
	if expected == 0 && found || expected != 0 && (!found || current.Revision != expected) {
		return ErrSourceConflict
	}
	journal.records[record.Request.Operation] = record
	if journal.fault != nil && !journal.faultOnce &&
		(journal.faultState == 0 || journal.faultState == record.State) {
		journal.faultOnce = true
		return journal.fault
	}
	return nil
}

type testSourceExporter struct {
	mu               sync.Mutex
	descriptor       Descriptor
	exported         bool
	exportCalls      int
	errorAfterExport error
	releaseCalls     int
	releaseErr       error
}

func (exporter *testSourceExporter) ReleaseReplicaMoveSnapshot(
	context.Context, SourceControlRequest, Descriptor,
) error {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	exporter.releaseCalls++
	return exporter.releaseErr
}

func (exporter *testSourceExporter) ObserveReplicaMoveSnapshot(
	context.Context,
	SourceControlRequest,
) (Descriptor, bool, error) {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	return exporter.descriptor, exporter.exported, nil
}

func (exporter *testSourceExporter) ExportReplicaMoveSnapshot(
	context.Context,
	SourceControlRequest,
) (Descriptor, error) {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	exporter.exportCalls++
	exporter.exported = true
	return exporter.descriptor, exporter.errorAfterExport
}

func TestSourceControlPersistsIntentAndSettlesUnknownOutcomes(t *testing.T) {
	request, descriptor := sourceControlFixture()
	fault := errors.New("durability response lost")
	journal := &memorySourceJournal{
		records: make(map[[32]byte]SourceControlRecord), fault: fault,
		faultState: SourceControlComplete,
	}
	exporter := &testSourceExporter{descriptor: descriptor, errorAfterExport: fault}
	service := newTestSourceControl(t, journal, exporter)
	record, err := service.Execute(context.Background(), request)
	if err != nil || record.State != SourceControlComplete || record.Revision != 2 ||
		record.Request != request || record.Descriptor != descriptor ||
		exporter.exportCalls != 1 || !journal.faultOnce {
		t.Fatalf("record=%+v exports=%d fault=%t err=%v",
			record, exporter.exportCalls, journal.faultOnce, err)
	}
	replayed, err := service.Execute(context.Background(), request)
	if err != nil || replayed != record || exporter.exportCalls != 1 {
		t.Fatalf("replay=%+v exports=%d err=%v", replayed, exporter.exportCalls, err)
	}

	// A crash after repository completion but before terminal publication leaves
	// Running durable. Observe resolves it without another scan/export.
	journal.mu.Lock()
	journal.records[request.Operation] = SourceControlRecord{
		Request: request, Revision: 7, State: SourceControlRunning,
	}
	journal.mu.Unlock()
	recovered, err := service.Execute(context.Background(), request)
	if err != nil || recovered.Revision != 8 || recovered.Descriptor != descriptor ||
		exporter.exportCalls != 1 {
		t.Fatalf("recovered=%+v exports=%d err=%v", recovered, exporter.exportCalls, err)
	}
}

func TestSourceControlReleaseRequiresCompleteAndRecoversAcrossJournalReopen(t *testing.T) {
	request, descriptor := sourceControlFixture()
	path := t.TempDir() + "/source-release-journal"
	journal, err := OpenSourceFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	running := SourceControlRecord{Request: request, Revision: 1, State: SourceControlRunning}
	if err = journal.PublishSourceExport(ctx, 0, running); err != nil {
		t.Fatal(err)
	}
	exporter := &testSourceExporter{descriptor: descriptor, exported: true}
	service := newTestSourceControl(t, journal, exporter)
	if _, err = service.Release(ctx, request); !errors.Is(err, ErrSourceConflict) ||
		exporter.releaseCalls != 0 {
		t.Fatalf("running release err=%v calls=%d", err, exporter.releaseCalls)
	}
	complete := SourceControlRecord{
		Request: request, Revision: 2, State: SourceControlComplete, Descriptor: descriptor,
	}
	if err = journal.PublishSourceExport(ctx, 1, complete); err != nil {
		t.Fatal(err)
	}
	// Model a repository release whose response is lost. The durable journal
	// remains Complete, so reopen retries the exact release rather than deleting
	// early or inventing another artifact identity.
	exporter.releaseErr = ErrSourceOutcomeUnknown
	if _, err = service.Release(ctx, request); !errors.Is(err, ErrSourceOutcomeUnknown) {
		t.Fatalf("outcome-unknown release err=%v", err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenSourceFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	exporter.releaseErr = nil
	service = newTestSourceControl(t, journal, exporter)
	released, err := service.Release(ctx, request)
	if err != nil || released.State != SourceControlReleased || released.Revision != 3 ||
		exporter.releaseCalls != 2 {
		t.Fatalf("released=%+v calls=%d err=%v", released, exporter.releaseCalls, err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenSourceFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	replayed := newTestSourceControl(t, journal, exporter)
	again, err := replayed.Release(ctx, request)
	if err != nil || again != released || exporter.releaseCalls != 2 {
		t.Fatalf("idempotent release=%+v calls=%d err=%v", again, exporter.releaseCalls, err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
}

type testSourcePlanProvider struct{ plan SourceExportPlan }

func (provider *testSourcePlanProvider) ObserveSourceExport(
	context.Context, SourceControlRequest,
) (Descriptor, bool, error) {
	return Descriptor{}, false, nil
}

func (provider *testSourcePlanProvider) ReleaseSourceExport(
	_ context.Context, request SourceControlRequest, descriptor Descriptor,
) error {
	return provider.plan.Repository.ReleasePublished(ArtifactReleaseRequest{
		Operation: request.Operation, Step: request.Step, Descriptor: descriptor,
	})
}

func (provider *testSourcePlanProvider) PinSourceExport(
	context.Context, SourceControlRequest,
) (SourceExportPlan, error) {
	return provider.plan, nil
}

func TestPinnedSourceControlExporterUsesCertifiedArtifactPath(t *testing.T) {
	cut, plan := sourceExportFixture(t, sourceExportLimits())
	request := SourceControlRequest{
		Operation: [32]byte{1}, Step: [32]byte{2}, Group: plan.Group,
		SourceMember: plan.SourceMember, TargetMember: plan.TargetMember,
		TargetStore: plan.TargetStore, TargetIncarnation: plan.TargetIncarnation,
		ReplicaSetVersion: plan.ExpectedFence.ReplicaSetVersion,
		SourceNode:        rafttransport.NodeID{8},
	}
	exporter := PinnedSourceControlExporter{Provider: &testSourcePlanProvider{plan: plan}}
	descriptor, err := exporter.ExportReplicaMoveSnapshot(context.Background(), request)
	if err != nil || !descriptorMatchesSourceRequest(descriptor, request) {
		t.Fatalf("descriptor=%+v err=%v", descriptor, err)
	}
	if stats := plan.Repository.Stats(); stats.Published != 1 || stats.Staged != 0 {
		t.Fatalf("repository stats=%+v", stats)
	}
	if _, found := cut.Relation(1); found {
		t.Fatal("exporter did not close its pinned snapshot")
	}
}

func TestSourceControlRejectsConflictsAndBoundsConcurrentExports(t *testing.T) {
	request, descriptor := sourceControlFixture()
	journal := &memorySourceJournal{records: make(map[[32]byte]SourceControlRecord)}
	entered := make(chan struct{})
	release := make(chan struct{})
	exporter := &blockingSourceExporter{descriptor: descriptor, entered: entered, release: release}
	service := newTestSourceControl(t, journal, exporter)
	done := make(chan error, 1)
	go func() {
		_, err := service.Execute(context.Background(), request)
		done <- err
	}()
	<-entered
	other := request
	other.Operation[0]++
	if _, err := service.Execute(context.Background(), other); !errors.Is(err, ErrBound) {
		t.Fatalf("concurrent bound err=%v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	conflict := request
	conflict.Step[0]++
	if _, err := service.Execute(context.Background(), conflict); !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("conflicting replay err=%v", err)
	}
}

type blockingSourceExporter struct {
	descriptor Descriptor
	entered    chan struct{}
	release    chan struct{}
	exported   bool
}

func (exporter *blockingSourceExporter) ObserveReplicaMoveSnapshot(
	context.Context, SourceControlRequest,
) (Descriptor, bool, error) {
	return exporter.descriptor, exporter.exported, nil
}

func (exporter *blockingSourceExporter) ExportReplicaMoveSnapshot(
	ctx context.Context, _ SourceControlRequest,
) (Descriptor, error) {
	close(exporter.entered)
	select {
	case <-exporter.release:
		exporter.exported = true
		return exporter.descriptor, nil
	case <-ctx.Done():
		return Descriptor{}, context.Cause(ctx)
	}
}

func (*blockingSourceExporter) ReleaseReplicaMoveSnapshot(
	context.Context, SourceControlRequest, Descriptor,
) error {
	return nil
}

func TestSourceControlCanonicalFixedProtocol(t *testing.T) {
	request, descriptor := sourceControlFixture()
	encoded, err := AppendSourceControlRequest(nil, request)
	if err != nil || len(encoded) != SourceControlRequestBytes {
		t.Fatalf("request bytes=%d err=%v", len(encoded), err)
	}
	opened, err := OpenSourceControlRequest(encoded)
	if err != nil || opened != request {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	releaseRequest, err := appendSourceControlReleaseRequest(nil, request)
	command, releasedRequest, readErr := readSourceControlCommand(bytes.NewReader(releaseRequest))
	if err != nil || readErr != nil || command != sourceControlRelease || releasedRequest != request {
		t.Fatalf("release command=%d request=%+v append=%v read=%v", command, releasedRequest, err, readErr)
	}
	forged := append([]byte(nil), encoded...)
	forged[len(forged)-1] = 1
	if _, err = OpenSourceControlRequest(forged); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("noncanonical request err=%v", err)
	}
	record := SourceControlRecord{
		Request: request, Revision: 2, State: SourceControlComplete, Descriptor: descriptor,
	}
	response, err := AppendSourceControlResponse(nil, record)
	if err != nil || len(response) != SourceControlResponseBytes {
		t.Fatalf("response bytes=%d err=%v", len(response), err)
	}
	openedRecord, err := OpenSourceControlResponse(response)
	if err != nil || openedRecord != record {
		t.Fatalf("opened response=%+v err=%v", openedRecord, err)
	}
	if _, err = OpenSourceControlResponse(append(response, 0)); !errors.Is(err, ErrSourceControl) {
		t.Fatalf("trailing response err=%v", err)
	}
}

func TestSourceControlClientExecutesAuthenticatedExactRequest(t *testing.T) {
	request, descriptor := sourceControlFixture()
	controller := rafttransport.PeerIdentity{
		Node: rafttransport.NodeID{9}, TrustDomain: sourceTrustDomain(request),
	}
	sourceIdentity := rafttransport.PeerIdentity{
		Node: request.SourceNode, TrustDomain: sourceTrustDomain(request),
	}
	journal := &memorySourceJournal{records: make(map[[32]byte]SourceControlRecord)}
	exporter := &testSourceExporter{descriptor: descriptor}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewSourceControlService(SourceControlOptions{
		Journal: journal, Exporter: exporter,
		Authorize: func(peer rafttransport.PeerIdentity, got SourceControlRequest) bool {
			return peer == controller && got == request
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	opener := bootstrapOpenFunc(func(_ context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		if node != request.SourceNode {
			return nil, ErrSourceConflict
		}
		clientSide, serverSide := net.Pipe()
		go func() {
			done <- service.Serve(context.Background(), &testPeerConn{
				Conn: serverSide, identity: controller, class: rafttransport.TrafficShardControl,
			})
		}()
		return &testPeerConn{
			Conn: clientSide, identity: sourceIdentity, class: rafttransport.TrafficShardControl,
		}, nil
	})
	client, err := NewSourceControlClient(SourceControlClientOptions{
		Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.PrepareReplicaMoveSnapshot(context.Background(), request)
	serveErr := <-done
	if err != nil || serveErr != nil || got != descriptor || exporter.exportCalls != 1 {
		t.Fatalf("descriptor=%+v exports=%d err=%v serveErr=%v",
			got, exporter.exportCalls, err, serveErr)
	}
	if err = client.ReleaseReplicaMoveSnapshot(context.Background(), request, descriptor); err != nil {
		t.Fatal(err)
	}
	if serveErr = <-done; serveErr != nil || exporter.releaseCalls != 1 {
		t.Fatalf("release calls=%d serveErr=%v", exporter.releaseCalls, serveErr)
	}
}

func TestSourceControlClientReturnsOutcomeUnknownAfterWrite(t *testing.T) {
	request, _ := sourceControlFixture()
	peer := rafttransport.PeerIdentity{Node: request.SourceNode, TrustDomain: sourceTrustDomain(request)}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	received := make(chan error, 1)
	opens := 0
	client, err := NewSourceControlClient(SourceControlClientOptions{
		Opener: bootstrapOpenFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
			opens++
			clientSide, serverSide := net.Pipe()
			go func() {
				_, readErr := ReadSourceControlRequest(serverSide)
				received <- readErr
				_ = serverSide.Close()
			}()
			return &testPeerConn{Conn: clientSide, identity: peer, class: rafttransport.TrafficShardControl}, nil
		}),
		ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.PrepareReplicaMoveSnapshot(context.Background(), request); !errors.Is(err, ErrSourceOutcomeUnknown) {
		t.Fatalf("response loss err=%v", err)
	}
	if receiveErr := <-received; receiveErr != nil || opens != 1 {
		t.Fatalf("receive err=%v opens=%d", receiveErr, opens)
	}
}

func newTestSourceControl(
	t testing.TB,
	journal SourceControlJournal,
	exporter SourceControlExporter,
) *SourceControlService {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewSourceControlService(SourceControlOptions{
		Journal: journal, Exporter: exporter,
		Authorize:    func(rafttransport.PeerIdentity, SourceControlRequest) bool { return true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func sourceControlFixture() (SourceControlRequest, Descriptor) {
	descriptor := testDescriptor(bytes.Repeat([]byte("source-control"), 400))
	request := SourceControlRequest{
		Operation: [32]byte{1}, Step: [32]byte{2}, Group: descriptor.Group,
		SourceMember: descriptor.SourceMember, TargetMember: descriptor.TargetMember,
		TargetStore: descriptor.TargetStore, TargetIncarnation: descriptor.TargetIncarnation,
		ReplicaSetVersion: descriptor.ReplicaSetVersion, SourceNode: rafttransport.NodeID{8},
	}
	return request, descriptor
}

func sourceTrustDomain(request SourceControlRequest) rafttransport.TrustDomain {
	return rafttransport.TrustDomain{
		ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation,
	}
}
