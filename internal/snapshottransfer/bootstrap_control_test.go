package snapshottransfer

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type memoryBootstrapJournal struct {
	mu         sync.Mutex
	records    map[[32]byte]BootstrapRecord
	fault      error
	faultState BootstrapState
	faultOnce  bool
}

func TestBootstrapRequestDiscriminatorMatchesCanonicalWire(t *testing.T) {
	request, _, _ := bootstrapControlFixture()
	raw, err := AppendBootstrapRequest(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	discriminator := BootstrapRequestDiscriminator()
	if !bytes.Equal(raw[:len(discriminator)], discriminator[:]) {
		t.Fatalf("discriminator=%x wire=%x", discriminator, raw[:len(discriminator)])
	}
}

func (journal *memoryBootstrapJournal) ReadBootstrap(
	_ context.Context,
	operation [32]byte,
) (BootstrapRecord, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	record, found := journal.records[operation]
	if !found {
		return BootstrapRecord{}, ErrBootstrapMissing
	}
	return record, nil
}

func (journal *memoryBootstrapJournal) PublishBootstrap(
	_ context.Context,
	expected uint64,
	record BootstrapRecord,
) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	current, found := journal.records[record.Request.Operation]
	if expected == 0 && found || expected != 0 && (!found || current.Revision != expected) {
		return ErrBootstrapConflict
	}
	journal.records[record.Request.Operation] = record
	if journal.fault != nil && !journal.faultOnce &&
		(journal.faultState == 0 || journal.faultState == record.State) {
		journal.faultOnce = true
		return journal.fault
	}
	return nil
}

type bootstrapReceiveFunc func(context.Context, rafttransport.NodeID, Descriptor) error

func (receive bootstrapReceiveFunc) Receive(
	ctx context.Context,
	node rafttransport.NodeID,
	descriptor Descriptor,
) error {
	return receive(ctx, node, descriptor)
}

type testBootstrapInstaller struct {
	mu                sync.Mutex
	identity          raftmember.RuntimeIdentity
	installed         bool
	installCalls      int
	errorAfterInstall error
}

func (installer *testBootstrapInstaller) ObserveInstalled(
	context.Context,
	Descriptor,
) (raftmember.RuntimeIdentity, bool, error) {
	installer.mu.Lock()
	defer installer.mu.Unlock()
	return installer.identity, installer.installed, nil
}

func (installer *testBootstrapInstaller) InstallPublishedLearner(
	context.Context,
	Descriptor,
) (raftmember.RuntimeIdentity, error) {
	installer.mu.Lock()
	defer installer.mu.Unlock()
	installer.installCalls++
	installer.installed = true
	return installer.identity, installer.errorAfterInstall
}

func TestBootstrapControlPersistsIntentBeforeReceiveAndReplaysTerminalIdentity(t *testing.T) {
	request, identity, source := bootstrapControlFixture()
	journal := &memoryBootstrapJournal{records: make(map[[32]byte]BootstrapRecord)}
	installer := &testBootstrapInstaller{identity: identity}
	receives := 0
	service := newTestBootstrapControl(t, journal, installer,
		func(_ context.Context, node rafttransport.NodeID, descriptor Descriptor) error {
			receives++
			if node != source || descriptor != request.Descriptor {
				t.Fatal("receiver observed changed source or descriptor")
			}
			record, err := journal.ReadBootstrap(context.Background(), request.Operation)
			if err != nil || record.State != BootstrapRunning || record.Revision != 1 {
				t.Fatalf("receive before durable intent: record=%+v err=%v", record, err)
			}
			return nil
		})
	record, err := service.Execute(context.Background(), request)
	if err != nil || record.State != BootstrapComplete || record.Revision != 2 ||
		record.Identity != identity || receives != 1 || installer.installCalls != 1 {
		t.Fatalf("record=%+v receives=%d installs=%d err=%v", record, receives, installer.installCalls, err)
	}
	replayed, err := service.Execute(context.Background(), request)
	if err != nil || replayed != record || receives != 1 || installer.installCalls != 1 {
		t.Fatalf("replay=%+v receives=%d installs=%d err=%v", replayed, receives, installer.installCalls, err)
	}
	observed, err := service.Observe(context.Background(), request.Operation)
	if err != nil || observed != record {
		t.Fatalf("observe=%+v err=%v", observed, err)
	}
}

func TestBootstrapControlSettlesInstallAndJournalOutcomeUnknown(t *testing.T) {
	request, identity, _ := bootstrapControlFixture()
	fault := errors.New("durability response lost")
	journal := &memoryBootstrapJournal{
		records: make(map[[32]byte]BootstrapRecord), fault: fault, faultState: BootstrapComplete,
	}
	installer := &testBootstrapInstaller{identity: identity, errorAfterInstall: fault}
	service := newTestBootstrapControl(t, journal, installer,
		func(context.Context, rafttransport.NodeID, Descriptor) error { return nil })
	record, err := service.Execute(context.Background(), request)
	if err != nil || record.State != BootstrapComplete || record.Identity != identity ||
		installer.installCalls != 1 || !journal.faultOnce {
		t.Fatalf("record=%+v installs=%d journalFault=%t err=%v",
			record, installer.installCalls, journal.faultOnce, err)
	}

	// Simulate a crash after install but before terminal publication by placing
	// the durable operation back at Running. Observation must bypass receive and
	// a second install.
	journal.mu.Lock()
	journal.records[request.Operation] = BootstrapRecord{
		Request: request, Revision: 7, State: BootstrapRunning,
	}
	journal.mu.Unlock()
	record, err = service.Execute(context.Background(), request)
	if err != nil || record.Revision != 8 || installer.installCalls != 1 {
		t.Fatalf("post-install replay=%+v installs=%d err=%v", record, installer.installCalls, err)
	}
}

func TestBootstrapControlDurablyCertifiesBeforeReleaseAndRedrivesTerminalCleanup(t *testing.T) {
	request, identity, source := bootstrapControlFixture()
	journal := &memoryBootstrapJournal{records: make(map[[32]byte]BootstrapRecord)}
	installer := &testBootstrapInstaller{identity: identity}
	releaseCalls := 0
	releaseFault := errors.New("release result lost")
	service, err := NewBootstrapControlService(BootstrapControlOptions{
		Journal:   journal,
		Receiver:  bootstrapReceiveFunc(func(context.Context, rafttransport.NodeID, Descriptor) error { return nil }),
		Installer: installer,
		Releaser: BootstrapArtifactReleaseFunc(func(_ context.Context, got BootstrapRequest, gotIdentity raftmember.RuntimeIdentity) error {
			releaseCalls++
			record, readErr := journal.ReadBootstrap(context.Background(), request.Operation)
			if readErr != nil || record.State != BootstrapComplete || got != request || gotIdentity != identity {
				t.Fatalf("release before exact terminal certificate: record=%+v request=%+v identity=%+v err=%v", record, got, gotIdentity, readErr)
			}
			if releaseCalls == 1 {
				return releaseFault
			}
			return nil
		}),
		Authorize:     func(rafttransport.PeerIdentity, BootstrapRequest) bool { return true },
		SourceNode:    func(Descriptor) (rafttransport.NodeID, bool) { return source, true },
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
		MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Execute(context.Background(), request); !errors.Is(err, releaseFault) {
		t.Fatalf("first release = %v", err)
	}
	record, err := service.Execute(context.Background(), request)
	if err != nil || record.State != BootstrapComplete || releaseCalls != 2 || installer.installCalls != 1 {
		t.Fatalf("retry record=%+v releases=%d installs=%d err=%v", record, releaseCalls, installer.installCalls, err)
	}
}

func TestBootstrapControlResumesReceiveFromDurableRunningIntent(t *testing.T) {
	request, identity, _ := bootstrapControlFixture()
	journal := &memoryBootstrapJournal{records: make(map[[32]byte]BootstrapRecord)}
	installer := &testBootstrapInstaller{identity: identity}
	disconnect := errors.New("snapshot stream disconnected")
	receives := 0
	service := newTestBootstrapControl(t, journal, installer,
		func(context.Context, rafttransport.NodeID, Descriptor) error {
			receives++
			if receives == 1 {
				return disconnect
			}
			return nil
		})
	if _, err := service.Execute(context.Background(), request); !errors.Is(err, disconnect) {
		t.Fatalf("first receive err=%v", err)
	}
	running, err := service.Observe(context.Background(), request.Operation)
	if err != nil || running.State != BootstrapRunning || running.Revision != 1 {
		t.Fatalf("running=%+v err=%v", running, err)
	}
	complete, err := service.Execute(context.Background(), request)
	if err != nil || complete.State != BootstrapComplete || receives != 2 || installer.installCalls != 1 {
		t.Fatalf("complete=%+v receives=%d installs=%d err=%v",
			complete, receives, installer.installCalls, err)
	}
}

func TestBootstrapControlRejectsConflictAndBoundsConcurrentWork(t *testing.T) {
	request, identity, _ := bootstrapControlFixture()
	journal := &memoryBootstrapJournal{records: make(map[[32]byte]BootstrapRecord)}
	installer := &testBootstrapInstaller{identity: identity}
	entered := make(chan struct{})
	release := make(chan struct{})
	service := newTestBootstrapControl(t, journal, installer,
		func(ctx context.Context, _ rafttransport.NodeID, _ Descriptor) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		})
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
	if _, err := service.Execute(context.Background(), conflict); !errors.Is(err, ErrBootstrapConflict) {
		t.Fatalf("conflicting replay err=%v", err)
	}
}

func TestBootstrapControlFixedRequestAndBoundedResponseProtocol(t *testing.T) {
	request, identity, _ := bootstrapControlFixture()
	encoded, err := AppendBootstrapRequest(nil, request)
	if err != nil || len(encoded) != BootstrapRequestBytes {
		t.Fatalf("request bytes=%d err=%v", len(encoded), err)
	}
	opened, err := OpenBootstrapRequest(encoded)
	if err != nil || opened != request {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	if _, err = OpenBootstrapRequest(append(encoded, 0)); !errors.Is(err, ErrBootstrapControl) {
		t.Fatalf("trailing request err=%v", err)
	}
	record := BootstrapRecord{Request: request, Revision: 2, State: BootstrapComplete, Identity: identity}
	response, err := AppendBootstrapResponse(nil, record)
	if err != nil || len(response) > MaxBootstrapResponseBytes {
		t.Fatalf("response bytes=%d err=%v", len(response), err)
	}
	openedResponse, err := OpenBootstrapResponse(response)
	if err != nil || openedResponse.Revision != record.Revision ||
		openedResponse.Request.Operation != request.Operation ||
		openedResponse.Request.Step != request.Step || openedResponse.Identity != identity {
		t.Fatalf("opened response=%+v err=%v", openedResponse, err)
	}
	forged := append([]byte(nil), response...)
	forged[9] = 1
	if _, err = OpenBootstrapResponse(forged); !errors.Is(err, ErrBootstrapControl) {
		t.Fatalf("noncanonical response err=%v", err)
	}
	if _, err = NewBootstrapControlService(BootstrapControlOptions{MaxConcurrent: AbsoluteMaxBootstrapConcurrency + 1}); !errors.Is(err, ErrBootstrapControl) {
		t.Fatalf("constructor bound err=%v", err)
	}
}

func TestBootstrapControlServeAuthenticatesAndReturnsTerminalObservation(t *testing.T) {
	request, identity, source := bootstrapControlFixture()
	journal := &memoryBootstrapJournal{records: make(map[[32]byte]BootstrapRecord)}
	installer := &testBootstrapInstaller{identity: identity}
	controller := rafttransport.PeerIdentity{Node: rafttransport.NodeID{9}, TrustDomain: rafttransport.TrustDomain{
		ClusterID:          request.Descriptor.Group.ClusterID,
		ClusterIncarnation: request.Descriptor.Group.ClusterIncarnation,
	}}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewBootstrapControlService(BootstrapControlOptions{
		Journal: journal, Receiver: bootstrapReceiveFunc(func(context.Context, rafttransport.NodeID, Descriptor) error { return nil }),
		Installer: installer, Releaser: BootstrapArtifactReleaseFunc(func(context.Context, BootstrapRequest, raftmember.RuntimeIdentity) error { return nil }), Authorize: func(peer rafttransport.PeerIdentity, got BootstrapRequest) bool {
			return peer == controller && got == request
		},
		SourceNode:   func(Descriptor) (rafttransport.NodeID, bool) { return source, true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(context.Background(), &testPeerConn{
			Conn: server, identity: controller, class: rafttransport.TrafficShardControl,
		})
	}()
	if err = WriteBootstrapRequest(client, request); err != nil {
		t.Fatal(err)
	}
	response, err := ReadBootstrapResponse(client)
	_ = client.Close()
	if serveErr := <-done; err != nil || serveErr != nil || response.Identity != identity ||
		response.Request.Operation != request.Operation || response.Request.Step != request.Step {
		t.Fatalf("response=%+v readErr=%v serveErr=%v", response, err, serveErr)
	}
}

func newTestBootstrapControl(
	t testing.TB,
	journal BootstrapJournal,
	installer BootstrapInstaller,
	receive bootstrapReceiveFunc,
) *BootstrapControlService {
	t.Helper()
	_, _, source := bootstrapControlFixture()
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewBootstrapControlService(BootstrapControlOptions{
		Journal: journal, Receiver: receive, Installer: installer,
		Releaser:     BootstrapArtifactReleaseFunc(func(context.Context, BootstrapRequest, raftmember.RuntimeIdentity) error { return nil }),
		Authorize:    func(rafttransport.PeerIdentity, BootstrapRequest) bool { return true },
		SourceNode:   func(Descriptor) (rafttransport.NodeID, bool) { return source, true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func bootstrapControlFixture() (BootstrapRequest, raftmember.RuntimeIdentity, rafttransport.NodeID) {
	payload := bytes.Repeat([]byte("bootstrap-control"), 300)
	descriptor := testDescriptor(payload)
	request := BootstrapRequest{Operation: [32]byte{1}, Step: [32]byte{2}, Descriptor: descriptor}
	identity := raftmember.RuntimeIdentity{
		Group: descriptor.Group, Distribution: "documents", Shard: "all",
		AllocationGeneration: 4, MemberID: descriptor.TargetMember,
		StoreID: descriptor.TargetStore, NodeIncarnation: descriptor.TargetIncarnation,
		RelationManifestDigest: [32]byte{7},
	}
	return request, identity, rafttransport.NodeID{3}
}
