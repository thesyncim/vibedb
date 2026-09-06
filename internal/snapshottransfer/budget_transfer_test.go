package snapshottransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func transferBudgetConfig(maxActive int, rate, burst uint64) migrationbudget.Config {
	limit := migrationbudget.RateLimit{BytesPerSecond: rate, BurstBytes: burst}
	return migrationbudget.Config{MaxActive: maxActive, CPU: limit, DiskRead: limit,
		DiskWrite: limit, NetworkSend: limit, NetworkReceive: limit}
}

func TestServiceAndReceiverUseOnePhysicalNodeBudget(t *testing.T) {
	payload := bytesForTransferBudget(MinChunkBytes)
	d := testDescriptor(payload)
	source := openTestRepository(t, t.TempDir()+"/source")
	appendAll(t, source, d, payload, 0)
	registry, sourceIdentity, targetIdentity := testRegistry(t)
	budget, err := migrationbudget.New(transferBudgetConfig(1, 1<<30, MinChunkBytes))
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		Repository: source, Registry: registry, Budget: budget,
		Authorize:      func(got Descriptor) bool { return got == d },
		ReadDeadline:   func() time.Time { return time.Now().Add(5 * time.Second) },
		WriteDeadline:  func() time.Time { return time.Now().Add(5 * time.Second) },
		MaxConnections: 2, MaxChunkBytes: MinChunkBytes, MaxInflightBytes: 2 * MinChunkBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := makeTransferRequest(d, 0)
	firstClient, firstServer := net.Pipe()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- service.Serve(context.Background(), &testPeerConn{
			Conn: firstServer, identity: targetIdentity, class: rafttransport.TrafficSnapshot,
		})
	}()
	if err := writeFull(firstClient, request[:]); err != nil {
		t.Fatal(err)
	}
	var response [responseBytes]byte
	if _, err := io.ReadFull(firstClient, response[:]); err != nil {
		t.Fatal(err)
	}

	secondClient, secondServer := net.Pipe()
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- service.Serve(context.Background(), &testPeerConn{
			Conn: secondServer, identity: targetIdentity, class: rafttransport.TrafficSnapshot,
		})
	}()
	if err := writeFull(secondClient, request[:]); err != nil {
		t.Fatal(err)
	}
	waitForTransferBudget(t, func() bool { return budget.Metrics().Acquires >= 2 })
	if got := budget.Metrics(); got.ActiveCapacity != 1 || got.Waiting != 0 {
		t.Fatalf("active permit held during peer write: %+v", got)
	}

	firstBody := make([]byte, binaryResponseLength(response))
	if _, err := io.ReadFull(firstClient, firstBody); err != nil {
		t.Fatal(err)
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(secondClient, response[:]); err != nil {
		t.Fatal(err)
	}
	secondBody := make([]byte, binaryResponseLength(response))
	if _, err := io.ReadFull(secondClient, secondBody); err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	_ = firstClient.Close()
	_ = secondClient.Close()
	if got := budget.Metrics(); got.Acquires != 2 || got.Releases != 2 || got.Active != 0 {
		t.Fatalf("budget lifecycle=%+v", got)
	}

	// A receiver on the same node receives through the same object, rather
	// than silently creating another active-capacity pool.
	target := openTestRepository(t, t.TempDir()+"/target")
	opener := &sourceProviderTestOpener{service: service, source: rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: sourceIdentity.Node}, target: targetIdentity}
	receiver := Receiver{Repository: target, Opener: opener, Budget: budget,
		ReadDeadline:  func() time.Time { return time.Now().Add(5 * time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(5 * time.Second) }, Workspace: make([]byte, MinChunkBytes)}
	if err := receiver.Receive(context.Background(), sourceIdentity.Node, d); err != nil {
		t.Fatal(err)
	}
	if got := receiver.Metrics().Budget; got.ActiveCapacity != 1 || got.Active != 0 {
		t.Fatalf("receiver budget=%+v", got)
	}
}

func TestReceiverNetworkBudgetPacesActualBodyAndReportsThrottle(t *testing.T) {
	payload := bytesForTransferBudget(MinChunkBytes * 2)
	d := testDescriptor(payload)
	source := openTestRepository(t, t.TempDir()+"/source")
	appendAll(t, source, d, payload, 0)
	target := openTestRepository(t, t.TempDir()+"/target")
	registry, sourceIdentity, targetIdentity := testRegistry(t)
	const rate = 128 << 10
	const burst = 512
	config := transferBudgetConfig(2, 1<<30, MinChunkBytes)
	config.NetworkReceive = migrationbudget.RateLimit{BytesPerSecond: rate / 8, BurstBytes: burst}
	budget, err := migrationbudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		Repository: source, Registry: registry,
		Authorize:      func(got Descriptor) bool { return got == d },
		ReadDeadline:   func() time.Time { return time.Now().Add(10 * time.Second) },
		WriteDeadline:  func() time.Time { return time.Now().Add(10 * time.Second) },
		MaxConnections: 2, MaxChunkBytes: MinChunkBytes, MaxInflightBytes: 2 * MinChunkBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	receiver := Receiver{Repository: target, Budget: budget,
		Opener:        &sourceProviderTestOpener{service: service, source: sourceIdentity, target: targetIdentity},
		ReadDeadline:  func() time.Time { return time.Now().Add(10 * time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(10 * time.Second) }, Workspace: make([]byte, MinChunkBytes)}
	started := time.Now()
	if err := receiver.Receive(context.Background(), sourceIdentity.Node, d); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond {
		t.Fatalf("body was not paced: elapsed=%s", elapsed)
	}
	metrics := budget.Metrics()
	if metrics.NetworkReceive.ThrottleEvents == 0 {
		t.Fatalf("expected receiver throttling: %+v", metrics)
	}
	if receiver.Metrics().Bytes != uint64(len(payload)) {
		t.Fatalf("receiver bytes=%d want=%d", receiver.Metrics().Bytes, len(payload))
	}
}

func TestServiceNetworkBudgetPacesActualBody(t *testing.T) {
	payload := bytesForTransferBudget(MinChunkBytes * 2)
	d := testDescriptor(payload)
	source := openTestRepository(t, t.TempDir()+"/source")
	appendAll(t, source, d, payload, 0)
	target := openTestRepository(t, t.TempDir()+"/target")
	registry, sourceIdentity, targetIdentity := testRegistry(t)
	config := transferBudgetConfig(2, 1<<30, MinChunkBytes)
	config.NetworkSend = migrationbudget.RateLimit{BytesPerSecond: 8 << 10, BurstBytes: 512}
	budget, err := migrationbudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		Repository: source, Registry: registry, Budget: budget,
		Authorize:      func(got Descriptor) bool { return got == d },
		ReadDeadline:   func() time.Time { return time.Now().Add(10 * time.Second) },
		WriteDeadline:  func() time.Time { return time.Now().Add(10 * time.Second) },
		MaxConnections: 2, MaxChunkBytes: MinChunkBytes, MaxInflightBytes: 2 * MinChunkBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	receiver := Receiver{Repository: target, Opener: &sourceProviderTestOpener{
		service: service, source: sourceIdentity, target: targetIdentity,
	}, Budget: budget, ReadDeadline: func() time.Time { return time.Now().Add(10 * time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(10 * time.Second) }, Workspace: make([]byte, MinChunkBytes)}
	started := time.Now()
	if err := receiver.Receive(context.Background(), sourceIdentity.Node, d); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) < 20*time.Millisecond || service.Stats().Budget.NetworkSend.ThrottleEvents == 0 {
		t.Fatalf("sender was not paced: elapsed=%s metrics=%+v", time.Since(started), service.Stats().Budget)
	}
}

func TestReceiverCancellationDuringBudgetWaitReleasesCapacity(t *testing.T) {
	payload := bytesForTransferBudget(MinChunkBytes)
	d := testDescriptor(payload)
	source := openTestRepository(t, t.TempDir()+"/source")
	appendAll(t, source, d, payload, 0)
	target := openTestRepository(t, t.TempDir()+"/target")
	registry, sourceIdentity, targetIdentity := testRegistry(t)
	config := transferBudgetConfig(1, 1, 1)
	// Keep local work fast. Only the target wire receive is intentionally slow;
	// cancellation must interrupt its token wait before an active append lease.
	fast := migrationbudget.RateLimit{BytesPerSecond: 1 << 30, BurstBytes: MinChunkBytes}
	config.CPU, config.DiskRead, config.DiskWrite, config.NetworkSend = fast, fast, fast, fast
	budget, err := migrationbudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		Repository: source, Registry: registry,
		Authorize:      func(got Descriptor) bool { return got == d },
		ReadDeadline:   func() time.Time { return time.Now().Add(5 * time.Second) },
		WriteDeadline:  func() time.Time { return time.Now().Add(5 * time.Second) },
		MaxConnections: 1, MaxChunkBytes: MinChunkBytes, MaxInflightBytes: MinChunkBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	receiver := Receiver{Repository: target, Budget: budget,
		Opener:        &sourceProviderTestOpener{service: service, source: sourceIdentity, target: targetIdentity},
		ReadDeadline:  func() time.Time { return time.Now().Add(5 * time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(5 * time.Second) }, Workspace: make([]byte, MinChunkBytes)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- receiver.Receive(ctx, sourceIdentity.Node, d) }()
	time.Sleep(25 * time.Millisecond)
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation err=%v", err)
	}
	metrics := budget.Metrics()
	if metrics.Active != 0 || metrics.ConsumeErrors == 0 || metrics.NetworkReceive.ThrottleEvents == 0 {
		t.Fatalf("cancellation leaked budget=%+v", metrics)
	}
}

func TestReceiverCancellationAtFullOffsetHonorsContext(t *testing.T) {
	payload := bytesForTransferBudget(MinChunkBytes)
	d := testDescriptor(payload)
	repository := openTestRepository(t, t.TempDir()+"/target")
	repository.fault = func(point repositoryFault) error {
		if point == faultAfterPublishRename {
			return errors.New("publish interrupted")
		}
		return nil
	}
	if _, _, err := repository.Append(d, 0, payload, sha256.Sum256(payload)); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("interrupted final append = %v", err)
	}
	repository.fault = nil
	config := transferBudgetConfig(1, 1<<30, 1)
	budget, err := migrationbudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.AttachBudget(budget); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	receiver := Receiver{Repository: repository, Budget: budget,
		Opener:        &sourceProviderTestOpener{},
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) }}
	if err := receiver.Receive(ctx, rafttransport.NodeID{99}, d); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled full-offset resume = %v", err)
	}
}

func TestRepositoryFinishRejectsRetiredReplacementRecord(t *testing.T) {
	request, descriptor := sourceControlFixture()
	payload := bytes.Repeat([]byte("replacement"), 600)
	descriptor = testDescriptor(payload)
	request.Group = descriptor.Group
	request.SourceMember = descriptor.SourceMember
	request.TargetMember = descriptor.TargetMember
	request.TargetStore = descriptor.TargetStore
	request.TargetIncarnation = descriptor.TargetIncarnation
	request.ReplicaSetVersion = descriptor.ReplicaSetVersion
	repository := openTestRepository(t, t.TempDir()+"/source")
	first := payload[:descriptor.ChunkBytes]
	if _, _, err := repository.Append(descriptor, 0, first, sha256.Sum256(first)); err != nil {
		t.Fatal(err)
	}
	retired := repository.records[descriptor.ArtifactHash]
	if _, err := repository.AbandonArtifact(testAbandonmentWitness(request, descriptor)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.Append(descriptor, 0, first, sha256.Sum256(first)); err != nil {
		t.Fatal(err)
	}
	if repository.records[descriptor.ArtifactHash] == retired {
		t.Fatal("replacement append reused retired record")
	}
	if err := repository.finish(context.Background(), nil, retired); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("retired finish = %v", err)
	}
}

func TestRepositoryBudgetAttachAndManifestAreConcurrentSafe(t *testing.T) {
	cut, plan := sourceExportFixture(t, sourceExportLimits())
	defer cut.Close()
	descriptor, _, err := ExportPinnedSnapshot(plan)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := migrationbudget.New(transferBudgetConfig(2, 1<<30, MinChunkBytes))
	if err != nil {
		t.Fatal(err)
	}
	const workers = 8
	const rounds = 8
	errs := make(chan error, workers*rounds)
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for round := 0; round < rounds; round++ {
				if err := plan.Repository.AttachBudget(budget); err != nil {
					errs <- err
					continue
				}
				if worker%2 == 0 {
					if _, err := plan.Repository.ManifestContext(context.Background(), descriptor); err != nil {
						errs <- err
					}
				}
			}
		}(worker)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestConsumeBudgetBytesCompletesCostAboveOneTokenBurst(t *testing.T) {
	config := transferBudgetConfig(1, 1<<30, 1)
	budget, err := migrationbudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	count, err := consumeBudgetBytes(context.Background(), budget, nil, 1,
		func(bytes uint64) migrationbudget.Cost { return migrationbudget.Cost{CPU: bytes * 2} })
	if err != nil || count != 1 {
		t.Fatalf("one-byte cost=%d err=%v", count, err)
	}
	if got := budget.Metrics().CPU.ConsumedBytes; got < 2 {
		t.Fatalf("CPU reservation=%d, want at least two tokens", got)
	}
}

func TestConsumeBudgetBytesCompletesCostAfterPressureBurstDownshift(t *testing.T) {
	config := transferBudgetConfig(1, 1<<30, 4)
	budget, err := migrationbudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Consume(context.Background(), migrationbudget.Cost{CPU: 4}); err != nil {
		t.Fatal(err)
	}
	lease, err := budget.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	budget.ApplyPressure(migrationbudget.PressureSample{Sequence: 1, Initial: true, QueueCapacity: 4})
	for sequence := uint64(2); sequence <= 4; sequence++ {
		budget.ApplyPressure(migrationbudget.PressureSample{Sequence: sequence, QueueDepth: 4, QueueCapacity: 4})
	}
	if got := budget.Pressure(); got.ScalePPM != 125_000 {
		t.Fatalf("pressure scale=%d, want 125000", got.ScalePPM)
	}
	count, err := consumeBudgetBytes(context.Background(), budget, lease, 1,
		func(bytes uint64) migrationbudget.Cost { return migrationbudget.Cost{CPU: bytes * 2} })
	if err != nil || count != 1 {
		t.Fatalf("downshifted one-byte cost=%d err=%v", count, err)
	}
	if got := budget.Metrics().CPU.ConsumedBytes; got < 6 {
		t.Fatalf("CPU reservation=%d, want at least six tokens", got)
	}
}

func bytesForTransferBudget(size int) []byte {
	result := make([]byte, size)
	for index := range result {
		result[index] = byte(index*31 + 7)
	}
	return result
}

func makeTransferRequest(d Descriptor, offset uint64) (request [requestBytes]byte) {
	copy(request[:8], requestMagic[:])
	encoded, _ := AppendDescriptor(request[8:8], d)
	copy(request[8:8+DescriptorBytes], encoded)
	binary.BigEndian.PutUint64(request[8+DescriptorBytes:], offset)
	return request
}

func binaryResponseLength(response [responseBytes]byte) int {
	return int(binary.BigEndian.Uint32(response[32:36]))
}

func waitForTransferBudget(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for migration budget state")
		}
		time.Sleep(time.Millisecond)
	}
}
