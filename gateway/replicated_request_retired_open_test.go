package gateway

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// retiredOpenWaveLedger adds the one atomic read used by the recovery path to
// the existing lifecycle fixture. Its mutable state is changed only while the
// SessionOpen barrier owns the test, so the inherited CAS implementation still
// exercises the exact production ledger transitions.
type retiredOpenWaveLedger struct {
	*lifecycleRunnerLedger
	mu    sync.Mutex
	reads int
}

func (ledger *retiredOpenWaveLedger) ReadWaveCut(
	_ context.Context,
	_ DurableRequestLedgerHome,
	key requestledger.RequestKey,
	steps []requestledger.StepRef,
) (durableRequestWaveReadCut, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if key != ledger.head.Key {
		return durableRequestWaveReadCut{}, ErrDurableRequestConflict
	}
	ledger.reads++
	route := ledger.route
	route.Command, route.Completion = bytes.Clone(route.Command), bytes.Clone(route.Completion)
	pending := ledger.pending
	pending.Steps = append(steps[:0], pending.Steps...)
	head := ledger.head
	head.InlinePlan = bytes.Clone(head.InlinePlan)
	return durableRequestWaveReadCut{
		Head: head, Route: route, Pending: pending, Applied: ledger.head.Revision,
	}, nil
}

func (ledger *retiredOpenWaveLedger) readCount() int {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.reads
}

type retiredOpenBarrierClient struct {
	base      *routeSessionMachineClient
	seen      chan []byte
	retired   chan error
	release   chan struct{}
	seenOnce  sync.Once
	mu        sync.Mutex
	openCalls int
}

func (client *retiredOpenBarrierClient) DoReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if len(request.Command) != 0 {
		command, err := replication.OpenCommand(request.Command)
		if err != nil {
			return nil, err
		}
		if command.Kind() == replication.CommandSessionOpen {
			client.mu.Lock()
			client.openCalls++
			client.mu.Unlock()
			client.seenOnce.Do(func() {
				client.seen <- bytes.Clone(request.Command)
				<-client.release
			})
		}
	}
	response, err := client.base.DoReplicated(ctx, endpoint, request)
	if err != nil && len(request.Command) != 0 {
		command, commandErr := replication.OpenCommand(request.Command)
		if commandErr == nil && command.Kind() == replication.CommandSessionOpen &&
			errors.Is(err, replicatedstate.ErrRetryRetired) {
			client.retired <- err
			return &shardservice.ReplicatedResponse{
				Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalRetryRetired,
				HasState: true, State: client.base.state,
				RequestDigest: replicatedRequestDigest(request.Command),
				Outcome:       raftserve.Outcome{Code: raftserve.OutcomeRetryRetired},
			}, nil
		}
	}
	return response, err
}

func (client *retiredOpenBarrierClient) openCallCount() int {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.openCalls
}

// retiredOpenRunnerProposer uses the real route-session machine for Open,
// acquire, release, and cleanup. The target mutation remains the existing
// lifecycle unit fixture's deterministic proposer, so this test isolates the
// retired-Open interleaving without introducing a second data fixture.
type retiredOpenRunnerProposer struct {
	*lifecycleRunnerProposer
	gate     *nativeDurableRequestRouteGateSessions
	executor *ReplicatedExecutor
}

func (proposer *retiredOpenRunnerProposer) prepareAcquire(
	ctx context.Context, route ReplicatedRoute, wave DurableRequestWave, head requestledger.HeadRecord,
) ([]byte, requestledger.Digest, error) {
	return proposer.gate.prepareAcquire(ctx, route, wave, head)
}

func (proposer *retiredOpenRunnerProposer) prepareRelease(
	ctx context.Context, route ReplicatedRoute, wave DurableRequestWave, pin requestledger.RoutePinRecord,
) ([]byte, error) {
	return proposer.gate.prepareRelease(ctx, route, wave, pin)
}

func (proposer *retiredOpenRunnerProposer) cleanup(
	ctx context.Context, route ReplicatedRoute, wave DurableRequestWave, pin requestledger.RoutePinRecord,
) error {
	return proposer.gate.cleanup(ctx, route, wave, pin)
}

func (proposer *retiredOpenRunnerProposer) Propose(
	ctx context.Context, route ReplicatedRoute, exact []byte,
) (ReplicatedResult, error) {
	view, err := replication.OpenCommand(exact)
	if err != nil {
		return ReplicatedResult{}, err
	}
	if view.Kind() == replication.CommandRouteGate {
		return proposer.executor.Propose(ctx, route, exact)
	}
	return proposer.lifecycleRunnerProposer.Propose(ctx, route, exact)
}

func newRetiredOpenMachineExecutors(t *testing.T) (
	ReplicatedRoute, *routeSessionMachineClient, *retiredOpenBarrierClient, *ReplicatedExecutor, *ReplicatedExecutor, chan []byte, chan struct{},
) {
	t.Helper()
	route, machine, _ := newRouteSessionMachine(t)
	seen := make(chan []byte, 1)
	retired := make(chan error, 1)
	release := make(chan struct{})
	barrier := &retiredOpenBarrierClient{base: machine, seen: seen, retired: retired, release: release}
	first, err := NewReplicatedExecutor(barrier, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewReplicatedExecutor(machine, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return route, machine, barrier, first, second, seen, release
}

func installRetiredOpenAcquire(
	t *testing.T,
	ctx context.Context,
	route ReplicatedRoute,
	wave DurableRequestWave,
	initial requestledger.HeadRecord,
	ledger *retiredOpenWaveLedger,
	executor *ReplicatedExecutor,
	openCommand []byte,
	persistProof bool,
) {
	t.Helper()
	open, err := replication.OpenCommand(openCommand)
	if err != nil || open.Kind() != replication.CommandSessionOpen {
		t.Fatalf("open command: %v", err)
	}
	result, err := executor.Propose(ctx, route, openCommand)
	if err != nil {
		t.Fatalf("peer open: %v", err)
	}
	completion, err := replication.OpenCompletion(result.Completion)
	if err != nil {
		t.Fatalf("peer open completion: %v", err)
	}
	session := &NativeSession{
		executor: executor, route: route, distribution: string(route.Distribution),
		shard: string(route.Shard), tenant: bytes.Clone(wave.Tenant), clientID: open.ClientID,
		retryHome: open.RetryHome, maxCommand: requestledger.MaxRouteGatePinCommandBytes,
		proposalCapability: serviceauthz.CapabilityDataWrite, scopedCoordination: true,
		membershipStableCoordination: true,
	}
	session.finishCompletion(open, completion)
	peerWave := wave
	readRoute := route
	readRoute.membershipStable = true
	gateRead, err := executor.ReadRouteGate(ctx, readRoute, 1)
	if err != nil {
		t.Fatalf("peer route-gate read: %v", err)
	}
	peerWave.GateEpoch = gateRead.Status.Epoch
	acquire, physical, err := appendDurableRequestRouteGateCommand(
		nil, route, peerWave, initial.KeyDigest, initial.RequestDigest,
		initial.PlanRoot, initial.ContinuationDigest, initial.NextStepOrdinal,
		routegate.OperationAcquireShared, session,
	)
	if err != nil {
		t.Fatalf("peer acquire command: %v", err)
	}
	acquiring, err := requestledger.NewRoutePinAcquiring(
		initial, wave.PinID, wave.Binding, physical, acquire,
	)
	if err != nil {
		t.Fatalf("peer acquiring pin: %v", err)
	}
	if _, err := ledger.ApplyCAS(ctx, wave.Home, wave.Key, DurableRequestLifecycleCAS{
		Operation:        requestledger.OperationBeginRoutePinAcquire,
		ExpectedRevision: initial.Revision, Revision: initial.Revision + 1,
		RoutePin: acquiring,
	}); err != nil {
		t.Fatalf("persist peer acquire intent: %v", err)
	}
	result, err = executor.Propose(ctx, route, acquire)
	if err != nil {
		t.Fatalf("peer acquire: %v", err)
	}
	if !persistProof {
		return
	}
	acquired, err := requestledger.RecordVerifiedRoutePinAcquired(
		acquiring, acquiring.Revision+1, result.Completion,
	)
	if err != nil {
		t.Fatalf("peer acquired pin: %v", err)
	}
	acquiredHead, err := requestledger.AdvanceHeadRoutePin(
		ledger.head, ledger.route, acquired, ledger.head.Revision+1,
	)
	if err != nil {
		t.Fatalf("peer acquired head: %v", err)
	}
	pending, err := requestledger.NewPendingWaveWithRoutePin(
		acquiredHead, wave.Build, acquiredHead.Revision+1, acquired,
		[]requestledger.StepRef{wave.Step},
	)
	if err != nil {
		t.Fatalf("peer pending: %v", err)
	}
	if _, err := ledger.ApplyCAS(ctx, wave.Home, wave.Key, DurableRequestLifecycleCAS{
		Operation:        requestledger.OperationRecordRoutePinAcquiredPutPending,
		ExpectedRevision: ledger.head.Revision, Revision: pending.Revision,
		RoutePin: acquired, Pending: pending,
	}); err != nil {
		t.Fatalf("persist peer acquired proof: %v", err)
	}
}

func TestDurableRequestLifecycleRunnerRecoversRetiredSessionOpenWithOneCut(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		persistProof bool
	}{
		{name: "acquiring_intent", persistProof: false},
		{name: "acquired_pending", persistProof: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			route, machine, barrier, executor, peer, seen, release := newRetiredOpenMachineExecutors(t)
			wave, initial, _ := lifecycleRunnerFixture(t)
			wave.LogicalTarget = DurableRequestLogicalTarget{
				Distribution: route.Distribution, Shard: route.Shard, Group: route.Group,
				RangeIdentity: route.RangeIdentity, LineageDigest: route.LineageDigest,
				ForwardingRuleDigest:   route.ForwardingRuleDigest,
				SchemaGeneration:       route.Command.SchemaGeneration,
				RelationManifestDigest: route.Command.RelationManifestDigest,
				MutationDigest:         wave.LogicalTarget.MutationDigest, BucketBits: wave.LogicalTarget.BucketBits,
				IntentScopes: wave.LogicalTarget.IntentScopes, Batches: wave.LogicalTarget.Batches,
			}
			wave.ExecutionPinRoute = route
			ledger := &retiredOpenWaveLedger{lifecycleRunnerLedger: &lifecycleRunnerLedger{
				head: initial, events: new(lifecycleRunnerEvents),
			}}
			resolver := &lifecycleRunnerResolver{route: route, events: ledger.events}
			gate := &nativeDurableRequestRouteGateSessions{executor: executor}
			proposer := &retiredOpenRunnerProposer{
				lifecycleRunnerProposer: &lifecycleRunnerProposer{
					t: t, events: ledger.events, faultKind: -1,
					attempts: make(map[replication.CommandKind][][]byte),
				},
				gate: gate, executor: executor,
			}
			runner, err := newDurableRequestLifecycleRunner(ledger, resolver, proposer)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ctx, err = serviceauthz.WithAuthority(ctx, serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
			if err != nil {
				t.Fatal(err)
			}
			resultCh := make(chan error, 1)
			finished := false
			releaseBarrier := sync.Once{}
			closeBarrier := func() { releaseBarrier.Do(func() { close(release) }) }
			defer func() {
				if !finished {
					closeBarrier()
					cancel()
					select {
					case <-resultCh:
					case <-time.After(5 * time.Second):
						t.Log("runner did not settle after test failure")
					}
				}
			}()
			go func() {
				_, runErr := runner.RunWave(ctx, wave)
				resultCh <- runErr
			}()
			var openCommand []byte
			select {
			case openCommand = <-seen:
			case <-time.After(5 * time.Second):
				t.Fatal("runner did not reach SessionOpen barrier")
			}
			installRetiredOpenAcquire(t, ctx, route, wave, initial, ledger, peer, openCommand, testCase.persistProof)
			if _, err := machine.machine.LookupCompletion(openCommand); !errors.Is(err, replicatedstate.ErrRetryRetired) {
				t.Fatalf("peer did not retire Open before recovery: %v", err)
			}
			closeBarrier()
			select {
			case err := <-barrier.retired:
				if !errors.Is(err, replicatedstate.ErrRetryRetired) {
					t.Fatalf("real retired Open refusal=%v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("runner did not submit the retired Open")
			}
			var runErr error
			select {
			case runErr = <-resultCh:
			case <-time.After(5 * time.Second):
				t.Fatal("runner did not settle after retired Open recovery")
			}
			finished = true
			if runErr != nil {
				t.Fatalf("runner recovery: %v", runErr)
			}
			if got := ledger.readCount(); got != 2 {
				t.Fatalf("wave-cut reads=%d want exactly initial+one recovery read", got)
			}
			if ledger.route.Phase != requestledger.RoutePinReleased || ledger.pending.Revision != 0 ||
				ledger.head.OutstandingRoutePinDigest != (requestledger.Digest{}) {
				t.Fatalf("final lifecycle state head=%+v route=%+v pending=%+v", ledger.head, ledger.route, ledger.pending)
			}
			if got := len(proposer.attempts[replication.CommandSessionOpen]); got != 0 {
				t.Fatalf("runner proposed SessionOpen after retirement: %d", got)
			}
			if got := barrier.openCallCount(); got != 1 {
				t.Fatalf("SessionOpen calls through runner=%d want one", got)
			}
			if got := len(proposer.attempts[replication.CommandMutationBatch]); got != 1 {
				t.Fatalf("target proposals=%d want one", got)
			}
		})
	}
}
