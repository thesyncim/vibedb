package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
)

var errLifecycleRunnerFault = errors.New("lifecycle runner fault")

type lifecycleRunnerEvents struct {
	mu     sync.Mutex
	values []string
}

func (events *lifecycleRunnerEvents) add(value string) {
	events.mu.Lock()
	events.values = append(events.values, value)
	events.mu.Unlock()
}

type lifecycleRunnerLedger struct {
	head         requestledger.HeadRecord
	route        requestledger.RoutePinRecord
	pending      requestledger.PendingWaveRecord
	continuation requestledger.ContinuationRecord
	events       *lifecycleRunnerEvents
	fault        requestledger.Operation
}

func (ledger *lifecycleRunnerLedger) ApplyCAS(
	_ context.Context,
	_ DurableRequestLedgerHome,
	_ requestledger.RequestKey,
	cas DurableRequestLifecycleCAS,
) (DurableRequestLifecycleCASResult, error) {
	ledger.events.add(fmt.Sprintf("cas:%d", cas.Operation))
	if cas.ExpectedRevision != ledger.head.Revision || cas.Revision != ledger.head.Revision+1 {
		return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
	}
	var err error
	switch cas.Operation {
	case requestledger.OperationBeginRoutePinAcquire,
		requestledger.OperationRecordRoutePinAcquired,
		requestledger.OperationBeginRoutePinRelease:
		ledger.head, err = requestledger.AdvanceHeadRoutePin(
			ledger.head, ledger.route, cas.RoutePin, cas.Revision,
		)
		ledger.route = cas.RoutePin
	case requestledger.OperationRecordRoutePinReleased:
		ledger.head, err = requestledger.MarkRoutePinReleased(
			ledger.head, cas.RoutePin, cas.Revision,
		)
		ledger.route = cas.RoutePin
	case requestledger.OperationPutPending:
		ledger.head, err = requestledger.InstallPendingWave(
			ledger.head, cas.Pending, requestledger.PayloadBuildRecord{}, ledger.route,
		)
		ledger.pending = cas.Pending
	case requestledger.OperationAdvance:
		ledger.continuation = cas.Continuation
		ledger.head, err = requestledger.AdvancePending(
			ledger.head, ledger.pending, cas.Continuation,
		)
		ledger.pending = requestledger.PendingWaveRecord{}
	default:
		err = ErrDurableRequestConflict
	}
	if err != nil {
		return DurableRequestLifecycleCASResult{}, err
	}
	result := DurableRequestLifecycleCASResult{
		Ledger: replicatedstate.RequestLedgerCompletionResult{
			ResultCode: replicatedstate.ResultApplied,
		},
		Applied: ledger.head.Revision,
	}
	if ledger.fault == cas.Operation {
		ledger.fault = requestledger.OperationInvalid
		return DurableRequestLifecycleCASResult{}, errLifecycleRunnerFault
	}
	return result, nil
}

func (ledger *lifecycleRunnerLedger) ReadRow(
	_ context.Context,
	_ DurableRequestLedgerHome,
	read DurableRequestLifecycleRead,
) (DurableRequestLifecycleRow, error) {
	switch read.Kind {
	case replicatedstate.RequestLedgerReadHead:
		return DurableRequestLifecycleRow{
			Applied: ledger.head.Revision, Found: true, Kind: read.Kind, Head: ledger.head,
		}, nil
	case replicatedstate.RequestLedgerReadRoutePin:
		return DurableRequestLifecycleRow{
			Applied: ledger.head.Revision,
			Found:   ledger.route.Phase != requestledger.RoutePinInvalid,
			Kind:    read.Kind, RoutePin: ledger.route,
		}, nil
	case replicatedstate.RequestLedgerReadPending:
		row := DurableRequestLifecycleRow{
			Applied: ledger.head.Revision,
			Found:   ledger.pending.Revision != 0, Kind: read.Kind,
		}
		if row.Found {
			row.Pending = ledger.pending
			row.Pending.Steps = append(read.PendingSteps[:0], ledger.pending.Steps...)
		}
		return row, nil
	case replicatedstate.RequestLedgerReadContinuation:
		return DurableRequestLifecycleRow{
			Applied: ledger.head.Revision, Found: ledger.continuation.Revision != 0,
			Kind: read.Kind, Continuation: ledger.continuation,
		}, nil
	default:
		return DurableRequestLifecycleRow{}, ErrDurableRequestConflict
	}
}

type lifecycleRunnerResolver struct {
	route  ReplicatedRoute
	calls  int
	events *lifecycleRunnerEvents
}

func (resolver *lifecycleRunnerResolver) ResolveDurableRequestParticipant(
	_ context.Context,
	_ DurableRequestLogicalParticipant,
) (ReplicatedRoute, error) {
	resolver.calls++
	resolver.events.add("resolve")
	return cloneDurableRequestRoute(resolver.route), nil
}

type lifecycleRunnerProposer struct {
	t            testing.TB
	events       *lifecycleRunnerEvents
	faultKind    int
	faultGate    routegate.Operation
	attempts     map[replication.CommandKind][][]byte
	fenceCalls   int
	fenceFaultAt int
}

func (proposer *lifecycleRunnerProposer) ValidateExecutionPinFence(
	_ context.Context,
	_ ReplicatedRoute,
	_ executionpin.LeaseCertificate,
	_ uint64,
) (ReplicatedExecutionPinReadResult, error) {
	proposer.fenceCalls++
	proposer.events.add("fence")
	if proposer.fenceFaultAt == proposer.fenceCalls {
		return ReplicatedExecutionPinReadResult{}, ErrDurableRequestConflict
	}
	return ReplicatedExecutionPinReadResult{Applied: 11, Found: true}, nil
}

func (proposer *lifecycleRunnerProposer) Propose(
	_ context.Context,
	_ ReplicatedRoute,
	exact []byte,
) (ReplicatedResult, error) {
	view, err := replication.OpenCommand(exact)
	if err != nil {
		return ReplicatedResult{}, err
	}
	proposer.events.add(fmt.Sprintf("propose:%d", view.Kind()))
	proposer.attempts[view.Kind()] = append(
		proposer.attempts[view.Kind()], bytes.Clone(exact),
	)
	fault := proposer.faultKind == int(view.Kind())
	if fault && proposer.faultGate != routegate.OperationInvalid {
		gate, gateErr := view.OpenRouteGate()
		fault = gateErr == nil && gate.Operation == proposer.faultGate
	}
	if fault {
		proposer.faultKind = -1
		return ReplicatedResult{}, errLifecycleRunnerFault
	}
	completion := lifecycleRunnerCompletion(proposer.t, view)
	result := ReplicatedResult{
		Outcome:    raftserve.Outcome{Code: raftserve.OutcomeCompletion, AppliedIndex: 11},
		Completion: completion,
	}
	if !validDurableRequestSettlement(exact, result) {
		proposer.t.Fatal("fixture produced invalid settlement")
	}
	return result, nil
}

func lifecycleRunnerCompletion(t testing.TB, command replication.CommandView) []byte {
	t.Helper()
	var code uint32
	var format uint16
	var result []byte
	var err error
	switch command.Kind() {
	case replication.CommandRouteGate:
		gate, err := command.OpenRouteGate()
		if err != nil {
			t.Fatal(err)
		}
		status := routegate.Status{Revision: 1, Epoch: gate.Epoch, RetainedRecords: 1}
		outcome := routegate.Outcome{Mutated: true, Status: status}
		if gate.Operation == routegate.OperationAcquireShared {
			outcome.Reason, outcome.Status.ActivePins = routegate.ReasonAcquired, 1
		} else {
			outcome.Reason, outcome.Status.ReleasedPins = routegate.ReasonReleased, 1
		}
		result, err = routegate.AppendOutcome(nil, outcome)
		if err != nil {
			t.Fatal(err)
		}
		code, format = replicatedstate.ResultRouteGate, replicatedstate.ResultFormatRouteGate
	case replication.CommandMutationBatch:
		result, err = replicatedstate.AppendMutationCompletionResult(
			nil, replicatedstate.ResultApplied, 1,
		)
		if err != nil {
			t.Fatal(err)
		}
		code, format = replicatedstate.ResultApplied, replicatedstate.ResultFormatMutation
	default:
		t.Fatalf("unexpected command kind %d", command.Kind())
	}
	digest := replication.CompletionResultDigest(code, format, result)
	encoded, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID: command.ClusterID, ClusterIncarnation: command.ClusterIncarnation,
		TopologyRecoveryEpoch: command.TopologyRecoveryEpoch,
		Distribution:          command.Distribution, Shard: command.Shard,
		AllocationGeneration: command.AllocationGeneration,
		ShardIncarnation:     command.ShardIncarnation, GroupID: command.GroupID,
		ReplicaSetVersion:      command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch,
		RoutingVersion:         command.RoutingVersion, RouteGeneration: command.RouteGeneration,
		Tenant: command.Tenant, ClientID: command.ClientID, ClientEpoch: command.ClientEpoch,
		ClientSequence: command.ClientSequence, Fingerprint: command.Fingerprint,
		RetryHome: command.RetryHome, AppliedSequence: 11,
		ResultCode: code, ResultFormat: format, Storage: replication.CompletionInline,
		ResultLength: uint64(len(result)), ResultDigest: digest, InlineResult: result,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func lifecycleRunnerFixture(t testing.TB) (
	DurableRequestWave,
	requestledger.HeadRecord,
	ReplicatedRoute,
) {
	t.Helper()
	participants, _ := transactionOrchestratorRoutes(t, 1)
	physical := participants[0]
	physical.Route.RangeIdentity = replication.Digest(lifecycleDigest("range"))
	physical.Route.LineageDigest = replication.Digest(lifecycleDigest("lineage"))
	physical.Route.ForwardingRuleDigest = replication.Digest(lifecycleDigest("forwarding"))
	tenant := []byte("tenant")
	retry := replication.RetryHome{9}
	outer := replicatedTransactionCommandHeader(
		physical.Route, tenant, retry, replication.ID128{7}, 1, 1,
	)
	outer.Kind = replication.CommandMutationBatch
	outer.Batches = physical.Batches
	outer.Fingerprint = nativeCommandFingerprint(outer)
	command, err := replication.AppendCommand(nil, outer)
	if err != nil {
		t.Fatal(err)
	}
	target := bytes.Clone(physical.Route.Group.GroupID[:])
	key := lifecycleKey()
	plan, err := requestledger.AppendPlan(nil, bytes.Repeat([]byte{0x5a}, 4096))
	if err != nil {
		t.Fatal(err)
	}
	cursor := []byte("next")
	contract := requestledger.ExecutionContract{
		CatalogGeneration: 7, PinID: requestledger.PinID{1},
		PinDigest:                    lifecycleDigest("pin"),
		RouteSchemaCertificateDigest: lifecycleDigest("route-cert"),
		MaxPendingWaveBytes:          requestledger.MaxPendingWaveRecordBytes,
		MaxContinuationBytes:         requestledger.MaxContinuationRecordBytes,
		MaxTerminalBytes:             requestledger.MaxLifecyclePayloadBytes,
		MaxActivePayloadBytes:        2 * requestledger.MaxPlanPageBytes,
		MaxActivePayloadChunks:       2,
		PlanBuildID:                  lifecycleDigest("plan-build"), PlanBuildGeneration: 1,
		PlanningLeaseExpiryIndex: math.MaxUint64, PlanningLeaseGeneration: 1,
		TerminalTransitionTag: 9, FinalWaveCount: 1,
		TerminalStateDigest:        requestledger.NextStateDigest(9, cursor),
		TerminalSummaryDigest:      lifecycleDigest("retirement"),
		AbortTerminalTransitionTag: 10, AbortFinalWaveCount: 1,
		AbortTerminalStateDigest: requestledger.NextStateDigest(10, cursor),
	}
	head, err := requestledger.NewHeadWithExecutionContract(
		key, lifecycleDigest("request"), lifecycleDigest("terminal"), contract, plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	homePoint, err := requestledger.Home(key)
	if err != nil {
		t.Fatal(err)
	}
	step := requestledger.StepRef{
		TargetSource:  requestledger.PayloadSourcePlan,
		CommandSource: requestledger.PayloadSourcePlan,
		TargetOffset:  0, TargetLength: uint64(len(target)),
		CommandOffset: uint64(len(target)), CommandLength: uint64(len(command)),
		TargetDigest:  requestledger.Digest(sha256.Sum256(target)),
		CommandDigest: requestledger.Digest(sha256.Sum256(command)),
	}
	var mutationDigester replication.TransactionMutationDigester
	mutationDigest, err := mutationDigester.Digest(physical.Batches)
	if err != nil {
		t.Fatal(err)
	}
	logical := DurableRequestLogicalParticipant{
		Distribution: physical.Route.Distribution, Shard: physical.Route.Shard,
		RangeIdentity: replication.Digest(lifecycleDigest("range")), Group: physical.Route.Group,
		SchemaGeneration:       physical.Route.Command.SchemaGeneration,
		RelationManifestDigest: physical.Route.Command.RelationManifestDigest,
		LineageDigest:          replication.Digest(lifecycleDigest("lineage")),
		ForwardingRuleDigest:   replication.Digest(lifecycleDigest("forwarding")),
		MutationDigest:         mutationDigest, BucketBits: physical.BucketBits,
		IntentScopes: physical.IntentScopes, Batches: physical.Batches,
	}
	lease := executionpin.LeaseCertificate{
		PinID: executionpin.PinID{1}, AcquireCertificateDigest: executionpin.Digest{2},
		AuthorityDigest: executionpin.Digest{3}, Controller: executionpin.ID{4},
		ControllerEpoch: 1, LeaseAppliedThrough: 100, Revision: 1, Applied: 10,
	}
	return DurableRequestWave{
		Home: DurableRequestLedgerHome{
			Identity: replication.Digest(lifecycleDigest("home")), Point: homePoint, route: physical.Route,
		},
		Key: key, Participant: logical,
		Identity: ReplicatedTransactionIdentity{
			ID: [16]byte{7}, RetryHome: retry, CatalogGeneration: 7,
			RecoveryDeadline: 1, CoordinatorOrdinal: 0,
		},
		Tenant: tenant, PinID: head.PinID, GateEpoch: 1,
		Binding: lifecycleDigest("logical-binding"), Step: step,
		ExecutionPinRoute: physical.Route, ExecutionPinLease: lease,
		Target: target, Command: command, Transition: 9, Cursor: cursor,
	}, head, physical.Route
}

func TestDurableRequestLifecycleRunnerResumesEveryDurableBoundary(t *testing.T) {
	wave, initial, route := lifecycleRunnerFixture(t)
	faults := []struct {
		name      string
		operation requestledger.Operation
		kind      int
		gate      routegate.Operation
	}{
		{"acquire_intent", requestledger.OperationBeginRoutePinAcquire, -1, 0},
		{"acquire_proposal", requestledger.OperationInvalid, int(replication.CommandRouteGate), routegate.OperationAcquireShared},
		{"acquire_proof", requestledger.OperationRecordRoutePinAcquired, -1, 0},
		{"put_pending", requestledger.OperationPutPending, -1, 0},
		{"work_proposal", requestledger.OperationInvalid, int(replication.CommandMutationBatch), 0},
		{"advance", requestledger.OperationAdvance, -1, 0},
		{"release_intent", requestledger.OperationBeginRoutePinRelease, -1, 0},
		{"release_proposal", requestledger.OperationInvalid, int(replication.CommandRouteGate), routegate.OperationReleaseShared},
		{"release_proof", requestledger.OperationRecordRoutePinReleased, -1, 0},
	}
	for _, testCase := range faults {
		t.Run(testCase.name, func(t *testing.T) {
			events := new(lifecycleRunnerEvents)
			ledger := &lifecycleRunnerLedger{
				head: initial, events: events, fault: testCase.operation,
			}
			resolver := &lifecycleRunnerResolver{route: route, events: events}
			proposer := &lifecycleRunnerProposer{
				t: t, events: events, faultKind: testCase.kind, faultGate: testCase.gate,
				attempts: make(map[replication.CommandKind][][]byte),
			}
			runner, err := newDurableRequestLifecycleRunner(ledger, resolver, proposer)
			if err != nil {
				t.Fatal(err)
			}
			var result DurableRequestWaveResult
			for attempt := 0; attempt < 3; attempt++ {
				result, err = runner.RunWave(t.Context(), wave)
				if err == nil {
					break
				}
				if !errors.Is(err, errLifecycleRunnerFault) {
					t.Fatalf("attempt %d: %v events=%v", attempt, err, events.values)
				}
			}
			if err != nil || result.Revision != initial.Revision+6 ||
				ledger.route.Phase != requestledger.RoutePinReleased ||
				ledger.head.OutstandingRoutePinDigest != (requestledger.Digest{}) ||
				ledger.pending.Revision != 0 {
				t.Fatalf("result=%+v head=%+v route=%+v err=%v", result, ledger.head, ledger.route, err)
			}
			for kind, attempts := range proposer.attempts {
				byCommand := make(map[[sha256.Size]byte][]byte)
				for _, exact := range attempts {
					digest := sha256.Sum256(exact)
					if prior := byCommand[digest]; prior != nil && !bytes.Equal(prior, exact) {
						t.Fatalf("kind %d digest collision changed retry bytes", kind)
					}
					byCommand[digest] = exact
				}
				if kind == replication.CommandRouteGate && len(byCommand) != 2 {
					t.Fatalf("route-gate commands=%d, want acquire and release", len(byCommand))
				}
			}
			if resolver.calls < 3 {
				t.Fatalf("route resolutions=%d, want at least acquire/work/release", resolver.calls)
			}
		})
	}
}

func TestDurableRequestLifecycleRunnerFencesEveryProposal(t *testing.T) {
	wave, initial, route := lifecycleRunnerFixture(t)
	events := new(lifecycleRunnerEvents)
	ledger := &lifecycleRunnerLedger{head: initial, events: events}
	resolver := &lifecycleRunnerResolver{route: route, events: events}
	proposer := &lifecycleRunnerProposer{
		t: t, events: events, faultKind: -1, fenceFaultAt: 1,
		attempts: make(map[replication.CommandKind][][]byte),
	}
	runner, err := newDurableRequestLifecycleRunner(ledger, resolver, proposer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runner.RunWave(t.Context(), wave); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("released/changed pin at wave admission = %v", err)
	}
	proposals := 0
	for _, attempts := range proposer.attempts {
		proposals += len(attempts)
	}
	if proposer.fenceCalls != 1 || proposals != 0 {
		t.Fatalf("fences=%d proposals=%d, want 1/0", proposer.fenceCalls, proposals)
	}
}

func TestDurableRequestLifecycleRunnerDerivesCursorFromAuthenticatedSettlement(t *testing.T) {
	wave, initial, route := lifecycleRunnerFixture(t)
	wave.Transition, wave.Cursor = 0, nil
	wave.Settle = func(observation []byte) (uint32, []byte, error) {
		if len(observation) == 0 {
			return 0, nil, ErrDurableRequestConflict
		}
		return 77, []byte("settled-cursor"), nil
	}
	events := &lifecycleRunnerEvents{}
	ledger := &lifecycleRunnerLedger{head: initial, events: events}
	resolver := &lifecycleRunnerResolver{route: route, events: events}
	proposer := &lifecycleRunnerProposer{
		t: t, events: events, faultKind: -1,
		attempts: make(map[replication.CommandKind][][]byte),
	}
	runner, err := newDurableRequestLifecycleRunner(ledger, resolver, proposer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runner.RunWave(context.Background(), wave); err != nil {
		t.Fatal(err)
	}
	if ledger.continuation.TransitionTag != 77 ||
		!bytes.Equal(ledger.continuation.Cursor, []byte("settled-cursor")) {
		t.Fatal("settlement-derived cursor was not persisted")
	}
}

func TestDurableRequestLifecycleRunnerRejectsCommandAfterRouteRefresh(t *testing.T) {
	wave, head, route := lifecycleRunnerFixture(t)
	events := new(lifecycleRunnerEvents)
	ledger := &lifecycleRunnerLedger{head: head, events: events}
	route.Command.RouteGeneration++
	resolver := &lifecycleRunnerResolver{route: route, events: events}
	proposer := &lifecycleRunnerProposer{
		t: t, events: events, attempts: make(map[replication.CommandKind][][]byte),
	}
	runner, err := newDurableRequestLifecycleRunner(ledger, resolver, proposer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runner.RunWave(t.Context(), wave); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("route mismatch error = %v", err)
	}
	if len(proposer.attempts) != 0 || ledger.head.Revision != head.Revision {
		t.Fatal("mismatched live route crossed durable or proposal boundary")
	}
}

func TestDurableRequestLifecycleRunnerFencesEveryReadThroughRF3Adapter(t *testing.T) {
	wave, head, route := lifecycleRunnerFixture(t)
	headRaw, err := requestledger.AppendHead(nil, head)
	if err != nil {
		t.Fatal(err)
	}
	minimums := make([]uint64, 0, 3)
	stub := lifecycleRF3Stub{
		apply: func(context.Context, DurableRequestLedgerHome, []byte) (ReplicatedRequestLedgerApplyResult, error) {
			return ReplicatedRequestLedgerApplyResult{}, errors.New("unexpected apply")
		},
		read: func(
			_ context.Context,
			_ DurableRequestLedgerHome,
			read ReplicatedRequestLedgerRead,
		) (ReplicatedRequestLedgerReadResult, error) {
			minimums = append(minimums, read.MinimumApplied)
			if read.Kind == replicatedstate.RequestLedgerReadHead {
				return ReplicatedRequestLedgerReadResult{
					Applied: 55, Found: true,
					AuthoritativeKind: read.Kind, Value: headRaw,
				}, nil
			}
			return ReplicatedRequestLedgerReadResult{
				Applied: 55, AuthoritativeKind: read.Kind,
			}, nil
		},
	}
	adapter := &DurableRequestLedgerRF3{client: stub}
	events := new(lifecycleRunnerEvents)
	runner, err := newDurableRequestLifecycleRunner(
		adapter,
		&lifecycleRunnerResolver{route: route, events: events},
		&lifecycleRunnerProposer{
			t: t, events: events, faultKind: -1,
			attempts: make(map[replication.CommandKind][][]byte),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	keyDigest, err := requestledger.KeyDigest(wave.Key)
	if err != nil {
		t.Fatal(err)
	}
	opened, routePin, pending, _, err := runner.openWaveRows(t.Context(), wave, keyDigest)
	if err != nil || opened.Revision != head.Revision ||
		routePin.Phase != requestledger.RoutePinInvalid || pending.Revision != 0 {
		t.Fatalf("rows=%+v/%+v/%+v err=%v", opened, routePin, pending, err)
	}
	if len(minimums) != 3 || minimums[0] != 1 || minimums[1] != 55 || minimums[2] != 55 {
		t.Fatalf("minimum-applied fences = %v, want [1 55 55]", minimums)
	}
}
