package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

type lifecycleRF3Stub struct {
	apply func(context.Context, DurableRequestLedgerHome, []byte) (ReplicatedRequestLedgerApplyResult, error)
	read  func(context.Context, DurableRequestLedgerHome, ReplicatedRequestLedgerRead) (ReplicatedRequestLedgerReadResult, error)
}

func TestDurableRequestLedgerRF3ReadsWaveCutInOneRequest(t *testing.T) {
	rows := lifecycleRowFixture(t)
	homePoint, _ := requestledger.Home(rows.head.Key)
	home := DurableRequestLedgerHome{
		Identity: replication.Digest(lifecycleDigest("range")), Point: homePoint,
	}
	headRaw, _ := requestledger.AppendHead(nil, rows.head)
	routeRaw, _ := requestledger.AppendRoutePin(nil, rows.route)
	pendingRaw, _ := requestledger.AppendPendingWave(nil, rows.pending)
	raw := make([]byte, 13, 13+len(headRaw)+len(routeRaw)+len(pendingRaw))
	raw[0] = 3
	binary.LittleEndian.PutUint32(raw[1:5], uint32(len(headRaw)))
	binary.LittleEndian.PutUint32(raw[5:9], uint32(len(routeRaw)))
	binary.LittleEndian.PutUint32(raw[9:13], uint32(len(pendingRaw)))
	raw = append(raw, headRaw...)
	raw = append(raw, routeRaw...)
	raw = append(raw, pendingRaw...)
	reads := 0
	stub := lifecycleRF3Stub{read: func(
		_ context.Context, _ DurableRequestLedgerHome, read ReplicatedRequestLedgerRead,
	) (ReplicatedRequestLedgerReadResult, error) {
		reads++
		if read.Kind != replicatedstate.RequestLedgerReadWave || read.MinimumApplied != 1 ||
			read.MaxBytes != uint32(replicatedstate.MaxRequestLedgerWaveReadBytes) {
			t.Fatalf("read=%+v", read)
		}
		return ReplicatedRequestLedgerReadResult{Applied: 17, Found: true, Retries: 2,
			AuthoritativeKind: replicatedstate.RequestLedgerReadWave, Value: raw}, nil
	}}
	metrics := &DurableRequestLedgerReadMetrics{}
	ledger := &DurableRequestLedgerRF3{client: stub, readCollector: metrics}
	steps := make([]requestledger.StepRef, requestledger.MaxPendingWaveSteps)
	cut, err := ledger.ReadWaveCut(context.Background(), home, rows.head.Key, steps)
	if err != nil || reads != 1 || cut.Applied != 17 ||
		cut.Head.KeyDigest != rows.head.KeyDigest ||
		cut.Route.RecordDigest != rows.route.RecordDigest ||
		cut.Pending.WaveDigest != rows.pending.WaveDigest ||
		len(cut.Pending.Steps) != len(rows.pending.Steps) {
		t.Fatalf("reads=%d cut=%+v err=%v", reads, cut, err)
	}
	if snapshot := metrics.Snapshot(replicatedstate.RequestLedgerReadWave); snapshot !=
		(DurableRequestLedgerReadSnapshot{Calls: 1, Retries: 2, ResponseBytes: uint64(len(raw))}) {
		t.Fatalf("metrics=%+v", snapshot)
	}
}

func TestDurableRequestLedgerRF3ReadsProgressAndTerminalCutsOnce(t *testing.T) {
	rows := lifecycleRowFixture(t)
	homePoint, _ := requestledger.Home(rows.head.Key)
	home := DurableRequestLedgerHome{
		Identity: replication.Digest(lifecycleDigest("range")), Point: homePoint,
	}
	headRaw, _ := requestledger.AppendHead(nil, rows.head)
	continuationRaw, _ := requestledger.AppendContinuation(nil, rows.continuation)
	progressRaw := make([]byte, 9, 9+len(headRaw)+len(continuationRaw))
	progressRaw[0] = 1
	binary.LittleEndian.PutUint32(progressRaw[1:5], uint32(len(headRaw)))
	binary.LittleEndian.PutUint32(progressRaw[5:9], uint32(len(continuationRaw)))
	progressRaw = append(progressRaw, headRaw...)
	progressRaw = append(progressRaw, continuationRaw...)
	terminalRaw, _ := requestledger.AppendTerminal(nil, rows.terminal)
	terminalCutRaw := make([]byte, 21, 21+len(headRaw)+len(terminalRaw))
	terminalCutRaw[0] = 8
	binary.LittleEndian.PutUint32(terminalCutRaw[1:5], uint32(len(headRaw)))
	binary.LittleEndian.PutUint32(terminalCutRaw[17:21], uint32(len(terminalRaw)))
	terminalCutRaw = append(terminalCutRaw, headRaw...)
	terminalCutRaw = append(terminalCutRaw, terminalRaw...)
	reads := 0
	stub := lifecycleRF3Stub{read: func(
		_ context.Context, _ DurableRequestLedgerHome, read ReplicatedRequestLedgerRead,
	) (ReplicatedRequestLedgerReadResult, error) {
		reads++
		switch read.Kind {
		case replicatedstate.RequestLedgerReadProgress:
			if reads == 3 {
				return ReplicatedRequestLedgerReadResult{Applied: 23, Found: true, Retries: 5,
					AuthoritativeKind: read.Kind, Value: []byte{1}}, nil
			}
			return ReplicatedRequestLedgerReadResult{Applied: 21, Found: true,
				AuthoritativeKind: read.Kind, Retries: 2, Value: progressRaw}, nil
		case replicatedstate.RequestLedgerReadTerminalCut:
			return ReplicatedRequestLedgerReadResult{Applied: 22, Found: true,
				AuthoritativeKind: read.Kind, Retries: 3, Value: terminalCutRaw}, nil
		default:
			return ReplicatedRequestLedgerReadResult{}, ErrDurableRequest
		}
	}}
	metrics := &DurableRequestLedgerReadMetrics{}
	ledger := &DurableRequestLedgerRF3{client: stub, readCollector: metrics}
	progress, err := ledger.ReadProgressCut(context.Background(), home, rows.head.Key)
	if err != nil || progress.Applied != 21 ||
		progress.Continuation.ContinuationDigest != rows.continuation.ContinuationDigest {
		t.Fatalf("progress=%+v err=%v", progress, err)
	}
	terminal, err := ledger.ReadTerminalCut(context.Background(), home, rows.head.Key)
	if err != nil || terminal.Applied != 22 ||
		terminal.Terminal.ResultDigest != rows.terminal.ResultDigest || reads != 2 {
		t.Fatalf("reads=%d terminal=%+v err=%v", reads, terminal, err)
	}
	if _, err := ledger.ReadProgressCut(context.Background(), home, rows.head.Key); err == nil {
		t.Fatal("malformed progress cut succeeded")
	}
	progressMetrics := DurableRequestLedgerReadSnapshot{
		Calls: 2, Retries: 7, ResponseBytes: uint64(len(progressRaw) + 1), Errors: 1,
	}
	if got := metrics.Snapshot(replicatedstate.RequestLedgerReadProgress); got != progressMetrics {
		t.Fatalf("progress metrics=%+v want=%+v", got, progressMetrics)
	}
	terminalMetrics := DurableRequestLedgerReadSnapshot{
		Calls: 1, Retries: 3, ResponseBytes: uint64(len(terminalCutRaw)),
	}
	if got := metrics.Snapshot(replicatedstate.RequestLedgerReadTerminalCut); got != terminalMetrics {
		t.Fatalf("terminal metrics=%+v want=%+v", got, terminalMetrics)
	}
}

func BenchmarkDurableRequestLedgerRF3ReadProgressCutMetrics(b *testing.B) {
	rows := lifecycleRowFixture(b)
	homePoint, _ := requestledger.Home(rows.head.Key)
	home := DurableRequestLedgerHome{
		Identity: replication.Digest(lifecycleDigest("range")), Point: homePoint,
	}
	headRaw, _ := requestledger.AppendHead(nil, rows.head)
	continuationRaw, _ := requestledger.AppendContinuation(nil, rows.continuation)
	raw := make([]byte, 9, 9+len(headRaw)+len(continuationRaw))
	raw[0] = 1
	binary.LittleEndian.PutUint32(raw[1:5], uint32(len(headRaw)))
	binary.LittleEndian.PutUint32(raw[5:9], uint32(len(continuationRaw)))
	raw = append(raw, headRaw...)
	raw = append(raw, continuationRaw...)
	stub := lifecycleRF3Stub{read: func(
		context.Context, DurableRequestLedgerHome, ReplicatedRequestLedgerRead,
	) (ReplicatedRequestLedgerReadResult, error) {
		return ReplicatedRequestLedgerReadResult{Applied: 17, Found: true,
			AuthoritativeKind: replicatedstate.RequestLedgerReadProgress, Value: raw}, nil
	}}
	for _, test := range []struct {
		name      string
		collector DurableRequestLedgerReadCollector
	}{
		{name: "metrics_disabled"},
		{name: "metrics_enabled", collector: &DurableRequestLedgerReadMetrics{}},
	} {
		b.Run(test.name, func(b *testing.B) {
			ledger := &DurableRequestLedgerRF3{client: stub, readCollector: test.collector}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				cut, err := ledger.ReadProgressCut(ctx, home, rows.head.Key)
				if err != nil || cut.Head.KeyDigest != rows.head.KeyDigest {
					b.Fatalf("cut=%+v err=%v", cut, err)
				}
			}
		})
	}
}

func BenchmarkDurableRequestLedgerRF3ReadWaveCut(b *testing.B) {
	head, _, _ := lifecycleHead(b)
	homePoint, _ := requestledger.Home(head.Key)
	home := DurableRequestLedgerHome{
		Identity: replication.Digest(lifecycleDigest("range")), Point: homePoint,
	}
	headRaw, _ := requestledger.AppendHead(nil, head)
	raw := make([]byte, 13, 13+len(headRaw))
	binary.LittleEndian.PutUint32(raw[1:5], uint32(len(headRaw)))
	raw = append(raw, headRaw...)
	stub := lifecycleRF3Stub{read: func(
		context.Context, DurableRequestLedgerHome, ReplicatedRequestLedgerRead,
	) (ReplicatedRequestLedgerReadResult, error) {
		return ReplicatedRequestLedgerReadResult{Applied: 17, Found: true,
			AuthoritativeKind: replicatedstate.RequestLedgerReadWave, Value: raw}, nil
	}}
	for _, test := range []struct {
		name      string
		collector DurableRequestLedgerReadCollector
	}{
		{name: "metrics_disabled"},
		{name: "metrics_enabled", collector: &DurableRequestLedgerReadMetrics{}},
	} {
		b.Run(test.name, func(b *testing.B) {
			ledger := &DurableRequestLedgerRF3{client: stub, readCollector: test.collector}
			steps := make([]requestledger.StepRef, requestledger.MaxPendingWaveSteps)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				cut, err := ledger.ReadWaveCut(ctx, home, head.Key, steps)
				if err != nil || cut.Head.KeyDigest != head.KeyDigest {
					b.Fatalf("cut=%+v err=%v", cut, err)
				}
			}
		})
	}
}

func TestDurableRequestLedgerRF3ReadMetricsCoverRowsAndFailures(t *testing.T) {
	head, _, _ := lifecycleHead(t)
	homePoint, _ := requestledger.Home(head.Key)
	home := DurableRequestLedgerHome{
		Identity: replication.Digest(lifecycleDigest("range")), Point: homePoint,
	}
	headRaw, err := requestledger.AppendHead(nil, head)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	stub := lifecycleRF3Stub{read: func(
		context.Context, DurableRequestLedgerHome, ReplicatedRequestLedgerRead,
	) (ReplicatedRequestLedgerReadResult, error) {
		calls++
		if calls == 1 {
			return ReplicatedRequestLedgerReadResult{
				Applied: 9, Found: true, AuthoritativeKind: replicatedstate.RequestLedgerReadHead,
				Retries: 3, Value: headRaw,
			}, nil
		}
		return ReplicatedRequestLedgerReadResult{
			Retries: 4, Value: []byte("partial"),
		}, context.Canceled
	}}
	metrics := &DurableRequestLedgerReadMetrics{}
	ledger := &DurableRequestLedgerRF3{client: stub, readCollector: metrics}
	read := DurableRequestLifecycleRead{
		Key: head.Key, Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: 1,
	}
	row, err := ledger.ReadRow(context.Background(), home, read)
	if err != nil || !row.Found || row.Head.KeyDigest != head.KeyDigest {
		t.Fatalf("row=%+v err=%v", row, err)
	}
	if _, err := ledger.ReadRow(context.Background(), home, read); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	want := DurableRequestLedgerReadSnapshot{
		Calls: 2, Retries: 7, ResponseBytes: uint64(len(headRaw) + len("partial")), Errors: 1,
	}
	if got := metrics.Snapshot(replicatedstate.RequestLedgerReadHead); got != want {
		t.Fatalf("metrics=%+v want=%+v", got, want)
	}
	if got := metrics.Snapshot(replicatedstate.RequestLedgerReadKind(0xff)); got !=
		(DurableRequestLedgerReadSnapshot{}) {
		t.Fatalf("unknown metrics=%+v", got)
	}
	metrics.ObserveDurableRequestLedgerRead(DurableRequestLedgerReadObservation{
		Kind: replicatedstate.RequestLedgerReadKind(0xff), Failed: true,
	})
	if got := metrics.Snapshot(replicatedstate.RequestLedgerReadHead); got != want {
		t.Fatalf("known metrics changed by unknown kind: %+v", got)
	}
}

func (stub lifecycleRF3Stub) Apply(ctx context.Context, home DurableRequestLedgerHome, raw []byte) (ReplicatedRequestLedgerApplyResult, error) {
	return stub.apply(ctx, home, raw)
}

func (stub lifecycleRF3Stub) Read(ctx context.Context, home DurableRequestLedgerHome, read ReplicatedRequestLedgerRead) (ReplicatedRequestLedgerReadResult, error) {
	return stub.read(ctx, home, read)
}

func lifecycleDigest(label string) requestledger.Digest {
	return requestledger.Digest(sha256.Sum256([]byte(label)))
}

func TestReplicatedRequestLedgerProposalIdentityPreservesGrammar(t *testing.T) {
	command := requestledger.CommandView{Command: requestledger.Command{
		Operation: requestledger.OperationAdvance, KeyDigest: lifecycleDigest("key"),
		ExpectedRevision: 41, Revision: 42,
	}}
	raw := bytes.Repeat([]byte{0xa5}, 4096)
	got, _, _, _ := replicatedRequestLedgerProposalIdentity(command, raw)
	hash := sha256.New()
	_, _ = hash.Write([]byte(replicatedRequestLedgerProposalDomain))
	_, _ = hash.Write(command.KeyDigest[:])
	_, _ = hash.Write([]byte{byte(command.Operation)})
	var fixed [24]byte
	binary.LittleEndian.PutUint64(fixed[:8], command.ExpectedRevision)
	binary.LittleEndian.PutUint64(fixed[8:16], command.Revision)
	binary.LittleEndian.PutUint64(fixed[16:24], uint64(len(raw)))
	_, _ = hash.Write(fixed[:])
	body := sha256.Sum256(raw)
	_, _ = hash.Write(body[:])
	var want replication.Digest
	_ = hash.Sum(want[:0])
	if got != want {
		t.Fatalf("proposal identity changed: got=%x want=%x", got, want)
	}
}

func TestDurableRequestLifecycleCommandFusesAcquiredRouteAndPending(t *testing.T) {
	head, _, _ := lifecycleHead(t)
	acquiring, err := requestledger.NewRoutePinAcquiring(
		head, requestledger.PinID{2}, lifecycleDigest("fused-binding"),
		lifecycleDigest("fused-physical"), []byte("fused-acquire-command"),
	)
	if err != nil {
		t.Fatal(err)
	}
	intentHead, err := requestledger.AdvanceHeadRoutePin(
		head, requestledger.RoutePinRecord{}, acquiring, head.Revision+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := requestledger.RecordVerifiedRoutePinAcquired(
		acquiring, acquiring.Revision+1, []byte("fused-acquire-completion"),
	)
	if err != nil {
		t.Fatal(err)
	}
	acquiredHead, err := requestledger.AdvanceHeadRoutePin(
		intentHead, acquiring, acquired, intentHead.Revision+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := requestledger.NewPendingWaveWithRoutePin(
		acquiredHead, requestledger.PayloadBuildRecord{}, acquiredHead.Revision+1,
		acquired, []requestledger.StepRef{{
			TargetSource:  requestledger.PayloadSourcePlan,
			CommandSource: requestledger.PayloadSourcePlan,
			TargetOffset:  0, TargetLength: 8, CommandOffset: 8, CommandLength: 16,
			TargetDigest:  lifecycleDigest("fused-target"),
			CommandDigest: lifecycleDigest("fused-command"),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	command, err := durableRequestLifecycleCommand(head.KeyDigest, DurableRequestLifecycleCAS{
		Operation:        requestledger.OperationRecordRoutePinAcquiredPutPending,
		ExpectedRevision: intentHead.Revision, Revision: pending.Revision,
		RoutePin: acquired, Pending: pending,
	})
	if err != nil {
		t.Fatal(err)
	}
	command.ExpectedRangeIdentity = lifecycleDigest("range")
	command.Home, _ = requestledger.Home(head.Key)
	raw, err := requestledger.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := requestledger.OpenCommandInto(
		raw, make([]requestledger.StepRef, requestledger.MaxPendingWaveSteps),
	)
	compound, ok := opened.AcquiredPending()
	if err != nil || !ok || opened.ExpectedRevision != intentHead.Revision ||
		opened.Revision != pending.Revision || compound.Route().RecordDigest != acquired.RecordDigest ||
		compound.Pending().Digest() != pending.WaveDigest {
		t.Fatalf("fused lifecycle command: ok=%v opened=%+v err=%v", ok, opened.Command, err)
	}
}

func BenchmarkReplicatedRequestLedgerProposalIdentity(b *testing.B) {
	command := requestledger.CommandView{Command: requestledger.Command{
		Operation: requestledger.OperationAdvance, KeyDigest: lifecycleDigest("key"),
		ExpectedRevision: 41, Revision: 42,
	}}
	raw := bytes.Repeat([]byte{0xa5}, 4096)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for range b.N {
		digest, _, _, _ := replicatedRequestLedgerProposalIdentity(command, raw)
		if digest == (replication.Digest{}) {
			b.Fatal("zero digest")
		}
	}
}

func lifecycleKey() requestledger.RequestKey {
	key := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, TenantDigest: lifecycleDigest("tenant"),
		IssuerEpoch: 1, IssuerSequence: 1, IssuerLane: requestledger.IssuerLane{1},
	}
	copy(key.Principal[:], []byte("principal-000001"))
	copy(key.Request[:], []byte("request-00000001"))
	return key
}

func lifecycleHead(t testing.TB) (requestledger.HeadRecord, []byte, []byte) {
	t.Helper()
	recipe := bytes.Repeat([]byte{0x5a}, 256)
	plan, err := requestledger.AppendPlan(nil, recipe)
	if err != nil {
		t.Fatal(err)
	}
	cursor := []byte("terminal-cursor")
	contract := requestledger.ExecutionContract{
		CatalogGeneration: 7, PinID: requestledger.PinID{1},
		PinDigest:                    lifecycleDigest("pin"),
		RouteSchemaCertificateDigest: lifecycleDigest("route-cert"),
		MaxPendingWaveBytes:          requestledger.MaxPendingWaveRecordBytes,
		MaxContinuationBytes:         requestledger.MaxContinuationRecordBytes,
		MaxTerminalBytes:             requestledger.MaxLifecyclePayloadBytes,
		MaxActivePayloadBytes:        2 * requestledger.MaxPlanPageBytes,
		MaxActivePayloadChunks:       2,
		PlanBuildID:                  lifecycleDigest("plan-build"),
		PlanBuildGeneration:          1,
		PlanningLeaseSpan:            requestledger.MaxPlanningLeaseSpan,
		PlanningLeaseGeneration:      1,
		TerminalTransitionTag:        9,
		FinalWaveCount:               1,
		TerminalStateDigest:          requestledger.NextStateDigest(9, cursor),
		TerminalSummaryDigest:        lifecycleDigest("retirement"),
		AbortTerminalTransitionTag:   10,
		AbortFinalWaveCount:          1,
		AbortTerminalStateDigest:     requestledger.NextStateDigest(10, cursor),
	}
	head, err := requestledger.NewHeadWithExecutionContract(
		lifecycleKey(), lifecycleDigest("request"), lifecycleDigest("terminal"), contract, plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	return head, plan, cursor
}

type lifecycleRows struct {
	head         requestledger.HeadRecord
	page         requestledger.PlanPageRecord
	pending      requestledger.PendingWaveRecord
	terminal     requestledger.TerminalRecord
	ack          requestledger.AckRecord
	continuation requestledger.ContinuationRecord
	chunk        requestledger.PayloadChunkRecord
	build        requestledger.PayloadBuildRecord
	route        requestledger.RoutePinRecord
	prepared     requestledger.PreparedTerminalRecord
	schema       requestledger.SchemaPinReleaseRecord
}

func lifecycleRowFixture(t testing.TB) lifecycleRows {
	t.Helper()
	head, plan, cursor := lifecycleHead(t)
	route, err := requestledger.NewRoutePinAcquiring(
		head, requestledger.PinID{2}, lifecycleDigest("binding"),
		lifecycleDigest("physical"), []byte("acquire-command"),
	)
	if err != nil {
		t.Fatal(err)
	}
	route, err = requestledger.RecordVerifiedRoutePinAcquired(route, 2, []byte("acquire-completion"))
	if err != nil {
		t.Fatal(err)
	}
	steps := []requestledger.StepRef{{
		TargetSource: requestledger.PayloadSourcePlan, CommandSource: requestledger.PayloadSourcePlan,
		TargetOffset: 0, TargetLength: 8, CommandOffset: 8, CommandLength: 16,
		TargetDigest: lifecycleDigest("target"), CommandDigest: lifecycleDigest("command"),
	}}
	pending, err := requestledger.NewPendingWaveWithRoutePin(
		head, requestledger.PayloadBuildRecord{}, 2, route, steps,
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.InstallPendingWave(head, pending, requestledger.PayloadBuildRecord{}, route)
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := requestledger.NewContinuation(
		head, pending, route, 3, head.TerminalTransitionTag, cursor, []byte("observation"),
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvancePending(head, pending, continuation)
	if err != nil {
		t.Fatal(err)
	}
	route, err = requestledger.BeginRoutePinRelease(route, 3, []byte("release-command"))
	if err != nil {
		t.Fatal(err)
	}
	route, err = requestledger.RecordVerifiedRoutePinReleased(route, 4, []byte("release-completion"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkRoutePinReleased(head, route, 4)
	if err != nil {
		t.Fatal(err)
	}
	var token requestledger.AckToken
	copy(token[:], []byte("ack-capability-token-00000000001"))
	prepared, err := requestledger.NewPreparedTerminal(
		head, continuation, 5, requestledger.OutcomeCommitted, 12, true,
		[]byte("result"), head.TerminalSummaryDigest, token,
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkTerminalPrepared(head, continuation, prepared)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := requestledger.NewSchemaPinRelease(head, prepared, 6, []byte("schema-release-command"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.InstallSchemaPinRelease(head, prepared, schema)
	if err != nil {
		t.Fatal(err)
	}
	intent := schema
	schema, err = requestledger.RecordVerifiedSchemaPinReleased(schema, 7, []byte("schema-release-completion"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkSchemaPinReleased(head, prepared, intent, schema)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := requestledger.NewTerminal(head, prepared, schema, 8)
	if err != nil {
		t.Fatal(err)
	}
	terminalHead, err := requestledger.MarkTerminal(head, prepared, schema, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := requestledger.NewAck(terminalHead, terminal, 9, 4096)
	if err != nil {
		t.Fatal(err)
	}

	buildHead, _, _ := lifecycleHead(t)
	build, err := requestledger.NewPayloadBuild(buildHead, lifecycleDigest("content"), 4, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := requestledger.NewPayloadChunk(build, []byte("data"))
	if err != nil {
		t.Fatal(err)
	}

	widePlan, err := requestledger.AppendPlan(nil, bytes.Repeat([]byte{0x31}, requestledger.MaxInlinePlanBytes+1))
	if err != nil {
		t.Fatal(err)
	}
	keyDigest, _ := requestledger.KeyDigest(lifecycleKey())
	root, _ := requestledger.PlanRoot(keyDigest, widePlan)
	paged, err := requestledger.NewPagedHead(
		lifecycleKey(), lifecycleDigest("request"), lifecycleDigest("terminal"), uint64(len(widePlan)), root,
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := requestledger.NewPlanPageData(paged, 0, requestledger.Digest{}, widePlan)
	if err != nil {
		t.Fatal(err)
	}
	_ = plan
	return lifecycleRows{head: head, page: page, pending: pending, terminal: terminal, ack: ack,
		continuation: continuation, chunk: chunk, build: build, route: route,
		prepared: prepared, schema: schema}
}

func TestDurableRequestLifecycleReopenMapsEveryStorageKind(t *testing.T) {
	rows := lifecycleRowFixture(t)
	tests := []struct {
		name string
		kind replicatedstate.RequestLedgerReadKind
		raw  func([]byte) ([]byte, error)
		ok   func(DurableRequestLifecycleRow) bool
	}{
		{"head", replicatedstate.RequestLedgerReadHead, func(dst []byte) ([]byte, error) { return requestledger.AppendHead(dst, rows.head) }, func(row DurableRequestLifecycleRow) bool { return row.Head.Key == rows.head.Key }},
		{"page", replicatedstate.RequestLedgerReadPlanPage, func(dst []byte) ([]byte, error) { return requestledger.AppendPlanPage(dst, rows.page) }, func(row DurableRequestLifecycleRow) bool { return row.PlanPage.Chain == rows.page.Chain }},
		{"pending", replicatedstate.RequestLedgerReadPending, func(dst []byte) ([]byte, error) { return requestledger.AppendPendingWave(dst, rows.pending) }, func(row DurableRequestLifecycleRow) bool { return row.Pending.WaveDigest == rows.pending.WaveDigest }},
		{"terminal", replicatedstate.RequestLedgerReadTerminal, func(dst []byte) ([]byte, error) { return requestledger.AppendTerminal(dst, rows.terminal) }, func(row DurableRequestLifecycleRow) bool {
			return row.Terminal.ResultDigest == rows.terminal.ResultDigest
		}},
		{"ack", replicatedstate.RequestLedgerReadAck, func(dst []byte) ([]byte, error) { return requestledger.AppendAck(dst, rows.ack) }, func(row DurableRequestLifecycleRow) bool { return row.Ack.AckDigest == rows.ack.AckDigest }},
		{"continuation", replicatedstate.RequestLedgerReadContinuation, func(dst []byte) ([]byte, error) { return requestledger.AppendContinuation(dst, rows.continuation) }, func(row DurableRequestLifecycleRow) bool {
			return row.Continuation.ContinuationDigest == rows.continuation.ContinuationDigest
		}},
		{"payload_chunk", replicatedstate.RequestLedgerReadPayloadChunk, func(dst []byte) ([]byte, error) { return requestledger.AppendPayloadChunk(dst, rows.chunk) }, func(row DurableRequestLifecycleRow) bool {
			return row.PayloadChunk.Chain == rows.chunk.Chain
		}},
		{"payload_build", replicatedstate.RequestLedgerReadPayloadBuild, func(dst []byte) ([]byte, error) { return requestledger.AppendPayloadBuild(dst, rows.build) }, func(row DurableRequestLifecycleRow) bool {
			return row.PayloadBuild.BuildDigest == rows.build.BuildDigest
		}},
		{"route_pin", replicatedstate.RequestLedgerReadRoutePin, func(dst []byte) ([]byte, error) { return requestledger.AppendRoutePin(dst, rows.route) }, func(row DurableRequestLifecycleRow) bool { return row.RoutePin.RecordDigest == rows.route.RecordDigest }},
		{"prepared", replicatedstate.RequestLedgerReadPrepared, func(dst []byte) ([]byte, error) { return requestledger.AppendPreparedTerminal(dst, rows.prepared) }, func(row DurableRequestLifecycleRow) bool {
			return row.Prepared.PreparedDigest == rows.prepared.PreparedDigest
		}},
		{"schema_pin", replicatedstate.RequestLedgerReadSchemaPin, func(dst []byte) ([]byte, error) { return requestledger.AppendSchemaPinRelease(dst, rows.schema) }, func(row DurableRequestLifecycleRow) bool {
			return row.SchemaPin.RecordDigest == rows.schema.RecordDigest
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := test.raw(nil)
			if err != nil {
				t.Fatal(err)
			}
			row := DurableRequestLifecycleRow{Found: true, Kind: test.kind, Raw: raw}
			steps := make([]requestledger.StepRef, requestledger.MaxPendingWaveSteps)
			if err := openDurableRequestLifecycleRow(&row, steps); err != nil || !test.ok(row) {
				t.Fatalf("mapped=%+v err=%v", row, err)
			}
		})
	}
}

func TestDurableRequestLedgerRF3AppliesTypedCASAndReopensAuthoritativeAck(t *testing.T) {
	rows := lifecycleRowFixture(t)
	createHead, _, _ := lifecycleHead(t)
	homePoint, _ := requestledger.Home(createHead.Key)
	home := DurableRequestLedgerHome{Identity: replication.Digest(lifecycleDigest("range")), Point: homePoint}
	command := DurableRequestLifecycleCAS{
		Operation: requestledger.OperationCreate, Revision: createHead.Revision,
		Head: createHead,
	}
	ackRaw, _ := requestledger.AppendAck(nil, rows.ack)
	stub := lifecycleRF3Stub{}
	stub.apply = func(_ context.Context, gotHome DurableRequestLedgerHome, raw []byte) (ReplicatedRequestLedgerApplyResult, error) {
		opened, err := requestledger.OpenCommandInto(raw, nil)
		if err != nil || gotHome.Identity != home.Identity || opened.Home != home.Point ||
			opened.ExpectedRangeIdentity != requestledger.Digest(home.Identity) {
			t.Fatalf("untrusted authority entered command: %+v %v", opened.Command, err)
		}
		return ReplicatedRequestLedgerApplyResult{
			Ledger: replicatedstate.RequestLedgerCompletionResult{
				Operation: opened.Operation, Phase: requestledger.PhaseSealed,
				ResultCode: replicatedstate.ResultApplied, Revision: opened.Revision,
				KeyDigest: opened.KeyDigest, RequestDigest: opened.RequestDigest,
				PlanRoot: opened.PlanRoot, RangeIdentity: opened.ExpectedRangeIdentity,
				StateDigest: lifecycleDigest("state"),
			},
			Native: ReplicatedResult{Outcome: raftserve.Outcome{AppliedIndex: 41}, Retries: 2},
		}, nil
	}
	stub.read = func(_ context.Context, _ DurableRequestLedgerHome, read ReplicatedRequestLedgerRead) (ReplicatedRequestLedgerReadResult, error) {
		if read.Kind != replicatedstate.RequestLedgerReadHead ||
			read.MaxBytes != uint32(requestledger.MaxHeadRecordBytes) {
			t.Fatalf("read bound was caller-selected: %+v", read)
		}
		return ReplicatedRequestLedgerReadResult{
			Applied: 42, Found: true, AuthoritativeKind: replicatedstate.RequestLedgerReadAck,
			Value: ackRaw, Retries: 1,
		}, nil
	}
	ledger := &DurableRequestLedgerRF3{client: stub}
	cas, err := ledger.ApplyCAS(context.Background(), home, createHead.Key, command)
	if err != nil || cas.Applied != 41 || cas.Retries != 2 {
		t.Fatalf("cas=%+v err=%v", cas, err)
	}
	row, err := ledger.ReadRow(context.Background(), home, DurableRequestLifecycleRead{
		Key: createHead.Key, Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: 41,
	})
	if err != nil || !row.Found || row.Kind != replicatedstate.RequestLedgerReadAck ||
		row.Ack.AckDigest != rows.ack.AckDigest || row.Applied != 42 {
		t.Fatalf("row=%+v err=%v", row, err)
	}
	if len(row.Raw) == 0 || &row.Raw[0] != &ackRaw[0] {
		t.Fatal("typed lifecycle read copied its owned RF3 response")
	}
}

func BenchmarkDurableRequestLedgerRF3ReopenOwnedHead(b *testing.B) {
	head, _, _ := lifecycleHead(b)
	homePoint, _ := requestledger.Home(head.Key)
	home := DurableRequestLedgerHome{
		Identity: replication.Digest(lifecycleDigest("range")), Point: homePoint,
	}
	raw, err := requestledger.AppendHead(nil, head)
	if err != nil {
		b.Fatal(err)
	}
	stub := lifecycleRF3Stub{
		read: func(context.Context, DurableRequestLedgerHome, ReplicatedRequestLedgerRead) (ReplicatedRequestLedgerReadResult, error) {
			return ReplicatedRequestLedgerReadResult{Applied: 1, Found: true,
				AuthoritativeKind: replicatedstate.RequestLedgerReadHead, Value: raw}, nil
		},
	}
	ledger := &DurableRequestLedgerRF3{client: stub}
	read := DurableRequestLifecycleRead{
		Key: head.Key, Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: 1,
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for range b.N {
		row, readErr := ledger.ReadRow(context.Background(), home, read)
		if readErr != nil || row.Head.Key != head.Key {
			b.Fatalf("row=%+v err=%v", row, readErr)
		}
	}
}
