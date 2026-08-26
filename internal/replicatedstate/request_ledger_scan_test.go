package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"math"
	"sort"
	"testing"

	"github.com/thesyncim/vibedb/internal/requestledger"
)

type requestLedgerImageRow struct {
	key, value []byte
}

func requestLedgerStateTestDigest(value string) requestledger.Digest {
	return requestledger.Digest(sha256.Sum256([]byte(value)))
}

func requestLedgerStateTestHead(t testing.TB, dynamic bool) (requestledger.HeadRecord, []byte) {
	t.Helper()
	plan, err := requestledger.AppendPlan(nil, []byte("canonical scanner recipe"))
	if err != nil {
		t.Fatal(err)
	}
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		TenantDigest: requestLedgerStateTestDigest("tenant"), Principal: requestledger.PrincipalID{1},
		Request: requestledger.RequestID{2}}
	keyDigest, err := requestledger.KeyDigest(key)
	if err != nil {
		t.Fatal(err)
	}
	root, err := requestledger.PlanRoot(keyDigest, plan)
	if err != nil {
		t.Fatal(err)
	}
	contract := requestledger.ExecutionContract{
		CatalogGeneration: 7, PinID: requestledger.PinID{3},
		PinDigest:                    requestLedgerStateTestDigest("pin"),
		RouteSchemaCertificateDigest: requestLedgerStateTestDigest("route-schema"),
		MaxPendingWaveBytes:          requestledger.MaxPendingWaveRecordBytes,
		MaxContinuationBytes:         requestledger.MaxContinuationRecordBytes,
		MaxTerminalBytes:             requestledger.MaxLifecyclePayloadBytes,
		PlanBuildID:                  requestLedgerStateTestDigest("build"),
		PlanBuildGeneration:          1,
		PlanningLeaseSpan:            requestledger.MaxPlanningLeaseSpan,
		PlanningLeaseGeneration:      1,
		TerminalTransitionTag:        11,
		FinalWaveCount:               1,
		TerminalStateDigest:          requestledger.NextStateDigest(11, []byte("terminal")),
		TerminalSummaryDigest:        requestLedgerStateTestDigest("terminal-summary"),
		AbortTerminalTransitionTag:   12,
		AbortFinalWaveCount:          1,
		AbortTerminalStateDigest:     requestledger.NextStateDigest(12, []byte("abort")),
	}
	if dynamic {
		contract.MaxActivePayloadBytes = requestledger.MaxPlanPageBytes
		contract.MaxActivePayloadChunks = 1
	}
	head, err := requestledger.NewHeadWithExecutionContract(
		key, requestLedgerStateTestDigest("request"), requestLedgerStateTestDigest("terminal-contract"),
		contract, plan,
	)
	if err != nil || head.PlanRoot != root {
		t.Fatalf("head: root=%x want=%x err=%v", head.PlanRoot, root, err)
	}
	return head, plan
}

func scanRequestLedgerImage(t testing.TB, head requestledger.HeadRecord, rows []requestLedgerImageRow) {
	t.Helper()
	sort.Slice(rows, func(left, right int) bool { return string(rows[left].key) < string(rows[right].key) })
	scanner := newRequestLedgerImageScanner(math.MaxUint64>>1, 1, RequestLedgerRange{Identity: requestledger.Digest{1}})
	for _, row := range rows {
		if err := validateSnapshotRequestLedgerRow(row.key, row.value); err != nil {
			t.Fatalf("snapshot row %x: %v", row.key, err)
		}
		if err := scanner.observe(row.key, row.value); err != nil {
			t.Fatalf("observe row %x: %v", row.key, err)
		}
	}
	if err := scanner.finishRequest(); err != nil {
		t.Fatalf("reopen scanner: %v", err)
	}
	wantHead, _ := requestledger.AppendHead(nil, head)
	gotHead, _ := requestledger.AppendHead(nil, scanner.head)
	if !bytes.Equal(gotHead, wantHead) {
		t.Fatal("scanner changed the canonical head")
	}
}

func TestRequestLedgerImageScannerReopensPayloadBuildPhases(t *testing.T) {
	head, _ := requestLedgerStateTestHead(t, true)
	home, _ := requestledger.Home(head.Key)
	headRaw, _ := requestledger.AppendHead(nil, head)
	data := []byte("dynamic payload")
	accumulator, err := requestledger.NewPayloadRootAccumulator(head.KeyDigest, uint64(len(data)))
	if err != nil || accumulator.Append(data) != nil {
		t.Fatal(err)
	}
	root, err := accumulator.Root()
	if err != nil {
		t.Fatal(err)
	}
	build, err := requestledger.NewPayloadBuild(head, root, uint64(len(data)), 1)
	if err != nil {
		t.Fatal(err)
	}
	buildKey := requestledger.AppendPayloadBuildKey(nil, home, head.KeyDigest)
	buildRaw, _ := requestledger.AppendPayloadBuild(nil, build)
	headKey := requestledger.AppendHeadKey(nil, home, head.KeyDigest)
	t.Run("staging", func(t *testing.T) {
		scanRequestLedgerImage(t, head, []requestLedgerImageRow{{headKey, headRaw}, {buildKey, buildRaw}})
	})

	chunk, err := requestledger.NewPayloadChunk(build, data)
	if err != nil {
		t.Fatal(err)
	}
	build, err = requestledger.AdvancePayloadBuild(build, chunk, build.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	build, err = requestledger.SealPayloadBuild(build, build.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	chunkRaw, _ := requestledger.AppendPayloadChunk(nil, chunk)
	buildRaw, _ = requestledger.AppendPayloadBuild(nil, build)
	chunkKey := requestledger.AppendPayloadChunkKey(nil, home, head.KeyDigest, root, 0)
	t.Run("sealed", func(t *testing.T) {
		scanRequestLedgerImage(t, head, []requestLedgerImageRow{
			{headKey, headRaw}, {chunkKey, chunkRaw}, {buildKey, buildRaw},
		})
	})
}

func requestLedgerStateTestPrepared(t testing.TB) (
	requestledger.HeadRecord, requestledger.ContinuationRecord, requestledger.PreparedTerminalRecord,
) {
	t.Helper()
	head, plan := requestLedgerStateTestHead(t, false)
	acquiring, err := requestledger.NewRoutePinAcquiring(head, requestledger.PinID{4},
		requestLedgerStateTestDigest("binding"), requestLedgerStateTestDigest("physical"), []byte("acquire"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, requestledger.RoutePinRecord{}, acquiring, 2)
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := requestledger.RecordVerifiedRoutePinAcquired(acquiring, 2, []byte("acquired"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, acquiring, acquired, 3)
	if err != nil {
		t.Fatal(err)
	}
	step := requestledger.StepRef{TargetSource: requestledger.PayloadSourcePlan,
		CommandSource: requestledger.PayloadSourcePlan, TargetOffset: 0, TargetLength: 1,
		CommandOffset: 1, CommandLength: 1, TargetDigest: requestLedgerStateTestDigest("target"),
		CommandDigest: requestLedgerStateTestDigest("command")}
	if len(plan) < 2 {
		t.Fatal("short plan")
	}
	pending, err := requestledger.NewPendingWaveWithRoutePin(
		head, requestledger.PayloadBuildRecord{}, 4, acquired, []requestledger.StepRef{step},
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.InstallPendingWave(head, pending, requestledger.PayloadBuildRecord{}, acquired)
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := requestledger.NewContinuation(
		head, pending, acquired, 5, head.TerminalTransitionTag, []byte("terminal"), []byte("observation"),
	)
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvancePending(head, pending, continuation)
	if err != nil {
		t.Fatal(err)
	}
	releasing, err := requestledger.BeginRoutePinRelease(acquired, 3, []byte("release"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, acquired, releasing, 6)
	if err != nil {
		t.Fatal(err)
	}
	released, err := requestledger.RecordVerifiedRoutePinReleased(releasing, 4, []byte("released"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkRoutePinReleased(head, released, 7)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := requestledger.NewPreparedTerminal(head, continuation, 8,
		requestledger.OutcomeCommitted, 1, true, []byte("result"),
		head.TerminalSummaryDigest, requestledger.AckToken{5})
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkTerminalPrepared(head, continuation, prepared)
	if err != nil {
		t.Fatal(err)
	}
	return head, continuation, prepared
}

func TestRequestLedgerImageScannerReopensSchemaReleasePhases(t *testing.T) {
	preparedHead, continuation, prepared := requestLedgerStateTestPrepared(t)
	home, _ := requestledger.Home(preparedHead.Key)
	continuationRaw, _ := requestledger.AppendContinuation(nil, continuation)
	preparedRaw, _ := requestledger.AppendPreparedTerminal(nil, prepared)
	base := func(head requestledger.HeadRecord, schemaRaw []byte) []requestLedgerImageRow {
		headRaw, _ := requestledger.AppendHead(nil, head)
		rows := []requestLedgerImageRow{
			{requestledger.AppendHeadKey(nil, home, head.KeyDigest), headRaw},
			{requestledger.AppendContinuationKey(nil, home, head.KeyDigest), continuationRaw},
			{requestledger.AppendPreparedTerminalKey(nil, home, head.KeyDigest), preparedRaw},
		}
		if schemaRaw != nil {
			rows = append(rows, requestLedgerImageRow{
				requestledger.AppendSchemaPinReleaseKey(nil, home, head.KeyDigest), schemaRaw,
			})
		}
		return rows
	}
	t.Run("prepared", func(t *testing.T) {
		scanRequestLedgerImage(t, preparedHead, base(preparedHead, nil))
	})
	intent, err := requestledger.NewSchemaPinRelease(preparedHead, prepared, 9, []byte("release-schema"))
	if err != nil {
		t.Fatal(err)
	}
	releasingHead, err := requestledger.InstallSchemaPinRelease(preparedHead, prepared, intent)
	if err != nil {
		t.Fatal(err)
	}
	intentRaw, _ := requestledger.AppendSchemaPinRelease(nil, intent)
	t.Run("releasing", func(t *testing.T) {
		scanRequestLedgerImage(t, releasingHead, base(releasingHead, intentRaw))
	})
	released, err := requestledger.RecordVerifiedSchemaPinReleased(intent, 10, []byte("released-schema"))
	if err != nil {
		t.Fatal(err)
	}
	releasedHead, err := requestledger.MarkSchemaPinReleased(releasingHead, prepared, intent, released)
	if err != nil {
		t.Fatal(err)
	}
	releasedRaw, _ := requestledger.AppendSchemaPinRelease(nil, released)
	t.Run("released", func(t *testing.T) {
		scanRequestLedgerImage(t, releasedHead, base(releasedHead, releasedRaw))
	})
}
