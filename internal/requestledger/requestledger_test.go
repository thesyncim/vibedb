package requestledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"math"
	"testing"
)

func testDigest(label string) Digest { return Digest(sha256.Sum256([]byte(label))) }

func testKey(sequenced bool) RequestKey {
	key := RequestKey{Scope: ScopeAuthenticated, TenantDigest: testDigest("tenant")}
	copy(key.Principal[:], []byte("principal-000001"))
	copy(key.Request[:], []byte("request-00000001"))
	if sequenced {
		key.IssuerEpoch, key.IssuerSequence = 9, 42
		copy(key.IssuerLane[:], []byte("lane0001"))
	}
	return key
}

func testPlan(t testing.TB, recipeBytes int) []byte {
	t.Helper()
	recipe := bytes.Repeat([]byte{0x5a}, recipeBytes)
	plan, err := AppendPlan(make([]byte, 0, len(recipe)+64), recipe)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testHead(t testing.TB, sequenced bool) (HeadRecord, []byte, []byte) {
	return testHeadForKey(t, testKey(sequenced))
}

func testHeadForKey(t testing.TB, key RequestKey) (HeadRecord, []byte, []byte) {
	t.Helper()
	plan := testPlan(t, 256)
	cursor := []byte("terminal-cursor")
	contract := ExecutionContract{
		CatalogGeneration: 7, PinID: PinID{1}, PinDigest: testDigest("pin"),
		RouteSchemaCertificateDigest: testDigest("route-cert"),
		MaxPendingWaveBytes:          MaxPendingWaveRecordBytes,
		MaxContinuationBytes:         MaxContinuationRecordBytes,
		MaxTerminalBytes:             MaxLifecyclePayloadBytes,
		TerminalTransitionTag:        9, FinalWaveCount: 1,
		TerminalStateDigest:        NextStateDigest(9, cursor),
		TerminalSummaryDigest:      testDigest("retirement"),
		AbortTerminalTransitionTag: 10, AbortFinalWaveCount: 1,
		AbortTerminalStateDigest: NextStateDigest(10, cursor),
		MaxActivePayloadBytes:    2 * MaxPlanPageBytes,
		MaxActivePayloadChunks:   2,
		PlanBuildID:              testDigest("plan-build"), PlanBuildGeneration: 1,
		PlanningLeaseExpiryIndex: math.MaxUint64,
		PlanningLeaseGeneration:  1,
	}
	head, err := NewHeadWithExecutionContract(key, testDigest("request-body"),
		testDigest("terminal-contract"), contract, plan)
	if err != nil {
		t.Fatal(err)
	}
	return head, plan, cursor
}

func testAcquiredRoutePin(t testing.TB, head HeadRecord) RoutePinRecord {
	t.Helper()
	pin, err := NewRoutePinAcquiring(
		head, PinID{2}, testDigest("route-binding"), testDigest("physical-route"),
		[]byte("exact-acquire-command"),
	)
	if err != nil {
		t.Fatal(err)
	}
	pin, err = RecordVerifiedRoutePinAcquired(pin, pin.Revision+1, []byte("exact-acquire-completion"))
	if err != nil {
		t.Fatal(err)
	}
	return pin
}

func testReleasedRoutePin(t testing.TB, pin RoutePinRecord) RoutePinRecord {
	t.Helper()
	var err error
	pin, err = BeginRoutePinRelease(pin, pin.Revision+1, []byte("exact-release-command"))
	if err != nil {
		t.Fatal(err)
	}
	pin, err = RecordVerifiedRoutePinReleased(pin, pin.Revision+1, []byte("exact-release-completion"))
	if err != nil {
		t.Fatal(err)
	}
	return pin
}

func TestRouteGateIdentityAndBindingDerivationIsExactAndAllocationFree(t *testing.T) {
	key := testDigest("route-gate-key")
	request := testDigest("route-gate-request")
	plan := testDigest("route-gate-plan")
	logical := testDigest("route-gate-logical-binding")
	physical := testDigest("route-gate-physical-witness")
	pin := PinID{1, 2, 3, 4}
	identity, err := DeriveRouteGateIdentity(key, request, plan, Digest{}, pin, 0)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := DeriveRouteGateBinding(identity, logical, physical, 7)
	if err != nil {
		t.Fatal(err)
	}
	changedIdentity, _ := DeriveRouteGateIdentity(key, request, plan, Digest{}, PinID{2}, 0)
	changedBinding, _ := DeriveRouteGateBinding(identity, logical, physical, 8)
	if identity == (Digest{}) || binding == (Digest{}) || changedIdentity == identity ||
		changedBinding == binding {
		t.Fatal("route-gate derivation did not bind every semantic input")
	}
	if _, err = DeriveRouteGateIdentity(key, request, plan, testDigest("prior"), pin, 0); err == nil {
		t.Fatal("wave zero accepted a prior continuation")
	}
	if _, err = DeriveRouteGateBinding(identity, logical, physical, 0); err == nil {
		t.Fatal("route-gate binding accepted epoch zero")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		gotIdentity, deriveErr := DeriveRouteGateIdentity(key, request, plan, Digest{}, pin, 0)
		if deriveErr != nil {
			panic(deriveErr)
		}
		if _, deriveErr = DeriveRouteGateBinding(gotIdentity, logical, physical, 7); deriveErr != nil {
			panic(deriveErr)
		}
	}); allocs != 0 {
		t.Fatalf("route-gate derivation allocations = %v", allocs)
	}
}

func TestSequencedKeyHomeAndStorageKeys(t *testing.T) {
	first := testKey(true)
	second := first
	second.IssuerSequence++
	second.Request[0]++
	fd, _ := KeyDigest(first)
	sd, _ := KeyDigest(second)
	if fd == sd {
		t.Fatal("full keys collided")
	}
	fh, _ := Home(first)
	sh, _ := Home(second)
	if fh != sh {
		t.Fatal("one issuer lane scattered")
	}
	second.IssuerLane[0]++
	different, _ := Home(second)
	if different == fh {
		t.Fatal("different issuer lanes collided")
	}

	keys := [][]byte{
		AppendHeadKey(nil, fh, fd), AppendPlanPageKey(nil, fh, fd, 8),
		AppendPendingKey(nil, fh, fd), AppendContinuationKey(nil, fh, fd),
		AppendTerminalKey(nil, fh, fd), AppendAckKey(nil, fh, fd),
		AppendPayloadBuildKey(nil, fh, fd), AppendPayloadChunkKey(nil, fh, fd, testDigest("build"), 3),
		AppendRoutePinKey(nil, fh, fd),
		AppendPreparedTerminalKey(nil, fh, fd), AppendSchemaPinReleaseKey(nil, fh, fd),
	}
	for _, raw := range keys {
		view, err := OpenStorageKey(raw)
		if err != nil || view.Home != fh || view.Key != fd {
			t.Fatalf("storage key: %+v %v", view, err)
		}
		if raw[0] != StoragePrefix || !bytes.Equal(raw[1:33], fh[:]) || !bytes.Equal(raw[33:65], fd[:]) {
			t.Fatal("storage order is not prefix/home/key")
		}
	}
}

func TestHeadCommandCanonicalRoundTripZeroAlloc(t *testing.T) {
	head, _, _ := testHead(t, true)
	payload, err := AppendHead(make([]byte, 0, 2048), head)
	if err != nil {
		t.Fatal(err)
	}
	home, _ := Home(head.Key)
	command := Command{Operation: OperationCreate, Revision: 1, KeyDigest: head.KeyDigest,
		RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot, SubjectDigest: head.TerminalContractDigest,
		ExpectedRangeIdentity: testDigest("range"), Home: home, Payload: payload}
	raw, err := AppendCommand(make([]byte, 0, len(payload)+512), command)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenCommandInto(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := AppendCommand(make([]byte, 0, len(raw)), view.Command)
	if err != nil || !bytes.Equal(raw, reencoded) {
		t.Fatal("command is not byte-canonical")
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, openErr := OpenCommandInto(raw, nil); openErr != nil {
			panic(openErr)
		}
	}); allocations != 0 {
		t.Fatalf("valid OpenCommandInto allocs=%v", allocations)
	}
}

func TestPagedBatchAtomicSealAndRootMismatch(t *testing.T) {
	plan := testPlan(t, MaxPlanPageBytes+12345)
	key := testKey(false)
	kd, _ := KeyDigest(key)
	root, _ := PlanRoot(kd, plan)
	base, _, cursor := testHead(t, false)
	contract := ExecutionContract{CatalogGeneration: base.CatalogGeneration, PinID: base.PinID, PinDigest: base.PinDigest,
		RouteSchemaCertificateDigest: base.RouteSchemaCertificateDigest, MaxPendingWaveBytes: base.MaxPendingWaveBytes,
		MaxContinuationBytes: base.MaxContinuationBytes, MaxTerminalBytes: base.MaxTerminalBytes,
		TerminalTransitionTag: base.TerminalTransitionTag, FinalWaveCount: 1,
		TerminalStateDigest: NextStateDigest(base.TerminalTransitionTag, cursor), TerminalSummaryDigest: base.TerminalSummaryDigest}
	contract.AbortTerminalTransitionTag = base.AbortTerminalTransitionTag
	contract.AbortFinalWaveCount = base.AbortFinalWaveCount
	contract.AbortTerminalStateDigest = base.AbortTerminalStateDigest
	contract.PlanBuildID = testDigest("paged-plan-build")
	contract.PlanBuildGeneration = 1
	contract.PlanningLeaseExpiryIndex = math.MaxUint64
	contract.PlanningLeaseGeneration = 1
	head, err := NewPagedHeadWithExecutionContract(key, testDigest("request-body"), testDigest("terminal-contract"), uint64(len(plan)), root, contract)
	if err != nil {
		t.Fatal(err)
	}
	pages := make([]PlanPageRecord, 0, head.PlanPageCount)
	var previous Digest
	for ordinal, offset := uint64(0), 0; offset < len(plan); ordinal++ {
		end := min(offset+MaxPlanPageBytes, len(plan))
		page, e := NewPlanPageData(head, ordinal, previous, plan[offset:end])
		if e != nil {
			t.Fatal(e)
		}
		pages = append(pages, page)
		previous = page.Chain
		offset = end
	}
	batchRaw, err := AppendPlanPageBatch(nil, pages)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := OpenPlanPageBatch(batchRaw)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := AdvanceHeadPageBatch(head, batch, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	if sealed.Phase != PhaseSealed || sealed.Revision != 2 || sealed.PageChain != root {
		t.Fatal("final batch not atomically sealed")
	}
	head.PlanRoot[0] ^= 1
	if _, err = AdvanceHeadPageBatch(head, batch, 2, true); err == nil {
		t.Fatal("mismatched expected root sealed")
	}
}

func TestPlanningExpiryCleanupRestartFencesOldBuild(t *testing.T) {
	plan := testPlan(t, 2*MaxPlanPageBytes+123)
	key := testKey(false)
	keyDigest, _ := KeyDigest(key)
	root, _ := PlanRoot(keyDigest, plan)
	base, _, cursor := testHead(t, false)
	contract := ExecutionContract{
		CatalogGeneration: base.CatalogGeneration, PinID: base.PinID, PinDigest: base.PinDigest,
		RouteSchemaCertificateDigest: base.RouteSchemaCertificateDigest,
		MaxPendingWaveBytes:          base.MaxPendingWaveBytes, MaxContinuationBytes: base.MaxContinuationBytes,
		MaxTerminalBytes: base.MaxTerminalBytes, PlanBuildID: testDigest("expiring-build"),
		PlanBuildGeneration: 1, PlanningLeaseExpiryIndex: 100, PlanningLeaseGeneration: 1,
		TerminalTransitionTag: base.TerminalTransitionTag, FinalWaveCount: 1,
		TerminalStateDigest:        NextStateDigest(base.TerminalTransitionTag, cursor),
		TerminalSummaryDigest:      base.TerminalSummaryDigest,
		AbortTerminalTransitionTag: base.AbortTerminalTransitionTag,
		AbortFinalWaveCount:        1, AbortTerminalStateDigest: base.AbortTerminalStateDigest,
	}
	head, err := NewPagedHeadWithExecutionContract(key, testDigest("request-body"),
		testDigest("terminal-contract"), uint64(len(plan)), root, contract)
	if err != nil {
		t.Fatal(err)
	}
	pages := make([]PlanPageRecord, 0, 2)
	var previous Digest
	for ordinal := uint64(0); ordinal < 2; ordinal++ {
		offset := ordinal * MaxPlanPageBytes
		page, pageErr := NewPlanPageData(head, ordinal, previous,
			plan[offset:offset+MaxPlanPageBytes])
		if pageErr != nil {
			t.Fatal(pageErr)
		}
		pages = append(pages, page)
		previous = page.Chain
	}
	batchRaw, _ := AppendPlanPageBatch(nil, pages)
	batch, _ := OpenPlanPageBatch(batchRaw)
	head, err = AdvanceHeadPageBatch(head, batch, 2, false)
	if err != nil {
		t.Fatal(err)
	}
	indexRecord, err := NewPlanningExpiryIndexRecord(head)
	if err != nil {
		t.Fatal(err)
	}
	indexRaw, _ := AppendPlanningExpiryIndex(nil, indexRecord)
	openedIndex, err := OpenPlanningExpiryIndex(indexRaw)
	if err != nil || openedIndex != indexRecord {
		t.Fatalf("expiry index roundtrip: %+v %v", openedIndex, err)
	}
	home, _ := Home(key)
	firstIndexKey := AppendPlanningExpiryKey(nil, 100, home, head.KeyDigest)
	secondIndexKey := AppendPlanningExpiryKey(nil, 101, home, head.KeyDigest)
	if bytes.Compare(firstIndexKey, secondIndexKey) >= 0 {
		t.Fatal("expiry index keys are not applied-index ordered")
	}
	openedApplied, openedHome, openedKey, openKeyErr := OpenPlanningExpiryKey(firstIndexKey)
	if openKeyErr != nil || openedApplied != 100 || openedHome != home || openedKey != head.KeyDigest {
		t.Fatalf("expiry key roundtrip: %d %x %x %v", openedApplied, openedHome, openedKey, openKeyErr)
	}
	if _, err = NewPlanningExpiryRequest(head, 99); err == nil {
		t.Fatal("planning expired before its applied-index lease")
	}
	expiry, err := NewPlanningExpiryRequest(head, 100)
	if err != nil {
		t.Fatal(err)
	}
	expiryRaw, _ := AppendPlanningExpiryRequest(nil, expiry)
	if opened, openErr := OpenPlanningExpiryRequest(expiryRaw); openErr != nil || opened != expiry {
		t.Fatalf("expiry request roundtrip: %+v %v", opened, openErr)
	}
	expired, err := MarkPlanningExpired(head, expiry, 3)
	if err != nil || expired.Phase != PhaseExpired || expired.PlanBuildGeneration != 1 {
		t.Fatalf("expire: phase=%d generation=%d err=%v", expired.Phase, expired.PlanBuildGeneration, err)
	}
	if _, err = NewPlanPageData(expired, 2, previous, plan[2*MaxPlanPageBytes:]); err == nil {
		t.Fatal("expired build accepted a new page")
	}
	cleanupRequest, err := NewPlanningCleanupRequest(expired, MaxAckGCDeleteRows, math.MaxUint32)
	if err != nil {
		t.Fatal(err)
	}
	cleanupRaw, _ := AppendPlanningCleanupRequest(nil, cleanupRequest)
	if opened, openErr := OpenPlanningCleanupRequest(cleanupRaw); openErr != nil || opened != cleanupRequest {
		t.Fatalf("cleanup request roundtrip: %+v %v", opened, openErr)
	}
	chunk, err := PlanPlanningCleanup(expired, cleanupRequest)
	if err != nil || !chunk.Final || chunk.PageCount != 2 {
		t.Fatalf("cleanup plan: %+v %v", chunk, err)
	}
	cleaned, err := AdvancePlanningCleanup(expired, cleanupRequest, chunk, 4)
	if err != nil || cleaned.AppendedPageCount != 0 || cleaned.AppendedPlanBytes != 0 {
		t.Fatalf("cleanup advance: %+v %v", cleaned, err)
	}
	nextBuildID := testDigest("restarted-build")
	restart, err := NewPlanningRestartRequest(cleaned, 101, 201, nextBuildID)
	if err != nil {
		t.Fatal(err)
	}
	restartRaw, _ := AppendPlanningRestartRequest(nil, restart)
	if opened, openErr := OpenPlanningRestartRequest(restartRaw); openErr != nil || opened != restart {
		t.Fatalf("restart request roundtrip: %+v %v", opened, openErr)
	}
	restarted, err := RestartPlanning(cleaned, restart, 5)
	if err != nil || restarted.Phase != PhasePlanning || restarted.PlanBuildGeneration != 2 ||
		restarted.PlanBuildID != nextBuildID {
		t.Fatalf("restart: phase=%d generation=%d err=%v", restarted.Phase, restarted.PlanBuildGeneration, err)
	}
	if _, err = AdvanceHeadPage(restarted, pages[0], 6); err == nil {
		t.Fatal("restarted generation accepted an old build page")
	}
	if ValidatePlanningExpiryIndex(restarted, indexRecord) == nil {
		t.Fatal("restarted generation accepted the old expiry index row")
	}
	if ValidatePlanningExpiry(restarted, expiry) == nil {
		t.Fatal("restarted generation accepted the old expiry request")
	}
	if _, err = RestartPlanning(restarted, restart, 6); err == nil {
		t.Fatal("restart replay escaped the fenced planning state")
	}
	newIndex, err := NewPlanningExpiryIndexRecord(restarted)
	if err != nil || newIndex.BuildGeneration != 2 || newIndex.PlanBuildID != nextBuildID {
		t.Fatalf("new expiry index: %+v %v", newIndex, err)
	}

	rangeIdentity := testDigest("range")
	commands := []Command{
		{Operation: OperationExpirePlanning, ExpectedRevision: 2, Revision: 3,
			KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
			SubjectDigest: head.PlanBuildID, ExpectedRangeIdentity: rangeIdentity, Home: home, Payload: expiryRaw},
		{Operation: OperationCleanupPlanning, ExpectedRevision: 3, Revision: 4,
			KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
			SubjectDigest: head.PlanBuildID, ExpectedRangeIdentity: rangeIdentity, Home: home, Payload: cleanupRaw},
		{Operation: OperationRestartPlanning, ExpectedRevision: 4, Revision: 5,
			KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
			SubjectDigest: nextBuildID, ExpectedRangeIdentity: rangeIdentity, Home: home, Payload: restartRaw},
	}
	for _, command := range commands {
		raw, appendErr := AppendCommand(nil, command)
		opened, openErr := OpenCommandInto(raw, nil)
		if appendErr != nil || openErr != nil || !bytes.Equal(opened.Bytes(), raw) {
			t.Fatalf("planning op %d: append=%v open=%v", command.Operation, appendErr, openErr)
		}
	}
	if OperationRestartPlanning != 21 || OperationCleanupPlanning != 22 {
		t.Fatal("planning restart operation codes changed")
	}
	overflow := cleaned
	overflow.PlanBuildGeneration = math.MaxUint64
	if _, err = NewPlanningRestartRequest(overflow, 101, 201, nextBuildID); err == nil {
		t.Fatal("planning build generation wrapped")
	}
}

func TestPayloadBuildWinnerChunksAndDynamicIovecs(t *testing.T) {
	head, plan, _ := testHead(t, false)
	data := bytes.Repeat([]byte{0x33}, MaxPlanPageBytes+17)
	acc, _ := NewPayloadRootAccumulator(head.KeyDigest, uint64(len(data)))
	for offset := 0; offset < len(data); {
		end := min(offset+MaxPlanPageBytes, len(data))
		if err := acc.Append(data[offset:end]); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	root, _ := acc.Root()
	build, err := NewPayloadBuild(head, root, uint64(len(data)), 2)
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewPayloadBuild(head, testDigest("other-root"), uint64(len(data)), 2)
	if err != nil {
		t.Fatal(err)
	}
	if other.BuildDigest == build.BuildDigest {
		t.Fatal("alternative build did not conflict")
	}
	for offset := 0; offset < len(data); {
		end := min(offset+MaxPlanPageBytes, len(data))
		chunk, e := NewPayloadChunk(build, data[offset:end])
		if e != nil {
			t.Fatal(e)
		}
		encoded, e := AppendPayloadChunk(nil, chunk)
		if e != nil {
			t.Fatal(e)
		}
		opened, e := OpenPayloadChunk(encoded)
		if e != nil || opened.Chain != chunk.Chain {
			t.Fatal("chunk roundtrip")
		}
		build, e = AdvancePayloadBuild(build, chunk, build.Revision+1)
		if e != nil {
			t.Fatal(e)
		}
		offset = end
	}
	build, err = SealPayloadBuild(build, build.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	steps := []StepRef{{TargetSource: PayloadSourceDynamic, CommandSource: PayloadSourceDynamic, TargetOffset: 3, TargetLength: 7, CommandOffset: 64, CommandLength: 99, TargetDigest: testDigest("target"), CommandDigest: testDigest("command")}, {TargetSource: PayloadSourcePlan, CommandSource: PayloadSourceDynamic, TargetOffset: 0, TargetLength: 8, CommandOffset: 1024, CommandLength: 128, TargetDigest: testDigest("target2"), CommandDigest: testDigest("command2")}}
	if uint64(len(plan)) < 8 {
		t.Fatal("bad fixture")
	}
	routePin := testAcquiredRoutePin(t, head)
	pending, err := NewPendingWaveWithRoutePin(head, build, 2, routePin, steps)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = InstallPendingWave(head, pending, PayloadBuildRecord{}, routePin); err == nil {
		t.Fatal("dynamic pending installed without sealed build")
	}
	if _, err = InstallPendingWave(head, pending, build, routePin); err != nil {
		t.Fatal(err)
	}
}

func TestRoutePinAndPayloadCleanupCommandCanonical(t *testing.T) {
	head, _, _ := testHead(t, false)
	home, err := Home(head.Key)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := NewRoutePinAcquiring(head, PinID{3}, testDigest("route-binding"),
		testDigest("physical-route"), bytes.Repeat([]byte{1}, MaxRouteGatePinCommandBytes))
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := RecordVerifiedRoutePinAcquired(
		pin, pin.Revision+1, bytes.Repeat([]byte{2}, MaxRouteGatePinCompletionBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	releasing, err := BeginRoutePinRelease(
		acquired, acquired.Revision+1, bytes.Repeat([]byte{3}, MaxRouteGatePinCommandBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	released, err := RecordVerifiedRoutePinReleased(
		releasing, releasing.Revision+1, bytes.Repeat([]byte{4}, MaxRouteGatePinCompletionBytes),
	)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		op    Operation
		phase RoutePinPhase
		pin   RoutePinRecord
	}{
		{OperationBeginRoutePinAcquire, RoutePinAcquiring, pin},
		{OperationRecordRoutePinAcquired, RoutePinAcquired, acquired},
		{OperationBeginRoutePinRelease, RoutePinReleasing, releasing},
		{OperationRecordRoutePinReleased, RoutePinReleased, released},
	}
	for _, test := range tests {
		payload, appendErr := AppendRoutePin(nil, test.pin)
		if appendErr != nil {
			t.Fatalf("op %d append route pin: %v", test.op, appendErr)
		}
		command := Command{
			Operation: test.op, ExpectedRevision: 8, Revision: 9,
			KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
			SubjectDigest: test.pin.RecordDigest, ExpectedRangeIdentity: testDigest("range"),
			Home: home, Payload: payload,
		}
		raw, appendErr := AppendCommand(nil, command)
		if appendErr != nil {
			t.Fatalf("op %d append command: %v", test.op, appendErr)
		}
		opened, openErr := OpenCommandInto(raw, nil)
		decoded, ok := opened.RoutePin()
		if openErr != nil || !ok || decoded.Phase != test.phase ||
			decoded.RecordDigest != test.pin.RecordDigest || !bytes.Equal(opened.Bytes(), raw) {
			t.Fatalf("op %d route pin command: phase=%d ok=%v err=%v", test.op, decoded.Phase, ok, openErr)
		}
		if allocations := testing.AllocsPerRun(100, func() {
			if _, openErr = OpenCommandInto(raw, nil); openErr != nil {
				panic(openErr)
			}
		}); allocations != 0 {
			t.Fatalf("op %d OpenCommandInto allocs=%v", test.op, allocations)
		}
	}

	wrongPhasePayload, _ := AppendRoutePin(nil, pin)
	wrongPhase, _ := AppendCommand(nil, Command{
		Operation: OperationRecordRoutePinAcquired, ExpectedRevision: 8, Revision: 9,
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		SubjectDigest: pin.RecordDigest, ExpectedRangeIdentity: testDigest("range"), Home: home,
		Payload: wrongPhasePayload,
	})
	if _, err = OpenCommandInto(wrongPhase, nil); err == nil {
		t.Fatal("route-pin operation accepted the wrong durable phase")
	}

	cleanup := PayloadCleanupRequest{BuildDigest: testDigest("cleanup-build"), MaxRows: 31, MaxBytes: 1 << 20}
	cleanupPayload, err := AppendPayloadCleanupRequest(nil, cleanup)
	if err != nil {
		t.Fatal(err)
	}
	cleanupCommand := Command{
		Operation: OperationCleanupPayload, ExpectedRevision: 9, Revision: 10,
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		SubjectDigest: cleanup.BuildDigest, ExpectedRangeIdentity: testDigest("range"),
		Home: home, Payload: cleanupPayload,
	}
	raw, err := AppendCommand(nil, cleanupCommand)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenCommandInto(raw, nil)
	decodedCleanup, ok := opened.PayloadCleanup()
	if err != nil || !ok || decodedCleanup != cleanup {
		t.Fatalf("cleanup command: %+v ok=%v err=%v", decodedCleanup, ok, err)
	}
	cleanupCommand.SubjectDigest[0] ^= 1
	wrongSubject, _ := AppendCommand(nil, cleanupCommand)
	if _, err = OpenCommandInto(wrongSubject, nil); err == nil {
		t.Fatal("payload cleanup accepted a mismatched build digest")
	}

	if OperationBeginRoutePinAcquire != 13 || OperationRecordRoutePinAcquired != 14 ||
		OperationBeginRoutePinRelease != 15 || OperationRecordRoutePinReleased != 16 ||
		OperationCleanupPayload != 17 {
		t.Fatal("route-pin/payload-cleanup operation codes changed")
	}
	if _, err = NewRoutePinAcquiring(head, PinID{3}, testDigest("route-binding"),
		testDigest("physical-route"), make([]byte, MaxRouteGatePinCommandBytes+1)); err == nil {
		t.Fatal("oversized route-gate command accepted")
	}
	if _, err = RecordVerifiedRoutePinAcquired(pin, pin.Revision+1,
		make([]byte, MaxRouteGatePinCompletionBytes+1)); err == nil {
		t.Fatal("oversized route-gate completion accepted")
	}
}

func terminalFixture(t *testing.T, sequenced bool) (HeadRecord, PreparedTerminalRecord, SchemaPinReleaseRecord, TerminalRecord) {
	return terminalFixtureForKey(t, testKey(sequenced))
}

func terminalFixtureForKey(t *testing.T, key RequestKey) (HeadRecord, PreparedTerminalRecord, SchemaPinReleaseRecord, TerminalRecord) {
	t.Helper()
	head, plan, cursor := testHeadForKey(t, key)
	steps := []StepRef{{TargetSource: PayloadSourcePlan, CommandSource: PayloadSourcePlan, TargetOffset: 0, TargetLength: 8, CommandOffset: 8, CommandLength: 16, TargetDigest: testDigest("target"), CommandDigest: testDigest("command")}}
	if len(plan) < 24 {
		t.Fatal("bad plan")
	}
	routePin := testAcquiredRoutePin(t, head)
	pending, err := NewPendingWaveWithRoutePin(head, PayloadBuildRecord{}, 2, routePin, steps)
	if err != nil {
		t.Fatal(err)
	}
	head, err = InstallPendingWave(head, pending, PayloadBuildRecord{}, routePin)
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := NewContinuation(head, pending, routePin, 3, head.TerminalTransitionTag, cursor, []byte("committed-observation"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = AdvancePending(head, pending, continuation)
	if err != nil {
		t.Fatal(err)
	}
	routePin = testReleasedRoutePin(t, routePin)
	head, err = MarkRoutePinReleased(head, routePin, 4)
	if err != nil {
		t.Fatal(err)
	}
	var token AckToken
	copy(token[:], []byte("random-capability-token-000000001"))
	prepared, err := NewPreparedTerminal(head, continuation, 5, OutcomeCommitted, 12, true,
		[]byte("result"), head.TerminalSummaryDigest, token)
	if err != nil {
		t.Fatal(err)
	}
	head, err = MarkTerminalPrepared(head, continuation, prepared)
	if err != nil {
		t.Fatal(err)
	}
	release, err := NewSchemaPinRelease(head, prepared, 6, []byte("exact-schema-release-command"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = InstallSchemaPinRelease(head, prepared, release)
	if err != nil {
		t.Fatal(err)
	}
	intent := release
	release, err = RecordVerifiedSchemaPinReleased(release, 7, []byte("exact-schema-release-completion"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = MarkSchemaPinReleased(head, prepared, intent, release)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := NewTerminal(head, prepared, release, 8)
	if err != nil {
		t.Fatal(err)
	}
	return head, prepared, release, terminal
}

func gcCompleteAckForKey(t *testing.T, key RequestKey) (AckRecord, IssuerSequenceRecord) {
	t.Helper()
	head, prepared, release, terminal := terminalFixtureForKey(t, key)
	head, err := MarkTerminal(head, prepared, release, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := NewAck(head, terminal, terminal.Revision+1, 10000)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := NewIssuerSequence(key, head.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	collect, err := NewCollectRequest(ack.AckDigest, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	ack, err = AdvanceAckGC(ack, collect, ack.Revision+1, 1, ack.PriorEncodedBytes, true)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err = MarkIssuerSequenceGCComplete(sequence, ack, sequence.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	return ack, sequence
}

func TestContinuationTerminalAckTokenAndGC(t *testing.T) {
	head, prepared, release, terminal := terminalFixture(t, true)
	raw, err := AppendTerminal(nil, terminal)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenTerminal(raw)
	if err != nil || opened.AckTokenDigest != terminal.AckTokenDigest {
		t.Fatal("terminal roundtrip")
	}
	forged := append([]byte(nil), raw...)
	forged[432] ^= 1
	binary.LittleEndian.PutUint32(forged[len(forged)-4:], crc32.Checksum(forged[:len(forged)-4], castagnoli))
	if _, err = OpenTerminal(forged); err == nil {
		t.Fatal("forged token accepted")
	}
	terminalHead, err := MarkTerminal(head, prepared, release, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := NewAck(terminalHead, terminal, 9, 10000)
	if err != nil {
		t.Fatal(err)
	}
	ackRaw, err := AppendAck(nil, ack)
	if err != nil {
		t.Fatal(err)
	}
	ack, err = OpenAck(ackRaw)
	if err != nil {
		t.Fatal(err)
	}
	request := AckRequest{TerminalRevision: terminal.Revision, ResultDigest: terminal.ResultDigest, AckToken: terminal.AckToken}
	encoded, _ := AppendAckRequest(nil, request)
	if _, err = OpenAckRequest(encoded); err != nil {
		t.Fatal(err)
	}
	request.AckToken[0] ^= 1
	if AckRequestDigest(request) == AckRequestDigest(AckRequest{TerminalRevision: terminal.Revision, ResultDigest: terminal.ResultDigest, AckToken: terminal.AckToken}) {
		t.Fatal("ack token not bound")
	}
	if ack.GCPhase != AckGCCollecting || ack.ReleaseCertificateDigest != terminal.SchemaPinReleaseCertificateDigest {
		t.Fatal("ACK retained a schema pin instead of its completed release witness")
	}
	collect, err := NewCollectRequest(ack.AckDigest, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = AdvanceAckGC(ack, collect, 10, 1, math.MaxUint64-24, false); err == nil {
		t.Fatal("reclaimed byte overflow accepted")
	}
	ack, err = AdvanceAckGC(ack, collect, 10, 1, 10000, true)
	if err != nil || ack.GCPhase != AckGCComplete {
		t.Fatal("final GC")
	}
	if !SameAckGCTransition(ack, collect) {
		t.Fatal("ACK did not retain its exact GC retry witness")
	}
	changedBudget, _ := NewCollectRequest(collect.ExpectedAckDigest, 2, collect.MaxBytes)
	if SameAckGCTransition(ack, changedBudget) {
		t.Fatal("distinct GC budget matched the retained retry witness")
	}
	ackRaw, err = AppendAck(ackRaw[:0], ack)
	if err != nil {
		t.Fatal(err)
	}
	reopenedAck, err := OpenAck(ackRaw)
	if err != nil || !SameAckGCTransition(reopenedAck, collect) {
		t.Fatalf("GC retry witness did not survive reopen: %v", err)
	}
}

func TestIssuerHighwaterCanonicalAntiResurrection(t *testing.T) {
	key1 := testKey(true)
	key1.IssuerSequence = 1
	key2 := key1
	key2.IssuerSequence = 2
	key2.Request[0]++

	ack1, sequence1 := gcCompleteAckForKey(t, key1)
	ack2, sequence2 := gcCompleteAckForKey(t, key2)
	highwater, err := NewIssuerHighwater(key1)
	if err != nil {
		t.Fatal(err)
	}
	request1, err := NewIssuerAdvanceRequest(highwater, sequence1, ack1)
	if err != nil {
		t.Fatal(err)
	}
	request2, err := NewIssuerAdvanceRequest(highwater, sequence2, ack2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = AdvanceIssuerHighwater(highwater, sequence2, ack2, request2, highwater.Revision+1); err == nil {
		t.Fatal("issuer highwater skipped an unretired sequence")
	}

	highwaterRaw, err := AppendIssuerHighwater(nil, highwater)
	if err != nil {
		t.Fatal(err)
	}
	sequenceRaw, err := AppendIssuerSequence(nil, sequence1)
	if err != nil {
		t.Fatal(err)
	}
	requestRaw, err := AppendIssuerAdvanceRequest(nil, request1)
	if err != nil {
		t.Fatal(err)
	}
	openedHighwater, err := OpenIssuerHighwater(highwaterRaw)
	if err != nil || openedHighwater != highwater {
		t.Fatalf("highwater roundtrip: %v", err)
	}
	openedSequence, err := OpenIssuerSequence(sequenceRaw)
	if err != nil || openedSequence != sequence1 {
		t.Fatalf("sequence roundtrip: %v", err)
	}
	openedRequest, err := OpenIssuerAdvanceRequest(requestRaw)
	if err != nil || openedRequest != request1 {
		t.Fatalf("advance request roundtrip: %v", err)
	}

	home, _ := Home(key1)
	issuer, _ := IssuerDigest(sequence1.Identity)
	highwaterKey := AppendIssuerHighwaterKey(nil, home, issuer)
	sequenceKey1 := AppendIssuerSequenceKey(nil, home, issuer, 1)
	sequenceKey2 := AppendIssuerSequenceKey(nil, home, issuer, 2)
	openedHome, openedIssuer, err := OpenIssuerHighwaterKey(highwaterKey)
	if err != nil || openedHome != home || openedIssuer != issuer {
		t.Fatalf("highwater key roundtrip: %v", err)
	}
	openedHome, openedIssuer, openedOrdinal, err := OpenIssuerSequenceKey(sequenceKey1)
	if err != nil || openedHome != home || openedIssuer != issuer || openedOrdinal != 1 {
		t.Fatalf("sequence key roundtrip: %v", err)
	}
	if bytes.Compare(sequenceKey1, sequenceKey2) >= 0 {
		t.Fatal("issuer sequence keys are not ordinal ordered")
	}

	command := Command{Operation: OperationAdvanceIssuerHighwater,
		ExpectedRevision: highwater.Revision, Revision: highwater.Revision + 1,
		KeyDigest: ack1.KeyDigest, RequestDigest: ack1.RequestDigest, PlanRoot: ack1.PlanRoot,
		SubjectDigest: request1.ExpectedHighwaterDigest, ExpectedRangeIdentity: testDigest("range"),
		Home: home, Payload: requestRaw}
	commandRaw, err := AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	openedCommand, err := OpenCommandInto(commandRaw, nil)
	if err != nil {
		t.Fatal(err)
	}
	openedAdvance, ok := openedCommand.IssuerAdvance()
	if !ok || openedAdvance != request1 {
		t.Fatal("issuer advance command accessor")
	}
	if OperationAdvanceIssuerHighwater != 23 || LastOperation != OperationAdvanceIssuerHighwater {
		t.Fatal("issuer highwater operation code changed")
	}
	wrongSubject := command
	wrongSubject.SubjectDigest[0] ^= 1
	wrongSubjectRaw, _ := AppendCommand(nil, wrongSubject)
	if _, err = OpenCommandInto(wrongSubjectRaw, nil); err == nil {
		t.Fatal("issuer advance accepted a mismatched highwater CAS digest")
	}
	for name, open := range map[string]func(){
		"highwater": func() { _, _ = OpenIssuerHighwater(highwaterRaw) },
		"sequence":  func() { _, _ = OpenIssuerSequence(sequenceRaw) },
		"request":   func() { _, _ = OpenIssuerAdvanceRequest(requestRaw) },
		"command":   func() { _, _ = OpenCommandInto(commandRaw, nil) },
	} {
		if allocations := testing.AllocsPerRun(1000, open); allocations != 0 {
			t.Fatalf("%s open allocs=%v", name, allocations)
		}
	}

	highwater, err = AdvanceIssuerHighwater(highwater, sequence1, ack1, request1, highwater.Revision+1)
	if err != nil || !SameIssuerAdvance(highwater, request1) {
		t.Fatalf("advance sequence 1: %v", err)
	}
	if !IssuerHighwaterCoversKey(highwater, key1) || !IssuerHighwaterCoversAck(highwater, ack1) {
		t.Fatal("retired sequence was not covered")
	}
	reused := key1
	reused.Request[0]++
	if !IssuerHighwaterCoversKey(highwater, reused) {
		t.Fatal("same issuer sequence with a different request ID resurrected")
	}
	if IssuerHighwaterCoversKey(highwater, key2) || IssuerHighwaterCoversKey(highwater, testKey(false)) {
		t.Fatal("highwater covered a future or arbitrary-ID request")
	}

	request2, err = NewIssuerAdvanceRequest(highwater, sequence2, ack2)
	if err != nil {
		t.Fatal(err)
	}
	highwater, err = AdvanceIssuerHighwater(highwater, sequence2, ack2, request2, highwater.Revision+1)
	if err != nil || highwater.HighwaterSequence != 2 {
		t.Fatalf("advance sequence 2: %v", err)
	}
	highwaterRaw, err = AppendIssuerHighwater(highwaterRaw[:0], highwater)
	if err != nil {
		t.Fatal(err)
	}
	highwater, err = OpenIssuerHighwater(highwaterRaw)
	if err != nil || !IssuerHighwaterCoversKey(highwater, reused) {
		t.Fatalf("anti-resurrection witness did not survive reopen: %v", err)
	}
	forged := append([]byte(nil), highwaterRaw...)
	forged[192] ^= 1
	binary.LittleEndian.PutUint32(forged[len(forged)-4:], crc32.Checksum(forged[:len(forged)-4], castagnoli))
	if _, err = OpenIssuerHighwater(forged); err == nil {
		t.Fatal("forged highwater accepted under recomputed checksum")
	}
	if _, err = NewIssuerHighwater(testKey(false)); err == nil {
		t.Fatal("arbitrary-ID request installed an issuer highwater")
	}
}

func TestPreparedTerminalSchemaReleaseAndCompleteCanonical(t *testing.T) {
	head, prepared, released, terminal := terminalFixture(t, false)
	preparedRaw, err := AppendPreparedTerminal(nil, prepared)
	if err != nil {
		t.Fatal(err)
	}
	openedPrepared, err := OpenPreparedTerminal(preparedRaw)
	if err != nil || openedPrepared.PreparedDigest != prepared.PreparedDigest ||
		openedPrepared.AckToken != prepared.AckToken || !bytes.Equal(openedPrepared.Result, prepared.Result) {
		t.Fatalf("prepared roundtrip: digest=%x err=%v", openedPrepared.PreparedDigest, err)
	}
	forgedPrepared := append([]byte(nil), preparedRaw...)
	forgedPrepared[preparedTerminalHeaderBytes] ^= 1
	binary.LittleEndian.PutUint32(forgedPrepared[len(forgedPrepared)-4:],
		crc32.Checksum(forgedPrepared[:len(forgedPrepared)-4], castagnoli))
	if _, err = OpenPreparedTerminal(forgedPrepared); err == nil {
		t.Fatal("prepared result changed under a recomputed checksum")
	}

	releasedRaw, err := AppendSchemaPinRelease(nil, released)
	if err != nil {
		t.Fatal(err)
	}
	openedRelease, err := OpenSchemaPinRelease(releasedRaw)
	if err != nil || openedRelease.CertificateDigest != released.CertificateDigest ||
		!bytes.Equal(openedRelease.Command, released.Command) ||
		!bytes.Equal(openedRelease.Completion, released.Completion) {
		t.Fatalf("schema release roundtrip: phase=%d err=%v", openedRelease.Phase, err)
	}
	forgedRelease := append([]byte(nil), releasedRaw...)
	forgedRelease[len(forgedRelease)-checksumBytes-1] ^= 1
	binary.LittleEndian.PutUint32(forgedRelease[len(forgedRelease)-4:],
		crc32.Checksum(forgedRelease[:len(forgedRelease)-4], castagnoli))
	if _, err = OpenSchemaPinRelease(forgedRelease); err == nil {
		t.Fatal("schema release evidence changed under a recomputed checksum")
	}

	preparedHead := head
	preparedHead.Revision = prepared.Revision
	preparedHead.SchemaPinReleaseCertificateDigest = Digest{}
	if err = validateHead(preparedHead); err != nil {
		t.Fatal(err)
	}
	preparedHeadRaw, err := AppendHead(nil, preparedHead)
	if err != nil {
		t.Fatal(err)
	}
	openedPreparedHead, err := OpenHead(preparedHeadRaw)
	if err != nil || openedPreparedHead.Phase != PhasePrepared ||
		openedPreparedHead.PreparedTerminalDigest != prepared.PreparedDigest {
		t.Fatalf("prepared head roundtrip: phase=%d err=%v", openedPreparedHead.Phase, err)
	}
	intent, err := NewSchemaPinRelease(preparedHead, prepared, 6, []byte("exact-schema-release-command"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewTerminal(preparedHead, prepared, intent, 7); err == nil {
		t.Fatal("terminal published before authenticated schema release completion")
	}

	home, _ := Home(preparedHead.Key)
	rangeIdentity := testDigest("range")
	intentRaw, _ := AppendSchemaPinRelease(nil, intent)
	terminalRaw, _ := AppendTerminal(nil, terminal)
	tests := []struct {
		op       Operation
		expected uint64
		revision uint64
		subject  Digest
		payload  []byte
	}{
		{OperationPrepareTerminal, 4, 5, prepared.PreparedDigest, preparedRaw},
		{OperationBeginSchemaPinRelease, 5, 6, intent.RecordDigest, intentRaw},
		{OperationRecordSchemaPinReleased, 6, 7, released.RecordDigest, releasedRaw},
		{OperationComplete, 7, 8, terminal.ResultDigest, terminalRaw},
	}
	for _, test := range tests {
		command := Command{Operation: test.op, ExpectedRevision: test.expected, Revision: test.revision,
			KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
			SubjectDigest: test.subject, ExpectedRangeIdentity: rangeIdentity, Home: home, Payload: test.payload}
		raw, appendErr := AppendCommand(nil, command)
		if appendErr != nil {
			t.Fatalf("op %d append: %v", test.op, appendErr)
		}
		opened, openErr := OpenCommandInto(raw, nil)
		if openErr != nil || !bytes.Equal(opened.Bytes(), raw) {
			t.Fatalf("op %d open: %v", test.op, openErr)
		}
		reencoded, reencodeErr := AppendCommand(nil, opened.Command)
		if reencodeErr != nil || !bytes.Equal(reencoded, raw) {
			t.Fatalf("op %d is not byte-canonical: %v", test.op, reencodeErr)
		}
		if allocations := testing.AllocsPerRun(100, func() {
			if _, openErr = OpenCommandInto(raw, nil); openErr != nil {
				panic(openErr)
			}
		}); allocations != 0 {
			t.Fatalf("op %d OpenCommandInto allocs=%v", test.op, allocations)
		}
		switch test.op {
		case OperationPrepareTerminal:
			value, ok := opened.PreparedTerminal()
			if !ok || value.PreparedDigest != prepared.PreparedDigest {
				t.Fatal("prepared accessor")
			}
		case OperationBeginSchemaPinRelease, OperationRecordSchemaPinReleased:
			value, ok := opened.SchemaPinRelease()
			if !ok || value.RecordDigest != test.subject {
				t.Fatal("schema release accessor")
			}
		}
	}

	wrongPhase, _ := AppendCommand(nil, Command{
		Operation: OperationBeginSchemaPinRelease, ExpectedRevision: 6, Revision: 7,
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		SubjectDigest: released.RecordDigest, ExpectedRangeIdentity: rangeIdentity, Home: home,
		Payload: releasedRaw,
	})
	if _, err = OpenCommandInto(wrongPhase, nil); err == nil {
		t.Fatal("schema release operation accepted the wrong phase")
	}
	installedHead, err := InstallSchemaPinRelease(preparedHead, prepared, intent)
	if err != nil {
		t.Fatal(err)
	}
	otherIntent, err := NewSchemaPinRelease(preparedHead, prepared, 6, []byte("different-release-command"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = MarkSchemaPinReleased(installedHead, prepared, otherIntent, released); err == nil {
		t.Fatal("released certificate accepted a different durable intent")
	}
	forgedTerminal := terminal
	forgedTerminal.Result = []byte("different-result")
	forgedTerminal.ResultDigest = ResultDigest(forgedTerminal.Result)
	forgedTerminal.TerminalSummaryDigest = terminalSummaryDigest(forgedTerminal)
	if err = validateTerminal(forgedTerminal); err != nil {
		t.Fatal(err)
	}
	if _, err = MarkTerminal(head, prepared, released, forgedTerminal); err == nil {
		t.Fatal("Complete accepted a result different from the prepared candidate")
	}
	if OperationPrepareTerminal != 18 || OperationBeginSchemaPinRelease != 19 ||
		OperationRecordSchemaPinReleased != 20 {
		t.Fatal("prepared-terminal operation codes changed")
	}

	tooLarge := prepared
	tooLarge.Result = make([]byte, MaxPreparedTerminalResultBytes+1)
	tooLarge.ResultDigest = ResultDigest(tooLarge.Result)
	tooLarge.PreparedDigest = preparedTerminalDigest(tooLarge)
	if _, err = AppendPreparedTerminal(nil, tooLarge); err == nil {
		t.Fatal("prepared result larger than publishable terminal accepted")
	}
}

func TestRevisionMatrixAndSemanticsSentinels(t *testing.T) {
	head, _, _ := testHead(t, false)
	home, _ := Home(head.Key)
	base := Command{KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot, SubjectDigest: testDigest("subject"), ExpectedRangeIdentity: testDigest("range"), Home: home, Payload: []byte{1}}
	for op := OperationCreate; op <= LastOperation; op++ {
		c := base
		c.Operation = op
		if op == OperationCreate || op == OperationBeginPayloadBuild {
			c.ExpectedRevision, c.Revision = 0, 1
		} else {
			c.ExpectedRevision, c.Revision = 8, 9
		}
		if err := validateCommandShape(c); err != nil {
			t.Fatalf("op %d valid: %v", op, err)
		}
		for _, pair := range [][2]uint64{{9, 9}, {9, 11}, {10, 9}, {math.MaxUint64, 0}} {
			c.ExpectedRevision, c.Revision = pair[0], pair[1]
			if err := validateCommandShape(c); err == nil {
				t.Fatalf("op %d accepted revisions %v", op, pair)
			}
		}
	}
	baseDigest := SemanticsDigest()
	if baseDigest == (Digest{}) {
		t.Fatal("zero semantics")
	}
	_, semanticSlots := semanticsDigestWithPerturbAndCount(-1, 0)
	for i := 0; i < semanticSlots; i++ {
		if semanticsDigestWithPerturb(i, 1) == baseDigest {
			t.Fatalf("semantic slot %d unbound", i)
		}
	}
}

func TestUsageAndReservationOverflow(t *testing.T) {
	u := Usage{HeadBytes: math.MaxUint64, ContinuationBytes: 1}
	if _, err := u.DurableBytes(); err == nil {
		t.Fatal("usage wrap")
	}
	head, _, _ := testHead(t, false)
	resident, future, err := Reservation(head)
	minimumLifecycle := uint64(RoutePinReservationBytes + SchemaPinReleaseReservationBytes +
		ReadyReservationBytes + FixedStorageKeyBytes + AckRecordBytes)
	if err != nil || resident == 0 || future < minimumLifecycle {
		t.Fatalf("reservation %d %d %v", resident, future, err)
	}
	sequenced, _, _ := testHead(t, true)
	_, sequencedFuture, err := Reservation(sequenced)
	if err != nil || sequencedFuture != future+IssuerSequenceReservationBytes {
		t.Fatalf("sequenced reservation=%d base=%d: %v", sequencedFuture, future, err)
	}
	u = Usage{IssuerBytes: math.MaxUint64, AckBytes: 1}
	if _, err = u.DurableBytes(); err == nil {
		t.Fatal("issuer usage wrap")
	}
}

func FuzzOpenCommand(f *testing.F) {
	_, _, _ = testHead(f, false)
	f.Add([]byte("bad"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var scratch [MaxPendingWaveSteps]StepRef
		_, _ = OpenCommandInto(raw, scratch[:])
	})
}

func FuzzOpenIssuerHighwater(f *testing.F) {
	record, _ := NewIssuerHighwater(testKey(true))
	raw, _ := AppendIssuerHighwater(nil, record)
	f.Add(raw)
	f.Add([]byte("bad"))
	f.Fuzz(func(t *testing.T, candidate []byte) { _, _ = OpenIssuerHighwater(candidate) })
}

func FuzzOpenIssuerSequence(f *testing.F) {
	record, _ := NewIssuerSequence(testKey(true), testDigest("request-body"))
	raw, _ := AppendIssuerSequence(nil, record)
	f.Add(raw)
	f.Add([]byte("bad"))
	f.Fuzz(func(t *testing.T, candidate []byte) { _, _ = OpenIssuerSequence(candidate) })
}

func FuzzOpenIssuerAdvanceRequest(f *testing.F) {
	record := IssuerAdvanceRequest{Sequence: 1, IssuerDigest: testDigest("issuer"),
		ExpectedHighwaterDigest: testDigest("highwater"), SequenceRecordDigest: testDigest("sequence"),
		AckDigest: testDigest("ack")}
	raw, _ := AppendIssuerAdvanceRequest(nil, record)
	f.Add(raw)
	f.Add([]byte("bad"))
	f.Fuzz(func(t *testing.T, candidate []byte) { _, _ = OpenIssuerAdvanceRequest(candidate) })
}

func BenchmarkOpenHead(b *testing.B) {
	head, _, _ := testHead(b, false)
	raw, _ := AppendHead(make([]byte, 0, 2048), head)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := OpenHead(raw); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkOpenCommand(b *testing.B) {
	head, _, _ := testHead(b, false)
	payload, _ := AppendHead(make([]byte, 0, 2048), head)
	home, _ := Home(head.Key)
	raw, _ := AppendCommand(make([]byte, 0, 4096), Command{Operation: OperationCreate, Revision: 1, KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot, SubjectDigest: head.TerminalContractDigest, ExpectedRangeIdentity: testDigest("range"), Home: home, Payload: payload})
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := OpenCommandInto(raw, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenIssuerHighwater(b *testing.B) {
	record, _ := NewIssuerHighwater(testKey(true))
	raw, _ := AppendIssuerHighwater(nil, record)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := OpenIssuerHighwater(raw); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenIssuerSequence(b *testing.B) {
	record, _ := NewIssuerSequence(testKey(true), testDigest("request-body"))
	raw, _ := AppendIssuerSequence(nil, record)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := OpenIssuerSequence(raw); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOpenIssuerAdvanceRequest(b *testing.B) {
	record := IssuerAdvanceRequest{Sequence: 1, IssuerDigest: testDigest("issuer"),
		ExpectedHighwaterDigest: testDigest("highwater"), SequenceRecordDigest: testDigest("sequence"),
		AckDigest: testDigest("ack")}
	raw, _ := AppendIssuerAdvanceRequest(nil, record)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := OpenIssuerAdvanceRequest(raw); err != nil {
			b.Fatal(err)
		}
	}
}
