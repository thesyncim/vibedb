package requestledger

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func advanceReleaseFixture(t testing.TB) (
	HeadRecord,
	PendingWaveRecord,
	RoutePinRecord,
	ContinuationRecord,
	RoutePinRecord,
	HeadRecord,
) {
	t.Helper()
	_, _, acquired, pending, pendingHead := acquiredPendingFixture(t)
	continuation, err := NewContinuation(
		pendingHead, pending, acquired, pendingHead.Revision+1, 7,
		[]byte("next-cursor"), []byte("settled-observation"),
	)
	if err != nil {
		t.Fatal(err)
	}
	releasing, err := BeginRoutePinRelease(
		acquired, acquired.Revision+1, []byte("exact-release-command"),
	)
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := AdvancePending(pendingHead, pending, continuation)
	if err != nil {
		t.Fatal(err)
	}
	finalHead, err := AdvanceHeadRoutePin(
		advanced, acquired, releasing, advanced.Revision+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	return pendingHead, pending, acquired, continuation, releasing, finalHead
}

func TestAdvanceReleaseCanonicalRoundTripAndLegacyEquivalence(t *testing.T) {
	pendingHead, pending, acquired, continuation, releasing, legacyFinal := advanceReleaseFixture(t)
	raw, err := AppendAdvanceRelease(nil, continuation, releasing)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenAdvanceRelease(raw)
	if err != nil {
		t.Fatal(err)
	}
	continuationRaw, _ := AppendContinuation(nil, continuation)
	routeRaw, _ := AppendRoutePin(nil, releasing)
	if !bytes.Equal(opened.Bytes(), raw) ||
		!bytes.Equal(opened.ContinuationBytes(), continuationRaw) ||
		!bytes.Equal(opened.RouteBytes(), routeRaw) ||
		opened.Continuation().ContinuationDigest != continuation.ContinuationDigest ||
		opened.Route().RecordDigest != releasing.RecordDigest {
		t.Fatal("compound record did not preserve the exact legacy row bytes")
	}
	advanced, err := AdvancePending(pendingHead, pending, opened.Continuation())
	if err != nil {
		t.Fatal(err)
	}
	fusedFinal, err := AdvanceHeadRoutePin(
		advanced, acquired, opened.Route(), advanced.Revision+1,
	)
	fusedRaw, fusedErr := AppendHead(nil, fusedFinal)
	legacyRaw, legacyErr := AppendHead(nil, legacyFinal)
	if err != nil || fusedErr != nil || legacyErr != nil || !bytes.Equal(fusedRaw, legacyRaw) {
		t.Fatalf("fused final head differs from legacy transitions: err=%v", err)
	}
	if err = ValidateAdvanceReleaseBytes(raw); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if validateErr := ValidateAdvanceReleaseBytes(raw); validateErr != nil {
			panic(validateErr)
		}
	}); allocations != 0 {
		t.Fatalf("ValidateAdvanceReleaseBytes allocations=%v", allocations)
	}
}

func TestAdvanceReleaseCommandAndCorruptionAdmission(t *testing.T) {
	pendingHead, _, _, continuation, releasing, finalHead := advanceReleaseFixture(t)
	payload, err := AppendAdvanceRelease(nil, continuation, releasing)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := AdvanceReleaseDigest(continuation, releasing)
	if err != nil {
		t.Fatal(err)
	}
	home, _ := Home(pendingHead.Key)
	command := Command{
		Operation:        OperationAdvanceBeginRoutePinRelease,
		ExpectedRevision: pendingHead.Revision, Revision: finalHead.Revision,
		KeyDigest: pendingHead.KeyDigest, RequestDigest: pendingHead.RequestDigest,
		PlanRoot: pendingHead.PlanRoot, SubjectDigest: subject,
		ExpectedRangeIdentity: testDigest("range"), Home: home, Payload: payload,
	}
	commandRaw, err := AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	openedCommand, err := OpenCommandInto(commandRaw, nil)
	compound, ok := openedCommand.AdvanceRelease()
	if err != nil || !ok ||
		compound.Continuation().ContinuationDigest != continuation.ContinuationDigest ||
		compound.Route().RecordDigest != releasing.RecordDigest {
		t.Fatalf("compound command open: ok=%v err=%v", ok, err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, openErr := OpenCommandInto(commandRaw, nil); openErr != nil {
			panic(openErr)
		}
	}); allocations != 0 {
		t.Fatalf("command open allocations=%v", allocations)
	}

	corrupt := bytes.Clone(payload)
	corrupt[len(corrupt)/2] ^= 1
	if err = ValidateAdvanceReleaseBytes(corrupt); err == nil {
		t.Fatal("accepted a corrupted compound checksum")
	}
	for _, truncated := range [][]byte{payload[:len(payload)-1], payload[:advanceReleaseHeaderBytes]} {
		if err = ValidateAdvanceReleaseBytes(truncated); err == nil {
			t.Fatal("accepted a truncated compound record")
		}
	}

	otherHead, _, _ := testHeadForKey(t, func() RequestKey {
		key := testKey(false)
		key.Request[0]++
		return key
	}())
	otherRoute := testAcquiredRoutePin(t, otherHead)
	otherReleasing, err := BeginRoutePinRelease(
		otherRoute, otherRoute.Revision+1, []byte("other-release-command"),
	)
	if err != nil {
		t.Fatal(err)
	}
	forged := make([]byte, advanceReleaseHeaderBytes)
	copy(forged[:4], advanceReleaseMagic[:])
	forged, _ = AppendContinuation(forged, continuation)
	routeStart := len(forged)
	forged, _ = AppendRoutePin(forged, otherReleasing)
	binary.LittleEndian.PutUint32(forged[8:12], uint32(routeStart-advanceReleaseHeaderBytes))
	binary.LittleEndian.PutUint32(forged[12:16], uint32(len(forged)-routeStart))
	forged = appendChecksum(forged, 0)
	if err = ValidateAdvanceReleaseBytes(forged); err == nil {
		t.Fatal("accepted valid nested records from different requests")
	}

	wrongSubject := command
	wrongSubject.SubjectDigest[0] ^= 1
	wrongRaw, err := AppendCommand(nil, wrongSubject)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenCommandInto(wrongRaw, nil); err == nil {
		t.Fatal("accepted a mismatched compound subject digest")
	}
}

func TestAdvanceReleaseMaximumBound(t *testing.T) {
	_, _, acquired, continuation, _, _ := advanceReleaseFixture(t)
	continuation.Cursor = bytes.Repeat([]byte{1}, MaxContinuationCursorBytes)
	continuation.Observation = bytes.Repeat([]byte{2}, MaxContinuationObservationBytes)
	continuation.ObservationDigest = ObservationDigest(continuation.Observation)
	continuation.NextStateDigest = NextStateDigest(continuation.TransitionTag, continuation.Cursor)
	continuation.ContinuationDigest = continuationDigest(continuation)
	releasing, err := BeginRoutePinRelease(
		acquired, acquired.Revision+1, bytes.Repeat([]byte{3}, MaxRouteGatePinCommandBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendAdvanceRelease(nil, continuation, releasing)
	if err != nil || len(raw) > MaxAdvanceReleaseRecordBytes || len(raw) > MaxLifecyclePayloadBytes {
		t.Fatalf("max compound bytes=%d bound=%d lifecycle=%d err=%v",
			len(raw), MaxAdvanceReleaseRecordBytes, MaxLifecyclePayloadBytes, err)
	}
	if _, err = OpenAdvanceRelease(raw); err != nil {
		t.Fatal(err)
	}
}
