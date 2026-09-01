package requestledger

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func acquiredPendingFixture(t testing.TB) (
	HeadRecord,
	RoutePinRecord,
	RoutePinRecord,
	PendingWaveRecord,
	HeadRecord,
) {
	t.Helper()
	head, _, _ := testHead(t, false)
	acquiring, err := NewRoutePinAcquiring(
		head, PinID{2}, testDigest("route-binding"), testDigest("physical-route"),
		[]byte("exact-acquire-command"),
	)
	if err != nil {
		t.Fatal(err)
	}
	intentHead, err := AdvanceHeadRoutePin(
		head, RoutePinRecord{}, acquiring, head.Revision+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := RecordVerifiedRoutePinAcquired(
		acquiring, acquiring.Revision+1, []byte("exact-acquire-completion"),
	)
	if err != nil {
		t.Fatal(err)
	}
	acquiredHead, err := AdvanceHeadRoutePin(
		intentHead, acquiring, acquired, intentHead.Revision+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	step := StepRef{
		TargetSource: PayloadSourcePlan, CommandSource: PayloadSourcePlan,
		TargetOffset: 0, TargetLength: 8, CommandOffset: 8, CommandLength: 16,
		TargetDigest: testDigest("target"), CommandDigest: testDigest("command"),
	}
	pending, err := NewPendingWaveWithRoutePin(
		acquiredHead, PayloadBuildRecord{}, acquiredHead.Revision+1, acquired,
		[]StepRef{step},
	)
	if err != nil {
		t.Fatal(err)
	}
	finalHead, err := InstallPendingWave(
		acquiredHead, pending, PayloadBuildRecord{}, acquired,
	)
	if err != nil {
		t.Fatal(err)
	}
	return intentHead, acquiring, acquired, pending, finalHead
}

func TestAcquiredPendingCanonicalRoundTripAndLegacyEquivalence(t *testing.T) {
	intentHead, _, acquired, pending, legacyFinal := acquiredPendingFixture(t)
	raw, err := AppendAcquiredPending(nil, acquired, pending)
	if err != nil {
		t.Fatal(err)
	}
	scratch := make([]StepRef, MaxPendingWaveSteps)
	opened, err := OpenAcquiredPendingInto(raw, scratch)
	if err != nil {
		t.Fatal(err)
	}
	routeRaw, _ := AppendRoutePin(nil, acquired)
	pendingRaw, _ := AppendPendingWave(nil, pending)
	if !bytes.Equal(opened.Bytes(), raw) || !bytes.Equal(opened.RouteBytes(), routeRaw) ||
		!bytes.Equal(opened.PendingBytes(), pendingRaw) || opened.Route().RecordDigest != acquired.RecordDigest ||
		opened.Pending().Record().WaveDigest != pending.WaveDigest {
		t.Fatal("compound record did not preserve the exact legacy row bytes")
	}
	acquiredHead, err := AdvanceHeadRoutePin(
		intentHead, acquiredPendingAcquiring(t, acquired), acquired, intentHead.Revision+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	fusedFinal, err := InstallPendingWave(
		acquiredHead, opened.Pending().Record(), PayloadBuildRecord{}, opened.Route(),
	)
	fusedRaw, fusedErr := AppendHead(nil, fusedFinal)
	legacyRaw, legacyErr := AppendHead(nil, legacyFinal)
	if err != nil || fusedErr != nil || legacyErr != nil || !bytes.Equal(fusedRaw, legacyRaw) {
		t.Fatalf("fused final head differs from legacy transitions: err=%v", err)
	}
	if err = ValidateAcquiredPendingBytes(raw); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if validateErr := ValidateAcquiredPendingBytes(raw); validateErr != nil {
			panic(validateErr)
		}
	}); allocations != 0 {
		t.Fatalf("ValidateAcquiredPendingBytes allocations=%v", allocations)
	}
}

func acquiredPendingAcquiring(t testing.TB, acquired RoutePinRecord) RoutePinRecord {
	t.Helper()
	head, _, _ := testHead(t, false)
	acquiring, err := NewRoutePinAcquiring(
		head, acquired.PinID, acquired.BindingDigest, acquired.PhysicalWitnessDigest,
		acquired.Command,
	)
	if err != nil || acquiring.RecordDigest != acquired.PriorRecordDigest {
		t.Fatalf("rebuild acquiring record: digest=%x want=%x err=%v",
			acquiring.RecordDigest, acquired.PriorRecordDigest, err)
	}
	return acquiring
}

func TestAcquiredPendingCommandAndCorruptionAdmission(t *testing.T) {
	intentHead, _, acquired, pending, _ := acquiredPendingFixture(t)
	payload, err := AppendAcquiredPending(nil, acquired, pending)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := AcquiredPendingDigest(acquired, pending)
	if err != nil {
		t.Fatal(err)
	}
	home, _ := Home(intentHead.Key)
	command := Command{
		Operation:        OperationRecordRoutePinAcquiredPutPending,
		ExpectedRevision: intentHead.Revision, Revision: pending.Revision,
		KeyDigest: intentHead.KeyDigest, RequestDigest: intentHead.RequestDigest,
		PlanRoot: intentHead.PlanRoot, SubjectDigest: subject,
		ExpectedRangeIdentity: testDigest("range"), Home: home, Payload: payload,
	}
	commandRaw, err := AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenCommandInto(commandRaw, nil); err != nil {
		t.Fatalf("validation-only open: %v", err)
	}
	scratch := make([]StepRef, MaxPendingWaveSteps)
	opened, err := OpenCommandInto(commandRaw, scratch)
	compound, ok := opened.AcquiredPending()
	if err != nil || !ok || compound.Route().RecordDigest != acquired.RecordDigest ||
		compound.Pending().Digest() != pending.WaveDigest {
		t.Fatalf("compound command open: ok=%v err=%v", ok, err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, openErr := OpenCommandInto(commandRaw, nil); openErr != nil {
			panic(openErr)
		}
	}); allocations != 0 {
		t.Fatalf("validation-only command open allocations=%v", allocations)
	}

	corrupt := bytes.Clone(payload)
	corrupt[len(corrupt)/2] ^= 1
	if err = ValidateAcquiredPendingBytes(corrupt); err == nil {
		t.Fatal("accepted a corrupted compound checksum")
	}
	for _, truncated := range [][]byte{payload[:len(payload)-1], payload[:acquiredPendingHeaderBytes]} {
		if err = ValidateAcquiredPendingBytes(truncated); err == nil {
			t.Fatal("accepted a truncated compound record")
		}
	}

	otherHead, _, _ := testHeadForKey(t, func() RequestKey {
		key := testKey(false)
		key.Request[0]++
		return key
	}())
	other := testAcquiredRoutePin(t, otherHead)
	forged := make([]byte, acquiredPendingHeaderBytes)
	copy(forged[:4], acquiredPendingMagic[:])
	forged, _ = AppendRoutePin(forged, other)
	pendingStart := len(forged)
	forged, _ = AppendPendingWave(forged, pending)
	binary.LittleEndian.PutUint32(forged[8:12], uint32(pendingStart-acquiredPendingHeaderBytes))
	binary.LittleEndian.PutUint32(forged[12:16], uint32(len(forged)-pendingStart))
	forged = appendChecksum(forged, 0)
	if err = ValidateAcquiredPendingBytes(forged); err == nil {
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

func TestAcquiredPendingMaximumBound(t *testing.T) {
	intentHead, _, acquired, _, _ := acquiredPendingFixture(t)
	acquiring := acquiredPendingAcquiring(t, acquired)
	acquiredHead, err := AdvanceHeadRoutePin(
		intentHead, acquiring, acquired, intentHead.Revision+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	steps := make([]StepRef, MaxPendingWaveSteps)
	for index := range steps {
		steps[index] = StepRef{
			TargetSource: PayloadSourcePlan, CommandSource: PayloadSourcePlan,
			TargetOffset: 0, TargetLength: 8, CommandOffset: 8, CommandLength: 16,
			TargetDigest: testDigest("max-target"), CommandDigest: testDigest("max-command"),
		}
	}
	pending, err := NewPendingWaveWithRoutePin(
		acquiredHead, PayloadBuildRecord{}, acquiredHead.Revision+1, acquired, steps,
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendAcquiredPending(nil, acquired, pending)
	if err != nil || len(raw) > MaxAcquiredPendingRecordBytes || len(raw) > MaxLifecyclePayloadBytes {
		t.Fatalf("max compound bytes=%d bound=%d lifecycle=%d err=%v",
			len(raw), MaxAcquiredPendingRecordBytes, MaxLifecyclePayloadBytes, err)
	}
	if _, err = OpenAcquiredPendingInto(raw, make([]StepRef, MaxPendingWaveSteps)); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenAcquiredPendingInto(raw, make([]StepRef, MaxPendingWaveSteps-1)); err == nil {
		t.Fatal("accepted insufficient caller-owned step scratch")
	}
}
