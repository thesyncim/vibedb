package requestledger

import "testing"

func TestMaterializedCreateExactRetryIsAllocationFree(t *testing.T) {
	plan := testPlan(t, MaxInlinePlanBytes+1)
	key := testKey(true)
	keyDigest, _ := KeyDigest(key)
	root, _ := PlanRoot(keyDigest, plan)
	base, _, _ := testHead(t, true)
	contract := ExecutionContract{
		CatalogGeneration: base.CatalogGeneration, PinID: base.PinID, PinDigest: base.PinDigest,
		RouteSchemaCertificateDigest: base.RouteSchemaCertificateDigest,
		MaxPendingWaveBytes:          base.MaxPendingWaveBytes, MaxContinuationBytes: base.MaxContinuationBytes,
		MaxTerminalBytes: base.MaxTerminalBytes, PlanBuildID: root, PlanBuildGeneration: 1,
		PlanningLeaseSpan: 37, PlanningLeaseGeneration: 1,
		TerminalTransitionTag: base.TerminalTransitionTag, FinalWaveCount: base.FinalWaveCount,
		TerminalStateDigest: base.TerminalStateDigest, TerminalSummaryDigest: base.TerminalSummaryDigest,
		AbortTerminalTransitionTag: base.AbortTerminalTransitionTag,
		AbortFinalWaveCount:        base.AbortFinalWaveCount, AbortTerminalStateDigest: base.AbortTerminalStateDigest,
	}
	template, err := NewPagedHeadWithExecutionContract(
		key, testDigest("create-request"), testDigest("create-terminal"), uint64(len(plan)), root, contract,
	)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := MaterializeCreate(template, 11)
	if err != nil || persisted.PlanningLeaseExpiryIndex != 48 {
		t.Fatalf("materialize expiry=%d: %v", persisted.PlanningLeaseExpiryIndex, err)
	}
	templateRaw, _ := AppendHead(nil, template)
	persistedRaw, _ := AppendHead(nil, persisted)
	if !SameCreateBytes(persistedRaw, templateRaw) {
		t.Fatal("exact materialized Create was not recognized")
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if !SameCreateBytes(persistedRaw, templateRaw) {
			panic("exact retry mismatch")
		}
	}); allocations != 0 {
		t.Fatalf("SameCreateBytes allocations = %v", allocations)
	}
	other := template
	other.PlanningLeaseSpan++
	otherRaw, _ := AppendHead(nil, other)
	if SameCreateBytes(persistedRaw, otherRaw) {
		t.Fatal("changed immutable planning span accepted")
	}
}

func BenchmarkSameCreateBytes(b *testing.B) {
	plan := testPlan(b, MaxInlinePlanBytes+1)
	key := testKey(true)
	keyDigest, _ := KeyDigest(key)
	root, _ := PlanRoot(keyDigest, plan)
	base, _, _ := testHead(b, true)
	contract := defaultExecutionContract(root)
	contract.CatalogGeneration, contract.PinID, contract.PinDigest = base.CatalogGeneration, base.PinID, base.PinDigest
	contract.RouteSchemaCertificateDigest = base.RouteSchemaCertificateDigest
	template, _ := NewPagedHeadWithExecutionContract(key, testDigest("bench-request"),
		testDigest("bench-terminal"), uint64(len(plan)), root, contract)
	persisted, _ := MaterializeCreate(template, 11)
	templateRaw, _ := AppendHead(nil, template)
	persistedRaw, _ := AppendHead(nil, persisted)
	b.ReportAllocs()
	b.SetBytes(int64(len(templateRaw)))
	b.ResetTimer()
	for range b.N {
		if !SameCreateBytes(persistedRaw, templateRaw) {
			b.Fatal("retry mismatch")
		}
	}
}
