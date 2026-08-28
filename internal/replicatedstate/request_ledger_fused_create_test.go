package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func fusedPlanningCreateCommand(
	t testing.TB,
	fixture machineFixture,
	key requestledger.RequestKey,
	sequence byte,
) ([]byte, requestledger.HeadRecord) {
	t.Helper()
	plan, err := requestledger.AppendPlan(nil, bytes.Repeat([]byte{sequence}, requestledger.MaxInlinePlanBytes+1))
	if err != nil {
		t.Fatal(err)
	}
	keyDigest, _ := requestledger.KeyDigest(key)
	root, _ := requestledger.PlanRoot(keyDigest, plan)
	basePlan, _ := requestledger.AppendPlan(nil, []byte("fused-create-contract"))
	base, err := requestledger.NewHead(key, basePlan)
	if err != nil {
		t.Fatal(err)
	}
	contract := requestledger.ExecutionContract{
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
	template, err := requestledger.NewPagedHeadWithExecutionContract(
		key, requestledger.Digest{0x81}, requestledger.Digest{0x82}, uint64(len(plan)), root, contract,
	)
	if err != nil {
		t.Fatal(err)
	}
	home, _ := requestledger.Home(key)
	headRaw, _ := requestledger.AppendHead(nil, template)
	inner, err := requestledger.AppendCommand(nil, requestledger.Command{
		Operation: requestledger.OperationCreate, Revision: 1, KeyDigest: template.KeyDigest,
		RequestDigest: template.RequestDigest, PlanRoot: template.PlanRoot,
		SubjectDigest:         template.TerminalContractDigest,
		ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Home:                  home, Payload: headRaw,
	})
	if err != nil {
		t.Fatal(err)
	}
	outer := commandValue(fixture.binding, uint64(sequence))
	outer.Kind, outer.AuthorityClass = replication.CommandRequestLedger, replication.CommandAuthorityRequestLedger
	outer.Batches, outer.RequestLedger = nil, inner
	outer.Fingerprint = sha256.Sum256(inner)
	return encodeCommand(t, outer), template
}

func TestFusedPlanningCreatePersistsIssuerFrontierExpiryAndExactRetry(t *testing.T) {
	fixture := newRequestLedgerMachineFixture(t, 64<<20)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x11}, Principal: requestledger.PrincipalID{0x21},
		Request: requestledger.RequestID{0x31}, IssuerEpoch: 9, IssuerSequence: 1,
		IssuerLane: requestledger.IssuerLane{0x41}}
	create, template := fusedPlanningCreateCommand(t, fixture, key, 1)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), create); err != nil {
		t.Fatal(err)
	}
	lookup, err := fixture.machine.LookupCompletion(create)
	if err != nil {
		t.Fatal(err)
	}
	completion, _ := replication.OpenCompletion(lookup.Bytes)
	result, err := OpenRequestLedgerCompletionResult(completion.ResultCode, completion.InlineResult)
	if err != nil || result.PlanningLeaseExpiryIndex != 2+template.PlanningLeaseSpan {
		t.Fatalf("create result = %+v, %v", result, err)
	}
	home, _ := requestledger.Home(key)
	read := RequestLedgerReadRequest{Key: key, ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Kind: RequestLedgerReadHead, MinimumApplied: 2, MaxBytes: uint32(RequestLedgerReadMaxBytes(RequestLedgerReadHead))}
	row, err := fixture.machine.RequestLedgerReadInto(read, make([]byte, 0, read.MaxBytes))
	if err != nil || !row.Found {
		t.Fatalf("head read = %+v, %v", row, err)
	}
	head, err := requestledger.OpenHead(row.Value)
	if err != nil || head.PlanningLeaseExpiryIndex != result.PlanningLeaseExpiryIndex ||
		head.PlanningLeaseSpan != template.PlanningLeaseSpan {
		t.Fatalf("materialized head = %+v, %v", head, err)
	}
	statusRead := RequestLedgerReadRequest{Key: key,
		ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Kind:                  RequestLedgerReadIssuerStatus, MinimumApplied: 2,
		MaxBytes: uint32(RequestLedgerReadMaxBytes(RequestLedgerReadIssuerStatus))}
	statusRow, err := fixture.machine.RequestLedgerReadInto(statusRead,
		make([]byte, 0, statusRead.MaxBytes))
	status, statusErr := requestledger.OpenIssuerLaneStatus(statusRow.Value)
	if err != nil || statusErr != nil || !statusRow.Found || status.Highwater.AdmittedSequence != 1 ||
		status.Highwater.HighwaterSequence != 0 {
		t.Fatalf("issuer status = %+v, %v/%v", status, err, statusErr)
	}
	before, _ := fixture.machine.RequestLedgerUsage()
	view, _ := replication.OpenCommand(create)
	retryOuter := commandValue(fixture.binding, 9)
	retryOuter.Kind, retryOuter.AuthorityClass = replication.CommandRequestLedger, replication.CommandAuthorityRequestLedger
	retryOuter.Batches, retryOuter.RequestLedger = nil, view.RequestLedgerBytes()
	retryOuter.ClientID, retryOuter.ClientEpoch, retryOuter.ClientSequence = id128(0xe1), 77, 1
	retryOuter.Fingerprint = sha256.Sum256(retryOuter.RequestLedger)
	retry := encodeCommand(t, retryOuter)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), retry); err != nil {
		t.Fatal(err)
	}
	retryLookup, _ := fixture.machine.LookupCompletion(retry)
	retryCompletion, _ := replication.OpenCompletion(retryLookup.Bytes)
	retryResult, err := OpenRequestLedgerCompletionResult(retryCompletion.ResultCode, retryCompletion.InlineResult)
	after, _ := fixture.machine.RequestLedgerUsage()
	if err != nil || !retryResult.ExactDuplicate ||
		retryResult.PlanningLeaseExpiryIndex != result.PlanningLeaseExpiryIndex || before != after {
		t.Fatalf("retry=%+v usage=%+v/%+v err=%v", retryResult, before, after, err)
	}
	reopened, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
	if err != nil {
		t.Fatalf("reopen fused state: %v", err)
	}
	reopenedUsage, err := reopened.RequestLedgerUsage()
	if err != nil || reopenedUsage != after {
		t.Fatalf("reopen usage=%+v want=%+v: %v", reopenedUsage, after, err)
	}
	_ = home
}

func TestFusedPlanningCreateRejectsIssuerGapWithoutRows(t *testing.T) {
	fixture := newRequestLedgerMachineFixture(t, 64<<20)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x51}, Principal: requestledger.PrincipalID{0x61},
		Request: requestledger.RequestID{0x71}, IssuerEpoch: 4, IssuerSequence: 2,
		IssuerLane: requestledger.IssuerLane{0x81}}
	create, _ := fusedPlanningCreateCommand(t, fixture, key, 2)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), create); err != nil {
		t.Fatal(err)
	}
	usage, err := fixture.machine.RequestLedgerUsage()
	if err != nil || usage.Rows != 0 || usage.ResidentBytes != 0 || usage.ReservedBytes != 0 {
		t.Fatalf("gap mutated state: %+v, %v", usage, err)
	}
}
