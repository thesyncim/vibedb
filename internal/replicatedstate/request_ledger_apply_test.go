package replicatedstate

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/store/durable"
)

func newRequestLedgerMachineFixture(t testing.TB, capacity uint64) machineFixture {
	t.Helper()
	dir := t.TempDir()
	openCollection := func(name string, options durable.Options) CollectionTarget {
		file, err := os.OpenFile(filepath.Join(dir, name+".vdb"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Create(file, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		return targetOf(collection)
	}
	system := systemTargetOf(openCollection("system", durable.Options{
		OpaqueValues: true, MaxDocumentBytes: requestledger.MaxCommandBytes,
		MaxBatchDocuments: requestledger.MaxAckGCDeleteRows + 8,
		MaxBatchBytes:     128 << 20, ResidentBytes: 192 << 20,
	}).Collection)
	user := openCollection("user", durable.Options{})
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	binding := testBinding()
	bootstrap := testBootstrap()
	options := Options{
		TxnLimits:   durable.TxnLimits{MaxCollections: 2, MaxDocuments: system.Limits.MaxDistinctMutations, MaxBytes: 128 << 20},
		MaxSessions: 128, RetryWindow: 8,
		RequestLedgerCapacityBytes: capacity, RequestLedgerCleanupReserveBytes: 1 << 20,
		RequestLedgerRange: RequestLedgerRange{Identity: requestledger.Digest{0x91}},
	}
	machine, err := Open(binding, bootstrap, system, UserCollection{Name: "docs", Target: user}, log, options)
	if err != nil {
		t.Fatalf("Open ledger fixture: %v", err)
	}
	return machineFixture{machine, binding, bootstrap, system, user, log, dir}
}

func TestRequestLedgerRecoveryReadUsesExactHeadBound(t *testing.T) {
	if got := RequestLedgerReadMaxBytes(RequestLedgerReadHead); got != requestledger.MaxHeadRecordBytes ||
		got >= requestledger.MaxCommandBytes {
		t.Fatalf("head read bound = %d, exact=%d command=%d", got,
			requestledger.MaxHeadRecordBytes, requestledger.MaxCommandBytes)
	}
}

func requestLedgerCreateCommand(t testing.TB, fixture machineFixture, key requestledger.RequestKey) ([]byte, requestledger.HeadRecord) {
	t.Helper()
	plan, err := requestledger.AppendPlan(nil, []byte("canonical durable recipe"))
	if err != nil {
		t.Fatal(err)
	}
	head, err := requestledger.NewHeadWithContract(
		key, requestledger.Digest{0x31}, requestledger.Digest{0x41}, plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	home, err := requestledger.Home(key)
	if err != nil {
		t.Fatal(err)
	}
	headBytes, err := requestledger.AppendHead(nil, head)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := requestledger.AppendCommand(nil, requestledger.Command{
		Operation: requestledger.OperationCreate, Revision: head.Revision,
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		SubjectDigest:         head.TerminalContractDigest,
		ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Home:                  home, Payload: headBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	outer := commandValue(fixture.binding, 1)
	outer.Kind = replication.CommandRequestLedger
	outer.AuthorityClass = replication.CommandAuthorityRequestLedger
	outer.Batches = nil
	outer.RequestLedger = inner
	outer.Fingerprint = sha256.Sum256(inner)
	return encodeCommand(t, outer), head
}

func TestRequestLedgerCreateSettlesWithoutSessionAndReopens(t *testing.T) {
	fixture := newRequestLedgerMachineFixture(t, 64<<20)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x11}, Principal: requestledger.PrincipalID{0x21},
		Request: requestledger.RequestID{0x31}}
	create, head := requestLedgerCreateCommand(t, fixture, key)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), create); err != nil {
		t.Fatalf("create: %v", err)
	}
	usage, err := fixture.machine.RequestLedgerUsage()
	if err != nil || usage.Rows != 1 || usage.ResidentBytes == 0 || usage.ReservedBytes == 0 {
		t.Fatalf("usage after create = %+v, %v", usage, err)
	}
	lookup, err := fixture.machine.LookupCompletion(create)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	result, err := OpenRequestLedgerCompletionResult(completion.ResultCode, completion.InlineResult)
	if err != nil || result.ResultCode != ResultApplied || result.Phase != requestledger.PhaseSealed ||
		result.KeyDigest != head.KeyDigest || !result.ExactDuplicate {
		t.Fatalf("create completion = %+v, %v", result, err)
	}

	// A replacement gateway uses a fresh outer identity. Inner revision CAS,
	// not a process-local session journal, proves the exact replay.
	view, err := replication.OpenCommand(create)
	if err != nil {
		t.Fatal(err)
	}
	outer := commandValue(fixture.binding, 2)
	outer.Kind = replication.CommandRequestLedger
	outer.AuthorityClass = replication.CommandAuthorityRequestLedger
	outer.Batches = nil
	outer.RequestLedger = view.RequestLedgerBytes()
	outer.ClientID = id128(0xe1)
	outer.ClientEpoch = 77
	outer.ClientSequence = 1
	outer.Fingerprint = sha256.Sum256(outer.RequestLedger)
	retry := encodeCommand(t, outer)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), retry); err != nil {
		t.Fatalf("fresh outer retry: %v", err)
	}
	retryLookup, err := fixture.machine.LookupCompletion(retry)
	if err != nil {
		t.Fatal(err)
	}
	retryCompletion, _ := replication.OpenCompletion(retryLookup.Bytes)
	retryResult, err := OpenRequestLedgerCompletionResult(retryCompletion.ResultCode, retryCompletion.InlineResult)
	if err != nil || !retryResult.ExactDuplicate || retryResult.StateDigest != result.StateDigest {
		t.Fatalf("retry completion = %+v, %v", retryResult, err)
	}
	after, _ := fixture.machine.RequestLedgerUsage()
	if after.Rows != usage.Rows || after.ResidentBytes != usage.ResidentBytes || after.ReservedBytes != usage.ReservedBytes {
		t.Fatalf("duplicate changed usage: before=%+v after=%+v", usage, after)
	}

	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	reopenedUsage, err := reopened.RequestLedgerUsage()
	if err != nil || reopenedUsage.Rows != after.Rows || reopenedUsage.ResidentBytes != after.ResidentBytes ||
		reopenedUsage.ReservedBytes != after.ReservedBytes {
		t.Fatalf("reopened usage = %+v, %v; want %+v", reopenedUsage, err, after)
	}
	read := RequestLedgerReadRequest{Key: key,
		ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Kind:                  RequestLedgerReadHead, MinimumApplied: 3,
		MaxBytes: uint32(RequestLedgerReadMaxBytes(RequestLedgerReadHead))}
	dst := make([]byte, 0, read.MaxBytes)
	readResult, err := reopened.RequestLedgerReadInto(read, dst)
	if err != nil || !readResult.Found || readResult.AuthoritativeKind != RequestLedgerReadHead {
		t.Fatalf("reopened head read = %+v, %v", readResult, err)
	}
	openedHead, err := requestledger.OpenHead(readResult.Value)
	if err != nil || openedHead.Key != key || openedHead.KeyDigest != head.KeyDigest {
		t.Fatalf("opened head = %+v, %v", openedHead, err)
	}
}

func TestRequestLedgerSequencedCreateReopensExactUsage(t *testing.T) {
	fixture := newRequestLedgerMachineFixture(t, 64<<20)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x12}, Principal: requestledger.PrincipalID{0x22},
		Request: requestledger.RequestID{0x32}, IssuerEpoch: 1, IssuerSequence: 1,
		IssuerLane: requestledger.IssuerLane{0x42}}
	create, _ := requestLedgerCreateCommand(t, fixture, key)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), create); err != nil {
		t.Fatalf("sequenced create: %v", err)
	}
	usage, err := fixture.machine.RequestLedgerUsage()
	if err != nil || usage.Rows != 3 || usage.ResidentBytes == 0 || usage.ReservedBytes == 0 {
		t.Fatalf("sequenced usage = %+v, %v", usage, err)
	}
	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen sequenced ledger: %v", err)
	}
	reopenedUsage, err := reopened.RequestLedgerUsage()
	if err != nil || reopenedUsage != usage {
		t.Fatalf("reopened sequenced usage = %+v, %v; want %+v", reopenedUsage, err, usage)
	}
}

func TestRequestLedgerRangeAdmissionPrecedesSnapshotReads(t *testing.T) {
	fixture := newRequestLedgerMachineFixture(t, 64<<20)
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x51}, Principal: requestledger.PrincipalID{0x61},
		Request: requestledger.RequestID{0x71}}
	create, _ := requestLedgerCreateCommand(t, fixture, key)
	outer, err := replication.OpenCommand(create)
	if err != nil {
		t.Fatal(err)
	}
	fixture.machine.options.RequestLedgerRange.Identity[0] ^= 0xff
	// A zero pointSnapshot would panic if planning attempted any durable read.
	plan, err := fixture.machine.planRequestLedgerCommand(outer, State{}, pointSnapshot{})
	if err != nil || plan.completion.ResultCode != ResultRequestLedgerWrongRange || len(plan.rows) != 0 {
		t.Fatalf("wrong range plan = %+v, %v", plan.completion, err)
	}
}

func TestRequestLedgerSequencedHomeNotKeyDigestOwnsRange(t *testing.T) {
	fixture := newRequestLedgerMachineFixture(t, 64<<20)
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x81}, Principal: requestledger.PrincipalID{0x91},
		Request: requestledger.RequestID{0xa1}, IssuerEpoch: 9, IssuerSequence: 44,
		IssuerLane: [8]byte{0xb1}}
	create, head := requestLedgerCreateCommand(t, fixture, key)
	outer, err := replication.OpenCommand(create)
	if err != nil {
		t.Fatal(err)
	}
	home, _ := requestledger.Home(key)
	keyHome := requestledger.LedgerHome(head.KeyDigest)
	if home == keyHome {
		t.Fatal("fixture did not separate sequenced Home from KeyDigest")
	}
	unitRange := func(start requestledger.LedgerHome) RequestLedgerRange {
		end := start
		for index := len(end) - 1; index >= 0; index-- {
			end[index]++
			if end[index] != 0 {
				break
			}
		}
		return RequestLedgerRange{Start: start, End: end,
			Identity: fixture.machine.options.RequestLedgerRange.Identity}
	}
	snapshot, err := fixture.system.Collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	fixture.machine.options.RequestLedgerRange = unitRange(home)
	plan, err := fixture.machine.planRequestLedgerCommand(outer, State{}, pointSnapshot{value: snapshot})
	if err != nil || plan.completion.ResultCode != ResultApplied || len(plan.rows) == 0 {
		t.Fatalf("home-owned create = %+v rows=%d, %v", plan.completion, len(plan.rows), err)
	}
	fixture.machine.options.RequestLedgerRange = unitRange(keyHome)
	plan, err = fixture.machine.planRequestLedgerCommand(outer, State{}, pointSnapshot{})
	if err != nil || plan.completion.ResultCode != ResultRequestLedgerWrongRange || len(plan.rows) != 0 {
		t.Fatalf("digest-owned create = %+v rows=%d, %v", plan.completion, len(plan.rows), err)
	}
}
