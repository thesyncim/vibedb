package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
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
	if got := RequestLedgerReadMaxBytes(RequestLedgerReadWave); got != MaxRequestLedgerWaveReadBytes ||
		got > requestledger.MaxCommandBytes {
		t.Fatalf("wave read bound = %d, exact=%d command=%d", got,
			MaxRequestLedgerWaveReadBytes, requestledger.MaxCommandBytes)
	}
	if got := RequestLedgerReadMaxBytes(RequestLedgerReadProgress); got !=
		MaxRequestLedgerProgressReadBytes || got > requestledger.MaxCommandBytes {
		t.Fatalf("progress read bound = %d, exact=%d command=%d", got,
			MaxRequestLedgerProgressReadBytes, requestledger.MaxCommandBytes)
	}
	if got := RequestLedgerReadMaxBytes(RequestLedgerReadTerminalCut); got !=
		MaxRequestLedgerTerminalReadBytes || got <= requestledger.MaxCommandBytes ||
		got > requestledger.MaxCommandBytes+(1<<20) {
		t.Fatalf("terminal cut bound = %d, exact=%d command=%d", got,
			MaxRequestLedgerTerminalReadBytes, requestledger.MaxCommandBytes)
	}
}

func TestRequestLedgerWaveRecoveryReadUsesOneCoherentCut(t *testing.T) {
	fixture := newRequestLedgerMachineFixture(t, 64<<20)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x11}, Principal: requestledger.PrincipalID{0x21},
		Request: requestledger.RequestID{0x31}}
	create, wantHead := requestLedgerCreateCommand(t, fixture, key)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), create); err != nil {
		t.Fatal(err)
	}
	read := RequestLedgerReadRequest{
		Key: key, ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Kind: RequestLedgerReadWave, MinimumApplied: 2,
		MaxBytes: uint32(RequestLedgerReadMaxBytes(RequestLedgerReadWave)),
	}
	result, err := fixture.machine.RequestLedgerReadInto(
		read, make([]byte, 0, read.MaxBytes),
	)
	if err != nil || !result.Found || result.AuthoritativeKind != RequestLedgerReadWave {
		t.Fatalf("wave read=%+v err=%v", result, err)
	}
	value, err := OpenRequestLedgerWaveReadValue(result.Value)
	if err != nil || value.RouteFound || value.PendingFound {
		t.Fatalf("wave value=%+v err=%v", value, err)
	}
	head, err := requestledger.OpenHead(value.Head)
	if err != nil || head.KeyDigest != wantHead.KeyDigest {
		t.Fatalf("head=%+v err=%v", head, err)
	}
	for _, corrupt := range [][]byte{
		result.Value[:len(result.Value)-1],
		append(append([]byte(nil), result.Value...), 0),
	} {
		if _, openErr := OpenRequestLedgerWaveReadValue(corrupt); openErr == nil {
			t.Fatal("accepted noncanonical wave value")
		}
	}
}

func TestRequestLedgerProgressAndTerminalCutsShareOneSnapshot(t *testing.T) {
	fixture := newRequestLedgerMachineFixture(t, 64<<20)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x13}, Principal: requestledger.PrincipalID{0x23},
		Request: requestledger.RequestID{0x33}}
	create, wantHead := requestLedgerCreateCommand(t, fixture, key)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), create); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	for _, kind := range []RequestLedgerReadKind{
		RequestLedgerReadProgress, RequestLedgerReadTerminalCut,
	} {
		read := RequestLedgerReadRequest{
			Key: key, ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
			Kind: kind, MinimumApplied: 2, MaxBytes: uint32(RequestLedgerReadMaxBytes(kind)),
		}
		result, err := reopened.RequestLedgerReadInto(
			read, make([]byte, 0, read.MaxBytes),
		)
		if err != nil || !result.Found || result.AuthoritativeKind != kind {
			t.Fatalf("kind=%d result=%+v err=%v", kind, result, err)
		}
		var headRaw []byte
		if kind == RequestLedgerReadProgress {
			value, openErr := OpenRequestLedgerProgressReadValue(result.Value)
			if openErr != nil || value.ContinuationFound {
				t.Fatalf("progress=%+v err=%v", value, openErr)
			}
			headRaw = value.Head
		} else {
			value, openErr := OpenRequestLedgerTerminalReadValue(result.Value)
			if openErr != nil || value.ContinuationFound || value.PreparedFound ||
				value.SchemaPinFound || value.TerminalFound {
				t.Fatalf("terminal=%+v err=%v", value, openErr)
			}
			headRaw = value.Head
		}
		head, openErr := requestledger.OpenHead(headRaw)
		if openErr != nil || head.KeyDigest != wantHead.KeyDigest {
			t.Fatalf("head=%+v err=%v", head, openErr)
		}
		if _, openErr = func() (any, error) {
			if kind == RequestLedgerReadProgress {
				return OpenRequestLedgerProgressReadValue(result.Value[:len(result.Value)-1])
			}
			return OpenRequestLedgerTerminalReadValue(result.Value[:len(result.Value)-1])
		}(); openErr == nil {
			t.Fatal("accepted truncated coherent cut")
		}
	}
}

func BenchmarkRequestLedgerWaveRecoveryRead(b *testing.B) {
	fixture := newRequestLedgerMachineFixture(b, 64<<20)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		b.Fatal(err)
	}
	key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x11}, Principal: requestledger.PrincipalID{0x21},
		Request: requestledger.RequestID{0x31}}
	create, _ := requestLedgerCreateCommand(b, fixture, key)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), create); err != nil {
		b.Fatal(err)
	}
	read := RequestLedgerReadRequest{
		Key: key, ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Kind: RequestLedgerReadWave, MinimumApplied: 2,
		MaxBytes: uint32(RequestLedgerReadMaxBytes(RequestLedgerReadWave)),
	}
	dst := make([]byte, 0, read.MaxBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := fixture.machine.RequestLedgerReadInto(read, dst[:0])
		if err != nil || !result.Found {
			b.Fatalf("result=%+v err=%v", result, err)
		}
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

func requestLedgerOuterCommand(
	t testing.TB,
	fixture machineFixture,
	sequence uint64,
	inner []byte,
) []byte {
	t.Helper()
	outer := commandValue(fixture.binding, sequence)
	outer.Kind = replication.CommandRequestLedger
	outer.AuthorityClass = replication.CommandAuthorityRequestLedger
	outer.Batches = nil
	outer.RequestLedger = inner
	outer.Fingerprint = sha256.Sum256(inner)
	return encodeCommand(t, outer)
}

func requestLedgerAcquireEvidence(
	t testing.TB,
	fixture machineFixture,
	head requestledger.HeadRecord,
) (requestledger.RoutePinRecord, requestledger.RoutePinRecord) {
	t.Helper()
	pin := requestledger.PinID{0x51, 0x52}
	logical := requestledger.Digest(sha256.Sum256([]byte("fused logical binding")))
	identityDigest, err := requestledger.DeriveRouteGateIdentity(
		head.KeyDigest, head.RequestDigest, head.PlanRoot, head.ContinuationDigest,
		pin, head.NextStepOrdinal,
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := routegate.Identity(identityDigest)
	provisional := requestLedgerRouteGateOuter(t, fixture.binding, 31, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: routegate.Binding{1},
	})
	provisionalView, err := replication.OpenCommand(provisional)
	if err != nil {
		t.Fatal(err)
	}
	physical, ok := replication.RouteGatePhysicalWitness(provisionalView)
	if !ok {
		t.Fatal("route-gate physical witness unavailable")
	}
	bindingDigest, err := requestledger.DeriveRouteGateBinding(
		identityDigest, logical, requestledger.Digest(physical), 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	commandRaw := requestLedgerRouteGateOuter(t, fixture.binding, 31, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: routegate.Binding(bindingDigest),
	})
	commandView, err := replication.OpenCommand(commandRaw)
	if err != nil {
		t.Fatal(err)
	}
	finalPhysical, ok := replication.RouteGatePhysicalWitness(commandView)
	if !ok || finalPhysical != physical {
		t.Fatal("route-gate command changed physical witness")
	}
	acquiring, err := requestledger.NewRoutePinAcquiring(
		head, pin, logical, requestledger.Digest(physical), commandRaw,
	)
	if err != nil {
		t.Fatal(err)
	}
	outcomeRaw, err := routegate.AppendOutcome(nil, routegate.Outcome{
		Reason: routegate.ReasonAcquired, Mutated: true,
		Status: routegate.Status{
			Revision: 1, Epoch: 1, ActivePins: 1, RetainedRecords: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resultDigest := replication.CompletionResultDigest(
		ResultRouteGate, ResultFormatRouteGate, outcomeRaw,
	)
	completionRaw, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID: commandView.ClusterID, ClusterIncarnation: commandView.ClusterIncarnation,
		TopologyRecoveryEpoch: commandView.TopologyRecoveryEpoch,
		Distribution:          commandView.Distribution, Shard: commandView.Shard,
		AllocationGeneration: commandView.AllocationGeneration,
		ShardIncarnation:     commandView.ShardIncarnation, GroupID: commandView.GroupID,
		ReplicaSetVersion:      commandView.ReplicaSetVersion,
		ActivePolicyGeneration: commandView.ActivePolicyGeneration,
		ProtectionEpoch:        commandView.ProtectionEpoch,
		RoutingVersion:         commandView.RoutingVersion, RouteGeneration: commandView.RouteGeneration,
		Tenant: commandView.Tenant, ClientID: commandView.ClientID, ClientEpoch: commandView.ClientEpoch,
		ClientSequence: commandView.ClientSequence, Fingerprint: commandView.Fingerprint,
		RetryHome: commandView.RetryHome, AppliedSequence: 11,
		ResultCode: ResultRouteGate, ResultFormat: ResultFormatRouteGate,
		Storage: replication.CompletionInline, ResultLength: uint64(len(outcomeRaw)),
		ResultDigest: resultDigest, InlineResult: outcomeRaw,
	})
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := requestledger.RecordVerifiedRoutePinAcquired(
		acquiring, acquiring.Revision+1, completionRaw,
	)
	if err != nil || !requestLedgerRouteCompletionEvidenceAvailable(acquiring, acquired) {
		t.Fatalf("valid acquire settlement rejected: %v", err)
	}
	return acquiring, acquired
}

func requestLedgerCompletionForCommand(
	t testing.TB,
	machine *Machine,
	command []byte,
) RequestLedgerCompletionResult {
	t.Helper()
	lookup, err := machine.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	result, err := OpenRequestLedgerCompletionResult(
		completion.ResultCode, completion.InlineResult,
	)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRequestLedgerFusedAcquirePendingIsAtomicAndReplayExact(t *testing.T) {
	fixture := newRequestLedgerMachineFixture(t, 64<<20)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	key := requestledger.RequestKey{
		Scope:        requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x15}, Principal: requestledger.PrincipalID{0x25},
		Request: requestledger.RequestID{0x35},
	}
	create, initial := requestLedgerCreateCommand(t, fixture, key)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), create); err != nil {
		t.Fatal(err)
	}
	home, _ := requestledger.Home(key)
	acquiring, acquired := requestLedgerAcquireEvidence(t, fixture, initial)
	intentHead, err := requestledger.AdvanceHeadRoutePin(
		initial, requestledger.RoutePinRecord{}, acquiring, initial.Revision+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	intentRaw, _ := requestledger.AppendRoutePin(nil, acquiring)
	intentInner, err := requestledger.AppendCommand(nil, requestledger.Command{
		Operation:        requestledger.OperationBeginRoutePinAcquire,
		ExpectedRevision: initial.Revision, Revision: intentHead.Revision,
		KeyDigest: initial.KeyDigest, RequestDigest: initial.RequestDigest,
		PlanRoot: initial.PlanRoot, SubjectDigest: acquiring.RecordDigest,
		ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Home:                  home, Payload: intentRaw,
	})
	if err != nil {
		t.Fatal(err)
	}
	intentCommand := requestLedgerOuterCommand(t, fixture, 2, intentInner)
	if _, err = fixture.machine.ApplyNormal(normalMeta(3), intentCommand); err != nil {
		t.Fatal(err)
	}
	storedIntent, err := fixture.machine.RequestLedgerReadInto(RequestLedgerReadRequest{
		Key: key, ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Kind: RequestLedgerReadHead, MinimumApplied: 3,
		MaxBytes: uint32(RequestLedgerReadMaxBytes(RequestLedgerReadHead)),
	}, make([]byte, 0, RequestLedgerReadMaxBytes(RequestLedgerReadHead)))
	if err != nil || !storedIntent.Found {
		t.Fatalf("stored intent head=%+v err=%v", storedIntent, err)
	}
	intentHead, err = requestledger.OpenHead(storedIntent.Value)
	if err != nil {
		t.Fatal(err)
	}
	acquiredHead, err := requestledger.AdvanceHeadRoutePin(
		intentHead, acquiring, acquired, intentHead.Revision+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	step := requestledger.StepRef{
		TargetSource:  requestledger.PayloadSourcePlan,
		CommandSource: requestledger.PayloadSourcePlan,
		TargetOffset:  0, TargetLength: 8, CommandOffset: 8, CommandLength: 8,
		TargetDigest:  requestledger.Digest(sha256.Sum256([]byte("fused target"))),
		CommandDigest: requestledger.Digest(sha256.Sum256([]byte("fused command"))),
	}
	pending, err := requestledger.NewPendingWaveWithRoutePin(
		acquiredHead, requestledger.PayloadBuildRecord{}, acquiredHead.Revision+1,
		acquired, []requestledger.StepRef{step},
	)
	if err != nil {
		t.Fatal(err)
	}
	legacyFinal, err := requestledger.InstallPendingWave(
		acquiredHead, pending, requestledger.PayloadBuildRecord{}, acquired,
	)
	if err != nil {
		t.Fatal(err)
	}
	compoundRaw, err := requestledger.AppendAcquiredPending(nil, acquired, pending)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := requestledger.AcquiredPendingDigest(acquired, pending)
	if err != nil {
		t.Fatal(err)
	}
	fusedInner, err := requestledger.AppendCommand(nil, requestledger.Command{
		Operation:        requestledger.OperationRecordRoutePinAcquiredPutPending,
		ExpectedRevision: intentHead.Revision, Revision: pending.Revision,
		KeyDigest: initial.KeyDigest, RequestDigest: initial.RequestDigest,
		PlanRoot: initial.PlanRoot, SubjectDigest: subject,
		ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Home:                  home, Payload: compoundRaw,
	})
	if err != nil {
		t.Fatal(err)
	}
	fusedCommand := requestLedgerOuterCommand(t, fixture, 3, fusedInner)
	if _, err = fixture.machine.ApplyNormal(normalMeta(4), fusedCommand); err != nil {
		t.Fatal(err)
	}
	first := requestLedgerCompletionForCommand(t, fixture.machine, fusedCommand)
	if first.ResultCode != ResultApplied || !first.ExactDuplicate || first.Revision != pending.Revision {
		t.Fatalf("first fused completion=%+v", first)
	}
	usage, err := fixture.machine.RequestLedgerUsage()
	if err != nil || usage.Rows != 3 {
		t.Fatalf("usage after fused apply=%+v err=%v", usage, err)
	}
	read := RequestLedgerReadRequest{
		Key: key, ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Kind: RequestLedgerReadWave, MinimumApplied: 4,
		MaxBytes: uint32(RequestLedgerReadMaxBytes(RequestLedgerReadWave)),
	}
	readResult, err := fixture.machine.RequestLedgerReadInto(
		read, make([]byte, 0, read.MaxBytes),
	)
	if err != nil || !readResult.Found {
		t.Fatalf("fused wave read=%+v err=%v", readResult, err)
	}
	wave, err := OpenRequestLedgerWaveReadValue(readResult.Value)
	wantHead, _ := requestledger.AppendHead(nil, legacyFinal)
	wantRoute, _ := requestledger.AppendRoutePin(nil, acquired)
	wantPending, _ := requestledger.AppendPendingWave(nil, pending)
	if err != nil || !wave.RouteFound || !wave.PendingFound ||
		!bytes.Equal(wave.Head, wantHead) || !bytes.Equal(wave.RoutePin, wantRoute) ||
		!bytes.Equal(wave.Pending, wantPending) {
		t.Fatalf("fused rows differ from legacy rows: found=%v/%v equal=%v/%v/%v sizes=%d/%d %d/%d %d/%d err=%v",
			wave.RouteFound, wave.PendingFound, bytes.Equal(wave.Head, wantHead),
			bytes.Equal(wave.RoutePin, wantRoute), bytes.Equal(wave.Pending, wantPending),
			len(wave.Head), len(wantHead), len(wave.RoutePin), len(wantRoute),
			len(wave.Pending), len(wantPending), err)
	}

	// A replacement gateway reuses the exact inner command with an unrelated
	// outer retry identity after losing the first response.
	replayCommand := requestLedgerOuterCommand(t, fixture, 4, fusedInner)
	if _, err = fixture.machine.ApplyNormal(normalMeta(5), replayCommand); err != nil {
		t.Fatal(err)
	}
	replay := requestLedgerCompletionForCommand(t, fixture.machine, replayCommand)
	afterReplay, _ := fixture.machine.RequestLedgerUsage()
	if !replay.ExactDuplicate || replay.ResultCode != ResultApplied ||
		replay.StateDigest != first.StateDigest || afterReplay != usage {
		t.Fatalf("replay=%+v usage=%+v want=%+v", replay, afterReplay, usage)
	}

	// A non-exact retry at a different CAS cut cannot partially rewrite either
	// nested row or the final head.
	conflictInner, err := requestledger.AppendCommand(nil, requestledger.Command{
		Operation:        requestledger.OperationRecordRoutePinAcquiredPutPending,
		ExpectedRevision: intentHead.Revision + 1, Revision: pending.Revision + 1,
		KeyDigest: initial.KeyDigest, RequestDigest: initial.RequestDigest,
		PlanRoot: initial.PlanRoot, SubjectDigest: subject,
		ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Home:                  home, Payload: compoundRaw,
	})
	if err != nil {
		t.Fatal(err)
	}
	conflictCommand := requestLedgerOuterCommand(t, fixture, 5, conflictInner)
	if _, err = fixture.machine.ApplyNormal(normalMeta(6), conflictCommand); err != nil {
		t.Fatal(err)
	}
	conflict := requestLedgerCompletionForCommand(t, fixture.machine, conflictCommand)
	afterConflict, _ := fixture.machine.RequestLedgerUsage()
	read.MinimumApplied = 6
	afterRead, err := fixture.machine.RequestLedgerReadInto(
		read, make([]byte, 0, read.MaxBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.ResultCode != ResultRequestLedgerConflict || conflict.ExactDuplicate ||
		afterConflict != usage || !bytes.Equal(afterRead.Value, readResult.Value) {
		t.Fatalf("conflict=%+v usage=%+v want=%+v", conflict, afterConflict, usage)
	}
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
	read := RequestLedgerReadRequest{
		Key: key, ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
		Kind: RequestLedgerReadIssuerStatus, MinimumApplied: 2,
		MaxBytes: uint32(RequestLedgerReadMaxBytes(RequestLedgerReadIssuerStatus)),
	}
	statusResult, err := reopened.RequestLedgerReadInto(read, make([]byte, 0, read.MaxBytes))
	if err != nil || !statusResult.Found ||
		statusResult.AuthoritativeKind != RequestLedgerReadIssuerStatus {
		t.Fatalf("reopened issuer status = %+v, %v", statusResult, err)
	}
	status, err := requestledger.OpenIssuerLaneStatus(statusResult.Value)
	if err != nil || status.Highwater.AdmittedSequence != 1 ||
		status.Highwater.HighwaterSequence != 0 || !status.NextFound || status.AdvanceReady ||
		status.Sequence.Sequence != 1 || status.Sequence.Phase != requestledger.IssuerSequenceActive {
		t.Fatalf("opened issuer status = %+v, %v", status, err)
	}

	// The same home and range cannot be used to read a different authenticated
	// principal/lane identity, even when the caller knows the RF3 endpoint.
	wrong := key
	wrong.Principal[0] ^= 0xff
	read.Key = wrong
	wrongResult, err := reopened.RequestLedgerReadInto(read, make([]byte, 0, read.MaxBytes))
	if err != nil || wrongResult.Found {
		t.Fatalf("foreign issuer status = %+v, %v", wrongResult, err)
	}
	read.Key = key
	read.ExpectedRangeIdentity[0] ^= 0xff
	if _, err := reopened.RequestLedgerReadInto(read, make([]byte, 0, read.MaxBytes)); err == nil {
		t.Fatal("issuer status accepted a stale ledger range identity")
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
		Request: requestledger.RequestID{0xa1}, IssuerEpoch: 9, IssuerSequence: 1,
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
