package raftserve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestServingRegistryHostChangedAckAttemptAdvancesDurableRetryFloor(t *testing.T) {
	runtime, base := newServingRuntime(t, 71)
	registry := testRegistry(t, 8, 16, 16)
	host, err := registry.NewHost(testServingHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Add(runtime); err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = host.Close()
			_ = registry.Close()
		}
	})
	driveServingHostIdle(t, host)
	if err := host.RequestCampaign(runtime.Identity().Group); err != nil {
		t.Fatal(err)
	}
	driveServingHostIdle(t, host)

	openValue := servingCommand(base, 0, 1)
	openValue.Kind = replication.CommandSessionOpen
	openValue.AckThrough = 0
	openValue.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	openValue.Batches = nil
	openData := encodeTestCommand(t, openValue)
	openWaiter, err := registry.Enqueue(host, runtime.Identity().Group, openData)
	if err != nil {
		t.Fatal(err)
	}
	openOutcome := driveServingWaiter(t, host, openWaiter)
	if openOutcome.Code != OutcomeCompletion {
		t.Fatalf("open outcome = %+v", openOutcome)
	}
	completionDst := make([]byte, 0, completionSlotBytes)
	completionDst, _, err = openWaiter.TakeCompletionInto(completionDst)
	if err != nil {
		t.Fatal(err)
	}
	openCompletion, err := replication.OpenCompletion(completionDst)
	if err != nil || openCompletion.ClientEpoch == 0 {
		t.Fatalf("open completion = %+v, %v", openCompletion, err)
	}

	command := servingCommand(base, openCompletion.ClientEpoch, 2)
	command.AckThrough = 0
	firstData := encodeTestCommand(t, command)
	first, err := registry.Enqueue(host, runtime.Identity().Group, firstData)
	if err != nil {
		t.Fatal(err)
	}
	command.AckThrough = 1
	ackData := encodeTestCommand(t, command)
	acknowledging, err := registry.Enqueue(host, runtime.Identity().Group, ackData)
	if err != nil {
		t.Fatal(err)
	}
	firstOutcome := driveServingWaiter(t, host, first)
	ackOutcome := driveServingWaiter(t, host, acknowledging)
	if firstOutcome.Code != OutcomeCompletion || ackOutcome.Code != OutcomeCompletion {
		t.Fatalf("changed Ack outcomes = %+v, %+v", firstOutcome, ackOutcome)
	}
	firstResult := make([]byte, 0, completionSlotBytes)
	firstResult, _, err = first.TakeCompletionInto(firstResult)
	if err != nil {
		t.Fatal(err)
	}
	ackResult := make([]byte, 0, completionSlotBytes)
	ackResult, _, err = acknowledging.TakeCompletionInto(ackResult)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstResult, ackResult) {
		t.Fatal("changed Ack attempt did not share the exact logical completion")
	}

	retry, err := registry.Enqueue(host, runtime.Identity().Group, openData)
	if err != nil {
		t.Fatal(err)
	}
	retryOutcome := driveServingWaiter(t, host, retry)
	if retryOutcome.Code != OutcomeRetryRetired ||
		!errors.Is(retryOutcome.Err(), replicatedstate.ErrRetryRetired) {
		t.Fatalf("AckThrough did not retire Open retry = %+v, %v",
			retryOutcome, retryOutcome.Err())
	}
	retry.Cancel()
	driveServingHostIdle(t, host)
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
}

func TestServingHostTerminalSettlementFailureCanCloseRealRuntime(t *testing.T) {
	runtime, base := newServingRuntime(t, 72)
	group := runtime.Identity().Group
	want := errors.New("terminal serving settlement failure")
	var admissions []multiraft.ProposalAdmission
	var terminations []multiraft.ProposalGroupTermination
	clientSettlements := 0
	host, err := multiraft.NewHostWithServingSinks(
		testServingHostLimits(), multiraft.ServingSinks{
			Settle: func(
				_ raftmember.AppliedSourceOwner,
				_ raftmember.AppliedSourceToken,
				batch raftmember.AppliedBatch,
			) error {
				hasCommand, scanErr := servingBatchHasClientCommand(batch)
				if scanErr != nil {
					return scanErr
				}
				if !hasCommand {
					return nil
				}
				clientSettlements++
				return want
			},
			Proposals: func(admission multiraft.ProposalAdmission) {
				admissions = append(admissions, admission)
			},
			ProposalGroups: func(termination multiraft.ProposalGroupTermination) {
				terminations = append(terminations, termination)
			},
			ProposalPending: func(
				raftmember.AppliedSourceOwner,
				raftmember.AppliedSourceToken,
			) bool {
				return false
			},
			ClaimSource: func(
				raftmember.AppliedSourceOwner,
			) (raftmember.AppliedSourceToken, error) {
				return raftmember.AppliedSourceToken{RegistryID: 1, OwnerEpoch: 1}, nil
			},
			ReleaseSource: func(
				raftmember.AppliedSourceOwner,
				raftmember.AppliedSourceToken,
			) error {
				return nil
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = host.Close()
		}
	})
	if err := host.Add(runtime); err != nil {
		t.Fatal(err)
	}
	driveServingHostIdle(t, host)
	if err := host.RequestCampaign(group); err != nil {
		t.Fatal(err)
	}
	driveServingHostIdle(t, host)
	if err := host.EnqueueProposal(group, servingOpenCommand(t, base)); err != nil {
		t.Fatal(err)
	}
	faulted := false
	for step := 0; step < 20000; step++ {
		progress, done, runErr := host.RunOne()
		if runErr == nil {
			if !done {
				t.Fatal("Host became idle before terminal settlement")
			}
			continue
		}
		if !done || progress.Kind != multiraft.ProgressFault ||
			!errors.Is(runErr, raftmember.ErrRuntimeFailed) || !errors.Is(runErr, want) {
			t.Fatalf("terminal Host settlement = %+v, %v, %v", progress, done, runErr)
		}
		faulted = true
		break
	}
	if !faulted {
		t.Fatal("Host did not surface terminal settlement failure")
	}
	if clientSettlements != 1 {
		t.Fatalf("terminal client settlements = %d, want 1", clientSettlements)
	}
	if len(admissions) != 0 || len(terminations) != 1 ||
		terminations[0].Group != group ||
		terminations[0].Reason != multiraft.ProposalGroupFaulted {
		t.Fatalf("terminal Host lifecycle = admissions %+v terminations %+v",
			admissions, terminations)
	}
	if err := host.Remove(group); err != nil {
		t.Fatalf("Host remove after terminal settlement = %v", err)
	}
	if err := host.Close(); err != nil {
		t.Fatalf("Host close after terminal settlement = %v", err)
	}
	closed = true
}

func TestServingHostRetryableSettlementStillBlocksCloseUntilRealRetry(t *testing.T) {
	runtime, base := newServingRuntime(t, 73)
	group := runtime.Identity().Group
	want := errors.New("retryable serving settlement failure")
	retry := true
	clientSettlements := 0
	host, err := multiraft.NewHostWithResultSettlementSink(
		testServingHostLimits(),
		func(batch raftmember.AppliedBatch) error {
			hasCommand, scanErr := servingBatchHasClientCommand(batch)
			if scanErr != nil {
				return scanErr
			}
			if !hasCommand {
				return nil
			}
			clientSettlements++
			if retry {
				return raftmember.RetryResultSettlement(want)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		retry = false
		if !closed {
			_ = host.Close()
		}
	})
	if err := host.Add(runtime); err != nil {
		t.Fatal(err)
	}
	driveServingHostIdle(t, host)
	if err := host.RequestCampaign(group); err != nil {
		t.Fatal(err)
	}
	driveServingHostIdle(t, host)
	if err := host.EnqueueProposal(group, servingOpenCommand(t, base)); err != nil {
		t.Fatal(err)
	}
	rejected := false
	for step := 0; step < 20000; step++ {
		_, done, runErr := host.RunOne()
		if runErr == nil {
			if !done {
				t.Fatal("Host became idle before retryable settlement")
			}
			continue
		}
		if done || !errors.Is(runErr, raftmember.ErrResultSettlementRejected) ||
			!errors.Is(runErr, want) {
			t.Fatalf("retryable Host settlement = done %v, %v", done, runErr)
		}
		rejected = true
		break
	}
	if !rejected {
		t.Fatal("Host did not surface retryable settlement failure")
	}
	if clientSettlements != 1 {
		t.Fatalf("rejected client settlements = %d, want 1", clientSettlements)
	}
	if err := host.Close(); !errors.Is(err, multiraft.ErrGroupBusy) ||
		!errors.Is(err, raftmember.ErrResultSettlementPending) {
		t.Fatalf("Host close during retryable settlement = %v", err)
	}
	if _, err := host.Status(group); err != nil {
		t.Fatalf("blocked Host close mutated ownership: %v", err)
	}
	retry = false
	driveServingHostIdle(t, host)
	if clientSettlements != 2 {
		t.Fatalf("retried client settlements = %d, want 2", clientSettlements)
	}
	if err := host.Close(); err != nil {
		t.Fatalf("Host close after settlement retry = %v", err)
	}
	closed = true
}

func servingBatchHasClientCommand(batch raftmember.AppliedBatch) (bool, error) {
	if batch.Len() <= 0 {
		return false, errors.New("empty applied settlement batch")
	}
	for index := 0; index < batch.Len(); index++ {
		entry, ok := batch.Entry(index)
		if !ok {
			return false, errors.New("invalid applied settlement entry")
		}
		if len(entry.Data) != 0 {
			return true, nil
		}
	}
	return false, nil
}

func servingOpenCommand(
	t testing.TB,
	base sqldriver.ReplicatedShardStoreIdentity,
) []byte {
	t.Helper()
	command := servingCommand(base, 0, 1)
	command.Kind = replication.CommandSessionOpen
	command.AckThrough = 0
	command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	command.Batches = nil
	return encodeTestCommand(t, command)
}

func driveServingWaiter(
	t testing.TB,
	host *multiraft.Host,
	waiter Waiter,
) Outcome {
	t.Helper()
	for step := 0; step < 20000; step++ {
		outcome, ready, err := waiter.Poll()
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			return outcome
		}
		_, done, runErr := host.RunOne()
		if runErr != nil {
			outcome, ready, pollErr := waiter.Poll()
			if pollErr == nil && ready {
				return outcome
			}
			t.Fatalf("RunOne step %d: %v (waiter ready %v, poll %v)",
				step, runErr, ready, pollErr)
		}
		if !done {
			t.Fatal("Host became idle with a pending serving waiter")
		}
	}
	t.Fatal("serving waiter did not settle")
	return Outcome{}
}

func driveServingHostIdle(t testing.TB, host *multiraft.Host) {
	t.Helper()
	for step := 0; step < 20000; step++ {
		_, done, err := host.RunOne()
		if err != nil {
			t.Fatalf("RunOne step %d: %v", step, err)
		}
		if !done {
			return
		}
	}
	t.Fatal("Host did not become idle")
}

func servingCommand(
	base sqldriver.ReplicatedShardStoreIdentity,
	epoch, sequence uint64,
) replication.Command {
	binding := base.Binding
	key, ok := orderedkey.AppendJSONString(nil, []byte(`"a"`), orderedkey.Ascending)
	if !ok {
		panic("invalid serving test key")
	}
	return replication.Command{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch:  binding.Authority.ProtectionEpoch,
		OwnershipEpoch:   binding.Authority.OwnershipEpoch,
		SchemaGeneration: binding.Authority.SchemaGeneration,
		RoutingVersion:   binding.Authority.RoutingVersion,
		RouteGeneration:  binding.Authority.RouteGeneration,
		Tenant:           []byte("tenant"), ClientID: replication.ID128{7},
		ClientEpoch: epoch, ClientSequence: sequence,
		Fingerprint: sha256.Sum256([]byte("serving-logical-request")),
		Batches: []replication.RelationMutationBatch{{
			Relation: 1,
			Mutations: []replication.Mutation{{
				Kind: replication.MutationPut, Key: key,
				Value: []byte(`{"id":"a","value":1}`),
			}},
		}},
	}
}

func newServingRuntime(
	t testing.TB,
	seed byte,
) (*raftmember.Runtime, sqldriver.ReplicatedShardStoreIdentity) {
	return newServingRuntimeWithRetryWindow(t, seed, 8)
}

func newServingRuntimeWithRetryWindow(
	t testing.TB,
	seed byte,
	retryWindow uint16,
) (*raftmember.Runtime, sqldriver.ReplicatedShardStoreIdentity) {
	return newServingRuntimeWithVoters(t, seed, retryWindow, 16, nil)
}

func newServingRuntimeWithVoters(
	t testing.TB,
	seed byte,
	retryWindow uint16,
	maxSessions uint64,
	voters []uint64,
) (*raftmember.Runtime, sqldriver.ReplicatedShardStoreIdentity) {
	t.Helper()
	identity := raftstore.Identity{
		Distribution: "orders", Shard: "0000-7fff",
		AllocationGeneration: uint64(seed) + 1,
		MemberID:             uint64(seed) + 1,
	}
	for index := range identity.ClusterID {
		identity.ClusterID[index] = seed + byte(index) + 1
		identity.ClusterIncarnation[index] = seed + byte(index) + 19
		identity.ShardIncarnation[index] = seed + byte(index) + 37
		identity.GroupID[index] = seed + byte(index) + 55
		identity.StoreID[index] = seed + byte(index) + 73
	}
	if len(voters) == 0 {
		voters = []uint64{identity.MemberID}
	}
	key := raftstore.Key{ID: "raftserve-test-key", Wrapped: []byte("opaque-test-wrapped-key")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1)
	}
	options := raftstore.Options{
		MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
		MaxRecords: 1024, MaxEntries: 8192,
		MaxLiveBytes: 2 * raftstore.MinimumReadyLiveBytes,
	}
	index, term := uint64(1), uint64(1)
	wal, err := raftstore.Create(
		filepath.Join(t.TempDir(), "runtime.wal"), identity, key,
		raftstore.Bootstrap{
			TopologyRecoveryEpoch: 29,
			Snapshot: &pb.Snapshot{
				Data: []byte("raftserve-static-bootstrap"),
				Metadata: &pb.SnapshotMetadata{
					Index: &index, Term: &term,
					ConfState: &pb.ConfState{Voters: voters},
				},
			},
		}, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	database, err := sqldriver.InitializeShardStore(
		filepath.Join(t.TempDir(), "runtime.vdb"),
		sqldriver.ShardStoreBinding{
			Distribution:         distribution.DistributionName(identity.Distribution),
			Shard:                distribution.ShardID(identity.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(identity.AllocationGeneration),
		},
	)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := session.Prepare(context.Background(), `CREATE TABLE docs (PRIMARY KEY (id))`)
	if err == nil {
		_, err = prepared.Exec(context.Background(), nil)
	}
	if prepared != nil {
		err = errors.Join(err, prepared.Close())
	}
	err = errors.Join(err, session.Close())
	if err != nil {
		t.Fatal(err)
	}
	authority := sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 31, ProtectionEpoch: 37, OwnershipEpoch: 41,
		SchemaGeneration: 43, RoutingVersion: 47, RouteGeneration: 53,
	}
	base, err := raftmember.BindPreparedSQL(wal, database, authority, "docs")
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		_ = database.Close()
		_ = wal.Close()
		t.Skipf("strict allocation unsupported: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	apply, _, err := raftmember.OpenPreparedApply(
		wal, database, authority, base,
		sqldriver.ReplicatedApplyOptions{
			MaxSessions: maxSessions, RetryWindow: retryWindow,
			TxnLimits: durable.TxnLimits{
				MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 64 << 20,
			},
			Placement: sqldriver.ReplicatedPlacementProfile{
				Format:   sqldriver.ReplicatedPlacementProfileFormat,
				ShardKey: "/id", TupleVersion: distribution.CurrentTupleVersion,
				MapperVersion: distribution.NativeMapperVersion,
				Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			},
		},
	)
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		_ = database.Close()
		_ = wal.Close()
		t.Skipf("strict allocation unsupported: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	runtime, err := raftmember.AdoptRuntime(wal, database, apply)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, base
}

func testServingHostLimits() multiraft.Limits {
	return multiraft.Limits{
		MaxGroups: 1, MaxQueueItems: 64, MaxQueueBytes: 64 << 20,
		MaxGroupItems: 64, MaxGroupBytes: 64 << 20,
		MaxOutboxItems: 64, MaxOutboxBytes: 64 << 20,
		MaxPendingTicks: 8,
	}
}
