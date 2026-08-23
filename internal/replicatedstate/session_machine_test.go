package replicatedstate

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestBoundedSessionWindowAckRetireAndEpochReuse(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	index := uint64(2)
	apply := func(command []byte) {
		t.Helper()
		publication, err := fixture.machine.ApplyNormal(normalMeta(index), command)
		if err != nil || publication.Applied != index {
			t.Fatalf("apply %d = %+v,%v", index, publication, err)
		}
		index++
	}

	commands := make([][]byte, 11)
	for sequence := uint64(1); sequence <= 10; sequence++ {
		commands[sequence] = encodeCommand(t, commandValue(fixture.binding, sequence))
		apply(commands[sequence])
	}
	if fixture.machine.state.SessionCount != 1 ||
		fixture.machine.state.SessionSlotCount != uint64(fixture.machine.options.RetryWindow) {
		t.Fatalf("bounded state = %+v", fixture.machine.state)
	}
	if _, err := fixture.machine.LookupCompletion(commands[1]); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("old retry error = %v", err)
	}
	retained, err := fixture.machine.LookupCompletion(commands[10])
	if err != nil {
		t.Fatal(err)
	}

	acknowledging := commandValue(fixture.binding, 10)
	acknowledging.AckThrough = 9
	ackCommand := encodeCommand(t, acknowledging)
	if err := fixture.machine.AdmitCommand(ackCommand); err != nil {
		t.Fatalf("acknowledging duplicate admission: %v", err)
	}
	apply(ackCommand)
	duplicate, err := fixture.machine.LookupCompletion(ackCommand)
	if err != nil || duplicate.AppliedSequence != retained.AppliedSequence ||
		!bytes.Equal(duplicate.Bytes, retained.Bytes) {
		t.Fatalf("ack duplicate = %+v,%v", duplicate, err)
	}
	if _, err := fixture.machine.LookupCompletion(commands[3]); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("acknowledged retry error = %v", err)
	}

	sequence11 := commandValue(fixture.binding, 11)
	sequence11.AckThrough = 9
	apply(encodeCommand(t, sequence11))
	regressed := commandValue(fixture.binding, 12)
	regressed.AckThrough = 8
	regressedBytes := encodeCommand(t, regressed)
	if err := fixture.machine.AdmitCommand(regressedBytes); !errors.Is(err, ErrSessionAck) {
		t.Fatalf("regressed ack admission = %v", err)
	}
	apply(regressedBytes)
	if _, err := fixture.machine.LookupCompletion(regressedBytes); !errors.Is(err, ErrCompletionNotFound) {
		t.Fatalf("refused ack lookup = %v", err)
	}
	sequence12 := commandValue(fixture.binding, 12)
	sequence12.AckThrough = 10
	apply(encodeCommand(t, sequence12))

	retire := commandValue(fixture.binding, 13)
	retire.Kind = replication.CommandSessionRetire
	retire.AckThrough = 12
	retire.Mutations = nil
	retireBytes := encodeCommand(t, retire)
	if err := fixture.machine.AdmitCommand(retireBytes); err != nil {
		t.Fatalf("retire admission: %v", err)
	}
	apply(retireBytes)
	retired, err := fixture.machine.LookupCompletion(retireBytes)
	if err != nil {
		t.Fatal(err)
	}
	retiredCompletion, err := replication.OpenCompletion(retired.Bytes)
	if err != nil || retiredCompletion.ResultCode != ResultSessionRetired {
		t.Fatalf("retire completion = %+v,%v", retiredCompletion, err)
	}

	sameEpoch := commandValue(fixture.binding, 14)
	sameEpoch.AckThrough = 12
	sameEpochBytes := encodeCommand(t, sameEpoch)
	if err := fixture.machine.AdmitCommand(sameEpochBytes); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("retired epoch admission = %v", err)
	}
	apply(sameEpochBytes)

	nextEpoch := commandValue(fixture.binding, 1)
	nextEpoch.ClientEpoch = 2
	nextEpochBytes := encodeCommand(t, nextEpoch)
	apply(nextEpochBytes)
	if _, err := fixture.machine.LookupCompletion(retireBytes); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("prior epoch retry = %v", err)
	}
	if fixture.machine.state.SessionCount != 1 ||
		fixture.machine.state.SessionSlotCount != uint64(fixture.machine.options.RetryWindow) {
		t.Fatalf("epoch reuse grew state = %+v", fixture.machine.state)
	}

	gap := commandValue(fixture.binding, 3)
	gap.ClientEpoch = 2
	gapBytes := encodeCommand(t, gap)
	if err := fixture.machine.AdmitCommand(gapBytes); !errors.Is(err, ErrSessionSequence) {
		t.Fatalf("gap admission = %v", err)
	}
	apply(gapBytes)
	next := commandValue(fixture.binding, 2)
	next.ClientEpoch = 2
	apply(encodeCommand(t, next))
	if _, err := fixture.machine.LookupCompletion(gapBytes); !errors.Is(err, ErrCompletionNotFound) {
		t.Fatalf("unaccepted gap lookup = %v", err)
	}
}

func TestStaleSessionRetireDoesNotSealLiveEpoch(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	first := encodeCommand(t, commandValue(fixture.binding, 1))
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), first); err != nil {
		t.Fatal(err)
	}

	stale := commandValue(fixture.binding, 2)
	stale.Kind = replication.CommandSessionRetire
	stale.AckThrough = 1
	stale.Mutations = nil
	stale.RoutingVersion++
	stale.RouteGeneration++
	staleBytes := encodeCommand(t, stale)
	if err := fixture.machine.AdmitCommand(staleBytes); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("stale retirement admission = %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), staleBytes); err != nil {
		t.Fatal(err)
	}
	lookup, err := fixture.machine.LookupCompletion(staleBytes)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != ResultStaleFence {
		t.Fatalf("stale retirement completion = %+v,%v", completion, err)
	}

	next := commandValue(fixture.binding, 3)
	next.AckThrough = 1
	nextBytes := encodeCommand(t, next)
	if err := fixture.machine.AdmitCommand(nextBytes); err != nil {
		t.Fatalf("live epoch was sealed: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), nextBytes); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalSessionRetireRetriesWithoutStrandingEpoch(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= uint64(fixture.machine.options.RetryWindow); sequence++ {
		if _, err := fixture.machine.ApplyNormal(
			normalMeta(sequence+1), encodeCommand(t, commandValue(fixture.binding, sequence)),
		); err != nil {
			t.Fatalf("seed sequence %d: %v", sequence, err)
		}
	}

	identity := commandValue(fixture.binding, 1)
	digest := SessionKey(identity.Tenant, identity.ClientID)
	sessionKey := SessionStorageKey(digest)
	high := uint64(math.MaxUint64 - 1)
	window := uint64(fixture.machine.options.RetryWindow)
	low := high - window + 1
	header, err := AppendSessionRecord(nil, SessionRecord{
		Tenant: identity.Tenant, ClientID: identity.ClientID,
		ClientEpoch: 1, RetryHome: identity.RetryHome,
		AckThrough: low - 1, HighSequence: high, Status: SessionActive,
		RetryWindow:       fixture.machine.options.RetryWindow,
		PhysicalSlotCount: fixture.machine.options.RetryWindow,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		if err := batch.Put(sessionKey[:], header); err != nil {
			return err
		}
		for offset := uint64(0); offset < window; offset++ {
			sequence := low + offset
			slot := uint16((sequence - 1) % window)
			slotKey, keyErr := SessionSlotStorageKey(digest, slot)
			if keyErr != nil {
				return keyErr
			}
			fingerprint := replication.Digest{byte(offset + 1)}
			logicalDigest := [32]byte{byte(offset + 1)}
			record, recordErr := AppendSessionSlot(nil, SessionSlot{
				Slot: slot, SessionDigest: digest, ClientEpoch: 1,
				ClientSequence: sequence, AppliedSequence: offset + 2,
				Fingerprint: fingerprint, LogicalCommandDigest: logicalDigest,
				ResultCode: ResultApplied, ReplicaSetVersion: 1,
				ActivePolicyGeneration: fixture.binding.ActivePolicyGeneration,
				ProtectionEpoch:        fixture.binding.ProtectionEpoch,
				RoutingVersion:         fixture.binding.RoutingVersion,
				RouteGeneration:        fixture.binding.RouteGeneration,
			})
			if recordErr != nil {
				return recordErr
			}
			if err := batch.Put(slotKey[:], record); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	machine, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen near-terminal session: %v", err)
	}

	ordinary := commandValue(fixture.binding, math.MaxUint64)
	ordinary.AckThrough = high
	ordinaryBytes := encodeCommand(t, ordinary)
	if err := machine.AdmitCommand(ordinaryBytes); !errors.Is(err, ErrSessionSequence) {
		t.Fatalf("terminal ordinary admission = %v", err)
	}

	retire := commandValue(fixture.binding, math.MaxUint64)
	retire.Kind = replication.CommandSessionRetire
	retire.AckThrough = high
	retire.Mutations = nil
	retire.RoutingVersion++
	retire.RouteGeneration++
	staleBytes := encodeCommand(t, retire)
	if err := machine.AdmitCommand(staleBytes); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("terminal stale retirement admission = %v", err)
	}
	publication, err := machine.ApplyNormal(normalMeta(window+2), staleBytes)
	if err != nil || publication.Applied != window+2 {
		t.Fatalf("terminal stale retirement apply = %+v,%v", publication, err)
	}
	if _, err := machine.LookupCompletion(staleBytes); !errors.Is(err, ErrCompletionNotFound) {
		t.Fatalf("terminal stale retirement lookup = %v", err)
	}

	machine, err = Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen terminal refusal: %v", err)
	}
	retire.RoutingVersion = fixture.binding.RoutingVersion
	retire.RouteGeneration = fixture.binding.RouteGeneration
	retireBytes := encodeCommand(t, retire)
	if err := machine.AdmitCommand(retireBytes); err != nil {
		t.Fatalf("refreshed terminal retirement admission: %v", err)
	}
	if _, err := machine.ApplyNormal(normalMeta(window+3), retireBytes); err != nil {
		t.Fatal(err)
	}
	lookup, err := machine.LookupCompletion(retireBytes)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != ResultSessionRetired {
		t.Fatalf("terminal retirement completion = %+v,%v", completion, err)
	}

	machine, err = Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen terminal retirement: %v", err)
	}
	nextEpoch := commandValue(fixture.binding, 1)
	nextEpoch.ClientEpoch = 2
	nextEpochBytes := encodeCommand(t, nextEpoch)
	if err := machine.AdmitCommand(nextEpochBytes); err != nil {
		t.Fatalf("next epoch admission: %v", err)
	}
	if _, err := machine.ApplyNormal(normalMeta(window+4), nextEpochBytes); err != nil {
		t.Fatal(err)
	}
}

func TestRetryHomeConflictReturnsOriginalUnlessRetryRetired(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	original := commandValue(fixture.binding, 1)
	original.RetryHome = replication.RetryHome{1, 2, 3, 4, 5, 6, 7, 8}
	originalBytes := encodeCommand(t, original)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), originalBytes); err != nil {
		t.Fatal(err)
	}
	want, err := fixture.machine.LookupCompletion(originalBytes)
	if err != nil {
		t.Fatal(err)
	}

	conflict := original
	conflict.RetryHome[0]++
	conflictBytes := encodeCommand(t, conflict)
	got, err := fixture.machine.LookupCompletion(conflictBytes)
	if !errors.Is(err, ErrRequestConflict) || !bytes.Equal(got.Bytes, want.Bytes) ||
		got.AppliedSequence != want.AppliedSequence {
		t.Fatalf("retry-home conflict = %+v,%v", got, err)
	}

	second := commandValue(fixture.binding, 2)
	second.RetryHome = original.RetryHome
	second.AckThrough = 1
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), encodeCommand(t, second)); err != nil {
		t.Fatal(err)
	}
	if got, err := fixture.machine.LookupCompletion(conflictBytes); !errors.Is(err, ErrRetryRetired) ||
		len(got.Bytes) != 0 {
		t.Fatalf("retired retry-home conflict = %+v,%v", got, err)
	}
}

func TestLogicalCommandDigestIgnoresAckAndMutableRoutingOnly(t *testing.T) {
	binding := testBinding()
	base := commandValue(binding, 1)
	encoded := encodeCommand(t, base)
	view, _ := replication.OpenCommand(encoded)
	want := LogicalCommandDigest(view)

	retry := base
	retry.AckThrough = 0
	retry.ReplicaSetVersion++
	retry.RoutingVersion++
	retry.RouteGeneration++
	retryBytes := encodeCommand(t, retry)
	retryView, _ := replication.OpenCommand(retryBytes)
	if got := LogicalCommandDigest(retryView); got != want {
		t.Fatalf("mutable routing changed logical digest: %x != %x", got, want)
	}

	changed := base
	changed.Mutations = append([]replication.Mutation(nil), base.Mutations...)
	changed.Mutations[0].Value = []byte(`{"n":2}`)
	changedBytes := encodeCommand(t, changed)
	changedView, _ := replication.OpenCommand(changedBytes)
	if got := LogicalCommandDigest(changedView); got == want {
		t.Fatal("mutation bytes did not change logical digest")
	}
}

func TestCompactSessionSlotReconstructsCanonicalOriginalCompletion(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	original := commandValue(fixture.binding, 1)
	originalBytes := encodeCommand(t, original)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), originalBytes); err != nil {
		t.Fatal(err)
	}
	view, err := replication.OpenCommand(originalBytes)
	if err != nil {
		t.Fatal(err)
	}
	want, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID:              view.ClusterID,
		ClusterIncarnation:     view.ClusterIncarnation,
		TopologyRecoveryEpoch:  view.TopologyRecoveryEpoch,
		Distribution:           view.Distribution,
		Shard:                  view.Shard,
		AllocationGeneration:   view.AllocationGeneration,
		ShardIncarnation:       view.ShardIncarnation,
		GroupID:                view.GroupID,
		ReplicaSetVersion:      view.ReplicaSetVersion,
		ActivePolicyGeneration: view.ActivePolicyGeneration,
		ProtectionEpoch:        view.ProtectionEpoch,
		RoutingVersion:         view.RoutingVersion,
		RouteGeneration:        view.RouteGeneration,
		Tenant:                 view.Tenant,
		ClientID:               view.ClientID,
		ClientEpoch:            view.ClientEpoch,
		ClientSequence:         view.ClientSequence,
		Fingerprint:            view.Fingerprint,
		RetryHome:              view.RetryHome,
		AppliedSequence:        2,
		ResultCode:             ResultApplied,
		ResultFormat:           ResultFormatMutation,
		Storage:                replication.CompletionInline,
		ResultDigest: replication.CompletionResultDigest(
			ResultApplied, ResultFormatMutation, nil,
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := fixture.machine.LookupCompletion(originalBytes)
	if err != nil || !bytes.Equal(first.Bytes, want) {
		t.Fatalf("canonical completion mismatch: lookup=%x want=%x err=%v", first.Bytes, want, err)
	}

	refreshed := original
	refreshed.ReplicaSetVersion++
	refreshed.ActivePolicyGeneration++
	refreshed.ProtectionEpoch++
	refreshed.OwnershipEpoch++
	refreshed.SchemaGeneration++
	refreshed.RoutingVersion++
	refreshed.RouteGeneration++
	refreshedBytes := encodeCommand(t, refreshed)
	if err := fixture.machine.AdmitCommand(refreshedBytes); err != nil {
		t.Fatalf("refreshed duplicate admission: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), refreshedBytes); err != nil {
		t.Fatalf("refreshed duplicate apply: %v", err)
	}
	duplicate, err := fixture.machine.LookupCompletion(refreshedBytes)
	if err != nil || !bytes.Equal(duplicate.Bytes, want) || duplicate.AppliedSequence != 2 {
		t.Fatalf("refreshed duplicate completion=%x applied=%d err=%v",
			duplicate.Bytes, duplicate.AppliedSequence, err)
	}
}
