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
	_, initialOpenBytes, initialEpoch := applySessionOpen(
		t, fixture.machine, 2, commandValue(fixture.binding, 1),
	)
	if initialEpoch != 2 || fixture.machine.state.SessionEpochHighWater != initialEpoch {
		t.Fatalf("initial session token/high-water = %d/%d, want 2",
			initialEpoch, fixture.machine.state.SessionEpochHighWater)
	}
	index := uint64(3)
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
	acknowledging.AckThrough = 10
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
	sequence11.AckThrough = 10
	apply(encodeCommand(t, sequence11))
	regressed := commandValue(fixture.binding, 12)
	regressed.AckThrough = 9
	regressedBytes := encodeCommand(t, regressed)
	if err := fixture.machine.AdmitCommand(regressedBytes); !errors.Is(err, ErrSessionAck) {
		t.Fatalf("regressed ack admission = %v", err)
	}
	apply(regressedBytes)
	if _, err := fixture.machine.LookupCompletion(regressedBytes); !errors.Is(err, ErrCompletionNotFound) {
		t.Fatalf("refused ack lookup = %v", err)
	}
	sequence12 := commandValue(fixture.binding, 12)
	sequence12.AckThrough = 11
	apply(encodeCommand(t, sequence12))

	retire := commandValue(fixture.binding, 13)
	retire.Kind = replication.CommandSessionRetire
	retire.AckThrough = 13
	retire.Batches = nil
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
	sameEpoch.AckThrough = 13
	sameEpochBytes := encodeCommand(t, sameEpoch)
	if err := fixture.machine.AdmitCommand(sameEpochBytes); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("retired epoch admission = %v", err)
	}
	apply(sameEpochBytes)

	releaseBytes := encodeCommand(t, sessionRelease(retire))
	apply(releaseBytes)
	if fixture.machine.state.SessionCount != 0 || fixture.machine.state.SessionSlotCount != 0 {
		t.Fatalf("release left a bounded session image: %+v", fixture.machine.state)
	}
	// Replaying the original open after exact release allocates a fresh token;
	// it cannot replay any old user mutations because Open carries none.
	apply(initialOpenBytes)
	nextEpoch := index - 1
	if nextEpoch <= initialEpoch || fixture.machine.state.SessionEpochHighWater != nextEpoch ||
		fixture.machine.state.SessionCount != 1 || fixture.machine.state.SessionSlotCount != 1 {
		t.Fatalf("replayed open did not create an empty fresh session: %+v", fixture.machine.state)
	}
	if _, err := fixture.machine.LookupCompletion(initialOpenBytes); err != nil {
		t.Fatalf("fresh open completion: %v", err)
	}
	nextCommand := commandValue(fixture.binding, 1)
	nextCommand.ClientEpoch = nextEpoch
	nextEpochBytes := encodeCommand(t, nextCommand)
	apply(nextEpochBytes)
	if _, err := fixture.machine.LookupCompletion(retireBytes); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("prior epoch retry = %v", err)
	}
	if fixture.machine.state.SessionCount != 1 ||
		fixture.machine.state.SessionSlotCount != 2 {
		t.Fatalf("epoch reuse grew state = %+v", fixture.machine.state)
	}

	gap := commandValue(fixture.binding, 3)
	gap.ClientEpoch = nextEpoch
	gapBytes := encodeCommand(t, gap)
	if err := fixture.machine.AdmitCommand(gapBytes); !errors.Is(err, ErrSessionSequence) {
		t.Fatalf("gap admission = %v", err)
	}
	apply(gapBytes)
	next := commandValue(fixture.binding, 2)
	next.ClientEpoch = nextEpoch
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
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	first := encodeCommand(t, commandValue(fixture.binding, 1))
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), first); err != nil {
		t.Fatal(err)
	}

	stale := commandValue(fixture.binding, 2)
	stale.Kind = replication.CommandSessionRetire
	stale.AckThrough = 2
	stale.Batches = nil
	stale.RoutingVersion++
	stale.RouteGeneration++
	staleBytes := encodeCommand(t, stale)
	if err := fixture.machine.AdmitCommand(staleBytes); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("stale retirement admission = %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), staleBytes); err != nil {
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
	next.AckThrough = 2
	nextBytes := encodeCommand(t, next)
	if err := fixture.machine.AdmitCommand(nextBytes); err != nil {
		t.Fatalf("live epoch was sealed: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), nextBytes); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalSessionRetireRetriesWithoutStrandingEpoch(t *testing.T) {
	testTerminalSessionCommandRetriesWithoutStrandingEpoch(
		t, replication.CommandSessionRetire, ResultSessionRetired,
	)
}

func TestTerminalSessionRevokeRetriesWithoutStrandingEpoch(t *testing.T) {
	testTerminalSessionCommandRetriesWithoutStrandingEpoch(
		t, replication.CommandSessionRevoke, ResultSessionRevoked,
	)
}

func testTerminalSessionCommandRetriesWithoutStrandingEpoch(
	t *testing.T,
	kind replication.CommandKind,
	wantResult uint32,
) {
	t.Helper()
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	for sequence := uint64(1); sequence <= uint64(fixture.machine.options.RetryWindow); sequence++ {
		if _, err := fixture.machine.ApplyNormal(
			normalMeta(sequence+2), encodeCommand(t, commandValue(fixture.binding, sequence)),
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
		ClientEpoch: 2, RetryHome: identity.RetryHome,
		AckThrough: low - 1, HighSequence: high, Status: SessionActive,
		LeaseDeadlineUnixNano: testSessionLeaseDeadlineUnixNano,
		RetryWindow:           fixture.machine.options.RetryWindow,
		PhysicalSlotCount:     fixture.machine.options.RetryWindow,
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
				Slot: slot, SessionDigest: digest, ClientEpoch: 2,
				ClientSequence: sequence, AppliedSequence: offset + 3,
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

	ordinary := commandValue(fixture.binding, math.MaxUint64-1)
	ordinary.AckThrough = high
	ordinaryBytes := encodeCommand(t, ordinary)
	if err := machine.AdmitCommand(ordinaryBytes); !errors.Is(err, ErrSessionSequence) {
		t.Fatalf("terminal ordinary admission = %v", err)
	}

	terminal := commandValue(fixture.binding, math.MaxUint64-1)
	terminal.Kind = kind
	terminal.AckThrough = high
	terminal.Batches = nil
	if kind == replication.CommandSessionRevoke {
		terminal.ExpectedDeadlineUnixNano = testSessionLeaseDeadlineUnixNano
	}
	terminal.RoutingVersion++
	terminal.RouteGeneration++
	staleBytes := encodeCommand(t, terminal)
	if err := machine.AdmitCommand(staleBytes); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("terminal stale retirement admission = %v", err)
	}
	publication, err := machine.ApplyNormal(normalMeta(window+3), staleBytes)
	if err != nil || publication.Applied != window+3 {
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
	terminal.RoutingVersion = fixture.binding.RoutingVersion
	terminal.RouteGeneration = fixture.binding.RouteGeneration
	terminalBytes := encodeCommand(t, terminal)
	if err := machine.AdmitCommand(terminalBytes); err != nil {
		t.Fatalf("refreshed terminal retirement admission: %v", err)
	}
	if _, err := machine.ApplyNormal(normalMeta(window+4), terminalBytes); err != nil {
		t.Fatal(err)
	}
	lookup, err := machine.LookupCompletion(terminalBytes)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != wantResult {
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
	release := sessionRelease(terminal)
	if _, err := machine.ApplyNormal(normalMeta(window+5), encodeCommand(t, release)); err != nil {
		t.Fatalf("terminal release: %v", err)
	}
	nextOpen := sessionOpenFor(commandValue(fixture.binding, 1))
	nextOpenBytes := encodeCommand(t, nextOpen)
	if err := machine.AdmitCommand(nextOpenBytes); err != nil {
		t.Fatalf("next session open admission: %v", err)
	}
	if _, err := machine.ApplyNormal(normalMeta(window+6), nextOpenBytes); err != nil {
		t.Fatal(err)
	}
	if machine.state.SessionEpochHighWater != window+6 {
		t.Fatalf("next session token = %d, want %d", machine.state.SessionEpochHighWater, window+6)
	}
}

func TestRetryHomeConflictReturnsOriginalUnlessRetryRetired(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	original := commandValue(fixture.binding, 1)
	original.RetryHome = replication.RetryHome{1, 2, 3, 4, 5, 6, 7, 8}
	applySessionOpen(t, fixture.machine, 2, original)
	originalBytes := encodeCommand(t, original)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), originalBytes); err != nil {
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
	second.AckThrough = 2
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), encodeCommand(t, second)); err != nil {
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
	changed.Batches = append([]replication.RelationMutationBatch(nil), base.Batches...)
	changed.Batches[0].Mutations = append([]replication.Mutation(nil), base.Batches[0].Mutations...)
	changed.Batches[0].Mutations[0].Value = []byte(`{"n":2}`)
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
	applySessionOpen(t, fixture.machine, 2, original)
	originalBytes := encodeCommand(t, original)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), originalBytes); err != nil {
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
		AppliedSequence:        3,
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
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), refreshedBytes); err != nil {
		t.Fatalf("refreshed duplicate apply: %v", err)
	}
	duplicate, err := fixture.machine.LookupCompletion(refreshedBytes)
	if err != nil || !bytes.Equal(duplicate.Bytes, want) || duplicate.AppliedSequence != 3 {
		t.Fatalf("refreshed duplicate completion=%x applied=%d err=%v",
			duplicate.Bytes, duplicate.AppliedSequence, err)
	}
}

func TestSessionOpenAllocatesOrderedTokensAndRetainsExactCompletion(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}

	first := commandValue(fixture.binding, 1)
	second := commandValue(fixture.binding, 1)
	second.ClientID = id128(88)
	firstOpen := encodeCommand(t, sessionOpenFor(first))
	secondOpen := encodeCommand(t, sessionOpenFor(second))
	// Admission is deliberately non-reserving: independently admitted opens
	// receive their definitive, collision-free tokens only in ordered apply.
	if err := fixture.machine.AdmitCommand(firstOpen); err != nil {
		t.Fatalf("first concurrent open admission: %v", err)
	}
	if err := fixture.machine.AdmitCommand(secondOpen); err != nil {
		t.Fatalf("second concurrent open admission: %v", err)
	}
	_, _, firstEpoch := applySessionOpen(t, fixture.machine, 2, first)
	_, _, secondEpoch := applySessionOpen(t, fixture.machine, 3, second)
	if firstEpoch != 2 || secondEpoch != 3 || firstEpoch == secondEpoch ||
		fixture.machine.state.SessionEpochHighWater != secondEpoch {
		t.Fatalf("ordered open tokens = %d,%d state=%+v", firstEpoch, secondEpoch, fixture.machine.state)
	}

	retained, err := fixture.machine.LookupCompletion(firstOpen)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), firstOpen); err != nil {
		t.Fatalf("exact open duplicate apply: %v", err)
	}
	duplicate, err := fixture.machine.LookupCompletion(firstOpen)
	if err != nil || duplicate.AppliedSequence != firstEpoch ||
		!bytes.Equal(duplicate.Bytes, retained.Bytes) {
		t.Fatalf("retained open completion = %+v, %v; want %+v", duplicate, err, retained)
	}

	conflict := sessionOpenFor(first)
	conflict.RetryHome[0] = 1
	conflictBytes := encodeCommand(t, conflict)
	if err := fixture.machine.AdmitCommand(conflictBytes); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflicting active open admission = %v, want ErrRequestConflict", err)
	}
	if original, err := fixture.machine.LookupCompletion(conflictBytes); !errors.Is(err, ErrRequestConflict) || !bytes.Equal(original.Bytes, retained.Bytes) {
		t.Fatalf("conflicting active open lookup = %+v, %v", original, err)
	}

	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen ordered session tokens: %v", err)
	}
	if reopened.state.SessionEpochHighWater != secondEpoch || reopened.state.SessionCount != 2 {
		t.Fatalf("reopened token fence = %+v", reopened.state)
	}
	for _, open := range [][]byte{firstOpen, secondOpen} {
		if _, err := reopened.LookupCompletion(open); err != nil {
			t.Fatalf("reopened open completion: %v", err)
		}
	}
}

func TestSessionOpenStaleFenceIsNeverStored(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	stale := commandValue(fixture.binding, 1)
	stale.RoutingVersion++
	stale.RouteGeneration++
	open := encodeCommand(t, sessionOpenFor(stale))
	if err := fixture.machine.AdmitCommand(open); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("stale open admission = %v, want ErrStaleCommand", err)
	}
	publication, err := fixture.machine.ApplyNormal(normalMeta(2), open)
	if err != nil || publication.Applied != 2 {
		t.Fatalf("committed stale open = %+v, %v", publication, err)
	}
	if _, err := fixture.machine.LookupCompletion(open); !errors.Is(err, ErrCompletionNotFound) {
		t.Fatalf("stale open lookup = %v, want ErrCompletionNotFound", err)
	}
	if fixture.machine.state.SessionCount != 0 || fixture.machine.state.SessionSlotCount != 0 ||
		fixture.machine.state.SessionEpochHighWater != 0 || fixture.system.Collection.Len() != 1 {
		t.Fatalf("stale open persisted state: %+v rows=%d",
			fixture.machine.state, fixture.system.Collection.Len())
	}
}

func TestSessionOpenRequiresReleaseAndReplayCreatesEmptyFreshSession(t *testing.T) {
	fixture := newSessionReleaseFixture(t, 1, 4)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	_, openBytes, oldEpoch := applySessionOpen(t, fixture.machine, 2, prototype)
	retirement := sessionRetirement(prototype)
	retirementBytes := applySessionReleaseCommand(t, fixture.machine, 3, retirement)

	freshWhileRetired := sessionOpenFor(prototype)
	freshWhileRetired.Fingerprint[0] ^= 0xff
	if err := fixture.machine.AdmitCommand(encodeCommand(t, freshWhileRetired)); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("open over retired image = %v, want ErrSessionRetired", err)
	}
	if err := fixture.machine.AdmitCommand(openBytes); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("exact retired open retry = %v, want ErrRetryRetired", err)
	}
	applySessionReleaseCommand(t, fixture.machine, 4, sessionRelease(retirement))
	if fixture.machine.state.SessionCount != 0 || fixture.machine.state.SessionSlotCount != 0 {
		t.Fatalf("release left session image: %+v", fixture.machine.state)
	}

	// The same old Open bytes are safe to replay: ordered apply assigns a new
	// token and Open has no mutation payload to resurrect.
	applySessionReleaseCommand(t, fixture.machine, 5, sessionOpenFor(prototype))
	if fixture.machine.state.SessionEpochHighWater != 5 || oldEpoch != 2 ||
		fixture.machine.state.SessionCount != 1 || fixture.machine.state.SessionSlotCount != 1 ||
		fixture.user.Collection.Len() != 0 {
		t.Fatalf("replayed open image = %+v user rows=%d",
			fixture.machine.state, fixture.user.Collection.Len())
	}
	if _, err := fixture.machine.LookupCompletion(retirementBytes); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("old retirement after fresh open = %v, want ErrRetryRetired", err)
	}

	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen fresh empty session: %v", err)
	}
	if reopened.state.SessionEpochHighWater != 5 || reopened.state.SessionCount != 1 ||
		reopened.state.SessionSlotCount != 1 {
		t.Fatalf("reopened fresh session token = %+v", reopened.state)
	}
}

func TestSessionOpenRetryRetiresAfterAckOrSlotEviction(t *testing.T) {
	t.Run("acknowledged", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		prototype := commandValue(fixture.binding, 1)
		_, openBytes, _ := applySessionOpen(t, fixture.machine, 2, prototype)
		prototype.AckThrough = 1
		applySessionReleaseCommand(t, fixture.machine, 3, prototype)
		if err := fixture.machine.AdmitCommand(openBytes); !errors.Is(err, ErrRetryRetired) {
			t.Fatalf("acknowledged open admission = %v, want ErrRetryRetired", err)
		}
		if _, err := fixture.machine.LookupCompletion(openBytes); !errors.Is(err, ErrRetryRetired) {
			t.Fatalf("acknowledged open lookup = %v, want ErrRetryRetired", err)
		}
	})

	t.Run("evicted", func(t *testing.T) {
		fixture := newSessionReleaseFixture(t, 4, 2)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		prototype := commandValue(fixture.binding, 1)
		_, openBytes, _ := applySessionOpen(t, fixture.machine, 2, prototype)
		applySessionReleaseCommand(t, fixture.machine, 3, prototype)
		applySessionReleaseCommand(t, fixture.machine, 4, commandValue(fixture.binding, 2))
		if err := fixture.machine.AdmitCommand(openBytes); !errors.Is(err, ErrRetryRetired) {
			t.Fatalf("evicted open admission = %v, want ErrRetryRetired", err)
		}
		if _, err := fixture.machine.LookupCompletion(openBytes); !errors.Is(err, ErrRetryRetired) {
			t.Fatalf("evicted open lookup = %v, want ErrRetryRetired", err)
		}
	})
}
