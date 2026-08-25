package replicatedstate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func sessionRenewal(
	binding Binding,
	sequence uint64,
	expected, next int64,
) replication.Command {
	command := commandValue(binding, sequence-1)
	command.Kind = replication.CommandSessionRenew
	command.Batches = nil
	command.ExpectedDeadlineUnixNano = expected
	command.NextDeadlineUnixNano = next
	return command
}

func sessionRevocation(
	binding Binding,
	sequence, ackThrough uint64,
	expected int64,
) replication.Command {
	command := commandValue(binding, sequence-1)
	command.Kind = replication.CommandSessionRevoke
	command.AckThrough = ackThrough
	command.Batches = nil
	command.ExpectedDeadlineUnixNano = expected
	command.NextDeadlineUnixNano = 0
	return command
}

func requireSessionResult(
	t testing.TB,
	machine *Machine,
	command []byte,
	want uint32,
) CompletionLookup {
	t.Helper()
	lookup, err := machine.LookupCompletion(command)
	if err != nil {
		t.Fatalf("lookup session result: %v", err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != want {
		t.Fatalf("session result = %+v, %v; want code %d", completion, err, want)
	}
	return lookup
}

func TestSessionLeaseOpenRenewCASAndStaleFence(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)
	lease, err := fixture.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.ClientEpoch != epoch || lease.HighSequence != 1 ||
		lease.AckThrough != 0 || lease.LeaseDeadlineUnixNano != testSessionLeaseDeadlineUnixNano ||
		lease.Status != SessionActive || lease.TerminalResult != 0 {
		t.Fatalf("opened lease = %+v, %v", lease, err)
	}

	firstDeadline := testSessionLeaseDeadlineUnixNano
	secondDeadline := firstDeadline + 1_000
	renew := sessionRenewal(fixture.binding, 2, firstDeadline, secondDeadline)
	renewBytes := encodeCommand(t, renew)
	if err := fixture.machine.AdmitCommand(renewBytes); err != nil {
		t.Fatalf("renew admission: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), renewBytes); err != nil {
		t.Fatal(err)
	}
	original := requireSessionResult(t, fixture.machine, renewBytes, ResultSessionRenewed)
	lease, err = fixture.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 2 ||
		lease.LeaseDeadlineUnixNano != secondDeadline || lease.Status != SessionActive {
		t.Fatalf("renewed lease = %+v, %v", lease, err)
	}

	// AckThrough is excluded from logical request identity, so an exact unknown-
	// commit retry can advance the cumulative acknowledgement without replacing
	// the retained completion.
	ackRetry := renew
	ackRetry.AckThrough = 1
	ackRetryBytes := encodeCommand(t, ackRetry)
	if err := fixture.machine.AdmitCommand(ackRetryBytes); err != nil {
		t.Fatalf("renew retry admission: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), ackRetryBytes); err != nil {
		t.Fatal(err)
	}
	retried := requireSessionResult(t, fixture.machine, ackRetryBytes, ResultSessionRenewed)
	if retried.AppliedSequence != original.AppliedSequence ||
		!bytes.Equal(retried.Bytes, original.Bytes) {
		t.Fatalf("renew retry replaced completion: original=%+v retried=%+v", original, retried)
	}

	thirdDeadline := secondDeadline + 1_000
	stale := sessionRenewal(fixture.binding, 3, secondDeadline, thirdDeadline)
	stale.AckThrough = 2
	stale.RoutingVersion++
	stale.RouteGeneration++
	staleBytes := encodeCommand(t, stale)
	if err := fixture.machine.AdmitCommand(staleBytes); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("stale renew admission = %v, want ErrStaleCommand", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), staleBytes); err != nil {
		t.Fatal(err)
	}
	requireSessionResult(t, fixture.machine, staleBytes, ResultStaleFence)
	lease, err = fixture.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 3 || lease.AckThrough != 2 ||
		lease.LeaseDeadlineUnixNano != secondDeadline || lease.Status != SessionActive {
		t.Fatalf("stale renewal changed lease = %+v, %v", lease, err)
	}

	mismatch := sessionRenewal(fixture.binding, 4, firstDeadline, thirdDeadline)
	mismatch.AckThrough = 2
	mismatchBytes := encodeCommand(t, mismatch)
	if err := fixture.machine.AdmitCommand(mismatchBytes); !errors.Is(err, ErrSessionLeaseDeadline) {
		t.Fatalf("mismatched renew admission = %v, want ErrSessionLeaseDeadline", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(6), mismatchBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.LookupCompletion(mismatchBytes); !errors.Is(err, ErrCompletionNotFound) {
		t.Fatalf("mismatched renew lookup = %v, want ErrCompletionNotFound", err)
	}
	lease, err = fixture.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 3 || lease.LeaseDeadlineUnixNano != secondDeadline {
		t.Fatalf("mismatched renewal changed lease = %+v, %v", lease, err)
	}
}

func TestSessionRenewSurvivesReopenAndContinuesCAS(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)
	secondDeadline := testSessionLeaseDeadlineUnixNano + 1_000
	first := sessionRenewal(
		fixture.binding, 2, testSessionLeaseDeadlineUnixNano, secondDeadline,
	)
	firstBytes := encodeCommand(t, first)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), firstBytes); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen renewed lease: %v", err)
	}
	requireSessionResult(t, reopened, firstBytes, ResultSessionRenewed)
	lease, err := reopened.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 2 ||
		lease.LeaseDeadlineUnixNano != secondDeadline || lease.Status != SessionActive {
		t.Fatalf("reopened renewed lease = %+v, %v", lease, err)
	}
	thirdDeadline := secondDeadline + 1_000
	second := sessionRenewal(fixture.binding, 3, secondDeadline, thirdDeadline)
	second.AckThrough = 2
	secondBytes := encodeCommand(t, second)
	if _, err := reopened.ApplyNormal(normalMeta(4), secondBytes); err != nil {
		t.Fatal(err)
	}
	requireSessionResult(t, reopened, secondBytes, ResultSessionRenewed)
	lease, err = reopened.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 3 || lease.AckThrough != 2 ||
		lease.LeaseDeadlineUnixNano != thirdDeadline || lease.Status != SessionActive {
		t.Fatalf("continued renewed lease = %+v, %v", lease, err)
	}
}

func TestSessionRenewWinningSequenceFencesDelayedRevoke(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)
	deadline := testSessionLeaseDeadlineUnixNano
	delayed := sessionRevocation(fixture.binding, 2, 1, deadline)
	delayedBytes := encodeCommand(t, delayed)
	renewedDeadline := deadline + 1_000
	renew := sessionRenewal(fixture.binding, 2, deadline, renewedDeadline)
	renewBytes := encodeCommand(t, renew)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), renewBytes); err != nil {
		t.Fatal(err)
	}
	original := requireSessionResult(t, fixture.machine, renewBytes, ResultSessionRenewed)
	conflict, err := fixture.machine.LookupCompletion(delayedBytes)
	if !errors.Is(err, ErrRequestConflict) || conflict.AppliedSequence != original.AppliedSequence ||
		!bytes.Equal(conflict.Bytes, original.Bytes) {
		t.Fatalf("renew/revoke sequence conflict = %+v, %v", conflict, err)
	}
	lease, err := fixture.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 2 || lease.Status != SessionActive ||
		lease.LeaseDeadlineUnixNano != renewedDeadline {
		t.Fatalf("delayed revoke overrode renewal = %+v, %v", lease, err)
	}
}

func TestDelayedSessionRevokeCannotSealNewerActivity(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)
	deadline := testSessionLeaseDeadlineUnixNano
	delayed := sessionRevocation(fixture.binding, 2, 1, deadline)
	delayedBytes := encodeCommand(t, delayed)

	activity := commandValue(fixture.binding, 1)
	activityBytes := encodeCommand(t, activity)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), activityBytes); err != nil {
		t.Fatal(err)
	}
	original := requireSessionResult(t, fixture.machine, activityBytes, ResultApplied)
	if err := fixture.machine.AdmitCommand(delayedBytes); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("delayed revoke admission = %v, want ErrRequestConflict", err)
	}
	conflict, err := fixture.machine.LookupCompletion(delayedBytes)
	if !errors.Is(err, ErrRequestConflict) || conflict.AppliedSequence != original.AppliedSequence ||
		!bytes.Equal(conflict.Bytes, original.Bytes) {
		t.Fatalf("delayed revoke conflict = %+v, %v", conflict, err)
	}
	lease, err := fixture.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 2 || lease.Status != SessionActive ||
		lease.LeaseDeadlineUnixNano != deadline {
		t.Fatalf("delayed revoke sealed newer activity: %+v, %v", lease, err)
	}

	fresh := sessionRevocation(fixture.binding, 3, 2, deadline)
	freshBytes := encodeCommand(t, fresh)
	if err := fixture.machine.AdmitCommand(freshBytes); err != nil {
		t.Fatalf("fresh revoke admission: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), freshBytes); err != nil {
		t.Fatal(err)
	}
	requireSessionResult(t, fixture.machine, freshBytes, ResultSessionRevoked)
	lease, err = fixture.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 3 || lease.AckThrough != 2 ||
		lease.Status != SessionRetired || lease.LeaseDeadlineUnixNano != 0 ||
		lease.TerminalResult != ResultSessionRevoked {
		t.Fatalf("revoked lease = %+v, %v", lease, err)
	}

	later := commandValue(fixture.binding, 3)
	later.AckThrough = 2
	laterBytes := encodeCommand(t, later)
	if err := fixture.machine.AdmitCommand(laterBytes); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("post-revoke activity admission = %v, want ErrSessionRetired", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), laterBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.LookupCompletion(laterBytes); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("post-revoke activity lookup = %v, want ErrSessionRetired", err)
	}
}

func TestSessionRevokeCASAndStaleFencePreserveLease(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)
	deadline := testSessionLeaseDeadlineUnixNano

	mismatch := sessionRevocation(fixture.binding, 2, 1, deadline-1)
	mismatchBytes := encodeCommand(t, mismatch)
	if err := fixture.machine.AdmitCommand(mismatchBytes); !errors.Is(err, ErrSessionLeaseDeadline) {
		t.Fatalf("mismatched revoke admission = %v, want ErrSessionLeaseDeadline", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), mismatchBytes); err != nil {
		t.Fatal(err)
	}
	lease, err := fixture.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 1 || lease.Status != SessionActive ||
		lease.LeaseDeadlineUnixNano != deadline {
		t.Fatalf("mismatched revoke changed lease = %+v, %v", lease, err)
	}

	stale := sessionRevocation(fixture.binding, 2, 1, deadline)
	stale.RoutingVersion++
	stale.RouteGeneration++
	staleBytes := encodeCommand(t, stale)
	if err := fixture.machine.AdmitCommand(staleBytes); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("stale revoke admission = %v, want ErrStaleCommand", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), staleBytes); err != nil {
		t.Fatal(err)
	}
	requireSessionResult(t, fixture.machine, staleBytes, ResultStaleFence)
	lease, err = fixture.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 2 || lease.AckThrough != 1 ||
		lease.Status != SessionActive || lease.LeaseDeadlineUnixNano != deadline {
		t.Fatalf("stale revoke changed lease = %+v, %v", lease, err)
	}

	fresh := sessionRevocation(fixture.binding, 3, 2, deadline)
	freshBytes := encodeCommand(t, fresh)
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), freshBytes); err != nil {
		t.Fatal(err)
	}
	requireSessionResult(t, fixture.machine, freshBytes, ResultSessionRevoked)
}

func TestSessionRevokeUnknownCommitReopenRetiredAndRelease(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)
	revoke := sessionRevocation(
		fixture.binding, 2, 1, testSessionLeaseDeadlineUnixNano,
	)
	revokeBytes := encodeCommand(t, revoke)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), revokeBytes); err != nil {
		t.Fatal(err)
	}
	before := requireSessionResult(t, fixture.machine, revokeBytes, ResultSessionRevoked)

	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen revoked session: %v", err)
	}
	after := requireSessionResult(t, reopened, revokeBytes, ResultSessionRevoked)
	if before.AppliedSequence != after.AppliedSequence || !bytes.Equal(before.Bytes, after.Bytes) {
		t.Fatalf("reopen changed revoke completion: before=%+v after=%+v", before, after)
	}
	lease, err := reopened.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.Status != SessionRetired ||
		lease.TerminalResult != ResultSessionRevoked || lease.LeaseDeadlineUnixNano != 0 {
		t.Fatalf("reopened revoke state = %+v, %v", lease, err)
	}

	fresh := sessionRevocation(fixture.binding, 3, 2, testSessionLeaseDeadlineUnixNano)
	if err := reopened.AdmitCommand(encodeCommand(t, fresh)); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("fresh revoke of retired session = %v, want ErrSessionRetired", err)
	}
	releaseBytes := encodeCommand(t, sessionRelease(revoke))
	if _, err := reopened.ApplyNormal(normalMeta(4), releaseBytes); err != nil {
		t.Fatalf("release revoked session: %v", err)
	}
	if _, err := reopened.LookupCompletion(releaseBytes); !errors.Is(err, ErrSessionReleased) {
		t.Fatalf("release lookup = %v, want ErrSessionReleased", err)
	}
	if _, err := reopened.LookupCompletion(revokeBytes); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("released revoke lookup = %v, want ErrRetryRetired", err)
	}
	if _, err := reopened.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch); !errors.Is(err, ErrSessionReleased) {
		t.Fatalf("released lease lookup = %v, want ErrSessionReleased", err)
	}
}

func TestFreshSessionRevokeRefusesCooperativelyRetiredSession(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, prototype)
	retire := sessionRetirement(commandValue(fixture.binding, 1))
	retireBytes := encodeCommand(t, retire)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), retireBytes); err != nil {
		t.Fatal(err)
	}
	requireSessionResult(t, fixture.machine, retireBytes, ResultSessionRetired)
	revoke := sessionRevocation(
		fixture.binding, 3, 2, testSessionLeaseDeadlineUnixNano,
	)
	if err := fixture.machine.AdmitCommand(encodeCommand(t, revoke)); !errors.Is(err, ErrSessionRetired) {
		t.Fatalf("revoke retired admission = %v, want ErrSessionRetired", err)
	}
	lease, err := fixture.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, 2)
	if err != nil || lease.Status != SessionRetired ||
		lease.TerminalResult != ResultSessionRetired ||
		lease.LeaseDeadlineUnixNano != testSessionLeaseDeadlineUnixNano {
		t.Fatalf("cooperative retirement lease = %+v, %v", lease, err)
	}
}

func TestLookupSessionLeaseEpochOutcomes(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, prototype)
	if _, err := fixture.machine.LookupSessionLease(
		prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, 0,
	); !errors.Is(err, ErrSessionEpoch) {
		t.Fatalf("zero epoch lookup = %v, want ErrSessionEpoch", err)
	}
	if _, err := fixture.machine.LookupSessionLease(
		prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, 3,
	); !errors.Is(err, ErrSessionEpoch) {
		t.Fatalf("future epoch lookup = %v, want ErrSessionEpoch", err)
	}
	retire := sessionRetirement(commandValue(fixture.binding, 1))
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), encodeCommand(t, retire)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(
		normalMeta(4), encodeCommand(t, sessionRelease(retire)),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.LookupSessionLease(
		prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, 2,
	); !errors.Is(err, ErrSessionReleased) {
		t.Fatalf("released epoch lookup = %v, want ErrSessionReleased", err)
	}
	_, _, nextEpoch := applySessionOpen(t, fixture.machine, 5, prototype)
	if _, err := fixture.machine.LookupSessionLease(
		prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, 2,
	); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("older epoch lookup = %v, want ErrRetryRetired", err)
	}
	if _, err := fixture.machine.LookupSessionLease(
		prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, nextEpoch+1,
	); !errors.Is(err, ErrSessionEpoch) {
		t.Fatalf("newer epoch lookup = %v, want ErrSessionEpoch", err)
	}
}

func TestReopenRejectsRevokedLeaseDeadlineMismatch(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, prototype)
	revoke := sessionRevocation(
		fixture.binding, 2, 1, testSessionLeaseDeadlineUnixNano,
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), encodeCommand(t, revoke)); err != nil {
		t.Fatal(err)
	}
	digest := SessionKey(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID)
	key := SessionStorageKey(digest)
	header, found := rawSessionReleaseRow(t, fixture.system.Collection, key[:])
	if !found {
		t.Fatal("revoked session header is missing")
	}
	binary.LittleEndian.PutUint64(header[112:120], uint64(testSessionLeaseDeadlineUnixNano))
	sealRecord(header, sessionRecordChecksumDomain)
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		return batch.Put(key[:], header)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("reopen revoked nonzero deadline = %v, want ErrSessionCorrupt", err)
	}
}

func TestReopenRejectsRevokedTerminalResultMismatch(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, prototype)
	revoke := sessionRevocation(
		fixture.binding, 2, 1, testSessionLeaseDeadlineUnixNano,
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), encodeCommand(t, revoke)); err != nil {
		t.Fatal(err)
	}
	digest := SessionKey(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID)
	slotKey, err := SessionSlotStorageKey(digest, 1)
	if err != nil {
		t.Fatal(err)
	}
	slot, found := rawSessionReleaseRow(t, fixture.system.Collection, slotKey[:])
	if !found {
		t.Fatal("revocation slot is missing")
	}
	binary.LittleEndian.PutUint32(slot[140:144], ResultSessionRetired)
	sealRecord(slot, sessionSlotChecksumDomain)
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		return batch.Put(slotKey[:], slot)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("reopen revoked retirement result = %v, want ErrSessionCorrupt", err)
	}
}

func TestSessionRevokeDecisionSyncFaultRecoversAtomically(t *testing.T) {
	store := newPersistentSessionLifecycleStore(t, 4)
	defer store.close(t)
	if _, err := store.machine.InstallSnapshot(store.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(store.binding, 1)
	_, _, epoch := applySessionOpen(t, store.machine, 2, prototype)
	capture := newSessionLeaseCaptureStore(t, store.dir)
	defer capture.close()
	requiredDocuments, err := RequiredBundleTransactionDocuments(
		store.user.Limits.MaxDistinctMutations, store.machine.options.RetryWindow, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	store.machine.options.TxnLimits.MaxCollections = 3
	store.machine.options.TxnLimits.MaxDocuments = requiredDocuments
	store.machineOptions.TxnLimits.MaxCollections = 3
	store.machineOptions.TxnLimits.MaxDocuments = requiredDocuments
	if err := store.machine.BeginTransitionCapture(capture.encoder); err != nil {
		t.Fatalf("begin lease fault capture: %v", err)
	}
	revoke := sessionRevocation(
		store.binding, 2, 1, testSessionLeaseDeadlineUnixNano,
	)
	revokeBytes := encodeCommand(t, revoke)
	restore := durable.InstallTxnMarkerSyncFaultForFacadeTest()
	_, applyErr := store.machine.ApplyNormal(normalMeta(3), revokeBytes)
	restore()
	if !errors.Is(applyErr, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("revoke decision sync = %v, want ErrCommitOutcomeUnknown", applyErr)
	}
	reopenSessionLeaseStoreAfterUnknownOutcome(t, store, capture)
	lease, err := store.machine.LookupSessionLease(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID, epoch)
	if err != nil {
		t.Fatalf("lookup recovered revoke lease: %v", err)
	}
	if lease.HighSequence != 2 || lease.AckThrough != 1 ||
		lease.Status != SessionRetired || lease.LeaseDeadlineUnixNano != 0 ||
		lease.TerminalResult != ResultSessionRevoked {
		t.Fatalf("recovered revoke did not roll forward = %+v", lease)
	}
	if capture.collection.Len() != 2 {
		t.Fatalf("rolled-forward revoke retained %d capture rows, want header plus transition",
			capture.collection.Len())
	}
	requireSessionResult(t, store.machine, revokeBytes, ResultSessionRevoked)
}

func reopenSessionLeaseStoreAfterUnknownOutcome(
	t testing.TB,
	store *persistentSessionLifecycleStore,
	capture *sessionLeaseCaptureStore,
) {
	t.Helper()
	closeErr := errors.Join(
		store.log.Close(), store.system.Collection.Close(), store.user.Collection.Close(),
		capture.collection.Close(), store.systemFile.Close(), store.userFile.Close(),
		capture.file.Close(),
	)
	if closeErr != nil && !errors.Is(closeErr, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("close unknown-outcome store: %v", closeErr)
	}
	store.log = nil
	store.system.Collection = nil
	store.user.Collection = nil
	store.systemFile = nil
	store.userFile = nil
	capture.collection = nil
	capture.file = nil
	var err error
	store.systemFile, err = os.OpenFile(store.systemPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	store.userFile, err = os.OpenFile(store.userPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	capture.file, err = os.OpenFile(capture.path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collections, log, err := durable.OpenCollectionsWithTransactions(
		store.dir, durable.TxnLogOptions{}, []durable.TransactionCollectionOpen{
			{File: store.systemFile, Options: store.systemOptions},
			{File: store.userFile, Options: store.userOptions},
			{File: capture.file, Options: capture.options},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	store.system = systemTargetOf(collections[0])
	store.user = targetOf(collections[1])
	capture.collection = collections[2]
	capture.encoder = &sessionLeaseCapture{target: TransitionCaptureTarget{
		Name: "capture", Collection: capture.collection,
	}}
	store.log = log
	options := store.machineOptions
	options.TransitionCapture = capture.encoder
	store.machine, err = Open(
		store.binding, store.bootstrap, store.system,
		UserCollection{Name: "docs", Target: store.user}, store.log,
		options,
	)
	if err != nil {
		t.Fatal(err)
	}
}

type sessionLeaseCaptureStore struct {
	path       string
	file       *os.File
	collection *durable.Collection
	options    durable.Options
	encoder    *sessionLeaseCapture
}

func newSessionLeaseCaptureStore(
	t testing.TB,
	dir string,
) *sessionLeaseCaptureStore {
	t.Helper()
	store := &sessionLeaseCaptureStore{
		path: filepath.Join(dir, "lease-capture.vdb"),
	}
	var err error
	store.file, err = os.OpenFile(store.path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	store.collection, err = durable.Create(store.file, store.options)
	if err != nil {
		t.Fatal(err)
	}
	store.encoder = &sessionLeaseCapture{target: TransitionCaptureTarget{
		Name: "capture", Collection: store.collection,
	}}
	return store
}

func (s *sessionLeaseCaptureStore) close() {
	if s == nil {
		return
	}
	if s.collection != nil {
		_ = s.collection.Close()
		s.collection = nil
	}
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
}

type sessionLeaseCapture struct {
	target  TransitionCaptureTarget
	current uint64
	pending uint64
}

func (c *sessionLeaseCapture) Target() TransitionCaptureTarget { return c.target }

func (*sessionLeaseCapture) MaxEncodedBytes(TransitionCaptureBounds) (int, error) {
	return 64, nil
}

func (c *sessionLeaseCapture) Begin(
	state State,
	publish func(key, value []byte) error,
) error {
	if c == nil || c.target.Collection == nil || state.Applied == 0 {
		return ErrTransitionCapture
	}
	if c.target.Collection.Len() == 0 {
		var key [8]byte
		if publish == nil {
			return ErrTransitionCapture
		}
		if err := publish(key[:], []byte(`{"capture":true}`)); err != nil {
			return err
		}
	}
	c.current = state.Applied
	c.pending = 0
	return nil
}

func (c *sessionLeaseCapture) AppendTransition(
	dst []byte,
	transition CapturedTransition,
) ([]byte, error) {
	if c == nil || transition.Applied != c.current+1 || c.pending != 0 {
		return dst, ErrTransitionCapture
	}
	start := len(dst)
	dst = append(dst, `{"applied":`...)
	dst = strconv.AppendUint(dst, transition.Applied, 10)
	dst = append(dst, '}')
	c.pending = transition.Applied
	if len(dst)-start > 64 {
		return dst[:start], ErrTransitionCapture
	}
	return dst, nil
}

func (c *sessionLeaseCapture) Published(transition CapturedTransition) error {
	if c == nil || c.pending != transition.Applied || transition.Applied != c.current+1 {
		return ErrTransitionCapture
	}
	c.current = transition.Applied
	c.pending = 0
	return nil
}
