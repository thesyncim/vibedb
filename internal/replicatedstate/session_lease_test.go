package replicatedstate

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	command.Mutations = nil
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
	command.Mutations = nil
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
	lease, err := fixture.machine.LookupSessionLease(prototype.Tenant, prototype.ClientID, epoch)
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
	lease, err = fixture.machine.LookupSessionLease(prototype.Tenant, prototype.ClientID, epoch)
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
	lease, err = fixture.machine.LookupSessionLease(prototype.Tenant, prototype.ClientID, epoch)
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
	lease, err = fixture.machine.LookupSessionLease(prototype.Tenant, prototype.ClientID, epoch)
	if err != nil || lease.HighSequence != 3 || lease.LeaseDeadlineUnixNano != secondDeadline {
		t.Fatalf("mismatched renewal changed lease = %+v, %v", lease, err)
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
	lease, err := fixture.machine.LookupSessionLease(prototype.Tenant, prototype.ClientID, epoch)
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
	lease, err = fixture.machine.LookupSessionLease(prototype.Tenant, prototype.ClientID, epoch)
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
	lease, err := fixture.machine.LookupSessionLease(prototype.Tenant, prototype.ClientID, epoch)
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
	lease, err = fixture.machine.LookupSessionLease(prototype.Tenant, prototype.ClientID, epoch)
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
	lease, err := reopened.LookupSessionLease(prototype.Tenant, prototype.ClientID, epoch)
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
	if _, err := reopened.LookupSessionLease(prototype.Tenant, prototype.ClientID, epoch); !errors.Is(err, ErrSessionReleased) {
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
	lease, err := fixture.machine.LookupSessionLease(prototype.Tenant, prototype.ClientID, 2)
	if err != nil || lease.Status != SessionRetired ||
		lease.TerminalResult != ResultSessionRetired ||
		lease.LeaseDeadlineUnixNano != testSessionLeaseDeadlineUnixNano {
		t.Fatalf("cooperative retirement lease = %+v, %v", lease, err)
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
	digest := SessionKey(prototype.Tenant, prototype.ClientID)
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
