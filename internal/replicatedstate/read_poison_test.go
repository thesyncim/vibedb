package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func corruptRetainedCompletion(t testing.TB, fixture machineFixture, command []byte) {
	t.Helper()
	view, err := replication.OpenCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	digest := SessionKey(view.Tenant, view.ClientID)
	key, err := SessionSlotStorageKey(
		digest, uint16((view.ClientSequence-1)%uint64(fixture.machine.options.RetryWindow)),
	)
	if err != nil {
		t.Fatal(err)
	}
	document, found, err := fixture.system.Collection.AppendRaw(nil, key[:])
	if err != nil || !found {
		t.Fatalf("completion document = %q,%v,%v", document, found, err)
	}
	document = bytes.Clone(document)
	document[8] ^= 1
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		return batch.Put(key[:], document)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLookupAndAdmitCorruptionPoisonMachine(t *testing.T) {
	for _, operation := range []string{"lookup", "admit"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			command := encodeCommand(t, commandValue(fixture.binding, 1))
			applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
			if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
				t.Fatal(err)
			}
			corruptRetainedCompletion(t, fixture, command)
			var err error
			if operation == "lookup" {
				_, err = fixture.machine.LookupCompletion(command)
			} else {
				err = fixture.machine.AdmitCommand(command)
			}
			if !errors.Is(err, ErrSessionCorrupt) {
				t.Fatalf("%s error = %v", operation, err)
			}
			if _, err := fixture.machine.ApplyNormal(normalMeta(4), nil); !errors.Is(err, ErrApplyPoisoned) {
				t.Fatalf("post-%s apply error = %v", operation, err)
			}
		})
	}
}

func TestSnapshotIntegrityFailurePoisonsMachine(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		return batch.Put(stateKey, []byte(`"00"`))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.Snapshot("docs"); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("Snapshot error = %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), nil); !errors.Is(err, ErrApplyPoisoned) {
		t.Fatalf("post-Snapshot apply error = %v", err)
	}
}

func TestExplicitImageAuditDetectsDirectUserDivergence(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	before, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	beforeDigest, auditErr := before.CanonicalImageDigest()
	closeErr := before.Close()
	if auditErr != nil || closeErr != nil {
		t.Fatalf("initial audit = %v, close = %v", auditErr, closeErr)
	}
	if err := fixture.user.Collection.Update(func(batch *durable.WriteBatch) error {
		return batch.Put([]byte("outside"), []byte("null"))
	}); err != nil {
		t.Fatal(err)
	}
	after, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	afterDigest, auditErr := after.CanonicalImageDigest()
	closeErr = after.Close()
	if auditErr != nil || closeErr != nil || afterDigest == beforeDigest {
		t.Fatalf("changed audit = %x/%x, err = %v, close = %v",
			beforeDigest, afterDigest, auditErr, closeErr)
	}
}

func TestOrdinaryReadAndAdmissionRefusalsDoNotPoison(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	missing := encodeCommand(t, commandValue(fixture.binding, 1))
	if _, err := fixture.machine.LookupCompletion(missing); !errors.Is(err, ErrCompletionNotFound) {
		t.Fatalf("not-found error = %v", err)
	}
	staleValue := commandValue(fixture.binding, 1)
	staleValue.ReplicaSetVersion = 100
	stale := encodeCommand(t, staleValue)
	if err := fixture.machine.AdmitCommand(stale); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("stale admission error = %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), missing); err != nil {
		t.Fatalf("apply after ordinary refusals: %v", err)
	}
	conflictValue := commandValue(fixture.binding, 1)
	conflictValue.RetryHome[0] = 1
	conflict := encodeCommand(t, conflictValue)
	if _, err := fixture.machine.LookupCompletion(conflict); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict lookup error = %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), nil); err != nil {
		t.Fatalf("apply after conflict: %v", err)
	}
}
