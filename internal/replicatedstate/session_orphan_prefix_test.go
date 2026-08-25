package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

// TestMissingSessionHeaderRejectsOrphanSlotPrefix proves every missing-header
// entry point performs the same direct ordered-prefix existence check. The slot
// value is a valid compact envelope; only the absent collision-verifiable
// header makes the image corrupt. No path may reinterpret it as a new session,
// a normal retry miss, or an already-completed release.
func TestMissingSessionHeaderRejectsOrphanSlotPrefix(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(testing.TB, *Machine, replication.Command) error
	}{
		{
			name: "session-open-apply",
			run: func(t testing.TB, machine *Machine, prototype replication.Command) error {
				_, err := machine.ApplyNormal(
					normalMeta(2), encodeCommand(t, sessionOpenFor(prototype)),
				)
				return err
			},
		},
		{
			name: "ordinary-admit",
			run: func(t testing.TB, machine *Machine, prototype replication.Command) error {
				return machine.AdmitCommand(encodeCommand(t, prototype))
			},
		},
		{
			name: "ordinary-lookup",
			run: func(t testing.TB, machine *Machine, prototype replication.Command) error {
				_, err := machine.LookupCompletion(encodeCommand(t, prototype))
				return err
			},
		},
		{
			name: "release-admit",
			run: func(t testing.TB, machine *Machine, prototype replication.Command) error {
				release := sessionRelease(sessionRetirement(prototype))
				return machine.AdmitCommand(encodeCommand(t, release))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSessionReleaseFixture(t, 1, 4)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			prototype := commandValue(fixture.binding, 1)
			open := sessionOpenFor(prototype)
			openBytes := encodeCommand(t, open)
			openView, err := replication.OpenCommand(openBytes)
			if err != nil {
				t.Fatal(err)
			}
			digest := SessionKey(open.AuthorityClass, open.Tenant, open.ClientID)
			slotKey, err := SessionSlotStorageKey(digest, 0)
			if err != nil {
				t.Fatal(err)
			}
			slotValue, err := AppendSessionSlot(nil, SessionSlot{
				Slot:                   0,
				SessionDigest:          digest,
				ClientEpoch:            2,
				ClientSequence:         1,
				AppliedSequence:        2,
				Fingerprint:            open.Fingerprint,
				LogicalCommandDigest:   LogicalCommandDigest(openView),
				ResultCode:             ResultSessionOpened,
				ReplicaSetVersion:      open.ReplicaSetVersion,
				ActivePolicyGeneration: open.ActivePolicyGeneration,
				ProtectionEpoch:        open.ProtectionEpoch,
				RoutingVersion:         open.RoutingVersion,
				RouteGeneration:        open.RouteGeneration,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
				return batch.Put(slotKey[:], slotValue)
			}); err != nil {
				t.Fatal(err)
			}
			wantState := mustRawSystemRow(t, fixture.system.Collection, stateKey)
			wantSlot := mustRawSystemRow(t, fixture.system.Collection, slotKey[:])

			if err := tc.run(t, fixture.machine, prototype); !errors.Is(err, ErrSessionCorrupt) {
				t.Fatalf("orphan-prefix path = %v, want ErrSessionCorrupt", err)
			}
			if _, err := fixture.machine.SessionCapacityState(); !errors.Is(err, ErrApplyPoisoned) {
				t.Fatalf("orphan-prefix machine state = %v, want ErrApplyPoisoned", err)
			}
			gotState := mustRawSystemRow(t, fixture.system.Collection, stateKey)
			gotSlot := mustRawSystemRow(t, fixture.system.Collection, slotKey[:])
			if fixture.system.Collection.Len() != 2 ||
				!bytes.Equal(gotState, wantState) || !bytes.Equal(gotSlot, wantSlot) {
				t.Fatalf("orphan-prefix path mutated image: rows=%d stateEqual=%t slotEqual=%t",
					fixture.system.Collection.Len(), bytes.Equal(gotState, wantState),
					bytes.Equal(gotSlot, wantSlot))
			}
		})
	}
}
