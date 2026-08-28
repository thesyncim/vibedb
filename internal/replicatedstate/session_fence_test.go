package replicatedstate

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestSessionFenceBatchedOldReferencesCoalesceAndCrashReopen(t *testing.T) {
	f := newNormalBatchFixture(t, 8, 1)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	sessions := openDistinctBatchSessions(t, f.machine, f.binding, 2, 3)
	applyAccountedTestFence(t, f.machine, 2, 4)
	if f.machine.state.HistoricalFenceCount != 1 || f.machine.state.HistoricalFenceSlots != 3 {
		t.Fatal("shared epoch references missing")
	}
	if err := f.group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	commands := make([][]byte, len(sessions))
	for i, session := range sessions {
		command := commandValue(f.machine.binding, 1)
		command.ClientID, command.ClientEpoch = session.ClientID, session.ClientEpoch
		commands[i] = encodeCommand(t, command)
	}
	entries := normalBatchEntries(f.machine.state.Applied+1, commands...)
	if n, _, err := f.machine.ApplyNormalBatch(entries, normalBatchWitnesses(entries)); err != nil || n != len(entries) {
		t.Fatalf("batch=%d %v", n, err)
	}
	if f.machine.state.HistoricalFenceCount != 0 || f.machine.state.HistoricalFenceSlots != 0 {
		t.Fatal("batch failed to drain shared historical fence")
	}
	f = f.crashReopen(t)
	if f.machine.state.HistoricalFenceCount != 1 || f.machine.state.HistoricalFenceSlots != 3 {
		t.Fatal("uncertified batch lost old fence references")
	}
	if n, _, err := f.machine.ApplyNormalBatch(entries, normalBatchWitnesses(entries)); err != nil || n != len(entries) {
		t.Fatalf("replay batch=%d %v", n, err)
	}
	if err := f.group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	f = f.crashReopen(t)
	for _, raw := range commands {
		if _, err := f.machine.LookupCompletion(raw); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSessionFenceStaleResultsExcludedAndReclaimed(t *testing.T) {
	f := newMachineFixture(t)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, f.machine, 2, commandValue(f.binding, 1))
	stale := commandValue(f.binding, 1)
	stale.RoutingVersion++
	applySessionReleaseCommand(t, f.machine, 3, stale)
	if f.machine.state.UnfencedSessionSlots != 1 {
		t.Fatal("stale result not accounted")
	}
	applyAccountedTestFence(t, f.machine, 2, 3)
	if f.machine.state.HistoricalFenceSlots != 1 {
		t.Fatal("stale client fence became historical authority")
	}
	reopenFenceFixture(t, &f)
	for ordinal := uint64(2); ordinal <= uint64(f.machine.options.RetryWindow)+1; ordinal++ {
		command := commandValue(f.machine.binding, ordinal)
		applySessionReleaseCommand(t, f.machine, f.machine.state.Applied+1, command)
	}
	if f.machine.state.UnfencedSessionSlots != 0 || f.machine.state.HistoricalFenceCount != 0 {
		t.Fatal("overwritten stale/history references retained")
	}
	reopenFenceFixture(t, &f)
}

func TestSessionFenceNormalBatchAccountsGrowingAndShrinkingEnvelope(t *testing.T) {
	batched := newNormalBatchFixture(t, 8, 1)
	sequential := newNormalBatchFixture(t, 8, 1)
	var commands [][]byte
	for _, fixture := range []normalBatchFixture{batched, sequential} {
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		session := openDistinctBatchSessions(t, fixture.machine, fixture.binding, 2, 1)[0]
		if commands == nil {
			stale := session
			stale.RoutingVersion++
			fresh := session
			fresh.ClientSequence++
			commands = [][]byte{encodeCommand(t, stale), encodeCommand(t, fresh)}
		}
	}
	initial, err := AppendState(nil, batched.machine.state)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal, command := range commands {
		entries := normalBatchEntries(uint64(ordinal)+3, command)
		want, err := sequential.machine.ApplyNormal(entries[0].Meta, command)
		if err != nil {
			t.Fatal(err)
		}
		n, publication, err := batched.machine.ApplyNormalBatch(entries, normalBatchWitnesses(entries))
		if err != nil || n != 1 {
			t.Fatalf("batch %d changed envelope geometry: applied=%d err=%v", ordinal, n, err)
		}
		assertPublicationEqual(t, publication, want)
		encoded, err := AppendState(nil, batched.machine.state)
		if err != nil {
			t.Fatal(err)
		}
		wantBytes, _ := AppendState(nil, sequential.machine.state)
		if !bytes.Equal(encoded, wantBytes) || (len(encoded) > len(initial)) != (ordinal == 0) {
			t.Fatalf("batch %d state diverged from sequential encoding", ordinal)
		}
		if err := batched.group.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		batched = batched.crashReopen(t)
		got, err := batched.machine.LookupCompletion(command)
		wantCompletion, wantErr := sequential.machine.LookupCompletion(command)
		if err != nil || wantErr != nil || !bytes.Equal(got.Bytes, wantCompletion.Bytes) {
			t.Fatalf("reopened completion %d differs: %v %v", ordinal, err, wantErr)
		}
	}
}

// This helper isolates the accounting half of an already-authorized ownership
// publication. Actual jump admission and durable capture authorization are
// independently exercised by the rangesplit post-seal/reopen integration gate.
func applyAccountedTestFence(t *testing.T, m *Machine, routingJump, generationJump uint64) {
	t.Helper()
	index := m.state.Applied + 1
	v := testOwnershipTransition(m.binding, m.state.ReplicaSetVersion)
	v.ToRoutingVersion = m.binding.RoutingVersion + routingJump
	v.ToRouteGeneration = m.binding.RouteGeneration + generationJump
	raw, err := AppendOwnershipTransition(nil, v)
	if err != nil {
		t.Fatal(err)
	}
	next := m.nextState(normalMeta(index), RecordOwnership, normalEntryDigest(normalMeta(index), raw))
	next.Binding.OwnershipEpoch = v.ToOwnershipEpoch
	next.Binding.RoutingVersion = v.ToRoutingVersion
	next.Binding.RouteGeneration = v.ToRouteGeneration
	rows, err := archiveSessionFence(m.state, &next)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.persistTransitionRows(next, nil, commandPlan{}, rows); err != nil {
		t.Fatal(err)
	}
}

func reopenFenceFixture(t *testing.T, f *machineFixture) {
	t.Helper()
	m, err := Open(f.binding, f.bootstrap, f.system, UserCollection{Name: "docs", Target: f.user}, f.log, f.machine.options)
	if err != nil {
		t.Fatal(err)
	}
	f.machine = m
}

func TestSessionFenceHistorySurvivesRepeatedUnequalJumpsAndDrains(t *testing.T) {
	f := newMachineFixture(t)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	_, oldest, epoch := applySessionOpen(t, f.machine, 2, commandValue(f.binding, 1))
	initial, err := f.machine.LookupCompletion(oldest)
	if err != nil {
		t.Fatal(err)
	}
	initial.Bytes = bytes.Clone(initial.Bytes)
	applyAccountedTestFence(t, f.machine, 2, 3)
	second := commandValue(f.machine.binding, 1)
	second.ClientID = id128(98)
	_, secondOpen, secondEpoch := applySessionOpen(t, f.machine, 4, second)
	applyAccountedTestFence(t, f.machine, 3, 2)
	if f.machine.state.HistoricalFenceCount != 2 || f.machine.state.HistoricalFenceSlots != 2 {
		t.Fatalf("history=%+v", f.machine.state)
	}
	reopenFenceFixture(t, &f)
	after, err := f.machine.LookupCompletion(oldest)
	if err != nil || !bytes.Equal(initial.Bytes, after.Bytes) {
		t.Fatalf("original result changed after two jumps: %v", err)
	}
	if _, err := f.machine.LookupCompletion(secondOpen); err != nil {
		t.Fatal(err)
	}
	for ordinal := uint64(1); ordinal <= uint64(f.machine.options.RetryWindow); ordinal++ {
		command := commandValue(f.machine.binding, ordinal)
		command.ClientEpoch = epoch
		applySessionReleaseCommand(t, f.machine, f.machine.state.Applied+1, command)
		if f.machine.state.HistoricalFenceCount > f.machine.state.SessionSlotCount {
			t.Fatal("history grew beyond retained slots")
		}
	}
	if f.machine.state.HistoricalFenceCount != 1 || f.machine.state.HistoricalFenceSlots != 1 {
		t.Fatal("last overwritten reference was not reclaimed")
	}
	reopenFenceFixture(t, &f)
	retire := commandValue(f.machine.binding, 1)
	retire.ClientID = id128(98)
	retire.ClientEpoch = secondEpoch
	retire = sessionRetirement(retire)
	applySessionReleaseCommand(t, f.machine, f.machine.state.Applied+1, retire)
	applySessionReleaseCommand(t, f.machine, f.machine.state.Applied+1, sessionRelease(retire))
	if f.machine.state.HistoricalFenceCount != 0 || f.machine.state.HistoricalFenceSlots != 0 {
		t.Fatal("released old fence not reclaimed")
	}
	reopenFenceFixture(t, &f)
	cut, err := f.system.Collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer cut.Close()
	if err := cut.RangeRaw(func(key, value []byte) error {
		if bytes.HasPrefix(key, sessionFencePrefix[:]) {
			t.Fatal("unreferenced fence row retained")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSessionFenceRejectsMissingExtraAndCorruptReferences(t *testing.T) {
	for _, kind := range []string{"missing", "extra", "count", "interval", "origin", "slot-fence"} {
		t.Run(kind, func(t *testing.T) {
			f := newMachineFixture(t)
			if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
				t.Fatal(err)
			}
			applySessionOpen(t, f.machine, 2, commandValue(f.binding, 1))
			applyAccountedTestFence(t, f.machine, 2, 3)
			key := sessionFenceKey(f.binding.RoutingVersion, f.binding.RouteGeneration)
			raw, found, err := f.system.Collection.AppendRaw(nil, key[:])
			if err != nil || !found {
				t.Fatal(err)
			}
			history, err := openSessionFence(raw)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.system.Collection.Update(func(batch *durable.WriteBatch) error {
				switch kind {
				case "missing":
					return batch.Delete(key[:])
				case "extra":
					key[17]++
				case "count":
					history.refs++
				case "interval":
					history.start = 2
				case "origin":
					history.origin[0]++
				case "slot-fence":
					command := commandValue(f.binding, 1)
					slotKey, _ := SessionSlotStorageKey(SessionKey(command.AuthorityClass, command.Tenant, command.ClientID), 0)
					slotRaw, _, err := f.system.Collection.AppendRaw(nil, slotKey[:])
					if err != nil {
						return err
					}
					view, err := OpenSessionSlot(slotRaw)
					if err != nil {
						return err
					}
					// The checksum is valid, but this exact fence never existed.
					copy := bytes.Clone(view.Bytes())
					copy[168]++
					sealRecord(copy, sessionSlotChecksumDomain)
					return batch.Put(slotKey[:], copy)
				}
				value, err := appendSessionFence(nil, history)
				if err != nil {
					return err
				}
				return batch.Put(key[:], value)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(f.binding, f.bootstrap, f.system, UserCollection{Name: "docs", Target: f.user}, f.log, f.machine.options); err == nil {
				t.Fatal("corrupt fence proof accepted")
			}
		})
	}
}

func TestSessionFenceCurrentPlanHasNoExtraRowsOrAllocations(t *testing.T) {
	state := State{SessionSlotCount: 1}
	plan := commandPlan{writeSlot: true, resultCode: ResultApplied}
	if got := testing.AllocsPerRun(1000, func() {
		out, err := accountSessionFencePlan(state, pointSnapshot{}, plan)
		if err != nil || len(out.systemRows) != 0 || out.fenceDelta != (sessionFenceDelta{}) {
			panic("current fence overhead")
		}
	}); got != 0 {
		t.Fatalf("allocations=%v", got)
	}
}
