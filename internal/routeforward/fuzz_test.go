package routeforward

import (
	"bytes"
	"testing"
)

func FuzzCommandCanonical(f *testing.F) {
	authority := testDigest(10)
	entry, _ := testEntry(f, TopologyMove, 2)
	key := EntryKey(entry)
	clearance := Clearance{
		Key: key, CatalogGeneration: 23, RouteGateEpoch: 2, RouteGateRevision: 7,
		OldestRetryApplied: 51, AuthorityRevision: 3,
		GateCertificate: testDigest(101), RetryCertificate: testDigest(102),
	}
	for _, command := range []Command{
		testPublish(authority, 1, entry),
		testActivate(authority, key, 2),
		{
			Operation: OperationPrune, Authority: authority, AuthorityEpoch: 1,
			ExpectedRevision: 3, Key: key, Clearance: clearance,
		},
		{
			Operation: OperationCompactRetired, Authority: authority, AuthorityEpoch: 1,
			NextAuthorityEpoch: 2, ExpectedRevision: 4, Key: compactKey(authority, 2),
		},
	} {
		raw, err := AppendCommand(nil, command)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		command, err := OpenCommand(raw)
		if err != nil {
			return
		}
		reencoded, err := AppendCommand(nil, command)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(reencoded, raw) {
			t.Fatal("accepted forwarding command has more than one encoding")
		}
	})
}

func FuzzSnapshotCanonical(f *testing.F) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 8)
	entry, _ := testEntry(f, TopologySplit, 2)
	machine.Apply(testPublish(authority, 1, entry))
	machine.Apply(testActivate(authority, EntryKey(entry), 2))
	total, _ := SnapshotBytes(1, 0)
	raw, err := AppendSnapshot(
		make([]byte, 0, total), machine, make([]LiveRecord, 1), nil,
	)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Fuzz(func(t *testing.T, encoded []byte) {
		restored, err := OpenSnapshot(encoded, 8)
		if err != nil {
			return
		}
		total, ok := SnapshotBytes(restored.live, restored.tombstones)
		if !ok {
			t.Fatal("accepted unbounded snapshot")
		}
		reencoded, err := AppendSnapshot(
			make([]byte, 0, total), restored,
			make([]LiveRecord, restored.live), make([]TombstoneRecord, restored.tombstones),
		)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(reencoded, encoded) {
			t.Fatal("accepted forwarding snapshot has more than one encoding")
		}
	})
}

func FuzzResolveNeverRewritesOldCommand(f *testing.F) {
	authority := testDigest(10)
	machine, _ := NewMachine(authority, 1, 8)
	entry, exact := testEntry(f, TopologyReplicaReplacement, 2)
	key := EntryKey(entry)
	machine.Apply(testPublish(authority, 1, entry))
	machine.Apply(testActivate(authority, key, 2))
	f.Add(exact)
	f.Fuzz(func(t *testing.T, candidate []byte) {
		decision, reason := machine.Resolve(key, candidate, ReadCut{
			Authority: authority, AuthorityEpoch: 1, AppliedRevision: 3, ReadIndex: 1,
			CatalogGeneration: 20, TargetApplied: 60,
		})
		if reason != ReasonActivated {
			return
		}
		if !bytes.Equal(candidate, exact) || !bytes.Equal(decision.OriginalCommand, candidate) ||
			len(candidate) == 0 || &decision.OriginalCommand[0] != &candidate[0] ||
			cap(decision.OriginalCommand) != len(candidate) {
			t.Fatal("forwarder accepted or produced bytes other than the exact old command")
		}
	})
}
