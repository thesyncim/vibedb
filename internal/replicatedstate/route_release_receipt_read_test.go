package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
)

func TestRouteReleaseReceiptReadReturnsOnlyExactCommittedRelease(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	prototype.AuthorityClass = replication.CommandAuthorityRouteSession
	_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)
	acquire := commandValue(fixture.binding, 1)
	acquire.AuthorityClass, acquire.ClientEpoch, acquire.AckThrough = replication.CommandAuthorityRouteSession, epoch, 1
	applyRouteSessionGate(t, fixture.machine, 3, acquire, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1, Identity: routegate.Identity{1}, Binding: routegate.Binding{2},
	}, routegate.ReasonAcquired)
	release := commandValue(fixture.binding, 2)
	release.AuthorityClass, release.ClientEpoch, release.AckThrough = replication.CommandAuthorityRouteSession, epoch, 2
	releaseBytes := applyRouteSessionGate(t, fixture.machine, 4, release, routegate.Command{
		Operation: routegate.OperationReleaseShared, Epoch: 1, Identity: routegate.Identity{1}, Binding: routegate.Binding{2},
	}, routegate.ReasonReleased)

	dst := make([]byte, 0, MaxRouteGateCompletionEnvelopeBytes)
	lookup, err := fixture.machine.RouteReleaseReceiptReadInto(releaseBytes, dst)
	if err != nil || len(lookup.Bytes) == 0 || lookup.AppliedSequence == 0 {
		t.Fatalf("exact release receipt=%+v err=%v", lookup, err)
	}
	if !bytes.Equal(lookup.Bytes, dst[:len(lookup.Bytes)]) {
		t.Fatal("receipt did not use caller-owned destination")
	}

	releaseGate, err := routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationReleaseShared, Epoch: 1, Identity: routegate.Identity{1}, Binding: routegate.Binding{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	conflict := release
	conflict.Kind, conflict.Batches, conflict.RouteGate = replication.CommandRouteGate, nil, releaseGate
	conflict.Fingerprint = sha256.Sum256(append([]byte("route-session-gate/"), releaseGate...))
	conflict.Fingerprint[0]++
	conflictBytes := encodeCommand(t, conflict)
	conflictLookup, conflictErr := fixture.machine.RouteReleaseReceiptReadInto(conflictBytes, dst[:0])
	if !errors.Is(conflictErr, ErrRequestConflict) || len(conflictLookup.Bytes) != 0 {
		t.Fatalf("conflicting receipt lookup=%+v err=%v; bytes must be withheld on error", conflictLookup, conflictErr)
	}

	acquireGate, err := routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1, Identity: routegate.Identity{1}, Binding: routegate.Binding{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	acquire.Kind, acquire.Batches, acquire.RouteGate = replication.CommandRouteGate, nil, acquireGate
	acquire.Fingerprint = sha256.Sum256(append([]byte("route-session-gate/"), acquireGate...))
	acquireBytes := encodeCommand(t, acquire)
	wrongLookup, wrongErr := fixture.machine.RouteReleaseReceiptReadInto(acquireBytes, dst[:0])
	if wrongErr == nil || len(wrongLookup.Bytes) != 0 {
		t.Fatalf("non-release receipt lookup=%+v err=%v", wrongLookup, wrongErr)
	}
}
