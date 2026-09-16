package raftservice

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type testRouteReleaseReceiptSource struct {
	command []byte
	cap     int
	result  replicatedstate.CompletionLookup
}

func (source *testRouteReleaseReceiptSource) RouteReleaseReceiptReadInto(
	command, dst []byte,
) (replicatedstate.CompletionLookup, error) {
	source.command = bytes.Clone(command)
	source.cap = cap(dst)
	copy(dst[:len(source.result.Bytes)], source.result.Bytes)
	result := source.result
	result.Bytes = dst[:len(source.result.Bytes)]
	return result, nil
}

func routeReleaseReceiptTestCommand(t *testing.T) []byte {
	t.Helper()
	group := peerServerTestGroup()
	gate, err := routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationReleaseShared,
		Epoch:     1,
		Identity:  routegate.Identity{1},
		Binding:   routegate.Binding{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	command := replication.Command{
		Kind:                   replication.CommandRouteGate,
		AuthorityClass:         replication.CommandAuthorityRouteSession,
		ClusterID:              replication.ID128(group.ClusterID),
		ClusterIncarnation:     replication.ID128(group.ClusterIncarnation),
		TopologyRecoveryEpoch:  group.TopologyRecoveryEpoch,
		Distribution:           "docs",
		Shard:                  "0000-ffff",
		AllocationGeneration:   3,
		ShardIncarnation:       replication.ID128(group.ShardIncarnation),
		GroupID:                replication.ID128(group.GroupID),
		ReplicaSetVersion:      7,
		ActivePolicyGeneration: 5,
		ProtectionEpoch:        6,
		OwnershipEpoch:         8,
		SchemaGeneration:       9,
		RoutingVersion:         10,
		RouteGeneration:        11,
		Tenant:                 []byte("tenant"),
		ClientID:               replication.ID128{7},
		ClientEpoch:            17,
		ClientSequence:         3,
		AckThrough:             2,
		Fingerprint:            replication.Digest(sha256.Sum256([]byte("release"))),
		RouteGate:              gate,
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestRouteReleaseReceiptReadUsesExactCommandAndQuorumFloor(t *testing.T) {
	command := routeReleaseReceiptTestCommand(t)
	completion := []byte("committed-release-completion")
	source := &testRouteReleaseReceiptSource{result: replicatedstate.CompletionLookup{
		Bytes: completion, AppliedSequence: 23,
	}}
	charge, ok := pointReadResponseCharge(replicatedstate.MaxRouteGateCompletionEnvelopeBytes)
	if !ok {
		t.Fatal("receipt response charge overflow")
	}
	owner := &Owner{
		started: true,
		ingress: make(chan ownerRequest, 1),
		limits:  Limits{MaxIngressItems: 1, MaxIngressBytes: int64(len(command)), MaxPendingReadItems: 1, MaxPendingReadBytes: charge},
	}
	go func() {
		request := <-owner.ingress
		if request.kind != requestReadRouteReleaseReceipt {
			t.Errorf("request kind = %d", request.kind)
		}
		if !bytes.Equal(request.read.routeReleaseCommand, command) {
			t.Errorf("owner command = %x, want %x", request.read.routeReleaseCommand, command)
		}
		owner.release(request.bytes)
		request.reply <- ownerReply{read: readAuthorization{
			routeReleaseReceipt: source, routeReleaseCommand: command, minimumApplied: 19,
		}}
	}()
	request := RouteReleaseReceiptReadRequest{
		Fence:      ServingFence{Group: peerServerTestGroup()},
		Capability: serviceauthz.CapabilityRequestLedger, Command: command,
		MinimumApplied: 7,
	}
	result, lease, err := owner.ReadRouteReleaseReceipt(t.Context(), request)
	if err != nil || lease == nil || result.Applied != 19 || result.AppliedSequence != 23 ||
		!bytes.Equal(result.Completion, completion) || !bytes.Equal(source.command, command) ||
		source.cap != replicatedstate.MaxRouteGateCompletionEnvelopeBytes {
		t.Fatalf("result=%+v source command=%x cap=%d err=%v", result, source.command, source.cap, err)
	}
	if _, next, err := owner.ReadRouteReleaseReceipt(t.Context(), request); !errors.Is(err, ErrPendingReadsFull) || next != nil {
		t.Fatalf("receipt budget = %v", err)
	}
	lease.Release()
	if owner.pendingReadItems != 0 || owner.pendingReadBytes != 0 {
		t.Fatal("receipt budget leaked")
	}
	for _, capability := range []serviceauthz.Capability{0, serviceauthz.CapabilityDataRead, serviceauthz.CapabilityDataWrite} {
		request.Capability = capability
		if got, next, err := owner.ReadRouteReleaseReceipt(t.Context(), request); !errors.Is(err, ErrRouteReleaseReceiptUnauthorized) || next != nil ||
			got.Applied != 0 || got.AppliedSequence != 0 || len(got.Completion) != 0 || got.State != (ServingState{}) {
			t.Fatalf("capability %x result=%+v lease=%v err=%v", capability, got, next, err)
		}
	}
}
