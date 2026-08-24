package raftservice

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestOwnerRejectsBeforeSerializedHostLaneStarts(t *testing.T) {
	registry, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: 1, MaxOutstandingIdentities: 1,
		MaxOutstandingAttempts: 1, MaxWaiters: 1,
		MaxAttemptsPerIdentity:     1,
		MaxRetainedCompletionBytes: replication.MaxEmptyResultCompletionEnvelopeBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	host, err := registry.NewHost(multiraft.Limits{
		MaxGroups: 1, MaxQueueItems: 1, MaxQueueBytes: raftmodel.MaxInboundMessageBytes,
		MaxGroupItems: 1, MaxGroupBytes: raftmodel.MaxInboundMessageBytes,
		MaxOutboxItems: 1, MaxOutboxBytes: raftmodel.MaxInboundMessageBytes,
		MaxPendingTicks: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	identity := raftmember.RuntimeIdentity{
		Group: peerServerTestGroup(), AllocationGeneration: 1, MemberID: 1,
		StoreID: [16]byte{1}, NodeIncarnation: 1,
	}
	owner, err := NewOwner(Options{
		Registry: registry, Host: host, Members: []raftmember.RuntimeIdentity{identity},
		CommandFences: []CommandFence{{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
			OwnershipEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{1},
			RoutingVersion:         1, RouteGeneration: 1,
		}},
		Limits: Limits{
			MaxIngressItems: 1, MaxIngressBytes: 1, MaxPendingOutboundBytes: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.Probe(context.Background(), identity.Group)
	if !errors.Is(err, ErrOwnerClosed) {
		t.Fatalf("pre-Run Probe = %v", err)
	}
}
