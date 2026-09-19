package main

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestRF3RetiredControlSettlesOnlyDurablyAuthorizedRetry(t *testing.T) {
	path := t.TempDir()
	journal, err := replicaaction.OpenFileJournal(path, 8)
	if err != nil {
		t.Fatal(err)
	}
	record := rf3RetirementRecoveryRecord(rf3RecoveryEnrollmentIntent())
	if err = journal.PublishReplicaAction(t.Context(), 0, record); err != nil {
		t.Fatal(err)
	}
	record.Revision, record.State = 2, replicaaction.RetirementAuthorized
	if err = journal.PublishReplicaAction(t.Context(), 1, record); err != nil {
		t.Fatal(err)
	}
	running := record
	running.Request.Operation = [32]byte{93}
	running.Revision, running.State = 1, replicaaction.Running
	if err = journal.PublishReplicaAction(t.Context(), 0, running); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}

	peer := rafttransport.PeerIdentity{Node: rafttransport.NodeID{81}, TrustDomain: rafttransport.TrustDomain{
		ClusterID: record.Request.Fence.Group.ClusterID, ClusterIncarnation: record.Request.Fence.Group.ClusterIncarnation}}
	registry, err := rafttransport.NewEmptyRegistry(rafttransport.NodeID{82}, peer.TrustDomain, rf3TransportRegistryLimits())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{Node: peer.Node, Capabilities: serviceauthz.CapabilityMembership}})
	if err != nil {
		t.Fatal(err)
	}
	for reopen := 0; reopen < 2; reopen++ {
		journal, err = replicaaction.OpenFileJournal(path, 8)
		if err != nil {
			t.Fatal(err)
		}
		mux, err := newRF3RetiredControlMux(journal, registry, policy, nil, func() time.Time { return time.Now().Add(time.Second) })
		if err != nil {
			t.Fatal(err)
		}
		request := record.Request
		for _, negative := range []struct {
			name    string
			request replicaaction.Request
			peer    rafttransport.PeerIdentity
		}{
			{"running has no removal proof", running.Request, peer},
			{"unknown operation", func() replicaaction.Request { r := request; r.Operation = [32]byte{94}; return r }(), peer},
			{"different storage", func() replicaaction.Request { r := request; r.Fence.StoreID[0]++; return r }(), peer},
			{"different command", func() replicaaction.Request { r := request; r.Fence.Command.OwnershipEpoch++; return r }(), peer},
			{"different principal", request, rafttransport.PeerIdentity{Node: rafttransport.NodeID{83}, TrustDomain: peer.TrustDomain}},
		} {
			t.Run(negative.name, func(t *testing.T) {
				raw, err := replicaaction.AppendRequest(nil, negative.request)
				if err != nil {
					t.Fatal(err)
				}
				conn := &rf3ControlBufferConnection{input: bytes.NewReader(raw), identity: negative.peer}
				if err = mux.Serve(t.Context(), conn); !errors.Is(err, replicaaction.ErrUnauthorized) {
					t.Fatalf("unauthorized replay=%v", err)
				}
				if conn.output.Len() != 0 {
					t.Fatal("unauthorized request received completion")
				}
			})
		}
		raw, err := replicaaction.AppendRequest(nil, request)
		if err != nil {
			t.Fatal(err)
		}
		conn := &rf3ControlBufferConnection{input: bytes.NewReader(raw), identity: peer}
		if err = mux.Serve(t.Context(), conn); err != nil {
			t.Fatal(err)
		}
		if conn.output.Len() != replicaaction.ResponseBytes {
			t.Fatal("missing durable completion ACK")
		}
		observed, err := journal.ReadReplicaAction(t.Context(), request.Operation, request.Kind)
		if err != nil || observed.State != replicaaction.Complete || observed.Revision != 3 {
			t.Fatalf("settled record=%+v,%v", observed, err)
		}
		unchanged, err := journal.ReadReplicaAction(t.Context(), running.Request.Operation, running.Request.Kind)
		if err != nil || unchanged.State != replicaaction.Running || unchanged.Revision != 1 {
			t.Fatalf("unproven state advanced: %+v,%v", unchanged, err)
		}
		if _, err = registry.LocalMember(request.Fence.Group); err == nil {
			t.Fatal("retired control recreated a group")
		}
		if err = journal.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
