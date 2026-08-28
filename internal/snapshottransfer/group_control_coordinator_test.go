package snapshottransfer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestGroupSourceControlRegistryAuthorizesCoordinatorBeforeJournal(t *testing.T) {
	request, descriptor := sourceControlFixture()
	otherRequest := request
	otherRequest.Group.GroupID[0]++
	otherRequest.Group.ShardIncarnation[0]++
	otherDescriptor := descriptor
	otherDescriptor.Group = otherRequest.Group
	registry, _, target, _ := twoGroupRegistry(t, descriptor, otherDescriptor)
	coordinator := rafttransport.PeerIdentity{TrustDomain: registry.TrustDomain(), Node: rafttransport.NodeID{0xf1}}
	if _, err := registry.Member(request.Group, coordinator.Node); err == nil {
		t.Fatal("coordinator must not be a Raft member or enrolled learner")
	}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	var revoked atomic.Bool
	exporter := &testSourceExporter{descriptor: descriptor}
	journal := &memorySourceJournal{records: make(map[[32]byte]SourceControlRecord)}
	service, err := NewSourceControlService(SourceControlOptions{
		Journal: journal, Exporter: exporter,
		Authorize: func(peer rafttransport.PeerIdentity, got SourceControlRequest) bool {
			return !revoked.Load() && peer == coordinator && got == request
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	otherExporter := &testSourceExporter{descriptor: otherDescriptor}
	otherService, err := NewSourceControlService(SourceControlOptions{
		Journal: &memorySourceJournal{records: make(map[[32]byte]SourceControlRecord)}, Exporter: otherExporter,
		Authorize: func(peer rafttransport.PeerIdentity, got SourceControlRequest) bool {
			return peer == target && got == otherRequest
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	router, err := NewGroupSourceControlRegistry(GroupSourceControlRegistryOptions{
		Registry: registry, ReadDeadline: deadline, MaxConnections: 1,
		Services: []GroupSourceControlService{{Group: request.Group, Service: service}, {Group: otherRequest.Group, Service: otherService}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		edit func(*SourceControlRequest)
	}{
		{"target member", func(r *SourceControlRequest) { r.TargetMember++ }},
		{"target store", func(r *SourceControlRequest) { r.TargetStore[0]++ }},
		{"target incarnation", func(r *SourceControlRequest) { r.TargetIncarnation++ }},
		{"source member", func(r *SourceControlRequest) { r.SourceMember += 10 }},
		{"source node", func(r *SourceControlRequest) { r.SourceNode[0]++ }},
		{"replica generation", func(r *SourceControlRequest) { r.ReplicaSetVersion++ }},
		{"unregistered group", func(r *SourceControlRequest) { r.Group.GroupID[1]++ }},
		{"other registered group", func(r *SourceControlRequest) { r.Group = otherRequest.Group }},
	} {
		t.Run(test.name, func(t *testing.T) {
			forged := request
			test.edit(&forged)
			if _, err := requestGroupControl(t, router, coordinator, forged); !errors.Is(err, ErrSourceUnauthorized) {
				t.Fatalf("forged request: %v", err)
			}
		})
	}
	if _, err := requestGroupControl(t, router, target, request); !errors.Is(err, ErrSourceUnauthorized) {
		t.Fatalf("enrolled identity without control permission: %v", err)
	}
	if len(journal.records) != 0 || exporter.exportCalls != 0 || otherExporter.exportCalls != 0 {
		t.Fatal("unauthorized requests reached a journal or exporter")
	}
	for range 2 {
		result, err := requestGroupControl(t, router, coordinator, request)
		if err != nil || result.Descriptor != descriptor || result.State != SourceControlComplete {
			t.Fatalf("authorized coordinator prepare/retry: %+v, %v", result, err)
		}
	}
	if exporter.exportCalls != 1 || otherExporter.exportCalls != 0 {
		t.Fatal("coordinator did not execute exactly one export in the selected group")
	}
	before, err := journal.ReadSourceExport(context.Background(), request.Operation)
	if err != nil {
		t.Fatal(err)
	}
	revoked.Store(true)
	if _, err := requestGroupControl(t, router, coordinator, request); !errors.Is(err, ErrSourceUnauthorized) {
		t.Fatalf("revoked coordinator replayed a completed export: %v", err)
	}
	after, err := journal.ReadSourceExport(context.Background(), request.Operation)
	if err != nil || before != after || exporter.exportCalls != 1 {
		t.Fatalf("revoked request changed durable export state: %v", err)
	}
}
