package snapshottransfer

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestGroupDataRegistryDynamicInventoryRetainsAuthorityAndNodeBounds(t *testing.T) {
	payload := bytes.Repeat([]byte("dynamic"), 1024)
	descriptor := testDescriptor(payload)
	other := descriptor
	other.Group.GroupID[0]++
	registry, _, target, _ := twoGroupRegistry(t, descriptor, other)
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	repository := openTestRepository(t, filepath.Join(t.TempDir(), "source"))
	appendAll(t, repository, descriptor, payload, 0)
	service, err := NewService(ServiceOptions{Repository: repository, Registry: registry,
		Authorize:    func(got Descriptor) bool { return got == descriptor },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 1,
		MaxChunkBytes: MinChunkBytes, MaxInflightBytes: MinChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	var current atomic.Pointer[Service]
	var resolutions atomic.Int64
	router, err := NewGroupDataRegistry(GroupDataRegistryOptions{Registry: registry,
		Resolve: func(group raftmember.GroupKey) *Service {
			resolutions.Add(1)
			return current.Load()
		}, ReadDeadline: deadline, MaxConnections: 1, MaxInflightBytes: MinChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := requestGroupChunk(t, router, target, descriptor); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("empty inventory: %v", err)
	}
	current.Store(service)
	if got, err := requestGroupChunk(t, router, target, descriptor); err != nil || !bytes.Equal(got, payload[:MinChunkBytes]) {
		t.Fatalf("adopted group bytes=%d err=%v", len(got), err)
	}
	foreignDescriptor := descriptor
	foreignDescriptor.Group.ClusterIncarnation[0]++
	foreignOther := foreignDescriptor
	foreignOther.Group.GroupID[0]++
	foreign, _, _, _ := twoGroupRegistry(t, foreignDescriptor, foreignOther)
	current.Store(&Service{registry: foreign})
	if _, err := requestGroupChunk(t, router, target, descriptor); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("foreign service trust domain: %v", err)
	}
	current.Store(service)
	unknown := descriptor
	unknown.Group.GroupID[0] += 2
	before := resolutions.Load()
	if _, err := requestGroupChunk(t, router, target, unknown); !errors.Is(err, ErrStaleFence) || resolutions.Load() != before {
		t.Fatalf("unknown group reached resolver: err=%v calls=%d", err, resolutions.Load()-before)
	}
	router.inflight.Store(MinChunkBytes)
	if _, err := requestGroupChunk(t, router, target, descriptor); !errors.Is(err, ErrBound) {
		t.Fatalf("shared byte ceiling: %v", err)
	}
	router.inflight.Store(0)
	router.slots <- struct{}{}
	client, server := net.Pipe()
	defer client.Close()
	before = resolutions.Load()
	if err := router.Serve(context.Background(), &testPeerConn{Conn: server, identity: target, class: rafttransport.TrafficSnapshot}); !errors.Is(err, ErrBound) || resolutions.Load() != before {
		t.Fatalf("shared connection ceiling: err=%v", err)
	}
	<-router.slots
	current.Store(nil)
	if _, err := requestGroupChunk(t, router, target, descriptor); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("retired group: %v", err)
	}
}

func TestGroupSourceControlRegistryDynamicInventoryChecksLocalSource(t *testing.T) {
	request, descriptor := sourceControlFixture()
	other := descriptor
	other.Group.GroupID[0]++
	registry, _, target, _ := twoGroupRegistry(t, descriptor, other)
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	exporter := &testSourceExporter{descriptor: descriptor}
	service, err := NewSourceControlService(SourceControlOptions{
		Journal: &memorySourceJournal{records: make(map[[32]byte]SourceControlRecord)}, Exporter: exporter,
		Authorize: func(peer rafttransport.PeerIdentity, got SourceControlRequest) bool {
			return peer == target && got == request
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	var current atomic.Pointer[SourceControlService]
	router, err := NewGroupSourceControlRegistry(GroupSourceControlRegistryOptions{
		Registry: registry, Resolve: func(raftmember.GroupKey) *SourceControlService { return current.Load() },
		ReadDeadline: deadline, MaxConnections: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := requestGroupControl(t, router, target, request); !errors.Is(err, ErrSourceUnauthorized) {
		t.Fatalf("empty inventory: %v", err)
	}
	current.Store(service)
	if record, err := requestGroupControl(t, router, target, request); err != nil || record.Descriptor != descriptor {
		t.Fatalf("adopted group: record=%+v err=%v", record, err)
	}
	wrong := request
	wrong.SourceMember += 2
	if _, err := requestGroupControl(t, router, target, wrong); !errors.Is(err, ErrSourceUnauthorized) || exporter.exportCalls != 1 {
		t.Fatalf("wrong local source: %v calls=%d", err, exporter.exportCalls)
	}
	current.Store(nil)
	if _, err := requestGroupControl(t, router, target, request); !errors.Is(err, ErrSourceUnauthorized) {
		t.Fatalf("retired inventory: %v", err)
	}
}
