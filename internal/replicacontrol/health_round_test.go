package replicacontrol

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type roundObserver struct{}

func (roundObserver) ObserveReplica(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaObservation, error) {
	return raftservice.ReplicaObservation{}, ErrControl
}
func (roundObserver) ObserveReplicaHealth(_ context.Context, group raftmember.GroupKey, target uint64) (raftservice.ReplicaHealthObservation, error) {
	_, cut := controlFixture()
	return raftservice.ReplicaHealthObservation{Identity: raftmember.RuntimeIdentity{Group: group, MemberID: target}, Status: cut.Status, Publication: cut.Publication}, nil
}

func TestHealthRoundReusesSerializesAndRotatesStreams(t *testing.T) {
	for _, oldPeer := range []bool{false, true} {
		t.Run(map[bool]string{false: "stream", true: "one-shot peer"}[oldPeer], func(t *testing.T) {
			request, _ := controlFixture()
			domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}
			var opens, authorized atomic.Int64
			deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
			service, err := NewService(ServiceOptions{Observer: roundObserver{}, Authorize: func(_ rafttransport.PeerIdentity, r Request) bool { authorized.Add(1); return r.HealthOnly }, ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 8})
			if err != nil {
				t.Fatal(err)
			}
			var workers sync.WaitGroup
			client, err := NewClient(ClientOptions{MaxHealthRoundConnections: 1, ReadDeadline: deadline, WriteDeadline: deadline, Opener: openerFunc(func(_ context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
				opens.Add(1)
				left, right := net.Pipe()
				server := &testConnection{Conn: right, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{8}}, class: rafttransport.TrafficShardControl}
				workers.Add(1)
				go func() {
					defer workers.Done()
					if oldPeer {
						defer server.Close()
						r, e := ReadRequest(server)
						if e == nil {
							_ = service.serveRequest(t.Context(), server, r)
						}
					} else {
						_ = service.Serve(t.Context(), server)
					}
				}()
				return &testConnection{Conn: left, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: node}, class: rafttransport.TrafficShardControl}, nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			round := client.BeginHealthRound(t.Context())
			defer round.Close()
			const requests = 300 // crosses the server's 256-frame rotation bound
			var calls sync.WaitGroup
			for i := 0; i < requests; i++ {
				calls.Add(1)
				go func(i int) {
					defer calls.Done()
					r := request
					r.Group.GroupID[0] = byte(i%250 + 1)
					r.Step[0] = byte(i)
					got, e := round.ObserveHealth(t.Context(), rafttransport.NodeID{9}, r)
					r.HealthOnly = true
					if e != nil || got.Request != r {
						t.Errorf("request %d: %v", i, e)
					}
				}(i)
			}
			calls.Wait()
			if err := round.Close(); err != nil {
				t.Fatal(err)
			}
			workers.Wait()
			wantOpens := int64(2)
			if oldPeer {
				wantOpens = requests
			}
			if opens.Load() != wantOpens || authorized.Load() != requests {
				t.Fatalf("opens=%d want=%d authorized=%d", opens.Load(), wantOpens, authorized.Load())
			}
			if _, e := round.ObserveHealth(t.Context(), rafttransport.NodeID{9}, request); !errors.Is(e, context.Canceled) {
				t.Fatalf("closed round: %v", e)
			}
		})
	}
}

func TestHealthRoundCloseCancelsBlockedDialAndReservesOnlyOneCache(t *testing.T) {
	entered := make(chan struct{})
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	client, err := NewClient(ClientOptions{MaxHealthRoundConnections: 2, ReadDeadline: deadline, WriteDeadline: deadline, Opener: openerFunc(func(ctx context.Context, _ rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		close(entered)
		<-ctx.Done()
		return nil, context.Cause(ctx)
	})})
	if err != nil {
		t.Fatal(err)
	}
	first := client.BeginHealthRound(t.Context())
	second := client.BeginHealthRound(t.Context())
	if first.limit != 2 || second.limit != 0 {
		t.Fatal("concurrent rounds retained multiple caches")
	}
	_ = second.Close()
	request, _ := controlFixture()
	done := make(chan error, 1)
	go func() { _, e := first.ObserveHealth(t.Context(), rafttransport.NodeID{9}, request); done <- e }()
	<-entered
	_ = first.Close()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	third := client.BeginHealthRound(t.Context())
	defer third.Close()
	if third.limit != 2 {
		t.Fatal("cache reservation leaked")
	}
}

func TestHealthRoundBoundsCacheAndPoisonsInvalidResponses(t *testing.T) {
	request, _ := controlFixture()
	domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}
	var opens atomic.Int64
	var invalid atomic.Bool
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	var workers sync.WaitGroup
	client, err := NewClient(ClientOptions{MaxHealthRoundConnections: 1, ReadDeadline: deadline, WriteDeadline: deadline, Opener: openerFunc(func(_ context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		opens.Add(1)
		left, right := net.Pipe()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer right.Close()
			for {
				r, e := ReadRequest(right)
				if e != nil {
					return
				}
				_, cut := controlFixture()
				h := HealthObservation{Request: r, MemberID: r.TargetMember, LeaderID: cut.Status.LeaderID, Term: cut.Status.Term, Commit: cut.Status.Commit, Applied: cut.Status.Applied, ReplicaSetVersion: cut.Publication.ReplicaSetVersion}
				if invalid.Load() {
					h.Request.Step[0]++
				}
				if WriteHealthObservation(right, h) != nil {
					return
				}
			}
		}()
		return &testConnection{Conn: left, identity: rafttransport.PeerIdentity{TrustDomain: domain, Node: node}, class: rafttransport.TrafficShardControl}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	round := client.BeginHealthRound(t.Context())
	defer round.Close()
	for _, node := range []rafttransport.NodeID{{9}, {10}, {10}, {9}} {
		if _, e := round.ObserveHealth(t.Context(), node, request); e != nil {
			t.Fatal(e)
		}
	}
	if opens.Load() != 3 || len(round.entries) != 1 {
		t.Fatalf("opens=%d entries=%d", opens.Load(), len(round.entries))
	}
	invalid.Store(true)
	if _, e := round.ObserveHealth(t.Context(), rafttransport.NodeID{9}, request); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if opens.Load() != 3 {
		t.Fatal("retried invalid response")
	}
	invalid.Store(false)
	if _, e := round.ObserveHealth(t.Context(), rafttransport.NodeID{9}, request); e != nil {
		t.Fatal(e)
	}
	if opens.Load() != 4 {
		t.Fatal("did not replace poisoned connection")
	}
	_ = round.Close()
	workers.Wait()
}

func TestHealthStreamReauthorizesEveryRequest(t *testing.T) {
	request, _ := controlFixture()
	request.HealthOnly = true
	domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	calls := 0
	expectedStep := request.Step
	service, err := NewService(ServiceOptions{Observer: roundObserver{}, ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1, Authorize: func(_ rafttransport.PeerIdentity, r Request) bool { calls++; return r.Step == expectedStep }})
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	defer left.Close()
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(t.Context(), &testConnection{Conn: right, identity: rafttransport.PeerIdentity{TrustDomain: domain}, class: rafttransport.TrafficShardControl})
	}()
	if e := WriteRequest(left, request); e != nil {
		t.Fatal(e)
	}
	if _, e := ReadHealthObservation(left); e != nil {
		t.Fatal(e)
	}
	request.Step[0]++
	if e := WriteRequest(left, request); e != nil {
		t.Fatal(e)
	}
	if e := <-done; !errors.Is(e, ErrUnauthorized) {
		t.Fatal(e)
	}
	if calls != 2 {
		t.Fatal("authorization was cached")
	}
}
