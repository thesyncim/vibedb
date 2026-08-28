package gateway

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestNativeTopologySessionCleanupRecoversFromReplicatedCut(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		for _, lost := range []replication.CommandKind{0, replication.CommandSessionRetire, replication.CommandSessionRelease} {
			t.Run(fmt.Sprintf("checkpoint-%t/lost-%d", checkpoint, lost), func(t *testing.T) {
				route, machine, reopen := newRouteSessionMachineWithCheckpoint(t, checkpoint)
				client := &routeSessionDropClient{base: machine}
				executor, err := NewReplicatedExecutor(client, 1, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
				if err != nil {
					t.Fatal(err)
				}
				options := NativeSessionOptions{Executor: executor, Route: route, Distribution: string(route.Distribution), Shard: string(route.Shard),
					Tenant: []byte("completed-topology-operation"), ClientID: replication.ID128{99}, RetryHome: replication.RetryHome{23},
					Resolver: BaseRelationResolver{Relation: 1}, ProposalCapability: serviceauthz.CapabilityTopology}
				session, err := NewNativeSession(options)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := session.Open(ctx, math.MaxInt64); err != nil {
					t.Fatal(err)
				}
				if _, err := session.Put(ctx, []byte("one"), []byte(`{"id":"one"}`)); err != nil {
					t.Fatal(err)
				}
				cut, err := machine.machine.Snapshot("docs")
				if err != nil {
					t.Fatal(err)
				}
				for _, mutate := range []func(*NativeSessionOptions){
					func(o *NativeSessionOptions) { o.ProposalCapability = serviceauthz.CapabilityDataWrite },
					func(o *NativeSessionOptions) { o.Route.Group.GroupID[0]++ },
					func(o *NativeSessionOptions) { o.Route.Command.OwnershipEpoch++ },
					func(o *NativeSessionOptions) { o.RetryHome[0]++ },
				} {
					forged := options
					mutate(&forged)
					if _, err := NewNativeTopologySessionCleanup(forged, cut); !errors.Is(err, ErrNativeSession) {
						t.Fatalf("substituted cleanup authority: %v", err)
					}
				}
				client.drop = lost
				appliedBefore := machine.state.Applied
				cleanup, err := NewNativeTopologySessionCleanup(options, cut)
				if err != nil || machine.state.Applied != appliedBefore {
					t.Fatalf("preparing cleanup proposed with a live snapshot: %v", err)
				}
				if closeErr := cut.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				err = cleanup.Run(ctx)
				if lost == 0 && err != nil || lost != 0 && err == nil {
					t.Fatalf("lost=%d first cleanup=%v", lost, err)
				}
				// A replacement controller has no original session or local journal.
				reopen()
				cut, err = machine.machine.Snapshot("docs")
				if err != nil {
					t.Fatal(err)
				}
				cleanup, err = NewNativeTopologySessionCleanup(options, cut)
				if err != nil {
					t.Fatal(err)
				}
				if closeErr := cut.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				err = cleanup.Run(ctx)
				if err != nil {
					t.Fatalf("replacement cleanup: %v", err)
				}
				state, err := machine.machine.SessionCapacityState()
				if err != nil || state.SessionCount != 0 || state.SessionSlotCount != 0 || state.AuthorityBindingCount != 1 {
					t.Fatalf("retained cleanup state=%+v err=%v", state, err)
				}
				applied := machine.state.Applied
				cut, err = machine.machine.Snapshot("docs")
				if err != nil {
					t.Fatal(err)
				}
				defer cut.Close()
				cleanup, err = NewNativeTopologySessionCleanup(options, cut)
				if err != nil {
					t.Fatal(err)
				}
				collection, ok := cut.Collection("docs")
				if !ok {
					t.Fatal("missing user collection")
				}
				value, found, err := collection.AppendRaw(nil, []byte("one"))
				if err != nil || !found || string(value) != `{"id":"one"}` {
					t.Fatalf("cleanup changed user data=%s found=%t err=%v", value, found, err)
				}
				if err := cut.Close(); err != nil {
					t.Fatal(err)
				}
				if err := cleanup.Run(ctx); err != nil || machine.state.Applied != applied {
					t.Fatalf("cleanup replay opened or proposed a new session: %v", err)
				}
			})
		}
	}
}
