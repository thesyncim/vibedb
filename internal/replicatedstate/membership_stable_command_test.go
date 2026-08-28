package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestMembershipStableGatePreservesLegacyReplayAndLogicalFences(t *testing.T) {
	classes := []struct {
		name   string
		class  replication.CommandAuthorityClass
		stable bool
	}{
		{"legacy-data", replication.CommandAuthorityData, false},
		{"legacy-route-session", replication.CommandAuthorityRouteSession, false},
		{"stable-data", replication.CommandAuthorityMembershipStableData, true},
		{"stable-route-session", replication.CommandAuthorityMembershipStableRouteSession, true},
	}
	fences := []struct {
		name            string
		change          func(*replication.Command)
		logicalMismatch bool
	}{
		{"older-membership", func(*replication.Command) {}, false},
		{"future-membership", func(c *replication.Command) { c.ReplicaSetVersion = 4 }, true},
		{"policy", func(c *replication.Command) { c.ActivePolicyGeneration++ }, true},
		{"protection", func(c *replication.Command) { c.ProtectionEpoch++ }, true},
		{"ownership", func(c *replication.Command) { c.OwnershipEpoch++ }, true},
		{"schema", func(c *replication.Command) { c.SchemaGeneration++ }, true},
		{"routing", func(c *replication.Command) { c.RoutingVersion++ }, true},
		{"route-generation", func(c *replication.Command) { c.RouteGeneration++ }, true},
	}
	for _, class := range classes {
		for _, fence := range fences {
			t.Run(class.name+"/"+fence.name, func(t *testing.T) {
				f := newMachineFixture(t)
				if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
					t.Fatal(err)
				}
				request := commandValue(f.binding, 1)
				request.AuthorityClass = class.class
				// Open precedes the configuration change. Replay must retain
				// the original epoch and authority class across the change.
				_, _, epoch := applySessionOpen(t, f.machine, 2, request)
				request.Kind, request.Batches, request.ClientEpoch = replication.CommandRouteGate, nil, epoch
				var err error
				request.RouteGate, err = routegate.AppendCommand(nil, routegate.Command{
					Operation: routegate.OperationAcquireShared, Epoch: 1,
					Identity: routegate.Identity(sha256.Sum256([]byte("membership-stable-gate"))),
					Binding:  routegate.Binding(sha256.Sum256([]byte("original-route"))),
				})
				if err != nil {
					t.Fatal(err)
				}
				fence.change(&request)
				encoded := encodeCommand(t, request)
				exact := bytes.Clone(encoded)
				if _, err = f.machine.ApplyConfiguration(raftmodel.ApplyMeta{Index: 3, Term: 2, Type: pb.EntryConfChange},
					&pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}); err != nil {
					t.Fatal(err)
				}
				want := uint32(ResultStaleFence)
				if class.stable && !fence.logicalMismatch {
					want = ResultRouteGate
				}
				admitErr := f.machine.AdmitCommand(encoded)
				if want == ResultRouteGate && admitErr != nil || want == ResultStaleFence && !errors.Is(admitErr, ErrStaleCommand) {
					t.Fatalf("admission=%v want result=%d", admitErr, want)
				}
				if _, err = f.machine.ApplyNormal(normalMeta(4), encoded); err != nil {
					t.Fatal(err)
				}
				first, err := f.machine.LookupCompletion(encoded)
				if err != nil {
					t.Fatal(err)
				}
				completion, err := replication.OpenCompletion(first.Bytes)
				if err != nil || completion.ResultCode != want {
					t.Fatalf("result=%d want=%d err=%v", completion.ResultCode, want, err)
				}
				reopened, err := Open(f.binding, f.bootstrap, f.system, UserCollection{Name: "docs", Target: f.user}, f.log, f.machine.options)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = reopened.ApplyNormal(normalMeta(5), encoded); err != nil {
					t.Fatal(err)
				}
				retry, err := reopened.LookupCompletion(encoded)
				if err != nil || !bytes.Equal(first.Bytes, retry.Bytes) || !bytes.Equal(exact, encoded) {
					t.Fatalf("exact command/completion changed: %v", err)
				}
				status, err := reopened.RouteGateStatus()
				wantPins := uint64(0)
				if want == ResultRouteGate {
					wantPins = 1
				}
				if err != nil || status.ActivePins != wantPins {
					t.Fatalf("gate=%+v err=%v", status, err)
				}
			})
		}
	}
}
