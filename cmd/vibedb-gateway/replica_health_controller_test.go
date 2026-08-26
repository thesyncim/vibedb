package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rebalance"
)

type testReplicaHealthCatalog struct{ snapshot *gateway.Snapshot }

func (catalog testReplicaHealthCatalog) Read(context.Context) (*gateway.Snapshot, error) {
	return catalog.snapshot, nil
}

type testFailureAuthority struct {
	certificates []rebalance.FailureQuorumCertificate
}

func (authority testFailureAuthority) VisitFailureCertificates(
	_ context.Context, _ *gateway.Snapshot, visit func(rebalance.FailureQuorumCertificate) error,
) error {
	for _, certificate := range authority.certificates {
		if err := visit(certificate); err != nil {
			return err
		}
	}
	return nil
}

type testHealthObserver struct{ rejected distribution.ShardID }

func (observer testHealthObserver) ObserveReplicaHealth(
	_ context.Context, _ *gateway.Snapshot, certificate rebalance.FailureQuorumCertificate,
) (gatewayReplicaHealthObservation, error) {
	if certificate.Shard == observer.rejected {
		return gatewayReplicaHealthObservation{}, errors.New("injected observation failure")
	}
	return gatewayReplicaHealthObservation{Publication: raftmodel.Publication{Applied: 1}}, nil
}

type testCandidateInventory struct{}

func (testCandidateInventory) ReplacementCandidates(
	context.Context, *gateway.Snapshot, rebalance.FailureQuorumCertificate,
) ([]rebalance.ReplacementCandidate, error) {
	return []rebalance.ReplacementCandidate{{Member: 9}}, nil
}

type testFailedReplicaSink struct{}

func (testFailedReplicaSink) SubmitFailedReplicaMove(context.Context, rebalance.FailedReplicaMoveIntent) error {
	return nil
}

func TestReplicaHealthControllerStreamsIndependentCertifiedFailures(t *testing.T) {
	snapshot := testReplicaHealthSnapshot(t)
	controller, err := newGatewayReplicaHealthController(
		testReplicaHealthCatalog{snapshot},
		testFailureAuthority{certificates: []rebalance.FailureQuorumCertificate{{Shard: "a"}, {Shard: "b"}}},
		testHealthObserver{rejected: "a"}, testCandidateInventory{}, testFailedReplicaSink{},
	)
	if err != nil {
		t.Fatal(err)
	}
	var scheduled int
	controller.schedule = func(
		_ context.Context, cut rebalance.FailedReplicaPlanningCut, _ rebalance.FailedReplicaMoveSink,
	) (rebalance.FailedReplicaMoveIntent, error) {
		scheduled++
		if cut.Catalog != snapshot || cut.Certificate.Shard != "b" || len(cut.Candidates) != 1 {
			t.Fatalf("wrong detached cut: %+v", cut)
		}
		return rebalance.FailedReplicaMoveIntent{}, nil
	}
	pass, err := controller.RunPass(context.Background())
	if err == nil || pass.Certificates != 2 || pass.Eligible != 1 || pass.Submitted != 1 || scheduled != 1 {
		t.Fatalf("pass=%+v scheduled=%d err=%v", pass, scheduled, err)
	}
}

type oneReplicaHealthPass struct {
	cancel context.CancelFunc
	calls  int
}

func (runner *oneReplicaHealthPass) RunPass(context.Context) (gatewayReplicaHealthPass, error) {
	runner.calls++
	runner.cancel()
	return gatewayReplicaHealthPass{Certificates: 2, Submitted: 1}, nil
}

func TestRunReplicaHealthControllerStartsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &oneReplicaHealthPass{cancel: cancel}
	var logs int
	runReplicaHealthController(ctx, runner, time.Hour, func(string, ...any) { logs++ })
	if runner.calls != 1 || logs != 1 {
		t.Fatalf("calls=%d logs=%d", runner.calls, logs)
	}
}

func testReplicaHealthSnapshot(t testing.TB) *gateway.Snapshot {
	t.Helper()
	manifest, err := distribution.NewManifest("data", 1, []distribution.Shard{{
		ID: "a", AllocationGeneration: 1,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"endpoint"}, Epoch: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := gateway.NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "data", Arity: 1, MapperVersion: 1}},
		Manifests:     []*distribution.Manifest{manifest},
	}, map[distribution.EndpointID]string{"endpoint": "127.0.0.1:1"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
