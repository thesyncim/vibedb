package splitcontroller

import (
	"bytes"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	vibejson "github.com/thesyncim/vibejson"
)

func TestPlanIntentCanonicalRestartRoundTripAndBounds(t *testing.T) {
	plan, catalog, _, split := testPlan(t)
	prefix := []byte("prefix")
	raw, err := AppendPlanIntent(append([]byte(nil), prefix...), catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	intent := raw[len(prefix):]
	if len(intent) == 0 || len(intent) > MaxPlanIntentBytes {
		t.Fatalf("intent bytes = %d", len(intent))
	}
	recovered, err := OpenPlanIntent(intent, catalog)
	if err != nil || recovered.OperationID() != plan.OperationID() {
		t.Fatalf("recovered=%v err=%v", recovered, err)
	}
	published, err := gateway.NewSnapshot(
		distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{{
				Name: "orders", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
			}},
			Placements: []distribution.TablePlacement{{
				Table: "docs", Distribution: "orders", Columns: []string{"/tenant"},
			}},
			Manifests: []*distribution.Manifest{split.Manifest()},
		},
		map[distribution.EndpointID]string{
			"node-a": "127.0.0.1:1", "node-b": "127.0.0.1:2",
		},
		catalog.Generation()+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err = OpenPlanIntent(intent, published)
	if err != nil || recovered.OperationID() != plan.OperationID() {
		t.Fatalf("published recovery=%v err=%v", recovered, err)
	}
	again, err := AppendPlanIntent(nil, catalog, recovered)
	if err != nil || !bytes.Equal(intent, again) {
		t.Fatalf("canonical replay changed: equal=%v err=%v", bytes.Equal(intent, again), err)
	}
	for _, invalid := range [][]byte{
		append(append([]byte(nil), intent...), ' '),
		intent[:len(intent)-1],
		make([]byte, MaxPlanIntentBytes+1),
	} {
		if _, err = OpenPlanIntent(invalid, catalog); !errors.Is(err, ErrPlanIntent) {
			t.Fatalf("invalid intent error = %v", err)
		}
	}
}

func TestPlanIntentAuthenticatesEveryPreparedReplicaAndRejectsIncompleteGrammar(t *testing.T) {
	plan, catalog, target, split := testPlan(t)
	raw, err := AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenPlanIntent(raw, catalog)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := recovered.Target(target.Child)
	if !ok || !reflect.DeepEqual(got.Replicas, target.Replicas) {
		t.Fatalf("prepared replicas changed across canonical replay: got=%+v want=%+v", got.Replicas, target.Replicas)
	}

	// The sole grammar has no fallback for the former route-only replica. An
	// otherwise canonical payload missing any prepared-local authority fails.
	var incomplete persistedPlanIntent
	if err = vibejson.Unmarshal(raw, &incomplete); err != nil {
		t.Fatal(err)
	}
	incomplete.Targets[0].Replicas[0].RuntimeRoot = ""
	encoded, err := vibejson.Marshal(&incomplete)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = vibejson.AppendCanonicalize(nil, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenPlanIntent(encoded, catalog); !errors.Is(err, ErrPlanIntent) {
		t.Fatalf("incomplete replica intent error = %v", err)
	}

	for _, mutate := range []func(*ChildTarget){
		func(candidate *ChildTarget) { candidate.Replicas[0].CertificateDigest[0] ^= 1 },
		func(candidate *ChildTarget) {
			candidate.Replicas[0].SQLPath = filepath.Join(candidate.Replicas[0].RuntimeRoot, "other.vdb")
		},
	} {
		candidate := cloneChildTarget(target)
		mutate(&candidate)
		changed, planErr := NewPlan(
			plan.sourceSnapshotForTest(t), split, plan.partitioner, []ChildTarget{candidate},
		)
		if planErr != nil {
			t.Fatal(planErr)
		}
		if changed.OperationID() == plan.OperationID() {
			t.Fatal("replica-local authenticated authority did not change operation id")
		}
	}
}
