package splitcontroller

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
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
