package driver

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func compositeFastPathBinding(t *testing.T) *placementBinding {
	t.Helper()
	var first, second distribution.KeyspacePoint
	first[0] = 0x55
	second[0] = 0xaa
	manifest, err := distribution.NewManifest("d", 17, []distribution.Shard{
		{
			ID: "s0", AllocationGeneration: 1,
			Range:   distribution.KeyRange{Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Point: first}},
			Leaders: []distribution.EndpointID{"ep-0a", "ep-0b"}, Epoch: 101,
		},
		{
			ID: "s1", AllocationGeneration: 2,
			Range:   distribution.KeyRange{Start: first, End: distribution.KeyspaceEnd{Point: second}},
			Leaders: []distribution.EndpointID{"ep-1"}, Epoch: 202,
		},
		{
			ID: "s2", AllocationGeneration: 3,
			Range:   distribution.KeyRange{Start: second, End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"ep-2"}, Epoch: 303,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return newTestBinding(t, []string{"/tenant", "/channel"}, manifest)
}

func TestNativePointFastPathMatchesGenericRouter(t *testing.T) {
	binding := compositeFastPathBinding(t)
	router := distribution.NewRouter()
	documents := [][]byte{
		[]byte(`{"tenant":"alpha","channel":"one"}`),
		[]byte(`{"tenant":"alpha\u002dbeta","channel":"gamma\u002ddelta"}`),
		[]byte(`{"tenant":42,"channel":42.0}`),
		[]byte(`{"tenant":"\ud83d\ude80","channel":-7.5e1}`),
	}
	var scalarScratch [distribution.KeyspaceWidth]distribution.Scalar
	var textScratch []byte

	for _, document := range documents {
		key, text, err := documentShardKey(
			document, binding, scalarScratch[:0], textScratch[:0],
		)
		textScratch = text
		if err != nil {
			t.Fatalf("documentShardKey(%s): %v", document, err)
		}
		got, err := routeShardKey(binding, key)
		if err != nil {
			t.Fatalf("routeShardKey(%s): %v", document, err)
		}

		cons := make(distribution.BoundConstraints, len(key))
		for i := range key {
			cons[i], err = distribution.FiniteDomain(key[i])
			if err != nil {
				t.Fatal(err)
			}
		}
		generic, err := router.Route(
			cons, binding.mapper, binding.manifest, binding.policy,
		)
		if err != nil {
			t.Fatalf("generic Route(%s): %v", document, err)
		}
		if generic.Kind != distribution.RouteSingle || len(generic.Targets) != 1 ||
			generic.Targets[0] != got {
			t.Fatalf("document %s: native target %+v, generic route %+v", document, got, generic)
		}
		if got.Endpoint == "" || got.OwnershipEpoch == 0 || got.Role != distribution.RoleLeader {
			t.Fatalf("document %s returned incomplete fenced target %+v", document, got)
		}
	}
}

func TestPlacedWriteComparesCompleteFencedTarget(t *testing.T) {
	binding := compositeFastPathBinding(t)
	document := []byte(`{"tenant":"alpha\u002dbeta","channel":"gamma\u002ddelta"}`)
	var state insertPreflightState
	if err := state.add(binding, document, 0); err != nil {
		t.Fatal(err)
	}
	good := distribution.Route{
		Kind: distribution.RouteSingle, Targets: []distribution.Target{state.target},
	}
	if err := checkShardKeyImmutable(binding, nil, good, document); err != nil {
		t.Fatalf("exact fenced target rejected: %v", err)
	}

	variants := map[string]func(*distribution.Target){
		"shard":           func(target *distribution.Target) { target.Shard += "-stale" },
		"endpoint":        func(target *distribution.Target) { target.Endpoint += "-stale" },
		"ownership epoch": func(target *distribution.Target) { target.OwnershipEpoch++ },
		"role":            func(target *distribution.Target) { target.Role = distribution.RoleReplica },
	}
	for name, mutate := range variants {
		t.Run(name, func(t *testing.T) {
			stale := good
			stale.Targets = append([]distribution.Target(nil), good.Targets...)
			mutate(&stale.Targets[0])
			if err := checkShardKeyImmutable(binding, nil, stale, document); !errors.Is(err, ErrShardKeyImmutable) {
				t.Fatalf("stale %s target: err=%v, want ErrShardKeyImmutable", name, err)
			}

			batch := insertPreflightState{target: stale.Targets[0], set: true}
			if err := batch.add(binding, document, 1); !errors.Is(err, ErrCrossShardWrite) {
				t.Fatalf("stale %s batch fence: err=%v, want ErrCrossShardWrite", name, err)
			}
		})
	}
}

func TestEscapedCompositePointFastPathWarmZeroAllocation(t *testing.T) {
	binding := compositeFastPathBinding(t)
	document := []byte(`{"tenant":"alpha\u002dbeta","channel":"gamma\u002ddelta"}`)
	state := insertPreflightState{}
	if err := state.add(binding, document, 0); err != nil {
		t.Fatal(err)
	}
	var routeErr error
	allocs := testing.AllocsPerRun(1_000, func() {
		state.set = false
		routeErr = state.add(binding, document, 0)
	})
	if routeErr != nil {
		t.Fatal(routeErr)
	}
	if allocs != 0 {
		t.Fatalf("escaped composite point fast path = %.2f allocs/run, want 0", allocs)
	}
}
