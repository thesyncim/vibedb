package replicatedstate

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
)

func TestRouteGateResultRowCanonicalAndAllocationFree(t *testing.T) {
	session := sha256.Sum256([]byte("topology-session"))
	record := routeGateResultRecord{
		SessionDigest: session, Slot: 7, ClientEpoch: 19, ClientSequence: 24,
		Outcome: routegate.Outcome{
			Reason: routegate.ReasonDrainPending, Mutated: true,
			Status: routegate.Status{
				Revision: 4, Epoch: 3, ActivePins: 2, RetainedRecords: 2,
				Drain: routegate.DrainRecord{
					Identity: routegate.Identity(sha256.Sum256([]byte("drain"))),
					Binding:  routegate.Binding(sha256.Sum256([]byte("plan"))),
					Epoch:    3, State: routegate.DrainPending,
				},
			},
		},
	}
	var storage [routeGateResultBytes]byte
	encoded, err := appendRouteGateResult(storage[:0], record)
	if err != nil || len(encoded) != routeGateResultBytes {
		t.Fatalf("appendRouteGateResult = %d, %v", len(encoded), err)
	}
	opened, err := openRouteGateResult(encoded)
	if err != nil || opened != record {
		t.Fatalf("openRouteGateResult = %+v, %v", opened, err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		_, appendErr := appendRouteGateResult(storage[:0], record)
		if appendErr != nil {
			panic(appendErr)
		}
	}); allocs != 0 {
		t.Fatalf("route-gate result append allocations = %v", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, openErr := openRouteGateResult(encoded); openErr != nil {
			panic(openErr)
		}
	}); allocs != 0 {
		t.Fatalf("route-gate result open allocations = %v", allocs)
	}
	for offset := range encoded {
		corrupt := append([]byte(nil), encoded...)
		corrupt[offset] ^= 1
		if _, err := openRouteGateResult(corrupt); err == nil {
			t.Fatalf("accepted corruption at byte %d", offset)
		}
	}
}

func TestMaxRouteGateCompletionBytesMatchesActualCodec(t *testing.T) {
	binding := testBinding()
	identity := routegate.Identity(sha256.Sum256([]byte("completion-pin")))
	recipe := routegate.Binding(sha256.Sum256([]byte("completion-recipe")))
	outcome := routegate.Outcome{
		Reason: routegate.ReasonAcquired, Mutated: true,
		Status: routegate.Status{Revision: 1, Epoch: 1, ActivePins: 1, RetainedRecords: 1},
	}
	var result [routegate.OutcomeBytes]byte
	resultBytes, err := routegate.AppendOutcome(result[:0], outcome)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := sha256.Sum256(append(identity[:], recipe[:]...))
	digest := replication.CompletionResultDigest(
		ResultRouteGate, ResultFormatRouteGate, resultBytes,
	)
	encoded, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          []byte(strings.Repeat("d", replication.MaxIdentityBytes)),
		Shard:                 []byte(strings.Repeat("s", replication.MaxIdentityBytes)),
		AllocationGeneration:  binding.AllocationGeneration,
		ShardIncarnation:      binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration,
		Tenant:          []byte(strings.Repeat("t", replication.MaxIdentityBytes)),
		ClientID:        id128(31), ClientEpoch: 2, ClientSequence: 3,
		Fingerprint: fingerprint, AppliedSequence: 4,
		ResultCode: ResultRouteGate, ResultFormat: ResultFormatRouteGate,
		Storage: replication.CompletionInline, ResultLength: routegate.OutcomeBytes,
		ResultDigest: digest, InlineResult: resultBytes,
	})
	if err != nil || len(encoded) != MaxRouteGateCompletionEnvelopeBytes ||
		MaxRouteGateCompletionEnvelopeBytes != 1185 ||
		MaxCompletionEnvelopeBytes != MaxExecutionPinCompletionEnvelopeBytes ||
		replication.MaxRouteGateCommandBytes+MaxRouteGateCompletionEnvelopeBytes != 2298 {
		t.Fatalf("max route-gate completion = %d/%d, %v",
			len(encoded), MaxRouteGateCompletionEnvelopeBytes, err)
	}
}
