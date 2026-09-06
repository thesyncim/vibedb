package replicacontrol

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func capacityTestGroup() raftmember.GroupKey {
	return raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
	}
}

func capacityTestRequest() CapacityRequest {
	return CapacityRequest{
		Operation: [32]byte{6}, Step: [32]byte{7}, Group: capacityTestGroup(), TargetMember: 8,
		ExpectedCatalogGeneration: 11, MinimumApplied: 9, MinimumSourceRevision: 4,
	}
}

func capacityTestObservation(knownEmpty bool) CapacityObservation {
	request := capacityTestRequest()
	request.Round = [32]byte{98}
	identity := raftmember.RuntimeIdentity{
		Group: request.Group, AllocationGeneration: 12, MemberID: request.TargetMember,
		StoreID: [16]byte{13}, NodeIncarnation: 14,
	}
	node := NodeCapacity{
		NodeID: rafttransport.NodeID{15}, NodeIncarnation: identity.NodeIncarnation, Revision: 16,
		Capacity:          autosplit.CapacityVector{autosplit.ResourceLiveBytes: 1 << 20},
		MigrationCapacity: 1 << 20, MaxReceives: 8,
	}
	observation := CapacityObservation{Request: request, Identity: identity,
		CatalogGeneration: request.ExpectedCatalogGeneration, Applied: 10, SourceRevision: 5,
		KnownEmpty: knownEmpty, DemandKind: CapacityDemandMeasured, Node: node}
	if !knownEmpty {
		observation.Demand[autosplit.ResourceLiveBytes] = 4096
		observation.MigrationBytes = 8192
	}
	return observation
}

func TestCapacityRequestRoundTripAndVersion(t *testing.T) {
	want := capacityTestRequest()
	want.Round = [32]byte{99}
	raw, err := AppendCapacityRequest(nil, want)
	if err != nil || len(raw) != CapacityRequestBytes {
		t.Fatalf("AppendCapacityRequest len=%d err=%v", len(raw), err)
	}
	got, err := OpenCapacityRequest(raw)
	if err != nil || got != want {
		t.Fatalf("OpenCapacityRequest got=%+v err=%v want=%+v", got, err, want)
	}
	mutated := bytes.Clone(raw)
	mutated[7] = 0
	if _, err := OpenCapacityRequest(mutated); !errors.Is(err, ErrControl) {
		t.Fatalf("old request version err=%v", err)
	}
	zero := want
	zero.ExpectedCatalogGeneration = 0
	if _, err := AppendCapacityRequest(nil, zero); !errors.Is(err, ErrControl) {
		t.Fatal("zero catalog generation accepted")
	}
}

func TestCapacityObservationRoundTripKnownEmptyAndNonEmpty(t *testing.T) {
	for _, knownEmpty := range []bool{false, true} {
		want, err := NewCapacityObservation(capacityTestObservation(knownEmpty))
		if err != nil {
			t.Fatalf("NewCapacityObservation(%t): %v", knownEmpty, err)
		}
		raw, err := AppendCapacityObservation(nil, want)
		if err != nil || len(raw) != CapacityObservationBytes {
			t.Fatalf("AppendCapacityObservation(%t) len=%d err=%v", knownEmpty, len(raw), err)
		}
		got, err := OpenCapacityObservation(raw)
		if err != nil || got != want {
			t.Fatalf("OpenCapacityObservation(%t) got=%+v err=%v want=%+v", knownEmpty, got, err, want)
		}
		if got.ObservationDigest != CapacityObservationDigest(got) {
			t.Fatalf("digest mismatch for knownEmpty=%t", knownEmpty)
		}
	}
}

func TestCapacityObservationRejectsStaleIdentityAndMutation(t *testing.T) {
	want, err := NewCapacityObservation(capacityTestObservation(false))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendCapacityObservation(nil, want)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]byte){
		func(raw []byte) { raw[239]++ },        // member identity
		func(raw []byte) { raw[415]++ },        // node capacity
		func(raw []byte) { raw[504]++ },        // round identity
		func(raw []byte) { raw[497] = 1 },      // canonical padding
		func(raw []byte) { raw[len(raw)-1]++ }, // digest
	} {
		mutated := bytes.Clone(raw)
		mutate(mutated)
		if _, err := OpenCapacityObservation(mutated); err == nil {
			t.Fatal("mutated observation accepted")
		}
	}
	stale := want
	stale.Applied = stale.Request.MinimumApplied - 1
	stale.ObservationDigest = CapacityObservationDigest(stale)
	if _, err := NewCapacityObservation(stale); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatalf("stale applied accepted: %v", err)
	}
}

func TestCapacityObservationRejectsSyntheticZeroAndSaturatingInvalidNode(t *testing.T) {
	zero := capacityTestObservation(false)
	zero.Demand = autosplit.CapacityVector{}
	zero.MigrationBytes = 0
	if _, err := NewCapacityObservation(zero); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatalf("synthetic zero demand accepted: %v", err)
	}
	empty := capacityTestObservation(true)
	empty.Node.Used[autosplit.ResourceLiveBytes] = empty.Node.Capacity[autosplit.ResourceLiveBytes] + 1
	if _, err := NewCapacityObservation(empty); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatalf("used beyond capacity accepted: %v", err)
	}
	overflow := capacityTestObservation(false)
	overflow.Demand[autosplit.ResourceLiveBytes] = math.MaxUint64
	overflow.Node.Capacity[autosplit.ResourceLiveBytes] = math.MaxUint64
	overflow.MigrationBytes = math.MaxUint64
	overflow.Node.MigrationCapacity = math.MaxUint64
	if _, err := NewCapacityObservation(overflow); err != nil {
		t.Fatalf("maximum representable bounded observation rejected: %v", err)
	}
}
