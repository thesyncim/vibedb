package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestScalingIntentIDIsStableAndBindsEveryRequestField(t *testing.T) {
	request := ScalingIntentRequest{
		Kind:              ScalingScaleIn,
		RequestID:         [32]byte{0x91},
		Drain:             NodeReference{NodeID: [16]byte{0x10}, Incarnation: 7},
		Targets:           []NodeReference{{NodeID: [16]byte{0x20}, Incarnation: 8}},
		DesiredNodeCount:  3,
		MaxMoves:          4,
		MaxMigrationBytes: 5 << 20,
		HysteresisPPM:     60_000,
	}
	if !request.Valid() {
		t.Fatal("base scaling request is invalid")
	}
	want := request.ID()
	if want == ([32]byte{}) || want != request.ID() {
		t.Fatalf("request ID is not stable: %x", want)
	}

	variants := []struct {
		name string
		edit func(*ScalingIntentRequest)
	}{
		{name: "kind", edit: func(value *ScalingIntentRequest) {
			value.Kind = ScalingRebalance
		}},
		{name: "request id", edit: func(value *ScalingIntentRequest) {
			value.RequestID = [32]byte{0x92}
		}},
		{name: "drain", edit: func(value *ScalingIntentRequest) {
			value.Drain = NodeReference{NodeID: [16]byte{0x11}, Incarnation: 7}
		}},
		{name: "targets", edit: func(value *ScalingIntentRequest) {
			value.Targets = []NodeReference{{NodeID: [16]byte{0x21}, Incarnation: 8}}
		}},
		{name: "desired count", edit: func(value *ScalingIntentRequest) {
			value.DesiredNodeCount++
		}},
		{name: "max moves", edit: func(value *ScalingIntentRequest) {
			value.MaxMoves++
		}},
		{name: "migration bytes", edit: func(value *ScalingIntentRequest) {
			value.MaxMigrationBytes++
		}},
		{name: "hysteresis", edit: func(value *ScalingIntentRequest) {
			value.HysteresisPPM++
		}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			candidate := request
			candidate.Targets = append([]NodeReference(nil), request.Targets...)
			variant.edit(&candidate)
			if !candidate.Valid() {
				t.Fatalf("variant became invalid: %+v", candidate)
			}
			if got := candidate.ID(); got == want {
				t.Fatalf("changing %s did not change intent ID %x", variant.name, got)
			}
		})
	}
}

func TestScalingMetadataCanonicalRecordsAndBoundedKeys(t *testing.T) {
	record := scalingTestNodeRecord([16]byte{0x11}, 7, NodeJoining, 1)
	raw, err := appendScalingNodeRecord(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openScalingNodeRecord(raw, record.NodeID, record.Incarnation)
	if err != nil || opened != record {
		t.Fatalf("canonical node record opened as %+v with err=%v", opened, err)
	}
	if _, err = openScalingNodeRecord(append(bytes.Clone(raw), 'x'), record.NodeID, record.Incarnation); err == nil ||
		(!errors.Is(err, ErrInvalidScalingMetadata) && !errors.Is(err, ErrReplicatedCatalog)) {
		t.Fatalf("trailing bytes accepted by node record envelope: %v", err)
	}
	wrongNode := record.NodeID
	wrongNode[0]++
	if _, err = openScalingNodeRecord(raw, wrongNode, record.Incarnation); err == nil ||
		(!errors.Is(err, ErrInvalidScalingMetadata) && !errors.Is(err, ErrReplicatedCatalog)) {
		t.Fatalf("record opened under a different physical node: %v", err)
	}

	nodeKey := scalingNodeKey(record.NodeID, record.Incarnation)
	if len(nodeKey) != len(fixedControlPlaneKey(bytes.Repeat([]byte{'x'}, scalingNodeIdentifierBytes))) {
		t.Fatalf("node key length=%d is not fixed", len(nodeKey))
	}
	if len(scalingIntentKey([32]byte{0x22})) != len(scalingIntentKey([32]byte{0x23})) ||
		len(enrollmentIntentKey([32]byte{0x22})) != len(enrollmentIntentKey([32]byte{0x23})) {
		t.Fatal("intent keys are not fixed width")
	}
	if bytes.Equal(nodeKey, scalingNodeKey(record.NodeID, record.Incarnation+1)) {
		t.Fatal("node incarnation is missing from the durable key")
	}

	digest := scalingDigest(raw)
	entry := scalingNodeDirectoryEntry{
		NodeID:      append([]byte(nil), record.NodeID[:]...),
		Incarnation: record.Incarnation,
		Revision:    record.Revision,
		Digest:      bytes.Clone(digest[:]),
	}
	if _, err = appendScalingNodeDirectoryAt(nil, []scalingNodeDirectoryEntry{entry}, 9); err != nil {
		t.Fatal(err)
	}
	duplicate := entry
	if _, err = appendScalingNodeDirectoryAt(nil, []scalingNodeDirectoryEntry{entry, duplicate}, 10); !errors.Is(err, ErrInvalidScalingMetadata) {
		t.Fatalf("duplicate directory entry accepted: %v", err)
	}
	if _, err = appendScalingIDDirectory(nil, scalingIntentDirectoryDocumentID[:], make([]scalingIDDirectoryEntry, MaxScalingIntents+1), maxScalingIntentDirectoryBytes); !errors.Is(err, ErrScalingMetadataBound) {
		t.Fatalf("scaling intent directory bound error=%v", err)
	}
}

func TestScalingStateMachinesRejectTerminalCreationAndGenericRetirement(t *testing.T) {
	if NodeJoining.Allows(NodeDecommissioned) || NodeActive.Allows(NodeDecommissioned) ||
		NodeDecommissioned.Allows(NodeActive) {
		t.Fatal("node lifecycle allows a terminal or reverse transition")
	}
	if EnrollmentReserved.Allows(EnrollmentEnrolled) || EnrollmentPrepared.Allows(EnrollmentMoving) {
		t.Fatal("enrollment state machine skipped a durable proof stage")
	}
	if ScalingReserved.Allows(ScalingComplete) || ScalingComplete.Allows(ScalingRunning) {
		t.Fatal("scaling state machine skipped or reversed a stage")
	}

	// The integration test exercises the persisted transition as well; this
	// local record check ensures a caller cannot smuggle a terminal witness
	// without the complete retirement scan fields.
	record := scalingTestNodeRecord([16]byte{0x12}, 1, NodeDecommissioned, 1)
	if record.Valid() {
		t.Fatal("decommissioned node without retirement witness passed validation")
	}
	record.Lifecycle = NodeJoining
	if !record.Valid() {
		t.Fatal("joining node fixture is invalid")
	}
}

func scalingTestNodeRecord(node rafttransport.NodeID, incarnation uint64, lifecycle NodeLifecycle, revision uint64) NodeRecord {
	capacity := autosplit.CapacityVector{}
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 100
	}
	return NodeRecord{
		NodeID:            node,
		Incarnation:       incarnation,
		ServiceKeyDigest:  replication.Digest{node[0]},
		DataEndpoint:      distribution.EndpointID(fmt.Sprintf("data-%02x", node[0])),
		NativeEndpoint:    distribution.EndpointID(fmt.Sprintf("native-%02x", node[0])),
		ControlEndpoint:   distribution.EndpointID(fmt.Sprintf("control-%02x", node[0])),
		DataAddress:       "127.0.0.1:8001",
		NativeAddress:     "127.0.0.1:8101",
		ControlAddress:    "127.0.0.1:8201",
		FailureDomain:     "zone-a",
		Roles:             NodeRoleStorage,
		Capacity:          capacity,
		MigrationCapacity: 1 << 30,
		MaxReceives:       8,
		Lifecycle:         lifecycle,
		Revision:          revision,
		CatalogGeneration: 5,
	}
}

func scalingTestGroup(seed byte) raftmember.GroupKey {
	group := raftmember.GroupKey{TopologyRecoveryEpoch: uint64(seed) + 1}
	for index := range group.ClusterID {
		group.ClusterID[index] = seed + byte(index) + 1
		group.ClusterIncarnation[index] = seed + byte(index) + 21
		group.ShardIncarnation[index] = seed + byte(index) + 41
		group.GroupID[index] = seed + byte(index) + 61
	}
	return group
}

func scalingTestCommand() raftservice.CommandFence {
	return raftservice.CommandFence{
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: 5,
		ProtectionEpoch:        6,
		OwnershipEpoch:         7,
		SchemaGeneration:       8,
		RelationManifestDigest: [32]byte{9},
		RoutingVersion:         1,
		RouteGeneration:        10,
	}
}

func scalingTestReplica(node byte, member uint64, incarnation uint64) ReplicaIdentity {
	return ReplicaIdentity{
		Member:          member,
		Node:            rafttransport.NodeID{node},
		NodeIncarnation: incarnation,
		StoreID:         [16]byte{node + 10},
		Endpoint:        distribution.EndpointID(fmt.Sprintf("data-replica-%02x", node)),
		NativeEndpoint:  distribution.EndpointID(fmt.Sprintf("native-replica-%02x", node)),
		ControlEndpoint: distribution.EndpointID(fmt.Sprintf("control-replica-%02x", node)),
	}
}

func scalingTestEnrollmentIntent(seed byte, targetNode byte, targetRevision uint64) GroupEnrollmentIntent {
	group := scalingTestGroup(seed)
	command := scalingTestCommand()
	return GroupEnrollmentIntent{
		IntentID:                 [32]byte{seed, 0x01},
		Group:                    group,
		Distribution:             ReplicatedCatalogDistribution,
		Shard:                    ReplicatedCatalogShard,
		AllocationGeneration:     1,
		CatalogGeneration:        5,
		ReplicaOrdinal:           0,
		Source:                   scalingTestReplica(1, 1, 21),
		SnapshotSourceMember:     1,
		Target:                   scalingTestReplica(targetNode, 4, 1),
		ExpectedRosterDigest:     replication.Digest{0x41},
		ExpectedDescriptorDigest: replication.Digest{0x42},
		ExpectedManifestDigest:   replication.Digest{0x43},
		ExpectedCommand:          command,
		TargetNodeRevision:       targetRevision,
		State:                    EnrollmentReserved,
		Revision:                 1,
	}
}

func scalingTestPreparedProof(intent GroupEnrollmentIntent, directoryRevision uint64) PreparedReplicaProof {
	proof := PreparedReplicaProof{
		IntentID:                   intent.IntentID,
		Group:                      intent.Group,
		Distribution:               intent.Distribution,
		Shard:                      intent.Shard,
		ReplicaOrdinal:             intent.ReplicaOrdinal,
		TargetMember:               intent.Target.Member,
		TargetNode:                 intent.Target.Node,
		TargetNodeIncarnation:      intent.Target.NodeIncarnation,
		TargetStoreID:              intent.Target.StoreID,
		TargetEndpoint:             intent.Target.Endpoint,
		TargetNativeEndpoint:       intent.Target.NativeEndpoint,
		TargetControlEndpoint:      intent.Target.ControlEndpoint,
		ExpectedRosterDigest:       intent.ExpectedRosterDigest,
		ExpectedDescriptorDigest:   intent.ExpectedDescriptorDigest,
		ExpectedManifestDigest:     intent.ExpectedManifestDigest,
		RelationManifestDigest:     intent.ExpectedCommand.RelationManifestDigest,
		DescriptorDigest:           intent.ExpectedDescriptorDigest,
		ManifestDigest:             intent.ExpectedManifestDigest,
		Command:                    intent.ExpectedCommand,
		AllocationGeneration:       intent.AllocationGeneration,
		CatalogGeneration:          intent.CatalogGeneration,
		CertifiedDirectoryRevision: directoryRevision,
	}
	proof.EnrollmentDigest = proof.ComputedEnrollmentDigest()
	return proof
}

func scalingTestEnrolledReceipt(intent GroupEnrollmentIntent) CertifiedEnrollmentReceipt {
	return CertifiedEnrollmentReceipt{
		IntentID:                  intent.IntentID,
		IntentDigest:              intent.Digest(),
		BaseCatalogGeneration:     intent.CatalogGeneration,
		BaseCatalogHeadDigest:     replication.Digest{0x51},
		BaseDescriptorDigest:      intent.ExpectedDescriptorDigest,
		EnrolledCatalogGeneration: intent.CatalogGeneration + 1,
		EnrolledCatalogHeadDigest: replication.Digest{0x52},
		EnrolledDescriptorDigest:  replication.Digest{0x53},
		Target:                    intent.Target,
		InitialReplicaSetVersion:  intent.ExpectedCommand.ReplicaSetVersion,
		GrantDigest:               replication.Digest{0x54},
		TransitionID:              [32]byte{0x55},
	}
}
