package gateway

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

func replicatedCatalogGenesisTestRecords(t *testing.T, snapshot *Snapshot) []NodeRecord {
	t.Helper()
	records := make([]NodeRecord, 0, len(snapshot.replicatedReplicas))
	for _, replica := range snapshot.replicatedReplicas {
		record := scalingTestNodeRecord(replica.Node, replica.NodeIncarnation, NodeActive, 1)
		record.ServiceKeyDigest = replication.Digest{replica.Node[0], 0x41}
		record.DataEndpoint = distributionEndpointID(replica.Endpoint)
		record.NativeEndpoint = distributionEndpointID(replica.NativeEndpoint)
		record.ControlEndpoint = distributionEndpointID(replica.ControlEndpoint)
		record.DataAddress = replica.DataAddress
		record.NativeAddress = replica.Address
		record.ControlAddress = replica.ControlAddress
		record.CatalogGeneration = snapshot.Generation()
		records = append(records, record)
	}
	return records
}

func distributionEndpointID(value string) distribution.EndpointID {
	return distribution.EndpointID(value)
}

func TestBuildReplicatedCatalogGenesisMutationsIsCanonicalAndComplete(t *testing.T) {
	snapshot := testCatalogAuthoritySnapshot(t, 1)
	records := replicatedCatalogGenesisTestRecords(t, snapshot)
	first, err := BuildReplicatedCatalogGenesisMutations(snapshot, records)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildReplicatedCatalogGenesisMutations(snapshot, records)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(records)+replicatedCatalogGenesisFixedMutationCount {
		t.Fatalf("mutation count=%d, want %d", len(first), len(records)+replicatedCatalogGenesisFixedMutationCount)
	}
	if len(second) != len(first) {
		t.Fatalf("repeat mutation count=%d, want %d", len(second), len(first))
	}
	for index := range first {
		left, right := first[index], second[index]
		if left.Kind != replication.MutationPutAbsentOrEqual || right.Kind != left.Kind ||
			!bytes.Equal(left.Key, right.Key) || !bytes.Equal(left.Value, right.Value) ||
			left.ExpectedValueLength != 0 || left.ExpectedValueDigest != (replication.Digest{}) {
			t.Fatalf("mutation %d is not deterministic absent-or-equal: left=%+v right=%+v", index, left, right)
		}
	}
	if !bytes.Equal(first[0].Key, replicatedCatalogGenesisKey) ||
		!bytes.Equal(first[1].Key, replicatedCatalogHeadKey) ||
		!bytes.Equal(first[2].Key, replicatedCatalogHeadWitnessKey) {
		t.Fatalf("proof mutation order=%x,%x,%x", first[0].Key, first[1].Key, first[2].Key)
	}
	for index, record := range records {
		mutation := first[3+index]
		if !bytes.Equal(mutation.Key, scalingNodeKey(record.NodeID, record.Incarnation)) {
			t.Fatalf("record mutation %d key=%x", index, mutation.Key)
		}
	}
	if !bytes.Equal(first[len(first)-2].Key, scalingNodeDirectoryKey) ||
		!bytes.Equal(first[len(first)-1].Key, replicatedServiceDirectoryKey) {
		t.Fatalf("directory mutation order=%x,%x", first[len(first)-2].Key, first[len(first)-1].Key)
	}
	if _, err := openReplicatedServiceDirectory(first[len(first)-1].Value); err != nil {
		t.Fatalf("empty service directory is not canonical: %v", err)
	}
	if _, err := openScalingNodeDirectory(first[len(first)-2].Value); err != nil {
		t.Fatalf("node directory is not canonical: %v", err)
	}
	if err := validateReplicatedCatalogGenesis(first[0].Value, first[1].Value); err != nil {
		t.Fatalf("genesis proof is not canonical: %v", err)
	}
	if err := validateReplicatedCatalogHeadWitness(first[2].Value, snapshot.Generation(), first[1].Value); err != nil {
		t.Fatalf("head witness is not canonical: %v", err)
	}
}

func TestBuildReplicatedCatalogGenesisMutationsRejectsInvalidPhysicalPlan(t *testing.T) {
	snapshot := testCatalogAuthoritySnapshot(t, 1)
	base := replicatedCatalogGenesisTestRecords(t, snapshot)
	for _, test := range []struct {
		name string
		edit func([]NodeRecord)
		want error
	}{
		{name: "joining", edit: func(records []NodeRecord) {
			records[0].Lifecycle = NodeJoining
		}, want: ErrInvalidScalingMetadata},
		{name: "duplicate", edit: func(records []NodeRecord) {
			records[1].NodeID = records[0].NodeID
		}, want: ErrScalingIdentity},
		{name: "route address", edit: func(records []NodeRecord) {
			records[0].DataAddress = "127.0.0.1:9999"
		}, want: ErrScalingIdentity},
		{name: "catalog generation", edit: func(records []NodeRecord) {
			records[0].CatalogGeneration = 2
		}, want: ErrInvalidScalingMetadata},
	} {
		t.Run(test.name, func(t *testing.T) {
			records := append([]NodeRecord(nil), base...)
			test.edit(records)
			_, err := BuildReplicatedCatalogGenesisMutations(snapshot, records)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
		})
	}
}

func TestBuildReplicatedCatalogGenesisMutationsRequiresGenerationOne(t *testing.T) {
	snapshot := testCatalogAuthoritySnapshot(t, 2)
	records := replicatedCatalogGenesisTestRecords(t, snapshot)
	if _, err := BuildReplicatedCatalogGenesisMutations(snapshot, records); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("generation-two snapshot error=%v", err)
	}
}
