package gatewayruntime

import (
	"context"
	"encoding/binary"
	stdjson "encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

type SQLBatchWireClient struct {
	mu        sync.Mutex
	states    map[raftmember.GroupKey]map[uint64]shardservice.ReplicatedMemberState
	values    map[raftmember.GroupKey][][]byte
	reads     map[raftmember.GroupKey]int
	intent    raftmember.GroupKey
	active    atomic.Int64
	maxActive atomic.Int64
}

func (client *SQLBatchWireClient) DoReplicated(
	_ context.Context,
	endpoint gateway.ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	client.mu.Lock()
	state := client.states[request.Fence.Group][endpoint.Member]
	client.mu.Unlock()
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	active := client.active.Add(1)
	for maximum := client.maxActive.Load(); active > maximum &&
		!client.maxActive.CompareAndSwap(maximum, active); maximum = client.maxActive.Load() {
	}
	defer client.active.Add(-1)
	batch, err := replicatedstate.OpenPointReadBatch(request.BatchRead)
	if err != nil || request.Operation != shardservice.ReplicatedReadBatchLeader {
		return nil, gateway.ErrReplicatedRoute
	}
	client.mu.Lock()
	client.reads[request.Fence.Group]++
	values := client.values[request.Fence.Group]
	intent := request.Fence.Group == client.intent
	client.mu.Unlock()
	if intent {
		return &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalReadIntentActive,
			HasState: true, State: state,
		}, nil
	}
	if batch.Count() != len(values) {
		return nil, gateway.ErrReplicatedRoute
	}
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedReadBatchResult, HasState: true, State: state,
		ReadApplied: state.Applied, Value: appendSQLBatchWireValues(nil, values),
	}, nil
}

func appendSQLBatchWireValues(destination []byte, values [][]byte) []byte {
	destination = binary.LittleEndian.AppendUint32(destination, uint32(len(values)))
	bitmapStart := len(destination)
	destination = append(destination, make([]byte, (len(values)+7)/8+len(values)*4)...)
	lengthStart := bitmapStart + (len(values)+7)/8
	for index := range values {
		destination[bitmapStart+index/8] |= 1 << uint(index&7)
		binary.LittleEndian.PutUint32(destination[lengthStart+index*4:], uint32(len(values[index])))
		destination = append(destination, values[index]...)
	}
	return destination
}

type SQLBatchWireFixture struct {
	reader  *gateway.ReplicatedDataReader
	client  *SQLBatchWireClient
	request serveRequest
}

func newSQLBatchWireFixture(
	t testing.TB,
	groups int,
	sameGroupRelations bool,
) SQLBatchWireFixture {
	t.Helper()
	if groups <= 0 {
		t.Fatal("positive group count required")
	}
	pointCount := groups
	if sameGroupRelations {
		groups, pointCount = 1, 2
	}
	endpoints := map[distribution.EndpointID]string{
		"peer-a": "127.0.0.1:7001", "peer-b": "127.0.0.1:7002", "peer-c": "127.0.0.1:7003",
		"native-a": "127.0.0.1:7101", "native-b": "127.0.0.1:7102", "native-c": "127.0.0.1:7103",
		"control-a": "127.0.0.1:7201", "control-b": "127.0.0.1:7202", "control-c": "127.0.0.1:7203",
	}
	config := distribution.ClusterConfig{}
	descriptors := make([]gateway.ReplicatedShardDescriptor, 0, groups)
	profiles := make([]gateway.ReplicatedTableProfile, 0, pointCount)
	for groupIndex := range groups {
		distributionName := distribution.DistributionName(fmt.Sprintf("wire-%03d", groupIndex))
		manifest, err := distribution.NewManifest(distributionName, 3, []distribution.Shard{{
			ID: "all", AllocationGeneration: 1,
			Range: distribution.KeyRange{Start: distribution.KeyspacePoint{},
				End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"peer-a", "peer-b", "peer-c"}, Epoch: 7,
		}})
		if err != nil {
			t.Fatal(err)
		}
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{
			Name: distributionName, Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		})
		config.Manifests = append(config.Manifests, manifest)
		group := raftmember.GroupKey{TopologyRecoveryEpoch: 11}
		for index := range group.ClusterID {
			group.ClusterID[index] = byte(index + 1)
			group.ClusterIncarnation[index] = byte(index + 21)
			group.ShardIncarnation[index] = byte(index + 41)
			group.GroupID[index] = byte(index + 61)
		}
		group.ShardIncarnation[14], group.ShardIncarnation[15] = byte(groupIndex>>8), byte(groupIndex+1)
		group.GroupID[14], group.GroupID[15] = byte(groupIndex>>8), byte(groupIndex+1)
		descriptors = append(descriptors, gateway.ReplicatedShardDescriptor{
			Distribution: distributionName, Shard: "all", Group: group, AllocationGeneration: 1,
			LogicalSchemaDigest:  replication.Digest{0x19},
			RangeIdentity:        replication.Digest{byte(groupIndex + 1), 0x51},
			LineageDigest:        replication.Digest{byte(groupIndex + 1), 0x52},
			ForwardingRuleDigest: replication.Digest{byte(groupIndex + 1), 0x53},
			Command: raftservice.CommandFence{
				ReplicaSetVersion: 1, ActivePolicyGeneration: 5, ProtectionEpoch: 6,
				OwnershipEpoch: 7, SchemaGeneration: 8,
				RelationManifestDigest: replication.Digest{9}, RoutingVersion: 3, RouteGeneration: 10,
			},
			Replicas: []gateway.ReplicatedReplicaDescriptor{
				{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{11}, NodeIncarnation: 21, Endpoint: "peer-a", NativeEndpoint: "native-a", ControlEndpoint: "control-a"},
				{Member: 2, Node: [16]byte{2}, StoreID: [16]byte{12}, NodeIncarnation: 22, Endpoint: "peer-b", NativeEndpoint: "native-b", ControlEndpoint: "control-b"},
				{Member: 3, Node: [16]byte{3}, StoreID: [16]byte{13}, NodeIncarnation: 23, Endpoint: "peer-c", NativeEndpoint: "native-c", ControlEndpoint: "control-c"},
			},
		})
	}
	request := serveRequest{Op: "read_batch", MaxResultBytes: 1 << 20}
	for pointIndex := range pointCount {
		groupIndex := pointIndex
		if sameGroupRelations {
			groupIndex = 0
		}
		table := fmt.Sprintf("table_%03d", pointIndex)
		config.Placements = append(config.Placements, distribution.TablePlacement{
			Table: table, Distribution: distribution.DistributionName(fmt.Sprintf("wire-%03d", groupIndex)),
			Columns: []string{"/id"},
		})
		relation := replication.RelationID(1)
		if sameGroupRelations {
			relation = replication.RelationID(pointIndex + 1)
		}
		profiles = append(profiles, gateway.ReplicatedTableProfile{
			Table: table, Relation: relation, PrimaryKey: "/id", SchemaGeneration: 8,
			LogicalSchemaDigest: replication.Digest{0x19}, MaxKeyBytes: 256,
			MaxDocumentBytes: 1 << 20,
		})
		request.Statements = append(request.Statements, serveStatement{
			SQL:    "SELECT * FROM " + table + " WHERE id = ?",
			Params: []serveParam{{Kind: "string", Text: fmt.Sprintf("key-%03d", pointIndex)}},
		})
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 5, nil, nil, descriptors, profiles,
	)
	if err != nil {
		t.Fatal(err)
	}
	client := &SQLBatchWireClient{
		states: make(map[raftmember.GroupKey]map[uint64]shardservice.ReplicatedMemberState),
		values: make(map[raftmember.GroupKey][][]byte), reads: make(map[raftmember.GroupKey]int),
	}
	for pointIndex := range pointCount {
		key, ok := orderedkey.AppendString(
			nil, []byte(fmt.Sprintf("key-%03d", pointIndex)), orderedkey.Ascending,
		)
		if !ok {
			t.Fatal("ordered key")
		}
		var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
		var scratch [replication.MaxMutationKeyBytes + 16]byte
		resolved, resolvedOK := snapshot.ResolveReplicatedTableKey(
			[]byte(fmt.Sprintf("table_%03d", pointIndex)), key,
			scratch[:0], replicas[:0],
		)
		if !resolvedOK {
			t.Fatalf("resolve point %d", pointIndex)
		}
		if client.states[resolved.Route.Group] == nil {
			client.states[resolved.Route.Group] = make(map[uint64]shardservice.ReplicatedMemberState)
			for _, endpoint := range resolved.Route.Replicas {
				client.states[resolved.Route.Group][endpoint.Member] = shardservice.ReplicatedMemberState{
					Fence: shardservice.ReplicatedFence{
						Group: resolved.Route.Group, AllocationGeneration: resolved.Route.AllocationGeneration,
						Command: resolved.Route.Command, MemberID: endpoint.Member,
						StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, Term: 7,
					},
					LeaderID: 2, Commit: uint64(20 + pointIndex), Applied: uint64(20 + pointIndex),
					CheckpointApplied: 19,
				}
			}
		}
		client.values[resolved.Route.Group] = append(client.values[resolved.Route.Group],
			[]byte(fmt.Sprintf(`{"id":"key-%03d","v":%d}`, pointIndex, pointIndex)))
	}
	replicated, err := gateway.NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := gateway.NewReplicatedDataReaderWithOptions(gateway.ReplicatedDataReaderOptions{
		Catalog: gateway.NewCatalogHolder(snapshot), Executor: replicated,
		MaxScatterConcurrency: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	return SQLBatchWireFixture{reader: reader, client: client, request: request}
}

type decodedSQLBatchWireResponse struct {
	OK           bool                 `json:"ok"`
	Found        []bool               `json:"found"`
	Documents    []stdjson.RawMessage `json:"documents"`
	Observations []struct {
		ClusterID             string `json:"cluster_id"`
		ClusterIncarnation    string `json:"cluster_incarnation"`
		TopologyRecoveryEpoch uint64 `json:"topology_recovery_epoch"`
		ShardIncarnation      string `json:"shard_incarnation"`
		GroupID               string `json:"group_id"`
		RouteID               string `json:"route_id"`
		Applied               uint64 `json:"applied"`
	} `json:"observations"`
}

func roundTripSQLBatchWire(
	t testing.TB,
	fixture SQLBatchWireFixture,
) ([]byte, decodedSQLBatchWireResponse) {
	t.Helper()
	encoded, err := vibejson.Marshal(&fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	responseBytes := []byte(roundTripNativeDataLine(t, fixture.reader, nil, string(encoded)))
	var response decodedSQLBatchWireResponse
	if err := stdjson.Unmarshal(responseBytes, &response); err != nil {
		t.Fatalf("decode response %s: %v", responseBytes, err)
	}
	return responseBytes, response
}

func TestReadBatchWireSameGroupMultiRelationHasOneObservation(t *testing.T) {
	_, response := roundTripSQLBatchWire(t, newSQLBatchWireFixture(t, 1, true))
	if !response.OK || len(response.Found) != 2 || len(response.Documents) != 2 ||
		len(response.Observations) != 1 || !response.Found[0] || !response.Found[1] {
		t.Fatalf("response=%+v", response)
	}
}

func TestReadBatchWireCrossGroupExposesVectorNotGlobalSnapshot(t *testing.T) {
	raw, response := roundTripSQLBatchWire(t, newSQLBatchWireFixture(t, 2, false))
	if !response.OK || len(response.Observations) != 2 ||
		response.Observations[0].Applied == 0 || response.Observations[1].Applied == 0 {
		t.Fatalf("response=%+v", response)
	}
	var top map[string]stdjson.RawMessage
	if err := stdjson.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	if top["applied"] != nil || top["route_id"] != nil || top["snapshot"] != nil {
		t.Fatalf("cross-group response claims global cut: %s", raw)
	}
}

func TestReadBatchWireSixtyFiveGroupsIsBoundedAndUncapped(t *testing.T) {
	fixture := newSQLBatchWireFixture(t, 65, false)
	_, response := roundTripSQLBatchWire(t, fixture)
	if !response.OK || len(response.Found) != 65 || len(response.Documents) != 65 ||
		len(response.Observations) != 65 {
		t.Fatalf("found=%d documents=%d observations=%d",
			len(response.Found), len(response.Documents), len(response.Observations))
	}
	if maximum := fixture.client.maxActive.Load(); maximum == 0 || maximum > 3 {
		t.Fatalf("active group reads=%d, want 1..3", maximum)
	}
}

func TestReadBatchWireExactByteBoundAndIntentNeverReturnPartialValues(t *testing.T) {
	fixture := newSQLBatchWireFixture(t, 2, false)
	raw, response := roundTripSQLBatchWire(t, fixture)
	if !response.OK {
		t.Fatalf("baseline response=%s", raw)
	}
	fixture.request.MaxResultBytes = uint32(len(raw))
	exact, exactResponse := roundTripSQLBatchWire(t, fixture)
	if !exactResponse.OK || len(exact) != len(raw) {
		t.Fatalf("exact-bound response=%s baseline_bytes=%d", exact, len(raw))
	}
	fixture.request.MaxResultBytes--
	overflow, overflowResponse := roundTripSQLBatchWire(t, fixture)
	if overflowResponse.OK || string(overflow) !=
		`{"ok":false,"code":"overloaded","retryable":true}`+"\n" {
		t.Fatalf("overflow response=%s", overflow)
	}

	fixture.request.MaxResultBytes = 1 << 20
	for group := range fixture.client.values {
		fixture.client.intent = group
		break
	}
	intent, intentResponse := roundTripSQLBatchWire(t, fixture)
	if intentResponse.OK || string(intent) !=
		`{"ok":false,"code":"conflict","retryable":true}`+"\n" {
		t.Fatalf("intent response=%s", intent)
	}
}

func BenchmarkReadBatchWireEightGroups(b *testing.B) {
	fixture := newSQLBatchWireFixture(b, 8, false)
	request, err := buildNativeSQLBatchReadRequest(fixture.request)
	if err != nil {
		b.Fatal(err)
	}
	writer := vibejson.NewWriter(io.Discard)
	b.ReportAllocs()
	for b.Loop() {
		result, readErr := fixture.reader.ReadSQLBatch(context.Background(), request)
		if readErr != nil {
			b.Fatal(readErr)
		}
		response := nativeSQLBatchWireResponse{
			Result: &result, Expected: len(request.Queries), Maximum: request.MaxResultBytes,
		}
		if writeErr := writeNativeSQLBatchResponse(writer, &response); writeErr != nil {
			result.Release()
			b.Fatal(writeErr)
		}
		result.Release()
	}
}
