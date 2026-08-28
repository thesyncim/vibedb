package gateway

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type sqlRF3TestTransport struct {
	mu       sync.Mutex
	routes   map[raftmember.GroupKey]ReplicatedRoute
	queries  int
	fail     bool
	sqlError bool
	started  chan struct{}
}

func (c *sqlRF3TestTransport) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint, req *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	route := c.routes[req.Fence.Group]
	state := shardservice.ReplicatedMemberState{Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration, Command: route.Command, MemberID: endpoint.Member, StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, Term: 3}, LeaderID: 2, Applied: 20, Commit: 20, CheckpointApplied: 1}
	if req.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: state}, nil
	}
	if req.Operation != shardservice.ReplicatedQueryLeader || req.Capability != serviceauthz.CapabilityDataRead || endpoint.Member != 2 {
		return nil, ErrReplicatedRoute
	}
	inner, err := shardservice.DecodeRequest(bytes.NewReader(req.Query))
	if err != nil {
		return nil, err
	}
	if inner.Authority != req.Authority || inner.Shard != route.Shard {
		return nil, ErrReplicatedRoute
	}
	if inner.Transaction.Operation != 0 || !inner.ReadFenceID.IsZero() {
		return nil, errors.New("RF3 must not use legacy read fences")
	}
	c.queries++
	if c.started != nil {
		select {
		case c.started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if c.fail && route.Shard == "right" {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalReadIntentActive, HasState: true, State: state}, nil
	}
	name, value := "id", `"a"`
	if route.Shard == "right" {
		value = `"z"`
	}
	if strings.Contains(strings.ToUpper(inner.SQL), "COUNT(") {
		name, value = "count(*)", "2"
	}
	var encoded bytes.Buffer
	response := shardservice.RowsResponse([]shardservice.Column{{Name: name, TypeOID: 114}}, [][]shardservice.Cell{{{Bytes: []byte(value)}}})
	if c.sqlError {
		response = shardservice.NewErrorResponse(shardservice.ErrorMalformedRequest, "SQL refused")
	}
	if err := shardservice.EncodeResponse(&encoded, response); err != nil {
		return nil, err
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedQueryResult, HasState: true, State: state, ReadApplied: 20, Value: encoded.Bytes()}, nil
}

func newSQLRF3TestExecutor(t *testing.T) (*Executor, *sqlRF3TestTransport) {
	t.Helper()
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	boundary := distribution.KeyspacePoint{0x80}
	manifest, err := distribution.NewManifest("data", 3, []distribution.Shard{
		{ID: "left", AllocationGeneration: 1, Epoch: 7, Leaders: []distribution.EndpointID{"peer-a", "peer-b", "peer-c"}, Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Point: boundary}}},
		{ID: "right", AllocationGeneration: 2, Epoch: 7, Leaders: []distribution.EndpointID{"peer-a", "peer-b", "peer-c"}, Range: distribution.KeyRange{Start: boundary, End: distribution.KeyspaceEnd{Max: true}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Manifests = []*distribution.Manifest{manifest}
	left, right := descriptor, descriptor
	left.Shard = "left"
	right.Shard = "right"
	right.AllocationGeneration = 2
	right.Group.GroupID[0]++
	right.Group.ShardIncarnation[0]++
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{left, right}, []ReplicatedTableProfile{profile})
	if err != nil {
		t.Fatal(err)
	}
	client := &sqlRF3TestTransport{routes: make(map[raftmember.GroupKey]ReplicatedRoute)}
	for _, id := range []distribution.ShardID{"left", "right"} {
		route, ok := snapshot.ResolveReplicatedRoute("data", id, nil)
		if !ok {
			t.Fatal("missing route")
		}
		client.routes[route.Group] = route
	}
	native, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return NewExecutor(&ReplicatedSQLTransport{Executor: native}, NewCatalogHolder(snapshot), Options{}), client
}

func TestRF3SQLReusesTargetingScatterGlobalOrderAndAggregate(t *testing.T) {
	executor, client := newSQLRF3TestExecutor(t)
	for _, test := range []struct {
		sql, want string
		calls     int
	}{
		{`SELECT id FROM messages ORDER BY id DESC LIMIT 1`, `"z"`, 2},
		{`SELECT COUNT(*) FROM messages`, "4", 2},
		{`SELECT id FROM messages WHERE id = 'a'`, "", 1},
	} {
		before := client.queries
		result, err := executor.Query(context.Background(), Query{SQL: test.sql, Class: ClassBatch})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 1 || client.queries-before != test.calls || result.ShardsFanned != test.calls {
			t.Fatalf("%s: rows=%v calls=%d shards=%d", test.sql, result.Rows, client.queries-before, result.ShardsFanned)
		}
		if test.want != "" && string(result.Rows[0][0].Bytes) != test.want {
			t.Fatalf("%s: got %s", test.sql, result.Rows[0][0].Bytes)
		}
		if len(result.Observations) != test.calls {
			t.Fatalf("missing group cuts: %+v", result.Observations)
		}
		for i, observation := range result.Observations {
			if observation.Applied != 20 || (i > 0 && bytes.Compare(result.Observations[i-1].RouteID[:], observation.RouteID[:]) >= 0) {
				t.Fatalf("invalid observation vector: %+v", result.Observations)
			}
		}
	}
	client.fail = true
	result, err := executor.Query(context.Background(), Query{SQL: `SELECT id FROM messages ORDER BY id`, Class: ClassBatch})
	if err == nil || result != nil {
		t.Fatalf("failed shard leaked result: %+v %v", result, err)
	}
	var refusal *ReplicatedRefusalError
	if !errors.As(err, &refusal) {
		t.Fatalf("lost refusal: %v", err)
	}
	client.fail, client.sqlError = false, true
	result, err = executor.Query(context.Background(), Query{SQL: `SELECT id FROM messages WHERE id = 'a'`, Class: ClassBatch})
	var sqlErr *ShardError
	if result != nil || !errors.As(err, &sqlErr) {
		t.Fatalf("SQL error became success: %+v %v", result, err)
	}
}
