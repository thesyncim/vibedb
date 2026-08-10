package main

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// serveShard is one in-process shard server bound to a real loopback listener,
// plus the ownership coordinates a request must carry to be admitted.
type serveShard struct {
	endpoint distribution.EndpointID
	own      shardservice.Ownership
	address  string
}

// startShard opens a fresh durable catalog, serves it on a loopback port, and
// returns the shard's endpoint, ownership, and bound address.
func startShard(t *testing.T, endpoint distribution.EndpointID, own shardservice.Ownership) serveShard {
	t.Helper()
	db, err := sqldriver.Open(filepath.Join(t.TempDir(), string(own.Shard)+".vdb"))
	if err != nil {
		t.Fatalf("Open %s: %v", own.Shard, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	srv, err := shardservice.NewServer(db, own, shardservice.Options{})
	if err != nil {
		t.Fatalf("NewServer %s: %v", own.Shard, err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen %s: %v", own.Shard, err)
	}
	go func() { _ = srv.Serve(ln) }()
	return serveShard{endpoint: endpoint, own: own, address: ln.Addr().String()}
}

// seedShard runs setup statements against a shard over the real TCP client.
func seedShard(t *testing.T, c *gateway.Client, sh serveShard, stmts ...string) {
	t.Helper()
	for _, sql := range stmts {
		req := &shardservice.ShardRequest{
			SQL:            sql,
			Distribution:   sh.own.Distribution,
			Shard:          sh.own.Shard,
			RoutingVersion: sh.own.RoutingVersion,
			OwnershipEpoch: sh.own.Epoch,
			ExecutionMode:  shardservice.ExecutionReadWrite,
		}
		if _, err := c.Do(context.Background(), sh.address, req); err != nil {
			t.Fatalf("seed %q on %s: %v", sql, sh.own.Shard, err)
		}
	}
}

// TestServeGatewayEndToEnd stands up two real loopback shard services, persists a
// catalog pinning their endpoints, then runs the serve front-end and proves a
// scatter query dispatched as JSON returns the globally merged, ordered result
// and that the listener shuts down cleanly on cancellation.
func TestServeGatewayEndToEnd(t *testing.T) {
	const version = distribution.RoutingVersion(3)
	shardA := startShard(t, "ep-a", shardservice.Ownership{Distribution: "tenant_data", Shard: "-80", Epoch: 7, RoutingVersion: version})
	shardB := startShard(t, "ep-b", shardservice.Ownership{Distribution: "tenant_data", Shard: "80-", Epoch: 9, RoutingVersion: version})

	client := gateway.NewClient(nil)
	seedShard(t, client, shardA,
		`CREATE TABLE messages (tenant_id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`INSERT INTO messages (tenant_id, n) VALUES ('a1', 1)`,
		`INSERT INTO messages (tenant_id, n) VALUES ('a3', 3)`)
	seedShard(t, client, shardB,
		`CREATE TABLE messages (tenant_id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`INSERT INTO messages (tenant_id, n) VALUES ('b2', 2)`,
		`INSERT INTO messages (tenant_id, n) VALUES ('b4', 4)`)

	// Persist a catalog generation pinning the two endpoints to their addresses.
	shards := []distribution.Shard{
		{ID: "-80", Range: keyRange(0x00, false, 0x80), Leaders: []distribution.EndpointID{shardA.endpoint}, Epoch: 7},
		{ID: "80-", Range: keyRange(0x80, true, 0x00), Leaders: []distribution.EndpointID{shardB.endpoint}, Epoch: 9},
	}
	man, err := distribution.NewManifest("tenant_data", version, shards)
	if err != nil {
		t.Fatalf("NewManifest: %v", err)
	}
	config := distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: 1}},
		Placements:    []distribution.TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}}},
		Manifests:     []*distribution.Manifest{man},
	}
	snap, err := gateway.NewSnapshot(config, map[distribution.EndpointID]string{
		shardA.endpoint: shardA.address,
		shardB.endpoint: shardB.address,
	}, 5)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	catalogPath := filepath.Join(t.TempDir(), "catalog.json")
	if err := gateway.SaveSnapshot(catalogPath, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Run the serve front-end on its own loopback listener.
	exec, holder, err := newGateway(catalogPath)
	if err != nil {
		t.Fatalf("newGateway: %v", err)
	}
	front, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("front listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- serveGateway(ctx, front, exec, holder, func(string, ...any) {})
	}()

	// Issue a scatter read as JSON and read back the merged, ordered result.
	conn, err := net.Dial("tcp", front.Addr().String())
	if err != nil {
		t.Fatalf("dial front: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	req := serveRequest{
		Distribution: "tenant_data",
		SQL:          `SELECT n FROM messages ORDER BY n`,
		Class:        "batch",
		Order:        []serveOrderKey{{Column: 0}},
	}
	if err := json.NewEncoder(conn).Encode(&req); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	var resp serveResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("response error: %s", resp.Error)
	}
	if resp.Route != "Scatter" || resp.ShardsFanned != 2 {
		t.Fatalf("route = %q fanning %d, want Scatter fanning 2", resp.Route, resp.ShardsFanned)
	}
	if resp.Generation != 5 {
		t.Fatalf("served generation = %d, want 5", resp.Generation)
	}
	if got := rawInts(t, resp.Rows); !intsEqual(got, []int64{1, 2, 3, 4}) {
		t.Fatalf("merged rows = %v, want [1 2 3 4]", got)
	}

	// Cancellation shuts the front-end down cleanly.
	_ = conn.Close()
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serveGateway returned %v, want nil on shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveGateway did not shut down on cancellation")
	}
}

// TestNewGatewayReloadsCatalogAfterStaleRefusal exercises the production file
// refresher: the process starts on generation 1, the catalog file is atomically
// replaced with generation 2, and a routing-version refusal advances the
// holder and retries only against the newer generation.
func TestNewGatewayReloadsCatalogAfterStaleRefusal(t *testing.T) {
	const liveVersion = distribution.RoutingVersion(4)
	shardA := startShard(t, "ep-a", shardservice.Ownership{
		Distribution: "tenant_data", Shard: "-80", Epoch: 7, RoutingVersion: liveVersion,
	})
	shardB := startShard(t, "ep-b", shardservice.Ownership{
		Distribution: "tenant_data", Shard: "80-", Epoch: 9, RoutingVersion: liveVersion,
	})
	client := gateway.NewClient(nil)
	seedShard(t, client, shardA,
		`CREATE TABLE messages (tenant_id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`INSERT INTO messages (tenant_id, n) VALUES ('a', 1)`)
	seedShard(t, client, shardB,
		`CREATE TABLE messages (tenant_id STRING PRIMARY KEY, n INTEGER NOT NULL)`,
		`INSERT INTO messages (tenant_id, n) VALUES ('b', 2)`)

	makeSnapshot := func(generation uint64, version distribution.RoutingVersion) *gateway.Snapshot {
		t.Helper()
		man, err := distribution.NewManifest("tenant_data", version, []distribution.Shard{
			{ID: "-80", Range: keyRange(0x00, false, 0x80), Leaders: []distribution.EndpointID{shardA.endpoint}, Epoch: 7},
			{ID: "80-", Range: keyRange(0x80, true, 0x00), Leaders: []distribution.EndpointID{shardB.endpoint}, Epoch: 9},
		})
		if err != nil {
			t.Fatalf("NewManifest: %v", err)
		}
		snap, err := gateway.NewSnapshot(distribution.ClusterConfig{
			Distributions: []distribution.DistributionSpec{{Name: "tenant_data", Arity: 1, MapperVersion: distribution.NativeMapperVersion}},
			Placements:    []distribution.TablePlacement{{Table: "messages", Distribution: "tenant_data", Columns: []string{"/tenant_id"}}},
			Manifests:     []*distribution.Manifest{man},
		}, map[distribution.EndpointID]string{
			shardA.endpoint: shardA.address,
			shardB.endpoint: shardB.address,
		}, generation)
		if err != nil {
			t.Fatalf("NewSnapshot: %v", err)
		}
		return snap
	}

	catalogPath := filepath.Join(t.TempDir(), "catalog.json")
	if err := gateway.SaveSnapshot(catalogPath, makeSnapshot(1, 3)); err != nil {
		t.Fatalf("SaveSnapshot initial: %v", err)
	}
	exec, holder, err := newGateway(catalogPath)
	if err != nil {
		t.Fatalf("newGateway: %v", err)
	}
	if err := gateway.SaveSnapshot(catalogPath, makeSnapshot(2, liveVersion)); err != nil {
		t.Fatalf("SaveSnapshot newer: %v", err)
	}

	res, err := exec.Query(context.Background(), gateway.Query{
		Distribution: "tenant_data",
		Constraints:  distribution.BoundConstraints{distribution.UnknownDomain()},
		SQL:          `SELECT n FROM messages ORDER BY n`,
		Class:        gateway.ClassBatch,
		Order:        []gateway.OrderKey{{Column: 0}},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if res.Generation != 2 || res.Retries != 1 || holder.Current().Generation() != 2 {
		t.Fatalf("generation/retries/current = %d/%d/%d, want 2/1/2",
			res.Generation, res.Retries, holder.Current().Generation())
	}
	got := make([]int64, len(res.Rows))
	for i, row := range res.Rows {
		if len(row) != 1 {
			t.Fatalf("row %d has %d cells, want 1", i, len(row))
		}
		got[i], err = strconv.ParseInt(string(row[0].Bytes), 10, 64)
		if err != nil {
			t.Fatalf("row %d is not an integer: %v", i, err)
		}
	}
	if !intsEqual(got, []int64{1, 2}) {
		t.Fatalf("merged rows = %v, want [1 2]", got)
	}
}

// keyRange builds a shard key range from a start byte and either an open upper
// bound (endMax) or an exclusive end byte.
func keyRange(start byte, endMax bool, end byte) distribution.KeyRange {
	var s distribution.KeyspacePoint
	s[0] = start
	if endMax {
		return distribution.KeyRange{Start: s, End: distribution.KeyspaceEnd{Max: true}}
	}
	var e distribution.KeyspacePoint
	e[0] = end
	return distribution.KeyRange{Start: s, End: distribution.KeyspaceEnd{Point: e}}
}

// rawInts decodes a single-column integer result into its int64 sequence.
func rawInts(t *testing.T, rows [][]json.RawMessage) []int64 {
	t.Helper()
	out := make([]int64, len(rows))
	for i, row := range rows {
		if len(row) != 1 {
			t.Fatalf("row %d has %d cells, want 1", i, len(row))
		}
		v, err := strconv.ParseInt(string(row[0]), 10, 64)
		if err != nil {
			t.Fatalf("row %d cell %q is not an integer: %v", i, row[0], err)
		}
		out[i] = v
	}
	return out
}

// intsEqual reports whether two int64 slices are element-wise equal.
func intsEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
