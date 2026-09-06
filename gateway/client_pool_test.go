package gateway

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/shardservice"
)

// TestClientReusesConnectionAfterCompleteFrames proves both success and a typed
// shard refusal leave the stream aligned for the next request. One shard-side
// Session serves all three calls.
func TestClientReusesConnectionAfterCompleteFrames(t *testing.T) {
	srv := newShardServer(t)
	var dials atomic.Uint64
	client := NewClient(func(context.Context, string) (net.Conn, error) {
		dials.Add(1)
		clientConn, serverConn := net.Pipe()
		go srv.ServeConn(serverConn)
		return clientConn, nil
	})
	t.Cleanup(func() { _ = client.Close() })

	wrongShard := ownedReq("SELECT 1")
	wrongShard.Shard = "80-"
	if _, err := client.Do(context.Background(), "shard-a", wrongShard); !errors.Is(err, sentinelFor(shardservice.ErrorNotOwner)) {
		t.Fatalf("wrong-shard error = %v, want NotOwner", err)
	}
	for range 2 {
		resp, err := client.Do(context.Background(), "shard-a", ownedReq("SELECT 1"))
		if err != nil {
			t.Fatalf("healthy request: %v", err)
		}
		if resp.Kind != shardservice.ResponseRows || len(resp.Rows) != 1 {
			t.Fatalf("healthy response = %+v, want one row", resp)
		}
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials = %d, want one persistent connection", got)
	}
}

// TestClientDiscardsBrokenConnection proves an incomplete response cannot poison
// a future call: the failed connection is closed and the next request redials.
func TestClientDiscardsBrokenConnection(t *testing.T) {
	var dials atomic.Uint64
	client := NewClient(func(context.Context, string) (net.Conn, error) {
		n := dials.Add(1)
		clientConn, serverConn := net.Pipe()
		if n == 1 {
			go func() {
				defer serverConn.Close()
				_, _ = shardservice.DecodeRequest(serverConn)
			}()
		} else {
			go serveBenchmarkShard(serverConn)
		}
		return clientConn, nil
	})
	t.Cleanup(func() { _ = client.Close() })

	if _, err := client.Do(context.Background(), "shard-a", ownedReq("SELECT 1")); err == nil {
		t.Fatal("incomplete response unexpectedly succeeded")
	}
	if _, err := client.Do(context.Background(), "shard-a", ownedReq("SELECT 1")); err != nil {
		t.Fatalf("request after broken connection: %v", err)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("dials = %d, want redial after transport failure", got)
	}
}

// TestClientIdlePoolBounds proves both endpoint cardinality and per-endpoint
// connection counts remain bounded without a background reaper.
func TestClientIdlePoolBounds(t *testing.T) {
	client := NewClientWithOptions(nil, ClientOptions{
		MaxIdleConnections:            2,
		MaxIdleConnectionsPerEndpoint: 1,
	})
	now := time.Unix(100, 0)
	client.now = func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	first := newTrackedPipe(t)
	newerAtFirstEndpoint := newTrackedPipe(t)
	secondEndpoint := newTrackedPipe(t)
	newestEndpoint := newTrackedPipe(t)

	client.put("shard-a", first, shardservice.FrameEncoder{})
	client.put("shard-a", newerAtFirstEndpoint, shardservice.FrameEncoder{})
	client.put("shard-b", secondEndpoint, shardservice.FrameEncoder{})
	client.put("shard-c", newestEndpoint, shardservice.FrameEncoder{})

	client.mu.Lock()
	if client.nidle != 2 || len(client.idle) != 2 {
		t.Fatalf("idle pool = %d connections across %d endpoints, want 2 across 2", client.nidle, len(client.idle))
	}
	client.mu.Unlock()
	if first.closes.Load() != 1 {
		t.Fatal("per-endpoint pressure did not evict the oldest connection")
	}
	if newerAtFirstEndpoint.closes.Load() != 1 {
		t.Fatal("total pressure did not evict the globally oldest connection")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if secondEndpoint.closes.Load() != 1 || newestEndpoint.closes.Load() != 1 {
		t.Fatal("Close did not release every retained connection")
	}
}

// TestClientExpiresIdleConnection uses a synthetic clock to prove expiry is
// deterministic and lazy: taking an aged connection closes it and requests a
// fresh dial without a timer goroutine.
func TestClientExpiresIdleConnection(t *testing.T) {
	client := NewClientWithOptions(nil, ClientOptions{IdleConnectionTimeout: time.Minute})
	now := time.Unix(100, 0)
	client.now = func() time.Time { return now }
	idle := newTrackedPipe(t)
	client.put("shard-a", idle, shardservice.FrameEncoder{})

	now = now.Add(time.Minute)
	got, err := client.take("shard-a")
	if err != nil || got.conn != nil {
		t.Fatalf("take expired = (%v, %v), want (nil, nil)", got.conn, err)
	}
	if idle.closes.Load() != 1 {
		t.Fatal("expired idle connection was not closed")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestClientCloseRefusesNewRequests(t *testing.T) {
	client := NewClient(nil)
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, err := client.Do(context.Background(), "shard-a", ownedReq("SELECT 1"))
	if !errors.Is(err, ErrClientClosed) {
		t.Fatalf("Do after Close = %v, want ErrClientClosed", err)
	}
}

type trackedPipe struct {
	net.Conn
	closes atomic.Uint64
	once   sync.Once
}

func (c *trackedPipe) Close() error {
	var err error
	c.once.Do(func() {
		c.closes.Add(1)
		err = c.Conn.Close()
	})
	return err
}

func newTrackedPipe(t *testing.T) *trackedPipe {
	t.Helper()
	client, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	return &trackedPipe{Conn: client}
}
