package gateway

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/shardservice"
)

// BenchmarkClientRoundTrip isolates the gateway transport cost with an in-memory
// shard peer. The fresh and pooled modes quantify connection setup, teardown,
// and allocation overhead separately from the shared codec work.
func BenchmarkClientRoundTrip(b *testing.B) {
	for _, mode := range []struct {
		name         string
		disableReuse bool
	}{
		{name: "pooled"},
		{name: "fresh", disableReuse: true},
	} {
		b.Run(mode.name, func(b *testing.B) {
			for _, parallel := range []bool{false, true} {
				name := "sequential"
				if parallel {
					name = "parallel"
				}
				b.Run(name, func(b *testing.B) {
					var dials atomic.Uint64
					client := NewClientWithOptions(func(context.Context, string) (net.Conn, error) {
						dials.Add(1)
						clientConn, shardConn := net.Pipe()
						go serveBenchmarkShard(shardConn)
						return clientConn, nil
					}, ClientOptions{DisableConnectionReuse: mode.disableReuse})
					req := ownedReq("SELECT 1")

					b.ReportAllocs()
					b.ResetTimer()
					run := func() {
						resp, err := client.Do(context.Background(), "shard-a", req)
						if err != nil {
							b.Fatalf("Do: %v", err)
						}
						if resp.Kind != shardservice.ResponseRows {
							b.Fatalf("response kind = %s, want Rows", resp.Kind)
						}
					}
					if parallel {
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								run()
							}
						})
					} else {
						for range b.N {
							run()
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(dials.Load())/float64(b.N), "dials/op")
					if err := client.Close(); err != nil {
						b.Fatalf("Close: %v", err)
					}
				})
			}
		})
	}
}

func serveBenchmarkShard(conn net.Conn) {
	defer conn.Close()
	for {
		if _, err := shardservice.DecodeRequest(conn); err != nil {
			return
		}
		if err := shardservice.EncodeResponse(conn, shardservice.RowsResponse(nil, nil)); err != nil {
			return
		}
	}
}
