package shardservice

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func benchmarkReplicatedSmallRequests(b *testing.B) []struct {
	name    string
	request *ReplicatedRequest
} {
	b.Helper()
	fence := testReplicatedFence()
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{1}, Generation: 1}
	return []struct {
		name    string
		request *ReplicatedRequest
	}{
		{
			name: "point_read",
			request: &ReplicatedRequest{
				Operation: ReplicatedReadLeader, Authority: authority,
				Capability: serviceauthz.CapabilityDataRead, Fence: fence,
				Relation: 1, Key: []byte("point-key"), MinimumApplied: 1,
				MaxValueBytes: 4096,
			},
		},
		{
			name: "small_proposal",
			request: &ReplicatedRequest{
				Operation: ReplicatedPropose, Authority: authority,
				Capability: serviceauthz.CapabilityDataWrite, Fence: fence,
				Command: testReplicatedCommandValue(b, fence, []byte("v")),
			},
		},
	}
}

func BenchmarkReplicatedBorrowedEncoderSmall(b *testing.B) {
	for _, test := range benchmarkReplicatedSmallRequests(b) {
		b.Run(test.name+"/fresh_global", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := EncodeReplicatedRequestBorrowed(io.Discard, test.request); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(test.name+"/warm_owned", func(b *testing.B) {
			var encoder FrameEncoder
			if err := encoder.EncodeReplicatedRequestBorrowed(io.Discard, test.request); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := encoder.EncodeReplicatedRequestBorrowed(io.Discard, test.request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReplicatedBorrowedEncoderTLSWrites(b *testing.B) {
	for _, test := range benchmarkReplicatedSmallRequests(b) {
		b.Run(test.name+"/fresh_global", func(b *testing.B) {
			benchmarkReplicatedTLSWrites(b, test.request, false)
		})
		b.Run(test.name+"/warm_owned", func(b *testing.B) {
			benchmarkReplicatedTLSWrites(b, test.request, true)
		})
	}
}

func benchmarkReplicatedTLSWrites(b *testing.B, request *ReplicatedRequest, owned bool) {
	b.Helper()
	authority := newShardTLSAuthority(b)
	serverIdentity := shardPeerIdentity(19, 41)
	clientIdentity := shardPeerIdentity(19, 61)
	serverProfile := authority.profile(b, serverIdentity)
	clientProfile := authority.profile(b, clientIdentity)
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		connection, err := serverProfile.Server(context.Background(), raw, rafttransport.TrafficShardNative, deadline)
		if err != nil {
			serverDone <- err
			return
		}
		_, err = io.Copy(io.Discard, connection)
		_ = connection.Close()
		serverDone <- err
	}()
	raw, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		b.Fatal(err)
	}
	meter := &replicatedTLSWriteMeter{Conn: raw}
	connection, err := clientProfile.Client(context.Background(), meter, serverIdentity.Node, rafttransport.TrafficShardNative, deadline)
	if err != nil {
		_ = raw.Close()
		_ = listener.Close()
		b.Fatal(err)
	}
	var encoder FrameEncoder
	if owned {
		if err := encoder.EncodeReplicatedRequestBorrowed(connection, request); err != nil {
			b.Fatal(err)
		}
	} else if err := EncodeReplicatedRequestBorrowed(connection, request); err != nil {
		b.Fatal(err)
	}
	baselineWrites := meter.writes.Load()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if owned {
			err = encoder.EncodeReplicatedRequestBorrowed(connection, request)
		} else {
			err = EncodeReplicatedRequestBorrowed(connection, request)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(meter.writes.Load()-baselineWrites)/float64(b.N), "tlswrites/op")
	_ = connection.Close()
	_ = listener.Close()
	if err := <-serverDone; err != nil {
		b.Fatal(err)
	}
}
