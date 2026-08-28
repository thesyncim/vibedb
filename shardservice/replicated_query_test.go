package shardservice

import (
	"bytes"
	"io"
	"testing"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestReplicatedSQLWireRoundTripBoundsAndCapability(t *testing.T) {
	var inner bytes.Buffer
	if err := EncodeRequest(&inner, &ShardRequest{SQL: `SELECT COUNT(*) FROM docs`, Distribution: "data", Shard: "all", AllocationGeneration: 5, RoutingVersion: 1, OwnershipEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	req := ReplicatedRequest{Operation: ReplicatedQueryLeader, Capability: serviceauthz.CapabilityDataRead, Fence: testReplicatedFence(), Query: inner.Bytes(), MaxValueBytes: 4096}
	req.Authority = serviceauthz.Authority{Generation: 1}
	req.Authority.Node[0] = 1
	for _, encode := range []func(io.Writer, *ReplicatedRequest) error{EncodeReplicatedRequest, EncodeReplicatedRequestBorrowed} {
		var frame bytes.Buffer
		if err := encode(&frame, &req); err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeReplicatedRequest(&frame)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Operation != req.Operation || decoded.Fence != req.Fence || !bytes.Equal(decoded.Query, req.Query) || cap(decoded.Query) != len(decoded.Query) {
			t.Fatal("SQL envelope changed")
		}
	}
	for _, mutate := range []func(*ReplicatedRequest){
		func(r *ReplicatedRequest) { r.Capability = serviceauthz.CapabilityBackup },
		func(r *ReplicatedRequest) { r.MaxValueBytes = MaxReplicatedSQLResultBytes + 1 },
		func(r *ReplicatedRequest) { r.Query = make([]byte, MaxReplicatedSQLRequestBytes+1) },
		func(r *ReplicatedRequest) { r.Operation = ReplicatedProbe },
		func(r *ReplicatedRequest) { r.MinimumApplied = 1 },
	} {
		bad := req
		mutate(&bad)
		if err := EncodeReplicatedRequest(io.Discard, &bad); err == nil {
			t.Fatal("invalid SQL envelope accepted")
		}
	}
}
