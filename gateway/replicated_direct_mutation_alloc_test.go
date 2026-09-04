package gateway

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestReplicatedDirectMutationPreparedEncodeZeroAlloc(t *testing.T) {
	request := directMutationAllocationFixture(t)
	encoded, _, err := appendReplicatedDirectMutationCommand(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	workspace := make([]byte, 0, len(encoded))
	var encodeWorkspace directMutationEncodeWorkspace
	prepared, preparedControl, err := appendReplicatedDirectMutationCommandPrepared(
		workspace[:0], &encodeWorkspace, request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(prepared, encoded) {
		t.Fatal("prepared direct mutation bytes differ from canonical encode")
	}
	if preparedControl.ExpectedRevision != request.Key.IssuerSequence {
		t.Fatalf("prepared revision=%d", preparedControl.ExpectedRevision)
	}
	controlBytes, sizeErr := distributedtxn.ReplicatedCommandSize(preparedControl)
	if sizeErr != nil {
		t.Fatal(sizeErr)
	}
	sequence, sequenceErr := replication.TransactionClientSequence(
		encodeWorkspace.control[:controlBytes],
	)
	if sequenceErr != nil || sequence != request.Key.IssuerSequence {
		t.Fatalf("prepared sequence=%d err=%v", sequence, sequenceErr)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		var encodeErr error
		workspace, _, encodeErr = appendReplicatedDirectMutationCommandPrepared(
			workspace[:0], &encodeWorkspace, request,
		)
		if encodeErr != nil {
			panic(encodeErr)
		}
	})
	if allocations != 0 {
		t.Fatalf("prepared direct mutation encode allocations=%f", allocations)
	}
}

func BenchmarkReplicatedDirectMutationPreparedEncode(b *testing.B) {
	request := directMutationAllocationFixture(b)
	encoded, _, err := appendReplicatedDirectMutationCommand(nil, request)
	if err != nil {
		b.Fatal(err)
	}
	workspace := make([]byte, 0, len(encoded))
	var encodeWorkspace directMutationEncodeWorkspace
	_, _, err = appendReplicatedDirectMutationCommandPrepared(
		workspace[:0], &encodeWorkspace, request,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for range b.N {
		workspace, _, err = appendReplicatedDirectMutationCommandPrepared(
			workspace[:0], &encodeWorkspace, request,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReplicatedDirectMutationEncode(b *testing.B) {
	request := directMutationAllocationFixture(b)
	encoded, _, err := appendReplicatedDirectMutationCommand(nil, request)
	if err != nil {
		b.Fatal(err)
	}
	workspace := make([]byte, 0, len(encoded))
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for range b.N {
		workspace, _, err = appendReplicatedDirectMutationCommand(workspace[:0], request)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func directMutationAllocationFixture(tb testing.TB) ReplicatedDirectMutation {
	tb.Helper()
	route, _, _ := testReplicatedRouteCommand(tb)
	tenant := []byte("tenant")
	return ReplicatedDirectMutation{
		Key: requestledger.RequestKey{
			Scope: requestledger.ScopeAuthenticated, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
			Principal: requestledger.PrincipalID{0x21}, Request: requestledger.RequestID{0x31},
			IssuerEpoch: 7, IssuerSequence: 1, IssuerLane: requestledger.IssuerLane{0x32},
		},
		RequestDigest: replication.Digest{0x41}, Tenant: tenant,
		Target: ReplicatedTransactionTarget{
			Route: route, BucketBits: 8,
			IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			Batches: []replication.RelationMutationBatch{{
				Relation: 1, Mutations: []replication.Mutation{{
					Kind: replication.MutationPutAbsentOrEqual,
					Key:  []byte("direct-key"), Value: []byte(`{"id":1,"state":"paid"}`),
				}},
			}},
		},
	}
}
