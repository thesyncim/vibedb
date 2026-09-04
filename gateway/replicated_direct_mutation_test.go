package gateway

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestReplicatedDirectMutationIsOneProposalWithCrossGatewayExactRetry(t *testing.T) {
	route, client, _ := newRouteSessionMachine(t)
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	tenant := []byte("tenant")
	key := requestledger.RequestKey{
		Scope:        requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		Principal:    requestledger.PrincipalID{0x21}, Request: requestledger.RequestID{0x31},
		IssuerEpoch: 7, IssuerSequence: 1, IssuerLane: requestledger.IssuerLane{0x32},
	}
	documentKey := []byte("direct-key")
	document := []byte(`{"id":1,"state":"paid"}`)
	request := ReplicatedDirectMutation{
		Key: key, RequestDigest: replication.Digest{0x41}, Tenant: tenant,
		Target: ReplicatedTransactionTarget{
			Route: route, BucketBits: 8,
			IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			Batches: []replication.RelationMutationBatch{{
				Relation: 1, Mutations: []replication.Mutation{{
					Kind: replication.MutationPutAbsentOrEqual, Key: documentKey, Value: document,
				}},
			}},
		},
	}
	first, err := executor.DirectMutate(ctx, request)
	if err != nil || !first.Committed || first.AffectedRows != 1 ||
		first.ResultCode != replicatedstate.ResultApplied || first.Applied != 2 ||
		client.state.Applied != 2 {
		t.Fatalf("first direct result=%+v applied=%d err=%v", first, client.state.Applied, err)
	}
	stored, err := client.machine.PointReadInto(
		1, documentKey, 2, replication.MaxMutationValueBytes, nil,
	)
	if err != nil || !stored.Found || !bytes.Equal(stored.Value, document) {
		t.Fatalf("direct value=%q found=%v err=%v", stored.Value, stored.Found, err)
	}

	// A fresh gateway can reconstruct the command solely from caller identity,
	// request digest, route, and canonical mutations. The duplicate proposal
	// returns the first entry's retained applied index and result.
	restarted, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := restarted.DirectMutate(ctx, request)
	if err != nil || !retry.Duplicate || retry.ID != first.ID || retry.Applied != first.Applied ||
		retry.Committed != first.Committed || retry.AffectedRows != first.AffectedRows ||
		retry.ResultCode != first.ResultCode || client.state.Applied != 3 {
		t.Fatalf("direct retry=%+v first=%+v applied=%d err=%v", retry, first, client.state.Applied, err)
	}

	conflicting := request
	conflicting.RequestDigest[0]++
	conflict, err := restarted.DirectMutate(ctx, conflicting)
	if !errors.Is(err, ErrReplicatedTransactionConflict) || conflict.Committed ||
		conflict.ResultCode != replicatedstate.ResultTransactionConflict ||
		client.state.Applied != 4 {
		t.Fatalf("direct conflict=%+v applied=%d err=%v", conflict, client.state.Applied, err)
	}
	stored, err = client.machine.PointReadInto(
		1, documentKey, 4, replication.MaxMutationValueBytes, nil,
	)
	if err != nil || !stored.Found || !bytes.Equal(stored.Value, document) {
		t.Fatalf("conflict changed direct value=%q found=%v err=%v", stored.Value, stored.Found, err)
	}

	next := request
	next.Key.Request[0]++
	next.Key.IssuerSequence++
	next.RequestDigest[0]++
	nextKey := []byte("direct-key-next")
	nextValue := []byte(`{"id":2,"state":"paid"}`)
	next.Target.Batches = []replication.RelationMutationBatch{{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: nextKey, Value: nextValue,
		}},
	}}
	advanced, err := restarted.DirectMutate(ctx, next)
	if err != nil || !advanced.Committed || advanced.Duplicate || advanced.ID != first.ID ||
		advanced.Applied != 5 || client.state.Applied != 5 {
		t.Fatalf("advanced direct=%+v applied=%d err=%v",
			advanced, client.state.Applied, err)
	}
	stale, err := restarted.DirectMutate(ctx, request)
	if !errors.Is(err, ErrReplicatedTransactionConflict) || stale.Committed ||
		stale.ResultCode != replicatedstate.ResultTransactionConflict || client.state.Applied != 6 {
		t.Fatalf("stale direct=%+v applied=%d err=%v", stale, client.state.Applied, err)
	}
}
