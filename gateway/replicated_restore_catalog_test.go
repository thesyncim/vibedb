package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestReplicatedRestoreCatalogExactCASAndLinearizableObservation(t *testing.T) {
	authority, client, _ := newCatalogAuthorityFixture(t)
	restoreOperator := serviceauthz.Authority{Node: [16]byte{0x91}, Generation: 17}
	gate := restoreCatalogGate(t, restoreOperator)
	catalog, err := NewReplicatedRestoreCatalog(ReplicatedRestoreCatalogOptions{
		Catalog: authority, Session: newRestoreCatalogSession(t, authority),
		Gate: gate, Operator: restoreOperator,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(t.Context(), restoreOperator)
	if err != nil {
		t.Fatal(err)
	}
	witness := restoreCatalogWitness(authority, 1)
	command, err := clusterrestore.AppendCatalogActivation(nil, witness)
	if err != nil {
		t.Fatal(err)
	}
	result, err := catalog.ProposeRestoreActivation(ctx, command)
	if err != nil || !bytes.Equal(result, witness.CatalogDigest[:]) {
		t.Fatalf("propose result=%x err=%v", result, err)
	}
	// PutAbsentOrEqual is the exact replay boundary, not a second transition.
	again, err := catalog.ProposeRestoreActivation(ctx, command)
	if err != nil || !bytes.Equal(again, result) {
		t.Fatalf("exact replay result=%x err=%v", again, err)
	}
	var observedReads atomic.Uint64
	client.mu.Lock()
	client.onRead = func(key []byte) {
		if bytes.Equal(key, restoreCatalogDocumentKey) {
			observedReads.Add(1)
		}
	}
	client.mu.Unlock()
	observed, err := catalog.ObserveRestoreActivation(ctx, witness.Operation)
	if err != nil || observed != witness || observedReads.Load() == 0 {
		t.Fatalf("observe=%+v reads=%d err=%v", observed, observedReads.Load(), err)
	}

	conflict := restoreCatalogWitness(authority, 2)
	conflict.Operation = witness.Operation
	conflict.CatalogDigest = restoreCatalogWitnessDigest(conflict)
	conflictingCommand, _ := clusterrestore.AppendCatalogActivation(nil, conflict)
	if _, err = catalog.ProposeRestoreActivation(ctx, conflictingCommand); !errors.Is(
		err, ErrRestoreCatalogConflict,
	) {
		t.Fatalf("conflicting one-time activation err=%v", err)
	}

	client.mu.Lock()
	stored := append([]byte(nil), client.rows[string(restoreCatalogDocumentKey)]...)
	client.mu.Unlock()
	opened, err := openRestoreCatalogDocument(stored)
	if err != nil || opened != witness {
		t.Fatalf("stored witness=%+v err=%v", opened, err)
	}
	for _, malformed := range [][]byte{
		stored[:len(stored)-1], append(append([]byte(nil), stored...), 0),
	} {
		if _, err = openRestoreCatalogDocument(malformed); !errors.Is(
			err, ErrReplicatedRestoreCatalog,
		) {
			t.Fatalf("malformed restore document accepted: %v", err)
		}
	}
}

func TestReplicatedRestoreCatalogSettlesResponseLossByLinearizableRead(t *testing.T) {
	authority, client, _ := newCatalogAuthorityFixture(t)
	operator := serviceauthz.Authority{Node: [16]byte{0x92}, Generation: 18}
	catalog, err := NewReplicatedRestoreCatalog(ReplicatedRestoreCatalogOptions{
		Catalog: authority, Session: newRestoreCatalogSession(t, authority),
		Gate: restoreCatalogGate(t, operator), Operator: operator,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := serviceauthz.WithAuthority(t.Context(), operator)
	witness := restoreCatalogWitness(authority, 3)
	command, _ := clusterrestore.AppendCatalogActivation(nil, witness)
	client.mu.Lock()
	client.unknownNext = true
	client.mu.Unlock()
	result, err := catalog.ProposeRestoreActivation(ctx, command)
	if err != nil || !bytes.Equal(result, witness.CatalogDigest[:]) {
		t.Fatalf("response-loss settlement=%x err=%v", result, err)
	}
	client.mu.Lock()
	pending := client.holdUnknown
	client.mu.Unlock()
	if !pending {
		t.Fatal("fixture did not lose the applied response")
	}
}

func TestReplicatedRestoreCatalogRejectsBackupTopologyAndMembershipCallers(t *testing.T) {
	authority, client, _ := newCatalogAuthorityFixture(t)
	restore := serviceauthz.Authority{Node: [16]byte{0xa1}, Generation: 21}
	backup := serviceauthz.Authority{Node: [16]byte{0xa2}, Generation: 21}
	topology := serviceauthz.Authority{Node: [16]byte{0xa3}, Generation: 21}
	membership := serviceauthz.Authority{Node: [16]byte{0xa4}, Generation: 21}
	policy, err := serviceauthz.NewPolicy(21, []serviceauthz.Entry{
		{Node: restore.Node, Capabilities: serviceauthz.CapabilityRestoreActivate},
		{Node: backup.Node, Capabilities: serviceauthz.CapabilityBackup},
		{Node: topology.Node, Capabilities: serviceauthz.CapabilityTopology},
		{Node: membership.Node, Capabilities: serviceauthz.CapabilityMembership},
	})
	if err != nil {
		t.Fatal(err)
	}
	gate, _ := serviceauthz.NewGate(policy)
	session := newRestoreCatalogSession(t, authority)
	for _, operator := range []serviceauthz.Authority{backup, topology, membership} {
		if _, err = NewReplicatedRestoreCatalog(ReplicatedRestoreCatalogOptions{
			Catalog: authority, Session: session, Gate: gate, Operator: operator,
		}); !errors.Is(err, ErrReplicatedRestoreCatalog) {
			t.Fatalf("constructor admitted capability %x: %v", operator.Node, err)
		}
	}
	catalog, err := NewReplicatedRestoreCatalog(ReplicatedRestoreCatalogOptions{
		Catalog: authority, Session: session, Gate: gate, Operator: restore,
	})
	if err != nil {
		t.Fatal(err)
	}
	witness := restoreCatalogWitness(authority, 4)
	command, _ := clusterrestore.AppendCatalogActivation(nil, witness)
	for _, caller := range []serviceauthz.Authority{backup, topology, membership} {
		ctx, _ := serviceauthz.WithAuthority(t.Context(), caller)
		if _, err = catalog.ProposeRestoreActivation(ctx, command); !errors.Is(
			err, ErrReplicatedRestoreCatalog,
		) {
			t.Fatalf("caller %x admitted: %v", caller.Node, err)
		}
	}
	client.mu.Lock()
	_, mutated := client.rows[string(restoreCatalogDocumentKey)]
	client.mu.Unlock()
	if mutated {
		t.Fatal("unauthorized caller published an activation row")
	}
}

func newRestoreCatalogSession(t *testing.T, authority *ReplicatedCatalogAuthority) *NativeSession {
	t.Helper()
	session, err := NewNativeSession(NativeSessionOptions{
		Executor: authority.executor, Route: authority.route,
		Distribution: string(ReplicatedCatalogDistribution), Shard: string(ReplicatedCatalogShard),
		Tenant: []byte("restore-catalog"), ClientID: replication.ID128{0x92},
		Resolver:           BaseRelationResolver{Relation: authority.relation},
		ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: 1,
		InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(context.Background(), authority.authority)
	if err == nil {
		_, err = session.Open(ctx, 1<<50)
	}
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func restoreCatalogGate(t *testing.T, operator serviceauthz.Authority) *serviceauthz.Gate {
	t.Helper()
	policy, err := serviceauthz.NewPolicy(operator.Generation, []serviceauthz.Entry{{
		Node: operator.Node, Capabilities: serviceauthz.CapabilityRestoreActivate,
	}})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		t.Fatal(err)
	}
	return gate
}

func restoreCatalogWitness(
	authority *ReplicatedCatalogAuthority, marker byte,
) clusterrestore.CatalogWitness {
	witness := clusterrestore.CatalogWitness{
		Operation:           [32]byte{marker},
		CatalogGroup:        restoreCatalogGroup(authority.route.Group),
		GroupsDigest:        [32]byte{marker + 1},
		TargetPolicyDigest:  [32]byte{marker + 2},
		TargetCatalogDigest: [32]byte{marker + 3},
	}
	witness.CatalogDigest = restoreCatalogWitnessDigest(witness)
	return witness
}

func restoreCatalogWitnessDigest(witness clusterrestore.CatalogWitness) (digest [sha256.Size]byte) {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/restore/catalog-witness/format-1\x00"))
	_, _ = hash.Write(witness.Operation[:])
	_, _ = hash.Write(witness.CatalogGroup[:])
	_, _ = hash.Write(witness.GroupsDigest[:])
	_, _ = hash.Write(witness.TargetPolicyDigest[:])
	_, _ = hash.Write(witness.TargetCatalogDigest[:])
	copy(digest[:], hash.Sum(nil))
	return digest
}
