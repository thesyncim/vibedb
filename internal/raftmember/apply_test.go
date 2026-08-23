package raftmember

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func testApplyOptions() sqldriver.ReplicatedApplyOptions {
	return sqldriver.ReplicatedApplyOptions{
		MaxSessions: 128,
		RetryWindow: 8,
		TxnLimits: durable.TxnLimits{
			MaxCollections: 16,
			MaxDocuments:   256,
			MaxBytes:       64 << 20,
		},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id",
			TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
			Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	}
}

func testApplyCommand(
	identity sqldriver.ReplicatedShardStoreIdentity,
	sequence uint64,
	key, document []byte,
) []byte {
	b := identity.Binding
	fingerprint := sha256.Sum256([]byte{byte(sequence), 0x7e})
	encoded, err := replication.AppendCommand(nil, replication.Command{
		ClusterID:             replication.ID128(b.ClusterID),
		ClusterIncarnation:    replication.ID128(b.ClusterIncarnation),
		TopologyRecoveryEpoch: b.TopologyRecoveryEpoch,
		Distribution:          b.Distribution, Shard: b.Shard,
		AllocationGeneration: b.AllocationGeneration,
		ShardIncarnation:     replication.ID128(b.ShardIncarnation),
		GroupID:              replication.ID128(b.GroupID), ReplicaSetVersion: 1,
		ActivePolicyGeneration: b.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        b.Authority.ProtectionEpoch,
		OwnershipEpoch:         b.Authority.OwnershipEpoch,
		SchemaGeneration:       b.Authority.SchemaGeneration,
		RoutingVersion:         b.Authority.RoutingVersion,
		RouteGeneration:        b.Authority.RouteGeneration,
		Tenant:                 []byte("tenant"), ClientID: replication.ID128{3},
		ClientEpoch: 1, ClientSequence: sequence, Fingerprint: fingerprint,
		Collection: identity.UserTable,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: key, Value: document,
		}},
	})
	if err != nil {
		panic(err)
	}
	return encoded
}

func TestOpenPreparedApplyAndExactRestart(t *testing.T) {
	walIdentity := testWALIdentity(101)
	_, wal, _, _ := createWAL(t, walIdentity)
	authority := testAuthorityProfile()
	path, database, _ := prepareSQLRoot(t, walIdentity, "apply")
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind apply SQL root", err)
	if err != nil {
		t.Fatalf("BindPreparedSQL: %v", err)
	}
	claim, applyIdentity, err := OpenPreparedApply(
		wal, database, authority, base, testApplyOptions(),
	)
	skipIfStrictAllocationUnsupported(t, "open prepared apply", err)
	if err != nil {
		t.Fatalf("OpenPreparedApply: %v", err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	key, _ := orderedkey.AppendJSONString(nil, []byte(`"a"`), orderedkey.Ascending)
	document := []byte(`{"id":"a","value":1}`)
	command := testApplyCommand(base, 1, key, document)
	if _, err := claim.ApplyNormal(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryNormal,
	}, command); err != nil {
		t.Fatalf("ApplyNormal: %v", err)
	}
	lookup, err := claim.LookupCompletion(command)
	if err != nil {
		t.Fatalf("LookupCompletion: %v", err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != replicatedstate.ResultApplied {
		t.Fatalf("completion = %+v,%v", completion, err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, reopenedClaim, err := OpenBoundSQLWithApply(
		path, wal, authority, base, applyIdentity,
	)
	skipIfStrictAllocationUnsupported(t, "reopen SQL root with apply", err)
	if err != nil {
		t.Fatalf("OpenBoundSQLWithApply: %v", err)
	}
	if reopenedClaim.Applied() != 2 {
		t.Fatalf("reopened Applied = %d, want 2", reopenedClaim.Applied())
	}
	if _, err := reopenedClaim.LookupCompletion(command); err != nil {
		t.Fatalf("reopened completion: %v", err)
	}
	session, err := reopened.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := session.Prepare(context.Background(), `SELECT id FROM docs WHERE id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := prepared.Query(context.Background(), []any{"a"})
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("replicated row is missing after exact restart")
	}
	if err := errors.Join(rows.Close(), prepared.Close(), session.Close()); err != nil {
		t.Fatal(err)
	}
	if err := reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenBoundSQLWithApplyForSettlementPropagatesPlacement(t *testing.T) {
	walIdentity := testWALIdentity(102)
	_, wal, _, _ := createWAL(t, walIdentity)
	authority := testAuthorityProfile()
	path, database, _ := prepareSQLRoot(t, walIdentity, "apply-settlement")
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind settlement SQL root", err)
	if err != nil {
		t.Fatal(err)
	}
	options := testApplyOptions()
	claim, identity, err := OpenPreparedApply(wal, database, authority, base, options)
	skipIfStrictAllocationUnsupported(t, "open settlement apply", err)
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, reopenedClaim, settled, err := OpenBoundSQLWithApplyForSettlement(
		path, wal, authority, base, options,
	)
	skipIfStrictAllocationUnsupported(t, "settle SQL root with apply", err)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = reopenedClaim.Close()
		_ = reopened.Close()
	}()
	if settled != identity || settled.Placement != options.Placement {
		t.Fatalf("settled apply identity = %+v, want %+v", settled, identity)
	}
	if actual, err := reopenedClaim.Identity(); err != nil || actual != identity {
		t.Fatalf("settled claim identity = %+v,%v, want %+v", actual, err, identity)
	}
}
