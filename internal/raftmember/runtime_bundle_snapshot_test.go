package raftmember

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestRuntimeSnapshotStateCoversCompleteRelationBundle(t *testing.T) {
	walIdentity := testWALIdentity(252)
	walOptions := testWALOptions()
	walOptions.MaxFileBytes = 256 << 20
	walOptions.MaxLiveBytes = 2 * raftstore.MinimumReadyLiveBytes
	walPath := filepath.Join(t.TempDir(), "runtime-bundle-snapshot.wal")
	index, term := uint64(1), uint64(1)
	wal, err := raftstore.Create(walPath, walIdentity, testWALKey(), raftstore.Bootstrap{
		TopologyRecoveryEpoch: testTopologyRecoveryEpoch,
		Snapshot: &pb.Snapshot{
			Data: []byte("raftmember-runtime-bundle-snapshot-bootstrap"),
			Metadata: &pb.SnapshotMetadata{
				Index: &index, Term: &term,
				ConfState: &pb.ConfState{Voters: []uint64{walIdentity.MemberID}},
			},
		},
	}, walOptions)
	if err != nil {
		t.Fatalf("create runtime bundle WAL: %v", err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	_, database, _ := prepareSQLRoot(t, walIdentity, "runtime-bundle-snapshot")
	session, err := database.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := session.Prepare(
		t.Context(), `CREATE TABLE email_claims (PRIMARY KEY (key))`,
	)
	if err == nil {
		_, err = prepared.Exec(t.Context(), nil)
	}
	if prepared != nil {
		err = errors.Join(err, prepared.Close())
	}
	err = errors.Join(err, session.Close())
	if err != nil {
		t.Fatal(err)
	}
	authority := testAuthorityProfile()
	binding, err := BindingFromWAL(wal, authority)
	if err != nil {
		t.Fatal(err)
	}
	base, err := database.BindReplicatedShardStoreBundle(
		binding, "docs", []sqldriver.ReplicatedGlobalIndexRelation{{
			Relation: 2, Table: "email_claims", IndexID: 41,
			Incarnation: 7, LocatorCount: 1, Unique: true,
			KeyEncoding: sqldriver.ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			BucketBits:    distribution.DefaultVirtualBucketBits,
		}},
	)
	skipIfStrictAllocationUnsupported(t, "bind runtime bundle SQL", err)
	if err != nil {
		t.Fatal(err)
	}
	apply, _, err := OpenPreparedApply(wal, database, authority, base, testApplyOptions())
	skipIfStrictAllocationUnsupported(t, "open runtime bundle apply", err)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	runtime, err := AdoptRuntime(wal, database, apply)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	drainRuntime(t, runtime, nil)
	if err = runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, runtime, nil)
	epoch := openRuntimeTestSession(t, runtime, apply, base)
	baseKey, ok := orderedkey.AppendJSONString(nil, []byte(`"bundle-doc"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode bundle base key")
	}
	document := []byte(`{"id":"bundle-doc","email":"a"}`)
	globalKey, err := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{distribution.NewString("a")})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := base.Relations[1].GlobalIndexStorageKeyPoint(globalKey); !ok {
		t.Fatal("canonical global-index key rejected by retained relation")
	}
	locator := []byte(`["bundle-doc"]`)
	commandValue := testApplyCommandValue(base, epoch, 2, baseKey, document)
	commandValue.Batches = append(commandValue.Batches, replication.RelationMutationBatch{
		Relation: 2,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: locator,
		}},
	})
	commandValue.Fingerprint[0] ^= 0x6b
	command, err := replication.AppendCommand(nil, commandValue)
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.Propose(command); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, runtime, nil)

	state, err := runtime.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	publication, err := runtime.Publication()
	if err != nil || state.Applied != publication.Applied ||
		state.DataChainDigest != publication.DataChainDigest ||
		state.ReplicaSetVersion != publication.ReplicaSetVersion {
		t.Fatalf("runtime snapshot state=%+v publication=%+v err=%v", state, publication, err)
	}
	cut, err := apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	manifest, writeErr := replicatedstate.WriteSnapshotArtifact(
		&artifact, cut, replicatedstate.SnapshotArtifactOptions{},
	)
	closeErr := cut.Close()
	if writeErr != nil || closeErr != nil || !manifest.Bundle ||
		manifest.State.Applied != state.Applied || len(manifest.Relations) != 2 ||
		manifest.Relations[0].Relation != 1 || manifest.Relations[0].Rows != 1 ||
		!bytes.Equal(manifest.Relations[0].Collection, []byte("docs")) ||
		manifest.Relations[1].Relation != 2 || manifest.Relations[1].Rows != 1 ||
		!bytes.Equal(manifest.Relations[1].Collection, []byte("email_claims")) ||
		manifest.UserRows != 2 || manifest.ImageDigest == ([32]byte{}) ||
		manifest.RelationManifestDigest != runtime.Identity().RelationManifestDigest {
		t.Fatalf("runtime bundle manifest=%+v write=%v close=%v", manifest, writeErr, closeErr)
	}
}
