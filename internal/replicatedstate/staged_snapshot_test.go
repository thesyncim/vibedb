package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestInitializeStagedSnapshotBindsRowsWithoutCopying(t *testing.T) {
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	user := createTargetAt(t, dir, "user", durable.Options{})
	if _, err := user.Collection.Put([]byte("a"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := user.Collection.Put([]byte("b"), []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	txnLog, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = txnLog.Close() })
	binding := testBinding()
	bootstrap := testBootstrap()
	cut := StagedSnapshotCut{
		Applied: 7, Term: 3,
		EntryDigest: sha256.Sum256([]byte("certified-staged-child")),
	}
	options := machineOptionsFor(user)
	if _, _, _, err := InitializeStagedSnapshot(
		binding, bootstrap, system, UserCollection{Name: "docs", Target: user},
		txnLog, options, cut, SnapshotArtifactOptions{TargetChunkBytes: 1},
	); !errors.Is(err, ErrStagedSnapshot) || system.Collection.Len() != 0 {
		t.Fatalf("invalid artifact options err=%v systemRows=%d", err, system.Collection.Len())
	}
	machine, base, manifest, err := InitializeStagedSnapshot(
		binding, bootstrap, system, UserCollection{Name: "docs", Target: user},
		txnLog, options, cut, SnapshotArtifactOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	publication := machine.Published()
	if publication.Applied != cut.Applied || publication.ReplicaSetVersion != 1 ||
		manifest.State.LastKind != RecordImportedSnapshot || manifest.UserRows != 2 ||
		base.GetMetadata().GetIndex() != cut.Applied {
		t.Fatalf("publication=%+v state=%+v rows=%d", publication, manifest.State, manifest.UserRows)
	}
	certificate, err := OpenSnapshotBase(base)
	if err != nil || certificate.Manifest.Digest != manifest.Digest {
		t.Fatalf("certificate=%+v err=%v", certificate, err)
	}

	// A crash after the hidden state row but before the Raft base install can
	// deterministically reconstruct the same candidate and certificate.
	reopened, retryBase, retryManifest, err := InitializeStagedSnapshot(
		binding, bootstrap, system, UserCollection{Name: "docs", Target: user},
		txnLog, options, cut, SnapshotArtifactOptions{},
	)
	if err != nil || reopened.Published().LogicalDigest != publication.LogicalDigest ||
		retryManifest.Digest != manifest.Digest || !proto.Equal(base, retryBase) {
		t.Fatalf("retry publication=%+v manifest=%+v baseEqual=%v err=%v",
			reopened.Published(), retryManifest, proto.Equal(base, retryBase), err)
	}
	beforeA, found, err := user.Collection.AppendRaw(nil, []byte("a"))
	if err != nil || !found {
		t.Fatalf("row before install found=%v err=%v", found, err)
	}
	installed, err := machine.InstallSnapshot(base)
	if err != nil || installed.Applied != cut.Applied ||
		installed.LogicalDigest != publication.LogicalDigest {
		t.Fatalf("install=%+v err=%v", installed, err)
	}
	afterA, found, err := user.Collection.AppendRaw(nil, []byte("a"))
	if err != nil || !found || !bytes.Equal(afterA, beforeA) {
		t.Fatalf("row after install=%q found=%v err=%v", afterA, found, err)
	}
	command := testCommand(binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("c"), Value: []byte(`{"n":3}`),
	})
	if _, err := machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: 8, Term: 3, Type: pb.EntryNormal,
	}, command); err != nil {
		t.Fatal(err)
	}
}
