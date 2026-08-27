package kubeoperator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibejson"
)

func TestRestoreGroupRejectsUnboundTemplateBeforeCreatingState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "restore")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	template := validRestoreTestTemplate()
	raw, err := vibejson.Marshal(&template)
	if err != nil {
		t.Fatal(err)
	}
	operation := clusterrestore.Operation{TargetCatalogDigest: sha256.Sum256([]byte("different"))}
	_, err = RestoreGroup(context.Background(), RestoreGroupConfig{
		Root: root, Template: raw, Operation: operation, Artifact: bytes.NewReader(nil),
	})
	if !errors.Is(err, ErrBootstrap) {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "staging")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unbound template created restore state: %v", err)
	}
}

func TestRestoreReplicaFactoryFailsClosedForBundle(t *testing.T) {
	template := validRestoreTestTemplate()
	digest := sha256.Sum256([]byte("template"))
	factory := &restoreReplicaFactory{root: t.TempDir(), template: template, templateDigest: digest}
	operation := clusterrestore.Operation{
		TargetCatalogDigest: digest,
		Targets:             []clusterrestore.TargetGroup{{}},
		Certificate:         clusterbackup.Certificate{Groups: []clusterbackup.GroupCut{{}}},
	}
	_, err := factory.OpenReplica(context.Background(), operation, 0, 0,
		replicatedstate.SnapshotArtifactManifest{Bundle: true})
	if !errors.Is(err, ErrBootstrap) {
		t.Fatalf("err=%v", err)
	}
	if entries, err := os.ReadDir(factory.root); err != nil || len(entries) != 0 {
		t.Fatalf("bundle rejection mutated root: entries=%d err=%v", len(entries), err)
	}
}

func TestRestoreRF3BootstrapIsDeterministicAndFresh(t *testing.T) {
	operation := clusterrestore.Operation{Digest: sha256.Sum256([]byte("operation"))}
	template := sha256.Sum256([]byte("template"))
	first := restoreRF3Bootstrap(operation, 4, template)
	second := restoreRF3Bootstrap(operation, 4, template)
	if !bytes.Equal(first.Data, second.Data) || first.GetMetadata().GetIndex() != 1 ||
		first.GetMetadata().GetTerm() != 1 || len(first.GetMetadata().GetConfState().Voters) != 3 ||
		first.GetMetadata().GetConfState().Voters[0] != 1 || first.GetMetadata().GetConfState().Voters[1] != 2 ||
		first.GetMetadata().GetConfState().Voters[2] != 3 {
		t.Fatalf("bootstrap=%+v", first)
	}
}

func validRestoreTestTemplate() bootstrapPrepare {
	return bootstrapPrepare{
		Distribution: "data", Shard: "data-0", AllocationGeneration: 1,
		Table: "items", CreateTable: "CREATE TABLE items (id TEXT PRIMARY KEY)",
		Apply: bootstrapApply{MaxSessions: 1, RetryWindow: 1, MaxCollections: 1,
			MaxDocuments: 1, MaxBytes: 1024, ShardKey: "id"},
	}
}
