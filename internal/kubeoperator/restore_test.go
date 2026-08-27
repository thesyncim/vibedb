package kubeoperator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

func TestRestoreStorageAllocationUsesFullCanonicalIdentity(t *testing.T) {
	operation := clusterrestore.Operation{Digest: sha256.Sum256([]byte("restore-operation"))}
	storage := restoreStorageIdentity(operation.Digest, 7, 2)
	decoded, err := hex.DecodeString(storage)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != storage {
		t.Fatalf("storage is not a full canonical 256-bit identity: %q %v", storage, err)
	}
	if restoreStorageIdentity(operation.Digest, 7, 2) != storage ||
		restoreStorageIdentity(operation.Digest, 6, 2) == storage ||
		restoreStorageIdentity(operation.Digest, 7, 1) == storage ||
		restoreStorageIdentity(sha256.Sum256([]byte("other-operation")), 7, 2) == storage {
		t.Fatal("storage identity is not deterministic and operation/group/replica scoped")
	}
	path := filepath.Join(t.TempDir(), "allocation.vibejson")
	allocation := restoreReplicaAllocation{Format: 1, Operation: hex.EncodeToString(operation.Digest[:]),
		GroupOrdinal: 7, ReplicaOrdinal: 2, LogID: strings.Repeat("ab", 16), UserStorage: storage}
	if err := writeRestoreVibe(path, &allocation); err != nil {
		t.Fatal(err)
	}
	if _, found, err := readRestoreAllocation(path, operation, 7, 2); err != nil || !found {
		t.Fatalf("exact allocation rejected: found=%v err=%v", found, err)
	}
	for _, invalid := range []string{storage[:32], strings.ToUpper(storage), strings.Repeat("ab", 32)} {
		allocation.UserStorage = invalid
		if err := writeRestoreVibe(path, &allocation); err != nil {
			t.Fatal(err)
		}
		if _, found, err := readRestoreAllocation(path, operation, 7, 2); err == nil || found {
			t.Fatalf("invalid allocation accepted: %q", invalid)
		}
	}
}

func TestRestoreReceiptMatchesCurrentApplyFormat(t *testing.T) {
	allocation := restoreReplicaAllocation{Format: 1, Operation: strings.Repeat("ab", sha256.Size),
		GroupOrdinal: 3, ReplicaOrdinal: 2}
	receipt := restoreReplicaReceipt{Format: 1, Operation: allocation.Operation,
		GroupOrdinal: allocation.GroupOrdinal, Replica: allocation.ReplicaOrdinal,
		RootDigest: strings.Repeat("cd", sha256.Size),
		Apply:      sqldriver.ReplicatedApplyIdentity{Format: sqldriver.ReplicatedApplyFormat, Storage: "apply-storage"}}
	// Format zero is the current unreleased apply grammar, not a missing receipt.
	if !restoreReceiptMatchesAllocation(receipt, allocation) {
		t.Fatal("receipt with the current replicated apply format rejected")
	}
	for name, mutate := range map[string]func(*restoreReplicaReceipt){
		"unsupported apply format": func(r *restoreReplicaReceipt) { r.Apply.Format++ },
		"missing apply storage":    func(r *restoreReplicaReceipt) { r.Apply.Storage = "" },
		"unsupported receipt":      func(r *restoreReplicaReceipt) { r.Format++ },
		"different operation":      func(r *restoreReplicaReceipt) { r.Operation = strings.Repeat("ef", sha256.Size) },
		"different group":          func(r *restoreReplicaReceipt) { r.GroupOrdinal++ },
		"different replica":        func(r *restoreReplicaReceipt) { r.Replica++ },
		"truncated root digest":    func(r *restoreReplicaReceipt) { r.RootDigest = r.RootDigest[:32] },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := receipt
			mutate(&invalid)
			if restoreReceiptMatchesAllocation(invalid, allocation) {
				t.Fatal("invalid receipt accepted")
			}
		})
	}
}

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

func TestRestoreSchemaRequiresExactOrderedBundleManifest(t *testing.T) {
	template := validRestoreTestTemplate()
	template.DDL = append(template.DDL,
		"CREATE INDEX by_email ON items (email)",
		"CREATE TABLE email_claims (PRIMARY KEY (key))",
	)
	template.GlobalIndexes = []restoreGlobalIndexTemplate{{
		Relation: 2, Table: "email_claims", IndexID: 41, Incarnation: 7,
		LocatorCount: 1, Unique: true, KeyEncoding: uint8(sqldriver.ReplicatedRelationKeyCanonicalTuple),
		KeyArity: 1, TupleVersion: uint32(distribution.CurrentTupleVersion),
		MapperVersion: uint32(distribution.NativeMapperVersion), BucketBits: distribution.DefaultVirtualBucketBits,
	}}
	factory := restoreReplicaFactory{template: template}
	manifest := replicatedstate.SnapshotArtifactManifest{
		Bundle: true, UserCollection: []byte("items"), RelationManifestDigest: [32]byte{1},
		Relations: []replicatedstate.SnapshotArtifactRelation{
			{Relation: replication.RelationID(1), Kind: replicatedstate.RelationJSON, Collection: []byte("items")},
			{Relation: replication.RelationID(2), Kind: replicatedstate.RelationGlobalIndex, Collection: []byte("email_claims")},
		},
	}
	if !validRestoreTemplate(template) || !factory.manifestMatchesTemplate(manifest) {
		t.Fatal("exact bundle schema rejected")
	}
	manifest.Relations[1].Collection = []byte("other")
	if factory.manifestMatchesTemplate(manifest) {
		t.Fatal("reordered or renamed relation accepted")
	}
	relations := factory.globalRelations()
	if len(relations) != 1 || relations[0].Relation != 2 || relations[0].IndexID != 41 ||
		relations[0].Table != "email_claims" || relations[0].TupleVersion != distribution.CurrentTupleVersion {
		t.Fatalf("relations=%+v", relations)
	}
}

func TestRestoreSchemaSetBindsDistinctDenseGroupSchemas(t *testing.T) {
	first, second := validRestoreTestTemplate(), validRestoreTestTemplate()
	second.BaseTable, second.Shard = "catalog", "controlplane"
	second.DDL = []string{"CREATE TABLE catalog (PRIMARY KEY (id))"}
	set := restoreSchemaSet{Format: 1, Groups: []restoreSchemaSlot{{Ordinal: 0, Schema: first}, {Ordinal: 1, Schema: second}}}
	raw, err := vibejson.Marshal(&set)
	if err != nil {
		t.Fatal(err)
	}
	operation := clusterrestore.Operation{Targets: make([]clusterrestore.TargetGroup, 2), TargetCatalogDigest: sha256.Sum256(raw)}
	selected, err := openRestoreSchemaSet(raw, operation, 1)
	if err != nil || selected.BaseTable != "catalog" {
		t.Fatalf("selected=%+v err=%v", selected, err)
	}
	set.Groups[1].Ordinal = 0
	raw, _ = vibejson.Marshal(&set)
	operation.TargetCatalogDigest = sha256.Sum256(raw)
	if err := ValidateRestoreSchemaSet(raw, operation, 1); err == nil {
		t.Fatal("duplicate ordinal accepted")
	}
	set.Groups = set.Groups[:1]
	raw, _ = vibejson.Marshal(&set)
	operation.TargetCatalogDigest = sha256.Sum256(raw)
	if err := ValidateRestoreSchemaSet(raw, operation, 0); err == nil {
		t.Fatal("missing schema accepted")
	}
}

func validRestoreTestTemplate() restoreSchemaTemplate {
	return restoreSchemaTemplate{
		Format: 1, Distribution: "data", Shard: "data-0", AllocationGeneration: 1,
		BaseTable: "items", DDL: []string{"CREATE TABLE items (PRIMARY KEY (id))"},
		Apply: bootstrapApply{MaxSessions: 1, RetryWindow: 1, MaxCollections: 1,
			MaxDocuments: 1, MaxBytes: 1024, ShardKey: "id"},
	}
}
