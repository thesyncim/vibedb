package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

// Unlike the sealed SQL profile test, this exercises real checkpoint recovery
// on hosts without strict physical-allocation support. It uses the same
// membership certificates and namespace mover, not an in-memory substitute.
func TestSchemaNamespacePromotesRealFinalizedCheckpointMembership(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, replicatedSchemaTargetsDirectory)
	if err := os.Mkdir(targetDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sourceID, targetID := [32]byte{0x41}, [32]byte{0x51}
	sourceName, targetName := hex.EncodeToString(sourceID[:])+".vjc", hex.EncodeToString(targetID[:])+".vjc"
	var files []*os.File
	create := func(directory, path, name string) durable.NamedCollection {
		t.Helper()
		file, err := os.OpenFile(filepath.Join(directory, path), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
		collection, err := durable.Create(file, durable.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close(); _ = file.Close() })
		return durable.NamedCollection{Name: name, Collection: collection}
	}
	source := []durable.NamedCollection{create(dir, "system.vjc", "system"), create(dir, sourceName, "user")}
	fresh := create(targetDir, targetName, "user")
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	group, err := durable.NewCheckpointGroup(log, source, durable.CheckpointGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	put := func(applied uint64, members []durable.NamedCollection, key string) {
		t.Helper()
		if err := group.Update(applied, members, durable.TxnLimits{MaxCollections: 2, MaxDocuments: 4, MaxBytes: 1 << 20}, func(batch *durable.DatabaseBatch) error {
			for _, member := range members {
				write, err := batch.Collection(member.Name)
				if err != nil {
					return err
				}
				if err := write.Put([]byte(key), []byte(`{"value":1}`)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	put(1, source, "before")
	authorization, command := sha256.Sum256([]byte("prepared schema")), sha256.Sum256([]byte("committed schema"))
	witness, err := group.PrepareMembershipTransition([]durable.NamedCollection{source[0], fresh}, authorization)
	if err != nil {
		t.Fatal(err)
	}
	put(2, source[:1], "schema")
	if err := group.FinalizeMembershipTransition(witness, authorization, 2, command); err != nil {
		t.Fatal(err)
	}
	if err := durable.ValidateFinalizedCheckpointMembershipTransition(dir, witness, authorization, 2, command); err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	for _, member := range append(source, fresh) {
		if err := member.Collection.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	marker := replicatedSchemaStageMarker{storages: [][32]byte{targetID}, sourceStorages: [][32]byte{sourceID}}
	target := ReplicatedShardStoreIdentity{Relations: []ReplicatedShardRelationIdentity{{Storage: hex.EncodeToString(targetID[:])}}}
	if err := activateReplicatedSchemaNamespace(dir, marker, target); err != nil {
		t.Fatal(err)
	}
	if err := durable.ValidateSelectedCheckpointMembershipTransition(dir, witness, authorization, 2, command); err == nil {
		t.Fatal("namespace move alone authorized source drain")
	}
	requests := make([]durable.TransactionCollectionOpen, 2)
	for i, name := range []string{"system.vjc", targetName} {
		file, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		requests[i] = durable.TransactionCollectionOpen{File: file, Options: durable.Options{}}
	}
	collections, recoveredLog, recovered, err := durable.OpenCollectionsWithCheckpointMembershipTransition(
		dir, durable.TxnLogOptions{}, requests, []string{"system", "user"}, witness, authorization, durable.CheckpointGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = recovered.Close()
		for _, collection := range collections {
			_ = collection.Close()
		}
		_ = recoveredLog.Close()
	}()
	if recovered.AppliedIndex() != 2 || collections[0].Len() != 2 || collections[1].Len() != 0 {
		t.Fatalf("selected wrong cut: applied=%d system=%d target=%d", recovered.AppliedIndex(), collections[0].Len(), collections[1].Len())
	}
	if err := durable.ValidateSelectedCheckpointMembershipTransition(dir, witness, authorization, 2, command); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, replicatedSchemaSourcesDirectory, sourceName)); err != nil {
		t.Fatal("source prematurely reclaimed", err)
	}
}
