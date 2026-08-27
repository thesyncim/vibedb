package driver

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func TestReplicatedChildReservationCatalogCodec(t *testing.T) {
	binding := testReplicatedBinding(181)
	meta := &tableMeta{PrimaryKey: "/id", Storage: strings.Repeat("1", storageIdentityBytes*2),
		Materialized: true, SealedRecoveryJournalBytes: ReplicatedUserRecoveryJournalBytes}
	options, err := durable.NormalizeOptions(durableOptions(&table{meta: meta}))
	if err != nil {
		t.Fatal(err)
	}
	base := ReplicatedShardStoreIdentity{
		Format: ReplicatedShardStoreFormat, Binding: binding, LogID: [16]byte{0xa1},
		UserTable: "docs", UserStorage: meta.Storage, UserPrimaryKey: meta.PrimaryKey,
		UserLimits: ReplicatedShardStoreLimits{MaxKeyBytes: options.MaxKeyBytes, MaxDocumentBytes: options.MaxDocumentBytes,
			MaxBatchDocuments: options.MaxBatchDocuments, MaxBatchBytes: options.MaxBatchBytes},
		Sidecars: canonicalReplicatedShardStoreSidecars(), RelationCount: 1,
		RelationSchemaGeneration: binding.Authority.SchemaGeneration,
	}
	base.Relations = []ReplicatedShardRelationIdentity{{Relation: 1, Kind: ReplicatedShardRelationJSON,
		Table: base.UserTable, Storage: base.UserStorage, Limits: base.UserLimits,
		LocalIndexDigest: replicatedLocalIndexDigest(nil)}}
	base.RelationManifestDigest = replicatedRelationManifestDigest(base)
	reserved, err := NewReplicatedChildApplyIdentity(base, strings.Repeat("a", storageIdentityBytes*2),
		strings.Repeat("b", storageIdentityBytes*2), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	reservedMeta := replicatedApplyMetaFromIdentity(reserved)
	catalog := catalogFile{Version: catalogVersion, Tables: map[string]*tableMeta{"docs": meta},
		ShardStore: &ShardStoreIdentity{Distribution: distribution.DistributionName(binding.Distribution),
			Shard: distribution.ShardID(binding.Shard), AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration), LogID: base.LogID},
		ReplicatedShardStore: &base, ReplicatedChildApply: &reservedMeta}
	raw, err := appendCatalogJSON(nil, catalog)
	if err != nil || !bytes.Contains(raw, []byte(`"replicated_child_apply":`)) {
		t.Fatalf("reservation omitted from catalog: %s, %v", raw, err)
	}
	var decoded catalogFileVibe
	if err := decodeCatalogJSON(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ReplicatedChildApply == nil || decoded.ReplicatedChildApply.identity() != reserved || decoded.ReplicatedApply != nil {
		t.Fatalf("reservation changed on roundtrip: %+v", decoded.ReplicatedChildApply)
	}
	again, err := appendCatalogJSON(nil, catalogFile(decoded))
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("noncanonical reservation roundtrip: %v", err)
	}
	retained := decoded.ReplicatedChildApply
	encodedMeta, err := vibejson.Marshal(&reservedMeta)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"replicated_child_apply", `replicated_child_\u0061pply`, "replicated_child_applz", "replicated_apply"} {
		bad := append(bytes.Clone(raw[:len(raw)-1]), []byte(`,"`+field+`":`)...)
		bad = append(bad, encodedMeta...)
		bad = append(bad, '}')
		if err := decodeCatalogJSON(bad, &decoded); err == nil {
			t.Fatalf("accepted duplicate/unknown/incompatible field %s", field)
		}
	}
	bad := bytes.Replace(raw, encodedMeta, []byte("null"), 1)
	if err := decodeCatalogJSON(bad, &decoded); err == nil {
		t.Fatal("accepted null reservation")
	}
	foreign := reservedMeta
	foreign.ValidationDigest[0] ^= 1
	foreignBytes, err := vibejson.Marshal(&foreign)
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeCatalogJSON(bytes.Replace(raw, encodedMeta, foreignBytes, 1), &decoded); err == nil {
		t.Fatal("accepted reservation with foreign validation digest")
	}
	// Decoded reservations must not retain the caller's catalog byte storage.
	clear(raw)
	if retained.identity() != reserved {
		t.Fatal("reservation aliases caller bytes")
	}
}

func TestReplicatedChildApplyReservationPersistsExactIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prepared-child.vdb")
	binding := testReplicatedBinding(181)
	binding.Distribution, binding.Shard = "orders", "right"
	binding.AllocationGeneration = 9
	database, err := InitializeShardStoreIdentity(path, ShardStoreIdentity{
		Distribution: "orders", Shard: "right", AllocationGeneration: 9,
		LogID: [16]byte{0xa1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	localIdentity := ShardStoreIdentity{
		Distribution: "orders", Shard: "right", AllocationGeneration: 9, LogID: [16]byte{0xa1},
	}
	database, err = InitializeShardStoreIdentity(path, localIdentity)
	if err != nil {
		t.Fatalf("exact identity retry: %v", err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	wrongIdentity := localIdentity
	wrongIdentity.LogID[0]++
	if wrong, openErr := InitializeShardStoreIdentity(path, wrongIdentity); wrong != nil || !errors.Is(openErr, ErrShardStoreIdentityMismatch) {
		if wrong != nil {
			_ = wrong.Close()
		}
		t.Fatalf("substituted log identity err=%v", openErr)
	}
	database, err = InitializeShardStoreIdentity(path, localIdentity)
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = testRuntimeExec(session, `CREATE TABLE docs (PRIMARY KEY (id))`, nil); err != nil {
		t.Fatal(err)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	const userStorage = "1111111111111111111111111111111111111111111111111111111111111111"
	base, err := database.BindReplicatedShardStoreStorageIdentity(binding, "docs", userStorage)
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		t.Skipf("sealed replicated sidecars require strict allocation support: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if base.UserStorage != userStorage {
		t.Fatalf("user storage = %q", base.UserStorage)
	}
	if _, err = database.BindReplicatedShardStoreStorageIdentity(
		binding, "docs", "2222222222222222222222222222222222222222222222222222222222222222",
	); !errors.Is(err, ErrReplicatedShardStoreIdentityMismatch) {
		t.Fatalf("substituted user storage err=%v", err)
	}
	options := testReplicatedApplyOptions()
	options.Placement.Range = distribution.KeyRange{Start: distribution.KeyspacePoint{0x80}, End: distribution.KeyspaceEnd{Max: true}}
	reserved, err := NewReplicatedChildApplyIdentity(
		base, strings.Repeat("a", storageIdentityBytes*2),
		strings.Repeat("b", storageIdentityBytes*2), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = database.ReserveReplicatedChildApply(base, reserved); err != nil {
		t.Fatal(err)
	}
	if err = database.ReserveReplicatedChildApply(base, reserved); err != nil {
		t.Fatalf("exact reservation retry: %v", err)
	}
	forged := reserved
	forged.Storage = strings.Repeat("c", storageIdentityBytes*2)
	if err = database.ReserveReplicatedChildApply(base, forged); !errors.Is(err, ErrReplicatedApplyMismatch) {
		t.Fatalf("substituted reservation err=%v", err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = OpenReplicatedShardStore(path, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	recovered, present, err := database.ReplicatedChildApplyReservation(base)
	if err != nil || !present || recovered != reserved {
		t.Fatalf("recovered=%+v present=%v err=%v", recovered, present, err)
	}
}
