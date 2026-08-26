package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func testReplicatedApplyOptions() ReplicatedApplyOptions {
	const captureQualifiedTxnBytes = 384 << 20
	limits := defaultDriverTxnLimits()
	limits.MaxBytes = captureQualifiedTxnBytes
	return ReplicatedApplyOptions{
		MaxSessions: 128,
		RetryWindow: 8,
		TxnLimits:   limits,
		Placement: ReplicatedPlacementProfile{
			Format: ReplicatedPlacementProfileFormat, ShardKey: "/id",
			TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
			Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	}
}

func testReplicatedApplyBootstrap() *pb.Snapshot {
	index, term := uint64(1), uint64(1)
	return &pb.Snapshot{
		Data: []byte("sql-replicated-apply-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term,
			ConfState: &pb.ConfState{Voters: []uint64{1}},
		},
	}
}

func bindReplicatedApplyTestRoot(
	t *testing.T,
	name string,
) (string, *Database, ReplicatedShardStoreIdentity) {
	t.Helper()
	path, database, binding, _ := prepareReplicatedTestRoot(t, name, false)
	identity := requireReplicatedShardStoreBind(t, database, binding, "docs")
	return path, database, identity
}

// corruptReplicatedApplyCollectionForTest injects a deliberately invalid
// logical image through the owning checkpoint group's same-applied transition.
// Keeping this seam in test code preserves the production direct-write fence.
func corruptReplicatedApplyCollectionForTest(
	t *testing.T,
	database *Database,
	identity ReplicatedShardStoreIdentity,
	limits durable.TxnLimits,
	name string,
	mutate func(*durable.WriteBatch) error,
) {
	t.Helper()
	core := database.connector.db
	core.mu.Lock()
	defer core.mu.Unlock()
	table := core.tables[identity.UserTable]
	if core.checkpointGroup == nil || core.replicatedApplyCollection == nil ||
		table == nil || table.collection == nil {
		t.Fatal("replicated apply checkpoint group is unavailable")
	}
	members := []durable.NamedCollection{
		{Name: replicatedstate.SystemCollectionName, Collection: core.replicatedApplyCollection},
		{Name: identity.UserTable, Collection: table.collection},
		{Name: replicatedstate.TransitionCaptureCollectionName, Collection: core.replicatedCaptureCollection},
	}
	if !core.checkpointGroup.Owns(members) {
		t.Fatal("replicated apply checkpoint group has unexpected membership")
	}
	if err := core.checkpointGroup.Update(
		core.checkpointGroup.AppliedIndex(), members, limits,
		func(batch *durable.DatabaseBatch) error {
			collection, err := batch.Collection(name)
			if err != nil {
				return err
			}
			return mutate(collection)
		},
	); err != nil {
		t.Fatalf("inject replicated apply corruption into %q: %v", name, err)
	}
}

func testReplicatedApplyCommand(
	identity ReplicatedShardStoreIdentity,
	clientEpoch uint64,
	sequence uint64,
	mutations ...replication.Mutation,
) []byte {
	command := testReplicatedApplyCommandValue(identity, clientEpoch, sequence, mutations)
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		panic(err)
	}
	return encoded
}

func testReplicatedApplyCommandValue(
	identity ReplicatedShardStoreIdentity,
	clientEpoch, sequence uint64,
	mutations []replication.Mutation,
) replication.Command {
	fingerprint := sha256.Sum256([]byte{byte(sequence), 0x4a})
	binding := identity.Binding
	return replication.Command{
		ClusterID:             replication.ID128(binding.ClusterID),
		ClusterIncarnation:    replication.ID128(binding.ClusterIncarnation),
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration:   binding.AllocationGeneration,
		ShardIncarnation:       replication.ID128(binding.ShardIncarnation),
		GroupID:                replication.ID128(binding.GroupID),
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        binding.Authority.ProtectionEpoch,
		OwnershipEpoch:         binding.Authority.OwnershipEpoch,
		SchemaGeneration:       binding.Authority.SchemaGeneration,
		RoutingVersion:         binding.Authority.RoutingVersion,
		RouteGeneration:        binding.Authority.RouteGeneration,
		Tenant:                 []byte("tenant"), ClientID: replication.ID128{9},
		ClientEpoch: clientEpoch, ClientSequence: sequence, Fingerprint: fingerprint,
		Batches: []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}},
	}
}

func testReplicatedApplySessionOpen(identity ReplicatedShardStoreIdentity) []byte {
	command := testReplicatedApplyCommandValue(identity, 0, 1, nil)
	command.Kind = replication.CommandSessionOpen
	command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	command.Fingerprint = sha256.Sum256([]byte("driver/test-session-open"))
	command.Batches = nil
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		panic(err)
	}
	return encoded
}

func applyReplicatedApplySessionOpen(
	t *testing.T,
	claim *ReplicatedApply,
	identity ReplicatedShardStoreIdentity,
	index uint64,
) uint64 {
	t.Helper()
	command := testReplicatedApplySessionOpen(identity)
	if err := claim.AdmitCommand(command); err != nil {
		t.Fatalf("AdmitCommand session open at %d: %v", index, err)
	}
	publication, err := claim.ApplyNormal(testReplicatedApplyMeta(index), command)
	if err != nil || publication.Applied != index {
		t.Fatalf("ApplyNormal session open at %d = %+v, %v", index, publication, err)
	}
	lookup, err := claim.LookupCompletion(command)
	if err != nil {
		t.Fatalf("LookupCompletion session open at %d: %v", index, err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened ||
		completion.ClientEpoch != index || completion.ClientSequence != 1 ||
		completion.AppliedSequence != index {
		t.Fatalf("session open completion at %d = %+v, %v", index, completion, err)
	}
	return completion.ClientEpoch
}

func testReplicatedApplyMeta(index uint64) raftmodel.ApplyMeta {
	return raftmodel.ApplyMeta{Index: index, Term: 2, Type: pb.EntryNormal}
}

func testReplicatedApplyKey(t *testing.T, database *Database, document []byte) []byte {
	t.Helper()
	core := database.connector.db
	core.mu.RLock()
	table := core.tables["docs"]
	key, err := documentKey(document, table.meta.PrimaryKey, table.primary, table.collection.MaxKeyBytes())
	core.mu.RUnlock()
	if err != nil {
		t.Fatalf("documentKey(%s): %v", document, err)
	}
	return []byte(key)
}

func TestReplicatedApplyUsesReplayBackedCheckpointGroup(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "checkpoint-group")
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(
		base, bootstrap, testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	if claim.Applied() != 1 || claim.CheckpointAppliedIndex() != 0 {
		t.Fatalf("bootstrap cuts = applied %d checkpoint %d",
			claim.Applied(), claim.CheckpointAppliedIndex())
	}
	core := database.connector.db
	core.mu.RLock()
	group := core.checkpointGroup
	system := core.replicatedApplyCollection
	user := core.tables[base.UserTable].collection
	capture := core.replicatedCaptureCollection
	core.mu.RUnlock()
	if group == nil {
		t.Fatal("replicated apply did not attach a checkpoint group")
	}
	members := []durable.NamedCollection{
		{Name: replicatedstate.SystemCollectionName, Collection: system},
		{Name: base.UserTable, Collection: user},
		{Name: replicatedstate.TransitionCaptureCollectionName, Collection: capture},
	}
	if !group.Owns(members) {
		t.Fatal("replicated apply checkpoint group has unexpected membership")
	}
	stats := group.Stats()
	if stats.Updates != 1 || stats.BarrierSyncs != 0 ||
		stats.JournalSyncs != 0 || stats.MarkerSyncs != 0 ||
		stats.CertificateSyncs != 0 {
		t.Fatalf("bootstrap apply sync stats = %+v", stats)
	}
	if _, err := user.Put([]byte("direct"), []byte(`{"id":"direct"}`)); !errors.Is(err, durable.ErrCheckpointGroupOwned) {
		t.Fatalf("direct replicated user Put = %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if claim.CheckpointAppliedIndex() != 1 {
		t.Fatalf("checkpoint cut = %d", claim.CheckpointAppliedIndex())
	}
	stats = group.Stats()
	if stats.BarrierSyncs != uint64(len(members)+1) ||
		stats.JournalSyncs != uint64(len(members)) ||
		stats.PhysicalCheckpoints != uint64(len(members)) ||
		stats.CertificateSyncs != 1 || stats.MarkerSyncs != 0 {
		t.Fatalf("checkpoint sync stats = %+v", stats)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	reacquired, _, err := database.OpenReplicatedApply(
		base, bootstrap, testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatalf("reacquire checkpoint-owned apply: %v", err)
	}
	if reacquired.Applied() != 1 || reacquired.CheckpointAppliedIndex() != 1 {
		t.Fatalf("reacquired cuts = applied %d checkpoint %d",
			reacquired.Applied(), reacquired.CheckpointAppliedIndex())
	}
	if err := reacquired.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyAuthenticatesAndMaintainsNativeExactIndexes(t *testing.T) {
	path, database, binding, _ := prepareReplicatedTestRoot(t, "native-exact-index", false)
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := testRuntimeExec(session, `CREATE INDEX by_email ON docs (email)`, nil); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	base := requireReplicatedShardStoreBind(t, database, binding, "docs")
	bootstrap := testReplicatedApplyBootstrap()
	options := testReplicatedApplyOptions()
	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)

	document := []byte(`{"id":"indexed","email":"a"}`)
	key := testReplicatedApplyKey(t, database, document)
	command := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil {
		t.Fatal(err)
	}
	core := database.connector.db
	core.mu.RLock()
	group := core.checkpointGroup
	core.mu.RUnlock()
	if group == nil {
		t.Fatal("native exact index apply has no checkpoint group")
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	probe := func(raw []byte) [][]byte {
		t.Helper()
		core.mu.RLock()
		collection := core.tables[base.UserTable].collection
		core.mu.RUnlock()
		snapshot, err := collection.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		var entries [1]vibejson.IndexEntry
		needle, err := vibejson.BuildIndex(raw, entries[:])
		if err != nil {
			t.Fatal(err)
		}
		masks, err := snapshot.AppendIndexMasks(nil, "by_email", needle)
		if err != nil {
			t.Fatal(err)
		}
		var keys [][]byte
		if err := snapshot.RangeMasksRaw(masks, func(indexedKey, _ []byte) error {
			keys = append(keys, bytes.Clone(indexedKey))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return keys
	}
	if keys := probe([]byte(`"a"`)); len(keys) != 1 || !bytes.Equal(keys[0], key) {
		t.Fatalf("native exact index keys = %q, want %q", keys, key)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	reopenedClaim, reopenedIdentity, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if err != nil || reopenedIdentity != identity || reopenedClaim.Applied() != 3 {
		t.Fatalf("indexed replicated reopen = %+v, %v", reopenedIdentity, err)
	}
	if err := reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyServesAuthenticatedBaseAndGlobalRelationBundle(t *testing.T) {
	path, database, binding, _ := prepareReplicatedTestRoot(t, "global-relation-bundle", false)
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE INDEX by_email ON docs (email)`,
		`CREATE TABLE email_claims (PRIMARY KEY (key))`,
	} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	base, err := database.BindReplicatedShardStoreBundle(
		binding, "docs", []ReplicatedGlobalIndexRelation{{
			Relation: 2, Table: "email_claims", IndexID: 41,
			Incarnation: 7, LocatorCount: 1, Unique: true,
		}},
	)
	skipReplicatedStrictAllocationUnsupported(t, database, base, err)
	if err != nil {
		t.Fatal(err)
	}
	if base.RelationCount != 2 ||
		base.RelationSchemaGeneration != binding.Authority.SchemaGeneration ||
		base.RelationManifestDigest == ([sha256.Size]byte{}) ||
		base.Relations[0].Kind != ReplicatedShardRelationJSON ||
		base.Relations[0].LocalIndexDigest == ([sha256.Size]byte{}) ||
		base.Relations[1].Kind != ReplicatedShardRelationGlobalIndex ||
		base.Relations[1].IndexID != 41 || base.Relations[1].Incarnation != 7 {
		t.Fatalf("bundle identity = %+v", base)
	}
	bootstrap := testReplicatedApplyBootstrap()
	options := testReplicatedApplyOptions()
	claim, applyIdentity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	capture, err := claim.BeginRangeSplitCapture(replicatedApplyCapturePartitioner(t, base))
	if err != nil || capture.Head() != 2 {
		t.Fatalf("bundle capture head=%d err=%v", capture.Head(), err)
	}
	document := []byte(`{"email":"a","id":"doc-1"}`)
	baseKey := testReplicatedApplyKey(t, database, document)
	globalKey := []byte{0x91, 0x01, 'a'}
	locator := []byte(`["doc-1"]`)
	commandValue := testReplicatedApplyCommandValue(base, epoch, 2, nil)
	commandValue.Fingerprint = sha256.Sum256([]byte("sql-global-relation-bundle-put"))
	commandValue.Batches = []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: baseKey, Value: document,
		}}},
		{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: locator,
		}}},
	}
	command, err := replication.AppendCommand(nil, commandValue)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil {
		t.Fatal(err)
	}
	if got := completionResultCode(t, claim, command); got != replicatedstate.ResultApplied {
		t.Fatalf("bundle result = %d", got)
	}
	if capture.Head() != 3 {
		t.Fatalf("bundle capture mutation head=%d", capture.Head())
	}
	core := database.connector.db
	core.mu.RLock()
	baseCollection := core.tables["docs"].collection
	globalCollection := core.tables["email_claims"].collection
	systemCollection := core.replicatedApplyCollection
	captureCollection := core.replicatedCaptureCollection
	group := core.checkpointGroup
	core.mu.RUnlock()
	if group == nil || !group.Owns([]durable.NamedCollection{
		{Name: replicatedstate.SystemCollectionName, Collection: systemCollection},
		{Name: "docs", Collection: baseCollection},
		{Name: "email_claims", Collection: globalCollection},
		{Name: replicatedstate.TransitionCaptureCollectionName, Collection: captureCollection},
	}) {
		t.Fatal("bundle was not activated as one checkpoint group")
	}
	if value, found, err := baseCollection.AppendRaw(nil, baseKey); err != nil || !found ||
		!bytes.Equal(value, document) {
		t.Fatalf("base value = %q,%v,%v", value, found, err)
	}
	if value, found, err := globalCollection.AppendRaw(nil, globalKey); err != nil || !found ||
		!bytes.Equal(value, locator) {
		t.Fatalf("global value = %q,%v,%v", value, found, err)
	}

	conflictDocument := []byte(`{"email":"a","id":"doc-2"}`)
	conflictKey := testReplicatedApplyKey(t, database, conflictDocument)
	conflictValue := testReplicatedApplyCommandValue(base, epoch, 3, nil)
	conflictValue.Fingerprint = sha256.Sum256([]byte("sql-global-relation-bundle-conflict"))
	conflictValue.Batches = []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: conflictKey, Value: conflictDocument,
		}}},
		{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey,
			Value: []byte(`["doc-2"]`),
		}}},
	}
	conflict, err := replication.AppendCommand(nil, conflictValue)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(4), conflict); err != nil {
		t.Fatal(err)
	}
	if got := completionResultCode(t, claim, conflict); got != replicatedstate.ResultIndexConflict {
		t.Fatalf("conflict result = %d", got)
	}
	if _, found, err := baseCollection.AppendRaw(nil, conflictKey); err != nil || found {
		t.Fatalf("conflicting base row escaped atomic bundle: found=%v err=%v", found, err)
	}
	cut, err := claim.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	streamedManifest, writeErr := replicatedstate.WriteSnapshotArtifact(
		io.Discard, cut, replicatedstate.SnapshotArtifactOptions{},
	)
	closeErr := cut.Close()
	if writeErr != nil || closeErr != nil || !streamedManifest.Bundle ||
		len(streamedManifest.Relations) != 2 || streamedManifest.Relations[0].Rows != 1 ||
		streamedManifest.Relations[1].Rows != 1 || streamedManifest.CaptureRows == 0 {
		t.Fatalf("streamed bundle manifest=%+v write=%v close=%v",
			streamedManifest, writeErr, closeErr)
	}
	preparation, err := claim.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: walBaseWorkspace(),
	})
	if err != nil || preparation == nil {
		t.Fatalf("bundle WAL base = %p,%v", preparation, err)
	}
	walCertificate, err := replicatedstate.OpenSnapshotBase(preparation.snapshotBase)
	if err != nil || !walCertificate.Manifest.Bundle ||
		len(walCertificate.Manifest.Relations) != 2 ||
		walCertificate.Manifest.Digest != streamedManifest.Digest {
		t.Fatalf("bundle WAL certificate=%+v err=%v", walCertificate.Manifest, err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	snapshotBase, bundleManifest, err := claim.BuildBundleSnapshotBase()
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := replicatedstate.OpenSnapshotBase(snapshotBase)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := replicatedstate.BuildSnapshotBase(certificate.Manifest, bootstrap)
	if err != nil || !proto.Equal(reencoded, snapshotBase) {
		t.Fatalf("active-capture bundle re-encode differs: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*replicatedstate.SnapshotArtifactManifest)
	}{
		{"missing", func(m *replicatedstate.SnapshotArtifactManifest) {
			m.CaptureRows = 0
			m.CaptureImageDigest = [sha256.Size]byte{}
		}},
		{"mismatched_rows", func(m *replicatedstate.SnapshotArtifactManifest) {
			m.CaptureRows++
		}},
		{"forged_digest", func(m *replicatedstate.SnapshotArtifactManifest) {
			m.CaptureImageDigest[0] ^= 1
		}},
	} {
		t.Run("bundle_capture_proof_"+test.name, func(t *testing.T) {
			candidate := bundleManifest
			test.mutate(&candidate)
			if _, err := replicatedstate.BuildSnapshotBase(candidate, bootstrap); err == nil {
				t.Fatal("modified capture proof retained the manifest certificate")
			}
		})
	}
	capacity, err := claim.CapacityQualificationProfile()
	if err != nil {
		t.Fatal(err)
	}
	if group.CheckpointAppliedIndex() != 4 || !bundleManifest.Bundle ||
		bundleManifest.State.Applied != 4 || len(bundleManifest.Relations) != 2 ||
		bundleManifest.Relations[0].Rows != 1 || bundleManifest.Relations[1].Rows != 1 ||
		bundleManifest.CaptureRows == 0 ||
		bundleManifest.CaptureImageDigest == ([sha256.Size]byte{}) ||
		certificate.Manifest.Digest != bundleManifest.Digest ||
		certificate.Manifest.RelationManifestDigest != bundleManifest.RelationManifestDigest ||
		capacity.RelationManifestDigest != bundleManifest.RelationManifestDigest ||
		capacity.Binding.Authority.SchemaGeneration != base.RelationSchemaGeneration {
		t.Fatalf("bundle checkpoint/certificate cut = group=%d manifest=%+v cert=%+v",
			group.CheckpointAppliedIndex(), bundleManifest, certificate.Manifest)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenReplicatedShardStoreWithApply(path, base, applyIdentity)
	if err != nil {
		t.Fatal(err)
	}
	reopenedClaim, reopenedIdentity, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if err != nil || reopenedIdentity != applyIdentity || reopenedClaim.Applied() != 4 {
		t.Fatalf("bundle reopen = %+v,%v", reopenedIdentity, err)
	}
	reopenedCore := reopened.connector.db
	reopenedCore.mu.RLock()
	reopenedBase := reopenedCore.tables["docs"].collection
	reopenedGlobal := reopenedCore.tables["email_claims"].collection
	reopenedGroup := reopenedCore.checkpointGroup
	reopenedCore.mu.RUnlock()
	if value, found, readErr := reopenedBase.AppendRaw(nil, baseKey); readErr != nil ||
		!found || !bytes.Equal(value, document) {
		t.Fatalf("reopened base value = %q,%v,%v", value, found, readErr)
	}
	if value, found, readErr := reopenedGlobal.AppendRaw(nil, globalKey); readErr != nil ||
		!found || !bytes.Equal(value, locator) {
		t.Fatalf("reopened global value = %q,%v,%v", value, found, readErr)
	}
	if reopenedGroup == nil || reopenedGroup.CheckpointAppliedIndex() != 4 {
		t.Fatalf("reopened checkpoint cut = %v", reopenedGroup)
	}
	recoveredCapture, err := reopenedClaim.BeginRangeSplitCapture(
		replicatedApplyCapturePartitioner(t, base),
	)
	if err != nil || recoveredCapture.Head() != 4 {
		t.Fatalf("reopened bundle capture head=%d err=%v", recoveredCapture.Head(), err)
	}
	if err := reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if _, found, readErr := reopenedGlobal.AppendRaw(nil, []byte(globalIndexMarkerKey)); readErr != nil || found {
		t.Fatalf("replicated global relation retained standalone marker: found=%v err=%v",
			found, readErr)
	}
	reader, err := reopened.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var locators [][]byte
	collect := func(value []byte) error {
		locators = append(locators, bytes.Clone(value))
		return nil
	}
	if err := reader.LookupGlobalIndex(
		context.Background(), "email_claims", 41, 7, globalKey, 1, true,
		8, 1024, collect,
	); err != nil || len(locators) != 1 || !bytes.Equal(locators[0], locator) {
		t.Fatalf("replicated global lookup = %q,%v", locators, err)
	}
	if err := reader.LookupGlobalIndex(
		context.Background(), "email_claims", 41, 8, globalKey, 1, true,
		8, 1024, collect,
	); !errors.Is(err, ErrGlobalIndexFence) {
		t.Fatalf("replicated global stale-incarnation lookup = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyOwnershipTransitionReopensThroughWriteOnceBinding(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "ownership-transition")
	bootstrap := testReplicatedApplyBootstrap()
	options := testReplicatedApplyOptions()
	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	conf := &pb.ConfState{Voters: []uint64{1, 2}}
	if _, err := claim.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryConfChange,
	}, conf); err != nil {
		t.Fatal(err)
	}
	binding := replicatedStateBinding(base)
	transition, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: binding, ExpectedReplicaSetVersion: 2,
		SourceMember: 1, TargetMember: 2,
		ToOwnershipEpoch:  binding.OwnershipEpoch + 1,
		ToRoutingVersion:  binding.RoutingVersion + 1,
		ToRouteGeneration: binding.RouteGeneration + 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.AdmitCommand(transition); err != nil {
		t.Fatal(err)
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), transition); err != nil {
		t.Fatal(err)
	}
	cut, err := claim.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	state := cut.State()
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if state.Binding.OwnershipEpoch != binding.OwnershipEpoch+1 ||
		state.Binding.RoutingVersion != binding.RoutingVersion+1 ||
		state.Binding.RouteGeneration != binding.RouteGeneration+1 {
		t.Fatalf("transitioned SQL state = %+v", state.Binding)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	reopenedClaim, reopenedIdentity, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if err != nil || reopenedIdentity != identity {
		t.Fatalf("reopen transitioned claim = %+v, %v", reopenedIdentity, err)
	}
	cut, err = reopenedClaim.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	reopenedState := cut.State()
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if reopenedState.Binding != state.Binding || reopenedState.Applied != 3 {
		t.Fatalf("reopened transitioned state = %+v", reopenedState)
	}
	if err := reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

type replicatedPlacementProbe struct {
	id       string
	document []byte
	key      []byte
	point    distribution.KeyspacePoint
}

func testReplicatedPlacementProbes(t *testing.T) []replicatedPlacementProbe {
	t.Helper()
	mapper := distribution.NewNativeMapper(1)
	probes := make([]replicatedPlacementProbe, 64)
	for i := range probes {
		id := fmt.Sprintf("placement-%02d", i)
		point, err := mapper.PointFor([]distribution.Scalar{distribution.NewString(id)})
		if err != nil {
			t.Fatal(err)
		}
		key, ok := orderedkey.AppendJSONString(nil, []byte(strconvQuote(id)), orderedkey.Ascending)
		if !ok {
			t.Fatal("test placement string did not encode")
		}
		probes[i] = replicatedPlacementProbe{
			id: id, document: []byte(fmt.Sprintf(`{"id":%s}`, strconvQuote(id))), key: key, point: point,
		}
	}
	sort.Slice(probes, func(i, j int) bool {
		return distribution.ComparePoints(probes[i].point, probes[j].point) < 0
	})
	return []replicatedPlacementProbe{probes[8], probes[24], probes[40]}
}

func strconvQuote(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func completionResultCode(t *testing.T, claim *ReplicatedApply, command []byte) uint32 {
	code, _ := completionResult(t, claim, command)
	return code
}

func completionResult(
	t *testing.T,
	claim *ReplicatedApply,
	command []byte,
) (uint32, uint16) {
	t.Helper()
	lookup, err := claim.LookupCompletion(command)
	if err != nil {
		t.Fatalf("LookupCompletion: %v", err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil {
		t.Fatalf("OpenCompletion: %v", err)
	}
	return completion.ResultCode, completion.ResultFormat
}

func TestReplicatedApplyCapacityQualificationProfile(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "capacity-profile")
	options := testReplicatedApplyOptions()
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}

	want := ReplicatedApplyCapacityProfile{
		Binding: base.Binding, ApplyFormat: ReplicatedApplyFormat,
		MaxSessions: options.MaxSessions, RetryWindow: options.RetryWindow,
	}
	want.RelationManifestDigest, err = claim.machine.RelationManifestDigest()
	if err != nil || want.RelationManifestDigest == ([sha256.Size]byte{}) {
		t.Fatalf("portable relation manifest digest = %x,%v",
			want.RelationManifestDigest, err)
	}
	checkpoint := uint64(0)
	assertProfile := func(label string, want ReplicatedApplyCapacityProfile, maxCheckpoint uint64) {
		t.Helper()
		got, err := claim.CapacityQualificationProfile()
		if err != nil {
			t.Fatalf("%s capacity profile: %v", label, err)
		}
		if got.CheckpointApplied < checkpoint || got.CheckpointApplied > maxCheckpoint ||
			got.CheckpointApplied > got.Applied {
			t.Fatalf(
				"%s checkpoint cut = %d, want monotonic [%d,%d] at applied %d",
				label, got.CheckpointApplied, checkpoint, maxCheckpoint, got.Applied,
			)
		}
		checkpoint = got.CheckpointApplied
		got.CheckpointApplied = 0
		if got != want {
			t.Fatalf("%s capacity profile = %+v; want %+v", label, got, want)
		}
	}
	assertProfile("uninitialized", want, 0)
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	want.Initialized, want.Applied = true, 1
	assertProfile("bootstrap", want, 0)

	document := []byte(`{"id":"capacity"}`)
	key := testReplicatedApplyKey(t, database, document)
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	command := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil {
		t.Fatal(err)
	}
	want.Applied, want.SessionCount, want.SessionSlotCount = 3, 1, 2
	want.SessionEpochHighWater = epoch
	// Pressure is deliberately a pre-publication barrier. Platform allocation
	// geometry may make it certify either earlier prefix, but never the just-
	// published index 3 transition and never a cut behind the prior observation.
	assertProfile("applied", want, 2)

	view, err := replication.OpenCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	digest := replicatedstate.SessionKey(view.AuthorityClass, view.Tenant, view.ClientID)
	var storageKey [33]byte
	storageKey[0] = 1
	copy(storageKey[1:], digest[:])
	core := database.connector.db
	document, found, err := core.replicatedApplyCollection.AppendRaw(nil, storageKey[:])
	if err != nil || !found {
		t.Fatalf("retained completion = %q, %t, %v", document, found, err)
	}
	document = bytes.Clone(document)
	if document[8] == '0' {
		document[8] = '1'
	} else {
		document[8] = '0'
	}
	corruptReplicatedApplyCollectionForTest(
		t, database, base, options.TxnLimits, replicatedstate.SystemCollectionName,
		func(batch *durable.WriteBatch) error {
			return batch.Put(storageKey[:], document)
		},
	)
	if err := claim.AdmitCommand(command); !errors.Is(err, replicatedstate.ErrSessionCorrupt) ||
		!errors.Is(err, replicatedstate.ErrApplyPoisoned) {
		t.Fatalf("first corrupt admission = %v, want session corruption plus poison", err)
	}
	if got, err := claim.CapacityQualificationProfile(); got != (ReplicatedApplyCapacityProfile{}) ||
		!errors.Is(err, replicatedstate.ErrApplyPoisoned) {
		t.Fatalf("poisoned capacity profile = %+v, %v; want zero, ErrApplyPoisoned", got, err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := claim.CapacityQualificationProfile(); got != (ReplicatedApplyCapacityProfile{}) ||
		!errors.Is(err, ErrReplicatedApplyClosed) {
		t.Fatalf("closed capacity profile = %+v, %v; want zero, ErrReplicatedApplyClosed", got, err)
	}
}

func TestReplicatedApplyMaximumRetryWindowCreatesAndReopens(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "maximum-retry-window")
	options := testReplicatedApplyOptions()
	options.RetryWindow = replicatedstate.MaxSessionRetryWindow
	limits := replicatedApplySystemLimits(options.RetryWindow)
	maxDocuments, err := replicatedstate.RequiredBundleTransactionDocuments(
		base.UserLimits.MaxBatchDocuments, options.RetryWindow, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	options.TxnLimits.MaxDocuments = maxDocuments
	if limits.MaxBatchDocuments != int(replicatedstate.MaxSessionRetryWindow)+2 ||
		limits.MaxBatchBytes < limits.MaxDocumentBytes+limits.MaxBatchDocuments*limits.MaxKeyBytes {
		t.Fatalf("maximum retry-window system limits = %+v", limits)
	}
	if err := validateReplicatedApplyOptions(base, options); err != nil {
		t.Fatalf("maximum retry-window options: %v", err)
	}
	tooSmall := options
	tooSmall.TxnLimits.MaxDocuments--
	if err := validateReplicatedApplyOptions(base, tooSmall); !errors.Is(
		err, ErrReplicatedApplyMismatch,
	) {
		t.Fatalf("one-short maximum retry-window transaction profile = %v", err)
	}

	bootstrap := testReplicatedApplyBootstrap()
	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatalf("create maximum retry-window apply storage: %v", err)
	}
	if identity.SystemLimits != limits {
		t.Fatalf("created maximum retry-window limits = %+v, want %+v", identity.SystemLimits, limits)
	}
	collection := database.connector.db.replicatedApplyCollection
	if collection.MaxBatchDocuments() != limits.MaxBatchDocuments ||
		collection.MaxBatchBytes() != limits.MaxBatchBytes {
		t.Fatalf(
			"created maximum retry-window collection = docs %d bytes %d, want %+v",
			collection.MaxBatchDocuments(), collection.MaxBatchBytes(), limits,
		)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	profile, err := claim.CapacityQualificationProfile()
	if err != nil || profile.SessionEpochHighWater != epoch ||
		profile.SessionCount != 1 || profile.SessionSlotCount != 1 {
		t.Fatalf("maximum retry-window capacity profile = %+v, %v", profile, err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatalf("validate maximum retry-window storage: %v", err)
	}
	reopenedClaim, reopenedIdentity, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if err != nil || reopenedIdentity != identity || reopenedClaim.Applied() != 2 {
		t.Fatalf("reopen maximum retry-window apply = %+v, %+v, %v", reopenedClaim, reopenedIdentity, err)
	}
	if err := reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyActivateValidateAndExactReopen(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "apply")
	options := testReplicatedApplyOptions()
	bootstrap := testReplicatedApplyBootstrap()

	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatalf("OpenReplicatedApply: %v", err)
	}
	if identity.Format != ReplicatedApplyFormat || identity.Storage == "" ||
		identity.ValidationDigest == ([32]byte{}) || identity.Placement != options.Placement ||
		identity.ValidationProfile != uint8(replicatedstate.ValidationDeterministicMutation) ||
		identity.TxnLimits != options.TxnLimits || identity.MaxSessions != options.MaxSessions ||
		identity.RetryWindow != options.RetryWindow ||
		identity.Sidecars != canonicalReplicatedApplySidecars() {
		t.Fatalf("apply identity = %+v", identity)
	}
	if got, err := claim.Identity(); err != nil || got != identity {
		t.Fatalf("claim.Identity = %+v,%v; want %+v", got, err, identity)
	}
	core := database.connector.db
	core.mu.RLock()
	hiddenPath := core.replicatedApplyPath(core.catalog.ReplicatedApply)
	if core.catalog.ReplicatedApply == nil || core.replicatedApplyCollection == nil ||
		core.catalog.ReplicatedApply.Sidecars != canonicalReplicatedApplySidecars() ||
		core.replicatedApplyCollection.SealedRecoveryJournalBytes() !=
			ReplicatedSystemRecoveryJournalBytes ||
		len(core.catalog.Tables) != 1 || core.tables["docs"] == nil {
		core.mu.RUnlock()
		t.Fatal("activation did not retain one visible table plus hidden participant")
	}
	core.mu.RUnlock()
	if _, err := os.Stat(hiddenPath); err != nil {
		t.Fatalf("hidden storage: %v", err)
	}
	if _, _, err := database.OpenReplicatedApply(base, bootstrap, options); !errors.Is(err, ErrReplicatedApplyBusy) {
		t.Fatalf("second claim = %v, want busy", err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)

	validDocument := []byte(`{"id":"a","n":1}`)
	validKey := testReplicatedApplyKey(t, database, validDocument)
	valid := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: validKey, Value: validDocument,
	})
	beforeClock := core.tables["docs"].conflicts.observe()
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), valid); err != nil {
		t.Fatalf("apply valid PUT: %v", err)
	}
	if got := completionResultCode(t, claim, valid); got != replicatedstate.ResultApplied {
		t.Fatalf("valid result = %d, want applied", got)
	}
	if _, format := completionResult(t, claim, valid); format != replicatedstate.ResultFormatMutation {
		t.Fatalf("valid result format = %d, want current format", format)
	}
	if after := core.tables["docs"].conflicts.observe(); after <= beforeClock {
		t.Fatalf("conflict clock did not advance: before=%d after=%d", beforeClock, after)
	}
	got, found, err := core.tables["docs"].collection.AppendRaw(nil, validKey)
	if err != nil || !found || !bytes.Equal(got, validDocument) {
		t.Fatalf("stored row = %q,%v,%v", got, found, err)
	}

	wrongKey, _ := orderedkey.AppendJSONString(nil, []byte(`"wrong"`), orderedkey.Ascending)
	invalid := testReplicatedApplyCommand(base, epoch, 3, replication.Mutation{
		Kind: replication.MutationPut, Key: wrongKey, Value: []byte(`{"id":"b"}`),
	})
	beforeRefusalClock := core.tables["docs"].conflicts.observe()
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(4), invalid); err != nil {
		t.Fatalf("apply invalid PUT refusal: %v", err)
	}
	if after := core.tables["docs"].conflicts.observe(); after != beforeRefusalClock {
		t.Fatalf("definite pre-user refusal advanced conflict clock: before=%d after=%d",
			beforeRefusalClock, after)
	}
	if got := completionResultCode(t, claim, invalid); got != replicatedstate.ResultInvalidDocument {
		t.Fatalf("invalid result = %d, want invalid-document", got)
	}
	if _, found, err := core.tables["docs"].collection.AppendRaw(nil, wrongKey); err != nil || found {
		t.Fatalf("invalid PUT durable row found=%v err=%v", found, err)
	}

	deleteAbsent := testReplicatedApplyCommand(base, epoch, 4, replication.Mutation{
		Kind: replication.MutationDelete, Key: wrongKey,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(5), deleteAbsent); err != nil {
		t.Fatalf("apply canonical absent DELETE: %v", err)
	}
	if got := completionResultCode(t, claim, deleteAbsent); got != replicatedstate.ResultApplied {
		t.Fatalf("absent DELETE result = %d, want applied", got)
	}
	deleteMalformed := testReplicatedApplyCommand(base, epoch, 5, replication.Mutation{
		Kind: replication.MutationDelete, Key: []byte("not-an-ordered-key"),
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(6), deleteMalformed); err != nil {
		t.Fatalf("apply malformed DELETE refusal: %v", err)
	}
	if got := completionResultCode(t, claim, deleteMalformed); got != replicatedstate.ResultInvalidDocument {
		t.Fatalf("malformed DELETE result = %d, want invalid-document", got)
	}

	semanticRefusals := []struct {
		name string
		key  []byte
		doc  []byte
		want uint32
	}{
		{"missing_primary", wrongKey, []byte(`{"other":1}`), replicatedstate.ResultInvalidDocument},
		{"null_primary", wrongKey, []byte(`{"id":null}`), replicatedstate.ResultInvalidDocument},
		{"object_primary", wrongKey, []byte(`{"id":{"x":1}}`), replicatedstate.ResultInvalidDocument},
		{"oversize_derived_key", wrongKey,
			[]byte(`{"id":"` + string(bytes.Repeat([]byte{'x'}, 300)) + `"}`),
			replicatedstate.ResultTargetBound},
	}
	nextIndex := uint64(7)
	nextSequence := uint64(6)
	for _, refusal := range semanticRefusals {
		t.Run(refusal.name, func(t *testing.T) {
			command := testReplicatedApplyCommand(base, epoch, nextSequence, replication.Mutation{
				Kind: replication.MutationPut, Key: refusal.key, Value: refusal.doc,
			})
			if _, err := claim.ApplyNormal(testReplicatedApplyMeta(nextIndex), command); err != nil {
				t.Fatalf("apply refusal: %v", err)
			}
			if got := completionResultCode(t, claim, command); got != refusal.want {
				t.Fatalf("result = %d, want %d", got, refusal.want)
			}
			nextIndex++
			nextSequence++
		})
	}

	// Validation observes only the final last-write-wins mutation, but still
	// runs before no-op elision. Invalid→valid is accepted; valid→invalid is a
	// deterministic refusal and cannot mutate the row.
	lwwDocument := []byte(`{"id":"lww","n":2}`)
	lwwKey := testReplicatedApplyKey(t, database, lwwDocument)
	lwwApplied := testReplicatedApplyCommand(base, epoch, nextSequence,
		replication.Mutation{Kind: replication.MutationPut, Key: lwwKey, Value: []byte(`{"id":"wrong"}`)},
		replication.Mutation{Kind: replication.MutationPut, Key: lwwKey, Value: lwwDocument},
	)
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(nextIndex), lwwApplied); err != nil {
		t.Fatalf("apply final-valid LWW: %v", err)
	}
	if got := completionResultCode(t, claim, lwwApplied); got != replicatedstate.ResultApplied {
		t.Fatalf("final-valid LWW result = %d", got)
	}
	nextIndex++
	nextSequence++
	lwwRefused := testReplicatedApplyCommand(base, epoch, nextSequence,
		replication.Mutation{Kind: replication.MutationPut, Key: lwwKey, Value: lwwDocument},
		replication.Mutation{Kind: replication.MutationPut, Key: lwwKey, Value: []byte(`{"id":"wrong"}`)},
	)
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(nextIndex), lwwRefused); err != nil {
		t.Fatalf("apply final-invalid LWW refusal: %v", err)
	}
	if got := completionResultCode(t, claim, lwwRefused); got != replicatedstate.ResultInvalidDocument {
		t.Fatalf("final-invalid LWW result = %d", got)
	}
	nextIndex++

	deletePresent := testReplicatedApplyCommand(base, epoch, nextSequence+1, replication.Mutation{
		Kind: replication.MutationDelete, Key: validKey,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(nextIndex), deletePresent); err != nil {
		t.Fatalf("apply present DELETE: %v", err)
	}
	if got := completionResultCode(t, claim, deletePresent); got != replicatedstate.ResultApplied {
		t.Fatalf("present DELETE result = %d", got)
	}
	finalApplied := nextIndex

	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatalf("read session during apply claim: %v", err)
	}
	if err := testRuntimeExec(session, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"direct"}`)}); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("direct INSERT = %v, want fenced", err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatalf("close apply claim: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close activated root: %v", err)
	}

	if db, err := OpenReplicatedShardStore(path, base); !errors.Is(err, ErrReplicatedApplyMismatch) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("base-only open = %v, want apply mismatch", err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatalf("exact activated open: %v", err)
	}
	reopenedClaim, reopenedIdentity, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if err != nil || reopenedIdentity != identity {
		t.Fatalf("reopen claim = %+v,%v; want %+v", reopenedIdentity, err, identity)
	}
	if reopenedClaim.Applied() != finalApplied {
		t.Fatalf("reopened Applied = %d, want %d", reopenedClaim.Applied(), finalApplied)
	}
	if _, err := reopenedClaim.LookupCompletion(valid); !errors.Is(err, replicatedstate.ErrRetryRetired) {
		t.Fatalf("reopened retired retry = %v, want ErrRetryRetired", err)
	}
	if got := completionResultCode(t, reopenedClaim, deletePresent); got != replicatedstate.ResultApplied {
		t.Fatalf("reopened completion result = %d", got)
	}
	if err := reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyPlacementRangeAndAdmissionParity(t *testing.T) {
	below, inside, upper := func() (
		replicatedPlacementProbe, replicatedPlacementProbe, replicatedPlacementProbe,
	) {
		probes := testReplicatedPlacementProbes(t)
		return probes[0], probes[1], probes[2]
	}()
	_, database, base := bindReplicatedApplyTestRoot(t, "placement-range")
	options := testReplicatedApplyOptions()
	options.Placement.Range = distribution.KeyRange{
		Start: inside.point, End: distribution.KeyspaceEnd{Point: upper.point},
	}
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = claim.Close()
		_ = database.Close()
	}()
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)

	apply := func(index, sequence uint64, probe replicatedPlacementProbe, kind replication.MutationKind) []byte {
		t.Helper()
		mutation := replication.Mutation{Kind: kind, Key: probe.key}
		if kind == replication.MutationPut {
			mutation.Value = probe.document
		}
		command := testReplicatedApplyCommand(base, epoch, sequence, mutation)
		if _, err := claim.ApplyNormal(testReplicatedApplyMeta(index), command); err != nil {
			t.Fatalf("ApplyNormal(%s): %v", probe.id, err)
		}
		return command
	}

	insidePut := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: inside.key, Value: inside.document,
	})
	if err := claim.AdmitCommand(insidePut); err != nil {
		t.Fatalf("admit range start: %v", err)
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), insidePut); err != nil {
		t.Fatal(err)
	}
	if code, format := completionResult(t, claim, insidePut); code != replicatedstate.ResultApplied || format != replicatedstate.ResultFormatMutation {
		t.Fatalf("range start completion = code %d format %d", code, format)
	}

	for i, probe := range []replicatedPlacementProbe{below, upper} {
		sequence := uint64(i + 3)
		mutations := []replication.Mutation{{
			Kind: replication.MutationPut, Key: probe.key, Value: probe.document,
		}}
		if i == 1 {
			// Placement validation observes the final collapsed mutation. An
			// invalid earlier value cannot mask the final wrong-shard value.
			mutations = append([]replication.Mutation{{
				Kind: replication.MutationPut, Key: probe.key, Value: []byte(`{"id":"mismatch"}`),
			}}, mutations...)
		}
		command := testReplicatedApplyCommand(base, epoch, sequence, mutations...)
		if err := claim.AdmitCommand(command); !errors.Is(err, replicatedstate.ErrAdmissionBound) {
			t.Fatalf("admit wrong-shard %s = %v", probe.id, err)
		}
		if _, err := claim.ApplyNormal(testReplicatedApplyMeta(sequence+1), command); err != nil {
			t.Fatal(err)
		}
		if code, format := completionResult(t, claim, command); code != replicatedstate.ResultWrongShard || format != replicatedstate.ResultFormatMutation {
			t.Fatalf("wrong-shard %s completion = code %d format %d", probe.id, code, format)
		}
	}

	absentUpper := testReplicatedApplyCommand(base, epoch, 5, replication.Mutation{
		Kind: replication.MutationDelete, Key: upper.key,
	})
	if err := claim.AdmitCommand(absentUpper); !errors.Is(err, replicatedstate.ErrAdmissionBound) {
		t.Fatalf("admit absent wrong-shard DELETE = %v", err)
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(6), absentUpper); err != nil {
		t.Fatal(err)
	}
	if code, format := completionResult(t, claim, absentUpper); code != replicatedstate.ResultWrongShard || format != replicatedstate.ResultFormatMutation {
		t.Fatalf("absent wrong-shard DELETE = code %d format %d", code, format)
	}

	presentInside := apply(7, 6, inside, replication.MutationDelete)
	if code := completionResultCode(t, claim, presentInside); code != replicatedstate.ResultApplied {
		t.Fatalf("present in-range DELETE = %d", code)
	}
	validator := newReplicatedSQLMutationValidator(
		base, database.connector.db.tables["docs"], options.Placement,
	)
	if got := validator.ValidateDelete(upper.key, upper.document, true); got != replicatedstate.MutationValidationWrongShard {
		t.Fatalf("pure present wrong-shard DELETE validation = %d", got)
	}

	// A forbidden out-of-band row is never followed by another apply. Capturing
	// a coherent cut remains cheap; the explicit canonical audit independently
	// re-routes the complete image and rejects the row.
	corruptReplicatedApplyCollectionForTest(
		t, database, base, options.TxnLimits, base.UserTable,
		func(batch *durable.WriteBatch) error { return batch.Put(upper.key, upper.document) },
	)
	snapshot, err := claim.machine.Snapshot("docs")
	if err != nil {
		t.Fatalf("coherent snapshot = %v", err)
	}
	_, auditErr := snapshot.CanonicalImageDigest()
	closeErr := snapshot.Close()
	if !errors.Is(auditErr, replicatedstate.ErrSchemaProfile) || closeErr != nil {
		t.Fatalf("wrong-shard image audit = %v, close = %v", auditErr, closeErr)
	}
}

func TestReplicatedSQLPlacementValidatorEscapedStringAndClosedScalarSet(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "placement-validator")
	defer database.Close()
	table := database.connector.db.tables["docs"]
	profile := testReplicatedApplyOptions().Placement
	validator := newReplicatedSQLMutationValidator(base, table, profile)

	escaped := []byte(`{"id":"quote\\\" slash\\\\ line\\n"}`)
	escapedKey, err := documentKey(escaped, "/id", table.primary, base.UserLimits.MaxKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	if got := validator.ValidatePut([]byte(escapedKey), escaped); got != replicatedstate.MutationValidationAccept {
		t.Fatalf("escaped shard-key PUT = %d", got)
	}
	if got := validator.ValidateDelete([]byte(escapedKey), escaped, true); got != replicatedstate.MutationValidationAccept {
		t.Fatalf("escaped present DELETE = %d", got)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if got := validator.ValidatePut([]byte(escapedKey), escaped); got != replicatedstate.MutationValidationAccept {
			t.Fatalf("escaped shard-key PUT = %d", got)
		}
		if got := validator.ValidateDelete([]byte(escapedKey), escaped, true); got != replicatedstate.MutationValidationAccept {
			t.Fatalf("escaped present DELETE = %d", got)
		}
	}); allocations != 0 {
		t.Fatalf("warmed escaped PUT + present DELETE allocations = %v, want 0", allocations)
	}

	boolean := []byte(`{"id":true}`)
	booleanKey, err := documentKey(boolean, "/id", table.primary, base.UserLimits.MaxKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	if got := validator.ValidatePut([]byte(booleanKey), boolean); got != replicatedstate.MutationValidationInvalid {
		t.Fatalf("boolean placement scalar = %d, want invalid", got)
	}
}

func TestReplicatedSQLPlacementValidatorCanonicalNumberParity(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "placement-number")
	defer database.Close()
	table := database.connector.db.tables["docs"]
	number, err := distribution.NewNumber("1")
	if err != nil {
		t.Fatal(err)
	}
	point, err := distribution.NewNativeMapper(1).PointFor([]distribution.Scalar{number})
	if err != nil {
		t.Fatal(err)
	}
	profile := testReplicatedApplyOptions().Placement
	profile.Range = distribution.KeyRange{
		Start: point, End: distribution.KeyspaceEnd{Max: true},
	}
	validator := newReplicatedSQLMutationValidator(base, table, profile)

	documents := [][]byte{[]byte(`{"id":1}`), []byte(`{"id":1.0}`), []byte(`{"id":1e0}`)}
	var canonicalKey []byte
	for _, document := range documents {
		key, err := documentKey(document, "/id", table.primary, base.UserLimits.MaxKeyBytes)
		if err != nil {
			t.Fatalf("documentKey(%s): %v", document, err)
		}
		if canonicalKey == nil {
			canonicalKey = []byte(key)
		} else if !bytes.Equal(canonicalKey, []byte(key)) {
			t.Fatalf("number spelling %s encoded key %x, want %x", document, key, canonicalKey)
		}
		if got := validator.ValidatePut([]byte(key), document); got != replicatedstate.MutationValidationAccept {
			t.Fatalf("number spelling %s PUT = %d", document, got)
		}
		if got := validator.ValidateDelete([]byte(key), document, true); got != replicatedstate.MutationValidationAccept {
			t.Fatalf("number spelling %s present DELETE = %d", document, got)
		}
	}
	if got := validator.ValidateDelete(canonicalKey, nil, false); got != replicatedstate.MutationValidationAccept {
		t.Fatalf("canonical absent number DELETE = %d", got)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if got := validator.ValidateDelete(canonicalKey, nil, false); got != replicatedstate.MutationValidationAccept {
			t.Fatalf("canonical absent number DELETE = %d", got)
		}
	}); allocations != 0 {
		t.Fatalf("warmed absent number DELETE allocations = %v, want 0", allocations)
	}
}

func TestReplicatedSQLPlacementValidatorScratchIsConcurrentAndAllocationFree(t *testing.T) {
	primary, err := vibejson.CompilePointer("/id")
	if err != nil {
		t.Fatal(err)
	}
	identity := ReplicatedShardStoreIdentity{
		Binding:        ReplicatedShardStoreBinding{Distribution: "test"},
		UserTable:      "docs",
		UserPrimaryKey: "/id",
		UserLimits:     ReplicatedShardStoreLimits{MaxKeyBytes: 256},
	}
	validator := newReplicatedSQLMutationValidator(
		identity, &table{primary: primary}, testReplicatedApplyOptions().Placement,
	)
	escaped := []byte(`{"id":"quote\\\" slash\\\\ line\\n"}`)
	escapedKeyText, err := documentKey(escaped, "/id", primary, identity.UserLimits.MaxKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	escapedKey := []byte(escapedKeyText)
	numberKey, ok := orderedkey.AppendNumber(nil, []byte("1"), orderedkey.Ascending)
	if !ok {
		t.Fatal("number key did not encode")
	}

	if got := validator.ValidatePut(escapedKey, escaped); got != replicatedstate.MutationValidationAccept {
		t.Fatalf("warm escaped PUT = %d", got)
	}
	if got := validator.ValidateDelete(numberKey, nil, false); got != replicatedstate.MutationValidationAccept {
		t.Fatalf("warm absent number DELETE = %d", got)
	}
	var putResult, deleteResult replicatedstate.MutationValidation
	if allocations := testing.AllocsPerRun(1000, func() {
		putResult = validator.ValidatePut(escapedKey, escaped)
		deleteResult = validator.ValidateDelete(numberKey, nil, false)
	}); allocations != 0 {
		t.Fatalf("warmed PUT + absent DELETE allocations = %v, want 0", allocations)
	}
	if putResult != replicatedstate.MutationValidationAccept ||
		deleteResult != replicatedstate.MutationValidationAccept {
		t.Fatalf("warmed validation = PUT %d, DELETE %d", putResult, deleteResult)
	}
	var canonicalNumberKey []byte
	for _, document := range [][]byte{
		[]byte(`{"id":1}`), []byte(`{"id":1.0}`), []byte(`{"id":1e0}`),
	} {
		keyText, keyErr := documentKey(document, "/id", primary, identity.UserLimits.MaxKeyBytes)
		if keyErr != nil {
			t.Fatalf("number key for %s: %v", document, keyErr)
		}
		key := []byte(keyText)
		if canonicalNumberKey == nil {
			canonicalNumberKey = bytes.Clone(key)
		} else if !bytes.Equal(key, canonicalNumberKey) {
			t.Fatalf("number key for %s = %x, want %x", document, key, canonicalNumberKey)
		}
		if got := validator.ValidatePut(key, document); got != replicatedstate.MutationValidationAccept {
			t.Fatalf("number PUT for %s = %d", document, got)
		}
	}

	const workers, iterations = 8, 2000
	start := make(chan struct{})
	failures := make(chan string, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer group.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				if got := validator.ValidatePut(escapedKey, escaped); got != replicatedstate.MutationValidationAccept {
					failures <- fmt.Sprintf("concurrent escaped PUT = %d", got)
					return
				}
				if got := validator.ValidateDelete(numberKey, nil, false); got != replicatedstate.MutationValidationAccept {
					failures <- fmt.Sprintf("concurrent absent DELETE = %d", got)
					return
				}
			}
		}()
	}
	close(start)
	group.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}

func TestReplicatedApplySettlementAndStrictIdentity(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "settlement")
	options := testReplicatedApplyOptions()
	claim, identity, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	settled, got, err := OpenReplicatedShardStoreWithApplyForSettlement(path, base, options)
	if err != nil || got != identity {
		t.Fatalf("settlement = %+v,%v; want %+v", got, err, identity)
	}
	if err := settled.Close(); err != nil {
		t.Fatal(err)
	}
	wrongOptions := options
	wrongOptions.MaxSessions++
	if db, _, err := OpenReplicatedShardStoreWithApplyForSettlement(
		path, base, wrongOptions,
	); !errors.Is(err, ErrReplicatedApplyMismatch) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("wrong settlement options = %v, want mismatch", err)
	}
	wrongIdentity := identity
	wrongIdentity.Storage = "0" + wrongIdentity.Storage[1:]
	if wrongIdentity.Storage == identity.Storage {
		wrongIdentity.Storage = "1" + wrongIdentity.Storage[1:]
	}
	if db, err := OpenReplicatedShardStoreWithApply(path, base, wrongIdentity); !errors.Is(
		err, ErrReplicatedApplyMismatch,
	) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("wrong exact identity = %v, want mismatch", err)
	}
	wrongCapture := identity
	wrongCapture.CaptureStorage = "0" + wrongCapture.CaptureStorage[1:]
	if wrongCapture.CaptureStorage == identity.CaptureStorage {
		wrongCapture.CaptureStorage = "1" + wrongCapture.CaptureStorage[1:]
	}
	if db, err := OpenReplicatedShardStoreWithApply(path, base, wrongCapture); !errors.Is(
		err, ErrReplicatedApplyMismatch,
	) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("wrong exact capture identity = %v, want mismatch", err)
	}
	wrongLimits := identity
	wrongLimits.CaptureLimits.MaxBatchBytes--
	if db, err := OpenReplicatedShardStoreWithApply(path, base, wrongLimits); !errors.Is(
		err, ErrReplicatedApplyMismatch,
	) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("wrong exact capture limits = %v, want mismatch", err)
	}

	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip ReplicatedApplyIdentity
	if err := json.Unmarshal(raw, &roundTrip); err != nil || roundTrip != identity {
		t.Fatalf("identity JSON roundtrip = %+v,%v", roundTrip, err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	object["unknown"] = json.RawMessage("1")
	corrupt, _ := json.Marshal(object)
	if err := json.Unmarshal(corrupt, &roundTrip); err == nil {
		t.Fatal("strict identity decoder accepted an unknown member")
	}

	missingPath := filepath.Join(path+".tables", identity.Storage+".vjc")
	if err := os.Remove(missingPath); err != nil {
		t.Fatalf("remove hidden store: %v", err)
	}
	if db, err := OpenReplicatedShardStoreWithApply(path, base, identity); err == nil {
		if db != nil {
			_ = db.Close()
		}
		t.Fatal("exact open accepted a missing hidden store")
	}

	path2, database2, base2 := bindReplicatedApplyTestRoot(t, "settlement-missing-capture")
	claim2, identity2, err := database2.OpenReplicatedApply(
		base2, testReplicatedApplyBootstrap(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = claim2.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database2.Close(); err != nil {
		t.Fatal(err)
	}
	capturePath := filepath.Join(path2+".tables", identity2.CaptureStorage+".vjc")
	if err = os.Remove(capturePath); err != nil {
		t.Fatalf("remove hidden capture store: %v", err)
	}
	if db, err := OpenReplicatedShardStoreWithApply(path2, base2, identity2); err == nil {
		if db != nil {
			_ = db.Close()
		}
		t.Fatal("exact open accepted a missing hidden capture store")
	}
}

func TestReplicatedApplyActivationPublicationSettlement(t *testing.T) {
	t.Run("definite cleanup", func(t *testing.T) {
		_, db, base := bindReplicatedApplyTestRoot(t, "definite-apply")
		options := testReplicatedApplyOptions()
		bootstrap := testReplicatedApplyBootstrap()
		claim, identity, err := db.openReplicatedApply(
			base, bootstrap, options,
			func(*database) (bool, error) {
				return false, errors.New("injected definite catalog failure")
			},
		)
		if claim != nil || identity != (ReplicatedApplyIdentity{}) || err == nil {
			t.Fatalf("definite activation = %p,%+v,%v", claim, identity, err)
		}
		core := db.connector.db
		core.mu.RLock()
		if core.catalog.ReplicatedApply != nil || core.replicatedApplyCollection != nil ||
			core.replicatedApplyFile != nil || core.replicatedCaptureCollection != nil ||
			core.replicatedCaptureFile != nil {
			core.mu.RUnlock()
			t.Fatal("definite failure retained a published apply descriptor or handle")
		}
		core.mu.RUnlock()

		// The failed candidate was adopted into the catalog-scoped transaction
		// log before descriptor persistence. Definite failure must detach it before
		// discard so a later candidate can be adopted and its first cross-store
		// transaction can commit without stale registration or poison.
		claim, identity, err = db.OpenReplicatedApply(base, bootstrap, options)
		if err != nil || claim == nil || identity.Storage == "" {
			t.Fatalf("activation after definite cleanup = %p,%+v,%v", claim, identity, err)
		}
		if _, err := claim.InstallSnapshot(bootstrap); err != nil {
			t.Fatalf("install bootstrap after definite cleanup: %v", err)
		}
		epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
		document := []byte(`{"id":"after-definite-failure","n":1}`)
		key := testReplicatedApplyKey(t, db, document)
		command := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
			Kind: replication.MutationPut, Key: key, Value: document,
		})
		if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil {
			t.Fatalf("transaction after definite cleanup: %v", err)
		}
		if got := completionResultCode(t, claim, command); got != replicatedstate.ResultApplied {
			t.Fatalf("transaction result after definite cleanup = %d, want applied", got)
		}
		stored, found, readErr := core.tables["docs"].collection.AppendRaw(nil, key)
		if readErr != nil || !found || !bytes.Equal(stored, document) {
			t.Fatalf(
				"row after definite cleanup = %q,%v,%v, want %s",
				stored, found, readErr, document,
			)
		}
		if err := claim.Close(); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("unknown exact retry", func(t *testing.T) {
		_, db, base := bindReplicatedApplyTestRoot(t, "unknown-apply")
		options := testReplicatedApplyOptions()
		claim, identity, err := db.openReplicatedApply(
			base, testReplicatedApplyBootstrap(), options,
			func(*database) (bool, error) {
				return false, durable.ErrCommitOutcomeUnknown
			},
		)
		if claim != nil || identity == (ReplicatedApplyIdentity{}) ||
			!errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			t.Fatalf("unknown activation = %p,%+v,%v", claim, identity, err)
		}
		claim, retried, err := db.OpenReplicatedApply(
			base, testReplicatedApplyBootstrap(), options,
		)
		if err != nil || claim == nil || retried != identity {
			t.Fatalf("exact retry = %p,%+v,%v; want %+v", claim, retried, err, identity)
		}
		if err := claim.Close(); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestReplicatedApplyTwoStorePublicationFencesFailClosed(t *testing.T) {
	for failAt := 1; failAt <= 4; failAt++ {
		t.Run(fmt.Sprintf("table_directory_fence_%d", failAt), func(t *testing.T) {
			_, db, base := bindReplicatedApplyTestRoot(t, fmt.Sprintf("two-store-fence-%d", failAt))
			core := db.connector.db
			calls := 0
			fault := errors.New("injected two-store namespace fence failure")
			core.syncDir = func(path string) error {
				if path == core.dataDir {
					calls++
					if calls == failAt {
						return fault
					}
				}
				return syncDirectory(path)
			}
			claim, identity, err := db.OpenReplicatedApply(
				base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
			)
			if claim != nil || identity != (ReplicatedApplyIdentity{}) ||
				!errors.Is(err, durable.ErrCommitOutcomeUnknown) || !errors.Is(err, fault) {
				t.Fatalf("faulted two-store activation = %p,%+v,%v", claim, identity, err)
			}
			core.mu.RLock()
			published := core.catalog.ReplicatedApply != nil
			live := core.replicatedApplyCollection != nil || core.replicatedApplyFile != nil ||
				core.replicatedCaptureCollection != nil || core.replicatedCaptureFile != nil
			core.mu.RUnlock()
			if published || live {
				t.Fatalf("faulted activation retained catalog=%t live=%t", published, live)
			}
			core.syncDir = nil
			claim, identity, err = db.OpenReplicatedApply(
				base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
			)
			if err != nil || claim == nil || identity.Storage == "" || identity.CaptureStorage == "" {
				t.Fatalf("retry after fence failure = %p,%+v,%v", claim, identity, err)
			}
			if err = claim.Close(); err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReplicatedApplySecondAdoptionFailureDetachesAndRecovers(t *testing.T) {
	for _, detachFault := range []bool{false, true} {
		t.Run(fmt.Sprintf("detach-fault-%t", detachFault), func(t *testing.T) {
			_, database, base := bindReplicatedApplyTestRoot(t, "capture-partial-adoption")
			core := database.connector.db
			adoptFault := errors.New("injected capture adoption failure")
			detachError := errors.New("injected adopted-base detach failure")
			adoptions := 0
			core.adoptCollection = func(collection *durable.Collection) error {
				adoptions++
				if adoptions == 2 {
					return adoptFault
				}
				return core.txnLog.AdoptCollection(collection)
			}
			if detachFault {
				core.detachCollection = func(*durable.Collection) error { return detachError }
			}
			claim, identity, err := database.OpenReplicatedApply(
				base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
			)
			if claim != nil || identity != (ReplicatedApplyIdentity{}) ||
				!errors.Is(err, adoptFault) || (detachFault && !errors.Is(err, detachError)) {
				t.Fatalf("partial adoption = %p,%+v,%v", claim, identity, err)
			}
			core.mu.RLock()
			published := core.catalog.ReplicatedApply != nil
			retained := core.replicatedApplyCollection != nil || core.replicatedApplyFile != nil ||
				core.replicatedCaptureCollection != nil || core.replicatedCaptureFile != nil
			core.mu.RUnlock()
			if published || retained != detachFault {
				t.Fatalf("failure ownership: published=%t retained=%t, want retained=%t",
					published, retained, detachFault)
			}
			core.adoptCollection = nil
			core.detachCollection = nil
			claim, identity, err = database.OpenReplicatedApply(
				base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
			)
			if err != nil || claim == nil || identity.Storage == "" || identity.CaptureStorage == "" {
				t.Fatalf("retry after partial adoption = %p,%+v,%v", claim, identity, err)
			}
			if err = claim.Close(); err != nil {
				t.Fatal(err)
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReplicatedApplyRejectsForeignCaptureStore(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "capture-binding-a")
	options := testReplicatedApplyOptions()
	claim, identity, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}

	foreignPath, foreignDatabase, foreignBase := bindReplicatedApplyTestRoot(t, "capture-binding-b")
	foreignClaim, foreignIdentity, err := foreignDatabase.OpenReplicatedApply(
		foreignBase, testReplicatedApplyBootstrap(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = foreignClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = foreignDatabase.Close(); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(path+".tables", identity.CaptureStorage+".vjc")
	foreign := filepath.Join(foreignPath+".tables", foreignIdentity.CaptureStorage+".vjc")
	if err = os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(durable.RecoveryJournalPath(target)); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(foreign, target); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(durable.RecoveryJournalPath(foreign), durable.RecoveryJournalPath(target)); err != nil {
		t.Fatal(err)
	}
	if opened, openErr := OpenReplicatedShardStoreWithApply(path, base, identity); openErr == nil {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatal("exact open accepted a foreign capture store")
	}
}

func TestReplicatedApplyActivationAdoptsWithoutReopen(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "adopt-without-reopen")
	core := database.connector.db
	closeCalls := 0
	var temporary [2]*durable.Collection
	core.closeCollection = func(collection *durable.Collection) error {
		if closeCalls >= len(temporary) {
			return errors.New("activation closed a post-publication hidden collection")
		}
		temporary[closeCalls] = collection
		closeCalls++
		return collection.Close()
	}
	claim, identity, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil || claim == nil || identity.Storage == "" {
		t.Fatalf("activation = %p,%+v,%v", claim, identity, err)
	}
	if closeCalls != 2 {
		t.Fatalf("activation close calls = %d, want two temporary closes", closeCalls)
	}
	core.mu.RLock()
	if core.replicatedApplyCollection == nil || core.replicatedApplyFile == nil ||
		core.replicatedCaptureCollection == nil || core.replicatedCaptureFile == nil ||
		core.replicatedApplyCollection == temporary[0] ||
		core.replicatedApplyCollection == temporary[1] ||
		core.replicatedCaptureCollection == temporary[0] ||
		core.replicatedCaptureCollection == temporary[1] ||
		core.catalog.ReplicatedApply == nil || len(core.retired) != 0 {
		core.mu.RUnlock()
		t.Fatal("activation did not directly adopt the published hidden collection")
	}
	core.mu.RUnlock()
	core.closeCollection = nil
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyPreflightAndPreRecoveryFences(t *testing.T) {
	t.Run("reserved name before mutation", func(t *testing.T) {
		_, database, base := bindReplicatedApplyTestRoot(t, "reserved-apply")
		reserved := base.Clone()
		reserved.UserTable = replicatedstate.SystemCollectionName
		reserved.Relations[0].Table = reserved.UserTable
		reserved.RelationManifestDigest = replicatedRelationManifestDigest(reserved)
		claim, identity, err := database.OpenReplicatedApply(
			reserved, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
		)
		if claim != nil || identity != (ReplicatedApplyIdentity{}) ||
			!errors.Is(err, ErrReplicatedApplyMismatch) {
			t.Fatalf("reserved activation = %p,%+v,%v", claim, identity, err)
		}
		core := database.connector.db
		core.mu.RLock()
		defer core.mu.RUnlock()
		if core.catalog.ReplicatedApply != nil || core.replicatedApplyCollection != nil ||
			core.replicatedApplyFile != nil {
			t.Fatal("reserved name mutated apply catalog or storage ownership")
		}
	})

	t.Run("mismatch before recovery", func(t *testing.T) {
		path, database, base := bindReplicatedApplyTestRoot(t, "pre-recovery")
		options := testReplicatedApplyOptions()
		claim, identity, err := database.OpenReplicatedApply(
			base, testReplicatedApplyBootstrap(), options,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := claim.Close(); err != nil {
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}

		syncCalls := 0
		wrong := identity
		wrong.ValidationDigest[0] ^= 0x80
		opened, err := openDatabaseWithShardStorePolicy(path, func(string) error {
			syncCalls++
			return nil
		}, shardStoreOpenPolicy{
			mode:                    shardStoreOpenReplicatedApplyExisting,
			expectedReplicated:      base,
			expectedReplicatedApply: wrong,
		})
		if opened != nil {
			_ = opened.closeTerminal()
		}
		if !errors.Is(err, ErrReplicatedApplyMismatch) || syncCalls != 0 {
			t.Fatalf("exact mismatch = %v, sync calls=%d", err, syncCalls)
		}

		identityMismatches := []struct {
			name   string
			mutate func(*ReplicatedApplyIdentity, *ReplicatedShardStoreIdentity)
		}{
			{"placement format", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.Format++
			}},
			{"capture storage", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.CaptureStorage = apply.Storage
			}},
			{"capture limits", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.CaptureLimits.MaxDocumentBytes--
			}},
			{"tuple version", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.TupleVersion++
			}},
			{"mapper version", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.MapperVersion++
			}},
			{"shard key", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.ShardKey += "/other"
			}},
			{"placement range", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.Range.Start[0] = 1
			}},
			{"placement range end", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.Range.End.Point[7] = 1
			}},
			{"placement range end max", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				for i := range apply.Placement.Range.End.Point {
					apply.Placement.Range.End.Point[i] = 0xff
				}
				apply.Placement.Range.End.Max = false
			}},
			{"system journal sidecar", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Sidecars.SystemRecoveryJournalBytes--
			}},
			{"user journal sidecar", func(_ *ReplicatedApplyIdentity, base *ReplicatedShardStoreIdentity) {
				base.Sidecars.UserRecoveryJournalBytes--
			}},
			{"transaction marker sidecar", func(_ *ReplicatedApplyIdentity, base *ReplicatedShardStoreIdentity) {
				base.Sidecars.TransactionMarkerBytes++
			}},
			{"allocation generation", func(_ *ReplicatedApplyIdentity, base *ReplicatedShardStoreIdentity) {
				base.Binding.AllocationGeneration++
			}},
			{"routing version", func(_ *ReplicatedApplyIdentity, base *ReplicatedShardStoreIdentity) {
				base.Binding.Authority.RoutingVersion++
			}},
			{"route generation", func(_ *ReplicatedApplyIdentity, base *ReplicatedShardStoreIdentity) {
				base.Binding.Authority.RouteGeneration++
			}},
		}
		for _, test := range identityMismatches {
			t.Run(test.name, func(t *testing.T) {
				wrongApply, wrongBase := identity, base
				test.mutate(&wrongApply, &wrongBase)
				syncCalls = 0
				opened, err := openDatabaseWithShardStorePolicy(path, func(string) error {
					syncCalls++
					return nil
				}, shardStoreOpenPolicy{
					mode:                    shardStoreOpenReplicatedApplyExisting,
					expectedReplicated:      wrongBase,
					expectedReplicatedApply: wrongApply,
				})
				if opened != nil {
					_ = opened.closeTerminal()
				}
				if err == nil || syncCalls != 0 {
					t.Fatalf("pre-recovery mismatch = %v, sync calls=%d", err, syncCalls)
				}
			})
		}

		syncCalls = 0
		wrongOptions := options
		wrongOptions.MaxSessions++
		opened, err = openDatabaseWithShardStorePolicy(path, func(string) error {
			syncCalls++
			return nil
		}, shardStoreOpenPolicy{
			mode:                      shardStoreOpenReplicatedApplySettlement,
			expectedReplicated:        base,
			expectedReplicatedOptions: wrongOptions,
		})
		if opened != nil {
			_ = opened.closeTerminal()
		}
		if !errors.Is(err, ErrReplicatedApplyMismatch) || syncCalls != 0 {
			t.Fatalf("settlement mismatch = %v, sync calls=%d", err, syncCalls)
		}

		syncCalls = 0
		wrongOptions = options
		wrongOptions.Placement.Range.Start[0] = 1
		opened, err = openDatabaseWithShardStorePolicy(path, func(string) error {
			syncCalls++
			return nil
		}, shardStoreOpenPolicy{
			mode:                      shardStoreOpenReplicatedApplySettlement,
			expectedReplicated:        base,
			expectedReplicatedOptions: wrongOptions,
		})
		if opened != nil {
			_ = opened.closeTerminal()
		}
		if !errors.Is(err, ErrReplicatedApplyMismatch) || syncCalls != 0 {
			t.Fatalf("settlement placement mismatch = %v, sync calls=%d", err, syncCalls)
		}
	})
}

func TestReplicatedApplyOpenFullScanRejectsPrimaryMismatch(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "full-scan")
	options := testReplicatedApplyOptions()
	bootstrap := testReplicatedApplyBootstrap()
	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	document := []byte(`{"id":"scan"}`)
	key := testReplicatedApplyKey(t, database, document)
	command := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a forbidden out-of-band mutation to an individually valid JSON
	// row whose document primary no longer matches its physical key.
	corruptReplicatedApplyCollectionForTest(
		t, database, base, options.TxnLimits, base.UserTable,
		func(batch *durable.WriteBatch) error {
			return batch.Put(key, []byte(`{"id":"other"}`))
		},
	)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatalf("exact storage open: %v", err)
	}
	badClaim, _, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if badClaim != nil {
		_ = badClaim.Close()
	}
	if err == nil {
		t.Fatal("Machine Open accepted a key/document primary mismatch")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyOpenFullScanRejectsWrongShard(t *testing.T) {
	probes := testReplicatedPlacementProbes(t)
	inside, upper := probes[1], probes[2]
	path, database, base := bindReplicatedApplyTestRoot(t, "full-scan-placement")
	options := testReplicatedApplyOptions()
	options.Placement.Range = distribution.KeyRange{
		Start: inside.point, End: distribution.KeyspaceEnd{Point: upper.point},
	}
	bootstrap := testReplicatedApplyBootstrap()
	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	corruptReplicatedApplyCollectionForTest(
		t, database, base, options.TxnLimits, base.UserTable,
		func(batch *durable.WriteBatch) error { return batch.Put(upper.key, upper.document) },
	)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	badClaim, _, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if badClaim != nil {
		_ = badClaim.Close()
	}
	if !errors.Is(err, replicatedstate.ErrSchemaProfile) {
		t.Fatalf("wrong-shard full scan = %v, want schema-profile refusal", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyIdentityStrictGrammar(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "identity-grammar")
	claim, identity, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	defer database.Close()
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		raw  []byte
	}{
		{"duplicate", bytes.Replace(raw, []byte(`{"format":`), []byte(`{"format":1,"format":`), 1)},
		{"null_format", bytes.Replace(raw, []byte(`{"format":0`), []byte(`{"format":null`), 1)},
		{"null_digest", bytes.Replace(raw, fields["validation_digest"], []byte("null"), 1)},
		{"uppercase_digest", bytes.Replace(raw, fields["validation_digest"], bytes.ToUpper(fields["validation_digest"]), 1)},
		{"null_placement", bytes.Replace(raw, fields["placement"], []byte("null"), 1)},
		{"nested_duplicate", bytes.Replace(raw, []byte(`"placement":{"format":`), []byte(`"placement":{"format":1,"format":`), 1)},
		{"nested_null_format", bytes.Replace(
			raw, []byte(`"placement":{"format":0`), []byte(`"placement":{"format":null`), 1,
		)},
		{"nested_unknown", bytes.Replace(raw, []byte(`"range_end_max":true`), []byte(`"unknown":0,"range_end_max":true`), 1)},
	}
	missing := make(map[string]json.RawMessage, len(fields)-1)
	for name, value := range fields {
		if name != "txn_max_bytes" {
			missing[name] = value
		}
	}
	missingRaw, _ := json.Marshal(missing)
	tests = append(tests, struct {
		name string
		raw  []byte
	}{"missing", missingRaw})
	missingPlacement := make(map[string]json.RawMessage, len(fields)-1)
	for name, value := range fields {
		if name != "placement" {
			missingPlacement[name] = value
		}
	}
	missingPlacementRaw, _ := json.Marshal(missingPlacement)
	tests = append(tests, struct {
		name string
		raw  []byte
	}{"missing_placement", missingPlacementRaw})

	var placementFields map[string]json.RawMessage
	if err := json.Unmarshal(fields["placement"], &placementFields); err != nil {
		t.Fatal(err)
	}
	placementCases := []struct {
		name   string
		mutate func(map[string]json.RawMessage)
	}{
		{"nested_missing", func(values map[string]json.RawMessage) { delete(values, "mapper_version") }},
		{"profile_format", func(values map[string]json.RawMessage) { values["format"] = []byte("2") }},
		{"profile_shard_key", func(values map[string]json.RawMessage) { values["shard_key"] = []byte(`"id"`) }},
		{"profile_tuple_version", func(values map[string]json.RawMessage) { values["tuple_version"] = []byte("2") }},
		{"profile_mapper_version", func(values map[string]json.RawMessage) { values["mapper_version"] = []byte("2") }},
		{"uppercase_start", func(values map[string]json.RawMessage) {
			values["range_start"] = []byte(`"ABCDEF0000000000"`)
		}},
		{"short_end", func(values map[string]json.RawMessage) { values["range_end"] = []byte(`"00"`) }},
		{"empty_range", func(values map[string]json.RawMessage) { values["range_end_max"] = []byte("false") }},
		{"noncanonical_max_end", func(values map[string]json.RawMessage) {
			values["range_end"] = []byte(`"0000000000000001"`)
		}},
	}
	for _, test := range placementCases {
		placementCopy := make(map[string]json.RawMessage, len(placementFields))
		for name, value := range placementFields {
			placementCopy[name] = bytes.Clone(value)
		}
		test.mutate(placementCopy)
		placementRaw, _ := json.Marshal(placementCopy)
		identityCopy := make(map[string]json.RawMessage, len(fields))
		for name, value := range fields {
			identityCopy[name] = bytes.Clone(value)
		}
		identityCopy["placement"] = placementRaw
		caseRaw, _ := json.Marshal(identityCopy)
		tests = append(tests, struct {
			name string
			raw  []byte
		}{test.name, caseRaw})
	}
	unsupportedFormat := make(map[string]json.RawMessage, len(fields))
	for name, value := range fields {
		unsupportedFormat[name] = bytes.Clone(value)
	}
	unsupportedFormat["format"] = []byte("2")
	unsupportedFormatRaw, _ := json.Marshal(unsupportedFormat)
	tests = append(tests, struct {
		name string
		raw  []byte
	}{"unsupported_format", unsupportedFormatRaw})
	unsupportedValidation := make(map[string]json.RawMessage, len(fields))
	for name, value := range fields {
		unsupportedValidation[name] = bytes.Clone(value)
	}
	unsupportedValidation["validation_profile"] = []byte("3")
	unsupportedValidationRaw, _ := json.Marshal(unsupportedValidation)
	tests = append(tests, struct {
		name string
		raw  []byte
	}{"unsupported_validation_profile", unsupportedValidationRaw})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var decoded ReplicatedApplyIdentity
			if err := json.Unmarshal(test.raw, &decoded); err == nil {
				t.Fatalf("accepted noncanonical identity: %s", test.raw)
			}
		})
	}
}

func TestReplicatedApplyClaimConnectorLifetime(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "claim-lifetime")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	); !errors.Is(err, ErrReplicatedApplyBusy) {
		t.Fatalf("second claim with live session = %v, want busy", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := claim.Identity(); err != nil {
		t.Fatalf("claim invalidated while connector refs remain: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatalf("idempotent claim Close: %v", err)
	}
}

func TestReplicatedApplyObserverConservativelyPublishesUnknownOutcome(t *testing.T) {
	_, database, binding, _ := prepareReplicatedTestRoot(t, "observer-unknown", false)
	base := requireReplicatedShardStoreBind(t, database, binding, "docs")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = claim.Close()
		_ = database.Close()
	})
	if _, err := claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	document := []byte(`{"id":"unknown"}`)
	key := testReplicatedApplyKey(t, database, document)
	command := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	// Fault this transition's completed decision append directly. There is no
	// marker Sync on the steady group path, and adding one here would invalidate
	// the durability contract this regression exercises.
	restore := durable.InstallCheckpointGroupDecisionAppendFaultForFacadeTest()
	t.Cleanup(restore)
	clock := &database.connector.db.tables["docs"].conflicts
	before := clock.observe()
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), command); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("decision-append apply = %v, want unknown outcome", err)
	}
	if after := clock.observe(); after <= before {
		t.Fatalf("unknown publication did not advance conflict clock: before=%d after=%d",
			before, after)
	}
}

func TestReplicatedApplyNormalBatchPublishesNetNoopConflictUnion(t *testing.T) {
	_, database, binding, _ := prepareReplicatedTestRoot(t, "observer-batch-net-noop", false)
	base := requireReplicatedShardStoreBind(t, database, binding, "docs")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = claim.Close()
		_ = database.Close()
	})
	if _, err := claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	secondOpen := testReplicatedApplyCommandValue(base, 0, 1, nil)
	secondOpen.ClientID = replication.ID128{10}
	secondOpen.Kind = replication.CommandSessionOpen
	secondOpen.Batches = nil
	secondOpen.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	secondOpen.Fingerprint = sha256.Sum256([]byte("driver/test-second-session-open"))
	secondOpenBytes, err := replication.AppendCommand(nil, secondOpen)
	if err != nil {
		t.Fatal(err)
	}
	if publication, applyErr := claim.ApplyNormal(
		testReplicatedApplyMeta(3), secondOpenBytes,
	); applyErr != nil || publication.Applied != 3 {
		t.Fatalf("second session open = %+v, %v", publication, applyErr)
	}
	document := []byte(`{"id":"batch-net-noop"}`)
	key := testReplicatedApplyKey(t, database, document)
	put := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	deleteValue := testReplicatedApplyCommandValue(base, 3, 2, []replication.Mutation{{
		Kind: replication.MutationDelete, Key: key,
	}})
	deleteValue.ClientID = secondOpen.ClientID
	deleteCommand, err := replication.AppendCommand(nil, deleteValue)
	if err != nil {
		t.Fatal(err)
	}
	entries := []raftmodel.NormalApply{
		{Meta: testReplicatedApplyMeta(4), Data: put},
		{Meta: testReplicatedApplyMeta(5), Data: deleteCommand},
	}
	core := database.connector.db
	clock := &core.tables["docs"].conflicts
	beforeClock := clock.observe()
	beforeStats := core.checkpointGroup.Stats()
	witnesses := make([][32]byte, len(entries))
	applied, publication, err := claim.ApplyNormalBatch(entries, witnesses)
	if err != nil || applied != len(entries) || publication.Applied != 5 {
		t.Fatalf("ApplyNormalBatch = %d, %+v, %v", applied, publication, err)
	}
	if witnesses[len(entries)-1] != publication.DataChainDigest {
		t.Fatalf("final batch witness = %x, want %x",
			witnesses[len(entries)-1], publication.DataChainDigest)
	}
	if afterClock := clock.observe(); afterClock <= beforeClock {
		t.Fatalf("net-noop batch conflict clock = before %d after %d",
			beforeClock, afterClock)
	}
	afterStats := core.checkpointGroup.Stats()
	if afterStats.TransactionHighWater != beforeStats.TransactionHighWater+1 ||
		afterStats.Updates != beforeStats.Updates+1 {
		t.Fatalf("batch group stats = before %+v after %+v", beforeStats, afterStats)
	}
	if _, found, err := core.tables["docs"].collection.AppendRaw(nil, key); err != nil || found {
		t.Fatalf("net-noop user row = found %v, err %v", found, err)
	}
}

func TestReplicatedApplyNormalBatchClearsWitnessesOnEveryWrapperEarlyReturn(t *testing.T) {
	entries := []raftmodel.NormalApply{{}, {}}
	assertCleared := func(t *testing.T, claim *ReplicatedApply) {
		t.Helper()
		witnesses := [][32]byte{{1}, {2}, {3}}
		applied, publication, err := claim.ApplyNormalBatch(entries, witnesses)
		if err == nil || applied != 0 || publication != (raftmodel.Publication{}) {
			t.Fatalf("early ApplyNormalBatch = %d, %+v, %v", applied, publication, err)
		}
		if witnesses[0] != ([32]byte{}) || witnesses[1] != ([32]byte{}) ||
			witnesses[2] != ([32]byte{3}) {
			t.Fatalf("early witnesses = %x %x %x", witnesses[0], witnesses[1], witnesses[2])
		}
	}

	t.Run("nil", func(t *testing.T) {
		assertCleared(t, (*ReplicatedApply)(nil))
	})
	t.Run("missing-database", func(t *testing.T) {
		assertCleared(t, &ReplicatedApply{})
	})

	core := &database{}
	claim := &ReplicatedApply{database: core, machine: new(replicatedstate.Machine)}
	core.replicatedApplyClaim = claim
	t.Run("locked-check", func(t *testing.T) {
		claim.closed = true
		defer func() { claim.closed = false }()
		assertCleared(t, claim)
	})
	t.Run("claim-mismatch", func(t *testing.T) {
		core.mu.Lock()
		owner := core.replicatedApplyClaim
		core.replicatedApplyClaim = nil
		core.mu.Unlock()
		defer func() {
			core.mu.Lock()
			core.replicatedApplyClaim = owner
			core.mu.Unlock()
		}()
		assertCleared(t, claim)
	})
	t.Run("nil-machine", func(t *testing.T) {
		core.mu.Lock()
		machine := claim.machine
		claim.machine = nil
		core.mu.Unlock()
		defer func() {
			core.mu.Lock()
			claim.machine = machine
			core.mu.Unlock()
		}()
		assertCleared(t, claim)
	})
	t.Run("activation-base", func(t *testing.T) {
		claim.activationBasePending[0] = 1
		defer func() { claim.activationBasePending = [sha256.Size]byte{} }()
		assertCleared(t, claim)
	})
}

func TestReplicatedApplyRetainsHiddenIncompleteClose(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "hidden-close")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	core := database.connector.db
	hidden := core.replicatedApplyCollection
	hiddenFile := core.replicatedApplyFile
	injected := errors.New("injected retryable hidden close")
	failed := false
	core.closeCollection = func(collection *durable.Collection) error {
		if collection == hidden && !failed {
			failed = true
			return injected
		}
		return collection.Close()
	}
	if err := core.close(); !errors.Is(err, injected) {
		t.Fatalf("first close = %v, want injected retryable error", err)
	}
	if core.closeCompleted() || core.replicatedApplyCollection != hidden ||
		core.replicatedApplyFile != hiddenFile {
		t.Fatal("retryable hidden close dropped ownership")
	}
	if _, err := hiddenFile.Stat(); err != nil {
		t.Fatalf("retained hidden descriptor: %v", err)
	}
	core.closeCollection = nil
	if err := core.close(); err != nil {
		t.Fatalf("retry hidden close: %v", err)
	}
	if !core.closeCompleted() || core.replicatedApplyCollection != nil ||
		core.replicatedApplyFile != nil {
		t.Fatal("successful hidden close retained ownership")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyConcurrentClaimRetirement(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "claim-race")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	document := []byte(`{"id":"race"}`)
	command := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
		Kind: replication.MutationPut,
		Key:  testReplicatedApplyKey(t, database, document), Value: document,
	})
	start := make(chan struct{})
	errs := make(chan error, 4)
	var workers sync.WaitGroup
	workers.Add(4)
	go func() {
		defer workers.Done()
		<-start
		_, applyErr := claim.ApplyNormal(testReplicatedApplyMeta(3), command)
		if applyErr != nil && !errors.Is(applyErr, ErrReplicatedApplyClosed) {
			errs <- applyErr
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		session, sessionErr := database.NewSession(context.Background())
		if sessionErr == nil {
			sessionErr = session.Close()
		}
		if sessionErr != nil && !errors.Is(sessionErr, ErrDatabaseClosed) {
			errs <- sessionErr
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		if closeErr := database.Close(); closeErr != nil {
			errs <- closeErr
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		if closeErr := claim.Close(); closeErr != nil {
			errs <- closeErr
		}
	}()
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent retirement: %v", err)
	}
	_ = claim.Close()
	_ = database.Close()
}

func TestReplicatedApplyProfileDigestGoldenAndBindings(t *testing.T) {
	identity := ReplicatedShardStoreIdentity{
		Binding: ReplicatedShardStoreBinding{
			Distribution: "dist", Shard: "shard", AllocationGeneration: 11,
			MemberID: 19, StoreID: [16]byte{20},
			Authority: ReplicatedAuthorityProfile{
				SchemaGeneration: 17, RoutingVersion: 12, RouteGeneration: 13,
			},
		},
		LogID: [16]byte{21}, UserTable: "docs", UserStorage: "local-storage",
		UserPrimaryKey: "/id",
		UserLimits: ReplicatedShardStoreLimits{
			MaxKeyBytes: 123, MaxDocumentBytes: 456,
			MaxBatchDocuments: 7, MaxBatchBytes: 890,
		},
		Sidecars:      canonicalReplicatedShardStoreSidecars(),
		RelationCount: 1, RelationSchemaGeneration: 17,
		Relations: make([]ReplicatedShardRelationIdentity, 1),
	}
	identity.Relations[0] = ReplicatedShardRelationIdentity{
		Relation: 1, Kind: ReplicatedShardRelationJSON,
		Table: identity.UserTable, Storage: identity.UserStorage, Limits: identity.UserLimits,
		LocalIndexDigest: sha256.Sum256([]byte("by_email:/email")),
	}
	identity.RelationManifestDigest = replicatedRelationManifestDigest(identity)
	placement := ReplicatedPlacementProfile{
		Format: ReplicatedPlacementProfileFormat, ShardKey: "/id",
		TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
		Range: distribution.KeyRange{
			Start: distribution.KeyspacePoint{0x10},
			End:   distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x90}},
		},
	}
	got := replicatedApplyProfileDigest(identity, placement)
	const wantDigest = "885cdd7ffa5c04b7d2f1d8872b1a163be260c71038ea66ecf62b7b43a8be25b7"
	if gotHex := hex.EncodeToString(got[:]); gotHex != wantDigest {
		t.Fatalf("profile digest = %s, want %s", gotHex, wantDigest)
	}

	boundMutations := []func(*ReplicatedShardStoreIdentity, *ReplicatedPlacementProfile){
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) { i.Binding.Distribution += "x" },
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) { i.Binding.Shard += "x" },
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) { i.Binding.AllocationGeneration++ },
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) {
			i.Binding.Authority.RoutingVersion++
		},
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) {
			i.Binding.Authority.RouteGeneration++
		},
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) {
			i.RelationSchemaGeneration++
		},
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) {
			i.Relations[0].LocalIndexDigest[0]++
		},
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.Format++ },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.ShardKey += "x" },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.TupleVersion++ },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.MapperVersion++ },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.Range.Start[7]++ },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.Range.End.Point[7]++ },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.Range.End.Max = true },
	}
	for index, mutate := range boundMutations {
		changedIdentity, changedPlacement := identity.Clone(), placement
		mutate(&changedIdentity, &changedPlacement)
		if digest := replicatedApplyProfileDigest(changedIdentity, changedPlacement); digest == got {
			t.Fatalf("bound mutation %d did not change digest", index)
		}
	}
	localMutations := []func(*ReplicatedShardStoreIdentity){
		func(i *ReplicatedShardStoreIdentity) { i.Binding.MemberID++ },
		func(i *ReplicatedShardStoreIdentity) { i.Binding.StoreID[0]++ },
		func(i *ReplicatedShardStoreIdentity) { i.LogID[0]++ },
		func(i *ReplicatedShardStoreIdentity) { i.UserStorage += "x" },
		func(i *ReplicatedShardStoreIdentity) { i.Relations[0].Storage += "x" },
	}
	for index, mutate := range localMutations {
		changed := identity.Clone()
		mutate(&changed)
		if digest := replicatedApplyProfileDigest(changed, placement); digest != got {
			t.Fatalf("member-local mutation %d changed digest: %x != %x", index, digest, got)
		}
	}
}

func TestReplicatedApplyIdentityJSONGolden(t *testing.T) {
	var validationDigest [32]byte
	for index := range validationDigest {
		validationDigest[index] = byte(index)
	}
	identity := ReplicatedApplyIdentity{
		Format: ReplicatedApplyFormat, Storage: "storage", CaptureStorage: "capture",
		ValidationProfile: uint8(replicatedstate.ValidationDeterministicMutation),
		ValidationDigest:  validationDigest,
		SystemLimits:      replicatedApplySystemLimits(8),
		CaptureLimits: ReplicatedShardStoreLimits{
			MaxKeyBytes: 8, MaxDocumentBytes: 4096, MaxBatchDocuments: 1, MaxBatchBytes: 4104,
		},
		MaxSessions: 5,
		RetryWindow: 8,
		TxnLimits:   durable.TxnLimits{MaxCollections: 6, MaxDocuments: 7, MaxBytes: 8},
		Placement: ReplicatedPlacementProfile{
			Format: ReplicatedPlacementProfileFormat, ShardKey: "/id",
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			Range: distribution.KeyRange{
				Start: distribution.KeyspacePoint{0x10},
				End:   distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x90}},
			},
		},
		Sidecars: canonicalReplicatedApplySidecars(),
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"format":0,"storage":"storage","capture_storage":"capture","validation_profile":2,"validation_digest":"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f","system_limits":{"max_key_bytes":35,"max_document_bytes":2048,"max_batch_documents":10,"max_batch_bytes":3108},"capture_limits":{"max_key_bytes":8,"max_document_bytes":4096,"max_batch_documents":1,"max_batch_bytes":4104},"max_sessions":5,"retry_window":8,"txn_max_collections":6,"txn_max_documents":7,"txn_max_bytes":8,"placement":{"format":0,"shard_key":"/id","tuple_version":1,"mapper_version":1,"range_start":"1000000000000000","range_end":"9000000000000000","range_end_max":false},"sidecars":{"system_recovery_journal_bytes":655872}}`
	if string(encoded) != want {
		t.Fatalf("identity JSON = %s, want %s", encoded, want)
	}
	metaEncoded, err := json.Marshal(replicatedApplyMetaFromIdentity(identity))
	if err != nil || string(metaEncoded) != want {
		t.Fatalf("catalog meta JSON = %s,%v, want %s", metaEncoded, err, want)
	}
	var decoded ReplicatedApplyIdentity
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != identity {
		t.Fatalf("identity round trip = %+v,%v", decoded, err)
	}
	maximum := identity
	maximum.RetryWindow = replicatedstate.MaxSessionRetryWindow
	maximum.SystemLimits = replicatedApplySystemLimits(maximum.RetryWindow)
	maximumEncoded, err := json.Marshal(maximum)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(maximumEncoded, &decoded); err != nil || decoded != maximum {
		t.Fatalf("maximum retry-window round trip = %+v,%v", decoded, err)
	}
	invalidMaximum := bytes.Replace(
		maximumEncoded,
		[]byte(`"max_batch_documents":258`),
		[]byte(`"max_batch_documents":257`),
		1,
	)
	if bytes.Equal(invalidMaximum, maximumEncoded) {
		t.Fatal("maximum retry-window system-limit mutation did not match")
	}
	if err := json.Unmarshal(invalidMaximum, new(ReplicatedApplyIdentity)); !errors.Is(err, ErrReplicatedApplyMismatch) {
		t.Fatalf("altered maximum retry-window system limits = %v, want mismatch", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		sidecar json.RawMessage
		remove  bool
	}{
		{name: "missing", remove: true},
		{name: "unknown", sidecar: json.RawMessage(`{"unknown":1,"system_recovery_journal_bytes":655872}`)},
		{name: "nested_missing", sidecar: json.RawMessage(`{}`)},
		{name: "mismatch", sidecar: json.RawMessage(`{"system_recovery_journal_bytes":197120}`)},
	} {
		t.Run("sidecars_"+test.name, func(t *testing.T) {
			changed := make(map[string]json.RawMessage, len(fields))
			for name, value := range fields {
				changed[name] = bytes.Clone(value)
			}
			if test.remove {
				delete(changed, "sidecars")
			} else {
				changed["sidecars"] = test.sidecar
			}
			raw, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, new(ReplicatedApplyIdentity)); err == nil {
				t.Fatalf("accepted noncanonical apply sidecars: %s", raw)
			}
		})
	}
	for name, raw := range map[string][]byte{
		"format_null": bytes.Replace(
			encoded, []byte(`{"format":0`), []byte(`{"format":null`), 1,
		),
		"placement_format_null": bytes.Replace(
			encoded, []byte(`"placement":{"format":0`),
			[]byte(`"placement":{"format":null`), 1,
		),
	} {
		t.Run(name, func(t *testing.T) {
			if bytes.Equal(raw, encoded) {
				t.Fatal("format-null mutation did not match")
			}
			if err := json.Unmarshal(raw, new(ReplicatedApplyIdentity)); err == nil {
				t.Fatalf("accepted null current-format sentinel: %s", raw)
			}
		})
	}
}

func TestReplicatedApplyCaptureParticipantCommitsAndRecoversWithCheckpointGroup(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "capture-checkpoint-reopen")
	options := testReplicatedApplyOptions()
	var err error
	options.TxnLimits.MaxBytes, err = ReplicatedApplyTransactionByteFloor(
		base, options.RetryWindow,
	)
	bootstrap := testReplicatedApplyBootstrap()
	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	partitioner := replicatedApplyCapturePartitioner(t, base)
	capture, err := claim.BeginRangeSplitCapture(partitioner)
	if err != nil || capture.Head() != 2 {
		t.Fatalf("begin capture head=%d err=%v", capture.Head(), err)
	}
	// Crash/reopen directly after the first header publication. The header is a
	// checkpoint-group-owned transition, so recovery must observe it even when
	// no captured Raft entry has followed it yet.
	if err = claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	claim, reopenedIdentity, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), options,
	)
	if err != nil || reopenedIdentity != identity {
		t.Fatalf("header-only reopen identity=%+v err=%v", reopenedIdentity, err)
	}
	capture, err = claim.BeginRangeSplitCapture(partitioner)
	if err != nil || capture.Head() != 2 {
		t.Fatalf("recovered header-only capture head=%d err=%v", capture.Head(), err)
	}
	document := []byte(`{"id":"capture","value":1}`)
	key := testReplicatedApplyKey(t, database, document)
	command := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	if _, err = claim.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil || capture.Head() != 3 {
		t.Fatalf("captured apply head=%d err=%v", capture.Head(), err)
	}
	if err = claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedClaim, reopenedIdentity, err := reopened.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), options,
	)
	if err != nil || reopenedIdentity != identity {
		t.Fatalf("reopen identity=%+v err=%v", reopenedIdentity, err)
	}
	defer reopenedClaim.Close()
	recovered, err := reopenedClaim.BeginRangeSplitCapture(partitioner)
	if err != nil || recovered.Head() != 3 {
		t.Fatalf("recovered capture head=%d err=%v", recovered.Head(), err)
	}
}

func replicatedApplyCapturePartitioner(
	t testing.TB, base ReplicatedShardStoreIdentity,
) *rangesplit.Partitioner {
	t.Helper()
	full := distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}
	manifest, err := distribution.NewManifest(
		distribution.DistributionName(base.Binding.Distribution),
		distribution.RoutingVersion(base.Binding.Authority.RoutingVersion),
		[]distribution.Shard{{
			ID:                   distribution.ShardID(base.Binding.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(base.Binding.AllocationGeneration),
			Range:                full, Leaders: []distribution.EndpointID{"source"},
			Epoch: distribution.OwnershipEpoch(base.Binding.Authority.OwnershipEpoch),
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := autosplit.PlanSplit(manifest, autosplit.SplitRequest{
		Recommendation: autosplit.Recommendation{
			Source: autosplit.SourceIdentity{
				Distribution:         distribution.DistributionName(base.Binding.Distribution),
				Shard:                distribution.ShardID(base.Binding.Shard),
				AllocationGeneration: distribution.ShardAllocationGeneration(base.Binding.AllocationGeneration),
				Range:                full, BucketBits: distribution.DefaultVirtualBucketBits,
				RoutingVersion: distribution.RoutingVersion(base.Binding.Authority.RoutingVersion),
				OwnershipEpoch: distribution.OwnershipEpoch(base.Binding.Authority.OwnershipEpoch),
			},
			Kind:       autosplit.RecommendationBinarySplit,
			Boundaries: [2]distribution.KeyspacePoint{{0x80}}, BoundaryCount: 1,
			CandidateBin: 32, BenefitPPM: 1,
		},
		RetainChild:         0,
		NextRoutingVersion:  distribution.RoutingVersion(base.Binding.Authority.RoutingVersion + 1),
		AllocationHighWater: distribution.ShardAllocationGeneration(base.Binding.AllocationGeneration),
		Destinations: []autosplit.Destination{{
			Shard:                "capture-right",
			AllocationGeneration: distribution.ShardAllocationGeneration(base.Binding.AllocationGeneration + 1),
			Leaders:              []distribution.EndpointID{"destination"},
			OwnershipEpoch:       distribution.OwnershipEpoch(base.Binding.Authority.OwnershipEpoch + 1),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitioner, err := rangesplit.NewPartitioner(
		plan, base.UserTable, []string{"/id"}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil {
		t.Fatal(err)
	}
	return partitioner
}

func TestReplicatedApplyTransactionByteFloorIsMandatoryAndExact(t *testing.T) {
	_, database, binding, local := prepareReplicatedTestRoot(
		t, "capture-transaction-floor", true,
	)
	defer database.Close()
	core := database.connector.db
	core.mu.RLock()
	table := core.tables["docs"]
	if table == nil || table.collection == nil || table.meta == nil {
		core.mu.RUnlock()
		t.Fatal("prepared base table is unavailable")
	}
	base := ReplicatedShardStoreIdentity{
		Format: ReplicatedShardStoreFormat, Binding: binding, LogID: local.LogID,
		UserTable: "docs", UserStorage: table.meta.Storage,
		UserPrimaryKey: table.meta.PrimaryKey,
		UserLimits: ReplicatedShardStoreLimits{
			MaxKeyBytes:       table.collection.MaxKeyBytes(),
			MaxDocumentBytes:  table.collection.MaxDocumentBytes(),
			MaxBatchDocuments: table.collection.MaxBatchDocuments(),
			MaxBatchBytes:     table.collection.MaxBatchBytes(),
		},
		Sidecars:      canonicalReplicatedShardStoreSidecars(),
		RelationCount: 2, RelationSchemaGeneration: binding.Authority.SchemaGeneration,
		Relations: make([]ReplicatedShardRelationIdentity, 2),
	}
	base.Relations[0] = ReplicatedShardRelationIdentity{
		Relation: 1, Kind: ReplicatedShardRelationJSON,
		Table: base.UserTable, Storage: base.UserStorage, Limits: base.UserLimits,
		LocalIndexDigest: replicatedLocalIndexDigest(table.meta.Indexes),
	}
	base.Relations[1] = ReplicatedShardRelationIdentity{
		Relation: 2, Kind: ReplicatedShardRelationGlobalIndex,
		Table: "email_index", Storage: base.UserStorage, Limits: base.UserLimits,
		IndexID: 91, Incarnation: 7, LocatorCount: 1, Unique: true,
	}
	core.mu.RUnlock()
	base.RelationManifestDigest = replicatedRelationManifestDigest(base)
	if err := validateReplicatedShardStoreIdentity(base); err != nil {
		t.Fatalf("synthetic retained identity: %v", err)
	}
	options := testReplicatedApplyOptions()
	floor, err := ReplicatedApplyTransactionByteFloor(base, options.RetryWindow)
	if err != nil {
		t.Fatal(err)
	}
	captureLimits, err := replicatedCaptureLimits(base)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(replicatedApplySystemLimits(options.RetryWindow).MaxBatchBytes) +
		int64(captureLimits.MaxBatchBytes)
	for ordinal := 0; ordinal < int(base.RelationCount); ordinal++ {
		want = min(
			int64(replication.MaxCommandBytes)+
				int64(replicatedApplySystemLimits(options.RetryWindow).MaxBatchBytes)+
				int64(captureLimits.MaxBatchBytes),
			want+int64(base.Relations[ordinal].Limits.MaxBatchBytes),
		)
	}
	if floor != want || floor > 64<<20 {
		t.Fatalf("transaction floor=%d want=%d and at most 64 MiB", floor, want)
	}
	exact := options
	exact.TxnLimits.MaxBytes = floor
	if err := validateReplicatedApplyOptions(base, exact); err != nil {
		t.Fatalf("exact transaction floor rejected: %v", err)
	}
	oneShort := exact
	oneShort.TxnLimits.MaxBytes--
	if err := validateReplicatedApplyOptions(base, oneShort); !errors.Is(
		err, ErrReplicatedApplyMismatch,
	) {
		t.Fatalf("one-byte-short transaction floor = %v", err)
	}
}

func TestReplicatedApplyPlacementOptionsFailClosed(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "placement-options")
	defer database.Close()
	valid := testReplicatedApplyOptions()
	if err := validateReplicatedApplyOptions(base, valid); err != nil {
		t.Fatalf("valid placement options: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*ReplicatedPlacementProfile)
	}{
		{"zero", func(p *ReplicatedPlacementProfile) { *p = ReplicatedPlacementProfile{} }},
		{"format", func(p *ReplicatedPlacementProfile) { p.Format++ }},
		{"shard key", func(p *ReplicatedPlacementProfile) { p.ShardKey += "x" }},
		{"tuple version", func(p *ReplicatedPlacementProfile) { p.TupleVersion++ }},
		{"mapper version", func(p *ReplicatedPlacementProfile) { p.MapperVersion++ }},
		{"empty range", func(p *ReplicatedPlacementProfile) {
			p.Range.End.Max = false
			p.Range.End.Point = p.Range.Start
		}},
		{"noncanonical max", func(p *ReplicatedPlacementProfile) { p.Range.End.Point[7] = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.mutate(&options.Placement)
			if err := validateReplicatedApplyOptions(base, options); !errors.Is(err, ErrReplicatedApplyMismatch) {
				t.Fatalf("placement options error = %v", err)
			}
		})
	}
}
