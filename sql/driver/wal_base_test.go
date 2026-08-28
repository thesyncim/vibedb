package driver

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func newWALBaseTestClaim(
	t testing.TB,
	name string,
) (string, *Database, *ReplicatedApply, ReplicatedShardStoreIdentity) {
	return newWALBaseTestClaimWithBinding(t, name, testReplicatedBinding(31))
}

func newWALBaseTestClaimWithBinding(
	t testing.TB,
	name string,
	binding ReplicatedShardStoreBinding,
) (string, *Database, *ReplicatedApply, ReplicatedShardStoreIdentity) {
	t.Helper()
	path, database, _, _ := prepareReplicatedTestRoot(t, name, false)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close WAL-base database: %v", err)
		}
	})
	identity := requireReplicatedShardStoreBind(t, database, binding, "docs")
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(
		identity,
		bootstrap,
		testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatalf("OpenReplicatedApply: %v", err)
	}
	t.Cleanup(func() {
		if err := claim.Close(); err != nil {
			t.Errorf("close WAL-base claim: %v", err)
		}
	})
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	return path, database, claim, identity
}

func walBaseMachineState(t testing.TB, claim *ReplicatedApply) replicatedstate.State {
	t.Helper()
	cut, err := claim.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	state := cut.State()
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	return state
}

func walBaseWorkspace() []byte {
	return make([]byte, 0, replicatedstate.MaxSnapshotArtifactChunkBytes)
}

func requiredWALBaseWorkspaceBytes(
	t testing.TB,
	claim *ReplicatedApply,
	base ReplicatedShardStoreIdentity,
) int {
	t.Helper()
	systemBytes, err := replicatedstate.RequiredSnapshotArtifactSystemPayloadCapacity(
		replicatedstate.DefaultSnapshotArtifactChunkBytes,
		claim.identity.SystemLimits.MaxKeyBytes,
		claim.identity.SystemLimits.MaxDocumentBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	userBytes, err := replicatedstate.RequiredSnapshotArtifactPayloadCapacity(
		replicatedstate.DefaultSnapshotArtifactChunkBytes,
		base.UserLimits.MaxKeyBytes,
		base.UserLimits.MaxDocumentBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	return max(systemBytes, userBytes)
}

func TestWALBaseBundleManifestBindsReplicatedStateDigestDomain(t *testing.T) {
	base := ReplicatedShardStoreIdentity{
		UserTable: "docs", RelationCount: 2,
		RelationSchemaGeneration: 19,
		Relations: []ReplicatedShardRelationIdentity{
			{Relation: 1, Kind: ReplicatedShardRelationJSON, Table: "docs"},
			{Relation: 2, Kind: ReplicatedShardRelationGlobalIndex,
				Table: "email_claims", IndexID: 41, Incarnation: 7,
				LocatorCount: 1, Unique: true,
				KeyEncoding: ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
				TupleVersion:  distribution.CurrentTupleVersion,
				MapperVersion: distribution.NativeMapperVersion,
				BucketBits:    distribution.DefaultVirtualBucketBits},
		},
	}
	machineDigest := sha256.Sum256([]byte(
		"replicated-state-machine relation manifest fixture",
	))
	manifest := replicatedstate.SnapshotArtifactManifest{
		UserCollection:         []byte("docs"),
		Bundle:                 true,
		RelationManifestDigest: machineDigest,
		Relations: []replicatedstate.SnapshotArtifactRelation{
			{Relation: 1, Kind: replicatedstate.RelationJSON, Collection: []byte("docs")},
			{Relation: 2, Kind: replicatedstate.RelationGlobalIndex,
				Collection: []byte("email_claims")},
		},
	}
	applyProfileDigest := replicatedRelationApplyManifestDigest(base)
	if applyProfileDigest == machineDigest {
		t.Fatal("test fixture did not separate replicated-state and SQL apply digest domains")
	}
	if !equalWALBaseManifestShape(manifest, base) ||
		!equalWALBaseManifest(manifest, base, machineDigest) {
		t.Fatal("exact replicated-state bundle manifest was rejected")
	}
	if equalWALBaseManifest(manifest, base, applyProfileDigest) {
		t.Fatal("SQL apply-profile digest was accepted as a replicated-state manifest digest")
	}
	forged := machineDigest
	forged[0] ^= 1
	if forged == ([sha256.Size]byte{}) || equalWALBaseManifest(manifest, base, forged) {
		t.Fatal("forged nonzero relation-manifest digest was accepted")
	}
}

func walBaseDataFiles(t testing.TB, database *Database) []string {
	t.Helper()
	core := database.connector.db
	core.mu.RLock()
	root := core.dataDir
	core.mu.RUnlock()
	var paths []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		paths = append(paths, relative)
		return nil
	}); err != nil {
		t.Fatalf("walk replicated data files: %v", err)
	}
	slices.Sort(paths)
	return paths
}

func TestReplicatedApplyCaptureWALBaseSealsOneScanWithoutArtifact(t *testing.T) {
	_, database, claim, _ := newWALBaseTestClaim(t, "capture")
	core := database.connector.db
	core.mu.RLock()
	group := core.checkpointGroup
	members := []durable.NamedCollection{
		{Name: replicatedstate.SystemCollectionName, Collection: core.replicatedApplyCollection},
		{Name: "docs", Collection: core.tables["docs"].collection},
		{Name: replicatedstate.TransitionCaptureCollectionName, Collection: core.replicatedCaptureCollection},
	}
	core.mu.RUnlock()
	if group == nil || !group.Owns(members) {
		t.Fatal("WAL-base capture checkpoint membership mismatch")
	}
	participants := uint64(len(members))
	beforeStats := group.Stats()
	beforeFiles := walBaseDataFiles(t, database)
	beforeCut, err := claim.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	beforeState := beforeCut.State()
	if err := beforeCut.Close(); err != nil {
		t.Fatal(err)
	}

	var scans atomic.Uint32
	previousHook := captureWALBaseScanHook
	captureWALBaseScanHook = func() { scans.Add(1) }
	t.Cleanup(func() { captureWALBaseScanHook = previousHook })
	preparation, err := claim.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: walBaseWorkspace(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if scans.Load() != 1 || preparation.scanPasses != 1 {
		t.Fatalf("capture scans = %d/%d", scans.Load(), preparation.scanPasses)
	}
	if err := claim.ValidateWALBasePreparation(preparation); err != nil {
		t.Fatalf("ValidateWALBasePreparation: %v", err)
	}
	witness := preparation.retention
	if witness.IsZero() || !witness.BindsAppliedIndex(1) {
		t.Fatalf("retention witness = %+v", witness)
	}
	base, err := preparation.SnapshotBase()
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := replicatedstate.OpenSnapshotBase(base)
	if err != nil {
		t.Fatal(err)
	}
	if certificate.Manifest.State.Applied != 1 ||
		certificate.Manifest.State.SnapshotBaseDigest != beforeState.SnapshotBaseDigest ||
		certificate.Manifest.Digest != preparation.artifactDigest ||
		certificate.Manifest.ImageDigest != preparation.imageDigest ||
		certificate.Digest != preparation.snapshotBaseDigest ||
		certificate.Manifest.EncodedBytes != preparation.encodedBytes {
		t.Fatalf("captured base certificate = %+v", certificate)
	}
	afterCut, err := claim.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	afterState := afterCut.State()
	if err := afterCut.Close(); err != nil {
		t.Fatal(err)
	}
	if afterState.Applied != beforeState.Applied ||
		afterState.SnapshotBaseDigest != beforeState.SnapshotBaseDigest {
		t.Fatalf("capture activated snapshot base: before=%+v after=%+v", beforeState, afterState)
	}

	afterStats := group.Stats()
	if afterStats.CheckpointAppliedIndex != 1 ||
		afterStats.CertificateSyncs-beforeStats.CertificateSyncs != participants ||
		afterStats.Checkpoints-beforeStats.Checkpoints != 1 ||
		afterStats.BarrierSyncs-beforeStats.BarrierSyncs != participants+1 ||
		afterStats.PhysicalCheckpoints-beforeStats.PhysicalCheckpoints != participants {
		t.Fatalf("capture seal stats: before=%+v after=%+v", beforeStats, afterStats)
	}
	if afterFiles := walBaseDataFiles(t, database); !slices.Equal(afterFiles, beforeFiles) {
		t.Fatalf("capture created or removed a local artifact: before=%v after=%v", beforeFiles, afterFiles)
	}

	base.Data[0] ^= 0xff
	detached, err := preparation.SnapshotBase()
	if err != nil || !proto.Equal(detached, preparation.snapshotBase) {
		t.Fatalf("SnapshotBase did not return a detached copy: %v", err)
	}
	repeated, err := claim.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: walBaseWorkspace(),
	})
	if err != nil {
		t.Fatal(err)
	}
	repeatedBase, err := repeated.SnapshotBase()
	if err != nil {
		t.Fatal(err)
	}
	repeatedWitness := repeated.retention
	if scans.Load() != 2 || repeated.scanPasses != 1 || repeatedWitness != witness ||
		!proto.Equal(repeatedBase, preparation.snapshotBase) {
		t.Fatalf("idempotent capture changed identity or scan count")
	}
	if got := group.Stats(); got != afterStats {
		t.Fatalf("idempotent seal changed group stats: got %+v want %+v", got, afterStats)
	}
}

func TestReplicatedApplySettlesExactWALGenerationBeforeReclaim(t *testing.T) {
	_, _, claim, _ := newWALBaseTestClaim(t, "settle-generation")
	preparation, err := claim.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: walBaseWorkspace(),
	})
	if err != nil {
		t.Fatal(err)
	}
	input, err := preparation.GenerationInput()
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := replicatedstate.OpenSnapshotBase(input.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if input.SnapshotBaseDigest != certificate.Digest {
		t.Fatalf("generation input snapshot-base digest = %x, want %x",
			input.SnapshotBaseDigest, certificate.Digest)
	}
	before, err := claim.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	beforeState := before.State()
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}
	activation := raftstore.GenerationActivation{
		Snapshot: input.Snapshot,
		Info: raftstore.GenerationInfo{
			FamilyID: [16]byte{1}, Generation: 1, BindingDigest: [32]byte{1},
			SnapshotBaseDigest:  certificate.Digest,
			RetentionCommitment: input.RetentionCommitment,
			BaseIndex:           input.Snapshot.GetMetadata().GetIndex(),
			BaseTerm:            input.Snapshot.GetMetadata().GetTerm(),
			HardCommit:          input.Snapshot.GetMetadata().GetIndex(),
		},
	}
	beforeSettlement, err := claim.DurabilityStats()
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.SettleGenerationActivation(activation); !errors.Is(
		err, ErrReplicatedApplyBusy,
	) {
		t.Fatalf("unlatched generation settlement = %v", err)
	}
	afterUnlatched, err := claim.DurabilityStats()
	if err != nil {
		t.Fatal(err)
	}
	if afterUnlatched != beforeSettlement {
		t.Fatalf("unlatched settlement mutated durability: before=%+v after=%+v",
			beforeSettlement, afterUnlatched)
	}
	if err := claim.LatchGenerationActivation(activation); err != nil {
		t.Fatalf("latch generation: %v", err)
	}
	foreignIdentities := []struct {
		name   string
		mutate func(*raftstore.GenerationActivation)
	}{
		{name: "family", mutate: func(value *raftstore.GenerationActivation) {
			value.Info.FamilyID[0] ^= 0x80
		}},
		{name: "generation", mutate: func(value *raftstore.GenerationActivation) {
			value.Info.Generation++
		}},
		{name: "binding", mutate: func(value *raftstore.GenerationActivation) {
			value.Info.BindingDigest[0] ^= 0x80
		}},
		{name: "retention", mutate: func(value *raftstore.GenerationActivation) {
			value.Info.RetentionCommitment[0] ^= 0x80
		}},
	}
	for _, test := range foreignIdentities {
		t.Run("reject_pending_"+test.name, func(t *testing.T) {
			foreign := activation
			test.mutate(&foreign)
			if err := claim.LatchGenerationActivation(foreign); !errors.Is(
				err, ErrReplicatedApplyMismatch,
			) {
				t.Fatalf("foreign latch = %v", err)
			}
			if err := claim.SettleGenerationActivation(foreign); !errors.Is(
				err, ErrReplicatedApplyMismatch,
			) {
				t.Fatalf("foreign settlement = %v", err)
			}
		})
	}
	if unsettled, err := claim.SnapshotArtifactCut(); unsettled != nil || !errors.Is(
		err, ErrReplicatedApplyBusy,
	) {
		t.Fatalf("public snapshot crossed selected-generation fence = %v, %v", unsettled, err)
	}
	unsettledState := walBaseMachineState(t, claim)
	if unsettledState.SnapshotBaseDigest != beforeState.SnapshotBaseDigest {
		t.Fatalf("rejected settlement changed base: before=%x after=%x",
			beforeState.SnapshotBaseDigest, unsettledState.SnapshotBaseDigest)
	}
	if err := claim.SettleGenerationActivation(activation); err != nil {
		t.Fatalf("settle generation: %v", err)
	}
	if settled, err := claim.SnapshotArtifactCut(); settled != nil || !errors.Is(
		err, ErrReplicatedApplyBusy,
	) {
		t.Fatalf("settled base escaped before WAL completion = %v, %v", settled, err)
	}
	settledState := walBaseMachineState(t, claim)
	if settledState.SnapshotBaseDigest != certificate.Digest ||
		settledState.Applied != certificate.Manifest.State.Applied {
		t.Fatalf("settled state = %+v, certificate = %+v", settledState, certificate)
	}
	if err := claim.SettleGenerationActivation(activation); err != nil {
		t.Fatalf("idempotent settlement: %v", err)
	}
}

func TestPendingWALGenerationCloseRetiresDatabaseBeforeClaimRelease(t *testing.T) {
	_, database, claim, identity := newWALBaseTestClaim(t, "pending-close")
	preparation, err := claim.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: walBaseWorkspace(),
	})
	if err != nil {
		t.Fatal(err)
	}
	input, err := preparation.GenerationInput()
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := replicatedstate.OpenSnapshotBase(input.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	activation := raftstore.GenerationActivation{
		Snapshot: input.Snapshot,
		Info: raftstore.GenerationInfo{
			FamilyID:            [16]byte{1},
			Generation:          1,
			BindingDigest:       [32]byte{1},
			SnapshotBaseDigest:  certificate.Digest,
			RetentionCommitment: input.RetentionCommitment,
			BaseIndex:           input.Snapshot.GetMetadata().GetIndex(),
			BaseTerm:            input.Snapshot.GetMetadata().GetTerm(),
			HardCommit:          input.Snapshot.GetMetadata().GetIndex(),
		},
	}
	if err := claim.LatchGenerationActivation(activation); err != nil {
		t.Fatalf("latch generation: %v", err)
	}
	if err := claim.Close(); err != nil {
		t.Fatalf("close pending claim: %v", err)
	}
	if !database.connector.closed {
		t.Fatal("pending claim release did not retire its connector")
	}
	if replacement, _, err := database.OpenReplicatedApply(
		identity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	); replacement != nil || !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("same-root replacement claim = %v, %v", replacement, err)
	}
	if session, err := database.NewSession(t.Context()); session != nil ||
		!errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("retired database session = %v, %v", session, err)
	}
}

func TestGenerationSettlementRejectsWrongBindingBeforeCheckpointMutation(t *testing.T) {
	_, _, source, _ := newWALBaseTestClaim(t, "settle-binding-source")
	_, _, target, _ := newWALBaseTestClaimWithBinding(
		t, "settle-binding-target", testReplicatedBinding(32),
	)
	preparation, err := source.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: walBaseWorkspace(),
	})
	if err != nil {
		t.Fatal(err)
	}
	input, err := preparation.GenerationInput()
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := replicatedstate.OpenSnapshotBase(input.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	activation := raftstore.GenerationActivation{
		Snapshot: input.Snapshot,
		Info: raftstore.GenerationInfo{
			FamilyID: [16]byte{1}, Generation: 1, BindingDigest: [32]byte{1},
			SnapshotBaseDigest:  certificate.Digest,
			RetentionCommitment: input.RetentionCommitment,
			BaseIndex:           input.Snapshot.GetMetadata().GetIndex(),
			BaseTerm:            input.Snapshot.GetMetadata().GetTerm(),
			HardCommit:          input.Snapshot.GetMetadata().GetIndex(),
		},
	}
	before, err := target.DurabilityStats()
	if err != nil {
		t.Fatal(err)
	}
	if err := target.SettleGenerationActivation(activation); !errors.Is(
		err, ErrReplicatedApplyMismatch,
	) {
		t.Fatalf("wrong-binding settlement = %v", err)
	}
	after, err := target.DurabilityStats()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("wrong-binding settlement mutated durability: before=%+v after=%+v", before, after)
	}
}

func TestReplicatedApplyCaptureWALBaseRejectsWorkspaceBeforeSeal(t *testing.T) {
	_, database, claim, base := newWALBaseTestClaim(t, "workspace")
	core := database.connector.db
	core.mu.RLock()
	group := core.checkpointGroup
	core.mu.RUnlock()
	before := group.Stats()
	required := requiredWALBaseWorkspaceBytes(t, claim, base)
	if required != replicatedstate.DefaultSnapshotArtifactChunkBytes {
		t.Fatalf("fixture workspace requirement = %d", required)
	}
	preparation, err := claim.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: make([]byte, 0, required-1),
	})
	if preparation != nil || !errors.Is(err, replicatedstate.ErrSnapshotArtifactBound) {
		t.Fatalf("undersized workspace = %+v, %v", preparation, err)
	}
	if after := group.Stats(); after != before {
		t.Fatalf("invalid workspace sealed a cut: before=%+v after=%+v", before, after)
	}
	if claim.walBaseCaptureActive {
		t.Fatal("invalid workspace retained capture ownership")
	}
}

func TestReplicatedApplyCaptureWALBaseMaxLegalRowHasNoPayloadGrowth(t *testing.T) {
	_, database, claim, base := newWALBaseTestClaim(t, "max-row")
	var key []byte
	var id []byte
	core := database.connector.db
	core.mu.RLock()
	table := core.tables[base.UserTable]
	for idBytes := 1; idBytes <= base.UserLimits.MaxKeyBytes; idBytes++ {
		candidateID := bytes.Repeat([]byte{'k'}, idBytes)
		candidateDocument := make([]byte, 0, idBytes+9)
		candidateDocument = append(candidateDocument, `{"id":"`...)
		candidateDocument = append(candidateDocument, candidateID...)
		candidateDocument = append(candidateDocument, `"}`...)
		candidateKey, err := documentKey(
			candidateDocument,
			table.meta.PrimaryKey,
			table.primary,
			base.UserLimits.MaxKeyBytes,
		)
		if err == nil && len(candidateKey) == base.UserLimits.MaxKeyBytes {
			id = candidateID
			key = []byte(candidateKey)
			break
		}
	}
	core.mu.RUnlock()
	if len(key) != base.UserLimits.MaxKeyBytes {
		t.Fatalf("could not construct exact maximum key: got %d", len(key))
	}
	prefix := make([]byte, 0, len(id)+20)
	prefix = append(prefix, `{"id":"`...)
	prefix = append(prefix, id...)
	prefix = append(prefix, `","pad":"`...)
	const suffix = `"}`
	if len(prefix)+len(suffix) > base.UserLimits.MaxDocumentBytes {
		t.Fatal("maximum-row document prefix exceeds the frozen document bound")
	}
	document := make([]byte, base.UserLimits.MaxDocumentBytes)
	cursor := copy(document, prefix)
	for index := cursor; index < len(document)-len(suffix); index++ {
		document[index] = 'x'
	}
	copy(document[len(document)-len(suffix):], suffix)
	if len(document) != base.UserLimits.MaxDocumentBytes {
		t.Fatalf("maximum document bytes = %d", len(document))
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	command := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{
		Kind:  replication.MutationPut,
		Key:   key,
		Value: document,
	})
	publication, err := claim.ApplyNormal(testReplicatedApplyMeta(3), command)
	if err != nil || publication.Applied != 3 {
		t.Fatalf("apply maximum row = %+v, %v", publication, err)
	}

	required := requiredWALBaseWorkspaceBytes(t, claim, base)
	if len(key) != replication.MaxMutationKeyBytes ||
		len(document) != replication.MaxMutationValueBytes ||
		required != replicatedstate.DefaultSnapshotArtifactChunkBytes {
		t.Fatalf(
			"exact maximum-row workspace = %d, key/document = %d/%d",
			required,
			len(key),
			len(document),
		)
	}
	workspace := make([]byte, 0, required)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	preparation, err := claim.CaptureWALBase(WALBaseCaptureOptions{Workspace: workspace})
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	coldAllocated := after.TotalAlloc - before.TotalAlloc
	coldAllocations := after.Mallocs - before.Mallocs
	if coldAllocated > uint64(16*required) {
		t.Fatalf(
			"maximum-row seal+capture transient allocation = %d bytes, bound %d",
			coldAllocated,
			16*required,
		)
	}
	if preparation.scanPasses != 1 || preparation.encodedBytes <= uint64(required) ||
		cap(workspace) != required {
		t.Fatalf(
			"maximum-row capture bounds = scans %d encoded %d workspace %d",
			preparation.scanPasses,
			preparation.encodedBytes,
			cap(workspace),
		)
	}
	runtime.GC()
	runtime.ReadMemStats(&before)
	warm, err := claim.CaptureWALBase(WALBaseCaptureOptions{Workspace: workspace})
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	warmAllocated := after.TotalAlloc - before.TotalAlloc
	warmAllocations := after.Mallocs - before.Mallocs
	if warmAllocated > uint64(6*required) {
		t.Fatalf(
			"maximum-row idempotent capture transient allocation = %d bytes, bound %d",
			warmAllocated,
			6*required,
		)
	}
	if coldAllocations > 1024 || warmAllocations > 512 {
		t.Fatalf(
			"maximum-row allocation counts = cold %d warm %d",
			coldAllocations,
			warmAllocations,
		)
	}
	if warm.scanPasses != 1 || warm.encodedBytes != preparation.encodedBytes {
		t.Fatalf("maximum-row warm capture = %+v", warm)
	}
	t.Logf(
		"maximum-row capture: workspace_peak=%d seal_capture=%dB/%dalloc idempotent_capture=%dB/%dalloc encoded=%d",
		required,
		coldAllocated,
		coldAllocations,
		warmAllocated,
		warmAllocations,
		preparation.encodedBytes,
	)
}

func TestReplicatedApplyWALBaseWitnessSurvivesPeriodicSlotRewrite(t *testing.T) {
	_, database, claim, _ := newWALBaseTestClaim(t, "stale")
	preparation, err := claim.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: walBaseWorkspace(),
	})
	if err != nil {
		t.Fatal(err)
	}
	publication, err := claim.ApplyNormal(
		raftmodel.ApplyMeta{Index: 2, Term: 2, Type: pb.EntryNormal},
		nil,
	)
	if err != nil || publication.Applied != 2 {
		t.Fatalf("concurrent suffix = %+v, %v", publication, err)
	}
	if err := claim.ValidateWALBasePreparation(preparation); err != nil {
		t.Fatalf("uncertified suffix invalidated preparation: %v", err)
	}

	core := database.connector.db
	core.mu.Lock()
	err = core.checkpointGroup.Checkpoint()
	core.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	err = claim.ValidateWALBasePreparation(preparation)
	if err != nil {
		t.Fatalf("monotonic rewritten-slot validation = %v", err)
	}
	next, err := claim.CaptureWALBase(WALBaseCaptureOptions{Workspace: walBaseWorkspace()})
	if err != nil {
		t.Fatal(err)
	}
	nextBase, err := next.SnapshotBase()
	if err != nil || nextBase.GetMetadata().GetIndex() != 2 {
		t.Fatalf("next base = %+v, %v", nextBase, err)
	}
	if err := claim.ValidateWALBasePreparation(preparation); !errors.Is(err, ErrWALBasePreparation) {
		t.Fatalf("higher sealed floor retained old preparation: %v", err)
	}
}

func TestReplicatedApplyCaptureWALBaseProgressesAcrossCheckpointChurn(t *testing.T) {
	_, database, claim, _ := newWALBaseTestClaim(t, "concurrent")
	reached := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCapture := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseCapture)
	var scans atomic.Uint32
	previousHook := captureWALBaseScanHook
	captureWALBaseScanHook = func() {
		if scans.Add(1) == 1 {
			close(reached)
			<-release
		}
	}
	t.Cleanup(func() { captureWALBaseScanHook = previousHook })

	type captureResult struct {
		preparation *WALBasePreparation
		err         error
	}
	completed := make(chan captureResult, 1)
	go func() {
		preparation, err := claim.CaptureWALBase(WALBaseCaptureOptions{
			Workspace: walBaseWorkspace(),
		})
		completed <- captureResult{preparation: preparation, err: err}
	}()
	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		t.Fatal("capture did not release the publication lock before scanning")
	}
	core := database.connector.db
	core.mu.RLock()
	group := core.checkpointGroup
	core.mu.RUnlock()
	checkpointStart := group.Stats()

	applied := make(chan error, 1)
	go func() {
		for index := uint64(2); index <= 5; index++ {
			publication, err := claim.ApplyNormal(
				raftmodel.ApplyMeta{Index: index, Term: 2, Type: pb.EntryNormal},
				nil,
			)
			if err != nil {
				applied <- err
				return
			}
			if publication.Applied != index {
				applied <- errors.New("concurrent apply returned the wrong publication")
				return
			}
			core.mu.Lock()
			err = core.checkpointGroup.Checkpoint()
			core.mu.Unlock()
			if err != nil {
				applied <- err
				return
			}
		}
		applied <- nil
	}()
	select {
	case err := <-applied:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent apply remained behind the capture scan")
	}
	checkpointEnd := group.Stats()
	if checkpointEnd.Checkpoints-checkpointStart.Checkpoints != 4 ||
		checkpointEnd.CertificateSyncs-checkpointStart.CertificateSyncs != 4 {
		t.Fatalf(
			"concurrent apply did not cross multiple checkpoint rewrites: before=%+v after=%+v",
			checkpointStart,
			checkpointEnd,
		)
	}
	if another, err := claim.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: walBaseWorkspace(),
	}); another != nil || !errors.Is(err, ErrReplicatedApplyBusy) {
		t.Fatalf("second capture = %+v, %v", another, err)
	}
	if err := claim.Close(); !errors.Is(err, ErrReplicatedApplyBusy) {
		t.Fatalf("Close during capture = %v", err)
	}
	if err := claim.ClaimRuntimeOwnership(database); !errors.Is(err, ErrReplicatedApplyBusy) {
		t.Fatalf("ClaimRuntimeOwnership during capture = %v", err)
	}
	releaseCapture()
	result := <-completed
	if result.err != nil || result.preparation == nil {
		t.Fatalf("capture = %+v, %v", result.preparation, result.err)
	}
	base, err := result.preparation.SnapshotBase()
	if err != nil || base.GetMetadata().GetIndex() != 1 || claim.Applied() != 5 {
		t.Fatalf("pinned base/current applied = %+v/%d, %v", base, claim.Applied(), err)
	}
	if err := claim.ValidateWALBasePreparation(result.preparation); err != nil {
		t.Fatalf("validate pinned preparation after suffix: %v", err)
	}
	if scans.Load() != 1 || claim.walBaseCaptureActive {
		t.Fatalf("capture cleanup = scans %d active %t", scans.Load(), claim.walBaseCaptureActive)
	}
	if err := claim.ClaimRuntimeOwnership(database); err != nil {
		t.Fatalf("retry ClaimRuntimeOwnership after capture: %v", err)
	}
	if err := claim.Close(); err != nil {
		t.Fatalf("retry Close after capture: %v", err)
	}
}

func TestWALBasePreparationIsBoundedAndOwnerExact(t *testing.T) {
	_, _, first, _ := newWALBaseTestClaim(t, "owner-first")
	_, _, second, _ := newWALBaseTestClaim(t, "owner-second")
	preparation, err := first.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: walBaseWorkspace(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.ValidateWALBasePreparation(preparation); !errors.Is(err, ErrWALBasePreparation) {
		t.Fatalf("foreign preparation validation = %v", err)
	}
	publication, err := second.ApplyNormal(
		testReplicatedApplyMeta(2),
		nil,
	)
	if err != nil || publication.Applied != 2 {
		t.Fatalf("foreign suffix = %+v, %v", publication, err)
	}
	foreign, err := second.CaptureWALBase(WALBaseCaptureOptions{
		Workspace: walBaseWorkspace(),
	})
	if err != nil {
		t.Fatal(err)
	}
	spliced := *preparation
	spliced.retention = foreign.retention
	if err := first.ValidateWALBasePreparation(&spliced); !errors.Is(err, ErrWALBasePreparation) {
		t.Fatalf("spliced retention validation = %v", err)
	}
	spliced = *preparation
	spliced.snapshotBase = foreign.snapshotBase
	spliced.snapshotBaseDigest = foreign.snapshotBaseDigest
	spliced.artifactDigest = foreign.artifactDigest
	spliced.imageDigest = foreign.imageDigest
	spliced.encodedBytes = foreign.encodedBytes
	if err := first.ValidateWALBasePreparation(&spliced); !errors.Is(err, ErrWALBasePreparation) {
		t.Fatalf("spliced base validation = %v", err)
	}
	if size := unsafe.Sizeof(*preparation); size > 384 {
		t.Fatalf("preparation grew beyond its constant-size envelope: %d", size)
	}
	if len(preparation.snapshotBase.GetData()) > replicatedstate.MaxSnapshotBaseCertificateBytes ||
		preparation.encodedBytes == 0 || preparation.scanPasses != 1 {
		t.Fatalf("bounded preparation = %+v", preparation)
	}
	corrupt := preparation.snapshotBase.GetData()
	corrupt[len(corrupt)/2] ^= 0xff
	if err := first.ValidateWALBasePreparation(preparation); !errors.Is(err, ErrWALBasePreparation) {
		t.Fatalf("corrupt preparation validation = %v", err)
	}
}

func BenchmarkReplicatedApplyCaptureWALBase(b *testing.B) {
	_, _, claim, _ := newWALBaseTestClaim(b, "benchmark")
	workspace := walBaseWorkspace()
	preparation, err := claim.CaptureWALBase(WALBaseCaptureOptions{Workspace: workspace})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(preparation.encodedBytes))
	b.ReportMetric(1, "scan/op")
	b.ResetTimer()
	for b.Loop() {
		preparation, err = claim.CaptureWALBase(WALBaseCaptureOptions{Workspace: workspace})
		if err != nil {
			b.Fatal(err)
		}
	}
}
