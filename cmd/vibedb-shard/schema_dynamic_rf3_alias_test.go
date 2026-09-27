package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	driverquery "github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

type aliasDynamicSchemaOwner struct {
	*dynamicSchemaOwner
	peerCommand []byte
	proposals   int
	installed   bool
}

func (o *aliasDynamicSchemaOwner) ObserveSchemaTransition(_ context.Context, _ raftmember.GroupKey, raw []byte) (bool, error) {
	return bytes.Equal(o.peerCommand, raw), nil
}

func (o *aliasDynamicSchemaOwner) Probe(ctx context.Context, group raftmember.GroupKey) (raftservice.ServingState, error) {
	if len(o.peerCommand) != 0 && !o.installed {
		return raftservice.ServingState{}, replicatedstate.ErrSchemaTransitionPending
	}
	return o.dynamicSchemaOwner.Probe(ctx, group)
}

func (o *aliasDynamicSchemaOwner) ProposeSchemaTransition(context.Context, raftservice.ServingFence, []byte) error {
	o.proposals++
	return replicatedstate.ErrSchemaTransitionPending
}

func (o *aliasDynamicSchemaOwner) FenceCommittedSchemaGeneration(_ context.Context, _ raftmember.GroupKey, _ []byte) error {
	return nil
}

func (o *aliasDynamicSchemaOwner) InstallSchemaGeneration(
	ctx context.Context, group raftmember.GroupKey, db *sqldriver.Database, apply *sqldriver.ReplicatedApply,
	base sqldriver.ReplicatedShardStoreIdentity, applyID sqldriver.ReplicatedApplyIdentity,
) error {
	if err := o.dynamicSchemaOwner.InstallSchemaGeneration(ctx, group, db, apply, base, applyID); err != nil {
		return err
	}
	o.installed = true
	return nil
}

func seedRF3CASAliasRow(t *testing.T, identity sqldriver.ReplicatedShardStoreIdentity, owner *dynamicSchemaOwner) {
	t.Helper()
	binding := identity.Binding
	client := replication.ID128{9}
	commandBase := replication.Command{
		ClusterID: replication.ID128(binding.ClusterID), ClusterIncarnation: replication.ID128(binding.ClusterIncarnation),
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch, Distribution: binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration, ShardIncarnation: replication.ID128(binding.ShardIncarnation),
		GroupID: replication.ID128(binding.GroupID), ReplicaSetVersion: 1,
		ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration, ProtectionEpoch: binding.Authority.ProtectionEpoch,
		OwnershipEpoch: binding.Authority.OwnershipEpoch, SchemaGeneration: binding.Authority.SchemaGeneration,
		RoutingVersion: binding.Authority.RoutingVersion, RouteGeneration: binding.Authority.RouteGeneration,
		Tenant: []byte("tenant"), ClientID: client,
	}
	open := commandBase
	open.Kind = replication.CommandSessionOpen
	open.ClientSequence = 1
	open.Fingerprint = replication.Digest(sha256.Sum256([]byte("schema-alias/session-open")))
	open.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	openRaw, err := replication.AppendCommand(nil, open)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.apply.AdmitCommand(openRaw); err != nil {
		t.Fatal(err)
	}
	indexOpen := uint64(2)
	term, kind := uint64(2), pb.EntryNormal
	if err := owner.log.Persist(raftmodel.PersistBatch{NodeIncarnation: owner.identity.NodeIncarnation,
		ReadyID: 1, MustSync: true, HardState: &pb.HardState{Term: &term, Commit: &indexOpen},
		Entries: []*pb.Entry{{Index: &indexOpen, Term: &term, Type: &kind, Data: openRaw}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.apply.ApplyNormal(raftmodel.ApplyMeta{Index: indexOpen, Term: term, Type: kind}, openRaw); err != nil {
		t.Fatal(err)
	}
	openLookup, err := owner.apply.LookupCompletion(openRaw)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := replication.OpenCompletion(openLookup.Bytes)
	if err != nil || opened.ResultCode != replicatedstate.ResultSessionOpened {
		t.Fatalf("seed session-open completion: %+v %v", opened, err)
	}
	document := []byte(`{"id":"cas-seed","value":"preserved"}`)
	key, ok := orderedkey.AppendJSONString(nil, []byte(`"cas-seed"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode seed key")
	}
	put := commandBase
	put.ClientEpoch, put.ClientSequence, put.AckThrough = opened.ClientEpoch, 2, 1
	put.Fingerprint = replication.Digest(sha256.Sum256([]byte("schema-alias/seed")))
	put.Batches = []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{
		Kind: replication.MutationPut, Key: key, Value: document,
	}}}}
	putRaw, err := replication.AppendCommand(nil, put)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.apply.AdmitCommand(putRaw); err != nil {
		t.Fatal(err)
	}
	indexPut := indexOpen + 1
	if err := owner.log.Persist(raftmodel.PersistBatch{NodeIncarnation: owner.identity.NodeIncarnation,
		ReadyID: 2, MustSync: true, HardState: &pb.HardState{Term: &term, Commit: &indexPut},
		Entries: []*pb.Entry{{Index: &indexPut, Term: &term, Type: &kind, Data: putRaw}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.apply.ApplyNormal(raftmodel.ApplyMeta{Index: indexPut, Term: term, Type: kind}, putRaw); err != nil {
		t.Fatal(err)
	}
	putLookup, err := owner.apply.LookupCompletion(putRaw)
	if err != nil {
		t.Fatal(err)
	}
	putCompletion, err := replication.OpenCompletion(putLookup.Bytes)
	if err != nil || putCompletion.ResultCode != replicatedstate.ResultApplied {
		t.Fatalf("seed mutation completion: %+v %v", putCompletion, err)
	}
}

// This reproduces a follower that staged its own catalog image, then applied
// the leader's already-committed transition before it wrote its local
// activation record. The command differs only in the replica-local catalog
// CAS, while a real leader no-op precedes it in the shared Raft log.
func TestRF3DynamicSchemaAdoptsCommittedPeerCatalogCAS(t *testing.T) {
	f := newRF3NodeRecoveryFixtureWithLearnerAndCreateTable(
		t, true, `CREATE TABLE docs (id TEXT PRIMARY KEY, value TEXT)`,
	)
	if _, err := f.store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	log, _ := f.store.GroupByID(f.boots[0].Descriptor.GroupID)
	db, apply, err := openRF3SelectedLog(f.paths[0], log, f.bases[0], f.applies[0])
	if err != nil {
		t.Fatal(err)
	}
	baseOwner := &dynamicSchemaOwner{log: log, db: db, apply: apply, identity: dynamicSchemaIdentity(t, apply, 1)}
	owner := &aliasDynamicSchemaOwner{dynamicSchemaOwner: baseOwner}
	t.Cleanup(func() { _ = owner.apply.Close(); _ = owner.db.Close() })
	activator, err := newRF3SchemaActivator(owner, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	spec := dynamicSchemaSpec(t, f, baseOwner)
	root := filepath.Dir(f.paths[0])
	identity := owner.identity
	if err := activator.RegisterDynamic(identity, apply, spec, root, log); err != nil {
		t.Fatal(err)
	}
	seedRF3CASAliasRow(t, f.bases[0], baseOwner)
	if err := assertRF3CASAliasSeed(t, owner.apply, false); err != nil {
		t.Fatalf("source seed before Stage: %v", err)
	}
	const sql = "ALTER TABLE docs ADD COLUMN marker TEXT"
	request := schemainstall.BuildRequest{Operation: [32]byte{81}, Group: identity.Group,
		AllocationGeneration: 1, FromSchemaGeneration: 1, FromRelationManifestDigest: identity.RelationManifestDigest,
		SourceApplied: apply.Applied(), SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
	target, err := activator.BuildSchema(t.Context(), request, sql)
	if err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(t.TempDir(), "catalog")
	if err := os.WriteFile(artifact, target.Catalog, 0600); err != nil {
		t.Fatal(err)
	}
	activator.files = dynamicSchemaArtifact(artifact)
	install := schemainstall.Request{Operation: request.Operation, Group: identity.Group, AllocationGeneration: 1,
		FromSchemaGeneration: 1, FromRelationManifestDigest: request.FromRelationManifestDigest,
		ToSchemaGeneration: target.Proof.Catalog.SchemaGeneration, ToRelationManifestDigest: target.Proof.Catalog.RelationManifestDigest,
		ApplyContractDigest: target.Proof.ApplyContract, BundleDigest: target.Proof.Catalog.Digest, BundleBytes: uint64(len(target.Catalog))}
	witness, err := activator.Stage(t.Context(), install, install.BundleDigest, "")
	if err != nil {
		t.Fatal(err)
	}
	installation := schemainstall.InstallationDigest(install, schemainstall.MaterializedArtifactDigest(install.BundleDigest, witness))
	authorization := schemainstall.Authorization{Operation: request.Operation, TargetCatalogGeneration: 2,
		TargetCatalogDigest: [32]byte{82}, PreparedGroupCount: 1, PreparedGroupRoot: [32]byte{83}, ContractDigest: schemainstall.ContractDigest()}

	proof, err := apply.RecoverPreparedReplicatedSchemaTarget(target.Catalog, request.Operation)
	if err != nil {
		t.Fatal(err)
	}
	authorizationDigest := schemainstall.AuthorizationDigest(authorization)
	localCAS, err := apply.ReplicatedSchemaCatalogCASDigest(proof, request.Operation, authorizationDigest)
	if err != nil {
		t.Fatal(err)
	}
	local, err := apply.AppendReplicatedSchemaTransition(nil, proof,
		rf3SchemaTransitionAuthority(install, authorization, authorizationDigest, localCAS))
	if err != nil {
		t.Fatal(err)
	}
	localTransition, err := replicatedstate.OpenSchemaTransition(local)
	if err != nil {
		t.Fatal(err)
	}
	peerTransition := localTransition.SchemaTransition
	peerTransition.CatalogCASDigest[0] ^= 0x80
	peer, err := replicatedstate.AppendSchemaTransition(nil, peerTransition)
	if err != nil {
		t.Fatal(err)
	}
	peerOpened, err := replicatedstate.OpenSchemaTransition(peer)
	if err != nil || peerOpened.CatalogCASDigest == localCAS {
		t.Fatalf("test peer CAS was not replica-local: %v", err)
	}

	// Persist the neutral leader no-op and peer transition together, then apply
	// them through the real shared group log before Commit starts.
	noopIndex := apply.Applied() + 1
	peerIndex := noopIndex + 1
	term, kind := uint64(2), pb.EntryNormal
	if err := log.Persist(raftmodel.PersistBatch{NodeIncarnation: identity.NodeIncarnation,
		ReadyID: 3, MustSync: true, HardState: &pb.HardState{Term: &term, Commit: &peerIndex},
		Entries: []*pb.Entry{
			{Index: &noopIndex, Term: &term, Type: &kind},
			{Index: &peerIndex, Term: &term, Type: &kind, Data: peer},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := apply.ApplyNormal(raftmodel.ApplyMeta{Index: noopIndex, Term: term, Type: kind}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := apply.ApplyNormal(raftmodel.ApplyMeta{Index: peerIndex, Term: term, Type: kind}, peer); err != nil {
		t.Fatal(err)
	}
	owner.peerCommand = bytes.Clone(peer)

	localAuthority := rf3SchemaTransitionAuthority(install, authorization, authorizationDigest, [32]byte{})
	assertRF3SchemaAliasNotObserved(t, apply, local, peerIndex)
	badAuthority := localAuthority
	badAuthority.RequestDigest[0] ^= 1
	if _, err := apply.AppendReplicatedSchemaTransitionAlias(nil, proof, badAuthority, peer, peerIndex); !errors.Is(err, sqldriver.ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("foreign operation authority accepted: %v", err)
	}
	badAuthority = localAuthority
	badAuthority.AuthorizationDigest[0] ^= 1
	if _, err := apply.AppendReplicatedSchemaTransitionAlias(nil, proof, badAuthority, peer, peerIndex); !errors.Is(err, sqldriver.ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("foreign authorization accepted: %v", err)
	}
	badProof := proof
	badProof.Catalog.SchemaGeneration++
	if _, err := apply.AppendReplicatedSchemaTransitionAlias(nil, badProof, localAuthority, peer, peerIndex); !errors.Is(err, sqldriver.ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("foreign target identity accepted: %v", err)
	}
	badPortable := peerTransition
	badPortable.ToManifest[0] ^= 1
	badPortableCommand, err := replicatedstate.AppendSchemaTransition(nil, badPortable)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apply.AppendReplicatedSchemaTransitionAlias(nil, proof, localAuthority, badPortableCommand, peerIndex); !errors.Is(err, sqldriver.ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("foreign portable transition field accepted: %v", err)
	}
	badSource := peerTransition
	badSource.From.RoutingVersion++
	badSourceCommand, err := replicatedstate.AppendSchemaTransition(nil, badSource)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apply.AppendReplicatedSchemaTransitionAlias(nil, proof, localAuthority, badSourceCommand, peerIndex); !errors.Is(err, sqldriver.ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("foreign source identity accepted: %v", err)
	}
	nonNeutral := &pb.Entry{Index: &noopIndex, Term: &term, Type: &kind, Data: []byte{1}}
	if err := rf3SchemaReplayNeutralSuffix(schemaRecoveryWAL{entries: []*pb.Entry{nonNeutral}},
		request.SourceApplied, noopIndex, request.Operation, request.Group); !errors.Is(err, schemainstall.ErrConflict) {
		t.Fatalf("non-neutral applied suffix accepted: %v", err)
	}
	assertRF3SchemaAliasNotObserved(t, apply, local, peerIndex)

	err = activator.Commit(t.Context(), install, authorization, installation, "")
	if err != nil {
		t.Logf("first Commit failure: %v", err)
		retryErr := activator.Commit(t.Context(), install, authorization, installation, "")
		t.Logf("retry Commit failure: %v", retryErr)
		t.Fatalf("Commit should adopt the already-applied peer transition using a local CAS alias: %v", err)
	}
	if owner.proposals != 0 {
		t.Fatalf("adoption proposed another command %d times", owner.proposals)
	}
	if err := activator.Activate(t.Context(), install, authorization, installation, ""); err != nil {
		t.Fatalf("Activate after adopted Commit: %v", err)
	}
	if active, err := activator.ObserveActive(t.Context(), install, authorization, installation, ""); err != nil || !active {
		t.Fatalf("adopted schema not active: %t %v", active, err)
	}
	if profile, err := owner.apply.CapacityQualificationProfile(); err != nil ||
		profile.Binding.Authority.SchemaGeneration != install.ToSchemaGeneration ||
		profile.RelationManifestDigest != install.ToRelationManifestDigest {
		t.Fatalf("activated target generation/profile: %+v %v", profile, err)
	}
	if err := assertRF3CASAliasSeed(t, owner.apply, true); err != nil {
		t.Fatal(err)
	}
	published, found, err := sqldriver.ObservePublishedReplicatedSchemaTransition(f.paths[0])
	if err != nil || !found || published.CatalogCASDigest != localCAS || published.CatalogCASDigest == peerOpened.CatalogCASDigest {
		t.Fatalf("published local alias found=%t CAS=%x err=%v", found, published.CatalogCASDigest, err)
	}
	persisted, found, err := sqldriver.ObservePersistedReplicatedSchemaTransition(f.paths[0])
	if err != nil || !found || persisted.CatalogCASDigest != localCAS {
		t.Fatalf("persisted local alias found=%t CAS=%x err=%v", found, persisted.CatalogCASDigest, err)
	}
	peerTransition.CatalogCASDigest = published.CatalogCASDigest
	if published.SchemaTransition != peerTransition {
		t.Fatal("local activation changed replicated transition fields beyond CatalogCASDigest")
	}
	entries, err := log.Entries(noopIndex, peerIndex+1, 1<<20)
	if err != nil || len(entries) != 2 || !bytes.Equal(entries[1].Data, peer) {
		t.Fatalf("peer WAL command changed after local aliasing: entries=%d err=%v", len(entries), err)
	}
	if err := errors.Join(owner.apply.Close(), owner.db.Close()); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	log, _ = f.store.GroupByID(identity.Group.GroupID)
	_, _, reopenedDB, reopenedApply, err := openRF3RetainedApply(f.paths[0], log, f.bases[0], f.applies[0])
	if err != nil {
		t.Fatalf("second open of aliased target: %v", err)
	}
	t.Cleanup(func() { _ = reopenedApply.Close(); _ = reopenedDB.Close() })
	if profile, err := reopenedApply.CapacityQualificationProfile(); err != nil ||
		profile.Binding.Authority.SchemaGeneration != install.ToSchemaGeneration ||
		profile.RelationManifestDigest != install.ToRelationManifestDigest {
		t.Fatalf("reopened target generation/profile: %+v %v", profile, err)
	}
	if err := assertRF3CASAliasSeed(t, reopenedApply, true); err != nil {
		t.Fatal(err)
	}

}

// A different fresh source exercises the crash cut after the local alias is
// durably persisted but before catalog publication. Cold recovery must settle
// against the unchanged peer WAL command and survive a second reopen.
func TestRF3DynamicSchemaAliasColdRecoveryBeforePublish(t *testing.T) {
	const sourceDDL = `CREATE TABLE docs (id STRING PRIMARY KEY, value STRING)`
	f := newRF3NodeRecoveryFixtureWithLearnerAndCreateTable(
		t, true, sourceDDL,
	)
	if _, err := f.store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	log, _ := f.store.GroupByID(f.boots[0].Descriptor.GroupID)
	db, apply, err := openRF3SelectedLog(f.paths[0], log, f.bases[0], f.applies[0])
	if err != nil {
		t.Fatal(err)
	}
	baseOwner := &dynamicSchemaOwner{log: log, db: db, apply: apply, identity: dynamicSchemaIdentity(t, apply, 1)}
	owner := &aliasDynamicSchemaOwner{dynamicSchemaOwner: baseOwner}
	t.Cleanup(func() { _ = owner.apply.Close(); _ = owner.db.Close() })
	activator, err := newRF3SchemaActivator(owner, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	identity := owner.identity
	spec := dynamicSchemaSpec(t, f, baseOwner)
	spec.CreateTable = sourceDDL
	if err := activator.RegisterDynamic(identity, apply, spec, filepath.Dir(f.paths[0]), log); err != nil {
		t.Fatal(err)
	}
	seedRF3CASAliasRow(t, f.bases[0], baseOwner)
	const sql = "ALTER TABLE docs ADD COLUMN marker TEXT"
	request := schemainstall.BuildRequest{Operation: [32]byte{91}, Group: identity.Group,
		AllocationGeneration: 1, FromSchemaGeneration: 1, FromRelationManifestDigest: identity.RelationManifestDigest,
		SourceApplied: apply.Applied(), SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
	target, err := activator.BuildSchema(t.Context(), request, sql)
	if err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(t.TempDir(), "catalog")
	if err := os.WriteFile(artifact, target.Catalog, 0600); err != nil {
		t.Fatal(err)
	}
	activator.files = dynamicSchemaArtifact(artifact)
	install := schemainstall.Request{Operation: request.Operation, Group: identity.Group, AllocationGeneration: 1,
		FromSchemaGeneration: 1, FromRelationManifestDigest: request.FromRelationManifestDigest,
		ToSchemaGeneration:       target.Proof.Catalog.SchemaGeneration,
		ToRelationManifestDigest: target.Proof.Catalog.RelationManifestDigest,
		ApplyContractDigest:      target.Proof.ApplyContract, BundleDigest: target.Proof.Catalog.Digest,
		BundleBytes: uint64(len(target.Catalog))}
	_, err = activator.Stage(t.Context(), install, install.BundleDigest, "")
	if err != nil {
		t.Fatal(err)
	}
	authorization := schemainstall.Authorization{Operation: request.Operation, TargetCatalogGeneration: 2,
		TargetCatalogDigest: [32]byte{92}, PreparedGroupCount: 1, PreparedGroupRoot: [32]byte{93},
		ContractDigest: schemainstall.ContractDigest()}
	proof, err := apply.RecoverPreparedReplicatedSchemaTarget(target.Catalog, request.Operation)
	if err != nil {
		t.Fatal(err)
	}
	authorizationDigest := schemainstall.AuthorizationDigest(authorization)
	localCAS, err := apply.ReplicatedSchemaCatalogCASDigest(proof, request.Operation, authorizationDigest)
	if err != nil {
		t.Fatal(err)
	}
	local, err := apply.AppendReplicatedSchemaTransition(nil, proof,
		rf3SchemaTransitionAuthority(install, authorization, authorizationDigest, localCAS))
	if err != nil {
		t.Fatal(err)
	}
	localView, err := replicatedstate.OpenSchemaTransition(local)
	if err != nil {
		t.Fatal(err)
	}
	peerView := localView.SchemaTransition
	peerView.CatalogCASDigest[0] ^= 0x20
	peer, err := replicatedstate.AppendSchemaTransition(nil, peerView)
	if err != nil {
		t.Fatal(err)
	}
	peerOpened, err := replicatedstate.OpenSchemaTransition(peer)
	if err != nil || peerOpened.CatalogCASDigest == localCAS {
		t.Fatalf("test peer CAS was not replica-local: %v", err)
	}
	noopIndex := apply.Applied() + 1
	peerIndex := noopIndex + 1
	term, kind := uint64(2), pb.EntryNormal
	if err := log.Persist(raftmodel.PersistBatch{NodeIncarnation: identity.NodeIncarnation,
		ReadyID: 3, MustSync: true, HardState: &pb.HardState{Term: &term, Commit: &peerIndex},
		Entries: []*pb.Entry{
			{Index: &noopIndex, Term: &term, Type: &kind},
			{Index: &peerIndex, Term: &term, Type: &kind, Data: peer},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := apply.ApplyNormal(raftmodel.ApplyMeta{Index: noopIndex, Term: term, Type: kind}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := apply.ApplyNormal(raftmodel.ApplyMeta{Index: peerIndex, Term: term, Type: kind}, peer); err != nil {
		t.Fatal(err)
	}
	if err := rf3SchemaReplayNeutralSuffix(log, request.SourceApplied, noopIndex, request.Operation, request.Group); err != nil {
		t.Fatalf("leader no-op suffix rejected: %v", err)
	}
	localAuthority := rf3SchemaTransitionAuthority(install, authorization, authorizationDigest, [32]byte{})
	assertRF3SchemaAliasNotObserved(t, apply, local, peerIndex)

	// Reordering declared schema fields changes canonical source bytes while
	// preserving the sorted portable schema digest. The alias proof must reject
	// this mismatch before its observer can mutate machine state.
	sourceRaw, err := os.ReadFile(f.paths[0])
	if err != nil {
		t.Fatal(err)
	}
	mutatedRaw := reverseRF3CanonicalSchemaFields(t, sourceRaw)
	sourceImage, err := sqldriver.ValidateReplicatedSchemaCatalogImage(sourceRaw)
	if err != nil {
		t.Fatal(err)
	}
	mutatedImage, err := sqldriver.ValidateReplicatedSchemaCatalogImage(mutatedRaw)
	if err != nil {
		t.Fatalf("mutated source image is not canonical and valid: %v", err)
	}
	if mutatedImage.Digest == sourceImage.Digest ||
		mutatedImage.RelationManifestDigest != sourceImage.RelationManifestDigest ||
		mutatedImage.LocalRelationManifestDigest != sourceImage.LocalRelationManifestDigest ||
		mutatedImage.ApplyProfileDigest != sourceImage.ApplyProfileDigest ||
		mutatedImage.SchemaGeneration != sourceImage.SchemaGeneration {
		t.Fatal("test metadata mutation changed image digest or portable identity unexpectedly")
	}
	if err := os.WriteFile(f.paths[0], mutatedRaw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.WriteFile(f.paths[0], sourceRaw, 0600) })
	mutatedAlias, mismatchErr := apply.AppendReplicatedSchemaTransitionAlias(nil, proof, localAuthority, peer, peerIndex)
	if restoreErr := os.WriteFile(f.paths[0], sourceRaw, 0600); restoreErr != nil {
		t.Fatal(restoreErr)
	}
	if !errors.Is(mismatchErr, sqldriver.ErrReplicatedSchemaCatalogImage) {
		applied, observed, observeErr := apply.ObserveReplicatedSchemaTransition(mutatedAlias)
		t.Fatalf("canonical source metadata mismatch was not rejected: alias accepted applied=%d observed=%t observeErr=%v err=%v", applied, observed, observeErr, mismatchErr)
	}
	assertRF3SchemaAliasNotObserved(t, apply, local, peerIndex)

	if _, err := apply.AppendReplicatedSchemaTransitionAlias(nil, proof, localAuthority, peer, peerIndex+1); !errors.Is(err, sqldriver.ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("wrong expected applied index accepted: %v", err)
	}
	assertRF3SchemaAliasNotObserved(t, apply, local, peerIndex)
	badFinalView := peerView
	badFinalView.FromApplyContract[0] ^= 1
	badFinalCommand, err := replicatedstate.AppendSchemaTransition(nil, badFinalView)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apply.AppendReplicatedSchemaTransitionAlias(nil, proof, localAuthority, badFinalCommand, peerIndex); !errors.Is(err, sqldriver.ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("mutated committed command passed the final live proof: %v", err)
	}
	assertRF3SchemaAliasNotObserved(t, apply, local, peerIndex)

	prefix := []byte("existing-caller-prefix")
	aliasWithPrefix, err := apply.AppendReplicatedSchemaTransitionAlias(bytes.Clone(prefix), proof,
		localAuthority, peer, peerIndex)
	if err != nil {
		t.Fatalf("build local alias with destination prefix: %v", err)
	}
	if !bytes.Equal(aliasWithPrefix[:len(prefix)], prefix) {
		t.Fatal("alias builder changed caller-owned destination prefix")
	}
	alias := bytes.Clone(aliasWithPrefix[len(prefix):])
	if _, err := replicatedstate.OpenSchemaTransition(alias); err != nil {
		t.Fatalf("alias builder returned invalid appended command suffix: %v", err)
	}
	if err := apply.PersistReplicatedSchemaTransitionAfterEmptySuffix(alias, request.SourceApplied, noopIndex); err != nil {
		t.Fatalf("persist local alias before crash: %v", err)
	}
	persisted, found, err := sqldriver.ObservePersistedReplicatedSchemaTransition(f.paths[0])
	if err != nil || !found || persisted.CatalogCASDigest != localCAS {
		t.Fatalf("prepublication persisted alias found=%t CAS=%x err=%v", found, persisted.CatalogCASDigest, err)
	}
	if published, found, err := sqldriver.ObservePublishedReplicatedSchemaTransition(f.paths[0]); err != nil || found {
		t.Fatalf("alias unexpectedly published before crash found=%t transition=%+v err=%v", found, published.SchemaTransition, err)
	}
	if err := errors.Join(apply.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	log, _ = f.store.GroupByID(identity.Group.GroupID)
	base, applyID, recoveredDB, recoveredApply, err := openRF3RetainedApply(f.paths[0], log, f.bases[0], f.applies[0])
	if err != nil {
		t.Fatalf("cold recovery from prepublication alias: %v", err)
	}
	t.Cleanup(func() { _ = recoveredApply.Close(); _ = recoveredDB.Close() })
	if base.Binding.Authority.SchemaGeneration != install.ToSchemaGeneration {
		t.Fatalf("cold recovery selected schema generation %d, want %d", base.Binding.Authority.SchemaGeneration, install.ToSchemaGeneration)
	}
	if profile, err := recoveredApply.CapacityQualificationProfile(); err != nil ||
		profile.Binding.Authority.SchemaGeneration != install.ToSchemaGeneration ||
		profile.RelationManifestDigest != install.ToRelationManifestDigest {
		t.Fatalf("cold-recovered target profile: %+v %v", profile, err)
	}
	if err := assertRF3CASAliasSeed(t, recoveredApply, true); err != nil {
		t.Fatal(err)
	}
	published, found, err := sqldriver.ObservePublishedReplicatedSchemaTransition(f.paths[0])
	if err != nil || !found || published.CatalogCASDigest != localCAS {
		t.Fatalf("cold-recovered published alias found=%t CAS=%x err=%v", found, published.CatalogCASDigest, err)
	}
	entries, err := log.Entries(noopIndex, peerIndex+1, 1<<20)
	if err != nil || len(entries) != 2 || !bytes.Equal(entries[1].Data, peer) {
		t.Fatalf("peer WAL command changed during cold recovery: entries=%d err=%v", len(entries), err)
	}
	if err := errors.Join(recoveredApply.Close(), recoveredDB.Close()); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	log, _ = f.store.GroupByID(identity.Group.GroupID)
	_, _, finalDB, finalApply, err := openRF3RetainedApply(f.paths[0], log, base, applyID)
	if err != nil {
		t.Fatalf("second reopen after cold alias recovery: %v", err)
	}
	t.Cleanup(func() { _ = finalApply.Close(); _ = finalDB.Close() })
	if profile, err := finalApply.CapacityQualificationProfile(); err != nil ||
		profile.Binding.Authority.SchemaGeneration != install.ToSchemaGeneration ||
		profile.RelationManifestDigest != install.ToRelationManifestDigest {
		t.Fatalf("second-reopen target profile: %+v %v", profile, err)
	}
	if err := assertRF3CASAliasSeed(t, finalApply, true); err != nil {
		t.Fatal(err)
	}
	if proposals := owner.proposals; proposals != 0 {
		t.Fatalf("crash recovery proposed a new command %d times", proposals)
	}
}

func reverseRF3CanonicalSchemaFields(t *testing.T, raw []byte) []byte {
	t.Helper()
	var catalog struct {
		Tables map[string]json.RawMessage `json:"tables"`
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatalf("decode source catalog: %v; raw=%s", err, raw)
	}
	var table struct {
		Schema json.RawMessage `json:"schema"`
	}
	if err := json.Unmarshal(catalog.Tables["docs"], &table); err != nil {
		t.Fatalf("decode docs table: raw table=%q err=%v; catalog=%s", catalog.Tables["docs"], err, raw)
	}
	var schema struct {
		Fields []json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(table.Schema, &schema); err != nil {
		t.Fatalf("decode docs schema %q: %v; raw=%s", table.Schema, err, raw)
	}
	if len(schema.Fields) < 2 {
		t.Fatalf("source schema has only %d declared fields", len(schema.Fields))
	}
	before, err := json.Marshal(schema.Fields)
	if err != nil {
		t.Fatal(err)
	}
	schema.Fields[0], schema.Fields[1] = schema.Fields[1], schema.Fields[0]
	after, err := json.Marshal(schema.Fields)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(raw, before) != 1 {
		t.Fatal("could not identify the unique canonical source schema-field array")
	}
	mutated := bytes.Replace(raw, before, after, 1)
	if bytes.Equal(mutated, raw) {
		t.Fatal("schema-field metadata mutation made no byte change")
	}
	return mutated
}

func assertRF3SchemaAliasNotObserved(t *testing.T, apply *sqldriver.ReplicatedApply, local []byte, expected uint64) {
	t.Helper()
	applied, observed, err := apply.ObserveReplicatedSchemaTransition(local)
	if err != nil || observed || applied != expected {
		t.Fatalf("rejected alias changed machine observation: applied=%d observed=%t err=%v", applied, observed, err)
	}
}

func assertRF3CASAliasSeed(t *testing.T, apply *sqldriver.ReplicatedApply, withMarker bool) error {
	t.Helper()
	var cut replicatedstate.DataReadCut
	if err := apply.DataReadCutInto(nil, apply.Applied(), &cut); err != nil {
		return err
	}
	defer cut.Close()
	reader, err := apply.NewDataReadSession(t.Context(), &cut, driverquery.ExecOptions{})
	if err != nil {
		return err
	}
	defer reader.Close()
	sql := `SELECT id, value FROM docs`
	if withMarker {
		sql = `SELECT id, value, marker FROM docs`
	}
	query, err := reader.Prepare(t.Context(), sql)
	if err != nil {
		return err
	}
	defer query.Close()
	rows, err := query.Query(t.Context(), nil)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return errors.New("reopened target lost seeded row")
	}
	if got := string(rows.Cell(0).AppendJSON(nil)); got != `"cas-seed"` {
		return fmt.Errorf("target id = %s", got)
	}
	if got := string(rows.Cell(1).AppendJSON(nil)); got != `"preserved"` {
		return fmt.Errorf("target value = %s", got)
	}
	if withMarker {
		if got := string(rows.Cell(2).AppendJSON(nil)); got != "null" {
			return fmt.Errorf("new nullable marker = %s", got)
		}
	}
	if rows.Next() {
		return errors.New("unexpected extra target row")
	}
	return nil
}
