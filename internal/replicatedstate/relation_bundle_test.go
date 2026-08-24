package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type relationBundleFixture struct {
	machine *Machine
	binding Binding
	system  CollectionTarget
	base    CollectionTarget
	global  CollectionTarget
	log     *durable.TxnLog
	group   *durable.CheckpointGroup
	index   store.IndexDefinition
	dir     string
	options Options
}

func newRelationBundleFixture(t testing.TB, checkpoint bool) relationBundleFixture {
	t.Helper()
	dir := t.TempDir()
	open := func(name string, options durable.Options) CollectionTarget {
		file, err := os.OpenFile(
			filepath.Join(dir, name+".vdb"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Create(file, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		return targetOf(collection)
	}
	index := store.IndexDefinition{Name: "by_email", Paths: []string{"/email"}}
	system := open("system", durable.Options{
		OpaqueValues: true, MaxBatchDocuments: 32,
	})
	system = systemTargetOf(system.Collection)
	base := open("base", durable.Options{Indexes: []store.IndexDefinition{index}})
	global := open("global", durable.Options{})
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	members := []durable.NamedCollection{
		{Name: systemCollectionName, Collection: system.Collection},
		{Name: "base", Collection: base.Collection},
		{Name: "global", Collection: global.Collection},
	}
	var group *durable.CheckpointGroup
	if checkpoint {
		group, err = durable.NewCheckpointGroup(
			log, members, durable.CheckpointGroupOptions{CheckpointEvery: 1024},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = group.Close() })
	}
	options := Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 3,
			MaxDocuments: base.Limits.MaxDistinctMutations +
				global.Limits.MaxDistinctMutations + 3,
			MaxBytes: 64 << 20,
		},
		MaxSessions: 128, RetryWindow: 8, CheckpointGroup: group,
	}
	binding := testBinding()
	machine, err := OpenBundle(
		binding, testBootstrap(), system,
		[]RelationCollection{
			{
				Relation: 1, Kind: RelationJSON, Name: "base", Target: base,
				LocalIndexes: []store.IndexDefinition{index},
			},
			{
				Relation: 2, Kind: RelationGlobalIndex, Name: "global", Target: global,
				GlobalIndex: GlobalIndexProfile{
					IndexID: 91, Incarnation: 7, LocatorCount: 1, Unique: true,
				},
			},
		},
		log, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.InstallSnapshot(testBootstrap()); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, machine, 2, commandValue(binding, 1))
	return relationBundleFixture{
		machine: machine, binding: binding, system: system, base: base,
		global: global, log: log, group: group, index: index, dir: dir, options: options,
	}
}

func (f relationBundleFixture) command(
	t testing.TB,
	sequence uint64,
	batches ...replication.RelationMutationBatch,
) []byte {
	t.Helper()
	command := commandValue(f.binding, sequence)
	command.Fingerprint = sha256.Sum256([]byte{0x62, byte(sequence), byte(sequence >> 8)})
	command.Batches = batches
	return encodeCommand(t, command)
}

func bundleCompletionResult(t testing.TB, machine *Machine, command []byte) uint32 {
	t.Helper()
	lookup, err := machine.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return completion.ResultCode
}

func exactIndexKeys(
	t testing.TB,
	collection *durable.Collection,
	name string,
	rawNeedle []byte,
) [][]byte {
	t.Helper()
	var entries [1]vibejson.IndexEntry
	needle, err := vibejson.BuildIndex(rawNeedle, entries[:])
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := snapshot.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	masks, err := snapshot.AppendIndexMasks(nil, name, needle)
	if err != nil {
		t.Fatal(err)
	}
	var keys [][]byte
	if err := snapshot.RangeMasksRaw(masks, func(key, _ []byte) error {
		keys = append(keys, bytes.Clone(key))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestRelationBundleAtomicBaseLocalAndGlobalIndexApply(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	globalKey := []byte{0x91, 0x01, 'a'}
	locator := []byte(`["doc-1"]`)
	put := fixture.command(t, 1,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("doc-1"),
			Value: []byte(`{"email":"a","n":1}`),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: locator,
		}}},
	)
	publication, err := fixture.machine.ApplyNormal(normalMeta(3), put)
	if err != nil || publication.Applied != 3 || bundleCompletionResult(t, fixture.machine, put) != ResultApplied {
		t.Fatalf("bundle put = %+v, %v", publication, err)
	}
	firstWitness, err := fixture.machine.LookupCompletion(put)
	if err != nil {
		t.Fatal(err)
	}
	secondWitness, err := fixture.machine.LookupCompletion(put)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(firstWitness.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstWitness.Bytes, secondWitness.Bytes) ||
		firstWitness.AppliedSequence != 3 || completion.AppliedSequence != 3 ||
		completion.ResultCode != ResultApplied || completion.ResultFormat != ResultFormatMutation ||
		completion.ResultLength != 0 || len(completion.InlineResult) != 0 ||
		completion.ResultDigest != replication.CompletionResultDigest(
			ResultApplied, ResultFormatMutation, nil,
		) {
		t.Fatal("bundle did not settle as one canonical completion witness")
	}
	if value, found, err := fixture.base.Collection.AppendRaw(nil, []byte("doc-1")); err != nil || !found || !bytes.Equal(value, []byte(`{"email":"a","n":1}`)) {
		t.Fatalf("base row = %q, %v, %v", value, found, err)
	}
	if value, found, err := fixture.global.Collection.AppendRaw(nil, globalKey); err != nil || !found || !bytes.Equal(value, locator) {
		t.Fatalf("global row = %q, %v, %v", value, found, err)
	}
	if keys := exactIndexKeys(t, fixture.base.Collection, fixture.index.Name, []byte(`"a"`)); len(keys) != 1 || !bytes.Equal(keys[0], []byte("doc-1")) {
		t.Fatalf("local exact index keys = %q", keys)
	}
	if fixture.group.AppliedIndex() != 3 || fixture.group.Stats().Updates != 3 {
		t.Fatalf("checkpoint group = %+v", fixture.group.Stats())
	}

	conflict := fixture.command(t, 2,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("doc-2"),
			Value: []byte(`{"email":"a","n":2}`),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey,
			Value: []byte(`["doc-2"]`),
		}}},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), conflict); err != nil {
		t.Fatal(err)
	}
	if bundleCompletionResult(t, fixture.machine, conflict) != ResultIndexConflict {
		t.Fatal("global uniqueness conflict did not settle deterministically")
	}
	if _, found, err := fixture.base.Collection.AppendRaw(nil, []byte("doc-2")); err != nil || found {
		t.Fatalf("conflicting base mutation escaped atomic bundle: found=%v err=%v", found, err)
	}
	if value, found, err := fixture.global.Collection.AppendRaw(nil, globalKey); err != nil || !found || !bytes.Equal(value, locator) {
		t.Fatalf("global row after conflict = %q, %v, %v", value, found, err)
	}

	idempotent := fixture.command(t, 3,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("doc-1"),
			Value: []byte(`{"email":"a","n":1}`),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey,
			// A different raw spelling is semantically the same locator. The
			// byte-native vibejson comparison must retain the original bytes and
			// avoid a write-amplifying replacement.
			Value: []byte(`["doc\u002d1"]`),
		}}},
	)
	before := fixture.machine.Published().DataChainDigest
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), idempotent); err != nil {
		t.Fatal(err)
	}
	if bundleCompletionResult(t, fixture.machine, idempotent) != ResultApplied ||
		fixture.machine.Published().DataChainDigest != before {
		t.Fatal("equal global locator was not an idempotent applied no-op")
	}

	deleteDigest := sha256.Sum256(locator)
	wrongLengthDelete := fixture.command(t, 4,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationDelete, Key: []byte("doc-1"),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationDeleteDigestEqual, Key: globalKey,
			ExpectedValueLength: uint64(len(locator) + 1), ExpectedValueDigest: deleteDigest,
		}}},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(6), wrongLengthDelete); err != nil {
		t.Fatal(err)
	}
	if bundleCompletionResult(t, fixture.machine, wrongLengthDelete) != ResultIndexConflict {
		t.Fatal("same-digest global delete with the wrong length did not conflict")
	}
	if _, found, _ := fixture.base.Collection.AppendRaw(nil, []byte("doc-1")); !found {
		t.Fatal("wrong-length global delete partially removed the base row")
	}

	wrongDigest := sha256.Sum256([]byte("stale-locator"))
	staleDelete := fixture.command(t, 5,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationDelete, Key: []byte("doc-1"),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationDeleteDigestEqual, Key: globalKey,
			ExpectedValueLength: uint64(len(locator)), ExpectedValueDigest: wrongDigest,
		}}},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(7), staleDelete); err != nil {
		t.Fatal(err)
	}
	if bundleCompletionResult(t, fixture.machine, staleDelete) != ResultIndexConflict {
		t.Fatal("stale global delete did not conflict")
	}
	if _, found, _ := fixture.base.Collection.AppendRaw(nil, []byte("doc-1")); !found {
		t.Fatal("stale global delete partially removed the base row")
	}

	deleteCommand := fixture.command(t, 6,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationDelete, Key: []byte("doc-1"),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationDeleteDigestEqual, Key: globalKey,
			ExpectedValueLength: uint64(len(locator)), ExpectedValueDigest: deleteDigest,
		}}},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(8), deleteCommand); err != nil {
		t.Fatal(err)
	}
	if bundleCompletionResult(t, fixture.machine, deleteCommand) != ResultApplied {
		t.Fatal("authenticated global delete did not apply")
	}
	if _, found, _ := fixture.base.Collection.AppendRaw(nil, []byte("doc-1")); found {
		t.Fatal("base row survived authenticated bundle delete")
	}
	if _, found, _ := fixture.global.Collection.AppendRaw(nil, globalKey); found {
		t.Fatal("global row survived authenticated bundle delete")
	}
	if keys := exactIndexKeys(t, fixture.base.Collection, fixture.index.Name, []byte(`"a"`)); len(keys) != 0 {
		t.Fatalf("local exact index retained deleted row: %q", keys)
	}
}

func TestRelationBundleUnknownRelationAndSchemaGenerationAreDistinct(t *testing.T) {
	fixture := newRelationBundleFixture(t, false)
	unknown := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 3,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("ghost"), Value: []byte(`{"n":1}`),
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), unknown); err != nil {
		t.Fatal(err)
	}
	if bundleCompletionResult(t, fixture.machine, unknown) != ResultUnknownRelation {
		t.Fatal("unknown relation was overloaded as another result")
	}
	if fixture.base.Collection.Len() != 0 || fixture.global.Collection.Len() != 0 {
		t.Fatal("unknown relation changed a durable image")
	}

	staleSchema := commandValue(fixture.binding, 2)
	staleSchema.SchemaGeneration++
	staleSchema.Fingerprint = sha256.Sum256([]byte("wrong-schema-generation"))
	encoded := encodeCommand(t, staleSchema)
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), encoded); err != nil {
		t.Fatal(err)
	}
	if bundleCompletionResult(t, fixture.machine, encoded) != ResultStaleFence {
		t.Fatal("schema-generation mismatch was not rejected by the authenticated fence")
	}
}

func TestRelationBundlePhysicalImagesDoNotEnterReplicatedWitness(t *testing.T) {
	left := newRelationBundleFixture(t, false)
	right := newRelationBundleFixture(t, false)
	if left.dir == right.dir || left.machine.applyContract != right.machine.applyContract ||
		left.machine.manifestDigest != right.machine.manifestDigest {
		t.Fatal("logical bundle contract depends on a replica-local physical image")
	}
	command := left.command(t, 1,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("doc-1"),
			Value: []byte(`{"email":"a"}`),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual,
			Key:  []byte{0x91, 0x01, 'a'}, Value: []byte(`["doc-1"]`),
		}}},
	)
	leftPublication, err := left.machine.ApplyNormal(normalMeta(3), command)
	if err != nil {
		t.Fatal(err)
	}
	rightPublication, err := right.machine.ApplyNormal(normalMeta(3), command)
	if err != nil {
		t.Fatal(err)
	}
	leftCompletion, err := left.machine.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	rightCompletion, err := right.machine.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	if bundleCompletionResult(t, left.machine, command) != ResultApplied ||
		bundleCompletionResult(t, right.machine, command) != ResultApplied ||
		leftPublication.DataChainDigest != rightPublication.DataChainDigest ||
		!bytes.Equal(leftCompletion.Bytes, rightCompletion.Bytes) {
		t.Fatal("healthy replicas produced different bundle witnesses")
	}
}

func TestOpenBundleRejectsAggregateCapacityBeforeImageScan(t *testing.T) {
	dir := t.TempDir()
	open := func(name string, options durable.Options) CollectionTarget {
		t.Helper()
		file, err := os.OpenFile(
			filepath.Join(dir, name+".vdb"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Create(file, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		return targetOf(collection)
	}
	system := open("bounded-system", durable.Options{
		OpaqueValues: true, MaxBatchDocuments: 32,
	})
	system = systemTargetOf(system.Collection)
	bounded := durable.Options{
		MaxKeyBytes: 64, MaxDocumentBytes: 1024,
		MaxBatchDocuments: 8, MaxBatchBytes: 16 << 10,
	}
	base := open("bounded-base", bounded)
	global := open("bounded-global", bounded)
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	relations := []RelationCollection{
		{Relation: 1, Kind: RelationJSON, Name: "bounded-base", Target: base},
		{Relation: 2, Kind: RelationGlobalIndex, Name: "bounded-global", Target: global,
			GlobalIndex: GlobalIndexProfile{IndexID: 1, Incarnation: 1, LocatorCount: 1, Unique: true}},
	}
	hotSystemBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + MaxSessionRecordBytes +
		sha256.Size + 3 + MaxSessionSlotRecordBytes
	releaseSystemBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + 8*(sha256.Size+3)
	systemBatchBytes := max(hotSystemBytes, releaseSystemBytes)
	good := Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 3,
			MaxDocuments: base.Limits.MaxDistinctMutations +
				global.Limits.MaxDistinctMutations + 3,
			MaxBytes: int64(systemBatchBytes + base.Limits.MaxBatchBytes +
				global.Limits.MaxBatchBytes),
		},
		MaxSessions: 128, RetryWindow: 8,
	}
	before := [...]uint64{
		system.Collection.Stats().SnapshotFullScanCalls,
		base.Collection.Stats().SnapshotFullScanCalls,
		global.Collection.Stats().SnapshotFullScanCalls,
	}
	for _, test := range []struct {
		name   string
		mutate func(*Options)
	}{
		{"collections", func(o *Options) { o.TxnLimits.MaxCollections-- }},
		{"mutations", func(o *Options) {
			o.TxnLimits.MaxDocuments = base.Limits.MaxDistinctMutations + 3
		}},
		{"bytes", func(o *Options) {
			o.TxnLimits.MaxBytes = int64(systemBatchBytes + base.Limits.MaxBatchBytes)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := good
			test.mutate(&options)
			if _, err := OpenBundle(
				testBinding(), testBootstrap(), system, relations, log, options,
			); !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("OpenBundle = %v, want %v", err, ErrInvalidOptions)
			}
			after := [...]uint64{
				system.Collection.Stats().SnapshotFullScanCalls,
				base.Collection.Stats().SnapshotFullScanCalls,
				global.Collection.Stats().SnapshotFullScanCalls,
			}
			if after != before {
				t.Fatalf("capacity refusal scanned an image: before=%v after=%v", before, after)
			}
		})
	}
}

func TestRelationBundleSnapshotCertificateReopenAndManifestRejection(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	locator := []byte(`["doc-1"]`)
	command := fixture.command(t, 1,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("doc-1"),
			Value: []byte(`{"email":"a"}`),
		}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual,
			Key:  []byte{0x91, 0x01, 'a'}, Value: locator,
		}}},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	base, manifest, err := fixture.machine.BuildBundleSnapshotBase()
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenSnapshotBase(base)
	if err != nil || !equalSnapshotArtifactManifest(opened.Manifest, manifest) ||
		len(opened.Manifest.Relations) != 2 ||
		opened.Manifest.RelationManifestDigest != fixture.machine.manifestDigest {
		t.Fatalf("open bundle certificate = %+v, %v", opened.Manifest, err)
	}
	shortDescriptors := proto.Clone(base).(*pb.Snapshot)
	shortDescriptors.Data = bytes.Clone(base.GetData())
	binary.LittleEndian.PutUint64(
		shortDescriptors.Data[72:80], replication.MaxRelationsPerBundle,
	)
	sealRecord(shortDescriptors.Data, snapshotBaseChecksumDomain)
	if _, err := OpenSnapshotBase(shortDescriptors); err == nil {
		t.Fatal("aggregate descriptor geometry was accepted before relation allocation")
	}
	publication, err := fixture.machine.InstallSnapshot(base)
	if err != nil || publication.Applied != 3 ||
		fixture.machine.state.SnapshotBaseDigest != opened.Digest {
		t.Fatalf("install bundle certificate = %+v, %v", publication, err)
	}
	if err := fixture.group.MaybeCheckpoint(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenBundle(
		fixture.binding, testBootstrap(), fixture.system,
		[]RelationCollection{
			{
				Relation: 1, Kind: RelationJSON, Name: "base", Target: fixture.base,
				LocalIndexes: []store.IndexDefinition{fixture.index},
			},
			{
				Relation: 2, Kind: RelationGlobalIndex, Name: "global", Target: fixture.global,
				GlobalIndex: GlobalIndexProfile{
					IndexID: 91, Incarnation: 7, LocatorCount: 1, Unique: true,
				},
			},
		},
		fixture.log, fixture.machine.options,
	)
	if err != nil || reopened.Applied() != 3 ||
		reopened.state.SnapshotBaseDigest != opened.Digest ||
		reopened.Published().DataChainDigest != fixture.machine.Published().DataChainDigest {
		t.Fatalf("reopen certified bundle = applied %d, err %v", reopened.Applied(), err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*SnapshotArtifactManifest)
	}{
		{"reordered", func(m *SnapshotArtifactManifest) {
			m.Relations[0], m.Relations[1] = m.Relations[1], m.Relations[0]
		}},
		{"duplicate", func(m *SnapshotArtifactManifest) {
			m.Relations[1].Relation = m.Relations[0].Relation
		}},
		{"sparse", func(m *SnapshotArtifactManifest) {
			m.Relations[1].Relation = 3
		}},
		{"unknown_generation", func(m *SnapshotArtifactManifest) {
			m.RelationManifestDigest = sha256.Sum256([]byte("other-schema-generation"))
		}},
		{"relation_image", func(m *SnapshotArtifactManifest) {
			m.Relations[1].ImageDigest[0] ^= 1
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneSnapshotArtifactManifest(manifest)
			test.mutate(&candidate)
			if _, err := BuildSnapshotBase(candidate, testBootstrap()); err == nil {
				t.Fatal("malformed relation manifest built a certificate")
			}
		})
	}

	wrongBinding := fixture.binding
	wrongBinding.SchemaGeneration++
	if _, err := OpenBundle(
		wrongBinding, testBootstrap(), fixture.system,
		[]RelationCollection{
			{Relation: 1, Kind: RelationJSON, Name: "base", Target: fixture.base,
				LocalIndexes: []store.IndexDefinition{fixture.index}},
			{Relation: 2, Kind: RelationGlobalIndex, Name: "global", Target: fixture.global,
				GlobalIndex: GlobalIndexProfile{IndexID: 91, Incarnation: 7, LocatorCount: 1, Unique: true}},
		}, fixture.log, fixture.machine.options,
	); err == nil {
		t.Fatal("reopen accepted an unknown schema generation")
	}
}

func copyRelationBundleCrashDirectory(t testing.TB, source string) string {
	t.Helper()
	destination := t.TempDir()
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		input, err := os.Open(filepath.Join(source, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		output, createErr := os.OpenFile(
			filepath.Join(destination, entry.Name()),
			os.O_CREATE|os.O_EXCL|os.O_RDWR, info.Mode().Perm(),
		)
		if createErr == nil {
			_, createErr = io.Copy(output, input)
		}
		createErr = errors.Join(createErr, input.Close())
		if output != nil {
			createErr = errors.Join(createErr, output.Sync(), output.Close())
		}
		if createErr != nil {
			t.Fatal(createErr)
		}
	}
	return destination
}

func assertRecoveredRelationBundleCut(
	t testing.TB,
	dir string,
	binding Binding,
	index store.IndexDefinition,
	command []byte,
) {
	t.Helper()
	open := func(name string) *os.File {
		file, err := os.OpenFile(filepath.Join(dir, name+".vdb"), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		return file
	}
	files := []*os.File{open("system"), open("base"), open("global")}
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	collections, log, group, err := durable.OpenCollectionsWithCheckpointGroup(
		dir, durable.TxnLogOptions{},
		[]durable.TransactionCollectionOpen{
			{File: files[0], Options: durable.Options{OpaqueValues: true, MaxBatchDocuments: 32}},
			{File: files[1], Options: durable.Options{Indexes: []store.IndexDefinition{index}}},
			{File: files[2], Options: durable.Options{}},
		},
		[]string{systemCollectionName, "base", "global"},
		durable.CheckpointGroupOptions{CheckpointEvery: 1024},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = group.Close()
		for _, collection := range collections {
			_ = collection.Close()
		}
		_ = log.Close()
	}()
	system := systemTargetOf(collections[0])
	base := targetOf(collections[1])
	global := targetOf(collections[2])
	options := Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 3,
			MaxDocuments: base.Limits.MaxDistinctMutations +
				global.Limits.MaxDistinctMutations + 3,
			MaxBytes: 64 << 20,
		},
		MaxSessions: 128, RetryWindow: 8, CheckpointGroup: group,
	}
	machine, err := OpenBundle(
		binding, testBootstrap(), system,
		[]RelationCollection{
			{Relation: 1, Kind: RelationJSON, Name: "base", Target: base,
				LocalIndexes: []store.IndexDefinition{index}},
			{Relation: 2, Kind: RelationGlobalIndex, Name: "global", Target: global,
				GlobalIndex: GlobalIndexProfile{IndexID: 91, Incarnation: 7, LocatorCount: 1, Unique: true}},
		},
		log, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	baseValue, baseFound, baseErr := base.Collection.AppendRaw(nil, []byte("doc-crash"))
	globalValue, globalFound, globalErr := global.Collection.AppendRaw(nil, []byte{0x91, 0x01, 'c'})
	if baseErr != nil || globalErr != nil || baseFound != globalFound {
		t.Fatalf("recovered relation skew base=(%q,%v,%v) global=(%q,%v,%v)",
			baseValue, baseFound, baseErr, globalValue, globalFound, globalErr)
	}
	switch machine.Applied() {
	case 2:
		if baseFound {
			t.Fatal("old certified cut retained a relation mutation")
		}
		if _, err := machine.LookupCompletion(command); !errors.Is(err, ErrCompletionNotFound) {
			t.Fatalf("old cut completion = %v", err)
		}
	case 3:
		if !baseFound || !bytes.Equal(baseValue, []byte(`{"email":"crash"}`)) ||
			!bytes.Equal(globalValue, []byte(`["doc-crash"]`)) ||
			bundleCompletionResult(t, machine, command) != ResultApplied {
			t.Fatalf("complete cut values = %q/%q", baseValue, globalValue)
		}
		keys := exactIndexKeys(t, base.Collection, index.Name, []byte(`"crash"`))
		if len(keys) != 1 || !bytes.Equal(keys[0], []byte("doc-crash")) {
			t.Fatalf("complete cut local index = %q", keys)
		}
	default:
		t.Fatalf("recovered noncanonical applied cut %d", machine.Applied())
	}
}

func TestRelationBundleCheckpointCrashPhasesNeverRecoverSkew(t *testing.T) {
	phases := []struct {
		name        string
		phase       durable.CheckpointGroupFaultPhaseForFacadeTest
		duringApply bool
	}{
		{"prepare_append", durable.CheckpointGroupFaultAfterPrepareAppendForFacadeTest, true},
		{"decision_append", durable.CheckpointGroupFaultAfterDecisionAppendForFacadeTest, true},
		{"journal_sync", durable.CheckpointGroupFaultAfterJournalSyncForFacadeTest, false},
		{"physical_checkpoint", durable.CheckpointGroupFaultAfterPhysicalCheckpointForFacadeTest, false},
		{"certificate_write", durable.CheckpointGroupFaultAfterCertificateWriteForFacadeTest, false},
		{"certificate_sync", durable.CheckpointGroupFaultAfterCertificateSyncForFacadeTest, false},
	}
	for _, test := range phases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRelationBundleFixture(t, true)
			if err := fixture.group.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			command := fixture.command(t, 1,
				replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
					Kind: replication.MutationPut, Key: []byte("doc-crash"),
					Value: []byte(`{"email":"crash"}`),
				}}},
				replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
					Kind: replication.MutationPutAbsentOrEqual,
					Key:  []byte{0x91, 0x01, 'c'}, Value: []byte(`["doc-crash"]`),
				}}},
			)
			var fired func() bool
			var restore func()
			if test.duringApply {
				fired, restore = durable.InstallCheckpointGroupFaultForFacadeTest(test.phase)
			}
			_, applyErr := fixture.machine.ApplyNormal(normalMeta(3), command)
			if test.duringApply {
				restore()
				if !fired() || applyErr == nil {
					t.Fatalf("apply fault fired=%v err=%v", fired(), applyErr)
				}
			} else {
				if applyErr != nil {
					t.Fatal(applyErr)
				}
				fired, restore = durable.InstallCheckpointGroupFaultForFacadeTest(test.phase)
				checkpointErr := fixture.group.Checkpoint()
				restore()
				if !fired() || checkpointErr == nil {
					t.Fatalf("checkpoint fault fired=%v err=%v", fired(), checkpointErr)
				}
			}
			crash := copyRelationBundleCrashDirectory(t, fixture.dir)
			assertRecoveredRelationBundleCut(t, crash, fixture.binding, fixture.index, command)
		})
	}
}

func BenchmarkMachineAdmitRelationBundleScaling(b *testing.B) {
	for _, relationCount := range []int{1, 2, 8} {
		b.Run(fmt.Sprintf("relations=%d", relationCount), func(b *testing.B) {
			dir := b.TempDir()
			open := func(name string, options durable.Options) CollectionTarget {
				file, err := os.OpenFile(
					filepath.Join(dir, name+".vdb"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
				)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = file.Close() })
				collection, err := durable.Create(file, options)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = collection.Close() })
				return targetOf(collection)
			}
			system := systemTargetOf(open("system", durable.Options{
				OpaqueValues: true, MaxBatchDocuments: 32,
			}).Collection)
			relations := make([]RelationCollection, relationCount)
			batches := make([]replication.RelationMutationBatch, relationCount)
			for ordinal := range relationCount {
				name := fmt.Sprintf("relation-%02d", ordinal+1)
				relations[ordinal] = RelationCollection{
					Relation: replication.RelationID(ordinal + 1), Kind: RelationJSON,
					Name: name, Target: open(name, durable.Options{}),
				}
				batches[ordinal] = replication.RelationMutationBatch{
					Relation: replication.RelationID(ordinal + 1),
					Mutations: []replication.Mutation{{
						Kind: replication.MutationPut,
						Key:  []byte("key"), Value: []byte(`{"n":1}`),
					}},
				}
			}
			log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = log.Close() })
			binding := testBinding()
			machine, err := OpenBundle(
				binding, testBootstrap(), system, relations, log, Options{
					TxnLimits: durable.TxnLimits{
						MaxCollections: relationCount + 1,
						MaxDocuments:   replication.MaxMutations + 3,
						MaxBytes:       32 << 20,
					},
					MaxSessions: 128, RetryWindow: 8,
				},
			)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := machine.InstallSnapshot(testBootstrap()); err != nil {
				b.Fatal(err)
			}
			applySessionOpen(b, machine, 2, commandValue(binding, 1))
			command := commandValue(binding, 1)
			command.Fingerprint = sha256.Sum256([]byte{0x71, byte(relationCount)})
			command.Batches = batches
			encoded := encodeCommand(b, command)
			b.ReportAllocs()
			b.ReportMetric(float64(relationCount), "relations/op")
			b.SetBytes(int64(len(encoded)))
			b.ResetTimer()
			for b.Loop() {
				if err := machine.AdmitCommand(encoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
