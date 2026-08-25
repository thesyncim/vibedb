package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type stagedSnapshotCountingValidator struct {
	puts int
}

func stagedSnapshotMachineOptions(
	t testing.TB,
	dir string,
	user CollectionTarget,
) Options {
	t.Helper()
	capture := createTargetAt(t, dir, TransitionCaptureCollectionName, durable.Options{
		OpaqueValues: true, MaxKeyBytes: 8,
		MaxDocumentBytes:  MaxTransitionCaptureRecordBytes,
		MaxBatchDocuments: 1, MaxBatchBytes: MaxTransitionCaptureRecordBytes + 8,
	})
	options := machineOptionsFor(user)
	options.TxnLimits.MaxCollections = 3
	options.TxnLimits.MaxDocuments++
	options.TxnLimits.MaxBytes = 512 << 20
	options.TransitionCaptureTarget = TransitionCaptureTarget{
		Name: TransitionCaptureCollectionName, Collection: capture.Collection,
	}
	return options
}

func (v *stagedSnapshotCountingValidator) ValidatePut(_, _ []byte) MutationValidation {
	v.puts++
	return MutationValidationAccept
}

func (*stagedSnapshotCountingValidator) ValidateDelete(
	_, _ []byte,
	_ bool,
) MutationValidation {
	return MutationValidationAccept
}

func TestImportedSnapshotRequiresExactSessionEpochFence(t *testing.T) {
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	user := createTargetAt(t, dir, "user-fence", durable.Options{})
	if _, err := user.Collection.Put([]byte("a"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	txnLog, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = txnLog.Close() })
	binding := testBinding()
	bootstrap := testBootstrap()
	options := stagedSnapshotMachineOptions(t, dir, user)
	cut := StagedSnapshotCut{
		Applied: 7, Term: 3,
		EntryDigest: sha256.Sum256([]byte("session-fenced-import")),
	}
	if _, _, _, err := InitializeStagedSnapshot(
		binding, bootstrap, system, UserCollection{Name: "docs", Target: user},
		txnLog, options, cut, SnapshotArtifactOptions{},
	); err != nil {
		t.Fatal(err)
	}
	raw, found, err := system.Collection.AppendRaw(nil, stateKey)
	if err != nil || !found {
		t.Fatalf("read imported state = %v, %v", found, err)
	}
	binary.LittleEndian.PutUint64(raw[360:368], cut.Applied-1)
	sealRecord(raw, stateChecksumDomain)
	if _, err := system.Collection.Put(stateKey, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(
		binding, bootstrap, system, UserCollection{Name: "docs", Target: user},
		txnLog, options,
	); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("reopen imported state below certified fence = %v, want ErrStateCorrupt", err)
	}
}

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
	options := stagedSnapshotMachineOptions(t, dir, user)
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
		!manifest.Seeded || manifest.TargetChunkBytes != 0 || manifest.Chunks != 0 ||
		manifest.SystemRows != 0 || manifest.PayloadBytes != 0 || manifest.EncodedBytes != 0 ||
		manifest.HeaderDigest != ([sha256.Size]byte{}) ||
		manifest.LastChunkDigest != ([sha256.Size]byte{}) ||
		manifest.CaptureRows != 0 ||
		manifest.CaptureImageDigest != snapshotArtifactEmptyCaptureImageDigest() ||
		base.GetMetadata().GetIndex() != cut.Applied {
		t.Fatalf("publication=%+v state=%+v rows=%d", publication, manifest.State, manifest.UserRows)
	}
	certificate, err := OpenSnapshotBase(base)
	if err != nil || certificate.Manifest.Digest != manifest.Digest {
		t.Fatalf("certificate=%+v err=%v", certificate, err)
	}
	if _, err := NewSnapshotArtifactStage(manifest, system, user, nil); !errors.Is(err, ErrSnapshotStage) {
		t.Fatalf("seeded manifest accepted as streamed artifact: %v", err)
	}
	for name, mutate := range map[string]func(*SnapshotArtifactManifest){
		"seed flag":    func(m *SnapshotArtifactManifest) { m.Seeded = false },
		"row count":    func(m *SnapshotArtifactManifest) { m.UserRows++ },
		"image digest": func(m *SnapshotArtifactManifest) { m.ImageDigest[0] ^= 1 },
		"chunk count":  func(m *SnapshotArtifactManifest) { m.Chunks = 1 },
		"capture rows": func(m *SnapshotArtifactManifest) { m.CaptureRows = 1 },
		"capture image": func(m *SnapshotArtifactManifest) {
			m.CaptureImageDigest = [sha256.Size]byte{}
		},
		"identity": func(m *SnapshotArtifactManifest) { m.Digest[0] ^= 1 },
	} {
		t.Run("reject seeded "+name+" mutation", func(t *testing.T) {
			invalid := cloneSnapshotArtifactManifest(manifest)
			mutate(&invalid)
			if _, err := BuildSnapshotBase(invalid, bootstrap); !errors.Is(err, ErrSnapshotBase) {
				t.Fatalf("BuildSnapshotBase error = %v", err)
			}
		})
	}
	forgedImage := cloneSnapshotArtifactManifest(manifest)
	forgedImage.ImageDigest[0] ^= 1
	stateEnvelope, err := AppendState(nil, forgedImage.State)
	if err != nil {
		t.Fatal(err)
	}
	forgedImage.Digest = seededSnapshotManifestDigest(
		stateEnvelope, forgedImage.UserCollection,
		forgedImage.ImageDigest, forgedImage.UserRows, forgedImage.CaptureImageDigest,
	)
	if _, err := BuildSnapshotBase(forgedImage, bootstrap); !errors.Is(err, ErrSnapshotBase) {
		t.Fatalf("self-consistent forged seeded image accepted: %v", err)
	}

	// A crash after the hidden state row but before the Raft base install can
	// deterministically reconstruct the same candidate and certificate.
	reopened, retryBase, retryManifest, err := InitializeStagedSnapshot(
		binding, bootstrap, system, UserCollection{Name: "docs", Target: user},
		txnLog, options, cut, SnapshotArtifactOptions{},
	)
	if err != nil || reopened.Published().DataChainDigest != publication.DataChainDigest ||
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
		installed.DataChainDigest != publication.DataChainDigest {
		t.Fatalf("install=%+v err=%v", installed, err)
	}
	afterA, found, err := user.Collection.AppendRaw(nil, []byte("a"))
	if err != nil || !found || !bytes.Equal(afterA, beforeA) {
		t.Fatalf("row after install=%q found=%v err=%v", afterA, found, err)
	}
	open := commandValue(binding, 1)
	openCommand := sessionOpenFor(open)
	encodedOpen := encodeCommand(t, openCommand)
	if err := machine.AdmitCommand(encodedOpen); err != nil {
		t.Fatalf("admit imported session open: %v", err)
	}
	if publication, err := machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: 8, Term: 3, Type: pb.EntryNormal,
	}, encodedOpen); err != nil || publication.Applied != 8 {
		t.Fatalf("apply imported session open = %+v, %v", publication, err)
	}
	lookup, err := machine.LookupCompletion(encodedOpen)
	if err != nil {
		t.Fatalf("lookup imported session open: %v", err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != ResultSessionOpened ||
		completion.ClientEpoch != 8 || completion.AppliedSequence != 8 {
		t.Fatalf("imported session open completion = %+v, %v", completion, err)
	}
	epoch := completion.ClientEpoch
	userCommand := commandValue(binding, 1)
	userCommand.ClientEpoch = epoch
	userCommand.Batches[0].Mutations = []replication.Mutation{{
		Kind: replication.MutationPut, Key: []byte("c"), Value: []byte(`{"n":3}`),
	}}
	command := encodeCommand(t, userCommand)
	if _, err := machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: 9, Term: 3, Type: pb.EntryNormal,
	}, command); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedStagedSnapshotRequiresCertifiedSeedBeforeFinish(t *testing.T) {
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	user := createTargetAt(t, dir, "seeded-user", durable.Options{})
	if _, err := user.Collection.Put([]byte("a"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := user.Collection.Put([]byte("b"), []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	validator := new(stagedSnapshotCountingValidator)
	user.Validator = validator
	user.ValidationDigest = sha256.Sum256([]byte("prepared-seed-one-pass"))
	txnLog, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = txnLog.Close() })
	options := stagedSnapshotMachineOptions(t, dir, user)
	capture := options.TransitionCaptureTarget.Collection
	prepared, err := PrepareStagedSnapshot(
		testBinding(), testBootstrap(), system,
		UserCollection{Name: "docs", Target: user}, txnLog, options,
		StagedSnapshotCut{
			Applied: 9, Term: 4,
			EntryDigest: sha256.Sum256([]byte("authenticated-seeded-cut")),
		},
		SnapshotArtifactOptions{},
	)
	if err != nil || !prepared.NeedsSeed() || prepared.AppliedIndex() != 9 ||
		prepared.SeedMember() != SystemCollectionName || validator.puts != 2 {
		t.Fatalf("prepare=%v needsSeed=%v applied=%d member=%q puts=%d",
			err, prepared.NeedsSeed(), prepared.AppliedIndex(), prepared.SeedMember(), validator.puts)
	}
	seed := durable.CheckpointGroupSeed{
		Applied: prepared.AppliedIndex(), Member: prepared.SeedMember(),
		Envelope: prepared.AppendSeedEnvelope(nil),
		Images: []durable.CheckpointGroupSeedImage{{
			Collection: user.Collection, Generation: prepared.UserGeneration(),
		}, {
			Collection: capture, Generation: prepared.CaptureGeneration(),
		}},
	}
	members := []durable.NamedCollection{
		{Name: SystemCollectionName, Collection: system.Collection},
		{Name: "docs", Collection: user.Collection},
		{Name: TransitionCaptureCollectionName, Collection: capture},
	}
	group, err := durable.NewSeededCheckpointGroup(
		txnLog, members, seed, durable.CheckpointGroupOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = group.Close() })
	if _, _, _, err := prepared.Finish(group); !errors.Is(err, ErrStagedSnapshot) {
		t.Fatalf("Finish before certified seed = %v", err)
	}
	if validator.puts != 2 {
		t.Fatalf("pre-seed Finish rescanned user: puts=%d", validator.puts)
	}
	if err := group.Seed(
		seed, members[0], options.TxnLimits, prepared.AppendSeedKey(nil),
	); err != nil {
		t.Fatal(err)
	}
	machine, base, manifest, err := prepared.Finish(group)
	if err != nil || machine.Published().Applied != 9 || !manifest.Seeded ||
		manifest.CaptureImageDigest != snapshotArtifactEmptyCaptureImageDigest() ||
		base.GetMetadata().GetIndex() != 9 || validator.puts != 2 || group.SeedPending() ||
		!group.Owns(members) {
		t.Fatalf("Finish publication=%+v seeded=%v index=%d puts=%d pending=%v err=%v",
			machine.Published(), manifest.Seeded, base.GetMetadata().GetIndex(),
			validator.puts, group.SeedPending(), err)
	}
	corruptKey := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	if err := group.Update(9, members[2:], options.TxnLimits,
		func(batch *durable.DatabaseBatch) error {
			captureBatch, err := batch.CollectionHandle(capture)
			if err != nil {
				return err
			}
			return captureBatch.Put(corruptKey, []byte("substituted-seeded-capture"))
		}); err != nil {
		t.Fatalf("substitute seeded capture: %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint substituted seeded capture: %v", err)
	}
	if _, err := machine.InstallSnapshot(base); !errors.Is(err, ErrSnapshotBase) {
		t.Fatalf("seeded install accepted substituted capture: %v", err)
	}
}

func TestPreparedStagedSnapshotGenerationFenceRejectsMutationBeforeAttach(t *testing.T) {
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	user := createTargetAt(t, dir, "generation-user", durable.Options{})
	if _, err := user.Collection.Put([]byte("a"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	txnLog, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = txnLog.Close() })
	prepared, err := PrepareStagedSnapshot(
		testBinding(), testBootstrap(), system,
		UserCollection{Name: "docs", Target: user}, txnLog,
		stagedSnapshotMachineOptions(t, dir, user),
		StagedSnapshotCut{
			Applied: 9, Term: 4,
			EntryDigest: sha256.Sum256([]byte("generation-fenced-seed")),
		},
		SnapshotArtifactOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if created, err := user.Collection.Put([]byte("a"), []byte(`{"n":2}`)); err != nil || created {
		t.Fatalf("same-count mutation created=%v err=%v", created, err)
	}
	seed := durable.CheckpointGroupSeed{
		Applied: prepared.AppliedIndex(), Member: prepared.SeedMember(),
		Envelope: prepared.AppendSeedEnvelope(nil),
		Images: []durable.CheckpointGroupSeedImage{{
			Collection: user.Collection, Generation: prepared.UserGeneration(),
		}},
	}
	group, err := durable.NewSeededCheckpointGroup(
		txnLog,
		[]durable.NamedCollection{
			{Name: SystemCollectionName, Collection: system.Collection},
			{Name: "docs", Collection: user.Collection},
		},
		seed, durable.CheckpointGroupOptions{},
	)
	if group != nil || !errors.Is(err, durable.ErrCheckpointGroupSeedChanged) ||
		system.Collection.Len() != 0 {
		t.Fatalf("changed image group=%v systemRows=%d err=%v", group, system.Collection.Len(), err)
	}
}

func TestPrepareStagedSnapshotRejectsNonemptyReservedCapture(t *testing.T) {
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	user := createTargetAt(t, dir, "nonempty-capture-user", durable.Options{})
	options := stagedSnapshotMachineOptions(t, dir, user)
	capture := options.TransitionCaptureTarget.Collection
	if _, err := capture.Put(
		[]byte{0, 0, 0, 0, 0, 0, 0, 1}, []byte("preexisting-capture"),
	); err != nil {
		t.Fatal(err)
	}
	txnLog, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = txnLog.Close() })
	if _, err := PrepareStagedSnapshot(
		testBinding(), testBootstrap(), system,
		UserCollection{Name: "docs", Target: user}, txnLog, options,
		StagedSnapshotCut{
			Applied: 9, Term: 4,
			EntryDigest: sha256.Sum256([]byte("reject-nonempty-capture")),
		}, SnapshotArtifactOptions{},
	); !errors.Is(err, ErrStagedSnapshot) {
		t.Fatalf("nonempty reserved capture preparation = %v", err)
	}
}

func TestPreparedStagedSnapshotCaptureGenerationFenceRejectsSubstitution(t *testing.T) {
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	user := createTargetAt(t, dir, "capture-generation-user", durable.Options{})
	options := stagedSnapshotMachineOptions(t, dir, user)
	txnLog, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = txnLog.Close() })
	prepared, err := PrepareStagedSnapshot(
		testBinding(), testBootstrap(), system,
		UserCollection{Name: "docs", Target: user}, txnLog, options,
		StagedSnapshotCut{
			Applied: 9, Term: 4,
			EntryDigest: sha256.Sum256([]byte("capture-generation-fence")),
		}, SnapshotArtifactOptions{},
	)
	if err != nil || prepared.CaptureGeneration() == 0 {
		t.Fatalf("prepare capture generation=%d err=%v", prepared.CaptureGeneration(), err)
	}
	if _, err := options.TransitionCaptureTarget.Collection.Put(
		[]byte{0, 0, 0, 0, 0, 0, 0, 1}, []byte("post-prepare-substitution"),
	); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := prepared.Initialize(); !errors.Is(err, ErrStagedSnapshot) {
		t.Fatalf("Finish accepted substituted capture generation: %v", err)
	}
}

func TestPreparedStagedSnapshotImageAuditCoversEmptyImage(t *testing.T) {
	dir := t.TempDir()
	system := createTargetAt(t, dir, "system", durable.Options{})
	user := createTargetAt(t, dir, "empty-user", durable.Options{})
	txnLog, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = txnLog.Close() })
	visits, finishes := 0, 0
	cut := StagedSnapshotCut{
		Applied: 9, Term: 4,
		EntryDigest: sha256.Sum256([]byte("empty-image-audit")),
		ImageAudit: StagedSnapshotImageAudit{
			Visit: func(_, _ []byte) error {
				visits++
				return nil
			},
			Finish: func() error {
				finishes++
				return nil
			},
		},
	}
	options := stagedSnapshotMachineOptions(t, dir, user)
	prepared, err := PrepareStagedSnapshot(
		testBinding(), testBootstrap(), system,
		UserCollection{Name: "docs", Target: user}, txnLog, options,
		cut, SnapshotArtifactOptions{},
	)
	if err != nil || prepared.UserGeneration() == 0 || visits != 0 || finishes != 1 {
		t.Fatalf("prepare=%v generation=%d visits=%d finishes=%d",
			err, prepared.UserGeneration(), visits, finishes)
	}
	cut.ImageAudit.Finish = nil
	if _, err := PrepareStagedSnapshot(
		testBinding(), testBootstrap(), system,
		UserCollection{Name: "docs", Target: user}, txnLog, options,
		cut, SnapshotArtifactOptions{},
	); !errors.Is(err, ErrStagedSnapshot) {
		t.Fatalf("partial image audit error = %v", err)
	}
}
