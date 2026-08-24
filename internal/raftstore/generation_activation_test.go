package raftstore

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type testGenerationActivationSettler struct {
	calls      int
	completes  int
	fail       error
	activation GenerationActivation
	completion GenerationActivationCompletion
}

func (settler *testGenerationActivationSettler) CompleteGenerationActivation(
	completion GenerationActivationCompletion,
) {
	settler.completes++
	settler.completion = completion
}

func (settler *testGenerationActivationSettler) SettleGenerationActivation(
	activation GenerationActivation,
) error {
	settler.calls++
	settler.activation = activation
	return settler.fail
}

func TestGenerationSelectionSettlesAndRotatesWithConstantNamespace(t *testing.T) {
	path, source, options, firstBuilder := prepareGenerationSource(t)
	firstCandidate, err := firstBuilder.Build()
	if err != nil {
		t.Fatal(err)
	}
	// A caller must not be able to publish a successfully linked candidate
	// while an outcome-unknown Build cleanup alias still pins the full inode.
	firstStagePath := filepath.Join(filepath.Dir(path), firstBuilder.stageBase())
	if err := os.Link(firstCandidate.Path, firstStagePath); err != nil {
		t.Fatal(err)
	}
	contender, _, _, err := firstBuilder.lockGenerationCandidate()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.PublishGenerationSelection(firstBuilder); !errors.Is(err, ErrLocked) {
		t.Fatalf("selection raced a serving candidate = %v", err)
	}
	if _, _, err := source.InitialState(); err != nil {
		t.Fatalf("contended selection fenced source: %v", err)
	}
	if err := contender.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PublishGenerationSelection(firstBuilder); err != nil {
		t.Fatalf("publish generation 1: %v", err)
	}
	if _, err := os.Lstat(firstStagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("selection retained generation stage alias: %v", err)
	}
	if direct, err := Open(
		firstCandidate.Path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	); direct != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("selected sibling direct open = %v, %v", direct, err)
	}
	if _, _, err := source.InitialState(); !errors.Is(err, ErrGenerationActivationPending) {
		t.Fatalf("source remained usable after family selection: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	selected, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options,
	)
	if err != nil {
		t.Fatalf("open selected generation 1: %v", err)
	}
	activation, err := selected.PendingGenerationActivation()
	if err != nil || activation.Info.Generation != 1 ||
		activation.Info.BindingDigest != firstCandidate.Info.BindingDigest ||
		activation.Info.SnapshotBaseDigest != firstCandidate.Info.SnapshotBaseDigest ||
		activation.Snapshot.GetMetadata().GetIndex() != 3 {
		t.Fatalf("pending generation 1 = %+v, %v", activation, err)
	}
	if _, _, err := selected.InitialState(); !errors.Is(err, ErrGenerationActivationPending) {
		t.Fatalf("selected generation served before settlement: %v", err)
	}
	if err := selected.CommitGenerationSelection(nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil settlement authority = %v", err)
	}
	settlementFailure := errors.New("test settlement failure")
	settler := &testGenerationActivationSettler{fail: settlementFailure}
	if err := selected.CommitGenerationSelection(settler); !errors.Is(err, settlementFailure) {
		t.Fatalf("failed settlement = %v", err)
	}
	if settler.calls != 1 || settler.activation.Info.BindingDigest != activation.Info.BindingDigest {
		t.Fatalf("failed settlement input = %+v/%d", settler.activation.Info, settler.calls)
	}
	if _, err := os.Lstat(firstCandidate.Path); err != nil {
		t.Fatalf("failed settlement reclaimed selected candidate: %v", err)
	}
	settler.fail = nil
	if err := selected.CommitGenerationSelection(settler); err != nil {
		t.Fatalf("commit generation 1: %v", err)
	}
	if settler.calls != 2 {
		t.Fatalf("settlement calls = %d, want 2", settler.calls)
	}
	if settler.completes != 1 || !settler.completion.Matches(
		activation.Info.FamilyID,
		activation.Info.Generation,
		activation.Info.BindingDigest,
	) {
		t.Fatalf("completion did not bind activation: %+v", settler)
	}
	if _, _, err := selected.InitialState(); err != nil {
		t.Fatalf("generation 1 remained fenced after commit: %v", err)
	}
	if _, err := os.Lstat(firstCandidate.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("generation 1 candidate name survived logical replacement: %v", err)
	}
	firstInfo, err := selected.GenerationInfo()
	if err != nil || firstInfo.Generation != 1 ||
		firstInfo.ParentBindingDigest != ([32]byte{}) {
		t.Fatalf("active generation 1 = %+v, %v", firstInfo, err)
	}

	if _, err := selected.BeginIncarnation(); err != nil {
		t.Fatal(err)
	}
	secondBuilder, err := selected.PrepareGeneration(testGenerationInput(
		testGenerationSnapshot(4, 3, "checkpoint-four"),
	), testKey())
	if err != nil {
		t.Fatalf("prepare generation 2: %v", err)
	}
	defer secondBuilder.Close()
	secondCandidate, err := secondBuilder.Build()
	if err != nil {
		t.Fatalf("build generation 2: %v", err)
	}
	if secondCandidate.Info.Generation != 2 ||
		secondCandidate.Info.ParentBindingDigest != firstInfo.BindingDigest {
		t.Fatalf("generation 2 lineage = %+v", secondCandidate.Info)
	}
	if _, err := selected.PublishGenerationSelection(secondBuilder); err != nil {
		t.Fatalf("publish generation 2: %v", err)
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	selected, err = Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options,
	)
	if err != nil {
		t.Fatalf("open selected generation 2: %v", err)
	}
	defer selected.Close()
	if pending, err := selected.PendingGenerationActivation(); err != nil ||
		pending.Info.Generation != 2 ||
		pending.Info.ParentBindingDigest != firstInfo.BindingDigest {
		t.Fatalf("pending generation 2 = %+v, %v", pending, err)
	}
	settler = &testGenerationActivationSettler{}
	if err := selected.CommitGenerationSelection(settler); err != nil {
		t.Fatalf("commit generation 2: %v", err)
	}
	secondInfo, err := selected.GenerationInfo()
	if err != nil || secondInfo.Generation != 2 ||
		secondInfo.ParentBindingDigest != firstInfo.BindingDigest {
		t.Fatalf("active generation 2 = %+v, %v", secondInfo, err)
	}
	if _, err := os.Lstat(secondCandidate.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("generation 2 candidate name survived logical replacement: %v", err)
	}

	familyPrefix := generationFamilyPrefix(secondInfo.FamilyID)
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	fullImages := 0
	buildLocks := 0
	manifests := 0
	for _, entry := range entries {
		switch entry.Name() {
		case filepath.Base(path):
			fullImages++
		case familyPrefix + ".build.lock":
			buildLocks++
		case familyPrefix + ".family":
			manifests++
		}
		if entry.Name() == generationCandidateBase(secondInfo.FamilyID, 1) ||
			entry.Name() == generationCandidateBase(secondInfo.FamilyID, 2) ||
			entry.Name() == generationCandidateBase(secondInfo.FamilyID, 1)+".stage" ||
			entry.Name() == generationCandidateBase(secondInfo.FamilyID, 2)+".stage" {
			fullImages++
		}
	}
	if fullImages != 1 || buildLocks != 1 || manifests != 1 {
		t.Fatalf("family namespace images/locks/manifests = %d/%d/%d, entries=%v",
			fullImages, buildLocks, manifests, entries)
	}
}

func TestFamilySlotCodecAuthenticatesEveryByte(t *testing.T) {
	identity := testIdentity()
	familyID := generationFamilyID("member.wal", identity)
	identityDigest := generationIdentityDigest(identity)
	key := familyManifestKey(testKey(), familyID, identityDigest)
	state := familyState{
		slotGeneration: 2, phase: familyPhaseSelecting,
		familyID: familyID, identityDigest: identityDigest,
		activeGeneration: 1, activeFileID: [16]byte{1},
		activeHeaderDigest: [32]byte{2}, activeBindingDigest: [32]byte{3},
		sourceFileID: [16]byte{4}, sourceCutDigest: [32]byte{5},
		snapshotBaseDigest: [32]byte{6}, retentionCommitment: [32]byte{7},
	}
	encoded, err := marshalFamilySlot(state, 0, key)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := unmarshalFamilySlot(encoded[:], 0, familyID, identityDigest, key)
	if err != nil || decoded.absent || decoded.state != state {
		t.Fatalf("family slot round trip = %+v, %v", decoded, err)
	}
	for offset := range encoded {
		damaged := encoded
		damaged[offset] ^= 0x80
		if _, err := unmarshalFamilySlot(
			damaged[:], 0, familyID, identityDigest, key,
		); err == nil {
			t.Fatalf("family slot accepted damage at byte %d", offset)
		}
	}
}

func TestTornFamilySlotRecoversLatestButRequiresQuarantine(t *testing.T) {
	path, source, options, builder := prepareGenerationSource(t)
	defer builder.Close()
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PublishGenerationSelection(builder); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	selected, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.CommitGenerationSelection(&testGenerationActivationSettler{}); err != nil {
		t.Fatal(err)
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}

	manifestPath := filepath.Join(
		filepath.Dir(path), familyManifestBase(builder.familyID),
	)
	manifest, err := os.OpenFile(manifestPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	if _, err := manifest.ReadAt(first[:], 0); err != nil {
		_ = manifest.Close()
		t.Fatal(err)
	}
	first[0] ^= 0x80
	if _, err := manifest.WriteAt(first[:], 0); err != nil {
		_ = manifest.Close()
		t.Fatal(err)
	}
	if err := manifest.Sync(); err != nil {
		_ = manifest.Close()
		t.Fatal(err)
	}
	if err := manifest.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if !recovered.RecoveredTornFamilySlot() {
		t.Fatal("authenticated fallback did not report torn family peer slot")
	}
	if _, _, err := recovered.InitialState(); !errors.Is(err, ErrGenerationFamilyQuarantined) {
		t.Fatalf("torn family served recovered state: %v", err)
	}
}

func TestPublishReturnsIdentityWithOutcomeUnknownFamilySlot(t *testing.T) {
	path, source, options, builder := prepareGenerationSource(t)
	defer builder.Close()
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected selecting family Sync failure")
	originalSync := source.options.ops.sync
	source.options.ops.sync = func(*os.File) error { return injected }
	identity, err := source.PublishGenerationSelection(builder)
	if !errors.Is(err, injected) || !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("outcome-unknown selection = %v", err)
	}
	if !identity.Valid() || identity.FamilyID != candidate.Info.FamilyID ||
		identity.Generation != candidate.Info.Generation ||
		identity.BindingDigest != candidate.Info.BindingDigest {
		t.Fatalf("outcome-unknown selection identity = %+v, candidate = %+v",
			identity, candidate.Info)
	}
	// No getter call is needed between Publish and Close; the returned identity
	// remains the exact conservative SQL fence even after the Store is gone.
	source.options.ops.sync = originalSync
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	selected, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Close()
	activation, err := selected.PendingGenerationActivation()
	if err != nil || activation.Info.FamilyID != identity.FamilyID ||
		activation.Info.Generation != identity.Generation ||
		activation.Info.BindingDigest != identity.BindingDigest {
		t.Fatalf("recovered selection identity = %+v, %v", activation.Info, err)
	}
	if err := selected.CommitGenerationSelection(
		&testGenerationActivationSettler{},
	); err != nil {
		t.Fatal(err)
	}
}

func TestPublishRejectsForeignGenerationStage(t *testing.T) {
	_, source, _, builder := prepareGenerationSource(t)
	defer builder.Close()
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	stagePath := filepath.Join(builder.parentPath, builder.stageBase())
	foreign, err := os.OpenFile(stagePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := foreign.Close(); err != nil {
		t.Fatal(err)
	}
	identity, err := source.PublishGenerationSelection(builder)
	if identity.Valid() || !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("foreign stage publication = %+v, %v", identity, err)
	}
	if _, _, err := source.InitialState(); err != nil {
		t.Fatalf("foreign stage fenced source: %v", err)
	}
}

func TestLostSelectingSlotCanNeverServeRetiredSource(t *testing.T) {
	path, source, options, builder := prepareGenerationSource(t)
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PublishGenerationSelection(builder); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(
		filepath.Dir(path), familyManifestBase(builder.familyID),
	)
	manifest, err := os.OpenFile(manifestPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	zero := make([]byte, familySlotBytes)
	if _, err := manifest.WriteAt(zero, 0); err != nil {
		_ = manifest.Close()
		t.Fatal(err)
	}
	if err := manifest.Sync(); err != nil {
		_ = manifest.Close()
		t.Fatal(err)
	}
	if err := manifest.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if !recovered.RecoveredTornFamilySlot() {
		t.Fatal("lost selecting slot was not quarantined")
	}
	if _, _, err := recovered.InitialState(); !errors.Is(err, ErrGenerationFamilyQuarantined) {
		t.Fatalf("retired source served after selecting-slot loss: %v", err)
	}
}

func TestCommitGenerationRetriesDirectoryBarrierBeforeActive(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same_handle"
		if restart {
			name = "reopen"
		}
		t.Run(name, func(t *testing.T) {
			path, source, options, builder := prepareGenerationSource(t)
			defer builder.Close()
			candidate, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := source.PublishGenerationSelection(builder); err != nil {
				t.Fatal(err)
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			selected, err := Open(
				path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
				testKey(), options,
			)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected directory sync failure")
			originalSync := selected.options.ops.syncDirectory
			syncCalls := 0
			selected.options.ops.syncDirectory = func(root *os.Root) error {
				syncCalls++
				if syncCalls == 1 {
					return injected
				}
				return originalSync(root)
			}
			settler := &testGenerationActivationSettler{}
			if err := selected.CommitGenerationSelection(settler); !errors.Is(err, injected) ||
				!errors.Is(err, ErrPersistenceUnknown) {
				t.Fatalf("directory barrier failure = %v", err)
			}
			if selected.family.state.phase != familyPhaseSelecting ||
				selected.base != selected.logicalBase || syncCalls != 1 || settler.completes != 0 {
				t.Fatalf("failed barrier state = phase %d base %q logical %q calls %d",
					selected.family.state.phase, selected.base, selected.logicalBase, syncCalls)
			}
			if _, err := os.Lstat(candidate.Path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rename did not land before injected barrier: %v", err)
			}
			if restart {
				if err := selected.Close(); err != nil {
					t.Fatal(err)
				}
				selected, err = Open(
					path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
					testKey(), options,
				)
				if err != nil {
					t.Fatal(err)
				}
				originalSync = selected.options.ops.syncDirectory
				selected.options.ops.syncDirectory = func(root *os.Root) error {
					syncCalls++
					return originalSync(root)
				}
			}
			defer selected.Close()
			if err := selected.CommitGenerationSelection(settler); err != nil {
				t.Fatalf("retry directory barrier: %v", err)
			}
			if syncCalls != 2 || selected.family.state.phase != familyPhaseActive ||
				selected.activationPending || settler.completes != 1 {
				t.Fatalf("settled barrier state = calls %d phase %d pending %t",
					syncCalls, selected.family.state.phase, selected.activationPending)
			}
		})
	}
}

func TestCommitGenerationRejectsAdvancedRetiringSourceCurrent(t *testing.T) {
	path, source, options, builder := prepareGenerationSource(t)
	defer builder.Close()
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PublishGenerationSelection(builder); err != nil {
		t.Fatal(err)
	}
	advanced := source.current
	advanced.activeSlot = 1 - advanced.activeSlot
	advanced.generation++
	advanced.currentIncarnation++
	advanced.retryPresent = false
	advanced.retry = retryKey{}
	advanced.retryDigest = [32]byte{}
	encoded, _, err := marshalCurrentSlot(advanced, advanced.activeSlot, source.header)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeExactAt(
		source.options.ops, source.file, encoded,
		int64(StaticHeaderBytes+advanced.activeSlot*CurrentSlotBytes),
	); err != nil {
		t.Fatal(err)
	}
	if err := source.options.ops.sync(source.file); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	selected, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Close()
	settler := &testGenerationActivationSettler{}
	if err := selected.CommitGenerationSelection(settler); !errors.Is(
		err, ErrGenerationSource,
	) {
		t.Fatalf("advanced retiring source commit = %v", err)
	}
	if selected.family.state.phase != familyPhaseSelecting ||
		!selected.activationPending || settler.completes != 0 {
		t.Fatalf("advanced source changed authority: phase=%d pending=%t completes=%d",
			selected.family.state.phase, selected.activationPending, settler.completes)
	}
	logical, logicalErr := os.Lstat(path)
	candidate, candidateErr := os.Lstat(builder.candidatePath)
	if logicalErr != nil || candidateErr != nil || os.SameFile(logical, candidate) {
		t.Fatalf("advanced source was replaced: logical=%v candidate=%v",
			logicalErr, candidateErr)
	}
}

func TestCommitGenerationRetainsFenceUntilPostAuthorityNamespaceProof(t *testing.T) {
	path, source, options, builder := prepareGenerationSource(t)
	defer builder.Close()
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PublishGenerationSelection(builder); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	selected, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Close()
	heldBase := filepath.Base(path) + ".held-after-active"
	originalSync := selected.options.ops.sync
	renamed := false
	selected.options.ops.sync = func(file *os.File) error {
		if err := originalSync(file); err != nil {
			return err
		}
		if file == selected.family.file && !renamed {
			if err := selected.root.Rename(selected.logicalBase, heldBase); err != nil {
				return err
			}
			renamed = true
		}
		return nil
	}
	settler := &testGenerationActivationSettler{}
	if err := selected.CommitGenerationSelection(settler); !errors.Is(
		err, ErrPersistenceUnknown,
	) || !errors.Is(err, ErrNamespaceChanged) {
		t.Fatalf("post-authority namespace proof = %v", err)
	}
	if !renamed || selected.family.state.phase != familyPhaseActive ||
		!selected.activationPending || settler.calls != 1 || settler.completes != 0 {
		t.Fatalf(
			"failed final proof state: renamed=%t phase=%d pending=%t settles=%d completes=%d",
			renamed, selected.family.state.phase, selected.activationPending,
			settler.calls, settler.completes,
		)
	}
	if _, err := selected.PendingGenerationActivation(); !errors.Is(
		err, ErrGenerationActivationPending,
	) {
		t.Fatalf("active authority exposed selecting payload = %v", err)
	}

	if err := selected.root.Rename(heldBase, selected.logicalBase); err != nil {
		t.Fatal(err)
	}
	if err := syncPinnedDirectory(selected.root); err != nil {
		t.Fatal(err)
	}
	selected.options.ops.sync = originalSync
	if err := selected.CommitGenerationSelection(settler); err != nil {
		t.Fatalf("retry final namespace proof: %v", err)
	}
	if selected.activationPending || settler.calls != 1 || settler.completes != 1 {
		t.Fatalf(
			"retried final proof repeated settlement: pending=%t settles=%d completes=%d",
			selected.activationPending, settler.calls, settler.completes,
		)
	}
}

func TestFamilyManifestAbsentPeerIsTornAfterInitialPublication(t *testing.T) {
	identity := testIdentity()
	familyID := generationFamilyID("member.wal", identity)
	identityDigest := generationIdentityDigest(identity)
	manifestKey := familyManifestKey(testKey(), familyID, identityDigest)
	base := familyState{
		familyID: familyID, identityDigest: identityDigest,
		activeFileID: [16]byte{1}, activeHeaderDigest: [32]byte{2},
	}
	tests := []struct {
		name  string
		slot  uint8
		state familyState
		torn  bool
	}{
		{name: "initial", slot: 0, state: func() familyState {
			state := base
			state.slotGeneration = 1
			state.phase = familyPhaseSource
			return state
		}(), torn: true},
		{name: "active_peer_erased", slot: 1, state: func() familyState {
			state := base
			state.slotGeneration = 3
			state.phase = familyPhaseActive
			state.activeGeneration = 1
			state.activeBindingDigest = [32]byte{3}
			state.sourceFileID = [16]byte{4}
			state.sourceCutDigest = [32]byte{5}
			state.snapshotBaseDigest = [32]byte{6}
			state.retentionCommitment = [32]byte{7}
			return state
		}(), torn: true},
		{name: "later_selecting_peer_erased", slot: 0, state: func() familyState {
			state := base
			state.slotGeneration = 4
			state.phase = familyPhaseSelecting
			state.activeGeneration = 2
			state.activeBindingDigest = [32]byte{3}
			state.parentBindingDigest = [32]byte{8}
			state.sourceFileID = [16]byte{4}
			state.sourceCutDigest = [32]byte{5}
			state.snapshotBaseDigest = [32]byte{6}
			state.retentionCommitment = [32]byte{7}
			return state
		}(), torn: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := marshalFamilySlot(test.state, test.slot, manifestKey)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.CreateTemp(t.TempDir(), "family-slot-")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if err := file.Truncate(familyManifestBytes); err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt(encoded[:], int64(test.slot)*familySlotBytes); err != nil {
				t.Fatal(err)
			}
			state, slot, torn, err := readFamilyManifest(
				file, familyID, identityDigest, manifestKey,
			)
			if err != nil || state != test.state || slot != test.slot || torn != test.torn {
				t.Fatalf("read family = state %+v slot %d torn %t err %v",
					state, slot, torn, err)
			}
		})
	}
}

func TestFamilyStateRejectsCrossTimelineSlots(t *testing.T) {
	familyID := generationFamilyID("member.wal", testIdentity())
	identityDigest := generationIdentityDigest(testIdentity())
	source := familyState{
		slotGeneration: 1, phase: familyPhaseSource,
		familyID: familyID, identityDigest: identityDigest,
		activeFileID: [16]byte{1}, activeHeaderDigest: [32]byte{2},
	}
	selecting := familyState{
		slotGeneration: 2, phase: familyPhaseSelecting,
		familyID: familyID, identityDigest: identityDigest,
		activeGeneration: 1, activeFileID: [16]byte{3},
		activeHeaderDigest: [32]byte{4}, activeBindingDigest: [32]byte{5},
		sourceFileID: [16]byte{1}, sourceCutDigest: [32]byte{6},
		snapshotBaseDigest: [32]byte{7}, retentionCommitment: [32]byte{8},
	}
	active := selecting
	active.slotGeneration = 3
	active.phase = familyPhaseActive
	nextSelecting := familyState{
		slotGeneration: 4, phase: familyPhaseSelecting,
		familyID: familyID, identityDigest: identityDigest,
		activeGeneration: 2, activeFileID: [16]byte{9},
		activeHeaderDigest: [32]byte{10}, activeBindingDigest: [32]byte{11},
		parentBindingDigest: active.activeBindingDigest,
		sourceFileID:        active.activeFileID, sourceCutDigest: [32]byte{12},
		snapshotBaseDigest: [32]byte{13}, retentionCommitment: [32]byte{14},
	}
	tests := []struct {
		name     string
		previous familyState
		next     familyState
	}{
		{name: "source_file", previous: source, next: func() familyState {
			state := selecting
			state.sourceFileID[0]++
			return state
		}()},
		{name: "skipped_generation", previous: active, next: func() familyState {
			state := nextSelecting
			state.activeGeneration++
			return state
		}()},
		{name: "changed_selected_authority", previous: selecting, next: func() familyState {
			state := active
			state.activeBindingDigest[0]++
			return state
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !validFamilyState(test.previous) || !validFamilyState(test.next) {
				t.Fatal("test constructed a noncanonical standalone slot")
			}
			if _, _, err := selectFamilyState([familySlotCount]decodedFamilySlot{
				{state: test.previous}, {state: test.next},
			}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("cross-timeline slots = %v", err)
			}
		})
	}
}

func TestCreateResumesExactWALAfterFamilyPublicationFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	failing := testOptions()
	injected := errors.New("injected family preallocation failure")
	originalPreallocate := failing.ops.preallocate
	failing.ops.preallocate = func(file *os.File, size int64) error {
		if size == familyManifestBytes {
			return injected
		}
		return originalPreallocate(file, size)
	}
	if store, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), failing,
	); store != nil || !errors.Is(err, injected) || !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("failed family publication = %v, %v", store, err)
	}
	if _, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), testOptions(),
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("orphan WAL served without mandatory family = %v", err)
	}
	resumed, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), testOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	if _, _, err := resumed.InitialState(); err != nil {
		t.Fatalf("resumed exact source = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(
		filepath.Dir(path), walCreateStageBase(filepath.Base(path)),
	)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("WAL creation stage survived resume: %v", err)
	}
}

func TestCreateReclaimsOneDeterministicPrepublicationStage(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "raft.wal")
	stagePath := filepath.Join(directory, walCreateStageBase(filepath.Base(path)))
	if err := os.WriteFile(stagePath, []byte("interrupted-create"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), testOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deterministic creation stage survived retry: %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".tmp" || filepath.Ext(entry.Name()) == ".stage" {
			t.Fatalf("unbounded creation debris %q", entry.Name())
		}
	}
}

func TestCreateSettlesAbsentStageNamespaceBeforeNameReuse(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "raft.wal")
	stagePath := filepath.Join(directory, walCreateStageBase(filepath.Base(path)))
	injected := errors.New("injected pre-reuse creation namespace barrier")
	failing := testOptions()
	syncs := 0
	failing.ops.syncDirectory = func(*os.Root) error {
		syncs++
		return injected
	}
	if store, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), failing,
	); store != nil || !errors.Is(err, injected) ||
		!errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("Create before absent-stage barrier = %v, %v", store, err)
	}
	if syncs != 1 {
		t.Fatalf("absent creation-stage barriers = %d, want 1", syncs)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed namespace settlement created logical WAL: %v", err)
	}

	// Model the directory state recovered after attempt A's unlink was visible
	// in memory but its failed barrier did not make that absence durable.
	if err := os.WriteFile(stagePath, []byte("resurrected-create-stage"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), testOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resurrected creation stage survived settled retry: %v", err)
	}
	if _, _, err := store.InitialState(); err != nil {
		t.Fatalf("settled Create did not serve: %v", err)
	}
}

func TestCreateSettlesAbsentFamilyStageNamespaceBeforeNameReuse(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "raft.wal")
	familyID := generationFamilyID(filepath.Base(path), testIdentity())
	familyPath := filepath.Join(directory, familyManifestBase(familyID))
	familyStagePath := familyPath + ".stage"

	// Leave the exact pristine official WAL behind without its mandatory family,
	// as an outcome-unknown first Create may do before family publication.
	orphaning := testOptions()
	originalPreallocate := orphaning.ops.preallocate
	orphanFailure := errors.New("injected initial family construction failure")
	orphaning.ops.preallocate = func(file *os.File, size int64) error {
		if size == familyManifestBytes {
			return orphanFailure
		}
		return originalPreallocate(file, size)
	}
	if store, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), orphaning,
	); store != nil || !errors.Is(err, orphanFailure) ||
		!errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("orphan pristine WAL = %v, %v", store, err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("pristine WAL was not published: %v", err)
	}
	if _, err := os.Lstat(familyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed construction published family: %v", err)
	}

	injected := errors.New("injected pre-reuse family namespace barrier")
	failing := testOptions()
	syncs := 0
	failing.ops.syncDirectory = func(*os.Root) error {
		syncs++
		return injected
	}
	if store, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), failing,
	); store != nil || !errors.Is(err, injected) ||
		!errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("Create before absent-family-stage barrier = %v, %v", store, err)
	}
	if syncs != 1 {
		t.Fatalf("absent family-stage barriers = %d, want 1", syncs)
	}

	// Model the old deterministic stage name resurrecting after that failed
	// barrier. The next Create must settle the parent before inspecting/removing
	// the name and publishing a different family inode.
	if err := os.WriteFile(
		familyStagePath, []byte("resurrected-family-stage"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	store, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), testOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := os.Lstat(familyStagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resurrected family stage survived settled retry: %v", err)
	}
	if family, err := os.Lstat(familyPath); err != nil ||
		!family.Mode().IsRegular() || family.Size() != familyManifestBytes {
		t.Fatalf("settled family publication = %+v, %v", family, err)
	}
	if _, _, err := store.InitialState(); err != nil {
		t.Fatalf("settled family Create did not serve: %v", err)
	}
}

func TestWrongIdentityCreateChurnUsesOnePathCreationNamespace(t *testing.T) {
	path, source, _ := createTestStore(t)
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	for attempt := byte(1); attempt <= 64; attempt++ {
		foreign := testIdentity()
		foreign.ClusterID[0] ^= attempt
		created, err := Create(
			path, foreign, testKey(), testBootstrap(), testOptions(),
		)
		if created != nil || !errors.Is(err, ErrIdentityMismatch) {
			t.Fatalf("foreign identity attempt %d = %v, %v", attempt, created, err)
		}
	}

	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	wantCreateLock := walCreateLockBase(filepath.Base(path))
	createLocks := 0
	families := 0
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case name == wantCreateLock:
			createLocks++
		case strings.HasSuffix(name, ".create.lock"):
			t.Fatalf("foreign identity created a second path lease %q", name)
		case strings.HasSuffix(name, ".create.stage"):
			t.Fatalf("foreign identity retained creation payload %q", name)
		case strings.HasSuffix(name, ".build.lock"):
			t.Fatalf("foreign identity entered generation-build namespace %q", name)
		case strings.HasSuffix(name, ".family"):
			families++
		}
	}
	if createLocks != 1 || families != 1 {
		t.Fatalf("bounded creation namespace: create locks=%d families=%d entries=%v",
			createLocks, families, entries)
	}
}

func TestOrdinaryOpenSettlesUnknownFamilyLinkBarrier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	familyID := generationFamilyID(filepath.Base(path), testIdentity())
	familyBase := familyManifestBase(familyID)
	failing := testOptions()
	injected := errors.New("injected family directory barrier failure")
	faulted := false
	failing.ops.syncDirectory = func(root *os.Root) error {
		family, familyErr := root.Lstat(familyBase)
		stage, stageErr := root.Lstat(familyBase + ".stage")
		if !faulted && familyErr == nil && stageErr == nil &&
			family.Mode().IsRegular() && stage.Mode().IsRegular() &&
			os.SameFile(family, stage) {
			faulted = true
			return injected
		}
		return syncPinnedDirectory(root)
	}
	if store, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), failing,
	); store != nil || !errors.Is(err, injected) || !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("unknown family link = %v, %v", store, err)
	}
	if !faulted {
		t.Fatal("family link publication barrier was not reached")
	}
	stagePath := filepath.Join(
		filepath.Dir(path), familyBase+".stage",
	)
	if _, err := os.Lstat(stagePath); err != nil {
		t.Fatalf("unknown publication lost stage witness: %v", err)
	}
	openOptions := testOptions()
	settlementSyncs := 0
	openOptions.ops.syncDirectory = func(root *os.Root) error {
		settlementSyncs++
		return syncPinnedDirectory(root)
	}
	reopened, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), openOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if settlementSyncs != 2 {
		t.Fatalf("ordinary Open family settlement Syncs = %d, want 2", settlementSyncs)
	}
	if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("family stage survived Open settlement: %v", err)
	}
}

func TestOrdinaryOpenSettlesFamilyStageAndRetainsSourceWitness(t *testing.T) {
	path, store, _ := createTestStore(t)
	familyID := store.family.state.familyID
	familyPath := filepath.Join(filepath.Dir(path), familyManifestBase(familyID))
	walStagePath := filepath.Join(filepath.Dir(path), walCreateStageBase(filepath.Base(path)))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, walStagePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(familyPath); err != nil {
		t.Fatal(err)
	}
	failing := testOptions()
	injected := errors.New("injected resumed family publication barrier")
	familyBase := filepath.Base(familyPath)
	faulted := false
	failing.ops.syncDirectory = func(root *os.Root) error {
		family, familyErr := root.Lstat(familyBase)
		stage, stageErr := root.Lstat(familyBase + ".stage")
		if !faulted && familyErr == nil && stageErr == nil &&
			family.Mode().IsRegular() && stage.Mode().IsRegular() &&
			os.SameFile(family, stage) {
			faulted = true
			return injected
		}
		return syncPinnedDirectory(root)
	}
	if resumed, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), failing,
	); resumed != nil || !errors.Is(err, injected) || !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("resumed family publication failure = %v, %v", resumed, err)
	}
	if !faulted {
		t.Fatal("resumed family link publication barrier was not reached")
	}
	familyStagePath := familyPath + ".stage"
	for _, stage := range []string{walStagePath, familyStagePath} {
		if _, err := os.Lstat(stage); err != nil {
			t.Fatalf("missing outcome-unknown stage %q: %v", stage, err)
		}
	}
	reopened, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), testOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := os.Lstat(familyStagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open retained family publication stage: %v", err)
	}
	if stage, err := os.Lstat(walStagePath); err != nil ||
		!os.SameFile(stage, reopened.fileInfo) {
		t.Fatalf("Open lost bounded source witness: %v", err)
	}
}

func TestOrdinarySourceOpenDefersCreateAliasBarrierUntilSelection(t *testing.T) {
	path, source, _ := createTestStore(t)
	walStagePath := filepath.Join(filepath.Dir(path), walCreateStageBase(filepath.Base(path)))
	if err := os.Link(path, walStagePath); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	openOptions := testOptions()
	syncs := 0
	openOptions.ops.syncDirectory = func(*os.Root) error {
		syncs++
		return errors.New("ordinary source Open must not Sync the parent")
	}
	reopened, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), openOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if syncs != 0 {
		t.Fatalf("ordinary source Open directory Syncs = %d, want 0", syncs)
	}
	if stage, err := os.Lstat(walStagePath); err != nil ||
		!os.SameFile(stage, reopened.fileInfo) {
		t.Fatalf("source Open lost bounded create witness: %v", err)
	}

	reopened.options.ops.syncDirectory = syncPinnedDirectory
	if _, err := reopened.BeginIncarnation(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	builder, err := reopened.PrepareGeneration(testGenerationInput(snapshot), testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	candidate, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := reopened.PublishGenerationSelection(builder)
	if err != nil || !identity.Valid() {
		t.Fatalf("selection after bounded witness = %+v, %v", identity, err)
	}
	if _, err := os.Lstat(walStagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("selection retained create alias: %v", err)
	}
	if identity.BindingDigest != candidate.Info.BindingDigest {
		t.Fatalf("selection identity = %+v, candidate = %+v", identity, candidate.Info)
	}
	// Any creation alias appearing after source->selecting is namespace
	// mutation; it cannot be a legitimate recovery witness anymore.
	if err := os.Link(path, walStagePath); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if selected, err := Open(
		path, testIdentity(), testBootstrap().TopologyRecoveryEpoch,
		testKey(), testOptions(),
	); selected != nil || !errors.Is(err, ErrNamespaceChanged) {
		t.Fatalf("selected Open accepted resurrected creation alias = %v, %v", selected, err)
	}
}

func TestCreateNeverAdoptsAdvancedSourceWithoutFamily(t *testing.T) {
	path, store, options := createTestStore(t)
	if _, err := store.BeginIncarnation(); err != nil {
		t.Fatal(err)
	}
	familyPath := filepath.Join(
		filepath.Dir(path), familyManifestBase(store.family.state.familyID),
	)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(familyPath); err != nil {
		t.Fatal(err)
	}
	if resumed, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), options,
	); resumed != nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("Create adopted advanced source = %v, %v", resumed, err)
	}
}

func TestCreateReleasesPathLeaseBeforeOpeningOfficialFamily(t *testing.T) {
	path, source, _, builder := prepareGenerationSource(t)
	defer builder.Close()
	if contender, err := Create(
		path, testIdentity(), testKey(), testBootstrap(), testOptions(),
	); contender != nil || !errors.Is(err, ErrLocked) {
		t.Fatalf("Create beside live official family = %v, %v", contender, err)
	}
	lease, err := acquireWALCreateLease(
		source.root, source.directoryInfo, source.logicalBase,
	)
	if err != nil {
		t.Fatalf("Create retained path lease: %v", err)
	}
	if err := lease.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(); err != nil {
		t.Fatalf("Create disturbed generation build lease: %v", err)
	}
	if _, _, err := source.InitialState(); err != nil {
		t.Fatalf("Create disturbed live family owner: %v", err)
	}
}
