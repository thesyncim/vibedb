package snapshottransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type abandonmentAuthority struct {
	witnesses map[[32]byte]ArtifactAbandonmentWitness
	err       error
}

func (authority abandonmentAuthority) ReadArtifactAbandonment(
	_ context.Context, operation [32]byte,
) (ArtifactAbandonmentWitness, bool, error) {
	witness, found := authority.witnesses[operation]
	return witness, found, authority.err
}

func testAbandonmentWitness(request SourceControlRequest, descriptor Descriptor) ArtifactAbandonmentWitness {
	return ArtifactAbandonmentWitness{
		Operation: request.Operation, Artifact: descriptor.ArtifactHash,
		TargetStore: descriptor.TargetStore, TargetIncarnation: descriptor.TargetIncarnation,
		SchemaGeneration: descriptor.SchemaGeneration, ReplicaSetVersion: descriptor.ReplicaSetVersion,
		Owner: request.SourceNode, OwnerEpoch: 7, LeaseRevision: 8,
		LeaseAppliedThrough: 90, AbandonedAppliedThrough: 91, AuthorityRevision: 9,
		Descriptor: descriptor,
	}
}

func TestAbandonmentWitnessBindsExactIdentityAndExpiredOwnerLease(t *testing.T) {
	request, descriptor := sourceControlFixture()
	witness := testAbandonmentWitness(request, descriptor)
	if !witness.Valid() {
		t.Fatal("fixture witness is invalid")
	}
	mutations := map[string]func(*ArtifactAbandonmentWitness){
		"artifact":           func(w *ArtifactAbandonmentWitness) { w.Artifact[0]++ },
		"store":              func(w *ArtifactAbandonmentWitness) { w.TargetStore[0]++ },
		"incarnation":        func(w *ArtifactAbandonmentWitness) { w.TargetIncarnation++ },
		"schema generation":  func(w *ArtifactAbandonmentWitness) { w.SchemaGeneration++ },
		"replica generation": func(w *ArtifactAbandonmentWitness) { w.ReplicaSetVersion++ },
		"owner":              func(w *ArtifactAbandonmentWitness) { w.Owner = [16]byte{} },
		"owner epoch":        func(w *ArtifactAbandonmentWitness) { w.OwnerEpoch = 0 },
		"lease revision":     func(w *ArtifactAbandonmentWitness) { w.LeaseRevision = 0 },
		"lease cut":          func(w *ArtifactAbandonmentWitness) { w.AbandonedAppliedThrough = w.LeaseAppliedThrough },
		"authority revision": func(w *ArtifactAbandonmentWitness) { w.AuthorityRevision = 0 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := witness
			mutate(&changed)
			if changed.Valid() {
				t.Fatal("changed witness remained valid")
			}
		})
	}
}

func TestAbandonmentWitnessCanonicalFixedGrammar(t *testing.T) {
	request, descriptor := sourceControlFixture()
	witness := testAbandonmentWitness(request, descriptor)
	raw, err := AppendAbandonmentWitness(nil, witness)
	if err != nil || len(raw) != AbandonmentWitnessBytes {
		t.Fatalf("append bytes=%d err=%v", len(raw), err)
	}
	opened, err := OpenAbandonmentWitness(raw)
	if err != nil || opened != witness {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	for _, changed := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, err = OpenAbandonmentWitness(changed); !errors.Is(err, ErrAbandonment) {
			t.Fatalf("noncanonical length accepted: %v", err)
		}
	}
	corrupt := bytes.Clone(raw)
	corrupt[40]++
	if _, err = OpenAbandonmentWitness(corrupt); !errors.Is(err, ErrAbandonment) {
		t.Fatalf("mismatched artifact accepted: %v", err)
	}
}

func TestAbandonmentCollectorProtectsSlowTransferAndRetiresExactStage(t *testing.T) {
	request, descriptor := sourceControlFixture()
	root := t.TempDir()
	journal, err := OpenSourceFileJournal(filepath.Join(root, "journal"), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	repository := openTestRepository(t, filepath.Join(root, "repo"))
	running := SourceControlRecord{Request: request, Revision: 1, State: SourceControlRunning}
	if err = journal.PublishSourceExport(t.Context(), 0, running); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("source-control"), 400)
	chunk := payload[:MinChunkBytes]
	if _, _, err = repository.Append(descriptor, 0, chunk, sha256Sum(chunk)); err != nil {
		t.Fatal(err)
	}

	limits := AbandonmentCollectorOptions{Journal: journal, Repository: repository,
		MaxRecords: 1, MaxReclaimedBytes: (1 << 20) + DescriptorBytes + cursorBytes,
		MaxRetainedBytes: 0}
	limits.Authority = abandonmentAuthority{witnesses: map[[32]byte]ArtifactAbandonmentWitness{}}
	collector, err := NewAbandonmentCollector(limits)
	if err != nil {
		t.Fatal(err)
	}
	pass, err := collector.RunPass(t.Context(), SourceExportCursor{})
	if !errors.Is(err, ErrRetainedBytes) || pass.Deleted != 0 || repository.Stats().Staged != 1 {
		t.Fatalf("slow transfer pass=%+v err=%v stats=%+v", pass, err, repository.Stats())
	}

	witness := testAbandonmentWitness(request, descriptor)
	witness.AbandonedAppliedThrough = witness.LeaseAppliedThrough
	limits.Authority = abandonmentAuthority{witnesses: map[[32]byte]ArtifactAbandonmentWitness{request.Operation: witness}}
	collector, err = NewAbandonmentCollector(limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = collector.RunPass(t.Context(), SourceExportCursor{}); !errors.Is(err, ErrAbandonment) {
		t.Fatalf("unexpired lease err=%v", err)
	}
	invalidPass, _ := collector.RunPass(t.Context(), SourceExportCursor{})
	if invalidPass.Cursor != (SourceExportCursor{}) {
		t.Fatalf("failed witness advanced resumable cursor: %+v", invalidPass.Cursor)
	}
	if repository.Stats().Staged != 1 {
		t.Fatal("invalid witness deleted slow transfer")
	}

	witness.AbandonedAppliedThrough++
	limits.Authority = abandonmentAuthority{witnesses: map[[32]byte]ArtifactAbandonmentWitness{request.Operation: witness}}
	collector, err = NewAbandonmentCollector(limits)
	if err != nil {
		t.Fatal(err)
	}
	pass, err = collector.RunPass(t.Context(), SourceExportCursor{})
	if err != nil || pass.Deleted != 1 || pass.ReclaimedBytes == 0 || pass.RetainedBytes != 0 {
		t.Fatalf("collected pass=%+v err=%v", pass, err)
	}
	record, err := journal.ReadSourceExport(t.Context(), request.Operation)
	if err != nil || record.State != SourceControlReleased || record.Descriptor != descriptor {
		t.Fatalf("retired record=%+v err=%v", record, err)
	}
	pass, err = collector.RunPass(t.Context(), SourceExportCursor{})
	if err != nil || pass.Deleted != 0 || pass.RetainedBytes != 0 {
		t.Fatalf("idempotent pass=%+v err=%v", pass, err)
	}
}

func TestAbandonmentCollectorCursorIsOrderedBoundedAndResumable(t *testing.T) {
	request, _ := sourceControlFixture()
	journal, err := OpenSourceFileJournal(filepath.Join(t.TempDir(), "journal"), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	for _, first := range []byte{3, 1, 2} {
		next := request
		next.Operation[0], next.Step[0] = first, first
		if err = journal.PublishSourceExport(t.Context(), 0, SourceControlRecord{
			Request: next, Revision: 1, State: SourceControlRunning,
		}); err != nil {
			t.Fatal(err)
		}
	}
	var cursor SourceExportCursor
	for want := byte(1); want <= 3; want++ {
		records, next, scanErr := journal.ScanSourceExports(t.Context(), cursor,
			make([]SourceControlRecord, 0, 1))
		if scanErr != nil || len(records) != 1 || records[0].Request.Operation[0] != want {
			t.Fatalf("scan want=%d records=%+v cursor=%+v err=%v", want, records, next, scanErr)
		}
		cursor = next
	}
	if !cursor.Done {
		t.Fatal("final cursor is not done")
	}
}

func TestRepositoryRecoversEveryAbandonmentNamespacePhase(t *testing.T) {
	for _, published := range []bool{false, true} {
		for _, phase := range []repositoryFault{faultAfterAbandonRename, faultAfterAbandonUnlink, faultAfterAbandonSync} {
			t.Run(map[bool]string{false: "staged", true: "published"}[published]+string(rune('0'+phase)), func(t *testing.T) {
				payload := bytes.Repeat([]byte{byte(phase)}, MinChunkBytes*2)
				descriptor := testDescriptor(payload)
				request := SourceControlRequest{Operation: [32]byte{1}, Step: [32]byte{2}, Group: descriptor.Group,
					SourceMember: descriptor.SourceMember, TargetMember: descriptor.TargetMember,
					TargetStore: descriptor.TargetStore, TargetIncarnation: descriptor.TargetIncarnation,
					ReplicaSetVersion: descriptor.ReplicaSetVersion, SourceNode: [16]byte{3}}
				witness := testAbandonmentWitness(request, descriptor)
				path := filepath.Join(t.TempDir(), "repo")
				limits := Limits{MaxArtifacts: 1, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 2 << 20}
				verify := func(*os.File, Descriptor) error { return nil }
				repository, openErr := openRepository(path, limits, verify)
				if openErr != nil {
					t.Fatal(openErr)
				}
				if published {
					appendAll(t, repository, descriptor, payload, 0)
				} else {
					chunk := payload[:MinChunkBytes]
					if _, _, openErr = repository.Append(descriptor, 0, chunk, sha256Sum(chunk)); openErr != nil {
						t.Fatal(openErr)
					}
				}
				repository.fault = func(got repositoryFault) error {
					if got == phase {
						return errors.New("injected abandonment crash")
					}
					return nil
				}
				if _, openErr = repository.AbandonArtifact(witness); !errors.Is(openErr, ErrOutcomeUnknown) {
					t.Fatalf("phase %d err=%v", phase, openErr)
				}
				if openErr = repository.Close(); openErr != nil {
					t.Fatal(openErr)
				}
				repository, openErr = openRepository(path, limits, verify)
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer repository.Close()
				if stats := repository.Stats(); stats.Artifacts != 0 || stats.DiskBytes != 0 {
					t.Fatalf("recovered stats=%+v", stats)
				}
				if reclaimed, retryErr := repository.AbandonArtifact(witness); retryErr != nil || reclaimed != 0 {
					t.Fatalf("retry reclaimed=%d err=%v", reclaimed, retryErr)
				}
				if _, statErr := os.Stat(filepath.Join(path, abandoningArtifactName(descriptor.ArtifactHash))); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("abandoning artifact retained: %v", statErr)
				}
			})
		}
	}
}

func TestAbandonmentCollectorSettlesDeleteBeforeJournalCrashAcrossReopen(t *testing.T) {
	request, descriptor := sourceControlFixture()
	root := t.TempDir()
	journalPath, repositoryPath := filepath.Join(root, "journal"), filepath.Join(root, "repo")
	journal, err := OpenSourceFileJournal(journalPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.PublishSourceExport(t.Context(), 0, SourceControlRecord{
		Request: request, Revision: 1, State: SourceControlRunning,
	}); err != nil {
		t.Fatal(err)
	}
	limits := Limits{MaxArtifacts: 1, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 2 << 20}
	verify := func(*os.File, Descriptor) error { return nil }
	repository, err := openRepository(repositoryPath, limits, verify)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("source-control"), 400)
	appendAll(t, repository, descriptor, payload, 0)
	witness := testAbandonmentWitness(request, descriptor)
	if _, err = repository.AbandonArtifact(witness); err != nil {
		t.Fatal(err)
	}
	if err = errors.Join(repository.Close(), journal.Close()); err != nil {
		t.Fatal(err)
	}

	journal, err = OpenSourceFileJournal(journalPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	repository, err = openRepository(repositoryPath, limits, verify)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	collector, err := NewAbandonmentCollector(AbandonmentCollectorOptions{
		Journal: journal, Repository: repository,
		Authority:  abandonmentAuthority{witnesses: map[[32]byte]ArtifactAbandonmentWitness{request.Operation: witness}},
		MaxRecords: 1, MaxReclaimedBytes: (1 << 20) + DescriptorBytes + cursorBytes,
		MaxRetainedBytes: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	pass, err := collector.RunPass(t.Context(), SourceExportCursor{})
	if err != nil || pass.Deleted != 1 || pass.ReclaimedBytes != 0 {
		t.Fatalf("settled pass=%+v err=%v", pass, err)
	}
	record, err := journal.ReadSourceExport(t.Context(), request.Operation)
	if err != nil || record.State != SourceControlReleased || record.Descriptor != descriptor {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func sha256Sum(raw []byte) [32]byte {
	return sha256.Sum256(raw)
}
