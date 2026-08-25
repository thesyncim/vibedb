package durable

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func checkpointGroupSeedImagesForTest(
	members []NamedCollection,
	seedMember string,
) []CheckpointGroupSeedImage {
	images := make([]CheckpointGroupSeedImage, 0, len(members)-1)
	for _, member := range members {
		if member.Name == seedMember {
			continue
		}
		images = append(images, CheckpointGroupSeedImage{
			Collection: member.Collection,
			Generation: member.Collection.Generation(),
		})
	}
	return images
}

func TestCheckpointGroupSeedCertifiesImportedStateAndReopens(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	if _, err := members[1].Collection.Put(
		[]byte("row"), []byte(`{"value":"staged"}`),
	); err != nil {
		t.Fatal(err)
	}
	// NewSeededCheckpointGroup must fold this ordinary staging suffix exactly
	// once before publishing the fixed certificate.
	if members[1].Collection.journal == nil || members[1].Collection.journal.Cursor() == 0 {
		t.Fatal("staged fixture did not carry a journal suffix")
	}
	seed := CheckpointGroupSeed{
		Applied: 9, Member: "system", Envelope: []byte(`{"state":"imported"}`),
	}
	seed.Images = checkpointGroupSeedImagesForTest(members, seed.Member)
	group, err := NewSeededCheckpointGroup(
		log, members, seed, CheckpointGroupOptions{CheckpointEvery: 8},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = group.Close() })
	if !group.SeedPending() || group.AppliedIndex() != 0 ||
		group.CheckpointAppliedIndex() != 0 {
		t.Fatalf("initial seed cut = pending %v, applied %d/%d",
			group.SeedPending(), group.AppliedIndex(), group.CheckpointAppliedIndex())
	}
	if members[1].Collection.journal.Cursor() != 0 {
		t.Fatal("staged suffix was not folded before seed certificate publication")
	}
	if _, found, err := members[0].Collection.AppendRaw(nil, []byte("state")); err != nil || found {
		t.Fatalf("seed member before Seed = found %v, err %v", found, err)
	}
	if got, found, err := members[1].Collection.AppendRaw(nil, []byte("row")); err != nil || !found || !bytes.Equal(got, []byte(`{"value":"staged"}`)) {
		t.Fatalf("staged image = %q, found %v, err %v", got, found, err)
	}
	if err := group.Seed(
		seed, members[0], defaultTxnLimits(), []byte("state"),
	); err != nil {
		t.Fatal(err)
	}
	if !group.SeedActivationPending() {
		t.Fatal("certified seed appeared activation-complete before snapshot-base binding")
	}
	if group.SeedPending() || group.AppliedIndex() != seed.Applied ||
		group.CheckpointAppliedIndex() != seed.Applied {
		t.Fatalf("certified seed cut = pending %v, applied %d/%d",
			group.SeedPending(), group.AppliedIndex(), group.CheckpointAppliedIndex())
	}
	if seeded, err := group.ValidateSeedState(
		seed.Applied, seed.Member, seed.Envelope,
	); err != nil || !seeded {
		t.Fatalf("ValidateSeedState = %v, %v", seeded, err)
	}
	if err := group.Seed(
		seed, members[0], defaultTxnLimits(), []byte("state"),
	); err != nil {
		t.Fatalf("idempotent Seed: %v", err)
	}

	crashImage := copyCheckpointGroupDirectory(t, dir)
	collections, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
	if reopened.SeedPending() || reopened.AppliedIndex() != seed.Applied ||
		reopened.CheckpointAppliedIndex() != seed.Applied {
		t.Fatalf("reopened seed cut = pending %v, applied %d/%d",
			reopened.SeedPending(), reopened.AppliedIndex(), reopened.CheckpointAppliedIndex())
	}
	if got, found, err := collections[0].AppendRaw(nil, []byte("state")); err != nil || !found || !bytes.Equal(got, seed.Envelope) {
		t.Fatalf("reopened seed row = %q, found %v, err %v", got, found, err)
	}
	if got, found, err := collections[1].AppendRaw(nil, []byte("row")); err != nil || !found || !bytes.Equal(got, []byte(`{"value":"staged"}`)) {
		t.Fatalf("reopened staged row = %q, found %v, err %v", got, found, err)
	}
}

func TestCheckpointGroupSeedCertifiesAlreadyImportedSeedMemberImage(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	seed := CheckpointGroupSeed{
		Applied: 9, Member: "system", Envelope: []byte(`{"state":"imported"}`),
	}
	if _, err := members[0].Collection.Put([]byte("state"), seed.Envelope); err != nil {
		t.Fatal(err)
	}
	if _, err := members[0].Collection.Put([]byte("session"), []byte(`"retained"`)); err != nil {
		t.Fatal(err)
	}
	if _, err := members[1].Collection.Put([]byte("row"), []byte(`"value"`)); err != nil {
		t.Fatal(err)
	}
	seed.Images = make([]CheckpointGroupSeedImage, 0, len(members))
	for _, member := range members {
		seed.Images = append(seed.Images, CheckpointGroupSeedImage{
			Collection: member.Collection, Generation: member.Collection.Generation(),
		})
	}
	group, err := NewSeededCheckpointGroup(log, members, seed, CheckpointGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = group.Seed(seed, members[0], defaultTxnLimits(), []byte("state")); err != nil {
		t.Fatal(err)
	}
	crashImage := copyCheckpointGroupDirectory(t, dir)
	collections, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
	defer reopened.Close()
	for _, check := range []struct {
		collection int
		key, value []byte
	}{{0, []byte("state"), seed.Envelope}, {0, []byte("session"), []byte(`"retained"`)},
		{1, []byte("row"), []byte(`"value"`)}} {
		got, found, readErr := collections[check.collection].AppendRaw(nil, check.key)
		if readErr != nil || !found || !bytes.Equal(got, check.value) {
			t.Fatalf("reopened %q = %q, found=%v err=%v", check.key, got, found, readErr)
		}
	}
}

func TestCheckpointGroupSeedGenerationFenceIsAtomicWithOwnership(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	if _, err := members[1].Collection.Put(
		[]byte("row"), []byte(`{"value":"staged"}`),
	); err != nil {
		t.Fatal(err)
	}
	snapshot, err := members[1].Collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	generation := snapshot.Generation()
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	seed := CheckpointGroupSeed{
		Applied: 9, Member: "system", Envelope: []byte(`{"state":"imported"}`),
		Images: []CheckpointGroupSeedImage{{
			Collection: members[1].Collection, Generation: generation,
		}},
	}
	previous := checkpointGroupAfterInitialValidationHook
	checkpointGroupAfterInitialValidationHook = func() {
		if _, putErr := members[1].Collection.Put(
			[]byte("row"), []byte(`{"value":"changed"}`),
		); putErr != nil {
			panic(putErr)
		}
	}
	group, err := NewSeededCheckpointGroup(
		log, members, seed, CheckpointGroupOptions{},
	)
	checkpointGroupAfterInitialValidationHook = previous
	if group != nil || !errors.Is(err, ErrCheckpointGroupSeedChanged) {
		t.Fatalf("changed seed image group=%v err=%v", group, err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, checkpointGroupFilename)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("changed seed image published certificate: %v", statErr)
	}
	if members[1].Collection.checkpointGroup.Load() != nil {
		t.Fatal("changed seed image retained checkpoint-group ownership")
	}
	log.regMu.Lock()
	registered := len(log.registered)
	log.regMu.Unlock()
	if registered != 0 {
		t.Fatalf("changed seed image registered %d member(s)", registered)
	}

	snapshot, err = members[1].Collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	seed.Images[0].Generation = snapshot.Generation()
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	group, err = NewSeededCheckpointGroup(
		log, members, seed, CheckpointGroupOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = group.Close() })
	if _, err := members[1].Collection.Put(
		[]byte("row"), []byte(`{"value":"too-late"}`),
	); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("post-attach mutation error = %v", err)
	}
}

func TestCheckpointGroupSeedRequiresAndFencesEveryImportedImage(t *testing.T) {
	newFixture := func(t *testing.T) (string, []NamedCollection, *TxnLog, CheckpointGroupSeed) {
		t.Helper()
		dir, members, log := newCheckpointGroupTestResources(t, "system", "user-a", "user-b")
		for i := 1; i < len(members); i++ {
			if _, err := members[i].Collection.Put(
				[]byte("row"), []byte(`{"value":"staged"}`),
			); err != nil {
				t.Fatal(err)
			}
		}
		seed := CheckpointGroupSeed{
			Applied: 9, Member: "system", Envelope: []byte(`{"state":"imported"}`),
		}
		seed.Images = checkpointGroupSeedImagesForTest(members, seed.Member)
		return dir, members, log, seed
	}
	assertRefusedWithoutPublication := func(
		t *testing.T,
		dir string,
		members []NamedCollection,
		group *CheckpointGroup,
		err error,
	) {
		t.Helper()
		if group != nil || !errors.Is(err, ErrCheckpointGroupSeedChanged) {
			t.Fatalf("group=%v err=%v", group, err)
		}
		if _, statErr := os.Stat(filepath.Join(dir, checkpointGroupFilename)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("refused seed published certificate: %v", statErr)
		}
		for _, member := range members {
			if member.Collection.checkpointGroup.Load() != nil {
				t.Fatalf("refused seed attached member %q", member.Name)
			}
		}
	}

	t.Run("missing witness", func(t *testing.T) {
		dir, members, log, seed := newFixture(t)
		seed.Images = seed.Images[:1]
		group, err := NewSeededCheckpointGroup(
			log, members, seed, CheckpointGroupOptions{},
		)
		assertRefusedWithoutPublication(t, dir, members, group, err)
	})

	t.Run("duplicate witness", func(t *testing.T) {
		dir, members, log, seed := newFixture(t)
		seed.Images[1] = seed.Images[0]
		group, err := NewSeededCheckpointGroup(
			log, members, seed, CheckpointGroupOptions{},
		)
		assertRefusedWithoutPublication(t, dir, members, group, err)
	})

	t.Run("second image changes", func(t *testing.T) {
		dir, members, log, seed := newFixture(t)
		previous := checkpointGroupAfterInitialValidationHook
		checkpointGroupAfterInitialValidationHook = func() {
			if _, err := members[2].Collection.Put(
				[]byte("row"), []byte(`{"value":"changed"}`),
			); err != nil {
				panic(err)
			}
		}
		group, err := NewSeededCheckpointGroup(
			log, members, seed, CheckpointGroupOptions{},
		)
		checkpointGroupAfterInitialValidationHook = previous
		assertRefusedWithoutPublication(t, dir, members, group, err)
	})

	t.Run("complete witness set", func(t *testing.T) {
		_, members, log, seed := newFixture(t)
		group, err := NewSeededCheckpointGroup(
			log, members, seed, CheckpointGroupOptions{},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = group.Close() })
		for _, member := range members[1:] {
			if _, err := member.Collection.Put(
				[]byte("row"), []byte(`{"value":"too-late"}`),
			); !errors.Is(err, ErrCheckpointGroupOwned) {
				t.Fatalf("post-attach mutation for %q = %v", member.Name, err)
			}
		}
	})
}

func TestCheckpointGroupSeedIsSoleCutZeroMutationAndBaseBindingDoesNotAdvance(t *testing.T) {
	_, members, log := newCheckpointGroupTestResources(t, "system", "user")
	if _, err := members[1].Collection.Put(
		[]byte("row"), []byte(`{"value":"staged"}`),
	); err != nil {
		t.Fatal(err)
	}
	seed := CheckpointGroupSeed{
		Applied: 9, Member: "system", Envelope: []byte(`{"state":"imported"}`),
	}
	seed.Images = checkpointGroupSeedImagesForTest(members, seed.Member)
	group, err := NewSeededCheckpointGroup(
		log, members, seed, CheckpointGroupOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = group.Close() })

	for _, applied := range []uint64{0, 1} {
		called := false
		err := group.Update(
			applied, members[:1], defaultTxnLimits(),
			func(batch *DatabaseBatch) error {
				called = true
				write, err := batch.Collection("system")
				if err != nil {
					return err
				}
				return write.Put([]byte("illegal"), []byte(`{"bad":true}`))
			},
		)
		if !errors.Is(err, ErrCheckpointGroupSequence) || called {
			t.Fatalf("cut-zero Update(%d) = called %v, err %v", applied, called, err)
		}
		if _, found, readErr := members[0].Collection.AppendRaw(nil, []byte("illegal")); readErr != nil || found {
			t.Fatalf("cut-zero Update(%d) row = found %v, err %v", applied, found, readErr)
		}
		stats := group.Stats()
		if stats.TransactionHighWater != 0 || stats.Updates != 0 ||
			stats.AppliedIndex != 0 || stats.CheckpointAppliedIndex != 0 {
			t.Fatalf("cut-zero Update(%d) stats = %+v", applied, stats)
		}
	}

	if err := group.Seed(
		seed, members[0], defaultTxnLimits(), []byte("state"),
	); err != nil {
		t.Fatal(err)
	}
	called := false
	err = group.Update(
		seed.Applied+1, members[:1], defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			called = true
			write, err := batch.Collection("system")
			if err != nil {
				return err
			}
			return write.Put([]byte("advanced"), []byte(`{"bad":true}`))
		},
	)
	if !errors.Is(err, ErrCheckpointGroupSequence) || called {
		t.Fatalf("pre-base advancing Update = called %v, err %v", called, err)
	}
	if _, found, readErr := members[0].Collection.AppendRaw(nil, []byte("advanced")); readErr != nil || found {
		t.Fatalf("pre-base advancing row = found %v, err %v", found, readErr)
	}
	stats := group.Stats()
	if stats.TransactionHighWater != 1 || stats.Updates != 1 ||
		stats.AppliedIndex != seed.Applied || stats.CheckpointAppliedIndex != seed.Applied {
		t.Fatalf("pre-base advancing stats = %+v", stats)
	}

	err = group.Update(
		seed.Applied, members[:1], defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			write, err := batch.Collection("system")
			if err != nil {
				return err
			}
			return write.Put([]byte("state"), []byte(`{"state":"base-bound"}`))
		},
	)
	if err != nil {
		t.Fatalf("same-applied base binding: %v", err)
	}
	if !group.SeedActivationPending() {
		t.Fatal("uncertified snapshot-base suffix appeared activation-complete")
	}
	stats = group.Stats()
	if stats.TransactionHighWater != 2 || stats.Updates != 2 ||
		stats.AppliedIndex != seed.Applied {
		t.Fatalf("post-base-binding stats = %+v", stats)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if group.SeedActivationPending() {
		t.Fatal("certified snapshot-base binding remained activation-pending")
	}
}

func TestCheckpointGroupCertificateRejectsImpossibleAppliedCuts(t *testing.T) {
	seedState := sha256.Sum256([]byte("seed-state"))
	seedMember := sha256.Sum256([]byte("seed-member"))
	for name, certificate := range map[string]checkpointGroupCertificate{
		"ordinary applied beyond transactions": {
			applied: 2, txnHighWater: 1,
		},
		"ordinary terminal applied": {
			applied: math.MaxUint64, txnHighWater: math.MaxUint64,
		},
		"seed advanced without second transaction": {
			seedApplied: 9, seedState: seedState, seedMember: seedMember,
			applied: 10, txnHighWater: 1,
		},
		"seed advanced beyond transaction prefix": {
			seedApplied: 9, seedState: seedState, seedMember: seedMember,
			applied: 12, txnHighWater: 3,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if validCheckpointGroupSeedCertificate(certificate) {
				t.Fatalf("accepted impossible certificate %+v", certificate)
			}
		})
	}
	for name, certificate := range map[string]checkpointGroupCertificate{
		"ordinary": {applied: 2, txnHighWater: 2},
		"seed c0": {
			seedApplied: 9, seedState: seedState, seedMember: seedMember,
		},
		"seed transaction": {
			seedApplied: 9, seedState: seedState, seedMember: seedMember,
			applied: 9, txnHighWater: 1,
		},
		"seed base": {
			seedApplied: 9, seedState: seedState, seedMember: seedMember,
			applied: 9, txnHighWater: 2,
		},
		"seed advanced": {
			seedApplied: 9, seedState: seedState, seedMember: seedMember,
			applied: 11, txnHighWater: 4,
		},
	} {
		t.Run("valid "+name, func(t *testing.T) {
			if !validCheckpointGroupSeedCertificate(certificate) {
				t.Fatalf("rejected reachable certificate %+v", certificate)
			}
		})
	}
}

func TestCheckpointGroupSeedPendingMissingCertificateIsExplicitAndFailClosed(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	if _, err := members[1].Collection.Put(
		[]byte("row"), []byte(`{"value":"staged"}`),
	); err != nil {
		t.Fatal(err)
	}
	if err := members[1].Collection.Flush(); err != nil {
		t.Fatal(err)
	}
	image := copyCheckpointGroupDirectory(t, dir)

	open := func(t *testing.T, root string, seeded bool, member string) error {
		t.Helper()
		requests, files := checkpointGroupTestOpenRequests(t, root)
		defer func() {
			for _, file := range files {
				_ = file.Close()
			}
		}()
		if seeded {
			_, _, _, err := OpenCollectionsWithSeededCheckpointGroup(
				root, TxnLogOptions{}, requests, []string{"system", "user"}, member,
				CheckpointGroupOptions{},
			)
			return err
		}
		_, _, _, err := OpenCollectionsWithCheckpointGroup(
			root, TxnLogOptions{}, requests, []string{"system", "user"},
			CheckpointGroupOptions{},
		)
		return err
	}
	if err := open(t, copyCheckpointGroupDirectory(t, image), false, ""); !errors.Is(err, ErrCheckpointGroupCorrupt) {
		t.Fatalf("ordinary missing-certificate open = %v", err)
	}
	if err := open(t, copyCheckpointGroupDirectory(t, image), true, "user"); !errors.Is(err, ErrCheckpointGroupCorrupt) {
		t.Fatalf("wrong seed-member missing-certificate open = %v", err)
	}
	if err := open(t, copyCheckpointGroupDirectory(t, image), true, "system"); !errors.Is(err, ErrCheckpointGroupMissing) {
		t.Fatalf("explicit seed-pending missing-certificate open = %v", err)
	}

	seed := CheckpointGroupSeed{
		Applied: 7, Member: "system", Envelope: []byte(`{"state":"imported"}`),
	}
	seed.Images = checkpointGroupSeedImagesForTest(members, seed.Member)
	group, err := NewSeededCheckpointGroup(
		log, members, seed, CheckpointGroupOptions{},
	)
	if err != nil {
		t.Fatalf("NewSeededCheckpointGroup: %v", err)
	}
	t.Cleanup(func() { _ = group.Close() })
	if err := group.Seed(seed, members[0], defaultTxnLimits(), []byte("state")); err != nil {
		t.Fatal(err)
	}
	active := copyCheckpointGroupDirectory(t, dir)
	if err := os.Remove(filepath.Join(active, checkpointGroupFilename)); err != nil {
		t.Fatal(err)
	}
	if err := open(t, active, true, "system"); !errors.Is(err, ErrCheckpointGroupCorrupt) {
		t.Fatalf("deleted active certificate = %v", err)
	}
}

func TestCheckpointGroupSeedCrashCuts(t *testing.T) {
	const applied = uint64(11)
	seed := CheckpointGroupSeed{
		Applied: applied, Member: "system", Envelope: []byte(`{"state":"imported"}`),
	}

	assertCrash := func(t *testing.T, image string, certified bool) {
		t.Helper()
		collections, _, group := openCheckpointGroupTestCopy(t, image)
		want := uint64(0)
		if certified {
			want = applied
		}
		if group.AppliedIndex() != want || group.CheckpointAppliedIndex() != want ||
			group.SeedPending() != !certified {
			t.Fatalf("recovered cut = pending %v, applied %d/%d, want pending %v cut %d",
				group.SeedPending(), group.AppliedIndex(), group.CheckpointAppliedIndex(),
				!certified, want)
		}
		got, found, err := collections[0].AppendRaw(nil, []byte("state"))
		if err != nil || found != certified || certified && !bytes.Equal(got, seed.Envelope) {
			t.Fatalf("recovered state = %q, found %v, err %v", got, found, err)
		}
		if got, found, err := collections[1].AppendRaw(nil, []byte("row")); err != nil || !found || !bytes.Equal(got, []byte(`{"value":"staged"}`)) {
			t.Fatalf("recovered staged row = %q, found %v, err %v", got, found, err)
		}
	}

	t.Run("certificate-publication", func(t *testing.T) {
		dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
		if _, err := members[1].Collection.Put(
			[]byte("row"), []byte(`{"value":"staged"}`),
		); err != nil {
			t.Fatal(err)
		}
		fault := errors.New("crash after initial seed certificate publication")
		previous := checkpointGroupFaultHook
		checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
			if point == checkpointGroupAfterCertificateRename {
				return fault
			}
			return nil
		}
		seed.Images = checkpointGroupSeedImagesForTest(members, seed.Member)
		group, err := NewSeededCheckpointGroup(
			log, members, seed, CheckpointGroupOptions{},
		)
		checkpointGroupFaultHook = previous
		if group == nil || err != nil || !group.SeedPending() {
			t.Fatalf("settled initial certificate = group %v, err %v", group, err)
		}
		assertCrash(t, copyCheckpointGroupDirectory(t, dir), false)
	})

	for _, tc := range []struct {
		name      string
		point     checkpointGroupFaultPoint
		certified bool
	}{
		{name: "prepared-suffix", point: checkpointGroupAfterPrepareAppend},
		{name: "decision-append", point: checkpointGroupAfterDecisionAppend},
		{name: "journal-barrier", point: checkpointGroupAfterJournalSync},
		{name: "final-certificate", point: checkpointGroupAfterCertificateSync, certified: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
			if _, err := members[1].Collection.Put(
				[]byte("row"), []byte(`{"value":"staged"}`),
			); err != nil {
				t.Fatal(err)
			}
			seed.Images = checkpointGroupSeedImagesForTest(members, seed.Member)
			group, err := NewSeededCheckpointGroup(
				log, members, seed, CheckpointGroupOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				clearCheckpointGroupTestPoison(group)
				_ = group.Close()
			})
			fault := errors.New("seed crash point")
			seen := false
			previous := checkpointGroupFaultHook
			checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
				if !seen && point == tc.point {
					seen = true
					return fault
				}
				return nil
			}
			err = group.Seed(seed, members[0], defaultTxnLimits(), []byte("state"))
			checkpointGroupFaultHook = previous
			if !seen || !errors.Is(err, fault) {
				t.Fatalf("Seed crash = seen %v, err %v", seen, err)
			}
			assertCrash(t, copyCheckpointGroupDirectory(t, dir), tc.certified)
		})
	}
}
