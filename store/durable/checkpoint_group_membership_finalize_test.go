package durable

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestCheckpointMembershipFinalizeExactSuccessorAndSilentRetry(t *testing.T) {
	dir, source, _, group := newCheckpointGroupTestStore(t, 128)
	checkpointGroupPut(t, group, 1, source, "before")
	fresh := openTxnNamedCollection(t, t.TempDir(), "user", txnTestOptions())
	target := []NamedCollection{source[0], fresh}
	authorization, command := sha256.Sum256([]byte("prepare")), sha256.Sum256([]byte("verified schema entry"))
	witness, err := group.PrepareMembershipTransition(target, authorization)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := openCheckpointMembershipCertificate(group.log)
	if err != nil {
		t.Fatal(err)
	}
	if err := group.FinalizeMembershipTransition(witness, authorization, 2, command); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatal("finalized before commit", err)
	}
	if err := ValidateFinalizedCheckpointMembershipTransition(dir, witness, authorization, 2, command); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatal("unfinalized record authorized namespace move", err)
	}
	checkpointGroupPut(t, group, 2, source[:1], "schema")
	for _, mutate := range []func(*CheckpointMembershipWitness, *[32]byte, *uint64, *[32]byte){
		func(w *CheckpointMembershipWitness, _ *[32]byte, _ *uint64, _ *[32]byte) { w.Source[0]++ },
		func(w *CheckpointMembershipWitness, _ *[32]byte, _ *uint64, _ *[32]byte) { w.Target[0]++ },
		func(_ *CheckpointMembershipWitness, a *[32]byte, _ *uint64, _ *[32]byte) { a[0]++ },
		func(_ *CheckpointMembershipWitness, _ *[32]byte, a *uint64, _ *[32]byte) { *a = 3 },
		func(_ *CheckpointMembershipWitness, _ *[32]byte, _ *uint64, c *[32]byte) { *c = [32]byte{} },
	} {
		w, a, applied, c := witness, authorization, uint64(2), command
		mutate(&w, &a, &applied, &c)
		before := group.Stats()
		if err := group.FinalizeMembershipTransition(w, a, applied, c); !errors.Is(err, ErrCheckpointMembershipTransition) {
			t.Fatal("substituted finalization accepted", err)
		}
		if group.Stats() != before {
			t.Fatal("invalid finalization performed durability work")
		}
	}
	if err := group.FinalizeMembershipTransition(witness, authorization, 2, command); err != nil {
		t.Fatal(err)
	}
	final, err := openCheckpointMembershipCertificate(group.log)
	if err != nil || checkpointMembershipWitness(final) != witness || final.prepared != witness || final.commandDigest != command {
		t.Fatalf("lost original command witness: %+v %v", final, err)
	}
	if final.source == prepared.source || final.target == prepared.target || final.members[1] != prepared.members[1] ||
		final.members[0].generation <= prepared.members[0].generation {
		t.Fatal("did not refresh exactly the shared system generation")
	}
	before := group.Stats()
	rawBefore, err := os.ReadFile(filepath.Join(dir, checkpointMembershipFilename))
	if err != nil {
		t.Fatal(err)
	}
	if err := group.FinalizeMembershipTransition(witness, authorization, 2, command); err != nil {
		t.Fatal(err)
	}
	rawAfter, err := os.ReadFile(filepath.Join(dir, checkpointMembershipFilename))
	if err != nil || group.Stats() != before || !slices.Equal(rawBefore, rawAfter) || len(rawAfter) != checkpointMembershipFileBytes {
		t.Fatal("retry rewrote or grew fixed certificate", err)
	}
	wrong := command
	wrong[0]++
	if err := group.FinalizeMembershipTransition(witness, authorization, 2, wrong); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatal("changed committed command rebound original witness", err)
	}
	if err := ValidateFinalizedCheckpointMembershipTransition(dir, witness, authorization, 2, command); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSelectedCheckpointMembershipTransition(dir, witness, authorization, 2, command); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatal("unselected target authorized source drain", err)
	}
	if err := ValidateFinalizedCheckpointMembershipTransition(dir, witness, authorization, 2, wrong); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatal("changed command authorized namespace move", err)
	}
	// Simulate a broken caller fence: even a valid finalization receipt cannot
	// authorize moving files after the source has advanced beyond its cut.
	checkpointGroupPut(t, group, 3, source[:1], "forbidden-source-write")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFinalizedCheckpointMembershipTransition(dir, witness, authorization, 2, command); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatal("stale source checkpoint authorized namespace move", err)
	}
}

func TestCheckpointMembershipFinalizeAfterSourceRecovery(t *testing.T) {
	dir, source, _, group := newCheckpointGroupTestStore(t, 128)
	checkpointGroupPut(t, group, 1, source, "before")
	fresh := openTxnNamedCollection(t, t.TempDir(), "user", txnTestOptions())
	auth, command := [32]byte{1}, [32]byte{2}
	witness, err := group.PrepareMembershipTransition([]NamedCollection{source[0], fresh}, auth)
	if err != nil {
		t.Fatal(err)
	}
	checkpointGroupPut(t, group, 2, source[:1], "schema")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Crash after the schema checkpoint but before finalization. Earlier cuts
	// recover the older apply frontier and require normal Raft replay first.
	image := copyCheckpointGroupDirectory(t, dir)
	requests, files := checkpointGroupTestOpenRequests(t, image)
	collections, log, recovered, err := OpenCollectionsWithCheckpointGroup(image, TxnLogOptions{}, requests,
		[]string{"system", "user"}, CheckpointGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = recovered.Close()
		for _, collection := range collections {
			_ = collection.Close()
		}
		_ = log.Close()
		for _, file := range files {
			_ = file.Close()
		}
	}()
	if err := recovered.FinalizeMembershipTransition(witness, auth, 2, command); err != nil {
		t.Fatal("exact committed source could not finalize after recovery", err)
	}
	if err := ValidateFinalizedCheckpointMembershipTransition(image, witness, auth, 2, command); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointMembershipFinalizeRejectsExtraTransactionAndForeignSource(t *testing.T) {
	for _, kind := range []string{"two entries", "same-index metadata", "membership", "seed", "marker", "span"} {
		t.Run(kind, func(t *testing.T) {
			_, source, _, group := newCheckpointGroupTestStore(t, 128)
			checkpointGroupPut(t, group, 1, source, "before")
			auth, command := [32]byte{1}, [32]byte{2}
			witness, err := group.PrepareMembershipTransition(source, auth)
			if err != nil {
				t.Fatal(err)
			}
			checkpointGroupPut(t, group, 2, source[:1], "schema")
			restore := func() {}
			switch kind {
			case "two entries":
				checkpointGroupPut(t, group, 3, source[:1], "later")
			case "same-index metadata":
				checkpointGroupPut(t, group, 2, source[:1], "later")
			case "membership":
				prior := group.members[0].storeID
				group.members[0].storeID[0]++
				restore = func() { group.members[0].storeID = prior }
			case "seed":
				prior := group.seedState
				group.seedState[0]++
				restore = func() { group.seedState = prior }
			case "marker":
				prior := group.markerID
				group.markerID[0]++
				restore = func() { group.markerID = prior }
			case "span":
				prior := group.maxApplySpan
				group.maxApplySpan++
				restore = func() { group.maxApplySpan = prior }
			}
			err = group.FinalizeMembershipTransition(witness, auth, group.applied, command)
			restore()
			want := ErrCheckpointMembershipTransition
			if kind == "marker" {
				want = ErrCheckpointGroupCorrupt // live marker ownership fails even before witness comparison
			}
			if !errors.Is(err, want) {
				t.Fatal("accepted non-exact source successor", err)
			}
		})
	}
}

func TestCheckpointMembershipFinalizationTornSlotRecoversPreparedWitness(t *testing.T) {
	dir, source, _, group := newCheckpointGroupTestStore(t, 128)
	checkpointGroupPut(t, group, 1, source, "before")
	fresh := openTxnNamedCollection(t, t.TempDir(), "user", txnTestOptions())
	auth, command := [32]byte{1}, [32]byte{2}
	witness, err := group.PrepareMembershipTransition([]NamedCollection{source[0], fresh}, auth)
	if err != nil {
		t.Fatal(err)
	}
	checkpointGroupPut(t, group, 2, source[:1], "schema")
	if err := group.FinalizeMembershipTransition(witness, auth, 2, command); err != nil {
		t.Fatal(err)
	}
	image := copyCheckpointGroupDirectory(t, dir)
	file, err := os.OpenFile(filepath.Join(image, checkpointMembershipFilename), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt([]byte{0xff}, int64((witness.Sequence+1)%checkpointMembershipSlots)*checkpointMembershipSlotBytes)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFinalizedCheckpointMembershipTransition(image, witness, auth, 2, command); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatal("torn finalization authorized target namespace", err)
	}
	_, _, recovered := openCheckpointGroupTestCopy(t, image)
	if err := recovered.FinalizeMembershipTransition(witness, auth, 2, command); err != nil {
		t.Fatal("prepared witness could not recover torn finalization", err)
	}
	if err := ValidateFinalizedCheckpointMembershipTransition(image, witness, auth, 2, command); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointMembershipFinalizationCrashCutsRecoverOriginalWitness(t *testing.T) {
	for _, point := range []checkpointGroupFaultPoint{0, checkpointGroupAfterMembershipWrite, checkpointGroupAfterMembershipSync} {
		t.Run(string(rune('0'+point)), func(t *testing.T) {
			dir, source, _, group := newCheckpointGroupTestStore(t, 128)
			checkpointGroupPut(t, group, 1, source, "before")
			fresh := openTxnNamedCollection(t, t.TempDir(), "user", txnTestOptions())
			auth, command := [32]byte{1}, [32]byte{2}
			witness, err := group.PrepareMembershipTransition([]NamedCollection{source[0], fresh}, auth)
			if err != nil {
				t.Fatal(err)
			}
			checkpointGroupPut(t, group, 2, source[:1], "schema")
			injected := errors.New("finalization crash cut")
			checkpointGroupFaultHook = func(at checkpointGroupFaultPoint) error {
				if at == point {
					return injected
				}
				return nil
			}
			err = group.FinalizeMembershipTransition(witness, auth, 2, command)
			checkpointGroupFaultHook = nil
			if point == 0 && err != nil || point != 0 && !errors.Is(err, injected) {
				t.Fatal("unexpected finalization result", err)
			}
			// Uncertain publication must not be mistaken for a durable exact
			// retry just because its bytes remain readable in the page cache.
			if point != 0 {
				if err := group.FinalizeMembershipTransition(witness, auth, 2, command); !errors.Is(err, ErrTxnLogPoisoned) {
					t.Fatal("uncertain publication did not poison live owner", err)
				}
			}
			image := copyCheckpointGroupDirectory(t, dir)
			for _, path := range []string{fresh.Collection.file.Name(), RecoveryJournalPath(fresh.Collection.file.Name())} {
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(image, filepath.Base(path)), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			requests, files := checkpointGroupTestOpenRequests(t, image)
			collections, log, activated, err := OpenCollectionsWithCheckpointMembershipTransition(image, TxnLogOptions{}, requests,
				[]string{"system", "user"}, witness, auth, CheckpointGroupOptions{})
			if err != nil {
				t.Fatal("original committed witness failed after finalize/reopen", err)
			}
			if activated.applied != 2 || activated.members[0].storeID != source[0].Collection.storeID || activated.members[1].storeID != fresh.Collection.storeID {
				t.Fatal("wrong finalized cut or shared/replaced membership")
			}
			if err := ValidateSelectedCheckpointMembershipTransition(image, witness, auth, 2, command); err != nil {
				t.Fatal("selected target not witnessed", err)
			}
			checkpointGroupPut(t, activated, 3, []NamedCollection{{Name: "system", Collection: collections[0]}, {Name: "user", Collection: collections[1]}}, "after-schema")
			if err = activated.Close(); err != nil {
				t.Fatal(err)
			}
			for _, collection := range collections {
				_ = collection.Close()
			}
			_ = log.Close()
			for _, file := range files {
				_ = file.Close()
			}
			requests, files = checkpointGroupTestOpenRequests(t, image)
			collections, log, activated, err = OpenCollectionsWithCheckpointMembershipTransition(image, TxnLogOptions{}, requests,
				[]string{"system", "user"}, witness, auth, CheckpointGroupOptions{})
			if err != nil || activated.applied != 3 {
				t.Fatal("already-selected target could not reopen after ordinary apply", err)
			}
			if err := ValidateSelectedCheckpointMembershipTransition(image, witness, auth, 2, command); err != nil {
				t.Fatal("later target checkpoint lost drain authority", err)
			}
			_ = activated.Close()
			for _, collection := range collections {
				_ = collection.Close()
			}
			_ = log.Close()
			for _, file := range files {
				_ = file.Close()
			}
		})
	}
}
