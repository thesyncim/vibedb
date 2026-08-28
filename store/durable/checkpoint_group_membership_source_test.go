package durable

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointMembershipObserveCommittedSourceIsReadOnlyAndExact(t *testing.T) {
	dir, source, _, group := newCheckpointGroupTestStore(t, 128)
	checkpointGroupPut(t, group, 1, source, "before")
	fresh := openTxnNamedCollection(t, t.TempDir(), "user", txnTestOptions())
	auth, command := [32]byte{1}, [32]byte{2}
	witness, err := group.PrepareMembershipTransition([]NamedCollection{source[0], fresh}, auth)
	if err != nil {
		t.Fatal(err)
	}
	observe := func() error { return group.ObserveCommittedSourceMembershipTransition(witness, auth, 2, command) }
	if err := observe(); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatal("accepted uncommitted source", err)
	}
	checkpointGroupPut(t, group, 2, source[:1], "schema")
	if err := observe(); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatal("accepted uncheckpointed live source", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for _, finalized := range []bool{false, true} {
		if finalized {
			if err := group.FinalizeMembershipTransition(witness, auth, 2, command); err != nil {
				t.Fatal(err)
			}
		}
		before := group.Stats()
		files := map[string][]byte{}
		for _, name := range []string{checkpointMembershipFilename, checkpointGroupFilename} {
			raw, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			files[name] = raw
		}
		if err := observe(); err != nil {
			t.Fatalf("exact source finalized=%v: %v", finalized, err)
		}
		for _, mutate := range []func(*CheckpointMembershipWitness, *[32]byte, *uint64, *[32]byte){
			func(w *CheckpointMembershipWitness, _ *[32]byte, _ *uint64, _ *[32]byte) { w.Source[0]++ },
			func(w *CheckpointMembershipWitness, _ *[32]byte, _ *uint64, _ *[32]byte) { w.Target[0]++ },
			func(w *CheckpointMembershipWitness, _ *[32]byte, _ *uint64, _ *[32]byte) { w.Sequence++ },
			func(_ *CheckpointMembershipWitness, a *[32]byte, _ *uint64, _ *[32]byte) { a[0]++ },
			func(_ *CheckpointMembershipWitness, _ *[32]byte, a *uint64, _ *[32]byte) { *a = 3 },
			func(_ *CheckpointMembershipWitness, _ *[32]byte, _ *uint64, c *[32]byte) { *c = [32]byte{} },
		} {
			w, a, applied, c := witness, auth, uint64(2), command
			mutate(&w, &a, &applied, &c)
			if err := group.ObserveCommittedSourceMembershipTransition(w, a, applied, c); !errors.Is(err, ErrCheckpointMembershipTransition) {
				t.Fatal("substituted source authority accepted", err)
			}
		}
		if finalized {
			changed := command
			changed[0]++
			if err := group.ObserveCommittedSourceMembershipTransition(witness, auth, 2, changed); !errors.Is(err, ErrCheckpointMembershipTransition) {
				t.Fatal("changed finalized command accepted", err)
			}
		}
		if group.Stats() != before {
			t.Fatal("source observation performed durability work")
		}
		for name, want := range files {
			got, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatal("source observation changed certificate", name, err)
			}
		}
	}
	checkpointGroupPut(t, group, 3, source[:1], "unexpected-later-source")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := observe(); !errors.Is(err, ErrCheckpointMembershipTransition) {
		t.Fatal("later source accepted", err)
	}
}

func TestCheckpointMembershipObserveCommittedSourceAfterRecovery(t *testing.T) {
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
	for _, finalized := range []bool{false, true} {
		if finalized {
			if err := group.FinalizeMembershipTransition(witness, auth, 2, command); err != nil {
				t.Fatal(err)
			}
		}
		image := copyCheckpointGroupDirectory(t, dir)
		_, _, recovered := openCheckpointGroupTestCopy(t, image)
		before := recovered.Stats()
		if err := recovered.ObserveCommittedSourceMembershipTransition(witness, auth, 2, command); err != nil || recovered.Stats() != before {
			t.Fatalf("source observation after recovery finalized=%v: %v", finalized, err)
		}
	}
}
