package replicatedstate

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func TestRandomizedMutationHistoryMatchesReferenceMapAndImageAudit(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewSource(20260811))
	reference := make(map[string][]byte)
	for step := 0; step < 80; step++ {
		count := 1 + random.Intn(10)
		mutations := make([]replication.Mutation, 0, count)
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("k%02d", random.Intn(12))
			if random.Intn(4) == 0 {
				mutations = append(mutations, replication.Mutation{
					Kind: replication.MutationDelete, Key: []byte(key),
				})
				delete(reference, key)
				continue
			}
			value := []byte(fmt.Sprintf(`{"v":%d}`, random.Intn(1000)))
			mutations = append(mutations, replication.Mutation{
				Kind: replication.MutationPut, Key: []byte(key), Value: value,
			})
			reference[key] = append([]byte(nil), value...)
		}
		command := testCommand(fixture.binding, uint64(step+1), mutations...)
		publication, err := fixture.machine.ApplyNormal(normalMeta(uint64(step+2)), command)
		if err != nil {
			t.Fatalf("step %d apply: %v", step, err)
		}
		if publication.DataChainDigest == ([32]byte{}) {
			t.Fatalf("step %d published a zero data-chain digest", step)
		}
		snapshot, err := fixture.user.Collection.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		observed := make(map[string]string)
		err = snapshot.RangeRaw(func(key, value []byte) error {
			observed[string(key)] = string(value)
			return nil
		})
		_ = snapshot.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(observed) != len(reference) {
			t.Fatalf("step %d observed rows=%d want=%d", step, len(observed), len(reference))
		}
		for key, value := range reference {
			if observed[key] != string(value) {
				t.Fatalf("step %d key %q = %q, want %q", step, key, observed[key], value)
			}
		}
	}
	snapshot, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	imageDigest, auditErr := snapshot.CanonicalImageDigest()
	closeErr := snapshot.Close()
	if auditErr != nil || closeErr != nil {
		t.Fatalf("canonical image audit = %x, %v; close = %v", imageDigest, auditErr, closeErr)
	}
	if want := referenceCanonicalImageDigest("docs", reference); imageDigest != want {
		t.Fatalf("canonical image digest = %x, want %x", imageDigest, want)
	}
	wantChain := fixture.machine.Published().DataChainDigest
	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := reopened.Published().DataChainDigest, wantChain; got != want {
		t.Fatalf("reopened digest = %x, want %x", got, want)
	}
}

func referenceCanonicalImageDigest(name string, rows map[string][]byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("vibedb/replicated-state/logical-image\x00"))
	_, _ = h.Write([]byte{byte(ValidationDeterministicMutation)})
	_, _ = h.Write(defaultUserValidationDigest[:])
	writeFrame := func(value []byte) {
		var length [8]byte
		binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(value)
	}
	writeFrame([]byte(name))
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		_, _ = h.Write([]byte{1})
		writeFrame([]byte(key))
		writeFrame(rows[key])
	}
	_, _ = h.Write([]byte{0})
	var digest [32]byte
	_ = h.Sum(digest[:0])
	return digest
}

func TestCoherentSnapshotRacesApply(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 2)
	done := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		defer close(done)
		for i := 0; i < 40; i++ {
			command := testCommand(fixture.binding, uint64(i+1), replication.Mutation{
				Kind: replication.MutationPut, Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("null"),
			})
			if _, err := fixture.machine.ApplyNormal(normalMeta(uint64(i+2)), command); err != nil {
				errCh <- err
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			wait.Wait()
			select {
			case err := <-errCh:
				t.Fatal(err)
			default:
			}
			return
		default:
		}
		snapshot, err := fixture.machine.Snapshot("docs")
		if err != nil {
			t.Fatal(err)
		}
		publication, state := snapshot.Publication(), snapshot.State()
		if publication.Applied != state.Applied || publication.DataChainDigest != state.DataChainDigest ||
			publication.ReplicaSetVersion != state.ReplicaSetVersion {
			_ = snapshot.Close()
			t.Fatalf("skewed snapshot publication=%+v state=%+v", publication, state)
		}
		user, ok := snapshot.Collection("docs")
		if !ok || user.Len() != state.CompletionCount {
			_ = snapshot.Close()
			t.Fatalf("snapshot rows=%d completions=%d", user.Len(), state.CompletionCount)
		}
		systemRows := uint64(0)
		if err := snapshot.RangeSystem(func(_, _ []byte) error { systemRows++; return nil }); err != nil {
			_ = snapshot.Close()
			t.Fatal(err)
		}
		if systemRows != state.CompletionCount+1 {
			_ = snapshot.Close()
			t.Fatalf("system rows=%d completions=%d", systemRows, state.CompletionCount)
		}
		if err := snapshot.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
