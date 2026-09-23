package replicatedstate

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

// A committed Raft suffix already owns durability. Ephemeral reads and planning
// must not fold that suffix into physical roots just to observe its applied cut.
func TestEphemeralMachineCutsDoNotCheckpoint(t *testing.T) {
	for _, operation := range []string{"point-batch", "completion", "admission", "apply-batch"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newNormalBatchFixture(t, 32, 8)
			machine := fixture.machine
			if _, err := machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			applySessionOpen(t, machine, 2, commandValue(fixture.binding, 1))
			value := []byte(`{"n":1}`)
			command := testCommand(fixture.binding, 1, replication.Mutation{
				Kind: replication.MutationPut, Key: []byte("k"), Value: value,
			})
			publication, err := machine.ApplyNormal(normalMeta(3), command)
			if err != nil {
				t.Fatal(err)
			}
			before := fixture.group.Stats()
			if before.CheckpointAppliedIndex >= publication.Applied {
				t.Fatal("fixture has no dirty replay-backed suffix")
			}
			switch operation {
			case "point-batch":
				packed, err := AppendPointReadBatch(nil, []PointRead{{Relation: 1, Key: []byte("k")}, {Relation: 1, Key: []byte("missing")}})
				if err != nil {
					t.Fatal(err)
				}
				result, err := machine.PointReadBatchInto(packed, publication.Applied, 1024, nil)
				if err != nil || result.Fence.Applied != publication.Applied {
					t.Fatalf("batch read: %+v %v", result, err)
				}
				rows, err := OpenPointReadBatchValue(result.Data)
				if err != nil {
					t.Fatal(err)
				}
				if got, found, ok := rows.Lookup(0); !ok || !found || !bytes.Equal(got, value) {
					t.Fatalf("applied value=%q found=%t valid=%t", got, found, ok)
				}
				if _, found, ok := rows.Lookup(1); !ok || found {
					t.Fatalf("missing found=%t valid=%t", found, ok)
				}
			case "completion":
				result, err := machine.LookupCompletion(command)
				if err != nil || result.AppliedSequence != publication.Applied {
					t.Fatalf("completion: %+v %v", result, err)
				}
			case "admission":
				if err := machine.AdmitCommand(testCommand(fixture.binding, 2, replication.Mutation{
					Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":2}`),
				})); err != nil {
					t.Fatal(err)
				}
			case "apply-batch":
				entries := normalBatchEntries(4, testCommand(fixture.binding, 2, replication.Mutation{
					Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":2}`),
				}))
				count, got, err := machine.ApplyNormalBatch(entries, normalBatchWitnesses(entries))
				if err != nil || count != 1 || got.Applied != 4 {
					t.Fatalf("apply batch: %d %+v %v", count, got, err)
				}
			}
			after := fixture.group.Stats()
			if after.CheckpointAppliedIndex != before.CheckpointAppliedIndex || after.BarrierSyncs != before.BarrierSyncs ||
				after.PhysicalCheckpoints != before.PhysicalCheckpoints {
				t.Fatalf("ephemeral cut forced durability work: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestLivePointBatchNeverMixesAppliedGenerations(t *testing.T) {
	fixture := newNormalBatchFixture(t, 32, 8)
	machine := fixture.machine
	if _, err := machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, machine, 2, commandValue(fixture.binding, 1))
	commands := make([][]byte, 32)
	for index := range commands {
		value := fmt.Appendf(nil, `{"n":%d}`, index)
		commands[index] = testCommand(fixture.binding, uint64(index+1),
			replication.Mutation{Kind: replication.MutationPut, Key: []byte("a"), Value: value},
			replication.Mutation{Kind: replication.MutationPut, Key: []byte("b"), Value: value})
	}
	if _, err := machine.ApplyNormal(normalMeta(3), commands[0]); err != nil {
		t.Fatal(err)
	}
	packed, err := AppendPointReadBatch(nil, []PointRead{{Relation: 1, Key: []byte("a")}, {Relation: 1, Key: []byte("b")}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for index, command := range commands[1:] {
			if _, err := machine.ApplyNormal(normalMeta(uint64(index+4)), command); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	defer func() {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for range 128 {
		result, err := machine.PointReadBatchInto(packed, 3, 1024, nil)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := OpenPointReadBatchValue(result.Data)
		if err != nil {
			t.Fatal(err)
		}
		left, leftFound, leftOK := rows.Lookup(0)
		right, rightFound, rightOK := rows.Lookup(1)
		want := fmt.Appendf(nil, `{"n":%d}`, result.Fence.Applied-3)
		if !leftOK || !rightOK || !leftFound || !rightFound || !bytes.Equal(left, right) || !bytes.Equal(left, want) {
			t.Fatalf("mixed applied cut %d: left=%q right=%q want=%q", result.Fence.Applied, left, right, want)
		}
	}
}
