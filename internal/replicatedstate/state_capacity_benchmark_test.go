package replicatedstate

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

// These benchmarks exercise actual admission and singleton persistence with
// both common serving rosters; codec-only fixtures otherwise use one voter.
func BenchmarkMachineStateCapacity(b *testing.B) {
	for _, voters := range []int{1, 3} {
		for _, apply := range []bool{false, true} {
			b.Run(fmt.Sprintf("RF%d/apply_%t", voters, apply), func(b *testing.B) {
				fixture := newMachineFixture(b)
				fixture.bootstrap.Metadata.ConfState.Voters = make([]uint64, voters)
				for i := range voters {
					fixture.bootstrap.Metadata.ConfState.Voters[i] = uint64(i + 1)
				}
				// No publication exists yet. Open against the exact selected roster
				// before installing it, rather than mutating a live machine's state.
				machine, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
					UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = machine.InstallSnapshot(fixture.bootstrap); err != nil {
					b.Fatal(err)
				}
				applySessionOpen(b, machine, 2, commandValue(fixture.binding, 1))
				command := commandValue(fixture.binding, 1)
				value := []byte(`{"value":0}`)
				command.Batches = []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{
					{Kind: replication.MutationPut, Key: []byte("key"), Value: value},
				}}}
				raw, err := replication.AppendCommand(nil, command)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if apply {
						value[len(value)-2] = byte('0' + command.ClientSequence%10)
						raw, err = replication.AppendCommand(raw[:0], command)
						if err == nil {
							_, err = machine.ApplyNormal(normalMeta(command.ClientSequence+1), raw)
						}
						command.AckThrough = command.ClientSequence
						command.ClientSequence++
					} else {
						err = machine.AdmitCommand(raw)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				if apply {
					lookup, err := machine.LookupCompletion(raw)
					if err != nil {
						b.Fatal(err)
					}
					completion, err := replication.OpenCompletion(lookup.Bytes)
					if err != nil || completion.ResultCode != ResultApplied {
						b.Fatalf("final apply result = %+v, %v", completion, err)
					}
				}
			})
		}
	}
}
