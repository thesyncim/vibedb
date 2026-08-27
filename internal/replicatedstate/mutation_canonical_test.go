package replicatedstate

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func canonicalPlanFixture(t testing.TB, batches ...replication.RelationMutationBatch) (*Machine, replication.CommandView) {
	t.Helper()
	command := commandValue(testBinding(), 1)
	command.Batches = batches
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	view, err := replication.OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	m := &Machine{relations: make([]relationCollection, len(batches))}
	for i := range m.relations {
		m.relations[i].kind = RelationJSON
	}
	m.canonicalMutations.begin(view, nil)
	return m, view
}

func TestCanonicalMutationRepresentationAndBorrowing(t *testing.T) {
	for _, test := range []struct{ raw, want string }{
		{`{"a":1,"z":2}`, `{"a":1,"z":2}`},
		{` { "z": 2, "a": 1.0, "e": 1e0 } `, `{"a":1.0,"e":1e0,"z":2}`},
		{"{\"z\":\"\u2028\u2029\",\"a\":0}", `{"a":0,"z":"\u2028\u2029"}`},
		{"{\"\u2028\":1,\"\u2029\":2}", `{"\u2028":1,"\u2029":2}`},
		{`{"\u0061":1,"a":2}`, `{"a":1,"a":2}`},
	} {
		t.Run(test.raw, func(t *testing.T) {
			raw := []byte(test.raw)
			m, _ := canonicalPlanFixture(t, replication.RelationMutationBatch{Relation: 1,
				Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("k"), Value: raw}},
			})
			got, code := m.canonicalMutationValue(raw)
			if code != ResultApplied || string(got) != test.want {
				t.Fatalf("got=%q code=%d want=%q", got, code, test.want)
			}
			if test.raw == test.want && (&got[0] != &raw[0] || m.canonicalMutations.ready || cap(m.canonicalMutations.arena) != 0) {
				t.Fatal("canonical input lost borrowed/no-arena fast path")
			}
			if len(got) > canonicalMutationUpperBytes(raw) || string(raw) != test.raw {
				t.Fatal("renderer exceeded bound or mutated input")
			}
		})
	}
}

func TestCanonicalMutationWholePlanArenaStableAndTransactionRows(t *testing.T) {
	for _, stored := range []bool{false, true} {
		t.Run(fmt.Sprint(stored), func(t *testing.T) {
			batches := make([]replication.RelationMutationBatch, 3)
			var wants [][]byte
			wantBudget := 0
			for relation := 0; relation < 2; relation++ {
				batches[relation].Relation = replication.RelationID(relation + 1)
				for key := 0; key < 64; key++ {
					value := []byte(fmt.Sprintf(" {\"z\":\"%s\",\"a\":%d} ", strings.Repeat("\u2028", key+1), key))
					wants = append(wants, []byte(fmt.Sprintf(`{"a":%d,"z":"%s"}`, key, strings.Repeat(`\u2028`, key+1))))
					wantBudget += canonicalMutationUpperBytes(value)
					batches[relation].Mutations = append(batches[relation].Mutations, replication.Mutation{
						Kind: replication.MutationPut, Key: []byte{byte(key)}, Value: value,
					})
				}
			}
			batches[2] = replication.RelationMutationBatch{Relation: 3, Mutations: []replication.Mutation{{
				Kind: replication.MutationPutAbsentOrEqual, Key: []byte("opaque"), Value: []byte(`[ "opaque" ]`),
			}}}
			m, view := canonicalPlanFixture(t, batches...)
			m.relations[2].kind = RelationGlobalIndex
			if stored {
				var rows []TransactionRelationPayloadView
				iterator := view.RelationBatches()
				for iterator.Next() {
					rows = append(rows, TransactionRelationPayloadView{Batch: iterator.Batch()})
				}
				m.canonicalMutations.begin(replication.CommandView{}, rows)
			}
			var got [][]byte
			for _, batch := range batches[:2] {
				for _, mutation := range batch.Mutations {
					value, code := m.canonicalMutationValue(mutation.Value)
					if code != ResultApplied {
						t.Fatalf("code=%d", code)
					}
					got = append(got, value)
				}
			}
			for i := range got {
				if !bytes.Equal(got[i], wants[i]) || cap(got[i]) != len(got[i]) {
					t.Fatalf("prior arena slice %d invalidated: %q", i, got[i])
				}
			}
			if m.canonicalMutations.budget != wantBudget || cap(m.canonicalMutations.arena) != wantBudget {
				t.Fatalf("budget=%d cap=%d want=%d (opaque bytes excluded)", m.canonicalMutations.budget, cap(m.canonicalMutations.arena), wantBudget)
			}
		})
	}
}

type canonicalMutationCapture struct {
	sessionLeaseCapture
	mutations     []TransitionMutation
	before, after [32]byte
}

func (c *canonicalMutationCapture) AppendTransition(dst []byte, transition CapturedTransition) ([]byte, error) {
	c.mutations = c.mutations[:0]
	c.before, c.after = transition.BeforeDataChainDigest, transition.AfterDataChainDigest
	for i := 0; i < transition.MutationCount(); i++ {
		mutation := transition.Mutation(i)
		c.mutations = append(c.mutations, TransitionMutation{
			Key: bytes.Clone(mutation.Key), Before: bytes.Clone(mutation.Before), After: bytes.Clone(mutation.After),
		})
	}
	return c.sessionLeaseCapture.AppendTransition(dst, transition)
}

func TestCanonicalMutationCaptureMatchesDurableAfterImageAndConditions(t *testing.T) {
	fixture := newCapturedRelationBundleFixture(t)
	capture := &canonicalMutationCapture{sessionLeaseCapture: sessionLeaseCapture{target: fixture.options.TransitionCaptureTarget}}
	if err := fixture.machine.BeginTransitionCapture(capture); err != nil {
		t.Fatal(err)
	}
	raw := []byte(" {\"z\":\"\u2028\",\"a\":1.0} ")
	want := []byte(`{"a":1.0,"z":"\u2028"}`)
	for ordinal, kind := range []replication.MutationKind{replication.MutationPut, replication.MutationPutAbsentOrEqual} {
		command := fixture.command(t, uint64(ordinal+1), replication.RelationMutationBatch{Relation: 1,
			Mutations: []replication.Mutation{{Kind: kind, Key: []byte("k"), Value: raw}},
		})
		if err := fixture.machine.AdmitCommand(command); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(uint64(ordinal+3)), command); err != nil {
			t.Fatal(err)
		}
		completion, _, _ := openMutationCompletion(t, fixture.machine, command)
		if completion.ResultCode != ResultApplied {
			t.Fatalf("canonical conditional comparison refused: %d", completion.ResultCode)
		}
		stored, found, err := fixture.base.Collection.AppendRaw(nil, []byte("k"))
		if err != nil || !found || !bytes.Equal(stored, want) {
			t.Fatalf("stored=%q found=%v err=%v", stored, found, err)
		}
		if ordinal == 0 && (len(capture.mutations) != 1 || !bytes.Equal(capture.mutations[0].After, stored)) {
			t.Fatalf("capture afterimage differs from durable image: %+v", capture.mutations)
		}
		if ordinal == 1 && len(capture.mutations) != 0 {
			t.Fatal("equal canonical conditional put emitted a false transition")
		}
		if ordinal == 0 {
			digest := mustDataChainTransitionDigest(t, nil, capture.before, fixture.machine.relations[0].contract,
				[]finalMutation{{key: []byte("k"), value: stored}})
			if capture.after != digest {
				t.Fatalf("source data-chain hashed different afterimage: %x want=%x", capture.after, digest)
			}
		}
	}
}

func TestCanonicalMutationMultiRelationDurableImage(t *testing.T) {
	fixture := newMultiJSONRelationBundleFixture(t, true)
	batches := []replication.RelationMutationBatch{{Relation: 1}, {Relation: 2}}
	for i := range batches {
		for key := 0; key < 8; key++ {
			batches[i].Mutations = append(batches[i].Mutations, replication.Mutation{
				Kind: replication.MutationPut, Key: []byte{byte(key)}, Value: []byte(fmt.Sprintf(` { "z": %d, "a": %d } `, key, i)),
			})
		}
	}
	command := fixture.command(t, 1, batches...)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	for i, collection := range []*durable.Collection{fixture.base.Collection, fixture.global.Collection} {
		for key := 0; key < 8; key++ {
			value, found, err := collection.AppendRaw(nil, []byte{byte(key)})
			if err != nil || !found || string(value) != fmt.Sprintf(`{"a":%d,"z":%d}`, i, key) {
				t.Fatalf("relation=%d key=%d value=%q found=%v err=%v", i, key, value, found, err)
			}
		}
	}
}

func TestCanonicalMutationBoundsMalformedAndRetention(t *testing.T) {
	raw := []byte("{\"s\":\"\u2028\"}")
	m, _ := canonicalPlanFixture(t, replication.RelationMutationBatch{Relation: 1,
		Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("k"), Value: raw}},
	})
	for _, malformed := range [][]byte{nil, []byte(`{"x":`), []byte(`{"x":01}`), []byte(`{} trailing`)} {
		if _, code := m.canonicalMutationValue(malformed); code != ResultInvalidDocument {
			t.Fatalf("malformed=%q code=%d", malformed, code)
		}
	}
	if !m.reserveCanonicalMutations() {
		t.Fatal("reserve")
	}
	m.canonicalMutations.budget--
	if _, code := m.canonicalMutationValue(raw); code != ResultTargetBound || len(m.canonicalMutations.arena) != 0 {
		t.Fatal("insufficient preflight budget did not fail before arena mutation")
	}
	m.canonicalMutations.arena = make([]byte, maxNormalBatchRetainedBufferBytes+1)
	m.canonicalMutations.release()
	if m.canonicalMutations.arena != nil || m.canonicalMutations.entries != nil || m.canonicalMutations.rows != nil || len(m.canonicalMutations.command.Bytes()) != 0 {
		t.Fatal("large scratch or borrowed input retained after plan release")
	}
}

func TestCanonicalMutationExpansionRespectsDocumentLimit(t *testing.T) {
	raw := []byte("{\"s\":\"\u2028\"}")
	fixture := newRelationBundleFixtureWithCollectionOptions(t, true, false,
		durable.Options{MaxDocumentBytes: len(raw), InlineValueBytes: len(raw)}, durable.Options{})
	command := fixture.command(t, 1, replication.RelationMutationBatch{Relation: 1,
		Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("k"), Value: raw}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	completion, _, _ := openMutationCompletion(t, fixture.machine, command)
	if completion.ResultCode != ResultTargetBound || fixture.base.Collection.Len() != 0 {
		t.Fatalf("result=%d rows=%d", completion.ResultCode, fixture.base.Collection.Len())
	}
}

func TestCanonicalMutationWarmZeroAllocations(t *testing.T) {
	for _, raw := range []string{`{"a":1,"z":2}`, ` { "z":2, "a":1 } `} {
		value := []byte(raw)
		m, view := canonicalPlanFixture(t, replication.RelationMutationBatch{Relation: 1,
			Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("k"), Value: value}},
		})
		if _, code := m.canonicalMutationValue(value); code != ResultApplied {
			t.Fatal(code)
		}
		allocs := testing.AllocsPerRun(100, func() {
			m.canonicalMutations.begin(view, nil)
			if _, code := m.canonicalMutationValue(value); code != ResultApplied {
				panic(code)
			}
		})
		if allocs != 0 {
			t.Fatalf("%q allocations=%g", raw, allocs)
		}
	}
}

func TestCanonicalMutationPreservesUnknownRelationResult(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	command := commandValue(fixture.binding, 1)
	command.Batches = []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(` { "n": 1 } `)}}},
		{Relation: 2, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{}`)}}},
	}
	encoded := encodeCommand(t, command)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), encoded); err != nil {
		t.Fatal(err)
	}
	completion, _, _ := openMutationCompletion(t, fixture.machine, encoded)
	if completion.ResultCode != ResultUnknownRelation || fixture.user.Collection.Len() != 0 {
		t.Fatalf("result=%d rows=%d", completion.ResultCode, fixture.user.Collection.Len())
	}
}

func TestCanonicalMutationBatchOwnsPriorPlanBeforeArenaReuse(t *testing.T) {
	fixture := newNormalBatchFixture(t, 0, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	sessions := openDistinctBatchSessions(t, fixture.machine, fixture.binding, 2, 2)
	commands := make([][]byte, 2)
	for i, mutation := range []replication.Mutation{
		{Kind: replication.MutationPut, Key: []byte("first"), Value: []byte(" {\"z\":\"\u2028\",\"a\":1} ")},
		{Kind: replication.MutationPut, Key: []byte("second"), Value: []byte(` { "z": 2, "a": 2 } `)},
	} {
		command := commandValue(fixture.binding, 1)
		command.ClientID, command.ClientEpoch = sessions[i].ClientID, sessions[i].ClientEpoch
		command.Batches[0].Mutations = []replication.Mutation{mutation}
		commands[i] = encodeCommand(t, command)
	}
	entries := normalBatchEntries(4, commands...)
	if count, _, err := fixture.machine.ApplyNormalBatch(entries, normalBatchWitnesses(entries)); err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	for _, test := range []struct{ key, want string }{
		{"first", `{"a":1,"z":"\u2028"}`}, {"second", `{"a":2,"z":2}`},
	} {
		value, found, err := fixture.user.Collection.AppendRaw(nil, []byte(test.key))
		if err != nil || !found || string(value) != test.want {
			t.Fatalf("%s value=%q found=%v err=%v", test.key, value, found, err)
		}
	}
}

func BenchmarkCanonicalMutationValue(b *testing.B) {
	for _, raw := range []string{`{"a":1,"z":2}`, ` { "z":2, "a":1 } `} {
		b.Run(raw, func(b *testing.B) {
			value := []byte(raw)
			m, view := canonicalPlanFixture(b, replication.RelationMutationBatch{Relation: 1,
				Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("k"), Value: value}},
			})
			if _, code := m.canonicalMutationValue(value); code != ResultApplied {
				b.Fatal(code)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.canonicalMutations.begin(view, nil)
				if _, code := m.canonicalMutationValue(value); code != ResultApplied {
					b.Fatal(code)
				}
			}
		})
	}
}
