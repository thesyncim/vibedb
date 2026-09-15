package replicatedstate

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func jsonDeltaMutation(t testing.TB, key, column string, delta int64) replication.Mutation {
	t.Helper()
	descriptor, err := replication.AppendJSONInt64Delta(nil, column, delta)
	if err != nil {
		t.Fatal(err)
	}
	return replication.Mutation{Kind: replication.MutationJSONInt64Delta, Key: []byte(key), Value: descriptor}
}

func jsonDeltasMutation(t testing.TB, key string, fields []replication.JSONInt64DeltaField) replication.Mutation {
	t.Helper()
	descriptor, err := replication.AppendJSONInt64Deltas(nil, fields)
	if err != nil {
		t.Fatal(err)
	}
	return replication.Mutation{Kind: replication.MutationJSONInt64Delta, Key: []byte(key), Value: descriptor}
}

func applyJSONDelta(
	t testing.TB,
	fixture relationBundleFixture,
	sequence, index uint64, admit bool,
	mutation replication.Mutation,
) (replication.CompletionView, int64) {
	t.Helper()
	command := fixture.command(t, sequence, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{mutation},
	})
	if admit {
		if err := fixture.machine.AdmitCommand(command); err != nil {
			t.Fatalf("admit delta %d: %v", sequence, err)
		}
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(index), command); err != nil {
		t.Fatalf("apply delta %d: %v", sequence, err)
	}
	completion, rows, _ := openMutationCompletion(t, fixture.machine, command)
	return completion, rows
}

func applyJSONDeltas(
	t testing.TB,
	fixture relationBundleFixture,
	sequence, index uint64,
	admit bool,
	mutation replication.Mutation,
) (replication.CompletionView, int64) {
	t.Helper()
	command := fixture.command(t, sequence, replication.RelationMutationBatch{
		Relation: 1, Mutations: []replication.Mutation{mutation},
	})
	if admit {
		if err := fixture.machine.AdmitCommand(command); err != nil {
			t.Fatalf("admit deltas %d: %v", sequence, err)
		}
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(index), command); err != nil {
		t.Fatalf("apply deltas %d: %v", sequence, err)
	}
	completion, rows, _ := openMutationCompletion(t, fixture.machine, command)
	return completion, rows
}

func TestJSONInt64DeltaPreservesDocumentAndExactReplayAfterReopen(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	key := []byte("counter")
	seed := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: key,
			Value: []byte(`{"id":"counter","score":9223372036854775807,"keep":{"x":true}}`),
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
		t.Fatal(err)
	}

	applyIndex := uint64(4)
	var replayCommand []byte
	var replayWitness []byte
	for sequence := uint64(2); sequence <= 5; sequence++ {
		command := fixture.command(t, sequence, replication.RelationMutationBatch{
			Relation: 1, Mutations: []replication.Mutation{jsonDeltaMutation(t, string(key), "score", 1)},
		})
		if err := fixture.machine.AdmitCommand(command); err != nil {
			t.Fatalf("admit delta %d: %v", sequence, err)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(applyIndex), command); err != nil {
			t.Fatalf("apply delta %d: %v", sequence, err)
		}
		applyIndex++
		completion, rows, witness := openMutationCompletion(t, fixture.machine, command)
		if completion.ResultCode != ResultApplied || rows != 1 {
			t.Fatalf("delta %d completion=%+v rows=%d", sequence, completion, rows)
		}
		// A Raft retry of the same request is a no-op and retains the exact
		// completion, even though it arrives at a later log index.
		if sequence == 4 {
			replayCommand = bytes.Clone(command)
			replayWitness = bytes.Clone(witness)
			if _, err := fixture.machine.ApplyNormal(normalMeta(applyIndex), command); err != nil {
				t.Fatalf("replay delta %d: %v", sequence, err)
			}
			applyIndex++
			_, retryRows, retryWitness := openMutationCompletion(t, fixture.machine, command)
			if retryRows != rows || !bytes.Equal(retryWitness, witness) {
				t.Fatalf("replay rows=%d exact=%v", retryRows, bytes.Equal(retryWitness, witness))
			}
		}
	}

	value, found, err := fixture.base.Collection.AppendRaw(nil, key)
	if err != nil || !found {
		t.Fatalf("counter row found=%v err=%v", found, err)
	}
	if string(value) != `{"id":"counter","keep":{"x":true},"score":9223372036854775811}` {
		t.Fatalf("counter after deltas=%s", value)
	}

	reopened := reopenRelationBundleFixture(t, fixture)
	if reopened.Applied() != 8 {
		t.Fatalf("reopened applied=%d, want 8", reopened.Applied())
	}
	value, found, err = reopened.relations[0].target.Collection.AppendRaw(nil, key)
	if err != nil || !found || string(value) != `{"id":"counter","keep":{"x":true},"score":9223372036854775811}` {
		t.Fatalf("reopened counter=%s found=%v err=%v", value, found, err)
	}
	if _, err := reopened.ApplyNormal(normalMeta(applyIndex), replayCommand); err != nil {
		t.Fatalf("replay after reopen: %v", err)
	}
	_, retryRows, retryWitness := openMutationCompletion(t, reopened, replayCommand)
	if retryRows != 1 || !bytes.Equal(retryWitness, replayWitness) {
		t.Fatalf("replay after reopen rows=%d exact=%v", retryRows, bytes.Equal(retryWitness, replayWitness))
	}
	value, found, err = reopened.relations[0].target.Collection.AppendRaw(nil, key)
	if err != nil || !found || string(value) != `{"id":"counter","keep":{"x":true},"score":9223372036854775811}` {
		t.Fatalf("counter after replay=%s found=%v err=%v", value, found, err)
	}
}

func TestJSONInt64DeltaNullMissingAndWideIntegersAreDeterministic(t *testing.T) {
	tests := []struct {
		name       string
		seed       []byte
		key        string
		delta      int64
		wantCode   uint32
		wantRows   int64
		wantStored string
	}{
		{name: "null propagates", seed: []byte(`{"score":null,"keep":"x"}`), key: "null-row", delta: 1, wantCode: ResultApplied, wantRows: 1, wantStored: `{"keep":"x","score":null}`},
		{name: "missing row", key: "missing-row", delta: 1, wantCode: ResultApplied, wantRows: 0, wantStored: ""},
		{name: "positive wide result", seed: []byte(`{"score":9223372036854775807,"keep":"x"}`), key: "positive-wide-row", delta: 1, wantCode: ResultApplied, wantRows: 1, wantStored: `{"keep":"x","score":9223372036854775808}`},
		{name: "negative wide result", seed: []byte(`{"score":-9223372036854775808,"keep":"x"}`), key: "negative-wide-row", delta: -1, wantCode: ResultApplied, wantRows: 1, wantStored: `{"keep":"x","score":-9223372036854775809}`},
		{name: "positive wide returns to int64", seed: []byte(`{"score":9223372036854775808,"keep":"x"}`), key: "positive-return-row", delta: -1, wantCode: ResultApplied, wantRows: 1, wantStored: `{"keep":"x","score":9223372036854775807}`},
		{name: "negative wide returns to int64", seed: []byte(`{"score":-9223372036854775809,"keep":"x"}`), key: "negative-return-row", delta: 1, wantCode: ResultApplied, wantRows: 1, wantStored: `{"keep":"x","score":-9223372036854775808}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRelationBundleFixture(t, true)
			if test.seed != nil {
				seed := fixture.command(t, 1, replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte(test.key), Value: test.seed}}})
				if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
					t.Fatal(err)
				}
			}
			sequence := uint64(2)
			index := uint64(4)
			if test.seed == nil {
				sequence = 1
				index = 3
			}
			completion, rows := applyJSONDelta(t, fixture, sequence, index, true, jsonDeltaMutation(t, test.key, "score", test.delta))
			if completion.ResultCode != test.wantCode || rows != test.wantRows {
				t.Fatalf("completion=%+v rows=%d, want code=%d rows=%d", completion, rows, test.wantCode, test.wantRows)
			}
			value, found, err := fixture.base.Collection.AppendRaw(nil, []byte(test.key))
			if err != nil || (test.wantStored != "" && (!found || string(value) != test.wantStored)) || (test.wantStored == "" && found) {
				t.Fatalf("stored=%q found=%v err=%v", value, found, err)
			}
		})
	}

	// The descriptor carries the full signed int64 delta, including its lower
	// boundary. A zero-valued row therefore materializes MinInt64 exactly.
	fixture := newRelationBundleFixture(t, true)
	seed := fixture.command(t, 1, replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("minimum-delta-row"), Value: []byte(`{"score":0,"keep":"x"}`)}}})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
		t.Fatal(err)
	}
	completion, rows := applyJSONDelta(t, fixture, 2, 4, true, jsonDeltaMutation(t, "minimum-delta-row", "score", math.MinInt64))
	if completion.ResultCode != ResultApplied || rows != 1 {
		t.Fatalf("minimum delta completion=%+v rows=%d", completion, rows)
	}
	value, found, err := fixture.base.Collection.AppendRaw(nil, []byte("minimum-delta-row"))
	if err != nil || !found || string(value) != `{"keep":"x","score":-9223372036854775808}` {
		t.Fatalf("minimum delta stored=%q found=%v err=%v", value, found, err)
	}
}

func TestMaterializeJSONInt64DeltaMatchesCanonicalPointUpdate(t *testing.T) {
	descriptor, err := replication.AppendJSONInt64Delta(nil, "score", 1)
	if err != nil {
		t.Fatal(err)
	}
	dynamic, code := materializeJSONInt64Delta(
		[]byte(`{"keep":"x","score":41,"id":"counter"}`), descriptor, 1024,
	)
	if code != ResultApplied || string(dynamic) != `{"keep":"x","score":42,"id":"counter"}` {
		t.Fatalf("dynamic=%s code=%d", dynamic, code)
	}
	// The helper leaves unrelated raw values untouched, which is the same
	// postimage that the ordinary assignment path publishes after canonicalize.
	if !bytes.Contains(dynamic, []byte(`"keep":"x"`)) || !bytes.Contains(dynamic, []byte(`"id":"counter"`)) {
		t.Fatalf("dynamic lost unrelated fields: %s", dynamic)
	}
}

func TestMaterializeJSONInt64DeltaMatchesNullableMissingAndIntegerSpellingRules(t *testing.T) {
	descriptor, err := replication.AppendJSONInt64Delta(nil, "score", 1)
	if err != nil {
		t.Fatal(err)
	}
	updated, code := materializeJSONInt64Delta([]byte(`{"keep":"x"}`), descriptor, 1024)
	if code != ResultApplied || string(updated) != `{"keep":"x","score":null}` {
		t.Fatalf("missing nullable field update=%s code=%d", updated, code)
	}
	for _, spelling := range []string{"1.0", "1e0"} {
		t.Run(spelling, func(t *testing.T) {
			updated, code := materializeJSONInt64Delta(
				[]byte(`{"keep":"x","score":`+spelling+`}`), descriptor, 1024,
			)
			if code != ResultInvalidDocument || updated != nil {
				t.Fatalf("spelling %s update=%s code=%d", spelling, updated, code)
			}
		})
	}

	// The exact result still obeys the ordinary document-size bound. This
	// input grows by one byte on increment, so a bound equal to the old row
	// must reject the candidate before publication.
	wide := []byte(`{"keep":"x","score":99999999999999999999}`)
	updated, code = materializeJSONInt64Delta(wide, descriptor, len(wide))
	if code != ResultTargetBound || updated != nil {
		t.Fatalf("wide target bound update=%s code=%d", updated, code)
	}
}

func TestMaterializeJSONInt64DeltasAppliesSimultaneouslyAndPreservesFields(t *testing.T) {
	descriptor, err := replication.AppendJSONInt64Deltas(nil, []replication.JSONInt64DeltaField{
		{Column: "count", Delta: 1}, {Column: "total", Delta: -2},
	})
	if err != nil {
		t.Fatal(err)
	}
	updated, code := materializeJSONInt64Delta(
		[]byte(`{"id":"counter","count":41,"total":100,"keep":{"x":true}}`), descriptor, 1024,
	)
	if code != ResultApplied || string(updated) != `{"id":"counter","count":42,"total":98,"keep":{"x":true}}` {
		t.Fatalf("updated=%s code=%d", updated, code)
	}

	fixture := newRelationBundleFixture(t, true)
	key := "multi-counter"
	seed := fixture.command(t, 1, replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
		Kind: replication.MutationPut, Key: []byte(key),
		Value: []byte(`{"count":9223372036854775807,"total":-9223372036854775808,"keep":"x"}`),
	}}})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
		t.Fatal(err)
	}
	completion, rows := applyJSONDeltas(t, fixture, 2, 4, true, jsonDeltasMutation(t, key, []replication.JSONInt64DeltaField{
		{Column: "count", Delta: 1}, {Column: "total", Delta: -1},
	}))
	if completion.ResultCode != ResultApplied || rows != 1 {
		t.Fatalf("completion=%+v rows=%d", completion, rows)
	}
	value, found, err := fixture.base.Collection.AppendRaw(nil, []byte(key))
	if err != nil || !found || string(value) != `{"count":9223372036854775808,"keep":"x","total":-9223372036854775809}` {
		t.Fatalf("stored=%q found=%v err=%v", value, found, err)
	}

	// The exact same command is an idempotent replay and must not apply either
	// field a second time.
	command := fixture.command(t, 2, replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{jsonDeltasMutation(t, key, []replication.JSONInt64DeltaField{
		{Column: "count", Delta: 1}, {Column: "total", Delta: -1},
	})}})
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), command); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(6), command); err != nil {
		t.Fatal(err)
	}
	value, found, err = fixture.base.Collection.AppendRaw(nil, []byte(key))
	if err != nil || !found || string(value) != `{"count":9223372036854775808,"keep":"x","total":-9223372036854775809}` {
		t.Fatalf("replay stored=%q found=%v err=%v", value, found, err)
	}
	reopened := reopenRelationBundleFixture(t, fixture)
	value, found, err = reopened.relations[0].target.Collection.AppendRaw(nil, []byte(key))
	if err != nil || !found || string(value) != `{"count":9223372036854775808,"keep":"x","total":-9223372036854775809}` {
		t.Fatalf("reopened multi stored=%q found=%v err=%v", value, found, err)
	}
}

func TestJSONInt64DeltasNullMissingAndInvalidDescriptorAreDeterministic(t *testing.T) {
	descriptor, err := replication.AppendJSONInt64Deltas(nil, []replication.JSONInt64DeltaField{
		{Column: "count", Delta: 1}, {Column: "total", Delta: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	updated, code := materializeJSONInt64Delta([]byte(`{"count":null,"keep":"x"}`), descriptor, 1024)
	if code != ResultApplied || string(updated) != `{"count":null,"keep":"x","total":null}` {
		t.Fatalf("null/missing updated=%s code=%d", updated, code)
	}
	for _, raw := range []string{`{"count":1.0,"total":2}`, `{"count":1e0,"total":2}`} {
		updated, code = materializeJSONInt64Delta([]byte(raw), descriptor, 1024)
		if code != ResultInvalidDocument || updated != nil {
			t.Fatalf("raw=%s updated=%s code=%d", raw, updated, code)
		}
	}
	bad := append([]byte(nil), descriptor...)
	// Both columns have five bytes. Make the second field a duplicate without
	// changing its framing; the apply helper must fail closed.
	firstLen := int(binary.LittleEndian.Uint16(bad[8:10]))
	second := 8 + 10 + firstLen
	copy(bad[second+10:second+10+firstLen], bad[18:18+firstLen])
	updated, code = materializeJSONInt64Delta([]byte(`{"count":1,"total":2}`), bad, 1024)
	if code != ResultInvalidDocument || updated != nil {
		t.Fatalf("duplicate descriptor updated=%s code=%d", updated, code)
	}

	// A failure in one assignment must leave the row untouched; no partially
	// built multi-field postimage may enter the relation or data chain.
	fixture := newRelationBundleFixture(t, true)
	key := "multi-invalid"
	original := []byte(`{"count":1,"total":1.0,"keep":"x"}`)
	seed := fixture.command(t, 1, replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{
		Kind: replication.MutationPut, Key: []byte(key), Value: original,
	}}})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
		t.Fatal(err)
	}
	before, found, err := fixture.base.Collection.AppendRaw(nil, []byte(key))
	if err != nil || !found {
		t.Fatalf("invalid multi seed found=%v err=%v", found, err)
	}
	completion, rows := applyJSONDeltas(t, fixture, 2, 4, false, jsonDeltasMutation(t, key, []replication.JSONInt64DeltaField{
		{Column: "count", Delta: 1}, {Column: "total", Delta: 1},
	}))
	if completion.ResultCode != ResultInvalidDocument || rows != 0 {
		t.Fatalf("invalid multi completion=%+v rows=%d", completion, rows)
	}
	value, found, err := fixture.base.Collection.AppendRaw(nil, []byte(key))
	if err != nil || !found || !bytes.Equal(value, before) {
		t.Fatalf("invalid multi changed row=%q found=%v err=%v", value, found, err)
	}
}
