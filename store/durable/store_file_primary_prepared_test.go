package durable

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

func openPreparedPrimaryTestCollection(
	t *testing.T, name string, options Options,
) (*Collection, []string, [][]byte) {
	t.Helper()
	built, keys, values := buildFilePrimaryCorpus(t, 256)
	file := createPrimaryPointFile(t, built, options, name)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	return collection, keys, values
}

func preparedPrimaryTestInput(
	t *testing.T, raw []byte,
) primaryPreparedPutInput {
	t.Helper()
	canonical, err := canonicalPreparedPrimaryTestValue(raw)
	if err != nil {
		t.Fatal(err)
	}
	return primaryPreparedPutInput{
		raw: raw, rawLength: len(raw), canonical: canonical, prepared: true,
	}
}

func canonicalPreparedPrimaryTestValue(src []byte) ([]byte, error) {
	return vibejson.AppendCanonicalize(nil, src)
}

func TestPrimaryPreparedPutParityAndCanonicalJournalBytes(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible, RecoveryJournal: true,
		CheckpointStrength: CheckpointFilesystem,
	}
	rawCollection, rawKeys, _ := openPreparedPrimaryTestCollection(
		t, "prepared-parity-raw.vibe", options,
	)
	preparedCollection, preparedKeys, _ := openPreparedPrimaryTestCollection(
		t, "prepared-parity-prepared.vibe", options,
	)
	rawKey := []byte(rawKeys[97])
	preparedKey := []byte(preparedKeys[97])
	raw := []byte(`{ "z": [ 3, 2, 1 ], "message": "prepared parity", "n": 7 }`)
	input := preparedPrimaryTestInput(t, raw)
	if bytes.Equal(raw, input.canonical) {
		t.Fatal("parity fixture is already canonical")
	}

	rawBefore := rawCollection.Stats()
	preparedBefore := preparedCollection.Stats()
	rawCreated, rawErr := rawCollection.putPrimaryWithSplit(rawKey, raw)
	preparedCreated, preparedErr := preparedCollection.putPrimaryPreparedWithSplit(
		preparedKey, input,
	)
	if rawErr != nil || preparedErr != nil || rawCreated != preparedCreated || rawCreated {
		t.Fatalf(
			"raw=(created=%v err=%v) prepared=(created=%v err=%v)",
			rawCreated, rawErr, preparedCreated, preparedErr,
		)
	}
	assertPrimaryRaw(t, rawCollection, rawKeys[97], input.canonical, true)
	assertPrimaryRaw(t, preparedCollection, preparedKeys[97], input.canonical, true)

	rawAfter := rawCollection.Stats()
	preparedAfter := preparedCollection.Stats()
	if rawAfter.JournalAcks-rawBefore.JournalAcks != 1 ||
		preparedAfter.JournalAcks-preparedBefore.JournalAcks != 1 ||
		rawAfter.ChainAcks != rawBefore.ChainAcks ||
		preparedAfter.ChainAcks != preparedBefore.ChainAcks {
		t.Fatalf(
			"journal/chain telemetry raw=%+v->%+v prepared=%+v->%+v",
			rawBefore, rawAfter, preparedBefore, preparedAfter,
		)
	}
	rawBytes := rawAfter.JournalGroupBytes.Sum - rawBefore.JournalGroupBytes.Sum
	preparedBytes := preparedAfter.JournalGroupBytes.Sum - preparedBefore.JournalGroupBytes.Sum
	wantBytes := uint64(storeio.RecoveryRecordPaddedSize(
		rawCollection.journal.Header().SectorSize,
		len(rawKey), len(input.canonical),
	))
	if rawBytes != wantBytes || preparedBytes != wantBytes {
		t.Fatalf(
			"journal bytes raw=%d prepared=%d, want canonical record %d",
			rawBytes, preparedBytes, wantBytes,
		)
	}
	if rawAfter.ConcurrentPrimaryFallbacks != rawBefore.ConcurrentPrimaryFallbacks ||
		preparedAfter.ConcurrentPrimaryFallbacks != preparedBefore.ConcurrentPrimaryFallbacks ||
		rawAfter.ConcurrentPrimaryPublishGroups != rawBefore.ConcurrentPrimaryPublishGroups ||
		preparedAfter.ConcurrentPrimaryPublishGroups != preparedBefore.ConcurrentPrimaryPublishGroups {
		t.Fatalf("prepared baseline changed concurrent telemetry")
	}
	if rawAfter.PublishedGeneration-rawBefore.PublishedGeneration !=
		preparedAfter.PublishedGeneration-preparedBefore.PublishedGeneration ||
		rawAfter.PrimaryCompactColumnPatchAttempts-
			rawBefore.PrimaryCompactColumnPatchAttempts !=
			preparedAfter.PrimaryCompactColumnPatchAttempts-
				preparedBefore.PrimaryCompactColumnPatchAttempts ||
		rawAfter.PrimaryCompactColumnPatches-rawBefore.PrimaryCompactColumnPatches !=
			preparedAfter.PrimaryCompactColumnPatches-
				preparedBefore.PrimaryCompactColumnPatches ||
		rawAfter.PrimaryOverlayFolds-rawBefore.PrimaryOverlayFolds !=
			preparedAfter.PrimaryOverlayFolds-preparedBefore.PrimaryOverlayFolds ||
		rawAfter.AutomaticCheckpoints-rawBefore.AutomaticCheckpoints !=
			preparedAfter.AutomaticCheckpoints-preparedBefore.AutomaticCheckpoints {
		t.Fatalf(
			"baseline telemetry drift raw=%+v->%+v prepared=%+v->%+v",
			rawBefore, rawAfter, preparedBefore, preparedAfter,
		)
	}
}

func TestPrimaryPreparedPutErrorPrecedence(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	preflight := io.ErrUnexpectedEOF
	tests := []struct {
		name       string
		key        func(*Collection) []byte
		raw        []byte
		rawLength  int
		want       error
		beforeCall func(*Collection)
	}{
		{
			name: "key", key: func(c *Collection) []byte {
				return bytes.Repeat([]byte("k"), c.options.MaxKeyBytes+1)
			}, raw: []byte(`{`), rawLength: 1, want: ErrKeyTooLarge,
		},
		{
			name: "empty-document", key: func(*Collection) []byte { return []byte("key") },
			raw: nil, rawLength: 0, want: ErrDocumentTooLarge,
		},
		{
			name: "document", key: func(*Collection) []byte { return []byte("key") },
			raw: bytes.Repeat([]byte("x"), 257), rawLength: 257,
			want: ErrDocumentTooLarge,
		},
		{
			name: "preflight", key: func(*Collection) []byte { return []byte("key") },
			raw: []byte(`{`), rawLength: 1, want: preflight,
		},
		{
			name: "persistence", key: func(*Collection) []byte { return nil },
			raw: nil, rawLength: 0,
			beforeCall: func(c *Collection) { c.poisonJournal(io.ErrClosedPipe) },
			want:       io.ErrClosedPipe,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			collection, _, _ := openPreparedPrimaryTestCollection(
				t, "prepared-precedence-"+test.name+".vibe", options,
			)
			inputRaw := test.raw
			inputLength := test.rawLength
			if test.name == "document" {
				inputRaw = bytes.Repeat(
					[]byte("x"), collection.options.MaxDocumentBytes+1,
				)
				inputLength = len(inputRaw)
			}
			if test.beforeCall != nil {
				test.beforeCall(collection)
			}
			_, err := collection.putPrimaryPreparedWithSplit(
				test.key(collection), primaryPreparedPutInput{
					raw: inputRaw, rawLength: inputLength,
					preflightErr: preflight, prepared: true,
				},
			)
			if test.name == "preflight" && err != preflight {
				t.Fatalf("prepared preflight identity = %v, want exact %v", err, preflight)
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("prepared error = %v, want %v", err, test.want)
			}
		})
	}

	closed, _, _ := openPreparedPrimaryTestCollection(
		t, "prepared-precedence-closed.vibe", options,
	)
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := closed.putPrimaryPreparedWithSplit(nil, primaryPreparedPutInput{
		preflightErr: preflight, prepared: true,
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("closed prepared error = %v, want ErrClosed", err)
	}
}

func TestPrimaryPreparedPutFallbacksAndRawLengthInvariant(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	tests := []struct {
		name       string
		input      func([]byte) primaryPreparedPutInput
		wantErr    error
		wantCreate bool
	}{
		{
			name: "not-prepared", wantCreate: true,
			input: func(raw []byte) primaryPreparedPutInput {
				return primaryPreparedPutInput{raw: raw, rawLength: len(raw)}
			},
		},
		{
			name: "index-full-fallback", wantCreate: true,
			input: func(raw []byte) primaryPreparedPutInput {
				return primaryPreparedPutInput{
					raw: raw, rawLength: len(raw),
					preflightErr: document.ErrIndexFull, prepared: true,
				}
			},
		},
		{
			name: "index-full-raw-invalid", wantErr: io.ErrUnexpectedEOF,
			input: func([]byte) primaryPreparedPutInput {
				raw := []byte(`{`)
				return primaryPreparedPutInput{
					raw: raw, rawLength: len(raw),
					preflightErr: document.ErrIndexFull, prepared: true,
				}
			},
		},
		{
			name: "raw-length-mismatch", wantErr: storeio.ErrInvalidWrite,
			input: func(raw []byte) primaryPreparedPutInput {
				return primaryPreparedPutInput{
					raw: raw, rawLength: len(raw) - 1,
					canonical: raw, prepared: true,
				}
			},
		},
		{
			name: "negative-raw-length", wantErr: storeio.ErrInvalidWrite,
			input: func(raw []byte) primaryPreparedPutInput {
				return primaryPreparedPutInput{
					raw: raw, rawLength: -1,
					canonical: raw, prepared: true,
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			collection, _, _ := openPreparedPrimaryTestCollection(
				t, "prepared-fallback-"+test.name+".vibe", options,
			)
			raw := []byte(`{ "fallback": true, "n": 1 }`)
			input := test.input(raw)
			key := []byte("prepared-fallback-" + test.name)
			created, err := collection.putPrimaryPreparedWithSplit(key, input)
			if test.wantErr != nil {
				if test.name == "index-full-raw-invalid" {
					if err == nil || errors.Is(err, document.ErrIndexFull) {
						t.Fatalf("raw invalid fallback error = %v", err)
					}
					return
				}
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("fallback error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil || created != test.wantCreate {
				t.Fatalf("fallback = created %v err %v", created, err)
			}
			canonical, canonicalErr := canonicalPreparedPrimaryTestValue(raw)
			if canonicalErr != nil {
				t.Fatal(canonicalErr)
			}
			assertPrimaryRaw(t, collection, string(key), canonical, true)
		})
	}
}

func TestPrimaryPreparedPutSchemaForcesRawValidation(t *testing.T) {
	schema, err := store.CompileSchema(store.SchemaDefinition{
		Root: store.SchemaObject,
		Fields: []store.SchemaField{{
			Path: "/id", Types: store.SchemaInteger, Required: true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		Collection: store.Options{Schema: schema},
	}
	file, err := os.CreateTemp(t.TempDir(), "prepared-schema-*.vibe")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = collection.Close() })
	raw := []byte(`{"not_id":true}`)
	input := primaryPreparedPutInput{
		raw: raw, rawLength: len(raw), canonical: []byte(`{"id":7}`), prepared: true,
	}
	_, err = collection.putPrimaryPreparedWithSplit([]byte("schema-key"), input)
	if err == nil {
		t.Fatal("schema prepared input bypassed raw schema validation")
	}
}

func TestPrimaryPreparedPutSplitParity(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	rawCollection, _, _ := openPreparedPrimaryTestCollection(
		t, "prepared-split-raw.vibe", options,
	)
	preparedCollection, _, _ := openPreparedPrimaryTestCollection(
		t, "prepared-split-prepared.vibe", options,
	)
	const inserts = 600
	for at := range inserts {
		key := []byte(fmt.Sprintf("prepared-split-%04d", at))
		raw := primaryStructuralSplitValue(at)
		input := preparedPrimaryTestInput(t, raw)
		rawCreated, rawErr := rawCollection.putPrimaryWithSplit(key, raw)
		preparedCreated, preparedErr := preparedCollection.putPrimaryPreparedWithSplit(
			key, input,
		)
		if rawErr != nil || preparedErr != nil || rawCreated != preparedCreated {
			t.Fatalf(
				"put %d raw=(%v,%v) prepared=(%v,%v)", at,
				rawCreated, rawErr, preparedCreated, preparedErr,
			)
		}
	}
	rawStats := rawCollection.Stats()
	preparedStats := preparedCollection.Stats()
	if rawStats.PrimaryLeafSplits == 0 ||
		preparedStats.PrimaryLeafSplits != rawStats.PrimaryLeafSplits ||
		preparedStats.PrimaryLeafSplitRequired != rawStats.PrimaryLeafSplitRequired {
		t.Fatalf(
			"split telemetry raw splits=%d required=%d prepared splits=%d required=%d",
			rawStats.PrimaryLeafSplits, rawStats.PrimaryLeafSplitRequired,
			preparedStats.PrimaryLeafSplits, preparedStats.PrimaryLeafSplitRequired,
		)
	}
}

func TestPrimaryPreparedPutSteadyStateAllocations(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible, RecoveryJournal: true,
		CheckpointStrength: CheckpointFilesystem,
	}
	prepared, keys, _ := openPreparedPrimaryTestCollection(
		t, "prepared-allocs.vibe", options,
	)
	rawCollection, rawKeys, _ := openPreparedPrimaryTestCollection(
		t, "prepared-allocs-raw.vibe", options,
	)
	key := []byte(keys[17])
	rawKey := []byte(rawKeys[17])
	raw := []byte(`{"alloc":"steady","n":1}`)
	input := preparedPrimaryTestInput(t, raw)
	if _, err := prepared.putPrimaryPreparedWithSplit(key, input); err != nil {
		t.Fatal(err)
	}
	if _, err := rawCollection.putPrimaryWithSplit(rawKey, raw); err != nil {
		t.Fatal(err)
	}
	preparedAllocs := testing.AllocsPerRun(100, func() {
		if _, err := prepared.putPrimaryPreparedWithSplit(key, input); err != nil {
			panic(err)
		}
	})
	rawAllocs := testing.AllocsPerRun(100, func() {
		if _, err := rawCollection.putPrimaryWithSplit(rawKey, raw); err != nil {
			panic(err)
		}
	})
	if preparedAllocs != rawAllocs || preparedAllocs > 1 {
		t.Fatalf(
			"steady-state allocations prepared=%v raw=%v, want no prepared overhead",
			preparedAllocs, rawAllocs,
		)
	}
}
