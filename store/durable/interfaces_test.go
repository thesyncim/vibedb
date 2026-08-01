package durable

import (
	"errors"
	"math/bits"
	"os"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

func TestIndexSessionOwnsScratchAndReturnsDetachedMetrics(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "index-session-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Indexes = []store.IndexDefinition{{Name: "by_group", Paths: []string{"/group"}}}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	for _, key := range []string{"a", "b"} {
		if _, err := collection.Put([]byte(key), []byte(`{"group":"x"}`)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	needed, err := vibejson.RequiredIndexEntries([]byte(`"x"`))
	if err != nil {
		t.Fatal(err)
	}
	needle, err := vibejson.BuildIndex([]byte(`"x"`), make([]vibejson.IndexEntry, needed))
	if err != nil {
		t.Fatal(err)
	}

	var session IndexSession
	session.Reset(snapshot)
	masks, err := session.AppendIndexMasks(nil, "by_group", needle)
	if err != nil {
		t.Fatal(err)
	}
	masks, err = session.AppendIndexCandidateMasks(masks[:0], "by_group", needle)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for _, mask := range masks {
		rows += bits.OnesCount64(mask.Bits)
	}
	if rows != 2 {
		t.Fatalf("mask rows = %d, want 2", rows)
	}
	metrics := session.Metrics()
	if metrics.Probes != 2 || metrics.CandidateRows != 4 ||
		metrics.CertificateRows != 4 || metrics.DocumentRecheckRows != 0 ||
		metrics.MatchedRows != 4 || metrics.CandidateChunks == 0 ||
		metrics.PostingPages == 0 {
		t.Fatalf("Metrics = %+v", metrics)
	}

	// Reset starts a new detached interval without mutating the value already
	// returned to the caller.
	session.Reset(snapshot)
	if reset := session.Metrics(); reset != (IndexSessionMetrics{}) {
		t.Fatalf("metrics after Reset = %+v", reset)
	}
	if metrics.Probes != 2 || metrics.CandidateRows != 4 {
		t.Fatalf("detached Metrics changed after Reset: %+v", metrics)
	}
	if indexes := session.AppendIndexes(nil); len(indexes) != 1 || indexes[0].Name != "by_group" {
		t.Fatalf("AppendIndexes = %+v", indexes)
	}
	if _, err := session.AppendIndexMasks(nil, "by_group", needle); err != nil {
		t.Fatal(err)
	}
	beforeFailure := session.Metrics()
	if _, err := session.AppendIndexMasks(nil, "missing", needle); !errors.Is(err, store.ErrIndexNotFound) {
		t.Fatalf("missing index error = %v", err)
	}
	afterFailure := session.Metrics()
	if afterFailure.Probes != beforeFailure.Probes+1 ||
		afterFailure.CandidateRows != beforeFailure.CandidateRows ||
		afterFailure.CertificateRows != beforeFailure.CertificateRows ||
		afterFailure.DocumentRecheckRows != beforeFailure.DocumentRecheckRows ||
		afterFailure.MatchedRows != beforeFailure.MatchedRows ||
		afterFailure.CandidateChunks != beforeFailure.CandidateChunks ||
		afterFailure.PostingPages != beforeFailure.PostingPages {
		t.Fatalf("failed probe reused prior counters: before=%+v after=%+v", beforeFailure, afterFailure)
	}

	session.Release()
	if _, err := session.AppendIndexMasks(nil, "by_group", needle); !errors.Is(err, ErrClosed) {
		t.Fatalf("probe after Release error = %v", err)
	}
}

func TestIndexSessionHasNoPublicState(t *testing.T) {
	typeOfSession := reflect.TypeFor[IndexSession]()
	for i := range typeOfSession.NumField() {
		field := typeOfSession.Field(i)
		if field.IsExported() {
			t.Fatalf("IndexSession field %q is exported", field.Name)
		}
	}
}

func TestNewIndexSessionBindsSnapshot(t *testing.T) {
	session := NewIndexSession(nil)
	if session == nil {
		t.Fatal("NewIndexSession returned nil")
	}
	if got := session.AppendIndexes(nil); len(got) != 0 {
		t.Fatalf("unbound catalog = %+v", got)
	}
}
