package store

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/storemem"
)

var errStoreBuilderInjectedFailure = errors.New("injected Builder build failure")

func TestStoreBuilderReleasesCompactedOwnershipOnDocumentFailure(t *testing.T) {
	builder := newStoreBuilderFailureFixture(t)
	var keyMetadata, keySource blockProbe
	_, err := builder.build(storeBuilderBuildSteps{
		compactDocuments: func(_ *Builder, state *State) error {
			// Key compaction has already transferred ownership into the state;
			// probe those blocks, then fail the document step so the release
			// path is what must return them.
			keyMetadata = blockProbe{block: state.baseKeys.block}
			keySource = blockProbe{block: state.baseKeys.sourceBlock}
			return errStoreBuilderInjectedFailure
		},
	})
	if !errors.Is(err, errStoreBuilderInjectedFailure) {
		t.Fatalf("Build error = %v, want injected failure", err)
	}
	keyMetadata.requireReleased(t, "key metadata")
	keySource.requireReleased(t, "key source")
	if !builder.closed {
		t.Fatal("builder remained open after ownership transfer")
	}
}

func TestStoreBuilderSuccessRetainsCompactedOwnership(t *testing.T) {
	builder := newStoreBuilderFailureFixture(t)
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	state := collection.state.Load()
	if state.baseKeys == nil || state.baseKeys.block == nil ||
		state.baseKeys.sourceBlock == nil || state.baseKeys.block.Len() == 0 ||
		state.baseKeys.sourceBlock.Len() == 0 {
		t.Fatalf("successful Build released compact keys: %+v", state.baseKeys)
	}
	if state.mappedDocs == nil || state.mappedDocs.block == nil ||
		state.mappedDocs.sourceBlock == nil || state.mappedDocs.block.Len() == 0 ||
		state.mappedDocs.sourceBlock.Len() == 0 {
		t.Fatalf("successful Build released compact documents: %+v", state.mappedDocs)
	}
	if raw, ok := collection.GetRaw("alpha"); !ok || string(raw.Bytes()) != `{"value":1}` {
		t.Fatalf("successful collection read = (%q,%v)", raw.Bytes(), ok)
	}
}

func newStoreBuilderFailureFixture(t *testing.T) *Builder {
	t.Helper()
	builder, err := NewBuilder(Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key string
		doc string
	}{
		{key: "alpha", doc: `{"value":1}`},
		{key: "beta", doc: `{"value":2}`},
		{key: "gamma", doc: `{"value":1}`},
	} {
		if err := builder.Append(row.key, []byte(row.doc)); err != nil {
			t.Fatal(err)
		}
	}
	return builder
}

type blockProbe struct {
	block *storemem.Block
}

func (p blockProbe) requireReleased(t *testing.T, name string) {
	t.Helper()
	if p.block == nil {
		t.Fatalf("%s probe was not captured", name)
	}
	if length := p.block.Len(); length != 0 {
		t.Fatalf("%s block length after failed Build = %d, want 0", name, length)
	}
}
