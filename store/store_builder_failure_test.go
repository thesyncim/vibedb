package store

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/storemem"
)

var errStoreBuilderInjectedFailure = errors.New("injected Builder build failure")

func TestStoreBuilderReleasesCompactedOwnershipOnIndexFailure(t *testing.T) {
	builder := newStoreBuilderFailureFixture(t)
	var keyMetadata, keySource blockProbe
	var packed *storePackedIndex
	_, err := builder.build(storeBuilderBuildSteps{
		buildExactIndexes: func(_ *Builder, collection *Collection, state *State) error {
			keyMetadata = blockProbe{block: state.baseKeys.block}
			keySource = blockProbe{block: state.baseKeys.sourceBlock}
			var buildErr error
			packed, buildErr = newStorePackedIndex(map[uint64][]storeIndexChunkMask{
				1: {{chunk: 0, mask: 1}},
			})
			if buildErr != nil {
				return buildErr
			}
			collection.indexes = map[string]*storeIndexBuild{
				"partial": {base: packed},
			}
			return errStoreBuilderInjectedFailure
		},
		compactDocuments: (*Builder).compactDocuments,
	})
	if !errors.Is(err, errStoreBuilderInjectedFailure) {
		t.Fatalf("Build error = %v, want injected failure", err)
	}
	keyMetadata.requireReleased(t, "key metadata")
	keySource.requireReleased(t, "key source")
	if packed == nil || packed.block != nil {
		t.Fatalf("partial packed index retained block: %+v", packed)
	}
	if !builder.closed {
		t.Fatal("builder remained open after ownership transfer")
	}
}

func TestStoreBuilderReleasesCompactedOwnershipOnDocumentFailure(t *testing.T) {
	builder := newStoreBuilderFailureFixture(t)
	var keyMetadata, keySource blockProbe
	var packed []*storePackedIndex
	_, err := builder.build(storeBuilderBuildSteps{
		buildExactIndexes: func(builder *Builder, collection *Collection, state *State) error {
			keyMetadata = blockProbe{block: state.baseKeys.block}
			keySource = blockProbe{block: state.baseKeys.sourceBlock}
			if err := builder.buildExactIndexes(collection, state); err != nil {
				return err
			}
			for _, index := range collection.indexes {
				if index.base != nil {
					packed = append(packed, index.base)
				}
			}
			return nil
		},
		compactDocuments: func(*Builder, *State) error {
			return errStoreBuilderInjectedFailure
		},
	})
	if !errors.Is(err, errStoreBuilderInjectedFailure) {
		t.Fatalf("Build error = %v, want injected failure", err)
	}
	keyMetadata.requireReleased(t, "key metadata")
	keySource.requireReleased(t, "key source")
	if len(packed) == 0 {
		t.Fatal("fixture did not construct a packed index")
	}
	for position, index := range packed {
		if index.block != nil {
			t.Fatalf("packed index %d retained its block", position)
		}
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
	index := collection.indexes["value"]
	if index == nil || index.base == nil || index.base.block == nil ||
		index.base.block.Len() == 0 {
		t.Fatalf("successful Build released packed index: %+v", index)
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
	// Builder no longer exposes a public index declaration, but Build still
	// constructs any exact indexes staged on the builder and transfers their
	// owned blocks together with the documents. Stage one directly to exercise
	// that bulk-index ownership path and its release on a failed Build.
	exact, err := CompileExactIndex(IndexDefinition{Name: "value", Paths: []string{"/value"}})
	if err != nil {
		t.Fatal(err)
	}
	exact.seed = builder.seed
	builder.exact = map[string]*ExactIndex{"value": exact}
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
