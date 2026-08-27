package rf3testfixture

import (
	"bytes"
	"errors"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
)

func TestCombineProcessManifestsBuildsStrictFourGroupBundle(t *testing.T) {
	documents := make([][]byte, 4)
	for index := range documents {
		documents[index] = []byte(`{"wal":{"path":"wal-` + string(rune('a'+index)) +
			`"},"sql":{"path":"sql-` + string(rune('a'+index)) +
			`"},"route":{"group":` + string(rune('1'+index)) +
			`},"listeners":{"peer":"peer","native":"native","snapshot":"snapshot","control":"control"},"tls":{"certificate":"cert","key":"key","roots":"roots","identity_oid":"oid"},"authorization_policy":"policy","replica_control":{"journal":"control"},"split_control":` + processBundleTestSplitControl + `,"members":[{"member_id":1}]}`)
	}
	bundle, err := CombineProcessManifests(documents...)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := vibejson.Parse(bundle)
	if err != nil {
		t.Fatal(err)
	}
	groups, ok := parsed.Get("groups")
	if !ok {
		t.Fatal("combined manifest has no groups")
	}
	values, ok := groups.Array()
	if !ok || len(values) != len(documents) {
		t.Fatalf("combined groups=%d", len(values))
	}
	for index, group := range values {
		if _, found := group.Get("listeners"); found {
			t.Fatalf("group %d retained common listener state", index)
		}
		if _, found := group.Get("wal"); !found {
			t.Fatalf("group %d lost its WAL state", index)
		}
	}
	for _, name := range processManifestCommonFields {
		if _, found := parsed.Get(name); !found {
			t.Fatalf("combined manifest lost %q", name)
		}
	}
}

func TestCombineProcessManifestsRejectsCommonCutDriftAndUnknownFields(t *testing.T) {
	base := processBundleTestManifest()
	drifted := bytes.Replace(base, []byte(`"peer":"one"`), []byte(`"peer":"two"`), 1)
	if _, err := CombineProcessManifests(base, drifted); !errors.Is(err, ErrProcessManifestBundle) {
		t.Fatalf("common drift error=%v", err)
	}
	unknown := bytes.Replace(base, []byte(`"members":[]`), []byte(`"members":[],"legacy":true`), 1)
	if _, err := CombineProcessManifests(base, unknown); !errors.Is(err, ErrProcessManifestBundle) {
		t.Fatalf("unknown field error=%v", err)
	}
}

func TestCombineProcessManifestsRejectsInputAndAggregateAboveServingBound(t *testing.T) {
	base := processBundleTestManifest()
	if _, err := CombineProcessManifests(nil, base); !errors.Is(err, ErrProcessManifestBundle) {
		t.Fatalf("empty input error=%v", err)
	}
	oversized := make([]byte, maxProcessManifestBundleBytes+1)
	if _, err := CombineProcessManifests(oversized, base); !errors.Is(err, ErrProcessManifestBundle) {
		t.Fatalf("oversized input error=%v", err)
	}
	padded := append(bytes.Clone(base), bytes.Repeat([]byte{' '}, maxProcessManifestBundleBytes/2)...)
	if _, err := CombineProcessManifests(padded, padded); !errors.Is(err, ErrProcessManifestBundle) {
		t.Fatalf("oversized aggregate error=%v", err)
	}
}

const processBundleTestSplitControl = `{"journal_path":"split","max_records":4096,"max_file_bytes":67108864,"grants":[],"child_registry":{"max_operations":8,"table":"docs","apply":{"shard_key":"/id"}}}`

func processBundleTestManifest() []byte {
	return []byte(`{"wal":{},"sql":{},"route":{},"listeners":{"peer":"one"},"tls":{},"authorization_policy":"policy","replica_control":{},"split_control":` + processBundleTestSplitControl + `,"members":[]}`)
}

func TestCombineProcessManifestsPreservesDistinctGroupTemplatesAndOneGlobalBound(t *testing.T) {
	first := processBundleTestManifest()
	second := bytes.ReplaceAll(first, []byte(`"docs"`), []byte(`"ledger"`))
	second = bytes.ReplaceAll(second, []byte(`"/id"`), []byte(`"/home"`))
	second = bytes.ReplaceAll(second, []byte(`"max_operations":8`), []byte(`"max_operations":3`))
	raw, err := CombineProcessManifests(first, second)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := vibejson.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	shared, _ := parsed.Get("split_control")
	if _, found := shared.Get("child_registry"); found {
		t.Fatal("shared control retained a group-specific schema")
	}
	bound, _ := shared.Get("max_operations")
	if value, ok := bound.Uint64(); !ok || value != 8 {
		t.Fatalf("global bound=%d, want max group bound 8, not summed bound 11", value)
	}
	groups, _ := parsed.Get("groups")
	for index, key := range []string{"/id", "/home"} {
		group, _ := groups.Index(index)
		registry, found := group.Get("child_registry")
		apply, _ := registry.Get("apply")
		pointer, _ := apply.Get("shard_key")
		if actual, ok := pointer.Text(); !found || !ok || actual != key {
			t.Fatalf("group %d key=%q want=%q", index, actual, key)
		}
	}
}

func TestCombineProcessManifestsRejectsInvalidSplitControl(t *testing.T) {
	base := processBundleTestManifest()
	for _, replacement := range []string{
		`{}`, `{"max_operations":0}`, `{"max_operations":-1}`, `{"max_operations":1.5}`, `{"max_operations":65}`,
	} {
		bad := bytes.Replace(base, []byte(`{"max_operations":8,"table":"docs","apply":{"shard_key":"/id"}}`), []byte(replacement), 1)
		if _, err := CombineProcessManifests(base, bad); !errors.Is(err, ErrProcessManifestBundle) {
			t.Fatalf("invalid registry %s error=%v", replacement, err)
		}
	}
	for _, pair := range [][2]string{{`"journal_path":"split"`, `"journal_path":"other"`},
		{`"max_records":4096`, `"max_records":4097`}, {`"max_file_bytes":67108864`, `"max_file_bytes":67108865`},
		{`"grants":[]`, `"grants":[1]`}} {
		bad := bytes.Replace(base, []byte(pair[0]), []byte(pair[1]), 1)
		if _, err := CombineProcessManifests(base, bad); !errors.Is(err, ErrProcessManifestBundle) {
			t.Fatalf("shared control drift %s error=%v", pair[0], err)
		}
	}
}
