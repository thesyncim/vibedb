package rf3testfixture

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
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

func TestCombineProcessManifestsWithNodeMetadataPreservesPhysicalGenesis(t *testing.T) {
	seedNode := rafttransport.NodeID{1}
	seed := nodecontrol.BootstrapGatewaySeed{NodeID: seedNode, Incarnation: 1,
		ControlAddress: "127.0.0.1:7701", SPKIPinDigest: replication.Digest{2}}
	metadata := ProcessNodeMetadata{
		NodeLog: ProcessNodeLogManifest{Format: 1, Path: filepath.Join(t.TempDir(), "node-log"),
			KeyID: "node-key", KeyMaterialPath: filepath.Join(t.TempDir(), "node-key-material"),
			Options: raftstore.NodeStoreOptions{MaxGroups: 64}},
		NodeIncarnation:      1,
		CanonicalSourceSeeds: []nodecontrol.BootstrapGatewaySeed{seed},
		CatalogGenesis:       []byte(`{"plan_path":"/tmp/plan","catalog_path":"/tmp/catalog","initial_node_directory":"/tmp/directory","session_journal":"/tmp/session","client_id":"0102030405060708090a0b0c0d0e0f10","retry_home":"0102030405060708","distribution":"catalog","shard":"controlplane","cluster_id":"0102030405060708090a0b0c0d0e0f10","cluster_incarnation":"1112131415161718191a1b1c1d1e1f20","topology_recovery_epoch":1,"allocation_generation":1,"shard_incarnation":"2122232425262728292a2b2c2d2e2f30","group_id":"3132333435363738393a3b3c3d3e3f40","member_id":1,"store_id":"4142434445464748494a4b4c4d4e4f50","node_id":"0102030405060708090a0b0c0d0e0f10","node_incarnation":1,"relation":1}`),
	}
	raw, err := CombineProcessManifestsWithNodeMetadata(metadata,
		processBundleTestManifest(), processBundleTestManifest())
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := vibejson.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"node_log", "node_incarnation", "canonical_source_seeds", "catalog_genesis", "groups"} {
		if _, found := parsed.Get(name); !found {
			t.Fatalf("combined managed manifest lost %q", name)
		}
	}
	if position := bytes.Index(raw, []byte(`"authorization_policy"`)); position < 0 ||
		bytes.Index(raw[position:], []byte(`"canonical_source_seeds"`)) < 0 ||
		bytes.Index(raw[position:], []byte(`"replica_control"`)) < 0 {
		t.Fatal("source seeds were not placed after authorization policy")
	}
	if policy := bytes.Index(raw, []byte(`"authorization_policy"`)); bytes.Index(raw[policy:], []byte(`"canonical_source_seeds"`)) >
		bytes.Index(raw[policy:], []byte(`"replica_control"`)) {
		t.Fatal("source seeds were placed after replica control")
	}
	if position := bytes.Index(raw, []byte(`"split_control"`)); position < 0 || bytes.Index(raw[position:], []byte(`"catalog_genesis"`)) < 0 {
		t.Fatal("catalog genesis was not placed after split control")
	}
}

func TestCombineManagedProcessManifestsPrefixesPhysicalNodeIdentity(t *testing.T) {
	root := t.TempDir()
	raw, err := CombineManagedProcessManifests(root, processBundleTestManifest(), processBundleTestManifest())
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := vibejson.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"node_log", "node_incarnation", "groups"} {
		if _, found := parsed.Get(name); !found {
			t.Fatalf("managed combined manifest lost %q", name)
		}
	}
	log, _ := parsed.Get("node_log")
	path, _ := log.Get("path")
	if actual, ok := path.Text(); !ok || actual != filepath.Join(root, "node-log") {
		t.Fatalf("managed node log path=%q", actual)
	}
}
