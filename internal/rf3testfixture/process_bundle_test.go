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
			`},"listeners":{"peer":"peer","native":"native","snapshot":"snapshot","control":"control"},"tls":{"certificate":"cert","key":"key","roots":"roots","identity_oid":"oid"},"authorization_policy":"policy","replica_control":{"journal":"control"},"split_control":{"journal":"split"},"members":[{"member_id":1}]}`)
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
	base := []byte(`{"wal":{},"sql":{},"route":{},"listeners":{"peer":"one"},"tls":{},"authorization_policy":"policy","replica_control":{},"split_control":{},"members":[]}`)
	drifted := bytes.Replace(base, []byte(`"peer":"one"`), []byte(`"peer":"two"`), 1)
	if _, err := CombineProcessManifests(base, drifted); !errors.Is(err, ErrProcessManifestBundle) {
		t.Fatalf("common drift error=%v", err)
	}
	unknown := bytes.Replace(base, []byte(`"members":[]`), []byte(`"members":[],"legacy":true`), 1)
	if _, err := CombineProcessManifests(base, unknown); !errors.Is(err, ErrProcessManifestBundle) {
		t.Fatalf("unknown field error=%v", err)
	}
}
