package gatewayruntime

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibejson"
)

func rf3FixturePointRequest(table, identifier string) serveRequest {
	return serveRequest{Op: "read_batch", Class: "interactive", MaxResultBytes: 1 << 20,
		Statements: []serveStatement{{SQL: "SELECT * FROM " + table + " WHERE id = ?",
			Params: []serveParam{{Kind: "string", Text: identifier}}}}}
}

// Require the replicated read envelope and a nonzero group cut, not a legacy
// SQL row response that could have been served without a quorum ReadIndex.
func rf3FixturePointResponseMatches(raw []byte, identifier string) bool {
	if len(raw) > 1<<20 {
		return false
	}
	document, err := vibejson.Parse(raw)
	if err != nil {
		return false
	}
	fields, objectOK := document.Object()
	okNode, present := document.Get("ok")
	ok, valid := okNode.Bool()
	if !objectOK || len(fields) != 4 || !present || !valid || !ok {
		return false
	}
	foundNode, present := document.Get("found")
	found, valid := foundNode.Array()
	if !present || !valid || len(found) != 1 {
		return false
	}
	ok, valid = found[0].Bool()
	if !valid || !ok {
		return false
	}
	rowsNode, present := document.Get("documents")
	rows, valid := rowsNode.Array()
	if !present || !valid || len(rows) != 1 {
		return false
	}
	idNode, present := rows[0].Get("id")
	id, valid := idNode.Text()
	if !present || !valid || id != identifier {
		return false
	}
	observationsNode, present := document.Get("observations")
	observations, valid := observationsNode.Array()
	if !present || !valid || len(observations) != 1 {
		return false
	}
	for _, name := range []string{"applied", "topology_recovery_epoch"} {
		node, present := observations[0].Get(name)
		value, valid := node.Uint64()
		if !present || !valid || value == 0 {
			return false
		}
	}
	for _, name := range []string{"cluster_id", "cluster_incarnation", "shard_incarnation", "group_id", "route_id"} {
		node, present := observations[0].Get(name)
		value, valid := node.Text()
		var identity [32]byte
		width := 16
		if name == "route_id" {
			width = 32
		}
		if !present || !valid || decodeFixedHex(value, identity[:width]) != nil || identity == ([32]byte{}) {
			return false
		}
	}
	return true
}

func TestRF3FixturePointUsesActualReadBatchWire(t *testing.T) {
	fixture := newSQLBatchWireFixture(t, 1, false)
	fixture.request = rf3FixturePointRequest("table_000", "key-000")
	if _, err := buildNativeSQLBatchReadRequest(fixture.request); err != nil {
		t.Fatal(err)
	}
	raw, _ := roundTripSQLBatchWire(t, fixture)
	if !rf3FixturePointResponseMatches(raw, "key-000") || rf3FixturePointResponseMatches(raw, "wrong") {
		t.Fatalf("actual replicated response: %s", raw)
	}
	for _, invalid := range [][]byte{
		[]byte(`{"ok":true,"rows":[["key-000",0]]}`),
		bytes.Replace(raw, []byte(`"found":[true]`), []byte(`"found":[false]`), 1),
		bytes.Replace(raw, []byte(`"applied":20`), []byte(`"applied":0`), 1),
		raw[:len(raw)-2],
	} {
		if bytes.Equal(raw, invalid) || rf3FixturePointResponseMatches(invalid, "key-000") {
			t.Fatalf("invalid or inactive mutation: %s", invalid)
		}
	}
}
