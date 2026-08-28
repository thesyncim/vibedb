//go:build linux

package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
)

// The shipped RF3 front door supports native point reads, not the static SQL
// transport. Check every live base row through that public boundary and every
// cross-hosted unique index through an authenticated, ReadIndex-fenced native
// relation lookup. Local secondary indexes and exact full cardinalities are
// separately checked on all recovered, exclusively opened voters below.
func (fixture *durableRF3ExternalFixture) verifyNativeMultiRelation(
	t *testing.T, client *durableRF3ExternalWireClient,
) []time.Duration {
	t.Helper()
	ctx, err := serviceauthz.WithAuthority(fixture.ctx, serviceauthz.Authority{
		Node: fixture.nodes[fixture.observerNode], Generation: 5})
	if err != nil {
		t.Fatal(err)
	}
	executor := mustDurableRF3ExternalExecutor(t, fixture.probeClient)
	var latencies []time.Duration
	for _, table := range []struct {
		name, prefix, index string
		indexGroup          int
	}{{"orders_a", "a", "by_email_a", durableRF3DataBGroup}, {"orders_b", "b", "by_email_b", durableRF3DataAGroup}} {
		program, err := fixture.snapshot.CompileGlobalIndex(table.name, table.index)
		if err != nil {
			t.Fatal(err)
		}
		for ordinal := 0; ordinal < 12; ordinal++ {
			id := fmt.Sprintf("churn-%s-%02d", table.prefix, ordinal)
			key, ok := orderedkey.AppendString(nil, []byte(id), orderedkey.Ascending)
			if !ok {
				t.Fatal("invalid native primary key")
			}
			raw, latency := client.roundTrip(t, []byte(fmt.Sprintf(
				`{"op":"get","table":%q,"key":%q,"consistency":"linearizable"}`,
				table.name, base64.RawURLEncoding.EncodeToString(key))))
			latencies = append(latencies, latency)
			var response struct {
				OK       bool   `json:"ok"`
				Found    bool   `json:"found"`
				Applied  uint64 `json:"applied"`
				Document struct {
					ID    string `json:"id"`
					Kind  string `json:"kind"`
					Email string `json:"email"`
					Value int    `json:"value"`
				} `json:"document"`
			}
			if err := vibejson.Unmarshal(raw, &response); err != nil || !response.OK ||
				response.Applied == 0 || response.Found != (ordinal%2 == 1) {
				t.Fatalf("native %s/%s response=%s err=%v", table.name, id, raw, err)
			}
			if response.Found && (response.Document.ID != id || response.Document.Value != 1000+ordinal ||
				response.Document.Kind != fmt.Sprintf("final-%02d", ordinal) ||
				response.Document.Email != fmt.Sprintf("final-%02d@example.test", ordinal)) {
				t.Fatalf("native %s/%s returned stale or partial document: %s", table.name, id, raw)
			}
			for _, phase := range []string{"initial", "final"} {
				value := fmt.Sprintf("%s-%02d@example.test", phase, ordinal)
				var workspace gateway.GlobalIndexWorkspace
				lookup, err := program.RouteKey([]distribution.Scalar{distribution.NewString(value)}, &workspace)
				if err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				result, err := executor.ReadPoint(ctx, fixture.routes[table.indexGroup], gateway.ReplicatedPointRead{
					Relation: 2, Key: lookup.KeyTuple, MinimumApplied: 1, MaxValueBytes: 4 << 20, Linearizable: true})
				latencies = append(latencies, time.Since(start))
				wantFound := phase == "final" && ordinal%2 == 1
				locator := []string{id}
				want, marshalErr := vibejson.Marshal(&locator)
				if err != nil || marshalErr != nil || result.Found != wantFound ||
					result.Applied == 0 || wantFound && !bytes.Equal(result.Value, want) {
					t.Fatalf("native global index %s=%q found=%t value=%s want=%s err=%v",
						table.index, value, result.Found, result.Value, want, err)
				}
			}
		}
	}
	return latencies
}

// No local handle is opened while a shard process owns these stores. This is
// an independent persisted-index oracle after the public crash/replay checks,
// not an alternate serving transport or a bypass of an active writer lock.
func (fixture *durableRF3ExternalFixture) verifyRecoveredMultiRelationIndexes(t *testing.T) {
	t.Helper()
	fixture.stopGateway(t, fixture.gatewayB)
	for _, process := range fixture.shards {
		replicaProcessStop(t, process)
	}
	for member := 0; member < durableRF3ExternalVoters; member++ {
		for _, group := range []int{durableRF3DataAGroup, durableRF3DataBGroup} {
			fixture.verifyRecoveredMultiRelationRoot(t, group, member)
		}
	}
}

func (fixture *durableRF3ExternalFixture) verifyRecoveredMultiRelationRoot(t *testing.T, group, member int) {
	t.Helper()
	root := filepath.Join(fixture.root, fmt.Sprintf("role-%d-member-%d", group, member+1))
	readIdentity := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || len(raw) > 256<<10 {
			t.Fatalf("read retained %s: %v", name, err)
		}
		return raw
	}
	var base sqldriver.ReplicatedShardStoreIdentity
	var apply sqldriver.ReplicatedApplyIdentity
	if err := vibejson.Unmarshal(readIdentity("sql-identity.json"), &base); err != nil {
		t.Fatal(err)
	}
	if err := vibejson.Unmarshal(readIdentity("apply-identity.json"), &apply); err != nil {
		t.Fatal(err)
	}
	database, err := sqldriver.OpenReplicatedShardStoreWithApply(filepath.Join(root, "member.vdb"), base, apply)
	if err != nil {
		t.Fatalf("open exact recovered role %d member %d: %v", group, member+1, err)
	}
	defer database.Close()
	session, err := database.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.SetResultLimits(32, 64<<10); err != nil {
		t.Fatal(err)
	}
	table, prefix, indexTable, indexID := "orders_a", "a", "orders_b_email", uint64(42)
	indexPrefix := "b"
	if group == durableRF3DataBGroup {
		table, prefix, indexTable, indexID, indexPrefix = "orders_b", "b", "orders_a_email", 41, "a"
	}
	for ordinal := 0; ordinal < 12; ordinal++ {
		id := fmt.Sprintf("churn-%s-%02d", prefix, ordinal)
		for _, phase := range []string{"initial", "final"} {
			var expected []string
			if phase == "final" && ordinal%2 == 1 {
				expected = []string{id}
			}
			verifyRecoveredSQLIDs(t, session, "SELECT id, value FROM "+table+" WHERE kind = ?",
				[]any{fmt.Sprintf("%s-%02d", phase, ordinal)}, expected)
			key, err := distribution.CurrentTupleCodec.AppendTuple(nil,
				[]distribution.Scalar{distribution.NewString(fmt.Sprintf("%s-%02d@example.test", phase, ordinal))})
			if err != nil {
				t.Fatal(err)
			}
			var actual []string
			err = session.LookupGlobalIndex(t.Context(), indexTable, indexID, 1, key, 1, true, 2, 4096,
				func(locator []byte) error {
					var values []string
					if err := vibejson.Unmarshal(locator, &values); err != nil || len(values) != 1 {
						return fmt.Errorf("invalid exact locator %s: %v", locator, err)
					}
					actual = append(actual, values[0])
					return nil
				})
			var want []string
			if len(expected) != 0 {
				want = []string{fmt.Sprintf("churn-%s-%02d", indexPrefix, ordinal)}
			}
			if err != nil || !slices.Equal(actual, want) {
				t.Fatalf("recovered role=%d member=%d index=%s phase=%s ordinal=%d actual=%v want=%v err=%v",
					group, member+1, indexTable, phase, ordinal, actual, want, err)
			}
		}
		if ordinal%2 == 0 {
			verifyRecoveredSQLIDs(t, session, "SELECT id, value FROM "+table+" WHERE id = ?", []any{id}, nil)
		}
	}
	expected := []string{"seed-" + prefix}
	for ordinal := 1; ordinal < 12; ordinal += 2 {
		expected = append(expected, fmt.Sprintf("churn-%s-%02d", prefix, ordinal))
	}
	verifyRecoveredSQLIDs(t, session, "SELECT id, value FROM "+table, nil, expected)
	count, err := session.Prepare(t.Context(), "SELECT COUNT(*) FROM "+indexTable)
	if err != nil {
		t.Fatal(err)
	}
	defer count.Close()
	cursor, err := count.Query(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() {
		t.Fatal("global-index cardinality has no result")
	}
	if actual, ok := cursor.Cell(0).Int64(); !ok || actual != 7 || cursor.Next() {
		t.Fatalf("recovered global-index %s cardinality=%d want=7", indexTable, actual)
	}
}

func verifyRecoveredSQLIDs(t *testing.T, session *sqldriver.Session, sql string, args []any, expected []string) {
	t.Helper()
	statement, err := session.Prepare(t.Context(), sql)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	cursor, err := statement.Query(t.Context(), args)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	var actual []string
	for cursor.Next() {
		id, ok := cursor.Cell(0).Text()
		if !ok {
			t.Fatal("persisted SQL index returned a non-string ID")
		}
		actual = append(actual, id)
	}
	slices.Sort(actual)
	slices.Sort(expected)
	if !slices.Equal(actual, expected) {
		t.Fatalf("persisted query %s args=%v ids=%v want=%v", sql, args, actual, expected)
	}
}
