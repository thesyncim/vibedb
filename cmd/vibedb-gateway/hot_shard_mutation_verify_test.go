//go:build linux

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
)

type hotMutationDocument struct {
	ID    string `json:"id"`
	Kind  string `json:"kind"`
	Email string `json:"email"`
	Value int    `json:"value"`
}

// Verify committed writes through authenticated native relation reads, without
// introducing gateway read pressure into a write-driven placement test.
// General SQL remains the separate static transport; local indexes are checked
// on exclusively reopened persisted stores at the end of this process gate.
func (client *hotMutationWireClient) getMessage(t *testing.T) (hotMutationDocument, bool) {
	t.Helper()
	key, ok := orderedkey.AppendString(nil, []byte("m-0"), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode native primary key")
	}
	verifier := client.verifier
	catalog, err := verifier.catalog.Read(verifier.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var document hotMutationDocument
	found := false
	for _, descriptor := range catalog.ReplicatedShardDescriptors() {
		if descriptor.Distribution != verifier.baseRoute.Distribution {
			continue
		}
		route, ok := catalog.ResolveReplicatedRoute(descriptor.Distribution, descriptor.Shard, make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount))
		if !ok {
			t.Fatal("current base route missing")
		}
		result, err := verifier.executor.ReadPoint(verifier.ctx, route, gateway.ReplicatedPointRead{
			Relation: 1, Key: key, MinimumApplied: 1, MaxValueBytes: 4 << 20, Linearizable: true})
		if err != nil || result.Applied == 0 {
			t.Fatalf("native base read %s: %v", descriptor.Shard, err)
		}
		if result.Found {
			if found {
				t.Fatal("message duplicated across split children")
			}
			found = true
			if err := vibejson.Unmarshal(result.Value, &document); err != nil {
				t.Fatal(err)
			}
		}
	}
	return document, found
}

type hotMutationVerifier struct {
	ctx        context.Context
	snapshot   *gateway.Snapshot
	catalog    *gateway.ReplicatedCatalogAuthority
	executor   *gateway.ReplicatedExecutor
	client     *gateway.AuthenticatedReplicatedClient
	baseRoute  gateway.ReplicatedRoute
	indexRoute gateway.ReplicatedRoute
}

func newHotMutationVerifier(t *testing.T, profile *rafttransport.PeerTLS, snapshot *gateway.Snapshot, catalog *gateway.ReplicatedCatalogAuthority, baseRoute, indexRoute gateway.ReplicatedRoute) *hotMutationVerifier {
	t.Helper()
	client := durableRF3ExternalReplicatedClient(t, profile)
	t.Cleanup(func() { _ = client.Close() })
	ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: profile.LocalIdentity().Node, Generation: 5})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := gateway.NewReplicatedExecutor(client, 8, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return &hotMutationVerifier{ctx: ctx, snapshot: snapshot, catalog: catalog, executor: executor, client: client, baseRoute: baseRoute, indexRoute: indexRoute}
}

func (verifier *hotMutationVerifier) syncFinalVoters(t *testing.T, catalog *gateway.Snapshot) map[distribution.ShardID]bool {
	t.Helper()
	wants := make(map[distribution.ShardID]bool)
	for _, descriptor := range catalog.ReplicatedShardDescriptors() {
		if descriptor.Distribution != verifier.baseRoute.Distribution && descriptor.Distribution != verifier.indexRoute.Distribution {
			continue
		}
		route, ok := catalog.ResolveReplicatedRoute(descriptor.Distribution, descriptor.Shard, make([]gateway.ReplicatedEndpoint, 0, gateway.ServingReplicaCount))
		if !ok {
			t.Fatal("final relation route missing")
		}
		id := "m-0"
		if descriptor.Distribution == verifier.indexRoute.Distribution {
			id = "l-1"
		}
		key, _ := orderedkey.AppendString(nil, []byte(id), orderedkey.Ascending)
		result, err := verifier.executor.ReadPoint(verifier.ctx, route, gateway.ReplicatedPointRead{
			Relation: 1, Key: key, MinimumApplied: 1, MaxValueBytes: 4 << 20, Linearizable: true})
		if err != nil {
			t.Fatal(err)
		}
		wants[descriptor.Shard] = result.Found
		for _, replica := range route.Replicas {
			ctx, cancel := context.WithTimeout(verifier.ctx, 5*time.Second)
			probe, err := verifier.client.ProbeReplicated(ctx, route, replica, serviceauthz.CapabilityDataRead)
			if err != nil || probe == nil || !probe.HasState {
				cancel()
				t.Fatalf("final voter probe: %v", err)
			}
			replica.NodeIncarnation = probe.State.Fence.NodeIncarnation
			authority, _ := serviceauthz.FromContext(ctx)
			response, err := verifier.client.DoReplicated(ctx, replica, &shardservice.ReplicatedRequest{
				Operation: shardservice.ReplicatedReadFollower, Capability: serviceauthz.CapabilityDataRead, Authority: authority,
				Fence:    probe.State.Fence,
				Relation: 1, Key: key, MinimumApplied: result.Applied, MaxValueBytes: 4 << 20})
			cancel()
			wantKind := shardservice.ReplicatedReadMissing
			if result.Found {
				wantKind = shardservice.ReplicatedReadFound
			}
			if err != nil || response == nil || response.Kind != wantKind || response.ReadApplied < result.Applied || !bytes.Equal(response.Value, result.Value) {
				t.Fatalf("final voter %s/%d catchup: %v response=%+v", descriptor.Shard, replica.Member, err, response)
			}
		}
	}
	return wants
}

// Query persisted indexes only after every final voter has crossed the final
// read fence and every process has released its exclusive file ownership.
func hotMutationVerifyFinalStores(t *testing.T, root string, catalog *gateway.Snapshot, plan *splitcontroller.Plan,
	client *hotMutationWireClient, processes []*rf3testfixture.ExternalProcess, cold, gatewayProcess *rf3testfixture.ExternalProcess) {
	t.Helper()
	hotMutationAssertMessage(t, client, "splitting", "split@example.com", 11)
	wants := client.verifier.syncFinalVoters(t, catalog)
	_ = client.connection.Close()
	replicaProcessStop(t, gatewayProcess)
	for _, process := range processes {
		replicaProcessStop(t, process)
	}
	replicaProcessStop(t, cold)
	open := func(path string, base sqldriver.ReplicatedShardStoreIdentity, apply sqldriver.ReplicatedApplyIdentity, inspect func(*sqldriver.Session)) {
		database, err := sqldriver.OpenReplicatedShardStoreWithApply(path, base, apply)
		if err != nil {
			t.Fatalf("reopen %s: %v", path, err)
		}
		defer database.Close()
		session, err := database.NewSession(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		inspect(session)
	}
	checkMessages := func(shard distribution.ShardID) func(*sqldriver.Session) {
		return func(session *sqldriver.Session) {
			var expected []string
			if wants[shard] {
				expected = []string{"m-0"}
			}
			verifyRecoveredSQLIDs(t, session, "SELECT id FROM messages", nil, expected)
			verifyRecoveredSQLIDs(t, session, "SELECT id FROM messages WHERE kind = ?", []any{"splitting"}, expected)
			for _, kind := range []string{"initial", "changed", "steady", "final"} {
				verifyRecoveredSQLIDs(t, session, "SELECT id FROM messages WHERE kind = ?", []any{kind}, nil)
			}
		}
	}
	for _, group := range []int{0, 1} {
		members := []int{2, 3, 4}
		if group == 1 {
			members = []int{1, 2, 3}
		}
		for _, member := range members {
			memberRoot := filepath.Join(root, fmt.Sprintf("group-%d-member-%d", group, member))
			var base sqldriver.ReplicatedShardStoreIdentity
			var apply sqldriver.ReplicatedApplyIdentity
			read := func(name string) []byte {
				raw, err := os.ReadFile(filepath.Join(memberRoot, name))
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			if err := vibejson.Unmarshal(read("sql-identity.json"), &base); err != nil {
				t.Fatal(err)
			}
			if err := vibejson.Unmarshal(read("apply-identity.json"), &apply); err != nil {
				t.Fatal(err)
			}
			inspect := checkMessages(client.verifier.baseRoute.Shard)
			if group == 1 {
				inspect = func(session *sqldriver.Session) { hotMutationVerifyPersistedGlobalIndex(t, session) }
			}
			open(filepath.Join(memberRoot, "member.vdb"), base, apply, inspect)
		}
	}
	for child := uint8(0); child < 2; child++ {
		target, ok := plan.Target(child)
		if !ok {
			continue
		}
		for _, replica := range target.Replicas {
			open(replica.SQLPath, replica.SQL, replica.Apply, checkMessages(distribution.ShardID(replica.SQL.Binding.Shard)))
		}
	}
}

func hotMutationVerifyPersistedGlobalIndex(t *testing.T, session *sqldriver.Session) {
	t.Helper()
	verifyRecoveredSQLIDs(t, session, "SELECT id FROM logs", nil, []string{"l-1"})
	for _, email := range []string{"initial@example.com", "changed@example.com", "steady@example.com", "final@example.com", "split@example.com"} {
		key, err := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{distribution.NewString(email)})
		if err != nil {
			t.Fatal(err)
		}
		var actual []string
		err = session.LookupGlobalIndex(t.Context(), "messages_email", 41, 7, key, 1, true, 2, 4096, func(raw []byte) error {
			var locator []string
			if err := vibejson.Unmarshal(raw, &locator); err != nil {
				return err
			}
			actual = append(actual, locator...)
			return nil
		})
		var expected []string
		if email == "split@example.com" {
			expected = []string{"m-0"}
		}
		if err != nil || !slices.Equal(actual, expected) {
			t.Fatalf("persisted global index %s: got=%v want=%v err=%v", email, actual, expected, err)
		}
	}
}

func (verifier *hotMutationVerifier) assertEmail(t *testing.T, email string, found bool) {
	t.Helper()
	program, err := verifier.snapshot.CompileGlobalIndex("messages", "by_email")
	if err != nil {
		t.Fatal(err)
	}
	var workspace gateway.GlobalIndexWorkspace
	lookup, err := program.RouteKey([]distribution.Scalar{distribution.NewString(email)}, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	result, err := verifier.executor.ReadPoint(verifier.ctx, verifier.indexRoute, gateway.ReplicatedPointRead{
		Relation: 2, Key: lookup.KeyTuple, MinimumApplied: 1, MaxValueBytes: 4 << 20, Linearizable: true})
	if err != nil || result.Applied == 0 || result.Found != found || found && !bytes.Equal(result.Value, []byte(`["m-0"]`)) {
		t.Fatalf("native global index email=%q found=%t value=%s want found=%t err=%v", email, result.Found, result.Value, found, err)
	}
}
