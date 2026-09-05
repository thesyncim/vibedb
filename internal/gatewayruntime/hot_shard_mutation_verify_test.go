//go:build linux

package gatewayruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
// This keeps verification independent of either the static SQL transport or
// the replicated-catalog RF3 SELECT path. Local indexes are checked on
// exclusively reopened persisted stores at the end of this process gate.
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
		if hotMutationKeyShard(t, catalog, descriptor.Distribution, "m-0") != descriptor.Shard {
			continue // Narrowed groups must refuse points outside their range.
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

func hotMutationKeyShard(t *testing.T, catalog *gateway.Snapshot, name distribution.DistributionName, id string) distribution.ShardID {
	t.Helper()
	manifest, ok := catalog.Manifest(name)
	if !ok {
		t.Fatal("verification manifest missing")
	}
	point, err := distribution.NewNativeMapper(1).PointFor([]distribution.Scalar{distribution.NewString(id)})
	if err != nil {
		t.Fatal(err)
	}
	shard, ok := manifest.ResolvePoint(point)
	if !ok {
		t.Fatal("verification point is not covered")
	}
	return shard
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
		ownsRow := hotMutationKeyShard(t, catalog, descriptor.Distribution, id) == descriptor.Shard
		if !ownsRow {
			// Fence every non-owning voter using an absent key in its own
			// range. Offline SQL below still proves m-0 was pruned there.
			for candidate := 0; candidate < 10_000; candidate++ {
				id = fmt.Sprintf("absent-verification-%d", candidate)
				if hotMutationKeyShard(t, catalog, descriptor.Distribution, id) == descriptor.Shard {
					break
				}
			}
			if hotMutationKeyShard(t, catalog, descriptor.Distribution, id) != descriptor.Shard {
				t.Fatal("no in-range verification key")
			}
		}
		key, _ := orderedkey.AppendString(nil, []byte(id), orderedkey.Ascending)
		result, err := verifier.executor.ReadPoint(verifier.ctx, route, gateway.ReplicatedPointRead{
			Relation: 1, Key: key, MinimumApplied: 1, MaxValueBytes: 4 << 20, Linearizable: true})
		if err != nil {
			t.Fatal(err)
		}
		if result.Found != ownsRow {
			t.Fatalf("final in-range row presence: shard=%s found=%t expected=%t", descriptor.Shard, result.Found, ownsRow)
		}
		wants[descriptor.Shard] = ownsRow
		for _, replica := range route.Replicas {
			ctx, cancel := context.WithTimeout(verifier.ctx, 5*time.Second)
			response, err := hotMutationReadFinalVoter(ctx, verifier.client, route, replica, key, result.Applied)
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

type hotMutationFinalVoterClient interface {
	ProbeReplicated(context.Context, gateway.ReplicatedRoute, gateway.ReplicatedEndpoint, serviceauthz.Capability) (*shardservice.ReplicatedResponse, error)
	DoReplicated(context.Context, gateway.ReplicatedEndpoint, *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error)
}

// A post-split read fence is a catch-up condition, not a one-shot transport
// assertion. The pool may discover a closed pre-restart socket on the first
// probe. Retry only closed streams and explicit read-behind within the same
// deadline; never retry malformed authority or treat a probe as row evidence.
func hotMutationReadFinalVoter(ctx context.Context, client hotMutationFinalVoterClient, route gateway.ReplicatedRoute, replica gateway.ReplicatedEndpoint, key []byte, applied uint64) (*shardservice.ReplicatedResponse, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		probe, err := client.ProbeReplicated(ctx, route, replica, serviceauthz.CapabilityDataRead)
		if err == nil {
			if probe == nil || !probe.HasState {
				return nil, errors.New("final voter probe has no serving state")
			}
			replica.NodeIncarnation = probe.State.Fence.NodeIncarnation
			authority, _ := serviceauthz.FromContext(ctx)
			var response *shardservice.ReplicatedResponse
			response, err = client.DoReplicated(ctx, replica, &shardservice.ReplicatedRequest{
				Operation: shardservice.ReplicatedReadFollower, Capability: serviceauthz.CapabilityDataRead, Authority: authority,
				Fence: probe.State.Fence, Relation: 1, Key: key, MinimumApplied: applied, MaxValueBytes: 4 << 20})
			if err == nil {
				if response != nil && response.Kind == shardservice.ReplicatedRefusal && response.Refusal == shardservice.ReplicatedRefusalReadBehind {
					err = gateway.ErrReplicatedReadBehind
				} else {
					return response, nil
				}
			}
		}
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, gateway.ErrReplicatedReadBehind) {
			return nil, err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
	}
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
