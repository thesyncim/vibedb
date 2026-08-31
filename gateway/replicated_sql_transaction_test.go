package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type replicatedSQLIndexedReadClient struct {
	states     map[string]shardservice.ReplicatedMemberState
	value      []byte
	reads      int
	refusal    shardservice.ReplicatedRefusalCode
	foundEmpty bool
}

func TestReplicatedSQLFlatInsertUsesCanonicalRuntimeDocuments(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	queries := []Query{{SQL: `INSERT INTO messages (n,id,label) VALUES (?,?,'quoted " value'), (3,'message-2',?)`, Params: []shardservice.Param{shardservice.NumberParam("1.25"), shardservice.StringParam("message-1"), shardservice.NullParam()}}}
	participants, handled, err := executor.planReplicatedSQLTransaction(t.Context(), snapshot, queries, executor.profileFor(ClassInteractive))
	if err != nil || !handled {
		t.Fatalf("flat lowering: %v", err)
	}
	got := map[string]bool{}
	for _, p := range participants {
		for _, b := range p.Batches {
			for _, m := range b.Mutations {
				got[string(m.Value)] = true
			}
		}
	}
	if !got[`{"id":"message-1","label":"quoted \" value","n":1.25}`] || !got[`{"id":"message-2","label":null,"n":3}`] || len(got) != 2 {
		t.Fatalf("documents=%v", got)
	}
}

func TestReplicatedSQLFlatInsertRoutesAcrossDataShards(t *testing.T) {
	snapshot, executor, keys := replicatedSQLSplitTransactionFixture(t)
	queries := []Query{{SQL: `INSERT INTO messages (id,n) VALUES (?,1),(?,2)`, Params: []shardservice.Param{shardservice.StringParam(keys[0]), shardservice.StringParam(keys[1])}}}
	participants, handled, err := executor.planReplicatedSQLTransaction(t.Context(), snapshot, queries, executor.profileFor(ClassInteractive))
	if err != nil || !handled || len(participants) != 2 {
		t.Fatalf("distributed flat INSERT participants=%d handled=%v err=%v", len(participants), handled, err)
	}
	for _, p := range participants {
		if len(p.Batches) != 1 || len(p.Batches[0].Mutations) != 1 {
			t.Fatalf("incorrect row distribution: %+v", p)
		}
	}
}

func (client *replicatedSQLIndexedReadClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	state, ok := client.states[endpoint.Address]
	if !ok {
		return nil, ErrReplicatedRoute
	}
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	if request.Operation != shardservice.ReplicatedReadLeader {
		return nil, ErrReplicatedRoute
	}
	client.reads++
	if client.refusal != shardservice.ReplicatedRefusalNone {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: client.refusal, HasState: true, State: state}, nil
	}
	if len(client.value) == 0 && !client.foundEmpty {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedReadMissing, HasState: true, State: state,
			ReadApplied: state.Applied,
		}, nil
	}
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedReadFound, HasState: true, State: state,
		ReadApplied: state.Applied, Value: client.value,
	}, nil
}

func TestReplicatedSQLTransactionLowersExactMultiTableMutationsByGroupAndRelation(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	queries := []Query{
		{
			SQL:    `INSERT INTO messages VALUES (?)`,
			Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"message-1","n":1}`)},
		},
		{
			SQL:    `DELETE FROM accounts WHERE id = ?`,
			Params: []shardservice.Param{shardservice.StringParam("account-1")},
		},
		{
			SQL: `UPDATE logs SET "$doc" = ? WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.DocumentParam(`{"id":"log-1","n":2}`),
				shardservice.StringParam("log-1"),
			},
		},
	}
	participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, queries, executor.profileFor(ClassInteractive),
	)
	if err != nil || !handled {
		t.Fatalf("plan = handled %v err %v", handled, err)
	}
	if len(participants) != 2 {
		t.Fatalf("participants = %d, want 2", len(participants))
	}
	var data, logs *ReplicatedTransactionParticipant
	for index := range participants {
		participant := &participants[index]
		switch participant.Route.Distribution {
		case "data":
			data = participant
		case "logs-data":
			logs = participant
		}
	}
	if data == nil || logs == nil {
		t.Fatalf("routes = %+v", participants)
	}
	if len(data.Batches) != 2 || data.Batches[0].Relation != 1 ||
		data.Batches[1].Relation != 2 {
		t.Fatalf("data batches = %+v", data.Batches)
	}
	if got := data.Batches[0].Mutations[0].Kind; got != replication.MutationDelete {
		t.Fatalf("account mutation = %d, want delete", got)
	}
	if got := data.Batches[1].Mutations[0].Kind; got != replication.MutationPutAbsent {
		t.Fatalf("message mutation = %d, want put-absent", got)
	}
	if len(logs.Batches) != 1 || logs.Batches[0].Relation != 1 ||
		logs.Batches[0].Mutations[0].Kind != replication.MutationPutPresent {
		t.Fatalf("logs batches = %+v", logs.Batches)
	}
	wantMessage, _ := orderedkey.AppendString(nil, []byte("message-1"), orderedkey.Ascending)
	if !bytes.Equal(data.Batches[1].Mutations[0].Key, wantMessage) ||
		!bytes.Equal(data.Batches[1].Mutations[0].Value, []byte(`{"id":"message-1","n":1}`)) {
		t.Fatalf("message mutation = %+v", data.Batches[1].Mutations[0])
	}
	for index := range participants {
		participant := &participants[index]
		if len(participant.Route.Replicas) != ServingReplicaCount ||
			participant.BucketBits != distribution.DefaultVirtualBucketBits ||
			len(participant.IntentScopes) == 0 {
			t.Fatalf("participant %d route/scope = %+v", index, participant)
		}
	}
}

func TestReplicatedSQLTransactionCallerDigestIsExactAndRouteIndependent(t *testing.T) {
	base := []Query{
		{SQL: "DELETE FROM messages WHERE id = ?", Class: ClassInteractive,
			Params: []shardservice.Param{shardservice.StringBytesParam([]byte("message"))}},
		{SQL: "DELETE FROM logs WHERE id = ?", Class: ClassInteractive,
			Params: []shardservice.Param{shardservice.BoolParam(false)}},
	}
	want := replicatedSQLTransactionRequestDigest(base)
	empty := append([]Query(nil), base...)
	empty[0].ParamTypes = []sqldriver.ParamType{}
	if got := replicatedSQLTransactionRequestDigest(empty); got != want {
		t.Fatal("empty parameter type metadata changed the legacy digest")
	}
	if got := replicatedSQLTransactionRequestDigest(base); got != want {
		t.Fatal("identical caller request changed digest")
	}
	tests := []struct {
		name   string
		mutate func([]Query)
	}{
		{"sql", func(q []Query) { q[0].SQL += " " }},
		{"class", func(q []Query) { q[0].Class = ClassBatch }},
		{"kind", func(q []Query) { q[0].Params[0].Kind = shardservice.ParamNumber }},
		{"bool", func(q []Query) { q[1].Params[0].Bool = true }},
		{"bytes", func(q []Query) { q[0].Params[0].Bytes = []byte("other") }},
		{"parameter type", func(q []Query) {
			q[0].ParamTypes = []sqldriver.ParamType{sqldriver.ParamTypeText}
		}},
		{"order", func(q []Query) { q[0], q[1] = q[1], q[0] }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := append([]Query(nil), base...)
			changed[0].Params = append([]shardservice.Param(nil), base[0].Params...)
			changed[1].Params = append([]shardservice.Param(nil), base[1].Params...)
			test.mutate(changed)
			if got := replicatedSQLTransactionRequestDigest(changed); got == want {
				t.Fatalf("%s change did not change digest", test.name)
			}
		})
	}
}

func TestReplicatedSQLTransactionRejectsDuplicateAndResidualBeforeExecution(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	tests := []struct {
		name    string
		queries []Query
		want    error
	}{
		{
			name: "duplicate relation key",
			queries: []Query{
				{SQL: `INSERT INTO messages VALUES (?)`, Params: []shardservice.Param{
					shardservice.DocumentParam(`{"id":"same"}`),
				}},
				{SQL: `DELETE FROM messages WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("same"),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrReplicatedSQLTransactionDuplicate,
		},
		{
			name: "residual predicate",
			queries: []Query{
				{SQL: `UPDATE messages SET "$doc" = ? WHERE id = ? AND n = 1`, Params: []shardservice.Param{
					shardservice.DocumentParam(`{"id":"message-1","n":2}`),
					shardservice.StringParam("message-1"),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrReplicatedSQLTransactionUnsupported,
		},
		{
			name: "primary key move within same shard",
			queries: []Query{
				{SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`, Params: []shardservice.Param{
					shardservice.DocumentParam(`{"id":"message-2"}`),
					shardservice.StringParam("message-1"),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrWriteShardKeyMove,
		},
		{
			name: "flat insert on conflict",
			queries: []Query{
				{SQL: `INSERT INTO messages (id) VALUES (?) ON CONFLICT DO NOTHING`, Params: []shardservice.Param{
					shardservice.StringParam("message-1"),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrReplicatedSQLTransactionUnsupported,
		},
		{
			name: "duplicate rows within multi row insert",
			queries: []Query{
				{SQL: `INSERT INTO messages VALUES (?),(?)`, Params: []shardservice.Param{
					shardservice.DocumentParam(`{"id":"message-1"}`),
					shardservice.DocumentParam(`{"id":"message-1"}`),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrReplicatedSQLTransactionDuplicate,
		},
		{
			name: "returning",
			queries: []Query{
				{SQL: `DELETE FROM messages WHERE id = ? RETURNING id`, Params: []shardservice.Param{
					shardservice.StringParam("message-1"),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrReplicatedSQLTransactionUnsupported,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			participants, handled, err := executor.planReplicatedSQLTransaction(
				t.Context(), snapshot, test.queries, executor.profileFor(ClassInteractive),
			)
			if !handled || !errors.Is(err, test.want) || len(participants) != 0 {
				t.Fatalf("plan = %d handled %v err %v, want %v", len(participants), handled, err, test.want)
			}
		})
	}
}

func TestReplicatedSQLTransactionLowersOneMultiRowInsertAcrossRF3Shards(t *testing.T) {
	snapshot, executor, keys := replicatedSQLSplitTransactionFixture(t)
	documents := [][]byte{
		[]byte(`{"id":"` + keys[0] + `","n":1}`),
		[]byte(`{"id":"` + keys[1] + `","n":2}`),
	}
	participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, []Query{{
			SQL: `INSERT INTO messages VALUES (?),(?)`,
			Params: []shardservice.Param{
				shardservice.DocumentBytesParam(documents[0]),
				shardservice.DocumentBytesParam(documents[1]),
			},
		}}, executor.profileFor(ClassInteractive),
	)
	if err != nil || !handled {
		t.Fatalf("plan handled=%v err=%v", handled, err)
	}
	if len(participants) != 2 {
		t.Fatalf("participants=%d want=2 cross-shard RF3 groups", len(participants))
	}
	if participants[0].Route.Command.RelationManifestDigest == participants[1].Route.Command.RelationManifestDigest ||
		participants[0].Route.LogicalSchemaDigest != participants[1].Route.LogicalSchemaDigest {
		t.Fatal("shared logical table did not preserve distinct shard machine fences")
	}
	seen := make(map[string][]byte, len(documents))
	borrowed := make(map[string]bool, len(documents))
	for participantIndex := range participants {
		participant := &participants[participantIndex]
		if len(participant.IntentScopes) != 1 {
			t.Fatalf("participant %d intent scopes=%v want one exact bucket", participantIndex, participant.IntentScopes)
		}
		for batchIndex := range participant.Batches {
			batch := &participant.Batches[batchIndex]
			if batch.Relation != 1 {
				t.Fatalf("participant %d relation=%d want messages relation 1", participantIndex, batch.Relation)
			}
			for mutationIndex := range batch.Mutations {
				mutation := &batch.Mutations[mutationIndex]
				if mutation.Kind != replication.MutationPutAbsent {
					t.Fatalf("mutation kind=%d want put-absent", mutation.Kind)
				}
				seen[string(mutation.Value)] = mutation.Key
				for _, document := range documents {
					if bytes.Equal(mutation.Value, document) {
						borrowed[string(document)] = &mutation.Value[0] == &document[0]
					}
				}
			}
		}
	}
	for _, document := range documents {
		if len(seen[string(document)]) == 0 {
			t.Fatalf("document %s was not lowered", document)
		}
		if !borrowed[string(document)] {
			t.Fatalf("document %s was copied during byte-native lowering", document)
		}
	}
}

func TestReplicatedSQLTransactionLowersFiniteDeleteAcrossRF3Shards(t *testing.T) {
	snapshot, executor, keys := replicatedSQLSplitTransactionFixture(t)
	participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, []Query{{
			SQL: `DELETE FROM messages WHERE id IN (?, ?)`,
			Params: []shardservice.Param{
				shardservice.StringParam(keys[0]), shardservice.StringParam(keys[1]),
			},
		}}, executor.profileFor(ClassInteractive),
	)
	if err != nil || !handled {
		t.Fatalf("plan handled=%v err=%v", handled, err)
	}
	if len(participants) != 2 {
		t.Fatalf("participants=%d want=2 cross-shard RF3 groups", len(participants))
	}
	mutations := 0
	for participantIndex := range participants {
		participant := &participants[participantIndex]
		if len(participant.IntentScopes) != 1 || len(participant.Batches) != 1 ||
			participant.Batches[0].Relation != 1 {
			t.Fatalf("participant %d=%+v", participantIndex, participant)
		}
		for mutationIndex := range participant.Batches[0].Mutations {
			if participant.Batches[0].Mutations[mutationIndex].Kind != replication.MutationDelete {
				t.Fatalf("participant %d mutation %d kind=%d", participantIndex, mutationIndex,
					participant.Batches[0].Mutations[mutationIndex].Kind)
			}
			mutations++
		}
	}
	if mutations != 2 {
		t.Fatalf("mutations=%d want=2", mutations)
	}
}

func replicatedSQLSplitTransactionFixture(
	t testing.TB,
) (*Snapshot, *Executor, [2]string) {
	t.Helper()
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	cut := point(0x80)
	manifest, err := distribution.NewManifest("data", 4, []distribution.Shard{
		{ID: "left", AllocationGeneration: 1,
			Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Point: cut}},
			Leaders: []distribution.EndpointID{"peer-a", "peer-b", "peer-c"}, Epoch: 7},
		{ID: "right", AllocationGeneration: 2,
			Range:   distribution.KeyRange{Start: cut, End: distribution.KeyspaceEnd{Max: true}},
			Leaders: []distribution.EndpointID{"right-a", "right-b", "right-c"}, Epoch: 7},
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Manifests = []*distribution.Manifest{manifest}
	descriptor.Shard = "left"
	descriptor.Command.RoutingVersion = 4
	descriptor.RangeIdentity[0]++
	left := descriptor
	right := descriptor
	right.Command.RelationManifestDigest[0]++
	right.Replicas = append([]ReplicatedReplicaDescriptor(nil), descriptor.Replicas...)
	right.Shard = "right"
	right.AllocationGeneration = 2
	right.Group.ShardIncarnation[0]++
	right.Group.GroupID[0]++
	right.RangeIdentity[0]++
	right.LineageDigest[0]++
	right.ForwardingRuleDigest[0]++
	for ordinal := range right.Replicas {
		letter := string(rune('a' + ordinal))
		right.Replicas[ordinal].Endpoint = distribution.EndpointID("right-" + letter)
		right.Replicas[ordinal].NativeEndpoint = distribution.EndpointID("right-native-" + letter)
		right.Replicas[ordinal].ControlEndpoint = distribution.EndpointID("right-control-" + letter)
		endpoints[right.Replicas[ordinal].Endpoint] = "127.0.0.1:" + string(rune('1'+ordinal)) + "401"
		endpoints[right.Replicas[ordinal].NativeEndpoint] = "127.0.0.1:" + string(rune('1'+ordinal)) + "411"
		endpoints[right.Replicas[ordinal].ControlEndpoint] = "127.0.0.1:" + string(rune('1'+ordinal)) + "421"
	}
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 7, nil, nil, []ReplicatedShardDescriptor{left, right},
		[]ReplicatedTableProfile{profile},
	)
	if err != nil {
		t.Fatal(err)
	}
	mapper := distribution.NewNativeMapper(1)
	var keys [2]string
	for candidate := 0; candidate < 10_000 && (keys[0] == "" || keys[1] == ""); candidate++ {
		key := fmt.Sprintf("multi-row-%d", candidate)
		point, pointErr := mapper.PointFor([]distribution.Scalar{distribution.NewString(key)})
		if pointErr != nil {
			t.Fatal(pointErr)
		}
		ordinal := 0
		if bytes.Compare(point[:], cut[:]) >= 0 {
			ordinal = 1
		}
		if keys[ordinal] == "" {
			keys[ordinal] = key
		}
	}
	if keys[0] == "" || keys[1] == "" {
		t.Fatal("could not find keys on both sides of split")
	}
	return snapshot, NewExecutor(nil, NewCatalogHolder(snapshot), Options{}), keys
}

func TestReplicatedSQLTransactionMultiRowInsertMaintainsGlobalIndexAtomically(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true, true)
	participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, []Query{{
			SQL: `INSERT INTO messages VALUES (?),(?)`,
			Params: []shardservice.Param{
				shardservice.DocumentParam(`{"id":"message-1","email":"one@example.test"}`),
				shardservice.DocumentParam(`{"id":"message-2","email":"two@example.test"}`),
			},
		}}, executor.profileFor(ClassInteractive),
	)
	if err != nil || !handled {
		t.Fatalf("plan handled=%v err=%v", handled, err)
	}
	baseMutations, indexMutations := 0, 0
	locators := make(map[string]bool, 2)
	for participantIndex := range participants {
		participant := &participants[participantIndex]
		for batchIndex := range participant.Batches {
			batch := &participant.Batches[batchIndex]
			if participant.Route.Distribution == "messages-email" {
				for mutationIndex := range batch.Mutations {
					mutation := &batch.Mutations[mutationIndex]
					if mutation.Kind != replication.MutationPutAbsentOrEqual {
						t.Fatalf("index mutation kind=%d", mutation.Kind)
					}
					locators[string(mutation.Value)] = true
					indexMutations++
				}
			} else if participant.Route.Distribution == "data" && batch.Relation == 2 {
				baseMutations += len(batch.Mutations)
			}
		}
	}
	if baseMutations != 2 || indexMutations != 2 ||
		!locators[`["message-1"]`] || !locators[`["message-2"]`] {
		t.Fatalf("base=%d index=%d locators=%v", baseMutations, indexMutations, locators)
	}
}

func TestReplicatedSQLTransactionRoutesReadyGlobalIndexAsIndependentRF3Participant(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true, true)
	participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, []Query{
			{SQL: `INSERT INTO messages VALUES (?)`, Params: []shardservice.Param{
				shardservice.DocumentParam(`{"id":"message-1","email":"m@example.test"}`),
			}},
			{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
				shardservice.StringParam("log-1"),
			}},
		}, executor.profileFor(ClassInteractive),
	)
	if len(participants) != 3 || !handled || err != nil {
		t.Fatalf("plan = %d handled %v err %v", len(participants), handled, err)
	}
	var index *ReplicatedTransactionParticipant
	for ordinal := range participants {
		if participants[ordinal].Route.Distribution == "messages-email" {
			index = &participants[ordinal]
		}
	}
	if index == nil || len(index.Batches) != 1 || index.Batches[0].Relation != 1 ||
		len(index.Batches[0].Mutations) != 1 ||
		index.Batches[0].Mutations[0].Kind != replication.MutationPutAbsentOrEqual ||
		!bytes.Equal(index.Batches[0].Mutations[0].Value, []byte(`["message-1"]`)) {
		t.Fatalf("global index participant = %+v", index)
	}
}

func TestReplicatedSQLTransactionGlobalIndexMutationsConsumeAdmissionBudget(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true, true)
	query := []Query{{
		SQL: `INSERT INTO messages VALUES (?)`, Params: []shardservice.Param{
			shardservice.DocumentParam(`{"id":"message-1","email":"m@example.test"}`),
		},
	}}
	profile := executor.profileFor(ClassInteractive)
	profile.MaxTransactionMutations = 1
	if participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, query, profile,
	); len(participants) != 0 || !handled || !errors.Is(err, ErrTransactionMutationLimit) {
		t.Fatalf("mutation bound participants=%d handled=%v err=%v", len(participants), handled, err)
	}
	profile = executor.profileFor(ClassInteractive)
	profile.MaxTransactionBytes = uint64(len(`{"id":"message-1","email":"m@example.test"}`)) + 32
	if participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, query, profile,
	); len(participants) != 0 || !handled || !errors.Is(err, ErrTransactionByteLimit) {
		t.Fatalf("byte bound participants=%d handled=%v err=%v", len(participants), handled, err)
	}
}

func TestReplicatedSQLTransactionGlobalIndexRejectsStaleSplitFence(t *testing.T) {
	snapshot, _ := replicatedSQLTransactionFixture(t, true, true)
	query := Query{
		SQL: `INSERT INTO messages VALUES (?)`, Params: []shardservice.Param{
			shardservice.DocumentParam(`{"id":"message-1","email":"m@example.test"}`),
		},
	}
	prepared, err := snapshot.Prepare(t.Context(), query.SQL)
	if err != nil {
		t.Fatal(err)
	}
	args, err := queryRuntimeArgs(query.Params)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.BindWrite(args)
	if err != nil || len(bound.globalIndexes) != 1 {
		t.Fatalf("bound global indexes=%d err=%v", len(bound.globalIndexes), err)
	}
	stale := bound.globalIndexes[0]
	stale.target.OwnershipEpoch++
	if _, _, err := snapshot.resolveReplicatedSQLGlobalIndex(&stale); !errors.Is(
		err, ErrReplicatedSQLWriteUnavailable,
	) {
		t.Fatalf("stale split ownership fence error=%v", err)
	}
	stale = bound.globalIndexes[0]
	stale.routingVersion++
	if _, _, err := snapshot.resolveReplicatedSQLGlobalIndex(&stale); !errors.Is(
		err, ErrReplicatedSQLWriteUnavailable,
	) {
		t.Fatalf("stale split routing fence error=%v", err)
	}
}

func TestReplicatedSQLTransactionGlobalIndexUpdateAndDeleteBindExactOldValue(t *testing.T) {
	for _, test := range []struct {
		name       string
		query      Query
		baseKind   replication.MutationKind
		indexKinds []replication.MutationKind
	}{{
		name: "update", query: Query{
			SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.DocumentParam(`{"id":"message-1","email":"new@example.test"}`),
				shardservice.StringParam("message-1"),
			},
		}, baseKind: replication.MutationPutDigestEqual,
		indexKinds: []replication.MutationKind{
			replication.MutationDeleteDigestEqual, replication.MutationPutAbsentOrEqual,
		},
	}, {
		name: "delete", query: Query{
			SQL:    `DELETE FROM messages WHERE id = ?`,
			Params: []shardservice.Param{shardservice.StringParam("message-1")},
		}, baseKind: replication.MutationDeleteDigestEqual,
		indexKinds: []replication.MutationKind{replication.MutationDeleteDigestEqual},
	}} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, executor := replicatedSQLTransactionFixture(t, true, true)
			old := []byte(`{"id":"message-1","email":"old@example.test"}`)
			client, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
			participants, handled, err := executor.planReplicatedSQLTransactionWithData(
				t.Context(), snapshot, []Query{test.query}, executor.profileFor(ClassInteractive), data,
			)
			if err != nil || !handled || client.reads != 1 || len(participants) != 2 {
				t.Fatalf("plan=%d handled=%v reads=%d err=%v", len(participants), handled, client.reads, err)
			}
			var base, index *ReplicatedTransactionParticipant
			for ordinal := range participants {
				switch participants[ordinal].Route.Distribution {
				case "data":
					base = &participants[ordinal]
				case "messages-email":
					index = &participants[ordinal]
				}
			}
			if base == nil || len(base.Batches) != 1 || len(base.Batches[0].Mutations) != 1 {
				t.Fatalf("base participant=%+v", base)
			}
			baseMutation := base.Batches[0].Mutations[0]
			if baseMutation.Kind != test.baseKind ||
				baseMutation.ExpectedValueLength != uint64(len(old)) ||
				baseMutation.ExpectedValueDigest != replication.Digest(sha256.Sum256(old)) {
				t.Fatalf("base mutation=%+v", baseMutation)
			}
			if index == nil || len(index.Batches) != 1 ||
				len(index.Batches[0].Mutations) != len(test.indexKinds) {
				t.Fatalf("index participant=%+v", index)
			}
			for ordinal, want := range test.indexKinds {
				mutation := index.Batches[0].Mutations[ordinal]
				if mutation.Kind != want {
					t.Fatalf("index mutation %d kind=%d want=%d", ordinal, mutation.Kind, want)
				}
				if want == replication.MutationDeleteDigestEqual &&
					(mutation.ExpectedValueLength == 0 ||
						mutation.ExpectedValueDigest == (replication.Digest{})) {
					t.Fatalf("index delete lacks exact old-value fence: %+v", mutation)
				}
			}
		})
	}
}

func TestReplicatedSQLDeclaredColumnUpdateIsExactCAS(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	old := []byte(`{"email":"old@example.test","id":"message-1","n":1}`)
	client, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
	participants, handled, err := executor.planReplicatedSQLTransactionWithData(
		t.Context(), snapshot, []Query{{
			SQL: `UPDATE messages SET email = ?, n = 2 WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.StringParam("new@example.test"),
				shardservice.StringParam("message-1"),
			},
		}}, executor.profileFor(ClassInteractive), data,
	)
	if err != nil || !handled || client.reads != 1 || len(participants) != 1 {
		t.Fatalf("plan=%d handled=%v reads=%d err=%v", len(participants), handled, client.reads, err)
	}
	mutation := participants[0].Batches[0].Mutations[0]
	if mutation.Kind != replication.MutationPutDigestEqual ||
		mutation.ExpectedValueLength != uint64(len(old)) ||
		mutation.ExpectedValueDigest != replication.Digest(sha256.Sum256(old)) {
		t.Fatalf("mutation=%+v", mutation)
	}
	if got := string(mutation.Value); got != `{"email":"new@example.test","id":"message-1","n":2}` {
		t.Fatalf("document=%s", got)
	}
}

func TestReplicatedSQLComputedUpdateRetainsCanonicalExactCAS(t *testing.T) {
	const source = `UPDATE messages SET n = n + ?, mirror = n, email = email || ? WHERE id = ?`
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	old := []byte(`{"n":9007199254740993,"id":"message-1","email":"old@example.test","mirror":0}`)
	client, data := attachReplicatedSQLIndexedReadClient(
		t, snapshot, old,
	)

	participants, handled, err := executor.planReplicatedSQLTransactionWithData(
		t.Context(), snapshot, []Query{{
			SQL: source,
			Params: []shardservice.Param{
				shardservice.NumberParam("1"),
				shardservice.StringParam(".invalid"),
				shardservice.StringParam("message-1"),
			},
			ParamTypes: []sqldriver.ParamType{
				sqldriver.ParamTypeUnspecified,
				sqldriver.ParamTypeText,
				sqldriver.ParamTypeUnspecified,
			},
		}}, executor.profileFor(ClassInteractive), data,
	)
	if err != nil || !handled || len(participants) != 1 || client.reads != 1 {
		t.Fatalf("plan=%d handled=%v reads=%d err=%v", len(participants), handled, client.reads, err)
	}
	mutation := participants[0].Batches[0].Mutations[0]
	if mutation.Kind != replication.MutationPutDigestEqual ||
		mutation.ExpectedValueLength != uint64(len(old)) ||
		mutation.ExpectedValueDigest != replication.Digest(sha256.Sum256(old)) {
		t.Fatalf("computed mutation=%+v", mutation)
	}
	want := `{"email":"old@example.test.invalid","id":"message-1","mirror":9007199254740993,"n":9007199254740994}`
	if string(mutation.Value) != want {
		t.Fatalf("computed postimage=%s, want %s", mutation.Value, want)
	}
}

func TestReplicatedSQLComputedUpdateDerivesGlobalIndexFromRetainedPostimage(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true, true)
	old := []byte(`{"email":"old@example.test","id":"message-1","n":1}`)
	client, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
	participants, handled, err := executor.planReplicatedSQLTransactionWithData(
		t.Context(), snapshot, []Query{{
			SQL: `UPDATE messages SET email = email || '.invalid', n = n + 1 WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.StringParam("message-1"),
			},
		}}, executor.profileFor(ClassInteractive), data,
	)
	if err != nil || !handled || len(participants) != 2 || client.reads != 1 {
		t.Fatalf("plan=%d handled=%v reads=%d err=%v", len(participants), handled, client.reads, err)
	}
	var base, index *ReplicatedTransactionParticipant
	for ordinal := range participants {
		switch participants[ordinal].Route.Distribution {
		case "data":
			base = &participants[ordinal]
		case "messages-email":
			index = &participants[ordinal]
		}
	}
	if base == nil || len(base.Batches) != 1 || len(base.Batches[0].Mutations) != 1 {
		t.Fatalf("base participant=%+v", base)
	}
	baseMutation := base.Batches[0].Mutations[0]
	if baseMutation.Kind != replication.MutationPutDigestEqual ||
		string(baseMutation.Value) != `{"email":"old@example.test.invalid","id":"message-1","n":2}` ||
		baseMutation.ExpectedValueDigest != replication.Digest(sha256.Sum256(old)) {
		t.Fatalf("computed base mutation=%+v", baseMutation)
	}
	if index == nil || len(index.Batches) != 1 || len(index.Batches[0].Mutations) != 2 {
		t.Fatalf("index participant=%+v", index)
	}
	remove, put := index.Batches[0].Mutations[0], index.Batches[0].Mutations[1]
	if remove.Kind != replication.MutationDeleteDigestEqual ||
		put.Kind != replication.MutationPutAbsentOrEqual ||
		bytes.Equal(remove.Key, put.Key) ||
		remove.ExpectedValueDigest == (replication.Digest{}) {
		t.Fatalf("computed index mutations=%+v / %+v", remove, put)
	}
}

func TestReplicatedSQLComputedPostimageIsOwnedByDurableLogicalProgram(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	old := []byte(`{"id":"message-1","n":41}`)
	_, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
	participants, handled, err := executor.planReplicatedSQLTransactionWithData(
		t.Context(), snapshot, []Query{{
			SQL: `UPDATE messages SET n = n + 1 WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.StringParam("message-1"),
			},
		}}, executor.profileFor(ClassInteractive), data,
	)
	if err != nil || !handled || len(participants) != 1 {
		t.Fatalf("plan=%d handled=%v err=%v", len(participants), handled, err)
	}
	build := durableRequestProgramBuildFixture(t)
	build.Participants = participants
	program, err := BuildDurableRequestLogicalProgram(build)
	if err != nil || len(program.Participants) != 1 ||
		len(program.Participants[0].Batches) != 1 ||
		len(program.Participants[0].Batches[0].Mutations) != 1 {
		t.Fatalf("durable program=%+v err=%v", program, err)
	}
	retained := program.Participants[0].Batches[0].Mutations[0]
	if retained.Kind != replication.MutationPutDigestEqual ||
		string(retained.Value) != `{"id":"message-1","n":42}` ||
		retained.ExpectedValueDigest != replication.Digest(sha256.Sum256(old)) {
		t.Fatalf("retained computed mutation=%+v", retained)
	}
	participants[0].Batches[0].Mutations[0].Value[0] = '['
	if string(program.Participants[0].Batches[0].Mutations[0].Value) !=
		`{"id":"message-1","n":42}` {
		t.Fatal("durable program borrowed the planner postimage")
	}
}

func TestReplicatedSQLComputedUpdateConcurrentPlanningOwnsEvaluatorState(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	const workers = 8
	type fixture struct {
		old  []byte
		data *ReplicatedExecutor
	}
	fixtures := make([]fixture, workers)
	for worker := range workers {
		old := []byte(fmt.Sprintf(`{"id":"message-1","n":%d}`, worker))
		_, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
		fixtures[worker] = fixture{old: old, data: data}
	}
	ctx := t.Context()
	profile := executor.profileFor(ClassInteractive)
	var wait sync.WaitGroup
	errorsByWorker := make([]error, workers)
	for worker := range workers {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			participants, handled, err := executor.planReplicatedSQLTransactionWithData(
				ctx, snapshot, []Query{{
					SQL: `UPDATE messages SET n = n + 1 WHERE id = ?`,
					Params: []shardservice.Param{
						shardservice.StringParam("message-1"),
					},
				}}, profile, fixtures[worker].data,
			)
			if err != nil || !handled || len(participants) != 1 {
				errorsByWorker[worker] = fmt.Errorf(
					"plan=%d handled=%v: %w", len(participants), handled, err,
				)
				return
			}
			mutation := participants[0].Batches[0].Mutations[0]
			want := fmt.Sprintf(`{"id":"message-1","n":%d}`, worker+1)
			if string(mutation.Value) != want ||
				mutation.ExpectedValueDigest != replication.Digest(sha256.Sum256(fixtures[worker].old)) {
				errorsByWorker[worker] = fmt.Errorf(
					"mutation=%+v, want postimage %s", mutation, want,
				)
			}
		}(worker)
	}
	wait.Wait()
	for worker, err := range errorsByWorker {
		if err != nil {
			t.Fatalf("worker %d: %v", worker, err)
		}
	}
}

func TestReplicatedSQLComputedUpdateValidatesBindingsBeforeCurrentRowRead(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	client, data := attachReplicatedSQLIndexedReadClient(
		t, snapshot, []byte(`{"id":"message-1","n":1}`),
	)
	participants, handled, err := executor.planReplicatedSQLTransactionWithData(
		t.Context(), snapshot, []Query{{
			SQL: `UPDATE messages SET n = n + CAST(? AS INTEGER) WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.StringParam("bad"),
				shardservice.StringParam("message-1"),
			},
			ParamTypes: []sqldriver.ParamType{
				sqldriver.ParamTypeText,
				sqldriver.ParamTypeUnspecified,
			},
		}}, executor.profileFor(ClassInteractive), data,
	)
	if err == nil || handled || len(participants) != 0 || client.reads != 0 {
		t.Fatalf("participants=%d handled=%v reads=%d err=%v", len(participants), handled, client.reads, err)
	}
}

func TestReplicatedSQLComputedUpdateRejectsPrimaryKeyMoveBeforeAdmission(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	client, data := attachReplicatedSQLIndexedReadClient(
		t, snapshot, []byte(`{"id":"message-1","n":1}`),
	)
	participants, handled, err := executor.planReplicatedSQLTransactionWithData(
		t.Context(), snapshot, []Query{{
			SQL: `UPDATE messages SET id = id || '-moved', n = n + 1 WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.StringParam("message-1"),
			},
		}}, executor.profileFor(ClassInteractive), data,
	)
	if !errors.Is(err, ErrWriteShardKeyMove) || !handled ||
		len(participants) != 0 || client.reads != 1 {
		t.Fatalf("participants=%d handled=%v reads=%d err=%v", len(participants), handled, client.reads, err)
	}
}

func TestReplicatedSQLConflictActionsRejectBeforeBindOrCurrentRowRead(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		insert     *sqlast.InsertStmt
		wantMarker string
	}{
		{
			name:       "computed do update",
			source:     `INSERT INTO messages (id, value) VALUES (?, ?) ON CONFLICT DO UPDATE SET value = value || EXCLUDED.value`,
			insert:     &sqlast.InsertStmt{Table: "messages", OnConflictUpdate: &sqlast.InsertConflictUpdate{}},
			wantMarker: "UPDATE",
		},
		{
			name:       "do nothing",
			source:     "INSERT INTO messages (id) VALUES (?) ON /* authored */\nCONFLICT DO NOTHING",
			insert:     &sqlast.InsertStmt{Table: "messages", OnConflictDoNothing: true},
			wantMarker: "ON CONFLICT",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wantPosition := strings.Index(test.source, test.wantMarker)
			if wantPosition < 0 && test.insert.OnConflictDoNothing {
				wantPosition = strings.Index(test.source, "ON /* authored */")
				test.insert.OnConflictPos = wantPosition
			}
			if test.insert.OnConflictUpdate != nil {
				test.insert.OnConflictUpdate.Pos = wantPosition
			}
			snapshot, executor := replicatedSQLTransactionFixture(t, true)
			client, data := attachReplicatedSQLIndexedReadClient(
				t, snapshot, []byte(`{"id":"message-1","value":"old"}`),
			)

			// Seed the RF3 lowerer with the already-prepared shape so this
			// defense remains independently testable while the ordinary planner
			// also fences conflict actions. The intentionally incomplete plan
			// would fail BindWrite, proving the RF3 check runs first.
			_, hash := snapshot.cachedPreparedPlan(test.source)
			snapshot.cachePreparedPlan(test.source, hash, &PreparedPlan{
				statement: sqlast.Statement{Kind: sqlast.KindInsert, Insert: test.insert},
				table:     test.insert.Table,
			})

			participants, handled, err := executor.planReplicatedSQLTransactionWithData(
				t.Context(), snapshot, []Query{{SQL: test.source}},
				executor.profileFor(ClassInteractive), data,
			)
			var unsupported *sqlast.FeatureNotSupportedError
			if !errors.As(err, &unsupported) || unsupported.Pos != wantPosition {
				t.Fatalf(
					"plan handled=%v error=%T %v, want positioned FeatureNotSupported at %d",
					handled, err, err, wantPosition,
				)
			}
			if !errors.Is(err, ErrReplicatedSQLTransactionUnsupported) {
				t.Fatalf("plan error %v does not retain ErrReplicatedSQLTransactionUnsupported", err)
			}
			if !handled || len(participants) != 0 || client.reads != 0 {
				t.Fatalf(
					"plan participants=%d handled=%v current-row reads=%d, want 0,true,0",
					len(participants), handled, client.reads,
				)
			}
		})
	}
}

func TestReplicatedSQLColumnUpdateMissingRetainsDurableNoOp(t *testing.T) {
	for _, source := range []string{
		`UPDATE messages SET n = 2 WHERE id = ?`,
		`UPDATE messages SET n = n / 0 WHERE id = ?`,
	} {
		t.Run(source, func(t *testing.T) {
			snapshot, executor := replicatedSQLTransactionFixture(t, true)
			client, data := attachReplicatedSQLIndexedReadClient(t, snapshot, nil)
			participants, handled, err := executor.planReplicatedSQLTransactionWithData(
				t.Context(), snapshot, []Query{{
					SQL: source,
					Params: []shardservice.Param{
						shardservice.StringParam("message-1"),
					},
				}}, executor.profileFor(ClassInteractive), data,
			)
			if err != nil || !handled || len(participants) != 1 || client.reads != 1 {
				t.Fatalf("plan=%d handled=%v reads=%d err=%v", len(participants), handled, client.reads, err)
			}
			mutation := participants[0].Batches[0].Mutations[0]
			if mutation.Kind != replication.MutationPutPresent || string(mutation.Value) != `{}` {
				t.Fatalf("durable no-op mutation=%+v", mutation)
			}
		})
	}
}

func TestReplicatedSQLColumnUpdateRejectsFoundEmptyDocument(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	client, data := attachReplicatedSQLIndexedReadClient(t, snapshot, nil)
	client.foundEmpty = true
	participants, handled, err := executor.planReplicatedSQLTransactionWithData(
		t.Context(), snapshot, []Query{{
			SQL: `UPDATE messages SET n = n + 1 WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.StringParam("message-1"),
			},
		}}, executor.profileFor(ClassInteractive), data,
	)
	if !errors.Is(err, ErrReplicatedRoute) || !handled ||
		len(participants) != 0 || client.reads != 1 {
		t.Fatalf("participants=%d handled=%v reads=%d err=%v", len(participants), handled, client.reads, err)
	}
}

func TestReplicatedSQLTransactionGlobalIndexSameKeyUsesExactReplacement(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true, true, true, true)
	old := []byte(`{"id":"message-1","email":"same@example.test","region":"old"}`)
	_, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
	participants, handled, err := executor.planReplicatedSQLTransactionWithData(
		t.Context(), snapshot, []Query{{
			SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.DocumentParam(`{"id":"message-1","email":"same@example.test","region":"new"}`),
				shardservice.StringParam("message-1"),
			},
		}}, executor.profileFor(ClassInteractive), data,
	)
	if err != nil || !handled || len(participants) != 3 {
		t.Fatalf("plan=%d handled=%v err=%v", len(participants), handled, err)
	}
	foundEmail, foundRegion := false, false
	for ordinal := range participants {
		participant := &participants[ordinal]
		switch participant.Route.Distribution {
		case "messages-email":
			foundEmail = true
			if len(participant.Batches) != 1 || len(participant.Batches[0].Mutations) != 1 {
				t.Fatalf("same-key index participant=%+v", participant)
			}
			mutation := participant.Batches[0].Mutations[0]
			if mutation.Kind != replication.MutationPutDigestEqual ||
				mutation.ExpectedValueLength == 0 ||
				mutation.ExpectedValueDigest == (replication.Digest{}) || len(mutation.Value) == 0 {
				t.Fatalf("same-key exact replacement=%+v", mutation)
			}
		case "messages-region":
			foundRegion = true
			if len(participant.Batches) != 1 || len(participant.Batches[0].Mutations) != 2 ||
				participant.Batches[0].Mutations[0].Kind != replication.MutationDeleteDigestEqual ||
				participant.Batches[0].Mutations[1].Kind != replication.MutationPutAbsentOrEqual {
				t.Fatalf("changed-key index participant=%+v", participant)
			}
		}
	}
	if !foundEmail || !foundRegion {
		t.Fatalf("index participants: email=%v region=%v", foundEmail, foundRegion)
	}
}

func attachReplicatedSQLIndexedReadClient(
	t testing.TB,
	snapshot *Snapshot,
	value []byte,
) (*replicatedSQLIndexedReadClient, *ReplicatedExecutor) {
	t.Helper()
	key, ok := orderedkey.AppendString(nil, []byte("message-1"), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode indexed read key")
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scalar [replication.MaxMutationKeyBytes + 16]byte
	resolved, ok := snapshot.ResolveReplicatedTableKey(
		[]byte("messages"), key, scalar[:0], replicas[:0],
	)
	if !ok {
		t.Fatal("resolve indexed base route")
	}
	states := make(map[string]shardservice.ReplicatedMemberState, len(resolved.Route.Replicas))
	leader := resolved.Route.Replicas[0].Member
	for _, endpoint := range resolved.Route.Replicas {
		fence := shardservice.ReplicatedFence{
			Group: resolved.Route.Group, AllocationGeneration: resolved.Route.AllocationGeneration,
			Command: resolved.Route.Command, MemberID: endpoint.Member,
			StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, Term: 1,
		}
		states[endpoint.Address] = shardservice.ReplicatedMemberState{
			Fence: fence, LeaderID: leader, Commit: 1, Applied: 1, CheckpointApplied: 1,
		}
	}
	client := &replicatedSQLIndexedReadClient{states: states, value: value}
	replicated, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client, replicated
}

func TestReplicatedSQLTransactionRejectsMixedAuthorityAndLeavesStaticBatchUnclaimed(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, false)
	mixed := []Query{
		{SQL: `DELETE FROM messages WHERE id = ?`, Params: []shardservice.Param{shardservice.StringParam("m")}},
		{SQL: `DELETE FROM legacy WHERE id = ?`, Params: []shardservice.Param{shardservice.StringParam("l")}},
	}
	participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, mixed, executor.profileFor(ClassInteractive),
	)
	if !handled || !errors.Is(err, ErrReplicatedSQLTransactionMixed) || len(participants) != 0 {
		t.Fatalf("mixed = %d handled %v err %v", len(participants), handled, err)
	}
	static := []Query{
		{SQL: `DELETE FROM legacy WHERE id = ?`, Params: []shardservice.Param{shardservice.StringParam("l1")}},
		{SQL: `DELETE FROM legacy WHERE id = ?`, Params: []shardservice.Param{shardservice.StringParam("l2")}},
	}
	participants, handled, err = executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, static, executor.profileFor(ClassInteractive),
	)
	if handled || err != nil || len(participants) != 0 {
		t.Fatalf("static = %d handled %v err %v", len(participants), handled, err)
	}
}

func replicatedSQLTransactionFixture(
	t testing.TB,
	replicatedOnly bool,
	withReadyIndex ...bool,
) (*Snapshot, *Executor) {
	t.Helper()
	config, endpoints, descriptor, _ := testReplicatedTableInput(t)
	config.Placements = []distribution.TablePlacement{
		{Table: "accounts", Distribution: "data", Columns: []string{"/id"}},
		{Table: "messages", Distribution: "data", Columns: []string{"/id"}},
	}
	profiles := []ReplicatedTableProfile{
		{
			Table: "accounts", Relation: 1, PrimaryKey: "/id", SchemaGeneration: 8,
			LogicalSchemaDigest: replication.Digest{10}, MaxKeyBytes: 256,
			MaxDocumentBytes: 4 << 20,
		},
		{
			Table: "messages", Relation: 2, PrimaryKey: "/id", SchemaGeneration: 8,
			LogicalSchemaDigest: replication.Digest{10}, MaxKeyBytes: 256,
			MaxDocumentBytes: 4 << 20,
		},
	}

	logsManifest, err := distribution.NewManifest("logs-data", 3, []distribution.Shard{{
		ID: "logs-all", AllocationGeneration: 1,
		Range: distribution.KeyRange{
			Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true},
		},
		Leaders: []distribution.EndpointID{"logs-a", "logs-b", "logs-c"}, Epoch: 7,
	}})
	if err != nil {
		t.Fatal(err)
	}
	config.Distributions = append(config.Distributions, distribution.DistributionSpec{
		Name: "logs-data", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
	})
	config.Manifests = append(config.Manifests, logsManifest)
	config.Placements = append(config.Placements, distribution.TablePlacement{
		Table: "logs", Distribution: "logs-data", Columns: []string{"/id"},
	})
	profiles = append(profiles, ReplicatedTableProfile{
		Table: "logs", Relation: 1, PrimaryKey: "/id", SchemaGeneration: 8,
		LogicalSchemaDigest: replication.Digest{10}, MaxKeyBytes: 256,
		MaxDocumentBytes: 4 << 20,
	})
	endpoints["logs-a"], endpoints["logs-b"], endpoints["logs-c"] =
		"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"
	endpoints["logs-native-a"], endpoints["logs-native-b"], endpoints["logs-native-c"] =
		"127.0.0.1:8101", "127.0.0.1:8102", "127.0.0.1:8103"
	endpoints["logs-control-a"], endpoints["logs-control-b"], endpoints["logs-control-c"] =
		"127.0.0.1:8201", "127.0.0.1:8202", "127.0.0.1:8203"
	var indexes []IndexDescriptor
	descriptors := []ReplicatedShardDescriptor{descriptor}
	if len(withReadyIndex) != 0 && withReadyIndex[0] {
		locatorPaths := []string{"/id"}
		if len(withReadyIndex) > 1 && withReadyIndex[1] {
			locatorPaths = append(locatorPaths, "/region")
		}
		indexManifest, indexErr := distribution.NewManifest(
			"messages-email", 1, []distribution.Shard{{
				ID: "messages-email-all", AllocationGeneration: 1,
				Range: distribution.KeyRange{
					Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true},
				},
				Leaders: []distribution.EndpointID{
					"messages-email-a", "messages-email-b", "messages-email-c",
				}, Epoch: 1,
			}},
		)
		if indexErr != nil {
			t.Fatal(indexErr)
		}
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{
			Name: "messages-email", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		})
		config.Manifests = append(config.Manifests, indexManifest)
		config.Placements = append(config.Placements, distribution.TablePlacement{
			Table: "messages_email", Distribution: "messages-email", Columns: []string{"/email"},
		})
		for ordinal, letter := range []string{"a", "b", "c"} {
			endpoints[distribution.EndpointID("messages-email-"+letter)] =
				"127.0.0.1:" + string(rune('1'+ordinal)) + "301"
			endpoints[distribution.EndpointID("messages-email-native-"+letter)] =
				"127.0.0.1:" + string(rune('1'+ordinal)) + "311"
			endpoints[distribution.EndpointID("messages-email-control-"+letter)] =
				"127.0.0.1:" + string(rune('1'+ordinal)) + "321"
		}
		indexes = []IndexDescriptor{{
			IndexID: 19, Incarnation: 1, Table: "messages", Name: "by_email",
			Relation: "messages_email", Paths: []string{"/email"},
			LocatorPaths: locatorPaths, PrimaryPath: "/id",
			Flags: IndexGlobal | IndexUnique | IndexOrdered, Lifecycle: IndexReady,
		}}
		indexDescriptor := descriptor
		indexDescriptor.Replicas = append(
			[]ReplicatedReplicaDescriptor(nil), descriptor.Replicas...,
		)
		indexDescriptor.Distribution, indexDescriptor.Shard =
			"messages-email", "messages-email-all"
		indexDescriptor.Group.ShardIncarnation[0] += 2
		indexDescriptor.Group.GroupID[0] += 2
		indexDescriptor.Command.OwnershipEpoch = 1
		indexDescriptor.Command.RoutingVersion = 1
		for ordinal := range indexDescriptor.Replicas {
			letter := string(rune('a' + ordinal))
			indexDescriptor.Replicas[ordinal].Endpoint =
				distribution.EndpointID("messages-email-" + letter)
			indexDescriptor.Replicas[ordinal].NativeEndpoint =
				distribution.EndpointID("messages-email-native-" + letter)
			indexDescriptor.Replicas[ordinal].ControlEndpoint =
				distribution.EndpointID("messages-email-control-" + letter)
			indexDescriptor.Replicas[ordinal].StoreID[0] += 80
		}
		descriptors = append(descriptors, indexDescriptor)
		profiles = append(profiles, ReplicatedTableProfile{
			Table: "messages_email", Relation: 1, PrimaryKey: "/email",
			SchemaGeneration: 8, LogicalSchemaDigest: replication.Digest{10},
			MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20,
		})
		if len(withReadyIndex) > 2 && withReadyIndex[2] {
			regionManifest, regionErr := distribution.NewManifest(
				"messages-region", 1, []distribution.Shard{{
					ID: "messages-region-all", AllocationGeneration: 1,
					Range: distribution.KeyRange{
						Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true},
					},
					Leaders: []distribution.EndpointID{
						"messages-region-a", "messages-region-b", "messages-region-c",
					}, Epoch: 1,
				}},
			)
			if regionErr != nil {
				t.Fatal(regionErr)
			}
			config.Distributions = append(config.Distributions, distribution.DistributionSpec{
				Name: "messages-region", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
			})
			config.Manifests = append(config.Manifests, regionManifest)
			config.Placements = append(config.Placements, distribution.TablePlacement{
				Table: "messages_region", Distribution: "messages-region", Columns: []string{"/region"},
			})
			for ordinal, letter := range []string{"a", "b", "c"} {
				endpoints[distribution.EndpointID("messages-region-"+letter)] =
					"127.0.0.1:" + string(rune('4'+ordinal)) + "301"
				endpoints[distribution.EndpointID("messages-region-native-"+letter)] =
					"127.0.0.1:" + string(rune('4'+ordinal)) + "311"
				endpoints[distribution.EndpointID("messages-region-control-"+letter)] =
					"127.0.0.1:" + string(rune('4'+ordinal)) + "321"
			}
			indexes = append(indexes, IndexDescriptor{
				IndexID: 20, Incarnation: 1, Table: "messages", Name: "by_region",
				Relation: "messages_region", Paths: []string{"/region"},
				LocatorPaths: []string{"/id"}, PrimaryPath: "/id",
				Flags: IndexGlobal | IndexUnique | IndexOrdered, Lifecycle: IndexReady,
			})
			regionDescriptor := descriptor
			regionDescriptor.Replicas = append(
				[]ReplicatedReplicaDescriptor(nil), descriptor.Replicas...,
			)
			regionDescriptor.Distribution, regionDescriptor.Shard =
				"messages-region", "messages-region-all"
			regionDescriptor.Group.ShardIncarnation[0] += 4
			regionDescriptor.Group.GroupID[0] += 4
			regionDescriptor.Command.OwnershipEpoch = 1
			regionDescriptor.Command.RoutingVersion = 1
			for ordinal := range regionDescriptor.Replicas {
				letter := string(rune('a' + ordinal))
				regionDescriptor.Replicas[ordinal].Endpoint =
					distribution.EndpointID("messages-region-" + letter)
				regionDescriptor.Replicas[ordinal].NativeEndpoint =
					distribution.EndpointID("messages-region-native-" + letter)
				regionDescriptor.Replicas[ordinal].ControlEndpoint =
					distribution.EndpointID("messages-region-control-" + letter)
				regionDescriptor.Replicas[ordinal].StoreID[0] += 100
			}
			descriptors = append(descriptors, regionDescriptor)
			profiles = append(profiles, ReplicatedTableProfile{
				Table: "messages_region", Relation: 1, PrimaryKey: "/region",
				SchemaGeneration: 8, LogicalSchemaDigest: replication.Digest{10},
				MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20,
			})
		}
	}
	logsDescriptor := descriptor
	logsDescriptor.Replicas = append(
		[]ReplicatedReplicaDescriptor(nil), descriptor.Replicas...,
	)
	logsDescriptor.Distribution, logsDescriptor.Shard = "logs-data", "logs-all"
	logsDescriptor.Group.ShardIncarnation[0]++
	logsDescriptor.Group.GroupID[0]++
	for index := range logsDescriptor.Replicas {
		letter := []byte{'a' + byte(index)}
		logsDescriptor.Replicas[index].Endpoint = distribution.EndpointID("logs-" + string(letter))
		logsDescriptor.Replicas[index].NativeEndpoint = distribution.EndpointID("logs-native-" + string(letter))
		logsDescriptor.Replicas[index].ControlEndpoint = distribution.EndpointID("logs-control-" + string(letter))
		logsDescriptor.Replicas[index].StoreID[0] += 40
	}

	if !replicatedOnly {
		legacyManifest, manifestErr := distribution.NewManifest("legacy-data", 1, []distribution.Shard{{
			ID: "legacy-all", AllocationGeneration: 1,
			Range: distribution.KeyRange{
				Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true},
			},
			Leaders: []distribution.EndpointID{"legacy"}, Epoch: 1,
		}})
		if manifestErr != nil {
			t.Fatal(manifestErr)
		}
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{
			Name: "legacy-data", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		})
		config.Manifests = append(config.Manifests, legacyManifest)
		config.Placements = append(config.Placements, distribution.TablePlacement{
			Table: "legacy", Distribution: "legacy-data", Columns: []string{"/id"},
		})
		endpoints["legacy"] = "127.0.0.1:9001"
	}

	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 7, indexes, nil,
		append(descriptors, logsDescriptor), profiles,
	)
	if err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(nil, NewCatalogHolder(snapshot), Options{})
	return snapshot, executor
}
