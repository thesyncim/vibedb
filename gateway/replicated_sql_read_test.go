package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func scatterSQLReadRequest(
	fixture scatterCatalogFixture,
) ReplicatedSQLBatchReadRequest {
	request := ReplicatedSQLBatchReadRequest{MaxResultBytes: fixture.request.MaxResultBytes}
	request.Queries = make([]Query, len(fixture.request.Points))
	for index := range fixture.request.Points {
		request.Queries[index] = Query{
			SQL: fmt.Sprintf("SELECT * FROM %s WHERE id = ?", fixture.request.Points[index].Table),
			Params: []shardservice.Param{
				shardservice.StringParam(fmt.Sprintf("key-%03d", index)),
			},
		}
	}
	return request
}

func sameGroupSQLReadFixture(t testing.TB) (scatterCatalogFixture, ReplicatedSQLBatchReadRequest) {
	t.Helper()
	config, endpoints, descriptor, first := testReplicatedTableInput(t)
	config.Placements = append(config.Placements, config.Placements[0])
	config.Placements[0].Table = "accounts"
	config.Placements[1].Table = "messages"
	first.Table = "accounts"
	second := first
	second.Table, second.Relation = "messages", 2
	profiles := []ReplicatedTableProfile{first, second}
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 5, nil, nil,
		[]ReplicatedShardDescriptor{descriptor}, profiles,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture := scatterCatalogFixture{
		snapshot: snapshot, descriptors: []ReplicatedShardDescriptor{descriptor},
		config: config, endpoints: endpoints, profiles: profiles,
	}
	fixture.request.MaxResultBytes = 1 << 20
	request := ReplicatedSQLBatchReadRequest{MaxResultBytes: 1 << 20}
	for index, table := range []string{"accounts", "messages"} {
		text := []byte{'a' + byte(index)}
		key, ok := orderedkey.AppendString(nil, text, orderedkey.Ascending)
		if !ok {
			t.Fatal("ordered key")
		}
		fixture.request.Points = append(fixture.request.Points,
			ReplicatedTableBatchPoint{Table: []byte(table), Key: key})
		request.Queries = append(request.Queries, Query{
			SQL:    "SELECT * FROM " + table + " WHERE id = ?",
			Params: []shardservice.Param{shardservice.StringBytesParam(text)},
		})
		var replicas [ServingReplicaCount]ReplicatedEndpoint
		var scratch [replication.MaxMutationKeyBytes + 16]byte
		resolved, resolvedOK := snapshot.ResolveReplicatedTableKey(
			[]byte(table), key, scratch[:0], replicas[:0],
		)
		if !resolvedOK {
			t.Fatalf("resolve %s", table)
		}
		fixture.routes = append(fixture.routes, resolved)
	}
	return fixture, request
}

func TestReplicatedSQLReadSameGroupMultiRelationUsesOneCoherentCut(t *testing.T) {
	fixture, request := sameGroupSQLReadFixture(t)
	client := &scatterReadClient{}
	reader := newScatterReader(t, fixture, client, nil, 2)
	result, err := reader.ReadSQLBatch(context.Background(), request)
	defer result.Release()
	if err != nil || result.Count() != 2 || len(result.Observations) != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if result.Observations[0].RouteID != fixture.routes[0].RouteID ||
		result.Observations[0].Group != fixture.routes[0].Route.Group {
		t.Fatalf("observation=%+v", result.Observations[0])
	}
	client.mu.Lock()
	reads := client.reads[fixture.routes[0].Route.Group]
	client.mu.Unlock()
	if reads != 1 {
		t.Fatalf("group reads=%d, want one ReadIndex", reads)
	}
	for index, want := range []string{"value-000", "value-001"} {
		raw, found, ok := result.Lookup(index)
		if !ok || !found || string(raw) != want {
			t.Fatalf("position %d raw=%q found=%v ok=%v", index, raw, found, ok)
		}
	}
}

func TestReplicatedSQLReadCrossGroupReturnsObservationVector(t *testing.T) {
	fixture := newScatterCatalogFixture(t, 2, 5)
	client := &scatterReadClient{}
	reader := newScatterReader(t, fixture, client, nil, 2)
	result, err := reader.ReadSQLBatch(context.Background(), scatterSQLReadRequest(fixture))
	defer result.Release()
	if err != nil || result.Count() != 2 || len(result.Observations) != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if bytes.Compare(result.Observations[0].RouteID[:], result.Observations[1].RouteID[:]) >= 0 {
		t.Fatalf("observations not canonical: %+v", result.Observations)
	}
}

func TestReplicatedSQLReadDrainsSixtyFiveGroupsWithoutParticipantCap(t *testing.T) {
	fixture := newScatterCatalogFixture(t, 65, 5)
	client := &scatterReadClient{delay: 200 * time.Microsecond}
	reader := newScatterReader(t, fixture, client, nil, 3)
	result, err := reader.ReadSQLBatch(context.Background(), scatterSQLReadRequest(fixture))
	defer result.Release()
	if err != nil || result.Count() != 65 || len(result.Observations) != 65 {
		t.Fatalf("count=%d observations=%d err=%v",
			result.Count(), len(result.Observations), err)
	}
	if maximum := client.maxActive.Load(); maximum == 0 || maximum > 3 {
		t.Fatalf("active reads=%d, want 1..3", maximum)
	}
}

func TestReplicatedSQLReadStaleRouteReplaysEveryQuery(t *testing.T) {
	oldFixture := newScatterCatalogFixture(t, 2, 5)
	newFixture := newScatterCatalogFixture(t, 2, 6)
	staleIndex := 0
	if bytes.Compare(oldFixture.routes[1].RouteID[:], oldFixture.routes[0].RouteID[:]) > 0 {
		staleIndex = 1
	}
	newFixture.descriptors[staleIndex].Command.RouteGeneration++
	newSnapshot, err := NewSnapshotWithReplicatedTableMetadata(
		newFixture.config, newFixture.endpoints, 6, nil, nil,
		newFixture.descriptors, newFixture.profiles,
	)
	if err != nil {
		t.Fatal(err)
	}
	client := &scatterReadClient{
		staleGroup:     oldFixture.routes[staleIndex].Route.Group,
		staleCommand:   newFixture.descriptors[staleIndex].Command,
		staleRemaining: true,
	}
	refreshes := 0
	reader := newScatterReader(t, oldFixture, client,
		func(context.Context, uint64) (*Snapshot, error) {
			refreshes++
			return newSnapshot, nil
		}, 1)
	result, err := reader.ReadSQLBatch(context.Background(), scatterSQLReadRequest(oldFixture))
	defer result.Release()
	if err != nil || result.Count() != 2 || len(result.Observations) != 2 || refreshes != 1 {
		t.Fatalf("result=%+v refreshes=%d err=%v", result, refreshes, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, resolved := range oldFixture.routes {
		if client.reads[resolved.Route.Group] != 2 {
			t.Fatalf("group %x reads=%d, want complete replay",
				resolved.Route.Group.GroupID, client.reads[resolved.Route.Group])
		}
	}
}

func TestReplicatedSQLReadIntentFailureReturnsNoPartialResult(t *testing.T) {
	fixture := newScatterCatalogFixture(t, 2, 5)
	intentIndex := 0
	if bytes.Compare(fixture.routes[1].RouteID[:], fixture.routes[0].RouteID[:]) > 0 {
		intentIndex = 1
	}
	client := &scatterReadClient{intentGroup: fixture.routes[intentIndex].Route.Group}
	reader := newScatterReader(t, fixture, client, nil, 1)
	result, err := reader.ReadSQLBatch(context.Background(), scatterSQLReadRequest(fixture))
	if !errors.Is(err, ErrReplicatedReadIntentActive) || result.Packed != nil ||
		result.Observations != nil || result.Count() != 0 {
		t.Fatalf("partial result=%+v err=%v", result, err)
	}
	if reader.readBytes.Load() != 0 {
		t.Fatalf("read bytes retained=%d", reader.readBytes.Load())
	}
}

func TestReplicatedSQLReadRejectsMemoryBeforeShardIO(t *testing.T) {
	fixture := newScatterCatalogFixture(t, 3, 5)
	client := &scatterReadClient{}
	reader := newScatterReader(t, fixture, client, nil, 2)
	reader.maxReadBytes = uint64(fixture.request.MaxResultBytes)
	result, err := reader.ReadSQLBatch(context.Background(), scatterSQLReadRequest(fixture))
	if !errors.Is(err, ErrReplicatedReadAdmission) || result.Packed != nil ||
		result.Observations != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for group, reads := range client.reads {
		if reads != 0 {
			t.Fatalf("group %x reads=%d before admission", group.GroupID, reads)
		}
	}
}

func TestReplicatedSQLReadRejectsTypedMetadataBeforeShardIO(t *testing.T) {
	fixture, request := sameGroupSQLReadFixture(t)
	client := &scatterReadClient{}
	reader := newScatterReader(t, fixture, client, nil, 2)
	base := request.Queries[0]
	tests := []struct {
		name  string
		query Query
		want  error
	}{
		{
			name: "count mismatch",
			query: Query{SQL: base.SQL, Params: base.Params,
				ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeText, sqldriver.ParamTypeText}},
			want: ErrPlanParameters,
		},
		{
			name: "invalid enum",
			query: Query{SQL: base.SQL, Params: base.Params,
				ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeInvalid}},
			want: ErrPlanParameters,
		},
		{
			name: "all unspecified",
			query: Query{SQL: base.SQL, Params: base.Params,
				ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeUnspecified}},
			want: ErrPlanParameters,
		},
		{
			name: "count over admission bound",
			query: Query{SQL: base.SQL, Params: base.Params,
				ParamTypes: make([]sqldriver.ParamType, maxGatewaySQLParameters+1)},
			want: ErrReplicatedReadAdmission,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := reader.ReadSQLBatch(t.Context(), ReplicatedSQLBatchReadRequest{
				Queries: []Query{test.query}, MaxResultBytes: request.MaxResultBytes,
			})
			defer result.Release()
			if !errors.Is(err, test.want) || result.Packed != nil {
				t.Fatalf("result=%+v err=%v, want %v", result, err, test.want)
			}
		})
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for group, reads := range client.reads {
		if reads != 0 {
			t.Fatalf("group %x reads=%d for refused typed metadata", group.GroupID, reads)
		}
	}
}

func TestReplicatedSQLReadRefusesProjectionAndJoinBeforeShardIO(t *testing.T) {
	fixture, _ := sameGroupSQLReadFixture(t)
	client := &scatterReadClient{}
	reader := newScatterReader(t, fixture, client, nil, 2)
	for _, query := range []Query{
		{SQL: "SELECT id FROM accounts WHERE id = ?",
			Params: []shardservice.Param{shardservice.StringParam("a")}},
		{SQL: "SELECT accounts.* FROM accounts JOIN messages ON accounts.id = messages.id WHERE accounts.id = ?",
			Params: []shardservice.Param{shardservice.StringParam("a")}},
	} {
		result, err := reader.ReadSQLBatch(context.Background(), ReplicatedSQLBatchReadRequest{
			Queries: []Query{query}, MaxResultBytes: 1 << 20,
		})
		if !errors.Is(err, ErrReplicatedSQLReadUnsupported) || result.Packed != nil {
			t.Fatalf("query=%q result=%+v err=%v", query.SQL, result, err)
		}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for group, reads := range client.reads {
		if reads != 0 {
			t.Fatalf("group %x reads=%d for refused SQL", group.GroupID, reads)
		}
	}
}

func BenchmarkReplicatedSQLReadEightGroups(b *testing.B) {
	fixture := newScatterCatalogFixture(b, 8, 5)
	fixture.request.MaxResultBytes = 64 << 10
	client := &scatterReadClient{}
	reader := newScatterReader(b, fixture, client, nil, 8)
	request := scatterSQLReadRequest(fixture)
	b.ReportAllocs()
	for b.Loop() {
		result, err := reader.ReadSQLBatch(context.Background(), request)
		if err != nil || result.Count() != 8 {
			b.Fatalf("count=%d err=%v", result.Count(), err)
		}
		result.Release()
	}
}
