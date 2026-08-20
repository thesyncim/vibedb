package gateway

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestGatewayDeclaresExactAndShardAccessScopes(t *testing.T) {
	snapshot := twoShardSnapshot(t, 1, 7)
	executor := NewExecutor(NewClient(nil), NewCatalogHolder(snapshot), Options{})
	query := Query{
		SQL: `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
		Params: []shardservice.Param{
			shardservice.StringParam("scoped-key"), shardservice.NumberParam("1"),
		},
		Class: ClassInteractive,
	}
	bound := writePlanBind(t, snapshot, query.SQL, query.Params, nil)
	call, _, _, err := executor.routeWrite(snapshot, &query, bound, executor.profileFor(query.Class))
	if err != nil {
		t.Fatal(err)
	}
	if call == nil || call.req.BucketBits != distribution.DefaultVirtualBucketBits ||
		len(call.req.AccessScopes) != 1 || call.req.AccessScopes[0].End != call.req.AccessScopes[0].Start+1 {
		t.Fatalf("exact write access = %+v", call)
	}
	mapper := distribution.NewNativeMapper(1)
	point, err := mapper.PointFor([]distribution.Scalar{distribution.NewString("scoped-key")})
	if err != nil {
		t.Fatal(err)
	}
	bucket, _ := distribution.VirtualBucketForPoint(point, mapper.VirtualBucketBits())
	if call.req.AccessScopes[0].Start != uint32(bucket) {
		t.Fatalf("write bucket = %d, want %d", call.req.AccessScopes[0].Start, bucket)
	}

	prepared, err := snapshot.Prepare(context.Background(), `SELECT n FROM messages ORDER BY n`)
	if err != nil {
		t.Fatal(err)
	}
	readBound, err := prepared.Bind(nil)
	if err != nil {
		t.Fatal(err)
	}
	read := Query{SQL: `SELECT n FROM messages ORDER BY n`, Class: ClassBatch}
	plan, err := executor.routeContext(
		context.Background(), snapshot, &read, readBound, executor.profileFor(ClassBatch),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.calls) != 2 {
		t.Fatalf("scatter calls = %d", len(plan.calls))
	}
	for i := range plan.calls {
		if plan.calls[i].req.BucketBits != distribution.DefaultVirtualBucketBits ||
			len(plan.calls[i].req.AccessScopes) != 1 {
			t.Fatalf("scatter call %d access = %+v", i, plan.calls[i].req.AccessScopes)
		}
		interval, ok := readBound.manifest.ShardBucketInterval(
			i, distribution.DefaultVirtualBucketBits,
		)
		if !ok || plan.calls[i].req.AccessScopes[0].Start != interval.Start ||
			plan.calls[i].req.AccessScopes[0].End != interval.End {
			t.Fatalf("scatter call %d scope = %+v, want %+v", i, plan.calls[i].req.AccessScopes, interval)
		}
	}
}

var (
	benchmarkIntentBits   uint8
	benchmarkIntentScopes []distributedtxn.IntentScope
)

func BenchmarkExactWriteAccessScopes(b *testing.B) {
	snapshot := testSnapshot(b, 1)
	params := []shardservice.Param{
		shardservice.StringParam("scoped-key"), shardservice.NumberParam("1"),
	}
	prepared, err := snapshot.Prepare(
		context.Background(), `INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
	)
	if err != nil {
		b.Fatal(err)
	}
	args, err := queryRuntimeArgs(params)
	if err != nil {
		b.Fatal(err)
	}
	bound, err := prepared.BindWrite(args)
	if err != nil {
		b.Fatal(err)
	}
	executor := NewExecutor(NewClient(nil), NewCatalogHolder(snapshot), Options{})
	targets, err := executor.insertTargets(
		bound, distribution.NewNativeMapperWithBucketBits(bound.spec.Arity, bound.spec.EffectiveBucketBits()),
	)
	if err != nil || len(targets) != 1 {
		b.Fatalf("targets = %+v, %v", targets, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkIntentBits, benchmarkIntentScopes = writeAccessScopes(bound, targets[0])
	}
}
