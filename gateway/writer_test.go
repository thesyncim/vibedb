package gateway

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
	sqlast "github.com/thesyncim/vibedb/sql"
)

// writePlanBind prepares and binds one mutating statement against the standard
// two-shard test snapshot, returning the bound write plan. It asserts the
// statement kind is a write and that both stages succeed unless wantErr is set.
func writePlanBind(t *testing.T, snap *Snapshot, sql string, args []shardservice.Param, wantErr error) *BoundWritePlan {
	t.Helper()
	prepared, err := snap.Prepare(context.Background(), sql)
	if wantErr == nil {
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		if prepared.statement.Kind == sqlast.KindSelect {
			t.Fatalf("prepared kind = SELECT, want a mutation")
		}
	}
	bound, err := prepared.BindWrite(runtimeArgsFor(t, args))
	if wantErr != nil {
		if !errors.Is(err, wantErr) {
			t.Fatalf("BindWrite err = %v, want errors.Is %v", err, wantErr)
		}
		return nil
	}
	if err != nil {
		t.Fatalf("BindWrite: %v", err)
	}
	return bound
}

// runtimeArgsFor converts wire parameters to the sql/driver scalar vocabulary
// the gateway binds against, mirroring queryRuntimeArgs.
func runtimeArgsFor(t *testing.T, params []shardservice.Param) []any {
	t.Helper()
	args, err := queryRuntimeArgs(params)
	if err != nil {
		t.Fatalf("queryRuntimeArgs: %v", err)
	}
	return args
}

// TestBindInsertRowKeys proves an INSERT extracts one full shard key per VALUES
// row: a flat row reads its shard-key ordinal from the named column and a
// whole-document row parses the document and reads the pointer out of it.
func TestBindInsertRowKeys(t *testing.T) {
	snap := testSnapshot(t, 1)

	flat := writePlanBind(t, snap,
		`INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
		[]shardservice.Param{shardservice.StringParam("acme"), shardservice.NumberParam("1")}, nil)
	if len(flat.rowKeys) != 1 || len(flat.rowKeys[0]) != 1 {
		t.Fatalf("flat insert row keys = %v, want one key of one ordinal", flat.rowKeys)
	}
	if s, ok := flat.rowKeys[0][0].StringValue(); !ok || s != "acme" {
		t.Fatalf("flat insert shard key = %q, want acme", s)
	}

	doc := writePlanBind(t, snap,
		`INSERT INTO messages VALUES (?)`,
		[]shardservice.Param{shardservice.DocumentParam(`{"tenant_id":"acme","n":1}`)}, nil)
	if len(doc.rowKeys) != 1 || len(doc.rowKeys[0]) != 1 {
		t.Fatalf("document insert row keys = %v, want one key of one ordinal", doc.rowKeys)
	}
	if s, ok := doc.rowKeys[0][0].StringValue(); !ok || s != "acme" {
		t.Fatalf("document insert shard key = %q, want acme", s)
	}
	if len(doc.keyPointers) != 1 {
		t.Fatalf("document insert key pointers = %d, want 1", len(doc.keyPointers))
	}
}

// TestBindInsertRejects proves the write planner fails closed on the INSERT
// shapes it cannot prove single-shard: a missing shard-key column, a document
// without the shard key, a NULL shard key, and a query source.
func TestBindInsertRejects(t *testing.T) {
	snap := testSnapshot(t, 1)

	// The shard-key column is absent from the flat column list, so the row's
	// shard key is unknown at routing time.
	writePlanBind(t, snap,
		`INSERT INTO messages (n) VALUES (?)`,
		[]shardservice.Param{shardservice.NumberParam("1")}, ErrDistributedWriteUnsupported)

	// A whole-document row that omits the shard key cannot be routed.
	writePlanBind(t, snap,
		`INSERT INTO messages VALUES (?)`,
		[]shardservice.Param{shardservice.DocumentParam(`{"n":1}`)}, ErrPlanParameters)

	// A NULL shard key is not a placement scalar.
	writePlanBind(t, snap,
		`INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
		[]shardservice.Param{shardservice.NullParam(), shardservice.NumberParam("1")}, ErrPlanParameters)

	// A query source requires a distributed source plan the write path has no
	// single-shard form for.
	writePlanBind(t, snap,
		`INSERT INTO messages SELECT * FROM messages`, nil, ErrDistributedWriteUnsupported)
}

// TestBindUpdateDelete proves an UPDATE or DELETE without a shard-key predicate
// is refused at bind, while one with a shard-key equality predicate binds its
// constraints and, for a whole-document UPDATE, materializes the replacement
// document for the immutability check.
func TestBindUpdateDelete(t *testing.T) {
	snap := testSnapshot(t, 1)

	// No predicate: a cross-shard scatter that cannot be proven single-shard.
	writePlanBind(t, snap,
		`UPDATE messages SET "$doc" = ?`,
		[]shardservice.Param{shardservice.DocumentParam(`{"tenant_id":"a","n":1}`)}, ErrDistributedWriteUnsupported)
	writePlanBind(t, snap,
		`DELETE FROM messages`, nil, ErrDistributedWriteUnsupported)

	// A shard-key equality predicate binds its constraints and carries the
	// replacement document for the immutability check.
	upd := writePlanBind(t, snap,
		`UPDATE messages SET "$doc" = ? WHERE tenant_id = ?`,
		[]shardservice.Param{
			shardservice.DocumentParam(`{"tenant_id":"acme","n":9}`),
			shardservice.StringParam("acme"),
		}, nil)
	if len(upd.constraints) != 1 {
		t.Fatalf("update constraints = %d, want one ordinal", len(upd.constraints))
	}
	if len(upd.updateDoc) == 0 {
		t.Fatal("update replacement document not materialized")
	}

	del := writePlanBind(t, snap,
		`DELETE FROM messages WHERE tenant_id = ?`,
		[]shardservice.Param{shardservice.StringParam("acme")}, nil)
	if len(del.constraints) != 1 {
		t.Fatalf("delete constraints = %d, want one ordinal", len(del.constraints))
	}
	if len(del.updateDoc) != 0 {
		t.Fatalf("delete carried a replacement document: %q", del.updateDoc)
	}
}

// TestBindWriteRejectsNonMutationAndDDL proves the write planner refuses
// statement kinds with no single-shard distributed form (DDL) and that a
// parameter arity mismatch is reported before any routing.
func TestBindWriteRejectsNonMutationAndDDL(t *testing.T) {
	snap := testSnapshot(t, 1)

	// DDL is refused at prepare time as a write-not-supported shape.
	if _, err := snap.Prepare(context.Background(), `DROP TABLE messages`); !errors.Is(err, ErrWriteNotSupported) {
		t.Fatalf("DDL prepare err = %v, want ErrWriteNotSupported", err)
	}

	// A parameter arity mismatch is a bind error before routing.
	writePlanBind(t, snap,
		`INSERT INTO messages (tenant_id, n) VALUES (?, ?)`,
		[]shardservice.Param{shardservice.StringParam("acme")}, ErrPlanParameters)
}

// TestRouteWriteInsertCrossShard proves an INSERT whose VALUES rows resolve to
// more than one shard is refused before dispatch, while a batch that resolves to
// one shard produces exactly one dispatch target.
func TestRouteWriteInsertCrossShard(t *testing.T) {
	snap := testSnapshot(t, 1)
	e := newRouteExecutor(t, snap)
	mapper := distribution.NewNativeMapper(1)

	a := shardKeyFor(t, snap, "-80")
	b := shardKeyFor(t, snap, "80-")

	cross := writePlanBind(t, snap,
		`INSERT INTO messages (tenant_id, n) VALUES (?, ?), (?, ?)`,
		[]shardservice.Param{
			shardservice.StringParam(a), shardservice.NumberParam("1"),
			shardservice.StringParam(b), shardservice.NumberParam("2"),
		}, nil)
	if _, err := e.insertTargets(cross, mapper); !errors.Is(err, ErrWriteCrossShard) {
		t.Fatalf("cross-shard insert = %v, want ErrWriteCrossShard", err)
	}

	single := writePlanBind(t, snap,
		`INSERT INTO messages (tenant_id, n) VALUES (?, ?), (?, ?)`,
		[]shardservice.Param{
			shardservice.StringParam(a), shardservice.NumberParam("1"),
			shardservice.StringParam(shardKeyFor(t, snap, "-80")), shardservice.NumberParam("2"),
		}, nil)
	targets, err := e.insertTargets(single, mapper)
	if err != nil {
		t.Fatalf("insertTargets: %v", err)
	}
	if len(targets) != 1 || targets[0].Shard != "-80" {
		t.Fatalf("insert targets = %+v, want one target on -80", targets)
	}
}

// TestRouteWriteShardKeyMove proves a whole-document UPDATE whose replacement
// routes to a different shard than the predicate target is refused, while a
// replacement that stays on the target shard is admitted.
func TestRouteWriteShardKeyMove(t *testing.T) {
	snap := testSnapshot(t, 1)
	e := newRouteExecutor(t, snap)
	mapper := distribution.NewNativeMapper(1)

	stay := shardKeyFor(t, snap, "-80")
	move := shardKeyFor(t, snap, "80-")

	admitted := writePlanBind(t, snap,
		`UPDATE messages SET "$doc" = ? WHERE tenant_id = ?`,
		[]shardservice.Param{
			shardservice.DocumentParam(`{"tenant_id":"` + stay + `","n":9}`),
			shardservice.StringParam(stay),
		}, nil)
	target := singleTargetFor(t, snap, stay)
	if err := e.writeDocShardKeyMatchesTarget(admitted, mapper, target); err != nil {
		t.Fatalf("same-shard replacement = %v, want admitted", err)
	}

	refused := writePlanBind(t, snap,
		`UPDATE messages SET "$doc" = ? WHERE tenant_id = ?`,
		[]shardservice.Param{
			shardservice.DocumentParam(`{"tenant_id":"` + move + `","n":9}`),
			shardservice.StringParam(stay),
		}, nil)
	if err := e.writeDocShardKeyMatchesTarget(refused, mapper, target); !errors.Is(err, ErrWriteShardKeyMove) {
		t.Fatalf("shard-key move = %v, want ErrWriteShardKeyMove", err)
	}
}

// shardKeyFor finds a distinct tenant_id that routes to want under the standard
// two-shard test manifest.
func shardKeyFor(t *testing.T, snap *Snapshot, want string) string {
	t.Helper()
	mapper := distribution.NewNativeMapper(1)
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("k%d", i)
		s := distribution.NewString(key)
		p, err := mapper.PointFor([]distribution.Scalar{s})
		if err != nil {
			t.Fatalf("PointFor: %v", err)
		}
		if id, ok := resolvePointShard(t, snap, "messages", p); ok && id == distribution.ShardID(want) {
			return key
		}
	}
	t.Fatalf("no key routes to shard %s", want)
	return ""
}

// singleTargetFor resolves a full shard key to its fenced leader target.
func singleTargetFor(t *testing.T, snap *Snapshot, key string) distribution.Target {
	t.Helper()
	mapper := distribution.NewNativeMapper(1)
	p, err := mapper.PointFor([]distribution.Scalar{distribution.NewString(key)})
	if err != nil {
		t.Fatalf("PointFor: %v", err)
	}
	target, ok := resolvePointTarget(t, snap, "messages", p)
	if !ok {
		t.Fatalf("key %q resolves to no target", key)
	}
	return target
}

// resolvePointShard resolves a point to its owning shard id through the table's
// pinned manifest.
func resolvePointShard(t *testing.T, snap *Snapshot, table string, p distribution.KeyspacePoint) (distribution.ShardID, bool) {
	t.Helper()
	_, _, man, ok := snap.plannerTableFor(table)
	if !ok {
		t.Fatalf("table %q has no placement", table)
	}
	return man.ResolvePoint(p)
}

// resolvePointTarget resolves a point to its owning fenced leader target.
func resolvePointTarget(t *testing.T, snap *Snapshot, table string, p distribution.KeyspacePoint) (distribution.Target, bool) {
	t.Helper()
	_, _, man, ok := snap.plannerTableFor(table)
	if !ok {
		t.Fatalf("table %q has no placement", table)
	}
	return man.ResolvePointTarget(p)
}
