package driver

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
)

type replicatedPointSessionFixture struct {
	claim *ReplicatedApply
	base  ReplicatedShardStoreIdentity
	key   []byte
	raw   []byte
}

func newReplicatedPointSessionFixture(
	t *testing.T,
	document string,
) replicatedPointSessionFixture {
	t.Helper()
	_, database, binding, _ := prepareReplicatedTestRoot(t, "sql-point-session", false)
	base := requireReplicatedShardStoreBind(t, database, binding, "docs")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = claim.Close() })
	bootstrap := testReplicatedApplyBootstrap()
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	key := testReplicatedApplyKey(t, database, []byte(document))
	command := testReplicatedApplyCommand(
		base, epoch, 2, replication.Mutation{
			Kind: replication.MutationPut, Key: key, Value: []byte(document),
		},
	)
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil {
		t.Fatal(err)
	}
	read, err := claim.PointReadInto(
		1, key, claim.Applied(), base.UserLimits.MaxDocumentBytes, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return replicatedPointSessionFixture{
		claim: claim, base: base, key: append([]byte(nil), key...),
		raw: append([]byte(nil), read.Value...),
	}
}

func TestReplicatedPointReadSessionCandidatePreservesQuerySemantics(t *testing.T) {
	fixture := newReplicatedPointSessionFixture(t, `{"id":"a","value":10,"name":"alpha"}`)
	ctx := context.Background()
	session, err := fixture.claim.NewPointReadSession(
		ctx, 1, fixture.key, true, fixture.raw,
		[]byte(fixture.base.UserPrimaryKey), query.ExecOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	prepared, err := session.Prepare(ctx,
		`SELECT id, value FROM docs WHERE id = 'a' AND value > 5 ORDER BY value DESC LIMIT 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	var cursor Cursor
	if err := prepared.QueryCandidateKeysInto(
		ctx, nil, []byte("/id"), [][]byte{fixture.key}, &cursor,
	); err != nil {
		t.Fatal(err)
	}
	if !cursor.Next() {
		t.Fatal("point hit returned no row")
	}
	if got := string(cursor.Cell(0).AppendJSON(nil)); got != `"a"` {
		t.Fatalf("id = %s, want %s", got, `"a"`)
	}
	if got := string(cursor.Cell(1).AppendJSON(nil)); got != "10" ||
		cursor.Cell(1).Kind() != query.TypeNumber {
		t.Fatalf("value = %s (%v), want 10 (number)", got, cursor.Cell(1).Kind())
	}
	if cursor.Next() {
		t.Fatal("point hit returned more than one row")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}

	residual, err := session.Prepare(ctx,
		`SELECT value FROM docs WHERE id = 'a' AND value > 100`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer residual.Close()
	cursor = Cursor{}
	if err := residual.QueryCandidateKeysInto(
		ctx, nil, []byte("/id"), [][]byte{fixture.key}, &cursor,
	); err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if cursor.Next() {
		t.Fatal("false residual predicate returned the point")
	}
}

func TestReplicatedPointReadSessionMissAndWrongKeyAreBounded(t *testing.T) {
	fixture := newReplicatedPointSessionFixture(t, `{"id":"a","value":10}`)
	ctx := context.Background()
	miss, err := fixture.claim.NewPointReadSession(
		ctx, 1, fixture.key, false, nil,
		[]byte(fixture.base.UserPrimaryKey), query.ExecOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer miss.Close()
	prepared, err := miss.Prepare(ctx, `SELECT COUNT(*) FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	var cursor Cursor
	if err := prepared.QueryCandidateKeysInto(
		ctx, nil, []byte("/id"), [][]byte{fixture.key}, &cursor,
	); err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() || string(cursor.Cell(0).AppendJSON(nil)) != "0" {
		t.Fatalf("miss count = %s, want 0", cursor.Cell(0).AppendJSON(nil))
	}

	wrongKey := append([]byte(nil), fixture.key...)
	wrongKey[len(wrongKey)-1]++
	wrong, err := fixture.claim.NewPointReadSession(
		ctx, 1, wrongKey, true, fixture.raw,
		[]byte(fixture.base.UserPrimaryKey), query.ExecOptions{},
	)
	if err == nil || !errors.Is(err, ErrReplicatedApplyMismatch) {
		t.Fatalf("wrong key constructor = %v, want mismatch", err)
	}
	if wrong != nil {
		_ = wrong.Close()
	}
}

func TestReplicatedPointReadSessionRejectsStaleIdentityAndPath(t *testing.T) {
	fixture := newReplicatedPointSessionFixture(t, `{"id":"a","value":10}`)
	ctx := context.Background()
	if _, err := fixture.claim.NewPointReadSession(
		ctx, 1, fixture.key, true, fixture.raw,
		[]byte("/other"), query.ExecOptions{},
	); !errors.Is(err, ErrReplicatedApplyMismatch) {
		t.Fatalf("wrong path = %v, want mismatch", err)
	}

	core := fixture.claim.database
	core.mu.Lock()
	prior := core.catalog.ReplicatedShardStore.RelationSchemaGeneration
	core.catalog.ReplicatedShardStore.RelationSchemaGeneration++
	core.mu.Unlock()
	_, err := fixture.claim.NewPointReadSession(
		ctx, 1, fixture.key, true, fixture.raw,
		[]byte(fixture.base.UserPrimaryKey), query.ExecOptions{},
	)
	core.mu.Lock()
	core.catalog.ReplicatedShardStore.RelationSchemaGeneration = prior
	core.mu.Unlock()
	if !errors.Is(err, ErrReplicatedApplyMismatch) {
		t.Fatalf("stale schema identity = %v, want mismatch", err)
	}

	mutated := append([]byte(nil), fixture.raw...)
	session, err := fixture.claim.NewPointReadSession(
		ctx, 1, fixture.key, true, mutated,
		[]byte(fixture.base.UserPrimaryKey), query.ExecOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	mutated[0] = 'x'
	defer session.Close()
	prepared, err := session.Prepare(ctx, `SELECT value FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	cursor := Cursor{}
	if err := prepared.QueryCandidateKeysInto(
		ctx, nil, []byte("/id"), [][]byte{fixture.key}, &cursor,
	); err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if !cursor.Next() || !bytes.Equal(cursor.Cell(0).AppendJSON(nil), []byte("10")) {
		t.Fatalf("detached point value = %s, want 10", cursor.Cell(0).AppendJSON(nil))
	}
}
