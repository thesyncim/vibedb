package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
)

func TestReplicatedReadSessionUsesBorrowedImmutableCut(t *testing.T) {
	_, db, binding, _ := prepareReplicatedTestRoot(t, "sql-read-cut", false)
	base := requireReplicatedShardStoreBind(t, db, binding, "docs")
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := db.OpenReplicatedApply(base, bootstrap, testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	if _, err = claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	put := func(index, sequence uint64, document string) {
		t.Helper()
		command := testReplicatedApplyCommand(base, epoch, sequence, replication.Mutation{
			Kind: replication.MutationPut, Key: testReplicatedApplyKey(t, db, []byte(document)), Value: []byte(document),
		})
		if _, err := claim.ApplyNormal(testReplicatedApplyMeta(index), command); err != nil {
			t.Fatal(err)
		}
	}
	put(3, 2, `{"id":"a","value":10}`)
	var cut replicatedstate.DataReadCut
	if err := claim.DataReadCutInto(nil, 3, &cut); err != nil {
		t.Fatal(err)
	}
	defer cut.Close()
	put(4, 3, `{"id":"b","value":20}`)
	ctx := context.Background()
	reader, err := claim.NewDataReadSession(ctx, &cut, query.ExecOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, test := range []struct{ sql, want string }{
		{`SELECT value FROM docs ORDER BY value DESC LIMIT 1`, "10"},
		{`SELECT COUNT(*) FROM docs`, "1"},
		{`SELECT value FROM docs WHERE id >= 'a' AND id < 'b' ORDER BY id LIMIT 64`, "10"},
		{`SELECT COUNT(*) FROM docs WHERE id >= 'b'`, "0"},
		{`SELECT SUM(value) FROM docs`, "10"},
		{`SELECT a.value FROM docs a JOIN docs b ON a.id = b.id`, "10"},
	} {
		t.Run(test.sql, func(t *testing.T) {
			p, err := reader.Prepare(ctx, test.sql)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			var cur Cursor
			defer cur.Close()
			if err := p.QueryInto(ctx, nil, &cur); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(test.sql, "id >=") && !reader.conn.exec.Stats.PrimaryRangeBounded {
				t.Fatal("RF3 range scanned the full snapshot")
			}
			if !cur.Next() {
				t.Fatal("no row")
			}
			if got := string(cur.Cell(0).AppendJSON(nil)); got != test.want {
				t.Fatalf("got %s want %s", got, test.want)
			}
			if cur.Next() {
				t.Fatal("unexpected second row")
			}
		})
	}
	if _, err := reader.Prepare(ctx, `DELETE FROM docs`); !errors.Is(err, ErrReadOnlyTransaction) {
		t.Fatalf("write: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot, ok := cut.Relation(1); !ok || snapshot == nil {
		t.Fatal("session closed its caller's cut")
	}
	if _, err := claim.NewDataReadSession(ctx, &replicatedstate.DataReadCut{}, query.ExecOptions{}); !errors.Is(err, ErrReplicatedApplyMismatch) {
		t.Fatalf("foreign cut: %v", err)
	}
}
