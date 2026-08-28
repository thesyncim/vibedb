package pgwire

import (
	"context"
	"testing"
)

type atomicTestBackend struct{ embeddedBackend }
type atomicTestSession struct{ BackendSession }

func (*atomicTestSession) AutocommitWrites() bool { return true }
func (b atomicTestBackend) NewSession(ctx context.Context, id SessionIdentity) (BackendSession, error) {
	s, err := b.embeddedBackend.NewSession(ctx, id)
	if err != nil {
		return nil, err
	}
	return &atomicTestSession{s}, nil
}

func TestAutocommitBackendWriteProtocolBoundaries(t *testing.T) {
	b := atomicTestBackend{embeddedBackend{testDatabase(t, "users", nil)}}
	s, err := NewServerWithBackend(b, Options{Auth: Trust()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := dial(t, s)
	c.startup(map[string]string{"user": "local"})
	requireWireOK(t, c.query(`INSERT INTO users ("_pgwire_key",id,name) VALUES ('a','a','first')`))
	expectError(t, c.query(`INSERT INTO users ("_pgwire_key",id,name) VALUES ('b','b','second'); DELETE FROM users WHERE id='a'`), sqlstateFeatureNotSupported)
	requireWireOK(t, c.query("BEGIN"))
	expectError(t, c.query(`DELETE FROM users WHERE id='a'`), sqlstateFeatureNotSupported)
	requireWireOK(t, c.query("ROLLBACK"))
	if got := rowsOf(t, c.query(`SELECT id FROM users`)); len(got) != 1 {
		t.Fatalf("refused write changed rows: %v", got)
	}
	c.send(msgParse, parseMsg("del", `DELETE FROM users WHERE id = $1`))
	c.send(msgBind, bindMsg("", "del", nil, [][]byte{[]byte("a")}, nil))
	c.send(msgExecute, executeMsg("", 0))
	c.send(msgSync, nil)
	msgs := c.until(msgReadyForQuery)
	requireWireOK(t, msgs)
	if got := commandTagOf(t, msgs); got != "DELETE 1" {
		t.Fatalf("tag=%s", got)
	}
	if got := rowsOf(t, c.query(`SELECT id FROM users`)); len(got) != 0 {
		t.Fatal("extended write not committed")
	}
}
