package gateway

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/pgwire"
)

func diagnosticWireRows(t *testing.T, messages map[byte][][]byte) []string {
	t.Helper()
	if len(messages['E']) != 0 {
		t.Fatalf("query returned ErrorResponse: %q", messages['E'])
	}
	rows := make([]string, 0, len(messages['D']))
	for _, body := range messages['D'] {
		if len(body) < 6 || binary.BigEndian.Uint16(body[:2]) != 1 {
			t.Fatalf("DataRow = %q, want one column", body)
		}
		length := int(int32(binary.BigEndian.Uint32(body[2:6])))
		if length < 0 || 6+length != len(body) {
			t.Fatalf("DataRow length = %d for %d-byte body", length, len(body))
		}
		rows = append(rows, string(body[6:]))
	}
	return rows
}

func TestPostgreSQLSimpleCacheOwnsSplitStatementSQL(t *testing.T) {
	client, _ := newTwoShardCluster(t, 3)
	t.Cleanup(func() { _ = client.Close() })
	executor := NewExecutor(client, NewCatalogHolder(twoShardSnapshot(t, 1, 3)), Options{})
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	server, err := pgwire.NewServerWithBackend(
		&PostgreSQLBackend{
			Executor: executor,
			Authorize: func(pgwire.SessionIdentity) (serviceauthz.Authority, error) {
				return authority, nil
			},
		},
		pgwire.Options{Auth: pgwire.Trust(), Database: "app"},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	session := openSQLDiagnosticWireSession(t, server)

	// The second statement is cached from a substring whose source pointer is
	// past the beginning of the reader's reused message body.
	const a1 = `    SELECT n FROM messages WHERE tenant_id = 'a1'`
	first := `SELECT n FROM messages WHERE tenant_id = 'a3';` + a1
	if got := diagnosticWireRows(t, session.query(t, first)); !equalStrings(got, []string{"3", "1"}) {
		t.Fatalf("split-query rows = %v, want [3 1]", got)
	}

	// Overwrite the old source span with a larger, distinct payload, then send
	// the exact cached key from offset zero. A borrowed compiled SQL string must
	// still execute the a1 plan and return row 1.
	churn := strings.Repeat(" ", 256) + `SELECT n FROM messages WHERE tenant_id = 'a3'`
	if got := diagnosticWireRows(t, session.query(t, churn)); !equalStrings(got, []string{"3"}) {
		t.Fatalf("churn rows = %v, want [3]", got)
	}
	if got := diagnosticWireRows(t, session.query(t, a1)); !equalStrings(got, []string{"1"}) {
		t.Fatalf("cached a1 rows = %v, want [1]", got)
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
