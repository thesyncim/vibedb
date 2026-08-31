package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/shardservice"
)

func TestShardClientPreservesRuntimeSQLDiagnosticsAndRecovers(t *testing.T) {
	client := pipeClient(newShardServer(t))
	ctx := context.Background()
	for _, setup := range []struct {
		sql    string
		params []shardservice.Param
	}{
		{sql: `CREATE TABLE docs (id STRING PRIMARY KEY, n INTEGER)`},
		{sql: `INSERT INTO docs (id, n) VALUES (?, ?)`, params: []shardservice.Param{
			shardservice.StringParam("a1"), shardservice.NumberParam("1"),
		}},
	} {
		if _, err := client.Do(ctx, "shard-a", ownedReq(setup.sql, setup.params...)); err != nil {
			t.Fatalf("setup %q: %v", setup.sql, err)
		}
	}

	tests := []struct {
		name       string
		sql        string
		state      string
		position   int
		redactText string
	}{
		{
			name: "division_by_zero", sql: `SELECT n / 0 FROM docs WHERE id = 'a1'`,
			state: "22012", position: 9,
		},
		{
			name: "numeric_range", sql: `SELECT CAST(CAST(n AS TEXT) || 'e999999999999999999999' AS NUMERIC) FROM docs WHERE id = 'a1'`,
			state: "22003", position: 7,
		},
		{
			name: "invalid_typed_input", sql: `SELECT CAST(id AS BOOLEAN) FROM docs WHERE id = 'a1'`,
			state: "22P02", position: 7, redactText: "a1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := ownedReq(test.sql)
			request.ExecutionMode = shardservice.ExecutionReadOnly
			_, err := client.Do(ctx, "shard-a", request)
			var shardErr *ShardError
			if !errors.As(err, &shardErr) || !errors.Is(err, ErrMalformedRequest) {
				t.Fatalf("error = %T %v, want diagnostic *ShardError preserving ErrMalformedRequest", err, err)
			}
			if shardErr.SQLState() != test.state {
				t.Fatalf("SQLState = %q, want %q", shardErr.SQLState(), test.state)
			}
			if position, ok := shardErr.SQLPosition(); !ok || position != test.position {
				t.Fatalf("SQLPosition = (%d, %v), want (%d, true)", position, ok, test.position)
			}
			if shardErr.Message == "" || strings.HasPrefix(shardErr.Message, "VDBSQL:") {
				t.Fatalf("gateway exposed envelope instead of message: %q", shardErr.Message)
			}
			if test.redactText != "" && strings.Contains(shardErr.Message, test.redactText) {
				t.Fatalf("invalid input leaked in diagnostic: %q", shardErr.Message)
			}

			recovery := ownedReq(`SELECT n FROM docs WHERE id = 'a1'`)
			recovery.ExecutionMode = shardservice.ExecutionReadOnly
			response, recoveryErr := client.Do(ctx, "shard-a", recovery)
			if recoveryErr != nil || len(response.Rows) != 1 || string(response.Rows[0][0].Bytes) != "1" {
				t.Fatalf("same client did not recover: response=%+v error=%v", response, recoveryErr)
			}
		})
	}
}

func TestLegacyMalformedShardErrorKeepsPriorBehavior(t *testing.T) {
	const message = "old shard refused this request"
	err := shardError(shardservice.NewErrorResponse(shardservice.ErrorMalformedRequest, message))
	if err.Error() != message || err.SQLState() != "" || err.SQLHint() != "" {
		t.Fatalf("legacy ShardError = %+v", err)
	}
	if position, ok := err.SQLPosition(); ok || position != 0 {
		t.Fatalf("legacy SQLPosition = (%d, %v), want absent", position, ok)
	}
	if !errors.Is(err, ErrMalformedRequest) {
		t.Fatal("legacy malformed sentinel was lost")
	}

	reservedGarbage := shardError(shardservice.NewErrorResponse(
		shardservice.ErrorMalformedRequest, "VDBSQL:not-an-envelope",
	))
	if reservedGarbage.SQLState() != "" {
		t.Fatalf("malformed reserved text became SQL error: %+v", reservedGarbage)
	}
}
