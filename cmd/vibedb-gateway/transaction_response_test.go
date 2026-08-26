package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

func TestWriteServeResponseCarriesTypedRF3TransactionOutcome(t *testing.T) {
	var transactionID replication.ID128
	for index := range transactionID {
		transactionID[index] = byte(index + 1)
	}
	response := &serveResponse{
		Kind: "Completion", RowsAffected: 3, Route: "Targeted",
		Generation: 7, ShardsFanned: 2,
		TransactionID: transactionID, Committed: true,
	}
	var output bytes.Buffer
	writer := vibejson.NewWriter(&output)
	if err := writeServeResponse(writer, response); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, fragment := range []string{
		`"transaction_id":"0102030405060708090a0b0c0d0e0f10"`,
		`"committed":true`,
		`"rows_affected":3`,
	} {
		if !strings.Contains(got, fragment) {
			t.Fatalf("response %q does not contain %q", got, fragment)
		}
	}
	if strings.Contains(got, "outcome_unknown") {
		t.Fatalf("committed response = %q", got)
	}
}

func TestWriteServeResponseCarriesOutcomeUnknownWithoutClaimingCommit(t *testing.T) {
	transactionID := replication.ID128{0x41}
	response := &serveResponse{
		TransactionID: transactionID, OutcomeUnknown: true,
		Error: "gateway: replicated transaction outcome is unknown",
	}
	var output bytes.Buffer
	writer := vibejson.NewWriter(&output)
	if err := writeServeResponse(writer, response); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, fragment := range []string{
		`"transaction_id":"41000000000000000000000000000000"`,
		`"outcome_unknown":true`,
	} {
		if !strings.Contains(got, fragment) {
			t.Fatalf("response %q does not contain %q", got, fragment)
		}
	}
	if strings.Contains(got, `"committed":true`) {
		t.Fatalf("outcome-unknown response claims commit: %q", got)
	}
}

func TestEncodeResultProducesCanonicalCommittedTransactionState(t *testing.T) {
	transactionID := replication.ID128{0x52}
	response := encodeResult(&gateway.Result{
		Kind: shardservice.ResponseCompletion, RowsAffected: 1,
		RouteKind: distribution.RouteTargeted, Generation: 9, ShardsFanned: 1,
		TransactionID: transactionID,
	})
	if response.TransactionID != transactionID || !response.Committed ||
		response.OutcomeUnknown {
		t.Fatalf("encoded transaction response=%+v", response)
	}
}

func TestWriteServeResponseRejectsImpossibleTransactionStates(t *testing.T) {
	for _, response := range []*serveResponse{
		nil,
		{Committed: true},
		{OutcomeUnknown: true},
		{TransactionID: replication.ID128{1}},
		{TransactionID: replication.ID128{1}, Committed: true, OutcomeUnknown: true},
	} {
		var output bytes.Buffer
		err := writeServeResponse(vibejson.NewWriter(&output), response)
		if !errors.Is(err, errServeResponseTransactionState) || output.Len() != 0 {
			t.Fatalf("response=%+v output=%q err=%v", response, output.Bytes(), err)
		}
	}
}

func TestExecBatchRejectsMalformedRequestIdentityBeforeExecutor(t *testing.T) {
	response := execRequest(context.Background(), nil, serveRequest{
		Op: "exec_batch", RequestID: "01",
		Statements: []serveStatement{
			{SQL: `DELETE FROM a WHERE id = 1`},
			{SQL: `DELETE FROM b WHERE id = 2`},
		},
	})
	if response == nil || response.Error == "" || response.TransactionID != (replication.ID128{}) {
		t.Fatalf("response = %+v", response)
	}
}
