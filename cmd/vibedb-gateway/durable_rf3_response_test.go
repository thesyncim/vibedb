package main

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

// The SQL completion and ACK are distinct public envelopes: SQL uses Kind,
// while issuer/ACK control responses use OK and Op. Do not infer one from the
// other when validating the external process gates.
type durableRF3ExternalWireResponse struct {
	Kind             string `json:"kind"`
	OK               bool   `json:"ok"`
	Op               string `json:"op"`
	RowsAffected     int64  `json:"rows_affected"`
	ShardsFanned     int    `json:"shards_fanned"`
	Committed        bool   `json:"committed"`
	OutcomeUnknown   bool   `json:"outcome_unknown"`
	RequestID        string `json:"request_id"`
	RequestDigest    string `json:"request_digest"`
	InstallationID   string `json:"installation_id"`
	IssuerEpoch      uint64 `json:"issuer_epoch"`
	LaneOrdinal      uint16 `json:"lane_ordinal"`
	GrantDigest      string `json:"grant_digest"`
	IssuerSequence   uint64 `json:"issuer_sequence"`
	TerminalRevision uint64 `json:"terminal_revision"`
	ResultDigest     string `json:"result_digest"`
	AckToken         string `json:"ack_token"`
	Applied          uint64 `json:"applied"`
	CollectionRounds uint64 `json:"collection_rounds"`
	Error            string `json:"error"`
}

func durableRF3ExternalExecResponse(t testing.TB, raw []byte) durableRF3ExternalWireResponse {
	t.Helper()
	var response durableRF3ExternalWireResponse
	if err := vibejson.Unmarshal(raw, &response); err != nil {
		t.Fatalf("decode external exec response=%s err=%v", raw, err)
	}
	return response
}

func (response durableRF3ExternalWireResponse) committed(rows int64, shards int) bool {
	return response.Kind == "Completion" && response.Committed && !response.OutcomeUnknown && response.Error == "" &&
		response.RowsAffected == rows && response.ShardsFanned == shards
}

func durableRF3ExternalAssertCommitted(t testing.TB, raw []byte, rows int64, shards int) {
	t.Helper()
	if !durableRF3ExternalExecResponse(t, raw).committed(rows, shards) {
		t.Fatalf("multi-relation terminal=%s", raw)
	}
}

// Completed ACK retries echo the same capability and do no collection work.
// Applied is the current ReadIndex observation, not the tombstone's creation
// index: unrelated writes and election no-ops may advance it. Compare every
// other byte, and require this observation to be nonzero and nondecreasing.
func durableRF3ExternalSameCompletedAck(before, after []byte) bool {
	var previous, next durableRF3ExternalWireResponse
	if vibejson.Unmarshal(before, &previous) != nil || vibejson.Unmarshal(after, &next) != nil ||
		!previous.OK || previous.Op != "ack_exec_batch" || previous.Error != "" ||
		previous.Applied == 0 || next.Applied < previous.Applied ||
		previous.CollectionRounds != 0 || next.CollectionRounds != 0 {
		return false
	}
	oldField := fmt.Appendf(nil, `"applied":%d,`, previous.Applied)
	newField := fmt.Appendf(nil, `"applied":%d,`, next.Applied)
	return bytes.Count(before, oldField) == 1 && bytes.Count(after, newField) == 1 &&
		bytes.Equal(bytes.Replace(before, oldField, newField, 1), after)
}

func TestDurableRF3CompletedAckReplayAllowsOnlyAdvancingObservation(t *testing.T) {
	var request durableExecBatchAckWireRequest
	if err := decodeDurableExecBatchAckRequest([]byte(validDurableExecBatchAckFixture), &request); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	response := durableExecBatchAckWireResponse{durableExecBatchAckWireRequest: request, Applied: 17}
	if err := writeDurableExecBatchAckResponse(vibejson.NewWriter(&output), &response); err != nil {
		t.Fatal(err)
	}
	before := output.Bytes()
	for name, test := range map[string]struct {
		old, new string
		want     bool
	}{
		"unchanged":  {`"applied":17`, `"applied":17`, true},
		"election":   {`"applied":17`, `"applied":20`, true},
		"rollback":   {`"applied":17`, `"applied":16`, false},
		"zero":       {`"applied":17`, `"applied":0`, false},
		"collection": {`"collection_rounds":0`, `"collection_rounds":1`, false},
		"terminal":   {`"terminal_revision":11`, `"terminal_revision":12`, false},
		"sequence":   {`"issuer_sequence":9`, `"issuer_sequence":10`, false},
		"token":      {`41414141`, `42424242`, false},
		"digest":     {`31313131`, `32323232`, false},
		"error":      {`"ok":true`, `"ok":false`, false},
	} {
		t.Run(name, func(t *testing.T) {
			after := bytes.Replace(before, []byte(test.old), []byte(test.new), 1)
			if got := durableRF3ExternalSameCompletedAck(before, after); got != test.want {
				t.Fatalf("ACK comparison=%t want=%t response=%s", got, test.want, after)
			}
		})
	}
}

func TestDurableRF3ExternalResponseUsesPublicCompletionEnvelope(t *testing.T) {
	var output bytes.Buffer
	err := writeServeResponse(vibejson.NewWriter(&output), &serveResponse{
		Kind: "Completion", RowsAffected: 2, ShardsFanned: 2,
		TransactionID: replication.ID128{1}, Committed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	response := durableRF3ExternalExecResponse(t, output.Bytes())
	if response.OK || !response.committed(2, 2) {
		t.Fatalf("actual public completion rejected: %s", output.Bytes())
	}
	durableRF3ExternalAssertCommitted(t, output.Bytes(), 2, 2)
	for name, mutate := range map[string]func(*durableRF3ExternalWireResponse){
		"missing_kind":    func(value *durableRF3ExternalWireResponse) { value.Kind, value.OK = "", true },
		"wrong_kind":      func(value *durableRF3ExternalWireResponse) { value.Kind = "Rows" },
		"not_committed":   func(value *durableRF3ExternalWireResponse) { value.Committed = false },
		"outcome_unknown": func(value *durableRF3ExternalWireResponse) { value.OutcomeUnknown = true },
		"error":           func(value *durableRF3ExternalWireResponse) { value.Error = "refused" },
		"rows":            func(value *durableRF3ExternalWireResponse) { value.RowsAffected++ },
		"shards":          func(value *durableRF3ExternalWireResponse) { value.ShardsFanned++ },
	} {
		t.Run(name, func(t *testing.T) {
			altered := response
			mutate(&altered)
			raw, err := vibejson.Marshal(&altered)
			if err != nil {
				t.Fatal(err)
			}
			if durableRF3ExternalExecResponse(t, raw).committed(2, 2) {
				t.Fatalf("invalid completion accepted: %s", raw)
			}
		})
	}
}
