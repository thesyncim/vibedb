package gatewayruntime

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

// Keep the process fixture's wire encoder and regression host-runnable.
// serveRequest's legacy top-level SQL field must not enter this strict grammar.
type hotMutationExecEnvelope struct {
	Op             string           `json:"op"`
	RequestID      string           `json:"request_id"`
	InstallationID string           `json:"installation_id"`
	IssuerEpoch    uint64           `json:"issuer_epoch"`
	LaneOrdinal    uint16           `json:"lane_ordinal"`
	GrantDigest    string           `json:"grant_digest"`
	IssuerSequence uint64           `json:"issuer_sequence"`
	Class          string           `json:"class"`
	Statements     []serveStatement `json:"statements"`
}

func hotMutationRequest(t *testing.T, reference gateway.ReplicatedIssuerReference,
	sequence uint64, statements []serveStatement,
) []byte {
	return hotMutationRequestClass(t, reference, sequence, statements, "interactive")
}

func hotMutationRequestClass(t *testing.T, reference gateway.ReplicatedIssuerReference,
	sequence uint64, statements []serveStatement, class string,
) []byte {
	t.Helper()
	var requestID replication.ID128
	binary.LittleEndian.PutUint64(requestID[:8], sequence)
	requestID[15] = 0xa5
	raw, err := vibejson.Marshal(&hotMutationExecEnvelope{Op: "exec_batch", RequestID: hex.EncodeToString(requestID[:]),
		InstallationID: hex.EncodeToString(reference.Installation[:]), IssuerEpoch: reference.Epoch,
		LaneOrdinal: reference.LaneOrdinal, GrantDigest: hex.EncodeToString(reference.GrantDigest[:]),
		IssuerSequence: sequence, Class: class, Statements: statements})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestHotMutationRequestUsesStrictExecBatchGrammar(t *testing.T) {
	for _, ordinal := range []uint16{0, gateway.MaxReplicatedIssuerLanes - 1} {
		t.Run(fmt.Sprint(ordinal), func(t *testing.T) {
			reference := gateway.ReplicatedIssuerReference{Installation: replication.ID128{1},
				Epoch: 1, LaneOrdinal: ordinal, GrantDigest: replication.Digest{2}}
			raw := hotMutationRequest(t, reference, 1, []serveStatement{{
				SQL:    `DELETE FROM messages WHERE id = ?`,
				Params: []serveParam{{Kind: "string", Text: "m-0"}},
			}})
			if err := validateDurableExecBatchEnvelope(raw); err != nil ||
				strings.Contains(string(raw), `"sql":""`) {
				t.Fatalf("strict mutation request=%s err=%v", raw, err)
			}
			var decoded serveRequest
			var scratch serveRequestDecodeScratch
			if err := decodeDurableExecBatchRequest(raw, &decoded, &scratch); err != nil ||
				decoded.wireIdentity.Reference != reference || len(decoded.Statements) != 1 ||
				len(decoded.Statements[0].Params) != 1 || decoded.Statements[0].Params[0].textValue() != "m-0" {
				t.Fatalf("mutation identity/parameter changed: %+v err=%v", decoded, err)
			}
			ack := strings.Replace(validDurableExecBatchAckFixture, `"lane_ordinal":3`,
				fmt.Sprintf(`"lane_ordinal":%d`, ordinal), 1)
			var decodedAck durableExecBatchAckWireRequest
			if err := decodeDurableExecBatchAckRequest([]byte(ack), &decodedAck); err != nil ||
				decodedAck.Identity.Reference.LaneOrdinal != ordinal {
				t.Fatalf("ACK lane=%d err=%v", decodedAck.Identity.Reference.LaneOrdinal, err)
			}
		})
	}
}

func TestHotMutationSeedClassPreservesIdentityAndMeasuredDefault(t *testing.T) {
	reference := gateway.ReplicatedIssuerReference{Installation: replication.ID128{1},
		Epoch: 1, LaneOrdinal: 0, GrantDigest: replication.Digest{2}}
	statements := []serveStatement{{SQL: "DELETE FROM orders_a WHERE id='seed-a'"}}
	var identity durableExecBatchIdentity
	for _, class := range []string{"interactive", "batch"} {
		raw := hotMutationRequest(t, reference, 1, statements)
		if class == "batch" {
			raw = hotMutationRequestClass(t, reference, 1, statements, class)
		}
		if err := validateDurableExecBatchEnvelope(raw); err != nil {
			t.Fatal(err)
		}
		var request serveRequest
		var scratch serveRequestDecodeScratch
		if err := decodeDurableExecBatchRequest(raw, &request, &scratch); err != nil {
			t.Fatal(err)
		}
		if request.Class != class || len(request.Statements) != 1 || string(request.Statements[0].wireSQL) != statements[0].SQL {
			t.Fatalf("class/statement changed: %+v", request)
		}
		if class == "interactive" {
			identity = request.wireIdentity
		} else if identity != request.wireIdentity {
			t.Fatal("setup class changed the durable request identity")
		}
	}
}

func TestHotMutationZeroLaneRetainsRequiredIdentityChecks(t *testing.T) {
	reference := gateway.ReplicatedIssuerReference{Installation: replication.ID128{1},
		Epoch: 1, GrantDigest: replication.Digest{2}}
	exec := string(hotMutationRequest(t, reference, 1,
		[]serveStatement{{SQL: `DELETE FROM messages WHERE id = 'm-0'`}}))
	ack := strings.Replace(validDurableExecBatchAckFixture, `"lane_ordinal":3`, `"lane_ordinal":0`, 1)
	for _, source := range []struct {
		name, raw string
		decode    func([]byte) error
	}{
		{"exec", exec, validateDurableExecBatchEnvelope},
		{"ack", ack, func(raw []byte) error {
			var decoded durableExecBatchAckWireRequest
			return decodeDurableExecBatchAckRequest(raw, &decoded)
		}},
	} {
		invalid := map[string]string{
			"negative lane":   `"lane_ordinal":-1`,
			"outside lane":    fmt.Sprintf(`"lane_ordinal":%d`, gateway.MaxReplicatedIssuerLanes),
			"overflow lane":   `"lane_ordinal":18446744073709551616`,
			"fractional lane": `"lane_ordinal":0.5`,
			"string lane":     `"lane_ordinal":"0"`,
		}
		for name, replacement := range invalid {
			t.Run(source.name+"/"+name, func(t *testing.T) {
				raw := strings.Replace(source.raw, `"lane_ordinal":0`, replacement, 1)
				if err := source.decode([]byte(raw)); err == nil {
					t.Fatalf("accepted invalid lane: %s", raw)
				}
			})
		}
		for _, field := range []string{"request_id", "installation_id", "grant_digest", "issuer_epoch", "issuer_sequence"} {
			t.Run(source.name+"/zero "+field, func(t *testing.T) {
				start := strings.Index(source.raw, `"`+field+`":`) + len(field) + 3
				end := start + strings.IndexByte(source.raw[start:], ',')
				value := "0"
				if source.raw[start] == '"' {
					value = `"` + strings.Repeat("0", end-start-2) + `"`
				}
				raw := source.raw[:start] + value + source.raw[end:]
				if err := source.decode([]byte(raw)); err == nil {
					t.Fatalf("accepted missing identity: %s", raw)
				}
			})
		}
	}
}
