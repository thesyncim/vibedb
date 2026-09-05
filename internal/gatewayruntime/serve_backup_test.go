package gatewayruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

type testGatewayBackupOperator struct {
	record      gateway.ReplicatedOperationRecord
	certificate clusterbackup.Certificate
	runs        int
	statuses    int
}

func (operator *testGatewayBackupOperator) Run(_ context.Context, operation [sha256.Size]byte) (
	gateway.ReplicatedOperationRecord, clusterbackup.Certificate, error,
) {
	operator.runs++
	operator.record.ID = operation
	return operator.record, operator.certificate, nil
}

func (operator *testGatewayBackupOperator) Status(_ context.Context, operation [sha256.Size]byte) (
	gateway.ReplicatedOperationRecord, error,
) {
	operator.statuses++
	operator.record.ID = operation
	return operator.record, nil
}

func TestGatewayBackupRequestUsesDedicatedAuthorityAndCanonicalResponse(t *testing.T) {
	operation := sha256.Sum256([]byte("backup operation"))
	certificateDigest := sha256.Sum256([]byte("certificate"))
	operator := &testGatewayBackupOperator{record: gateway.ReplicatedOperationRecord{
		Cursor: [8]uint64{3}, Proof: sha256.Sum256([]byte("export")),
	}, certificate: clusterbackup.Certificate{Digest: certificateDigest}}
	request := serveRequest{Op: "backup", BackupID: hex.EncodeToString(operation[:])}
	if capability := serveRequestCapability(&request); capability != serviceauthz.CapabilityBackup {
		t.Fatalf("capability=%v", capability)
	}
	response := executeGatewayBackup(context.Background(), operator, request)
	if response.Error != "" || response.BackupStage != 3 || operator.runs != 1 {
		t.Fatalf("response=%+v runs=%d", response, operator.runs)
	}
	var output bytes.Buffer
	if err := writeServeResponse(vibejson.NewWriter(&output), response); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`"backup_id":"` + hex.EncodeToString(operation[:]) + `"`,
		`"backup_stage":3`,
		`"backup_proof":"` + hex.EncodeToString(certificateDigest[:]) + `"`,
	} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("response %q lacks %q", output.String(), expected)
		}
	}
}

func TestGatewayBackupStatusAndRequestGrammarAreBounded(t *testing.T) {
	operation := sha256.Sum256([]byte("status operation"))
	operator := &testGatewayBackupOperator{record: gateway.ReplicatedOperationRecord{Cursor: [8]uint64{2}}}
	request := serveRequest{Op: "backup_status", BackupID: hex.EncodeToString(operation[:])}
	response := executeGatewayBackup(context.Background(), operator, request)
	if response.Error != "" || operator.statuses != 1 || operator.runs != 0 {
		t.Fatalf("response=%+v status=%d runs=%d", response, operator.statuses, operator.runs)
	}
	invalid := []serveRequest{
		{Op: "backup", BackupID: strings.ToUpper(request.BackupID)},
		{Op: "backup", BackupID: request.BackupID, SQL: "SELECT 1"},
		{Op: "backup", BackupID: request.BackupID, MaxResultBytes: 1},
		{Op: "backup", BackupID: request.BackupID, Params: []serveParam{{Kind: "null"}}},
		{Op: "query", BackupID: request.BackupID},
	}
	for index := range invalid {
		if got := executeGatewayBackup(context.Background(), operator, invalid[index]); got.Error != errGatewayBackupOperator.Error() {
			t.Fatalf("invalid %d response=%+v", index, got)
		}
	}
}

func TestGatewayBackupRawEnvelopeIsExactAndCanonical(t *testing.T) {
	operation := sha256.Sum256([]byte("raw operation"))
	id := hex.EncodeToString(operation[:])
	valid := []byte(`{"op":"backup","backup_id":"` + id + `"}`)
	if !gatewayBackupRequestCandidate(valid) || validateGatewayBackupEnvelope(valid) != nil {
		t.Fatal("canonical envelope rejected")
	}
	invalid := [][]byte{
		[]byte(`{"backup_id":"` + id + `","op":"backup"}`),
		[]byte(`{"op":"backup","backup_id":"` + id + `","max_result_bytes":0}`),
		[]byte(`{"op":"backup","backup_id":"` + id + `","sql":""}`),
		[]byte(`{"op":"backup","backup_id":"` + id + `"} `),
		[]byte(`{"op":"backup","backup_id":"` + strings.ToUpper(id) + `"}`),
	}
	for index, raw := range invalid {
		if validateGatewayBackupEnvelope(raw) == nil {
			t.Fatalf("invalid %d accepted: %s", index, raw)
		}
	}
}

func TestGatewayBackupOperatorContextRejectsNil(t *testing.T) {
	ctx := context.Background()
	if got := gatewayBackupOperatorFromContext(withGatewayBackupOperator(ctx, nil)); got != nil {
		t.Fatalf("operator=%T", got)
	}
	operator := &testGatewayBackupOperator{}
	if got := gatewayBackupOperatorFromContext(withGatewayBackupOperator(ctx, operator)); got != operator {
		t.Fatalf("operator=%T", got)
	}
}
