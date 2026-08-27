package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	vibejson "github.com/thesyncim/vibejson"
)

const maxGatewayBackupRequestBytes = 112

var (
	errInvalidGatewayBackupRequest = errors.New("vibedb-gateway: invalid backup request")
	gatewayBackupRequestFields     = vibejson.MakeFieldSet("op", "backup_id")
	gatewayBackupRequestDecoder    = func() vibejson.Decoder[gatewayBackupWireRequest] {
		decoder, err := vibejson.CompileDecoder[gatewayBackupWireRequest](vibejson.DecoderOptions{
			MaxDepth: 2, ZeroCopy: true, CaseSensitive: true, Replace: true,
		})
		if err != nil {
			panic(err)
		}
		return decoder
	}()
)

type gatewayBackupWireRequest struct{ request serveRequest }

type gatewayBackupOperator interface {
	Run(context.Context, [sha256.Size]byte) (gateway.ReplicatedOperationRecord, clusterbackup.Certificate, error)
	Status(context.Context, [sha256.Size]byte) (gateway.ReplicatedOperationRecord, error)
}

type gatewayBackupOperatorContextKey struct{}

func withGatewayBackupOperator(ctx context.Context, operator gatewayBackupOperator) context.Context {
	if ctx == nil || operator == nil {
		return ctx
	}
	return context.WithValue(ctx, gatewayBackupOperatorContextKey{}, operator)
}

func gatewayBackupOperatorFromContext(ctx context.Context) gatewayBackupOperator {
	operator, _ := ctx.Value(gatewayBackupOperatorContextKey{}).(gatewayBackupOperator)
	return operator
}

func validGatewayBackupRequest(request serveRequest) bool {
	return (request.Op == "backup" || request.Op == "backup_status") &&
		len(request.BackupID) == sha256.Size*2 && request.RequestID == "" &&
		request.InstallationID == "" && request.IssuerEpoch == 0 && request.LaneOrdinal == 0 &&
		request.GrantDigest == "" && request.IssuerSequence == 0 && request.IssuerLane == "" &&
		request.IssuerAuthenticator == "" && request.SQL == "" && request.Class == "" &&
		request.MaxResultBytes == 0 && len(request.Params) == 0 && len(request.Statements) == 0
}

func gatewayBackupRequestCandidate(raw []byte) bool {
	return exactOperationCandidate(raw, []byte(`"backup"`)) ||
		exactOperationCandidate(raw, []byte(`"backup_status"`))
}

func validateGatewayBackupEnvelope(raw []byte) error {
	if len(raw) == 0 || len(raw) > maxGatewayBackupRequestBytes || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return errInvalidGatewayBackupRequest
	}
	var request gatewayBackupWireRequest
	if gatewayBackupRequestDecoder.Decode(raw, &request) != nil || !validGatewayBackupRequest(request.request) {
		return errInvalidGatewayBackupRequest
	}
	return nil
}

func (wire *gatewayBackupWireRequest) UnmarshalVibeJSON(cursor vibejson.DecodeCursor) (vibejson.DecodeCursor, error) {
	wire.request = serveRequest{}
	if cursor.BeginObject("gateway backup") != nil || !cursor.Field(true, gatewayBackupRequestFields.Field(0)) {
		return cursor, errInvalidGatewayBackupRequest
	}
	var operation []byte
	var err error
	cursor, operation, err = durableExecBatchAckString(cursor)
	if err != nil || !(bytes.Equal(operation, []byte("backup")) ||
		bytes.Equal(operation, []byte("backup_status"))) ||
		!cursor.Field(false, gatewayBackupRequestFields.Field(1)) {
		return cursor, errInvalidGatewayBackupRequest
	}
	if bytes.Equal(operation, []byte("backup")) {
		wire.request.Op = "backup"
	} else {
		wire.request.Op = "backup_status"
	}
	var operationID [sha256.Size]byte
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, operationID[:]); err != nil ||
		operationID == ([sha256.Size]byte{}) || !cursor.ExpectObjectClose() {
		return cursor, errInvalidGatewayBackupRequest
	}
	wire.request.BackupID = hex.EncodeToString(operationID[:])
	return cursor, nil
}

func executeGatewayBackup(ctx context.Context, operator gatewayBackupOperator, request serveRequest) *serveResponse {
	if operator == nil || !validGatewayBackupRequest(request) {
		return &serveResponse{Error: errGatewayBackupOperator.Error()}
	}
	var operation [sha256.Size]byte
	if count, err := hex.Decode(operation[:], []byte(request.BackupID)); err != nil || count != len(operation) ||
		hex.EncodeToString(operation[:]) != request.BackupID || operation == ([sha256.Size]byte{}) {
		return &serveResponse{Error: errGatewayBackupOperator.Error()}
	}
	if request.Op == "backup_status" {
		record, err := operator.Status(ctx, operation)
		if err != nil {
			return &serveResponse{Error: err.Error()}
		}
		return backupServeResponse(operation, record)
	}
	record, certificate, err := operator.Run(ctx, operation)
	if err != nil {
		return &serveResponse{Error: err.Error()}
	}
	response := backupServeResponse(operation, record)
	response.BackupProof = certificate.Digest
	return response
}

func backupServeResponse(operation [sha256.Size]byte, record gateway.ReplicatedOperationRecord) *serveResponse {
	return &serveResponse{BackupID: operation, BackupStage: record.Cursor[0], BackupProof: record.Proof}
}
