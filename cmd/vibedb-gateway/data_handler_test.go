package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

type nativeDataReaderStub struct {
	request gateway.ReplicatedTableReadRequest
	result  gateway.ReplicatedTableReadResult
	err     error
	calls   int
}

func (stub *nativeDataReaderStub) Read(
	_ context.Context,
	request gateway.ReplicatedTableReadRequest,
) (gateway.ReplicatedTableReadResult, error) {
	stub.calls++
	stub.request = request
	return stub.result, stub.err
}

func TestExecuteNativeDataReadPreservesByteNativePosition(t *testing.T) {
	request := nativeDataWireRequest{
		Operation: nativeDataOperationGet, Consistency: nativeDataAtLeastApplied,
		Table: []byte("accounts"), KeyBytes: 4, Applied: 41,
		RouteID: replication.Digest{1},
	}
	copy(request.Key[:], []byte{1, 2, 3, 4})
	reader := &nativeDataReaderStub{result: gateway.ReplicatedTableReadResult{
		Position: gateway.ReplicatedReadPosition{
			RouteID: replication.Digest{1}, Applied: 42,
		},
		Found: true, Value: []byte(`{"id":"a"}`), Retries: 2,
	}}
	response := executeNativeDataRead(context.Background(), reader, &request)
	if !response.OK || response.Applied != 42 || response.Position != request.RouteID ||
		!response.Found || !bytes.Equal(response.Document, reader.result.Value) ||
		response.Retries != 2 || reader.calls != 1 {
		t.Fatalf("response = %+v, calls=%d", response, reader.calls)
	}
	if reader.request.Consistency != gateway.ReplicatedDataReadAtLeastApplied ||
		reader.request.Position.RouteID != request.RouteID || reader.request.Position.Applied != 41 ||
		!bytes.Equal(reader.request.Table, request.Table) ||
		!bytes.Equal(reader.request.Key, request.OrderedKey()) {
		t.Fatalf("read = %+v", reader.request)
	}
}

func TestExecuteNativeDataReadRejectsWritesBeforeIO(t *testing.T) {
	reader := &nativeDataReaderStub{}
	request := nativeDataWireRequest{Operation: nativeDataOperationPut}
	response := executeNativeDataRead(context.Background(), reader, &request)
	if response.OK || response.Code != nativeDataResponseInvalidRequest || reader.calls != 0 {
		t.Fatalf("response = %+v calls=%d", response, reader.calls)
	}
}

func TestNativeDataResponseForErrorIsClosedAndTyped(t *testing.T) {
	tests := []struct {
		err       error
		code      nativeDataResponseCode
		retryable bool
	}{
		{gateway.ErrReplicatedDataRead, nativeDataResponseInvalidRequest, false},
		{gateway.ErrReplicatedTableRoute, nativeDataResponseTableNotReplicated, false},
		{gateway.ErrReplicatedReadPositionMismatch, nativeDataResponsePositionMismatch, false},
		{gateway.ErrReplicatedUnauthorized, nativeDataResponseUnauthorized, false},
		{gateway.ErrReplicatedReadBehind, nativeDataResponseReadBehind, true},
		{raftmodel.ErrAdmissionBound, nativeDataResponseOverloaded, true},
		{raftservice.ErrServingFence, nativeDataResponseStaleCatalog, true},
		{context.DeadlineExceeded, nativeDataResponseUnavailable, true},
		{&gateway.ReplicatedRefusalError{Code: shardservice.ReplicatedRefusalUnavailable}, nativeDataResponseUnavailable, true},
		{errors.New("unexpected"), nativeDataResponseInternal, false},
	}
	for _, test := range tests {
		response := nativeDataResponseForError(test.err)
		if response.OK || response.Code != test.code || response.Retryable != test.retryable {
			t.Fatalf("error %v response = %+v", test.err, response)
		}
	}
}
