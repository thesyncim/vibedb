package gatewayruntime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
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
		{errors.Join(raftservice.ErrServingFence, gateway.ErrReplicatedUnauthorized), nativeDataResponseStaleCatalog, true},
		{errors.Join(raftservice.ErrServingFence, gateway.ErrReplicatedDataRead), nativeDataResponseStaleCatalog, true},
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

func TestHandleConnDataDispatchesRF3ReadWithoutSQLFallback(t *testing.T) {
	reader := &nativeDataReaderStub{result: gateway.ReplicatedTableReadResult{
		Position: gateway.ReplicatedReadPosition{
			RouteID: replication.Digest{1}, Applied: 42,
		},
		Found: true, Value: []byte(`{"id":"a"}`),
	}}
	response := roundTripNativeDataLine(t, reader, nil,
		`{"op":"get","table":"docs","key":"AQIDBA","consistency":"linearizable"}`)
	want := `{"ok":true,"route_id":"0100000000000000000000000000000000000000000000000000000000000000","applied":42,"found":true,"document":{"id":"a"}}` + "\n"
	if response != want || reader.calls != 1 {
		t.Fatalf("response = %q, calls=%d", response, reader.calls)
	}
}

func TestHandleConnDataReturnsClosedTypedFailures(t *testing.T) {
	tests := []struct {
		name      string
		reader    nativeDataReader
		authorize func(serviceauthz.Capability) bool
		line      string
		want      string
	}{
		{
			name:   "malformed",
			reader: &nativeDataReaderStub{},
			line:   `{"op":"get","table":"docs","key":"padded==","consistency":"linearizable"}`,
			want:   `{"ok":false,"code":"invalid_request","retryable":false}` + "\n",
		},
		{
			name:   "recognized operation cannot fall through",
			reader: &nativeDataReaderStub{},
			line:   `{"op":"get"}`,
			want:   `{"ok":false,"code":"invalid_request","retryable":false}` + "\n",
		},
		{
			name:   "escaped operation cannot fall through",
			reader: &nativeDataReaderStub{},
			line:   `{"o\u0070":"get","table":"docs"}`,
			want:   `{"ok":false,"code":"invalid_request","retryable":false}` + "\n",
		},
		{
			name:   "reordered native operation cannot fall through",
			reader: &nativeDataReaderStub{},
			line:   `{"table":"docs","op":"delete"}`,
			want:   `{"ok":false,"code":"invalid_request","retryable":false}` + "\n",
		},
		{
			name:   "authorization",
			reader: &nativeDataReaderStub{},
			authorize: func(serviceauthz.Capability) bool {
				return false
			},
			line: `{"op":"get","table":"docs","key":"AQIDBA","consistency":"linearizable"}`,
			want: `{"ok":false,"code":"unauthorized","retryable":false}` + "\n",
		},
		{
			name: "rf3 disabled",
			line: `{"op":"get","table":"docs","key":"AQIDBA","consistency":"linearizable"}`,
			want: `{"ok":false,"code":"unavailable","retryable":true}` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := roundTripNativeDataLine(t, test.reader, test.authorize, test.line)
			if got != test.want {
				t.Fatalf("response = %q, want %q", got, test.want)
			}
		})
	}
}

func TestNativeDataRequestCandidateRequiresFirstUnescapedOperation(t *testing.T) {
	accepted := []string{
		`{"op":"get","table":"docs"}`,
		" \t{ \n\"op\" : \"put\" ,\"table\":\"docs\"}",
		`{"op":"delete" ,"table":"docs"}`,
		`{"op":"get"}`,
		`{"op":"put" garbage`,
	}
	for _, source := range accepted {
		if !nativeDataRequestCandidate([]byte(source)) {
			t.Fatalf("candidate rejected %q", source)
		}
	}
	rejected := []string{
		``, `null`, `{}`, `{"table":"docs","op":"get"}`,
		`{"o\u0070":"get","table":"docs"}`,
		`{"op":"query","sql":"SELECT 1"}`,
		`{"op":"getter","table":"docs"}`,
	}
	for _, source := range rejected {
		if nativeDataRequestCandidate([]byte(source)) {
			t.Fatalf("candidate accepted %q", source)
		}
	}
}

func roundTripNativeDataLine(
	t testing.TB,
	reader nativeDataReader,
	authorize func(serviceauthz.Capability) bool,
	line string,
) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnPolicy(ctx, server, nil, reader, func(string, ...any) {}, authorize)
	}()
	if deadlineErr := client.SetDeadline(time.Now().Add(5 * time.Second)); deadlineErr != nil {
		t.Fatal(deadlineErr)
	}
	if _, err := client.Write(append([]byte(line), '\n')); err != nil {
		t.Fatal(err)
	}
	response, err := bufio.NewReader(client).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("connection handler did not stop")
	}
	return response
}
