package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

var errInvalidNativeSQLBatchResponse = errors.New(
	"vibedb-gateway: invalid replicated SQL batch response",
)

type nativeSQLBatchReader interface {
	ReadSQLBatch(
		context.Context,
		gateway.ReplicatedSQLBatchReadRequest,
	) (gateway.ReplicatedTableScatterReadResult, error)
}

// nativeSQLBatchWireResponse borrows one admitted gateway result until the
// complete response has been emitted. Expected binds positional response
// cardinality to the exact ingress statement count. Maximum is a full JSON
// response byte bound, not merely a raw-document payload hint.
type nativeSQLBatchWireResponse struct {
	Result   *gateway.ReplicatedTableScatterReadResult
	Expected int
	Maximum  uint32
	bytes    uint32
}

func buildNativeSQLBatchReadRequest(
	request serveRequest,
) (gateway.ReplicatedSQLBatchReadRequest, error) {
	if request.Op != "read_batch" || request.SQL != "" || len(request.Params) != 0 ||
		request.RequestID != "" || len(request.Statements) == 0 || request.MaxResultBytes == 0 {
		return gateway.ReplicatedSQLBatchReadRequest{}, errInvalidNativeDataRequest
	}
	class, err := parseClass(request.Class)
	if err != nil {
		return gateway.ReplicatedSQLBatchReadRequest{}, errInvalidNativeDataRequest
	}
	queries := make([]gateway.Query, len(request.Statements))
	for index := range request.Statements {
		params, paramErr := buildParams(request.Statements[index].Params)
		if paramErr != nil || request.Statements[index].SQL == "" {
			return gateway.ReplicatedSQLBatchReadRequest{}, errInvalidNativeDataRequest
		}
		queries[index] = gateway.Query{
			SQL: request.Statements[index].SQL, Params: params, Class: class,
		}
	}
	return gateway.ReplicatedSQLBatchReadRequest{
		Queries: queries, MaxResultBytes: request.MaxResultBytes,
	}, nil
}

func validateNativeSQLBatchResponse(response *nativeSQLBatchWireResponse) error {
	if response == nil || response.Result == nil || response.Expected <= 0 ||
		response.Maximum == 0 || response.Result.Count() != response.Expected ||
		len(response.Result.Packed) == 0 || len(response.Result.Observations) == 0 {
		return errInvalidNativeSQLBatchResponse
	}
	size := uint64(len(`{"ok":true,"found":[`))
	cursor := response.Result.Cursor()
	for index := 0; index < response.Expected; index++ {
		raw, found, ok := cursor.Next()
		if !ok || found && (len(raw) == 0 || !vibejson.Valid(raw)) || !found && len(raw) != 0 {
			return errInvalidNativeSQLBatchResponse
		}
		if index != 0 {
			size++
		}
		if found {
			size += uint64(len("true"))
		} else {
			size += uint64(len("false"))
		}
	}
	if _, _, ok := cursor.Next(); ok {
		return errInvalidNativeSQLBatchResponse
	}

	size += uint64(len(`],"documents":[`))
	cursor = response.Result.Cursor()
	for index := 0; index < response.Expected; index++ {
		raw, found, ok := cursor.Next()
		if !ok {
			return errInvalidNativeSQLBatchResponse
		}
		if index != 0 {
			size++
		}
		if found {
			size += uint64(len(raw))
		} else {
			size += uint64(len("null"))
		}
	}

	size += uint64(len(`],"observations":[`))
	var previous replication.Digest
	for index := range response.Result.Observations {
		observation := &response.Result.Observations[index]
		if observation.Group == (raftmember.GroupKey{}) ||
			observation.RouteID == (replication.Digest{}) || observation.Applied == 0 ||
			observation.Retries < 0 || index != 0 && bytes.Compare(previous[:], observation.RouteID[:]) >= 0 {
			return errInvalidNativeSQLBatchResponse
		}
		if index != 0 {
			size++
		}
		previous = observation.RouteID
		size += nativeSQLBatchObservationBytes(observation)
	}
	size += uint64(len("]}\n"))
	if size > uint64(response.Maximum) || size > uint64(^uint32(0)) {
		return gateway.ErrReplicatedReadAdmission
	}
	response.bytes = uint32(size)
	return nil
}

func nativeSQLBatchObservationBytes(observation *gateway.ReplicatedGroupReadObservation) uint64 {
	if observation == nil {
		return 0
	}
	// Every identity has fixed hexadecimal width. Only the two counters vary.
	size := uint64(len(`{"cluster_id":""`) + 32)
	size += uint64(len(`,"cluster_incarnation":""`) + 32)
	size += uint64(len(`,"topology_recovery_epoch":`)) + decimalUintBytes(observation.Group.TopologyRecoveryEpoch)
	size += uint64(len(`,"shard_incarnation":""`) + 32)
	size += uint64(len(`,"group_id":""`) + 32)
	size += uint64(len(`,"route_id":""`) + 64)
	size += uint64(len(`,"applied":`)) + decimalUintBytes(observation.Applied)
	if observation.Retries != 0 {
		size += uint64(len(`,"retries":`)) + decimalUintBytes(uint64(observation.Retries))
	}
	return size + 1 // object close
}

func decimalUintBytes(value uint64) uint64 {
	var scratch [20]byte
	return uint64(len(strconv.AppendUint(scratch[:0], value, 10)))
}

func writeNativeSQLBatchResponse(
	writer *vibejson.Writer,
	response *nativeSQLBatchWireResponse,
) error {
	if writer == nil || response == nil {
		return errInvalidNativeSQLBatchResponse
	}
	if response.bytes == 0 {
		if err := validateNativeSQLBatchResponse(response); err != nil {
			return err
		}
	}
	if err := writer.BeginObject(); err != nil {
		return err
	}
	if err := writer.Key("ok"); err != nil {
		return err
	}
	if err := writer.Bool(true); err != nil {
		return err
	}
	if err := writer.Key("found"); err != nil {
		return err
	}
	if err := writer.BeginArray(); err != nil {
		return err
	}
	cursor := response.Result.Cursor()
	for range response.Expected {
		_, found, _ := cursor.Next()
		if err := writer.Bool(found); err != nil {
			return err
		}
	}
	if err := writer.EndArray(); err != nil {
		return err
	}
	if err := writer.Key("documents"); err != nil {
		return err
	}
	if err := writer.BeginArray(); err != nil {
		return err
	}
	cursor = response.Result.Cursor()
	for range response.Expected {
		raw, found, _ := cursor.Next()
		if found {
			if err := writer.RawUnchecked(raw); err != nil {
				return err
			}
		} else if err := writer.Null(); err != nil {
			return err
		}
	}
	if err := writer.EndArray(); err != nil {
		return err
	}
	if err := writer.Key("observations"); err != nil {
		return err
	}
	if err := writer.BeginArray(); err != nil {
		return err
	}
	for index := range response.Result.Observations {
		if err := writeNativeSQLBatchObservation(writer, &response.Result.Observations[index]); err != nil {
			return err
		}
	}
	if err := writer.EndArray(); err != nil {
		return err
	}
	if err := writer.EndObject(); err != nil {
		return err
	}
	if err := writer.Newline(); err != nil {
		return err
	}
	return writer.Flush()
}

func writeNativeSQLBatchObservation(
	writer *vibejson.Writer,
	observation *gateway.ReplicatedGroupReadObservation,
) error {
	if err := writer.BeginObject(); err != nil {
		return err
	}
	identities := [...]struct {
		name  string
		value []byte
	}{
		{"cluster_id", observation.Group.ClusterID[:]},
		{"cluster_incarnation", observation.Group.ClusterIncarnation[:]},
		{"shard_incarnation", observation.Group.ShardIncarnation[:]},
		{"group_id", observation.Group.GroupID[:]},
		{"route_id", observation.RouteID[:]},
	}
	for index := range identities {
		if index == 2 {
			if err := writer.Key("topology_recovery_epoch"); err != nil {
				return err
			}
			if err := writer.Uint(observation.Group.TopologyRecoveryEpoch); err != nil {
				return err
			}
		}
		if err := writer.Key(identities[index].name); err != nil {
			return err
		}
		if err := writeNativeHex(writer, identities[index].value); err != nil {
			return err
		}
	}
	if err := writer.Key("applied"); err != nil {
		return err
	}
	if err := writer.Uint(observation.Applied); err != nil {
		return err
	}
	if observation.Retries != 0 {
		if err := writer.Key("retries"); err != nil {
			return err
		}
		if err := writer.Int(int64(observation.Retries)); err != nil {
			return err
		}
	}
	return writer.EndObject()
}

func writeNativeSQLBatchConnResponse(
	connection net.Conn,
	writer *vibejson.Writer,
	response *nativeSQLBatchWireResponse,
	timeout time.Duration,
) error {
	if connection == nil || writer == nil || timeout <= 0 {
		return errInvalidNativeSQLBatchResponse
	}
	if err := connection.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	writeErr := writeNativeSQLBatchResponse(writer, response)
	clearErr := connection.SetWriteDeadline(time.Time{})
	if writeErr != nil {
		return writeErr
	}
	return clearErr
}
