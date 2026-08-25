// Package shardcontrol defines the authenticated, bounded control protocol
// used by the distributed split controller. It carries fixed binary identity
// and canonical vibejson payload bytes; remote diagnostic strings are absent.
package shardcontrol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	vibejson "github.com/thesyncim/vibejson"
)

var ErrWire = errors.New("shardcontrol: invalid control frame")

const (
	wireFormat             = 1
	requestTag             = byte('C')
	responseTag            = byte('R')
	MaxPayloadBytes        = 1 << 20
	requestFixedBodyBytes  = 1 + 1 + 1 + 1 + 32 + 32 + 32 + 8*8 + 4
	responseFixedBodyBytes = 1 + 1 + 2 + 32 + 32 + 32 + 4
	maxFrameBodyBytes      = requestFixedBodyBytes + MaxPayloadBytes
)

// Action is the closed split step set. Values intentionally equal the current
// splitcontroller ActionKind values, but the wire package remains independent.
type Action uint8

const (
	ActionAwaitSourceLeader Action = iota + 1
	ActionStartCapture
	ActionBuildArtifacts
	ActionStageChild
	ActionCatchUpTail
	ActionSealSource
	ActionCertifyCutover
	ActionActivateChild
	ActionCreateChildWAL
	ActionAdoptChildRuntime
	ActionAwaitChildReady
	ActionPublishCatalog
	ActionAwaitCatalogDrain
	ActionPruneRetained
	ActionComplete
	// ActionReconcileSplit asks the source controller host to reconstruct the
	// replicated plan and execute exactly one durable next step. It carries no
	// guessed data-plane fence; the host obtains and validates the coherent cut
	// before issuing an ordinary fenced per-action RPC.
	ActionReconcileSplit
)

// ResultCode is a closed result class. Accepted means the step's durable
// idempotency witness exists before response. Retry never proves execution.
type ResultCode uint8

const (
	ResultAccepted ResultCode = iota + 1
	ResultRetry
	ResultStaleFence
	ResultConflict
	ResultBound
	ResultUnauthorized
)

// Fence binds the request to exact catalog and replicated-state authorities.
// Zero in any field is invalid; controllers cannot submit an unfenced action.
type Fence struct {
	CatalogGeneration uint64
	Allocation        uint64
	OwnershipEpoch    uint64
	SchemaGeneration  uint64
	RoutingVersion    uint64
	RouteGeneration   uint64
	ReplicaSetVersion uint64
	Applied           uint64
}

// Request carries one exact operation and step identity. PlanDigest binds the
// complete persisted split intent; Payload is canonical and capacity-clamped.
type Request struct {
	Action     Action
	Child      uint8
	Operation  [32]byte
	Step       [32]byte
	PlanDigest [32]byte
	Fence      Fence
	Payload    []byte
}

// Response repeats request identity so a late response cannot settle another
// step. ResultDigest binds the durable result witness and optional payload.
type Response struct {
	Code         ResultCode
	Operation    [32]byte
	Step         [32]byte
	ResultDigest [32]byte
	Payload      []byte
}

func validAction(action Action) bool {
	return action >= ActionAwaitSourceLeader && action <= ActionReconcileSplit
}

func validResult(code ResultCode) bool {
	return code >= ResultAccepted && code <= ResultUnauthorized
}

func validFence(fence Fence) bool {
	return fence.CatalogGeneration != 0 && fence.Allocation != 0 &&
		fence.OwnershipEpoch != 0 && fence.SchemaGeneration != 0 &&
		fence.RoutingVersion != 0 && fence.RouteGeneration != 0 &&
		fence.ReplicaSetVersion != 0 && fence.Applied != 0
}

func canonicalPayload(payload []byte, allowEmpty bool) bool {
	if len(payload) == 0 {
		return allowEmpty
	}
	if len(payload) > MaxPayloadBytes || !vibejson.Valid(payload) {
		return false
	}
	canonical, err := vibejson.AppendCanonicalize(nil, payload)
	return err == nil && bytes.Equal(canonical, payload)
}

func validRequest(request *Request) bool {
	if request == nil || !validAction(request.Action) || request.Operation == ([32]byte{}) ||
		request.Step == ([32]byte{}) || request.PlanDigest == ([32]byte{}) ||
		!canonicalPayload(request.Payload, false) {
		return false
	}
	if request.Action == ActionReconcileSplit {
		return request.Child == 0 && request.Fence == (Fence{})
	}
	return validFence(request.Fence)
}

func validResponse(response *Response) bool {
	return response != nil && validResult(response.Code) &&
		response.Operation != ([32]byte{}) && response.Step != ([32]byte{}) &&
		response.ResultDigest != ([32]byte{}) && canonicalPayload(response.Payload, true)
}

func AppendRequest(dst []byte, request *Request) ([]byte, error) {
	if !validRequest(request) {
		return dst, ErrWire
	}
	start := len(dst)
	dst = append(dst, make([]byte, 5+requestFixedBodyBytes+len(request.Payload))...)
	dst[start] = requestTag
	binary.LittleEndian.PutUint32(dst[start+1:start+5], uint32(requestFixedBodyBytes+len(request.Payload)))
	body := dst[start+5:]
	body[0], body[1], body[2] = wireFormat, byte(request.Action), request.Child
	copy(body[4:36], request.Operation[:])
	copy(body[36:68], request.Step[:])
	copy(body[68:100], request.PlanDigest[:])
	putFence(body[100:164], request.Fence)
	binary.LittleEndian.PutUint32(body[164:168], uint32(len(request.Payload)))
	copy(body[168:], request.Payload)
	return dst, nil
}

func OpenRequest(frame []byte) (Request, error) {
	body, err := openFrame(frame, requestTag, requestFixedBodyBytes)
	if err != nil || len(body) < requestFixedBodyBytes || body[0] != wireFormat || body[3] != 0 {
		return Request{}, ErrWire
	}
	payloadBytes := int(binary.LittleEndian.Uint32(body[164:168]))
	if payloadBytes == 0 || payloadBytes > MaxPayloadBytes || len(body) != requestFixedBodyBytes+payloadBytes {
		return Request{}, ErrWire
	}
	request := Request{Action: Action(body[1]), Child: body[2], Fence: openFence(body[100:164])}
	copy(request.Operation[:], body[4:36])
	copy(request.Step[:], body[36:68])
	copy(request.PlanDigest[:], body[68:100])
	request.Payload = body[168:len(body):len(body)]
	if !validRequest(&request) {
		return Request{}, ErrWire
	}
	return request, nil
}

func AppendResponse(dst []byte, response *Response) ([]byte, error) {
	if !validResponse(response) {
		return dst, ErrWire
	}
	start := len(dst)
	dst = append(dst, make([]byte, 5+responseFixedBodyBytes+len(response.Payload))...)
	dst[start] = responseTag
	binary.LittleEndian.PutUint32(dst[start+1:start+5], uint32(responseFixedBodyBytes+len(response.Payload)))
	body := dst[start+5:]
	body[0], body[1] = wireFormat, byte(response.Code)
	copy(body[4:36], response.Operation[:])
	copy(body[36:68], response.Step[:])
	copy(body[68:100], response.ResultDigest[:])
	binary.LittleEndian.PutUint32(body[100:104], uint32(len(response.Payload)))
	copy(body[104:], response.Payload)
	return dst, nil
}

func OpenResponse(frame []byte) (Response, error) {
	body, err := openFrame(frame, responseTag, responseFixedBodyBytes)
	if err != nil || len(body) < responseFixedBodyBytes || body[0] != wireFormat || body[2] != 0 || body[3] != 0 {
		return Response{}, ErrWire
	}
	payloadBytes := int(binary.LittleEndian.Uint32(body[100:104]))
	if payloadBytes > MaxPayloadBytes || len(body) != responseFixedBodyBytes+payloadBytes {
		return Response{}, ErrWire
	}
	response := Response{Code: ResultCode(body[1])}
	copy(response.Operation[:], body[4:36])
	copy(response.Step[:], body[36:68])
	copy(response.ResultDigest[:], body[68:100])
	response.Payload = body[104:len(body):len(body)]
	if !validResponse(&response) {
		return Response{}, ErrWire
	}
	return response, nil
}

func WriteRequest(writer io.Writer, request *Request) error {
	frame, err := AppendRequest(nil, request)
	if err != nil {
		return err
	}
	return writeFull(writer, frame)
}

func ReadRequest(reader io.Reader) (Request, error) {
	frame, err := readFrame(reader, requestTag, requestFixedBodyBytes+MaxPayloadBytes)
	if err != nil {
		return Request{}, err
	}
	return OpenRequest(frame)
}

func WriteResponse(writer io.Writer, response *Response) error {
	frame, err := AppendResponse(nil, response)
	if err != nil {
		return err
	}
	return writeFull(writer, frame)
}

func ReadResponse(reader io.Reader) (Response, error) {
	frame, err := readFrame(reader, responseTag, responseFixedBodyBytes+MaxPayloadBytes)
	if err != nil {
		return Response{}, err
	}
	return OpenResponse(frame)
}

func openFrame(frame []byte, tag byte, minimum int) ([]byte, error) {
	if len(frame) < 5 || frame[0] != tag {
		return nil, ErrWire
	}
	length := uint64(binary.LittleEndian.Uint32(frame[1:5]))
	if length < uint64(minimum) || length > uint64(maxFrameBodyBytes) || length != uint64(len(frame)-5) {
		return nil, ErrWire
	}
	return frame[5:len(frame):len(frame)], nil
}

func readFrame(reader io.Reader, tag byte, maximum int) ([]byte, error) {
	if reader == nil {
		return nil, ErrWire
	}
	var header [5]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil || header[0] != tag {
		return nil, errors.Join(ErrWire, err)
	}
	length := uint64(binary.LittleEndian.Uint32(header[1:]))
	if length == 0 || length > uint64(maximum) {
		return nil, ErrWire
	}
	frame := make([]byte, 5+int(length))
	copy(frame, header[:])
	if _, err := io.ReadFull(reader, frame[5:]); err != nil {
		return nil, errors.Join(ErrWire, err)
	}
	return frame, nil
}

func writeFull(writer io.Writer, frame []byte) error {
	if writer == nil {
		return ErrWire
	}
	for len(frame) != 0 {
		written, err := writer.Write(frame)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(frame) {
			return ErrWire
		}
		frame = frame[written:]
	}
	return nil
}

func putFence(dst []byte, fence Fence) {
	values := [...]uint64{fence.CatalogGeneration, fence.Allocation, fence.OwnershipEpoch,
		fence.SchemaGeneration, fence.RoutingVersion, fence.RouteGeneration,
		fence.ReplicaSetVersion, fence.Applied}
	for index, value := range values {
		binary.LittleEndian.PutUint64(dst[index*8:], value)
	}
}

func openFence(src []byte) Fence {
	return Fence{
		CatalogGeneration: binary.LittleEndian.Uint64(src[0:8]),
		Allocation:        binary.LittleEndian.Uint64(src[8:16]),
		OwnershipEpoch:    binary.LittleEndian.Uint64(src[16:24]),
		SchemaGeneration:  binary.LittleEndian.Uint64(src[24:32]),
		RoutingVersion:    binary.LittleEndian.Uint64(src[32:40]),
		RouteGeneration:   binary.LittleEndian.Uint64(src[40:48]),
		ReplicaSetVersion: binary.LittleEndian.Uint64(src[48:56]),
		Applied:           binary.LittleEndian.Uint64(src[56:64]),
	}
}
