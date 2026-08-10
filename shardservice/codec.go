package shardservice

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
)

// The shard-service codec: a stdlib-only, big-endian length-prefixed framing
// mirroring pgwire/proto.go, with every decode bounded so a malformed or
// oversized frame is rejected rather than turned into an allocation a remote
// peer chose the size of.
//
// A frame is one tag byte followed by an int32 length that, per the pgwire
// convention, covers itself but not the tag, and then the body. The body opens
// with a one-byte wire version. Encoding is deterministic: the same value
// always produces the same bytes, which is what makes the golden vectors a
// stable compatibility artifact.

// Frame tags. A request and a response travel in opposite directions, so a
// single tag value is unambiguous within its direction.
const (
	tagRequest  = 'Q'
	tagResponse = 'R'
)

// Limits. Each bounds an allocation a peer would otherwise size. The frame body
// bound caps the whole message; the element counts bound the per-collection
// slices before they are grown against bytes that may not be present.
const (
	// maxFrameBody is the largest request or response body this codec reads. It
	// is generous for a large statement or document parameter and small enough
	// that a frame cannot request an arbitrary allocation.
	maxFrameBody = 16 << 20

	// maxParams, maxColumns, and maxRows bound the three repeated collections.
	// The frame-body bound already limits total bytes; these keep a small frame
	// from naming an enormous element count.
	maxParams  = 1 << 16
	maxColumns = 1 << 16
	maxRows    = 1 << 24
)

// errBadLength reports a frame length field that cannot describe a frame:
// negative, or smaller than the four bytes the field itself occupies.
var errBadLength = errors.New("shardservice: frame length field is not a valid length")

// errFrameTooLarge reports a body larger than this codec accepts.
var errFrameTooLarge = errors.New("shardservice: frame body is larger than this service accepts")

// errBadTag reports a frame whose tag is not the one expected for its direction.
var errBadTag = errors.New("shardservice: frame has an unexpected tag byte")

// errBadVersion reports a body opening with an unknown wire version.
var errBadVersion = errors.New("shardservice: frame declares an unsupported wire version")

// errTruncated reports a body that ended in the middle of a field.
var errTruncated = errors.New("shardservice: frame body ended inside a field")

// errImpossibleCount reports an element count larger than the remaining body
// could hold. It is separated from truncation because it is the signature of a
// deliberately malformed frame rather than a short read.
var errImpossibleCount = errors.New("shardservice: frame declares more elements than its body can hold")

// errTrailing reports bytes left after a body decoded, which means the sender
// and this decoder disagree about the frame's shape.
var errTrailing = errors.New("shardservice: frame has trailing bytes after its last field")

// errFieldTooLarge reports a value whose length does not fit the codec's bound.
var errFieldTooLarge = errors.New("shardservice: field length exceeds the frame bound")

// errBadEnum reports a discriminator byte outside its closed set.
var errBadEnum = errors.New("shardservice: frame carries an out-of-range enumerator")

// errNegativeDuration reports a request deadline encoded as a negative
// duration.
var errNegativeDuration = errors.New("shardservice: request deadline is negative")

// errBadPresence reports an optional-field marker other than absent (0) or
// present (1).
var errBadPresence = errors.New("shardservice: frame carries an invalid optional-field marker")

// errUnexpectedReadPosition reports a position attached to a response shape
// that cannot represent a read.
var errUnexpectedReadPosition = errors.New("shardservice: read position is only valid on a row response")

// errNonCanonicalPosition reports an absent optional paired with a nonzero
// in-memory payload.
var errNonCanonicalPosition = errors.New("shardservice: absent logical position has a nonzero payload")

// encbuf accumulates a frame body. Its appenders never fail on a well-formed
// value; a field that overruns the length bound latches err so the whole encode
// reports it.
type encbuf struct {
	b   []byte
	err error
}

func (e *encbuf) u8(v uint8)   { e.b = append(e.b, v) }
func (e *encbuf) u32(v uint32) { e.b = binary.BigEndian.AppendUint32(e.b, v) }
func (e *encbuf) u64(v uint64) { e.b = binary.BigEndian.AppendUint64(e.b, v) }

// bytes appends a uint32 length prefix and the raw bytes, bounding the length.
func (e *encbuf) bytes(p []byte) {
	if len(p) > maxFrameBody {
		if e.err == nil {
			e.err = errFieldTooLarge
		}
		return
	}
	e.u32(uint32(len(p)))
	e.b = append(e.b, p...)
}

func (e *encbuf) str(s string) {
	if len(s) > maxFrameBody {
		if e.err == nil {
			e.err = errFieldTooLarge
		}
		return
	}
	e.u32(uint32(len(s)))
	e.b = append(e.b, s...)
}

// position appends one explicitly presence-tagged logical position. An absent
// option must carry the zero payload in memory; a present partial value is
// rejected.
func (e *encbuf) position(has bool, p Position) error {
	if !has {
		if !p.IsZero() {
			return errNonCanonicalPosition
		}
		e.u8(0)
		return nil
	}
	if err := p.Validate(); err != nil {
		return err
	}
	e.u8(1)
	e.u8(uint8(len(p.Distribution)))
	e.b = append(e.b, p.Distribution...)
	e.u8(uint8(len(p.Shard)))
	e.b = append(e.b, p.Shard...)
	e.b = append(e.b, p.LogID[:]...)
	e.u64(p.Index)
	return nil
}

// deccur is a cursor over one frame body. Every accessor is total: it either
// consumes exactly what it needs or latches a sticky failure and returns a zero
// value, so a decoder may read the whole body and test once at the end.
type deccur struct {
	b   []byte
	why error
}

func (d *deccur) fail(why error) {
	if d.why == nil {
		d.why = why
	}
	d.b = nil
}

func (d *deccur) bad() bool { return d.why != nil }

func (d *deccur) u8() uint8 {
	if len(d.b) < 1 {
		d.fail(errTruncated)
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *deccur) u32() uint32 {
	if len(d.b) < 4 {
		d.fail(errTruncated)
		return 0
	}
	v := binary.BigEndian.Uint32(d.b)
	d.b = d.b[4:]
	return v
}

func (d *deccur) u64() uint64 {
	if len(d.b) < 8 {
		d.fail(errTruncated)
		return 0
	}
	v := binary.BigEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

// fixed16 consumes one fixed-width log identity.
func (d *deccur) fixed16() [16]byte {
	var v [16]byte
	if len(d.b) < len(v) {
		d.fail(errTruncated)
		return v
	}
	copy(v[:], d.b[:len(v)])
	d.b = d.b[len(v):]
	return v
}

// slice consumes a uint32-length-prefixed byte run, refusing a length the
// remaining body cannot satisfy.
func (d *deccur) slice() []byte {
	n := d.u32()
	if d.bad() {
		return nil
	}
	if int64(n) > int64(len(d.b)) {
		d.fail(errTruncated)
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}

// str returns the next length-prefixed run as a copied string. Frame bodies are
// dropped after decode, so a retained field must own its bytes.
func (d *deccur) str() string {
	return string(d.slice())
}

// bytesCopy returns the next length-prefixed run as an owned copy.
func (d *deccur) bytesCopy() []byte {
	v := d.slice()
	if d.bad() {
		return nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

// position decodes one presence-tagged logical position and validates all of
// its identity components before returning it to admission.
func (d *deccur) position() (bool, Position, error) {
	present := d.u8()
	if d.bad() {
		return false, Position{}, d.why
	}
	switch present {
	case 0:
		return false, Position{}, nil
	case 1:
		distributionName, err := d.positionIdentity("distribution")
		if err != nil {
			return false, Position{}, err
		}
		shard, err := d.positionIdentity("shard")
		if err != nil {
			return false, Position{}, err
		}
		p := Position{
			Distribution: distribution.DistributionName(distributionName),
			Shard:        distribution.ShardID(shard),
			LogID:        d.fixed16(),
			Index:        d.u64(),
		}
		if d.bad() {
			return false, Position{}, d.why
		}
		if err := p.Validate(); err != nil {
			return false, Position{}, err
		}
		return true, p, nil
	default:
		return false, Position{}, errBadPresence
	}
}

// positionIdentity validates a length-prefixed identity directly against the
// frame backing bytes before copying it into a retained string. A peer can
// therefore never induce an allocation larger than MaxPositionIdentityBytes
// through a position field.
func (d *deccur) positionIdentity(field string) (string, error) {
	n := int(d.u8())
	if d.bad() {
		return "", d.why
	}
	if n == 0 {
		return "", &PositionValidationError{Reason: field + " is empty"}
	}
	if n > len(d.b) {
		d.fail(errTruncated)
		return "", d.why
	}
	raw := d.b[:n]
	d.b = d.b[n:]
	if !utf8.Valid(raw) {
		return "", &PositionValidationError{Reason: field + " is not valid UTF-8"}
	}
	return string(raw), nil
}

// count reads a uint32 element count and refuses one that cannot fit: each
// element occupies at least elem bytes, and no count may exceed max.
func (d *deccur) count(elem, max int) int {
	n := d.u32()
	if d.bad() {
		return 0
	}
	if int64(n) > int64(max) {
		d.fail(errImpossibleCount)
		return 0
	}
	if elem > 0 && int64(n)*int64(elem) > int64(len(d.b)) {
		d.fail(errImpossibleCount)
		return 0
	}
	return int(n)
}

// end reports whether the body was consumed exactly.
func (d *deccur) end() error {
	if d.why != nil {
		return d.why
	}
	if len(d.b) != 0 {
		return errTrailing
	}
	return nil
}

// writeFrame prefixes body with tag and the pgwire-style self-covering length,
// then writes the whole frame in one call.
func writeFrame(w io.Writer, tag byte, body []byte) error {
	if len(body) > maxFrameBody {
		return errFrameTooLarge
	}
	frame := make([]byte, 5+len(body))
	frame[0] = tag
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(body)+4))
	copy(frame[5:], body)
	_, err := w.Write(frame)
	return err
}

// readFrame reads one framed body, validating the tag and bounding the length
// before any allocation sized by it.
func readFrame(r io.Reader, tag byte) ([]byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	if hdr[0] != tag {
		return nil, errBadTag
	}
	length := int32(binary.BigEndian.Uint32(hdr[1:]))
	if length < 4 {
		return nil, errBadLength
	}
	if int64(length)-4 > int64(maxFrameBody) {
		return nil, errFrameTooLarge
	}
	size := int(length) - 4
	if size == 0 {
		return nil, nil
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

// EncodeRequest writes req as one framed message. It is deterministic: equal
// requests encode to identical bytes.
func EncodeRequest(w io.Writer, req *ShardRequest) error {
	if req == nil {
		return errors.New("shardservice: EncodeRequest requires a non-nil request")
	}
	if req.Deadline < 0 {
		return errNegativeDuration
	}
	if !req.ReadPolicy.valid() {
		return errBadEnum
	}
	if !req.ExecutionMode.valid() {
		return errBadEnum
	}
	if len(req.Params) > maxParams {
		return errFieldTooLarge
	}

	var e encbuf
	e.u8(wireVersion1)
	e.str(req.SQL)
	e.str(string(req.Distribution))
	e.str(string(req.Shard))
	e.u64(uint64(req.AllocationGeneration))
	e.u64(uint64(req.RoutingVersion))
	e.u64(uint64(req.OwnershipEpoch))
	e.u8(uint8(req.ReadPolicy))
	e.u8(uint8(req.ExecutionMode))
	e.u64(uint64(req.Deadline))
	e.u64(req.MaxResultBytes)
	e.u64(req.MaxRows)
	e.u32(uint32(len(req.Params)))
	for i := range req.Params {
		p := req.Params[i]
		if !p.Kind.valid() {
			return errBadEnum
		}
		e.u8(uint8(p.Kind))
		switch p.Kind {
		case ParamBool:
			if p.Bool {
				e.u8(1)
			} else {
				e.u8(0)
			}
		case ParamNumber, ParamString, ParamDocument:
			e.str(p.Text)
		case ParamNull:
			// no payload
		}
	}
	if err := e.position(req.HasMinPosition, req.MinPosition); err != nil {
		return err
	}
	if e.err != nil {
		return e.err
	}
	return writeFrame(w, tagRequest, e.b)
}

// DecodeRequest reads one framed request. A malformed or oversized frame yields
// a typed error, never a panic or an out-of-memory allocation.
func DecodeRequest(r io.Reader) (*ShardRequest, error) {
	body, err := readFrame(r, tagRequest)
	if err != nil {
		return nil, err
	}
	d := deccur{b: body}
	if d.u8() != wireVersion1 {
		return nil, errBadVersion
	}

	req := &ShardRequest{}
	req.SQL = d.str()
	req.Distribution = distribution.DistributionName(d.str())
	req.Shard = distribution.ShardID(d.str())
	req.AllocationGeneration = distribution.ShardAllocationGeneration(d.u64())
	req.RoutingVersion = distribution.RoutingVersion(d.u64())
	req.OwnershipEpoch = distribution.OwnershipEpoch(d.u64())
	policy := ReadPolicy(d.u8())
	req.ReadPolicy = policy
	mode := ExecutionMode(d.u8())
	req.ExecutionMode = mode
	deadline := d.u64()
	if deadline > math.MaxInt64 {
		return nil, errNegativeDuration
	}
	req.Deadline = time.Duration(deadline)
	req.MaxResultBytes = d.u64()
	req.MaxRows = d.u64()

	// Each parameter occupies at least a one-byte kind.
	n := d.count(1, maxParams)
	if n > 0 {
		req.Params = make([]Param, n)
		for i := 0; i < n; i++ {
			kind := ParamKind(d.u8())
			if d.bad() {
				break
			}
			if !kind.valid() {
				return nil, errBadEnum
			}
			p := Param{Kind: kind}
			switch kind {
			case ParamBool:
				marker := d.u8()
				if marker > 1 {
					return nil, errBadEnum
				}
				p.Bool = marker == 1
			case ParamNumber, ParamString, ParamDocument:
				p.Text = d.str()
			case ParamNull:
			}
			req.Params[i] = p
		}
	}
	req.HasMinPosition, req.MinPosition, err = d.position()
	if err != nil {
		return nil, err
	}
	if err := d.end(); err != nil {
		return nil, err
	}
	if !policy.valid() {
		return nil, errBadEnum
	}
	if !mode.valid() {
		return nil, errBadEnum
	}
	return req, nil
}

// EncodeResponse writes resp as one framed message. It is deterministic: equal
// responses encode to identical bytes.
func EncodeResponse(w io.Writer, resp *ShardResponse) error {
	if resp == nil {
		return errors.New("shardservice: EncodeResponse requires a non-nil response")
	}
	if !resp.Kind.valid() {
		return errBadEnum
	}
	if resp.HasReadPosition && resp.Kind != ResponseRows {
		return errUnexpectedReadPosition
	}

	var e encbuf
	e.u8(wireVersion1)
	e.u8(uint8(resp.Kind))
	switch resp.Kind {
	case ResponseRows:
		if len(resp.Columns) > maxColumns {
			return errFieldTooLarge
		}
		if len(resp.Rows) > maxRows {
			return errFieldTooLarge
		}
		e.u32(uint32(len(resp.Columns)))
		for i := range resp.Columns {
			e.str(resp.Columns[i].Name)
			e.u32(uint32(resp.Columns[i].TypeOID))
		}
		e.u32(uint32(len(resp.Rows)))
		for i := range resp.Rows {
			if len(resp.Rows[i]) != len(resp.Columns) {
				return errRowArity
			}
			for j := range resp.Rows[i] {
				c := resp.Rows[i][j]
				if c.Null {
					e.u8(1)
				} else {
					e.u8(0)
					e.bytes(c.Bytes)
				}
			}
		}
	case ResponseCompletion:
		e.u64(uint64(resp.RowsAffected))
	case ResponseError:
		if !resp.ErrorKind.valid() {
			return errBadEnum
		}
		e.u8(uint8(resp.ErrorKind))
		e.str(resp.ErrorMessage)
	}
	if err := e.position(resp.HasReadPosition, resp.ReadPosition); err != nil {
		return err
	}
	if e.err != nil {
		return e.err
	}
	return writeFrame(w, tagResponse, e.b)
}

// errRowArity reports a row whose cell count does not match the column count.
var errRowArity = errors.New("shardservice: response row cell count does not match column count")

// DecodeResponse reads one framed response. A malformed or oversized frame
// yields a typed error, never a panic or an out-of-memory allocation.
func DecodeResponse(r io.Reader) (*ShardResponse, error) {
	body, err := readFrame(r, tagResponse)
	if err != nil {
		return nil, err
	}
	d := deccur{b: body}
	if d.u8() != wireVersion1 {
		return nil, errBadVersion
	}
	kind := ResponseKind(d.u8())
	if d.bad() {
		return nil, d.why
	}
	if !kind.valid() {
		return nil, errBadEnum
	}

	resp := &ShardResponse{Kind: kind}
	switch kind {
	case ResponseRows:
		// Each column occupies at least a 4-byte name length plus a 4-byte OID.
		nc := d.count(8, maxColumns)
		if nc > 0 {
			resp.Columns = make([]Column, nc)
			for i := 0; i < nc; i++ {
				resp.Columns[i].Name = d.str()
				resp.Columns[i].TypeOID = int32(d.u32())
			}
		}
		// A zero-column result set carries no rows; reject rows without columns
		// so a tiny frame cannot name an enormous row count.
		perRow := nc // each cell occupies at least its one-byte null flag
		nr := d.count(perRow, maxRows)
		if d.bad() {
			return nil, d.why
		}
		if nc == 0 && nr != 0 {
			return nil, errRowArity
		}
		if nr > 0 {
			resp.Rows = make([][]Cell, nr)
			for i := 0; i < nr; i++ {
				row := make([]Cell, nc)
				for j := 0; j < nc; j++ {
					marker := d.u8()
					if marker > 1 {
						return nil, errBadEnum
					}
					if marker == 1 {
						row[j] = Cell{Null: true}
					} else {
						row[j] = Cell{Bytes: d.bytesCopy()}
					}
				}
				resp.Rows[i] = row
				if d.bad() {
					break
				}
			}
		}
	case ResponseCompletion:
		resp.RowsAffected = int64(d.u64())
	case ResponseError:
		ek := ErrorKind(d.u8())
		if d.bad() {
			return nil, d.why
		}
		if !ek.valid() {
			return nil, errBadEnum
		}
		resp.ErrorKind = ek
		resp.ErrorMessage = d.str()
	}
	resp.HasReadPosition, resp.ReadPosition, err = d.position()
	if err != nil {
		return nil, err
	}
	if resp.HasReadPosition && kind != ResponseRows {
		return nil, errUnexpectedReadPosition
	}
	if err := d.end(); err != nil {
		return nil, err
	}
	return resp, nil
}
