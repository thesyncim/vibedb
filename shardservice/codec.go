package shardservice

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedagg"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// The shard-service codec: a big-endian length-prefixed framing mirroring
// pgwire/proto.go, with every decode bounded so a malformed or
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

	// transactionMarker makes the optional trailing envelope unambiguous while
	// leaving every ordinary frame byte-for-byte unchanged.
	transactionMarker       = 0xd7
	accessScopeMarker       = 0xd8
	readFenceMarker         = 0xd9
	globalIndexLookupMarker = 0xda
	primaryKeyReadMarker    = 0xdb
	mutationCaptureMarker   = 0xdc
	documentScanMarker      = 0xdd
	partialAggregateMarker  = 0xde
	rowBatchMarker          = 0xdf
	exchangeMarker          = 0xe0
	repartitionMarker       = 0xe1
	authorityMarker         = 0xe2
	parameterTypesMarker    = 0xe3
	mutationImageMarker     = 0xe4
	// primaryKeyReadExtendedMarker adds the relation and catalog-frozen
	// document bound. Keep primaryKeyReadMarker on its original grammar so
	// zero-bound candidate requests remain byte-for-byte compatible.
	primaryKeyReadExtendedMarker = 0xe5
)

// Limits. Each bounds an allocation a peer would otherwise size. The frame body
// bound caps the whole message; the element counts bound the per-collection
// slices before they are grown against bytes that may not be present.
const (
	// The envelope must contain the largest admitted canonical result, including
	// the terminal cut's head, continuation, prepared result and release proof.
	// Per-operation limits and the shared byte budget still apply before any
	// peer-sized allocation; a caller-supplied limit cannot enlarge this ceiling.
	maxFrameBody = max(distributedtxn.MaxMutationBytes+(64<<10),
		replicatedReadResponseFixedBodyBytes+replicatedRequestLedgerReadValueHeaderBytes+
			replicatedstate.MaxRequestLedgerTerminalReadBytes)

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

// errFrameBudget reports aggregate native-frame pressure after the peer's
// length header was validated but before a body allocation was made.
var errFrameBudget = errors.New("shardservice: in-flight frame byte budget is full")

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

// errBadParam reports a typed parameter whose byte payload does not match its
// discriminator (for example malformed JSON or an invalid exact number).
var errBadParam = errors.New("shardservice: frame carries an invalid parameter payload")

var errBadParameterTypes = errors.New("shardservice: frame carries invalid SQL parameter type metadata")

// errBadTransaction reports a non-canonical command, a corrupt durable stage
// record, or a reply whose role and typed state disagree.
var errBadTransaction = errors.New("shardservice: frame carries an invalid transaction envelope")

// errBadGlobalIndexLookup reports a non-canonical raw lookup envelope or an
// attempt to combine that read-only lane with SQL or a transaction command.
var errBadGlobalIndexLookup = errors.New("shardservice: frame carries an invalid global-index lookup")

// errBadPrimaryKeyRead reports a non-canonical primary-key candidate envelope
// or an attempt to combine it with a non-read-only SQL lane.
var errBadPrimaryKeyRead = errors.New("shardservice: frame carries invalid primary-key candidates")

// errBadMutationCapture reports a capture marker combined with a lane that
// could mutate or change the selected snapshot semantics.
var errBadMutationCapture = errors.New("shardservice: frame carries an invalid mutation capture")

var errBadDocumentScan = errors.New("shardservice: frame carries an invalid document scan")

var errBadPartialAggregate = errors.New("shardservice: frame carries an invalid partial aggregate fragment")

var errBadRowBatch = errors.New("shardservice: frame carries an invalid row batch")

var errBadExchange = errors.New("shardservice: frame carries an invalid exchange command")

var errBadRepartition = errors.New("shardservice: frame carries an invalid repartition producer")

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

func (e *encbuf) fixed16(v [16]byte) { e.b = append(e.b, v[:]...) }

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

func (d *deccur) fixed8() [8]byte {
	var v [8]byte
	if len(d.b) < len(v) {
		d.fail(errTruncated)
		return v
	}
	copy(v[:], d.b[:len(v)])
	d.b = d.b[len(v):]
	return v
}

func (d *deccur) fixed32() [32]byte {
	var v [32]byte
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

// maxPooledFrameBuffer bounds recycled encoder arenas. Larger frames keep
// freshly allocated arenas instead of pinning megabytes per pooled buffer.
const maxPooledFrameBuffer = 64 << 10

// frameEncoderPool recycles frame body arenas across messages. Every encoder
// output flows straight into writeEncodedFrame's single Write and is dead
// afterwards, so the frame takes ownership and recycles on all paths; a
// missed recycle only loses the saving to the collector.
//
// Read buffers are deliberately NOT pooled: decoded parameters, keys, and
// cells alias the frame body zero-copy, so a reused read arena would corrupt
// live requests and responses.
var frameEncoderPool = sync.Pool{New: func() any { return []byte(nil) }}

// newFrameEncoder reserves the wire header in the same arena as the body. This
// keeps the single-Write contract without copying a completed multi-megabyte
// body into a second whole-frame allocation.
func newFrameEncoder(payloadHint int) encbuf {
	capacity := 5 + 256
	if payloadHint > 0 && payloadHint <= maxFrameBody-256 {
		capacity += payloadHint
	}
	if buf, _ := frameEncoderPool.Get().([]byte); cap(buf) >= capacity &&
		cap(buf) <= maxPooledFrameBuffer {
		return encbuf{b: buf[:5]}
	}
	return encbuf{b: make([]byte, 5, capacity)}
}

func writeEncodedFrame(w io.Writer, tag byte, frame []byte) error {
	if len(frame) < 5 || len(frame)-5 > maxFrameBody {
		return errFrameTooLarge
	}
	frame[0] = tag
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(frame)-1))
	n, err := w.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if cap(frame) <= maxPooledFrameBuffer {
		frameEncoderPool.Put(frame)
	}
	return err
}

// readFrame reads one framed body, validating the tag and bounding the length
// before any allocation sized by it.
func readFrame(r io.Reader, tag byte) ([]byte, error) {
	body, _, err := readFrameBudgeted(r, tag, nil)
	return body, err
}

// readFrameBudgeted reserves aggregate body bytes after validating the fixed
// header and before allocating. On every read error it releases the reservation;
// on success the caller owns charged until it has finished processing the frame.
func readFrameBudgeted(
	r io.Reader,
	tag byte,
	budget *replicatedFrameByteBudget,
) (body []byte, charged int64, err error) {
	return readFrameBudgetedLimit(r, tag, budget, maxFrameBody)
}

func readFrameBudgetedLimit(
	r io.Reader,
	tag byte,
	budget *replicatedFrameByteBudget,
	maxBody int,
) (body []byte, charged int64, err error) {
	if maxBody < 0 || maxBody > maxFrameBody {
		return nil, 0, errFrameTooLarge
	}
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, 0, err
	}
	if hdr[0] != tag {
		return nil, 0, errBadTag
	}
	length := int32(binary.BigEndian.Uint32(hdr[1:]))
	if length < 4 {
		return nil, 0, errBadLength
	}
	if int64(length)-4 > int64(maxBody) {
		return nil, 0, errFrameTooLarge
	}
	size := int(length) - 4
	if size == 0 {
		return nil, 0, nil
	}
	charged = int64(size)
	if budget != nil && !budget.reserve(charged) {
		return nil, 0, errFrameBudget
	}
	body = make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		if budget != nil {
			budget.release(charged)
		}
		return nil, 0, err
	}
	return body, charged, nil
}

func validateTransactionRequest(tx *TransactionRequest, cacheDecodedMeta bool) error {
	if tx == nil {
		return errBadTransaction
	}
	if tx.Operation == TransactionNone {
		if !tx.ID.IsZero() || tx.Revision != 0 || tx.RecoveryPulse != 0 || tx.SegmentIndex != 0 || len(tx.Record) != 0 ||
			len(tx.ManifestSegment) != 0 {
			return errBadTransaction
		}
		return nil
	}
	if !tx.Operation.valid() {
		return errBadEnum
	}
	if (tx.Operation == TransactionPulseCoordinator) != (tx.RecoveryPulse != 0) ||
		tx.RecoveryPulse > distributedtxn.MaxRecoveryPulses {
		return errBadTransaction
	}
	if tx.Operation.stages() {
		if !tx.ID.IsZero() || tx.Revision != 0 || tx.SegmentIndex != 0 || len(tx.Record) == 0 ||
			len(tx.ManifestSegment) != 0 {
			return errBadTransaction
		}
		var err error
		if tx.Operation == TransactionStageCoordinator {
			var targets [distributedtxn.MaxInlineTargets]distributedtxn.TransactionTargetRef
			_, err = distributedtxn.OpenCoordinatorInto(tx.Record, targets[:])
		} else {
			var scopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
			_, err = distributedtxn.OpenTargetInto(tx.Record, scopes[:])
		}
		if err != nil {
			return errors.Join(errBadTransaction, err)
		}
		return nil
	}
	if tx.Operation.stagesManifestCoordinator() {
		if !tx.ID.IsZero() || tx.Revision != 0 || tx.SegmentIndex != 0 || len(tx.Record) == 0 ||
			len(tx.ManifestSegment) == 0 {
			return errBadTransaction
		}
		record, err := distributedtxn.OpenManifestCoordinator(tx.Record)
		if err != nil {
			return errors.Join(errBadTransaction, err)
		}
		meta, err := inspectTransactionManifestFirstSegment(tx.ManifestSegment, record.Manifest)
		if err != nil {
			return errors.Join(errBadTransaction, err)
		}
		if cacheDecodedMeta {
			tx.manifestMeta = &meta
		}
		return nil
	}
	if tx.Operation.stagesManifestSegment() {
		if tx.ID.IsZero() || tx.Revision != 0 || tx.SegmentIndex != 0 || len(tx.Record) != 0 ||
			len(tx.ManifestSegment) == 0 {
			return errBadTransaction
		}
		meta, err := inspectTransactionManifestSegment(tx.ManifestSegment)
		if err != nil || meta.index == 0 {
			return errors.Join(errBadTransaction, err)
		}
		if cacheDecodedMeta {
			tx.manifestMeta = &meta
		}
		return nil
	}
	if tx.Operation.readsManifestSegment() {
		if tx.ID.IsZero() || tx.Revision != 0 || len(tx.Record) != 0 || len(tx.ManifestSegment) != 0 {
			return errBadTransaction
		}
		return nil
	}
	if tx.Operation == TransactionScanCoordinator {
		if tx.Revision != 0 || tx.SegmentIndex != 0 || len(tx.Record) != 0 || len(tx.ManifestSegment) != 0 {
			return errBadTransaction
		}
		return nil
	}
	if tx.ID.IsZero() || tx.SegmentIndex != 0 || len(tx.Record) != 0 || len(tx.ManifestSegment) != 0 {
		return errBadTransaction
	}
	lookup := tx.Operation == TransactionLookupCoordinator ||
		tx.Operation == TransactionLookupTarget || tx.Operation == TransactionReadTarget
	if (lookup && tx.Revision != 0) || (!lookup && tx.Revision == 0) {
		return errBadTransaction
	}
	return nil
}

func validateTransactionReply(tx TransactionReply) error {
	if tx.Role == TransactionRoleNone {
		if !tx.ID.IsZero() || tx.Revision != 0 ||
			tx.RecoveryPulse != 0 ||
			tx.CoordinatorState != distributedtxn.CoordinatorInvalid ||
			tx.TargetState != distributedtxn.TargetInvalid ||
			tx.RecordKind != TransactionRecordNone || tx.SegmentIndex != 0 || len(tx.Record) != 0 {
			return errBadTransaction
		}
		return nil
	}
	if tx.ID.IsZero() || tx.Revision == 0 {
		return errBadTransaction
	}
	switch tx.Role {
	case TransactionRoleCoordinator:
		if tx.CoordinatorState < distributedtxn.CoordinatorStaging ||
			tx.CoordinatorState > distributedtxn.CoordinatorRetired ||
			tx.TargetState != distributedtxn.TargetInvalid {
			return errBadTransaction
		}
		switch tx.RecordKind {
		case TransactionRecordNone:
			if tx.SegmentIndex != 0 || len(tx.Record) != 0 {
				return errBadTransaction
			}
		case TransactionRecordInlineCoordinator:
			if tx.SegmentIndex != 0 || len(tx.Record) == 0 {
				return errBadTransaction
			}
			var targets [distributedtxn.MaxInlineTargets]distributedtxn.TransactionTargetRef
			record, err := distributedtxn.OpenCoordinatorInto(tx.Record, targets[:])
			if err != nil || record.ID != tx.ID {
				return errors.Join(errBadTransaction, err)
			}
		case TransactionRecordManifestCoordinator:
			if tx.SegmentIndex != 0 || len(tx.Record) == 0 {
				return errBadTransaction
			}
			record, err := distributedtxn.OpenManifestCoordinator(tx.Record)
			if err != nil || record.ID != tx.ID {
				return errors.Join(errBadTransaction, err)
			}
		case TransactionRecordManifestSegment:
			if len(tx.Record) == 0 {
				return errBadTransaction
			}
			meta, err := inspectTransactionManifestSegment(tx.Record)
			if err != nil || meta.index != tx.SegmentIndex {
				return errors.Join(errBadTransaction, err)
			}
		default:
			return errBadTransaction
		}
	case TransactionRoleTarget:
		if tx.RecoveryPulse != 0 || tx.TargetState < distributedtxn.TargetStaged ||
			tx.TargetState > distributedtxn.TargetReleased ||
			tx.CoordinatorState != distributedtxn.CoordinatorInvalid {
			return errBadTransaction
		}
		if tx.SegmentIndex != 0 {
			return errBadTransaction
		}
		switch tx.RecordKind {
		case TransactionRecordNone:
			if len(tx.Record) != 0 {
				return errBadTransaction
			}
		case TransactionRecordTarget:
			if len(tx.Record) == 0 {
				return errBadTransaction
			}
			var scopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
			record, err := distributedtxn.OpenTargetInto(tx.Record, scopes[:])
			if err != nil || record.ID != tx.ID {
				return errors.Join(errBadTransaction, err)
			}
		default:
			return errBadTransaction
		}
	default:
		return errBadEnum
	}
	return nil
}

// openTransactionManifestSegment bounds all decoder scratch to one manifest
// page. The segmented lane is cold recovery/control traffic; it never sizes an
// allocation from an unauthenticated aggregate target count.
type transactionManifestScratch struct {
	targets [distributedtxn.MaxManifestPageTargets]distributedtxn.TransactionTargetRef
	// Prefix compression can reconstruct two full 255-byte identities per
	// target from one 64 KiB page. This exact one-page maximum is a scratch
	// bound, never an aggregate transaction allocation.
	identities [distributedtxn.MaxManifestPageTargets * distributedtxn.MaxShardIdentityBytes * 2]byte
}

var transactionManifestScratchPool = sync.Pool{New: func() any {
	return new(transactionManifestScratch)
}}

func borrowTransactionManifestScratch() *transactionManifestScratch {
	return transactionManifestScratchPool.Get().(*transactionManifestScratch)
}

func releaseTransactionManifestScratch(scratch *transactionManifestScratch) {
	if scratch != nil {
		transactionManifestScratchPool.Put(scratch)
	}
}

func inspectTransactionManifestSegment(raw []byte) (transactionManifestSegmentMeta, error) {
	scratch := borrowTransactionManifestScratch()
	defer releaseTransactionManifestScratch(scratch)
	page, err := distributedtxn.OpenManifestSegment(raw, scratch.targets[:], scratch.identities[:])
	if err != nil {
		return transactionManifestSegmentMeta{}, err
	}
	return transactionManifestMeta(page), nil
}

func inspectTransactionManifestFirstSegment(
	raw []byte,
	descriptor distributedtxn.ManifestDescriptor,
) (transactionManifestSegmentMeta, error) {
	reader, err := distributedtxn.NewManifestReader(descriptor)
	if err != nil {
		return transactionManifestSegmentMeta{}, err
	}
	scratch := borrowTransactionManifestScratch()
	defer releaseTransactionManifestScratch(scratch)
	page, err := reader.OpenNext(raw, scratch.targets[:], scratch.identities[:])
	if err != nil {
		return transactionManifestSegmentMeta{}, err
	}
	meta := transactionManifestMeta(page)
	if !manifestFirstPageWithinDescriptor(meta, len(raw), descriptor) {
		return transactionManifestSegmentMeta{}, distributedtxn.ErrCorrupt
	}
	if descriptor.SegmentCount == 1 {
		if err := reader.Seal(); err != nil {
			return transactionManifestSegmentMeta{}, err
		}
	}
	return meta, nil
}

func transactionManifestMeta(page distributedtxn.ManifestPage) transactionManifestSegmentMeta {
	first := page.Targets[0]
	meta := transactionManifestSegmentMeta{
		valid: true, index: page.Segment.Index,
		firstTarget:     page.Segment.FirstTarget,
		targetCount:     page.Segment.TargetCount,
		distributionLen: uint8(len(first.Distribution)), shardLen: uint8(len(first.Shard)),
		routingVersion: first.RoutingVersion, allocation: first.AllocationGeneration,
		ownershipEpoch: first.OwnershipEpoch,
	}
	copy(meta.distribution[:], first.Distribution)
	copy(meta.shard[:], first.Shard)
	return meta
}

func manifestFirstPageWithinDescriptor(
	meta transactionManifestSegmentMeta,
	rawBytes int,
	descriptor distributedtxn.ManifestDescriptor,
) bool {
	if !meta.valid || meta.index != 0 || meta.firstTarget != 0 || rawBytes <= 0 ||
		descriptor.SegmentCount == 0 || uint64(meta.targetCount) > descriptor.TargetCount ||
		uint64(rawBytes) > descriptor.EncodedBytes {
		return false
	}
	if descriptor.SegmentCount == 1 {
		return uint64(meta.targetCount) == descriptor.TargetCount &&
			uint64(rawBytes) == descriptor.EncodedBytes
	}
	return uint64(meta.targetCount) < descriptor.TargetCount &&
		uint64(rawBytes) < descriptor.EncodedBytes
}

func validateExchangeRequest(req *ShardRequest) error {
	if !req.Exchange.canonical() {
		return errBadExchange
	}
	if !req.Exchange.present() {
		return nil
	}
	if req.SQL != "" || len(req.Params) != 0 || len(req.ParamTypes) != 0 || req.PartialAggregate ||
		req.RowBatch.present() || req.HasMinPosition || req.MinPosition != (Position{}) ||
		req.MaxResultBytes != 0 || req.MaxRows != 0 || req.BucketBits != 0 ||
		len(req.AccessScopes) != 0 || !req.ReadFenceID.IsZero() ||
		req.GlobalIndexLookup.present() || req.PrimaryKeyRead.present() ||
		req.mutationCapturePresent() || req.DocumentScan.present() || req.Repartition.present() ||
		req.Transaction.Operation != TransactionNone {
		return errBadExchange
	}
	wantMode := ExecutionReadWrite
	if req.Exchange.Operation == ExchangePull {
		wantMode = ExecutionReadOnly
	}
	if req.ExecutionMode != wantMode {
		return errBadExchange
	}
	return nil
}

func validateRepartitionRequest(req *ShardRequest) error {
	if !req.Repartition.canonical() {
		return errBadRepartition
	}
	if !req.Repartition.present() {
		return nil
	}
	if req.SQL == "" || req.ExecutionMode != ExecutionReadOnly ||
		req.MaxRows == 0 || req.MaxResultBytes == 0 || req.RowBatch.present() ||
		req.GlobalIndexLookup.present() || req.PrimaryKeyRead.present() ||
		req.mutationCapturePresent() || req.DocumentScan.present() || req.Exchange.present() ||
		req.Transaction.Operation != TransactionNone {
		return errBadRepartition
	}
	return nil
}

// ValidateRequest validates the complete semantic request grammar without
// serializing it. The replicated semantic dispatcher uses this entry point
// for its local path so local execution observes the same bounds and field
// exclusions as the authenticated wire path. It deliberately does not mutate
// req or retain any of its borrowed buffers.
func ValidateRequest(req *ShardRequest) error {
	if req == nil {
		return errors.New("shardservice: ValidateRequest requires a non-nil request")
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
	authorityPresent := req.Authority.Node != (rafttransport.NodeID{}) ||
		req.Authority.Generation != 0
	if authorityPresent && !req.Authority.Valid() {
		return errBadPresence
	}
	if len(req.Params) > maxParams {
		return errFieldTooLarge
	}
	if len(req.ParamTypes) != 0 &&
		(req.SQL == "" || !validSQLParameterTypes(req.Params, req.ParamTypes)) {
		return errBadParameterTypes
	}
	for i := range req.Params {
		p := req.Params[i]
		if !p.Kind.valid() {
			return errBadEnum
		}
		if !p.Valid() {
			return errBadParam
		}
	}
	if err := validateTransactionRequest(&req.Transaction, false); err != nil {
		return err
	}
	if err := validateExchangeRequest(req); err != nil {
		return err
	}
	if err := validateRepartitionRequest(req); err != nil {
		return err
	}
	if !req.GlobalIndexLookup.canonical() {
		return errBadGlobalIndexLookup
	}
	globalIndexLookupBytes := uint64(4)
	for i := range req.GlobalIndexLookup.KeyTuples {
		globalIndexLookupBytes += uint64(4 + len(req.GlobalIndexLookup.KeyTuples[i]))
		if globalIndexLookupBytes > maxFrameBody {
			return errFieldTooLarge
		}
	}
	if len(req.PrimaryKeyRead.Keys) > maxParams {
		return errFieldTooLarge
	}
	if !req.PrimaryKeyRead.canonical() {
		return errBadPrimaryKeyRead
	}
	if !req.DocumentScan.canonical() {
		return errBadDocumentScan
	}
	if req.MutationCapture && req.MutationImageCapture {
		return errBadMutationCapture
	}
	mutationCapture := req.mutationCapturePresent()
	primaryReadBytes := uint64(1 + 4 + len(req.PrimaryKeyRead.PrimaryPath) + 4)
	if req.PrimaryKeyRead.Relation != 0 || req.PrimaryKeyRead.MaxDocumentBytes != 0 {
		primaryReadBytes += 1 + 4
	}
	for i := range req.PrimaryKeyRead.Keys {
		primaryReadBytes += uint64(4 + len(req.PrimaryKeyRead.Keys[i]))
		if primaryReadBytes > maxFrameBody {
			return errFieldTooLarge
		}
	}
	if !distributedtxn.ValidateIntentScopes(req.AccessScopes, req.BucketBits) {
		return errBadTransaction
	}
	if req.Transaction.Operation != TransactionNone &&
		(len(req.ParamTypes) != 0 || !req.ReadFenceID.IsZero() || req.GlobalIndexLookup.present() || req.PrimaryKeyRead.present() || mutationCapture || req.DocumentScan.present()) {
		if req.GlobalIndexLookup.present() {
			return errBadGlobalIndexLookup
		}
		return errBadTransaction
	}
	if req.GlobalIndexLookup.present() &&
		(req.SQL != "" || len(req.Params) != 0 || req.PrimaryKeyRead.present() || mutationCapture || req.DocumentScan.present() || req.ExecutionMode != ExecutionReadOnly) {
		return errBadGlobalIndexLookup
	}
	if req.PrimaryKeyRead.present() && (req.SQL == "" || mutationCapture || req.DocumentScan.present() || req.ExecutionMode != ExecutionReadOnly) {
		return errBadPrimaryKeyRead
	}
	if mutationCapture && (req.SQL == "" || req.ExecutionMode != ExecutionReadOnly || req.DocumentScan.present() ||
		!req.ReadFenceID.IsZero()) {
		return errBadMutationCapture
	}
	if req.DocumentScan.present() && (req.SQL != "" || len(req.Params) != 0 ||
		req.ExecutionMode != ExecutionReadOnly || !req.ReadFenceID.IsZero() ||
		req.MaxRows == 0 || req.MaxResultBytes == 0 || mutationCapture) {
		return errBadDocumentScan
	}
	if req.PartialAggregate && (req.SQL == "" || req.ExecutionMode != ExecutionReadOnly ||
		mutationCapture || req.DocumentScan.present() || req.GlobalIndexLookup.present() ||
		req.Transaction.Operation != TransactionNone) {
		return errBadPartialAggregate
	}
	if !req.RowBatch.canonical() || (req.RowBatch.present() &&
		(req.SQL == "" || req.ExecutionMode != ExecutionReadOnly || mutationCapture ||
			req.DocumentScan.present() || req.GlobalIndexLookup.present() ||
			req.Transaction.Operation != TransactionNone || req.MaxRows == 0 ||
			req.MaxResultBytes == 0)) {
		return errBadRowBatch
	}
	if req.HasMinPosition {
		if err := req.MinPosition.Validate(); err != nil {
			return err
		}
	} else if !req.MinPosition.IsZero() {
		return errNonCanonicalPosition
	}
	if _, err := requestFrameBytes(req); err != nil {
		return err
	}
	return nil
}

// RequestFrameBytes returns the exact encoded request size, including the
// five-byte frame header, without materializing a frame. It shares the same
// checked size grammar used by ValidateRequest and is intended for admission
// reservations at semantic and remote transport boundaries.
func RequestFrameBytes(req *ShardRequest) (int, error) {
	if err := ValidateRequest(req); err != nil {
		return 0, err
	}
	return requestFrameBytes(req)
}

func requestFrameBytes(req *ShardRequest) (int, error) {
	if req == nil {
		return 0, errBadParam
	}
	total := 1 // wire version
	add := func(n int) error {
		if n < 0 || n > maxFrameBody-total {
			return errFrameTooLarge
		}
		total += n
		return nil
	}
	addBytes := func(p []byte) error {
		if len(p) > maxFrameBody {
			return errFieldTooLarge
		}
		return add(4 + len(p))
	}
	addString := func(s string) error {
		if len(s) > maxFrameBody {
			return errFieldTooLarge
		}
		return add(4 + len(s))
	}
	for _, value := range []string{string(req.SQL), string(req.Distribution), string(req.Shard)} {
		if err := addString(value); err != nil {
			return 0, err
		}
	}
	if err := add(8*3 + 1 + 1 + 8 + 8 + 8 + 4); err != nil {
		return 0, err
	}
	for _, parameter := range req.Params {
		if err := add(1); err != nil {
			return 0, err
		}
		switch parameter.Kind {
		case ParamBool:
			if err := add(1); err != nil {
				return 0, err
			}
		case ParamNumber, ParamString, ParamDocument:
			if err := addBytes(parameter.Bytes); err != nil {
				return 0, err
			}
		}
	}
	if req.HasMinPosition {
		if err := add(1 + 1 + len(req.MinPosition.Distribution) + 1 + len(req.MinPosition.Shard) + 16 + 8); err != nil {
			return 0, err
		}
	} else if err := add(1); err != nil {
		return 0, err
	}
	if len(req.AccessScopes) != 0 {
		if err := add(1 + 1 + 4 + 8*len(req.AccessScopes)); err != nil {
			return 0, err
		}
	}
	if !req.ReadFenceID.IsZero() {
		if err := add(1 + 16); err != nil {
			return 0, err
		}
	}
	if req.GlobalIndexLookup.present() {
		if err := add(1 + 8 + 8 + 4 + 1 + 1); err != nil {
			return 0, err
		}
		if err := addBytes(req.GlobalIndexLookup.Relation); err != nil {
			return 0, err
		}
		for _, tuple := range req.GlobalIndexLookup.KeyTuples {
			if err := addBytes(tuple); err != nil {
				return 0, err
			}
		}
	}
	if req.PrimaryKeyRead.present() {
		primaryKeyReadBytes := 1 + 4
		if req.PrimaryKeyRead.Relation != 0 || req.PrimaryKeyRead.MaxDocumentBytes != 0 {
			primaryKeyReadBytes += 1 + 4
		}
		if err := add(primaryKeyReadBytes); err != nil {
			return 0, err
		}
		if err := addBytes(req.PrimaryKeyRead.PrimaryPath); err != nil {
			return 0, err
		}
		for _, key := range req.PrimaryKeyRead.Keys {
			if err := addBytes(key); err != nil {
				return 0, err
			}
		}
	}
	if req.MutationCapture {
		if err := add(1); err != nil {
			return 0, err
		}
	}
	if req.DocumentScan.present() {
		if err := add(1); err != nil {
			return 0, err
		}
		if err := addBytes(req.DocumentScan.Relation); err != nil {
			return 0, err
		}
		if err := addBytes(req.DocumentScan.After); err != nil {
			return 0, err
		}
	}
	if req.PartialAggregate {
		if err := add(1); err != nil {
			return 0, err
		}
	}
	if req.RowBatch.present() {
		if err := add(1 + 4 + 4); err != nil {
			return 0, err
		}
	}
	if req.Repartition.present() {
		if err := add(1 + repartitionRequestBytes(req.Repartition)); err != nil {
			return 0, err
		}
	}
	if req.Exchange.present() {
		if err := add(1 + exchangeRequestBytes(req.Exchange)); err != nil {
			return 0, err
		}
	}
	if req.Transaction.Operation != TransactionNone {
		if err := add(1 + transactionRequestBytes(req.Transaction)); err != nil {
			return 0, err
		}
	}
	if req.Authority.Valid() {
		if err := add(1 + 16 + 8); err != nil {
			return 0, err
		}
	}
	if len(req.ParamTypes) != 0 {
		if err := add(1 + 4 + len(req.ParamTypes)); err != nil {
			return 0, err
		}
	}
	if req.MutationImageCapture {
		if err := add(1); err != nil {
			return 0, err
		}
	}
	return total + 5, nil // tag and four-byte length are included in the frame header
}

func repartitionRequestBytes(request RepartitionRequest) int {
	// fixed operation plus stage, attempt, producer, key/target counts and
	// block rows/bytes, followed by the memory ceiling.
	total := 16 + 4*7 + 8
	for _, column := range request.KeyColumns {
		_ = column
		total += 4
	}
	for _, target := range request.Targets {
		total += 4 + len(target.Address) + 4 + len(target.Distribution) +
			4 + len(target.Shard) + 8*3
	}
	return total
}

func exchangeRequestBytes(request ExchangeRequest) int {
	total := 1 + 16 + 4 + 4 + 4
	switch request.Operation {
	case ExchangeOpen:
		total += 4*3 + 8*4
	case ExchangePush:
		// producer, sequence, rows, final marker, and the length-prefixed
		// mailbox bytes.
		total += 4 + 4 + 4 + 1 + 4 + len(request.Batch.Data)
	case ExchangePull:
		total++
		if request.HasAck {
			total += 4 + 4
		}
	case ExchangeReduce:
		total += 16 + 4*3 + 4 + len(request.Kinds) + 4 + 4*len(request.GroupKeys) + 8 + 4 + 4
	}
	return total
}

func transactionRequestBytes(request TransactionRequest) int {
	total := 1
	switch {
	case request.Operation.stages():
		total += 4 + len(request.Record)
	case request.Operation.stagesManifestCoordinator():
		total += 4 + len(request.Record) + 4 + len(request.ManifestSegment)
	case request.Operation.stagesManifestSegment():
		total += 16 + 4 + len(request.ManifestSegment)
	case request.Operation.readsManifestSegment():
		total += 16 + 4
	default:
		total += 16 + 8
		if request.Operation == TransactionPulseCoordinator {
			total++
		}
	}
	return total
}

func encodeRepartitionRequest(e *encbuf, request RepartitionRequest) {
	e.fixed16(request.Operation)
	e.u32(request.Stage)
	e.u32(request.Attempt)
	e.u32(uint32(request.Producer))
	e.u32(uint32(len(request.KeyColumns)))
	for _, column := range request.KeyColumns {
		e.u32(uint32(column))
	}
	e.u32(uint32(len(request.Targets)))
	for i := range request.Targets {
		target := request.Targets[i]
		e.bytes(target.Address)
		e.str(string(target.Distribution))
		e.str(string(target.Shard))
		e.u64(uint64(target.AllocationGeneration))
		e.u64(uint64(target.RoutingVersion))
		e.u64(uint64(target.OwnershipEpoch))
	}
	e.u32(request.BlockRows)
	e.u32(request.BlockBytes)
	e.u64(request.MaxMemory)
}

func decodeRepartitionRequest(d *deccur) (RepartitionRequest, error) {
	request := RepartitionRequest{
		Operation: exchange.ID(d.fixed16()), Stage: d.u32(), Attempt: d.u32(),
	}
	producer := d.u32()
	if producer > math.MaxUint16 {
		return RepartitionRequest{}, errBadRepartition
	}
	request.Producer = uint16(producer)
	keys := d.count(4, MaxRepartitionKeyColumns)
	if keys != 0 {
		request.KeyColumns = make([]uint16, keys)
		for i := range request.KeyColumns {
			column := d.u32()
			if column > math.MaxUint16 {
				return RepartitionRequest{}, errBadRepartition
			}
			request.KeyColumns[i] = uint16(column)
		}
	}
	targets := d.count(36, MaxRepartitionTargets)
	if targets != 0 {
		request.Targets = make([]RepartitionTarget, targets)
		for i := range request.Targets {
			request.Targets[i] = RepartitionTarget{
				Address: d.slice(), Distribution: distribution.DistributionName(d.str()),
				Shard:                distribution.ShardID(d.str()),
				AllocationGeneration: distribution.ShardAllocationGeneration(d.u64()),
				RoutingVersion:       distribution.RoutingVersion(d.u64()),
				OwnershipEpoch:       distribution.OwnershipEpoch(d.u64()),
			}
		}
	}
	request.BlockRows = d.u32()
	request.BlockBytes = d.u32()
	request.MaxMemory = d.u64()
	if d.bad() {
		return RepartitionRequest{}, d.why
	}
	if !request.canonical() {
		return RepartitionRequest{}, errBadRepartition
	}
	return request, nil
}

func encodeExchangeRequest(e *encbuf, request ExchangeRequest) {
	e.u8(uint8(request.Operation))
	e.fixed16(request.Key.Operation)
	e.u32(request.Key.Stage)
	e.u32(request.Key.Partition)
	e.u32(request.Key.Attempt)
	switch request.Operation {
	case ExchangeOpen:
		e.u32(uint32(request.Producers))
		e.u32(uint32(request.QueuedBatches))
		e.u32(uint32(request.ProducerBatches))
		e.u64(request.BufferedRows)
		e.u64(request.BufferedBytes)
		e.u64(request.TotalRows)
		e.u64(request.TotalBytes)
	case ExchangePush:
		encodeExchangeBatch(e, request.Batch)
	case ExchangePull:
		if request.HasAck {
			e.u8(1)
			e.u32(uint32(request.AckProducer))
			e.u32(request.AckSequence)
		} else {
			e.u8(0)
		}
	case ExchangeReduce:
		e.fixed16(request.Output.Operation)
		e.u32(request.Output.Stage)
		e.u32(request.Output.Partition)
		e.u32(request.Output.Attempt)
		e.u32(uint32(len(request.Kinds)))
		for _, kind := range request.Kinds {
			e.u8(uint8(kind))
		}
		e.u32(uint32(len(request.GroupKeys)))
		for _, column := range request.GroupKeys {
			e.u32(uint32(column))
		}
		e.u64(request.MaxStateBytes)
		e.u32(request.BlockRows)
		e.u32(request.BlockBytes)
	}
}

func decodeExchangeRequest(d *deccur) (ExchangeRequest, error) {
	request := ExchangeRequest{
		Operation: ExchangeOperation(d.u8()),
		Key: exchange.Key{
			Operation: exchange.ID(d.fixed16()),
			Stage:     d.u32(), Partition: d.u32(), Attempt: d.u32(),
		},
	}
	switch request.Operation {
	case ExchangeOpen:
		producers := d.u32()
		queued := d.u32()
		producerBatches := d.u32()
		if producers > math.MaxUint16 || queued > math.MaxUint16 || producerBatches > math.MaxUint16 {
			return ExchangeRequest{}, errBadExchange
		}
		request.Producers = uint16(producers)
		request.QueuedBatches = uint16(queued)
		request.ProducerBatches = uint16(producerBatches)
		request.BufferedRows = d.u64()
		request.BufferedBytes = d.u64()
		request.TotalRows = d.u64()
		request.TotalBytes = d.u64()
	case ExchangePush:
		batch, err := decodeExchangeBatch(d)
		if err != nil {
			return ExchangeRequest{}, err
		}
		request.Batch = batch
	case ExchangePull:
		present := d.u8()
		if present > 1 {
			return ExchangeRequest{}, errBadPresence
		}
		request.HasAck = present == 1
		if request.HasAck {
			producer := d.u32()
			if producer > math.MaxUint16 {
				return ExchangeRequest{}, errBadExchange
			}
			request.AckProducer = uint16(producer)
			request.AckSequence = d.u32()
		}
	case ExchangeCancel:
	case ExchangeReduce:
		request.Output = exchange.Key{
			Operation: exchange.ID(d.fixed16()), Stage: d.u32(),
			Partition: d.u32(), Attempt: d.u32(),
		}
		kinds := d.count(1, MaxExchangeReducerColumns)
		if kinds > 0 {
			request.Kinds = make([]distributedagg.Kind, kinds)
			for i := range request.Kinds {
				request.Kinds[i] = distributedagg.Kind(d.u8())
			}
		}
		keys := d.count(4, MaxRepartitionKeyColumns)
		if keys > 0 {
			request.GroupKeys = make([]uint16, keys)
			for i := range request.GroupKeys {
				column := d.u32()
				if column > math.MaxUint16 {
					return ExchangeRequest{}, errBadExchange
				}
				request.GroupKeys[i] = uint16(column)
			}
		}
		request.MaxStateBytes = d.u64()
		request.BlockRows = d.u32()
		request.BlockBytes = d.u32()
	default:
		return ExchangeRequest{}, errBadEnum
	}
	if d.bad() {
		return ExchangeRequest{}, d.why
	}
	if !request.canonical() {
		return ExchangeRequest{}, errBadExchange
	}
	return request, nil
}

func encodeExchangeBatch(e *encbuf, batch exchange.Batch) {
	e.u32(uint32(batch.Producer))
	e.u32(batch.Sequence)
	e.u32(batch.Rows)
	if batch.Final {
		e.u8(1)
	} else {
		e.u8(0)
	}
	e.bytes(batch.Data)
}

func decodeExchangeBatch(d *deccur) (exchange.Batch, error) {
	producer := d.u32()
	batch := exchange.Batch{Sequence: d.u32(), Rows: d.u32()}
	final := d.u8()
	if producer > math.MaxUint16 || final > 1 {
		return exchange.Batch{}, errBadExchange
	}
	batch.Producer = uint16(producer)
	batch.Final = final == 1
	batch.Data = d.slice()
	if d.bad() {
		return exchange.Batch{}, d.why
	}
	if !canonicalExchangeBatch(batch) {
		return exchange.Batch{}, errBadExchange
	}
	return batch, nil
}

func encodeExchangeReply(e *encbuf, reply ExchangeReply) {
	e.u8(uint8(reply.Operation))
	if reply.Operation != ExchangePull {
		return
	}
	if reply.EOF {
		e.u8(1)
		return
	}
	e.u8(0)
	encodeExchangeBatch(e, reply.Batch)
}

func decodeExchangeReply(d *deccur) (ExchangeReply, error) {
	reply := ExchangeReply{Operation: ExchangeOperation(d.u8())}
	if !reply.Operation.valid() {
		return ExchangeReply{}, errBadEnum
	}
	if reply.Operation == ExchangePull {
		eof := d.u8()
		if eof > 1 {
			return ExchangeReply{}, errBadPresence
		}
		reply.EOF = eof == 1
		if !reply.EOF {
			batch, err := decodeExchangeBatch(d)
			if err != nil {
				return ExchangeReply{}, err
			}
			reply.Batch = batch
		}
	}
	if d.bad() {
		return ExchangeReply{}, d.why
	}
	if !reply.canonical() {
		return ExchangeReply{}, errBadExchange
	}
	return reply, nil
}

func encodeTransactionRequest(e *encbuf, tx TransactionRequest) {
	e.u8(uint8(tx.Operation))
	if tx.Operation.stages() {
		e.bytes(tx.Record)
		return
	}
	if tx.Operation.stagesManifestCoordinator() {
		e.bytes(tx.Record)
		e.bytes(tx.ManifestSegment)
		return
	}
	if tx.Operation.stagesManifestSegment() {
		e.fixed16(tx.ID)
		e.bytes(tx.ManifestSegment)
		return
	}
	if tx.Operation.readsManifestSegment() {
		e.fixed16(tx.ID)
		e.u32(tx.SegmentIndex)
		return
	}
	e.fixed16(tx.ID)
	e.u64(tx.Revision)
	if tx.Operation == TransactionPulseCoordinator {
		e.u8(tx.RecoveryPulse)
	}
}

func decodeTransactionRequest(d *deccur) (TransactionRequest, error) {
	tx := TransactionRequest{Operation: TransactionOperation(d.u8())}
	if d.bad() {
		return TransactionRequest{}, d.why
	}
	if tx.Operation == TransactionNone {
		return TransactionRequest{}, errBadTransaction
	}
	if tx.Operation.stages() {
		tx.Record = d.slice()
	} else if tx.Operation.stagesManifestCoordinator() {
		tx.Record = d.slice()
		tx.ManifestSegment = d.slice()
	} else if tx.Operation.stagesManifestSegment() {
		tx.ID = distributedtxn.ID(d.fixed16())
		tx.ManifestSegment = d.slice()
	} else if tx.Operation.readsManifestSegment() {
		tx.ID = distributedtxn.ID(d.fixed16())
		tx.SegmentIndex = d.u32()
	} else {
		tx.ID = distributedtxn.ID(d.fixed16())
		tx.Revision = d.u64()
		if tx.Operation == TransactionPulseCoordinator {
			tx.RecoveryPulse = d.u8()
		}
	}
	if d.bad() {
		return TransactionRequest{}, d.why
	}
	if err := validateTransactionRequest(&tx, true); err != nil {
		return TransactionRequest{}, err
	}
	return tx, nil
}

func encodeTransactionReply(e *encbuf, tx TransactionReply) {
	e.u8(uint8(tx.Role))
	e.fixed16(tx.ID)
	e.u64(tx.Revision)
	e.u8(tx.RecoveryPulse)
	if tx.Role == TransactionRoleCoordinator {
		e.u8(uint8(tx.CoordinatorState))
	} else {
		e.u8(uint8(tx.TargetState))
	}
	e.u8(uint8(tx.RecordKind))
	e.u32(tx.SegmentIndex)
	e.bytes(tx.Record)
}

func decodeTransactionReply(d *deccur) (TransactionReply, error) {
	tx := TransactionReply{Role: TransactionRole(d.u8())}
	if tx.Role == TransactionRoleNone {
		return TransactionReply{}, errBadTransaction
	}
	tx.ID = distributedtxn.ID(d.fixed16())
	tx.Revision = d.u64()
	tx.RecoveryPulse = d.u8()
	state := d.u8()
	if tx.Role == TransactionRoleCoordinator {
		tx.CoordinatorState = distributedtxn.CoordinatorState(state)
	} else if tx.Role == TransactionRoleTarget {
		tx.TargetState = distributedtxn.TargetState(state)
	}
	tx.RecordKind = TransactionRecordKind(d.u8())
	tx.SegmentIndex = d.u32()
	tx.Record = d.slice()
	if d.bad() {
		return TransactionReply{}, d.why
	}
	if err := validateTransactionReply(tx); err != nil {
		return TransactionReply{}, err
	}
	return tx, nil
}

// EncodeRequest writes req as one framed message. It is deterministic: equal
// requests encode to identical bytes.
func EncodeRequest(w io.Writer, req *ShardRequest) error {
	if err := ValidateRequest(req); err != nil {
		return err
	}

	e := newFrameEncoder(len(req.Exchange.Batch.Data))
	e.u8(wireVersion)
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
		e.u8(uint8(p.Kind))
		switch p.Kind {
		case ParamBool:
			if p.Bool {
				e.u8(1)
			} else {
				e.u8(0)
			}
		case ParamNumber, ParamString, ParamDocument:
			e.bytes(p.Bytes)
		case ParamNull:
			// no payload
		}
	}
	if err := e.position(req.HasMinPosition, req.MinPosition); err != nil {
		return err
	}
	if len(req.AccessScopes) != 0 {
		e.u8(accessScopeMarker)
		e.u8(req.BucketBits)
		e.u32(uint32(len(req.AccessScopes)))
		for i := range req.AccessScopes {
			e.u32(req.AccessScopes[i].Start)
			e.u32(req.AccessScopes[i].End)
		}
	}
	if !req.ReadFenceID.IsZero() {
		e.u8(readFenceMarker)
		e.fixed16(req.ReadFenceID)
	}
	if req.GlobalIndexLookup.present() {
		lookup := req.GlobalIndexLookup
		e.u8(globalIndexLookupMarker)
		e.bytes(lookup.Relation)
		e.u64(lookup.IndexID)
		e.u64(lookup.Incarnation)
		e.u32(uint32(len(lookup.KeyTuples)))
		for i := range lookup.KeyTuples {
			e.bytes(lookup.KeyTuples[i])
		}
		e.u8(lookup.LocatorCount)
		if lookup.Unique {
			e.u8(1)
		} else {
			e.u8(0)
		}
	}
	if req.PrimaryKeyRead.present() {
		if req.PrimaryKeyRead.Relation != 0 || req.PrimaryKeyRead.MaxDocumentBytes != 0 {
			e.u8(primaryKeyReadExtendedMarker)
			e.u8(uint8(req.PrimaryKeyRead.Relation))
			e.u32(req.PrimaryKeyRead.MaxDocumentBytes)
		} else {
			e.u8(primaryKeyReadMarker)
		}
		e.bytes(req.PrimaryKeyRead.PrimaryPath)
		e.u32(uint32(len(req.PrimaryKeyRead.Keys)))
		for i := range req.PrimaryKeyRead.Keys {
			e.bytes(req.PrimaryKeyRead.Keys[i])
		}
	}
	if req.MutationCapture {
		e.u8(mutationCaptureMarker)
	}
	if req.DocumentScan.present() {
		e.u8(documentScanMarker)
		e.bytes(req.DocumentScan.Relation)
		e.bytes(req.DocumentScan.After)
	}
	if req.PartialAggregate {
		e.u8(partialAggregateMarker)
	}
	if req.RowBatch.present() {
		e.u8(rowBatchMarker)
		e.u32(req.RowBatch.BatchRows)
		e.u32(req.RowBatch.BatchBytes)
	}
	if req.Repartition.present() {
		e.u8(repartitionMarker)
		encodeRepartitionRequest(&e, req.Repartition)
	}
	if req.Exchange.present() {
		e.u8(exchangeMarker)
		encodeExchangeRequest(&e, req.Exchange)
	}
	if req.Transaction.Operation != TransactionNone {
		e.u8(transactionMarker)
		encodeTransactionRequest(&e, req.Transaction)
	}
	if req.Authority.Valid() {
		e.u8(authorityMarker)
		e.fixed16(req.Authority.Node)
		e.u64(req.Authority.Generation)
	}
	if len(req.ParamTypes) != 0 {
		e.u8(parameterTypesMarker)
		e.u32(uint32(len(req.ParamTypes)))
		for _, parameterType := range req.ParamTypes {
			e.u8(uint8(parameterType))
		}
	}
	if req.MutationImageCapture {
		e.u8(mutationImageMarker)
	}
	if e.err != nil {
		return e.err
	}
	return writeEncodedFrame(w, tagRequest, e.b)
}

// DecodeRequest reads one framed request. A malformed or oversized frame yields
// a typed error, never a panic or an out-of-memory allocation.
func DecodeRequest(r io.Reader) (*ShardRequest, error) {
	req := &ShardRequest{}
	if err := decodeBorrowedRequest(r, req); err != nil {
		return nil, err
	}
	return req, nil
}

// decodeBorrowedRequest fills a zeroed shell from one framed request. The
// shell must be zero on entry: absent optional lanes leave their fields
// untouched, so a reused shell would otherwise serve stale values.
// DecodeRequest allocates a fresh shell per call; the connection loop
// borrows zeroed shells from the request pool instead.
func decodeBorrowedRequest(r io.Reader, req *ShardRequest) error {
	body, err := readFrame(r, tagRequest)
	if err != nil {
		return err
	}
	d := deccur{b: body}
	if d.u8() != wireVersion {
		return errBadVersion
	}
	return decodeRequestFields(&d, req)
}

// decodeRequestFields fills a request shell from one decoded frame.
func decodeRequestFields(d *deccur, req *ShardRequest) error {
	var err error
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
		return errNegativeDuration
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
				return errBadEnum
			}
			p := Param{Kind: kind}
			switch kind {
			case ParamBool:
				marker := d.u8()
				if marker > 1 {
					return errBadEnum
				}
				p.Bool = marker == 1
			case ParamNumber, ParamString, ParamDocument:
				p.Bytes = d.slice()
			case ParamNull:
			}
			if !d.bad() && !p.Valid() {
				return errBadParam
			}
			req.Params[i] = p
		}
	}
	req.HasMinPosition, req.MinPosition, err = d.position()
	if err != nil {
		return err
	}
	if len(d.b) != 0 && d.b[0] == accessScopeMarker {
		d.u8()
		req.BucketBits = d.u8()
		count := d.count(8, distributedtxn.MaxIntentScopes)
		if count != 0 {
			req.AccessScopes = make([]distributedtxn.IntentScope, count)
			for i := range req.AccessScopes {
				req.AccessScopes[i] = distributedtxn.IntentScope{Start: d.u32(), End: d.u32()}
			}
		}
		if d.bad() || !distributedtxn.ValidateIntentScopes(req.AccessScopes, req.BucketBits) {
			return errBadTransaction
		}
	}
	if len(d.b) != 0 && d.b[0] == readFenceMarker {
		d.u8()
		req.ReadFenceID = distributedtxn.ID(d.fixed16())
		if d.bad() || req.ReadFenceID.IsZero() {
			return errBadTransaction
		}
	}
	if len(d.b) != 0 && d.b[0] == globalIndexLookupMarker {
		d.u8()
		req.GlobalIndexLookup.Relation = d.slice()
		req.GlobalIndexLookup.IndexID = d.u64()
		req.GlobalIndexLookup.Incarnation = d.u64()
		count := d.count(4, maxParams)
		if count != 0 {
			req.GlobalIndexLookup.KeyTuples = make([][]byte, count)
			for i := range req.GlobalIndexLookup.KeyTuples {
				req.GlobalIndexLookup.KeyTuples[i] = d.slice()
			}
		}
		req.GlobalIndexLookup.LocatorCount = d.u8()
		unique := d.u8()
		if unique > 1 {
			return errBadEnum
		}
		req.GlobalIndexLookup.Unique = unique == 1
		if d.bad() || !req.GlobalIndexLookup.canonical() {
			return errBadGlobalIndexLookup
		}
	}
	if len(d.b) != 0 && (d.b[0] == primaryKeyReadMarker || d.b[0] == primaryKeyReadExtendedMarker) {
		extended := d.b[0] == primaryKeyReadExtendedMarker
		d.u8()
		if extended {
			req.PrimaryKeyRead.Relation = replication.RelationID(d.u8())
			req.PrimaryKeyRead.MaxDocumentBytes = d.u32()
		}
		req.PrimaryKeyRead.PrimaryPath = d.slice()
		count := d.count(4, maxParams)
		if count != 0 {
			req.PrimaryKeyRead.Keys = make([][]byte, count)
			for i := range req.PrimaryKeyRead.Keys {
				req.PrimaryKeyRead.Keys[i] = d.slice()
				if len(req.PrimaryKeyRead.Keys[i]) == 0 {
					return errBadPrimaryKeyRead
				}
			}
		}
		if d.bad() || !req.PrimaryKeyRead.canonical() {
			return errBadPrimaryKeyRead
		}
	}
	if len(d.b) != 0 && d.b[0] == mutationCaptureMarker {
		d.u8()
		req.MutationCapture = true
	}
	if len(d.b) != 0 && d.b[0] == documentScanMarker {
		d.u8()
		req.DocumentScan.Relation = d.slice()
		req.DocumentScan.After = d.slice()
		if d.bad() || !req.DocumentScan.canonical() {
			return errBadDocumentScan
		}
	}
	if len(d.b) != 0 && d.b[0] == partialAggregateMarker {
		d.u8()
		req.PartialAggregate = true
	}
	if len(d.b) != 0 && d.b[0] == rowBatchMarker {
		d.u8()
		req.RowBatch.BatchRows = d.u32()
		req.RowBatch.BatchBytes = d.u32()
		if d.bad() || !req.RowBatch.canonical() {
			return errBadRowBatch
		}
	}
	if len(d.b) != 0 && d.b[0] == repartitionMarker {
		d.u8()
		req.Repartition, err = decodeRepartitionRequest(d)
		if err != nil {
			return err
		}
	}
	if len(d.b) != 0 && d.b[0] == exchangeMarker {
		d.u8()
		req.Exchange, err = decodeExchangeRequest(d)
		if err != nil {
			return err
		}
	}
	if len(d.b) != 0 && d.b[0] == transactionMarker {
		d.u8()
		req.Transaction, err = decodeTransactionRequest(d)
		if err != nil {
			return err
		}
	}
	if len(d.b) != 0 && d.b[0] == authorityMarker {
		d.u8()
		req.Authority.Node = d.fixed16()
		req.Authority.Generation = d.u64()
		if d.bad() || !req.Authority.Valid() {
			return errBadPresence
		}
	}
	if len(d.b) != 0 && d.b[0] == parameterTypesMarker {
		d.u8()
		count := d.count(1, maxParams)
		if count == 0 || count != len(req.Params) {
			return errBadParameterTypes
		}
		req.ParamTypes = make([]sqldriver.ParamType, count)
		for index := range req.ParamTypes {
			req.ParamTypes[index] = sqldriver.ParamType(d.u8())
		}
		if d.bad() || req.SQL == "" ||
			!validSQLParameterTypes(req.Params, req.ParamTypes) {
			return errBadParameterTypes
		}
	}
	if len(d.b) != 0 && d.b[0] == mutationImageMarker {
		d.u8()
		req.MutationImageCapture = true
	}
	if req.MutationCapture && req.MutationImageCapture {
		return errBadMutationCapture
	}
	mutationCapture := req.mutationCapturePresent()
	if req.Transaction.Operation != TransactionNone &&
		(len(req.ParamTypes) != 0 || !req.ReadFenceID.IsZero() || req.GlobalIndexLookup.present() || req.PrimaryKeyRead.present() || mutationCapture || req.DocumentScan.present()) {
		if req.GlobalIndexLookup.present() {
			return errBadGlobalIndexLookup
		}
		return errBadTransaction
	}
	if err := d.end(); err != nil {
		return err
	}
	if !policy.valid() {
		return errBadEnum
	}
	if !mode.valid() {
		return errBadEnum
	}
	if err := validateExchangeRequest(req); err != nil {
		return err
	}
	if err := validateRepartitionRequest(req); err != nil {
		return err
	}
	if req.GlobalIndexLookup.present() &&
		(req.SQL != "" || len(req.Params) != 0 || req.PrimaryKeyRead.present() || mutationCapture || req.DocumentScan.present() || req.ExecutionMode != ExecutionReadOnly) {
		return errBadGlobalIndexLookup
	}
	if req.PrimaryKeyRead.present() && (req.SQL == "" || mutationCapture || req.DocumentScan.present() || req.ExecutionMode != ExecutionReadOnly) {
		return errBadPrimaryKeyRead
	}
	if mutationCapture && (req.SQL == "" || req.ExecutionMode != ExecutionReadOnly || req.DocumentScan.present() ||
		!req.ReadFenceID.IsZero()) {
		return errBadMutationCapture
	}
	if req.DocumentScan.present() && (req.SQL != "" || len(req.Params) != 0 ||
		req.ExecutionMode != ExecutionReadOnly || !req.ReadFenceID.IsZero() ||
		req.MaxRows == 0 || req.MaxResultBytes == 0 || mutationCapture) {
		return errBadDocumentScan
	}
	if req.PartialAggregate && (req.SQL == "" || req.ExecutionMode != ExecutionReadOnly ||
		mutationCapture || req.DocumentScan.present() || req.GlobalIndexLookup.present() ||
		req.Transaction.Operation != TransactionNone) {
		return errBadPartialAggregate
	}
	if !req.RowBatch.canonical() || (req.RowBatch.present() &&
		(req.SQL == "" || req.ExecutionMode != ExecutionReadOnly || mutationCapture ||
			req.DocumentScan.present() || req.GlobalIndexLookup.present() ||
			req.Transaction.Operation != TransactionNone || req.MaxRows == 0 ||
			req.MaxResultBytes == 0)) {
		return errBadRowBatch
	}
	return nil
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
	if resp.HasReadPosition && resp.Kind != ResponseRows &&
		(resp.Kind != ResponseRowBatch || !resp.RowBatch.Final) {
		return errUnexpectedReadPosition
	}
	if err := validateTransactionReply(resp.Transaction); err != nil {
		return err
	}
	if resp.Kind == ResponseRowBatch && resp.Transaction.Role != TransactionRoleNone {
		return errBadRowBatch
	}
	if !resp.DocumentScan.canonical() ||
		(resp.DocumentScan.Present && resp.Kind != ResponseRows) {
		return errBadDocumentScan
	}
	if !resp.Exchange.canonical() {
		return errBadExchange
	}
	if resp.Exchange.present() && (resp.Kind != ResponseCompletion || resp.RowsAffected != 0 ||
		resp.HasReadPosition || resp.DocumentScan.Present ||
		resp.Transaction.Role != TransactionRoleNone || len(resp.Columns) != 0 || len(resp.Rows) != 0) {
		return errBadExchange
	}

	e := newFrameEncoder(len(resp.Exchange.Batch.Data))
	e.u8(wireVersion)
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
	case ResponseRowBatch:
		batch := resp.RowBatch
		if batch.ColumnCount == 0 || batch.ColumnCount > maxColumns ||
			len(resp.Rows) > int(MaxRowBatchRows) ||
			uint64(len(resp.Rows))*uint64(batch.ColumnCount) > uint64(MaxRowBatchCells) ||
			(!batch.Final && len(resp.Rows) == 0) {
			return errBadRowBatch
		}
		if batch.Sequence == 0 {
			if len(resp.Columns) != int(batch.ColumnCount) {
				return errBadRowBatch
			}
		} else if len(resp.Columns) != 0 {
			return errBadRowBatch
		}
		e.u32(batch.Sequence)
		if batch.Final {
			e.u8(1)
		} else {
			e.u8(0)
		}
		e.u32(batch.ColumnCount)
		if batch.Sequence == 0 {
			for i := range resp.Columns {
				e.str(resp.Columns[i].Name)
				e.u32(uint32(resp.Columns[i].TypeOID))
			}
		}
		e.u32(uint32(len(resp.Rows)))
		for i := range resp.Rows {
			if len(resp.Rows[i]) != int(batch.ColumnCount) {
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
	if resp.DocumentScan.Present {
		e.u8(documentScanMarker)
		if resp.DocumentScan.Complete {
			e.u8(1)
		} else {
			e.u8(0)
		}
		e.bytes(resp.DocumentScan.Next)
	}
	if resp.Exchange.present() {
		e.u8(exchangeMarker)
		encodeExchangeReply(&e, resp.Exchange)
	}
	if resp.Transaction.Role != TransactionRoleNone {
		e.u8(transactionMarker)
		encodeTransactionReply(&e, resp.Transaction)
	}
	if e.err != nil {
		return e.err
	}
	return writeEncodedFrame(w, tagResponse, e.b)
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
	if d.u8() != wireVersion {
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
			// d.count proved that nr*nc fits in the bounded frame. Retain
			// one cell slab and the owned frame, as for ResponseRowBatch.
			cells := make([]Cell, nr*nc)
			for i := 0; i < nr; i++ {
				start := i * nc
				row := cells[start : start+nc : start+nc]
				for j := 0; j < nc; j++ {
					marker := d.u8()
					if marker > 1 {
						return nil, errBadEnum
					}
					if marker == 1 {
						row[j] = Cell{Null: true}
					} else {
						payload := d.slice()
						row[j] = Cell{Bytes: payload[:len(payload):len(payload)]}
					}
				}
				resp.Rows[i] = row
				if d.bad() {
					break
				}
			}
		}
	case ResponseRowBatch:
		resp.RowBatch.Sequence = d.u32()
		final := d.u8()
		if final > 1 {
			return nil, errBadEnum
		}
		resp.RowBatch.Final = final == 1
		nc32 := d.u32()
		if nc32 == 0 || nc32 > maxColumns {
			return nil, errBadRowBatch
		}
		nc := int(nc32)
		resp.RowBatch.ColumnCount = nc32
		if resp.RowBatch.Sequence == 0 {
			resp.Columns = make([]Column, nc)
			for i := range resp.Columns {
				resp.Columns[i].Name = d.str()
				resp.Columns[i].TypeOID = int32(d.u32())
			}
		}
		nr := d.count(nc, maxRows)
		if d.bad() || nr > int(MaxRowBatchRows) ||
			uint64(nr)*uint64(nc) > uint64(MaxRowBatchCells) ||
			(!resp.RowBatch.Final && nr == 0) {
			return nil, errBadRowBatch
		}
		if nr != 0 {
			resp.Rows = make([][]Cell, nr)
			cells := make([]Cell, nr*nc)
			for i := range resp.Rows {
				start := i * nc
				row := cells[start : start+nc : start+nc]
				for j := range row {
					marker := d.u8()
					if marker > 1 {
						return nil, errBadEnum
					}
					if marker == 1 {
						row[j] = Cell{Null: true}
					} else {
						// The response owns this bounded frame through its cell
						// slices, avoiding one payload allocation per cell.
						row[j] = Cell{Bytes: d.slice()}
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
	if resp.HasReadPosition && kind != ResponseRows &&
		(kind != ResponseRowBatch || !resp.RowBatch.Final) {
		return nil, errUnexpectedReadPosition
	}
	if len(d.b) != 0 && d.b[0] == documentScanMarker {
		d.u8()
		complete := d.u8()
		if complete > 1 {
			return nil, errBadEnum
		}
		resp.DocumentScan = DocumentScanReply{
			Present: true, Complete: complete == 1, Next: d.bytesCopy(),
		}
		if d.bad() || !resp.DocumentScan.canonical() || kind != ResponseRows {
			return nil, errBadDocumentScan
		}
	}
	if len(d.b) != 0 && d.b[0] == exchangeMarker {
		d.u8()
		resp.Exchange, err = decodeExchangeReply(&d)
		if err != nil {
			return nil, err
		}
		if kind != ResponseCompletion || resp.RowsAffected != 0 || resp.HasReadPosition ||
			resp.DocumentScan.Present {
			return nil, errBadExchange
		}
	}
	if len(d.b) != 0 && d.b[0] == transactionMarker {
		d.u8()
		resp.Transaction, err = decodeTransactionReply(&d)
		if err != nil {
			return nil, err
		}
	}
	if resp.Exchange.present() && resp.Transaction.Role != TransactionRoleNone {
		return nil, errBadExchange
	}
	if err := d.end(); err != nil {
		return nil, err
	}
	return resp, nil
}
