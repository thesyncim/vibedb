package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	MaxRouteGateReleaseCommandBytes    = 1113
	MaxRouteGateReleaseCompletionBytes = 1185
	MaxAckGCDeleteRows                 = 256
	gcRequestHeaderBytes               = 88
	MaxGCRequestBytes                  = gcRequestHeaderBytes +
		MaxRouteGateReleaseCommandBytes + MaxRouteGateReleaseCompletionBytes + checksumBytes
)

var (
	gcRequestMagic           = [4]byte{'V', 'R', 'L', 'G'}
	releaseCertificateDomain = []byte("vibedb/request-ledger/release-certificate\x00")
)

type GCAction uint8

const (
	GCActionInvalid GCAction = iota
	GCActionReleasePin
	GCActionCollect
)

// GCRequest carries either the exact bounded routegate evidence to be verified
// by replicated-state, or a bounded collection budget. It never carries a
// caller-selected cursor or reclaimed-byte count: those are derived from rows.
type GCRequest struct {
	Action            GCAction
	ExpectedAckDigest Digest
	MaxRows           uint16
	MaxBytes          uint32
	ReleaseCommand    []byte
	ReleaseCompletion []byte
}

func NewReleasePinRequest(expectedAck Digest, command, completion []byte) (GCRequest, error) {
	r := GCRequest{Action: GCActionReleasePin, ExpectedAckDigest: expectedAck,
		ReleaseCommand: command, ReleaseCompletion: completion}
	if err := validateGCRequest(r); err != nil {
		return GCRequest{}, err
	}
	return r, nil
}

func NewCollectRequest(expectedAck Digest, maxRows uint16, maxBytes uint32) (GCRequest, error) {
	r := GCRequest{Action: GCActionCollect, ExpectedAckDigest: expectedAck,
		MaxRows: maxRows, MaxBytes: maxBytes}
	if err := validateGCRequest(r); err != nil {
		return GCRequest{}, err
	}
	return r, nil
}

func (request GCRequest) ReleaseCertificateDigest() Digest {
	if request.Action != GCActionReleasePin {
		return Digest{}
	}
	commandDigest := sha256.Sum256(request.ReleaseCommand)
	completionDigest := sha256.Sum256(request.ReleaseCompletion)
	const domain = "vibedb/request-ledger/release-certificate\x00"
	var framed [len(domain) + 16 + 2*sha256.Size]byte
	at := copy(framed[:], releaseCertificateDomain)
	binary.LittleEndian.PutUint64(framed[at:at+8], uint64(len(request.ReleaseCommand)))
	binary.LittleEndian.PutUint64(framed[at+8:at+16], uint64(len(request.ReleaseCompletion)))
	at += 16
	at += copy(framed[at:], commandDigest[:])
	copy(framed[at:], completionDigest[:])
	return Digest(sha256.Sum256(framed[:]))
}

func AppendGCRequest(dst []byte, request GCRequest) ([]byte, error) {
	if err := validateGCRequest(request); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, gcRequestHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], gcRequestMagic[:])
	out[8] = byte(request.Action)
	binary.LittleEndian.PutUint16(out[10:12], request.MaxRows)
	binary.LittleEndian.PutUint32(out[12:16], request.MaxBytes)
	binary.LittleEndian.PutUint32(out[16:20], uint32(len(request.ReleaseCommand)))
	binary.LittleEndian.PutUint32(out[20:24], uint32(len(request.ReleaseCompletion)))
	putDigest(out[24:56], request.ExpectedAckDigest)
	putDigest(out[56:88], request.ReleaseCertificateDigest())
	dst = append(dst, request.ReleaseCommand...)
	dst = append(dst, request.ReleaseCompletion...)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenGCRequest(raw []byte) (GCRequest, error) {
	if len(raw) < gcRequestHeaderBytes+checksumBytes || len(raw) > MaxGCRequestBytes ||
		!magicOK(raw, gcRequestMagic) || !zeroBytes(raw[4:8]) || raw[9] != 0 ||
		!checksumOK(raw) {
		return GCRequest{}, ErrCorrupt
	}
	commandBytes := binary.LittleEndian.Uint32(raw[16:20])
	completionBytes := binary.LittleEndian.Uint32(raw[20:24])
	want, ok := exactLength(gcRequestHeaderBytes+checksumBytes, uint64(commandBytes), uint64(completionBytes))
	if !ok || want != len(raw) {
		return GCRequest{}, ErrCorrupt
	}
	commandEnd := gcRequestHeaderBytes + int(commandBytes)
	request := GCRequest{
		Action: GCAction(raw[8]), MaxRows: binary.LittleEndian.Uint16(raw[10:12]),
		MaxBytes:          binary.LittleEndian.Uint32(raw[12:16]),
		ExpectedAckDigest: readDigest(raw[24:56]),
		ReleaseCommand:    raw[gcRequestHeaderBytes:commandEnd:commandEnd],
		ReleaseCompletion: raw[commandEnd : len(raw)-checksumBytes : len(raw)-checksumBytes],
	}
	if err := validateGCRequest(request); err != nil ||
		readDigest(raw[56:88]) != request.ReleaseCertificateDigest() {
		return GCRequest{}, ErrCorrupt
	}
	return request, nil
}

func validateGCRequest(request GCRequest) error {
	if !nonzeroDigest(request.ExpectedAckDigest) {
		return ErrCorrupt
	}
	switch request.Action {
	case GCActionReleasePin:
		if request.MaxRows != 0 || request.MaxBytes != 0 ||
			len(request.ReleaseCommand) == 0 || len(request.ReleaseCommand) > MaxRouteGateReleaseCommandBytes ||
			len(request.ReleaseCompletion) == 0 || len(request.ReleaseCompletion) > MaxRouteGateReleaseCompletionBytes {
			return ErrCorrupt
		}
	case GCActionCollect:
		if request.MaxRows == 0 || request.MaxRows > MaxAckGCDeleteRows || request.MaxBytes == 0 ||
			len(request.ReleaseCommand) != 0 || len(request.ReleaseCompletion) != 0 {
			return ErrCorrupt
		}
	default:
		return ErrCorrupt
	}
	return nil
}
