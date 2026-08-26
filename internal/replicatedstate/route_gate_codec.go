package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/vibedb/internal/routegate"
)

const (
	routeGateHeadPrefix   byte = 0x20
	routeGatePinPrefix    byte = 0x21
	routeGateResultPrefix byte = 0x22

	routeGatePinKeyBytes    = 1 + sha256.Size
	routeGateResultKeyBytes = 1 + sha256.Size + 2
	routeGateResultBytes    = 224
)

var (
	routeGateHeadKey = []byte{routeGateHeadPrefix}

	routeGateResultMagic = [8]byte{'V', 'D', 'B', 'R', 'G', 'R', 'E', 'S'}
)

const routeGateResultChecksumDomain = "vibedb/replicated-state/route-gate-result-checksum\x00"

type routeGateResultRecord struct {
	SessionDigest  [sha256.Size]byte
	Slot           uint16
	ClientEpoch    uint64
	ClientSequence uint64
	Outcome        routegate.Outcome
}

func routeGatePinStorageKey(identity routegate.Identity) ([routeGatePinKeyBytes]byte, error) {
	var key [routeGatePinKeyBytes]byte
	if identity == (routegate.Identity{}) {
		return key, routegate.ErrCorrupt
	}
	key[0] = routeGatePinPrefix
	copy(key[1:], identity[:])
	return key, nil
}

func routeGateResultStorageKey(
	session [sha256.Size]byte,
	slot uint16,
) ([routeGateResultKeyBytes]byte, error) {
	var key [routeGateResultKeyBytes]byte
	if session == ([sha256.Size]byte{}) || slot >= MaxSessionRetryWindow {
		return key, ErrSessionCorrupt
	}
	key[0] = routeGateResultPrefix
	copy(key[1:1+sha256.Size], session[:])
	binary.BigEndian.PutUint16(key[1+sha256.Size:], slot)
	return key, nil
}

func appendRouteGateResult(dst []byte, record routeGateResultRecord) ([]byte, error) {
	if record.SessionDigest == ([sha256.Size]byte{}) || record.Slot >= MaxSessionRetryWindow ||
		record.ClientEpoch == 0 || record.ClientSequence == 0 {
		return dst, ErrSessionCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, routeGateResultBytes)...)
	frame := dst[start:]
	copy(frame[:8], routeGateResultMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], uint16(routeGateResultBytes))
	binary.LittleEndian.PutUint16(frame[10:12], record.Slot)
	copy(frame[16:48], record.SessionDigest[:])
	binary.LittleEndian.PutUint64(frame[48:56], record.ClientEpoch)
	binary.LittleEndian.PutUint64(frame[56:64], record.ClientSequence)
	encoded, err := routegate.AppendOutcome(frame[64:64], record.Outcome)
	if err != nil || len(encoded) != routegate.OutcomeBytes {
		return dst[:start], ErrSessionCorrupt
	}
	sealRouteGateResult(frame)
	return dst, nil
}

func openRouteGateResult(raw []byte) (routeGateResultRecord, error) {
	if len(raw) != routeGateResultBytes || !bytes.Equal(raw[:8], routeGateResultMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != routeGateResultBytes ||
		binary.LittleEndian.Uint32(raw[12:16]) != 0 ||
		!allZero(raw[64+routegate.OutcomeBytes:routeGateResultBytes-recordChecksumLen]) ||
		!verifyRouteGateResult(raw) {
		return routeGateResultRecord{}, fmt.Errorf("%w: route-gate result", ErrSessionCorrupt)
	}
	record := routeGateResultRecord{
		Slot:           binary.LittleEndian.Uint16(raw[10:12]),
		ClientEpoch:    binary.LittleEndian.Uint64(raw[48:56]),
		ClientSequence: binary.LittleEndian.Uint64(raw[56:64]),
	}
	copy(record.SessionDigest[:], raw[16:48])
	outcome, err := routegate.OpenOutcome(raw[64 : 64+routegate.OutcomeBytes])
	if err != nil || record.SessionDigest == ([sha256.Size]byte{}) ||
		record.Slot >= MaxSessionRetryWindow || record.ClientEpoch == 0 || record.ClientSequence == 0 {
		return routeGateResultRecord{}, fmt.Errorf("%w: route-gate result semantics", ErrSessionCorrupt)
	}
	record.Outcome = outcome
	return record, nil
}

func sealRouteGateResult(frame []byte) {
	var material [len(routeGateResultChecksumDomain) + routeGateResultBytes - recordChecksumLen]byte
	cursor := copy(material[:], routeGateResultChecksumDomain)
	copy(material[cursor:], frame[:routeGateResultBytes-recordChecksumLen])
	digest := sha256.Sum256(material[:])
	copy(frame[routeGateResultBytes-recordChecksumLen:], digest[:])
}

func verifyRouteGateResult(frame []byte) bool {
	if len(frame) != routeGateResultBytes {
		return false
	}
	var material [len(routeGateResultChecksumDomain) + routeGateResultBytes - recordChecksumLen]byte
	cursor := copy(material[:], routeGateResultChecksumDomain)
	copy(material[cursor:], frame[:routeGateResultBytes-recordChecksumLen])
	digest := sha256.Sum256(material[:])
	return bytes.Equal(digest[:], frame[routeGateResultBytes-recordChecksumLen:])
}
