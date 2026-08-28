// Package manifestgen derives opaque build-compatibility identities from the
// sole canonical wire/disk manifest. It is cold generator and verification
// machinery, never part of a serving codec.
package manifestgen

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const requestLedgerToken = "$REQUEST_LEDGER_SEMANTICS"

var (
	wireDomain = []byte("vibedb/buildgate/wire-manifest\x00")
	diskDomain = []byte("vibedb/buildgate/disk-manifest\x00")
)

// Derive validates the canonical line grammar, substitutes the exact request
// ledger semantics digest in both domains, and derives one opaque 128-bit
// identity per domain. Lines must be strictly ordered within the disk block
// followed by the wire block; formatting therefore cannot have aliases.
func Derive(raw []byte, requestLedger [sha256.Size]byte) (wire, disk [16]byte, err error) {
	if len(raw) == 0 || raw[len(raw)-1] != '\n' || requestLedger == ([sha256.Size]byte{}) {
		return wire, disk, errors.New("buildgate manifest: invalid envelope")
	}
	hex := lowerHex(requestLedger)
	lines := bytes.Split(raw[:len(raw)-1], []byte{'\n'})
	wireLines := make([][]byte, 0, len(lines))
	diskLines := make([][]byte, 0, len(lines))
	var lastDisk, lastWire []byte
	wireTokens, diskTokens := 0, 0
	seenWire := false
	for _, line := range lines {
		if !canonicalLine(line) {
			return wire, disk, errors.New("buildgate manifest: invalid line")
		}
		var body []byte
		var target *[][]byte
		if bytes.HasPrefix(line, []byte("disk ")) {
			if seenWire {
				return wire, disk, errors.New("buildgate manifest: disk entry follows wire block")
			}
			body, target = line[5:], &diskLines
			if lastDisk != nil && bytes.Compare(lastDisk, body) >= 0 {
				return wire, disk, errors.New("buildgate manifest: disk entries are not strictly ordered")
			}
			lastDisk = body
			diskTokens += bytes.Count(body, []byte(requestLedgerToken))
		} else if bytes.HasPrefix(line, []byte("wire ")) {
			seenWire = true
			body, target = line[5:], &wireLines
			if lastWire != nil && bytes.Compare(lastWire, body) >= 0 {
				return wire, disk, errors.New("buildgate manifest: wire entries are not strictly ordered")
			}
			lastWire = body
			wireTokens += bytes.Count(body, []byte(requestLedgerToken))
		} else {
			return wire, disk, errors.New("buildgate manifest: unknown domain")
		}
		expanded := bytes.ReplaceAll(body, []byte(requestLedgerToken), hex[:])
		*target = append(*target, expanded)
	}
	if len(wireLines) == 0 || len(diskLines) == 0 || wireTokens != 1 || diskTokens != 1 {
		return wire, disk, errors.New("buildgate manifest: incomplete domain")
	}
	return derive(wireDomain, wireLines), derive(diskDomain, diskLines), nil
}

func canonicalLine(line []byte) bool {
	if len(line) == 0 || bytes.Count(line, []byte{'='}) == 0 {
		return false
	}
	for _, value := range line {
		if value < 0x20 || value > 0x7e {
			return false
		}
	}
	return true
}

func derive(domain []byte, lines [][]byte) (identity [16]byte) {
	hash := sha256.New()
	_, _ = hash.Write(domain)
	var length [4]byte
	for _, line := range lines {
		binary.BigEndian.PutUint32(length[:], uint32(len(line)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(line)
	}
	var sum [sha256.Size]byte
	_ = hash.Sum(sum[:0])
	copy(identity[:], sum[:len(identity)])
	return identity
}

func lowerHex(value [sha256.Size]byte) (out [sha256.Size * 2]byte) {
	const alphabet = "0123456789abcdef"
	for index, octet := range value {
		out[index*2] = alphabet[octet>>4]
		out[index*2+1] = alphabet[octet&0x0f]
	}
	return out
}
