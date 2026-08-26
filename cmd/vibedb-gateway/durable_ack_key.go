package main

import (
	"errors"
	"os"

	"github.com/thesyncim/vibedb/gateway"
)

var errInvalidDurableAckKey = errors.New("gateway: durable ACK derivation key must be exactly 64 lowercase hexadecimal bytes")

// loadDurableAckKey reads the cluster-shared terminal capability root. The
// format is deliberately fixed and newline-free so every gateway derives the
// same bytes and malformed secret-management output fails closed.
func loadDurableAckKey(path string) (gateway.DurableRequestAckDerivationKey, error) {
	if path == "" {
		return gateway.DurableRequestAckDerivationKey{}, errInvalidDurableAckKey
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return gateway.DurableRequestAckDerivationKey{}, err
	}
	var key gateway.DurableRequestAckDerivationKey
	if len(raw) != len(key)*2 {
		return gateway.DurableRequestAckDerivationKey{}, errInvalidDurableAckKey
	}
	for index := range key {
		high, highOK := lowerHexNibble(raw[index*2])
		low, lowOK := lowerHexNibble(raw[index*2+1])
		if !highOK || !lowOK {
			return gateway.DurableRequestAckDerivationKey{}, errInvalidDurableAckKey
		}
		key[index] = high<<4 | low
	}
	if key == (gateway.DurableRequestAckDerivationKey{}) {
		return gateway.DurableRequestAckDerivationKey{}, errInvalidDurableAckKey
	}
	return key, nil
}

func lowerHexNibble(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	default:
		return 0, false
	}
}
