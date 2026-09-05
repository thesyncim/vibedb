package raftstore

import (
	"bytes"
	"crypto/subtle"
)

// AuthenticatedWrappedKeyMetadata returns the retained provider metadata only
// when key opens this exact live node log. A child log can reuse an existing
// provider key without inventing wrapped metadata or retaining another
// plaintext key copy.
func (store *NodeStore) AuthenticatedWrappedKeyMetadata(key Key) ([]byte, error) {
	if store == nil {
		return nil, ErrClosed
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.usable(); err != nil {
		return nil, err
	}
	if err := validateKey(key, false); err != nil {
		return nil, err
	}
	if key.ID != store.key.ID || (key.Wrapped != nil && !bytes.Equal(key.Wrapped, store.key.Wrapped)) {
		return nil, ErrKeyMismatch
	}
	crypto, err := makeFileCrypto(key, store.engine.LogID())
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare(crypto.dataKey[:], store.crypto.dataKey[:]) != 1 ||
		subtle.ConstantTimeCompare(crypto.nonceKey[:], store.crypto.nonceKey[:]) != 1 {
		return nil, ErrKeyMismatch
	}
	return bytes.Clone(store.key.Wrapped), nil
}
