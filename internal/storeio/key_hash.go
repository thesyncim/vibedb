package storeio

import (
	"encoding/binary"
	"math/bits"
)

// KeyHash returns the deterministic 64-bit keyed hash of a document key. It is
// SipHash-1-3 with its 128-bit key derived from StoreID. Hashes only prune;
// leaves still verify complete key bytes. It is the shared key-hash the ordered
// primary graph — leaves, anchors, tablet routers, and the exact-index codec —
// all route with, so it lives on its own rather than beside any one consumer.
func KeyHash(storeID [16]byte, key string) uint64 {
	k0 := binary.LittleEndian.Uint64(storeID[0:8])
	k1 := binary.LittleEndian.Uint64(storeID[8:16])
	v0 := k0 ^ 0x736f6d6570736575
	v1 := k1 ^ 0x646f72616e646f6d
	v2 := k0 ^ 0x6c7967656e657261
	v3 := k1 ^ 0x7465646279746573

	i := 0
	for ; i+8 <= len(key); i += 8 {
		m := uint64(key[i]) | uint64(key[i+1])<<8 | uint64(key[i+2])<<16 | uint64(key[i+3])<<24 |
			uint64(key[i+4])<<32 | uint64(key[i+5])<<40 | uint64(key[i+6])<<48 | uint64(key[i+7])<<56
		v3 ^= m
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
		v0 ^= m
	}
	b := uint64(len(key)) << 56
	for j := i; j < len(key); j++ {
		b |= uint64(key[j]) << uint(8*(j-i))
	}
	v3 ^= b
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0 ^= b
	v2 ^= 0xff
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	return v0 ^ v1 ^ v2 ^ v3
}

// KeyHashBytes is KeyHash for an already materialized key. It deliberately
// duplicates the eight-byte load loop rather than converting key to string:
// batch writers retain keys in byte arenas, and hashing them must not allocate.
func KeyHashBytes(storeID [16]byte, key []byte) uint64 {
	k0 := binary.LittleEndian.Uint64(storeID[0:8])
	k1 := binary.LittleEndian.Uint64(storeID[8:16])
	v0 := k0 ^ 0x736f6d6570736575
	v1 := k1 ^ 0x646f72616e646f6d
	v2 := k0 ^ 0x6c7967656e657261
	v3 := k1 ^ 0x7465646279746573

	i := 0
	for ; i+8 <= len(key); i += 8 {
		m := binary.LittleEndian.Uint64(key[i : i+8])
		v3 ^= m
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
		v0 ^= m
	}
	b := uint64(len(key)) << 56
	for j := i; j < len(key); j++ {
		b |= uint64(key[j]) << uint(8*(j-i))
	}
	v3 ^= b
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0 ^= b
	v2 ^= 0xff
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	return v0 ^ v1 ^ v2 ^ v3
}

// sipRound passes state by value so the compiler keeps the whole permutation
// in registers and inlines it. The former pointer-parameter spelling exceeded
// the inlining budget: every round paid a call plus four spilled words each
// way, which the point-read profile measured as the single largest cost of a
// warm read (~84ns of SipHash for a 14-byte key against ~20ns inlined).
func sipRound(v0, v1, v2, v3 uint64) (uint64, uint64, uint64, uint64) {
	v0 += v1
	v1 = bits.RotateLeft64(v1, 13)
	v1 ^= v0
	v0 = bits.RotateLeft64(v0, 32)
	v2 += v3
	v3 = bits.RotateLeft64(v3, 16)
	v3 ^= v2
	v0 += v3
	v3 = bits.RotateLeft64(v3, 21)
	v3 ^= v0
	v2 += v1
	v1 = bits.RotateLeft64(v1, 17)
	v1 ^= v2
	v2 = bits.RotateLeft64(v2, 32)
	return v0, v1, v2, v3
}
