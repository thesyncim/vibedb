// Package schemachange implements the bounded change stream used to reconcile
// unpublished schema images. Capture is not permission to activate an image.
package schemachange

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

var ErrCapture = errors.New("schemachange: invalid capture")

const (
	headerBytes       = 304
	entryBytes        = 296
	mutationBytes     = 48
	maxMutations      = replication.MaxRelationsPerBundle * replicatedstate.MaxDistinctMutations
	MaxCaptureRecords = 1 << 20
	MaxCaptureBytes   = 1 << 30
)

var captureMagic = [8]byte{'V', 'D', 'B', 'S', 'D', 'L', 0, 0}
var captureDomain = []byte("vibedb/schema-change/capture\x00")

// AbortReason is durable and never authorizes schema publication.
type AbortReason uint8

const (
	NotAborted AbortReason = iota
	AbortCapacity
	AbortSourceChanged
)

// CaptureConfig is immutable for the operation. Limits include the terminal
// seal/abort entry; MaxBytes counts raw encoded bytes, including the header,
// not filesystem allocation. One terminal entry is always reserved, so
// exhaustion cannot poison a foreground write.
type CaptureConfig struct {
	Operation, PlanDigest, BindingDigest, ManifestDigest [32]byte
	SchemaGeneration, MaxRecords, MaxBytes               uint64
}

func (c CaptureConfig) valid() bool {
	return c.Operation != [32]byte{} && c.PlanDigest != [32]byte{} &&
		c.BindingDigest != [32]byte{} && c.ManifestDigest != [32]byte{} &&
		c.SchemaGeneration != 0 && c.SchemaGeneration != math.MaxUint64 &&
		c.MaxRecords != 0 && c.MaxRecords <= MaxCaptureRecords &&
		c.MaxBytes >= headerBytes+entryBytes && c.MaxBytes <= MaxCaptureBytes
}

// Validate checks the canonical operation identity and bounded retention
// configuration without opening storage or granting capture authority.
func (c CaptureConfig) Validate() error {
	if !c.valid() {
		return ErrCapture
	}
	return nil
}

// Publication is the exact source cut on one side of a change record.
type Publication struct {
	Applied, Term, Ownership, Routing, Route uint64
	EntryDigest, DataDigest                  [32]byte
}

func (p Publication) valid() bool {
	return p.Applied != 0 && p.Applied != math.MaxUint64 && p.Term != 0 && p.Term != math.MaxUint64 &&
		p.Ownership != 0 && p.Routing != 0 && p.Route != 0 && p.EntryDigest != [32]byte{} && p.DataDigest != [32]byte{}
}

func statePublication(s replicatedstate.State) Publication {
	return Publication{s.Applied, s.LastTerm, s.Binding.OwnershipEpoch, s.Binding.RoutingVersion,
		s.Binding.RouteGeneration, s.LastEntryDigest, s.DataChainDigest}
}

// Mutation carries a fixed-size exact before witness and borrowed after bytes.
// Presence is explicit: an absent row is distinct from an empty opaque value.
type Mutation struct {
	Relation                    replication.RelationID
	Key, After                  []byte
	BeforePresent, AfterPresent bool
	BeforeBytes                 uint32
	BeforeDigest                [32]byte
}

func (m Mutation) MatchesBefore(value []byte, found bool) bool {
	return found == m.BeforePresent && (!found || uint64(len(value)) == uint64(m.BeforeBytes) && sha256.Sum256(value) == m.BeforeDigest)
}

type Entry struct {
	Before, After          Publication
	PreviousDigest, Digest [32]byte
	Abort                  AbortReason
	Mutations              []Mutation
}

// CaptureWorkspace is serial caller-owned scratch. Entries borrow its buffers
// until the next read. Reuse it to avoid allocating a descriptor per mutation.
type CaptureWorkspace struct {
	raw       []byte
	mutations []Mutation
}

func (w *CaptureWorkspace) clearMutations() {
	clear(w.mutations)
	w.mutations = w.mutations[:0]
}

func recordDigest(raw []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(captureDomain)
	_, _ = h.Write(raw)
	var digest [32]byte
	_ = h.Sum(digest[:0])
	return digest
}

func putEnvelope(raw []byte, kind byte, abort AbortReason) {
	copy(raw[:8], captureMagic[:])
	// Bytes 8:10 are a zero grammar sentinel, not a compatibility version.
	raw[10], raw[11] = kind, byte(abort)
	binary.LittleEndian.PutUint32(raw[12:16], uint32(len(raw)))
}

func validEnvelope(raw []byte, kind byte, minimum int) bool {
	return len(raw) >= minimum && len(raw) <= replicatedstate.MaxTransitionCaptureRecordBytes &&
		bytes.Equal(raw[:8], captureMagic[:]) && raw[8] == 0 && raw[9] == 0 && raw[10] == kind &&
		uint64(binary.LittleEndian.Uint32(raw[12:16])) == uint64(len(raw)) &&
		recordDigest(raw[:len(raw)-32]) == [32]byte(raw[len(raw)-32:])
}

func putPublication(raw []byte, p Publication) {
	for i, v := range [...]uint64{p.Applied, p.Term, p.Ownership, p.Routing, p.Route} {
		binary.LittleEndian.PutUint64(raw[i*8:], v)
	}
	copy(raw[40:72], p.EntryDigest[:])
	copy(raw[72:104], p.DataDigest[:])
}

func openPublication(raw []byte) Publication {
	return Publication{binary.LittleEndian.Uint64(raw), binary.LittleEndian.Uint64(raw[8:]),
		binary.LittleEndian.Uint64(raw[16:]), binary.LittleEndian.Uint64(raw[24:]), binary.LittleEndian.Uint64(raw[32:]),
		[32]byte(raw[40:72]), [32]byte(raw[72:104])}
}

func appendHeader(dst []byte, config CaptureConfig, base Publication) ([]byte, error) {
	if !config.valid() || !base.valid() {
		return dst, ErrCapture
	}
	start := len(dst)
	dst = append(dst, make([]byte, headerBytes)...)
	raw := dst[start:]
	putEnvelope(raw, 1, NotAborted)
	for i, digest := range [][32]byte{config.Operation, config.PlanDigest, config.BindingDigest, config.ManifestDigest} {
		copy(raw[16+i*32:], digest[:])
	}
	putPublication(raw[144:248], base)
	for i, value := range [...]uint64{config.SchemaGeneration, config.MaxRecords, config.MaxBytes} {
		binary.LittleEndian.PutUint64(raw[248+i*8:], value)
	}
	digest := recordDigest(raw[:headerBytes-32])
	copy(raw[headerBytes-32:], digest[:])
	return dst, nil
}

func openHeader(raw []byte) (CaptureConfig, Publication, [32]byte, error) {
	if len(raw) != headerBytes || !validEnvelope(raw, 1, headerBytes) || raw[11] != 0 {
		return CaptureConfig{}, Publication{}, [32]byte{}, ErrCapture
	}
	config := CaptureConfig{[32]byte(raw[16:48]), [32]byte(raw[48:80]), [32]byte(raw[80:112]), [32]byte(raw[112:144]),
		binary.LittleEndian.Uint64(raw[248:]), binary.LittleEndian.Uint64(raw[256:]), binary.LittleEndian.Uint64(raw[264:])}
	base := openPublication(raw[144:248])
	if !config.valid() || !base.valid() {
		return CaptureConfig{}, Publication{}, [32]byte{}, ErrCapture
	}
	return config, base, [32]byte(raw[272:]), nil
}

func appendSeal(dst []byte, cursor Cursor) ([]byte, error) {
	if !cursor.Publication.valid() || cursor.Digest == [32]byte{} {
		return dst, ErrCapture
	}
	start := len(dst)
	dst = append(dst, make([]byte, entryBytes)...)
	raw := dst[start:]
	putEnvelope(raw, 3, NotAborted)
	putPublication(raw[24:128], cursor.Publication)
	putPublication(raw[128:232], cursor.Publication)
	copy(raw[232:264], cursor.Digest[:])
	digest := recordDigest(raw[:entryBytes-32])
	copy(raw[entryBytes-32:], digest[:])
	return dst, nil
}

func openSeal(raw []byte, cursor Cursor) ([32]byte, error) {
	if len(raw) != entryBytes || !validEnvelope(raw, 3, entryBytes) || raw[11] != 0 ||
		binary.LittleEndian.Uint64(raw[16:24]) != 0 || !cursor.Publication.valid() ||
		openPublication(raw[24:128]) != cursor.Publication || openPublication(raw[128:232]) != cursor.Publication ||
		[32]byte(raw[232:264]) != cursor.Digest || cursor.Digest == [32]byte{} {
		return [32]byte{}, ErrCapture
	}
	return [32]byte(raw[264:]), nil
}

func recordBytes(bounds replicatedstate.TransitionCaptureBounds) (int, error) {
	if bounds.Transitions > maxMutations || bounds.KeyBytes > bounds.Transitions*replication.MaxMutationKeyBytes ||
		bounds.BeforeBytes > bounds.Transitions*replication.MaxMutationValueBytes ||
		bounds.AfterBytes > bounds.Transitions*replication.MaxMutationValueBytes ||
		bounds.KeyBytes > replication.MaxCommandBytes || bounds.AfterBytes > replication.MaxCommandBytes-bounds.KeyBytes {
		return 0, ErrCapture
	}
	return entryBytes + int(bounds.Transitions)*mutationBytes + int(bounds.KeyBytes+bounds.AfterBytes), nil
}

func openEntry(raw []byte, w *CaptureWorkspace) (entry Entry, err error) {
	if w == nil {
		return entry, ErrCapture
	}
	w.clearMutations()
	defer func() {
		if err != nil {
			w.clearMutations()
		}
	}()
	if !validEnvelope(raw, 2, entryBytes) || raw[11] > byte(AbortSourceChanged) || binary.LittleEndian.Uint32(raw[20:24]) != 0 {
		return entry, ErrCapture
	}
	count := uint64(binary.LittleEndian.Uint32(raw[16:20]))
	if count > maxMutations || count > uint64((len(raw)-entryBytes)/mutationBytes) || raw[11] != 0 && count != 0 {
		return entry, ErrCapture
	}
	entry.Before, entry.After = openPublication(raw[24:128]), openPublication(raw[128:232])
	entry.PreviousDigest, entry.Digest = [32]byte(raw[232:264]), [32]byte(raw[len(raw)-32:])
	entry.Abort = AbortReason(raw[11])
	if !entry.Before.valid() || !entry.After.valid() || entry.After.Applied != entry.Before.Applied+1 ||
		entry.After.Term < entry.Before.Term || entry.PreviousDigest == [32]byte{} ||
		entry.Abort != AbortSourceChanged && (entry.Before.Ownership != entry.After.Ownership || entry.Before.Routing != entry.After.Routing || entry.Before.Route != entry.After.Route) {
		return Entry{}, ErrCapture
	}
	if cap(w.mutations) < int(count) {
		w.mutations = make([]Mutation, 0, int(count))
	}
	tail := raw[264 : len(raw)-32]
	var previous Mutation
	var relationCount int
	var payloadBytes uint64
	for range count {
		if len(tail) < mutationBytes {
			return Entry{}, ErrCapture
		}
		m := Mutation{Relation: replication.RelationID(binary.LittleEndian.Uint16(tail)),
			BeforePresent: tail[2]&1 != 0, AfterPresent: tail[2]&2 != 0,
			BeforeBytes: binary.LittleEndian.Uint32(tail[8:12]), BeforeDigest: [32]byte(tail[16:48])}
		keyBytes, afterBytes := uint64(binary.LittleEndian.Uint32(tail[4:8])), uint64(binary.LittleEndian.Uint32(tail[12:16]))
		if m.Relation == 0 || m.Relation > replication.MaxRelationID || tail[2] == 0 || tail[2] > 3 || tail[3] != 0 ||
			keyBytes == 0 || keyBytes > replication.MaxMutationKeyBytes || afterBytes > replication.MaxMutationValueBytes ||
			m.BeforeBytes > replication.MaxMutationValueBytes || !m.BeforePresent && (m.BeforeBytes != 0 || m.BeforeDigest != [32]byte{}) ||
			m.BeforePresent && m.BeforeDigest == [32]byte{} || !m.AfterPresent && afterBytes != 0 ||
			keyBytes+afterBytes > uint64(len(tail)-mutationBytes) {
			return Entry{}, ErrCapture
		}
		tail = tail[mutationBytes:]
		m.Key = tail[:int(keyBytes):int(keyBytes)]
		tail = tail[int(keyBytes):]
		if m.AfterPresent {
			m.After = tail[:int(afterBytes):int(afterBytes)]
		}
		tail = tail[int(afterBytes):]
		if m.Relation < previous.Relation || m.Relation == previous.Relation && bytes.Compare(m.Key, previous.Key) <= 0 {
			return Entry{}, ErrCapture
		}
		if m.Relation != previous.Relation {
			relationCount = 0
		}
		relationCount++
		payloadBytes += keyBytes + afterBytes
		if relationCount > replicatedstate.MaxDistinctMutations || payloadBytes > replication.MaxCommandBytes {
			return Entry{}, ErrCapture
		}
		w.mutations = append(w.mutations, m)
		previous = m
	}
	if len(tail) != 0 {
		return Entry{}, ErrCapture
	}
	entry.Mutations = w.mutations
	return entry, nil
}
