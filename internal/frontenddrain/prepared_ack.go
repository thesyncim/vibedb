// Package frontenddrain contains the authenticated physical receiver
// protocol used by a gateway while it publishes a prepared frontend drain.
//
// The protocol lives below gatewayruntime and shardservice so the gateway and
// a standalone storage process share exactly one wire grammar.  The source
// service-directory cut is carried in the request: a storage receiver never
// invents a grant from a local static manifest.  The request's source fences,
// canonical cut digest, and TLS peer binding make the proof replayable while
// keeping each receiver's gate installation atomic.
package frontenddrain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

var (
	// ErrPreparedAckWire is returned for malformed, non-canonical, or
	// internally inconsistent receiver ACK frames.
	ErrPreparedAckWire = errors.New("frontenddrain: invalid prepared receiver acknowledgement")
	// ErrPreparedAckAuth is returned when the authenticated control peer does
	// not match the source principal bound into the request.
	ErrPreparedAckAuth = errors.New("frontenddrain: prepared receiver acknowledgement authentication failed")
	// ErrPreparedAckState is returned when the receiver cannot prove that the
	// request names its exact active storage identity and a Prepared grant.
	ErrPreparedAckState = errors.New("frontenddrain: prepared receiver acknowledgement state mismatch")
	// ErrPreparedAckCutMoved is returned after an authenticated source read
	// proves that an InstallExact request raced a newer cut. Callers must
	// reread the authority and derive a new request before retrying.
	ErrPreparedAckCutMoved = errors.New("frontenddrain: prepared acknowledgement source cut moved")
)

// CutOperation is the closed operation set shared by the source-read and
// receiver-install paths.  The wire routes remain one canonical protocol:
// ReadLatest obtains an authority-backed cut and InstallExact installs the
// exact digest-bound cut returned by that source.
type CutOperation uint8

const (
	CutOperationReadLatest CutOperation = iota + 1
	CutOperationInstallExact
)

func (operation CutOperation) Valid() bool {
	return operation == CutOperationReadLatest || operation == CutOperationInstallExact
}

// PreparedAckSubject is the compact child-owned proof used when a prepared
// drain has no accepted connection token.  A grant digest may be zero for an
// empty drain; the child identity and drain fence still bind the lifecycle.
type PreparedAckSubject struct {
	DrainID                [32]byte
	GrantDigest            [32]byte
	PhysicalNode           rafttransport.NodeID
	PhysicalIncarnation    uint64
	GatewayServiceID       rafttransport.NodeID
	GatewayIncarnation     uint64
	GatewaySessionID       [16]byte
	GatewaySessionRevision uint64
	NodeRevision           uint64
	Lifecycle              uint8
	DrainFenceDigest       [32]byte
}

func (subject PreparedAckSubject) valid() bool {
	return subject.DrainID != ([32]byte{}) && subject.PhysicalNode != (rafttransport.NodeID{}) &&
		subject.PhysicalIncarnation != 0 && subject.GatewayServiceID != (rafttransport.NodeID{}) &&
		subject.GatewayIncarnation != 0 && subject.GatewaySessionID != ([16]byte{}) &&
		subject.GatewaySessionRevision != 0 && subject.NodeRevision != 0 &&
		subject.Lifecycle != 0 && subject.DrainFenceDigest != ([32]byte{})
}

// Valid reports whether the compact child proof has all identity and fence
// coordinates. The lifecycle byte is interpreted by the authority-backed
// source and is deliberately kept closed at this package boundary.
func (subject PreparedAckSubject) Valid() bool { return subject.valid() }

// PreparedAckDiscriminator is the fixed shard-control route for a physical
// receiver ACK.  Gateway-control participant ACKs intentionally use a
// separate grammar and traffic class.
var PreparedAckDiscriminator = [...]byte{'V', 'B', 'D', 'S', 'A', 'C', 'K', 1}

const (
	preparedAckHeaderBytes     = 360
	preparedAckDigestBytes     = sha256.Size
	MaxPreparedAckSourceRoster = 16
	// A complete service directory is bounded by the replicated mutation
	// limit.  The frame has its own header and digest, so reserve that space
	// before accepting a length from the wire.
	MaxPreparedAckCutBytes   = replication.MaxMutationValueBytes
	MaxPreparedAckFrameBytes = preparedAckHeaderBytes + MaxPreparedAckCutBytes + preparedAckDigestBytes
)

// PreparedAckSource is one catalog-authorized physical catalog source. The
// endpoint and SPKI pin are copied from the same node-directory cut as the
// service projection; receivers may persist this roster, but never accept an
// endpoint supplied by an ACK caller.
type PreparedAckSource struct {
	NodeID         rafttransport.NodeID `json:"node_id"`
	Incarnation    uint64               `json:"incarnation"`
	ControlAddress string               `json:"control_address"`
	SPKIPinDigest  [32]byte             `json:"spki_pin_digest"`
}

func (source PreparedAckSource) valid() bool {
	return source.NodeID != (rafttransport.NodeID{}) && source.Incarnation != 0 &&
		source.ControlAddress != "" && len(source.ControlAddress) <= 1024 &&
		source.SPKIPinDigest != ([32]byte{})
}

// Valid reports whether this source has complete physical identity and dial
// coordinates. TLS still authenticates the peer before any request is sent.
func (source PreparedAckSource) Valid() bool { return source.valid() }

// PreparedAckRequestHeaderBytes is the fixed install request prefix. The
// request appends a bounded canonical cut and a frame digest.
const PreparedAckRequestHeaderBytes = preparedAckHeaderBytes

// PreparedAckCut is the canonical source proof consumed by a storage
// receiver.  Directory/head fields are deliberately duplicated outside the
// service cut: they bind the service projection to the exact physical and
// catalog source epoch used by EnforceFrontendDrain.
type PreparedAckCut struct {
	DirectoryRevision        uint64
	DirectoryDigest          replication.Digest
	CatalogGeneration        uint64
	CatalogHeadDigest        replication.Digest
	ServiceDirectoryRevision uint64
	// ServiceDirectoryDigest binds the complete projected service directory,
	// including catalog-only scope advances that may share its Revision.
	// Marshal/Digest fill it from ServiceDirectory when a local caller leaves
	// it unset; a non-zero supplied value must match exactly.
	ServiceDirectoryDigest replication.Digest
	ServiceDirectory       serviceauthz.ServiceDirectoryCut
	// SourceRoster is the bounded current catalog-source roster. It is part of
	// the canonical cut digest so a receiver can durably discover a replacement
	// source before the prior source is retired.
	SourceRoster []PreparedAckSource
	// Subjects carries compact child proofs for drains that do not have a
	// continuation grant (the empty-token case). It is sorted by DrainID in
	// the canonical JSON cut.
	Subjects []PreparedAckSubject
}

// PreparedAckRequest is the complete authenticated request. SourcePrincipal
// and SourcePrincipalKeyDigest must equal the peer identity observed by the
// TLS control connection; receiver identity must match the physical binding
// in SourceCut.GrantDigest and SourceCut's Prepared grant are inseparable.
type PreparedAckRequest struct {
	Nonce                    [16]byte
	DrainID                  [32]byte
	GrantDigest              [32]byte
	SourcePrincipal          rafttransport.NodeID
	SourcePrincipalKeyDigest [32]byte
	ReceiverNode             rafttransport.NodeID
	ReceiverIncarnation      uint64
	ReceiverServiceKeyDigest [32]byte
	ReceiverNodeRevision     uint64
	// RequirePrepared requests the admission-barrier state. When false, an
	// InstallExact request may carry an Enforcing or Retired cut, or no drain
	// subject at all during receiver recovery.
	RequirePrepared bool
	SourceCut       PreparedAckCut
}

// PreparedAckResponse is returned only after the receiver has installed the
// committed gate. AppliedRevision is the gate revision observed after the
// atomic install, and all source/identity coordinates echo the request.
type PreparedAckResponse struct {
	Nonce                    [16]byte
	DrainID                  [32]byte
	GrantDigest              [32]byte
	ReceiverNode             rafttransport.NodeID
	ReceiverIncarnation      uint64
	ReceiverServiceKeyDigest [32]byte
	ReceiverNodeRevision     uint64
	DirectoryRevision        uint64
	DirectoryDigest          replication.Digest
	CatalogGeneration        uint64
	CatalogHeadDigest        replication.Digest
	ServiceDirectoryRevision uint64
	ServiceDirectoryDigest   replication.Digest
	AppliedRevision          uint64
	SourceCutDigest          [32]byte
}

func (cut PreparedAckCut) valid() bool {
	if cut.DirectoryRevision == 0 || cut.DirectoryDigest == (replication.Digest{}) ||
		cut.CatalogGeneration == 0 || cut.CatalogHeadDigest == (replication.Digest{}) ||
		cut.ServiceDirectoryRevision == 0 || !cut.ServiceDirectory.Valid() ||
		cut.ServiceDirectory.Revision != cut.ServiceDirectoryRevision ||
		cut.ServiceDirectory.CatalogGeneration != cut.CatalogGeneration {
		return false
	}
	if cut.ServiceDirectoryDigest != (replication.Digest{}) {
		if cut.ServiceDirectoryDigest != serviceDirectoryDigest(cut.ServiceDirectory) {
			return false
		}
	}
	for index, subject := range cut.Subjects {
		if !subject.valid() || index > 0 && bytes.Compare(cut.Subjects[index-1].DrainID[:], subject.DrainID[:]) >= 0 {
			return false
		}
	}
	if len(cut.SourceRoster) > MaxPreparedAckSourceRoster {
		return false
	}
	for index, source := range cut.SourceRoster {
		if !source.valid() || index > 0 && bytes.Compare(cut.SourceRoster[index-1].NodeID[:], source.NodeID[:]) >= 0 {
			return false
		}
		if index > 0 && cut.SourceRoster[index-1].ControlAddress == source.ControlAddress {
			return false
		}
	}
	return true
}

// Valid reports whether the source cut has all required fences and an
// internally matching service-directory revision.
func (cut PreparedAckCut) Valid() bool { return cut.valid() }

func (request PreparedAckRequest) valid() bool {
	if request.Nonce == ([16]byte{}) ||
		request.SourcePrincipal == (rafttransport.NodeID{}) ||
		request.SourcePrincipalKeyDigest == ([32]byte{}) || request.ReceiverNode == (rafttransport.NodeID{}) ||
		request.ReceiverIncarnation == 0 || request.ReceiverServiceKeyDigest == ([32]byte{}) ||
		request.ReceiverNodeRevision == 0 || !request.SourceCut.valid() {
		return false
	}
	if request.GrantDigest != ([32]byte{}) && request.DrainID == ([32]byte{}) {
		return false
	}
	if request.RequirePrepared && request.DrainID == ([32]byte{}) {
		return false
	}
	return true
}

// Valid reports whether request fields and the embedded canonical source cut
// satisfy the closed protocol shape. It does not authenticate the TLS peer.
func (request PreparedAckRequest) Valid() bool { return request.valid() }

// SourceCutDigest returns the digest of the canonical encoded source cut.
func (request PreparedAckRequest) SourceCutDigest() [32]byte {
	return request.SourceCut.Digest()
}

// Digest returns the digest of the canonical encoded cut.
func (cut PreparedAckCut) Digest() [32]byte {
	canonical, err := canonicalPreparedAckCut(cut)
	if err != nil {
		return [32]byte{}
	}
	raw, err := marshalPreparedAckCut(canonical)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(raw)
}

// ServiceDirectoryDigest returns the digest bound into the complete cut.
func (cut PreparedAckCut) ServiceDirectoryDigestValue() [32]byte {
	if !cut.ServiceDirectory.Valid() {
		return [32]byte{}
	}
	return serviceDirectoryDigest(cut.ServiceDirectory)
}

func serviceDirectoryDigest(directory serviceauthz.ServiceDirectoryCut) [32]byte {
	raw, err := vibejson.Marshal(&directory)
	if err != nil || len(raw) == 0 {
		return [32]byte{}
	}
	return sha256.Sum256(raw)
}

// ServiceDirectoryCutDigest computes the canonical digest carried by a full
// source cut. Callers must pass a validated, canonical directory cut.
func ServiceDirectoryCutDigest(directory serviceauthz.ServiceDirectoryCut) [32]byte {
	if !directory.Valid() {
		return [32]byte{}
	}
	return serviceDirectoryDigest(directory)
}

func canonicalPreparedAckCut(cut PreparedAckCut) (PreparedAckCut, error) {
	if !cut.valid() {
		return PreparedAckCut{}, ErrPreparedAckWire
	}
	digest := serviceDirectoryDigest(cut.ServiceDirectory)
	if digest == ([32]byte{}) || cut.ServiceDirectoryDigest != (replication.Digest{}) &&
		cut.ServiceDirectoryDigest != replication.Digest(digest) {
		return PreparedAckCut{}, ErrPreparedAckWire
	}
	cut.ServiceDirectoryDigest = replication.Digest(digest)
	cut.Subjects = append([]PreparedAckSubject(nil), cut.Subjects...)
	return cut, nil
}

func marshalPreparedAckCut(cut PreparedAckCut) ([]byte, error) {
	canonical, err := canonicalPreparedAckCut(cut)
	if err != nil {
		return nil, ErrPreparedAckWire
	}
	raw, err := vibejson.Marshal(&canonical)
	if err != nil || len(raw) == 0 || len(raw) > MaxPreparedAckCutBytes {
		return nil, ErrPreparedAckWire
	}
	return raw, nil
}

func appendPreparedAckHeader(dst []byte, request PreparedAckRequest, cutDigest [32]byte, cutBytes uint32) ([]byte, error) {
	if !request.valid() || cutBytes == 0 || uint64(cutBytes) > MaxPreparedAckCutBytes {
		return dst, ErrPreparedAckWire
	}
	start := len(dst)
	dst = append(dst, make([]byte, preparedAckHeaderBytes)...)
	raw := dst[start:]
	copy(raw[:8], PreparedAckDiscriminator[:])
	copy(raw[8:24], request.Nonce[:])
	copy(raw[24:56], request.DrainID[:])
	copy(raw[56:88], request.GrantDigest[:])
	copy(raw[88:104], request.SourcePrincipal[:])
	copy(raw[104:136], request.SourcePrincipalKeyDigest[:])
	copy(raw[136:152], request.ReceiverNode[:])
	binary.LittleEndian.PutUint64(raw[152:160], request.ReceiverIncarnation)
	copy(raw[160:192], request.ReceiverServiceKeyDigest[:])
	binary.LittleEndian.PutUint64(raw[192:200], request.ReceiverNodeRevision)
	binary.LittleEndian.PutUint64(raw[200:208], request.SourceCut.DirectoryRevision)
	copy(raw[208:240], request.SourceCut.DirectoryDigest[:])
	binary.LittleEndian.PutUint64(raw[240:248], request.SourceCut.CatalogGeneration)
	copy(raw[248:280], request.SourceCut.CatalogHeadDigest[:])
	binary.LittleEndian.PutUint64(raw[280:288], request.SourceCut.ServiceDirectoryRevision)
	binary.LittleEndian.PutUint32(raw[288:292], cutBytes)
	copy(raw[292:324], cutDigest[:])
	copy(raw[324:356], request.SourceCut.ServiceDirectoryDigest[:])
	if request.RequirePrepared {
		binary.LittleEndian.PutUint32(raw[356:360], 1)
	}
	return dst, nil
}

// Marshal returns one canonical request frame, including the route
// discriminator consumed by shardcontrol.Mux.
func (request PreparedAckRequest) Marshal() ([]byte, error) {
	cut, err := marshalPreparedAckCut(request.SourceCut)
	if err != nil || !request.valid() {
		return nil, ErrPreparedAckWire
	}
	cutDigest := sha256.Sum256(cut)
	canonicalCut, err := canonicalPreparedAckCut(request.SourceCut)
	if err != nil {
		return nil, ErrPreparedAckWire
	}
	request.SourceCut = canonicalCut
	cut, err = marshalPreparedAckCut(request.SourceCut)
	if err != nil {
		return nil, err
	}
	cutDigest = sha256.Sum256(cut)
	frame, err := appendPreparedAckHeader(nil, request, cutDigest, uint32(len(cut)))
	if err != nil {
		return nil, err
	}
	frame = append(frame, cut...)
	digest := sha256.Sum256(frame)
	frame = append(frame, digest[:]...)
	return frame, nil
}

// OpenPreparedAckRequest validates the frame length, digest, and canonical
// JSON source cut before returning a request.
func OpenPreparedAckRequest(raw []byte) (PreparedAckRequest, error) {
	if len(raw) < preparedAckHeaderBytes+preparedAckDigestBytes ||
		len(raw) > MaxPreparedAckFrameBytes || !bytes.Equal(raw[:8], PreparedAckDiscriminator[:]) {
		return PreparedAckRequest{}, ErrPreparedAckWire
	}
	cutBytes := binary.LittleEndian.Uint32(raw[288:292])
	if cutBytes == 0 || uint64(cutBytes) > MaxPreparedAckCutBytes ||
		int(cutBytes) != len(raw)-preparedAckHeaderBytes-preparedAckDigestBytes {
		return PreparedAckRequest{}, ErrPreparedAckWire
	}
	frameDigest := sha256.Sum256(raw[:len(raw)-preparedAckDigestBytes])
	if !bytes.Equal(frameDigest[:], raw[len(raw)-preparedAckDigestBytes:]) {
		return PreparedAckRequest{}, ErrPreparedAckWire
	}
	cutRaw := raw[preparedAckHeaderBytes : preparedAckHeaderBytes+int(cutBytes)]
	cutDigest := sha256.Sum256(cutRaw)
	if !bytes.Equal(cutDigest[:], raw[292:324]) {
		return PreparedAckRequest{}, ErrPreparedAckWire
	}
	var serviceDigest replication.Digest
	copy(serviceDigest[:], raw[324:356])
	flags := binary.LittleEndian.Uint32(raw[356:360])
	if flags&^uint32(1) != 0 {
		return PreparedAckRequest{}, ErrPreparedAckWire
	}
	var request PreparedAckRequest
	copy(request.Nonce[:], raw[8:24])
	copy(request.DrainID[:], raw[24:56])
	copy(request.GrantDigest[:], raw[56:88])
	copy(request.SourcePrincipal[:], raw[88:104])
	copy(request.SourcePrincipalKeyDigest[:], raw[104:136])
	copy(request.ReceiverNode[:], raw[136:152])
	request.ReceiverIncarnation = binary.LittleEndian.Uint64(raw[152:160])
	copy(request.ReceiverServiceKeyDigest[:], raw[160:192])
	request.ReceiverNodeRevision = binary.LittleEndian.Uint64(raw[192:200])
	request.SourceCut.DirectoryRevision = binary.LittleEndian.Uint64(raw[200:208])
	copy(request.SourceCut.DirectoryDigest[:], raw[208:240])
	request.SourceCut.CatalogGeneration = binary.LittleEndian.Uint64(raw[240:248])
	copy(request.SourceCut.CatalogHeadDigest[:], raw[248:280])
	request.SourceCut.ServiceDirectoryRevision = binary.LittleEndian.Uint64(raw[280:288])
	request.SourceCut.ServiceDirectoryDigest = serviceDigest
	request.RequirePrepared = flags&1 != 0
	if err := vibejson.Unmarshal(cutRaw, &request.SourceCut); err != nil || !request.valid() {
		return PreparedAckRequest{}, ErrPreparedAckWire
	}
	canonical, err := marshalPreparedAckCut(request.SourceCut)
	if err != nil || !bytes.Equal(canonical, cutRaw) {
		return PreparedAckRequest{}, ErrPreparedAckWire
	}
	if request.SourceCutDigest() != cutDigest {
		return PreparedAckRequest{}, ErrPreparedAckWire
	}
	return request, nil
}

func (response PreparedAckResponse) valid(request PreparedAckRequest) bool {
	return request.valid() && response.Nonce == request.Nonce && response.DrainID == request.DrainID &&
		response.GrantDigest == request.GrantDigest && response.ReceiverNode == request.ReceiverNode &&
		response.ReceiverIncarnation == request.ReceiverIncarnation &&
		response.ReceiverServiceKeyDigest == request.ReceiverServiceKeyDigest &&
		response.ReceiverNodeRevision == request.ReceiverNodeRevision &&
		response.DirectoryRevision == request.SourceCut.DirectoryRevision &&
		response.DirectoryDigest == request.SourceCut.DirectoryDigest &&
		response.CatalogGeneration == request.SourceCut.CatalogGeneration &&
		response.CatalogHeadDigest == request.SourceCut.CatalogHeadDigest &&
		response.ServiceDirectoryRevision == request.SourceCut.ServiceDirectoryRevision &&
		response.ServiceDirectoryDigest == replication.Digest(request.SourceCut.ServiceDirectoryDigestValue()) &&
		response.AppliedRevision != 0 && response.SourceCutDigest == request.SourceCutDigest()
}

// PreparedAckCutMovedAppliedRevision is the explicit status carried by the
// fixed receiver response when the source reader reports an authenticated cut
// movement. It is outside the native gate revision domain.
const PreparedAckCutMovedAppliedRevision uint64 = ^uint64(0)

// CutMoved reports the explicit source-cut movement status.
func (response PreparedAckResponse) CutMoved() bool {
	return response.AppliedRevision == PreparedAckCutMovedAppliedRevision
}

// Valid reports whether response echoes the request and includes a committed
// post-install revision.
func (response PreparedAckResponse) Valid(request PreparedAckRequest) bool {
	return response.valid(request)
}

// Marshal encodes the fixed response plus a SHA-256 frame digest.
func (response PreparedAckResponse) Marshal() []byte {
	raw := make([]byte, PreparedAckResponseBytes)
	copy(raw[:16], response.Nonce[:])
	copy(raw[16:48], response.DrainID[:])
	copy(raw[48:80], response.GrantDigest[:])
	copy(raw[80:96], response.ReceiverNode[:])
	binary.LittleEndian.PutUint64(raw[96:104], response.ReceiverIncarnation)
	copy(raw[104:136], response.ReceiverServiceKeyDigest[:])
	binary.LittleEndian.PutUint64(raw[136:144], response.ReceiverNodeRevision)
	binary.LittleEndian.PutUint64(raw[144:152], response.DirectoryRevision)
	copy(raw[152:184], response.DirectoryDigest[:])
	binary.LittleEndian.PutUint64(raw[184:192], response.CatalogGeneration)
	copy(raw[192:224], response.CatalogHeadDigest[:])
	binary.LittleEndian.PutUint64(raw[224:232], response.ServiceDirectoryRevision)
	copy(raw[232:264], response.ServiceDirectoryDigest[:])
	binary.LittleEndian.PutUint64(raw[264:272], response.AppliedRevision)
	copy(raw[272:304], response.SourceCutDigest[:])
	digest := sha256.Sum256(raw[:304])
	copy(raw[304:], digest[:])
	return raw
}

const PreparedAckResponseBytes = 336

// OpenPreparedAckResponse validates the fixed response and request binding.
func OpenPreparedAckResponse(raw []byte, request PreparedAckRequest) (PreparedAckResponse, error) {
	if len(raw) != PreparedAckResponseBytes || !request.valid() {
		return PreparedAckResponse{}, ErrPreparedAckWire
	}
	digest := sha256.Sum256(raw[:304])
	if !bytes.Equal(digest[:], raw[304:]) {
		return PreparedAckResponse{}, ErrPreparedAckWire
	}
	var response PreparedAckResponse
	copy(response.Nonce[:], raw[:16])
	copy(response.DrainID[:], raw[16:48])
	copy(response.GrantDigest[:], raw[48:80])
	copy(response.ReceiverNode[:], raw[80:96])
	response.ReceiverIncarnation = binary.LittleEndian.Uint64(raw[96:104])
	copy(response.ReceiverServiceKeyDigest[:], raw[104:136])
	response.ReceiverNodeRevision = binary.LittleEndian.Uint64(raw[136:144])
	response.DirectoryRevision = binary.LittleEndian.Uint64(raw[144:152])
	copy(response.DirectoryDigest[:], raw[152:184])
	response.CatalogGeneration = binary.LittleEndian.Uint64(raw[184:192])
	copy(response.CatalogHeadDigest[:], raw[192:224])
	response.ServiceDirectoryRevision = binary.LittleEndian.Uint64(raw[224:232])
	copy(response.ServiceDirectoryDigest[:], raw[232:264])
	response.AppliedRevision = binary.LittleEndian.Uint64(raw[264:272])
	copy(response.SourceCutDigest[:], raw[272:304])
	if !response.valid(request) {
		return PreparedAckResponse{}, ErrPreparedAckWire
	}
	return response, nil
}

// MaxFrameBytes is exported for listener-side accounting and tests.
func MaxFrameBytes() int {
	if MaxPreparedAckCutBytes > math.MaxInt-preparedAckHeaderBytes-preparedAckDigestBytes {
		return math.MaxInt
	}
	return preparedAckHeaderBytes + int(MaxPreparedAckCutBytes) + preparedAckDigestBytes
}
