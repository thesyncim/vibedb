package frontenddrain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

// PreparedAckCutReadDiscriminator is the single canonical GatewayControl
// source route. Its request carries either ReadLatest or InstallExact; the
// route is intentionally not versioned or accepted by a legacy parser.
var PreparedAckCutReadDiscriminator = [...]byte{'V', 'B', 'D', 'S', 'R', 'E', 'A', 'D'}

var PreparedAckCutReadResponseDiscriminator = [...]byte{'V', 'B', 'D', 'S', 'R', 'E', 'S', 1}

// PreparedAckCutReadMovedResponseDiscriminator is an authenticated response
// used when InstallExact raced a newer source cut. The TLS peer and all query
// identity fields remain bound, so callers may retry only after rereading the
// authoritative floor.
var PreparedAckCutReadMovedResponseDiscriminator = [...]byte{'V', 'B', 'D', 'S', 'R', 'E', 'M', 1}

// Canonical names for the unrelease source/install protocol.
var ServiceCutReadLatestDiscriminator = PreparedAckCutReadDiscriminator

const (
	preparedAckCutReadRequestPayloadBytes    = 320
	preparedAckCutReadRequestBytes           = preparedAckCutReadRequestPayloadBytes + sha256.Size
	preparedAckCutReadResponseHeaderBytes    = 324
	PreparedAckCutReadResponseCutBytesOffset = 320
	preparedAckCutReadMovedResponseBodyBytes = 320
	preparedAckCutReadMovedResponseBytes     = preparedAckCutReadMovedResponseBodyBytes + sha256.Size
)

// PreparedAckCutReadRequestBytes is the fixed request size consumed after the
// gateway-control mux has selected PreparedAckCutReadDiscriminator.
const PreparedAckCutReadRequestBytes = preparedAckCutReadRequestBytes

// PreparedAckCutReadResponseHeaderBytes is the fixed response prefix. The
// response appends a bounded canonical PreparedAckCut and frame digest.
const PreparedAckCutReadResponseHeaderBytes = preparedAckCutReadResponseHeaderBytes

// PreparedAckCutReadMovedResponseBytes is the fixed authenticated frame size
// returned for an InstallExact source-cut race.
const PreparedAckCutReadMovedResponseBytes = preparedAckCutReadMovedResponseBytes

// PreparedAckCutReadFloor is the last complete source coordinate observed by
// a receiver. An all-zero floor is the bootstrap form of ReadLatest; a
// non-zero floor must contain every coordinate and digest.
type PreparedAckCutReadFloor struct {
	DirectoryRevision        uint64
	DirectoryDigest          replication.Digest
	CatalogGeneration        uint64
	CatalogHeadDigest        replication.Digest
	ServiceDirectoryRevision uint64
	ServiceDirectoryDigest   replication.Digest
}

// ReadFloor returns the complete source coordinate represented by a cut.
// Receiver node identity is carried separately by the request and is not
// conflated with the source directory revision.
func (cut PreparedAckCut) ReadFloor() PreparedAckCutReadFloor {
	return PreparedAckCutReadFloor{
		DirectoryRevision: cut.DirectoryRevision, DirectoryDigest: cut.DirectoryDigest,
		CatalogGeneration: cut.CatalogGeneration, CatalogHeadDigest: cut.CatalogHeadDigest,
		ServiceDirectoryRevision: cut.ServiceDirectoryRevision,
		ServiceDirectoryDigest:   cut.ServiceDirectoryDigestValue(),
	}
}

// Canonical operation names used by source and receiver callers.
const (
	ReadLatest   = CutOperationReadLatest
	InstallExact = CutOperationInstallExact
)

type ServiceCut = PreparedAckCut
type ServiceCutReadLatestRequest = PreparedAckCutReadRequest
type ServiceCutReadLatestResponse = PreparedAckCutReadResponse
type ServiceCutInstallExactRequest = PreparedAckRequest
type ServiceCutInstallExactResponse = PreparedAckResponse

func (floor PreparedAckCutReadFloor) zero() bool {
	return floor == (PreparedAckCutReadFloor{})
}

func (floor PreparedAckCutReadFloor) valid() bool {
	if floor.zero() {
		return true
	}
	return floor.DirectoryRevision != 0 && floor.DirectoryDigest != (replication.Digest{}) &&
		floor.CatalogGeneration != 0 && floor.CatalogHeadDigest != (replication.Digest{}) &&
		floor.ServiceDirectoryRevision != 0 && floor.ServiceDirectoryDigest != (replication.Digest{})
}

// Valid reports whether the floor is either the empty bootstrap floor or a
// complete set of source coordinates.
func (floor PreparedAckCutReadFloor) Valid() bool { return floor.valid() }

// AtLeastFloor reports whether cut is at or beyond every independent source
// coordinate in floor. A digest is compared only when its associated scalar
// coordinate (and all earlier coordinates) is unchanged; a newer catalog or
// directory epoch may legitimately carry a new service projection digest at
// the same service revision.
func (cut PreparedAckCut) AtLeastFloor(floor PreparedAckCutReadFloor) bool {
	if floor.zero() {
		return true
	}
	if !floor.valid() || cut.DirectoryRevision < floor.DirectoryRevision ||
		cut.CatalogGeneration < floor.CatalogGeneration ||
		cut.ServiceDirectoryRevision < floor.ServiceDirectoryRevision {
		return false
	}
	if cut.DirectoryRevision == floor.DirectoryRevision && cut.DirectoryDigest != floor.DirectoryDigest {
		return false
	}
	if cut.CatalogGeneration == floor.CatalogGeneration && cut.CatalogHeadDigest != floor.CatalogHeadDigest {
		return false
	}
	if cut.DirectoryRevision == floor.DirectoryRevision && cut.CatalogGeneration == floor.CatalogGeneration &&
		cut.ServiceDirectoryRevision == floor.ServiceDirectoryRevision &&
		cut.ServiceDirectoryDigestValue() != floor.ServiceDirectoryDigest {
		return false
	}
	return true
}

// PreparedAckCutReadRequest identifies the exact physical receiver and its
// source floor. ReadLatest does not require a drain, grant, or node revision;
// InstallExact carries those values only when the admission barrier has a
// prepared subject to assert.
type PreparedAckCutReadRequest struct {
	Operation                CutOperation
	RequirePrepared          bool
	Nonce                    [16]byte
	DrainID                  [32]byte
	GrantDigest              [32]byte
	ReceiverNode             rafttransport.NodeID
	ReceiverIncarnation      uint64
	ReceiverServiceKeyDigest [32]byte
	ReceiverNodeRevision     uint64
	SourceFloor              PreparedAckCutReadFloor
	SourceCutDigest          [32]byte
}

func (request PreparedAckCutReadRequest) operation() CutOperation {
	return request.Operation
}

func (request PreparedAckCutReadRequest) valid() bool {
	operation := request.operation()
	if !operation.Valid() || request.Nonce == ([16]byte{}) || request.ReceiverNode == (rafttransport.NodeID{}) ||
		request.ReceiverIncarnation == 0 || request.ReceiverServiceKeyDigest == ([32]byte{}) ||
		!request.SourceFloor.valid() || request.GrantDigest != ([32]byte{}) && request.DrainID == ([32]byte{}) {
		return false
	}
	if request.RequirePrepared && request.DrainID == ([32]byte{}) {
		return false
	}
	if operation == CutOperationInstallExact {
		return request.ReceiverNodeRevision != 0 && request.SourceCutDigest != ([32]byte{}) &&
			!request.SourceFloor.zero()
	}
	return request.ReceiverNodeRevision == 0 && request.SourceCutDigest == ([32]byte{})
}

// Valid reports whether all source-cut query coordinates are present for the
// selected operation.
func (request PreparedAckCutReadRequest) Valid() bool { return request.valid() }

// Marshal returns the fixed query frame, including its route discriminator and
// frame digest.
func (request PreparedAckCutReadRequest) Marshal() []byte {
	if !request.valid() {
		return nil
	}
	raw := make([]byte, preparedAckCutReadRequestBytes)
	copy(raw[:8], PreparedAckCutReadDiscriminator[:])
	binary.LittleEndian.PutUint64(raw[8:16], uint64(request.operation()))
	if request.RequirePrepared {
		binary.LittleEndian.PutUint64(raw[16:24], 1)
	}
	copy(raw[24:40], request.Nonce[:])
	copy(raw[40:72], request.DrainID[:])
	copy(raw[72:104], request.GrantDigest[:])
	copy(raw[104:120], request.ReceiverNode[:])
	binary.LittleEndian.PutUint64(raw[120:128], request.ReceiverIncarnation)
	copy(raw[128:160], request.ReceiverServiceKeyDigest[:])
	binary.LittleEndian.PutUint64(raw[160:168], request.ReceiverNodeRevision)
	binary.LittleEndian.PutUint64(raw[168:176], request.SourceFloor.DirectoryRevision)
	copy(raw[176:208], request.SourceFloor.DirectoryDigest[:])
	binary.LittleEndian.PutUint64(raw[208:216], request.SourceFloor.CatalogGeneration)
	copy(raw[216:248], request.SourceFloor.CatalogHeadDigest[:])
	binary.LittleEndian.PutUint64(raw[248:256], request.SourceFloor.ServiceDirectoryRevision)
	copy(raw[256:288], request.SourceFloor.ServiceDirectoryDigest[:])
	copy(raw[288:320], request.SourceCutDigest[:])
	digest := sha256.Sum256(raw[:preparedAckCutReadRequestPayloadBytes])
	copy(raw[preparedAckCutReadRequestPayloadBytes:], digest[:])
	return raw
}

// OpenPreparedAckCutReadRequest validates a complete fixed query frame.
func OpenPreparedAckCutReadRequest(raw []byte) (PreparedAckCutReadRequest, error) {
	if len(raw) != preparedAckCutReadRequestBytes || !bytes.Equal(raw[:8], PreparedAckCutReadDiscriminator[:]) {
		return PreparedAckCutReadRequest{}, ErrPreparedAckWire
	}
	digest := sha256.Sum256(raw[:preparedAckCutReadRequestPayloadBytes])
	if !bytes.Equal(digest[:], raw[preparedAckCutReadRequestPayloadBytes:]) {
		return PreparedAckCutReadRequest{}, ErrPreparedAckWire
	}
	var request PreparedAckCutReadRequest
	request.Operation = CutOperation(binary.LittleEndian.Uint64(raw[8:16]))
	flags := binary.LittleEndian.Uint64(raw[16:24])
	if flags&^uint64(1) != 0 {
		return PreparedAckCutReadRequest{}, ErrPreparedAckWire
	}
	request.RequirePrepared = flags&1 != 0
	copy(request.Nonce[:], raw[24:40])
	copy(request.DrainID[:], raw[40:72])
	copy(request.GrantDigest[:], raw[72:104])
	copy(request.ReceiverNode[:], raw[104:120])
	request.ReceiverIncarnation = binary.LittleEndian.Uint64(raw[120:128])
	copy(request.ReceiverServiceKeyDigest[:], raw[128:160])
	request.ReceiverNodeRevision = binary.LittleEndian.Uint64(raw[160:168])
	request.SourceFloor.DirectoryRevision = binary.LittleEndian.Uint64(raw[168:176])
	copy(request.SourceFloor.DirectoryDigest[:], raw[176:208])
	request.SourceFloor.CatalogGeneration = binary.LittleEndian.Uint64(raw[208:216])
	copy(request.SourceFloor.CatalogHeadDigest[:], raw[216:248])
	request.SourceFloor.ServiceDirectoryRevision = binary.LittleEndian.Uint64(raw[248:256])
	copy(request.SourceFloor.ServiceDirectoryDigest[:], raw[256:288])
	copy(request.SourceCutDigest[:], raw[288:320])
	if !request.valid() {
		return PreparedAckCutReadRequest{}, ErrPreparedAckWire
	}
	return request, nil
}

// PreparedAckCutReadResponse carries the source authority's complete cut.
// Every full coordinate is repeated in the fixed header so a receiver can
// reject a stale or catalog-only-mutated response without trusting JSON.
type PreparedAckCutReadResponse struct {
	Operation                CutOperation
	RequirePrepared          bool
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
	SourceCutDigest          [32]byte
	Cut                      PreparedAckCut
}

// PreparedAckCutReadMovedResponse identifies an authenticated source-cut
// movement without pretending that the stale InstallExact request installed.
// The caller must reread and fully rederive its request from the authority.
type PreparedAckCutReadMovedResponse struct {
	Operation                CutOperation
	RequirePrepared          bool
	Nonce                    [16]byte
	DrainID                  [32]byte
	GrantDigest              [32]byte
	ReceiverNode             rafttransport.NodeID
	ReceiverIncarnation      uint64
	ReceiverServiceKeyDigest [32]byte
	ReceiverNodeRevision     uint64
	SourceFloor              PreparedAckCutReadFloor
	SourceCutDigest          [32]byte
}

func (response PreparedAckCutReadMovedResponse) valid(request PreparedAckCutReadRequest) bool {
	return request.valid() && request.Operation == CutOperationInstallExact &&
		response.Operation == request.Operation && response.RequirePrepared == request.RequirePrepared &&
		response.Nonce == request.Nonce && response.DrainID == request.DrainID &&
		response.GrantDigest == request.GrantDigest && response.ReceiverNode == request.ReceiverNode &&
		response.ReceiverIncarnation == request.ReceiverIncarnation &&
		response.ReceiverServiceKeyDigest == request.ReceiverServiceKeyDigest &&
		response.ReceiverNodeRevision == request.ReceiverNodeRevision &&
		response.SourceFloor.valid() && !response.SourceFloor.zero() &&
		preparedAckCutReadFloorStrictlyAdvanced(response.SourceFloor, request.SourceFloor) &&
		response.SourceCutDigest != ([32]byte{}) && response.SourceCutDigest != request.SourceCutDigest
}

// Valid reports whether the moved response is bound to the exact query.
func (response PreparedAckCutReadMovedResponse) Valid(request PreparedAckCutReadRequest) bool {
	return response.valid(request)
}

// Marshal encodes the fixed moved response and its frame digest.
func (response PreparedAckCutReadMovedResponse) Marshal() ([]byte, error) {
	if response.Operation != CutOperationInstallExact || response.Nonce == ([16]byte{}) ||
		response.ReceiverNode == (rafttransport.NodeID{}) || response.ReceiverIncarnation == 0 ||
		response.ReceiverServiceKeyDigest == ([32]byte{}) || response.ReceiverNodeRevision == 0 ||
		response.SourceCutDigest == ([32]byte{}) {
		return nil, ErrPreparedAckWire
	}
	raw := make([]byte, preparedAckCutReadMovedResponseBytes)
	copy(raw[:8], PreparedAckCutReadMovedResponseDiscriminator[:])
	binary.LittleEndian.PutUint64(raw[8:16], uint64(response.Operation))
	if response.RequirePrepared {
		binary.LittleEndian.PutUint64(raw[16:24], 1)
	}
	copy(raw[24:40], response.Nonce[:])
	copy(raw[40:72], response.DrainID[:])
	copy(raw[72:104], response.GrantDigest[:])
	copy(raw[104:120], response.ReceiverNode[:])
	binary.LittleEndian.PutUint64(raw[120:128], response.ReceiverIncarnation)
	copy(raw[128:160], response.ReceiverServiceKeyDigest[:])
	binary.LittleEndian.PutUint64(raw[160:168], response.ReceiverNodeRevision)
	binary.LittleEndian.PutUint64(raw[168:176], response.SourceFloor.DirectoryRevision)
	copy(raw[176:208], response.SourceFloor.DirectoryDigest[:])
	binary.LittleEndian.PutUint64(raw[208:216], response.SourceFloor.CatalogGeneration)
	copy(raw[216:248], response.SourceFloor.CatalogHeadDigest[:])
	binary.LittleEndian.PutUint64(raw[248:256], response.SourceFloor.ServiceDirectoryRevision)
	copy(raw[256:288], response.SourceFloor.ServiceDirectoryDigest[:])
	copy(raw[288:320], response.SourceCutDigest[:])
	digest := sha256.Sum256(raw[:preparedAckCutReadMovedResponseBodyBytes])
	copy(raw[preparedAckCutReadMovedResponseBodyBytes:], digest[:])
	return raw, nil
}

// OpenPreparedAckCutReadMovedResponse validates a moved response against its
// original query and preserves the authenticated cut digest for diagnostics.
func OpenPreparedAckCutReadMovedResponse(raw []byte, request PreparedAckCutReadRequest) (PreparedAckCutReadMovedResponse, error) {
	if len(raw) != preparedAckCutReadMovedResponseBytes || !request.valid() ||
		!bytes.Equal(raw[:8], PreparedAckCutReadMovedResponseDiscriminator[:]) {
		return PreparedAckCutReadMovedResponse{}, ErrPreparedAckWire
	}
	digest := sha256.Sum256(raw[:preparedAckCutReadMovedResponseBodyBytes])
	if !bytes.Equal(digest[:], raw[preparedAckCutReadMovedResponseBodyBytes:]) {
		return PreparedAckCutReadMovedResponse{}, ErrPreparedAckWire
	}
	flags := binary.LittleEndian.Uint64(raw[16:24])
	if flags&^uint64(1) != 0 {
		return PreparedAckCutReadMovedResponse{}, ErrPreparedAckWire
	}
	var response PreparedAckCutReadMovedResponse
	response.Operation = CutOperation(binary.LittleEndian.Uint64(raw[8:16]))
	response.RequirePrepared = flags&1 != 0
	copy(response.Nonce[:], raw[24:40])
	copy(response.DrainID[:], raw[40:72])
	copy(response.GrantDigest[:], raw[72:104])
	copy(response.ReceiverNode[:], raw[104:120])
	response.ReceiverIncarnation = binary.LittleEndian.Uint64(raw[120:128])
	copy(response.ReceiverServiceKeyDigest[:], raw[128:160])
	response.ReceiverNodeRevision = binary.LittleEndian.Uint64(raw[160:168])
	response.SourceFloor.DirectoryRevision = binary.LittleEndian.Uint64(raw[168:176])
	copy(response.SourceFloor.DirectoryDigest[:], raw[176:208])
	response.SourceFloor.CatalogGeneration = binary.LittleEndian.Uint64(raw[208:216])
	copy(response.SourceFloor.CatalogHeadDigest[:], raw[216:248])
	response.SourceFloor.ServiceDirectoryRevision = binary.LittleEndian.Uint64(raw[248:256])
	copy(response.SourceFloor.ServiceDirectoryDigest[:], raw[256:288])
	copy(response.SourceCutDigest[:], raw[288:320])
	if !response.valid(request) {
		return PreparedAckCutReadMovedResponse{}, ErrPreparedAckWire
	}
	return response, nil
}

func preparedAckCutReadFloorStrictlyAdvanced(
	current, prior PreparedAckCutReadFloor,
) bool {
	if !current.valid() || current.zero() {
		return false
	}
	if prior.zero() {
		return true
	}
	if !prior.valid() || current.DirectoryRevision < prior.DirectoryRevision ||
		current.CatalogGeneration < prior.CatalogGeneration ||
		current.ServiceDirectoryRevision < prior.ServiceDirectoryRevision {
		return false
	}
	if current.DirectoryRevision == prior.DirectoryRevision && current.DirectoryDigest != prior.DirectoryDigest ||
		current.CatalogGeneration == prior.CatalogGeneration && current.CatalogHeadDigest != prior.CatalogHeadDigest ||
		current.DirectoryRevision == prior.DirectoryRevision && current.CatalogGeneration == prior.CatalogGeneration &&
			current.ServiceDirectoryRevision == prior.ServiceDirectoryRevision &&
			current.ServiceDirectoryDigest != prior.ServiceDirectoryDigest {
		return false
	}
	return current.DirectoryRevision > prior.DirectoryRevision ||
		current.CatalogGeneration > prior.CatalogGeneration ||
		current.ServiceDirectoryRevision > prior.ServiceDirectoryRevision
}

func (response PreparedAckCutReadResponse) valid(request PreparedAckCutReadRequest) bool {
	if !request.valid() || response.Operation != request.operation() || response.RequirePrepared != request.RequirePrepared ||
		response.Nonce != request.Nonce || response.DrainID != request.DrainID || response.GrantDigest != request.GrantDigest ||
		response.ReceiverNode != request.ReceiverNode || response.ReceiverIncarnation != request.ReceiverIncarnation ||
		response.ReceiverServiceKeyDigest != request.ReceiverServiceKeyDigest || !response.Cut.Valid() ||
		response.SourceCutDigest != response.Cut.Digest() ||
		response.DirectoryRevision != response.Cut.DirectoryRevision || response.DirectoryDigest != response.Cut.DirectoryDigest ||
		response.CatalogGeneration != response.Cut.CatalogGeneration || response.CatalogHeadDigest != response.Cut.CatalogHeadDigest ||
		response.ServiceDirectoryRevision != response.Cut.ServiceDirectoryRevision ||
		response.ServiceDirectoryDigest != replication.Digest(response.Cut.ServiceDirectoryDigestValue()) {
		return false
	}
	if request.operation() == CutOperationInstallExact {
		return response.ReceiverNodeRevision == request.ReceiverNodeRevision &&
			response.SourceCutDigest == request.SourceCutDigest &&
			response.Cut.DirectoryRevision == request.SourceFloor.DirectoryRevision &&
			response.Cut.DirectoryDigest == request.SourceFloor.DirectoryDigest &&
			response.Cut.CatalogGeneration == request.SourceFloor.CatalogGeneration &&
			response.Cut.CatalogHeadDigest == request.SourceFloor.CatalogHeadDigest &&
			response.Cut.ServiceDirectoryRevision == request.SourceFloor.ServiceDirectoryRevision &&
			response.Cut.ServiceDirectoryDigestValue() == request.SourceFloor.ServiceDirectoryDigest
	}
	return response.ReceiverNodeRevision != 0 && response.Cut.AtLeastFloor(request.SourceFloor) &&
		(response.ReceiverNodeRevision >= request.ReceiverNodeRevision || request.ReceiverNodeRevision == 0)
}

// Valid reports whether the response is bound to the query and complete cut.
func (response PreparedAckCutReadResponse) Valid(request PreparedAckCutReadRequest) bool {
	return response.valid(request)
}

// Marshal encodes a canonical response and returns an error for an invalid or
// oversized cut. The frame digest covers the fixed header and cut bytes.
func (response PreparedAckCutReadResponse) Marshal() ([]byte, error) {
	canonical, err := canonicalPreparedAckCut(response.Cut)
	if err != nil {
		return nil, ErrPreparedAckWire
	}
	response.Cut = canonical
	if response.SourceCutDigest != ([32]byte{}) && response.SourceCutDigest != response.Cut.Digest() {
		return nil, ErrPreparedAckWire
	}
	response.ServiceDirectoryDigest = response.Cut.ServiceDirectoryDigest
	response.SourceCutDigest = response.Cut.Digest()
	if !response.Operation.Valid() || response.Nonce == ([16]byte{}) || response.ReceiverNode == (rafttransport.NodeID{}) ||
		response.ReceiverIncarnation == 0 || response.ReceiverServiceKeyDigest == ([32]byte{}) ||
		response.ReceiverNodeRevision == 0 || response.SourceCutDigest == ([32]byte{}) {
		return nil, ErrPreparedAckWire
	}
	cut, err := marshalPreparedAckCut(response.Cut)
	if err != nil || len(cut) > int(^uint32(0)) {
		return nil, ErrPreparedAckWire
	}
	raw := make([]byte, preparedAckCutReadResponseHeaderBytes+len(cut)+sha256.Size)
	copy(raw[:8], PreparedAckCutReadResponseDiscriminator[:])
	binary.LittleEndian.PutUint64(raw[8:16], uint64(response.Operation))
	if response.RequirePrepared {
		binary.LittleEndian.PutUint64(raw[16:24], 1)
	}
	copy(raw[24:40], response.Nonce[:])
	copy(raw[40:72], response.DrainID[:])
	copy(raw[72:104], response.GrantDigest[:])
	copy(raw[104:120], response.ReceiverNode[:])
	binary.LittleEndian.PutUint64(raw[120:128], response.ReceiverIncarnation)
	copy(raw[128:160], response.ReceiverServiceKeyDigest[:])
	binary.LittleEndian.PutUint64(raw[160:168], response.ReceiverNodeRevision)
	binary.LittleEndian.PutUint64(raw[168:176], response.DirectoryRevision)
	copy(raw[176:208], response.DirectoryDigest[:])
	binary.LittleEndian.PutUint64(raw[208:216], response.CatalogGeneration)
	copy(raw[216:248], response.CatalogHeadDigest[:])
	binary.LittleEndian.PutUint64(raw[248:256], response.ServiceDirectoryRevision)
	copy(raw[256:288], response.ServiceDirectoryDigest[:])
	copy(raw[288:320], response.SourceCutDigest[:])
	binary.LittleEndian.PutUint32(raw[320:324], uint32(len(cut)))
	copy(raw[324:324+len(cut)], cut)
	digest := sha256.Sum256(raw[:len(raw)-sha256.Size])
	copy(raw[len(raw)-sha256.Size:], digest[:])
	return raw, nil
}

// OpenPreparedAckCutReadResponse validates a bounded variable-length source
// cut response against the original query.
func OpenPreparedAckCutReadResponse(raw []byte, request PreparedAckCutReadRequest) (PreparedAckCutReadResponse, error) {
	if len(raw) < preparedAckCutReadResponseHeaderBytes+sha256.Size || len(raw) > MaxPreparedAckFrameBytes ||
		!bytes.Equal(raw[:8], PreparedAckCutReadResponseDiscriminator[:]) || !request.valid() {
		return PreparedAckCutReadResponse{}, ErrPreparedAckWire
	}
	cutBytes := binary.LittleEndian.Uint32(raw[320:324])
	if cutBytes == 0 || uint64(cutBytes) > MaxPreparedAckCutBytes ||
		int(cutBytes) != len(raw)-preparedAckCutReadResponseHeaderBytes-sha256.Size {
		return PreparedAckCutReadResponse{}, ErrPreparedAckWire
	}
	digest := sha256.Sum256(raw[:len(raw)-sha256.Size])
	if !bytes.Equal(digest[:], raw[len(raw)-sha256.Size:]) {
		return PreparedAckCutReadResponse{}, ErrPreparedAckWire
	}
	flags := binary.LittleEndian.Uint64(raw[16:24])
	if flags&^uint64(1) != 0 {
		return PreparedAckCutReadResponse{}, ErrPreparedAckWire
	}
	var response PreparedAckCutReadResponse
	response.Operation = CutOperation(binary.LittleEndian.Uint64(raw[8:16]))
	response.RequirePrepared = flags&1 != 0
	copy(response.Nonce[:], raw[24:40])
	copy(response.DrainID[:], raw[40:72])
	copy(response.GrantDigest[:], raw[72:104])
	copy(response.ReceiverNode[:], raw[104:120])
	response.ReceiverIncarnation = binary.LittleEndian.Uint64(raw[120:128])
	copy(response.ReceiverServiceKeyDigest[:], raw[128:160])
	response.ReceiverNodeRevision = binary.LittleEndian.Uint64(raw[160:168])
	response.DirectoryRevision = binary.LittleEndian.Uint64(raw[168:176])
	copy(response.DirectoryDigest[:], raw[176:208])
	response.CatalogGeneration = binary.LittleEndian.Uint64(raw[208:216])
	copy(response.CatalogHeadDigest[:], raw[216:248])
	response.ServiceDirectoryRevision = binary.LittleEndian.Uint64(raw[248:256])
	copy(response.ServiceDirectoryDigest[:], raw[256:288])
	copy(response.SourceCutDigest[:], raw[288:320])
	if err := decodePreparedAckCut(raw[324:324+int(cutBytes)], &response.Cut); err != nil {
		return PreparedAckCutReadResponse{}, err
	}
	if !response.valid(request) {
		return PreparedAckCutReadResponse{}, ErrPreparedAckWire
	}
	return response, nil
}

func decodePreparedAckCut(raw []byte, cut *PreparedAckCut) error {
	if len(raw) == 0 || cut == nil {
		return ErrPreparedAckWire
	}
	if err := vibejson.Unmarshal(raw, cut); err != nil {
		return err
	}
	canonical, err := marshalPreparedAckCut(*cut)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ErrPreparedAckWire
	}
	return nil
}
