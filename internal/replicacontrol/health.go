package replicacontrol

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

// HealthObservationBytes is the exact size of one authenticated health
// response. It contains a distinct response magic, the complete request wire
// image, and six detached Raft/publication counters.
const HealthObservationBytes = 8 + RequestBytes + 6*8

var healthObservationMagic = [8]byte{'V', 'B', 'R', 'H', 'E', 'A', 'L', 0}

// HealthObservationDiscriminator identifies the fixed health response
// grammar. It is deliberately distinct from the complete-cut response magic.
func HealthObservationDiscriminator() [8]byte { return healthObservationMagic }

// HealthObservation is the bounded liveness result used by revision control.
// It has no State, Publication, progress map, or snapshot certificate.
type HealthObservation struct {
	Request           Request
	MemberID          uint64
	LeaderID          uint64
	Term              uint64
	Commit            uint64
	Applied           uint64
	ReplicaSetVersion uint64
}

// AppendHealthObservation appends one canonical fixed-size health response.
func AppendHealthObservation(dst []byte, observation HealthObservation) ([]byte, error) {
	if !validHealthObservation(observation) {
		return dst, ErrControl
	}
	if len(dst) > math.MaxInt-HealthObservationBytes {
		return dst, ErrBound
	}
	start := len(dst)
	dst = append(dst, make([]byte, HealthObservationBytes)...)
	b := dst[start:]
	copy(b[:8], healthObservationMagic[:])
	var request [RequestBytes]byte
	encodedRequest, err := AppendRequest(request[:0], observation.Request)
	if err != nil || len(encodedRequest) != RequestBytes {
		return dst[:start], err
	}
	copy(b[8:8+RequestBytes], encodedRequest)
	values := [...]uint64{
		observation.MemberID, observation.LeaderID, observation.Term,
		observation.Commit, observation.Applied, observation.ReplicaSetVersion,
	}
	for index, value := range values {
		offset := 8 + RequestBytes + index*8
		binary.BigEndian.PutUint64(b[offset:offset+8], value)
	}
	return dst, nil
}

// OpenHealthObservation opens one exact canonical health response.
func OpenHealthObservation(raw []byte) (HealthObservation, error) {
	if len(raw) != HealthObservationBytes || !bytes.Equal(raw[:8], healthObservationMagic[:]) {
		return HealthObservation{}, ErrControl
	}
	request, err := OpenRequest(raw[8 : 8+RequestBytes])
	if err != nil || !request.HealthOnly {
		return HealthObservation{}, ErrControl
	}
	observation := HealthObservation{Request: request}
	values := [...]*uint64{
		&observation.MemberID, &observation.LeaderID, &observation.Term,
		&observation.Commit, &observation.Applied, &observation.ReplicaSetVersion,
	}
	for index := range values {
		offset := 8 + RequestBytes + index*8
		*values[index] = binary.BigEndian.Uint64(raw[offset : offset+8])
	}
	if !validHealthObservation(observation) {
		return HealthObservation{}, ErrControl
	}
	return observation, nil
}

// ReadHealthObservation reads one complete fixed-size health response.
func ReadHealthObservation(reader io.Reader) (HealthObservation, error) {
	var raw [HealthObservationBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return HealthObservation{}, err
	}
	return OpenHealthObservation(raw[:])
}

// WriteHealthObservation writes one complete fixed-size health response.
func WriteHealthObservation(writer io.Writer, observation HealthObservation) error {
	var raw [HealthObservationBytes]byte
	encoded, err := AppendHealthObservation(raw[:0], observation)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded)
}

func validHealthObservation(observation HealthObservation) bool {
	request := observation.Request
	return request.HealthOnly && validRequest(request) &&
		request.ExpectedReplicaSetVersion != 0 &&
		observation.MemberID == request.TargetMember && observation.MemberID != 0 &&
		observation.LeaderID != 0 && observation.Term != 0 &&
		observation.Commit != 0 && observation.Applied != 0 &&
		observation.Applied <= observation.Commit &&
		observation.ReplicaSetVersion == request.ExpectedReplicaSetVersion
}
