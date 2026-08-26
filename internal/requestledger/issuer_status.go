package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const IssuerLaneStatusBytes = 80 + IssuerHighwaterRecordBytes + IssuerSequenceRecordBytes + AckRecordBytes + checksumBytes

var issuerLaneStatusMagic = [4]byte{'V', 'R', 'L', 'U'}
var issuerLaneStatusDigestDomain = []byte("vibedb/request-ledger/issuer-lane-status\x00")

// IssuerLaneStatus is one bounded coherent point-read image for a sequenced
// issuer lane. NextFound identifies the exact sequence immediately following
// HighwaterSequence. AdvanceReady is true only when Sequence and Ack contain
// the complete witness required by NewIssuerAdvanceRequest.
type IssuerLaneStatus struct {
	RangeIdentity Digest
	Highwater     IssuerHighwaterRecord
	NextFound     bool
	AdvanceReady  bool
	Sequence      IssuerSequenceRecord
	Ack           AckRecord
}

func NewIssuerLaneStatus(
	rangeIdentity Digest,
	highwater IssuerHighwaterRecord,
	sequence *IssuerSequenceRecord,
	ack *AckRecord,
) (IssuerLaneStatus, error) {
	status := IssuerLaneStatus{RangeIdentity: rangeIdentity, Highwater: highwater}
	if sequence != nil {
		status.NextFound = true
		status.Sequence = *sequence
	}
	if ack != nil {
		status.AdvanceReady = true
		status.Ack = *ack
	}
	if err := validateIssuerLaneStatus(status); err != nil {
		return IssuerLaneStatus{}, err
	}
	return status, nil
}

func AppendIssuerLaneStatus(dst []byte, status IssuerLaneStatus) ([]byte, error) {
	if err := validateIssuerLaneStatus(status); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, IssuerLaneStatusBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], issuerLaneStatusMagic[:])
	if status.NextFound {
		out[4] = 1
	}
	if status.AdvanceReady {
		out[5] = 1
	}
	binary.LittleEndian.PutUint64(out[8:16], status.Highwater.HighwaterSequence)
	copy(out[16:48], status.RangeIdentity[:])
	digest := issuerLaneStatusDigest(status)
	copy(out[48:80], digest[:])
	var err error
	highwaterAt := start + 80
	dst, err = AppendIssuerHighwater(dst[:highwaterAt], status.Highwater)
	if err != nil {
		return dst[:start], err
	}
	sequenceAt := highwaterAt + IssuerHighwaterRecordBytes
	if status.NextFound {
		dst, err = AppendIssuerSequence(dst[:sequenceAt], status.Sequence)
		if err != nil {
			return dst[:start], err
		}
	}
	ackAt := sequenceAt + IssuerSequenceRecordBytes
	if status.AdvanceReady {
		dst, err = AppendAck(dst[:ackAt], status.Ack)
		if err != nil {
			return dst[:start], err
		}
	}
	dst = dst[:start+IssuerLaneStatusBytes-checksumBytes]
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenIssuerLaneStatus(raw []byte) (IssuerLaneStatus, error) {
	if len(raw) != IssuerLaneStatusBytes || !magicOK(raw, issuerLaneStatusMagic) ||
		raw[4] > 1 || raw[5] > 1 || !zeroBytes(raw[6:8]) || !checksumOK(raw) {
		return IssuerLaneStatus{}, ErrCorrupt
	}
	var rangeIdentity Digest
	copy(rangeIdentity[:], raw[16:48])
	encodedDigest := readDigest(raw[48:80])
	highwater, err := OpenIssuerHighwater(raw[80 : 80+IssuerHighwaterRecordBytes])
	if err != nil || binary.LittleEndian.Uint64(raw[8:16]) != highwater.HighwaterSequence {
		return IssuerLaneStatus{}, ErrCorrupt
	}
	status := IssuerLaneStatus{RangeIdentity: rangeIdentity, Highwater: highwater, NextFound: raw[4] == 1, AdvanceReady: raw[5] == 1}
	sequenceAt := 80 + IssuerHighwaterRecordBytes
	ackAt := sequenceAt + IssuerSequenceRecordBytes
	if status.NextFound {
		status.Sequence, err = OpenIssuerSequence(raw[sequenceAt:ackAt])
		if err != nil {
			return IssuerLaneStatus{}, ErrCorrupt
		}
	} else if !zeroBytes(raw[sequenceAt:ackAt]) {
		return IssuerLaneStatus{}, ErrCorrupt
	}
	if status.AdvanceReady {
		status.Ack, err = OpenAck(raw[ackAt : ackAt+AckRecordBytes])
		if err != nil {
			return IssuerLaneStatus{}, ErrCorrupt
		}
	} else if !zeroBytes(raw[ackAt : ackAt+AckRecordBytes]) {
		return IssuerLaneStatus{}, ErrCorrupt
	}
	if err := validateIssuerLaneStatus(status); err != nil {
		return IssuerLaneStatus{}, ErrCorrupt
	}
	if encodedDigest != issuerLaneStatusDigest(status) {
		return IssuerLaneStatus{}, ErrCorrupt
	}
	return status, nil
}

func issuerLaneStatusDigest(status IssuerLaneStatus) Digest {
	var framed [16 + 4*sha256.Size]byte
	if status.NextFound {
		framed[0] = 1
	}
	if status.AdvanceReady {
		framed[1] = 1
	}
	binary.LittleEndian.PutUint64(framed[8:16], status.Highwater.HighwaterSequence)
	copy(framed[16:48], status.RangeIdentity[:])
	copy(framed[48:80], status.Highwater.HighwaterDigest[:])
	copy(framed[80:112], status.Sequence.RecordDigest[:])
	copy(framed[112:144], status.Ack.AckDigest[:])
	hash := sha256.New()
	_, _ = hash.Write(issuerLaneStatusDigestDomain)
	_, _ = hash.Write(framed[:])
	var digest Digest
	_ = hash.Sum(digest[:0])
	return digest
}

func validateIssuerLaneStatus(status IssuerLaneStatus) error {
	if !nonzeroDigest(status.RangeIdentity) || validateIssuerHighwater(status.Highwater) != nil ||
		status.AdvanceReady && !status.NextFound {
		return ErrInvalidState
	}
	if !status.NextFound {
		if status.Sequence != (IssuerSequenceRecord{}) || status.Ack != (AckRecord{}) {
			return ErrInvalidState
		}
		return nil
	}
	sequence := status.Sequence
	if status.Highwater.HighwaterSequence == ^uint64(0) ||
		validateIssuerSequence(sequence) != nil || sequence.Identity != status.Highwater.Identity ||
		sequence.Home != status.Highwater.Home || sequence.IssuerDigest != status.Highwater.IssuerDigest ||
		sequence.Sequence != status.Highwater.HighwaterSequence+1 {
		return ErrInvalidState
	}
	if !status.AdvanceReady {
		if sequence.Phase != IssuerSequenceActive || status.Ack != (AckRecord{}) {
			return ErrInvalidState
		}
		return nil
	}
	if sequence.Phase != IssuerSequenceGCComplete || validateAck(status.Ack) != nil ||
		status.Ack.GCPhase != AckGCComplete || status.Ack.KeyDigest != sequence.KeyDigest ||
		status.Ack.RequestDigest != sequence.RequestDigest || status.Ack.AckDigest != sequence.AckDigest ||
		status.Ack.Key.IssuerSequence != sequence.Sequence || status.Ack.Key.Request != sequence.Request {
		return ErrInvalidState
	}
	identity, err := IssuerIdentityFor(status.Ack.Key)
	if err != nil || identity != status.Highwater.Identity {
		return ErrInvalidState
	}
	return nil
}
