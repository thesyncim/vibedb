package executionpin

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
)

const (
	AcquireCertificateBytes  = 352
	LeaseCertificateBytes    = 184
	TerminalCertificateBytes = 288
)

var (
	acquireCertificateMagic  = [8]byte{'V', 'E', 'L', 'A', 'C', 'Q', 0, 0}
	leaseCertificateMagic    = [8]byte{'V', 'E', 'L', 'L', 'E', 'A', 0, 0}
	terminalCertificateMagic = [8]byte{'V', 'E', 'L', 'T', 'E', 'R', 0, 0}

	acquireCertificateDomain  = []byte("vibedb/logical-execution-pin/acquire-certificate\x00")
	leaseCertificateDomain    = []byte("vibedb/logical-execution-pin/lease-certificate\x00")
	terminalCertificateDomain = []byte("vibedb/logical-execution-pin/terminal-certificate\x00")
)

type AcquireCertificate struct {
	PinID               PinID
	Binding             Binding
	AuthorityDigest     Digest
	Applied             uint64
	Controller          ID
	ControllerEpoch     uint64
	LeaseAppliedThrough uint64
}

func (certificate AcquireCertificate) Valid() bool {
	derived, err := DerivePinID(certificate.Binding)
	return err == nil && certificate.PinID == derived &&
		certificate.AuthorityDigest != (Digest{}) && certificate.Applied != 0 &&
		certificate.Controller != (ID{}) && certificate.ControllerEpoch != 0 &&
		certificate.LeaseAppliedThrough > certificate.Applied
}

type LeaseCertificate struct {
	PinID                    PinID
	AcquireCertificateDigest Digest
	AuthorityDigest          Digest
	Controller               ID
	ControllerEpoch          uint64
	LeaseAppliedThrough      uint64
	Revision                 uint64
	Applied                  uint64
}

func (certificate LeaseCertificate) Valid() bool {
	return certificate.PinID != (PinID{}) &&
		certificate.AcquireCertificateDigest != (Digest{}) &&
		certificate.AuthorityDigest != (Digest{}) && certificate.Controller != (ID{}) &&
		certificate.ControllerEpoch != 0 && certificate.LeaseAppliedThrough > certificate.Applied &&
		certificate.Revision != 0 && certificate.Applied != 0
}

type TerminalCertificate struct {
	Status                      Status
	PinID                       PinID
	RequestKeyDigest            Digest
	AcquireCertificateDigest    Digest
	LeaseCertificateDigest      Digest
	AuthorityDigest             Digest
	PrepareTerminalDigest       Digest
	Controller                  ID
	ControllerEpoch             uint64
	ExpectedLeaseAppliedThrough uint64
	Applied                     uint64
}

func (certificate TerminalCertificate) Valid() bool {
	if certificate.PinID == (PinID{}) || certificate.RequestKeyDigest == (Digest{}) ||
		certificate.AuthorityDigest == (Digest{}) || certificate.Controller == (ID{}) ||
		certificate.ControllerEpoch == 0 || certificate.ExpectedLeaseAppliedThrough == 0 ||
		certificate.Applied == 0 {
		return false
	}
	switch certificate.Status {
	case StatusReleased:
		return certificate.AcquireCertificateDigest != (Digest{}) &&
			certificate.LeaseCertificateDigest != (Digest{}) &&
			certificate.PrepareTerminalDigest != (Digest{})
	case StatusExpired:
		return certificate.PrepareTerminalDigest == (Digest{}) &&
			certificate.Applied > certificate.ExpectedLeaseAppliedThrough &&
			((certificate.AcquireCertificateDigest == (Digest{}) &&
				certificate.LeaseCertificateDigest == (Digest{})) ||
				(certificate.AcquireCertificateDigest != (Digest{}) &&
					certificate.LeaseCertificateDigest != (Digest{})))
	default:
		return false
	}
}

func AppendAcquireCertificate(dst []byte, certificate AcquireCertificate) ([]byte, error) {
	if !certificate.Valid() {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, AcquireCertificateBytes)...)
	frame := dst[start:]
	copy(frame[0:8], acquireCertificateMagic[:])
	copy(frame[8:40], certificate.PinID[:])
	encodedBinding := appendBinding(frame[40:40], certificate.Binding)
	if len(encodedBinding) != bindingBytes {
		panic("executionpin: impossible acquire binding geometry")
	}
	copy(frame[248:280], certificate.AuthorityDigest[:])
	binary.LittleEndian.PutUint64(frame[280:288], certificate.Applied)
	copy(frame[288:304], certificate.Controller[:])
	binary.LittleEndian.PutUint64(frame[304:312], certificate.ControllerEpoch)
	binary.LittleEndian.PutUint64(frame[312:320], certificate.LeaseAppliedThrough)
	sealSHA(frame, acquireCertificateDomain)
	return dst, nil
}

func OpenAcquireCertificate(raw []byte) (AcquireCertificate, error) {
	if len(raw) != AcquireCertificateBytes || !bytes.Equal(raw[0:8], acquireCertificateMagic[:]) ||
		!verifySHA(raw, acquireCertificateDomain) {
		return AcquireCertificate{}, ErrCorrupt
	}
	binding, ok := openBinding(raw[40:248])
	if !ok {
		return AcquireCertificate{}, ErrCorrupt
	}
	certificate := AcquireCertificate{Binding: binding}
	copy(certificate.PinID[:], raw[8:40])
	copy(certificate.AuthorityDigest[:], raw[248:280])
	certificate.Applied = binary.LittleEndian.Uint64(raw[280:288])
	copy(certificate.Controller[:], raw[288:304])
	certificate.ControllerEpoch = binary.LittleEndian.Uint64(raw[304:312])
	certificate.LeaseAppliedThrough = binary.LittleEndian.Uint64(raw[312:320])
	if !certificate.Valid() {
		return AcquireCertificate{}, ErrCorrupt
	}
	return certificate, nil
}

func AppendLeaseCertificate(dst []byte, certificate LeaseCertificate) ([]byte, error) {
	if !certificate.Valid() {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, LeaseCertificateBytes)...)
	frame := dst[start:]
	copy(frame[0:8], leaseCertificateMagic[:])
	copy(frame[8:40], certificate.PinID[:])
	copy(frame[40:72], certificate.AcquireCertificateDigest[:])
	copy(frame[72:104], certificate.AuthorityDigest[:])
	copy(frame[104:120], certificate.Controller[:])
	binary.LittleEndian.PutUint64(frame[120:128], certificate.ControllerEpoch)
	binary.LittleEndian.PutUint64(frame[128:136], certificate.LeaseAppliedThrough)
	binary.LittleEndian.PutUint64(frame[136:144], certificate.Revision)
	binary.LittleEndian.PutUint64(frame[144:152], certificate.Applied)
	sealSHA(frame, leaseCertificateDomain)
	return dst, nil
}

func OpenLeaseCertificate(raw []byte) (LeaseCertificate, error) {
	if len(raw) != LeaseCertificateBytes || !bytes.Equal(raw[0:8], leaseCertificateMagic[:]) ||
		!verifySHA(raw, leaseCertificateDomain) {
		return LeaseCertificate{}, ErrCorrupt
	}
	var certificate LeaseCertificate
	copy(certificate.PinID[:], raw[8:40])
	copy(certificate.AcquireCertificateDigest[:], raw[40:72])
	copy(certificate.AuthorityDigest[:], raw[72:104])
	copy(certificate.Controller[:], raw[104:120])
	certificate.ControllerEpoch = binary.LittleEndian.Uint64(raw[120:128])
	certificate.LeaseAppliedThrough = binary.LittleEndian.Uint64(raw[128:136])
	certificate.Revision = binary.LittleEndian.Uint64(raw[136:144])
	certificate.Applied = binary.LittleEndian.Uint64(raw[144:152])
	if !certificate.Valid() {
		return LeaseCertificate{}, ErrCorrupt
	}
	return certificate, nil
}

func AppendTerminalCertificate(dst []byte, certificate TerminalCertificate) ([]byte, error) {
	if !certificate.Valid() {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, TerminalCertificateBytes)...)
	frame := dst[start:]
	copy(frame[0:8], terminalCertificateMagic[:])
	frame[8] = byte(certificate.Status)
	copy(frame[16:48], certificate.PinID[:])
	copy(frame[48:80], certificate.RequestKeyDigest[:])
	copy(frame[80:112], certificate.AcquireCertificateDigest[:])
	copy(frame[112:144], certificate.LeaseCertificateDigest[:])
	copy(frame[144:176], certificate.AuthorityDigest[:])
	copy(frame[176:208], certificate.PrepareTerminalDigest[:])
	copy(frame[208:224], certificate.Controller[:])
	binary.LittleEndian.PutUint64(frame[224:232], certificate.ControllerEpoch)
	binary.LittleEndian.PutUint64(frame[232:240], certificate.ExpectedLeaseAppliedThrough)
	binary.LittleEndian.PutUint64(frame[248:256], certificate.Applied)
	sealSHA(frame, terminalCertificateDomain)
	return dst, nil
}

func OpenTerminalCertificate(raw []byte) (TerminalCertificate, error) {
	if len(raw) != TerminalCertificateBytes ||
		!bytes.Equal(raw[0:8], terminalCertificateMagic[:]) || !allZero(raw[9:16]) ||
		!verifySHA(raw, terminalCertificateDomain) {
		return TerminalCertificate{}, ErrCorrupt
	}
	certificate := TerminalCertificate{Status: Status(raw[8])}
	copy(certificate.PinID[:], raw[16:48])
	copy(certificate.RequestKeyDigest[:], raw[48:80])
	copy(certificate.AcquireCertificateDigest[:], raw[80:112])
	copy(certificate.LeaseCertificateDigest[:], raw[112:144])
	copy(certificate.AuthorityDigest[:], raw[144:176])
	copy(certificate.PrepareTerminalDigest[:], raw[176:208])
	copy(certificate.Controller[:], raw[208:224])
	certificate.ControllerEpoch = binary.LittleEndian.Uint64(raw[224:232])
	certificate.ExpectedLeaseAppliedThrough = binary.LittleEndian.Uint64(raw[232:240])
	if !allZero(raw[240:248]) {
		return TerminalCertificate{}, ErrCorrupt
	}
	certificate.Applied = binary.LittleEndian.Uint64(raw[248:256])
	if !certificate.Valid() {
		return TerminalCertificate{}, ErrCorrupt
	}
	return certificate, nil
}

func AcquireCertificateDigest(certificate AcquireCertificate) (Digest, error) {
	var storage [AcquireCertificateBytes]byte
	encoded, err := AppendAcquireCertificate(storage[:0], certificate)
	if err != nil {
		return Digest{}, err
	}
	return Digest(sha256.Sum256(encoded)), nil
}

func LeaseCertificateDigest(certificate LeaseCertificate) (Digest, error) {
	var storage [LeaseCertificateBytes]byte
	encoded, err := AppendLeaseCertificate(storage[:0], certificate)
	if err != nil {
		return Digest{}, err
	}
	return Digest(sha256.Sum256(encoded)), nil
}

func TerminalCertificateDigest(certificate TerminalCertificate) (Digest, error) {
	var storage [TerminalCertificateBytes]byte
	encoded, err := AppendTerminalCertificate(storage[:0], certificate)
	if err != nil {
		return Digest{}, err
	}
	return Digest(sha256.Sum256(encoded)), nil
}

func (record Record) AcquireCertificate() (AcquireCertificate, bool) {
	if !record.Valid() || record.AcquireApplied == 0 {
		return AcquireCertificate{}, false
	}
	return AcquireCertificate{
		PinID: record.PinID, Binding: record.Binding,
		AuthorityDigest: record.AcquireAuthorityDigest,
		Applied:         record.AcquireApplied, Controller: record.AcquireController,
		ControllerEpoch:     record.AcquireControllerEpoch,
		LeaseAppliedThrough: record.AcquireLeaseAppliedThrough,
	}, true
}

func (record Record) LeaseCertificate() (LeaseCertificate, bool) {
	acquire, ok := record.AcquireCertificate()
	if !ok || record.LeaseRevision == 0 || record.LeaseApplied == 0 {
		return LeaseCertificate{}, false
	}
	digest, err := AcquireCertificateDigest(acquire)
	if err != nil {
		return LeaseCertificate{}, false
	}
	return LeaseCertificate{
		PinID: record.PinID, AcquireCertificateDigest: digest,
		AuthorityDigest: record.CurrentAuthorityDigest, Controller: record.Controller,
		ControllerEpoch: record.ControllerEpoch, LeaseAppliedThrough: record.LeaseAppliedThrough,
		Revision: record.LeaseRevision, Applied: record.LeaseApplied,
	}, true
}

func (record Record) TerminalCertificate() (TerminalCertificate, bool) {
	if !record.Valid() || record.Status == StatusActive || record.TerminalApplied == 0 {
		return TerminalCertificate{}, false
	}
	var acquireDigest, leaseDigest Digest
	if acquire, ok := record.AcquireCertificate(); ok {
		var err error
		acquireDigest, err = AcquireCertificateDigest(acquire)
		if err != nil {
			return TerminalCertificate{}, false
		}
		lease, leaseOK := record.LeaseCertificate()
		if !leaseOK {
			return TerminalCertificate{}, false
		}
		leaseDigest, err = LeaseCertificateDigest(lease)
		if err != nil {
			return TerminalCertificate{}, false
		}
	}
	return TerminalCertificate{
		Status: record.Status, PinID: record.PinID,
		RequestKeyDigest:         record.Binding.RequestKeyDigest,
		AcquireCertificateDigest: acquireDigest, LeaseCertificateDigest: leaseDigest,
		AuthorityDigest:       record.TerminalAuthorityDigest,
		PrepareTerminalDigest: record.PrepareTerminalDigest,
		Controller:            record.Controller, ControllerEpoch: record.ControllerEpoch,
		ExpectedLeaseAppliedThrough: record.LeaseAppliedThrough,
		Applied:                     record.TerminalApplied,
	}, true
}
