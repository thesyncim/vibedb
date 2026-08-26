package requestledger

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/systemkey"
)

const (
	IssuerHighwaterStoragePrefix byte = systemkey.RequestLedgerFirst + 4
	IssuerSequenceStoragePrefix  byte = systemkey.RequestLedgerFirst + 5
	IssuerHighwaterKeyBytes           = 1 + 32 + 32
	IssuerSequenceKeyBytes            = 1 + 32 + 32 + 8
	IssuerHighwaterRecordBytes        = 352
	IssuerSequenceRecordBytes         = 320
	IssuerAdvanceRequestBytes         = 160

	// IssuerSequenceReservationBytes is the exact durable byte reservation
	// charged to every sequenced request at admission. The lane high-water row
	// is shared and is accounted separately when the lane is first installed.
	IssuerSequenceReservationBytes = IssuerSequenceKeyBytes + IssuerSequenceRecordBytes
	IssuerHighwaterResidentBytes   = IssuerHighwaterKeyBytes + IssuerHighwaterRecordBytes
)

var (
	issuerDigestDomain          = []byte("vibedb/request-ledger/issuer\x00")
	issuerHighwaterMagic        = [4]byte{'V', 'R', 'L', 'K'}
	issuerHighwaterDigestDomain = []byte("vibedb/request-ledger/issuer-highwater\x00")
	issuerSequenceMagic         = [4]byte{'V', 'R', 'L', 'V'}
	issuerSequenceDigestDomain  = []byte("vibedb/request-ledger/issuer-sequence\x00")
	issuerAdvanceMagic          = [4]byte{'V', 'R', 'L', 'F'}
	issuerAdvanceDigestDomain   = []byte("vibedb/request-ledger/issuer-advance\x00")
)

type IssuerIdentity struct {
	Scope        ScopeKind
	TenantDigest Digest
	Principal    PrincipalID
	IssuerEpoch  uint64
	IssuerLane   IssuerLane
}

func IssuerIdentityFor(key RequestKey) (IssuerIdentity, error) {
	if !key.Valid() || key.IssuerEpoch == 0 {
		return IssuerIdentity{}, ErrInvalidKey
	}
	return IssuerIdentity{Scope: key.Scope, TenantDigest: key.TenantDigest,
		Principal: key.Principal, IssuerEpoch: key.IssuerEpoch, IssuerLane: key.IssuerLane}, nil
}

func (identity IssuerIdentity) Valid() bool {
	return (identity.Scope == ScopeAuthenticated || identity.Scope == ScopeLocalInstall) &&
		nonzeroDigest(identity.TenantDigest) && identity.Principal != (PrincipalID{}) &&
		identity.IssuerEpoch != 0 && identity.IssuerLane != (IssuerLane{})
}

func IssuerDigest(identity IssuerIdentity) (Digest, error) {
	if !identity.Valid() {
		return Digest{}, ErrInvalidKey
	}
	const domain = "vibedb/request-ledger/issuer\x00"
	var framed [len(domain) + 8 + 32 + 16 + 8 + 8]byte
	at := copy(framed[:], issuerDigestDomain)
	framed[at] = byte(identity.Scope)
	at += 8
	at += copy(framed[at:], identity.TenantDigest[:])
	at += copy(framed[at:], identity.Principal[:])
	binary.LittleEndian.PutUint64(framed[at:at+8], identity.IssuerEpoch)
	copy(framed[at+8:], identity.IssuerLane[:])
	return Digest(sha256.Sum256(framed[:])), nil
}

func issuerHome(identity IssuerIdentity) (LedgerHome, error) {
	if !identity.Valid() {
		return LedgerHome{}, ErrInvalidKey
	}
	const domain = "vibedb/request-ledger/issuer-home\x00"
	var framed [len(domain) + 8 + 32 + 16 + 8 + 8]byte
	at := copy(framed[:], domain)
	framed[at] = byte(identity.Scope)
	at += 8
	at += copy(framed[at:], identity.TenantDigest[:])
	at += copy(framed[at:], identity.Principal[:])
	binary.LittleEndian.PutUint64(framed[at:at+8], identity.IssuerEpoch)
	copy(framed[at+8:], identity.IssuerLane[:])
	return LedgerHome(sha256.Sum256(framed[:])), nil
}

type IssuerHighwaterRecord struct {
	Identity                 IssuerIdentity
	Home                     LedgerHome
	IssuerDigest             Digest
	HighwaterSequence        uint64
	LastKeyDigest            Digest
	LastAckDigest            Digest
	PriorHighwaterDigest     Digest
	LastAdvanceRequestDigest Digest
	HighwaterDigest          Digest
	Revision                 uint64
}

func NewIssuerHighwater(key RequestKey) (IssuerHighwaterRecord, error) {
	identity, err := IssuerIdentityFor(key)
	if err != nil {
		return IssuerHighwaterRecord{}, err
	}
	home, err := issuerHome(identity)
	if err != nil {
		return IssuerHighwaterRecord{}, err
	}
	digest, err := IssuerDigest(identity)
	if err != nil {
		return IssuerHighwaterRecord{}, err
	}
	record := IssuerHighwaterRecord{Identity: identity, Home: home, IssuerDigest: digest, Revision: 1}
	record.HighwaterDigest = issuerHighwaterDigest(record)
	return record, validateIssuerHighwater(record)
}

func AppendIssuerHighwaterKey(dst []byte, home LedgerHome, issuer Digest) []byte {
	dst = append(dst, IssuerHighwaterStoragePrefix)
	dst = append(dst, home[:]...)
	return append(dst, issuer[:]...)
}

func OpenIssuerHighwaterKey(raw []byte) (home LedgerHome, issuer Digest, err error) {
	if len(raw) != IssuerHighwaterKeyBytes || raw[0] != IssuerHighwaterStoragePrefix {
		return home, issuer, ErrCorrupt
	}
	copy(home[:], raw[1:33])
	copy(issuer[:], raw[33:65])
	if home == (LedgerHome{}) || !nonzeroDigest(issuer) {
		return LedgerHome{}, Digest{}, ErrCorrupt
	}
	return home, issuer, nil
}

func AppendIssuerHighwater(dst []byte, record IssuerHighwaterRecord) ([]byte, error) {
	if err := validateIssuerHighwater(record); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, IssuerHighwaterRecordBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], issuerHighwaterMagic[:])
	out[8] = byte(record.Identity.Scope)
	binary.LittleEndian.PutUint64(out[16:24], record.Revision)
	binary.LittleEndian.PutUint64(out[24:32], record.Identity.IssuerEpoch)
	binary.LittleEndian.PutUint64(out[32:40], record.HighwaterSequence)
	copy(out[40:72], record.Home[:])
	putDigest(out[72:104], record.Identity.TenantDigest)
	copy(out[104:120], record.Identity.Principal[:])
	copy(out[120:128], record.Identity.IssuerLane[:])
	putDigest(out[128:160], record.IssuerDigest)
	putDigest(out[160:192], record.LastKeyDigest)
	putDigest(out[192:224], record.LastAckDigest)
	putDigest(out[224:256], record.PriorHighwaterDigest)
	putDigest(out[256:288], record.LastAdvanceRequestDigest)
	putDigest(out[288:320], record.HighwaterDigest)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenIssuerHighwater(raw []byte) (IssuerHighwaterRecord, error) {
	if len(raw) != IssuerHighwaterRecordBytes || !magicOK(raw, issuerHighwaterMagic) ||
		!zeroBytes(raw[4:8]) || !zeroBytes(raw[9:16]) || !zeroBytes(raw[320:348]) || !checksumOK(raw) {
		return IssuerHighwaterRecord{}, ErrCorrupt
	}
	record := IssuerHighwaterRecord{
		Identity: IssuerIdentity{Scope: ScopeKind(raw[8]), IssuerEpoch: binary.LittleEndian.Uint64(raw[24:32]),
			TenantDigest: readDigest(raw[72:104])},
		Revision: binary.LittleEndian.Uint64(raw[16:24]), HighwaterSequence: binary.LittleEndian.Uint64(raw[32:40]),
		IssuerDigest: readDigest(raw[128:160]), LastKeyDigest: readDigest(raw[160:192]),
		LastAckDigest: readDigest(raw[192:224]), PriorHighwaterDigest: readDigest(raw[224:256]),
		LastAdvanceRequestDigest: readDigest(raw[256:288]), HighwaterDigest: readDigest(raw[288:320]),
	}
	copy(record.Home[:], raw[40:72])
	copy(record.Identity.Principal[:], raw[104:120])
	copy(record.Identity.IssuerLane[:], raw[120:128])
	if err := validateIssuerHighwater(record); err != nil {
		return IssuerHighwaterRecord{}, ErrCorrupt
	}
	return record, nil
}

func validateIssuerHighwater(record IssuerHighwaterRecord) error {
	issuer, issuerErr := IssuerDigest(record.Identity)
	home, homeErr := issuerHome(record.Identity)
	if issuerErr != nil || homeErr != nil || issuer != record.IssuerDigest || home != record.Home ||
		record.Revision == 0 || record.HighwaterSequence != record.Revision-1 ||
		(record.HighwaterSequence == 0) !=
			(!nonzeroDigest(record.LastKeyDigest) && !nonzeroDigest(record.LastAckDigest) &&
				!nonzeroDigest(record.PriorHighwaterDigest) && !nonzeroDigest(record.LastAdvanceRequestDigest)) ||
		record.HighwaterSequence != 0 &&
			(!nonzeroDigest(record.LastKeyDigest) || !nonzeroDigest(record.LastAckDigest) ||
				!nonzeroDigest(record.PriorHighwaterDigest) || !nonzeroDigest(record.LastAdvanceRequestDigest)) ||
		record.HighwaterDigest != issuerHighwaterDigest(record) {
		return ErrCorrupt
	}
	return nil
}

func issuerHighwaterDigest(record IssuerHighwaterRecord) Digest {
	const domain = "vibedb/request-ledger/issuer-highwater\x00"
	var framed [len(domain) + 8*3 + 6*sha256.Size + 16 + 16]byte
	at := copy(framed[:], issuerHighwaterDigestDomain)
	framed[at] = byte(record.Identity.Scope)
	binary.LittleEndian.PutUint64(framed[at+8:at+16], record.Revision)
	binary.LittleEndian.PutUint64(framed[at+16:at+24], record.HighwaterSequence)
	at += 24
	for _, digest := range [...]Digest{record.Identity.TenantDigest, record.IssuerDigest,
		record.LastKeyDigest, record.LastAckDigest, record.PriorHighwaterDigest, record.LastAdvanceRequestDigest} {
		at += copy(framed[at:], digest[:])
	}
	at += copy(framed[at:], record.Identity.Principal[:])
	binary.LittleEndian.PutUint64(framed[at:at+8], record.Identity.IssuerEpoch)
	at += 8
	copy(framed[at:], record.Identity.IssuerLane[:])
	return Digest(sha256.Sum256(framed[:]))
}

type IssuerSequencePhase uint8

const (
	IssuerSequenceInvalid IssuerSequencePhase = iota
	IssuerSequenceActive
	IssuerSequenceGCComplete
)

type IssuerSequenceRecord struct {
	Identity      IssuerIdentity
	Home          LedgerHome
	IssuerDigest  Digest
	Request       RequestID
	KeyDigest     Digest
	RequestDigest Digest
	AckDigest     Digest
	RecordDigest  Digest
	Sequence      uint64
	Revision      uint64
	Phase         IssuerSequencePhase
}

func NewIssuerSequence(key RequestKey, requestDigest Digest) (IssuerSequenceRecord, error) {
	identity, err := IssuerIdentityFor(key)
	if err != nil || !nonzeroDigest(requestDigest) {
		return IssuerSequenceRecord{}, ErrInvalidKey
	}
	home, _ := issuerHome(identity)
	issuer, _ := IssuerDigest(identity)
	keyDigest, _ := KeyDigest(key)
	record := IssuerSequenceRecord{Identity: identity, Home: home, IssuerDigest: issuer,
		Request: key.Request, KeyDigest: keyDigest, RequestDigest: requestDigest,
		Sequence: key.IssuerSequence, Revision: 1, Phase: IssuerSequenceActive}
	record.RecordDigest = issuerSequenceDigest(record)
	return record, validateIssuerSequence(record)
}

func MarkIssuerSequenceGCComplete(
	record IssuerSequenceRecord,
	ack AckRecord,
	revision uint64,
) (IssuerSequenceRecord, error) {
	if err := validateIssuerSequence(record); err != nil || errOrNil(validateAck(ack)) != nil ||
		record.Phase != IssuerSequenceActive || ack.GCPhase != AckGCComplete ||
		ack.KeyDigest != record.KeyDigest || ack.RequestDigest != record.RequestDigest ||
		ack.Key.IssuerSequence != record.Sequence || ack.Key.Request != record.Request ||
		!nextRevision(record.Revision, revision) {
		return IssuerSequenceRecord{}, ErrInvalidState
	}
	record.Revision = revision
	record.Phase = IssuerSequenceGCComplete
	record.AckDigest = ack.AckDigest
	record.RecordDigest = issuerSequenceDigest(record)
	return record, validateIssuerSequence(record)
}

func AppendIssuerSequenceKey(dst []byte, home LedgerHome, issuer Digest, sequence uint64) []byte {
	dst = append(dst, IssuerSequenceStoragePrefix)
	dst = append(dst, home[:]...)
	dst = append(dst, issuer[:]...)
	return binary.BigEndian.AppendUint64(dst, sequence)
}

func OpenIssuerSequenceKey(raw []byte) (home LedgerHome, issuer Digest, sequence uint64, err error) {
	if len(raw) != IssuerSequenceKeyBytes || raw[0] != IssuerSequenceStoragePrefix {
		return home, issuer, 0, ErrCorrupt
	}
	copy(home[:], raw[1:33])
	copy(issuer[:], raw[33:65])
	sequence = binary.BigEndian.Uint64(raw[65:73])
	if home == (LedgerHome{}) || !nonzeroDigest(issuer) || sequence == 0 {
		return LedgerHome{}, Digest{}, 0, ErrCorrupt
	}
	return home, issuer, sequence, nil
}

func AppendIssuerSequence(dst []byte, record IssuerSequenceRecord) ([]byte, error) {
	if err := validateIssuerSequence(record); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, IssuerSequenceRecordBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], issuerSequenceMagic[:])
	out[8] = byte(record.Phase)
	out[9] = byte(record.Identity.Scope)
	binary.LittleEndian.PutUint64(out[16:24], record.Revision)
	binary.LittleEndian.PutUint64(out[24:32], record.Sequence)
	binary.LittleEndian.PutUint64(out[32:40], record.Identity.IssuerEpoch)
	copy(out[40:72], record.Home[:])
	putDigest(out[72:104], record.Identity.TenantDigest)
	copy(out[104:120], record.Identity.Principal[:])
	copy(out[120:136], record.Request[:])
	copy(out[136:144], record.Identity.IssuerLane[:])
	putDigest(out[144:176], record.IssuerDigest)
	putDigest(out[176:208], record.KeyDigest)
	putDigest(out[208:240], record.RequestDigest)
	putDigest(out[240:272], record.AckDigest)
	putDigest(out[272:304], record.RecordDigest)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenIssuerSequence(raw []byte) (IssuerSequenceRecord, error) {
	if len(raw) != IssuerSequenceRecordBytes || !magicOK(raw, issuerSequenceMagic) ||
		!zeroBytes(raw[4:8]) || !zeroBytes(raw[10:16]) || !zeroBytes(raw[304:316]) || !checksumOK(raw) {
		return IssuerSequenceRecord{}, ErrCorrupt
	}
	record := IssuerSequenceRecord{
		Phase: IssuerSequencePhase(raw[8]), Identity: IssuerIdentity{Scope: ScopeKind(raw[9]),
			IssuerEpoch: binary.LittleEndian.Uint64(raw[32:40]), TenantDigest: readDigest(raw[72:104])},
		Revision: binary.LittleEndian.Uint64(raw[16:24]), Sequence: binary.LittleEndian.Uint64(raw[24:32]),
		IssuerDigest: readDigest(raw[144:176]), KeyDigest: readDigest(raw[176:208]),
		RequestDigest: readDigest(raw[208:240]), AckDigest: readDigest(raw[240:272]),
		RecordDigest: readDigest(raw[272:304]),
	}
	copy(record.Home[:], raw[40:72])
	copy(record.Identity.Principal[:], raw[104:120])
	copy(record.Request[:], raw[120:136])
	copy(record.Identity.IssuerLane[:], raw[136:144])
	if err := validateIssuerSequence(record); err != nil {
		return IssuerSequenceRecord{}, ErrCorrupt
	}
	return record, nil
}

func validateIssuerSequence(record IssuerSequenceRecord) error {
	issuer, issuerErr := IssuerDigest(record.Identity)
	home, homeErr := issuerHome(record.Identity)
	derivedKey, keyErr := KeyDigest(RequestKey{Scope: record.Identity.Scope,
		TenantDigest: record.Identity.TenantDigest, Principal: record.Identity.Principal,
		Request: record.Request, IssuerEpoch: record.Identity.IssuerEpoch,
		IssuerSequence: record.Sequence, IssuerLane: record.Identity.IssuerLane})
	if issuerErr != nil || homeErr != nil || issuer != record.IssuerDigest || home != record.Home ||
		keyErr != nil || derivedKey != record.KeyDigest ||
		record.Request == (RequestID{}) || !nonzeroDigest(record.KeyDigest) || !nonzeroDigest(record.RequestDigest) ||
		record.Sequence == 0 || record.Revision == 0 ||
		record.Phase < IssuerSequenceActive || record.Phase > IssuerSequenceGCComplete ||
		(record.Phase == IssuerSequenceActive && (record.Revision != 1 || nonzeroDigest(record.AckDigest))) ||
		(record.Phase == IssuerSequenceGCComplete && (record.Revision <= 1 || !nonzeroDigest(record.AckDigest))) ||
		record.RecordDigest != issuerSequenceDigest(record) {
		return ErrCorrupt
	}
	return nil
}

func issuerSequenceDigest(record IssuerSequenceRecord) Digest {
	const domain = "vibedb/request-ledger/issuer-sequence\x00"
	var framed [len(domain) + 24 + 5*sha256.Size + 16 + 16 + 8 + 8]byte
	at := copy(framed[:], issuerSequenceDigestDomain)
	framed[at], framed[at+1] = byte(record.Phase), byte(record.Identity.Scope)
	binary.LittleEndian.PutUint64(framed[at+8:at+16], record.Revision)
	binary.LittleEndian.PutUint64(framed[at+16:at+24], record.Sequence)
	at += 24
	for _, digest := range [...]Digest{record.Identity.TenantDigest, record.IssuerDigest,
		record.KeyDigest, record.RequestDigest, record.AckDigest} {
		at += copy(framed[at:], digest[:])
	}
	at += copy(framed[at:], record.Identity.Principal[:])
	at += copy(framed[at:], record.Request[:])
	binary.LittleEndian.PutUint64(framed[at:at+8], record.Identity.IssuerEpoch)
	at += 8
	copy(framed[at:], record.Identity.IssuerLane[:])
	return Digest(sha256.Sum256(framed[:]))
}

type IssuerAdvanceRequest struct {
	Sequence                uint64
	IssuerDigest            Digest
	ExpectedHighwaterDigest Digest
	SequenceRecordDigest    Digest
	AckDigest               Digest
}

func NewIssuerAdvanceRequest(
	highwater IssuerHighwaterRecord,
	sequence IssuerSequenceRecord,
	ack AckRecord,
) (IssuerAdvanceRequest, error) {
	if err := validateIssuerHighwater(highwater); err != nil ||
		errOrNil(validateIssuerSequence(sequence)) != nil || errOrNil(validateAck(ack)) != nil ||
		sequence.Phase != IssuerSequenceGCComplete || ack.GCPhase != AckGCComplete ||
		sequence.IssuerDigest != highwater.IssuerDigest || sequence.AckDigest != ack.AckDigest ||
		sequence.KeyDigest != ack.KeyDigest || sequence.RequestDigest != ack.RequestDigest ||
		sequence.Request != ack.Key.Request || sequence.Sequence != ack.Key.IssuerSequence {
		return IssuerAdvanceRequest{}, ErrInvalidState
	}
	request := IssuerAdvanceRequest{Sequence: sequence.Sequence, IssuerDigest: highwater.IssuerDigest,
		ExpectedHighwaterDigest: highwater.HighwaterDigest,
		SequenceRecordDigest:    sequence.RecordDigest, AckDigest: ack.AckDigest}
	if err := validateIssuerAdvanceRequest(request); err != nil {
		return IssuerAdvanceRequest{}, err
	}
	return request, nil
}

func AppendIssuerAdvanceRequest(dst []byte, request IssuerAdvanceRequest) ([]byte, error) {
	if err := validateIssuerAdvanceRequest(request); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, IssuerAdvanceRequestBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], issuerAdvanceMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], request.Sequence)
	putDigest(out[16:48], request.IssuerDigest)
	putDigest(out[48:80], request.ExpectedHighwaterDigest)
	putDigest(out[80:112], request.SequenceRecordDigest)
	putDigest(out[112:144], request.AckDigest)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenIssuerAdvanceRequest(raw []byte) (IssuerAdvanceRequest, error) {
	if len(raw) != IssuerAdvanceRequestBytes || !magicOK(raw, issuerAdvanceMagic) ||
		!zeroBytes(raw[4:8]) || !zeroBytes(raw[144:156]) || !checksumOK(raw) {
		return IssuerAdvanceRequest{}, ErrCorrupt
	}
	request := IssuerAdvanceRequest{Sequence: binary.LittleEndian.Uint64(raw[8:16]),
		IssuerDigest: readDigest(raw[16:48]), ExpectedHighwaterDigest: readDigest(raw[48:80]),
		SequenceRecordDigest: readDigest(raw[80:112]), AckDigest: readDigest(raw[112:144])}
	if err := validateIssuerAdvanceRequest(request); err != nil {
		return IssuerAdvanceRequest{}, ErrCorrupt
	}
	return request, nil
}

func validateIssuerAdvanceRequest(request IssuerAdvanceRequest) error {
	if request.Sequence == 0 || !nonzeroDigest(request.IssuerDigest) ||
		!nonzeroDigest(request.ExpectedHighwaterDigest) || !nonzeroDigest(request.SequenceRecordDigest) ||
		!nonzeroDigest(request.AckDigest) {
		return ErrCorrupt
	}
	return nil
}

func IssuerAdvanceRequestDigest(request IssuerAdvanceRequest) Digest {
	const domain = "vibedb/request-ledger/issuer-advance\x00"
	var framed [len(domain) + 8 + 4*sha256.Size]byte
	at := copy(framed[:], issuerAdvanceDigestDomain)
	binary.LittleEndian.PutUint64(framed[at:at+8], request.Sequence)
	at += 8
	for _, digest := range [...]Digest{request.IssuerDigest, request.ExpectedHighwaterDigest,
		request.SequenceRecordDigest, request.AckDigest} {
		at += copy(framed[at:], digest[:])
	}
	return Digest(sha256.Sum256(framed[:]))
}

func AdvanceIssuerHighwater(
	highwater IssuerHighwaterRecord,
	sequence IssuerSequenceRecord,
	ack AckRecord,
	request IssuerAdvanceRequest,
	revision uint64,
) (IssuerHighwaterRecord, error) {
	if err := validateIssuerHighwater(highwater); err != nil || errOrNil(validateIssuerSequence(sequence)) != nil ||
		errOrNil(validateAck(ack)) != nil || errOrNil(validateIssuerAdvanceRequest(request)) != nil ||
		sequence.Phase != IssuerSequenceGCComplete || ack.GCPhase != AckGCComplete ||
		highwater.HighwaterSequence == ^uint64(0) || sequence.Sequence != highwater.HighwaterSequence+1 ||
		request.Sequence != sequence.Sequence || request.IssuerDigest != highwater.IssuerDigest ||
		request.ExpectedHighwaterDigest != highwater.HighwaterDigest ||
		request.SequenceRecordDigest != sequence.RecordDigest || request.AckDigest != ack.AckDigest ||
		sequence.IssuerDigest != highwater.IssuerDigest || sequence.AckDigest != ack.AckDigest ||
		ack.KeyDigest != sequence.KeyDigest || ack.RequestDigest != sequence.RequestDigest ||
		ack.Key.IssuerSequence != sequence.Sequence || ack.Key.Request != sequence.Request ||
		!nextRevision(highwater.Revision, revision) {
		return IssuerHighwaterRecord{}, ErrInvalidState
	}
	identity, identityErr := IssuerIdentityFor(ack.Key)
	if identityErr != nil || identity != highwater.Identity || identity != sequence.Identity {
		return IssuerHighwaterRecord{}, ErrInvalidState
	}
	prior := highwater.HighwaterDigest
	highwater.Revision = revision
	highwater.HighwaterSequence = sequence.Sequence
	highwater.LastKeyDigest = sequence.KeyDigest
	highwater.LastAckDigest = ack.AckDigest
	highwater.PriorHighwaterDigest = prior
	highwater.LastAdvanceRequestDigest = IssuerAdvanceRequestDigest(request)
	highwater.HighwaterDigest = issuerHighwaterDigest(highwater)
	return highwater, validateIssuerHighwater(highwater)
}

func SameIssuerAdvance(highwater IssuerHighwaterRecord, request IssuerAdvanceRequest) bool {
	return validateIssuerHighwater(highwater) == nil && validateIssuerAdvanceRequest(request) == nil &&
		highwater.HighwaterSequence == request.Sequence &&
		highwater.PriorHighwaterDigest == request.ExpectedHighwaterDigest &&
		highwater.LastAdvanceRequestDigest == IssuerAdvanceRequestDigest(request)
}

// IssuerHighwaterCoversKey reports whether a sequenced identity has already
// been retired by its issuer lane. It is the anti-resurrection admission check
// performed before looking up a per-request row. Arbitrary-ID keys never match.
func IssuerHighwaterCoversKey(highwater IssuerHighwaterRecord, key RequestKey) bool {
	if validateIssuerHighwater(highwater) != nil || key.IssuerEpoch == 0 ||
		key.IssuerSequence == 0 || key.IssuerSequence > highwater.HighwaterSequence {
		return false
	}
	identity, err := IssuerIdentityFor(key)
	return err == nil && identity == highwater.Identity
}

func IssuerHighwaterCoversAck(highwater IssuerHighwaterRecord, ack AckRecord) bool {
	if validateIssuerHighwater(highwater) != nil || validateAck(ack) != nil || ack.GCPhase != AckGCComplete ||
		ack.Key.IssuerEpoch == 0 ||
		ack.Key.IssuerSequence == 0 || ack.Key.IssuerSequence > highwater.HighwaterSequence {
		return false
	}
	return IssuerHighwaterCoversKey(highwater, ack.Key)
}
