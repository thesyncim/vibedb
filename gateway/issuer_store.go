package gateway

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

var (
	ErrGatewayIssuerStore      = errors.New("gateway: invalid issuer store")
	ErrGatewayIssuerStoreInUse = errors.New("gateway: issuer store is already open")
	ErrGatewayIssuerGrant      = errors.New("gateway: invalid issuer lane grant")
)

const (
	gatewayIssuerStoreFormat = 1
	gatewayIssuerStoreHeader = 104
	MaxGatewayIssuerLanes    = 1024
)

var (
	gatewayIssuerStoreMagic  = [4]byte{'V', 'I', 'S', '1'}
	gatewayIssuerGrantDomain = []byte("vibedb/gateway/issuer-lane-grant\x00")
)

// GatewayIssuerStoreOptions fixes one gateway installation's issuer identity.
// Lanes cannot be resized in place. Installation and Epoch may be zero only
// when creating a store, which installs random identity, epoch one, random
// nonzero lanes, and a signing secret without consulting a wall clock.
type GatewayIssuerStoreOptions struct {
	Path         string
	Lanes        uint16
	Installation replication.ID128
	Epoch        uint64
}

// GatewayIssuerContract is the non-secret part of the installed handshake.
// Client request sequences are deliberately absent: clients own them and the
// replicated request ledger enforces monotonicity.
type GatewayIssuerContract struct {
	Installation replication.ID128
	Epoch        uint64
	Lanes        []requestledger.IssuerLane
	Digest       replication.Digest
}

// GatewayIssuerLaneGrant binds one lane to an authenticated tenant/principal.
// It authorizes an identity shape, not a request sequence allocation.
type GatewayIssuerLaneGrant struct {
	Installation  replication.ID128
	Epoch         uint64
	Lane          requestledger.IssuerLane
	Scope         requestledger.ScopeKind
	TenantDigest  requestledger.Digest
	Principal     requestledger.PrincipalID
	Authenticator replication.Digest
}

// GatewayIssuerStore holds installation authority only. It deliberately has
// no request counter, reservation API, or recovery high-water.
type GatewayIssuerStore struct {
	mu           sync.Mutex
	lock         *os.File
	installation replication.ID128
	epoch        uint64
	lanes        []requestledger.IssuerLane
	secret       replication.Digest
	closed       bool
}

func OpenGatewayIssuerStore(options GatewayIssuerStoreOptions) (*GatewayIssuerStore, error) {
	if options.Path == "" || options.Lanes == 0 || options.Lanes > MaxGatewayIssuerLanes ||
		(options.Installation == (replication.ID128{})) != (options.Epoch == 0) {
		return nil, ErrGatewayIssuerStore
	}
	lock, err := openGatewayIssuerLock(options.Path + ".lock")
	if err != nil {
		return nil, err
	}
	fail := func(openErr error) (*GatewayIssuerStore, error) {
		return nil, errors.Join(openErr, closeGatewayIssuerLock(lock))
	}
	raw, err := os.ReadFile(options.Path)
	if err == nil {
		installation, epoch, lanes, secret, openErr := openGatewayIssuerStoreRecord(raw)
		if openErr != nil || len(lanes) != int(options.Lanes) ||
			options.Installation != (replication.ID128{}) &&
				(installation != options.Installation || epoch != options.Epoch) {
			return fail(ErrGatewayIssuerStore)
		}
		return &GatewayIssuerStore{lock: lock, installation: installation,
			epoch: epoch, lanes: lanes, secret: secret}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fail(err)
	}
	installation, epoch := options.Installation, options.Epoch
	if installation == (replication.ID128{}) {
		if _, err = io.ReadFull(rand.Reader, installation[:]); err != nil ||
			installation == (replication.ID128{}) {
			return fail(errors.Join(ErrGatewayIssuerStore, err))
		}
		epoch = 1
	}
	lanes := make([]requestledger.IssuerLane, options.Lanes)
	for index := range lanes {
		for {
			if _, err = io.ReadFull(rand.Reader, lanes[index][:]); err != nil {
				return fail(err)
			}
			if lanes[index] != (requestledger.IssuerLane{}) && uniqueGatewayIssuerLane(lanes, index) {
				break
			}
		}
	}
	var secret replication.Digest
	if _, err = io.ReadFull(rand.Reader, secret[:]); err != nil || secret == (replication.Digest{}) {
		return fail(errors.Join(ErrGatewayIssuerStore, err))
	}
	raw, err = appendGatewayIssuerStoreRecord(nil, installation, epoch, lanes, secret)
	if err != nil {
		return fail(err)
	}
	if err = installGatewayIssuerStore(options.Path, raw); err != nil {
		return fail(err)
	}
	return &GatewayIssuerStore{lock: lock, installation: installation,
		epoch: epoch, lanes: lanes, secret: secret}, nil
}

func uniqueGatewayIssuerLane(lanes []requestledger.IssuerLane, index int) bool {
	for prior := 0; prior < index; prior++ {
		if lanes[prior] == lanes[index] {
			return false
		}
	}
	return true
}

func installGatewayIssuerStore(path string, raw []byte) error {
	temporary := path + ".installing"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		if err = os.Remove(temporary); err == nil {
			file, err = os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		}
	}
	if err != nil {
		return err
	}
	if err = writeGatewayIssuerFull(file, raw); err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(temporary, path); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(path))
}

func (store *GatewayIssuerStore) Contract() (GatewayIssuerContract, error) {
	if store == nil {
		return GatewayIssuerContract{}, ErrGatewayIssuerStore
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return GatewayIssuerContract{}, ErrGatewayIssuerStore
	}
	contract := GatewayIssuerContract{Installation: store.installation, Epoch: store.epoch,
		Lanes: append([]requestledger.IssuerLane(nil), store.lanes...)}
	contract.Digest = gatewayIssuerContractDigest(contract)
	return contract, nil
}

func (store *GatewayIssuerStore) GrantLane(scope requestledger.ScopeKind,
	tenant requestledger.Digest, principal requestledger.PrincipalID, lane uint16,
) (GatewayIssuerLaneGrant, error) {
	if store == nil {
		return GatewayIssuerLaneGrant{}, ErrGatewayIssuerStore
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed || int(lane) >= len(store.lanes) ||
		(scope != requestledger.ScopeAuthenticated && scope != requestledger.ScopeLocalInstall) ||
		tenant == (requestledger.Digest{}) || principal == (requestledger.PrincipalID{}) {
		return GatewayIssuerLaneGrant{}, ErrGatewayIssuerGrant
	}
	grant := GatewayIssuerLaneGrant{Installation: store.installation, Epoch: store.epoch,
		Lane: store.lanes[lane], Scope: scope, TenantDigest: tenant, Principal: principal}
	grant.Authenticator = gatewayIssuerGrantAuthenticator(store.secret, grant)
	return grant, nil
}

// RequestKey validates a grant and projects the client-supplied nonce and
// sequence into the canonical ledger key. It never allocates either value.
func (store *GatewayIssuerStore) RequestKey(grant GatewayIssuerLaneGrant,
	request requestledger.RequestID, sequence uint64,
) (requestledger.RequestKey, error) {
	if store == nil {
		return requestledger.RequestKey{}, ErrGatewayIssuerStore
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed || request == (requestledger.RequestID{}) || sequence == 0 ||
		grant.Installation != store.installation || grant.Epoch != store.epoch ||
		!gatewayIssuerStoreHasLane(store.lanes, grant.Lane) {
		return requestledger.RequestKey{}, ErrGatewayIssuerGrant
	}
	want := gatewayIssuerGrantAuthenticator(store.secret, grant)
	if subtle.ConstantTimeCompare(want[:], grant.Authenticator[:]) != 1 {
		return requestledger.RequestKey{}, ErrGatewayIssuerGrant
	}
	key := requestledger.RequestKey{Scope: grant.Scope, Principal: grant.Principal,
		Request: request, TenantDigest: grant.TenantDigest, IssuerEpoch: grant.Epoch,
		IssuerSequence: sequence, IssuerLane: grant.Lane}
	if !key.Valid() {
		return requestledger.RequestKey{}, ErrGatewayIssuerGrant
	}
	return key, nil
}

func gatewayIssuerStoreHasLane(lanes []requestledger.IssuerLane, lane requestledger.IssuerLane) bool {
	for index := range lanes {
		if lanes[index] == lane {
			return true
		}
	}
	return false
}

func (store *GatewayIssuerStore) Close() error {
	if store == nil {
		return ErrGatewayIssuerStore
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	return closeGatewayIssuerLock(store.lock)
}

func appendGatewayIssuerStoreRecord(dst []byte, installation replication.ID128, epoch uint64,
	lanes []requestledger.IssuerLane, secret replication.Digest,
) ([]byte, error) {
	if installation == (replication.ID128{}) || epoch == 0 || secret == (replication.Digest{}) ||
		len(lanes) == 0 || len(lanes) > MaxGatewayIssuerLanes {
		return dst, ErrGatewayIssuerStore
	}
	for index := range lanes {
		if lanes[index] == (requestledger.IssuerLane{}) || !uniqueGatewayIssuerLane(lanes, index) {
			return dst, ErrGatewayIssuerStore
		}
	}
	start := len(dst)
	dst = append(dst, make([]byte, gatewayIssuerStoreHeader+8*len(lanes))...)
	record := dst[start:]
	copy(record[:4], gatewayIssuerStoreMagic[:])
	record[4] = gatewayIssuerStoreFormat
	copy(record[8:24], installation[:])
	binary.LittleEndian.PutUint64(record[24:32], epoch)
	binary.LittleEndian.PutUint16(record[32:34], uint16(len(lanes)))
	copy(record[40:72], secret[:])
	for index := range lanes {
		copy(record[gatewayIssuerStoreHeader+8*index:], lanes[index][:])
	}
	digest := gatewayIssuerStoreRecordDigest(record)
	copy(record[72:104], digest[:])
	return dst, nil
}

func openGatewayIssuerStoreRecord(raw []byte) (replication.ID128, uint64,
	[]requestledger.IssuerLane, replication.Digest, error,
) {
	if len(raw) < gatewayIssuerStoreHeader || !equal4(raw[:4], gatewayIssuerStoreMagic) ||
		raw[4] != gatewayIssuerStoreFormat || raw[5] != 0 || raw[6] != 0 || raw[7] != 0 ||
		binary.LittleEndian.Uint16(raw[34:36]) != 0 || binary.LittleEndian.Uint32(raw[36:40]) != 0 {
		return replication.ID128{}, 0, nil, replication.Digest{}, ErrGatewayIssuerStore
	}
	count := int(binary.LittleEndian.Uint16(raw[32:34]))
	if count == 0 || count > MaxGatewayIssuerLanes || len(raw) != gatewayIssuerStoreHeader+8*count {
		return replication.ID128{}, 0, nil, replication.Digest{}, ErrGatewayIssuerStore
	}
	want := gatewayIssuerStoreRecordDigest(raw)
	if !equal32(raw[72:104], want) {
		return replication.ID128{}, 0, nil, replication.Digest{}, ErrGatewayIssuerStore
	}
	var installation replication.ID128
	copy(installation[:], raw[8:24])
	epoch := binary.LittleEndian.Uint64(raw[24:32])
	var secret replication.Digest
	copy(secret[:], raw[40:72])
	lanes := make([]requestledger.IssuerLane, count)
	for index := range lanes {
		copy(lanes[index][:], raw[gatewayIssuerStoreHeader+8*index:])
	}
	if installation == (replication.ID128{}) || epoch == 0 || secret == (replication.Digest{}) {
		return replication.ID128{}, 0, nil, replication.Digest{}, ErrGatewayIssuerStore
	}
	for index := range lanes {
		if lanes[index] == (requestledger.IssuerLane{}) || !uniqueGatewayIssuerLane(lanes, index) {
			return replication.ID128{}, 0, nil, replication.Digest{}, ErrGatewayIssuerStore
		}
	}
	return installation, epoch, lanes, secret, nil
}

func gatewayIssuerStoreRecordDigest(record []byte) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write(record[:72])
	_, _ = hash.Write(record[104:])
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func gatewayIssuerContractDigest(contract GatewayIssuerContract) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write(contract.Installation[:])
	var number [8]byte
	binary.LittleEndian.PutUint64(number[:], contract.Epoch)
	_, _ = hash.Write(number[:])
	for index := range contract.Lanes {
		_, _ = hash.Write(contract.Lanes[index][:])
	}
	var digest replication.Digest
	copy(digest[:], hash.Sum(nil))
	return digest
}

func gatewayIssuerGrantAuthenticator(secret replication.Digest, grant GatewayIssuerLaneGrant) replication.Digest {
	mac := hmac.New(sha256.New, secret[:])
	_, _ = mac.Write(gatewayIssuerGrantDomain)
	_, _ = mac.Write(grant.Installation[:])
	var framed [16]byte
	binary.LittleEndian.PutUint64(framed[:8], grant.Epoch)
	framed[8] = byte(grant.Scope)
	_, _ = mac.Write(framed[:])
	_, _ = mac.Write(grant.Lane[:])
	_, _ = mac.Write(grant.TenantDigest[:])
	_, _ = mac.Write(grant.Principal[:])
	var digest replication.Digest
	copy(digest[:], mac.Sum(nil))
	return digest
}

func writeGatewayIssuerFull(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		written, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}
