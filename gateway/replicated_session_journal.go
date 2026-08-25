package gateway

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/replication"
)

var ErrNativeSessionJournal = errors.New("gateway: invalid durable native session journal")

const (
	nativeSessionJournalFormat = 1
	nativeSessionJournalHeader = 180
)

var nativeSessionJournalMagic = [4]byte{'V', 'N', 'S', '1'}

type durableNativeSessionState struct {
	clientID            replication.ID128
	retryHome           replication.RetryHome
	phase               nativeSessionPhase
	epoch               uint64
	nextSequence        uint64
	ackThrough          uint64
	leaseDeadline       int64
	terminalSequence    uint64
	terminalFingerprint replication.Digest
	pending             bool
	command             []byte
}

type NativeSessionJournalOptions struct {
	Path            string
	ClientID        replication.ID128
	RetryHome       replication.RetryHome
	MaxCommandBytes int
	Binding         replication.Digest
}

// NativeSessionJournal alternates two compact complete records. The inactive
// file is truncated and rewritten, synced, then becomes the higher generation;
// a torn write leaves the previous file byte-for-byte valid. Small controller
// commands therefore consume small files rather than a MaxCommandBytes slot.
type NativeSessionJournal struct {
	mu         sync.Mutex
	base       string
	maxCommand int
	generation uint64
	active     int
	state      durableNativeSessionState
	binding    replication.Digest
}

func OpenNativeSessionJournal(options NativeSessionJournalOptions) (*NativeSessionJournal, error) {
	if options.Path == "" || options.ClientID == (replication.ID128{}) ||
		options.Binding == (replication.Digest{}) ||
		options.MaxCommandBytes <= 0 || options.MaxCommandBytes > replication.MaxCommandBytes {
		return nil, ErrNativeSessionJournal
	}
	journal := &NativeSessionJournal{
		base: options.Path, maxCommand: options.MaxCommandBytes,
		binding: options.Binding, active: -1,
	}
	var found bool
	for slot := 0; slot < 2; slot++ {
		raw, err := os.ReadFile(journal.slotPath(slot))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		generation, binding, state, openErr := openNativeSessionJournalRecord(raw, options.MaxCommandBytes)
		if openErr != nil {
			continue
		}
		if binding != options.Binding || state.clientID != options.ClientID || state.retryHome != options.RetryHome {
			return nil, ErrNativeSessionJournal
		}
		if found && generation == journal.generation {
			return nil, ErrNativeSessionJournal
		}
		if !found || generation > journal.generation {
			journal.generation, journal.state, journal.active, found = generation, state, slot, true
		}
	}
	if found {
		return journal, nil
	}
	// Refuse silently replacing corrupt existing authority.
	for slot := 0; slot < 2; slot++ {
		if _, err := os.Stat(journal.slotPath(slot)); err == nil {
			return nil, ErrNativeSessionJournal
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	journal.state = durableNativeSessionState{
		clientID: options.ClientID, retryHome: options.RetryHome,
		phase: nativeSessionNew, nextSequence: 1,
	}
	if err := journal.storeLocked(journal.state); err != nil {
		return nil, err
	}
	return journal, nil
}

func (journal *NativeSessionJournal) slotPath(slot int) string {
	if slot == 0 {
		return journal.base + ".0"
	}
	return journal.base + ".1"
}

func (journal *NativeSessionJournal) load() (durableNativeSessionState, error) {
	if journal == nil {
		return durableNativeSessionState{}, ErrNativeSessionJournal
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	state := journal.state
	state.command = append([]byte(nil), state.command...)
	return state, nil
}

func (journal *NativeSessionJournal) store(state durableNativeSessionState) error {
	if journal == nil {
		return ErrNativeSessionJournal
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return journal.storeLocked(state)
}

func (journal *NativeSessionJournal) storeLocked(state durableNativeSessionState) error {
	if journal.generation == ^uint64(0) || state.clientID != journal.state.clientID && journal.generation != 0 ||
		state.retryHome != journal.state.retryHome && journal.generation != 0 {
		return ErrNativeSessionJournal
	}
	next := journal.generation + 1
	raw, err := appendNativeSessionJournalRecord(nil, next, journal.binding, state, journal.maxCommand)
	if err != nil {
		return err
	}
	slot := 0
	if journal.active == 0 {
		slot = 1
	}
	path := journal.slotPath(slot)
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return statErr
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if created {
		directory, openErr := os.Open(filepath.Dir(journal.base))
		if openErr != nil {
			return errors.Join(openErr, file.Close())
		}
		if syncErr := errors.Join(directory.Sync(), directory.Close()); syncErr != nil {
			return errors.Join(syncErr, file.Close())
		}
	}
	if err = writeNativeSessionJournalFull(file, raw); err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	state.command = append(state.command[:0:0], state.command...)
	journal.generation, journal.active, journal.state = next, slot, state
	return nil
}

func appendNativeSessionJournalRecord(
	dst []byte, generation uint64, binding replication.Digest,
	state durableNativeSessionState, maxCommand int,
) ([]byte, error) {
	if generation == 0 || binding == (replication.Digest{}) || !validDurableNativeSessionState(state, maxCommand) {
		return dst, ErrNativeSessionJournal
	}
	start := len(dst)
	dst = append(dst, make([]byte, nativeSessionJournalHeader+len(state.command))...)
	record := dst[start:]
	copy(record[:4], nativeSessionJournalMagic[:])
	record[4], record[5] = nativeSessionJournalFormat, byte(state.phase)
	if state.pending {
		record[6] = 1
	}
	binary.LittleEndian.PutUint64(record[8:16], generation)
	copy(record[16:32], state.clientID[:])
	copy(record[32:40], state.retryHome[:])
	binary.LittleEndian.PutUint64(record[40:48], state.epoch)
	binary.LittleEndian.PutUint64(record[48:56], state.nextSequence)
	binary.LittleEndian.PutUint64(record[56:64], state.ackThrough)
	binary.LittleEndian.PutUint64(record[64:72], uint64(state.leaseDeadline))
	binary.LittleEndian.PutUint64(record[72:80], state.terminalSequence)
	copy(record[80:112], state.terminalFingerprint[:])
	binary.LittleEndian.PutUint32(record[112:116], uint32(len(state.command)))
	copy(record[116:148], binding[:])
	copy(record[nativeSessionJournalHeader:], state.command)
	digest := nativeSessionJournalDigest(record)
	copy(record[148:180], digest[:])
	return dst, nil
}

func openNativeSessionJournalRecord(
	raw []byte, maxCommand int,
) (uint64, replication.Digest, durableNativeSessionState, error) {
	if len(raw) < nativeSessionJournalHeader || len(raw) > nativeSessionJournalHeader+maxCommand ||
		!equal4(raw[:4], nativeSessionJournalMagic) || raw[4] != nativeSessionJournalFormat ||
		raw[7] != 0 {
		return 0, replication.Digest{}, durableNativeSessionState{}, ErrNativeSessionJournal
	}
	commandBytes := int(binary.LittleEndian.Uint32(raw[112:116]))
	if commandBytes > maxCommand || len(raw) != nativeSessionJournalHeader+commandBytes {
		return 0, replication.Digest{}, durableNativeSessionState{}, ErrNativeSessionJournal
	}
	want := nativeSessionJournalDigest(raw)
	if !equal32(raw[148:180], want) {
		return 0, replication.Digest{}, durableNativeSessionState{}, ErrNativeSessionJournal
	}
	state := durableNativeSessionState{
		phase: nativeSessionPhase(raw[5]), pending: raw[6] == 1,
		epoch:            binary.LittleEndian.Uint64(raw[40:48]),
		nextSequence:     binary.LittleEndian.Uint64(raw[48:56]),
		ackThrough:       binary.LittleEndian.Uint64(raw[56:64]),
		leaseDeadline:    int64(binary.LittleEndian.Uint64(raw[64:72])),
		terminalSequence: binary.LittleEndian.Uint64(raw[72:80]),
	}
	if raw[6] > 1 {
		return 0, replication.Digest{}, durableNativeSessionState{}, ErrNativeSessionJournal
	}
	copy(state.clientID[:], raw[16:32])
	copy(state.retryHome[:], raw[32:40])
	copy(state.terminalFingerprint[:], raw[80:112])
	var binding replication.Digest
	copy(binding[:], raw[116:148])
	state.command = append([]byte(nil), raw[nativeSessionJournalHeader:]...)
	generation := binary.LittleEndian.Uint64(raw[8:16])
	if generation == 0 || !validDurableNativeSessionState(state, maxCommand) {
		return 0, replication.Digest{}, durableNativeSessionState{}, ErrNativeSessionJournal
	}
	return generation, binding, state, nil
}

func validDurableNativeSessionState(state durableNativeSessionState, maxCommand int) bool {
	if state.clientID == (replication.ID128{}) || state.phase < nativeSessionNew ||
		state.phase > nativeSessionReleased || len(state.command) > maxCommand ||
		state.pending != (len(state.command) != 0) {
		return false
	}
	if state.phase == nativeSessionNew {
		if state.epoch != 0 || state.nextSequence != 1 || state.ackThrough != 0 ||
			state.leaseDeadline != 0 || state.terminalSequence != 0 ||
			state.terminalFingerprint != (replication.Digest{}) {
			return false
		}
	} else if state.epoch == 0 || state.ackThrough == 0 ||
		state.nextSequence != 0 && state.nextSequence != state.ackThrough+1 {
		return false
	}
	switch state.phase {
	case nativeSessionActive:
		if state.leaseDeadline <= 0 || state.terminalSequence != 0 ||
			state.terminalFingerprint != (replication.Digest{}) {
			return false
		}
	case nativeSessionRetired, nativeSessionReleased:
		if state.terminalSequence == 0 || state.terminalSequence != state.ackThrough ||
			state.terminalFingerprint == (replication.Digest{}) ||
			state.phase == nativeSessionReleased && state.pending {
			return false
		}
	}
	if state.pending {
		command, err := replication.OpenCommand(state.command)
		if err != nil || command.ClientID != state.clientID || command.RetryHome != state.retryHome {
			return false
		}
		switch command.Kind() {
		case replication.CommandSessionOpen:
			return state.phase == nativeSessionNew && command.ClientEpoch == 0 && command.ClientSequence == 1
		case replication.CommandSessionRelease:
			return state.phase == nativeSessionRetired && command.ClientEpoch == state.epoch &&
				command.ClientSequence == state.terminalSequence && command.Fingerprint == state.terminalFingerprint
		default:
			return state.phase == nativeSessionActive && command.ClientEpoch == state.epoch &&
				command.ClientSequence == state.nextSequence && command.AckThrough == state.ackThrough
		}
	}
	return true
}

func nativeSessionJournalDigest(record []byte) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write(record[:148])
	_, _ = hash.Write(record[180:])
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func writeNativeSessionJournalFull(writer io.Writer, raw []byte) error {
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

func equal4(raw []byte, value [4]byte) bool {
	return len(raw) == 4 && raw[0] == value[0] && raw[1] == value[1] && raw[2] == value[2] && raw[3] == value[3]
}

func equal32(raw []byte, value [32]byte) bool {
	if len(raw) != len(value) {
		return false
	}
	var diff byte
	for index := range value {
		diff |= raw[index] ^ value[index]
	}
	return diff == 0
}
