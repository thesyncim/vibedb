package shardcontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	ErrJournalBound    = errors.New("shardcontrol: durable replay journal bound exceeded")
	ErrJournalConflict = errors.New("shardcontrol: durable replay identity conflicts")
	ErrJournalClosed   = errors.New("shardcontrol: durable replay journal is closed")
)

const journalHeaderBytes = 4 + 1 + 3 + 4 + 32 + 32 + 32

var (
	journalMagic = [4]byte{'V', 'S', 'C', '1'}
	journalCRC   = crc32.MakeTable(crc32.Castagnoli)
)

type journalKind uint8

const (
	journalIntent journalKind = iota + 1
	journalResult
)

type JournalLimits struct {
	MaxRecords   int
	MaxFileBytes int64
}

func (limits JournalLimits) valid() bool {
	return limits.MaxRecords > 0 && limits.MaxRecords <= 1<<20 &&
		limits.MaxFileBytes >= int64(journalHeaderBytes+frameHeaderBytes+requestFixedBodyBytes) &&
		limits.MaxFileBytes <= 1<<40
}

type stepKey struct {
	operation [32]byte
	step      [32]byte
}

type replayRecord struct {
	requestDigest [32]byte
	response      []byte
}

// ActionExecutor performs one action whose exact (operation, step) replay is
// idempotent. JournalExecutor persists intent before calling it and persists
// the canonical response before returning Accepted.
type ActionExecutor interface {
	ExecuteAction(context.Context, rafttransport.PeerIdentity, Request) (Response, error)
}

type JournalExecutor struct {
	mu      sync.Mutex
	file    *os.File
	limits  JournalLimits
	fileEnd int64
	records map[stepKey]*replayRecord
	actions ActionExecutor
	sticky  error
	closed  bool
}

func OpenJournalExecutor(
	path string, limits JournalLimits, actions ActionExecutor,
) (*JournalExecutor, error) {
	if path == "" || !limits.valid() || actions == nil {
		return nil, ErrJournalBound
	}
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if created {
		directory, openErr := os.Open(filepath.Dir(path))
		if openErr != nil {
			_ = file.Close()
			return nil, openErr
		}
		err = errors.Join(directory.Sync(), directory.Close())
		if err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	executor := &JournalExecutor{
		file: file, limits: limits, records: make(map[stepKey]*replayRecord), actions: actions,
	}
	if err = executor.recover(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return executor, nil
}

func (executor *JournalExecutor) recover() error {
	info, err := executor.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > executor.limits.MaxFileBytes {
		return ErrJournalBound
	}
	var offset int64
	var header [journalHeaderBytes]byte
	for offset < info.Size() {
		remaining := info.Size() - offset
		if remaining < journalHeaderBytes+4 {
			return executor.truncateTail(offset)
		}
		if _, err = executor.file.ReadAt(header[:], offset); err != nil {
			return err
		}
		payloadBytes := int64(binary.LittleEndian.Uint32(header[8:12]))
		entryBytes := int64(journalHeaderBytes) + payloadBytes + 4
		if payloadBytes <= 0 || payloadBytes > int64(frameHeaderBytes+maxFrameBodyBytes) || entryBytes > remaining {
			if entryBytes > remaining {
				return executor.truncateTail(offset)
			}
			return ErrWire
		}
		entry := make([]byte, entryBytes)
		if _, err = executor.file.ReadAt(entry, offset); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(entry[len(entry)-4:]) != crc32.Checksum(entry[:len(entry)-4], journalCRC) {
			if entryBytes == remaining {
				return executor.truncateTail(offset)
			}
			return ErrWire
		}
		if err = executor.replay(entry); err != nil {
			return err
		}
		offset += entryBytes
	}
	executor.fileEnd = offset
	_, err = executor.file.Seek(offset, io.SeekStart)
	return err
}

func (executor *JournalExecutor) replay(entry []byte) error {
	if !bytes.Equal(entry[:4], journalMagic[:]) || entry[5] != 0 || entry[6] != 0 || entry[7] != 0 {
		return ErrWire
	}
	key := stepKey{}
	copy(key.operation[:], entry[12:44])
	copy(key.step[:], entry[44:76])
	var digest [32]byte
	copy(digest[:], entry[76:108])
	payload := entry[journalHeaderBytes : len(entry)-4]
	record := executor.records[key]
	switch journalKind(entry[4]) {
	case journalIntent:
		request, err := OpenRequest(payload)
		if err != nil || request.Operation != key.operation || request.Step != key.step ||
			sha256.Sum256(payload) != digest || record != nil {
			return ErrJournalConflict
		}
		if len(executor.records) >= executor.limits.MaxRecords {
			return ErrJournalBound
		}
		executor.records[key] = &replayRecord{requestDigest: digest}
	case journalResult:
		response, err := OpenResponse(payload)
		if err != nil || response.Operation != key.operation || response.Step != key.step ||
			sha256.Sum256(payload) != digest || record == nil || record.response != nil {
			return ErrJournalConflict
		}
		record.response = bytes.Clone(payload)
	default:
		return ErrWire
	}
	return nil
}

func (executor *JournalExecutor) truncateTail(offset int64) error {
	if err := executor.file.Truncate(offset); err != nil {
		return err
	}
	if err := executor.file.Sync(); err != nil {
		return err
	}
	executor.fileEnd = offset
	_, err := executor.file.Seek(offset, io.SeekStart)
	return err
}

func (executor *JournalExecutor) ExecuteControl(
	ctx context.Context, peer rafttransport.PeerIdentity, request Request,
) (Response, error) {
	if executor == nil || ctx == nil || !validRequest(&request) {
		return Response{}, ErrWire
	}
	requestFrame, err := AppendRequest(nil, &request)
	if err != nil {
		return Response{}, err
	}
	requestDigest := sha256.Sum256(requestFrame)
	key := stepKey{operation: request.Operation, step: request.Step}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.closed {
		return Response{}, ErrJournalClosed
	}
	if executor.sticky != nil {
		return Response{}, executor.sticky
	}
	record := executor.records[key]
	if record != nil {
		if record.requestDigest != requestDigest {
			return Response{}, ErrJournalConflict
		}
		if record.response != nil {
			return OpenResponse(record.response)
		}
	} else {
		if len(executor.records) >= executor.limits.MaxRecords {
			return Response{}, ErrJournalBound
		}
		if err = executor.appendEntry(journalIntent, key, requestDigest, requestFrame); err != nil {
			return Response{}, err
		}
		record = &replayRecord{requestDigest: requestDigest}
		executor.records[key] = record
	}
	response, err := executor.actions.ExecuteAction(ctx, peer, request)
	if err != nil {
		return Response{}, err
	}
	if response.Operation != request.Operation || response.Step != request.Step || !validResponse(&response) {
		return Response{}, ErrWire
	}
	responseFrame, err := AppendResponse(nil, &response)
	if err != nil {
		return Response{}, err
	}
	responseDigest := sha256.Sum256(responseFrame)
	if err = executor.appendEntry(journalResult, key, responseDigest, responseFrame); err != nil {
		return Response{}, err
	}
	record.response = bytes.Clone(responseFrame)
	return response, nil
}

func (executor *JournalExecutor) appendEntry(
	kind journalKind, key stepKey, digest [32]byte, payload []byte,
) error {
	entryBytes := journalHeaderBytes + len(payload) + 4
	if executor.fileEnd+int64(entryBytes) > executor.limits.MaxFileBytes {
		return ErrJournalBound
	}
	entry := make([]byte, entryBytes)
	copy(entry[:4], journalMagic[:])
	entry[4] = byte(kind)
	binary.LittleEndian.PutUint32(entry[8:12], uint32(len(payload)))
	copy(entry[12:44], key.operation[:])
	copy(entry[44:76], key.step[:])
	copy(entry[76:108], digest[:])
	copy(entry[journalHeaderBytes:], payload)
	binary.LittleEndian.PutUint32(entry[len(entry)-4:], crc32.Checksum(entry[:len(entry)-4], journalCRC))
	written, err := executor.file.Write(entry)
	if err != nil || written != len(entry) {
		executor.sticky = errors.Join(ErrOutcomeUnknown, err)
		return executor.sticky
	}
	if err = executor.file.Sync(); err != nil {
		executor.sticky = errors.Join(ErrOutcomeUnknown, err)
		return executor.sticky
	}
	executor.fileEnd += int64(entryBytes)
	return nil
}

func (executor *JournalExecutor) Close() error {
	if executor == nil {
		return nil
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.closed {
		return ErrJournalClosed
	}
	executor.closed = true
	return executor.file.Close()
}
