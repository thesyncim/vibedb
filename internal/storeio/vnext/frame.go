// Package vnext is an isolated layout laboratory for the next durable Store
// format. Nothing in the production read or write path imports it.
package vnext

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	Quantum          = 4 << 10
	MaxSpan          = 64 << 10
	FrameHeaderSize  = 64
	FrameTrailerSize = 8

	frameVersion = uint16(1)
	frameMagic   = "VJNXFR01"
)

type frameKind uint8

const (
	frameFingerprint frameKind = iota + 1
	frameRawBlock
	framePackedBlock
)

var (
	ErrCorrupt      = errors.New("vibejson: corrupt vNext frame")
	ErrInvalidFrame = errors.New("vibejson: invalid vNext frame")

	frameCRC = crc32.MakeTable(crc32.Castagnoli)
)

// Identity is the immutable identity shared by laboratory frames. StoreID
// prevents cross-file grafting; LogicalID remains stable across replacements.
type Identity struct {
	StoreID    [16]byte
	Generation uint64
	LogicalID  uint64
}

type frameHeader struct {
	identity      Identity
	kind          frameKind
	span          uint32
	payloadLength uint32
}

func initFrame(dst []byte, identity Identity, kind frameKind, payloadLength int) ([]byte, error) {
	if identity.StoreID == ([16]byte{}) || identity.Generation == 0 || identity.LogicalID == 0 ||
		kind < frameFingerprint || kind > framePackedBlock || !validSpan(len(dst)) ||
		payloadLength < 0 || payloadLength > len(dst)-FrameHeaderSize-FrameTrailerSize {
		return nil, ErrInvalidFrame
	}
	clear(dst)
	copy(dst[0:8], frameMagic)
	binary.LittleEndian.PutUint16(dst[8:10], frameVersion)
	binary.LittleEndian.PutUint16(dst[10:12], FrameHeaderSize)
	dst[12] = byte(kind)
	binary.LittleEndian.PutUint32(dst[16:20], uint32(len(dst)))
	binary.LittleEndian.PutUint32(dst[20:24], uint32(payloadLength))
	binary.LittleEndian.PutUint64(dst[24:32], identity.Generation)
	binary.LittleEndian.PutUint64(dst[32:40], identity.LogicalID)
	copy(dst[40:56], identity.StoreID[:])
	end := FrameHeaderSize + payloadLength
	return dst[FrameHeaderSize:end:end], nil
}

func sealFrame(frame []byte) error {
	header, ok := decodeFrameHeader(frame)
	if !ok {
		return ErrInvalidFrame
	}
	payloadEnd := FrameHeaderSize + int(header.payloadLength)
	trailer := int(header.span) - FrameTrailerSize
	if !allZero(frame[13:16]) || !allZero(frame[56:64]) ||
		!allZero(frame[payloadEnd:trailer]) {
		return ErrInvalidFrame
	}
	checksum := crc32.Checksum(frame[:trailer], frameCRC)
	binary.LittleEndian.PutUint32(frame[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(frame[trailer+4:trailer+8], ^checksum)
	return nil
}

func openFrame(src []byte, kind frameKind) (frameHeader, []byte, error) {
	header, ok := decodeFrameHeader(src)
	if !ok || header.kind != kind {
		return frameHeader{}, nil, ErrCorrupt
	}
	frame := src[:int(header.span)]
	trailer := len(frame) - FrameTrailerSize
	checksum := binary.LittleEndian.Uint32(frame[trailer : trailer+4])
	if binary.LittleEndian.Uint32(frame[trailer+4:]) != ^checksum ||
		crc32.Checksum(frame[:trailer], frameCRC) != checksum ||
		!allZero(frame[13:16]) || !allZero(frame[56:64]) ||
		!allZero(frame[FrameHeaderSize+int(header.payloadLength):trailer]) {
		return frameHeader{}, nil, ErrCorrupt
	}
	end := FrameHeaderSize + int(header.payloadLength)
	return header, frame[FrameHeaderSize:end:end], nil
}

func decodeFrameHeader(src []byte) (frameHeader, bool) {
	if len(src) < FrameHeaderSize || string(src[0:8]) != frameMagic ||
		binary.LittleEndian.Uint16(src[8:10]) != frameVersion ||
		binary.LittleEndian.Uint16(src[10:12]) != FrameHeaderSize {
		return frameHeader{}, false
	}
	span := binary.LittleEndian.Uint32(src[16:20])
	payloadLength := binary.LittleEndian.Uint32(src[20:24])
	if uint64(span) > uint64(len(src)) || !validSpan(int(span)) ||
		uint64(payloadLength) > uint64(span)-FrameHeaderSize-FrameTrailerSize {
		return frameHeader{}, false
	}
	header := frameHeader{
		kind:          frameKind(src[12]),
		span:          span,
		payloadLength: payloadLength,
		identity: Identity{
			Generation: binary.LittleEndian.Uint64(src[24:32]),
			LogicalID:  binary.LittleEndian.Uint64(src[32:40]),
		},
	}
	copy(header.identity.StoreID[:], src[40:56])
	if header.identity.StoreID == ([16]byte{}) || header.identity.Generation == 0 ||
		header.identity.LogicalID == 0 || header.kind < frameFingerprint ||
		header.kind > framePackedBlock {
		return frameHeader{}, false
	}
	return header, true
}

func validSpan(span int) bool {
	return span >= Quantum && span <= MaxSpan && span%Quantum == 0
}

func allZero(src []byte) bool {
	var combined byte
	for _, value := range src {
		combined |= value
	}
	return combined == 0
}

func corrupt(what string) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, what)
}
