package splitcapture

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	MaxPortableSpecBytes = 32 << 10
	headerBytes          = 312
	checksumBytes        = sha256.Size
)

var (
	ErrCommand = errors.New("splitcapture: invalid activation command")
	magic      = [8]byte{'V', 'D', 'B', 'S', 'C', 'A', 0, 0}
	domain     = []byte("vibedb/split-capture/activation-checksum\x00")
)

// Command binds one portable split recipe to the exact source publication
// immediately preceding its Raft entry. Applied is deliberately absent: the
// state machine materializes it from the committed entry index.
type Command struct {
	Operation, PlanDigest, PartitionerDigest, RelationManifestDigest [32]byte
	LineageDigest                                                    [32]byte
	BindingDigest                                                    [32]byte
	PriorEntryDigest, PriorDataChainDigest                           [32]byte
	PriorApplied, PriorTerm, SourceGeneration, SchemaGeneration      uint64
	Spec                                                             []byte
}

type View struct{ Command }

func AppendCommand(dst []byte, c Command) ([]byte, error) {
	if err := validate(c); err != nil {
		return dst, err
	}
	total := headerBytes + len(c.Spec) + checksumBytes
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	f := dst[start:]
	copy(f[:8], magic[:])
	binary.LittleEndian.PutUint16(f[8:10], 1)
	binary.LittleEndian.PutUint16(f[10:12], headerBytes)
	binary.LittleEndian.PutUint32(f[12:16], uint32(total))
	binary.LittleEndian.PutUint32(f[16:20], uint32(len(c.Spec)))
	o := 20
	for _, v := range [][32]byte{c.Operation, c.PlanDigest, c.PartitionerDigest, c.RelationManifestDigest, c.LineageDigest, c.BindingDigest, c.PriorEntryDigest, c.PriorDataChainDigest} {
		copy(f[o:o+32], v[:])
		o += 32
	}
	binary.LittleEndian.PutUint64(f[o:o+8], c.PriorApplied)
	o += 8
	binary.LittleEndian.PutUint64(f[o:o+8], c.PriorTerm)
	o += 8
	binary.LittleEndian.PutUint64(f[o:o+8], c.SourceGeneration)
	o += 8
	binary.LittleEndian.PutUint64(f[o:o+8], c.SchemaGeneration)
	o += 8
	copy(f[headerBytes:headerBytes+len(c.Spec)], c.Spec)
	h := sha256.New()
	_, _ = h.Write(domain)
	_, _ = h.Write(f[:total-checksumBytes])
	_ = h.Sum(f[total-checksumBytes : total-checksumBytes])
	return dst, nil
}

func OpenCommand(raw []byte) (View, error) {
	if len(raw) < headerBytes+checksumBytes || len(raw) > headerBytes+MaxPortableSpecBytes+checksumBytes || !bytes.Equal(raw[:8], magic[:]) || binary.LittleEndian.Uint16(raw[8:10]) != 1 || binary.LittleEndian.Uint16(raw[10:12]) != headerBytes || int(binary.LittleEndian.Uint32(raw[12:16])) != len(raw) || int(binary.LittleEndian.Uint32(raw[16:20])) != len(raw)-headerBytes-checksumBytes {
		return View{}, ErrCommand
	}
	h := sha256.New()
	_, _ = h.Write(domain)
	_, _ = h.Write(raw[:len(raw)-checksumBytes])
	var sum [32]byte
	_ = h.Sum(sum[:0])
	if !bytes.Equal(sum[:], raw[len(raw)-checksumBytes:]) {
		return View{}, ErrCommand
	}
	var c Command
	o := 20
	vals := []*[32]byte{&c.Operation, &c.PlanDigest, &c.PartitionerDigest, &c.RelationManifestDigest, &c.LineageDigest, &c.BindingDigest, &c.PriorEntryDigest, &c.PriorDataChainDigest}
	for _, v := range vals {
		copy(v[:], raw[o:o+32])
		o += 32
	}
	c.PriorApplied = binary.LittleEndian.Uint64(raw[o : o+8])
	o += 8
	c.PriorTerm = binary.LittleEndian.Uint64(raw[o : o+8])
	o += 8
	c.SourceGeneration = binary.LittleEndian.Uint64(raw[o : o+8])
	o += 8
	c.SchemaGeneration = binary.LittleEndian.Uint64(raw[o : o+8])
	o += 8
	if raw[o] != 0 || raw[o+1] != 0 || raw[o+2] != 0 || raw[o+3] != 0 {
		return View{}, ErrCommand
	}
	c.Spec = raw[headerBytes : len(raw)-checksumBytes : len(raw)-checksumBytes]
	if err := validate(c); err != nil {
		return View{}, err
	}
	return View{c}, nil
}

func validate(c Command) error {
	for _, v := range [][32]byte{c.Operation, c.PlanDigest, c.PartitionerDigest, c.RelationManifestDigest, c.LineageDigest, c.BindingDigest, c.PriorEntryDigest, c.PriorDataChainDigest} {
		if v == ([32]byte{}) {
			return ErrCommand
		}
	}
	if c.PriorApplied == 0 || c.PriorTerm == 0 || c.SourceGeneration == 0 || c.SchemaGeneration == 0 || len(c.Spec) == 0 || len(c.Spec) > MaxPortableSpecBytes {
		return ErrCommand
	}
	return nil
}
