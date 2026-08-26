package storeio

import (
	"encoding/binary"
	"fmt"
)

const publicationDescriptorHeader = 16

type PublicationMutation struct {
	Delete     bool
	Key, Value []byte
}
type PublicationDescriptorView struct {
	image                  []byte
	count, ordinal, cursor int
}

func EncodePublicationDescriptor(dst []byte, mutations []PublicationMutation) ([]byte, error) {
	used := publicationDescriptorHeader + 8
	for _, m := range mutations {
		used += 12 + len(m.Key) + len(m.Value)
	}
	length := (used + 4095) &^ 4095
	if len(mutations) == 0 || length > len(dst) {
		return nil, fmt.Errorf("%w: publication descriptor bounds", ErrInvalidWrite)
	}
	b := dst[:length]
	clear(b)
	copy(b[:8], "SPUBDS00")
	binary.LittleEndian.PutUint32(b[8:12], DevelopmentFormatVersion)
	binary.LittleEndian.PutUint32(b[12:16], uint32(len(mutations)))
	at := publicationDescriptorHeader
	for _, m := range mutations {
		if len(m.Key) == 0 || m.Delete && len(m.Value) != 0 {
			return nil, fmt.Errorf("%w: publication mutation", ErrInvalidWrite)
		}
		if m.Delete {
			b[at] = 1
		}
		binary.LittleEndian.PutUint32(b[at+4:at+8], uint32(len(m.Key)))
		binary.LittleEndian.PutUint32(b[at+8:at+12], uint32(len(m.Value)))
		at += 12
		copy(b[at:], m.Key)
		at += len(m.Key)
		copy(b[at:], m.Value)
		at += len(m.Value)
	}
	sumAt := len(b) - 8
	sum := PageChecksum(b[:sumAt])
	binary.LittleEndian.PutUint32(b[sumAt:], sum)
	binary.LittleEndian.PutUint32(b[sumAt+4:], ^sum)
	return b, nil
}

func OpenPublicationDescriptor(src []byte) (PublicationDescriptorView, error) {
	if len(src) < 4096 || len(src)&4095 != 0 || string(src[:8]) != "SPUBDS00" || binary.LittleEndian.Uint32(src[8:12]) != DevelopmentFormatVersion {
		return PublicationDescriptorView{}, ErrGenerationMigrationManifestCorrupt
	}
	sumAt := len(src) - 8
	sum := binary.LittleEndian.Uint32(src[sumAt:])
	if binary.LittleEndian.Uint32(src[sumAt+4:]) != ^sum || PageChecksum(src[:sumAt]) != sum {
		return PublicationDescriptorView{}, ErrGenerationMigrationManifestCorrupt
	}
	v := PublicationDescriptorView{image: src, count: int(binary.LittleEndian.Uint32(src[12:16])), cursor: publicationDescriptorHeader}
	if v.count == 0 {
		return PublicationDescriptorView{}, ErrGenerationMigrationManifestCorrupt
	}
	probe := v
	for {
		_, ok, err := probe.Next()
		if err != nil {
			return PublicationDescriptorView{}, err
		}
		if !ok {
			break
		}
	}
	if !allZero(src[probe.cursor:sumAt]) {
		return PublicationDescriptorView{}, ErrGenerationMigrationManifestCorrupt
	}
	return v, nil
}

func (v *PublicationDescriptorView) Next() (PublicationMutation, bool, error) {
	if v == nil || v.ordinal == v.count {
		return PublicationMutation{}, false, nil
	}
	if v.ordinal > v.count || v.cursor+12 > len(v.image)-8 {
		return PublicationMutation{}, false, ErrGenerationMigrationManifestCorrupt
	}
	at := v.cursor
	flag := v.image[at]
	k := int(binary.LittleEndian.Uint32(v.image[at+4 : at+8]))
	n := int(binary.LittleEndian.Uint32(v.image[at+8 : at+12]))
	end := at + 12 + k + n
	if flag > 1 || k == 0 || flag == 1 && n != 0 || end > len(v.image)-8 {
		return PublicationMutation{}, false, ErrGenerationMigrationManifestCorrupt
	}
	m := PublicationMutation{Delete: flag == 1, Key: v.image[at+12 : at+12+k], Value: v.image[at+12+k : end]}
	v.cursor = end
	v.ordinal++
	return m, true, nil
}
