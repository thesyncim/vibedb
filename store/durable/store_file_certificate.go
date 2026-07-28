package durable

import (
	"encoding/binary"

	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	fileIndexTupleCertificateVersion = 1
	fileIndexTupleCertificateHeader  = 4
	fileIndexTupleCertificateLength  = 2
)

// appendFileIndexCertificate appends one exact tuple representative. A
// single-column certificate is the scalar's JSON itself. Compound tuples use
// a private length-prefixed envelope; values remain exact JSON scalars so
// validation and semantic equality share the ordinary scalar comparator.
func appendFileIndexCertificate(dst []byte, values []vibejson.RawValue, maxBytes int) ([]byte, bool) {
	if len(values) == 0 || len(values) > store.MaxIndexColumns {
		return dst, false
	}
	total := 0
	if len(values) == 1 {
		if !fileIndexCertificateScalar(values[0]) {
			return dst, false
		}
		total = len(values[0].Bytes())
	} else {
		total = fileIndexTupleCertificateHeader
		for _, value := range values {
			if !fileIndexCertificateScalar(value) ||
				len(value.Bytes()) > int(^uint16(0)) {
				return dst, false
			}
			total += fileIndexTupleCertificateLength + len(value.Bytes())
		}
	}
	if total == 0 || total > maxBytes ||
		len(dst) > int(^uint(0)>>1)-total {
		return dst, false
	}
	if len(values) == 1 {
		return append(dst, values[0].Bytes()...), true
	}
	start := len(dst)
	dst = append(dst, fileIndexTupleCertificateVersion, byte(len(values)), 0, 0)
	for _, value := range values {
		lengthAt := len(dst)
		dst = append(dst, 0, 0)
		binary.LittleEndian.PutUint16(dst[lengthAt:lengthAt+2], uint16(len(value.Bytes())))
		dst = append(dst, value.Bytes()...)
	}
	return dst[:len(dst):len(dst)], len(dst)-start == total
}

func fileIndexCertificateValid(certificate []byte, columns int) bool {
	cursor := newFileIndexCertificateCursor(certificate, columns)
	for {
		raw, ok := cursor.next()
		if !ok {
			break
		}
		if !vibejson.Valid(raw.Bytes()) || !fileIndexCertificateScalar(raw) {
			return false
		}
	}
	return cursor.done()
}

func fileIndexCertificatesEqual(left, right []byte, columns int) bool {
	leftCursor := newFileIndexCertificateCursor(left, columns)
	rightCursor := newFileIndexCertificateCursor(right, columns)
	for {
		leftRaw, leftOK := leftCursor.next()
		rightRaw, rightOK := rightCursor.next()
		if leftOK != rightOK {
			return false
		}
		if !leftOK {
			return leftCursor.done() && rightCursor.done()
		}
		if !fileIndexRawValuesEqual(leftRaw, rightRaw) {
			return false
		}
	}
}

func fileIndexCertificateMatches(certificate []byte, values []vibejson.Index, columns int) bool {
	if len(values) != columns {
		return false
	}
	cursor := newFileIndexCertificateCursor(certificate, columns)
	for position := range len(values) {
		raw, ok := cursor.next()
		if !ok || !fileIndexRawScalarEqual(raw, values[position].Root()) {
			return false
		}
	}
	return cursor.done()
}

type fileIndexCertificateCursor struct {
	src       []byte
	position  int
	remaining int
	single    bool
	valid     bool
}

func newFileIndexCertificateCursor(certificate []byte, columns int) fileIndexCertificateCursor {
	if columns < 1 || columns > store.MaxIndexColumns || len(certificate) == 0 {
		return fileIndexCertificateCursor{}
	}
	if columns == 1 {
		return fileIndexCertificateCursor{
			src: certificate, remaining: 1, single: true, valid: true,
		}
	}
	if len(certificate) < fileIndexTupleCertificateHeader ||
		certificate[0] != fileIndexTupleCertificateVersion ||
		int(certificate[1]) != columns ||
		certificate[2] != 0 || certificate[3] != 0 {
		return fileIndexCertificateCursor{}
	}
	return fileIndexCertificateCursor{
		src: certificate, position: fileIndexTupleCertificateHeader,
		remaining: columns, valid: true,
	}
}

func (c *fileIndexCertificateCursor) next() (vibejson.RawValue, bool) {
	if c == nil || !c.valid || c.remaining == 0 {
		return vibejson.RawValue{}, false
	}
	if c.single {
		c.position = len(c.src)
		c.remaining = 0
		return vibejson.RawValue{Src: c.src}, true
	}
	if c.position+fileIndexTupleCertificateLength > len(c.src) {
		c.valid = false
		return vibejson.RawValue{}, false
	}
	length := int(binary.LittleEndian.Uint16(
		c.src[c.position : c.position+fileIndexTupleCertificateLength],
	))
	c.position += fileIndexTupleCertificateLength
	end := c.position + length
	if length == 0 || end > len(c.src) {
		c.valid = false
		return vibejson.RawValue{}, false
	}
	raw := vibejson.RawValue{Src: c.src[c.position:end:end]}
	c.position = end
	c.remaining--
	return raw, true
}

func (c fileIndexCertificateCursor) done() bool {
	return c.valid && c.remaining == 0 && c.position == len(c.src)
}
