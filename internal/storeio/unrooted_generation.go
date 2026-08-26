package storeio

import (
	"fmt"
	"os"
)

// UnrootedGenerationWriter durably fills one previously published reservation
// in ascending physical order. Sequentiality makes the manifest cursor a
// complete bounded crash-resume witness; no per-page bitmap is retained.
type UnrootedGenerationWriter struct {
	file        *os.File
	reservation UnrootedGenerationReservation
	storeID     [16]byte
	generation  uint64
	written     uint64
}

func NewUnrootedGenerationWriter(file *os.File, reservation UnrootedGenerationReservation, storeID [16]byte, generation, resumeBytes uint64) (*UnrootedGenerationWriter, error) {
	if file == nil || reservation.Length == 0 || storeID == ([16]byte{}) || generation == 0 || resumeBytes > reservation.Length || resumeBytes&4095 != 0 {
		return nil, fmt.Errorf("%w: unrooted writer", ErrInvalidWrite)
	}
	return &UnrootedGenerationWriter{file: file, reservation: reservation, storeID: storeID, generation: generation, written: resumeBytes}, nil
}

func (w *UnrootedGenerationWriter) Append(ref PageRef, image []byte) error {
	if w == nil || ref.Offset != w.reservation.Offset+w.written || ref.Generation != w.generation || ref.Length == 0 || uint64(ref.Length) > w.reservation.Length-w.written || len(image) != int(ref.Length) {
		return fmt.Errorf("%w: unrooted append bounds", ErrInvalidWrite)
	}
	header, _, err := OpenPage(image)
	if err != nil || header.StoreID != w.storeID || header.Generation != ref.Generation || header.LogicalID != ref.LogicalID || header.Kind != ref.Kind || header.PageSize != ref.Length {
		return errorsJoinInvalid(err, "unrooted append page")
	}
	for at := 0; at < len(image); {
		n, writeErr := w.file.WriteAt(image[at:], int64(ref.Offset)+int64(at))
		if writeErr != nil {
			return writeErr
		}
		if n == 0 {
			return fmt.Errorf("%w: zero-length unrooted write", ErrInvalidWrite)
		}
		at += n
	}
	w.written += uint64(ref.Length)
	return nil
}

func (w *UnrootedGenerationWriter) Sync() error {
	if w == nil {
		return ErrInvalidWrite
	}
	return w.file.Sync()
}
func (w *UnrootedGenerationWriter) WrittenBytes() uint64 {
	if w == nil {
		return 0
	}
	return w.written
}

func errorsJoinInvalid(err error, context string) error {
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrInvalidWrite, context, err)
	}
	return fmt.Errorf("%w: %s", ErrInvalidWrite, context)
}
