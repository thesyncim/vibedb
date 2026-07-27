//go:build !linux

package storeio

// Err reports a negative completion result. Non-Linux builds cannot receive a
// real ring completion; the method exists so the internal selector compiles.
func (c Completion) Err() error {
	if c.Result >= 0 {
		return nil
	}
	return ErrUnavailable
}
