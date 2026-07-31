//go:build !linux || (!amd64 && !arm64 && !riscv64 && !loong64)

package storeio

// Ring is unavailable on this platform. It retains the Linux method surface so
// the portable Store I/O selector compiles without platform conditionals.
type Ring struct {
	bufferRegistrationBroken bool
}

func Open(Config) (*Ring, error) { return nil, ErrUnavailable }

func (*Ring) Features() Features { return Features{} }

func (*Ring) vectorWriteCapacity() int { return 0 }

func (*Ring) RegisterFiles([]int) error { return ErrUnavailable }

func (*Ring) RegisterBuffers(int, int) error { return ErrUnavailable }

func (*Ring) Buffer(int) ([]byte, error) { return nil, ErrUnavailable }

func (*Ring) useReadArena([]byte) error { return ErrUnavailable }

func (*Ring) registerFrameArena([]byte) error { return ErrUnavailable }

func (*Ring) PrepareWriteFixed(int, int, int, int64, uint64, bool) error {
	return ErrUnavailable
}

func (*Ring) prepareWriteFixedDataSync(int, int, int, int64, uint64, bool) error {
	return ErrUnavailable
}

func (*Ring) prepareWriteVectorDataSync(
	int, []Write, int, []byte, int, uint64, bool,
) (uint32, error) {
	return 0, ErrUnavailable
}

func (*Ring) prepareWriteBytes(int, []byte, int64, uint64, bool) error {
	return ErrUnavailable
}

func (*Ring) prepareWriteFrame(int, []byte, int64, uint64, bool) error {
	return ErrUnavailable
}

func (*Ring) prepareReadArena(int, int, int, int64, uint64) error { return ErrUnavailable }

func (*Ring) PrepareDataSync(int, uint64, bool) error { return ErrUnavailable }

func (*Ring) SubmitAndWait(uint32) error { return ErrUnavailable }

func (*Ring) Available() uint32 { return 0 }

func (*Ring) Pop() (Completion, bool, error) { return Completion{}, false, ErrUnavailable }

func (*Ring) PopBatch(uint32) ([]Completion, error) { return nil, ErrUnavailable }

func (*Ring) Close() error { return nil }
