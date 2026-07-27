//go:build !darwin

package storeio

import "os"

func materializationPhaseBarrier(file *os.File) error {
	return materializationSync(file)
}

func materializationSync(file *os.File) error { return dataSync(file) }
