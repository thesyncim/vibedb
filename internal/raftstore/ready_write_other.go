//go:build !linux

package raftstore

import "os"

func defaultDurableReadyWrite() func(*os.File, []byte, int64) (int, error) {
	return nil
}
