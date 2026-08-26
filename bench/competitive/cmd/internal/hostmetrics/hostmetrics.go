// Package hostmetrics exposes the small set of host measurements whose
// semantics are strong enough for competitive benchmark guards.
package hostmetrics

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// LinuxPhysicalWriteBytes reports storage-layer bytes charged to this process
// by /proc/self/io. It excludes writes satisfied without reaching the block
// layer and is therefore never labelled as device or media bytes.
func LinuxPhysicalWriteBytes() (int64, bool, error) {
	f, err := os.Open("/proc/self/io")
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		name, value, ok := strings.Cut(scanner.Text(), ":")
		if !ok || name != "write_bytes" {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return n, true, err
	}
	if err := scanner.Err(); err != nil {
		return 0, false, err
	}
	return 0, false, fmt.Errorf("/proc/self/io has no write_bytes field")
}
