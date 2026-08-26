//go:build !linux && !darwin

package hostmetrics

import "fmt"

func PhysicalMemoryBytes() (int64, error) {
	return 0, fmt.Errorf("physical memory measurement unsupported")
}
func MaxRSSBytes() (int64, error) { return 0, fmt.Errorf("RSS measurement unsupported") }
func DropFileCaches() (string, error) {
	return "unsupported", fmt.Errorf("whole-host file-cache drop unsupported")
}
