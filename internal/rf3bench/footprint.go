package rf3bench

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
)

// Footprint is a filesystem cut for a set of benchmark storage roots.
// ApparentBytes is the sum of logical file lengths. AllocatedBytes is the
// blocks physically charged by the filesystem when the platform exposes the
// conventional Stat_t.Blocks field; otherwise it conservatively equals the
// apparent length. Directories and symlinks are not counted as data files.
type Footprint struct {
	ApparentBytes  uint64
	AllocatedBytes uint64
	Files          uint64
}

// MeasureFootprint walks each exact storage root without following symlinks.
// It is intended for detached before/after benchmark cuts, not admission or a
// live storage accounting contract.
func MeasureFootprint(roots ...string) (Footprint, error) {
	if len(roots) == 0 {
		return Footprint{}, errors.New("rf3bench: no storage roots")
	}
	var footprint Footprint
	for _, root := range roots {
		if root == "" {
			return Footprint{}, errors.New("rf3bench: empty storage root")
		}
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			apparent := uint64(max(info.Size(), 0))
			footprint.Files++
			footprint.ApparentBytes += apparent
			footprint.AllocatedBytes += allocatedFileBytes(info, apparent)
			return nil
		})
		if err != nil {
			return Footprint{}, err
		}
	}
	return footprint, nil
}

func allocatedFileBytes(info fs.FileInfo, fallback uint64) uint64 {
	value := reflect.ValueOf(info.Sys())
	if !value.IsValid() {
		return fallback
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return fallback
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return fallback
	}
	blocks := value.FieldByName("Blocks")
	if !blocks.IsValid() {
		return fallback
	}
	var count uint64
	switch blocks.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if blocks.Int() < 0 {
			return fallback
		}
		count = uint64(blocks.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		count = blocks.Uint()
	default:
		return fallback
	}
	// POSIX stat reports st_blocks in 512-byte units, independently of the
	// filesystem's allocation unit or du display block size.
	return count * 512
}
