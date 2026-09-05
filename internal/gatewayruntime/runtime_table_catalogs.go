package gatewayruntime

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	vibejson "github.com/thesyncim/vibejson"
)

const (
	maxGatewayTableCatalogPathBytes = 64 << 10
	maxGatewayTableCatalogPaths     = 128
)

func loadGatewayTableCatalogPaths(path string) ([]string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxGatewayTableCatalogPathBytes {
		return nil, errors.Join(ErrInvalidConfig, err)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	opened, statErr := file.Stat()
	if statErr != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, errors.Join(ErrInvalidConfig, statErr, file.Close())
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maxGatewayTableCatalogPathBytes+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > maxGatewayTableCatalogPathBytes {
		return nil, fmt.Errorf("%w: table catalog path list exceeds bounds", ErrInvalidConfig)
	}
	var paths []string
	if err := vibejson.Unmarshal(raw, &paths); err != nil || paths == nil || len(paths) > maxGatewayTableCatalogPaths {
		return nil, errors.Join(ErrInvalidConfig, err)
	}
	canonical, err := vibejson.Marshal(&paths)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errors.Join(ErrInvalidConfig, err)
	}
	seen := make(map[string]struct{}, len(paths))
	for _, entry := range paths {
		if err := validateGatewayConfigPath("table catalog fragment", entry, true); err != nil {
			return nil, err
		}
		if _, duplicate := seen[entry]; duplicate {
			return nil, fmt.Errorf("%w: duplicate table catalog fragment", ErrInvalidConfig)
		}
		seen[entry] = struct{}{}
	}
	return paths, nil
}
