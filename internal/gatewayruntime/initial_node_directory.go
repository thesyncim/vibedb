package gatewayruntime

import (
	"bytes"
	"errors"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibejson"
	"io"
	"os"
)

func loadInitialNodeDirectory(path string) ([]gateway.NodeRecord, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	const maximum = 16 << 20
	raw, readErr := io.ReadAll(io.LimitReader(file, maximum+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nil, err
	}
	if len(raw) > maximum {
		return nil, gateway.ErrScalingMetadataBound
	}
	var records []gateway.NodeRecord
	if err := vibejson.Unmarshal(raw, &records); err != nil {
		return nil, err
	}
	if len(records) == 0 || len(records) > gateway.MaxScalingNodes {
		return nil, gateway.ErrInvalidScalingMetadata
	}
	canonical, err := vibejson.Marshal(&records)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, gateway.ErrInvalidScalingMetadata
	}
	for index, record := range records {
		if !record.Valid() || record.Lifecycle != gateway.NodeActive || record.Revision != 1 || index > 0 && bytes.Compare(records[index-1].NodeID[:], record.NodeID[:]) >= 0 {
			return nil, gateway.ErrInvalidScalingMetadata
		}
	}
	return records, nil
}
