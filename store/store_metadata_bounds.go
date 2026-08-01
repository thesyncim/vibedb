package store

import "errors"

// ErrCheckpointTooLarge reports collection metadata that exceeds the format's
// 32-bit counts or lengths. It bounds the in-memory builders (packed index,
// mapped keys/docs, document templates) independently of the deleted on-disk
// image serializer that once shared this file.
var ErrCheckpointTooLarge = errors.New("vibedb: collection image metadata exceeds format bounds")
