package durable

import (
	"strings"
	"testing"
)

func TestPublicErrorsUseVibeDBNamespace(t *testing.T) {
	t.Parallel()
	errors := map[string]error{
		"ErrBatchClosed":                 ErrBatchClosed,
		"ErrBatchTooLarge":               ErrBatchTooLarge,
		"ErrCheckpointRequired":          ErrCheckpointRequired,
		"ErrClosed":                      ErrClosed,
		"ErrCollectionExists":            ErrCollectionExists,
		"ErrCollectionName":              ErrCollectionName,
		"ErrCommitOutcomeUnknown":        ErrCommitOutcomeUnknown,
		"ErrDatabaseClosed":              ErrDatabaseClosed,
		"ErrDocumentTooLarge":            ErrDocumentTooLarge,
		"ErrIndexBuildInProgress":        ErrIndexBuildInProgress,
		"ErrKeyTooLarge":                 ErrKeyTooLarge,
		"ErrNotEmpty":                    ErrNotEmpty,
		"ErrOverflowChainCorrupt":        ErrOverflowChainCorrupt,
		"ErrPrimaryBatchUnsupportedLane": ErrPrimaryBatchUnsupportedLane,
		"ErrPrimaryCutoverUnsupported":   ErrPrimaryCutoverUnsupported,
		"ErrPrimaryLeafSplitRequired":    ErrPrimaryLeafSplitRequired,
		"ErrPrimaryMacroSplitRequired":   ErrPrimaryMacroSplitRequired,
		"ErrStoreDirectIOUnsupported":    ErrStoreDirectIOUnsupported,
		"ErrUnsupportedPageSize":         ErrUnsupportedPageSize,
		"ErrUnsupportedDatabaseLayout":   ErrUnsupportedDatabaseLayout,
		"ErrWriterLocked":                ErrWriterLocked,
		"ErrWriterLockUnsupported":       ErrWriterLockUnsupported,
	}
	for name, err := range errors {
		if err == nil || !strings.HasPrefix(err.Error(), "vibedb: ") {
			t.Errorf("%s = %q, want vibedb namespace", name, err)
		}
	}
}
