//go:build race

package shardservice

// The race runtime randomly discards sync.Pool entries, so pooled-parser
// allocation counts are meaningful only in the ordinary test build.
const raceDetectorEnabled = true
