//go:build windows

package durable

// Windows has no Unix directory-fsync primitive. Journal creation itself is
// flushed before this point; database publication uses the platform's
// write-through rename path for namespace durability.
func syncRecoveryJournalParent(string) error { return nil }
