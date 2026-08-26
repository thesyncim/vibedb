// Package buildgate provides the allocation-free compatibility boundary for
// the one current VibeDB wire and disk grammar.
//
// Grammar identifiers are opaque fixed bytes. They are deliberately not
// versions: a build either names the exact grammar it implements or it does
// not. Capabilities permit independently deployable current-format features
// without adding a decoder ladder. Peer admission requires exact grammar
// equality and mutual satisfaction of required capabilities.
//
// Disk adoption is a separate fail-closed boundary. AdoptDisk inspects a
// target without modifying it, obtains a permit tied to that exact identity,
// and only then permits mutation or repair.
package buildgate
