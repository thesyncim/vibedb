// Package storeio implements the durable page/device engine behind
// store/durable: the superblock, the common page envelope, every directory
// and leaf page kind, checksums, and the copy-on-write commit protocol. The
// on-disk byte format this package reads and writes is implemented by its
// encode/decode functions. /docs/format.md explains that format, but the
// codecs and byte-exact golden tests are authoritative when prose and code
// disagree.
package storeio
