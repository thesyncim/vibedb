# Development format 0 golden images

These files are byte-exact fixtures for the current unreleased on-disk format.
They are not a compatibility promise.

When a codec change is intentional, regenerate or replace the affected fixture
and update its test oracle. Keep malformed old layouts rejected. Do not add a
compatibility branch for an obsolete development image.

See `docs/format.md` for the readable format map. The codecs and validation in
`internal/storeio` are authoritative.
