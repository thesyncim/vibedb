# Development format 0 goldens

These fixtures are sparse hexadecimal renderings of complete byte images.
The `size` line fixes the image length, each following line gives a hexadecimal
offset and byte string, and every byte not named by a line is authoritatively
zero.

`TestFormat0PrintGolden` regenerates the text from public encoders without
writing the fixture. For example:

```sh
STOREIO_FORMAT0_GOLDEN=empty_inline_superblock \
  go test ./internal/storeio -run '^TestFormat0PrintGolden$' -count=1 -v
```

`sealed_inline_superblock` pins the non-zero main-file physical-capacity field
and its enclosing inline-root checksum.

Changing a fixture is an intentional on-disk format change and should be
reviewed byte-for-byte.
