#!/usr/bin/env bash
set -euo pipefail

# Keep mmap-heavy package binaries serial within a runner. Parallelism is
# provided by isolated Actions runners, not competing arenas on one machine.
shard=${1:?usage: test-shard.sh durable|sql|process|core [--list]}
case "$shard" in
  durable|sql|process|core) ;;
  *) echo "unknown test shard: $shard" >&2; exit 2 ;;
esac
if [[ $# -gt 2 || ( $# -eq 2 && $2 != --list ) ]]; then
  echo 'usage: test-shard.sh durable|sql|process|core [--list]' >&2
  exit 2
fi

# Discover packages each run so new packages cannot silently lose coverage.
# Capture first: a failed go list must not look like an empty successful shard.
packages=$(go list ./...)
selected=()
while IFS= read -r package; do
  case "${package#github.com/thesyncim/vibedb}" in
    /store/durable) owner=durable ;;
    /query|/store|/sql/*|/pgwire) owner=sql ;;
    /cmd/vibedb-gateway|/cmd/vibedb-shard) owner=process ;;
    *) owner=core ;;
  esac
  if [[ "$owner" == "$shard" ]]; then
    selected+=("$package")
  fi
done <<< "$packages"
if [[ ${#selected[@]} -eq 0 ]]; then
  echo "no packages selected for $shard" >&2
  exit 1
fi
if [[ ${2:-} == --list ]]; then
  printf '%s\n' "${selected[@]}"
  exit 0
fi
printf 'Test shard %s: %s packages\n' "$shard" "${#selected[@]}"
exec go test -p=1 -timeout=25m "${selected[@]}"
