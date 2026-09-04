#!/usr/bin/env bash
set -euo pipefail

export GOEXPERIMENT=${GOEXPERIMENT:-simd}

# Keep mmap-heavy durable package binaries serial. The remaining shards use
# two package workers: measured SQL and process packages otherwise leave half
# of the runner idle, while core's many packages serialize avoidably.
shard=${1:?usage: test-shard.sh durable|durable-churn|durable-large-cache|sql|process|core [--list]}
case "$shard" in
  durable|durable-churn|durable-large-cache|sql|process|core) ;;
  *) echo "unknown test shard: $shard" >&2; exit 2 ;;
esac
if [[ $# -gt 2 || ( $# -eq 2 && $2 != --list ) ]]; then
  echo 'usage: test-shard.sh durable|durable-churn|durable-large-cache|sql|process|core [--list]' >&2
  exit 2
fi

# Discover packages each run so new packages cannot silently lose coverage.
# Capture first: a failed go list must not look like an empty successful shard.
packages=$(go list ./...)
selected=()
package_shard=$shard
if [[ "$shard" == durable-churn || "$shard" == durable-large-cache ]]; then
  package_shard=durable
fi
while IFS= read -r package; do
  case "${package#github.com/thesyncim/vibedb}" in
    /store/durable) owner=durable ;;
    /query|/store|/sql/*|/pgwire) owner=sql ;;
    /cmd/vibedb-gateway|/cmd/vibedb-shard|/internal/gatewayruntime) owner=process ;;
    *) owner=core ;;
  esac
  if [[ "$owner" == "$package_shard" ]]; then
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
# The two pressure qualifications consumed 167s of 287s in the measured
# package. Complementary anchored filters retain every test and subtest.
pressure_tests='^(TestFilePrimaryChurnQualification|TestFilePrimaryLargerThanCacheQualification)$'
package_parallelism=1
case "$shard" in
  sql|process|core) package_parallelism=2 ;;
esac
test_args=(-json "-p=${package_parallelism}" -timeout=25m)
case "$shard" in
  durable) test_args+=(-skip "$pressure_tests") ;;
  durable-churn) test_args+=(-run '^TestFilePrimaryChurnQualification$') ;;
  durable-large-cache) test_args+=(-run '^TestFilePrimaryLargerThanCacheQualification$') ;;
esac
exec go test "${test_args[@]}" "${selected[@]}"
