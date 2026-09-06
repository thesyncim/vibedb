#!/usr/bin/env bash
set -euo pipefail

lane=${1:?usage: storage-race.sh all|storeio|durable-heavy|durable-rest}
selected='Primary|CompactRankAffine|BufferedInplace|Committer|PageCache|WriteTransaction'
heavy='^TestFilePrimaryAdvancedRepackAmplification$'
case "$lane" in
  all)
    work=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/vibedb-storage-race.XXXXXX")
    trap 'rm -rf "$work"' EXIT

    # Compile the durable race binary once, then exercise the complementary
    # filters concurrently. The storeio package is independent and can build
    # while the durable binary compiles.
    go test -race -timeout=25m -skip 'Qualification' -run "$selected" \
      ./internal/storeio/ >"$work/storeio.log" 2>&1 &
    storeio_pid=$!
    go test -race -c -o "$work/durable.test" ./store/durable/
    "$work/durable.test" -test.timeout=25m -test.run="$heavy" \
      >"$work/durable-heavy.log" 2>&1 &
    heavy_pid=$!
    "$work/durable.test" -test.timeout=25m -test.run="$selected" \
      -test.skip="Qualification|${heavy}" >"$work/durable-rest.log" 2>&1 &
    rest_pid=$!

    status=0
    for result in \
      "$storeio_pid:$work/storeio.log" \
      "$heavy_pid:$work/durable-heavy.log" \
      "$rest_pid:$work/durable-rest.log"; do
      pid=${result%%:*}
      log=${result#*:}
      if ! wait "$pid"; then
        status=1
      fi
      cat "$log"
    done
    exit "$status"
    ;;
  storeio)
    exec go test -race -timeout=25m -skip 'Qualification' -run "$selected" \
      ./internal/storeio/
    ;;
  durable-heavy)
    exec go test -race -timeout=25m -run "$heavy" ./store/durable/
    ;;
  durable-rest)
    exec go test -race -timeout=25m \
      -skip "Qualification|${heavy}" -run "$selected" ./store/durable/
    ;;
  *)
    echo "unknown storage race lane: $lane" >&2
    exit 2
    ;;
esac
