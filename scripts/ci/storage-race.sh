#!/usr/bin/env bash
set -euo pipefail

lane=${1:?usage: storage-race.sh storeio|durable-heavy|durable-rest}
selected='Primary|BufferedInplace|Committer|PageCache|WriteTransaction'
heavy='^TestFilePrimaryAdvancedRepackAmplification$'
case "$lane" in
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
