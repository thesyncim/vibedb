#!/usr/bin/env bash
# Run the bounded clean-Linux competitive qualification used by pull-request CI.
# The outputs are raw evidence, not publication-grade results or win claims.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 ABSOLUTE_NEW_OUTPUT_DIRECTORY" >&2
  exit 2
fi

evidence=$1
if [[ ${evidence} != /* || -e ${evidence} ]]; then
  echo "output must be an absolute path that does not exist" >&2
  exit 2
fi
if [[ $(uname -s) != Linux ]]; then
  echo "competitive qualification requires Linux" >&2
  exit 1
fi

repo=$(git rev-parse --show-toplevel)
if [[ -n $(git -C "${repo}" status --porcelain=v1 --untracked-files=normal) ]]; then
  echo "competitive qualification requires a clean working tree" >&2
  exit 1
fi
revision=$(git -C "${repo}" rev-parse HEAD)
mkdir -m 700 "${evidence}"
scratch=$(mktemp -d)
trap 'rm -rf -- "${scratch}"' EXIT

filesystem=$(stat -f -c '%T' "${evidence}")
{
  printf 'meta\tcommand_schema\tvibedb.ci-evidence/1\n'
  printf 'meta\trevision\t%s\n' "${revision}"
  printf 'meta\tvcs_modified\tfalse\n'
  printf 'meta\tgo_version\t%s\n' "$(go version | tr '\t\r\n' '   ')"
  printf 'meta\tgoos\t%s\n' "$(go env GOOS)"
  printf 'meta\tgoarch\t%s\n' "$(go env GOARCH)"
  printf 'meta\tkernel\t%s\n' "$(uname -srvmo | tr '\t\r\n' '   ')"
  printf 'meta\tfilesystem\t%s\n' "${filesystem}"
  printf 'meta\tembedded_repetitions\t9\n'
  printf 'meta\trf3_repetitions\t3\n'
  printf 'meta\tchaos_repetitions\t3\n'
  printf 'meta\tcorpus_documents\t256\n'
  printf 'meta\tmeasured_operations\t512\n'
} > "${evidence}/metadata.tsv"
sync "${evidence}/metadata.tsv"

cd "${repo}/bench/competitive"
go build -trimpath -o "${scratch}/mixed" ./cmd/mixed
go build -trimpath -o "${scratch}/mixedsuite" ./cmd/mixedsuite
go build -trimpath -o "${scratch}/footprint" ./cmd/footprint
go build -trimpath -o "${scratch}/publishcheck" ./cmd/publishcheck

common_suite=(
  -mixed-bin="${scratch}/mixed"
  -repetitions=9 -conditioning=true -timeout=3m
  -workload=ycsb-a -corpus=256 -operations=512 -warmup=64
  -checkpoint-mutations=0 -clients=1 -cardinality=low
  -document-shape=inline
)
"${scratch}/mixedsuite" "${common_suite[@]}" \
  -engines=vibedb,bbolt,badger,pebble,sqlite -durability=ordinary-sync \
  -exact-indexes=0 \
  -output="${evidence}/mixed-ordinary-sync.tsv"
"${scratch}/mixedsuite" "${common_suite[@]}" \
  -engines=vibedb,sqlite -durability=ordinary-sync -exact-indexes=3 \
  -output="${evidence}/mixed-indexed-ordinary-sync.tsv"
"${scratch}/mixedsuite" "${common_suite[@]}" \
  -engines=vibedb,sqlite -durability=power-safe -exact-indexes=3 \
  -output="${evidence}/mixed-power-safe.tsv"

for engine in badger bbolt pebble sqlite vibedb; do
  "${scratch}/footprint" -header -engine="${engine}" -corpus=2048 \
    -durability=ordinary-sync -document-shape=overflow-heavy -exact-indexes=0 \
    -storage-profile=intrinsic > "${evidence}/footprint-${engine}.tsv"
done
for engine in sqlite vibedb; do
  "${scratch}/footprint" -header -engine="${engine}" -corpus=2048 \
    -durability=ordinary-sync -document-shape=overflow-heavy -exact-indexes=3 \
    -storage-profile=intrinsic > "${evidence}/footprint-indexed-${engine}.tsv"
done

cd "${repo}"
for run in 1 2 3; do
  run_directory=$(printf '%s/rf3/run-%02d' "${evidence}" "${run}")
  mkdir -m 700 -p "${run_directory}"
  for workload in read write mixed; do
    VIBEDB_RF3_BENCH=1 \
    VIBEDB_RF3_OUTPUT="${run_directory}" \
    VIBEDB_RF3_OPERATIONS=512 \
    VIBEDB_RF3_WARMUP=32 \
    VIBEDB_RF3_WORKLOAD="${workload}" \
      go test -count=1 -timeout=10m -run '^TestRF3EvidenceMatrix$' ./internal/raftservice
  done
done
go run ./bench/rf3chaos -output "${evidence}/rf3-chaos.tsv" -runs 3 -timeout 3m

"${scratch}/publishcheck" -qualification -evidence "${evidence}" -write-receipt
sync "${evidence}/VALIDATED.tsv"
